"""Connection pool, transaction retry policy, schema init, advisory lock.

Foundation shared by the method-group mixins (:mod:`src.pg_catalog`,
:mod:`src.pg_sessions`, :mod:`src.pg_tapes`) that together form
:class:`src.pg_db.PgDatabaseManager`.
"""
import hashlib
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    import psycopg
    from psycopg import errors
    from psycopg.rows import dict_row
    from psycopg_pool import PoolTimeout
else:
    try:
        import psycopg
        from psycopg import errors
        from psycopg.rows import dict_row
        from psycopg_pool import PoolTimeout
    except ImportError:  # pragma: no cover - handled by require_psycopg at runtime
        psycopg = None
        errors = None
        dict_row = None
        PoolTimeout = None

from .constants import PROJECT_ROOT
from .pg_bulk import make_pool, require_psycopg


class PgRow(dict):
    """Small row wrapper matching the row access the app expects."""

    def __init__(self, data):
        super().__init__(data)
        self._keys = tuple(data.keys())

    def __getitem__(self, key):
        if isinstance(key, int):
            return dict.__getitem__(self, self._keys[key])
        return dict.__getitem__(self, key)

    def keys(self):
        return self._keys
def _rows(items):
    return [PgRow(dict(item)) for item in items]
def _row(item):
    return PgRow(dict(item)) if item is not None else None
def _valid_columns(kwargs):
    for key in kwargs:
        if not key.replace("_", "").isalnum():
            raise RuntimeError(f"[DB] Unsafe column name: {key}")
def _now_utc():
    return datetime.now(timezone.utc)
def _as_utc(value):
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc)
    return value
def _coerce_timestamptz(value):
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value)
        except ValueError:
            return value
    return _as_utc(value)
def _coerce_timestamp_kwargs(kwargs):
    timestamp_columns = {"created_at", "started_at", "completed_at", "updated_at"}
    return {
        key: _coerce_timestamptz(value) if key in timestamp_columns else value
        for key, value in kwargs.items()
    }


class PgConnectionCore:
    """Pool, retrying transaction runner, schema init and archiver lock."""

    # Session-level advisory lock key ('LTO8'): cross-process single-writer
    # guard for tape-write runs. The in-process tape I/O lock cannot stop a
    # second `python run.py` instance; this can.
    ARCHIVER_LOCK_KEY = 0x4C544F38

    def __init__(self, conninfo, *, init_schema=True, pool=None):
        require_psycopg()
        self.db_path = conninfo
        self._lock_conn: Optional[Any] = None
        self._pool: Any = pool or make_pool(
            conninfo, min_size=1, max_size=8, row_factory=dict_row)
        try:
            if init_schema:
                self._init_schema()
                self._ensure_runtime_constraints()
        except Exception:
            self.close()
            raise

    def _init_schema(self):
        sql_dir = Path(PROJECT_ROOT) / "scripts" / "sql"
        migrations = (
            "001_postgres_schema.sql",
            "002_postgres_indexes.sql",
            "003_postgres_constraints.sql",
            "004_postgres_archive_runs_sessions.sql",
            "005_postgres_session_label_unique.sql",
            "006_postgres_remote_streaming.sql",
            "008_postgres_remote_provenance.sql",
            "009_postgres_remote_session_fk.sql",
            "010_postgres_local_manifest_archive.sql",
            "011_postgres_tape_status.sql",
            "013_postgres_tape_reset_safety.sql",
        )
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                for migration in migrations:
                    cur.execute((sql_dir / migration).read_text(encoding="utf-8"))
            conn.commit()

    def apply_directory_catalog_schema(self):
        """Explicitly install the directory-catalog schema migration.

        This migration is intentionally not part of startup schema init because
        production migration must happen only after a verified backup and on the
        chosen target database.
        """
        sql_path = (Path(PROJECT_ROOT) / "scripts" / "sql"
                    / "007_postgres_directory_catalog.sql")
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql_path.read_text(encoding="utf-8"))
            conn.commit()

    # ------------------------------------------------------------------
    # Migration 015 - per-chunk/container packaging formats (Plan 2)
    # ------------------------------------------------------------------

    CONTAINER_FORMAT_MIGRATION = "015_postgres_container_formats.sql"
    STORED_TAR_PLAN_MIGRATION = "016_postgres_stored_tar_plans.sql"
    STORED_TAR_PUBLICATION_MIGRATION = (
        "017_postgres_stored_tar_publication.sql")
    MANIFEST_DIRECTORY_CATALOG_MIGRATION = (
        "018_postgres_manifest_directory_catalog.sql")

    @classmethod
    def container_format_migration_path(cls):
        return (Path(PROJECT_ROOT) / "scripts" / "sql"
                / cls.CONTAINER_FORMAT_MIGRATION)

    @classmethod
    def container_format_migration_checksum(cls):
        """SHA-256 identity of the explicit migration file (read-only)."""
        return hashlib.sha256(
            cls.container_format_migration_path().read_bytes()).hexdigest()

    def apply_container_format_schema(
            self, *, exception_session_id=None, expected_boundary=None,
            approval_id=None, approval_reason=None, staging_evidence=None,
            require_archiver_lock=False):
        """Atomically apply migration 015, optionally with one proved exception.

        Migration 015 is never part of startup.  The normal call backfills every
        pre-existing chunk as immutable ZIP.  The optional arguments are the
        deliberately narrow operator-approved exception required for a legacy
        mixed-format session: all five values must be present together, and the
        SQL transaction independently re-runs the database evidence query before
        assigning a never-started suffix directly to ``stored_tar``.

        ``staging_evidence`` is produced by the guarded CLI immediately before
        this call, while process/advisory-lock checks prove quiescence.  It never
        contains a source filename or staging-root path.
        """
        supplied = (
            exception_session_id, expected_boundary, approval_id,
            approval_reason, staging_evidence)
        if any(value is not None for value in supplied) and not all(
                value is not None for value in supplied):
            raise ValueError(
                "container-format exception requires session, expected "
                "boundary, approval id/reason, and staging evidence")
        if exception_session_id is not None:
            if int(exception_session_id) <= 0 or int(expected_boundary) < 0:
                raise ValueError("invalid container-format exception identity")
            approval_id = str(approval_id).strip()
            approval_reason = str(approval_reason).strip()
            if (not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", approval_id)
                    or not approval_reason or len(approval_reason) > 512
                    or any(not char.isprintable()
                           for char in approval_reason)):
                raise ValueError(
                    "approval id/reason are blank, malformed, or too long")
            self._validate_container_format_staging_evidence(staging_evidence)

        reporter = getattr(self, "container_format_schema_report", None)
        if callable(reporter):
            report = reporter()
            state = report.get("installation_state")
            if state == "partial" or (report.get("metadata")
                                       and not report.get("ready")):
                raise RuntimeError(
                    "[DB] Refusing to apply migration 015 over a partial or "
                    "drifted installation: " + "; ".join(report["issues"]))

        sql_path = self.container_format_migration_path()
        checksum = self.container_format_migration_checksum()
        settings = {
            "lto.container_format_migration_checksum": checksum,
            "lto.container_format_exception_session_id": (
                "" if exception_session_id is None
                else str(int(exception_session_id))),
            "lto.container_format_expected_boundary": (
                "" if expected_boundary is None
                else str(int(expected_boundary))),
            "lto.container_format_approval_id": (
                "" if approval_id is None else str(approval_id).strip()),
            "lto.container_format_approval_reason": (
                "" if approval_reason is None else str(approval_reason).strip()),
            "lto.container_format_staging_evidence": (
                "" if staging_evidence is None else json.dumps(
                    staging_evidence, sort_keys=True, separators=(",", ":"))),
        }

        def apply_on(conn):
            # One transaction owns both authorization and every DDL/backfill
            # statement.  When the guarded CLI holds the archiver lock, this is
            # deliberately the *same PostgreSQL session*: losing that session
            # loses both the lock and the migration transaction.
            with conn.transaction():
                if require_archiver_lock:
                    owned = conn.execute(
                        """SELECT EXISTS (
                               SELECT 1 FROM pg_locks
                               WHERE locktype='advisory' AND granted
                                 AND pid=pg_backend_pid()
                                 AND classid=%s AND objid=%s
                                 AND objsubid=1) AS owned""",
                        (self.ARCHIVER_LOCK_KEY >> 32,
                         self.ARCHIVER_LOCK_KEY & 0xFFFFFFFF),
                    ).fetchone()["owned"]
                    if not owned:
                        raise RuntimeError(
                            "[DB] Migration 015 requires the current database "
                            "session to own the archiver advisory lock")
                for name, value in settings.items():
                    conn.execute("SELECT set_config(%s, %s, true)",
                                 (name, value))
                with conn.cursor() as cur:
                    cur.execute(sql_path.read_text(encoding="utf-8"))

        if require_archiver_lock:
            if self._lock_conn is None:
                raise RuntimeError(
                    "[DB] Migration 015 requires a pinned archiver lock")
            apply_on(self._lock_conn)
        else:
            with self._pool.connection() as conn:
                apply_on(conn)
        return [self.CONTAINER_FORMAT_MIGRATION]

    def apply_stored_tar_plan_schema(self):
        """Explicitly install Tasks 2.1-2.4 Stored-TAR persistence."""
        sql_dir = Path(PROJECT_ROOT) / "scripts" / "sql"
        migrations = (
            self.STORED_TAR_PLAN_MIGRATION,
            self.STORED_TAR_PUBLICATION_MIGRATION,
        )
        with self._pool.connection() as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    for migration in migrations:
                        cur.execute((sql_dir / migration).read_text(
                            encoding="utf-8"))
        return list(migrations)

    @classmethod
    def manifest_directory_catalog_migration_path(cls):
        return (Path(PROJECT_ROOT) / "scripts" / "sql"
                / cls.MANIFEST_DIRECTORY_CATALOG_MIGRATION)

    @classmethod
    def manifest_directory_catalog_migration_checksum(cls):
        """SHA-256 identity of explicit migration 018 (read-only)."""
        return hashlib.sha256(
            cls.manifest_directory_catalog_migration_path().read_bytes()
        ).hexdigest()

    def apply_manifest_directory_catalog_schema(
            self, *, require_archiver_lock=False):
        """Atomically apply migration 018 through its checksum guard.

        Migration 018 is explicit-only and is deliberately absent from
        :meth:`_init_schema`.  A single PostgreSQL transaction owns its
        checksum authorization, all DDL, and the legacy ``plan_source``
        backfill.  The guarded inspector command pins the archiver advisory
        lock to this same database session; isolated tests may apply directly
        without that operational precondition.
        """
        reporter = getattr(
            self, "manifest_directory_catalog_schema_report", None)
        if callable(reporter):
            report = reporter()
            state = report.get("installation_state")
            if state == "partial" or (
                    state == "installed"
                    and not report.get("ready")):
                raise RuntimeError(
                    "[DB] Refusing to apply migration 018 over a partial or "
                    "drifted installation: "
                    + "; ".join(report.get("schema_issues")
                                or report.get("issues") or ("unknown drift",)))

        sql_path = self.manifest_directory_catalog_migration_path()
        checksum = self.manifest_directory_catalog_migration_checksum()

        def apply_on(conn):
            with conn.transaction():
                if require_archiver_lock:
                    owned = conn.execute(
                        """SELECT EXISTS (
                               SELECT 1 FROM pg_locks
                               WHERE locktype='advisory' AND granted
                                 AND pid=pg_backend_pid()
                                 AND classid=%s AND objid=%s
                                 AND objsubid=1) AS owned""",
                        (self.ARCHIVER_LOCK_KEY >> 32,
                         self.ARCHIVER_LOCK_KEY & 0xFFFFFFFF),
                    ).fetchone()["owned"]
                    if not owned:
                        raise RuntimeError(
                            "[DB] Migration 018 requires the current database "
                            "session to own the archiver advisory lock")
                conn.execute(
                    "SELECT set_config(%s, %s, true)",
                    ("lto.manifest_directory_catalog_migration_checksum",
                     checksum))
                with conn.cursor() as cur:
                    cur.execute(sql_path.read_text(encoding="utf-8"))

        if require_archiver_lock:
            if self._lock_conn is None:
                raise RuntimeError(
                    "[DB] Migration 018 requires a pinned archiver lock")
            apply_on(self._lock_conn)
        else:
            with self._pool.connection() as conn:
                apply_on(conn)
        return [self.MANIFEST_DIRECTORY_CATALOG_MIGRATION]

    @staticmethod
    def _validate_container_format_staging_evidence(evidence):
        """Accept only a fresh, path-free local-staging attestation."""
        expected_keys = {
            "root_accessible", "checked_chunk_indexes", "entry_count",
            "unreadable_count", "checked_at"}
        if not isinstance(evidence, dict) or set(evidence) != expected_keys:
            raise ValueError(
                "staging evidence must contain only the approved aggregate "
                "fields")
        if evidence["root_accessible"] is not True:
            raise ValueError("staging root was not proved accessible")
        indexes = evidence["checked_chunk_indexes"]
        if (not isinstance(indexes, list)
                or any(isinstance(value, bool) or not isinstance(value, int)
                       or value < 0 for value in indexes)
                or indexes != sorted(set(indexes))):
            raise ValueError("staging chunk indexes are not canonical")
        for name in ("entry_count", "unreadable_count"):
            value = evidence[name]
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"staging evidence {name} is invalid")
        try:
            checked_at = datetime.fromisoformat(str(evidence["checked_at"]))
            if checked_at.tzinfo is None:
                raise ValueError
            age = (datetime.now(timezone.utc)
                   - checked_at.astimezone(timezone.utc)).total_seconds()
        except (TypeError, ValueError) as exc:
            raise ValueError("staging evidence timestamp is invalid") from exc
        if age < -5 or age > 300:
            raise ValueError("staging evidence is stale or from the future")

    # ------------------------------------------------------------------
    # Migration 014 — incremental scan frontier (Plan 1, Task 2.1)
    # ------------------------------------------------------------------

    #: Tables the BASE half creates. All of them must exist before the frontier
    #: can even be considered.
    INCREMENTAL_SCAN_TABLES = (
        "remote_scan_scopes",
        "remote_scan_directories",
        "remote_scan_segments",
        "remote_chunk_scan_segments",
        "remote_scan_errors",
        "remote_worker_attempts",
        "remote_frontier_bootstraps",
    )
    #: Nullable-for-legacy columns the BASE half adds to remote_chunks.
    INCREMENTAL_SCAN_CHUNK_COLUMNS = (
        "owner_token", "lease_expires_at", "attempt_id", "membership_state",
        "expected_file_count", "expected_bytes",
    )
    #: What the FINALIZE half creates. Installed != finalized.
    INCREMENTAL_SCAN_FINAL_INDEX = "uq_remote_plan_files_chunk_ordinal"

    def incremental_scan_schema_installed(self):
        """True when migration 014's BASE half is fully present.

        Read-only. Checks every table AND every added column: a half-applied
        migration must not read as installed, because the frontier would then
        write rows into a schema that cannot hold all of them.
        """
        with self._pool.connection() as conn:
            if not all(self._table_exists_conn(conn, name)
                       for name in self.INCREMENTAL_SCAN_TABLES):
                return False
            return all(
                self._column_exists_conn(conn, "remote_chunks", column)
                for column in self.INCREMENTAL_SCAN_CHUNK_COLUMNS)

    def incremental_scan_schema_finalized(self):
        """True when migration 014's FINALIZE half has also been applied.

        The finalize step is what makes duplicate chunk membership structurally
        impossible. Until it has run, the base tables exist but nothing
        prevents two ordinals colliding, so the frontier must not activate.
        """
        with self._pool.connection() as conn:
            return conn.execute(
                """SELECT 1 FROM pg_indexes
                   WHERE schemaname='public' AND indexname=%s""",
                (self.INCREMENTAL_SCAN_FINAL_INDEX,),
            ).fetchone() is not None

    def incremental_scan_schema_preflight(self):
        """Read-only report of what applying migration 014 would face.

        Never writes. Returns a dict an operator (and the ``inspect_db.py``
        command) can read BEFORE deciding to execute anything:

          ``installed`` / ``finalized``   current state
          ``archiver_lock_held``          another process is running
          ``duplicate_ordinal_groups``    rows the FINALIZE half would refuse
          ``blocking``                    human-readable reasons not to proceed
        """
        report = {
            "database": None,
            "installed": False,
            "finalized": False,
            "archiver_lock_held": None,
            "duplicate_ordinal_groups": None,
            "duplicate_ordinal_sample": [],
            "blocking": [],
        }
        with self._pool.connection() as conn:
            report["database"] = conn.execute(
                "SELECT current_database() AS db").fetchone()["db"]
            report["installed"] = all(
                self._table_exists_conn(conn, name)
                for name in self.INCREMENTAL_SCAN_TABLES) and all(
                self._column_exists_conn(conn, "remote_chunks", column)
                for column in self.INCREMENTAL_SCAN_CHUNK_COLUMNS)
            report["finalized"] = conn.execute(
                """SELECT 1 FROM pg_indexes
                   WHERE schemaname='public' AND indexname=%s""",
                (self.INCREMENTAL_SCAN_FINAL_INDEX,),
            ).fetchone() is not None

            # Another archiver holding the cluster-wide advisory lock means a
            # live run. Migrating under it is how a session gets its schema
            # changed mid-write.
            # A bigint advisory key appears in pg_locks split across
            # classid (high 32 bits) / objid (low 32), with objsubid=1.
            # Constraining all three avoids matching an unrelated advisory lock.
            held = conn.execute(
                """SELECT COUNT(*) AS n FROM pg_locks
                   WHERE locktype='advisory' AND granted
                     AND classid=%s AND objid=%s AND objsubid=1""",
                (self.ARCHIVER_LOCK_KEY >> 32,
                 self.ARCHIVER_LOCK_KEY & 0xFFFFFFFF),
            ).fetchone()["n"]
            report["archiver_lock_held"] = bool(held)
            if held:
                report["blocking"].append(
                    "an archiver process holds the cluster advisory lock; stop "
                    "it before migrating")

            rows = conn.execute(
                """SELECT plan_id, chunk_index, ordinal, COUNT(*) AS n
                   FROM remote_plan_files
                   GROUP BY plan_id, chunk_index, ordinal
                   HAVING COUNT(*) > 1
                   ORDER BY plan_id, chunk_index, ordinal
                   LIMIT 10"""
            ).fetchall()
            total = conn.execute(
                """SELECT COUNT(*) AS n FROM (
                       SELECT 1 FROM remote_plan_files
                       GROUP BY plan_id, chunk_index, ordinal
                       HAVING COUNT(*) > 1) d"""
            ).fetchone()["n"]
            report["duplicate_ordinal_groups"] = total
            report["duplicate_ordinal_sample"] = [dict(r) for r in rows]
            if total:
                report["blocking"].append(
                    f"{total} duplicate (plan_id, chunk_index, ordinal) "
                    "group(s) in remote_plan_files; the finalize step will "
                    "refuse rather than resequence them")
        return report

    def apply_incremental_scan_schema(self, finalize=False):
        """Install migration 014. Explicit, never part of startup.

        ``finalize=False`` applies only the additive BASE half. ``True`` also
        applies the FINALIZE half, which AUDITS legacy membership and raises
        rather than repairing it.

        This is intentionally not called from ``_init_schema``: a production
        migration must happen after a verified backup, on the database the
        operator chose, with no archiver running — none of which this method
        can establish for itself. ``inspect_db.py
        --apply-incremental-scan-schema`` is the guarded entry point that does.
        """
        sql_dir = Path(PROJECT_ROOT) / "scripts" / "sql"
        files = ["014_postgres_incremental_scan.sql"]
        if finalize:
            files.append("014_postgres_incremental_scan_finalize.sql")
        applied = []
        for name in files:
            with self._pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute((sql_dir / name).read_text(encoding="utf-8"))
                conn.commit()
            applied.append(name)
        return applied

    @staticmethod
    def _table_exists_conn(conn, table_name):
        return conn.execute(
            """SELECT 1
               FROM information_schema.tables
               WHERE table_schema='public' AND table_name=%s""",
            (table_name,),
        ).fetchone() is not None

    @staticmethod
    def _column_exists_conn(conn, table_name, column_name):
        return conn.execute(
            """SELECT 1
               FROM information_schema.columns
               WHERE table_schema='public'
                 AND table_name=%s AND column_name=%s""",
            (table_name, column_name),
        ).fetchone() is not None

    def directory_catalog_schema_installed(self):
        required = (
            "directory_archive_stats",
            "directory_archive_bundles",
            "directory_tree_index",
        )
        with self._pool.connection() as conn:
            return all(self._table_exists_conn(conn, name) for name in required)

    def _require_directory_catalog_schema(self):
        if not self.directory_catalog_schema_installed():
            raise RuntimeError(
                "[DB] Directory catalog schema is not installed on this "
                "database. Apply scripts/sql/007_postgres_directory_catalog.sql "
                "explicitly to the migrated PostgreSQL database after creating "
                "and verifying a production backup. See "
                "docs/directory_catalog_migration_runbook.md."
            )

    def _ensure_runtime_constraints(self):
        with self._pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    DO $$
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1
                            FROM pg_constraint
                            WHERE conname = 'uq_files_record_key'
                              AND conrelid = 'files_index'::regclass
                        ) THEN
                            ALTER TABLE files_index
                                ADD CONSTRAINT uq_files_record_key
                                UNIQUE (record_key);
                        END IF;
                    END $$;
                """)
            conn.commit()

    def _transaction(self, operation, description, attempts=5) -> Any:
        # Serialization conflicts and deadlocks retry quickly; OperationalError
        # and PoolTimeout (connection drops, Docker DB restarts) back off longer
        # so a restarting server can come back. Every write routed through here
        # is an idempotent keyed upsert/update, so re-running a batch whose
        # commit outcome is unknown converges to the same state.
        retryable = (errors.SerializationFailure, errors.DeadlockDetected,
                     psycopg.OperationalError, PoolTimeout)
        for attempt in range(1, attempts + 1):
            try:
                with self._pool.connection() as conn:
                    with conn.transaction():
                        return operation(conn)
            except retryable as e:
                if attempt == attempts:
                    raise
                if isinstance(e, (errors.SerializationFailure,
                                  errors.DeadlockDetected)):
                    wait_s = min(5, attempt)
                else:
                    wait_s = min(30, 2 ** attempt)
                print(f"[DB] {type(e).__name__} during {description}; "
                      f"retrying in {wait_s}s ({attempt}/{attempts})...")
                time.sleep(wait_s)
        raise RuntimeError(f"[DB] Transaction attempts exhausted: {description}")

    def _run_read(self, operation, description, attempts=5) -> Any:
        # Read-only counterpart to _transaction. Connection checkout can raise
        # PoolTimeout (a brief pool-exhaustion spike) or OperationalError (a
        # Docker-PG restart / momentary drop); without a retry here that
        # transient condition escapes a bare `with self._pool.connection()` on
        # a read straight into a pipeline thread's generic handler and tears
        # down a long streaming run — even though the same blip on a write is
        # absorbed by _transaction. Reads are naturally idempotent, so
        # re-running is always safe; back off like the write path so a
        # restarting server has time to come back.
        retryable = (psycopg.OperationalError, PoolTimeout)
        for attempt in range(1, attempts + 1):
            try:
                with self._pool.connection() as conn:
                    return operation(conn)
            except retryable as e:
                if attempt == attempts:
                    raise
                wait_s = min(30, 2 ** attempt)
                print(f"[DB] {type(e).__name__} during {description}; "
                      f"retrying in {wait_s}s ({attempt}/{attempts})...")
                time.sleep(wait_s)
        raise RuntimeError(f"[DB] Read attempts exhausted: {description}")

    @staticmethod
    def _require_updated(cur, message):
        if cur.rowcount == 0:
            raise RuntimeError(message)

    def acquire_archiver_lock(self):
        """Take the cross-process single-writer lock for a tape-write run.

        Rides a dedicated pooled connection (session-level advisory locks
        survive commits) that stays pinned until ``release_archiver_lock``.
        Raises RuntimeError when another archiver instance holds it.
        """
        if self._lock_conn is not None:
            return
        conn = self._pool.getconn()
        try:
            conn.autocommit = True
            locked = conn.execute(
                "SELECT pg_try_advisory_lock(%s) AS locked",
                (self.ARCHIVER_LOCK_KEY,),
            ).fetchone()["locked"]
        except Exception:
            self._pool.putconn(conn)
            raise
        if not locked:
            self._pool.putconn(conn)
            raise RuntimeError(
                "[LOCK] Another archiver instance already holds the "
                "tape-writer lock. Only one tape-write run may be active at "
                "a time; finish or close the other instance and retry."
            )
        self._lock_conn = conn

    def release_archiver_lock(self):
        conn = self._lock_conn
        self._lock_conn = None
        if conn is None:
            return
        try:
            conn.execute(
                "SELECT pg_advisory_unlock(%s)", (self.ARCHIVER_LOCK_KEY,))
        except Exception:
            pass  # dropping the session releases the lock anyway
        try:
            conn.autocommit = False
            if self._pool is not None:
                self._pool.putconn(conn)
        except Exception:
            pass

    def close(self):
        self.release_archiver_lock()
        if self._pool is not None:
            self._pool.close()
            self._pool = None
