"""SQLite implementation of the catalog core hooks.

:class:`SqliteConnectionCore` overrides only the engine-specific hooks of
:class:`src.pg_core.PgConnectionCore`; the method-group mixins (catalog,
sessions, tapes, scan frontier, containers, directory catalog, tape reset) are
the *same classes* on both backends and never look at the backend.

What is different here, in one place:

* The schema is created automatically, in one transaction, from
  ``scripts/sql/sqlite/NNN_*.sql`` and recorded in ``schema_migrations``. The
  PostgreSQL "explicit migration" entry points therefore report *installed at
  creation* instead of running anything.
* The single-writer archiver lock is an OS file lock
  (:mod:`src.sqlite_lock`, the authority) plus an owner-token row in
  ``archiver_lock`` (loss detection). Everything fails closed.
* Bulk loads use ``executemany`` instead of ``COPY``; ``RETURNING (xmax = 0)``
  is replaced by counting pre-existing keys inside the write transaction;
  the two plpgsql functions called from Python are the same logic in Python.
"""
from __future__ import annotations

import hashlib
import json
import os
import socket
import sqlite3
from pathlib import Path
from typing import Any, Optional

from . import db_backend, sqlite_adapter, sqlite_lock
from .constants import PROJECT_ROOT
from .pg_core import ArchiverLockLost, PgConnectionCore, _now_utc
from .sqlite_adapter import (BusyError, PoolTimeout, RetryableError,
                             TriggerAbort)

#: Reported by every "explicit migration" entry point on SQLite.
INSTALLED_AT_CREATION = "installed at creation"

_COPY_BATCH = 500


def schema_dir():
    return Path(PROJECT_ROOT) / "scripts" / "sql" / "sqlite"


def migration_files():
    """``[(version, name, path)]`` in apply order."""
    found = []
    for path in sorted(schema_dir().glob("[0-9][0-9][0-9]_*.sql")):
        found.append((int(path.name[:3]), path.name, path))
    versions = [item[0] for item in found]
    if versions != list(range(1, len(versions) + 1)):
        raise RuntimeError(
            "[DB] SQLite migration files are not numbered 001..N without "
            f"gaps: {[item[1] for item in found]}")
    return found


def _file_checksum(path):
    # Normalised line endings: a checkout with CRLF must not look like a
    # different migration.
    data = Path(path).read_bytes().replace(b"\r\n", b"\n")
    return hashlib.sha256(data).hexdigest()


def _statements(script):
    """Split a native SQLite script into complete statements (trigger bodies
    included)."""
    buffer = ""
    for line in script.splitlines(keepends=True):
        buffer += line
        if sqlite3.complete_statement(buffer):
            statement = buffer.strip()
            buffer = ""
            if statement and statement != ";":
                yield statement
    if buffer.strip():
        raise RuntimeError(
            "[DB] SQLite schema script ends with an incomplete statement")


def schema_fingerprint(raw):
    """SHA-256 over every schema object definition, in a stable order."""
    digest = hashlib.sha256()
    rows = raw.execute(
        "SELECT type, name, tbl_name, sql FROM sqlite_master "
        "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name").fetchall()
    for row in rows:
        for value in row:
            text = "" if value is None else " ".join(str(value).split())
            encoded = text.encode("utf-8")
            digest.update(len(encoded).to_bytes(8, "big"))
            digest.update(encoded)
    return digest.hexdigest()


class SqliteConnectionCore(PgConnectionCore):
    """Pool, transactions, schema and archiver lock on SQLite."""

    BACKEND = db_backend.BACKEND_SQLITE

    def __init__(self, conninfo, *, init_schema=True, pool=None):
        self._sqlite_path = db_backend.sqlite_path(conninfo)
        self._file_lock: Optional[sqlite_lock.ArchiverLock] = None
        self._lock_token: Optional[str] = None
        super().__init__(conninfo, init_schema=init_schema, pool=pool)

    # ------------------------------------------------------------------
    # Connection / retry hooks
    # ------------------------------------------------------------------

    @staticmethod
    def _require_driver():
        sqlite_adapter.require_sqlite_version()

    def _open_pool(self, conninfo):
        directory = os.path.dirname(self._sqlite_path)
        if directory and not os.path.isdir(directory):
            raise RuntimeError(
                "[DB] The folder for the SQLite catalog does not exist: "
                f"{directory}")
        return sqlite_adapter.SqlitePool(self._sqlite_path, min_size=1,
                                         max_size=8)

    @staticmethod
    def _retryable_write_errors():
        return ((BusyError,), (PoolTimeout,))

    @staticmethod
    def _retryable_read_errors():
        return (RetryableError,)

    # ------------------------------------------------------------------
    # Schema: automatic creation + forward migrations
    # ------------------------------------------------------------------

    def _init_schema(self):
        files = migration_files()
        conn = self._pool.getconn()
        try:
            conn.autocommit = True
            raw = conn.raw
            has_table = raw.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' "
                "AND name='schema_migrations'").fetchone() is not None
            if not has_table:
                others = raw.execute(
                    "SELECT count(*) FROM sqlite_master "
                    "WHERE name NOT LIKE 'sqlite_%'").fetchone()[0]
                if others:
                    raise RuntimeError(
                        "[DB] Refusing to initialise "
                        f"{self._sqlite_path}: it already contains tables "
                        "but no schema_migrations record. It is not a "
                        "catalog created by this application.")
            applied = {}
            if has_table:
                applied = {
                    int(row[0]): (row[1], row[2], row[3])
                    for row in raw.execute(
                        "SELECT version, name, checksum, schema_fingerprint "
                        "FROM schema_migrations ORDER BY version").fetchall()}
            known = {version for version, _name, _path in files}
            unknown = sorted(set(applied) - known)
            if unknown:
                raise RuntimeError(
                    "[DB] This SQLite catalog was written by a NEWER version "
                    f"of the application (schema versions {unknown} are "
                    "unknown here). Refusing to open it.")
            for version, name, path in files:
                checksum = _file_checksum(path)
                if version in applied:
                    if applied[version][1] != checksum:
                        raise RuntimeError(
                            f"[DB] SQLite migration {name} was changed after "
                            "it was applied to this catalog (checksum "
                            "mismatch). Refusing to open it.")
                    continue
                if version != (max(applied) + 1 if applied else 1):
                    raise RuntimeError(
                        f"[DB] SQLite migration {name} is out of order")
                self._apply_migration(conn, version, name, path, checksum)
                applied[version] = (name, checksum, None)
            recorded = raw.execute(
                "SELECT schema_fingerprint FROM schema_migrations "
                "ORDER BY version DESC LIMIT 1").fetchone()[0]
            actual = schema_fingerprint(raw)
            if recorded != actual:
                raise RuntimeError(
                    "[DB] The SQLite catalog schema does not match the "
                    "schema this application installed (definition "
                    "fingerprint drift). Refusing to open it; restore a "
                    "verified backup.")
        finally:
            try:
                conn.autocommit = False
            finally:
                self._pool.putconn(conn)

    def _apply_migration(self, conn, version, name, path, checksum):
        script = Path(path).read_text(encoding="utf-8")
        with conn.transaction():
            raw = conn.raw
            try:
                for statement in _statements(script):
                    raw.execute(statement)
            except sqlite3.Error as exc:
                raise sqlite_adapter.map_error(exc) from exc
            fingerprint = schema_fingerprint(raw)
            if version == 1:
                self._seed_schema_authority(conn, checksum, fingerprint)
            conn.execute(
                """INSERT INTO schema_migrations
                   (version, name, checksum, schema_fingerprint, applied_at)
                   VALUES (%s, %s, %s, %s, %s)""",
                (version, name, checksum, fingerprint, _now_utc()))

    @staticmethod
    def _seed_schema_authority(conn, checksum, fingerprint):
        """The schema-authority rows the pre-write gates read.

        Same bootstrap idiom as PostgreSQL migrations 015/018: the insert is
        allowed once, into an empty table, while the creating transaction
        carries the checksum; afterwards triggers make the rows immutable.
        """
        from .pg_containers import (
            CONTAINER_FORMAT_SCHEMA_VERSION,
            REQUIRED_STORED_TAR_READER_CONTRACT_VERSION, STORED_TAR_DIALECT)
        from .pg_directory_catalog import (
            MANIFEST_DIRECTORY_CATALOG_SCHEMA_VERSION)
        now = _now_utc()
        short = fingerprint[:32]
        conn.set_txn_setting("lto.sqlite_schema_bootstrap_checksum", checksum)
        conn.execute(
            """INSERT INTO container_format_schema_metadata
               (singleton, schema_version, required_reader_contract_version,
                stored_tar_dialect, migration_checksum, schema_fingerprint,
                applied_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (True, CONTAINER_FORMAT_SCHEMA_VERSION,
             REQUIRED_STORED_TAR_READER_CONTRACT_VERSION, STORED_TAR_DIALECT,
             checksum, short, now))
        conn.execute(
            """INSERT INTO manifest_directory_catalog_schema_metadata
               (singleton, schema_version, migration_checksum,
                schema_fingerprint, applied_at)
               VALUES (%s, %s, %s, %s, %s)""",
            (True, MANIFEST_DIRECTORY_CATALOG_SCHEMA_VERSION, checksum,
             short, now))
        conn.execute(
            """INSERT INTO container_format_schema_authority
               (authority_version, schema_fingerprint, migration_checksums,
                contract_definition, applied_at)
               VALUES (%s, %s, %s, %s, %s)""",
            (2, short,
             json.dumps({"sqlite_001": checksum}, sort_keys=True),
             json.dumps({"backend": "sqlite", "schema_version": 1},
                        sort_keys=True), now))

    def _ensure_runtime_constraints(self):
        """Nothing to add: the SQLite schema is complete at creation."""

    # -- schema identity -------------------------------------------------

    @classmethod
    def sqlite_schema_version(cls):
        return len(migration_files())

    @classmethod
    def sqlite_schema_checksum(cls):
        """Checksum of the first (authority) schema file."""
        return _file_checksum(migration_files()[0][2])

    def sqlite_schema_status(self):
        """Read-only: applied migrations and whether the definitions drifted."""
        def operation(conn):
            rows = conn.execute(
                "SELECT version, name, checksum, schema_fingerprint, "
                "applied_at FROM schema_migrations ORDER BY version"
            ).fetchall()
            actual = schema_fingerprint(conn.raw)
            recorded = rows[-1]["schema_fingerprint"] if rows else None
            return {
                "backend": self.BACKEND,
                "database": self._sqlite_path,
                "schema_version": rows[-1]["version"] if rows else 0,
                "migrations": [dict(row) for row in rows],
                "recorded_fingerprint": recorded,
                "actual_fingerprint": actual,
                "drift": recorded != actual,
            }
        return self._run_read(operation, "sqlite schema status")

    def _installed_at_creation(self):
        return [f"{INSTALLED_AT_CREATION} (SQLite schema "
                f"v{self.sqlite_schema_version()})"]

    # -- "explicit migration" entry points: nothing to run ---------------

    def apply_directory_catalog_schema(self):
        return self._installed_at_creation()

    def apply_incremental_scan_schema(self, finalize=False):
        return self._installed_at_creation()

    def apply_container_format_schema(
            self, *, exception_session_id=None, expected_boundary=None,
            approval_id=None, approval_reason=None, staging_evidence=None,
            require_archiver_lock=False):
        if any(value is not None for value in (
                exception_session_id, expected_boundary, approval_id,
                approval_reason, staging_evidence)):
            # The legacy mixed-format exception rewrites pre-existing chunks;
            # a SQLite catalog has no pre-migration chunks to convert.
            raise db_backend.PostgresOnlyFeature("legacy_backfills")
        return self._installed_at_creation()

    def apply_stored_tar_plan_schema(self, *, require_archiver_lock=False):
        return self._installed_at_creation()

    def apply_manifest_directory_catalog_schema(
            self, *, require_archiver_lock=False):
        return self._installed_at_creation()

    def apply_container_format_schema_authority_v2(
            self, *, require_archiver_lock=False):
        return self._installed_at_creation()

    def _require_directory_catalog_schema(self):
        if not self.directory_catalog_schema_installed():
            raise RuntimeError(
                "[DB] Directory catalog tables are missing from this SQLite "
                "catalog; the file is damaged or was not created by this "
                "application.")

    # ------------------------------------------------------------------
    # Introspection hooks
    # ------------------------------------------------------------------

    @staticmethod
    def _index_exists_conn(conn, index_name):
        return conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='index' AND name=%s",
            (index_name,)).fetchone() is not None

    @staticmethod
    def _constraint_exists_conn(conn, table_name, constraint_name):
        row = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name=%s",
            (table_name,)).fetchone()
        return bool(row) and (
            f"CONSTRAINT {constraint_name} " in (row["sql"] or ""))

    @staticmethod
    def _view_exists_conn(conn, view_name):
        return conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='view' AND name=%s",
            (view_name,)).fetchone() is not None

    @staticmethod
    def _single_column_references_conn(conn, parent_table):
        found = []
        tables = [row["name"] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name").fetchall()]
        for table in tables:
            groups = {}
            for row in conn.raw.execute(
                    f'PRAGMA foreign_key_list("{table}")').fetchall():
                # (id, seq, table, from, to, on_update, on_delete, match)
                groups.setdefault(row[0], []).append(row)
            for rows in groups.values():
                if len(rows) != 1 or rows[0][2] != parent_table:
                    continue
                column, parent_column = rows[0][3], rows[0][4]
                if not column.replace("_", "").isalnum():
                    raise RuntimeError(
                        f"[DB] Unsafe column name: {column}")
                found.append((table, column, parent_column))
        return sorted(found)

    # ------------------------------------------------------------------
    # Bulk / statement hooks
    # ------------------------------------------------------------------

    @staticmethod
    def _set_local_setting_conn(conn, name, value):
        conn.set_txn_setting(name, value)

    @staticmethod
    def _create_stage_table_conn(conn, stage_table, source_table, col_sql):
        # SQLite has no ON COMMIT DROP: the temp table belongs to the pooled
        # connection, so clear the previous transaction's copy first.
        conn.execute(f"DROP TABLE IF EXISTS {stage_table}")
        conn.execute(
            f"CREATE TEMP TABLE {stage_table} AS "
            f"SELECT {col_sql} FROM {source_table} WHERE 0")

    @staticmethod
    def _copy_rows_conn(conn, table, columns, rows):
        sql = (f"INSERT INTO {table} ({', '.join(columns)}) VALUES ("
               + ", ".join(["%s"] * len(columns)) + ")")
        batch = []
        with conn.cursor() as cur:
            for row in rows:
                batch.append(tuple(row))
                if len(batch) >= _COPY_BATCH:
                    cur.executemany(sql, batch)
                    batch = []
            if batch:
                cur.executemany(sql, batch)

    @staticmethod
    def _upsert_from_stage_counts_conn(conn, table, stage_table, col_sql,
                                       conflict_column, conflict_action):
        # Called inside a write transaction (BEGIN IMMEDIATE), so the
        # pre-count and the upsert see the same rows.
        total = conn.execute(
            f"SELECT count(*) AS n FROM {stage_table}").fetchone()["n"]
        existing = conn.execute(
            f"""SELECT count(*) AS n FROM {stage_table} s
                JOIN {table} t ON t.{conflict_column} = s.{conflict_column}"""
        ).fetchone()["n"]
        conn.execute(
            f"""INSERT INTO {table} ({col_sql})
                SELECT {col_sql} FROM {stage_table} WHERE true
                ON CONFLICT ({conflict_column}) {conflict_action}""")
        inserted = int(total) - int(existing)
        updates = conflict_action.lstrip().upper().startswith("DO UPDATE")
        return inserted, (int(total) if updates else inserted)

    @staticmethod
    def _insert_local_session_conn(conn, session_label, source_dir,
                                   total_chunks, backup_mode, created_at):
        existing = conn.execute(
            "SELECT session_id FROM local_sessions WHERE session_label=%s "
            "FOR UPDATE", (session_label,)).fetchone()
        if existing is not None:
            return existing["session_id"], False
        row = conn.execute(
            """INSERT INTO local_sessions
               (session_label, source_dir, total_chunks, backup_mode,
                created_at, status)
               VALUES (%s, %s, %s, %s, %s, 'active')
               RETURNING session_id""",
            (session_label, source_dir, total_chunks, backup_mode,
             created_at)).fetchone()
        return row["session_id"], True

    @staticmethod
    def _vacuum_analyze_conn(conn):
        conn.execute("VACUUM")
        conn.execute("ANALYZE")

    # -- the two plpgsql functions called from Python ----------------------

    @staticmethod
    def _sql_assign_new_chunk_format_conn(
            conn, session_id, chunk_index, stored_tar_write_enabled,
            schema_version, reader_contract_version):
        """``lto_assign_new_chunk_format`` (migration 015), same rules."""
        meta = conn.execute(
            """SELECT schema_version, required_reader_contract_version
               FROM container_format_schema_metadata WHERE singleton"""
        ).fetchone()
        installed = meta["schema_version"] if meta else None
        if installed is None or installed != schema_version:
            raise TriggerAbort(
                "container-format schema missing or version mismatch "
                f"(installed {installed}, caller {schema_version})")
        session = conn.execute(
            "SELECT default_packaging_format FROM remote_sessions "
            "WHERE session_id=%s", (session_id,)).fetchone()
        if session is None or session["default_packaging_format"] is None:
            raise TriggerAbort(
                f"remote session {session_id} does not exist")
        session_default = session["default_packaging_format"]
        boundary = conn.execute(
            "SELECT first_stored_tar_chunk_index "
            "FROM remote_packaging_boundaries WHERE session_id=%s",
            (session_id,)).fetchone()
        boundary_index = (
            boundary["first_stored_tar_chunk_index"] if boundary else None)
        if boundary_index is not None:
            result = ("stored_tar" if chunk_index >= boundary_index
                      else "zip")
        else:
            result = session_default
        if result != session_default:
            raise TriggerAbort(
                "session default/boundary conflict for session "
                f"{session_id}, chunk {chunk_index}")
        if result == "stored_tar":
            if not stored_tar_write_enabled:
                raise TriggerAbort(
                    "Stored TAR assignment is disabled for session "
                    f"{session_id}, chunk {chunk_index}")
            required = meta["required_reader_contract_version"]
            if (reader_contract_version is None
                    or reader_contract_version != required):
                raise TriggerAbort(
                    "Stored TAR reader contract mismatch (required "
                    f"{required}, caller {reader_contract_version})")
        return result

    def _sql_transition_chunk_plan_source_conn(
            self, conn, session_id, chunk_index, plan_manifest_artifact_id,
            plan4_gate_id, plan4_evidence_id, approval_identity):
        """``lto_transition_chunk_plan_source`` (migration 018), same rules.

        The guards themselves stay in the schema: the UPDATE below is checked
        by ``trg_remote_chunks_plan_source_guard__upd`` and the audit row is
        written by ``..__audit``; this only supplies the three identities as
        transaction-scoped settings, exactly as the plpgsql function does.
        """
        if not all(str(value or "").strip() for value in (
                plan4_gate_id, plan4_evidence_id, approval_identity)):
            raise TriggerAbort(
                "Plan 4 gate, evidence, and approval IDs are required")
        existing = conn.execute(
            """SELECT plan_source, plan_manifest_artifact_id
               FROM remote_chunks
               WHERE session_id=%s AND chunk_index=%s FOR UPDATE""",
            (session_id, chunk_index)).fetchone()
        if existing is None:
            raise TriggerAbort("remote chunk does not exist")
        if existing["plan_source"] == "manifest":
            audit = conn.execute(
                """SELECT chunk_plan_transition_id
                   FROM remote_chunk_plan_source_transitions
                   WHERE session_id=%s AND chunk_index=%s
                     AND new_plan_manifest_artifact_id=%s
                     AND plan4_gate_id=%s AND plan4_evidence_id=%s
                     AND approval_identity=%s""",
                (session_id, chunk_index, plan_manifest_artifact_id,
                 plan4_gate_id, plan4_evidence_id, approval_identity)
            ).fetchone()
            if audit is None:
                raise TriggerAbort(
                    "manifest chunk authority differs from requested audited "
                    "transition")
            return audit["chunk_plan_transition_id"]
        self._set_local_setting_conn(
            conn, "lto.chunk_plan_transition_gate_id", plan4_gate_id)
        self._set_local_setting_conn(
            conn, "lto.chunk_plan_transition_evidence_id", plan4_evidence_id)
        self._set_local_setting_conn(
            conn, "lto.chunk_plan_transition_approval_identity",
            approval_identity)
        conn.execute(
            """UPDATE remote_chunks
               SET plan_source='manifest', plan_manifest_artifact_id=%s
               WHERE session_id=%s AND chunk_index=%s""",
            (plan_manifest_artifact_id, session_id, chunk_index))
        audit = conn.execute(
            """SELECT chunk_plan_transition_id
               FROM remote_chunk_plan_source_transitions
               WHERE session_id=%s AND chunk_index=%s""",
            (session_id, chunk_index)).fetchone()
        if audit is None:
            raise TriggerAbort(
                "the audited plan-source transition wrote no audit row")
        return audit["chunk_plan_transition_id"]

    # ------------------------------------------------------------------
    # Archiver lock: OS file lock (authority) + owner token row
    # ------------------------------------------------------------------

    def _archiver_lock_held_conn(self, conn):
        return bool(sqlite_lock.probe_held(self._sqlite_path))

    def _owns_archiver_lock_conn(self, conn):
        if conn is None or conn is not self._lock_conn:
            return False
        try:
            self.verify_archiver_lock()
        except Exception:
            return False
        return True

    def acquire_archiver_lock(self):
        """Take the cross-process single-writer lock for a tape-write run.

        Raises ``RuntimeError`` when another archiver instance holds it or
        when the lock cannot be taken for any reason (fail closed).
        """
        if self._lock_conn is not None:
            return
        lock = sqlite_lock.ArchiverLock(self._sqlite_path)
        try:
            token = lock.acquire()
        except sqlite_lock.ArchiverLockHeld as exc:
            raise RuntimeError(
                "[LOCK] Another archiver instance already holds the "
                "tape-writer lock. Only one tape-write run may be active at "
                "a time; finish or close the other instance and retry. "
                f"({exc})") from exc
        except sqlite_lock.ArchiverLockError as exc:
            raise RuntimeError(str(exc)) from exc
        conn = None
        try:
            conn = self._pool.getconn()
            conn.autocommit = True
            with conn.transaction():
                conn.execute("DELETE FROM archiver_lock")
                conn.execute(
                    """INSERT INTO archiver_lock
                       (singleton, owner_token, pid, host, acquired_at)
                       VALUES (1, %s, %s, %s, %s)""",
                    (token, os.getpid(), socket.gethostname(), _now_utc()))
        except BaseException:
            lock.release()
            if conn is not None:
                try:
                    conn.autocommit = False
                    self._pool.putconn(conn)
                except Exception:
                    pass
            raise
        self._file_lock = lock
        self._lock_token = token
        self._lock_conn = conn
        self._lock_backend_pid = os.getpid()
        self._archiver_lock_expected = True

    def verify_archiver_lock(self):
        """Prove THIS process still owns the archiver lock, or raise.

        Two independent proofs, both required: the OS lock handle is still
        the one that locked ``<database>.archiver.lock`` (and the lock file
        still names our token), and the ``archiver_lock`` row still carries
        our token. Fails closed on every doubt and never re-acquires.
        """
        lock = self._file_lock
        conn = self._lock_conn
        token = self._lock_token
        if lock is None or conn is None or token is None:
            raise ArchiverLockLost(
                "[LOCK] The archiver lock is not held by this process. "
                "Refusing to start a tape write without it.")
        try:
            lock.verify(token)
            row = conn.execute(
                "SELECT owner_token, pid FROM archiver_lock "
                "WHERE singleton=1").fetchone()
        except Exception as exc:
            raise ArchiverLockLost(
                "[LOCK] The archiver lock can no longer be proven "
                f"({type(exc).__name__}: {exc}). Another archiver or a tape "
                "reset may now hold it. Stopping: this run must not write "
                "to tape again.") from exc
        if row is None or row["owner_token"] != token:
            raise ArchiverLockLost(
                "[LOCK] The archiver lock record no longer names this "
                "process (expected our owner token, found "
                f"{'no record' if row is None else 'another owner'}). "
                "Stopping: this run must not write to tape again.")
        return True

    def release_archiver_lock(self):
        conn = self._lock_conn
        lock = self._file_lock
        token = self._lock_token
        self._lock_conn = None
        self._file_lock = None
        self._lock_token = None
        self._lock_backend_pid = None
        self._archiver_lock_expected = False
        if conn is not None:
            try:
                conn.execute(
                    "DELETE FROM archiver_lock WHERE owner_token=%s",
                    (token,))
            except Exception:
                pass    # a stale row is harmless: the OS lock is authority
            try:
                conn.autocommit = False
                if self._pool is not None:
                    self._pool.putconn(conn)
            except Exception:
                pass
        if lock is not None:
            lock.release()


class SqliteSchemaReports:
    """Schema reports for a catalog whose schema is installed at creation.

    PostgreSQL proves each explicit migration by introspecting ``pg_catalog``
    and comparing a plpgsql fingerprint. Here there is one schema, one
    recorded fingerprint, and the reports answer the same questions from
    ``sqlite_master``: are the objects there, and are their definitions the
    ones this application installed.
    """

    def _sqlite_structure_issues_conn(self, conn, tables=(), columns=None,
                                      indexes=(), triggers=(),
                                      constraints=()):
        issues = []
        for table in tables:
            if not self._table_exists_conn(conn, table):
                issues.append(f"missing table: {table}")
        for table, names in (columns or {}).items():
            if not self._table_exists_conn(conn, table):
                if table not in tables:
                    issues.append(f"missing table: {table}")
                continue
            for name in names:
                if not self._column_exists_conn(conn, table, name):
                    issues.append(f"missing column: {table}.{name}")
        for index in indexes:
            if not self._index_exists_conn(conn, index):
                issues.append(f"missing index: {index}")
        existing_triggers = {row["name"] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger'"
        ).fetchall()}
        for trigger in triggers:
            if trigger not in existing_triggers:
                issues.append(f"missing trigger: {trigger}")
        all_sql = "\n".join(
            row["sql"] or "" for row in conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='table'"
            ).fetchall())
        for constraint in constraints:
            if f"CONSTRAINT {constraint} " not in all_sql:
                issues.append(f"missing constraint: {constraint}")
        recorded = conn.execute(
            "SELECT schema_fingerprint FROM schema_migrations "
            "ORDER BY version DESC LIMIT 1").fetchone()
        actual = schema_fingerprint(conn.raw)
        if recorded is None:
            issues.append("schema_migrations has no applied migration")
        elif recorded["schema_fingerprint"] != actual:
            issues.append("SQLite catalog-definition fingerprint drift")
        return issues, actual

    def _container_format_schema_report_conn(self, conn):
        from .pg_containers import (
            CONTAINER_FORMAT_SCHEMA_VERSION,
            REQUIRED_STORED_TAR_READER_CONTRACT_VERSION, STORED_TAR_DIALECT)
        checksum = self.sqlite_schema_checksum()
        issues, _actual = self._sqlite_structure_issues_conn(
            conn, tables=self.CONTAINER_FORMAT_TABLES,
            columns=self.CONTAINER_FORMAT_COLUMNS,
            indexes=self.CONTAINER_FORMAT_INDEXES,
            triggers=self.CONTAINER_FORMAT_TRIGGERS,
            constraints=self.CONTAINER_FORMAT_CONSTRAINTS)
        report = {
            "database": self._database_name_conn(conn),
            "expected_schema_version": CONTAINER_FORMAT_SCHEMA_VERSION,
            "expected_reader_contract_version":
                REQUIRED_STORED_TAR_READER_CONTRACT_VERSION,
            "expected_dialect": STORED_TAR_DIALECT,
            "expected_migration_checksum": checksum,
            "metadata": None,
            "installation_state": "absent",
            "optional_007_state": "installed",
            "format_counts": {},
            "prefix_evidence_counts": {"corroborated": 0, "status_only": 0},
            "null_chunk_formats": None,
            "issues": issues,
        }
        if self._table_exists_conn(conn, "container_format_schema_metadata"):
            rows = conn.execute(
                "SELECT * FROM container_format_schema_metadata").fetchall()
            if len(rows) == 1:
                metadata = dict(rows[0])
                report["metadata"] = metadata
                report["installation_state"] = "installed"
                expected = {
                    "schema_version": CONTAINER_FORMAT_SCHEMA_VERSION,
                    "required_reader_contract_version":
                        REQUIRED_STORED_TAR_READER_CONTRACT_VERSION,
                    "stored_tar_dialect": STORED_TAR_DIALECT,
                    "migration_checksum": checksum,
                }
                for key, value in expected.items():
                    if metadata.get(key) != value:
                        issues.append(
                            f"metadata drift: {key} is "
                            f"{metadata.get(key)!r}, expected {value!r}")
            else:
                report["installation_state"] = "partial"
                issues.append(
                    "container format metadata does not contain exactly one "
                    "authority row")
        if self._table_exists_conn(conn, "remote_chunks"):
            report["format_counts"] = {
                row["packaging_format"]: int(row["n"])
                for row in conn.execute(
                    """SELECT packaging_format, count(*) AS n
                       FROM remote_chunks GROUP BY packaging_format"""
                ).fetchall()}
            report["null_chunk_formats"] = 0
        report["ready"] = not issues
        return report

    def _container_format_schema_authority_v2_report_conn(self, conn):
        report = {
            "database": self._database_name_conn(conn),
            "expected_migration_checksum": self.sqlite_schema_checksum(),
            "installed": False,
            "ready": False,
            "row": None,
            "actual_schema_fingerprint": None,
            "issues": [],
        }
        issues = report["issues"]
        if not self._table_exists_conn(
                conn, "container_format_schema_authority"):
            issues.append("v2 authority table is absent")
            return report
        row = conn.execute(
            """SELECT authority_id, authority_version, schema_fingerprint,
                      migration_checksums, applied_at
               FROM container_format_schema_authority
               WHERE authority_version=2""").fetchone()
        if row is None:
            issues.append("v2 authority row (version 2) is absent")
            return report
        report["installed"] = True
        report["row"] = dict(row)
        structure, actual = self._sqlite_structure_issues_conn(conn)
        report["actual_schema_fingerprint"] = actual
        issues.extend(structure)
        report["ready"] = not issues
        return report

    def _manifest_directory_catalog_schema_report_conn(self, conn):
        from .pg_directory_catalog import (
            MANIFEST_DIRECTORY_CATALOG_SCHEMA_VERSION)
        checksum = self.sqlite_schema_checksum()
        report = {
            "database": self._database_name_conn(conn),
            "expected_schema_version":
                MANIFEST_DIRECTORY_CATALOG_SCHEMA_VERSION,
            "expected_migration_checksum": checksum,
            "metadata": None,
            "installation_state": "absent",
            "optional_007_state": "installed",
            "optional_012_state": "absent",
            "schema_issues": [],
            "prerequisite_issues": [],
            "authority_issues": [],
            "issues": [],
            "apply_blocking": [],
            "schema_ready": False,
            "ready": False,
            "actual_schema_fingerprint": None,
            "plan_source_counts": {},
            "manifest_chunk_count": 0,
            "invalid_manifest_chunk_count": 0,
            "shared_plan_artifact_reference_count": 0,
            "invalid_chunk_plan_source_transition_count": 0,
            "inconsistent_transition_boundary_count": 0,
            "inconsistent_effective_plan_source_count": 0,
            "duplicate_active_transition_boundary_count": 0,
        }
        schema_issues, actual = self._sqlite_structure_issues_conn(
            conn, tables=self.MANIFEST_DIRECTORY_CATALOG_TABLES,
            columns={
                "remote_chunks":
                    self.MANIFEST_DIRECTORY_CATALOG_CHUNK_COLUMNS,
                "remote_session_plan_transitions":
                    self.MANIFEST_DIRECTORY_CATALOG_TRANSITION_COLUMNS,
                "remote_chunk_plan_source_transitions":
                    self.MANIFEST_DIRECTORY_CATALOG_CHUNK_TRANSITION_COLUMNS,
            },
            indexes=self.MANIFEST_DIRECTORY_CATALOG_INDEXES,
            triggers=self.MANIFEST_DIRECTORY_CATALOG_TRIGGERS)
        report["actual_schema_fingerprint"] = actual
        if not self._view_exists_conn(
                conn, self.MANIFEST_DIRECTORY_CATALOG_VIEW):
            schema_issues.append(
                f"missing view: {self.MANIFEST_DIRECTORY_CATALOG_VIEW}")
        report["schema_issues"] = schema_issues
        if self._table_exists_conn(
                conn, "manifest_directory_catalog_schema_metadata"):
            rows = conn.execute(
                "SELECT * FROM manifest_directory_catalog_schema_metadata"
            ).fetchall()
            if len(rows) == 1:
                metadata = dict(rows[0])
                report["metadata"] = metadata
                report["installation_state"] = "installed"
                expected = {
                    "schema_version":
                        MANIFEST_DIRECTORY_CATALOG_SCHEMA_VERSION,
                    "migration_checksum": checksum,
                }
                for key, value in expected.items():
                    if metadata.get(key) != value:
                        schema_issues.append(
                            f"metadata drift: {key} is "
                            f"{metadata.get(key)!r}, expected {value!r}")
            else:
                report["installation_state"] = "partial"
                schema_issues.append(
                    "manifest directory catalog metadata does not contain "
                    "exactly one authority row")
        if (report["installation_state"] == "installed"
                and not any(issue.startswith(("missing table:",
                                              "missing column:"))
                            for issue in schema_issues)):
            for key, value in self._manifest_authority_report_conn(
                    conn).items():
                report[key] = value
        report["schema_ready"] = bool(
            report["installation_state"] == "installed"
            and not schema_issues)
        report["issues"] = (list(schema_issues)
                            + list(report["authority_issues"]))
        report["ready"] = bool(
            report["schema_ready"] and not report["authority_issues"])
        if report["installation_state"] == "partial":
            report["apply_blocking"].append(
                "the manifest directory catalog schema is damaged")
        elif not report["ready"]:
            report["apply_blocking"].extend(report["issues"])
        return report
