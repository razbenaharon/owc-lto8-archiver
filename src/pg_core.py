"""Connection pool, transaction retry policy, schema init, advisory lock.

Foundation shared by the method-group mixins (:mod:`src.pg_catalog`,
:mod:`src.pg_sessions`, :mod:`src.pg_tapes`) that together form
:class:`src.pg_db.PgDatabaseManager`.
"""
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

    @staticmethod
    def _table_exists_conn(conn, table_name):
        return conn.execute(
            """SELECT 1
               FROM information_schema.tables
               WHERE table_schema='public' AND table_name=%s""",
            (table_name,),
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
