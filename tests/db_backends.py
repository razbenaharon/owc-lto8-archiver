"""Run one behavioural catalog suite against every supported backend.

``@each_backend`` turns a ``unittest.TestCase`` into one generated class per
backend, named ``<Class>_sqlite`` and ``<Class>_postgresql``:

* **SQLite** always runs. It needs no server: every test database is a file
  in a private temporary directory, removed afterwards. Nothing is ever
  created in the repository root.
* **PostgreSQL** runs only when ``LTO_TEST_PG_DSN`` names a disposable server,
  through :mod:`pg_test_guard` with all of its fail-closed rules unchanged.
  Unset means "skip"; an unsafe DSN still raises.

Inside a decorated class use ``self.BACKEND`` and :func:`create_test_db`::

    @each_backend
    class TapeTests(unittest.TestCase):
        def setUp(self):
            self.tdb = create_test_db(self.BACKEND, "tapes")
            self.db = self.tdb.manager()

        def tearDown(self):
            self.tdb.drop()

The handle (:class:`BackendTestDb`) gives raw-SQL helpers that work on both
engines (``query`` / ``exec`` / ``connect``) and a backend-neutral ``errors``
namespace. A test that is inherently PostgreSQL-specific stays in a plain
class guarded by ``pg_test_guard`` and says so in its docstring, or is marked
with :func:`postgresql_only` / :func:`sqlite_only` inside a decorated class.
"""
import os
import shutil
import sys
import tempfile
import unittest
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pg_test_guard                                    # noqa: E402

SQLITE = "sqlite"
POSTGRESQL = "postgresql"
BACKENDS = (SQLITE, POSTGRESQL)

__all__ = [
    "BACKENDS", "POSTGRESQL", "SQLITE", "BackendTestDb", "create_test_db",
    "each_backend", "postgres_enabled", "postgresql_only", "sqlite_only",
]


def postgres_enabled():
    """True when a validated disposable PostgreSQL server is reachable.

    Delegates to the fail-closed guard: an UNSAFE target raises here.
    """
    try:
        import psycopg  # noqa: F401
    except ImportError:
        return False
    return pg_test_guard.pg_available()


def _sqlite_errors():
    from src import sqlite_adapter as a
    return SimpleNamespace(
        Error=a.SqliteError,
        IntegrityError=a.IntegrityError,
        UniqueViolation=a.UniqueViolation,
        ForeignKeyViolation=a.ForeignKeyViolation,
        NotNullViolation=a.NotNullViolation,
        CheckViolation=a.CheckViolation,
        # RAISE(ABORT, ...) in a trigger: the plpgsql RAISE EXCEPTION twin.
        RaiseException=a.TriggerAbort,
        # Any refusal by a schema guard (trigger or constraint).
        GuardViolation=(a.IntegrityError,),
        # A guard PostgreSQL raises WITH ERRCODE check_violation.
        CheckGuard=(a.TriggerAbort, a.CheckViolation),
        ReadOnly=(a.ReadOnlyError,),
    )


def _postgres_errors():
    import psycopg
    from psycopg import errors
    return SimpleNamespace(
        Error=psycopg.Error,
        IntegrityError=errors.IntegrityError,
        UniqueViolation=errors.UniqueViolation,
        ForeignKeyViolation=errors.ForeignKeyViolation,
        NotNullViolation=errors.NotNullViolation,
        CheckViolation=errors.CheckViolation,
        RaiseException=errors.RaiseException,
        GuardViolation=(errors.RaiseException, errors.IntegrityError),
        CheckGuard=(errors.CheckViolation,),
        ReadOnly=(errors.ReadOnlySqlTransaction,),
    )


class BackendTestDb:
    """One throwaway catalog database on one backend."""

    def __init__(self, backend, tag):
        if backend not in BACKENDS:
            raise ValueError(f"unknown backend: {backend!r}")
        self.backend = backend
        self._managers = []
        self._tmpdir = None
        if backend == SQLITE:
            self._tmpdir = tempfile.mkdtemp(prefix="lto_sqlite_test_")
            clean = "".join(ch if ch.isalnum() else "_" for ch in str(tag))
            self.path = os.path.join(self._tmpdir, f"{clean[:24]}.sqlite3")
            self.name = self.path
            self.conninfo = "sqlite:///" + self.path.replace("\\", "/")
            self.errors = _sqlite_errors()
        else:
            self.path = None
            self.name, self.conninfo = pg_test_guard.create_test_database(tag)
            self.errors = _postgres_errors()

    is_sqlite = property(lambda self: self.backend == SQLITE)
    is_postgresql = property(lambda self: self.backend == POSTGRESQL)

    # -- managers -------------------------------------------------------
    def manager(self, *, migrations="all", **kwargs):
        """A catalog manager on this database.

        ``migrations``: ``"all"`` brings a PostgreSQL database to the fully
        migrated schema (the only schema SQLite has); ``"startup"`` leaves it
        with what startup applies on its own. SQLite ignores the argument.
        """
        if self.is_sqlite:
            from src.sqlite_db import SqliteDatabaseManager
            db = SqliteDatabaseManager(self.conninfo, **kwargs)
        else:
            from src.pg_db import PgDatabaseManager
            db = PgDatabaseManager(self.conninfo, **kwargs)
            if migrations == "all" and kwargs.get("init_schema", True):
                self.apply_all_migrations(db)
        self._managers.append(db)
        return db

    @staticmethod
    def apply_all_migrations(db):
        """Every explicit migration, in order (a no-op report on SQLite)."""
        if not db.directory_catalog_schema_installed():
            db.apply_directory_catalog_schema()
        if not db.incremental_scan_schema_finalized():
            db.apply_incremental_scan_schema(finalize=True)
        if not db.container_format_schema_report()["ready"]:
            db.apply_container_format_schema()
        if db.stored_tar_plan_schema_report()[
                "installation_state"] != "installed":
            db.apply_stored_tar_plan_schema()
        if db.manifest_directory_catalog_schema_report()[
                "installation_state"] != "installed":
            db.apply_manifest_directory_catalog_schema()
        if not db.container_format_schema_authority_v2_report()["installed"]:
            db.apply_container_format_schema_authority_v2()
        return db

    # -- raw SQL ---------------------------------------------------------
    def connect(self, *, autocommit=True):
        """A dict-row connection (PostgreSQL SQL dialect on both backends)."""
        from src import db_backend
        return db_backend.connect(self.conninfo, autocommit=autocommit)

    def query(self, sql, params=()):
        conn = self.connect()
        try:
            return conn.execute(sql, params).fetchall()
        finally:
            conn.close()

    def exec(self, sql, params=()):
        conn = self.connect()
        try:
            conn.execute(sql, params)
        finally:
            conn.close()

    def run(self, sql, params=()):
        """Execute one statement; rows when it returns any, else ``[]``."""
        conn = self.connect()
        try:
            cur = conn.execute(sql, params)
            return cur.fetchall() if cur.description else []
        finally:
            conn.close()

    def has_column(self, table, column):
        from src import db_backend
        conn = self.connect()
        try:
            return db_backend.column_exists(conn, table, column)
        finally:
            conn.close()

    def insert_remote_chunk(self, session_id, chunk_index, status="pending"):
        """Insert a chunk row directly, on whatever schema is installed
        (the packaging columns exist only after migration 015)."""
        if self.has_column("remote_chunks", "packaging_format"):
            self.run(
                """INSERT INTO remote_chunks (session_id,chunk_index,status,
                       updated_at,packaging_format,packaging_assigned_at)
                   VALUES (%s,%s,%s,now(),'zip',now())""",
                (session_id, chunk_index, status))
        else:
            self.run(
                """INSERT INTO remote_chunks (session_id,chunk_index,status,
                       updated_at) VALUES (%s,%s,%s,now())""",
                (session_id, chunk_index, status))

    def scalar(self, sql, params=()):
        rows = self.query(sql, params)
        if not rows:
            return None
        return rows[0][next(iter(rows[0].keys()))]

    # -- teardown --------------------------------------------------------
    def drop(self):
        for db in self._managers:
            try:
                db.close()
            except Exception:
                pass
        self._managers = []
        if self.is_sqlite:
            if self._tmpdir:
                shutil.rmtree(self._tmpdir, ignore_errors=True)
                self._tmpdir = None
        else:
            pg_test_guard.drop_test_database(self.name)


def create_test_db(backend, tag="db"):
    return BackendTestDb(backend, tag)


def _backend_gate(wanted, reason):
    def decorator(function):
        def wrapper(self, *args, **kwargs):
            if getattr(self, "BACKEND", None) != wanted:
                raise unittest.SkipTest(reason)
            return function(self, *args, **kwargs)
        wrapper.__name__ = function.__name__
        wrapper.__doc__ = function.__doc__
        wrapper.__dict__.update(getattr(function, "__dict__", {}))
        return wrapper
    return decorator


def postgresql_only(reason):
    """Mark one test in an ``@each_backend`` class as PostgreSQL-specific."""
    return _backend_gate(POSTGRESQL, "PostgreSQL-only: " + reason)


def sqlite_only(reason):
    """Mark one test in an ``@each_backend`` class as SQLite-specific."""
    return _backend_gate(SQLITE, "SQLite-only: " + reason)


def each_backend(cls):
    """Generate ``<cls>_sqlite`` and ``<cls>_postgresql`` in ``cls``'s module.

    The decorated class itself is kept as an uncollected base
    (``__test__ = False``), so it can still be subclassed or referenced.
    """
    module = sys.modules[cls.__module__]
    pg_ok = postgres_enabled()
    for backend in BACKENDS:
        name = f"{cls.__name__}_{backend}"
        variant = type(name, (cls,), {
            "BACKEND": backend,
            "__test__": True,
            "__module__": cls.__module__,
            "__qualname__": name,
            "__doc__": cls.__doc__,
        })
        if backend == POSTGRESQL:
            variant = unittest.skipUnless(
                pg_ok, pg_test_guard.SKIP_REASON)(variant)
        setattr(module, name, variant)
    cls.__test__ = False
    cls.BACKEND = None
    return cls
