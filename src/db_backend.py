"""Backend-neutral seam for everything that opens its OWN database connection.

The catalog manager (:mod:`src.pg_db` / :mod:`src.sqlite_db`) owns a pool. A
handful of modules deliberately do not use it -- the GUI inspector, stale
session reconcile, tape reset, the read-only validation reports -- and open a
short-lived connection themselves. They all go through :func:`connect` so the
same code runs on PostgreSQL and on SQLite.

This module also owns the single list of features that are PostgreSQL-only in
v1.0 (:data:`POSTGRESQL_ONLY_FEATURES`) and the one refusal used for them
(:class:`PostgresOnlyFeature`), so the CLI, the GUI and the documentation all
cite the same place.

Importing this module never imports psycopg: a SQLite-only install works
without it.
"""
from __future__ import annotations

import os

from .cli_errors import OperationalError

BACKEND_SQLITE = "sqlite"
BACKEND_POSTGRESQL = "postgresql"

#: Feature key -> operator-facing name. PostgreSQL-only in v1.0. Every entry
#: refuses on SQLite through :func:`require_postgresql`, before doing any work.
POSTGRESQL_ONLY_FEATURES = {
    "explicit_migrations": (
        "explicit schema migration commands (inspect_db.py --apply-*-schema, "
        "--create-migrated-db, --apply-directory-catalog-schema)"),
    "legacy_backfills": (
        "legacy data backfill / transition / rehearsal commands "
        "(directory-catalog backfill, session-37 transition, frontier "
        "bootstrap, membership backfill, plan-1 rehearsal)"),
    "database_compare": (
        "database comparison and migrated-database validation "
        "(inspect_db.py --compare-db, --validate-migrated-db)"),
    "schema_audit": "schema provenance audit (src/schema_audit.py)",
    "sealed_batches": (
        "sealed tape-write batches and their observation mode "
        "([PERFORMANCE] tape_write_batch_mode)"),
    "small_file_manifest_maintenance": (
        "small-file manifest export / validate / prune "
        "(src/local_manifest_archive.py)"),
    "pg_dump_backup": "pg_dump based backup and restore-verification",
}


class PostgresOnlyFeature(OperationalError):
    """A PostgreSQL-only feature was requested on the SQLite backend.

    An expected, operator-facing refusal (never a traceback): it subclasses
    :class:`src.cli_errors.OperationalError`, which is a ``RuntimeError``.
    """

    def __init__(self, feature):
        self.feature = feature
        description = POSTGRESQL_ONLY_FEATURES.get(feature, feature)
        super().__init__(
            "[DB] Not available on the SQLite backend: " + description + ". "
            "This feature needs PostgreSQL ([DATABASE] backend = postgresql). "
            "Nothing was changed.")


def backend_of(target):
    """``'sqlite'`` or ``'postgresql'`` for a DSN, config, manager or
    connection."""
    if target is None:
        return BACKEND_POSTGRESQL
    if isinstance(target, (str, os.PathLike)):
        text = os.fspath(target).strip().lower()
        if text.startswith("sqlite:"):
            return BACKEND_SQLITE
        return BACKEND_POSTGRESQL
    marker = getattr(target, "BACKEND", None)
    if marker in (BACKEND_SQLITE, BACKEND_POSTGRESQL):
        return marker
    if type(target).__module__.endswith("sqlite_adapter"):
        return BACKEND_SQLITE
    explicit = getattr(target, "db_backend", None)
    if explicit in (BACKEND_SQLITE, BACKEND_POSTGRESQL):
        return explicit
    dsn = getattr(target, "db_dsn", None) or getattr(target, "db_path", None)
    if isinstance(dsn, str):
        return backend_of(dsn)
    return BACKEND_POSTGRESQL


def is_sqlite(target):
    return backend_of(target) == BACKEND_SQLITE


def require_postgresql(target, feature):
    """Raise :class:`PostgresOnlyFeature` when ``target`` is SQLite."""
    if feature not in POSTGRESQL_ONLY_FEATURES:
        raise KeyError(f"unknown PostgreSQL-only feature key: {feature!r}")
    if is_sqlite(target):
        raise PostgresOnlyFeature(feature)


def sqlite_path(dsn):
    from .sqlite_adapter import path_from_dsn
    return os.path.abspath(path_from_dsn(dsn))


def connect(dsn, *, autocommit=True, read_only=False):
    """Open one dict-row connection to the catalog behind ``dsn``.

    ``read_only`` is enforced by the engine: SQLite opens the file
    ``mode=ro`` (a missing file is an error, never created); PostgreSQL sets
    ``default_transaction_read_only`` for the session.
    """
    if is_sqlite(dsn):
        from . import sqlite_adapter
        return sqlite_adapter.connect(
            dsn, autocommit=autocommit, read_only=read_only)
    from .pg_bulk import require_psycopg
    require_psycopg()
    import psycopg
    from psycopg.rows import dict_row
    conn = psycopg.connect(dsn, autocommit=autocommit, row_factory=dict_row)
    if read_only:
        try:
            conn.execute("SET default_transaction_read_only = on")
            if not autocommit:
                conn.commit()
        except Exception:
            conn.close()
            raise
    return conn


def set_session_read_only(conn):
    """Make an already-open connection refuse writes for its lifetime."""
    if is_sqlite(conn):
        conn.raw.execute("PRAGMA query_only=ON")
    else:
        conn.execute("SET default_transaction_read_only = on")


def table_exists(conn, table_name):
    if is_sqlite(conn):
        return conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type IN ('table','view') "
            "AND name=%s", (table_name,)).fetchone() is not None
    return conn.execute(
        """SELECT 1
           FROM information_schema.tables
           WHERE table_schema='public' AND table_name=%s""",
        (table_name,),
    ).fetchone() is not None


def column_exists(conn, table_name, column_name):
    if is_sqlite(conn):
        return conn.execute(
            "SELECT 1 FROM pragma_table_info(%s) WHERE name=%s",
            (table_name, column_name)).fetchone() is not None
    return conn.execute(
        """SELECT 1
           FROM information_schema.columns
           WHERE table_schema='public'
             AND table_name=%s AND column_name=%s""",
        (table_name, column_name),
    ).fetchone() is not None


def database_name(conn):
    """Human-readable identity of the database a connection is on."""
    if is_sqlite(conn):
        return os.path.basename(conn.path)
    return conn.execute(
        "SELECT current_database() AS db").fetchone()["db"]


#: PostgreSQL session-level advisory lock key ('LTO8').
ARCHIVER_LOCK_KEY = 0x4C544F38


def pg_archiver_lock_holders(conn):
    """How many PostgreSQL sessions hold the archiver advisory lock."""
    # A bigint advisory key appears in pg_locks split across classid (high 32
    # bits) / objid (low 32), with objsubid=1. Constraining all three avoids
    # matching an unrelated advisory lock.
    return int(conn.execute(
        """SELECT COUNT(*) AS n FROM pg_locks
           WHERE locktype='advisory' AND granted
             AND classid=%s AND objid=%s AND objsubid=1""",
        (ARCHIVER_LOCK_KEY >> 32, ARCHIVER_LOCK_KEY & 0xFFFFFFFF),
    ).fetchone()["n"])


def pg_session_owns_archiver_lock(conn):
    """True when THIS PostgreSQL session holds the archiver advisory lock."""
    return bool(conn.execute(
        """SELECT EXISTS (
               SELECT 1 FROM pg_locks
               WHERE locktype='advisory' AND granted
                 AND pid=pg_backend_pid()
                 AND classid=%s AND objid=%s
                 AND objsubid=1) AS owned""",
        (ARCHIVER_LOCK_KEY >> 32, ARCHIVER_LOCK_KEY & 0xFFFFFFFF),
    ).fetchone()["owned"])


def archiver_lock_holders(dsn):
    """Number of archiver-lock holders for the catalog behind ``dsn``.

    The one "is any archiver running?" probe for code that has no manager.
    SQLite answers from the OS file lock and fails closed (an unreadable lock
    file counts as held).
    """
    if is_sqlite(dsn):
        from .sqlite_lock import probe_held
        return 1 if probe_held(sqlite_path(dsn)) else 0
    conn = connect(dsn, autocommit=True, read_only=True)
    try:
        return pg_archiver_lock_holders(conn)
    finally:
        conn.close()
