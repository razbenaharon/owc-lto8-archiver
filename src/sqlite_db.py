"""SQLite DatabaseManager for the archive catalog (facade module).

The same method-group mixins as :class:`src.pg_db.PgDatabaseManager`, on top
of :class:`src.sqlite_core.SqliteConnectionCore`. Nothing in the mixins is
backend-specific; every difference is a hook in the core.

This module must stay importable without psycopg.
"""
from .pg_catalog import PgCatalogMixin
from .pg_containers import PgContainerMixin
from .pg_directory_catalog import PgDirectoryCatalogMixin
from .pg_scan import PgScanMixin
from .pg_sessions import PgSessionMixin
from .pg_tape_reset import PgTapeResetMixin
from .pg_tapes import PgTapeMixin
from .sqlite_core import SqliteConnectionCore, SqliteSchemaReports

__all__ = ["SqliteDatabaseManager"]


class SqliteDatabaseManager(SqliteSchemaReports,
                            PgCatalogMixin, PgContainerMixin,
                            PgDirectoryCatalogMixin,
                            PgScanMixin, PgSessionMixin,
                            PgTapeMixin, PgTapeResetMixin,
                            SqliteConnectionCore):
    """SQLite-backed catalog manager with the full DatabaseManager API.

    ``SqliteSchemaReports`` comes first so its schema reports replace the
    ``pg_catalog``-introspecting ones; every other method is shared.
    """
