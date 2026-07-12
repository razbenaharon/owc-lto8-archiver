"""Read-only PostgreSQL access for the coverage refresh.

Mirrors :class:`src.inspector_repository.InspectorRepository`: autocommit so no
transaction is left open pinning xmin, plus a session-level read-only guard.
One connection is opened per refresh and closed right after — the web app
holds no idle connections between refreshes.
"""
from typing import Any, TYPE_CHECKING, cast

if TYPE_CHECKING:
    import psycopg
    from psycopg.rows import dict_row
else:
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError:  # pragma: no cover - optional until PG access is needed
        psycopg = None
        dict_row = None

from .coverage import COVERAGE_SQL


class CoverageRepository:
    def __init__(self, dsn):
        if psycopg is None:
            raise RuntimeError(
                "[DB] psycopg 3 is required for coverage queries "
                "(pip install 'psycopg[binary]').")
        self.conn: Any = psycopg.connect(
            dsn, autocommit=True, row_factory=cast(Any, dict_row))
        self.conn.execute("SET default_transaction_read_only = on")

    def close(self):
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc, _tb):
        self.close()

    def fetch_coverage_rows(self, max_segs, threshold_bytes=10 * 1024 * 1024):
        """Run the aggregation and return JSON-safe row dicts.

        ``threshold_bytes`` is the ``index_min_file_mb`` boundary: packed files
        below it live only in the directory catalog, so the query adds their
        per-directory totals on top of ``files_index`` (see :data:`COVERAGE_SQL`).
        """
        cursor = self.conn.execute(COVERAGE_SQL, {
            'max_segs': int(max_segs),
            'threshold_bytes': int(threshold_bytes),
        })
        rows = []
        for row in cursor:
            last = row.get('last_backup')
            rows.append({
                'host': row['host'],
                'dir_prefix': row['dir_prefix'],
                'tape_bytes': int(row['tape_bytes'] or 0),
                'tape_files': int(row['tape_files'] or 0),
                'last_backup': last.isoformat() if last is not None else None,
            })
        return rows
