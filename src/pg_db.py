"""PostgreSQL DatabaseManager for the archive catalog (facade module).

The implementation is split by concern into sibling modules with strictly
downward dependencies:

- :mod:`src.pg_core`     — pool, retrying transactions, schema, advisory lock
- :mod:`src.pg_catalog`  — files_index catalog: bulk upsert, search, counts
- :mod:`src.pg_sessions` — local/remote sessions, snapshots, plans, chunks
- :mod:`src.pg_tapes`    — tape registry and label-wide maintenance

This module assembles them into the single ``PgDatabaseManager`` class the
rest of the application uses, and re-exports the helpers that tests and
tooling import from here.
"""
from .pg_catalog import PgCatalogMixin
from .pg_core import (PgConnectionCore, PgRow, _as_utc, _coerce_timestamptz,
                      _coerce_timestamp_kwargs, _now_utc, _row, _rows,
                      _valid_columns)
from .pg_sessions import (PgSessionMixin, _canonical_remote_path,
                          _plan_fingerprint, _snapshot_fingerprint,
                          _streaming_fingerprint)
from .pg_tapes import PgTapeMixin

# Everything historically importable from src.pg_db stays importable here.
__all__ = [
    "PgDatabaseManager", "PgConnectionCore", "PgCatalogMixin",
    "PgSessionMixin", "PgTapeMixin", "PgRow",
    "_as_utc", "_canonical_remote_path", "_coerce_timestamp_kwargs",
    "_coerce_timestamptz", "_now_utc", "_plan_fingerprint", "_row", "_rows",
    "_snapshot_fingerprint", "_streaming_fingerprint", "_valid_columns",
]


class PgDatabaseManager(PgCatalogMixin, PgSessionMixin, PgTapeMixin,
                        PgConnectionCore):
    """PostgreSQL-backed subset of the DatabaseManager API.

    This covers the live local archive/restore/catalog workflows. The mixins
    hold the method groups; state (``self._pool``, ``self._lock_conn``) and
    the transaction/retry machinery live in :class:`PgConnectionCore`.
    """
