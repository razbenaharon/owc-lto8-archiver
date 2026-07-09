"""Read-only reports for directory-catalog migration validation."""
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    import psycopg
    from psycopg.rows import dict_row
else:
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError:  # pragma: no cover - runtime dependency path
        psycopg = None
        dict_row = None

from .pg_bulk import require_psycopg
from .pg_core import PgConnectionCore


DIRECTORY_TABLES = (
    "directory_archive_stats",
    "directory_archive_bundles",
    "directory_tree_index",
)

LEGACY_COMPARE_TABLES = (
    "files_index",
    "tapes",
    "catalog_directories",
    "archive_bundles",
    "archive_runs",
    "local_sessions",
    "local_chunks_manifest",
    "remote_sessions",
    "remote_chunks",
    "remote_snapshots",
    "remote_snapshot_files",
    "remote_plans",
    "remote_plan_files",
    "remote_file_state",
)


def _connect(conninfo):
    require_psycopg()
    return cast(Any, psycopg.connect(
        conninfo, autocommit=True, row_factory=cast(Any, dict_row)))


def _table_exists(conn, table_name):
    return conn.execute(
        """SELECT 1
           FROM information_schema.tables
           WHERE table_schema='public' AND table_name=%s""",
        (table_name,),
    ).fetchone() is not None


def _count(conn, table_name):
    if not _table_exists(conn, table_name):
        return None
    return conn.execute(f"SELECT COUNT(*) AS n FROM {table_name}").fetchone()["n"]


def describe_database(conninfo):
    with _connect(conninfo) as conn:
        conn.execute("SET default_transaction_read_only = on")
        row = conn.execute(
            """SELECT current_database() AS database,
                      inet_server_addr()::text AS server_addr,
                      inet_server_port() AS server_port,
                      current_user AS current_user"""
        ).fetchone()
    return dict(row)


def archiver_lock_status(conninfo):
    key = PgConnectionCore.ARCHIVER_LOCK_KEY
    with _connect(conninfo) as conn:
        conn.execute("SET default_transaction_read_only = on")
        row = conn.execute(
            """SELECT COUNT(*) AS holders
               FROM pg_locks
               WHERE locktype='advisory'
                 AND classid = ((%s::bigint >> 32) & 4294967295)::integer
                 AND objid = (%s::bigint & 4294967295)::integer
                 AND granted""",
            (key, key),
        ).fetchone()
    return int(row["holders"])


def row_counts(conninfo, tables):
    with _connect(conninfo) as conn:
        conn.execute("SET default_transaction_read_only = on")
        return {table: _count(conn, table) for table in tables}


def _used_space_estimates(conn):
    if not _table_exists(conn, "tapes"):
        return []
    if not _table_exists(conn, "directory_archive_bundles"):
        rows = conn.execute(
            """SELECT t.volume_label AS tape_label,
                      t.used_space AS recorded_used_space,
                      COALESCE(SUM(f.file_size_bytes), 0) AS calculated_used_space
               FROM tapes t
               LEFT JOIN files_index f ON f.tape_label=t.volume_label
               GROUP BY t.volume_label, t.used_space
               ORDER BY t.volume_label"""
        ).fetchall()
        return [dict(row) for row in rows]
    rows = conn.execute(
        """WITH calculated AS (
               SELECT t.volume_label AS tape_label,
                      t.used_space AS recorded_used_space,
                      COALESCE((
                          SELECT SUM(f.file_size_bytes)
                          FROM files_index f
                          LEFT JOIN archive_bundles ab ON ab.bundle_id=f.bundle_id
                          WHERE f.tape_label=t.volume_label
                            AND NOT EXISTS (
                                SELECT 1
                                FROM directory_archive_bundles db
                                WHERE db.tape_label=t.volume_label
                                  AND db.stored_bundle_path=ab.tape_path
                            )
                      ), 0)
                      + COALESCE((
                          SELECT SUM(byte_count)
                          FROM directory_archive_bundles db
                          WHERE db.tape_label=t.volume_label
                      ), 0) AS calculated_used_space
               FROM tapes t
           )
           SELECT * FROM calculated ORDER BY tape_label"""
    ).fetchall()
    return [dict(row) for row in rows]


def validate_directory_catalog(conninfo):
    warnings = []
    with _connect(conninfo) as conn:
        conn.execute("SET default_transaction_read_only = on")
        existing = {table for table in DIRECTORY_TABLES
                    if _table_exists(conn, table)}
        missing = [table for table in DIRECTORY_TABLES if table not in existing]
        counts = {table: _count(conn, table) for table in DIRECTORY_TABLES}
        if missing:
            warnings.append({
                "type": "missing_directory_tables",
                "tables": missing,
            })
            return {
                "ok": False,
                "directory_counts": counts,
                "used_space": _used_space_estimates(conn),
                "warnings": warnings,
            }
        rows = conn.execute(
            """SELECT b.bundle_id, b.tape_label, b.stored_bundle_path,
                      b.file_count AS bundle_file_count,
                      b.byte_count AS bundle_bytes,
                      COALESCE(SUM(t.direct_file_count), 0) AS direct_files,
                      COALESCE(SUM(t.direct_bytes), 0) AS direct_bytes
               FROM directory_archive_bundles b
               LEFT JOIN directory_tree_index t ON t.bundle_id=b.bundle_id
               GROUP BY b.bundle_id, b.tape_label, b.stored_bundle_path,
                        b.file_count, b.byte_count
               ORDER BY b.bundle_id"""
        ).fetchall()
        for row in rows:
            if row["bundle_file_count"] != row["direct_files"]:
                warnings.append(dict(row) | {
                    "type": "bundle_direct_file_count_mismatch"})
            if row["bundle_bytes"] != row["direct_bytes"]:
                warnings.append(dict(row) | {
                    "type": "bundle_direct_byte_count_mismatch"})
        for table, value in counts.items():
            if value == 0:
                warnings.append({
                    "type": "empty_directory_catalog_table",
                    "table": table,
                })
        used_space = _used_space_estimates(conn)
        for row in used_space:
            if row["recorded_used_space"] != row["calculated_used_space"]:
                warnings.append(dict(row) | {
                    "type": "tape_used_space_mismatch"})
    return {
        "ok": not warnings,
        "directory_counts": counts,
        "bundles_checked": len(rows) if not missing else 0,
        "used_space": used_space,
        "warnings": warnings,
    }


def compare_databases(source_conninfo, target_conninfo):
    source_counts = row_counts(source_conninfo, LEGACY_COMPARE_TABLES)
    target_counts = row_counts(target_conninfo, LEGACY_COMPARE_TABLES)
    mismatches = []
    for table in LEGACY_COMPARE_TABLES:
        if source_counts[table] != target_counts[table]:
            mismatches.append({
                "table": table,
                "source": source_counts[table],
                "target": target_counts[table],
            })
    target_validation = validate_directory_catalog(target_conninfo)
    return {
        "ok": not mismatches and target_validation["ok"],
        "source_counts": source_counts,
        "target_counts": target_counts,
        "mismatches": mismatches,
        "target_directory_validation": target_validation,
    }
