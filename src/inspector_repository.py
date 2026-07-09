"""Read-only, bounded PostgreSQL query layer for the database inspector."""
import gzip
import json
import os
from typing import Any, TYPE_CHECKING, cast

if TYPE_CHECKING:
    import psycopg
    from psycopg.rows import dict_row
else:
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError:  # pragma: no cover - optional until PG backend is selected
        psycopg = None
        dict_row = None

try:
    import zstandard as zstd
except ImportError:  # pragma: no cover - requirements includes zstandard
    zstd = None

from .catalog_query import prefix_pattern, substring_pattern
from .db import _derived_file_name


DEFAULT_PAGE_SIZE = 250
MAX_PAGE_SIZE = 500


class InspectorRepository:
    """Open one read-only connection for inspector worker use."""

    def __init__(self, db_path):
        self.db_path = db_path
        if psycopg is None:
            raise RuntimeError(
                "[DB] psycopg 3 is required for PostgreSQL inspector access.")
        # autocommit=True keeps each read in its own short transaction. Without
        # it, psycopg's default deferred transaction would stay open for the
        # connection's lifetime ("idle in transaction"), pinning xmin and
        # blocking VACUUM while an inspector tab is left open.
        self.conn: Any = psycopg.connect(
            db_path, autocommit=True, row_factory=cast(Any, dict_row))
        self.conn.execute("SET default_transaction_read_only = on")

    def _execute(self, sql, params=()) -> Any:
        return self.conn.execute(sql, params)

    def close(self):
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc, _tb):
        self.close()

    def _require_v3(self):
        columns = {
            row["column_name"]
            for row in self._execute(
                """SELECT column_name
                   FROM information_schema.columns
                   WHERE table_name='files_index'"""
            )
        }
        available = {"directory_id", "catalog_name",
                     "catalog_backup_date"}.issubset(columns)
        if not available:
            raise RuntimeError(
                "[DB] PostgreSQL catalog indexes are not available. Apply "
                "scripts/sql/001_postgres_schema.sql and "
                "scripts/sql/002_postgres_indexes.sql before using the inspector."
            )

    def require_catalog_v3(self):
        self._require_v3()

    @staticmethod
    def _limit(limit):
        return max(1, min(int(limit or DEFAULT_PAGE_SIZE), MAX_PAGE_SIZE))

    @staticmethod
    def _hydrate(row):
        item = dict(row)
        item.pop("file_hash", None)
        item.pop("file_hash_blob", None)
        if not item.get("file_name"):
            item["file_name"] = item.get("catalog_name") or _derived_file_name(
                item.get("stored_path"), item.get("original_path"))
        if not item.get("backup_date"):
            item["backup_date"] = item.get("catalog_backup_date") or item.get(
                "run_started_at")
        item["source_host"] = item.get("source_host") or "so02"
        if not item.get("container_name"):
            item["container_name"] = item.get("bundle_tape_path")
        return item

    @staticmethod
    def _page(rows, limit, cursor_columns):
        has_more = len(rows) > limit
        visible = rows[:limit]
        next_cursor = None
        if has_more and visible:
            last = visible[-1]
            next_cursor = {column: last[column] for column in cursor_columns}
        return {
            "rows": [dict(row) for row in visible],
            "next_cursor": next_cursor,
            "has_more": has_more,
        }

    def list_tapes(self):
        self._require_v3()
        return [dict(row) for row in self._execute(
            """SELECT t.*,
                      COALESCE(d.root_count, 0) AS root_directory_count
               FROM tapes t
               LEFT JOIN (
                   SELECT tape_label, COUNT(*) AS root_count
                   FROM catalog_directories
                   WHERE parent_id IS NULL
                   GROUP BY tape_label
               ) d ON d.tape_label=t.volume_label
               ORDER BY t.date_formatted DESC, t.volume_label"""
        )]

    def list_source_hosts(self):
        rows = self._execute(
            """SELECT DISTINCT COALESCE(source_host,'so02') AS source_host
               FROM files_index
               ORDER BY source_host"""
        )
        return [row["source_host"] for row in rows if row["source_host"]]

    def _table_exists(self, name):
        return self._execute(
            """SELECT 1
               FROM information_schema.tables
               WHERE table_schema='public' AND table_name=%s""",
            (name,),
        ).fetchone() is not None

    def list_sessions(self):
        rows = []
        if self._table_exists("local_sessions"):
            rows.extend(dict(row) for row in self._execute("""
                SELECT 'local' AS kind,s.session_id,s.session_label,s.status,
                       COALESCE(s.backup_mode,'auto') AS mode,s.created_at,
                       s.completed_at,s.total_chunks AS chunks,
                       COALESCE((SELECT COUNT(*) FROM local_chunks_manifest m
                                 WHERE m.session_id=s.session_id),0) AS manifest_rows,
                       COALESCE((SELECT SUM(dir_size_bytes) FROM local_chunks_manifest m
                                 WHERE m.session_id=s.session_id),0) AS manifest_bytes,
                       COALESCE((SELECT COUNT(*) FROM files_index f
                                 WHERE f.local_session_id=s.session_id),0) AS file_records
                FROM local_sessions s ORDER BY s.session_id"""))
        if self._table_exists("remote_sessions"):
            if self._table_exists("remote_plan_files"):
                rows.extend(dict(row) for row in self._execute("""
                    SELECT 'remote' AS kind,s.session_id,s.session_label,s.status,
                           '' AS mode,s.created_at,s.completed_at,s.chunk_count AS chunks,
                           COALESCE((SELECT COUNT(*) FROM remote_plan_files pf
                                     WHERE pf.plan_id=s.plan_id),0) AS manifest_rows,
                           COALESCE((SELECT SUM(sf.file_size_bytes)
                                     FROM remote_plan_files pf
                                     JOIN remote_snapshot_files sf
                                       ON sf.snapshot_file_id=pf.snapshot_file_id
                                     WHERE pf.plan_id=s.plan_id),0) AS manifest_bytes,
                           0 AS file_records
                    FROM remote_sessions s ORDER BY s.session_id"""))
            elif self._table_exists("remote_manifest"):
                rows.extend(dict(row) for row in self._execute("""
                    SELECT 'remote' AS kind,s.session_id,s.session_label,s.status,
                           '' AS mode,s.created_at,s.completed_at,s.chunk_count AS chunks,
                           COALESCE((SELECT COUNT(*) FROM remote_manifest m
                                     WHERE m.session_id=s.session_id),0) AS manifest_rows,
                           COALESCE((SELECT SUM(file_size_bytes) FROM remote_manifest m
                                     WHERE m.session_id=s.session_id),0) AS manifest_bytes,
                           0 AS file_records
                    FROM remote_sessions s ORDER BY s.session_id"""))
        return sorted(rows, key=lambda r: (r["kind"], int(r["session_id"])))

    def list_directory_bundles(self, tape_label=None, cursor=None, limit=None):
        page_size = self._limit(limit)
        where = []
        params = []
        if tape_label and tape_label != "All":
            where.append("tape_label=%s")
            params.append(tape_label)
        if cursor:
            where.append("bundle_id > %s")
            params.append(cursor["bundle_id"])
        sql = """SELECT * FROM directory_archive_bundles"""
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY bundle_id LIMIT %s"
        rows = self._execute(sql, params + [page_size + 1]).fetchall()
        return self._page(rows, page_size, ("bundle_id",))

    def list_directory_stats(self, tape_label=None, cursor=None, limit=None):
        page_size = self._limit(limit)
        where = []
        params = []
        if tape_label and tape_label != "All":
            where.append("tape_label=%s")
            params.append(tape_label)
        if cursor:
            where.append("stat_id > %s")
            params.append(cursor["stat_id"])
        sql = """SELECT * FROM directory_archive_stats"""
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY stat_id LIMIT %s"
        rows = self._execute(sql, params + [page_size + 1]).fetchall()
        return self._page(rows, page_size, ("stat_id",))

    def find_directory_tree(self, query, tape_label=None, limit=None):
        page_size = self._limit(limit)
        params = [substring_pattern(query)]
        where = ["original_dir_path ILIKE %s ESCAPE '\\'"]
        if tape_label and tape_label != "All":
            where.append("tape_label=%s")
            params.append(tape_label)
        rows = self._execute(
            """SELECT * FROM directory_tree_index
               WHERE """ + " AND ".join(where) +
            """ ORDER BY original_dir_path, dir_id LIMIT %s""",
            params + [page_size],
        ).fetchall()
        return [dict(row) for row in rows]

    @staticmethod
    def _open_manifest(path, compression):
        compression = (compression or "").lower()
        if compression in ("zstd", "zst") or str(path).endswith(".zst"):
            if zstd is None:
                raise RuntimeError("zstandard is required to read .jsonl.zst manifests")
            raw = open(path, "rb")
            reader = zstd.ZstdDecompressor().stream_reader(raw)
            import io
            return raw, reader, io.TextIOWrapper(reader, encoding="utf-8")
        if compression == "gzip" or str(path).endswith(".gz"):
            return (gzip.open(path, "rt", encoding="utf-8"),)
        return (open(path, "r", encoding="utf-8"),)

    @staticmethod
    def _close_manifest(handle):
        for item in reversed(handle):
            try:
                item.close()
            except Exception:
                pass

    def search_small_file_manifests(self, query, tape_label=None, limit=None):
        page_size = self._limit(limit)
        where = ["manifest_path IS NOT NULL"]
        params = []
        if tape_label and tape_label != "All":
            where.append("tape_label=%s")
            params.append(tape_label)
        bundles = self._execute(
            """SELECT bundle_id, tape_label, stored_bundle_path,
                      manifest_path, manifest_compression
               FROM directory_archive_bundles
               WHERE """ + " AND ".join(where) +
            " ORDER BY backup_date DESC",
            params,
        ).fetchall()
        needle = (query or "").lower()
        hits = []
        for bundle in bundles:
            if len(hits) >= page_size:
                break
            path = bundle["manifest_path"]
            if not path or not os.path.exists(path):
                continue
            handle = self._open_manifest(path, bundle["manifest_compression"])
            try:
                for line in handle[-1]:
                    if len(hits) >= page_size:
                        break
                    try:
                        item = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    text = (
                        str(item.get("file_name") or "") + "\n"
                        + str(item.get("relative_path") or "")
                    ).lower()
                    if needle in text:
                        row = dict(item)
                        row.update({
                            "bundle_id": bundle["bundle_id"],
                            "tape_label": bundle["tape_label"],
                            "stored_bundle_path": bundle["stored_bundle_path"],
                            "manifest_path": path,
                        })
                        hits.append(row)
            finally:
                self._close_manifest(handle)
        return hits

    def validate_directory_catalog(self, tape_label=None):
        params = []
        tape_filter = ""
        if tape_label and tape_label != "All":
            tape_filter = "WHERE b.tape_label=%s"
            params.append(tape_label)
        rows = self._execute(
            f"""SELECT b.bundle_id, b.tape_label, b.stored_bundle_path,
                      b.file_count, b.byte_count,
                      COALESCE(SUM(t.direct_file_count), 0) AS direct_files,
                      COALESCE(SUM(t.direct_bytes), 0) AS direct_bytes
               FROM directory_archive_bundles b
               LEFT JOIN directory_tree_index t ON t.bundle_id=b.bundle_id
               {tape_filter}
               GROUP BY b.bundle_id, b.tape_label, b.stored_bundle_path,
                        b.file_count, b.byte_count
               ORDER BY b.bundle_id""",
            params,
        ).fetchall()
        warnings = []
        for row in rows:
            if row["file_count"] != row["direct_files"]:
                warnings.append(dict(row) | {"warning": "file_count mismatch"})
            if row["byte_count"] != row["direct_bytes"]:
                warnings.append(dict(row) | {"warning": "byte_count mismatch"})
        return {"bundles_checked": len(rows), "warnings": warnings}

    def unreferenced_remote_data_summary(self):
        required = {
            'remote_sessions', 'remote_snapshots', 'remote_snapshot_files',
            'remote_plans', 'remote_plan_files',
        }
        if not all(self._table_exists(table) for table in required):
            return {
                'supported': False, 'active_sessions': 0,
                'plans': 0, 'plan_files': 0,
                'snapshots': 0, 'snapshot_files': 0,
            }
        return dict(self._execute("""
            SELECT
              1 AS supported,
              (SELECT COUNT(*) FROM remote_sessions
               WHERE status='active') AS active_sessions,
              (SELECT COUNT(*) FROM remote_plans p
               WHERE NOT EXISTS (
                 SELECT 1 FROM remote_sessions s WHERE s.plan_id=p.plan_id
               )) AS plans,
              (SELECT COUNT(*) FROM remote_plan_files pf
               WHERE EXISTS (
                 SELECT 1 FROM remote_plans p
                 WHERE p.plan_id=pf.plan_id AND NOT EXISTS (
                   SELECT 1 FROM remote_sessions s WHERE s.plan_id=p.plan_id
                 )
               )) AS plan_files,
              (SELECT COUNT(*) FROM remote_snapshots sn
               WHERE NOT EXISTS (
                 SELECT 1 FROM remote_plans p
                 JOIN remote_sessions s ON s.plan_id=p.plan_id
                 WHERE p.snapshot_id=sn.snapshot_id
               )) AS snapshots,
              (SELECT COUNT(*) FROM remote_snapshot_files sf
               WHERE EXISTS (
                 SELECT 1 FROM remote_snapshots sn
                 WHERE sn.snapshot_id=sf.snapshot_id AND NOT EXISTS (
                   SELECT 1 FROM remote_plans p
                   JOIN remote_sessions s ON s.plan_id=p.plan_id
                   WHERE p.snapshot_id=sn.snapshot_id
                 )
               )) AS snapshot_files
        """).fetchone())

    def list_child_directories(self, parent_id=None, cursor=None, limit=None,
                               tape_label=None):
        self._require_v3()
        page_size = self._limit(limit)
        params = []
        if parent_id is None:
            if not tape_label:
                raise RuntimeError("[DB] tape_label is required for root directories.")
            where = "tape_label=%s AND parent_id IS NULL"
            params.append(tape_label)
        else:
            where = "parent_id=%s"
            params.append(parent_id)
        if cursor:
            where += " AND (name > %s OR (name = %s AND directory_id > %s))"
            params.extend([cursor["name"], cursor["name"], cursor["directory_id"]])
        rows = self._execute(
            f"""SELECT directory_id, tape_label, parent_id, name, normalized_path
                FROM catalog_directories
                WHERE {where}
                ORDER BY name, directory_id
                LIMIT %s""",
            params + [page_size + 1],
        ).fetchall()
        return self._page(rows, page_size, ("name", "directory_id"))

    def subtree_sizes(self, directory_ids):
        """Recursive file-byte and file-count totals per directory subtree.

        Sizes are not stored on catalog_directories, so each directory's total
        is the sum of files_index rows across the directory and all of its
        descendants. The recursive descent rides idx_catalog_dirs_parent_id and
        the per-directory sum rides the (directory_id, file_size_bytes) index,
        so one batched query covers a whole page of sibling directories.
        """
        self._require_v3()
        ids = [int(directory_id) for directory_id in (directory_ids or [])]
        if not ids:
            return {}
        rows = self._execute(
            """WITH RECURSIVE subtree AS (
                   SELECT directory_id AS root_id, directory_id
                   FROM catalog_directories
                   WHERE directory_id = ANY(%s)
                   UNION ALL
                   SELECT s.root_id, c.directory_id
                   FROM catalog_directories c
                   JOIN subtree s ON c.parent_id = s.directory_id
               )
               SELECT s.root_id,
                      COALESCE(SUM(f.file_size_bytes), 0) AS recursive_bytes,
                      COUNT(f.file_id) AS recursive_file_count
               FROM subtree s
               LEFT JOIN files_index f ON f.directory_id = s.directory_id
               GROUP BY s.root_id""",
            (ids,),
        ).fetchall()
        return {
            row["root_id"]: {
                "recursive_bytes": row["recursive_bytes"] or 0,
                "recursive_file_count": row["recursive_file_count"] or 0,
            }
            for row in rows
        }

    def list_directory_files(self, directory_id, sort="name", filters=None, cursor=None,
                             limit=None):
        self._require_v3()
        page_size = self._limit(limit)
        sort_sql, cursor_sql, cursor_columns = self._sort_parts(sort, cursor)
        filters = filters or {}
        where = ["f.directory_id=%s"]
        params = [directory_id]
        if filters.get("name_prefix"):
            where.append("f.catalog_name ILIKE %s ESCAPE '\\'")
            params.append(prefix_pattern(filters["name_prefix"]))
        if filters.get("date_from"):
            where.append("f.catalog_backup_date >= %s::date")
            params.append(filters["date_from"])
        if filters.get("date_to"):
            where.append("f.catalog_backup_date < (%s::date + INTERVAL '1 day')")
            params.append(filters["date_to"])
        if filters.get("source_host"):
            where.append("f.source_host=%s")
            params.append(filters["source_host"])
        if cursor_sql:
            where.append(cursor_sql[0][4:])
            params.extend(cursor_sql[1])
        rows = self._execute(
            f"""{self._file_select()}
                WHERE {' AND '.join(where)}
                ORDER BY {sort_sql}
                LIMIT %s""",
            params + [page_size + 1],
        ).fetchall()
        visible = rows[:page_size]
        has_more = len(rows) > page_size
        next_cursor = None
        if has_more and visible:
            last = visible[-1]
            next_cursor = {column: last[column] for column in cursor_columns}
        return {
            "rows": [self._hydrate(row) for row in visible],
            "next_cursor": next_cursor,
            "has_more": has_more,
        }

    def search_catalog_fts(self, query, scope=None, cursor=None, limit=None):
        self._require_v3()
        page_size = self._limit(limit)
        scope = scope or {}
        where = ["(f.catalog_name ILIKE %s ESCAPE '\\' "
                 "OR f.original_path ILIKE %s ESCAPE '\\')"]
        like = substring_pattern(query)
        params = [like, like]
        cursor_column = "f.file_id"
        if scope.get("tape_label"):
            where.append("f.tape_label=%s")
            params.append(scope["tape_label"])
        if scope.get("directory_id"):
            where.append("f.directory_id=%s")
            params.append(scope["directory_id"])
        if scope.get("source_host"):
            where.append("f.source_host=%s")
            params.append(scope["source_host"])
        if cursor:
            where.append(f"{cursor_column} > %s")
            params.append(cursor["file_id"])
        rows = self._execute(
            f"""{self._file_select()}
                WHERE {' AND '.join(where)}
                ORDER BY f.file_id
                LIMIT %s""",
            params + [page_size + 1],
        ).fetchall()
        visible = rows[:page_size]
        return {
            "rows": [self._hydrate(row) for row in visible],
            "next_cursor": (
                {"file_id": visible[-1]["file_id"]}
                if len(rows) > page_size and visible else None),
            "has_more": len(rows) > page_size,
        }

    def get_file(self, file_id):
        row = self._execute(
            self._file_select() + " WHERE f.file_id=%s",
            (file_id,),
        ).fetchone()
        return self._hydrate(row) if row else None

    @staticmethod
    def _file_select():
        prefix = """SELECT f.*, b.tape_path AS bundle_tape_path,
                           r.started_at AS run_started_at
                    FROM """
        prefix += "files_index f"
        return prefix + """
                    LEFT JOIN archive_bundles b ON b.bundle_id=f.bundle_id
                    LEFT JOIN archive_runs r ON r.run_id=f.archive_run_id"""

    @staticmethod
    def _sort_parts(sort, cursor):
        sort = sort or "name"
        if sort == "size":
            order = "f.file_size_bytes, f.catalog_name, f.file_id"
            columns = ("file_size_bytes", "catalog_name", "file_id")
            if cursor:
                return order, (
                    """AND (f.file_size_bytes > %s
                         OR (f.file_size_bytes = %s AND f.catalog_name > %s)
                         OR (f.file_size_bytes = %s AND f.catalog_name = %s
                             AND f.file_id > %s))""",
                    [cursor["file_size_bytes"], cursor["file_size_bytes"],
                     cursor["catalog_name"], cursor["file_size_bytes"],
                     cursor["catalog_name"], cursor["file_id"]],
                ), columns
            return order, None, columns
        if sort == "date":
            order = "f.catalog_backup_date, f.catalog_name, f.file_id"
            columns = ("catalog_backup_date", "catalog_name", "file_id")
            if cursor:
                return order, (
                    """AND (f.catalog_backup_date > %s
                         OR (f.catalog_backup_date = %s AND f.catalog_name > %s)
                         OR (f.catalog_backup_date = %s AND f.catalog_name = %s
                             AND f.file_id > %s))""",
                    [cursor["catalog_backup_date"], cursor["catalog_backup_date"],
                     cursor["catalog_name"], cursor["catalog_backup_date"],
                     cursor["catalog_name"], cursor["file_id"]],
                ), columns
            return order, None, columns
        order = "f.catalog_name, f.file_id"
        columns = ("catalog_name", "file_id")
        if cursor:
            return order, (
                "AND (f.catalog_name > %s OR (f.catalog_name = %s AND f.file_id > %s))",
                [cursor["catalog_name"], cursor["catalog_name"], cursor["file_id"]],
            ), columns
        return order, None, columns
