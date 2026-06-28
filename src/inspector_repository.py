"""Read-only, bounded query layer for the database inspector."""
import os
import sqlite3

from .catalog_v3 import catalog_v3_available
from .db import _derived_file_name


DEFAULT_PAGE_SIZE = 250
MAX_PAGE_SIZE = 500


class InspectorRepository:
    """Open one read-only SQLite connection for inspector worker use."""

    def __init__(self, db_path):
        self.db_path = os.path.abspath(db_path)
        uri = "file:" + self.db_path.replace("\\", "/") + "?mode=ro"
        self.conn = sqlite3.connect(uri, uri=True, timeout=30)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA query_only=ON")
        self.conn.execute("PRAGMA busy_timeout=30000")

    def close(self):
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc, _tb):
        self.close()

    def _require_v3(self):
        if not catalog_v3_available(self.conn):
            raise RuntimeError(
                "[DB] Catalog v3 indexes are not available. Run "
                "`python run.py --catalog-v3-preflight` and then migrate a "
                "validated copy before using lazy inspector browsing."
            )

    @staticmethod
    def _limit(limit):
        return max(1, min(int(limit or DEFAULT_PAGE_SIZE), MAX_PAGE_SIZE))

    @staticmethod
    def _hydrate(row):
        item = dict(row)
        if not item.get("file_name"):
            item["file_name"] = item.get("catalog_name") or _derived_file_name(
                item.get("stored_path"), item.get("original_path"))
        if not item.get("backup_date"):
            item["backup_date"] = item.get("catalog_backup_date") or item.get(
                "run_started_at")
        if not item.get("file_hash") and item.get("file_hash_blob"):
            item["file_hash"] = bytes(item["file_hash_blob"]).hex()
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
        return [dict(row) for row in self.conn.execute(
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

    def list_child_directories(self, parent_id=None, cursor=None, limit=None,
                               tape_label=None):
        self._require_v3()
        page_size = self._limit(limit)
        params = []
        if parent_id is None:
            if not tape_label:
                raise RuntimeError("[DB] tape_label is required for root directories.")
            where = "tape_label=? AND parent_id IS NULL"
            params.append(tape_label)
        else:
            where = "parent_id=?"
            params.append(parent_id)
        if cursor:
            where += " AND (name > ? OR (name = ? AND directory_id > ?))"
            params.extend([cursor["name"], cursor["name"], cursor["directory_id"]])
        rows = self.conn.execute(
            f"""SELECT directory_id, tape_label, parent_id, name, normalized_path
                FROM catalog_directories
                WHERE {where}
                ORDER BY name, directory_id
                LIMIT ?""",
            params + [page_size + 1],
        ).fetchall()
        return self._page(rows, page_size, ("name", "directory_id"))

    def list_directory_files(self, directory_id, sort="name", filters=None, cursor=None,
                             limit=None):
        self._require_v3()
        page_size = self._limit(limit)
        sort_sql, cursor_sql, cursor_columns = self._sort_parts(sort, cursor)
        filters = filters or {}
        where = ["f.directory_id=?"]
        params = [directory_id]
        if filters.get("name_prefix"):
            where.append("f.catalog_name LIKE ? ESCAPE '\\'")
            prefix = filters["name_prefix"]
            params.append(prefix.replace("%", r"\%").replace("_", r"\_") + "%")
        if filters.get("date_from"):
            where.append("DATE(f.catalog_backup_date) >= ?")
            params.append(filters["date_from"])
        if filters.get("date_to"):
            where.append("DATE(f.catalog_backup_date) <= ?")
            params.append(filters["date_to"])
        if cursor_sql:
            where.append(cursor_sql[0][4:])
            params.extend(cursor_sql[1])
        rows = self.conn.execute(
            f"""{self._file_select()}
                WHERE {' AND '.join(where)}
                ORDER BY {sort_sql}
                LIMIT ?""",
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
        where = ["files_index_fts MATCH ?"]
        params = [query]
        if scope.get("tape_label"):
            where.append("f.tape_label=?")
            params.append(scope["tape_label"])
        if scope.get("directory_id"):
            where.append("f.directory_id=?")
            params.append(scope["directory_id"])
        if cursor:
            where.append("files_index_fts.rowid > ?")
            params.append(cursor["file_id"])
        rows = self.conn.execute(
            f"""{self._file_select(from_fts=True)}
                WHERE {' AND '.join(where)}
                LIMIT ?""",
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
        row = self.conn.execute(
            self._file_select() + " WHERE f.file_id=?",
            (file_id,),
        ).fetchone()
        return self._hydrate(row) if row else None

    @staticmethod
    def _file_select(from_fts=False):
        prefix = """SELECT f.*, b.tape_path AS bundle_tape_path,
                           r.started_at AS run_started_at
                    FROM """
        if from_fts:
            prefix += """files_index_fts
                         JOIN files_index f ON f.file_id=files_index_fts.rowid"""
        else:
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
                    """AND (f.file_size_bytes > ?
                         OR (f.file_size_bytes = ? AND f.catalog_name > ?)
                         OR (f.file_size_bytes = ? AND f.catalog_name = ?
                             AND f.file_id > ?))""",
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
                    """AND (f.catalog_backup_date > ?
                         OR (f.catalog_backup_date = ? AND f.catalog_name > ?)
                         OR (f.catalog_backup_date = ? AND f.catalog_name = ?
                             AND f.file_id > ?))""",
                    [cursor["catalog_backup_date"], cursor["catalog_backup_date"],
                     cursor["catalog_name"], cursor["catalog_backup_date"],
                     cursor["catalog_name"], cursor["file_id"]],
                ), columns
            return order, None, columns
        order = "f.catalog_name, f.file_id"
        columns = ("catalog_name", "file_id")
        if cursor:
            return order, (
                "AND (f.catalog_name > ? OR (f.catalog_name = ? AND f.file_id > ?))",
                [cursor["catalog_name"], cursor["catalog_name"], cursor["file_id"]],
            ), columns
        return order, None, columns
