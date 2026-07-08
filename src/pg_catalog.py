"""File-catalog method group: files_index, bundles, runs, search/counts."""
from collections import defaultdict
from typing import Any, Iterable, List

from .catalog_query import contains_pattern, prefix_pattern, substring_pattern
from .catalog_v3 import catalog_directory_chain, catalog_file_name
from .constants import DB_UPSERT_BATCH_SIZE, LEGACY_DEFAULT_SOURCE_HOST
from .db import _derived_file_name, _file_record_key, _short_source_host
from .pg_bulk import copy_rows
from .pg_core import _as_utc, _now_utc, _rows
from .pipeline_types import FileRecord
from .runtime import CANCEL


class PgCatalogMixin:
    """files_index catalog: bulk upsert, dedup keys, search and counts.

    Mixin over :class:`src.pg_core.PgConnectionCore` (uses ``self._pool`` and
    ``self._transaction``); assembled in :class:`src.pg_db.PgDatabaseManager`.
    """

    @staticmethod
    def _hydrate_file_row(row):
        if row is None:
            return None
        item = dict(row)
        item["file_name"] = item.get("catalog_name") or _derived_file_name(
            item.get("stored_path"), item.get("original_path"))
        item["backup_date"] = item.get("catalog_backup_date") or item.get(
            "run_started_at")
        item["source_host"] = _short_source_host(
            item.get("source_host") or LEGACY_DEFAULT_SOURCE_HOST)
        item["container_name"] = item.get("bundle_tape_path")
        return item

    @staticmethod
    def _catalog_select():
        return """SELECT f.*, b.tape_path AS bundle_tape_path,
                         r.started_at AS run_started_at
                  FROM files_index AS f
                  LEFT JOIN archive_bundles AS b ON b.bundle_id = f.bundle_id
                  LEFT JOIN archive_runs AS r ON r.run_id = f.archive_run_id"""

    def _catalog_rows(self, where="", params=(), order_by=""):
        sql = self._catalog_select()
        if where:
            sql += " WHERE " + where
        if order_by:
            sql += " ORDER BY " + order_by
        with self._pool.connection() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._hydrate_file_row(row) for row in rows]

    @staticmethod
    def _collect_directory_specs(records):
        """Pre-compute the directory chain for a batch of file records.

        Returns ``(specs, targets)`` where ``specs`` maps
        ``(tape_label, normalized_path)`` -> ``(parent_path, name)`` for every
        distinct directory the batch touches (deduplicated across files), and
        ``targets`` is a list, parallel to ``records``, giving each file's
        immediate parent directory key. Files with no directory component fall
        back to a per-tape synthetic ``ROOT`` bucket, matching the previous
        per-record behaviour.
        """
        specs = {}
        targets = []
        for record in records:
            tape_label = record.get("tape_label") or ""
            source_host = _short_source_host(
                record.get("source_host") or LEGACY_DEFAULT_SOURCE_HOST)
            canonical = record.get("canonical_source_path")
            original_path = (str(canonical) if canonical
                             else record.get("original_path") or "")
            dir_path = original_path or record.get("stored_path")
            chain = catalog_directory_chain(dir_path, source_host)
            if chain:
                for normalized_path, parent_path, name in chain:
                    specs[(tape_label, normalized_path)] = (parent_path, name)
                targets.append((tape_label, chain[-1][0]))
            else:
                specs[(tape_label, "ROOT")] = (None, "ROOT")
                targets.append((tape_label, "ROOT"))
        return specs, targets

    def _ensure_directories(self, conn, specs):
        """Batch-upsert ``catalog_directories`` and return their ids.

        ``specs`` maps ``(tape_label, normalized_path)`` -> ``(parent_path,
        name)``. Directories are grouped by tree depth and inserted parents-first
        with a single multi-row upsert per level, so the whole chain resolves in
        O(depth) round-trips instead of one per directory per file (the former
        N+1 that dominated Phase-3 sync time). Returns
        ``{(tape_label, normalized_path): directory_id}``.
        """
        resolved = {}
        if not specs:
            return resolved
        by_depth = defaultdict(list)
        for (tape_label, normalized_path), (parent_path, name) in specs.items():
            by_depth[normalized_path.count("/")].append(
                (tape_label, normalized_path, parent_path, name))
        # libpq caps a single statement at 65,535 bind parameters (4 per row).
        # Slicing each level keeps the upsert safe even if DB_UPSERT_BATCH_SIZE
        # is ever raised past that ceiling.
        max_rows = 5000
        for depth in sorted(by_depth):
            level = by_depth[depth]
            for start in range(0, len(level), max_rows):
                values = []
                params = []
                for tape_label, normalized_path, parent_path, name in (
                        level[start:start + max_rows]):
                    parent_id = (resolved.get((tape_label, parent_path))
                                 if parent_path is not None else None)
                    values.append("(%s, %s, %s, %s)")
                    params.extend([tape_label, parent_id, name, normalized_path])
                rows = conn.execute(
                    "INSERT INTO catalog_directories "
                    "(tape_label, parent_id, name, normalized_path) "
                    f"VALUES {', '.join(values)} "
                    "ON CONFLICT (tape_label, normalized_path) DO UPDATE "
                    "SET name = EXCLUDED.name "
                    "RETURNING directory_id, tape_label, normalized_path",
                    params,
                ).fetchall()
                for row in rows:
                    resolved[(row["tape_label"], row["normalized_path"])] = (
                        row["directory_id"])
        return resolved

    def _normalize_file_records(self, conn, records):
        bundle_paths = {
            (record["tape_label"], record.get("container_name"))
            for record in records
            if record.get("is_packed") and record.get("container_name")
        }
        for tape_label, tape_path in bundle_paths:
            conn.execute(
                """INSERT INTO archive_bundles(tape_label, tape_path)
                   VALUES (%s, %s)
                   ON CONFLICT (tape_label, tape_path) DO NOTHING""",
                (tape_label, tape_path),
            )
        bundle_ids = {}
        if bundle_paths:
            placeholders = ", ".join(["(%s, %s)"] * len(bundle_paths))
            params = []
            for tape_label, tape_path in bundle_paths:
                params.extend([tape_label, tape_path])
            rows = conn.execute(
                f"""WITH wanted(tape_label, tape_path) AS (VALUES {placeholders})
                    SELECT b.tape_label, b.tape_path, b.bundle_id
                    FROM archive_bundles AS b
                    JOIN wanted AS w
                      ON w.tape_label = b.tape_label
                     AND w.tape_path = b.tape_path""",
                params,
            ).fetchall()
            bundle_ids = {
                (row["tape_label"], row["tape_path"]): row["bundle_id"]
                for row in rows
            }

        now = _now_utc()
        run_specs = {}
        for record in records:
            if record.get("archive_run_id") is not None:
                continue
            backup_date = _as_utc(record.get("backup_date") or now)
            tape_label = record.get("tape_label") or ""
            run_label = f"{str(backup_date)[:10]}:{tape_label}"
            local_session_id = record.get("local_session_id")
            kind = "local" if local_session_id is not None else "remote"
            # Only the matching typed column is populated; the FK guarantees the
            # reference is valid and the CHECK keeps it consistent with `kind`.
            # (Remote runs currently carry no session id at catalog time.)
            run_specs[(run_label, tape_label)] = (
                run_label, tape_label, kind, local_session_id, None,
                backup_date, backup_date)
        for spec in run_specs.values():
            conn.execute(
                """INSERT INTO archive_runs
                   (run_label, tape_label, session_kind,
                    local_session_id, remote_session_id,
                    started_at, completed_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (run_label, tape_label) DO NOTHING""",
                spec,
            )
        run_ids = {
            key: conn.execute(
                """SELECT run_id FROM archive_runs
                   WHERE run_label=%s AND tape_label=%s""",
                key,
            ).fetchone()["run_id"]
            for key in run_specs
        }

        dir_specs, dir_targets = self._collect_directory_specs(records)
        resolved_dirs = self._ensure_directories(conn, dir_specs)

        normalized = {}
        for idx, record in enumerate(records):
            canonical = record.get("canonical_source_path")
            if canonical is not None:
                canonical = str(canonical)
                if not canonical.startswith("/") or "\\" in canonical:
                    raise RuntimeError(
                        "[DB] Remote catalog records require an absolute POSIX "
                        f"canonical SOURCE path, got: {canonical}"
                    )
            original_path = canonical or record.get("original_path") or ""
            tape_label = record.get("tape_label") or ""
            source_host = _short_source_host(
                record.get("source_host") or LEGACY_DEFAULT_SOURCE_HOST)
            session_id = record.get("local_session_id")
            chunk_index = record.get("local_chunk_index")
            key = _file_record_key(
                original_path, tape_label, session_id, chunk_index, source_host)
            container = record.get("container_name")
            bundle_id = bundle_ids.get((tape_label, container))
            if record.get("is_packed") and bundle_id is None:
                raise RuntimeError(
                    f"[DB] Packed file has no archive bundle: {container}")
            backup_date = _as_utc(record.get("backup_date") or now)
            run_label = f"{str(backup_date)[:10]}:{tape_label}"
            archive_run_id = record.get("archive_run_id") or run_ids[
                (run_label, tape_label)]
            directory_id = resolved_dirs[dir_targets[idx]]
            normalized[key] = {
                "original_path": original_path,
                "file_size_bytes": int(record.get("file_size_bytes") or 0),
                "tape_label": tape_label,
                "source_host": source_host,
                "is_packed": bool(record.get("is_packed")),
                "stored_path": record.get("stored_path") or "",
                "local_session_id": session_id,
                "local_chunk_index": chunk_index,
                "bundle_id": bundle_id,
                "record_key": key,
                "archive_run_id": archive_run_id,
                "directory_id": directory_id,
                "catalog_name": catalog_file_name(
                    record.get("stored_path"), original_path),
                "catalog_backup_date": backup_date,
            }
        return normalized

    def _bulk_upsert_batch(self, conn, records, update_existing):
        normalized_by_key = self._normalize_file_records(conn, records)
        total = len(normalized_by_key)
        columns = (
            "original_path", "file_size_bytes", "tape_label", "source_host",
            "is_packed", "stored_path", "local_session_id", "local_chunk_index",
            "bundle_id", "record_key", "archive_run_id", "directory_id",
            "catalog_name", "catalog_backup_date",
        )
        col_sql = ", ".join(columns)
        update_sql = ", ".join(
            f"{column}=EXCLUDED.{column}"
            for column in columns if column != "record_key"
        )
        conn.execute(
            "CREATE TEMP TABLE _stage ON COMMIT DROP AS "
            f"SELECT {col_sql} FROM files_index WITH NO DATA"
        )
        with conn.cursor() as cur:
            copy_rows(cur, "_stage", columns, (
                [row[column] for column in columns]
                for row in normalized_by_key.values()
            ))
        conflict = (
            f"DO UPDATE SET {update_sql}" if update_existing else "DO NOTHING")
        # RETURNING (xmax = 0) distinguishes freshly inserted rows (xmax 0) from
        # updated ones without a second membership scan. With DO NOTHING, only
        # inserted rows are returned, so anything not returned was a skip.
        affected = conn.execute(
            f"""INSERT INTO files_index ({col_sql})
                SELECT {col_sql} FROM _stage
                ON CONFLICT (record_key) {conflict}
                RETURNING (xmax = 0) AS inserted"""
        ).fetchall()
        inserted = sum(1 for row in affected if row["inserted"])
        if update_existing:
            return {
                "inserted": inserted,
                "updated": len(affected) - inserted,
                "skipped": 0,
            }
        return {
            "inserted": inserted,
            "updated": 0,
            "skipped": total - inserted,
        }

    def bulk_upsert_files(self, records: Iterable[FileRecord],
                          batch_size=DB_UPSERT_BATCH_SIZE,
                          update_existing=True):
        totals = {"inserted": 0, "updated": 0, "skipped": 0}
        batch = []
        registered = set()

        def flush(items):
            if not items:
                return

            def operation(conn):
                labels = {item.get("tape_label") for item in items}
                found = set()
                missing = []
                for label in labels - registered:
                    row = conn.execute(
                        "SELECT 1 FROM tapes WHERE volume_label = %s",
                        (label,),
                    ).fetchone()
                    if row:
                        found.add(label)
                    else:
                        missing.append(label)
                if missing:
                    raise RuntimeError(
                        f"[DB] Cannot index files for unregistered tape(s): {missing}")
                return self._bulk_upsert_batch(conn, items, update_existing), found

            stats, found = self._transaction(
                operation, f"file catalog batch ({len(items):,} rows)")
            registered.update(found)
            for key in totals:
                totals[key] += stats[key]

        for record in records:
            if CANCEL.is_set():
                raise RuntimeError("file catalog sync cancelled")
            batch.append(record)
            if len(batch) >= max(1, batch_size):
                flush(batch)
                batch = []
        flush(batch)
        return totals

    def file_record_exists(self, original_path, tape_label, local_session_id=None,
                           local_chunk_index=None,
                           source_host=LEGACY_DEFAULT_SOURCE_HOST):
        source_host = _short_source_host(
            source_host or LEGACY_DEFAULT_SOURCE_HOST)
        key = _file_record_key(
            original_path, tape_label, local_session_id, local_chunk_index,
            source_host)
        with self._pool.connection() as conn:
            return bool(conn.execute(
                "SELECT 1 FROM files_index WHERE record_key=%s", (key,)
            ).fetchone())

    def insert_file(self, file_name, original_path, file_size_bytes,
                    tape_label, is_packed, container_name, stored_path,
                    local_session_id=None, local_chunk_index=None,
                    source_host="local"):
        stats = self.bulk_upsert_files([{
            "file_name": file_name,
            "original_path": original_path,
            "file_size_bytes": file_size_bytes,
            "tape_label": tape_label,
            "source_host": source_host,
            "is_packed": is_packed,
            "container_name": container_name,
            "stored_path": stored_path,
            "local_session_id": local_session_id,
            "local_chunk_index": local_chunk_index,
        }])
        return bool(stats["inserted"])

    def search_files(self, name_query=None, date_from=None, date_to=None,
                     limit=None, offset=None, source_host=None, after_id=None):
        return self.search_catalog(
            name_query=name_query, date_from=date_from, date_to=date_to,
            limit=limit, offset=offset, source_host=source_host,
            after_id=after_id)

    @staticmethod
    def _catalog_filter(name_query=None, tape_label=None, date_from=None,
                        date_to=None, source_host=None):
        """Build a WHERE clause + params shared by search and count queries.

        ``name_query`` honours ``*``/``?`` wildcards; every other character
        (notably ``_``) is matched literally via ``ESCAPE '\\'``.
        """
        where = ["1=1"]
        params = []
        if name_query:
            pattern = contains_pattern(name_query)
            where.append("(f.catalog_name ILIKE %s ESCAPE '\\' "
                         "OR f.original_path ILIKE %s ESCAPE '\\')")
            params.extend([pattern, pattern])
        if date_from:
            where.append("f.catalog_backup_date >= %s::date")
            params.append(date_from)
        if date_to:
            where.append("f.catalog_backup_date < (%s::date + INTERVAL '1 day')")
            params.append(date_to)
        if tape_label:
            where.append("f.tape_label = %s")
            params.append(tape_label)
        if source_host:
            where.append("f.source_host = %s")
            params.append(_short_source_host(source_host))
        return where, params

    def search_catalog(self, name_query=None, tape_label=None,
                       date_from=None, date_to=None, limit=None, offset=None,
                       source_host=None, after_id=None):
        where, params = self._catalog_filter(
            name_query, tape_label, date_from, date_to, source_host)
        if after_id is not None:
            # Keyset pagination for bulk consumers (restore-all): OFFSET paging
            # rescans every skipped row, which is O(n^2) over a large result.
            where.append("f.file_id > %s")
            params.append(int(after_id))
        sql = self._catalog_select() + " WHERE " + " AND ".join(where)
        sql += (" ORDER BY f.file_id" if after_id is not None
                else " ORDER BY f.original_path, f.catalog_name")
        if limit is not None:
            sql += " LIMIT %s"
            params.append(int(limit))
            if offset is not None and after_id is None:
                sql += " OFFSET %s"
                params.append(int(offset))
        with self._pool.connection() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._hydrate_file_row(row) for row in rows]

    def count_search_files(self, name_query=None, date_from=None, date_to=None,
                           source_host=None):
        where, params = self._catalog_filter(
            name_query, date_from=date_from, date_to=date_to,
            source_host=source_host)
        sql = "SELECT COUNT(*) AS n FROM files_index f WHERE " + " AND ".join(where)
        with self._pool.connection() as conn:
            return conn.execute(sql, params).fetchone()["n"]

    def get_file_by_id(self, file_id):
        rows = self._catalog_rows("f.file_id = %s", (file_id,))
        return rows[0] if rows else None

    def search_by_directory(self, dir_path, limit=None, offset=None,
                            source_host=None, after_id=None, tape_label=None):
        needle = dir_path.strip().rstrip("/\\")
        if not needle:
            return []
        where = ("(f.original_path ILIKE %s ESCAPE '\\' "
                 "OR f.original_path ILIKE %s ESCAPE '\\')")
        params: List[Any] = [prefix_pattern(needle), substring_pattern(needle)]
        if source_host:
            where += " AND f.source_host = %s"
            params.append(_short_source_host(source_host))
        if tape_label:
            where += " AND f.tape_label = %s"
            params.append(tape_label)
        if after_id is not None:
            where += " AND f.file_id > %s"
            params.append(int(after_id))
            sql = self._catalog_select() + " WHERE " + where
            sql += " ORDER BY f.file_id LIMIT %s"
            params.append(int(limit or 250))
            with self._pool.connection() as conn:
                rows = conn.execute(sql, params).fetchall()
            return [self._hydrate_file_row(row) for row in rows]
        if limit is None:
            return self._catalog_rows(where, params, "f.original_path")
        sql = self._catalog_select() + " WHERE " + where
        sql += " ORDER BY f.original_path LIMIT %s OFFSET %s"
        params.extend([int(limit), int(offset or 0)])
        with self._pool.connection() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._hydrate_file_row(row) for row in rows]

    def count_by_directory(self, dir_path, source_host=None):
        needle = dir_path.strip().rstrip("/\\")
        if not needle:
            return 0
        where = ("(f.original_path ILIKE %s ESCAPE '\\' "
                 "OR f.original_path ILIKE %s ESCAPE '\\')")
        params: List[Any] = [prefix_pattern(needle), substring_pattern(needle)]
        if source_host:
            where += " AND f.source_host = %s"
            params.append(_short_source_host(source_host))
        with self._pool.connection() as conn:
            return conn.execute(
                "SELECT COUNT(*) AS n FROM files_index f WHERE " + where,
                params,
            ).fetchone()["n"]

    def list_backup_sessions(self):
        with self._pool.connection() as conn:
            return _rows(conn.execute("""
                SELECT DATE(f.catalog_backup_date) AS session_date,
                       f.tape_label, COUNT(*) AS file_count,
                       SUM(f.file_size_bytes) AS total_bytes
                FROM files_index f
                GROUP BY DATE(f.catalog_backup_date), f.tape_label
                ORDER BY session_date DESC
            """).fetchall())

    def search_by_session(self, session_date, tape_label, limit=None,
                          offset=None, after_id=None):
        where = """f.catalog_backup_date >= %s::date
                   AND f.catalog_backup_date < (%s::date + INTERVAL '1 day')
                   AND f.tape_label = %s"""
        params = [session_date, session_date, tape_label]
        if after_id is not None:
            where += " AND f.file_id > %s"
            params.append(int(after_id))
            sql = self._catalog_select() + " WHERE " + where
            sql += " ORDER BY f.file_id LIMIT %s"
            params.append(int(limit or 250))
            with self._pool.connection() as conn:
                rows = conn.execute(sql, params).fetchall()
            return [self._hydrate_file_row(row) for row in rows]
        if limit is None:
            return self._catalog_rows(where, params, "f.original_path")
        sql = self._catalog_select() + " WHERE " + where
        sql += " ORDER BY f.original_path LIMIT %s OFFSET %s"
        params.extend([int(limit), int(offset or 0)])
        with self._pool.connection() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._hydrate_file_row(row) for row in rows]

    def count_by_session(self, session_date, tape_label):
        with self._pool.connection() as conn:
            return conn.execute(
                """SELECT COUNT(*) AS n
                   FROM files_index f
                   WHERE f.catalog_backup_date >= %s::date
                     AND f.catalog_backup_date < (%s::date + INTERVAL '1 day')
                     AND f.tape_label = %s""",
                (session_date, session_date, tape_label),
            ).fetchone()["n"]

    def list_source_hosts(self):
        with self._pool.connection() as conn:
            rows = conn.execute(
                """SELECT DISTINCT COALESCE(source_host,'so02') AS source_host
                   FROM files_index
                   ORDER BY source_host"""
            ).fetchall()
        return [row["source_host"] for row in rows if row["source_host"]]

    def delete_files(self, file_ids):
        """Delete file records in ONE transaction and reconcile used_space.

        Replaces the per-row delete loop (one transaction per record) and fixes
        the ``tapes.used_space`` drift a single-record delete used to leave
        behind until the next full recalculation.
        """
        ids = sorted({int(file_id) for file_id in file_ids})
        if not ids:
            return 0

        def operation(conn):
            rows = conn.execute(
                "DELETE FROM files_index WHERE file_id = ANY(%s) "
                "RETURNING tape_label",
                (ids,),
            ).fetchall()
            labels = sorted({row["tape_label"] for row in rows})
            if labels:
                conn.execute(
                    """UPDATE tapes t
                       SET used_space = COALESCE(
                           (SELECT SUM(f.file_size_bytes) FROM files_index f
                            WHERE f.tape_label = t.volume_label), 0)
                       WHERE t.volume_label = ANY(%s)""",
                    (labels,),
                )
            return len(rows)

        return self._transaction(
            operation, f"delete {len(ids)} file record(s)")

    def delete_file(self, file_id):
        removed = self.delete_files([file_id])
        if not removed:
            raise RuntimeError(f"[DB] File record not found: {file_id}")
