"""File and directory catalog method group."""
from collections import defaultdict
import gzip
import hashlib
import io
import json
import os
import posixpath
from typing import Any, Iterable, List

from .catalog_query import contains_pattern, prefix_pattern, substring_pattern
from .catalog_v3 import catalog_directory_chain, catalog_file_name
from .constants import DB_UPSERT_BATCH_SIZE, LEGACY_DEFAULT_SOURCE_HOST
from .db import _derived_file_name, _file_record_key, _short_source_host
from .pg_bulk import copy_rows
from .pg_core import _as_utc, _now_utc, _rows
from .pipeline_types import FileRecord
from .runtime import CANCEL

try:
    import zstandard as zstd
except ImportError:  # pragma: no cover - requirements includes zstandard
    zstd = None


def _catalog_hash(*values):
    digest = hashlib.sha256()
    for value in values:
        raw = str("" if value is None else value).encode(
            "utf-8", errors="surrogatepass")
        digest.update(len(raw).to_bytes(8, "big"))
        digest.update(raw)
    return digest.digest()


def _norm_source_path(value):
    value = str(value or "").replace("\\", "/")
    value = posixpath.normpath(value)
    return "" if value == "." else value


def _dirname(value):
    value = _norm_source_path(value)
    if not value:
        return ""
    parent = posixpath.dirname(value)
    return parent if parent and parent != "." else ""


def _basename(value):
    value = _norm_source_path(value)
    if not value or value == "/":
        return value or ""
    return posixpath.basename(value)


def _ancestors(dir_path):
    path = _norm_source_path(dir_path)
    if not path:
        return []
    parts = [part for part in path.strip("/").split("/") if part]
    if path.startswith("/"):
        current = ""
        out = ["/"]
        for part in parts:
            current = current + "/" + part
            out.append(current)
        return out
    current = ""
    out = []
    for part in parts:
        current = part if not current else current + "/" + part
        out.append(current)
    return out


def _common_parent(paths):
    parents = [_dirname(path) for path in paths if path]
    if not parents:
        return ""
    try:
        return posixpath.commonpath(parents)
    except ValueError:
        return parents[0]


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
        item["container_id"] = item.get("restore_container_id")
        item["container_format"] = item.get("restore_container_format")
        item["format_version"] = item.get("restore_format_version")
        item["tar_dialect"] = item.get("restore_tar_dialect")
        item["member_name"] = (item.get("stored_path")
                               if item.get("container_format") else None)
        item["container_name"] = item.get("tape_container_locator")
        item["source_base_path"] = PgCatalogMixin._source_base_path_for_member(
            item.get("original_path"), item.get("member_name"))
        return item

    @staticmethod
    def _source_base_path_for_member(original_path, member_name):
        original = str(original_path or "").replace("\\", "/")
        member = str(member_name or "").replace("\\", "/").lstrip("/")
        if member and original.endswith(member):
            return original[:-len(member)].rstrip("/")
        return _dirname(original)

    @staticmethod
    def _catalog_select(container_schema=True):
        if not container_schema:
            return """SELECT f.*, b.tape_path AS bundle_tape_path,
                         r.started_at AS run_started_at,
                         NULL::BIGINT AS restore_container_id,
                         CASE WHEN f.is_packed AND b.bundle_id IS NOT NULL
                              THEN 'zip' ELSE NULL END
                             AS restore_container_format,
                         CASE WHEN f.is_packed AND b.bundle_id IS NOT NULL
                              THEN 'legacy-zip-v1' ELSE NULL END
                             AS restore_format_version,
                         NULL::TEXT AS restore_tar_dialect,
                         NULL::TEXT AS local_container_locator,
                         b.tape_path AS tape_container_locator,
                         NULL::BIGINT AS tape_generation_id,
                         NULL::INTEGER AS tape_generation,
                         NULL::BIGINT AS sidecar_artifact_id,
                         NULL::TEXT AS sidecar_artifact_version,
                         NULL::TEXT AS local_sidecar_locator,
                         NULL::TEXT AS tape_sidecar_locator,
                         NULL::BIGINT AS expected_member_count,
                         NULL::BIGINT AS expected_logical_bytes
                  FROM files_index AS f
                  LEFT JOIN archive_bundles AS b ON b.bundle_id = f.bundle_id
                  LEFT JOIN archive_runs AS r ON r.run_id = f.archive_run_id"""
        return """SELECT f.*, b.tape_path AS bundle_tape_path,
                         r.started_at AS run_started_at,
                         c.container_id AS restore_container_id,
                         CASE
                           WHEN c.container_id IS NOT NULL
                             THEN c.container_format
                           WHEN f.is_packed AND b.bundle_id IS NOT NULL
                             THEN 'zip'
                           ELSE NULL
                         END AS restore_container_format,
                         CASE
                           WHEN c.container_id IS NOT NULL THEN c.format_version
                           WHEN f.is_packed AND b.bundle_id IS NOT NULL
                             THEN 'legacy-zip-v1'
                           ELSE NULL
                         END AS restore_format_version,
                         c.tar_dialect AS restore_tar_dialect,
                         c.temporary_data_locator AS local_container_locator,
                         COALESCE(c.tape_path, b.tape_path)
                             AS tape_container_locator,
                         COALESCE(c.tape_generation_id, r.tape_generation_id)
                             AS tape_generation_id,
                         tg.generation AS tape_generation,
                         sidecar.artifact_id AS sidecar_artifact_id,
                         sidecar.artifact_version AS sidecar_artifact_version,
                         COALESCE(sidecar.local_locator,
                                  c.permanent_local_metadata_locator)
                             AS local_sidecar_locator,
                         sidecar.tape_locator AS tape_sidecar_locator,
                         c.expected_member_count,
                         c.expected_logical_bytes
                  FROM files_index AS f
                  LEFT JOIN archive_bundles AS b ON b.bundle_id = f.bundle_id
                  LEFT JOIN archive_runs AS r ON r.run_id = f.archive_run_id
                  LEFT JOIN archive_containers AS c
                    ON c.container_id = b.container_id
                  LEFT JOIN tape_generations AS tg
                    ON tg.generation_id = COALESCE(
                        c.tape_generation_id, r.tape_generation_id)
                  LEFT JOIN LATERAL (
                    SELECT a.artifact_id, a.artifact_version,
                           a.local_locator, a.tape_locator
                    FROM archive_artifacts a
                    WHERE a.container_id=c.container_id
                      AND a.artifact_kind='tar_sidecar'
                      AND a.readiness_state='ready'
                    ORDER BY a.artifact_id DESC LIMIT 1
                  ) sidecar ON TRUE"""

    def _container_restore_schema_available(self, conn):
        return all(self._table_exists_conn(conn, name) for name in (
            "archive_containers", "archive_artifacts", "tape_generations"))

    def _catalog_rows(self, where="", params=(), order_by=""):
        with self._pool.connection() as conn:
            sql = self._catalog_select(
                self._container_restore_schema_available(conn))
            if where:
                sql += " WHERE " + where
            if order_by:
                sql += " ORDER BY " + order_by
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
        bundle_paths = {}
        for record in records:
            if not record.get("is_packed") or not record.get("container_name"):
                continue
            key = (record["tape_label"], record.get("container_name"))
            identity = (record.get("container_id"),
                        record.get("container_format") or "zip")
            previous = bundle_paths.setdefault(key, identity)
            if previous != identity:
                raise RuntimeError(
                    "packed records disagree on archive container identity")
        has_bundle_container_link = self._column_exists_conn(
            conn, "archive_bundles", "container_id")
        for (tape_label, tape_path), (container_id, container_format) in \
                bundle_paths.items():
            if has_bundle_container_link:
                conn.execute(
                    """INSERT INTO archive_bundles
                           (tape_label, tape_path, container_id, container_format)
                       VALUES (%s, %s, %s, %s)
                       ON CONFLICT (tape_label, tape_path) DO UPDATE
                       SET container_id=EXCLUDED.container_id,
                           container_format=EXCLUDED.container_format
                       WHERE archive_bundles.container_id IS NULL""",
                    (tape_label, tape_path, container_id, container_format),
                )
            else:
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
            identity_columns = (", b.container_id, b.container_format"
                                if has_bundle_container_link else "")
            rows = conn.execute(
                f"""WITH wanted(tape_label, tape_path) AS (VALUES {placeholders})
                    SELECT b.tape_label, b.tape_path, b.bundle_id
                           {identity_columns}
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
            if has_bundle_container_link:
                for row in rows:
                    expected = bundle_paths[
                        (row["tape_label"], row["tape_path"])]
                    if ((row["container_id"], row["container_format"])
                            != expected):
                        raise RuntimeError(
                            "archive_bundles identity conflicts with its container")

        now = _now_utc()
        run_specs = {}
        for record in records:
            if record.get("archive_run_id") is not None:
                continue
            backup_date = _as_utc(record.get("backup_date") or now)
            tape_label = record.get("tape_label") or ""
            local_session_id = record.get("local_session_id")
            remote_session_id = record.get("remote_session_id")
            remote_chunk_index = record.get("remote_chunk_index")
            tape_generation_id = record.get("tape_generation_id")
            if (remote_session_id is not None
                    and remote_chunk_index is not None
                    and tape_generation_id is not None):
                session = conn.execute(
                    "SELECT session_label FROM remote_sessions "
                    "WHERE session_id=%s", (int(remote_session_id),)
                ).fetchone()
                if session is None:
                    raise RuntimeError(
                        f"remote session {remote_session_id} does not exist")
                run_label = (
                    f"{session['session_label']}:chunk-"
                    f"{int(remote_chunk_index) + 1}:generation-"
                    f"{int(tape_generation_id)}")
                key = ("remote", int(remote_session_id),
                       int(remote_chunk_index), int(tape_generation_id))
                run_specs[key] = (
                    run_label, tape_label, "remote", None,
                    int(remote_session_id), int(remote_chunk_index),
                    int(tape_generation_id), backup_date, backup_date)
            else:
                run_label = f"{str(backup_date)[:10]}:{tape_label}"
                kind = "local" if local_session_id is not None else "remote"
                key = ("legacy", run_label, tape_label)
                run_specs[key] = (
                    run_label, tape_label, kind, local_session_id,
                    remote_session_id, None, None, backup_date, backup_date)
        run_ids = self._ensure_archive_runs(conn, run_specs)

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
            remote_session_id = record.get("remote_session_id")
            remote_chunk_index = record.get("remote_chunk_index")
            key = _file_record_key(
                original_path, tape_label, session_id, chunk_index, source_host,
                remote_session_id=remote_session_id,
                remote_chunk_index=remote_chunk_index)
            container = record.get("container_name")
            bundle_id = bundle_ids.get((tape_label, container))
            if record.get("is_packed") and bundle_id is None:
                raise RuntimeError(
                    f"[DB] Packed file has no archive bundle: {container}")
            backup_date = _as_utc(record.get("backup_date") or now)
            if record.get("archive_run_id") is not None:
                archive_run_id = record["archive_run_id"]
            elif (remote_session_id is not None
                    and remote_chunk_index is not None
                    and record.get("tape_generation_id") is not None):
                archive_run_id = run_ids[
                    ("remote", int(remote_session_id),
                     int(remote_chunk_index),
                     int(record["tape_generation_id"]))]
            else:
                run_label = f"{str(backup_date)[:10]}:{tape_label}"
                archive_run_id = run_ids[("legacy", run_label, tape_label)]
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
                "remote_session_id": remote_session_id,
                "remote_chunk_index": remote_chunk_index,
                "bundle_id": bundle_id,
                "record_key": key,
                "archive_run_id": archive_run_id,
                "directory_id": directory_id,
                "catalog_name": catalog_file_name(
                    record.get("stored_path"), original_path),
                "catalog_backup_date": backup_date,
            }
        return normalized

    def _ensure_archive_runs(self, conn, run_specs):
        has_remote_identity = self._column_exists_conn(
            conn, "archive_runs", "remote_chunk_index")
        for spec in run_specs.values():
            if has_remote_identity:
                conn.execute(
                    """INSERT INTO archive_runs
                       (run_label, tape_label, session_kind,
                        local_session_id, remote_session_id,
                        remote_chunk_index, tape_generation_id,
                        started_at, completed_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT DO NOTHING""",
                    spec,
                )
            else:
                legacy_spec = spec[:5] + spec[-2:]
                conn.execute(
                    """INSERT INTO archive_runs
                       (run_label, tape_label, session_kind,
                        local_session_id, remote_session_id,
                        started_at, completed_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (run_label, tape_label) DO NOTHING""",
                    legacy_spec,
                )
        result = {}
        for key, spec in run_specs.items():
            if key[0] == "remote" and has_remote_identity:
                row = conn.execute(
                    """SELECT run_id FROM archive_runs
                       WHERE remote_session_id=%s AND remote_chunk_index=%s
                         AND tape_generation_id=%s""", key[1:]).fetchone()
            else:
                row = conn.execute(
                    """SELECT run_id FROM archive_runs
                       WHERE run_label=%s AND tape_label=%s""",
                    key[1:]).fetchone()
            if row is None:
                raise RuntimeError("archive run upsert lost its identity")
            result[key] = row["run_id"]
        return result

    def _bulk_upsert_batch(self, conn, records, update_existing):
        normalized_by_key = self._normalize_file_records(conn, records)
        total = len(normalized_by_key)
        columns = (
            "original_path", "file_size_bytes", "tape_label", "source_host",
            "is_packed", "stored_path", "local_session_id", "local_chunk_index",
            "remote_session_id", "remote_chunk_index",
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

    def bulk_upsert_directory_catalog(
            self, records: Iterable[FileRecord], tape_label, source_host,
            *, local_session_id=None, local_chunk_index=None,
            remote_session_id=None, remote_chunk_index=None,
            tape_generation_id=None, archive_run_id=None,
            tape_root="", backup_date=None,
            index_min_file_mb=10, update_existing=True):
        """Record bundle-level and directory-level catalog rows.

        ``records`` is the packer metadata for one staged write. Only packed
        records participate here: loose files remain represented by
        ``files_index`` and legacy ``catalog_directories``.
        """
        packed = [dict(record) for record in records if record.get("is_packed")]
        if not packed:
            return {"bundles": 0, "stats": 0, "tree_rows": 0}
        self._require_directory_catalog_schema()

        threshold_bytes = int(float(index_min_file_mb or 10) * 1024 * 1024)
        backup_date = _as_utc(backup_date or _now_utc())
        source_host = _short_source_host(source_host or LEGACY_DEFAULT_SOURCE_HOST)

        def operation(conn):
            if not conn.execute(
                    "SELECT 1 FROM tapes WHERE volume_label=%s",
                    (tape_label,)).fetchone():
                raise RuntimeError(
                    f"[DB] Cannot index directory catalog for unregistered "
                    f"tape: {tape_label}")

            resolved_run_id = archive_run_id
            if resolved_run_id is None:
                run_label = f"{str(backup_date)[:10]}:{tape_label}"
                kind = "local" if local_session_id is not None else "remote"
                key = ("legacy", run_label, tape_label)
                run_specs = {key: (
                    run_label, tape_label, kind, local_session_id,
                    remote_session_id, None, None,
                    backup_date, backup_date)}
                resolved_run_id = self._ensure_archive_runs(
                    conn, run_specs)[key]
            catalog_chunk_index = (remote_chunk_index
                                   if remote_session_id is not None
                                   else local_chunk_index)
            has_dir_container_link = self._column_exists_conn(
                conn, "directory_archive_bundles", "container_id")

            by_bundle = defaultdict(list)
            for record in packed:
                by_bundle[record.get("container_name") or ""].append(record)

            bundle_rows = []
            for container, items in sorted(by_bundle.items()):
                if not container:
                    continue
                paths = [
                    item.get("canonical_source_path") or item.get("original_path")
                    for item in items
                ]
                if remote_session_id is not None:
                    if any(not str(path or "").startswith("/")
                           or "\\" in str(path or "") for path in paths):
                        raise RuntimeError(
                            "remote directory catalog requires canonical "
                            "absolute POSIX source paths")
                    original_root = (_common_parent(paths)
                                     or _dirname(paths[0]))
                else:
                    original_root = (
                        items[0].get("original_root_dir")
                        or _common_parent(paths)
                        or _dirname(paths[0])
                    )
                stored_bundle_path = (
                    container if (":" in container or container.startswith("\\\\"))
                    else str(tape_root and os.path.join(
                        tape_root, container) or container)
                )
                manifest_name = next(
                    (item.get("manifest_name") for item in items
                     if item.get("manifest_name")), None)
                manifest_path = next(
                    (item.get("manifest_path") for item in items
                     if item.get("manifest_path")), None)
                if manifest_name and tape_root:
                    manifest_path = os.path.join(tape_root, manifest_name)
                file_count = len(items)
                byte_count = sum(int(item.get("file_size_bytes") or 0)
                                 for item in items)
                small = [
                    item for item in items
                    if int(item.get("file_size_bytes") or 0) < threshold_bytes
                ]
                large = [
                    item for item in items
                    if int(item.get("file_size_bytes") or 0) >= threshold_bytes
                ]
                key = _catalog_hash(
                    "directory_archive_bundle", source_host, tape_label,
                    local_session_id, catalog_chunk_index, remote_session_id,
                    stored_bundle_path, original_root)
                container_ids = {item.get("container_id") for item in items}
                container_formats = {
                    item.get("container_format") for item in items}
                artifact_sizes = {
                    item.get("actual_artifact_bytes") for item in items}
                if (len(container_ids) != 1 or len(container_formats) != 1
                        or len(artifact_sizes) != 1):
                    raise RuntimeError(
                        "directory bundle records disagree on container identity")
                bundle_rows.append({
                    "source_host": source_host,
                    "original_dir_path": original_root,
                    "tape_label": tape_label,
                    "archive_run_id": resolved_run_id,
                    "local_session_id": local_session_id,
                    "remote_session_id": remote_session_id,
                    "chunk_index": catalog_chunk_index,
                    "stored_bundle_path": stored_bundle_path,
                    "manifest_path": manifest_path,
                    "manifest_format": next(
                        (item.get("manifest_format") for item in items
                         if item.get("manifest_format")), "jsonl"),
                    "manifest_compression": next(
                        (item.get("manifest_compression") for item in items
                         if item.get("manifest_compression")), "zstd"),
                    "file_count": file_count,
                    "byte_count": byte_count,
                    "small_file_count": len(small),
                    "small_file_bytes": sum(
                        int(item.get("file_size_bytes") or 0) for item in small),
                    "large_file_count": len(large),
                    "large_file_bytes": sum(
                        int(item.get("file_size_bytes") or 0) for item in large),
                    "backup_date": backup_date,
                    "record_key": key,
                    "container_id": next(iter(container_ids)),
                    "container_format": next(iter(container_formats)),
                    "tape_generation_id": tape_generation_id,
                    "actual_artifact_bytes": next(iter(artifact_sizes)),
                    "_items": items,
                })

            bundle_columns = (
                "source_host", "original_dir_path", "tape_label",
                "archive_run_id", "local_session_id", "remote_session_id",
                "chunk_index", "stored_bundle_path", "manifest_path",
                "manifest_format", "manifest_compression", "file_count",
                "byte_count", "small_file_count", "small_file_bytes",
                "large_file_count", "large_file_bytes", "backup_date",
                "record_key",
            )
            if has_dir_container_link:
                bundle_columns += (
                    "container_id", "container_format",
                    "tape_generation_id", "actual_artifact_bytes",
                )
            conn.execute(
                "CREATE TEMP TABLE _dir_bundles ON COMMIT DROP AS "
                "SELECT " + ", ".join(bundle_columns) +
                " FROM directory_archive_bundles WITH NO DATA"
            )
            with conn.cursor() as cur:
                copy_rows(cur, "_dir_bundles", bundle_columns, (
                    [row[column] for column in bundle_columns]
                    for row in bundle_rows
                ))
            update_sql = ", ".join(
                f"{column}=EXCLUDED.{column}"
                for column in bundle_columns if column != "record_key")
            conflict = (
                f"DO UPDATE SET {update_sql}" if update_existing
                else "DO NOTHING")
            conn.execute(
                "INSERT INTO directory_archive_bundles ("
                + ", ".join(bundle_columns) + ") "
                "SELECT " + ", ".join(bundle_columns) + " FROM _dir_bundles "
                f"ON CONFLICT (record_key) {conflict}"
            )
            db_bundle_ids = {
                row["record_key"]: row["bundle_id"]
                for row in conn.execute(
                    """SELECT bundle_id, record_key
                       FROM directory_archive_bundles
                       WHERE record_key = ANY(%s)""",
                    ([row["record_key"] for row in bundle_rows],),
                ).fetchall()
            }

            stats_rows = []
            tree_rows = []
            for bundle in bundle_rows:
                items = bundle.pop("_items")
                root = bundle["original_dir_path"]
                bundle_id = db_bundle_ids[bundle["record_key"]]
                direct = [
                    item for item in items
                    if _dirname(item.get("canonical_source_path")
                                or item.get("original_path")) == root
                ]
                stats_rows.append((
                    source_host, root, tape_label, resolved_run_id,
                    local_session_id, remote_session_id, catalog_chunk_index,
                    bundle["stored_bundle_path"], len(direct),
                    sum(int(item.get("file_size_bytes") or 0)
                        for item in direct),
                    len(items), bundle["byte_count"],
                    bundle["small_file_count"], bundle["small_file_bytes"],
                    bundle["large_file_count"], bundle["large_file_bytes"],
                    1, backup_date,
                    _catalog_hash(
                        "directory_archive_stats", source_host, tape_label,
                        local_session_id, catalog_chunk_index, remote_session_id,
                        bundle["stored_bundle_path"], root),
                ))

                direct_counts = defaultdict(lambda: [0, 0, 0, 0, 0, 0])
                recursive_counts = defaultdict(lambda: [0, 0, 0, 0, 0, 0])
                for item in items:
                    size = int(item.get("file_size_bytes") or 0)
                    original = (item.get("canonical_source_path")
                                or item.get("original_path"))
                    parent = _dirname(original)
                    is_small = size < threshold_bytes
                    direct_counts[parent][0] += 1
                    direct_counts[parent][1] += size
                    direct_counts[parent][2 if is_small else 4] += 1
                    direct_counts[parent][3 if is_small else 5] += size
                    for ancestor in _ancestors(parent):
                        recursive_counts[ancestor][0] += 1
                        recursive_counts[ancestor][1] += size
                        recursive_counts[ancestor][2 if is_small else 4] += 1
                        recursive_counts[ancestor][3 if is_small else 5] += size

                for dir_path in sorted(recursive_counts):
                    dc = direct_counts[dir_path]
                    rc = recursive_counts[dir_path]
                    parent = None if dir_path == "/" else (_dirname(dir_path) or None)
                    depth = 0 if dir_path == "/" else len(
                        [p for p in dir_path.strip("/").split("/") if p])
                    tree_rows.append((
                        source_host, dir_path, parent, _basename(dir_path),
                        depth, tape_label, resolved_run_id, local_session_id,
                        remote_session_id, catalog_chunk_index, bundle_id,
                        bundle["stored_bundle_path"], bundle["manifest_path"],
                        dc[0], dc[1], rc[0], rc[1],
                        dc[2], dc[3], rc[2], rc[3],
                        dc[4], dc[5], rc[4], rc[5],
                        backup_date,
                        _catalog_hash(
                            "directory_tree_index", source_host, tape_label,
                            local_session_id, catalog_chunk_index,
                            remote_session_id, bundle["stored_bundle_path"],
                            dir_path),
                    ))

            stats_columns = (
                "source_host", "original_dir_path", "tape_label",
                "archive_run_id", "local_session_id", "remote_session_id",
                "chunk_index", "stored_root_path", "direct_file_count",
                "direct_bytes", "recursive_file_count", "recursive_bytes",
                "small_file_count", "small_file_bytes", "large_file_count",
                "large_file_bytes", "packed_bundle_count", "backup_date",
                "record_key",
            )
            conn.execute(
                "CREATE TEMP TABLE _dir_stats ON COMMIT DROP AS "
                "SELECT " + ", ".join(stats_columns) +
                " FROM directory_archive_stats WITH NO DATA"
            )
            with conn.cursor() as cur:
                copy_rows(cur, "_dir_stats", stats_columns, stats_rows)
            stats_update = ", ".join(
                f"{column}=EXCLUDED.{column}"
                for column in stats_columns if column != "record_key")
            conn.execute(
                "INSERT INTO directory_archive_stats ("
                + ", ".join(stats_columns) + ") "
                "SELECT " + ", ".join(stats_columns) + " FROM _dir_stats "
                "ON CONFLICT (record_key) DO UPDATE SET " + stats_update
            )

            tree_columns = (
                "source_host", "original_dir_path",
                "parent_original_dir_path", "dir_name", "depth",
                "tape_label", "archive_run_id", "local_session_id",
                "remote_session_id", "chunk_index", "bundle_id",
                "stored_bundle_path", "manifest_path", "direct_file_count",
                "direct_bytes", "recursive_file_count", "recursive_bytes",
                "direct_small_file_count", "direct_small_file_bytes",
                "recursive_small_file_count", "recursive_small_file_bytes",
                "direct_large_file_count", "direct_large_file_bytes",
                "recursive_large_file_count", "recursive_large_file_bytes",
                "backup_date", "record_key",
            )
            conn.execute(
                "CREATE TEMP TABLE _dir_tree ON COMMIT DROP AS "
                "SELECT " + ", ".join(tree_columns) +
                " FROM directory_tree_index WITH NO DATA"
            )
            with conn.cursor() as cur:
                copy_rows(cur, "_dir_tree", tree_columns, tree_rows)
            tree_update = ", ".join(
                f"{column}=EXCLUDED.{column}"
                for column in tree_columns if column != "record_key")
            conn.execute(
                "INSERT INTO directory_tree_index ("
                + ", ".join(tree_columns) + ") "
                "SELECT " + ", ".join(tree_columns) + " FROM _dir_tree "
                "ON CONFLICT (record_key) DO UPDATE SET " + tree_update
            )
            return {
                "bundles": len(bundle_rows),
                "stats": len(stats_rows),
                "tree_rows": len(tree_rows),
            }

        return self._transaction(
            operation, f"directory catalog batch ({len(packed):,} records)")

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
        with self._pool.connection() as conn:
            sql = self._catalog_select(
                self._container_restore_schema_available(conn))
            sql += " WHERE " + " AND ".join(where)
            sql += (" ORDER BY f.file_id" if after_id is not None
                    else " ORDER BY f.original_path, f.catalog_name")
            if limit is not None:
                sql += " LIMIT %s"
                params.append(int(limit))
                if offset is not None and after_id is None:
                    sql += " OFFSET %s"
                    params.append(int(offset))
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
            params.append(int(limit or 250))
            with self._pool.connection() as conn:
                sql = self._catalog_select(
                    self._container_restore_schema_available(conn))
                sql += " WHERE " + where
                sql += " ORDER BY f.file_id LIMIT %s"
                rows = conn.execute(sql, params).fetchall()
            return [self._hydrate_file_row(row) for row in rows]
        if limit is None:
            return self._catalog_rows(where, params, "f.original_path")
        params.extend([int(limit), int(offset or 0)])
        with self._pool.connection() as conn:
            sql = self._catalog_select(
                self._container_restore_schema_available(conn))
            sql += " WHERE " + where
            sql += " ORDER BY f.original_path LIMIT %s OFFSET %s"
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
            params.append(int(limit or 250))
            with self._pool.connection() as conn:
                sql = self._catalog_select(
                    self._container_restore_schema_available(conn))
                sql += " WHERE " + where
                sql += " ORDER BY f.file_id LIMIT %s"
                rows = conn.execute(sql, params).fetchall()
            return [self._hydrate_file_row(row) for row in rows]
        if limit is None:
            return self._catalog_rows(where, params, "f.original_path")
        params.extend([int(limit), int(offset or 0)])
        with self._pool.connection() as conn:
            sql = self._catalog_select(
                self._container_restore_schema_available(conn))
            sql += " WHERE " + where
            sql += " ORDER BY f.original_path LIMIT %s OFFSET %s"
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

    def list_pruned_manifest_segments(self):
        """Absolute local files approved for post-prune search/restore."""
        with self._pool.connection() as conn:
            if not self._table_exists_conn(conn, "local_manifest_exports"):
                return []
            rows = conn.execute(
                """SELECT e.archive_root, s.manifest_relpath
                   FROM local_manifest_segments s
                   JOIN local_manifest_exports e ON e.export_id=s.export_id
                   WHERE e.status='pruned'""").fetchall()
        return [os.path.abspath(os.path.join(
            row["archive_root"], row["manifest_relpath"])) for row in rows]

    def list_directory_bundles(self, tape_label=None, limit=500):
        self._require_directory_catalog_schema()
        where = []
        params = []
        if tape_label:
            where.append("tape_label=%s")
            params.append(tape_label)
        sql = "SELECT * FROM directory_archive_bundles"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY backup_date DESC, stored_bundle_path LIMIT %s"
        params.append(int(limit or 500))
        with self._pool.connection() as conn:
            return _rows(conn.execute(sql, params).fetchall())

    def list_directory_stats(self, tape_label=None, limit=500):
        self._require_directory_catalog_schema()
        where = []
        params = []
        if tape_label:
            where.append("tape_label=%s")
            params.append(tape_label)
        sql = "SELECT * FROM directory_archive_stats"
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY backup_date DESC, original_dir_path LIMIT %s"
        params.append(int(limit or 500))
        with self._pool.connection() as conn:
            return _rows(conn.execute(sql, params).fetchall())

    def find_directory_tree(self, original_dir_path, tape_label=None,
                            source_host=None, limit=500):
        self._require_directory_catalog_schema()
        needle = _norm_source_path(original_dir_path)
        where = ["original_dir_path ILIKE %s ESCAPE '\\'"]
        params = [contains_pattern(needle)]
        if tape_label:
            where.append("tape_label=%s")
            params.append(tape_label)
        if source_host:
            where.append("source_host=%s")
            params.append(_short_source_host(source_host))
        sql = (
            "SELECT * FROM directory_tree_index WHERE "
            + " AND ".join(where)
            + " ORDER BY original_dir_path, backup_date DESC LIMIT %s"
        )
        params.append(int(limit or 500))
        with self._pool.connection() as conn:
            return _rows(conn.execute(sql, params).fetchall())

    def find_directory_restore_bundles(self, dir_path, source_host=None,
                                       tape_label=None):
        """Return explicit restore identities for containers covering a route.

        Legacy directory rows without migration-015 linkage are adapted to ZIP
        here.  Linked rows are routed only by ``archive_containers`` truth.
        The persisted canonical bundle root supplies ``source_base_path``;
        ``remote_sessions.remote_path`` is deliberately never consulted.
        """
        self._require_directory_catalog_schema()
        needle = _norm_source_path(dir_path)
        if not needle:
            return []
        where = ["(t.original_dir_path = %s "
                 "OR t.original_dir_path ILIKE %s ESCAPE '\\')"]
        params: List[Any] = [needle, prefix_pattern(needle + "/")]
        if source_host:
            where.append("t.source_host = %s")
            params.append(_short_source_host(source_host))
        if tape_label:
            where.append("t.tape_label = %s")
            params.append(tape_label)
        with self._pool.connection() as conn:
            rows = conn.execute(
                "SELECT DISTINCT t.tape_label, t.stored_bundle_path, "
                "       t.remote_session_id, d.original_dir_path, "
                "       d.container_id AS directory_container_id, "
                "       d.container_format AS directory_container_format, "
                "       d.tape_generation_id AS directory_generation_id, "
                "       c.container_id, c.container_format, c.format_version, "
                "       c.tar_dialect, c.temporary_data_locator, "
                "       c.permanent_local_metadata_locator, c.tape_path, "
                "       c.tape_generation_id, c.expected_member_count, "
                "       c.expected_logical_bytes, tg.generation, "
                "       a.artifact_id, a.artifact_version, a.local_locator, "
                "       a.tape_locator AS sidecar_tape_locator "
                "FROM directory_tree_index t "
                "JOIN directory_archive_bundles d ON d.bundle_id=t.bundle_id "
                "LEFT JOIN archive_containers c ON c.container_id=d.container_id "
                "LEFT JOIN tape_generations tg ON tg.generation_id="
                "COALESCE(c.tape_generation_id,d.tape_generation_id) "
                "LEFT JOIN LATERAL ("
                "  SELECT aa.artifact_id, aa.artifact_version, "
                "         aa.local_locator, aa.tape_locator "
                "  FROM archive_artifacts aa "
                "  WHERE aa.container_id=c.container_id "
                "    AND aa.artifact_kind='tar_sidecar' "
                "    AND aa.readiness_state='ready' "
                "  ORDER BY aa.artifact_id DESC LIMIT 1"
                ") a ON TRUE "
                "WHERE " + " AND ".join(where) +
                " AND t.stored_bundle_path IS NOT NULL",
                params,
            ).fetchall()
            out = []
            for row in rows:
                if (row["directory_container_id"] is not None
                        and row["container_id"] is None):
                    raise RuntimeError(
                        "[DB] Directory restore container linkage is broken; "
                        "refusing a legacy-format guess")
                if (row["container_id"] is None
                        and row["directory_container_format"] not in
                        (None, "zip")):
                    raise RuntimeError(
                        "[DB] Unlinked directory container is not an explicit "
                        "legacy ZIP; refusing restore")
                out.append({
                    "tape_label": row["tape_label"],
                    "stored_bundle_path": row["stored_bundle_path"],
                    "base_path": row["original_dir_path"],
                    "source_base_path": row["original_dir_path"],
                    "container_id": row["container_id"],
                    "container_format": (
                        row["container_format"] if row["container_id"]
                        is not None else "zip"),
                    "format_version": (
                        row["format_version"] if row["container_id"]
                        is not None else "legacy-zip-v1"),
                    "tar_dialect": row["tar_dialect"],
                    "local_container_locator": row["temporary_data_locator"],
                    "tape_container_locator": (
                        row["tape_path"] or row["stored_bundle_path"]),
                    "tape_generation_id": (
                        row["tape_generation_id"]
                        or row["directory_generation_id"]),
                    "tape_generation": row["generation"],
                    "sidecar_artifact_id": row["artifact_id"],
                    "sidecar_artifact_version": row["artifact_version"],
                    "local_sidecar_locator": (
                        row["local_locator"]
                        or row["permanent_local_metadata_locator"]),
                    "tape_sidecar_locator": row["sidecar_tape_locator"],
                    "expected_member_count": row["expected_member_count"],
                    "expected_logical_bytes": row["expected_logical_bytes"],
                })
        return out

    @staticmethod
    def _derive_bundle_base_path(conn, stored_bundle_path, remote_session_id):
        """SOURCE prefix that a bundle's ZIP entry names are relative to.

        Prefer the canonical root already persisted by the directory catalog.
        Legacy rows can still derive it from an indexed packed member.  The
        ``remote_session_id`` argument remains for call compatibility only;
        newline-composite ``remote_sessions.remote_path`` is never a root."""
        row = conn.execute(
            "SELECT original_dir_path FROM directory_archive_bundles "
            "WHERE stored_bundle_path=%s "
            "ORDER BY backup_date DESC LIMIT 1",
            (stored_bundle_path,),
        ).fetchone()
        if row and row["original_dir_path"]:
            return str(row["original_dir_path"]).replace(
                "\\", "/").rstrip("/")
        row = conn.execute(
            "SELECT f.original_path, f.stored_path "
            "FROM files_index f "
            "JOIN archive_bundles b ON b.bundle_id = f.bundle_id "
            "WHERE b.tape_path = %s AND f.is_packed "
            "AND f.original_path <> '' AND f.stored_path <> '' LIMIT 1",
            (stored_bundle_path,),
        ).fetchone()
        if row:
            op = str(row["original_path"]).replace("\\", "/")
            sp = str(row["stored_path"]).replace("\\", "/").lstrip("/")
            if sp and op.endswith(sp):
                return op[:len(op) - len(sp)].rstrip("/")
        return ""

    def validate_directory_catalog(self, tape_label=None):
        self._require_directory_catalog_schema()
        params = []
        tape_filter = ""
        if tape_label:
            tape_filter = "WHERE b.tape_label=%s"
            params.append(tape_label)
        with self._pool.connection() as conn:
            rows = conn.execute(
                f"""SELECT b.bundle_id, b.tape_label, b.stored_bundle_path,
                          b.file_count AS bundle_file_count,
                          b.byte_count AS bundle_bytes,
                          COALESCE(SUM(t.direct_file_count), 0) AS direct_files,
                          COALESCE(SUM(t.direct_bytes), 0) AS direct_bytes,
                          COALESCE(MAX(t.recursive_file_count), 0) AS max_recursive_files,
                          COALESCE(MAX(t.recursive_bytes), 0) AS max_recursive_bytes
                   FROM directory_archive_bundles b
                   LEFT JOIN directory_tree_index t ON t.bundle_id=b.bundle_id
                   {tape_filter}
                   GROUP BY b.bundle_id, b.tape_label, b.stored_bundle_path,
                            b.file_count, b.byte_count
                   ORDER BY b.tape_label, b.stored_bundle_path""",
                params,
            ).fetchall()
        warnings = []
        for row in rows:
            if row["direct_files"] != row["bundle_file_count"]:
                warnings.append({
                    "type": "direct_file_count_mismatch",
                    "bundle_id": row["bundle_id"],
                    "stored_bundle_path": row["stored_bundle_path"],
                    "expected": row["bundle_file_count"],
                    "actual": row["direct_files"],
                })
            if row["direct_bytes"] != row["bundle_bytes"]:
                warnings.append({
                    "type": "direct_byte_count_mismatch",
                    "bundle_id": row["bundle_id"],
                    "stored_bundle_path": row["stored_bundle_path"],
                    "expected": row["bundle_bytes"],
                    "actual": row["direct_bytes"],
                })
        return {"bundles_checked": len(rows), "warnings": warnings}

    @staticmethod
    def _open_manifest_reader(path, compression):
        compression = (compression or "").lower()
        if compression in ("zstd", "zst") or str(path).endswith(".zst"):
            if zstd is None:
                raise RuntimeError(
                    "[DB] zstandard is required to read .jsonl.zst manifests.")
            raw = open(path, "rb")
            reader = zstd.ZstdDecompressor().stream_reader(raw)
            return raw, reader, io.TextIOWrapper(reader, encoding="utf-8")
        if compression == "gzip" or str(path).endswith(".gz"):
            return (gzip.open(path, "rt", encoding="utf-8"),)
        return (open(path, "r", encoding="utf-8"),)

    @staticmethod
    def _close_manifest_reader(handle):
        for item in reversed(handle):
            try:
                item.close()
            except Exception:
                pass

    def search_small_file_manifests(self, query, tape_label=None, limit=100):
        self._require_directory_catalog_schema()
        needle = (query or "").lower()
        if not needle:
            return []
        where = ["manifest_path IS NOT NULL"]
        params = []
        if tape_label:
            where.append("tape_label=%s")
            params.append(tape_label)
        with self._pool.connection() as conn:
            bundles = conn.execute(
                """SELECT bundle_id, tape_label, stored_bundle_path,
                          manifest_path, manifest_compression
                   FROM directory_archive_bundles
                   WHERE """ + " AND ".join(where) +
                " ORDER BY backup_date DESC",
                params,
            ).fetchall()
        matches = []
        for bundle in bundles:
            path = bundle["manifest_path"]
            manifest_drive = os.path.splitdrive(str(path or ""))[0].casefold()
            bundle_drive = os.path.splitdrive(
                str(bundle["stored_bundle_path"] or ""))[0].casefold()
            if manifest_drive == "z:" or (
                    manifest_drive and bundle_drive
                    and manifest_drive == bundle_drive):
                raise RuntimeError(
                    "[DB] Small-file manifest points at LTFS. Local metadata "
                    "is required; refusing to scan tape.")
            if not path or not os.path.exists(path):
                continue
            handle = self._open_manifest_reader(
                path, bundle["manifest_compression"])
            try:
                text = handle[-1]
                for line in text:
                    if len(matches) >= int(limit or 100):
                        return matches
                    try:
                        item = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    haystack = (
                        str(item.get("file_name") or "") + "\n"
                        + str(item.get("relative_path") or "")
                    ).lower()
                    if needle in haystack:
                        hit = dict(item)
                        hit.update({
                            "bundle_id": bundle["bundle_id"],
                            "tape_label": bundle["tape_label"],
                            "stored_bundle_path": bundle["stored_bundle_path"],
                            "manifest_path": path,
                        })
                        matches.append(hit)
            finally:
                self._close_manifest_reader(handle)
        return matches

    def _legacy_directory_bundle_key(self, bundle, rows):
        if not rows:
            return None
        paths = [row["original_path"] for row in rows]
        original_root = _common_parent(paths) or _dirname(paths[0])
        first = rows[0]
        return _catalog_hash(
            "directory_archive_bundle",
            _short_source_host(bundle["source_host"]
                               or LEGACY_DEFAULT_SOURCE_HOST),
            bundle["tape_label"],
            first["local_session_id"],
            first["local_chunk_index"],
            None,
            bundle["tape_path"],
            original_root,
        )

    @staticmethod
    def _legacy_directory_tree_estimate(rows):
        dirs = set()
        for row in rows:
            parent = _dirname(row["original_path"])
            dirs.update(_ancestors(parent))
        return len(dirs)

    def _legacy_unmappable_directory_catalog_counts(self, conn, tape_label=None):
        params = []
        tape_filter = ""
        if tape_label:
            tape_filter = "AND f.tape_label=%s"
            params.append(tape_label)
        row = conn.execute(
            f"""SELECT
                      COUNT(*) FILTER (
                          WHERE f.is_packed AND f.bundle_id IS NULL
                      ) AS packed_without_bundle_id,
                      COUNT(*) FILTER (
                          WHERE f.is_packed AND f.bundle_id IS NOT NULL
                            AND b.bundle_id IS NULL
                      ) AS packed_missing_archive_bundle
               FROM files_index f
               LEFT JOIN archive_bundles b ON b.bundle_id=f.bundle_id
               WHERE 1=1 {tape_filter}""",
            params,
        ).fetchone()
        return {
            "packed_without_bundle_id": row["packed_without_bundle_id"] or 0,
            "packed_missing_archive_bundle": (
                row["packed_missing_archive_bundle"] or 0),
        }

    def backfill_directory_catalog_from_files_index(
            self, tape_label=None, *, dry_run=True, batch_size=100,
            progress=True):
        """Best-effort legacy backfill from existing packed file rows.

        Exact manifest paths cannot be invented for old rows, so this only
        backfills bundle/directory accounting for packed rows that still link
        to ``archive_bundles``.
        """
        self._require_directory_catalog_schema()
        where = ["f.is_packed", "f.bundle_id IS NOT NULL"]
        params = []
        if tape_label:
            where.append("f.tape_label=%s")
            params.append(tape_label)
        with self._pool.connection() as conn:
            bundles = conn.execute(
                """SELECT DISTINCT f.tape_label, f.source_host, f.bundle_id,
                          b.tape_path
                   FROM files_index f
                   JOIN archive_bundles b ON b.bundle_id=f.bundle_id
                   WHERE """ + " AND ".join(where) +
                 " ORDER BY f.tape_label, b.tape_path",
                params,
            ).fetchall()
            unmappable = self._legacy_unmappable_directory_catalog_counts(
                conn, tape_label=tape_label)

        batch_size = max(1, int(batch_size or 100))
        totals = {
            "bundles_seen": len(bundles),
            "bundles_already_present": 0,
            "bundles_pending": 0,
            "bundles_backfilled": 0,
            "file_rows_used": 0,
            "estimated_tree_rows": 0,
            "dry_run": bool(dry_run),
            "batch_size": batch_size,
            "unmappable": unmappable,
            "warnings": [],
        }
        for index, bundle in enumerate(bundles, start=1):
            if progress and (index == 1 or index % batch_size == 0):
                print(f"[DB] Directory backfill progress: "
                      f"{index:,}/{len(bundles):,} bundle(s) scanned")
            with self._pool.connection() as conn:
                rows = conn.execute(
                    """SELECT f.original_path, f.file_size_bytes, f.stored_path,
                              f.catalog_name, f.local_session_id,
                              f.local_chunk_index, f.catalog_backup_date
                       FROM files_index f
                       WHERE f.tape_label=%s AND f.bundle_id=%s
                       ORDER BY f.file_id""",
                     (bundle["tape_label"], bundle["bundle_id"]),
                ).fetchall()
            if not rows:
                continue
            key = self._legacy_directory_bundle_key(bundle, rows)
            with self._pool.connection() as conn:
                exists = bool(conn.execute(
                    """SELECT 1 FROM directory_archive_bundles
                       WHERE record_key=%s""",
                    (key,),
                ).fetchone())
            if exists:
                totals["bundles_already_present"] += 1
                continue
            totals["bundles_pending"] += 1
            totals["file_rows_used"] += len(rows)
            totals["estimated_tree_rows"] += self._legacy_directory_tree_estimate(
                rows)
            if dry_run:
                continue
            records = [{
                "file_name": row["catalog_name"] or _basename(row["stored_path"]),
                "original_path": row["original_path"],
                "file_size_bytes": row["file_size_bytes"],
                "is_packed": True,
                "container_name": bundle["tape_path"],
                "stored_path": row["stored_path"],
                "catalog_policy": "legacy_backfill",
                "manifest_name": None,
                "manifest_path": None,
                "manifest_format": None,
                "manifest_compression": None,
            } for row in rows]
            first = rows[0]
            stats = self.bulk_upsert_directory_catalog(
                records,
                bundle["tape_label"],
                bundle["source_host"],
                local_session_id=first["local_session_id"],
                local_chunk_index=first["local_chunk_index"],
                tape_root="",
                backup_date=first["catalog_backup_date"],
                update_existing=False,
            )
            totals["bundles_backfilled"] += stats["bundles"]

        totals["warnings"].append(
            "Legacy manifest paths cannot be reconstructed unless the old "
            "catalog already recorded them; existing small-file rows were "
            "preserved in files_index.")
        if unmappable["packed_without_bundle_id"]:
            totals["warnings"].append(
                "Some packed legacy files have no bundle_id and cannot be "
                "mapped into directory_archive_bundles.")
        if unmappable["packed_missing_archive_bundle"]:
            totals["warnings"].append(
                "Some packed legacy files reference missing archive_bundles "
                "rows and cannot be mapped exactly.")
        return totals

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
                for label in labels:
                    used = self._calculate_tape_used_space_conn(conn, label)
                    conn.execute(
                        "UPDATE tapes SET used_space=%s WHERE volume_label=%s",
                        (used, label),
                    )
            return len(rows)

        return self._transaction(
            operation, f"delete {len(ids)} file record(s)")

    def delete_file(self, file_id):
        removed = self.delete_files([file_id])
        if not removed:
            raise RuntimeError(f"[DB] File record not found: {file_id}")
