"""Permanent local small-file manifests and guarded hot-catalog pruning.

The compressed JSONL files are the per-file source of truth after pruning.
PostgreSQL retains only the export ledger and folder-level aggregates.  This
module never removes operational session/plan/chunk tables.
"""
from collections import defaultdict
from datetime import datetime, timezone
import fnmatch
import hashlib
import io
import itertools
import json
import os
import posixpath
import re
import tempfile
from typing import Any, TYPE_CHECKING, cast

if TYPE_CHECKING:
    import psycopg
    from psycopg.rows import dict_row
else:
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError:  # pragma: no cover
        psycopg = None
        dict_row = None

try:
    import zstandard as zstd
except ImportError:  # pragma: no cover
    zstd = None

from .pg_bulk import copy_rows, require_psycopg
from .pg_core import PgConnectionCore


SMALL_FILE_THRESHOLD_BYTES = 10 * 1024 * 1024
TERMINAL_REMOTE_CHUNK = "done"
TERMINAL_LOCAL_CHUNK = "backed_up"
DEFAULT_PRUNE_BATCH_SIZE = 100_000


def active_archive_processes():
    """Return local transfer/archive processes, excluding this inspector."""
    try:
        import psutil
    except ImportError as exc:  # destructive safety check must not degrade
        raise RuntimeError(
            "[MANIFEST] psutil is required to prove no archive process is "
            "running before export/prune.") from exc
    current_pid = os.getpid()
    hard_names = {"robocopy.exe", "robocopy", "scp.exe", "scp",
                  "sftp.exe", "sftp", "tar.exe", "tar"}
    results = []
    for proc in psutil.process_iter(["pid", "name", "cmdline"]):
        try:
            if proc.info["pid"] == current_pid:
                continue
            name = (proc.info.get("name") or "").lower()
            cmdline = " ".join(proc.info.get("cmdline") or [])
            lowered = cmdline.lower().replace("\\", "/")
            is_archiver = (
                name in hard_names
                or bool(re.search(r"(^|/)run\.py(?:\s|$)", lowered))
                or "remote_orchestrator" in lowered
                or "local_orchestrator" in lowered
                or "catalog-sync" in lowered
            )
            if is_archiver:
                results.append({"pid": proc.info["pid"], "name": name,
                                "command": cmdline[:500]})
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    return results


def _connect(conninfo, *, autocommit=False):
    require_psycopg()
    return cast(Any, psycopg.connect(
        conninfo, autocommit=autocommit,
        row_factory=cast(Any, dict_row)))


def _now():
    return datetime.now(timezone.utc)


def _safe_component(value):
    text = re.sub(r"[^A-Za-z0-9._-]+", "_", str(value or "unknown"))
    return text.strip("._") or "unknown"


def _normal_path(value):
    value = str(value or "").replace("\\", "/")
    value = re.sub(r"/+", "/", value)
    return value.rstrip("/") or ("/" if value.startswith("/") else "")


def _dirname(value):
    path = _normal_path(value)
    if not path:
        return "ROOT"
    parent = posixpath.dirname(path)
    return parent or ("/" if path.startswith("/") else "ROOT")


def _ancestors(path):
    path = _normal_path(path)
    if not path or path == "ROOT":
        return ["ROOT"]
    if path == "/":
        return ["/"]
    absolute = path.startswith("/")
    parts = [part for part in path.strip("/").split("/") if part]
    out = ["/"] if absolute else []
    current = "" if absolute else None
    for part in parts:
        if absolute:
            current = current + "/" + part
        else:
            current = part if current is None else current + "/" + part
        out.append(current)
    return out or (["/"] if absolute else ["ROOT"])


def _json_default(value):
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, memoryview):
        return bytes(value).hex()
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).hex()
    raise TypeError(f"Cannot JSON-encode {type(value).__name__}")


def validate_archive_root(archive_root, protected_paths=()):
    """Require a permanent root that is not inside a staging/cleanup tree."""
    root = os.path.abspath(os.path.expanduser(str(archive_root or "")))
    if not archive_root:
        raise RuntimeError("[MANIFEST] local archive root is not configured.")
    for protected in protected_paths:
        if not protected:
            continue
        protected = os.path.abspath(os.path.expanduser(str(protected)))
        try:
            root_under = os.path.commonpath([root, protected]) == protected
            protected_under = os.path.commonpath([root, protected]) == root
        except ValueError:
            continue  # different Windows volumes cannot contain one another
        if root_under or protected_under:
            raise RuntimeError(
                "[MANIFEST] Archive root must be separate from staging and "
                f"cleanup paths: {root} conflicts with {protected}")
    return root


def _table_exists(conn, name):
    return conn.execute(
        """SELECT 1 FROM information_schema.tables
           WHERE table_schema='public' AND table_name=%s""", (name,)
    ).fetchone() is not None


def _classification_inputs(conn):
    """Build set-based ownership SQL for the production classifier.

    Older code ran two correlated aggregate subqueries for every small-file
    row.  On the live catalog that left a classifier running for many minutes.
    Materialising one row per physical bundle path makes ownership and coverage
    deterministic while scanning ``directory_archive_bundles`` only once.
    """
    local = "COALESCE(f.local_session_id, ar.local_session_id)"
    remote = "COALESCE(f.remote_session_id, ar.remote_session_id)"
    if not _table_exists(conn, "directory_archive_bundles"):
        return "", "FALSE", local, remote, ""
    prelude = """bundle_ownership AS MATERIALIZED (
            SELECT dab.tape_label, dab.stored_bundle_path,
                   CASE WHEN COUNT(DISTINCT dab.local_session_id)=1
                          AND COUNT(DISTINCT dab.remote_session_id)=0
                        THEN MIN(dab.local_session_id) END
                     AS bundle_local_session_id,
                   CASE WHEN COUNT(DISTINCT dab.remote_session_id)=1
                          AND COUNT(DISTINCT dab.local_session_id)=0
                        THEN MIN(dab.remote_session_id) END
                     AS bundle_remote_session_id
            FROM directory_archive_bundles dab
            GROUP BY dab.tape_label, dab.stored_bundle_path
        ),"""
    coverage = """(
        f.bundle_id IS NOT NULL AND b.tape_path IS NOT NULL
        AND bo.stored_bundle_path IS NOT NULL
    )"""
    bundle_join = """LEFT JOIN bundle_ownership bo
              ON bo.tape_label=f.tape_label
             AND bo.stored_bundle_path=b.tape_path"""
    return (
        prelude,
        coverage,
        "COALESCE(f.local_session_id, ar.local_session_id, "
        "bo.bundle_local_session_id)",
        "COALESCE(f.remote_session_id, ar.remote_session_id, "
        "bo.bundle_remote_session_id)",
        bundle_join,
    )


def _classification_cte(coverage_expression, local_owner=None,
                        remote_owner=None, *, prelude="", bundle_join="",
                        file_predicate=""):
    """SQL CTE that assigns one conservative eligibility decision per row."""
    local_owner = local_owner or "COALESCE(f.local_session_id, ar.local_session_id)"
    remote_owner = remote_owner or "COALESCE(f.remote_session_id, ar.remote_session_id)"
    return f"""
        WITH {prelude} local_chunk_state AS MATERIALIZED (
            SELECT lc.session_id,
                   COUNT(DISTINCT lc.chunk_index) AS chunk_count,
                   COUNT(*) FILTER (WHERE lc.status <> 'backed_up')
                     AS nonterminal_count
            FROM local_chunks_manifest lc GROUP BY lc.session_id
        ), local_chunk_terminal AS MATERIALIZED (
            SELECT lc.session_id, lc.chunk_index,
                   BOOL_AND(lc.status = 'backed_up') AS terminal
            FROM local_chunks_manifest lc
            GROUP BY lc.session_id, lc.chunk_index
        ), remote_chunk_state AS MATERIALIZED (
            SELECT rc.session_id, COUNT(*) AS chunk_count,
                   COUNT(*) FILTER (WHERE rc.status <> 'done')
                     AS nonterminal_count
            FROM remote_chunks rc GROUP BY rc.session_id
        ), remote_chunk_terminal AS MATERIALIZED (
            SELECT rc.session_id, rc.chunk_index,
                   BOOL_AND(rc.status = 'done') AS terminal
            FROM remote_chunks rc
            GROUP BY rc.session_id, rc.chunk_index
        ), raw_ownership AS MATERIALIZED (
            SELECT f.file_id, f.record_key, f.tape_label, f.source_host,
                   f.original_path, f.file_size_bytes, f.archive_run_id,
                   {local_owner}
                       AS owner_local_session_id,
                   {remote_owner}
                       AS owner_remote_session_id,
                   COALESCE(f.local_chunk_index, f.remote_chunk_index)
                       AS owner_chunk_index,
                   f.local_chunk_index, f.remote_chunk_index, f.bundle_id,
                   f.is_packed, ar.session_kind, ar.completed_at AS run_completed,
                   {coverage_expression} AS covered_by_directory_catalog
            FROM files_index f
            JOIN archive_runs ar ON ar.run_id=f.archive_run_id
            LEFT JOIN archive_bundles b ON b.bundle_id=f.bundle_id
            {bundle_join}
            WHERE f.file_size_bytes < %s
            {file_predicate}
        ), ownership AS (
            SELECT o.*,
                   ls.status AS local_status, ls.completed_at AS local_completed,
                   ls.total_chunks AS local_total_chunks,
                   rs.status AS remote_status, rs.completed_at AS remote_completed,
                   rs.chunk_count AS remote_total_chunks,
                   lcs.chunk_count AS local_chunk_count,
                   lcs.nonterminal_count AS local_nonterminal_chunks,
                   rcs.chunk_count AS remote_chunk_count,
                   rcs.nonterminal_count AS remote_nonterminal_chunks,
                   owner_lc.terminal AS owner_local_chunk_terminal,
                   owner_rc.terminal AS owner_remote_chunk_terminal
            FROM raw_ownership o
            LEFT JOIN local_sessions ls
              ON ls.session_id=o.owner_local_session_id
            LEFT JOIN remote_sessions rs
              ON rs.session_id=o.owner_remote_session_id
            LEFT JOIN local_chunk_state lcs
              ON lcs.session_id=o.owner_local_session_id
            LEFT JOIN remote_chunk_state rcs
              ON rcs.session_id=o.owner_remote_session_id
            LEFT JOIN local_chunk_terminal owner_lc
              ON owner_lc.session_id=o.owner_local_session_id
             AND owner_lc.chunk_index=o.local_chunk_index
            LEFT JOIN remote_chunk_terminal owner_rc
              ON owner_rc.session_id=o.owner_remote_session_id
             AND owner_rc.chunk_index=o.remote_chunk_index
        ), classified AS (
            SELECT o.*,
              CASE
                WHEN run_completed IS NULL THEN 'archive_run_incomplete'
                WHEN owner_local_session_id IS NOT NULL
                 AND owner_remote_session_id IS NOT NULL
                  THEN 'session_ownership_unknown'
                WHEN owner_local_session_id IS NULL
                 AND owner_remote_session_id IS NULL
                 AND session_kind <> 'legacy'
                  THEN 'session_ownership_unknown'
                WHEN owner_local_session_id IS NULL
                 AND owner_remote_session_id IS NULL
                 AND session_kind = 'legacy' THEN NULL
                WHEN owner_local_session_id IS NOT NULL AND
                     (local_status IS DISTINCT FROM 'completed'
                      OR local_completed IS NULL)
                  THEN 'session_incomplete'
                WHEN owner_local_session_id IS NOT NULL AND
                     (COALESCE(local_chunk_count,0)
                        IS DISTINCT FROM local_total_chunks
                      OR COALESCE(local_nonterminal_chunks,0) > 0
                      OR (local_chunk_index IS NOT NULL AND
                          owner_local_chunk_terminal IS DISTINCT FROM TRUE))
                  THEN 'chunk_not_done'
                WHEN owner_remote_session_id IS NOT NULL AND
                     (remote_status IS DISTINCT FROM 'completed'
                      OR remote_completed IS NULL)
                  THEN 'session_incomplete'
                WHEN owner_remote_session_id IS NOT NULL AND
                     (COALESCE(remote_chunk_count,0)
                        IS DISTINCT FROM remote_total_chunks
                      OR COALESCE(remote_nonterminal_chunks,0) > 0
                      OR (remote_chunk_index IS NOT NULL AND
                          owner_remote_chunk_terminal IS DISTINCT FROM TRUE))
                  THEN 'chunk_not_done'
                WHEN is_packed AND bundle_id IS NULL
                  THEN 'session_ownership_unknown'
                ELSE NULL
              END AS skip_reason
            FROM ownership o
        )
    """


def _classification_cte_for_connection(conn, *, file_predicate=""):
    prelude, coverage, local, remote, bundle_join = (
        _classification_inputs(conn))
    return _classification_cte(
        coverage, local, remote, prelude=prelude,
        bundle_join=bundle_join, file_predicate=file_predicate)


def dry_run_export(conninfo, threshold_bytes=SMALL_FILE_THRESHOLD_BYTES):
    with _connect(conninfo, autocommit=True) as conn:
        cte = _classification_cte_for_connection(conn)
        rows = conn.execute(
            cte + """SELECT COALESCE(skip_reason, 'eligible') AS outcome,
                             COUNT(*) AS rows,
                             COALESCE(SUM(file_size_bytes), 0) AS bytes
                      FROM classified GROUP BY 1 ORDER BY 1""",
            (int(threshold_bytes),),
        ).fetchall()
    outcomes = {row["outcome"]: {
        "rows": int(row["rows"] or 0), "bytes": int(row["bytes"] or 0)
    } for row in rows}
    return {
        "mode": "dry-run", "threshold_bytes": int(threshold_bytes),
        "outcomes": outcomes,
        "eligible_rows": outcomes.get("eligible", {}).get("rows", 0),
        "skipped_rows": sum(v["rows"] for k, v in outcomes.items()
                            if k != "eligible"),
    }


def _session_directory(row):
    if row["local_session_id"] is not None:
        return f"session_local_{row['local_session_id']}"
    if row["remote_session_id"] is not None:
        return f"session_remote_{row['remote_session_id']}"
    return f"session_legacy_run_{row['archive_run_id']}"


def _segment_relpath(row):
    tape = _safe_component(row["tape_label"])
    session = _safe_component(_session_directory(row))
    name = (f"bundle_{row['bundle_id']}.jsonl.zst"
            if row["bundle_id"] is not None else "unbundled.jsonl.zst")
    return os.path.join(tape, session, name)


def _write_segment(root, relpath, rows):
    if zstd is None:
        raise RuntimeError("[MANIFEST] zstandard is required.")
    final_path = os.path.abspath(os.path.join(root, relpath))
    if os.path.commonpath([root, final_path]) != root:
        raise RuntimeError("[MANIFEST] Refusing path outside archive root.")
    os.makedirs(os.path.dirname(final_path), exist_ok=True)
    fd, temp_path = tempfile.mkstemp(
        prefix=".manifest_", suffix=".tmp", dir=os.path.dirname(final_path))
    os.close(fd)
    count = total_bytes = covered_rows = covered_bytes = 0
    try:
        with open(temp_path, "wb") as raw:
            with zstd.ZstdCompressor(level=6).stream_writer(raw) as writer:
                for row in rows:
                    payload = dict(row)
                    writer.write((json.dumps(
                        payload, default=_json_default, ensure_ascii=False,
                        separators=(",", ":")) + "\n").encode("utf-8"))
                    count += 1
                    size = int(row["file_size_bytes"] or 0)
                    total_bytes += size
                    if row.get("covered_by_directory_catalog", False):
                        covered_rows += 1
                        covered_bytes += size
        os.replace(temp_path, final_path)
    except Exception:
        try:
            os.remove(temp_path)
        except OSError:
            pass
        raise
    digest = hashlib.sha256()
    with open(final_path, "rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return {
        "manifest_relpath": relpath.replace("\\", "/"),
        "row_count": count,
        "original_bytes": total_bytes,
        "covered_rows": covered_rows,
        "covered_bytes": covered_bytes,
        "uncovered_rows": count - covered_rows,
        "uncovered_bytes": total_bytes - covered_bytes,
        "compressed_bytes": os.path.getsize(final_path),
        "sha256_hex": digest.hexdigest(),
    }


def _build_folder_aggregates(conn, export_id):
    direct_rows = conn.execute(
        """SELECT f.source_host, f.tape_label,
                  COALESCE(NULLIF(regexp_replace(
                    replace(f.original_path, chr(92), '/'), '/[^/]*$', ''),
                    ''), 'ROOT') AS original_dir_path,
                  COUNT(*) AS file_count,
                  COALESCE(SUM(f.file_size_bytes),0) AS file_bytes,
                  COUNT(*) FILTER (
                    WHERE NOT er.covered_by_directory_catalog)
                    AS uncovered_file_count,
                  COALESCE(SUM(f.file_size_bytes) FILTER (
                    WHERE NOT er.covered_by_directory_catalog),0)
                    AS uncovered_bytes,
                  MAX(f.catalog_backup_date) AS backup_date
           FROM local_manifest_export_rows er
           JOIN files_index f ON f.file_id=er.source_file_id
                              AND f.record_key=er.source_record_key
           WHERE er.export_id=%s AND er.eligible
           GROUP BY f.source_host, f.tape_label, 3""", (export_id,)
    ).fetchall()
    # direct count/bytes, recursive count/bytes, direct uncovered count/bytes,
    # recursive uncovered count/bytes, newest backup timestamp
    aggregates = defaultdict(lambda: [0, 0, 0, 0, 0, 0, 0, 0, None])
    for row in direct_rows:
        direct = _normal_path(row["original_dir_path"]) or "ROOT"
        key = (row["source_host"], row["tape_label"], direct)
        agg = aggregates[key]
        agg[0] += int(row["file_count"] or 0)
        agg[1] += int(row["file_bytes"] or 0)
        agg[4] += int(row["uncovered_file_count"] or 0)
        agg[5] += int(row["uncovered_bytes"] or 0)
        for ancestor in _ancestors(direct):
            recursive = aggregates[(row["source_host"], row["tape_label"],
                                    ancestor)]
            recursive[2] += int(row["file_count"] or 0)
            recursive[3] += int(row["file_bytes"] or 0)
            recursive[6] += int(row["uncovered_file_count"] or 0)
            recursive[7] += int(row["uncovered_bytes"] or 0)
            stamp = row["backup_date"]
            if recursive[8] is None or stamp > recursive[8]:
                recursive[8] = stamp
        if agg[8] is None or row["backup_date"] > agg[8]:
            agg[8] = row["backup_date"]
    values = [
        (export_id, host, tape, path, data[0], data[1], data[4], data[5],
         data[2], data[3], data[6], data[7], data[8] or _now())
        for (host, tape, path), data in aggregates.items()
    ]
    if values:
        with conn.cursor() as cur:
            cur.executemany(
                """INSERT INTO local_manifest_folder_aggregates
                   (export_id, source_host, tape_label, original_dir_path,
                    direct_file_count, direct_bytes,
                    direct_uncovered_file_count, direct_uncovered_bytes,
                    recursive_file_count, recursive_bytes,
                    recursive_uncovered_file_count, recursive_uncovered_bytes,
                    backup_date)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (export_id, source_host, tape_label,
                                original_dir_path) DO UPDATE SET
                     direct_file_count=EXCLUDED.direct_file_count,
                     direct_bytes=EXCLUDED.direct_bytes,
                     direct_uncovered_file_count=
                       EXCLUDED.direct_uncovered_file_count,
                     direct_uncovered_bytes=EXCLUDED.direct_uncovered_bytes,
                     recursive_file_count=EXCLUDED.recursive_file_count,
                     recursive_bytes=EXCLUDED.recursive_bytes,
                     recursive_uncovered_file_count=
                       EXCLUDED.recursive_uncovered_file_count,
                     recursive_uncovered_bytes=
                       EXCLUDED.recursive_uncovered_bytes,
                     backup_date=EXCLUDED.backup_date""", values)
    return len(values)


def _build_catalog_aggregates(conn, export_id):
    result = conn.execute(
        """WITH RECURSIVE eligible AS (
               SELECT f.directory_id, f.file_size_bytes
               FROM local_manifest_export_rows er
               JOIN files_index f ON f.file_id=er.source_file_id
                                  AND f.record_key=er.source_record_key
               WHERE er.export_id=%s AND er.eligible
           ), ancestry AS (
               SELECT e.directory_id AS leaf_id,
                      e.directory_id AS ancestor_id, e.file_size_bytes
               FROM eligible e
               UNION ALL
               SELECT a.leaf_id, d.parent_id, a.file_size_bytes
               FROM ancestry a
               JOIN catalog_directories d ON d.directory_id=a.ancestor_id
               WHERE d.parent_id IS NOT NULL
           )
           INSERT INTO local_manifest_catalog_aggregates
             (export_id, directory_id, direct_file_count, direct_bytes,
              recursive_file_count, recursive_bytes)
           SELECT %s, ancestor_id,
                  COUNT(*) FILTER (WHERE leaf_id=ancestor_id),
                  COALESCE(SUM(file_size_bytes)
                    FILTER (WHERE leaf_id=ancestor_id),0),
                  COUNT(*), COALESCE(SUM(file_size_bytes),0)
           FROM ancestry GROUP BY ancestor_id
           ON CONFLICT (export_id, directory_id) DO UPDATE SET
             direct_file_count=EXCLUDED.direct_file_count,
             direct_bytes=EXCLUDED.direct_bytes,
             recursive_file_count=EXCLUDED.recursive_file_count,
             recursive_bytes=EXCLUDED.recursive_bytes""",
        (export_id, export_id)).rowcount
    return int(result or 0)


def execute_export(conninfo, archive_root, hot_backup_path, *,
                   threshold_bytes=SMALL_FILE_THRESHOLD_BYTES):
    """Snapshot eligible rows and write immutable local manifest segments."""
    if not hot_backup_path or not os.path.isfile(hot_backup_path):
        raise RuntimeError("[MANIFEST] A verified hot backup path is required.")
    root = os.path.abspath(archive_root)
    os.makedirs(root, exist_ok=True)
    with _connect(conninfo) as conn:
        if not _table_exists(conn, "local_manifest_exports"):
            raise RuntimeError("[MANIFEST] Schema migration 010 is not applied.")
        # Close the information_schema read transaction, then take the same
        # transaction-level lock used by prune before selecting any candidate.
        conn.commit()
        _acquire_prune_lock(conn)
        open_export = conn.execute(
            """SELECT export_id, status FROM local_manifest_exports
               WHERE status NOT IN ('pruned','failed','validation_failed')
               ORDER BY export_id DESC LIMIT 1""").fetchone()
        if open_export:
            raise RuntimeError(
                "[MANIFEST] Finish or investigate export "
                f"{open_export['export_id']} ({open_export['status']}) first.")
        cte = _classification_cte_for_connection(conn)
        export_id = conn.execute(
            """INSERT INTO local_manifest_exports
               (threshold_bytes, archive_root, hot_backup_path, status)
               VALUES (%s,%s,%s,'snapshotting') RETURNING export_id""",
            (int(threshold_bytes), root, os.path.abspath(hot_backup_path)),
        ).fetchone()["export_id"]
        conn.execute(
            cte + """INSERT INTO local_manifest_export_rows
              (export_id, source_file_id, source_record_key, tape_label,
               source_host, original_path, file_size_bytes, archive_run_id,
               local_session_id, remote_session_id, chunk_index, bundle_id,
               covered_by_directory_catalog, eligible, skip_reason)
              SELECT %s, file_id, record_key, tape_label, source_host,
                     original_path, file_size_bytes, archive_run_id,
                     owner_local_session_id, owner_remote_session_id,
                     owner_chunk_index, bundle_id,
                     covered_by_directory_catalog, skip_reason IS NULL,
                     skip_reason
              FROM classified""", (int(threshold_bytes), export_id))
        summary = conn.execute(
            """SELECT COUNT(*) AS candidate_rows,
                      COALESCE(SUM(file_size_bytes),0) AS candidate_bytes,
                      COUNT(*) FILTER (WHERE eligible) AS eligible_rows,
                      COALESCE(SUM(file_size_bytes) FILTER (WHERE eligible),0)
                        AS eligible_bytes,
                      COUNT(*) FILTER (WHERE NOT eligible) AS skipped_rows
               FROM local_manifest_export_rows WHERE export_id=%s""",
            (export_id,),
        ).fetchone()
        conn.execute(
            """UPDATE local_manifest_exports SET status='exporting',
                 candidate_rows=%s, candidate_bytes=%s, eligible_rows=%s,
                 eligible_bytes=%s, skipped_rows=%s WHERE export_id=%s""",
            (summary["candidate_rows"], summary["candidate_bytes"],
             summary["eligible_rows"], summary["eligible_bytes"],
             summary["skipped_rows"], export_id))
        conn.commit()  # the immutable candidate snapshot is now durable

        segments = []
        try:
            with conn.cursor(name=f"local_manifest_export_{export_id}") as cur:
                cur.itersize = 5000
                cur.execute(
                    """SELECT f.file_id AS source_file_id, f.original_path,
                              f.file_size_bytes, f.tape_label, f.source_host,
                              f.is_packed, f.stored_path,
                              er.local_session_id, f.local_chunk_index,
                              er.remote_session_id, f.remote_chunk_index,
                              f.bundle_id, b.tape_path AS container_name,
                              f.archive_run_id, f.catalog_name AS file_name,
                              f.catalog_backup_date AS backup_date,
                              er.covered_by_directory_catalog,
                              encode(f.record_key, 'hex') AS source_record_key
                       FROM local_manifest_export_rows er
                       JOIN files_index f ON f.file_id=er.source_file_id
                                          AND f.record_key=er.source_record_key
                       LEFT JOIN archive_bundles b ON b.bundle_id=f.bundle_id
                       WHERE er.export_id=%s AND er.eligible
                       ORDER BY f.tape_label,
                                COALESCE(er.local_session_id,-1),
                                COALESCE(er.remote_session_id,-1),
                                f.archive_run_id, COALESCE(f.bundle_id,-1),
                                f.file_id""", (export_id,))
                for rel, group in itertools.groupby(cur, key=_segment_relpath):
                    segments.append(_write_segment(
                        root, rel, (dict(row) for row in group)))
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.executemany(
                        """INSERT INTO local_manifest_segments
                           (export_id, manifest_relpath, row_count,
                            original_bytes, covered_rows, covered_bytes,
                            uncovered_rows, uncovered_bytes,
                            compressed_bytes, sha256_hex)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                           ON CONFLICT (export_id, manifest_relpath) DO UPDATE SET
                             row_count=EXCLUDED.row_count,
                             original_bytes=EXCLUDED.original_bytes,
                             covered_rows=EXCLUDED.covered_rows,
                             covered_bytes=EXCLUDED.covered_bytes,
                             uncovered_rows=EXCLUDED.uncovered_rows,
                             uncovered_bytes=EXCLUDED.uncovered_bytes,
                             compressed_bytes=EXCLUDED.compressed_bytes,
                             sha256_hex=EXCLUDED.sha256_hex""",
                        [(export_id, s["manifest_relpath"], s["row_count"],
                          s["original_bytes"], s["covered_rows"],
                          s["covered_bytes"], s["uncovered_rows"],
                          s["uncovered_bytes"], s["compressed_bytes"],
                          s["sha256_hex"]) for s in segments])
                folder_rows = _build_folder_aggregates(conn, export_id)
                catalog_aggregate_rows = _build_catalog_aggregates(
                    conn, export_id)
                conn.execute(
                    """UPDATE local_manifest_exports SET status='exported',
                         segment_count=%s, exported_at=now()
                       WHERE export_id=%s""", (len(segments), export_id))
        except Exception as exc:
            conn.rollback()
            conn.execute(
                """UPDATE local_manifest_exports SET status='failed',
                     error_message=%s WHERE export_id=%s""",
                (str(exc), export_id))
            conn.commit()
            raise
        conn.commit()
        return {
            "export_id": int(export_id), "archive_root": root,
            "segments": len(segments), "folder_aggregates": folder_rows,
            "catalog_aggregates": catalog_aggregate_rows,
            "eligible_rows": int(summary["eligible_rows"] or 0),
            "eligible_bytes": int(summary["eligible_bytes"] or 0),
            "skipped_rows": int(summary["skipped_rows"] or 0),
        }


def validate_export(conninfo, export_id):
    """Re-hash/decompress every segment and compare exact file-id membership."""
    if zstd is None:
        raise RuntimeError("[MANIFEST] zstandard is required.")
    with _connect(conninfo) as conn:
        export = conn.execute(
            "SELECT * FROM local_manifest_exports WHERE export_id=%s",
            (export_id,)).fetchone()
        if not export:
            raise RuntimeError(f"[MANIFEST] Export not found: {export_id}")
        segments = conn.execute(
            """SELECT * FROM local_manifest_segments WHERE export_id=%s
               ORDER BY manifest_relpath""", (export_id,)).fetchall()
        conn.execute(
            "CREATE TEMP TABLE _manifest_validation_ids "
            "(source_file_id BIGINT NOT NULL) ON COMMIT DROP")
        total_rows = total_bytes = 0
        errors = []
        for segment in segments:
            path = os.path.abspath(os.path.join(
                export["archive_root"], segment["manifest_relpath"]))
            try:
                if os.path.commonpath([export["archive_root"], path]) != os.path.abspath(export["archive_root"]):
                    raise RuntimeError("segment escapes archive root")
                digest = hashlib.sha256()
                with open(path, "rb") as raw:
                    for block in iter(lambda: raw.read(1024 * 1024), b""):
                        digest.update(block)
                if digest.hexdigest() != segment["sha256_hex"]:
                    raise RuntimeError("SHA-256 mismatch")
                ids = []
                seg_rows = seg_bytes = 0
                with open(path, "rb") as raw:
                    reader = zstd.ZstdDecompressor().stream_reader(raw)
                    with io.TextIOWrapper(reader, encoding="utf-8") as text:
                        for line in text:
                            item = json.loads(line)
                            ids.append((int(item["source_file_id"]),))
                            seg_rows += 1
                            seg_bytes += int(item["file_size_bytes"] or 0)
                            if len(ids) >= 5000:
                                with conn.cursor() as cur:
                                    copy_rows(cur, "_manifest_validation_ids",
                                              ("source_file_id",), ids)
                                ids = []
                if ids:
                    with conn.cursor() as cur:
                        copy_rows(cur, "_manifest_validation_ids",
                                  ("source_file_id",), ids)
                if seg_rows != int(segment["row_count"]):
                    raise RuntimeError("row-count mismatch")
                if seg_bytes != int(segment["original_bytes"]):
                    raise RuntimeError("byte-total mismatch")
                total_rows += seg_rows
                total_bytes += seg_bytes
            except Exception as exc:
                errors.append({"segment": segment["manifest_relpath"],
                               "error": str(exc)})
        membership = conn.execute(
            """SELECT
                 (SELECT COUNT(*) FROM local_manifest_export_rows er
                  WHERE er.export_id=%s AND er.eligible AND NOT EXISTS (
                    SELECT 1 FROM _manifest_validation_ids v
                    WHERE v.source_file_id=er.source_file_id)) AS missing,
                 (SELECT COUNT(*) FROM _manifest_validation_ids v
                  WHERE NOT EXISTS (
                    SELECT 1 FROM local_manifest_export_rows er
                    WHERE er.export_id=%s AND er.eligible
                      AND er.source_file_id=v.source_file_id)) AS extra,
                 (SELECT COALESCE(SUM(n - 1),0) FROM (
                    SELECT COUNT(*) AS n FROM _manifest_validation_ids
                    GROUP BY source_file_id HAVING COUNT(*) > 1
                  ) duplicates) AS duplicates""",
            (export_id, export_id)).fetchone()
        folder = conn.execute(
            """SELECT COALESCE(SUM(direct_file_count),0) AS rows,
                      COALESCE(SUM(direct_bytes),0) AS bytes
               FROM local_manifest_folder_aggregates WHERE export_id=%s""",
            (export_id,)).fetchone()
        catalog_folder = conn.execute(
            """SELECT COALESCE(SUM(direct_file_count),0) AS rows,
                      COALESCE(SUM(direct_bytes),0) AS bytes
               FROM local_manifest_catalog_aggregates WHERE export_id=%s""",
            (export_id,)).fetchone()
        passed = (
            not errors and int(membership["missing"] or 0) == 0
            and int(membership["extra"] or 0) == 0
            and int(membership["duplicates"] or 0) == 0
            and total_rows == int(export["eligible_rows"] or 0)
            and total_bytes == int(export["eligible_bytes"] or 0)
            and int(folder["rows"] or 0) == total_rows
            and int(folder["bytes"] or 0) == total_bytes)
        passed = (passed
                  and int(catalog_folder["rows"] or 0) == total_rows
                  and int(catalog_folder["bytes"] or 0) == total_bytes)
        report = {
            "export_id": int(export_id), "passed": passed,
            "segments": len(segments), "rows": total_rows,
            "bytes": total_bytes, "missing_ids": int(membership["missing"] or 0),
            "extra_ids": int(membership["extra"] or 0),
            "duplicate_ids": int(membership["duplicates"] or 0),
            "folder_direct_rows": int(folder["rows"] or 0),
            "folder_direct_bytes": int(folder["bytes"] or 0),
            "catalog_direct_rows": int(catalog_folder["rows"] or 0),
            "catalog_direct_bytes": int(catalog_folder["bytes"] or 0),
            "errors": errors,
        }
        conn.execute(
            """UPDATE local_manifest_exports SET status=%s,
                 validation_passed=%s, validation_report=%s,
                 validated_at=now() WHERE export_id=%s""",
            ("validated" if passed else "validation_failed", passed,
             json.dumps(report), export_id))
        conn.commit()
        return report


def _acquire_prune_lock(conn):
    locked = conn.execute(
        "SELECT pg_try_advisory_xact_lock(%s) AS locked",
        (PgConnectionCore.ARCHIVER_LOCK_KEY,),
    ).fetchone()["locked"]
    if not locked:
        raise RuntimeError(
            "[MANIFEST] Refusing prune while the archiver lock is held.")


def _used_space_by_tape(conn, tape_labels):
    """Same accounting model as PgTapeMixin, including pruned manifests."""
    has_directory_catalog = _table_exists(conn, "directory_archive_bundles")
    result = {}
    for label in tape_labels:
        if has_directory_catalog:
            base = conn.execute(
                """WITH bundle_paths AS (
                       SELECT stored_bundle_path FROM directory_archive_bundles
                       WHERE tape_label=%s
                   ), legacy AS (
                       SELECT COALESCE(SUM(f.file_size_bytes),0) AS n
                       FROM files_index f
                       LEFT JOIN archive_bundles b ON b.bundle_id=f.bundle_id
                       WHERE f.tape_label=%s AND NOT EXISTS (
                         SELECT 1 FROM bundle_paths p
                         WHERE p.stored_bundle_path=b.tape_path)
                   ), bundles AS (
                       SELECT COALESCE(SUM(byte_count),0) AS n
                       FROM directory_archive_bundles WHERE tape_label=%s
                   ) SELECT (SELECT n FROM legacy)+(SELECT n FROM bundles) AS n""",
                (label, label, label)).fetchone()["n"]
            manifest_column = "direct_uncovered_bytes"
        else:
            base = conn.execute(
                """SELECT COALESCE(SUM(file_size_bytes),0) AS n
                   FROM files_index WHERE tape_label=%s""", (label,)
            ).fetchone()["n"]
            manifest_column = "direct_bytes"
        manifest = conn.execute(
            f"""SELECT COALESCE(SUM(a.{manifest_column}),0) AS n
                 FROM local_manifest_folder_aggregates a
                 JOIN local_manifest_exports e ON e.export_id=a.export_id
                 WHERE a.tape_label=%s AND e.status='pruned'""", (label,)
        ).fetchone()["n"]
        result[label] = int(base or 0) + int(manifest or 0)
    return result


def _prune_progress(conn, export_id):
    row = conn.execute(
        """SELECT COUNT(*) FILTER (WHERE eligible AND pruned_at IS NOT NULL)
                    AS rows,
                  COALESCE(SUM(file_size_bytes) FILTER (
                    WHERE eligible AND pruned_at IS NOT NULL),0) AS bytes,
                  COUNT(*) FILTER (WHERE eligible AND pruned_at IS NULL)
                    AS pending_rows
           FROM local_manifest_export_rows WHERE export_id=%s""",
        (export_id,)).fetchone()
    return {
        "rows": int(row["rows"] or 0),
        "bytes": int(row["bytes"] or 0),
        "pending_rows": int(row["pending_rows"] or 0),
    }


def _store_prune_report(conn, export_id, report, *, status="pruning"):
    conn.execute(
        """UPDATE local_manifest_exports
           SET status=%s, prune_report=%s WHERE export_id=%s""",
        (status, json.dumps(report), export_id))


def prune_export(conninfo, export_id, hot_backup_path, *, execute=False,
                 batch_size=DEFAULT_PRUNE_BATCH_SIZE):
    """Delete only a validated snapshot in resumable guarded transactions.

    Every committed batch is identity matched and has its terminal ownership,
    chunks, advisory lock, and local process state rechecked.  ``pruned_at`` on
    the immutable snapshot is the durable progress marker if a later batch is
    blocked; a failed current batch rolls back without losing earlier progress.
    """
    batch_size = int(batch_size or DEFAULT_PRUNE_BATCH_SIZE)
    if batch_size < 1:
        raise RuntimeError("[MANIFEST] Prune batch size must be positive.")
    validation = validate_export(conninfo, export_id)
    if not validation["passed"]:
        raise RuntimeError("[MANIFEST] Export validation failed; refusing prune.")
    with _connect(conninfo) as conn:
        export = conn.execute(
            "SELECT * FROM local_manifest_exports WHERE export_id=%s",
            (export_id,)).fetchone()
        if not export:
            raise RuntimeError(f"[MANIFEST] Export not found: {export_id}")
        if (not hot_backup_path or
                os.path.abspath(hot_backup_path) != export["hot_backup_path"] or
                not os.path.isfile(hot_backup_path)):
            raise RuntimeError(
                "[MANIFEST] The same verified pre-export hot backup is required.")
        skip_reasons = conn.execute(
            """SELECT skip_reason, COUNT(*) AS rows
               FROM local_manifest_export_rows
               WHERE export_id=%s AND NOT eligible
               GROUP BY skip_reason ORDER BY skip_reason""",
            (export_id,)).fetchall()
        tapes = [row["tape_label"] for row in conn.execute(
            """SELECT DISTINCT tape_label FROM local_manifest_export_rows
               WHERE export_id=%s AND eligible ORDER BY tape_label""",
            (export_id,)).fetchall()]
        progress = _prune_progress(conn, export_id)
        previous_report = export.get("prune_report") or {}
        previous_batches = (previous_report.get("batches", [])
                            if isinstance(previous_report, dict) else [])
        current_accounting = _used_space_by_tape(conn, tapes)
        stored_accounting = (previous_report.get("per_tape_accounting", {})
                             if isinstance(previous_report, dict) else {})
        stored_before = (stored_accounting.get("before")
                         if isinstance(stored_accounting, dict) else None)
        # A resumed prune has already removed committed batches from
        # files_index.  Preserve the pre-first-batch baseline recorded in the
        # durable report instead of treating the partial catalog as "before".
        before_accounting = (stored_before if isinstance(stored_before, dict)
                             else current_accounting)
        report = {
            "export_id": int(export_id),
            "mode": "execute" if execute else "dry-run",
            "batch_size": batch_size,
            "eligible_delete_rows": int(export["eligible_rows"] or 0),
            "eligible_delete_bytes": int(export["eligible_bytes"] or 0),
            "skipped": {row["skip_reason"]: int(row["rows"])
                        for row in skip_reasons},
            "deleted_rows": progress["rows"],
            "deleted_bytes": progress["bytes"],
            "pending_rows": progress["pending_rows"],
            "batches": list(previous_batches),
            "per_tape_accounting": {
                "before": before_accounting, "after": before_accounting,
                "passed": True},
        }
        if not execute:
            conn.rollback()
            return report
        conn.rollback()

        try:
            while True:
                processes = active_archive_processes()
                if processes:
                    raise RuntimeError(
                        "[MANIFEST] Archive/transfer process appeared before "
                        f"the next prune batch: {processes}")
                with conn.transaction():
                    _acquire_prune_lock(conn)
                    processes = active_archive_processes()
                    if processes:
                        raise RuntimeError(
                            "[MANIFEST] Archive/transfer process appeared after "
                            f"the prune lock was acquired: {processes}")
                    conn.execute(
                        """CREATE TEMP TABLE _manifest_prune_batch
                           (source_file_id BIGINT PRIMARY KEY) ON COMMIT DROP""")
                    selected = conn.execute(
                        """INSERT INTO _manifest_prune_batch(source_file_id)
                           SELECT er.source_file_id
                           FROM local_manifest_export_rows er
                           WHERE er.export_id=%s AND er.eligible
                             AND er.pruned_at IS NULL
                           ORDER BY er.source_file_id LIMIT %s
                           RETURNING source_file_id""",
                        (export_id, batch_size)).rowcount
                    if not selected:
                        break
                    recheck_sql = _classification_cte_for_connection(
                        conn, file_predicate="""AND EXISTS (
                          SELECT 1 FROM _manifest_prune_batch pb
                          WHERE pb.source_file_id=f.file_id)""")
                    no_longer_terminal = conn.execute(
                        recheck_sql + """SELECT COUNT(*) AS n
                            FROM classified c
                            JOIN local_manifest_export_rows er
                              ON er.source_file_id=c.file_id
                            JOIN _manifest_prune_batch pb
                              ON pb.source_file_id=c.file_id
                            WHERE er.export_id=%s AND er.eligible
                              AND c.skip_reason IS NOT NULL""",
                        (int(export["threshold_bytes"]), export_id),
                    ).fetchone()["n"]
                    if no_longer_terminal:
                        raise RuntimeError(
                            f"[MANIFEST] {no_longer_terminal} batch row(s) no "
                            "longer have terminal ownership/chunks.")
                    mismatch = conn.execute(
                        """SELECT COUNT(*) AS n
                           FROM _manifest_prune_batch pb
                           JOIN local_manifest_export_rows er
                             ON er.export_id=%s
                            AND er.source_file_id=pb.source_file_id
                           LEFT JOIN files_index f
                             ON f.file_id=er.source_file_id
                            AND f.record_key=er.source_record_key
                            AND f.file_size_bytes=er.file_size_bytes
                           WHERE f.file_id IS NULL""",
                        (export_id,)).fetchone()["n"]
                    if mismatch:
                        raise RuntimeError(
                            f"[MANIFEST] {mismatch} batch row(s) changed or "
                            "disappeared; current batch rolled back.")
                    deleted = conn.execute(
                        """WITH gone AS (
                               DELETE FROM files_index f
                               USING _manifest_prune_batch pb,
                                     local_manifest_export_rows er
                               WHERE er.export_id=%s AND er.eligible
                                 AND er.source_file_id=pb.source_file_id
                                 AND f.file_id=er.source_file_id
                                 AND f.record_key=er.source_record_key
                               RETURNING f.file_size_bytes
                           ) SELECT COUNT(*) AS rows,
                                    COALESCE(SUM(file_size_bytes),0) AS bytes
                             FROM gone""", (export_id,)).fetchone()
                    deleted_rows = int(deleted["rows"] or 0)
                    if deleted_rows != int(selected):
                        raise RuntimeError(
                            "[MANIFEST] Batch delete count mismatch; current "
                            "batch rolled back.")
                    marked = conn.execute(
                        """UPDATE local_manifest_export_rows er
                           SET pruned_at=now()
                           FROM _manifest_prune_batch pb
                           WHERE er.export_id=%s AND er.eligible
                             AND er.source_file_id=pb.source_file_id
                             AND er.pruned_at IS NULL""",
                        (export_id,)).rowcount
                    if int(marked or 0) != deleted_rows:
                        raise RuntimeError(
                            "[MANIFEST] Batch progress marker mismatch; current "
                            "batch rolled back.")
                    progress = _prune_progress(conn, export_id)
                    report["deleted_rows"] = progress["rows"]
                    report["deleted_bytes"] = progress["bytes"]
                    report["pending_rows"] = progress["pending_rows"]
                    report["batches"].append({
                        "batch": len(report["batches"]) + 1,
                        "rows": deleted_rows,
                        "bytes": int(deleted["bytes"] or 0),
                        "cumulative_rows": progress["rows"],
                        "cumulative_bytes": progress["bytes"],
                        "completed_at": _now().isoformat(),
                    })
                    _store_prune_report(conn, export_id, report)

            with conn.transaction():
                _acquire_prune_lock(conn)
                processes = active_archive_processes()
                if processes:
                    raise RuntimeError(
                        "[MANIFEST] Archive/transfer process appeared before "
                        f"prune finalization: {processes}")
                progress = _prune_progress(conn, export_id)
                if (progress["pending_rows"] != 0
                        or progress["rows"] != int(export["eligible_rows"] or 0)
                        or progress["bytes"] != int(export["eligible_bytes"] or 0)):
                    raise RuntimeError(
                        "[MANIFEST] Final prune progress does not match the "
                        "validated snapshot; finalization rolled back.")
                remaining = conn.execute(
                    """SELECT COUNT(*) AS n
                       FROM local_manifest_export_rows er
                       JOIN files_index f ON f.file_id=er.source_file_id
                       WHERE er.export_id=%s AND er.eligible""",
                    (export_id,)).fetchone()["n"]
                if remaining:
                    raise RuntimeError(
                        f"[MANIFEST] {remaining} validated candidate row(s) "
                        "remain in files_index; finalization rolled back.")
                conn.execute(
                    """UPDATE local_manifest_exports SET status='pruned',
                         pruned_at=now() WHERE export_id=%s""", (export_id,))
                after_accounting = _used_space_by_tape(conn, tapes)
                report["per_tape_accounting"] = {
                    "before": before_accounting, "after": after_accounting,
                    "passed": before_accounting == after_accounting}
                if before_accounting != after_accounting:
                    raise RuntimeError(
                        "[MANIFEST] Per-tape accounting changed; prune "
                        "finalization rolled back.")
                snapshot_rows = conn.execute(
                    "DELETE FROM local_manifest_export_rows WHERE export_id=%s",
                    (export_id,)).rowcount
                report["postgres_per_file_snapshot_rows_removed"] = int(
                    snapshot_rows or 0)
                report["pending_rows"] = 0
                _store_prune_report(
                    conn, export_id, report, status="pruned")
        except Exception as exc:
            conn.rollback()
            try:
                with conn.transaction():
                    progress = _prune_progress(conn, export_id)
                    report["deleted_rows"] = progress["rows"]
                    report["deleted_bytes"] = progress["bytes"]
                    report["pending_rows"] = progress["pending_rows"]
                    report["blocked_at"] = _now().isoformat()
                    report["blocking_error"] = str(exc)
                    _store_prune_report(conn, export_id, report)
            except Exception:
                conn.rollback()
            raise
        conn.execute("ANALYZE files_index")
        conn.commit()
        return report


def export_status(conninfo):
    with _connect(conninfo, autocommit=True) as conn:
        if not _table_exists(conn, "local_manifest_exports"):
            return []
        rows = conn.execute(
            """SELECT export_id, threshold_bytes, archive_root, status,
                      candidate_rows, eligible_rows, eligible_bytes,
                      skipped_rows, segment_count, validation_passed,
                      started_at, exported_at, validated_at, pruned_at,
                      error_message
               FROM local_manifest_exports ORDER BY export_id DESC LIMIT 20"""
        ).fetchall()
        return [dict(row) for row in rows]


def pruned_manifest_paths(conninfo):
    """Absolute segment paths whose PostgreSQL export reached pruned."""
    with _connect(conninfo, autocommit=True) as conn:
        if not _table_exists(conn, "local_manifest_exports"):
            return []
        rows = conn.execute(
            """SELECT e.archive_root, s.manifest_relpath
               FROM local_manifest_segments s
               JOIN local_manifest_exports e ON e.export_id=s.export_id
               WHERE e.status='pruned'""").fetchall()
    return [os.path.abspath(os.path.join(
        row["archive_root"], row["manifest_relpath"])) for row in rows]


def export_legacy_cold_database(cold_conninfo, archive_root, cold_backup_path):
    """One-time, read-only export used before retiring lto_cold_manifest.

    The caller must supply the already-created cold dump and its non-empty
    pg_restore list.  This function never drops/stops the old database.
    """
    backup = os.path.abspath(str(cold_backup_path or ""))
    restore_list = os.path.splitext(backup)[0] + ".restore_list.txt"
    if (not os.path.isfile(backup) or not os.path.getsize(backup)
            or not os.path.isfile(restore_list)
            or not os.path.getsize(restore_list)):
        raise RuntimeError(
            "[MANIFEST] A non-empty cold dump and verified .restore_list.txt "
            "are required before legacy cold export.")
    root = os.path.abspath(archive_root)
    out_dir = os.path.join(root, "cold_db_export")
    os.makedirs(out_dir, exist_ok=True)
    relpath = "cold_db_export/small_file_manifest_cold.jsonl.zst"
    with _connect(cold_conninfo) as conn:
        if not _table_exists(conn, "small_file_manifest_cold"):
            raise RuntimeError(
                "[MANIFEST] Legacy cold payload table does not exist.")
        expected = conn.execute(
            """SELECT COUNT(*) AS rows, COALESCE(SUM(size_bytes),0) AS bytes
               FROM small_file_manifest_cold""").fetchone()
        with conn.cursor(name="legacy_cold_local_export") as cur:
            cur.itersize = 5000
            cur.execute(
                """SELECT migration_id, source_hot_row_id, source_host,
                          tape_label, archive_run_id, local_session_id,
                          remote_session_id, chunk_index, bundle_id,
                          stored_bundle_path, original_root_dir, relative_path,
                          file_name, size_bytes, mtime, source_kind
                   FROM small_file_manifest_cold
                   ORDER BY migration_id, source_hot_row_id""")

            def mapped():
                for row in cur:
                    item = dict(row)
                    relative = item.get("relative_path") or item.get("file_name")
                    original = item.get("original_root_dir")
                    if original and relative:
                        original = original.rstrip("/\\") + "/" + str(relative).lstrip("/\\")
                    elif not original:
                        original = relative
                    yield {
                        "manifest_record_id": (
                            f"C:{item.get('migration_id')}:"
                            f"{item.get('source_hot_row_id')}"),
                        "source_file_id": item.get("source_hot_row_id"),
                        "original_path": original,
                        "file_size_bytes": item.get("size_bytes"),
                        "tape_label": item.get("tape_label"),
                        "source_host": item.get("source_host"),
                        "is_packed": item.get("source_kind") == "packed",
                        "stored_path": relative,
                        "local_session_id": item.get("local_session_id"),
                        "remote_session_id": item.get("remote_session_id"),
                        "bundle_id": item.get("bundle_id"),
                        "container_name": item.get("stored_bundle_path"),
                        "archive_run_id": item.get("archive_run_id"),
                        "file_name": item.get("file_name"),
                        "backup_date": item.get("mtime"),
                        "source_kind": item.get("source_kind"),
                    }

            segment = _write_segment(root, relpath, mapped())
    passed = (
        segment["row_count"] == int(expected["rows"] or 0)
        and segment["original_bytes"] == int(expected["bytes"] or 0))
    report = {
        "source": "lto_cold_manifest.small_file_manifest_cold",
        "cold_backup_path": backup,
        "cold_restore_list_path": restore_list,
        "manifest_relpath": segment["manifest_relpath"],
        "rows": segment["row_count"], "bytes": segment["original_bytes"],
        "compressed_bytes": segment["compressed_bytes"],
        "sha256_hex": segment["sha256_hex"], "passed": passed,
        "exported_at": _now().isoformat(),
    }
    report_path = os.path.join(out_dir, "export_report.json")
    fd, temp_path = tempfile.mkstemp(
        prefix=".cold_export_report_", suffix=".tmp", dir=out_dir)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2)
            handle.write("\n")
        os.replace(temp_path, report_path)
    except Exception:
        try:
            os.remove(temp_path)
        except OSError:
            pass
        raise
    if not passed:
        raise RuntimeError(
            "[MANIFEST] Legacy cold export count/byte validation failed.")
    return report


def iter_manifest_records(archive_root, *, query=None, tape_label=None,
                          date_from=None, date_to=None, allowed_paths=None):
    """Stream local archive matches without requiring the retired cold DB."""
    if zstd is None:
        raise RuntimeError("[MANIFEST] zstandard is required.")
    root = os.path.abspath(archive_root)
    allowed = ({os.path.normcase(os.path.abspath(path))
                for path in allowed_paths} if allowed_paths is not None else None)
    legacy_report = os.path.join(root, "cold_db_export", "export_report.json")
    legacy_allowed = False
    try:
        with open(legacy_report, "r", encoding="utf-8") as handle:
            legacy_allowed = bool(json.load(handle).get("passed"))
    except (OSError, ValueError, TypeError):
        pass
    pattern = (query or "*").lower()
    if not any(ch in pattern for ch in "*?"):
        pattern = f"*{pattern}*"
    for dirpath, _, names in os.walk(root):
        for name in sorted(names):
            if not name.endswith(".jsonl.zst"):
                continue
            path = os.path.join(dirpath, name)
            if allowed is not None and os.path.normcase(os.path.abspath(path)) not in allowed:
                if not (legacy_allowed and os.path.dirname(path) ==
                        os.path.join(root, "cold_db_export")):
                    continue
            with open(path, "rb") as raw:
                reader = zstd.ZstdDecompressor().stream_reader(raw)
                with io.TextIOWrapper(reader, encoding="utf-8") as text:
                    for line in text:
                        row = json.loads(line)
                        hay = (str(row.get("file_name") or "") + " " +
                               str(row.get("original_path") or "")).lower()
                        if not fnmatch.fnmatchcase(hay, pattern):
                            continue
                        if tape_label and row.get("tape_label") != tape_label:
                            continue
                        stamp = str(row.get("backup_date") or "")[:10]
                        if date_from and stamp < str(date_from):
                            continue
                        if date_to and stamp > str(date_to):
                            continue
                        row["file_id"] = row.get(
                            "manifest_record_id", "M:" + str(row["source_file_id"]))
                        row["backup_date"] = row.get("backup_date")
                        row["container_name"] = row.get("container_name")
                        yield row


def search_manifests(archive_root, query=None, *, limit=100, **filters):
    rows = []
    for row in iter_manifest_records(archive_root, query=query, **filters):
        rows.append(row)
        if len(rows) >= max(1, int(limit or 100)):
            break
    return rows


def find_manifest_record(archive_root, source_file_id, *, allowed_paths=None):
    requested = str(source_file_id)
    numeric = None
    if requested.startswith("M:"):
        numeric = int(requested.removeprefix("M:"))
    for row in iter_manifest_records(
            archive_root, allowed_paths=allowed_paths):
        if (str(row.get("manifest_record_id")) == requested
                or (numeric is not None
                    and int(row["source_file_id"]) == numeric)):
            return row
    return None
