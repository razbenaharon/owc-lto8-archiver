"""Reconcile the catalog, the per-file manifests, and the Storage Map source.

One command proves the post-Plan-4 source-of-truth boundaries hold:

1. **No per-small-file inventory in PostgreSQL** — zero packed `files_index`
   rows below the manifest threshold remain.
2. **Manifests are complete** — every pruned export's on-disk JSONL.zst
   segments parse, and their row/byte totals equal the export ledger, the
   folder aggregates, and (with ``--heavy``) the recorded SHA-256s.
3. **Tape accounting is preserved** — per-tape used space computed the way
   the archiver computes it (live rows + pruned-manifest aggregates) matches
   the `tapes.used_space` counters.
4. **The Storage Map is derivable** — the coverage SQL the web app runs
   returns the same grand totals as the DB-side sources it aggregates, so a
   Storage Map number can always be traced back to catalog + manifests.
5. **Closed tapes are protected** — every tape marked ``full`` reports zero
   available bytes from ``tape_budget_bytes``.

Exit code 0 only when every check passes. Run it after any export/prune and
before any run that changes tape state:

    python scripts/validate_archive_reconciliation.py [--heavy] [--json]
"""
import argparse
import hashlib
import io
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import ConfigManager  # noqa: E402
from src.constants import tape_budget_bytes  # noqa: E402
from src.pg_bulk import build_conninfo  # noqa: E402
from src.local_manifest_archive import (  # noqa: E402
    SMALL_FILE_THRESHOLD_BYTES, _connect, _used_space_by_tape)

try:
    import zstandard as zstd
except ImportError:  # pragma: no cover
    zstd = None


def _conninfo(cfg):
    section = cfg.config["DATABASE"] if cfg.config.has_section("DATABASE") else {}
    return build_conninfo(
        host=section.get("host", "localhost"),
        port=section.get("port", "5432"),
        dbname=section.get("dbname", "lto_archive"),
        user=section.get("user", "lto"),
        sslmode=section.get("sslmode", "prefer"))


def _segment_totals(path, *, heavy=False, expected_sha=None):
    """Parse one JSONL.zst segment fully; return (rows, bytes, dirs)."""
    if zstd is None:
        raise RuntimeError("zstandard is required")
    if heavy and expected_sha:
        digest = hashlib.sha256()
        with open(path, "rb") as handle:
            for block in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(block)
        if digest.hexdigest() != expected_sha:
            raise RuntimeError(f"SHA-256 mismatch: {path}")
    rows = total = 0
    dirs = set()
    with open(path, "rb") as raw:
        reader = zstd.ZstdDecompressor().stream_reader(raw)
        with io.TextIOWrapper(reader, encoding="utf-8") as text:
            for line in text:
                item = json.loads(line)
                rows += 1
                total += int(item.get("file_size_bytes") or 0)
                original = str(item.get("original_path") or "").replace("\\", "/")
                dirs.add(original.rsplit("/", 1)[0] if "/" in original else "ROOT")
    return rows, total, dirs


def run_checks(conninfo, archive_root, *, heavy=False,
               threshold_bytes=SMALL_FILE_THRESHOLD_BYTES):
    checks = []

    def check(name, passed, detail):
        checks.append({"check": name, "passed": bool(passed), "detail": detail})

    with _connect(conninfo, autocommit=True) as conn:
        # 1. No per-small-file inventory left in PostgreSQL.
        residual = conn.execute(
            """SELECT COUNT(*) AS n FROM files_index
               WHERE is_packed AND file_size_bytes < %s""",
            (threshold_bytes,)).fetchone()["n"]
        check("no_per_small_file_rows", residual == 0,
              {"residual_packed_small_rows": int(residual)})

        exports = conn.execute(
            """SELECT export_id, archive_root, status, eligible_rows,
                      eligible_bytes, validation_passed
               FROM local_manifest_exports
               WHERE status='pruned' ORDER BY export_id""").fetchall()
        check("pruned_export_exists", bool(exports),
              {"pruned_exports": [int(e["export_id"]) for e in exports]})

        # 2. Manifest completeness: disk == ledger == aggregates.
        for export in exports:
            export_id = int(export["export_id"])
            segments = conn.execute(
                """SELECT manifest_relpath, row_count, original_bytes,
                          sha256_hex
                   FROM local_manifest_segments WHERE export_id=%s""",
                (export_id,)).fetchall()
            disk_rows = disk_bytes = 0
            disk_dirs = set()
            errors = []
            for segment in segments:
                path = os.path.join(export["archive_root"],
                                    segment["manifest_relpath"])
                try:
                    rows, total, dirs = _segment_totals(
                        path, heavy=heavy,
                        expected_sha=segment["sha256_hex"])
                except Exception as exc:  # capture, keep checking the rest
                    errors.append(f"{segment['manifest_relpath']}: {exc}")
                    continue
                if rows != int(segment["row_count"]) or total != int(
                        segment["original_bytes"]):
                    errors.append(
                        f"{segment['manifest_relpath']}: row/byte mismatch")
                disk_rows += rows
                disk_bytes += total
                disk_dirs |= dirs
            aggregates = conn.execute(
                """SELECT COALESCE(SUM(direct_file_count),0) AS rows,
                          COALESCE(SUM(direct_bytes),0) AS bytes,
                          COUNT(*) FILTER (WHERE direct_file_count > 0)
                            AS directories
                   FROM local_manifest_folder_aggregates
                   WHERE export_id=%s""", (export_id,)).fetchone()
            check(f"export_{export_id}_manifests_complete",
                  not errors
                  and disk_rows == int(export["eligible_rows"] or 0)
                  and disk_bytes == int(export["eligible_bytes"] or 0)
                  and disk_rows == int(aggregates["rows"] or 0)
                  and disk_bytes == int(aggregates["bytes"] or 0)
                  and len(disk_dirs) == int(aggregates["directories"] or 0),
                  {"segments": len(segments),
                   "disk_rows": disk_rows, "disk_bytes": disk_bytes,
                   "ledger_rows": int(export["eligible_rows"] or 0),
                   "aggregate_rows": int(aggregates["rows"] or 0),
                   "disk_directories": len(disk_dirs),
                   "aggregate_directories": int(aggregates["directories"] or 0),
                   "validation_passed": bool(export["validation_passed"]),
                   "errors": errors[:10]})

        # 3. Tape accounting preserved across the prune.
        tapes = conn.execute(
            """SELECT volume_label, total_capacity, used_space, status,
                      status_reason FROM tapes ORDER BY volume_label""").fetchall()
        labels = [t["volume_label"] for t in tapes]
        modeled = _used_space_by_tape(conn, labels)
        accounting_detail = {}
        accounting_ok = True
        for tape in tapes:
            label = tape["volume_label"]
            recorded = int(tape["used_space"] or 0)
            computed = int(modeled.get(label, 0))
            accounting_detail[label] = {
                "recorded_used_space": recorded, "computed": computed,
                "match": recorded == computed}
            if recorded != computed:
                accounting_ok = False
        check("tape_accounting_preserved", accounting_ok, accounting_detail)

        # 4. Storage Map derivability: the coverage SQL grand totals equal
        # the DB-side sources it merges (files_index POSIX rows + catalog
        # small-file totals + pruned-manifest uncovered aggregates).
        from storage_map.webapp.coverage import COVERAGE_SQL
        coverage = conn.execute(
            COVERAGE_SQL % {"threshold_bytes": threshold_bytes,
                            "max_segs": 64}).fetchall()
        coverage_bytes = sum(int(r["tape_bytes"] or 0) for r in coverage)
        coverage_files = sum(int(r["tape_files"] or 0) for r in coverage)
        source = conn.execute(
            """WITH fi AS (
                   SELECT COALESCE(SUM(sub.bytes),0) AS bytes,
                          COUNT(*) AS files
                   FROM (SELECT MAX(file_size_bytes) AS bytes
                         FROM files_index WHERE original_path LIKE '/%%'
                         GROUP BY source_host, original_path) sub
               ), dti AS (
                   SELECT COALESCE(SUM(direct_small_file_bytes),0) AS bytes,
                          COALESCE(SUM(direct_small_file_count),0) AS files
                   FROM directory_tree_index
                   WHERE original_dir_path LIKE '/%%'
               ), pruned AS (
                   SELECT COALESCE(SUM(a.direct_uncovered_bytes),0) AS bytes,
                          COALESCE(SUM(a.direct_uncovered_file_count),0) AS files
                   FROM local_manifest_folder_aggregates a
                   JOIN local_manifest_exports e ON e.export_id=a.export_id
                   WHERE e.status='pruned' AND a.original_dir_path LIKE '/%%'
               )
               SELECT (SELECT bytes FROM fi)+(SELECT bytes FROM dti)
                        +(SELECT bytes FROM pruned) AS bytes,
                      (SELECT files FROM fi)+(SELECT files FROM dti)
                        +(SELECT files FROM pruned) AS files""").fetchone()
        check("storage_map_derivable",
              coverage_bytes == int(source["bytes"] or 0)
              and coverage_files == int(source["files"] or 0),
              {"coverage_bytes": coverage_bytes,
               "coverage_files": coverage_files,
               "source_bytes": int(source["bytes"] or 0),
               "source_files": int(source["files"] or 0)})

        # 5. Closed tapes refuse writes.
        protection_detail = {}
        protection_ok = True
        for tape in tapes:
            _, available = tape_budget_bytes(
                tape["total_capacity"], tape["used_space"] or 0,
                status=tape["status"])
            protected = str(tape["status"] or "").lower() == "full"
            protection_detail[tape["volume_label"]] = {
                "status": tape["status"], "reason": tape["status_reason"],
                "available_bytes": available}
            if protected and available != 0:
                protection_ok = False
        check("closed_tapes_report_zero_budget", protection_ok,
              protection_detail)

    return {"passed": all(c["passed"] for c in checks), "checks": checks}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--heavy", action="store_true",
                        help="Also verify segment SHA-256s (reads all bytes).")
    parser.add_argument("--json", action="store_true",
                        help="Print the full JSON report.")
    args = parser.parse_args(argv)
    cfg = ConfigManager()
    report = run_checks(_conninfo(cfg), cfg.local_manifest_archive_root,
                        heavy=args.heavy)
    if args.json:
        print(json.dumps(report, indent=2, default=str))
    else:
        for item in report["checks"]:
            print(f"[{'PASS' if item['passed'] else 'FAIL'}] {item['check']}")
        print("RESULT:", "PASS" if report["passed"] else "FAIL")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
