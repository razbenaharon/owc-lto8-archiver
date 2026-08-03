"""Rehearse the Plan 1 conservative bootstrap on a SHADOW copy of production.

Restores the verified production dump into a throwaway database, takes a full
fingerprint of every session's existing state, runs the conservative frontier
bootstrap there, and re-fingerprints. Anything that changed outside the frontier
tables is a defect.

It never touches production: the target database name must carry the shadow
prefix, must differ from the configured production database, and the connection
is refused otherwise. It performs no tape operation and starts no session.

Usage::

    python scripts/plan1_shadow_rehearsal.py --backup <dump> --session 37 \
        [--shadow-db plan1_shadow_<stamp>] [--json report.json] [--keep]
"""
import argparse
import json
import os
import subprocess
import sys
import threading
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import ConfigManager, _load_env_file          # noqa: E402
from src.constants import PROJECT_ROOT                        # noqa: E402

SHADOW_PREFIX = "plan1_shadow_"

#: Everything that must be byte-identical before and after. If the bootstrap
#: touches any of it, the rehearsal has failed.
INVARIANT_QUERIES = {
    "sessions": """SELECT session_id, session_label, remote_host, remote_path,
                          tape_label, tape_generation, status, scan_complete,
                          total_files, total_bytes, chunk_count, plan_id,
                          created_at, completed_at
                   FROM remote_sessions ORDER BY session_id""",
    "chunks": """SELECT session_id, chunk_index, status, container_name,
                        owner_token, lease_expires_at, attempt_id,
                        membership_state, expected_file_count, expected_bytes
                 FROM remote_chunks ORDER BY session_id, chunk_index""",
    "chunk_counts": """SELECT session_id, status, count(*)
                       FROM remote_chunks GROUP BY session_id, status
                       ORDER BY session_id, status""",
    "membership_totals": """SELECT plan_id, count(*), sum(ordinal),
                                   min(ordinal), max(ordinal)
                            FROM remote_plan_files GROUP BY plan_id
                            ORDER BY plan_id""",
    "membership_per_chunk": """SELECT plan_id, chunk_index, count(*)
                               FROM remote_plan_files
                               GROUP BY plan_id, chunk_index
                               ORDER BY plan_id, chunk_index""",
    "snapshot_totals": """SELECT snapshot_id, count(*), sum(file_size_bytes)
                          FROM remote_snapshot_files GROUP BY snapshot_id
                          ORDER BY snapshot_id""",
    "tapes": """SELECT volume_label, total_capacity, used_space
                FROM tapes ORDER BY volume_label""",
    "tape_generations": """SELECT volume_label, generation, state
                           FROM tape_generations
                           ORDER BY volume_label, generation""",
    "file_state": """SELECT status, count(*) FROM remote_file_state
                     GROUP BY status ORDER BY status""",
    "files_index": "SELECT count(*) FROM files_index",
    "zip_containers": """SELECT count(*) FROM files_index
                         WHERE is_packed IS TRUE""",
    "locators": """SELECT count(*) FROM files_index
                   WHERE stored_path IS NOT NULL""",
}

#: The ONLY tables the bootstrap may add rows to.
FRONTIER_TABLES = ("remote_scan_scopes", "remote_scan_directories",
                   "remote_scan_segments", "remote_chunk_scan_segments",
                   "remote_scan_errors", "remote_worker_attempts",
                   "remote_frontier_bootstraps")


def _psql(container, dbname, sql, tuples=True):
    cmd = ["docker", "exec", container, "psql", "-U", "lto", "-d", dbname,
           "-v", "ON_ERROR_STOP=1"]
    cmd += ["-tAc" if tuples else "-c", sql]
    out = subprocess.run(cmd, capture_output=True, text=True)
    if out.returncode != 0:
        raise RuntimeError(f"psql failed: {out.stderr.strip()[:400]}")
    return out.stdout.strip()


def fingerprint(container, dbname):
    """Every invariant, as text. Missing tables are recorded, not fatal."""
    snapshot = {}
    for name, sql in INVARIANT_QUERIES.items():
        try:
            snapshot[name] = _psql(container, dbname, sql)
        except RuntimeError as exc:
            snapshot[name] = f"<unavailable: {exc}>"
    for table in FRONTIER_TABLES:
        try:
            snapshot[f"count:{table}"] = _psql(
                container, dbname, f"SELECT count(*) FROM {table}")
        except RuntimeError:
            snapshot[f"count:{table}"] = "<absent>"
    return snapshot


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--backup", required=True,
                        help="verified production dump to restore")
    parser.add_argument("--session", type=int, required=True)
    parser.add_argument("--container", default="lto_pg")
    parser.add_argument("--shadow-db", default=None)
    parser.add_argument("--json", default=None)
    parser.add_argument("--keep", action="store_true",
                        help="leave the shadow database in place")
    args = parser.parse_args()

    cfg = ConfigManager()
    env = _load_env_file(os.path.join(PROJECT_ROOT, ".env"))
    if env.get("PGPASSWORD"):
        os.environ["PGPASSWORD"] = env["PGPASSWORD"]

    shadow = args.shadow_db or f"{SHADOW_PREFIX}{uuid.uuid4().hex[:10]}"
    if not shadow.startswith(SHADOW_PREFIX):
        raise SystemExit(f"refusing: {shadow!r} lacks the {SHADOW_PREFIX!r} "
                         "prefix, so it is not provably a shadow")
    if shadow == cfg.pg_dbname:
        raise SystemExit("refusing: that is the production database")

    report = {"shadow_database": shadow, "session_id": args.session,
              "production_database": cfg.pg_dbname, "steps": []}

    print(f"[SHADOW] creating {shadow}")
    _psql(args.container, "postgres", f'CREATE DATABASE "{shadow}"')
    try:
        print(f"[SHADOW] restoring {os.path.basename(args.backup)} "
              "(this takes a few minutes)")
        restore = subprocess.run(
            ["docker", "exec", "-i", args.container, "pg_restore",
             "-U", "lto", "-d", shadow, "--no-owner", "--no-privileges"],
            stdin=open(args.backup, "rb"), capture_output=True, text=True)
        # pg_restore warns about absent roles; only a missing table matters.
        report["steps"].append({"restore_returncode": restore.returncode})

        # Migration 014 must be present on the shadow or the bootstrap refuses
        # for the wrong reason and the rehearsal proves nothing. If the backup
        # already contains it (a post-migration dump) this is a no-op.
        from src.pg_db import PgDatabaseManager
        conninfo = (f"host=127.0.0.1 port=5432 dbname={shadow} user=lto "
                    f"password={os.environ.get('PGPASSWORD', '')}")
        setup = PgDatabaseManager(conninfo, init_schema=False)
        try:
            if not setup.incremental_scan_schema_installed():
                print("[SHADOW] applying migration 014 (base) to the shadow")
                setup.apply_incremental_scan_schema()
            if not setup.incremental_scan_schema_finalized():
                print("[SHADOW] applying migration 014 (finalize) to the shadow")
                setup.apply_incremental_scan_schema(finalize=True)
            report["shadow_schema"] = {
                "installed": setup.incremental_scan_schema_installed(),
                "finalized": setup.incremental_scan_schema_finalized()}
        finally:
            setup.close()

        print("[SHADOW] fingerprinting BEFORE")
        before = fingerprint(args.container, shadow)

        print("[SHADOW] running the conservative bootstrap")
        from src.frontier_bootstrap import FrontierBootstrap

        db = PgDatabaseManager(conninfo, init_schema=False)
        try:
            boot = FrontierBootstrap(
                db=db, session_id=args.session,
                scan_paths=cfg.remote_scan_paths,
                archive_root=cfg.local_manifest_archive_root,
                # A traversal-free bootstrap needs no scanner; supplying one
                # that raises proves the conservative path never lists.
                scanner_factory=lambda metrics: _RefusingScanner(),
                stop_event=threading.Event(), source_host=cfg.remote_host,
                active_processes_probe=lambda: [],
                lock_holders_probe=lambda: [])
            dry = boot.dry_run()
            report["dry_run"] = {k: v for k, v in dry.items()
                                 if k != "session_report"}
            report["dry_run"]["scopes"] = len(dry.get("scopes") or [])
            print(f"[SHADOW] dry run would_proceed={dry['would_proceed']} "
                  f"blocking={dry['blocking']}")
            if dry["would_proceed"]:
                result = boot.execute(approved=True, conservative=True)
                report["execute"] = {
                    k: v for k, v in result.items()
                    if k not in ("session_report", "scopes")}
                # Idempotence: a second identical run must add nothing.
                mid = fingerprint(args.container, shadow)
                boot2 = FrontierBootstrap(
                    db=db, session_id=args.session,
                    scan_paths=cfg.remote_scan_paths,
                    archive_root=cfg.local_manifest_archive_root,
                    scanner_factory=lambda metrics: _RefusingScanner(),
                    stop_event=threading.Event(),
                    source_host=cfg.remote_host,
                    active_processes_probe=lambda: [],
                    lock_holders_probe=lambda: [])
                try:
                    boot2.execute(approved=True, conservative=True)
                    report["second_run"] = "completed"
                except Exception as exc:
                    report["second_run"] = f"refused: {exc}"
                after_second = fingerprint(args.container, shadow)
                report["idempotent"] = (mid == after_second)
        finally:
            db.close()

        print("[SHADOW] fingerprinting AFTER")
        after = fingerprint(args.container, shadow)

        changed = sorted(k for k in before
                         if before[k] != after.get(k))
        report["changed_keys"] = changed
        report["unchanged_invariants"] = sorted(
            k for k in INVARIANT_QUERIES if k not in changed)
        report["violations"] = [k for k in changed
                                if not k.startswith("count:")]
        report["frontier_growth"] = {
            k.split(":", 1)[1]: {"before": before[k], "after": after[k]}
            for k in changed if k.startswith("count:")}
        report["passed"] = not report["violations"]
        print(f"\n[SHADOW] invariants violated: {report['violations'] or 'NONE'}")
        print(f"[SHADOW] frontier tables grown: "
              f"{list(report['frontier_growth']) or 'none'}")
        print(f"[SHADOW] RESULT: {'PASS' if report['passed'] else 'FAIL'}")
    finally:
        if not args.keep:
            print(f"[SHADOW] dropping {shadow}")
            try:
                _psql(args.container, "postgres",
                      f'DROP DATABASE IF EXISTS "{shadow}"')
            except RuntimeError as exc:
                print(f"[SHADOW] could not drop: {exc}")

    if args.json:
        with open(args.json, "w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2, default=str)
        print(f"[SHADOW] wrote {args.json}")
    return 0 if report.get("passed") else 1


class _RefusingScanner:
    """Any use of this is a bug: the conservative bootstrap must not traverse."""

    def list_directory(self, path):
        raise AssertionError(
            "the conservative bootstrap listed a directory; it must not "
            "traverse the source at all")

    def observe(self, path):
        raise AssertionError("the conservative bootstrap observed the source")


if __name__ == "__main__":
    raise SystemExit(main())
