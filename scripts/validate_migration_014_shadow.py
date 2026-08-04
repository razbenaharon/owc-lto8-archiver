"""Validate migration 014 on a disposable SHADOW database, with evidence.

Plan 1 review. The isolated tests prove behaviour; this produces the artifact a
reviewer can read: the exact comparison queries, their results before and after
the migration, and an explicit verdict per claim.

It creates its own throwaway database, populates it with data shaped like a
real interrupted session, migrates it, compares, and drops it. It NEVER touches
the production catalog:

* the target database name is generated per run and must not already exist;
* the connection is whatever PG* points at — pass a disposable server;
* a database named ``lto_archive`` is refused outright.

    # against a disposable server (NOT the production lto_pg container)
    set PGHOST=127.0.0.1 & set PGPORT=15432 & set PGUSER=lto
    set PGPASSWORD=... & python scripts/validate_migration_014_shadow.py
    python scripts/validate_migration_014_shadow.py --json shadow_014.json
"""
import argparse
import json
import os
import sys
import uuid

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from src.pg_bulk import build_conninfo                       # noqa: E402

#: Never migrate these, whatever the environment says.
PROTECTED_DATABASES = {"lto_archive", "postgres", "template0", "template1"}

#: Each entry is (claim, SQL). Every one is run before AND after the migration
#: and the results must be identical — that is the whole test.
INVARIANT_QUERIES = [
    ("sessions are unchanged",
     """SELECT session_id, session_label, remote_path, tape_label,
               scan_complete, status, plan_id, total_files, total_bytes,
               chunk_count, tape_generation
        FROM remote_sessions ORDER BY session_id"""),
    ("chunk states are unchanged (including 'backing')",
     """SELECT session_id, chunk_index, status, error_msg
        FROM remote_chunks ORDER BY session_id, chunk_index"""),
    ("plan membership and ordinals are unchanged",
     """SELECT plan_id, snapshot_file_id, chunk_index, ordinal
        FROM remote_plan_files ORDER BY plan_file_id"""),
    ("snapshot files are unchanged",
     """SELECT snapshot_id, remote_path, file_size_bytes
        FROM remote_snapshot_files ORDER BY snapshot_file_id"""),
    ("per-file transfer state is unchanged",
     """SELECT session_id, plan_file_id, status
        FROM remote_file_state ORDER BY plan_file_id"""),
    ("tapes and their generations are unchanged",
     """SELECT t.volume_label, t.total_capacity, t.status,
               t.current_generation, g.generation, g.state
        FROM tapes t LEFT JOIN tape_generations g ON g.tape_id = t.tape_id
        ORDER BY t.volume_label, g.generation"""),
    ("the ZIP/loose catalog is unchanged",
     """SELECT COUNT(*) AS files, COALESCE(SUM(file_size_bytes), 0) AS bytes
        FROM files_index"""),
]

#: Run only AFTER the migration; each must return zero rows / the stated value.
POST_MIGRATION_QUERIES = [
    ("no chunk acquired an owner, lease, attempt or membership seal",
     """SELECT COUNT(*) AS n FROM remote_chunks
        WHERE owner_token IS NOT NULL OR lease_expires_at IS NOT NULL
           OR attempt_id IS NOT NULL OR membership_state IS NOT NULL
           OR expected_file_count IS NOT NULL OR expected_bytes IS NOT NULL""",
     0),
    ("no directory was marked scanned merely because catalog rows exist",
     "SELECT COUNT(*) AS n FROM remote_scan_directories", 0),
    ("no segment was invented",
     "SELECT COUNT(*) AS n FROM remote_scan_segments", 0),
    ("no scope was created without a bootstrap",
     "SELECT COUNT(*) AS n FROM remote_scan_scopes", 0),
    ("no chunk/segment membership was created",
     "SELECT COUNT(*) AS n FROM remote_chunk_scan_segments", 0),
    ("no bootstrap run was started",
     "SELECT COUNT(*) AS n FROM remote_frontier_bootstraps", 0),
    ("the 'backing' chunk is still backing",
     """SELECT COUNT(*) AS n FROM remote_chunks WHERE status='backing'""", 1),
    ("the duplicate-ordinal guard exists",
     """SELECT COUNT(*) AS n FROM pg_indexes
        WHERE indexname='uq_remote_plan_files_chunk_ordinal'""", 1),
]


def _require_psycopg():
    try:
        import psycopg
        from psycopg.rows import dict_row
        return psycopg, dict_row
    except ImportError:
        raise SystemExit("[SHADOW] psycopg is required.")


def _fetch(conn, sql):
    return [dict(row) for row in conn.execute(sql).fetchall()]


def build_legacy_fixture(db, exec_sql):
    """Data shaped like a session interrupted mid-flight."""
    db.register_tape("SHADOW_LEGACY", 12000)
    session_id = db.create_remote_streaming_session(
        session_label="REMOTE_shadow_legacy", remote_host="srv02",
        remote_user="lto", remote_path="/vault/a\n/vault/b",
        tape_label="SHADOW_LEGACY", staging_dir="C:\\stage")
    for chunk_index in range(6):
        db.append_remote_streaming_chunk(
            session_id, chunk_index,
            [(chunk_index, f"/vault/a/c{chunk_index}/f{i}", f"f{i}", 1000 + i)
             for i in range(8)])
    for chunk_index, status in ((0, "done"), (1, "done"), (2, "done"),
                                (3, "backing"), (4, "fetch_failed")):
        exec_sql("UPDATE remote_chunks SET status=%s "
                 "WHERE session_id=%s AND chunk_index=%s",
                 (status, session_id, chunk_index))
    db.mark_remote_scan_complete(session_id)
    # A loose catalog row, so "the ZIP/loose catalog is unchanged" has content.
    db.bulk_upsert_files([{
        "original_path": "/vault/a/c0/f0", "file_size_bytes": 1000,
        "tape_label": "SHADOW_LEGACY", "source_host": "srv02",
        "is_packed": False, "container_name": None,
        "stored_path": "/vault/a/c0/f0"}])
    return session_id


def main(argv=None):
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--json", metavar="PATH",
                        help="write the full evidence record")
    parser.add_argument("--keep", action="store_true",
                        help="do not drop the shadow database (for inspection)")
    args = parser.parse_args(argv)

    psycopg, dict_row = _require_psycopg()
    dbname = f"lto_shadow_014_{uuid.uuid4().hex[:10]}"
    if dbname in PROTECTED_DATABASES:                    # belt and braces
        raise SystemExit("[SHADOW] refusing a protected database name")

    admin = build_conninfo(dbname="postgres")
    target_env = os.environ.get("PGDATABASE", "")
    if target_env in PROTECTED_DATABASES - {"postgres"}:
        raise SystemExit(
            f"[SHADOW] PGDATABASE={target_env!r} names a protected database.")

    report = {"shadow_database": dbname, "invariants": [], "post": [],
              "verdict": "unknown"}
    print(f"[SHADOW] creating disposable database {dbname}")
    with psycopg.connect(admin, autocommit=True) as conn:
        conn.execute(f'CREATE DATABASE "{dbname}"')
    conninfo = build_conninfo(dbname=dbname)

    try:
        from src.pg_db import PgDatabaseManager
        db = PgDatabaseManager(conninfo)

        def exec_sql(sql, params=()):
            with psycopg.connect(conninfo, autocommit=True) as c:
                c.execute(sql, params)

        session_id = build_legacy_fixture(db, exec_sql)
        report["session_id"] = session_id
        print(f"[SHADOW] built a legacy session (id {session_id})")

        print("[SHADOW] pre-migration state:")
        print(f"           installed={db.incremental_scan_schema_installed()} "
              f"finalized={db.incremental_scan_schema_finalized()}")
        report["installed_before"] = db.incremental_scan_schema_installed()

        with psycopg.connect(conninfo, autocommit=True,
                             row_factory=dict_row) as conn:
            before = {claim: _fetch(conn, sql)
                      for claim, sql in INVARIANT_QUERIES}

        print("[SHADOW] applying migration 014 (base + finalize)")
        applied = db.apply_incremental_scan_schema(finalize=True)
        report["applied"] = applied
        report["installed_after"] = db.incremental_scan_schema_installed()
        report["finalized_after"] = db.incremental_scan_schema_finalized()

        with psycopg.connect(conninfo, autocommit=True,
                             row_factory=dict_row) as conn:
            after = {claim: _fetch(conn, sql)
                     for claim, sql in INVARIANT_QUERIES}

        print()
        print("=" * 78)
        print("INVARIANTS — identical before and after")
        print("=" * 78)
        all_ok = True
        for claim, sql in INVARIANT_QUERIES:
            same = before[claim] == after[claim]
            all_ok &= same
            print(f"  [{'ok  ' if same else 'FAIL'}] {claim}")
            print(f"         rows before={len(before[claim])} "
                  f"after={len(after[claim])}")
            report["invariants"].append({
                "claim": claim, "sql": " ".join(sql.split()),
                "identical": same,
                "rows_before": len(before[claim]),
                "rows_after": len(after[claim]),
                "before": before[claim] if not same else None,
                "after": after[claim] if not same else None,
            })

        print()
        print("=" * 78)
        print("POST-MIGRATION — nothing was inferred or invented")
        print("=" * 78)
        with psycopg.connect(conninfo, autocommit=True,
                             row_factory=dict_row) as conn:
            for claim, sql, expected in POST_MIGRATION_QUERIES:
                value = _fetch(conn, sql)[0]["n"]
                ok = value == expected
                all_ok &= ok
                print(f"  [{'ok  ' if ok else 'FAIL'}] {claim}")
                print(f"         expected={expected} actual={value}")
                report["post"].append({
                    "claim": claim, "sql": " ".join(sql.split()),
                    "expected": expected, "actual": value, "ok": ok})

        # Production has one scanner, so migration readiness is a hard
        # prerequisite rather than a feature-selection decision.
        from src.scan_frontier import incremental_scan_schema_ready
        schema_ready, schema_reason = incremental_scan_schema_ready(db)
        all_ok &= schema_ready
        print()
        print(f"  [{'ok  ' if schema_ready else 'FAIL'}] the persistent "
              f"frontier schema is usable (reason={schema_reason})")
        report["frontier_schema_ready"] = schema_ready
        report["frontier_schema_reason"] = schema_reason

        # And the frontier has enough state to restart once bootstrapped.
        db.create_scan_scopes(session_id, ["/vault/a"])
        scope = db.get_scan_scopes(session_id)[0]
        db.enqueue_scan_directories(scope["scan_scope_id"], [("/vault/a", 0)])
        claimed = db.claim_next_directory(session_id, "shadow", "att")
        db.publish_scan_segment(
            claimed["scan_directory_id"], first_scan_ordinal=0,
            last_scan_ordinal=7, locator="scan_segments/s/d/seg.jsonl.zst",
            file_count=8, byte_count=8000)
        db.mark_directory_partial(claimed["scan_directory_id"], "shadow")
        resumable = db.claim_next_directory(session_id, "shadow2", "att2")
        restart_ok = (resumable is not None
                      and resumable["scan_directory_id"]
                      == claimed["scan_directory_id"])
        all_ok &= restart_ok
        print(f"  [{'ok  ' if restart_ok else 'FAIL'}] frontier state is "
              "sufficient to resume the interrupted directory")
        report["restart_resumes_partial_directory"] = restart_ok

        db.close()
        report["verdict"] = "PASSED" if all_ok else "FAILED"
        print()
        print("=" * 78)
        print(f"  MIGRATION 014 SHADOW VALIDATION: {report['verdict']}")
        print("=" * 78)
    finally:
        if not args.keep:
            with psycopg.connect(admin, autocommit=True) as conn:
                conn.execute(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE datname = %s AND pid <> pg_backend_pid()", (dbname,))
                conn.execute(f'DROP DATABASE IF EXISTS "{dbname}"')
            print(f"[SHADOW] dropped {dbname}")
        else:
            print(f"[SHADOW] kept {dbname} for inspection")

    if args.json:
        with open(args.json, "w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2, default=str)
        print(f"[SHADOW] evidence written to {args.json}")
    return 0 if report["verdict"] == "PASSED" else 1


if __name__ == "__main__":
    sys.exit(main())
