"""Plan 1 rehearsal runner — collect the evidence Plan 3 reviews.

Plan 1, Task 4.3. The rehearsal has three stages, and only the first two can run
without the physical drive:

  1. offline        the full test suite (fakes only; no drive, no PostgreSQL)
  2. isolated PG    migration 014 + the frontier repository against a throwaway
                    database
  3. hardware pilot ONE finite write group and a local ZIP/loose restore,
                    against the real cartridge

Stage 3 is deliberately NOT automated here. It writes to a real cartridge, and
the no-physical-intervention policy says a change that risks a state only a
human at the drive can clear is a bad trade. So this script prints the checklist
and the preconditions, and an operator performs it deliberately.

    python scripts/plan1_rehearsal.py                  # stages 1-2 + checklist
    python scripts/plan1_rehearsal.py --offline-only
    python scripts/plan1_rehearsal.py --json report.json
"""
import argparse
import json
import os
import subprocess
import sys
import time

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

#: The test files that constitute the Plan 1 evidence, in dependency order.
PLAN1_SUITES = [
    ("characterization", "tests/test_pipeline_characterization.py"),
    ("scan telemetry", "tests/test_scan_metrics.py"),
    ("frontier schema gate", "tests/test_scan_mode_gate.py"),
    ("scan coordinator", "tests/test_scan_frontier.py"),
    ("lifecycle transitions", "tests/test_lifecycle_transitions.py"),
    ("finite-group tape path", "tests/test_finite_group_only_tape_path.py"),
    ("incident invariants", "tests/test_incident_invariants.py"),
    ("migration 014", "tests/test_migration_014.py"),
    ("scan artifacts", "tests/test_archive_artifacts.py"),
    ("directory frontier", "tests/test_incremental_scan_frontier.py"),
    ("segment publication", "tests/test_segment_chunk_publication.py"),
    ("claims + reconciliation", "tests/test_claims_and_reconciliation.py"),
    ("session report", "tests/test_session_frontier_report.py"),
    ("frontier bootstrap", "tests/test_frontier_bootstrap.py"),
    ("execution contract", "tests/test_execution_contract.py"),
    ("status vocabulary", "tests/test_status_vocabulary.py"),
    ("end-to-end rehearsal", "tests/test_plan1_rehearsal.py"),
]

#: Pre-existing regressions that must still pass untouched.
REGRESSION_SUITES = [
    ("ZIP packing / restore", "tests/test_packer_on_existing.py"),
    ("loose restore", "tests/test_retriever_restore.py"),
    ("remote hardening", "tests/test_remote_hardening.py"),
    ("remote failure hardening", "tests/test_remote_failure_hardening.py"),
    ("ready queue (phase 4)", "tests/test_phase4_ready_queue.py"),
    ("control signals (phase 4.5)", "tests/test_phase45_control_signals.py"),
    ("ownership hardening", "tests/test_phase35_ownership_hardening.py"),
    ("staging space", "tests/test_staging_space.py"),
    ("reporting", "tests/test_reporting_and_robocopy.py"),
    ("resource governor", "tests/test_resource_governor.py"),
    ("session reconcile", "tests/test_session_reconcile.py"),
]

#: Stage 3 — the operator-supervised tape run.
#:
#: Each entry is (claim, offline proof, what the hardware run ADDS). The middle
#: column is the point: almost every claim here is ALREADY proven offline
#: against fakes. The tape run does not re-derive them — it confirms a real IBM
#: drive and a real cartridge behave as the fakes assume. Listing the claims
#: without that distinction would imply Plan 1 is unverified, which is not what
#: the evidence says.
HARDWARE_REHEARSAL_CLAIMS = [
    ("the target cartridge is announced BEFORE expensive fetch work",
     "test_incident_invariants.py::CartridgeIdentityTests::"
     "test_both_loops_announce_the_target_cartridge_without_reading_it",
     "that the announcement appears early in the real console output"),
    ("no automatic eject",
     "test_execution_contract.py::Clause09_NoAutoEjectTests",
     "that LtfsCmdEject is never invoked against the real drive"),
    ("no LTFS activity during idle periods",
     "test_execution_contract.py::Clause03/Clause04",
     "that the IBM LTFS log shows no drive activity while fetching/packing"),
    ("one ownership acquisition per finite write group",
     "test_execution_contract.py::Clause05to08::"
     "test_one_ownership_acquisition_and_one_gate_for_five_chunks",
     "that the real Global mutex generation increments once per group"),
    ("one readiness and one cartridge verification per group",
     "same test (readiness=1, cartridge=1 for a 5-chunk group)",
     "that LtfsCmdDrives.exe runs exactly once per group"),
    ("no ownership release between group members",
     "test_execution_contract.py::Clause05to08::"
     "test_ownership_is_never_released_between_members",
     "that the readiness cache survives real consecutive writes"),
    ("fail-closed handling of wrong media",
     "test_incident_invariants.py::CartridgeIdentityTests",
     "that a deliberately wrong cartridge is REFUSED by the real gate, with "
     "nothing written and nothing cataloged"),
    ("fail-closed handling of an ambiguous write",
     "test_execution_contract.py::Clause10_BackingIsAmbiguousTests",
     "DO NOT provoke a real write error — it can latch the cartridge "
     "read-only (incident 010). Confirm only that a clean run leaves no chunk "
     "in 'backing'"),
    ("no automatic reset of backing",
     "test_execution_contract.py::Clause10 + test_status_vocabulary.py",
     "that a resumed run refuses to start while a backing chunk exists"),
    ("safe restart after interruption",
     "test_incremental_scan_frontier.py::ContinuationTests",
     "that a Ctrl+C at a chunk boundary leaves a resumable session and a "
     "synced LTFS index"),
]

#: Environment facts no test can establish. Confirm BEFORE the supervised run.
HARDWARE_PRECONDITIONS = [
    "Confirm the mount letter with Test-Path AND the IBM LTFS log — incident "
    "011: Test-Path returned True against a dead mount.",
    "Confirm the loaded cartridge is a PILOT tape, never a production one.",
    "Confirm sync_type=time@5 from LTFS event 61259, NOT ltfs.conf.local — "
    "incident 006: an MSI reinstall silently reset it.",
    "Confirm no restart is staged (SCCM Software Center) — incident 005: a "
    "60-second warning against a ~70-minute chunk cycle.",
    "Use a SMALL synthetic source tree: nested dirs, an empty dir, one large "
    "file, one name with a space, one non-ASCII name.",
    "Restore the written chunk locally (ZIP and loose) and diff it.",
    "Confirm SUMMARY.csv gained the scan_* columns and that no file or "
    "directory NAME appears in any of them.",
    "Confirm backup_logs/tape_write/ holds the durable raw robocopy log "
    "(incident 009).",
]

#: Neither offline work nor the tape run establishes these. Recorded so they
#: are never mistaken for covered.
REMAINS_UNVERIFIED = [
    "Real-drive behaviour under a latching WRITE-PERM error (incident 010). "
    "Provoking one risks the cartridge, so that path is proven against fakes "
    "only — deliberately.",
    "Behaviour at production scale (~82M files, multi-day sessions). The "
    "frontier is measured on synthetic data and a shadow database only.",
    "Production-scale frontier traversal. Migration 014 is active, but Session "
    "37 has only received the conservative structure-only bootstrap.",
]


def run_suite(paths, label, verbose=False):
    """Run pytest over the given files and return a result record."""
    started = time.time()
    command = [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider",
               *paths]
    process = subprocess.run(command, cwd=PROJECT_ROOT, capture_output=True,
                             text=True, encoding="utf-8", errors="replace")
    tail = (process.stdout or "").strip().splitlines()
    summary = tail[-1] if tail else ""
    record = {
        "stage": label,
        "passed": process.returncode == 0,
        "summary": summary,
        "seconds": round(time.time() - started, 1),
    }
    if verbose or process.returncode != 0:
        record["output"] = (process.stdout or "")[-4000:]
    return record


def main(argv=None):
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--offline-only", action="store_true",
                        help="skip the isolated-PostgreSQL stage")
    parser.add_argument("--verbose", action="store_true")
    parser.add_argument("--json", metavar="PATH",
                        help="write the full evidence record as JSON")
    args = parser.parse_args(argv)

    report = {"stages": [],
              "hardware_validation":
                  "NOT RUN, OPERATOR-SUPERVISED TAPE WRITE REQUIRED LATER"}

    print("=" * 78)
    print("PLAN 1 REHEARSAL — stage 1: offline (fakes only, no drive, no DB)")
    print("=" * 78)
    for label, path in PLAN1_SUITES + REGRESSION_SUITES:
        record = run_suite([path], label, verbose=args.verbose)
        report["stages"].append(record)
        mark = "ok  " if record["passed"] else "FAIL"
        print(f"  [{mark}] {label:<28} {record['summary']}")
        if not record["passed"] and record.get("output"):
            print(record["output"])

    if not args.offline_only:
        print()
        print("=" * 78)
        print("PLAN 1 REHEARSAL — stage 2: isolated PostgreSQL")
        print("=" * 78)
        record = run_suite(["tests/test_pg_integration.py"],
                           "isolated PostgreSQL", verbose=args.verbose)
        ran_nothing = ("skipped" in record["summary"]
                       and "passed" not in record["summary"])
        if ran_nothing:
            # A stage that ran nothing must not read as a stage that passed.
            # "pytest exited 0" and "the behaviour was demonstrated" are
            # different claims, and only the second one is evidence.
            record["evidence"] = "ABSENT (no PostgreSQL server reachable)"
            record["passed"] = False
        report["stages"].append(record)
        mark = ("ok  " if record["passed"]
                else ("----" if ran_nothing else "FAIL"))
        print(f"  [{mark}] isolated PostgreSQL          {record['summary']}")
        if ran_nothing:
            print("         NOTE: every test skipped — no PostgreSQL server was")
            print("         reachable. Start it (docker compose up -d db) and")
            print("         re-run; a skipped stage is NOT evidence.")

    offline_ok = all(stage["passed"] for stage in report["stages"])
    missing = [stage["stage"] for stage in report["stages"]
               if stage.get("evidence", "").startswith("ABSENT")]
    failed = [stage["stage"] for stage in report["stages"]
              if not stage["passed"] and stage["stage"] not in missing]
    report["offline_and_isolated_passed"] = offline_ok
    report["stages_without_evidence"] = missing
    report["stages_failed"] = failed

    print()
    print("=" * 78)
    print("PLAN 1 REHEARSAL — stage 3: OPERATOR-SUPERVISED TAPE RUN")
    print("=" * 78)
    print("  Hardware validation: NOT RUN, OPERATOR-SUPERVISED TAPE WRITE")
    print("  REQUIRED LATER. This stage is not runnable from here and is not")
    print("  a Plan 1 code defect — it is a production-activation gate.")
    print()
    print("  Preconditions to confirm first:")
    for index, item in enumerate(HARDWARE_PRECONDITIONS, 1):
        print(f"    {index:>2}. [ ] {item}")
    print()
    print("  Claims — each is ALREADY PROVEN OFFLINE; the tape run only")
    print("  confirms a real drive matches the fakes:")
    for index, (claim, proof, adds) in enumerate(HARDWARE_REHEARSAL_CLAIMS, 1):
        print(f"    {index:>2}. {claim}")
        print(f"        offline proof : {proof}")
        print(f"        tape run adds : {adds}")
    print()
    print("  REMAINS UNVERIFIED even after the tape run:")
    for item in REMAINS_UNVERIFIED:
        print(f"    -  {item}")
    report["hardware_preconditions"] = HARDWARE_PRECONDITIONS
    report["hardware_claims"] = [
        {"claim": c, "offline_proof": p, "tape_run_adds": a}
        for c, p, a in HARDWARE_REHEARSAL_CLAIMS]
    report["remains_unverified"] = REMAINS_UNVERIFIED

    print()
    print("=" * 78)
    if failed:
        verdict = f"FAILED ({', '.join(failed)})"
    elif missing:
        verdict = f"INCOMPLETE — no evidence for: {', '.join(missing)}"
    else:
        verdict = "PASSED"
    print(f"  Stages 1-2: {verdict}")
    print("  Stage 3   : awaiting the operator's pilot")
    print("  Production: the persistent frontier is the sole scanner; there is "
          "no incremental-scan feature flag")
    print("=" * 78)

    if args.json:
        with open(args.json, "w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2)
        print(f"\n[REHEARSAL] Evidence written to {args.json}")
    return 0 if offline_ok else 1


if __name__ == "__main__":
    sys.exit(main())
