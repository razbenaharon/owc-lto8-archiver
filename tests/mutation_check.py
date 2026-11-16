"""Mutation check: prove the suite KILLS deliberate safety faults (audit M11).

The audit injected 55 faults into a copy of the code and 16 survived with no
test failing. This script re-runs that experiment for the faults that matter
most, so "the tests prove the gates are obeyed" is a measured claim rather
than a hope.

What it does
------------
1. Copies ``src/``, ``scripts/`` and ``tests/`` into a fresh temp directory.
   **The real tree is never modified.**
2. Runs the relevant test files against the unmutated copy first. They must
   pass there, otherwise a later failure proves nothing.
3. For each mutation: applies it to the copy by EXACT text replacement,
   runs the relevant test files in a new interpreter with the copy as its
   working directory, records the outcome, and restores the file.
4. Prints a table and exits non-zero unless every mutation was killed.

A mutation whose anchor text is not found exactly once is an ERROR, never a
skip: the product moved and this list must be updated with it.

Outcomes
--------
``KILLED``    a test ran and failed - the suite caught the fault.
``SURVIVED``  every test passed with the fault present.
``ERROR``     the anchor was missing/ambiguous, or pytest did not run tests to
              a verdict (collection error, timeout, no tests). Not a kill.

Usage (from the repository root)::

    python tests/mutation_check.py            # all mutations
    python tests/mutation_check.py -k gate    # only names containing "gate"
    python tests/mutation_check.py --list

It needs no tape, no PostgreSQL and no network. It is NOT collected by pytest
(its name does not match ``test_*.py``); run it on demand - it starts one
pytest process per mutation and takes a few minutes.
"""
import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
COPIED = ("src", "scripts", "tests", "config.example.ini")

REMOTE = "tests/test_write_gate_obeyed_remote.py"
LOCAL = "tests/test_write_gate_obeyed_local.py"
RESTORE = "tests/test_restore_hardening.py"
WATCHDOG = "tests/test_watchdog_hard_stop_sync.py"
FRONTIER = ("tests/test_frontier_is_the_production_scanner.py",
            "tests/test_segment_chunk_publication.py")
OBSERVABILITY = "tests/test_tape_write_observability.py"


def M(name, what, edits, tests):
    """One mutation: ``edits`` is a list of (relative file, old, new)."""
    if isinstance(tests, str):
        tests = (tests,)
    return {"name": name, "what": what, "edits": edits, "tests": tuple(tests)}


MUTATIONS = [
    # ------------------------------------------------------------------
    # The three survivors the audit named
    # ------------------------------------------------------------------
    M("writer-ignores-gate-block",
      "the safety gate computes 'block' and the remote writer carries on",
      [("src/remote_writer.py",
        "            if gate_block is not None:\n"
        "                return gate_block\n",
        "            if gate_block is not None:\n"
        "                pass\n")],
      REMOTE),
    M("media-health-gate-always-healthy",
      "_verify_ltfs_media_health returns 'healthy' without looking",
      [("src/remote_orchestrator.py",
        "        if mount_started_at is None:\n"
        "            mount_started_at = (ltfs_current_mount_status()\n",
        "        return None\n"
        "        if mount_started_at is None:\n"
        "            mount_started_at = (ltfs_current_mount_status()\n")],
      (REMOTE, LOCAL)),
    M("media-health-reader-always-ok",
      "ltfs_media_health reports ok whatever the IBM log says",
      [("src/windows_update_guard.py",
        '    info["ok"] = not info["fatal"] and not info["degraded"]\n',
        '    info["ok"] = True\n')],
      (REMOTE, LOCAL)),
    M("chunk-not-marked-backing",
      "the chunk is never marked 'backing' before the copy",
      [("src/remote_writer.py",
        "                write_started['v'] = True\n"
        "                self.host.db.update_chunk_status(session_id, chunk_index,\n"
        "                ChunkStatus.BACKING.value)\n",
        "                write_started['v'] = True\n")],
      REMOTE),
    M("backing-marked-after-copy",
      "the 'write started' marker moves to AFTER the copy",
      [("src/backup.py",
        "                            _mark_writer_started_durably()\n",
        ""),
       ("src/backup.py",
        "                                raw_sink=raw_log)\n",
        "                                raw_sink=raw_log)\n"
        "                            _mark_writer_started_durably()\n")],
      REMOTE),

    # ------------------------------------------------------------------
    # Remote write group
    # ------------------------------------------------------------------
    M("gate-result-inverted",
      "the remote writer stops on 'permit' and writes on 'block'",
      [("src/remote_writer.py",
        "            if gate_block is not None:\n",
        "            if gate_block is None:\n")],
      REMOTE),
    M("remote-skips-archiver-lock",
      "a lost single-writer lock no longer stops the remote group",
      [("src/remote_writer.py",
        "        if lock_stop is not None:\n",
        "        if False:\n")],
      REMOTE),
    M("lock-guard-accepts-unproven",
      "assert_archiver_lock_held never asks the database",
      [("src/archiver_lock_guard.py",
        "    verify()\n",
        "    pass\n")],
      (REMOTE, LOCAL)),
    M("fit-check-result-ignored",
      "tape full / unregistered / over-budget no longer stops the group",
      [("src/remote_writer.py",
        "        if not self.host._ensure_remote_chunk_fits_tape(\n",
        "        if False and not self.host._ensure_remote_chunk_fits_tape(\n")],
      REMOTE),
    M("tape-full-status-ignored",
      "tapes.status='full' is read as active everywhere",
      [("src/constants.py",
        "    return str(status or TAPE_STATUS_ACTIVE).strip().lower() "
        "== TAPE_STATUS_FULL\n",
        "    return False\n")],
      (REMOTE, LOCAL)),
    M("mount-gate-fails-open",
      "an unverifiable LTFS mount is treated as time@5",
      [("src/remote_orchestrator.py",
        "        status = ltfs_current_mount_status(expect_seconds=300)\n"
        '        if status.get("ok"):\n',
        "        status = ltfs_current_mount_status(expect_seconds=300)\n"
        '        if status.get("ok") or not status.get("determinate"):\n')],
      (REMOTE, LOCAL)),
    M("stale-mount-declaration-accepted",
      "a time@5 line from a PREVIOUS mount approves the write",
      [("src/windows_update_guard.py",
        "    if not bound:\n",
        "    if False:\n")],
      (REMOTE, LOCAL)),
    M("sync-interval-not-checked",
      "any 'time' sync mode passes, whatever its interval",
      [("src/windows_update_guard.py",
        '    info["ok"] = (info["sync_type"] == "time"\n'
        '                  and info["sync_seconds"] == expect_seconds)\n'
        '    info["reason"] = None if info["ok"] else "not_time5"\n',
        '    info["ok"] = (info["sync_type"] == "time")\n'
        '    info["reason"] = None if info["ok"] else "not_time5"\n')],
      REMOTE),
    M("cartridge-mismatch-ignored",
      "the wrong cartridge in the drive no longer blocks",
      [("src/remote_orchestrator.py",
        "        if not _tape_labels_match(mounted, tape_label):\n",
        "        if False:\n")],
      REMOTE),
    M("reboot-gate-result-ignored",
      "a staged restart is detected and the write starts anyway",
      [("src/remote_orchestrator.py",
        "        if reasons:\n"
        "            return self._record_stop(StopResult(\n"
        "                exit_code=ExitCode.TRANSIENT_RESUMABLE,\n"
        "                reason=self._reboot_reason_slug(reasons, sccm), "
        "resumable=True,\n",
        "        if False:\n"
        "            return self._record_stop(StopResult(\n"
        "                exit_code=ExitCode.TRANSIENT_RESUMABLE,\n"
        "                reason=self._reboot_reason_slug(reasons, sccm), "
        "resumable=True,\n")],
      REMOTE),
    M("sccm-unknown-not-blocking",
      "an SCCM client that cannot be asked counts as 'no restart'",
      [("src/remote_orchestrator.py",
        "            assessment = assess_reboot_state(block_on_unknown=True)\n",
        "            assessment = assess_reboot_state(block_on_unknown=False)\n")],
      (REMOTE, LOCAL)),
    M("reboot-check-crash-fails-open",
      "a restart check that raises no longer blocks",
      [("src/remote_orchestrator.py",
        "            reasons.append(\n"
        '                f"The restart state could not be evaluated ({exc}) — "\n'
        '                "refusing to start a tape write blind")\n',
        "")],
      (REMOTE, LOCAL)),
    M("prior-backing-chunk-rewritten",
      "a chunk a prior run left 'backing' is written again",
      [("src/remote_orchestrator.py",
        "        if self._chunk_backing_from_prior_run(session_id, ci):\n",
        "        if False:\n")],
      REMOTE),
    M("backing-check-fails-open",
      "an unreadable chunk status is treated as 'clear'",
      [("src/remote_orchestrator.py",
        '                "treating the chunk as AMBIGUOUS rather than clear",\n'
        "                exc_info=True)\n"
        "            return True\n",
        '                "treating the chunk as AMBIGUOUS rather than clear",\n'
        "                exc_info=True)\n"
        "            return False\n")],
      REMOTE),
    M("failed-write-made-retryable",
      "a copy that failed after starting is filed as 'never started'",
      [("src/remote_writer.py",
        "                if write_started['v']:\n",
        "                if False:\n")],
      REMOTE),
    M("remote-writer-ejects",
      "the remote writer uses the ejecting backup class",
      [("src/remote_writer.py",
        "            backup_cls = LTOBackup if eject_after else _NoEjectBackup\n",
        "            backup_cls = LTOBackup\n")],
      REMOTE),
    M("between-chunk-health-check-removed",
      "a drive fault logged during chunk N no longer stops chunk N+1",
      [("src/remote_writer.py",
        "                if position > 0 and mount_started_at:\n",
        "                if False:\n")],
      REMOTE),
    M("between-chunk-stop-ignored",
      "a stop requested mid-group no longer ends it at the boundary",
      [("src/remote_writer.py",
        "                if CANCEL.is_set() or stop_pipeline.is_set():\n",
        "                if False:\n")],
      REMOTE),

    # ------------------------------------------------------------------
    # The copy verdict
    # ------------------------------------------------------------------
    M("retry-limit-exceeded-is-success",
      "robocopy rc 0 with RETRY LIMIT EXCEEDED is trusted (incident 009)",
      [("src/robocopy.py",
        "    if 'RETRY LIMIT EXCEEDED' in output:\n",
        "    if False:\n")],
      (REMOTE, LOCAL, OBSERVABILITY)),
    M("copy-verdict-ignored",
      "a failed copy verdict is cataloged and marked done",
      [("src/backup.py",
        "            if not verdict.is_success:\n",
        "            if False:\n")],
      (REMOTE, LOCAL)),
    M("tape-copy-retries",
      "the tape copy is launched with retries enabled",
      [("src/backup.py",
        "             '/R:0', '/W:0',\n",
        "             '/R:3', '/W:10',\n")],
      REMOTE),

    # ------------------------------------------------------------------
    # Local (attended) writer
    # ------------------------------------------------------------------
    M("local-skips-archiver-lock",
      "the local writer never proves the single-writer lock",
      [("src/local_orchestrator.py",
        "        assert_archiver_lock_held(self.db)\n",
        "        pass\n")],
      LOCAL),
    M("local-gate-result-ignored",
      "the local gate computes a refusal and returns 'ok'",
      [("src/local_orchestrator.py",
        "        if detail is None:\n"
        "            return True\n",
        "        if True:\n"
        "            return True\n")],
      LOCAL),
    M("local-gate-crash-fails-open",
      "a local gate that raises permits the write",
      [("src/local_orchestrator.py",
        '            detail = (f"the pre-write safety gate could not be '
        'evaluated ({e}); "\n'
        '                      "refusing to start a tape write blind")\n',
        "            detail = None\n")],
      LOCAL),
    M("local-second-gate-removed",
      "the local re-check immediately before each copy is gone",
      [("src/local_orchestrator.py",
        "                if not self._pre_write_gate_ok(\n"
        "                        session['session_id'], chunk_index, tape_label,\n"
        "                        source):\n"
        "                    return False\n",
        "                if False:\n"
        "                    return False\n")],
      LOCAL),
    M("local-wrong-cartridge-ignored",
      "an assigned chunk is written to whatever cartridge is mounted",
      [("src/local_orchestrator.py",
        "        if assigned and tape_label.upper() != assigned.upper():\n",
        "        if False:\n")],
      LOCAL),

    # ------------------------------------------------------------------
    # Outside the write path
    # ------------------------------------------------------------------
    M("reconcile-before-queueing-dropped",
      "a migrated session's rediscovered files go straight to the planner",
      [("src/scan_frontier.py",
        "        if not self.legacy_session:\n"
        "            return pairs\n",
        "        if True:\n"
        "            return pairs\n")],
      FRONTIER),
    M("restore-reports-success-on-failure",
      "an incomplete restore exits 0",
      [("src/retriever.py",
        "        return 0 if self.ok else RESTORE_INCOMPLETE_EXIT_CODE\n",
        "        return 0\n")],
      RESTORE),
    M("restore-ok-ignores-failures",
      "RestoreResult.ok is true although files failed or were skipped",
      [("src/retriever.py",
        "        return (not self.cancelled and not self.failures\n"
        "                and self.skipped == 0)\n",
        "        return not self.cancelled\n")],
      RESTORE),
    M("restore-path-containment-removed",
      "a restore name that escapes the restore directory is accepted",
      [("src/retriever.py",
        "        if not inside:\n"
        "            raise RestoreContainmentError(\n",
        "        if False:\n"
        "            raise RestoreContainmentError(\n")],
      RESTORE),
    M("watchdog-hard-stop-list-emptied",
      "the watchdog's never-auto-restart reason list is empty",
      [("scripts/archive_watchdog.ps1",
        "$HardStopReasons = @(\n",
        "$HardStopReasons = @()\n$Discarded = @(\n")],
      WATCHDOG),
    M("watchdog-ignores-hard-stop-reason",
      "the watchdog relaunches after a recorded hard-stop reason",
      [("scripts/archive_watchdog.ps1",
        "    if ($reason -and ($HardStopReasons -contains $reason)) {\n",
        "    if ($false) {\n")],
      WATCHDOG),
]


# ---------------------------------------------------------------------------
def _read(path):
    with open(path, encoding="utf-8-sig", newline="") as fh:
        raw = fh.read()
    return raw, raw.replace("\r\n", "\n")


def _write(path, text, crlf):
    if crlf:
        text = text.replace("\n", "\r\n")
    with open(path, "w", encoding="utf-8", newline="") as fh:
        fh.write(text)


def _apply(copy_root, mutation):
    """Apply every edit; return {path: original bytes} or raise ValueError."""
    originals = {}
    staged = {}
    for rel, old, new in mutation["edits"]:
        path = os.path.join(copy_root, rel.replace("/", os.sep))
        if path not in staged:
            with open(path, "rb") as fh:
                originals[path] = fh.read()
            raw, text = _read(path)
            staged[path] = (text, "\r\n" in raw)
        text, crlf = staged[path]
        found = text.count(old)
        if found != 1:
            raise ValueError(
                f"anchor found {found} time(s) in {rel} (need exactly 1): "
                f"{old.strip().splitlines()[0]!r}")
        staged[path] = (text.replace(old, new), crlf)
    try:
        for path, (text, crlf) in staged.items():
            _write(path, text, crlf)
    except BaseException:
        _restore(originals)             # never leave a half-applied mutation
        raise
    return originals


def _restore(originals):
    for path, data in originals.items():
        with open(path, "wb") as fh:
            fh.write(data)


def _run_pytest(copy_root, tests, timeout):
    env = dict(os.environ)
    # Never a PostgreSQL run, and never a stale .pyc of the unmutated source.
    env.pop("LTO_TEST_PG_DSN", None)
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    cmd = [sys.executable, "-B", "-m", "pytest", *tests, "-q",
           "-p", "no:cacheprovider", "--no-header", "-rfE", "--tb=no"]
    try:
        proc = subprocess.run(cmd, cwd=copy_root, env=env, text=True,
                              capture_output=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return None, "timed out", []
    out = (proc.stdout or "") + (proc.stderr or "")
    # Only a test that RAN and failed counts. A setup/collection ERROR is
    # infrastructure breaking, not the suite catching the fault.
    failed = re.findall(r"^(?:FAILED|SUBFAIL|SUBFAILED)\S* (\S+)", out, re.M)
    summary = (out.strip().splitlines() or [""])[-1]
    return proc.returncode, summary, failed


def _short(test_id):
    parts = test_id.split("::")
    return f"{os.path.basename(parts[0])}::{parts[-1]}"


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("-k", default="", help="only mutations whose name "
                        "contains this text")
    parser.add_argument("--list", action="store_true")
    parser.add_argument("--timeout", type=int, default=600)
    parser.add_argument("--keep", action="store_true",
                        help="keep the temp copy for inspection")
    args = parser.parse_args(argv)

    selected = [m for m in MUTATIONS if args.k in m["name"]]
    if args.list:
        for m in selected:
            print(f"{m['name']:40} {m['what']}")
        return 0
    if not selected:
        print("no mutation matches", file=sys.stderr)
        return 2

    copy_root = tempfile.mkdtemp(prefix="lto_mutation_")
    print(f"[mutation] working copy: {copy_root}")
    try:
        for item in COPIED:
            src = os.path.join(REPO_ROOT, item)
            dst = os.path.join(copy_root, item)
            if os.path.isdir(src):
                shutil.copytree(src, dst, ignore=shutil.ignore_patterns(
                    "__pycache__", "*.pyc", ".pytest_cache"))
            else:
                shutil.copy2(src, dst)

        all_tests = sorted({t for m in selected for t in m["tests"]})
        print(f"[mutation] baseline on the unmutated copy: {len(all_tests)} "
              "test file(s)")
        code, summary, _ = _run_pytest(copy_root, all_tests, args.timeout)
        if code != 0:
            print(f"[mutation] BASELINE FAILED (exit {code}): {summary}\n"
                  "           nothing below would mean anything; aborting.",
                  file=sys.stderr)
            return 2
        print(f"[mutation] baseline ok: {summary}")

        rows = []
        for m in selected:
            started = time.time()
            try:
                originals = _apply(copy_root, m)
            except (OSError, ValueError) as exc:
                rows.append((m, "ERROR", f"anchor: {exc}", []))
                print(f"  ERROR    {m['name']}: {exc}")
                continue
            try:
                code, summary, failed = _run_pytest(
                    copy_root, m["tests"], args.timeout)
            finally:
                _restore(originals)
            if code == 1 and failed:
                outcome = "KILLED"
            elif code == 0:
                outcome = "SURVIVED"
            else:
                outcome = "ERROR"
            rows.append((m, outcome, summary, failed))
            print(f"  {outcome:8} {m['name']} "
                  f"({len(failed)} failing, {time.time() - started:.0f}s)")

        print("\n| mutation | fault injected | outcome | killed by (first of N) |")
        print("| --- | --- | --- | --- |")
        for m, outcome, summary, failed in rows:
            if outcome == "KILLED":
                detail = f"{_short(failed[0])} (of {len(failed)})"
            else:
                detail = summary
            print(f"| {m['name']} | {m['what']} | {outcome} | {detail} |")

        killed = sum(1 for r in rows if r[1] == "KILLED")
        print(f"\n[mutation] {killed}/{len(rows)} killed, "
              f"{sum(1 for r in rows if r[1] == 'SURVIVED')} survived, "
              f"{sum(1 for r in rows if r[1] == 'ERROR')} error(s)")
        return 0 if killed == len(rows) else 1
    finally:
        if args.keep:
            print(f"[mutation] kept: {copy_root}")
        else:
            shutil.rmtree(copy_root, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
