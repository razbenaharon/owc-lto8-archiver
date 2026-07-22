"""Hardening the long remote-archive run against network/DNS/SSH/EOF failures.

Covers the 2026-07-17 incident (a momentary DNS blip crashed the run to an
``input()`` EOF and it sat idle ~3 days) and the safety gaps found alongside it:
the resume path never re-validated the LTFS sync mode, a stale time@5 line from a
previous mount could approve a write, and a prior-run 'backing' chunk could be
double-written. All tests are mock-only — no real tape, no real SSH, no
production run.
"""
import io
import os
import json
import tempfile
import threading
import unittest
from contextlib import ExitStack, redirect_stdout
from types import SimpleNamespace
from unittest import mock

from src import cli
from src import remote_orchestrator as ro
from src import runtime as rt
from src import status_file
from src import windows_update_guard as wug
from src.exit_codes import (ExitCode, StopResult, REASON_NETWORK_RETRY_EXHAUSTED,
                            REASON_SCCM_REBOOT_PENDING,
                            REASON_WINDOWS_REBOOT_PENDING,
                            REASON_STOPPED_AT_CHUNK_BOUNDARY,
                            REASON_AMBIGUOUS_BACKING_CHUNK,
                            REASON_LTFS_SYNC_MODE_NOT_TIME5,
                            REASON_LTFS_MOUNT_UNVERIFIABLE,
                            REASON_AMBIGUOUS_ACTIVE_SESSIONS,
                            REASON_SSH_PERMISSION_DENIED,
                            REASON_SSH_HOST_KEY_MISMATCH,
                            REASON_MISSING_NONINTERACTIVE_CREDENTIAL,
                            REASON_NO_ACTIVE_SESSION,
                            REASON_NONINTERACTIVE_REQUIRES_RESUME,
                            REASON_USER_REQUESTED_STOP,
                            CLASS_DNS_RESOLUTION_FAILURE,
                            CLASS_CONNECTION_TIMEOUT)
from src.pipeline_types import StagedChunk


def _orch(**attrs):
    """A RemoteOrchestrator with just the stop-result plumbing wired up."""
    orch = object.__new__(ro.RemoteOrchestrator)
    orch.notifier = None
    orch._staged_lock = threading.Lock()
    orch._staged_bytes = 0
    orch._stop_lock = threading.Lock()
    orch._stop_result = None
    orch._last_fetch_failure = None
    orch.cfg = SimpleNamespace(backup_log_dir=None)
    for key, value in attrs.items():
        setattr(orch, key, value)
    return orch


def _desc(chunk_index=3):
    return StagedChunk(chunk_index=chunk_index, fetch_dir="f", pack_dir="p",
                       metadata=[], staged_bytes=10)


# ---------------------------------------------------------------------------
# Item 2 + 28: fetch-error classification (permanent FIRST, then transient)
# ---------------------------------------------------------------------------
class FetchErrorClassificationTests(unittest.TestCase):
    def test_dns_is_transient_with_precise_class(self):
        kind, cls, reason = ro._classify_fetch_error(
            "remote tar/ssh exit 255: ssh: Could not resolve hostname so01: "
            "Name or service not known")
        self.assertEqual(kind, "transient")
        self.assertEqual(cls, CLASS_DNS_RESOLUTION_FAILURE)
        self.assertIsNone(reason)

    def test_timeout_is_transient_connection_timeout(self):
        kind, cls, _ = ro._classify_fetch_error(
            "remote tar/ssh exit 255: ssh: connect ... Connection timed out")
        self.assertEqual(kind, "transient")
        self.assertEqual(cls, CLASS_CONNECTION_TIMEOUT)

    def test_permission_denied_is_permanent(self):
        kind, _, reason = ro._classify_fetch_error(
            "remote tar/ssh exit 255: Permission denied (publickey,password).")
        self.assertEqual(kind, "permanent")
        self.assertEqual(reason, REASON_SSH_PERMISSION_DENIED)

    def test_host_key_mismatch_is_permanent(self):
        kind, _, reason = ro._classify_fetch_error(
            "Host key verification failed.")
        self.assertEqual(kind, "permanent")
        self.assertEqual(reason, REASON_SSH_HOST_KEY_MISMATCH)

    def test_missing_credential_is_permanent(self):
        kind, _, reason = ro._classify_fetch_error(
            "remote_password is set, but no password-capable SSH helper was found.")
        self.assertEqual(kind, "permanent")
        self.assertEqual(reason, REASON_MISSING_NONINTERACTIVE_CREDENTIAL)

    def test_bare_exit_255_alone_is_not_transient(self):
        # A bare 255 with no network signature must NOT be treated as transient:
        # on its own it could mask an auth/config failure.
        self.assertFalse(ro._is_transient_fetch_error("ssh exit 255"))
        self.assertEqual(ro._classify_fetch_error("ssh exit 255")[0], "unknown")

    def test_both_network_and_auth_is_permanent(self):
        # Scenario 28: mixed signals classify FATAL — a network hiccup does not
        # excuse an auth failure.
        kind, _, reason = ro._classify_fetch_error(
            "ssh: connection reset by peer; then Permission denied (publickey).")
        self.assertEqual(kind, "permanent")
        self.assertEqual(reason, REASON_SSH_PERMISSION_DENIED)

    def test_missing_file_stays_unknown(self):
        self.assertEqual(
            ro._classify_fetch_error("tar: x: No such file or directory")[0],
            "unknown")


# ---------------------------------------------------------------------------
# Item 3 / scenario 7: jittered, bounded, non-negative backoff
# ---------------------------------------------------------------------------
class BackoffJitterTests(unittest.TestCase):
    def test_delay_is_bounded_and_jittered(self):
        for attempt in range(0, 8):
            for _ in range(50):
                d = ro.RemoteOrchestrator._fetch_backoff_delay(attempt, 10.0)
                self.assertGreaterEqual(d, 0.0)
                self.assertLessEqual(d, 60.0)

    def test_jitter_actually_varies(self):
        seen = {ro.RemoteOrchestrator._fetch_backoff_delay(1, 10.0)
                for _ in range(50)}
        self.assertGreater(len(seen), 1, "backoff should be jittered")

    def test_zero_base_is_zero(self):
        self.assertEqual(ro.RemoteOrchestrator._fetch_backoff_delay(3, 0), 0.0)


# ---------------------------------------------------------------------------
# Items 4/36/37: the fetch failure classification reaches a StopResult
# ---------------------------------------------------------------------------
class FetchFailureStopResultTests(unittest.TestCase):
    def _o(self):
        return _orch()

    def test_transient_exhaustion_maps_to_network_retry_exhausted(self):
        orch = self._o()
        orch._last_fetch_failure = {
            "kind": "transient", "classification": CLASS_DNS_RESOLUTION_FAILURE,
            "permanent_reason": None, "detail": "Could not resolve hostname"}
        result = orch._record_fetch_failure_stop(7, 4)
        self.assertEqual(result.exit_code, ExitCode.TRANSIENT_RESUMABLE)
        self.assertEqual(result.reason, REASON_NETWORK_RETRY_EXHAUSTED)
        self.assertEqual(result.error_classification, CLASS_DNS_RESOLUTION_FAILURE)

    def test_timeout_exhaustion_keeps_generic_reason_precise_class(self):
        orch = self._o()
        orch._last_fetch_failure = {
            "kind": "transient", "classification": CLASS_CONNECTION_TIMEOUT,
            "permanent_reason": None, "detail": "Connection timed out"}
        result = orch._record_fetch_failure_stop(7, 4)
        self.assertEqual(result.reason, REASON_NETWORK_RETRY_EXHAUSTED)
        self.assertEqual(result.error_classification, CLASS_CONNECTION_TIMEOUT)

    def test_permanent_maps_to_fatal_config(self):
        orch = self._o()
        orch._last_fetch_failure = {
            "kind": "permanent", "classification": None,
            "permanent_reason": REASON_SSH_PERMISSION_DENIED,
            "detail": "Permission denied"}
        result = orch._record_fetch_failure_stop(7, 4)
        self.assertEqual(result.exit_code, ExitCode.FATAL_CONFIG)
        self.assertEqual(result.reason, REASON_SSH_PERMISSION_DENIED)
        self.assertFalse(result.resumable)

    def test_fetch_one_batch_records_classification_on_exhaustion(self):
        orch = _orch(fetch_transient_retries=2, fetch_transient_retry_base=0,
                     remote_user="u", remote_host="h", remote_password=None,
                     ssh_cipher="c", use_mbuffer=False, mbuffer_size="512M",
                     fetch_cores=None)
        dns = ("remote tar/ssh exit 255: ssh: Could not resolve hostname so01: "
               "Name or service not known")
        with mock.patch.object(ro, "_remote_tar_fetch",
                               return_value=(False, dns)):
            with redirect_stdout(io.StringIO()):
                ok, err = orch._fetch_one_batch("b", [(0, "rel", 0)], "d",
                                                threading.Event())
        self.assertFalse(ok)
        self.assertIsNotNone(orch._last_fetch_failure)
        self.assertEqual(orch._last_fetch_failure["kind"], "transient")
        self.assertEqual(orch._last_fetch_failure["classification"],
                         CLASS_DNS_RESOLUTION_FAILURE)


# ---------------------------------------------------------------------------
# Items 5,6,9 + refinements 1/6: the single pre-write safety gate
# ---------------------------------------------------------------------------
class PreWriteSafetyGateTests(unittest.TestCase):
    def setUp(self):
        # CANCEL is a process-global Event; make sure every test starts clear.
        ro.CANCEL.clear()
        self.addCleanup(ro.CANCEL.clear)

    def _gate_orch(self, backing=(), mount_block=None,
                   reboot=([], {"determinate": True})):
        orch = _orch()
        orch.db = SimpleNamespace(
            get_chunks_with_status=lambda sid, st: (
                list(backing) if st == "backing" else []))
        orch._verify_current_mount_time5 = lambda: mount_block
        orch._pre_tape_write_reboot_check = lambda sid, desc, tape: reboot
        return orch

    def test_clear_state_permits_the_write(self):
        orch = self._gate_orch()
        self.assertIsNone(orch._pre_write_safety_gate(
            7, _desc(), "T", threading.Event()))

    def test_cancel_blocks_with_user_stop(self):
        orch = self._gate_orch()
        ro.CANCEL.set()
        result = orch._pre_write_safety_gate(7, _desc(), "T", threading.Event())
        self.assertEqual(result.exit_code, ExitCode.USER_STOP)
        self.assertEqual(result.reason, REASON_USER_REQUESTED_STOP)

    def test_stop_flag_returns_recorded_specific_reason(self):
        # Scenarios 40/41: a set stop_pipeline is never mapped to a generic 10;
        # it returns the specific reason the setter recorded (e.g. SCCM).
        orch = self._gate_orch()
        orch._record_stop(StopResult(
            exit_code=ExitCode.TRANSIENT_RESUMABLE,
            reason=REASON_SCCM_REBOOT_PENDING, source="reboot-sentinel"))
        ev = threading.Event(); ev.set()
        result = orch._pre_write_safety_gate(7, _desc(), "T", ev)
        self.assertEqual(result.reason, REASON_SCCM_REBOOT_PENDING)

    def test_stop_flag_without_record_is_generic_boundary(self):
        orch = self._gate_orch()
        ev = threading.Event(); ev.set()
        result = orch._pre_write_safety_gate(7, _desc(), "T", ev)
        self.assertEqual(result.exit_code, ExitCode.TRANSIENT_RESUMABLE)
        self.assertEqual(result.reason, REASON_STOPPED_AT_CHUNK_BOUNDARY)

    def test_mount_not_time5_blocks_safety(self):
        block = StopResult(exit_code=ExitCode.SAFETY_BLOCK,
                           reason=REASON_LTFS_SYNC_MODE_NOT_TIME5, source="gate")
        orch = self._gate_orch(mount_block=block)
        result = orch._pre_write_safety_gate(7, _desc(), "T", threading.Event())
        self.assertEqual(result.exit_code, ExitCode.SAFETY_BLOCK)
        self.assertEqual(result.reason, REASON_LTFS_SYNC_MODE_NOT_TIME5)

    def test_sccm_reboot_maps_to_sccm_reason(self):
        orch = self._gate_orch(reboot=(
            ["Configuration Manager (SCCM) has a restart pending"],
            {"reboot_pending": True, "determinate": True}))
        with redirect_stdout(io.StringIO()):
            result = orch._pre_write_safety_gate(7, _desc(), "T", threading.Event())
        self.assertEqual(result.exit_code, ExitCode.TRANSIENT_RESUMABLE)
        self.assertEqual(result.reason, REASON_SCCM_REBOOT_PENDING)

    def test_windows_reboot_maps_to_windows_reason(self):
        orch = self._gate_orch(reboot=(
            ["Component Based Servicing has a restart pending"],
            {"reboot_pending": False, "hard_reboot_pending": False,
             "determinate": True}))
        with redirect_stdout(io.StringIO()):
            result = orch._pre_write_safety_gate(7, _desc(), "T", threading.Event())
        self.assertEqual(result.reason, REASON_WINDOWS_REBOOT_PENDING)

    def test_prior_backing_chunk_blocks_safety(self):
        orch = self._gate_orch(backing=(3,))
        with redirect_stdout(io.StringIO()):
            result = orch._pre_write_safety_gate(7, _desc(3), "T", threading.Event())
        self.assertEqual(result.exit_code, ExitCode.SAFETY_BLOCK)
        self.assertEqual(result.reason, REASON_AMBIGUOUS_BACKING_CHUNK)

    def test_current_run_own_backing_is_not_blocked(self):
        # Scenario 24: the gate reads status BEFORE this run writes, so the
        # chunk it is about to move to 'backing' is not flagged.
        orch = self._gate_orch(backing=())  # nothing 'backing' at gate entry
        self.assertIsNone(orch._pre_write_safety_gate(
            7, _desc(3), "T", threading.Event()))


# ---------------------------------------------------------------------------
# Item 3 (correction 2) / scenarios 19-21: current-mount time@5 binding
# ---------------------------------------------------------------------------
class CurrentMountBindingTests(unittest.TestCase):
    def _run(self, proc_start, event_time, message):
        payload = json.dumps({"ProcStart": proc_start, "EventTime": event_time,
                              "EventMsg": message})
        completed = SimpleNamespace(returncode=0, stdout=payload, stderr="")
        with mock.patch.object(wug.os, "name", "nt"), \
             mock.patch.object(wug.subprocess, "run", return_value=completed):
            return wug.ltfs_current_mount_status(expect_seconds=300)

    def test_bound_time5_is_ok(self):
        info = self._run("2026-07-22T10:00:00+03:00", "2026-07-22T10:05:00+03:00",
                         'Sync type is "time", Sync time is 300 sec')
        self.assertTrue(info["ok"])
        self.assertTrue(info["bound_to_current"])
        self.assertIsNone(info["reason"])

    def test_bound_but_not_time5(self):
        info = self._run("2026-07-22T10:00:00+03:00", "2026-07-22T10:05:00+03:00",
                         'Sync type is "time", Sync time is 60 sec')
        self.assertFalse(info["ok"])
        self.assertTrue(info["determinate"])
        self.assertEqual(info["reason"], "not_time5")

    def test_only_old_mount_declaration_is_unverifiable(self):
        # Scenario 20: the newest 61259 predates the running LTFS process.
        info = self._run("2026-07-22T10:00:00+03:00", "2026-07-22T09:00:00+03:00",
                         'Sync type is "time", Sync time is 300 sec')
        self.assertFalse(info["ok"])
        self.assertFalse(info["bound_to_current"])
        self.assertEqual(info["reason"], "unverifiable")

    def test_no_running_mount_is_unverifiable(self):
        # Scenario 21: the current mount cannot be identified — fail closed.
        info = self._run("", "2026-07-22T10:05:00+03:00",
                         'Sync type is "time", Sync time is 300 sec')
        self.assertFalse(info["mount_identified"])
        self.assertFalse(info["ok"])
        self.assertEqual(info["reason"], "unverifiable")

    def test_verify_maps_ok_to_permit(self):
        orch = _orch()
        with mock.patch.object(ro, "ltfs_current_mount_status",
                               return_value={"ok": True, "determinate": True,
                                             "sync_type": "time",
                                             "sync_seconds": 300,
                                             "declared_at": "x",
                                             "mount_started_at": "y"}):
            self.assertIsNone(orch._verify_current_mount_time5())

    def test_verify_maps_not_time5_to_safety_block(self):
        orch = _orch()
        with mock.patch.object(ro, "ltfs_current_mount_status",
                               return_value={"ok": False, "determinate": True,
                                             "reason": "not_time5",
                                             "sync_type": "time",
                                             "sync_seconds": 60, "error": None}), \
             mock.patch.object(ro, "send_best_effort"), redirect_stdout(io.StringIO()):
            result = orch._verify_current_mount_time5()
        self.assertEqual(result.reason, REASON_LTFS_SYNC_MODE_NOT_TIME5)

    def test_verify_maps_unverifiable_to_safety_block(self):
        orch = _orch()
        with mock.patch.object(ro, "ltfs_current_mount_status",
                               return_value={"ok": False, "determinate": False,
                                             "reason": "unverifiable",
                                             "error": "no LTFS process"}), \
             mock.patch.object(ro, "send_best_effort"), redirect_stdout(io.StringIO()):
            result = orch._verify_current_mount_time5()
        self.assertEqual(result.reason, REASON_LTFS_MOUNT_UNVERIFIABLE)


# ---------------------------------------------------------------------------
# Item 9 (correction 4) / scenarios 23-24: prior-run 'backing' detection
# ---------------------------------------------------------------------------
class PriorBackingDetectionTests(unittest.TestCase):
    def test_prior_backing_stops_before_threads(self):
        calls = []
        orch = _orch()
        orch.db = SimpleNamespace(
            get_chunks_with_status=lambda sid, st: (
                calls.append((sid, st)) or ([5] if st == "backing" else [])))
        with redirect_stdout(io.StringIO()):
            result = orch._detect_prior_backing_chunks(42)
        self.assertIsNotNone(result)
        self.assertEqual(result.exit_code, ExitCode.SAFETY_BLOCK)
        self.assertEqual(result.reason, REASON_AMBIGUOUS_BACKING_CHUNK)
        self.assertEqual(result.chunk_index, 5)

    def test_no_backing_returns_none(self):
        orch = _orch()
        orch.db = SimpleNamespace(get_chunks_with_status=lambda sid, st: [])
        self.assertIsNone(orch._detect_prior_backing_chunks(42))

    def test_backing_block_does_not_mutate_db_status(self):
        # A safety block must NOT flip the chunk's status (no auto-reconcile).
        mutations = []
        orch = _orch()
        orch.db = SimpleNamespace(
            get_chunks_with_status=lambda sid, st: [5] if st == "backing" else [],
            update_chunk_status=lambda *a: mutations.append(a))
        with redirect_stdout(io.StringIO()):
            result = orch._detect_prior_backing_chunks(42)
        self.assertIsNotNone(result)
        self.assertEqual(mutations, [], "resume block must not mutate DB status")


# ---------------------------------------------------------------------------
# Items 15/16/17/47: best-effort atomic status + last-failure files
# ---------------------------------------------------------------------------
class StatusFileTests(unittest.TestCase):
    def test_status_written_atomically(self):
        with tempfile.TemporaryDirectory() as tmp:
            ok = status_file.write_status(
                tmp, session_id=3, chunk_id=4, phase="fetch-retry",
                retry_attempt=2, error_classification=CLASS_DNS_RESOLUTION_FAILURE,
                error_message="Could not resolve hostname", next_retry_delay=8,
                reason=REASON_NETWORK_RETRY_EXHAUSTED, detailed_reason="dns")
            self.assertTrue(ok)
            path = os.path.join(tmp, status_file.STATUS_FILENAME)
            self.assertTrue(os.path.isfile(path))
            self.assertFalse(os.path.exists(path + ".tmp"),
                             "temp file must not survive an atomic write")
            data = json.loads(open(path, encoding="utf-8").read())
            self.assertEqual(data["error_classification"],
                             CLASS_DNS_RESOLUTION_FAILURE)
            self.assertEqual(data["reason"], REASON_NETWORK_RETRY_EXHAUSTED)
            self.assertEqual(data["detailed_reason"], "dns")

    def test_status_write_failure_is_swallowed(self):
        # Pointing log_dir at a regular file makes makedirs fail; must not raise.
        with tempfile.NamedTemporaryFile() as f:
            log_dir = os.path.join(f.name, "subdir")  # f.name is a file
            self.assertFalse(status_file.write_status(log_dir, session_id=1))

    def test_last_failure_snapshot(self):
        with tempfile.TemporaryDirectory() as tmp:
            sr = StopResult(exit_code=ExitCode.TRANSIENT_RESUMABLE,
                            reason=REASON_NETWORK_RETRY_EXHAUSTED,
                            error_classification=CLASS_DNS_RESOLUTION_FAILURE,
                            detailed_reason="dns")
            self.assertTrue(status_file.write_last_failure(tmp, sr))
            data = json.loads(open(
                os.path.join(tmp, status_file.LAST_FAILURE_FILENAME),
                encoding="utf-8").read())
            self.assertEqual(data["exit_code"], int(ExitCode.TRANSIENT_RESUMABLE))
            self.assertEqual(data["reason"], REASON_NETWORK_RETRY_EXHAUSTED)

    def test_finalize_preserves_exit_code_when_status_write_fails(self):
        # Scenario 47: a status-write failure never changes the stop result.
        orch = _orch(cfg=SimpleNamespace(backup_log_dir="/whatever"))
        sr = StopResult(exit_code=ExitCode.TRANSIENT_RESUMABLE,
                        reason=REASON_NETWORK_RETRY_EXHAUSTED)
        with mock.patch.object(ro, "write_last_failure",
                               side_effect=OSError("disk full")), \
             mock.patch.object(ro, "write_status", side_effect=OSError("boom")):
            final = orch._finalize(sr)
        self.assertEqual(final.exit_code, ExitCode.TRANSIENT_RESUMABLE)
        self.assertEqual(final.reason, REASON_NETWORK_RETRY_EXHAUSTED)


# ---------------------------------------------------------------------------
# Refinement 6 / scenario 48: most-specific stop reason wins
# ---------------------------------------------------------------------------
class RecordStopTests(unittest.TestCase):
    def test_specific_reason_is_not_overwritten_by_generic(self):
        orch = _orch()
        orch._record_stop(StopResult(exit_code=ExitCode.TRANSIENT_RESUMABLE,
                                     reason=REASON_SCCM_REBOOT_PENDING))
        orch._record_stop(StopResult(exit_code=ExitCode.TRANSIENT_RESUMABLE,
                                     reason=REASON_STOPPED_AT_CHUNK_BOUNDARY))
        self.assertEqual(orch._stop_result.reason, REASON_SCCM_REBOOT_PENDING)

    def test_generic_is_replaced_by_specific(self):
        orch = _orch()
        orch._record_stop(StopResult(exit_code=ExitCode.TRANSIENT_RESUMABLE,
                                     reason=REASON_STOPPED_AT_CHUNK_BOUNDARY))
        orch._record_stop(StopResult(exit_code=ExitCode.TRANSIENT_RESUMABLE,
                                     reason=REASON_NETWORK_RETRY_EXHAUSTED))
        self.assertEqual(orch._stop_result.reason, REASON_NETWORK_RETRY_EXHAUSTED)


# ---------------------------------------------------------------------------
# Refinement 3 / scenarios 32-35: headless session-count dispatch
# ---------------------------------------------------------------------------
class HeadlessDispatchTests(unittest.TestCase):
    def _o(self, sessions):
        orch = _orch(remote_host="h", remote_session_path="/p")
        orch.db = SimpleNamespace(
            list_active_remote_sessions=lambda h, p: list(sessions))
        return orch

    def test_without_resume_requires_resume(self):
        orch = self._o([])
        with redirect_stdout(io.StringIO()):
            result = orch._run_non_interactive(resume=False)
        self.assertEqual(result.exit_code, ExitCode.FATAL_CONFIG)
        self.assertEqual(result.reason, REASON_NONINTERACTIVE_REQUIRES_RESUME)

    def test_resume_no_session_is_fatal(self):
        orch = self._o([])
        with redirect_stdout(io.StringIO()):
            result = orch._run_non_interactive(resume=True)
        self.assertEqual(result.exit_code, ExitCode.FATAL_CONFIG)
        self.assertEqual(result.reason, REASON_NO_ACTIVE_SESSION)

    def test_resume_many_sessions_is_ambiguous(self):
        orch = self._o([{"session_id": 1, "session_label": "A"},
                        {"session_id": 2, "session_label": "B"}])
        with redirect_stdout(io.StringIO()):
            result = orch._run_non_interactive(resume=True)
        self.assertEqual(result.exit_code, ExitCode.SAFETY_BLOCK)
        self.assertEqual(result.reason, REASON_AMBIGUOUS_ACTIVE_SESSIONS)

    def test_resume_single_session_runs_it(self):
        orch = self._o([{"session_id": 99, "session_label": "ONE"}])
        marker = StopResult(exit_code=ExitCode.COMPLETED, reason="completed")
        orch._run_session = mock.Mock(return_value=marker)
        with redirect_stdout(io.StringIO()):
            result = orch._run_non_interactive(resume=True)
        orch._run_session.assert_called_once_with(99)
        self.assertIs(result, marker)

    def test_headless_resume_after_completed_session_finds_nothing(self):
        # After the final chunk completed under Ctrl+C, the session is committed
        # 'completed', so it is no longer active. A later headless resume finds
        # NO active session and does NO cleanup/recovery/staging work.
        consulted = {"list": 0}

        def _list(host, path):
            consulted["list"] += 1
            return []   # the completed session is no longer 'active'

        orch = _orch(remote_host="h", remote_session_path="/p")
        orch.db = SimpleNamespace(list_active_remote_sessions=_list)
        orch._run_session = mock.Mock(
            side_effect=AssertionError("must not resume/recover"))
        orch._detect_prior_backing_chunks = mock.Mock(
            side_effect=AssertionError("must not run a resume precheck"))
        orch._stage_chunk = mock.Mock(
            side_effect=AssertionError("must not fetch/pack"))
        with redirect_stdout(io.StringIO()):
            result = orch._run_non_interactive(resume=True)
        self.assertEqual(result.exit_code, ExitCode.FATAL_CONFIG)
        self.assertEqual(result.reason, REASON_NO_ACTIVE_SESSION)
        orch._run_session.assert_not_called()
        orch._detect_prior_backing_chunks.assert_not_called()
        orch._stage_chunk.assert_not_called()
        self.assertEqual(consulted["list"], 1)


# ---------------------------------------------------------------------------
# Item 4 / scenario 14: a raising notifier never propagates
# ---------------------------------------------------------------------------
class TelegramResilienceTests(unittest.TestCase):
    def test_send_best_effort_swallows_a_raising_notifier(self):
        class Boom:
            def send(self, text):
                raise RuntimeError("network down / getaddrinfo failed")

        with redirect_stdout(io.StringIO()):
            self.assertFalse(ro.send_best_effort(Boom(), "hi"))


# ---------------------------------------------------------------------------
# Refinement 1 / scenarios 29-30/46: exactly one authoritative gate
# ---------------------------------------------------------------------------
class SingleGateStructureTests(unittest.TestCase):
    """Static assertions: authorization goes through exactly one boundary
    (_write_chunk → _pre_write_safety_gate), under the tape I/O lock, and the
    writer loops carry no duplicate reboot/gate decision."""

    def setUp(self):
        import inspect
        self.src = inspect.getsource(ro.RemoteOrchestrator._run_session)
        self.stream = inspect.getsource(
            ro.RemoteOrchestrator._run_streaming_session)
        self.write = inspect.getsource(ro.RemoteOrchestrator._write_chunk)

    def test_every_write_goes_through_write_chunk(self):
        self.assertIn("_write_chunk(", self.src)
        self.assertIn("_write_chunk(", self.stream)

    def test_write_chunk_is_the_only_gate_caller_on_the_write_path(self):
        # The loops must NOT call the gate directly — _write_chunk owns it.
        self.assertNotIn("_pre_write_safety_gate", self.src)
        self.assertNotIn("_pre_write_safety_gate", self.stream)
        self.assertIn("_pre_write_safety_gate", self.write)

    def test_reboot_check_is_not_called_directly_in_the_loops(self):
        self.assertNotIn("_pre_tape_write_reboot_check", self.src)
        self.assertNotIn("_pre_tape_write_reboot_check", self.stream)

    def test_authorize_and_write_are_under_the_tape_lock(self):
        # acquire -> gate -> writer launch -> release, all in _write_chunk.
        a = self.write.index("_acquire_tape_io_lock")
        g = self.write.index("_pre_write_safety_gate")
        w = self.write.index(".run(")
        r = self.write.index("_release_tape_io_lock")
        self.assertLess(a, g, "the tape lock must be held before authorizing")
        self.assertLess(g, w, "the gate must pass before the writer launches")
        self.assertLess(w, r, "the writer launches before the lock is released")

    def test_status_flush_happens_after_the_lock_is_released(self):
        # The 'done' commit + cleanup must be OUTSIDE the lock.
        r = self.write.rindex("_release_tape_io_lock")
        done = self.write.rindex("'done'")
        self.assertLess(r, done,
                        "the 'done' commit must happen after releasing the lock")


class GatePrecedenceTests(unittest.TestCase):
    """Required precedence: an already-recorded stop wins; a later CANCEL never
    replaces it; CANCEL as the first stop source becomes 40/user_requested_stop."""

    def setUp(self):
        ro.CANCEL.clear()
        self.addCleanup(ro.CANCEL.clear)

    def _gate_orch(self):
        orch = _orch()
        orch.db = SimpleNamespace(get_chunks_with_status=lambda sid, st: [])
        orch._verify_current_mount_time5 = lambda: None
        orch._pre_tape_write_reboot_check = lambda sid, desc, tape: ([], {})
        return orch

    def test_recorded_safety_block_survives_a_later_cancel(self):
        orch = self._gate_orch()
        orch._record_stop(StopResult(exit_code=ExitCode.SAFETY_BLOCK,
                                     reason=REASON_LTFS_MOUNT_UNVERIFIABLE))
        ro.CANCEL.set()
        result = orch._pre_write_safety_gate(7, _desc(), "T", threading.Event())
        self.assertEqual(result.exit_code, ExitCode.SAFETY_BLOCK)
        self.assertEqual(result.reason, REASON_LTFS_MOUNT_UNVERIFIABLE)

    def test_recorded_network_stop_survives_a_later_cancel(self):
        orch = self._gate_orch()
        orch._record_stop(StopResult(exit_code=ExitCode.TRANSIENT_RESUMABLE,
                                     reason=REASON_NETWORK_RETRY_EXHAUSTED,
                                     error_classification=CLASS_DNS_RESOLUTION_FAILURE))
        ro.CANCEL.set()
        result = orch._pre_write_safety_gate(7, _desc(), "T", threading.Event())
        self.assertEqual(result.reason, REASON_NETWORK_RETRY_EXHAUSTED)

    def test_cancel_as_first_stop_source_is_user_stop(self):
        orch = self._gate_orch()
        ro.CANCEL.set()
        result = orch._pre_write_safety_gate(7, _desc(), "T", threading.Event())
        self.assertEqual(result.exit_code, ExitCode.USER_STOP)
        self.assertEqual(result.reason, REASON_USER_REQUESTED_STOP)

    def test_first_ctrl_c_records_complete_stop_result(self):
        # The first cooperative Ctrl+C constructs and records a COMPLETE
        # StopResult — every field present — and it becomes the authoritative
        # recorded result.
        orch = self._gate_orch()
        ro.CANCEL.set()
        result = orch._pre_write_safety_gate(7, _desc(3), "T", threading.Event())
        self.assertEqual(result.exit_code, ExitCode.USER_STOP)          # exit_code
        self.assertEqual(result.reason, REASON_USER_REQUESTED_STOP)     # reason
        self.assertIsNone(result.error_classification)   # present; None for a stop
        self.assertTrue(result.detailed_reason)                        # detailed_reason
        self.assertTrue(result.resumable)                              # resumable
        self.assertTrue(result.timestamp)                              # timestamp
        self.assertEqual(result.source, "gate")                        # source
        self.assertEqual(result.session_id, 7)
        self.assertEqual(result.chunk_index, 3)
        self.assertIs(orch._get_recorded_stop(), result,
                      "the first Ctrl+C result is recorded authoritatively")


class WriteChunkRaceTests(unittest.TestCase):
    """The gate→write race is closed: authorization and the write-start happen
    under the tape I/O lock, with a final recorded-stop re-check right before the
    tape. A stop visible before the writer starts prevents the write; a stop that
    lands after it starts does not interrupt it but blocks the next write."""

    def setUp(self):
        ro.CANCEL.clear()
        self.addCleanup(ro.CANCEL.clear)

    class _FakeWriter:
        """Stands in for LTOBackup.run. ``start`` controls whether the tape write
        actually begins (fires on_write_start → the chunk moves to 'backing');
        ``raises`` simulates a writer failure; ``on_run`` runs mid-write."""
        def __init__(self, start=True, on_run=None, raises=None):
            self.calls = 0
            self._on_run = on_run
            self._raises = raises
            self._start = start
            self.terminated = False   # a real writer would flip this if killed
        def run(self, on_write_start=None, **kw):
            self.calls += 1
            if self._start and on_write_start is not None:
                on_write_start()      # the write physically starts -> 'backing'
            if self._on_run:
                self._on_run()
            if self._raises:
                raise self._raises

    def _o(self, writer):
        orch = _orch()
        orch._consumer_chunk = None
        orch.remote_host = "h.x"
        orch.remote_session_path = "/p"
        orch.skipped_tracker = None
        orch.governor = None
        orch.cfg = SimpleNamespace(lto_drive="Z:", backup_log_dir=None,
                                   ibm_eject_cmd="")
        statuses = []
        orch.db = SimpleNamespace(
            get_chunks_with_status=lambda sid, st: [],
            get_chunk_size_summary=lambda sid, ci=None: {ci: (10, 10, 1)},
            get_tape=lambda label: {"total_capacity": 10**9},
            recalculate_tape_used_space=lambda label: 0,
            update_chunk_status=lambda sid, ci, st: statuses.append(st))
        orch._verify_current_mount_time5 = lambda: None
        orch._pre_tape_write_reboot_check = lambda sid, desc, tape: ([], {})
        orch._backup_writer = lambda cls=None: writer
        orch._cleanup_dir = lambda p: None
        return orch, statuses

    def test_recorded_stop_before_write_prevents_the_writer(self):
        writer = self._FakeWriter()
        orch, statuses = self._o(writer)
        orch._record_stop(StopResult(exit_code=ExitCode.SAFETY_BLOCK,
                                     reason=REASON_LTFS_MOUNT_UNVERIFIABLE))
        result = orch._write_chunk(7, _desc(0), "T", False, threading.Event())
        self.assertIsNotNone(result)
        self.assertEqual(result.reason, REASON_LTFS_MOUNT_UNVERIFIABLE)
        self.assertEqual(writer.calls, 0, "no write may start after a stop")
        self.assertNotIn("backing", statuses, "must not transition to 'backing'")

    def test_stop_recorded_during_the_gate_is_caught_before_tape(self):
        # Simulate a stop landing while the gate ran its subprocess checks: the
        # gate returns permit, but the final recorded-stop re-check catches it.
        writer = self._FakeWriter()
        orch, statuses = self._o(writer)

        def gate_permits_but_stop_lands(*a, **k):
            orch._record_stop(StopResult(
                exit_code=ExitCode.TRANSIENT_RESUMABLE,
                reason=REASON_SCCM_REBOOT_PENDING))
            return None
        orch._pre_write_safety_gate = gate_permits_but_stop_lands
        result = orch._write_chunk(7, _desc(0), "T", False, threading.Event())
        self.assertEqual(result.reason, REASON_SCCM_REBOOT_PENDING)
        self.assertEqual(writer.calls, 0)
        self.assertNotIn("backing", statuses)

    def test_stop_after_write_started_does_not_interrupt_but_blocks_next(self):
        stop = threading.Event()
        # The writer sets the stop flag mid-write; the active write still finishes.
        writer = self._FakeWriter(on_run=stop.set)
        orch, statuses = self._o(writer)
        first = orch._write_chunk(7, _desc(0), "T", False, stop)
        self.assertIsNone(first, "the active write must complete, not be cut")
        self.assertEqual(writer.calls, 1)
        self.assertEqual(statuses, ["backing", "done"])
        # The next chunk hits the gate with stop set and is blocked (no 2nd write).
        second = orch._write_chunk(7, _desc(1), "T", False, stop)
        self.assertIsNotNone(second)
        self.assertEqual(writer.calls, 1, "the next write must not start")


class CooperativeCancelWriteTests(unittest.TestCase):
    """The cooperative-Ctrl+C vs forced-interruption distinction at _write_chunk.

    Cooperative cancel during a write lets the (protected) write finish and
    commit 'done'; a forced/crashed write is left 'backing' (ambiguous)."""

    def setUp(self):
        ro.CANCEL.clear()
        self.addCleanup(ro.CANCEL.clear)

    def _o(self, writer):
        orch = _orch()
        orch._consumer_chunk = None
        orch.remote_host = "h.x"
        orch.remote_session_path = "/p"
        orch.skipped_tracker = None
        orch.governor = None
        orch.cfg = SimpleNamespace(lto_drive="Z:", backup_log_dir=None,
                                   ibm_eject_cmd="")
        statuses = []
        orch.db = SimpleNamespace(
            get_chunks_with_status=lambda sid, st: [],
            get_chunk_size_summary=lambda sid, ci=None: {ci: (10, 10, 1)},
            get_tape=lambda label: {"total_capacity": 10**9},
            recalculate_tape_used_space=lambda label: 0,
            update_chunk_status=lambda sid, ci, st: statuses.append(st))
        orch._verify_current_mount_time5 = lambda: None
        orch._pre_tape_write_reboot_check = lambda sid, desc, tape: ([], {})
        orch._backup_writer = lambda cls=None: writer
        orch._cleanup_dir = lambda p: None
        return orch, statuses

    def test_cancel_before_writer_launch_starts_no_write(self):
        writer = WriteChunkRaceTests._FakeWriter()
        orch, statuses = self._o(writer)
        ro.CANCEL.set()
        result = orch._write_chunk(7, _desc(0), "T", False, threading.Event())
        self.assertEqual(result.exit_code, ExitCode.USER_STOP)
        self.assertEqual(result.reason, REASON_USER_REQUESTED_STOP)
        self.assertTrue(result.preserve_pack, "the unwritten pack is preserved")
        self.assertEqual(writer.calls, 0)
        self.assertNotIn("backing", statuses)

    def test_cancel_during_active_write_finishes_and_commits_done(self):
        # Ctrl+C arrives while the (protected) writer runs: it is NOT terminated,
        # the write finishes, and the chunk reaches 'done' — never left ambiguous.
        writer = WriteChunkRaceTests._FakeWriter(on_run=ro.CANCEL.set)
        orch, statuses = self._o(writer)
        with redirect_stdout(io.StringIO()):
            first = orch._write_chunk(7, _desc(0), "T", False, threading.Event())
        self.assertIsNone(first, "a completed write commits 'done', not a stop")
        self.assertEqual(statuses, ["backing", "done"])
        self.assertFalse(writer.terminated, "the active writer must not be killed")
        self.assertEqual(writer.calls, 1)
        # The NEXT chunk is blocked before any write starts.
        writer2 = WriteChunkRaceTests._FakeWriter()
        orch2, statuses2 = self._o(writer2)
        second = orch2._write_chunk(7, _desc(1), "T", False, threading.Event())
        self.assertEqual(second.exit_code, ExitCode.USER_STOP)
        self.assertEqual(writer2.calls, 0, "no next write starts after cancel")

    def test_forced_interruption_leaves_chunk_ambiguous_backing(self):
        # A forced/crashed write (robocopy cut) raises with CANCEL set: the chunk
        # is left 'backing' — NOT done, NOT backup_failed — and the result is
        # 20/ambiguous (physical ambiguity ESCALATES over the user stop).
        writer = WriteChunkRaceTests._FakeWriter(
            on_run=ro.CANCEL.set, raises=RuntimeError("robocopy killed"))
        orch, statuses = self._o(writer)
        with redirect_stdout(io.StringIO()):
            result = orch._write_chunk(7, _desc(0), "T", False, threading.Event())
        self.assertEqual(result.exit_code, ExitCode.SAFETY_BLOCK)
        self.assertEqual(result.reason, REASON_AMBIGUOUS_BACKING_CHUNK)
        self.assertEqual(statuses, ["backing"],
                         "an interrupted write stays 'backing' (ambiguous)")
        self.assertNotIn("done", statuses)
        self.assertNotIn("backup_failed", statuses)

    def test_successful_write_then_stop_is_not_ambiguous(self):
        # A clean write commits 'done'; a later stop cannot make it ambiguous.
        writer = WriteChunkRaceTests._FakeWriter()
        orch, statuses = self._o(writer)
        result = orch._write_chunk(7, _desc(0), "T", False, threading.Event())
        self.assertIsNone(result)
        self.assertEqual(statuses[-1], "done")
        # Simulate the resume precheck: this chunk is 'done', so not 'backing'.
        orch.db.get_chunks_with_status = lambda sid, st: []
        self.assertIsNone(orch._detect_prior_backing_chunks(7))


class PostBackingFailureAmbiguityTests(unittest.TestCase):
    """Any failure once the write has STARTED is physically ambiguous — the chunk
    stays 'backing', never done/backup_failed/pending. A failure BEFORE the write
    started is safely re-fetchable."""

    def setUp(self):
        ro.CANCEL.clear()
        self.addCleanup(ro.CANCEL.clear)

    def _o(self, writer):
        return CooperativeCancelWriteTests._o(self, writer)

    def test_writer_raises_after_backing_without_cancel_stays_backing(self):
        # robocopy critical failure / exception / unexpected exit / LTFS I/O
        # error after start all surface as a raise from backup.run.
        writer = WriteChunkRaceTests._FakeWriter(
            start=True, raises=RuntimeError("CRITICAL: robocopy failed exit 8"))
        orch, statuses = self._o(writer)
        with redirect_stdout(io.StringIO()):
            result = orch._write_chunk(7, _desc(0), "T", False, threading.Event())
        self.assertEqual(result.exit_code, ExitCode.SAFETY_BLOCK)
        self.assertEqual(result.reason, REASON_AMBIGUOUS_BACKING_CHUNK)
        self.assertEqual(statuses, ["backing"])
        self.assertNotIn("backup_failed", statuses)
        self.assertNotIn("pending", statuses)
        self.assertNotIn("done", statuses)

    def test_forced_kill_after_backing_returns_ambiguous_not_user_stop(self):
        # Requirement 1: a force-killed writer after write_started returns
        # 20/ambiguous_backing_chunk EVEN when CANCEL is set — never 40.
        writer = WriteChunkRaceTests._FakeWriter(
            start=True, on_run=ro.CANCEL.set, raises=RuntimeError("killed"))
        orch, statuses = self._o(writer)
        with redirect_stdout(io.StringIO()):
            result = orch._write_chunk(7, _desc(0), "T", False, threading.Event())
        self.assertEqual(result.exit_code, ExitCode.SAFETY_BLOCK)
        self.assertEqual(result.reason, REASON_AMBIGUOUS_BACKING_CHUNK)
        self.assertNotEqual(result.exit_code, ExitCode.USER_STOP)
        self.assertEqual(statuses, ["backing"])
        self.assertNotIn("backup_failed", statuses)
        self.assertNotIn("done", statuses)

    def test_ambiguity_escalates_over_a_concurrent_user_stop(self):
        # Thread-safe safety escalation: while the write runs, another component
        # records a 40/user_requested_stop; the write then fails ambiguously. The
        # ambiguity must WIN over the concurrently-recorded user stop.
        holder = {}

        def during_write():
            holder["orch"]._record_stop(StopResult(
                exit_code=ExitCode.USER_STOP, reason=REASON_USER_REQUESTED_STOP))
        writer = WriteChunkRaceTests._FakeWriter(
            start=True, on_run=during_write, raises=RuntimeError("robocopy exit 8"))
        orch, statuses = self._o(writer)
        holder["orch"] = orch
        with redirect_stdout(io.StringIO()):
            result = orch._write_chunk(7, _desc(0), "T", False, threading.Event())
        self.assertEqual(result.reason, REASON_AMBIGUOUS_BACKING_CHUNK)
        self.assertEqual(orch._get_recorded_stop().reason,
                         REASON_AMBIGUOUS_BACKING_CHUNK,
                         "the recorded reason must escalate to the ambiguity")

    def test_record_stop_escalate_overrides_recorded_reason(self):
        # Unit-level proof of the escalation flag itself.
        orch = _orch()
        orch._record_stop(StopResult(exit_code=ExitCode.USER_STOP,
                                     reason=REASON_USER_REQUESTED_STOP))
        orch._record_stop(StopResult(exit_code=ExitCode.SAFETY_BLOCK,
                                     reason=REASON_AMBIGUOUS_BACKING_CHUNK),
                          escalate=True)
        self.assertEqual(orch._get_recorded_stop().reason,
                         REASON_AMBIGUOUS_BACKING_CHUNK)

    def test_failure_before_write_started_is_retryable(self):
        # A pre-write failure (e.g. drive not ready) never set 'backing', so the
        # chunk is safely re-fetchable — NOT ambiguous.
        writer = WriteChunkRaceTests._FakeWriter(
            start=False, raises=RuntimeError("LTO drive is not ready"))
        orch, statuses = self._o(writer)
        with redirect_stdout(io.StringIO()):
            result = orch._write_chunk(7, _desc(0), "T", False, threading.Event())
        self.assertEqual(result.exit_code, ExitCode.TRANSIENT_RESUMABLE)
        self.assertEqual(statuses, ["backup_failed"])
        self.assertNotIn("backing", statuses)

    def test_resume_after_ambiguous_returns_ambiguous_without_reconcile(self):
        # After any of the above left the chunk 'backing', the resume precheck
        # returns 20/ambiguous and does NO fetch/pack/rewrite/DB reconciliation.
        consulted = []
        orch = _orch()
        orch.db = SimpleNamespace(
            get_chunks_with_status=lambda sid, st: (
                consulted.append((sid, st)) or ([0] if st == "backing" else [])))
        # If any mutating/staging method were called, these would blow up.
        orch._stage_chunk = mock.Mock(side_effect=AssertionError("no staging"))
        with redirect_stdout(io.StringIO()):
            result = orch._detect_prior_backing_chunks(7)
        self.assertEqual(result.exit_code, ExitCode.SAFETY_BLOCK)
        self.assertEqual(result.reason, REASON_AMBIGUOUS_BACKING_CHUNK)
        orch._stage_chunk.assert_not_called()
        self.assertEqual(consulted, [(7, "backing")],
                         "resume only reads status; it does not reconcile")


class FinalChunkCancelTerminalTests(unittest.TestCase):
    """Requirement 2: a cooperative Ctrl+C on the LAST chunk. The chunk commits
    'done' AND the session is committed 'completed' immediately (nothing left to
    resume), yet THIS invocation still exits 40/user_requested_stop — never
    0/completed. The user-stop is recorded even with no next gate."""

    def setUp(self):
        ro.CANCEL.clear()
        self.addCleanup(ro.CANCEL.clear)

    def _run(self):
        orch = _orch(prefetch_ahead=1, staging_max_bytes=10**9, fetch_cores=None)
        orch.cfg = SimpleNamespace(lto_drive="Z:", backup_log_dir=None)
        orch.notifier = None
        orch.chunk_max_files = 10**9
        state = {"done": set()}
        session_calls = []
        orch.db = SimpleNamespace(
            get_remote_session=lambda sid: {"tape_label": "T",
                                            "scan_complete": True},
            # Reflects committed chunks, so completion is DB-authoritative.
            get_pending_chunks=lambda sid: [c for c in (0,)
                                            if c not in state["done"]],
            count_chunks=lambda sid: 1,
            get_chunk_size_summary=lambda sid, ci=None: {0: (10, 10, 1)},
            get_chunk_files=lambda sid, ci: [],
            update_remote_session=lambda sid, **kw: session_calls.append(kw))
        orch._detect_prior_backing_chunks = lambda sid: None
        orch._validate_pending_chunk_limits = lambda *a: None
        orch._await_staging_capacity = lambda *a, **k: None
        orch._start_pipeline_heartbeat = lambda *a, **k: None
        orch._stage_chunk = lambda sid, ci, files: _desc(ci)

        def fake_write(session_id, desc, tape_label, eject_after, stop_pipeline):
            # The final write completes successfully; a cooperative cancel has
            # arrived — commit the chunk 'done' and record the user-stop.
            ro.CANCEL.set()
            state["done"].add(desc.chunk_index)
            orch._record_stop(StopResult(
                exit_code=ExitCode.USER_STOP,
                reason=REASON_USER_REQUESTED_STOP, resumable=True,
                source="write", session_id=session_id,
                chunk_index=desc.chunk_index))
            return None
        orch._write_chunk = fake_write

        with mock.patch.object(ro, "_ensure_lto_drive_ready", return_value=True), \
             mock.patch.object(ro, "RebootSentinel", _FakeSentinel), \
             mock.patch.object(ro, "send_best_effort"), \
             redirect_stdout(io.StringIO()):
            result = orch._run_session(11)
        return result, session_calls

    def test_returns_user_stop_and_marks_session_completed(self):
        result, session_calls = self._run()
        # Exit code stays 40 for this invocation...
        self.assertEqual(result.exit_code, ExitCode.USER_STOP)
        self.assertEqual(result.reason, REASON_USER_REQUESTED_STOP)
        # ...but the session IS committed complete (no later resume needed).
        self.assertTrue(
            any(kw.get("status") == "completed" for kw in session_calls),
            "the final committed chunk must mark the session completed")

    def test_status_files_preserve_the_user_stop(self):
        # Requirement 2 (status verification): last_failure.json records 40.
        with tempfile.TemporaryDirectory() as tmp:
            sr = StopResult(exit_code=ExitCode.USER_STOP,
                            reason=REASON_USER_REQUESTED_STOP)
            status_file.write_last_failure(tmp, sr)
            data = json.loads(open(
                os.path.join(tmp, status_file.LAST_FAILURE_FILENAME),
                encoding="utf-8").read())
            self.assertEqual(data["exit_code"], int(ExitCode.USER_STOP))
            self.assertEqual(data["reason"], REASON_USER_REQUESTED_STOP)

    def test_cli_exit_code_is_40_when_orchestrator_returns_user_stop(self):
        # CLI exit behavior: headless maps the orchestrator's USER_STOP to 40.
        result = StopResult(exit_code=ExitCode.USER_STOP,
                            reason=REASON_USER_REQUESTED_STOP)
        fake_orch = mock.Mock()
        fake_orch.run.return_value = result
        stack = ExitStack()
        self.addCleanup(stack.close)
        for name in ("_prepare_robocopy_exclusion", "_remove_robocopy_exclusion",
                     "install_cancel_handler", "uninstall_cancel_handler",
                     "reset_cancel", "_terminate_all_procs",
                     "unpin_current_process", "_cleanup_askpass_helpers"):
            stack.enter_context(mock.patch.object(cli, name, lambda *a, **k: None))
        stack.enter_context(mock.patch.object(
            cli, "RemoteOrchestrator", return_value=fake_orch))
        stack.enter_context(redirect_stdout(io.StringIO()))
        code = cli.run_remote_archiver_headless(_fake_cfg(), _FakeDB(), resume=True)
        self.assertEqual(code, int(ExitCode.USER_STOP))


class _FakeSentinel:
    triggered = False
    def __init__(self, *a, **k):
        pass
    def start(self):
        return self
    def stop(self):
        pass


class ProtectedProcessCancelTests(unittest.TestCase):
    """A cooperative cancel spares the active tape write; only a forced second
    Ctrl+C kills it. (runtime.py process registry.)"""

    class _FakeProc:
        def __init__(self):
            self.pid = 2**31 - 1   # not a real pid; psutil path falls back
            self.terminated = False
        def terminate(self):
            self.terminated = True
        def kill(self):
            self.terminated = True
        def wait(self, timeout=None):
            return 0

    def test_cooperative_terminate_spares_protected_but_kills_plain(self):
        prot = self._FakeProc()
        plain = self._FakeProc()
        rt.register_proc(prot, protected=True)
        rt.register_proc(plain)
        try:
            rt._terminate_all_procs()  # cooperative
            self.assertTrue(plain.terminated, "plain transfers are stopped")
            self.assertFalse(prot.terminated, "the tape write is spared")
            rt._terminate_all_procs(include_protected=True)  # forced
            self.assertTrue(prot.terminated, "force cuts the tape write")
        finally:
            rt.unregister_proc(prot)
            rt.unregister_proc(plain)

    def test_first_ctrl_c_spares_protected_second_kills_it(self):
        rt.CANCEL.clear()
        prot = self._FakeProc()
        plain = self._FakeProc()
        rt.register_proc(prot, protected=True)
        rt.register_proc(plain)
        try:
            with redirect_stdout(io.StringIO()):
                rt._cancel_handler(None, None)   # first: cooperative
            self.assertTrue(rt.CANCEL.is_set())
            self.assertTrue(plain.terminated)
            self.assertFalse(prot.terminated,
                             "first Ctrl+C must not cut the active tape write")
            with redirect_stdout(io.StringIO()):
                with self.assertRaises(KeyboardInterrupt):
                    rt._cancel_handler(None, None)  # second: force
            self.assertTrue(prot.terminated,
                            "second Ctrl+C force-quits, cutting the write")
        finally:
            rt.CANCEL.clear()
            rt.unregister_proc(prot)
            rt.unregister_proc(plain)


# ---------------------------------------------------------------------------
# Refinement 5 / scenario 38: the docs no longer mis-describe unmount
# ---------------------------------------------------------------------------
class DocsAssertionTests(unittest.TestCase):
    def test_doc_does_not_call_unmount_unsupported_or_claim_verified_sync(self):
        path = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                            "docs", "performance_insights_and_recommendations.md")
        text = open(path, encoding="utf-8").read().lower()
        self.assertNotIn("unmount is unsupported", text)
        self.assertNotIn("index-sync verified", text)
        # And it states the incompatibility positively (allowing line wraps).
        self.assertIn("not compatible with this", text)
        self.assertIn("supported", text)  # "unmount is a supported ... mode"


# ---------------------------------------------------------------------------
# Scenarios 1/2/31: EOF hardening + headless never touches input()
# ---------------------------------------------------------------------------
def _fake_cfg():
    return SimpleNamespace(
        remote_host="h", remote_user="u", remote_path="/p",
        windows_update_guard=False, backup_log_dir=None,
        lto_drive="Z:", ibm_eject_cmd="", staging_dir="s", restore_dir="r",
        local_manifest_archive_root=None)


class _FakeDB:
    def __init__(self):
        self.closed = False
    def acquire_archiver_lock(self):
        pass
    def release_archiver_lock(self):
        pass
    def close(self):
        self.closed = True


class MenuEofTests(unittest.TestCase):
    def _patch_menu(self, stack, input_side_effect, remote_result=None):
        db = _FakeDB()
        stack.enter_context(mock.patch.object(cli, "ConfigManager",
                                              return_value=_fake_cfg()))
        stack.enter_context(mock.patch.object(cli, "create_database_manager",
                                              return_value=db))
        stack.enter_context(mock.patch.object(cli, "configure_file_logging"))
        stack.enter_context(mock.patch.object(cli, "TapeManager"))
        stack.enter_context(mock.patch.object(cli, "LTORetriever"))
        stack.enter_context(mock.patch.object(cli.os, "system"))
        if remote_result is not None:
            stack.enter_context(mock.patch.object(
                cli, "run_remote_archiver", return_value=remote_result))
        stack.enter_context(mock.patch("builtins.input",
                                       side_effect=input_side_effect))
        stack.enter_context(redirect_stdout(io.StringIO()))
        return db

    def test_eof_at_menu_exits_cleanly_code_zero(self):
        with ExitStack() as stack:
            db = self._patch_menu(stack, EOFError())
            with self.assertRaises(SystemExit) as cm:
                cli.main()
        self.assertEqual(cm.exception.code, 0)
        self.assertTrue(db.closed)

    def test_eof_preserves_prior_archive_result_code(self):
        # Scenario 31: choose 6 (returns 10/network), then stdin closes → exit 10.
        result = StopResult(exit_code=ExitCode.TRANSIENT_RESUMABLE,
                            reason=REASON_NETWORK_RETRY_EXHAUSTED)
        with ExitStack() as stack:
            self._patch_menu(stack, ["6", EOFError()], remote_result=result)
            with self.assertRaises(SystemExit) as cm:
                cli.main()
        self.assertEqual(cm.exception.code, int(ExitCode.TRANSIENT_RESUMABLE))


class HeadlessCliTests(unittest.TestCase):
    def _patch_env(self, stack):
        stack.enter_context(mock.patch.object(
            cli, "_prepare_robocopy_exclusion", return_value=False))
        stack.enter_context(mock.patch.object(cli, "_remove_robocopy_exclusion"))
        stack.enter_context(mock.patch.object(cli, "install_cancel_handler"))
        stack.enter_context(mock.patch.object(cli, "uninstall_cancel_handler"))
        stack.enter_context(mock.patch.object(cli, "reset_cancel"))
        stack.enter_context(mock.patch.object(cli, "_terminate_all_procs"))
        stack.enter_context(mock.patch.object(cli, "unpin_current_process"))
        stack.enter_context(mock.patch.object(cli, "_cleanup_askpass_helpers"))
        # input must never be reached in a headless run.
        stack.enter_context(mock.patch(
            "builtins.input",
            side_effect=AssertionError("headless must not call input()")))
        stack.enter_context(redirect_stdout(io.StringIO()))

    def test_headless_returns_exit_code_without_input(self):
        result = StopResult(exit_code=ExitCode.TRANSIENT_RESUMABLE,
                            reason=REASON_NETWORK_RETRY_EXHAUSTED)
        fake_orch = mock.Mock()
        fake_orch.run.return_value = result
        with ExitStack() as stack:
            self._patch_env(stack)
            stack.enter_context(mock.patch.object(
                cli, "RemoteOrchestrator", return_value=fake_orch))
            code = cli.run_remote_archiver_headless(_fake_cfg(), _FakeDB(),
                                                    resume=True)
        self.assertEqual(code, int(ExitCode.TRANSIENT_RESUMABLE))
        fake_orch.run.assert_called_once_with(non_interactive=True, resume=True)


# ---------------------------------------------------------------------------
# Scenario 27 (+ 22/25/26): SSH auth-method split for a non-interactive run
# ---------------------------------------------------------------------------
class SshNonInteractiveTests(unittest.TestCase):
    def test_key_auth_uses_batchmode_yes(self):
        from src.remote_transport import _ssh_stream_command
        with mock.patch("src.remote_transport._has_command",
                        side_effect=lambda n: n == "ssh"):
            cmd, env, err = _ssh_stream_command("u", "h", "tar", password="")
        self.assertIsNone(err)
        self.assertIn("BatchMode=yes", cmd)

    def test_password_path_does_not_force_batchmode_yes(self):
        # Scenario 25: the password path must not be broken by an unconditional
        # BatchMode=yes (that would defeat sshpass/askpass).
        from src.remote_transport import _ssh_stream_command
        with mock.patch("src.remote_transport._has_command",
                        side_effect=lambda n: n in ("ssh", "sshpass")):
            cmd, env, err = _ssh_stream_command("u", "h", "tar", password="pw")
        self.assertIsNone(err)
        self.assertNotIn("BatchMode=yes", cmd)

    def test_no_credential_helper_is_a_classifiable_permanent_error(self):
        # Scenarios 22/26: no promptless credential → a permanent, fail-fast
        # error (never a hang, never input()).
        from src.remote_transport import _ssh_stream_command
        with mock.patch("src.remote_transport._has_command", return_value=False):
            cmd, env, err = _ssh_stream_command("u", "h", "tar", password="pw")
        self.assertIsNone(cmd)
        kind, _, reason = ro._classify_fetch_error(err)
        self.assertEqual(kind, "permanent")
        self.assertEqual(reason, REASON_MISSING_NONINTERACTIVE_CREDENTIAL)


if __name__ == "__main__":
    unittest.main()
