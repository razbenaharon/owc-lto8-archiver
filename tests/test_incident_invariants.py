"""Every rule in docs/incidents/, asserted against the Plan 1 code.

This file exists because a refactor is exactly when hard-won safety rules get
lost — they live in prose, the code moves, and nothing notices. Each test below
names the incident it protects, so a future change that breaks one is told which
outage it is re-enabling.

The rules come from ``docs/incidents/000-no-physical-intervention-policy.md``
and the numbered incidents. They are asserted BEHAVIOURALLY where possible (run
the code, watch what it does to a fake drive) and structurally only where
behaviour cannot be reached without a real device.
"""
import os
import threading
import unittest
from types import SimpleNamespace
from unittest import mock

from src import remote_orchestrator as ro
from src.exit_codes import (ExitCode, StopResult,
                            REASON_AMBIGUOUS_BACKING_CHUNK,
                            REASON_TAPE_WRITE_FAILED,
                            REASON_UNEXPECTED_TAPE_OR_DB_STATE)
from src.pipeline_types import ChunkStatus, ContainerFormat, StagedChunk
from src.remote_writer import RemoteChunkWriter

from lto_fakes import TapeOperationLog

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GiB = 1024 ** 3


def executable_source(module_name):
    """A module's source with comments and docstrings removed.

    These tests search for forbidden operations by name, and this codebase
    documents its safety rules *in* the modules that enforce them — the module
    that must never eject explains at length why it never ejects. Searching raw
    text would therefore flag the explanation as the violation. Stripping prose
    keeps the search honest: a real call is still code, and string literals
    (an executable path, say) are deliberately kept.
    """
    import ast
    import io
    import tokenize

    path = os.path.join(PROJECT_ROOT, "src", f"{module_name}.py")
    with open(path, encoding="utf-8") as handle:
        source = handle.read()

    docstring_lines = set()
    for node in ast.walk(ast.parse(source)):
        body = getattr(node, "body", None)
        if not isinstance(body, list):      # ast.Lambda.body is an expr
            continue
        for child in body:
            if (isinstance(child, ast.Expr)
                    and isinstance(child.value, ast.Constant)
                    and isinstance(child.value.value, str)):
                docstring_lines.update(
                    range(child.lineno, (child.end_lineno or child.lineno) + 1))

    kept = []
    for token in tokenize.generate_tokens(io.StringIO(source).readline):
        if token.type == tokenize.COMMENT:
            continue
        if token.start[0] in docstring_lines:
            continue
        kept.append(token.string)
    return "\n".join(kept)


def _desc(index, skip_tape=False):
    return StagedChunk(chunk_index=index, fetch_dir=f"/tmp/_f{index}",
                       pack_dir=f"/tmp/_p{index}", metadata=[],
                       staged_bytes=0 if skip_tape else GiB,
                       source_missing_files=["missing"] if skip_tape else [],
                       session_id=37, packaging_format=ContainerFormat.ZIP,
                       skip_tape=skip_tape)


class _WriterHarness(unittest.TestCase):
    """A writer whose every tape-facing collaborator is recorded."""

    def setUp(self):
        ro.CANCEL.clear()
        self.addCleanup(ro.CANCEL.clear)
        self.log = TapeOperationLog()
        self.written = []
        self.statuses = []

        orch = ro.RemoteOrchestrator.__new__(ro.RemoteOrchestrator)
        orch.cfg = SimpleNamespace(lto_drive="X:\\", backup_log_dir=None,
                                   ibm_eject_cmd="", eject_after_session=False)
        orch.notifier = None
        orch.remote_host = "host.example"
        orch.remote_session_path = "/strg"
        orch.skipped_tracker = mock.MagicMock()
        orch.governor = None
        orch._consumer_chunk = None
        orch._ownership_acquisitions = 0
        orch._readiness_checks = 0
        orch._cartridge_verifications = 0
        orch._staged_lock = threading.RLock()
        orch._staged_bytes = 0
        orch._stop_lock = threading.Lock()
        orch._stop_result = None
        orch._cleanup_dir = lambda *_a: None
        orch._preserve_desc = mock.MagicMock()
        orch._discard_desc = mock.MagicMock()
        orch._ensure_remote_chunk_fits_tape = lambda *a, **k: True

        outer = self

        def gate(session_id, desc, tape_label, stop_pipeline):
            outer.log.record("gate", tape=tape_label)
            return None
        orch._pre_write_safety_gate = gate

        db = mock.MagicMock()
        db.get_chunk_size_summary.return_value = {}
        db.get_chunk_packaging_format.return_value = ContainerFormat.ZIP

        def status(session_id, chunk_index, value):
            outer.statuses.append((chunk_index, value))
        db.update_chunk_status.side_effect = status
        orch.db = db

        class Writer:
            def eject_tape(self, drive):
                outer.log.record("eject", drive=drive)

            def run(self, **kwargs):
                index = kwargs["remote_chunk_index"]
                outer.log.record("write", chunk=index)
                kwargs["on_write_start"]()
                outer.written.append(index)
                if index in getattr(outer, "fail_after_start", ()):
                    raise RuntimeError("LTFS write error 19: media is "
                                       "write protected")
        orch._backup_writer = lambda cls=None: Writer()
        self.orch = orch
        self.writer = RemoteChunkWriter(orch)


# =============================================================================
# Policy 000 rule 1 — never eject remotely
# =============================================================================
class NeverEjectTests(_WriterHarness):
    """`LtfsCmdEject` is physical and irreversible from here: there is no
    software 'load' for a cartridge sitting out of the slot."""

    def test_a_normal_group_never_ejects(self):
        with mock.patch("builtins.print"):
            block = self.writer._write_chunk_group(
                37, [_desc(0), _desc(1)], "Tape_03", False, threading.Event())
        self.assertIsNone(block)
        self.assertEqual(self.log.count("eject"), 0)

    def test_eject_after_session_true_still_never_ejects(self):
        self.orch.cfg.eject_after_session = True
        with mock.patch("builtins.print"):
            self.writer._write_chunk_group(
                37, [_desc(0)], "Tape_03", self.orch._eject_after_session(),
                threading.Event())
        self.assertEqual(self.log.count("eject"), 0)

    def test_a_skip_tape_chunk_never_ejects(self):
        with mock.patch("builtins.print"), \
                mock.patch.object(ro, "send_best_effort", lambda *a, **k: None):
            self.writer._write_skip_tape_chunk(37, _desc(0, skip_tape=True),
                                               "Tape_03", True)
        self.assertEqual(self.log.count("eject"), 0)

    def test_the_orchestrator_refuses_the_configured_eject(self):
        with mock.patch("builtins.print"):
            self.assertFalse(self.orch._eject_after_session())


# =============================================================================
# Policy 000 rule 4 + 6 — stop at chunk boundaries, bounded blast radius
# =============================================================================
class ChunkBoundaryStopTests(_WriterHarness):
    def test_a_stop_between_chunks_ends_the_group_at_a_boundary(self):
        stop = threading.Event()
        descs = [_desc(i) for i in range(4)]
        original = self.writer.host._write_one_chunk_owned

        def stop_after_first(session_id, desc, tape_label, eject_after):
            result = original(session_id, desc, tape_label, eject_after)
            stop.set()                      # a staged restart arrives
            return result
        self.writer.host._write_one_chunk_owned = stop_after_first

        with mock.patch("builtins.print"):
            block = self.writer._write_chunk_group(37, descs, "Tape_03", False,
                                                   stop)
        # Chunk 0 committed; the group stopped BEFORE starting chunk 1.
        self.assertEqual(self.written, [0])
        self.assertIsNotNone(block)
        self.assertTrue(block.preserve_pack,
                        "an unstarted chunk must keep its pack")

    def test_a_recorded_reboot_stop_is_honoured_between_chunks(self):
        """Incident 005: RebootSentinel records a stop mid-group; the group
        must notice it at the next boundary, not at the next group."""
        descs = [_desc(i) for i in range(3)]
        original = self.writer.host._write_one_chunk_owned

        def reboot_after_first(session_id, desc, tape_label, eject_after):
            result = original(session_id, desc, tape_label, eject_after)
            self.orch._record_stop(StopResult(
                exit_code=ExitCode.TRANSIENT_RESUMABLE,
                reason="windows_reboot_pending", resumable=True,
                source="sentinel"))
            return result
        self.writer.host._write_one_chunk_owned = reboot_after_first

        with mock.patch("builtins.print"):
            block = self.writer._write_chunk_group(37, descs, "Tape_03", False,
                                                   threading.Event())
        self.assertEqual(self.written, [0])
        self.assertEqual(block.reason, "windows_reboot_pending")

    def test_ownership_is_released_even_when_the_group_aborts(self):
        from src import ltfs_ownership as own
        stop = threading.Event()
        stop.set()
        with mock.patch("builtins.print"):
            self.writer._write_chunk_group(37, [_desc(0)], "Tape_03", False,
                                           stop)
        self.assertFalse(own.owns_ltfs())


# =============================================================================
# Policy 000 rule 7 + incident 010 — a hard write error stops everything
# =============================================================================
class LatchingWriteErrorTests(_WriterHarness):
    def test_a_write_error_after_start_leaves_the_chunk_ambiguous(self):
        self.fail_after_start = {0}
        with mock.patch("builtins.print"), \
                mock.patch.object(ro, "send_best_effort", lambda *a, **k: None):
            block = self.writer._write_chunk_group(
                37, [_desc(0), _desc(1)], "Tape_03", False, threading.Event())
        self.assertEqual(block.reason, REASON_AMBIGUOUS_BACKING_CHUNK)
        self.assertFalse(block.resumable)
        # 'backing' was set and never cleared: the bytes may be on tape.
        self.assertIn((0, ChunkStatus.BACKING.value), self.statuses)
        self.assertNotIn((0, ChunkStatus.DONE.value), self.statuses)
        self.assertNotIn((0, ChunkStatus.BACKUP_FAILED.value), self.statuses)

    def test_no_later_chunk_is_attempted_after_a_hard_error(self):
        """Rule 7: never retry blindly into a failing drive."""
        self.fail_after_start = {0}
        with mock.patch("builtins.print"), \
                mock.patch.object(ro, "send_best_effort", lambda *a, **k: None):
            self.writer._write_chunk_group(
                37, [_desc(i) for i in range(5)], "Tape_03", False,
                threading.Event())
        self.assertEqual(self.written, [0], "the group kept writing after a "
                                            "hard error")

    def test_no_later_group_runs_after_a_hard_error(self):
        """The pipeline coordinator must stop, not fetch the next group."""
        from src.remote_pipeline import RemotePipelineCoordinator
        from src.ready_queue import ReadyItem, ReadyQueue, ReadyQueueLimits

        groups = []

        def failing_group(session_id, descs, tape_label, eject, stop_evt):
            groups.append([d.chunk_index for d in descs])
            return StopResult(
                exit_code=ExitCode.SAFETY_BLOCK,
                reason=REASON_AMBIGUOUS_BACKING_CHUNK, resumable=False,
                source="write", session_id=session_id,
                chunk_index=descs[0].chunk_index, preserve_pack=True)

        host = self.orch
        host._write_chunk_group = failing_group
        host._producer_err = None
        host._signal_producer_completion = lambda q, e: q.close()
        host._validate_chunk_file_limit = lambda *a, **k: None
        host._await_staging_capacity = lambda *a, **k: None
        host._stage_chunk = lambda sid, ci, files: _desc(ci)
        host.db.get_chunks_with_status.return_value = [0, 1, 2, 3]
        host.db.get_chunk_size_summary.return_value = {0: (10, 10, 1)}
        host.db.get_chunk_files.return_value = []

        queue = ReadyQueue(ReadyQueueLimits(1, 1, 10 ** 12, 48))
        coordinator = RemotePipelineCoordinator(
            host=host, session_id=37, tape_label="Tape_03", ready_q=queue,
            stop_event=threading.Event(), metrics=mock.MagicMock(),
            backlog_limit=64)
        with mock.patch("builtins.print"):
            outcome = coordinator.run()
        self.assertEqual(len(groups), 1, "a second group ran after a hard "
                                         "tape failure")
        self.assertTrue(outcome.failed)


# =============================================================================
# Incident 011 — the cartridge identity check
# =============================================================================
class CartridgeIdentityTests(unittest.TestCase):
    """Defect 3: a resumed session wrote to whatever was loaded, cataloging it
    under the session's row. Nothing failed. That must stay impossible."""

    def _orch(self, mounted):
        orch = ro.RemoteOrchestrator.__new__(ro.RemoteOrchestrator)
        orch.cfg = SimpleNamespace(lto_drive="X:\\")
        orch.notifier = None
        return orch, mounted

    def test_a_mismatched_cartridge_blocks_the_write(self):
        orch, _ = self._orch("Tape_03")
        with mock.patch.object(ro, "get_volume_label", return_value="Tape_03"), \
                mock.patch("builtins.print"), \
                mock.patch.object(ro, "send_best_effort", lambda *a, **k: None):
            block = orch._verify_mounted_cartridge("Tape_02")
        self.assertIsNotNone(block)
        self.assertEqual(block.reason, REASON_UNEXPECTED_TAPE_OR_DB_STATE)
        self.assertFalse(block.resumable)

    def test_a_differently_cased_label_is_the_same_cartridge(self):
        """LTFS reports the mounted label in its own case (observed
        uppercase, e.g. 'TAPE_03'); the catalog's convention is mixed-case
        ('Tape_03'). Same cartridge, so this must NOT block -- neither the
        LTFS volume nor the catalog row is ever renamed to make them agree."""
        orch, _ = self._orch("TAPE_03")
        with mock.patch.object(ro, "get_volume_label", return_value="TAPE_03"), \
                mock.patch("builtins.print"), \
                mock.patch.object(ro, "send_best_effort", lambda *a, **k: None):
            block = orch._verify_mounted_cartridge("Tape_03")
        self.assertIsNone(block)

    def test_an_exact_match_still_passes(self):
        orch, _ = self._orch("Tape_03")
        with mock.patch.object(ro, "get_volume_label", return_value="Tape_03"), \
                mock.patch("builtins.print"), \
                mock.patch.object(ro, "send_best_effort", lambda *a, **k: None):
            block = orch._verify_mounted_cartridge("Tape_03")
        self.assertIsNone(block)

    def test_a_whitespace_mounted_label_still_blocks(self):
        """A blank/whitespace label must never be silently normalized into a
        match, even against a real expected label."""
        orch, _ = self._orch("   ")
        with mock.patch.object(ro, "get_volume_label", return_value="   "), \
                mock.patch("builtins.print"), \
                mock.patch.object(ro, "send_best_effort", lambda *a, **k: None):
            block = orch._verify_mounted_cartridge("Tape_03")
        self.assertIsNotNone(block)
        self.assertEqual(block.reason, REASON_UNEXPECTED_TAPE_OR_DB_STATE)
        self.assertFalse(block.resumable)

    def test_an_unreadable_label_fails_closed(self):
        orch, _ = self._orch(None)
        for behaviour in ({"return_value": None},
                          {"side_effect": OSError("device not ready")}):
            with mock.patch.object(ro, "get_volume_label", **behaviour), \
                    mock.patch("builtins.print"), \
                    mock.patch.object(ro, "send_best_effort",
                                      lambda *a, **k: None):
                block = orch._verify_mounted_cartridge("Tape_02")
            self.assertIsNotNone(block, behaviour)

    def test_the_group_gate_still_verifies_the_cartridge(self):
        import inspect
        gate = inspect.getsource(ro.RemoteOrchestrator._pre_write_safety_gate)
        self.assertIn("_verify_mounted_cartridge", gate)
        # ...exactly once per group, not per chunk.
        self.assertEqual(gate.count("_verify_mounted_cartridge("), 1)

    def test_both_loops_announce_the_target_cartridge_without_reading_it(self):
        """The early check moved into the group, so silence is not acceptable:
        the operator is told which cartridge is expected, and when it will be
        enforced."""
        import inspect
        for method in (ro.RemoteOrchestrator._run_streaming_session,
                       ro.RemoteOrchestrator._run_session):
            source = inspect.getsource(method)
            self.assertIn("_announce_target_cartridge", source,
                          method.__name__)

        orch = ro.RemoteOrchestrator.__new__(ro.RemoteOrchestrator)
        log = TapeOperationLog()
        with mock.patch("builtins.print"), \
                mock.patch.object(ro, "get_volume_label",
                                  side_effect=lambda d: log.record("label")), \
                mock.patch.object(ro, "_ensure_lto_drive_ready",
                                  side_effect=lambda *a, **k: log.record("ready")):
            message = orch._announce_target_cartridge(37, "Tape_03")
        self.assertIn("Tape_03", message)
        self.assertIn("REFUSED", message)
        self.assertEqual(log.kinds(), [],
                         "the announcement touched the drive")


class TapeLabelCanonicalizationTests(unittest.TestCase):
    """Unit coverage for the comparison helper itself (incident 011 fix):
    case-insensitive on real labels, but blank/whitespace never "matches"
    anything -- including another blank -- just because both normalize to
    the same empty string."""

    def test_differently_cased_labels_match(self):
        self.assertTrue(ro._tape_labels_match("TAPE_03", "Tape_03"))
        self.assertTrue(ro._tape_labels_match("tape_03", "TAPE_03"))

    def test_exact_matches_pass(self):
        self.assertTrue(ro._tape_labels_match("Tape_03", "Tape_03"))

    def test_genuinely_different_labels_fail(self):
        self.assertFalse(ro._tape_labels_match("Tape_02", "Tape_03"))
        self.assertFalse(ro._tape_labels_match("TAPE_02", "Tape_03"))

    def test_whitespace_or_empty_labels_fail_rather_than_normalize(self):
        self.assertFalse(ro._tape_labels_match("", "Tape_03"))
        self.assertFalse(ro._tape_labels_match("Tape_03", ""))
        self.assertFalse(ro._tape_labels_match("   ", "Tape_03"))
        self.assertFalse(ro._tape_labels_match(None, "Tape_03"))
        # Both sides blank must still fail closed -- not "equal because both
        # normalize to ''".
        self.assertFalse(ro._tape_labels_match("", ""))
        self.assertFalse(ro._tape_labels_match("   ", "   "))
        self.assertFalse(ro._tape_labels_match(None, None))


# =============================================================================
# Incident 005 — the guards that actually hold
# =============================================================================
class RestartGuardTests(unittest.TestCase):
    def test_the_pre_write_reboot_check_is_still_in_the_gate(self):
        """'Refuse to START work likely to be interrupted' — the guard that
        holds against a 60-second SCCM warning."""
        import inspect
        gate = inspect.getsource(ro.RemoteOrchestrator._pre_write_safety_gate)
        self.assertIn("_pre_tape_write_reboot_check", gate)

    def test_the_mount_must_be_time5_before_every_group(self):
        """`sync_type=unmount` is what turned a restart into ~126 GB of loss."""
        import inspect
        gate = inspect.getsource(ro.RemoteOrchestrator._pre_write_safety_gate)
        self.assertIn("_verify_current_mount_time5", gate)

    def test_both_loops_still_arm_the_reboot_sentinel(self):
        import inspect
        for method in (ro.RemoteOrchestrator._run_streaming_session,
                       ro.RemoteOrchestrator._run_session):
            self.assertIn("RebootSentinel", inspect.getsource(method),
                          method.__name__)

    def test_media_health_is_checked_before_every_group(self):
        import inspect
        gate = inspect.getsource(ro.RemoteOrchestrator._pre_write_safety_gate)
        self.assertIn("_verify_ltfs_media_health", gate)


# =============================================================================
# Policy 000 rule 8 — no ltfsck / format / reset, ever, unprompted
# =============================================================================
class NoDestructiveRecoveryTests(unittest.TestCase):
    def test_the_remote_pipeline_never_invokes_a_destructive_helper(self):
        forbidden = ("ltfsck", "mkltfs", "LtfsCmdFormat", "LtfsCmdUnformat",
                     "LtfsCmdRollback", "LtfsCmdLoad", "LtfsCmdEject",
                     "replace_formatted_tape", "reset_drive")
        for name in ("remote_orchestrator", "remote_writer", "remote_staging",
                     "remote_pipeline", "scan_frontier", "archive_artifacts",
                     "pg_scan"):
            source = executable_source(name)
            for token in forbidden:
                self.assertNotIn(token, source, f"{name}.py CALLS {token}")

    def test_the_new_scan_path_never_touches_the_tape(self):
        """A scan is a SOURCE operation. It must not be able to reach LTFS."""
        for name in ("scan_frontier", "archive_artifacts", "pg_scan"):
            source = executable_source(name)
            for token in ("lto_drive", "_acquire_tape_io_lock",
                          "get_volume_label", "_ensure_lto_drive_ready",
                          "LTOBackup", "robocopy"):
                self.assertNotIn(token, source,
                                 f"{name}.py reaches the tape via {token}")


# =============================================================================
# Incident 009 — never infer success from a return code
# =============================================================================
class RobocopyEvidenceTests(unittest.TestCase):
    def test_the_durable_raw_log_and_classifier_are_untouched(self):
        from src import tape_write_log
        self.assertTrue(hasattr(tape_write_log, "TapeWriteRawLog"))
        from src.robocopy import _parse_robocopy_summary
        # The parser must still flag a MISSING summary rather than
        # reporting zeroed counters as "no failures" (incident 009).
        parsed = _parse_robocopy_summary("ERROR 32 Copying File x.bin\n")
        self.assertFalse(parsed["summary_found"])

    def test_the_writer_still_delegates_the_commit_to_ltobackup(self):
        """The conservative classifier lives in LTOBackup; the extraction must
        not have bypassed it."""
        import inspect
        source = inspect.getsource(RemoteChunkWriter._write_one_chunk_owned)
        self.assertIn("_backup_writer", source)
        self.assertIn("on_write_start", source)
        # The writer must not decide success from a return code itself.
        self.assertNotIn("returncode", source)


# =============================================================================
# Incident 008 — the fetch-overrun trap is still configurable and armed
# =============================================================================
class FetchOverrunTests(unittest.TestCase):
    def test_the_watchdog_decision_is_still_pure_and_testable(self):
        from src.remote_staging import _fetch_watchdog_action
        self.assertEqual(
            _fetch_watchdog_action(cur=300, last_growth_at=0, now=0,
                                   total_bytes=100, abort_factor=2.0,
                                   stall_timeout=600, free_bytes=10 ** 12,
                                   reserve_bytes=1),
            "overrun")
        self.assertEqual(
            _fetch_watchdog_action(cur=1, last_growth_at=0, now=0,
                                   total_bytes=100, abort_factor=2.0,
                                   stall_timeout=600, free_bytes=0,
                                   reserve_bytes=1),
            "diskfull")
        self.assertIsNone(
            _fetch_watchdog_action(cur=90, last_growth_at=0, now=0,
                                   total_bytes=100, abort_factor=2.0,
                                   stall_timeout=0, free_bytes=10 ** 12,
                                   reserve_bytes=1))

    def test_the_overrun_alert_threshold_survived_the_move(self):
        from src.remote_staging import _FETCH_OVERRUN_WARN_FACTOR
        self.assertEqual(_FETCH_OVERRUN_WARN_FACTOR, 1.10)


# =============================================================================
# Incident 006 — the drive letter is not stable
# =============================================================================
class DriveLetterTests(unittest.TestCase):
    def test_nothing_new_hardcodes_a_drive_letter(self):
        for name in ("remote_orchestrator", "remote_writer", "remote_staging",
                     "remote_pipeline", "scan_frontier", "archive_artifacts",
                     "pg_scan"):
            source = executable_source(name)
            for letter in ("Z:\\", "E:\\", '"Z:"', "'Z:'"):
                self.assertNotIn(letter, source,
                                 f"{name}.py hardcodes {letter}")


if __name__ == "__main__":
    unittest.main()
