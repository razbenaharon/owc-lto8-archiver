"""Plan 1 execution contract — one test per clause, verified without tape.

The plan's "Execution contract" section states twelve properties. This file
asserts each one individually so the compliance matrix can cite a single test
per clause, rather than pointing at a suite and hoping.

Everything here is proved by **code inspection plus fake-backed behaviour**.
No real LTFS backend, no real ownership mutex contention, no drive, no
cartridge. The one thing that genuinely cannot be proved here — that a real
IBM drive behaves as the fakes do — is the operator-supervised hardware
rehearsal, which is explicitly a later activation gate.
"""
import inspect
import threading
import unittest
from types import SimpleNamespace
from unittest import mock

from src import ltfs_ownership as own
from src import remote_orchestrator as ro
from src.exit_codes import ExitCode, REASON_AMBIGUOUS_BACKING_CHUNK
from src.pipeline_types import ChunkStatus, StagedChunk
from src.remote_writer import RemoteChunkWriter

from lto_fakes import TapeLockObserver, TapeOperationLog

GiB = 1024 ** 3


def _desc(index, skip_tape=False):
    return StagedChunk(chunk_index=index, fetch_dir=f"/tmp/_f{index}",
                       pack_dir=f"/tmp/_p{index}", metadata=[],
                       staged_bytes=GiB, source_missing_files=[],
                       skip_tape=skip_tape)


class _ContractHarness(unittest.TestCase):
    """A writer whose every tape-facing action is ordered and recorded."""

    def setUp(self):
        ro.CANCEL.clear()
        self.addCleanup(ro.CANCEL.clear)
        self.timeline = TapeOperationLog()
        self.statuses = []
        outer = self

        orch = ro.RemoteOrchestrator.__new__(ro.RemoteOrchestrator)
        orch.cfg = SimpleNamespace(lto_drive="X:\\", backup_log_dir=None,
                                   ibm_eject_cmd="", eject_after_session=False)
        orch.notifier = None
        orch.governor = None
        orch.remote_host = "srv02.example"
        orch.remote_session_path = "/strg"
        orch.skipped_tracker = mock.MagicMock()
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

        def gate(session_id, desc, tape_label, stop_pipeline):
            outer.timeline.record("readiness")
            outer.timeline.record("cartridge")
            return None
        orch._pre_write_safety_gate = gate

        db = mock.MagicMock()
        db.get_chunk_size_summary.return_value = {}
        db.update_chunk_status.side_effect = (
            lambda sid, ci, status: outer.statuses.append((ci, status)))
        orch.db = db

        class Writer:
            def eject_tape(self, drive):
                outer.timeline.record("eject")

            def run(self, **kwargs):
                index = kwargs["remote_chunk_index"]
                outer.timeline.record("write", chunk=index)
                kwargs["on_write_start"]()
                if index in getattr(outer, "fail_at", ()):
                    raise RuntimeError("LTFS ERROR 19: media is write protected")
        orch._backup_writer = lambda cls=None: Writer()

        self.orch = orch
        self.writer = RemoteChunkWriter(orch)

    def run_group(self, indices, stop=None):
        with mock.patch("builtins.print"), \
                mock.patch.object(ro, "send_best_effort", lambda *a, **k: None):
            return self.writer._write_chunk_group(
                37, [_desc(i) for i in indices], "Tape_TEST", False,
                stop or threading.Event())


# =============================================================================
# Clause 1 — the finite write group is the ONLY active tape-writing path
# =============================================================================
class Clause01_OnlyTapePathTests(_ContractHarness):
    def test_the_group_writer_is_the_sole_caller_of_the_backup_writer(self):
        """Every tape write in the remote path goes through one method."""
        callers = []
        for name in ("remote_orchestrator", "remote_pipeline",
                     "remote_staging", "scan_frontier", "frontier_bootstrap",
                     "startup_reconcile", "archive_artifacts", "pg_scan"):
            module = __import__(f"src.{name}", fromlist=["x"])
            source = inspect.getsource(module)
            if "_backup_writer(" in source:
                callers.append(name)
        # Only the orchestrator, and only as the façade that BUILDS the writer.
        self.assertEqual(callers, ["remote_orchestrator"])
        façade = inspect.getsource(ro.RemoteOrchestrator._backup_writer)
        self.assertIn("cls(", façade)

    def test_only_the_writer_module_runs_the_copy(self):
        from src import remote_writer
        source = inspect.getsource(remote_writer)
        self.assertIn("_backup_writer", source)
        self.assertIn(".run(", source)


# =============================================================================
# Clause 2 — no pre-group LTFS probes in the three entry methods
# =============================================================================
class Clause02_NoPreGroupProbeTests(unittest.TestCase):
    PROBES = ("_ensure_lto_drive_ready", "_verify_mounted_cartridge",
              "get_volume_label", "_acquire_tape_io_lock", "eject_tape")

    def test_start_new_session_has_no_probe(self):
        source = inspect.getsource(ro.RemoteOrchestrator._start_new_session)
        for probe in self.PROBES:
            self.assertNotIn(probe, source, probe)

    def test_run_streaming_session_has_no_probe(self):
        source = inspect.getsource(
            ro.RemoteOrchestrator._run_streaming_session)
        for probe in self.PROBES:
            self.assertNotIn(probe, source, probe)

    def test_run_session_has_no_probe(self):
        source = inspect.getsource(ro.RemoteOrchestrator._run_session)
        for probe in self.PROBES:
            self.assertNotIn(probe, source, probe)

    def test_the_target_cartridge_is_announced_instead(self):
        for method in (ro.RemoteOrchestrator._run_streaming_session,
                       ro.RemoteOrchestrator._run_session):
            self.assertIn("_announce_target_cartridge",
                          inspect.getsource(method), method.__name__)


# =============================================================================
# Clause 3 — no LTFS access before a finite group is ready
# =============================================================================
class Clause03_NoAccessBeforeReadyTests(unittest.TestCase):
    def test_a_queue_below_the_threshold_produces_no_group_and_no_access(self):
        from src.ready_queue import ReadyItem, ReadyQueue, ReadyQueueLimits
        log = TapeOperationLog()
        queue = ReadyQueue(ReadyQueueLimits(20 * GiB, 40 * GiB, 80 * GiB, 48))
        queue.put(ReadyItem(chunk_index=0, pack_dir="/tmp/p",
                            prepared_bytes=int(1.7 * GiB), file_count=1,
                            desc=_desc(0)))
        with mock.patch.object(ro, "_ensure_lto_drive_ready",
                               side_effect=lambda *a, **k: log.record("ready")), \
                mock.patch.object(ro, "get_volume_label",
                                  side_effect=lambda d: log.record("label")):
            items, reason = queue.wait_for_group(timeout=0.5, poll=0.05)
        self.assertEqual(items, [])
        self.assertEqual(log.kinds(), [])
        self.assertFalse(own.owns_ltfs())

    def test_an_empty_group_touches_nothing(self):
        harness = _ContractHarness("run")
        harness.setUp()
        self.assertIsNone(harness.run_group([]))
        self.assertEqual(harness.timeline.kinds(), [])


# =============================================================================
# Clause 4 — no LTFS access while waiting for future chunks
# =============================================================================
class Clause04_NoAccessWhileWaitingTests(unittest.TestCase):
    def test_waiting_for_the_next_group_holds_no_ownership(self):
        from src.ready_queue import ReadyQueue, ReadyQueueLimits
        queue = ReadyQueue(ReadyQueueLimits(20 * GiB, 40 * GiB, 80 * GiB, 48))
        items, reason = queue.wait_for_group(timeout=0.4, poll=0.05)
        self.assertEqual(items, [])
        self.assertFalse(own.owns_ltfs())

    def test_the_pipeline_poll_loop_contains_no_tape_call(self):
        from src.remote_pipeline import RemotePipelineCoordinator
        source = inspect.getsource(RemotePipelineCoordinator)
        for token in ("_ensure_lto_drive_ready", "get_volume_label",
                      "_acquire_tape_io_lock", "eject_tape", "lto_drive"):
            self.assertNotIn(token, source, token)


# =============================================================================
# Clauses 5, 6, 7, 8 — ownership and gate arithmetic across a group
# =============================================================================
class Clause05to08_OwnershipArithmeticTests(_ContractHarness):
    def test_one_ownership_acquisition_and_one_gate_for_five_chunks(self):
        generation_before = own.OWNERSHIP.generation
        self.assertIsNone(self.run_group(range(5)))
        self.assertEqual(self.timeline.count("readiness"), 1)
        self.assertEqual(self.timeline.count("cartridge"), 1)
        self.assertEqual(self.timeline.count("write"), 5)
        self.assertEqual(self.orch._ownership_acquisitions, 1)
        self.assertEqual(self.orch._readiness_checks, 1)
        self.assertEqual(self.orch._cartridge_verifications, 1)
        # Exactly ONE physical kernel-mutex entry for the whole group.
        self.assertEqual(own.OWNERSHIP.generation - generation_before, 1)

    def test_members_are_written_consecutively_with_nothing_in_between(self):
        self.run_group(range(4))
        kinds = self.timeline.kinds()
        self.assertEqual(kinds[:2], ["readiness", "cartridge"])
        self.assertEqual(kinds[2:], ["write"] * 4)

    def test_ownership_is_never_released_between_members(self):
        """The observer records depth; it must never return to 0 mid-group."""
        observer = TapeLockObserver()
        depths = []
        outer = self

        class Writer:
            def eject_tape(self, drive):
                pass

            def run(self, **kwargs):
                depths.append(observer.depth)
                kwargs["on_write_start"]()
        self.orch._backup_writer = lambda cls=None: Writer()

        patches = observer.patches()
        for patch in patches:
            patch.start()
        try:
            self.run_group(range(4))
        finally:
            for patch in reversed(patches):
                patch.stop()
        self.assertEqual(len(depths), 4)
        for depth in depths:
            self.assertGreaterEqual(depth, 1, "ownership was released mid-group")
        self.assertEqual(observer.depth, 0, "ownership was not released at end")

    def test_ownership_is_released_after_the_group(self):
        self.run_group(range(3))
        self.assertFalse(own.owns_ltfs())

    def test_ownership_is_released_even_when_the_group_aborts(self):
        self.fail_at = {1}
        self.run_group(range(4))
        self.assertFalse(own.owns_ltfs())


# =============================================================================
# Clause 9 — remote auto-eject paths are disabled
# =============================================================================
class Clause09_NoAutoEjectTests(_ContractHarness):
    def test_a_completed_group_never_ejects(self):
        self.run_group(range(3))
        self.assertEqual(self.timeline.count("eject"), 0)

    def test_the_flag_is_refused_even_when_configured_true(self):
        self.orch.cfg.eject_after_session = True
        with mock.patch("builtins.print"):
            self.assertFalse(self.orch._eject_after_session())

    def test_no_remote_module_can_reach_an_eject(self):
        for name in ("remote_pipeline", "remote_staging", "scan_frontier",
                     "frontier_bootstrap", "startup_reconcile"):
            module = __import__(f"src.{name}", fromlist=["x"])
            self.assertNotIn("eject", inspect.getsource(module).lower()
                             .replace("rejected", "").replace("reject", ""),
                             name)


# =============================================================================
# Clause 10 — 'backing' is always ambiguous
# =============================================================================
class Clause10_BackingIsAmbiguousTests(_ContractHarness):
    def test_a_failure_after_the_write_started_leaves_backing_set(self):
        self.fail_at = {0}
        block = self.run_group([0, 1])
        self.assertEqual(block.reason, REASON_AMBIGUOUS_BACKING_CHUNK)
        self.assertFalse(block.resumable)
        self.assertIn((0, ChunkStatus.BACKING.value), self.statuses)
        for forbidden in (ChunkStatus.DONE.value,
                          ChunkStatus.BACKUP_FAILED.value,
                          ChunkStatus.PENDING.value):
            self.assertNotIn((0, forbidden), self.statuses)

    def test_no_later_chunk_is_attempted_after_an_ambiguous_failure(self):
        self.fail_at = {0}
        self.run_group(range(5))
        self.assertEqual(
            [e.chunk for e in self.timeline.of_kind("write")], [0])

    def test_the_transition_matrix_forbids_every_retry_out_of_backing(self):
        from src.pipeline_types import (CHUNK_TRANSITIONS,
                                        is_allowed_chunk_transition)
        self.assertEqual(CHUNK_TRANSITIONS[ChunkStatus.BACKING],
                         frozenset({ChunkStatus.DONE}))
        for target in (ChunkStatus.PENDING, ChunkStatus.FETCHING,
                       ChunkStatus.PACKING, ChunkStatus.FETCH_FAILED,
                       ChunkStatus.BACKUP_FAILED):
            self.assertFalse(
                is_allowed_chunk_transition(ChunkStatus.BACKING, target))

    def test_no_claim_or_reclaim_path_can_touch_a_backing_chunk(self):
        from src.pg_sessions import PgSessionMixin
        self.assertNotIn("backing", PgSessionMixin.RECLAIMABLE_CHUNK_STATES)
        for name in ("claim_chunk_for_staging", "renew_chunk_claim",
                     "release_chunk_claim", "list_expired_chunk_claims",
                     "reclaim_expired_chunk"):
            source = inspect.getsource(getattr(PgSessionMixin, name))
            self.assertTrue(
                "status <> 'backing'" in source
                or "RECLAIMABLE_CHUNK_STATES" in source
                or "status IN ('pending','fetch_failed','backup_failed')" in source,
                f"{name} does not exclude 'backing'")


# =============================================================================
# Clause 11 — a database read failure is not proof of "no backing chunk"
# =============================================================================
class Clause11_FailClosedTests(unittest.TestCase):
    def _orch(self, exc):
        orch = ro.RemoteOrchestrator.__new__(ro.RemoteOrchestrator)
        orch.notifier = None
        orch.db = SimpleNamespace(
            get_chunks_with_status=mock.Mock(side_effect=exc))
        return orch

    def test_every_failure_mode_blocks_the_run(self):
        for exc in (RuntimeError("connection lost"),
                    OSError("socket closed"),
                    ValueError("malformed result"),
                    TimeoutError("statement timeout")):
            orch = self._orch(exc)
            with mock.patch("builtins.print"), \
                    mock.patch.object(ro, "send_best_effort",
                                      lambda *a, **k: None):
                block = orch._detect_prior_backing_chunks(37)
            self.assertIsNotNone(block, repr(exc))
            self.assertEqual(block.exit_code, ExitCode.SAFETY_BLOCK)
            self.assertEqual(block.reason, REASON_AMBIGUOUS_BACKING_CHUNK)
            self.assertFalse(block.resumable)

    def test_the_per_chunk_probe_also_fails_closed(self):
        orch = self._orch(RuntimeError("connection lost"))
        with mock.patch("src.remote_orchestrator.get_logger"):
            self.assertTrue(orch._chunk_backing_from_prior_run(37, 4))

    def test_multiple_backing_rows_all_block(self):
        orch = ro.RemoteOrchestrator.__new__(ro.RemoteOrchestrator)
        orch.notifier = None
        orch.db = SimpleNamespace(
            get_chunks_with_status=lambda sid, status: [4, 9, 17])
        with mock.patch("builtins.print"), \
                mock.patch.object(ro, "send_best_effort", lambda *a, **k: None):
            block = orch._detect_prior_backing_chunks(37)
        self.assertIsNotNone(block)
        self.assertEqual(block.reason, REASON_AMBIGUOUS_BACKING_CHUNK)
        self.assertIn("5", block.detailed_reason)      # 1-based chunk numbers

    def test_elapsed_time_alone_never_releases_a_claim(self):
        from src.pg_sessions import PgSessionMixin
        source = inspect.getsource(PgSessionMixin.reclaim_expired_chunk)
        self.assertIn("if not evidence", source)
        self.assertIn("not proof", source)


# =============================================================================
# Clause 12 — session facts are measured, not read from documentation
# =============================================================================
class Clause12_NoTrustedDocumentationTests(unittest.TestCase):
    def test_the_report_hardcodes_no_session_or_chunk_facts(self):
        from src.startup_reconcile import session_frontier_report
        source = inspect.getsource(session_frontier_report)
        for hardcoded in ("37", "112", "113", "96", "108",
                          "Tape_02", "Tape_03"):
            self.assertNotIn(hardcoded, source, hardcoded)

    def test_no_plan_1_module_hardcodes_a_session_id(self):
        import re
        for name in ("remote_pipeline", "scan_frontier", "pg_scan",
                     "startup_reconcile", "frontier_bootstrap",
                     "archive_artifacts", "remote_writer"):
            module = __import__(f"src.{name}", fromlist=["x"])
            source = inspect.getsource(module)
            self.assertIsNone(
                re.search(r"session_id\s*==\s*\d+", source), name)
            self.assertIsNone(
                re.search(r"chunk_index\s*==\s*\d{2,}", source), name)

    def test_the_report_reads_membership_from_plan_rows_not_cached_totals(self):
        from src.pg_sessions import PgSessionMixin
        source = inspect.getsource(
            PgSessionMixin.get_session_membership_summary)
        self.assertIn("remote_plan_files", source)
        self.assertNotIn("s.chunk_count", source)
        self.assertNotIn("s.total_files", source)


if __name__ == "__main__":
    unittest.main()
