"""Phase 4: byte-bounded ready queue and multi-chunk write groups.

All bounded by explicit timeouts. No tape, no Z:\\, no IBM helper: LTFS is a
fake adapter and the writer is a recording double.
"""
import os
import threading
import time
import unittest
from types import SimpleNamespace
from unittest import mock

from src import ltfs
from src import ltfs_ownership as own
from src import ready_queue as rq
from src.exit_codes import (ExitCode, StopResult,
                            REASON_AMBIGUOUS_BACKING_CHUNK,
                            REASON_TAPE_WRITE_FAILED,
                            REASON_USER_REQUESTED_STOP)
from src.ready_queue import (ReadyItem, ReadyQueue, ReadyQueueLimits,
                             STATE_PRESERVED, STATE_READY, STATE_WRITTEN)

GiB = 1024 ** 3
TIMEOUT = 20          # every blocking test is bounded by this


def _limits(min_b=20 * GiB, target=40 * GiB, max_b=80 * GiB, chunks=48):
    return ReadyQueueLimits(min_b, target, max_b, chunks)


def _item(index, gib=1.7):
    return ReadyItem(chunk_index=index, pack_dir=f"/tmp/_pack_{index}",
                     prepared_bytes=int(gib * GiB), file_count=200000,
                     desc=SimpleNamespace(chunk_index=index,
                                          pack_dir=f"/tmp/_pack_{index}",
                                          staged_bytes=int(gib * GiB),
                                          skip_tape=False))


# =============================================================================
# Limits validation
# =============================================================================
class LimitValidationTests(unittest.TestCase):
    def test_valid_limits_accepted(self):
        self.assertTrue(_limits().validate())

    def test_min_must_be_positive(self):
        for bad in (0, -1):
            with self.assertRaises(ValueError):
                ReadyQueueLimits(bad, 40 * GiB, 80 * GiB, 48)

    def test_ordering_is_enforced(self):
        with self.assertRaises(ValueError):
            ReadyQueueLimits(50 * GiB, 40 * GiB, 80 * GiB, 48)   # min > target
        with self.assertRaises(ValueError):
            ReadyQueueLimits(20 * GiB, 90 * GiB, 80 * GiB, 48)   # target > max

    def test_chunk_count_must_be_positive(self):
        with self.assertRaises(ValueError):
            ReadyQueueLimits(20 * GiB, 40 * GiB, 80 * GiB, 0)

    def test_config_exposes_validated_limits(self):
        from src.config import ConfigManager
        limits = ConfigManager().ready_queue_limits
        self.assertTrue(limits.validate())
        self.assertLessEqual(limits.min_start_bytes, limits.target_bytes)
        self.assertLessEqual(limits.target_bytes, limits.max_bytes)

    def test_inconsistent_config_falls_back_to_documented_defaults(self):
        from src.config import ConfigManager
        cfg = ConfigManager()
        with mock.patch.object(cfg, "_ready_queue_raw",
                               side_effect=lambda k: {
                                   'min_ready_bytes_before_writer_start': 90 * GiB,
                                   'target_ready_bytes': 40 * GiB,
                                   'max_ready_bytes': 80 * GiB,
                                   'max_ready_chunks': 48}[k]):
            limits = cfg.ready_queue_limits
        self.assertEqual(limits.min_start_bytes, 20 * GiB)   # default restored

    def test_example_config_documents_the_section(self):
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with open(os.path.join(root, "config.example.ini"), encoding="utf-8") as fh:
            body = fh.read()
        self.assertIn("[PIPELINE]", body)
        for key in ("min_ready_bytes_before_writer_start", "target_ready_bytes",
                    "max_ready_bytes", "max_ready_chunks"):
            self.assertIn(key, body)


# =============================================================================
# Queue counters and limits
# =============================================================================
class ReadyQueueCounterTests(unittest.TestCase):
    def setUp(self):
        self.q = ReadyQueue(_limits())

    def test_multiple_chunks_can_be_queued(self):
        for i in range(5):
            self.assertTrue(self.q.put(_item(i)))
        self.assertEqual(self.q.ready_chunks, 5)

    def test_ready_bytes_are_exact(self):
        self.q.put(_item(0, gib=1.7))
        self.q.put(_item(1, gib=2.3))
        self.assertEqual(self.q.ready_bytes,
                         int(1.7 * GiB) + int(2.3 * GiB))

    def test_prepared_bytes_come_from_the_pack_not_the_remote_size(self):
        it = _item(0, gib=1.7)
        self.assertEqual(it.prepared_bytes, it.desc.staged_bytes)

    def test_chunk_count_limit_is_enforced(self):
        q = ReadyQueue(_limits(chunks=3))
        for i in range(3):
            self.assertTrue(q.put(_item(i, gib=0.001)))
        stop = threading.Event()
        stop.set()
        self.assertFalse(q.put(_item(99, gib=0.001), stop_event=stop))
        self.assertEqual(q.ready_chunks, 3)

    def test_byte_limit_is_enforced(self):
        q = ReadyQueue(_limits(min_b=1 * GiB, target=2 * GiB, max_b=4 * GiB))
        self.assertTrue(q.put(_item(0, gib=3)))
        stop = threading.Event()
        stop.set()
        self.assertFalse(q.put(_item(1, gib=3), stop_event=stop))
        self.assertEqual(q.ready_chunks, 1)

    def test_single_oversized_chunk_never_deadlocks(self):
        q = ReadyQueue(_limits(min_b=1 * GiB, target=2 * GiB, max_b=4 * GiB))
        self.assertTrue(q.put(_item(0, gib=99)))

    def test_counters_are_correct_across_the_full_lifecycle(self):
        items = [_item(i) for i in range(4)]
        for it in items:
            self.q.put(it)
        self.assertEqual(self.q.ready_chunks, 4)
        self.q.request_drain()
        group, reason = self.q.wait_for_group(timeout=TIMEOUT)
        self.assertEqual(len(group), 4)
        self.assertEqual(self.q.ready_chunks, 0)
        self.assertEqual(self.q.writing_chunks, 4)
        self.q.mark_written(group[0])
        self.q.mark_failed(group[1])
        self.q.mark_preserved(group[2])
        self.q.preserve_remaining([group[3]])
        self.assertEqual(self.q.writing_chunks, 0)
        m = self.q.metrics()
        self.assertEqual(m["chunks_written"], 1)
        self.assertEqual(m["chunks_failed"], 1)
        self.assertEqual(m["ready_chunks"], 0)

    def test_producer_blocked_time_is_measured(self):
        q = ReadyQueue(_limits(chunks=1))
        q.put(_item(0))
        stop = threading.Event()

        def unblock():
            time.sleep(0.3)
            stop.set()

        t = threading.Thread(target=unblock)
        t.start()
        self.assertFalse(q.put(_item(1), stop_event=stop))
        t.join(timeout=TIMEOUT)
        self.assertGreater(q.producer_blocked_seconds, 0)


# =============================================================================
# Writer-start rules
# =============================================================================
class WriterStartRuleTests(unittest.TestCase):
    def setUp(self):
        self.q = ReadyQueue(_limits())

    def test_writer_does_not_start_on_one_small_chunk(self):
        """The core Phase 4 behaviour: 1.7 GiB must NOT trigger a write while
        production continues and staging is healthy."""
        self.q.put(_item(0, gib=1.7))
        self.assertIsNone(self.q.start_reason())

    def test_writer_starts_at_the_minimum_threshold(self):
        for i in range(12):            # 12 x 1.7 GiB = 20.4 GiB
            self.q.put(_item(i, gib=1.7))
        self.assertEqual(self.q.start_reason(), "min_ready_bytes_reached")

    def test_writer_starts_at_the_chunk_ceiling(self):
        q = ReadyQueue(_limits(chunks=3))
        for i in range(3):
            q.put(_item(i, gib=0.01))
        self.assertEqual(q.start_reason(), "max_ready_chunks_reached")

    def test_writer_starts_for_a_final_partial_group(self):
        self.q.put(_item(0, gib=1.7))
        self.assertIsNone(self.q.start_reason())
        self.q.close()
        self.assertEqual(self.q.start_reason(),
                         "producer_finished_final_group")

    def test_writer_starts_when_producer_input_is_exhausted(self):
        self.q.put(_item(0, gib=1.7))
        self.q.set_producer_exhausted(True)
        self.assertEqual(self.q.start_reason(), "producer_input_exhausted")

    def test_writer_starts_to_relieve_staging_pressure(self):
        self.q.put(_item(0, gib=1.7))
        self.q.set_staging_pressure(True)
        self.assertEqual(self.q.start_reason(), "staging_pressure_drain")

    def test_writer_starts_on_explicit_drain_request(self):
        self.q.put(_item(0, gib=1.7))
        self.q.request_drain()
        self.assertEqual(self.q.start_reason(), "explicit_drain_requested")

    def test_empty_queue_never_starts(self):
        self.q.close()
        self.assertIsNone(self.q.start_reason())

    def test_group_snapshot_is_finite_and_does_not_grow(self):
        for i in range(12):
            self.q.put(_item(i, gib=1.7))
        group, _ = self.q.wait_for_group(timeout=TIMEOUT)
        self.assertEqual(len(group), 12)
        # More work arriving now must NOT join the in-flight group.
        self.q.put(_item(99, gib=1.7))
        self.assertEqual(len(group), 12)
        self.assertEqual(self.q.ready_chunks, 1)

    def test_wait_for_group_honours_its_timeout(self):
        self.q.put(_item(0, gib=1.7))         # below threshold, stays waiting
        started = time.time()
        group, reason = self.q.wait_for_group(timeout=1.0, poll=0.1)
        self.assertEqual(group, [])
        self.assertEqual(reason, "timeout")
        self.assertLess(time.time() - started, TIMEOUT)


# =============================================================================
# Ownership lifetime across a group (fake adapter, no tape)
# =============================================================================
class GroupOwnershipTests(unittest.TestCase):
    """One acquisition, one readiness check, several chunks."""

    def setUp(self):
        ltfs.reset_readiness("test setup")
        self.addCleanup(ltfs.reset_readiness, "test teardown")
        self.status_calls = []
        self.label_calls = []

        outer = self

        class CountingCommand(ltfs.LtfsDriveCommand):
            def drive_status(self, drive_path):
                outer.status_calls.append(drive_path)
                return "LTFS_MOUNTED", "", None

        previous = ltfs.set_ltfs_drive_command(CountingCommand())
        self.addCleanup(ltfs.set_ltfs_drive_command, previous)

    def test_one_ownership_period_covers_many_readiness_calls(self):
        """Inside one ownership period readiness is verified exactly once.

        This is the mechanism the orchestrator group relies on: the Phase 1
        cache stays valid because Phase 3 ownership is never released between
        chunks (a nested acquire/release does not invalidate).
        """
        from src import runtime as rt
        rt._acquire_tape_io_lock("group")
        try:
            for _ in range(6):        # six "chunks"
                self.assertTrue(ltfs._ensure_lto_drive_ready_unlocked("X:\\"))
            self.assertEqual(len(self.status_calls), 1)
        finally:
            rt._release_tape_io_lock()

    def test_nested_acquire_release_does_not_invalidate_readiness(self):
        from src import runtime as rt
        rt._acquire_tape_io_lock("group")
        try:
            ltfs._ensure_lto_drive_ready_unlocked("X:\\")
            snapshot = ltfs.READINESS.snapshot()
            self.assertIsNotNone(snapshot)
            # A nested acquire/release (what LTOBackup.run does per chunk)
            rt._acquire_tape_io_lock("chunk")
            rt._release_tape_io_lock()
            self.assertIsNotNone(ltfs.READINESS.snapshot(),
                                 "nested release invalidated readiness")
        finally:
            rt._release_tape_io_lock()
        self.assertIsNone(ltfs.READINESS.snapshot())

    def test_no_ltfs_access_while_the_queue_is_filled(self):
        """Filling the ready queue must touch nothing LTFS-related."""
        q = ReadyQueue(_limits())
        acquisitions_before = own.OWNERSHIP.generation
        for i in range(12):
            q.put(_item(i, gib=1.7))
        self.assertEqual(self.status_calls, [], "readiness probed while filling")
        self.assertEqual(self.label_calls, [], "cartridge read while filling")
        self.assertEqual(own.OWNERSHIP.generation, acquisitions_before,
                         "ownership acquired while filling the queue")
        self.assertFalse(own.owns_ltfs())

    def test_ownership_not_held_while_waiting_for_future_work(self):
        q = ReadyQueue(_limits())
        q.put(_item(0, gib=1.7))              # below threshold -> writer waits
        group, reason = q.wait_for_group(timeout=1.0, poll=0.1)
        self.assertEqual(group, [])
        self.assertFalse(own.owns_ltfs(),
                         "ownership held while waiting for the producer")


# =============================================================================
# Group semantics in the orchestrator (fully mocked)
# =============================================================================
class GroupWriteSemanticsTests(unittest.TestCase):
    """Drives RemoteOrchestrator._write_chunk_group with everything faked."""

    def _orchestrator(self, write_results):
        from src import remote_orchestrator as ro
        orch = ro.RemoteOrchestrator.__new__(ro.RemoteOrchestrator)
        orch.cfg = SimpleNamespace(lto_drive="X:\\", backup_log_dir=None)
        orch.db = mock.MagicMock()
        # Real dict: the fits-tape step unpacks a 3-tuple from .get(...).
        orch.db.get_chunk_size_summary.return_value = {}
        orch.notifier = None
        orch.remote_host = "host.example"
        orch.remote_session_path = "/strg"
        orch.skipped_tracker = mock.MagicMock()
        orch._consumer_chunk = None
        orch._ownership_acquisitions = 0
        orch._readiness_checks = 0
        orch._cartridge_verifications = 0
        orch._staged_lock = threading.RLock()
        orch._staged_bytes = 0
        orch._recorded_stop = None
        orch._get_recorded_stop = lambda: None
        orch._record_stop = lambda s, escalate=False: s
        orch._pre_write_safety_gate = lambda *a, **k: None
        orch._cleanup_dir = lambda *_a: None
        orch._preserve_desc = mock.MagicMock()
        orch._discard_desc = mock.MagicMock()
        orch._ensure_remote_chunk_fits_tape = lambda *a, **k: True
        orch.governor = None
        orch._backup_writer = lambda cls=None: self._writer(write_results, orch)
        return orch

    def _writer(self, results, orch):
        outer = self

        class FakeWriter:
            def eject_tape(self, *_a):
                pass

            def run(self, **kwargs):
                idx = kwargs["remote_chunk_index"]
                outer.written.append(idx)
                outcome = results.get(idx)
                if outcome == "fail_after_start":
                    kwargs["on_write_start"]()
                    raise RuntimeError("tape write failed after starting")
                if outcome == "fail_before_start":
                    raise RuntimeError("drive not ready")
                kwargs["on_write_start"]()
        return FakeWriter()

    def setUp(self):
        self.written = []
        ltfs.reset_readiness("test setup")
        self.addCleanup(ltfs.reset_readiness, "test teardown")

    def _descs(self, indices):
        return [SimpleNamespace(chunk_index=i, pack_dir=f"/tmp/_pack_{i}",
                                staged_bytes=int(1.7 * GiB), skip_tape=False,
                                metadata=[], fetch_dir=f"/tmp/_fetch_{i}",
                                source_missing_files=[])
                for i in indices]

    def test_group_writes_all_chunks_consecutively(self):
        orch = self._orchestrator({})
        stop = threading.Event()
        block = orch._write_chunk_group(37, self._descs([0, 1, 2]), "T", False,
                                        stop)
        self.assertIsNone(block)
        self.assertEqual(self.written, [0, 1, 2])
        self.assertEqual(orch._ownership_acquisitions, 1,
                         "ownership acquired more than once for one group")
        self.assertEqual(orch._readiness_checks, 1)
        self.assertEqual(orch._cartridge_verifications, 1)

    def test_ownership_released_after_the_group(self):
        orch = self._orchestrator({})
        orch._write_chunk_group(37, self._descs([0, 1]), "T", False,
                                threading.Event())
        self.assertFalse(own.owns_ltfs())

    def test_later_failure_preserves_earlier_successes(self):
        orch = self._orchestrator({2: "fail_after_start"})
        block = orch._write_chunk_group(37, self._descs([0, 1, 2, 3]), "T",
                                        False, threading.Event())
        self.assertIsNotNone(block)
        self.assertEqual(block.reason, REASON_AMBIGUOUS_BACKING_CHUNK)
        self.assertEqual(block.chunk_index, 2)
        # 0 and 1 were written and committed; 3 never started.
        self.assertEqual(self.written, [0, 1, 2])
        done = [c.args[2] for c in orch.db.update_chunk_status.call_args_list]
        self.assertGreaterEqual(done.count('done'), 2)

    def test_unstarted_chunks_are_not_written_after_a_failure(self):
        orch = self._orchestrator({1: "fail_before_start"})
        block = orch._write_chunk_group(37, self._descs([0, 1, 2, 3]), "T",
                                        False, threading.Event())
        self.assertIsNotNone(block)
        self.assertEqual(block.reason, REASON_TAPE_WRITE_FAILED)
        self.assertNotIn(2, self.written)
        self.assertNotIn(3, self.written)

    def test_ownership_released_after_a_failure(self):
        orch = self._orchestrator({0: "fail_after_start"})
        orch._write_chunk_group(37, self._descs([0, 1]), "T", False,
                                threading.Event())
        self.assertFalse(own.owns_ltfs())

    def test_cancellation_between_chunks_stops_the_group(self):
        from src.runtime import CANCEL
        orch = self._orchestrator({})
        original = orch._backup_writer

        def cancelling_writer(cls=None):
            w = original(cls)
            real_run = w.run

            def run(**kwargs):
                real_run(**kwargs)
                CANCEL.set()
            w.run = run
            return w

        orch._backup_writer = cancelling_writer
        try:
            block = orch._write_chunk_group(37, self._descs([0, 1, 2]), "T",
                                            False, threading.Event())
        finally:
            CANCEL.clear()
        self.assertEqual(self.written, [0])
        self.assertIsNotNone(block)
        self.assertEqual(block.reason, REASON_USER_REQUESTED_STOP)
        self.assertTrue(block.preserve_pack)
        self.assertFalse(own.owns_ltfs(), "ownership leaked on cancellation")

    def test_stop_pipeline_between_chunks_preserves_remaining(self):
        orch = self._orchestrator({})
        stop = threading.Event()
        original = orch._backup_writer

        def stopping_writer(cls=None):
            w = original(cls)
            real_run = w.run

            def run(**kwargs):
                real_run(**kwargs)
                stop.set()
            w.run = run
            return w

        orch._backup_writer = stopping_writer
        block = orch._write_chunk_group(37, self._descs([0, 1, 2]), "T", False,
                                        stop)
        self.assertEqual(self.written, [0])
        self.assertTrue(block.preserve_pack)

    def test_skip_tape_chunks_never_take_ownership(self):
        orch = self._orchestrator({})
        descs = self._descs([0])
        descs[0].skip_tape = True
        with mock.patch("src.remote_orchestrator._write_source_missing_only_log",
                        return_value=None):
            block = orch._write_chunk_group(37, descs, "T", False,
                                            threading.Event())
        self.assertIsNone(block)
        self.assertEqual(orch._ownership_acquisitions, 0)
        self.assertEqual(self.written, [])

    def test_empty_group_is_a_no_op(self):
        orch = self._orchestrator({})
        self.assertIsNone(orch._write_chunk_group(37, [], "T", False,
                                                  threading.Event()))
        self.assertEqual(orch._ownership_acquisitions, 0)


# =============================================================================
# Legacy session compatibility
# =============================================================================
class LegacySessionCompatibilityTests(unittest.TestCase):
    """Session 37 shape: active, scan_complete=false, mixed chunk states."""

    def test_queue_works_with_an_incomplete_scan(self):
        q = ReadyQueue(_limits())
        session = {"session_id": 37, "status": "active", "scan_complete": False}
        self.assertFalse(session["scan_complete"])
        for i in (108, 109, 110):
            q.put(_item(i, gib=1.7))
        # Below threshold and the scan is still running -> must NOT start.
        self.assertIsNone(q.start_reason())
        # A safe stop still drains what is prepared.
        q.request_drain()
        group, reason = q.wait_for_group(timeout=TIMEOUT)
        self.assertEqual([i.chunk_index for i in group], [108, 109, 110])
        self.assertEqual(reason, "explicit_drain_requested")

    def test_partial_scan_is_never_mistaken_for_a_complete_plan(self):
        q = ReadyQueue(_limits())
        q.put(_item(0, gib=1.7))
        self.assertFalse(q.closed)
        self.assertIsNone(q.start_reason())     # no "everything is ready" claim

    def test_chunk_ids_and_pack_paths_are_unchanged(self):
        it = _item(108)
        self.assertEqual(it.chunk_index, 108)
        self.assertEqual(it.pack_dir, "/tmp/_pack_108")
        self.assertEqual(it.desc.chunk_index, 108)

    def test_queue_preserves_chunk_ordering(self):
        q = ReadyQueue(_limits(chunks=8))
        for i in (110, 108, 109):
            q.put(_item(i, gib=0.01))
        q.request_drain()
        group, _ = q.wait_for_group(timeout=TIMEOUT)
        self.assertEqual([i.chunk_index for i in group], [108, 109, 110])

    def test_no_group_level_durable_state_exists(self):
        """Phase 4 introduces no batch/durable persistence."""
        for name in dir(rq):
            self.assertNotIn("durable", name.lower())
        self.assertFalse(hasattr(ReadyQueue, "persist"))
        self.assertFalse(hasattr(ReadyItem, "batch_id"))


# =============================================================================
# Metrics
# =============================================================================
class MetricsTests(unittest.TestCase):
    def test_metrics_expose_the_required_fields(self):
        q = ReadyQueue(_limits())
        q.put(_item(0))
        m = q.metrics()
        for field in ("ready_chunks", "ready_bytes", "ready_target_bytes",
                      "ready_max_bytes", "writing_chunks", "writing_bytes",
                      "producer_blocked_seconds", "writer_waiting_seconds",
                      "chunks_written", "bytes_written", "groups_started"):
            self.assertIn(field, m)

    def test_group_counter_increments_once_per_group(self):
        q = ReadyQueue(_limits())
        for i in range(12):
            q.put(_item(i, gib=1.7))
        q.wait_for_group(timeout=TIMEOUT)
        self.assertEqual(q.metrics()["groups_started"], 1)

    def test_orchestrator_tracks_ownership_and_readiness_counts(self):
        from src import remote_orchestrator as ro
        import inspect
        body = inspect.getsource(ro)
        for counter in ("_ownership_acquisitions", "_readiness_checks",
                        "_cartridge_verifications"):
            self.assertIn(counter, body)


if __name__ == "__main__":
    unittest.main()
