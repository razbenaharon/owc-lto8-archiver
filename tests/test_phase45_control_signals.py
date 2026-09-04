"""Phase 4.5: wiring the ready-queue control signals into production.

Covers the five deliverables that Phase 4 left as plumbing:

* A. staging-pressure draining (hysteresis, local staging only, no LTFS);
* B. producer-completion semantics (normal completion vs failure vs stop, and a
     temporary pause that must NOT start a tiny group);
* C. validating the ready-queue byte ceiling against the staging budget;
* E. an orchestrator-level group-boundary proof (one ownership period, one
     readiness verification, one cartridge read, N writes, nothing between);
* F. the ownership startup preflight preventing worker-thread startup.

Every test is offline and bounded: LTFS is a fake adapter or a counting command,
the writer is a recording double, and nothing touches Z:\\, an IBM helper or a
live database.
"""
import threading
import time
import unittest
from types import SimpleNamespace
from unittest import mock

from src import ltfs
from src import ltfs_ownership as own
from src import remote_orchestrator as ro
from src.config import ConfigManager
from src.exit_codes import ExitCode, REASON_LTFS_OWNERSHIP_UNAVAILABLE
from src.ltfs_ownership import LtfsOwnershipError, FAILURE_CROSS_SESSION_UNAVAILABLE
from src.pipeline_types import ContainerFormat
from src.ready_queue import ReadyItem, ReadyQueue, ReadyQueueLimits

GiB = 1024 ** 3
TIMEOUT = 20


def _limits(min_b=20 * GiB, target=40 * GiB, max_b=80 * GiB, chunks=48):
    return ReadyQueueLimits(min_b, target, max_b, chunks)


def _item(index, gib=1.7):
    return ReadyItem(chunk_index=index, pack_dir=f"/tmp/_pack_{index}",
                     prepared_bytes=int(gib * GiB), file_count=200000,
                     desc=SimpleNamespace(chunk_index=index,
                                          pack_dir=f"/tmp/_pack_{index}",
                                          staged_bytes=int(gib * GiB),
                                          skip_tape=False))


def _bare_orchestrator(staging_max_gb=100):
    """A RemoteOrchestrator with only the fields the control-signal methods need,
    built via __new__ so no config/DB/thread machinery runs."""
    orch = ro.RemoteOrchestrator.__new__(ro.RemoteOrchestrator)
    orch.staging_max_bytes = int(staging_max_gb * GiB)
    orch._staged_bytes = 0
    orch._staged_lock = threading.Lock()
    orch._staging_pressure_active = False
    orch._producer_err = None
    return orch


# =============================================================================
# A. Staging-pressure draining
# =============================================================================
class StagingPressureDecisionTests(unittest.TestCase):
    """The pure hysteretic decision, isolated from the blocking capacity loop."""

    def setUp(self):
        self.orch = _bare_orchestrator(staging_max_gb=100)
        self.need = 20 * GiB

    def test_engages_when_the_next_chunk_will_not_fit(self):
        # 85 + 20 = 105 GiB > 100 GiB cap -> engage.
        self.assertIs(self.orch._staging_pressure_decision(85 * GiB, self.need),
                      True)

    def test_clears_only_with_room_for_two_footprints(self):
        # 50 + 2*20 = 90 GiB <= 100 GiB -> comfortable, clear.
        self.assertIs(self.orch._staging_pressure_decision(50 * GiB, self.need),
                      False)

    def test_holds_inside_the_hysteresis_band(self):
        # 70 + 20 = 90 <= 100 (fits) but 70 + 40 = 110 > 100 (not comfortable).
        self.assertIsNone(
            self.orch._staging_pressure_decision(70 * GiB, self.need))

    def test_nothing_staged_is_never_pressure(self):
        self.assertIs(self.orch._staging_pressure_decision(0, self.need), False)


class StagingPressureWiringTests(unittest.TestCase):
    """A.1-A.5: the orchestrator toggles the queue signal from local staging."""

    def setUp(self):
        self.orch = _bare_orchestrator(staging_max_gb=100)
        self.need = 20 * GiB
        self.q = ReadyQueue(_limits())
        self.toggles = []
        real = self.q.set_staging_pressure

        def counting(value=True):
            self.toggles.append(bool(value))
            return real(value)
        self.q.set_staging_pressure = counting

    def test_pressure_call_reaches_the_queue_signal(self):
        """A.1: the production capacity method calls set_staging_pressure."""
        self.orch._apply_staging_pressure(self.q, 90 * GiB, self.need)
        self.assertTrue(self.orch._staging_pressure_active)
        self.assertEqual(self.toggles, [True])

    def test_partial_group_drains_under_pressure(self):
        """A.2: a sub-minimum group drains once pressure is engaged."""
        self.q.put(_item(0, gib=1.7))            # 1.7 GiB, far below 20 GiB min
        self.assertIsNone(self.q.start_reason())
        self.orch._apply_staging_pressure(self.q, 90 * GiB, self.need)
        group, reason = self.q.wait_for_group(timeout=TIMEOUT)
        self.assertEqual(reason, "staging_pressure_drain")
        self.assertEqual([i.chunk_index for i in group], [0])

    def test_pressure_clears_only_after_capacity_recovers(self):
        """A.3: engaged, held through the band, cleared only when comfortable."""
        self.orch._apply_staging_pressure(self.q, 90 * GiB, self.need)   # engage
        self.assertTrue(self.orch._staging_pressure_active)
        self.orch._apply_staging_pressure(self.q, 70 * GiB, self.need)   # band
        self.assertTrue(self.orch._staging_pressure_active, "cleared too early")
        self.orch._apply_staging_pressure(self.q, 40 * GiB, self.need)   # recover
        self.assertFalse(self.orch._staging_pressure_active)
        self.assertEqual(self.toggles, [True, False])

    def test_oscillation_does_not_produce_repeated_toggles(self):
        """A.4: bouncing around the boundary must not restart the signal."""
        for resident in (90, 71, 88, 72, 95, 70):     # all engage-or-hold
            self.orch._apply_staging_pressure(self.q, resident * GiB, self.need)
        # Engaged exactly once; never cleared while pressure persists.
        self.assertEqual(self.toggles, [True])

    def test_calculating_pressure_touches_no_ltfs(self):
        """A.5: no readiness probe, cartridge read or ownership acquisition."""
        ltfs.reset_readiness("A.5 setup")
        self.addCleanup(ltfs.reset_readiness, "A.5 teardown")
        status_calls = []

        class CountingCommand(ltfs.LtfsDriveCommand):
            def drive_status(self, drive_path):
                status_calls.append(drive_path)
                return "LTFS_MOUNTED", "", None

        previous = ltfs.set_ltfs_drive_command(CountingCommand())
        self.addCleanup(ltfs.set_ltfs_drive_command, previous)
        gen_before = own.OWNERSHIP.generation
        for resident in (90, 40, 90, 40):
            self.orch._apply_staging_pressure(self.q, resident * GiB, self.need)
        self.assertEqual(status_calls, [])
        self.assertEqual(own.OWNERSHIP.generation, gen_before)
        self.assertFalse(own.owns_ltfs())

    def test_await_capacity_engages_pressure_when_blocked(self):
        """A.1 integration: the real capacity gate signals while it waits."""
        import tempfile
        orch = _bare_orchestrator(staging_max_gb=100)
        orch.staging_dir = tempfile.mkdtemp()
        orch._physical_estimate = lambda b, f: 20 * GiB    # need = 40 GiB
        orch._staged_bytes = 90 * GiB                       # 90 + 40 > 100
        orch.governor = None
        stop = threading.Event()
        q = ReadyQueue(_limits())

        # Keep the integration check independent of the host/CI runner's free
        # disk. Without this patch, a runner below the production 20 GiB reserve
        # raises inside the daemon thread and pytest can report the test as
        # passing with only PytestUnhandledThreadExceptionWarning.
        disk = SimpleNamespace(free=100 * GiB)
        with mock.patch("src.remote_staging.shutil.disk_usage", return_value=disk):
            t = threading.Thread(
                target=orch._await_staging_capacity,
                args=(1, 1, stop), kwargs={"ready_q": q}, daemon=True)
            t.start()
            try:
                deadline = time.time() + 5
                while time.time() < deadline and not orch._staging_pressure_active:
                    time.sleep(0.05)
                self.assertTrue(orch._staging_pressure_active,
                                "capacity gate did not engage staging pressure")
            finally:
                stop.set()
                t.join(timeout=TIMEOUT)
        self.assertFalse(t.is_alive(), "capacity-gate worker did not stop")


class DeadPressureConfigRemovedTests(unittest.TestCase):
    """Phase 5B: the unused staging_pressure_*_ratio knobs were removed; the
    single authoritative model is need-based."""

    def test_config_has_no_pressure_ratio_properties(self):
        cfg = ConfigManager()
        self.assertFalse(hasattr(cfg, "staging_pressure_high_ratio"))
        self.assertFalse(hasattr(cfg, "staging_pressure_low_ratio"))

    def test_orchestrator_has_no_pressure_ratio_fields(self):
        import inspect
        src = inspect.getsource(ro)
        self.assertNotIn("staging_pressure_high_ratio", src)
        self.assertNotIn("staging_pressure_low_ratio", src)

    def test_example_config_documents_no_ratio_keys(self):
        import os
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        for name in ("config.example.ini", "config.ini"):
            path = os.path.join(root, name)
            if not os.path.exists(path):
                continue
            with open(path, encoding="utf-8") as fh:
                body = fh.read()
            self.assertNotIn("staging_pressure_high_ratio", body)
            self.assertNotIn("staging_pressure_low_ratio", body)

    def test_need_based_decision_is_the_only_model(self):
        # The decision depends only on resident/need/staging_max — no ratios.
        orch = _bare_orchestrator(staging_max_gb=100)
        self.assertIs(orch._staging_pressure_decision(85 * GiB, 20 * GiB), True)
        self.assertIs(orch._staging_pressure_decision(40 * GiB, 20 * GiB), False)
        self.assertIsNone(orch._staging_pressure_decision(70 * GiB, 20 * GiB))


# =============================================================================
# B. Producer-completion semantics
# =============================================================================
class ProducerCompletionTests(unittest.TestCase):
    def setUp(self):
        self.orch = _bare_orchestrator()

    def test_temporary_emptiness_does_not_start_a_group(self):
        """B.1: below the minimum, open queue, no signals -> keep waiting."""
        q = ReadyQueue(_limits())
        q.put(_item(0, gib=1.7))
        self.assertFalse(q.closed)
        self.assertIsNone(q.start_reason())

    def test_normal_completion_drains_a_final_partial_group(self):
        """B.2: scan finished, all staged, no error -> final partial drains."""
        q = ReadyQueue(_limits())
        q.put(_item(0, gib=1.7))
        self.orch._producer_err = None
        self.orch._signal_producer_completion(q, threading.Event())
        self.assertTrue(q.closed)
        self.assertEqual(q.start_reason(), "producer_finished_final_group")

    def test_incomplete_scan_is_supported(self):
        """B.3: scan_complete=false keeps the queue open (no completion claim)."""
        q = ReadyQueue(_limits())
        session = {"session_id": 37, "scan_complete": False}
        self.assertFalse(session["scan_complete"])
        for i in (108, 109):
            q.put(_item(i, gib=1.7))
        self.assertFalse(q.closed)
        self.assertIsNone(q.start_reason())

    def test_producer_failure_preserves_packs_and_stops_unambiguously(self):
        """B.4: a terminal failure never forces a final write; packs survive."""
        q = ReadyQueue(_limits())
        for i in (0, 1):
            q.put(_item(i, gib=1.7))
        stop = threading.Event()
        stop.set()                                   # failure already set the stop
        self.orch._producer_err = "chunk 3 could not be staged"
        self.orch._signal_producer_completion(q, stop)
        # Input marked exhausted AND closed, but the consumer breaks on the stop.
        group, reason = q.wait_for_group(stop_event=stop, timeout=TIMEOUT)
        self.assertEqual(group, [])
        self.assertEqual(reason, "stop_requested")
        preserved = q.drain_ready()
        self.assertEqual([i.chunk_index for i in preserved], [0, 1])

    def test_safe_stop_preserves_queued_packs(self):
        """B.5: a safe stop (no producer error) preserves rather than drains."""
        q = ReadyQueue(_limits())
        q.put(_item(0, gib=1.7))
        stop = threading.Event()
        stop.set()
        self.orch._producer_err = None
        self.orch._signal_producer_completion(q, stop)
        group, reason = q.wait_for_group(stop_event=stop, timeout=TIMEOUT)
        self.assertEqual(group, [])
        self.assertEqual(reason, "stop_requested")
        self.assertEqual([i.chunk_index for i in q.drain_ready()], [0])


# =============================================================================
# C. Ready-queue limits validated against staging capacity
# =============================================================================
class _CfgStub:
    _READY_QUEUE_DEFAULTS = ConfigManager._READY_QUEUE_DEFAULTS

    def __init__(self, limits, staging_gb, reserve):
        self._limits = limits
        self._staging = staging_gb
        self._reserve = reserve

    @property
    def ready_queue_limits(self):
        return self._limits

    @property
    def staging_max_gb(self):
        return self._staging

    @property
    def ready_queue_staging_reserve_bytes(self):
        return self._reserve


def _validate(limits, staging_gb, reserve):
    return ConfigManager.validated_ready_queue_limits(
        _CfgStub(limits, staging_gb, reserve))


class QueueVsStagingValidationTests(unittest.TestCase):
    def test_valid_configuration_is_accepted(self):
        limits, reserve, eff, source = _validate(
            _limits(max_b=80 * GiB), staging_gb=700, reserve=520 * GiB)
        self.assertEqual(source, "configured")
        self.assertLessEqual(limits.max_bytes + reserve, eff)

    def test_boundary_equality_is_accepted(self):
        # max_ready + reserve == effective staging exactly (<=, not <).
        limits, reserve, eff, source = _validate(
            _limits(max_b=80 * GiB), staging_gb=100, reserve=20 * GiB)
        self.assertEqual(source, "configured")
        self.assertEqual(limits.max_bytes + reserve, eff)

    def test_conflict_falls_back_atomically_to_defaults(self):
        # A huge max_ready cannot fit; the WHOLE set reverts to the defaults,
        # never a single clamped value.
        limits, reserve, eff, source = _validate(
            _limits(min_b=20 * GiB, target=300 * GiB, max_b=600 * GiB),
            staging_gb=700, reserve=200 * GiB)
        self.assertEqual(source, "fallback_default")
        d = ConfigManager._READY_QUEUE_DEFAULTS
        self.assertEqual(limits.max_bytes, d['max_ready_bytes'])
        self.assertEqual(limits.min_start_bytes,
                         d['min_ready_bytes_before_writer_start'])
        self.assertLessEqual(limits.max_bytes + reserve, eff)

    def test_impossible_configuration_fails_closed(self):
        # Staging so small that even the default ceiling + reserve cannot fit.
        with self.assertRaises(ValueError):
            _validate(_limits(max_b=600 * GiB), staging_gb=50, reserve=40 * GiB)

    def test_malformed_reserve_uses_auto_default(self):
        cfg = ConfigManager()
        with mock.patch.object(
                type(cfg), 'chunk_cap_gb',
                new_callable=mock.PropertyMock, return_value=250):
            # 0 in config means auto = 2*chunk_cap + LOCAL_STAGING_RESERVE.
            self.assertGreater(cfg.ready_queue_staging_reserve_bytes, 0)

    def test_config_defaults_validate_against_real_staging(self):
        cfg = ConfigManager()
        limits, reserve, eff, source = cfg.validated_ready_queue_limits()
        self.assertTrue(limits.validate())
        self.assertLessEqual(limits.max_bytes + reserve, eff)


# =============================================================================
# E. Orchestrator-level group-boundary proof
# =============================================================================
class GroupBoundaryProofTests(unittest.TestCase):
    """One ownership period, one readiness verification, one cartridge read,
    N writes, and zero device work between chunks or while waiting."""

    def setUp(self):
        ltfs.reset_readiness("E setup")
        self.addCleanup(ltfs.reset_readiness, "E teardown")
        # Distinct, independently-counted metrics (the handoff's list):
        self.drive_status_calls = []     # actual fake LtfsCmdDrives adapter calls
        self.label_reads = []            # actual cartridge-label reads
        self.writer_invocations = []     # writer.run() calls
        self.timeline = []               # ordered device events, to prove "none
                                         # between chunks"

        outer = self

        class CountingCommand(ltfs.LtfsDriveCommand):
            def drive_status(self, drive_path):
                outer.drive_status_calls.append(drive_path)
                outer.timeline.append(("drive_status", drive_path))
                return "LTFS_MOUNTED", "", None

        previous = ltfs.set_ltfs_drive_command(CountingCommand())
        self.addCleanup(ltfs.set_ltfs_drive_command, previous)

    def _orchestrator(self):
        orch = ro.RemoteOrchestrator.__new__(ro.RemoteOrchestrator)
        orch.cfg = SimpleNamespace(lto_drive="X:\\", backup_log_dir=None)
        orch.db = mock.MagicMock()
        orch.db.get_chunk_size_summary.return_value = {}
        orch.db.get_chunk_packaging_format.return_value = ContainerFormat.ZIP
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
        orch._get_recorded_stop = lambda: None
        orch._record_stop = lambda s, escalate=False: s
        orch._cleanup_dir = lambda *_a: None
        orch._preserve_desc = mock.MagicMock()
        orch._discard_desc = mock.MagicMock()
        orch._ensure_remote_chunk_fits_tape = lambda *a, **k: True
        orch.governor = None

        outer = self

        # A gate that does the REAL single readiness verification (through the
        # injectable adapter) plus a single cartridge read for the group.
        def gate(session_id, desc, tape_label, stop_pipeline):
            ltfs._ensure_lto_drive_ready_unlocked("X:\\")
            outer.label_reads.append(tape_label)
            outer.timeline.append(("label_read", tape_label))
            return None
        orch._pre_write_safety_gate = gate

        class NestingWriter:
            """Simulates LTOBackup.run: it nests the tape I/O lock (a recursive
            ownership entry) around its write, exactly as the real writer does."""

            def eject_tape(self, *_a):
                pass

            def run(self, **kwargs):
                from src import runtime as rt
                idx = kwargs["remote_chunk_index"]
                rt._acquire_tape_io_lock(f"write chunk {idx}")   # nested
                try:
                    outer.writer_invocations.append(idx)
                    outer.timeline.append(("write", idx))
                    kwargs["on_write_start"]()
                finally:
                    rt._release_tape_io_lock(f"write chunk {idx}")
        orch._backup_writer = lambda cls=None: NestingWriter()
        return orch

    def _descs(self, indices):
        return [SimpleNamespace(chunk_index=i, pack_dir=f"/tmp/_pack_{i}",
                                staged_bytes=int(1.7 * GiB), skip_tape=False,
                                session_id=37,
                                packaging_format=ContainerFormat.ZIP,
                                metadata=[], fetch_dir=f"/tmp/_fetch_{i}",
                                source_missing_files=[])
                for i in indices]

    def test_group_boundary_metrics_are_one_per_group(self):
        orch = self._orchestrator()
        gen_before = own.OWNERSHIP.generation
        n = 5
        block = orch._write_chunk_group(37, self._descs(range(n)), "Tape_03",
                                        False, threading.Event())
        self.assertIsNone(block)

        # N writer invocations.
        self.assertEqual(self.writer_invocations, list(range(n)))
        # Exactly ONE actual fake-LtfsCmdDrives adapter call for the group.
        self.assertEqual(len(self.drive_status_calls), 1)
        # Exactly ONE cartridge-label read.
        self.assertEqual(len(self.label_reads), 1)
        # Exactly ONE physical kernel-mutex acquisition (generation delta), even
        # though the writer nested a recursive ownership entry per chunk.
        self.assertEqual(own.OWNERSHIP.generation - gen_before, 1)
        # The orchestrator's own Python counters agree: 1 per group, not per N.
        self.assertEqual(orch._ownership_acquisitions, 1)
        self.assertEqual(orch._readiness_checks, 1)
        self.assertEqual(orch._cartridge_verifications, 1)
        # Ownership released at the end of the group.
        self.assertFalse(own.owns_ltfs())

    def test_no_device_work_between_chunks(self):
        orch = self._orchestrator()
        orch._write_chunk_group(37, self._descs(range(4)), "Tape_03", False,
                                threading.Event())
        # The gate (drive_status + label) fires ONCE up front; then only writes.
        kinds = [k for k, _ in self.timeline]
        self.assertEqual(kinds[0], "drive_status")
        self.assertEqual(kinds[1], "label_read")
        self.assertEqual(kinds[2:], ["write"] * 4)
        # No drive_status or label read is interleaved between the writes.
        self.assertNotIn("drive_status", kinds[2:])
        self.assertNotIn("label_read", kinds[2:])

    def test_no_ltfs_while_waiting_for_the_next_group(self):
        """Between groups the writer holds no ownership and issues no device op."""
        q = ReadyQueue(_limits())
        q.put(_item(0, gib=1.7))          # below the minimum -> writer waits
        drive_status_before = len(self.drive_status_calls)
        group, reason = q.wait_for_group(timeout=1.0, poll=0.1)
        self.assertEqual(group, [])
        self.assertFalse(own.owns_ltfs())
        self.assertEqual(len(self.drive_status_calls), drive_status_before)


# =============================================================================
# F. Ownership startup preflight prevents worker startup
# =============================================================================
class OwnershipPreflightStartupTests(unittest.TestCase):
    def _orchestrator(self):
        orch = ro.RemoteOrchestrator.__new__(ro.RemoteOrchestrator)
        orch.db = mock.MagicMock()
        orch.cfg = SimpleNamespace(lto_drive="X:\\")
        orch._finalize = lambda result, phase="pipeline": result
        return orch

    def test_streaming_path_starts_no_threads_when_preflight_fails(self):
        orch = self._orchestrator()
        boom = LtfsOwnershipError(
            "Global mutex unavailable",
            kind=FAILURE_CROSS_SESSION_UNAVAILABLE)
        with mock.patch.object(ro.OWNERSHIP, "assert_production_scope",
                               side_effect=boom), \
                mock.patch.object(ro, "threading") as fake_threading:
            result = orch._run_streaming_session(37)
        # Zero worker threads constructed, let alone started.
        self.assertEqual(fake_threading.Thread.call_count, 0)
        self.assertEqual(result.exit_code, ExitCode.SAFETY_BLOCK)
        self.assertEqual(result.reason, REASON_LTFS_OWNERSHIP_UNAVAILABLE)
        # The failure is raised before any DB or device work.
        orch.db.get_remote_session.assert_not_called()

    def test_resume_path_starts_no_threads_when_preflight_fails(self):
        orch = self._orchestrator()
        orch.db.get_remote_session.return_value = {
            "tape_label": "Tape_03", "scan_complete": True}
        boom = LtfsOwnershipError(
            "Global mutex unavailable",
            kind=FAILURE_CROSS_SESSION_UNAVAILABLE)
        with mock.patch.object(ro.OWNERSHIP, "assert_production_scope",
                               side_effect=boom), \
                mock.patch.object(ro, "threading") as fake_threading:
            result = orch._run_session(37)
        self.assertEqual(fake_threading.Thread.call_count, 0)
        self.assertEqual(result.reason, REASON_LTFS_OWNERSHIP_UNAVAILABLE)
        # Preflight runs before pending-chunk / capacity work.
        orch.db.get_pending_chunks.assert_not_called()

    def test_preflight_passes_through_when_ownership_is_available(self):
        orch = self._orchestrator()
        with mock.patch.object(ro.OWNERSHIP, "assert_production_scope",
                               return_value=True):
            self.assertIsNone(
                orch._assert_ownership_preflight(37, "startup"))


if __name__ == "__main__":
    unittest.main()
