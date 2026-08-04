"""Phase 5D: the bounded, non-authoritative async observation subsystem.

Offline and deterministic. Proves the writer-side is a sub-millisecond
snapshot-copy + non-blocking enqueue, that a full queue / observer exception /
DB timeout / log failure are all non-fatal, that shutdown is bounded, and that
observation touches no forbidden resource. Read-only PostgreSQL behaviour is
proven in the gated PG integration file.
"""
import dataclasses
import json
import os
import shutil
import tempfile
import time
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest import mock

from src.sealed_batch_observation import (
    ObservationConfig, ObservationWorker, WriteGroupSnapshot, WriteGroupOutcome,
    _build_record_logger, _validate_log_path, ObservationLogPathError,
    maybe_build_observation_worker, DB_OBSERVATION_UNAVAILABLE,
    MISSING_CHUNK_ROW, STATUS_CHANGED_DURING_WINDOW, STALE_OBSERVATION)
from src.sealed_batch_observer import SealedBatchObserver

GiB = 1024 ** 3


def _snap(n=12, cid="cid", session=37, ts=None):
    ts = ts or datetime.now(timezone.utc).isoformat()
    ids = tuple(200 + i for i in range(n))
    return WriteGroupSnapshot(
        correlation_id=cid, session_id=session, chunk_ids=ids,
        prepared_bytes=tuple(int(1.7 * GiB) for _ in ids),
        total_prepared_bytes=sum(int(1.7 * GiB) for _ in ids),
        pack_identities=tuple(f"_pack_{i}" for i in ids), expected_tape="Tape_03",
        selection_reason="min_ready_bytes_reached", ready_queue_generation=1,
        scan_complete=False, producer_state="active", staging_pressure=False,
        safe_stop=False, snapshot_ts_utc=ts, writer_path="streaming")


class _FakeReader:
    def __init__(self, statuses=None, fail=False, secs=0.001):
        self.statuses, self.fail, self.secs, self.calls = statuses, fail, secs, 0

    def read(self, session_id, chunk_ids):
        self.calls += 1
        if self.fail:
            return None, self.secs, None, "boom"
        st = self.statuses if self.statuses is not None else {
            ci: "packing" for ci in chunk_ids}
        return st, self.secs, "read committed", None


class _ObsTest(unittest.TestCase):
    """Base: a temp dir with LIFO cleanup so the log handle is closed BEFORE the
    directory is removed (Windows cannot delete an open file)."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)

    def make_worker(self, reader=None, queue_max=100, log_max_bytes=10 * 1024 * 1024,
                    log_backups=5):
        cfg = ObservationConfig(
            enabled=True, queue_max=queue_max, shutdown_timeout_seconds=2.0,
            log_path=os.path.join(self.tmp, "obs.jsonl"),
            log_max_bytes=log_max_bytes, log_backups=log_backups)
        logger = _build_record_logger(cfg.log_path, cfg.log_max_bytes,
                                      cfg.log_backups)
        w = ObservationWorker(
            cfg, SealedBatchObserver(),
            reader if reader is not None else _FakeReader(), logger)
        self.addCleanup(w.close)          # LIFO: runs before rmtree
        return w, cfg

    def read_last(self, cfg):
        return json.loads(open(cfg.log_path, encoding="utf-8")
                          .read().splitlines()[-1])


# ===========================================================================
# Immutable snapshot
# ===========================================================================
class ImmutableSnapshotTests(unittest.TestCase):
    def test_snapshot_is_frozen(self):
        s = _snap()
        with self.assertRaises(dataclasses.FrozenInstanceError):
            s.session_id = 99
        with self.assertRaises(dataclasses.FrozenInstanceError):
            s.chunk_ids = (1,)

    def test_snapshot_fields_are_immutable_types(self):
        s = _snap()
        self.assertIsInstance(s.chunk_ids, tuple)
        self.assertIsInstance(s.prepared_bytes, tuple)
        self.assertIsInstance(s.pack_identities, tuple)


# ===========================================================================
# Log-path guard
# ===========================================================================
class LogPathGuardTests(unittest.TestCase):
    def _cfg(self):
        return SimpleNamespace(staging_dir=r"C:\staging", lto_drive=r"Z:\\",
                               backup_log_dir=None)

    def test_rejects_repo_path(self):
        from src.constants import PROJECT_ROOT
        with self.assertRaises(ObservationLogPathError):
            _validate_log_path(os.path.join(PROJECT_ROOT, "obs.jsonl"),
                               self._cfg())

    def test_rejects_staging_path(self):
        with self.assertRaises(ObservationLogPathError):
            _validate_log_path(r"C:\staging\packs\obs.jsonl", self._cfg())

    def test_rejects_ltfs_path(self):
        with self.assertRaises(ObservationLogPathError):
            _validate_log_path(r"Z:\obs.jsonl", self._cfg())

    def test_rejects_evidence_path(self):
        with self.assertRaises(ObservationLogPathError):
            _validate_log_path(r"C:\lto8-evidence\obs.jsonl", self._cfg())

    def test_accepts_neutral_path(self):
        p = os.path.join(tempfile.gettempdir(), "lto8_obs_test", "obs.jsonl")
        self.assertTrue(_validate_log_path(p, self._cfg()))


# ===========================================================================
# Worker: records + correlation + consistency-window codes
# ===========================================================================
class WorkerRecordTests(_ObsTest):
    def test_snapshot_produces_observation_record(self):
        w, cfg = self.make_worker()
        w._process_snapshot(_snap(3, cid="abc"))
        rec = self.read_last(cfg)
        self.assertEqual(rec["record_kind"], "observation")
        self.assertEqual(rec["group_correlation_id"], "abc")
        self.assertEqual(rec["ordered_chunk_ids"], [200, 201, 202])
        self.assertEqual(rec["transaction_isolation"], "read committed")
        self.assertIn("consistency_window_seconds", rec)

    def test_outcome_record_is_correlated(self):
        w, cfg = self.make_worker()
        w._process_outcome(WriteGroupOutcome(
            correlation_id="abc", session_id=37, selected_chunk_ids=(200,),
            started_chunk_ids=(200,), completed_chunk_ids=(200,),
            failing_chunk=None, stop_reason=None, group_start_ts=1.0,
            group_finish_ts=2.5, ownership_acquisitions=1, readiness_checks=1,
            cartridge_verifications=1, writer_invocations=1))
        rec = self.read_last(cfg)
        self.assertEqual(rec["record_kind"], "outcome")
        self.assertEqual(rec["group_correlation_id"], "abc")
        self.assertEqual(rec["ownership_acquisitions"], 1)
        self.assertEqual(rec["group_duration_seconds"], 1.5)

    def test_db_unavailable_classification(self):
        w, cfg = self.make_worker(reader=_FakeReader(fail=True))
        w._process_snapshot(_snap(2))
        self.assertIn(DB_OBSERVATION_UNAVAILABLE,
                      self.read_last(cfg)["consistency_window_codes"])

    def test_missing_and_changed_classifications(self):
        w, cfg = self.make_worker(reader=_FakeReader(statuses={201: "done"}))
        w._process_snapshot(_snap(2))       # 200 missing, 201 done
        codes = self.read_last(cfg)["consistency_window_codes"]
        self.assertIn(MISSING_CHUNK_ROW, codes)
        self.assertIn(STATUS_CHANGED_DURING_WINDOW, codes)

    def test_stale_observation_classification(self):
        w, cfg = self.make_worker()
        w._stale = 0.0
        old = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        w._process_snapshot(_snap(1, ts=old))
        self.assertIn(STALE_OBSERVATION,
                      self.read_last(cfg)["consistency_window_codes"])


# ===========================================================================
# Bounded queue + non-fatal failures + bounded shutdown
# ===========================================================================
class BoundedAndNonFatalTests(_ObsTest):
    def test_full_queue_drops_and_is_non_blocking(self):
        w, _ = self.make_worker(queue_max=2)      # not started -> nothing drains
        t0 = time.perf_counter()
        self.assertTrue(w.submit_snapshot(_snap(1, cid="a")))
        self.assertTrue(w.submit_snapshot(_snap(1, cid="b")))
        self.assertFalse(w.submit_snapshot(_snap(1, cid="c")))    # dropped
        self.assertLess(time.perf_counter() - t0, 0.05)
        self.assertEqual(w.dropped, 1)

    def test_observer_exception_is_non_fatal(self):
        w, _ = self.make_worker()
        with mock.patch.object(w._observer, "observe_group",
                               side_effect=RuntimeError("boom")):
            w.start()
            w.submit_snapshot(_snap(1))
            w.shutdown(timeout=2)
        self.assertGreaterEqual(w.failed, 1)

    def test_log_failure_is_non_fatal(self):
        w, _ = self.make_worker()
        with mock.patch.object(w._log, "info", side_effect=OSError("disk full")):
            w._process_snapshot(_snap(1))         # must not raise

    def test_db_timeout_is_non_fatal(self):
        w, _ = self.make_worker(reader=_FakeReader(fail=True))
        w.start()
        w.submit_snapshot(_snap(1))
        w.shutdown(timeout=2)
        self.assertGreaterEqual(w.completed, 1)

    def test_shutdown_is_bounded(self):
        w, _ = self.make_worker()
        w.start()
        t0 = time.perf_counter()
        w.shutdown(timeout=1.0)
        self.assertLess(time.perf_counter() - t0, 3.0)

    def test_shutdown_without_start_is_safe(self):
        w, _ = self.make_worker()
        w.shutdown()          # no thread: must not raise or block


# ===========================================================================
# Log rotation
# ===========================================================================
class LogRotationTests(_ObsTest):
    def test_log_rotates_and_is_bounded(self):
        w, cfg = self.make_worker(log_max_bytes=2000, log_backups=3)
        for i in range(200):
            w._process_snapshot(_snap(3, cid=f"c{i}"))
        files = [f for f in os.listdir(self.tmp) if f.startswith("obs.jsonl")]
        self.assertGreaterEqual(len(files), 2)
        self.assertLessEqual(len(files), 1 + cfg.log_backups)


# ===========================================================================
# Zero influence (tripwires)
# ===========================================================================
class ZeroInfluenceTests(_ObsTest):
    def test_writer_side_opens_no_db(self):
        w, _ = self.make_worker(reader=_FakeReader())
        snap = _snap(48)
        with mock.patch("psycopg.connect",
                        side_effect=AssertionError("writer opened DB")):
            for _ in range(50):
                w.submit_snapshot(snap)           # enqueue only; no drain

    def test_observation_touches_no_ltfs_or_ownership(self):
        from src import ltfs
        from src import ltfs_ownership as own
        ltfs.reset_readiness("5d")
        self.addCleanup(ltfs.reset_readiness, "5d done")
        calls = []

        class Counting(ltfs.LtfsDriveCommand):
            def drive_status(self, d):
                calls.append(d)
                return "LTFS_MOUNTED", "", None
        prev = ltfs.set_ltfs_drive_command(Counting())
        self.addCleanup(ltfs.set_ltfs_drive_command, prev)
        gen = own.OWNERSHIP.generation
        w, _ = self.make_worker()
        for _ in range(30):
            w._process_snapshot(_snap(6))
        self.assertEqual(calls, [])
        self.assertEqual(own.OWNERSHIP.generation, gen)

    def test_disabled_builds_nothing(self):
        cfg = SimpleNamespace(
            sealed_tape_write_batches_observation_enabled=False)
        self.assertIsNone(maybe_build_observation_worker(cfg, "dsn"))


# ===========================================================================
# Orchestrator boundary capture (pure, no side effects)
# ===========================================================================
class BoundaryCaptureTests(unittest.TestCase):
    def _orch(self):
        from src import remote_orchestrator as ro
        o = ro.RemoteOrchestrator.__new__(ro.RemoteOrchestrator)
        o._staging_pressure_active = False
        o._producer_err = None
        o._ownership_acquisitions = 5
        o._readiness_checks = 5
        o._cartridge_verifications = 5
        return o

    def _descs(self, ids):
        return [SimpleNamespace(chunk_index=i, staged_bytes=int(1.7 * GiB),
                                pack_dir=f"/staging/_pack_s0037_{i:03d}")
                for i in ids]

    def test_snapshot_capture_is_pure_and_correct(self):
        o = self._orch()
        with mock.patch("psycopg.connect",
                        side_effect=AssertionError("capture opened DB")):
            snap = o._capture_write_group_snapshot(
                "cid", 37, self._descs([200, 201, 202]),
                "min_ready_bytes_reached", "Tape_03", 3, False, "active", False,
                "streaming")
        self.assertEqual(snap.chunk_ids, (200, 201, 202))
        self.assertEqual(snap.pack_identities,
                         ("_pack_s0037_200", "_pack_s0037_201",
                          "_pack_s0037_202"))
        self.assertEqual(snap.total_prepared_bytes, 3 * int(1.7 * GiB))

    def test_outcome_capture_deltas(self):
        o = self._orch()
        o._ownership_acquisitions = 6
        o._readiness_checks = 6
        o._cartridge_verifications = 6
        out = o._capture_group_outcome(
            "cid", 37, self._descs([200, 201, 202]), None, (5, 5, 5),
            100.0, 130.0)
        self.assertEqual(out.completed_chunk_ids, (200, 201, 202))
        self.assertEqual(out.ownership_acquisitions, 1)
        self.assertEqual(out.writer_invocations, 3)

    def test_outcome_capture_partial_failure(self):
        o = self._orch()
        block = SimpleNamespace(chunk_index=201, reason="tape_write_failed")
        out = o._capture_group_outcome(
            "cid", 37, self._descs([200, 201, 202]), block, (5, 5, 5),
            100.0, 110.0)
        self.assertEqual(out.completed_chunk_ids, (200,))
        self.assertEqual(out.started_chunk_ids, (200, 201))
        self.assertEqual(out.failing_chunk, 201)


# ===========================================================================
# Performance budget
# ===========================================================================
class PerformanceBudgetTests(_ObsTest):
    def _writer_p99(self, n):
        from src import remote_orchestrator as ro
        o = ro.RemoteOrchestrator.__new__(ro.RemoteOrchestrator)
        o._staging_pressure_active = False
        o._producer_err = None
        descs = [SimpleNamespace(chunk_index=200 + i, staged_bytes=GiB,
                                 pack_dir=f"/s/_pack_{i}") for i in range(n)]
        w, _ = self.make_worker(queue_max=1000000)
        samples = []
        for _ in range(1000):
            t0 = time.perf_counter()
            snap = o._capture_write_group_snapshot(
                "cid", 37, descs, "r", "Tape_03", 1, False, "active", False,
                "streaming")
            w.submit_snapshot(snap)
            samples.append(time.perf_counter() - t0)
        samples.sort()
        return samples[int(0.99 * len(samples)) - 1], sum(samples) / len(samples)

    def test_writer_thread_snapshot_enqueue_under_1ms_p99(self):
        for n in (12, 48):
            p99, avg = self._writer_p99(n)
            print(f"\n[perf] writer snapshot+enqueue n={n}: "
                  f"p99={p99*1e6:.1f} us avg={avg*1e6:.1f} us")
            self.assertLess(p99, 0.001,
                            f"p99 {p99*1e3:.3f} ms exceeds 1 ms for n={n}")


if __name__ == "__main__":
    unittest.main()
