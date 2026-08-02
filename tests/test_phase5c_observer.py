"""Phase 5C: the observation-only sealed-batch shadow.

Offline and deterministic. Proves the observer computes correct records and
comparisons, correctly handles Session 37's real shape (done chunks, the
`backing` chunk 108, an incomplete scan), and has ZERO production influence:
tripwire fakes assert it opens no DB connection, touches no LTFS/ownership, and
never mutates a ReadyQueue or chunk. No Z:\\, no IBM helper, no robocopy.
"""
import time
import unittest
from types import SimpleNamespace
from unittest import mock

from src import sealed_batch_observer as obs
from src.sealed_batch_observer import (
    SealedBatchObserver, ReadyGroupSnapshot, SnapshotMember,
    maybe_build_observer,
    MATCH, EXPECTED_DIFFERENCE, OBSERVER_ERROR, SCHEDULER_ERROR,
    DATA_INCONSISTENCY, RECONCILIATION_REQUIRED,
    PROPOSED_SEALABLE, PROPOSED_BLOCKED,
    MM_ALREADY_DONE, MM_BACKING_AMBIGUOUS, MM_INELIGIBLE_IN_DB,
    MM_DUPLICATE_CLAIM, MM_BYTE_COUNT, MM_MISSING_PACK_META, OBSERVER_VERSION)

GiB = 1024 ** 3
S37 = 37
TAPE = "Tape_03"


def _member(ci, gib=1.7, pack=None, files=200000, fp=None):
    return SnapshotMember(chunk_index=ci, prepared_bytes=int(gib * GiB),
                          pack_identity=pack or f"_pack_s0037_{ci:03d}",
                          file_count=files, pack_fingerprint=fp or f"fp-{ci}")


def _snap(members, reason="min_ready_bytes_reached", scan_complete=False,
          producer="active", pressure=False, safe_stop=False, tape=TAPE,
          gen=1, agg=None):
    return ReadyGroupSnapshot(
        session_id=S37, ready_queue_generation=gen, selection_reason=reason,
        members=list(members), expected_tape=tape, scan_complete=scan_complete,
        producer_state=producer, staging_pressure=pressure, safe_stop=safe_stop,
        aggregate_prepared_bytes=agg)


def _status(members, default="packing", overrides=None):
    m = {mm.chunk_index: default for mm in members}
    if overrides:
        m.update(overrides)
    return m


# ===========================================================================
# Construction guard + record format
# ===========================================================================
class ObserverConstructionTests(unittest.TestCase):
    def test_flag_false_builds_no_observer(self):
        cfg = SimpleNamespace(sealed_tape_write_batches_observation_enabled=False)
        self.assertIsNone(maybe_build_observer(cfg))

    def test_missing_flag_builds_no_observer(self):
        self.assertIsNone(maybe_build_observer(SimpleNamespace()))

    def test_flag_true_builds_observer(self):
        cfg = SimpleNamespace(sealed_tape_write_batches_observation_enabled=True)
        self.assertIsInstance(maybe_build_observer(cfg), SealedBatchObserver)


class ObservationRecordFormatTests(unittest.TestCase):
    def test_record_has_every_required_field(self):
        members = [_member(110), _member(111), _member(112)]
        rec = SealedBatchObserver().observe_group(
            _snap(members, gen=7), _status(members))
        for field in ("observation_id", "session_id", "observation_timestamp",
                      "ready_queue_generation", "selection_reason",
                      "ordered_chunk_ids", "prepared_bytes_per_chunk",
                      "aggregate_prepared_bytes", "pack_identities",
                      "expected_tape", "proposed_fingerprint", "scan_complete",
                      "producer_state", "staging_pressure_state",
                      "safe_stop_state", "remote_status_per_member",
                      "proposed_lifecycle_state", "validation_result",
                      "classification", "mismatch_codes", "observer_version",
                      "observation_schema_version"):
            self.assertIn(field, rec)
        self.assertEqual(rec["observer_version"], OBSERVER_VERSION)
        self.assertEqual(rec["ordered_chunk_ids"], [110, 111, 112])
        self.assertEqual(rec["ready_queue_generation"], 7)
        self.assertEqual(rec["aggregate_prepared_bytes"],
                         sum(m.prepared_bytes for m in members))

    def test_fingerprint_is_deterministic(self):
        members = [_member(110), _member(111)]
        a = SealedBatchObserver().observe_group(_snap(members), _status(members))
        b = SealedBatchObserver().observe_group(_snap(members), _status(members))
        self.assertEqual(a["proposed_fingerprint"], b["proposed_fingerprint"])


# ===========================================================================
# Mismatch taxonomy
# ===========================================================================
class MismatchTaxonomyTests(unittest.TestCase):
    def setUp(self):
        self.o = SealedBatchObserver()

    def test_clean_group_matches(self):
        members = [_member(110), _member(111)]
        rec = self.o.observe_group(_snap(members), _status(members))
        self.assertEqual(rec["classification"], MATCH)
        self.assertEqual(rec["mismatch_codes"], [])
        self.assertEqual(rec["proposed_lifecycle_state"], PROPOSED_SEALABLE)

    def test_already_done_is_data_inconsistency(self):
        members = [_member(110)]
        rec = self.o.observe_group(
            _snap(members), _status(members, overrides={110: "done"}))
        self.assertIn(MM_ALREADY_DONE, rec["mismatch_codes"])
        self.assertEqual(rec["classification"], DATA_INCONSISTENCY)
        self.assertEqual(rec["proposed_lifecycle_state"], PROPOSED_BLOCKED)

    def test_backing_requires_reconciliation(self):
        members = [_member(108)]
        rec = self.o.observe_group(
            _snap(members), _status(members, overrides={108: "backing"}))
        self.assertIn(MM_BACKING_AMBIGUOUS, rec["mismatch_codes"])
        self.assertEqual(rec["classification"], RECONCILIATION_REQUIRED)
        self.assertEqual(rec["proposed_lifecycle_state"], PROPOSED_BLOCKED)

    def test_missing_remote_row_is_inconsistent(self):
        members = [_member(110)]
        rec = self.o.observe_group(_snap(members), {})    # no status at all
        self.assertIn(MM_INELIGIBLE_IN_DB, rec["mismatch_codes"])
        self.assertEqual(rec["classification"], DATA_INCONSISTENCY)

    def test_duplicate_membership_flagged(self):
        members = [_member(110), _member(110)]
        rec = self.o.observe_group(_snap(members), _status(members))
        self.assertIn(MM_DUPLICATE_CLAIM, rec["mismatch_codes"])
        self.assertEqual(rec["classification"], DATA_INCONSISTENCY)

    def test_scheduler_byte_total_mismatch(self):
        members = [_member(110, gib=1.0)]
        rec = self.o.observe_group(
            _snap(members, agg=999), _status(members))
        self.assertIn(MM_BYTE_COUNT, rec["mismatch_codes"])
        self.assertEqual(rec["classification"], SCHEDULER_ERROR)

    def test_missing_pack_metadata(self):
        m = SnapshotMember(chunk_index=110, prepared_bytes=GiB,
                           pack_identity=None)
        rec = self.o.observe_group(_snap([m]), {110: "packing"})
        self.assertIn(MM_MISSING_PACK_META, rec["mismatch_codes"])
        self.assertIn(rec["classification"], (OBSERVER_ERROR,))

    def test_ineligible_failed_status(self):
        members = [_member(110)]
        rec = self.o.observe_group(
            _snap(members), _status(members, overrides={110: "backup_failed"}))
        self.assertIn(MM_INELIGIBLE_IN_DB, rec["mismatch_codes"])
        self.assertEqual(rec["classification"], DATA_INCONSISTENCY)


# ===========================================================================
# Session 37 semantics
# ===========================================================================
class Session37SemanticsTests(unittest.TestCase):
    def setUp(self):
        self.o = SealedBatchObserver()

    def test_done_chunks_0_to_107_are_never_proposed_clean(self):
        # A future-work group of genuinely prepared chunks excludes done chunks;
        # if a done chunk leaks in, it is flagged, never silently reused.
        members = [_member(108 + i) for i in range(3)]   # 108,109,110
        rec = self.o.observe_group(
            _snap(members), _status(members, overrides={108: "done"}))
        self.assertIn(MM_ALREADY_DONE, rec["mismatch_codes"])
        self.assertEqual(rec["classification"], DATA_INCONSISTENCY)

    def test_chunk_108_backing_is_flagged_not_reused(self):
        members = [_member(108)]
        rec = self.o.observe_group(
            _snap(members, scan_complete=False),
            _status(members, overrides={108: "backing"}))
        # The observer relies ONLY on PostgreSQL truth: 'backing' -> reconcile,
        # regardless of any physical evidence outside the database.
        self.assertEqual(rec["classification"], RECONCILIATION_REQUIRED)
        self.assertEqual(rec["proposed_lifecycle_state"], PROPOSED_BLOCKED)

    def test_incomplete_scan_does_not_block_a_prepared_group(self):
        members = [_member(200), _member(201)]
        rec = self.o.observe_group(
            _snap(members, scan_complete=False, producer="active"),
            _status(members, default="packing"))
        self.assertEqual(rec["classification"], MATCH)
        self.assertEqual(rec["proposed_lifecycle_state"], PROPOSED_SEALABLE)
        self.assertFalse(rec["scan_complete"])

    def test_sealable_group_does_not_imply_session_completeness(self):
        members = [_member(200)]
        rec = self.o.observe_group(
            _snap(members, scan_complete=False), _status(members))
        # The record carries no session-completeness claim; it is per-group.
        self.assertNotIn("session_complete", rec)
        self.assertFalse(rec["scan_complete"])

    def test_temporary_emptiness_is_not_producer_completion(self):
        rec = self.o.observe_group(
            _snap([], reason="waiting", producer="active"), {})
        # No members, producer still active -> not a final/sealable group.
        self.assertNotEqual(rec["proposed_lifecycle_state"], PROPOSED_SEALABLE)
        self.assertEqual(rec["producer_state"], "active")


# ===========================================================================
# Ten Session-37 replay fixtures
# ===========================================================================
class ReplayFixtureTests(unittest.TestCase):
    def setUp(self):
        self.o = SealedBatchObserver()

    def test_01_normal_threshold_group(self):
        members = [_member(200 + i) for i in range(12)]
        rec = self.o.observe_group(
            _snap(members, reason="min_ready_bytes_reached"), _status(members))
        self.assertEqual(rec["classification"], MATCH)
        self.assertEqual(rec["proposed_lifecycle_state"], PROPOSED_SEALABLE)

    def test_02_staging_pressure_partial_group(self):
        members = [_member(200), _member(201)]           # below min
        rec = self.o.observe_group(
            _snap(members, reason="staging_pressure_drain", pressure=True),
            _status(members))
        self.assertEqual(rec["classification"], MATCH)
        self.assertTrue(rec["staging_pressure_state"])

    def test_03_final_partial_after_producer_completion(self):
        members = [_member(400)]
        rec = self.o.observe_group(
            _snap(members, reason="producer_finished_final_group",
                  producer="closed"), _status(members))
        self.assertEqual(rec["classification"], MATCH)
        self.assertEqual(rec["producer_state"], "closed")

    def test_04_safe_stop_with_prepared_chunks(self):
        members = [_member(200), _member(201)]
        rec = self.o.observe_group(
            _snap(members, reason="explicit_drain_requested", safe_stop=True),
            _status(members))
        self.assertTrue(rec["safe_stop_state"])
        self.assertEqual(rec["classification"], MATCH)

    def test_05_producer_failure(self):
        members = [_member(200)]
        rec = self.o.observe_group(
            _snap(members, producer="failed"), _status(members))
        self.assertEqual(rec["producer_state"], "failed")
        # queued packs remain valid prepared work -> still sealable set
        self.assertEqual(rec["classification"], MATCH)

    def test_06_ownership_timeout_before_writing(self):
        # No chunk write started -> all pre-write, unambiguous, still eligible.
        members = [_member(200), _member(201)]
        rec = self.o.observe_group(
            _snap(members), _status(members, default="pending"))
        self.assertEqual(rec["classification"], MATCH)
        self.assertEqual(rec["proposed_lifecycle_state"], PROPOSED_SEALABLE)

    def test_07_later_chunk_failure_after_earlier_completions(self):
        members = [_member(200), _member(201), _member(202)]
        rec = self.o.observe_group(
            _snap(members),
            _status(members, overrides={200: "done", 201: "done",
                                        202: "backing"}))
        # Done members must never be re-proposed; the backing one needs
        # reconciliation. Highest severity wins.
        self.assertIn(MM_ALREADY_DONE, rec["mismatch_codes"])
        self.assertIn(MM_BACKING_AMBIGUOUS, rec["mismatch_codes"])
        self.assertEqual(rec["classification"], DATA_INCONSISTENCY)

    def test_08_crash_after_seal_before_ownership(self):
        # Sealed group, nothing written -> all members still pre-write.
        members = [_member(200), _member(201)]
        rec = self.o.observe_group(
            _snap(members, reason="min_ready_bytes_reached"),
            _status(members, default="pending"))
        self.assertEqual(rec["classification"], MATCH)

    def test_09_crash_after_some_completions(self):
        members = [_member(200), _member(201), _member(202)]
        rec = self.o.observe_group(
            _snap(members),
            _status(members, overrides={200: "done"}))
        self.assertIn(MM_ALREADY_DONE, rec["mismatch_codes"])
        self.assertEqual(rec["classification"], DATA_INCONSISTENCY)

    def test_10_sync_pending_and_process_loss(self):
        # All copies done, awaiting sync: every member 'done' in authority.
        members = [_member(200), _member(201)]
        rec = self.o.observe_group(
            _snap(members, reason="producer_finished_final_group",
                  producer="closed"),
            _status(members, default="done"))
        # A fresh selection must not re-propose done chunks.
        self.assertIn(MM_ALREADY_DONE, rec["mismatch_codes"])
        self.assertEqual(rec["classification"], DATA_INCONSISTENCY)


# ===========================================================================
# Comparison to Phase 4.5 selection
# ===========================================================================
class SchedulerComparisonTests(unittest.TestCase):
    def setUp(self):
        self.o = SealedBatchObserver()

    def test_identical_selection_matches(self):
        members = [_member(200), _member(201)]
        rec, cmp = self.o.compare_to_scheduler(
            _snap(members), _status(members), [200, 201])
        self.assertTrue(cmp["chunks_match"])
        self.assertEqual(cmp["classification"], MATCH)

    def test_chunk_108_selection_flags_reconciliation(self):
        members = [_member(108)]
        rec, cmp = self.o.compare_to_scheduler(
            _snap(members), _status(members, overrides={108: "backing"}), [108])
        # ReadyQueue would include it; the observer flags reconciliation.
        self.assertEqual(cmp["classification"], RECONCILIATION_REQUIRED)

    def test_order_difference_is_expected(self):
        members = [_member(201), _member(200)]           # observer order 201,200
        rec, cmp = self.o.compare_to_scheduler(
            _snap(members), _status(members), [200, 201])  # scheduler order
        self.assertFalse(cmp["chunks_match"])
        self.assertEqual(cmp["classification"], EXPECTED_DIFFERENCE)


# ===========================================================================
# Zero production influence (tripwire fakes)
# ===========================================================================
class ZeroInfluenceTests(unittest.TestCase):
    def test_observer_opens_no_db_connection(self):
        members = [_member(200 + i) for i in range(12)]
        with mock.patch("psycopg.connect",
                        side_effect=AssertionError("observer opened a DB "
                                                   "connection")):
            SealedBatchObserver().observe_group(_snap(members), _status(members))

    def test_observer_touches_no_ltfs_or_ownership(self):
        from src import ltfs
        from src import ltfs_ownership as own
        ltfs.reset_readiness("5c setup")
        self.addCleanup(ltfs.reset_readiness, "5c teardown")
        status_calls = []

        class Counting(ltfs.LtfsDriveCommand):
            def drive_status(self, drive):
                status_calls.append(drive)
                return "LTFS_MOUNTED", "", None
        prev = ltfs.set_ltfs_drive_command(Counting())
        self.addCleanup(ltfs.set_ltfs_drive_command, prev)
        gen = own.OWNERSHIP.generation
        members = [_member(200 + i) for i in range(12)]
        for _ in range(20):
            SealedBatchObserver().observe_group(_snap(members), _status(members))
        self.assertEqual(status_calls, [])
        self.assertEqual(own.OWNERSHIP.generation, gen)
        self.assertFalse(own.owns_ltfs())

    def test_observer_does_not_mutate_the_snapshot(self):
        members = [_member(200), _member(201)]
        snap = _snap(members)
        before = [(m.chunk_index, m.prepared_bytes) for m in snap.members]
        SealedBatchObserver().observe_group(snap, _status(members))
        after = [(m.chunk_index, m.prepared_bytes) for m in snap.members]
        self.assertEqual(before, after)
        self.assertEqual(len(snap.members), 2)

    def test_sink_failure_never_propagates(self):
        def bad_sink(record):
            raise RuntimeError("sink exploded")
        members = [_member(200)]
        # Must not raise: an observer/sink failure can never block the writer.
        rec = SealedBatchObserver(sink=bad_sink).observe_group(
            _snap(members), _status(members))
        self.assertIsNotNone(rec)

    def test_disabled_observer_is_none_so_no_activity(self):
        cfg = SimpleNamespace(sealed_tape_write_batches_observation_enabled=False)
        observer = maybe_build_observer(cfg)
        self.assertIsNone(observer)


# ===========================================================================
# Performance budget
# ===========================================================================
class PerformanceBudgetTests(unittest.TestCase):
    def _time(self, n_members, n_groups):
        o = SealedBatchObserver()
        members = [_member(200 + i, gib=0.01) for i in range(n_members)]
        snap = _snap(members)
        status = _status(members)
        t0 = time.perf_counter()
        for _ in range(n_groups):
            o.observe_group(snap, status)
        return time.perf_counter() - t0

    def test_12_chunk_group_is_fast(self):
        dt = self._time(12, 1)
        self.assertLess(dt, 0.5)

    def test_48_chunk_group_is_fast(self):
        dt = self._time(48, 1)
        self.assertLess(dt, 0.5)

    def test_1000_replay_groups_within_budget(self):
        dt = self._time(12, 1000)
        # Pure in-memory computation for 1000 12-chunk groups must be well under
        # a couple of seconds; this bounds the future async observation budget.
        self.assertLess(dt, 5.0)
        print(f"\n[perf] 1000 x 12-chunk observations: {dt*1000:.1f} ms "
              f"({dt:.4f}s)")


if __name__ == "__main__":
    unittest.main()
