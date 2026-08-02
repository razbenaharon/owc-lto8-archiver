"""Phase 5A: proving the sealed-batch design with an executable model.

Every test runs against the in-memory SQLite reference model in
``sealed_batch_model`` — no production PostgreSQL, no migration, no tape, no
LTFS. The numbering matches the handoff's required proofs (1-18).
"""
import unittest

from tests.sealed_batch_model import (
    SealedBatchModel, SealedBatchError, ImmutableBatchError, GenerationConflict,
    compute_batch_fingerprint,
    BUILDING, SEALED, WRITING, SYNC_PENDING, DURABLE, FAILED, CANCELLED,
    CHUNK_PENDING, CHUNK_BACKING, CHUNK_DONE, FORMAT_GENERATION)

GiB = 1024 ** 3


def _build(model, session_id=37, tape="Tape_03", chunks=((108, 1.7),)):
    bid = model.begin_batch(session_id, tape)
    for idx, gib in chunks:
        model.add_chunk(bid, idx, pack_identity=f"_pack_s0037_{idx:03d}",
                        prepared_bytes=int(gib * GiB), file_count=200000,
                        pack_fingerprint=f"fp-{idx}")
    return bid


def _seal_and_claim(model, bid, owner="host/pid1"):
    model.seal(bid)
    return model.claim_for_write(bid, owner)


class SealedBatchModelTests(unittest.TestCase):
    def setUp(self):
        self.m = SealedBatchModel()

    # 1 -----------------------------------------------------------------
    def test_building_data_is_not_sealed(self):
        bid = _build(self.m, chunks=((108, 1.7), (109, 1.7)))
        self.assertEqual(self.m.state(bid), BUILDING)
        self.assertIsNone(self.m._row(bid)["batch_fingerprint"])
        # A building batch is explicitly not authoritative for reconstruction.
        with self.assertRaises(SealedBatchError):
            self.m.reconstruct(bid)

    # 2 -----------------------------------------------------------------
    def test_sealing_is_atomic_and_one_shot(self):
        bid = _build(self.m)
        fp = self.m.seal(bid)
        self.assertEqual(self.m.state(bid), SEALED)
        self.assertTrue(fp)
        self.assertIsNotNone(self.m._row(bid)["sealed_at"])
        # Re-sealing a non-building batch is rejected (single guarded UPDATE).
        with self.assertRaises(ImmutableBatchError):
            self.m.seal(bid)

    # 3 -----------------------------------------------------------------
    def test_sealed_batch_is_immutable(self):
        bid = _build(self.m)
        self.m.seal(bid)
        with self.assertRaises(ImmutableBatchError):
            self.m.add_chunk(bid, 200, "_pack_s0037_200", GiB, 1)

    def test_empty_batch_cannot_be_sealed(self):
        bid = self.m.begin_batch(37, "Tape_03")
        with self.assertRaises(SealedBatchError):
            self.m.seal(bid)

    # 4 -----------------------------------------------------------------
    def test_ordered_membership_is_deterministic(self):
        bid = _build(self.m, chunks=((110, 1.0), (108, 1.0), (109, 1.0)))
        members = self.m._members(bid)
        # Insertion order defines the ordinal; ordering is stable and explicit.
        self.assertEqual([m["chunk_index"] for m in members], [110, 108, 109])
        self.assertEqual([m["ordinal"] for m in members], [0, 1, 2])

    # 5 -----------------------------------------------------------------
    def test_fingerprint_detects_mutation(self):
        members = [
            {"ordinal": 0, "chunk_index": 108, "pack_identity": "p108",
             "prepared_bytes": GiB, "file_count": 10, "pack_fingerprint": "a"},
            {"ordinal": 1, "chunk_index": 109, "pack_identity": "p109",
             "prepared_bytes": GiB, "file_count": 10, "pack_fingerprint": "b"},
        ]
        base = compute_batch_fingerprint("Tape_03", FORMAT_GENERATION, members)

        # (a) reordering
        reordered = [dict(members[1], ordinal=0), dict(members[0], ordinal=1)]
        self.assertNotEqual(
            base, compute_batch_fingerprint("Tape_03", FORMAT_GENERATION,
                                            reordered))
        # (b) byte-count change
        mutated_bytes = [dict(members[0], prepared_bytes=GiB + 1), members[1]]
        self.assertNotEqual(
            base, compute_batch_fingerprint("Tape_03", FORMAT_GENERATION,
                                            mutated_bytes))
        # (c) pack-identity change
        mutated_path = [dict(members[0], pack_identity="p999"), members[1]]
        self.assertNotEqual(
            base, compute_batch_fingerprint("Tape_03", FORMAT_GENERATION,
                                            mutated_path))
        # (d) membership change (drop a chunk)
        self.assertNotEqual(
            base, compute_batch_fingerprint("Tape_03", FORMAT_GENERATION,
                                            members[:1]))
        # (e) expected-tape change
        self.assertNotEqual(
            base, compute_batch_fingerprint("Tape_99", FORMAT_GENERATION,
                                            members))

    # 6 -----------------------------------------------------------------
    def test_crash_before_seal_leaves_no_authoritative_batch(self):
        _build(self.m, chunks=((108, 1.7), (109, 1.7)))       # never sealed
        discarded = self.m.discard_unsealed(37)
        self.assertEqual(len(discarded), 1)
        # The chunks are released, so they can be re-batched after restart.
        self.assertEqual(
            self.m.db.execute("SELECT COUNT(*) FROM tape_write_active_chunk")
            .fetchone()[0], 0)

    # 7 -----------------------------------------------------------------
    def test_crash_after_seal_reconstructs_the_exact_group(self):
        bid = _build(self.m, chunks=((108, 1.7), (109, 1.9), (110, 2.1)))
        fp = self.m.seal(bid)
        # Simulate a fresh process: a NEW model instance would read the same
        # rows; here we reconstruct from the persisted rows and re-verify.
        members, ok = self.m.reconstruct(bid)
        self.assertTrue(ok, "fingerprint did not re-verify after seal")
        self.assertEqual([m["chunk_index"] for m in members], [108, 109, 110])
        self.assertEqual(self.m._row(bid)["batch_fingerprint"], fp)

    def test_reconstruction_detects_tampering(self):
        bid = _build(self.m, chunks=((108, 1.7), (109, 1.9)))
        self.m.seal(bid)
        # Tamper with a persisted row (as a bit-flip / bad edit would).
        self.m.db.execute(
            "UPDATE tape_write_batch_chunks SET prepared_bytes=1 "
            "WHERE batch_id=? AND chunk_index=108", (bid,))
        self.m.db.commit()
        _, ok = self.m.reconstruct(bid)
        self.assertFalse(ok, "tampering was not detected by the fingerprint")

    # 8 -----------------------------------------------------------------
    def test_later_chunk_failure_preserves_earlier_successes(self):
        bid = _build(self.m, chunks=((108, 1.7), (109, 1.7), (110, 1.7)))
        _seal_and_claim(self.m, bid)
        for idx in (108, 109):
            self.m.record_chunk_write_start(bid, idx)
            self.m.commit_chunk_done(bid, idx)
        # chunk 110 starts then fails after write began -> stays 'backing'.
        self.m.record_chunk_write_start(bid, 110)
        self.m.fail_chunk(bid, 110, leave_backing=True)
        self.m.fail_batch(bid, release_unwritten=True)
        # 108 and 109 remain authoritative 'done' regardless of batch failure.
        self.assertEqual(self.m.chunk_state(bid, 108), CHUNK_DONE)
        self.assertEqual(self.m.chunk_state(bid, 109), CHUNK_DONE)
        self.assertEqual(self.m.chunk_state(bid, 110), CHUNK_BACKING)
        self.assertEqual(self.m.state(bid), FAILED)

    # 9 -----------------------------------------------------------------
    def test_unstarted_chunks_remain_reusable(self):
        bid = _build(self.m, chunks=((108, 1.7), (109, 1.7), (110, 1.7)))
        _seal_and_claim(self.m, bid)
        self.m.record_chunk_write_start(bid, 108)
        self.m.commit_chunk_done(bid, 108)
        # 109 and 110 never started; a batch failure must leave them reusable.
        self.m.fail_batch(bid, release_unwritten=True)
        self.assertEqual(self.m.chunk_state(bid, 109), CHUNK_PENDING)
        self.assertEqual(self.m.chunk_state(bid, 110), CHUNK_PENDING)
        # Their claims were released, so a NEW batch can take them.
        bid2 = self.m.begin_batch(37, "Tape_03")
        self.m.add_chunk(bid2, 109, "_pack_s0037_109", GiB, 1)   # no conflict
        self.assertEqual(self.m.chunk_state(bid2, 109), CHUNK_PENDING)

    # 10 ----------------------------------------------------------------
    def test_no_durability_before_sync_confirmation(self):
        bid = _build(self.m)
        _seal_and_claim(self.m, bid)
        self.m.record_chunk_write_start(bid, 108)
        self.m.commit_chunk_done(bid, 108)
        self.m.request_sync(bid)
        self.assertEqual(self.m.state(bid), SYNC_PENDING)
        # Durable is refused until LTFS sync is CONFIRMED.
        with self.assertRaises(SealedBatchError):
            self.m.mark_durable(bid)
        self.m.confirm_sync(bid)
        self.m.mark_durable(bid)
        self.assertEqual(self.m.state(bid), DURABLE)
        self.assertTrue(self.m.is_prune_safe(bid))

    def test_prune_safe_requires_durable(self):
        bid = _build(self.m)
        _seal_and_claim(self.m, bid)
        self.m.record_chunk_write_start(bid, 108)
        self.m.commit_chunk_done(bid, 108)
        self.m.request_sync(bid)
        self.m.confirm_sync(bid)
        self.assertFalse(self.m.is_prune_safe(bid))   # not yet marked durable
        self.m.mark_durable(bid)
        self.assertTrue(self.m.is_prune_safe(bid))

    # 11 ----------------------------------------------------------------
    def test_sync_then_crash_is_reconciled_idempotently(self):
        bid = _build(self.m)
        _seal_and_claim(self.m, bid)
        self.m.record_chunk_write_start(bid, 108)
        self.m.commit_chunk_done(bid, 108)
        self.m.request_sync(bid)
        self.m.confirm_sync(bid)
        self.m.mark_durable(bid)
        durable_at = self.m._row(bid)["durable_at"]
        # A crash after durable, then a recovery retry: mark_durable is a no-op.
        self.m.mark_durable(bid)
        self.assertEqual(self.m._row(bid)["durable_at"], durable_at)
        self.assertEqual(self.m.state(bid), DURABLE)

    # 12 ----------------------------------------------------------------
    def test_cancellation_does_not_delete_uncommitted_packs(self):
        bid = _build(self.m, chunks=((108, 1.7), (109, 1.7)))
        self.m.seal(bid)
        # Cancel after sealing but before writing: packs are retained (the model
        # never deletes pack rows; membership stays for audit, claims release).
        self.m.cancel_batch(bid, release_unwritten=True)
        self.assertEqual(self.m.state(bid), CANCELLED)
        members = self.m._members(bid)
        self.assertEqual(len(members), 2, "membership/packs were destroyed")
        self.assertTrue(all(m["chunk_state"] == CHUNK_PENDING for m in members))

    # 13 ----------------------------------------------------------------
    def test_incomplete_scan_does_not_block_sealing(self):
        # scan_complete=false: sealing a physical group of currently-ready
        # chunks must succeed without any claim that the session plan is final.
        bid = _build(self.m, chunks=((108, 1.7), (109, 1.7)))
        fp = self.m.seal(bid)
        self.assertTrue(fp)
        self.assertEqual(self.m.state(bid), SEALED)

    # 14 ----------------------------------------------------------------
    def test_sealing_a_group_does_not_seal_the_session_plan(self):
        # The batch carries no session-terminality flag: a sealed physical group
        # is independent of session scan completion.
        bid = _build(self.m, chunks=((108, 1.7),))
        self.m.seal(bid)
        cols = {r[1] for r in self.m.db.execute(
            "PRAGMA table_info(tape_write_batches)").fetchall()}
        self.assertNotIn("scan_complete", cols)
        self.assertNotIn("session_terminal", cols)

    # 15 ----------------------------------------------------------------
    def test_legacy_zip_packs_are_untouched(self):
        # The model records a stable pack identity and format generation; it
        # never rewrites, converts or regenerates the pack.
        bid = _build(self.m)
        self.assertEqual(self.m._row(bid)["format_generation"], FORMAT_GENERATION)
        m0 = self.m._members(bid)[0]
        self.assertEqual(m0["pack_identity"], "_pack_s0037_108")

    # 16 ----------------------------------------------------------------
    def test_one_chunk_cannot_join_two_active_batches(self):
        b1 = self.m.begin_batch(37, "Tape_03")
        self.m.add_chunk(b1, 108, "_pack_s0037_108", GiB, 1)
        b2 = self.m.begin_batch(37, "Tape_03")
        with self.assertRaises(SealedBatchError):
            self.m.add_chunk(b2, 108, "_pack_s0037_108", GiB, 1)
        # b2 did not gain the chunk.
        self.assertIsNone(self.m.chunk_state(b2, 108))

    def test_released_chunk_can_rejoin_after_batch_cancel(self):
        b1 = self.m.begin_batch(37, "Tape_03")
        self.m.add_chunk(b1, 108, "_pack_s0037_108", GiB, 1)
        self.m.cancel_batch(b1, release_unwritten=True)
        b2 = self.m.begin_batch(37, "Tape_03")
        self.m.add_chunk(b2, 108, "_pack_s0037_108", GiB, 1)   # now allowed
        self.assertEqual(self.m.chunk_state(b2, 108), CHUNK_PENDING)

    # 17 ----------------------------------------------------------------
    def test_abandoned_writer_claim_is_recovered_safely(self):
        clock = [1000.0]
        m = SealedBatchModel(now=lambda: clock[0])
        bid = _build(m, chunks=((108, 1.7),))
        lease = _seal_and_claim(m, bid, owner="dead/host")
        gen_before = m.generation(bid)
        # Time advances past the lease; the previous writer died holding it.
        clock[0] = 1000.0 + 10_000
        recovered = m.recover_abandoned_writer()
        self.assertEqual(recovered, [bid])
        self.assertEqual(m.state(bid), SEALED)             # re-claimable
        self.assertGreater(m.generation(bid), gen_before)  # stale token loses
        # The stale lease's generation can no longer win a claim CAS.
        with self.assertRaises(GenerationConflict):
            m.claim_for_write(bid, "new/host",
                              expected_generation=gen_before)
        # A fresh claim (no generation assertion) succeeds.
        new_lease = m.claim_for_write(bid, "new/host")
        self.assertNotEqual(new_lease, lease)

    # 18 ----------------------------------------------------------------
    def test_ownership_failure_before_writing_leaves_chunks_pre_write(self):
        bid = _build(self.m, chunks=((108, 1.7), (109, 1.7)))
        self.m.seal(bid)
        # Ownership could not be acquired -> the batch is never claimed for
        # writing; no chunk ever entered 'backing'. Treat as a failed batch that
        # releases its (unwritten) chunks; all stay pre-write and unambiguous.
        self.assertEqual(self.m.state(bid), SEALED)
        self.m.fail_batch(bid, release_unwritten=True)
        for idx in (108, 109):
            self.assertEqual(self.m.chunk_state(bid, idx), CHUNK_PENDING)

    # extra: claim CAS / generation ------------------------------------
    def test_claim_requires_sealed_state(self):
        bid = _build(self.m)
        with self.assertRaises(GenerationConflict):
            self.m.claim_for_write(bid, "host")            # still building
        self.m.seal(bid)
        self.m.claim_for_write(bid, "host")
        with self.assertRaises(GenerationConflict):
            self.m.claim_for_write(bid, "host2")           # already writing


if __name__ == "__main__":
    unittest.main()
