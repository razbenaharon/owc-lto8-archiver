"""Plan 3, Tasks 1.4 and 1.5 - manifest-first sealing and terminal state.

The tests that matter here are the crash tests. Sealing publishes a local
artifact and then commits a database row; every interleaving of those two must
end with **exactly one** chunk and **exactly one** artifact, or a restart
duplicates work that was already written to tape.
"""
import os
import unittest
from tempfile import TemporaryDirectory

from src.archive_artifacts import ArtifactError, resolve_locator
from src.manifest_first import (
    ManifestChunkSealer,
    ManifestSealError,
    PendingChunk,
    TerminalStatePublisher,
    resolve_disposition,
)
from src import plan_manifest as pm
from src import terminal_manifest as tm

LABEL = "REMOTE_srv01_20260709_234150"


class FakeDb:
    """Records what the sealer asks of the database, and nothing more."""

    def __init__(self):
        self.chunks = {}
        self.artifacts = {}
        self.terminal = {}
        self.snapshot_file_rows = 0        # must stay 0 for manifest chunks
        self.plan_file_rows = 0            # must stay 0 for manifest chunks
        self.fail_create_with = None

    def find_manifest_chunk_by_artifact(self, session_id, chunk_index, key):
        row = self.chunks.get((session_id, chunk_index))
        if row and row["local_locator"] == key:
            return row
        return None

    def create_manifest_chunk(self, session_id, chunk_index, *, plan_locator,
                              packaging_format, expected_file_count,
                              expected_bytes, artifact_size_bytes=None,
                              scan_segment_consumption=()):
        if self.fail_create_with is not None:
            error, self.fail_create_with = self.fail_create_with, None
            raise error
        key = (session_id, chunk_index)
        if key in self.chunks:
            existing = self.chunks[key]
            if (existing["local_locator"] == plan_locator
                    and existing["expected_file_count"] == expected_file_count):
                return {"created": False, "chunk_index": chunk_index}
            raise RuntimeError("chunk already exists with different membership")
        self.chunks[key] = {
            "session_id": session_id, "chunk_index": chunk_index,
            "plan_source": "manifest", "membership_state": "sealed",
            "expected_file_count": expected_file_count,
            "expected_bytes": expected_bytes,
            "packaging_format": packaging_format,
            "local_locator": plan_locator,
            "consumed": list(scan_segment_consumption),
        }
        return {"created": True, "chunk_index": chunk_index}

    def record_terminal_manifest(self, session_id, chunk_index, *,
                                 terminal_locator, disposition_counts,
                                 archived_bytes=0, artifact_size_bytes=None):
        self.terminal[(session_id, chunk_index)] = {
            "locator": terminal_locator, "counts": dict(disposition_counts),
            "archived_bytes": archived_bytes,
        }
        return 1


def pending(chunk_index=113, count=3, **kw):
    chunk = PendingChunk(session_label=LABEL, chunk_index=chunk_index, **kw)
    for i in range(count):
        chunk.add(path=f"/vault/D/shared-sets/TSM/f{i}.npy", size=100 + i,
                  scan_segment_id="seg-1", scan_ordinal=1000 + i)
    chunk.note_consumption("seg-1", 1000, 1000 + count - 1)
    return chunk


class SealingTests(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.root = self._tmp.name
        self.addCleanup(self._tmp.cleanup)
        self.db = FakeDb()
        self.sealer = ManifestChunkSealer(self.db, self.root, 37, LABEL)

    def test_seal_publishes_artifact_then_creates_the_chunk(self):
        result = self.sealer.seal(pending())
        self.assertTrue(result["created"])
        self.assertEqual(result["file_count"], 3)
        self.assertEqual(result["byte_total"], 100 + 101 + 102)
        self.assertTrue(os.path.isfile(
            resolve_locator(self.root, result["locator"])))
        row = self.db.chunks[(37, 113)]
        self.assertEqual(row["plan_source"], "manifest")
        self.assertEqual(row["membership_state"], "sealed")
        self.assertEqual(row["packaging_format"], "stored_tar")

    def test_no_permanent_small_file_rows_are_created(self):
        """The whole reason manifest-first planning exists."""
        self.sealer.seal(pending(count=5000))
        self.assertEqual(self.db.snapshot_file_rows, 0)
        self.assertEqual(self.db.plan_file_rows, 0)

    def test_ordinals_are_stable_and_chunk_local(self):
        result = self.sealer.seal(pending(count=4))
        _h, records, _t = pm.read_plan_manifest(self.root, result["locator"])
        self.assertEqual([r["plan_ordinal"] for r in records], [0, 1, 2, 3])

    def test_container_ordinal_is_stable_for_every_member(self):
        result = self.sealer.seal(pending())
        _h, records, _t = pm.read_plan_manifest(self.root, result["locator"])
        self.assertTrue(all(r["container_ordinal"] is not None
                            for r in records))

    def test_scan_segment_consumption_is_recorded_with_the_chunk(self):
        self.sealer.seal(pending())
        self.assertEqual(self.db.chunks[(37, 113)]["consumed"],
                         [("seg-1", 1000, 1002)])

    def test_empty_chunk_is_refused(self):
        with self.assertRaises(ManifestSealError):
            self.sealer.seal(PendingChunk(session_label=LABEL, chunk_index=1))

    # -- crash points -----------------------------------------------------
    def test_crash_after_artifact_before_row_does_not_duplicate(self):
        """Artifact ready, row missing: re-entry finishes the SAME chunk."""
        self.db.fail_create_with = RuntimeError("connection lost mid-commit")
        with self.assertRaises(RuntimeError):
            self.sealer.seal(pending())
        # The artifact is on disk; the chunk is not in the database.
        locator = pm.plan_manifest_locator(LABEL, 113)
        self.assertTrue(os.path.isfile(resolve_locator(self.root, locator)))
        self.assertEqual(self.db.chunks, {})

        # Re-entry: the artifact name is taken, so the sealer adopts it after
        # proving equivalence, and creates exactly one chunk.
        result = self.sealer.seal(pending())
        self.assertTrue(result["created"])
        self.assertEqual(len(self.db.chunks), 1)
        self.assertEqual(result["locator"], locator)

    def test_reentry_after_full_success_creates_nothing_new(self):
        first = self.sealer.seal(pending())
        second = self.sealer.seal(pending())
        self.assertTrue(first["created"])
        self.assertFalse(second["created"])
        self.assertEqual(len(self.db.chunks), 1)

    def test_same_index_with_different_membership_is_refused(self):
        self.sealer.seal(pending())
        with self.assertRaises(ManifestSealError):
            # A different plan for a chunk index that already has an artifact.
            self.sealer.seal(pending(count=2))

    def test_sealed_chunk_never_gains_members(self):
        self.sealer.seal(pending(count=3))
        row = self.db.chunks[(37, 113)]
        self.assertEqual(row["expected_file_count"], 3)
        with self.assertRaises(ManifestSealError):
            self.sealer.seal(pending(count=4))
        self.assertEqual(self.db.chunks[(37, 113)]["expected_file_count"], 3)

    def test_a_later_chunk_takes_late_discoveries(self):
        self.sealer.seal(pending(chunk_index=113, count=2))
        result = self.sealer.seal(pending(chunk_index=114, count=1))
        self.assertTrue(result["created"])
        self.assertEqual(sorted(i for _s, i in self.db.chunks), [113, 114])


class BoundedChunkTests(unittest.TestCase):
    def test_file_count_bound_is_respected(self):
        chunk = PendingChunk(session_label=LABEL, chunk_index=0, max_files=2)
        self.assertTrue(chunk.has_room(10))
        chunk.add(path="/a", size=1, scan_segment_id="s", scan_ordinal=0)
        chunk.add(path="/b", size=1, scan_segment_id="s", scan_ordinal=1)
        self.assertFalse(chunk.has_room(1))

    def test_byte_bound_never_rejects_the_first_member(self):
        chunk = PendingChunk(session_label=LABEL, chunk_index=0, max_bytes=10)
        self.assertTrue(chunk.has_room(10_000_000))
        chunk.add(path="/big", size=10_000_000, scan_segment_id="s",
                  scan_ordinal=0)
        self.assertFalse(chunk.has_room(1))


class TerminalStateTests(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.root = self._tmp.name
        self.addCleanup(self._tmp.cleanup)
        self.db = FakeDb()
        self.sealer = ManifestChunkSealer(self.db, self.root, 37, LABEL)
        sealed = self.sealer.seal(pending())
        self.plan_locator = sealed["locator"]
        _h, self.plan_records, _t = pm.read_plan_manifest(
            self.root, self.plan_locator)
        self.publisher = TerminalStatePublisher(self.db, self.root, 37, LABEL)

    def archived(self, ordinal):
        return {"disposition": "archived", "final_evidence": "copied+cataloged",
                "observed_archived_size": self.plan_records[ordinal][
                    "expected_size"],
                "writer_completed": True, "catalog_committed": True}

    def test_every_ordinal_gets_exactly_one_disposition(self):
        outcomes = {i: self.archived(i) for i in range(3)}
        result = self.publisher.publish(
            113, plan_records=self.plan_records, outcomes=outcomes,
            plan_locator=self.plan_locator)
        self.assertTrue(result["created"])
        self.assertEqual(result["counts"]["archived"], 3)
        _h, records, _t = tm.read_terminal_manifest(self.root,
                                                    result["locator"])
        self.assertEqual(len(records), 3)
        joined = tm.join_to_plan(self.plan_records, records)
        self.assertEqual(len(joined), 3)

    def test_a_missing_disposition_blocks_publication(self):
        outcomes = {0: self.archived(0), 1: self.archived(1)}
        with self.assertRaises(ManifestSealError):
            self.publisher.publish(113, plan_records=self.plan_records,
                                   outcomes=outcomes)
        self.assertEqual(self.db.terminal, {})

    def test_archived_without_writer_completion_is_refused(self):
        outcomes = {i: self.archived(i) for i in range(3)}
        outcomes[1]["writer_completed"] = False
        with self.assertRaises(ManifestSealError):
            self.publisher.publish(113, plan_records=self.plan_records,
                                   outcomes=outcomes)

    def test_archived_without_catalog_commit_is_refused(self):
        outcomes = {i: self.archived(i) for i in range(3)}
        outcomes[2]["catalog_committed"] = False
        with self.assertRaises(ManifestSealError):
            self.publisher.publish(113, plan_records=self.plan_records,
                                   outcomes=outcomes)

    def test_ambiguous_writer_must_be_unresolved(self):
        outcomes = {i: self.archived(i) for i in range(3)}
        outcomes[0] = {"disposition": "source_missing",
                       "final_evidence": "stat said ENOENT",
                       "writer_ambiguous": True}
        with self.assertRaises(ManifestSealError):
            self.publisher.publish(113, plan_records=self.plan_records,
                                   outcomes=outcomes)

    def test_ambiguous_writer_recorded_as_unresolved_publishes(self):
        outcomes = {i: self.archived(i) for i in range(3)}
        outcomes[0] = {"disposition": "unresolved",
                       "final_evidence": "robocopy exit ambiguous",
                       "writer_ambiguous": True}
        result = self.publisher.publish(
            113, plan_records=self.plan_records, outcomes=outcomes,
            plan_locator=self.plan_locator)
        self.assertEqual(result["counts"]["unresolved"], 1)
        self.assertEqual(result["counts"]["archived"], 2)

    def test_all_source_missing_chunk_needs_no_tape_write(self):
        outcomes = {i: {"disposition": "source_missing",
                        "final_evidence": "source deleted before fetch"}
                    for i in range(3)}
        result = self.publisher.publish(
            113, plan_records=self.plan_records, outcomes=outcomes,
            plan_locator=self.plan_locator)
        self.assertEqual(result["counts"]["source_missing"], 3)
        self.assertEqual(result["counts"]["archived"], 0)
        self.assertEqual(
            self.db.terminal[(37, 113)]["archived_bytes"], 0)

    def test_terminal_aggregates_are_persisted(self):
        outcomes = {i: self.archived(i) for i in range(3)}
        self.publisher.publish(113, plan_records=self.plan_records,
                               outcomes=outcomes,
                               plan_locator=self.plan_locator)
        recorded = self.db.terminal[(37, 113)]
        self.assertEqual(recorded["counts"]["archived"], 3)
        self.assertEqual(recorded["archived_bytes"], 100 + 101 + 102)

    def test_republishing_does_not_duplicate(self):
        outcomes = {i: self.archived(i) for i in range(3)}
        first = self.publisher.publish(113, plan_records=self.plan_records,
                                       outcomes=outcomes,
                                       plan_locator=self.plan_locator)
        second = self.publisher.publish(113, plan_records=self.plan_records,
                                        outcomes=outcomes,
                                        plan_locator=self.plan_locator)
        self.assertTrue(first["created"])
        self.assertFalse(second["created"])

    def test_terminal_manifest_is_not_written_to_tape(self):
        outcomes = {i: self.archived(i) for i in range(3)}
        result = self.publisher.publish(113, plan_records=self.plan_records,
                                        outcomes=outcomes,
                                        plan_locator=self.plan_locator)
        path = resolve_locator(self.root, result["locator"])
        self.assertTrue(path.startswith(os.path.abspath(self.root)))
        self.assertFalse(path.lower().startswith("z:"))


class DispositionResolutionTests(unittest.TestCase):
    def test_writer_and_catalog_success_is_archived(self):
        self.assertEqual(
            resolve_disposition(writer_state="copied",
                                catalog_state="committed"),
            ("archived", False))

    def test_ambiguous_writer_is_unresolved(self):
        self.assertEqual(
            resolve_disposition(writer_state="ambiguous",
                                catalog_state="not_started"),
            ("unresolved", True))

    def test_ambiguous_catalog_is_unresolved(self):
        self.assertEqual(
            resolve_disposition(writer_state="copied",
                                catalog_state="ambiguous"),
            ("unresolved", True))

    def test_copy_success_with_catalog_failure_is_not_archived(self):
        disposition, ambiguous = resolve_disposition(
            writer_state="copied", catalog_state="failed")
        self.assertEqual(disposition, "unresolved")
        self.assertFalse(ambiguous)

    def test_source_error_wins_over_incomplete_writer_state(self):
        self.assertEqual(
            resolve_disposition(writer_state="not_started",
                                catalog_state="not_started",
                                source_error="source_permission_denied"),
            ("source_permission_denied", False))

    def test_ambiguity_wins_over_a_source_error(self):
        """A file that might have reached tape is never a clean exception."""
        self.assertEqual(
            resolve_disposition(writer_state="ambiguous",
                                catalog_state="not_started",
                                source_error="source_missing"),
            ("unresolved", True))


if __name__ == "__main__":                   # pragma: no cover
    unittest.main()
