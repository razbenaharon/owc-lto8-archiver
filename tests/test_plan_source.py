"""Plan 3, Task 1.3 - the PlanSource boundary.

The point of these tests is equivalence and refusal:

* a legacy chunk and an equivalent manifest chunk must produce **identical**
  ordered entries and identical summaries, or the boundary is not a boundary;
* a manifest chunk whose artifact is unreadable must **block**, never quietly
  fall back to a database whose rows do not exist for it by design.
"""
import unittest
from tempfile import TemporaryDirectory

from src.plan_source import (
    ChunkRef,
    LegacyDbPlanSource,
    ManifestPlanSource,
    PlanEntry,
    PlanSourceError,
    PlanSourceUnavailable,
    build_plan_source,
    plan_source_for_chunk,
    chunk_ref_from_row,
)
from src import plan_manifest as pm

LABEL = "REMOTE_srv01_20260709_234150"

MEMBERS = [
    ("/vault/D/shared-sets/DSETB/a.npy", 1024),
    ("/vault/D/shared-sets/DSETB/b.npy", 2048),
    ("/vault/D/shared-sets/DSETB/c.npy", 4096),
]


class FakeDb:
    """The two legacy reads the adapter is allowed to make, and nothing else."""

    def __init__(self, rows, summary=None):
        self.rows = rows
        self._summary = summary
        self.calls = []

    def get_chunk_files(self, session_id, chunk_index):
        self.calls.append(("get_chunk_files", session_id, chunk_index))
        return list(self.rows)

    def get_chunk_size_summary(self, session_id, chunk_index=None):
        self.calls.append(("get_chunk_size_summary", session_id, chunk_index))
        if self._summary is not None:
            return self._summary
        planned = sum(int(r["file_size_bytes"]) for r in self.rows)
        present = sum(int(r["file_size_bytes"]) for r in self.rows
                      if r.get("status") != "source_missing")
        return {chunk_index: (planned, present, len(self.rows))}


def legacy_rows(members=MEMBERS, status="pending"):
    return [
        {"manifest_id": 900000 + i, "remote_path": path,
         "file_size_bytes": size, "local_rel_path": None,
         "status": status, "error_msg": None, "updated_at": None}
        for i, (path, size) in enumerate(members)
    ]


class LegacyAdapterTests(unittest.TestCase):
    def setUp(self):
        self.db = FakeDb(legacy_rows())
        self.ref = ChunkRef(session_id=37, chunk_index=50,
                            session_label=LABEL, plan_source="legacy_db")
        self.source = LegacyDbPlanSource(self.db)

    def test_entries_preserve_database_order(self):
        entries = self.source.iter_chunk_entries(self.ref)
        self.assertEqual([e.source_path for e in entries],
                         [m[0] for m in MEMBERS])
        self.assertEqual([e.plan_ordinal for e in entries], [0, 1, 2])

    def test_entry_is_readable_with_the_legacy_column_names(self):
        entry = self.source.iter_chunk_entries(self.ref)[0]
        self.assertEqual(entry["remote_path"], MEMBERS[0][0])
        self.assertEqual(entry["file_size_bytes"], MEMBERS[0][1])
        self.assertEqual(entry["manifest_id"], 900000)
        self.assertIsNone(entry.get("local_rel_path"))
        self.assertIsNone(entry.get("nonexistent"))

    def test_summary_matches_the_pipeline_tuple_shape(self):
        summary = self.source.summary(self.ref)
        self.assertEqual(summary.as_tuple(), (1024 + 2048 + 4096,
                                              1024 + 2048 + 4096, 3))

    def test_duplicate_path_is_rejected_not_deduplicated(self):
        rows = legacy_rows()
        rows[2]["remote_path"] = rows[0]["remote_path"]
        source = LegacyDbPlanSource(FakeDb(rows))
        with self.assertRaises(PlanSourceError):
            source.iter_chunk_entries(self.ref)

    def test_adapter_does_not_decide_packaging_format(self):
        entry = self.source.iter_chunk_entries(self.ref)[0]
        self.assertIsNone(entry.container_format)


class ManifestAdapterTests(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.root = self._tmp.name
        self.addCleanup(self._tmp.cleanup)
        self.published = pm.write_plan_manifest(
            self.root, session_label=LABEL, chunk_index=113,
            provenance_kind="frontier",
            records=[{
                "canonical_path": path,
                "expected_size": size,
                "plan_ordinal": i,
                "session_label": LABEL,
                "chunk_index": 113,
                "provenance_kind": "frontier",
                "scan_segment_id": "seg-1",
                "scan_ordinal": 1000 + i,
                "storage_class": "container",
                "container_format": "stored_tar",
                "container_ordinal": 0,
                "routing_precision": "exact",
            } for i, (path, size) in enumerate(MEMBERS)])
        self.ref = ChunkRef(
            session_id=37, chunk_index=113, session_label=LABEL,
            plan_source="manifest",
            plan_artifact_locator=self.published.locator)
        self.source = ManifestPlanSource(self.root)

    def test_entries_come_from_the_artifact(self):
        entries = self.source.iter_chunk_entries(self.ref)
        self.assertEqual([e.source_path for e in entries],
                         [m[0] for m in MEMBERS])
        self.assertEqual([e.size_bytes for e in entries],
                         [m[1] for m in MEMBERS])
        self.assertTrue(all(e.plan_file_id is None for e in entries))

    def test_missing_locator_blocks_and_never_falls_back(self):
        ref = ChunkRef(session_id=37, chunk_index=113, session_label=LABEL,
                       plan_source="manifest", plan_artifact_locator=None)
        with self.assertRaises(PlanSourceUnavailable):
            self.source.iter_chunk_entries(ref)

    def test_unreadable_artifact_blocks_and_never_falls_back(self):
        ref = ChunkRef(session_id=37, chunk_index=113, session_label=LABEL,
                       plan_source="manifest",
                       plan_artifact_locator="plan_manifests/gone.jsonl.zst")
        with self.assertRaises(PlanSourceUnavailable):
            self.source.iter_chunk_entries(ref)

    def test_artifact_naming_another_chunk_is_refused(self):
        ref = ChunkRef(session_id=37, chunk_index=999, session_label=LABEL,
                       plan_source="manifest",
                       plan_artifact_locator=self.published.locator)
        with self.assertRaises(PlanSourceError):
            self.source.iter_chunk_entries(ref)

    def test_artifact_naming_another_session_is_refused(self):
        ref = ChunkRef(session_id=37, chunk_index=113,
                       session_label="SOME_OTHER_SESSION",
                       plan_source="manifest",
                       plan_artifact_locator=self.published.locator)
        with self.assertRaises(PlanSourceError):
            self.source.iter_chunk_entries(ref)

    def test_exception_states_are_joined_by_ordinal(self):
        source = ManifestPlanSource(
            self.root, exception_states=lambda ref: {1: "source_missing"})
        entries = source.iter_chunk_entries(self.ref)
        self.assertIsNone(entries[0].status)
        self.assertEqual(entries[1].status, "source_missing")
        summary = source.summary(self.ref)
        self.assertEqual(summary.planned_bytes, 1024 + 2048 + 4096)
        self.assertEqual(summary.present_bytes, 1024 + 4096)

    def test_adapter_does_not_decide_packaging_format(self):
        """The artifact records the planned container, but the adapter does not
        choose it - format authority stays on the chunk/container rows."""
        entries = self.source.iter_chunk_entries(self.ref)
        self.assertEqual(entries[0].container_format, "stored_tar")
        self.assertFalse(hasattr(self.source, "packaging_format"))


class EquivalenceTests(unittest.TestCase):
    """The same membership through both adapters must be indistinguishable."""

    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.root = self._tmp.name
        self.addCleanup(self._tmp.cleanup)

    def test_legacy_and_manifest_produce_identical_streams(self):
        legacy = LegacyDbPlanSource(FakeDb(legacy_rows()))
        legacy_ref = ChunkRef(session_id=37, chunk_index=5,
                              session_label=LABEL, plan_source="legacy_db")

        published = pm.write_plan_manifest(
            self.root, session_label=LABEL, chunk_index=5,
            provenance_kind="legacy_db_export",
            records=[{
                "canonical_path": path, "expected_size": size,
                "plan_ordinal": i, "normalized_chunk_ordinal": i,
                "session_label": LABEL, "chunk_index": 5,
                "provenance_kind": "legacy_db_export",
                "legacy_source_plan_file_id": 900000 + i,
                "legacy_original_ordinal": i,
                "source_snapshot_id": 42,
                "storage_class": "container", "container_format": "zip",
                "container_ordinal": 0, "routing_precision": "exact",
            } for i, (path, size) in enumerate(MEMBERS)])
        manifest = ManifestPlanSource(self.root)
        manifest_ref = ChunkRef(
            session_id=37, chunk_index=5, session_label=LABEL,
            plan_source="manifest", plan_artifact_locator=published.locator)

        left = legacy.iter_chunk_entries(legacy_ref)
        right = manifest.iter_chunk_entries(manifest_ref)
        self.assertEqual([(e.plan_ordinal, e.source_path, e.size_bytes)
                          for e in left],
                         [(e.plan_ordinal, e.source_path, e.size_bytes)
                          for e in right])
        self.assertEqual(legacy.summary(legacy_ref).as_tuple()[0],
                         manifest.summary(manifest_ref).as_tuple()[0])
        self.assertEqual(legacy.summary(legacy_ref).file_count,
                         manifest.summary(manifest_ref).file_count)


class SelectionTests(unittest.TestCase):
    def test_selection_is_by_persisted_plan_source_only(self):
        db = FakeDb(legacy_rows())
        legacy_ref = ChunkRef(session_id=37, chunk_index=0,
                              plan_source="legacy_db")
        self.assertIsInstance(build_plan_source(legacy_ref, db=db),
                              LegacyDbPlanSource)
        manifest_ref = ChunkRef(session_id=37, chunk_index=113,
                                plan_source="manifest",
                                plan_artifact_locator="x")
        self.assertIsInstance(
            build_plan_source(manifest_ref, db=db, archive_root="/tmp/root"),
            ManifestPlanSource)

    def test_manifest_without_an_archive_root_blocks(self):
        ref = ChunkRef(session_id=37, chunk_index=113, plan_source="manifest")
        with self.assertRaises(PlanSourceUnavailable):
            build_plan_source(ref, db=FakeDb([]), archive_root=None)

    def test_unknown_plan_source_is_refused(self):
        with self.assertRaises(PlanSourceError):
            ChunkRef(session_id=37, chunk_index=0, plan_source="whatever")

    def test_row_without_plan_source_column_is_legacy(self):
        ref = chunk_ref_from_row({"session_id": 37, "chunk_index": 12})
        self.assertEqual(ref.plan_source, "legacy_db")

    def test_mixed_adapters_within_one_session(self):
        """Session 37's shape: legacy chunks and manifest chunks side by side."""
        db = FakeDb(legacy_rows())
        sources = [
            build_plan_source(
                ChunkRef(session_id=37, chunk_index=i,
                         plan_source=("manifest" if i >= 113 else "legacy_db"),
                         plan_artifact_locator=("loc" if i >= 113 else None)),
                db=db, archive_root="/tmp/root")
            for i in (0, 48, 49, 112, 113, 114)
        ]
        self.assertEqual([s.name for s in sources],
                         ["legacy_db"] * 4 + ["manifest"] * 2)


class ResolverTests(unittest.TestCase):
    """`plan_source_for_chunk` must honour persisted metadata.

    The resolver deliberately ignores values it cannot trust (a test double that
    returns a non-mapping, a plan_source outside the schema's CHECK). That
    tolerance must never become "everything is legacy" -- a manifest chunk
    silently read as legacy would archive nothing while looking healthy.
    """

    class Db:
        def __init__(self, chunk_row=None, session_row=None):
            self._chunk_row = chunk_row
            self._session_row = session_row

        def get_remote_chunk(self, session_id, chunk_index):
            return self._chunk_row

        def get_remote_session(self, session_id):
            return self._session_row

        def get_chunk_files(self, session_id, chunk_index):
            return []

        def get_chunk_size_summary(self, session_id, chunk_index=None):
            return {}

    def test_persisted_manifest_selects_the_manifest_adapter(self):
        db = self.Db(
            chunk_row={"session_id": 37, "chunk_index": 113,
                       "plan_source": "manifest",
                       "plan_manifest_locator": "plan_manifests/x.jsonl.zst",
                       "session_label": LABEL})
        source, ref = plan_source_for_chunk(db, 37, 113,
                                            archive_root="/tmp/root")
        self.assertIsInstance(source, ManifestPlanSource)
        self.assertEqual(ref.plan_source, "manifest")
        self.assertEqual(ref.plan_artifact_locator,
                         "plan_manifests/x.jsonl.zst")
        self.assertEqual(ref.session_label, LABEL)

    def test_persisted_legacy_selects_the_legacy_adapter(self):
        db = self.Db(chunk_row={"session_id": 37, "chunk_index": 5,
                                "plan_source": "legacy_db",
                                "session_label": LABEL})
        source, ref = plan_source_for_chunk(db, 37, 5)
        self.assertIsInstance(source, LegacyDbPlanSource)
        self.assertEqual(ref.plan_source, "legacy_db")

    def test_database_without_migration_018_is_legacy(self):
        class OldDb:
            def get_chunk_files(self, *a):
                return []

            def get_chunk_size_summary(self, *a, **k):
                return {}

        source, ref = plan_source_for_chunk(OldDb(), 37, 5)
        self.assertIsInstance(source, LegacyDbPlanSource)
        self.assertEqual(ref.plan_source, "legacy_db")

    def test_manifest_chunk_with_a_ready_locator_still_blocks_when_absent(self):
        """The adapter is selected; the unreadable artifact is what blocks."""
        db = self.Db(chunk_row={"session_id": 37, "chunk_index": 113,
                                "plan_source": "manifest",
                                "plan_manifest_locator": "gone.jsonl.zst"})
        source, ref = plan_source_for_chunk(db, 37, 113,
                                            archive_root="/tmp/does-not-exist")
        with self.assertRaises(PlanSourceUnavailable):
            source.iter_chunk_entries(ref)

    def test_non_mapping_row_carries_no_planning_metadata(self):
        source, ref = plan_source_for_chunk(self.Db(chunk_row=object()), 37, 0)
        self.assertIsInstance(source, LegacyDbPlanSource)
        self.assertIsNone(ref.plan_artifact_locator)

    def test_blank_locator_is_treated_as_absent(self):
        db = self.Db(chunk_row={"session_id": 37, "chunk_index": 113,
                                "plan_source": "manifest",
                                "plan_manifest_locator": "   "})
        source, ref = plan_source_for_chunk(db, 37, 113,
                                            archive_root="/tmp/root")
        self.assertIsNone(ref.plan_artifact_locator)
        with self.assertRaises(PlanSourceUnavailable):
            source.iter_chunk_entries(ref)


class PlanEntryTests(unittest.TestCase):
    def test_as_legacy_row_roundtrips_the_expected_keys(self):
        entry = PlanEntry(plan_ordinal=0, source_path="/a", size_bytes=5,
                          plan_file_id=7, status="pending")
        row = entry.as_legacy_row()
        self.assertEqual(row["remote_path"], "/a")
        self.assertEqual(row["file_size_bytes"], 5)
        self.assertEqual(row["manifest_id"], 7)
        self.assertEqual(row["status"], "pending")

    def test_unknown_key_raises_keyerror(self):
        entry = PlanEntry(plan_ordinal=0, source_path="/a", size_bytes=5)
        with self.assertRaises(KeyError):
            entry["not_a_column"]


if __name__ == "__main__":                   # pragma: no cover
    unittest.main()
