"""Plan 3, Task 3.2 - non-destructive legacy membership export."""
import unittest
from tempfile import TemporaryDirectory

from src.legacy_export import (
    LegacyExportError,
    export_chunk,
    export_session_chunks,
    verify_export,
)
from src import plan_manifest as pm

LABEL = "REMOTE_srv01_20260709_234150"


class FakeDb:
    """Yields membership the way the streaming cursor does."""

    def __init__(self, rows_by_chunk):
        self.rows = rows_by_chunk
        self.mutations = 0          # must stay 0: the export deletes nothing

    def iter_legacy_chunk_membership(self, session_id, chunk_index,
                                     **_kw):
        for row in self.rows.get(chunk_index, []):
            yield dict(row)


def rows(count=3, chunk_index=5, start_ordinal=0, size=1000):
    return [{
        "plan_file_id": 900000 + i,
        "original_ordinal": start_ordinal + i,
        "chunk_index": chunk_index,
        "snapshot_file_id": 500000 + i,
        "remote_path": f"/vault/D/shared-sets/TSM/f{i}.npy",
        "file_size_bytes": size + i,
    } for i in range(count)]


class ExportTests(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.root = self._tmp.name
        self.addCleanup(self._tmp.cleanup)
        self.db = FakeDb({5: rows()})

    def export(self, **kw):
        return export_chunk(self.db, self.root, session_id=37,
                            session_label=LABEL, chunk_index=5, **kw)

    def test_export_preserves_legacy_identity(self):
        result = self.export()
        _h, records, _t = pm.read_plan_manifest(self.root, result.locator)
        self.assertEqual([r["legacy_source_plan_file_id"] for r in records],
                         [900000, 900001, 900002])
        self.assertEqual([r["source_snapshot_id"] for r in records],
                         [500000, 500001, 500002])

    def test_scan_segment_provenance_is_left_null(self):
        """The database cannot prove it, so the export must not invent it."""
        result = self.export()
        _h, records, _t = pm.read_plan_manifest(self.root, result.locator)
        for record in records:
            self.assertIsNone(record.get("scan_segment_id"))
            self.assertIsNone(record.get("scan_ordinal"))

    def test_global_ordinals_are_normalized_to_chunk_local(self):
        self.db = FakeDb({5: rows(count=3, start_ordinal=140_000)})
        result = self.export()
        _h, records, _t = pm.read_plan_manifest(self.root, result.locator)
        self.assertEqual([r["plan_ordinal"] for r in records], [0, 1, 2])
        self.assertEqual([r["legacy_original_ordinal"] for r in records],
                         [140_000, 140_001, 140_002])
        self.assertEqual([r["normalized_chunk_ordinal"] for r in records],
                         [0, 1, 2])

    def test_small_files_export_as_coarse(self):
        result = self.export()
        self.assertEqual(result.routing_precision, "coarse")
        self.assertTrue(any("COARSE" in w for w in result.warnings))
        header, records, _t = pm.read_plan_manifest(self.root, result.locator)
        self.assertFalse(pm.is_authoritative(header, records))

    def test_a_coarse_export_is_never_authoritative(self):
        result = self.export()
        header, _records, _t = pm.read_plan_manifest(self.root, result.locator)
        self.assertFalse(header.get("authoritative"))
        self.assertEqual(header.get("export_kind"), "auxiliary")

    def test_large_files_export_as_exact_loose(self):
        self.db = FakeDb({5: rows(count=2, size=50 * 1024 * 1024)})
        result = self.export()
        self.assertEqual(result.routing_precision, "exact")
        header, records, _t = pm.read_plan_manifest(self.root, result.locator)
        self.assertTrue(all(r["storage_class"] == "loose" for r in records))
        self.assertTrue(pm.is_authoritative(header, records))

    def test_proven_routing_exports_as_exact_container_members(self):
        routing = {r["remote_path"]: (0, "zip") for r in rows()}
        result = self.export(container_routing=routing)
        self.assertEqual(result.routing_precision, "exact")
        header, records, _t = pm.read_plan_manifest(self.root, result.locator)
        self.assertTrue(all(r["container_ordinal"] == 0 for r in records))
        self.assertTrue(pm.is_authoritative(header, records))

    def test_export_deletes_or_modifies_nothing(self):
        self.export()
        self.assertEqual(self.db.mutations, 0)

    def test_an_empty_chunk_is_refused(self):
        db = FakeDb({5: []})
        with self.assertRaises(LegacyExportError):
            export_chunk(db, self.root, session_id=37, session_label=LABEL,
                         chunk_index=5)

    def test_reexport_of_the_same_membership_is_adopted(self):
        first = self.export()
        second = self.export()
        self.assertTrue(first.created)
        self.assertFalse(second.created)
        self.assertEqual(first.locator, second.locator)

    def test_reexport_with_different_membership_is_refused(self):
        self.export()
        self.db = FakeDb({5: rows(count=2)})
        with self.assertRaises(LegacyExportError):
            self.export()

    def test_verify_proves_equivalence(self):
        self.export()
        report = verify_export(self.db, self.root, session_id=37,
                               session_label=LABEL, chunk_index=5)
        self.assertTrue(report["equivalent"])
        self.assertEqual(report["records"], 3)

    def test_verify_detects_a_drifted_membership(self):
        self.export()
        self.db = FakeDb({5: rows(count=3, size=999)})
        with self.assertRaises(LegacyExportError):
            verify_export(self.db, self.root, session_id=37,
                          session_label=LABEL, chunk_index=5)


class BoundedScopeTests(unittest.TestCase):
    """~29 million plan rows: an implicit full export must be impossible."""

    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.root = self._tmp.name
        self.addCleanup(self._tmp.cleanup)
        self.db = FakeDb({i: rows(count=2, chunk_index=i) for i in range(20)})

    def test_an_unbounded_request_is_refused(self):
        with self.assertRaises(LegacyExportError):
            export_session_chunks(self.db, self.root, session_id=37,
                                  session_label=LABEL,
                                  chunk_indexes=range(20))

    def test_no_chunks_named_is_refused(self):
        with self.assertRaises(LegacyExportError):
            export_session_chunks(self.db, self.root, session_id=37,
                                  session_label=LABEL, chunk_indexes=[])

    def test_a_named_bounded_set_exports(self):
        results = export_session_chunks(
            self.db, self.root, session_id=37, session_label=LABEL,
            chunk_indexes=[1, 2, 3])
        self.assertEqual([r.chunk_index for r in results], [1, 2, 3])
        self.assertTrue(all(r.created for r in results))

    def test_raising_the_bound_must_be_deliberate(self):
        results = export_session_chunks(
            self.db, self.root, session_id=37, session_label=LABEL,
            chunk_indexes=range(12), max_chunks=12)
        self.assertEqual(len(results), 12)


if __name__ == "__main__":                   # pragma: no cover
    unittest.main()
