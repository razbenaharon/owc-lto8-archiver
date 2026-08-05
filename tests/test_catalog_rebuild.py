"""Plan 3, Tasks 4.2 and 4.3 - shadow rebuild and semantic comparison."""
import os
import unittest
from tempfile import TemporaryDirectory

from src.catalog_rebuild import (
    CatalogRebuilder,
    RebuildIncomplete,
    RebuildRefused,
    canonical_session_view,
    compare_catalogs,
    discover_artifacts,
)
from src import plan_manifest as pm
from src import scan_state_manifest as ssm
from src import terminal_manifest as tm

LABEL = "REMOTE_srv01_20260709_234150"


class FakeShadowDb:
    def __init__(self, name, empty=True):
        self.name = name
        self._empty = empty
        self.written = []
        self.tape_reads = 0

    def current_database_name(self):
        return self.name

    def shadow_database_is_empty(self):
        return self._empty

    def write_rebuilt_session(self, label, **kw):
        self.written.append((label, kw))


def build_artifacts(root, *, label=LABEL, chunks=(113,), archived=True,
                    generations=1, coarse=False):
    for generation in range(generations):
        ssm.write_scan_state_segment(
            root, session_label=label, generation=generation,
            records=[{
                "record_type": "directory", "session_label": label,
                "directory_id": generation + 1,
                "path": f"/vault/D/shared-sets/d{generation}",
                "parent_directory_id": None, "state": "listed",
                "listing_complete": True, "traversal_complete": True,
                "planning_complete": True, "child_count": 0}])
    ssm.write_session_descriptor(
        root, session_label=label, session_state="active", scan_complete=False,
        records=[{"record_type": "scan_state_generation",
                  "session_label": label, "generation": g,
                  "locator": ssm.scan_state_locator(label, generation=g)}
                 for g in range(generations)])

    for chunk_index in chunks:
        if coarse:
            records = [{
                "canonical_path": f"/vault/D/shared-sets/f{i}",
                "expected_size": 100 + i, "plan_ordinal": i,
                "normalized_chunk_ordinal": i, "session_label": label,
                "chunk_index": chunk_index,
                "provenance_kind": "legacy_db_export",
                "legacy_source_plan_file_id": 900 + i,
                "legacy_original_ordinal": i, "source_snapshot_id": 1,
                "storage_class": "container", "container_format": "zip",
                "container_ordinal": None, "routing_precision": "coarse",
            } for i in range(3)]
            kind = "legacy_db_export"
        else:
            records = [{
                "canonical_path": f"/vault/D/shared-sets/f{i}",
                "expected_size": 100 + i, "plan_ordinal": i,
                "session_label": label, "chunk_index": chunk_index,
                "provenance_kind": "frontier", "scan_segment_id": "seg-1",
                "scan_ordinal": 1000 + i, "storage_class": "container",
                "container_format": "stored_tar", "container_ordinal": 0,
                "routing_precision": "exact",
            } for i in range(3)]
            kind = "frontier"
        plan = pm.write_plan_manifest(
            root, session_label=label, chunk_index=chunk_index,
            provenance_kind=kind, records=records,
            packaging_format="stored_tar")
        if archived:
            tm.write_terminal_manifest(
                root, session_label=label, chunk_index=chunk_index,
                plan_artifact_locator=plan.locator,
                plan_ordinals=[r["plan_ordinal"] for r in records],
                records=[{
                    "plan_ordinal": r["plan_ordinal"],
                    "canonical_path": r["canonical_path"],
                    "session_label": label, "chunk_index": chunk_index,
                    "disposition": "archived",
                    "final_evidence": "copied+cataloged",
                    "storage_class": "container",
                    "observed_archived_size": r["expected_size"],
                    "writer_completed": True, "catalog_committed": True,
                } for r in records])


class SafetyTests(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.root = self._tmp.name
        self.addCleanup(self._tmp.cleanup)
        build_artifacts(self.root)

    def test_production_by_name_is_refused(self):
        with self.assertRaises(RebuildRefused):
            CatalogRebuilder(self.root, FakeShadowDb("lto_prod"),
                             shadow_name="lto_prod",
                             production_name="lto_prod")

    def test_production_by_identity_is_refused(self):
        """Named a shadow, but the connection IS production."""
        with self.assertRaises(RebuildRefused):
            CatalogRebuilder(self.root, FakeShadowDb("lto_prod"),
                             shadow_name="shadow_db",
                             production_name="lto_prod")

    def test_identity_mismatch_is_refused(self):
        with self.assertRaises(RebuildRefused):
            CatalogRebuilder(self.root, FakeShadowDb("some_other_db"),
                             shadow_name="shadow_db")

    def test_a_non_empty_shadow_is_refused(self):
        with self.assertRaises(RebuildRefused):
            CatalogRebuilder(self.root,
                             FakeShadowDb("shadow_db", empty=False),
                             shadow_name="shadow_db")

    def test_an_unnamed_shadow_is_refused(self):
        with self.assertRaises(RebuildRefused):
            CatalogRebuilder(self.root, FakeShadowDb("x"), shadow_name="  ")


class DiscoveryTests(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.root = self._tmp.name
        self.addCleanup(self._tmp.cleanup)
        build_artifacts(self.root)

    def test_ready_artifacts_are_found_by_stable_identity(self):
        found = discover_artifacts(self.root)
        self.assertIn(LABEL, found.session_descriptors)
        self.assertIn((LABEL, 113), found.plans)
        self.assertIn((LABEL, 113), found.terminals)

    def test_part_files_are_skipped_not_parsed(self):
        stray = os.path.join(self.root, "plan_manifests",
                             f"session_{LABEL}", "chunk_000113",
                             "plan.plan-manifest-v1.g0001.jsonl.zst.part")
        os.makedirs(os.path.dirname(stray), exist_ok=True)
        with open(stray, "wb") as handle:
            handle.write(b"not a real artifact")
        found = discover_artifacts(self.root)
        self.assertEqual(len(found.skipped_parts), 1)
        self.assertIn((LABEL, 113), found.plans)

    def test_a_tape_locator_is_never_opened(self):
        found = discover_artifacts(self.root, tape_root="Z:\\")
        self.assertEqual(found.skipped_tape_locators, [])


class RebuildTests(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.root = self._tmp.name
        self.addCleanup(self._tmp.cleanup)
        build_artifacts(self.root)
        self.db = FakeShadowDb("shadow_db")
        self.rebuilder = CatalogRebuilder(self.root, self.db,
                                          shadow_name="shadow_db",
                                          production_name="lto_prod")

    def test_rebuild_reconstructs_the_session(self):
        report = self.rebuilder.rebuild()
        self.assertEqual(len(report["sessions"]), 1)
        session = report["sessions"][0]
        self.assertEqual(session["session_label"], LABEL)
        self.assertEqual(session["chunks"], 1)
        self.assertEqual(session["scan_state_generations"], 1)
        self.assertEqual(report["tape_reads"], 0)

    def test_rebuild_writes_through_the_repository(self):
        self.rebuilder.rebuild()
        self.assertEqual(len(self.db.written), 1)

    def test_dry_run_writes_nothing(self):
        report = self.rebuilder.rebuild(dry_run=True)
        self.assertTrue(report["dry_run"])
        self.assertEqual(self.db.written, [])

    def test_rebuild_is_idempotent_in_its_report(self):
        first = self.rebuilder.rebuild(dry_run=True)
        second = self.rebuilder.rebuild(dry_run=True)
        self.assertEqual(first["sessions"][0]["chunks"],
                         second["sessions"][0]["chunks"])

    def test_a_missing_scan_state_generation_stops_the_rebuild(self):
        locator = ssm.scan_state_locator(LABEL, generation=0)
        os.remove(os.path.join(self.root, locator.replace("/", os.sep)))
        with self.assertRaises(RebuildIncomplete):
            self.rebuilder.rebuild()

    def test_a_session_without_a_descriptor_cannot_be_rebuilt(self):
        with TemporaryDirectory() as bare:
            rebuilder = CatalogRebuilder(bare, FakeShadowDb("shadow_db"),
                                         shadow_name="shadow_db")
            with self.assertRaises(RebuildIncomplete):
                rebuilder.rebuild()

    def test_coarse_legacy_chunks_are_reported_as_ineligible(self):
        with TemporaryDirectory() as root:
            build_artifacts(root, coarse=True)
            rebuilder = CatalogRebuilder(root, FakeShadowDb("shadow_db"),
                                         shadow_name="shadow_db")
            report = rebuilder.rebuild(dry_run=True)
            self.assertEqual(report["sessions"][0]["coarse_chunks"], 1)

    def test_no_tape_access_occurs_during_rebuild(self):
        self.rebuilder.rebuild()
        self.assertEqual(self.db.tape_reads, 0)


class ComparisonTests(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.root = self._tmp.name
        self.addCleanup(self._tmp.cleanup)
        build_artifacts(self.root)
        rebuilder = CatalogRebuilder(self.root, FakeShadowDb("shadow_db"),
                                     shadow_name="shadow_db")
        self.view = canonical_session_view(
            rebuilder.rebuild(dry_run=True)["sessions"])

    def test_identical_catalogs_compare_equal(self):
        result = compare_catalogs(self.view, self.view)
        self.assertTrue(result["equivalent"], result["discrepancies"])
        self.assertEqual(result["tape_reads"], 0)

    def test_generated_ids_are_ignored(self):
        other = canonical_session_view([{
            "session_label": LABEL, "scan_complete": False,
            "_chunks": [{
                "chunk_index": 113, "packaging_format": "stored_tar",
                "provenance_kind": "frontier", "record_count": 3,
                "byte_total": 303,
                "dispositions": {"archived": 3},
                # Different generated ids entirely; none are compared.
                "plan_records": [{
                    "plan_ordinal": i,
                    "canonical_path": f"/vault/D/shared-sets/f{i}",
                    "expected_size": 100 + i, "container_ordinal": 0,
                    "routing_precision": "exact",
                    "artifact_id": 999, "container_id": 888,
                } for i in range(3)]}]}])
        self.assertTrue(compare_catalogs(self.view, other)["equivalent"])

    def test_a_missing_path_fails_the_comparison(self):
        broken = _mutate(self.view, drop_ordinal=1)
        result = compare_catalogs(self.view, broken)
        self.assertFalse(result["equivalent"])
        self.assertTrue(any(d["kind"] == "entry_missing_in_rebuild"
                            for d in result["discrepancies"]))

    def test_a_same_size_path_substitution_is_detected(self):
        """Same ordinal, same size, different path: caught by identity."""
        broken = _mutate(self.view, rename_ordinal=(1, "/vault/other"))
        result = compare_catalogs(self.view, broken)
        self.assertFalse(result["equivalent"])
        self.assertTrue(any(d.get("field") == "canonical_path"
                            for d in result["discrepancies"]))

    def test_a_wrong_size_fails_the_comparison(self):
        broken = _mutate(self.view, resize_ordinal=(0, 999999))
        self.assertFalse(compare_catalogs(self.view, broken)["equivalent"])

    def test_a_format_difference_fails_the_comparison(self):
        broken = _mutate(self.view, packaging_format="zip")
        result = compare_catalogs(self.view, broken)
        self.assertFalse(result["equivalent"])
        self.assertTrue(any(d.get("field") == "packaging_format"
                            for d in result["discrepancies"]))

    def test_a_disposition_difference_fails_the_comparison(self):
        broken = _mutate(self.view, dispositions={"archived": 2,
                                                  "source_missing": 1})
        self.assertFalse(compare_catalogs(self.view, broken)["equivalent"])

    def test_a_missing_chunk_fails_the_comparison(self):
        broken = {LABEL: {"scan_complete": False, "chunks": []}}
        result = compare_catalogs(self.view, broken)
        self.assertFalse(result["equivalent"])
        self.assertTrue(any(d["kind"] == "chunk_missing_in_rebuild"
                            for d in result["discrepancies"]))

    def test_a_missing_session_fails_the_comparison(self):
        result = compare_catalogs(self.view, {})
        self.assertFalse(result["equivalent"])

    def test_the_residual_risk_is_always_reported(self):
        result = compare_catalogs(self.view, self.view)
        self.assertIn("same-size", result["residual_risk"])

    def test_routing_precision_difference_is_detected(self):
        broken = _mutate(self.view, precision_ordinal=(0, "coarse"))
        self.assertFalse(compare_catalogs(self.view, broken)["equivalent"])


def _mutate(view, *, drop_ordinal=None, rename_ordinal=None,
            resize_ordinal=None, packaging_format=None, dispositions=None,
            precision_ordinal=None):
    import copy

    out = copy.deepcopy(view)
    for session in out.values():
        for chunk in session["chunks"]:
            if packaging_format is not None:
                chunk["packaging_format"] = packaging_format
            if dispositions is not None:
                chunk["dispositions"] = dispositions
            entries = chunk["entries"]
            if drop_ordinal is not None:
                chunk["entries"] = [e for e in entries
                                    if e["plan_ordinal"] != drop_ordinal]
            for entry in chunk["entries"]:
                if rename_ordinal and entry["plan_ordinal"] == rename_ordinal[0]:
                    entry["canonical_path"] = rename_ordinal[1]
                if resize_ordinal and entry["plan_ordinal"] == resize_ordinal[0]:
                    entry["expected_size"] = resize_ordinal[1]
                if (precision_ordinal
                        and entry["plan_ordinal"] == precision_ordinal[0]):
                    entry["routing_precision"] = precision_ordinal[1]
    return out


if __name__ == "__main__":                   # pragma: no cover
    unittest.main()


class CoarseEquivalenceTests(unittest.TestCase):
    """Coarse routing is reportable but never rebuild-equivalent."""

    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.root = self._tmp.name
        self.addCleanup(self._tmp.cleanup)
        build_artifacts(self.root, coarse=True)
        rebuilder = CatalogRebuilder(self.root, FakeShadowDb("shadow_db"),
                                     shadow_name="shadow_db")
        self.view = canonical_session_view(
            rebuilder.rebuild(dry_run=True)["sessions"])

    def test_a_coarse_scope_is_not_equivalent_even_when_identical(self):
        result = compare_catalogs(self.view, self.view)
        self.assertFalse(result["equivalent"])
        self.assertTrue(any(
            d["kind"] == "coarse_routing_is_not_rebuild_equivalent"
            for d in result["discrepancies"]))

    def test_the_coarse_chunks_are_named(self):
        result = compare_catalogs(self.view, self.view)
        self.assertEqual(result["coarse_chunks"], [(LABEL, 113)])

    def test_an_explicit_exception_records_rather_than_hides_it(self):
        result = compare_catalogs(self.view, self.view, allow_coarse=True)
        self.assertTrue(result["equivalent"])
        self.assertTrue(any(
            d["kind"] == "coarse_routing_accepted_by_exception"
            for d in result["discrepancies"]))

    def test_an_exact_scope_is_equivalent_without_an_exception(self):
        with TemporaryDirectory() as exact_root:
            build_artifacts(exact_root)
            rebuilder = CatalogRebuilder(exact_root,
                                         FakeShadowDb("shadow_db"),
                                         shadow_name="shadow_db")
            view = canonical_session_view(
                rebuilder.rebuild(dry_run=True)["sessions"])
            self.assertTrue(compare_catalogs(view, view)["equivalent"])
