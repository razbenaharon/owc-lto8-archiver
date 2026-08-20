"""Plan 3, Task 1.2 - the four versioned artifact schemas.

These tests are adversarial on purpose. Every rule the plan states as a
conditional ("a frontier record REQUIRES...", "a legacy container member MAY
omit ... ONLY WITH coarse") gets a case that violates it, because a validator
that is never shown a bad record is not known to reject one.
"""
import io
import os
import unittest
from tempfile import TemporaryDirectory

import zstandard as zstd

from src.archive_artifacts import (
    ArtifactConflict,
    ArtifactError,
    PLAN_MANIFEST_VERSION,
    Plan3ArtifactWriter,
    plan3_artifact_locator,
    read_plan3_artifact,
    resolve_locator,
)
from src import plan_manifest as pm
from src import terminal_manifest as tm
from src import scan_state_manifest as ssm

LABEL = "REMOTE_srv01_20260709_234150"


def frontier_record(ordinal, *, path=None, **over):
    record = {
        "canonical_path": path or f"/vault/D/shared-sets/x/f{ordinal}",
        "expected_size": 100 + ordinal,
        "plan_ordinal": ordinal,
        "session_label": LABEL,
        "chunk_index": 113,
        "provenance_kind": "frontier",
        "scan_segment_id": "seg-0001",
        "scan_ordinal": 5000 + ordinal,
        "storage_class": "container",
        "container_format": "stored_tar",
        "container_ordinal": 0,
        "routing_precision": "exact",
    }
    record.update(over)
    return record


def legacy_rec(ordinal, **over):
    record = {
        "canonical_path": f"/vault/D/shared-sets/y/f{ordinal}",
        "expected_size": 200 + ordinal,
        "plan_ordinal": ordinal,
        "normalized_chunk_ordinal": ordinal,
        "session_label": LABEL,
        "chunk_index": 7,
        "provenance_kind": "legacy_db_export",
        "legacy_source_plan_file_id": 900000 + ordinal,
        "legacy_original_ordinal": ordinal,
        "source_snapshot_id": 42,
        "storage_class": "container",
        "container_format": "zip",
        "container_ordinal": 0,
        "routing_precision": "exact",
    }
    record.update(over)
    return record


class PlanManifestSchemaTests(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.root = self._tmp.name
        self.addCleanup(self._tmp.cleanup)

    def write_frontier(self, records, **kw):
        return pm.write_plan_manifest(
            self.root, session_label=LABEL, chunk_index=113,
            provenance_kind="frontier", records=records, **kw)

    # -- happy paths ------------------------------------------------------
    def test_frontier_roundtrip_is_exact(self):
        published = self.write_frontier([frontier_record(i) for i in range(3)])
        self.assertEqual(published.record_count, 3)
        self.assertEqual(published.byte_total, 100 + 101 + 102)
        self.assertTrue(published.size_bytes > 0)
        header, records, trailer = pm.read_plan_manifest(
            self.root, published.locator)
        self.assertEqual(header["provenance_kind"], "frontier")
        self.assertEqual([r["plan_ordinal"] for r in records], [0, 1, 2])
        self.assertEqual(trailer["record_count"], 3)
        self.assertTrue(pm.is_authoritative(header, records))

    def test_loose_record_has_no_container_identity(self):
        published = self.write_frontier([
            frontier_record(0, storage_class="loose", container_format=None,
                            container_ordinal=None)])
        _h, records, _t = pm.read_plan_manifest(self.root, published.locator)
        self.assertIsNone(records[0]["container_format"])
        self.assertIsNone(records[0]["container_ordinal"])

    def test_legacy_export_may_have_null_scan_segment_identity(self):
        published = pm.write_plan_manifest(
            self.root, session_label=LABEL, chunk_index=7,
            provenance_kind="legacy_db_export",
            records=[legacy_rec(0), legacy_rec(1)])
        _h, records, _t = pm.read_plan_manifest(self.root, published.locator)
        self.assertIsNone(records[0].get("scan_segment_id"))
        self.assertEqual(records[0]["legacy_source_plan_file_id"], 900000)

    def test_legacy_coarse_member_may_omit_container_ordinal(self):
        published = pm.write_plan_manifest(
            self.root, session_label=LABEL, chunk_index=7,
            provenance_kind="legacy_db_export",
            records=[legacy_rec(0, container_ordinal=None,
                                routing_precision="coarse")])
        header, records, _t = pm.read_plan_manifest(self.root, published.locator)
        self.assertFalse(pm.is_authoritative(header, records))

    # -- conditional rule violations --------------------------------------
    def test_frontier_without_scan_segment_is_refused(self):
        with self.assertRaises(ArtifactError):
            self.write_frontier([frontier_record(0, scan_segment_id=None)])

    def test_frontier_without_scan_ordinal_is_refused(self):
        with self.assertRaises(ArtifactError):
            self.write_frontier([frontier_record(0, scan_ordinal=None)])

    def test_frontier_may_not_be_coarse(self):
        with self.assertRaises(ArtifactError):
            self.write_frontier([frontier_record(0, routing_precision="coarse")])

    def test_frontier_container_member_needs_container_ordinal(self):
        with self.assertRaises(ArtifactError):
            self.write_frontier([frontier_record(0, container_ordinal=None)])

    def test_loose_record_claiming_a_container_is_refused(self):
        with self.assertRaises(ArtifactError):
            self.write_frontier([
                frontier_record(0, storage_class="loose",
                                container_format="zip", container_ordinal=None)])

    def test_legacy_exact_member_must_carry_container_ordinal(self):
        with self.assertRaises(ArtifactError):
            pm.write_plan_manifest(
                self.root, session_label=LABEL, chunk_index=7,
                provenance_kind="legacy_db_export",
                records=[legacy_rec(0, container_ordinal=None,
                                    routing_precision="exact")])

    def test_legacy_without_plan_file_id_is_refused(self):
        with self.assertRaises(ArtifactError):
            pm.write_plan_manifest(
                self.root, session_label=LABEL, chunk_index=7,
                provenance_kind="legacy_db_export",
                records=[legacy_rec(0, legacy_source_plan_file_id=None)])

    def test_normalized_ordinal_must_equal_plan_ordinal(self):
        with self.assertRaises(ArtifactError):
            pm.write_plan_manifest(
                self.root, session_label=LABEL, chunk_index=7,
                provenance_kind="legacy_db_export",
                records=[legacy_rec(0, normalized_chunk_ordinal=5)])

    def test_coarse_artifact_cannot_be_published_as_authoritative(self):
        with self.assertRaises(ArtifactError):
            pm.write_plan_manifest(
                self.root, session_label=LABEL, chunk_index=7,
                provenance_kind="legacy_db_export",
                records=[legacy_rec(0, container_ordinal=None,
                                    routing_precision="coarse")],
                extra_header={"routing_authority": "authoritative"})

    def test_duplicate_ordinal_is_refused(self):
        with self.assertRaises(ArtifactError):
            self.write_frontier([frontier_record(0), frontier_record(0)])

    def test_duplicate_path_is_refused(self):
        with self.assertRaises(ArtifactError):
            self.write_frontier([
                frontier_record(0, path="/same"),
                frontier_record(1, path="/same")])

    def test_ordinals_must_be_dense_from_zero(self):
        with self.assertRaises(ArtifactError):
            self.write_frontier([frontier_record(1), frontier_record(2)])

    def test_record_disagreeing_with_header_identity_is_refused(self):
        with self.assertRaises(ArtifactError):
            self.write_frontier([frontier_record(0, chunk_index=999)])

    def test_mixed_provenance_in_one_artifact_is_refused(self):
        with self.assertRaises(ArtifactError):
            self.write_frontier([frontier_record(0),
                                 legacy_rec(1, chunk_index=113)])

    def test_unknown_version_fails_closed(self):
        published = self.write_frontier([frontier_record(0)])
        with self.assertRaises(ArtifactError):
            read_plan3_artifact(self.root, published.locator,
                                expected_version="plan-manifest-v99")


class PublicationProtocolTests(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.root = self._tmp.name
        self.addCleanup(self._tmp.cleanup)

    def test_publication_is_no_clobber(self):
        pm.write_plan_manifest(
            self.root, session_label=LABEL, chunk_index=1,
            provenance_kind="frontier",
            records=[frontier_record(0, chunk_index=1)])
        with self.assertRaises(ArtifactConflict):
            pm.write_plan_manifest(
                self.root, session_label=LABEL, chunk_index=1,
                provenance_kind="frontier",
                records=[frontier_record(0, chunk_index=1)])

    def test_a_part_file_is_never_published_and_never_read(self):
        locator = plan3_artifact_locator(
            "plan_manifests", LABEL, chunk_index=2, kind="plan",
            version=PLAN_MANIFEST_VERSION)
        writer = Plan3ArtifactWriter(
            self.root, locator, version=PLAN_MANIFEST_VERSION,
            header={"session_label": LABEL, "chunk_index": 2},
            validate_record=pm.validate_plan_record)
        writer.open()
        writer.add(frontier_record(0, chunk_index=2))
        writer.close(publish=False)
        self.assertTrue(os.path.isfile(writer.part_path))
        self.assertFalse(os.path.exists(resolve_locator(self.root, locator)))
        with self.assertRaises(ArtifactError):
            read_plan3_artifact(self.root, locator + ".part",
                                expected_version=PLAN_MANIFEST_VERSION)

    def test_abandon_removes_the_part(self):
        locator = plan3_artifact_locator(
            "plan_manifests", LABEL, chunk_index=3, kind="plan",
            version=PLAN_MANIFEST_VERSION)
        writer = Plan3ArtifactWriter(
            self.root, locator, version=PLAN_MANIFEST_VERSION,
            header={"session_label": LABEL, "chunk_index": 3},
            validate_record=pm.validate_plan_record)
        writer.open()
        writer.add(frontier_record(0, chunk_index=3))
        writer.abandon()
        self.assertFalse(os.path.exists(writer.part_path))

    def test_truncated_zstd_is_refused(self):
        published = pm.write_plan_manifest(
            self.root, session_label=LABEL, chunk_index=4,
            provenance_kind="frontier",
            records=[frontier_record(0, chunk_index=4)])
        path = resolve_locator(self.root, published.locator)
        data = open(path, "rb").read()
        with open(path, "wb") as handle:
            handle.write(data[: len(data) // 2])
        with self.assertRaises(ArtifactError):
            pm.read_plan_manifest(self.root, published.locator)

    def test_missing_trailer_is_refused(self):
        locator = plan3_artifact_locator(
            "plan_manifests", LABEL, chunk_index=5, kind="plan",
            version=PLAN_MANIFEST_VERSION)
        path = resolve_locator(self.root, locator)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as raw:
            comp = zstd.ZstdCompressor().stream_writer(raw)
            text = io.TextIOWrapper(comp, encoding="utf-8")
            text.write('{"kind":"header","version":"plan-manifest-v1"}\n')
            text.flush()
            text.detach()
            comp.close()
        with self.assertRaises(ArtifactError):
            pm.read_plan_manifest(self.root, locator)

    def test_locator_is_keyed_by_stable_label_not_numeric_id(self):
        locator = pm.plan_manifest_locator(LABEL, 113, generation=2)
        self.assertIn(f"session_{LABEL}", locator)
        self.assertIn("chunk_000113", locator)
        self.assertIn("g0002", locator)
        self.assertTrue(locator.endswith(".jsonl.zst"))

    def test_locator_rejects_traversal_in_a_label(self):
        locator = pm.plan_manifest_locator("../../evil", 0)
        # The label is sanitized, so the resolved path stays under the root.
        resolved = resolve_locator(self.root, locator)
        self.assertTrue(resolved.startswith(os.path.abspath(self.root)))


class TerminalStateSchemaTests(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.root = self._tmp.name
        self.addCleanup(self._tmp.cleanup)

    def rec(self, ordinal, disposition="archived", **over):
        record = {
            "plan_ordinal": ordinal,
            "canonical_path": f"/vault/x/f{ordinal}",
            "session_label": LABEL,
            "chunk_index": 113,
            "disposition": disposition,
            "final_evidence": "container copied and cataloged",
            "storage_class": "container",
        }
        if disposition == "archived":
            record["observed_archived_size"] = 100 + ordinal
            record["writer_completed"] = True
            record["catalog_committed"] = True
        record.update(over)
        return record

    def write(self, records, **kw):
        return tm.write_terminal_manifest(
            self.root, session_label=LABEL, chunk_index=113,
            records=records, **kw)

    def test_every_disposition_roundtrips(self):
        records = [self.rec(i, d) for i, d in enumerate(tm.DISPOSITIONS)]
        published = self.write(records)
        header, read, _t = tm.read_terminal_manifest(
            self.root, published.locator)
        self.assertEqual(len(read), len(tm.DISPOSITIONS))
        self.assertEqual(header["disposition_counts"]["archived"], 1)

    def test_archived_requires_writer_completion(self):
        with self.assertRaises(ArtifactError):
            self.write([self.rec(0, writer_completed=False)])

    def test_archived_requires_catalog_commit(self):
        with self.assertRaises(ArtifactError):
            self.write([self.rec(0, catalog_committed=False)])

    def test_archived_loose_file_needs_its_full_restore_locator(self):
        with self.assertRaises(ArtifactError):
            self.write([self.rec(0, storage_class="loose")])

    def test_archived_loose_file_with_full_locator_is_accepted(self):
        published = self.write([self.rec(
            0, storage_class="loose", tape_label="Tape_02",
            tape_generation_id=2, tape_generation_value=1,
            stored_path="/loose/f0", archive_date="2026-07-24",
            archive_run_identity="2026-07-24:Tape_02",
            source_host="srv01", restore_locator="Tape_02:/loose/f0")])
        _h, records, _t = tm.read_terminal_manifest(self.root,
                                                    published.locator)
        self.assertEqual(records[0]["tape_label"], "Tape_02")

    def test_non_archived_may_not_report_an_archived_size(self):
        with self.assertRaises(ArtifactError):
            self.write([self.rec(0, "source_missing",
                                 observed_archived_size=5)])

    def test_ambiguous_writer_cannot_be_a_source_exception(self):
        with self.assertRaises(ArtifactError):
            self.write([self.rec(0, "source_missing", writer_ambiguous=True)])

    def test_disposition_without_evidence_is_refused(self):
        with self.assertRaises(ArtifactError):
            self.write([self.rec(0, "source_missing", final_evidence="")])

    def test_duplicate_ordinal_is_refused(self):
        with self.assertRaises(ArtifactError):
            self.write([self.rec(0), self.rec(0)])

    def test_missing_ordinal_blocks_publication(self):
        with self.assertRaises(ArtifactError):
            self.write([self.rec(0), self.rec(2)], plan_ordinals=[0, 1, 2])

    def test_extra_disposition_blocks_publication(self):
        with self.assertRaises(ArtifactError):
            self.write([self.rec(0), self.rec(1)], plan_ordinals=[0])

    def test_exactly_one_disposition_per_ordinal_is_publishable(self):
        ok, reason = tm.is_publishable([0, 1], [self.rec(0), self.rec(1)])
        self.assertTrue(ok, reason)

    def test_join_to_plan_detects_a_path_substitution(self):
        plan = [{"plan_ordinal": 0, "canonical_path": "/a"}]
        terminal = [self.rec(0, canonical_path="/b")]
        with self.assertRaises(ArtifactError):
            tm.join_to_plan(plan, terminal)

    def test_join_to_plan_matches_by_ordinal(self):
        plan = [{"plan_ordinal": 0, "canonical_path": "/vault/x/f0"}]
        joined = tm.join_to_plan(plan, [self.rec(0)])
        self.assertEqual(len(joined), 1)


class ScanStateSchemaTests(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.root = self._tmp.name
        self.addCleanup(self._tmp.cleanup)

    def directory(self, directory_id, **over):
        record = {
            "record_type": "directory",
            "session_label": LABEL,
            "directory_id": directory_id,
            "path": f"/vault/d{directory_id}",
            "parent_directory_id": None,
            "state": "listed",
            "listing_complete": True,
            "traversal_complete": False,
            "planning_complete": False,
            "child_count": 0,
        }
        record.update(over)
        return record

    def segment(self, segment_id="seg-1", first=0, last=99):
        return {
            "record_type": "segment", "session_label": LABEL,
            "segment_id": segment_id, "locator": "scan_segments/a.jsonl.zst",
            "first_ordinal": first, "last_ordinal": last,
        }

    def write(self, records, **kw):
        return ssm.write_scan_state_segment(
            self.root, session_label=LABEL, records=records, **kw)

    def test_empty_directory_is_preserved(self):
        published = self.write([self.directory(1, child_count=0)])
        _h, records, _t = ssm.read_scan_state_segment(
            self.root, published.locator)
        self.assertEqual(records[0]["child_count"], 0)

    def test_traversal_and_planning_state_are_independent(self):
        published = self.write([
            self.directory(1, state="traversed", traversal_complete=True,
                           planning_complete=False)])
        _h, records, _t = ssm.read_scan_state_segment(
            self.root, published.locator)
        self.assertTrue(records[0]["traversal_complete"])
        self.assertFalse(records[0]["planning_complete"])

    def test_planned_without_listed_is_refused(self):
        with self.assertRaises(ArtifactError):
            self.write([self.directory(1, listing_complete=False,
                                       planning_complete=True)])

    def test_missing_explicit_boolean_is_refused(self):
        record = self.directory(1)
        del record["traversal_complete"]
        with self.assertRaises(ArtifactError):
            self.write([record])

    def test_consumption_range_escaping_its_segment_is_refused(self):
        with self.assertRaises(ArtifactError):
            self.write([
                self.segment("seg-1", 0, 50),
                {"record_type": "consumption", "session_label": LABEL,
                 "segment_id": "seg-1", "chunk_index": 3,
                 "first_ordinal": 0, "last_ordinal": 80},
            ])

    def test_consumption_naming_an_unknown_segment_is_refused(self):
        with self.assertRaises(ArtifactError):
            self.write([
                {"record_type": "consumption", "session_label": LABEL,
                 "segment_id": "ghost", "chunk_index": 3,
                 "first_ordinal": 0, "last_ordinal": 1},
            ])

    def test_consumption_inside_its_segment_is_accepted(self):
        published = self.write([
            self.segment("seg-1", 0, 99),
            {"record_type": "consumption", "session_label": LABEL,
             "segment_id": "seg-1", "chunk_index": 3,
             "first_ordinal": 0, "last_ordinal": 49},
        ])
        _h, records, _t = ssm.read_scan_state_segment(
            self.root, published.locator)
        self.assertEqual(len(records), 2)

    def test_part_locator_is_never_ready_segment_evidence(self):
        with self.assertRaises(ArtifactError):
            self.write([{
                "record_type": "segment", "session_label": LABEL,
                "segment_id": "s", "locator": "scan_segments/a.jsonl.zst.part",
                "first_ordinal": 0, "last_ordinal": 1}])

    def test_coverage_final_requires_every_scope_final(self):
        with self.assertRaises(ArtifactError):
            self.write([{
                "record_type": "scope", "session_label": LABEL,
                "root_path": "/vault/D/shared-sets/DSETB",
                "coverage_state": "provisional"}], coverage_final=True)

    def test_scan_state_generations_are_append_only(self):
        first = self.write([self.directory(1)], generation=0)
        second = self.write([self.directory(2)], generation=1,
                            prior_locator=first.locator)
        self.assertNotEqual(first.locator, second.locator)
        header, _r, _t = ssm.read_scan_state_segment(self.root, second.locator)
        self.assertEqual(header["prior_artifact_locator"], first.locator)


class SessionDescriptorTests(unittest.TestCase):
    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.root = self._tmp.name
        self.addCleanup(self._tmp.cleanup)

    def generation(self, number):
        return {"record_type": "scan_state_generation", "session_label": LABEL,
                "generation": number,
                "locator": ssm.scan_state_locator(LABEL, generation=number)}

    def test_empty_session_is_still_describable(self):
        published = ssm.write_session_descriptor(
            self.root, session_label=LABEL, records=[], session_state="active")
        header, records, _t = ssm.read_session_descriptor(
            self.root, published.locator)
        self.assertEqual(records, [])
        self.assertEqual(header["session_label"], LABEL)

    def test_descriptor_names_the_ordered_generation_set(self):
        published = ssm.write_session_descriptor(
            self.root, session_label=LABEL,
            records=[self.generation(0), self.generation(1)])
        _h, records, _t = ssm.read_session_descriptor(
            self.root, published.locator)
        self.assertEqual([r["generation"] for r in records], [0, 1])

    def test_generation_gap_is_refused(self):
        with self.assertRaises(ArtifactError):
            ssm.write_session_descriptor(
                self.root, session_label=LABEL,
                records=[self.generation(0), self.generation(2)])

    def test_out_of_order_generations_are_refused(self):
        with self.assertRaises(ArtifactError):
            ssm.write_session_descriptor(
                self.root, session_label=LABEL,
                records=[self.generation(1), self.generation(0)])

    def test_duplicate_generation_is_refused(self):
        with self.assertRaises(ArtifactError):
            ssm.write_session_descriptor(
                self.root, session_label=LABEL,
                records=[self.generation(0), self.generation(0)])


class CrossArtifactTests(unittest.TestCase):
    """The three artifacts must collectively prove the chunk."""

    def setUp(self):
        self._tmp = TemporaryDirectory()
        self.root = self._tmp.name
        self.addCleanup(self._tmp.cleanup)

    def test_plan_and_terminal_agree_without_any_content_hash(self):
        plan = pm.write_plan_manifest(
            self.root, session_label=LABEL, chunk_index=113,
            provenance_kind="frontier",
            records=[frontier_record(i) for i in range(3)])
        _ph, plan_records, _pt = pm.read_plan_manifest(self.root, plan.locator)

        terminal_records = [{
            "plan_ordinal": r["plan_ordinal"],
            "canonical_path": r["canonical_path"],
            "session_label": LABEL, "chunk_index": 113,
            "disposition": "archived",
            "final_evidence": "copied+cataloged",
            "storage_class": "container",
            "observed_archived_size": r["expected_size"],
            "writer_completed": True, "catalog_committed": True,
        } for r in plan_records]
        terminal = tm.write_terminal_manifest(
            self.root, session_label=LABEL, chunk_index=113,
            records=terminal_records,
            plan_ordinals=[r["plan_ordinal"] for r in plan_records],
            plan_artifact_locator=plan.locator)

        _th, term_records, _tt = tm.read_terminal_manifest(
            self.root, terminal.locator)
        joined = tm.join_to_plan(plan_records, term_records)
        self.assertEqual(len(joined), 3)
        self.assertEqual(terminal.byte_total, plan.byte_total)

    def test_same_size_substitution_is_undetectable(self):
        """The documented residual risk, asserted so it stays documented."""
        a = pm.write_plan_manifest(
            self.root, session_label=LABEL, chunk_index=10,
            provenance_kind="frontier",
            records=[frontier_record(0, chunk_index=10, path="/vault/a")])
        _h, records, _t = pm.read_plan_manifest(self.root, a.locator)
        # Nothing in the record commits to content: only path, size, ordinal.
        self.assertEqual(
            set(records[0]) & {"sha256", "md5", "content_hash", "checksum"},
            set())


if __name__ == "__main__":                   # pragma: no cover
    unittest.main()
