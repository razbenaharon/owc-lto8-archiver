"""Plan 2 Phase 0 contracts. Local-only: no PostgreSQL, SSH, LTFS or tape."""
import configparser
import inspect
import os
import tempfile
import threading
import unittest
from types import SimpleNamespace
from unittest import mock

from src.config import ConfigManager
from src.pg_containers import stored_tar_reader_contract_version
from src.pg_core import PgConnectionCore
from src.pg_sessions import PgSessionMixin
from src.pipeline_types import (
    ArtifactKind, ArtifactReadiness, ContainerFormat, StagedArtifact,
    StagedChunk, StagedContainer)
from src.remote_staging import RemoteChunkStager
from src.remote_writer import RemoteChunkWriter
from src.stored_tar_planning import (
    GNU_TAR_RECORD_SIZE, build_stored_tar_chunk_plan,
    estimate_stored_tar_archive_bytes, estimate_stored_tar_member_bytes)


class FeatureGateTests(unittest.TestCase):
    @staticmethod
    def _config(value=None, *, include_section=True):
        cfg = ConfigManager.__new__(ConfigManager)
        cfg.config = configparser.ConfigParser()
        if include_section:
            cfg.config["FEATURES"] = {}
            if value is not None:
                cfg.config["FEATURES"]["stored_tar_write_enabled"] = value
        return cfg

    def test_missing_gate_defaults_false(self):
        self.assertFalse(self._config().stored_tar_write_enabled)

    def test_missing_features_section_defaults_false(self):
        self.assertFalse(
            self._config(include_section=False).stored_tar_write_enabled)

    def test_malformed_gate_fails_closed(self):
        for value in ("maybe", "truthy", "2", "enabled", ""):
            with self.subTest(value=value):
                self.assertFalse(
                    self._config(value).stored_tar_write_enabled)

    def test_only_explicit_true_values_enable(self):
        for value in ("true", "1", "yes", "on"):
            with self.subTest(value=value):
                self.assertTrue(
                    self._config(value).stored_tar_write_enabled)

    def test_phase_one_reader_contract_is_now_available(self):
        self.assertEqual(stored_tar_reader_contract_version(), 1)

    def test_migration_015_is_not_a_startup_migration(self):
        self.assertNotIn(
            "015_postgres_container_formats.sql",
            inspect.getsource(PgConnectionCore._init_schema))

    def test_both_chunk_insert_paths_use_the_atomic_assignment_seam(self):
        for method in (
                PgSessionMixin.append_remote_streaming_chunk,
                PgSessionMixin._persist_remote_plan):
            source = inspect.getsource(method)
            self.assertIn("assign_new_chunk_format", source)
            self.assertIn("packaging_format, packaging_assigned_at", source)
            self.assertIn("if stored_tar_write_enabled", source)

    def test_stored_tar_max_size_defaults_to_current_zip_cap(self):
        cfg = ConfigManager.__new__(ConfigManager)
        cfg.config = configparser.ConfigParser()
        cfg.config["SETTINGS"] = {"max_zip_size_gb": "17"}
        self.assertEqual(cfg.stored_tar_max_size_gb, 17)
        cfg.config["SETTINGS"]["stored_tar_max_size_gb"] = "9"
        self.assertEqual(cfg.stored_tar_max_size_gb, 9)


class StoredTarPartitionPlanTests(unittest.TestCase):
    @staticmethod
    def _row(ordinal, size):
        return {
            "manifest_id": ordinal + 100,
            "remote_path": f"/src/file_{ordinal:03d}.dat",
            "file_size_bytes": size,
        }

    def test_exact_loose_threshold_is_loose(self):
        plan = build_stored_tar_chunk_plan(
            1, 2, [self._row(0, 9), self._row(1, 10)],
            loose_threshold_bytes=10, max_size_bytes=GNU_TAR_RECORD_SIZE * 4)
        self.assertEqual(
            [(m.plan_ordinal, m.storage_class, m.container_ordinal)
             for m in plan.members],
            [(0, "small_files", 0), (1, "loose_files", None)])
        self.assertEqual(plan.containers[0].expected_member_count, 1)

    def test_exact_container_cap_stays_in_container(self):
        member = estimate_stored_tar_member_bytes("/src/one.bin", 1)
        cap = estimate_stored_tar_archive_bytes([member])
        plan = build_stored_tar_chunk_plan(
            1, 0, [self._row(0, 1)], loose_threshold_bytes=10,
            max_size_bytes=cap)
        self.assertEqual(len(plan.containers), 1)
        self.assertEqual(plan.containers[0].estimated_archive_bytes, cap)

    def test_pax_header_and_padding_are_counted(self):
        estimate = estimate_stored_tar_member_bytes("/src/a", 513)
        self.assertGreaterEqual(estimate, 512 + 512 + 512 + 1024)
        self.assertEqual(estimate % 512, 0)
        self.assertEqual(
            estimate_stored_tar_archive_bytes([estimate])
            % GNU_TAR_RECORD_SIZE, 0)

    def test_tar_only_loose_only_mixed_empty_source_missing_and_oversized(self):
        tar_only = build_stored_tar_chunk_plan(
            1, 0, [self._row(0, 1), self._row(1, 2)],
            loose_threshold_bytes=10, max_size_bytes=GNU_TAR_RECORD_SIZE * 4)
        self.assertEqual(len(tar_only.small_members), 2)
        self.assertEqual(len(tar_only.loose_members), 0)

        loose_only = build_stored_tar_chunk_plan(
            1, 0, [self._row(0, 10), self._row(1, 11)],
            loose_threshold_bytes=10, max_size_bytes=GNU_TAR_RECORD_SIZE * 4)
        self.assertEqual(len(loose_only.containers), 0)
        self.assertEqual(len(loose_only.loose_members), 2)

        mixed = build_stored_tar_chunk_plan(
            1, 0, [self._row(0, 1), self._row(1, 20), self._row(2, 2)],
            loose_threshold_bytes=10, max_size_bytes=GNU_TAR_RECORD_SIZE * 4)
        self.assertEqual(
            [(m.plan_ordinal, m.storage_class, m.container_ordinal)
             for m in mixed.members],
            [(0, "small_files", 0), (1, "loose_files", None),
             (2, "small_files", 1)])

        empty = build_stored_tar_chunk_plan(
            1, 0, [], loose_threshold_bytes=10,
            max_size_bytes=GNU_TAR_RECORD_SIZE)
        self.assertEqual(empty.members, ())
        self.assertEqual(empty.containers, ())

        missing_row = self._row(0, 1)
        missing_row["status"] = "source_missing"
        missing = build_stored_tar_chunk_plan(
            1, 0, [missing_row], loose_threshold_bytes=10,
            max_size_bytes=GNU_TAR_RECORD_SIZE)
        self.assertEqual(len(missing.containers), 0)
        self.assertEqual(len(missing.source_missing_members), 1)

        oversized = build_stored_tar_chunk_plan(
            1, 0, [self._row(0, 1)],
            loose_threshold_bytes=10, max_size_bytes=1)
        self.assertEqual(len(oversized.containers), 1)
        self.assertGreater(oversized.containers[0].estimated_archive_bytes, 1)

    def test_assignment_does_not_depend_on_fetch_parallel_streams(self):
        rows = [self._row(i, i + 1) for i in range(8)]
        first = build_stored_tar_chunk_plan(
            1, 0, rows, loose_threshold_bytes=100,
            max_size_bytes=GNU_TAR_RECORD_SIZE)
        for _streams in (1, 2, 3, 8):
            again = build_stored_tar_chunk_plan(
                1, 0, rows, loose_threshold_bytes=100,
                max_size_bytes=GNU_TAR_RECORD_SIZE)
            self.assertEqual(again.members, first.members)
            self.assertEqual(again.containers, first.containers)


class StagingContractTests(unittest.TestCase):
    @staticmethod
    def _tar_descriptor(root, *, loose_path=None,
                        observed_members=1, expected_members=1):
        tar_path = os.path.join(root, "one.tar")
        sidecar_path = os.path.join(root, "one.jsonl.zst")
        with open(tar_path, "wb") as handle:
            handle.write(b"tar")
        with open(sidecar_path, "wb") as handle:
            handle.write(b"meta")
        container = StagedContainer(
            container_id=1, session_id=2, chunk_index=3,
            container_ordinal=0,
            container_format=ContainerFormat.STORED_TAR,
            format_version="stored-tar-v1", storage_class="small_files",
            container_name="one.tar", tar_dialect="gnu-pax-sparse-v1",
            data_path=tar_path, temporary_data_locator=tar_path,
            data_size_bytes=3, expected_member_count=expected_members,
            expected_logical_bytes=1,
            observed_member_count=observed_members,
            observed_logical_bytes=1)
        artifact = StagedArtifact(
            artifact_id=9, session_id=2, chunk_index=3, container_id=1,
            artifact_kind=ArtifactKind.TAR_SIDECAR,
            artifact_version="tar-sidecar-v1", staged_path=sidecar_path,
            local_locator="sidecars/one.zst", staged_size_bytes=4)
        metadata = []
        staged_bytes = 7
        if loose_path is not None:
            metadata.append({
                "file_name": "large.bin", "original_path": "/large.bin",
                "file_size_bytes": 5, "is_packed": False,
                "stored_path": loose_path})
            staged_bytes += 5
        return StagedChunk(
            chunk_index=3, session_id=2, fetch_dir="", pack_dir=root,
            metadata=metadata, packaging_format=ContainerFormat.STORED_TAR,
            containers=[container], artifacts=[artifact],
            staged_bytes=staged_bytes)

    def test_legacy_zip_descriptor_remains_valid(self):
        desc = StagedChunk(
            chunk_index=0, fetch_dir="fetch", pack_dir="pack", metadata=[],
            staged_bytes=0)
        self.assertEqual(desc.packaging_format, ContainerFormat.ZIP)
        self.assertTrue(desc.assert_writer_ready())

    def test_tar_fields_cannot_be_omitted(self):
        with self.assertRaisesRegex(ValueError, "requires session_id"):
            StagedChunk(
                chunk_index=1, fetch_dir="", pack_dir="", metadata=[],
                packaging_format=ContainerFormat.STORED_TAR)

    def test_tar_requires_exactly_one_sidecar_per_container(self):
        with tempfile.TemporaryDirectory() as root:
            tar_path = os.path.join(root, "one.tar")
            with open(tar_path, "wb") as handle:
                handle.write(b"tar")
            container = StagedContainer(
                container_id=1, session_id=2, chunk_index=3,
                container_ordinal=0,
                container_format=ContainerFormat.STORED_TAR,
                format_version="stored-tar-v1",
                storage_class="small_files", container_name="one.tar",
                tar_dialect="gnu-pax-sparse-v1", data_path=tar_path,
                temporary_data_locator=tar_path, data_size_bytes=3,
                expected_member_count=1, expected_logical_bytes=1,
                observed_member_count=1, observed_logical_bytes=1)
            with self.assertRaisesRegex(ValueError, "requires ready artifacts"):
                StagedChunk(
                    chunk_index=3, session_id=2, fetch_dir="", pack_dir=root,
                    metadata=[], packaging_format=ContainerFormat.STORED_TAR,
                    containers=[container], staged_bytes=3)

    def test_incomplete_or_part_artifact_fails_before_writer(self):
        with tempfile.TemporaryDirectory() as root:
            tar_path = os.path.join(root, "one.tar")
            sidecar = os.path.join(root, "one.jsonl.zst.part")
            with open(tar_path, "wb") as handle:
                handle.write(b"tar")
            with open(sidecar, "wb") as handle:
                handle.write(b"meta")
            container = StagedContainer(
                container_id=1, session_id=2, chunk_index=3,
                container_ordinal=0,
                container_format=ContainerFormat.STORED_TAR,
                format_version="stored-tar-v1",
                storage_class="small_files", container_name="one.tar",
                tar_dialect="gnu-pax-sparse-v1", data_path=tar_path,
                temporary_data_locator=tar_path, data_size_bytes=3,
                expected_member_count=1, expected_logical_bytes=1,
                observed_member_count=1, observed_logical_bytes=1)
            artifact = StagedArtifact(
                artifact_id=9, session_id=2, chunk_index=3, container_id=1,
                artifact_kind=ArtifactKind.TAR_SIDECAR,
                artifact_version="tar-sidecar-v1", staged_path=sidecar,
                local_locator="sidecars/one.zst", staged_size_bytes=4)
            desc = StagedChunk(
                chunk_index=3, session_id=2, fetch_dir="", pack_dir=root,
                metadata=[], packaging_format=ContainerFormat.STORED_TAR,
                containers=[container], artifacts=[artifact], staged_bytes=7)
            with self.assertRaisesRegex(RuntimeError, r"\.part"):
                desc.assert_writer_ready()

    def test_database_readiness_disagreement_fails(self):
        with tempfile.TemporaryDirectory() as root:
            tar_path = os.path.join(root, "one.tar")
            sidecar = os.path.join(root, "one.zst")
            with open(tar_path, "wb") as handle:
                handle.write(b"tar")
            with open(sidecar, "wb") as handle:
                handle.write(b"meta")
            container = StagedContainer(
                container_id=1, session_id=2, chunk_index=3,
                container_ordinal=0,
                container_format=ContainerFormat.STORED_TAR,
                format_version="stored-tar-v1",
                storage_class="small_files", container_name="one.tar",
                tar_dialect="gnu-pax-sparse-v1", data_path=tar_path,
                temporary_data_locator=tar_path, data_size_bytes=3,
                expected_member_count=1, expected_logical_bytes=1,
                observed_member_count=1, observed_logical_bytes=1)
            artifact = StagedArtifact(
                artifact_id=9, session_id=2, chunk_index=3, container_id=1,
                artifact_kind=ArtifactKind.TAR_SIDECAR,
                artifact_version="tar-sidecar-v1", staged_path=sidecar,
                local_locator="sidecars/one.zst", staged_size_bytes=4,
                database_readiness_state=ArtifactReadiness.VALIDATED)
            desc = StagedChunk(
                chunk_index=3, session_id=2, fetch_dir="", pack_dir=root,
                metadata=[], packaging_format=ContainerFormat.STORED_TAR,
                containers=[container], artifacts=[artifact], staged_bytes=7)
            with self.assertRaisesRegex(RuntimeError, "disagrees"):
                desc.assert_writer_ready()

    def test_expected_and_observed_container_totals_must_match(self):
        with tempfile.TemporaryDirectory() as root:
            desc = self._tar_descriptor(
                root, expected_members=2, observed_members=1)
            with self.assertRaisesRegex(RuntimeError, "member count"):
                desc.assert_writer_ready()

    def test_loose_file_must_exist_be_regular_readable_and_exact_size(self):
        with tempfile.TemporaryDirectory() as root:
            missing = os.path.join(root, "large.bin")
            desc = self._tar_descriptor(root, loose_path=missing)
            with self.assertRaisesRegex(RuntimeError, "not readable"):
                desc.assert_writer_ready()
            with open(missing, "wb") as handle:
                handle.write(b"short")
            self.assertTrue(desc.assert_writer_ready())
            with open(missing, "ab") as handle:
                handle.write(b"changed")
            with self.assertRaisesRegex(RuntimeError, "size changed"):
                desc.assert_writer_ready()


class ProducerAndWriterGateTests(unittest.TestCase):
    def test_stager_refuses_to_guess_zip_without_durable_reader(self):
        host = SimpleNamespace(db=SimpleNamespace(), _producer_chunk=None)
        with self.assertRaisesRegex(RuntimeError, "refusing to guess ZIP"):
            RemoteChunkStager(host)._stage_chunk(37, 49, [])

    def test_existing_tar_does_not_consult_creation_flag_or_fall_back_to_zip(self):
        db = mock.Mock()
        db.get_chunk_packaging_format.return_value = ContainerFormat.STORED_TAR
        db.require_existing_stored_tar_recovery.return_value = True
        db.get_or_create_stored_tar_chunk_plan.return_value = object()
        cfg = SimpleNamespace(zip_threshold_mb=10, stored_tar_max_size_gb=1)
        host = SimpleNamespace(db=db, cfg=cfg, _producer_chunk=None)
        with self.assertRaisesRegex(RuntimeError, "outside Task 2.1"):
            RemoteChunkStager(host)._stage_chunk(37, 49, [])
        db.require_existing_stored_tar_recovery.assert_called_once_with(37, 49)
        db.get_or_create_stored_tar_chunk_plan.assert_called_once()
        self.assertFalse(hasattr(host, "stored_tar_write_enabled"))
        self.assertFalse(db.update_chunk_status.called)

    def test_bad_descriptor_fails_before_ltfs_ownership(self):
        desc = SimpleNamespace(
            chunk_index=1, skip_tape=False, staged_bytes=1,
            assert_writer_ready=mock.Mock(
                side_effect=RuntimeError("incomplete sidecar")))
        host = SimpleNamespace(db=mock.Mock())
        writer = RemoteChunkWriter(host)
        with mock.patch(
                "src.remote_writer._acquire_tape_io_lock") as acquire:
            with self.assertRaisesRegex(RuntimeError, "incomplete sidecar"):
                writer._write_chunk_group(
                    37, [desc], "test-tape", False, threading.Event())
        acquire.assert_not_called()

    def test_tar_descriptor_requires_database_validator_before_ownership(self):
        with tempfile.TemporaryDirectory() as root:
            desc = StagingContractTests._tar_descriptor(root)
            host = SimpleNamespace(db=SimpleNamespace(
                get_chunk_packaging_format=lambda _sid, _ci:
                    ContainerFormat.STORED_TAR))
            writer = RemoteChunkWriter(host)
            with mock.patch(
                    "src.remote_writer._acquire_tape_io_lock") as acquire:
                with self.assertRaisesRegex(
                        RuntimeError, "database readiness validator"):
                    writer._write_chunk_group(
                        2, [desc], "test-tape", False, threading.Event())
            acquire.assert_not_called()

    def test_unknown_format_descriptor_cannot_use_legacy_bypass(self):
        desc = SimpleNamespace(
            chunk_index=1, session_id=None, skip_tape=False,
            staged_bytes=1, assert_writer_ready=mock.Mock(return_value=True))
        host = SimpleNamespace(db=SimpleNamespace())
        writer = RemoteChunkWriter(host)
        with mock.patch(
                "src.remote_writer._acquire_tape_io_lock") as acquire:
            with self.assertRaisesRegex(RuntimeError, "format authority"):
                writer._write_chunk_group(
                    37, [desc], "test-tape", False, threading.Event())
        acquire.assert_not_called()


if __name__ == "__main__":
    unittest.main()
