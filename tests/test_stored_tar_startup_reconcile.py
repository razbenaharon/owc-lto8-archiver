import copy
import io
import json
import os
import tarfile
import tempfile
import unittest
from contextlib import ExitStack, contextmanager
from dataclasses import asdict
from types import SimpleNamespace
from unittest import mock

from src.archive_artifacts import (TAR_SIDECAR_VERSION,
                                   publish_stored_tar_pair,
                                   tar_sidecar_locator)
from src.pipeline_types import (ArtifactKind, ArtifactReadiness,
                                ContainerFormat, ContainerValidationState,
                                SourceDisposition, StagedArtifact,
                                StagedContainer)
from src.startup_reconcile import (TAR_RECONCILE_BLOCKED,
                                   TAR_RECONCILE_READY,
                                   TAR_RECONCILE_REBUILD,
                                   reconcile_tar_artifacts)
from src.tar_container import validate_stored_tar_part


GNU_RECORD_SIZE = 512 * 512


def _write_tar(path, payload=b"abc"):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with tarfile.open(path, "w", format=tarfile.PAX_FORMAT) as archive:
        info = tarfile.TarInfo("present")
        info.size = len(payload)
        info.mtime = 0
        archive.addfile(info, io.BytesIO(payload))
    size = os.path.getsize(path)
    with open(path, "ab") as handle:
        handle.write(bytes((-size) % GNU_RECORD_SIZE))


class _ReconcileDB:
    def __init__(self, container, plan):
        self.container = container
        self.plan = plan
        self.artifacts = []
        self.block_calls = []

    def get_archive_containers(self, _session_id, _chunk_index):
        return [copy.deepcopy(self.container)]

    def get_archive_artifacts(self, _session_id, _chunk_index):
        return copy.deepcopy(self.artifacts)

    def get_stored_tar_chunk_plan(self, _session_id, _chunk_index):
        return self.plan

    def reconcile_stored_tar_build_owner(
            self, container_id, old_owner, old_state, new_owner):
        assert self.container["container_id"] == container_id
        if (self.container["owner_token"] != old_owner
                or self.container["validation_state"] != old_state):
            raise RuntimeError("owner/state CAS failed")
        self.container["owner_token"] = new_owner
        return copy.deepcopy(self.container)

    def reset_reconciled_stored_tar_build(
            self, container_id, owner, expected_state):
        assert self.container["container_id"] == container_id
        if (self.container["owner_token"] != owner
                or self.container["validation_state"] != expected_state):
            raise RuntimeError("reset CAS failed")
        self.container.update({
            "validation_state": "planned", "owner_token": None,
            "validated_part_locator": None, "validation_summary": None,
            "disposition_counts": None, "observed_member_count": None,
            "observed_logical_bytes": None, "actual_artifact_bytes": None,
            "temporary_data_locator": None,
            "permanent_local_metadata_locator": None,
        })
        return copy.deepcopy(self.container)

    def mark_stored_tar_validated_part(
            self, container_id, owner, part, validation, diagnostics=()):
        assert self.container["container_id"] == container_id
        if self.container["owner_token"] != owner:
            raise RuntimeError("validation owner CAS failed")
        summary = asdict(validation)
        summary["source_diagnostics"] = [
            asdict(item) if not isinstance(item, dict) else dict(item)
            for item in diagnostics]
        for item in summary["source_diagnostics"]:
            disposition = item.get("disposition")
            if hasattr(disposition, "value"):
                item["disposition"] = disposition.value
        self.container.update({
            "validation_state": "validated_part",
            "validated_part_locator": part,
            "validation_summary": summary,
            "observed_member_count": validation.member_count,
            "observed_logical_bytes": validation.logical_bytes,
            "actual_artifact_bytes": validation.archive_size,
        })
        return copy.deepcopy(self.container)

    def publish_stored_tar_pair(self, **values):
        if self.container["validation_state"] == "ready":
            return {"container": copy.deepcopy(self.container),
                    "artifact": copy.deepcopy(self.artifacts[0])}
        if self.container["owner_token"] != values["owner_token"]:
            raise RuntimeError("paired publication owner CAS failed")
        self.container.update({
            "temporary_data_locator": values["temporary_data_locator"],
            "permanent_local_metadata_locator": values["sidecar_locator"],
            "actual_artifact_bytes": values["tar_size_bytes"],
            "observed_member_count": values["observed_member_count"],
            "observed_logical_bytes": values["observed_logical_bytes"],
            "disposition_counts": dict(values["disposition_counts"]),
            "validation_state": "ready", "owner_token": None,
        })
        artifact = {
            "artifact_id": 19, "session_id": 2, "chunk_index": 3,
            "container_id": self.container["container_id"],
            "artifact_kind": "tar_sidecar",
            "artifact_version": values["sidecar_version"],
            "local_locator": values["sidecar_locator"],
            "artifact_size_bytes": values["sidecar_size_bytes"],
            "readiness_state": "ready",
        }
        self.artifacts = [artifact]
        return {"container": copy.deepcopy(self.container),
                "artifact": copy.deepcopy(artifact)}

    def block_stored_tar_container(
            self, container_id, expected_state, expected_owner_token=None):
        assert self.container["container_id"] == container_id
        if self.container["validation_state"] == "blocked":
            return copy.deepcopy(self.container)
        if (self.container["validation_state"] != expected_state
                or self.container["owner_token"] != expected_owner_token):
            raise RuntimeError("block CAS failed")
        self.container["validation_state"] = "blocked"
        self.container["owner_token"] = None
        for artifact in self.artifacts:
            artifact["readiness_state"] = "blocked"
        self.block_calls.append(container_id)
        return copy.deepcopy(self.container)

    def validate_staged_chunk_readiness(self, desc):
        desc.assert_writer_ready()
        return True


class StoredTarStartupReconcileTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = self.temp.name
        self.pack = os.path.join(self.root, "_pack_s0002_003")
        self.manifests = os.path.join(self.root, "manifests")
        os.makedirs(self.pack)
        self.final = os.path.join(self.pack, "container_0000.tar")
        self.part = self.final + ".owner.part"
        self.member = SimpleNamespace(
            member_name="present", canonical_source_path="/remote/present",
            remote_path="/remote/present", file_size_bytes=3,
            plan_ordinal=0, container_ordinal=0)
        self.plan = SimpleNamespace(members=(self.member,))
        self.container = {
            "container_id": 7, "session_id": 2, "chunk_index": 3,
            "container_ordinal": 0, "container_format": "stored_tar",
            "format_version": "stored-tar-v1",
            "tar_dialect": "gnu-pax-sparse-v1",
            "storage_class": "small_files",
            "container_name": "container_0000.tar",
            "expected_member_count": 1, "expected_logical_bytes": 3,
            "observed_member_count": None, "observed_logical_bytes": None,
            "actual_artifact_bytes": None,
            "validated_part_locator": None, "validation_summary": None,
            "disposition_counts": None, "temporary_data_locator": None,
            "permanent_local_metadata_locator": None,
            "validation_state": "planned", "writer_state": "not_started",
            "catalog_state": "not_started", "owner_token": None,
            "lease_expires_at": None,
        }
        self.db = _ReconcileDB(self.container, self.plan)

    def tearDown(self):
        self.temp.cleanup()

    @contextmanager
    def _no_tape_guard(self):
        targets = (
            "src.ltfs.get_volume_label", "src.ltfs.eject_tape_drive",
            "src.ltfs._ensure_lto_drive_ready",
            "src.ltfs._ensure_lto_drive_ready_unlocked",
            "src.ltfs.TapeManager.check_tape",
            "src.ltfs.TapeManager.tape_info",
            "src.ltfs.TapeManager.eject_tape",
        )
        with ExitStack() as stack:
            spies = [stack.enter_context(mock.patch(
                target, side_effect=AssertionError(
                    f"tape/LTFS operation during reconciliation: {target}")))
                     for target in targets]
            yield
            for spy in spies:
                self.assertEqual(spy.call_count, 0)

    def _set_validated(self):
        _write_tar(self.part)
        validation = validate_stored_tar_part(
            self.part, [self._plan_dict()], container_ordinal=0)
        self.container.update({
            "validation_state": "validated_part", "owner_token": "dead",
            "validated_part_locator": self.part,
            "validation_summary": {
                **asdict(validation), "source_diagnostics": []},
            "observed_member_count": validation.member_count,
            "observed_logical_bytes": validation.logical_bytes,
            "actual_artifact_bytes": validation.archive_size,
            "lease_expires_at": "expired",
        })
        return validation

    def _plan_dict(self):
        return {
            "member_name": "present",
            "canonical_source_path": "/remote/present",
            "expected_size": 3,
            "plan_ordinal": 0,
            "container_ordinal": 0,
        }

    def _publish(self, *, stop=None):
        validation = validate_stored_tar_part(
            self.part, [self._plan_dict()], container_ordinal=0)

        def hook(stage):
            if stage == stop:
                raise RuntimeError(f"crash:{stage}")

        return publish_stored_tar_pair(
            self.manifests, self.part, self.final, [self._plan_dict()], (),
            validation=validation, session_id=2, chunk_index=3,
            container_id=7, container_ordinal=0, owner_token="dead",
            db=self.db, pack_dir=self.pack, crash_hook=hook)

    def _snapshot(self):
        files = []
        for root, _dirs, names in os.walk(self.root):
            for name in names:
                path = os.path.join(root, name)
                files.append((os.path.relpath(path, self.root),
                              os.path.getsize(path)))
        return (copy.deepcopy(self.db.container),
                copy.deepcopy(self.db.artifacts), sorted(files))

    def _run_twice(self, *, owner_probe=lambda _owner, _row: False):
        kwargs = dict(
            archive_root=self.manifests, pack_dir=self.pack,
            remote_base="/remote", owner_probe=owner_probe,
            recovery_owner_token="reconciler")
        with self._no_tape_guard():
            first = reconcile_tar_artifacts(self.db, 2, 3, **kwargs)
            after_first = self._snapshot()
            second = reconcile_tar_artifacts(self.db, 2, 3, **kwargs)
            after_second = self._snapshot()
        self.assertEqual(after_second, after_first,
                         "second reconciliation changed durable/local state")
        return first, second

    def test_orphan_tar_part_is_quarantined_outside_pack(self):
        orphan = os.path.join(self.pack, "unclaimed.tar.part")
        _write_tar(orphan)
        first, _second = self._run_twice()
        self.assertEqual(first["verdict"], TAR_RECONCILE_REBUILD)
        self.assertFalse(os.path.exists(orphan))
        self.assertTrue(any(item["case"] == "orphan_tar_part"
                            for item in first["cases"]))

    def test_expired_build_owner_is_reclaimed_only_after_absence_proof(self):
        self.container.update({
            "validation_state": "building", "owner_token": "expired",
            "validated_part_locator": self.part,
            "lease_expires_at": "expired"})
        _write_tar(self.part)
        first, _second = self._run_twice(
            owner_probe=lambda owner, _row: False if owner == "expired" else None)
        self.assertEqual(first["verdict"], TAR_RECONCILE_READY)
        self.assertEqual(self.container["validation_state"], "ready")

    def test_validated_tar_part_before_sidecar_is_adopted(self):
        self._set_validated()
        first, _second = self._run_twice()
        self.assertTrue(any(item["case"] ==
                            "validated_tar_part_before_sidecar"
                            for item in first["cases"]))

    def test_final_sidecar_plus_tar_part_is_adopted(self):
        self._set_validated()
        with self.assertRaisesRegex(RuntimeError, "after_sidecar_publication"):
            self._publish(stop="after_sidecar_publication")
        first, _second = self._run_twice()
        self.assertTrue(any(item["case"] == "final_sidecar_plus_tar_part"
                            for item in first["cases"]))

    def test_final_pair_before_database_commit_is_adopted(self):
        self._set_validated()
        with self.assertRaisesRegex(RuntimeError, "before_paired_db_commit"):
            self._publish(stop="before_paired_db_commit")
        first, _second = self._run_twice()
        self.assertTrue(any(item["case"] == "final_pair_before_db_commit"
                            for item in first["cases"]))

    def test_final_tar_without_sidecar_reconstructs_all_present_only(self):
        self._set_validated()
        os.rename(self.part, self.final)
        first, _second = self._run_twice()
        self.assertTrue(any(item["case"] == "final_tar_absent_sidecar"
                            for item in first["cases"]))
        sidecar = os.path.join(
            self.manifests, *tar_sidecar_locator(2, 3, 0).split("/"))
        self.assertTrue(os.path.isfile(sidecar))

    def test_final_tar_without_sidecar_never_infers_absent_disposition(self):
        self._set_validated()
        missing = SimpleNamespace(
            member_name="missing", canonical_source_path="/remote/missing",
            remote_path="/remote/missing", file_size_bytes=9,
            plan_ordinal=1, container_ordinal=0)
        self.plan.members = (self.member, missing)
        self.container["expected_member_count"] = 2
        self.container["expected_logical_bytes"] = 12
        self.container["validation_summary"]["source_diagnostics"] = []
        os.rename(self.part, self.final)
        first, _second = self._run_twice()
        self.assertEqual(first["verdict"], TAR_RECONCILE_BLOCKED)
        self.assertEqual(self.container["validation_state"], "blocked")
        self.assertFalse(os.path.exists(os.path.join(
            self.manifests, *tar_sidecar_locator(2, 3, 0).split("/"))))

    def test_ready_pair_missing_member_blocks_durably(self):
        self._set_validated()
        result = self._publish()
        os.remove(result.sidecar_path)
        first, _second = self._run_twice()
        self.assertEqual(first["verdict"], TAR_RECONCILE_BLOCKED)
        self.assertEqual(self.container["validation_state"], "blocked")

    def test_conflicting_final_and_part_name_collision_blocks(self):
        self._set_validated()
        _write_tar(self.final, b"xyz")
        first, _second = self._run_twice()
        self.assertEqual(first["verdict"], TAR_RECONCILE_BLOCKED)
        self.assertTrue(any(item["case"] == "final_part_name_collision"
                            for item in first["cases"]))

    def test_ready_resume_pack_is_revalidated_after_restart(self):
        self._set_validated()
        publication = self._publish()
        row = self.container
        artifact = self.db.artifacts[0]
        staged_container = StagedContainer(
            container_id=7, session_id=2, chunk_index=3,
            container_ordinal=0, container_format=ContainerFormat.STORED_TAR,
            format_version=row["format_version"],
            storage_class=row["storage_class"],
            container_name=row["container_name"], data_path=self.final,
            temporary_data_locator=self.final,
            data_size_bytes=row["actual_artifact_bytes"],
            expected_member_count=1, expected_logical_bytes=3,
            observed_member_count=1, observed_logical_bytes=3,
            permanent_local_metadata_locator=artifact["local_locator"],
            validation_state=ContainerValidationState.READY,
            database_validation_state=ContainerValidationState.READY,
            tar_dialect=row["tar_dialect"])
        staged_artifact = StagedArtifact(
            artifact_id=19, session_id=2, chunk_index=3, container_id=7,
            artifact_kind=ArtifactKind.TAR_SIDECAR,
            artifact_version=TAR_SIDECAR_VERSION,
            staged_path=publication.pack_sidecar_path,
            local_locator=artifact["local_locator"],
            staged_size_bytes=artifact["artifact_size_bytes"],
            readiness_state=ArtifactReadiness.READY,
            database_readiness_state=ArtifactReadiness.READY)
        inventory = []
        for name in os.listdir(self.pack):
            path = os.path.join(self.pack, name)
            if os.path.isfile(path):
                inventory.append([name, os.path.getsize(path)])
        payload = {
            "version": 1, "session_id": 2, "chunk_index": 3,
            "fetch_dir": os.path.join(self.root, "fetch"),
            "pack_dir": self.pack, "staged_bytes": sum(x[1] for x in inventory),
            "skip_tape": False, "packaging_format": "stored_tar",
            "metadata": [], "containers": [asdict(staged_container)],
            "artifacts": [asdict(staged_artifact)],
            "pack_inventory": sorted(inventory),
        }
        for section in ("containers", "artifacts"):
            for item in payload[section]:
                for key, value in list(item.items()):
                    if hasattr(value, "value"):
                        item[key] = value.value
        with open(os.path.join(self.pack, "_resume_pack.json"), "w",
                  encoding="utf-8") as handle:
            json.dump(payload, handle)
        first, _second = self._run_twice()
        self.assertEqual(first["resume_pack"], "ready", first["blocking"])
        self.assertTrue(any(item["case"] == "ready_resume_pack"
                            for item in first["cases"]))

    def test_ready_database_artifact_state_mismatch_blocks(self):
        self._set_validated()
        self._publish()
        self.container["actual_artifact_bytes"] += 1
        first, _second = self._run_twice()
        self.assertEqual(first["verdict"], TAR_RECONCILE_BLOCKED)
        self.assertEqual(self.container["validation_state"], "blocked")

    def test_concurrent_live_owner_is_never_touched(self):
        self._set_validated()
        before = self._snapshot()
        first, _second = self._run_twice(
            owner_probe=lambda _owner, _row: True)
        self.assertEqual(first["verdict"], TAR_RECONCILE_BLOCKED)
        self.assertEqual(self._snapshot(), before)
        self.assertTrue(any(item["case"] == "concurrent_build_owner"
                            for item in first["cases"]))

    def test_indeterminate_owner_is_never_touched(self):
        self._set_validated()
        before = self._snapshot()
        first, _second = self._run_twice(
            owner_probe=lambda _owner, _row: None)
        self.assertEqual(first["verdict"], TAR_RECONCILE_BLOCKED)
        self.assertEqual(self._snapshot(), before)
        self.assertTrue(any(item["case"] ==
                            "expired_or_unknown_build_owner"
                            for item in first["cases"]))


if __name__ == "__main__":
    unittest.main()
