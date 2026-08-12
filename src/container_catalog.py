"""Catalog adapters for immutable archive containers.

The Stored-TAR producer deliberately does not materialize its members in the
pack directory.  :class:`TarCatalogAdapter` therefore joins the durable sealed
plan with the validated JSONL.zst sidecar and emits the same ``FileRecord``
shape that the legacy ZIP packer hands to :class:`src.backup.LTOBackup`.

This module is local-metadata only.  It never follows a tape locator and never
extracts or hashes a member.
"""
import os

from .archive_artifacts import search_tar_sidecar
from .pipeline_types import ArtifactKind, ContainerFormat


class TarCatalogAdapter:
    """Stream canonical catalog records for one ready Stored-TAR chunk.

    The sealed database plan remains membership authority.  The sidecar proves
    which planned ordinals were actually archived and supplies their exact TAR
    member names.  Existing staged metadata is used only to verify catalog
    policy and the producer handoff; transient staging paths are never emitted
    as source provenance.
    """

    def __init__(self, db, staged_chunk, *, index_min_file_mb=10,
                 index_packed_small_files=False):
        self.db = db
        self.staged_chunk = staged_chunk
        self.index_min_file_mb = float(index_min_file_mb or 10)
        self.index_packed_small_files = bool(index_packed_small_files)

    def _catalog_policy(self, size, staged_record):
        expected = (
            "index" if self.index_packed_small_files
            or int(size) >= int(self.index_min_file_mb * 1024 * 1024)
            else "manifest_only"
        )
        supplied = staged_record.get("catalog_policy")
        if supplied not in (None, "index", "manifest_only"):
            raise RuntimeError(
                f"Stored TAR metadata has invalid catalog_policy {supplied!r}")
        if supplied is not None and supplied != expected:
            raise RuntimeError(
                "Stored TAR catalog policy disagrees with the configured "
                "index threshold")
        return expected

    def records(self):
        chunk = self.staged_chunk
        if ContainerFormat(chunk.packaging_format) is not \
                ContainerFormat.STORED_TAR:
            raise ValueError("TarCatalogAdapter requires a Stored-TAR chunk")
        if chunk.session_id is None:
            raise ValueError("Stored-TAR cataloging requires a remote session")

        plan = self.db.get_stored_tar_chunk_plan(
            int(chunk.session_id), int(chunk.chunk_index))
        if (int(plan.session_id) != int(chunk.session_id)
                or int(plan.chunk_index) != int(chunk.chunk_index)):
            raise RuntimeError("Stored TAR plan belongs to another chunk")

        containers = {int(item.container_ordinal): item
                      for item in chunk.containers}
        artifacts = {
            int(item.container_id): item for item in chunk.artifacts
            if item.container_id is not None
            and ArtifactKind(item.artifact_kind) is ArtifactKind.TAR_SIDECAR
        }
        staged = {}
        loose = []
        for record in chunk.metadata:
            if not record.get("is_packed"):
                loose.append(record)
                continue
            canonical = str(record.get("canonical_source_path")
                            or record.get("original_path") or "")
            key = (int(record.get("container_id")), canonical)
            if not canonical.startswith("/") or "\\" in canonical:
                raise RuntimeError(
                    "Stored TAR catalog metadata lacks a canonical POSIX "
                    "remote source path")
            if key in staged:
                raise RuntimeError("duplicate Stored TAR staged metadata")
            staged[key] = record

        emitted = set()
        plan_members = {}
        for member in plan.small_members:
            ordinal = int(member.container_ordinal)
            plan_members.setdefault(ordinal, {})[int(member.plan_ordinal)] = member

        for planned_container in plan.containers:
            ordinal = int(planned_container.container_ordinal)
            container = containers.get(ordinal)
            if container is None:
                raise RuntimeError(
                    f"Stored TAR plan container {ordinal} is not staged")
            if (container.container_name != planned_container.container_name
                    or int(container.expected_member_count)
                    != int(planned_container.expected_member_count)
                    or int(container.expected_logical_bytes)
                    != int(planned_container.expected_logical_bytes)):
                raise RuntimeError(
                    f"Stored TAR staged container {ordinal} disagrees with plan")
            artifact = artifacts.get(int(container.container_id))
            if artifact is None:
                raise RuntimeError(
                    f"Stored TAR container {ordinal} has no ready sidecar")

            result = search_tar_sidecar(
                artifact.staged_path,
                limit=max(1, int(container.expected_member_count) + 1),
                expected_container_id=container.container_id,
                expected_member_count=container.expected_member_count,
                max_records=max(1, len(plan_members.get(ordinal, {})) + 1),
            )
            header = result.header
            identity = {
                "session_id": int(chunk.session_id),
                "chunk_index": int(chunk.chunk_index),
                "container_ordinal": ordinal,
            }
            mismatch = [key for key, value in identity.items()
                        if int(header.get(key, -1)) != value]
            if mismatch:
                raise RuntimeError(
                    "Stored TAR sidecar identity mismatch: "
                    + ", ".join(mismatch))
            if int(result.footer.get("tar_size_bytes", -1)) != int(
                    container.data_size_bytes):
                raise RuntimeError("Stored TAR sidecar data-size mismatch")

            expected_by_ordinal = plan_members.get(ordinal, {})
            sidecar_ordinals = {int(item["ordinal"])
                                for item in result.expected_members}
            if sidecar_ordinals != set(expected_by_ordinal):
                raise RuntimeError(
                    "Stored TAR sidecar ordinals disagree with sealed plan")

            for item in result.matches:
                member = expected_by_ordinal.get(int(item["ordinal"]))
                if member is None:
                    raise RuntimeError("Stored TAR sidecar invented a plan member")
                canonical = str(member.remote_path)
                if (item["original_path"] != canonical
                        or int(item["file_size_bytes"])
                        != int(member.file_size_bytes)):
                    raise RuntimeError(
                        "Stored TAR sidecar member disagrees with sealed plan")
                staged_key = (int(container.container_id), canonical)
                staged_record = staged.get(staged_key)
                if staged_record is None:
                    raise RuntimeError(
                        "Stored TAR sidecar member is absent from staged metadata")
                if (int(staged_record.get("file_size_bytes", -1))
                        != int(member.file_size_bytes)):
                    raise RuntimeError(
                        "Stored TAR staged metadata size disagrees with plan")
                emitted.add(staged_key)
                yield {
                    "file_name": os.path.basename(canonical),
                    "original_path": canonical,
                    "canonical_source_path": canonical,
                    "file_size_bytes": int(member.file_size_bytes),
                    "is_packed": True,
                    "container_name": container.container_name,
                    "stored_path": item["member_name"],
                    "catalog_policy": self._catalog_policy(
                        member.file_size_bytes, staged_record),
                    "manifest_name": os.path.basename(artifact.staged_path),
                    "manifest_path": artifact.local_locator,
                    "manifest_format": "jsonl",
                    "manifest_compression": "zstd",
                    "container_id": int(container.container_id),
                    "container_format": ContainerFormat.STORED_TAR.value,
                    "container_ordinal": ordinal,
                    "artifact_id": int(artifact.artifact_id),
                    "artifact_kind": ArtifactKind.TAR_SIDECAR.value,
                    "artifact_version": artifact.artifact_version,
                    "actual_artifact_bytes": int(container.data_size_bytes),
                }

        if emitted != set(staged):
            raise RuntimeError(
                "Stored TAR staged metadata has members absent from the "
                "validated plan/sidecar")
        # Loose records already carry canonical remote paths from the legacy
        # plan mapping.  Preserve their exact shape and identity.
        yield from loose

    def materialize(self):
        """Compatibility helper for APIs that require reiterable metadata."""
        return list(self.records())


__all__ = ["TarCatalogAdapter"]
