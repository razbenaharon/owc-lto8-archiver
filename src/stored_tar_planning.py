"""Deterministic Stored TAR partition planning.

Task 2.1 only decides which sealed plan ordinals become TAR members and which
remain loose.  It deliberately does not create a TAR or inspect source content.
"""
from dataclasses import dataclass
from typing import Iterable, List, Optional, Tuple

from .pipeline_types import ContainerFormat
from .tar_container import STORED_TAR_DIALECT, STORED_TAR_FORMAT_VERSION


TAR_BLOCK_SIZE = 512
GNU_TAR_BLOCKING_FACTOR = 512
GNU_TAR_RECORD_SIZE = TAR_BLOCK_SIZE * GNU_TAR_BLOCKING_FACTOR
END_OF_ARCHIVE_BYTES = 2 * TAR_BLOCK_SIZE


def _round_up(value, block_size):
    value = int(value)
    block_size = int(block_size)
    if value <= 0:
        return 0
    return ((value + block_size - 1) // block_size) * block_size


def _pax_record_size(key, value):
    """Return the exact byte length of one PAX key/value record."""
    body = f" {key}={value}\n".encode("utf-8", errors="surrogatepass")
    digits = len(str(len(body) + 1))
    while True:
        total = len(body) + digits
        new_digits = len(str(total))
        if new_digits == digits:
            return total
        digits = new_digits


def estimate_stored_tar_member_bytes(name, logical_size):
    """Conservative GNU pax/sparse-v1 member size estimate in bytes.

    The estimate includes a PAX extended header, the regular file header, data
    padding, and deterministic sparse dialect metadata.  It is used only for
    choosing container boundaries; the finalized artifact size stays
    authoritative later.
    """
    logical_size = int(logical_size)
    if logical_size < 0:
        raise ValueError("logical size cannot be negative")
    name = str(name)
    pax_payload = sum((
        _pax_record_size("path", name),
        _pax_record_size("size", str(logical_size)),
        _pax_record_size("GNU.sparse.major", "1"),
        _pax_record_size("GNU.sparse.minor", "0"),
        _pax_record_size("GNU.sparse.name", name),
        _pax_record_size("GNU.sparse.realsize", str(logical_size)),
    ))
    return (
        TAR_BLOCK_SIZE
        + _round_up(pax_payload, TAR_BLOCK_SIZE)
        + TAR_BLOCK_SIZE
        + _round_up(logical_size, TAR_BLOCK_SIZE)
    )


def _archive_bytes_from_payload(payload):
    """Finalized archive bytes for a payload already summed once.

    Extracted so ``build_stored_tar_chunk_plan`` can keep a running payload sum
    instead of re-summing the whole open container per member (the old
    ``estimate_stored_tar_archive_bytes(current_estimates + [x])`` was O(n) per
    file → O(n²) per chunk). The rounding/zero/negative rules are unchanged.
    """
    payload = int(payload)
    if payload < 0:
        raise ValueError("member estimates cannot sum below zero")
    if payload == 0:
        return 0
    return _round_up(payload + END_OF_ARCHIVE_BYTES, GNU_TAR_RECORD_SIZE)


def estimate_stored_tar_archive_bytes(member_estimates):
    """Estimate finalized archive bytes including end and blocking padding."""
    return _archive_bytes_from_payload(
        sum(int(value) for value in member_estimates))


@dataclass(frozen=True)
class StoredTarPlanMember:
    manifest_id: int
    plan_ordinal: int
    remote_path: str
    file_size_bytes: int
    storage_class: str
    container_ordinal: Optional[int] = None
    estimated_tar_bytes: int = 0


@dataclass(frozen=True)
class StoredTarContainerPlan:
    session_id: int
    chunk_index: int
    container_ordinal: int
    container_name: str
    expected_member_count: int
    expected_logical_bytes: int
    estimated_archive_bytes: int
    max_size_bytes: int
    container_format: ContainerFormat = ContainerFormat.STORED_TAR
    format_version: str = STORED_TAR_FORMAT_VERSION
    tar_dialect: str = STORED_TAR_DIALECT
    storage_class: str = "small_files"


@dataclass(frozen=True)
class StoredTarChunkPlan:
    session_id: int
    chunk_index: int
    max_size_bytes: int
    containers: Tuple[StoredTarContainerPlan, ...]
    members: Tuple[StoredTarPlanMember, ...]

    @property
    def small_members(self):
        return tuple(m for m in self.members if m.storage_class == "small_files")

    @property
    def loose_members(self):
        return tuple(m for m in self.members if m.storage_class == "loose_files")

    @property
    def source_missing_members(self):
        return tuple(
            m for m in self.members if m.storage_class == "source_missing")


def build_stored_tar_chunk_plan(
        session_id, chunk_index, chunk_files, *, loose_threshold_bytes,
        max_size_bytes):
    """Partition sealed chunk files by stable plan ordinal order."""
    max_size_bytes = int(max_size_bytes)
    loose_threshold_bytes = int(loose_threshold_bytes)
    if max_size_bytes <= 0:
        raise ValueError("stored TAR max size must be positive")
    if loose_threshold_bytes < 0:
        raise ValueError("loose threshold cannot be negative")

    ordered = list(chunk_files)
    members: List[StoredTarPlanMember] = []
    containers: List[StoredTarContainerPlan] = []
    current_members: List[StoredTarPlanMember] = []
    # Running sum of the open container's member estimates. Kept incrementally
    # so the per-member fit check is O(1) instead of re-summing a list that
    # grows to the whole chunk (200k for Session 37) — see
    # _archive_bytes_from_payload.
    current_payload = 0
    current_logical = 0

    def flush_container():
        nonlocal current_members, current_payload, current_logical
        if not current_members:
            return
        ordinal = len(containers)
        archive_estimate = _archive_bytes_from_payload(current_payload)
        container_name = (
            f"chunk_{int(chunk_index):06d}_stored_tar_{ordinal:04d}.tar")
        containers.append(StoredTarContainerPlan(
            session_id=int(session_id),
            chunk_index=int(chunk_index),
            container_ordinal=ordinal,
            container_name=container_name,
            expected_member_count=len(current_members),
            expected_logical_bytes=current_logical,
            estimated_archive_bytes=archive_estimate,
            max_size_bytes=max_size_bytes,
        ))
        for item in current_members:
            members.append(StoredTarPlanMember(
                manifest_id=item.manifest_id,
                plan_ordinal=item.plan_ordinal,
                remote_path=item.remote_path,
                file_size_bytes=item.file_size_bytes,
                storage_class=item.storage_class,
                container_ordinal=ordinal,
                estimated_tar_bytes=item.estimated_tar_bytes,
            ))
        current_members = []
        current_payload = 0
        current_logical = 0

    for ordinal, row in enumerate(ordered):
        size = int(row["file_size_bytes"])
        manifest_id = int(row["manifest_id"])
        remote_path = str(row["remote_path"])
        if row.get("status") == "source_missing":
            flush_container()
            members.append(StoredTarPlanMember(
                manifest_id=manifest_id,
                plan_ordinal=ordinal,
                remote_path=remote_path,
                file_size_bytes=size,
                storage_class="source_missing",
            ))
            continue
        if size >= loose_threshold_bytes:
            flush_container()
            members.append(StoredTarPlanMember(
                manifest_id=manifest_id,
                plan_ordinal=ordinal,
                remote_path=remote_path,
                file_size_bytes=size,
                storage_class="loose_files",
            ))
            continue

        member_estimate = estimate_stored_tar_member_bytes(remote_path, size)
        candidate = _archive_bytes_from_payload(current_payload + member_estimate)
        if current_members and candidate > max_size_bytes:
            flush_container()
        current_members.append(StoredTarPlanMember(
            manifest_id=manifest_id,
            plan_ordinal=ordinal,
            remote_path=remote_path,
            file_size_bytes=size,
            storage_class="small_files",
            estimated_tar_bytes=member_estimate,
        ))
        current_payload += member_estimate
        current_logical += size

    flush_container()
    return StoredTarChunkPlan(
        session_id=int(session_id),
        chunk_index=int(chunk_index),
        max_size_bytes=max_size_bytes,
        containers=tuple(containers),
        members=tuple(members),
    )
