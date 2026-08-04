"""Typed state shared across the archive pipeline.

These replace string-keyed dicts whose typos were silent ``None``s at
runtime: :class:`StagedChunk` crosses the producer -> tape-writer thread
boundary, :class:`StreamState` is the streaming session's shared counters,
and :class:`FileRecord` annotates the packer/catalog metadata records
(annotation only — the records stay plain dicts because the DB layer
consumes them via ``.get()``).
"""
import os
import stat
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, TypedDict


class SessionStatus(str, Enum):
    """Persisted ``remote_sessions.status``."""

    ACTIVE = "active"
    COMPLETED = "completed"
    ABANDONED = "abandoned"


class ChunkStatus(str, Enum):
    """Persisted ``remote_chunks.status``.

    ``BACKING`` is the one that matters. It means **the physical tape write has
    begun** — set from the writer-start callback, never before. Any failure at
    or after that point is physically ambiguous: bytes may be on tape. The only
    automatic move out of it is forward to ``DONE`` (the write completed and
    committed). There is no automatic **retry** transition, and
    :data:`CHUNK_TRANSITIONS` encodes that rather than leaving it to a comment
    somebody can miss.
    """

    PENDING = "pending"
    FETCHING = "fetching"
    PACKING = "packing"
    BACKING = "backing"
    DONE = "done"
    FETCH_FAILED = "fetch_failed"
    BACKUP_FAILED = "backup_failed"


class FileTransferStatus(str, Enum):
    """Persisted ``remote_file_state.status``.

    The richer source outcomes (``SOURCE_PERMISSION_DENIED``,
    ``SOURCE_UNREADABLE``, ``SOURCE_CHANGED``, ``UNRESOLVED``) are declared here
    by Task 1.5 but are only writable once migration 014 widens the CHECK
    constraint — see :func:`allowed_file_statuses`.
    """

    PENDING = "pending"
    FETCHING = "fetching"
    FETCHED = "fetched"
    FETCH_FAILED = "fetch_failed"
    SOURCE_MISSING = "source_missing"
    # --- added by migration 014 -------------------------------------------
    SOURCE_PERMISSION_DENIED = "source_permission_denied"
    SOURCE_UNREADABLE = "source_unreadable"
    SOURCE_CHANGED = "source_changed"
    UNRESOLVED = "unresolved"


class SourceDisposition(str, Enum):
    """Observable disposition of one sealed Stored-TAR plan ordinal.

    This is deliberately separate from ``remote_file_state.status``.  A TAR
    sidecar describes what one immutable container attempt observed, while the
    remote-file state machine describes the wider chunk workflow.
    """

    ARCHIVED = "archived"
    SOURCE_MISSING = "source_missing"
    SOURCE_PERMISSION_DENIED = "source_permission_denied"
    SOURCE_UNREADABLE = "source_unreadable"
    SOURCE_CHANGED = "source_changed"
    UNRESOLVED = "unresolved"


#: The subset legal on a database that has NOT applied migration 014.
LEGACY_FILE_STATUSES = frozenset({
    FileTransferStatus.PENDING, FileTransferStatus.FETCHING,
    FileTransferStatus.FETCHED, FileTransferStatus.FETCH_FAILED,
    FileTransferStatus.SOURCE_MISSING,
})


def allowed_file_statuses(migration_014_applied=False):
    """Which file outcomes may be persisted on this database."""
    if migration_014_applied:
        return frozenset(FileTransferStatus)
    return LEGACY_FILE_STATUSES


class MembershipState(str, Enum):
    """Whether a chunk's file membership may still change.

    ``SEALED`` is the point after which a chunk's contents are immutable: a
    later append would silently change what a already-written tape chunk was
    supposed to contain. Persisted by migration 014.
    """

    BUILDING = "building"
    SEALED = "sealed"


class ContainerFormat(str, Enum):
    """Durable ``remote_chunks`` / ``archive_containers`` format authority.

    A filename extension is intentionally not represented here: readers route
    from this persisted value, never from ``.zip``/``.tar`` spelling.
    """

    ZIP = "zip"
    STORED_TAR = "stored_tar"


class ArtifactKind(str, Enum):
    """Versioned metadata artifacts reserved by migration 015."""

    ZIP_MANIFEST = "zip_manifest"
    TAR_SIDECAR = "tar_sidecar"
    PLAN_MANIFEST = "plan_manifest"
    TERMINAL_MANIFEST = "terminal_manifest"


class ArtifactReadiness(str, Enum):
    """Publication state of one local/tape metadata artifact."""

    PLANNED = "planned"
    WRITING = "writing"
    VALIDATED = "validated"
    READY = "ready"
    FAILED = "failed"
    BLOCKED = "blocked"


class ContainerValidationState(str, Enum):
    """Local validation state of one container data file."""

    PLANNED = "planned"
    BUILDING = "building"
    VALIDATED_PART = "validated_part"
    READY = "ready"
    FAILED = "failed"
    BLOCKED = "blocked"


class ContainerWriterState(str, Enum):
    """Physical copy state, deliberately separate from catalog state."""

    NOT_STARTED = "not_started"
    WRITING = "writing"
    COPIED = "copied"
    FAILED = "failed"
    AMBIGUOUS = "ambiguous"


class ContainerCatalogState(str, Enum):
    """Catalog commit state after a successful physical copy."""

    NOT_STARTED = "not_started"
    COMMITTING = "committing"
    COMMITTED = "committed"
    FAILED = "failed"
    AMBIGUOUS = "ambiguous"


class ContainerRecord(TypedDict, total=False):
    """Typed database-facing archive-container record."""

    container_id: int
    session_id: int
    chunk_index: int
    container_ordinal: int
    container_format: str
    format_version: str
    tar_dialect: Optional[str]
    storage_class: str
    container_name: str
    temporary_data_locator: Optional[str]
    permanent_local_metadata_locator: Optional[str]
    expected_member_count: int
    expected_logical_bytes: int
    observed_member_count: Optional[int]
    observed_logical_bytes: Optional[int]
    actual_artifact_bytes: Optional[int]
    validated_part_locator: Optional[str]
    validation_summary: Optional[Dict[str, Any]]
    disposition_counts: Optional[Dict[str, int]]
    validation_state: str
    writer_state: str
    catalog_state: str


class ArtifactRecord(TypedDict, total=False):
    """Typed database-facing local/tape artifact record."""

    artifact_id: int
    session_id: int
    chunk_index: int
    container_id: Optional[int]
    artifact_kind: str
    artifact_version: str
    local_locator: Optional[str]
    tape_locator: Optional[str]
    artifact_size_bytes: Optional[int]
    readiness_state: str


@dataclass(frozen=True)
class StoredTarExpectedMember:
    """One immutable plan/sidecar expectation for a Stored TAR member.

    ``source_exception`` is evidence about a planned source entry that was not
    archived.  Such a record may omit ``name`` because Plan 2 sidecars must not
    invent a TAR member name for an absent source.  The TAR reader accepts only
    the three explicit source outcomes that Plan 2 permits; it never infers one
    from the TAR's contents.
    """

    name: Optional[str]
    logical_size: int
    ordinal: int
    source_exception: Optional[FileTransferStatus] = None


@dataclass(frozen=True)
class StoredTarMember:
    """Observed regular-file member returned by the strict streaming reader."""

    name: str
    normalized_name: str
    logical_size: int
    stored_size: int
    ordinal: int
    sparse: bool = False
    sparse_extent_count: int = 0


@dataclass(frozen=True)
class StoredTarContainer:
    """Successful full-container validation result.

    ``archive_size`` includes the two end blocks and GNU ``-b 512`` zero
    padding.  Content hashes are intentionally absent from this contract.
    """

    container_format: ContainerFormat
    format_version: str
    tar_dialect: str
    members: Tuple[StoredTarMember, ...]
    member_count: int
    logical_bytes: int
    archive_size: int


@dataclass(frozen=True)
class StoredTarSourceDiagnostic:
    """Machine-attributed source evidence captured by direct TAR transport."""

    plan_ordinal: int
    path: str
    disposition: SourceDisposition
    evidence: str


@dataclass(frozen=True)
class StoredTarValidationSummary:
    """Owner-scoped proof that a still-unpublished TAR part matches its plan."""

    container_ordinal: int
    member_count: int
    logical_bytes: int
    archive_size: int
    plan_ordinal_count: int
    disposition_counts: Dict[str, int]
    members: Tuple[StoredTarMember, ...]


#: Allowed chunk transitions. Anything not listed is refused and leaves the old
#: state — an unknown transition is a bug, and guessing at one is how a chunk
#: that may be on tape gets quietly retried.
CHUNK_TRANSITIONS = {
    ChunkStatus.PENDING: frozenset({
        ChunkStatus.FETCHING, ChunkStatus.FETCH_FAILED, ChunkStatus.DONE}),
    ChunkStatus.FETCHING: frozenset({
        ChunkStatus.PACKING, ChunkStatus.FETCH_FAILED, ChunkStatus.PENDING}),
    ChunkStatus.PACKING: frozenset({
        ChunkStatus.BACKING, ChunkStatus.BACKUP_FAILED,
        ChunkStatus.FETCH_FAILED, ChunkStatus.DONE, ChunkStatus.PENDING}),
    # Forward to DONE only — that is the write completing and committing.
    # There is NO automatic retry out of 'backing': the chunk may already be on
    # tape, so re-fetching and re-writing it would double-write. Only a human
    # who has reconciled the cartridge against the catalog may move it back.
    ChunkStatus.BACKING: frozenset({ChunkStatus.DONE}),
    # Terminal.
    ChunkStatus.DONE: frozenset(),
    # A failed chunk is re-drivable: the next run re-fetches it from scratch.
    ChunkStatus.FETCH_FAILED: frozenset({
        ChunkStatus.PENDING, ChunkStatus.FETCHING}),
    ChunkStatus.BACKUP_FAILED: frozenset({
        ChunkStatus.PENDING, ChunkStatus.FETCHING, ChunkStatus.PACKING}),
}


class ForbiddenTransition(ValueError):
    """A transition that is not in :data:`CHUNK_TRANSITIONS`."""


def is_allowed_chunk_transition(from_status, to_status):
    """True when ``from_status -> to_status`` is permitted.

    An unknown status on either side is **not** permitted: failing closed on a
    value nobody declared is the whole point of having the matrix.
    """
    try:
        source = ChunkStatus(from_status)
        target = ChunkStatus(to_status)
    except ValueError:
        return False
    return target in CHUNK_TRANSITIONS.get(source, frozenset())


def _status_text(value):
    """The plain persisted string, whether given an enum member or a str.

    ``str()`` on a ``(str, Enum)`` member is ``'ChunkStatus.BACKING'`` on modern
    Pythons, which is neither what the database stores nor what an operator
    reading a stop reason should see.
    """
    return value.value if isinstance(value, ChunkStatus) else str(value)


def assert_chunk_transition(from_status, to_status):
    """Raise :class:`ForbiddenTransition` unless the move is allowed."""
    if is_allowed_chunk_transition(from_status, to_status):
        return
    source = _status_text(from_status)
    target = _status_text(to_status)
    detail = ""
    if source == ChunkStatus.BACKING.value:
        detail = (" — 'backing' means the physical write began, so the chunk "
                  "may already be on tape; only a human who has reconciled the "
                  "cartridge against the catalog may move it")
    raise ForbiddenTransition(
        f"chunk transition {source!r} -> {target!r} is not allowed; "
        f"the old state is preserved{detail}")


class ScanDirectoryState(str, Enum):
    """State of ONE directory's *immediate* listing.

    Deliberately separate from :class:`ScanCoverageState`: "I read this
    directory's own entries" and "every descendant of it is final" are
    different facts, and conflating them is how a partially-explored tree gets
    reported as covered.
    """

    PENDING = "pending"
    SCANNING = "scanning"
    #: Interrupted mid-listing. This is the ONLY directory a crash may replay.
    PARTIAL = "partial"
    COMPLETE = "complete"
    ERROR = "error"
    #: A source change invalidated it; it and its ancestors must be requeued.
    INVALIDATED = "invalidated"


class ScanCoverageState(str, Enum):
    """Traversal-only finality of a directory's whole subtree.

    ``FINAL`` means: every descendant listing is terminal, the before/after
    source observations agree, and no unresolved error remains. Segment
    allocation is deliberately irrelevant to this fact — see
    :class:`ScanPlanningState`.
    """

    PROVISIONAL = "provisional"
    FINAL = "final"
    ERROR = "error"
    INVALIDATED = "invalidated"


class ScanPlanningState(str, Enum):
    """How much of a directory's discovered work has been assigned to chunks.

    Independent of coverage on purpose, so an operator can tell "the source
    tree was explored" from "all discovered entries were assigned to plans".
    """

    UNPLANNED = "unplanned"
    PARTIALLY_ALLOCATED = "partially_allocated"
    FULLY_ALLOCATED = "fully_allocated"
    BLOCKED = "blocked"


class ScanSegmentState(str, Enum):
    """Readiness of one local scan-segment artifact.

    A ``.part`` file is never persisted as a ready locator: an artifact is
    ``WRITING`` until it has been atomically published, and only then ``READY``.
    """

    WRITING = "writing"
    READY = "ready"
    PARTIALLY_CONSUMED = "partially_consumed"
    CONSUMED = "consumed"
    INVALIDATED = "invalidated"


#: A scope is either a directory tree or a single explicitly-selected file.
SCOPE_KIND_DIRECTORY = "directory"
SCOPE_KIND_FILE = "file"


@dataclass(frozen=True)
class ScanScope:
    """One configured source root, as persisted order rather than config order.

    ``scope_ordinal`` is what makes a resumed scan deterministic: the config
    list may be reordered (a warning), but the traversal follows the order the
    session recorded. Adding or removing a root is refused, not absorbed.
    """

    scan_scope_id: Optional[int]
    session_id: Optional[int]
    scope_ordinal: int
    source_root: str
    scope_kind: str = SCOPE_KIND_DIRECTORY
    coverage_state: ScanCoverageState = ScanCoverageState.PROVISIONAL
    planning_complete: bool = False

    def __post_init__(self):
        if self.scope_kind not in (SCOPE_KIND_DIRECTORY, SCOPE_KIND_FILE):
            raise ValueError(f"unknown scope kind: {self.scope_kind!r}")
        if not self.source_root or not self.source_root.startswith("/"):
            raise ValueError(
                f"scope root must be an absolute POSIX path: "
                f"{self.source_root!r}")


@dataclass(frozen=True)
class ScanSegmentRef:
    """A ready range of scanned entries, addressed by stable scan ordinals.

    The locator is stored **root-relative** and resolved against
    ``ConfigManager.local_manifest_archive_root`` at read time, so a metadata
    backup can be relocated without rewriting database rows.
    """

    scan_segment_id: Optional[int]
    scan_directory_id: Optional[int]
    first_scan_ordinal: int
    last_scan_ordinal: int
    locator: str
    artifact_version: str = "scan-segment-v1"
    state: ScanSegmentState = ScanSegmentState.WRITING
    file_count: int = 0
    byte_count: int = 0
    #: Next ordinal in this segment that no chunk has consumed yet. Advanced
    #: transactionally under the segment row's lock; gaps/overlap are rejected.
    next_unconsumed_ordinal: Optional[int] = None

    def __post_init__(self):
        if self.last_scan_ordinal < self.first_scan_ordinal:
            raise ValueError("segment ordinal range is inverted")
        if self.locator.endswith(".part"):
            raise ValueError(
                "a .part path is never a ready segment locator; publish the "
                "artifact atomically first")

    @property
    def ordinal_count(self) -> int:
        return self.last_scan_ordinal - self.first_scan_ordinal + 1


class FileRecord(TypedDict, total=False):
    """One packer/catalog metadata record (see LTOPacker._pack_entries)."""
    file_name: str
    original_path: str
    file_size_bytes: int
    is_packed: bool
    container_name: Optional[str]
    stored_path: str
    canonical_source_path: Optional[str]
    catalog_policy: str
    manifest_name: Optional[str]
    manifest_path: Optional[str]
    manifest_format: Optional[str]
    manifest_compression: Optional[str]
    original_root_dir: Optional[str]
    container_id: Optional[int]
    container_format: str
    container_ordinal: Optional[int]
    artifact_id: Optional[int]
    artifact_kind: Optional[str]
    artifact_version: Optional[str]
    actual_artifact_bytes: Optional[int]
    tape_generation_id: Optional[int]
    archive_run_id: Optional[int]


class ScanMetrics:
    """Aggregate scan/planning telemetry for one pipeline run (Task 0.2).

    Purpose: make the three candidate scan models comparable with measurements
    instead of argument — how much time goes into *exploring* the source, how
    much into *database membership* work, and how much is pure *replay* of
    already-visited files after a restart.

    Two rules are structural, not stylistic:

    * **No individual file or directory name is ever recorded.** Every field is
      a count, a byte total or an elapsed time, so the metrics can be appended
      to the shared ``backup_logs/SUMMARY.csv`` backup row without leaking a
      source path (the existing ``source_host``/``source_path`` columns remain
      the only path-bearing fields, unchanged).
    * **Recording never changes control flow.** Every method is total: it takes
      a lock, adds a number, and returns. A metrics failure must not be able to
      stop a scan, a stage or a tape write, so nothing here raises on bad input.

    ``sql_executions`` deliberately counts *round trips*, separately from
    ``sql_rows``: the current membership filter is one bulk query per chunk, and
    conflating the two would reproduce the "one query per file" myth the
    characterization map exists to disprove.
    """

    __slots__ = ("_lock", "_t0", "enumeration_seconds", "entries_seen",
                 "entries_new", "entries_duplicate", "listing_starts",
                 "discarded_partial_entries", "membership_query_seconds",
                 "membership_query_paths", "membership_query_count",
                 "plan_insert_seconds", "plan_insert_rows", "plan_insert_calls",
                 "sql_executions", "sql_rows", "seconds_to_first_sealed_chunk",
                 "seconds_to_first_staged_chunk",
                 "seconds_to_first_writer_group")

    def __init__(self):
        self._lock = threading.Lock()
        self._t0 = time.monotonic()
        self.enumeration_seconds = 0.0
        self.entries_seen = 0
        self.entries_new = 0
        self.entries_duplicate = 0
        self.listing_starts = 0
        self.discarded_partial_entries = 0
        self.membership_query_seconds = 0.0
        self.membership_query_paths = 0
        self.membership_query_count = 0
        self.plan_insert_seconds = 0.0
        self.plan_insert_rows = 0
        self.plan_insert_calls = 0
        self.sql_executions = 0
        self.sql_rows = 0
        self.seconds_to_first_sealed_chunk = None
        self.seconds_to_first_staged_chunk = None
        self.seconds_to_first_writer_group = None

    # -- exploration ------------------------------------------------------
    def note_listing_start(self):
        """One remote listing was (re)started — a root today, a directory once
        the frontier lands. Counting restarts is how replay becomes visible."""
        with self._lock:
            self.listing_starts += 1

    def note_enumeration(self, seconds, entries):
        with self._lock:
            self.enumeration_seconds += max(0.0, float(seconds or 0))
            self.entries_seen += max(0, int(entries or 0))

    def note_discarded_partial(self, count=1):
        with self._lock:
            self.discarded_partial_entries += max(0, int(count or 0))

    # -- database membership ---------------------------------------------
    def note_membership_query(self, seconds, path_count, duplicates):
        with self._lock:
            self.membership_query_seconds += max(0.0, float(seconds or 0))
            self.membership_query_paths += max(0, int(path_count or 0))
            self.membership_query_count += 1
            self.sql_executions += 1
            self.sql_rows += max(0, int(path_count or 0))
            self.entries_duplicate += max(0, int(duplicates or 0))

    def note_plan_insert(self, seconds, rows):
        with self._lock:
            self.plan_insert_seconds += max(0.0, float(seconds or 0))
            self.plan_insert_rows += max(0, int(rows or 0))
            self.plan_insert_calls += 1
            self.sql_executions += 1
            self.sql_rows += max(0, int(rows or 0))
            self.entries_new += max(0, int(rows or 0))

    # -- latency milestones ----------------------------------------------
    def _mark(self, attr):
        with self._lock:
            if getattr(self, attr) is None:
                setattr(self, attr, round(time.monotonic() - self._t0, 3))

    def mark_first_sealed_chunk(self):
        self._mark("seconds_to_first_sealed_chunk")

    def mark_first_staged_chunk(self):
        self._mark("seconds_to_first_staged_chunk")

    def mark_first_writer_group(self):
        self._mark("seconds_to_first_writer_group")

    # -- export -----------------------------------------------------------
    def snapshot(self) -> Dict[str, Any]:
        """Flat ``scan_*`` mapping for the SUMMARY.csv backup row."""
        with self._lock:
            return {
                "scan_enumeration_seconds": round(self.enumeration_seconds, 3),
                "scan_entries_seen": self.entries_seen,
                "scan_entries_new": self.entries_new,
                "scan_entries_duplicate": self.entries_duplicate,
                "scan_listing_starts": self.listing_starts,
                "scan_discarded_partial_entries":
                    self.discarded_partial_entries,
                "scan_membership_query_seconds":
                    round(self.membership_query_seconds, 3),
                "scan_membership_query_paths": self.membership_query_paths,
                "scan_membership_query_count": self.membership_query_count,
                "scan_plan_insert_seconds": round(self.plan_insert_seconds, 3),
                "scan_plan_insert_rows": self.plan_insert_rows,
                "scan_plan_insert_calls": self.plan_insert_calls,
                "scan_sql_executions": self.sql_executions,
                "scan_sql_rows": self.sql_rows,
                "scan_seconds_to_first_sealed_chunk":
                    self.seconds_to_first_sealed_chunk,
                "scan_seconds_to_first_staged_chunk":
                    self.seconds_to_first_staged_chunk,
                "scan_seconds_to_first_writer_group":
                    self.seconds_to_first_writer_group,
            }


@dataclass(frozen=True)
class StagedContainer:
    """One database-identified container ready in local staging.

    ``data_path`` is the concrete file copied by the writer;
    ``temporary_data_locator`` is the locator persisted in
    ``archive_containers``.  They are kept explicit because later phases may
    store locators relative to a configured root while staging uses an absolute
    path.  ``database_validation_state`` is the state read back from PostgreSQL,
    so the in-memory handoff cannot claim readiness the database does not.
    """

    container_id: int
    session_id: int
    chunk_index: int
    container_ordinal: int
    container_format: ContainerFormat
    format_version: str
    storage_class: str
    container_name: str
    data_path: str
    temporary_data_locator: str
    data_size_bytes: int
    expected_member_count: int
    expected_logical_bytes: int
    observed_member_count: int
    observed_logical_bytes: int
    permanent_local_metadata_locator: Optional[str] = None
    validation_state: ContainerValidationState = ContainerValidationState.READY
    database_validation_state: ContainerValidationState = (
        ContainerValidationState.READY)
    tar_dialect: Optional[str] = None

    def __post_init__(self):
        object.__setattr__(
            self, "container_format", ContainerFormat(self.container_format))
        object.__setattr__(
            self, "validation_state",
            ContainerValidationState(self.validation_state))
        object.__setattr__(
            self, "database_validation_state",
            ContainerValidationState(self.database_validation_state))
        numeric = (
            self.container_id, self.session_id, self.chunk_index,
            self.container_ordinal, self.data_size_bytes,
            self.expected_member_count, self.expected_logical_bytes,
            self.observed_member_count, self.observed_logical_bytes,
        )
        if any(int(value) < 0 for value in numeric):
            raise ValueError("staged container identities/counts cannot be negative")
        if (not self.format_version or not self.storage_class
                or not self.container_name or not self.data_path
                or not self.temporary_data_locator):
            raise ValueError(
                "staged container format/storage/name/data locators are required")
        if self.container_format is ContainerFormat.STORED_TAR:
            if not self.tar_dialect:
                raise ValueError("Stored TAR container requires a persisted dialect")
        elif self.tar_dialect is not None:
            raise ValueError("ZIP container cannot carry a TAR dialect")

    def assert_writer_ready(self):
        if self.validation_state is not ContainerValidationState.READY:
            raise RuntimeError(
                f"container {self.container_id} is not locally ready: "
                f"{self.validation_state.value}")
        if self.database_validation_state is not self.validation_state:
            raise RuntimeError(
                f"container {self.container_id} readiness disagrees with the "
                "database")
        if self.data_path.lower().endswith(".part"):
            raise RuntimeError(
                f"container {self.container_id} still has a .part name")
        if int(self.expected_member_count) != int(self.observed_member_count):
            raise RuntimeError(
                f"container {self.container_id} member count disagrees with "
                "its expected count")
        if int(self.expected_logical_bytes) != int(self.observed_logical_bytes):
            raise RuntimeError(
                f"container {self.container_id} logical bytes disagree with "
                "its expected bytes")
        actual = _readable_regular_file_size(
            self.data_path, f"container {self.container_id}")
        if actual != int(self.data_size_bytes):
            raise RuntimeError(
                f"container {self.container_id} size changed: expected "
                f"{self.data_size_bytes}, found {actual}")


@dataclass(frozen=True)
class StagedArtifact:
    """One database-identified artifact copied beside a staged container."""

    artifact_id: int
    session_id: int
    chunk_index: int
    artifact_kind: ArtifactKind
    artifact_version: str
    staged_path: str
    local_locator: str
    staged_size_bytes: int
    readiness_state: ArtifactReadiness = ArtifactReadiness.READY
    database_readiness_state: ArtifactReadiness = ArtifactReadiness.READY
    container_id: Optional[int] = None

    def __post_init__(self):
        object.__setattr__(self, "artifact_kind", ArtifactKind(self.artifact_kind))
        object.__setattr__(
            self, "readiness_state", ArtifactReadiness(self.readiness_state))
        object.__setattr__(
            self, "database_readiness_state",
            ArtifactReadiness(self.database_readiness_state))
        if any(int(value) < 0 for value in (
                self.artifact_id, self.session_id, self.chunk_index,
                self.staged_size_bytes)):
            raise ValueError("staged artifact identities/sizes cannot be negative")
        if self.container_id is not None and int(self.container_id) < 0:
            raise ValueError("staged artifact container identity cannot be negative")
        if not self.artifact_version or not self.staged_path or not self.local_locator:
            raise ValueError("staged artifact version and locators are required")
        container_scoped = self.artifact_kind in {
            ArtifactKind.ZIP_MANIFEST, ArtifactKind.TAR_SIDECAR}
        if container_scoped != (self.container_id is not None):
            raise ValueError(
                f"artifact {self.artifact_kind.value} has the wrong scope")

    def assert_writer_ready(self):
        if self.readiness_state is not ArtifactReadiness.READY:
            raise RuntimeError(
                f"artifact {self.artifact_id} is not locally ready: "
                f"{self.readiness_state.value}")
        if self.database_readiness_state is not self.readiness_state:
            raise RuntimeError(
                f"artifact {self.artifact_id} readiness disagrees with the "
                "database")
        if self.staged_path.lower().endswith(".part"):
            raise RuntimeError(
                f"artifact {self.artifact_id} still has a .part name")
        actual = _readable_regular_file_size(
            self.staged_path, f"artifact {self.artifact_id}")
        if actual != int(self.staged_size_bytes):
            raise RuntimeError(
                f"artifact {self.artifact_id} size changed: expected "
                f"{self.staged_size_bytes}, found {actual}")


@dataclass
class StagedChunk:
    """A fetched-and-packed chunk, queued for the tape writer.

    ``fetch_seconds``/``pack_seconds`` are producer-side timings that overlap
    the previous chunk's tape write (see the SUMMARY.csv notes in AGENTS.md).
    ``skip_tape`` marks a chunk whose every source file went missing — it is
    logged and marked done without any tape I/O.
    """
    chunk_index: int
    fetch_dir: str
    pack_dir: str
    metadata: List[FileRecord]
    staged_bytes: int = 0
    fetch_seconds: Optional[float] = None
    fetch_bytes: Optional[int] = None
    pack_seconds: Optional[float] = None
    pack_bytes: Optional[int] = None
    ram_stats: dict = field(default_factory=dict)
    #: ``ScanMetrics.snapshot()`` taken when this chunk was staged, so the
    #: SUMMARY.csv row for its tape write carries the run's scan telemetry.
    scan_stats: dict = field(default_factory=dict)
    source_missing_files: list = field(default_factory=list)
    skip_tape: bool = False
    session_id: Optional[int] = None
    packaging_format: ContainerFormat = ContainerFormat.ZIP
    containers: List[StagedContainer] = field(default_factory=list)
    artifacts: List[StagedArtifact] = field(default_factory=list)
    writer_state: ContainerWriterState = ContainerWriterState.NOT_STARTED
    catalog_state: ContainerCatalogState = ContainerCatalogState.NOT_STARTED

    def __post_init__(self):
        self.packaging_format = ContainerFormat(self.packaging_format)
        self.writer_state = ContainerWriterState(self.writer_state)
        self.catalog_state = ContainerCatalogState(self.catalog_state)
        if int(self.staged_bytes) < 0:
            raise ValueError("staged_bytes cannot be negative")
        if self.skip_tape:
            self._assert_skip_tape_contract()
        elif self.packaging_format is ContainerFormat.STORED_TAR:
            if self.containers or self.artifacts:
                self._assert_container_identity_contract(require_readable=False)
            else:
                self._assert_loose_only_tar_contract(require_readable=False)
        elif self.containers or self.artifacts:
            # Historical ZIP descriptors intentionally have no migration-015
            # identity objects.  Once a ZIP descriptor does carry them, it is
            # subject to the same strict identity/state contract as TAR.
            self._assert_container_identity_contract(require_readable=False)
        else:
            self._assert_queued_states()

    @property
    def container_bytes(self):
        return sum(int(item.data_size_bytes) for item in self.containers)

    @property
    def artifact_bytes(self):
        return sum(int(item.staged_size_bytes) for item in self.artifacts)

    @property
    def loose_file_bytes(self):
        return sum(int(item.get("file_size_bytes", 0) or 0)
                   for item in self.metadata if not item.get("is_packed"))

    @property
    def prepared_bytes(self):
        """Actual files the writer admits: data + metadata + loose files."""
        if self.packaging_format is ContainerFormat.STORED_TAR:
            return self.container_bytes + self.artifact_bytes + self.loose_file_bytes
        return int(self.staged_bytes)

    def _assert_queued_states(self):
        if self.writer_state is not ContainerWriterState.NOT_STARTED:
            raise ValueError("queued chunk must not have started writing")
        if self.catalog_state is not ContainerCatalogState.NOT_STARTED:
            raise ValueError("queued chunk must not have started cataloging")

    def _assert_container_identity_contract(self, *, require_readable):
        if self.session_id is None:
            raise ValueError("identified StagedChunk requires session_id")
        if not self.containers:
            raise ValueError("identified StagedChunk requires container records")
        if not self.artifacts:
            raise ValueError("identified StagedChunk requires ready artifacts")

        container_ids = set()
        container_ordinals = set()
        container_by_id = {}
        declared_paths = {}
        for container in self.containers:
            if container.session_id != int(self.session_id):
                raise ValueError("staged container belongs to another session")
            if container.chunk_index != int(self.chunk_index):
                raise ValueError("staged container belongs to another chunk")
            if container.container_format is not self.packaging_format:
                raise ValueError(
                    "staged container format disagrees with its chunk")
            if container.container_id in container_ids:
                raise ValueError("duplicate staged container identity")
            if container.container_ordinal in container_ordinals:
                raise ValueError("duplicate staged container ordinal")
            _assert_path_below(
                self.pack_dir, container.data_path,
                f"container {container.container_id}")
            _declare_staged_path(
                declared_paths, container.data_path, container.data_size_bytes,
                f"container {container.container_id}")
            container_ids.add(container.container_id)
            container_ordinals.add(container.container_ordinal)
            container_by_id[container.container_id] = container

        required_kind = (ArtifactKind.TAR_SIDECAR
                         if self.packaging_format is ContainerFormat.STORED_TAR
                         else ArtifactKind.ZIP_MANIFEST)
        container_artifacts = {
            container_id: 0 for container_id in container_ids}
        artifact_ids = set()
        for artifact in self.artifacts:
            if artifact.session_id != int(self.session_id):
                raise ValueError("staged artifact belongs to another session")
            if artifact.chunk_index != int(self.chunk_index):
                raise ValueError("staged artifact belongs to another chunk")
            if artifact.artifact_id in artifact_ids:
                raise ValueError("duplicate staged artifact identity")
            artifact_ids.add(artifact.artifact_id)
            if artifact.container_id is not None:
                if artifact.container_id not in container_ids:
                    raise ValueError("staged artifact references an unknown container")
                if artifact.artifact_kind is required_kind:
                    container_artifacts[artifact.container_id] += 1
                else:
                    raise ValueError(
                        "container artifact kind disagrees with chunk format")
            _assert_path_below(
                self.pack_dir, artifact.staged_path,
                f"artifact {artifact.artifact_id}")
            _declare_staged_path(
                declared_paths, artifact.staged_path,
                artifact.staged_size_bytes, f"artifact {artifact.artifact_id}")
        if any(count != 1 for count in container_artifacts.values()):
            raise ValueError(
                f"each {self.packaging_format.value} container requires "
                f"exactly one {required_kind.value} artifact")

        expected = self.container_bytes + self.artifact_bytes + self.loose_file_bytes
        if int(self.staged_bytes) != expected:
            raise ValueError(
                f"staged byte total {self.staged_bytes} does not match "
                f"container/artifact/loose total {expected}")

        self._assert_queued_states()

        loose_paths = self._assert_file_records(
            container_by_id, require_readable)
        for path, size in loose_paths.items():
            _declare_staged_path(
                declared_paths, path, size, "loose staged file")

        if require_readable:
            for container in self.containers:
                container.assert_writer_ready()
            for artifact in self.artifacts:
                artifact.assert_writer_ready()
            _assert_exact_pack_inventory(self.pack_dir, declared_paths)

    def _assert_file_records(self, container_by_id, require_readable):
        """Validate packed/loose metadata using the chunk's durable format."""
        loose_paths = {}
        for record in self.metadata:
            is_packed = record.get("is_packed")
            if not isinstance(is_packed, bool):
                raise ValueError(
                    "identified file metadata requires a boolean is_packed")
            if is_packed:
                try:
                    record_format = ContainerFormat(record.get(
                        "container_format"))
                except (TypeError, ValueError) as exc:
                    raise ValueError(
                        "packed metadata requires container_format") \
                        from exc
                if record_format is not self.packaging_format:
                    raise ValueError(
                        "packed metadata format disagrees with its chunk")
                container_id = record.get("container_id")
                if container_id not in container_by_id:
                    raise ValueError(
                        "packed metadata references an unknown container")
                ordinal = record.get("container_ordinal")
                if (ordinal is not None and int(ordinal) !=
                        int(container_by_id[container_id].container_ordinal)):
                    raise ValueError(
                        "packed Stored TAR metadata has a conflicting "
                        "container ordinal")
                continue

            if record.get("container_id") is not None:
                raise ValueError(
                    "loose-file metadata cannot reference a container")
            try:
                logical_size = int(record["file_size_bytes"])
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError(
                    "loose-file metadata requires a non-negative size") from exc
            if logical_size < 0:
                raise ValueError(
                    "loose-file metadata requires a non-negative size")
            stored_path = record.get("stored_path")
            if not stored_path:
                raise ValueError("loose-file metadata requires stored_path")
            local_path = (stored_path if os.path.isabs(stored_path) else
                          os.path.join(self.pack_dir, stored_path))
            _assert_path_below(self.pack_dir, local_path, "loose staged file")
            path_key = os.path.normcase(os.path.abspath(local_path))
            if path_key in loose_paths:
                raise ValueError("duplicate loose staged file path")
            loose_paths[path_key] = (local_path, logical_size)
            if require_readable:
                actual = _readable_regular_file_size(
                    local_path, "loose staged file")
                if actual != logical_size:
                    raise RuntimeError(
                        "loose staged file size changed: expected "
                        f"{logical_size}, found {actual}")
        return {
            path: size for path, size in loose_paths.values()}

    def _assert_skip_tape_contract(self):
        """Prove that the no-write shortcut contains no archive object."""
        if self.session_id is None:
            raise ValueError("skip_tape StagedChunk requires session_id")
        if int(self.staged_bytes) != 0 or self.prepared_bytes != 0:
            raise ValueError("skip_tape chunk cannot contain staged bytes")
        if self.metadata or self.containers or self.artifacts:
            raise ValueError("skip_tape chunk cannot contain archive objects")
        if not self.source_missing_files:
            raise ValueError(
                "skip_tape requires explicit source-missing evidence")
        if self.writer_state is not ContainerWriterState.NOT_STARTED:
            raise ValueError("skip_tape chunk cannot have writer state")
        if self.catalog_state is not ContainerCatalogState.NOT_STARTED:
            raise ValueError("skip_tape chunk cannot have catalog state")

    def assert_writer_ready(self):
        """Fail before LTFS ownership if the staged handoff is incomplete."""
        if self.skip_tape:
            self._assert_skip_tape_contract()
            return True
        if (self.packaging_format is ContainerFormat.STORED_TAR
                and not (self.containers or self.artifacts)):
            self._assert_loose_only_tar_contract(require_readable=True)
        elif (self.packaging_format is ContainerFormat.STORED_TAR
                or self.containers or self.artifacts):
            self._assert_container_identity_contract(require_readable=True)
        else:
            self._assert_queued_states()
        return True

    def _assert_loose_only_tar_contract(self, *, require_readable):
        if self.session_id is None:
            raise ValueError("identified StagedChunk requires session_id")
        if not self.metadata:
            raise ValueError("Stored TAR chunk requires container records or loose files")
        if any(item.get("is_packed") for item in self.metadata):
            raise ValueError("packed Stored TAR metadata requires container records")
        loose_paths = self._assert_file_records({}, require_readable)
        expected = sum(int(item.get("file_size_bytes", 0) or 0)
                       for item in self.metadata)
        if int(self.staged_bytes) != expected:
            raise ValueError(
                f"staged byte total {self.staged_bytes} does not match "
                f"loose-file total {expected}")
        if require_readable:
            declared = {
                os.path.normcase(os.path.abspath(path)): int(size)
                for path, size in loose_paths.items()}
            _assert_exact_pack_inventory(self.pack_dir, declared)
        self._assert_queued_states()


def _readable_regular_file_size(path, label):
    """Open one local staged file and return its size, rejecting links/dirs."""
    try:
        path_stat = os.lstat(path)
        if not stat.S_ISREG(path_stat.st_mode):
            raise RuntimeError(f"{label} is not a regular file")
        with open(path, "rb") as handle:
            opened_stat = os.fstat(handle.fileno())
            if not stat.S_ISREG(opened_stat.st_mode):
                raise RuntimeError(f"{label} is not a regular file")
            return int(opened_stat.st_size)
    except RuntimeError:
        raise
    except OSError as exc:
        raise RuntimeError(f"{label} is not readable: {exc}") from exc


def _assert_path_below(root, candidate, label):
    """Require a staged object to live under the declared local pack root."""
    if not root:
        raise ValueError(f"{label} requires a pack directory")
    root_abs = os.path.abspath(root)
    candidate_abs = os.path.abspath(candidate)
    try:
        contained = os.path.commonpath((root_abs, candidate_abs)) == root_abs
    except ValueError:
        contained = False
    if not contained:
        raise ValueError(f"{label} is outside the staged pack directory")


def _declare_staged_path(declared, path, expected_size, label):
    """Add one copied object, rejecting cross-namespace path aliases."""
    key = os.path.normcase(os.path.abspath(path))
    if key in declared:
        raise ValueError(
            f"{label} reuses a staged path already owned by another object")
    declared[key] = int(expected_size)


def _assert_exact_pack_inventory(pack_dir, declared):
    """Prove Robocopy's complete source tree equals the declared handoff.

    The writer copies the whole pack directory.  Summing descriptor fields is
    therefore insufficient: an undeclared stale file would be written without
    being admitted or accounted.  Reparse points are rejected so lexical
    containment cannot escape the pack root.
    """
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    try:
        root_stat = os.lstat(pack_dir)
        if (not stat.S_ISDIR(root_stat.st_mode)
                or getattr(root_stat, "st_file_attributes", 0) & reparse_flag):
            raise RuntimeError("staged pack root is not a plain directory")
        actual = {}
        for root, dirs, files in os.walk(pack_dir, followlinks=False):
            for name in dirs:
                info = os.lstat(os.path.join(root, name))
                if (not stat.S_ISDIR(info.st_mode)
                        or getattr(info, "st_file_attributes", 0)
                        & reparse_flag):
                    raise RuntimeError(
                        "staged pack contains a reparse/non-directory entry")
            for name in files:
                path = os.path.join(root, name)
                key = os.path.normcase(os.path.abspath(path))
                if key in actual:
                    raise RuntimeError("staged pack contains a duplicate path")
                actual[key] = _readable_regular_file_size(
                    path, "staged pack file")
    except RuntimeError:
        raise
    except OSError as exc:
        raise RuntimeError(f"staged pack inventory is unreadable: {exc}") from exc

    missing = set(declared) - set(actual)
    extra = set(actual) - set(declared)
    mismatched = {
        path for path in set(declared) & set(actual)
        if int(declared[path]) != int(actual[path])}
    if missing or extra or mismatched:
        raise RuntimeError(
            "staged pack inventory disagrees with declared objects "
            f"(missing={len(missing)}, extra={len(extra)}, "
            f"size_mismatch={len(mismatched)})")


def validate_staged_chunk_writer_admission(
        staged_chunk, db, *, expected_session_id=None):
    """Validate one staged descriptor before any tape ownership/readiness.

    ZIP compatibility remains deliberately light-weight, but it must be
    explicit: an object with no format authority is not guessed to be ZIP.
    Stored TAR additionally requires the database comparison method; local
    readiness alone cannot prove that the in-memory identities are durable.
    """
    assert_ready = getattr(staged_chunk, "assert_writer_ready", None)
    if callable(assert_ready):
        assert_ready()

    if getattr(staged_chunk, "skip_tape", False) and not callable(assert_ready):
        if (int(getattr(staged_chunk, "staged_bytes", -1)) != 0
                or getattr(staged_chunk, "metadata", None)
                or getattr(staged_chunk, "containers", None)
                or getattr(staged_chunk, "artifacts", None)
                or not getattr(staged_chunk, "source_missing_files", None)):
            raise RuntimeError(
                "legacy skip_tape descriptor lacks empty/source-missing proof")

    raw_format = getattr(staged_chunk, "packaging_format", None)
    if raw_format is None:
        reader = getattr(db, "get_chunk_packaging_format", None)
        session_id = getattr(staged_chunk, "session_id", None)
        chunk_index = getattr(staged_chunk, "chunk_index", None)
        if not callable(reader) or session_id is None or chunk_index is None:
            raise RuntimeError(
                "staged chunk has no durable packaging-format authority")
        raw_format = reader(session_id, chunk_index)
    try:
        packaging_format = ContainerFormat(raw_format)
    except (TypeError, ValueError) as exc:
        raise RuntimeError("staged chunk has an unknown packaging format") \
            from exc

    descriptor_session = getattr(staged_chunk, "session_id", None)
    chunk_index = getattr(staged_chunk, "chunk_index", None)
    if (expected_session_id is not None
            and descriptor_session != int(expected_session_id)):
        raise RuntimeError(
            "staged chunk session identity disagrees with its finite group")

    # Every remote descriptor is compared with the durable per-chunk format.
    # This prevents a TAR-assigned chunk from being reinterpreted as legacy ZIP
    # by a stale or forged in-memory descriptor.
    if descriptor_session is not None and chunk_index is not None:
        reader = getattr(db, "get_chunk_packaging_format", None)
        if not callable(reader):
            raise RuntimeError(
                "staged chunk has no durable packaging-format reader")
        try:
            durable_format = ContainerFormat(
                reader(int(descriptor_session), int(chunk_index)))
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                "durable chunk packaging format is absent or unknown") from exc
        if durable_format is not packaging_format:
            raise RuntimeError(
                "staged descriptor disagrees with durable chunk format")

    identity_aware = bool(
        getattr(staged_chunk, "containers", None)
        or getattr(staged_chunk, "artifacts", None))
    if packaging_format is ContainerFormat.STORED_TAR or identity_aware:
        if not callable(assert_ready):
            raise RuntimeError(
                "identified descriptor has no local readiness validator")
        validate_db = getattr(db, "validate_staged_chunk_readiness", None)
        if not callable(validate_db):
            raise RuntimeError(
                "identified descriptor has no database readiness validator")
        validate_db(staged_chunk)
    return packaging_format


@dataclass
class StreamState:
    """Shared counters of a streaming remote session.

    Mutated by the scanner thread and read by the pipeline; every access to
    ``remaining_bytes``/``next_chunk_index`` happens under the session's
    ``remaining_lock`` (see RemoteOrchestrator._run_streaming_session).

    ``metrics`` carries its own lock, so it is the one field safe to touch from
    the scanner, stager and writer threads without ``remaining_lock``.
    """
    remaining_bytes: int = 0
    next_chunk_index: int = 0
    chunks: int = 0
    files: int = 0
    bytes: int = 0
    scan_error: Optional[str] = None
    metrics: ScanMetrics = field(default_factory=ScanMetrics)
