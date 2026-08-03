"""Typed state shared across the archive pipeline.

These replace string-keyed dicts whose typos were silent ``None``s at
runtime: :class:`StagedChunk` crosses the producer -> tape-writer thread
boundary, :class:`StreamState` is the streaming session's shared counters,
and :class:`FileRecord` annotates the packer/catalog metadata records
(annotation only — the records stay plain dicts because the DB layer
consumes them via ``.get()``).
"""
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, TypedDict


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
