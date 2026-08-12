"""``scan-state-segment-v1`` and ``session-descriptor-v1``.

Plan 3, Task 1.2. These two artifacts exist so a session can be rebuilt without
the source database (Task 4.2). They are published from the *same committed
frontier evidence* Plan 1 already persists - they never re-derive scan state by
walking the source, and they never touch tape.

``scan-state-segment-v1``
    Append-only generations of the frontier's own state: scopes, the directory
    queue topology including empty directories, listing state, traversal-only
    recursive coverage, planning state, partial continuation, source
    observations, exclusions and errors, coverage finality, segment locators and
    ordinal ranges, and every ``remote_chunk_scan_segments`` consumption range.

    Listing, traversal and planning state are recorded **independently**. A
    directory can be fully listed but not yet planned, or traversed for coverage
    without being planned at all; collapsing those into one "done" flag is how a
    rebuild silently loses work that had not been planned yet.

``session-descriptor-v1``
    A small append-only generation published at session creation and update. It
    names the complete ordered set of scan-state generations, so an empty or
    partially planned session is still rebuildable. It is the entry point of the
    rebuild: find the latest ready descriptor, and it tells you everything else
    to read.
"""
from .archive_artifacts import (
    SCAN_STATE_SEGMENT_NAMESPACE,
    SCAN_STATE_SEGMENT_VERSION,
    SESSION_DESCRIPTOR_NAMESPACE,
    SESSION_DESCRIPTOR_VERSION,
    ArtifactError,
    Plan3ArtifactWriter,
    plan3_artifact_locator,
    read_plan3_artifact,
)

COVERAGE_STATES = ("provisional", "final", "error")
DIRECTORY_STATES = ("pending", "listing", "listed", "traversed", "planned",
                    "excluded", "error")
RECORD_TYPES = ("scope", "directory", "segment", "consumption", "exclusion",
                "error")


def scan_state_locator(session_label, *, generation=0):
    return plan3_artifact_locator(
        SCAN_STATE_SEGMENT_NAMESPACE, session_label, kind="scanstate",
        version=SCAN_STATE_SEGMENT_VERSION, generation=generation)


def session_descriptor_locator(session_label, *, generation=0):
    return plan3_artifact_locator(
        SESSION_DESCRIPTOR_NAMESPACE, session_label, kind="session",
        version=SESSION_DESCRIPTOR_VERSION, generation=generation)


def validate_scan_state_record(record, header):
    record_type = record.get("record_type")
    if record_type not in RECORD_TYPES:
        raise ArtifactError(
            f"scan-state: record_type must be one of {RECORD_TYPES}, got "
            f"{record_type!r}")
    if record.get("session_label") != header.get("session_label"):
        raise ArtifactError(
            "scan-state: record session_label disagrees with the header")

    if record_type == "scope":
        if not str(record.get("root_path") or "").strip():
            raise ArtifactError("scan-state scope: root_path is required")
        if record.get("coverage_state") not in COVERAGE_STATES:
            raise ArtifactError(
                f"scan-state scope {record.get('root_path')!r}: coverage_state "
                f"must be one of {COVERAGE_STATES}")

    elif record_type == "directory":
        if record.get("directory_id") is None:
            raise ArtifactError("scan-state directory: directory_id required")
        if not str(record.get("path") or "").strip():
            raise ArtifactError("scan-state directory: path is required")
        if record.get("state") not in DIRECTORY_STATES:
            raise ArtifactError(
                f"scan-state directory {record.get('path')!r}: state must be "
                f"one of {DIRECTORY_STATES}, got {record.get('state')!r}")
        # Independent, never collapsed: a rebuild must be able to tell a
        # directory that was traversed for coverage from one that was planned.
        for flag in ("listing_complete", "traversal_complete",
                     "planning_complete"):
            if not isinstance(record.get(flag), bool):
                raise ArtifactError(
                    f"scan-state directory {record.get('path')!r}: {flag} must "
                    "be an explicit boolean")
        if record.get("planning_complete") and not record.get(
                "listing_complete"):
            raise ArtifactError(
                f"scan-state directory {record.get('path')!r}: cannot be "
                "planned without having been listed")
        if record.get("child_count") is None:
            raise ArtifactError(
                f"scan-state directory {record.get('path')!r}: child_count is "
                "required so empty directories are preserved explicitly")

    elif record_type == "segment":
        for field in ("segment_id", "locator", "first_ordinal", "last_ordinal"):
            if record.get(field) is None:
                raise ArtifactError(f"scan-state segment: {field} is required")
        if str(record["locator"]).endswith(".part"):
            raise ArtifactError(
                "scan-state segment: a .part locator is never ready evidence")
        if int(record["last_ordinal"]) < int(record["first_ordinal"]):
            raise ArtifactError(
                f"scan-state segment {record['segment_id']!r}: ordinal range is "
                "inverted")

    elif record_type == "consumption":
        for field in ("segment_id", "chunk_index", "first_ordinal",
                      "last_ordinal"):
            if record.get(field) is None:
                raise ArtifactError(
                    f"scan-state consumption: {field} is required")
        if int(record["last_ordinal"]) < int(record["first_ordinal"]):
            raise ArtifactError(
                "scan-state consumption: ordinal range is inverted")
    return record


def _validate_scan_state_artifact(header, records, trailer):
    segments = {r["segment_id"]: r for r in records
                if r.get("record_type") == "segment"}
    for record in records:
        if record.get("record_type") != "consumption":
            continue
        segment = segments.get(record["segment_id"])
        if segment is None:
            # A consumption range naming a segment this generation does not
            # describe cannot be checked, and an unverifiable range is exactly
            # what would let a rebuild replan covered work.
            raise ArtifactError(
                f"scan-state: consumption names unknown segment "
                f"{record['segment_id']!r}")
        if (int(record["first_ordinal"]) < int(segment["first_ordinal"])
                or int(record["last_ordinal"]) > int(segment["last_ordinal"])):
            raise ArtifactError(
                f"scan-state: chunk {record['chunk_index']} consumes "
                f"{record['first_ordinal']}..{record['last_ordinal']} which "
                f"escapes segment {record['segment_id']!r} range "
                f"{segment['first_ordinal']}..{segment['last_ordinal']}")

    declared = header.get("coverage_final")
    if declared:
        unfinished = [r for r in records
                      if r.get("record_type") == "scope"
                      and r.get("coverage_state") != "final"]
        if unfinished:
            raise ArtifactError(
                f"scan-state claims coverage_final but {len(unfinished)} "
                "scopes are not final")


def write_scan_state_segment(archive_root, *, session_label, records,
                             generation=0, frontier_generation=None,
                             coverage_final=False, prior_locator=None,
                             extra_header=None):
    """Publish one append-only scan-state generation."""
    header = {
        "session_label": str(session_label),
        "generation": int(generation),
        "frontier_generation": frontier_generation,
        "coverage_final": bool(coverage_final),
        "prior_artifact_locator": prior_locator,
    }
    if extra_header:
        header.update(extra_header)
    locator = scan_state_locator(session_label, generation=generation)
    writer = Plan3ArtifactWriter(
        archive_root, locator, version=SCAN_STATE_SEGMENT_VERSION,
        header=header, validate_record=validate_scan_state_record,
        validate_artifact=_validate_scan_state_artifact,
        size_field=None, ordinal_field=None, path_field=None,
        require_unique_paths=False)
    with writer:
        for record in records:
            writer.add(record)
    return writer.publish()


def read_scan_state_segment(archive_root, locator):
    return read_plan3_artifact(
        archive_root, locator, expected_version=SCAN_STATE_SEGMENT_VERSION,
        validate_artifact=_scan_state_reader_validation)


def _scan_state_reader_validation(header, records, trailer):
    for record in records:
        validate_scan_state_record(record, header)
    _validate_scan_state_artifact(header, records, trailer)


# ---------------------------------------------------------------------------
# session-descriptor-v1
# ---------------------------------------------------------------------------


def validate_descriptor_record(record, header):
    kind = record.get("record_type")
    if kind not in ("scan_state_generation", "scope", "chunk"):
        raise ArtifactError(
            f"session-descriptor: unknown record_type {kind!r}")
    if record.get("session_label") != header.get("session_label"):
        raise ArtifactError(
            "session-descriptor: record session_label disagrees with header")
    if kind == "scan_state_generation":
        for field in ("generation", "locator"):
            if record.get(field) is None:
                raise ArtifactError(
                    f"session-descriptor generation: {field} is required")
        if str(record["locator"]).endswith(".part"):
            raise ArtifactError(
                "session-descriptor: a .part locator is never ready evidence")
    return record


def _validate_descriptor_artifact(header, records, trailer):
    generations = [int(r["generation"]) for r in records
                   if r.get("record_type") == "scan_state_generation"]
    if generations != sorted(generations):
        raise ArtifactError(
            "session-descriptor: scan-state generations must be listed in order")
    if len(generations) != len(set(generations)):
        raise ArtifactError(
            "session-descriptor: duplicate scan-state generation")
    if generations and generations != list(range(generations[0],
                                                 generations[0] + len(generations))):
        # A gap means some generation was never published or has been pruned;
        # a rebuild that skipped it would silently lose that scan state.
        raise ArtifactError(
            f"session-descriptor: scan-state generations have a gap: "
            f"{generations}")


def write_session_descriptor(archive_root, *, session_label, records,
                             generation=0, session_state=None,
                             source_host=None, scan_complete=None,
                             prior_locator=None, extra_header=None):
    """Publish one append-only session descriptor generation."""
    header = {
        "session_label": str(session_label),
        "generation": int(generation),
        "session_state": session_state,
        "source_host": source_host,
        "scan_complete": scan_complete,
        "prior_artifact_locator": prior_locator,
    }
    if extra_header:
        header.update(extra_header)
    locator = session_descriptor_locator(session_label, generation=generation)
    writer = Plan3ArtifactWriter(
        archive_root, locator, version=SESSION_DESCRIPTOR_VERSION,
        header=header, validate_record=validate_descriptor_record,
        validate_artifact=_validate_descriptor_artifact,
        size_field=None, ordinal_field=None, path_field=None,
        require_unique_paths=False)
    with writer:
        for record in records:
            writer.add(record)
    return writer.publish()


def read_session_descriptor(archive_root, locator):
    return read_plan3_artifact(
        archive_root, locator, expected_version=SESSION_DESCRIPTOR_VERSION,
        validate_artifact=_descriptor_reader_validation)


def _descriptor_reader_validation(header, records, trailer):
    for record in records:
        validate_descriptor_record(record, header)
    _validate_descriptor_artifact(header, records, trailer)
