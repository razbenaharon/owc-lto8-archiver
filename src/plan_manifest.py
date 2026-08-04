"""``plan-manifest-v1`` - the immutable membership of one chunk.

Plan 3, Task 1.2. This artifact is the whole point of the manifest-first
design: a chunk planned from the frontier owns **no** ``remote_plan_files`` or
``remote_snapshot_files`` rows, so this file *is* its membership. Everything
downstream - staging, the writer, the terminal manifest, the directory catalog,
the shadow rebuild - reads the plan from here.

Two provenance kinds share the schema, and they are deliberately not
interchangeable:

``frontier``
    A chunk planned from committed scan evidence. It can prove exactly where
    every member came from, so it *must*: scan-segment identity and ordinal are
    required, routing precision is always ``exact``, and every container member
    carries a stable container ordinal.

``legacy_db_export``
    A non-destructive export of a chunk that already has PostgreSQL membership
    (Task 3.2). The current database cannot prove scan-segment provenance for
    those rows, so those fields are **null** rather than invented. Where the
    per-member container routing also cannot be proven, the record is marked
    ``coarse`` and the artifact is auxiliary evidence only - it can never become
    authoritative, drive row-free restore, or qualify a row for pruning until a
    later exact-routing generation supersedes it.

No content hashes. A same-size, same-structure replacement of a source file is
undetectable here; that residual risk is recorded rather than papered over with
a hash the pipeline cannot afford to compute.
"""
from .archive_artifacts import (
    PLAN_MANIFEST_NAMESPACE,
    PLAN_MANIFEST_VERSION,
    ArtifactError,
    Plan3ArtifactWriter,
    plan3_artifact_locator,
    read_plan3_artifact,
)

PROVENANCE_KINDS = ("frontier", "legacy_db_export")
STORAGE_CLASSES = ("container", "loose")
CONTAINER_FORMATS = ("zip", "stored_tar")
ROUTING_PRECISIONS = ("exact", "coarse")


def plan_manifest_locator(session_label, chunk_index, *, generation=0):
    return plan3_artifact_locator(
        PLAN_MANIFEST_NAMESPACE, session_label, chunk_index=chunk_index,
        kind="plan", version=PLAN_MANIFEST_VERSION, generation=generation)


def _require(record, field, where):
    value = record.get(field)
    if value is None or (isinstance(value, str) and not value.strip()):
        raise ArtifactError(f"plan-manifest {where}: {field} is required")
    return value


def _refuse(record, field, where, why):
    if record.get(field) is not None:
        raise ArtifactError(
            f"plan-manifest {where}: {field} must be null {why}")


def validate_plan_record(record, header):
    """Enforce every conditional rule for one detail record.

    Called both while writing and again on the bytes read back from disk.
    """
    ordinal = record.get("plan_ordinal")
    if ordinal is None or int(ordinal) < 0:
        raise ArtifactError(
            "plan-manifest record: plan_ordinal must be a non-negative int")
    where = f"ordinal {int(ordinal)}"

    _require(record, "canonical_path", where)
    size = record.get("expected_size")
    if size is None or int(size) < 0:
        raise ArtifactError(
            f"plan-manifest {where}: expected_size must be a non-negative int")

    # Stable logical identity must agree with the header on every record, so a
    # record can never be lifted out of one chunk's artifact into another's.
    for field in ("session_label", "chunk_index"):
        if record.get(field) != header.get(field):
            raise ArtifactError(
                f"plan-manifest {where}: {field}={record.get(field)!r} "
                f"disagrees with the header {header.get(field)!r}")

    kind = record.get("provenance_kind")
    if kind not in PROVENANCE_KINDS:
        raise ArtifactError(
            f"plan-manifest {where}: provenance_kind must be one of "
            f"{PROVENANCE_KINDS}, got {kind!r}")

    storage_class = record.get("storage_class")
    if storage_class not in STORAGE_CLASSES:
        raise ArtifactError(
            f"plan-manifest {where}: storage_class must be one of "
            f"{STORAGE_CLASSES}, got {storage_class!r}")

    precision = record.get("routing_precision")
    if precision not in ROUTING_PRECISIONS:
        raise ArtifactError(
            f"plan-manifest {where}: routing_precision must be one of "
            f"{ROUTING_PRECISIONS}, got {precision!r}")

    container_format = record.get("container_format")
    container_ordinal = record.get("container_ordinal")

    if storage_class == "loose":
        # A loose file is not in a container, so claiming one is a routing lie.
        _refuse(record, "container_format", where, "for a loose record")
        _refuse(record, "container_ordinal", where, "for a loose record")
    else:
        if container_format not in CONTAINER_FORMATS:
            raise ArtifactError(
                f"plan-manifest {where}: container_format must be one of "
                f"{CONTAINER_FORMATS} for a container record, got "
                f"{container_format!r}")

    if kind == "frontier":
        _require(record, "scan_segment_id", where)
        if record.get("scan_ordinal") is None:
            raise ArtifactError(
                f"plan-manifest {where}: frontier records require scan_ordinal")
        if precision != "exact":
            raise ArtifactError(
                f"plan-manifest {where}: a frontier record is always "
                f"routing_precision='exact', got {precision!r}")
        if storage_class == "container" and container_ordinal is None:
            raise ArtifactError(
                f"plan-manifest {where}: a frontier container member requires "
                "a stable container_ordinal")
    else:                                    # legacy_db_export
        _require(record, "legacy_source_plan_file_id", where)
        if record.get("legacy_original_ordinal") is None:
            raise ArtifactError(
                f"plan-manifest {where}: a legacy export requires "
                "legacy_original_ordinal")
        _require(record, "source_snapshot_id", where)
        # The normalized chunk-local ordinal IS plan_ordinal; state it too so
        # the mapping is explicit in the file rather than implied by position.
        if int(record.get("normalized_chunk_ordinal", ordinal)) != int(ordinal):
            raise ArtifactError(
                f"plan-manifest {where}: normalized_chunk_ordinal must equal "
                "plan_ordinal")
        if (storage_class == "container" and container_ordinal is None
                and precision != "coarse"):
            raise ArtifactError(
                f"plan-manifest {where}: a legacy container member may omit "
                "container_ordinal only with routing_precision='coarse'")

    return record


def _validate_plan_artifact(header, records, trailer):
    """Whole-artifact rules that a single record cannot see."""
    kinds = {r.get("provenance_kind") for r in records}
    if len(kinds) > 1:
        raise ArtifactError(
            f"plan-manifest mixes provenance kinds {sorted(kinds)}; one "
            "artifact describes one provenance")
    declared = header.get("provenance_kind")
    if kinds and declared not in kinds:
        raise ArtifactError(
            f"plan-manifest header provenance_kind={declared!r} disagrees with "
            f"its records {sorted(kinds)}")
    if records:
        ordinals = [int(r["plan_ordinal"]) for r in records]
        if ordinals[0] != 0:
            raise ArtifactError(
                "plan-manifest chunk-local ordinals must start at 0, got "
                f"{ordinals[0]}")
        if ordinals != list(range(len(ordinals))):
            raise ArtifactError(
                "plan-manifest chunk-local ordinals must be dense and "
                "contiguous from 0")
    if header.get("routing_authority") == "authoritative" and any(
            r.get("routing_precision") == "coarse" for r in records):
        raise ArtifactError(
            "a plan manifest containing coarse routing cannot be published as "
            "authoritative; it is auxiliary evidence until an exact-routing "
            "generation supersedes it")


def is_authoritative(header, records):
    """True only when every member is exactly routed.

    Coarse evidence is real and worth keeping, but it must never satisfy
    row-free restore, rebuild equivalence, or pruning eligibility.
    """
    if header.get("provenance_kind") == "frontier":
        return True
    return all(r.get("routing_precision") == "exact" for r in records)


def write_plan_manifest(archive_root, *, session_label, chunk_index,
                        provenance_kind, records, generation=0,
                        packaging_format=None, source_scopes=None,
                        prior_locator=None, tape_label=None,
                        tape_generation_id=None, extra_header=None):
    """Publish one plan manifest through the two-phase protocol.

    Returns the :class:`PublishedPlan3Artifact` the caller persists.
    """
    if provenance_kind not in PROVENANCE_KINDS:
        raise ArtifactError(
            f"unknown provenance_kind {provenance_kind!r}")
    locator = plan_manifest_locator(session_label, chunk_index,
                                    generation=generation)
    header = {
        "session_label": str(session_label),
        "chunk_index": int(chunk_index),
        "provenance_kind": provenance_kind,
        "generation": int(generation),
        "packaging_format": packaging_format,
        "source_scopes": list(source_scopes or ()),
        "prior_artifact_locator": prior_locator,
        "tape_label": tape_label,
        "tape_generation_id": tape_generation_id,
    }
    if extra_header:
        header.update(extra_header)

    writer = Plan3ArtifactWriter(
        archive_root, locator, version=PLAN_MANIFEST_VERSION, header=header,
        validate_record=validate_plan_record,
        validate_artifact=_validate_plan_artifact,
        size_field="expected_size", ordinal_field="plan_ordinal",
        path_field="canonical_path")
    with writer:
        for record in records:
            writer.add(record)
    return writer.publish()


def read_plan_manifest(archive_root, locator):
    """Parse and fully re-validate a published plan manifest."""
    return read_plan3_artifact(
        archive_root, locator, expected_version=PLAN_MANIFEST_VERSION,
        validate_artifact=_plan_reader_validation)


def _plan_reader_validation(header, records, trailer):
    for record in records:
        validate_plan_record(record, header)
    _validate_plan_artifact(header, records, trailer)
