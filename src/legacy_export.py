"""Export fixed legacy chunk membership to local plan manifests.

Plan 3, Task 3.2. A chunk whose membership lives in ``remote_plan_files`` can
have that membership written out as a ``plan-manifest-v1`` artifact with
``provenance_kind='legacy_db_export'``. The export is **auxiliary evidence**:

* it is recorded separately from the authoritative plan locator;
* the chunk keeps ``plan_source='legacy_db'``;
* **no source row is deleted or modified** - Plan 4 owns deletion, and this
  plan does not begin Plan 4.

Two honesty rules make the export trustworthy rather than merely present:

**Scan-segment provenance is left null.** The current database genuinely cannot
prove which scan segment a legacy row came from. Writing a plausible-looking
identity would turn "unknown" into "known", which is precisely the failure the
rest of Plan 3 is built to avoid.

**Container routing is marked coarse unless it is proven.** A legacy ZIP bundle
that cannot be tied to a container and a member gets ``routing_precision='coarse'``
and a null container ordinal. Such an artifact is schema-valid, but
:func:`src.plan_manifest.is_authoritative` refuses it as authority, so it can
never drive row-free restore or qualify anything for pruning until an
exact-routing generation supersedes it.

**Scale.** A production session here holds ~29 million plan rows across ~113
chunks. Exporting all of it is not the default and never happens implicitly:
the caller names the chunks, and :func:`export_session_chunks` refuses an
unbounded request.
"""
from dataclasses import dataclass, field

from .archive_artifacts import ArtifactConflict, ArtifactError
from .logsetup import get_logger
from .plan_manifest import (
    plan_manifest_locator,
    read_plan_manifest,
    write_plan_manifest,
)

#: A deliberately conservative default. Exporting more than this in one call
#: has to be asked for explicitly, because the row counts here are enormous.
DEFAULT_MAX_CHUNKS = 8


class LegacyExportError(RuntimeError):
    """An export that must not be completed as if it were equivalent."""


@dataclass
class ChunkExportResult:
    chunk_index: int
    locator: str
    record_count: int
    byte_total: int
    created: bool
    routing_precision: str
    warnings: tuple = field(default_factory=tuple)


def _record(row, chunk_index, session_label, *, container_ordinal,
            routing_precision, storage_class, container_format,
            snapshot_id, normalized_ordinal):
    return {
        "canonical_path": row["remote_path"],
        "expected_size": int(row["file_size_bytes"]),
        "plan_ordinal": normalized_ordinal,
        "normalized_chunk_ordinal": normalized_ordinal,
        "session_label": session_label,
        "chunk_index": int(chunk_index),
        "provenance_kind": "legacy_db_export",
        "legacy_source_plan_file_id": int(row["plan_file_id"]),
        "legacy_original_ordinal": int(row["original_ordinal"]),
        "source_snapshot_id": int(snapshot_id),
        "storage_class": storage_class,
        "container_format": container_format,
        "container_ordinal": container_ordinal,
        "routing_precision": routing_precision,
        # Deliberately absent: scan_segment_id / scan_ordinal. The database
        # cannot prove them for a legacy row, and inventing them would be a lie
        # a rebuild would later trust.
    }


def export_chunk(db, archive_root, *, session_id, session_label, chunk_index,
                 generation=0, container_routing=None, packaging_format=None,
                 large_file_threshold_bytes=10 * 1024 * 1024):
    """Export one legacy chunk's membership. Reads only; writes one artifact.

    ``container_routing`` optionally maps a canonical source path to
    ``(container_ordinal, container_format)`` when per-member routing has been
    proven. Anything absent from it is exported as coarse.
    """
    routing = container_routing or {}
    warnings = []
    records = []
    snapshot_id = None
    normalized = 0
    saw_coarse = False

    for row in db.iter_legacy_chunk_membership(session_id, chunk_index):
        if snapshot_id is None:
            snapshot_id = row["snapshot_file_id"]
        proven = routing.get(row["remote_path"])
        size = int(row["file_size_bytes"])
        if proven is not None:
            container_ordinal, container_format = proven
            precision, storage_class = "exact", "container"
        elif size >= int(large_file_threshold_bytes):
            # A large file was stored loose, so there is no container to route
            # into and nothing coarse about saying so.
            container_ordinal, container_format = None, None
            precision, storage_class = "exact", "loose"
        else:
            # A small file lived in some ZIP, but WHICH ZIP and which member is
            # not provable from the catalog. Coarse, and it stays coarse.
            container_ordinal, container_format = None, "zip"
            precision, storage_class = "coarse", "container"
            saw_coarse = True
        records.append(_record(
            row, chunk_index, session_label,
            container_ordinal=container_ordinal,
            routing_precision=precision, storage_class=storage_class,
            container_format=container_format,
            snapshot_id=row["snapshot_file_id"], normalized_ordinal=normalized))
        normalized += 1

    if not records:
        raise LegacyExportError(
            f"chunk {chunk_index} of session {session_id} has no membership "
            "rows to export")

    locator = plan_manifest_locator(session_label, chunk_index,
                                    generation=generation)
    try:
        published = write_plan_manifest(
            archive_root, session_label=session_label, chunk_index=chunk_index,
            provenance_kind="legacy_db_export", records=records,
            generation=generation, packaging_format=packaging_format,
            extra_header={"export_kind": "auxiliary",
                          "authoritative": False})
        created = True
    except ArtifactConflict:
        # An equivalent export already exists. Adopt it only after proving it
        # matches; a same-named artifact with different membership is a defect.
        header, existing, trailer = read_plan_manifest(archive_root, locator)
        mine = [(r["plan_ordinal"], r["canonical_path"], r["expected_size"])
                for r in records]
        theirs = [(r["plan_ordinal"], r["canonical_path"], r["expected_size"])
                  for r in existing]
        if mine != theirs:
            raise LegacyExportError(
                f"an export already exists at {locator!r} with different "
                "membership; refusing to treat it as equivalent") from None
        published = _AdoptedExport(locator, len(existing),
                                   int(trailer.get("byte_total") or 0))
        created = False
    except ArtifactError as exc:
        raise LegacyExportError(
            f"legacy export for chunk {chunk_index} failed: {exc}") from exc

    if saw_coarse:
        warnings.append(
            "per-member container routing could not be proven for this chunk; "
            "the export is COARSE and cannot become authoritative, drive "
            "row-free restore, or qualify anything for pruning")

    get_logger().info(
        "legacy_chunk_exported: session=%s chunk=%s records=%d coarse=%s "
        "locator=%s", session_id, chunk_index, published.record_count,
        saw_coarse, locator)
    return ChunkExportResult(
        chunk_index=int(chunk_index), locator=locator,
        record_count=published.record_count,
        byte_total=published.byte_total, created=created,
        routing_precision="coarse" if saw_coarse else "exact",
        warnings=tuple(warnings))


@dataclass
class _AdoptedExport:
    locator: str
    record_count: int
    byte_total: int


def verify_export(db, archive_root, *, session_id, session_label, chunk_index,
                  generation=0):
    """Prove a published export equals the chunk's current DB membership."""
    _header, records, _trailer = read_plan_manifest(
        archive_root,
        plan_manifest_locator(session_label, chunk_index,
                              generation=generation))
    expected = [(row["remote_path"], int(row["file_size_bytes"]))
                for row in db.iter_legacy_chunk_membership(
                    session_id, chunk_index)]
    actual = [(r["canonical_path"], int(r["expected_size"])) for r in records]
    if expected != actual:
        raise LegacyExportError(
            f"export for chunk {chunk_index} no longer equals its database "
            f"membership ({len(actual)} exported vs {len(expected)} in the "
            "database)")
    ordinals = [int(r["plan_ordinal"]) for r in records]
    if ordinals != list(range(len(records))):
        raise LegacyExportError(
            f"export for chunk {chunk_index} has a broken chunk-local ordinal "
            "mapping")
    return {"chunk_index": int(chunk_index), "records": len(records),
            "equivalent": True}


def export_session_chunks(db, archive_root, *, session_id, session_label,
                          chunk_indexes, generation=0,
                          max_chunks=DEFAULT_MAX_CHUNKS, **kw):
    """Export a NAMED, BOUNDED set of chunks.

    There is no "export everything" mode. The production session this was built
    for holds ~29 million plan rows; an implicit full export would take hours
    and fill the metadata root, so the bound is a required argument rather than
    a suggestion.
    """
    chunks = sorted({int(index) for index in chunk_indexes})
    if not chunks:
        raise LegacyExportError("no chunks were named for export")
    if len(chunks) > int(max_chunks):
        raise LegacyExportError(
            f"refusing to export {len(chunks)} chunks in one call (bound is "
            f"{max_chunks}); name a smaller set or raise max_chunks "
            "deliberately")
    results = []
    for chunk_index in chunks:
        results.append(export_chunk(
            db, archive_root, session_id=session_id,
            session_label=session_label, chunk_index=chunk_index,
            generation=generation, **kw))
    return results
