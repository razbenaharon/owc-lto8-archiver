"""Seal manifest-first chunks, and publish their terminal state.

Plan 3, Tasks 1.4 and 1.5. Two flows live here because they are the two ends of
one chunk's life and share the same ordering discipline.

**Sealing (Task 1.4).** Consume ready scan segments, accumulate one bounded
chunk, assign stable chunk-local plan ordinals and stable container ordinals,
publish and validate the plan manifest, then atomically insert the chunk with
``plan_source='manifest'`` and ``membership_state='sealed'``.

The artifact is published *before* the database row on purpose. The two orders
fail differently:

* artifact first, row second - a crash leaves a ready artifact and no chunk.
  Recoverable: look the artifact up by its stable identity and finish, which is
  exactly what :meth:`ManifestChunkSealer.seal` does on re-entry.
* row first, artifact second - a crash leaves a chunk whose membership does not
  exist anywhere. Not recoverable: the chunk's files are unknowable.

So the recoverable order is the only acceptable one, and the idempotency lookup
is what makes it recoverable rather than merely survivable.

**Terminal state (Task 1.5).** Publish ``terminal-state-v1`` only once every
plan ordinal has exactly one final disposition and every writer/catalog state
needed to justify ``archived`` is durable. An ambiguous writer outcome - a copy
that might have reached tape - is ``unresolved``, never a source exception and
never ``archived``. Resolving it any other way would require reading the tape.
"""
from dataclasses import dataclass, field

from .archive_artifacts import ArtifactConflict, ArtifactError
from .logsetup import get_logger
from .plan_manifest import plan_manifest_locator, write_plan_manifest
from .terminal_manifest import (
    archived_bytes,
    disposition_counts,
    is_publishable,
    terminal_manifest_locator,
    write_terminal_manifest,
)


class ManifestSealError(RuntimeError):
    """A sealing step that must stop the run rather than guess."""


@dataclass
class PendingChunk:
    """One bounded chunk being accumulated from ready scan segments."""

    session_label: str
    chunk_index: int
    packaging_format: str = "stored_tar"
    max_files: int = 200_000
    max_bytes: int = 0                        # 0 = no byte bound
    members: list = field(default_factory=list)
    consumed: list = field(default_factory=list)
    _container_ordinal: int = 0

    @property
    def file_count(self):
        return len(self.members)

    @property
    def byte_total(self):
        return sum(int(m["expected_size"]) for m in self.members)

    def has_room(self, size_bytes):
        if self.file_count >= self.max_files:
            return False
        if self.max_bytes and self.byte_total + int(size_bytes) > self.max_bytes:
            return bool(not self.members)     # never reject the first member
        return True

    def add(self, *, path, size, scan_segment_id, scan_ordinal,
            storage_class="container", container_ordinal=None):
        """Append one member with a stable chunk-local ordinal."""
        ordinal = len(self.members)
        if storage_class == "container" and container_ordinal is None:
            container_ordinal = self._container_ordinal
        record = {
            "canonical_path": str(path),
            "expected_size": int(size),
            "plan_ordinal": ordinal,
            "session_label": self.session_label,
            "chunk_index": int(self.chunk_index),
            "provenance_kind": "frontier",
            "scan_segment_id": scan_segment_id,
            "scan_ordinal": int(scan_ordinal),
            "storage_class": storage_class,
            "container_format": (self.packaging_format
                                 if storage_class == "container" else None),
            "container_ordinal": (container_ordinal
                                  if storage_class == "container" else None),
            "routing_precision": "exact",
        }
        self.members.append(record)
        return ordinal

    def note_consumption(self, segment_id, first_ordinal, last_ordinal):
        self.consumed.append((segment_id, int(first_ordinal),
                              int(last_ordinal)))


class ManifestChunkSealer:
    """Publish a plan artifact, then atomically create its chunk."""

    def __init__(self, db, archive_root, session_id, session_label):
        self.db = db
        self.archive_root = archive_root
        self.session_id = int(session_id)
        self.session_label = str(session_label)

    def seal(self, pending, *, generation=0, source_scopes=None):
        """Seal ``pending``. Safe to re-enter after any crash point.

        Returns ``{"chunk_index", "locator", "created", "file_count",
        "byte_total"}``.
        """
        if not pending.members:
            raise ManifestSealError(
                "refusing to seal an empty manifest chunk; an empty chunk "
                "would consume scan segments and archive nothing")

        chunk_index = int(pending.chunk_index)
        locator = plan_manifest_locator(self.session_label, chunk_index,
                                        generation=generation)

        # Crash point A: an artifact may already be ready from a previous
        # attempt whose database outcome we never learned. Ask by stable
        # identity BEFORE allocating anything.
        existing = self.db.find_manifest_chunk_by_artifact(
            self.session_id, chunk_index, locator)
        if existing is not None:
            # Idempotent ONLY for the identical plan. A different membership at
            # an index that is already sealed is a frontier defect - two plans
            # for one chunk - and must stop the run, not be absorbed as "already
            # done". A sealed chunk never gains (or loses) members; a late
            # discovery belongs in a later chunk.
            self._assert_equivalent(pending, locator)
            get_logger().info(
                "manifest_chunk_already_sealed: session=%s chunk=%s locator=%s",
                self.session_id, chunk_index, locator)
            return {"chunk_index": chunk_index, "locator": locator,
                    "created": False,
                    "file_count": int(existing["expected_file_count"]),
                    "byte_total": int(existing["expected_bytes"])}

        published = self._publish(pending, locator, generation, source_scopes)

        # Crash point B: artifact is ready, row is not yet written. Re-entry
        # lands on the lookup above and finishes without a second chunk.
        result = self.db.create_manifest_chunk(
            self.session_id, chunk_index,
            plan_locator=published.locator,
            packaging_format=pending.packaging_format,
            expected_file_count=published.record_count,
            expected_bytes=published.byte_total,
            artifact_size_bytes=published.size_bytes,
            scan_segment_consumption=pending.consumed)

        get_logger().info(
            "manifest_chunk_sealed: session=%s chunk=%s files=%d bytes=%d "
            "format=%s locator=%s", self.session_id, chunk_index,
            published.record_count, published.byte_total,
            pending.packaging_format, published.locator)
        return {"chunk_index": chunk_index, "locator": published.locator,
                "created": bool(result.get("created")),
                "file_count": published.record_count,
                "byte_total": published.byte_total}

    def _publish(self, pending, locator, generation, source_scopes):
        try:
            return write_plan_manifest(
                self.archive_root, session_label=self.session_label,
                chunk_index=pending.chunk_index, provenance_kind="frontier",
                records=pending.members, generation=generation,
                packaging_format=pending.packaging_format,
                source_scopes=source_scopes)
        except ArtifactConflict:
            # The final name is already taken by an equivalent attempt. Adopt it
            # only after a full re-parse proves it matches what we would have
            # written; a same-named artifact with different membership is a
            # frontier defect, not a race to accept.
            return self._adopt_equivalent(pending, locator)
        except ArtifactError as exc:
            raise ManifestSealError(
                f"plan manifest for chunk {pending.chunk_index} could not be "
                f"published: {exc}") from exc

    def _assert_equivalent(self, pending, locator):
        """Refuse unless the published artifact IS this pending membership."""
        from .plan_manifest import read_plan_manifest

        header, records, trailer = read_plan_manifest(self.archive_root,
                                                      locator)
        expected = [(int(m["plan_ordinal"]), m["canonical_path"],
                     int(m["expected_size"])) for m in pending.members]
        actual = [(int(r["plan_ordinal"]), r["canonical_path"],
                   int(r["expected_size"])) for r in records]
        if expected != actual:
            raise ManifestSealError(
                f"an artifact already exists at {locator!r} with DIFFERENT "
                f"membership ({len(actual)} members published, "
                f"{len(expected)} offered); a sealed chunk never changes. "
                "Two plans targeted one chunk index - reconcile the frontier "
                "rather than letting one win.")
        return header, records, trailer

    def _adopt_equivalent(self, pending, locator):
        import os

        from .archive_artifacts import PublishedPlan3Artifact, resolve_locator

        header, records, trailer = self._assert_equivalent(pending, locator)
        path = resolve_locator(self.archive_root, locator)
        return PublishedPlan3Artifact(
            locator=locator, path=path, version=header["version"],
            size_bytes=os.path.getsize(path), record_count=len(records),
            byte_total=int(trailer.get("byte_total") or 0),
            header=header, trailer=trailer)


class TerminalStatePublisher:
    """Publish a chunk's final dispositions, once and only when provable."""

    def __init__(self, db, archive_root, session_id, session_label):
        self.db = db
        self.archive_root = archive_root
        self.session_id = int(session_id)
        self.session_label = str(session_label)

    def publish(self, chunk_index, *, plan_records, outcomes, generation=0,
                plan_locator=None, packaging_format=None):
        """Publish ``terminal-state-v1`` for one chunk.

        ``outcomes`` maps plan ordinal -> a partial disposition record. Every
        planned ordinal must appear exactly once, or nothing is published:
        a terminal manifest missing an ordinal would let a directory that never
        finished look complete.
        """
        plan_ordinals = [int(r["plan_ordinal"]) for r in plan_records]
        records = self._build(plan_records, outcomes, chunk_index)

        ok, reason = is_publishable(plan_ordinals, records)
        if not ok:
            raise ManifestSealError(
                f"refusing to publish terminal state for chunk {chunk_index}: "
                f"{reason}")

        locator = terminal_manifest_locator(self.session_label, chunk_index,
                                            generation=generation)
        try:
            published = write_terminal_manifest(
                self.archive_root, session_label=self.session_label,
                chunk_index=chunk_index, records=records,
                plan_ordinals=plan_ordinals, generation=generation,
                plan_artifact_locator=plan_locator,
                packaging_format=packaging_format)
        except ArtifactConflict:
            get_logger().info(
                "terminal_manifest_already_published: chunk=%s locator=%s",
                chunk_index, locator)
            return {"locator": locator, "created": False}

        self.db.record_terminal_manifest(
            self.session_id, chunk_index,
            terminal_locator=published.locator,
            disposition_counts=disposition_counts(records),
            archived_bytes=archived_bytes(records),
            artifact_size_bytes=published.size_bytes)
        return {"locator": published.locator, "created": True,
                "counts": disposition_counts(records)}

    def _build(self, plan_records, outcomes, chunk_index):
        records = []
        for plan_record in plan_records:
            ordinal = int(plan_record["plan_ordinal"])
            outcome = dict(outcomes.get(ordinal) or {})
            disposition = outcome.get("disposition")
            if disposition is None:
                raise ManifestSealError(
                    f"plan ordinal {ordinal} of chunk {chunk_index} has no "
                    "final disposition; terminal state is not publishable")

            # 'archived' is a claim about durable state, so it is derived here
            # rather than trusted from the caller: writer completion AND
            # catalog commit, or it is not archived.
            if disposition == "archived" and not (
                    outcome.get("writer_completed")
                    and outcome.get("catalog_committed")):
                raise ManifestSealError(
                    f"plan ordinal {ordinal} of chunk {chunk_index} claims "
                    "'archived' without durable writer and catalog completion")
            if outcome.get("writer_ambiguous") and disposition != "unresolved":
                raise ManifestSealError(
                    f"plan ordinal {ordinal} of chunk {chunk_index} has an "
                    "ambiguous writer outcome; it must be 'unresolved' so the "
                    "chunk blocks instead of being read off the tape")

            record = {
                "plan_ordinal": ordinal,
                "canonical_path": plan_record["canonical_path"],
                "session_label": self.session_label,
                "chunk_index": int(chunk_index),
                "storage_class": plan_record.get("storage_class"),
            }
            record.update(outcome)
            records.append(record)
        return records


def resolve_disposition(*, writer_state, catalog_state, source_error=None):
    """The single place a per-file outcome becomes a terminal disposition.

    Returns ``(disposition, writer_ambiguous)``. Keeping this one function means
    ``archived`` cannot be reached by two slightly different code paths with
    two slightly different ideas of "done".
    """
    if writer_state == "ambiguous" or catalog_state == "ambiguous":
        return "unresolved", True
    if source_error:
        return source_error, False
    if writer_state == "copied" and catalog_state == "committed":
        return "archived", False
    return "unresolved", False
