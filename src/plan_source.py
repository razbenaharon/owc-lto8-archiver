"""One typed stream for a chunk's membership, whatever backs it.

Plan 3, Task 1.3. A chunk's membership now comes from one of two places:

``LegacyDbPlanSource``
    The ``remote_plan_files`` -> ``remote_snapshot_files`` join. Every chunk that
    already exists uses this, and it must keep behaving exactly as it did.

``ManifestPlanSource``
    A ready local ``plan-manifest-v1`` artifact. A manifest-first chunk owns no
    per-file rows at all, so the artifact *is* the membership.

The adapter is chosen **only** by persisted ``remote_chunks.plan_source``.
Nothing infers it from the presence of rows, the presence of a file, or the
shape of a chunk - inference is how two sources end up disagreeing about what a
chunk contains.

Two rules earn this module its existence:

* **Neither adapter decides packaging format.** Format lives on the chunk and
  the container (Plan 2). A plan source answers "what is in this chunk", never
  "how is it packed".
* **A manifest chunk with no readable artifact blocks.** It never falls back to
  the database, because a manifest-first chunk's rows are *absent by design* -
  an empty result would look like an empty chunk and silently archive nothing.

This is an iterator boundary, not a framework: two methods, one typed record.
"""
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Optional

from .archive_artifacts import ArtifactError
from .logsetup import get_logger

#: Persisted values of ``remote_chunks.plan_source``.
PLAN_SOURCE_LEGACY_DB = "legacy_db"
PLAN_SOURCE_MANIFEST = "manifest"


class PlanSourceError(RuntimeError):
    """A membership question that must not be answered with a guess."""


class PlanSourceUnavailable(PlanSourceError):
    """The evidence for this chunk is not readable; the chunk must block."""


@dataclass(frozen=True)
class ChunkRef:
    """Stable identity of one chunk, in both key spaces.

    ``session_id``/``chunk_index`` is the database key. ``session_label`` is the
    stable logical identity artifacts are organised by, and is what survives a
    rebuild that regenerates numeric ids.
    """

    session_id: int
    chunk_index: int
    session_label: Optional[str] = None
    plan_source: str = PLAN_SOURCE_LEGACY_DB
    plan_artifact_locator: Optional[str] = None

    def __post_init__(self):
        if self.plan_source not in (PLAN_SOURCE_LEGACY_DB,
                                    PLAN_SOURCE_MANIFEST):
            raise PlanSourceError(
                f"unknown plan_source {self.plan_source!r} for chunk "
                f"{self.chunk_index} of session {self.session_id}")


#: Legacy row keys, mapped onto the typed record's own field names. Staging code
#: has consumed these keys since before Plan 1; keeping them working is what
#: lets the boundary land without rewriting the 2,000-line stager.
_LEGACY_KEY_MAP = {
    "manifest_id": "plan_file_id",
    "remote_path": "source_path",
    "file_size_bytes": "size_bytes",
    "local_rel_path": "local_rel_path",
    "status": "status",
    "error_msg": "error_msg",
    "updated_at": "updated_at",
}


@dataclass(frozen=True)
class PlanEntry:
    """One planned member of a chunk.

    Mapping access is supported deliberately: existing consumers index rows by
    the legacy column names, and a typed record that cannot be read the old way
    would turn a boundary change into a rewrite.
    """

    plan_ordinal: int
    source_path: str
    size_bytes: int
    plan_file_id: Optional[int] = None
    local_rel_path: Optional[str] = None
    status: Optional[str] = None
    error_msg: Optional[str] = None
    updated_at: Any = None
    storage_class: Optional[str] = None
    container_format: Optional[str] = None
    container_ordinal: Optional[int] = None
    routing_precision: Optional[str] = None
    scan_segment_id: Optional[str] = None
    scan_ordinal: Optional[int] = None
    extra: dict = field(default_factory=dict, repr=False)

    # -- mapping compatibility -------------------------------------------
    def __getitem__(self, key):
        attribute = _LEGACY_KEY_MAP.get(key, key)
        try:
            return getattr(self, attribute)
        except AttributeError:
            if key in self.extra:
                return self.extra[key]
            raise KeyError(key) from None

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def keys(self):
        return tuple(_LEGACY_KEY_MAP)

    def as_legacy_row(self):
        return {key: self[key] for key in _LEGACY_KEY_MAP}


@dataclass(frozen=True)
class PlanSummary:
    """Aggregates a caller needs before staging, without materializing rows."""

    file_count: int
    planned_bytes: int
    present_bytes: int

    def as_tuple(self):
        """The ``(planned, present, count)`` shape the pipeline already uses."""
        return (self.planned_bytes, self.present_bytes, self.file_count)


class PlanSource:
    """Narrow read interface over one chunk's membership."""

    name = "plan-source"

    def iter_chunk_entries(self, chunk_ref):
        raise NotImplementedError

    def summary(self, chunk_ref):
        raise NotImplementedError

    # Shared by both adapters: duplicate membership is a defect to surface, not
    # something to silently de-duplicate. A chunk that lists a path twice would
    # fetch it twice and count it twice.
    @staticmethod
    def _reject_duplicates(entries, chunk_ref):
        seen_paths = set()
        seen_ordinals = set()
        for entry in entries:
            if entry.source_path in seen_paths:
                raise PlanSourceError(
                    f"chunk {chunk_ref.chunk_index} of session "
                    f"{chunk_ref.session_id} lists {entry.source_path!r} more "
                    "than once")
            if entry.plan_ordinal in seen_ordinals:
                raise PlanSourceError(
                    f"chunk {chunk_ref.chunk_index} of session "
                    f"{chunk_ref.session_id} reuses plan ordinal "
                    f"{entry.plan_ordinal}")
            seen_paths.add(entry.source_path)
            seen_ordinals.add(entry.plan_ordinal)
        return entries


class LegacyDbPlanSource(PlanSource):
    """Membership from ``remote_plan_files`` / ``remote_snapshot_files``.

    Preserves the existing query, its ``ORDER BY pf.ordinal`` and its exact row
    fields. ``PgSessionMixin.get_chunk_files()`` stays available and unchanged
    for every legacy DB-backed chunk (Plan 3 execution contract, item 2).
    """

    name = PLAN_SOURCE_LEGACY_DB

    def __init__(self, db):
        self.db = db

    def iter_chunk_entries(self, chunk_ref):
        rows = self.db.get_chunk_files(chunk_ref.session_id,
                                       chunk_ref.chunk_index)
        entries = [
            PlanEntry(
                plan_ordinal=ordinal,
                source_path=row["remote_path"],
                size_bytes=int(row["file_size_bytes"]),
                plan_file_id=row.get("manifest_id"),
                local_rel_path=row.get("local_rel_path"),
                status=row.get("status"),
                error_msg=row.get("error_msg"),
                updated_at=row.get("updated_at"),
            )
            for ordinal, row in enumerate(rows)
        ]
        return self._reject_duplicates(entries, chunk_ref)

    def summary(self, chunk_ref):
        totals = self.db.get_chunk_size_summary(
            chunk_ref.session_id, chunk_ref.chunk_index)
        planned, present, count = totals.get(chunk_ref.chunk_index, (0, 0, 0))
        return PlanSummary(file_count=int(count), planned_bytes=int(planned),
                           present_bytes=int(present))


class ManifestPlanSource(PlanSource):
    """Membership from a ready local ``plan-manifest-v1`` artifact.

    ``exception_states`` are the per-file outcomes the database still holds for
    a manifest chunk (there are only ever a handful: an entry becomes a row only
    when something went wrong). They are joined on ordinal so a resumed chunk
    does not re-fetch a file already known to be missing.
    """

    name = PLAN_SOURCE_MANIFEST

    def __init__(self, archive_root, *, exception_states=None):
        self.archive_root = archive_root
        self._exception_states = exception_states or (lambda ref: {})

    def _read(self, chunk_ref):
        from .plan_manifest import read_plan_manifest

        locator = chunk_ref.plan_artifact_locator
        if not locator:
            raise PlanSourceUnavailable(
                f"chunk {chunk_ref.chunk_index} of session "
                f"{chunk_ref.session_id} is manifest-planned but has no plan "
                "artifact locator; refusing to guess its membership from the "
                "database")
        try:
            return read_plan_manifest(self.archive_root, locator)
        except (ArtifactError, OSError) as exc:
            # Deliberately NOT a database fallback. A manifest chunk's per-file
            # rows do not exist, so the database would answer "empty" and the
            # chunk would archive nothing while looking successful.
            get_logger().error(
                "manifest_plan_unavailable: session=%s chunk=%s locator=%s "
                "error=%s", chunk_ref.session_id, chunk_ref.chunk_index,
                locator, exc)
            raise PlanSourceUnavailable(
                f"plan manifest {locator!r} for chunk {chunk_ref.chunk_index} "
                f"of session {chunk_ref.session_id} is unreadable: {exc}. The "
                "chunk blocks; it will not fall back to a database inventory."
            ) from exc

    def iter_chunk_entries(self, chunk_ref):
        header, records, _trailer = self._read(chunk_ref)
        if (chunk_ref.session_label
                and header.get("session_label") != chunk_ref.session_label):
            raise PlanSourceError(
                f"plan manifest names session {header.get('session_label')!r} "
                f"but was read for {chunk_ref.session_label!r}")
        if int(header.get("chunk_index", -1)) != int(chunk_ref.chunk_index):
            raise PlanSourceError(
                f"plan manifest names chunk {header.get('chunk_index')!r} but "
                f"was read for {chunk_ref.chunk_index}")

        states = self._exception_states(chunk_ref) or {}
        entries = [
            PlanEntry(
                plan_ordinal=int(record["plan_ordinal"]),
                source_path=record["canonical_path"],
                size_bytes=int(record["expected_size"]),
                plan_file_id=None,
                local_rel_path=record.get("local_rel_path"),
                status=states.get(int(record["plan_ordinal"])),
                storage_class=record.get("storage_class"),
                container_format=record.get("container_format"),
                container_ordinal=record.get("container_ordinal"),
                routing_precision=record.get("routing_precision"),
                scan_segment_id=record.get("scan_segment_id"),
                scan_ordinal=record.get("scan_ordinal"),
            )
            for record in records
        ]
        return self._reject_duplicates(entries, chunk_ref)

    def summary(self, chunk_ref):
        header, records, trailer = self._read(chunk_ref)
        planned = int(trailer.get("byte_total") or 0)
        present = sum(int(r["expected_size"]) for r in records
                      if (self._exception_states(chunk_ref) or {}).get(
                          int(r["plan_ordinal"])) != "source_missing")
        return PlanSummary(file_count=len(records), planned_bytes=planned,
                           present_bytes=present)


def build_plan_source(chunk_ref, *, db, archive_root=None,
                      exception_states=None):
    """Select the adapter from persisted chunk metadata, and nothing else."""
    if chunk_ref.plan_source == PLAN_SOURCE_MANIFEST:
        if not archive_root:
            raise PlanSourceUnavailable(
                "a manifest-planned chunk needs a local manifest archive root; "
                "configure [LOCAL_MANIFEST_ARCHIVE] root")
        return ManifestPlanSource(archive_root,
                                  exception_states=exception_states)
    return LegacyDbPlanSource(db)


def plan_source_for_chunk(db, session_id, chunk_index, *, archive_root=None,
                          exception_states=None):
    """Resolve ``(plan_source, chunk_ref)`` for one chunk from persisted state.

    Deliberately a module function rather than a method on the orchestrator: the
    pipeline coordinator, the orchestrator and the reconcilers all need the same
    answer, and a hook on the façade would force every caller (and every test
    double) to reimplement the selection.

    A database that predates migration 018 exposes no ``get_remote_chunk`` and
    no ``plan_source`` column. Every chunk there is ``legacy_db`` *by
    definition*, so defaulting to the legacy adapter is a statement of fact, not
    a fallback. What is never allowed is the other direction: a chunk persisted
    as ``manifest`` whose artifact cannot be read blocks (see
    :class:`ManifestPlanSource`).
    """
    row = _mapping_or_empty(_call(db, "get_remote_chunk", session_id,
                                  chunk_index))

    session_label = row.get("session_label")
    if session_label is None:
        session_label = _mapping_or_empty(
            _call(db, "get_remote_session", session_id)).get("session_label")

    # Only a value the schema could actually have persisted is honoured;
    # migration 018 constrains the column with a CHECK, so anything else means
    # "this database does not record a plan source", not "trust it anyway".
    persisted = row.get("plan_source")
    if persisted not in (PLAN_SOURCE_LEGACY_DB, PLAN_SOURCE_MANIFEST):
        persisted = PLAN_SOURCE_LEGACY_DB

    locator = row.get("plan_manifest_locator")
    if not isinstance(locator, str) or not locator.strip():
        locator = None

    chunk_ref = ChunkRef(
        session_id=int(session_id),
        chunk_index=int(chunk_index),
        session_label=session_label if isinstance(session_label, str) else None,
        plan_source=persisted,
        plan_artifact_locator=locator)
    source = build_plan_source(chunk_ref, db=db, archive_root=archive_root,
                               exception_states=exception_states)
    return source, chunk_ref


def _call(db, method, *args):
    reader = getattr(db, method, None)
    if not callable(reader):
        return None
    return reader(*args)


def _mapping_or_empty(value):
    """Read a row only when it really is a mapping.

    Production rows arrive through ``dict_row``, so they always are. A test
    double that returns something else carries no planning metadata, and
    inventing one from it would be worse than reading none.
    """
    return value if isinstance(value, Mapping) else {}


def chunk_ref_from_row(row, *, session_label=None):
    """Build a :class:`ChunkRef` from a persisted ``remote_chunks`` row.

    A row from a database without migration 018 has no ``plan_source`` column;
    such a chunk is legacy by definition, which is why the default is
    ``legacy_db`` rather than an error.
    """
    plan_source = (row.get("plan_source") if hasattr(row, "get")
                   else None) or PLAN_SOURCE_LEGACY_DB
    locator = (row.get("plan_manifest_locator") if hasattr(row, "get")
               else None)
    return ChunkRef(
        session_id=int(row["session_id"]),
        chunk_index=int(row["chunk_index"]),
        session_label=session_label or (row.get("session_label")
                                        if hasattr(row, "get") else None),
        plan_source=plan_source,
        plan_artifact_locator=locator)
