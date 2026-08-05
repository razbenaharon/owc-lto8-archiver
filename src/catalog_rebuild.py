"""Rebuild a catalog from local artifacts, and compare it to the original.

Plan 3, Tasks 4.2 and 4.3. The question both answer is the same one: *if the
PostgreSQL catalog were lost, could it be reconstructed from what is on local
disk?* Answering yes requires that the reconstruction never consults the
original database and never reads tape.

**No automatic tape access.** Rebuild and comparison read only artifacts under
the permanent metadata root. A locator that points at the LTFS mount is
classified and refused, not opened - the classification is lexical, so nothing
touches the media even to find out.

**The shadow database must be named explicitly and must be empty.** The
configured production database is refused by name *and* by identity, because a
rebuild writing into production would be indistinguishable from corruption.

**Only internal numeric ids are regenerated.** Everything that identifies
something in the real world - session label, chunk index, tape label and
generation, canonical path, stored path - is reproduced exactly, which is what
makes the comparison in :func:`compare_catalogs` meaningful.
"""
import os
from dataclasses import dataclass, field

from .archive_artifacts import (
    ArtifactError,
    PLAN_MANIFEST_NAMESPACE,
    SESSION_DESCRIPTOR_NAMESPACE,
    TERMINAL_STATE_NAMESPACE,
    is_ltfs_locator,
)
from .logsetup import get_logger
from .plan_manifest import is_authoritative, read_plan_manifest
from .scan_state_manifest import read_scan_state_segment, read_session_descriptor
from .terminal_manifest import read_terminal_manifest


class RebuildRefused(RuntimeError):
    """A rebuild that must not run against the named target."""


class RebuildIncomplete(RuntimeError):
    """Artifacts are missing, unready or contradictory."""


@dataclass
class DiscoveredArtifacts:
    """What was found under the metadata root, by stable identity."""

    session_descriptors: dict = field(default_factory=dict)
    scan_states: dict = field(default_factory=dict)
    plans: dict = field(default_factory=dict)
    terminals: dict = field(default_factory=dict)
    skipped_parts: list = field(default_factory=list)
    skipped_tape_locators: list = field(default_factory=list)

    def summary(self):
        return {
            "session_descriptors": len(self.session_descriptors),
            "scan_state_generations": sum(
                len(v) for v in self.scan_states.values()),
            "plan_manifests": len(self.plans),
            "terminal_manifests": len(self.terminals),
            "skipped_parts": len(self.skipped_parts),
            "skipped_tape_locators": len(self.skipped_tape_locators),
        }


def _namespace_root(archive_root, namespace):
    return os.path.join(os.path.abspath(str(archive_root)), namespace)


def discover_artifacts(archive_root, *, tape_root=None):
    """Find every ready artifact under the metadata root.

    ``.part`` files are evidence of an interrupted publication, never input:
    they are recorded as skipped so a reconciliation can see them, and are not
    parsed.
    """
    found = DiscoveredArtifacts()
    root = os.path.abspath(str(archive_root))

    for namespace, bucket in (
            (SESSION_DESCRIPTOR_NAMESPACE, "session_descriptors"),
            (PLAN_MANIFEST_NAMESPACE, "plans"),
            (TERMINAL_STATE_NAMESPACE, "terminals"),
            ("scan_states", "scan_states")):
        base = _namespace_root(root, namespace)
        if not os.path.isdir(base):
            continue
        for dirpath, _dirs, files in os.walk(base):
            for name in sorted(files):
                path = os.path.join(dirpath, name)
                locator = os.path.relpath(path, root).replace(os.sep, "/")
                if name.endswith(".part"):
                    found.skipped_parts.append(locator)
                    continue
                if is_ltfs_locator(path, tape_root=tape_root):
                    # Never opened. A tape locator is not local evidence, and
                    # the rebuild does not fall back to tape to obtain any.
                    found.skipped_tape_locators.append(locator)
                    continue
                if not name.endswith(".jsonl.zst"):
                    continue
                _index_artifact(found, bucket, locator, dirpath, root)
    return found


def _index_artifact(found, bucket, locator, dirpath, root):
    """Key an artifact by the stable identity encoded in its path."""
    parts = locator.split("/")
    label = parts[1][len("session_"):] if len(parts) > 1 else ""
    if bucket == "session_descriptors":
        found.session_descriptors.setdefault(label, []).append(locator)
    elif bucket == "scan_states":
        found.scan_states.setdefault(label, []).append(locator)
    else:
        chunk = None
        for element in parts:
            if element.startswith("chunk_"):
                chunk = int(element[len("chunk_"):])
        if chunk is None:
            return
        getattr(found, bucket)[(label, chunk)] = locator


class CatalogRebuilder:
    """Write a shadow catalog from local artifacts only."""

    def __init__(self, archive_root, shadow_db, *, shadow_name,
                 production_name=None, tape_root=None):
        self.archive_root = os.path.abspath(str(archive_root))
        self.db = shadow_db
        self.shadow_name = str(shadow_name)
        self.production_name = production_name
        self.tape_root = tape_root
        self._assert_target_is_safe()

    def _assert_target_is_safe(self):
        """Refuse production by name and by identity, before anything runs."""
        if not self.shadow_name.strip():
            raise RebuildRefused("a shadow database must be named explicitly")
        if self.production_name and (
                self.shadow_name.strip().casefold()
                == str(self.production_name).strip().casefold()):
            raise RebuildRefused(
                f"refusing to rebuild into the configured production database "
                f"{self.shadow_name!r}")
        actual = None
        reader = getattr(self.db, "current_database_name", None)
        if callable(reader):
            actual = reader()
        if actual is not None:
            if self.production_name and (
                    str(actual).casefold()
                    == str(self.production_name).strip().casefold()):
                raise RebuildRefused(
                    f"the connected database IS production ({actual!r}); "
                    "refusing to rebuild into it")
            if str(actual).casefold() != self.shadow_name.strip().casefold():
                raise RebuildRefused(
                    f"connected to {actual!r} but was told to rebuild "
                    f"{self.shadow_name!r}; refusing on identity mismatch")
        checker = getattr(self.db, "shadow_database_is_empty", None)
        if callable(checker) and not checker():
            raise RebuildRefused(
                f"shadow database {self.shadow_name!r} is not empty; a rebuild "
                "writes a fresh catalog and must not merge into existing rows")

    def rebuild(self, *, session_labels=None, dry_run=False):
        """Reconstruct the named sessions. Stops on missing evidence."""
        found = discover_artifacts(self.archive_root,
                                   tape_root=self.tape_root)
        labels = sorted(session_labels or found.session_descriptors)
        if not labels:
            raise RebuildIncomplete(
                "no session descriptor was found; an empty or partially "
                "planned session cannot be rebuilt without one")

        report = {"sessions": [], "artifacts": found.summary(),
                  "dry_run": bool(dry_run), "tape_reads": 0}
        for label in labels:
            report["sessions"].append(
                self._rebuild_session(label, found, dry_run=dry_run))
        get_logger().info(
            "catalog_rebuild_complete: shadow=%s sessions=%d dry_run=%s",
            self.shadow_name, len(labels), dry_run)
        return report

    def _rebuild_session(self, label, found, *, dry_run):
        descriptors = sorted(found.session_descriptors.get(label, []))
        if not descriptors:
            raise RebuildIncomplete(
                f"session {label!r} has no ready session descriptor")
        header, records, _trailer = read_session_descriptor(
            self.archive_root, descriptors[-1])

        named = [r["locator"] for r in records
                 if r.get("record_type") == "scan_state_generation"]
        missing = [loc for loc in named
                   if loc not in set(found.scan_states.get(label, []))]
        if missing:
            raise RebuildIncomplete(
                f"session {label!r} names {len(missing)} scan-state "
                "generation(s) that are not present; the chain is broken and "
                "the rebuild would silently lose that scan state")

        scan_states = []
        for locator in named:
            scan_states.append(read_scan_state_segment(self.archive_root,
                                                       locator))

        chunks = []
        for (chunk_label, chunk_index), locator in sorted(found.plans.items()):
            if chunk_label != label:
                continue
            plan_header, plan_records, plan_trailer = read_plan_manifest(
                self.archive_root, locator)
            terminal = found.terminals.get((label, chunk_index))
            terminal_records = []
            if terminal:
                _th, terminal_records, _tt = read_terminal_manifest(
                    self.archive_root, terminal)
            chunks.append({
                "chunk_index": chunk_index,
                "plan_locator": locator,
                "terminal_locator": terminal,
                "record_count": len(plan_records),
                "byte_total": int(plan_trailer.get("byte_total") or 0),
                "packaging_format": plan_header.get("packaging_format"),
                "provenance_kind": plan_header.get("provenance_kind"),
                "authoritative": is_authoritative(plan_header, plan_records),
                "dispositions": _disposition_counts(terminal_records),
                "plan_records": plan_records,
                "terminal_records": terminal_records,
            })

        result = {
            "session_label": label,
            "session_state": header.get("session_state"),
            "scan_complete": header.get("scan_complete"),
            "scan_state_generations": len(scan_states),
            "chunks": len(chunks),
            "coarse_chunks": sum(1 for c in chunks if not c["authoritative"]),
        }
        if not dry_run:
            writer = getattr(self.db, "write_rebuilt_session", None)
            if callable(writer):
                writer(label, header=header, scan_states=scan_states,
                       chunks=chunks)
        result["chunk_detail"] = [
            {k: v for k, v in c.items()
             if k not in ("plan_records", "terminal_records")}
            for c in chunks]
        result["_chunks"] = chunks
        return result


def _disposition_counts(records):
    counts = {}
    for record in records:
        counts[record["disposition"]] = counts.get(record["disposition"], 0) + 1
    return counts


# ---------------------------------------------------------------------------
# Task 4.3 - semantic comparison
# ---------------------------------------------------------------------------

#: Fields a rebuild legitimately regenerates. Comparing them would report
#: differences that mean nothing.
IGNORED_FIELDS = (
    "session_id", "chunk_id", "plan_file_id", "snapshot_file_id",
    "artifact_id", "container_id", "directory_id", "part_id", "bundle_id",
    "created_at", "updated_at", "calculated_at", "published_at",
    "owner_token", "lease_expires_at", "attempt_id",
)


def compare_catalogs(original, rebuilt, *, allow_coarse=False):
    """Compare two catalogs by stable logical identity.

    Both arguments are the canonical dictionaries produced by
    :func:`canonical_session_view`. Read-only; returns a machine-readable
    discrepancy report.

    Any unexplained difference fails the comparison. **Coarse routing also
    fails it**: the plan is explicit that coarse legacy evidence is reportable
    but never satisfies rebuild equivalence, because a chunk whose members
    cannot be routed to a container has not actually been proven
    reconstructable. ``allow_coarse=True`` records the exception explicitly -
    it is for a scope whose coarse chunks have been individually approved, and
    it still reports every one of them.

    A same-size content substitution is **outside** this hashless comparison and
    is reported as a residual risk on every run, not only when something else
    goes wrong.
    """
    discrepancies = []
    coarse = _coarse_chunks(original) | _coarse_chunks(rebuilt)
    for label, index in sorted(coarse):
        discrepancies.append({
            "kind": ("coarse_routing_accepted_by_exception" if allow_coarse
                     else "coarse_routing_is_not_rebuild_equivalent"),
            "session_label": label, "chunk_index": index,
            "detail": ("per-member container routing is unproven, so this "
                       "chunk cannot be declared reconstructable"),
            "approved_exception": bool(allow_coarse),
        })

    left_labels = set(original)
    right_labels = set(rebuilt)
    for label in sorted(left_labels - right_labels):
        discrepancies.append({"kind": "session_missing_in_rebuild",
                              "session_label": label})
    for label in sorted(right_labels - left_labels):
        discrepancies.append({"kind": "session_only_in_rebuild",
                              "session_label": label})

    for label in sorted(left_labels & right_labels):
        discrepancies.extend(_compare_session(label, original[label],
                                              rebuilt[label]))

    blocking = [d for d in discrepancies if not d.get("approved_exception")]
    return {
        "equivalent": not blocking,
        "discrepancies": discrepancies,
        "coarse_chunks": sorted(coarse),
        "compared_sessions": sorted(left_labels & right_labels),
        "residual_risk": (
            "No content hashes are compared. A same-size, same-structure "
            "replacement of a source file is undetectable by this comparison."),
        "tape_reads": 0,
    }


def _coarse_chunks(view):
    """Every (label, chunk) whose routing is not fully exact."""
    found = set()
    for label, session in (view or {}).items():
        for chunk in session.get("chunks", []):
            if any(e.get("routing_precision") == "coarse"
                   for e in chunk.get("entries", [])):
                found.add((label, int(chunk["chunk_index"])))
    return found


def _compare_session(label, left, right):
    out = []
    for field_name in ("scan_complete",):
        if left.get(field_name) != right.get(field_name):
            out.append({"kind": "session_field_differs", "session_label": label,
                        "field": field_name, "original": left.get(field_name),
                        "rebuilt": right.get(field_name)})

    left_chunks = {c["chunk_index"]: c for c in left.get("chunks", [])}
    right_chunks = {c["chunk_index"]: c for c in right.get("chunks", [])}
    for index in sorted(set(left_chunks) - set(right_chunks)):
        out.append({"kind": "chunk_missing_in_rebuild",
                    "session_label": label, "chunk_index": index})
    for index in sorted(set(right_chunks) - set(left_chunks)):
        out.append({"kind": "chunk_only_in_rebuild",
                    "session_label": label, "chunk_index": index})

    for index in sorted(set(left_chunks) & set(right_chunks)):
        out.extend(_compare_chunk(label, index, left_chunks[index],
                                  right_chunks[index]))
    return out


def _compare_chunk(label, index, left, right):
    out = []
    for field_name in ("packaging_format", "record_count", "byte_total",
                       "provenance_kind", "dispositions", "tape_label",
                       "tape_generation"):
        if field_name in left or field_name in right:
            if left.get(field_name) != right.get(field_name):
                out.append({
                    "kind": "chunk_field_differs", "session_label": label,
                    "chunk_index": index, "field": field_name,
                    "original": left.get(field_name),
                    "rebuilt": right.get(field_name)})

    left_entries = {int(e["plan_ordinal"]): e
                    for e in left.get("entries", [])}
    right_entries = {int(e["plan_ordinal"]): e
                     for e in right.get("entries", [])}
    for ordinal in sorted(set(left_entries) - set(right_entries)):
        out.append({"kind": "entry_missing_in_rebuild", "session_label": label,
                    "chunk_index": index, "plan_ordinal": ordinal,
                    "path": left_entries[ordinal].get("canonical_path")})
    for ordinal in sorted(set(right_entries) - set(left_entries)):
        out.append({"kind": "entry_only_in_rebuild", "session_label": label,
                    "chunk_index": index, "plan_ordinal": ordinal})
    for ordinal in sorted(set(left_entries) & set(right_entries)):
        a, b = left_entries[ordinal], right_entries[ordinal]
        # A path substitution at the same ordinal AND the same size is exactly
        # the case a hashless comparison can still catch, because the path is
        # part of the logical identity.
        for field_name in ("canonical_path", "expected_size",
                           "container_ordinal", "routing_precision"):
            if a.get(field_name) != b.get(field_name):
                out.append({
                    "kind": "entry_field_differs", "session_label": label,
                    "chunk_index": index, "plan_ordinal": ordinal,
                    "field": field_name, "original": a.get(field_name),
                    "rebuilt": b.get(field_name)})
    return out


def canonical_session_view(sessions):
    """Normalize a rebuild report (or a database read) for comparison."""
    view = {}
    for session in sessions:
        chunks = []
        for chunk in session.get("_chunks", session.get("chunks", [])):
            if not isinstance(chunk, dict):
                continue
            entries = [{
                "plan_ordinal": int(r["plan_ordinal"]),
                "canonical_path": r["canonical_path"],
                "expected_size": int(r["expected_size"]),
                "container_ordinal": r.get("container_ordinal"),
                "routing_precision": r.get("routing_precision"),
            } for r in chunk.get("plan_records", [])]
            chunks.append({
                "chunk_index": int(chunk["chunk_index"]),
                "packaging_format": chunk.get("packaging_format"),
                "provenance_kind": chunk.get("provenance_kind"),
                "record_count": int(chunk.get("record_count") or 0),
                "byte_total": int(chunk.get("byte_total") or 0),
                "dispositions": dict(chunk.get("dispositions") or {}),
                "entries": entries,
            })
        view[session["session_label"]] = {
            "scan_complete": session.get("scan_complete"),
            "chunks": sorted(chunks, key=lambda c: c["chunk_index"]),
        }
    return view
