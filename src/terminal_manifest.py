"""``terminal-state-v1`` - one final disposition per planned ordinal.

Plan 3, Task 1.5. A plan manifest says what a chunk was *supposed* to contain.
This artifact says what actually happened to each of those entries, exactly
once. It is published only when both conditions hold:

* every plan ordinal has one final disposition, and
* every writer and catalog state needed to justify ``archived`` is durable.

``archived`` is therefore impossible before the writer copied the container and
the catalog committed it. An ambiguous writer outcome - a copy that might have
reached tape - does not become ``archived`` and does not become a source
exception either: it stays ``unresolved``, which blocks final readiness and any
later pruning. That is the conservative direction on purpose, because the only
way to resolve it otherwise is to read the tape, which this repository forbids.

The terminal manifest is **local** durable evidence. It belongs in the database
backup procedure, not in a second tape write after the data write.
"""
from .archive_artifacts import (
    TERMINAL_STATE_NAMESPACE,
    TERMINAL_STATE_VERSION,
    ArtifactError,
    Plan3ArtifactWriter,
    plan3_artifact_locator,
    read_plan3_artifact,
)

#: Every disposition an ordinal can end in. ``unresolved`` is a real terminal
#: value for the artifact - the outcome is known to be unknown - but it is not
#: a *successful* one, and it blocks completion.
DISPOSITIONS = (
    "archived",
    "source_missing",
    "source_permission_denied",
    "source_unreadable",
    "source_changed",
    "unresolved",
)

#: Explicit source-side exceptions. A directory whose only non-archived entries
#: are these can still reach ``complete_with_source_exceptions``.
SOURCE_EXCEPTIONS = (
    "source_missing",
    "source_permission_denied",
    "source_unreadable",
    "source_changed",
)


def terminal_manifest_locator(session_label, chunk_index, *, generation=0):
    return plan3_artifact_locator(
        TERMINAL_STATE_NAMESPACE, session_label, chunk_index=chunk_index,
        kind="terminal", version=TERMINAL_STATE_VERSION, generation=generation)


#: What an ``archived`` loose file must carry so a rebuild can reconstruct its
#: ``files_index`` row without the source database and without touching tape.
_LOOSE_ARCHIVED_FIELDS = (
    "tape_label",
    "tape_generation_id",
    "tape_generation_value",
    "stored_path",
    "archive_date",
    "archive_run_identity",
    "source_host",
    "session_label",
    "chunk_index",
    "restore_locator",
)


def validate_terminal_record(record, header):
    ordinal = record.get("plan_ordinal")
    if ordinal is None or int(ordinal) < 0:
        raise ArtifactError(
            "terminal-state record: plan_ordinal must be a non-negative int")
    where = f"ordinal {int(ordinal)}"

    path = record.get("canonical_path")
    if not path or not str(path).strip():
        raise ArtifactError(f"terminal-state {where}: canonical_path required")

    for field in ("session_label", "chunk_index"):
        if record.get(field) != header.get(field):
            raise ArtifactError(
                f"terminal-state {where}: {field}={record.get(field)!r} "
                f"disagrees with the header {header.get(field)!r}")

    disposition = record.get("disposition")
    if disposition not in DISPOSITIONS:
        raise ArtifactError(
            f"terminal-state {where}: disposition must be one of "
            f"{DISPOSITIONS}, got {disposition!r}")

    if not str(record.get("final_evidence") or "").strip():
        raise ArtifactError(
            f"terminal-state {where}: final_evidence is required - a "
            "disposition without evidence is an assertion, not a record")

    if disposition == "archived":
        size = record.get("observed_archived_size")
        if size is None or int(size) < 0:
            raise ArtifactError(
                f"terminal-state {where}: an archived entry requires "
                "observed_archived_size")
        if not record.get("writer_completed"):
            raise ArtifactError(
                f"terminal-state {where}: 'archived' requires a durable writer "
                "completion")
        if not record.get("catalog_committed"):
            raise ArtifactError(
                f"terminal-state {where}: 'archived' requires a durable "
                "catalog commit")
        if record.get("storage_class") == "loose":
            missing = [f for f in _LOOSE_ARCHIVED_FIELDS
                       if record.get(f) in (None, "")]
            if missing:
                raise ArtifactError(
                    f"terminal-state {where}: an archived loose file must "
                    f"carry its exact restore locator; missing {missing}")
    else:
        # Nothing that did not reach tape may claim an archived size, and an
        # ambiguous writer result must never be recorded as a clean exception.
        if record.get("observed_archived_size") not in (None, 0):
            raise ArtifactError(
                f"terminal-state {where}: only an archived entry may report "
                "observed_archived_size")
        if disposition in SOURCE_EXCEPTIONS and record.get("writer_ambiguous"):
            raise ArtifactError(
                f"terminal-state {where}: an ambiguous writer outcome cannot "
                "be recorded as a source exception; it is 'unresolved'")
    return record


def _validate_terminal_artifact(header, records, trailer):
    ordinals = [int(r["plan_ordinal"]) for r in records]
    if len(ordinals) != len(set(ordinals)):
        raise ArtifactError(
            "terminal-state: an ordinal has more than one disposition")
    expected = header.get("expected_record_count")
    if expected is not None and int(expected) != len(records):
        raise ArtifactError(
            f"terminal-state: header expects {expected} ordinals but the "
            f"artifact carries {len(records)}")
    if records and ordinals != list(range(len(ordinals))):
        raise ArtifactError(
            "terminal-state: ordinals must be dense and contiguous from 0 so "
            "no planned entry can be silently absent")

    counts = disposition_counts(records)
    declared = header.get("disposition_counts")
    if declared and {k: int(v) for k, v in declared.items() if v} != {
            k: v for k, v in counts.items() if v}:
        raise ArtifactError(
            f"terminal-state: header disposition_counts {declared!r} disagree "
            f"with the records {counts!r}")


def disposition_counts(records):
    counts = {name: 0 for name in DISPOSITIONS}
    for record in records:
        counts[record["disposition"]] += 1
    return counts


def archived_bytes(records):
    return sum(int(r.get("observed_archived_size") or 0)
               for r in records if r.get("disposition") == "archived")


def is_publishable(plan_ordinals, records):
    """Every planned ordinal has exactly one disposition.

    Returns ``(ok, reason)``. The caller must not publish on a false, because a
    terminal manifest missing an ordinal would let a directory look complete.
    """
    planned = set(int(o) for o in plan_ordinals)
    seen = [int(r["plan_ordinal"]) for r in records]
    if len(seen) != len(set(seen)):
        return False, "an ordinal has more than one disposition"
    missing = planned - set(seen)
    if missing:
        return False, f"{len(missing)} planned ordinals have no disposition"
    extra = set(seen) - planned
    if extra:
        return False, f"{len(extra)} dispositions have no planned ordinal"
    return True, ""


def write_terminal_manifest(archive_root, *, session_label, chunk_index,
                            records, plan_ordinals=None, generation=0,
                            plan_artifact_locator=None, packaging_format=None,
                            extra_header=None):
    """Publish the terminal state for one chunk.

    ``plan_ordinals`` is the membership taken from the plan manifest. Passing it
    is how the exactly-one-disposition-per-ordinal rule is enforced before a
    single byte is written.
    """
    records = list(records)
    if plan_ordinals is not None:
        ok, reason = is_publishable(plan_ordinals, records)
        if not ok:
            raise ArtifactError(
                f"refusing to publish terminal state for chunk {chunk_index}: "
                f"{reason}")
    records.sort(key=lambda r: int(r["plan_ordinal"]))

    header = {
        "session_label": str(session_label),
        "chunk_index": int(chunk_index),
        "generation": int(generation),
        "plan_artifact_locator": plan_artifact_locator,
        "packaging_format": packaging_format,
        "expected_record_count": len(records),
        "disposition_counts": disposition_counts(records),
        "archived_bytes": archived_bytes(records),
    }
    if extra_header:
        header.update(extra_header)

    locator = terminal_manifest_locator(session_label, chunk_index,
                                        generation=generation)
    writer = Plan3ArtifactWriter(
        archive_root, locator, version=TERMINAL_STATE_VERSION, header=header,
        validate_record=validate_terminal_record,
        validate_artifact=_validate_terminal_artifact,
        size_field="observed_archived_size", ordinal_field="plan_ordinal",
        path_field="canonical_path")
    with writer:
        for record in records:
            writer.add(record)
    return writer.publish()


def read_terminal_manifest(archive_root, locator):
    return read_plan3_artifact(
        archive_root, locator, expected_version=TERMINAL_STATE_VERSION,
        validate_artifact=_terminal_reader_validation)


def _terminal_reader_validation(header, records, trailer):
    for record in records:
        validate_terminal_record(record, header)
    _validate_terminal_artifact(header, records, trailer)


def join_to_plan(plan_records, terminal_records):
    """Join terminal outcomes onto plan entries by ordinal.

    Raises when the join is not exact: a missing or duplicated ordinal means the
    pair cannot prove what happened to the chunk.
    """
    plan_by_ordinal = {int(r["plan_ordinal"]): r for r in plan_records}
    terminal_by_ordinal = {}
    for record in terminal_records:
        ordinal = int(record["plan_ordinal"])
        if ordinal in terminal_by_ordinal:
            raise ArtifactError(
                f"terminal ordinal {ordinal} appears more than once")
        terminal_by_ordinal[ordinal] = record

    missing = sorted(set(plan_by_ordinal) - set(terminal_by_ordinal))
    extra = sorted(set(terminal_by_ordinal) - set(plan_by_ordinal))
    if missing or extra:
        raise ArtifactError(
            f"plan/terminal ordinals do not match: {len(missing)} missing, "
            f"{len(extra)} unexpected")

    joined = []
    for ordinal, plan_record in sorted(plan_by_ordinal.items()):
        terminal_record = terminal_by_ordinal[ordinal]
        if terminal_record["canonical_path"] != plan_record["canonical_path"]:
            raise ArtifactError(
                f"ordinal {ordinal} names {terminal_record['canonical_path']!r} "
                f"in the terminal manifest but "
                f"{plan_record['canonical_path']!r} in the plan")
        joined.append((plan_record, terminal_record))
    return joined
