"""The deterministic directory status rules.

Plan 3, Task 2.2. Kept as a pure function over an explicit evidence record so
the five states can be exercised exhaustively without a database, and so there
is exactly one place the precedence lives.

The rule the plan is most insistent about, and the one this module exists to
enforce: **never collapse the evidence into one count comparison.**
``archived_count >= expected_count`` is not proof — a duplicate contribution
inflates ``archived_count`` past ``expected_count`` while a file is still
missing. So completeness is decided by seven independent booleans, and the
counts are used only to answer "is anything still outstanding", never "is
everything done".
"""
from dataclasses import dataclass

from .pipeline_types import DirectoryBackupStatus

#: Dispositions that are explicit, understood source-side outcomes. A directory
#: whose only non-archived entries are these can still complete, with the
#: exception qualifier. ``unresolved`` is deliberately NOT here.
SOURCE_EXCEPTION_FIELDS = (
    "source_missing_count",
    "source_permission_denied_count",
    "source_unreadable_count",
    "source_changed_count",
)


@dataclass(frozen=True)
class DirectoryEvidence:
    """Everything the status decision is allowed to look at.

    Every field is stated explicitly rather than derived, because the whole
    point is that these questions are answered independently.
    """

    # -- source coverage --------------------------------------------------
    scan_is_final: bool = False

    # -- the seven independent booleans -----------------------------------
    all_planned_items_terminal: bool = False
    all_required_items_archived: bool = False
    all_parts_written: bool = False
    all_writer_completions_succeeded: bool = False
    all_parts_cataloged: bool = False
    all_local_validation_succeeded: bool = False

    # -- ambiguity --------------------------------------------------------
    has_ambiguous_evidence: bool = False
    unresolved_count: int = 0

    # -- explicit source exceptions ---------------------------------------
    source_missing_count: int = 0
    source_permission_denied_count: int = 0
    source_unreadable_count: int = 0
    source_changed_count: int = 0

    # -- outstanding work -------------------------------------------------
    expected_count: int = 0
    archived_count: int = 0
    parts_pending_write: int = 0
    parts_pending_catalog: int = 0
    parts_pending_validation: int = 0

    # -- evidence completeness --------------------------------------------
    missing_evidence: bool = False

    @property
    def source_exception_count(self):
        return sum(getattr(self, name) for name in SOURCE_EXCEPTION_FIELDS)

    @property
    def has_outstanding_work(self):
        """Known work that has not finished, counted without trusting totals."""
        return bool(
            self.parts_pending_write
            or self.parts_pending_catalog
            or self.parts_pending_validation
            or not self.all_planned_items_terminal
            or not self.all_required_items_archived
            or not self.all_parts_written
            or not self.all_writer_completions_succeeded
            or not self.all_parts_cataloged
            or not self.all_local_validation_succeeded)


def resolve_directory_status(evidence):
    """Return ``(status, reason)`` from the plan's precedence, in order.

    ``ambiguous`` overrides everything -> otherwise non-final source coverage is
    ``provisional`` -> otherwise known outstanding work is ``incomplete`` ->
    otherwise explicit source exceptions select
    ``complete_with_source_exceptions`` -> otherwise ``complete``.
    This function deliberately mirrors migration 018's
    ``directory_completeness_derived_status_ck`` **exactly**. The schema is the
    authority; if these two ever disagree, persisting a computed status raises a
    CheckViolation at runtime instead of quietly storing a different answer.
    Keep them in step.
    """
    # 1. Ambiguity wins, always. An unresolved disposition or conflicting
    #    evidence may not be resolved by a more optimistic later rule, because
    #    the only way to actually settle it is reading the tape.
    if evidence.has_ambiguous_evidence:
        return DirectoryBackupStatus.AMBIGUOUS, "conflicting or ambiguous evidence"
    if evidence.unresolved_count:
        return (DirectoryBackupStatus.AMBIGUOUS,
                f"{evidence.unresolved_count} entries are unresolved")

    # 2. A directory that is still being discovered cannot be called finished,
    #    however complete the entries found so far happen to look. Missing or
    #    errored scan evidence lands here too: it is recorded by leaving
    #    `scan_is_final` false, so an unproven directory can never pass beyond
    #    this point toward `complete`.
    if not evidence.scan_is_final:
        return (DirectoryBackupStatus.PROVISIONAL,
                "source coverage is not final"
                + (" (evidence is missing or errored)"
                   if evidence.missing_evidence else ""))

    # 3. Missing evidence past a final scan is outstanding work, never success.
    if evidence.missing_evidence:
        return (DirectoryBackupStatus.INCOMPLETE,
                "required artifact or contribution evidence is missing")

    # 4. Known outstanding work. `all_writer_completions_succeeded` takes part
    #    here, exactly as the schema's AND block does - a KNOWN write failure is
    #    work to redo, not uncertainty about what reached the tape. Genuine
    #    uncertainty arrives as `has_ambiguous_evidence` and was handled above.
    if evidence.has_outstanding_work:
        return DirectoryBackupStatus.INCOMPLETE, _outstanding_reason(evidence)

    # 5. Explicit, understood source-side exceptions.
    if evidence.source_exception_count:
        return (DirectoryBackupStatus.COMPLETE_WITH_SOURCE_EXCEPTIONS,
                f"{evidence.source_exception_count} explicit source exceptions")

    return DirectoryBackupStatus.COMPLETE, "every planned entry archived"


def _outstanding_reason(evidence):
    reasons = []
    if not evidence.all_planned_items_terminal:
        reasons.append("planned entries are not all terminal")
    if not evidence.all_required_items_archived:
        reasons.append("required entries are not all archived")
    if not evidence.all_parts_written or evidence.parts_pending_write:
        reasons.append(f"{evidence.parts_pending_write} parts await writing")
    if not evidence.all_parts_cataloged or evidence.parts_pending_catalog:
        reasons.append(f"{evidence.parts_pending_catalog} parts await catalog")
    if (not evidence.all_local_validation_succeeded
            or evidence.parts_pending_validation):
        reasons.append(
            f"{evidence.parts_pending_validation} parts await local validation")
    return "; ".join(reasons) or "outstanding work remains"


def evidence_from_parts(parts, *, scan_is_final, missing_evidence=False):
    """Fold a directory's archive-part rows into one evidence record.

    Deliberately does NOT compare ``archived_count`` with ``expected_count`` to
    decide completeness. Duplicate contributions inflate the archived side, so
    that comparison can report "done" for a directory that is missing a file.
    """
    parts = list(parts)
    totals = {name: 0 for name in (
        "expected_count", "archived_count", "unresolved_count",
        "source_missing_count", "source_permission_denied_count",
        "source_unreadable_count", "source_changed_count")}
    pending_write = pending_catalog = pending_validation = 0
    ambiguous = False
    writer_all_ok = True
    all_written = True
    all_cataloged = True
    all_validated = True

    for part in parts:
        totals["expected_count"] += int(part.get("direct_expected_count") or 0)
        totals["archived_count"] += int(part.get("direct_archived_count") or 0)
        totals["unresolved_count"] += int(
            part.get("direct_unresolved_count") or 0)
        for name in SOURCE_EXCEPTION_FIELDS:
            totals[name] += int(part.get(f"direct_{name}") or 0)

        writer_state = part.get("writer_state") or "not_started"
        catalog_state = part.get("catalog_state") or "not_started"
        validation = part.get("local_validation_state") or "not_validated"

        if writer_state == "ambiguous" or catalog_state == "ambiguous" or (
                validation == "ambiguous"):
            ambiguous = True
        if writer_state != "copied":
            all_written = False
            pending_write += 1
        if writer_state == "failed":
            writer_all_ok = False
        if catalog_state != "committed":
            all_cataloged = False
            pending_catalog += 1
        if validation != "succeeded":
            all_validated = False
            pending_validation += 1

    # "Terminal" means every planned entry reached SOME final outcome, not
    # merely that none is unresolved: an entry that is neither archived nor an
    # exception has no outcome at all. Migration 018 enforces exactly this
    # partition (directory_completeness_terminal_partition_ck), so computing it
    # any other way would raise a CheckViolation on persist.
    exceptions = sum(totals[name] for name in SOURCE_EXCEPTION_FIELDS)
    accounted = (totals["archived_count"] + exceptions
                 + totals["unresolved_count"])
    all_terminal = accounted == totals["expected_count"]

    # And "archived" additionally requires nothing left unresolved, mirroring
    # directory_completeness_required_archive_ck.
    all_archived = (totals["unresolved_count"] == 0
                    and totals["archived_count"] + exceptions
                    == totals["expected_count"])

    return DirectoryEvidence(
        scan_is_final=bool(scan_is_final),
        all_planned_items_terminal=all_terminal,
        all_required_items_archived=all_archived,
        all_parts_written=all_written,
        all_writer_completions_succeeded=writer_all_ok,
        all_parts_cataloged=all_cataloged,
        all_local_validation_succeeded=all_validated,
        has_ambiguous_evidence=ambiguous,
        unresolved_count=totals["unresolved_count"],
        source_missing_count=totals["source_missing_count"],
        source_permission_denied_count=totals[
            "source_permission_denied_count"],
        source_unreadable_count=totals["source_unreadable_count"],
        source_changed_count=totals["source_changed_count"],
        expected_count=totals["expected_count"],
        archived_count=totals["archived_count"],
        parts_pending_write=pending_write,
        parts_pending_catalog=pending_catalog,
        parts_pending_validation=pending_validation,
        missing_evidence=bool(missing_evidence))
