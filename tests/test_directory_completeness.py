"""Plan 3, Task 2.2 - deterministic directory status.

Two properties are worth more than the rest here:

* **precedence is total and ordered** - ambiguity beats provisional beats
  incomplete beats exception-qualified completion beats completion, and no
  later rule can rescue an earlier one;
* **a duplicate contribution never produces `complete`** - the plan forbids
  accepting ``archived_count >= expected_count`` as duplicate-safe proof, so
  there is an explicit test that inflating the archived side does not help.
"""
import unittest

from src.directory_status import (
    DirectoryEvidence,
    evidence_from_parts,
    resolve_directory_status,
)
from src.pipeline_types import DirectoryBackupStatus as S


def complete_evidence(**over):
    """Evidence for a directory that genuinely finished."""
    base = dict(
        scan_is_final=True,
        all_planned_items_terminal=True,
        all_required_items_archived=True,
        all_parts_written=True,
        all_writer_completions_succeeded=True,
        all_parts_cataloged=True,
        all_local_validation_succeeded=True,
        expected_count=10, archived_count=10)
    base.update(over)
    return DirectoryEvidence(**base)


def part(**over):
    base = dict(direct_expected_count=1, direct_archived_count=1,
                direct_unresolved_count=0,
                direct_source_missing_count=0,
                direct_source_permission_denied_count=0,
                direct_source_unreadable_count=0,
                direct_source_changed_count=0,
                writer_state="copied", catalog_state="committed",
                local_validation_state="succeeded")
    base.update(over)
    return base


class BaselineTests(unittest.TestCase):
    def test_a_finished_directory_is_complete(self):
        status, _reason = resolve_directory_status(complete_evidence())
        self.assertEqual(status, S.COMPLETE)


class PrecedenceTests(unittest.TestCase):
    """`ambiguous` overrides everything; the order is total."""

    def test_ambiguous_beats_an_otherwise_complete_directory(self):
        status, _ = resolve_directory_status(
            complete_evidence(has_ambiguous_evidence=True))
        self.assertEqual(status, S.AMBIGUOUS)

    def test_an_unresolved_entry_is_ambiguous_not_incomplete(self):
        status, _ = resolve_directory_status(
            complete_evidence(unresolved_count=1))
        self.assertEqual(status, S.AMBIGUOUS)

    def test_a_known_write_failure_is_incomplete(self):
        """`failed` is a KNOWN outcome, so it is work to redo, not doubt.

        Migration 018's `directory_completeness_derived_status_ck` places
        `all_writer_completions_succeeded` inside the incomplete AND-block, and
        this resolver mirrors that constraint exactly - if the two disagreed,
        persisting a computed status would raise a CheckViolation.
        """
        status, _ = resolve_directory_status(
            complete_evidence(all_writer_completions_succeeded=False))
        self.assertEqual(status, S.INCOMPLETE)

    def test_ambiguous_beats_provisional(self):
        status, _ = resolve_directory_status(
            complete_evidence(scan_is_final=False,
                              has_ambiguous_evidence=True))
        self.assertEqual(status, S.AMBIGUOUS)

    def test_provisional_beats_incomplete(self):
        status, _ = resolve_directory_status(
            complete_evidence(scan_is_final=False, all_parts_written=False,
                              parts_pending_write=2))
        self.assertEqual(status, S.PROVISIONAL)

    def test_provisional_beats_complete(self):
        status, _ = resolve_directory_status(
            complete_evidence(scan_is_final=False))
        self.assertEqual(status, S.PROVISIONAL)

    def test_incomplete_beats_exception_qualified_completion(self):
        status, _ = resolve_directory_status(
            complete_evidence(all_parts_cataloged=False,
                              parts_pending_catalog=1,
                              source_missing_count=1))
        self.assertEqual(status, S.INCOMPLETE)

    def test_source_exceptions_qualify_an_otherwise_complete_directory(self):
        status, _ = resolve_directory_status(
            complete_evidence(source_missing_count=2))
        self.assertEqual(status, S.COMPLETE_WITH_SOURCE_EXCEPTIONS)

    def test_each_explicit_exception_kind_qualifies(self):
        for field in ("source_missing_count",
                      "source_permission_denied_count",
                      "source_unreadable_count",
                      "source_changed_count"):
            with self.subTest(field=field):
                status, _ = resolve_directory_status(
                    complete_evidence(**{field: 1}))
                self.assertEqual(status, S.COMPLETE_WITH_SOURCE_EXCEPTIONS)

    def test_unresolved_never_qualifies_as_a_source_exception(self):
        status, _ = resolve_directory_status(
            complete_evidence(unresolved_count=1, source_missing_count=1))
        self.assertEqual(status, S.AMBIGUOUS)


class IndependentBooleanTests(unittest.TestCase):
    """Each boolean must be able to block completion on its own."""

    def test_each_boolean_blocks_completion_independently(self):
        for flag in ("all_planned_items_terminal",
                     "all_required_items_archived",
                     "all_parts_written",
                     "all_parts_cataloged",
                     "all_local_validation_succeeded"):
            with self.subTest(flag=flag):
                status, _ = resolve_directory_status(
                    complete_evidence(**{flag: False}))
                self.assertEqual(status, S.INCOMPLETE)

    def test_scan_finality_blocks_independently(self):
        status, _ = resolve_directory_status(
            complete_evidence(scan_is_final=False))
        self.assertEqual(status, S.PROVISIONAL)

    def test_writer_success_blocks_independently(self):
        status, _ = resolve_directory_status(
            complete_evidence(all_writer_completions_succeeded=False))
        self.assertEqual(status, S.INCOMPLETE)


class DuplicateOvercountTests(unittest.TestCase):
    """The plan's explicit prohibition, tested directly."""

    def test_inflated_archived_count_does_not_produce_complete(self):
        # A duplicate contribution reports MORE archived than expected while a
        # part is still unwritten. The count comparison would say "done".
        evidence = complete_evidence(
            expected_count=10, archived_count=14,
            all_parts_written=False, parts_pending_write=1)
        status, _ = resolve_directory_status(evidence)
        self.assertEqual(status, S.INCOMPLETE)

    def test_duplicate_parts_do_not_satisfy_an_unwritten_directory(self):
        parts = [part(), part(), part(direct_archived_count=5,
                                      writer_state="writing")]
        evidence = evidence_from_parts(parts, scan_is_final=True)
        status, _ = resolve_directory_status(evidence)
        self.assertNotEqual(status, S.COMPLETE)
        self.assertEqual(status, S.INCOMPLETE)


class MissingEvidenceTests(unittest.TestCase):
    def test_missing_evidence_degrades_and_never_reaches_complete(self):
        """Never `complete`; which of the lesser states depends on finality."""
        past_final = resolve_directory_status(
            complete_evidence(missing_evidence=True))[0]
        self.assertEqual(past_final, S.INCOMPLETE)
        still_scanning = resolve_directory_status(
            complete_evidence(missing_evidence=True, scan_is_final=False))[0]
        self.assertEqual(still_scanning, S.PROVISIONAL)
        self.assertNotIn(S.COMPLETE, (past_final, still_scanning))

    def test_missing_evidence_does_not_override_ambiguity(self):
        status, _ = resolve_directory_status(
            complete_evidence(missing_evidence=True,
                              has_ambiguous_evidence=True))
        self.assertEqual(status, S.AMBIGUOUS)


class TransitionTests(unittest.TestCase):
    def test_provisional_to_complete_when_the_scan_finalizes(self):
        provisional = complete_evidence(scan_is_final=False)
        self.assertEqual(resolve_directory_status(provisional)[0],
                         S.PROVISIONAL)
        finalized = complete_evidence(scan_is_final=True)
        self.assertEqual(resolve_directory_status(finalized)[0], S.COMPLETE)

    def test_catalog_pending_then_cataloged(self):
        pending = complete_evidence(all_parts_cataloged=False,
                                    parts_pending_catalog=1)
        self.assertEqual(resolve_directory_status(pending)[0], S.INCOMPLETE)
        self.assertEqual(resolve_directory_status(complete_evidence())[0],
                         S.COMPLETE)

    def test_a_known_writer_failure_in_parts_is_incomplete(self):
        """`failed` and `ambiguous` are different facts, and Plan 2 keeps them
        as separate writer states on purpose.

        A part whose write is KNOWN to have failed did not reach tape, so the
        directory is merely unfinished and the part can be rewritten. Only a
        result that MIGHT have reached tape is ambiguous - calling a known
        failure ambiguous would block completion forever on work that simply
        needs redoing.
        """
        parts = [part(), part(writer_state="failed")]
        evidence = evidence_from_parts(parts, scan_is_final=True)
        status, _ = resolve_directory_status(evidence)
        self.assertEqual(status, S.INCOMPLETE)

    def test_an_ambiguous_writer_outcome_is_ambiguous(self):
        parts = [part(), part(writer_state="ambiguous")]
        evidence = evidence_from_parts(parts, scan_is_final=True)
        status, _ = resolve_directory_status(evidence)
        self.assertEqual(status, S.AMBIGUOUS)

    def test_a_written_part_with_a_failed_completion_is_incomplete(self):
        """Mirrors the schema: a known failure is redoable work."""
        evidence = complete_evidence(all_writer_completions_succeeded=False)
        self.assertEqual(resolve_directory_status(evidence)[0], S.INCOMPLETE)


class EvidenceFoldingTests(unittest.TestCase):
    def test_all_succeeded_parts_fold_to_complete(self):
        evidence = evidence_from_parts([part(), part()], scan_is_final=True)
        self.assertEqual(resolve_directory_status(evidence)[0], S.COMPLETE)

    def test_an_ambiguous_part_state_folds_to_ambiguous(self):
        for field in ("writer_state", "catalog_state",
                      "local_validation_state"):
            with self.subTest(field=field):
                evidence = evidence_from_parts(
                    [part(), part(**{field: "ambiguous"})], scan_is_final=True)
                self.assertEqual(resolve_directory_status(evidence)[0],
                                 S.AMBIGUOUS)

    def test_an_unresolved_entry_in_a_part_folds_to_ambiguous(self):
        evidence = evidence_from_parts(
            [part(direct_unresolved_count=1)], scan_is_final=True)
        self.assertEqual(resolve_directory_status(evidence)[0], S.AMBIGUOUS)

    def test_source_exceptions_fold_and_qualify(self):
        evidence = evidence_from_parts(
            [part(direct_expected_count=2, direct_archived_count=1,
                  direct_source_missing_count=1)], scan_is_final=True)
        self.assertEqual(resolve_directory_status(evidence)[0],
                         S.COMPLETE_WITH_SOURCE_EXCEPTIONS)

    def test_an_empty_directory_with_a_final_scan_is_complete(self):
        evidence = evidence_from_parts([], scan_is_final=True)
        self.assertEqual(resolve_directory_status(evidence)[0], S.COMPLETE)

    def test_an_empty_directory_still_scanning_is_provisional(self):
        evidence = evidence_from_parts([], scan_is_final=False)
        self.assertEqual(resolve_directory_status(evidence)[0], S.PROVISIONAL)

    def test_pending_write_is_counted_per_part(self):
        evidence = evidence_from_parts(
            [part(), part(writer_state="writing"),
             part(writer_state="not_started")], scan_is_final=True)
        self.assertEqual(evidence.parts_pending_write, 2)
        self.assertEqual(resolve_directory_status(evidence)[0], S.INCOMPLETE)


class IdempotenceTests(unittest.TestCase):
    def test_resolution_is_a_pure_function(self):
        evidence = complete_evidence(source_missing_count=1)
        first = resolve_directory_status(evidence)
        second = resolve_directory_status(evidence)
        self.assertEqual(first, second)

    def test_folding_is_stable_under_part_reordering(self):
        parts = [part(), part(writer_state="writing"),
                 part(direct_source_missing_count=1)]
        a = evidence_from_parts(parts, scan_is_final=True)
        b = evidence_from_parts(list(reversed(parts)), scan_is_final=True)
        self.assertEqual(resolve_directory_status(a),
                         resolve_directory_status(b))


class StatusVocabularyTests(unittest.TestCase):
    def test_exactly_the_five_documented_states_exist(self):
        self.assertEqual(
            {s.value for s in S},
            {"ambiguous", "provisional", "incomplete",
             "complete_with_source_exceptions", "complete"})


if __name__ == "__main__":                   # pragma: no cover
    unittest.main()
