"""Plan 3, Task 3.1 - the planning-source transition proposal.

The single most important test here is
``test_the_boundary_follows_the_data_not_a_literal``: it runs the same code
against a differently-shaped session and asserts the boundary moves. A proposal
that happened to agree with today's production numbers because those numbers
were written into the source would pass every other test in this file.
"""
import unittest

from src.session_transition import (
    TransitionBlocked,
    build_transition_proposal,
    render_proposal,
)

# The verified production shape on 2026-08-04, used as a FIXTURE only.
# AGENTS.md is the written baseline, not live proof; these numbers are here to
# be reproduced by the code, never to be trusted by it.
PRODUCTION_SHAPE = {
    "session_id": 37,
    "session_label": "REMOTE_srv01_20260709_234150",
    "scan_complete": False,
    "tape_generation": 1,
    "chunk_count": 113,
    "max_chunk_index": 112,
    "chunks_contiguous": True,
    "chunk_states": {"done": 49, "pending": 64},
    "owned_chunks": 0,
    "duplicate_ordinals": 0,
    "sessions_sharing_plan": 1,
    "formats": {},
    "conflicting_formats": 0,
    "tape_generation_mismatch": False,
    "null_membership_state": 113,
    "source_scopes": [f"/vault/D/shared-sets/s{i}" for i in range(65)],
    "uncovered_scope_remains": True,
    "frontier_generation": 0,
    "bootstrap_state": "running",
}


class FakeDb:
    def __init__(self, audit):
        self._audit = audit

    def audit_session_transition_evidence(self, session_id):
        return self._audit


def audit(**over):
    data = dict(PRODUCTION_SHAPE)
    data.update(over)
    return data


def proposal(**over):
    return build_transition_proposal(FakeDb(audit(**over)), 37)


class BaselineTests(unittest.TestCase):
    def test_the_written_baseline_is_reproduced_from_evidence(self):
        result = proposal()
        self.assertEqual(result.last_legacy_planned_chunk, 112)
        self.assertEqual(result.first_manifest_first_chunk, 113)
        self.assertEqual(result.existing_chunk_count, 113)
        self.assertEqual(result.chunk_states, {"done": 49, "pending": 64})
        self.assertTrue(result.activatable)

    def test_the_boundary_follows_the_data_not_a_literal(self):
        """Change the session's shape; the boundary must move with it."""
        smaller = proposal(chunk_count=10, max_chunk_index=9,
                           chunk_states={"done": 4, "pending": 6})
        self.assertEqual(smaller.last_legacy_planned_chunk, 9)
        self.assertEqual(smaller.first_manifest_first_chunk, 10)

        larger = proposal(chunk_count=900, max_chunk_index=899,
                          chunk_states={"done": 900})
        self.assertEqual(larger.last_legacy_planned_chunk, 899)
        self.assertEqual(larger.first_manifest_first_chunk, 900)

    def test_no_session_or_chunk_literal_appears_in_the_module(self):
        import inspect

        from src import session_transition

        source = inspect.getsource(session_transition)
        for literal in ("37", "112", "113", "49", "Tape_02", "Tape_03"):
            self.assertNotIn(literal, source, literal)

    def test_a_missing_session_is_refused(self):
        with self.assertRaises(TransitionBlocked):
            build_transition_proposal(FakeDb(None), 999)


class BlockerTests(unittest.TestCase):
    def test_an_active_chunk_blocks(self):
        for state in ("fetching", "packing", "writing"):
            with self.subTest(state=state):
                result = proposal(chunk_states={"done": 5, state: 1})
                self.assertFalse(result.activatable)
                self.assertTrue(any(state in b for b in result.blockers))

    def test_a_backing_chunk_blocks_as_ambiguous(self):
        result = proposal(chunk_states={"done": 5, "backing": 1})
        self.assertFalse(result.activatable)
        self.assertTrue(any("unknowable" in b for b in result.blockers))

    def test_an_owned_chunk_blocks(self):
        self.assertFalse(proposal(owned_chunks=1).activatable)

    def test_a_duplicate_ordinal_blocks(self):
        self.assertFalse(proposal(duplicate_ordinals=3).activatable)

    def test_a_shared_plan_blocks(self):
        result = proposal(sessions_sharing_plan=2)
        self.assertFalse(result.activatable)
        self.assertTrue(any("shared" in b for b in result.blockers))

    def test_non_contiguous_chunks_block(self):
        self.assertFalse(proposal(chunks_contiguous=False).activatable)

    def test_an_incomplete_bootstrap_blocks(self):
        self.assertFalse(proposal(bootstrap_state="absent").activatable)
        self.assertFalse(proposal(bootstrap_state="failed").activatable)

    def test_a_format_conflict_blocks(self):
        self.assertFalse(proposal(conflicting_formats=2).activatable)

    def test_a_tape_generation_mismatch_blocks(self):
        self.assertFalse(
            proposal(tape_generation_mismatch=True).activatable)

    def test_several_blockers_are_all_reported(self):
        result = proposal(owned_chunks=1, duplicate_ordinals=1,
                          sessions_sharing_plan=3)
        self.assertGreaterEqual(len(result.blockers), 3)


class WarningTests(unittest.TestCase):
    def test_null_membership_state_warns_without_blocking(self):
        result = proposal()
        self.assertTrue(result.activatable)
        self.assertTrue(any("membership_state" in w for w in result.warnings))

    def test_a_final_scan_with_no_future_work_warns_against_fabrication(self):
        result = proposal(scan_complete=True, uncovered_scope_remains=False)
        self.assertTrue(any("fabricate" in w for w in result.warnings))
        self.assertTrue(result.activatable)

    def test_uncovered_scope_means_future_work_is_real(self):
        result = proposal()
        self.assertTrue(result.uncovered_scope_remains)
        self.assertEqual(len(result.source_scopes), 65)


class RenderingTests(unittest.TestCase):
    def test_the_report_states_no_tape_access(self):
        text = render_proposal(proposal())
        self.assertIn("No tape or LTFS access", text)

    def test_the_report_shows_the_derived_boundary(self):
        text = render_proposal(proposal())
        self.assertIn("last legacy chunk  : 112", text)
        self.assertIn("first manifest chunk: 113", text)

    def test_blockers_are_rendered_prominently(self):
        text = render_proposal(proposal(owned_chunks=1))
        self.assertIn("BLOCKERS", text)

    def test_a_clean_proposal_says_so(self):
        self.assertIn("no blockers", render_proposal(proposal()))

    def test_the_dict_form_is_machine_readable(self):
        data = proposal().as_dict()
        self.assertEqual(data["first_manifest_first_chunk"], 113)
        self.assertTrue(data["activatable"])
        self.assertIsInstance(data["blockers"], list)


if __name__ == "__main__":                   # pragma: no cover
    unittest.main()
