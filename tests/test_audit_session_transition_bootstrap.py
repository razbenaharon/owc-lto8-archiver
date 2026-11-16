"""Audit M4a - the transition proposal must speak the schema's vocabulary.

``remote_frontier_bootstraps.state`` is constrained by migration 014 to
``planned | running | completed | failed | abandoned``. The proposal used to
compare against ``'complete'``, a value the database can never hold, so a
*finished* bootstrap always produced a blocker. These tests drive the real
audit query against disposable PostgreSQL in both directions.
"""
import unittest

from db_backends import (create_test_db, each_backend, postgresql_only,
                         sqlite_only)
from pg_test_guard import SKIP_REASON
from test_directory_catalog_pg import _DirectoryCatalogFixture, _pg_available

from src.session_transition import (ACTIVATABLE_BOOTSTRAP_STATES,
                                    build_transition_proposal)


@each_backend
class BootstrapStateVocabularyTests(_DirectoryCatalogFixture,
                                    unittest.TestCase):
    def _set_bootstrap(self, state):
        self._exec("DELETE FROM remote_frontier_bootstraps WHERE session_id=%s",
                   (self.session_id,))
        if state is not None:
            self._exec(
                """INSERT INTO remote_frontier_bootstraps
                       (bootstrap_id, session_id, state)
                   VALUES (%s, %s, %s)""",
                (f"bs-{state}", self.session_id, state))

    def _bootstrap_blockers(self):
        proposal = build_transition_proposal(self.db, self.session_id)
        return [b for b in proposal.blockers if "bootstrap" in b]

    def test_a_completed_bootstrap_does_not_block(self):
        self._set_bootstrap("completed")
        self.assertEqual(self._bootstrap_blockers(), [])

    def test_the_durable_running_state_does_not_block(self):
        self._set_bootstrap("running")
        self.assertEqual(self._bootstrap_blockers(), [])

    def test_every_unfinished_or_missing_bootstrap_blocks(self):
        for state in ("planned", "failed", "abandoned", None):
            with self.subTest(state=state):
                self._set_bootstrap(state)
                self.assertEqual(len(self._bootstrap_blockers()), 1)

    def test_every_accepted_state_is_one_the_schema_can_hold(self):
        text = " ".join(
            definition for definition in self.tdb.check_constraints(
                "remote_frontier_bootstraps") if "state" in definition)
        for state in ACTIVATABLE_BOOTSTRAP_STATES:
            self.assertIn(f"'{state}'", text)


if __name__ == "__main__":
    unittest.main()
