"""One chunk-status vocabulary, proven equal to the literals it replaced.

Plan 1 review, item 6 ("repeated state-transition rules"). The chunk status
strings were spelled out independently in four places — ``session_reconcile``,
``startup_reconcile``, ``pg_sessions.RECLAIMABLE_CHUNK_STATES`` and the database
CHECK constraint — all of which have to agree with
``src.pipeline_types.ChunkStatus``. Nothing made them agree except care, and a
future status change had four places to remember.

They now derive from ``ChunkStatus``. This file exists so that change is
**proven safe rather than assumed**: every derived constant is asserted equal to
the exact literal tuple it replaced, so the refactor cannot have altered a
single value. The database CHECK constraint is compared too, because a
vocabulary that agrees with itself but not with PostgreSQL is no better.
"""
import os
import re
import unittest

from src.pipeline_types import CHUNK_TRANSITIONS, ChunkStatus

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

#: The exact values these constants held before they were derived. Written out
#: deliberately: this is the frozen baseline, not something computed from the
#: same source it is meant to check.
BASELINE = {
    "session_reconcile.TRANSIENT_CHUNK_STATES":
        ("fetching", "packing", "backing"),
    "session_reconcile.FAILED_CHUNK_STATES":
        ("fetch_failed", "backup_failed"),
    "session_reconcile.DONE_CHUNK_STATE": "done",
    "session_reconcile.PENDING_CHUNK_STATE": "pending",
    "startup_reconcile.AMBIGUOUS_CHUNK_STATE": "backing",
    "startup_reconcile.TRANSIENT_CHUNK_STATES":
        ("fetching", "packing", "backing"),
    "pg_sessions.RECLAIMABLE_CHUNK_STATES": ("fetching", "packing"),
}


class DerivedConstantsMatchTheBaselineTests(unittest.TestCase):
    def test_session_reconcile_constants_are_unchanged(self):
        from src import session_reconcile as sr
        self.assertEqual(sr.TRANSIENT_CHUNK_STATES,
                         BASELINE["session_reconcile.TRANSIENT_CHUNK_STATES"])
        self.assertEqual(sr.FAILED_CHUNK_STATES,
                         BASELINE["session_reconcile.FAILED_CHUNK_STATES"])
        self.assertEqual(sr.DONE_CHUNK_STATE,
                         BASELINE["session_reconcile.DONE_CHUNK_STATE"])
        self.assertEqual(sr.PENDING_CHUNK_STATE,
                         BASELINE["session_reconcile.PENDING_CHUNK_STATE"])

    def test_startup_reconcile_constants_are_unchanged(self):
        from src import startup_reconcile as st
        self.assertEqual(st.AMBIGUOUS_CHUNK_STATE,
                         BASELINE["startup_reconcile.AMBIGUOUS_CHUNK_STATE"])
        self.assertEqual(st.TRANSIENT_CHUNK_STATES,
                         BASELINE["startup_reconcile.TRANSIENT_CHUNK_STATES"])

    def test_the_reclaimable_states_are_unchanged(self):
        from src.pg_sessions import PgSessionMixin
        self.assertEqual(PgSessionMixin.RECLAIMABLE_CHUNK_STATES,
                         BASELINE["pg_sessions.RECLAIMABLE_CHUNK_STATES"])


class VocabulariesAgreeTests(unittest.TestCase):
    def test_every_constant_names_a_declared_status(self):
        from src import session_reconcile as sr
        from src import startup_reconcile as st
        from src.pg_sessions import PgSessionMixin

        declared = {status.value for status in ChunkStatus}
        used = set(sr.TRANSIENT_CHUNK_STATES) | set(sr.FAILED_CHUNK_STATES)
        used |= {sr.DONE_CHUNK_STATE, sr.PENDING_CHUNK_STATE}
        used |= set(st.TRANSIENT_CHUNK_STATES)
        used |= {st.AMBIGUOUS_CHUNK_STATE}
        used |= set(PgSessionMixin.RECLAIMABLE_CHUNK_STATES)
        self.assertEqual(used - declared, set(),
                         "a constant names a status ChunkStatus does not")

    def test_the_ambiguous_state_is_excluded_from_reclamation(self):
        from src import startup_reconcile as st
        from src.pg_sessions import PgSessionMixin
        self.assertNotIn(st.AMBIGUOUS_CHUNK_STATE,
                         PgSessionMixin.RECLAIMABLE_CHUNK_STATES)

    def test_the_reclaimable_states_are_exactly_the_pre_write_transients(self):
        """Reclaimable == transient MINUS the ambiguous one. If a new transient
        state is ever added, this is where the omission shows up."""
        from src import startup_reconcile as st
        from src.pg_sessions import PgSessionMixin
        expected = tuple(s for s in st.TRANSIENT_CHUNK_STATES
                         if s != st.AMBIGUOUS_CHUNK_STATE)
        self.assertEqual(PgSessionMixin.RECLAIMABLE_CHUNK_STATES, expected)

    def test_the_database_check_constraint_agrees_with_chunkstatus(self):
        with open(os.path.join(PROJECT_ROOT, "scripts", "sql",
                               "001_postgres_schema.sql"),
                  encoding="utf-8") as handle:
            schema = handle.read()
        match = re.search(
            r"remote_chunks.*?CHECK \(status IN \((.*?)\)\)", schema, re.S)
        self.assertIsNotNone(match)
        in_database = set(re.findall(r"'([a-z_]+)'", match.group(1)))
        self.assertEqual(in_database, {s.value for s in ChunkStatus})

    def test_every_declared_status_has_a_transition_rule(self):
        self.assertEqual(set(CHUNK_TRANSITIONS), set(ChunkStatus))


if __name__ == "__main__":
    unittest.main()
