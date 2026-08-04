"""Plan 1 / Task 1.5 — typed lifecycle states and the single transition owner.

Chunk status used to be written from half a dozen call sites with bare strings,
so "which transitions are legal" existed only as a mental model. The one
transition that must never happen automatically — an ambiguous ``backing`` chunk
being retried, when its bytes may already be on tape — was protected by
convention.

This module pins the matrix and the single owner. The table-driven cases are
deliberately exhaustive over the declared statuses: a new status that nobody
wires into ``CHUNK_TRANSITIONS`` shows up here as an unreachable state rather
than as a silent runtime refusal.
"""
import inspect
import unittest
from itertools import product
from types import SimpleNamespace
from unittest import mock

from src.pipeline_types import (CHUNK_TRANSITIONS, ChunkStatus,
                                FileTransferStatus, ForbiddenTransition,
                                LEGACY_FILE_STATUSES, MembershipState,
                                SessionStatus, allowed_file_statuses,
                                assert_chunk_transition,
                                is_allowed_chunk_transition)


# =============================================================================
# A. The enums
# =============================================================================
class EnumVocabularyTests(unittest.TestCase):
    def test_chunk_statuses_match_the_database_check_constraint(self):
        import os
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with open(os.path.join(root, "scripts", "sql",
                               "001_postgres_schema.sql"), encoding="utf-8") as h:
            schema = h.read()
        for status in ChunkStatus:
            self.assertIn(f"'{status.value}'", schema, status.value)

    def test_session_statuses_match_the_database_check_constraint(self):
        self.assertEqual({s.value for s in SessionStatus},
                         {"active", "completed", "abandoned"})

    def test_enums_are_string_backed(self):
        for enum in (SessionStatus, ChunkStatus, FileTransferStatus,
                     MembershipState):
            for member in enum:
                self.assertIsInstance(member.value, str)
                self.assertEqual(str(member.value), member.value)

    def test_legacy_databases_cannot_use_the_richer_file_outcomes(self):
        legacy = allowed_file_statuses(migration_014_applied=False)
        self.assertEqual(legacy, LEGACY_FILE_STATUSES)
        for richer in (FileTransferStatus.SOURCE_CHANGED,
                       FileTransferStatus.SOURCE_UNREADABLE,
                       FileTransferStatus.SOURCE_PERMISSION_DENIED,
                       FileTransferStatus.UNRESOLVED):
            self.assertNotIn(richer, legacy)
        self.assertEqual(allowed_file_statuses(migration_014_applied=True),
                         frozenset(FileTransferStatus))

    def test_membership_seal_is_a_declared_state(self):
        self.assertEqual({s.value for s in MembershipState},
                         {"building", "sealed"})


# =============================================================================
# B. The transition matrix
# =============================================================================
class TransitionMatrixTests(unittest.TestCase):
    def test_every_status_has_a_declared_successor_set(self):
        self.assertEqual(set(CHUNK_TRANSITIONS), set(ChunkStatus))

    def test_no_automatic_retry_out_of_backing(self):
        """THE invariant. A 'backing' chunk may already be on tape."""
        for target in (ChunkStatus.PENDING, ChunkStatus.FETCHING,
                       ChunkStatus.PACKING, ChunkStatus.FETCH_FAILED,
                       ChunkStatus.BACKUP_FAILED):
            self.assertFalse(
                is_allowed_chunk_transition(ChunkStatus.BACKING, target),
                f"backing -> {target.value} would risk a double write")

    def test_backing_may_only_move_forward_to_done(self):
        self.assertEqual(CHUNK_TRANSITIONS[ChunkStatus.BACKING],
                         frozenset({ChunkStatus.DONE}))

    def test_done_is_terminal(self):
        self.assertEqual(CHUNK_TRANSITIONS[ChunkStatus.DONE], frozenset())

    def test_the_normal_staging_path_is_allowed(self):
        for source, target in ((ChunkStatus.PENDING, ChunkStatus.FETCHING),
                               (ChunkStatus.FETCHING, ChunkStatus.PACKING),
                               (ChunkStatus.PACKING, ChunkStatus.BACKING),
                               (ChunkStatus.BACKING, ChunkStatus.DONE)):
            self.assertTrue(is_allowed_chunk_transition(source, target),
                            f"{source.value} -> {target.value}")

    def test_a_failed_chunk_can_be_re_driven(self):
        for source in (ChunkStatus.FETCH_FAILED, ChunkStatus.BACKUP_FAILED):
            self.assertTrue(
                is_allowed_chunk_transition(source, ChunkStatus.PENDING))

    def test_unknown_statuses_are_refused_on_either_side(self):
        for source, target in (("nonsense", ChunkStatus.DONE),
                               (ChunkStatus.PENDING, "nonsense"),
                               (None, None), ("", "")):
            self.assertFalse(is_allowed_chunk_transition(source, target))

    def test_the_full_matrix_is_exhaustively_table_driven(self):
        for source, target in product(ChunkStatus, ChunkStatus):
            expected = target in CHUNK_TRANSITIONS[source]
            self.assertEqual(is_allowed_chunk_transition(source, target),
                             expected, f"{source.value} -> {target.value}")

    def test_a_forbidden_transition_raises_with_the_reason(self):
        with self.assertRaises(ForbiddenTransition) as caught:
            assert_chunk_transition(ChunkStatus.BACKING, ChunkStatus.PENDING)
        message = str(caught.exception)
        self.assertIn("old state is preserved", message)
        self.assertIn("may already be on tape", message)

    def test_an_allowed_transition_does_not_raise(self):
        self.assertIsNone(
            assert_chunk_transition(ChunkStatus.PACKING, ChunkStatus.BACKING))


# =============================================================================
# C. One repository owner
# =============================================================================
class _FakeCursor:
    def __init__(self, rowcount=1, row=None):
        self.rowcount = rowcount
        self._row = row

    def fetchone(self):
        return self._row


class _FakeConn:
    """Records SQL and answers the two lookups transition_chunk performs."""

    def __init__(self, current_status=None, has_owner_column=False,
                 rowcount=1):
        self.current_status = current_status
        self.has_owner_column = has_owner_column
        self.rowcount = rowcount
        self.statements = []

    def execute(self, sql, params=None):
        self.statements.append((" ".join(sql.split()), params))
        if "SELECT status FROM remote_chunks" in sql:
            row = (None if self.current_status is None
                   else {"status": self.current_status})
            return _FakeCursor(row=row)
        return _FakeCursor(rowcount=self.rowcount)


class _Repo:
    """PgSessionMixin with the connection machinery stubbed out."""

    def __init__(self, conn):
        self.conn = conn

    @staticmethod
    def _column_exists_conn(conn, table, column):
        return conn.has_owner_column

    @staticmethod
    def _require_updated(cur, message):
        if cur.rowcount == 0:
            raise RuntimeError(message)

    def _transaction(self, operation, description):
        return operation(self.conn)


def _repo(**kwargs):
    from src.pg_sessions import PgSessionMixin

    class Repo(_Repo, PgSessionMixin):
        pass

    return Repo(_FakeConn(**kwargs))


class TransitionChunkTests(unittest.TestCase):
    def test_it_is_the_only_place_chunk_status_is_written(self):
        from src.pg_sessions import PgSessionMixin
        wrapper = inspect.getsource(PgSessionMixin.update_chunk_status)
        self.assertIn("transition_chunk", wrapper)
        # The wrapper contains no SQL of its own any more.
        self.assertNotIn("UPDATE remote_chunks", wrapper)

    def test_a_normal_transition_updates_and_timestamps(self):
        repo = _repo(current_status="packing")
        self.assertTrue(repo.transition_chunk(37, 4, ChunkStatus.BACKING))
        update = [s for s, _ in repo.conn.statements
                  if s.startswith("UPDATE remote_chunks")]
        self.assertEqual(len(update), 1)
        self.assertIn("status=%s", update[0])
        self.assertIn("updated_at=%s", update[0])

    def test_a_forbidden_transition_is_refused_before_any_write(self):
        repo = _repo(current_status="backing")
        with self.assertRaises(ForbiddenTransition):
            repo.transition_chunk(37, 4, ChunkStatus.PENDING)
        self.assertFalse([s for s, _ in repo.conn.statements
                          if s.startswith("UPDATE remote_chunks")],
                         "a forbidden transition still issued an UPDATE")

    def test_a_forbidden_caller_predicate_is_refused(self):
        repo = _repo()
        with self.assertRaises(ForbiddenTransition):
            repo.transition_chunk(37, 4, ChunkStatus.FETCHING,
                                  expected_from=ChunkStatus.BACKING)

    def test_a_compare_and_swap_predicate_becomes_a_where_clause(self):
        repo = _repo()
        repo.transition_chunk(37, 4, ChunkStatus.PACKING,
                              expected_from=ChunkStatus.FETCHING)
        update = [s for s, _ in repo.conn.statements
                  if s.startswith("UPDATE remote_chunks")][0]
        self.assertIn("status = ANY(%s)", update)

    def test_a_lost_compare_and_swap_returns_false_without_raising(self):
        repo = _repo(rowcount=0)
        self.assertFalse(
            repo.transition_chunk(37, 4, ChunkStatus.PACKING,
                                  expected_from=ChunkStatus.FETCHING))

    def test_an_unconditional_miss_is_still_an_error(self):
        repo = _repo(rowcount=0, current_status=None)
        with self.assertRaises(RuntimeError):
            repo.transition_chunk(37, 4, ChunkStatus.DONE, validate=False)

    def test_a_claim_without_the_columns_is_refused_not_ignored(self):
        """A claim you believe you hold but that is unenforced is worse than
        none, so this fails closed rather than dropping the predicate."""
        for kwargs in ({"owner_token": "t"}, {"attempt_id": "a"}):
            repo = _repo(has_owner_column=False)
            with self.assertRaises(RuntimeError) as caught:
                repo.transition_chunk(37, 4, ChunkStatus.PACKING,
                                      validate=False, **kwargs)
            self.assertIn("migration 014", str(caught.exception))

    def test_a_claim_with_the_columns_becomes_a_where_clause(self):
        repo = _repo(has_owner_column=True)
        repo.transition_chunk(37, 4, ChunkStatus.PACKING, validate=False,
                              owner_token="tok", attempt_id="att")
        update = [s for s, _ in repo.conn.statements
                  if s.startswith("UPDATE remote_chunks")][0]
        self.assertIn("owner_token=%s", update)
        self.assertIn("attempt_id=%s", update)

    def test_the_error_message_is_written_deliberately(self):
        repo = _repo(current_status="packing")
        repo.transition_chunk(37, 4, ChunkStatus.BACKUP_FAILED,
                              error_msg="robocopy exit 8")
        update = [s for s, _ in repo.conn.statements
                  if s.startswith("UPDATE remote_chunks")][0]
        self.assertIn("error_msg=%s", update)

    def test_the_error_message_can_be_cleared_deliberately(self):
        repo = _repo(current_status="fetch_failed")
        repo.transition_chunk(37, 4, ChunkStatus.PENDING, clear_error=True)
        update = [s for s, _ in repo.conn.statements
                  if s.startswith("UPDATE remote_chunks")][0]
        self.assertIn("error_msg=NULL", update)

    def test_no_error_argument_leaves_the_message_untouched(self):
        repo = _repo(current_status="packing")
        repo.transition_chunk(37, 4, ChunkStatus.BACKING)
        update = [s for s, _ in repo.conn.statements
                  if s.startswith("UPDATE remote_chunks")][0]
        self.assertNotIn("error_msg", update)

    def test_done_cleans_up_transferred_file_state(self):
        repo = _repo(current_status="backing")
        repo.transition_chunk(37, 4, ChunkStatus.DONE)
        deletes = [s for s, _ in repo.conn.statements
                   if s.startswith("DELETE FROM remote_file_state")]
        self.assertEqual(len(deletes), 1)
        # A source-missing row is provenance, not transfer state: it survives.
        self.assertIn("'source_missing'", deletes[0])

    def test_a_non_done_transition_deletes_nothing(self):
        repo = _repo(current_status="fetching")
        repo.transition_chunk(37, 4, ChunkStatus.PACKING)
        self.assertFalse([s for s, _ in repo.conn.statements
                          if s.startswith("DELETE")])

    def test_restarting_the_same_transition_is_idempotent(self):
        """A retry that re-applies the state it already reached is not a
        forbidden self-transition."""
        repo = _repo(current_status="backing")
        self.assertTrue(repo.transition_chunk(37, 4, ChunkStatus.BACKING))

    def test_the_compatibility_wrapper_stays_unvalidated(self):
        """Existing callers keep the old unconditional behaviour."""
        repo = _repo(current_status="backing")
        repo.update_chunk_status(37, 4, "pending")     # would be forbidden
        self.assertTrue([s for s, _ in repo.conn.statements
                         if s.startswith("UPDATE remote_chunks")])


# =============================================================================
# D. Runtime call sites are typed
# =============================================================================
class TypedCallSiteTests(unittest.TestCase):
    def test_the_writer_and_stager_use_the_enum_not_bare_strings(self):
        from src import remote_staging, remote_writer
        for module in (remote_staging, remote_writer):
            source = inspect.getsource(module)
            for literal in ("'backing'", "'done'", "'fetching'", "'packing'",
                            "'fetch_failed'", "'backup_failed'"):
                self.assertNotIn(
                    f"chunk_index, {literal}", source,
                    f"{module.__name__} still writes {literal} as a bare string")
            self.assertIn("ChunkStatus.", source)


if __name__ == "__main__":
    unittest.main()
