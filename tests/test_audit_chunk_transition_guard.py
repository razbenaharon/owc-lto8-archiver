"""Audit H5 - chunk state transitions are enforced, in Python AND in the DB.

``backing`` means the physical tape write began; ``done`` means it committed.
Moving either back into the pool lets the pipeline write the chunk a second
time. Before this fix every pipeline call site went through
``update_chunk_status``, which skipped the matrix, and nothing in the database
stopped a raw UPDATE. These tests run against disposable PostgreSQL.
"""
import unittest
from typing import Any, cast

try:
    import psycopg
    from psycopg import errors as pg_errors
    from psycopg.rows import dict_row
except ImportError:                          # pragma: no cover - guard handles it
    psycopg = None
    pg_errors = None
    dict_row = None

from pg_test_guard import (SKIP_REASON, create_test_database,
                           drop_test_database, pg_available)

from src.pipeline_types import (CHUNK_OVERRIDE_TRANSITIONS, CHUNK_TRANSITIONS,
                                ChunkStatus, ChunkTransitionOverride,
                                ForbiddenTransition,
                                assert_chunk_override_transition)

ALL_STATUSES = [status.value for status in ChunkStatus]


def _pg_available():
    return psycopg is not None and pg_available()


class OverrideVocabularyTests(unittest.TestCase):
    """Pure rules: an override is a short, named list - not a bypass."""

    def test_overrides_only_ever_leave_backing_or_done(self):
        for name, pairs in CHUNK_OVERRIDE_TRANSITIONS.items():
            for source, target in pairs:
                with self.subTest(override=name.value, move=(source, target)):
                    self.assertIn(source, (ChunkStatus.BACKING,
                                           ChunkStatus.DONE))
                    # An override exists only for moves the matrix forbids.
                    self.assertNotIn(target, CHUNK_TRANSITIONS[source])

    def test_every_override_name_has_a_declared_move_list(self):
        self.assertEqual(set(CHUNK_OVERRIDE_TRANSITIONS),
                         set(ChunkTransitionOverride))

    def test_no_override_can_put_a_done_chunk_back_into_backing(self):
        for name in ChunkTransitionOverride:
            with self.assertRaises(ForbiddenTransition):
                assert_chunk_override_transition(name, "done", "backing")

    def test_an_unknown_override_or_status_is_refused(self):
        for args in (("anything_goes", "done", "pending"),
                     ("tape_reset", "nonsense", "pending"),
                     (None, "done", "pending")):
            with self.assertRaises(ForbiddenTransition):
                assert_chunk_override_transition(*args)


class _ChunkFixture:
    def setUp(self):
        from src.pg_db import PgDatabaseManager

        self.dbname, self.conninfo = create_test_database("lto_h5")
        self.db = PgDatabaseManager(self.conninfo)
        self.addCleanup(self._drop)
        self.db.register_tape("H5T", 12000)
        snap = self._q(
            """INSERT INTO remote_snapshots (remote_host,remote_path,
                   fingerprint,total_files,total_bytes,created_at)
               VALUES ('h','/r','fp-h5',1,1,now()) RETURNING snapshot_id"""
        )[0]["snapshot_id"]
        plan = self._q(
            """INSERT INTO remote_plans (snapshot_id,fingerprint,chunk_count,
                   created_at) VALUES (%s,'pf-h5',1,now())
               RETURNING plan_id""", (snap,))[0]["plan_id"]
        self.session_id = self._q(
            """INSERT INTO remote_sessions (session_label,remote_host,
                   remote_user,remote_path,tape_label,staging_dir,total_files,
                   total_bytes,chunk_count,plan_id,created_at,status,
                   tape_generation)
               VALUES ('H5','h','u','/r','H5T','C:\\stg',1,1,1,%s,now(),
                       'active',1) RETURNING session_id""",
            (plan,))[0]["session_id"]

    def _drop(self):
        try:
            self.db.close()
        except Exception:
            pass
        drop_test_database(self.dbname)

    def _q(self, sql, params=()):
        with cast(Any, psycopg).connect(
                self.conninfo, autocommit=True,
                row_factory=cast(Any, dict_row)) as conn:
            cur = conn.execute(sql, params)
            return cur.fetchall() if cur.description else []

    def _chunk(self, status, index=0):
        """Create (or replace) a chunk directly in ``status``."""
        self._q("DELETE FROM remote_chunks WHERE session_id=%s "
                "AND chunk_index=%s", (self.session_id, index))
        self._q(
            """INSERT INTO remote_chunks (session_id,chunk_index,status,
                   updated_at) VALUES (%s,%s,%s,now())""",
            (self.session_id, index, status))

    def _status(self, index=0):
        return self._q(
            "SELECT status FROM remote_chunks WHERE session_id=%s "
            "AND chunk_index=%s", (self.session_id, index))[0]["status"]

    def _raw_update(self, to_status, index=0):
        self._q("UPDATE remote_chunks SET status=%s WHERE session_id=%s "
                "AND chunk_index=%s", (to_status, self.session_id, index))

    def _overrides(self):
        return self._q(
            """SELECT from_status, to_status, override_name, reason,
                      authorized_by, consumed_at
               FROM remote_chunk_transition_overrides
               WHERE session_id=%s ORDER BY override_id""",
            (self.session_id,))


@unittest.skipUnless(_pg_available(), SKIP_REASON)
class ValidatedSetterTests(_ChunkFixture, unittest.TestCase):
    """The pipeline's setter is no longer a way round the matrix."""

    def test_the_auditors_repro_backing_cannot_return_to_pending(self):
        self._chunk("backing")
        with self.assertRaises(ForbiddenTransition):
            self.db.update_chunk_status(self.session_id, 0, "pending")
        self.assertEqual(self._status(), "backing")

    def test_backing_is_only_ever_released_forward_to_done(self):
        for target in ALL_STATUSES:
            if target in ("backing", "done"):
                continue
            with self.subTest(target=target):
                self._chunk("backing")
                with self.assertRaises(ForbiddenTransition):
                    self.db.update_chunk_status(self.session_id, 0, target)
                self.assertEqual(self._status(), "backing")
        self._chunk("backing")
        self.db.update_chunk_status(self.session_id, 0, "done")
        self.assertEqual(self._status(), "done")

    def test_done_is_terminal_for_every_target(self):
        for target in ALL_STATUSES:
            if target == "done":
                continue
            with self.subTest(target=target):
                self._chunk("done")
                with self.assertRaises(ForbiddenTransition):
                    self.db.update_chunk_status(self.session_id, 0, target)
                self.assertEqual(self._status(), "done")

    def test_an_undeclared_status_is_refused(self):
        self._chunk("pending")
        with self.assertRaises(ValueError):
            self.db.update_chunk_status(self.session_id, 0, "writing")
        self.assertEqual(self._status(), "pending")

    def test_there_is_no_unvalidated_mode_left(self):
        self._chunk("backing")
        with self.assertRaises(TypeError):
            self.db.transition_chunk(self.session_id, 0, "pending",
                                     validate=False)
        self.assertEqual(self._status(), "backing")

    def test_every_transition_the_pipeline_performs_is_still_allowed(self):
        """One entry per real call site in remote_staging / remote_writer."""
        paths = {
            "normal write": ["pending", "fetching", "packing", "backing",
                             "done"],
            "fetch failed then retried": ["pending", "fetching",
                                          "fetch_failed", "fetching"],
            "pack failed": ["fetching", "packing", "fetch_failed"],
            "crash while packing, re-fetched": ["packing", "fetching"],
            "crash while fetching, re-fetched": ["fetching", "fetching"],
            "all files source-missing (skip tape)": ["fetching", "done"],
            "failure before the write started": ["packing", "backup_failed",
                                                 "fetching"],
            "preserved pack written after a reset": ["pending", "backing",
                                                     "done"],
            "preserved pack fails before the write": ["pending",
                                                      "backup_failed"],
            "write completed without a start callback": ["packing", "done"],
        }
        for name, states in paths.items():
            with self.subTest(path=name):
                self._chunk(states[0])
                for target in states[1:]:
                    self.db.update_chunk_status(self.session_id, 0, target)
                    self.assertEqual(self._status(), target)

    def test_a_claim_release_cannot_return_a_done_chunk_to_the_pool(self):
        self.db.apply_incremental_scan_schema()
        self._chunk("done")
        self._q("UPDATE remote_chunks SET owner_token='owner-A' "
                "WHERE session_id=%s", (self.session_id,))
        with self.assertRaises(ForbiddenTransition):
            self.db.release_chunk_claim(self.session_id, 0, "owner-A",
                                        to_status="pending")
        self.assertEqual(self._status(), "done")
        # Dropping the lease without a status change is still fine.
        self.assertTrue(
            self.db.release_chunk_claim(self.session_id, 0, "owner-A"))
        self.assertEqual(self._status(), "done")


@unittest.skipUnless(_pg_available(), SKIP_REASON)
class DatabaseGuardTests(_ChunkFixture, unittest.TestCase):
    """Migration 020: even a raw UPDATE cannot break the rule."""

    def test_a_raw_update_cannot_move_backing_backwards(self):
        for target in ALL_STATUSES:
            if target in ("backing", "done"):
                continue
            with self.subTest(target=target):
                self._chunk("backing")
                with self.assertRaises(pg_errors.CheckViolation):
                    self._raw_update(target)
                self.assertEqual(self._status(), "backing")

    def test_a_raw_update_cannot_move_done_anywhere(self):
        for target in ALL_STATUSES:
            if target == "done":
                continue
            with self.subTest(target=target):
                self._chunk("done")
                with self.assertRaises(pg_errors.CheckViolation):
                    self._raw_update(target)
                self.assertEqual(self._status(), "done")

    def test_a_set_based_update_is_refused_as_a_whole(self):
        self._chunk("pending", 0)
        self._chunk("done", 1)
        with self.assertRaises(pg_errors.CheckViolation):
            self._q("UPDATE remote_chunks SET status='pending' "
                    "WHERE session_id=%s", (self.session_id,))
        self.assertEqual(self._status(1), "done")

    def test_the_forward_and_pre_write_moves_are_untouched(self):
        self._chunk("backing")
        self._raw_update("done")
        self.assertEqual(self._status(), "done")
        self._chunk("packing")
        for target in ("pending", "fetching", "fetch_failed", "packing",
                       "backup_failed", "backing"):
            self._raw_update(target)
            self.assertEqual(self._status(), target)

    def test_other_columns_of_a_terminal_chunk_stay_writable(self):
        self._chunk("done")
        self._q("UPDATE remote_chunks SET error_msg='note', status='done', "
                "updated_at=now() WHERE session_id=%s", (self.session_id,))
        self.assertEqual(self._status(), "done")

    def test_the_migration_is_idempotent_and_rewrites_nothing(self):
        self._chunk("backing", 0)
        self._chunk("done", 1)
        before = self._q("SELECT chunk_index, status, updated_at, xmin::text "
                         "FROM remote_chunks ORDER BY chunk_index")
        self.db._init_schema()
        self.db._init_schema()
        after = self._q("SELECT chunk_index, status, updated_at, xmin::text "
                        "FROM remote_chunks ORDER BY chunk_index")
        self.assertEqual(before, after)
        triggers = self._q(
            """SELECT tgname FROM pg_trigger
               WHERE tgrelid='remote_chunks'::regclass
                 AND tgname='trg_remote_chunks_status_transition'""")
        self.assertEqual(len(triggers), 1)
        with self.assertRaises(pg_errors.CheckViolation):
            self._raw_update("pending", 1)


@unittest.skipUnless(_pg_available(), SKIP_REASON)
class NamedOverrideTests(_ChunkFixture, unittest.TestCase):
    """The only way backwards is explicit, named, audited and single-use."""

    def _override(self, to_status="pending", expected_from="done",
                  override="operator_reconciled", reason="cartridge audited",
                  authorized_by="operator@lab"):
        return self.db.override_chunk_transition(
            self.session_id, 0, to_status, expected_from=expected_from,
            override=override, reason=reason, authorized_by=authorized_by)

    def test_a_named_override_moves_the_chunk_and_leaves_an_audit_row(self):
        self._chunk("done")
        self.assertTrue(self._override())
        self.assertEqual(self._status(), "pending")
        rows = self._overrides()
        self.assertEqual(len(rows), 1)
        self.assertEqual(
            (rows[0]["from_status"], rows[0]["to_status"],
             rows[0]["override_name"], rows[0]["reason"],
             rows[0]["authorized_by"]),
            ("done", "pending", "operator_reconciled", "cartridge audited",
             "operator@lab"))
        self.assertIsNotNone(rows[0]["consumed_at"])

    def test_an_override_authorises_exactly_one_transition(self):
        self._chunk("done")
        self.assertTrue(self._override())
        self._raw_update("fetching")
        self._raw_update("packing")
        self._raw_update("backing")
        self._raw_update("done")
        with self.assertRaises(pg_errors.CheckViolation):
            self._raw_update("pending")
        self.assertEqual(self._status(), "done")

    def test_an_override_is_scoped_to_its_own_chunk_and_move(self):
        self._chunk("done", 0)
        self._chunk("done", 1)
        self._q(
            """INSERT INTO remote_chunk_transition_overrides
                   (session_id,chunk_index,from_status,to_status,
                    override_name,reason,authorized_by)
               VALUES (%s,0,'done','pending','operator_reconciled','r','me')""",
            (self.session_id,))
        with self.assertRaises(pg_errors.CheckViolation):
            self._raw_update("pending", 1)
        with self.assertRaises(pg_errors.CheckViolation):
            self._raw_update("fetch_failed", 0)
        self._raw_update("pending", 0)
        self.assertEqual((self._status(0), self._status(1)),
                         ("pending", "done"))

    def test_a_lost_compare_and_swap_records_nothing(self):
        self._chunk("backing")
        self.assertFalse(self._override(expected_from="done"))
        self.assertEqual(self._status(), "backing")
        self.assertEqual(self._overrides(), [])

    def test_an_override_must_be_named_permitted_and_justified(self):
        self._chunk("done")
        cases = (
            dict(override="because_i_said_so"),
            dict(override="tape_reset", to_status="backing"),
            dict(override="tape_reset", to_status="fetching"),
            dict(reason="   "),
            dict(authorized_by=""),
        )
        for kwargs in cases:
            with self.subTest(**kwargs):
                with self.assertRaises((ForbiddenTransition, ValueError)):
                    self._override(**kwargs)
                self.assertEqual(self._status(), "done")
        self.assertEqual(self._overrides(), [])

    def test_the_pipeline_setter_cannot_use_a_pending_override(self):
        """An override row does not turn update_chunk_status into a bypass."""
        self._chunk("done")
        self._q(
            """INSERT INTO remote_chunk_transition_overrides
                   (session_id,chunk_index,from_status,to_status,
                    override_name,reason,authorized_by)
               VALUES (%s,0,'done','pending','operator_reconciled','r','me')""",
            (self.session_id,))
        with self.assertRaises(ForbiddenTransition):
            self.db.update_chunk_status(self.session_id, 0, "pending")
        self.assertEqual(self._status(), "done")


@unittest.skipUnless(_pg_available(), SKIP_REASON)
class TapeResetUsesTheNamedOverrideTests(_ChunkFixture, unittest.TestCase):
    """The reset is the one recovery that legitimately un-does 'done'."""

    def _hardware_succeeded_operation(self, operation_id="op-h5"):
        sha = "0" * 64
        self.db.create_tape_reset_intent(
            operation_id=operation_id, tape_label="H5T", expected_drive="X:",
            expected_drive_serial="SER", expected_ltfs_gen=1, expected_vcr=1,
            backup_path="b.dump", backup_sha256=sha,
            restore_list_path="r.txt", restore_list_sha256=sha,
            shadow_database="shadow", impact_report_path="i.json",
            impact_report_sha256=sha, impact={})
        self.db.mark_tape_reset_hardware_started(operation_id)
        self.db.mark_tape_reset_hardware_succeeded(operation_id, {"ok": True})
        return operation_id

    def test_cleanup_moves_chunks_back_with_one_audit_row_each(self):
        self._chunk("done", 0)
        self._chunk("backing", 1)
        self._chunk("packing", 2)
        self._chunk("done", 3)          # written to another tape: untouched
        operation_id = self._hardware_succeeded_operation()
        transitions = [
            {"session_id": self.session_id, "chunk_index": index,
             "before": before, "after": "pending", "reason": "erased"}
            for index, before in ((0, "done"), (1, "backing"), (2, "packing"))]
        self.db.apply_tape_reset_cleanup(operation_id, transitions)

        self.assertEqual([self._status(i) for i in range(4)],
                         ["pending", "pending", "pending", "done"])
        rows = self._overrides()
        self.assertEqual(
            sorted((r["from_status"], r["to_status"], r["override_name"])
                   for r in rows),
            [("backing", "pending", "tape_reset"),
             ("done", "pending", "tape_reset")])
        for row in rows:
            self.assertEqual(row["authorized_by"], f"tape-reset:{operation_id}")
            self.assertIn(operation_id, row["reason"])
            self.assertIsNotNone(row["consumed_at"])

    def test_scope_drift_rolls_everything_back_including_the_audit_rows(self):
        self._chunk("done", 0)
        self._chunk("fetching", 1)      # the report said 'backing'
        operation_id = self._hardware_succeeded_operation()
        transitions = [
            {"session_id": self.session_id, "chunk_index": 0,
             "before": "done", "after": "pending"},
            {"session_id": self.session_id, "chunk_index": 1,
             "before": "backing", "after": "pending"}]
        with self.assertRaisesRegex(RuntimeError, "scope drift"):
            self.db.apply_tape_reset_cleanup(operation_id, transitions)
        self.assertEqual([self._status(0), self._status(1)],
                         ["done", "fetching"])
        self.assertEqual(self._overrides(), [])

    def test_a_reset_cannot_be_asked_to_make_an_unlisted_move(self):
        from src.pipeline_types import ForbiddenTransition as Forbidden

        self._chunk("done", 0)
        operation_id = self._hardware_succeeded_operation()
        with self.assertRaises(Forbidden):
            self.db.apply_tape_reset_cleanup(operation_id, [
                {"session_id": self.session_id, "chunk_index": 0,
                 "before": "done", "after": "backing"}])
        self.assertEqual(self._status(0), "done")


if __name__ == "__main__":
    unittest.main()
