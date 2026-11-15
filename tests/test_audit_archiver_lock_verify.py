"""Audit H6 - the single-writer lock must be PROVEN, not assumed.

PostgreSQL: the archiver lock is a session-level advisory lock on one pinned
connection. When that session dies the lock is released, the pool reconnects,
and nothing else in the application notices. The tests kill the lock backend
(``pg_terminate_backend``).

SQLite: the lock is an OS file lock on ``<db>.archiver.lock`` plus an owner
token row. The tests drop the OS lock handle behind the manager's back, and
separately overwrite the token row / lock-file token.

On both backends every write entry must then refuse, and nothing may silently
re-acquire the lock.
"""
import threading
import unittest
from types import SimpleNamespace
from unittest import mock

from db_backends import create_test_db, each_backend, sqlite_only

from src.archiver_lock_guard import (archiver_lock_stop,
                                     assert_archiver_lock_held)
from src.exit_codes import ExitCode, REASON_ARCHIVER_LOCK_LOST


class LockGuardUnitTests(unittest.TestCase):
    """No database needed: the guard itself fails closed."""

    def test_a_database_layer_that_cannot_answer_is_refused(self):
        with self.assertRaisesRegex(RuntimeError, "cannot prove ownership"):
            assert_archiver_lock_held(SimpleNamespace())

    def test_any_error_while_asking_becomes_a_non_resumable_safety_block(self):
        def boom():
            raise OSError("socket closed")

        stop = archiver_lock_stop(SimpleNamespace(verify_archiver_lock=boom),
                                  session_id=7, chunk_index=3)
        self.assertEqual(stop.exit_code, ExitCode.SAFETY_BLOCK)
        self.assertEqual(stop.reason, REASON_ARCHIVER_LOCK_LOST)
        self.assertFalse(stop.resumable)
        self.assertTrue(stop.preserve_pack)
        self.assertEqual((stop.session_id, stop.chunk_index), (7, 3))

    def test_a_proven_lock_produces_no_stop(self):
        self.assertIsNone(archiver_lock_stop(
            SimpleNamespace(verify_archiver_lock=lambda: True)))


@each_backend
class ArchiverLockVerificationTests(unittest.TestCase):
    def setUp(self):
        self.tdb = create_test_db(self.BACKEND, "lto_lockv")
        self.addCleanup(self.tdb.drop)
        self.db = self.tdb.manager(migrations="startup")

    def _admin(self, sql, params=()):
        return self.tdb.run(sql, params)

    def _kill_lock_backend(self):
        """Make the lock disappear underneath a manager that believes it
        still holds it."""
        if self.tdb.is_sqlite:
            # The OS lock handle is gone (closed), exactly as if the process
            # had lost it; the manager's own bookkeeping is left untouched.
            self.db._file_lock.release()
            return
        pid = self.db._lock_backend_pid
        self.assertIsNotNone(pid)
        killed = self._admin("SELECT pg_terminate_backend(%s) AS ok", (pid,))
        self.assertTrue(killed[0]["ok"])
        # Wait until the server has really dropped the session and its lock.
        for _ in range(100):
            rows = self._admin(
                "SELECT 1 FROM pg_stat_activity WHERE pid=%s", (pid,))
            if not rows:
                return
            threading.Event().wait(0.05)
        self.fail("the lock backend did not terminate")

    def _lock_holders(self):
        if self.tdb.is_sqlite:
            from src.sqlite_lock import probe_held
            return [1] if probe_held(self.tdb.path) else []
        key = self.db.ARCHIVER_LOCK_KEY
        return self._admin(
            """SELECT l.pid FROM pg_locks l
               JOIN pg_database d ON d.oid=l.database
               WHERE l.locktype='advisory' AND l.granted
                 AND d.datname=current_database()
                 AND l.classid=%s AND l.objid=%s AND l.objsubid=1""",
            (key >> 32, key & 0xFFFFFFFF))

    # -- the proof itself -------------------------------------------------
    def test_verify_passes_while_the_pinned_session_holds_the_lock(self):
        self.db.acquire_archiver_lock()
        self.assertTrue(self.db.verify_archiver_lock())
        # Ordinary pooled work does not disturb the pinned session.
        self.db.register_tape("LK1", 12000)
        self.assertTrue(self.db.verify_archiver_lock())

    def test_verify_refuses_when_the_lock_was_never_taken(self):
        from src.pg_core import ArchiverLockLost

        with self.assertRaises(ArchiverLockLost):
            self.db.verify_archiver_lock()

    def test_verify_refuses_after_release(self):
        from src.pg_core import ArchiverLockLost

        self.db.acquire_archiver_lock()
        self.db.release_archiver_lock()
        with self.assertRaises(ArchiverLockLost):
            self.db.verify_archiver_lock()

    def test_a_terminated_lock_backend_is_detected_and_never_reacquired(self):
        from src.pg_core import ArchiverLockLost

        self.db.acquire_archiver_lock()
        self.assertEqual(len(self._lock_holders()), 1)
        self._kill_lock_backend()

        # The pool reconnects, so the rest of the application keeps working -
        # which is exactly why the loss used to be silent.
        self.assertFalse(self.db.tape_exists("nope"))

        with self.assertRaises(ArchiverLockLost):
            self.db.verify_archiver_lock()
        # Failing closed must not have quietly taken the lock again...
        self.assertEqual(self._lock_holders(), [])
        with self.assertRaises(ArchiverLockLost):
            self.db.verify_archiver_lock()
        # ...so a second archiver CAN take it, and this one still refuses.
        other = self.tdb.manager(init_schema=False)
        try:
            other.acquire_archiver_lock()
            self.assertTrue(other.verify_archiver_lock())
            with self.assertRaises(ArchiverLockLost):
                self.db.verify_archiver_lock()
        finally:
            other.close()

    def test_a_lock_held_by_a_different_session_does_not_satisfy_verify(self):
        """Same process, same key, but not the pinned session: still a loss."""
        from src.pg_core import ArchiverLockLost

        self.db.acquire_archiver_lock()
        if self.tdb.is_sqlite:
            self.db._lock_token = "0" * 32      # the token is not "ours"
        else:
            self.db._lock_backend_pid += 1      # the pinned pid is not "ours"
        with self.assertRaises(ArchiverLockLost):
            self.db.verify_archiver_lock()

    @sqlite_only("the owner-token row is the SQLite loss-detection layer")
    def test_an_overwritten_owner_token_row_is_a_loss(self):
        from src.pg_core import ArchiverLockLost

        self.db.acquire_archiver_lock()
        self.assertTrue(self.db.verify_archiver_lock())
        self._admin("UPDATE archiver_lock SET owner_token='someone-else'")
        with self.assertRaises(ArchiverLockLost):
            self.db.verify_archiver_lock()
        # The OS lock is still ours: nobody else can take it meanwhile.
        other = self.tdb.manager(init_schema=False)
        with self.assertRaisesRegex(RuntimeError, "Another archiver"):
            other.acquire_archiver_lock()

    @sqlite_only("the owner-token row is the SQLite loss-detection layer")
    def test_a_deleted_owner_token_row_is_a_loss(self):
        from src.pg_core import ArchiverLockLost

        self.db.acquire_archiver_lock()
        self._admin("DELETE FROM archiver_lock")
        with self.assertRaises(ArchiverLockLost):
            self.db.verify_archiver_lock()

    @sqlite_only("the lock file is the SQLite authority")
    def test_a_rewritten_lock_file_token_is_a_loss(self):
        import json
        import os
        from src.pg_core import ArchiverLockLost
        from src.sqlite_lock import OWNER_RECORD_BYTES, lock_path_for

        self.db.acquire_archiver_lock()
        record = json.dumps({"token": "f" * 32, "pid": 1}).encode()
        fd = self.db._file_lock._fd
        os.lseek(fd, 0, os.SEEK_SET)
        os.write(fd, record.ljust(OWNER_RECORD_BYTES, b" "))
        self.assertTrue(os.path.exists(lock_path_for(self.tdb.path)))
        with self.assertRaises(ArchiverLockLost):
            self.db.verify_archiver_lock()

    @sqlite_only("PostgreSQL's repeated acquire is a documented no-op")
    def test_a_repeated_acquire_does_not_hide_a_lost_lock(self):
        from src.pg_core import ArchiverLockLost

        self.db.acquire_archiver_lock()
        self.db.acquire_archiver_lock()             # still ours: fine
        self._kill_lock_backend()
        with self.assertRaises(ArchiverLockLost):
            self.db.acquire_archiver_lock()

    def test_a_second_manager_is_refused_while_the_lock_is_held(self):
        self.db.acquire_archiver_lock()
        other = self.tdb.manager(init_schema=False)
        with self.assertRaisesRegex(RuntimeError, "Another archiver"):
            other.acquire_archiver_lock()
        self.assertEqual(len(self._lock_holders()), 1)
        self.db.release_archiver_lock()
        other.acquire_archiver_lock()
        self.assertTrue(other.verify_archiver_lock())

    # -- the write entries ------------------------------------------------
    def _seed_chunk(self, status="packing"):
        self.db.register_tape("LKT", 12000)
        snap = self._admin(
            """INSERT INTO remote_snapshots (remote_host,remote_path,
                   fingerprint,total_files,total_bytes,created_at)
               VALUES ('h','/r','fp-lock',1,1,now()) RETURNING snapshot_id"""
        )[0]["snapshot_id"]
        plan = self._admin(
            """INSERT INTO remote_plans (snapshot_id,fingerprint,chunk_count,
                   created_at) VALUES (%s,'pf-lock',1,now())
               RETURNING plan_id""", (snap,))[0]["plan_id"]
        session_id = self._admin(
            """INSERT INTO remote_sessions (session_label,remote_host,
                   remote_user,remote_path,tape_label,staging_dir,total_files,
                   total_bytes,chunk_count,plan_id,created_at,status,
                   tape_generation)
               VALUES ('LOCKS','h','u','/r','LKT','C:\\stg',1,1,1,%s,now(),
                       'active',1) RETURNING session_id""",
            (plan,))[0]["session_id"]
        self.tdb.insert_remote_chunk(session_id, 0, status)
        return session_id

    def _status(self, session_id):
        return self._admin(
            "SELECT status FROM remote_chunks WHERE session_id=%s",
            (session_id,))[0]["status"]

    def test_the_backing_transition_refuses_once_the_lock_is_lost(self):
        """'backing' is the instant a physical write begins."""
        from src.pg_core import ArchiverLockLost

        session_id = self._seed_chunk("packing")
        self.db.acquire_archiver_lock()
        self._kill_lock_backend()
        with self.assertRaises(ArchiverLockLost):
            self.db.update_chunk_status(session_id, 0, "backing")
        self.assertEqual(self._status(session_id), "packing")

    def test_the_backing_transition_proceeds_while_the_lock_is_held(self):
        session_id = self._seed_chunk("packing")
        self.db.acquire_archiver_lock()
        self.db.update_chunk_status(session_id, 0, "backing")
        self.assertEqual(self._status(session_id), "backing")

    def test_the_remote_write_group_stops_before_touching_the_tape(self):
        from test_remote_failure_hardening import _desc, _orch

        self.db.acquire_archiver_lock()
        self._kill_lock_backend()

        orch = _orch()
        orch._consumer_chunk = None
        launched = []
        orch._backup_writer = lambda cls=None: launched.append(cls)
        orch.db = SimpleNamespace(
            verify_archiver_lock=self.db.verify_archiver_lock,
            get_chunk_packaging_format=lambda sid, ci: _desc(0).packaging_format,
            get_tape=lambda label: {"total_capacity": 10**9},
            update_chunk_status=lambda *a: launched.append(a))
        with mock.patch("src.remote_writer._acquire_tape_io_lock") as acquire, \
                mock.patch("src.remote_writer._release_tape_io_lock"):
            stop = orch._write_chunk_group(
                7, [_desc(0)], "T", False, threading.Event())
        self.assertIsNotNone(stop)
        self.assertEqual(stop.exit_code, ExitCode.SAFETY_BLOCK)
        self.assertEqual(stop.reason, REASON_ARCHIVER_LOCK_LOST)
        self.assertFalse(stop.resumable)
        acquire.assert_not_called()
        self.assertEqual(launched, [])
        # The stop is recorded, so the pipeline cannot start another group.
        self.assertEqual(orch._get_recorded_stop().reason,
                         REASON_ARCHIVER_LOCK_LOST)

    def test_the_local_orchestrator_refuses_before_preparing_the_tape(self):
        from src.local_orchestrator import LocalOrchestrator
        from src.pg_core import ArchiverLockLost

        self.db.acquire_archiver_lock()
        self._kill_lock_backend()

        local = object.__new__(LocalOrchestrator)
        local.db = self.db
        local._prepare_tape_for_chunk = mock.Mock(
            side_effect=AssertionError("the tape must not be touched"))
        with self.assertRaises(ArchiverLockLost):
            local._process_chunk({"session_id": 1}, 0, [])
        local._prepare_tape_for_chunk.assert_not_called()


if __name__ == "__main__":
    unittest.main()
