"""Deleting a session is refused while it is active or an archiver runs (M8).

The inspector used to delete an ``active`` session with one click. The refusal
is enforced below the widget: ``PgDatabaseManager.delete_session`` (the only
write path) and ``InspectorRepository.session_delete_blockers`` (the read-only
pre-flight that also checks the archiver lock).

The pre-flight's rules are tested without a server; the write path is tested
end to end against a throwaway PostgreSQL database and skips when none is
configured.
"""
import unittest
import uuid
from datetime import datetime, timezone
from unittest import mock

from db_backends import (create_test_db, each_backend, postgresql_only,
                         sqlite_only)
from pg_test_guard import (SKIP_REASON, create_test_database,
                           drop_test_database, pg_available)
from src import inspector_repository
from src.inspector_repository import InspectorRepository


class _Result:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class _StubRepository(InspectorRepository):
    """The real blocker logic over canned rows; no connection."""

    def __init__(self, statuses):
        self.db_path = "stub-dsn"
        self.statuses = statuses

    def _table_exists(self, name):
        return name in ("local_sessions", "remote_sessions")

    def _execute(self, sql, params=()):
        table = "local_sessions" if "local_sessions" in sql else "remote_sessions"
        status = self.statuses.get((table, params[0]), "missing")
        return _Result(None if status == "missing" else {"status": status})


class SessionDeleteBlockerRules(unittest.TestCase):
    def _blockers(self, statuses, kind, session_id, holders=0):
        repo = _StubRepository(statuses)
        with mock.patch.object(inspector_repository, "archiver_lock_status",
                               return_value=holders) as probe:
            reasons = repo.session_delete_blockers(kind, session_id)
        probe.assert_called_once_with("stub-dsn")
        return reasons

    def test_finished_session_with_no_archiver_is_deletable(self):
        statuses = {("remote_sessions", 36): "abandoned",
                    ("local_sessions", 4): "completed"}
        self.assertEqual(self._blockers(statuses, "remote", 36), [])
        self.assertEqual(self._blockers(statuses, "local", 4), [])

    def test_active_session_is_blocked_for_both_kinds(self):
        statuses = {("remote_sessions", 37): "active",
                    ("local_sessions", 5): " Active "}
        for kind, session_id in (("remote", 37), ("local", 5)):
            reasons = self._blockers(statuses, kind, session_id)
            self.assertEqual(len(reasons), 1)
            self.assertIn("marked active", reasons[0])

    def test_held_archiver_lock_blocks_even_a_finished_session(self):
        reasons = self._blockers(
            {("remote_sessions", 36): "abandoned"}, "remote", 36, holders=1)
        self.assertEqual(len(reasons), 1)
        self.assertIn("archiver lock is held", reasons[0])

    def test_every_reason_is_reported_together(self):
        reasons = self._blockers(
            {("remote_sessions", 37): "active"}, "remote", 37, holders=2)
        self.assertEqual(len(reasons), 2)

    def test_missing_session_and_unknown_kind_are_blocked(self):
        self.assertIn("does not exist",
                      self._blockers({}, "remote", 99)[0])
        repo = _StubRepository({})
        self.assertIn("unknown session kind",
                      repo.session_delete_blockers("tape", 1)[0])

    def test_require_raises_with_the_reasons(self):
        repo = _StubRepository({("remote_sessions", 37): "active"})
        with mock.patch.object(inspector_repository, "archiver_lock_status",
                               return_value=0):
            with self.assertRaisesRegex(RuntimeError, "marked active"):
                repo.require_session_deletable("remote", 37)
            repo.statuses[("remote_sessions", 37)] = "completed"
            repo.require_session_deletable("remote", 37)


def _pg_available():
    try:
        return pg_available()
    except Exception:
        return False


@each_backend
class DeleteSessionWritePathTests(unittest.TestCase):
    """End to end against a throwaway database - never the live catalog."""

    @classmethod
    def setUpClass(cls):
        from src.pg_db import PgDatabaseManager

        cls.tdb = create_test_db(getattr(cls, 'BACKEND', None) or 'postgresql', "lto_sessdel")
        cls.dbname, cls.conninfo = cls.tdb.name, cls.tdb.conninfo
        cls.db = cls.tdb.manager(migrations="startup")
        cls.db.apply_directory_catalog_schema()
        cls.db.register_tape("Tape_T")

    @classmethod
    def tearDownClass(cls):
        try:
            cls.db.close()
        except Exception:
            pass
        cls.tdb.drop()

    def _exec(self, sql, params=()):
        with self.db._pool.connection() as conn:
            conn.execute(self.tdb.adapt(sql), params)

    def _count(self, sql, params=()):
        with self.db._pool.connection() as conn:
            return conn.execute(sql, params).fetchone()["n"]

    def _remote(self, session_id, status):
        self._exec(
            """INSERT INTO remote_sessions
               (session_id, session_label, remote_host, remote_user,
                remote_path, tape_label, staging_dir, chunk_count,
                created_at, status, scan_complete, tape_generation)
               VALUES (%s,%s,'h','u','/p','Tape_T','C:\\stg',1,%s,%s,TRUE,1)""",
            (session_id, f"S{session_id}_{uuid.uuid4().hex[:6]}",
             datetime.now(timezone.utc), status))
        self._exec(
            """INSERT INTO remote_chunks
               (session_id, chunk_index, status, updated_at)
               VALUES (%s, 0, 'pending', %s)""",
            (session_id, datetime.now(timezone.utc)))

    def _remote_rows(self, session_id):
        return (
            self._count("SELECT COUNT(*) AS n FROM remote_sessions "
                        "WHERE session_id=%s", (session_id,)),
            self._count("SELECT COUNT(*) AS n FROM remote_chunks "
                        "WHERE session_id=%s", (session_id,)))

    def test_active_remote_session_is_refused_and_nothing_is_deleted(self):
        self._remote(9101, "active")
        with self.assertRaisesRegex(RuntimeError, "marked active"):
            self.db.delete_session("remote", 9101)
        self.assertEqual(self._remote_rows(9101), (1, 1))

    def test_finished_remote_session_is_deleted_with_its_chunks(self):
        self._remote(9102, "abandoned")
        self.assertEqual(self.db.delete_session("remote", 9102), 1)
        self.assertEqual(self._remote_rows(9102), (0, 0))

    def test_active_local_session_is_refused_then_deletable_when_done(self):
        session_id = self.db.create_local_session(
            f"L_{uuid.uuid4().hex[:8]}", "D:\\source",
            [[{"name": "dir_a", "size_bytes": 10}]])
        with self.assertRaisesRegex(RuntimeError, "marked active"):
            self.db.delete_session("local", session_id)
        self.assertEqual(self._count(
            "SELECT COUNT(*) AS n FROM local_chunks_manifest "
            "WHERE session_id=%s", (session_id,)), 1)
        self.db.update_local_session(session_id, status="completed")
        self.assertEqual(self.db.delete_session("local", session_id), 1)
        self.assertIsNone(self.db.get_local_session(session_id))

    def test_missing_session_still_reports_not_found(self):
        with self.assertRaisesRegex(RuntimeError, "not found"):
            self.db.delete_session("remote", 987654)

    def test_repository_preflight_reads_the_real_tables(self):
        self._remote(9103, "active")
        self._remote(9104, "completed")
        # archiver_lock_status counts advisory locks cluster-wide; another
        # test database on this shared server must not decide this test.
        with mock.patch.object(inspector_repository, "archiver_lock_status",
                               return_value=0):
            with InspectorRepository(self.conninfo) as repo:
                self.assertIn("marked active",
                              repo.session_delete_blockers("remote", 9103)[0])
                self.assertEqual(
                    repo.session_delete_blockers("remote", 9104), [])
                self.assertIn("does not exist",
                              repo.session_delete_blockers("remote", 1)[0])


if __name__ == "__main__":
    unittest.main()
