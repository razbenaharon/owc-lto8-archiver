"""Stale-session reconciliation, and the CLI's expected-refusal contract.

The classification rules are pure and tested without a database. The guard
behaviour that actually protects a live archiver is tested end to end against a
throwaway PostgreSQL database, skipped automatically when no server is
reachable (same convention as ``test_pg_integration``).
"""
from datetime import datetime, timedelta, timezone
import io
import unittest
import uuid
from contextlib import redirect_stderr, redirect_stdout
from typing import Any, TYPE_CHECKING, cast
from unittest import mock

if TYPE_CHECKING:
    import psycopg
    from psycopg.rows import dict_row
else:
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError:  # pragma: no cover
        psycopg = None
        dict_row = None

import inspect_db
from src.cli_errors import OperationalError
from pg_test_guard import (SKIP_REASON, create_test_database,
                           drop_test_database, pg_available)
from src.session_reconcile import (
    ABANDONED_STATUS,
    COMPLETED_STATUS,
    DEFAULT_IDLE_SECONDS,
    VERDICT_AMBIGUOUS,
    VERDICT_STALE_ABANDONED,
    VERDICT_STALE_COMPLETED,
    _classify,
    reconcile_stale_remote_sessions,
    session_forensics,
    status_file_session,
)

NOW = datetime(2026, 7, 26, 12, 0, tzinfo=timezone.utc)
LONG_AGO = NOW - timedelta(days=18)


def _session(**overrides):
    base = {
        "session_id": 36,
        "session_label": "REMOTE_test",
        "status": "active",
        "scan_complete": True,
        "chunk_count": 0,
        "created_at": LONG_AGO,
    }
    base.update(overrides)
    return base


def _states(mapping, last_update=LONG_AGO):
    return [{"status": status, "chunks": count, "last_update": last_update}
            for status, count in mapping.items()]


class ClassificationTests(unittest.TestCase):
    """Every verdict is derived from evidence; ambiguity always wins."""

    def _verdict(self, session, states, idle=DEFAULT_IDLE_SECONDS):
        verdict, reason, evidence = _classify(session, states, idle, NOW)
        return verdict, reason, evidence

    def test_session_36_shape_is_abandoned(self):
        """Scan never finished, 9 pending + 1 failed: unfinishable, not done."""
        verdict, reason, evidence = self._verdict(
            _session(chunk_count=11, scan_complete=False),
            _states({"done": 1, "fetch_failed": 1, "pending": 9}))
        self.assertEqual(verdict, VERDICT_STALE_ABANDONED)
        self.assertIn("pending", reason)
        self.assertEqual(evidence["chunks_by_state"],
                         {"done": 1, "fetch_failed": 1, "pending": 9})
        self.assertEqual(evidence["idle_seconds"],
                         int((NOW - LONG_AGO).total_seconds()))

    def test_all_chunks_done_after_a_complete_scan_is_completed(self):
        verdict, _, _ = self._verdict(
            _session(chunk_count=6, scan_complete=True),
            _states({"done": 6}))
        self.assertEqual(verdict, VERDICT_STALE_COMPLETED)

    def test_recent_activity_is_never_stale(self):
        """The idle threshold is the first gate: a busy session is untouchable."""
        verdict, reason, _ = self._verdict(
            _session(chunk_count=11, scan_complete=False),
            _states({"pending": 9, "done": 2},
                    last_update=NOW - timedelta(minutes=30)))
        self.assertEqual(verdict, VERDICT_AMBIGUOUS)
        self.assertIn("idle threshold", reason)

    def test_transient_chunk_blocks_classification(self):
        """'backing' means a tape write was in flight — outcome unknowable."""
        for state in ("fetching", "packing", "backing"):
            with self.subTest(state=state):
                verdict, reason, _ = self._verdict(
                    _session(chunk_count=11),
                    _states({state: 1, "pending": 10}))
                self.assertEqual(verdict, VERDICT_AMBIGUOUS)
                self.assertIn(state, reason)

    def test_session_that_planned_nothing_is_abandoned(self):
        verdict, reason, _ = self._verdict(_session(chunk_count=0), [])
        self.assertEqual(verdict, VERDICT_STALE_ABANDONED)
        self.assertIn("archived", reason)

    def test_incomplete_scan_with_all_chunks_done_is_ambiguous(self):
        """More chunks could still have belonged to the plan; refuse to guess."""
        verdict, reason, _ = self._verdict(
            _session(chunk_count=3, scan_complete=False),
            _states({"done": 3}))
        self.assertEqual(verdict, VERDICT_AMBIGUOUS)
        self.assertIn("membership", reason)

    def test_chunk_count_mismatch_is_ambiguous(self):
        """Rows present disagree with the recorded plan size: do not guess."""
        verdict, reason, _ = self._verdict(
            _session(chunk_count=9, scan_complete=True),
            _states({"done": 3}))
        self.assertEqual(verdict, VERDICT_AMBIGUOUS)
        self.assertIn("does not add up", reason)


class StatusFileTests(unittest.TestCase):
    def test_missing_status_file_is_not_an_error(self):
        self.assertIsNone(status_file_session(None))
        self.assertIsNone(status_file_session("C:\\nope\\nowhere"))


class CliRefusalTests(unittest.TestCase):
    """Deliberate refusals print one line and exit 1 — never a traceback."""

    def setUp(self):
        cfg = mock.MagicMock()
        cfg.local_manifest_archive_root = "C:\\archive_root"
        cfg.staging_dir = "C:\\staging"
        cfg.backup_log_dir = "C:\\logs"
        patcher = mock.patch.object(inspect_db, "_config", return_value=cfg)
        patcher.start()
        self.addCleanup(patcher.stop)
        conninfo = mock.patch.object(
            inspect_db, "_conninfo", return_value="dbname=test")
        conninfo.start()
        self.addCleanup(conninfo.stop)

    def _run(self, argv):
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            code = inspect_db.main(argv)
        return code, out.getvalue(), err.getvalue()

    def test_held_archiver_lock_refuses_cleanly(self):
        with mock.patch.object(inspect_db, "archiver_lock_status",
                               return_value=1):
            code, _, err = self._run(
                ["--export-small-file-manifests", "--dry-run"])
        self.assertEqual(code, 1)
        self.assertIn("Refusing maintenance while the archiver lock is held",
                      err)
        self.assertNotIn("Traceback", err)
        self.assertNotIn("raise ", err)

    def test_running_archive_process_refuses_cleanly(self):
        with mock.patch.object(inspect_db, "archiver_lock_status",
                               return_value=0), \
             mock.patch.object(inspect_db, "active_archive_processes",
                               return_value=[{"pid": 1, "name": "robocopy"}]):
            code, _, err = self._run(
                ["--export-small-file-manifests", "--dry-run"])
        self.assertEqual(code, 1)
        self.assertIn("archive/transfer processes", err)
        self.assertNotIn("Traceback", err)

    def test_missing_required_flag_refuses_cleanly(self):
        with mock.patch.object(inspect_db, "_open_db",
                               return_value=mock.MagicMock()):
            code, _, err = self._run(["--backfill-directory-catalog"])
        self.assertEqual(code, 1)
        self.assertIn("--dry-run", err)
        self.assertNotIn("Traceback", err)

    def test_manifest_validate_without_heavy_refuses_cleanly(self):
        code, _, err = self._run(["--validate-local-manifest-export"])
        self.assertEqual(code, 1)
        self.assertIn("--heavy", err)
        self.assertNotIn("Traceback", err)

    def test_reconcile_requires_exactly_one_mode(self):
        for argv in (["--reconcile-stale-sessions"],
                     ["--reconcile-stale-sessions", "--dry-run", "--execute"]):
            with self.subTest(argv=argv):
                code, _, err = self._run(argv)
                self.assertEqual(code, 1)
                self.assertIn("exactly one of", err)
                self.assertNotIn("Traceback", err)

    def test_reconcile_execute_requires_yes(self):
        code, _, err = self._run(
            ["--reconcile-stale-sessions", "--execute"])
        self.assertEqual(code, 1)
        self.assertIn("--execute requires --yes", err)

    def test_unexpected_exception_is_not_swallowed(self):
        """A bug must keep its traceback; only refusals are caught."""
        with mock.patch.object(inspect_db, "archiver_lock_status",
                               side_effect=KeyError("renamed_column")):
            with self.assertRaises(KeyError):
                self._run(["--export-small-file-manifests", "--dry-run"])

    def test_plain_runtime_error_is_not_treated_as_a_refusal(self):
        with mock.patch.object(inspect_db, "archiver_lock_status",
                               side_effect=RuntimeError("internal invariant")):
            with self.assertRaises(RuntimeError) as caught:
                self._run(["--export-small-file-manifests", "--dry-run"])
        self.assertNotIsInstance(caught.exception, OperationalError)


def _connect(*args, **kwargs) -> Any:
    return cast(Any, psycopg).connect(*args, **kwargs)


def _pg_available():
    """Delegates to the fail-closed guard: an UNSAFE target raises here."""
    if psycopg is None:
        return False
    return pg_available()


@unittest.skipUnless(_pg_available(), SKIP_REASON)
class ReconcileIntegrationTests(unittest.TestCase):
    """End-to-end against a throwaway database — never the live catalog."""

    @classmethod
    def setUpClass(cls):
        from src.pg_db import PgDatabaseManager

        cls.dbname, cls.conninfo = create_test_database("lto_reconcile")
        cls.db = PgDatabaseManager(cls.conninfo)
        cls.db.apply_directory_catalog_schema()
        cls.db.register_tape("Tape_T")

    @classmethod
    def tearDownClass(cls):
        try:
            cls.db.close()
        except Exception:
            pass
        drop_test_database(cls.dbname)

    def setUp(self):
        self._exec("DELETE FROM remote_chunks")
        self._exec("DELETE FROM remote_sessions")
        # Both liveness probes are host/cluster-wide, not scoped to this test
        # database: active_archive_processes() sees every archiver process on
        # the machine, and archiver_lock_status() counts advisory locks across
        # the whole PostgreSQL cluster. On an operator box with a real archive
        # running, either would abort every test here. Tests that exercise the
        # guards re-patch these with live values.
        for target, value in (("active_archive_processes", []),
                              ("archiver_lock_status", 0)):
            quiet = mock.patch(f"src.session_reconcile.{target}",
                               return_value=value)
            quiet.start()
            self.addCleanup(quiet.stop)

    def _exec(self, sql, params=()):
        with _connect(self.conninfo, autocommit=True) as conn:
            conn.execute(sql, params)

    def _scalar(self, sql, params=()):
        with _connect(self.conninfo, autocommit=True) as conn:
            row = conn.execute(sql, params).fetchone()
        return None if row is None else row[0]

    def _make_session(self, session_id, *, chunks, chunk_count,
                      scan_complete=False, age_days=18, status="active"):
        stamp = datetime.now(timezone.utc) - timedelta(days=age_days)
        self._exec(
            # tape_generation is NOT NULL as of migration 013; a fixture that
            # omits it fails on any migrated database. Latent until the
            # isolated-PostgreSQL suite could actually be run.
            """INSERT INTO remote_sessions
               (session_id, session_label, remote_host, remote_user,
                remote_path, tape_label, staging_dir, chunk_count,
                created_at, status, scan_complete, tape_generation)
               VALUES (%s,%s,'h','u','/p','Tape_T','C:\\stg',%s,%s,%s,%s,1)""",
            (session_id, f"S{session_id}_{uuid.uuid4().hex[:6]}", chunk_count,
             stamp, status, scan_complete))
        index = 0
        for state, count in chunks.items():
            for _ in range(count):
                self._exec(
                    """INSERT INTO remote_chunks
                       (session_id, chunk_index, status, updated_at)
                       VALUES (%s,%s,%s,%s)""",
                    (session_id, index, state, stamp))
                index += 1

    def _status(self, session_id):
        return self._scalar(
            "SELECT status FROM remote_sessions WHERE session_id=%s",
            (session_id,))

    def _reconcile(self, **kwargs):
        return reconcile_stale_remote_sessions(self.conninfo, **kwargs)

    # -- the guard that protects a live run ---------------------------------

    def test_live_archiver_process_blocks_everything(self):
        self._make_session(36, chunks={"pending": 2}, chunk_count=2)
        with mock.patch("src.session_reconcile.active_archive_processes",
                        return_value=[{"pid": 999, "name": "robocopy.exe"}]):
            with self.assertRaises(OperationalError) as caught:
                self._reconcile(execute=True)
        self.assertIn("Refusing to reconcile", str(caught.exception))
        self.assertEqual(self._status(36), "active")

    def test_held_archiver_lock_blocks_everything(self):
        self._make_session(36, chunks={"pending": 2}, chunk_count=2)
        with mock.patch("src.session_reconcile.archiver_lock_status",
                        return_value=1):
            with self.assertRaises(OperationalError):
                self._reconcile(execute=True)
        self.assertEqual(self._status(36), "active")

    def test_forensics_reports_live_and_proposes_nothing(self):
        self._make_session(36, chunks={"pending": 2}, chunk_count=2)
        with mock.patch("src.session_reconcile.archiver_lock_status",
                        return_value=1):
            report = session_forensics(self.conninfo)
        entry = report["sessions"][0]
        self.assertEqual(entry["verdict"], "live")
        self.assertIsNone(entry["proposed_status"])

    # -- classification applied for real ------------------------------------

    def test_dry_run_plans_but_changes_nothing(self):
        self._make_session(36, chunks={"done": 1, "fetch_failed": 1,
                                       "pending": 9}, chunk_count=11)
        result = self._reconcile(execute=False)
        self.assertEqual(len(result["planned"]), 1)
        self.assertEqual(result["planned"][0]["proposed_status"],
                         ABANDONED_STATUS)
        self.assertEqual(result["changed"], [])
        self.assertEqual(self._status(36), "active")

    def test_execute_marks_the_stale_session_abandoned(self):
        self._make_session(36, chunks={"done": 1, "fetch_failed": 1,
                                       "pending": 9}, chunk_count=11)
        result = self._reconcile(execute=True)
        self.assertEqual(self._status(36), ABANDONED_STATUS)
        self.assertEqual(result["changed"][0]["rows_updated"], 1)
        self.assertIsNotNone(self._scalar(
            "SELECT completed_at FROM remote_sessions WHERE session_id=36"))

    def test_finished_session_becomes_completed_not_abandoned(self):
        self._make_session(40, chunks={"done": 4}, chunk_count=4,
                           scan_complete=True)
        self._reconcile(execute=True)
        self.assertEqual(self._status(40), COMPLETED_STATUS)

    def test_reconciliation_is_idempotent(self):
        self._make_session(36, chunks={"pending": 3}, chunk_count=3)
        first = self._reconcile(execute=True)
        second = self._reconcile(execute=True)
        self.assertEqual(first["changed"][0]["rows_updated"], 1)
        self.assertEqual(second["planned"], [])
        self.assertEqual(second["changed"], [])
        self.assertEqual(self._status(36), ABANDONED_STATUS)

    def test_recent_session_is_left_active(self):
        """A session that was working an hour ago is never reclassified."""
        self._make_session(37, chunks={"done": 1, "pending": 5},
                           chunk_count=6, age_days=0)
        result = self._reconcile(execute=True)
        self.assertEqual(result["planned"], [])
        self.assertEqual(result["left_alone"][0]["verdict"], VERDICT_AMBIGUOUS)
        self.assertEqual(self._status(37), "active")

    def test_transient_chunk_is_left_active(self):
        self._make_session(38, chunks={"backing": 1, "pending": 2},
                           chunk_count=3)
        result = self._reconcile(execute=True)
        self.assertEqual(result["planned"], [])
        self.assertEqual(self._status(38), "active")

    def test_stale_and_live_sessions_are_handled_independently(self):
        """The stale row reconciles; the busy one keeps its active status."""
        self._make_session(36, chunks={"pending": 9}, chunk_count=9)
        self._make_session(37, chunks={"done": 2, "pending": 4},
                           chunk_count=6, age_days=0)
        self._reconcile(execute=True)
        self.assertEqual(self._status(36), ABANDONED_STATUS)
        self.assertEqual(self._status(37), "active")

    def test_session_ids_filter_limits_the_blast_radius(self):
        self._make_session(36, chunks={"pending": 2}, chunk_count=2)
        self._make_session(41, chunks={"pending": 2}, chunk_count=2)
        self._reconcile(execute=True, session_ids=[36])
        self.assertEqual(self._status(36), ABANDONED_STATUS)
        self.assertEqual(self._status(41), "active")

    def test_unknown_session_id_is_refused(self):
        self._make_session(36, chunks={"pending": 2}, chunk_count=2)
        with self.assertRaises(OperationalError):
            self._reconcile(execute=True, session_ids=[999])
        self.assertEqual(self._status(36), "active")

    # -- the cleanup guard itself -------------------------------------------

    def test_cleanup_refuses_while_any_session_row_is_active(self):
        self._make_session(36, chunks={"pending": 2}, chunk_count=2)
        with self.assertRaises(OperationalError) as caught:
            self.db.cleanup_unreferenced_remote_data()
        self.assertIn("Refusing cleanup", str(caught.exception))

    def test_cleanup_unblocks_only_after_reconciliation(self):
        """Fail-safe end to end: blocked, then legitimately cleared."""
        self._make_session(36, chunks={"pending": 2}, chunk_count=2)
        with self.assertRaises(OperationalError):
            self.db.cleanup_unreferenced_remote_data()
        self._reconcile(execute=True)
        self.assertEqual(
            self.db.get_unreferenced_remote_data_summary()["active_sessions"],
            0)
        self.db.cleanup_unreferenced_remote_data()


#: The correlated-subquery form ``InspectorRepository.list_sessions`` used
#: before the per-plan pre-aggregation. Kept verbatim as the oracle: the new
#: query is only allowed to be faster, never to answer differently.
LEGACY_REMOTE_SESSION_SQL = """
    SELECT 'remote' AS kind,s.session_id,s.session_label,s.status,
           '' AS mode,s.created_at,s.completed_at,s.chunk_count AS chunks,
           COALESCE((SELECT COUNT(*) FROM remote_plan_files pf
                     WHERE pf.plan_id=s.plan_id),0) AS manifest_rows,
           COALESCE((SELECT SUM(sf.file_size_bytes)
                     FROM remote_plan_files pf
                     JOIN remote_snapshot_files sf
                       ON sf.snapshot_file_id=pf.snapshot_file_id
                     WHERE pf.plan_id=s.plan_id),0) AS manifest_bytes,
           0 AS file_records
    FROM remote_sessions s ORDER BY s.session_id"""


@unittest.skipUnless(_pg_available(), SKIP_REASON)
class SessionQueryEquivalenceTests(unittest.TestCase):
    """The optimized Sessions query must agree with the one it replaced."""

    @classmethod
    def setUpClass(cls):
        from src.pg_db import PgDatabaseManager

        cls.dbname, cls.conninfo = create_test_database("lto_sessq")
        cls.db = PgDatabaseManager(cls.conninfo)
        cls.db.apply_directory_catalog_schema()
        cls.db.register_tape("Tape_Q")
        cls._build_fixture()

    @classmethod
    def tearDownClass(cls):
        try:
            cls.db.close()
        except Exception:
            pass
        drop_test_database(cls.dbname)

    @classmethod
    def _build_fixture(cls):
        """Every shape that could make the two forms diverge."""
        now = datetime.now(timezone.utc)
        with _connect(cls.conninfo, autocommit=True) as conn:
            conn.execute(
                """INSERT INTO remote_snapshots
                   (snapshot_id, remote_host, remote_path, fingerprint,
                    total_files, total_bytes, created_at)
                   VALUES (1,'h','/p','\\x01',3,600,%s)""", (now,))
            for file_id, size in ((1, 100), (2, 200), (3, 300)):
                conn.execute(
                    """INSERT INTO remote_snapshot_files
                       (snapshot_file_id, snapshot_id, remote_path,
                        file_size_bytes) VALUES (%s,1,%s,%s)""",
                    (file_id, f"/p/f{file_id}", size))
            # plan 1: three files. plan 2: a plan with no files at all.
            for plan_id, fingerprint in ((1, b"\x11"), (2, b"\x22")):
                conn.execute(
                    """INSERT INTO remote_plans
                       (plan_id, snapshot_id, fingerprint, chunk_count,
                        created_at) VALUES (%s,1,%s,1,%s)""",
                    (plan_id, fingerprint, now))
            for ordinal, file_id in enumerate((1, 2, 3)):
                conn.execute(
                    """INSERT INTO remote_plan_files
                       (plan_id, snapshot_file_id, chunk_index, ordinal)
                       VALUES (1,%s,0,%s)""", (file_id, ordinal))
            sessions = [
                (1, 1),    # ordinary session with a populated plan
                (2, 1),    # a second session sharing the same plan
                (3, 2),    # a plan that has no plan files
                (4, None),  # a session with no plan at all
            ]
            for session_id, plan_id in sessions:
                conn.execute(
                    """INSERT INTO remote_sessions
                       (session_id, session_label, remote_host, remote_user,
                        remote_path, tape_label, staging_dir, chunk_count,
                        plan_id, created_at, status, tape_generation)
                       VALUES (%s,%s,'h','u','/p','Tape_Q','C:\\stg',1,%s,%s,
                               'completed',1)""",
                    (session_id, f"S{session_id}", plan_id, now))

    def _legacy_rows(self):
        with _connect(self.conninfo, autocommit=True,
                      row_factory=cast(Any, dict_row)) as conn:
            return [dict(r) for r in
                    conn.execute(LEGACY_REMOTE_SESSION_SQL).fetchall()]

    def _new_rows(self):
        from src.inspector_repository import InspectorRepository

        with InspectorRepository(self.conninfo) as repo:
            return [r for r in repo.list_sessions() if r["kind"] == "remote"]

    def test_new_query_matches_the_correlated_form_exactly(self):
        legacy = self._legacy_rows()
        new = self._new_rows()
        self.assertEqual(len(new), 4)
        self.assertEqual(
            [(r["session_id"], int(r["manifest_rows"]),
              int(r["manifest_bytes"])) for r in new],
            [(r["session_id"], int(r["manifest_rows"]),
              int(r["manifest_bytes"])) for r in legacy])

    def test_totals_are_the_expected_values(self):
        by_id = {r["session_id"]: r for r in self._new_rows()}
        self.assertEqual((int(by_id[1]["manifest_rows"]),
                          int(by_id[1]["manifest_bytes"])), (3, 600))
        # a shared plan must not double-count, and must not drop the session
        self.assertEqual((int(by_id[2]["manifest_rows"]),
                          int(by_id[2]["manifest_bytes"])), (3, 600))
        self.assertEqual((int(by_id[3]["manifest_rows"]),
                          int(by_id[3]["manifest_bytes"])), (0, 0))
        self.assertEqual((int(by_id[4]["manifest_rows"]),
                          int(by_id[4]["manifest_bytes"])), (0, 0))

    def test_left_join_never_multiplies_session_rows(self):
        ids = [r["session_id"] for r in self._new_rows()]
        self.assertEqual(sorted(ids), sorted(set(ids)))


if __name__ == "__main__":
    unittest.main()
