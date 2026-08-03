"""Plan 1 / Task 4.1 — the read-only session frontier/membership report.

Its purpose is to replace *written notes about a session* with a
*measurement of the session*. The written baseline for session 37 in AGENTS.md
is a hypothesis from a point in time; this report is what an operator should
trust instead.

That only works if two things are true, and both are tested here:

* it **hardcodes nothing** — no session id, no chunk number, no expected
  status. A report that already knows the answer cannot notice that the answer
  changed.
* it is **read-only and fails towards ``blocked``**. Missing evidence, an
  unreadable table, an unknown lock state — each produces "blocked", never a
  guessed boundary.
"""
import inspect
import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

from src.startup_reconcile import (VERDICT_BLOCKED, VERDICT_READY,
                                   session_frontier_report)

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class ReportDB:
    """A session the report can measure. Every answer is settable."""

    def __init__(self, *, scan_complete=True, chunk_states=None,
                 membership=None, shared=(), frontier=False, scopes=(),
                 failures=()):
        self.scan_complete = scan_complete
        self._chunk_states = chunk_states or [
            {"status": "done", "n": 108, "max_index": 107}]
        self._membership = membership or {
            "member_rows": 1000, "distinct_chunks": 108,
            "max_chunk_index": 107, "member_bytes": 12345}
        self._shared = list(shared)
        self._frontier = frontier
        self._scopes = list(scopes)
        self._failures = set(failures)

    def _maybe_fail(self, name):
        if name in self._failures:
            raise RuntimeError(f"{name} is unreadable")

    def get_remote_session(self, session_id):
        self._maybe_fail("session")
        return {"session_id": session_id, "session_label": "REMOTE_x",
                "status": "active", "scan_complete": self.scan_complete,
                "tape_label": "Tape_03", "plan_id": 5}

    def get_session_membership_summary(self, session_id):
        self._maybe_fail("membership")
        return dict(self._membership)

    def get_chunk_state_counts(self, session_id):
        self._maybe_fail("chunk_states")
        return [dict(row) for row in self._chunk_states]

    def find_sessions_sharing_plan(self, session_id):
        self._maybe_fail("shared")
        return list(self._shared)

    def session_has_frontier_state(self, session_id):
        self._maybe_fail("frontier")
        return self._frontier

    def get_scan_scopes(self, session_id):
        return list(self._scopes)


def _report(db, **kwargs):
    kwargs.setdefault("lock_holders", [])
    kwargs.setdefault("active_processes", [])
    return session_frontier_report(db, 37, **kwargs)


# =============================================================================
# A. It measures rather than assumes
# =============================================================================
class NoHardcodedFactsTests(unittest.TestCase):
    def test_the_report_hardcodes_no_session_or_chunk_number(self):
        source = inspect.getsource(session_frontier_report)
        for hardcoded in ("37", "112", "113", "Tape_02", "Tape_03",
                          "session 37"):
            self.assertNotIn(hardcoded, source, hardcoded)

    def test_it_reports_whatever_the_catalog_says(self):
        db = ReportDB(chunk_states=[{"status": "done", "n": 5, "max_index": 4}],
                      membership={"member_rows": 12, "distinct_chunks": 5,
                                  "max_chunk_index": 4, "member_bytes": 99})
        report = _report(db)
        self.assertEqual(report["max_chunk_index"], 4)
        self.assertEqual(report["membership"]["member_rows"], 12)
        self.assertEqual(report["membership"]["member_bytes"], 99)

    def test_a_different_catalog_gives_a_different_answer(self):
        first = _report(ReportDB(
            membership={"member_rows": 1, "distinct_chunks": 1,
                        "max_chunk_index": 0, "member_bytes": 1}))
        second = _report(ReportDB(
            membership={"member_rows": 9, "distinct_chunks": 3,
                        "max_chunk_index": 2, "member_bytes": 90}))
        self.assertNotEqual(first["max_chunk_index"],
                            second["max_chunk_index"])


# =============================================================================
# B. Ready vs blocked
# =============================================================================
class VerdictTests(unittest.TestCase):
    def test_a_quiescent_complete_session_is_ready(self):
        report = _report(ReportDB())
        self.assertEqual(report["verdict"], VERDICT_READY)
        self.assertEqual(report["blocking"], [])

    def test_an_incomplete_scan_blocks(self):
        report = _report(ReportDB(scan_complete=False))
        self.assertEqual(report["verdict"], VERDICT_BLOCKED)
        self.assertTrue(any("scan never completed" in b
                            for b in report["blocking"]))

    def test_a_backing_chunk_blocks_and_says_why(self):
        report = _report(ReportDB(chunk_states=[
            {"status": "done", "n": 108, "max_index": 107},
            {"status": "backing", "n": 1, "max_index": 108}]))
        self.assertEqual(report["verdict"], VERDICT_BLOCKED)
        self.assertEqual(report["transient_chunks"]["backing"], 1)
        self.assertTrue(any("cannot be known from the catalog alone" in b
                            for b in report["blocking"]))

    def test_mid_flight_chunks_block(self):
        for state in ("fetching", "packing"):
            report = _report(ReportDB(chunk_states=[
                {"status": state, "n": 2, "max_index": 3}]))
            self.assertEqual(report["verdict"], VERDICT_BLOCKED, state)
            self.assertTrue(any("mid-flight" in b for b in report["blocking"]))

    def test_a_shared_plan_blocks(self):
        report = _report(ReportDB(shared=[{"session_id": 38,
                                           "session_label": "other"}]))
        self.assertEqual(report["verdict"], VERDICT_BLOCKED)
        self.assertTrue(any("share this plan" in b
                            for b in report["blocking"]))
        self.assertEqual(len(report["shared_plan_sessions"]), 1)

    def test_a_held_archiver_lock_blocks(self):
        report = _report(ReportDB(), lock_holders=[{"pid": 4242}])
        self.assertEqual(report["verdict"], VERDICT_BLOCKED)
        self.assertTrue(any("advisory lock" in b for b in report["blocking"]))

    def test_running_archive_processes_block(self):
        report = _report(ReportDB(), active_processes=["Robocopy.exe"])
        self.assertEqual(report["verdict"], VERDICT_BLOCKED)

    def test_unknown_liveness_blocks_rather_than_assuming_quiet(self):
        for kwargs in ({"lock_holders": None}, {"active_processes": None}):
            report = _report(ReportDB(), **kwargs)
            self.assertEqual(report["verdict"], VERDICT_BLOCKED, str(kwargs))
            self.assertTrue(any("liveness was not established" in b
                                for b in report["blocking"]))


# =============================================================================
# C. Unreadable evidence never becomes a guess
# =============================================================================
class UnreadableEvidenceTests(unittest.TestCase):
    def test_an_unreadable_session_row_stops_the_report(self):
        report = _report(ReportDB(failures={"session"}))
        self.assertEqual(report["verdict"], VERDICT_BLOCKED)
        self.assertTrue(report["errors"])

    def test_each_unreadable_table_is_reported_and_blocks(self):
        for failure in ("membership", "chunk_states", "shared", "frontier"):
            report = _report(ReportDB(failures={failure}))
            self.assertEqual(report["verdict"], VERDICT_BLOCKED, failure)
            self.assertTrue(any(failure.replace("_", " ") in e.lower()
                                or failure in e
                                for e in report["errors"]), failure)

    def test_an_unreadable_artifact_root_blocks(self):
        with mock.patch("src.archive_artifacts.find_orphan_parts",
                        side_effect=OSError("permission denied")):
            report = _report(ReportDB(), archive_root="/nowhere")
        self.assertEqual(report["verdict"], VERDICT_BLOCKED)
        self.assertTrue(any("could not be inspected" in b
                            for b in report["blocking"]))


# =============================================================================
# D. Frontier and artifact evidence
# =============================================================================
class FrontierEvidenceTests(unittest.TestCase):
    def test_a_session_with_no_frontier_state_says_so(self):
        report = _report(ReportDB(frontier=False))
        self.assertFalse(report["frontier"]["bound"])
        self.assertEqual(report["frontier"]["scopes"], [])

    def test_a_frontier_bound_session_lists_its_scopes(self):
        report = _report(ReportDB(
            frontier=True,
            scopes=[{"scan_scope_id": 1, "source_root": "/vault/a",
                     "coverage_state": "provisional"}]))
        self.assertTrue(report["frontier"]["bound"])
        self.assertEqual(report["frontier"]["scopes"][0]["source_root"],
                         "/vault/a")

    def test_an_orphan_part_blocks_and_is_listed(self):
        from src.archive_artifacts import (JsonlZstArtifactWriter,
                                           segment_locator)
        with tempfile.TemporaryDirectory() as root:
            writer = JsonlZstArtifactWriter(root, segment_locator(37, 1, 0))
            writer.open()
            writer.add(path="/vault/a/f", size=1, ordinal=0)
            writer.close(publish=False)

            report = _report(ReportDB(), archive_root=root)
        self.assertEqual(report["verdict"], VERDICT_BLOCKED)
        self.assertEqual(len(report["artifacts"]["orphan_parts"]), 1)
        self.assertTrue(any("interrupted scan artifact" in b
                            for b in report["blocking"]))


# =============================================================================
# E. Read-only, and no LTFS
# =============================================================================
class ReadOnlyTests(unittest.TestCase):
    def test_the_report_calls_no_mutating_repository_method(self):
        calls = []

        class Recording(ReportDB):
            def __getattr__(self, name):
                calls.append(name)
                raise AttributeError(name)

        _report(Recording())
        for name in calls:
            for mutation in ("update", "insert", "delete", "create", "seal",
                             "claim", "reclaim", "transition", "apply"):
                self.assertNotIn(mutation, name.lower(), name)

    def test_it_never_touches_ltfs(self):
        source = inspect.getsource(session_frontier_report)
        for token in ("lto_drive", "get_volume_label", "_acquire_tape_io_lock",
                      "_ensure_lto_drive_ready", "LtfsCmd", "eject"):
            self.assertNotIn(token, source, token)

    def test_the_repository_queries_are_read_only(self):
        from src.pg_sessions import PgSessionMixin
        for name in ("get_session_membership_summary", "get_chunk_state_counts",
                     "find_sessions_sharing_plan"):
            source = inspect.getsource(getattr(PgSessionMixin, name))
            self.assertIn("_run_read", source, name)
            for mutation in ("UPDATE ", "INSERT ", "DELETE "):
                self.assertNotIn(mutation, source, f"{name}: {mutation}")

    def test_membership_is_read_from_plan_rows_not_cached_totals(self):
        """The cached session totals are what an interrupted run leaves stale."""
        from src.pg_sessions import PgSessionMixin
        source = inspect.getsource(
            PgSessionMixin.get_session_membership_summary)
        self.assertIn("remote_plan_files", source)
        self.assertIn("remote_snapshot_files", source)
        self.assertNotIn("s.total_files", source)
        self.assertNotIn("s.chunk_count", source)


# =============================================================================
# F. The CLI command
# =============================================================================
class ReportCommandTests(unittest.TestCase):
    def test_the_command_requires_a_session_id(self):
        import inspect_db
        parser = mock.MagicMock()
        parser.error.side_effect = SystemExit(2)
        args = SimpleNamespace(session_id=None)
        with self.assertRaises(SystemExit):
            inspect_db._run_session_frontier_report(
                SimpleNamespace(pg_dbname="db"), args, parser)

    def test_unknown_liveness_is_passed_through_as_none(self):
        import inspect_db
        db = ReportDB()
        cfg = SimpleNamespace(pg_dbname="db",
                              local_manifest_archive_root=None)
        with mock.patch.object(inspect_db, "_open_db", return_value=db), \
                mock.patch.object(inspect_db, "_conninfo", return_value=""), \
                mock.patch.object(inspect_db, "archiver_lock_status",
                                  side_effect=RuntimeError("no server")), \
                mock.patch.object(inspect_db, "active_archive_processes",
                                  side_effect=RuntimeError("no psutil")), \
                mock.patch.object(inspect_db, "_print_json") as printed:
            db.close = lambda: None
            rc = inspect_db._run_session_frontier_report(
                cfg, SimpleNamespace(session_id=[37]), mock.MagicMock())
        self.assertEqual(rc, 0)
        report = printed.call_args.args[0]["reports"][0]
        self.assertEqual(report["verdict"], VERDICT_BLOCKED)
        self.assertTrue(any("liveness was not established" in b
                            for b in report["blocking"]))


if __name__ == "__main__":
    unittest.main()
