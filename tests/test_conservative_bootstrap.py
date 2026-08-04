"""The conservative frontier bootstrap for an INCOMPLETE historical scan.

Plan 1, Task 4.2, completed. The bootstrap previously refused any session whose
scan had not finished — which is every session it was written for. Session 37's
scan died on an SSH reset, so ``scan_complete=false`` is its defining property,
not a disqualification.

What these tests pin down is the difference between *unfinished* and *unsafe*:

* unfinished (``scan_complete=false``) is the expected input — allowed;
* unknown (``backing``, mid-flight chunks, live workers, held locks) is unsafe —
  still refused, because nothing automated may act on a tape write whose
  outcome the catalog cannot describe (incident 010).

And what the conservative mode may write: scope rows, one pending root per
directory scope, and a bootstrap record. Nothing else — no traversal, no
completion, no membership.
"""
import unittest
from unittest import mock

from src.frontier_bootstrap import (STATE_COMPLETED, STATE_RUNNING,
                                    BootstrapRefused, FrontierBootstrap)


def _report(**overrides):
    base = {
        "errors": [],
        "transient_chunks": {},
        "shared_plan_sessions": [],
        "liveness": {"lock_holders": 0, "active_processes": []},
        "artifacts": {"orphan_parts": []},
        "scan_complete": False,
        "blocking": ["the scan never completed, so the plan's full membership "
                     "is unknown"],
    }
    base.update(overrides)
    return base


class AnIncompleteScanIsTheExpectedInputTests(unittest.TestCase):
    def test_an_unfinished_scan_alone_does_not_block(self):
        self.assertEqual(FrontierBootstrap._bootstrap_blockers(_report()), [])

    def test_the_generic_report_still_blocks_on_it(self):
        """The two questions stay different. The read-only session report must
        keep calling an unfinished session unfinished; only the bootstrap gate
        is allowed to accept it."""
        from src.startup_reconcile import session_frontier_report
        db = mock.Mock()
        db.get_remote_session.return_value = {
            "session_id": 1, "scan_complete": False}
        db.get_session_membership_summary.return_value = {}
        db.get_chunk_state_counts.return_value = []
        db.find_sessions_sharing_plan.return_value = []
        db.session_has_frontier_state.return_value = False
        db.get_scan_scopes.return_value = []
        report = session_frontier_report(db, 1, lock_holders=[],
                                         active_processes=[])
        self.assertEqual(report["verdict"], "blocked")
        self.assertTrue(any("scan never completed" in reason
                            for reason in report["blocking"]))


class UnsafeStateStillBlocksTests(unittest.TestCase):
    def _blocked_by(self, **overrides):
        blockers = FrontierBootstrap._bootstrap_blockers(_report(**overrides))
        self.assertTrue(blockers, "expected this state to block")
        return " ".join(blockers)

    def test_a_backing_chunk_blocks(self):
        self.assertIn("backing",
                      self._blocked_by(transient_chunks={"backing": 1}))

    def test_a_mid_flight_chunk_blocks(self):
        self.assertIn("mid-flight",
                      self._blocked_by(transient_chunks={"fetching": 2}))

    def test_a_shared_plan_blocks(self):
        self.assertIn("shares its plan",
                      self._blocked_by(shared_plan_sessions=[36]))

    def test_a_held_archiver_lock_blocks(self):
        self.assertIn("lock", self._blocked_by(
            liveness={"lock_holders": [{"pid": 9}], "active_processes": []}))

    def test_a_running_archive_process_blocks(self):
        self.assertIn("processes are running", self._blocked_by(
            liveness={"lock_holders": 0,
                      "active_processes": [{"pid": 9, "name": "python"}]}))

    def test_an_orphan_part_artifact_blocks(self):
        self.assertIn("orphaned", self._blocked_by(
            artifacts={"orphan_parts": ["/root/x.part"]}))

    def test_an_unreadable_session_blocks(self):
        self.assertIn("could not be read",
                      self._blocked_by(errors=["chunk states: boom"]))

    def test_a_missing_report_blocks(self):
        self.assertTrue(FrontierBootstrap._bootstrap_blockers(None))

    def test_gates_are_read_as_facts_not_prose(self):
        """Rewording the report's messages must not disable a gate."""
        reworded = _report(transient_chunks={"backing": 1},
                           blocking=["completely different wording"])
        self.assertTrue(FrontierBootstrap._bootstrap_blockers(reworded))


class LivenessEvidenceIsMeasuredTests(unittest.TestCase):
    """The gate is only as good as what feeds it.

    ``_session_report`` used to pass ``lock_holders=[]`` and
    ``active_processes=[]`` — hard-coded emptiness that told the report nothing
    was running without looking, making every liveness gate vacuous.
    """

    def _bootstrap(self, **kwargs):
        return FrontierBootstrap(
            db=mock.Mock(), session_id=1, scan_paths=["/s"],
            archive_root="/root", scanner_factory=lambda m: mock.Mock(),
            stop_event=mock.Mock(), **kwargs)

    def test_the_probes_are_actually_called(self):
        procs = mock.Mock(return_value=[{"pid": 7, "name": "python"}])
        locks = mock.Mock(return_value=[{"pid": 8}])
        boot = self._bootstrap(active_processes_probe=procs,
                               lock_holders_probe=locks)
        with mock.patch("src.startup_reconcile.session_frontier_report") as rep:
            boot._session_report()
        procs.assert_called_once()
        locks.assert_called_once()
        self.assertEqual(rep.call_args.kwargs["active_processes"],
                         [{"pid": 7, "name": "python"}])
        self.assertEqual(rep.call_args.kwargs["lock_holders"], [{"pid": 8}])

    def test_a_failed_probe_blocks_rather_than_reading_as_quiet(self):
        boot = self._bootstrap(
            active_processes_probe=mock.Mock(side_effect=OSError("nope")),
            lock_holders_probe=mock.Mock(side_effect=OSError("nope")))
        self.assertTrue(boot._active_processes())
        self.assertTrue(boot._lock_holders())

    def test_a_missing_lock_probe_is_unknown_not_empty(self):
        boot = self._bootstrap(lock_holders_probe=None)
        holders = boot._lock_holders()
        self.assertTrue(holders, "unknown must not read as 'nobody'")
        self.assertIn("probe_unavailable", holders[0])


class ConservativeExecutionWritesTheMinimumTests(unittest.TestCase):
    def _bootstrap(self, db):
        return FrontierBootstrap(
            db=db, session_id=37, scan_paths=["/s/a", "/s/b"],
            archive_root="/root", scanner_factory=lambda m: mock.Mock(),
            stop_event=mock.Mock(),
            active_processes_probe=lambda: [],
            lock_holders_probe=lambda: [])

    @staticmethod
    def _quiet_db():
        db = mock.Mock()
        db.get_scan_scopes.return_value = []
        db.get_frontier_bootstrap.return_value = None
        db.session_has_frontier_state.return_value = False
        db.start_frontier_bootstrap.return_value = {"bootstrap_id": 5}
        db.get_remote_session.return_value = {
            "session_id": 37, "scan_complete": False}
        db.get_session_membership_summary.return_value = {}
        db.get_chunk_state_counts.return_value = []
        db.find_sessions_sharing_plan.return_value = []
        return db

    def test_it_traverses_nothing_and_completes_nothing(self):
        db = self._quiet_db()
        boot = self._bootstrap(db)
        result = boot.execute(approved=True, conservative=True)

        self.assertEqual(result["mode"], "conservative")
        self.assertEqual(result["directories_listed"], 0)
        self.assertEqual(result["segments_published"], 0)
        self.assertFalse(result["coverage_final"])
        self.assertFalse(result["scan_marked_complete"])
        # Never claims a directory, never lists, never marks the scan done.
        db.claim_next_directory.assert_not_called()
        db.mark_remote_scan_complete.assert_not_called()
        db.finalize_scan_scope.assert_not_called()
        db.finalize_directory_subtree.assert_not_called()

    def test_it_touches_no_existing_chunk_or_membership(self):
        db = self._quiet_db()
        self._bootstrap(db).execute(approved=True, conservative=True)
        for forbidden in ("append_remote_streaming_chunk", "seal_remote_chunk",
                          "transition_chunk", "consume_segment_range",
                          "import_legacy_scan_segment"):
            getattr(db, forbidden).assert_not_called()

    def test_it_records_running_not_completed(self):
        """Coverage is not final and the source was never walked. Recording it
        completed would be the same lie as inferring coverage from rows."""
        db = self._quiet_db()
        self._bootstrap(db).execute(approved=True, conservative=True)
        states = [c.kwargs.get("state")
                  for c in db.update_frontier_bootstrap.call_args_list]
        self.assertIn(STATE_RUNNING, states)
        self.assertNotIn(STATE_COMPLETED, states)

    def test_it_requires_explicit_approval(self):
        db = self._quiet_db()
        with self.assertRaises(BootstrapRefused):
            self._bootstrap(db).execute(approved=False, conservative=True)
        db.start_frontier_bootstrap.assert_not_called()

    def test_it_refuses_when_a_gate_fails(self):
        db = self._quiet_db()
        db.get_chunk_state_counts.return_value = [
            {"status": "backing", "n": 1}]
        with self.assertRaises(BootstrapRefused) as caught:
            self._bootstrap(db).execute(approved=True, conservative=True)
        self.assertIn("backing", str(caught.exception))
        db.start_frontier_bootstrap.assert_not_called()

    def test_a_repeated_bootstrap_after_completion_is_refused(self):
        db = self._quiet_db()
        db.get_frontier_bootstrap.return_value = {"state": STATE_COMPLETED}
        with self.assertRaises(BootstrapRefused) as caught:
            self._bootstrap(db).execute(approved=True, conservative=True)
        self.assertIn("already been bootstrapped", str(caught.exception))

    def test_repeating_it_writes_nothing_new(self):
        """Idempotent by construction: scopes are created only when none are
        persisted, so a second conservative run over an unchanged config adds
        no scope and no root."""
        db = self._quiet_db()
        db.get_scan_scopes.return_value = [
            {"scan_scope_id": 1, "source_root": "/s/a",
             "scope_kind": "directory"},
            {"scan_scope_id": 2, "source_root": "/s/b",
             "scope_kind": "directory"}]
        self._bootstrap(db).execute(approved=True, conservative=True)
        db.create_scan_scopes.assert_not_called()
        db.enqueue_scan_directories.assert_not_called()


if __name__ == "__main__":
    unittest.main()
