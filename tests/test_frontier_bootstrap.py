"""Plan 1 / Task 4.2 — the one-time frontier bootstrap.

The acceptance gate is a shadow rehearsal proving three things, and each is a
distinct way the migration could corrupt a live session:

* **no existing member is duplicated** — a path already in the session's plan
  (same path, same size) is recorded as covered and never appended again;
* **no newly discovered path is skipped** — anything the source has that the
  plan does not becomes available for future chunks;
* **no directory becomes final without traversal evidence** — existing catalog
  rows are NOT coverage. A directory is complete only after it was listed.

Plus the refusal that matters most: a same-path/different-size observation is
``source_changed``. The old membership may already be on tape, so it is
retained, recorded unresolved, and nothing is replanned automatically.

Everything is a fake source and an in-memory frontier database. No SSH, no
PostgreSQL, no LTFS.
"""
import copy
import os
import tempfile
import threading
import unittest
from unittest import mock

from src.frontier_bootstrap import (STATE_COMPLETED, STATE_FAILED,
                                    BootstrapRefused, FrontierBootstrap)

from test_incremental_scan_frontier import FakeSource, FrontierDB


class BootstrapDB(FrontierDB):
    """FrontierDB plus the legacy snapshot and bootstrap-run behaviour."""

    def __init__(self, snapshot=None, chunk_states=None, shared=()):
        super().__init__()
        #: The legacy session's existing membership: {path: size}.
        self.snapshot = dict(snapshot or {})
        self.bootstraps = {}
        self.import_calls = []
        self._chunk_states = chunk_states or [
            {"status": "done", "n": 108, "max_index": 107}]
        self._shared = list(shared)

    # -- what the pre-flight session report reads -------------------------
    def get_remote_session(self, session_id):
        return {"session_id": session_id, "session_label": "REMOTE_legacy",
                "status": "active", "scan_complete": True,
                "tape_label": "Tape_03", "plan_id": 5}

    def get_session_membership_summary(self, session_id):
        return {"member_rows": len(self.snapshot),
                "distinct_chunks": 1,
                "max_chunk_index": 0,
                "member_bytes": sum(self.snapshot.values())}

    def get_chunk_state_counts(self, session_id):
        return [dict(row) for row in self._chunk_states]

    def find_sessions_sharing_plan(self, session_id):
        return list(self._shared)

    # -- legacy reconciliation -------------------------------------------
    def import_legacy_scan_segment(self, session_id, segment_id, entries):
        self.import_calls.append(segment_id)
        segment = next(s for s in self.segments
                       if s["scan_segment_id"] == segment_id)
        if segment.get("legacy_import_state", "not_imported") != "not_imported":
            return {"covered": [], "new": [], "source_changed": [],
                    "already_imported": True}
        covered, fresh, changed = [], [], []
        for path, size in entries:
            known = self.snapshot.get(path)
            if known is None:
                fresh.append((path, size))
            elif known == size:
                covered.append((path, size))
            else:
                changed.append((path, known, size))
        for path, planned, observed in changed:
            self.errors.append({
                "scan_directory_id": segment["scan_directory_id"],
                "category": "source_changed", "path": path,
                "disposition": "unresolved",
                "message": f"planned {planned}, source now {observed}"})
        segment["legacy_import_state"] = "blocked" if changed else "imported"
        return {"covered": covered, "new": fresh, "source_changed": changed,
                "already_imported": False}

    def get_ready_segments(self, session_id, limit=50):
        return list(self.segments[:limit])

    def session_has_frontier_state(self, session_id):
        return bool(self.scopes)

    # -- bootstrap runs ---------------------------------------------------
    def start_frontier_bootstrap(self, session_id, source_host=None):
        open_run = next(
            (r for r in self.bootstraps.values()
             if r["session_id"] == session_id
             and r["state"] in ("planned", "running")), None)
        if open_run is not None:
            open_run["state"] = "running"
            return dict(open_run)
        run = {"bootstrap_id": f"b{len(self.bootstraps) + 1}",
               "session_id": session_id, "source_host": source_host,
               "state": "running", "coverage_final": False}
        self.bootstraps[run["bootstrap_id"]] = run
        return dict(run)

    def update_frontier_bootstrap(self, bootstrap_id, **fields):
        self.bootstraps[bootstrap_id].update(fields)
        return True

    def get_frontier_bootstrap(self, session_id):
        runs = [r for r in self.bootstraps.values()
                if r["session_id"] == session_id]
        return dict(runs[-1]) if runs else None


TREE = {
    "/vault/a": {"files": {"old.bin": 10, "fresh.bin": 20}, "dirs": ["sub"]},
    "/vault/a/sub": {"files": {"deep.bin": 30}, "dirs": []},
}


class _Bootstrap(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.root = self._dir.name
        self.addCleanup(self._dir.cleanup)
        self.stop = threading.Event()

    def _bootstrap(self, db, source, scan_paths=("/vault/a",), **kwargs):
        return FrontierBootstrap(
            db=db, session_id=37, scan_paths=list(scan_paths),
            archive_root=self.root,
            scanner_factory=lambda metrics: source,
            stop_event=self.stop, source_host="srv02", **kwargs)


# =============================================================================
# A. The dry run changes nothing
# =============================================================================
class DryRunTests(_Bootstrap):
    def test_a_clean_session_would_proceed(self):
        db = BootstrapDB()
        report = self._bootstrap(db, FakeSource(TREE)).dry_run()
        self.assertTrue(report["would_proceed"], report["blocking"])
        self.assertEqual(report["scopes"], ["/vault/a"])

    def test_the_dry_run_creates_nothing(self):
        db = BootstrapDB()
        source = FakeSource(TREE)
        self._bootstrap(db, source).dry_run()
        self.assertEqual(db.scopes, [])
        self.assertEqual(db.directories, [])
        self.assertEqual(db.segments, [])
        self.assertEqual(db.bootstraps, {})
        self.assertEqual(source.listed, [], "the dry run listed the source")

    def test_overlapping_roots_block_before_anything_happens(self):
        db = BootstrapDB()
        report = self._bootstrap(db, FakeSource(TREE),
                                 scan_paths=("/strg", "/vault/a")).dry_run()
        self.assertFalse(report["would_proceed"])
        self.assertTrue(any("overlapping" in b for b in report["blocking"]))

    def test_an_already_completed_bootstrap_blocks_a_second(self):
        db = BootstrapDB()
        db.bootstraps["b0"] = {"bootstrap_id": "b0", "session_id": 37,
                               "state": STATE_COMPLETED}
        report = self._bootstrap(db, FakeSource(TREE)).dry_run()
        self.assertFalse(report["would_proceed"])
        self.assertTrue(any("already been bootstrapped" in b
                            for b in report["blocking"]))

    def test_execute_without_approval_is_refused(self):
        db = BootstrapDB()
        with self.assertRaises(BootstrapRefused) as caught:
            self._bootstrap(db, FakeSource(TREE)).execute()
        self.assertIn("explicitly approved", str(caught.exception))
        self.assertEqual(db.scopes, [])

    def test_execute_refuses_when_the_dry_run_blocks(self):
        db = BootstrapDB()
        bootstrap = self._bootstrap(db, FakeSource(TREE),
                                    scan_paths=("/strg", "/vault/a"))
        with self.assertRaises(BootstrapRefused):
            bootstrap.execute(approved=True)
        self.assertEqual(db.scopes, [])


# =============================================================================
# B. The shadow rehearsal — the acceptance gate
# =============================================================================
class ShadowRehearsalTests(_Bootstrap):
    """A legacy session that already holds /vault/a/old.bin at 10 bytes."""

    def _run(self, snapshot, tree=None):
        db = BootstrapDB(snapshot=snapshot)
        source = FakeSource(tree or TREE)
        result = self._bootstrap(db, source).execute(approved=True)
        return db, source, result

    def test_no_existing_member_is_duplicated(self):
        db, _source, result = self._run({"/vault/a/old.bin": 10})
        self.assertEqual(result["entries_covered"], 1)
        # It was recognised as already covered, so it is not offered as new.
        self.assertEqual(result["entries_changed"], 0)

    def test_no_newly_discovered_path_is_skipped(self):
        db, _source, result = self._run({"/vault/a/old.bin": 10})
        # fresh.bin and sub/deep.bin are not in the snapshot.
        self.assertEqual(result["entries_new"], 2)

    def test_every_directory_is_actually_listed(self):
        _db, source, _result = self._run({"/vault/a/old.bin": 10})
        self.assertEqual(sorted(source.listed), ["/vault/a", "/vault/a/sub"])

    def test_no_directory_is_final_without_traversal_evidence(self):
        """Catalog rows are not coverage."""
        db = BootstrapDB(snapshot={"/vault/a/old.bin": 10,
                                   "/vault/a/fresh.bin": 20,
                                   "/vault/a/sub/deep.bin": 30})
        source = FakeSource(TREE)
        # Before the bootstrap the snapshot already "describes" everything...
        self.assertEqual(db.directories, [])
        self._bootstrap(db, source).execute(approved=True)
        # ...yet finality only exists because the directories were listed.
        self.assertEqual(sorted(source.listed), ["/vault/a", "/vault/a/sub"])
        for row in db.directories:
            self.assertEqual(row["listing_state"], "complete")

    def test_a_source_change_is_recorded_and_never_replanned(self):
        # The plan says old.bin is 10 bytes; the source now says 10 too for
        # fresh.bin but old.bin has changed.
        db, _source, result = self._run({"/vault/a/old.bin": 999})
        self.assertEqual(result["entries_changed"], 1)
        changed = [e for e in db.errors if e["category"] == "source_changed"]
        self.assertEqual(len(changed), 1)
        self.assertEqual(changed[0]["disposition"], "unresolved")
        self.assertEqual(changed[0]["path"], "/vault/a/old.bin")

    def test_the_existing_membership_is_untouched(self):
        snapshot = {"/vault/a/old.bin": 999}
        db, _source, _result = self._run(dict(snapshot))
        self.assertEqual(db.snapshot, snapshot)

    def test_the_chunk_format_is_not_changed(self):
        """The bootstrap plans nothing and seals nothing."""
        db, _source, _result = self._run({"/vault/a/old.bin": 10})
        self.assertFalse(hasattr(db, "sealed_chunks"))
        import inspect
        source = inspect.getsource(FrontierBootstrap)
        for forbidden in ("append_remote_streaming_chunk", "seal_remote_chunk",
                          "consume_segment_range"):
            self.assertNotIn(forbidden, source, forbidden)


# =============================================================================
# C. The run record makes it resumable
# =============================================================================
class RunRecordTests(_Bootstrap):
    def test_the_run_is_recorded_with_its_counters(self):
        db = BootstrapDB(snapshot={"/vault/a/old.bin": 10})
        result = self._bootstrap(db, FakeSource(TREE)).execute(approved=True)
        run = db.bootstraps[result["bootstrap_id"]]
        self.assertEqual(run["session_id"], 37)
        self.assertEqual(run["source_host"], "srv02")
        self.assertEqual(run["directories_listed"], 2)
        self.assertEqual(run["segments_published"], 2)
        self.assertEqual(run["entries_covered"], 1)
        self.assertEqual(run["entries_new"], 2)

    def test_a_second_run_resumes_the_same_record(self):
        db = BootstrapDB()
        first = self._bootstrap(db, FakeSource(TREE),
                                max_directories=1).execute(approved=True)
        second = self._bootstrap(db, FakeSource(TREE)).execute(approved=True)
        self.assertEqual(first["bootstrap_id"], second["bootstrap_id"])
        self.assertEqual(len(db.bootstraps), 1)

    def test_a_resumed_run_lists_only_what_is_left(self):
        db = BootstrapDB()
        self._bootstrap(db, FakeSource(TREE),
                        max_directories=1).execute(approved=True)
        second_source = FakeSource(TREE)
        self._bootstrap(db, second_source).execute(approved=True)
        self.assertEqual(second_source.listed, ["/vault/a/sub"])

    def test_a_segment_is_imported_exactly_once_across_runs(self):
        db = BootstrapDB(snapshot={"/vault/a/old.bin": 10})
        self._bootstrap(db, FakeSource(TREE),
                        max_directories=1).execute(approved=True)
        before = list(db.import_calls)
        result = self._bootstrap(db, FakeSource(TREE)).execute(approved=True)
        # The first segment is revisited but short-circuits as imported, so it
        # contributes nothing the second time.
        self.assertEqual(result["segments_imported"], 1)
        self.assertGreater(len(db.import_calls), len(before))

    def test_a_failed_traversal_records_the_failure_and_leaves_legacy_usable(self):
        db = BootstrapDB()
        source = FakeSource(TREE, unreadable={"/vault/a"})
        bootstrap = self._bootstrap(db, source)
        with self.assertRaises(RuntimeError):
            bootstrap.execute(approved=True)
        run = list(db.bootstraps.values())[0]
        self.assertEqual(run["state"], STATE_FAILED)
        self.assertFalse(run["coverage_final"])

    def test_coverage_is_final_only_when_every_scope_is(self):
        db = BootstrapDB()
        result = self._bootstrap(db, FakeSource(TREE),
                                 max_directories=1).execute(approved=True)
        self.assertFalse(result["coverage_final"])
        result = self._bootstrap(db, FakeSource(TREE)).execute(approved=True)
        self.assertTrue(result["coverage_final"])
        self.assertEqual(
            db.bootstraps[result["bootstrap_id"]]["state"], STATE_COMPLETED)


# =============================================================================
# D. It never touches the tape
# =============================================================================
class NoTapeAccessTests(unittest.TestCase):
    def test_the_bootstrap_module_cannot_reach_ltfs(self):
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with open(os.path.join(root, "src", "frontier_bootstrap.py"),
                  encoding="utf-8") as handle:
            source = handle.read()
        for token in ("lto_drive", "_acquire_tape_io_lock", "get_volume_label",
                      "_ensure_lto_drive_ready", "LtfsCmd", "robocopy",
                      "LTOBackup"):
            self.assertNotIn(token, source, token)


if __name__ == "__main__":
    unittest.main()
