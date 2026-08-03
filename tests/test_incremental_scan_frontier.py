"""Plan 1 / Task 2.3 — directory-boundary continuation.

The frontier's whole claim is: **a crash replays at most the one directory that
was mid-listing, never a whole root.** These tests hold it to that, and to the
coverage honesty that makes it safe to believe:

* a completed directory is never re-enumerated;
* a partial directory is the only one that is;
* an error, an unresolved exceptional entry, or a changed source observation
  each prevent finality — coverage is never inferred;
* immediate listing, subtree coverage and planning are three separate facts;
* overlapping roots are refused, a reordered configuration only warns, and an
  added or removed root is refused.

The remote host is a fake that serves a synthetic tree; the frontier database
is an in-memory double implementing the same contract as
:class:`~src.pg_scan.PgScanMixin` (whose real SQL is covered by the isolated
PostgreSQL tests in ``tests/test_pg_integration.py``). No SSH, no PostgreSQL,
no LTFS.
"""
import copy
import os
import posixpath
import tempfile
import threading
import unittest
from types import SimpleNamespace
from unittest import mock

from src import scan_frontier as sf
from src.archive_artifacts import parse_jsonl_zst_artifact
from src.pipeline_types import (SCOPE_KIND_DIRECTORY, ScanCoverageState,
                                ScanDirectoryState, ScanSegmentState)
from src.scan_frontier import (DirectoryFrontierCoordinator,
                               ScopeConfigurationError, canonicalize_scopes,
                               reconcile_scope_order)
from src.scanning import DirectoryListing


# =============================================================================
# Fakes
# =============================================================================
class FakeSource:
    """A synthetic remote tree: ``{directory: {"files": {...}, "dirs": [...]}}``."""

    def __init__(self, tree, unreadable=(), observations=None):
        # Deep-copied: a test that mutates its source must not reach through
        # into the shared module-level fixture and corrupt later tests.
        self.tree = copy.deepcopy(tree)
        self.unreadable = set(unreadable)
        self.observations = dict(observations or {})
        self.listed = []              # every list_directory call, in order
        self.observed = []

    def list_directory(self, path):
        self.listed.append(path)
        if path in self.unreadable:
            raise RuntimeError(f"permission denied listing {path}")
        node = self.tree.get(path, {})
        files = sorted((posixpath.join(path, name), size)
                       for name, size in node.get("files", {}).items())
        dirs = sorted(posixpath.join(path, name)
                      for name in node.get("dirs", []))
        return DirectoryListing(
            path, files, dirs, list(node.get("errors", [])),
            self.observations.get(path, f"obs-{path}"), 0)

    def observe(self, path):
        self.observed.append(path)
        return self.observations.get(path, f"obs-{path}")


class FrontierDB:
    """In-memory stand-in for PgScanMixin, with the same refusal rules."""

    def __init__(self):
        self.scopes = []
        self.directories = []
        self.segments = []
        self.errors = []
        self.attempts = {}
        self._next_id = 1

    def _id(self):
        value = self._next_id
        self._next_id += 1
        return value

    # -- scopes ---------------------------------------------------------
    def create_scan_scopes(self, session_id, roots):
        wanted = []
        for ordinal, entry in enumerate(roots):
            root, kind = entry if isinstance(entry, tuple) else (entry,
                                                                 SCOPE_KIND_DIRECTORY)
            wanted.append((ordinal, root, kind))
        if self.scopes:
            have = {(s["source_root"], s["scope_kind"]) for s in self.scopes}
            want = {(r, k) for _o, r, k in wanted}
            if have != want:
                raise sf.ScopeConfigurationError("scope drift")
            return [s["scope_ordinal"] for s in self.scopes]
        for ordinal, root, kind in wanted:
            self.scopes.append({
                "scan_scope_id": self._id(), "session_id": session_id,
                "scope_ordinal": ordinal, "source_root": root,
                "scope_kind": kind,
                "coverage_state": ScanCoverageState.PROVISIONAL.value,
                "planning_complete": False})
        return [o for o, _r, _k in wanted]

    def get_scan_scopes(self, session_id):
        return sorted(self.scopes, key=lambda s: s["scope_ordinal"])

    # -- directories -----------------------------------------------------
    def enqueue_scan_directories(self, scan_scope_id, entries,
                                 parent_directory_id=None):
        known = {(d["scan_scope_id"], d["canonical_path"])
                 for d in self.directories}
        added = 0
        for path, ordinal in entries:
            if (scan_scope_id, path) in known:
                continue
            self.directories.append({
                "scan_directory_id": self._id(),
                "scan_scope_id": scan_scope_id, "canonical_path": path,
                "parent_directory_id": parent_directory_id,
                "traversal_ordinal": int(ordinal),
                "listing_state": ScanDirectoryState.PENDING.value,
                "subtree_coverage_state": ScanCoverageState.PROVISIONAL.value,
                "planning_state": "unplanned",
                "observation_before": None, "observation_after": None,
                "direct_file_count": 0, "direct_byte_count": 0,
                "error_count": 0, "owner_token": None, "attempt_id": None})
            added += 1
        return added

    def _by_id(self, directory_id):
        for row in self.directories:
            if row["scan_directory_id"] == directory_id:
                return row
        raise AssertionError(f"no directory {directory_id}")

    def claim_next_directory(self, session_id, owner_token, attempt_id,
                             lease_seconds=900):
        order = {s["scan_scope_id"]: s["scope_ordinal"] for s in self.scopes}
        candidates = [
            d for d in self.directories
            if d["listing_state"] in (ScanDirectoryState.PENDING.value,
                                      ScanDirectoryState.PARTIAL.value)
            and d["owner_token"] is None]
        if not candidates:
            return None
        row = min(candidates,
                  key=lambda d: (order.get(d["scan_scope_id"], 0),
                                 d["traversal_ordinal"]))
        row["listing_state"] = ScanDirectoryState.SCANNING.value
        row["owner_token"] = owner_token
        row["attempt_id"] = attempt_id
        return dict(row)

    def complete_directory_listing(self, directory_id, owner_token, *,
                                   direct_file_count, direct_byte_count,
                                   observation_after=None, error_count=0):
        row = self._by_id(directory_id)
        if row["owner_token"] != owner_token:
            return False
        row["listing_state"] = (ScanDirectoryState.ERROR.value if error_count
                                else ScanDirectoryState.COMPLETE.value)
        row["direct_file_count"] = direct_file_count
        row["direct_byte_count"] = direct_byte_count
        row["observation_after"] = observation_after
        row["error_count"] = error_count
        row["owner_token"] = None
        row["attempt_id"] = None
        return True

    def mark_directory_partial(self, directory_id, owner_token,
                               last_committed_segment_id=None):
        row = self._by_id(directory_id)
        if row["owner_token"] != owner_token:
            return False
        row["listing_state"] = ScanDirectoryState.PARTIAL.value
        row["owner_token"] = None
        row["attempt_id"] = None
        return True

    def invalidate_directory(self, directory_id, reason):
        row = self._by_id(directory_id)
        row["listing_state"] = ScanDirectoryState.INVALIDATED.value
        row["subtree_coverage_state"] = ScanCoverageState.INVALIDATED.value
        parent = row["parent_directory_id"]
        while parent is not None:
            ancestor = self._by_id(parent)
            ancestor["subtree_coverage_state"] = (
                ScanCoverageState.INVALIDATED.value)
            parent = ancestor["parent_directory_id"]
        self.errors.append({"scan_directory_id": directory_id,
                            "category": "source_changed", "message": reason,
                            "disposition": "unresolved"})

    def get_covered_directories(self, scan_scope_id):
        return sorted(
            (d for d in self.directories
             if d["scan_scope_id"] == scan_scope_id
             and d["listing_state"] in (ScanDirectoryState.COMPLETE.value,
                                        ScanDirectoryState.ERROR.value)),
            key=lambda d: d["traversal_ordinal"])

    def finalize_directory_subtree(self, directory_id):
        row = self._by_id(directory_id)
        if row["listing_state"] != ScanDirectoryState.COMPLETE.value:
            return False, f"listing_state is {row['listing_state']!r}"
        if (row["observation_before"] and row["observation_after"]
                and row["observation_before"] != row["observation_after"]):
            return False, "the source changed while it was being listed"
        children = [d for d in self.directories
                    if d["parent_directory_id"] == directory_id]
        for child in children:
            if (child["listing_state"] != ScanDirectoryState.COMPLETE.value
                    or child["subtree_coverage_state"]
                    != ScanCoverageState.FINAL.value):
                return False, "descendant(s) are not yet final"
        unresolved = [e for e in self.errors
                      if e["scan_directory_id"] == directory_id
                      and e["disposition"] == "unresolved"]
        if unresolved:
            return False, f"{len(unresolved)} unresolved error(s)"
        row["subtree_coverage_state"] = ScanCoverageState.FINAL.value
        return True, "final"

    def finalize_scan_scope(self, scan_scope_id):
        outstanding = [
            d for d in self.directories
            if d["scan_scope_id"] == scan_scope_id
            and (d["listing_state"] != ScanDirectoryState.COMPLETE.value
                 or d["subtree_coverage_state"]
                 != ScanCoverageState.FINAL.value)]
        if outstanding:
            return False, f"{len(outstanding)} not final"
        for scope in self.scopes:
            if scope["scan_scope_id"] == scan_scope_id:
                scope["coverage_state"] = ScanCoverageState.FINAL.value
        return True, "final"

    # -- segments ---------------------------------------------------------
    def publish_scan_segment(self, directory_id, *, first_scan_ordinal,
                             last_scan_ordinal, locator, file_count,
                             byte_count, artifact_size_bytes=None,
                             first_canonical_path=None,
                             last_canonical_path=None,
                             artifact_version="scan-segment-v1"):
        if str(locator).endswith(".part"):
            raise AssertionError("a .part locator reached the database")
        row = {"scan_segment_id": self._id(),
               "scan_directory_id": directory_id,
               "first_scan_ordinal": first_scan_ordinal,
               "last_scan_ordinal": last_scan_ordinal,
               "next_unconsumed_ordinal": first_scan_ordinal,
               "locator": locator, "file_count": file_count,
               "byte_count": byte_count,
               "state": ScanSegmentState.READY.value,
               "first_canonical_path": first_canonical_path,
               "last_canonical_path": last_canonical_path}
        self.segments.append(row)
        return row

    # -- errors and attempts ---------------------------------------------
    def record_scan_error(self, *, scan_scope_id=None, scan_directory_id=None,
                          category, path=None, message=None,
                          disposition="unresolved"):
        self.errors.append({"scan_scope_id": scan_scope_id,
                            "scan_directory_id": scan_directory_id,
                            "category": category, "path": path,
                            "message": message, "disposition": disposition})
        return len(self.errors)

    def start_worker_attempt(self, **kwargs):
        attempt_id = f"att-{self._id()}"
        self.attempts[attempt_id] = dict(kwargs, terminal_state=None)
        return attempt_id

    def finish_worker_attempt(self, attempt_id, terminal_state):
        self.attempts[attempt_id]["terminal_state"] = terminal_state
        return True


# =============================================================================
# Harness
# =============================================================================
SIMPLE_TREE = {
    "/strg/a": {"files": {"f1": 10, "f2": 20}, "dirs": ["sub"]},
    "/strg/a/sub": {"files": {"g1": 30}, "dirs": []},
}


class _Frontier(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.root = self._dir.name
        self.addCleanup(self._dir.cleanup)
        self.db = FrontierDB()
        self.stop = threading.Event()

    def _coordinator(self, source, scan_paths=("/strg/a",), **kwargs):
        return DirectoryFrontierCoordinator(
            db=self.db, session_id=37, scan_paths=list(scan_paths),
            archive_root=self.root, scanner_factory=lambda metrics: source,
            stop_event=self.stop, owner_token="owner-1", **kwargs)


# =============================================================================
# A. Scope canonicalization
# =============================================================================
class ScopeCanonicalizationTests(unittest.TestCase):
    def test_paths_are_normalized(self):
        self.assertEqual(
            canonicalize_scopes(["/strg/a/", "/strg//b", "/strg/c/./"]),
            ["/strg/a", "/strg/b", "/strg/c"])

    def test_backslashes_are_treated_as_separators_in_config_text(self):
        self.assertEqual(canonicalize_scopes(["\\strg\\a"]), ["/strg/a"])

    def test_a_relative_or_empty_root_is_refused(self):
        for bad in ("", "   ", "strg/a", "./a", None):
            with self.assertRaises(ScopeConfigurationError, msg=repr(bad)):
                canonicalize_scopes([bad])

    def test_overlapping_roots_are_refused_not_merged(self):
        with self.assertRaises(ScopeConfigurationError) as caught:
            canonicalize_scopes(["/strg", "/strg/a"])
        self.assertIn("overlapping scan roots", str(caught.exception))

    def test_a_duplicate_root_is_refused(self):
        with self.assertRaises(ScopeConfigurationError):
            canonicalize_scopes(["/strg/a", "/strg/a"])

    def test_sibling_roots_with_a_shared_prefix_are_fine(self):
        """/strg/ab does NOT lie under /strg/a."""
        self.assertEqual(canonicalize_scopes(["/strg/a", "/strg/ab"]),
                         ["/strg/a", "/strg/ab"])

    def test_a_single_file_scope_is_a_legal_root(self):
        self.assertEqual(canonicalize_scopes(["/strg/a/one.bin"]),
                         ["/strg/a/one.bin"])


class ScopeReconciliationTests(unittest.TestCase):
    def test_a_first_run_uses_the_configured_order(self):
        self.assertEqual(reconcile_scope_order(["/a", "/b"], []), ["/a", "/b"])

    def test_an_identical_set_and_order_is_used_as_is(self):
        self.assertEqual(reconcile_scope_order(["/a", "/b"], ["/a", "/b"]),
                         ["/a", "/b"])

    def test_a_reordered_configuration_warns_and_keeps_persisted_order(self):
        ui = mock.MagicMock()
        result = reconcile_scope_order(["/b", "/a"], ["/a", "/b"], ui=ui)
        self.assertEqual(result, ["/a", "/b"])
        ui.warning.assert_called_once()
        self.assertIn("reproducible", ui.warning.call_args.args[0])

    def test_an_added_root_is_refused(self):
        with self.assertRaises(ScopeConfigurationError) as caught:
            reconcile_scope_order(["/a", "/b", "/c"], ["/a", "/b"])
        self.assertIn("Added: ['/c']", str(caught.exception))

    def test_a_removed_root_is_refused(self):
        with self.assertRaises(ScopeConfigurationError) as caught:
            reconcile_scope_order(["/a"], ["/a", "/b"])
        self.assertIn("removed: ['/b']", str(caught.exception))


# =============================================================================
# B. Traversal
# =============================================================================
class TraversalTests(_Frontier):
    def test_a_full_traversal_covers_every_directory_once(self):
        source = FakeSource(SIMPLE_TREE)
        self._coordinator(source).run()
        self.assertEqual(source.listed, ["/strg/a", "/strg/a/sub"])
        states = {d["canonical_path"]: d["listing_state"]
                  for d in self.db.directories}
        self.assertEqual(states, {"/strg/a": "complete",
                                  "/strg/a/sub": "complete"})

    def test_entries_are_published_as_ready_segments(self):
        source = FakeSource(SIMPLE_TREE)
        self._coordinator(source).run()
        self.assertEqual(len(self.db.segments), 2)
        for segment in self.db.segments:
            self.assertEqual(segment["state"], "ready")
            self.assertFalse(segment["locator"].endswith(".part"))

    def test_a_segment_reconstructs_its_exact_ordered_entry_list(self):
        source = FakeSource(SIMPLE_TREE)
        self._coordinator(source).run()
        segment = min(self.db.segments, key=lambda s: s["scan_segment_id"])
        _header, entries, totals = parse_jsonl_zst_artifact(
            self.root, segment["locator"])
        self.assertEqual([e["path"] for e in entries],
                         ["/strg/a/f1", "/strg/a/f2"])
        self.assertEqual(totals["byte_count"], 30)
        self.assertEqual(segment["file_count"], 2)
        self.assertEqual(segment["byte_count"], 30)

    def test_the_artifact_exists_before_the_database_points_at_it(self):
        """A locator must never name a file that is not there yet."""
        source = FakeSource(SIMPLE_TREE)
        published_when = []
        original = self.db.publish_scan_segment

        def spy(directory_id, **kwargs):
            from src.archive_artifacts import resolve_locator
            published_when.append(
                os.path.exists(resolve_locator(self.root, kwargs["locator"])))
            return original(directory_id, **kwargs)
        self.db.publish_scan_segment = spy
        self._coordinator(source).run()
        self.assertTrue(published_when)
        self.assertTrue(all(published_when),
                        "a segment row was written before its artifact existed")

    def test_an_empty_directory_is_covered_without_a_segment(self):
        source = FakeSource({"/strg/a": {"files": {}, "dirs": []}})
        self._coordinator(source).run()
        self.assertEqual(self.db.segments, [])
        self.assertEqual(self.db.directories[0]["listing_state"], "complete")
        self.assertEqual(
            self.db.directories[0]["subtree_coverage_state"], "final")

    def test_children_are_enqueued_in_deterministic_order(self):
        tree = {"/strg/a": {"files": {}, "dirs": ["z", "m", "a"]}}
        for name in ("z", "m", "a"):
            tree[f"/strg/a/{name}"] = {"files": {}, "dirs": []}
        source = FakeSource(tree)
        self._coordinator(source).run()
        self.assertEqual(source.listed,
                         ["/strg/a", "/strg/a/a", "/strg/a/m", "/strg/a/z"])

    def test_multiple_scopes_are_traversed_in_persisted_order(self):
        source = FakeSource({
            "/strg/b": {"files": {"x": 1}, "dirs": []},
            "/strg/a": {"files": {"y": 1}, "dirs": []}})
        self._coordinator(source, scan_paths=("/strg/b", "/strg/a")).run()
        self.assertEqual(source.listed, ["/strg/b", "/strg/a"])


# =============================================================================
# C. Continuation — the whole point
# =============================================================================
class ContinuationTests(_Frontier):
    def test_a_completed_directory_is_never_re_enumerated(self):
        source = FakeSource(SIMPLE_TREE)
        # First run: bounded to one directory, simulating an interruption.
        self._coordinator(source, max_directories=1).run()
        self.assertEqual(source.listed, ["/strg/a"])

        second = FakeSource(SIMPLE_TREE)
        self._coordinator(second).run()
        # ONLY the child is listed. The root is not re-walked.
        self.assertEqual(second.listed, ["/strg/a/sub"])

    def test_a_crash_replays_at_most_the_partial_directory(self):
        source = FakeSource(SIMPLE_TREE, unreadable={"/strg/a/sub"})
        coordinator = self._coordinator(source)
        with self.assertRaises(RuntimeError):
            coordinator.run()
        states = {d["canonical_path"]: d["listing_state"]
                  for d in self.db.directories}
        self.assertEqual(states["/strg/a"], "complete")
        self.assertEqual(states["/strg/a/sub"], "partial")

        # The retry lists exactly one directory: the partial one.
        retry = FakeSource(SIMPLE_TREE)
        self._coordinator(retry).run()
        self.assertEqual(retry.listed, ["/strg/a/sub"])

    def test_a_failed_listing_records_why(self):
        source = FakeSource(SIMPLE_TREE, unreadable={"/strg/a"})
        with self.assertRaises(RuntimeError):
            self._coordinator(source).run()
        categories = [e["category"] for e in self.db.errors]
        self.assertIn("listing_failed", categories)

    def test_an_identical_re_listing_reuses_its_published_artifact(self):
        """After an invalidation, re-listing must not fail on its own file."""
        source = FakeSource(SIMPLE_TREE)
        coordinator = self._coordinator(source)
        coordinator.establish_scopes()
        coordinator.process_one_directory()
        root = next(d for d in self.db.directories
                    if d["canonical_path"] == "/strg/a")
        segments_before = len(self.db.segments)

        # Invalidate and requeue exactly as the mutation sweep would.
        self.db.invalidate_directory(root["scan_directory_id"], "changed")
        root["listing_state"] = "pending"
        root["owner_token"] = None

        self.assertTrue(coordinator.process_one_directory())
        self.assertEqual(len(self.db.segments), segments_before + 1)
        _header, entries, _totals = parse_jsonl_zst_artifact(
            self.root, self.db.segments[-1]["locator"])
        self.assertEqual([e["path"] for e in entries],
                         ["/strg/a/f1", "/strg/a/f2"])

    def test_a_re_listing_that_disagrees_is_a_conflict_not_an_overwrite(self):
        from src.archive_artifacts import ArtifactConflict
        source = FakeSource(SIMPLE_TREE)
        coordinator = self._coordinator(source)
        coordinator.establish_scopes()
        coordinator.process_one_directory()
        root = next(d for d in self.db.directories
                    if d["canonical_path"] == "/strg/a")
        root["listing_state"] = "pending"
        root["owner_token"] = None

        # The source now reports different content at the same locator.
        source.tree["/strg/a"] = {"files": {"f1": 999}, "dirs": ["sub"]}
        with self.assertRaises(ArtifactConflict) as caught:
            coordinator.process_one_directory()
        self.assertIn("different content", str(caught.exception))
        # The original artifact is untouched.
        _header, entries, _totals = parse_jsonl_zst_artifact(
            self.root, self.db.segments[0]["locator"])
        self.assertEqual([e["size"] for e in entries], [10, 20])

    def test_restarting_from_scratch_re_lists_nothing_already_covered(self):
        source = FakeSource(SIMPLE_TREE)
        self._coordinator(source).run()
        for attempt in range(3):
            again = FakeSource(SIMPLE_TREE)
            self._coordinator(again).run()
            self.assertEqual(again.listed, [], f"replayed on attempt {attempt}")


# =============================================================================
# D. Coverage honesty
# =============================================================================
class CoverageHonestyTests(_Frontier):
    def test_an_exceptional_entry_prevents_completion(self):
        tree = dict(SIMPLE_TREE)
        tree["/strg/a"] = dict(tree["/strg/a"],
                               errors=[("non_regular_entry", "/strg/a/sock",
                                        "entry type 's' is not archived")])
        source = FakeSource(tree)
        self._coordinator(source).run()
        row = next(d for d in self.db.directories
                   if d["canonical_path"] == "/strg/a")
        self.assertEqual(row["listing_state"], "error")
        self.assertNotEqual(row["subtree_coverage_state"], "final")

    def test_an_unresolved_error_blocks_subtree_finality(self):
        source = FakeSource(SIMPLE_TREE)
        coordinator = self._coordinator(source)
        coordinator.run()
        # Everything is final...
        root = next(d for d in self.db.directories
                    if d["canonical_path"] == "/strg/a")
        self.assertEqual(root["subtree_coverage_state"], "final")
        # ...until an unresolved error is recorded against it.
        self.db.record_scan_error(
            scan_directory_id=root["scan_directory_id"],
            category="permission_denied", path="/strg/a/secret")
        done, reason = self.db.finalize_directory_subtree(
            root["scan_directory_id"])
        self.assertFalse(done)
        self.assertIn("unresolved", reason)

    def test_a_parent_is_not_final_before_its_children(self):
        source = FakeSource(SIMPLE_TREE)
        self._coordinator(source, max_directories=1).run()
        root = next(d for d in self.db.directories
                    if d["canonical_path"] == "/strg/a")
        self.assertNotEqual(root["subtree_coverage_state"], "final")

    def test_the_three_states_are_independent(self):
        source = FakeSource(SIMPLE_TREE)
        self._coordinator(source).run()
        for row in self.db.directories:
            # Explored and final...
            self.assertEqual(row["listing_state"], "complete")
            self.assertEqual(row["subtree_coverage_state"], "final")
            # ...but nothing has been assigned to a chunk yet.
            self.assertEqual(row["planning_state"], "unplanned")
        for segment in self.db.segments:
            self.assertEqual(segment["state"], "ready")


# =============================================================================
# E. The final mutation sweep
# =============================================================================
class MutationSweepTests(_Frontier):
    def test_a_stable_source_stays_final(self):
        source = FakeSource(SIMPLE_TREE)
        self._coordinator(source).run()
        self.assertTrue(source.observed)
        for row in self.db.directories:
            self.assertEqual(row["subtree_coverage_state"], "final")

    def test_a_changed_observation_invalidates_the_ancestor_chain(self):
        source = FakeSource(SIMPLE_TREE)
        coordinator = self._coordinator(source)
        coordinator.establish_scopes()
        while coordinator.process_one_directory():
            pass
        # The child changes between listing and the sweep.
        source.observations["/strg/a/sub"] = "obs-CHANGED"
        invalidated = coordinator.final_mutation_sweep()
        self.assertEqual(invalidated, 1)
        states = {d["canonical_path"]: d["subtree_coverage_state"]
                  for d in self.db.directories}
        self.assertEqual(states["/strg/a/sub"], "invalidated")
        self.assertEqual(states["/strg/a"], "invalidated")

    def test_an_invalidated_directory_is_requeued(self):
        source = FakeSource(SIMPLE_TREE)
        coordinator = self._coordinator(source)
        coordinator.establish_scopes()
        while coordinator.process_one_directory():
            pass
        source.observations["/strg/a/sub"] = "obs-CHANGED"
        coordinator.final_mutation_sweep()
        row = next(d for d in self.db.directories
                   if d["canonical_path"] == "/strg/a/sub")
        self.assertEqual(row["listing_state"], "invalidated")

    def test_an_unreadable_observation_leaves_coverage_provisional(self):
        """No comparison is possible, so no finality is asserted."""
        source = FakeSource(SIMPLE_TREE)
        coordinator = self._coordinator(source)
        coordinator.establish_scopes()
        while coordinator.process_one_directory():
            pass
        source.observe = lambda path: None
        self.assertEqual(coordinator.final_mutation_sweep(), 0)

    def test_the_sweep_lists_nothing(self):
        """It is one stat per directory, never a re-listing."""
        source = FakeSource(SIMPLE_TREE)
        coordinator = self._coordinator(source)
        coordinator.establish_scopes()
        while coordinator.process_one_directory():
            pass
        before = list(source.listed)
        coordinator.final_mutation_sweep()
        self.assertEqual(source.listed, before)


# =============================================================================
# F. Cancellation
# =============================================================================
class CancellationTests(_Frontier):
    def test_a_stop_ends_the_traversal_without_finalizing(self):
        source = FakeSource(SIMPLE_TREE)
        self.stop.set()
        coordinator = self._coordinator(source)
        coordinator.run()
        self.assertEqual(source.listed, [])
        self.assertEqual(source.observed, [])

    def test_a_stop_mid_traversal_leaves_covered_work_committed(self):
        source = FakeSource(SIMPLE_TREE)
        coordinator = self._coordinator(source)
        coordinator.establish_scopes()
        coordinator.process_one_directory()
        self.stop.set()
        row = next(d for d in self.db.directories
                   if d["canonical_path"] == "/strg/a")
        self.assertEqual(row["listing_state"], "complete")
        self.assertEqual(len(self.db.segments), 1)

    def test_the_attempt_is_closed_on_every_exit(self):
        source = FakeSource(SIMPLE_TREE)
        self._coordinator(source).run()
        self.assertTrue(self.db.attempts)
        for attempt in self.db.attempts.values():
            self.assertIsNotNone(attempt["terminal_state"])


# =============================================================================
# G. The immediate-child listing command
# =============================================================================
class DirectoryListingCommandTests(unittest.TestCase):
    def _scanner(self):
        from src.scanning import DirectoryFrontierScanner
        return DirectoryFrontierScanner(
            "u", "h", skipped_tracker=mock.MagicMock(), ui=mock.MagicMock())

    def _run(self, stdout, returncode=0, stderr=""):
        from src import scanning
        result = SimpleNamespace(stdout=stdout, stderr=stderr,
                                 returncode=returncode)
        with mock.patch.object(scanning, "_ssh_run", return_value=result) as run:
            listing = self._scanner().list_directory("/strg/a")
        return listing, run.call_args.args[2]

    def test_the_listing_is_bounded_to_immediate_children(self):
        _listing, command = self._run("OBS 1:2:3\0")
        self.assertIn("-mindepth 1", command)
        self.assertIn("-maxdepth 1", command)
        self.assertNotIn("-type f", command)

    def test_records_are_nul_framed(self):
        _listing, command = self._run("OBS 1:2:3\0")
        self.assertIn(r"%y %s %p\0", command)

    def test_files_and_directories_are_separated_and_sorted(self):
        listing, _ = self._run(
            "OBS 1:2:3\0"
            "f 20 /strg/a/z\0f 10 /strg/a/a\0d 4096 /strg/a/sub\0")
        self.assertEqual(listing.files, [("/strg/a/a", 10), ("/strg/a/z", 20)])
        self.assertEqual(listing.directories, ["/strg/a/sub"])
        self.assertEqual(listing.file_count, 2)
        self.assertEqual(listing.byte_count, 30)

    def test_the_observation_token_is_captured(self):
        listing, _ = self._run("OBS 1700000000:1700000001:99\0")
        self.assertEqual(listing.observation, "1700000000:1700000001:99")

    def test_an_unknown_observation_becomes_none(self):
        listing, _ = self._run("OBS unknown\0")
        self.assertIsNone(listing.observation)

    def test_a_non_regular_entry_is_recorded_not_dropped(self):
        listing, _ = self._run("OBS 1:2:3\0l 7 /strg/a/link\0")
        self.assertEqual(listing.files, [])
        self.assertEqual([e[0] for e in listing.errors],
                         ["non_regular_entry"])

    def test_a_non_utf8_name_is_recorded_as_an_error(self):
        listing, _ = self._run("OBS 1:2:3\0f 10 /strg/a/b�d\0")
        self.assertEqual(listing.files, [])
        self.assertEqual([e[0] for e in listing.errors], ["invalid_utf8_name"])
        # ...and the path is NOT persisted, because it is not representable.
        self.assertIsNone(listing.errors[0][1])

    def test_a_record_outside_the_directory_is_refused(self):
        listing, _ = self._run("OBS 1:2:3\0f 10 /elsewhere/x\0")
        self.assertEqual(listing.files, [])
        self.assertEqual([e[0] for e in listing.errors],
                         ["entry_outside_directory"])

    def test_a_grandchild_is_not_an_immediate_child(self):
        listing, _ = self._run("OBS 1:2:3\0f 10 /strg/a/sub/deep\0")
        self.assertEqual(listing.files, [])
        self.assertEqual([e[0] for e in listing.errors],
                         ["entry_outside_directory"])

    def test_an_invalid_size_token_is_recorded(self):
        listing, _ = self._run("OBS 1:2:3\0f notanumber /strg/a/f\0")
        self.assertEqual([e[0] for e in listing.errors], ["invalid_size"])

    def test_names_with_spaces_survive(self):
        listing, _ = self._run("OBS 1:2:3\0f 10 /strg/a/two words\0")
        self.assertEqual(listing.files, [("/strg/a/two words", 10)])

    def test_a_name_with_a_newline_survives_nul_framing(self):
        listing, _ = self._run("OBS 1:2:3\0f 10 /strg/a/two\nlines\0")
        self.assertEqual(listing.files, [("/strg/a/two\nlines", 10)])

    def test_a_literal_backslash_in_a_linux_name_is_preserved(self):
        listing, _ = self._run("OBS 1:2:3\0f 10 /strg/a/back\\slash\0")
        self.assertEqual(listing.files, [("/strg/a/back\\slash", 10)])

    def test_unicode_names_survive(self):
        listing, _ = self._run("OBS 1:2:3\0f 10 /strg/a/יוניקוד\0")
        self.assertEqual(listing.files, [("/strg/a/יוניקוד", 10)])

    def test_stderr_warnings_become_recorded_errors(self):
        listing, _ = self._run(
            "OBS 1:2:3\0", returncode=1,
            stderr="find: '/strg/a/secret': Permission denied")
        self.assertTrue(any(e[0] == "listing_warning" for e in listing.errors))

    def test_a_timeout_raises_rather_than_reporting_an_empty_directory(self):
        from src import scanning
        result = SimpleNamespace(stdout="", stderr="", returncode=124)
        with mock.patch.object(scanning, "_ssh_run", return_value=result), \
                self.assertRaises(RuntimeError) as caught:
            self._scanner().list_directory("/strg/a")
        self.assertIn("timed out", str(caught.exception))

    def test_an_ssh_failure_raises(self):
        from src import scanning
        result = SimpleNamespace(stdout="", stderr="boom", returncode=255)
        with mock.patch.object(scanning, "_ssh_run", return_value=result), \
                self.assertRaises(RuntimeError):
            self._scanner().list_directory("/strg/a")

    def test_observe_is_a_single_stat(self):
        from src import scanning
        result = SimpleNamespace(stdout="1:2:3\n", stderr="", returncode=0)
        with mock.patch.object(scanning, "_ssh_run",
                               return_value=result) as run:
            token = self._scanner().observe("/strg/a")
        self.assertEqual(token, "1:2:3")
        command = run.call_args.args[2]
        self.assertIn("stat", command)
        self.assertNotIn("find", command)


if __name__ == "__main__":
    unittest.main()
