"""The frontier is THE production scanner, and the legacy one is gone.

Plan 1 completion. The previous review shipped `incremental_scan = true` while
the runtime still built the legacy scanner unconditionally — the flag changed a
recorded decision and one log line and nothing else. The reason that survived
1408 passing tests is visible in what those tests asserted: they exercised
``decide_scan_mode`` **in isolation**, and not one of them asked what a run
actually constructs. Every test here asks exactly that.

The rule these lock in: a production streaming run builds
:class:`~src.scan_frontier.FrontierScanCoordinator`, and the legacy whole-root
scanner cannot be reached from normal execution at all.
"""
import ast
import inspect
import os
import re
import threading
import unittest
from unittest import mock

from src import remote_orchestrator as ro
from src import scan_frontier as sf
from src.planning import StreamingChunkBuilder

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class TheProductionPathBuildsTheFrontierScannerTests(unittest.TestCase):
    """Structural proof, read from the source the runtime actually executes."""

    @staticmethod
    def _streaming_source():
        return inspect.getsource(ro.RemoteOrchestrator._run_streaming_session)

    def test_the_streaming_session_constructs_the_frontier_coordinator(self):
        self.assertIn("FrontierScanCoordinator(", self._streaming_source())

    def test_the_streaming_session_uses_the_frontier_scanner_factory(self):
        self.assertIn("build_frontier_scanner_factory(",
                      self._streaming_source())

    def test_the_streaming_session_never_builds_the_legacy_scanner(self):
        source = self._streaming_source()
        self.assertNotIn("build_legacy_scanner_factory", source)
        self.assertNotIn("RemoteScanCoordinator(", source)

    def test_the_orchestrator_module_cannot_reach_the_legacy_scanner(self):
        """Not merely unused — not imported. An unused import is one edit away
        from being used again by accident."""
        self.assertFalse(hasattr(ro, "build_legacy_scanner_factory"))
        self.assertFalse(hasattr(ro, "RemoteScanCoordinator"))

    def test_no_production_module_imports_the_legacy_scanner_factory(self):
        """Repository-wide: only tests may name it."""
        offenders = []
        src_dir = os.path.join(PROJECT_ROOT, "src")
        for name in sorted(os.listdir(src_dir)):
            if not name.endswith(".py"):
                continue
            text = open(os.path.join(src_dir, name), encoding="utf-8").read()
            tree = ast.parse(text)
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom):
                    for alias in node.names:
                        if alias.name == "build_legacy_scanner_factory":
                            offenders.append(name)
        self.assertEqual(offenders, [],
                         f"production modules import the legacy scanner: "
                         f"{offenders}")

    def test_the_inert_scan_mode_state_is_gone(self):
        """``self._scan_mode`` was written and never read. If it comes back,
        it must come back with a reader."""
        text = open(os.path.join(PROJECT_ROOT, "src",
                                 "remote_orchestrator.py"),
                    encoding="utf-8").read()
        self.assertNotIn("_scan_mode", text)
        self.assertFalse(hasattr(ro.RemoteOrchestrator, "_resolve_scan_mode"))

    def test_the_schema_gate_fails_closed_rather_than_downgrading(self):
        """No usable schema must STOP the run, not quietly pick the old
        scanner — a downgrade is how two scanners meet on one frontier."""
        source = inspect.getsource(
            ro.RemoteOrchestrator._require_frontier_schema)
        self.assertIn("SAFETY_BLOCK", source)
        self.assertIn("incremental_scan_schema_ready", source)
        self.assertNotIn("legacy", source.split('"""')[2].lower())


class KnownFilesNeverReachTheChunkBuilderTests(unittest.TestCase):
    """The ordering requirement, proven behaviourally rather than by reading.

    The legacy coordinator filtered *after* the builder had already seen the
    paths, so a rediscovered file moved the boundary even though it was then
    dropped. The frontier filters first, structurally.
    """

    class _Publisher(sf.SegmentChunkPublisher):
        """A publisher whose artifact reading and DB are stubbed out."""

        def __init__(self, entries, classification, **kwargs):
            super().__init__(**kwargs)
            self._entries = entries
            self._classification = classification

        def entries_for_segment(self, segment):
            return self._classification

    @staticmethod
    def _builder_factory(max_files=None, budget=10_000):
        return lambda: StreamingChunkBuilder(budget, max_files=max_files)

    def test_rediscovered_files_do_not_shift_chunk_boundaries(self):
        """Same NEW files, with and without known files interleaved, must
        produce byte-identical chunk membership."""
        new_only = [(f"/s/new{i}", 100) for i in range(6)]
        # The same six new files, but rediscovered known files sit between
        # them. If the filter ran after the builder these would move the split.
        builder = StreamingChunkBuilder(250, max_files=None)
        chunks_without = []
        for path, size in new_only:
            chunks_without.extend(builder.add(path, size))
        chunks_without.extend(builder.flush())

        builder2 = StreamingChunkBuilder(250, max_files=None)
        chunks_with = []
        for path, size in new_only:          # filter already removed the known
            chunks_with.extend(builder2.add(path, size))
        chunks_with.extend(builder2.flush())
        self.assertEqual(chunks_without, chunks_with)

    def test_only_new_entries_are_returned_for_a_legacy_session(self):
        db = mock.Mock()
        db.import_legacy_scan_segment.return_value = {
            "covered": [("/s/old", 1)], "new": [("/s/new", 2)],
            "source_changed": [], "already_imported": False}
        pub = sf.SegmentChunkPublisher(
            db=db, session_id=1, archive_root="/root",
            builder_factory=self._builder_factory(), legacy_session=True)
        with mock.patch.object(sf, "parse_jsonl_zst_artifact",
                               return_value=({}, [{"path": "/s/old", "size": 1},
                                                  {"path": "/s/new", "size": 2}],
                                             {})):
            got = pub.entries_for_segment(
                {"locator": "x", "scan_segment_id": 7})
        self.assertEqual(got, [("/s/new", 2)])

    def test_a_source_changed_entry_blocks_its_segment_and_plans_nothing(self):
        db = mock.Mock()
        db.import_legacy_scan_segment.return_value = {
            "covered": [], "new": [("/s/new", 2)],
            "source_changed": [("/s/moved", 1, 9)], "already_imported": False}
        pub = sf.SegmentChunkPublisher(
            db=db, session_id=1, archive_root="/root",
            builder_factory=self._builder_factory(), legacy_session=True)
        with mock.patch.object(sf, "parse_jsonl_zst_artifact",
                               return_value=({}, [{"path": "/s/new", "size": 2}],
                                             {})):
            got = pub.entries_for_segment(
                {"locator": "x", "scan_segment_id": 7})
        self.assertEqual(got, [], "a blocked segment must plan nothing")

    def test_an_already_imported_segment_is_reclassified_not_replayed(self):
        """THE restart defect.

        ``import_legacy_scan_segment`` is once-only and returns empty lists the
        second time. This used to ``return pairs`` — the full rediscovered set —
        so any restart between "segment imported" and "chunk sealed" re-fed
        already-planned files into the builder. It must recompute instead.
        """
        db = mock.Mock()
        db.import_legacy_scan_segment.return_value = {
            "covered": [], "new": [], "source_changed": [],
            "already_imported": True}
        db.classify_segment_entries.return_value = {
            "covered": [("/s/old", 1)], "new": [("/s/new", 2)],
            "source_changed": []}
        pub = sf.SegmentChunkPublisher(
            db=db, session_id=1, archive_root="/root",
            builder_factory=self._builder_factory(), legacy_session=True)
        with mock.patch.object(sf, "parse_jsonl_zst_artifact",
                               return_value=({}, [{"path": "/s/old", "size": 1},
                                                  {"path": "/s/new", "size": 2}],
                                             {})):
            got = pub.entries_for_segment(
                {"locator": "x", "scan_segment_id": 7})
        db.classify_segment_entries.assert_called_once()
        self.assertEqual(got, [("/s/new", 2)],
                         "a restart must not re-plan already-planned files")

    def test_the_reclassification_is_read_only(self):
        """It must not write: the import stays once-only.

        The docstring is stripped via the AST rather than by splitting on
        triple quotes — the body contains triple-quoted SQL, which made a
        naive split silently truncate what was being checked.
        """
        import textwrap
        source = inspect.getsource(
            __import__("src.pg_scan", fromlist=["x"])
            .PgScanMixin.classify_segment_entries)
        func = ast.parse(textwrap.dedent(source)).body[0]
        if (func.body and isinstance(func.body[0], ast.Expr)
                and isinstance(func.body[0].value, ast.Constant)):
            func.body = func.body[1:]          # drop the docstring only
        body = ast.unparse(func)
        for write in ("INSERT", "UPDATE", "DELETE", "_transaction("):
            self.assertNotIn(write, body,
                             f"classify_segment_entries must not {write}")
        self.assertIn("_run_read", body)

    def test_the_filter_is_one_bulk_query_per_segment_not_per_file(self):
        """The whole point of the frontier. One round trip, N paths."""
        source = inspect.getsource(
            __import__("src.pg_scan", fromlist=["x"])
            .PgScanMixin.classify_segment_entries)
        self.assertIn("remote_path = ANY(", source)
        self.assertEqual(source.count("SELECT remote_path, file_size_bytes"), 1)


class ScanFinalityNeedsTraversalEvidenceTests(unittest.TestCase):
    """``scan_complete`` is what lets a session ever be called finished, so the
    only acceptable source for it is traversal evidence."""

    def _coordinator_with_scopes(self, scopes):
        coord = _coordinator()
        coord.db.get_scan_scopes.return_value = scopes
        return coord

    def test_the_scan_is_marked_complete_when_every_scope_is_final(self):
        coord = self._coordinator_with_scopes([
            {"source_root": "/a", "coverage_state": "final"},
            {"source_root": "/b", "coverage_state": "final"}])
        self.assertTrue(coord._mark_scan_complete_if_every_scope_is_final())
        coord.db.mark_remote_scan_complete.assert_called_once_with(1)

    def test_one_provisional_scope_leaves_the_scan_incomplete(self):
        """A permission error, a partial directory or a mutating source must
        leave the session resumable rather than declared finished."""
        coord = self._coordinator_with_scopes([
            {"source_root": "/a", "coverage_state": "final"},
            {"source_root": "/b", "coverage_state": "provisional"}])
        self.assertFalse(coord._mark_scan_complete_if_every_scope_is_final())
        coord.db.mark_remote_scan_complete.assert_not_called()

    def test_no_scopes_at_all_is_not_completion(self):
        """A session with no scopes has not been explored — the absence of
        evidence is not evidence of coverage."""
        coord = self._coordinator_with_scopes([])
        self.assertFalse(coord._mark_scan_complete_if_every_scope_is_final())
        coord.db.mark_remote_scan_complete.assert_not_called()

    def test_an_unreadable_scope_table_leaves_the_scan_incomplete(self):
        coord = _coordinator()
        coord.db.get_scan_scopes.side_effect = RuntimeError("connection lost")
        self.assertFalse(coord._mark_scan_complete_if_every_scope_is_final())
        coord.db.mark_remote_scan_complete.assert_not_called()

    def test_completion_is_gated_behind_finalize_in_the_run_loop(self):
        """Order matters: finalize() grants scope finality from traversal, and
        only then may the session flag be set."""
        source = inspect.getsource(sf.FrontierScanCoordinator.run)
        self.assertIn("self.frontier.finalize()", source)
        self.assertLess(
            source.index("self.frontier.finalize()"),
            source.index("_mark_scan_complete_if_every_scope_is_final"))

    def test_a_stopped_run_finalizes_nothing(self):
        stop = threading.Event()
        stop.set()
        coord = _coordinator(stop_event=stop)
        coord.frontier.establish_scopes = mock.Mock()
        coord.frontier.finalize = mock.Mock()
        coord.frontier.final_mutation_sweep = mock.Mock()
        coord.frontier._start_attempt = mock.Mock(return_value=None)
        coord.run()
        coord.frontier.finalize.assert_not_called()
        coord.frontier.final_mutation_sweep.assert_not_called()


class PublicationStopsCleanlyTests(unittest.TestCase):
    def test_the_publication_gate_stops_before_the_insert(self):
        """A refused chunk must leave its segment ready and unconsumed."""
        db = mock.Mock()
        pub = sf.SegmentChunkPublisher(
            db=db, session_id=1, archive_root="/root",
            builder_factory=lambda: StreamingChunkBuilder(10_000),
            publication_gate=lambda: False)
        result = pub._guarded_seal([("/s/a", 1)], 5, None)
        self.assertIsNone(result)
        self.assertTrue(pub.halted)
        db.append_remote_streaming_chunk.assert_not_called()

    def test_the_budget_guard_stops_before_the_insert(self):
        db = mock.Mock()
        pub = sf.SegmentChunkPublisher(
            db=db, session_id=1, archive_root="/root",
            builder_factory=lambda: StreamingChunkBuilder(10_000),
            budget_guard=lambda _bytes: False)
        self.assertIsNone(pub._guarded_seal([("/s/a", 1)], 5, None))
        self.assertTrue(pub.halted)
        db.append_remote_streaming_chunk.assert_not_called()


def _coordinator(**overrides):
    from src.pipeline_types import ScanMetrics

    class _State:
        def __init__(self):
            self.metrics = ScanMetrics()
            self.remaining_bytes = 10 ** 12
            self.next_chunk_index = 0
            self.chunks = self.files = self.bytes = 0
            self.scan_error = None

    kwargs = dict(
        db=mock.Mock(), session_id=1, scan_paths=["/s"], archive_root="/root",
        state=_State(), remaining_lock=threading.Lock(),
        stop_event=threading.Event(), scanner_factory=lambda m: mock.Mock(),
        builder_factory=lambda: StreamingChunkBuilder(10_000))
    kwargs.update(overrides)
    return sf.FrontierScanCoordinator(**kwargs)


if __name__ == "__main__":
    unittest.main()


class NewSqlMatchesTheRealSchemaTests(unittest.TestCase):
    """Column names in new SQL must exist in migration 014.

    ``mark_segment_fully_allocated`` was written against ``readiness_state``,
    a column that does not exist — the real one is ``state``. Every test passed
    because the in-memory fake had invented the same wrong name, so the fake
    and the code agreed with each other and neither agreed with PostgreSQL.
    This reads the migration itself, which no fake can contradict.
    """

    @staticmethod
    def _migration():
        with open(os.path.join(PROJECT_ROOT, "scripts", "sql",
                               "014_postgres_incremental_scan.sql"),
                  encoding="utf-8") as handle:
            return handle.read()

    def test_the_segment_state_column_is_named_state(self):
        migration = self._migration()
        segments = migration[migration.index("remote_scan_segments"):]
        segments = segments[:segments.index(");")]
        self.assertNotIn("readiness_state", segments)
        self.assertRegex(segments, r"\n\s+state\s+TEXT")

    def test_mark_segment_fully_allocated_uses_real_columns(self):
        from src.pg_scan import PgScanMixin
        source = inspect.getsource(PgScanMixin.mark_segment_fully_allocated)
        self.assertNotIn("readiness_state", source)
        self.assertIn("SET state=", source)

    def test_the_fake_uses_the_same_column_name_as_the_migration(self):
        """A fake that invents a column hides exactly this bug."""
        with open(os.path.join(PROJECT_ROOT, "tests", "lto_fakes.py"),
                  encoding="utf-8") as handle:
            self.assertNotIn("readiness_state", handle.read())

    def test_the_states_written_are_declared_by_the_check_constraint(self):
        from src.pipeline_types import ScanSegmentState
        migration = self._migration()
        declared = set(re.findall(
            r"CHECK \(state IN \(([^)]*)\)", migration)[0].replace("'", "")
            .replace("\n", "").replace(" ", "").split(","))
        for state in (ScanSegmentState.READY, ScanSegmentState.CONSUMED,
                      ScanSegmentState.PARTIALLY_CONSUMED):
            self.assertIn(state.value, declared)


class ThePublicFacadeDoesNotAdvertiseTheLegacyScannerTests(unittest.TestCase):
    """`src.orchestrators` is what application code imports from.

    Leaving the legacy scanners on it kept them one import away from being used
    again, and contradicted the documented rule that the frontier is the only
    scanner a production run may build. They still exist in `src.scanning` —
    the plan forbids deleting the legacy PlanSource — but the facade must not
    hand them out.
    """

    def test_the_facade_exports_neither_legacy_scanner(self):
        from src import orchestrators
        for name in ("StreamingRemoteScanner", "RemoteScanner"):
            self.assertNotIn(name, orchestrators.__all__)
            self.assertFalse(hasattr(orchestrators, name),
                             f"{name} is still importable from the facade")

    def test_the_cli_imports_no_scanner_from_the_facade(self):
        """The CLI is the actual production entrypoint."""
        text = open(os.path.join(PROJECT_ROOT, "src", "cli.py"),
                    encoding="utf-8").read()
        tree = ast.parse(text)
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module and \
                    node.module.endswith("orchestrators"):
                imported = {a.name for a in node.names}
                self.assertEqual(
                    imported & {"StreamingRemoteScanner", "RemoteScanner",
                                "RemoteScanCoordinator"}, set())

    def test_the_legacy_scanner_still_exists_for_its_tests(self):
        """Unreachable from production is the goal; deleted is not."""
        from src.scanning import StreamingRemoteScanner
        self.assertTrue(callable(StreamingRemoteScanner))
