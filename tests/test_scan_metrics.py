"""Plan 1 / Task 0.2 — scan telemetry and the scan-model benchmark harness.

Two rules are load-bearing and are asserted here rather than merely documented:

* a metrics failure is **non-fatal** and must not alter scan, tape or session
  state;
* the metric fields carry **no file or directory name**, so they are safe to
  append to the shared ``SUMMARY.csv`` backup row.

Everything here is offline: no SSH, no PostgreSQL, no tape.
"""
import os
import sys
import threading
import unittest
from unittest import mock

from src.pipeline_types import ScanMetrics, StagedChunk, StreamState
from src.reporting import SCAN_METRIC_COLUMNS

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "scripts"))

import benchmark_scan_models as bench          # noqa: E402


# =============================================================================
# A. ScanMetrics
# =============================================================================
class ScanMetricsTests(unittest.TestCase):
    def test_snapshot_exposes_exactly_the_summary_columns(self):
        snapshot = ScanMetrics().snapshot()
        self.assertEqual(sorted(snapshot), sorted(SCAN_METRIC_COLUMNS))

    def test_round_trips_are_counted_separately_from_rows(self):
        """One bulk query over 200k paths is ONE execution, not 200k."""
        metrics = ScanMetrics()
        metrics.note_membership_query(0.5, 200000, duplicates=199000)
        snapshot = metrics.snapshot()
        self.assertEqual(snapshot["scan_membership_query_count"], 1)
        self.assertEqual(snapshot["scan_sql_executions"], 1)
        self.assertEqual(snapshot["scan_sql_rows"], 200000)
        self.assertEqual(snapshot["scan_entries_duplicate"], 199000)

    def test_plan_inserts_are_counted_as_new_entries(self):
        metrics = ScanMetrics()
        metrics.note_plan_insert(0.2, 1500)
        metrics.note_plan_insert(0.1, 500)
        snapshot = metrics.snapshot()
        self.assertEqual(snapshot["scan_plan_insert_calls"], 2)
        self.assertEqual(snapshot["scan_plan_insert_rows"], 2000)
        self.assertEqual(snapshot["scan_entries_new"], 2000)
        self.assertEqual(snapshot["scan_sql_executions"], 2)

    def test_listing_starts_make_replay_visible(self):
        metrics = ScanMetrics()
        for _ in range(3):
            metrics.note_listing_start()
            metrics.note_enumeration(1.0, 10000)
        snapshot = metrics.snapshot()
        self.assertEqual(snapshot["scan_listing_starts"], 3)
        self.assertEqual(snapshot["scan_entries_seen"], 30000)
        self.assertEqual(snapshot["scan_enumeration_seconds"], 3.0)

    def test_discarded_partial_buffer_entries_are_counted(self):
        metrics = ScanMetrics()
        metrics.note_discarded_partial()
        self.assertEqual(metrics.snapshot()["scan_discarded_partial_entries"], 1)

    def test_latency_marks_are_first_write_wins(self):
        metrics = ScanMetrics()
        metrics.mark_first_sealed_chunk()
        first = metrics.snapshot()["scan_seconds_to_first_sealed_chunk"]
        metrics.mark_first_sealed_chunk()
        self.assertEqual(
            metrics.snapshot()["scan_seconds_to_first_sealed_chunk"], first)
        self.assertIsNotNone(first)
        # The other two milestones stay unset until they actually happen.
        self.assertIsNone(
            metrics.snapshot()["scan_seconds_to_first_staged_chunk"])
        self.assertIsNone(
            metrics.snapshot()["scan_seconds_to_first_writer_group"])

    def test_no_snapshot_value_can_carry_a_path(self):
        metrics = ScanMetrics()
        metrics.note_listing_start()
        metrics.note_enumeration(1.5, 12)
        metrics.note_membership_query(0.1, 12, 4)
        metrics.note_plan_insert(0.1, 8)
        metrics.mark_first_sealed_chunk()
        for key, value in metrics.snapshot().items():
            self.assertIsInstance(value, (int, float, type(None)), key)

    def test_negative_and_garbage_inputs_never_raise(self):
        metrics = ScanMetrics()
        metrics.note_enumeration(-5, -5)
        metrics.note_membership_query(None, None, None)
        metrics.note_plan_insert(None, None)
        metrics.note_discarded_partial(-3)
        snapshot = metrics.snapshot()
        self.assertEqual(snapshot["scan_entries_seen"], 0)
        self.assertEqual(snapshot["scan_enumeration_seconds"], 0.0)
        self.assertEqual(snapshot["scan_discarded_partial_entries"], 0)

    def test_counters_are_safe_under_concurrent_threads(self):
        metrics = ScanMetrics()

        def worker():
            for _ in range(500):
                metrics.note_plan_insert(0.0, 1)

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(20)
        self.assertEqual(metrics.snapshot()["scan_plan_insert_rows"], 2000)

    def test_stream_state_carries_its_own_metrics(self):
        state = StreamState()
        self.assertIsInstance(state.metrics, ScanMetrics)
        # Two states never share counters.
        self.assertIsNot(StreamState().metrics, state.metrics)

    def test_staged_chunk_carries_a_scan_snapshot(self):
        chunk = StagedChunk(chunk_index=0, fetch_dir="f", pack_dir="p",
                            metadata=[])
        self.assertEqual(chunk.scan_stats, {})
        chunk.scan_stats = ScanMetrics().snapshot()
        self.assertIn("scan_entries_seen", chunk.scan_stats)


# =============================================================================
# B. Scanner instrumentation is observational only
# =============================================================================
class ScannerInstrumentationTests(unittest.TestCase):
    def test_scanner_accepts_metrics_and_never_fails_on_a_broken_counter(self):
        from src.scanning import StreamingRemoteScanner

        class ExplodingMetrics:
            def note_listing_start(self):
                raise RuntimeError("counter exploded")

            def note_enumeration(self, *_a):
                raise RuntimeError("counter exploded")

        scanner = StreamingRemoteScanner(
            "u", "h", skipped_tracker=mock.MagicMock(), ui=mock.MagicMock(),
            metrics=ExplodingMetrics())
        # _note swallows the failure: a counter must not break a scan.
        scanner._note("note_listing_start")
        scanner._note("note_enumeration", 1.0, 1)
        scanner._note("no_such_method")

    def test_scanner_without_metrics_is_unchanged(self):
        from src.scanning import StreamingRemoteScanner
        scanner = StreamingRemoteScanner(
            "u", "h", skipped_tracker=mock.MagicMock(), ui=mock.MagicMock())
        self.assertIsNone(scanner.metrics)
        scanner._note("note_listing_start")     # no-op, no attribute error


# =============================================================================
# C. Offline listing-replay harness
# =============================================================================
class ListingReplayHarnessTests(unittest.TestCase):
    def setUp(self):
        self.entries = bench.synthetic_listing(4000)
        self.budget = 2 * 1024 ** 3
        self.max_files = 500

    def _run(self, model, restarts=2):
        return model(self.entries, restarts, self.budget, self.max_files,
                     len(self.entries) // 2)

    def test_synthetic_listing_is_deterministic(self):
        self.assertEqual(bench.synthetic_listing(50),
                         bench.synthetic_listing(50))

    def test_listing_parser_accepts_nul_and_newline_framing(self):
        import tempfile
        for separator in (b"\0", b"\n"):
            with tempfile.TemporaryDirectory() as tmp:
                path = os.path.join(tmp, "listing")
                with open(path, "wb") as handle:
                    handle.write(separator.join(
                        [b"100 /vault/a", b"200 /vault/b c", b""]))
                self.assertEqual(bench.parse_listing(path),
                                 [("/vault/a", 100), ("/vault/b c", 200)])

    def test_all_three_models_report_the_four_axes_separately(self):
        for model in bench.MODELS:
            data = self._run(model).as_dict()
            self.assertIn("exploration", data)
            self.assertIn("database_membership", data)
            self.assertIn("replay", data)
            self.assertIn("time_to_first", data)
            # Round trips and rows are never the same figure by construction.
            self.assertIn("sql_executions", data["database_membership"])
            self.assertIn("sql_rows", data["database_membership"])
            self.assertIn("decision_criterion", data)

    def test_root_replay_re_enumerates_and_the_frontier_does_not(self):
        replay = self._run(bench.run_current_root_replay).as_dict()
        frontier = self._run(bench.run_persistent_directory_frontier).as_dict()
        self.assertGreater(replay["replay"]["entries_replayed"], 0)
        self.assertLess(frontier["replay"]["entries_replayed"],
                        replay["replay"]["entries_replayed"])
        # The frontier replays at most the bounded partial directories.
        self.assertLessEqual(frontier["replay"]["directories_replayed"], 2)

    def test_full_scan_seals_nothing_until_the_whole_source_is_listed(self):
        result = self._run(bench.run_full_scan_before_processing).as_dict()
        # Its first sealed chunk cannot precede the complete enumeration...
        self.assertGreaterEqual(
            result["time_to_first"]["first_sealed_chunk_seconds"],
            result["exploration"]["enumeration_seconds"])
        # ...and it publishes no membership queries at all, because there is
        # never a partially-known catalog to compare against.
        self.assertEqual(result["database_membership"]["sql_rows"],
                         result["database_membership"]["plan_insert_rows"])

    def test_every_model_seals_the_same_work_in_the_end(self):
        sealed = {model(self.entries, 2, self.budget, self.max_files,
                        len(self.entries) // 2).chunks_sealed
                  for model in bench.MODELS}
        self.assertEqual(len(sealed), 1, f"models disagree on work: {sealed}")

    def test_membership_filter_is_bulk_not_per_file(self):
        result = self._run(bench.run_current_root_replay).as_dict()
        database = result["database_membership"]
        self.assertLess(database["sql_executions"],
                        result["exploration"]["entries_seen"] / 10,
                        "the harness modelled one round trip per file")


# =============================================================================
# D. Isolated-PostgreSQL benchmark guard
# =============================================================================
class PgBenchmarkGuardTests(unittest.TestCase):
    def test_a_non_test_dsn_is_refused(self):
        for dsn in ("postgresql://u@localhost/lto_archive", "", None):
            with self.assertRaises(SystemExit):
                bench._assert_isolated_dsn(dsn)

    def test_a_named_test_database_is_accepted(self):
        for dsn in ("postgresql://u@localhost/lto_archive_test",
                    "postgresql://u@h/bench_catalog",
                    "postgresql://u@h/scratch1"):
            self.assertIsNone(bench._assert_isolated_dsn(dsn))

    def test_pg_benchmark_is_never_reached_with_a_production_dsn(self):
        with mock.patch.object(bench, "_assert_isolated_dsn",
                               side_effect=SystemExit("blocked")):
            with self.assertRaises(SystemExit):
                bench.run_pg_benchmark("postgresql://u@h/lto_archive", [10])


if __name__ == "__main__":
    unittest.main()
