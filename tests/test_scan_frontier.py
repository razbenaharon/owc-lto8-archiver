"""Plan 1 / Task 1.1 — RemoteScanCoordinator.

The extraction is only worth doing if it is provably behaviour-preserving, so
these tests pin the three properties the nested closures had and the refactor
could quietly have lost:

* **cancellation** — ``src.runtime.CANCEL`` and the caller's stop event end the
  loop at the same points as before;
* **completion signalling** — ``on_finished`` fires on *every* exit path, so
  the pipeline coordinator can never wait forever on a producer that has gone;
* **error propagation** — a scan failure records the session's scan error,
  reaches the caller's callback, and never marks the scan complete.

No SSH, no PostgreSQL, no staging, no tape: every collaborator is injected.
"""
import threading
import unittest
from types import SimpleNamespace
from unittest import mock

from src import runtime as rt
from src import scan_frontier as sf
from src.pipeline_types import StreamState
from src.scan_frontier import (RemoteScanCoordinator, TapeBudgetExceeded,
                               build_legacy_scanner_factory)

TIMEOUT = 20


class FakeDB:
    def __init__(self, pending=(), existing=(), insert_result=None):
        self.pending = list(pending)
        self.existing = set(existing)
        self.insert_result = insert_result
        self.appended = []
        self.membership_queries = []
        self.scan_complete = False
        self.scan_error = None

    def get_pending_chunks(self, session_id):
        return list(self.pending)

    def get_remote_existing_snapshot_paths(self, session_id, paths):
        self.membership_queries.append(list(paths))
        return {p for p in paths if p in self.existing}

    def append_remote_streaming_chunk(self, session_id, chunk_index, rows):
        rows = list(rows)
        self.appended.append((chunk_index, rows))
        if self.insert_result is not None:
            return self.insert_result
        return {"inserted_files": len(rows),
                "inserted_bytes": sum(int(r[3]) for r in rows)}

    def mark_remote_scan_complete(self, session_id):
        self.scan_complete = True

    def mark_remote_scan_error(self, session_id, message):
        self.scan_error = message


class FakeScanner:
    def __init__(self, records=(), raises=None, on_start=None):
        self.records = list(records)
        self.raises = raises
        self.on_start = on_start
        self.scan_paths = None

    def iter_scan(self, scan_paths, stop_evt=None):
        self.scan_paths = list(scan_paths)
        if self.on_start is not None:
            self.on_start()
        if self.raises is not None:
            raise self.raises
        for record in self.records:
            if stop_evt is not None and stop_evt.is_set():
                return
            yield record


def build_coordinator(db, *, records=(), raises=None, on_start=None,
                      remaining_bytes=10 ** 15, budget_bytes=8192,
                      max_files=2, on_budget_exceeded=None,
                      on_scan_error=None, next_chunk_index=0,
                      publication_gate=None):
    """A coordinator with every collaborator injected.

    ``coordinator.published`` records the chunk indices publication announced,
    and ``coordinator.finished`` records that the run signalled completion —
    the two observable outputs the pipeline coordinator consumes.
    """
    state = StreamState(remaining_bytes=remaining_bytes,
                        next_chunk_index=next_chunk_index)
    scanner = FakeScanner(records=records, raises=raises, on_start=on_start)
    published = []
    finished = []
    coordinator = RemoteScanCoordinator(
        db=db,
        session_id=37,
        scan_paths=["/strg/a", "/strg/b"],
        state=state,
        remaining_lock=threading.Lock(),
        stop_event=threading.Event(),
        budget_bytes=budget_bytes,
        alloc_unit=4096,
        padding_factor=1.0,
        max_files=max_files,
        scanner_factory=lambda metrics: scanner,
        on_budget_exceeded=on_budget_exceeded,
        on_scan_error=on_scan_error,
        on_chunk_published=published.append,
        publication_gate=publication_gate,
        on_finished=lambda: finished.append(True),
    )
    coordinator.published = published
    coordinator.finished = finished
    return coordinator, scanner


class _Silent(unittest.TestCase):
    def setUp(self):
        rt.CANCEL.clear()
        self.addCleanup(rt.CANCEL.clear)
        patcher = mock.patch.object(sf, "_status", lambda *a, **k: None)
        patcher.start()
        self.addCleanup(patcher.stop)
        printer = mock.patch("builtins.print", lambda *a, **k: None)
        printer.start()
        self.addCleanup(printer.stop)


# =============================================================================
# A. publish_legacy_chunk
# =============================================================================
class PublishLegacyChunkTests(_Silent):
    def test_membership_filter_is_one_bulk_query_per_chunk(self):
        db = FakeDB()
        coordinator, _ = build_coordinator(db)
        chunk = [(f"/strg/f{i}", 100) for i in range(500)]
        self.assertTrue(coordinator.publish_legacy_chunk(chunk))
        self.assertEqual(len(db.membership_queries), 1)
        self.assertEqual(len(db.membership_queries[0]), 500)
        snapshot = coordinator.state.metrics.snapshot()
        self.assertEqual(snapshot["scan_sql_executions"], 2)   # filter + insert
        self.assertEqual(snapshot["scan_membership_query_count"], 1)

    def test_a_fully_known_chunk_is_dropped_without_an_insert(self):
        db = FakeDB(existing=["/strg/a", "/strg/b"])
        coordinator, _ = build_coordinator(db)
        self.assertTrue(coordinator.publish_legacy_chunk(
            [("/strg/a", 1), ("/strg/b", 2)]))
        self.assertEqual(db.appended, [])
        self.assertEqual(coordinator.state.chunks, 0)
        self.assertEqual(
            coordinator.state.metrics.snapshot()["scan_entries_duplicate"], 2)

    def test_partial_duplicates_publish_only_the_new_paths(self):
        db = FakeDB(existing=["/strg/a"])
        coordinator, _ = build_coordinator(db)
        coordinator.publish_legacy_chunk([("/strg/a", 1), ("/strg/b", 2)])
        (chunk_index, rows), = db.appended
        self.assertEqual(chunk_index, 0)
        self.assertEqual([row[1] for row in rows], ["/strg/b"])
        # (chunk_index, path, basename, size)
        self.assertEqual(rows[0], (0, "/strg/b", "b", 2))

    def test_published_chunk_updates_counters_and_reaches_the_queue(self):
        db = FakeDB()
        coordinator, _ = build_coordinator(db, remaining_bytes=1000)
        self.assertTrue(coordinator.publish_legacy_chunk([("/strg/a", 400)]))
        state = coordinator.state
        self.assertEqual(state.next_chunk_index, 1)
        self.assertEqual(state.chunks, 1)
        self.assertEqual(state.files, 1)
        self.assertEqual(state.bytes, 400)
        self.assertEqual(state.remaining_bytes, 600)
        self.assertEqual(coordinator.published, [0])

    def test_an_insert_that_deduplicates_everything_advances_nothing(self):
        db = FakeDB(insert_result={"inserted_files": 0, "inserted_bytes": 0})
        coordinator, _ = build_coordinator(db)
        self.assertTrue(coordinator.publish_legacy_chunk([("/strg/a", 1)]))
        self.assertEqual(coordinator.state.next_chunk_index, 0)
        self.assertEqual(coordinator.published, [])

    def test_exceeding_the_tape_budget_stops_and_records_the_reason(self):
        db = FakeDB()
        seen = []
        coordinator, _ = build_coordinator(
            db, remaining_bytes=10, on_budget_exceeded=seen.append)
        self.assertFalse(coordinator.publish_legacy_chunk([("/strg/a", 100)]))
        self.assertEqual(len(seen), 1)
        self.assertIn("remains on the mounted tape", seen[0])
        self.assertEqual(db.scan_error, seen[0])
        self.assertEqual(db.appended, [])

    def test_without_a_callback_the_budget_stop_raises(self):
        db = FakeDB()
        coordinator, _ = build_coordinator(db, remaining_bytes=10)
        with self.assertRaises(TapeBudgetExceeded):
            coordinator.publish_legacy_chunk([("/strg/a", 100)])

    def test_a_db_without_the_membership_helper_still_publishes(self):
        class Minimal(FakeDB):
            get_remote_existing_snapshot_paths = None

        db = Minimal()
        del Minimal.get_remote_existing_snapshot_paths
        coordinator, _ = build_coordinator(db)
        self.assertTrue(coordinator.publish_legacy_chunk([("/strg/a", 1)]))
        self.assertEqual(len(db.appended), 1)

    def test_a_stop_during_publication_returns_false(self):
        db = FakeDB()
        coordinator, _ = build_coordinator(db)
        coordinator.stop_event.set()
        self.assertFalse(coordinator.publish_legacy_chunk([("/strg/a", 1)]))

    def test_a_closed_publication_gate_stops_publication(self):
        """Task 1.3: the gate bounds how far exploration runs ahead."""
        db = FakeDB()
        coordinator, _ = build_coordinator(db, publication_gate=lambda: False)
        self.assertFalse(coordinator.publish_legacy_chunk([("/strg/a", 1)]))
        # It refuses BEFORE any database work, so a paused scanner costs
        # nothing.
        self.assertEqual(db.membership_queries, [])
        self.assertEqual(db.appended, [])

    def test_an_open_publication_gate_is_consulted_once_per_chunk(self):
        db = FakeDB()
        calls = []
        coordinator, _ = build_coordinator(
            db, publication_gate=lambda: calls.append(1) or True)
        coordinator.publish_legacy_chunk([("/strg/a", 1)])
        coordinator.publish_legacy_chunk([("/strg/b", 1)])
        self.assertEqual(len(calls), 2)


# =============================================================================
# B. run() — ordering, closure, cancellation, errors
# =============================================================================
class CoordinatorRunTests(_Silent):
    def test_the_resumed_backlog_is_no_longer_pushed_before_exploration(self):
        """Task 1.3: the backlog does NOT pass through the scanner any more.

        The stager reads pending chunks from authoritative status instead, so
        old pending work can never sit in front of renewed exploration.
        """
        db = FakeDB(pending=[4, 5, 6])
        order = []
        coordinator, _ = build_coordinator(
            db, records=[("/strg/new", 10)],
            on_start=lambda: order.append("scan"),
            budget_bytes=10 ** 9, max_files=10 ** 6)
        coordinator.run()
        self.assertEqual(order, ["scan"])
        # Only the newly sealed chunk is announced; 4/5/6 were never touched.
        self.assertEqual(coordinator.published, [0])

    def test_completion_is_signalled_on_a_clean_finish(self):
        db = FakeDB()
        coordinator, _ = build_coordinator(db, records=[("/strg/a", 10)],
                                           budget_bytes=10 ** 9)
        coordinator.run()
        self.assertEqual(coordinator.finished, [True])
        self.assertTrue(db.scan_complete)

    def test_completion_is_signalled_after_a_scan_failure(self):
        db = FakeDB()
        errors = []
        coordinator, _ = build_coordinator(
            db, raises=RuntimeError("ssh died"), on_scan_error=errors.append)
        coordinator.run()
        self.assertEqual(coordinator.finished, [True])
        self.assertEqual([str(e) for e in errors], ["ssh died"])
        self.assertEqual(db.scan_error, "ssh died")
        self.assertFalse(db.scan_complete)

    def test_completion_is_signalled_after_a_budget_stop(self):
        db = FakeDB()
        coordinator, _ = build_coordinator(
            db, records=[("/strg/a", 10 ** 9)], remaining_bytes=10,
            budget_bytes=10 ** 12, on_budget_exceeded=lambda _msg: None)
        coordinator.run()
        self.assertEqual(coordinator.finished, [True])
        self.assertFalse(db.scan_complete)

    def test_without_a_callback_a_scan_failure_propagates(self):
        db = FakeDB()
        coordinator, _ = build_coordinator(db, raises=ValueError("boom"))
        with self.assertRaises(ValueError):
            coordinator.run()
        # ...and completion is STILL signalled on the way out.
        self.assertEqual(coordinator.finished, [True])

    def test_a_stop_event_ends_exploration_without_marking_it_complete(self):
        db = FakeDB()
        coordinator, _ = build_coordinator(db, records=[("/strg/a", 10)],
                                           budget_bytes=10 ** 9)
        coordinator.stop_event.set()
        coordinator.run()
        self.assertFalse(db.scan_complete)
        self.assertEqual(coordinator.finished, [True])

    def test_a_global_cancel_ends_exploration(self):
        db = FakeDB()
        coordinator, _ = build_coordinator(db, records=[("/strg/a", 10)],
                                           budget_bytes=10 ** 9)
        rt.CANCEL.set()
        coordinator.run()
        self.assertFalse(db.scan_complete)

    def test_the_scanner_receives_the_configured_scope_and_metrics(self):
        db = FakeDB()
        captured = {}

        state = StreamState(remaining_bytes=10 ** 12)
        scanner = FakeScanner(records=[])

        def factory(metrics):
            captured["metrics"] = metrics
            return scanner

        coordinator = RemoteScanCoordinator(
            db=db, session_id=37, scan_paths=["/strg/a", "/strg/b"],
            state=state, remaining_lock=threading.Lock(),
            stop_event=threading.Event(), budget_bytes=10 ** 9,
            alloc_unit=4096, padding_factor=1.0, max_files=10,
            scanner_factory=factory)
        coordinator.run()
        self.assertIs(captured["metrics"], state.metrics)
        self.assertEqual(scanner.scan_paths, ["/strg/a", "/strg/b"])

    def test_missing_scanner_factory_is_an_explicit_error(self):
        db = FakeDB()
        errors = []
        coordinator = RemoteScanCoordinator(
            db=db, session_id=37, scan_paths=["/strg"],
            state=StreamState(), remaining_lock=threading.Lock(),
            stop_event=threading.Event(), budget_bytes=1, alloc_unit=1,
            padding_factor=1.0, max_files=1, on_scan_error=errors.append)
        coordinator.run()
        self.assertIn("scanner_factory", str(errors[0]))
        self.assertFalse(db.scan_complete)

    def test_chunk_boundaries_follow_discovery_order_and_the_file_ceiling(self):
        db = FakeDB()
        coordinator, _ = build_coordinator(
            db, records=[(f"/strg/f{i}", 100) for i in range(6)],
            budget_bytes=10 ** 9, max_files=2)
        coordinator.run()
        self.assertEqual([ci for ci, _ in db.appended], [0, 1, 2])
        first_chunk_paths = [row[1] for row in db.appended[0][1]]
        self.assertEqual(first_chunk_paths, ["/strg/f0", "/strg/f1"])

    def test_a_trailing_partial_chunk_is_flushed(self):
        db = FakeDB()
        coordinator, _ = build_coordinator(
            db, records=[(f"/strg/f{i}", 100) for i in range(5)],
            budget_bytes=10 ** 9, max_files=2)
        coordinator.run()
        self.assertEqual(len(db.appended), 3)
        self.assertEqual(len(db.appended[-1][1]), 1)


# =============================================================================
# C. The legacy scanner factory
# =============================================================================
class LegacyScannerFactoryTests(_Silent):
    def test_it_builds_the_current_production_scanner_with_metrics(self):
        metrics = StreamState().metrics
        with mock.patch.object(sf, "StreamingRemoteScanner") as fake:
            factory = build_legacy_scanner_factory(
                remote_user="u", remote_host="h", remote_password="p",
                skipped_tracker="tracker", ui="ui", cipher="aes")
            factory(metrics)
        fake.assert_called_once()
        kwargs = fake.call_args.kwargs
        self.assertIs(kwargs["metrics"], metrics)
        self.assertEqual(kwargs["cipher"], "aes")
        self.assertEqual(fake.call_args.args, ("u", "h"))


# =============================================================================
# D. The orchestrator is wiring, not a second scanner
# =============================================================================
class OrchestratorIsWiringTests(unittest.TestCase):
    def test_the_streaming_session_no_longer_implements_a_scanner(self):
        """The orchestrator delegates discovery; it does not perform it.

        Updated at Plan 1 completion: the coordinator it delegates to is now
        :class:`FrontierScanCoordinator`, not ``RemoteScanCoordinator``. The
        point of the test is unchanged — the orchestrator must not have kept a
        private copy of the scanning logic — so the "left behind" list still
        applies, minus the builder factory the coordinator now legitimately
        receives from it.
        """
        import inspect
        from src.remote_orchestrator import RemoteOrchestrator
        source = inspect.getsource(RemoteOrchestrator._run_streaming_session)
        self.assertIn("FrontierScanCoordinator", source)
        self.assertNotIn("RemoteScanCoordinator", source)
        for gone in ("_scanner_planner", "_append_chunk", "_chunk_rows",
                     "iter_scan("):
            self.assertNotIn(gone, source, f"{gone} was left behind")

    def test_the_coordinator_owns_publication_and_discovery(self):
        for name in ("run", "publish_legacy_chunk"):
            self.assertTrue(hasattr(RemoteScanCoordinator, name))


if __name__ == "__main__":
    unittest.main()
