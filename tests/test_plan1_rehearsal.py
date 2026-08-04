"""Plan 1 / Task 4.3 — end-to-end rehearsal on a synthetic dataset.

Every earlier test file proves one component. This one runs the whole
progression against a single synthetic source and checks the properties that
only appear when the pieces are combined:

* scan, staging and tape writing genuinely OVERLAP — a chunk reaches the writer
  while the scanner is still exploring;
* a restart re-lists only the directory that was mid-listing, end to end;
* an error directory, a single-file scope and an empty directory all survive
  the full path;
* the writer takes ONE ownership period per finite group and touches the drive
  nowhere else;
* and the production feature is still OFF.

The hardware pilot (one finite group against the real drive, plus a local
ZIP/loose restore) cannot run here and must not: it needs the physical drive.
``scripts/plan1_rehearsal.py`` is its runner, and the checklist it prints is the
evidence Plan 3 reviews. What runs here is everything that does not need the
cartridge.
"""
import os
import tempfile
import threading
import time
import unittest
from types import SimpleNamespace
from unittest import mock

from src import ltfs_ownership as own
from src import remote_orchestrator as ro
from src.archive_artifacts import parse_jsonl_zst_artifact
from src.planning import StreamingChunkBuilder
from src.ready_queue import ReadyQueue, ReadyQueueLimits
from src.scan_frontier import (DirectoryFrontierCoordinator,
                               SegmentChunkPublisher)

from lto_fakes import TapeOperationLog
from test_incremental_scan_frontier import FakeSource, FrontierDB
from test_segment_chunk_publication import PublisherDB

GiB = 1024 ** 3

#: A source with every shape that has caused trouble: nested directories, an
#: empty one, an unreadable one, an awkward filename, and a large file.
SYNTHETIC_TREE = {
    "/vault/pilot": {"files": {"root.bin": 1024},
                    "dirs": ["docs", "empty", "denied", "big"]},
    "/vault/pilot/docs": {"files": {"a.txt": 10, "b with space.txt": 20,
                                   "יוניקוד.txt": 30},
                         "dirs": []},
    "/vault/pilot/empty": {"files": {}, "dirs": []},
    "/vault/pilot/denied": {"files": {}, "dirs": []},
    "/vault/pilot/big": {"files": {"large.iso": 8 * GiB}, "dirs": []},
}


class _Rehearsal(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.root = self._dir.name
        self.addCleanup(self._dir.cleanup)
        self.db = FrontierDB()
        self.stop = threading.Event()
        ro.CANCEL.clear()
        self.addCleanup(ro.CANCEL.clear)

    def _coordinator(self, source, scan_paths=("/vault/pilot",), **kwargs):
        return DirectoryFrontierCoordinator(
            db=self.db, session_id=37, scan_paths=list(scan_paths),
            archive_root=self.root, scanner_factory=lambda metrics: source,
            stop_event=self.stop, owner_token="rehearsal", **kwargs)


# =============================================================================
# A. The synthetic dataset survives the whole path
# =============================================================================
class SyntheticDatasetTests(_Rehearsal):
    def test_the_whole_tree_is_covered_once(self):
        source = FakeSource(SYNTHETIC_TREE)
        self._coordinator(source).run()
        self.assertEqual(sorted(source.listed), sorted(SYNTHETIC_TREE))
        for row in self.db.directories:
            self.assertEqual(row["listing_state"], "complete",
                             row["canonical_path"])

    def test_an_empty_directory_is_covered_with_no_segment(self):
        source = FakeSource(SYNTHETIC_TREE)
        self._coordinator(source).run()
        empty = next(d for d in self.db.directories
                     if d["canonical_path"] == "/vault/pilot/empty")
        self.assertEqual(empty["listing_state"], "complete")
        self.assertEqual(empty["direct_file_count"], 0)
        self.assertNotIn(empty["scan_directory_id"],
                         [s["scan_directory_id"] for s in self.db.segments])

    def test_an_unreadable_directory_stops_the_run_and_stays_partial(self):
        source = FakeSource(SYNTHETIC_TREE, unreadable={"/vault/pilot/denied"})
        with self.assertRaises(RuntimeError):
            self._coordinator(source).run()
        denied = next(d for d in self.db.directories
                      if d["canonical_path"] == "/vault/pilot/denied")
        self.assertEqual(denied["listing_state"], "partial")
        self.assertTrue(any(e["category"] == "listing_failed"
                            for e in self.db.errors))

    def test_awkward_filenames_survive_into_the_artifact(self):
        source = FakeSource(SYNTHETIC_TREE)
        self._coordinator(source).run()
        docs = next(d for d in self.db.directories
                    if d["canonical_path"] == "/vault/pilot/docs")
        segment = next(s for s in self.db.segments
                       if s["scan_directory_id"] == docs["scan_directory_id"])
        _header, entries, _totals = parse_jsonl_zst_artifact(
            self.root, segment["locator"])
        names = [os.path.basename(e["path"]) for e in entries]
        self.assertIn("b with space.txt", names)
        self.assertIn("יוניקוד.txt", names)

    def test_a_single_file_scope_is_supported(self):
        from src.pipeline_types import SCOPE_KIND_FILE
        source = FakeSource({"/vault/pilot/one.bin": {"files": {}, "dirs": []}})
        coordinator = self._coordinator(source,
                                        scan_paths=("/vault/pilot/one.bin",))
        coordinator.file_scope_hints = {"/vault/pilot/one.bin"}
        coordinator.establish_scopes()
        self.assertEqual(self.db.scopes[0]["scope_kind"], SCOPE_KIND_FILE)
        # A file scope enqueues no directory to list.
        self.assertEqual(self.db.directories, [])

    def test_a_restart_re_lists_only_the_interrupted_directory(self):
        source = FakeSource(SYNTHETIC_TREE)
        self._coordinator(source, max_directories=2).run()
        listed_first = list(source.listed)
        self.assertEqual(len(listed_first), 2)

        resumed = FakeSource(SYNTHETIC_TREE)
        self._coordinator(resumed).run()
        self.assertEqual(set(listed_first) & set(resumed.listed), set(),
                         "a completed directory was re-enumerated")
        self.assertEqual(sorted(listed_first + resumed.listed),
                         sorted(SYNTHETIC_TREE))


# =============================================================================
# B. Scan -> seal -> write, with real overlap
# =============================================================================
class OverlapRehearsalTests(_Rehearsal):
    def test_a_chunk_reaches_the_writer_while_scanning_continues(self):
        """The property Phase 4 exists for, proved end to end."""
        from src.remote_pipeline import RemotePipelineCoordinator

        events = []
        pending = []
        first_write = threading.Event()

        class Host:
            _producer_err = None
            _ownership_acquisitions = _readiness_checks = 0
            _cartridge_verifications = 0

            def __init__(self, db):
                self.db = db

            def _validate_chunk_file_limit(self, *a, **k):
                pass

            def _await_staging_capacity(self, *a, **k):
                pass

            def _stage_chunk(self, session_id, chunk_index, files):
                from src.pipeline_types import ContainerFormat, StagedChunk
                events.append(("stage", chunk_index))
                return StagedChunk(chunk_index=chunk_index,
                                   fetch_dir=f"/tmp/_f{chunk_index}",
                                   pack_dir=f"/tmp/_p{chunk_index}",
                                   metadata=[], staged_bytes=GiB,
                                   session_id=session_id,
                                   packaging_format=ContainerFormat.ZIP)

            def _write_chunk_group(self, session_id, descs, tape, eject, stop):
                events.append(("write", tuple(d.chunk_index for d in descs)))
                first_write.set()
                for desc in descs:
                    if desc.chunk_index in pending:
                        pending.remove(desc.chunk_index)
                return None

            def _record_fetch_failure_stop(self, *a, **k):
                pass

            def _record_stop(self, result, escalate=False):
                return result

            def _discard_desc(self, desc):
                pass

            def _preserve_desc(self, *a, **k):
                pass

            def _signal_producer_completion(self, queue, stop):
                queue.close()

        class ScanningDB:
            """Chunks appear over time, as a real scan would produce them."""

            def __init__(self):
                self.sealed = []

            def get_chunks_with_status(self, session_id, status):
                return list(pending)

            def get_chunk_size_summary(self, session_id, chunk_index=None):
                return {chunk_index: (1024, 1024, 1)}

            def get_chunk_files(self, session_id, chunk_index):
                return []

        db = ScanningDB()
        host = Host(db)
        queue = ReadyQueue(ReadyQueueLimits(1, 1, 10 ** 12, 48))
        coordinator = RemotePipelineCoordinator(
            host=host, session_id=37, tape_label="Tape_PILOT", ready_q=queue,
            stop_event=threading.Event(), metrics=mock.MagicMock(),
            backlog_limit=64, poll_seconds=0.05)

        def scanner():
            """Seal chunks slowly, and keep going after the first write."""
            for index in range(4):
                events.append(("seal", index))
                pending.append(index)
                if index == 1:
                    # Do not finish scanning until a write has happened.
                    first_write.wait(10)
                time.sleep(0.05)
            coordinator.note_scanner_finished()

        coordinator.scan_coordinator = SimpleNamespace(run=scanner)
        with mock.patch("builtins.print"):
            outcome = coordinator.run()

        kinds = [kind for kind, _ in events]
        self.assertIn("write", kinds)
        first_write_at = kinds.index("write")
        self.assertIn("seal", kinds[first_write_at:],
                      "scanning stopped as soon as the writer started")
        self.assertGreater(outcome.completed_chunks, 0)

    def test_sealed_chunks_come_from_the_artifacts(self):
        source = FakeSource(SYNTHETIC_TREE)
        self._coordinator(source).run()

        publisher_db = PublisherDB()
        for segment in self.db.segments:
            publisher_db.add_segment(
                segment["scan_segment_id"], segment["locator"],
                segment["first_scan_ordinal"], segment["last_scan_ordinal"])
        publisher = SegmentChunkPublisher(
            db=publisher_db, session_id=37, archive_root=self.root,
            builder_factory=lambda: StreamingChunkBuilder(
                10 ** 12, alloc_unit=1, padding_factor=1.0, max_files=2))
        sealed = publisher.publish_ready_segments(0)

        self.assertTrue(sealed)
        every_path = [row[1] for _ci, rows in publisher_db.appended
                      for row in rows]
        self.assertEqual(len(every_path), len(set(every_path)),
                         "a path was planned into two chunks")
        # Every sealed chunk carries its expectation.
        for chunk in publisher_db.sealed:
            self.assertGreater(chunk["expected_file_count"], 0)
            self.assertGreaterEqual(chunk["expected_bytes"], 0)


# =============================================================================
# C. The finite group, with ownership observed
# =============================================================================
class FiniteGroupRehearsalTests(unittest.TestCase):
    def test_one_ownership_period_covers_the_whole_group(self):
        from src.pipeline_types import StagedChunk
        from src.remote_writer import RemoteChunkWriter

        log = TapeOperationLog()
        orch = ro.RemoteOrchestrator.__new__(ro.RemoteOrchestrator)
        orch.cfg = SimpleNamespace(lto_drive="X:\\", backup_log_dir=None,
                                   ibm_eject_cmd="")
        orch.notifier = None
        orch.governor = None
        orch.remote_host = "srv02.example"
        orch.remote_session_path = "/vault/pilot"
        orch.skipped_tracker = mock.MagicMock()
        orch.db = mock.MagicMock()
        orch.db.get_chunk_size_summary.return_value = {}
        from src.pipeline_types import ContainerFormat
        orch.db.get_chunk_packaging_format.return_value = ContainerFormat.ZIP
        orch._consumer_chunk = None
        orch._ownership_acquisitions = 0
        orch._readiness_checks = 0
        orch._cartridge_verifications = 0
        orch._staged_lock = threading.RLock()
        orch._staged_bytes = 0
        orch._stop_lock = threading.Lock()
        orch._stop_result = None
        orch._cleanup_dir = lambda *_a: None
        orch._preserve_desc = mock.MagicMock()
        orch._discard_desc = mock.MagicMock()
        orch._ensure_remote_chunk_fits_tape = lambda *a, **k: True

        def gate(session_id, desc, tape_label, stop_pipeline):
            log.record("gate")
            return None
        orch._pre_write_safety_gate = gate

        class Writer:
            def eject_tape(self, drive):
                log.record("eject")

            def run(self, **kwargs):
                log.record("write", chunk=kwargs["remote_chunk_index"])
                kwargs["on_write_start"]()
        orch._backup_writer = lambda cls=None: Writer()

        descs = [StagedChunk(chunk_index=i, fetch_dir=f"/tmp/_f{i}",
                             pack_dir=f"/tmp/_p{i}", metadata=[],
                             staged_bytes=GiB, source_missing_files=[],
                             session_id=37,
                             packaging_format=ContainerFormat.ZIP)
                 for i in range(5)]

        generation_before = own.OWNERSHIP.generation
        with mock.patch("builtins.print"):
            block = RemoteChunkWriter(orch)._write_chunk_group(
                37, descs, "Tape_PILOT", False, threading.Event())

        self.assertIsNone(block)
        # ONE gate, FIVE writes, ZERO ejects, ONE physical ownership entry.
        self.assertEqual(log.count("gate"), 1)
        self.assertEqual(log.count("write"), 5)
        self.assertEqual(log.count("eject"), 0)
        self.assertEqual(own.OWNERSHIP.generation - generation_before, 1)
        self.assertFalse(own.owns_ltfs())
        self.assertEqual(orch._ownership_acquisitions, 1)


# =============================================================================
# D. The frontier is the fixed production scanner
# =============================================================================
class ProductionGateTests(unittest.TestCase):
    def test_shipped_config_has_no_incremental_scan_feature_flag(self):
        import configparser
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        parser = configparser.ConfigParser()
        parser.read(os.path.join(root, "config.example.ini"), encoding="utf-8")
        self.assertFalse(parser.has_option("REMOTE", "incremental_scan"))

    def test_config_manager_exposes_no_incremental_scan_feature_flag(self):
        from src.config import ConfigManager
        self.assertFalse(hasattr(ConfigManager, "incremental_scan_enabled"))

    def test_migration_014_is_not_applied_at_startup(self):
        import inspect
        from src.pg_core import PgConnectionCore
        startup = inspect.getsource(PgConnectionCore._init_schema)
        self.assertNotIn("014", startup)

    def test_stored_tar_creation_is_still_disabled(self):
        """Plan 2 Phase 0 exposes truth but cannot create a TAR yet."""
        import configparser
        import inspect
        from src.pg_containers import stored_tar_reader_contract_version
        from src.pg_sessions import PgSessionMixin

        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        parser = configparser.ConfigParser()
        parser.read(os.path.join(root, "config.example.ini"), encoding="utf-8")
        self.assertFalse(parser.getboolean(
            "FEATURES", "stored_tar_write_enabled"))
        self.assertIsNone(stored_tar_reader_contract_version())
        assignment = inspect.getsource(
            PgSessionMixin.append_remote_streaming_chunk)
        self.assertIn("if stored_tar_write_enabled", assignment)
        with open(os.path.join(root, "src", "remote_staging.py"),
                  encoding="utf-8") as handle:
            staging = handle.read()
        self.assertIn("producer is not implemented until Plan 2 Phase 2", staging)

    def test_no_postgresql_pruning_happens_in_plan_1(self):
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        for name in ("scan_frontier.py", "pg_scan.py", "frontier_bootstrap.py",
                     "startup_reconcile.py", "archive_artifacts.py"):
            with open(os.path.join(root, "src", name), encoding="utf-8") as h:
                source = h.read()
            for pruning in ("DELETE FROM remote_snapshot_files",
                            "DELETE FROM remote_plan_files",
                            "DELETE FROM files_index", "TRUNCATE"):
                self.assertNotIn(pruning, source, f"{name}: {pruning}")


if __name__ == "__main__":
    unittest.main()
