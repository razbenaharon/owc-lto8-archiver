"""Plan 1 / Task 0.1 — characterization of the CURRENT remote pipeline.

These tests freeze the behavioural contract before Plan 1 refactors it. They
assert what the code does **today**, including the parts Plan 1 intends to
change; a test here failing after a refactor is the signal to update the test
*and* the map in ``src/remote_orchestrator``'s module docstring in the same
commit.

Nothing here reaches staging, PostgreSQL, LTFS or SSH: the scanner, the stager,
the writer and the database are all fakes, and every blocking wait is bounded.
"""
import inspect
import os
import re
import tempfile
import threading
import time
import unittest
from types import SimpleNamespace
from unittest import mock

from src import remote_orchestrator as ro
from src import scan_frontier as sf
from src.exit_codes import ExitCode
from src.pipeline_types import ContainerFormat, StagedChunk
from src.ready_queue import ReadyQueueLimits

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TIMEOUT = 20            # every blocking assertion in this module is bounded


# =============================================================================
# Fakes
# =============================================================================
from lto_fakes import FakeFrontierCatalog


class FakeStreamingDB(FakeFrontierCatalog):
    """In-memory stand-in for the PostgreSQL session/chunk API.

    Only the calls ``_run_streaming_session`` makes are implemented, and each
    one records itself so ordering can be asserted.

    Since Plan 1 completion the streaming session drives the DIRECTORY
    FRONTIER rather than a whole-root scan, so this inherits
    :class:`~tests.lto_fakes.FakeFrontierCatalog` for the migration-014 half
    of the API: schema readiness, scopes, the directory queue, ready segments
    and segment-range consumption.
    """

    def __init__(self, pending=(), tape_label="Tape_TEST", scan_complete=False):
        super().__init__()
        self.calls = []                     # ordered method-name log
        self.pending = list(pending)
        self.tape_label = tape_label
        self.scan_complete = scan_complete
        self.appended = []                  # (chunk_index, rows)
        self.existing_paths_queries = []    # each element is one bulk query
        self.statuses = {}
        self.session_updates = []
        self.scan_error = None
        self._next_index = len(self.pending)

    # -- session ---------------------------------------------------------
    def get_remote_session(self, session_id):
        self.calls.append("get_remote_session")
        return {
            "session_id": session_id,
            "session_label": f"REMOTE_fake_{session_id}",
            "tape_label": self.tape_label,
            "scan_complete": self.scan_complete,
            "tape_generation": 1,
        }

    def update_remote_session(self, session_id, **kwargs):
        self.session_updates.append(kwargs)

    def mark_remote_scan_complete(self, session_id):
        self.calls.append("mark_remote_scan_complete")
        self.scan_complete = True

    def mark_remote_scan_error(self, session_id, message):
        self.calls.append("mark_remote_scan_error")
        self.scan_error = message

    # -- chunks ----------------------------------------------------------
    def get_pending_chunks(self, session_id):
        self.calls.append("get_pending_chunks")
        return list(self.pending)

    def count_chunks(self, session_id):
        return self._next_index

    def get_next_remote_chunk_index(self, session_id):
        return self._next_index

    def get_chunk_size_summary(self, session_id, chunk_index=None):
        if chunk_index is None:
            return {ci: (1024, 1024, 1) for ci in self.pending}
        return {chunk_index: (1024, 1024, 1)}

    def get_chunk_files(self, session_id, chunk_index):
        return [{"remote_path": f"/src/f{chunk_index}", "file_size_bytes": 1024}]

    def get_chunks_with_status(self, session_id, status):
        return [ci for ci, st in self.statuses.items() if st == status]

    def update_chunk_status(self, session_id, chunk_index, status):
        self.statuses[chunk_index] = status

    def get_remote_existing_snapshot_paths(self, session_id, paths):
        # Deliberately BULK: one query per chunk, never one per file.
        self.existing_paths_queries.append(list(paths))
        return set()

    def append_remote_streaming_chunk(
            self, session_id, chunk_index, rows, *,
            stored_tar_write_enabled=False, reader_contract_version=None,
            require_container_format_schema=False):
        self.calls.append("append_remote_streaming_chunk")
        rows = list(rows)
        self.appended.append((chunk_index, rows))
        if chunk_index >= self._next_index:
            self._next_index = chunk_index + 1
        self.statuses.setdefault(chunk_index, "pending")
        return {
            "inserted_files": len(rows),
            "inserted_bytes": sum(int(r[3]) for r in rows),
        }

    def get_chunk_packaging_format(self, session_id, chunk_index):
        return ContainerFormat.ZIP

    # -- tape ------------------------------------------------------------
    def get_tape(self, label):
        return {"total_capacity": 12000, "current_generation": 1,
                "status": "active"}

    def recalculate_tape_used_space(self, label):
        return 0

    def get_pending_remote_reserved_bytes(self, session_id):
        return 0


class FakeScanner:
    """Stands in for ``StreamingRemoteScanner``.

    ``records`` is the file list to emit; ``on_start`` and ``per_record`` are
    hooks so a test can observe *when* exploration begins relative to the
    resumed-chunk backlog.
    """

    instances = []

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        FakeScanner.instances.append(self)

    # class-level injection points (set by the harness)
    records = ()
    on_start = None
    per_record = None

    def iter_scan(self, scan_paths, stop_evt=None):
        if FakeScanner.on_start is not None:
            FakeScanner.on_start()
        for path, size in FakeScanner.records:
            if stop_evt is not None and stop_evt.is_set():
                return
            if FakeScanner.per_record is not None:
                FakeScanner.per_record(path, size)
            yield path, size


class _FakeSentinel:
    def __init__(self, *args, **kwargs):
        pass

    def start(self):
        return self

    def stop(self):
        return None


def build_streaming_orchestrator(db, *, prefetch_ahead=1, chunk_budget=4096,
                                 chunk_max_files=1000, min_start_bytes=1):
    """A ``RemoteOrchestrator`` wired for ``_run_streaming_session`` with fakes.

    Every device-, network- and database-touching collaborator is replaced;
    what remains real is the scheduling itself (``_scanner_planner``,
    ``_stager``, the ready-queue writer loop) — which is exactly what these
    tests characterize.
    """
    orch = ro.RemoteOrchestrator.__new__(ro.RemoteOrchestrator)
    orch.db = db
    orch.cfg = SimpleNamespace(
        lto_drive="X:\\", backup_log_dir=None, ibm_eject_cmd="",
        eject_after_session=False, allow_resume_oversized_chunks=False,
        windows_update_block_on_pending_reboot=False,
        # Where scan-segment artifacts go. A temp dir, never staging and
        # never the LTFS mount (the autouse conftest guard would block a
        # real drive letter anyway).
        local_manifest_archive_root=tempfile.mkdtemp(
            prefix="char_artifacts_"))
    orch.ui = mock.MagicMock()
    orch.notifier = None
    orch.skipped_tracker = mock.MagicMock()
    orch.remote_host = "host.example"
    orch.remote_user = "u"
    orch.remote_password = ""
    orch.remote_path = "/strg"
    orch.remote_scan_paths = ["/strg"]
    orch.remote_session_path = "/strg"
    orch.staging_dir = os.path.join(PROJECT_ROOT, "_never_created_")
    orch.staging_padding = 1.0
    orch.staging_max_bytes = 10 ** 12
    orch.prefetch_ahead = prefetch_ahead
    orch.chunk_max_files = chunk_max_files
    orch.ssh_cipher = ""
    orch.fetch_cores = None
    orch.governor = None
    orch.ready_limits = ReadyQueueLimits(
        min_start_bytes, min_start_bytes, 10 ** 12, 48)
    orch._staged_bytes = 0
    orch._staged_lock = threading.Lock()
    orch._stop_lock = threading.Lock()
    orch._stop_result = None
    orch._producer_err = None
    orch._last_fetch_failure = None
    orch._staging_pressure_active = False
    orch._ownership_acquisitions = 0
    orch._readiness_checks = 0
    orch._cartridge_verifications = 0

    # -- collaborators stubbed out (no device, no network, no DB engine) --
    orch._assert_ownership_preflight = lambda *a, **k: None
    orch._assert_feature_gate = lambda *a, **k: None
    orch._verify_session_tape_generation = lambda *a, **k: None
    orch._detect_prior_backing_chunks = lambda *a, **k: None
    orch._verify_mounted_cartridge = lambda *a, **k: None
    orch._validate_ltfs_sync_mode = lambda: True
    orch._chunk_budget = lambda: chunk_budget
    orch._start_pipeline_heartbeat = lambda *a, **k: None
    orch._build_observation_worker = lambda: None
    orch._await_staging_capacity = lambda *a, **k: None
    orch._finalize = lambda result, phase="pipeline": result
    orch._preserve_desc = mock.MagicMock()
    orch._discard_desc = mock.MagicMock()
    orch._remote_tape_capacity_context = lambda label, session_id=None: {
        "used_bytes": 0, "capacity_bytes": 10 ** 15,
        "reserved_bytes": 0, "available_bytes": 10 ** 15}

    orch.written_groups = []

    def _write_group(session_id, descs, tape_label, eject_after, stop_pipeline):
        orch.written_groups.append([d.chunk_index for d in descs])
        for d in descs:
            db.update_chunk_status(session_id, d.chunk_index, "done")
            if d.chunk_index in db.pending:
                db.pending.remove(d.chunk_index)
        return None
    orch._write_chunk_group = _write_group

    def _stage(session_id, chunk_index, chunk_files):
        return StagedChunk(chunk_index=chunk_index,
                           fetch_dir=f"/tmp/_fetch_{chunk_index}",
                           pack_dir=f"/tmp/_pack_{chunk_index}",
                           metadata=[], staged_bytes=1024,
                           session_id=session_id,
                           packaging_format=ContainerFormat.ZIP)
    orch._stage_chunk = _stage
    return orch


def _shout(msg, *a, **k):
    import traceback; traceback.print_exc()


class FakeDirectoryScanner:
    """A directory scanner over the flat record list these tests supply.

    Since Plan 1 completion the streaming session drives the DIRECTORY frontier,
    so the collaborator to fake is ``DirectoryFrontierScanner.list_directory``
    rather than the whole-root ``iter_scan``.

    The records are modelled as **one file per subdirectory** rather than as a
    single flat listing. That matters: publication happens after each directory,
    so a one-directory tree would finish the whole scan before the writer saw
    anything, and the interleaving these tests exist to observe would be
    invisible. A tree of N leaf directories reproduces the real shape — traverse
    a little, publish a little, while staging and writing run behind.
    """

    instances = []
    records = ()
    on_start = None
    per_record = None
    listed = []

    def __init__(self, *args, **kwargs):
        FakeDirectoryScanner.instances.append(self)
        self.metrics = kwargs.get("metrics")

    @staticmethod
    def _child(index):
        return f"/src/_d{index}"

    def list_directory(self, path):
        from src.scanning import DirectoryListing

        if FakeDirectoryScanner.on_start is not None:
            FakeDirectoryScanner.on_start()
        FakeDirectoryScanner.listed.append(path)
        records = FakeDirectoryScanner.records

        # A directory already listed once has nothing new: re-listing must not
        # re-emit entries (that would be replay, which the frontier forbids).
        if FakeDirectoryScanner.listed.count(path) > 1:
            return DirectoryListing(path, [], [], [], "obs:1", 0)

        for index, record in enumerate(records):
            if path == self._child(index):
                if FakeDirectoryScanner.per_record is not None:
                    FakeDirectoryScanner.per_record(*record)
                return DirectoryListing(
                    path, [(record[0], record[1])], [], [], "obs:1", 0)

        # The root: one child directory per record, no files of its own.
        children = [self._child(i) for i in range(len(records))]
        return DirectoryListing(path, [], children, [], "obs:1", 0)

    def observe(self, path):
        """The final mutation sweep re-reads this token before finality.

        Returning the SAME token the listing carried means "unchanged", so
        coverage may become final — which is what these tests want. A test that
        wants an invalidation returns something else.
        """
        return "obs:1"


class StreamingHarness:
    """Patches the module-level collaborators ``_run_streaming_session`` uses."""

    def __init__(self, records=(), on_scan_start=None, per_record=None):
        self.records = tuple(records)
        self.on_scan_start = on_scan_start
        self.per_record = per_record
        self._patches = []

    def __enter__(self):
        FakeScanner.instances = []
        FakeScanner.records = self.records
        FakeScanner.on_start = self.on_scan_start
        FakeScanner.per_record = self.per_record
        FakeDirectoryScanner.instances = []
        FakeDirectoryScanner.listed = []
        FakeDirectoryScanner.records = self.records
        FakeDirectoryScanner.on_start = self.on_scan_start
        FakeDirectoryScanner.per_record = self.per_record
        self._patches = [
            # Task 1.1 moved discovery into src.scan_frontier, so the scanner
            # must be patched where it is USED, not on the facade.
            mock.patch.object(sf, "StreamingRemoteScanner", FakeScanner),
            # The production scanner since Plan 1 completion.
            mock.patch.object(sf, "DirectoryFrontierScanner",
                              FakeDirectoryScanner),
            mock.patch.object(ro, "RebootSentinel", _FakeSentinel),
            mock.patch.object(ro, "_ensure_lto_drive_ready", return_value=True),
            mock.patch.object(ro, "_volume_cluster_size", return_value=4096),
            mock.patch.object(ro, "send_best_effort", lambda *a, **k: None),
            mock.patch.object(ro, "_phase", lambda *a, **k: None),
            mock.patch.object(ro, "_status", lambda *a, **k: None),
            mock.patch.object(sf, "_status", lambda *a, **k: None),
            mock.patch.object(sf.get_logger(), "exception", _shout),
            mock.patch("builtins.print", lambda *a, **k: None),
        ]
        for patch in self._patches:
            patch.start()
        ro.CANCEL.clear()
        return self

    def __exit__(self, *exc):
        for patch in reversed(self._patches):
            patch.stop()
        FakeScanner.on_start = None
        FakeScanner.per_record = None
        FakeScanner.records = ()
        ro.CANCEL.clear()
        return False


# =============================================================================
# A. Entry flows
# =============================================================================
class EntryFlowCharacterizationTests(unittest.TestCase):
    def test_cli_entry_points_reach_the_orchestrator_run(self):
        """Both documented entry flows end at ``RemoteOrchestrator.run``."""
        from src import cli
        interactive = inspect.getsource(cli.run_remote_archiver)
        self.assertIn("RemoteOrchestrator", interactive)
        self.assertIn(".run(", interactive)
        # The headless entry does not build a second orchestrator: it reuses
        # the interactive one with non_interactive=True.
        headless = inspect.getsource(cli.run_remote_archiver_headless)
        self.assertIn("run_remote_archiver(", headless)
        self.assertIn("non_interactive=True", headless)
        self.assertIn("run_remote_archiver", inspect.getsource(cli.main))

    def test_incomplete_scan_resume_is_delegated_to_the_streaming_loop(self):
        """``_run_session`` owns only scan-COMPLETE sessions."""
        orch = ro.RemoteOrchestrator.__new__(ro.RemoteOrchestrator)
        orch.db = mock.MagicMock()
        orch.db.get_remote_session.return_value = {
            "tape_label": "Tape_TEST", "scan_complete": False}
        sentinel = object()
        orch._run_streaming_session = lambda sid: sentinel

        self.assertIs(orch._run_session(37), sentinel)
        # Delegation happens before ANY preflight, pending-chunk or device work.
        orch.db.get_pending_chunks.assert_not_called()

    def test_headless_without_resume_never_prompts(self):
        orch = ro.RemoteOrchestrator.__new__(ro.RemoteOrchestrator)
        orch.db = mock.MagicMock()
        orch._finalize = lambda result, phase="pipeline": result
        with mock.patch("builtins.input", side_effect=AssertionError("prompted")):
            result = orch._run_non_interactive(resume=False)
        self.assertEqual(result.exit_code, ExitCode.FATAL_CONFIG)
        orch.db.list_active_remote_sessions.assert_not_called()


# =============================================================================
# B. New-session creation
# =============================================================================
class NewSessionCharacterizationTests(unittest.TestCase):
    def test_new_session_creates_a_growable_streaming_plan(self):
        source = inspect.getsource(ro.RemoteOrchestrator._start_new_session)
        self.assertIn("create_remote_streaming_session", source)
        # ...and never the fixed-plan constructor.
        self.assertNotIn("create_remote_session_with_plan", source)

    def test_streaming_session_creation_marks_the_scan_incomplete(self):
        from src.pg_sessions import PgSessionMixin
        source = inspect.getsource(
            PgSessionMixin.create_remote_streaming_session)
        self.assertIn("remote_snapshots", source)
        self.assertIn("remote_plans", source)
        self.assertIn("scan_complete=FALSE", source)


# =============================================================================
# C. Scanner / planner scheduling
# =============================================================================
class ScannerPlannerCharacterizationTests(unittest.TestCase):
    def test_a_resumed_backlog_cannot_starve_renewed_exploration(self):
        """Task 1.3 fairness gate.

        Before the coordinator, the scanner pushed every resumed pending chunk
        through a bounded hand-off queue *before* exploring, so a large backlog
        plus a slow stager postponed looking at the source indefinitely. Now
        work selection is authoritative and the backlog never passes through
        the scanner: exploration starts while the stager is still parked on the
        very first backlog chunk.
        """
        db = FakeStreamingDB(pending=[0, 1, 2, 3])
        scan_started = threading.Event()
        release_stager = threading.Event()
        staged = []

        orch = build_streaming_orchestrator(db, prefetch_ahead=1)

        def blocking_stage(session_id, chunk_index, chunk_files):
            staged.append(chunk_index)
            release_stager.wait(TIMEOUT)
            return StagedChunk(chunk_index=chunk_index,
                               fetch_dir=f"/tmp/_fetch_{chunk_index}",
                               pack_dir=f"/tmp/_pack_{chunk_index}",
                               metadata=[], staged_bytes=1024,
                               session_id=session_id,
                               packaging_format=ContainerFormat.ZIP)
        orch._stage_chunk = blocking_stage

        try:
            with StreamingHarness(records=[("/vault/new", 10)],
                                  on_scan_start=scan_started.set):
                runner = threading.Thread(
                    target=orch._run_streaming_session, args=(37,), daemon=True)
                runner.start()
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline and not staged:
                    time.sleep(0.02)
                self.assertTrue(staged, "the stager never picked up the backlog")
                # THE POINT: the stager is parked inside chunk 0 with three more
                # backlog chunks unstaged, and exploration still runs.
                self.assertTrue(
                    scan_started.wait(5),
                    "exploration was starved behind the resumed backlog")
                release_stager.set()
                runner.join(TIMEOUT)
                self.assertFalse(runner.is_alive())
        finally:
            release_stager.set()

    def test_publication_pauses_at_the_backlog_limit_and_resumes(self):
        """The limit is a bound on how far the scanner runs ahead, not a
        barrier that must fully drain."""
        from src.remote_pipeline import RemotePipelineCoordinator

        db = FakeStreamingDB(pending=[0, 1, 2])
        for ci in (0, 1, 2):
            db.statuses[ci] = "pending"
        host = SimpleNamespace(db=db)
        coordinator = RemotePipelineCoordinator(
            host=host, session_id=37, tape_label="T",
            ready_q=mock.MagicMock(), stop_event=threading.Event(),
            metrics=mock.MagicMock(), backlog_limit=3)

        self.assertEqual(coordinator.sealed_but_unstaged(), 3)
        # At the limit the gate holds; it does not return False (that means
        # "stop"), it simply waits until the backlog falls.
        blocked = threading.Event()

        def gate():
            blocked.set()
            return coordinator.publication_gate()

        waiter = threading.Thread(target=gate, daemon=True)
        waiter.start()
        self.assertTrue(blocked.wait(5))
        time.sleep(0.2)
        self.assertTrue(waiter.is_alive(), "the gate did not pause at the limit")
        # One chunk taken by the stager is enough to admit the scanner again.
        coordinator.next_chunk_to_stage()
        waiter.join(10)
        self.assertFalse(waiter.is_alive())

    def test_an_unreadable_backlog_holds_publication(self):
        """An unknown backlog must never be treated as an empty one."""
        from src.remote_pipeline import RemotePipelineCoordinator

        class Exploding:
            def get_chunks_with_status(self, session_id, status):
                raise RuntimeError("connection lost")

        coordinator = RemotePipelineCoordinator(
            host=SimpleNamespace(db=Exploding()), session_id=37,
            tape_label="T", ready_q=mock.MagicMock(),
            stop_event=threading.Event(), metrics=mock.MagicMock(),
            backlog_limit=4)
        self.assertEqual(coordinator.sealed_but_unstaged(), 4)
        coordinator.stop_event.set()
        self.assertFalse(coordinator.publication_gate())

    def test_chunks_reach_the_writer_before_the_scan_completes(self):
        """Acceptance trace: publication precedes completion, and prepared
        chunks are written while scanning is still running."""
        db = FakeStreamingDB(pending=[])
        events = []
        first_group = threading.Event()

        orch = build_streaming_orchestrator(
            db, prefetch_ahead=2, chunk_budget=1024, chunk_max_files=1)

        def _write_group(session_id, descs, tape_label, eject_after, stop_evt):
            events.append(("write", tuple(d.chunk_index for d in descs)))
            first_group.set()
            for d in descs:
                db.update_chunk_status(session_id, d.chunk_index, "done")
            return None
        orch._write_chunk_group = _write_group

        def per_record(path, size):
            events.append(("scan", path))
            # Give the writer a chance to consume what has been published.
            if path.endswith("2"):
                first_group.wait(TIMEOUT)

        records = [(f"/src/f{i}", 900) for i in range(6)]
        with StreamingHarness(records=records, per_record=per_record):
            # CHANGED BY PLAN 1 COMPLETION: scan finality is no longer a single
            # `mark_remote_scan_complete` call at the end of a whole-root walk.
            # The frontier finalizes each SCOPE, and only after its directories
            # are terminal and the mutation sweep agrees. The trace hook
            # therefore watches scope finalization instead.
            hook = db.finalize_scan_scope

            def marked(scan_scope_id):
                events.append(("scan_complete", None))
                return hook(scan_scope_id)
            db.finalize_scan_scope = marked
            result = orch._run_streaming_session(37)

        kinds = [k for k, _ in events]
        self.assertIn("write", kinds, "no group ever reached the writer")
        self.assertIn("scan_complete", kinds)
        self.assertLess(kinds.index("write"), kinds.index("scan_complete"),
                        "the writer only ran after the scan completed")
        self.assertLess(kinds.index("scan"), kinds.index("write"))
        self.assertEqual(result.exit_code, ExitCode.COMPLETED)

    def test_scanner_runs_find_from_every_configured_root_each_run(self):
        """The production scanner re-enumerates whole roots; there is no
        persisted position to resume from."""
        from src.scanning import StreamingRemoteScanner
        source = inspect.getsource(StreamingRemoteScanner._iter_scan_one)
        self.assertRegex(source, r"find \{shlex\.quote\(scan_path\)\} -type f")
        self.assertIn(r"-printf '%s %p\\0'", source)
        # iter_scan loops over EVERY configured root, unconditionally.
        loop = inspect.getsource(StreamingRemoteScanner.iter_scan)
        self.assertIn("for scan_path in scan_paths", loop)
        self.assertNotIn("resume", loop.lower())

    def test_membership_filtering_is_bulk_and_happens_BEFORE_boundary_choice(self):
        """THE defect Plan 1 completion fixed. Read the old name in git.

        This test used to be called
        ``..._happens_after_boundary_choice`` and it PINNED THE BUG: with the
        legacy scanner, ``publish_legacy_chunk`` filtered already-planned paths
        only once a chunk had been sealed, so a rediscovered file had already
        moved the ``StreamingChunkBuilder`` boundary before being dropped. A
        resumed scan therefore produced different boundaries from the original
        run, for the same source.

        The frontier makes the ordering structural instead of incidental:
        ``SegmentChunkPublisher.entries_for_segment`` reconciles a whole segment
        in ONE set-based query and hands the builder only the genuinely new
        entries. There is no code path in which a known file reaches
        ``builder.add()``.

        Both halves of the original claim are still asserted — the filter is
        bulk (one query per segment, not one per file), and the boundary is
        decided by survivors only — but the second half is now asserted with the
        opposite expectation, which is the point.
        """
        db = FakeStreamingDB(pending=[])
        # Every scanned path is ALREADY planned, so all of them must be
        # filtered out before the builder ever sees them.
        db.legacy_snapshot = {f"/src/f{i}": 100 for i in range(6)}

        orch = build_streaming_orchestrator(
            db, chunk_budget=8192, chunk_max_files=2)
        records = [(f"/src/f{i}", 100) for i in range(6)]
        with StreamingHarness(records=records):
            orch._run_streaming_session(37)

        # Nothing was appended: every entry was already planned.
        self.assertEqual(db.appended, [])
        # ...and NO chunk was sealed either, because the builder was never
        # given a single entry. Under the old order six known files would have
        # driven three boundary decisions before being dropped.
        self.assertFalse(db.sealed_chunks)
        # The reconciliation is once per SEGMENT (one per listed directory),
        # not once per file: six directories, six imports, six files.
        self.assertEqual([s["legacy_import_state"] for s in db.segments],
                         ["imported"] * 6)

    def test_only_unplanned_entries_move_the_chunk_boundary(self):
        """The positive case: known files are invisible to the builder.

        Six files, three of them already planned. The boundary must fall
        exactly where it would if only the three new ones had ever existed.
        """
        db = FakeStreamingDB(pending=[])
        db.legacy_snapshot = {"/src/f0": 100, "/src/f2": 100, "/src/f4": 100}

        orch = build_streaming_orchestrator(
            db, chunk_budget=10 ** 9, chunk_max_files=2)
        records = [(f"/src/f{i}", 100) for i in range(6)]
        with StreamingHarness(records=records):
            orch._run_streaming_session(37)

        planned = [path for _index, rows in db.appended for (_ci, path, _n, _s)
                   in rows]
        self.assertEqual(sorted(planned),
                         ["/src/f1", "/src/f3", "/src/f5"])

    def test_publication_pauses_at_the_backlog_limit_and_resumes(self):
        """The limit is a bound on how far the scanner runs ahead, not a
        barrier that must fully drain."""
        from src.remote_pipeline import RemotePipelineCoordinator

        db = FakeStreamingDB(pending=[0, 1, 2])
        for ci in (0, 1, 2):
            db.statuses[ci] = "pending"
        host = SimpleNamespace(db=db)
        coordinator = RemotePipelineCoordinator(
            host=host, session_id=37, tape_label="T",
            ready_q=mock.MagicMock(), stop_event=threading.Event(),
            metrics=mock.MagicMock(), backlog_limit=3)

        self.assertEqual(coordinator.sealed_but_unstaged(), 3)
        # At the limit the gate holds; it does not return False (that means
        # "stop"), it simply waits until the backlog falls.
        blocked = threading.Event()

        def gate():
            blocked.set()
            return coordinator.publication_gate()

        waiter = threading.Thread(target=gate, daemon=True)
        waiter.start()
        self.assertTrue(blocked.wait(5))
        time.sleep(0.2)
        self.assertTrue(waiter.is_alive(), "the gate did not pause at the limit")
        # One chunk taken by the stager is enough to admit the scanner again.
        coordinator.next_chunk_to_stage()
        waiter.join(10)
        self.assertFalse(waiter.is_alive())

    def test_an_unreadable_backlog_holds_publication(self):
        """An unknown backlog must never be treated as an empty one."""
        from src.remote_pipeline import RemotePipelineCoordinator

        class Exploding:
            def get_chunks_with_status(self, session_id, status):
                raise RuntimeError("connection lost")

        coordinator = RemotePipelineCoordinator(
            host=SimpleNamespace(db=Exploding()), session_id=37,
            tape_label="T", ready_q=mock.MagicMock(),
            stop_event=threading.Event(), metrics=mock.MagicMock(),
            backlog_limit=4)
        self.assertEqual(coordinator.sealed_but_unstaged(), 4)
        coordinator.stop_event.set()
        self.assertFalse(coordinator.publication_gate())

    def test_file_count_ceiling_seals_a_chunk(self):
        from src.planning import StreamingChunkBuilder
        builder = StreamingChunkBuilder(10 ** 9, alloc_unit=1,
                                        padding_factor=1.0, max_files=2)
        sealed = []
        for i in range(5):
            sealed.extend(builder.add(f"f{i}", 1))
        self.assertEqual([len(chunk) for chunk in sealed], [2, 2])


# =============================================================================
# D. Surviving state and duplicate protection
# =============================================================================
class SurvivingStateCharacterizationTests(unittest.TestCase):
    def test_frontier_state_exists_in_sql_but_is_never_applied_at_startup(self):
        """UPDATED BY TASK 2.1, as this test's original message required.

        Migration 014 now defines the frontier tables — but defining them is
        not the same as having them. They are absent from the startup schema
        init on purpose, so a database only gains them when an operator applies
        014 explicitly, after a verified backup.
        """
        import inspect
        from src.pg_core import PgConnectionCore

        base = os.path.join(PROJECT_ROOT, "scripts", "sql",
                            "014_postgres_incremental_scan.sql")
        with open(base, encoding="utf-8") as handle:
            migration = handle.read().lower()
        for table in ("remote_scan_directories", "remote_scan_segments",
                      "remote_scan_scopes", "remote_chunk_scan_segments"):
            self.assertIn(table, migration, table)

        # ...and startup does not apply it.
        startup = inspect.getsource(PgConnectionCore._init_schema)
        self.assertNotIn("014", startup)
        self.assertNotIn("incremental_scan", startup)

    def test_the_legacy_session_tables_still_hold_no_frontier_columns(self):
        """The base schema file is untouched: 014 is purely additive."""
        with open(os.path.join(PROJECT_ROOT, "scripts", "sql",
                               "001_postgres_schema.sql"),
                  encoding="utf-8") as handle:
            schema = handle.read().lower()
        for absent in ("remote_scan_directories", "remote_scan_segments",
                       "owner_token", "membership_state"):
            self.assertNotIn(absent, schema, absent)

    def test_duplicate_protection_relies_on_these_constraints_only(self):
        path = os.path.join(PROJECT_ROOT, "scripts", "sql",
                            "001_postgres_schema.sql")
        with open(path, encoding="utf-8") as handle:
            schema = handle.read()
        self.assertIn("UNIQUE (snapshot_id, remote_path)", schema)
        self.assertIn("UNIQUE (plan_id, snapshot_file_id)", schema)
        self.assertIn("PRIMARY KEY (session_id, chunk_index)", schema)
        # There is NO ordinal uniqueness and no membership seal today.
        self.assertNotIn("UNIQUE (plan_id, chunk_index, ordinal)", schema)
        self.assertNotRegex(schema, r"membership_state")

    def test_chunk_status_vocabulary_is_unchanged(self):
        path = os.path.join(PROJECT_ROOT, "scripts", "sql",
                            "001_postgres_schema.sql")
        with open(path, encoding="utf-8") as handle:
            schema = handle.read()
        match = re.search(
            r"remote_chunks.*?CHECK \(status IN \((.*?)\)\)", schema, re.S)
        self.assertIsNotNone(match)
        statuses = set(re.findall(r"'([a-z_]+)'", match.group(1)))
        self.assertEqual(
            statuses,
            {"pending", "fetching", "packing", "backing", "done",
             "fetch_failed", "backup_failed"})

    def test_backing_has_no_automatic_reset(self):
        """'backing' is ambiguous: no code path clears it without a human."""
        source = inspect.getsource(ro)
        self.assertIn("_detect_prior_backing_chunks", source)
        detect = inspect.getsource(
            ro.RemoteOrchestrator._detect_prior_backing_chunks)
        self.assertIn("Refusing to resume blindly", detect)
        self.assertNotIn("update_chunk_status", detect)


# =============================================================================
# E. Dormant directory-first code is NOT the production path
# =============================================================================
class RemovedDirectoryFirstPathTests(unittest.TestCase):
    """Task 1.6 removed the dormant directory-first code after the audit.

    It had no production caller, and two defects made reviving it as-is unsafe:
    ``stat_directory`` ran a full recursive ``find`` per directory (planning a
    tree walked it once per node), and ``iter_large_files`` compared its
    threshold in megabytes while ``stat_directory`` compared the same setting in
    bytes. Task 2.3's directory frontier replaces it from scratch.
    """

    def test_the_dormant_symbols_are_gone_everywhere(self):
        import src.orchestrators as facade
        import src.planning as planning
        import src.scanning as scanning
        for symbol in ("DirectoryFirstRemoteScanner", "DirectoryUnitPlanner",
                       "DirectoryPlanUnit"):
            for module in (facade, planning, scanning, ro):
                self.assertFalse(hasattr(module, symbol),
                                 f"{module.__name__}.{symbol} survived")
            self.assertNotIn(symbol, facade.__all__)

    def test_their_configuration_knobs_are_gone_too(self):
        from src.config import ConfigManager
        for name in ("remote_scan_mode", "remote_scan_depth",
                     "directory_chunk_max_gb", "directory_chunk_max_files"):
            self.assertFalse(hasattr(ConfigManager, name), name)

    def test_the_retained_planner_and_scanner_have_production_callers(self):
        import inspect
        from src import scan_frontier
        # StreamingChunkBuilder is what the coordinator seals chunks with...
        self.assertIn("StreamingChunkBuilder",
                      inspect.getsource(scan_frontier))
        # ...and ChunkPlanner is what it computes footprints with.
        from src.planning import ChunkPlanner, StreamingChunkBuilder
        self.assertIs(StreamingChunkBuilder(1).planner.__class__, ChunkPlanner)
        # RemoteScanner is retained as StreamingRemoteScanner's base class.
        from src.scanning import RemoteScanner, StreamingRemoteScanner
        self.assertTrue(issubclass(StreamingRemoteScanner, RemoteScanner))


# =============================================================================
# F. Coverage honesty
# =============================================================================
class CoverageHonestyCharacterizationTests(unittest.TestCase):
    def test_a_recoverable_skip_still_allows_coverage_to_finalize(self):
        """A permission-denied subtree is recorded as skipped, and coverage
        can still finalize.

        CHANGED BY PLAN 1 COMPLETION, deliberately. Finality no longer comes
        from ``mark_remote_scan_complete`` at the end of a whole-root walk — the
        frontier finalizes each scope only after its directories are terminal
        and the mutation sweep agrees. The assertion therefore moved from "the
        session flag was set" to "the scope reached final coverage", which is
        the fact that actually means the source was explored. What has not
        changed, and is still asserted, is that **no catalog row proves
        coverage**: only traversal does.
        """
        db = FakeStreamingDB(pending=[])
        orch = build_streaming_orchestrator(db, chunk_budget=10 ** 9)

        def per_record(path, size):
            orch.skipped_tracker.add(
                "remote", "/vault/denied", "Permission denied", "scan")

        with StreamingHarness(records=[("/vault/ok", 10)], per_record=per_record):
            orch._run_streaming_session(37)

        self.assertTrue(orch.skipped_tracker.add.called)
        self.assertEqual([s["coverage_state"] for s in db.scopes], ["final"])
        # The session flag follows scope finality, never precedes it: it is
        # set here only BECAUSE every scope reached final coverage.
        self.assertIn("mark_remote_scan_complete", db.calls)

    def test_scan_failure_records_an_error_and_stops_resumably(self):
        db = FakeStreamingDB(pending=[])
        orch = build_streaming_orchestrator(db)

        def boom(path, size):
            raise RuntimeError("ssh died mid-scan")

        with StreamingHarness(records=[("/vault/a", 1)], per_record=boom):
            result = orch._run_streaming_session(37)

        self.assertEqual(db.scan_error, "ssh died mid-scan")
        self.assertFalse(db.scan_complete)
        self.assertTrue(result.resumable)


# =============================================================================
# G. Data boundaries (unchanged by Plan 1)
# =============================================================================
class DataBoundaryCharacterizationTests(unittest.TestCase):
    def test_the_four_data_boundaries_are_where_the_map_says(self):
        from src.backup import LTOBackup
        from src.packer import LTOPacker
        from src.pg_catalog import PgCatalogMixin
        from src.retriever import LTORetriever
        self.assertTrue(hasattr(LTOPacker, "run"))
        self.assertTrue(hasattr(LTOBackup, "run"))
        self.assertTrue(hasattr(PgCatalogMixin, "bulk_upsert_files"))
        for name in ("run", "_restore_many", "_restore_loose"):
            self.assertTrue(hasattr(LTORetriever, name),
                            f"LTORetriever.{name} moved")


class BoundedWriteGroupRunTests(unittest.TestCase):
    """`max_write_groups_per_run` must end the run ON a chunk boundary.

    The runbook requires that the next finite group never starts by itself.
    The limit is checked after a group commits and before the next is
    selected, so the writer is never interrupted mid-group.
    """

    def _coordinator(self, groups, *, max_write_groups):
        from src.remote_pipeline import RemotePipelineCoordinator

        written = []

        def wait_for_group(stop_event=None, poll=0.5, timeout=None):
            if not groups:
                return [], "producer_closed_empty"
            indexes = groups.pop(0)
            return ([SimpleNamespace(chunk_index=i, prepared_bytes=1,
                                     desc=SimpleNamespace(chunk_index=i))
                     for i in indexes], "max_ready_chunks_reached")

        ready_q = mock.MagicMock()
        ready_q.wait_for_group.side_effect = wait_for_group
        ready_q.ready_chunks = 0

        host = SimpleNamespace(
            db=FakeStreamingDB(pending=[]),
            _write_chunk_group=lambda *a, **k: (
                written.append(a[1]) or None))

        coordinator = RemotePipelineCoordinator(
            host=host, session_id=37, tape_label="T", ready_q=ready_q,
            stop_event=threading.Event(), metrics=mock.MagicMock(),
            max_write_groups=max_write_groups)
        coordinator._observe_start = lambda items, reason: (None, None, 0.0)
        coordinator._observe_finish = lambda *a, **k: None
        coordinator._run_stager = lambda: None
        coordinator._scanner_done.set()
        return coordinator, written

    def test_a_limit_of_one_stops_after_the_first_group(self):
        coordinator, written = self._coordinator(
            [[49], [50], [51]], max_write_groups=1)
        coordinator.run()
        self.assertEqual(coordinator.outcome.groups_written, [(49,)])
        self.assertEqual(coordinator.outcome.completed_chunks, 1)
        self.assertEqual(
            coordinator.outcome.stopped_reason, "max_write_groups_reached")
        self.assertEqual(len(written), 1, "a second group was written")

    def test_zero_means_unbounded(self):
        coordinator, written = self._coordinator(
            [[49], [50]], max_write_groups=0)
        coordinator.run()
        self.assertEqual(
            coordinator.outcome.groups_written, [(49,), (50,)])
        self.assertIsNone(coordinator.outcome.stopped_reason)

    def test_the_limit_never_truncates_a_group_in_progress(self):
        # A group of three chunks still writes all three; the bound applies
        # between groups, never inside one.
        coordinator, written = self._coordinator(
            [[49, 50, 51], [52]], max_write_groups=1)
        coordinator.run()
        self.assertEqual(coordinator.outcome.groups_written, [(49, 50, 51)])
        self.assertEqual(coordinator.outcome.completed_chunks, 3)
        self.assertEqual(len(written), 1)

    def test_config_default_is_unbounded(self):
        from src.config import ConfigManager

        self.assertEqual(
            ConfigManager.max_write_groups_per_run.fget(
                SimpleNamespace(_get_int=lambda *a, **k: a[2])), 0)


if __name__ == "__main__":
    unittest.main()
