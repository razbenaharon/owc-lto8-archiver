import os
import tempfile
import threading
import unittest
from types import SimpleNamespace
from unittest import mock

from src.constants import LOCAL_STAGING_RESERVE_BYTES
from src.orchestrators import (
    ChunkPlanner,
    RemoteOrchestrator,
    RemoteScanner,
    StreamingChunkBuilder,
    StreamingRemoteScanner,
)
from src.pipeline_types import StagedChunk
from src.remote_transport import (
    _ASKPASS_HELPERS,
    _cleanup_askpass_helpers,
    _openssh_askpass_env,
    _ssh_run,
    _ssh_stream_command,
)
from src.skipped import SkippedFileTracker


class RemoteStagingSafetyTests(unittest.TestCase):
    def _orchestrator(self):
        orch = RemoteOrchestrator.__new__(RemoteOrchestrator)
        orch.staging_dir = r"C:\stage"
        orch.staging_max_bytes = 10**12
        orch.staging_padding = 1.0
        orch.fetch_abort_factor = 2.0
        orch.notifier = None
        orch._staged_bytes = 0
        orch._staged_lock = threading.Lock()
        return orch

    def test_await_staging_capacity_rejects_disk_exhaustion(self):
        orch = self._orchestrator()
        planned = 100
        free = 2 * planned + LOCAL_STAGING_RESERVE_BYTES - 1
        usage = SimpleNamespace(total=free, used=0, free=free)
        with mock.patch("src.remote_orchestrator.shutil.disk_usage",
                        return_value=usage):
            with self.assertRaisesRegex(RuntimeError, "Insufficient local staging"):
                orch._await_staging_capacity(planned, 0, threading.Event())

    def test_chunk_budget_creates_staging_and_reserves_floor(self):
        with tempfile.TemporaryDirectory() as tmp:
            orch = self._orchestrator()
            orch.staging_dir = os.path.join(tmp, "missing-stage")
            orch.fill_pct = 1.0
            orch.chunk_cap_bytes = 10**15
            free = LOCAL_STAGING_RESERVE_BYTES + 1234
            usage = SimpleNamespace(total=free, used=0, free=free)
            with mock.patch("src.remote_orchestrator.shutil.disk_usage",
                            return_value=usage):
                self.assertEqual(orch._chunk_budget(), 1234)
            self.assertTrue(os.path.isdir(orch.staging_dir))

    def test_remote_write_rejects_chunk_before_backing_status(self):
        class FakeDB:
            def __init__(self):
                self.statuses = []

            def get_chunk_files(self, session_id, chunk_index):
                return [
                    {"file_size_bytes": 1000, "status": "fetched"},
                    {"file_size_bytes": 1000, "status": "source_missing"},
                ]

            def get_chunk_size_summary(self, session_id, chunk_index=None):
                # planned counts every file; present excludes source_missing.
                return {0: (2000, 1000, 2)}

            def get_tape(self, label):
                return {"total_capacity": 1 / 1024**3}

            def recalculate_tape_used_space(self, label):
                return 0

            def update_chunk_status(self, session_id, chunk_index, status):
                self.statuses.append(status)

        orch = self._orchestrator()
        orch.db = FakeDB()
        orch.cfg = SimpleNamespace(ibm_eject_cmd="", lto_drive="", backup_log_dir="")
        desc = StagedChunk(
            chunk_index=0,
            pack_dir=r"C:\stage\pack",
            fetch_dir=r"C:\stage\fetch",
            metadata=[{"is_packed": True}],
            staged_bytes=0,
        )
        self.assertFalse(orch._write_chunk(1, desc, "T1", eject_after=False))
        self.assertEqual(orch.db.statuses, ["backup_failed"])


class ChunkPlannerFootprintTests(unittest.TestCase):
    def test_small_files_round_up_to_allocation_clusters(self):
        # 20 one-byte files each allocate a full 4 KiB cluster, so a budget of
        # 10 clusters must split them into two chunks (logical packing would
        # have put all 20 in one).
        planner = ChunkPlanner(10 * 4096, alloc_unit=4096, padding_factor=1.0)
        chunks = planner.plan([(f"/f/{i}", 1) for i in range(20)])
        self.assertEqual([len(c) for c in chunks], [10, 10])

    def test_padding_factor_shrinks_effective_budget(self):
        planner = ChunkPlanner(100, alloc_unit=1, padding_factor=2.0)
        self.assertEqual(planner.footprint(60), 120)
        # Padded past the budget -> dedicated chunk; the 40-byte file fits.
        chunks = planner.plan([("/f/a", 60), ("/f/b", 40)])
        self.assertEqual(len(chunks), 2)

    def test_zero_size_file_occupies_one_cluster(self):
        planner = ChunkPlanner(10**6, alloc_unit=4096, padding_factor=1.0)
        self.assertEqual(planner.footprint(0), 4096)

    def test_plan_preserves_logical_sizes(self):
        planner = ChunkPlanner(10 * 4096, alloc_unit=4096, padding_factor=1.0)
        chunks = planner.plan([("/f/a", 5)])
        self.assertEqual(chunks, [[("/f/a", 5)]])


class StreamingChunkBuilderTests(unittest.TestCase):
    def test_emits_chunk_at_threshold(self):
        builder = StreamingChunkBuilder(100, alloc_unit=1, padding_factor=1.0)
        self.assertEqual(builder.add("/f/a", 60), [])
        self.assertEqual(builder.add("/f/b", 50), [[("/f/a", 60)]])
        self.assertEqual(builder.flush(), [[("/f/b", 50)]])

    def test_oversized_file_gets_dedicated_chunk(self):
        builder = StreamingChunkBuilder(100, alloc_unit=1, padding_factor=1.0)
        self.assertEqual(builder.add("/f/a", 20), [])
        self.assertEqual(
            builder.add("/f/huge", 120),
            [[("/f/a", 20)], [("/f/huge", 120)]],
        )
        self.assertEqual(builder.flush(), [])

    def test_applies_cluster_padding(self):
        builder = StreamingChunkBuilder(
            10 * 4096, alloc_unit=4096, padding_factor=1.0)
        for idx in range(10):
            self.assertEqual(builder.add(f"/f/{idx}", 1), [])
        self.assertEqual(builder.add("/f/10", 1),
                         [[(f"/f/{idx}", 1) for idx in range(10)]])


class FetchWatchdogTests(unittest.TestCase):
    def _orchestrator(self):
        orch = RemoteOrchestrator.__new__(RemoteOrchestrator)
        orch.staging_dir = r"C:\stage"
        orch.staging_padding = 1.0
        orch.fetch_abort_factor = 2.0
        orch.notifier = None
        return orch

    def test_watchdog_aborts_on_hard_overrun(self):
        orch = self._orchestrator()
        stop, abort = threading.Event(), threading.Event()
        plenty = SimpleNamespace(total=10**12, used=0, free=10**12)
        with mock.patch("src.remote_orchestrator._dir_tree_size", return_value=300), \
             mock.patch("src.remote_orchestrator.shutil.disk_usage",
                        return_value=plenty):
            orch._start_fetch_monitor(stop, abort, r"C:\stage\_fetch", 100)
            self.assertTrue(abort.wait(timeout=15),
                            "watchdog did not abort a 3x overrun")
        stop.set()

    def test_watchdog_aborts_when_staging_hits_reserve_floor(self):
        orch = self._orchestrator()
        stop, abort = threading.Event(), threading.Event()
        low = SimpleNamespace(total=10**12, used=0,
                              free=LOCAL_STAGING_RESERVE_BYTES - 1)
        with mock.patch("src.remote_orchestrator._dir_tree_size", return_value=10), \
             mock.patch("src.remote_orchestrator.shutil.disk_usage",
                        return_value=low):
            orch._start_fetch_monitor(stop, abort, r"C:\stage\_fetch", 100)
            self.assertTrue(abort.wait(timeout=15),
                            "watchdog did not abort on exhausted staging disk")
        stop.set()

    def test_watchdog_stays_quiet_within_plan(self):
        orch = self._orchestrator()
        stop, abort = threading.Event(), threading.Event()
        plenty = SimpleNamespace(total=10**12, used=0, free=10**12)
        with mock.patch("src.remote_orchestrator._dir_tree_size", return_value=90), \
             mock.patch("src.remote_orchestrator.shutil.disk_usage",
                        return_value=plenty):
            orch._start_fetch_monitor(stop, abort, r"C:\stage\_fetch", 100)
            self.assertFalse(abort.wait(timeout=3))
        stop.set()


class RemotePasswordSafetyTests(unittest.TestCase):
    def test_askpass_cleanup_removes_helpers_and_registry_entries(self):
        env = _openssh_askpass_env("secret")
        helper = env["SSH_ASKPASS"]
        self.assertTrue(os.path.exists(helper))
        self.assertIn(helper, _ASKPASS_HELPERS)
        _cleanup_askpass_helpers()
        self.assertFalse(os.path.exists(helper))
        self.assertNotIn(helper, _ASKPASS_HELPERS)

    def test_plink_password_fallback_is_disabled_for_commands(self):
        def has_command(name):
            return name == "plink"

        with mock.patch("src.remote_transport._has_command",
                        side_effect=has_command):
            result = _ssh_run("user", "host", "true", password="secret")
        self.assertEqual(result.returncode, 255)
        self.assertIn("disabled", result.stderr)

    def test_plink_password_fallback_is_disabled_for_streaming(self):
        def has_command(name):
            return name == "plink"

        with mock.patch("src.remote_transport._has_command",
                        side_effect=has_command):
            cmd, env, err = _ssh_stream_command(
                "user", "host", "tar", password="secret")
        self.assertIsNone(cmd)
        self.assertIsNone(env)
        assert err is not None
        self.assertIn("disabled", err)


class RemoteScannerTests(unittest.TestCase):
    def test_partial_find_warnings_are_recorded_and_manifest_continues(self):
        tracker = SkippedFileTracker()
        scanner = RemoteScanner(
            "user", "host", skipped_tracker=tracker, timeout=10)
        result = SimpleNamespace(
            returncode=1,
            stdout="10 /data/ok.txt\0",
            stderr="find: ‘/data/private’: Permission denied",
        )
        with mock.patch("src.scanning._ssh_run", return_value=result):
            manifest = scanner.scan(["/data"])
        self.assertEqual(manifest, [("/data/ok.txt", 10)])
        self.assertEqual(tracker.count(), 1)
        item = tracker.items()[0]
        self.assertEqual(item.path, "/data/private")
        self.assertIn("Permission denied", item.reason)

    def test_truncated_record_outside_scan_roots_is_rejected(self):
        # A stream cut mid-record leaves a fragment like '931839 /strg' whose
        # path is an ancestor directory of the scan roots. Planning it would
        # make the fetch tar recurse the whole tree — it must be dropped.
        tracker = SkippedFileTracker()
        scanner = RemoteScanner(
            "user", "host", skipped_tracker=tracker, timeout=10)
        result = SimpleNamespace(
            returncode=0,
            stdout="10 /strg/E/data/ok.txt\0931839 /strg",
            stderr="",
        )
        empty = SimpleNamespace(returncode=0, stdout="", stderr="")
        with mock.patch("src.scanning._ssh_run",
                        side_effect=[result, empty]):
            manifest = scanner.scan(["/strg/E/data", "/strg/D/data"])
        self.assertEqual(manifest, [("/strg/E/data/ok.txt", 10)])
        self.assertEqual(tracker.count(), 1)
        item = tracker.items()[0]
        self.assertEqual(item.path, "/strg")
        self.assertIn("outside scan roots", item.reason)

    def test_permission_denied_empty_root_is_skipped_and_next_root_continues(self):
        tracker = SkippedFileTracker()
        scanner = RemoteScanner(
            "user", "host", skipped_tracker=tracker, timeout=10)
        denied = SimpleNamespace(
            returncode=1,
            stdout="",
            stderr="find: ג€˜/data/privateג€™: Permission denied",
        )
        ok = SimpleNamespace(
            returncode=0,
            stdout="12 /data/public/ok.txt\0",
            stderr="",
        )
        with mock.patch("src.scanning._ssh_run",
                        side_effect=[denied, ok]):
            manifest = scanner.scan(["/data/private", "/data/public"])
        self.assertEqual(manifest, [("/data/public/ok.txt", 12)])
        self.assertEqual(tracker.count(), 1)
        self.assertEqual(tracker.items()[0].path, "/data/private")
        self.assertIn("Permission denied", tracker.items()[0].reason)

    def test_timeout_discards_partial_scan_and_fails_session_creation(self):
        tracker = SkippedFileTracker()
        scanner = RemoteScanner(
            "user", "host", skipped_tracker=tracker, timeout=10)
        result = SimpleNamespace(
            returncode=124,
            stdout="10 /data/ok.txt\0",
            stderr="SSH command timed out after 10s",
        )
        with mock.patch("src.scanning._ssh_run", return_value=result):
            with self.assertRaisesRegex(RuntimeError, "timed out"):
                scanner.scan(["/data"])

    def test_scan_accepts_single_file_scan_root(self):
        tracker = SkippedFileTracker()
        scanner = RemoteScanner(
            "user", "host", skipped_tracker=tracker, timeout=10)
        result = SimpleNamespace(
            returncode=0, stdout="7 /data/one.bin\0", stderr="")
        with mock.patch("src.scanning._ssh_run", return_value=result):
            manifest = scanner.scan(["/data/one.bin"])
        self.assertEqual(manifest, [("/data/one.bin", 7)])
        self.assertEqual(tracker.count(), 0)


class _ChunkPipe:
    def __init__(self, chunks):
        self.chunks = list(chunks)

    def read(self, _size=-1):
        if not self.chunks:
            return b""
        return self.chunks.pop(0)


class _FakePopen:
    def __init__(self, stdout_chunks, stderr=b"", returncode=0):
        self.stdout = _ChunkPipe(stdout_chunks)
        self.stderr = _ChunkPipe([stderr] if stderr else [])
        self.returncode = returncode

    def wait(self, timeout=None):
        return self.returncode

    def terminate(self):
        self.returncode = -15

    def kill(self):
        self.returncode = -9


class StreamingRemoteScannerTests(unittest.TestCase):
    def test_parses_records_split_across_reads(self):
        scanner = StreamingRemoteScanner("user", "host")
        proc = _FakePopen([b"10 /data/a", b".bin\0", b"12 /data/b.bin\0"])
        with mock.patch("src.scanning._ssh_stream_command",
                        return_value=(["ssh"], None, None)), \
             mock.patch("src.scanning.subprocess.Popen",
                        return_value=proc):
            rows = list(scanner.iter_scan(["/data"]))
        self.assertEqual(rows, [("/data/a.bin", 10), ("/data/b.bin", 12)])

    def test_rejects_record_outside_scan_root(self):
        tracker = SkippedFileTracker()
        scanner = StreamingRemoteScanner("user", "host",
                                         skipped_tracker=tracker)
        proc = _FakePopen([b"10 /data/ok.bin\0", b"99 /strg\0"])
        with mock.patch("src.scanning._ssh_stream_command",
                        return_value=(["ssh"], None, None)), \
             mock.patch("src.scanning.subprocess.Popen",
                        return_value=proc):
            rows = list(scanner.iter_scan(["/data"]))
        self.assertEqual(rows, [("/data/ok.bin", 10)])
        self.assertEqual(tracker.count(), 1)
        self.assertIn("outside scan roots", tracker.items()[0].reason)

    def test_nonzero_find_with_warning_keeps_valid_rows(self):
        tracker = SkippedFileTracker()
        scanner = StreamingRemoteScanner("user", "host",
                                         skipped_tracker=tracker)
        proc = _FakePopen(
            [b"10 /data/ok.bin\0"],
            stderr="find: '/data/private': Permission denied\n".encode(),
            returncode=1,
        )
        with mock.patch("src.scanning._ssh_stream_command",
                        return_value=(["ssh"], None, None)), \
             mock.patch("src.scanning.subprocess.Popen",
                        return_value=proc):
            rows = list(scanner.iter_scan(["/data"]))
        self.assertEqual(rows, [("/data/ok.bin", 10)])
        self.assertEqual(tracker.count(), 1)
        self.assertEqual(tracker.items()[0].path, "/data/private")


if __name__ == "__main__":
    unittest.main()
