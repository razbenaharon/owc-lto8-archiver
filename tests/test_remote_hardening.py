import os
import tempfile
import threading
import unittest
from types import SimpleNamespace
from unittest import mock

from src.constants import LOCAL_STAGING_RESERVE_BYTES
from src.orchestrators import RemoteOrchestrator, RemoteScanner
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
        orch._staged_bytes = 0
        orch._staged_lock = threading.Lock()
        return orch

    def test_await_staging_capacity_rejects_disk_exhaustion(self):
        orch = self._orchestrator()
        planned = 100
        free = 2 * planned + LOCAL_STAGING_RESERVE_BYTES - 1
        usage = SimpleNamespace(total=free, used=0, free=free)
        with mock.patch("src.orchestrators.shutil.disk_usage",
                        return_value=usage):
            with self.assertRaisesRegex(RuntimeError, "Insufficient local staging"):
                orch._await_staging_capacity(planned, threading.Event())

    def test_chunk_budget_creates_staging_and_reserves_floor(self):
        with tempfile.TemporaryDirectory() as tmp:
            orch = self._orchestrator()
            orch.staging_dir = os.path.join(tmp, "missing-stage")
            orch.fill_pct = 1.0
            orch.chunk_cap_bytes = 10**15
            free = LOCAL_STAGING_RESERVE_BYTES + 1234
            usage = SimpleNamespace(total=free, used=0, free=free)
            with mock.patch("src.orchestrators.shutil.disk_usage",
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

            def get_tape(self, label):
                return {"total_capacity": 1 / 1024**3}

            def recalculate_tape_used_space(self, label):
                return 0

            def update_chunk_status(self, session_id, chunk_index, status):
                self.statuses.append(status)

        orch = self._orchestrator()
        orch.db = FakeDB()
        orch.cfg = SimpleNamespace(ibm_eject_cmd="", lto_drive="", backup_log_dir="")
        desc = {
            "chunk_index": 0,
            "pack_dir": r"C:\stage\pack",
            "fetch_dir": r"C:\stage\fetch",
            "metadata": [{"is_packed": True}],
            "staged_bytes": 0,
        }
        self.assertFalse(orch._write_chunk(1, desc, "T1", eject_after=False))
        self.assertEqual(orch.db.statuses, ["backup_failed"])


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
        with mock.patch("src.orchestrators._ssh_run", return_value=result):
            manifest = scanner.scan(["/data"])
        self.assertEqual(manifest, [("/data/ok.txt", 10)])
        self.assertEqual(tracker.count(), 1)
        item = tracker.items()[0]
        self.assertEqual(item.path, "/data/private")
        self.assertIn("Permission denied", item.reason)


if __name__ == "__main__":
    unittest.main()
