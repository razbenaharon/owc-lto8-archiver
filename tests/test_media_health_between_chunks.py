"""Audit (low): drive/medium health was checked once per write group.

A fault LTFS logs while chunk N is being written must stop chunk N+1. The
per-chunk re-check is allowed to read ONLY the passive IBM log
(``ltfs_media_health`` -> LogFile.csv): no readiness probe, no label read, no
mount/event-log query between the chunks of a group (AGENTS.md).
"""
import threading
import unittest
from types import SimpleNamespace
from unittest import mock

from src import ltfs
from src import remote_orchestrator as ro
from src.exit_codes import REASON_LTFS_MEDIA_DEGRADED
from src.pipeline_types import ContainerFormat

MOUNTED = "2026-11-01T10:00:00"
HEALTH_OK = {"ok": True, "determinate": True, "since": MOUNTED,
             "fatal": [], "degraded": []}
EVENT = {"at": "2026/11/01 11:00:00.000", "id": 21702,
         "meaning": "write perm", "message": "LOCATE returned write perm"}
HEALTH_BAD = {"ok": False, "determinate": True, "since": MOUNTED,
              "fatal": [EVENT], "degraded": []}


class MediaHealthBetweenChunks(unittest.TestCase):
    def setUp(self):
        self.written = []
        ltfs.reset_readiness("test setup")
        self.addCleanup(ltfs.reset_readiness, "test teardown")
        orch = ro.RemoteOrchestrator.__new__(ro.RemoteOrchestrator)
        orch.cfg = SimpleNamespace(lto_drive="X:\\", backup_log_dir=None)
        orch.db = mock.MagicMock()
        orch.db.get_chunk_packaging_format.return_value = ContainerFormat.ZIP
        orch.notifier = None
        orch.remote_host = "host.example"
        orch.remote_session_path = "/strg"
        orch.skipped_tracker = mock.MagicMock()
        orch._consumer_chunk = None
        orch._ownership_acquisitions = 0
        orch._readiness_checks = 0
        orch._cartridge_verifications = 0
        orch._staged_lock = threading.RLock()
        orch._staged_bytes = 0
        orch._get_recorded_stop = lambda: None
        orch._record_stop = lambda s, escalate=False: s
        orch._cleanup_dir = lambda *_a: None
        orch._ensure_remote_chunk_fits_tape = lambda *a, **k: True
        orch.governor = None
        outer = self

        class Writer:
            def run(self, **kwargs):
                outer.written.append(kwargs["remote_chunk_index"])
                kwargs["on_write_start"]()

        orch._backup_writer = lambda cls=None: Writer()

        def gate(*_a, **_k):
            # What the real gate leaves behind after its media-health check.
            orch._gate_mount_started_at = MOUNTED
            return None

        orch._pre_write_safety_gate = gate
        self.orch = orch
        forbidden = mock.Mock(side_effect=AssertionError(
            "LTFS/mount access between chunks of a group"))
        for name in ("ltfs_current_mount_status", "get_volume_label",
                     "_ensure_lto_drive_ready"):
            patcher = mock.patch.object(ro, name, forbidden)
            patcher.start()
            self.addCleanup(patcher.stop)
        printer = mock.patch("builtins.print")
        printer.start()
        self.addCleanup(printer.stop)

    def _descs(self, indices):
        return [SimpleNamespace(
            chunk_index=i, pack_dir=f"/tmp/_pack_{i}", staged_bytes=10,
            skip_tape=False, session_id=37,
            packaging_format=ContainerFormat.ZIP, metadata=[],
            fetch_dir=f"/tmp/_fetch_{i}") for i in indices]

    def _write(self, health_results):
        with mock.patch.object(ro, "ltfs_media_health",
                               side_effect=health_results) as health:
            block = self.orch._write_chunk_group(
                37, self._descs([0, 1, 2]), "T", False, threading.Event())
        return block, health

    def test_fault_logged_during_a_chunk_stops_the_next_one(self):
        block, health = self._write([dict(HEALTH_BAD)])
        self.assertEqual(self.written, [0])
        self.assertIsNotNone(block)
        self.assertEqual(block.reason, REASON_LTFS_MEDIA_DEGRADED)
        self.assertEqual(block.chunk_index, 1)
        self.assertTrue(block.preserve_pack)
        # Scoped to the same mount the gate verified.
        health.assert_called_once_with(since_iso=MOUNTED)

    def test_unreadable_log_between_chunks_fails_closed(self):
        block, _health = self._write(
            [{"ok": False, "determinate": False, "error": "locked"}])
        self.assertEqual(self.written, [0])
        self.assertEqual(block.reason, REASON_LTFS_MEDIA_DEGRADED)

    def test_the_real_gate_check_records_the_mount_it_scoped_to(self):
        # Without this the per-chunk re-check would silently never run.
        with mock.patch.object(ro, "ltfs_media_health",
                               return_value=dict(HEALTH_OK)):
            self.assertIsNone(self.orch._verify_ltfs_media_health(
                mount_started_at=MOUNTED))
        self.assertEqual(self.orch._gate_mount_started_at, MOUNTED)

    def test_healthy_log_lets_the_whole_group_through(self):
        block, health = self._write([dict(HEALTH_OK), dict(HEALTH_OK)])
        self.assertIsNone(block)
        self.assertEqual(self.written, [0, 1, 2])
        # Once before each LATER chunk; the first is covered by the gate.
        self.assertEqual(health.call_count, 2)


if __name__ == "__main__":
    unittest.main()
