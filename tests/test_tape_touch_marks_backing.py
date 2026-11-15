"""Audit H2: a hard tape error at the very start of a write is not transient.

Creating the destination directory on tape used to happen BEFORE the chunk was
marked ``backing``. On write-protected media that first touch failed, the
failure was classified "nothing reached the tape" (10/tape_write_failed,
re-fetchable), the ready pack was thrown away, and no tape error was recorded.

Everything here runs against temp directories; the "tape" is a temp folder and
robocopy is never started.
"""
import os
import shutil
import tempfile
import threading
import unittest
from types import SimpleNamespace
from unittest import mock

from src import ltfs
from src.backup import LTOBackup
from src.exit_codes import (ExitCode, REASON_AMBIGUOUS_BACKING_CHUNK,
                            REASON_TAPE_WRITE_FAILED)
from src.pipeline_types import ContainerFormat

_REAL_MAKEDIRS = os.makedirs


class _WriteProtectedTape:
    """os.makedirs stand-in: any directory creation under ``tape`` fails."""

    def __init__(self, tape, events):
        self.tape = os.path.normcase(os.path.abspath(tape))
        self.events = events

    def __call__(self, path, *args, **kwargs):
        target = os.path.normcase(os.path.abspath(path))
        if (target + os.sep).startswith(self.tape + os.sep):
            self.events.append("tape_mkdir")
            raise OSError(19, "The media is write protected")
        return _REAL_MAKEDIRS(path, *args, **kwargs)


class _Workspace(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.pack = os.path.join(self.tmp, "_pack_s0037_001")
        self.tape = os.path.join(self.tmp, "fake_ltfs_mount")
        _REAL_MAKEDIRS(self.pack)
        _REAL_MAKEDIRS(self.tape)
        with open(os.path.join(self.pack, "Bundle_0001.zip"), "wb") as fh:
            fh.write(b"x" * 2048)
        self.events = []
        ltfs.reset_readiness("test setup")
        self.addCleanup(ltfs.reset_readiness, "test teardown")

    def _patches(self):
        robocopy = mock.Mock(side_effect=AssertionError(
            "robocopy must not start after a hard tape error"))
        return robocopy, (
            mock.patch("src.backup._ensure_lto_drive_ready",
                       return_value=True),
            mock.patch("src.backup._run_robocopy_tuned", robocopy),
            mock.patch("src.backup.os.makedirs",
                       _WriteProtectedTape(self.tape, self.events)),
        )


class BackupMarksWriteStartedBeforeTouchingTheTape(_Workspace):
    def test_backing_is_marked_before_the_tape_directory_is_created(self):
        db = mock.MagicMock()
        db.tape_exists.return_value = True
        backup = LTOBackup(db, "", governor=None, log_dir=self.tmp)
        backup.eject_tape = lambda _drive: None
        robocopy, patches = self._patches()
        with patches[0], patches[1], patches[2], \
                mock.patch("src.backup.note_tape_io_error") as io_error:
            with self.assertRaises(RuntimeError) as ctx:
                backup.run(
                    self.pack, self.tape, "Tape_T",
                    on_write_start=lambda: self.events.append("backing"))

        # The chunk was 'backing' BEFORE the first write-touch of the tape.
        self.assertEqual(self.events, ["backing", "tape_mkdir"])
        self.assertIn("write protected", str(ctx.exception))
        robocopy.assert_not_called()
        # ...and it is recorded as a tape I/O error (readiness invalidated).
        io_error.assert_called_once()
        # The staged pack is untouched.
        self.assertTrue(
            os.path.exists(os.path.join(self.pack, "Bundle_0001.zip")))


class WriterClassifiesAnEarlyTapeErrorAsHard(_Workspace):
    def _orchestrator(self, writer):
        from src import remote_orchestrator as ro
        orch = ro.RemoteOrchestrator.__new__(ro.RemoteOrchestrator)
        orch.cfg = SimpleNamespace(lto_drive=self.tape, backup_log_dir=self.tmp)
        orch.db = mock.MagicMock()
        orch.db.tape_exists.return_value = True
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
        orch._pre_write_safety_gate = lambda *a, **k: None
        orch._cleanup_dir = mock.MagicMock()
        orch._ensure_remote_chunk_fits_tape = lambda *a, **k: True
        orch.governor = None
        orch._backup_writer = lambda cls=None: writer(orch)
        return orch

    def _desc(self):
        return SimpleNamespace(
            chunk_index=0, pack_dir=self.pack, fetch_dir=None,
            staged_bytes=2048, skip_tape=False, session_id=37,
            packaging_format=ContainerFormat.ZIP,
            metadata=[{"is_packed": True, "container_name": "Bundle_0001.zip",
                       "file_name": "a.txt", "original_path": "/vault/a.txt",
                       "file_size_bytes": 10, "stored_path": "a.txt"}],
            source_missing_files=[], containers=[], artifacts=[])

    def _statuses(self, orch):
        return [c.args[2] for c in orch.db.update_chunk_status.call_args_list]

    def test_write_protected_mkdir_is_a_hard_stop_and_keeps_the_pack(self):
        def real_writer(orch):
            backup = LTOBackup(orch.db, "", governor=None, log_dir=self.tmp)
            backup.eject_tape = lambda _drive: None
            return backup

        orch = self._orchestrator(real_writer)
        robocopy, patches = self._patches()
        with patches[0], patches[1], patches[2]:
            block = orch._write_chunk_group(
                37, [self._desc()], "Tape_T", False, threading.Event())

        self.assertIsNotNone(block)
        # Hard tape failure: a human must look; never re-fetch / auto-retry.
        self.assertEqual(block.exit_code, ExitCode.SAFETY_BLOCK)
        self.assertEqual(block.reason, REASON_AMBIGUOUS_BACKING_CHUNK)
        self.assertFalse(block.resumable)
        self.assertTrue(block.preserve_pack)
        statuses = self._statuses(orch)
        self.assertIn("backing", statuses)
        self.assertNotIn("backup_failed", statuses)
        self.assertNotIn("done", statuses)
        robocopy.assert_not_called()
        # Nothing cleaned the pack away.
        orch._cleanup_dir.assert_not_called()
        self.assertTrue(
            os.path.exists(os.path.join(self.pack, "Bundle_0001.zip")))

    def test_failure_before_any_tape_touch_also_keeps_the_pack(self):
        class NotReadyWriter:
            def __init__(self, _orch):
                pass

            def run(self, **_kwargs):
                raise RuntimeError("LTO drive is not ready for backup.")

        orch = self._orchestrator(NotReadyWriter)
        block = orch._write_chunk_group(
            37, [self._desc()], "Tape_T", False, threading.Event())
        self.assertEqual(block.reason, REASON_TAPE_WRITE_FAILED)
        self.assertNotIn("backing", self._statuses(orch))
        # The pack is intact local data: it must be kept for a direct resume,
        # not deleted and re-fetched.
        self.assertTrue(block.preserve_pack)


if __name__ == "__main__":
    unittest.main()
