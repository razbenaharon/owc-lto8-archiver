"""Audit H3: the local archive path must obey the same tape-safety rules.

Before the fix the local orchestrator
  * ejected the cartridge after EVERY chunk, whatever
    ``[HARDWARE] eject_after_session`` said;
  * started tape writes with no LTFS sync-mode check, no media-health check and
    no pending-restart check;
  * accepted a label typed by the operator when the cartridge label could not
    be read, and cataloged the write under it.

Everything is faked: the "tape" is a temp directory, the writer is a recording
double, and the gate inputs (LTFS event log, SCCM) are patched at the functions
the remote writer's gate already uses.
"""
import os
import shutil
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

from src import local_orchestrator as lo
from src.windows_update_guard import (RebootAssessment, RebootSignal,
                                      SEVERITY_CRITICAL)

MOUNT_OK = {"ok": True, "determinate": True, "sync_type": "time",
            "sync_seconds": 300, "declared_at": "2026-11-01T10:00:00",
            "mount_started_at": "2026-11-01T10:00:00"}
HEALTH_OK = {"ok": True, "determinate": True, "since": "2026-11-01T10:00:00",
             "fatal": [], "degraded": []}


def _clear_assessment():
    return RebootAssessment([], {"determinate": True})


class _FakeWriter:
    def __init__(self, log):
        self.log = log

    def run(self, **kwargs):
        self.log.append(("write", kwargs["source"]))

    def eject_tape(self, drive):
        self.log.append(("eject", drive))


class _Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.tmp, True)
        self.source = os.path.join(self.tmp, "source")
        os.makedirs(os.path.join(self.source, "projA"))
        with open(os.path.join(self.source, "projA", "f.bin"), "wb") as fh:
            fh.write(b"x" * 128)
        self.tape = os.path.join(self.tmp, "fake_ltfs_mount")
        os.makedirs(self.tape)
        self.log = []

        orch = lo.LocalOrchestrator.__new__(lo.LocalOrchestrator)
        orch.cfg = SimpleNamespace(
            lto_drive=self.tape, ibm_eject_cmd=None,
            eject_after_session=False,
            windows_update_block_on_pending_reboot=True)
        orch.db = mock.MagicMock()
        orch.ui = mock.MagicMock()
        orch.skipped_tracker = mock.MagicMock()
        orch.notifier = None
        orch.governor = mock.MagicMock()
        orch.source_dir = self.source
        orch.staging_dir = os.path.join(self.tmp, "staging")
        orch._backup_writer = lambda cls=None: _FakeWriter(self.log)
        self.orch = orch
        self.session = {"session_id": 7, "source_dir": self.source,
                        "session_label": "LOCAL_source_x", "total_chunks": 2,
                        "backup_mode": "direct"}
        self.entries = [{"top_level_dir": "projA", "dir_size_bytes": 128,
                         "tape_label": None}]

        # Gate inputs, patched where the reused remote gate looks them up.
        self.mount = mock.patch(
            "src.remote_orchestrator.ltfs_current_mount_status",
            return_value=dict(MOUNT_OK))
        self.health = mock.patch(
            "src.remote_orchestrator.ltfs_media_health",
            return_value=dict(HEALTH_OK))
        self.reboot = mock.patch(
            "src.remote_orchestrator.assess_reboot_state",
            return_value=_clear_assessment())
        self.eject = mock.patch(
            "src.local_orchestrator.eject_tape_drive", create=True,
            side_effect=lambda *a, **k: self.log.append(("eject", a[0])))
        for patcher in (self.mount, self.health, self.reboot, self.eject,
                        mock.patch("builtins.print")):
            patcher.start()
            self.addCleanup(patcher.stop)

    def _writes(self):
        return [e for e in self.log if e[0] == "write"]

    def _ejects(self):
        return [e for e in self.log if e[0] == "eject"]

    def _direct(self):
        return self.orch._process_direct_chunk(
            self.session, 0, self.entries, "Tape_L")


class NoPerChunkEject(_Base):
    def test_a_finished_chunk_does_not_eject(self):
        self.assertTrue(self._direct())
        self.assertEqual(len(self._writes()), 1)
        self.assertEqual(self._ejects(), [])

    def test_a_finished_chunk_does_not_eject_even_when_the_setting_is_on(self):
        # The setting means "at session end, after confirmation" — never a
        # mechanical eject after each chunk.
        self.orch.cfg.eject_after_session = True
        self.assertTrue(self._direct())
        self.assertEqual(self._ejects(), [])


class SessionEndEject(_Base):
    def _run_session(self, typed, setting, interactive=True):
        self.orch.cfg.eject_after_session = setting
        self.orch.db.get_local_session.return_value = self.session
        self.orch.db.get_local_pending_chunks.return_value = [0, 1]
        self.orch.db.get_local_chunk_entries.return_value = self.entries
        self.orch._process_chunk = lambda *a, **k: True
        answers = iter(typed)
        with mock.patch("src.local_orchestrator._ensure_lto_drive_ready",
                        return_value=True), \
                mock.patch("src.ltfs._stdin_is_interactive",
                           return_value=interactive), \
                mock.patch("builtins.input",
                           side_effect=lambda *_a: next(answers)):
            self.orch._run_session(7)

    def test_setting_off_never_ejects(self):
        self._run_session(["", "EJECT"], setting=False)
        self.assertEqual(self._ejects(), [])

    def test_setting_on_ejects_once_at_session_end_after_typed_word(self):
        # First answer: the between-chunk prompt. Second: the confirmation.
        self._run_session(["", "EJECT"], setting=True)
        self.assertEqual(len(self._ejects()), 1)

    def test_setting_on_without_confirmation_does_not_eject(self):
        self._run_session(["", ""], setting=True)
        self.assertEqual(self._ejects(), [])
        self._run_session(["", "y"], setting=True)
        self.assertEqual(self._ejects(), [])

    def test_setting_on_with_non_interactive_stdin_does_not_eject(self):
        self._run_session(["", "EJECT"], setting=True, interactive=False)
        self.assertEqual(self._ejects(), [])


class PreWriteGates(_Base):
    def _patch(self, name, **kwargs):
        patcher = mock.patch(f"src.remote_orchestrator.{name}", **kwargs)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_all_gates_clear_allows_the_write(self):
        self.assertTrue(self._direct())
        self.assertEqual(len(self._writes()), 1)

    def test_mount_not_time5_blocks_the_write(self):
        self._patch("ltfs_current_mount_status", return_value={
            "ok": False, "determinate": True, "reason": "not_time5",
            "sync_type": "unmount", "sync_seconds": 0})
        self.assertFalse(self._direct())
        self.assertEqual(self._writes(), [])

    def test_unverifiable_mount_blocks_the_write(self):
        self._patch("ltfs_current_mount_status", return_value={
            "ok": False, "determinate": False, "error": "event log unreadable"})
        self.assertFalse(self._direct())
        self.assertEqual(self._writes(), [])

    def test_degraded_media_blocks_the_write(self):
        event = {"at": "2026-11-01T11:00:00", "id": 21702,
                 "meaning": "write perm", "message": "LOCATE write-perm"}
        self._patch("ltfs_media_health", return_value={
            "ok": False, "determinate": True, "fatal": [event],
            "degraded": []})
        self.assertFalse(self._direct())
        self.assertEqual(self._writes(), [])

    def test_unreadable_media_health_blocks_the_write(self):
        self._patch("ltfs_media_health", return_value={
            "ok": False, "determinate": False, "error": "LogFile.csv missing"})
        self.assertFalse(self._direct())
        self.assertEqual(self._writes(), [])

    def test_pending_restart_blocks_the_write(self):
        pending = RebootAssessment(
            [RebootSignal("sccm_reboot_pending", SEVERITY_CRITICAL, "SCCM",
                          "Configuration Manager (SCCM) has a restart pending")],
            {"determinate": True, "reboot_pending": True})
        self._patch("assess_reboot_state", return_value=pending)
        self.assertFalse(self._direct())
        self.assertEqual(self._writes(), [])

    def test_a_gate_that_cannot_be_evaluated_blocks_the_write(self):
        self._patch("ltfs_current_mount_status",
                    side_effect=OSError("wevtutil exploded"))
        self.assertFalse(self._direct())
        self.assertEqual(self._writes(), [])


class CartridgeIdentity(_Base):
    def _prepare(self, label_result):
        typed = mock.Mock(return_value="Tape_TYPED")
        kwargs = ({"side_effect": label_result}
                  if isinstance(label_result, Exception)
                  else {"return_value": label_result})
        with mock.patch("src.local_orchestrator.get_volume_label", **kwargs), \
                mock.patch("builtins.input", typed):
            result = self.orch._prepare_tape_for_chunk(
                self.session, 0, self.entries)
        return result, typed

    def test_unreadable_label_is_not_replaced_by_typed_input(self):
        result, typed = self._prepare(None)
        self.assertIsNone(result)
        typed.assert_not_called()
        self.orch.db.register_tape.assert_not_called()

    def test_label_read_error_fails_closed(self):
        result, typed = self._prepare(OSError("device not ready"))
        self.assertIsNone(result)
        typed.assert_not_called()

    def test_a_read_label_is_used(self):
        self.orch.db.tape_exists.return_value = True
        self.orch.db.count_tape_file_records.return_value = 0
        self.orch._tape_root_is_empty = lambda: True
        self.orch._ensure_chunk_fits_tape = lambda *a, **k: True
        result, _typed = self._prepare("Tape_L")
        self.assertEqual(result, "Tape_L")


if __name__ == "__main__":
    unittest.main()
