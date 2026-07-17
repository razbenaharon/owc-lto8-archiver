"""Tests for the forced-Windows-Update guard.

The registry itself is never touched here: _read_value/_write_value/
_delete_value are swapped for an in-memory dict so the snapshot/restore
logic — the part that has to put this host back exactly as it found it —
can be exercised without changing real machine state.
"""
import json
import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

from src import windows_update_guard as wug

REG_SZ = 1
REG_DWORD = 4


class FakeRegistry:
    """Minimal stand-in keyed by (path, name), mirroring winreg semantics."""

    def __init__(self, initial=None):
        self.data = dict(initial or {})

    def read(self, _root, path, name):
        entry = self.data.get((path, name))
        return (None, None) if entry is None else entry

    def write(self, _root, path, name, value, regtype):
        self.data[(path, name)] = (value, regtype)

    def delete(self, _root, path, name):
        self.data.pop((path, name), None)


class _GuardTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.state_file = os.path.join(self.tmp.name, "_wu_guard.json")

        self.reg = FakeRegistry()
        patches = [
            mock.patch.object(wug, "_read_value", self.reg.read),
            mock.patch.object(wug, "_write_value", self.reg.write),
            mock.patch.object(wug, "_delete_value", self.reg.delete),
            mock.patch.object(wug, "_STATE_FILE", self.state_file),
            mock.patch.object(wug, "BACKUP_LOG_DIR", self.tmp.name),
            mock.patch.object(wug, "_is_admin", lambda: True),
            # winreg is only referenced for its HKEY/REG_* constants here.
            mock.patch.object(wug, "winreg", SimpleNamespace(
                HKEY_LOCAL_MACHINE=0, REG_SZ=REG_SZ, REG_DWORD=REG_DWORD,
                KEY_READ=0x20019, KEY_SET_VALUE=0x0002)),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)


class PauseTests(_GuardTestCase):
    def test_pause_sets_expiry_and_blocks_autoreboot(self):
        self.assertTrue(wug.pause_windows_updates(7))

        expiry, regtype = self.reg.data[(wug._UX_PATH, "PauseUpdatesExpiryTime")]
        self.assertEqual(regtype, REG_SZ)
        self.assertTrue(expiry.endswith("Z"))

        # All five Settings-app pause values are written, not just the expiry.
        for name in wug._UX_PAUSE_VALUES:
            self.assertIn((wug._UX_PATH, name), self.reg.data)

        self.assertEqual(
            self.reg.data[(wug._AU_PATH, "NoAutoRebootWithLoggedOnUsers")],
            (1, REG_DWORD))

    def test_pause_days_clamped_to_windows_maximum(self):
        wug.pause_windows_updates(9999)
        start, _ = self.reg.data[(wug._UX_PATH, "PauseQualityUpdatesStartTime")]
        end, _ = self.reg.data[(wug._UX_PATH, "PauseQualityUpdatesEndTime")]
        span_days = (wug.datetime.strptime(end, "%Y-%m-%dT%H:%M:%SZ")
                     - wug.datetime.strptime(start, "%Y-%m-%dT%H:%M:%SZ")).days
        self.assertEqual(span_days, wug._MAX_PAUSE_DAYS)

    def test_state_file_written_before_registry_is_touched(self):
        """A crash mid-pause must still leave the next run enough to undo it."""
        def explode(*_a, **_kw):
            self.assertTrue(os.path.exists(self.state_file),
                            "snapshot must be persisted before the first write")
            raise OSError("registry write failed")

        with mock.patch.object(wug, "_write_value", explode):
            self.assertFalse(wug.pause_windows_updates(7))

    def test_failed_pause_rolls_back_and_clears_state(self):
        with mock.patch.object(wug, "_write_value",
                               mock.Mock(side_effect=OSError("nope"))):
            self.assertFalse(wug.pause_windows_updates(7))
        self.assertFalse(os.path.exists(self.state_file))

    def test_pause_without_admin_is_a_noop(self):
        with mock.patch.object(wug, "_is_admin", lambda: False):
            self.assertFalse(wug.pause_windows_updates(7))
        self.assertEqual(self.reg.data, {})
        self.assertFalse(os.path.exists(self.state_file))


class RestoreTests(_GuardTestCase):
    def test_resume_restores_absent_values_as_absent(self):
        wug.pause_windows_updates(7)
        wug.resume_windows_updates()

        # Nothing was set before the pause, so nothing may survive it.
        self.assertEqual(self.reg.data, {})
        self.assertFalse(os.path.exists(self.state_file))

    def test_resume_restores_preexisting_values_verbatim(self):
        self.reg.data[(wug._UX_PATH, "PauseUpdatesExpiryTime")] = (
            "2026-01-01T00:00:00Z", REG_SZ)
        self.reg.data[(wug._AU_PATH, "NoAutoRebootWithLoggedOnUsers")] = (
            0, REG_DWORD)

        wug.pause_windows_updates(7)
        self.assertNotEqual(
            self.reg.data[(wug._UX_PATH, "PauseUpdatesExpiryTime")][0],
            "2026-01-01T00:00:00Z")

        wug.resume_windows_updates()
        self.assertEqual(
            self.reg.data[(wug._UX_PATH, "PauseUpdatesExpiryTime")],
            ("2026-01-01T00:00:00Z", REG_SZ))
        self.assertEqual(
            self.reg.data[(wug._AU_PATH, "NoAutoRebootWithLoggedOnUsers")],
            (0, REG_DWORD))

    def test_stale_guard_from_killed_run_is_restored_on_next_start(self):
        """The force-kill deadlock recovery must not leave updates paused."""
        wug.pause_windows_updates(7)
        self.assertTrue(self.reg.data)  # pause is live

        # Simulate the next process start: state file survived, memory did not.
        self.assertTrue(wug.restore_stale_guard())
        self.assertEqual(self.reg.data, {})
        self.assertFalse(os.path.exists(self.state_file))

    def test_restore_stale_guard_without_state_file_is_a_noop(self):
        self.assertFalse(wug.restore_stale_guard())

    def test_corrupt_state_file_does_not_raise(self):
        with open(self.state_file, "w", encoding="utf-8") as fh:
            fh.write("{not json")
        self.assertFalse(wug.restore_stale_guard())

    def test_resume_without_pause_is_a_noop(self):
        wug.resume_windows_updates()
        self.assertEqual(self.reg.data, {})


class PendingRebootTests(_GuardTestCase):
    def test_pending_file_rename_is_reported(self):
        self.reg.data[(r"SYSTEM\CurrentControlSet\Control\Session Manager",
                       "PendingFileRenameOperations")] = (["a", "b"], 7)
        # OpenKey is only used for the two presence-probe keys; both absent.
        with mock.patch.object(wug.winreg, "OpenKey",
                               mock.Mock(side_effect=OSError), create=True):
            reasons = wug.pending_reboot_reasons()
        self.assertEqual(len(reasons), 1)
        self.assertIn("renamed", reasons[0])

    def test_clean_host_reports_no_reasons(self):
        with mock.patch.object(wug.winreg, "OpenKey",
                               mock.Mock(side_effect=OSError), create=True):
            self.assertEqual(wug.pending_reboot_reasons(), [])


class CliWiringTests(unittest.TestCase):
    """The decision logic in cli._start_windows_update_guard."""

    def _cfg(self, **overrides):
        data = {
            "windows_update_guard": True,
            "windows_update_pause_days": 7,
            "windows_update_block_on_pending_reboot": True,
        }
        data.update(overrides)
        return SimpleNamespace(**data)

    def test_disabled_guard_proceeds_without_applying(self):
        from src.cli import _start_windows_update_guard
        proceed, applied = _start_windows_update_guard(
            self._cfg(windows_update_guard=False))
        self.assertTrue(proceed)
        self.assertFalse(applied)

    def test_pending_reboot_blocks_the_run(self):
        from src import cli
        with mock.patch.object(cli, "restore_stale_guard", lambda: False), \
             mock.patch.object(cli, "pending_reboot_reasons",
                               lambda: ["update staged"]), \
             mock.patch.object(cli, "pause_windows_updates") as pause:
            proceed, applied = cli._start_windows_update_guard(self._cfg())
        self.assertFalse(proceed)
        self.assertFalse(applied)
        pause.assert_not_called()

    def test_pending_reboot_override_proceeds_and_pauses(self):
        from src import cli
        with mock.patch.object(cli, "restore_stale_guard", lambda: False), \
             mock.patch.object(cli, "pending_reboot_reasons",
                               lambda: ["update staged"]), \
             mock.patch.object(cli, "pause_windows_updates", lambda d: True):
            proceed, applied = cli._start_windows_update_guard(
                self._cfg(windows_update_block_on_pending_reboot=False))
        self.assertTrue(proceed)
        self.assertTrue(applied)

    def test_clean_host_pauses_and_reports_applied(self):
        from src import cli
        with mock.patch.object(cli, "restore_stale_guard", lambda: False), \
             mock.patch.object(cli, "pending_reboot_reasons", lambda: []), \
             mock.patch.object(cli, "pause_windows_updates", lambda d: True):
            proceed, applied = cli._start_windows_update_guard(self._cfg())
        self.assertTrue(proceed)
        self.assertTrue(applied)

    def test_stale_guard_is_restored_before_a_new_pause(self):
        """Ordering matters: a new snapshot must not capture the old pause."""
        from src import cli
        calls = []
        with mock.patch.object(cli, "restore_stale_guard",
                               lambda: calls.append("restore")), \
             mock.patch.object(cli, "pending_reboot_reasons", lambda: []), \
             mock.patch.object(cli, "pause_windows_updates",
                               lambda d: calls.append("pause") or True):
            cli._start_windows_update_guard(self._cfg())
        self.assertEqual(calls, ["restore", "pause"])


if __name__ == "__main__":
    unittest.main()
