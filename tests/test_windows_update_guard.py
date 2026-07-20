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
            # The sentinel now unions SCCM's own restart intent with the Windows
            # markers. Stub it clear so these cases keep testing the markers;
            # SCCM's own behaviour is covered in test_sccm_reboot_guard.py.
            # Without this the sentinel would shell out to PowerShell mid-test.
            mock.patch.object(wug, "sccm_reboot_status", lambda: dict(
                installed=False, reboot_pending=False,
                hard_reboot_pending=False, in_grace_period=False,
                deadline=None, error=None, determinate=True,
                registry_reboot_data=False)),
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


class ManagedPolicyTests(_GuardTestCase):
    """The host that lost 126 GB is WSUS-managed; the pause is cosmetic there."""

    def test_unmanaged_host_reports_not_managed(self):
        info = wug.managed_update_policy()
        self.assertFalse(info["managed"])
        self.assertFalse(info["pause_disabled"])
        self.assertEqual(info["notes"], [])

    def test_wsus_managed_host_is_detected(self):
        self.reg.data[(wug._WU_POLICY_PATH, "WUServer")] = (
            "https://wsus.example:8531", REG_SZ)
        self.reg.data[(wug._AU_PATH, "UseWUServer")] = (1, REG_DWORD)
        info = wug.managed_update_policy()
        self.assertTrue(info["managed"])
        self.assertIn("wsus.example", info["notes"][0])

    def test_pause_disabled_by_policy_is_detected(self):
        """SetDisablePauseUXAccess=1 means the pause we write is ignored."""
        self.reg.data[(wug._WU_POLICY_PATH, "SetDisablePauseUXAccess")] = (
            1, REG_DWORD)
        info = wug.managed_update_policy()
        self.assertTrue(info["managed"])
        self.assertTrue(info["pause_disabled"])

    def test_compliance_deadline_is_reported_with_days(self):
        self.reg.data[(wug._WU_POLICY_PATH, "SetComplianceDeadline")] = (
            1, REG_DWORD)
        self.reg.data[(wug._WU_POLICY_PATH,
                       "ConfigureDeadlineForQualityUpdates")] = (2, REG_DWORD)
        info = wug.managed_update_policy()
        self.assertTrue(info["managed"])
        self.assertEqual(info["deadline_days"], 2)

    def test_managed_host_status_never_claims_it_is_paused(self):
        """A false 'paused' line is worse than none — it invites trust."""
        self.reg.data[(wug._WU_POLICY_PATH, "SetDisablePauseUXAccess")] = (
            1, REG_DWORD)
        policy = wug.managed_update_policy()
        with mock.patch("builtins.print") as p:
            wug.print_guard_status(True, policy)
        out = " ".join(str(c.args[0]) for c in p.call_args_list if c.args)
        self.assertIn("NOT reliable protection", out)
        self.assertNotIn("Windows Update paused for this run", out)

    def test_unmanaged_host_status_confirms_the_pause(self):
        policy = wug.managed_update_policy()
        with mock.patch("builtins.print") as p:
            wug.print_guard_status(True, policy)
        out = " ".join(str(c.args[0]) for c in p.call_args_list if c.args)
        self.assertIn("paused for this run", out)


class RebootSentinelTests(_GuardTestCase):
    """The sentinel is the only real guard on an admin-managed host."""

    def test_sentinel_sets_stop_event_when_restart_is_staged(self):
        stop = wug.threading.Event()
        s = wug.RebootSentinel(stop, poll_seconds=0.01)
        with mock.patch.object(wug, "pending_reboot_reasons",
                               lambda: ["update staged"]):
            s.start()
            self.assertTrue(stop.wait(timeout=3),
                            "sentinel must ask the pipeline to stop")
        s.stop()
        self.assertTrue(s.triggered)

    def test_sentinel_stays_quiet_on_a_clean_host(self):
        stop = wug.threading.Event()
        s = wug.RebootSentinel(stop, poll_seconds=0.01)
        with mock.patch.object(wug, "pending_reboot_reasons", lambda: []):
            s.start()
            self.assertFalse(stop.wait(timeout=0.5))
        s.stop()
        self.assertFalse(s.triggered)

    def test_sentinel_fires_on_detect_callback(self):
        stop = wug.threading.Event()
        seen = []
        s = wug.RebootSentinel(stop, poll_seconds=0.01, on_detect=seen.append)
        with mock.patch.object(wug, "pending_reboot_reasons",
                               lambda: ["staged"]):
            s.start()
            stop.wait(timeout=3)
        s.stop()
        self.assertEqual(seen, [["staged"]])

    def test_registry_error_never_kills_the_pipeline(self):
        stop = wug.threading.Event()
        s = wug.RebootSentinel(stop, poll_seconds=0.01)
        with mock.patch.object(wug, "pending_reboot_reasons",
                               mock.Mock(side_effect=OSError("hive gone"))):
            s.start()
            self.assertFalse(stop.wait(timeout=0.4),
                             "a registry hiccup must not stop the run")
        s.stop()

    def test_on_detect_failure_still_stops_the_pipeline(self):
        """A broken Telegram notifier must not cost us the clean stop."""
        stop = wug.threading.Event()

        def boom(_reasons):
            raise RuntimeError("notifier down")

        s = wug.RebootSentinel(stop, poll_seconds=0.01, on_detect=boom)
        with mock.patch.object(wug, "pending_reboot_reasons",
                               lambda: ["staged"]):
            s.start()
            self.assertTrue(stop.wait(timeout=3))
        s.stop()


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
