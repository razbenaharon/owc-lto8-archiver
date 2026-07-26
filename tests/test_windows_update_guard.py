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
                               lambda **_kw: ["update staged"]):
            s.start()
            self.assertTrue(stop.wait(timeout=3),
                            "sentinel must ask the pipeline to stop")
        s.stop()
        self.assertTrue(s.triggered)

    def test_sentinel_stays_quiet_on_a_clean_host(self):
        stop = wug.threading.Event()
        s = wug.RebootSentinel(stop, poll_seconds=0.01)
        with mock.patch.object(wug, "pending_reboot_reasons", lambda **_kw: []):
            s.start()
            self.assertFalse(stop.wait(timeout=0.5))
        s.stop()
        self.assertFalse(s.triggered)

    def test_sentinel_fires_on_detect_callback(self):
        stop = wug.threading.Event()
        seen = []
        s = wug.RebootSentinel(stop, poll_seconds=0.01, on_detect=seen.append)
        with mock.patch.object(wug, "pending_reboot_reasons",
                               lambda **_kw: ["staged"]):
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
                               lambda **_kw: ["staged"]):
            s.start()
            self.assertTrue(stop.wait(timeout=3))
        s.stop()

    def test_include_soft_is_forwarded_so_the_override_actually_holds(self):
        """2026-07-26: the sentinel tripped on a marker the start gate ignored.

        With block_on_pending_reboot=false the start gate proceeded, then the
        sentinel stopped the run 60s later on the same PendingFileRenameOperations
        entry — so the run could never write a chunk.
        """
        seen = {}

        def fake(include_soft=True):
            seen["include_soft"] = include_soft
            return []

        stop = wug.threading.Event()
        s = wug.RebootSentinel(stop, poll_seconds=0.01, include_soft=False)
        with mock.patch.object(wug, "pending_reboot_reasons", fake), \
             mock.patch.object(wug, "sccm_reboot_status",
                               lambda: {"reboot_pending": False,
                                        "hard_reboot_pending": False,
                                        "in_grace_period": False,
                                        "deadline": None, "determinate": True,
                                        "error": None}):
            s.start()
            self.assertFalse(stop.wait(timeout=0.5))
        s.stop()
        self.assertIs(seen["include_soft"], False)


class SoftRebootMarkerTests(unittest.TestCase):
    """PendingFileRenameOperations is not a staged restart.

    It lists file moves to apply *if* a restart happens and never causes one.
    Edge and Defender updates leave entries there for days, so treating it as
    a hard marker blocks the pipeline permanently. The hard markers must stay
    unconditional.
    """

    def setUp(self):
        self.hard_keys = set()

        def fake_open(_root, path, *_a, **_kw):
            if path in self.hard_keys:
                return mock.MagicMock()
            raise OSError("key absent")

        self.renames = None
        patches = [
            mock.patch.object(wug, "winreg", SimpleNamespace(
                HKEY_LOCAL_MACHINE=0, REG_SZ=REG_SZ, REG_DWORD=REG_DWORD,
                KEY_READ=0x20019, KEY_SET_VALUE=0x0002, OpenKey=fake_open)),
            mock.patch.object(wug, "_read_value",
                              lambda *_a, **_kw: (self.renames, REG_SZ)),
            mock.patch.object(wug, "sccm_reboot_status", lambda: dict(
                installed=False, reboot_pending=False,
                hard_reboot_pending=False, in_grace_period=False,
                deadline=None, error=None, determinate=True,
                registry_reboot_data=False)),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)

    CBS = (r"SOFTWARE\Microsoft\Windows\CurrentVersion\Component Based "
           r"Servicing\RebootPending")

    def test_soft_marker_is_reported_by_default(self):
        self.renames = ["\\??\\C:\\x"]
        self.assertIn("Files are queued to be renamed on the next restart",
                      wug.pending_reboot_reasons())

    def test_soft_marker_is_dropped_when_excluded(self):
        self.renames = ["\\??\\C:\\x"]
        self.assertEqual(wug.pending_reboot_reasons(include_soft=False), [])

    def test_hard_marker_survives_the_exclusion(self):
        """include_soft=False must not weaken the signals that matter."""
        self.renames = ["\\??\\C:\\x"]
        self.hard_keys.add(self.CBS)
        reasons = wug.pending_reboot_reasons(include_soft=False)
        self.assertEqual(
            reasons, ["Component Based Servicing has a restart pending"])

    def test_sccm_intent_survives_the_exclusion(self):
        self.renames = ["\\??\\C:\\x"]
        with mock.patch.object(wug, "sccm_reboot_status", lambda: dict(
                installed=True, reboot_pending=True,
                hard_reboot_pending=False, in_grace_period=False,
                deadline=None, error=None, determinate=True,
                registry_reboot_data=False)):
            reasons, _ = wug.reboot_block_reasons(include_soft=False)
        self.assertTrue(any("SCCM" in r for r in reasons), reasons)


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
                               lambda **_kw: ["update staged"]), \
             mock.patch.object(cli, "pause_windows_updates") as pause:
            proceed, applied = cli._start_windows_update_guard(self._cfg())
        self.assertFalse(proceed)
        self.assertFalse(applied)
        pause.assert_not_called()

    def test_pending_reboot_override_proceeds_and_pauses(self):
        from src import cli
        with mock.patch.object(cli, "restore_stale_guard", lambda: False), \
             mock.patch.object(cli, "pending_reboot_reasons",
                               lambda **_kw: ["update staged"]), \
             mock.patch.object(cli, "pause_windows_updates", lambda d: True):
            proceed, applied = cli._start_windows_update_guard(
                self._cfg(windows_update_block_on_pending_reboot=False))
        self.assertTrue(proceed)
        self.assertTrue(applied)

    def test_clean_host_pauses_and_reports_applied(self):
        from src import cli
        with mock.patch.object(cli, "restore_stale_guard", lambda: False), \
             mock.patch.object(cli, "pending_reboot_reasons", lambda **_kw: []), \
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
             mock.patch.object(cli, "pending_reboot_reasons", lambda **_kw: []), \
             mock.patch.object(cli, "pause_windows_updates",
                               lambda d: calls.append("pause") or True):
            cli._start_windows_update_guard(self._cfg())
        self.assertEqual(calls, ["restore", "pause"])


if __name__ == "__main__":
    unittest.main()


class LtfsMediaHealthTests(unittest.TestCase):
    """The guard that would have caught the 2026-07-24 cartridge freeze.

    Four days before Tape_02 was frozen permanently, the drive had already
    failed 45 LOCATE operations with a write-perm error. LTFS masked every one
    ("Replace a return code to -1201") and nothing read the log. These tests pin
    the two properties that matter: the early warning is detected, and ordinary
    mode-sense chatter does not cry wolf.
    """

    # id, level, timestamp, pid, tid, drive, message
    ROWS = [
        ('61259', 'Information', '2026/07/20 10:00:00.000', '1', '2', 'Z',
         'Sync type is "time", Sync time is 300 sec'),
        ('62173', 'Information', '2026/07/20 10:05:00.000', '1', '2', 'Z',
         'Error on modesense: Not Ready to Ready Transition, Medium May Have '
         'Changed (-20601) 0000000000.'),
        ('17267', 'Error', '2026/07/20 19:42:11.557', '1', '2', 'Z',
         'Locate command returns write-perm error (-20301). Replace a return '
         'code to -1201.'),
        ('62173', 'Information', '2026/07/24 07:07:41.022', '1', '2', 'Z',
         'Error on write: Track Following Error (Servo) (-20301) 0000000000.'),
        ('12045', 'Error', '2026/07/24 07:08:18.709', '1', '2', 'Z',
         'Cannot write block: backend call failed (-20301). Dropping to '
         'read-only mode.'),
    ]

    def _log(self, rows=None):
        import csv as _csv
        fd, path = tempfile.mkstemp(suffix=".csv")
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as fh:
            _csv.writer(fh).writerows(rows if rows is not None else self.ROWS)
        self.addCleanup(os.unlink, path)
        return path

    def test_benign_mode_sense_is_not_a_fault(self):
        """The one false positive that would make the guard untrustworthy."""
        rows = [r for r in self.ROWS if r[2].startswith('2026/07/20 10:')]
        health = wug.ltfs_media_health(log_path=self._log(rows))
        self.assertTrue(health["determinate"])
        self.assertTrue(health["ok"], health)
        self.assertEqual(health["fatal"], [])
        self.assertEqual(health["degraded"], [])

    def test_locate_write_perm_is_caught_as_early_warning(self):
        health = wug.ltfs_media_health(log_path=self._log())
        self.assertTrue(health["determinate"])
        self.assertFalse(health["ok"])
        ids = {e["id"] for e in health["degraded"]}
        self.assertIn(17267, ids)          # the masked LOCATE failure
        self.assertIn(62173, ids)          # the servo error, by message
        self.assertIn(12045, {e["id"] for e in health["fatal"]})

    def test_since_scopes_evidence_to_the_current_mount(self):
        """A previous cartridge's faults must not block the current one."""
        health = wug.ltfs_media_health(
            since_iso="2026-07-25T00:00:00", log_path=self._log())
        self.assertTrue(health["determinate"])
        self.assertTrue(health["ok"], health)

    def test_newest_event_is_reported_first(self):
        health = wug.ltfs_media_health(log_path=self._log())
        self.assertEqual(health["fatal"][0]["id"], 12045)
        self.assertTrue(
            health["degraded"][0]["at"] > health["degraded"][-1]["at"])

    def test_unreadable_log_is_indeterminate_so_callers_fail_closed(self):
        health = wug.ltfs_media_health(log_path=os.path.join(
            tempfile.gettempdir(), "definitely-not-here-ltfs.csv"))
        self.assertFalse(health["determinate"])
        self.assertFalse(health["ok"])
        self.assertIn("cannot read", health["error"])


class LtfsMountAnchorTests(unittest.TestCase):
    """The mount window must be anchored to the process that owns the mount.

    2026-07-26: after a cartridge swap the LTFS services were restarted but the
    GUI helper ``LtfsGuiCancelShutdown`` survived from the previous day. The
    anchor was "earliest process named *ltfs*", so the evidence window opened
    ~28 h too early, swept in the OLD cartridge's LOCATE faults, and refused to
    write to a brand-new, provably clean tape.
    """

    MOUNT_START = "2026-07-26T14:18:50.4401480+03:00"
    STALE_HELPER_START = "2026-07-25T10:17:39.3542840+03:00"

    def _patch_query(self, payload):
        """Stand in for the single PowerShell round-trip."""
        proc = mock.Mock(returncode=0, stdout=json.dumps(payload), stderr="")
        p = mock.patch.object(wug.subprocess, "run", return_value=proc)
        self.addCleanup(p.stop)
        p.start()

    def _payload(self, **over):
        data = {
            "MountProcStart": self.MOUNT_START,
            "ProcStart": self.STALE_HELPER_START,
            "EventTime": "2026-07-26T14:18:50.5588063+03:00",
            "EventMsg": 'Sync type is "time", Sync time is 300 sec',
        }
        data.update(over)
        return data

    @mock.patch.object(wug.os, "name", "nt")
    def test_anchors_on_the_mount_process_not_a_stale_helper(self):
        self._patch_query(self._payload())
        info = wug.ltfs_current_mount_status()
        self.assertEqual(info["mount_started_at"], self.MOUNT_START)
        self.assertNotEqual(info["mount_started_at"], self.STALE_HELPER_START)
        self.assertTrue(info["determinate"])
        self.assertTrue(info["ok"], info)

    @mock.patch.object(wug.os, "name", "nt")
    def test_falls_back_to_the_broad_match_when_no_mount_process(self):
        """SDE builds that name the mount process differently still work."""
        self._patch_query(self._payload(MountProcStart=""))
        info = wug.ltfs_current_mount_status()
        self.assertEqual(info["mount_started_at"], self.STALE_HELPER_START)
        self.assertTrue(info["determinate"])

    @mock.patch.object(wug.os, "name", "nt")
    def test_no_ltfs_process_at_all_fails_closed(self):
        self._patch_query(self._payload(MountProcStart="", ProcStart=""))
        info = wug.ltfs_current_mount_status()
        self.assertFalse(info["determinate"])
        self.assertFalse(info["mount_identified"])
        self.assertIsNone(info["mount_started_at"])

    @mock.patch.object(wug.os, "name", "nt")
    def test_declaration_predating_the_mount_process_fails_closed(self):
        """A 61259 line from the previous mount must never approve this one."""
        self._patch_query(self._payload(
            EventTime="2026-07-25T10:14:37.6480000+03:00"))
        info = wug.ltfs_current_mount_status()
        self.assertFalse(info["bound_to_current"])
        self.assertFalse(info["determinate"])
