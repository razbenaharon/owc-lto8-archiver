"""SCCM restart detection and the pack-preserving stop.

Why this file exists: on 2026-07-15 a restart killed an active tape write and
chunks 18-91 of session 37 (~126 GB) were lost. The post-mortem in CLAUDE.md
blamed a WSUS compliance deadline, but System log event 1074 names the actual
initiator — ``CcmExec.exe``, with the Software Center wording "Your computer
will restart at 15/07/2026 10:39:01 to complete the installation of
applications and software updates". SCCM is a different control plane from
Windows Update, so the WU pause could never have influenced it and the WU
pending-restart markers are not authoritative for it.

The warning was 60 seconds. A chunk cycle is ~70 minutes. That gap is why the
check has to run synchronously immediately before each write rather than rely on
the sentinel's 60s poll.
"""
import json
import os
import tempfile
import threading
import unittest
from types import SimpleNamespace
from unittest import mock

from src import windows_update_guard as wug
from src import remote_orchestrator as ro
from src import remote_staging as rs
from src.pipeline_types import StagedChunk


def _sdk(pending=False, hard=False, grace=False, deadline=""):
    return {"ReturnValue": 0, "RebootPending": pending,
            "IsHardRebootPending": hard, "InGracePeriod": grace,
            "RebootDeadline": deadline}


class SccmRebootStatusTests(unittest.TestCase):
    """Scenarios 1-6: what the client SDK reports, and what we do when it can't."""

    def setUp(self):
        patcher = mock.patch.object(wug.os, "name", "nt")
        patcher.start()
        self.addCleanup(patcher.stop)
        reg = mock.patch.object(wug, "_sccm_registry_reboot_data",
                                return_value=False)
        reg.start()
        self.addCleanup(reg.stop)

    def test_reboot_pending_is_reported(self):
        with mock.patch.object(wug, "_sccm_query_client_sdk",
                               return_value=_sdk(pending=True)):
            s = wug.sccm_reboot_status()
        self.assertTrue(s["reboot_pending"])
        self.assertTrue(s["determinate"])
        self.assertTrue(s["installed"])

    def test_hard_reboot_pending_is_reported(self):
        with mock.patch.object(wug, "_sccm_query_client_sdk",
                               return_value=_sdk(pending=True, hard=True)):
            s = wug.sccm_reboot_status()
        self.assertTrue(s["hard_reboot_pending"])

    def test_grace_period_is_reported(self):
        with mock.patch.object(wug, "_sccm_query_client_sdk",
                               return_value=_sdk(pending=True, grace=True)):
            s = wug.sccm_reboot_status()
        self.assertTrue(s["in_grace_period"])

    def test_real_deadline_is_kept_and_epoch_is_discarded(self):
        with mock.patch.object(
                wug, "_sccm_query_client_sdk",
                return_value=_sdk(pending=True,
                                  deadline="2026-07-18T10:00:00.0000000+03:00")):
            s = wug.sccm_reboot_status()
        self.assertEqual(s["deadline"], "2026-07-18T10:00:00.0000000+03:00")

        # SCCM reports "no deadline" as the epoch, not as null. Surfacing
        # 1970 as a deadline would read as a wildly overdue restart.
        with mock.patch.object(
                wug, "_sccm_query_client_sdk",
                return_value=_sdk(deadline="1970-01-01T04:00:00.0000000+04:00")):
            s = wug.sccm_reboot_status()
        self.assertIsNone(s["deadline"])

    def test_query_failure_is_indeterminate_not_clear(self):
        """A client that will not answer must never read as 'no restart'."""
        with mock.patch.object(wug, "_sccm_query_client_sdk",
                               side_effect=RuntimeError("rpc unavailable")):
            s = wug.sccm_reboot_status()
        self.assertFalse(s["determinate"])
        self.assertFalse(s["reboot_pending"])
        self.assertIn("rpc unavailable", s["error"])

    def test_host_without_sccm_is_determinate_and_clear(self):
        """No client means nothing can stage an SCCM restart. Not an error."""
        with mock.patch.object(
                wug, "_sccm_query_client_sdk",
                side_effect=RuntimeError("Invalid namespace root\\ccm")):
            s = wug.sccm_reboot_status()
        self.assertTrue(s["determinate"])
        self.assertFalse(s["reboot_pending"])
        self.assertFalse(s["installed"])

    def test_registry_asserts_pending_only_when_sdk_is_unreachable(self):
        with mock.patch.object(wug, "_sccm_registry_reboot_data",
                               return_value=True), \
             mock.patch.object(wug, "_sccm_query_client_sdk",
                               side_effect=RuntimeError("wmi down")):
            s = wug.sccm_reboot_status()
        self.assertTrue(s["reboot_pending"])

        # The SDK is the authority: when it answers, it wins over the registry.
        with mock.patch.object(wug, "_sccm_registry_reboot_data",
                               return_value=True), \
             mock.patch.object(wug, "_sccm_query_client_sdk",
                               return_value=_sdk(pending=False)):
            s = wug.sccm_reboot_status()
        self.assertFalse(s["reboot_pending"])


def _clear_sccm(**over):
    base = dict(reboot_pending=False, hard_reboot_pending=False,
                in_grace_period=False, deadline=None, error=None,
                determinate=True, installed=True, registry_reboot_data=False)
    base.update(over)
    return base


def _signal(code, severity, message="m", source="s"):
    return wug.RebootSignal(code, severity, source, message)


class RebootBlockReasonsTests(unittest.TestCase):
    """Scenario 7, plus the unknown-state asymmetry between the two callers."""

    def test_windows_marker_alone_blocks_without_sccm(self):
        with mock.patch.object(
                wug, "pending_reboot_signals",
                return_value=[_signal(wug.REBOOT_CBS_PENDING,
                                      wug.SEVERITY_CRITICAL,
                                      "CBS has a restart pending")]), \
             mock.patch.object(wug, "sccm_reboot_status",
                               return_value=_clear_sccm()):
            reasons, _ = wug.reboot_block_reasons()
        self.assertEqual(len(reasons), 1)

    def test_unknown_sccm_blocks_a_write_but_not_the_sentinel(self):
        unknown = _clear_sccm(error="wmi down", determinate=False)
        with mock.patch.object(wug, "pending_reboot_signals", return_value=[]), \
             mock.patch.object(wug, "sccm_reboot_status", return_value=unknown):
            gate, _ = wug.reboot_block_reasons(block_on_unknown=True)
            sentinel, _ = wug.reboot_block_reasons(block_on_unknown=False)

        # The gate refuses to start a write blind...
        self.assertTrue(gate)
        # ...but a WMI hiccup must not stop a healthy run mid-flight.
        self.assertEqual(sentinel, [])


class RebootSeverityClassificationTests(unittest.TestCase):
    """2026-07-28: severity, not message text, decides whether a write blocks.

    Root cause being locked down here: `PendingFileRenameOperations` is a list
    of moves to apply *if* a restart happens — it never causes one. It used to
    be indistinguishable from a real staged restart, so the only way to stop it
    blocking every tape write was `block_on_pending_reboot = false`, which also
    disabled blocking on SCCM, CBS and Windows Update. The guard that matters
    was switched off to silence a stale rename queue.
    """

    # --- case 1: the false positive that caused the override ------------------

    def test_file_rename_alone_is_warning_only_and_does_not_block(self):
        rename = _signal(wug.REBOOT_FILE_RENAME, wug.SEVERITY_WARNING,
                         "Files are queued to be renamed on the next restart")
        with mock.patch.object(wug, "pending_reboot_signals",
                               return_value=[rename]), \
             mock.patch.object(wug, "sccm_reboot_status",
                               return_value=_clear_sccm()):
            a = wug.assess_reboot_state()
            reasons, _ = wug.reboot_block_reasons()

        self.assertFalse(a.blocking, "a stale rename queue must not block")
        self.assertEqual(reasons, [])
        # ...but it is NOT discarded: it stays visible in diagnostics.
        self.assertEqual(len(a.warnings), 1)
        self.assertIn(wug.REBOOT_FILE_RENAME, a.codes)
        self.assertIn("non-blocking", a.warning_summary())
        self.assertIn(wug.REBOOT_FILE_RENAME, a.warning_summary())

    # --- case 2: SCCM ---------------------------------------------------------

    def test_sccm_pending_is_critical_and_blocks(self):
        with mock.patch.object(wug, "pending_reboot_signals", return_value=[]), \
             mock.patch.object(wug, "sccm_reboot_status",
                               return_value=_clear_sccm(reboot_pending=True)):
            a = wug.assess_reboot_state()
        self.assertTrue(a.blocking)
        self.assertIn(wug.REBOOT_SCCM_PENDING, [s.code for s in a.critical])

    def test_sccm_hard_reboot_is_critical_and_blocks(self):
        with mock.patch.object(wug, "pending_reboot_signals", return_value=[]), \
             mock.patch.object(
                 wug, "sccm_reboot_status",
                 return_value=_clear_sccm(reboot_pending=True,
                                          hard_reboot_pending=True,
                                          in_grace_period=True,
                                          deadline="2026-07-29T10:00:00Z")):
            a = wug.assess_reboot_state()
        codes = [s.code for s in a.critical]
        self.assertIn(wug.REBOOT_SCCM_HARD, codes)
        self.assertTrue(a.blocking)
        # The deadline and grace period must reach the operator-facing text.
        joined = " ".join(a.blocking_reasons)
        self.assertIn("2026-07-29T10:00:00Z", joined)
        self.assertIn("grace period", joined)

    # --- case 3: Windows Update / CBS ----------------------------------------

    def test_windows_update_and_cbs_markers_are_critical(self):
        for code in (wug.REBOOT_WU_REQUIRED, wug.REBOOT_CBS_PENDING,
                     wug.REBOOT_CBS_IN_PROGRESS,
                     wug.REBOOT_CBS_PACKAGES_PENDING,
                     wug.REBOOT_WU_POST_REBOOT_REPORTING,
                     wug.REBOOT_COMPUTER_RENAME, wug.REBOOT_DOMAIN_JOIN):
            with self.subTest(code=code):
                with mock.patch.object(
                        wug, "pending_reboot_signals",
                        return_value=[_signal(code, wug.SEVERITY_CRITICAL)]), \
                     mock.patch.object(wug, "sccm_reboot_status",
                                       return_value=_clear_sccm()):
                    a = wug.assess_reboot_state()
                self.assertTrue(a.blocking, f"{code} must block a tape write")

    # --- case 4: critical always wins over warning ---------------------------

    def test_critical_wins_when_mixed_with_a_warning(self):
        signals = [
            _signal(wug.REBOOT_FILE_RENAME, wug.SEVERITY_WARNING, "renames"),
            _signal(wug.REBOOT_CBS_PENDING, wug.SEVERITY_CRITICAL, "cbs"),
        ]
        with mock.patch.object(wug, "pending_reboot_signals",
                               return_value=signals), \
             mock.patch.object(wug, "sccm_reboot_status",
                               return_value=_clear_sccm()):
            a = wug.assess_reboot_state()
            reasons, _ = wug.reboot_block_reasons()

        self.assertTrue(a.blocking)
        self.assertEqual(a.blocking_reasons, ["cbs"])
        self.assertEqual(reasons, ["cbs"])
        # The warning survives alongside it rather than being swallowed.
        self.assertEqual(a.warning_reasons, ["renames"])

    # --- case 5: nothing at all ----------------------------------------------

    def test_no_indicators_is_not_blocking(self):
        with mock.patch.object(wug, "pending_reboot_signals", return_value=[]), \
             mock.patch.object(wug, "sccm_reboot_status",
                               return_value=_clear_sccm()):
            a = wug.assess_reboot_state()
        self.assertFalse(a.blocking)
        self.assertEqual(a.signals, [])
        self.assertEqual(a.warning_summary(), "")

    # --- case 6: unknown must stay fail-closed -------------------------------

    def test_indeterminate_sccm_still_fails_closed_at_the_write_boundary(self):
        unknown = _clear_sccm(determinate=False, error="rpc unavailable")
        with mock.patch.object(wug, "pending_reboot_signals", return_value=[]), \
             mock.patch.object(wug, "sccm_reboot_status", return_value=unknown):
            gate = wug.assess_reboot_state(block_on_unknown=True)
            sentinel = wug.assess_reboot_state(block_on_unknown=False)

        self.assertTrue(gate.blocking, "unknown must never read as safe")
        self.assertIn(wug.REBOOT_SCCM_INDETERMINATE,
                      [s.code for s in gate.critical])
        # The sentinel's documented asymmetry is preserved untouched.
        self.assertFalse(sentinel.blocking)

    def test_severity_is_not_decided_by_message_text(self):
        """A reworded message must not change the block/allow outcome."""
        renamed = _signal(wug.REBOOT_FILE_RENAME, wug.SEVERITY_WARNING,
                          "Completely different wording")
        with mock.patch.object(wug, "pending_reboot_signals",
                               return_value=[renamed]), \
             mock.patch.object(wug, "sccm_reboot_status",
                               return_value=_clear_sccm()):
            self.assertFalse(wug.assess_reboot_state().blocking)


class PendingRebootSignalRegistryTests(unittest.TestCase):
    """The registry readers classify the real markers correctly."""

    def _with_registry(self, existing_keys=(), values=None):
        values = values or {}
        return (
            mock.patch.object(wug, "_key_exists",
                              side_effect=lambda p: p in existing_keys),
            mock.patch.object(
                wug, "_read_value",
                side_effect=lambda root, path, name: (
                    values.get((path, name)), None)),
        )

    def setUp(self):
        if wug.winreg is None:
            self.skipTest("winreg unavailable on this platform")

    def test_pending_file_rename_value_yields_a_warning_signal(self):
        k, v = self._with_registry(
            values={(wug._SESSION_MANAGER_PATH,
                     "PendingFileRenameOperations"): ["a\0b"]})
        with k, v:
            signals = wug.pending_reboot_signals()
        self.assertEqual([s.code for s in signals], [wug.REBOOT_FILE_RENAME])
        self.assertEqual(signals[0].severity, wug.SEVERITY_WARNING)
        # The source must point at the actual registry value, for traceability.
        self.assertIn("PendingFileRenameOperations", signals[0].source)

    def test_cbs_reboot_pending_key_yields_a_critical_signal(self):
        cbs = [p for c, p, _m in wug._CRITICAL_KEY_MARKERS
               if c == wug.REBOOT_CBS_PENDING][0]
        k, v = self._with_registry(existing_keys={cbs})
        with k, v:
            signals = wug.pending_reboot_signals()
        self.assertEqual([s.code for s in signals], [wug.REBOOT_CBS_PENDING])
        self.assertEqual(signals[0].severity, wug.SEVERITY_CRITICAL)

    def test_computer_rename_mismatch_is_critical(self):
        k, v = self._with_registry(values={
            (wug._COMPUTERNAME_ACTIVE, "ComputerName"): "EXAMPLE-HOST",
            (wug._COMPUTERNAME_PENDING, "ComputerName"): "LAB-HPLB-10",
        })
        with k, v:
            signals = wug.pending_reboot_signals()
        self.assertEqual([s.code for s in signals],
                         [wug.REBOOT_COMPUTER_RENAME])

    def test_matching_computer_name_is_not_a_signal(self):
        k, v = self._with_registry(values={
            (wug._COMPUTERNAME_ACTIVE, "ComputerName"): "EXAMPLE-HOST",
            (wug._COMPUTERNAME_PENDING, "ComputerName"): "EXAMPLE-HOST",
        })
        with k, v:
            self.assertEqual(wug.pending_reboot_signals(), [])

    def test_legacy_pending_reboot_reasons_filters_by_severity(self):
        signals = [
            _signal(wug.REBOOT_CBS_PENDING, wug.SEVERITY_CRITICAL, "cbs"),
            _signal(wug.REBOOT_FILE_RENAME, wug.SEVERITY_WARNING, "renames"),
        ]
        with mock.patch.object(wug, "pending_reboot_signals",
                               return_value=signals):
            self.assertEqual(wug.pending_reboot_reasons(), ["cbs", "renames"])
            self.assertEqual(
                wug.pending_reboot_reasons(include_soft=False), ["cbs"])


class LtfsSyncModeTests(unittest.TestCase):
    """Scenario 18: startup must block writes unless the mount declared time@5."""

    def _orch(self):
        orch = object.__new__(ro.RemoteOrchestrator)
        orch.notifier = None
        return orch

    def test_time_at_5_is_accepted(self):
        with mock.patch.object(ro, "ltfs_sync_mode_status",
                               return_value=dict(determinate=True, ok=True,
                                                 sync_type="time",
                                                 sync_seconds=300,
                                                 declared_at="x", error=None)):
            self.assertTrue(self._orch()._validate_ltfs_sync_mode())

    def test_unmount_mode_blocks_tape_writes(self):
        """The 2026-07-15 configuration. Under it a stop is not recoverable."""
        with mock.patch.object(ro, "ltfs_sync_mode_status",
                               return_value=dict(determinate=True, ok=False,
                                                 sync_type="unmount",
                                                 sync_seconds=None,
                                                 declared_at="x", error=None)), \
             mock.patch.object(ro, "send_best_effort"):
            self.assertFalse(self._orch()._validate_ltfs_sync_mode())

    def test_unreadable_log_warns_but_does_not_block(self):
        """A failed log query is not a reason to refuse to archive."""
        with mock.patch.object(ro, "ltfs_sync_mode_status",
                               return_value=dict(determinate=False, ok=False,
                                                 sync_type=None,
                                                 sync_seconds=None,
                                                 declared_at=None,
                                                 error="log missing")):
            self.assertTrue(self._orch()._validate_ltfs_sync_mode())


class PackPreservationTests(unittest.TestCase):
    """Scenarios 14-17: a stop keeps the pack, and the resume writes it directly."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.pack_dir = os.path.join(self.tmp.name, "_pack_s0037_022")
        os.makedirs(self.pack_dir)
        with open(os.path.join(self.pack_dir, "part0.tar"), "wb") as fh:
            fh.write(b"payload-bytes")

        self.orch = object.__new__(ro.RemoteOrchestrator)
        self.orch._staged_lock = threading.Lock()
        self.orch._staged_bytes = 1000
        self.orch.notifier = None
        self.orch.db = SimpleNamespace(
            get_chunk_packaging_format=lambda session_id, chunk_index: "zip")

        self.desc = StagedChunk(
            chunk_index=22,
            fetch_dir=os.path.join(self.tmp.name, "_fetch_s0037_022"),
            pack_dir=self.pack_dir,
            metadata=[{"file_name": "a.txt", "file_size_bytes": 13}],
            staged_bytes=1000,
        )

    def test_preserve_keeps_the_pack_and_writes_a_marker(self):
        self.orch._preserve_desc(37, self.desc, "stop requested before write")
        self.assertTrue(os.path.isfile(os.path.join(self.pack_dir, "part0.tar")),
                        "the pack itself must survive the stop")
        self.assertTrue(os.path.isfile(
            os.path.join(self.pack_dir, ro._RESUME_MARKER)))
        # The budget is released even though the bytes stay on disk.
        self.assertEqual(self.orch._staged_bytes, 0)

    def test_resume_reuses_the_preserved_pack_without_refetching(self):
        self.orch._preserve_desc(37, self.desc, "stop requested before write")
        self.orch._staged_bytes = 0
        got = self.orch._try_resume_pack(37, 22, self.pack_dir)
        self.assertIsNotNone(got, "an intact preserved pack must be reused")
        self.assertEqual(got.chunk_index, 22)
        self.assertEqual(got.metadata, self.desc.metadata)
        self.assertEqual(got.staged_bytes, 1000)
        self.assertEqual(self.orch._staged_bytes, 1000)

    def test_pack_without_a_marker_is_not_reused(self):
        """A pack interrupted mid-write has no marker and must be re-fetched."""
        self.assertIsNone(self.orch._try_resume_pack(37, 22, self.pack_dir))

    def test_tampered_pack_fails_its_integrity_check(self):
        self.orch._preserve_desc(37, self.desc, "queued at shutdown")
        with open(os.path.join(self.pack_dir, "part0.tar"), "ab") as fh:
            fh.write(b"extra")  # size no longer matches the recorded inventory
        self.assertIsNone(self.orch._try_resume_pack(37, 22, self.pack_dir),
                          "a changed pack must never reach the tape as good")

    def test_extra_file_in_pack_fails_its_integrity_check(self):
        self.orch._preserve_desc(37, self.desc, "queued at shutdown")
        with open(os.path.join(self.pack_dir, "stray.tar"), "wb") as fh:
            fh.write(b"stray")
        self.assertIsNone(self.orch._try_resume_pack(37, 22, self.pack_dir))

    def test_marker_from_a_different_chunk_is_rejected(self):
        self.orch._preserve_desc(37, self.desc, "queued at shutdown")
        self.assertIsNone(self.orch._try_resume_pack(37, 23, self.pack_dir))
        self.assertIsNone(self.orch._try_resume_pack(38, 22, self.pack_dir))


class PreTapeWriteGateTests(unittest.TestCase):
    """Scenarios 8-13: the gate runs before each write and never interrupts one."""

    def setUp(self):
        # object.__new__ skips __init__, so every attribute the gate reads has
        # to be supplied here by hand — including `cfg`, whose absence made all
        # three tests raise AttributeError once the gate started consulting
        # `[WINDOWS_UPDATE] block_on_pending_reboot`.
        self.orch = object.__new__(ro.RemoteOrchestrator)
        self.orch.notifier = None
        self.orch.cfg = SimpleNamespace(
            windows_update_block_on_pending_reboot=True)
        self.desc = StagedChunk(chunk_index=22, fetch_dir="f", pack_dir="p",
                                metadata=[], staged_bytes=10)

    @staticmethod
    def _assessment(signals=(), sccm=None):
        return wug.RebootAssessment(list(signals),
                                    sccm if sccm is not None
                                    else _clear_sccm())

    def test_clear_state_allows_the_write(self):
        with mock.patch.object(ro, "assess_reboot_state",
                               return_value=self._assessment()):
            reasons, _ = self.orch._pre_tape_write_reboot_check(37, self.desc, "T")
        self.assertEqual(reasons, [])

    def test_pending_restart_blocks_the_write(self):
        crit = _signal(wug.REBOOT_SCCM_PENDING, wug.SEVERITY_CRITICAL,
                       "SCCM has a restart pending")
        with mock.patch.object(
                ro, "assess_reboot_state",
                return_value=self._assessment([crit])), \
             mock.patch.object(ro, "send_best_effort") as notify:
            reasons, _ = self.orch._pre_tape_write_reboot_check(37, self.desc, "T")
        self.assertTrue(reasons)
        notify.assert_called_once()

    def test_file_rename_warning_alone_allows_the_write(self):
        """Case 1 at the gate: the exact state that forced the 07-26 override."""
        warn = _signal(wug.REBOOT_FILE_RENAME, wug.SEVERITY_WARNING,
                       "Files are queued to be renamed on the next restart")
        with mock.patch.object(ro, "assess_reboot_state",
                               return_value=self._assessment([warn])), \
             mock.patch.object(ro, "send_best_effort") as notify:
            reasons, _ = self.orch._pre_tape_write_reboot_check(37, self.desc, "T")
        self.assertEqual(reasons, [], "a stale rename queue must not stop a write")
        notify.assert_not_called()

    def test_critical_beats_a_warning_at_the_gate(self):
        """Case 4 at the gate: mixed signals must still block."""
        signals = [
            _signal(wug.REBOOT_FILE_RENAME, wug.SEVERITY_WARNING, "renames"),
            _signal(wug.REBOOT_CBS_PENDING, wug.SEVERITY_CRITICAL, "cbs"),
        ]
        with mock.patch.object(ro, "assess_reboot_state",
                               return_value=self._assessment(signals)), \
             mock.patch.object(ro, "send_best_effort"):
            reasons, _ = self.orch._pre_tape_write_reboot_check(37, self.desc, "T")
        self.assertEqual(reasons, ["cbs"])

    def test_indeterminate_state_blocks_the_write(self):
        """Case 6 at the gate: unknown stays fail-closed."""
        unknown = _signal(wug.REBOOT_SCCM_INDETERMINATE, wug.SEVERITY_CRITICAL,
                          "SCCM restart state could not be determined")
        with mock.patch.object(
                ro, "assess_reboot_state",
                return_value=self._assessment(
                    [unknown], _clear_sccm(determinate=False))), \
             mock.patch.object(ro, "send_best_effort"):
            reasons, _ = self.orch._pre_tape_write_reboot_check(37, self.desc, "T")
        self.assertTrue(reasons)

    def test_block_flag_follows_the_operator_config(self):
        self.orch.cfg = SimpleNamespace(
            windows_update_block_on_pending_reboot=False)
        self.assertFalse(self.orch._block_on_soft_reboot_marker())

        self.orch.cfg = SimpleNamespace(
            windows_update_block_on_pending_reboot=True)
        self.assertTrue(self.orch._block_on_soft_reboot_marker())

        # An older config with the key absent must fail safe (block).
        self.orch.cfg = SimpleNamespace()
        self.assertTrue(self.orch._block_on_soft_reboot_marker())

    def test_operator_override_can_still_proceed_past_a_critical_reason(self):
        """block_on_pending_reboot=false remains a deliberate escape hatch."""
        self.orch.cfg = SimpleNamespace(
            windows_update_block_on_pending_reboot=False)
        crit = _signal(wug.REBOOT_SCCM_PENDING, wug.SEVERITY_CRITICAL, "sccm")
        with mock.patch.object(ro, "assess_reboot_state",
                               return_value=self._assessment([crit])):
            reasons, _ = self.orch._pre_tape_write_reboot_check(37, self.desc, "T")
        self.assertEqual(reasons, [])

    def test_gate_failure_falls_back_to_critical_windows_markers(self):
        """The gate must never be the thing that takes the pipeline down."""
        with mock.patch.object(ro, "assess_reboot_state",
                               side_effect=RuntimeError("boom")), \
             mock.patch.object(ro, "pending_reboot_reasons",
                               return_value=["CBS restart pending"]) as legacy:
            reasons, sccm = self.orch._pre_tape_write_reboot_check(
                37, self.desc, "T")
        self.assertEqual(reasons, ["CBS restart pending"])
        self.assertIsNone(sccm)
        # The fallback must not resurrect the warning-only marker.
        self.assertIs(legacy.call_args.kwargs["include_soft"], False)


class RebootSentinelSeverityTests(unittest.TestCase):
    """The sentinel must not trip on a warning-only indicator either.

    Before the fix it did, within 60s, unless the operator disabled the guard —
    which is what made the override necessary in the first place.
    """

    def test_warning_only_does_not_trigger_the_sentinel(self):
        warn = _signal(wug.REBOOT_FILE_RENAME, wug.SEVERITY_WARNING, "renames")
        stop = threading.Event()
        sentinel = wug.RebootSentinel(stop)
        with mock.patch.object(wug, "assess_reboot_state",
                               return_value=wug.RebootAssessment([warn],
                                                                 _clear_sccm())):
            self.assertFalse(sentinel._check_once())
        self.assertFalse(stop.is_set())
        self.assertFalse(sentinel.triggered)

    def test_critical_triggers_the_sentinel_and_sets_the_stop_event(self):
        crit = _signal(wug.REBOOT_SCCM_PENDING, wug.SEVERITY_CRITICAL, "sccm")
        stop = threading.Event()
        sentinel = wug.RebootSentinel(stop)
        with mock.patch.object(wug, "assess_reboot_state",
                               return_value=wug.RebootAssessment([crit],
                                                                 _clear_sccm())):
            self.assertTrue(sentinel._check_once())
        self.assertTrue(stop.is_set())
        self.assertTrue(sentinel.triggered)


class TransientFetchRetryTests(unittest.TestCase):
    """The 2026-07-17 root cause: a momentary DNS failure must not kill the run.

    ``ssh: Could not resolve hostname srv01`` stopped the whole streaming session
    and, with the monitor offline, it sat idle ~3 days. A transient blip should
    cost a short backoff, not the run.
    """

    DNS_ERR = ("remote tar/ssh exit 255: ssh: Could not resolve hostname "
               "srv01.example.edu: Name or service not known")

    def _orch(self, retries):
        orch = object.__new__(ro.RemoteOrchestrator)
        orch.fetch_transient_retries = retries
        orch.fetch_transient_retry_base = 0  # no real sleeping in tests
        orch.remote_user = "u"
        orch.remote_host = "h"
        orch.remote_password = None
        orch.ssh_cipher = "c"
        orch.use_mbuffer = False
        orch.mbuffer_size = "512M"
        orch.fetch_cores = None
        return orch

    def test_classifier_matches_the_incident_error(self):
        self.assertTrue(ro._is_transient_fetch_error(self.DNS_ERR))
        self.assertFalse(ro._is_transient_fetch_error(
            "tar: missing.txt: No such file or directory"))

    def test_transient_error_is_retried_then_succeeds(self):
        orch = self._orch(retries=5)
        abort = threading.Event()
        calls = {"n": 0}

        def fake_fetch(*a, **k):
            calls["n"] += 1
            return (True, "") if calls["n"] >= 3 else (False, self.DNS_ERR)

        with mock.patch.object(rs, "_remote_tar_fetch", side_effect=fake_fetch):
            ok, err = orch._fetch_one_batch("base", [(0, "rel", 0)], "d", abort)
        self.assertTrue(ok)
        self.assertEqual(calls["n"], 3, "should retry until it succeeds")

    def test_fatal_error_is_not_retried(self):
        orch = self._orch(retries=5)
        abort = threading.Event()
        calls = {"n": 0}

        def fake_fetch(*a, **k):
            calls["n"] += 1
            return False, "tar: missing.txt: No such file or directory"

        with mock.patch.object(rs, "_remote_tar_fetch", side_effect=fake_fetch):
            ok, err = orch._fetch_one_batch("base", [(0, "rel", 0)], "d", abort)
        self.assertFalse(ok)
        self.assertEqual(calls["n"], 1, "a fatal error must fail fast")

    def test_retries_are_bounded(self):
        orch = self._orch(retries=3)
        abort = threading.Event()
        calls = {"n": 0}

        def fake_fetch(*a, **k):
            calls["n"] += 1
            return False, self.DNS_ERR

        with mock.patch.object(rs, "_remote_tar_fetch", side_effect=fake_fetch):
            ok, err = orch._fetch_one_batch("base", [(0, "rel", 0)], "d", abort)
        self.assertFalse(ok)
        self.assertEqual(calls["n"], 4, "1 initial try + 3 retries, then give up")

    def test_abort_during_retry_stops_immediately(self):
        orch = self._orch(retries=5)
        abort = threading.Event()

        def fake_fetch(*a, **k):
            abort.set()  # a sibling stream failed while we were retrying
            return False, self.DNS_ERR

        with mock.patch.object(rs, "_remote_tar_fetch", side_effect=fake_fetch):
            ok, err = orch._fetch_one_batch("base", [(0, "rel", 0)], "d", abort)
        self.assertFalse(ok)
        self.assertEqual(err, "cancelled")


if __name__ == "__main__":
    unittest.main()
