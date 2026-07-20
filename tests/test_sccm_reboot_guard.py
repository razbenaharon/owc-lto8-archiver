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
from unittest import mock

from src import windows_update_guard as wug
from src import remote_orchestrator as ro
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


class RebootBlockReasonsTests(unittest.TestCase):
    """Scenario 7, plus the unknown-state asymmetry between the two callers."""

    def test_windows_marker_alone_blocks_without_sccm(self):
        with mock.patch.object(wug, "pending_reboot_reasons",
                               return_value=["CBS has a restart pending"]), \
             mock.patch.object(wug, "sccm_reboot_status",
                               return_value=dict(reboot_pending=False,
                                                 hard_reboot_pending=False,
                                                 in_grace_period=False,
                                                 deadline=None, error=None,
                                                 determinate=True)):
            reasons, _ = wug.reboot_block_reasons()
        self.assertEqual(len(reasons), 1)

    def test_unknown_sccm_blocks_a_write_but_not_the_sentinel(self):
        unknown = dict(reboot_pending=False, hard_reboot_pending=False,
                       in_grace_period=False, deadline=None,
                       error="wmi down", determinate=False)
        with mock.patch.object(wug, "pending_reboot_reasons", return_value=[]), \
             mock.patch.object(wug, "sccm_reboot_status", return_value=unknown):
            gate, _ = wug.reboot_block_reasons(block_on_unknown=True)
            sentinel, _ = wug.reboot_block_reasons(block_on_unknown=False)

        # The gate refuses to start a write blind...
        self.assertTrue(gate)
        # ...but a WMI hiccup must not stop a healthy run mid-flight.
        self.assertEqual(sentinel, [])


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
        self.orch = object.__new__(ro.RemoteOrchestrator)
        self.orch.notifier = None
        self.desc = StagedChunk(chunk_index=22, fetch_dir="f", pack_dir="p",
                                metadata=[], staged_bytes=10)

    def test_clear_state_allows_the_write(self):
        with mock.patch.object(ro, "reboot_block_reasons",
                               return_value=([], {"determinate": True})):
            reasons, _ = self.orch._pre_tape_write_reboot_check(37, self.desc, "T")
        self.assertEqual(reasons, [])

    def test_pending_restart_blocks_the_write(self):
        with mock.patch.object(
                ro, "reboot_block_reasons",
                return_value=(["SCCM has a restart pending"],
                              {"reboot_pending": True})), \
             mock.patch.object(ro, "send_best_effort") as notify:
            reasons, _ = self.orch._pre_tape_write_reboot_check(37, self.desc, "T")
        self.assertTrue(reasons)
        notify.assert_called_once()

    def test_gate_failure_falls_back_to_windows_markers(self):
        """The gate must never be the thing that takes the pipeline down."""
        with mock.patch.object(ro, "reboot_block_reasons",
                               side_effect=RuntimeError("boom")), \
             mock.patch.object(ro, "pending_reboot_reasons",
                               return_value=["CBS restart pending"]):
            reasons, sccm = self.orch._pre_tape_write_reboot_check(
                37, self.desc, "T")
        self.assertEqual(reasons, ["CBS restart pending"])
        self.assertIsNone(sccm)


class TransientFetchRetryTests(unittest.TestCase):
    """The 2026-07-17 root cause: a momentary DNS failure must not kill the run.

    ``ssh: Could not resolve hostname so01`` stopped the whole streaming session
    and, with the monitor offline, it sat idle ~3 days. A transient blip should
    cost a short backoff, not the run.
    """

    DNS_ERR = ("remote tar/ssh exit 255: ssh: Could not resolve hostname "
               "so01.iem.technion.ac.il: Name or service not known")

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

        with mock.patch.object(ro, "_remote_tar_fetch", side_effect=fake_fetch):
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

        with mock.patch.object(ro, "_remote_tar_fetch", side_effect=fake_fetch):
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

        with mock.patch.object(ro, "_remote_tar_fetch", side_effect=fake_fetch):
            ok, err = orch._fetch_one_batch("base", [(0, "rel", 0)], "d", abort)
        self.assertFalse(ok)
        self.assertEqual(calls["n"], 4, "1 initial try + 3 retries, then give up")

    def test_abort_during_retry_stops_immediately(self):
        orch = self._orch(retries=5)
        abort = threading.Event()

        def fake_fetch(*a, **k):
            abort.set()  # a sibling stream failed while we were retrying
            return False, self.DNS_ERR

        with mock.patch.object(ro, "_remote_tar_fetch", side_effect=fake_fetch):
            ok, err = orch._fetch_one_batch("base", [(0, "rel", 0)], "d", abort)
        self.assertFalse(ok)
        self.assertEqual(err, "cancelled")


if __name__ == "__main__":
    unittest.main()
