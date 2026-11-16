"""Audit M1: the forced-restart (SCCM) check must not fail OPEN.

Two holes:

* ``sccm_reboot_status`` answered "no restart pending, determinate" whenever
  the client SDK query said *Invalid namespace* — even when SCCM's own restart
  scheduling data was present in the registry, and even on a host that does
  have the Configuration Manager client (a broken / mid-repair client).
* ``_pre_tape_write_reboot_check`` fell back to the Windows markers alone when
  the whole assessment raised, so an unevaluable state allowed a tape write.

And the constraint that keeps the fix honest: a host with **no SCCM client at
all** (the normal case outside a managed fleet) is determinate and must not be
blocked — not by a missing namespace and not by a PowerShell hiccup.

Nothing here queries WMI, PowerShell or the real registry.
"""
import unittest
from types import SimpleNamespace
from unittest import mock

from src import remote_orchestrator as ro
from src import windows_update_guard as wug
from src.pipeline_types import StagedChunk

NO_NAMESPACE = RuntimeError("Invalid namespace root\\ccm\\ClientSDK")
RPC_DOWN = RuntimeError("The RPC server is unavailable")


class _Base(unittest.TestCase):
    def setUp(self):
        for target, kwargs in (
                ((wug.os, "name"), {"new": "nt"}),
                ((wug, "pending_reboot_signals"), {"return_value": []}),
        ):
            patcher = mock.patch.object(*target, **kwargs)
            patcher.start()
            self.addCleanup(patcher.stop)

    def _status(self, query_error, client, registry=False):
        with mock.patch.object(wug, "_sccm_query_client_sdk",
                               side_effect=query_error), \
                mock.patch.object(wug, "_sccm_client_installed",
                                  return_value=client, create=True), \
                mock.patch.object(wug, "_sccm_registry_reboot_data",
                                  return_value=registry):
            status = wug.sccm_reboot_status()
            assessment = wug.assess_reboot_state(block_on_unknown=True)
        return status, assessment


class SccmPresentButUnaskable(_Base):
    def test_missing_namespace_with_client_installed_is_indeterminate(self):
        status, assessment = self._status(NO_NAMESPACE, client=True)
        self.assertFalse(status["determinate"])
        self.assertTrue(assessment.blocking)
        self.assertIn(wug.REBOOT_SCCM_INDETERMINATE,
                      [s.code for s in assessment.critical])

    def test_missing_namespace_with_staged_restart_data_is_pending(self):
        # "Does not exist" must not outvote SCCM's own restart scheduling data.
        status, assessment = self._status(NO_NAMESPACE, client=True,
                                          registry=True)
        self.assertTrue(status["reboot_pending"])
        self.assertTrue(assessment.blocking)
        status, assessment = self._status(NO_NAMESPACE, client=False,
                                          registry=True)
        self.assertTrue(status["reboot_pending"])
        self.assertTrue(assessment.blocking)

    def test_missing_namespace_with_unknown_presence_is_indeterminate(self):
        status, assessment = self._status(NO_NAMESPACE, client=None)
        self.assertFalse(status["determinate"])
        self.assertTrue(assessment.blocking)

    def test_unreachable_client_is_indeterminate(self):
        status, assessment = self._status(RPC_DOWN, client=True)
        self.assertFalse(status["determinate"])
        self.assertTrue(assessment.blocking)


class NoSccmClientAtAll(_Base):
    """The open-source user's machine: must never be blocked by SCCM."""

    def test_missing_namespace_without_client_is_determinate_and_clear(self):
        status, assessment = self._status(NO_NAMESPACE, client=False)
        self.assertTrue(status["determinate"])
        self.assertFalse(status["reboot_pending"])
        self.assertFalse(status["installed"])
        self.assertFalse(assessment.blocking)

    def test_any_query_failure_without_client_is_determinate_and_clear(self):
        status, assessment = self._status(RPC_DOWN, client=False)
        self.assertTrue(status["determinate"])
        self.assertFalse(assessment.blocking)

    def test_windows_signals_still_block_without_sccm(self):
        cbs = wug.RebootSignal(wug.REBOOT_CBS_PENDING, wug.SEVERITY_CRITICAL,
                               "reg", "CBS restart pending")
        with mock.patch.object(wug, "pending_reboot_signals",
                               return_value=[cbs]):
            _status, assessment = self._status(NO_NAMESPACE, client=False)
        self.assertTrue(assessment.blocking)


class ClientPresenceProbe(unittest.TestCase):
    """The probe reads the CcmExec service key and is honest about doubt."""

    def _probe(self, open_key):
        fake = mock.MagicMock()
        fake.OpenKey = open_key
        with mock.patch.object(wug, "winreg", fake):
            return wug._sccm_client_installed()

    def test_service_key_present_means_installed(self):
        self.assertIs(self._probe(mock.MagicMock()), True)

    def test_service_key_absent_means_not_installed(self):
        self.assertIs(
            self._probe(mock.Mock(side_effect=FileNotFoundError())), False)

    def test_unreadable_registry_is_unknown_not_absent(self):
        self.assertIsNone(
            self._probe(mock.Mock(side_effect=PermissionError("denied"))))

    def test_no_registry_module_is_unknown(self):
        with mock.patch.object(wug, "winreg", None):
            self.assertIsNone(wug._sccm_client_installed())


class GateFailsClosedWhenItCannotEvaluate(unittest.TestCase):
    def setUp(self):
        self.orch = object.__new__(ro.RemoteOrchestrator)
        self.orch.notifier = None
        self.orch.cfg = SimpleNamespace(
            windows_update_block_on_pending_reboot=True)
        self.desc = StagedChunk(chunk_index=22, fetch_dir="f", pack_dir="p",
                                metadata=[], staged_bytes=10)

    def _check(self, windows_markers):
        with mock.patch.object(ro, "assess_reboot_state",
                               side_effect=RuntimeError("wmi exploded")), \
                mock.patch.object(ro, "pending_reboot_reasons",
                                  return_value=list(windows_markers)), \
                mock.patch("builtins.print"):
            return self.orch._pre_tape_write_reboot_check(37, self.desc, "T")

    def test_unevaluable_state_blocks_even_with_no_windows_marker(self):
        reasons, sccm = self._check([])
        self.assertTrue(reasons, "an unevaluable restart state allowed a write")
        self.assertIn("could not be evaluated", reasons[-1])
        self.assertFalse(sccm["determinate"])

    def test_windows_markers_are_kept_alongside_the_unknown(self):
        reasons, _sccm = self._check(["CBS restart pending"])
        self.assertEqual(reasons[0], "CBS restart pending")
        self.assertEqual(len(reasons), 2)

    def test_the_stop_is_transient_not_a_safety_block(self):
        reasons, sccm = self._check([])
        from src.exit_codes import REASON_SCCM_REBOOT_PENDING
        self.assertEqual(self.orch._reboot_reason_slug(reasons, sccm),
                         REASON_SCCM_REBOOT_PENDING)

    def test_operator_override_still_applies(self):
        self.orch.cfg = SimpleNamespace(
            windows_update_block_on_pending_reboot=False)
        reasons, _sccm = self._check([])
        self.assertEqual(reasons, [])


if __name__ == "__main__":
    unittest.main()
