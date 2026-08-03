"""Plan 1 / Task 0.3 — the incremental-scan activation gate.

``decide_scan_mode`` is the single place the frontier can be turned on. The
properties asserted here are the ones that keep a live session safe:

* default OFF, and any unrecognised config value also means OFF;
* the flag alone never activates anything — migration 014 must be installed
  **and** finalized;
* a database that cannot answer is "not ready", never "ready";
* a session already bound to frontier state is **blocked**, not quietly handed
  back to whole-root replay, so two scanners can never run against one frontier;
* nothing in this gate migrates, backfills or writes anything.
"""
import configparser
import os
import unittest
from types import SimpleNamespace
from unittest import mock

from src import remote_orchestrator as ro
from src.config import ConfigManager
from src.exit_codes import ExitCode, REASON_SCAN_FRONTIER_UNAVAILABLE
from src.scan_frontier import (MODE_BLOCKED, MODE_FRONTIER, MODE_LEGACY,
                               decide_scan_mode,
                               incremental_scan_schema_ready)

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class _DB:
    """Database double whose schema answers are set per test."""

    def __init__(self, installed=None, finalized=None, raises=False):
        self._installed = installed
        self._finalized = finalized
        self._raises = raises
        if installed is None:
            del self.incremental_scan_schema_installed
        if finalized is None:
            del self.incremental_scan_schema_finalized

    def incremental_scan_schema_installed(self):
        if self._raises:
            raise RuntimeError("connection lost")
        return self._installed

    def incremental_scan_schema_finalized(self):
        if self._raises:
            raise RuntimeError("connection lost")
        return self._finalized


def _db(installed=True, finalized=True, raises=False):
    """A namespace-backed double, so a probe can be genuinely absent."""
    obj = SimpleNamespace()
    if installed is not None:
        obj.incremental_scan_schema_installed = (
            (lambda: (_ for _ in ()).throw(RuntimeError("connection lost")))
            if raises else (lambda: installed))
    if finalized is not None:
        obj.incremental_scan_schema_finalized = (
            (lambda: (_ for _ in ()).throw(RuntimeError("connection lost")))
            if raises else (lambda: finalized))
    return obj


def _cfg(enabled):
    return SimpleNamespace(incremental_scan_enabled=enabled)


# =============================================================================
# A. Configuration
# =============================================================================
def _config_without_the_key():
    """A ConfigManager over a config.ini that does not mention the setting.

    Deliberately NOT ``ConfigManager()``: that reads the live ``config.ini`` on
    whatever host the suite runs on, so it asserts the operator's current
    production setting rather than the code's default. That made this test fail
    the moment the flag was legitimately enabled in production on 2026-08-03 —
    it was measuring the wrong thing. Pointing it at a config that omits the key
    tests the fallback in ``src/config.py``, which is what "default" means and
    is the thing a regression would actually break.
    """
    import tempfile
    handle = tempfile.NamedTemporaryFile(
        "w", suffix=".ini", delete=False, encoding="utf-8")
    handle.write("[REMOTE]\nremote_host = example\n")
    handle.close()
    return ConfigManager(config_path=handle.name)


class ConfigFlagTests(unittest.TestCase):
    def test_default_is_off(self):
        self.assertFalse(_config_without_the_key().incremental_scan_enabled)

    def test_malformed_values_fall_back_to_off(self):
        cfg = ConfigManager()
        for raw in ("maybe", "", "0", "off", "TRUEISH", "  ", "2"):
            with mock.patch.object(cfg.config, "get", return_value=raw):
                self.assertFalse(cfg.incremental_scan_enabled, raw)

    def test_explicit_truthy_values_are_accepted(self):
        cfg = ConfigManager()
        for raw in ("true", "TRUE", " yes ", "on", "1"):
            with mock.patch.object(cfg.config, "get", return_value=raw):
                self.assertTrue(cfg.incremental_scan_enabled, raw)

    def test_example_config_ships_the_flag_disabled(self):
        parser = configparser.ConfigParser()
        parser.read(os.path.join(PROJECT_ROOT, "config.example.ini"),
                    encoding="utf-8")
        self.assertEqual(
            parser.get("REMOTE", "incremental_scan").strip().lower(), "false")

    def test_example_config_documents_the_migration_precondition(self):
        with open(os.path.join(PROJECT_ROOT, "config.example.ini"),
                  encoding="utf-8") as handle:
            text = handle.read()
        self.assertIn("incremental_scan", text)
        self.assertIn("migration 014", text)


# =============================================================================
# B. Schema readiness
# =============================================================================
class SchemaReadinessTests(unittest.TestCase):
    def test_both_halves_of_migration_014_are_required(self):
        ready, reason = incremental_scan_schema_ready(
            _db(installed=True, finalized=False))
        self.assertFalse(ready)
        self.assertEqual(reason, "migration_014_not_finalized")

        ready, reason = incremental_scan_schema_ready(
            _db(installed=False, finalized=True))
        self.assertFalse(ready)
        self.assertEqual(reason, "migration_014_not_installed")

    def test_an_absent_probe_is_not_installed(self):
        ready, reason = incremental_scan_schema_ready(SimpleNamespace())
        self.assertFalse(ready)
        self.assertEqual(reason, "migration_014_not_installed")

    def test_a_database_that_cannot_answer_is_never_ready(self):
        ready, reason = incremental_scan_schema_ready(_db(raises=True))
        self.assertFalse(ready)
        self.assertEqual(reason, "schema_state_indeterminate")

    def test_both_present_and_finalized_is_ready(self):
        ready, reason = incremental_scan_schema_ready(_db())
        self.assertTrue(ready)
        self.assertEqual(reason, "schema_ready")


# =============================================================================
# C. The gate
# =============================================================================
class DecideScanModeTests(unittest.TestCase):
    def test_disabled_flag_keeps_the_legacy_scanner(self):
        decision = decide_scan_mode(_cfg(False), _db())
        self.assertEqual(decision.mode, MODE_LEGACY)
        self.assertEqual(decision.reason, "disabled_by_config")
        self.assertFalse(decision.frontier_enabled)
        self.assertFalse(decision.blocked)

    def test_a_config_without_the_attribute_is_treated_as_disabled(self):
        decision = decide_scan_mode(SimpleNamespace(), _db())
        self.assertEqual(decision.mode, MODE_LEGACY)

    def test_enabled_but_unmigrated_keeps_the_legacy_scanner_and_says_why(self):
        decision = decide_scan_mode(_cfg(True), _db(installed=False))
        self.assertEqual(decision.mode, MODE_LEGACY)
        self.assertEqual(decision.reason, "migration_014_not_installed")
        self.assertIn("nothing was migrated", decision.detail)

    def test_enabled_but_unfinalized_keeps_the_legacy_scanner(self):
        decision = decide_scan_mode(_cfg(True), _db(finalized=False))
        self.assertEqual(decision.mode, MODE_LEGACY)
        self.assertEqual(decision.reason, "migration_014_not_finalized")

    def test_indeterminate_schema_keeps_the_legacy_scanner(self):
        decision = decide_scan_mode(_cfg(True), _db(raises=True))
        self.assertEqual(decision.mode, MODE_LEGACY)
        self.assertEqual(decision.reason, "schema_state_indeterminate")

    def test_explicit_activation_requires_flag_and_finalized_schema(self):
        decision = decide_scan_mode(_cfg(True), _db())
        self.assertEqual(decision.mode, MODE_FRONTIER)
        self.assertTrue(decision.frontier_enabled)
        self.assertEqual(decision.reason, "enabled_and_schema_ready")

    def test_a_frontier_bound_session_is_blocked_not_returned_to_replay(self):
        for cfg, db in ((_cfg(False), _db()),
                        (_cfg(True), _db(installed=False)),
                        (_cfg(True), _db(raises=True))):
            decision = decide_scan_mode(cfg, db,
                                        session_bound_to_frontier=True)
            self.assertEqual(decision.mode, MODE_BLOCKED)
            self.assertTrue(decision.blocked)
            self.assertFalse(decision.frontier_enabled)
            self.assertIn("cannot fall back to whole-root replay",
                          decision.detail)

    def test_a_frontier_bound_session_with_a_ready_schema_runs_the_frontier(self):
        decision = decide_scan_mode(_cfg(True), _db(),
                                    session_bound_to_frontier=True)
        self.assertEqual(decision.mode, MODE_FRONTIER)

    def test_the_gate_writes_nothing(self):
        db = mock.MagicMock(spec=["incremental_scan_schema_installed",
                                  "incremental_scan_schema_finalized"])
        db.incremental_scan_schema_installed.return_value = True
        db.incremental_scan_schema_finalized.return_value = True
        decide_scan_mode(_cfg(True), db)
        # Only the two read-only probes were ever called.
        self.assertEqual(db.method_calls and
                         {c[0] for c in db.method_calls},
                         {"incremental_scan_schema_installed",
                          "incremental_scan_schema_finalized"})


# =============================================================================
# D. Orchestrator wiring
# =============================================================================
class OrchestratorFrontierGateTests(unittest.TestCase):
    """The orchestrator side of the gate, after Plan 1 completion.

    The old ``_resolve_scan_mode``/``_session_bound_to_frontier`` pair chose
    between two scanners and fell back to the legacy one on uncertainty. There
    is no second scanner to fall back to any more, so the orchestrator's job
    changed shape: it must **refuse to scan at all** when the frontier schema
    is unusable, and it must decide whether an imported segment needs
    reconciling against a pre-frontier snapshot.

    A downgrade is not an option here, and that is deliberate: a fallback is
    exactly how two scanners end up running against one frontier.
    """

    def _orchestrator(self, db):
        orch = ro.RemoteOrchestrator.__new__(ro.RemoteOrchestrator)
        orch.cfg = _cfg(True)
        orch.db = db
        orch.notifier = None
        return orch

    # -- the schema gate ------------------------------------------------
    def test_a_ready_schema_lets_the_run_proceed(self):
        orch = self._orchestrator(_db(installed=True, finalized=True))
        self.assertIsNone(orch._require_frontier_schema(37))

    def test_an_uninstalled_schema_stops_the_run(self):
        orch = self._orchestrator(_db(installed=False))
        with mock.patch("builtins.print"),                 mock.patch.object(ro, "send_best_effort", lambda *a, **k: None):
            block = orch._require_frontier_schema(37)
        self.assertIsNotNone(block)
        self.assertEqual(block.exit_code, ExitCode.SAFETY_BLOCK)
        self.assertEqual(block.reason, REASON_SCAN_FRONTIER_UNAVAILABLE)
        self.assertFalse(block.resumable)

    def test_an_unfinalized_schema_stops_the_run(self):
        """Both halves of migration 014 are required. The finalized half is
        what makes the unique ordinal constraint real."""
        orch = self._orchestrator(_db(installed=True, finalized=False))
        with mock.patch("builtins.print"),                 mock.patch.object(ro, "send_best_effort", lambda *a, **k: None):
            block = orch._require_frontier_schema(37)
        self.assertIsNotNone(block)
        self.assertEqual(block.reason, REASON_SCAN_FRONTIER_UNAVAILABLE)

    def test_an_indeterminate_schema_stops_the_run(self):
        """A database that cannot answer is never treated as ready."""
        orch = self._orchestrator(_db(installed=True, finalized=True,
                                      raises=True))
        with mock.patch("builtins.print"),                 mock.patch.object(ro, "send_best_effort", lambda *a, **k: None):
            block = orch._require_frontier_schema(37)
        self.assertIsNotNone(block)
        self.assertEqual(block.reason, REASON_SCAN_FRONTIER_UNAVAILABLE)

    def test_the_gate_never_offers_a_legacy_fallback(self):
        """Structural: the refusal path must not mention a downgrade."""
        import inspect
        source = inspect.getsource(
            ro.RemoteOrchestrator._require_frontier_schema)
        self.assertNotIn("build_legacy_scanner_factory", source)
        self.assertNotIn("MODE_LEGACY", source)
        self.assertIn("SAFETY_BLOCK", source)

    # -- legacy reconciliation decision ---------------------------------
    def test_a_session_without_frontier_state_needs_reconciling(self):
        db = _db(installed=True, finalized=True)
        db.session_has_frontier_state = lambda session_id: False
        self.assertTrue(self._orchestrator(db)._session_predates_frontier(37))

    def test_a_frontier_born_session_needs_no_reconciling(self):
        db = _db(installed=True, finalized=True)
        db.session_has_frontier_state = lambda session_id: True
        self.assertFalse(self._orchestrator(db)._session_predates_frontier(37))

    def test_an_unreadable_probe_fails_towards_reconciling(self):
        """Reconciling a session that did not need it costs one set-based query
        per segment. Skipping it for one that did would re-plan files that are
        already on tape, so the uncertain answer is the expensive one."""
        db = _db(installed=True, finalized=True)

        def boom(session_id):
            raise RuntimeError("connection lost")
        db.session_has_frontier_state = boom
        self.assertTrue(self._orchestrator(db)._session_predates_frontier(37))

    def test_a_missing_probe_fails_towards_reconciling(self):
        db = _db(installed=True, finalized=True)
        if hasattr(db, "session_has_frontier_state"):
            del db.session_has_frontier_state
        self.assertTrue(self._orchestrator(db)._session_predates_frontier(37))

    # -- the run stops at the gate --------------------------------------
    def test_streaming_session_stops_before_any_device_or_thread_work(self):
        orch = ro.RemoteOrchestrator.__new__(ro.RemoteOrchestrator)
        orch.cfg = _cfg(True)
        orch.db = mock.MagicMock()
        orch.db.get_remote_session.return_value = {
            "tape_label": "Tape_TEST", "scan_complete": False}
        orch.db.incremental_scan_schema_installed.return_value = False
        orch.notifier = None
        orch._assert_ownership_preflight = lambda *a, **k: None
        orch._assert_feature_gate = lambda *a, **k: None
        orch._finalize = lambda result, phase="pipeline": result
        with mock.patch("builtins.print"),                 mock.patch.object(ro, "send_best_effort", lambda *a, **k: None),                 mock.patch.object(ro, "threading") as fake_threading:
            result = orch._run_streaming_session(37)
        self.assertEqual(result.reason, REASON_SCAN_FRONTIER_UNAVAILABLE)
        # Stopped before any worker thread, the tape generation check, the
        # drive-ready probe and the cartridge read.
        self.assertEqual(fake_threading.Thread.call_count, 0)
        orch.db.get_tape.assert_not_called()


if __name__ == "__main__":
    unittest.main()
