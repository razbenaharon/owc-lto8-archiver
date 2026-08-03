"""The fail-closed schema gate for the sole production frontier scanner.

There is no runtime scan-mode selection and no incremental-scan feature flag.
The properties asserted here keep the fixed production path safe:

* migration 014 must be installed **and** finalized;
* a database that cannot answer is "not ready", never "ready";
* the orchestrator stops before worker or device work when it is not ready;
* probing readiness never migrates, backfills or writes anything.
"""
import configparser
import os
import tempfile
import unittest
import warnings
from types import SimpleNamespace
from unittest import mock

from src import remote_orchestrator as ro
from src.config import ConfigManager
from src.exit_codes import ExitCode, REASON_SCAN_FRONTIER_UNAVAILABLE
from src.scan_frontier import incremental_scan_schema_ready

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


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


# =============================================================================
# A. Configuration surface
# =============================================================================
class ConfigurationSurfaceTests(unittest.TestCase):
    def test_config_manager_exposes_no_incremental_scan_feature_flag(self):
        self.assertFalse(hasattr(ConfigManager, "incremental_scan_enabled"))

    def test_example_config_has_no_incremental_scan_feature_flag(self):
        parser = configparser.ConfigParser()
        parser.read(os.path.join(PROJECT_ROOT, "config.example.ini"),
                    encoding="utf-8")
        self.assertFalse(parser.has_option("REMOTE", "incremental_scan"))

    def test_example_config_documents_the_fixed_frontier_path(self):
        with open(os.path.join(PROJECT_ROOT, "config.example.ini"),
                  encoding="utf-8") as handle:
            text = handle.read()
        self.assertIn("sole production scanner", text)
        self.assertIn("no incremental-scan feature flag", text)
        self.assertIn("migration 014", text.lower())

    def test_retired_key_is_warned_once_and_ignored(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = os.path.join(temp_dir, "old.ini")
            with open(path, "w", encoding="utf-8") as handle:
                handle.write("[REMOTE]\nincremental_scan = true\n")
            with mock.patch("src.config._incremental_scan_warning_emitted",
                            False), mock.patch(
                                "src.config._load_env_file", return_value={}
                            ), warnings.catch_warnings(record=True) as seen:
                warnings.simplefilter("always")
                first = ConfigManager(config_path=path)
                ConfigManager(config_path=path)

        matching = [item for item in seen
                    if "deprecated and ignored" in str(item.message)]
        self.assertEqual(len(matching), 1)
        self.assertFalse(hasattr(first, "incremental_scan_enabled"))


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
# C. The production gate
# =============================================================================
class SchemaProbePurityTests(unittest.TestCase):
    def test_the_schema_probe_writes_nothing(self):
        db = mock.MagicMock(spec=["incremental_scan_schema_installed",
                                  "incremental_scan_schema_finalized"])
        db.incremental_scan_schema_installed.return_value = True
        db.incremental_scan_schema_finalized.return_value = True
        incremental_scan_schema_ready(db)
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
        orch.cfg = SimpleNamespace()
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

    def test_the_run_path_has_no_mode_or_feature_flag_branch(self):
        import inspect
        source = inspect.getsource(
            ro.RemoteOrchestrator._run_streaming_session)
        self.assertIn("_require_frontier_schema", source)
        self.assertNotIn("decide_scan_mode", source)
        self.assertNotIn("incremental_scan_enabled", source)

    # -- legacy reconciliation decision ---------------------------------
    def test_bootstrap_row_marks_a_migrated_session_after_scopes_exist(self):
        db = _db(installed=True, finalized=True)
        db.get_frontier_bootstrap = lambda session_id: {
            "session_id": session_id, "state": "running"}
        db.session_has_frontier_state = lambda _session_id: (_ for _ in ()).throw(
            AssertionError("frontier scope presence is not an origin signal"))
        self.assertTrue(self._orchestrator(db)._session_predates_frontier(37))

    def test_no_bootstrap_requires_proof_that_snapshot_membership_is_empty(self):
        db = _db(installed=True, finalized=True)
        db.get_frontier_bootstrap = lambda _session_id: None
        db.session_has_snapshot_membership = lambda _session_id: False
        db.session_has_frontier_state = lambda _session_id: (_ for _ in ()).throw(
            AssertionError("frontier scope presence must not be probed"))
        self.assertFalse(self._orchestrator(db)._session_predates_frontier(37))

        db.session_has_snapshot_membership = lambda _session_id: True
        self.assertTrue(self._orchestrator(db)._session_predates_frontier(37))

        db.session_has_snapshot_membership = lambda _session_id: None
        self.assertTrue(self._orchestrator(db)._session_predates_frontier(37))

        def boom(_session_id):
            raise RuntimeError("membership unavailable")
        db.session_has_snapshot_membership = boom
        self.assertTrue(self._orchestrator(db)._session_predates_frontier(37))

    def test_an_unreadable_probe_fails_towards_reconciling(self):
        """Reconciling a session that did not need it costs one set-based query
        per segment. Skipping it for one that did would re-plan files that are
        already on tape, so the uncertain answer is the expensive one."""
        db = _db(installed=True, finalized=True)

        def boom(session_id):
            raise RuntimeError("connection lost")
        db.get_frontier_bootstrap = boom
        self.assertTrue(self._orchestrator(db)._session_predates_frontier(37))

    def test_a_missing_probe_fails_towards_reconciling(self):
        db = _db(installed=True, finalized=True)
        self.assertTrue(self._orchestrator(db)._session_predates_frontier(37))

    def test_an_indeterminate_probe_result_fails_towards_reconciling(self):
        db = _db(installed=True, finalized=True)
        db.get_frontier_bootstrap = lambda _session_id: False
        self.assertTrue(self._orchestrator(db)._session_predates_frontier(37))

    # -- the run stops at the gate --------------------------------------
    def test_streaming_session_stops_before_any_device_or_thread_work(self):
        orch = ro.RemoteOrchestrator.__new__(ro.RemoteOrchestrator)
        orch.cfg = SimpleNamespace()
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
