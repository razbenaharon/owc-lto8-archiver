"""Phase 5B.5: the sealed-batch feature GATE is wired into startup, disabled.

Proves the fail-closed gate does nothing when the flag is false, blocks before
worker threads when the flag is true but the feature is not ready, and — even
when it passes — creates or schedules no batch (5B.5 wires only the gate).
"""
import threading
import unittest
from types import SimpleNamespace
from unittest import mock

from src import remote_orchestrator as ro
from src import sealed_batch_repository as sbr
from src.exit_codes import ExitCode, REASON_SEALED_BATCH_FEATURE_UNAVAILABLE
from src.sealed_batch_repository import SchemaDriftError


def _orch(flag):
    o = ro.RemoteOrchestrator.__new__(ro.RemoteOrchestrator)
    o.cfg = SimpleNamespace(sealed_tape_write_batches_enabled=flag,
                            db_dsn="postgresql://unused/nodb")
    o._finalize = lambda result, phase="pipeline": result
    return o


class FeatureGateTests(unittest.TestCase):
    def test_flag_false_creates_no_repository(self):
        o = _orch(False)
        with mock.patch.object(
                sbr, "SealedBatchRepository",
                side_effect=AssertionError("repository constructed while flag "
                                           "disabled")):
            self.assertIsNone(o._assert_feature_gate(37, "startup"))

    def test_flag_false_is_a_noop(self):
        o = _orch(False)
        # No DB, no schema check, no repository: the gate simply returns None.
        self.assertIsNone(o._assert_feature_gate(37, "startup"))

    def test_flag_true_missing_schema_blocks(self):
        o = _orch(True)
        fake = mock.MagicMock(unsafe=True)
        fake.schema_applied.return_value = False
        with mock.patch.object(sbr, "SealedBatchRepository", return_value=fake):
            block = o._assert_feature_gate(37, "startup")
        self.assertIsNotNone(block)
        self.assertEqual(block.exit_code, ExitCode.SAFETY_BLOCK)
        self.assertEqual(block.reason, REASON_SEALED_BATCH_FEATURE_UNAVAILABLE)

    def test_flag_true_schema_drift_blocks(self):
        o = _orch(True)
        fake = mock.MagicMock(unsafe=True)
        fake.schema_applied.return_value = True
        fake.assert_schema_valid.side_effect = SchemaDriftError("drift")
        with mock.patch.object(sbr, "SealedBatchRepository", return_value=fake):
            block = o._assert_feature_gate(37, "startup")
        self.assertIsNotNone(block)
        self.assertEqual(block.reason, REASON_SEALED_BATCH_FEATURE_UNAVAILABLE)

    def test_flag_true_valid_schema_passes_gate_only(self):
        o = _orch(True)
        fake = mock.MagicMock(unsafe=True)
        fake.schema_applied.return_value = True
        fake.assert_schema_valid.return_value = None
        with mock.patch.object(sbr, "SealedBatchRepository", return_value=fake):
            self.assertIsNone(o._assert_feature_gate(37, "startup"))
        # Passing the gate must NOT create or schedule any batch in 5B.5.
        fake.create_building_batch.assert_not_called()
        fake.add_chunk.assert_not_called()
        fake.seal_batch.assert_not_called()
        fake.claim_for_write.assert_not_called()

    def test_streaming_startup_blocks_before_threads_on_bad_feature(self):
        # With the flag on and schema missing, _run_streaming_session must return
        # the block before any worker thread is constructed.
        o = ro.RemoteOrchestrator.__new__(ro.RemoteOrchestrator)
        o.cfg = SimpleNamespace(sealed_tape_write_batches_enabled=True,
                                db_dsn="postgresql://unused/nodb", lto_drive="X:")
        o.db = mock.MagicMock()
        o._finalize = lambda result, phase="pipeline": result
        fake = mock.MagicMock(unsafe=True)
        fake.schema_applied.return_value = False
        with mock.patch.object(ro.OWNERSHIP, "assert_production_scope",
                               return_value=True), \
                mock.patch.object(sbr, "SealedBatchRepository",
                                  return_value=fake), \
                mock.patch.object(ro, "threading") as fake_threading:
            result = o._run_streaming_session(37)
        self.assertEqual(fake_threading.Thread.call_count, 0)
        self.assertEqual(result.reason, REASON_SEALED_BATCH_FEATURE_UNAVAILABLE)
        o.db.get_remote_session.assert_not_called()


if __name__ == "__main__":
    unittest.main()
