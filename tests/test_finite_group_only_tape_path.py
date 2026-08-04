"""Plan 1 / Task 1.4 — the finite write group is the ONLY tape access path.

The prime directive is that a failure must be recoverable without anybody
travelling to the drive. Every drive, volume, readiness and cartridge call
outside a finite write group is a chance to leave the mount in a state only a
human at the machine can clear — and, before this task, the pipeline made
several of them *at startup*, then sat idle for a whole fetch+pack cycle before
writing anything.

What is asserted here:

* zero tape calls before a finite group is ready, while waiting, between group
  members, and after release;
* exactly one readiness/cartridge gate per group;
* a NEW session names its cartridge without reading the drive, and a headless
  fresh start is still refused before any scanning or staging;
* a RESUMED session uses its persisted label/generation, never the config;
* the generation comparison is database-only and fails closed on a missing,
  null or non-active generation;
* the remote pipeline never ejects, whatever the config says.

No real drive, no IBM helper, no SSH: ``FakeLtfsAdapter``/``TapeOperationLog``
record every attempted operation.
"""
import threading
import unittest
from types import SimpleNamespace
from unittest import mock

from src import ltfs
from src import remote_orchestrator as ro
from src.exit_codes import (ExitCode, REASON_UNEXPECTED_TAPE_OR_DB_STATE,
                            REASON_NONINTERACTIVE_REQUIRES_RESUME)
from src.pipeline_types import StagedChunk

from lto_fakes import TapeOperationLog

GiB = 1024 ** 3


def _desc(index):
    return StagedChunk(chunk_index=index, fetch_dir=f"/tmp/_f{index}",
                       pack_dir=f"/tmp/_p{index}", metadata=[],
                       staged_bytes=GiB, source_missing_files=[])


class _TapeCallRecorder:
    """Patches every tape-touching entry point the orchestrator can reach."""

    def __init__(self):
        self.log = TapeOperationLog()
        self._patches = []

    def __enter__(self):
        def readiness(drive, *a, **k):
            self.log.record("readiness", drive=drive)
            return True

        def label(drive, *a, **k):
            self.log.record("volume_label", drive=drive)
            return "Tape_TEST"

        self._patches = [
            mock.patch.object(ro, "_ensure_lto_drive_ready", readiness),
            mock.patch.object(ro, "get_volume_label", label),
            mock.patch.object(ltfs, "_ensure_lto_drive_ready", readiness),
        ]
        for patch in self._patches:
            patch.start()
        return self

    def __exit__(self, *exc):
        for patch in reversed(self._patches):
            patch.stop()
        return False


# =============================================================================
# A. New-session target selection needs no device
# =============================================================================
class NewSessionTapeLabelTests(unittest.TestCase):
    def _orch(self, configured=""):
        orch = ro.RemoteOrchestrator.__new__(ro.RemoteOrchestrator)
        orch.cfg = SimpleNamespace(remote_tape_label=configured,
                                   lto_drive="X:\\")
        return orch

    def test_a_configured_label_is_used_without_reading_the_drive(self):
        orch = self._orch("Tape_04")
        with _TapeCallRecorder() as recorder, mock.patch("builtins.print"):
            self.assertEqual(orch._resolve_tape_label(), "Tape_04")
        self.assertEqual(recorder.log.kinds(), [],
                         "resolving the target cartridge touched the drive")

    def test_a_blank_setting_prompts_and_still_touches_nothing(self):
        orch = self._orch("")
        with _TapeCallRecorder() as recorder, mock.patch("builtins.print"), \
                mock.patch("builtins.input", return_value=" Tape_05 "):
            self.assertEqual(orch._resolve_tape_label(), "Tape_05")
        self.assertEqual(recorder.log.kinds(), [])

    def test_cancelling_the_prompt_yields_no_label(self):
        orch = self._orch("")
        with mock.patch("builtins.print"), \
                mock.patch("builtins.input", return_value="  "):
            self.assertIsNone(orch._resolve_tape_label())

    def test_an_illegal_label_is_refused(self):
        orch = self._orch("")
        for bad in ("", "   ", "Tape\n04", "Tape\t04", "a\rb"):
            self.assertIsNone(ro.RemoteOrchestrator._valid_tape_label(bad), bad)
        self.assertEqual(
            ro.RemoteOrchestrator._valid_tape_label("  Tape_04 "), "Tape_04")

    def test_new_session_no_longer_probes_drive_readiness(self):
        import inspect
        source = inspect.getsource(ro.RemoteOrchestrator._start_new_session)
        self.assertNotIn("_ensure_lto_drive_ready", source)
        self.assertNotIn("get_volume_label", source)

    def test_a_headless_fresh_start_is_refused_with_or_without_a_label(self):
        for configured in ("", "Tape_04"):
            orch = ro.RemoteOrchestrator.__new__(ro.RemoteOrchestrator)
            orch.cfg = SimpleNamespace(remote_tape_label=configured)
            orch.db = mock.MagicMock()
            orch._finalize = lambda result, phase="pipeline": result
            with mock.patch("builtins.print"), \
                    mock.patch("builtins.input",
                               side_effect=AssertionError("prompted")):
                result = orch._run_non_interactive(resume=False)
            self.assertEqual(result.exit_code, ExitCode.FATAL_CONFIG)
            self.assertEqual(result.reason,
                             REASON_NONINTERACTIVE_REQUIRES_RESUME)
            # Nothing was scanned, staged or looked up.
            orch.db.list_active_remote_sessions.assert_not_called()


# =============================================================================
# B. Neither session loop touches the drive before a group
# =============================================================================
class NoIdleTapeAccessTests(unittest.TestCase):
    def test_neither_loop_probes_readiness_or_reads_the_cartridge(self):
        import inspect
        for method in (ro.RemoteOrchestrator._run_streaming_session,
                       ro.RemoteOrchestrator._run_session):
            source = inspect.getsource(method)
            self.assertNotIn("_ensure_lto_drive_ready", source, method.__name__)
            self.assertNotIn("_verify_mounted_cartridge", source,
                             method.__name__)
            self.assertNotIn("get_volume_label", source, method.__name__)
            self.assertNotIn("eject_tape", source, method.__name__)

    def test_the_group_writer_is_the_only_gate_caller(self):
        import inspect
        from src.remote_writer import RemoteChunkWriter
        group = inspect.getsource(RemoteChunkWriter._write_chunk_group)
        self.assertEqual(group.count("_pre_write_safety_gate("), 1)
        one = inspect.getsource(RemoteChunkWriter._write_one_chunk_owned)
        self.assertNotIn("_pre_write_safety_gate", one)
        self.assertNotIn("_ensure_lto_drive_ready", one)
        self.assertNotIn("_verify_mounted_cartridge", one)

    def test_a_skip_tape_chunk_never_reaches_the_drive(self):
        from src.remote_writer import RemoteChunkWriter
        orch = ro.RemoteOrchestrator.__new__(ro.RemoteOrchestrator)
        orch.cfg = SimpleNamespace(backup_log_dir=None, lto_drive="X:\\")
        orch.db = mock.MagicMock()
        orch.notifier = None
        orch.remote_host = "host.example"
        orch.remote_session_path = "/strg"
        orch._cleanup_dir = lambda *_a: None
        orch._consumer_chunk = None
        ejects = []
        orch._backup_writer = lambda cls=None: SimpleNamespace(
            eject_tape=lambda drive: ejects.append(drive))
        writer = RemoteChunkWriter(orch)
        desc = _desc(0)
        desc.skip_tape = True
        with _TapeCallRecorder() as recorder, mock.patch("builtins.print"), \
                mock.patch.object(ro, "send_best_effort", lambda *a, **k: None):
            # eject_after=True on purpose: it must still refuse.
            block = writer._write_skip_tape_chunk(37, desc, "Tape_TEST", True)
        self.assertIsNone(block)
        self.assertEqual(recorder.log.kinds(), [])
        self.assertEqual(ejects, [], "a skip-tape chunk ejected the cartridge")


# =============================================================================
# C. The remote pipeline never ejects
# =============================================================================
class NoRemoteEjectTests(unittest.TestCase):
    def _orch(self, configured):
        orch = ro.RemoteOrchestrator.__new__(ro.RemoteOrchestrator)
        orch.cfg = SimpleNamespace(eject_after_session=configured)
        return orch

    def test_the_flag_is_refused_when_enabled(self):
        with mock.patch("builtins.print"):
            self.assertFalse(self._orch(True)._eject_after_session())

    def test_the_flag_is_false_when_disabled(self):
        self.assertFalse(self._orch(False)._eject_after_session())

    def test_an_absent_flag_is_false(self):
        orch = ro.RemoteOrchestrator.__new__(ro.RemoteOrchestrator)
        orch.cfg = SimpleNamespace()
        self.assertFalse(orch._eject_after_session())

    def test_completion_does_not_eject(self):
        import inspect
        source = inspect.getsource(
            ro.RemoteOrchestrator._run_streaming_session)
        self.assertNotIn("eject_tape", source)


# =============================================================================
# D. Generation verification is database-only and fails closed
# =============================================================================
class SessionGenerationGuardTests(unittest.TestCase):
    def _orch(self, tape=None, active=None, raises=False):
        orch = ro.RemoteOrchestrator.__new__(ro.RemoteOrchestrator)
        orch.notifier = None
        db = SimpleNamespace()
        db.get_tape = lambda label: tape

        def reader(label):
            if raises:
                raise RuntimeError("connection lost")
            return active
        if active is not None or raises:
            db.get_active_tape_generation = reader
        orch.db = db
        return orch

    def _row(self, generation=1):
        return {"session_id": 37, "tape_label": "Tape_TEST",
                "tape_generation": generation}

    def test_matching_active_generation_passes(self):
        orch = self._orch(tape={"current_generation": 1}, active=1)
        with _TapeCallRecorder() as recorder:
            self.assertIsNone(orch._verify_session_tape_generation(self._row()))
        self.assertEqual(recorder.log.kinds(), [],
                         "the generation check touched the drive")

    def test_a_mismatched_generation_is_refused(self):
        orch = self._orch(tape={"current_generation": 2}, active=2)
        with mock.patch("builtins.print"):
            block = orch._verify_session_tape_generation(self._row(1))
        self.assertEqual(block.exit_code, ExitCode.SAFETY_BLOCK)
        self.assertEqual(block.reason, REASON_UNEXPECTED_TAPE_OR_DB_STATE)
        self.assertFalse(block.resumable)

    def test_a_missing_tape_row_is_refused(self):
        orch = self._orch(tape=None)
        with mock.patch("builtins.print"):
            block = orch._verify_session_tape_generation(self._row(1))
        self.assertIsNotNone(block)
        self.assertIn("not established", block.detailed_reason)

    def test_a_null_generation_is_refused(self):
        for session_gen, catalog_gen in ((None, 1), (1, None), (None, None)):
            orch = self._orch(tape={"current_generation": catalog_gen})
            with mock.patch("builtins.print"):
                block = orch._verify_session_tape_generation(
                    self._row(session_gen))
            self.assertIsNotNone(block, (session_gen, catalog_gen))

    def test_a_retired_generation_is_refused(self):
        orch = self._orch(tape={"current_generation": 1}, active=2)
        with mock.patch("builtins.print"):
            block = orch._verify_session_tape_generation(self._row(1))
        self.assertIsNotNone(block)
        self.assertIn("ACTIVE generation is 2", block.detailed_reason)

    def test_an_unreadable_generation_is_refused(self):
        orch = self._orch(tape={"current_generation": 1}, raises=True)
        with mock.patch("builtins.print"):
            block = orch._verify_session_tape_generation(self._row(1))
        self.assertIsNotNone(block)
        self.assertIn("could not be read", block.detailed_reason)

    def test_a_pre_013_row_without_the_column_is_not_blocked(self):
        orch = self._orch(tape={"current_generation": 1})
        self.assertIsNone(orch._verify_session_tape_generation(
            {"session_id": 37, "tape_label": "Tape_TEST"}))

    def test_a_pre_013_database_without_the_reader_still_passes(self):
        orch = self._orch(tape={"current_generation": 1})
        self.assertIsNone(
            orch._verify_session_tape_generation(self._row(1)))


# =============================================================================
# E. Registration establishes the generation atomically
# =============================================================================
class AtomicTapeRegistrationTests(unittest.TestCase):
    def test_register_tape_creates_the_active_generation_in_one_transaction(self):
        import inspect
        from src.pg_tapes import PgTapeMixin
        source = inspect.getsource(PgTapeMixin.register_tape)
        # Both inserts are inside ONE operation handed to _transaction.
        self.assertIn("INSERT INTO tapes", source)
        self.assertIn("INSERT INTO tape_generations", source)
        self.assertEqual(source.count("self._transaction("), 1)
        self.assertIn("'active'", source)

    def test_the_active_generation_reader_is_read_only(self):
        import inspect
        from src.pg_tapes import PgTapeMixin
        source = inspect.getsource(PgTapeMixin.get_active_tape_generation)
        self.assertIn("_run_read", source)
        for mutation in ("INSERT", "UPDATE", "DELETE"):
            self.assertNotIn(mutation, source.upper().replace(
                "READ-ONLY", ""), mutation)


if __name__ == "__main__":
    unittest.main()
