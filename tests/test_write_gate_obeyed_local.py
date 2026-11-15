"""The attended LOCAL writer obeys its pre-write gates (audit M11).

Drives the real ``LocalOrchestrator._process_chunk`` (direct-copy mode) with
every gate method left real and only the outermost boundaries faked - see
``write_gate_harness``. The local contract differs from the remote one and is
asserted as it is: a refusal is ``False`` (never a ``StopResult``), a lost
archiver lock raises, and the chunk is ``staged`` before the copy and
``backed_up`` only after it.
"""
import os
import unittest
from unittest import mock

from write_gate_harness import (AFTER_MOUNT_CSV, FakeCatalog, GateWorld,
                                MOUNT_STARTED, REBOOT_REQUIRED_KEY,
                                TAPE_LABEL, _iso_local)

from src.local_orchestrator import LocalOrchestrator
from src.skipped import SkippedFileTracker

LOCAL_SESSION = 5


class FakeLocalCatalog(FakeCatalog):
    """Adds the local-session half of the catalog."""

    def __init__(self, log):
        super().__init__(log)
        self.local_status_writes = []
        self.tape_file_records = 0

    def get_local_indexed_original_paths(self, session_id, chunk_index,
                                         tape_label):
        return set()

    def file_record_exists(self, *args, **kwargs):
        return False

    def count_tape_file_records(self, tape_label):
        return self.tape_file_records

    def register_tape(self, tape_label, capacity_gb):
        self.log.append("register_tape")
        self.tapes[tape_label] = {"tape_label": tape_label,
                                  "total_capacity": capacity_gb,
                                  "status": "active", "used": 0}

    def assign_local_chunk_tape(self, session_id, chunk_index, tape_label):
        self.log.append("assign_tape")

    def update_local_chunk_status(self, session_id, chunk_index, status):
        self.local_status_writes.append(status)
        self.log.append(f"local_status:{status}")


@unittest.skipUnless(os.name == "nt", "gate evidence readers are Windows-only")
class _LocalGateCase(unittest.TestCase):
    def setUp(self):
        self.world = world = GateWorld(self)
        world.db = FakeLocalCatalog(world.log)
        self.source = os.path.join(world.tmp, "source")
        self.entries = []
        for name in ("dirA", "dirB"):
            os.makedirs(os.path.join(self.source, name))
            with open(os.path.join(self.source, name, "big.bin"), "wb") as fh:
                fh.write(b"y" * 4096)
            self.entries.append({"top_level_dir": name,
                                 "dir_size_bytes": 4096, "tape_label": None})
        self.session = {"session_id": LOCAL_SESSION, "source_dir": self.source,
                        "backup_mode": "direct", "session_label": "local_5",
                        "total_chunks": 1}

        orch = LocalOrchestrator.__new__(LocalOrchestrator)
        orch.cfg = world.cfg()
        orch.db = world.db
        orch.ui = None
        orch.skipped_tracker = SkippedFileTracker()
        orch.notifier = None
        orch.source_dir = self.source
        orch.staging_dir = world.staging
        orch.fill_pct = 0.5
        orch.governor = None
        self.orch = orch

    def process(self):
        return self.orch._process_chunk(self.session, 0, self.entries)

    def assign(self, label=TAPE_LABEL):
        for entry in self.entries:
            entry["tape_label"] = label

    def assert_nothing_written(self, statuses=()):
        world = self.world
        self.assertEqual(world.copies, 0, world.log)
        self.assertEqual(world.count("tape_mkdir"), 0, world.log)
        self.assertEqual(world.files_on_tape(), [])
        self.assertEqual(world.count("eject"), 0)
        self.assertEqual(world.count("catalog_commit"), 0)
        self.assertEqual(world.db.local_status_writes, list(statuses))
        self.assertFalse(world.ownership_held)

    def assert_refused(self):
        self.assertIs(self.process(), False)
        self.assert_nothing_written()


class LocalGateBlocksAreObeyed(_LocalGateCase):

    def test_lost_archiver_lock_is_a_hard_stop(self):
        self.world.db.lock_error = RuntimeError("advisory lock not held")
        with self.assertRaises(RuntimeError):
            self.process()
        self.assert_nothing_written()
        self.assertEqual(self.world.count("label"), 0)

    def test_a_catalog_that_cannot_prove_the_lock_is_refused(self):
        self.world.db.verify_archiver_lock = None
        with self.assertRaises(RuntimeError):
            self.process()
        self.assert_nothing_written()

    def test_an_unreadable_cartridge_label_is_refused(self):
        self.world.label_error = OSError("The device is not ready")
        self.assert_refused()

    def test_a_cartridge_with_no_label_is_refused(self):
        self.world.volume_label = None
        self.assert_refused()

    def test_the_wrong_cartridge_for_an_assigned_chunk_is_refused(self):
        self.assign("Tape_OTHER")
        self.assert_refused()

    def test_an_unassigned_chunk_is_refused_on_a_tape_marked_full(self):
        self.world.db.tapes[TAPE_LABEL]["status"] = "full"
        self.assert_refused()

    def test_an_unassigned_chunk_that_does_not_fit_is_refused(self):
        tape = self.world.db.tapes[TAPE_LABEL]
        tape["total_capacity"] = 1
        tape["used"] = 1000 ** 3 - 100
        self.assert_refused()

    def test_an_unregistered_non_empty_tape_is_refused(self):
        del self.world.db.tapes[TAPE_LABEL]
        with open(os.path.join(self.world.tape, "stray.bin"), "wb") as fh:
            fh.write(b"z")
        self.assertIs(self.process(), False)
        self.assertEqual(self.world.copies, 0)
        self.assertEqual(self.world.count("register_tape"), 0)
        self.assertEqual(self.world.db.local_status_writes, [])

    @unittest.expectedFailure
    def test_an_assigned_chunk_is_refused_on_a_tape_marked_full(self):
        """PRODUCT BUG (reported, not fixed here - src/ is out of scope).

        ``LocalOrchestrator._prepare_tape_for_chunk`` runs
        ``_ensure_chunk_fits_tape`` (the tape ``full`` status and the DB
        capacity budget) only ``if not assigned``. A resumed chunk that already
        has a tape assignment therefore skips both checks: retiring a
        cartridge with ``tapes.status='full'`` (the documented way to take a
        latched read-only tape out of rotation, incident 010) does NOT stop
        the local path from writing to it again.
        """
        self.assign()
        self.world.db.tapes[TAPE_LABEL]["status"] = "full"
        self.assert_refused()

    def test_sync_type_unmount_is_refused(self):
        self.world.declare_sync_mode("unmount", 0)
        self.assert_refused()

    def test_a_drive_fault_on_this_mount_is_refused(self):
        self.world.log_ltfs_event(11344, AFTER_MOUNT_CSV)
        self.assert_refused()

    def test_a_staged_windows_restart_is_refused(self):
        self.world.registry.keys[REBOOT_REQUIRED_KEY] = {}
        self.assert_refused()

    def test_an_sccm_pending_restart_is_refused(self):
        self.world.sccm["IsHardRebootPending"] = True
        self.assert_refused()


class LocalUnevaluableGatesFailClosed(_LocalGateCase):

    def test_an_unreadable_event_log_is_refused(self):
        self.world.mount_query_fails = True
        self.assert_refused()

    def test_a_stale_time5_declaration_is_refused(self):
        self.world.mount["EventTime"] = _iso_local(
            MOUNT_STARTED.replace(hour=9))
        self.assert_refused()

    def test_an_unreadable_drive_log_is_refused(self):
        os.remove(self.world.ltfs_csv)
        self.assert_refused()

    def test_an_unanswerable_sccm_client_is_refused(self):
        self.world.sccm_query_fails = True
        self.assert_refused()

    def test_a_gate_that_crashes_is_refused(self):
        self.world.registry.broken = RuntimeError("registry provider crashed")
        self.assert_refused()

    def test_a_gate_helper_that_raises_is_refused(self):
        """Even an unexpected exception inside the gate itself fails closed."""
        self.orch._verify_current_mount_time5 = mock.Mock(
            side_effect=ValueError("unexpected"))
        self.assert_refused()


class LocalPermittedWriteOrdering(_LocalGateCase):

    def test_staged_precedes_the_copy_and_backed_up_follows_it(self):
        world = self.world
        self.assertIs(self.process(), True)
        log = world.log
        self.assertEqual(world.copies, 2)                    # dirA, dirB
        self.assertEqual(world.db.local_status_writes,
                         ["staged", "backed_up"])
        staged = log.index("local_status:staged")
        first_copy = log.index("copy")
        last_copy = len(log) - 1 - log[::-1].index("copy")
        backed_up = log.index("local_status:backed_up")
        self.assertLess(staged, first_copy)
        self.assertLess(last_copy, backed_up,
                        "'backed_up' was recorded before the last copy")
        for evidence in ("archiver_lock", "label", "ps:mount", "ps:sccm"):
            self.assertLess(log.index(evidence), staged, evidence)
        self.assertEqual(world.unowned, [])
        self.assertEqual(len(world.files_on_tape()), 2)
        self.assertEqual(world.count("eject"), 0)
        self.assertFalse(world.ownership_held)

    def test_the_gate_is_re_evaluated_immediately_before_every_copy(self):
        """A fault that appears while dirA is copied must stop dirB."""
        world = self.world
        world.on_copy = lambda _s, _d: world.log_ltfs_event(
            17267, AFTER_MOUNT_CSV, "write-perm error on LOCATE")
        self.assertIs(self.process(), False)
        self.assertEqual(world.copies, 1)
        self.assertEqual(world.db.local_status_writes, ["staged"])
        self.assertEqual(len(world.files_on_tape()), 1)
        self.assertEqual(world.count("eject"), 0)

    def test_a_restart_staged_after_the_first_gate_stops_the_write(self):
        world = self.world
        calls = {"n": 0}
        real = world._powershell

        def powershell(argv, *args, **kwargs):
            if "DetermineIfRebootPending" in argv[-1]:
                calls["n"] += 1
                if calls["n"] >= 2:        # staged after the first gate
                    world.sccm["RebootPending"] = True
            return real(argv, *args, **kwargs)

        with mock.patch("src.windows_update_guard.subprocess.run", powershell):
            self.assertIs(self.process(), False)
        self.assertEqual(world.copies, 0)
        self.assertEqual(world.db.local_status_writes, ["staged"])
        self.assertEqual(world.files_on_tape(), [])


class LocalFailedCopy(_LocalGateCase):

    def _fail(self, behaviour):
        world = self.world
        world.copy_plan = [behaviour]
        self.assertIs(self.process(), False)
        self.assertEqual(world.copies, 1, "the copy was retried or continued")
        self.assertEqual(world.db.local_status_writes, ["staged"])
        self.assertEqual(world.count("catalog_commit"), 0)
        self.assertEqual(world.count("eject"), 0)
        self.assertFalse(world.ownership_held)

    def test_a_hard_robocopy_failure(self):
        self._fail("rc8")

    def test_exit_code_zero_with_retry_limit_exceeded_is_not_success(self):
        self._fail("retry_limit_rc0")

    def test_exit_code_zero_without_a_summary_is_not_success(self):
        self._fail("no_summary")

    def test_a_copy_that_raises(self):
        self._fail("raise")


class LocalEjectIsNeverAutomatic(_LocalGateCase):

    def test_no_eject_when_the_setting_is_off(self):
        self.assertIs(self.orch._eject_at_session_end(), False)
        self.assertEqual(self.world.count("eject"), 0)

    def test_no_eject_without_the_typed_confirmation(self):
        self.orch.cfg.eject_after_session = True
        with mock.patch("src.ltfs._stdin_is_interactive", return_value=True):
            for answer in ("", "y", "yes", "eject"):
                with self.subTest(answer=answer),                         mock.patch("builtins.input", return_value=answer):
                    self.assertIs(self.orch._eject_at_session_end(), False)
            with mock.patch("builtins.input", side_effect=EOFError):
                self.assertIs(self.orch._eject_at_session_end(), False)
        self.assertEqual(self.world.count("eject"), 0)

    def test_a_piped_stdin_can_never_confirm_an_eject(self):
        self.orch.cfg.eject_after_session = True
        with mock.patch("src.ltfs._stdin_is_interactive", return_value=False),                 mock.patch("builtins.input", return_value="EJECT"):
            self.assertIs(self.orch._eject_at_session_end(), False)
        self.assertEqual(self.world.count("eject"), 0)

    def test_the_typed_word_ejects_exactly_once(self):
        """Control: the harness does observe an eject when one happens."""
        self.orch.cfg.eject_after_session = True
        with mock.patch("src.ltfs._stdin_is_interactive", return_value=True),                 mock.patch("builtins.input", return_value="EJECT"):
            self.assertIs(self.orch._eject_at_session_end(), True)
        self.assertEqual(self.world.count("eject"), 1)

if __name__ == "__main__":
    unittest.main()
