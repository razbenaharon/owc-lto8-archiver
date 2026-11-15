"""The remote writer OBEYS its pre-write gates (audit M11).

Every test drives the real ``RemoteOrchestrator._write_chunk_group`` - the
sole tape-writing entry of the remote pipeline - with every gate method left
real. Only the outermost boundaries are faked (see ``write_gate_harness``),
and all of them write to one ordered call log.

For each gate three things are proven:

* **block** -> the copy function is never called, the chunk status is
  unchanged, the pack stays on disk, nothing reaches the fake tape, nothing
  ejects, and the right ``StopResult`` comes back;
* **cannot be evaluated** -> blocked just the same (fail closed);
* **all clear** -> ``backing`` is recorded BEFORE the first write-touch and
  the copy, ``done`` only AFTER a successful copy.
"""
import os
import threading
import unittest

from write_gate_harness import (AFTER_MOUNT_CSV, BEFORE_MOUNT_CSV, GateWorld,
                                MOUNT_STARTED, REBOOT_REQUIRED_KEY,
                                SESSION_ID, TAPE_LABEL, _iso_local,
                                ownership_unavailable)

from src.exit_codes import (
    ExitCode, REASON_AMBIGUOUS_BACKING_CHUNK, REASON_ARCHIVER_LOCK_LOST,
    REASON_LTFS_MEDIA_DEGRADED, REASON_LTFS_MOUNT_UNVERIFIABLE,
    REASON_LTFS_OWNERSHIP_UNAVAILABLE, REASON_LTFS_SYNC_MODE_NOT_TIME5,
    REASON_SCCM_REBOOT_PENDING, REASON_STOPPED_AT_CHUNK_BOUNDARY,
    REASON_TAPE_WRITE_FAILED, REASON_UNEXPECTED_TAPE_OR_DB_STATE,
    REASON_USER_REQUESTED_STOP, REASON_WINDOWS_REBOOT_PENDING, StopResult)
from src.runtime import CANCEL


@unittest.skipUnless(os.name == "nt", "gate evidence readers are Windows-only")
class _RemoteGateCase(unittest.TestCase):
    def setUp(self):
        self.world = GateWorld(self)
        self.orch = self.world.remote_orchestrator()
        self.stop = threading.Event()

    def write(self, *descs, eject_after=False):
        return self.orch._write_chunk_group(
            SESSION_ID, list(descs), TAPE_LABEL, eject_after, self.stop)

    # -- the shared "nothing happened" contract ---------------------------
    def assert_blocked(self, block, desc, exit_code, reason, *,
                       resumable, status="packing"):
        world = self.world
        self.assertIsInstance(block, StopResult)
        self.assertEqual((block.exit_code, block.reason), (exit_code, reason),
                         block.detailed_reason)
        self.assertEqual(block.resumable, resumable)
        self.assertEqual(block.session_id, SESSION_ID)
        self.assertEqual(block.chunk_index, desc.chunk_index)
        # The decision is the recorded, authoritative stop of the run.
        self.assertIs(self.orch._get_recorded_stop(), block)
        self.assert_nothing_written(desc, status)

    def assert_nothing_written(self, desc, status="packing"):
        world = self.world
        self.assertEqual(world.copies, 0, world.log)
        self.assertEqual(world.count("tape_mkdir"), 0, world.log)
        self.assertEqual(world.files_on_tape(), [])
        self.assertEqual(world.count("eject"), 0, world.log)
        self.assertEqual(world.db.status_writes, [], "chunk status changed")
        self.assertEqual(world.db.status_of(desc.chunk_index), status)
        self.assertTrue(
            os.path.isfile(os.path.join(desc.pack_dir, "Bundle_0001.zip")),
            "the staged pack was not preserved")
        self.assertFalse(world.ownership_held, "ownership leaked")
        self.assertEqual(world.count("ownership:release_without_acquire"), 0)
        self.assertEqual(world.count("catalog_commit"), 0)


# ===========================================================================
# 1. Every gate that says BLOCK is obeyed
# ===========================================================================
class GateBlocksAreObeyed(_RemoteGateCase):

    # -- archiver single-writer lock --------------------------------------
    def test_lost_archiver_lock_blocks_before_ownership(self):
        desc = self.world.stage_chunk(0)
        self.world.db.lock_error = RuntimeError("advisory lock not held")
        block = self.write(desc)
        self.assert_blocked(block, desc, ExitCode.SAFETY_BLOCK,
                            REASON_ARCHIVER_LOCK_LOST, resumable=False)
        self.assertTrue(block.preserve_pack)
        self.assertEqual(self.world.count("ownership:acquire"), 0)

    def test_a_catalog_that_cannot_prove_the_lock_is_refused(self):
        desc = self.world.stage_chunk(0)
        self.world.db.verify_archiver_lock = None     # "no way to check"
        block = self.write(desc)
        self.assert_blocked(block, desc, ExitCode.SAFETY_BLOCK,
                            REASON_ARCHIVER_LOCK_LOST, resumable=False)

    # -- tape status / capacity -------------------------------------------
    def test_a_tape_marked_full_is_refused_before_ownership(self):
        desc = self.world.stage_chunk(0)
        self.world.db.tapes[TAPE_LABEL]["status"] = "full"
        block = self.write(desc)
        self.assert_blocked(block, desc, ExitCode.SAFETY_BLOCK,
                            REASON_UNEXPECTED_TAPE_OR_DB_STATE,
                            resumable=False)
        self.assertTrue(block.preserve_pack)
        self.assertEqual(self.world.count("ownership:acquire"), 0)

    def test_an_unregistered_tape_is_refused(self):
        desc = self.world.stage_chunk(0)
        del self.world.db.tapes[TAPE_LABEL]
        block = self.write(desc)
        self.assert_blocked(block, desc, ExitCode.SAFETY_BLOCK,
                            REASON_UNEXPECTED_TAPE_OR_DB_STATE,
                            resumable=False)

    def test_a_group_that_only_overflows_cumulatively_is_refused(self):
        """Each chunk fits alone; together they exceed the tape budget."""
        tape = self.world.db.tapes[TAPE_LABEL]
        tape["total_capacity"] = 1                       # 1 GB (decimal)
        tape["used"] = 1000 ** 3 - 3000                  # 3000 bytes left
        first = self.world.stage_chunk(0, size=2048)
        second = self.world.stage_chunk(1, size=2048)
        block = self.write(first, second)
        self.assert_blocked(block, first, ExitCode.SAFETY_BLOCK,
                            REASON_UNEXPECTED_TAPE_OR_DB_STATE,
                            resumable=False)
        self.assert_nothing_written(second)
        self.assertEqual(self.world.count("ownership:acquire"), 0)

    def test_an_unreadable_tape_row_never_reaches_the_copy(self):
        desc = self.world.stage_chunk(0)
        self.world.db.tape_read_error = RuntimeError("connection lost")
        with self.assertRaises(RuntimeError):
            self.write(desc)
        self.assert_nothing_written(desc)

    # -- LTFS ownership ----------------------------------------------------
    def test_unavailable_ownership_blocks_and_releases_nothing(self):
        desc = self.world.stage_chunk(0)
        self.world.ownership_error = ownership_unavailable()
        block = self.write(desc)
        self.assert_blocked(block, desc, ExitCode.SAFETY_BLOCK,
                            REASON_LTFS_OWNERSHIP_UNAVAILABLE, resumable=True)
        self.assertTrue(block.preserve_pack)
        # No gate evidence was even read without ownership.
        for event in ("ps:mount", "label", "ps:sccm", "readiness"):
            self.assertEqual(self.world.count(event), 0, event)
        self.assertEqual(self.world.count("ownership:release"), 0)

    # -- LTFS sync mode (time@5) -------------------------------------------
    def test_sync_type_unmount_blocks(self):
        desc = self.world.stage_chunk(0)
        self.world.declare_sync_mode("unmount", 0)
        block = self.write(desc)
        self.assert_blocked(block, desc, ExitCode.SAFETY_BLOCK,
                            REASON_LTFS_SYNC_MODE_NOT_TIME5, resumable=False)

    def test_a_time_mode_with_the_wrong_interval_blocks(self):
        desc = self.world.stage_chunk(0)
        self.world.declare_sync_mode("time", 600)
        block = self.write(desc)
        self.assert_blocked(block, desc, ExitCode.SAFETY_BLOCK,
                            REASON_LTFS_SYNC_MODE_NOT_TIME5, resumable=False)

    # -- media health ------------------------------------------------------
    def test_a_fatal_drive_event_on_this_mount_blocks(self):
        desc = self.world.stage_chunk(0)
        self.world.log_ltfs_event(11344, AFTER_MOUNT_CSV,
                                  "cartridge frozen by a permanent error")
        block = self.write(desc)
        self.assert_blocked(block, desc, ExitCode.SAFETY_BLOCK,
                            REASON_LTFS_MEDIA_DEGRADED, resumable=False)

    def test_a_single_locate_write_perm_warning_blocks(self):
        """The early warning missed for four days before Tape_02 froze."""
        desc = self.world.stage_chunk(0)
        self.world.log_ltfs_event(17267, AFTER_MOUNT_CSV,
                                  "write-perm error on LOCATE")
        block = self.write(desc)
        self.assert_blocked(block, desc, ExitCode.SAFETY_BLOCK,
                            REASON_LTFS_MEDIA_DEGRADED, resumable=False)

    # -- mounted cartridge -------------------------------------------------
    def test_the_wrong_cartridge_blocks(self):
        desc = self.world.stage_chunk(0)
        self.world.volume_label = "Tape_OTHER"
        block = self.write(desc)
        self.assert_blocked(block, desc, ExitCode.SAFETY_BLOCK,
                            REASON_UNEXPECTED_TAPE_OR_DB_STATE,
                            resumable=False)

    # -- pending restart ---------------------------------------------------
    def test_a_staged_windows_restart_blocks(self):
        desc = self.world.stage_chunk(0)
        self.world.registry.keys[REBOOT_REQUIRED_KEY] = {}
        block = self.write(desc)
        self.assert_blocked(block, desc, ExitCode.TRANSIENT_RESUMABLE,
                            REASON_WINDOWS_REBOOT_PENDING, resumable=True)

    def test_an_sccm_pending_restart_blocks(self):
        desc = self.world.stage_chunk(0)
        self.world.sccm["RebootPending"] = True
        block = self.write(desc)
        self.assert_blocked(block, desc, ExitCode.TRANSIENT_RESUMABLE,
                            REASON_SCCM_REBOOT_PENDING, resumable=True)

    # -- chunk-transition state -------------------------------------------
    def test_a_chunk_left_backing_by_a_prior_run_is_never_rewritten(self):
        desc = self.world.stage_chunk(0, status="backing")
        block = self.write(desc)
        self.assert_blocked(block, desc, ExitCode.SAFETY_BLOCK,
                            REASON_AMBIGUOUS_BACKING_CHUNK, resumable=False,
                            status="backing")

    # -- stop flags --------------------------------------------------------
    def test_an_operator_cancel_blocks(self):
        desc = self.world.stage_chunk(0)
        CANCEL.set()
        block = self.write(desc)
        self.assert_blocked(block, desc, ExitCode.USER_STOP,
                            REASON_USER_REQUESTED_STOP, resumable=True)

    def test_a_pipeline_stop_flag_blocks(self):
        desc = self.world.stage_chunk(0)
        self.stop.set()
        block = self.write(desc)
        self.assert_blocked(block, desc, ExitCode.TRANSIENT_RESUMABLE,
                            REASON_STOPPED_AT_CHUNK_BOUNDARY, resumable=True)

    def test_an_already_recorded_stop_blocks_and_is_returned_unchanged(self):
        desc = self.world.stage_chunk(0)
        earlier = self.orch._record_stop(StopResult(
            exit_code=ExitCode.TRANSIENT_RESUMABLE,
            reason=REASON_SCCM_REBOOT_PENDING, resumable=True,
            source="reboot-sentinel"))
        block = self.write(desc)
        self.assertIs(block, earlier)
        self.assert_nothing_written(desc)

    # -- descriptor admission ---------------------------------------------
    def test_a_descriptor_from_another_session_never_reaches_the_tape(self):
        desc = self.world.stage_chunk(0)
        desc.session_id = SESSION_ID + 1
        self.world.db.seed_chunk(0, "packing", session_id=SESSION_ID + 1)
        with self.assertRaises(RuntimeError):
            self.write(desc)
        self.assert_nothing_written(desc)
        self.assertEqual(self.world.count("ownership:acquire"), 0)


# ===========================================================================
# 2. A gate that CANNOT BE EVALUATED blocks (fail closed)
# ===========================================================================
class UnevaluableGatesFailClosed(_RemoteGateCase):

    def test_an_unreadable_event_log_blocks(self):
        desc = self.world.stage_chunk(0)
        self.world.mount_query_fails = True
        block = self.write(desc)
        self.assert_blocked(block, desc, ExitCode.SAFETY_BLOCK,
                            REASON_LTFS_MOUNT_UNVERIFIABLE, resumable=False)

    def test_no_running_ltfs_process_blocks(self):
        desc = self.world.stage_chunk(0)
        self.world.mount["MountProcStart"] = ""
        self.world.mount["ProcStart"] = ""
        block = self.write(desc)
        self.assert_blocked(block, desc, ExitCode.SAFETY_BLOCK,
                            REASON_LTFS_MOUNT_UNVERIFIABLE, resumable=False)

    def test_a_time5_declaration_from_a_previous_mount_blocks(self):
        desc = self.world.stage_chunk(0)
        self.world.mount["EventTime"] = _iso_local(
            MOUNT_STARTED.replace(hour=9))        # before the mount started
        block = self.write(desc)
        self.assert_blocked(block, desc, ExitCode.SAFETY_BLOCK,
                            REASON_LTFS_MOUNT_UNVERIFIABLE, resumable=False)

    def test_a_missing_mount_declaration_blocks(self):
        desc = self.world.stage_chunk(0)
        self.world.mount["EventTime"] = ""
        self.world.mount["EventMsg"] = ""
        block = self.write(desc)
        self.assert_blocked(block, desc, ExitCode.SAFETY_BLOCK,
                            REASON_LTFS_MOUNT_UNVERIFIABLE, resumable=False)

    def test_an_unreadable_drive_log_blocks(self):
        desc = self.world.stage_chunk(0)
        os.remove(self.world.ltfs_csv)
        block = self.write(desc)
        self.assert_blocked(block, desc, ExitCode.SAFETY_BLOCK,
                            REASON_LTFS_MEDIA_DEGRADED, resumable=False)

    def test_an_unreadable_cartridge_label_blocks(self):
        desc = self.world.stage_chunk(0)
        self.world.label_error = OSError("The device is not ready")
        block = self.write(desc)
        self.assert_blocked(block, desc, ExitCode.SAFETY_BLOCK,
                            REASON_UNEXPECTED_TAPE_OR_DB_STATE,
                            resumable=False)

    def test_a_cartridge_with_no_label_blocks(self):
        for label in (None, "", "   "):
            with self.subTest(label=label):
                self.setUp()
                desc = self.world.stage_chunk(0)
                self.world.volume_label = label
                block = self.write(desc)
                self.assert_blocked(block, desc, ExitCode.SAFETY_BLOCK,
                                    REASON_UNEXPECTED_TAPE_OR_DB_STATE,
                                    resumable=False)

    def test_an_unanswerable_sccm_client_blocks(self):
        """SCCM is installed but cannot be asked: unknown is not 'clear'."""
        desc = self.world.stage_chunk(0)
        self.world.sccm_query_fails = True
        block = self.write(desc)
        self.assert_blocked(block, desc, ExitCode.TRANSIENT_RESUMABLE,
                            REASON_SCCM_REBOOT_PENDING, resumable=True)

    def test_a_restart_check_that_crashes_blocks(self):
        desc = self.world.stage_chunk(0)
        self.world.registry.broken = RuntimeError("registry provider crashed")
        block = self.write(desc)
        self.assertEqual(block.exit_code, ExitCode.TRANSIENT_RESUMABLE)
        self.assertIn(block.reason, (REASON_SCCM_REBOOT_PENDING,
                                     REASON_WINDOWS_REBOOT_PENDING))
        self.assert_nothing_written(desc)

    def test_an_unreadable_chunk_status_blocks_as_ambiguous(self):
        desc = self.world.stage_chunk(0)
        self.world.db.status_read_error = RuntimeError("connection lost")
        block = self.write(desc)
        self.assert_blocked(block, desc, ExitCode.SAFETY_BLOCK,
                            REASON_AMBIGUOUS_BACKING_CHUNK, resumable=False)

    def test_a_host_without_sccm_is_determinate_and_may_write(self):
        """Control: 'no SCCM client' is a known state, not an unknown one."""
        from write_gate_harness import SCCM_SERVICE_KEY
        desc = self.world.stage_chunk(0)
        self.world.sccm_query_fails = True
        del self.world.registry.keys[SCCM_SERVICE_KEY]
        self.assertIsNone(self.write(desc))
        self.assertEqual(self.world.copies, 1)

    def test_a_fault_from_a_previous_mount_does_not_block(self):
        """Control: evidence is scoped to the current mount."""
        desc = self.world.stage_chunk(0)
        self.world.log_ltfs_event(11344, BEFORE_MOUNT_CSV)
        self.assertIsNone(self.write(desc))
        self.assertEqual(self.world.copies, 1)


# ===========================================================================
# 3. All clear: backing BEFORE the copy, done only AFTER it
# ===========================================================================
class PermittedWriteOrdering(_RemoteGateCase):

    def test_backing_precedes_the_first_tape_touch_and_done_follows_the_copy(
            self):
        world = self.world
        desc = world.stage_chunk(0)
        self.assertIsNone(self.write(desc))

        log = world.log
        self.assertEqual(world.db.status_writes, [(0, "backing"), (0, "done")])
        self.assertEqual(world.copies, 1)
        backing = log.index("status:0:backing")
        mkdir = log.index("tape_mkdir")
        copy = log.index("copy")
        done = log.index("status:0:done")
        self.assertLess(backing, mkdir,
                        "the tape was touched before the chunk was 'backing'")
        self.assertLess(mkdir, copy)
        self.assertLess(copy, done, "'done' was recorded before the copy")
        # The catalog commit sits between the copy and 'done'.
        self.assertLess(copy, log.index("catalog_commit"))
        self.assertLess(log.index("catalog_commit"), done)
        # Every gate was consulted before the write was marked started.
        for evidence in ("archiver_lock", "ps:mount", "label", "ps:sccm"):
            self.assertLess(log.index(evidence), backing, evidence)
        self.assertEqual(world.files_on_tape(),
                         [os.path.join("_pack_s0037_000", "Bundle_0001.zip")])
        self.assertEqual(world.count("eject"), 0)
        self.assertIsNone(self.orch._get_recorded_stop())
        # The committed pack is flushed from staging.
        self.assertFalse(os.path.exists(desc.pack_dir))

    def test_the_gate_and_the_copy_run_inside_one_ownership_period(self):
        world = self.world
        first, second = world.stage_chunk(0), world.stage_chunk(1)
        self.assertIsNone(self.write(first, second))
        log = world.log
        self.assertEqual(world.count("ownership:acquire"), 1)
        self.assertEqual(world.count("ownership:release"), 1)
        acquire = log.index("ownership:acquire")
        release = log.index("ownership:release")
        self.assertEqual(world.unowned, [],
                         "tape-facing work ran without LTFS ownership")
        for event in ("label", "copy", "tape_mkdir", "status:1:done"):
            self.assertLess(acquire, log.index(event), event)
            self.assertLess(len(log) - 1 - log[::-1].index(event), release,
                            event)
        self.assertFalse(world.ownership_held)

    def test_the_cartridge_is_verified_once_per_group_not_per_chunk(self):
        world = self.world
        descs = [world.stage_chunk(i) for i in range(3)]
        self.assertIsNone(self.write(*descs))
        self.assertEqual(world.copies, 3)
        self.assertEqual(world.count("label"), 1)
        self.assertEqual(world.count("ps:sccm"), 1)
        self.assertEqual(
            world.db.status_writes,
            [(0, "backing"), (0, "done"), (1, "backing"), (1, "done"),
             (2, "backing"), (2, "done")])

    def test_the_remote_writer_does_not_eject_after_a_successful_write(self):
        desc = self.world.stage_chunk(0)
        self.assertIsNone(self.write(desc))
        self.assertEqual(self.world.count("eject"), 0)

    def test_a_skip_tape_chunk_touches_no_tape_at_all(self):
        world = self.world
        desc = world.stage_chunk(0, status="fetching")
        desc.skip_tape = True
        desc.metadata = []
        desc.staged_bytes = 0
        desc.source_missing_files = ["/data/gone.txt"]
        self.assertIsNone(self.write(desc))
        self.assertEqual(world.db.status_of(0), "done")
        self.assertEqual(world.copies, 0)
        self.assertEqual(world.count("ownership:acquire"), 0)
        for event in ("ps:mount", "label", "readiness", "tape_mkdir", "eject"):
            self.assertEqual(world.count(event), 0, event)

    def test_a_completed_copy_under_cancel_commits_and_stops_the_group(self):
        """A cooperative Ctrl+C lets the protected copy finish: that chunk is
        committed, the next one is never started."""
        world = self.world
        first, second = world.stage_chunk(0), world.stage_chunk(1)
        world.on_copy = lambda _s, _d: CANCEL.set()
        block = self.write(first, second)
        self.assertEqual((block.exit_code, block.reason),
                         (ExitCode.USER_STOP, REASON_USER_REQUESTED_STOP))
        self.assertEqual(world.db.status_of(0), "done")
        self.assertEqual(world.db.status_of(1), "packing")
        self.assertEqual(world.copies, 1)
        self.assertTrue(os.path.isdir(second.pack_dir))
        self.assertFalse(world.ownership_held)


# ===========================================================================
# 4. A failed copy stays 'backing': no retry, no eject, pack kept
# ===========================================================================
class FailedCopyStaysAmbiguous(_RemoteGateCase):

    def assert_ambiguous(self, block, desc):
        world = self.world
        self.assertEqual((block.exit_code, block.reason),
                         (ExitCode.SAFETY_BLOCK,
                          REASON_AMBIGUOUS_BACKING_CHUNK),
                         block.detailed_reason)
        self.assertFalse(block.resumable)
        self.assertTrue(block.preserve_pack)
        self.assertEqual(block.chunk_index, desc.chunk_index)
        self.assertIs(self.orch._get_recorded_stop(), block)
        # 'backing' and nothing after it: never done / backup_failed / pending.
        self.assertEqual(world.db.status_writes,
                         [(desc.chunk_index, "backing")])
        self.assertEqual(world.db.status_of(desc.chunk_index), "backing")
        self.assertLessEqual(world.copies, 1, "the copy was retried")
        self.assertEqual(world.count("eject"), 0)
        self.assertTrue(
            os.path.isfile(os.path.join(desc.pack_dir, "Bundle_0001.zip")))
        self.assertIn(("writer_ambiguous", desc.chunk_index),
                      world.db.container_events)
        self.assertFalse(world.ownership_held)

    def _fail(self, behaviour):
        desc = self.world.stage_chunk(0)
        self.world.copy_plan = [behaviour]
        block = self.write(desc)
        self.assert_ambiguous(block, desc)
        self.assertEqual(self.world.copies, 1)
        self.assertEqual(self.world.count("catalog_commit"), 0,
                         "a failed copy was cataloged")
        return block

    def test_a_hard_robocopy_failure(self):
        self._fail("rc8")

    def test_exit_code_zero_with_retry_limit_exceeded_is_not_success(self):
        """Incident 009: robocopy exits 0 after giving up."""
        self._fail("retry_limit_rc0")

    def test_exit_code_zero_without_a_summary_is_not_success(self):
        self._fail("no_summary")

    def test_a_copy_that_raises(self):
        self._fail("raise")

    def test_a_write_protected_first_touch_never_starts_the_copy(self):
        desc = self.world.stage_chunk(0)
        self.world.tape_mkdir_error = OSError(19, "The media is write "
                                                  "protected")
        block = self.write(desc)
        self.assert_ambiguous(block, desc)
        self.assertEqual(self.world.copies, 0)
        self.assertLess(self.world.index("status:0:backing"),
                        self.world.index("tape_mkdir"))

    def test_a_catalog_failure_after_the_copy_stays_backing(self):
        desc = self.world.stage_chunk(0)
        self.world.db.catalog_commit_error = RuntimeError("deadlock detected")
        block = self.write(desc)
        self.assert_ambiguous(block, desc)
        self.assertEqual(self.world.copies, 1)

    def test_physical_ambiguity_outranks_a_cancel(self):
        desc = self.world.stage_chunk(0)
        self.world.copy_plan = ["raise"]
        self.world.on_copy = lambda _s, _d: CANCEL.set()
        block = self.write(desc)
        self.assert_ambiguous(block, desc)

    def test_a_failure_stops_the_group_and_preserves_later_packs(self):
        world = self.world
        first, second, third = (world.stage_chunk(i) for i in range(3))
        world.copy_plan = ["ok", "rc8"]
        block = self.write(first, second, third)
        self.assertEqual(block.reason, REASON_AMBIGUOUS_BACKING_CHUNK)
        self.assertEqual(block.chunk_index, 1)
        self.assertEqual(world.copies, 2)
        self.assertEqual([world.db.status_of(i) for i in range(3)],
                         ["done", "backing", "packing"])
        self.assertTrue(os.path.isdir(second.pack_dir))
        self.assertTrue(os.path.isdir(third.pack_dir))
        self.assertEqual(world.count("eject"), 0)
        self.assertFalse(world.ownership_held)

    def test_a_drive_that_is_not_ready_never_marks_backing(self):
        """A failure BEFORE the write started is re-drivable, not ambiguous."""
        world = self.world
        desc = world.stage_chunk(0)
        world.drive_ready = False
        block = self.write(desc)
        self.assertEqual((block.exit_code, block.reason),
                         (ExitCode.TRANSIENT_RESUMABLE,
                          REASON_TAPE_WRITE_FAILED))
        self.assertTrue(block.preserve_pack)
        self.assertEqual(world.db.status_writes, [(0, "backup_failed")])
        self.assertEqual(world.copies, 0)
        self.assertEqual(world.count("tape_mkdir"), 0)
        self.assertEqual(world.count("eject"), 0)
        self.assertTrue(os.path.isdir(desc.pack_dir))


# ===========================================================================
# 5. Between chunks: a fault logged during chunk N stops chunk N+1
# ===========================================================================
class BetweenChunkChecks(_RemoteGateCase):

    def test_a_fault_logged_during_one_chunk_stops_the_next(self):
        world = self.world
        first, second = world.stage_chunk(0), world.stage_chunk(1)
        world.on_copy = lambda _s, _d: world.log_ltfs_event(
            17267, AFTER_MOUNT_CSV, "write-perm error on LOCATE")
        block = self.write(first, second)
        self.assertEqual((block.exit_code, block.reason),
                         (ExitCode.SAFETY_BLOCK, REASON_LTFS_MEDIA_DEGRADED))
        self.assertEqual(block.chunk_index, 1)
        self.assertEqual(world.copies, 1)
        self.assertEqual(world.db.status_of(0), "done")
        self.assertEqual(world.db.status_of(1), "packing")
        self.assertTrue(os.path.isdir(second.pack_dir))
        # The between-chunk check is passive: no second label read.
        self.assertEqual(world.count("label"), 1)
        self.assertFalse(world.ownership_held)

    def test_a_stop_requested_during_one_chunk_stops_the_next(self):
        world = self.world
        first, second = world.stage_chunk(0), world.stage_chunk(1)
        world.on_copy = lambda _s, _d: self.stop.set()
        block = self.write(first, second)
        self.assertEqual(block.exit_code, ExitCode.USER_STOP)
        self.assertEqual(world.copies, 1)
        self.assertEqual(world.db.status_of(1), "packing")
        self.assertTrue(os.path.isdir(second.pack_dir))


if __name__ == "__main__":
    unittest.main()
