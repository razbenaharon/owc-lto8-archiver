"""Phase 3: cross-process LTFS ownership.

The cross-process property is proved with REAL subprocesses -- a thread-only
test would pass against the old ``threading.RLock`` and prove nothing. Every
subprocess is bounded by an explicit timeout so a failure cannot hang the suite,
and none of them touches the tape: they take the mutex and sleep.
"""
import os
import subprocess
import sys
import textwrap
import threading
import time
import unittest
from unittest import mock

from src import ltfs
from src import ltfs_ownership as own
from src import runtime as rt
from src.ltfs_ownership import LtfsOwnership, LtfsOwnershipError

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# A mutex name unique to this test run: never collides with the production one.
TEST_MUTEX = f"OWC_LTO8_TEST_{os.getpid()}"

HOLDER = textwrap.dedent(
    """
    import sys, time
    sys.path.insert(0, r"{root}")
    from src.ltfs_ownership import LtfsOwnership
    own = LtfsOwnership(name="{name}", timeout=5)
    own.acquire("holder")
    print("ACQUIRED", flush=True)
    time.sleep({hold})
    own.release(operation="holder")
    print("RELEASED", flush=True)
    """
)

CRASHER = textwrap.dedent(
    """
    import os, sys
    sys.path.insert(0, r"{root}")
    from src.ltfs_ownership import LtfsOwnership
    own = LtfsOwnership(name="{name}", timeout=5)
    own.acquire("crasher")
    print("ACQUIRED", flush=True)
    sys.stdout.flush()
    os._exit(1)          # die holding the mutex -> Windows marks it abandoned
    """
)


def _spawn(script, name, hold=3.0):
    return subprocess.Popen(
        [sys.executable, "-c",
         script.format(root=PROJECT_ROOT, name=name, hold=hold)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def _wait_for_acquired(proc, timeout=20):
    """Block until the child reports ACQUIRED, or fail."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        line = proc.stdout.readline()
        if not line:
            break
        if "ACQUIRED" in line:
            return True
    return False


class CrossProcessOwnershipTests(unittest.TestCase):
    """True multi-process proofs."""

    def test_second_process_cannot_own_simultaneously(self):
        name = TEST_MUTEX + "_A"
        holder = _spawn(HOLDER, name, hold=4.0)
        self.addCleanup(holder.kill)
        self.assertTrue(_wait_for_acquired(holder), "child never acquired")

        mine = LtfsOwnership(name=name, timeout=1.0)
        started = time.time()
        with self.assertRaises(LtfsOwnershipError) as ctx:
            mine.acquire("contender", timeout=1.0)
        waited = time.time() - started
        self.assertLess(waited, 15, "acquire did not honour its timeout")
        self.assertEqual(ctx.exception.classification,
                         "ltfs_ownership_unavailable")
        self.assertFalse(mine.owned_by_this_process())

    def test_second_process_touches_no_tape_adapter_on_denial(self):
        """A denied acquisition must perform no LTFS operation whatsoever."""
        name = TEST_MUTEX + "_B"
        holder = _spawn(HOLDER, name, hold=4.0)
        self.addCleanup(holder.kill)
        self.assertTrue(_wait_for_acquired(holder))

        calls = []

        class RecordingCommand(ltfs.LtfsDriveCommand):
            def drive_status(self, drive_path):
                calls.append(drive_path)
                return "LTFS_MOUNTED", "", None

        previous = ltfs.set_ltfs_drive_command(RecordingCommand())
        self.addCleanup(ltfs.set_ltfs_drive_command, previous)

        mine = LtfsOwnership(name=name, timeout=0.5)
        with self.assertRaises(LtfsOwnershipError):
            mine.acquire("contender", timeout=0.5)
        self.assertEqual(calls, [], "a denied owner still probed the drive")

    def test_ownership_transfers_after_holder_releases(self):
        name = TEST_MUTEX + "_C"
        holder = _spawn(HOLDER, name, hold=1.0)
        self.addCleanup(holder.kill)
        self.assertTrue(_wait_for_acquired(holder))

        mine = LtfsOwnership(name=name, timeout=20)
        self.assertTrue(mine.acquire("successor", timeout=20))
        try:
            self.assertTrue(mine.owned_by_this_process())
        finally:
            mine.release(operation="successor")

    def test_abandoned_mutex_is_detected_and_recovered(self):
        """A peer that dies holding ownership must surface as ABANDONED.

        Our handle must exist BEFORE the crasher runs: Windows destroys the
        mutex when its last handle closes, and a recreated object reports a
        clean acquisition instead of an abandonment. LtfsOwnership opens its
        handle in __init__ for exactly this reason, so constructing it first is
        also what production does.
        """
        name = TEST_MUTEX + "_D"
        mine = LtfsOwnership(name=name, timeout=15)      # handle opens here
        self.assertIsNotNone(mine._handle, "handle was not opened eagerly")

        crasher = _spawn(CRASHER, name)
        self.addCleanup(crasher.kill)
        self.assertTrue(_wait_for_acquired(crasher))
        crasher.wait(timeout=15)          # died holding the mutex

        seen = []
        previous_cb = own.set_ownership_change_callback(seen.append)
        self.addCleanup(own.set_ownership_change_callback, previous_cb)

        self.assertTrue(mine.acquire("recoverer", timeout=15))
        try:
            self.assertTrue(mine._last_abandoned,
                            "abandoned mutex was not reported")
            self.assertTrue(any("abandoned=True" in s for s in seen),
                            "abandoned recovery did not invalidate readiness")
        finally:
            mine.release(operation="recoverer")

    def test_abandoned_recovery_invalidates_readiness_cache(self):
        name = TEST_MUTEX + "_D2"
        mine = LtfsOwnership(name=name, timeout=15)
        crasher = _spawn(CRASHER, name)
        self.addCleanup(crasher.kill)
        self.assertTrue(_wait_for_acquired(crasher))
        crasher.wait(timeout=15)

        # Prime a readiness entry, then prove the recovery clears it.
        previous_cb = own.set_ownership_change_callback(
            lambda detail: ltfs.note_tape_ownership_lost(detail))
        self.addCleanup(own.set_ownership_change_callback, previous_cb)
        ltfs.READINESS.store(ltfs.ReadinessState(
            drive="X:\\", device_detected=True, mount_status_known=True,
            mount_status="LTFS_MOUNTED", expected_cartridge_verified=False,
            cartridge_label=None, pid=os.getpid(), verified_at=time.time(),
            generation=0))
        self.assertIsNotNone(ltfs.READINESS.snapshot())
        mine.acquire("recoverer", timeout=15)
        try:
            self.assertIsNone(ltfs.READINESS.snapshot(),
                              "readiness survived an abandoned-mutex recovery")
        finally:
            mine.release(operation="recoverer")


class OwnershipSemanticsTests(unittest.TestCase):
    def setUp(self):
        self.own = LtfsOwnership(name=TEST_MUTEX + "_E", timeout=5)

    def test_released_after_normal_operation(self):
        self.own.acquire("normal")
        self.own.release(operation="normal")
        self.assertFalse(self.own.owned_by_this_process())
        self.assertEqual(self.own.depth, 0)

    def test_released_after_exception(self):
        with self.assertRaises(ValueError):
            self.own.acquire("boom")
            try:
                raise ValueError("failure inside the critical section")
            finally:
                self.own.release(operation="boom")
        self.assertFalse(self.own.owned_by_this_process())

    def test_recursive_acquisition_does_not_deadlock(self):
        self.own.acquire("outer", timeout=5)
        self.own.acquire("inner", timeout=5)
        self.assertEqual(self.own.depth, 2)
        self.own.release(operation="inner")
        self.assertTrue(self.own.owned_by_this_process())
        self.own.release(operation="outer")
        self.assertFalse(self.own.owned_by_this_process())

    def test_release_without_ownership_raises(self):
        with self.assertRaises(LtfsOwnershipError):
            self.own.release()

    def test_mutex_name_is_derived_from_stable_config(self):
        # Pin the identity so the property (stable, config-derived, PID-free)
        # is asserted without depending on the operator's untracked config.ini.
        with mock.patch.object(own, "configured_ownership_id",
                               return_value="drive_0000000000"):
            name = own.default_mutex_name()
            self.assertTrue(name.startswith("OWC_LTO8_LTFS_OWNERSHIP_"))
            self.assertNotIn(str(os.getpid()), name)
            self.assertEqual(name, own.default_mutex_name())   # deterministic

    def test_generation_increments_on_each_acquisition(self):
        before = self.own.generation
        self.own.acquire("g1"); self.own.release()
        self.own.acquire("g2"); self.own.release()
        self.assertEqual(self.own.generation, before + 2)


class ProtectedOperationTests(unittest.TestCase):
    """Low-level LTFS helpers must refuse to run without ownership."""

    def setUp(self):
        ltfs.reset_readiness("test setup")
        self.addCleanup(ltfs.reset_readiness, "test teardown")

    def test_ltfscmddrives_requires_ownership(self):
        cmd = ltfs.DefaultLtfsDriveCommand()
        with self.assertRaises(LtfsOwnershipError):
            cmd.drive_status("X:\\")

    def test_volume_label_read_requires_ownership(self):
        with self.assertRaises(LtfsOwnershipError):
            ltfs._read_volume_label_unlocked("X:\\")

    def test_eject_requires_ownership(self):
        with self.assertRaises(LtfsOwnershipError):
            ltfs._eject_tape_unlocked("X:\\", ibm_eject_cmd="fake.exe")

    def test_readiness_runs_only_under_ownership(self):
        """_ensure_lto_drive_ready takes the lock, so the probe is protected."""
        seen = {}

        class OwnershipAwareCommand(ltfs.LtfsDriveCommand):
            def drive_status(self, drive_path):
                seen["owned"] = own.owns_ltfs()
                return "LTFS_MOUNTED", "", None

        previous = ltfs.set_ltfs_drive_command(OwnershipAwareCommand())
        self.addCleanup(ltfs.set_ltfs_drive_command, previous)
        self.assertTrue(ltfs._ensure_lto_drive_ready("X:\\"))
        self.assertTrue(seen["owned"], "readiness ran without ownership")

    def test_cartridge_verification_runs_only_under_ownership(self):
        seen = {}

        class Cmd(ltfs.LtfsDriveCommand):
            def drive_status(self, drive_path):
                return "LTFS_MOUNTED", "", None

        def fake_label(drive):
            seen["owned"] = own.owns_ltfs()
            return "Tape_TEST"

        previous = ltfs.set_ltfs_drive_command(Cmd())
        self.addCleanup(ltfs.set_ltfs_drive_command, previous)
        with mock.patch.object(ltfs, "_read_volume_label_unlocked", fake_label):
            self.assertTrue(ltfs._ensure_lto_drive_ready(
                "X:\\", expected_label="Tape_TEST"))
        self.assertTrue(seen["owned"], "cartridge check ran without ownership")


class ReadinessOwnershipIntegrationTests(unittest.TestCase):
    def setUp(self):
        ltfs.reset_readiness("test setup")
        self.addCleanup(ltfs.reset_readiness, "test teardown")

        class Cmd(ltfs.LtfsDriveCommand):
            def __init__(self):
                self.calls = 0

            def drive_status(self, drive_path):
                self.calls += 1
                return "LTFS_MOUNTED", "", None

        self.cmd = Cmd()
        previous = ltfs.set_ltfs_drive_command(self.cmd)
        self.addCleanup(ltfs.set_ltfs_drive_command, previous)

    def test_releasing_ownership_invalidates_readiness(self):
        self.assertTrue(ltfs._ensure_lto_drive_ready("X:\\"))
        # _ensure_lto_drive_ready released the lock on the way out.
        self.assertIsNone(ltfs.READINESS.snapshot(),
                          "readiness survived an ownership release")

    def test_reacquisition_reverifies_and_cannot_reuse_stale_state(self):
        ltfs._ensure_lto_drive_ready("X:\\")
        first = self.cmd.calls
        ltfs._ensure_lto_drive_ready("X:\\")
        self.assertEqual(self.cmd.calls, first + 1,
                         "stale readiness was reused across ownership")

    def test_cached_readiness_is_reusable_while_ownership_is_held(self):
        """Within one ownership period the cache still works (Phase 4 relies
        on this: one verification per batch, not per chunk)."""
        rt._acquire_tape_io_lock("batch")
        try:
            ltfs._ensure_lto_drive_ready_unlocked("X:\\")
            calls = self.cmd.calls
            for _ in range(4):
                ltfs._ensure_lto_drive_ready_unlocked("X:\\")
            self.assertEqual(self.cmd.calls, calls,
                             "re-verified while ownership was held")
        finally:
            rt._release_tape_io_lock()

    def test_failed_acquisition_leaves_readiness_invalid(self):
        name = TEST_MUTEX + "_F"
        holder = _spawn(HOLDER, name, hold=3.0)
        self.addCleanup(holder.kill)
        self.assertTrue(_wait_for_acquired(holder))
        mine = LtfsOwnership(name=name, timeout=0.5)
        with self.assertRaises(LtfsOwnershipError):
            mine.acquire("denied", timeout=0.5)
        self.assertIsNone(ltfs.READINESS.snapshot())


class RuntimeLockIntegrationTests(unittest.TestCase):
    def test_tape_io_lock_takes_cross_process_ownership(self):
        rt._acquire_tape_io_lock("integration")
        try:
            self.assertTrue(own.owns_ltfs())
        finally:
            rt._release_tape_io_lock()
        self.assertFalse(own.owns_ltfs())

    def test_tape_io_lock_is_recursive(self):
        rt._acquire_tape_io_lock("outer")
        rt._acquire_tape_io_lock("inner")
        try:
            self.assertTrue(own.owns_ltfs())
        finally:
            rt._release_tape_io_lock()
            rt._release_tape_io_lock()
        self.assertFalse(own.owns_ltfs())

    def test_lock_released_when_body_raises(self):
        with self.assertRaises(ValueError):
            rt._acquire_tape_io_lock("raises")
            try:
                raise ValueError("boom")
            finally:
                rt._release_tape_io_lock()
        self.assertFalse(own.owns_ltfs())

    def test_threads_do_not_deadlock_and_are_serialised(self):
        order, errors = [], []

        def worker(n):
            try:
                rt._acquire_tape_io_lock(f"thread-{n}")
                try:
                    order.append(("in", n))
                    time.sleep(0.05)
                    order.append(("out", n))
                finally:
                    rt._release_tape_io_lock()
            except Exception as e:            # pragma: no cover
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
            self.assertFalse(t.is_alive(), "thread deadlocked on the tape lock")
        self.assertEqual(errors, [])
        # Strict alternation proves serialisation: never two 'in' in a row.
        for a, b in zip(order, order[1:]):
            if a[0] == "in":
                self.assertEqual(b, ("out", a[1]))


class WatchdogAndHelperTests(unittest.TestCase):
    def test_watchdog_no_longer_probes_the_mount(self):
        path = os.path.join(PROJECT_ROOT, "scripts", "archive_watchdog.ps1")
        with open(path, encoding="utf-8-sig") as fh:
            body = fh.read()
        active = [ln for ln in body.splitlines()
                  if ln.strip() and not ln.strip().startswith("#")]
        active_text = "\n".join(active)
        self.assertNotIn("vol ${letter}:", active_text)
        self.assertNotIn("Test-Path $drive", active_text)

    def test_watchdog_still_exits_early_when_archiver_is_running(self):
        path = os.path.join(PROJECT_ROOT, "scripts", "archive_watchdog.ps1")
        with open(path, encoding="utf-8-sig") as fh:
            body = fh.read()
        self.assertIn("archiver already running", body)
        self.assertIn("exit 0", body)

    def test_manual_helper_acquires_the_same_mutex(self):
        path = os.path.join(PROJECT_ROOT, "scripts", "post_remount_check.py")
        with open(path, encoding="utf-8") as fh:
            body = fh.read()
        self.assertIn("from src.ltfs_ownership import", body)
        self.assertIn("OWNERSHIP.acquire(", body)
        self.assertIn("OWNERSHIP.release(", body)


if __name__ == "__main__":
    unittest.main()
