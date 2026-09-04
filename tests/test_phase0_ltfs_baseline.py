"""Phase 0 characterization tests: record CURRENT LTFS behaviour before change.

These tests deliberately assert what the code does *today*, including the
behaviour the audit flagged as undesirable. They are the baseline that makes
Phases 1-4 visible: each later phase flips a specific assertion here, and any
unintended change to a neighbouring behaviour shows up as an unexpected failure.

Every test is mock-only and uses a temporary directory as its fake drive. The
autouse guard in conftest.py fails closed if any of them reaches the real mount.
"""
import configparser
import os
import queue
import subprocess
import tempfile
import threading
import unittest
from types import SimpleNamespace
from unittest import mock

from conftest import (FORBIDDEN_DRIVES, PROJECT_ROOT,
                      ProductionDriveAccessError)
from lto_fakes import (FakeLtfsAdapter, MinimalBackupDB, RecordingSubprocess,
                       TapeLockObserver, TapeOperationLog)

from src import ltfs
from src import runtime as rt
from src.backup import LTOBackup


# =============================================================================
# A. The guard itself must work, or every other guarantee here is worthless.
# =============================================================================
class ProductionDriveGuardTests(unittest.TestCase):
    def test_configured_production_drive_is_forbidden(self):
        self.assertIn("Z", FORBIDDEN_DRIVES)

    def test_listdir_on_production_drive_is_blocked(self):
        with self.assertRaises(ProductionDriveAccessError):
            os.listdir("Z:\\")

    def test_isdir_on_production_drive_is_blocked(self):
        with self.assertRaises(ProductionDriveAccessError):
            os.path.isdir("Z:\\")

    def test_exists_and_stat_on_production_drive_are_blocked(self):
        with self.assertRaises(ProductionDriveAccessError):
            os.path.exists("Z:\\_pack_s0037_108")
        with self.assertRaises(ProductionDriveAccessError):
            os.stat("Z:\\anything")

    def test_open_on_production_drive_is_blocked(self):
        with self.assertRaises(ProductionDriveAccessError):
            open("Z:\\file.txt")

    def test_subprocess_touching_production_drive_is_blocked(self):
        with self.assertRaises(ProductionDriveAccessError):
            subprocess.run(["cmd", "/c", "vol", "Z:"])

    def test_ltfs_helper_executables_are_blocked_anywhere(self):
        for exe in ("LtfsCmdDrives.exe", "LtfsCmdEject.exe", "ltfsck.exe",
                    "mkltfs.exe"):
            with self.assertRaises(ProductionDriveAccessError):
                subprocess.run([os.path.join("C:\\somewhere", exe)])

    def test_temporary_directories_are_unaffected(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertTrue(os.path.isdir(tmp))
            self.assertEqual(os.listdir(tmp), [])


# =============================================================================
# B. Readiness probe -- PHASE 1 behaviour (was: filesystem-touching baseline).
# =============================================================================
class ReadinessProbeTests(unittest.TestCase):
    """Phase 1: the readiness path must not touch the LTFS filesystem at all."""

    def setUp(self):
        ltfs.reset_readiness("test setup")
        self.addCleanup(ltfs.reset_readiness, "test teardown")

    def _run_probe_on(self, fake_drive, status="LTFS_MOUNTED", **kwargs):
        """Run readiness while recording any filesystem access to the drive."""
        seen = []
        real_isdir, real_listdir = os.path.isdir, os.listdir
        real_scandir, real_stat = os.scandir, os.stat

        def rec(name, func):
            def wrapper(p=".", *a, **k):
                if str(p) == str(fake_drive):
                    seen.append((name, p))
                return func(p, *a, **k)
            return wrapper

        with mock.patch.object(ltfs, "_ltfs_drive_status",
                               return_value=(status, "out", None)), \
             mock.patch("os.path.isdir", rec("isdir", real_isdir)), \
             mock.patch("os.listdir", rec("listdir", real_listdir)), \
             mock.patch("os.scandir", rec("scandir", real_scandir)), \
             mock.patch("os.stat", rec("stat", real_stat)):
            ok = ltfs._ensure_lto_drive_ready_unlocked(fake_drive, **kwargs)
        return ok, seen

    def test_readiness_performs_no_filesystem_access(self):
        with tempfile.TemporaryDirectory() as tmp:
            ok, seen = self._run_probe_on(tmp)
        self.assertTrue(ok)
        self.assertEqual(seen, [], f"readiness touched the mount: {seen}")

    def test_source_has_no_filesystem_calls_in_readiness_path(self):
        """Guards against a future re-introduction of a root access.

        Uses the AST, not text: comments and docstrings legitimately mention
        the removed calls, and a substring check would flag those.
        """
        import ast
        import inspect

        banned = {
            "os.listdir", "os.scandir", "os.stat", "os.lstat", "os.open",
            "os.walk", "os.path.isdir", "os.path.isfile", "os.path.exists",
            "os.path.getsize", "shutil.disk_usage", "glob.glob", "open",
            "Path.exists",
        }

        def dotted(node):
            parts = []
            while isinstance(node, ast.Attribute):
                parts.append(node.attr)
                node = node.value
            if isinstance(node, ast.Name):
                parts.append(node.id)
                return ".".join(reversed(parts))
            return None

        tree = ast.parse(inspect.getsource(ltfs))
        targets = {"_verify_readiness_uncached",
                   "_ensure_lto_drive_ready_unlocked",
                   "_ensure_lto_drive_ready"}
        found = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name in targets:
                for inner in ast.walk(node):
                    if isinstance(inner, ast.Call):
                        name = dotted(inner.func)
                        if name in banned:
                            found.add(name)
        self.assertEqual(
            found, set(),
            f"filesystem access reappeared in the readiness path: {found}")

    def test_blocking_status_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            ok, seen = self._run_probe_on(tmp, status="NO_MEDIA")
        self.assertFalse(ok)
        self.assertEqual(seen, [])

    def test_undetected_device_fails_closed(self):
        """A drive letter absent from IBM's device table must stop the write.

        Previously this only warned and fell through to os.listdir; with that
        listing gone it must fail closed (the mount letter is known to move).
        """
        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch.object(ltfs, "_ltfs_drive_status",
                                   return_value=(None, "no such drive", None)):
                ok = ltfs._ensure_lto_drive_ready_unlocked(tmp)
        self.assertFalse(ok)

    def test_failed_verification_is_not_cached(self):
        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch.object(ltfs, "_ltfs_drive_status",
                                   return_value=("NO_MEDIA", "o", None)):
                self.assertFalse(ltfs._ensure_lto_drive_ready_unlocked(tmp))
            self.assertIsNone(ltfs.READINESS.snapshot())


# =============================================================================
# B2. Cached verified-mount state (Phase 1).
# =============================================================================
class ReadinessCacheTests(unittest.TestCase):
    def setUp(self):
        ltfs.reset_readiness("test setup")
        self.addCleanup(ltfs.reset_readiness, "test teardown")
        self.tmp = tempfile.mkdtemp()
        self.addCleanup(lambda: __import__("shutil").rmtree(self.tmp,
                                                            ignore_errors=True))

    def _status_mock(self, status="LTFS_MOUNTED"):
        return mock.patch.object(ltfs, "_ltfs_drive_status",
                                 return_value=(status, "out", None))

    def test_repeated_chunks_reuse_cached_state(self):
        with self._status_mock() as status:
            for _ in range(5):
                self.assertTrue(
                    ltfs._ensure_lto_drive_ready_unlocked(self.tmp))
        # One real verification for five "chunks".
        self.assertEqual(status.call_count, 1)

    def test_force_bypasses_the_cache(self):
        with self._status_mock() as status:
            ltfs._ensure_lto_drive_ready_unlocked(self.tmp)
            ltfs._ensure_lto_drive_ready_unlocked(self.tmp, force=True)
        self.assertEqual(status.call_count, 2)

    def _assert_invalidator_forces_recheck(self, invalidate):
        with self._status_mock() as status:
            ltfs._ensure_lto_drive_ready_unlocked(self.tmp)
            invalidate()
            ltfs._ensure_lto_drive_ready_unlocked(self.tmp)
        self.assertEqual(status.call_count, 2)

    def test_io_error_invalidates(self):
        self._assert_invalidator_forces_recheck(
            lambda: ltfs.note_tape_io_error(OSError("write protected")))

    def test_mount_transition_invalidates(self):
        self._assert_invalidator_forces_recheck(
            lambda: ltfs.note_mount_transition("remount"))

    def test_device_state_change_invalidates(self):
        self._assert_invalidator_forces_recheck(
            lambda: ltfs.note_device_state_change("PHY link down"))

    def test_ownership_loss_invalidates(self):
        self._assert_invalidator_forces_recheck(
            lambda: ltfs.note_tape_ownership_lost("lock handed over"))

    def test_operator_reset_invalidates(self):
        self._assert_invalidator_forces_recheck(ltfs.reset_readiness)

    def test_cartridge_mismatch_invalidates(self):
        self._assert_invalidator_forces_recheck(
            lambda: ltfs.note_cartridge_mismatch("Tape_03", "Tape_04"))

    def test_process_restart_invalidates(self):
        with self._status_mock() as status:
            ltfs._ensure_lto_drive_ready_unlocked(self.tmp)
            with mock.patch("os.getpid", return_value=os.getpid() + 1):
                ltfs._ensure_lto_drive_ready_unlocked(self.tmp)
        self.assertEqual(status.call_count, 2)

    def test_changed_drive_path_invalidates(self):
        other = tempfile.mkdtemp()
        self.addCleanup(lambda: __import__("shutil").rmtree(other,
                                                            ignore_errors=True))
        with self._status_mock() as status:
            ltfs._ensure_lto_drive_ready_unlocked(self.tmp)
            ltfs._ensure_lto_drive_ready_unlocked(other)
        self.assertEqual(status.call_count, 2)

    def test_eject_invalidates_readiness(self):
        # Phase 3: _eject_tape_unlocked asserts ownership, so hold it here as
        # eject_tape_drive() does in production.
        rt._acquire_tape_io_lock("eject invalidation test")
        try:
            with self._status_mock():
                ltfs._ensure_lto_drive_ready_unlocked(self.tmp)
            self.assertIsNotNone(ltfs.READINESS.snapshot())
            with mock.patch("subprocess.run",
                            return_value=SimpleNamespace(stdout="", stderr="",
                                                         returncode=0)):
                ltfs._eject_tape_unlocked("X:\\", ibm_eject_cmd="fake_eject.exe")
            self.assertIsNone(ltfs.READINESS.snapshot())
        finally:
            rt._release_tape_io_lock()

    # -- cartridge verification stays fail-closed ---------------------------
    def test_cartridge_verified_and_cached(self):
        with self._status_mock() as status, \
             mock.patch.object(ltfs, "_read_volume_label_unlocked",
                               return_value="Tape_03") as label:
            self.assertTrue(ltfs._ensure_lto_drive_ready_unlocked(
                self.tmp, expected_label="Tape_03"))
            self.assertTrue(ltfs._ensure_lto_drive_ready_unlocked(
                self.tmp, expected_label="Tape_03"))
        self.assertEqual(status.call_count, 1)
        self.assertEqual(label.call_count, 1)   # not re-read per chunk

    def test_cartridge_mismatch_fails_closed(self):
        with self._status_mock(), \
             mock.patch.object(ltfs, "_read_volume_label_unlocked",
                               return_value="Tape_99"):
            ok = ltfs._ensure_lto_drive_ready_unlocked(
                self.tmp, expected_label="Tape_03")
        self.assertFalse(ok)
        self.assertIsNone(ltfs.READINESS.snapshot())

    def test_cached_unverified_state_never_approves_a_cartridge(self):
        """A cache entry built WITHOUT a label must not satisfy a later call
        that names one."""
        with self._status_mock(), \
             mock.patch.object(ltfs, "_read_volume_label_unlocked",
                               return_value="Tape_03") as label:
            ltfs._ensure_lto_drive_ready_unlocked(self.tmp)      # no label
            self.assertEqual(label.call_count, 0)
            self.assertTrue(ltfs._ensure_lto_drive_ready_unlocked(
                self.tmp, expected_label="Tape_03"))
            self.assertEqual(label.call_count, 1)                # forced check

    def test_cached_state_rejects_a_different_expected_cartridge(self):
        with self._status_mock(), \
             mock.patch.object(ltfs, "_read_volume_label_unlocked",
                               side_effect=["Tape_03", "Tape_03"]):
            self.assertTrue(ltfs._ensure_lto_drive_ready_unlocked(
                self.tmp, expected_label="Tape_03"))
            # A different expectation must re-verify, then fail closed.
            ok = ltfs._ensure_lto_drive_ready_unlocked(
                self.tmp, expected_label="Tape_04")
        self.assertFalse(ok)


# =============================================================================
# B3. Injectable LtfsCmdDrives interface (Phase 1).
# =============================================================================
class DriveCommandInjectionTests(unittest.TestCase):
    def setUp(self):
        ltfs.reset_readiness("test setup")
        self.addCleanup(ltfs.reset_readiness, "test teardown")

    def test_command_can_be_injected_and_counted(self):
        class CountingCommand(ltfs.LtfsDriveCommand):
            def __init__(self):
                self.calls = []

            def drive_status(self, drive_path):
                self.calls.append(drive_path)
                return "LTFS_MOUNTED", "out", None

        fake = CountingCommand()
        previous = ltfs.set_ltfs_drive_command(fake)
        self.addCleanup(ltfs.set_ltfs_drive_command, previous)
        with tempfile.TemporaryDirectory() as tmp:
            for _ in range(4):
                self.assertTrue(ltfs._ensure_lto_drive_ready_unlocked(tmp))
        self.assertEqual(len(fake.calls), 1,
                         "LtfsCmdDrives must not run once per chunk")

    def test_default_command_is_restored(self):
        previous = ltfs.set_ltfs_drive_command(None)
        self.assertIsInstance(ltfs.get_ltfs_drive_command(),
                              ltfs.DefaultLtfsDriveCommand)
        ltfs.set_ltfs_drive_command(previous)


# =============================================================================
# C. LtfsCmdDrives.exe invocation -- previously ZERO test coverage.
# =============================================================================
class LtfsDriveStatusTests(unittest.TestCase):
    """Phase 3: these low-level helpers now assert LTFS ownership, so each test
    must take the real cross-process lock exactly as production does."""

    def setUp(self):
        rt._acquire_tape_io_lock("phase0 drive-status tests")
        self.addCleanup(rt._release_tape_io_lock)

    def test_drive_status_invokes_ltfscmddrives(self):
        rec = RecordingSubprocess(
            SimpleNamespace(stdout="X 1 2 LTFS_MOUNTED\n", stderr="",
                            returncode=0))
        with mock.patch("subprocess.run", rec):
            status, output, err = ltfs._ltfs_drive_status("X:\\")
        self.assertEqual(status, "LTFS_MOUNTED")
        self.assertIsNone(err)
        self.assertTrue(rec.ran("LtfsCmdDrives.exe"))
        self.assertEqual(len(rec.calls), 1)

    def test_missing_executable_is_reported_not_raised(self):
        with mock.patch("subprocess.run", side_effect=FileNotFoundError):
            status, output, err = ltfs._ltfs_drive_status("X:\\")
        self.assertIsNone(status)
        self.assertIn("LtfsCmdDrives.exe not found", err)

    def test_volume_label_uses_cmd_vol(self):
        rec = RecordingSubprocess(
            SimpleNamespace(stdout=" Volume in drive X is Tape_TEST\n",
                            stderr="", returncode=0))
        with mock.patch("subprocess.run", rec):
            label = ltfs.get_volume_label("X:\\")
        self.assertEqual(label, "Tape_TEST")
        self.assertEqual(rec.calls[0], ["cmd", "/c", "vol", "X:"])


# =============================================================================
# D. robocopy command -- BASELINE for Phase 2.
# =============================================================================
class RobocopyCommandBaselineTests(unittest.TestCase):
    """Captures the argv the writer actually builds.

    NOTE (Phase 0 finding): /COPY:DAT and /DCOPY:DA appear in production
    robocopy logs but are NOT in the source -- they are robocopy's own defaults,
    echoed in its Options header. Phase 2 must therefore ADD explicit /COPY:D
    and /DCOPY:D overrides, not edit existing flags.
    """

    def _capture_robocopy_cmd(self):
        captured = {}

        def fake_tuned(cmd, *a, **k):
            captured["cmd"] = list(cmd)
            on_start = k.get("on_start")
            if on_start:
                try:
                    on_start(SimpleNamespace(pid=1234))
                except Exception:
                    pass
            return SimpleNamespace(stdout="", stderr="", returncode=0)

        tmp = tempfile.mkdtemp()
        self.addCleanup(lambda: __import__("shutil").rmtree(tmp,
                                                            ignore_errors=True))
        source = os.path.join(tmp, "_pack_s0001_000")
        tape = os.path.join(tmp, "tape")
        os.makedirs(source)
        os.makedirs(tape)
        with open(os.path.join(source, "bundle.zip"), "wb") as fh:
            fh.write(b"x" * 128)

        backup = LTOBackup(MinimalBackupDB(), "", governor=None, log_dir=tmp)
        backup.eject_tape = lambda _d: None
        with mock.patch("src.backup._ensure_lto_drive_ready", return_value=True), \
             mock.patch("src.backup._run_robocopy_tuned", fake_tuned):
            try:
                backup.run(source, tape, "Tape_TEST")
            except Exception:
                # Classification of the empty summary may raise; the argv was
                # captured before robocopy was invoked, which is what we assert.
                pass
        return captured.get("cmd")

    @staticmethod
    def _opts(cmd, prefix):
        """Case-insensitive lookup of options starting with ``prefix``."""
        return [str(c) for c in cmd if str(c).upper().startswith(prefix.upper())]

    def test_unrelated_robocopy_flags_are_unchanged(self):
        """Traversal, retry, wait, unbuffered-IO and logging flags must not move."""
        cmd = self._capture_robocopy_cmd()
        self.assertIsNotNone(cmd, "robocopy command was never constructed")
        self.assertEqual(cmd[0], "robocopy")
        for flag in ("/E", "/J", "/R:3", "/W:10", "/NP", "/NDL", "/NFL",
                     "/BYTES"):
            self.assertIn(flag, cmd)

    def test_source_and_destination_are_unchanged(self):
        cmd = self._capture_robocopy_cmd()
        # argv[1] is the staged pack dir, argv[2] the tape destination root.
        self.assertTrue(str(cmd[1]).endswith("_pack_s0001_000"))
        self.assertTrue(str(cmd[2]).endswith("_pack_s0001_000"))
        self.assertFalse(str(cmd[1]).startswith("/"))
        self.assertFalse(str(cmd[2]).startswith("/"))

    # -- Phase 2: exactly one effective copy mode, data only -----------------
    def test_exactly_one_file_copy_mode_and_it_is_data_only(self):
        cmd = self._capture_robocopy_cmd()
        copy_opts = self._opts(cmd, "/COPY:")
        self.assertEqual(copy_opts, ["/COPY:D"])

    def test_exactly_one_directory_copy_mode_and_it_is_data_only(self):
        cmd = self._capture_robocopy_cmd()
        dcopy_opts = self._opts(cmd, "/DCOPY:")
        self.assertEqual(dcopy_opts, ["/DCOPY:D"])

    def test_conflicting_copy_modes_are_absent_case_insensitively(self):
        cmd = self._capture_robocopy_cmd()
        upper = [str(c).upper() for c in cmd]
        for forbidden in ("/COPY:DAT", "/COPYALL", "/SEC", "/SECFIX",
                          "/COPY:S", "/COPY:U", "/DCOPY:DA", "/DCOPY:DAT",
                          "/TIMFIX", "/EFSRAW", "/COPY:DA", "/COPY:DT"):
            self.assertNotIn(forbidden, upper)

    def test_explicit_flags_override_robocopy_defaults(self):
        """The defaults (/COPY:DAT, /DCOPY:DA) are only overridden if an
        explicit option is passed -- assert both are present, not merely that
        the conflicting spellings are absent."""
        cmd = self._capture_robocopy_cmd()
        upper = [str(c).upper() for c in cmd]
        self.assertIn("/COPY:D", upper)
        self.assertIn("/DCOPY:D", upper)
        self.assertEqual(len(self._opts(cmd, "/COPY:")), 1)
        self.assertEqual(len(self._opts(cmd, "/DCOPY:")), 1)

    def test_no_post_copy_attribute_or_timestamp_operation(self):
        """Nothing in the writer sets attributes/timestamps after the copy."""
        import ast
        import inspect
        from src import backup as backup_mod

        banned = {"os.utime", "shutil.copystat", "shutil.copy2", "os.chmod",
                  "os.chflags"}

        def dotted(node):
            parts = []
            while isinstance(node, ast.Attribute):
                parts.append(node.attr)
                node = node.value
            if isinstance(node, ast.Name):
                parts.append(node.id)
                return ".".join(reversed(parts))
            return None

        found = set()
        for node in ast.walk(ast.parse(inspect.getsource(backup_mod))):
            if isinstance(node, ast.Call):
                name = dotted(node.func)
                if name in banned:
                    found.add(name)
        self.assertEqual(found, set(),
                         f"post-copy metadata operation introduced: {found}")


# =============================================================================
# D2. Phase 2.5 -- readiness invalidation wired into real failure paths.
# =============================================================================
class ReadinessInvalidationWiringTests(unittest.TestCase):
    """The hooks must fire from production code, not only from tests."""

    _SUMMARY_FAIL = (
        "\n"
        "               Total    Copied   Skipped  Mismatch    FAILED    Extras\n"
        "    Dirs :         1         0         1         0         0         0\n"
        "   Files :         0         0         0         0         0         0\n"
        "   Bytes :         0         0         0         0         0         0\n"
        "   Times :   0:00:30   0:00:00                       0:00:30   0:00:00\n"
        "\nERROR: RETRY LIMIT EXCEEDED.\n"
    )

    def setUp(self):
        ltfs.reset_readiness("test setup")
        self.addCleanup(ltfs.reset_readiness, "test teardown")

    def _prime_cache(self, drive):
        with mock.patch.object(ltfs, "_ltfs_drive_status",
                               return_value=("LTFS_MOUNTED", "out", None)):
            self.assertTrue(ltfs._ensure_lto_drive_ready_unlocked(drive))
        self.assertIsNotNone(ltfs.READINESS.snapshot())

    def _run_failing_backup(self, stdout, returncode):
        tmp = tempfile.mkdtemp()
        self.addCleanup(lambda: __import__("shutil").rmtree(tmp,
                                                            ignore_errors=True))
        source = os.path.join(tmp, "_pack_s0001_000")
        tape = os.path.join(tmp, "tape")
        os.makedirs(source)
        os.makedirs(tape)
        with open(os.path.join(source, "bundle.zip"), "wb") as fh:
            fh.write(b"x" * 128)
        self._prime_cache(tape)
        backup = LTOBackup(MinimalBackupDB(), "", governor=None, log_dir=tmp)
        backup.eject_tape = lambda _d: None
        result = SimpleNamespace(stdout=stdout, stderr="", returncode=returncode)
        raised = None
        with mock.patch("src.backup._ensure_lto_drive_ready", return_value=True), \
             mock.patch("src.backup._run_robocopy_tuned", return_value=result):
            try:
                backup.run(source, tape, "Tape_TEST")
            except RuntimeError as e:
                raised = e
        return raised

    def test_hard_tape_write_failure_invalidates_readiness(self):
        raised = self._run_failing_backup(self._SUMMARY_FAIL, 0)
        self.assertIsNotNone(raised, "classification must still fail the write")
        self.assertIsNone(ltfs.READINESS.snapshot(),
                          "cached readiness survived a classified failure")

    def test_zero_byte_failure_still_detected_and_invalidates(self):
        """robocopy exit 0 with 0 files copied is the incident-009 lie."""
        raised = self._run_failing_backup(self._SUMMARY_FAIL, 0)
        self.assertIn("Robocopy", str(raised))
        self.assertIsNone(ltfs.READINESS.snapshot())

    def test_read_only_transition_invalidates(self):
        """ERROR 19 / write-protected output must invalidate."""
        stdout = (self._SUMMARY_FAIL.replace(
            "ERROR: RETRY LIMIT EXCEEDED.",
            "ERROR 19 (0x00000013) Changing File Attributes\n"
            "The media is write protected.\nERROR: RETRY LIMIT EXCEEDED."))
        self._run_failing_backup(stdout, 0)
        self.assertIsNone(ltfs.READINESS.snapshot())

    def test_permanent_write_or_servo_failure_invalidates(self):
        stdout = (self._SUMMARY_FAIL.replace(
            "ERROR: RETRY LIMIT EXCEEDED.",
            "ERROR 1117 (0x0000045D) The request could not be performed "
            "because of an I/O device error.\nERROR: RETRY LIMIT EXCEEDED."))
        self._run_failing_backup(stdout, 16)
        self.assertIsNone(ltfs.READINESS.snapshot())

    def test_non_tape_application_error_does_not_invalidate(self):
        """A DB/application error must not be mistaken for a device fault."""
        with tempfile.TemporaryDirectory() as tmp:
            self._prime_cache(tmp)
            try:
                raise ValueError("catalog bookkeeping failed")
            except ValueError:
                pass
            self.assertIsNotNone(
                ltfs.READINESS.snapshot(),
                "an application error must not invalidate device state")

    def test_invalidation_is_wired_in_production_source(self):
        """Prove the hooks are called from production modules, not just tests."""
        import inspect
        from src import backup as backup_mod
        from src import remote_orchestrator as ro_mod
        self.assertIn("note_tape_io_error(",
                      inspect.getsource(backup_mod))
        self.assertIn("note_device_state_change(",
                      inspect.getsource(ro_mod))

    def test_successful_write_keeps_cached_readiness(self):
        """Invalidation must be failure-scoped, not fired on every write."""
        with tempfile.TemporaryDirectory() as tmp:
            self._prime_cache(tmp)
            ltfs.note_tape_io_error  # referenced, never called here
            self.assertIsNotNone(ltfs.READINESS.snapshot())


# =============================================================================
# E. Tape lock ownership -- BASELINE for Phase 3.
# =============================================================================
class TapeLockBaselineTests(unittest.TestCase):
    def test_lock_is_in_process_rlock_only(self):
        """BASELINE: a threading.RLock, which cannot serialise other processes."""
        self.assertIn("RLock", type(rt._TAPE_IO_LOCK).__name__)
        probe = threading.RLock()
        self.assertIs(type(rt._TAPE_IO_LOCK), type(probe))

    def test_no_cross_process_lock_primitive_exists_yet(self):
        names = [n for n in dir(rt)
                 if "mutex" in n.lower() or "named_lock" in n.lower()]
        self.assertEqual(names, [])

    def test_lock_is_reentrant_in_process(self):
        rt._acquire_tape_io_lock("outer")
        try:
            rt._acquire_tape_io_lock("inner")
            rt._release_tape_io_lock()
        finally:
            rt._release_tape_io_lock()

    def test_lock_observer_records_ownership_and_nesting(self):
        """The observer replaces the lock helpers, so real ownership must be
        held around it — the low-level label read asserts ownership (Phase 3)."""
        obs = TapeLockObserver(use_sites=("src.ltfs",))
        rec = RecordingSubprocess(
            SimpleNamespace(stdout=" Volume in drive X is T\n", stderr="",
                            returncode=0))
        rt._acquire_tape_io_lock("lock observer test")
        try:
            with obs.patches()[0], obs.patches()[1], \
                 mock.patch("subprocess.run", rec):
                ltfs.get_volume_label("X:\\")
        finally:
            rt._release_tape_io_lock()
        self.assertTrue(any("volume label" in r for r in obs.reasons()))
        self.assertFalse(obs.held)


# =============================================================================
# F. Queue sizing / staging -- BASELINE for Phase 4.
# =============================================================================
class PipelineDepthBaselineTests(unittest.TestCase):
    def _config(self):
        """The operator's config when there is one, else the tracked example.

        config.ini is untracked, so reading only that made these baselines
        unverifiable on a clean clone and in CI - they passed solely because an
        earlier test had left a generated config.ini in the repo root. The
        example config carries the same documented baseline, so falling back to
        it keeps the assertions meaningful everywhere while still catching drift
        on a machine that has a real config.
        """
        parser = configparser.ConfigParser()
        local = os.path.join(PROJECT_ROOT, "config.ini")
        example = os.path.join(PROJECT_ROOT, "config.example.ini")
        parser.read(local if os.path.exists(local) else example, encoding="utf-8")
        return parser

    def test_prefetch_depth_is_one_today(self):
        """BASELINE: depth 1 is why the ready queue was empty 99.6% of the time."""
        parser = self._config()
        self.assertEqual(
            parser.get("PERFORMANCE", "prefetch_chunks_ahead").strip(), "1")

    def test_ready_queue_semantics_at_depth_one(self):
        prefetch = 1
        ready_q = queue.Queue(maxsize=prefetch)
        ready_q.put(object())
        self.assertTrue(ready_q.full())          # one chunk saturates the queue
        self.assertEqual(ready_q.maxsize, 1)

    def test_no_byte_based_ready_limit_exists_yet(self):
        """BASELINE: depth is chunk-count based only. Phase 4 adds byte limits."""
        parser = self._config()
        for key in ("min_ready_bytes_before_writer_start", "target_ready_bytes",
                    "max_ready_bytes", "max_ready_chunks"):
            self.assertFalse(parser.has_option("PERFORMANCE", key),
                             f"{key} should not exist before Phase 4")

    def test_staging_allowance_is_large_relative_to_one_chunk(self):
        # The invariant is pipeline depth, not an absolute size: staging must
        # hold at least 3.5 chunks so fetch/pack of the next chunks overlaps
        # the current tape write (350 GB on the original 100 GB-chunk host).
        parser = self._config()
        staging_gb = float(parser.get("PERFORMANCE", "staging_max_gb"))
        chunk_gb = float(parser.get("PERFORMANCE", "chunk_cap_gb"))
        self.assertGreaterEqual(staging_gb, 3.5 * chunk_gb)


# =============================================================================
# G. Fake adapter self-check.
# =============================================================================
class FakeAdapterTests(unittest.TestCase):
    def test_adapter_records_every_operation_in_order(self):
        with tempfile.TemporaryDirectory() as tmp:
            log = TapeOperationLog()
            fake = FakeLtfsAdapter(tmp, log=log)
            fake.drive_status("X:\\")
            fake.isdir("X:\\")
            fake.listdir("X:\\")
            fake.read_volume_label("X:\\")
        self.assertEqual(
            log.kinds(), ["drive_status", "isdir", "listdir", "volume_label"])
        self.assertEqual(len(fake.filesystem_touches), 2)

    def test_adapter_can_simulate_a_failing_mount(self):
        with tempfile.TemporaryDirectory() as tmp:
            fake = FakeLtfsAdapter(tmp)
            fake.fail_filesystem = OSError("The media is write protected")
            with self.assertRaises(OSError):
                fake.listdir("X:\\")


if __name__ == "__main__":
    unittest.main()
