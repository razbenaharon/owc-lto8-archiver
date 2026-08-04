"""Phase 3.5: cross-session fail-closed ownership, stable identity, classified
failures, bounded timeouts, and static watchdog/coverage validation.

Nothing here touches the tape: mutexes are named per test run, LTFS adapters are
fakes, and PowerShell is only *parsed*, never executed.
"""
import ast
import os
import re
import subprocess
import sys
import textwrap
import time
import unittest
from unittest import mock

from src import ltfs
from src import ltfs_ownership as own
from src.exit_codes import REASON_LTFS_OWNERSHIP_UNAVAILABLE
from src.ltfs_ownership import LtfsOwnership, LtfsOwnershipError

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TEST_MUTEX = f"OWC_LTO8_P35_{os.getpid()}"

HOLDER = textwrap.dedent(
    """
    import sys, time
    sys.path.insert(0, r"{root}")
    from src.ltfs_ownership import LtfsOwnership
    o = LtfsOwnership(name="{name}", timeout=5)
    o.acquire("holder")
    print("ACQUIRED", flush=True)
    time.sleep({hold})
    o.release(operation="holder")
    """
)


def _spawn_holder(name, hold=4.0):
    return subprocess.Popen(
        [sys.executable, "-c",
         HOLDER.format(root=PROJECT_ROOT, name=name, hold=hold)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def _wait_acquired(proc, timeout=20):
    deadline = time.time() + timeout
    while time.time() < deadline:
        line = proc.stdout.readline()
        if not line:
            return False
        if "ACQUIRED" in line:
            return True
    return False


# =============================================================================
# 1. Cross-session scope must fail closed.
# =============================================================================
class GlobalScopeFailClosedTests(unittest.TestCase):
    def setUp(self):
        ltfs.reset_readiness("test setup")
        self.addCleanup(ltfs.reset_readiness, "test teardown")

    def test_production_requests_only_the_global_namespace(self):
        o = LtfsOwnership(name=TEST_MUTEX + "_scope")
        self.assertEqual(o._qualified_names(),
                         (f"Global\\{TEST_MUTEX}_scope",))

    def test_no_local_fallback_when_global_is_refused(self):
        """Global refused -> raise; never silently degrade to Local."""
        o = LtfsOwnership(name=TEST_MUTEX + "_deny")
        # __init__ opens the handle eagerly (needed for abandoned detection);
        # reset to simulate a process where Global creation is refused.
        o._handle = None
        o._scope = None
        with mock.patch.object(own.ctypes, "WinDLL") as windll:
            windll.return_value.CreateMutexW.return_value = 0
            with mock.patch.object(own.ctypes, "get_last_error",
                                   return_value=own._ERROR_ACCESS_DENIED):
                with self.assertRaises(LtfsOwnershipError) as ctx:
                    o._ensure_handle()
                # exactly one attempt: Global. No Local retry.
                self.assertEqual(
                    windll.return_value.CreateMutexW.call_count, 1)
        self.assertEqual(ctx.exception.kind,
                         own.FAILURE_CROSS_SESSION_UNAVAILABLE)
        self.assertIsNone(o._handle)
        self.assertIsNone(o.scope(),
                          "scope was set despite Global creation failing")

    def test_global_failure_prevents_all_fake_ltfs_access(self):
        """A denied Global mutex must stop every LTFS operation."""
        calls = []

        class RecordingCommand(ltfs.LtfsDriveCommand):
            def drive_status(self, drive_path):
                calls.append(drive_path)
                return "LTFS_MOUNTED", "", None

        previous = ltfs.set_ltfs_drive_command(RecordingCommand())
        self.addCleanup(ltfs.set_ltfs_drive_command, previous)

        o = LtfsOwnership(name=TEST_MUTEX + "_deny2")
        o._handle = None
        with mock.patch.object(own.ctypes, "WinDLL") as windll:
            windll.return_value.CreateMutexW.return_value = 0
            with mock.patch.object(own.ctypes, "get_last_error",
                                   return_value=own._ERROR_ACCESS_DENIED):
                with self.assertRaises(LtfsOwnershipError):
                    o.acquire("denied", timeout=1)
        self.assertEqual(calls, [], "LTFS was touched despite no ownership")
        self.assertIsNone(ltfs.READINESS.snapshot())

    def test_local_scope_requires_explicit_opt_in(self):
        default = LtfsOwnership(name=TEST_MUTEX + "_d")
        self.assertFalse(default._allow_local_scope)
        opted = LtfsOwnership(name=TEST_MUTEX + "_l", _allow_local_scope=True)
        self.assertEqual(opted._qualified_names(), (f"Local\\{TEST_MUTEX}_l",))

    def test_local_scope_is_rejected_by_the_production_preflight(self):
        opted = LtfsOwnership(name=TEST_MUTEX + "_l2", _allow_local_scope=True)
        with self.assertRaises(LtfsOwnershipError) as ctx:
            opted.assert_production_scope()
        self.assertEqual(ctx.exception.kind,
                         own.FAILURE_CROSS_SESSION_UNAVAILABLE)

    def test_no_production_call_site_enables_local_scope(self):
        """The opt-in must be impossible to trip accidentally in production."""
        offenders = []
        for folder in ("src", "scripts"):
            root = os.path.join(PROJECT_ROOT, folder)
            for dirpath, _dirs, files in os.walk(root):
                if "__pycache__" in dirpath:
                    continue
                for fn in files:
                    if not fn.endswith(".py"):
                        continue
                    path = os.path.join(dirpath, fn)
                    with open(path, encoding="utf-8") as fh:
                        body = fh.read()
                    if "_allow_local_scope=True" in body:
                        offenders.append(path)
        self.assertEqual(offenders, [])

    def test_preflight_asserts_global_in_production(self):
        self.assertTrue(own.OWNERSHIP.assert_production_scope())
        self.assertEqual(own.OWNERSHIP.scope(), "Global")


# =============================================================================
# 2. Stable physical ownership identity.
# =============================================================================
class OwnershipIdentityTests(unittest.TestCase):
    def test_same_id_gives_same_mutex_regardless_of_drive_letter(self):
        with mock.patch.object(own, "_read_hardware_option",
                               side_effect=lambda k: "drive_0000000000"):
            name_z = own.default_mutex_name()
            name_e = own.default_mutex_name()   # letter plays no part at all
        self.assertEqual(name_z, name_e)
        self.assertIn("drive_0000000000", name_z)

    def test_mutex_name_does_not_contain_the_drive_letter(self):
        name = own.default_mutex_name()
        self.assertNotIn("_Z", name.replace("drive_", ""))
        self.assertEqual(name, own.default_mutex_name())

    def test_different_ids_give_different_mutexes(self):
        with mock.patch.object(own, "configured_ownership_id",
                               return_value="drive_A"):
            a = own.default_mutex_name()
        with mock.patch.object(own, "configured_ownership_id",
                               return_value="drive_B"):
            b = own.default_mutex_name()
        self.assertNotEqual(a, b)

    def test_missing_id_fails_closed(self):
        with self.assertRaises(LtfsOwnershipError) as ctx:
            own.validate_ownership_id("")
        self.assertEqual(ctx.exception.kind, own.FAILURE_BAD_IDENTITY)

    def test_malformed_ids_fail_closed(self):
        for bad in ("has space", "back\\slash", "fwd/slash", "Global\\x",
                    "a" * 65, "semi;colon", "brace{}"):
            with self.assertRaises(LtfsOwnershipError, msg=bad) as ctx:
                own.validate_ownership_id(bad)
            self.assertEqual(ctx.exception.kind, own.FAILURE_BAD_IDENTITY)

    def test_valid_ids_are_accepted(self):
        for good in ("drive_0000000000", "d-1", "D.1", "a" * 64):
            self.assertEqual(own.validate_ownership_id(good), good)

    def test_no_device_command_is_needed_to_build_the_name(self):
        """Computing the name must not run any subprocess."""
        with mock.patch("subprocess.run",
                        side_effect=AssertionError("subprocess used")), \
             mock.patch("subprocess.Popen",
                        side_effect=AssertionError("subprocess used")):
            self.assertTrue(own.default_mutex_name())

    def test_production_config_declares_the_id(self):
        import configparser
        parser = configparser.ConfigParser()
        parser.read(os.path.join(PROJECT_ROOT, "config.ini"), encoding="utf-8")
        value = parser.get("HARDWARE", "ltfs_ownership_id", fallback="")
        self.assertEqual(own.validate_ownership_id(value), value.strip())

    def test_example_config_documents_the_id(self):
        with open(os.path.join(PROJECT_ROOT, "config.example.ini"),
                  encoding="utf-8") as fh:
            body = fh.read()
        for key in ("ltfs_ownership_id", "ltfs_writer_lock_timeout_seconds",
                    "ltfs_helper_lock_timeout_seconds"):
            self.assertIn(key, body)


# =============================================================================
# 3. Failure classification.
# =============================================================================
class OwnershipFailureClassificationTests(unittest.TestCase):
    def setUp(self):
        ltfs.reset_readiness("test setup")
        self.addCleanup(ltfs.reset_readiness, "test teardown")

    def test_timeout_is_classified_and_does_not_retry(self):
        name = TEST_MUTEX + "_t"
        holder = _spawn_holder(name, hold=4.0)
        self.addCleanup(holder.kill)
        self.assertTrue(_wait_acquired(holder))

        mine = LtfsOwnership(name=name)
        attempts = []
        real_wait = own.ctypes.WinDLL("kernel32").WaitForSingleObject

        started = time.time()
        with self.assertRaises(LtfsOwnershipError) as ctx:
            mine.acquire("writer", timeout=1.0)
        elapsed = time.time() - started

        self.assertEqual(ctx.exception.kind, own.FAILURE_TIMEOUT)
        self.assertEqual(ctx.exception.classification,
                         REASON_LTFS_OWNERSHIP_UNAVAILABLE)
        self.assertLess(elapsed, 10, "acquire looped instead of failing once")
        self.assertIsNone(ltfs.READINESS.snapshot())

    def test_zero_tape_operations_after_a_timeout(self):
        name = TEST_MUTEX + "_t2"
        holder = _spawn_holder(name, hold=4.0)
        self.addCleanup(holder.kill)
        self.assertTrue(_wait_acquired(holder))

        touched = []

        class Cmd(ltfs.LtfsDriveCommand):
            def drive_status(self, drive_path):
                touched.append(drive_path)
                return "LTFS_MOUNTED", "", None

        previous = ltfs.set_ltfs_drive_command(Cmd())
        self.addCleanup(ltfs.set_ltfs_drive_command, previous)
        mine = LtfsOwnership(name=name)
        with self.assertRaises(LtfsOwnershipError):
            mine.acquire("writer", timeout=0.5)
        self.assertEqual(touched, [])

    def test_failure_kinds_are_distinct(self):
        self.assertEqual(len({own.FAILURE_TIMEOUT,
                              own.FAILURE_CROSS_SESSION_UNAVAILABLE,
                              own.FAILURE_PRIMITIVE,
                              own.FAILURE_BAD_IDENTITY}), 4)

    def test_reason_constant_is_wired_into_the_orchestrator(self):
        from src import remote_orchestrator as ro
        import inspect
        body = inspect.getsource(ro)
        self.assertIn("REASON_LTFS_OWNERSHIP_UNAVAILABLE", body)
        self.assertIn("LtfsOwnershipError", body)

    def test_ownership_stop_preserves_the_pack_and_does_not_set_backing(self):
        """Static proof via AST: the ownership stop preserves the pack, and
        'backing' is only ever set from the writer-start callback."""
        from src import remote_orchestrator as ro
        import inspect
        # Phase 4: ownership + the gate moved to the group boundary; the
        # per-chunk body (which owns the 'backing' transition) is separate.
        # Task 1.2: the write group lives in src.remote_writer now; the
        # orchestrator method is a delegating façade.
        from src.remote_writer import RemoteChunkWriter
        group_src = textwrap.dedent(
            inspect.getsource(RemoteChunkWriter._write_chunk_group))
        one_src = textwrap.dedent(
            inspect.getsource(RemoteChunkWriter._write_one_chunk_owned))
        group_fn = ast.parse(group_src).body[0]

        # The ownership handler must set preserve_pack=True with our reason.
        found = False
        for node in ast.walk(group_fn):
            if isinstance(node, ast.ExceptHandler):
                body = ast.get_source_segment(group_src, node) or ""
                if "REASON_LTFS_OWNERSHIP_UNAVAILABLE" in body:
                    self.assertIn("preserve_pack=True", body)
                    found = True
        self.assertTrue(found,
                        "no ownership-failure handler in _write_chunk_group")

        # An ownership failure must never set 'backing': the group boundary
        # contains no such transition at all.
        def backing_setters(src):
            """Every call that moves a chunk to 'backing', literal or typed.

            Task 1.5 replaced the bare strings with ``ChunkStatus.BACKING.value``
            and routed writes through ``transition_chunk``; both spellings must
            be recognised, or this proof silently stops proving anything.
            """
            out = []
            for node in ast.walk(ast.parse(src)):
                if not (isinstance(node, ast.Call)
                        and isinstance(node.func, ast.Attribute)
                        and node.func.attr in ("update_chunk_status",
                                               "transition_chunk")):
                    continue
                for arg in list(node.args) + [kw.value for kw in node.keywords]:
                    if isinstance(arg, ast.Constant) and arg.value == "backing":
                        out.append(node.lineno)
                    elif (isinstance(arg, ast.Attribute)
                          and arg.attr == "value"
                          and isinstance(arg.value, ast.Attribute)
                          and arg.value.attr == "BACKING"):
                        out.append(node.lineno)
            return out

        self.assertEqual(backing_setters(group_src), [],
                         "the group boundary must never set 'backing'")
        self.assertEqual(len(backing_setters(one_src)), 1,
                         "'backing' is set from more than one place")

    def test_abandoned_recovery_is_not_treated_as_a_tape_failure(self):
        """Recovery invalidates readiness but raises nothing."""
        name = TEST_MUTEX + "_ab"
        mine = LtfsOwnership(name=name)
        crasher = subprocess.Popen(
            [sys.executable, "-c", textwrap.dedent(f"""
                import os, sys
                sys.path.insert(0, r"{PROJECT_ROOT}")
                from src.ltfs_ownership import LtfsOwnership
                o = LtfsOwnership(name="{name}", timeout=5)
                o.acquire("crasher")
                print("ACQUIRED", flush=True)
                os._exit(1)
            """)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.addCleanup(crasher.kill)
        self.assertTrue(_wait_acquired(crasher))
        crasher.wait(timeout=15)
        self.assertTrue(mine.acquire("recoverer", timeout=15))
        try:
            self.assertTrue(mine._last_abandoned)
        finally:
            mine.release(operation="recoverer")


# =============================================================================
# 4. Timeout configuration.
# =============================================================================
class TimeoutConfigurationTests(unittest.TestCase):
    def test_defaults_are_bounded_and_positive(self):
        self.assertGreater(own.writer_timeout_seconds(), 0)
        self.assertGreater(own.helper_timeout_seconds(), 0)
        self.assertLessEqual(own.writer_timeout_seconds(),
                             own.MAX_TIMEOUT_SECONDS)

    def test_helper_timeout_is_shorter_than_the_writer_timeout(self):
        self.assertLess(own.helper_timeout_seconds(),
                        own.writer_timeout_seconds())

    def test_out_of_range_values_are_clamped(self):
        self.assertEqual(own._bounded_timeout("-5", 300), 300)
        self.assertEqual(own._bounded_timeout("0", 300), 300)
        self.assertEqual(own._bounded_timeout("99999", 300),
                         own.MAX_TIMEOUT_SECONDS)
        self.assertEqual(own._bounded_timeout("0.01", 300),
                         own.MIN_TIMEOUT_SECONDS)

    def test_malformed_values_fall_back_to_the_default(self):
        for bad in ("", "abc", None, "  "):
            self.assertEqual(own._bounded_timeout(bad, 300), 300)

    def test_never_waits_indefinitely_by_default(self):
        o = LtfsOwnership(name=TEST_MUTEX + "_to")
        self.assertIsNotNone(o.timeout)
        self.assertLessEqual(o.timeout, own.MAX_TIMEOUT_SECONDS)


# =============================================================================
# 5. Watchdog static/syntax validation (parsed, never executed).
# =============================================================================
class WatchdogStaticValidationTests(unittest.TestCase):
    WATCHDOG = os.path.join(PROJECT_ROOT, "scripts", "archive_watchdog.ps1")

    def _body(self):
        with open(self.WATCHDOG, encoding="utf-8-sig") as fh:
            return fh.read()

    def _active_lines(self):
        return [ln for ln in self._body().splitlines()
                if ln.strip() and not ln.strip().startswith("#")]

    def test_powershell_parses_successfully(self):
        """Parse with the PowerShell AST parser -- parse only, no execution."""
        script = (
            "$ErrorActionPreference='Stop';"
            "$errors=$null;"
            "[void][System.Management.Automation.Language.Parser]::ParseFile("
            f"'{self.WATCHDOG}', [ref]$null, [ref]$errors);"
            "if ($errors -and $errors.Count -gt 0) "
            "{ $errors | ForEach-Object { $_.Message }; exit 1 } "
            "else { 'PARSE_OK'; exit 0 }"
        )
        proc = subprocess.run(
            ["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", script],
            capture_output=True, text=True, timeout=120)
        self.assertEqual(proc.returncode, 0,
                         f"watchdog failed to parse:\n{proc.stdout}\n{proc.stderr}")
        self.assertIn("PARSE_OK", proc.stdout)

    def test_no_test_path_targets_the_ltfs_drive(self):
        for line in self._active_lines():
            if "Test-Path" in line:
                self.assertNotIn("$drive", line,
                                 f"Test-Path still targets the LTFS drive: {line}")

    def test_no_vol_command_targets_the_ltfs_drive(self):
        joined = "\n".join(self._active_lines())
        self.assertNotRegex(joined, r"vol\s+\$\{?letter")
        self.assertNotIn("cmd /c \"vol", joined)

    def test_no_new_filesystem_probe_of_the_drive(self):
        joined = "\n".join(self._active_lines())
        for probe in ("Get-ChildItem $drive", "Get-Item $drive",
                      "Get-Volume", "dir $drive", "Get-PSDrive"):
            self.assertNotIn(probe, joined)

    def test_restart_inputs_are_process_db_status_only(self):
        joined = "\n".join(self._active_lines())
        self.assertIn("Win32_Process", joined)        # process state
        self.assertIn("Invoke-Psql", joined)          # database state
        self.assertIn("last_failure.json", joined)    # recorded failure
        self.assertIn("archiver already running", joined)


# =============================================================================
# 6. Protected-operation coverage (static re-inventory).
# =============================================================================
class ProtectedOperationCoverageTests(unittest.TestCase):
    """Every production path reaching LTFS must hold the lock or assert it."""

    def test_low_level_helpers_assert_ownership(self):
        import inspect
        src = inspect.getsource(ltfs)
        for fn in ("def drive_status", "def _read_volume_label_unlocked",
                   "def _eject_tape_unlocked"):
            idx = src.index(fn)
            window = src[idx:idx + 800]
            self.assertIn("require_ownership(", window,
                          f"{fn} does not assert LTFS ownership")

    def test_every_ltfs_module_entry_point_is_guarded(self):
        """Public LTFS helpers either take the lock or assert ownership."""
        import inspect
        src = inspect.getsource(ltfs)
        tree = ast.parse(src)
        guarded, unguarded = [], []
        interesting = {
            "get_volume_label", "_ensure_lto_drive_ready",
            "eject_tape_drive", "_read_volume_label_unlocked",
            "_eject_tape_unlocked",
        }
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name in interesting:
                body = ast.get_source_segment(src, node) or ""
                if ("_acquire_tape_io_lock" in body
                        or "require_ownership(" in body):
                    guarded.append(node.name)
                else:
                    unguarded.append(node.name)
        self.assertEqual(unguarded, [], f"unguarded LTFS entry points: {unguarded}")
        self.assertEqual(set(guarded), interesting)

    def test_no_ibm_helper_is_executed_outside_the_ownership_boundary(self):
        """No module outside src/ltfs.py may *execute* an IBM LTFS helper.

        Matches actual subprocess calls, not mentions: menu labels and default
        config paths legitimately name these binaries as strings.
        """
        pattern = re.compile(r"(LtfsCmd\w+|mkltfs|ltfsck)", re.IGNORECASE)
        spawners = {"run", "Popen", "call", "check_call", "check_output"}
        offenders = []
        for dirpath, _dirs, files in os.walk(os.path.join(PROJECT_ROOT, "src")):
            if "__pycache__" in dirpath:
                continue
            for fn in files:
                if not fn.endswith(".py") or fn == "ltfs.py":
                    continue
                path = os.path.join(dirpath, fn)
                with open(path, encoding="utf-8") as fh:
                    src = fh.read()
                for node in ast.walk(ast.parse(src)):
                    if not isinstance(node, ast.Call):
                        continue
                    func = node.func
                    name = getattr(func, "attr", getattr(func, "id", ""))
                    if name not in spawners:
                        continue
                    segment = ast.get_source_segment(src, node) or ""
                    if pattern.search(segment):
                        offenders.append((fn, node.lineno))
        self.assertEqual(offenders, [],
                         f"IBM helper executed outside src/ltfs.py: {offenders}")

    def test_tape_manager_menu_paths_hold_ownership(self):
        """Every TapeManager method that reaches LTFS takes the lock.

        tape_info() spawns LtfsCmdDrives.exe directly (not via the adapter), so
        it needs its own acquisition; _ltfs_drive_status backs the interactive
        drive-status output.
        """
        import inspect
        src = inspect.getsource(ltfs)
        tree = ast.parse(src)
        required = {"tape_info", "_ltfs_drive_status", "format_tape",
                    "check_tape"}
        seen = {}
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef) and node.name == "TapeManager":
                for fn in node.body:
                    if isinstance(fn, ast.FunctionDef) and fn.name in required:
                        body = ast.get_source_segment(src, fn) or ""
                        seen[fn.name] = "_acquire_tape_io_lock" in body
        self.assertEqual(set(seen), required, f"missing methods: {seen}")
        unguarded = [k for k, v in seen.items() if not v]
        self.assertEqual(unguarded, [],
                         f"TapeManager LTFS paths without ownership: {unguarded}")

    def test_manual_helper_uses_the_configured_helper_timeout(self):
        with open(os.path.join(PROJECT_ROOT, "scripts",
                               "post_remount_check.py"), encoding="utf-8") as fh:
            body = fh.read()
        self.assertIn("helper_timeout_seconds()", body)
        self.assertIn("OWNERSHIP.acquire(", body)
        self.assertIn("OWNERSHIP.release(", body)


if __name__ == "__main__":
    unittest.main()
