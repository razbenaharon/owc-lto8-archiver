"""Audit H16: the main program must exit non-zero when it failed.

``run.py`` printed operator-facing errors and then fell off the end of the
script, so a failed database backup, a headless run that could not reach the
database, or a failed menu action all exited 0 — indistinguishable from
success for a scheduled task or the watchdog.

``run.py`` is executed for real (``runpy`` with ``__name__ == "__main__"``),
with only its collaborators replaced: no configuration file is read or created
and no database, tape or network is contacted.
"""
import io
import os
import runpy
import sys
import unittest
from contextlib import ExitStack, redirect_stdout
from types import SimpleNamespace
from unittest import mock

from src import cli
from src.exit_codes import ExitCode, StopResult, REASON_BAD_CONFIG

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RUN_PY = os.path.join(REPO_ROOT, "run.py")


def _fake_cfg():
    return SimpleNamespace(
        remote_host="h", remote_user="u", remote_path="/p",
        windows_update_guard=False, backup_log_dir=None,
        lto_drive="X:", ibm_eject_cmd="", staging_dir="s", restore_dir="r",
        local_manifest_archive_root=None)


class _FakeDB:
    def __init__(self):
        self.closed = False

    def acquire_archiver_lock(self):
        pass

    def release_archiver_lock(self):
        pass

    def close(self):
        self.closed = True


class RunPyExitCodes(unittest.TestCase):
    """Invoke the real entry point and read the process exit code."""

    def _run(self, argv, **replacements):
        """Execute run.py as __main__; return the exit code it would produce.

        ``replacements`` maps ``module.attribute`` (dots as ``__``) to the
        stand-in. Every collaborator that could reach a real resource has a
        safe default.
        """
        defaults = {
            "src.config.ConfigManager": mock.Mock(return_value=_fake_cfg()),
            "src.logsetup.configure_file_logging": mock.Mock(),
            "src.db.create_database_manager": mock.Mock(
                return_value=_FakeDB()),
            "src.pg_backup.create_database_backup": mock.Mock(
                side_effect=AssertionError("unexpected real DB backup")),
            "src.cli.main": mock.Mock(
                side_effect=AssertionError("unexpected interactive menu")),
            "src.cli.run_remote_archiver_headless": mock.Mock(
                side_effect=AssertionError("unexpected headless run")),
            "src.tape_reset.execute_reset": mock.Mock(
                side_effect=AssertionError("unexpected tape reset")),
        }
        defaults.update(replacements)
        cwd = os.getcwd()
        path = list(sys.path)
        code = 0
        with ExitStack() as stack:
            for target, stand_in in defaults.items():
                stack.enter_context(mock.patch(target, stand_in))
            stack.enter_context(mock.patch.object(
                sys, "argv", ["run.py"] + list(argv)))
            stack.enter_context(mock.patch.dict(os.environ))
            os.environ.pop("LTO_DEBUG", None)
            stack.enter_context(redirect_stdout(io.StringIO()))
            try:
                runpy.run_path(RUN_PY, run_name="__main__")
            except SystemExit as exc:
                code = exc.code
            finally:
                os.chdir(cwd)
                sys.path[:] = path
        return 0 if code is None else code

    def test_failed_database_backup_exits_nonzero(self):
        code = self._run(["--backup-db"], **{
            "src.pg_backup.create_database_backup": mock.Mock(
                side_effect=RuntimeError("[DB BACKUP] pg_dump failed"))})
        self.assertEqual(code, int(ExitCode.FATAL_CONFIG))

    def test_successful_database_backup_exits_zero(self):
        code = self._run(["--backup-db"], **{
            "src.pg_backup.create_database_backup": mock.Mock(
                return_value="db_backups/x.dump")})
        self.assertEqual(code, 0)

    def test_headless_run_without_database_exits_nonzero(self):
        code = self._run(["remote-archive", "--resume", "--non-interactive"],
                         **{"src.db.create_database_manager": mock.Mock(
                             side_effect=RuntimeError(
                                 "[DB] Could not connect to PostgreSQL"))})
        self.assertEqual(code, int(ExitCode.FATAL_CONFIG))

    def test_headless_run_propagates_the_pipeline_exit_code(self):
        code = self._run(["remote-archive", "--resume"], **{
            "src.cli.run_remote_archiver_headless": mock.Mock(
                return_value=int(ExitCode.SAFETY_BLOCK))})
        self.assertEqual(code, int(ExitCode.SAFETY_BLOCK))

    def test_menu_startup_failure_exits_nonzero(self):
        code = self._run([], **{"src.cli.main": mock.Mock(
            side_effect=RuntimeError("[DB] Could not connect to PostgreSQL"))})
        self.assertEqual(code, int(ExitCode.FATAL_CONFIG))

    def test_menu_exit_code_is_propagated(self):
        code = self._run([], **{"src.cli.main": mock.Mock(
            return_value=int(ExitCode.SAFETY_BLOCK))})
        self.assertEqual(code, int(ExitCode.SAFETY_BLOCK))

    def test_clean_menu_exit_is_zero(self):
        code = self._run([], **{"src.cli.main": mock.Mock(return_value=0)})
        self.assertEqual(code, 0)

    def test_keyboard_interrupt_exits_with_user_stop(self):
        code = self._run([], **{"src.cli.main": mock.Mock(
            side_effect=KeyboardInterrupt)})
        self.assertEqual(code, int(ExitCode.USER_STOP))

    def test_removed_flag_exits_nonzero(self):
        code = self._run(["--optimize-db"])
        self.assertEqual(code, int(ExitCode.FATAL_CONFIG))

    def test_failed_tape_reset_exits_nonzero(self):
        code = self._run(
            ["tape-reset", "--tape", "T", "--impact-report", "r.json"],
            **{"src.tape_reset.execute_reset": mock.Mock(
                side_effect=RuntimeError("reset refused"))})
        self.assertEqual(code, int(ExitCode.FATAL_CONFIG))


class MenuExitCodes(unittest.TestCase):
    """The interactive menu reports a failed action through its exit code."""

    def _main(self, inputs, **patches):
        db = _FakeDB()
        with ExitStack() as stack:
            stack.enter_context(mock.patch.object(
                cli, "ConfigManager", return_value=_fake_cfg()))
            stack.enter_context(mock.patch.object(
                cli, "create_database_manager", return_value=db))
            stack.enter_context(mock.patch.object(
                cli, "configure_file_logging"))
            stack.enter_context(mock.patch.object(cli, "TapeManager"))
            stack.enter_context(mock.patch.object(cli, "LTORetriever"))
            stack.enter_context(mock.patch.object(cli.os, "system"))
            for name, value in patches.items():
                stack.enter_context(mock.patch.object(cli, name, value))
            stack.enter_context(mock.patch("builtins.input",
                                           side_effect=inputs))
            stack.enter_context(redirect_stdout(io.StringIO()))
            try:
                code = cli.main()
            except SystemExit as exc:
                code = exc.code
        self.assertTrue(db.closed)
        return 0 if code is None else code

    def test_failed_backup_then_exit_is_nonzero(self):
        code = self._main(["9", "0"], create_database_backup=mock.Mock(
            side_effect=RuntimeError("[DB BACKUP] pg_dump failed")))
        self.assertEqual(code, int(ExitCode.FATAL_CONFIG))

    def test_failed_backup_then_closed_stdin_is_nonzero(self):
        code = self._main(["9", EOFError()], create_database_backup=mock.Mock(
            side_effect=RuntimeError("[DB BACKUP] pg_dump failed")))
        self.assertEqual(code, int(ExitCode.FATAL_CONFIG))

    def test_failed_remote_archive_then_exit_is_nonzero(self):
        result = StopResult(exit_code=ExitCode.FATAL_CONFIG,
                            reason=REASON_BAD_CONFIG)
        code = self._main(["6", "0"],
                          run_remote_archiver=mock.Mock(return_value=result))
        self.assertEqual(code, int(ExitCode.FATAL_CONFIG))

    def test_failed_local_archive_then_exit_is_nonzero(self):
        code = self._main(["1", "0"], run_archiver=mock.Mock(
            return_value=int(ExitCode.FATAL_CONFIG)))
        self.assertEqual(code, int(ExitCode.FATAL_CONFIG))

    def test_plain_exit_is_zero(self):
        self.assertEqual(self._main(["x", "0"]), 0)

    def test_successful_backup_then_exit_is_zero(self):
        code = self._main(["9", "0"], create_database_backup=mock.Mock(
            return_value="db_backups/x.dump"))
        self.assertEqual(code, 0)


class LocalArchiverReturnsACode(unittest.TestCase):
    def _run(self, orchestrator_run, lock_error=None):
        db = _FakeDB()
        if lock_error is not None:
            db.acquire_archiver_lock = mock.Mock(side_effect=lock_error)
        fake = mock.Mock()
        fake.run = orchestrator_run
        with ExitStack() as stack:
            for name in ("_prepare_robocopy_exclusion",
                         "_remove_robocopy_exclusion",
                         "install_cancel_handler", "uninstall_cancel_handler",
                         "reset_cancel", "_terminate_all_procs",
                         "unpin_current_process", "_cleanup_askpass_helpers"):
                stack.enter_context(mock.patch.object(
                    cli, name, mock.Mock(return_value=False)))
            stack.enter_context(mock.patch.object(
                cli, "LocalOrchestrator", return_value=fake))
            stack.enter_context(redirect_stdout(io.StringIO()))
            return cli.run_archiver(_fake_cfg(), db)

    def test_error_is_reported_as_failure(self):
        code = self._run(mock.Mock(side_effect=RuntimeError("tape is FULL")))
        self.assertEqual(code, int(ExitCode.FATAL_CONFIG))

    def test_lock_failure_is_reported_as_failure(self):
        code = self._run(mock.Mock(), lock_error=RuntimeError("lock held"))
        self.assertEqual(code, int(ExitCode.FATAL_CONFIG))

    def test_interrupt_is_a_user_stop(self):
        code = self._run(mock.Mock(side_effect=KeyboardInterrupt))
        self.assertEqual(code, int(ExitCode.USER_STOP))

    def test_clean_run_is_zero(self):
        self.assertEqual(self._run(mock.Mock(return_value=None)), 0)


if __name__ == "__main__":
    unittest.main()
