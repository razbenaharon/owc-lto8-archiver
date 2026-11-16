"""An expected refusal prints one readable message, never a Python traceback.

A bad ``config.ini`` or a removed command-line flag is an answer for the
operator, not a crash. ``run.py`` logged such errors through a logger that had
no handler yet, and Python's last-resort handler then wrote the whole
traceback to the console ahead of the readable line.

Both runners are executed for real (``runpy``); the configuration is replaced
so that no file is read or created.
"""
import io
import logging
import os
import runpy
import sys
import tempfile
import unittest
from contextlib import ExitStack, redirect_stderr, redirect_stdout
from unittest import mock

from src import logsetup
from src.config import ConfigError
from src.exit_codes import ExitCode

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

BAD_CONFIG = ConfigError(
    "[CONFIG] config.ini has 1 invalid setting(s); refusing to start "
    "rather than guess:\n  [SETTINGS] zip_threshold_mb = 'abc'")


def _run(script, argv, config_error=BAD_CONFIG):
    """Run a root runner as __main__; return (exit code, stdout, stderr)."""
    out, err = io.StringIO(), io.StringIO()
    cwd, path = os.getcwd(), list(sys.path)
    code = 0
    with ExitStack() as stack:
        stack.enter_context(mock.patch(
            "src.config.ConfigManager", mock.Mock(side_effect=config_error)))
        stack.enter_context(mock.patch(
            "src.cli.main",
            mock.Mock(side_effect=AssertionError("unexpected menu"))))
        stack.enter_context(mock.patch.object(
            sys, "argv", [script] + list(argv)))
        stack.enter_context(mock.patch.dict(os.environ))
        os.environ.pop("LTO_DEBUG", None)
        stack.enter_context(redirect_stdout(out))
        stack.enter_context(redirect_stderr(err))
        try:
            runpy.run_path(os.path.join(REPO_ROOT, script),
                           run_name="__main__")
        except SystemExit as exc:
            code = exc.code
        finally:
            os.chdir(cwd)
            sys.path[:] = path
    return (0 if code is None else code), out.getvalue(), err.getvalue()


class _NoFileLog(unittest.TestCase):
    """Start each test with the app logger unconfigured, as a fresh process."""

    def setUp(self):
        logger = logsetup.get_logger()
        saved = list(logger.handlers)
        for handler in saved:
            logger.removeHandler(handler)
        self.addCleanup(lambda: [logger.addHandler(h) for h in saved])

    def assertReadable(self, text, expected):
        self.assertIn(expected, text)
        self.assertNotIn("Traceback", text)
        self.assertNotIn('File "', text)


class RunPyPrintsNoTraceback(_NoFileLog):
    def test_bad_config_on_backup_db(self):
        code, out, err = _run("run.py", ["--backup-db"])
        self.assertEqual(code, int(ExitCode.FATAL_CONFIG))
        self.assertReadable(out + err, "zip_threshold_mb")

    def test_bad_config_on_headless_remote_archive(self):
        code, out, err = _run("run.py", ["remote-archive", "--resume"])
        self.assertEqual(code, int(ExitCode.FATAL_CONFIG))
        self.assertReadable(out + err, "zip_threshold_mb")

    def test_bad_config_on_tape_reset(self):
        code, out, err = _run(
            "run.py", ["tape-reset", "--tape", "T", "--impact-report", "r"])
        self.assertEqual(code, int(ExitCode.FATAL_CONFIG))
        self.assertReadable(out + err, "zip_threshold_mb")

    def test_removed_flag(self):
        code, out, err = _run("run.py", ["--dry-run"])
        self.assertEqual(code, int(ExitCode.FATAL_CONFIG))
        self.assertReadable(out + err, "--dry-run")

    def test_traceback_still_reaches_a_configured_file_log(self):
        logger = logsetup.get_logger()
        with tempfile.TemporaryDirectory() as tmp:
            log_path = os.path.join(tmp, "archiver.log")
            handler = logging.FileHandler(log_path, encoding="utf-8")
            logger.addHandler(handler)
            try:
                code, out, err = _run("run.py", ["--dry-run"])
            finally:
                logger.removeHandler(handler)
                handler.close()
            with open(log_path, encoding="utf-8") as fh:
                logged = fh.read()
        self.assertEqual(code, int(ExitCode.FATAL_CONFIG))
        self.assertReadable(out + err, "--dry-run")
        self.assertIn("Traceback", logged)

    def test_lto_debug_still_raises_for_bug_reports(self):
        out, err = io.StringIO(), io.StringIO()
        cwd, path = os.getcwd(), list(sys.path)
        try:
            with mock.patch.dict(os.environ, {"LTO_DEBUG": "1"}), \
                    mock.patch.object(sys, "argv", ["run.py", "--dry-run"]), \
                    redirect_stdout(out), redirect_stderr(err):
                with self.assertRaises(RuntimeError):
                    runpy.run_path(os.path.join(REPO_ROOT, "run.py"),
                                   run_name="__main__")
        finally:
            os.chdir(cwd)
            sys.path[:] = path


class InspectDbPrintsNoTraceback(_NoFileLog):
    def test_bad_config(self):
        code, out, err = _run("inspect_db.py", ["--manifest-status"])
        self.assertEqual(code, 1)
        self.assertReadable(out + err, "zip_threshold_mb")

    def test_bad_config_when_opening_the_inspector(self):
        code, out, err = _run("inspect_db.py", [])
        self.assertEqual(code, 1)
        self.assertReadable(out + err, "zip_threshold_mb")


if __name__ == "__main__":
    unittest.main()
