"""Audit H1: a console Ctrl+C must not kill the tape-write robocopy mid-write.

The app promises that an active tape write finishes and commits after a
cooperative Ctrl+C. Windows, however, delivers CTRL_C_EVENT to *every* process
attached to the console — the robocopy child included — so the child died
mid-write regardless of what the Python handler decided. The same happened with
``scripts/graceful_stop.py``, which raises CTRL_C on the target's console.

The behavioural test runs the real ``_run_robocopy_tuned`` with the real
robocopy between two TEMP directories (never a tape), inside its own hidden
console, and raises a real CTRL_C_EVENT on that console mid-copy.
"""
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
import textwrap
import unittest
from unittest import mock

from src import robocopy as robocopy_mod

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

_HELPER = textwrap.dedent("""
    import ctypes, json, signal, sys, threading, time
    repo, src, dst, out = sys.argv[1:5]
    sys.path.insert(0, repo)
    from src.robocopy import _run_robocopy_tuned, _parse_robocopy_summary

    # "Ignore Ctrl+C" is an inherited process attribute; a test runner started
    # in its own process group would hand it down and hide the bug. Restore
    # normal Ctrl+C processing so this console behaves like an operator's.
    ctypes.windll.kernel32.SetConsoleCtrlHandler(None, False)
    seen = []
    signal.signal(signal.SIGINT, lambda *a: seen.append(1))

    def fire():
        time.sleep(1.0)
        # Group 0 = every process attached to THIS console, exactly what a
        # keyboard Ctrl+C (and scripts/graceful_stop.py) does.
        ctypes.windll.kernel32.GenerateConsoleCtrlEvent(0, 0)

    threading.Thread(target=fire, daemon=True).start()
    started = time.time()
    # /IPG throttles the copy so it is still in flight when CTRL_C arrives.
    rc = _run_robocopy_tuned(
        ['robocopy', src, dst, '/E', '/IPG:300', '/R:0', '/W:0', '/NP',
         '/BYTES'])
    duration = time.time() - started
    time.sleep(0.3)
    summary = _parse_robocopy_summary(rc.stdout)
    with open(out, 'w') as fh:
        json.dump({'returncode': rc.returncode,
                   'summary_found': summary['summary_found'],
                   'files_copied': summary['files_copied'],
                   'bytes_copied': summary['bytes_copied'],
                   'duration': duration,
                   'python_saw_sigint': bool(seen)}, fh)
""")


@unittest.skipUnless(os.name == 'nt' and shutil.which('robocopy'),
                     "needs Windows robocopy")
class TapeWriteSurvivesConsoleCtrlC(unittest.TestCase):
    def test_ctrl_c_on_the_console_does_not_kill_the_tape_write(self):
        size = 16 * 1024 * 1024
        with tempfile.TemporaryDirectory() as tmp:
            src = os.path.join(tmp, 'src')
            dst = os.path.join(tmp, 'dst')
            os.makedirs(src)
            os.makedirs(dst)
            with open(os.path.join(src, 'payload.bin'), 'wb') as fh:
                fh.write(os.urandom(size))
            helper = os.path.join(tmp, 'helper.py')
            with open(helper, 'w') as fh:
                fh.write(_HELPER)
            out = os.path.join(tmp, 'out.json')

            # Own, hidden console: the CTRL_C stays inside it and can never
            # reach the pytest process or the developer's terminal.
            startup = subprocess.STARTUPINFO()
            startup.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            startup.wShowWindow = 0
            proc = subprocess.Popen(
                [sys.executable, helper, REPO_ROOT, src, dst, out],
                creationflags=subprocess.CREATE_NEW_CONSOLE,
                startupinfo=startup)
            proc.wait(timeout=180)
            self.assertTrue(os.path.exists(out),
                            "helper died without reporting (exit "
                            f"{proc.returncode})")
            with open(out) as fh:
                result = json.load(fh)

            # The event really was delivered to the console...
            self.assertTrue(result['python_saw_sigint'], result)
            # ...while the copy was still running...
            self.assertGreater(result['duration'], 1.0, result)
            # ...and robocopy nevertheless ran to completion.
            self.assertTrue(result['summary_found'], result)
            self.assertLess(result['returncode'], 8, result)
            self.assertEqual(result['files_copied'], 1, result)
            self.assertEqual(result['bytes_copied'], size, result)
            self.assertEqual(
                os.path.getsize(os.path.join(dst, 'payload.bin')), size)


class TapeWriteChildIsIsolated(unittest.TestCase):
    """Popen-argument level: the tape write is isolated, a restore is not."""

    class _Proc:
        returncode = 1
        pid = 0

        def __init__(self):
            import io
            self.stdout = io.StringIO("")
            self.stderr = io.StringIO("")

        def wait(self):
            return self.returncode

        def communicate(self):
            return "", ""

    @unittest.skipUnless(os.name == 'nt', "Windows process-group flag")
    def test_tape_write_popen_uses_a_new_process_group(self):
        with mock.patch.object(robocopy_mod.subprocess, 'Popen',
                               return_value=self._Proc()) as popen, \
                mock.patch.object(robocopy_mod, '_apply_proc_tuning'):
            robocopy_mod._run_robocopy_tuned(['robocopy', 'a', 'b'])
        flags = popen.call_args.kwargs.get('creationflags', 0)
        self.assertTrue(flags & subprocess.CREATE_NEW_PROCESS_GROUP)


def _load_graceful_stop():
    path = os.path.join(REPO_ROOT, 'scripts', 'graceful_stop.py')
    spec = importlib.util.spec_from_file_location('graceful_stop_under_test',
                                                  path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _tasklist(stdout, returncode=0):
    return mock.Mock(return_value=subprocess.CompletedProcess(
        ['tasklist'], returncode, stdout, ''))


class GracefulStopRefusesDuringATapeWrite(unittest.TestCase):
    def setUp(self):
        self.mod = _load_graceful_stop()
        self.sent = []
        patcher = mock.patch.object(
            self.mod, '_send_ctrl_c',
            side_effect=lambda pid: self.sent.append(pid) or 0)
        patcher.start()
        self.addCleanup(patcher.stop)

    def _run(self, argv, tasklist):
        with mock.patch.object(self.mod.subprocess, 'run', tasklist):
            return self.mod.main(argv)

    def test_refuses_while_robocopy_is_running(self):
        # tasklist reports the image with a capital R (AGENTS.md).
        listing = '"python.exe","10","Console","1","9,000 K"\n' \
                  '"Robocopy.exe","4242","Console","1","12,000 K"\n'
        code = self._run(['1234'], _tasklist(listing))
        self.assertNotEqual(code, 0)
        self.assertEqual(self.sent, [])

    def test_sends_when_no_robocopy_is_running(self):
        listing = '"python.exe","10","Console","1","9,000 K"\n'
        code = self._run(['1234'], _tasklist(listing))
        self.assertEqual(code, 0)
        self.assertEqual(self.sent, [1234])

    def test_refuses_when_the_process_list_cannot_be_read(self):
        code = self._run(['1234'], _tasklist('', returncode=1))
        self.assertNotEqual(code, 0)
        self.assertEqual(self.sent, [])
        boom = mock.Mock(side_effect=OSError("tasklist missing"))
        code = self._run(['1234'], boom)
        self.assertNotEqual(code, 0)
        self.assertEqual(self.sent, [])

    def test_explicit_flag_is_required_to_signal_during_a_write(self):
        listing = '"Robocopy.exe","4242","Console","1","12,000 K"\n'
        code = self._run(['1234', '--allow-active-tape-write'],
                         _tasklist(listing))
        self.assertEqual(code, 0)
        self.assertEqual(self.sent, [1234])

    def test_bad_arguments_do_not_signal(self):
        code = self._run([], _tasklist(''))
        self.assertNotEqual(code, 0)
        code = self._run(['not-a-pid'], _tasklist(''))
        self.assertNotEqual(code, 0)
        self.assertEqual(self.sent, [])


if __name__ == '__main__':
    unittest.main()
