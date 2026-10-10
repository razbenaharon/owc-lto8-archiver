"""_robocopy_file must decide success from robocopy's own output, never from
its exit code alone (incident 009: exit 0 after RETRY LIMIT EXCEEDED, nothing
copied). Restore and packing both trust its bool result."""
import os
import shutil
import tempfile
import unittest
from unittest import mock

from src import robocopy as robocopy_mod
from src.robocopy import _robocopy_file
from src.runtime import CANCEL


def _summary(copied_files, copied_bytes, total_bytes=None, failed=0):
    total_bytes = copied_bytes if total_bytes is None else total_bytes
    return (
        "\n------------------------------------------------------------------------------\n\n"
        "               Total    Copied   Skipped  Mismatch    FAILED    Extras\n"
        "    Dirs :         1         0         1         0         0         0\n"
        f"   Files :         1         {copied_files}         0         0         {failed}         0\n"
        f"   Bytes :      {total_bytes}      {copied_bytes}         0         0         0         0\n"
        "   Times :   0:00:00   0:00:00                       0:00:00   0:00:00\n"
        "   Ended : Sunday, October 11, 2026 10:00:00 AM\n"
    )


class _FakeProc:
    """Stands in for the robocopy Popen: optionally lands bytes in the
    destination directory (robocopy's argv[2]) and returns canned output."""

    def __init__(self, cmd, landed_bytes, stdout, returncode):
        self.cmd = cmd
        self._landed_bytes = landed_bytes
        self._stdout = stdout
        self.returncode = returncode
        self.pid = 0

    def wait(self):
        if self._landed_bytes is not None:
            with open(os.path.join(self.cmd[2], self.cmd[3]), "wb") as fh:
                fh.write(self._landed_bytes)
            self._landed_bytes = None
        return self.returncode

    def communicate(self):
        self.wait()
        return self._stdout, ""


class RobocopyFileVerdictTests(unittest.TestCase):
    def setUp(self):
        CANCEL.clear()
        self.tmp = tempfile.mkdtemp(prefix="lto_robocopy_file_")
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.src_dir = os.path.join(self.tmp, "src")
        self.dst_dir = os.path.join(self.tmp, "dst")
        os.makedirs(self.src_dir)
        self.payload = b"x" * 1000
        self.src = os.path.join(self.src_dir, "data.bin")
        with open(self.src, "wb") as fh:
            fh.write(self.payload)
        self.dst = os.path.join(self.dst_dir, "data.bin")
        self.cmds = []

    def _run(self, landed_bytes, stdout, returncode):
        def _popen(cmd, **_kw):
            self.cmds.append(cmd)
            return _FakeProc(cmd, landed_bytes, stdout, returncode)
        with mock.patch.object(robocopy_mod.subprocess, "Popen", _popen), \
                mock.patch("builtins.print"):
            return _robocopy_file(self.src, self.dst)

    def test_complete_summary_and_full_file_is_success(self):
        self.assertTrue(self._run(self.payload, _summary(1, 1000), 1))
        with open(self.dst, "rb") as fh:
            self.assertEqual(fh.read(), self.payload)

    def test_exit_zero_after_retry_limit_exceeded_is_failure(self):
        # Incident 009 shape: nothing copied, no summary, exit code 0.
        out = ("2026/07/24 03:12:40 ERROR 1117 (0x0000045D) Copying File "
               f"{self.src}\nThe request could not be performed because of an "
               "I/O device error.\nERROR: RETRY LIMIT EXCEEDED.\n")
        self.assertFalse(self._run(None, out, 0))

    def test_missing_summary_is_failure_even_with_full_file(self):
        self.assertFalse(self._run(self.payload, "", 1))

    def test_failed_file_in_summary_is_failure(self):
        self.assertFalse(self._run(self.payload, _summary(0, 0, failed=1), 1))

    def test_truncated_landed_file_is_failure(self):
        self.assertFalse(self._run(self.payload[:500], _summary(1, 1000), 1))

    def test_cancel_is_failure_even_with_clean_output(self):
        CANCEL.set()
        self.addCleanup(CANCEL.clear)
        self.assertFalse(self._run(self.payload, _summary(1, 1000), 1))

    def test_command_keeps_the_job_summary(self):
        self._run(self.payload, _summary(1, 1000), 1)
        cmd = self.cmds[0]
        self.assertNotIn("/NJS", cmd)
        self.assertIn("/BYTES", cmd)


@unittest.skipUnless(os.name == "nt" and shutil.which("robocopy"),
                     "needs real robocopy (Windows)")
class RealRobocopyFileTests(unittest.TestCase):
    """Runs the real robocopy between two local temp directories (no tape),
    so the parser is checked against genuine output, not a hand-written one."""

    def setUp(self):
        CANCEL.clear()
        self.tmp = tempfile.mkdtemp(prefix="lto_robocopy_real_")
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)

    def test_real_copy_is_accepted(self):
        src_dir = os.path.join(self.tmp, "src")
        os.makedirs(src_dir)
        src = os.path.join(src_dir, "real.bin")
        payload = os.urandom(256 * 1024)
        with open(src, "wb") as fh:
            fh.write(payload)
        dst = os.path.join(self.tmp, "dst", "real.bin")

        self.assertTrue(_robocopy_file(src, dst))
        with open(dst, "rb") as fh:
            self.assertEqual(fh.read(), payload)


if __name__ == "__main__":
    unittest.main()
