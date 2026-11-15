"""Audit (low): LTOBackup printed binary quantities (bytes / 1024**3,
bytes / 1024**2 per second) labelled as decimal GB / MB/s."""
import io
import os
import shutil
import tempfile
import unittest
from contextlib import redirect_stdout
from types import SimpleNamespace
from unittest import mock

from src.backup import LTOBackup

_OK = ("   Total    Copied   Skipped  Mismatch    FAILED    Extras\n"
       "Files : 1 1 0 0 0 0\n"
       "Bytes : 1024 1024 0 0 0 0\n"
       "Speed : 104857600 Bytes/sec.\n")


class _DB:
    def tape_exists(self, _label):
        return True

    def bulk_upsert_files(self, records, update_existing=True):
        list(records)
        return {"inserted": 1, "updated": 0, "skipped": 0}

    def recalculate_tape_used_space(self, _label):
        return 5 * 1024 ** 3


class UnitLabels(unittest.TestCase):
    def test_binary_values_are_labelled_binary(self):
        tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, tmp, True)
        source = os.path.join(tmp, "source")
        tape = os.path.join(tmp, "fake_ltfs_mount")
        os.makedirs(source)
        os.makedirs(tape)
        with open(os.path.join(source, "a.bin"), "wb") as fh:
            fh.write(b"x" * 1024)
        backup = LTOBackup(_DB(), "", governor=None, log_dir=tmp)
        backup.eject_tape = lambda _drive: None
        result = SimpleNamespace(stdout=_OK, stderr="", returncode=1)
        out = io.StringIO()
        with mock.patch("src.backup._ensure_lto_drive_ready",
                        return_value=True), \
                mock.patch("src.backup._run_robocopy_tuned",
                           return_value=result), redirect_stdout(out):
            backup.run(source, tape, "Tape_T")
        text = out.getvalue()
        self.assertIn("5.000 GiB", text)          # 5 * 1024**3 bytes
        self.assertIn("100.0 MiB/s", text)        # 104857600 B/s
        self.assertIn("GiB to copy", text)
        self.assertNotRegex(text, r"\d GB\b")
        self.assertNotIn("MB/s", text)


if __name__ == "__main__":
    unittest.main()
