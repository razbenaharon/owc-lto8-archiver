"""_count_robocopy_source_files must equal the number of files robocopy itself
enumerates, or the classifier's partial-copy check would reject good writes."""
import os
import shutil
import subprocess
import tempfile
import unittest

from src.backup import _count_robocopy_source_files
from src.robocopy import _parse_robocopy_summary


def _touch(path, size=10):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as fh:
        fh.write(b"x" * size)


class CountRobocopySourceFilesTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="lto_src_count_")
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.src = os.path.join(self.tmp, "src")
        for rel in ("a.bin", "b.bin", os.path.join("sub", "c.bin"),
                    os.path.join("xdir", "x1.bin"),
                    os.path.join("xdir", "deep", "x2.bin"),
                    os.path.join("sub", "named", "n.bin")):
            _touch(os.path.join(self.src, rel))
        self.xdir = os.path.join(self.src, "xdir")

    def test_counts_every_file_without_excludes(self):
        self.assertEqual(_count_robocopy_source_files(self.src), 6)
        self.assertEqual(_count_robocopy_source_files(self.src, []), 6)

    def test_full_path_xd_prunes_its_whole_subtree(self):
        self.assertEqual(
            _count_robocopy_source_files(self.src, [self.xdir]), 4)

    def test_bare_name_xd_prunes_any_directory_of_that_name(self):
        self.assertEqual(
            _count_robocopy_source_files(self.src, ["named"]), 5)

    @unittest.skipUnless(os.name == "nt" and shutil.which("robocopy"),
                         "needs real robocopy (Windows)")
    def test_matches_real_robocopy_enumeration(self):
        # /L lists only: nothing is copied anywhere. /XF of one file must stay
        # in the count (robocopy reports it as skipped); /XD drops a subtree.
        dst = os.path.join(self.tmp, "dst")
        xf = os.path.join(self.src, "a.bin")
        cmd = ["robocopy", self.src, dst, "/E", "/L", "/R:0", "/W:0",
               "/NP", "/NDL", "/NFL", "/BYTES",
               "/XF", xf, "/XD", self.xdir, "named"]
        out = subprocess.run(cmd, capture_output=True, text=True,
                             encoding="utf-8", errors="replace").stdout
        summary = _parse_robocopy_summary(out)
        self.assertTrue(summary["summary_found"], out)
        self.assertEqual(
            _count_robocopy_source_files(self.src, [self.xdir, "named"]),
            summary["files_total"])
        self.assertGreaterEqual(summary["files_skipped"], 1)


if __name__ == "__main__":
    unittest.main()
