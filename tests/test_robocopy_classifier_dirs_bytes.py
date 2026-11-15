"""Audit (low): the robocopy classifier ignored a directory-level failure and
never compared the bytes robocopy accounted for with the expected total.

Pure parsing/classification - no robocopy, no tape."""
import unittest

from src.robocopy import _parse_robocopy_summary, classify_robocopy_result


def _summary(dirs_failed=0, files=(3, 3, 0, 0, 0, 0),
             byte_counts=(3000, 3000, 0, 0, 0, 0)):
    return (
        "               Total    Copied   Skipped  Mismatch    FAILED    Extras\n"
        f"    Dirs :         4         3         1         0         {dirs_failed}         0\n"
        "   Files : " + " ".join(str(n) for n in files) + "\n"
        "   Bytes : " + " ".join(str(n) for n in byte_counts) + "\n"
        "   Times :   0:00:01   0:00:01                       0:00:00   0:00:00\n")


def _classify(out, **expected):
    return classify_robocopy_result(1, _parse_robocopy_summary(out), out,
                                    **expected)


class DirectoryFailure(unittest.TestCase):
    def test_dirs_line_is_parsed(self):
        parsed = _parse_robocopy_summary(_summary(dirs_failed=2))
        self.assertEqual(parsed["dirs_failed"], 2)
        self.assertEqual(parsed["bytes_skipped"], 0)

    def test_failed_directory_is_not_success(self):
        verdict = _classify(_summary(dirs_failed=1), expected_files=3,
                            expected_bytes=3000, expected_source_files=3)
        self.assertFalse(verdict.is_success)
        self.assertEqual(verdict.category, "dirs_failed")

    def test_clean_directories_stay_success(self):
        verdict = _classify(_summary(), expected_files=3,
                            expected_bytes=3000, expected_source_files=3)
        self.assertTrue(verdict.is_success)


class ByteAccounting(unittest.TestCase):
    def test_fewer_bytes_than_expected_is_not_success(self):
        out = _summary(byte_counts=(2000, 2000, 0, 0, 0, 0))
        verdict = _classify(out, expected_files=3, expected_bytes=3000,
                            expected_source_files=3)
        self.assertFalse(verdict.is_success)
        self.assertEqual(verdict.category, "incomplete_bytes")

    def test_skipped_bytes_count_as_accounted(self):
        # A resume: one file already on tape (skipped), two copied.
        out = _summary(files=(3, 2, 1, 0, 0, 0),
                       byte_counts=(3000, 2000, 1000, 0, 0, 0))
        verdict = _classify(out, expected_files=3, expected_bytes=3000,
                            expected_source_files=3)
        self.assertTrue(verdict.is_success)

    def test_more_bytes_than_expected_is_fine(self):
        # Direct path: expected covers only the newly submitted files.
        out = _summary(byte_counts=(5000, 3000, 2000, 0, 0, 0))
        verdict = _classify(out, expected_files=3, expected_bytes=3000,
                            expected_source_files=3)
        self.assertTrue(verdict.is_success)

    def test_summary_without_a_bytes_line_is_not_success(self):
        out = ("   Files : 3 2 1 0 0 0\n")
        verdict = _classify(out, expected_files=3, expected_bytes=3000,
                            expected_source_files=3)
        self.assertFalse(verdict.is_success)
        self.assertEqual(verdict.category, "missing_byte_summary")

    def test_no_expectation_no_check(self):
        out = _summary(byte_counts=(2000, 2000, 0, 0, 0, 0))
        self.assertTrue(_classify(out).is_success)


if __name__ == "__main__":
    unittest.main()
