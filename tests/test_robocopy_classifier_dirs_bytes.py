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


class ReviewFollowUp(unittest.TestCase):
    """Independent review of the classifier (2026-11-16)."""

    def test_negative_return_code_is_not_success(self):
        # A killed process reports a negative code on POSIX-style runtimes;
        # a clean-looking summary must not turn that into success.
        out = _summary(files=(1, 1, 0, 0, 0, 0),
                       byte_counts=(1, 1, 0, 0, 0, 0))
        verdict = classify_robocopy_result(
            -9, _parse_robocopy_summary(out), out,
            expected_files=1, expected_bytes=1, expected_source_files=1)
        self.assertFalse(verdict.is_success)
        self.assertEqual(verdict.category, "nonzero_return_code")

    def _rounded(self, copied):
        return (
            "               Total    Copied   Skipped  Mismatch    FAILED    Extras\n"
            "    Dirs :         1         1         0         0         0         0\n"
            "   Files :         2         2         0         0         0         0\n"
            f"   Bytes :   {copied}   {copied}         0         0         0         0\n"
            "   Times :   0:00:01   0:00:01                       0:00:00   0:00:00\n")

    def test_rounded_byte_summary_far_below_expected_is_not_success(self):
        out = self._rounded("1.00 m")
        verdict = _classify(out, expected_files=2, expected_bytes=10_000_000,
                            expected_source_files=2)
        self.assertFalse(verdict.is_success)
        self.assertEqual(verdict.category, "incomplete_bytes")

    def test_rounded_byte_summary_matching_expected_stays_success(self):
        # "9.53 m" is how robocopy prints 10,000,000 bytes without /BYTES.
        out = self._rounded("9.53 m")
        verdict = _classify(out, expected_files=2, expected_bytes=10_000_000,
                            expected_source_files=2)
        self.assertTrue(verdict.is_success)

    def test_exact_byte_counts_keep_full_integer_precision(self):
        # Going through float loses the last digit above 2**53.
        big = 9007199254740993
        out = _summary(files=(1, 1, 0, 0, 0, 0),
                       byte_counts=(big, big, 0, 0, 0, 0))
        self.assertEqual(_parse_robocopy_summary(out)["bytes_copied"], big)
        verdict = _classify(out, expected_files=1, expected_bytes=big,
                            expected_source_files=1)
        self.assertTrue(verdict.is_success)
