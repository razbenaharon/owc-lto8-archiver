import csv
import os
import tempfile
import unittest

from src.reporting import SUMMARY_CSV, append_backup_summary_row
from src.robocopy import _parse_robocopy_summary


class ReportingAndRobocopyTests(unittest.TestCase):
    def test_backup_summary_includes_skipped_report_fields(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = append_backup_summary_row(tmp, {
                "status": "completed_with_skips",
                "skipped_files_count": 2,
                "skipped_files_report": os.path.join(tmp, "skipped.csv"),
                "record_counts": {},
                "rc_sum": {},
            })
            self.assertEqual(os.path.basename(path), SUMMARY_CSV)
            with open(path, newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(rows[0]["status"], "completed_with_skips")
            self.assertEqual(rows[0]["skipped_files_count"], "2")
            self.assertTrue(rows[0]["skipped_files_report"].endswith("skipped.csv"))

    def test_robocopy_bytes_parser_accepts_integer_byte_summary(self):
        output = """
        Files : 2 1 1 0 0 0
        Bytes : 123456789 98765432 24691357 0 0 0
        Speed : 104857600 Bytes/Sec.
        Times : 0:00:01 0:00:01
        """
        parsed = _parse_robocopy_summary(output)
        self.assertEqual(parsed["bytes_copied"], 98765432)
        self.assertEqual(parsed["files_copied"], 1)
        self.assertAlmostEqual(parsed["speed_mbs"], 100.0)


if __name__ == "__main__":
    unittest.main()
