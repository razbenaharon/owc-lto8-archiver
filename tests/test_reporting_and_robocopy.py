import csv
import os
import tempfile
import unittest

from src.reporting import SUMMARY_CSV, append_backup_summary_row
from src.robocopy import _parse_robocopy_summary


class _RobocopyResult:
    def __init__(self, returncode):
        self.returncode = returncode


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

    def test_old_summary_header_is_migrated_before_append(self):
        old_header = [
            "record_type", "operation", "status", "source_host", "source_path",
            "tape_label", "backup_mode", "local_session_id",
            "local_chunk_index", "started_at", "finished_at",
            "total_time_seconds", "robocopy_elapsed", "fetch_seconds",
            "pack_seconds", "db_sync_seconds", "copied_bytes",
            "planned_bytes", "fetch_bytes", "pack_bytes", "files_copied",
            "files_skipped", "files_failed", "already_on_tape",
            "source_missing_files", "records_inserted", "records_updated",
            "records_skipped", "tape_used_after_bytes",
            "robocopy_exit_code", "robocopy_speed_mbs", "before_bytes",
            "after_bytes", "reduction_pct",
        ]
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, SUMMARY_CSV)
            with open(path, "w", newline="", encoding="utf-8") as handle:
                csv.writer(handle).writerow(old_header)

            append_backup_summary_row(tmp, {
                "status": "completed_with_skips",
                "source": r"C:\staging\_pack_s0034_003",
                "tape_label": "Tape_02",
                "copied_bytes": 107380679989,
                "new_used": 3633327538007,
                "skipped_files_count": 1,
                "skipped_files_report": os.path.join(tmp, "skipped.csv"),
                "record_counts": {"files_inserted": 1, "files_skipped": 25897},
                "rc_sum": {
                    "elapsed": "0:04:59",
                    "files_copied": 6,
                    "files_failed": 0,
                    "speed_mbs": 342.1,
                },
            }, _RobocopyResult(0))

            with open(path, newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            with open(path, newline="", encoding="utf-8") as handle:
                header = next(csv.reader(handle))

            self.assertIn("skipped_files_count", header)
            self.assertIn("skipped_files_report", header)
            self.assertEqual(header[-2:], [
                "governor_wait_seconds", "governor_wait_reasons"])
            self.assertEqual(rows[-1]["tape_used_after_bytes"], "3633327538007")
            self.assertEqual(rows[-1]["robocopy_exit_code"], "0")
            self.assertEqual(rows[-1]["robocopy_speed_mbs"], "342.1")
            self.assertEqual(rows[-1]["records_inserted"], "1")
            self.assertEqual(rows[-1]["records_skipped"], "25897")
            self.assertEqual(rows[-1]["skipped_files_count"], "1")

    def test_shifted_summary_row_is_repaired_during_migration(self):
        old_header = [
            "record_type", "operation", "status", "source_host", "source_path",
            "tape_label", "backup_mode", "local_session_id",
            "local_chunk_index", "started_at", "finished_at",
            "total_time_seconds", "robocopy_elapsed", "fetch_seconds",
            "pack_seconds", "db_sync_seconds", "copied_bytes",
            "planned_bytes", "fetch_bytes", "pack_bytes", "files_copied",
            "files_skipped", "files_failed", "already_on_tape",
            "source_missing_files", "records_inserted", "records_updated",
            "records_skipped", "tape_used_after_bytes",
            "robocopy_exit_code", "robocopy_speed_mbs", "before_bytes",
            "after_bytes", "reduction_pct",
        ]
        shifted_row = [
            "backup", "backup", "completed_with_skips", "so01",
            r"C:\temp_for_disk\staging\_pack_s0034_003", "Tape_02",
            "staged/packed", "", "", "2026-07-03T08:07:35",
            "2026-07-03T08:12:43", "308.315", "0:04:59", "9077.322",
            "707.575", "8.770", "107380679989", "107380679989",
            "107372328901", "107380679989", "6", "0", "0", "0", "0",
            "1", r"C:\owc-lto8-archiver\backup_logs\skipped.csv",
            "25897", "0", "0", "3633327538007", "1", "342.051774",
            "",
        ]
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, SUMMARY_CSV)
            with open(path, "w", newline="", encoding="utf-8") as handle:
                writer = csv.writer(handle)
                writer.writerow(old_header)
                writer.writerow(shifted_row)

            append_backup_summary_row(tmp, {"record_counts": {}, "rc_sum": {}})

            with open(path, newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))

            repaired = rows[0]
            self.assertEqual(repaired["skipped_files_count"], "1")
            self.assertTrue(
                repaired["skipped_files_report"].endswith("skipped.csv"))
            self.assertEqual(repaired["records_inserted"], "25897")
            self.assertEqual(repaired["records_updated"], "0")
            self.assertEqual(repaired["records_skipped"], "0")
            self.assertEqual(repaired["tape_used_after_bytes"], "3633327538007")
            self.assertEqual(repaired["robocopy_exit_code"], "1")
            self.assertEqual(repaired["robocopy_speed_mbs"], "342.051774")

    def test_ram_telemetry_columns_append_and_blank_when_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = append_backup_summary_row(tmp, {
                "status": "completed",
                "fetch_ram_peak_pct": "72.5",
                "pack_process_peak_mb": "3100.0",
                "governor_wait_reasons": "tape_active",
                "record_counts": {},
                "rc_sum": {},
            })
            with open(path, newline="", encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
            with open(path, newline="", encoding="utf-8") as handle:
                header = next(csv.reader(handle))

            self.assertEqual(rows[0]["fetch_ram_peak_pct"], "72.5")
            self.assertEqual(rows[0]["pack_process_peak_mb"], "3100.0")
            self.assertEqual(rows[0]["governor_wait_reasons"], "tape_active")
            self.assertEqual(rows[0]["db_sync_ram_peak_pct"], "")
            self.assertEqual(header[-2:], [
                "governor_wait_seconds", "governor_wait_reasons"])

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
        self.assertTrue(parsed["summary_found"])

    def test_robocopy_parser_flags_missing_summary(self):
        # Output cut off before the "Files :" line (robocopy killed mid-run):
        # the zeroed counters must not read as "no failures".
        parsed = _parse_robocopy_summary(
            "2026/07/03 08:12:43 ERROR 32 (0x00000020) Copying File x.bin\n")
        self.assertFalse(parsed["summary_found"])
        self.assertEqual(parsed["files_failed"], 0)


if __name__ == "__main__":
    unittest.main()
