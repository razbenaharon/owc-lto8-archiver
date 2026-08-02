import csv
import inspect
import os
import stat
import tempfile
import unittest
from unittest import mock
from pathlib import Path

from src.pg_tapes import PgTapeMixin
from src.remote_orchestrator import RemoteOrchestrator
from src.tape_reset import (execute_reset, latest_chunk_write_evidence,
                            positive_format_evidence_since, read_json,
                            write_immutable_json)


class TapeResetUnitTests(unittest.TestCase):
    def test_immutable_audit_is_write_once(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "impact.json"
            written, _ = write_immutable_json(path, {"scope": "Tape_03"})
            self.assertEqual(read_json(written), {"scope": "Tape_03"})
            self.assertFalse(os.stat(path).st_mode & stat.S_IWRITE)
            with self.assertRaises(FileExistsError):
                write_immutable_json(path, {"scope": "different"})

    def test_latest_successful_write_decides_physical_owner(self):
        fields = ["source_path", "tape_label", "status", "started_at",
                  "finished_at", "copied_bytes", "planned_bytes",
                  "tape_used_after_bytes", "robocopy_exit_code"]
        rows = [
            [r"C:\staging\_pack_s0037_049", "Tape_02", "completed",
             "1", "2026-07-20T01:00:00", "1", "1", "1", "1"],
            [r"C:\staging\_pack_s0037_049", "Tape_03", "completed",
             "2", "2026-07-30T01:00:00", "1", "1", "1", "1"],
            [r"C:\staging\_pack_s0037_108", "Tape_03", "failed_critical",
             "3", "2026-07-31T01:00:00", "0", "1", "1", "0"],
        ]
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "SUMMARY.csv"
            with open(path, "w", newline="", encoding="utf-8") as fh:
                writer = csv.writer(fh)
                writer.writerow(fields)
                writer.writerows(rows)
            latest, history = latest_chunk_write_evidence(path)
        self.assertEqual(latest[(37, 49)]["tape_label"], "Tape_03")
        self.assertNotIn((37, 108), latest)
        self.assertEqual(len(history[(37, 49)]), 2)

    def test_legacy_formatted_tape_replacement_is_disabled(self):
        with self.assertRaisesRegex(RuntimeError, "tape-reset"):
            PgTapeMixin.replace_formatted_tape(object(), "Tape_03")

    def test_generation_mismatch_blocks_old_session(self):
        class DB:
            @staticmethod
            def get_tape(_label):
                return {"current_generation": 2}

        orch = object.__new__(RemoteOrchestrator)
        orch.db = DB()
        result = orch._verify_session_tape_generation({
            "session_id": 37, "tape_label": "Tape_03",
            "tape_generation": 1})
        self.assertIsNotNone(result)
        self.assertFalse(result.resumable)

    def test_reset_path_contains_no_eject_command(self):
        source = inspect.getsource(execute_reset)
        self.assertNotIn("eject_tape", source)
        self.assertNotIn("LtfsCmdEject", source)

    def test_positive_ltfs_evidence_overrides_wrapper_exit_code(self):
        identity = {"label": "Tape_03", "volume_lock_status": "0x00"}
        rows = [
            {"id": 15024, "message": "Medium formatted successfully."},
            {"id": 15013, "message": "Volume UUID is: new-uuid."},
            {"id": 11031, "message": "Volume mounted successfully. Gen = 1"},
            {"id": 17228, "message": "Volume Lock Status = 0x00."},
        ]
        with mock.patch("src.tape_reset.mounted_identity", return_value=identity), \
                mock.patch("src.tape_reset._ltfs_rows", return_value=rows):
            evidence = positive_format_evidence_since(
                "Z:\\", "Tape_03", "2026-08-02T12:51:56+03:00",
                returncode=60201, output="management wrapper error")
        self.assertTrue(evidence["management_wrapper_reported_error"])
        self.assertEqual(evidence["mounted_identity"]["label"], "Tape_03")


if __name__ == "__main__":
    unittest.main()
