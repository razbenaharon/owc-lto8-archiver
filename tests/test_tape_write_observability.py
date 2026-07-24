"""Robocopy tape-write observability and success-classification coverage.

These tests pin down the durable per-write raw log and the strengthened
success/failure classification introduced to close the "robocopy exited 0 but
committed nothing" gap. Everything is deterministic and uses fakes/mocks — no
real robocopy, no real tape, and (critically) no tape-side readback validation.
"""
import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

from src.backup import LTOBackup
from src.robocopy import (
    _parse_robocopy_summary,
    _run_robocopy_tuned,
    classify_robocopy_result,
)
from src.runtime import CANCEL
from src.tape_write_log import TapeWriteRawLog


# --- robocopy summary fixtures ------------------------------------------------

_SUMMARY_OK = (
    "   Total    Copied   Skipped  Mismatch    FAILED    Extras\n"
    "Files : 3 3 0 0 0 0\n"
    "Bytes : 1600000000 1600000000 0 0 0 0\n"
    "Speed : 104857600 Bytes/Sec.\n"
    "Times : 0:00:15 0:00:15\n"
)
_SUMMARY_ALL_SKIPPED = (
    "Files : 3 0 3 0 0 0\n"
    "Bytes : 1600000000 0 1600000000 0 0 0\n"
    "Times : 0:00:01 0:00:01\n"
)
_ERROR_LINE = "2026/07/24 07:00:41 ERROR 32 (0x00000020) Copying File x.zip\n"


# --- fakes --------------------------------------------------------------------

class _FakeStream:
    """Minimal line-yielding stream for a faked robocopy process."""

    def __init__(self, lines):
        self._lines = list(lines)

    def readline(self):
        return self._lines.pop(0) if self._lines else ''

    def close(self):
        pass


class _FakePopen:
    def __init__(self, stdout_lines, stderr_lines, returncode):
        self.stdout = _FakeStream(stdout_lines)
        self.stderr = _FakeStream(stderr_lines)
        self.pid = 4321
        self._rc = returncode
        self.returncode = None

    def wait(self):
        self.returncode = self._rc
        return self._rc


class _CommitTrackingDB:
    """FakeDB that records whether any file records were committed."""

    def __init__(self):
        self.file_commits = 0
        self.recalc_calls = 0

    def tape_exists(self, tape_label):
        return True

    def bulk_upsert_files(self, records, update_existing=True):
        list(records)
        self.file_commits += 1
        return {"inserted": 1, "updated": 0, "skipped": 0}

    def recalculate_tape_used_space(self, tape_label):
        self.recalc_calls += 1
        return 0


# =============================================================================
# Parser (scenarios 3, 4)
# =============================================================================
class SummaryParserTests(unittest.TestCase):
    def test_full_counters_parsed(self):
        p = _parse_robocopy_summary(_SUMMARY_OK)
        self.assertTrue(p["summary_found"])
        self.assertFalse(p["summary_malformed"])
        self.assertEqual(p["files_total"], 3)
        self.assertEqual(p["files_copied"], 3)
        self.assertEqual(p["files_failed"], 0)
        self.assertEqual(p["bytes_copied"], 1600000000)

    def test_missing_summary_is_flagged_not_malformed(self):
        # robocopy killed before printing its summary (scenario 3).
        p = _parse_robocopy_summary(_ERROR_LINE)
        self.assertFalse(p["summary_found"])
        self.assertFalse(p["summary_malformed"])
        self.assertEqual(p["files_failed"], 0)

    def test_malformed_summary_is_flagged(self):
        # A "Files :" header present but its counters cannot be parsed
        # (truncated/garbled — scenario 4).
        p = _parse_robocopy_summary("Files : 3 3 oops\n")
        self.assertFalse(p["summary_found"])
        self.assertTrue(p["summary_malformed"])


# =============================================================================
# Classifier (scenarios 2, 3, 4, 8)
# =============================================================================
class ClassifierTests(unittest.TestCase):
    def _classify(self, output, returncode, expected_files=3,
                  expected_bytes=1600000000):
        return classify_robocopy_result(
            returncode, _parse_robocopy_summary(output), output,
            expected_files=expected_files, expected_bytes=expected_bytes)

    def test_clean_success(self):
        v = self._classify(_SUMMARY_OK, 1)
        self.assertTrue(v.is_success)
        self.assertEqual(v.category, "success")

    def test_all_skipped_resume_is_success(self):
        # A legitimate resume where everything is already on tape: copied==0 but
        # skipped>0 must remain success (idempotent write).
        v = self._classify(_SUMMARY_ALL_SKIPPED, 1)
        self.assertTrue(v.is_success)

    def test_error_with_valid_summary_is_recovered_transient(self):
        # scenario 2 (deliberate rule preserved): an ERROR line accompanied by a
        # complete summary with 0 failures is a recovered transient -> success,
        # and the error evidence is still captured.
        v = self._classify(_ERROR_LINE + _SUMMARY_OK, 1)
        self.assertTrue(v.is_success)
        self.assertEqual(len(v.error_lines), 1)

    def test_error_without_summary_fails(self):
        # scenario 2: ERROR present AND summary missing -> untrusted.
        v = self._classify(_ERROR_LINE, 0)
        self.assertFalse(v.is_success)
        self.assertEqual(v.category, "missing_summary")
        self.assertIn("ERROR detected and final summary missing", v.detail)

    def test_return_zero_without_summary_fails(self):
        # scenario 3 / 8: THE bug — exit 0, no summary, no ERROR must NOT commit.
        v = self._classify("Started ...\nSome progress line\n", 0)
        self.assertFalse(v.is_success)
        self.assertEqual(v.category, "missing_summary")

    def test_malformed_summary_fails(self):
        # scenario 4.
        v = self._classify("Files : 3 3 oops\n", 0)
        self.assertFalse(v.is_success)
        self.assertEqual(v.category, "malformed_summary")

    def test_nonzero_return_code_fails(self):
        v = self._classify(_SUMMARY_OK, 8)
        self.assertFalse(v.is_success)
        self.assertEqual(v.category, "nonzero_return_code")

    def test_files_failed_fails(self):
        out = "Files : 3 2 0 0 1 0\nBytes : 1 1 0 0 0 0\nTimes : 0:00:01 0:00:01\n"
        v = self._classify(out, 8)
        self.assertFalse(v.is_success)

    def test_retry_limit_exceeded_fails(self):
        out = _SUMMARY_OK + "\nERROR: RETRY LIMIT EXCEEDED.\n"
        v = self._classify(out, 1)
        self.assertFalse(v.is_success)
        self.assertEqual(v.category, "retry_limit_exceeded")

    def test_zero_copy_unexpected_fails(self):
        # Valid summary reporting no work at all while source work was expected.
        out = ("Files : 0 0 0 0 0 0\nBytes : 0 0 0 0 0 0\n"
               "Times : 0:00:00 0:00:00\n")
        v = self._classify(out, 0, expected_files=3, expected_bytes=1600000000)
        self.assertFalse(v.is_success)
        self.assertEqual(v.category, "zero_copy_unexpected")

    def test_classifier_touches_no_filesystem(self):
        # scenario 13: the verdict is derived from robocopy evidence only. Prove
        # it reads nothing from disk/tape by making all filesystem probes raise.
        boom = mock.Mock(side_effect=AssertionError("tape-side access!"))
        with mock.patch("os.path.exists", boom), \
             mock.patch("os.listdir", boom), \
             mock.patch("os.scandir", boom), \
             mock.patch("os.walk", boom):
            v = self._classify(_SUMMARY_OK, 1)
        self.assertTrue(v.is_success)


# =============================================================================
# Incremental streaming + durable log (scenarios 1, 5, 6)
# =============================================================================
class StreamingRawLogTests(unittest.TestCase):
    def _run_with_fake_robocopy(self, tmp, stdout_lines, stderr_lines, rc):
        raw = TapeWriteRawLog(
            tmp, 37, 49, "Tape_02", os.path.join(tmp, "src"),
            os.path.join(tmp, "dst"), ["robocopy", "src", "dst", "/E"],
            expected_files=3, expected_bytes=1600000000)
        fake = _FakePopen(stdout_lines, stderr_lines, rc)
        with mock.patch("src.robocopy.subprocess.Popen", return_value=fake), \
             mock.patch("src.robocopy.register_proc"), \
             mock.patch("src.robocopy.unregister_proc"), \
             mock.patch("src.robocopy._apply_proc_tuning"):
            result = _run_robocopy_tuned(["robocopy"], raw_sink=raw)
        return raw, result

    def test_successful_output_is_persisted_completely(self):
        # scenario 1.
        with tempfile.TemporaryDirectory() as tmp:
            lines = ["Job started\n", "Files : 3 3 0 0 0 0\n", "Done\n"]
            raw, result = self._run_with_fake_robocopy(tmp, lines, [], 1)
            raw.close()
            self.assertEqual(result.returncode, 1)
            self.assertEqual(result.stdout, "".join(lines))
            body = open(raw.path, encoding="utf-8").read()
            for ln in lines:
                self.assertIn(ln.strip(), body)

    def test_partial_output_then_failure_is_persisted(self):
        # scenario 5: robocopy emits some lines then fails without a summary.
        with tempfile.TemporaryDirectory() as tmp:
            stdout = ["Job started\n", "Copying bundle 1\n"]
            stderr = ["fatal device error\n"]
            raw, result = self._run_with_fake_robocopy(tmp, stdout, stderr, 16)
            raw.close()
            self.assertEqual(result.returncode, 16)
            body = open(raw.path, encoding="utf-8").read()
            self.assertIn("Copying bundle 1", body)
            self.assertIn("[stderr] fatal device error", body)
            # stderr is returned unprefixed on the process object.
            self.assertEqual(result.stderr, "fatal device error\n")

    def test_log_survives_and_records_note_on_exception(self):
        # scenario 6: an exception propagates before a footer -> the durable log
        # still exists and carries the evidence + an abort note.
        with tempfile.TemporaryDirectory() as tmp:
            raw = TapeWriteRawLog(
                tmp, 37, 49, "Tape_02", "src", "dst", ["robocopy"])
            raw.write("partial robocopy line\n")
            self.assertFalse(raw.footer_written)
            raw.note("tape write aborted by RuntimeError: boom")
            raw.close()
            body = open(raw.path, encoding="utf-8").read()
            self.assertIn("partial robocopy line", body)
            self.assertIn("aborted by RuntimeError", body)

    def test_footer_records_classification(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw = TapeWriteRawLog(tmp, 1, 2, "T", "s", "d", ["robocopy"])
            rc_sum = _parse_robocopy_summary(_SUMMARY_OK)
            v = classify_robocopy_result(1, rc_sum, _SUMMARY_OK,
                                         expected_files=3)
            raw.write_footer(1, rc_sum, v)
            self.assertTrue(raw.footer_written)
            raw.close()
            body = open(raw.path, encoding="utf-8").read()
            self.assertIn("classification     : SUCCESS", body)
            self.assertIn("files_copied       : 3", body)


# =============================================================================
# Integration through LTOBackup.run (scenarios 7, 8, 9, 10, 12, 13)
# =============================================================================
class BackupRunClassificationTests(unittest.TestCase):
    def _run_backup(self, stdout, returncode, cancel=False):
        """Drive a direct (non-packed) backup with a faked robocopy result.

        Returns (db, raised_exc_or_None, source_dir_kept_open=False).
        """
        tmp = tempfile.mkdtemp()
        self.addCleanup(lambda: _rmtree(tmp))
        source = os.path.join(tmp, "source")
        tape = os.path.join(tmp, "tape")
        os.makedirs(source)
        os.makedirs(tape)
        with open(os.path.join(source, "bundle.bin"), "wb") as f:
            f.write(b"x" * 1024)
        db = _CommitTrackingDB()
        backup = LTOBackup(db, "", governor=None, log_dir=tmp)
        backup.eject_tape = lambda _drive: None
        result = SimpleNamespace(stdout=stdout, stderr="", returncode=returncode)
        raised = None
        if cancel:
            CANCEL.set()
        try:
            with mock.patch("src.backup._ensure_lto_drive_ready",
                            return_value=True), \
                 mock.patch("src.backup._run_robocopy_tuned",
                            return_value=result):
                try:
                    backup.run(source, tape, "Tape_02")
                except RuntimeError as e:
                    raised = e
        finally:
            if cancel:
                CANCEL.clear()
        return db, raised

    def test_valid_success_commits(self):
        # scenario 9.
        db, raised = self._run_backup(_SUMMARY_OK, 1)
        self.assertIsNone(raised)
        self.assertGreaterEqual(db.file_commits, 1)

    def test_exit_zero_without_summary_never_commits(self):
        # scenarios 8 + 10: exit 0 but no summary -> raise, zero file commits.
        db, raised = self._run_backup("Started...\nprogress...\n", 0)
        self.assertIsNotNone(raised)
        self.assertEqual(db.file_commits, 0)
        self.assertIn("incomplete/untrustworthy", str(raised))

    def test_malformed_summary_never_commits(self):
        # scenario 10.
        db, raised = self._run_backup("Files : 3 3 oops\n", 0)
        self.assertIsNotNone(raised)
        self.assertEqual(db.file_commits, 0)

    def test_failure_message_carries_raw_log_path(self):
        # scenario 7.
        db, raised = self._run_backup(_ERROR_LINE, 0)
        self.assertIsNotNone(raised)
        self.assertIn("Raw log:", str(raised))
        # The referenced raw log exists on disk.
        path = str(raised).split("Raw log:", 1)[1].split("|", 1)[0].strip()
        self.assertTrue(os.path.isfile(path))

    def test_cancel_without_summary_is_cut_midflight(self):
        # scenario 12: cooperative Ctrl+C semantics unchanged — a cancel with no
        # summary is the "cut mid-flight" path and never commits.
        db, raised = self._run_backup("progress...\n", 0, cancel=True)
        self.assertIsNotNone(raised)
        self.assertIn("cut mid-flight", str(raised))
        self.assertEqual(db.file_commits, 0)

    def test_remote_write_raw_log_is_labelled_with_session_and_chunk(self):
        # Remote-pipeline writes pass remote_* ids (local_* are None); the raw
        # log must still land under session_<id>/chunk_<idx>, not session_na.
        tmp = tempfile.mkdtemp()
        self.addCleanup(lambda: _rmtree(tmp))
        source = os.path.join(tmp, "source")
        tape = os.path.join(tmp, "tape")
        os.makedirs(source)
        os.makedirs(tape)
        with open(os.path.join(source, "bundle.bin"), "wb") as f:
            f.write(b"x" * 1024)
        db = _CommitTrackingDB()
        backup = LTOBackup(db, "", governor=None, log_dir=tmp)
        backup.eject_tape = lambda _drive: None
        result = SimpleNamespace(stdout=_SUMMARY_OK, stderr="", returncode=1)
        with mock.patch("src.backup._ensure_lto_drive_ready", return_value=True), \
             mock.patch("src.backup._run_robocopy_tuned", return_value=result):
            backup.run(source, tape, "Tape_02",
                       remote_session_id=37, remote_chunk_index=49)
        logdir = os.path.join(tmp, "tape_write", "session_37")
        self.assertTrue(os.path.isdir(logdir), f"missing {logdir}")
        logs = [f for f in os.listdir(logdir) if f.startswith("chunk_49_")]
        self.assertTrue(logs, f"no chunk_49_* log in {logdir}: {os.listdir(logdir)}")

    def test_cancel_with_complete_summary_still_commits(self):
        # scenario 12: a protected write that COMPLETED under cancel still
        # commits (its data is on tape); skipping would create ambiguity.
        db, raised = self._run_backup(_SUMMARY_OK, 1, cancel=True)
        self.assertIsNone(raised)
        self.assertGreaterEqual(db.file_commits, 1)


def _rmtree(path):
    import shutil
    shutil.rmtree(path, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
