"""Plan-2 Task-4.2 fail-closed Session-37 format boundary rules."""
import unittest

from src.session_reconcile import classify_session37_format_boundary_category


def _row(status, *, fixed=True, **overrides):
    row = {
        "status": status,
        "fixed_membership": fixed,
        "has_owner_evidence": False,
        "has_lease_evidence": False,
        "has_attempt_evidence": False,
        "has_error_evidence": False,
        "file_state_count": 0,
        "worker_attempt_count": 0,
        "catalog_file_count": 0,
        "archive_run_count": 0,
        "container_count": 0,
        "written_container_count": 0,
        "artifact_count": 0,
        "directory_evidence_count": 0,
        "sealed_batch_evidence_count": 0,
        "sealed_batch_written_count": 0,
        "writer_started_at": None,
        "writer_completed_at": None,
        "catalog_committed_at": None,
    }
    row.update(overrides)
    return row


class Session37FormatBoundaryRuleTests(unittest.TestCase):
    def test_task42_category_table(self):
        cases = [
            (
                "done_with_written_catalog_evidence",
                _row("done", catalog_file_count=1),
                "done_with_written_or_catalog_evidence",
                "zip",
                False,
            ),
            (
                "copy_may_have_succeeded_without_catalog_finality",
                _row("packing", writer_started_at="2026-08-04T00:00:00Z"),
                "copy_may_have_succeeded_without_catalog_finality",
                None,
                False,
            ),
            (
                "fetching",
                _row("fetching", has_owner_evidence=True),
                "fetching",
                "zip",
                False,
            ),
            (
                "packing",
                _row("packing", worker_attempt_count=1),
                "packing",
                "zip",
                False,
            ),
            (
                "backing",
                _row("backing"),
                "backing",
                None,
                False,
            ),
            (
                "pending_fixed_membership_never_owned",
                _row("pending"),
                "pending_never_owned_fixed_membership",
                "stored_tar",
                True,
            ),
            (
                "pending_fixed_membership_with_evidence",
                _row("pending", file_state_count=1),
                "pending_with_existing_evidence",
                "zip",
                False,
            ),
            (
                "fetch_failed",
                _row("fetch_failed", has_error_evidence=True),
                "fetch_failed",
                "zip",
                False,
            ),
            (
                "backup_failed",
                _row("backup_failed", has_error_evidence=True),
                "backup_failed",
                "zip",
                False,
            ),
            (
                "absent_conflicting_membership",
                _row("pending", fixed=False),
                "absent_or_conflicting_membership",
                None,
                False,
            ),
            (
                "stale_conflicting_status",
                _row("unknown"),
                "stale_or_conflicting_evidence",
                None,
                False,
            ),
            (
                "future_chunk",
                {"future_chunk": True},
                "future_chunk_after_persisted_boundary",
                "stored_tar",
                True,
            ),
        ]
        for name, evidence, category, assigned, eligible in cases:
            with self.subTest(name=name):
                result = classify_session37_format_boundary_category(evidence)
                self.assertEqual(result["observed_category"], category)
                self.assertEqual(result["assigned_format"], assigned)
                self.assertEqual(
                    result["eligible_stored_tar_exception"], eligible)

    def test_status_alone_does_not_authorize_conversion(self):
        for status in ("done", "pending"):
            with self.subTest(status=status):
                result = classify_session37_format_boundary_category(
                    _row(status, fixed=False))
                self.assertEqual(
                    result["observed_category"],
                    "absent_or_conflicting_membership")
                self.assertIsNone(result["assigned_format"])
                self.assertFalse(result["eligible_stored_tar_exception"])

    def test_pending_status_with_fixed_membership_is_not_enough_when_owned(self):
        result = classify_session37_format_boundary_category(
            _row("pending", has_owner_evidence=True))
        self.assertEqual(
            result["observed_category"], "pending_with_existing_evidence")
        self.assertEqual(result["assigned_format"], "zip")
        self.assertFalse(result["eligible_stored_tar_exception"])


if __name__ == "__main__":
    unittest.main()
