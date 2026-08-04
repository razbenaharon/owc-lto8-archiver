"""Plan 2 migration-015 CLI safety. Local-only; never opens LTFS or PostgreSQL."""
import os
import json
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

import inspect_db
from src.cli_errors import OperationalError


class ContainerFormatStagingInspectionTests(unittest.TestCase):
    @staticmethod
    def _db(root):
        return SimpleNamespace(get_remote_session=lambda _session_id: {
            "staging_dir": root})

    def test_candidate_pack_is_inventoried_without_exposing_member_names(self):
        with tempfile.TemporaryDirectory() as root:
            candidate = os.path.join(root, "_pack_s0037_049")
            os.mkdir(candidate)
            with open(os.path.join(candidate, "must_not_be_read.bin"), "wb") \
                    as handle:
                handle.write(b"local fixture")
            real_scandir = os.scandir
            scanned = []

            def recording_scandir(path):
                scanned.append(os.path.abspath(path))
                return real_scandir(path)

            cfg = SimpleNamespace(lto_drive="Z:\\")
            with mock.patch.object(
                    inspect_db, "_windows_drive_device_target",
                    side_effect=lambda drive: drive.casefold()), \
                    mock.patch.object(
                        inspect_db.os, "scandir",
                        side_effect=recording_scandir):
                evidence = inspect_db._inspect_container_format_staging(
                    cfg, self._db(root), 37, [49])
            self.assertEqual(evidence["entry_count"], 1)
            self.assertEqual(scanned, [os.path.abspath(root), candidate])
            self.assertNotIn("must_not_be_read.bin", str(evidence))

    def test_valid_resume_marker_and_actual_inventory_are_reported_read_only(self):
        with tempfile.TemporaryDirectory() as root:
            candidate = os.path.join(root, "_pack_s0037_049")
            os.mkdir(candidate)
            member = os.path.join(candidate, "member.bin")
            with open(member, "wb") as handle:
                handle.write(b"fixture")
            marker = os.path.join(candidate, "_resume_pack.json")
            with open(marker, "w", encoding="utf-8") as handle:
                json.dump({
                    "version": 1, "session_id": 37, "chunk_index": 49,
                    "packaging_format": "zip",
                    "pack_inventory": [["member.bin", 7]],
                }, handle)

            with mock.patch.object(
                    inspect_db, "_windows_drive_device_target",
                    side_effect=lambda drive: drive.casefold()):
                evidence = inspect_db._inspect_container_format_staging(
                    SimpleNamespace(lto_drive="Z:\\"), self._db(root), 37,
                    [49], strict=False, include_details=True)

            observed = evidence["chunks"][0]
            self.assertTrue(observed["pack_entry_present"])
            self.assertEqual(
                observed["resume_marker"]["marker_state"], "valid")
            self.assertEqual(
                observed["resume_marker"]["inventory_state"], "matched")
            self.assertTrue(os.path.exists(marker))

    def test_existing_pack_without_resume_marker_reports_absent_not_empty(self):
        with tempfile.TemporaryDirectory() as root:
            candidate = os.path.join(root, "_pack_s0037_049")
            os.mkdir(candidate)
            with open(os.path.join(candidate, "member.bin"), "wb") as handle:
                handle.write(b"fixture")
            with mock.patch.object(
                    inspect_db, "_windows_drive_device_target",
                    side_effect=lambda drive: drive.casefold()):
                evidence = inspect_db._inspect_container_format_staging(
                    SimpleNamespace(lto_drive="Z:\\"), self._db(root), 37,
                    [49], strict=False, include_details=True)
        observed = evidence["chunks"][0]
        self.assertTrue(observed["pack_entry_present"])
        self.assertEqual(
            observed["resume_marker"]["marker_state"], "absent")
        self.assertEqual(
            observed["resume_marker"]["inventory_state"], "readable")
        self.assertEqual(observed["resume_marker"]["pack_file_count"], 1)

    def test_absent_staging_root_is_unknown_in_rehearsal_mode(self):
        evidence = inspect_db._inspect_container_format_staging(
            SimpleNamespace(lto_drive="Z:\\"), self._db(""), 37, [49],
            strict=False, include_details=True)
        self.assertEqual(evidence["root_state"], "absent")
        self.assertEqual(evidence["evidence_state"], "unknown")
        self.assertIsNone(evidence["entry_count"])

    def test_unreadable_staging_root_is_unknown_in_rehearsal_mode(self):
        with tempfile.TemporaryDirectory() as root, \
                mock.patch.object(
                    inspect_db, "_windows_drive_device_target",
                    side_effect=lambda drive: drive.casefold()), \
                mock.patch.object(
                    inspect_db, "_assert_plain_local_directory"), \
                mock.patch.object(
                    inspect_db.os, "scandir", side_effect=OSError("denied")):
            evidence = inspect_db._inspect_container_format_staging(
                SimpleNamespace(lto_drive="Z:\\"), self._db(root), 37, [49],
                strict=False, include_details=True)
        self.assertEqual(evidence["root_state"], "unreadable")
        self.assertEqual(evidence["evidence_state"], "unknown")
        self.assertIsNone(evidence["unreadable_count"])

    def test_missing_ltfs_configuration_refuses_before_root_enumeration(self):
        with tempfile.TemporaryDirectory() as root, \
                mock.patch.object(inspect_db.os, "scandir") as scandir:
            with self.assertRaisesRegex(OperationalError, "configuration is absent"):
                inspect_db._inspect_container_format_staging(
                    SimpleNamespace(lto_drive=""), self._db(root), 37, [49])
            scandir.assert_not_called()

    def test_device_and_unc_staging_aliases_are_rejected_without_scandir(self):
        cfg = SimpleNamespace(lto_drive="Z:\\")
        for alias in (r"\\?\Z:\staging", r"\\server\share\staging"):
            with self.subTest(alias=alias), \
                    mock.patch.object(inspect_db.os, "scandir") as scandir:
                with self.assertRaises(OperationalError):
                    inspect_db._inspect_container_format_staging(
                        cfg, self._db(alias), 37, [49])
                scandir.assert_not_called()

    def test_same_device_mapping_is_rejected_without_root_enumeration(self):
        with tempfile.TemporaryDirectory() as root, \
                mock.patch.object(
                    inspect_db, "_windows_drive_device_target",
                    return_value="same-device"), \
                mock.patch.object(inspect_db.os, "scandir") as scandir:
            with self.assertRaisesRegex(OperationalError, "LTFS drive"):
                inspect_db._inspect_container_format_staging(
                    SimpleNamespace(lto_drive="Z:\\"), self._db(root), 37,
                    [49])
            scandir.assert_not_called()


class ContainerFormatCliDispatchTests(unittest.TestCase):
    def test_dry_run_and_execute_together_are_rejected_before_database_open(self):
        args = SimpleNamespace(dry_run=True, execute=True)
        parser = mock.Mock()
        parser.error.side_effect = RuntimeError("exclusive modes")
        with mock.patch.object(inspect_db, "_open_read_only_db") as open_db:
            with self.assertRaisesRegex(RuntimeError, "exclusive modes"):
                inspect_db._apply_container_format_schema(
                    SimpleNamespace(), args, parser)
        open_db.assert_not_called()

    def test_read_only_manager_uses_server_enforced_default(self):
        cfg = SimpleNamespace(
            pg_host="127.0.0.1", pg_port="15432", pg_dbname="fixture",
            pg_user="fixture", pg_password="", pg_sslmode="disable")
        with mock.patch("src.pg_db.PgDatabaseManager") as manager:
            inspect_db._open_read_only_db(cfg)
        conninfo = manager.call_args.args[0]
        self.assertIn("default_transaction_read_only=on", conninfo)
        self.assertEqual(manager.call_args.kwargs, {"init_schema": False})

    def test_stored_tar_execute_refuses_before_database_open_when_gate_false(self):
        args = SimpleNamespace(
            dry_run=False, execute=True,
            stored_tar_exception_session_id=37)
        with mock.patch.object(inspect_db, "_open_read_only_db") as open_db:
            with self.assertRaisesRegex(
                    OperationalError, "stored_tar_write_enabled is false"):
                inspect_db._apply_container_format_schema(
                    SimpleNamespace(stored_tar_write_enabled=False), args,
                    mock.Mock())
        open_db.assert_not_called()

    def test_rehearsal_still_reports_unknown_staging_with_gate_false(self):
        db = mock.Mock()
        db.classify_format_boundary.return_value = {
            "derived_boundary": 49,
            "chunks": [{"chunk_index": 49}],
        }
        db.get_remote_session.return_value = {"staging_dir": ""}
        captured = []
        cfg = SimpleNamespace(
            pg_dbname="fixture", stored_tar_write_enabled=False,
            backup_log_dir=None, lto_drive="Z:\\")
        with mock.patch.object(inspect_db, "_open_read_only_db", return_value=db), \
                mock.patch.object(inspect_db, "_conninfo", return_value="fixture"), \
                mock.patch.object(inspect_db, "liveness_evidence", return_value={}), \
                mock.patch.object(inspect_db, "_print_json",
                                  side_effect=captured.append):
            result = inspect_db._run_session37_boundary_rehearsal(
                cfg, SimpleNamespace(session_id=[]))
        self.assertEqual(result, 0)
        self.assertFalse(captured[0]["stored_tar_write_enabled"])
        self.assertEqual(
            captured[0]["staging_evidence"]["evidence_state"], "unknown")
        db.close.assert_called_once()

    def test_normal_zip_execute_still_runs_with_gate_false(self):
        args = SimpleNamespace(
            dry_run=False, execute=True, yes=True, backup_file="backup.dump",
            stored_tar_exception_session_id=None,
            expected_format_boundary=None, format_approval_id=None,
            format_approval_reason=None)
        cfg = SimpleNamespace(
            pg_dbname="fixture", stored_tar_write_enabled=False)
        read_db = mock.Mock()
        read_db.container_format_schema_preflight.return_value = {
            "blocking": []}
        write_db = mock.Mock()
        write_db.container_format_schema_preflight.return_value = {
            "blocking": []}
        write_db.apply_container_format_schema.return_value = [
            "015_postgres_container_formats.sql"]
        write_db.validate_container_format_schema.return_value = {"ready": True}
        receipt = {"receipt_id": "fixture"}
        with mock.patch.object(
                inspect_db, "_open_read_only_db", return_value=read_db), \
                mock.patch.object(
                    inspect_db, "_open_no_init_db", return_value=write_db), \
                mock.patch.object(inspect_db, "_conninfo", return_value="fixture"), \
                mock.patch.object(inspect_db, "archiver_lock_status", return_value=[]), \
                mock.patch.object(inspect_db, "active_archive_processes",
                                  return_value=[]), \
                mock.patch.object(inspect_db, "verify_backup_receipt",
                                  return_value=receipt), \
                mock.patch.object(inspect_db, "_print_json"):
            result = inspect_db._apply_container_format_schema(
                cfg, args, mock.Mock())
        self.assertEqual(result, 0)
        write_db.apply_container_format_schema.assert_called_once_with(
            exception_session_id=None, expected_boundary=None,
            approval_id=None, approval_reason=None, staging_evidence=None,
            require_archiver_lock=True)


if __name__ == "__main__":
    unittest.main()
