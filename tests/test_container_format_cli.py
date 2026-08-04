"""Plan 2 migration-015 CLI safety. Local-only; never opens LTFS or PostgreSQL."""
import os
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

    def test_candidate_presence_is_found_from_root_listing_without_descent(self):
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
            self.assertEqual(scanned, [os.path.abspath(root)])
            self.assertNotIn("must_not_be_read.bin", str(evidence))

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


if __name__ == "__main__":
    unittest.main()
