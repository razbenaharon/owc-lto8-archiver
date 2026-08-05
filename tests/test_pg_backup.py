import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

from src.constants import PROJECT_ROOT
from src.pg_backup import (
    _backup_filename,
    _is_loopback_host,
    _timestamped_migration_db_name,
    _safe_filename_part,
    create_backup_receipt,
    verify_backup_receipt,
    verify_backup_file,
)


class PostgresBackupHelperTests(unittest.TestCase):
    @staticmethod
    def _identity(database="fixture"):
        return {
            "database": database, "server_addr": "127.0.0.1",
            "server_port": 15432, "database_user": "lto",
            "database_oid": "123", "system_identifier": "456"}

    def test_safe_filename_part_removes_windows_unsafe_characters(self):
        self.assertEqual(
            _safe_filename_part("archive:prod/db", "database"),
            "archive_prod_db",
        )

    def test_backup_filename_uses_db_name_and_custom_dump_suffix(self):
        self.assertEqual(
            _backup_filename("lto_archive", "20260702_120000"),
            "lto_archive_20260702_120000.dump",
        )

    def test_production_backup_filename_has_explicit_prefix(self):
        self.assertEqual(
            _backup_filename(
                "lto_archive", "20260702_120000",
                prefix="prod_before_directory_catalog"),
            "prod_before_directory_catalog_lto_archive_20260702_120000.dump",
        )

    def test_loopback_host_detection_matches_local_postgres_defaults(self):
        self.assertTrue(_is_loopback_host("localhost"))
        self.assertTrue(_is_loopback_host("127.0.0.1"))
        self.assertFalse(_is_loopback_host("db.example.org"))

    def test_timestamped_migration_db_name_is_explicit(self):
        self.assertEqual(
            _timestamped_migration_db_name("lto_archive", "20260702_120000"),
            "lto_archive_directory_catalog_20260702_120000",
        )

    def test_backup_verification_fails_closed_on_empty_file(self):
        class Cfg:
            pg_host = "localhost"

        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "empty.dump"
            path.write_bytes(b"")
            with self.assertRaisesRegex(RuntimeError, "missing or empty"):
                verify_backup_file(Cfg(), path)

    def test_target_bound_receipt_detects_file_or_database_change(self):
        with TemporaryDirectory() as tmp:
            backup = Path(tmp) / "fixture.dump"
            backup.write_bytes(b"custom-format-fixture")
            cfg = object()
            patches = (
                mock.patch("src.pg_backup.verify_backup_file",
                           return_value=str(backup)),
                mock.patch("src.pg_backup._backup_database_identity",
                           return_value=self._identity()),
                mock.patch("src.pg_backup._backup_catalog_fingerprint",
                           return_value="catalog-v1"),
            )
            with patches[0], patches[1], patches[2]:
                create_backup_receipt(cfg, backup)
                verified = verify_backup_receipt(cfg, backup)
                self.assertTrue(verified["verified"])
                self.assertEqual(verified["target_database"], "fixture")

                backup.write_bytes(b"changed")
                with self.assertRaisesRegex(RuntimeError, "does not match"):
                    verify_backup_receipt(cfg, backup)

            backup.write_bytes(b"custom-format-fixture")
            with mock.patch(
                    "src.pg_backup.verify_backup_file",
                    return_value=str(backup)), \
                    mock.patch(
                        "src.pg_backup._backup_database_identity",
                        return_value=self._identity("other")), \
                    mock.patch(
                        "src.pg_backup._backup_catalog_fingerprint",
                        return_value="catalog-v1"):
                with self.assertRaisesRegex(RuntimeError, "different database"):
                    verify_backup_receipt(cfg, backup)

    def test_directory_catalog_migration_is_additive_only(self):
        sql = (Path(PROJECT_ROOT) / "scripts" / "sql"
               / "007_postgres_directory_catalog.sql").read_text(
                   encoding="utf-8").upper()
        forbidden = (
            "CREATE DATABASE",
            "DROP DATABASE",
            "DROP TABLE",
            "TRUNCATE",
            "DELETE FROM FILES_INDEX",
            "ALTER TABLE FILES_INDEX",
        )
        for token in forbidden:
            self.assertNotIn(token, sql)


class WindowsDeviceTargetTests(unittest.TestCase):
    """`_windows_device_target` must resolve, never guess.

    The IBM LTFS driver lists the same device symlink twice for its drive
    letter (verified 2026-08-05: survives a reboot, remounts fine with it
    present).  A repeated target resolves to exactly one device and is
    therefore determinate; two DIFFERENT targets are not, and must still be
    refused so a dump can never land on the LTFS drive.
    """

    @staticmethod
    def _query_returning(targets):
        import ctypes

        def fake_query(name, buffer, size):
            data = "\0".join(targets) + "\0\0"
            encoded = data.encode("utf-16-le")
            ctypes.memmove(buffer, encoded, len(encoded))
            return len(data)

        return fake_query

    def _resolve(self, targets):
        import ctypes

        from src import pg_backup

        with mock.patch.object(pg_backup.os, "name", "nt"), \
                mock.patch.object(
                    ctypes.windll.kernel32, "QueryDosDeviceW",
                    self._query_returning(targets)):
            return pg_backup._windows_device_target("Z:")

    def test_single_mapping_resolves(self):
        self.assertEqual(
            self._resolve(["\\Device\\HarddiskVolume3"]),
            "\\device\\harddiskvolume3")

    def test_repeated_identical_mapping_resolves(self):
        # The live LTFS shape: the same target listed twice.
        target = "\\Device\\UfsIoDev_4EEB10F8LTFS"
        self.assertEqual(
            self._resolve([target, target]), target.casefold())

    def test_differing_mappings_are_still_refused(self):
        with self.assertRaisesRegex(RuntimeError, "multiple mappings"):
            self._resolve(["\\Device\\HarddiskVolume3",
                           "\\Device\\HarddiskVolume7"])

    def test_repeated_network_mapping_is_still_refused(self):
        # Deduplication must not smuggle an aliased target past the next gate.
        target = "\\Device\\LanmanRedirector\\server\\share"
        with self.assertRaisesRegex(RuntimeError, "Aliased or non-local"):
            self._resolve([target, target])


if __name__ == "__main__":
    unittest.main()
