import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from src.constants import PROJECT_ROOT
from src.pg_backup import (
    _backup_filename,
    _is_loopback_host,
    _timestamped_migration_db_name,
    _safe_filename_part,
    verify_backup_file,
)


class PostgresBackupHelperTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
