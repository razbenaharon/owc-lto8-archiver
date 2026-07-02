import unittest

from src.pg_backup import _backup_filename, _is_loopback_host, _safe_filename_part


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

    def test_loopback_host_detection_matches_local_postgres_defaults(self):
        self.assertTrue(_is_loopback_host("localhost"))
        self.assertTrue(_is_loopback_host("127.0.0.1"))
        self.assertFalse(_is_loopback_host("db.example.org"))


if __name__ == "__main__":
    unittest.main()
