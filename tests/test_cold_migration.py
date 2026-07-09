import inspect
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from src import cold_migration


class ColdMigrationSafetyTests(unittest.TestCase):
    def test_cold_payload_schema_text_has_no_payload_indexes_or_pk(self):
        source = inspect.getsource(cold_migration.ensure_cold_schema)
        payload = source.split("CREATE TABLE IF NOT EXISTS small_file_cold_loads")[0]
        self.assertIn("CREATE TABLE IF NOT EXISTS {COLD_PAYLOAD_TABLE}", payload)
        self.assertNotIn("PRIMARY KEY", payload)
        self.assertNotIn("CREATE INDEX", payload)
        self.assertNotIn("UNIQUE", payload)

    def test_hot_delete_path_does_not_delete_by_size_predicate(self):
        source = inspect.getsource(cold_migration.remove_migrated_hot_rows)
        self.assertNotIn("file_size_bytes <", source)
        self.assertIn("small_file_cold_migration_sources", source)
        self.assertIn("source_hot_row_id", source)
        self.assertIn("covered_by_hot_accounting", source)

    def test_threshold_bytes_uses_cold_config_value(self):
        cfg = SimpleNamespace(cold_small_file_threshold_mb=10)
        self.assertEqual(cold_migration.threshold_bytes(cfg), 10 * 1024 * 1024)

    def test_backup_must_be_after_validation(self):
        with tempfile.NamedTemporaryFile(delete=False) as handle:
            path = handle.name
        try:
            now = datetime.now(timezone.utc)
            os.utime(path, (now.timestamp(), now.timestamp()))
            self.assertTrue(
                cold_migration.backup_is_after_validation(
                    path, now - timedelta(seconds=1)))
            self.assertFalse(
                cold_migration.backup_is_after_validation(
                    path, now + timedelta(seconds=60)))
        finally:
            os.remove(path)


if __name__ == "__main__":
    unittest.main()
