import inspect
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest import mock

from src import cold_migration
from src.catalog_query import escape_like_literal


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


class ColdSearchEscapingTests(unittest.TestCase):
    """search_cold's needle must escape LIKE metacharacters (same rule as
    the hot catalog search) so a literal '_'/'%' in a query matches itself
    instead of acting as a wildcard."""

    def test_needle_building_escapes_underscore_and_percent(self):
        needle = f"%{escape_like_literal('a_b%c')}%"
        self.assertEqual(needle, "%a\\_b\\%c%")

    def test_search_cold_uses_escape_like_literal_and_no_ddl(self):
        source = inspect.getsource(cold_migration.search_cold)
        self.assertIn("escape_like_literal", source)
        self.assertIn("ESCAPE '\\\\'", source)
        # Read path must never run DDL (CREATE/ALTER) against the payload
        # table; it should raise instead when the table is missing.
        self.assertNotIn("CREATE TABLE", source)
        self.assertIn("raise RuntimeError", source)


class _StubCursorResult:
    def __init__(self, row=None):
        self._row = row

    def fetchone(self):
        return self._row


class _StubConnNoMigrationsTable(object):
    """Minimal psycopg-connection stand-in: information_schema lookups (and
    everything else) return no row, simulating a fresh DB with no migration
    tables yet."""

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        return _StubCursorResult(row=None)


class MigrationStatusMissingTableTests(unittest.TestCase):
    """migration_status/migration_info must be read-only: return []/None
    instead of running DDL when small_file_cold_migrations does not exist."""

    def test_migration_status_returns_empty_list_when_table_absent(self):
        with mock.patch.object(
                cold_migration, "_connect",
                return_value=_StubConnNoMigrationsTable()):
            result = cold_migration.migration_status("fake-conninfo")
        self.assertEqual(result, [])

    def test_migration_info_returns_none_when_table_absent(self):
        with mock.patch.object(
                cold_migration, "_connect",
                return_value=_StubConnNoMigrationsTable()):
            result = cold_migration.migration_info("fake-conninfo", 1)
        self.assertIsNone(result)


class PurgeFailedMigrationPayloadTests(unittest.TestCase):
    """purge_failed_migration_payload must delete cold payload rows left by
    FAILED hot migrations and report how many rows were purged."""

    def test_purge_deletes_payload_rows_for_failed_migrations(self):
        class _HotStub:
            def execute(self, sql, params=None):
                return SimpleNamespace(
                    fetchall=lambda: [{'migration_id': 7}])

        class _ColdStub:
            def __init__(self):
                self.calls = []

            def execute(self, sql, params=None):
                self.calls.append((sql, params))
                return SimpleNamespace(rowcount=3)

        hot = _HotStub()
        cold = _ColdStub()
        purged = cold_migration.purge_failed_migration_payload(hot, cold)

        self.assertEqual(purged, 3)
        self.assertEqual(len(cold.calls), 2)
        delete_sql, delete_params = cold.calls[0]
        self.assertIn("DELETE FROM", delete_sql)
        self.assertEqual(delete_params, ([7],))

    def test_purge_returns_zero_and_skips_cold_when_no_failed_migrations(self):
        class _HotStub:
            def execute(self, sql, params=None):
                return SimpleNamespace(fetchall=lambda: [])

        class _ColdStub:
            def execute(self, sql, params=None):
                raise AssertionError(
                    "cold.execute should not run when there are no failed "
                    "migrations")

        purged = cold_migration.purge_failed_migration_payload(
            _HotStub(), _ColdStub())
        self.assertEqual(purged, 0)


if __name__ == "__main__":
    unittest.main()
