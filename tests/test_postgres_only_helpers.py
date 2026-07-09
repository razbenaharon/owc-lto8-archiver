import unittest
from datetime import datetime, timezone

from src.catalog_v3 import catalog_directory_chain, catalog_file_name
from src.db import DatabaseManager, _apply_canonical_remote_paths, _file_record_key
from src.inspector_repository import InspectorRepository
from src.pg_db import _canonical_remote_path, _coerce_timestamptz, _now_utc
from inspect_db import _DbOverrideConfig


class PostgresOnlyHelperTests(unittest.TestCase):
    def test_database_manager_direct_sqlite_constructor_is_removed(self):
        with self.assertRaisesRegex(RuntimeError, "SQLite DatabaseManager has been removed"):
            DatabaseManager("archive.db")

    def test_record_key_includes_source_host(self):
        left = _file_record_key("/data/a.bin", "TAPE001", source_host="so02")
        right = _file_record_key("/data/a.bin", "TAPE001", source_host="so03")
        self.assertNotEqual(left, right)
        self.assertEqual(len(left), 32)

    def test_remote_catalog_chain_uses_short_source_host_root(self):
        chain = catalog_directory_chain("/srv/project/file.dat", "so02.example")
        self.assertEqual(
            chain,
            [
                ("so02", None, "so02"),
                ("so02/srv", "so02", "srv"),
                ("so02/srv/project", "so02/srv", "project"),
            ],
        )

    def test_catalog_file_name_prefers_stored_path_leaf(self):
        self.assertEqual(
            catalog_file_name("Bundle_001.zip/path/to/a.txt", "/srv/original/b.txt"),
            "a.txt",
        )

    def test_apply_canonical_remote_paths_rejects_ambiguous_mapping(self):
        metadata = [{"stored_path": "safe/name.txt"}]
        rows = [
            {"local_rel_path": "safe/name.txt", "remote_path": "/a/name.txt"},
            {"local_rel_path": "safe/name.txt", "remote_path": "/b/name.txt"},
        ]
        with self.assertRaisesRegex(RuntimeError, "Ambiguous canonical source"):
            _apply_canonical_remote_paths(metadata, rows)

    def test_canonical_remote_path_folds_backslashes(self):
        # §1.2: snapshot rows and plan-file lookups must agree even when a
        # Linux filename legally contains a backslash.
        self.assertEqual(
            _canonical_remote_path("/data/weird\\name.txt"),
            "/data/weird/name.txt")
        self.assertEqual(
            _canonical_remote_path("/data/plain.txt"), "/data/plain.txt")

    def test_postgres_timestamps_are_timezone_aware_utc(self):
        now = _now_utc()
        self.assertIsNotNone(now.tzinfo)
        self.assertEqual(now.utcoffset(), timezone.utc.utcoffset(now))

    def test_session_timestamp_strings_are_coerced_to_utc(self):
        value = _coerce_timestamptz("2026-07-02T09:30:00")
        self.assertIsInstance(value, datetime)
        assert isinstance(value, datetime)
        self.assertIsNotNone(value.tzinfo)
        self.assertEqual(value.utcoffset(), timezone.utc.utcoffset(value))

    def test_inspector_sort_filters_use_psycopg_placeholders(self):
        cursor = {"catalog_name": "a.txt", "file_id": 10}
        _order, cursor_sql, _columns = InspectorRepository._sort_parts(
            "name", cursor)
        self.assertIsNotNone(cursor_sql)
        assert cursor_sql is not None
        self.assertIn("%s", cursor_sql[0])
        self.assertNotIn("?", cursor_sql[0])

    def test_inspect_db_override_changes_only_database_name(self):
        class Base:
            pg_host = "localhost"
            pg_port = "5432"
            pg_user = "lto"
            pg_password = "secret"
            pg_sslmode = "prefer"
            pg_dbname = "lto_archive"

        cfg = _DbOverrideConfig(Base(), "lto_archive_migrated")
        self.assertEqual(cfg.pg_dbname, "lto_archive_migrated")
        self.assertIn("/lto_archive_migrated?", cfg.db_dsn)
        self.assertIn("lto:***@", cfg.db_display_ref)


if __name__ == "__main__":
    unittest.main()
