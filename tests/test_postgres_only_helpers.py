import unittest
import json
import os
import tempfile
from datetime import datetime, timezone
from types import SimpleNamespace

import zstandard as zstd

from src.catalog_v3 import catalog_directory_chain, catalog_file_name
from src.db import DatabaseManager, _apply_canonical_remote_paths, _file_record_key
from src.container_catalog import TarCatalogAdapter
from src.inspector_repository import InspectorRepository
from src.pg_db import _canonical_remote_path, _coerce_timestamptz, _now_utc
from src.pipeline_types import ArtifactKind, ContainerFormat
from src.stored_tar_planning import (StoredTarChunkPlan,
                                     StoredTarContainerPlan,
                                     StoredTarPlanMember)
from inspect_db import _DbOverrideConfig


class PostgresOnlyHelperTests(unittest.TestCase):
    def _tar_adapter_fixture(self, root):
        sidecar = os.path.join(root, "c0.jsonl.zst")
        records = [
            {"record_type": "header", "version": "tar-sidecar-v1",
             "session_id": 7, "chunk_index": 3, "container_id": 11,
             "container_ordinal": 0, "tar_size_bytes": 4096},
            {"record_type": "member", "member_name": "project/a.txt",
             "canonical_source_path": "/remote/project/a.txt",
             "logical_size": 5, "observed_archived_size": 5,
             "plan_ordinal": 10},
            {"record_type": "footer", "member_count": 1,
             "logical_bytes": 5, "tar_size_bytes": 4096},
        ]
        with open(sidecar, "wb") as raw:
            with zstd.ZstdCompressor().stream_writer(raw) as writer:
                for record in records:
                    writer.write((json.dumps(record) + "\n").encode("utf-8"))
        plan = StoredTarChunkPlan(
            session_id=7, chunk_index=3, max_size_bytes=10_000,
            containers=(StoredTarContainerPlan(
                session_id=7, chunk_index=3, container_ordinal=0,
                container_name="c0.tar", expected_member_count=1,
                expected_logical_bytes=5, estimated_archive_bytes=4096,
                max_size_bytes=10_000),),
            members=(StoredTarPlanMember(
                manifest_id=1, plan_ordinal=10,
                remote_path="/remote/project/a.txt", file_size_bytes=5,
                storage_class="small_files", container_ordinal=0),))
        packed = {
            "is_packed": True, "container_id": 11,
            "container_format": "stored_tar", "container_ordinal": 0,
            "canonical_source_path": "/remote/project/a.txt",
            "original_path": "/remote/project/a.txt",
            "file_size_bytes": 5, "catalog_policy": "manifest_only",
        }
        loose = {"is_packed": False, "stored_path": "large.bin",
                 "canonical_source_path": "/remote/large.bin",
                 "original_path": "/remote/large.bin",
                 "file_size_bytes": 20 * 1024 * 1024}
        chunk = SimpleNamespace(
            packaging_format=ContainerFormat.STORED_TAR, session_id=7,
            chunk_index=3, metadata=[packed, loose],
            containers=[SimpleNamespace(
                container_id=11, container_ordinal=0,
                container_name="c0.tar", expected_member_count=1,
                expected_logical_bytes=5, data_size_bytes=4096)],
            artifacts=[SimpleNamespace(
                artifact_id=12, container_id=11,
                artifact_kind=ArtifactKind.TAR_SIDECAR,
                artifact_version="tar-sidecar-v1", staged_path=sidecar,
                local_locator="tar_sidecars/s7/c3/c0.jsonl.zst")])
        db = SimpleNamespace(get_stored_tar_chunk_plan=lambda *_: plan)
        return db, chunk, packed, loose

    def test_tar_catalog_adapter_joins_sidecar_plan_and_passes_loose_record(self):
        with tempfile.TemporaryDirectory() as root:
            db, chunk, _packed, loose = self._tar_adapter_fixture(root)
            records = list(TarCatalogAdapter(
                db, chunk, index_min_file_mb=10,
                index_packed_small_files=False).records())
        self.assertEqual(records[0]["original_path"],
                         "/remote/project/a.txt")
        self.assertEqual(records[0]["stored_path"], "project/a.txt")
        self.assertEqual(records[0]["catalog_policy"], "manifest_only")
        self.assertEqual(records[0]["container_id"], 11)
        self.assertIs(records[1], loose)

    def test_tar_catalog_adapter_rejects_transient_or_wrong_source_path(self):
        with tempfile.TemporaryDirectory() as root:
            db, chunk, packed, _loose = self._tar_adapter_fixture(root)
            packed["canonical_source_path"] = os.path.join(root, "a.txt")
            packed["original_path"] = packed["canonical_source_path"]
            with self.assertRaisesRegex(RuntimeError, "canonical POSIX"):
                list(TarCatalogAdapter(db, chunk).records())

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
