import inspect
import os
import tempfile
import unittest

from src import local_manifest_archive as archive


class LocalManifestPathTests(unittest.TestCase):
    def test_archive_root_must_not_overlap_staging(self):
        with tempfile.TemporaryDirectory() as base:
            staging = os.path.join(base, "staging")
            inside = os.path.join(staging, "manifests")
            with self.assertRaisesRegex(RuntimeError, "separate"):
                archive.validate_archive_root(inside, (staging,))

    def test_archive_root_on_separate_tree_is_allowed(self):
        with tempfile.TemporaryDirectory() as base:
            root = os.path.join(base, "permanent")
            staging = os.path.join(base, "staging")
            self.assertEqual(
                archive.validate_archive_root(root, (staging,)),
                os.path.abspath(root))


class EligibilitySafetyTests(unittest.TestCase):
    def test_threshold_is_exactly_ten_mib(self):
        self.assertEqual(
            archive.SMALL_FILE_THRESHOLD_BYTES, 10 * 1024 * 1024)

    def test_classification_requires_terminal_sessions_and_chunks(self):
        sql = archive._classification_cte("FALSE")
        self.assertIn("session_ownership_unknown", sql)
        self.assertIn("local_status IS DISTINCT FROM 'completed'", sql)
        self.assertIn("local_chunk_state AS MATERIALIZED", sql)
        self.assertIn("owner_local_chunk_terminal IS DISTINCT FROM TRUE", sql)
        self.assertIn("remote_status IS DISTINCT FROM 'completed'", sql)
        self.assertIn("remote_chunk_state AS MATERIALIZED", sql)
        self.assertIn("owner_remote_chunk_terminal IS DISTINCT FROM TRUE", sql)

    def test_live_classifier_uses_one_set_based_bundle_ownership_scan(self):
        class _Result:
            @staticmethod
            def fetchone():
                return (1,)

        class _Conn:
            @staticmethod
            def execute(_sql, _params):
                return _Result()

        sql = archive._classification_cte_for_connection(_Conn())
        self.assertIn("bundle_ownership AS MATERIALIZED", sql)
        self.assertIn("LEFT JOIN bundle_ownership bo", sql)
        self.assertNotIn("FROM directory_archive_bundles dab\n        WHERE", sql)

    def test_prune_is_snapshot_identity_driven(self):
        source = inspect.getsource(archive.prune_export)
        self.assertIn("source_file_id", source)
        self.assertIn("source_record_key", source)
        self.assertIn("pg_try_advisory_xact_lock", inspect.getsource(
            archive._acquire_prune_lock))
        self.assertNotIn("file_size_bytes <", source)

    def test_prune_never_touches_operational_tables(self):
        source = inspect.getsource(archive.prune_export).lower()
        for table in (
            "remote_snapshot_files", "remote_plan_files", "remote_file_state",
            "remote_chunks", "remote_sessions", "local_chunks_manifest",
            "local_sessions",
        ):
            self.assertNotIn("delete from " + table, source)


class CompressedManifestTests(unittest.TestCase):
    def test_write_search_and_find_round_trip(self):
        rows = [{
            "source_file_id": 42,
            "source_record_key": "ab" * 32,
            "original_path": "/data/project/tiny.txt",
            "file_size_bytes": 7,
            "tape_label": "TAPE001",
            "source_host": "server1",
            "is_packed": True,
            "stored_path": "project/tiny.txt",
            "container_name": "E:/Bundle_1.zip",
            "file_name": "tiny.txt",
            "backup_date": "2026-07-01T00:00:00+00:00",
            "covered_by_directory_catalog": True,
        }]
        with tempfile.TemporaryDirectory() as root:
            result = archive._write_segment(
                root, "TAPE001/session_1/bundle_1.jsonl.zst", rows)
            self.assertEqual(result["row_count"], 1)
            self.assertEqual(result["covered_rows"], 1)
            matches = archive.search_manifests(root, "tiny*", limit=10)
            self.assertEqual(len(matches), 1)
            self.assertEqual(matches[0]["file_id"], "M:42")
            found = archive.find_manifest_record(root, "M:42")
            self.assertEqual(found["stored_path"], "project/tiny.txt")

    def test_search_round_trip_preserves_packed_and_unpacked_restore_fields(self):
        common = {
            "source_record_key": "ab" * 32,
            "tape_label": "TAPE001",
            "source_host": "server1",
            "backup_date": "2026-07-01T00:00:00+00:00",
            "covered_by_directory_catalog": True,
        }
        rows = [
            dict(common, source_file_id=42,
                 original_path="/data/project/packed.txt",
                 file_size_bytes=7, is_packed=True,
                 stored_path="project/packed.txt",
                 container_name="E:/Bundle_1.zip", file_name="packed.txt"),
            dict(common, source_file_id=43,
                 original_path="/data/project/loose.txt",
                 file_size_bytes=9, is_packed=False,
                 stored_path="E:/loose.txt", container_name=None,
                 file_name="loose.txt"),
        ]
        with tempfile.TemporaryDirectory() as root:
            archive._write_segment(
                root, "TAPE001/session_1/mixed.jsonl.zst", rows)
            found = archive.search_manifests(root, "*.txt", limit=10)
            self.assertEqual(len(found), 2)
            by_id = {item["file_id"]: item for item in found}
            self.assertTrue(by_id["M:42"]["is_packed"])
            self.assertEqual(
                by_id["M:42"]["container_name"], "E:/Bundle_1.zip")
            self.assertFalse(by_id["M:43"]["is_packed"])
            self.assertEqual(by_id["M:43"]["stored_path"], "E:/loose.txt")


if __name__ == "__main__":
    unittest.main()
