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


class PackDirOwnershipTests(unittest.TestCase):
    """Ownership recovered from the on-tape ``_pack_sNNNN_NNN`` bundle path.

    Legacy remote rows carry no ownership column anywhere, but the tape path
    written by RemoteOrchestrator still names the owning session and chunk.
    """

    class _Conn:
        @staticmethod
        def execute(_sql, _params):
            class _Result:
                @staticmethod
                def fetchone():
                    return (1,)
            return _Result()

    def test_pattern_matches_remote_pack_dir_and_captures_session_and_chunk(self):
        import re
        pattern = archive._PACK_DIR_OWNERSHIP_RE
        match = re.search(pattern, r"E:\_pack_s0034_000\Bundle_001.zip")
        self.assertIsNotNone(match)
        self.assertEqual(match.group(1), "0034")
        self.assertEqual(match.group(2), "000")

    def test_pattern_ignores_local_session_dirs(self):
        import re
        pattern = archive._PACK_DIR_OWNERSHIP_RE
        self.assertIsNone(
            re.search(pattern, r"E:\_local_s0034_c001_b001\Bundle_001.zip"))

    def test_pattern_requires_whole_path_components(self):
        import re
        pattern = archive._PACK_DIR_OWNERSHIP_RE
        for path in (r"E:\not_pack_s0034_000\B.zip",
                     r"E:\_pack_s34_0\B.zip",
                     r"E:\_pack_s0034_000_extra\B.zip"):
            self.assertIsNone(re.search(pattern, path), path)

    def test_derivation_is_last_resort_and_never_overrides_stored_ownership(self):
        sql = archive._classification_cte_for_connection(self._Conn())
        self.assertIn("pack_dir_ownership AS MATERIALIZED", sql)
        self.assertIn("LEFT JOIN pack_dir_ownership pdo", sql)
        # Recorded ownership always wins; the derived value is the final
        # COALESCE fallback, so a row that already knows its session is
        # unaffected by the tape path.
        self.assertIn(
            "COALESCE(f.remote_session_id, ar.remote_session_id, "
            "bo.bundle_remote_session_id, pdo.pack_remote_session_id)", sql)

    def test_derived_chunk_feeds_the_terminal_chunk_gate(self):
        sql = archive._classification_cte_for_connection(self._Conn())
        self.assertIn(
            "COALESCE(f.remote_chunk_index, pdo.pack_remote_chunk_index)", sql)
        # The derived chunk must still be checked for terminal status.
        self.assertIn("owner_remote_chunk_terminal IS DISTINCT FROM TRUE", sql)

    def test_derivation_absent_without_directory_archive_bundles(self):
        class _NoTable:
            @staticmethod
            def execute(_sql, _params):
                class _Result:
                    @staticmethod
                    def fetchone():
                        return None
                return _Result()

        sql = archive._classification_cte_for_connection(_NoTable())
        self.assertNotIn("pack_dir_ownership", sql)

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
