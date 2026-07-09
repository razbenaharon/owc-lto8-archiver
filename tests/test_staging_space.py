import os
import json
import tempfile
import unittest
from collections import namedtuple
from unittest import mock

from src import packer
from src.skipped import SkippedFileTracker

try:
    import zstandard as zstd
except ImportError:  # pragma: no cover - dependency test environment issue
    zstd = None


class StagingSpaceTests(unittest.TestCase):
    def test_ensure_staging_space_uses_current_free_space(self):
        usage = namedtuple("usage", "total used free")
        original_disk_usage = packer.shutil.disk_usage
        gib = 1024**3
        try:
            packer.shutil.disk_usage = lambda path: usage(200 * gib, 0, 100 * gib)
            with tempfile.TemporaryDirectory() as tmp:
                free = packer.ensure_staging_space(
                    os.path.join(tmp, "staging"),
                    1 * gib,
                    context="test batch",
                )
            self.assertEqual(free, 100 * gib)
        finally:
            packer.shutil.disk_usage = original_disk_usage

    def test_packer_does_not_create_empty_zip_for_loose_only_batch(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = os.path.join(tmp, "source")
            dest = os.path.join(tmp, "staging")
            os.makedirs(source)
            large = os.path.join(source, "large.bin")
            with open(large, "wb") as handle:
                handle.write(b"x" * 20)
            with mock.patch("src.packer._robocopy_file", return_value=True):
                metadata = packer.LTOPacker(max_zip_size_gb=1).run(
                    source, dest, threshold_mb=0)
            assert metadata is not None
            self.assertEqual(len(metadata), 1)
            self.assertFalse(metadata[0]["is_packed"])
            self.assertNotIn("file_hash", metadata[0])
            self.assertEqual(
                [name for name in os.listdir(dest) if name.endswith(".zip")],
                [])

    def test_packer_metadata_is_hashless_for_zip_entries(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = os.path.join(tmp, "source")
            dest = os.path.join(tmp, "staging")
            os.makedirs(source)
            with open(os.path.join(source, "small.txt"), "wb") as handle:
                handle.write(b"hello")
            metadata = packer.LTOPacker(max_zip_size_gb=1).run(
                source, dest, threshold_mb=1)
            assert metadata is not None
            self.assertEqual(len(metadata), 1)
            self.assertTrue(metadata[0]["is_packed"])
            self.assertNotIn("file_hash", metadata[0])
            self.assertEqual(
                [name for name in os.listdir(dest) if name.endswith(".zip")],
                ["Bundle_001.zip"])
            self.assertEqual(metadata[0]["catalog_policy"], "manifest_only")

    @unittest.skipIf(zstd is None, "zstandard not installed")
    def test_packer_writes_zstd_jsonl_manifest_for_small_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            source = os.path.join(tmp, "source")
            dest = os.path.join(tmp, "staging")
            os.makedirs(os.path.join(source, "sub"))
            with open(os.path.join(source, "sub", "small.txt"), "wb") as handle:
                handle.write(b"hello")
            metadata = packer.LTOPacker(
                max_zip_size_gb=1,
                index_min_file_mb=10,
                manifest_compression="zstd",
            ).run(source, dest, threshold_mb=100)
            assert metadata is not None
            manifest = os.path.join(dest, "Bundle_001.manifest.jsonl.zst")
            self.assertTrue(os.path.exists(manifest))
            with open(manifest, "rb") as raw:
                reader = zstd.ZstdDecompressor().stream_reader(raw)
                text = reader.read().decode("utf-8")
            rows = [json.loads(line) for line in text.splitlines()]
            self.assertEqual(rows[0]["relative_path"], "sub/small.txt")
            self.assertEqual(rows[0]["size_bytes"], 5)

    def test_ensure_staging_space_rejects_unsafe_batch(self):
        usage = namedtuple("usage", "total used free")
        original_disk_usage = packer.shutil.disk_usage
        gib = 1024**3
        try:
            packer.shutil.disk_usage = lambda path: usage(200 * gib, 0, 21 * gib)
            with tempfile.TemporaryDirectory() as tmp:
                with self.assertRaises(packer.StagingSpaceError):
                    packer.ensure_staging_space(
                        os.path.join(tmp, "staging"),
                        1 * gib,
                        context="test batch",
                    )
        finally:
            packer.shutil.disk_usage = original_disk_usage

    def test_manifest_pack_tracks_missing_file_without_failing(self):
        with tempfile.TemporaryDirectory() as tmp:
            dest = os.path.join(tmp, "staging")
            missing = os.path.join(tmp, "missing.txt")
            tracker = SkippedFileTracker()
            metadata = packer.LTOPacker(max_zip_size_gb=1).run_manifest(
                source_root=tmp,
                dest=dest,
                threshold_mb=1,
                file_entries=[{"path": missing, "rel": "missing.txt", "size": 5}],
                skipped_tracker=tracker,
                source_name="local",
            )
            self.assertEqual(metadata, [])
            self.assertEqual(tracker.count(), 1)
            report = tracker.write_csv(tmp)
            assert report is not None
            with open(report, encoding="utf-8") as handle:
                text = handle.read()
            self.assertIn("missing.txt", text)
            self.assertIn("local", text)


if __name__ == "__main__":
    unittest.main()
