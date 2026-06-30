import os
import tempfile
import unittest
from collections import namedtuple
from unittest import mock

from src import packer


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
            self.assertEqual(len(metadata), 1)
            self.assertTrue(metadata[0]["is_packed"])
            self.assertNotIn("file_hash", metadata[0])
            self.assertEqual(
                [name for name in os.listdir(dest) if name.endswith(".zip")],
                ["Bundle_001.zip"])

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


if __name__ == "__main__":
    unittest.main()
