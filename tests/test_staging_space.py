import os
import tempfile
import unittest
from collections import namedtuple

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
