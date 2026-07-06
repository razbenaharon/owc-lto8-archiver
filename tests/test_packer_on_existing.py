"""LTOPacker.run on_existing modes (review §1.3).

The remote pipeline's producer thread must never reach the packer's
interactive stdin prompt: 'clean' repacks a stale dest without prompting and
'reuse' keeps it. Disk-space checks are stubbed so the tests only exercise the
mode dispatch and packing outcome.
"""
import os
import shutil
import tempfile
import unittest
import zipfile

import src.packer as packer_mod
from src.packer import LTOPacker


class _NoopBudget:
    def __init__(self, *args, **kwargs):
        pass

    def refresh(self):
        return 0

    def consume(self, *args, **kwargs):
        pass


class PackerOnExistingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="lto_packer_test_")
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.source = os.path.join(self.tmp, "src")
        self.dest = os.path.join(self.tmp, "dest")
        os.makedirs(self.source)
        with open(os.path.join(self.source, "a.txt"), "w",
                  encoding="utf-8") as handle:
            handle.write("hello")
        self._orig_budget = packer_mod.StagingSpaceBudget
        packer_mod.StagingSpaceBudget = _NoopBudget
        self.addCleanup(
            setattr, packer_mod, "StagingSpaceBudget", self._orig_budget)

    def test_on_existing_clean_repacks_without_stdin(self):
        os.makedirs(self.dest)
        with open(os.path.join(self.dest, "stale.bin"), "w",
                  encoding="utf-8") as handle:
            handle.write("stale")

        metadata = LTOPacker(max_zip_size_gb=1).run(
            source=self.source, dest=self.dest, threshold_mb=100,
            on_existing="clean")

        self.assertFalse(os.path.exists(os.path.join(self.dest, "stale.bin")))
        self.assertEqual([m["file_name"] for m in metadata], ["a.txt"])
        bundle = os.path.join(self.dest, metadata[0]["container_name"])
        with zipfile.ZipFile(bundle) as zf:
            self.assertEqual(zf.read("a.txt").decode("utf-8"), "hello")

    def test_on_existing_reuse_returns_empty_metadata(self):
        os.makedirs(self.dest)
        with open(os.path.join(self.dest, "existing.zip"), "w",
                  encoding="utf-8") as handle:
            handle.write("x")

        result = LTOPacker(max_zip_size_gb=1).run(
            source=self.source, dest=self.dest, threshold_mb=100,
            on_existing="reuse")

        self.assertEqual(result, [])
        self.assertTrue(os.path.exists(os.path.join(self.dest, "existing.zip")))


if __name__ == "__main__":
    unittest.main()
