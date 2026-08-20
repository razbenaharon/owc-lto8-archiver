"""Invariants for the manifest-backed inventory architecture.

The live end-to-end checks run in ``scripts/validate_archive_reconciliation.py``
against the real catalog; these tests pin the pure logic it depends on:
segment parsing is exact, and a tape marked ``full`` (Tape_01/Tape_02, the
closed production tapes) can never be offered write budget.
"""
import importlib.util
import json
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.constants import tape_budget_bytes, tape_is_full  # noqa: E402

try:
    import zstandard as zstd
except ImportError:  # pragma: no cover
    zstd = None

_SCRIPT = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                       "scripts", "validate_archive_reconciliation.py")
_SPEC = importlib.util.spec_from_file_location(
    "validate_archive_reconciliation", _SCRIPT)
recon = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(recon)


class ClosedTapeProtectionTests(unittest.TestCase):
    """A closed production tape must refuse writes through the budget path."""

    def test_full_tape_reports_zero_available_bytes(self):
        capacity, available = tape_budget_bytes(12288, 0, status="full")
        self.assertEqual(available, 0)
        self.assertGreater(capacity, 0)

    def test_full_tape_zero_budget_even_when_counters_look_empty(self):
        # recalculate_tape_used_space rewrites used_space from the catalog on
        # every mount, so byte counters can never express retirement — only
        # status='full' survives. Zero used bytes must still yield zero budget.
        _, available = tape_budget_bytes(12288, 0, reserved_bytes=0,
                                         status="FULL")
        self.assertEqual(available, 0)
        self.assertTrue(tape_is_full("full"))
        self.assertTrue(tape_is_full(" Full "))

    def test_active_tape_keeps_budget(self):
        _, available = tape_budget_bytes(12288, 0, status="active")
        self.assertGreater(available, 0)


@unittest.skipIf(zstd is None, "zstandard not installed")
class SegmentTotalsTests(unittest.TestCase):
    """The reconciliation parser must count exactly what a segment holds."""

    def _write_segment(self, path, records):
        with open(path, "wb") as raw:
            with zstd.ZstdCompressor(level=3).stream_writer(raw) as writer:
                for record in records:
                    writer.write((json.dumps(record) + "\n").encode("utf-8"))

    def test_counts_rows_bytes_and_directories(self):
        import tempfile
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, "bundle_1.jsonl.zst")
            self._write_segment(path, [
                {"original_path": "/data/a/x.bin", "file_size_bytes": 10},
                {"original_path": "/data/a/y.bin", "file_size_bytes": 20},
                {"original_path": "/data/b/z.bin", "file_size_bytes": 5},
                {"original_path": "rootfile.bin", "file_size_bytes": 1},
            ])
            rows, total, dirs = recon._segment_totals(path)
            self.assertEqual(rows, 4)
            self.assertEqual(total, 36)
            self.assertEqual(dirs, {"/data/a", "/data/b", "ROOT"})

    def test_heavy_mode_rejects_sha_mismatch(self):
        import tempfile
        with tempfile.TemporaryDirectory() as root:
            path = os.path.join(root, "bundle_2.jsonl.zst")
            self._write_segment(path, [
                {"original_path": "/d/f", "file_size_bytes": 1}])
            with self.assertRaises(RuntimeError):
                recon._segment_totals(path, heavy=True, expected_sha="0" * 64)


if __name__ == "__main__":
    unittest.main()
