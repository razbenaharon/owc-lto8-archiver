import importlib.util
import os
import shutil
import sys
import tempfile
import unittest
from pathlib import Path


_SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "benchmark_stored_tar.py"
_SPEC = importlib.util.spec_from_file_location("benchmark_stored_tar", _SCRIPT)
benchmark = importlib.util.module_from_spec(_SPEC)
assert _SPEC.loader is not None
sys.modules[_SPEC.name] = benchmark
_SPEC.loader.exec_module(benchmark)


class StoredTarBenchmarkSmokeTests(unittest.TestCase):
    def test_small_profile_reports_required_metrics_and_cleans_workspaces(self):
        with tempfile.TemporaryDirectory() as tmp:
            summary = benchmark.run_benchmark(
                profiles=["small"],
                output_root=tmp,
                threshold_mb=64,
                max_zip_size_gb=1.0,
                keep_workspaces=False,
            )

            self.assertEqual(summary["version"], benchmark.HARNESS_VERSION)
            self.assertEqual(len(summary["profiles"]), 1)
            profile = summary["profiles"][0]
            self.assertTrue(profile["equivalent_membership"])

            expected = profile["expected_membership"]
            current = profile["current"]
            stored = profile["stored_tar"]

            self.assertEqual(current["membership"], expected)
            self.assertEqual(stored["membership"], expected)

            required_fields = {
                "file_count", "logical_bytes", "output_bytes", "wall_seconds",
                "cpu_seconds", "peak_rss_mb", "peak_staging_bytes",
                "final_staging_bytes", "peak_entry_count", "final_entry_count",
                "time_to_writer_ready_seconds", "status", "membership",
            }
            self.assertTrue(required_fields.issubset(current))
            self.assertTrue(required_fields.issubset(stored))
            self.assertEqual(current["status"], "ok")
            self.assertEqual(stored["status"], "ok")

            run_root = summary["run_root"]
            self.assertTrue(os.path.isfile(os.path.join(run_root, "summary.json")))
            self.assertTrue(os.path.isfile(os.path.join(run_root, "summary.md")))

            self.assertFalse(os.path.exists(os.path.join(run_root, "small", "current")))
            self.assertFalse(os.path.exists(os.path.join(run_root, "small", "stored_tar")))

    def test_sparse_profile_uses_same_logical_membership(self):
        with tempfile.TemporaryDirectory() as tmp:
            summary = benchmark.run_benchmark(
                profiles=["sparse"],
                output_root=tmp,
                threshold_mb=1024,
                max_zip_size_gb=1.0,
                keep_workspaces=False,
            )

            profile = summary["profiles"][0]
            self.assertEqual(profile["profile"], "sparse")
            self.assertTrue(profile["equivalent_membership"])
            self.assertEqual(len(profile["expected_membership"]), 1)
            self.assertEqual(
                profile["current"]["logical_bytes"],
                profile["stored_tar"]["logical_bytes"])

    def test_keep_workspaces_preserves_only_harness_owned_paths(self):
        with tempfile.TemporaryDirectory() as tmp:
            summary = benchmark.run_benchmark(
                profiles=["small"],
                output_root=tmp,
                threshold_mb=64,
                keep_workspaces=True,
            )
            run_root = summary["run_root"]
            self.assertTrue(os.path.isdir(os.path.join(run_root, "small", "current")))
            self.assertTrue(os.path.isdir(os.path.join(run_root, "small", "stored_tar")))
            shutil.rmtree(run_root, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
