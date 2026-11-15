"""scripts/verify_campaign_store.py must not pass when it verified nothing
(audit M13). Everything here is a synthetic store in a temp directory."""
import contextlib
import hashlib
import importlib.util
import io
import json
import os
import tempfile
import unittest
from unittest import mock

_SCRIPT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "scripts", "verify_campaign_store.py")
_spec = importlib.util.spec_from_file_location(
    "verify_campaign_store_under_test", _SCRIPT)
verifier = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(verifier)

EXAMPLE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "examples", "container-receipt.example.json")


class _Store(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = tmp.name
        self.chunk_dir = os.path.join(self.root, "chunk_000007")
        os.makedirs(self.chunk_dir)
        self.tar_bytes = b"synthetic stored-tar bytes " * 64
        self.sidecar_bytes = b"synthetic sidecar bytes"

    def _good_receipt(self):
        return {
            "schema_version": 1, "chunk_index": 7, "session_id": 2,
            "missing_members": [],
            "expected_totals": {"member_count": 3, "logical_bytes": 1000},
            "observed_totals": {"member_count": 3, "logical_bytes": 1000},
            "containers": [{
                "container_ordinal": 0,
                "tar_locator": "chunk_000007/container_0000.tar",
                "tar_size": len(self.tar_bytes),
                "tar_sha256": hashlib.sha256(self.tar_bytes).hexdigest(),
                "sidecar_locator":
                    "tar_sidecars/session_000002/chunk_000007/"
                    "container_0000.jsonl.zst",
                "sidecar_size": len(self.sidecar_bytes),
                "sidecar_sha256":
                    hashlib.sha256(self.sidecar_bytes).hexdigest(),
                "member_count": 3, "logical_bytes": 1000,
            }],
        }

    def _write(self, receipt, *, tar=True):
        with open(os.path.join(self.chunk_dir, "receipt.json"), "w",
                  encoding="utf-8") as handle:
            json.dump(receipt, handle)
        if tar:
            with open(os.path.join(self.chunk_dir, "container_0000.tar"),
                      "wb") as handle:
                handle.write(self.tar_bytes)

    def _write_sidecar(self, base, data=None):
        path = os.path.join(base, "tar_sidecars", "session_000002",
                            "chunk_000007", "container_0000.jsonl.zst")
        os.makedirs(os.path.dirname(path))
        with open(path, "wb") as handle:
            handle.write(self.sidecar_bytes if data is None else data)
        return path

    def _run(self, *extra, mode="full"):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = verifier.main(["--root", self.root, "--mode", mode, *extra])
        return code, out.getvalue() + err.getvalue()

    def _chunk(self, **kw):
        kw.setdefault("full", True)
        kw.setdefault("already_ok", set())
        return verifier.verify_chunk(self.root, 7, self.chunk_dir, **kw)


class NothingToVerifyTests(_Store):
    def test_nearly_empty_receipt_fails(self):
        self._write({"schema_version": 1}, tar=False)
        code, text = self._run()
        self.assertNotEqual(code, 0)
        self.assertIn("FAIL", text)
        self.assertNotIn("  OK  ", text)
        self.assertIn("ok=0", text)

    def test_zero_containers_fails(self):
        for containers in ([], None, {}, "x"):
            with self.subTest(containers=containers):
                receipt = self._good_receipt()
                receipt["containers"] = containers
                self._write(receipt, tar=False)
                result = self._chunk()
                self.assertEqual(result["status"], "fail")
                self.assertIn("container", result["detail"])

    def test_missing_required_top_level_fields_fail(self):
        for field in ("chunk_index", "session_id", "containers",
                      "expected_totals", "observed_totals"):
            with self.subTest(field=field):
                receipt = self._good_receipt()
                receipt.pop(field)
                self._write(receipt)
                result = self._chunk()
                self.assertEqual(result["status"], "fail")
                self.assertIn(field, result["detail"])

    def test_missing_required_container_fields_fail(self):
        for field in ("container_ordinal", "tar_locator", "tar_size",
                      "tar_sha256", "member_count", "logical_bytes"):
            for mode_full in (False, True):
                with self.subTest(field=field, full=mode_full):
                    receipt = self._good_receipt()
                    receipt["containers"][0].pop(field)
                    self._write(receipt)
                    result = self._chunk(full=mode_full)
                    self.assertEqual(result["status"], "fail")
                    self.assertIn(field, result["detail"])

    def test_malformed_checksum_fails_even_in_structure_mode(self):
        receipt = self._good_receipt()
        receipt["containers"][0]["tar_sha256"] = "00"
        self._write(receipt)
        self.assertEqual(self._chunk(full=False)["status"], "fail")

    def test_totals_without_counts_fail(self):
        receipt = self._good_receipt()
        receipt["expected_totals"] = {}
        receipt["observed_totals"] = {}
        self._write(receipt)
        result = self._chunk()
        self.assertEqual(result["status"], "fail")
        self.assertIn("expected_totals", result["detail"])

    def test_container_entry_that_is_not_an_object_fails(self):
        receipt = self._good_receipt()
        receipt["containers"] = ["container_0000.tar"]
        self._write(receipt)
        self.assertEqual(self._chunk()["status"], "fail")

    def test_receipt_that_is_not_an_object_fails(self):
        self._write([], tar=False)
        self.assertEqual(self._chunk()["status"], "fail")

    def test_run_that_verified_no_container_does_not_exit_zero(self):
        self._write({"schema_version": 1}, tar=False)
        code, _text = self._run(mode="structure")
        self.assertNotEqual(code, 0)


class MissingFileTests(_Store):
    def test_missing_container_fails(self):
        self._write(self._good_receipt(), tar=False)
        code, text = self._run()
        self.assertNotEqual(code, 0)
        self.assertIn("container_0000.tar", text)

    def test_required_sidecar_that_does_not_exist_fails(self):
        self._write(self._good_receipt())
        sidecars = os.path.join(self.root, "_metadata")
        os.makedirs(sidecars)
        code, text = self._run("--sidecar-root", sidecars)
        self.assertNotEqual(code, 0)
        self.assertIn("sidecar", text)

    def test_sidecar_inside_the_store_is_verified(self):
        self._write(self._good_receipt())
        self._write_sidecar(self.root, data=b"truncated")
        result = self._chunk()
        self.assertEqual(result["status"], "fail")
        self.assertIn("sidecar", result["detail"])

    def test_sidecar_checksum_is_verified_in_full_mode(self):
        self._write(self._good_receipt())
        wrong = bytes(len(self.sidecar_bytes))
        self._write_sidecar(self.root, data=wrong)
        self.assertEqual(self._chunk(full=False)["status"], "ok")
        result = self._chunk(full=True)
        self.assertEqual(result["status"], "fail")
        self.assertIn("sidecar", result["detail"])

    def test_unchecked_sidecar_is_reported_not_silently_passed(self):
        # Sidecars normally live outside the container store. That is not a
        # failure, but the output must say they were not verified.
        self._write(self._good_receipt())
        code, text = self._run()
        self.assertEqual(code, 0)
        self.assertIn("sidecar", text.lower())
        self.assertIn("not checked", text.lower())
        result = self._chunk()
        self.assertEqual(result["sidecars_not_checked"], 1)
        self.assertEqual(result["sidecars_verified"], 0)


class StillPassesTests(_Store):
    def test_intact_store_passes_in_both_modes(self):
        self._write(self._good_receipt())
        sidecars = os.path.join(self.root, "_metadata")
        self._write_sidecar(sidecars)
        for mode in ("structure", "full"):
            with self.subTest(mode=mode):
                code, text = self._run(
                    "--sidecar-root", sidecars, mode=mode)
                self.assertEqual(code, 0, text)
                self.assertIn("ok=1", text)

    def test_structure_mode_says_checksums_were_not_checked(self):
        self._write(self._good_receipt())
        code, text = self._run(mode="structure")
        self.assertEqual(code, 0)
        self.assertIn("SHA-256 was NOT checked", text)

    def test_corrupt_container_still_fails_in_full_mode(self):
        self._write(self._good_receipt())
        with open(os.path.join(self.chunk_dir, "container_0000.tar"),
                  "r+b") as handle:
            handle.write(b"X")
        self.assertEqual(self._chunk(full=False)["status"], "ok")
        self.assertEqual(self._chunk(full=True)["status"], "fail")

    def test_published_example_receipt_has_every_required_field(self):
        with open(EXAMPLE, encoding="utf-8") as handle:
            receipt = json.load(handle)
        self.assertEqual(verifier._receipt_shape_problems(receipt), [])
        for container in receipt["containers"]:
            self.assertEqual(
                verifier._container_shape_problems(container), [])


class ResumeTrustTests(_Store):
    """review F2: --resume may skip a re-hash only on recorded evidence."""

    def setUp(self):
        super().setUp()
        self._write(self._good_receipt())
        self.tar = os.path.join(self.chunk_dir, "container_0000.tar")
        self.report = os.path.join(self.root, "report.json")

    def _full(self, *extra):
        return self._run("--report", self.report, *extra)

    def _same_size_corruption(self, keep_mtime=False):
        before = os.stat(self.tar)
        with open(self.tar, "r+b") as handle:
            handle.write(b"X")
        if keep_mtime:
            os.utime(self.tar, ns=(before.st_atime_ns, before.st_mtime_ns))
        else:
            os.utime(self.tar, ns=(before.st_atime_ns,
                                   before.st_mtime_ns + 5_000_000_000))

    def test_changed_container_is_rehashed_on_resume(self):
        self.assertEqual(self._full()[0], 0)
        self._same_size_corruption()
        code, text = self._full("--resume")
        self.assertNotEqual(code, 0)
        self.assertIn("SHA-256 mismatch", text)

    def test_unchanged_container_is_trusted_on_resume(self):
        self.assertEqual(self._full()[0], 0)
        with mock.patch.object(
                verifier, "_sha256",
                side_effect=AssertionError("must not re-hash")):
            code, text = self._full("--resume")
        self.assertEqual(code, 0, text)
        # ...and the evidence survives into the new report for the next run.
        with mock.patch.object(
                verifier, "_sha256",
                side_effect=AssertionError("must not re-hash")):
            self.assertEqual(self._full("--resume")[0], 0)

    def test_receipt_checksum_changed_since_the_report_forces_a_rehash(self):
        self.assertEqual(self._full()[0], 0)
        receipt = self._good_receipt()
        receipt["containers"][0]["tar_sha256"] = "0" * 64
        self._write(receipt, tar=False)
        code, text = self._full("--resume")
        self.assertNotEqual(code, 0)
        self.assertIn("SHA-256 mismatch", text)

    def test_old_report_without_evidence_forces_a_rehash(self):
        self.assertEqual(self._full()[0], 0)
        with open(self.report, encoding="utf-8") as handle:
            payload = json.load(handle)
        for chunk in payload["chunks"]:
            for container in chunk["containers"]:
                for key in ("size", "mtime_ns", "tar_sha256"):
                    container.pop(key, None)
        with open(self.report, "w", encoding="utf-8") as handle:
            json.dump(payload, handle)
        self._same_size_corruption(keep_mtime=True)
        code, text = self._full("--resume")
        self.assertNotEqual(code, 0)
        self.assertIn("SHA-256 mismatch", text)


class RegularFileTests(_Store):
    """review F3: a receipted container must be a regular file."""

    def test_directory_in_place_of_a_container_fails(self):
        os.makedirs(os.path.join(self.chunk_dir, "container_0000.tar"))
        receipt = self._good_receipt()
        receipt["containers"][0]["tar_size"] = os.path.getsize(
            os.path.join(self.chunk_dir, "container_0000.tar"))
        self._write(receipt, tar=False)
        for full in (False, True):
            with self.subTest(full=full):
                result = self._chunk(full=full)
                self.assertEqual(result["status"], "fail")
                self.assertIn("not a regular file", result["detail"])
                self.assertEqual(result["bytes_verified"], 0)


if __name__ == "__main__":
    unittest.main()
