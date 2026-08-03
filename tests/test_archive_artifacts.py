"""Plan 1 / Task 2.2 — local scan-segment artifacts.

A segment artifact is what lets a restart know a directory was already covered
without re-listing it. That makes its failure modes safety-relevant, so the
tests below concentrate on them:

* an interrupted write leaves a ``.part``, never a truncated "ready" file;
* publication is atomic and refuses to clobber;
* a reader validates the declared aggregates against the actual contents and
  refuses a truncated or version-mismatched file;
* locators are relative and cannot escape the archive root.

Everything here is a temporary directory: no staging, no LTFS, no PostgreSQL.
"""
import json
import os
import tempfile
import unittest
from unittest import mock

from src.archive_artifacts import (ARTIFACT_NAMESPACE, ARTIFACT_VERSION,
                                   ArtifactConflict, ArtifactError,
                                   JsonlZstArtifactWriter, artifact_root,
                                   find_orphan_parts,
                                   parse_jsonl_zst_artifact, resolve_locator,
                                   segment_locator)


class _Tmp(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.root = self._dir.name
        self.addCleanup(self._dir.cleanup)
        self.locator = segment_locator(37, 5, 0)

    def _writer(self, locator=None, **kwargs):
        return JsonlZstArtifactWriter(
            self.root, locator or self.locator, session_id=37,
            scan_directory_id=5, scope="/vault/a", **kwargs)

    def _write(self, entries, locator=None):
        with self._writer(locator) as writer:
            for path, size, ordinal in entries:
                writer.add(path=path, size=size, ordinal=ordinal)
        return writer


# =============================================================================
# A. Locators
# =============================================================================
class LocatorTests(_Tmp):
    def test_a_locator_is_relative_and_namespaced(self):
        locator = segment_locator(37, 5, 0)
        self.assertTrue(locator.startswith(f"{ARTIFACT_NAMESPACE}/"))
        self.assertFalse(os.path.isabs(locator))
        self.assertTrue(locator.endswith(".jsonl.zst"))

    def test_locators_are_stable_and_collision_free(self):
        self.assertEqual(segment_locator(37, 5, 0), segment_locator(37, 5, 0))
        distinct = {segment_locator(37, 5, 0), segment_locator(37, 5, 1),
                    segment_locator(37, 6, 0), segment_locator(38, 5, 0)}
        self.assertEqual(len(distinct), 4)

    def test_the_namespace_is_separate_from_manifest_exports(self):
        self.assertEqual(os.path.basename(artifact_root(self.root)),
                         ARTIFACT_NAMESPACE)

    def test_an_escaping_locator_is_refused(self):
        for bad in ("../outside.zst", "/absolute/x.zst", "C:\\x.zst",
                    "scan_segments/../../x.zst", "", "   "):
            with self.assertRaises(ArtifactError, msg=bad):
                resolve_locator(self.root, bad)

    def test_a_contained_locator_resolves_under_the_root(self):
        resolved = resolve_locator(self.root, self.locator)
        self.assertTrue(resolved.startswith(os.path.abspath(self.root)))


# =============================================================================
# B. Writing and publishing
# =============================================================================
class WriterTests(_Tmp):
    def test_a_clean_write_publishes_and_reports_aggregates(self):
        writer = self._write([("/vault/a/f0", 10, 0), ("/vault/a/f1", 20, 1)])
        self.assertTrue(writer.published)
        self.assertEqual(writer.file_count, 2)
        self.assertEqual(writer.byte_count, 30)
        self.assertEqual((writer.first_ordinal, writer.last_ordinal), (0, 1))
        self.assertEqual(writer.first_path, "/vault/a/f0")
        self.assertEqual(writer.last_path, "/vault/a/f1")
        self.assertTrue(os.path.exists(resolve_locator(self.root,
                                                       self.locator)))

    def test_the_part_file_is_unique_per_attempt(self):
        first = self._writer()
        second = self._writer()
        self.assertNotEqual(first.part_path, second.part_path)
        self.assertTrue(first.part_path.endswith(".part"))

    def test_nothing_final_exists_until_publication(self):
        writer = self._writer().open()
        writer.add(path="/vault/a/f", size=1, ordinal=0)
        writer.flush()
        final = resolve_locator(self.root, self.locator)
        self.assertFalse(os.path.exists(final))
        self.assertTrue(os.path.exists(writer.part_path))
        writer.close(publish=True)
        self.assertTrue(os.path.exists(final))

    def test_an_interrupted_write_leaves_a_part_not_a_ready_file(self):
        try:
            with self._writer() as writer:
                writer.add(path="/vault/a/f", size=1, ordinal=0)
                raise RuntimeError("ssh died mid-listing")
        except RuntimeError:
            pass
        self.assertFalse(writer.published)
        self.assertFalse(os.path.exists(resolve_locator(self.root,
                                                        self.locator)))
        self.assertEqual(find_orphan_parts(self.root), [writer.part_path])

    def test_publication_refuses_to_clobber(self):
        self._write([("/vault/a/f", 10, 0)])
        second = self._writer().open()
        second.add(path="/vault/a/other", size=99, ordinal=0)
        with self.assertRaises(ArtifactConflict) as caught:
            second.close(publish=True)
        self.assertIn("reconcile the frontier", str(caught.exception))
        # The original is untouched.
        _header, entries, _totals = parse_jsonl_zst_artifact(
            self.root, self.locator)
        self.assertEqual([e["path"] for e in entries], ["/vault/a/f"])

    def test_publishing_twice_is_idempotent(self):
        writer = self._write([("/vault/a/f", 10, 0)])
        self.assertTrue(writer.publish())

    def test_publishing_without_a_part_is_an_error(self):
        writer = self._writer().open()
        writer.close(publish=False)
        os.remove(writer.part_path)
        with self.assertRaises(ArtifactError):
            writer.publish()

    def test_abandon_removes_only_the_unpublished_attempt(self):
        writer = self._writer().open()
        writer.add(path="/vault/a/f", size=1, ordinal=0)
        writer.abandon()
        self.assertEqual(find_orphan_parts(self.root), [])
        self.assertFalse(os.path.exists(resolve_locator(self.root,
                                                        self.locator)))

    def test_ordinals_must_ascend(self):
        writer = self._writer().open()
        writer.add(path="/vault/a/f0", size=1, ordinal=5)
        for bad in (5, 4, 0):
            with self.assertRaises(ArtifactError, msg=str(bad)):
                writer.add(path="/vault/a/x", size=1, ordinal=bad)
        writer.abandon()

    def test_an_empty_segment_still_publishes(self):
        """A directory with no files is a real, coverable fact."""
        with self._writer() as writer:
            pass
        self.assertTrue(writer.published)
        _header, entries, totals = parse_jsonl_zst_artifact(
            self.root, self.locator)
        self.assertEqual(entries, [])
        self.assertEqual(totals["file_count"], 0)
        self.assertIsNone(totals["first_scan_ordinal"])

    def test_writing_without_zstandard_is_an_explicit_error(self):
        import src.archive_artifacts as artifacts
        with mock.patch.object(artifacts, "zstd", None), \
                self.assertRaises(ArtifactError) as caught:
            artifacts.JsonlZstArtifactWriter(self.root, self.locator)
        self.assertIn("zstandard is required", str(caught.exception))


# =============================================================================
# C. Reading back
# =============================================================================
class ReaderTests(_Tmp):
    def test_a_published_segment_reconstructs_its_exact_ordered_list(self):
        entries = [(f"/vault/a/f{i}", i * 10, i) for i in range(50)]
        self._write(entries)
        header, parsed, totals = parse_jsonl_zst_artifact(self.root,
                                                          self.locator)
        self.assertEqual(header["version"], ARTIFACT_VERSION)
        self.assertEqual(header["scan_directory_id"], 5)
        self.assertEqual(header["scope"], "/vault/a")
        self.assertEqual([(e["path"], e["size"], e["ordinal"]) for e in parsed],
                         entries)
        self.assertEqual(totals["file_count"], 50)
        self.assertEqual(totals["byte_count"], sum(i * 10 for i in range(50)))
        self.assertEqual((totals["first_scan_ordinal"],
                          totals["last_scan_ordinal"]), (0, 49))

    def test_it_carries_no_content_hash(self):
        self._write([("/vault/a/f", 10, 0)])
        _header, entries, _totals = parse_jsonl_zst_artifact(self.root,
                                                             self.locator)
        for key in entries[0]:
            self.assertNotIn("hash", key.lower())
            self.assertNotIn("sha", key.lower())
            self.assertNotIn("checksum", key.lower())

    def test_optional_hints_round_trip(self):
        with self._writer() as writer:
            writer.add(path="/vault/a/big", size=10 ** 10, ordinal=0,
                       entry_hint="file", storage_hint="loose")
        _header, entries, _totals = parse_jsonl_zst_artifact(self.root,
                                                             self.locator)
        self.assertEqual(entries[0]["entry_hint"], "file")
        self.assertEqual(entries[0]["storage_hint"], "loose")

    def test_reading_a_part_path_is_refused(self):
        with self.assertRaises(ArtifactError):
            parse_jsonl_zst_artifact(self.root, self.locator + ".part")

    def test_a_version_mismatch_is_refused(self):
        with JsonlZstArtifactWriter(self.root, self.locator,
                                    version="scan-segment-v99") as writer:
            writer.add(path="/vault/a/f", size=1, ordinal=0)
        with self.assertRaises(ArtifactError) as caught:
            parse_jsonl_zst_artifact(self.root, self.locator)
        self.assertIn("unsupported artifact version", str(caught.exception))

    def test_a_truncated_artifact_is_refused(self):
        """No footer means the stream was cut; it must not parse as complete."""
        import zstandard as zstd
        path = resolve_locator(self.root, self.locator)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as raw:
            writer = zstd.ZstdCompressor().stream_writer(raw)
            writer.write(json.dumps({"kind": "header",
                                     "version": ARTIFACT_VERSION}).encode()
                         + b"\n")
            writer.write(json.dumps({"kind": "entry", "path": "/a",
                                     "size": 1, "ordinal": 0}).encode() + b"\n")
            writer.close()
        with self.assertRaises(ArtifactError) as caught:
            parse_jsonl_zst_artifact(self.root, self.locator)
        self.assertIn("truncated", str(caught.exception))

    def test_a_headerless_artifact_is_refused(self):
        import zstandard as zstd
        path = resolve_locator(self.root, self.locator)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as raw:
            writer = zstd.ZstdCompressor().stream_writer(raw)
            writer.write(json.dumps({"kind": "footer", "file_count": 0,
                                     "byte_count": 0,
                                     "first_scan_ordinal": None,
                                     "last_scan_ordinal": None}).encode()
                         + b"\n")
            writer.close()
        with self.assertRaises(ArtifactError) as caught:
            parse_jsonl_zst_artifact(self.root, self.locator)
        self.assertIn("no header", str(caught.exception))

    def test_a_declared_aggregate_that_disagrees_is_refused(self):
        import zstandard as zstd
        path = resolve_locator(self.root, self.locator)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as raw:
            writer = zstd.ZstdCompressor().stream_writer(raw)
            for payload in (
                {"kind": "header", "version": ARTIFACT_VERSION},
                {"kind": "entry", "path": "/a", "size": 1, "ordinal": 0},
                # Claims two files; the artifact holds one.
                {"kind": "footer", "file_count": 2, "byte_count": 1,
                 "first_scan_ordinal": 0, "last_scan_ordinal": 0},
            ):
                writer.write(json.dumps(payload).encode() + b"\n")
            writer.close()
        with self.assertRaises(ArtifactError) as caught:
            parse_jsonl_zst_artifact(self.root, self.locator)
        self.assertIn("aggregate mismatch", str(caught.exception))


# =============================================================================
# D. Path edge cases (Linux filenames are bytes)
# =============================================================================
class PathEdgeCaseTests(_Tmp):
    def test_awkward_but_legal_linux_names_round_trip(self):
        names = [
            "/vault/a/with space",
            "/vault/a/with\ttab",
            "/vault/a/with'quote",
            '/vault/a/with"doublequote',
            "/vault/a/with\\backslash",       # legal on Linux, not a separator
            "/vault/a/יוניקוד",
            "/vault/a/emoji-\U0001F600",
            "/vault/a/" + "x" * 200,
            "/vault/a/trailing.dot.",
        ]
        self._write([(name, i, i) for i, name in enumerate(names)])
        _header, entries, _totals = parse_jsonl_zst_artifact(self.root,
                                                             self.locator)
        self.assertEqual([e["path"] for e in entries], names)

    def test_a_newline_in_a_name_cannot_break_the_jsonl_framing(self):
        """JSON escapes it; one record still occupies exactly one line."""
        self._write([("/vault/a/two\nlines", 5, 0)])
        _header, entries, totals = parse_jsonl_zst_artifact(self.root,
                                                            self.locator)
        self.assertEqual(entries[0]["path"], "/vault/a/two\nlines")
        self.assertEqual(totals["file_count"], 1)


# =============================================================================
# E. Orphan discovery
# =============================================================================
class OrphanPartTests(_Tmp):
    def test_no_orphans_on_a_clean_root(self):
        self.assertEqual(find_orphan_parts(self.root), [])

    def test_orphans_are_found_but_not_removed(self):
        writer = self._writer().open()
        writer.add(path="/vault/a/f", size=1, ordinal=0)
        writer.close(publish=False)
        found = find_orphan_parts(self.root)
        self.assertEqual(found, [writer.part_path])
        # Discovery is read-only: it is evidence for reconciliation, not a
        # cleanup pass.
        self.assertTrue(os.path.exists(writer.part_path))

    def test_a_published_segment_leaves_no_orphan(self):
        self._write([("/vault/a/f", 1, 0)])
        self.assertEqual(find_orphan_parts(self.root), [])


if __name__ == "__main__":
    unittest.main()
