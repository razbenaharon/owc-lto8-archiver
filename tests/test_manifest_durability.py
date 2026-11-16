"""Per-file manifest protection (audit M6).

After a validated export is pruned, the ``*.jsonl.zst`` manifests are the ONLY
copy of the small-file inventory. These tests pin three properties:

* a later export can never replace an earlier export's segment;
* the bytes are really fsynced (on Windows too) before the name is published;
* a reader verifies the declared totals and refuses trailing data.

They also prove files written the way earlier versions wrote them still read.
"""
import hashlib
import io
import json
import os
import tempfile
import unittest
from typing import Any, cast
from unittest import mock

import zstandard as zstd

from src import archive_artifacts as artifacts
from src import local_manifest_archive as archive
from src import plan_manifest as pm
from src.archive_artifacts import (
    ArtifactConflict,
    ArtifactError,
    JsonlZstArtifactWriter,
    PLAN_MANIFEST_VERSION,
    Plan3ArtifactWriter,
    parse_jsonl_zst_artifact,
    plan3_artifact_locator,
    read_plan3_artifact,
    resolve_locator,
    segment_locator,
)
from pg_test_guard import (SKIP_REASON, create_test_database,
                           drop_test_database, pg_available)

EXAMPLES = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "examples")
LABEL = "REMOTE_examplehost_20260101_120000"


def _example_rows():
    path = os.path.join(EXAMPLES, "file-manifest-segment.example.jsonl")
    with open(path, encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def _sha256(path):
    with open(path, "rb") as handle:
        return hashlib.sha256(handle.read()).hexdigest()


def _legacy_write_segment(path, rows):
    """Byte-for-byte how ``_write_segment`` wrote a segment before M6."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as raw:
        with zstd.ZstdCompressor(level=6).stream_writer(raw) as writer:
            for row in rows:
                writer.write((json.dumps(
                    row, ensure_ascii=False, separators=(",", ":"))
                    + "\n").encode("utf-8"))


def _legacy_write_plan3(path, lines):
    """How ``Plan3ArtifactWriter`` framed a file before M6 (closefd default)."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    raw = open(path, "wb")
    compressor = zstd.ZstdCompressor().stream_writer(raw)
    text = io.TextIOWrapper(compressor, encoding="utf-8")
    for payload in lines:
        text.write(json.dumps(payload, ensure_ascii=False,
                              separators=(",", ":"), sort_keys=True))
        text.write("\n")
    text.flush()
    text.detach()
    compressor.close()
    if not raw.closed:
        raw.close()


class _FsyncRecorder:
    """Record every *successful* fsync and what the file looked like then."""

    def __init__(self, final_path):
        self.final_path = final_path
        self.events = []
        self._real = os.fsync

    def __call__(self, fd):
        self._real(fd)                  # raises if the handle cannot be flushed
        self.events.append({
            "size": os.fstat(fd).st_size,
            "published": os.path.exists(self.final_path)})


def plan_record(ordinal, size):
    return {
        "canonical_path": f"/vault/example/f{ordinal}",
        "expected_size": size,
        "plan_ordinal": ordinal,
        "session_label": LABEL,
        "chunk_index": 7,
        "provenance_kind": "frontier",
        "scan_segment_id": "seg-0001",
        "scan_ordinal": 100 + ordinal,
        "storage_class": "container",
        "container_format": "stored_tar",
        "container_ordinal": 0,
        "routing_precision": "exact",
    }


class SegmentOverwriteTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = tmp.name
        self.rows = _example_rows()

    def test_segment_names_are_unique_per_export(self):
        row = self.rows[0]
        first = archive._segment_relpath(row, 1)
        second = archive._segment_relpath(row, 2)
        self.assertNotEqual(first, second)
        self.assertEqual(os.path.dirname(first), os.path.dirname(second))
        for rel in (first, second):
            self.assertTrue(rel.endswith(".jsonl.zst"))
            self.assertIn("bundle_11", os.path.basename(rel))
        unbundled = dict(row, bundle_id=None)
        self.assertNotEqual(archive._segment_relpath(unbundled, 1),
                            archive._segment_relpath(unbundled, 2))

    def test_an_existing_segment_is_never_overwritten(self):
        rel = os.path.join("Tape_Example_01", "session_remote_2",
                           "bundle_11.jsonl.zst")
        archive._write_segment(self.root, rel, self.rows[:1])
        final = os.path.join(self.root, rel)
        before = _sha256(final)

        with self.assertRaisesRegex(RuntimeError, "Refusing to overwrite"):
            archive._write_segment(self.root, rel, self.rows[1:])

        self.assertEqual(_sha256(final), before)
        found = archive.search_manifests(self.root, "*", limit=10)
        self.assertEqual([r["source_file_id"] for r in found], [1001])
        leftovers = [name for name in os.listdir(os.path.dirname(final))
                     if name != os.path.basename(final)]
        self.assertEqual(leftovers, [])

    def test_publish_is_exclusive_even_if_the_name_appears_mid_write(self):
        rel = os.path.join("T", "s", "bundle_1.jsonl.zst")
        final = os.path.join(self.root, rel)

        def rows():
            yield self.rows[0]
            # Another writer claims the name while this segment is streaming.
            with open(final, "wb") as handle:
                handle.write(b"the other export's only copy")
            yield self.rows[1]

        with self.assertRaisesRegex(RuntimeError, "Refusing to overwrite"):
            archive._write_segment(self.root, rel, rows())
        with open(final, "rb") as handle:
            self.assertEqual(handle.read(), b"the other export's only copy")
        self.assertEqual(os.listdir(os.path.dirname(final)),
                         [os.path.basename(final)])

    def test_segment_is_fsynced_before_it_is_published(self):
        rel = os.path.join("T", "s", "bundle_1.jsonl.zst")
        final = os.path.join(self.root, rel)
        recorder = _FsyncRecorder(final)
        with mock.patch.object(archive.os, "fsync", recorder):
            result = archive._write_segment(self.root, rel, self.rows)
        self.assertEqual(recorder.events, [
            {"size": result["compressed_bytes"], "published": False}])


class ArtifactDurabilityTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = tmp.name

    def test_plan_manifest_is_fsynced_before_it_is_published(self):
        locator = pm.plan_manifest_locator(LABEL, 7)
        final = resolve_locator(self.root, locator)
        recorder = _FsyncRecorder(final)
        with mock.patch.object(artifacts.os, "fsync", recorder):
            published = pm.write_plan_manifest(
                self.root, session_label=LABEL, chunk_index=7,
                provenance_kind="frontier",
                records=[plan_record(0, 10), plan_record(1, 20)])
        self.assertTrue(recorder.events, "no successful fsync was performed")
        for event in recorder.events:
            self.assertEqual(event, {"size": published.size_bytes,
                                     "published": False})

    def test_scan_segment_is_fsynced_before_it_is_published(self):
        locator = segment_locator(2, 5, 0)
        final = resolve_locator(self.root, locator)
        recorder = _FsyncRecorder(final)
        with mock.patch.object(artifacts.os, "fsync", recorder):
            with JsonlZstArtifactWriter(self.root, locator) as writer:
                writer.add(path="/vault/a", size=10, ordinal=0)
        self.assertEqual(recorder.events, [
            {"size": os.path.getsize(final), "published": False}])

    def test_fsync_path_really_flushes_on_this_platform(self):
        path = os.path.join(self.root, "x.bin")
        with open(path, "wb") as handle:
            handle.write(b"data")
        self.assertTrue(artifacts._fsync_path(path))
        with open(path, "rb") as handle:
            self.assertEqual(handle.read(), b"data")

    def test_scan_segment_publish_never_replaces_an_existing_file(self):
        locator = segment_locator(2, 5, 0)
        writer = JsonlZstArtifactWriter(self.root, locator)
        writer.add(path="/vault/a", size=10, ordinal=0)
        writer.close(publish=False)
        final = resolve_locator(self.root, locator)
        real_exists = os.path.exists

        def racing_exists(path):
            # The pre-check sees nothing; the name appears right after it.
            answer = real_exists(path)
            if str(path).endswith(os.path.basename(final)) and not answer:
                with open(final, "wb") as handle:
                    handle.write(b"winner")
            return answer
        with mock.patch.object(artifacts.os.path, "exists", racing_exists):
            with self.assertRaises(ArtifactConflict):
                writer.publish()
        with open(final, "rb") as handle:
            self.assertEqual(handle.read(), b"winner")


class StrictReaderTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = tmp.name
        self.published = pm.write_plan_manifest(
            self.root, session_label=LABEL, chunk_index=7,
            provenance_kind="frontier",
            records=[plan_record(0, 10), plan_record(1, 20)])
        self.locator = self.published.locator
        self.path = self.published.path

    def _read(self, locator=None):
        return read_plan3_artifact(
            self.root, locator or self.locator,
            expected_version=PLAN_MANIFEST_VERSION)

    def _lines(self):
        with open(self.path, "rb") as raw:
            data = zstd.ZstdDecompressor().decompressobj().decompress(
                raw.read())
        return [json.loads(line) for line in data.decode("utf-8").splitlines()]

    def _rewrite(self, lines):
        os.remove(self.path)
        _legacy_write_plan3(self.path, lines)

    def test_published_artifact_reads_back(self):
        header, records, trailer = self._read()
        self.assertEqual(len(records), 2)
        self.assertEqual(trailer["byte_total"], 30)

    def test_declared_byte_total_is_verified(self):
        lines = self._lines()
        lines[-1]["byte_total"] = 31
        self._rewrite(lines)
        with self.assertRaisesRegex(ArtifactError, "byte_total"):
            self._read()

    def test_record_size_changed_under_an_unchanged_trailer_is_refused(self):
        lines = self._lines()
        lines[1]["expected_size"] = 11
        self._rewrite(lines)
        with self.assertRaisesRegex(ArtifactError, "byte_total"):
            self._read()

    def test_missing_declared_totals_are_refused(self):
        original = self._lines()
        for key in ("record_count", "byte_total"):
            with self.subTest(key=key):
                lines = [dict(line) for line in original]
                lines[-1].pop(key)
                self._rewrite(lines)
                with self.assertRaisesRegex(ArtifactError, key):
                    self._read()

    def test_second_compressed_frame_is_refused(self):
        extra = zstd.ZstdCompressor().compress(json.dumps(
            dict(plan_record(2, 5), kind="entry")).encode("utf-8") + b"\n")
        with open(self.path, "ab") as handle:
            handle.write(extra)
        with self.assertRaisesRegex(ArtifactError, "trailing data"):
            self._read()

    def test_appended_copy_of_itself_is_refused(self):
        with open(self.path, "rb") as handle:
            data = handle.read()
        with open(self.path, "ab") as handle:
            handle.write(data)
        with self.assertRaisesRegex(ArtifactError, "trailing data"):
            self._read()

    def test_trailing_garbage_is_refused(self):
        for junk in (b"\x00", b"garbage after the frame" * 100):
            with self.subTest(junk=junk[:8]):
                with open(self.path, "rb") as handle:
                    data = handle.read()
                target = self.path + ".junk.jsonl.zst"
                with open(target, "wb") as handle:
                    handle.write(data + junk)
                locator = self.locator + ".junk.jsonl.zst"
                with self.assertRaisesRegex(ArtifactError, "trailing data"):
                    self._read(locator)

    def test_truncated_frame_is_refused(self):
        with open(self.path, "rb") as handle:
            data = handle.read()
        with open(self.path, "wb") as handle:
            handle.write(data[:len(data) // 2])
        with self.assertRaises(ArtifactError):
            self._read()

    def test_records_after_the_trailer_are_refused(self):
        lines = self._lines()
        lines.append(dict(plan_record(2, 5), kind="entry"))
        self._rewrite(lines)
        with self.assertRaisesRegex(ArtifactError, "follow the artifact trailer"):
            self._read()

    def test_scan_segment_with_a_second_frame_is_refused(self):
        locator = segment_locator(2, 5, 0)
        with JsonlZstArtifactWriter(self.root, locator) as writer:
            writer.add(path="/vault/a", size=10, ordinal=0)
        parse_jsonl_zst_artifact(self.root, locator)
        with open(resolve_locator(self.root, locator), "ab") as handle:
            handle.write(zstd.ZstdCompressor().compress(b'{"kind":"entry"}\n'))
        with self.assertRaisesRegex(ArtifactError, "trailing data"):
            parse_jsonl_zst_artifact(self.root, locator)


class BackwardCompatibilityTests(unittest.TestCase):
    """Manifests written by earlier versions must still read correctly."""

    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = tmp.name
        self.rows = _example_rows()

    def _strip(self, rows):
        return [{key: row[key] for key in self.rows[0]} for row in rows]

    def test_example_rows_round_trip_through_the_current_writer(self):
        rel = archive._segment_relpath(self.rows[0], export_id=3)
        result = archive._write_segment(self.root, rel, self.rows)
        self.assertEqual(result["row_count"], 2)
        self.assertEqual(result["original_bytes"], 2048 + 524288)
        self.assertEqual(
            result["sha256_hex"], _sha256(os.path.join(self.root, rel)))
        found = archive.search_manifests(self.root, "*", limit=10)
        self.assertEqual(self._strip(found), self.rows)
        self.assertEqual(
            archive.find_manifest_record(self.root, "M:1002")["file_name"],
            "sample_0001.bin")

    def test_segment_written_by_the_earlier_writer_still_reads(self):
        # Old layout and old name: <tape>/<session>/bundle_<id>.jsonl.zst
        rel = archive._segment_relpath(self.rows[0])
        self.assertEqual(os.path.basename(rel), "bundle_11.jsonl.zst")
        _legacy_write_segment(os.path.join(self.root, rel), self.rows)
        found = archive.search_manifests(self.root, "*", limit=10)
        self.assertEqual(self._strip(found), self.rows)

    def test_old_and_new_segments_of_one_bundle_coexist(self):
        old_rel = archive._segment_relpath(self.rows[0])
        new_rel = archive._segment_relpath(self.rows[0], export_id=9)
        _legacy_write_segment(os.path.join(self.root, old_rel), self.rows[:1])
        archive._write_segment(self.root, new_rel, self.rows[1:])
        found = archive.search_manifests(self.root, "*", limit=10)
        self.assertEqual(sorted(r["source_file_id"] for r in found),
                         [1001, 1002])

    def test_plan_manifest_framed_by_the_earlier_writer_still_reads(self):
        published = pm.write_plan_manifest(
            self.root, session_label=LABEL, chunk_index=7,
            provenance_kind="frontier",
            records=[plan_record(0, 10), plan_record(1, 20)])
        with open(published.path, "rb") as raw:
            data = zstd.ZstdDecompressor().decompressobj().decompress(
                raw.read())
        lines = [json.loads(line) for line in data.decode().splitlines()]
        os.remove(published.path)
        _legacy_write_plan3(published.path, lines)
        header, records, trailer = pm.read_plan_manifest(
            self.root, published.locator)
        self.assertEqual([r["plan_ordinal"] for r in records], [0, 1])
        self.assertEqual(trailer["byte_total"], 30)

    def test_scan_segment_framed_by_the_earlier_writer_still_reads(self):
        locator = segment_locator(2, 5, 0)
        path = resolve_locator(self.root, locator)
        _legacy_write_plan3(path, [
            {"kind": "header", "version": artifacts.ARTIFACT_VERSION,
             "session_id": 2, "scan_directory_id": 5, "scope": None},
            {"kind": "entry", "path": "/vault/a", "size": 10, "ordinal": 0},
            {"kind": "footer", "file_count": 1, "byte_count": 10,
             "first_scan_ordinal": 0, "last_scan_ordinal": 0},
        ])
        _header, entries, totals = parse_jsonl_zst_artifact(self.root, locator)
        self.assertEqual(len(entries), 1)
        self.assertEqual(totals["byte_count"], 10)


@unittest.skipUnless(pg_available(), SKIP_REASON)
class SecondExportKeepsFirstExportsSegmentsTests(unittest.TestCase):
    """End to end: a second export must not replace the segment of an earlier,
    already-pruned export of the same session."""

    @classmethod
    def setUpClass(cls):
        from src.pg_db import PgDatabaseManager

        cls.dbname, cls.conninfo = create_test_database("m6_manifest")
        cls.db = PgDatabaseManager(cls.conninfo)
        cls.db.apply_directory_catalog_schema()

    @classmethod
    def tearDownClass(cls):
        try:
            cls.db.close()
        except Exception:
            pass
        drop_test_database(cls.dbname)

    def _query(self, sql, params=()):
        import psycopg
        from psycopg.rows import dict_row
        with cast(Any, psycopg.connect(
                self.conninfo, autocommit=True,
                row_factory=cast(Any, dict_row))) as conn:
            return conn.execute(sql, params).fetchall()

    def _small_file(self, session_id, name):
        self.db.bulk_upsert_files([{
            "original_path": f"/m6/top/{name}", "file_size_bytes": 5,
            "tape_label": "TM6", "source_host": "srv02", "is_packed": False,
            "container_name": None, "stored_path": f"/m6/top/{name}",
            "local_session_id": session_id, "local_chunk_index": 0,
        }])

    def _export(self, root, backup):
        exported = archive.execute_export(self.conninfo, root, backup)
        export_id = exported["export_id"]
        self.assertTrue(
            archive.validate_export(self.conninfo, export_id)["passed"])
        with mock.patch(
                "src.local_manifest_archive.active_archive_processes",
                return_value=[]):
            archive.prune_export(
                self.conninfo, export_id, backup, execute=True)
        return export_id

    def _segments(self, export_id):
        return self._query(
            "SELECT manifest_relpath, sha256_hex FROM local_manifest_segments "
            "WHERE export_id=%s ORDER BY manifest_relpath", (export_id,))

    def test_second_export_does_not_replace_the_first_exports_segment(self):
        self.db.register_tape("TM6")
        session_id = self.db.create_local_session(
            "M6_SESSION", "/m6", [[{"name": "top", "size_bytes": 5}]], "pack")
        self._small_file(session_id, "first.txt")
        self.db.update_local_chunk_status(session_id, 0, "backed_up")
        self.db.update_local_session(
            session_id, status="completed",
            completed_at="2026-07-16T00:00:00Z")
        with tempfile.TemporaryDirectory() as root:
            backup = os.path.join(root, "verified_hot.dump")
            with open(backup, "wb") as handle:
                handle.write(b"test-only throwaway database backup marker")
            first = self._export(root, backup)
            first_segments = self._segments(first)
            self.assertEqual(len(first_segments), 1)

            # A row of the same tape/session group turns up later.
            self._small_file(session_id, "second.txt")
            second = self._export(root, backup)
            second_segments = self._segments(second)
            self.assertEqual(len(second_segments), 1)

            self.assertNotEqual(first_segments[0]["manifest_relpath"],
                                second_segments[0]["manifest_relpath"])
            for segment in first_segments + second_segments:
                path = os.path.join(root, segment["manifest_relpath"])
                self.assertEqual(_sha256(path), segment["sha256_hex"])
            names = sorted(
                row["file_name"] for row in archive.search_manifests(
                    root, "*", limit=10,
                    allowed_paths=archive.pruned_manifest_paths(self.conninfo)))
            self.assertEqual(names, ["first.txt", "second.txt"])


if __name__ == "__main__":
    unittest.main()
