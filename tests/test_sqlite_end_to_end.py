"""End to end on the SQLite backend: archive -> catalog -> restore.

No server, no tape. A temporary directory stands in for the cartridge and the
real pipeline pieces run against a real SQLite catalog file:

    synthetic source files
      -> LTOAnalyzer allocation plan -> local session + chunk manifest (catalog)
      -> LTOPacker (small files into a ZIP bundle, large files loose)
      -> LTOBackup (the real robocopy, temp dir -> temp dir) + catalog commit
      -> catalog search
      -> LTORetriever restore
      -> every restored file is byte-identical to its source.

Only the two hardware questions are answered by a patch: "is the LTO drive
ready" and "which cartridge is mounted".
"""
import hashlib
import os
import shutil
import tempfile
import unittest
from unittest import mock

from db_backends import SQLITE, create_test_db

from src.backup import _NoEjectBackup
from src.packer import LTOAnalyzer, LTOPacker
from src.retriever import LTORetriever
from src.runtime import CANCEL

TAPE_LABEL = "E2E_SQLITE_T1"


def _digest(path):
    sha = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(65536), b""):
            sha.update(block)
    return sha.hexdigest()


class SqliteArchiveRestoreRoundTrip(unittest.TestCase):
    def setUp(self):
        CANCEL.clear()
        self.tdb = create_test_db(SQLITE, "e2e")
        self.addCleanup(self.tdb.drop)
        self.db = self.tdb.manager()
        self.tmp = tempfile.mkdtemp(prefix="lto_e2e_")
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.source = os.path.join(self.tmp, "source")
        self.staging = os.path.join(self.tmp, "staging")
        self.tape = os.path.join(self.tmp, "tape")
        self.restore = os.path.join(self.tmp, "restored")
        self.logs = os.path.join(self.tmp, "logs")
        for path in (self.source, self.staging, self.tape, self.restore,
                     self.logs):
            os.makedirs(path)
        self.files = self._write_source()

    def _write_source(self):
        """Small files (packed) and files above the pack threshold (loose)."""
        files = {
            os.path.join("project_a", "notes.txt"): b"alpha\n" * 50,
            os.path.join("project_a", "sub", "data.csv"):
                b"id,value\n" + b"1,2\n" * 300,
            os.path.join("project_a", "sub", "empty.bin"): b"",
            os.path.join("project_a", "unicode_שלום.txt"):
                "shalom שלום".encode("utf-8"),
            # > 1 MiB: stays a loose file on "tape".
            os.path.join("project_a", "big", "volume.raw"):
                bytes(range(256)) * 5000,
            os.path.join("project_b", "model.bin"):
                hashlib.sha256(b"seed").digest() * 40000,
            os.path.join("project_b", "readme.md"): b"# readme\n",
        }
        for rel, content in files.items():
            path = os.path.join(self.source, rel)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "wb") as handle:
                handle.write(content)
        return files

    def _chunk_files(self, entries):
        collected = []
        for entry in entries:
            root = os.path.join(self.source, entry["top_level_dir"])
            for current, _dirs, names in os.walk(root):
                for name in names:
                    path = os.path.join(current, name)
                    collected.append({
                        "path": path,
                        "rel": os.path.relpath(path, self.source),
                        "size": os.path.getsize(path)})
        return sorted(collected, key=lambda item: item["rel"].lower())

    def _archive(self):
        self.db.register_tape(TAPE_LABEL, 12000)
        self.db.acquire_archiver_lock()
        self.addCleanup(self.db.release_archiver_lock)

        chunks = LTOAnalyzer().build_local_allocation_plan(
            self.source, first_tape_budget_bytes=10 * 1024 ** 4)
        session_id = self.db.create_local_session(
            "LOCAL_e2e_sqlite", self.source, chunks, backup_mode="pack")
        session = self.db.get_local_session(session_id)
        pending = self.db.get_local_pending_chunks(session_id)
        self.assertTrue(pending)

        for chunk_index in pending:
            self.db.verify_archiver_lock()
            entries = self.db.get_local_chunk_entries(session_id, chunk_index)
            self.db.assign_local_chunk_tape(session_id, chunk_index,
                                            TAPE_LABEL)
            self.db.update_local_chunk_status(session_id, chunk_index,
                                              "staged")
            pack_dir = os.path.join(
                self.staging, f"_local_s{session_id:04d}_c{chunk_index:03d}")
            metadata = LTOPacker(
                1, index_min_file_mb=0, index_packed_small_files=True,
            ).run_manifest(
                source_root=self.source, dest=pack_dir, threshold_mb=1,
                file_entries=self._chunk_files(entries),
                bundle_prefix=f"Bundle_s{session_id:04d}_c{chunk_index:03d}",
                source_name="local", session_id=session_id,
                chunk_index=chunk_index)
            with mock.patch("src.backup._ensure_lto_drive_ready",
                            return_value=True):
                _NoEjectBackup(
                    self.db, "", log_dir=self.logs, index_min_file_mb=0,
                    index_packed_small_files=True,
                ).run(source=pack_dir, tape_drive=self.tape,
                      tape_label=TAPE_LABEL, packer_metadata=metadata,
                      local_session_id=session_id,
                      local_chunk_index=chunk_index,
                      tape_parent_dir=session["session_label"])
            shutil.rmtree(pack_dir, ignore_errors=True)
            self.db.update_local_chunk_status(session_id, chunk_index,
                                              "backed_up")
        self.assertEqual(self.db.get_local_pending_chunks(session_id), [])
        self.db.update_local_session(session_id, status="completed")
        return session_id

    def test_archive_catalog_restore_is_byte_identical(self):
        session_id = self._archive()

        # -- catalog: every source file is indexed, with its real size ------
        records = self.db.search_catalog(tape_label=TAPE_LABEL)
        by_source = {os.path.normcase(os.path.normpath(r["original_path"])): r
                     for r in records}
        self.assertEqual(len(records), len(self.files))
        for rel, content in self.files.items():
            source_path = os.path.normcase(
                os.path.normpath(os.path.join(self.source, rel)))
            self.assertIn(source_path, by_source)
            self.assertEqual(by_source[source_path]["file_size_bytes"],
                             len(content))
        self.assertTrue(any(r["is_packed"] for r in records))
        self.assertTrue(any(not r["is_packed"] for r in records))
        hit = self.db.search_catalog(name_query="volume.raw")
        self.assertEqual([r["file_name"] for r in hit], ["volume.raw"])
        self.assertEqual(self.db.count_tape_file_records(TAPE_LABEL),
                         len(self.files))
        self.assertGreater(
            self.db.recalculate_tape_used_space(TAPE_LABEL), 0)
        self.assertEqual(
            self.db.get_local_session(session_id)["status"], "completed")

        # -- restore: straight from the temp "tape" --------------------------
        drive = os.path.splitdrive(os.path.abspath(self.tmp))[0] + "\\"
        retriever = LTORetriever(
            self.db, tape_drive=drive,
            staging_dir=os.path.join(self.tmp, "restore_staging"),
            restore_dir=self.restore)
        with mock.patch("src.retriever.get_volume_label",
                        return_value=TAPE_LABEL):
            outcome = retriever._restore_many(records)
        self.assertEqual(outcome.failed_count, 0, outcome.failures)
        self.assertEqual(outcome.restored_count, len(self.files))

        restored = {}
        for current, _dirs, names in os.walk(self.restore):
            for name in names:
                path = os.path.join(current, name)
                restored.setdefault(_digest(path), []).append(path)
        for rel, content in self.files.items():
            digest = hashlib.sha256(content).hexdigest()
            self.assertIn(
                digest, restored,
                f"{rel} was not restored with identical content")
        self.assertEqual(sum(len(paths) for paths in restored.values()),
                         len(self.files))

    def test_a_second_archive_of_the_same_files_does_not_duplicate_rows(self):
        self._archive()
        first = self.db.count_tape_file_records(TAPE_LABEL)
        records = self.db.search_catalog(tape_label=TAPE_LABEL)
        again = self.db.bulk_upsert_files(
            [{"original_path": r["original_path"],
              "file_size_bytes": r["file_size_bytes"],
              "tape_label": r["tape_label"],
              "source_host": r["source_host"],
              "is_packed": r["is_packed"],
              "container_name": r.get("container_name"),
              "stored_path": r["stored_path"],
              "local_session_id": r["local_session_id"],
              "local_chunk_index": r["local_chunk_index"]}
             for r in records])
        self.assertEqual(again["inserted"], 0)
        self.assertEqual(self.db.count_tape_file_records(TAPE_LABEL), first)


if __name__ == "__main__":
    unittest.main()
