"""End to end on the SQLite backend: archive -> catalog -> restore.

No server, no tape. A temporary directory stands in for the cartridge and the
real pipeline pieces run against a real SQLite catalog file:

    synthetic source files
      -> LTOAnalyzer allocation plan -> local session + chunk manifest (catalog)
      -> LTOPacker (small files into a ZIP bundle, large files loose)
      -> LTOBackup (copy to the temp "tape", the real result classifier) and
         its catalog commit
      -> catalog search
      -> LTORetriever restore
      -> every restored file is byte-identical to its source.

What is replaced, and only this:

* the two hardware questions - "is the LTO drive ready" and "which cartridge
  is mounted";
* the ``robocopy.exe`` child process. An in-process copier does the same
  copy and reports in robocopy's own summary format, so the production
  classifier still decides whether the write succeeded. No robocopy process
  is started: on the archive host a stray one would look like a live tape
  write to the operational guards.
"""
import hashlib
import os
import shutil
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

from db_backends import SQLITE, create_test_db

from src.backup import _NoEjectBackup
from src.constants import LEGACY_TAPE_PROFILE
from src.packer import LTOAnalyzer, LTOPacker
from src.retriever import LTORetriever
from src.runtime import CANCEL

TAPE_LABEL = "E2E_SQLITE_T1"


def _robocopy_stand_in(cmd, priority=None, affinity=None, on_start=None,
                       raw_sink=None):
    """``robocopy <src> <dst> /E ...`` in process, with robocopy's report."""
    source, target = str(cmd[1]), str(cmd[2])
    files = dirs = copied_bytes = 0
    for current, _subdirs, names in os.walk(source):
        relative = os.path.relpath(current, source)
        destination = os.path.normpath(os.path.join(target, relative))
        os.makedirs(destination, exist_ok=True)
        dirs += 1
        for name in names:
            shutil.copy2(os.path.join(current, name),
                         os.path.join(destination, name))
            files += 1
            copied_bytes += os.path.getsize(os.path.join(current, name))
    report = "\n".join((
        "", "               Total    Copied   Skipped  Mismatch    FAILED    "
        "Extras",
        f"    Dirs : {dirs:9d} {dirs:9d}         0         0         0"
        "         0",
        f"   Files : {files:9d} {files:9d}         0         0         0"
        "         0",
        f"   Bytes : {copied_bytes:9d} {copied_bytes:9d}         0         0"
        "         0         0",
        "   Times :   0:00:01   0:00:01                       0:00:00   "
        "0:00:00",
        "   Speed :            1000000 Bytes/sec.", ""))
    if raw_sink is not None:
        raw_sink.write(report)
    return SimpleNamespace(stdout=report, stderr="",
                           returncode=1 if files else 0)


def _copy_file_stand_in(src, dst, display_name=None):
    """``robocopy`` for one file: it lands at dirname(dst)/basename(src)."""
    target_dir = os.path.dirname(os.path.abspath(dst))
    os.makedirs(target_dir, exist_ok=True)
    shutil.copy2(src, os.path.join(target_dir, os.path.basename(src)))
    return True


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
            self.source, tape_profile=LEGACY_TAPE_PROFILE,
            first_tape_budget_bytes=10 * 1024 ** 4)
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
                            return_value=True), \
                    mock.patch("src.backup._run_robocopy_tuned",
                               _robocopy_stand_in):
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
                        return_value=TAPE_LABEL), \
                mock.patch("src.retriever._robocopy_file",
                           _copy_file_stand_in):
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
