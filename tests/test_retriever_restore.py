"""Restore-path regression tests (review §1.1).

robocopy always writes ``dst_dir/<src basename>`` — it cannot rename a file
during a copy — so handing it a collision-renamed destination used to silently
overwrite the existing same-named file. These tests pin the fixed behaviour
with a fake copier that mimics real robocopy semantics exactly.
"""
import os
import shutil
import tempfile
import unittest
from typing import Any, cast

from src import retriever as retriever_mod
from src.retriever import LTORetriever
from src.runtime import CANCEL


def _fake_robocopy(src, dst, display_name=None):
    """Mimic robocopy: the file lands at dirname(dst)/basename(SRC)."""
    dst_dir = os.path.dirname(os.path.abspath(dst))
    os.makedirs(dst_dir, exist_ok=True)
    landed = os.path.join(dst_dir, os.path.basename(src))
    shutil.copy2(src, landed)
    return True


class RestoreCollisionTests(unittest.TestCase):
    def setUp(self):
        CANCEL.clear()
        self.tmp = tempfile.mkdtemp(prefix="lto_restore_test_")
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.tape = os.path.join(self.tmp, "tape")
        self.restore = os.path.join(self.tmp, "restore")
        os.makedirs(self.tape)
        os.makedirs(self.restore)
        # The fake tape lives in the tmp dir, so the configured drive must be
        # the tmp dir's own drive — the retriever remaps the stored drive
        # letter onto the configured LTFS drive before reading.
        tmp_drive = os.path.splitdrive(os.path.abspath(self.tmp))[0] + "\\"
        self.retriever = LTORetriever(
            db=cast(Any, None), tape_drive=tmp_drive,
            staging_dir=os.path.join(self.tmp, "staging"),
            restore_dir=self.restore)
        self._orig_robocopy = retriever_mod._robocopy_file
        retriever_mod._robocopy_file = _fake_robocopy
        self.addCleanup(
            setattr, retriever_mod, "_robocopy_file", self._orig_robocopy)

    def _tape_file(self, name, content):
        path = os.path.join(self.tape, name)
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(content)
        return path

    def test_copy_file_to_honors_renamed_destination(self):
        src = self._tape_file("data.csv", "second")
        existing = os.path.join(self.restore, "data.csv")
        with open(existing, "w", encoding="utf-8") as handle:
            handle.write("first")
        dst = self.retriever._unique_dest_path(existing)
        self.assertNotEqual(dst, existing)

        self.assertTrue(self.retriever._copy_file_to(src, dst))

        with open(existing, encoding="utf-8") as handle:
            self.assertEqual(handle.read(), "first")   # never clobbered
        with open(dst, encoding="utf-8") as handle:
            self.assertEqual(handle.read(), "second")
        # The scratch dir used for the rename hop must not be left behind.
        leftovers = [name for name in os.listdir(self.restore)
                     if name.startswith(".restore_tmp_")]
        self.assertEqual(leftovers, [])

    def test_restore_loose_does_not_clobber_same_basename(self):
        src = self._tape_file("report.txt", "tape-copy")
        existing = os.path.join(self.restore, "report.txt")
        with open(existing, "w", encoding="utf-8") as handle:
            handle.write("already-restored")
        record = {"stored_path": src, "file_name": "report.txt",
                  "original_path": "/srv/data/report.txt"}

        self.retriever._restore_loose(record)

        with open(existing, encoding="utf-8") as handle:
            self.assertEqual(handle.read(), "already-restored")
        renamed = os.path.join(self.restore, "report_1.txt")
        with open(renamed, encoding="utf-8") as handle:
            self.assertEqual(handle.read(), "tape-copy")

    def test_restore_many_raises_on_cancel(self):
        CANCEL.set()
        with self.assertRaisesRegex(RuntimeError, "Cancelled"):
            self.retriever._restore_many([
                {"tape_label": "T", "is_packed": False,
                 "stored_path": "x", "file_name": "x",
                 "original_path": "/x"},
            ])


class DirectoryCompleteRestoreTests(unittest.TestCase):
    """Bundle-complete directory restore extracts the small files that have no
    individual files_index row, and only the requested directory's subtree —
    driven by the ZIP's own entry list + a derived base, not per-file lookups."""

    def setUp(self):
        CANCEL.clear()
        self.tmp = tempfile.mkdtemp(prefix="lto_dircomplete_")
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.tape = os.path.join(self.tmp, "tape")
        self.restore = os.path.join(self.tmp, "restore")
        os.makedirs(self.tape)
        os.makedirs(self.restore)
        tmp_drive = os.path.splitdrive(os.path.abspath(self.tmp))[0] + "\\"
        # ZIP entries are relative to the source base (here '/strg'); mix two
        # tiny files under the requested dir with a sibling dir's file.
        import zipfile as _zip
        self.zip_path = os.path.join(self.tape, "Bundle_001.zip")
        with _zip.ZipFile(self.zip_path, "w") as zf:
            zf.writestr("D/shared/APAS/feat/fold 1/a.npy", "AAA")
            zf.writestr("D/shared/APAS/feat/fold 1/sub/b.npy", "BBB")
            zf.writestr("D/shared/suture_1/other.png", "XXX")   # sibling — skip
            zf.writestr("D/shared/APAS/feat_v2/c.npy", "CCC")    # prefix trap — skip

        class _DB:
            def find_directory_restore_bundles(_self, dir_path,
                                               source_host=None, tape_label=None):
                return [{"tape_label": "T1",
                         "stored_bundle_path": self.zip_path,
                         "base_path": "/strg"}]
        self.retriever = LTORetriever(
            db=cast(Any, _DB()), tape_drive=tmp_drive,
            staging_dir=os.path.join(self.tmp, "staging"),
            restore_dir=self.restore)
        self.retriever._verify_tape = lambda label: None  # no real tape
        self._orig_robocopy = retriever_mod._robocopy_file
        retriever_mod._robocopy_file = _fake_robocopy
        self.addCleanup(
            setattr, retriever_mod, "_robocopy_file", self._orig_robocopy)

    def _restored(self):
        found = []
        for root, _dirs, files in os.walk(self.restore):
            for f in files:
                found.append(os.path.relpath(
                    os.path.join(root, f), self.restore).replace("\\", "/"))
        return sorted(found)

    def test_extracts_small_files_and_only_the_requested_subtree(self):
        self.retriever._restore_directory_complete(
            "/strg/D/shared/APAS/feat")
        restored = self._restored()
        # both tiny files under feat/ are restored (they have no DB row)...
        self.assertIn("feat/fold 1/a.npy", restored)
        self.assertIn("feat/fold 1/sub/b.npy", restored)
        # ...and the sibling dir + the "feat_v2" prefix trap are NOT.
        self.assertTrue(all("suture_1" not in r for r in restored))
        self.assertTrue(all("feat_v2" not in r for r in restored))
        self.assertEqual(len(restored), 2)


if __name__ == "__main__":
    unittest.main()
