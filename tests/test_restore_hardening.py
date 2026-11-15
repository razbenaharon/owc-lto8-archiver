"""Restore hardening regression tests (audit H13, H14, M5).

Principle under test: a restore never reports success for a file that was not
restored with verified content, never leaves a partial file under the final
name, and never writes outside the chosen destination.

Every "tape" here is a temporary directory; nothing touches LTFS.
"""
import io
import os
import shutil
import tempfile
import unittest
import zipfile
from contextlib import redirect_stdout
from typing import Any, cast
from unittest import mock

from src import retriever as retriever_mod
from src.retriever import LTORetriever
from src.runtime import CANCEL


def _fake_robocopy(src, dst, display_name=None):
    """Mimic robocopy: the file lands at dirname(dst)/basename(SRC)."""
    dst_dir = os.path.dirname(os.path.abspath(dst))
    os.makedirs(dst_dir, exist_ok=True)
    try:
        shutil.copy2(src, os.path.join(dst_dir, os.path.basename(src)))
    except OSError:
        return False
    return True


class _RestoreCase(unittest.TestCase):
    def setUp(self):
        CANCEL.clear()
        self.tmp = tempfile.mkdtemp(prefix="lto_restore_hardening_")
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.tape = os.path.join(self.tmp, "tape")
        self.restore = os.path.join(self.tmp, "restore")
        self.outside = os.path.join(self.tmp, "outside")
        for path in (self.tape, self.restore, self.outside):
            os.makedirs(path)
        tmp_drive = os.path.splitdrive(os.path.abspath(self.tmp))[0] + "\\"
        self.retriever = LTORetriever(
            db=cast(Any, None), tape_drive=tmp_drive,
            staging_dir=os.path.join(self.tmp, "staging"),
            restore_dir=self.restore)
        self.retriever._verify_tape = mock.Mock()
        patcher = mock.patch.object(
            retriever_mod, "_robocopy_file", _fake_robocopy)
        patcher.start()
        self.addCleanup(patcher.stop)

    def _files_under(self, root):
        found = []
        for dirpath, _dirs, names in os.walk(root):
            for name in names:
                found.append(os.path.relpath(
                    os.path.join(dirpath, name), root).replace("\\", "/"))
        return sorted(found)

    def _assert_inside_restore(self, path):
        root = os.path.normcase(os.path.realpath(self.restore))
        final = os.path.normcase(os.path.realpath(path))
        self.assertNotEqual(final, root)
        self.assertEqual(os.path.commonpath([root, final]), root,
                         f"{path!r} escapes the restore directory")


class RestoreContainmentTests(_RestoreCase):
    """M5: no stored name may route a restore outside the destination."""

    # Each value is a hostile stored name; none may leave the restore dir.
    ESCAPES = {
        "drive_letter_mid_path": "sub/Z:evil.txt",
        "drive_letter_mid_path_backslash": "sub\\Z:\\evil.txt",
        "drive_relative_leaf": "Z:evil.txt",
        "absolute_drive": "Z:\\Windows\\evil.txt",
        "absolute_posix": "/etc/evil.txt",
        "absolute_backslash": "\\evil.txt",
        "unc": "\\\\server\\share\\evil.txt",
        "unc_forward": "//server/share/evil.txt",
        "device_namespace": "\\\\?\\Z:\\evil.txt",
        "dot_device_namespace": "\\\\.\\Z:\\evil.txt",
        "parent_traversal": "../../evil.txt",
        "parent_traversal_backslash": "..\\..\\evil.txt",
        "parent_traversal_mid": "a/../../../evil.txt",
        "alternate_data_stream": "report.txt:hidden",
        "ads_with_type": "report.txt::$DATA",
    }

    def test_every_escape_class_stays_inside_the_restore_dir(self):
        for label, name in self.ESCAPES.items():
            with self.subTest(label=label):
                dst = self.retriever._destination_for_record(
                    {"file_name": name, "original_path": None})
                self._assert_inside_restore(dst)
                rel = os.path.relpath(dst, self.restore)
                self.assertNotIn(":", rel)
                self.assertNotIn("..", rel.split(os.sep))

    def test_escape_via_original_path_under_restore_base(self):
        # The relative path is derived from original_path, which is catalog
        # data and just as untrusted as file_name.
        record = {"file_name": "evil.txt",
                  "original_path": "/srv/data/sub/Z:evil.txt"}
        dst = self.retriever._destination_for_record(
            record, restore_base="/srv/data")
        self._assert_inside_restore(dst)
        self.assertNotIn(":", os.path.relpath(dst, self.restore))

    def test_parent_traversal_components_are_dropped(self):
        # Pins the ".." filter itself: removing it must fail a test even
        # though the final containment assertion would still catch an escape.
        safe = LTORetriever._safe_restore_relpath
        self.assertEqual(safe("../../evil.txt"), "evil.txt")
        self.assertEqual(safe("a/../b/./c.txt"), os.path.join("a", "b", "c.txt"))
        self.assertEqual(safe("..\\..\\x\\..\\y"), os.path.join("x", "y"))
        self.assertEqual(safe(".."), "")

    def test_containment_assertion_rejects_what_sanitising_missed(self):
        # Defence in depth: the final check is independent of the sanitiser.
        for raw in ("..\\outside\\evil.txt", "Z:evil.txt", "Z:\\evil.txt",
                    "\\\\server\\share\\evil.txt", "\\evil.txt", ""):
            with self.subTest(raw=raw):
                with self.assertRaises(retriever_mod.RestoreContainmentError):
                    self.retriever._contained_restore_path(raw)

    def test_reserved_device_names_are_neutralised(self):
        safe = LTORetriever._safe_restore_relpath
        for name in ("NUL", "con", "COM1.txt", "lpt9.log", "aux", "PRN.tar.gz",
                     "CONIN$", "nul .txt"):
            with self.subTest(name=name):
                out = safe("dir/" + name)
                leaf = os.path.basename(out)
                stem = leaf.split(".", 1)[0].rstrip(" ").upper()
                self.assertNotIn(stem, retriever_mod._RESERVED_DEVICE_NAMES)
                self.assertTrue(leaf.lower().lstrip("_").startswith(
                    name.split(".", 1)[0].rstrip(" ").lower()))

    def test_trailing_dots_and_spaces_cannot_alias_another_name(self):
        safe = LTORetriever._safe_restore_relpath
        self.assertEqual(safe("dir./file.txt. "), os.path.join("dir", "file.txt"))
        self.assertEqual(safe("a/.../b"), os.path.join("a", "b"))

    def test_ordinary_names_are_unchanged(self):
        safe = LTORetriever._safe_restore_relpath
        for name in ("a.txt", "feat/fold 1/sub/b.npy", ".hidden", "x..y.txt",
                     "console.log", "nullable.txt", "שלום/קובץ.txt"):
            with self.subTest(name=name):
                self.assertEqual(safe(name), os.path.join(*name.split("/")))

    def test_junction_inside_restore_dir_is_not_followed_out(self):
        try:
            import _winapi
            _winapi.CreateJunction(
                self.outside, os.path.join(self.restore, "link"))
        except (ImportError, AttributeError, OSError):
            self.skipTest("directory junctions are unavailable")
        with self.assertRaises(retriever_mod.RestoreContainmentError):
            self.retriever._destination_for_record(
                {"file_name": "link/evil.txt", "original_path": None})

    def test_hostile_loose_record_is_written_inside_or_not_at_all(self):
        src = os.path.join(self.tape, "payload.bin")
        with open(src, "wb") as handle:
            handle.write(b"payload")
        with redirect_stdout(io.StringIO()):
            for name in ("..\\..\\outside\\evil.txt", "sub/Z:evil.txt",
                         "report.txt:hidden"):
                self.retriever._restore_loose({
                    "stored_path": src, "file_name": name,
                    "original_path": None, "tape_label": "T",
                    "is_packed": False})
        self.assertEqual(self._files_under(self.outside), [])
        restored = self._files_under(self.restore)
        self.assertEqual(len(restored), 3)
        for rel in restored:
            self.assertNotIn(":", rel)
            with open(os.path.join(self.restore, rel), "rb") as handle:
                self.assertEqual(handle.read(), b"payload")

    def test_directory_restore_zip_entry_cannot_escape(self):
        zip_path = os.path.join(self.tape, "Bundle_001.zip")
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("D/ok.txt", "OK")
            zf.writestr("D/sub/Z:evil.txt", "EVIL1")
            zf.writestr("D/../../outside/evil.txt", "EVIL2")

        class _DB:
            @staticmethod
            def find_directory_restore_bundles(dir_path, **_kw):
                return [{"tape_label": "T1", "stored_bundle_path": zip_path,
                         "base_path": "/vault"}]
        self.retriever.db = cast(Any, _DB())
        with redirect_stdout(io.StringIO()):
            self.retriever._restore_directory_complete("/vault/D")
        self.assertEqual(self._files_under(self.outside), [])
        for rel in self._files_under(self.restore):
            self.assertNotIn(":", rel)
            self._assert_inside_restore(os.path.join(self.restore, rel))
        self.assertIn("D/ok.txt", self._files_under(self.restore))


if __name__ == "__main__":
    unittest.main()
