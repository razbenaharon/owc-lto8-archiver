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


class RestoreAccountingTests(_RestoreCase):
    """H13: the final message and the result count what really happened."""

    def _zip_records(self, zip_name, names):
        return [{"tape_label": "T1", "is_packed": True, "bundle_id": 1,
                 "container_name": os.path.join(self.tape, zip_name),
                 "stored_path": name, "file_name": os.path.basename(name),
                 "original_path": "/src/" + name} for name in names]

    def _loose_record(self, name, content=None):
        path = os.path.join(self.tape, name)
        if content is not None:
            with open(path, "wb") as handle:
                handle.write(content)
        return {"tape_label": "T1", "is_packed": False, "stored_path": path,
                "file_name": name, "original_path": "/src/" + name}

    def _run_many(self, records):
        out = io.StringIO()
        with redirect_stdout(out):
            result = self.retriever._restore_many(records)
        return result, out.getvalue()

    def test_zero_files_restored_is_not_reported_as_restored(self):
        # The bundle is absent from the "tape": nothing can be restored.
        records = self._zip_records("Missing.zip", ["a.txt", "b.txt", "c.txt"])
        result, text = self._run_many(records)
        self.assertEqual(self._files_under(self.restore), [])
        self.assertNotIn("Complete.", text)
        self.assertIn("INCOMPLETE. 0 of 3 file(s) restored", text)
        for name in ("a.txt", "b.txt", "c.txt"):
            self.assertIn("/src/" + name, text)
        self.assertFalse(result.ok)
        self.assertEqual(
            (result.restored_count, result.failed_count, result.skipped),
            (0, 3, 0))
        self.assertNotEqual(result.exit_code, 0)

    def test_partial_success_counts_each_outcome(self):
        with zipfile.ZipFile(os.path.join(self.tape, "B.zip"), "w") as zf:
            zf.writestr("in_zip.txt", "ZIP")
        records = (
            [self._loose_record("ok.bin", b"OK"),
             self._loose_record("gone.bin")]            # source file absent
            + self._zip_records("B.zip", ["in_zip.txt", "not_in_zip.txt"]))
        result, text = self._run_many(records)
        self.assertEqual(
            (result.restored_count, result.failed_count, result.skipped),
            (2, 2, 0))
        self.assertFalse(result.ok)
        self.assertEqual(self._files_under(self.restore),
                         ["in_zip.txt", "ok.bin"])
        self.assertIn("2 of 4 file(s) restored", text)
        failed_names = {name for name, _reason in result.failures}
        self.assertEqual(failed_names,
                         {"/src/gone.bin", "/src/not_in_zip.txt"})
        self.assertEqual(sorted(os.path.basename(p) for p in result.restored),
                         ["in_zip.txt", "ok.bin"])

    def test_full_success_is_reported_complete(self):
        records = [self._loose_record("one.bin", b"1"),
                   self._loose_record("two.bin", b"22")]
        result, text = self._run_many(records)
        self.assertTrue(result.ok)
        self.assertEqual(result.exit_code, 0)
        self.assertEqual(result.restored_count, 2)
        self.assertIn("Complete. 2 file(s) restored", text)

    def test_unsafe_name_is_a_recorded_failure_not_a_crash(self):
        records = [self._loose_record("ok.bin", b"OK"),
                   dict(self._loose_record("x.bin", b"X"), file_name="..",
                        original_path=None)]
        result, _text = self._run_many(records)
        self.assertEqual((result.restored_count, result.failed_count), (1, 1))

    def test_records_on_unregistered_tapes_are_skipped_not_restored(self):
        page = [dict(self._loose_record("one.bin", b"1"), file_id=1)]

        class _DB:
            @staticmethod
            def list_tapes():
                return [{"volume_label": "T1"}]
        self.retriever.db = cast(Any, _DB())
        calls = []

        def fetch_after(after_id, tape):
            calls.append((after_id, tape))
            return page if after_id == 0 else []
        out = io.StringIO()
        with redirect_stdout(out):
            result = self.retriever._restore_all_pages(fetch_after, 3)
        self.assertEqual(
            (result.restored_count, result.failed_count, result.skipped),
            (1, 0, 2))
        self.assertFalse(result.ok)
        self.assertIn("INCOMPLETE", out.getvalue())
        self.assertIn("1 of 3 file(s) restored", out.getvalue())

    def test_directory_restore_reports_a_bundle_it_could_not_read(self):
        class _DB:
            @staticmethod
            def find_directory_restore_bundles(dir_path, **_kw):
                return [{"tape_label": "T1", "base_path": "/vault",
                         "stored_bundle_path":
                             os.path.join(self.tape, "Missing.zip")}]
        self.retriever.db = cast(Any, _DB())
        out = io.StringIO()
        with redirect_stdout(out):
            result = self.retriever._restore_directory_complete("/vault/D")
        self.assertFalse(result.ok)
        self.assertNotIn("complete", out.getvalue().lower().replace(
            "incomplete", ""))
        self.assertIn("Missing.zip", out.getvalue())

    def test_run_returns_the_result_and_keeps_it_on_last_result(self):
        record = dict(self._zip_records("Missing.zip", ["a.txt"])[0], file_id=7)

        class _DB:
            @staticmethod
            def count_search_files(*_a):
                return 1

            @staticmethod
            def search_files(*_a, **_kw):
                return [dict(record, file_size_bytes=1, backup_date=None,
                             source_host="h")]

            @staticmethod
            def get_file_by_id(_id):
                return record
        self.retriever.db = cast(Any, _DB())
        out = io.StringIO()
        with redirect_stdout(out), mock.patch(
                "builtins.input", side_effect=["1", "a.txt", "7"]):
            result = self.retriever.run()
        self.assertIs(result, self.retriever.last_result)
        self.assertFalse(result.ok)
        self.assertEqual(result.exit_code,
                         retriever_mod.RESTORE_INCOMPLETE_EXIT_CODE)
        self.assertEqual(result.reason, "restore_incomplete")
        self.assertIn("INCOMPLETE", out.getvalue())

    def test_cancel_raises_and_marks_last_result_cancelled(self):
        class _DB:
            @staticmethod
            def count_search_files(*_a):
                return 1

            @staticmethod
            def search_files(*_a, **_kw):
                return [{"file_id": 1, "file_name": "a", "file_size_bytes": 1,
                         "backup_date": None, "tape_label": "T1"}]

            @staticmethod
            def list_tapes():
                CANCEL.set()
                return [{"volume_label": "T1"}]
        self.retriever.db = cast(Any, _DB())
        self.addCleanup(CANCEL.clear)
        with redirect_stdout(io.StringIO()), mock.patch(
                "builtins.input", side_effect=["1", "a", "ALL"]):
            with self.assertRaisesRegex(RuntimeError, "Cancelled"):
                self.retriever.run()
        self.assertTrue(self.retriever.last_result.cancelled)
        self.assertFalse(self.retriever.last_result.ok)
        self.assertNotEqual(self.retriever.last_result.exit_code, 0)


class AtomicZipExtractionTests(_RestoreCase):
    """H14: a failed ZIP extraction never leaves a file under the final name,
    and never stops the remaining files from being restored."""

    GOOD_A = b"A" * 4096
    BAD = b"corrupt-me-" * 400
    GOOD_C = b"C" * 4096

    def _bundle(self, name="Bundle_001.zip", compression=zipfile.ZIP_STORED):
        """A ZIP whose middle member fails its CRC when read."""
        path = os.path.join(self.tape, name)
        with zipfile.ZipFile(path, "w", compression) as zf:
            zf.writestr("D/a.bin", self.GOOD_A)
            zf.writestr("D/bad.bin", self.BAD)
            zf.writestr("D/c.bin", self.GOOD_C)
        with open(path, "rb") as handle:
            raw = bytearray(handle.read())
        if compression == zipfile.ZIP_STORED:
            at = raw.index(self.BAD) + len(self.BAD) // 2
        else:       # inside the deflate stream of the middle member
            first = raw.index(b"D/bad.bin") + len(b"D/bad.bin")
            at = first + 6
        raw[at] ^= 0xFF
        with open(path, "wb") as handle:
            handle.write(bytes(raw))
        return path

    def _records(self, zip_path, names=("D/a.bin", "D/bad.bin", "D/c.bin")):
        return [{"tape_label": "T1", "is_packed": True, "bundle_id": 1,
                 "container_name": zip_path, "stored_path": name,
                 "file_name": os.path.basename(name),
                 "original_path": "/vault/" + name} for name in names]

    def _assert_no_partials(self):
        leftovers = [rel for rel in self._files_under(self.restore)
                     if ".restore_" in rel or rel.endswith(".part")]
        self.assertEqual(leftovers, [])

    def _read(self, rel):
        with open(os.path.join(self.restore, rel), "rb") as handle:
            return handle.read()

    def test_single_corrupt_member_leaves_nothing_under_the_final_name(self):
        for compression in (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED):
            with self.subTest(compression=compression):
                zip_path = self._bundle(f"B{compression}.zip", compression)
                record = self._records(zip_path, ["D/bad.bin"])[0]
                with redirect_stdout(io.StringIO()):
                    ok = self.retriever._restore_packed(record)
                self.assertFalse(ok)
                self.assertEqual(self._files_under(self.restore), [])
                self.assertEqual(self.retriever._result.failures[-1][0],
                                 "/vault/D/bad.bin")

    def test_bulk_restore_continues_past_a_corrupt_member(self):
        zip_path = self._bundle()
        out = io.StringIO()
        with redirect_stdout(out):
            result = self.retriever._restore_many(self._records(zip_path))
        self.assertEqual(self._files_under(self.restore), ["a.bin", "c.bin"])
        self.assertEqual(self._read("a.bin"), self.GOOD_A)
        self.assertEqual(self._read("c.bin"), self.GOOD_C)
        self._assert_no_partials()
        self.assertFalse(result.ok)
        self.assertEqual((result.restored_count, result.failed_count), (2, 1))
        self.assertEqual(result.failures[0][0], "/vault/D/bad.bin")
        self.assertNotIn("Complete.", out.getvalue())

    def test_directory_restore_continues_past_a_corrupt_member(self):
        zip_path = self._bundle()

        class _DB:
            @staticmethod
            def find_directory_restore_bundles(dir_path, **_kw):
                return [{"tape_label": "T1", "stored_bundle_path": zip_path,
                         "base_path": "/vault"}]
        self.retriever.db = cast(Any, _DB())
        out = io.StringIO()
        with redirect_stdout(out):
            result = self.retriever._restore_directory_complete("/vault/D")
        self.assertEqual(self._files_under(self.restore),
                         ["D/a.bin", "D/c.bin"])
        self.assertEqual(self._read("D/c.bin"), self.GOOD_C)
        self._assert_no_partials()
        self.assertFalse(result.ok)
        self.assertEqual((result.restored_count, result.failed_count), (2, 1))
        self.assertIn("/vault/D/bad.bin", out.getvalue())
        self.assertIn("INCOMPLETE", out.getvalue())

    def test_existing_destination_is_never_replaced(self):
        zip_path = self._bundle()
        existing = os.path.join(self.restore, "a.bin")
        with open(existing, "wb") as handle:
            handle.write(b"already here")
        with redirect_stdout(io.StringIO()):
            self.retriever._restore_many(
                self._records(zip_path, ["D/a.bin"]))
        self.assertEqual(self._read("a.bin"), b"already here")
        self.assertEqual(self._read("a_1.bin"), self.GOOD_A)

    def test_publication_race_does_not_clobber_a_file_created_meanwhile(self):
        zip_path = os.path.join(self.tape, "Race.zip")
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("x.bin", "from-zip")
        target = os.path.join(self.restore, "x.bin")
        with zipfile.ZipFile(zip_path) as zf:
            real_open = zf.open

            def racing_open(*args, **kwargs):
                # Another writer claims the final name mid-extraction.
                with open(target, "wb") as handle:
                    handle.write(b"winner")
                return real_open(*args, **kwargs)
            with mock.patch.object(zf, "open", racing_open):
                published = self.retriever._extract_zip_entry(
                    zf, "x.bin", target)
        self.assertEqual(self._read("x.bin"), b"winner")
        self.assertNotEqual(os.path.normcase(published),
                            os.path.normcase(target))
        with open(published, "rb") as handle:
            self.assertEqual(handle.read(), b"from-zip")
        self._assert_no_partials()

    def test_extraction_is_fsynced_before_it_is_published(self):
        zip_path = os.path.join(self.tape, "Sync.zip")
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("x.bin", "payload")
        target = os.path.join(self.restore, "x.bin")
        events = []
        real_fsync = os.fsync

        def recording_fsync(fd):
            real_fsync(fd)
            events.append(("fsync", os.fstat(fd).st_size,
                           os.path.exists(target)))
        with zipfile.ZipFile(zip_path) as zf, mock.patch.object(
                retriever_mod.os, "fsync", recording_fsync):
            self.retriever._extract_zip_entry(zf, "x.bin", target)
        self.assertEqual(events, [("fsync", len("payload"), False)])
        self.assertEqual(self._read("x.bin"), b"payload")

    def test_cancel_during_extraction_publishes_nothing(self):
        zip_path = os.path.join(self.tape, "Cancel.zip")
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("x.bin", "payload")
        CANCEL.set()
        self.addCleanup(CANCEL.clear)
        with zipfile.ZipFile(zip_path) as zf:
            with self.assertRaises(retriever_mod.RestoreCancelled):
                self.retriever._extract_zip_entry(
                    zf, "x.bin", os.path.join(self.restore, "x.bin"))
        self.assertEqual(self._files_under(self.restore), [])


class ReviewFollowUpTests(_RestoreCase):
    def test_catalog_error_in_directory_restore_is_a_failure(self):
        # review F1
        class _DB:
            @staticmethod
            def find_directory_restore_bundles(dir_path, **_kw):
                raise RuntimeError("[DB] directory catalog is not installed")
        self.retriever.db = cast(Any, _DB())
        out = io.StringIO()
        with redirect_stdout(out), mock.patch(
                "builtins.input", side_effect=["6", "/vault/D"]):
            result = self.retriever.run()
        self.assertFalse(result.ok)
        self.assertNotEqual(result.exit_code, 0)
        self.assertEqual(result.failed_count, 1)
        self.assertIn("directory catalog is not installed", out.getvalue())
        self.assertIn("INCOMPLETE", out.getvalue())

if __name__ == "__main__":
    unittest.main()
