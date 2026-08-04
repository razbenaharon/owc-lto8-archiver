import io
import json
import os
import shutil
import tarfile
import tempfile
import unittest
import zipfile
from typing import Any, cast
from unittest import mock

import zstandard as zstd

from src import retriever as retriever_mod
from src.archive_artifacts import (ArtifactError, TAR_SIDECAR_VERSION,
                                   search_tar_sidecar)
from src.retriever import LTORetriever
from src.runtime import CANCEL
from src.tar_container import (STORED_TAR_DIALECT,
                               STORED_TAR_FORMAT_VERSION, GNU_RECORD_SIZE)


def _fake_robocopy(src, dst, display_name=None):
    os.makedirs(os.path.dirname(os.path.abspath(dst)), exist_ok=True)
    shutil.copy2(src, os.path.join(
        os.path.dirname(os.path.abspath(dst)), os.path.basename(src)))
    return True


def _write_tar(path, members):
    with tarfile.open(path, "w", format=tarfile.PAX_FORMAT) as tf:
        for name, content in members:
            payload = content if isinstance(content, bytes) else content.encode()
            info = tarfile.TarInfo(name)
            info.size = len(payload)
            info.mtime = 0
            tf.addfile(info, io.BytesIO(payload))
    current = os.path.getsize(path)
    with open(path, "ab") as handle:
        handle.write(bytes((-current) % GNU_RECORD_SIZE))


def _write_sidecar(path, container_id, source_base, members):
    records = [{
        "kind": "header", "version": TAR_SIDECAR_VERSION,
        "container_id": container_id, "source_base_path": source_base,
    }]
    logical_bytes = 0
    for ordinal, (name, content) in enumerate(members):
        payload = content if isinstance(content, bytes) else content.encode()
        logical_bytes += len(payload)
        records.append({
            "kind": "member", "member_name": name,
            "logical_size": len(payload), "ordinal": ordinal,
        })
    records.append({
        "kind": "footer", "member_count": len(members),
        "logical_bytes": logical_bytes,
    })
    with open(path, "wb") as raw:
        with zstd.ZstdCompressor().stream_writer(raw) as compressed:
            for record in records:
                compressed.write((json.dumps(record) + "\n").encode())


class MixedContainerRestoreTests(unittest.TestCase):
    def setUp(self):
        CANCEL.clear()
        self.tmp = tempfile.mkdtemp(prefix="mixed_restore_")
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.restore = os.path.join(self.tmp, "restore")
        self.staging = os.path.join(self.tmp, "staging")
        os.makedirs(self.restore)
        self.retriever = LTORetriever(
            db=cast(Any, None), tape_drive="Z:\\",
            staging_dir=self.staging, restore_dir=self.restore,
            manifest_archive_root=self.tmp)
        self.retriever._resolve_tape_path = lambda path: path
        self.retriever._verify_tape = mock.Mock()
        self.robocopy = mock.patch.object(
            retriever_mod, "_robocopy_file", side_effect=_fake_robocopy)
        self.robocopy_mock = self.robocopy.start()
        self.addCleanup(self.robocopy.stop)

    def _tar_record(self, tar_path, sidecar_path, members, *, container_id=7):
        return {
            "tape_label": "T2", "tape_generation_id": 22,
            "tape_generation": 2, "is_packed": True,
            "container_id": container_id, "container_format": "stored_tar",
            "format_version": STORED_TAR_FORMAT_VERSION,
            "tar_dialect": STORED_TAR_DIALECT,
            "local_container_locator": tar_path,
            "tape_container_locator": "Z:\\containers\\opaque.data",
            "local_sidecar_locator": sidecar_path,
            "sidecar_artifact_version": TAR_SIDECAR_VERSION,
            "expected_member_count": len(members),
            "expected_logical_bytes": sum(len(c if isinstance(c, bytes)
                                               else c.encode())
                                          for _n, c in members),
        }

    def test_tar_only_local_restore_uses_format_not_extension(self):
        members = [("project/a.txt", b"tar-data")]
        tar_path = os.path.join(self.tmp, "looks-like.zip")
        sidecar = os.path.join(self.tmp, "one.jsonl.zst")
        _write_tar(tar_path, members)
        _write_sidecar(sidecar, 7, "/srv", members)
        record = self._tar_record(tar_path, sidecar, members)
        record.update({
            "member_name": "project/a.txt", "stored_path": "project/a.txt",
            "original_path": "/srv/project/a.txt", "file_name": "a.txt",
            "file_size_bytes": 8,
        })

        self.assertEqual(self.retriever._restore_container([record]), 1)

        with open(os.path.join(self.restore, "a.txt"), "rb") as handle:
            self.assertEqual(handle.read(), b"tar-data")
        self.retriever._verify_tape.assert_not_called()

    def test_zip_tar_and_loose_restore_in_one_request(self):
        zip_path = os.path.join(self.tmp, "zip.container")
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("zip/a.txt", "zip")
        loose_path = os.path.join(self.tmp, "loose.bin")
        with open(loose_path, "wb") as handle:
            handle.write(b"loose")
        members = [("tar/b.txt", b"tar")]
        tar_path = os.path.join(self.tmp, "tar.container")
        sidecar = os.path.join(self.tmp, "mixed.jsonl.zst")
        _write_tar(tar_path, members)
        _write_sidecar(sidecar, 9, "/srv", members)
        tar_record = self._tar_record(
            tar_path, sidecar, members, container_id=9)
        tar_record.update({
            "member_name": "tar/b.txt", "stored_path": "tar/b.txt",
            "original_path": "/srv/tar/b.txt", "file_name": "b.txt",
            "file_size_bytes": 3,
        })
        records = [{
            "tape_label": "T1", "is_packed": True,
            "container_id": None, "container_format": "zip",
            "container_name": zip_path, "tape_container_locator": zip_path,
            "stored_path": "zip/a.txt", "original_path": "/srv/zip/a.txt",
            "file_name": "a.txt", "file_size_bytes": 3,
        }, tar_record, {
            "tape_label": "T3", "is_packed": False,
            "stored_path": loose_path, "original_path": "/srv/loose.bin",
            "file_name": "loose.bin", "file_size_bytes": 5,
        }]

        self.retriever._restore_many(records)

        found = {}
        for name in ("a.txt", "b.txt", "loose.bin"):
            with open(os.path.join(self.restore, name), "rb") as handle:
                found[name] = handle.read()
        self.assertEqual(found, {
            "a.txt": b"zip", "b.txt": b"tar", "loose.bin": b"loose"})
        self.assertEqual(
            [call.args[0] for call in self.retriever._verify_tape.call_args_list],
            ["T1", "T3"])

    def test_tar_publication_renames_without_clobbering(self):
        members = [("a.txt", b"new")]
        tar_path = os.path.join(self.tmp, "data.tar")
        sidecar = os.path.join(self.tmp, "data.jsonl.zst")
        _write_tar(tar_path, members)
        _write_sidecar(sidecar, 11, "/srv", members)
        record = self._tar_record(
            tar_path, sidecar, members, container_id=11)
        record.update({
            "member_name": "a.txt", "stored_path": "a.txt",
            "original_path": "/srv/a.txt", "file_name": "a.txt",
            "file_size_bytes": 3,
        })
        with open(os.path.join(self.restore, "a.txt"), "wb") as handle:
            handle.write(b"unrelated")

        self.assertEqual(self.retriever._restore_container([record]), 1)

        with open(os.path.join(self.restore, "a.txt"), "rb") as handle:
            self.assertEqual(handle.read(), b"unrelated")
        with open(os.path.join(self.restore, "a_1.txt"), "rb") as handle:
            self.assertEqual(handle.read(), b"new")
        self.assertFalse(any(name.startswith(".restore_tar_")
                             for name in os.listdir(self.restore)))

    def test_cancellation_removes_only_own_unique_temp(self):
        members = [("a.bin", b"x" * (2 * 1024 * 1024))]
        tar_path = os.path.join(self.tmp, "cancel.tar")
        sidecar = os.path.join(self.tmp, "cancel.jsonl.zst")
        _write_tar(tar_path, members)
        _write_sidecar(sidecar, 13, "/srv", members)
        record = self._tar_record(
            tar_path, sidecar, members, container_id=13)
        record.update({
            "member_name": "a.bin", "stored_path": "a.bin",
            "original_path": "/srv/a.bin", "file_name": "a.bin",
            "file_size_bytes": len(members[0][1]),
        })
        unrelated = os.path.join(self.restore, ".restore_tar_unrelated.part")
        with open(unrelated, "wb") as handle:
            handle.write(b"keep")
        original = self.retriever._copy_tar_stream

        def cancel_then_copy(source, output, expected_size):
            CANCEL.set()
            return original(source, output, expected_size)

        with mock.patch.object(
                self.retriever, "_copy_tar_stream",
                side_effect=cancel_then_copy):
            with self.assertRaisesRegex(RuntimeError, "Cancelled"):
                self.retriever._restore_container([record])
        self.assertTrue(os.path.exists(unrelated))
        self.assertEqual(os.listdir(self.restore),
                         [".restore_tar_unrelated.part"])

    def test_missing_or_ltfs_sidecar_refuses_before_any_tape_operation(self):
        template = {
            "tape_label": "T9", "is_packed": True, "container_id": 99,
            "container_format": "stored_tar",
            "format_version": STORED_TAR_FORMAT_VERSION,
            "tar_dialect": STORED_TAR_DIALECT,
            "tape_container_locator": "Z:\\containers\\data.tar",
        }
        with mock.patch.object(
                self.retriever, "_bundle_staging_space_ok") as space:
            for locator in (None, os.path.join(self.tmp, "missing.jsonl.zst"),
                            "Z:\\metadata\\data.jsonl.zst"):
                record = dict(template, local_sidecar_locator=locator)
                self.assertEqual(
                    self.retriever._restore_container([record]), 0)
        self.retriever._verify_tape.assert_not_called()
        space.assert_not_called()
        self.assertEqual(self.robocopy_mock.call_count, 0)

    def test_unsafe_sidecar_member_is_refused(self):
        sidecar = os.path.join(self.tmp, "unsafe.jsonl.zst")
        _write_sidecar(sidecar, 5, "/srv", [("../escape", b"bad")])
        with self.assertRaises(ArtifactError):
            search_tar_sidecar(sidecar, expected_container_id=5)
        self.assertEqual(os.listdir(self.restore), [])

    def test_multi_container_directory_restore(self):
        zip_path = os.path.join(self.tmp, "directory.zip")
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("project/from_zip.txt", "zip")
        members = [("project/from_tar.txt", b"tar")]
        tar_path = os.path.join(self.tmp, "directory.tar")
        sidecar = os.path.join(self.tmp, "directory.jsonl.zst")
        _write_tar(tar_path, members)
        _write_sidecar(sidecar, 31, "/srv", members)

        class _DB:
            @staticmethod
            def find_directory_restore_bundles(*_args, **_kwargs):
                return [{
                    "tape_label": "TZ", "container_id": None,
                    "container_format": "zip", "format_version": "legacy-zip-v1",
                    "stored_bundle_path": zip_path,
                    "tape_container_locator": zip_path,
                    "source_base_path": "/srv", "base_path": "/srv",
                    "is_packed": True,
                }, {
                    "tape_label": "TT", "container_id": 31,
                    "container_format": "stored_tar",
                    "format_version": STORED_TAR_FORMAT_VERSION,
                    "tar_dialect": STORED_TAR_DIALECT,
                    "stored_bundle_path": "Z:\\opaque\\directory.tar",
                    "tape_container_locator": "Z:\\opaque\\directory.tar",
                    "local_container_locator": tar_path,
                    "local_sidecar_locator": sidecar,
                    "sidecar_artifact_version": TAR_SIDECAR_VERSION,
                    "expected_member_count": 1,
                    "expected_logical_bytes": 3,
                    "source_base_path": "/srv", "base_path": "/srv",
                    "is_packed": True,
                }]

        self.retriever.db = cast(Any, _DB())
        self.retriever._restore_directory_complete("/srv/project")

        for name, expected in (("from_zip.txt", b"zip"),
                               ("from_tar.txt", b"tar")):
            with open(os.path.join(self.restore, "project", name), "rb") as handle:
                self.assertEqual(handle.read(), expected)
        self.retriever._verify_tape.assert_called_once_with("TZ")

    def test_local_sidecar_search_is_bounded_to_selected_directory(self):
        sidecar = os.path.join(self.tmp, "search.jsonl.zst")
        members = [("one/a.txt", b"a"), ("two/b.txt", b"b")]
        _write_sidecar(sidecar, 41, "/srv", members)

        result = search_tar_sidecar(
            sidecar, directory="/srv/two", expected_container_id=41,
            limit=1)

        self.assertEqual(len(result.expected_members), 2)
        self.assertEqual([row["member_name"] for row in result.matches],
                         ["two/b.txt"])


class SidecarOnlyDirectoryRestoreTests(unittest.TestCase):
    def test_member_without_files_index_row_is_located_and_restored(self):
        CANCEL.clear()
        with tempfile.TemporaryDirectory(prefix="sidecar_only_") as root:
            restore = os.path.join(root, "restore")
            tar_path = os.path.join(root, "container.bin")
            sidecar = os.path.join(root, "container.jsonl.zst")
            members = [("project/sub/rare.dat", b"rare")]
            os.makedirs(restore)
            _write_tar(tar_path, members)
            _write_sidecar(sidecar, 21, "/srv", members)

            class _DB:
                @staticmethod
                def find_directory_restore_bundles(*_args, **_kwargs):
                    return [{
                        "tape_label": "T4", "tape_generation_id": 44,
                        "tape_generation": 1, "container_id": 21,
                        "container_format": "stored_tar",
                        "format_version": STORED_TAR_FORMAT_VERSION,
                        "tar_dialect": STORED_TAR_DIALECT,
                        "local_container_locator": tar_path,
                        "tape_container_locator": "Z:\\opaque\\container.bin",
                        "stored_bundle_path": "Z:\\opaque\\container.bin",
                        "source_base_path": "/srv", "base_path": "/srv",
                        "local_sidecar_locator": sidecar,
                        "sidecar_artifact_version": TAR_SIDECAR_VERSION,
                        "expected_member_count": 1,
                        "expected_logical_bytes": 4,
                    }]

            retriever = LTORetriever(
                db=cast(Any, _DB()), tape_drive="Z:\\",
                staging_dir=os.path.join(root, "stage"), restore_dir=restore,
                manifest_archive_root=root)
            retriever._verify_tape = mock.Mock()

            retriever._restore_directory_complete("/srv/project/sub")

            with open(os.path.join(restore, "sub", "rare.dat"), "rb") as handle:
                self.assertEqual(handle.read(), b"rare")
            retriever._verify_tape.assert_not_called()


if __name__ == "__main__":
    unittest.main()
