import io
import os
import shutil
import tarfile
import tempfile
import threading
import unittest
from dataclasses import replace
from types import SimpleNamespace
from unittest import mock

from src.archive_artifacts import (
    ArtifactConflict,
    ArtifactError,
    publish_no_clobber,
    publish_stored_tar_pair,
    search_tar_sidecar,
    tar_sidecar_locator,
)
from src.pipeline_types import SourceDisposition, StoredTarSourceDiagnostic
from src.remote_staging import RemoteChunkStager
from src.remote_transport import RemoteTarStoreResult
from src.tar_container import (
    StoredTarError,
    validate_stored_tar_part,
)


GNU_RECORD_SIZE = 512 * 512


def _write_tar(path, members, *, link=None):
    with tarfile.open(path, "w", format=tarfile.PAX_FORMAT) as archive:
        for name, payload in members:
            info = tarfile.TarInfo(name)
            info.size = len(payload)
            info.mtime = 0
            archive.addfile(info, io.BytesIO(payload))
        if link is not None:
            info = tarfile.TarInfo(link)
            info.type = tarfile.SYMTYPE
            info.linkname = "target"
            info.mtime = 0
            archive.addfile(info)
    size = os.path.getsize(path)
    with open(path, "ab") as handle:
        handle.write(bytes((-size) % GNU_RECORD_SIZE))


def _plan(*items):
    return [
        {
            "member_name": name,
            "canonical_source_path": f"/remote/{name}",
            "expected_size": size,
            "plan_ordinal": ordinal,
            "container_ordinal": 0,
        }
        for ordinal, name, size in items
    ]


class _PairDB:
    def __init__(self):
        self.calls = []
        self.lock = threading.Lock()

    def publish_stored_tar_pair(self, **values):
        with self.lock:
            self.calls.append(values)
        return {"ready": True}


class StoredTarPartValidationTests(unittest.TestCase):
    def test_complete_plan_and_machine_diagnostics_are_exactly_equivalent(self):
        with tempfile.TemporaryDirectory() as root:
            part = os.path.join(root, "one.tar.abc.part")
            _write_tar(part, [("present", b"abc")])
            plan = _plan((4, "present", 3), (9, "odd\nname\\x", 7))
            diagnostic = StoredTarSourceDiagnostic(
                9, "odd\nname\\x", SourceDisposition.SOURCE_UNREADABLE,
                "read_probe_failed")
            summary = validate_stored_tar_part(
                part, plan, container_ordinal=0,
                source_diagnostics=(diagnostic,))
        self.assertEqual(summary.member_count, 1)
        self.assertEqual(summary.logical_bytes, 3)
        self.assertEqual(summary.plan_ordinal_count, 2)
        self.assertEqual(summary.disposition_counts["archived"], 1)
        self.assertEqual(summary.disposition_counts["source_unreadable"], 1)

    def test_unclassified_missing_member_blocks_validation(self):
        with tempfile.TemporaryDirectory() as root:
            part = os.path.join(root, "one.tar.abc.part")
            _write_tar(part, [("present", b"abc")])
            with self.assertRaisesRegex(StoredTarError, "missing TAR member"):
                validate_stored_tar_part(
                    part, _plan((0, "present", 3), (1, "missing", 1)),
                    container_ordinal=0)

    def test_wrong_diagnostic_path_and_duplicate_ordinal_are_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            part = os.path.join(root, "one.tar.abc.part")
            _write_tar(part, [])
            plan = _plan((2, "missing", 1))
            wrong = StoredTarSourceDiagnostic(
                2, "other", SourceDisposition.SOURCE_MISSING, "lstat_missing")
            with self.assertRaisesRegex(StoredTarError, "path disagrees"):
                validate_stored_tar_part(
                    part, plan, container_ordinal=0,
                    source_diagnostics=(wrong,))
            duplicate_plan = plan + [dict(plan[0])]
            with self.assertRaisesRegex(StoredTarError, "duplicate plan ordinal"):
                validate_stored_tar_part(
                    part, duplicate_plan, container_ordinal=0)

    def test_unresolved_and_source_changed_diagnostics_block(self):
        with tempfile.TemporaryDirectory() as root:
            part = os.path.join(root, "one.tar.abc.part")
            _write_tar(part, [])
            for disposition in (
                    SourceDisposition.UNRESOLVED,
                    SourceDisposition.SOURCE_CHANGED):
                with self.subTest(disposition=disposition):
                    diagnostic = StoredTarSourceDiagnostic(
                        0, "a", disposition, "observation")
                    with self.assertRaisesRegex(StoredTarError, "blocked"):
                        validate_stored_tar_part(
                            part, _plan((0, "a", 1)), container_ordinal=0,
                            source_diagnostics=(diagnostic,))

    def test_invalid_part_truncation_and_unsupported_member_fail_closed(self):
        with tempfile.TemporaryDirectory() as root:
            invalid = os.path.join(root, "invalid.tar.part")
            with open(invalid, "wb") as handle:
                handle.write(b"not a tar")
            with self.assertRaises(StoredTarError):
                validate_stored_tar_part(
                    invalid, _plan((0, "a", 1)), container_ordinal=0)

            unsupported = os.path.join(root, "link.tar.part")
            _write_tar(unsupported, [], link="link")
            with self.assertRaisesRegex(StoredTarError, "unsupported TAR member"):
                validate_stored_tar_part(
                    unsupported, _plan((0, "link", 0)),
                    container_ordinal=0)

    def test_validation_requires_unpublished_name(self):
        with tempfile.TemporaryDirectory() as root:
            final = os.path.join(root, "one.tar")
            _write_tar(final, [("a", b"x")])
            with self.assertRaisesRegex(StoredTarError, "unpublished .part"):
                validate_stored_tar_part(
                    final, _plan((0, "a", 1)), container_ordinal=0)

    def test_parse_interruption_leaves_only_the_unpublished_part(self):
        with tempfile.TemporaryDirectory() as root:
            part = os.path.join(root, "one.tar.crash.part")
            _write_tar(part, [("a", b"x")])
            with mock.patch(
                    "src.tar_container.validate_stored_tar",
                    side_effect=RuntimeError("parse crash")):
                with self.assertRaisesRegex(RuntimeError, "parse crash"):
                    validate_stored_tar_part(
                        part, _plan((0, "a", 1)), container_ordinal=0)
            self.assertTrue(os.path.isfile(part))
            self.assertFalse(os.path.exists(os.path.join(root, "one.tar")))


class StoredTarStagerTests(unittest.TestCase):
    def test_transient_retry_restarts_at_zero_then_crash_preserves_validated_part(self):
        with tempfile.TemporaryDirectory() as root:
            pack = os.path.join(root, "pack")
            os.makedirs(pack)

            class DB:
                def __init__(self):
                    self.validated = None

                def get_archive_containers(self, _session, _chunk):
                    return [{
                        "container_id": 7, "container_ordinal": 0,
                        "owner_token": None, "validated_part_locator": None,
                        "validation_state": "planned",
                    }]

                def claim_stored_tar_container_build(self, *args, **kwargs):
                    return True

                def mark_stored_tar_validated_part(self, *args):
                    self.validated = args
                    return True

            db = DB()
            host = SimpleNamespace(
                db=db, remote_path="/remote", remote_user="u",
                remote_host="h", remote_password="", ssh_cipher="",
                use_mbuffer=False, mbuffer_size="1G", fetch_cores=None,
                fetch_transient_retries=1, fetch_transient_retry_base=0,
                cfg=SimpleNamespace(
                    local_manifest_archive_root=os.path.join(root, "man")),
                _fetch_backoff_delay=lambda _attempt, _base: 0,
                _note_fetch_failure=lambda *args, **kwargs: None,
            )
            container = SimpleNamespace(
                container_ordinal=0, container_name="one.tar",
                tar_dialect="gnu-pax-sparse-v1",
                format_version="stored-tar-v1")
            member = SimpleNamespace(
                plan_ordinal=0, remote_path="/remote/a",
                file_size_bytes=1)
            attempts = []

            def store(_user, _host, _base, _paths, part, **_kwargs):
                attempts.append(part)
                if len(attempts) == 1:
                    with open(part, "wb") as handle:
                        handle.write(b"partial")
                    return RemoteTarStoreResult(
                        False, part, 7, 255, "Connection reset by peer",
                        error="Connection reset by peer")
                self.assertFalse(os.path.exists(part))
                _write_tar(part, [("a", b"x")])
                return RemoteTarStoreResult(
                    True, part, os.path.getsize(part), 0, "")

            def crash(stage):
                if stage == "after_validated_part_state":
                    raise RuntimeError("validated crash")

            with mock.patch(
                    "src.remote_staging._remote_tar_store",
                    side_effect=store):
                with self.assertRaisesRegex(RuntimeError, "validated crash"):
                    RemoteChunkStager(host)._build_stored_tar_container(
                        2, 3, container, [member], pack,
                        owner_token="owner", crash_hook=crash)
            self.assertEqual(len(attempts), 2)
            self.assertEqual(attempts[0], attempts[1])
            self.assertIsNotNone(db.validated)
            self.assertTrue(os.path.isfile(attempts[-1]))
            self.assertFalse(os.path.exists(os.path.join(pack, "one.tar")))


class StoredTarPairPublicationTests(unittest.TestCase):
    def _fixture(self, root, *, payload=b"abc", with_exception=False):
        pack = os.path.join(root, "pack")
        manifests = os.path.join(root, "permanent")
        os.makedirs(pack)
        part = os.path.join(pack, "container.tar.unique.part")
        final = os.path.join(pack, "container.tar")
        _write_tar(part, [("present", payload)])
        plan = _plan((0, "present", len(payload)))
        diagnostics = ()
        if with_exception:
            plan += _plan((1, "missing\n\\name", 9))
            diagnostics = (StoredTarSourceDiagnostic(
                1, "missing\n\\name", SourceDisposition.SOURCE_MISSING,
                "lstat_missing"),)
        validation = validate_stored_tar_part(
            part, plan, container_ordinal=0,
            source_diagnostics=diagnostics)
        return pack, manifests, part, final, plan, diagnostics, validation

    @staticmethod
    def _publish(fixture, db, *, hook=None):
        pack, manifests, part, final, plan, diagnostics, validation = fixture
        return publish_stored_tar_pair(
            manifests, part, final, plan, diagnostics,
            validation=validation, session_id=2, chunk_index=3,
            container_id=4, container_ordinal=0, owner_token="owner-a",
            db=db, pack_dir=pack, crash_hook=hook)

    def test_sidecar_roundtrip_and_pack_copy_account_for_every_ordinal(self):
        with tempfile.TemporaryDirectory() as root:
            fixture = self._fixture(root, with_exception=True)
            result = self._publish(fixture, _PairDB())
            parsed = search_tar_sidecar(
                result.sidecar_path, expected_container_id=4,
                expected_member_count=1)
            self.assertEqual(
                [item["ordinal"] for item in parsed.expected_members], [0, 1])
            self.assertEqual(
                parsed.expected_members[1]["source_exception"],
                "source_missing")
            self.assertTrue(os.path.isfile(result.pack_sidecar_path))
            shutil.rmtree(fixture[0])
            self.assertTrue(os.path.isfile(result.sidecar_path))

    def test_permission_and_unreadable_dispositions_roundtrip(self):
        for disposition in (
                SourceDisposition.SOURCE_PERMISSION_DENIED,
                SourceDisposition.SOURCE_UNREADABLE):
            with self.subTest(disposition=disposition), \
                    tempfile.TemporaryDirectory() as root:
                fixture = list(self._fixture(root, with_exception=True))
                diagnostic = StoredTarSourceDiagnostic(
                    1, "missing\n\\name", disposition, "machine_probe")
                fixture[5] = (diagnostic,)
                fixture[6] = validate_stored_tar_part(
                    fixture[2], fixture[4], container_ordinal=0,
                    source_diagnostics=(diagnostic,))
                result = self._publish(tuple(fixture), _PairDB())
                parsed = search_tar_sidecar(result.sidecar_path)
                self.assertEqual(
                    parsed.expected_members[1]["source_exception"],
                    disposition.value)

    def test_count_or_byte_summary_mismatch_blocks_publication(self):
        with tempfile.TemporaryDirectory() as root:
            fixture = list(self._fixture(root))
            fixture[6] = replace(fixture[6], member_count=2)
            with self.assertRaisesRegex(ArtifactError, "summary changed"):
                self._publish(tuple(fixture), _PairDB())

    def test_same_size_conflicting_final_tar_is_not_reused(self):
        with tempfile.TemporaryDirectory() as root:
            fixture = self._fixture(root, payload=b"aaa")
            _write_tar(fixture[3], [("present", b"bbb")])
            with self.assertRaisesRegex(ArtifactConflict, "conflicts"):
                self._publish(fixture, _PairDB())
            with open(fixture[3], "rb") as handle:
                final_bytes = handle.read()
            with open(fixture[2], "rb") as handle:
                self.assertNotEqual(final_bytes, handle.read())

    def test_no_clobber_preserves_existing_windows_destination(self):
        with tempfile.TemporaryDirectory() as root:
            part = os.path.join(root, "x.part")
            final = os.path.join(root, "x")
            with open(part, "wb") as handle:
                handle.write(b"new")
            with open(final, "wb") as handle:
                handle.write(b"old")
            with self.assertRaises(ArtifactConflict):
                publish_no_clobber(part, final)
            with open(final, "rb") as handle:
                self.assertEqual(handle.read(), b"old")

    def test_each_publication_crash_window_is_ordered_and_recoverable(self):
        stages = (
            "after_sidecar_validation",
            "after_sidecar_publication",
            "after_tar_publication",
            "before_paired_db_commit",
            "after_paired_db_commit",
        )
        for stage in stages:
            with self.subTest(stage=stage), tempfile.TemporaryDirectory() as root:
                fixture = self._fixture(root, with_exception=True)
                db = _PairDB()

                def crash(current):
                    if current == stage:
                        raise RuntimeError(f"crash:{stage}")

                with self.assertRaisesRegex(RuntimeError, f"crash:{stage}"):
                    self._publish(fixture, db, hook=crash)
                sidecar = os.path.join(
                    fixture[1], *tar_sidecar_locator(2, 3, 0).split("/"))
                if stage == "after_sidecar_validation":
                    self.assertFalse(os.path.exists(sidecar))
                    self.assertTrue(os.path.exists(fixture[2]))
                    self.assertFalse(os.path.exists(fixture[3]))
                else:
                    self.assertTrue(os.path.isfile(sidecar))
                if stage in (
                        "after_tar_publication", "before_paired_db_commit",
                        "after_paired_db_commit"):
                    self.assertTrue(os.path.isfile(fixture[3]))
                    self.assertTrue(os.path.isfile(sidecar))
                expected_calls = 1 if stage == "after_paired_db_commit" else 0
                self.assertEqual(len(db.calls), expected_calls)
                recovered = self._publish(fixture, db)
                self.assertTrue(os.path.isfile(recovered.tar_path))
                self.assertTrue(os.path.isfile(recovered.sidecar_path))

    def test_final_sidecar_plus_tar_part_is_adopted(self):
        with tempfile.TemporaryDirectory() as root:
            fixture = self._fixture(root, with_exception=True)
            with self.assertRaisesRegex(RuntimeError, "stop"):
                self._publish(
                    fixture, _PairDB(),
                    hook=lambda stage: (_ for _ in ()).throw(
                        RuntimeError("stop"))
                    if stage == "after_sidecar_publication" else None)
            self.assertTrue(os.path.isfile(fixture[2]))
            self.assertFalse(os.path.exists(fixture[3]))
            recovered = self._publish(fixture, _PairDB())
            self.assertTrue(os.path.isfile(recovered.tar_path))

    def test_equivalent_final_tar_and_sidecar_are_reused_idempotently(self):
        with tempfile.TemporaryDirectory() as root:
            first = self._fixture(root)
            result = self._publish(first, _PairDB())
            second_part = result.tar_path + ".retry.part"
            shutil.copyfile(result.tar_path, second_part)
            second = (
                first[0], first[1], second_part, result.tar_path,
                first[4], first[5], validate_stored_tar_part(
                    second_part, first[4], container_ordinal=0))
            again = self._publish(second, _PairDB())
            self.assertEqual(again.tar_path, result.tar_path)

    def test_final_tar_without_sidecar_refuses_exception_reconstruction(self):
        with tempfile.TemporaryDirectory() as root:
            fixture = self._fixture(root, with_exception=True)
            os.rename(fixture[2], fixture[3])
            with self.assertRaisesRegex(ArtifactError, "source exceptions existed"):
                self._publish(fixture, _PairDB())

    def test_final_tar_without_sidecar_reconstructs_only_complete_exact_plan(self):
        with tempfile.TemporaryDirectory() as root:
            fixture = self._fixture(root)
            os.rename(fixture[2], fixture[3])
            result = self._publish(fixture, _PairDB())
            self.assertTrue(os.path.isfile(result.sidecar_path))

    def test_two_concurrent_equivalent_publishers_do_not_clobber(self):
        with tempfile.TemporaryDirectory() as root:
            first = self._fixture(root)
            second_part = first[2] + ".second.part"
            shutil.copyfile(first[2], second_part)
            second = (
                first[0], first[1], second_part, first[3], first[4], first[5],
                validate_stored_tar_part(
                    second_part, first[4], container_ordinal=0))
            db = _PairDB()
            errors = []

            def worker(fixture):
                try:
                    self._publish(fixture, db)
                except Exception as exc:  # captured for the test assertion
                    errors.append(exc)

            threads = [
                threading.Thread(target=worker, args=(fixture,))
                for fixture in (first, second)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()
            self.assertEqual(errors, [])
            self.assertTrue(os.path.isfile(first[3]))


if __name__ == "__main__":
    unittest.main()
