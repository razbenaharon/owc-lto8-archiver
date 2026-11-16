"""Regression tests for the fetch/pack integrity audit (H7, H8, H9, H12).

Every test drives the real ``RemoteChunkStager`` over synthetic files in a
temp dir. The remote side is a dict of ``name -> bytes`` served as a real tar
stream into the host's real ``tar`` extractor, so the Windows name mangling
these findings are about (trailing space/dot stripping, NFC composition) is
the genuine behaviour, not a model of it. Nothing here opens SSH or touches a
tape path.
"""
import io
import os
import shutil
import subprocess
import tarfile
import tempfile
import threading
import unicodedata
import unittest
import zipfile
from types import SimpleNamespace
from unittest import mock

from src.orchestrators import RemoteOrchestrator
from src.paths import (_local_collision_key, _long,
                       _remote_fetch_base_and_rel, _winsafe_extracted_rel)
from src.pipeline_types import (ContainerFormat, SourceDisposition,
                                StoredTarSourceDiagnostic)
from src.skipped import SkippedFileTracker

REMOTE_ROOT = "/data"


def _real_tar_available():
    return os.name == "nt" and shutil.which("tar") is not None


class FakeRemote:
    """A synthetic remote tree, streamed through the real local extractor."""

    def __init__(self, files):
        self.files = dict(files)        # remote rel path -> bytes
        self.requests = []              # every rel-path list asked for
        self.probes = []
        self.probe_error = None
        self.probe_overrides = {}       # rel -> SourceDisposition

    def fetch(self, _user, _host, base, rel_paths, dest, **_kwargs):
        assert base == REMOTE_ROOT, base
        self.requests.append(list(rel_paths))
        stream = io.BytesIO()
        with tarfile.open(fileobj=stream, mode="w",
                          format=tarfile.PAX_FORMAT) as archive:
            for rel in rel_paths:
                payload = self.files.get(rel)
                if payload is None:
                    continue            # GNU tar: "Cannot stat" (tolerated)
                info = tarfile.TarInfo(rel)
                info.size = len(payload)
                archive.addfile(info, io.BytesIO(payload))
        os.makedirs(dest, exist_ok=True)
        done = subprocess.run(["tar", "-C", dest, "-xf", "-"],
                              input=stream.getvalue(), capture_output=True)
        if done.returncode != 0:
            return False, (f"local tar exit {done.returncode}: "
                           f"{done.stderr.decode('utf-8', 'replace')}")
        return True, ""

    def probe(self, _user, _host, base, members, **_kwargs):
        assert base == REMOTE_ROOT, base
        members = list(members)
        self.probes.append(members)
        if self.probe_error:
            return (), (self.probe_error,)
        observed = []
        for ordinal, rel in members:
            disposition = self.probe_overrides.get(rel)
            if disposition is None and rel not in self.files:
                disposition = SourceDisposition.SOURCE_MISSING
            if disposition is not None:
                observed.append(StoredTarSourceDiagnostic(
                    plan_ordinal=ordinal, path=rel,
                    disposition=disposition, evidence="fake"))
        return tuple(observed), ()


class FakeDB:
    def __init__(self, rows, chunk_format=ContainerFormat.ZIP):
        self.rows = {row["manifest_id"]: row for row in rows}
        self.chunk_statuses = []
        self.chunk_format = chunk_format

    # -- reads -----------------------------------------------------------
    def count_chunks(self, _session_id):
        return 1

    def get_chunk_files(self, _session_id, _chunk_index):
        return [dict(row) for row in self.rows.values()]

    def get_chunk_packaging_format(self, _session_id, _chunk_index):
        return self.chunk_format

    # -- writes ----------------------------------------------------------
    def update_chunk_status(self, _session_id, _chunk_index, status):
        self.chunk_statuses.append(status)

    def update_manifest_row(self, manifest_id, session_id=None, **fields):
        self.rows[manifest_id].update(fields)

    def update_manifest_rows_fetching(self, ids, session_id=None):
        for manifest_id in ids:
            self.rows[manifest_id]["status"] = "fetching"

    def update_manifest_rows_fetched(self, updates, session_id=None):
        for local_rel, manifest_id in updates:
            self.rows[manifest_id].update(
                status="fetched", local_rel_path=local_rel)

    def update_manifest_rows_fetch_failed(self, ids, err, session_id=None):
        for manifest_id in ids:
            self.rows[manifest_id].update(
                status="fetch_failed", error_msg=str(err))


def make_rows(remote_files, missing=()):
    rows = []
    names = list(remote_files) + list(missing)
    for index, rel in enumerate(names):
        size = len(remote_files[rel]) if rel in remote_files else 5
        rows.append({
            "manifest_id": index + 1,
            "remote_path": f"{REMOTE_ROOT}/{rel}",
            "file_size_bytes": size,
            "status": "pending",
            "error_msg": None,
            "local_rel_path": None,
        })
    return rows


def make_host(staging, db):
    host = RemoteOrchestrator.__new__(RemoteOrchestrator)
    host.db = db
    host.staging_dir = staging
    host.remote_path = REMOTE_ROOT
    host.remote_user = "u"
    host.remote_host = "synthetic.invalid"
    host.remote_password = ""
    host.ssh_cipher = ""
    host.use_mbuffer = False
    host.mbuffer_size = "1G"
    host.fetch_cores = None
    host.fetch_parallel_streams = 1
    host.fetch_transient_retries = 0
    host.fetch_transient_retry_base = 0
    host.fetch_abort_factor = 2.0
    host.fetch_stall_timeout = 600
    host.metadata_batch_size = 1000
    host.pack_file_batch_size = 1000
    host.pack_parallel_workers = 1
    host.ram_sample_interval = 0
    host.notifier = None
    host.governor = None
    host.skipped_tracker = SkippedFileTracker()
    host._staged_bytes = 0
    host._staged_lock = threading.Lock()
    host._producer_chunk = None
    host._last_fetch_failure = None
    host.cfg = SimpleNamespace(
        max_zip_size_gb=1, index_min_file_mb=10,
        index_packed_small_files=False, small_file_manifest_enabled=False,
        small_file_manifest_format="jsonl",
        small_file_manifest_compression="none", zip_threshold_mb=10,
        stored_tar_max_size_gb=1,
        local_manifest_archive_root=os.path.join(staging, "_manifests"))
    return host


class _Harness(unittest.TestCase):
    def setUp(self):
        if not _real_tar_available():
            self.skipTest("needs the Windows tar extractor")
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._cleanup_tmp)
        self.staging = self._tmp.name

    def _cleanup_tmp(self):
        shutil.rmtree(_long(self.staging), ignore_errors=True)
        try:
            self._tmp.cleanup()
        except OSError:
            pass

    def build(self, remote_files, missing=()):
        self.remote = FakeRemote(remote_files)
        self.db = FakeDB(make_rows(remote_files, missing))
        self.host = make_host(self.staging, self.db)
        self.fetch_dir = os.path.join(self.staging, "_fetch_s0001_000")
        self.pack_dir = os.path.join(self.staging, "_pack_s0001_000")
        patches = [
            mock.patch("src.remote_staging._remote_tar_fetch",
                       side_effect=self.remote.fetch),
            mock.patch("src.remote_staging._remote_source_status_probe",
                       side_effect=self.remote.probe, create=True),
        ]
        for patcher in patches:
            patcher.start()
            self.addCleanup(patcher.stop)

    def fetch(self):
        return self.host._fetch_chunk(
            1, 0, self.db.get_chunk_files(1, 0), self.fetch_dir)

    def stage(self):
        return self.host._stage_chunk(1, 0, self.db.get_chunk_files(1, 0))

    def row(self, rel):
        wanted = f"{REMOTE_ROOT}/{rel}"
        return next(row for row in self.db.rows.values()
                    if row["remote_path"] == wanted)

    def staged_bytes(self, rel):
        local_rel = self.row(rel)["local_rel_path"]
        self.assertTrue(local_rel, f"{rel!r} has no staged path")
        path = os.path.join(self.fetch_dir, local_rel.replace("/", os.sep))
        with open(_long(path), "rb") as handle:
            return handle.read()

    def requested(self):
        return [rel for batch in self.remote.requests for rel in batch]


class TrailingSpaceNameTests(_Harness):
    """H8: a significant trailing character must survive to the request."""

    def test_request_mapping_keeps_the_exact_remote_name(self):
        for name in ("d/file ", "d/file.", "d/tab\t", "d/ lead", "d /x"):
            with self.subTest(name=name):
                base, rel = _remote_fetch_base_and_rel(
                    REMOTE_ROOT, f"{REMOTE_ROOT}/{name}")
                self.assertEqual(base, REMOTE_ROOT)
                self.assertEqual(rel, name)

    def test_staged_mapping_is_an_unambiguous_encoding(self):
        # Windows cannot hold "file " and "file" side by side: both land on
        # one name, so they must share a collision key and never a path.
        self.assertEqual(_winsafe_extracted_rel("d/file "), "d/file")
        self.assertEqual(_local_collision_key(_winsafe_extracted_rel("d/file ")),
                         _local_collision_key(_winsafe_extracted_rel("d/FILE")))

    def test_trailing_space_file_is_requested_by_its_exact_name(self):
        self.build({"d/file ": b"SPACE"})
        ok, missing, count = self.fetch()
        self.assertTrue(ok)
        self.assertEqual((missing, count), ([], 1))
        self.assertEqual(self.requested(), ["d/file "])
        self.assertEqual(self.staged_bytes("d/file "), b"SPACE")

    def test_same_size_sibling_never_donates_its_content(self):
        for order in (("d/file ", "d/file"), ("d/file", "d/file ")):
            with self.subTest(order=order):
                shutil.rmtree(_long(self.staging), ignore_errors=True)
                os.makedirs(self.staging)
                content = {"d/file ": b"SPACE", "d/file": b"PLAIN"}
                self.build({name: content[name] for name in order})
                ok, missing, count = self.fetch()
                self.assertTrue(ok)
                self.assertEqual((missing, count), ([], 2))
                self.assertCountEqual(self.requested(), ["d/file ", "d/file"])
                self.assertEqual(self.staged_bytes("d/file "), b"SPACE")
                self.assertEqual(self.staged_bytes("d/file"), b"PLAIN")
                self.assertNotEqual(
                    _local_collision_key(self.row("d/file ")["local_rel_path"]),
                    _local_collision_key(self.row("d/file")["local_rel_path"]))

    def test_trailing_dot_collision_is_kept_apart(self):
        self.build({"d/name.": b"DOTTED", "d/name": b"PLAIN!"})
        ok, _missing, count = self.fetch()
        self.assertTrue(ok)
        self.assertEqual(count, 2)
        self.assertEqual(self.staged_bytes("d/name."), b"DOTTED")
        self.assertEqual(self.staged_bytes("d/name"), b"PLAIN!")


NFD_NAME = unicodedata.normalize("NFD", "d/café.txt")
NFC_NAME = unicodedata.normalize("NFC", "d/café.txt")


class UnicodeNormalisationTests(_Harness):
    """H9: a name is not 'missing at source' because its form changed."""

    def test_decomposed_name_is_staged_not_recorded_missing(self):
        self.assertNotEqual(NFD_NAME, NFC_NAME)
        self.build({NFD_NAME: b"NFD-DATA"})
        ok, missing, count = self.fetch()
        self.assertTrue(ok)
        self.assertEqual(missing, [])
        self.assertEqual(count, 1)
        self.assertEqual(self.requested(), [NFD_NAME])
        self.assertEqual(self.row(NFD_NAME)["status"], "fetched")
        self.assertEqual(self.staged_bytes(NFD_NAME), b"NFD-DATA")
        self.assertEqual(self.remote.probes, [])

    def test_composed_and_decomposed_twins_are_not_merged(self):
        for order in ((NFD_NAME, NFC_NAME), (NFC_NAME, NFD_NAME)):
            with self.subTest(first=ascii(order[0])):
                shutil.rmtree(_long(self.staging), ignore_errors=True)
                os.makedirs(self.staging)
                content = {NFD_NAME: b"DECOMPOSED", NFC_NAME: b"COMPOSED!!"}
                self.build({name: content[name] for name in order})
                ok, missing, count = self.fetch()
                self.assertTrue(ok)
                self.assertEqual((missing, count), ([], 2))
                self.assertEqual(self.staged_bytes(NFD_NAME), b"DECOMPOSED")
                self.assertEqual(self.staged_bytes(NFC_NAME), b"COMPOSED!!")

    def test_ambiguous_normalisation_match_is_reported_not_guessed(self):
        from src.paths import _resolve_extracted_path
        root = os.path.join(self.staging, "amb")
        os.makedirs(root)
        composed = "ḗ.txt"                    # e + macron + acute
        variants = (unicodedata.normalize("NFD", composed),
                    "ḗ.txt")
        for name in variants:
            with open(os.path.join(root, name), "wb") as handle:
                handle.write(b"x")
        self.assertEqual(len(os.listdir(root)), 2)
        path, rel, ambiguous = _resolve_extracted_path(root, composed)
        self.assertIsNone(path)
        self.assertTrue(ambiguous)
        os.remove(os.path.join(root, variants[1]))
        path, rel, ambiguous = _resolve_extracted_path(root, composed)
        self.assertFalse(ambiguous)
        self.assertEqual(rel, variants[0])
        self.assertTrue(os.path.isfile(path))


class SourceMissingNeedsProofTests(_Harness):
    """H9: only the source host can excuse a planned file from a chunk."""

    def test_remote_confirmed_absence_is_recorded_as_source_missing(self):
        self.build({"d/a.txt": b"AAAAA"}, missing=("d/gone.txt",))
        ok, missing, count = self.fetch()
        self.assertTrue(ok)
        self.assertEqual(count, 1)
        self.assertEqual([item["remote_path"] for item in missing],
                         [f"{REMOTE_ROOT}/d/gone.txt"])
        self.assertEqual(self.row("d/gone.txt")["status"], "source_missing")
        self.assertEqual(self.remote.probes, [[(0, "d/gone.txt")]])

    def test_locally_absent_but_remotely_present_file_fails_the_chunk(self):
        self.build({"d/a.txt": b"AAAAA", "d/b.txt": b"BBBBB"})
        real_fetch = self.remote.fetch

        def lossy(user, host, base, rel_paths, dest, **kwargs):
            # The extractor silently fails to materialise b.txt.
            kept = [rel for rel in rel_paths if rel != "d/b.txt"]
            return real_fetch(user, host, base, kept, dest, **kwargs)

        with mock.patch("src.remote_staging._remote_tar_fetch",
                        side_effect=lossy):
            ok, missing, _count = self.fetch()
        self.assertFalse(ok)
        self.assertEqual(missing, [])
        self.assertEqual(self.row("d/b.txt")["status"], "fetch_failed")
        self.assertIn("reports it as present", self.row("d/b.txt")["error_msg"])

    def test_unanswerable_probe_fails_the_chunk(self):
        self.build({"d/a.txt": b"AAAAA"}, missing=("d/gone.txt",))
        self.remote.probe_error = "source status probe failed (exit 255)"
        ok, missing, _count = self.fetch()
        self.assertFalse(ok)
        self.assertEqual(missing, [])
        self.assertEqual(self.row("d/gone.txt")["status"], "fetch_failed")

    def test_non_regular_source_is_not_an_excuse(self):
        self.build({"d/a.txt": b"AAAAA"}, missing=("d/now_a_dir",))
        self.remote.probe_overrides["d/now_a_dir"] = (
            SourceDisposition.SOURCE_CHANGED)
        ok, _missing, _count = self.fetch()
        self.assertFalse(ok)
        self.assertEqual(self.row("d/now_a_dir")["status"], "fetch_failed")

    def test_stale_source_missing_row_is_refetched_when_source_has_it(self):
        self.build({"d/a.txt": b"AAAAA", NFD_NAME: b"NFD-DATA"})
        stale = self.row(NFD_NAME)
        stale.update(status="source_missing",
                     error_msg="missing after tar fetch")
        ok, missing, count = self.fetch()
        self.assertTrue(ok)
        self.assertEqual((missing, count), ([], 2))
        self.assertEqual(self.row(NFD_NAME)["status"], "fetched")
        self.assertEqual(self.staged_bytes(NFD_NAME), b"NFD-DATA")

    def test_recorded_missing_row_stays_missing_when_source_agrees(self):
        self.build({"d/a.txt": b"AAAAA"}, missing=("d/gone.txt",))
        self.row("d/gone.txt").update(status="source_missing")
        ok, missing, count = self.fetch()
        self.assertTrue(ok)
        self.assertEqual(count, 1)
        self.assertEqual(len(missing), 1)
        self.assertEqual(self.requested(), ["d/a.txt"])


def _bundle_contents(pack_dir):
    """{zip entry name: bytes} over every bundle in a pack dir."""
    found = {}
    for name in sorted(os.listdir(pack_dir)):
        if name.lower().endswith(".zip"):
            with zipfile.ZipFile(os.path.join(pack_dir, name)) as archive:
                for entry in archive.namelist():
                    found[entry] = archive.read(entry)
    return found


class PackReconciliationTests(_Harness):
    """H7: a chunk reaches the writer with every fetched file, or not at all."""

    FILES = {"d/one.txt": b"ONE-ONE", "d/two.txt": b"TWO-TWO-TWO",
             "d/three.txt": b"THREE"}

    def _stage_with_locked_file(self, locked_rel, workers=1):
        """Run the real packer while one fetched file is unreadable."""
        import msvcrt
        from src.packer import LTOPacker
        self.host.pack_parallel_workers = workers
        real_run = LTOPacker.run

        def run_with_lock(packer, *args, **kwargs):
            victim = os.path.join(
                self.fetch_dir, locked_rel.replace("/", os.sep))
            handle = open(victim, "r+b")
            size = os.path.getsize(victim)
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, size)
            try:
                return real_run(packer, *args, **kwargs)
            finally:
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, size)
                handle.close()

        with mock.patch.object(LTOPacker, "run", run_with_lock):
            return self.stage()

    def test_clean_chunk_is_staged_with_every_file(self):
        self.build(self.FILES)
        desc = self.stage()
        self.assertIsNotNone(desc)
        self.assertEqual(len(desc.metadata), 3)
        self.assertEqual(
            _bundle_contents(self.pack_dir),
            {"d/one.txt": b"ONE-ONE", "d/two.txt": b"TWO-TWO-TWO",
             "d/three.txt": b"THREE"})

    def test_file_that_fails_while_packing_fails_the_chunk(self):
        from src.remote_staging import ChunkReconciliationError
        for workers in (1, 2):
            with self.subTest(workers=workers):
                shutil.rmtree(_long(self.staging), ignore_errors=True)
                os.makedirs(self.staging)
                self.build(self.FILES)
                with self.assertRaises(ChunkReconciliationError) as caught:
                    self._stage_with_locked_file("d/two.txt", workers)
                self.assertIn("3 file(s) fetched but 2 packed",
                              str(caught.exception))
                self.assertIn(f"{REMOTE_ROOT}/d/two.txt", str(caught.exception))
                self.assertEqual(self.db.chunk_statuses[-1], "fetch_failed")
                # Never left where the writer or a resume could pick it up...
                self.assertFalse(os.path.exists(self.pack_dir))
                self.assertIsNone(
                    self.host._try_resume_pack(1, 0, self.pack_dir))
                # ...but kept for diagnosis.
                kept = self.pack_dir + ".reconcile_failed"
                self.assertTrue(os.path.isdir(kept))
                self.assertNotIn("d/two.txt", _bundle_contents(kept))
                self.assertEqual(self.host._staged_bytes, 0)

    def test_confirmed_source_missing_file_is_reconciled_not_fatal(self):
        self.build(self.FILES, missing=("d/gone.txt",))
        desc = self.stage()
        self.assertEqual(len(desc.metadata), 3)
        self.assertEqual(len(desc.source_missing_files), 1)

    def test_odd_names_reach_the_bundle_with_their_own_bytes(self):
        files = {"d/file ": b"SPACE", "d/file": b"PLAIN",
                 NFD_NAME: b"DECOMPOSED", NFC_NAME: b"COMPOSED!!"}
        self.build(files)
        desc = self.stage()
        self.assertEqual(len(desc.metadata), 4)
        bundle = _bundle_contents(self.pack_dir)
        by_source = {item["canonical_source_path"]: bundle[item["stored_path"]]
                     for item in desc.metadata}
        self.assertEqual(
            by_source,
            {f"{REMOTE_ROOT}/{rel}": data for rel, data in files.items()})


class ReconcileFunctionTests(unittest.TestCase):
    """The pure comparison: count AND identity AND size."""

    def _case(self):
        planned = [
            {"remote_path": "/data/a", "file_size_bytes": 3},
            {"remote_path": "/data/b", "file_size_bytes": 4},
            {"remote_path": "/data/gone", "file_size_bytes": 9},
        ]
        missing = [{"remote_path": "/data/gone"}]
        rows = [
            {"remote_path": "/data/a", "local_rel_path": "a"},
            {"remote_path": "/data/b", "local_rel_path": "b"},
            {"remote_path": "/data/gone", "local_rel_path": None},
        ]
        metadata = [
            {"stored_path": "a", "file_size_bytes": 3, "is_packed": True},
            {"stored_path": "b", "file_size_bytes": 4, "is_packed": True},
        ]
        return planned, missing, rows, metadata

    def _problems(self, planned, missing, rows, metadata, fetched=2):
        from src.remote_staging import reconcile_packed_chunk
        return reconcile_packed_chunk(planned, missing, fetched, rows, metadata)

    def test_consistent_chunk_has_no_discrepancy(self):
        self.assertEqual(self._problems(*self._case()), [])

    def test_dropped_file_is_named(self):
        planned, missing, rows, metadata = self._case()
        problems = self._problems(planned, missing, rows, metadata[:1])
        self.assertTrue(any("fetched but not packed: /data/b" in item
                            for item in problems), problems)

    def test_same_count_but_wrong_identity_is_caught(self):
        planned, missing, rows, metadata = self._case()
        metadata[1] = {"stored_path": "stray", "file_size_bytes": 4,
                       "is_packed": True}
        problems = self._problems(planned, missing, rows, metadata)
        self.assertTrue(any("packed but not planned: stray" in item
                            for item in problems), problems)
        self.assertTrue(any("fetched but not packed: /data/b" in item
                            for item in problems), problems)

    def test_same_identity_but_wrong_size_is_caught(self):
        planned, missing, rows, metadata = self._case()
        metadata[0]["file_size_bytes"] = 0
        problems = self._problems(planned, missing, rows, metadata)
        self.assertTrue(any("packed size 0 differs from planned 3" in item
                            for item in problems), problems)

    def test_fetch_count_must_account_for_every_planned_file(self):
        planned, missing, rows, metadata = self._case()
        problems = self._problems(planned, [], rows, metadata)
        self.assertTrue(any("planned 3 file(s)" in item for item in problems),
                        problems)


class MixedStoredTarDB(FakeDB):
    """FakeDB plus the Stored TAR container/artifact repository surface."""

    def __init__(self, rows, chunk_plan):
        super().__init__(rows, chunk_format="stored_tar")
        self.chunk_plan = chunk_plan
        self.container = {
            "container_id": 7, "session_id": 1, "chunk_index": 0,
            "container_ordinal": 0, "container_format": "stored_tar",
            "format_version": "stored-tar-v1",
            "tar_dialect": "gnu-pax-sparse-v1",
            "storage_class": "small_files",
            "container_name": "container_000.tar",
            "temporary_data_locator": None,
            "permanent_local_metadata_locator": None,
            "expected_member_count": 1, "expected_logical_bytes": 3,
            "observed_member_count": None, "observed_logical_bytes": None,
            "actual_artifact_bytes": None, "validation_state": "planned",
            "writer_state": "not_started", "catalog_state": "not_started",
            "owner_token": None, "validated_part_locator": None,
            "validation_summary": None,
        }
        self.artifacts = []

    def require_existing_stored_tar_recovery(self, _session, _chunk):
        return True

    def get_or_create_stored_tar_chunk_plan(self, *_args, **_kwargs):
        return self.chunk_plan

    def get_archive_containers(self, _session, _chunk):
        return [dict(self.container)]

    def get_archive_artifacts(self, _session, _chunk):
        return [dict(item) for item in self.artifacts]

    def claim_stored_tar_container_build(self, _id, owner, part_path):
        self.container.update(owner_token=owner,
                              validated_part_locator=part_path,
                              validation_state="building")
        return True

    def mark_stored_tar_validated_part(self, _id, owner, part_path,
                                       validation, diagnostics):
        self.container.update({
            "owner_token": owner, "validated_part_locator": part_path,
            "validation_state": "validated_part",
            "actual_artifact_bytes": validation.archive_size,
            "observed_member_count": validation.member_count,
            "observed_logical_bytes": validation.logical_bytes,
            "validation_summary": {
                "source_diagnostics": list(diagnostics or ())},
        })
        return True

    def publish_stored_tar_pair(self, **values):
        self.container.update({
            "temporary_data_locator": values["temporary_data_locator"],
            "permanent_local_metadata_locator": values["sidecar_locator"],
            "actual_artifact_bytes": values["tar_size_bytes"],
            "observed_member_count": values["observed_member_count"],
            "observed_logical_bytes": values["observed_logical_bytes"],
            "validation_state": "ready", "owner_token": None,
        })
        artifact = {
            "artifact_id": 9, "session_id": 1, "chunk_index": 0,
            "container_id": 7, "artifact_kind": "tar_sidecar",
            "artifact_version": values["sidecar_version"],
            "local_locator": values["sidecar_locator"],
            "artifact_size_bytes": values["sidecar_size_bytes"],
            "readiness_state": "ready",
        }
        self.artifacts = [artifact]
        return {"container": dict(self.container), "artifact": dict(artifact)}


class MixedStoredTarChunkTests(_Harness):
    """H12: a container plus loose large files must carry the large files."""

    LARGE = b"L" * 4096

    def _build_mixed(self, remote_files):
        from test_stored_tar_producer import GNU_RECORD_SIZE, _write_tar
        from src.remote_transport import RemoteTarStoreResult
        self.remote = FakeRemote(remote_files)
        rows = make_rows({"small.txt": b"abc", "big/large.bin": self.LARGE})
        small = SimpleNamespace(
            manifest_id=1, plan_ordinal=0,
            remote_path=f"{REMOTE_ROOT}/small.txt", file_size_bytes=3,
            storage_class="small_files", container_ordinal=0)
        large = SimpleNamespace(
            manifest_id=2, plan_ordinal=1,
            remote_path=f"{REMOTE_ROOT}/big/large.bin",
            file_size_bytes=len(self.LARGE), storage_class="loose",
            container_ordinal=None)
        container_plan = SimpleNamespace(
            session_id=1, chunk_index=0, container_ordinal=0,
            container_name="container_000.tar", expected_member_count=1,
            expected_logical_bytes=3, estimated_archive_bytes=GNU_RECORD_SIZE,
            max_size_bytes=GNU_RECORD_SIZE,
            container_format=ContainerFormat.STORED_TAR,
            format_version="stored-tar-v1", tar_dialect="gnu-pax-sparse-v1",
            storage_class="small_files")
        chunk_plan = SimpleNamespace(
            containers=(container_plan,), members=(small, large),
            small_members=(small,), loose_members=(large,),
            source_missing_members=())
        self.db = MixedStoredTarDB(rows, chunk_plan)
        self.host = make_host(self.staging, self.db)
        self.host.staging_padding = 1.0
        self.fetch_dir = os.path.join(self.staging, "_fetch_s0001_000")
        self.pack_dir = os.path.join(self.staging, "_pack_s0001_000")

        def store(_user, _host, _base, paths, part, **_kwargs):
            self.assertEqual(paths, ["small.txt"])
            _write_tar(part, [("small.txt", b"abc")])
            return RemoteTarStoreResult(
                True, part, os.path.getsize(part), 0, "")

        for patcher in (
                mock.patch("src.remote_staging._remote_tar_fetch",
                           side_effect=self.remote.fetch),
                mock.patch("src.remote_staging._remote_source_status_probe",
                           side_effect=self.remote.probe),
                mock.patch("src.remote_staging._remote_tar_store",
                           side_effect=store)):
            patcher.start()
            self.addCleanup(patcher.stop)

    def test_large_file_is_carried_into_the_pack_and_declared(self):
        self._build_mixed({"big/large.bin": self.LARGE})
        desc = self.stage()
        self.assertTrue(desc.assert_writer_ready())
        loose = [item for item in desc.metadata if not item["is_packed"]]
        self.assertEqual(
            [(item["canonical_source_path"], item["file_size_bytes"])
             for item in loose],
            [(f"{REMOTE_ROOT}/big/large.bin", len(self.LARGE))])
        staged = os.path.join(self.pack_dir, "big", "large.bin")
        with open(staged, "rb") as handle:
            self.assertEqual(handle.read(), self.LARGE)
        self.assertTrue(os.path.isfile(
            os.path.join(self.pack_dir, "container_000.tar")))
        self.assertEqual(desc.loose_file_bytes, len(self.LARGE))
        self.assertEqual(self.remote.requests, [["big/large.bin"]])

    def test_large_file_that_cannot_be_copied_fails_the_chunk(self):
        from src.remote_staging import ChunkReconciliationError
        self._build_mixed({"big/large.bin": self.LARGE})
        with mock.patch("src.packer._robocopy_file", return_value=False):
            with self.assertRaises(ChunkReconciliationError):
                self.stage()
        self.assertEqual(self.db.chunk_statuses[-1], "fetch_failed")

    def test_large_file_named_like_a_pack_object_is_refused(self):
        from src.remote_staging import RemoteChunkStager
        fetch = os.path.join(self.staging, "f")
        pack = os.path.join(self.staging, "p")
        os.makedirs(fetch)
        os.makedirs(pack)
        for root in (fetch, pack):
            with open(os.path.join(root, "container_000.tar"), "wb") as handle:
                handle.write(b"x")
        with self.assertRaisesRegex(RuntimeError, "would overwrite"):
            RemoteChunkStager._assert_loose_targets_free(fetch, pack)


if __name__ == "__main__":
    unittest.main()
