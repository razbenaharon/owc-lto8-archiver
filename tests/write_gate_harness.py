"""Shared harness for the behavioural pre-write gate tests (audit M11).

The audit found that the suite proved the gates *exist* but not that the
writers *obey* them: a mutant whose writer ignored a "block" verdict, whose
media-health check always answered "healthy", or which never marked a chunk
``backing`` before the copy, passed every test. The cause was that tests
stubbed the gate methods themselves (``orch._pre_write_safety_gate = lambda:
None``) or asserted on source text.

This harness stubs nothing inside the product. It replaces only the outermost
boundaries the product reaches the world through, and records every one of
them in ONE ordered call log so tests can assert on ordering:

==========================  ==================================================
boundary                    stand-in
==========================  ==================================================
PowerShell (event log,      :meth:`GateWorld._powershell` - answers the LTFS
SCCM client SDK)            mount query and the SCCM restart query; any other
                            command is an error
Windows registry            :class:`FakeRegistry` (pending-restart markers)
IBM ``LogFile.csv``         a temp CSV file the real parser reads
mounted volume label        :meth:`GateWorld._read_label`
LTFS ownership mutex        :meth:`GateWorld._acquire` / ``_release``
drive readiness probe       :meth:`GateWorld._readiness`
first write-touch (mkdir)   :meth:`GateWorld._makedirs` - real mkdir, recorded
robocopy (the tape copy)    :meth:`GateWorld._copy` - copies into a temp dir
tape eject                  :meth:`GateWorld._eject`
PostgreSQL catalog          :class:`FakeCatalog`
==========================  ==================================================

The "tape" is a temp directory; nothing here can reach a real drive.

Not collected by pytest (no ``test_`` prefix).
"""
import contextlib
import json
import os
import shutil
import tempfile
import threading
from datetime import datetime
from types import SimpleNamespace
from unittest import mock

from lto_fakes import MinimalBackupDB, TAPE_LOCK_USE_SITES

from src import ltfs
from src.ltfs_ownership import LtfsOwnershipError
from src.pipeline_types import (CHUNK_TRANSITIONS, ChunkStatus,
                                ContainerFormat, ForbiddenTransition,
                                StagedChunk)
from src.runtime import CANCEL
from src.skipped import SkippedFileTracker

SESSION_ID = 37
TAPE_LABEL = "Tape_T"
# Local wall-clock instants, as the IBM CSV writes them (naive local time).
MOUNT_STARTED = datetime(2026, 8, 1, 10, 0, 0)
BEFORE_MOUNT_CSV = "2026/07/31 09:00:00.000"
AFTER_MOUNT_CSV = "2026/08/01 11:00:00.000"

REBOOT_REQUIRED_KEY = (
    r"SOFTWARE\Microsoft\Windows\CurrentVersion\WindowsUpdate\Auto Update"
    r"\RebootRequired")
SCCM_SERVICE_KEY = r"SYSTEM\CurrentControlSet\Services\CcmExec"

_REAL_MAKEDIRS = os.makedirs


def _iso_local(naive):
    """The ISO-8601 offset form PowerShell's ``ToString('o')`` produces."""
    return naive.astimezone().isoformat()


class FakeRegistry:
    """Stands in for the ``winreg`` module: a dict of keys -> values."""

    HKEY_LOCAL_MACHINE = object()
    KEY_READ = 0

    def __init__(self, log):
        self.log = log
        #: key path -> {value name: value}. A missing key raises like winreg.
        self.keys = {SCCM_SERVICE_KEY: {}}
        #: Set to an exception to make every registry access fail unexpectedly.
        self.broken = None

    @contextlib.contextmanager
    def OpenKey(self, _root, path, _reserved=0, _access=0):
        self.log.append("registry")
        if self.broken is not None:
            raise self.broken
        if path not in self.keys:
            raise FileNotFoundError(path)
        yield path

    def QueryValueEx(self, key, name):
        values = self.keys.get(key, {})
        if name not in values:
            raise FileNotFoundError(name)
        return values[name], 1


class FakeCatalog(MinimalBackupDB):
    """In-memory catalog. Enforces the product's own chunk-transition table."""

    def __init__(self, log):
        super().__init__()
        self.log = log
        self.chunk_status = {}
        self.status_writes = []
        self.tapes = {TAPE_LABEL: {
            "tape_label": TAPE_LABEL, "total_capacity": 12000,
            "status": "active", "used": 0}}
        self.lock_error = None
        self.status_read_error = None
        self.tape_read_error = None
        self.catalog_commit_error = None

    # -- single-writer lock ----------------------------------------------
    def verify_archiver_lock(self):
        self.log.append("archiver_lock")
        if self.lock_error is not None:
            raise self.lock_error

    # -- chunks -----------------------------------------------------------
    def seed_chunk(self, chunk_index, status="packing", session_id=SESSION_ID):
        self.chunk_status[(session_id, chunk_index)] = status

    def status_of(self, chunk_index, session_id=SESSION_ID):
        return self.chunk_status[(session_id, chunk_index)]

    def get_chunk_packaging_format(self, session_id, chunk_index):
        return ContainerFormat.ZIP

    def get_chunks_with_status(self, session_id, status):
        if self.status_read_error is not None:
            raise self.status_read_error
        return sorted(c for (sid, c), st in self.chunk_status.items()
                      if sid == session_id and st == status)

    def update_chunk_status(self, session_id, chunk_index, status):
        old = ChunkStatus(self.chunk_status[(session_id, chunk_index)])
        new = ChunkStatus(status)
        if new not in CHUNK_TRANSITIONS[old]:
            raise ForbiddenTransition(f"{old.value} -> {new.value}")
        self.chunk_status[(session_id, chunk_index)] = new.value
        self.status_writes.append((chunk_index, new.value))
        self.log.append(f"status:{chunk_index}:{new.value}")

    # -- catalog ----------------------------------------------------------
    def bulk_upsert_files(self, records, update_existing=True):
        self.log.append("catalog_commit")
        if self.catalog_commit_error is not None:
            raise self.catalog_commit_error
        return super().bulk_upsert_files(records, update_existing)

    # -- tapes ------------------------------------------------------------
    def tape_exists(self, tape_label):
        return tape_label in self.tapes

    def get_tape(self, tape_label):
        if self.tape_read_error is not None:
            raise self.tape_read_error
        return self.tapes.get(tape_label)

    def recalculate_tape_used_space(self, tape_label):
        super().recalculate_tape_used_space(tape_label)
        return self.tapes[tape_label]["used"]


class GateWorld:
    """Every outermost boundary of a tape write, healthy by default.

    A test flips ONE attribute to simulate one bad condition and then drives
    the real writer.
    """

    def __init__(self, testcase):
        self.log = []
        self.tmp = tempfile.mkdtemp(prefix="gateworld_")
        testcase.addCleanup(shutil.rmtree, self.tmp, True)
        self.tape = os.path.join(self.tmp, "fake_ltfs_mount")
        self.staging = os.path.join(self.tmp, "staging")
        self.logs = os.path.join(self.tmp, "logs")
        for path in (self.tape, self.staging, self.logs):
            _REAL_MAKEDIRS(path)
        self.ltfs_csv = os.path.join(self.tmp, "LogFile.csv")
        with open(self.ltfs_csv, "w", encoding="utf-8"):
            pass

        self.db = FakeCatalog(self.log)
        self.registry = FakeRegistry(self.log)

        # -- knobs (healthy defaults) ---------------------------------------
        #: The mount's own declaration (LTFS event 61259) and process start.
        self.mount = {
            "MountProcStart": _iso_local(MOUNT_STARTED),
            "ProcStart": _iso_local(MOUNT_STARTED),
            "EventTime": _iso_local(MOUNT_STARTED.replace(minute=1)),
            "EventMsg": 'LTFS61259I Sync type is "time", Sync time is 300 sec.',
        }
        self.mount_query_fails = False
        self.sccm = {"ReturnValue": 0, "RebootPending": False,
                     "IsHardRebootPending": False, "InGracePeriod": False,
                     "RebootDeadline": ""}
        self.sccm_query_fails = False
        self.volume_label = TAPE_LABEL
        self.label_error = None
        self.ownership_error = None
        self.drive_ready = True
        #: One entry consumed per copy call; "ok" once the list is exhausted.
        self.copy_plan = []
        #: Called with (source, dest) at the start of every copy.
        self.on_copy = None
        #: Set to an OSError to make the first write-touch (mkdir) fail.
        self.tape_mkdir_error = None

        self._ownership_depth = 0
        self._inside_copy = False
        #: Boundary calls that happened while LTFS ownership was NOT held.
        self.unowned = []

        stack = contextlib.ExitStack()
        testcase.addCleanup(stack.close)
        patches = [
            mock.patch("src.windows_update_guard.subprocess.run",
                       self._powershell),
            mock.patch("src.windows_update_guard.winreg", self.registry),
            mock.patch("src.windows_update_guard._LTFS_LOG_CSV_DEFAULT",
                       self.ltfs_csv),
            mock.patch("src.ltfs._read_volume_label_unlocked",
                       self._read_label),
            mock.patch("src.backup._ensure_lto_drive_ready", self._readiness),
            mock.patch("src.local_orchestrator._ensure_lto_drive_ready",
                       self._readiness),
            mock.patch("src.backup.os.makedirs", self._makedirs),
            mock.patch("src.backup._run_robocopy_tuned", self._copy),
            mock.patch("src.backup.eject_tape_drive", self._eject),
            mock.patch("src.local_orchestrator.eject_tape_drive", self._eject),
            mock.patch("src.ltfs.eject_tape_drive", self._eject),
            mock.patch("src.ltfs._eject_tape_unlocked", self._eject),
        ]
        for site in TAPE_LOCK_USE_SITES:
            patches.append(mock.patch(f"{site}._acquire_tape_io_lock",
                                      self._acquire))
            patches.append(mock.patch(f"{site}._release_tape_io_lock",
                                      self._release))
        for patch in patches:
            stack.enter_context(patch)

        CANCEL.clear()
        testcase.addCleanup(CANCEL.clear)
        ltfs.reset_readiness("gate world setup")
        testcase.addCleanup(ltfs.reset_readiness, "gate world teardown")

    # ------------------------------------------------------------------
    # Boundary stand-ins
    # ------------------------------------------------------------------
    def _note(self, event, needs_ownership=False):
        self.log.append(event)
        if needs_ownership and self._ownership_depth == 0:
            self.unowned.append(event)

    def _powershell(self, argv, *args, **kwargs):
        script = argv[-1]
        if "61259" in script:
            self._note("ps:mount")
            if self.mount_query_fails:
                return SimpleNamespace(returncode=1, stdout="",
                                       stderr="Get-WinEvent : access denied")
            return SimpleNamespace(returncode=0, stdout=json.dumps(self.mount),
                                   stderr="")
        if "DetermineIfRebootPending" in script:
            self._note("ps:sccm")
            if self.sccm_query_fails:
                return SimpleNamespace(returncode=1, stdout="",
                                       stderr="Invoke-CimMethod : RPC failure")
            return SimpleNamespace(returncode=0, stdout=json.dumps(self.sccm),
                                   stderr="")
        raise AssertionError(f"unexpected command: {argv!r}"[:200])

    def _read_label(self, drive_path):
        self._note("label", needs_ownership=True)
        if self.label_error is not None:
            raise self.label_error
        return self.volume_label

    def _acquire(self, reason=None, timeout=None):
        if self._ownership_depth == 0 and self.ownership_error is not None:
            self.log.append("ownership:refused")
            raise self.ownership_error
        self._ownership_depth += 1
        if self._ownership_depth == 1:
            self.log.append("ownership:acquire")

    def _release(self, *args, **kwargs):
        if self._ownership_depth == 0:
            self.log.append("ownership:release_without_acquire")
            return
        self._ownership_depth -= 1
        if self._ownership_depth == 0:
            self.log.append("ownership:release")

    def _readiness(self, drive_path, *args, **kwargs):
        self._note("readiness", needs_ownership=True)
        return self.drive_ready

    def _makedirs(self, path, *args, **kwargs):
        tape = os.path.normcase(os.path.abspath(self.tape))
        target = os.path.normcase(os.path.abspath(path))
        if ((target + os.sep).startswith(tape + os.sep)
                and not self._inside_copy):
            self._note("tape_mkdir", needs_ownership=True)
            if self.tape_mkdir_error is not None:
                raise self.tape_mkdir_error
        return _REAL_MAKEDIRS(path, *args, **kwargs)

    def _eject(self, *args, **kwargs):
        self._note("eject")

    def _copy(self, cmd, priority=None, affinity=None, on_start=None,
              raw_sink=None):
        """The fake tape copy. ``cmd`` is the real robocopy argv."""
        self._note("copy", needs_ownership=True)
        source, dest = cmd[1], cmd[2]
        if self.on_copy is not None:
            self.on_copy(source, dest)
        behaviour = self.copy_plan.pop(0) if self.copy_plan else "ok"
        files = total = 0
        for root, _dirs, names in os.walk(source):
            for name in names:
                files += 1
                total += os.path.getsize(os.path.join(root, name))
        full_summary = (
            "   Dirs :         1         1         0         0         0         0\n"
            f"  Files : {files} {files} 0 0 0 0\n"
            f"  Bytes : {total} {total} 0 0 0 0\n"
            "  Times :   0:00:01   0:00:01\n")
        if behaviour == "ok":
            self._inside_copy = True
            try:
                shutil.copytree(source, dest, dirs_exist_ok=True)
            finally:
                self._inside_copy = False
            return SimpleNamespace(stdout=full_summary, stderr="",
                                   returncode=1)
        if behaviour == "rc8":
            return SimpleNamespace(
                stdout=("   Dirs : 1 0 0 0 1 0\n"
                        f"  Files : {files} 0 0 0 {files} 0\n"
                        f"  Bytes : {total} 0 0 0 {total} 0\n"),
                stderr="", returncode=8)
        if behaviour == "retry_limit_rc0":
            # Incident 009: robocopy exits 0 and prints a clean-looking
            # summary although it gave up. Only the banner says so.
            return SimpleNamespace(
                stdout="ERROR: RETRY LIMIT EXCEEDED.\n" + full_summary,
                stderr="", returncode=0)
        if behaviour == "no_summary":
            return SimpleNamespace(stdout="", stderr="", returncode=0)
        if behaviour == "raise":
            raise OSError(1117, "The request could not be performed because "
                                "of an I/O device error")
        raise AssertionError(f"unknown copy behaviour {behaviour!r}")

    # ------------------------------------------------------------------
    # Scenario helpers
    # ------------------------------------------------------------------
    def log_ltfs_event(self, event_id, when=AFTER_MOUNT_CSV,
                       message="drive fault"):
        """Append one row to the fake IBM LogFile.csv."""
        with open(self.ltfs_csv, "a", encoding="utf-8", newline="") as fh:
            fh.write(f'{event_id},E,{when},1234,5678,tape0,"{message}"\n')

    def declare_sync_mode(self, sync_type, seconds):
        self.mount["EventMsg"] = (
            f'LTFS61259I Sync type is "{sync_type}", '
            f'Sync time is {seconds} sec.')

    def stage_chunk(self, chunk_index, status="packing", size=2048):
        """Create a real pack directory and its descriptor."""
        pack = os.path.join(self.staging,
                            f"_pack_s{SESSION_ID:04d}_{chunk_index:03d}")
        _REAL_MAKEDIRS(pack)
        with open(os.path.join(pack, "Bundle_0001.zip"), "wb") as fh:
            fh.write(b"x" * size)
        self.db.seed_chunk(chunk_index, status)
        return StagedChunk(
            chunk_index=chunk_index, fetch_dir=pack + "_fetch", pack_dir=pack,
            metadata=[{"is_packed": True, "container_name": "Bundle_0001.zip",
                       "file_name": "a.txt", "original_path": "/data/a.txt",
                       "file_size_bytes": 10, "stored_path": "a.txt"}],
            staged_bytes=size, session_id=SESSION_ID,
            packaging_format=ContainerFormat.ZIP)

    def cfg(self, **extra):
        values = dict(
            lto_drive=self.tape, backup_log_dir=self.logs, ibm_eject_cmd="",
            index_min_file_mb=10, index_packed_small_files=False,
            windows_update_block_on_pending_reboot=True,
            eject_after_session=False, staging_dir=self.staging)
        values.update(extra)
        return SimpleNamespace(**values)

    def remote_orchestrator(self):
        """A RemoteOrchestrator with EVERY gate method left real."""
        from src import remote_orchestrator as ro
        orch = ro.RemoteOrchestrator.__new__(ro.RemoteOrchestrator)
        orch.cfg = self.cfg()
        orch.db = self.db
        orch.notifier = None
        orch.governor = None
        orch.tape_priority = None
        orch.tape_cores = None
        orch.remote_host = "src-host.example"
        orch.remote_session_path = "/data"
        orch.skipped_tracker = SkippedFileTracker()
        orch._stop_lock = threading.Lock()
        orch._stop_result = None
        orch._staged_lock = threading.Lock()
        orch._staged_bytes = 0
        orch._consumer_chunk = None
        orch._ownership_acquisitions = 0
        orch._readiness_checks = 0
        orch._cartridge_verifications = 0
        return orch

    # ------------------------------------------------------------------
    # Observations
    # ------------------------------------------------------------------
    def count(self, event):
        return self.log.count(event)

    def index(self, event):
        return self.log.index(event)

    @property
    def copies(self):
        return self.count("copy")

    def files_on_tape(self):
        found = []
        for root, _dirs, names in os.walk(self.tape):
            for name in names:
                found.append(os.path.relpath(os.path.join(root, name),
                                             self.tape))
        return sorted(found)

    @property
    def ownership_held(self):
        return self._ownership_depth > 0


def ownership_unavailable():
    return LtfsOwnershipError("another process owns the LTFS volume",
                              operation="test", kind="timeout")
