"""Reusable test doubles for LTFS interaction.

Phase 0 infrastructure. These exist so later phases can assert on *which* tape
operations happen and in what order, rather than only on their side effects —
the audit showed the per-chunk readiness probe was the operation nobody could
see. Nothing here touches a real drive; every fake is backed by a temporary
directory or an in-memory record.
"""
import os
import subprocess
from types import SimpleNamespace
from unittest import mock


# Modules that do `from .runtime import _acquire_tape_io_lock`, so the name must
# be patched at each use site rather than on src.runtime alone.
# Keep this in step with `grep -l _acquire_tape_io_lock src/*.py`. A module
# missing from this list is NOT observed, and a test that relies on the
# observer then silently proves nothing. Plan 1's Task 1.2 moved the remote
# write group into src.remote_writer, which is where the group's single
# ownership period is now taken — omitting it made TapeLockObserver blind to
# the one acquisition that matters most.
TAPE_LOCK_USE_SITES = (
    "src.ltfs", "src.backup", "src.local_orchestrator",
    "src.remote_orchestrator", "src.remote_writer", "src.retriever",
    "src.tape_reset",
)


class TapeOperationLog:
    """Ordered record of every attempted tape operation."""

    def __init__(self):
        self.events = []

    def record(self, kind, **detail):
        self.events.append(SimpleNamespace(kind=kind, **detail))

    def kinds(self):
        return [e.kind for e in self.events]

    def of_kind(self, kind):
        return [e for e in self.events if e.kind == kind]

    def count(self, kind):
        return len(self.of_kind(kind))

    def __repr__(self):
        return f"<TapeOperationLog {self.kinds()}>"


class FakeLtfsAdapter:
    """Stands in for every IBM-LTFS-facing operation.

    Backed by ``root`` (a temp directory) so path-shaped code keeps working
    while every call is recorded. ``status`` / ``volume_label`` are settable so
    a test can simulate an unmounted drive or a swapped cartridge, and
    ``fail_filesystem`` makes root access raise the way a read-only or
    disconnected mount does.
    """

    def __init__(self, root, status="LTFS_MOUNTED", volume_label="Tape_TEST",
                 log=None):
        self.root = root
        self.status = status
        self.volume_label = volume_label
        self.log = log or TapeOperationLog()
        self.fail_filesystem = None      # set to an OSError to simulate failure

    # -- non-filesystem checks ------------------------------------------------
    def drive_status(self, drive_path):
        self.log.record("drive_status", drive=drive_path)
        return self.status, f"{drive_path} 1 2 {self.status}", None

    def read_volume_label(self, drive_path):
        self.log.record("volume_label", drive=drive_path)
        return self.volume_label

    # -- filesystem-touching operations --------------------------------------
    def listdir(self, drive_path):
        self.log.record("listdir", drive=drive_path)
        if self.fail_filesystem:
            raise self.fail_filesystem
        return os.listdir(self.root)

    def isdir(self, drive_path):
        self.log.record("isdir", drive=drive_path)
        if self.fail_filesystem:
            raise self.fail_filesystem
        return True

    # -- write path -----------------------------------------------------------
    def robocopy(self, cmd):
        self.log.record("robocopy", cmd=list(cmd))
        return SimpleNamespace(stdout="", stderr="", returncode=0)

    def eject(self, drive_path):
        self.log.record("eject", drive=drive_path)

    # -- assertions helpers ---------------------------------------------------
    @property
    def filesystem_touches(self):
        """Operations that reach the LTFS filesystem (and can dirty the index)."""
        return [e for e in self.log.events if e.kind in ("listdir", "isdir")]


class RecordingSubprocess:
    """Injectable ``subprocess.run`` double that records every command."""

    def __init__(self, result=None):
        self.calls = []
        self.result = result or SimpleNamespace(
            stdout="", stderr="", returncode=0)

    def __call__(self, cmd, *args, **kwargs):
        self.calls.append(list(cmd) if isinstance(cmd, (list, tuple)) else cmd)
        return self.result

    def commands(self):
        return [" ".join(map(str, c)) if isinstance(c, list) else str(c)
                for c in self.calls]

    def ran(self, needle):
        return any(needle.lower() in c.lower() for c in self.commands())


class TapeLockObserver:
    """Makes tape-lock ownership observable.

    Records acquire/release with the reason string and tracks nesting depth, so
    a test can assert that an operation ran *inside* the lock and that recursive
    acquisition does not deadlock.
    """

    def __init__(self, use_sites=TAPE_LOCK_USE_SITES):
        self.use_sites = use_sites
        self.events = []
        self.depth = 0
        self.max_depth = 0
        self.timeouts = []
        self._stack = []

    def _acquire(self, reason=None, *args, **kwargs):
        # Mirrors runtime._acquire_tape_io_lock(reason, timeout=...). Accepting
        # the full signature matters: a TypeError here would surface as the
        # observed code failing, not as the observer being out of date.
        self.timeouts.append(kwargs.get("timeout"))
        self.depth += 1
        self.max_depth = max(self.max_depth, self.depth)
        self._stack.append(reason)
        self.events.append(("acquire", reason, self.depth))

    def _release(self, *args, **kwargs):
        reason = self._stack.pop() if self._stack else None
        self.events.append(("release", reason, self.depth))
        self.depth -= 1

    @property
    def held(self):
        return self.depth > 0

    def reasons(self):
        return [r for kind, r, _ in self.events if kind == "acquire"]

    def patches(self):
        """Context managers patching the lock helpers at every use site."""
        out = []
        for site in self.use_sites:
            out.append(mock.patch(f"{site}._acquire_tape_io_lock",
                                  side_effect=self._acquire))
            out.append(mock.patch(f"{site}._release_tape_io_lock",
                                  side_effect=self._release))
        return out


class MinimalBackupDB:
    """Smallest DB double that lets ``LTOBackup.run`` complete."""

    def __init__(self):
        self.file_commits = 0
        self.recalc_calls = 0

    def tape_exists(self, tape_label):
        return True

    def bulk_upsert_files(self, records, update_existing=True):
        list(records)
        self.file_commits += 1
        return {"inserted": 1, "updated": 0, "skipped": 0}

    def recalculate_tape_used_space(self, tape_label):
        self.recalc_calls += 1
        return 0
