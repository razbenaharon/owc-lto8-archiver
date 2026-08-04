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

    @property
    def acquisition_count(self):
        """Number of ownership periods, including recursive acquisitions."""
        return sum(kind == "acquire" for kind, _, _ in self.events)

    @property
    def release_count(self):
        """Number of completed ownership releases observed so far."""
        return sum(kind == "release" for kind, _, _ in self.events)

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
        self.directory_commits = 0
        self.recalc_calls = 0
        self.container_events = []

    def tape_exists(self, tape_label):
        return True

    def bulk_upsert_files(self, records, update_existing=True):
        list(records)
        self.file_commits += 1
        return {"inserted": 1, "updated": 0, "skipped": 0}

    def directory_catalog_schema_installed(self):
        return True

    def bulk_upsert_directory_catalog(self, records, *args, **kwargs):
        list(records)
        self.directory_commits += 1
        return {"bundles": 1, "stats": 1, "tree_rows": 1}

    def mark_remote_chunk_writer_started(self, session_id, chunk_index,
                                         tape_label, tape_root, staged_chunk):
        self.container_events.append(("writer_started", chunk_index))
        return {"tape_generation_id": 1, "tape_generation": 1}

    def mark_remote_chunk_copy_succeeded(self, session_id, chunk_index,
                                         tape_label, tape_root, staged_chunk,
                                         **kwargs):
        self.container_events.append(("copy_succeeded", chunk_index))
        return {"tape_generation_id": 1, "tape_generation": 1,
                "archive_run_id": 1}

    def mark_remote_chunk_catalog_committing(self, session_id, chunk_index):
        self.container_events.append(("catalog_committing", chunk_index))

    def mark_remote_chunk_catalog_committed(self, session_id, chunk_index):
        self.container_events.append(("catalog_committed", chunk_index))

    def mark_remote_chunk_catalog_failed(self, session_id, chunk_index):
        self.container_events.append(("catalog_failed", chunk_index))

    def mark_remote_chunk_write_ambiguous(self, session_id, chunk_index):
        self.container_events.append(("writer_ambiguous", chunk_index))

    def recalculate_tape_used_space(self, tape_label):
        self.recalc_calls += 1
        return 0


# ===========================================================================
# Frontier catalog fake (Plan 1 completion)
# ===========================================================================
class FakeFrontierCatalog:
    """In-memory stand-in for the migration-014 half of the catalog.

    The frontier is now the production scanner, so any fake that stands in for
    the database during a streaming run has to answer the frontier's questions
    too — schema readiness, scopes, the directory queue, ready segments and
    segment-range consumption. Before Plan 1 completion a streaming fake only
    needed the whole-root scanner's handful of calls.

    Deliberately reports the schema as INSTALLED and FINALIZED, because that is
    production's state; a test that wants the not-ready path sets the flags to
    False and asserts the run stops.
    """

    def __init__(self):
        self.schema_installed = True
        self.schema_finalized = True
        self.scopes = []
        self.directories = []
        self.segments = []
        self.scan_errors = []
        self.attempts = {}
        self.consumed = []
        self.frontier_state = False
        self._next_frontier_id = 1

    # -- schema ----------------------------------------------------------
    def incremental_scan_schema_installed(self):
        return self.schema_installed

    def incremental_scan_schema_finalized(self):
        return self.schema_finalized

    def session_has_frontier_state(self, session_id):
        return self.frontier_state

    def _fid(self):
        value = self._next_frontier_id
        self._next_frontier_id += 1
        return value

    # -- scopes ----------------------------------------------------------
    def create_scan_scopes(self, session_id, roots):
        for ordinal, (root, kind) in enumerate(roots):
            self.scopes.append({
                "scan_scope_id": self._fid(), "session_id": session_id,
                "scope_ordinal": ordinal, "source_root": root,
                "scope_kind": kind, "coverage_state": "provisional"})
        return list(self.scopes)

    def get_scan_scopes(self, session_id):
        return list(self.scopes)

    def finalize_scan_scope(self, scan_scope_id):
        for scope in self.scopes:
            if scope["scan_scope_id"] == scan_scope_id:
                unfinished = [d for d in self.directories
                              if d["scan_scope_id"] == scan_scope_id
                              and d["listing_state"] != "complete"]
                if not unfinished:
                    scope["coverage_state"] = "final"
                return scope["coverage_state"]
        return None

    # -- directories -----------------------------------------------------
    def enqueue_scan_directories(self, scan_scope_id, entries,
                                 parent_directory_id=None):
        added = []
        for path, ordinal in entries:
            if any(d["scan_scope_id"] == scan_scope_id
                   and d["canonical_path"] == path for d in self.directories):
                continue                       # unique (scope, path)
            row = {"scan_directory_id": self._fid(),
                   "scan_scope_id": scan_scope_id, "canonical_path": path,
                   "traversal_ordinal": ordinal,
                   "parent_directory_id": parent_directory_id,
                   "listing_state": "pending",
                   "subtree_coverage_state": "provisional",
                   "owner_token": None}
            self.directories.append(row)
            added.append(row)
        return added

    def claim_next_directory(self, session_id, owner_token, attempt_id,
                             **kwargs):
        for row in sorted(self.directories,
                          key=lambda d: (d["scan_scope_id"],
                                         d["traversal_ordinal"])):
            if row["listing_state"] in ("pending", "partial"):
                row["listing_state"] = "scanning"
                row["owner_token"] = owner_token
                return row
        return None

    def complete_directory_listing(self, directory_id, owner_token, **kwargs):
        for row in self.directories:
            if row["scan_directory_id"] == directory_id:
                row["listing_state"] = ("error" if kwargs.get("error_count")
                                        else "complete")
                row["owner_token"] = None
                return row["listing_state"]
        return None

    def mark_directory_partial(self, directory_id, owner_token, **kwargs):
        for row in self.directories:
            if row["scan_directory_id"] == directory_id:
                row["listing_state"] = "partial"
                row["owner_token"] = None
        return None

    def finalize_directory_subtree(self, directory_id):
        for row in self.directories:
            if row["scan_directory_id"] == directory_id:
                if row["listing_state"] == "complete":
                    row["subtree_coverage_state"] = "final"
                    return True, "final"
                return False, row["listing_state"]
        return False, "missing"

    def get_covered_directories(self, scan_scope_id):
        return [d for d in self.directories
                if d["scan_scope_id"] == scan_scope_id
                and d["listing_state"] == "complete"]

    def record_scan_error(self, **kwargs):
        self.scan_errors.append(kwargs)
        return kwargs

    # -- segments --------------------------------------------------------
    def publish_scan_segment(self, directory_id, *, first_scan_ordinal,
                             last_scan_ordinal, locator, **kwargs):
        row = {"scan_segment_id": self._fid(),
               "scan_directory_id": directory_id, "locator": locator,
               "first_scan_ordinal": first_scan_ordinal,
               "last_scan_ordinal": last_scan_ordinal,
               "next_unconsumed_ordinal": first_scan_ordinal,
               "state": "ready",
               "legacy_import_state": "not_imported"}
        row.update({k: v for k, v in kwargs.items() if k not in row})
        self.segments.append(row)
        return row

    def get_ready_segments(self, session_id, limit=50):
        return [s for s in self.segments
                if s["state"] == "ready"][:limit]

    def consume_segment_range(self, segment_id, session_id, chunk_index,
                              count):
        segment = next(s for s in self.segments
                       if s["scan_segment_id"] == segment_id)
        first = segment["next_unconsumed_ordinal"]
        last = min(segment["last_scan_ordinal"], first + count - 1)
        segment["next_unconsumed_ordinal"] = last + 1
        if segment["next_unconsumed_ordinal"] > segment["last_scan_ordinal"]:
            segment["state"] = "consumed"
        self.consumed.append((segment_id, chunk_index, first, last))
        return first, last

    # -- worker attempts -------------------------------------------------
    def start_worker_attempt(self, **kwargs):
        attempt_id = self._fid()
        self.attempts[attempt_id] = dict(kwargs, state="running")
        return attempt_id

    def finish_worker_attempt(self, attempt_id, terminal_state):
        if attempt_id in self.attempts:
            self.attempts[attempt_id]["state"] = terminal_state
        return terminal_state

    # -- legacy reconciliation -------------------------------------------
    #: A legacy session's existing membership, ``{path: size}``. Empty means
    #: every rediscovered entry is genuinely new.
    legacy_snapshot = None

    def _snapshot(self):
        if self.legacy_snapshot is None:
            self.legacy_snapshot = {}
        return self.legacy_snapshot

    def _classify(self, entries):
        snapshot = self._snapshot()
        covered, fresh, changed = [], [], []
        for path, size in entries:
            known = snapshot.get(path.replace("\\", "/"))
            if known is None:
                fresh.append((path, size))
            elif known == size:
                covered.append((path, size))
            else:
                changed.append((path, known, size))
        return covered, fresh, changed

    def import_legacy_scan_segment(self, session_id, segment_id, entries):
        segment = next((s for s in self.segments
                        if s["scan_segment_id"] == segment_id), None)
        if segment is None:
            raise RuntimeError(f"segment {segment_id} does not exist")
        if segment["legacy_import_state"] != "not_imported":
            # Once-only, exactly like the repository: EMPTY lists, which is why
            # the caller must reclassify rather than replay the raw entries.
            return {"covered": [], "new": [], "source_changed": [],
                    "already_imported": True}
        covered, fresh, changed = self._classify(entries)
        segment["legacy_import_state"] = "blocked" if changed else "imported"
        for path, planned, observed in changed:
            self.scan_errors.append({"category": "source_changed", "path": path,
                                     "disposition": "unresolved"})
        return {"covered": covered, "new": fresh, "source_changed": changed,
                "already_imported": False}

    def classify_segment_entries(self, session_id, entries):
        """Read-only reclassification. Must NOT change import state."""
        covered, fresh, changed = self._classify(entries)
        return {"covered": covered, "new": fresh, "source_changed": changed}

    # -- chunk sealing ---------------------------------------------------
    #: ``[{chunk_index, expected_file_count, expected_bytes, ...}, ...]``
    sealed_chunks = None

    def seal_remote_chunk(self, session_id, chunk_index, *,
                          expected_file_count, expected_bytes,
                          scan_segment_id=None, first_scan_ordinal=None,
                          last_scan_ordinal=None):
        """Seal once. A second identical seal is a no-op; a differing one is a
        contradiction, exactly as the repository treats it."""
        if self.sealed_chunks is None:
            self.sealed_chunks = []
        for existing in self.sealed_chunks:
            if existing["chunk_index"] == chunk_index:
                if (existing["expected_file_count"] == expected_file_count
                        and existing["expected_bytes"] == expected_bytes):
                    return False
                raise RuntimeError(
                    f"chunk {chunk_index} already sealed with a different "
                    "expectation")
        self.sealed_chunks.append({
            "chunk_index": chunk_index,
            "expected_file_count": expected_file_count,
            "expected_bytes": expected_bytes,
            "scan_segment_id": scan_segment_id,
            "first_scan_ordinal": first_scan_ordinal,
            "last_scan_ordinal": last_scan_ordinal})
        return True

    def mark_segment_fully_allocated(self, scan_segment_id):
        for segment in self.segments:
            if segment["scan_segment_id"] == scan_segment_id:
                if segment["state"] in ("ready", "partially_consumed"):
                    segment["state"] = "consumed"
                    return True
        return False
