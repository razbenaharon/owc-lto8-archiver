"""RemoteOrchestrator: streaming remote-host -> staging -> tape pipeline.

Flow map (Plan 1 Task 0.1, updated at Plan 1 completion)
=======================================================

This map records what the code does **today**. It began life as a
characterization of the pre-Plan-1 flow so the refactor could be proved
behaviour-preserving; Plan 1 then deliberately changed that flow, and the map
was updated with it. Every statement below is pinned by a test in
``tests/test_pipeline_characterization.py``; if you change the behaviour,
change that test in the same commit.

Where the old behaviour is worth remembering — because the new behaviour exists
to fix it — it is marked **WAS**, not left standing as if it were current.

Entry flows
-----------
Both public entries converge on the same two session loops::

    run.py -> src.cli.main() -> run_remote_archiver()          (interactive)
    src.cli.run_remote_archiver_headless()                     (headless)
        -> RemoteOrchestrator.run(non_interactive=..., resume=...)
            -> _run_non_interactive()   # --resume only; never prompts
            -> _start_new_session()     # interactive fresh start
            -> _run_session(session_id) # resume

``_run_session()`` immediately delegates *every* session whose
``scan_complete`` is false back to ``_run_streaming_session()``; only a
scan-complete session runs the resume loop in ``_run_session()`` itself.
So there are two scheduling loops, not two pipelines.

New session
-----------
``_start_new_session()`` calls ``PgSessionMixin.create_remote_streaming_session()``,
which creates a *growable* ``remote_snapshots`` row and a ``remote_plans`` row
with ``remote_sessions.scan_complete = FALSE``. Chunks are appended to that plan
as the scan discovers them.

Scanning and planning (``_run_streaming_session`` -> ``FrontierScanCoordinator``)
-------------------------------------------------------------------
1. Work selection is **authoritative**: the stager reads pending chunks from
   their persisted status, so a resumed backlog never queues in front of renewed
   exploration. (WAS: every non-``done`` chunk was pushed onto a bounded
   ``chunk_q`` before exploration started, so a large backlog plus a slow stager
   could postpone scanning indefinitely.)
2. :class:`~src.scan_frontier.FrontierScanCoordinator` explores the source **one
   directory at a time**, publishing each listing as a ready segment artifact and
   committing per directory, so a crash replays at most that directory.
   (WAS: ``StreamingRemoteScanner.iter_scan()`` ran ``find`` from every
   configured root on every incomplete-session run, recovering by replaying
   visited files rather than resuming a persisted position.)
3. ``SegmentChunkPublisher`` reads a segment's entries from its local artifact
   and, for a session that pre-dates the frontier, reconciles it **once** against
   the legacy snapshot — one set-based query per segment, on canonical path AND
   size — passing only genuinely new entries onward.
4. ``StreamingChunkBuilder`` then chooses chunk boundaries in discovery order
   using ``ChunkPlanner.footprint()``, the byte budget from ``_chunk_budget()``
   and ``chunk_max_files``. Because step 3 already removed known paths, a
   rediscovered file **cannot** influence a boundary. (WAS: the membership filter
   ran *after* the builder had seen the paths, so a resumed scan produced
   different boundaries from the original run for the same source.)
5. Scan finality comes from traversal evidence only: ``scan_complete`` is written
   when every scope reports final coverage, never inferred from catalog rows.
5. ``_append_chunk()`` and ``PgSessionMixin.append_remote_streaming_chunk()``
   each issue **one chunk-bulk** membership query (``remote_path = ANY(...)``).
   This is *not* one SQL round trip per file; the per-file cost is in the
   ``executemany`` inserts.

Surviving state
---------------
Across a crash only these survive: session-wide ``scan_complete`` /
``scan_error``, the growing ``remote_snapshot_files`` / ``remote_plan_files``
rows, ``remote_chunks`` rows, and the session totals. The partial in-memory
``StreamingChunkBuilder`` buffer, the current root/directory, the traversal
stack, any continuation cursor, empty directories, and per-directory finality
do **not** survive: nothing in the schema proves a directory was fully scanned.

Duplicate protection
--------------------
``UNIQUE(snapshot_id, remote_path)``, ``UNIQUE(plan_id, snapshot_file_id)``,
``PRIMARY KEY(session_id, chunk_index)`` plus the application-level filters in
``_append_chunk``. There is no membership *seal* and no unique
``(plan_id, chunk_index, ordinal)`` constraint, so a sealed chunk can still gain
members and two ordinals can collide.

Coverage honesty
----------------
Recoverable ``find`` warnings (permission denied on a subtree) are recorded as
skipped rows and still allow the run to reach ``mark_remote_scan_complete()``.
Global ``scan_complete`` therefore does not mean "every directory was read".

Overlap
-------
Scanner, stager and finite-group writer do overlap, through ``chunk_q`` and
``ReadyQueue``; chunks reach the writer long before ``scan_complete`` is set.

Data boundaries (unchanged by Plan 1)
-------------------------------------
``src.packer.LTOPacker`` (ZIP/loose metadata), ``src.backup.LTOBackup``
(Robocopy + catalog sync), ``src.pg_catalog.PgCatalogMixin`` (file/directory
rows), ``src.retriever.LTORetriever`` (ZIP/loose restore).
"""
import gc
import json
import os
import random
import time
import queue
import shutil
import threading
import posixpath
import uuid
from datetime import datetime, timezone
from collections import defaultdict
from typing import Optional

from .backup import LTOBackup, _NoEjectBackup
from .constants import (DEFAULT_TAPE_CAPACITY_GB, LOCAL_STAGING_RESERVE_BYTES,
                        LTFS_WRITE_WARNING, tape_budget_bytes, tape_is_full,
                        tape_status_reason_suffix)
from .db import _apply_canonical_remote_paths
from .exit_codes import (
    ExitCode, StopResult,
    REASON_NETWORK_RETRY_EXHAUSTED, REASON_SCCM_REBOOT_PENDING,
    REASON_WINDOWS_REBOOT_PENDING, REASON_STOPPED_AT_CHUNK_BOUNDARY,
    REASON_TAPE_WRITE_FAILED,
    REASON_AMBIGUOUS_BACKING_CHUNK, REASON_LTFS_SYNC_MODE_NOT_TIME5,
    REASON_LTFS_MEDIA_DEGRADED,
    REASON_LTFS_MOUNT_UNVERIFIABLE, REASON_UNEXPECTED_TAPE_OR_DB_STATE,
    REASON_LTFS_OWNERSHIP_UNAVAILABLE,
    REASON_SEALED_BATCH_FEATURE_UNAVAILABLE, REASON_SCAN_FRONTIER_UNAVAILABLE,
    REASON_AMBIGUOUS_ACTIVE_SESSIONS, REASON_SSH_AUTHENTICATION_FAILED,
    REASON_SSH_PERMISSION_DENIED, REASON_SSH_HOST_KEY_MISMATCH,
    REASON_MISSING_NONINTERACTIVE_CREDENTIAL, REASON_BAD_CONFIG,
    REASON_NO_ACTIVE_SESSION, REASON_NONINTERACTIVE_REQUIRES_RESUME,
    REASON_USER_REQUESTED_STOP, REASON_COMPLETED,
    CLASS_DNS_RESOLUTION_FAILURE, CLASS_CONNECTION_TIMEOUT,
    CLASS_CONNECTION_RESET, CLASS_CONNECTION_REFUSED,
    CLASS_NETWORK_UNREACHABLE, CLASS_TEMPORARY_TRANSPORT_FAILURE)
from .logsetup import get_logger
from .status_file import write_status, write_last_failure
from .windows_update_guard import (RebootSentinel, assess_reboot_state,
                                   ltfs_current_mount_status,
                                   ltfs_media_health,
                                   ltfs_sync_mode_status,
                                   pending_reboot_reasons,
                                   reboot_block_reasons)
from .ltfs import (_ensure_lto_drive_ready, get_volume_label,
                   note_device_state_change)
from .ltfs_ownership import (LtfsOwnershipError, OWNERSHIP,
                             writer_timeout_seconds)
from .packer import LTOPacker
from .paths import (_LEGACY_PATH_LIMIT, _dir_tree_size,
                    _disambiguate_local_rel, _exceeds_legacy_path_limit,
                    _long, _remote_fetch_base_and_rel,
                    _reserved_name_component, _volume_cluster_size,
                    _winsafe_extracted_rel)
from .pipeline_types import ScanMetrics, StagedChunk, StreamState
from .ready_queue import ReadyQueue
from .remote_pipeline import RemotePipelineCoordinator
from .ram_telemetry import RamStageSampler
from .remote_transport import _remote_tar_fetch
from .resource_governor import ResourceGovernor
from .reporting import _write_source_missing_only_log
from .cli_errors import OperationalError
from .local_manifest_archive import validate_archive_root
from .planning import StreamingChunkBuilder
from .scan_frontier import (FrontierScanCoordinator,
                            build_frontier_scanner_factory,
                            incremental_scan_schema_ready)
from .runtime import (CANCEL, _acquire_tape_io_lock, _fmt_eta, _phase,
                      _priority_class, _progress_done, _progress_line,
                      _release_tape_io_lock, _status, compute_affinity_sets,
                      pin_current_process, unpin_current_process)
from .skipped import SkippedFileTracker
from .telegram_notify import TelegramNotifier, send_best_effort
from .ui import ConsoleUI

# Fetch/staging helpers now live with the code that uses them
# (src.remote_staging, Task 1.2). Re-exported here because they are part of this
# module's established public surface for tooling and tests.
from .remote_staging import (                                    # noqa: E402
    _FETCH_OVERRUN_WARN_FACTOR, _RESUME_MARKER, RemoteChunkStager,
    _classify_fetch_error, _fetch_watchdog_action,
    _is_transient_fetch_error)
from .remote_writer import RemoteChunkWriter                     # noqa: E402


class RemoteOrchestrator:
    """Public façade over the remote host -> staging -> tape pipeline.

    After Plan 1 this class **wires** the pipeline; it does not implement it.
    Behaviour lives in four focused modules, each owning one invariant:

    ===========================  ==================================================
    :mod:`src.scan_frontier`     which scanner may run, discovery, chunk sealing
    :mod:`src.remote_staging`    fetch + pack onto local staging — never any tape
    :mod:`src.remote_writer`     the finite write group — the ONLY tape path
    :mod:`src.remote_pipeline`   the single scheduling loop over the three
    ===========================  ==================================================

    What this class still owns: configuration, session lifecycle, the recorded
    stop reason, the pre-write safety gate, staging capacity, and the terminal
    classification of a run.

    Per-chunk flow:

    1. streaming ``find`` over SSH yields ``(path, size)`` records;
    2. :class:`~src.planning.StreamingChunkBuilder` seals chunks **in discovery
       order** against the staging budget and ``chunk_max_files`` — this is not
       greedy bin-packing, and boundaries therefore differ between a first run
       and a resumed one;
    3. per chunk: tar-over-SSH fetch -> ``LTOPacker.run()`` -> a finite write
       group -> ``LTOBackup.run()`` -> flush staging.

    Sessions persist in ``remote_sessions`` with growable
    ``remote_snapshots`` / ``remote_snapshot_files`` / ``remote_plans`` /
    ``remote_plan_files`` rows and per-chunk ``remote_chunks`` state, so an
    interrupted run resumes from authoritative chunk status. (There is no
    ``remote_manifest`` table; the older docstring naming one was wrong.)

    Safety invariants this façade must not let anyone route around:

    * **No LTFS access outside a finite write group.** Not at startup, not while
      waiting for work, not between group members, not at completion.
    * **The remote pipeline never ejects.** A cartridge ejected with nobody at
      the drive cannot be reloaded remotely.
    * **``backing`` is ambiguous and is never automatically retried.** It means
      the physical write began; the bytes may be on tape.
    """

    # Group/ownership metrics as CLASS defaults so they exist even on an
    # instance built without __init__ (the test suite constructs partial
    # orchestrators via __new__ to exercise the write path in isolation).
    _ownership_acquisitions = 0
    _readiness_checks = 0
    _cartridge_verifications = 0

    def __init__(self, cfg, db, ui=None, skipped_tracker=None):
        self.cfg          = cfg
        self.db           = db
        self.ui           = ui or ConsoleUI()
        self.skipped_tracker = skipped_tracker or SkippedFileTracker()
        self.notifier: Optional[TelegramNotifier] = (
            TelegramNotifier.from_config(cfg))
        self.remote_host  = cfg.remote_host
        self.remote_user  = cfg.remote_user
        self.remote_password = cfg.remote_password
        self.remote_path  = cfg.remote_path
        self.remote_scan_paths = cfg.remote_scan_paths
        self.remote_session_path = self._remote_session_key()
        self.confirm_before_backup = cfg.confirm_before_backup
        self.staging_dir  = cfg.staging_dir
        self.fill_pct     = cfg.staging_fill_pct

        # --- continuous-streaming pipeline tuning (from [PERFORMANCE]) --------
        self.chunk_cap_bytes   = int(cfg.chunk_cap_gb * 1024**3)
        self.staging_max_bytes = int(cfg.staging_max_gb * 1024**3)
        self.prefetch_ahead    = cfg.prefetch_chunks_ahead
        # Phase 4 byte-bounded ready queue (from [PIPELINE]). Phase 4.5 also
        # validates the limits against the staging budget so the queue can never
        # consume the space the producer needs for an active fetch+pack; on a
        # conflict this returns the documented default set (or fails closed).
        (self.ready_limits, self.ready_queue_reserve_bytes,
         self.effective_staging_bytes, self.ready_limits_source) = (
            cfg.validated_ready_queue_limits())
        # Staging-pressure drain state (Phase 4.5). The single authoritative
        # model is need-based: engage when the next chunk's fetch+pack footprint
        # cannot fit under the staging cap, clear only once there is comfortable
        # room again (hysteresis). See `_staging_pressure_decision`. Engaged and
        # cleared purely from local staging figures — never from LTFS state.
        self._staging_pressure_active = False
        # Group/ownership metrics — proved after a pilot by comparing these to
        # the number of chunks written: ownership and readiness must each be 1
        # per group, not 1 per chunk.
        self._ownership_acquisitions = 0
        self._readiness_checks = 0
        self._cartridge_verifications = 0
        self.staging_padding   = cfg.staging_padding_factor
        self.fetch_abort_factor = cfg.fetch_overrun_abort_factor
        self.fetch_stall_timeout = cfg.fetch_stall_timeout_seconds
        self.fetch_transient_retries = cfg.fetch_transient_retries
        self.fetch_transient_retry_base = cfg.fetch_transient_retry_base_seconds
        self.chunk_max_files  = cfg.chunk_max_files
        self.metadata_batch_size = cfg.governor_metadata_batch_size
        self.pack_file_batch_size = cfg.governor_pack_file_batch_size
        self.pack_parallel_workers = cfg.pack_parallel_workers
        self.fetch_parallel_streams = cfg.fetch_parallel_streams
        self.ram_sample_interval = cfg.governor_memory_sample_interval_seconds
        self.heartbeat_secs    = cfg.telegram_heartbeat_minutes * 60
        self.ssh_cipher        = cfg.ssh_cipher
        self.ssh_timeout       = cfg.ssh_command_timeout_seconds
        self.use_mbuffer       = cfg.use_mbuffer
        self.mbuffer_size      = cfg.mbuffer_size
        self.tape_priority     = _priority_class(cfg.robocopy_priority)
        self.fetch_cores, self.tape_cores = compute_affinity_sets(cfg.cpu_affinity)

        # Producer/consumer coordination (initialised per session).
        self._staged_bytes = 0                 # bytes currently resident in staging
        self._staged_lock  = threading.Lock()
        self._producer_err = None              # first fatal producer error, if any
        # The most specific stop reason recorded by whatever component decided to
        # stop; the terminal paths return this unchanged rather than re-deriving
        # a generic reason from the bare stop flag. See _record_stop.
        self._stop_result: Optional[StopResult] = None
        self._stop_lock = threading.Lock()
        # Classification of the last fetch failure, so a stop caused by an
        # exhausted retry / permanent auth failure carries the precise reason.
        self._last_fetch_failure = None
        self.governor = ResourceGovernor(cfg, self.staging_dir)

    def _eject_after_session(self):
        """Whether to physically eject once the session's last chunk is written.

        **Always False for the remote pipeline (Plan 1 Task 1.4).**

        `LtfsCmdEject` is mechanical and there is no software "load" for a
        cartridge out of the slot, so an eject at the end of a run that finishes
        unattended strands the drive until somebody travels to it — exactly what
        the no-physical-intervention policy exists to prevent. The setting was
        already defaulted off; Task 1.4 goes further and *refuses* it here,
        because "the operator turned it on once" is not a reason to leave an
        unattended run one config edit away from a cartridge nobody can reload.

        The setting still means what it says for the local orchestrator, which
        runs attended. This override is scoped to the remote pipeline only.
        """
        if bool(getattr(self.cfg, "eject_after_session", False)):
            get_logger().warning(
                "remote_eject_refused: [HARDWARE] eject_after_session is true, "
                "but the remote pipeline never ejects — a cartridge ejected "
                "with nobody at the drive cannot be reloaded remotely")
            print("[TAPE] NOTE: eject_after_session is ignored by the remote "
                  "pipeline; the cartridge stays loaded.")
        return False

    def _backup_writer(self, cls=LTOBackup):
        return cls(
            self.db,
            self.cfg.ibm_eject_cmd,
            tape_priority=self.tape_priority,
            tape_affinity=self.tape_cores,
            log_dir=self.cfg.backup_log_dir,
            notifier=self.notifier,
            governor=self.governor,
            index_min_file_mb=self.cfg.index_min_file_mb,
        )

    # ------------------------------------------------------------------
    # Extracted components (Task 1.2)
    #
    # Staging and writing live in their own modules now; this class stays the
    # public façade that owns session state, configuration and stop decisions.
    # The delegating methods below exist because they ARE the façade's API —
    # callers, and the pre-existing test suite, address the orchestrator. Each
    # component holds a back-reference, so overriding a hook on the
    # orchestrator still changes what the component does.
    #
    # Built lazily and cached: the test suite constructs partial orchestrators
    # via ``__new__``, so nothing may depend on ``__init__`` having run.
    # ------------------------------------------------------------------

    def _stager(self):
        component = getattr(self, "_stager_component", None)
        if component is None:
            component = RemoteChunkStager(self)
            self._stager_component = component
        return component

    def _writer(self):
        component = getattr(self, "_writer_component", None)
        if component is None:
            component = RemoteChunkWriter(self)
            self._writer_component = component
        return component

    # -- staging façade ---------------------------------------------------
    def _stage_chunk(self, session_id, chunk_index, chunk_files):
        return self._stager()._stage_chunk(session_id, chunk_index, chunk_files)

    def _discard_desc(self, desc):
        return self._stager()._discard_desc(desc)

    def _pack_inventory(self, pack_dir):
        return self._stager()._pack_inventory(pack_dir)

    def _preserve_desc(self, session_id, desc, why):
        return self._stager()._preserve_desc(session_id, desc, why)

    def _try_resume_pack(self, session_id, chunk_index, pack_dir):
        return self._stager()._try_resume_pack(session_id, chunk_index, pack_dir)

    def _fetch_chunk(self, session_id, chunk_index, chunk_files, fetch_dir):
        return self._stager()._fetch_chunk(
            session_id, chunk_index, chunk_files, fetch_dir)

    def _fetch_one_batch(self, remote_base, base_batch, fetch_dir, fetch_abort):
        return self._stager()._fetch_one_batch(
            remote_base, base_batch, fetch_dir, fetch_abort)

    @staticmethod
    def _fetch_backoff_delay(attempt, base):
        return RemoteChunkStager._fetch_backoff_delay(attempt, base)

    def _note_fetch_failure(self, err, retry_attempt=None,
                            next_retry_delay=None):
        return self._stager()._note_fetch_failure(
            err, retry_attempt=retry_attempt, next_retry_delay=next_retry_delay)

    def _fetch_batches_parallel(self, work_items, fetch_dir, fetch_abort,
                                streams):
        return self._stager()._fetch_batches_parallel(
            work_items, fetch_dir, fetch_abort, streams)

    def _fetch_collisions(self, session_id, collisions, fetch_dir,
                          fetch_abort, *args, **kwargs):
        return self._stager()._fetch_collisions(
            session_id, collisions, fetch_dir, fetch_abort, *args, **kwargs)

    def _start_fetch_monitor(self, stop_evt, abort_evt, fetch_dir, total_bytes):
        return self._stager()._start_fetch_monitor(
            stop_evt, abort_evt, fetch_dir, total_bytes)

    def _cleanup_remote_staging_dirs(self):
        return self._stager()._cleanup_remote_staging_dirs()

    def _cleanup_dir(self, path):
        return self._stager()._cleanup_dir(path)

    # -- writer façade ----------------------------------------------------
    def _write_chunk_group(self, session_id, descs, tape_label, eject_after,
                           stop_pipeline):
        return self._writer()._write_chunk_group(
            session_id, descs, tape_label, eject_after, stop_pipeline)

    def _write_skip_tape_chunk(self, session_id, desc, tape_label, eject_after):
        return self._writer()._write_skip_tape_chunk(
            session_id, desc, tape_label, eject_after)

    def _write_one_chunk_owned(self, session_id, desc, tape_label, eject_after):
        return self._writer()._write_one_chunk_owned(
            session_id, desc, tape_label, eject_after)

    def _ensure_remote_chunk_fits_tape(self, tape_label, planned_bytes,
                                       chunk_index):
        return self._writer()._ensure_remote_chunk_fits_tape(
            tape_label, planned_bytes, chunk_index)

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run(self, non_interactive=False, resume=False):
        """Interactive menu entry, or a promptless headless entry.

        Returns a :class:`StopResult`. Interactive callers may ignore it;
        headless callers map ``result.exit_code`` to the process exit code.
        ``non_interactive=True`` never prompts and never calls ``input()``.
        """
        try:
            self._validate_config()
            if non_interactive:
                return self._run_non_interactive(resume=resume)

            existing = self.db.get_active_remote_session(self.remote_host, self.remote_session_path)
            if existing:
                pending = self.db.get_pending_chunks(existing['session_id'])
                total   = self.db.count_chunks(existing['session_id'])
                done    = total - len(pending)
                print(f"\n[REMOTE] Found active session: {existing['session_label']}")
                print(f"         Created : {existing['created_at']}")
                print(f"         Progress: {done}/{total} chunks completed.")
                print("1. Resume from last completed chunk")
                print("2. Abandon and start a fresh session")
                print("0. Cancel")
                choice = self.ui.prompt("Choose: ").strip()
                if choice == '1':
                    return self._run_session(existing['session_id'])
                elif choice == '2':
                    print("[REMOTE] Starting a fresh-session scan. The current session "
                          "will remain resumable until the replacement is approved.")
                    return self._start_new_session(replacing_session=existing)
                else:
                    return StopResult(
                        exit_code=ExitCode.USER_STOP,
                        reason=REASON_USER_REQUESTED_STOP, resumable=True,
                        source="menu")

            return self._start_new_session()
        except RuntimeError as e:
            # _validate_config and similar operator-facing config errors.
            get_logger().exception("remote orchestrator config error")
            print(str(e))
            return self._finalize(StopResult(
                exit_code=ExitCode.FATAL_CONFIG, reason=REASON_BAD_CONFIG,
                resumable=False, source="config", detailed_reason=str(e)),
                phase="config")
        finally:
            self.skipped_tracker.print_summary(self.ui, self.cfg.backup_log_dir)

    def _run_non_interactive(self, resume=False):
        """Promptless dispatch for headless launches. Never calls input().

        Dispatches strictly by active-session count so it can never block on a
        prompt or guess which session to resume:

          --resume, exactly one active session   -> resume it
          --resume, zero active sessions          -> FATAL_CONFIG/no_active_session
          --resume, >1 active sessions            -> SAFETY_BLOCK/ambiguous
          without --resume                        -> FATAL_CONFIG/requires_resume

        A fresh session needs a tape choice and a confirmation prompt, so a
        promptless fresh start is out of scope until that workflow is defined.
        """
        if not resume:
            msg = ("--non-interactive requires --resume: a fresh session needs "
                   "interactive tape selection and confirmation.")
            print(f"[REMOTE] {msg}")
            return self._finalize(StopResult(
                exit_code=ExitCode.FATAL_CONFIG,
                reason=REASON_NONINTERACTIVE_REQUIRES_RESUME, resumable=False,
                source="headless", detailed_reason=msg), phase="headless")

        active = self.db.list_active_remote_sessions(
            self.remote_host, self.remote_session_path)
        if not active:
            msg = "no active session to resume for this host/path."
            print(f"[REMOTE] {msg}")
            return self._finalize(StopResult(
                exit_code=ExitCode.FATAL_CONFIG, reason=REASON_NO_ACTIVE_SESSION,
                resumable=False, source="headless", detailed_reason=msg),
                phase="headless")
        if len(active) > 1:
            labels = ", ".join(str(s.get('session_label')) for s in active)
            msg = (f"{len(active)} active sessions match this host/path "
                   f"({labels}); refusing to guess which to resume.")
            print(f"[REMOTE] {msg}")
            return self._finalize(StopResult(
                exit_code=ExitCode.SAFETY_BLOCK,
                reason=REASON_AMBIGUOUS_ACTIVE_SESSIONS, resumable=False,
                source="headless", detailed_reason=msg), phase="headless")

        session = active[0]
        print(f"[REMOTE] Headless resume of session: {session.get('session_label')}")
        return self._run_session(session['session_id'])

    # ------------------------------------------------------------------
    # Session lifecycle
    # ------------------------------------------------------------------

    def _validate_config(self):
        missing = [k for k in ('remote_host', 'remote_user', 'remote_path')
                   if not getattr(self.cfg, k)]
        if not self.remote_scan_paths:
            missing.append('remote_selected_paths')
        if missing:
            raise RuntimeError(
                f"[REMOTE] Missing values in [REMOTE] config section: "
                f"{', '.join(missing)}\n"
                f"Edit config.ini and fill in remote_host, remote_user, remote_path."
            )

        # M2: every selected scan path must equal remote_path or be a
        # posix-descendant of it — the same precondition the per-file fetch
        # resolver (_remote_fetch_base_and_rel) enforces. Reject a misconfigured
        # path up front with a clear message instead of failing mid-fetch.
        root = posixpath.normpath((self.remote_path or '').replace('\\', '/').strip())
        outside = []
        for raw in self.remote_scan_paths:
            p = posixpath.normpath((raw or '').replace('\\', '/').strip())
            if root == '/' or p == root or p.startswith(root.rstrip('/') + '/'):
                continue
            outside.append(raw)
        if outside:
            raise RuntimeError(
                "[REMOTE] These remote_selected_paths are not under "
                f"remote_path ({self.remote_path}):\n  "
                + "\n  ".join(outside)
                + "\nEach selected path must equal remote_path or be a "
                "subdirectory of it. Fix [REMOTE] remote_selected_paths in "
                "config.ini."
            )

    def _remote_session_key(self):
        if not self.remote_scan_paths or self.remote_scan_paths == [self.remote_path]:
            return self.remote_path
        return self.remote_path + '\n' + '\n'.join(self.remote_scan_paths)

    def _start_new_session(self, replacing_session=None):
        self._cleanup_remote_staging_dirs()

        # Task 1.4: the target cartridge is NAMED, never detected. Reading the
        # mounted volume label here was a device access outside any write group
        # — and worse, it made "whatever happens to be loaded right now" the
        # session's permanent catalog key. The physical cartridge is verified
        # once per finite group, under ownership, where a mismatch can be acted
        # on safely.
        tape_label = self._resolve_tape_label()
        if not tape_label:
            return StopResult(
                exit_code=ExitCode.USER_STOP, reason=REASON_USER_REQUESTED_STOP,
                resumable=True, source="new-session",
                detailed_reason="no tape label")

        if self.db.tape_exists(tape_label) and \
                self.db.count_tape_file_records(tape_label) > 0:
            print(f"[REMOTE] NOTE: tape '{tape_label}' already holds archived "
                  "data. A new session appends its own directory set to the "
                  "tape; existing data is not overwritten.")

        if not self.db.tape_exists(tape_label):
            print(f"[TAPE] '{tape_label}' not in database. Registering...")
            cap = input(f"Tape capacity in GB (default {DEFAULT_TAPE_CAPACITY_GB} "
                        "for 12 TB, Enter to skip): ").strip()
            self.db.register_tape(
                tape_label,
                int(cap) if cap.isdigit() else DEFAULT_TAPE_CAPACITY_GB)

        tape_context = self._remote_tape_capacity_context(tape_label)

        ts            = datetime.now().strftime('%Y%m%d_%H%M%S')
        session_label = f"REMOTE_{self.remote_host.split('.')[0]}_{ts}"

        print(f"\n[REMOTE] Session : {session_label}")
        print(f"[REMOTE] Base    : {self.remote_user}@{self.remote_host}:{self.remote_path}")
        if self.remote_scan_paths == [self.remote_path]:
            print(f"[REMOTE] Scanning {self.remote_path} ...")
        else:
            print("[REMOTE] Selected paths:")
            for path in self.remote_scan_paths:
                print(f"  - {path}")

        if not self._confirm_start(tape_label, tape_context):
            if replacing_session:
                print("[REMOTE] Cancelled before creating backup session. "
                      f"Previous session remains resumable: "
                      f"{replacing_session['session_label']}")
            else:
                print("[REMOTE] Cancelled before creating backup session.")
            return StopResult(
                exit_code=ExitCode.USER_STOP, reason=REASON_USER_REQUESTED_STOP,
                resumable=True, source="new-session",
                detailed_reason="not confirmed before write")

        session_id = self.db.create_remote_streaming_session(
            session_label=session_label,
            remote_host=self.remote_host,
            remote_user=self.remote_user,
            remote_path=self.remote_session_path,
            tape_label=tape_label,
            staging_dir=self.staging_dir,
        )
        if replacing_session:
            self.db.update_remote_session(
                replacing_session['session_id'],
                status='abandoned',
            )
            print(f"[REMOTE] Abandoned session: {replacing_session['session_label']}")

        return self._run_streaming_session(session_id)

    @staticmethod
    def _valid_tape_label(label):
        """A catalog volume label must be a nonempty single-line token.

        ``tapes.volume_label`` is the catalog's primary key for a cartridge and
        is embedded in restore paths, so a blank, whitespace-only or multi-line
        value must be refused at the point it is chosen — not discovered later
        by a foreign key error mid-session.
        """
        label = (label or '').strip()
        if not label:
            return None
        if any(ch in label for ch in '\r\n\t'):
            return None
        return label

    def _prompt_remote_tape_label(self):
        """Ask which cartridge this new session targets. No device access.

        Only reached for an INTERACTIVE fresh session with ``[REMOTE]
        tape_label`` blank. ``_run_non_interactive`` never gets here: a headless
        launch without ``--resume`` is refused before scanning or staging,
        whether the setting is blank or populated.
        """
        print("\n[TAPE] Which cartridge will this session write to?")
        print("       (set [REMOTE] tape_label in config.ini to skip this)")
        raw = input("Tape volume label (or Enter to cancel): ")
        return self._valid_tape_label(raw)

    def _resolve_tape_label(self):
        """The NEW session's target cartridge, chosen without touching the drive."""
        configured = self._valid_tape_label(
            getattr(self.cfg, 'remote_tape_label', ''))
        if configured:
            print(f"[TAPE] Target cartridge from config: {configured}")
            return configured
        return self._prompt_remote_tape_label()

    def _confirm_start(self, tape_label, tape_context):
        if not self.confirm_before_backup:
            return True
        print("\n[REMOTE] Approval required before backup starts.")
        print(f"  Host : {self.remote_user}@{self.remote_host}")
        print(f"  Tape : {tape_label}")
        print(f"  Base : {self.remote_path}")
        print("  Mode : continuous streaming scan -> fetch/pack -> tape")
        print(f"  Chunk: target up to {self._chunk_budget() / 1024**3:.2f} GiB")
        print(f"  Stage: prefetch {self.prefetch_ahead} ahead, cap "
              f"{self.staging_max_bytes / 1024**3:.0f} GiB")
        print(f"  Tape : {tape_context['available_bytes'] / 1024**3:.2f} GiB "
              "available under the DB safety budget")
        print("  Paths:")
        for path in self.remote_scan_paths:
            print(f"    - {path}")
        choice = input("Type 'yes' to start writing to tape: ").strip().lower()
        return choice == 'yes'

    def _remote_tape_capacity_context(self, tape_label, session_id=None):
        tape = self.db.get_tape(tape_label)
        if not tape:
            raise RuntimeError(f"[DB] Tape '{tape_label}' is not registered.")
        # Refuse to *start* on a retired cartridge rather than discovering it
        # mid-stream: this is the same "don't begin work that cannot finish"
        # rule as _pre_tape_write_reboot_check.
        if tape_is_full(tape.get('status')):
            raise RuntimeError(
                f"[TAPE] Tape '{tape_label}' is marked FULL in the database"
                f"{tape_status_reason_suffix(tape)}. Load and select a "
                "different cartridge before starting a write."
            )
        used_bytes = self.db.recalculate_tape_used_space(tape_label)
        reserved_bytes = 0
        if session_id is not None and hasattr(
                self.db, 'get_pending_remote_reserved_bytes'):
            reserved_bytes = self.db.get_pending_remote_reserved_bytes(session_id)
        capacity_bytes, available_bytes = tape_budget_bytes(
            tape['total_capacity'], used_bytes, reserved_bytes,
            status=tape.get('status'))
        print(f"[TAPE] '{tape_label}': DB occupied "
              f"{used_bytes / 1024**3:.2f} GiB; "
              f"reserved pending {reserved_bytes / 1024**3:.2f} GiB; "
              f"streaming available {available_bytes / 1024**3:.2f} GiB.")
        return {
            'used_bytes': used_bytes,
            'capacity_bytes': capacity_bytes,
            'reserved_bytes': reserved_bytes,
            'available_bytes': available_bytes,
        }

    # ------------------------------------------------------------------
    # Remote scanning
    # ------------------------------------------------------------------
    # Staging budget
    # ------------------------------------------------------------------

    def _chunk_budget(self):
        # Cap each chunk at chunk_cap_gb so the deep-prefetch pipeline can keep
        # 2+ chunks resident on the NVMe staging disk under the staging_max cap.
        os.makedirs(self.staging_dir, exist_ok=True)
        free = shutil.disk_usage(self.staging_dir).free
        usable = max(0, free - LOCAL_STAGING_RESERVE_BYTES)
        free_budget = int(usable * self.fill_pct)
        return min(free_budget, self.chunk_cap_bytes)

    # ------------------------------------------------------------------
    # Pipeline execution
    # ------------------------------------------------------------------

    def _assert_ownership_preflight(self, session_id, source):
        """Cross-process ownership preflight, before ANY worker thread or device
        access (Phase 3.5 / Phase 4.5-F).

        Config-only: it validates ``ltfs_ownership_id``, the timeout
        configuration and that the ``Global\\`` mutex can be created, so a host
        that cannot guarantee cross-session ownership fails here rather than
        part-way through a write. It performs no tape, mount, filesystem or IBM
        helper operation. Returns a finalized :class:`StopResult` to abort on
        failure, or ``None`` to proceed."""
        try:
            OWNERSHIP.assert_production_scope()
            return None
        except LtfsOwnershipError as e:
            msg = f"LTFS ownership preflight failed ({e.kind}): {e}"
            print(f"\n[TAPE] {msg}")
            get_logger().error("ltfs_ownership_preflight_failed: %s", e)
            return self._finalize(StopResult(
                exit_code=ExitCode.SAFETY_BLOCK,
                reason=REASON_LTFS_OWNERSHIP_UNAVAILABLE, resumable=False,
                source=source, session_id=session_id,
                detailed_reason=msg))

    def _assert_feature_gate(self, session_id, source):
        """Fail-closed sealed-batch feature gate (Phase 5B.5), before any worker.

        When the flag is DISABLED (the default), this returns immediately without
        constructing a repository, querying a batch table, running a schema
        check, or otherwise changing behaviour or adding startup latency.

        When the flag is ENABLED, it verifies the sealed-batch feature is ready
        (schema applied, checksum valid, exact schema validation passes) and
        fails closed BEFORE worker threads start — never falling back to legacy
        behaviour after the operator explicitly opted in. Passing the gate does
        NOT create or schedule any batch: Phase 5B.5 wires only the gate."""
        # getattr default False: a config without the flag is treated as
        # disabled (the fail-closed-safe direction), so the gate is a no-op.
        if not getattr(self.cfg, "sealed_tape_write_batches_enabled", False):
            return None
        from .sealed_batch_repository import (SealedBatchRepository,
                                              assert_feature_ready)
        try:
            repo = SealedBatchRepository(self.cfg.db_dsn)
            assert_feature_ready(self.cfg, repo)
            get_logger().info(
                "sealed_batch_feature_ready: gate passed; no scheduling in 5B.5")
            return None
        except Exception as e:
            msg = (f"sealed_tape_write_batches_enabled=true but the feature is "
                   f"not ready: {e}")
            print(f"\n[FEATURES] {msg}")
            get_logger().error("sealed_batch_feature_gate_failed: %s", e)
            return self._finalize(StopResult(
                exit_code=ExitCode.SAFETY_BLOCK,
                reason=REASON_SEALED_BATCH_FEATURE_UNAVAILABLE, resumable=False,
                source=source, session_id=session_id, detailed_reason=msg))

    # ------------------------------------------------------------------
    # Phase 5D: non-authoritative asynchronous observation
    # ------------------------------------------------------------------

    def _build_observation_worker(self):
        """Build the bounded observation worker, or None when observation mode
        is disabled (the default). Never raises into the caller: an init failure
        disables observation only and the writer continues normally."""
        try:
            from .sealed_batch_observation import maybe_build_observation_worker
            return maybe_build_observation_worker(self.cfg, self.cfg.db_dsn)
        except Exception:
            get_logger().exception("observation_worker_build_failed")
            return None

    def _capture_write_group_snapshot(self, correlation_id, session_id, descs,
                                      reason, tape_label, generation,
                                      scan_complete, producer_state, safe_stop,
                                      writer_path):
        """Copy an IMMUTABLE snapshot at the finite-group boundary. In-memory
        only: no DB query, no hashing, no JSON, no pack-content read (pack
        identity is the dir BASENAME already in metadata)."""
        from .sealed_batch_observation import WriteGroupSnapshot
        chunk_ids = tuple(d.chunk_index for d in descs)
        prepared = tuple(int(getattr(d, 'staged_bytes', 0) or 0) for d in descs)
        packs = tuple(
            (os.path.basename(getattr(d, 'pack_dir', '') or '') or None)
            for d in descs)
        return WriteGroupSnapshot(
            correlation_id=correlation_id, session_id=session_id,
            chunk_ids=chunk_ids, prepared_bytes=prepared,
            total_prepared_bytes=sum(prepared), pack_identities=packs,
            expected_tape=tape_label, selection_reason=reason,
            ready_queue_generation=int(generation),
            scan_complete=bool(scan_complete), producer_state=producer_state,
            staging_pressure=bool(getattr(self, '_staging_pressure_active',
                                          False)),
            safe_stop=bool(safe_stop),
            snapshot_ts_utc=datetime.now(timezone.utc).isoformat(),
            writer_path=writer_path)

    def _capture_group_outcome(self, correlation_id, session_id, descs,
                               stop_block, counts_before, group_start,
                               group_finish):
        """Build the correlated actual-outcome event. Reads only in-memory
        counters and the stop result; changes no writer or chunk state."""
        from .sealed_batch_observation import WriteGroupOutcome
        selected = tuple(d.chunk_index for d in descs)
        if stop_block is None:
            completed = selected
            started = selected
            failing = None
            reason = None
        else:
            failing = stop_block.chunk_index
            completed = tuple(d.chunk_index for d in descs
                              if failing is not None and d.chunk_index < failing)
            started = completed + ((failing,) if failing is not None else ())
            reason = stop_block.reason
        own0, rdy0, cart0 = counts_before
        return WriteGroupOutcome(
            correlation_id=correlation_id, session_id=session_id,
            selected_chunk_ids=selected, started_chunk_ids=started,
            completed_chunk_ids=completed, failing_chunk=failing,
            stop_reason=reason, group_start_ts=group_start,
            group_finish_ts=group_finish,
            ownership_acquisitions=self._ownership_acquisitions - own0,
            readiness_checks=self._readiness_checks - rdy0,
            cartridge_verifications=self._cartridge_verifications - cart0,
            writer_invocations=len(started))

    def _session_predates_frontier(self, session_id):
        """True when this session still has whole-root snapshot rows to match.

        Such a session was planned before the frontier existed, so every
        segment imported for it must be reconciled ONCE against
        ``remote_snapshot_files`` before its entries reach the chunk builder.
        A session that already owns frontier scopes needs no such reconciliation
        — its membership came from segments in the first place.

        Fails towards *reconcile* on any uncertainty: reconciling a session that
        did not need it costs one set-based query per segment, while skipping it
        for one that did would re-plan files that are already on tape.
        """
        probe = getattr(self.db, 'session_has_frontier_state', None)
        if probe is None:
            return True
        try:
            return not bool(probe(session_id))
        except Exception:
            get_logger().warning(
                "could not determine frontier state for session %s; "
                "reconciling imported segments against the legacy snapshot",
                session_id, exc_info=True)
            return True

    def _scan_artifact_root(self):
        """Where scan-segment artifacts live — proven, not assumed.

        The frontier writes a segment artifact per directory *during scanning*.
        If that root ever resolved onto the LTFS mount, scanning would be
        writing to tape outside a finite write group — the one thing the whole
        ownership design exists to prevent — and onto a medium that cannot take
        small random writes. If it resolved inside staging, archive cleanup
        would delete the frontier's evidence underneath it.

        So the containment is checked here rather than trusted from config.
        Raising is correct: the caller has not started a scan, claimed a
        directory or touched the drive at this point, so a misconfigured root
        stops the run with nothing to undo.
        """
        root = validate_archive_root(
            self.cfg.local_manifest_archive_root,
            (self.staging_dir, getattr(self.cfg, "lto_drive", None)))
        drive = os.path.splitdrive(os.path.abspath(root))[0].rstrip(":").upper()
        lto = os.path.splitdrive(
            str(getattr(self.cfg, "lto_drive", "") or ""))[0]
        if drive and lto and drive == lto.rstrip(":").upper():
            raise OperationalError(
                f"[SCAN] The scan-artifact root {root} is on the LTFS drive "
                f"{lto}. Scan segments are written during traversal, outside "
                "any write group; they must live on local storage.")
        return root

    def _require_frontier_schema(self, session_id):
        """Refuse to scan unless the frontier schema is usable. Fail closed.

        The frontier is now the only scanner, so an unusable schema is a hard
        stop rather than a quiet downgrade — a downgrade is what would put a
        second scanner on one frontier. Nothing has been claimed or written at
        this point, so the session stays exactly where it was.
        """
        ready, reason = incremental_scan_schema_ready(self.db)
        if ready:
            return None
        msg = (f"session {session_id}: the incremental-scan schema is not "
               f"usable ({reason}). Apply migration 014 with "
               f"`inspect_db.py --apply-incremental-scan-schema --execute "
               f"--yes --finalize` before scanning.")
        print(f"\n[SCAN] SAFETY STOP: {msg}")
        get_logger().error("scan_frontier_unavailable: %s", msg)
        send_best_effort(self.notifier, f"[PIPELINE] SAFETY STOP: {msg}")
        return StopResult(
            exit_code=ExitCode.SAFETY_BLOCK,
            reason=REASON_SCAN_FRONTIER_UNAVAILABLE, resumable=False,
            source="scan-mode", session_id=session_id, detailed_reason=msg)

    def _build_pipeline(self, *, session_id, tape_label, ready_q, stop_event,
                        metrics, scan_coordinator, scan_complete, writer_path):
        """Construct the single scheduling loop both session kinds use."""
        return RemotePipelineCoordinator(
            host=self,
            session_id=session_id,
            tape_label=tape_label,
            ready_q=ready_q,
            stop_event=stop_event,
            metrics=metrics,
            scan_coordinator=scan_coordinator,
            backlog_limit=getattr(
                self.cfg, 'max_unstaged_backlog_chunks', 64),
            observation_worker=self._build_observation_worker(),
            writer_path=writer_path,
            scan_complete=scan_complete,
        )

    def _run_streaming_session(self, session_id):
        """Scan, persist, stage, and write chunks as one continuous pipeline.

        Returns a :class:`StopResult` describing how the run ended."""
        # Ownership PREFLIGHT, before any worker thread or device access.
        preflight_block = self._assert_ownership_preflight(session_id, "startup")
        if preflight_block is not None:
            return preflight_block

        # Sealed-batch feature gate (disabled by default -> no-op, no DB).
        feature_block = self._assert_feature_gate(session_id, "startup")
        if feature_block is not None:
            return feature_block

        session_row = self.db.get_remote_session(session_id)
        tape_label = session_row['tape_label']

        # The frontier is the only scanner, so this is a hard schema gate now
        # rather than a mode choice: no usable schema means no scan, not a
        # quiet downgrade to whole-root replay. Nothing here touches the drive,
        # the mount or the source.
        scan_mode_block = self._require_frontier_schema(session_id)
        if scan_mode_block is not None:
            return self._finalize(scan_mode_block, phase="scan-mode")

        # PostgreSQL-only: it compares the session's persisted generation with
        # the catalog's, and needs no tape, mount or ownership to do it.
        generation_block = self._verify_session_tape_generation(session_row)
        if generation_block is not None:
            return self._finalize(generation_block, phase="resume-precheck")

        # Task 1.4: NO readiness probe and NO cartridge read here. Both are
        # device accesses, and running them at startup means the drive is
        # touched while the pipeline may then sit idle for a whole fetch+pack
        # cycle before it writes anything. They now happen exactly once per
        # finite write group, inside the ownership period. What replaces the
        # early check is an announcement, not silence — see incident 011.
        self._announce_target_cartridge(session_id, tape_label)

        self._staged_bytes = 0
        self._producer_err = None
        self._last_fetch_failure = None
        self._producer_chunk = None
        self._consumer_chunk = None
        self._staging_pressure_active = False

        # Before any thread starts: a chunk left 'backing' by a prior run may
        # already be on tape. Refuse to resume blindly rather than double-write.
        prior_block = self._detect_prior_backing_chunks(session_id)
        if prior_block is not None:
            return self._finalize(prior_block, phase="resume-precheck")

        if self.fetch_cores:
            pin_current_process(self.fetch_cores, label='fetch/pack')

        _phase('PIPELINE', "Continuous remote stream to tape | "
                           f"prefetch {self.prefetch_ahead} ahead | "
                           f"staging cap {self.staging_max_bytes / 1024**3:.0f} GB")
        print(f"[WARNING] {LTFS_WRITE_WARNING}")

        if not self._validate_ltfs_sync_mode():
            return self._finalize(StopResult(
                exit_code=ExitCode.SAFETY_BLOCK,
                reason=REASON_LTFS_SYNC_MODE_NOT_TIME5, resumable=False,
                source="streaming", session_id=session_id,
                detailed_reason="LTFS mount is not time@5"))

        # Phase 4: byte-bounded ready queue. The producer keeps preparing while
        # several chunks wait on NVMe; the writer then drains a finite group
        # under one ownership period instead of one chunk per acquisition.
        ready_q = ReadyQueue(self.ready_limits, name=f"session{session_id}")
        print(f"  Ready queue: start >= {self.ready_limits.min_start_bytes / 1024**3:.0f} GiB, "
              f"target {self.ready_limits.target_bytes / 1024**3:.0f} GiB, "
              f"max {self.ready_limits.max_bytes / 1024**3:.0f} GiB / "
              f"{self.ready_limits.max_chunks} chunks")
        stop_pipeline = threading.Event()
        hb_stop = threading.Event()

        # Same forced-update protection as the resume path: stop at a chunk
        # boundary while LTFS can still sync, rather than be killed mid-write.
        reboot_sentinel = RebootSentinel(
            stop_pipeline,
            include_soft=self._block_on_soft_reboot_marker(),
            on_detect=lambda reasons: (
                self._record_reboot_stop(reasons),
                send_best_effort(
                    self.notifier,
                    "[PIPELINE] Windows staged a restart — stopping at the next "
                    "chunk boundary so the tape index is synced. Re-run option 6 "
                    "to resume after the host restarts."))).start()

        tape_context = self._remote_tape_capacity_context(
            tape_label, session_id=session_id)
        remaining_lock = threading.Lock()
        state = StreamState(
            remaining_bytes=tape_context['available_bytes'],
            next_chunk_index=(
                self.db.get_next_remote_chunk_index(session_id)
                if hasattr(self.db, 'get_next_remote_chunk_index')
                else self.db.count_chunks(session_id)
            ),
        )

        def _on_budget_exceeded(msg):
            """The next chunk cannot fit under the tape's DB safety budget."""
            self._record_stop(StopResult(
                exit_code=ExitCode.SAFETY_BLOCK,
                reason=REASON_UNEXPECTED_TAPE_OR_DB_STATE, resumable=False,
                source="scanner", session_id=session_id,
                detailed_reason=msg))
            stop_pipeline.set()

        def _on_scan_error(exc):
            self._producer_err = str(exc)
            self._record_stop(StopResult(
                exit_code=ExitCode.TRANSIENT_RESUMABLE,
                reason=REASON_STOPPED_AT_CHUNK_BOUNDARY, resumable=True,
                source="scanner", session_id=session_id,
                detailed_reason=f"scan failed: {exc}"))
            stop_pipeline.set()

        # Task 1.3: ONE coordinator schedules scanner -> stager -> ready queue ->
        # finite write group, for every session kind. Work selection is
        # authoritative (chunk status), so the resumed backlog never queues in
        # front of renewed exploration; the scanner is admitted to publish while
        # sealed-but-unstaged work is under the configured limit.
        pipeline = self._build_pipeline(
            session_id=session_id,
            tape_label=tape_label,
            ready_q=ready_q,
            stop_event=stop_pipeline,
            metrics=state.metrics,
            scan_coordinator=None,          # filled in below
            scan_complete=bool(session_row and session_row.get(
                'scan_complete')),
            writer_path='streaming')

        # Plan 1 completion: the frontier scanner is THE production scanner.
        # There is no legacy fallback here on purpose — a runtime fallback is
        # how two scanners end up running against one frontier. Git history and
        # the verified PostgreSQL backup are the rollback path.
        budget_bytes = self._chunk_budget()
        alloc_unit = _volume_cluster_size(self.staging_dir)
        padding_factor = self.staging_padding
        max_files = self.chunk_max_files

        def _builder_factory():
            return StreamingChunkBuilder(
                budget_bytes, alloc_unit=alloc_unit,
                padding_factor=padding_factor, max_files=max_files)

        scan_coordinator = FrontierScanCoordinator(
            db=self.db,
            session_id=session_id,
            scan_paths=self.remote_scan_paths,
            archive_root=self._scan_artifact_root(),
            state=state,
            remaining_lock=remaining_lock,
            stop_event=stop_pipeline,
            builder_factory=_builder_factory,
            # A session that pre-dates the frontier still has whole-root
            # snapshot rows, so each imported segment is reconciled against
            # them ONCE, set-based, before anything reaches the chunk builder.
            legacy_session=self._session_predates_frontier(session_id),
            scanner_factory=build_frontier_scanner_factory(
                remote_user=self.remote_user,
                remote_host=self.remote_host,
                remote_password=self.remote_password,
                skipped_tracker=self.skipped_tracker,
                ui=self.ui,
            ),
            ui=self.ui,
            on_budget_exceeded=_on_budget_exceeded,
            on_scan_error=_on_scan_error,
            publication_gate=pipeline.publication_gate,
            on_finished=pipeline.note_scanner_finished,
        )
        pipeline.scan_coordinator = scan_coordinator

        self._start_pipeline_heartbeat(hb_stop, ready_q, "streaming")
        try:
            outcome = pipeline.run()
        finally:
            hb_stop.set()
            reboot_sentinel.stop()
            if pipeline.observation_worker is not None:
                pipeline.observation_worker.shutdown()   # bounded
            if self.fetch_cores:
                unpin_current_process()
        completed = outcome.completed_chunks
        failed = outcome.failed
        stop_block = outcome.stop_block

        # Authoritative completion: the scan finished AND every chunk is 'done'.
        # An ambiguous ('backing') chunk keeps the session incomplete, so it is
        # handled by the stop_block branch below (returns 20).
        session_row = self.db.get_remote_session(session_id)
        scan_done = bool(session_row and session_row.get('scan_complete'))
        remaining = self.db.get_pending_chunks(session_id) if scan_done else [None]
        if scan_done and not remaining:
            # Everything is committed — nothing to resume. Mark the session
            # complete NOW, even if the user cancelled during the final write.
            self.db.update_remote_session(
                session_id, status='completed',
                completed_at=datetime.now().isoformat())
            recorded = self._get_recorded_stop()
            if recorded is not None and recorded.exit_code != ExitCode.COMPLETED:
                # Cancel-during-final-write: session complete, this run exits with
                # the recorded stop. Do NOT physically eject on a user cancel.
                print("\n[REMOTE] Session complete (all streamed chunks "
                      f"archived); this run was stopped by the user "
                      f"({recorded.reason}).")
                send_best_effort(
                    self.notifier,
                    "[PIPELINE] Session complete — all chunks archived; run "
                    f"stopped by user ({recorded.reason}).")
                return self._finalize(recorded, phase="streaming")
            # Task 1.4: completion is a DATABASE event. No eject, no readiness
            # probe, no cartridge read — nothing here touches the drive, so a
            # session that finishes unattended leaves it exactly as it found it.
            print("[REMOTE] Leaving the cartridge loaded (the remote pipeline "
                  "never ejects).")
            print("\n[REMOTE] Session complete. All streamed chunks archived.")
            send_best_effort(
                self.notifier,
                f"[PIPELINE] Session complete - {completed} chunk(s) "
                "written in this run.")
            return self._finalize(StopResult(
                exit_code=ExitCode.COMPLETED, reason=REASON_COMPLETED,
                resumable=False, source="streaming", session_id=session_id),
                phase="done")

        if stop_block is not None:
            # _write_chunk already recorded the specific reason (gate or write).
            print(f"\n[REMOTE] Streaming pipeline stopped at a chunk boundary "
                  f"({stop_block.reason}). Re-run option 6 to resume.")
            return self._finalize(self._stop_result or stop_block,
                                  phase="streaming")
        if CANCEL.is_set():
            print("\n[ABORTED] Stopped by user. Session saved - re-run option 6 "
                  "to resume from the interrupted chunk.")
            return self._finalize(self._stop_result or StopResult(
                exit_code=ExitCode.USER_STOP, reason=REASON_USER_REQUESTED_STOP,
                resumable=True, source="streaming", session_id=session_id),
                phase="streaming")
        if failed or self._producer_err or state.scan_error:
            msg = self._producer_err or state.scan_error or (
                "a chunk failed during tape write")
            print(f"\n[REMOTE] Streaming pipeline stopped: {msg}. "
                  "Re-run to resume when the condition is fixed.")
            send_best_effort(
                self.notifier,
                f"[PIPELINE] STOPPED: {msg}. Re-run to resume.")
            return self._finalize(self._stop_result or StopResult(
                exit_code=ExitCode.TRANSIENT_RESUMABLE,
                reason=REASON_STOPPED_AT_CHUNK_BOUNDARY, resumable=True,
                source="streaming", session_id=session_id, detailed_reason=msg),
                phase="streaming")
        if scan_done:
            print(f"\n[REMOTE] Scan complete; {len(remaining)} chunk(s) "
                  "remain pending. Re-run to resume.")
        # Scan not complete or chunks remain: resumable, not an error.
        return self._finalize(self._stop_result or StopResult(
            exit_code=ExitCode.TRANSIENT_RESUMABLE,
            reason=REASON_STOPPED_AT_CHUNK_BOUNDARY, resumable=True,
            source="streaming", session_id=session_id,
            detailed_reason="scan or chunks still pending"), phase="streaming")

    def _run_session(self, session_id):
        """Stream pending chunks to tape with a deep-prefetch pipeline.

        A background producer fetches + packs chunks onto NVMe staging up to
        `prefetch_ahead` chunks in front of the tape writer, while this thread
        (the consumer) keeps robocopy streaming to the LTO drive. The staging
        footprint is capped (backpressure) so the disk never overruns, and the
        tape never starves on the network (no shoe-shining)."""
        session_row    = self.db.get_remote_session(session_id)
        tape_label     = session_row['tape_label']
        if not session_row.get('scan_complete', True):
            return self._run_streaming_session(session_id)

        # Ownership PREFLIGHT for the scan-complete resume path, before any
        # worker thread or device access (the streaming path above runs its own).
        # Config-only: validates ltfs_ownership_id and proves the Global\ mutex
        # can be created, so a host that cannot guarantee cross-session ownership
        # fails here instead of part-way through a write. No LTFS/mount/device op.
        preflight_block = self._assert_ownership_preflight(session_id, "resume")
        if preflight_block is not None:
            return preflight_block

        # Sealed-batch feature gate (disabled by default -> no-op, no DB).
        feature_block = self._assert_feature_gate(session_id, "resume")
        if feature_block is not None:
            return feature_block

        generation_block = self._verify_session_tape_generation(session_row)
        if generation_block is not None:
            return self._finalize(generation_block, phase="resume-precheck")

        pending_chunks = self.db.get_pending_chunks(session_id)
        total_chunks   = self.db.count_chunks(session_id)
        done_count     = total_chunks - len(pending_chunks)

        if total_chunks == 0:
            # A session without a plan archived nothing; recording it as
            # 'completed' would fabricate provenance in the catalog.
            print("[REMOTE] Session has no planned chunks; marking it "
                  "abandoned. Start a fresh session to archive.")
            self.db.update_remote_session(
                session_id, status='abandoned',
                completed_at=datetime.now().isoformat()
            )
            return self._finalize(StopResult(
                exit_code=ExitCode.SAFETY_BLOCK,
                reason=REASON_UNEXPECTED_TAPE_OR_DB_STATE, resumable=False,
                source="resume", session_id=session_id,
                detailed_reason="session has no planned chunks"), phase="resume")

        if not pending_chunks:
            print("[REMOTE] All chunks already completed.")
            self.db.update_remote_session(
                session_id, status='completed',
                completed_at=datetime.now().isoformat()
            )
            return self._finalize(StopResult(
                exit_code=ExitCode.COMPLETED, reason=REASON_COMPLETED,
                resumable=False, source="resume", session_id=session_id),
                phase="done")

        # Task 1.4: no readiness probe and no cartridge read here either. The
        # finite write group owns both, under LTFS ownership. A resumed session
        # is exactly the case incident 011 is about — its tape_label comes from
        # a row written days ago — so it gets the same loud announcement.
        self._announce_target_cartridge(session_id, tape_label)

        # Before any thread starts: a chunk left 'backing' by a prior run may
        # already be on tape. Refuse to resume blindly rather than double-write.
        # PostgreSQL-only; no device access.
        prior_block = self._detect_prior_backing_chunks(session_id)
        if prior_block is not None:
            return self._finalize(prior_block, phase="resume-precheck")

        # --- per-session pipeline state ---
        self._staged_bytes   = 0
        self._producer_err   = None
        self._last_fetch_failure = None
        self._producer_chunk = None
        self._consumer_chunk = None
        last_chunk = pending_chunks[-1]
        # The scan-complete resume path explores nothing, so its exploration
        # counters stay zero by construction — which is exactly the comparison
        # Task 0.2 needs: time-to-first-staged/first-write without any scan.
        metrics = ScanMetrics()

        # Pin fetch/packing (this process) to the fetch cores so the tape
        # writer's cores stay free of SSH decryption + Python packing.
        if self.fetch_cores:
            pin_current_process(self.fetch_cores, label='fetch/pack')

        _phase('PIPELINE', f"Streaming {len(pending_chunks)} chunk(s) to tape "
                           f"({done_count}/{total_chunks} already done) | prefetch "
                           f"{self.prefetch_ahead} ahead | staging cap "
                           f"{self.staging_max_bytes / 1024**3:.0f} GB")
        print(f"[WARNING] {LTFS_WRITE_WARNING}")

        # Only per-chunk byte totals stay resident. One GROUP BY aggregate
        # replaces the former per-chunk full-row fetches (millions of rows
        # over the wire for large sessions); the producer still re-reads each
        # chunk's rows from the catalog just before staging it.
        size_summary = self.db.get_chunk_size_summary(session_id)
        planned = {ci: size_summary.get(ci, (0, 0, 0)) for ci in pending_chunks}
        self._validate_pending_chunk_limits(
            session_id, pending_chunks, planned)

        # Task 1.3: the SAME byte-bounded ready queue and the SAME finite write
        # group as a still-scanning session. The old group-of-one bypass here
        # (one ownership period per chunk) is gone: a scan-complete resume is
        # just this pipeline with no scanner attached.
        ready_q       = ReadyQueue(self.ready_limits, name=f"session{session_id}")
        stop_pipeline = threading.Event()

        # Race a forced update restart rather than trying to prevent one: stop
        # at the next chunk boundary so LTFS syncs its index while it still can.
        reboot_sentinel = RebootSentinel(
            stop_pipeline,
            include_soft=self._block_on_soft_reboot_marker(),
            on_detect=lambda reasons: (
                self._record_reboot_stop(reasons),
                send_best_effort(
                    self.notifier,
                    "[PIPELINE] Windows staged a restart — stopping at the next "
                    "chunk boundary so the tape index is synced. Re-run option 6 "
                    "to resume after the host restarts."))).start()

        pipeline = self._build_pipeline(
            session_id=session_id,
            tape_label=tape_label,
            ready_q=ready_q,
            stop_event=stop_pipeline,
            metrics=metrics,
            scan_coordinator=None,          # scan already complete
            scan_complete=True,
            writer_path='resume')

        hb_stop = threading.Event()
        self._start_pipeline_heartbeat(hb_stop, ready_q, total_chunks)
        try:
            outcome = pipeline.run()
        finally:
            hb_stop.set()
            reboot_sentinel.stop()
            if pipeline.observation_worker is not None:
                pipeline.observation_worker.shutdown()   # bounded
            if self.fetch_cores:
                unpin_current_process()
        completed = outcome.completed_chunks
        failed = outcome.failed
        stop_block = outcome.stop_block

        # Authoritative completion from the DB: are ALL chunks committed 'done'?
        # An ambiguous ('backing') chunk is NOT done, so it keeps the session
        # incomplete and is handled by the stop_block branch below.
        remaining = self.db.get_pending_chunks(session_id)
        if not remaining:
            # Everything is on tape and committed — nothing to resume. Mark the
            # session complete NOW, even if the user cancelled during the final
            # write (the data is complete; there is nothing left for a resume).
            self.db.update_remote_session(
                session_id, status='completed',
                completed_at=datetime.now().isoformat())
            recorded = self._get_recorded_stop()
            if recorded is not None and recorded.exit_code != ExitCode.COMPLETED:
                # Cancel-during-final-write: session complete, but THIS run exits
                # with the recorded stop (e.g. 40/user_requested_stop).
                print("\n[REMOTE] Session complete (all chunks archived); this "
                      f"run was stopped by the user ({recorded.reason}).")
                send_best_effort(
                    self.notifier,
                    "[PIPELINE] Session complete — all chunks archived; run "
                    f"stopped by user ({recorded.reason}).")
                return self._finalize(recorded, phase="resume")
            print("\n[REMOTE] Session complete. All chunks archived to tape.")
            send_best_effort(
                self.notifier,
                f"[PIPELINE] Session complete — all {total_chunks} chunk(s) "
                "archived to tape.")
            return self._finalize(StopResult(
                exit_code=ExitCode.COMPLETED, reason=REASON_COMPLETED,
                resumable=False, source="resume", session_id=session_id),
                phase="done")

        if stop_block is not None:
            print(f"\n[REMOTE] Stopped at a chunk boundary "
                  f"({stop_block.reason}). The tape index was synced and the "
                  "session is resumable — re-run option 6 to resume.")
            return self._finalize(self._stop_result or stop_block, phase="resume")
        if CANCEL.is_set():
            print("\n[ABORTED] Stopped by user. Session saved — "
                  "re-run option 6 to resume from the interrupted chunk.")
            return self._finalize(self._stop_result or StopResult(
                exit_code=ExitCode.USER_STOP, reason=REASON_USER_REQUESTED_STOP,
                resumable=True, source="resume", session_id=session_id),
                phase="resume")
        if failed or self._producer_err:
            msg = self._producer_err or "a chunk failed during tape write"
            print(f"\n[REMOTE] Pipeline stopped: {msg}. "
                  f"Re-run to resume from the failed chunk.")
            send_best_effort(
                self.notifier,
                f"[PIPELINE] STOPPED: {msg}. Re-run to resume from the "
                "failed chunk.")
            return self._finalize(self._stop_result or StopResult(
                exit_code=ExitCode.TRANSIENT_RESUMABLE,
                reason=REASON_STOPPED_AT_CHUNK_BOUNDARY, resumable=True,
                source="resume", session_id=session_id, detailed_reason=msg),
                phase="resume")
        # Some chunks still pending (e.g. a partial run): resumable, not an error.
        return self._finalize(self._stop_result or StopResult(
            exit_code=ExitCode.TRANSIENT_RESUMABLE,
            reason=REASON_STOPPED_AT_CHUNK_BOUNDARY, resumable=True,
            source="resume", session_id=session_id,
            detailed_reason="chunks still pending"), phase="resume")

    # ------------------------------------------------------------------
    # Producer: fetch + pack a chunk onto staging  (runs off the main thread)
    # ------------------------------------------------------------------

    def _physical_estimate(self, logical_bytes, file_count):
        """Upper-bound staging footprint for a set of files: logical bytes
        plus one allocation cluster per file (size-on-disk rounding), times
        the configured padding factor."""
        cluster = _volume_cluster_size(self.staging_dir)
        return int((logical_bytes + file_count * cluster) * self.staging_padding)

    def _validate_chunk_file_limit(self, session_id, chunk_index, file_count):
        limit = int(getattr(self, 'chunk_max_files', 100000))
        if int(file_count) <= limit:
            return
        if getattr(self.cfg, 'allow_resume_oversized_chunks', False):
            print(f"[REMOTE] Warning: session {session_id} chunk "
                  f"{chunk_index + 1} has {int(file_count):,} files, above "
                  f"chunk_max_files={limit:,}; override is enabled.")
            return
        session = self.db.get_remote_session(session_id)
        label = (session or {}).get('session_label', f'id {session_id}')
        raise RuntimeError(
            "[REMOTE] Refusing to resume an oversized legacy chunk. "
            f"Session: {label} (id {session_id}); chunk: {chunk_index + 1}; "
            f"file count: {int(file_count):,}; configured limit: {limit:,}. "
            "Abandon and replan the session so chunks are split safely, or set "
            "allow_resume_oversized_chunks=true only for an explicit one-time "
            "override."
        )

    def _validate_pending_chunk_limits(self, session_id, pending_chunks,
                                       size_summary):
        for chunk_index in pending_chunks:
            _planned_bytes, _present_bytes, file_count = size_summary.get(
                chunk_index, (0, 0, 0))
            self._validate_chunk_file_limit(
                session_id, chunk_index, file_count)

    # ------------------------------------------------------------------
    # Staging-pressure drain (Phase 4.5)
    # ------------------------------------------------------------------

    def _staging_pressure_decision(self, resident, need):
        """Pure hysteretic engage/clear decision from the SAME figures the
        capacity gate uses (``resident`` = staged bytes on disk, ``need`` = the
        next chunk's ~2x fetch+pack footprint).

        Returns ``True`` to engage, ``False`` to clear, ``None`` to hold the
        current state (the hysteresis band). Touches no LTFS state whatsoever.

        * engage when the next chunk's footprint will not fit under the cap
          (``resident + need > staging_max``) — the producer is about to block,
          so the writer should drain a finite ready group even below the minimum;
        * clear only once there is comfortable room for *two* more footprints
          (``resident + 2*need <= staging_max``), so a value oscillating around
          the exact boundary cannot flap the signal and start tiny groups;
        * nothing staged (``resident <= 0``) is never pressure.
        """
        if self.staging_max_bytes <= 0 or resident <= 0:
            return False
        if resident + need > self.staging_max_bytes:
            return True
        if resident + 2 * need <= self.staging_max_bytes:
            return False
        return None

    def _apply_staging_pressure(self, ready_q, resident, need):
        """Engage/clear the ready queue's staging-pressure drain (Phase 4.5).

        This is the production wiring: the orchestrator's own capacity gate calls
        it with the authoritative resident-staging figure, and it is the only
        place ``ReadyQueue.set_staging_pressure`` is toggled. Reads local staging
        only; issues no device, mount or IBM-helper call."""
        if ready_q is None or not hasattr(ready_q, 'set_staging_pressure'):
            return
        decision = self._staging_pressure_decision(resident, need)
        if decision is True and not self._staging_pressure_active:
            self._staging_pressure_active = True
            ready_q.set_staging_pressure(True)
            get_logger().info(
                "staging_pressure_engaged: resident=%d need=%d staging_max=%d",
                resident, need, self.staging_max_bytes)
        elif decision is False and self._staging_pressure_active:
            self._staging_pressure_active = False
            ready_q.set_staging_pressure(False)
            get_logger().info(
                "staging_pressure_cleared: resident=%d need=%d staging_max=%d",
                resident, need, self.staging_max_bytes)

    def _signal_producer_completion(self, ready_q, stop_evt):
        """Translate WHY the producer thread is exiting into ready-queue state.

        Distinguishes the states the writer-start rules care about:

        * **normal completion** — the scan finished and every planned chunk was
          staged and enqueued, with no stop: ``close()`` so a final partial group
          below the minimum threshold drains cleanly. This is the only path that
          forces a sub-minimum tape write, and only because it is genuine
          end-of-work, not a stop.
        * **terminal producer failure** (``self._producer_err`` set): mark input
          exhausted AND close. The stop is already recorded and ``stop_evt`` set,
          so the consumer breaks *before* ``wait_for_group`` and the outer finally
          preserves every queued pack — a failure never forces a final write.
        * **safe stop / cancel**: close to release a possibly-waiting consumer;
          the consumer breaks on ``stop_evt`` and queued packs are preserved.

        A *temporary* upstream pause never reaches here: the stager keeps looping
        on ``chunk_q.get()``, so transient emptiness never starts a tiny group.
        """
        if self._producer_err is not None:
            ready_q.set_producer_exhausted(True)
            ready_q.close("producer failed terminally; queued packs preserved "
                          "for resume")
        elif CANCEL.is_set() or stop_evt.is_set():
            ready_q.close("producer stopped at a boundary; consumer preserves "
                          "queued packs")
        else:
            ready_q.close("producer finished normally; a final partial group "
                          "may drain")

    def _await_staging_capacity(self, planned_bytes, planned_files, stop_evt,
                                ready_q=None):
        """Block until there is room to stage another chunk without breaching the
        staging cap or starving the disk. Accounts for the ~2x transient
        footprint while a chunk is packed (fetch_dir + pack_dir coexist),
        sized on the estimated physical (allocated) footprint rather than
        the plan's logical byte total.

        When ``ready_q`` is a byte-bounded :class:`~src.ready_queue.ReadyQueue`
        (the streaming path), staging pressure is engaged/cleared here from the
        SAME ``resident``/``need`` figures used for the capacity decision, so the
        writer can drain a finite group and free staging rather than deadlock the
        producer against an unmet minimum."""
        # peak while fetch + pack dirs coexist
        need  = 2 * self._physical_estimate(planned_bytes, planned_files)
        floor = LOCAL_STAGING_RESERVE_BYTES
        warned = False
        while not (CANCEL.is_set() or stop_evt.is_set()):
            with self._staged_lock:
                resident = self._staged_bytes
            # Local-only staging-pressure signal (no LTFS access). Engaged when
            # this chunk's footprint will not fit; cleared with hysteresis.
            self._apply_staging_pressure(ready_q, resident, need)
            try:
                free = shutil.disk_usage(self.staging_dir).free
            except OSError:
                free = need + floor
            governor = getattr(self, 'governor', None)
            if governor:
                if not warned:
                    _status('PIPELINE',
                            "Backpressure - waiting for RAM/staging/tape "
                            "governor before fetching the next chunk.")
                    warned = True
                if not governor.wait_or_pause(
                        "fetch", "start", needed_bytes=need,
                        queued_bytes=resident, stop_evt=stop_evt):
                    return
            room_cap  = (resident + need) <= self.staging_max_bytes
            room_disk = (free - need) >= floor
            if not room_disk:
                raise RuntimeError(
                    "Insufficient local staging space for remote chunk. "
                    f"Need {need / 1024**3:.2f} GiB peak staging + "
                    f"{floor / 1024**3:.0f} GiB reserve; current free on "
                    f"'{self.staging_dir}': {free / 1024**3:.2f} GiB."
                )
            alone     = (resident == 0)    # nothing else resident: may exceed cap
            if room_cap or alone:
                return
            if not warned:
                _status('PIPELINE',
                        f"Backpressure — {resident / 1024**3:.0f} GB staged, "
                        f"waiting for the tape to drain before fetching the next "
                        f"chunk (cap {self.staging_max_bytes / 1024**3:.0f} GB).")
                warned = True
            time.sleep(2)

    def _validate_ltfs_sync_mode(self):
        """Block tape writes unless the live mount declared time@5.

        Under ``sync_type=unmount`` LTFS writes its index only at unmount, so a
        forced restart loses every chunk written since the mount — that is what
        took chunks 18-91 (~126 GB) of session 37 on 2026-07-15, and no amount
        of stopping cleanly recovers it, because a clean pipeline stop does not
        unmount. Under time@5 the index is at most 5 minutes stale, which is
        what makes the stop-at-a-boundary strategy sound in the first place.

        Verified against the mount's own event-log declaration rather than the
        config file, because the two demonstrably drift: an MSI reinstall reset
        ``ltfs.conf.local`` on 2026-07-16 with nothing to announce it.
        """
        status = ltfs_sync_mode_status(expect_seconds=300)
        if status["ok"]:
            print(f"[TAPE] LTFS sync mode verified: "
                  f"{status['sync_type']}@{status['sync_seconds']}s "
                  f"(declared {status['declared_at']}).")
            get_logger().info("ltfs_sync_mode_ok: type=%s seconds=%s at=%s",
                              status["sync_type"], status["sync_seconds"],
                              status["declared_at"])
            return True

        if not status["determinate"]:
            # Could not read the declaration. Warn loudly but do not block: the
            # event log is not load-bearing for correctness, and refusing to run
            # because a log query failed would be its own outage.
            print(f"[TAPE] WARNING: could not verify the LTFS sync mode "
                  f"({status['error']}). Proceeding; if this host was recently "
                  f"reinstalled, confirm the mount is time@5 before trusting a "
                  f"forced-restart stop to be recoverable.")
            get_logger().warning("ltfs_sync_mode_indeterminate: %s",
                                 status["error"])
            return True

        declared = f"{status['sync_type']}@{status['sync_seconds']}s"
        msg = (f"LTFS mount declared sync mode {declared}, expected time@300s. "
               f"Refusing to start tape writes: under this mode a forced "
               f"restart can lose every chunk written since the mount.")
        print(f"\n[TAPE] {msg}")
        print("[TAPE] Fix the mount's sync_type and reload the cartridge, then "
              "re-run. See docs/performance_insights_and_recommendations.md.")
        get_logger().error("ltfs_sync_mode_blocked: declared=%s at=%s",
                           declared, status["declared_at"])
        send_best_effort(self.notifier, f"[PIPELINE] {msg}")
        return False

    # ------------------------------------------------------------------
    # Structured stop-result plumbing + the single pre-write safety gate
    # ------------------------------------------------------------------

    def _record_stop(self, result: StopResult, escalate=False) -> StopResult:
        """Record the reason a stop was decided; the most specific one wins.

        Called at the same point a component decides to stop, so the reason is
        the specific one that component knows. A later, generic reason (only
        ``stopped_at_chunk_boundary``) never overwrites a specific one already
        recorded — that is how an SCCM/network stop keeps its precise reason even
        though the bare ``stop_pipeline`` flag is what the writer loop observes.

        ``escalate=True`` is the thread-safe SAFETY escalation: a physical
        tape ambiguity (a write that failed after it started) must win over ANY
        previously recorded reason — including a user stop — because the
        ambiguous chunk needs human reconciliation regardless of why the run
        ended. Returns the winning (recorded) result.
        """
        with self._stop_lock:
            existing = self._stop_result
            if (existing is None or escalate
                    or (existing.is_generic and not result.is_generic)):
                self._stop_result = result
            return self._stop_result

    def _get_recorded_stop(self):
        """Thread-safe read of the recorded stop result, or None."""
        with self._stop_lock:
            return self._stop_result

    def _write_status_snapshot(self, **fields):
        """Best-effort status.json update. Never raises, never changes flow."""
        try:
            log_dir = getattr(self.cfg, "backup_log_dir", None)
            write_status(log_dir, **fields)
        except Exception:
            get_logger().warning("status snapshot failed (ignored)",
                                 exc_info=True)

    def _finalize(self, result: StopResult, phase="pipeline") -> StopResult:
        """Record the terminal result and persist the status/last-failure files.

        The file writes are best-effort: a failure to write must not change the
        exit code we return, must not raise, and must not hide the stop reason.
        """
        final = self._record_stop(result)
        try:
            log_dir = getattr(self.cfg, "backup_log_dir", None)
            if final.exit_code == ExitCode.COMPLETED:
                write_status(log_dir, session_id=final.session_id, phase="done",
                             exit_code=int(final.exit_code), reason=final.reason,
                             resumable=final.resumable)
            else:
                write_last_failure(log_dir, final, phase=phase)
                write_status(
                    log_dir, session_id=final.session_id,
                    chunk_id=final.chunk_index, phase=phase,
                    error_classification=final.error_classification,
                    error_message=final.detailed_reason,
                    resumable=final.resumable, exit_code=int(final.exit_code),
                    reason=final.reason, detailed_reason=final.detailed_reason)
        except Exception:
            get_logger().warning("finalize status write failed (ignored)",
                                 exc_info=True)
        return final

    def _verify_current_mount_time5(self):
        """Gate check: verify the LIVE mount is time@5, bound to the running
        LTFS process. Returns a blocking StopResult, or None when it is safe.

        Unlike the lenient startup smoke-check ``_validate_ltfs_sync_mode`` (which
        proceeds when the event log cannot be read), the gate fails **closed**:
        an unverifiable current mount blocks the write, because approving a write
        on a stale time@5 line from a previous mount is exactly the risk this
        binding exists to remove. Read-only — never probes or remounts the drive.
        """
        status = ltfs_current_mount_status(expect_seconds=300)
        if status.get("ok"):
            get_logger().info(
                "ltfs_current_mount_ok: type=%s seconds=%s at=%s proc=%s",
                status.get("sync_type"), status.get("sync_seconds"),
                status.get("declared_at"), status.get("mount_started_at"))
            return None
        if status.get("determinate") and status.get("reason") == "not_time5":
            declared = f"{status.get('sync_type')}@{status.get('sync_seconds')}s"
            msg = (f"LTFS current mount declared {declared}, expected time@300s. "
                   "Refusing to start a tape write: under this mode a forced "
                   "restart can lose every chunk written since the mount.")
            print(f"\n[TAPE] {msg}")
            get_logger().error("ltfs_sync_mode_blocked: declared=%s", declared)
            send_best_effort(self.notifier, f"[PIPELINE] {msg}")
            return StopResult(
                exit_code=ExitCode.SAFETY_BLOCK,
                reason=REASON_LTFS_SYNC_MODE_NOT_TIME5, resumable=False,
                source="gate", detailed_reason=msg)
        # Indeterminate: the live mount could not be verified / bound. Fail closed.
        err = status.get("error") or "current LTFS mount could not be verified"
        msg = (f"Cannot verify the live LTFS mount is time@5 ({err}). Refusing to "
               "start a tape write: a write whose index sync cannot be trusted is "
               "not recoverable after a forced restart.")
        print(f"\n[TAPE] {msg}")
        get_logger().error("ltfs_mount_unverifiable: %s", err)
        send_best_effort(self.notifier, f"[PIPELINE] {msg}")
        return StopResult(
            exit_code=ExitCode.SAFETY_BLOCK,
            reason=REASON_LTFS_MOUNT_UNVERIFIABLE, resumable=False,
            source="gate", detailed_reason=msg)

    def _verify_ltfs_media_health(self, mount_started_at=None):
        """Gate check: refuse a write when the drive/medium is reporting faults.

        Returns a blocking StopResult, or None when it is safe. Read-only — it
        reads the LTFS event log and never touches the drive.

        The 2026-07-24 loss of Tape_02 was preceded by four days of LOCATE
        write-perm errors that LTFS masked and nobody read (roughly one per chunk
        cycle, 45 in total) before the servo failed mid-write and froze the
        cartridge permanently. Stopping on the *first* such event is the point:
        a paused run costs hours, a frozen cartridge costs a trip to the drive
        and a replacement.

        Like the mount-mode gate this fails **closed** — an unreadable LTFS log
        blocks the write, because "the drive might be dying and we cannot tell"
        is not a state in which to start writing to tape.
        """
        if mount_started_at is None:
            mount_started_at = (ltfs_current_mount_status()
                                or {}).get("mount_started_at")
        if not mount_started_at:
            # Without the mount's start time the query would sweep the whole log
            # and block forever on a previous cartridge's faults. Fail closed
            # rather than bound the evidence wrongly.
            msg = ("Cannot determine when the current LTFS mount started, so "
                   "drive-health evidence cannot be scoped to this cartridge. "
                   "Refusing to start a tape write.")
            print(f"\n[TAPE] {msg}")
            get_logger().error("ltfs_media_health_unscoped: no mount start time")
            send_best_effort(self.notifier, f"[PIPELINE] {msg}")
            return StopResult(
                exit_code=ExitCode.SAFETY_BLOCK,
                reason=REASON_LTFS_MEDIA_DEGRADED, resumable=False,
                source="gate", detailed_reason=msg)

        health = ltfs_media_health(since_iso=mount_started_at)

        if not health.get("determinate"):
            err = health.get("error") or "LTFS event log could not be read"
            msg = (f"Cannot verify LTFS drive/medium health ({err}). Refusing to "
                   "start a tape write: an unreadable drive log is exactly how "
                   "the 2026-07-24 cartridge freeze went unnoticed for four days.")
            print(f"\n[TAPE] {msg}")
            get_logger().error("ltfs_media_health_unverifiable: %s", err)
            send_best_effort(self.notifier, f"[PIPELINE] {msg}")
            return StopResult(
                exit_code=ExitCode.SAFETY_BLOCK,
                reason=REASON_LTFS_MEDIA_DEGRADED, resumable=False,
                source="gate", detailed_reason=msg)

        if health.get("ok"):
            get_logger().info("ltfs_media_health_ok: since=%s",
                              health.get("since"))
            return None

        fatal = health.get("fatal") or []
        degraded = health.get("degraded") or []
        worst = fatal or degraded
        kind = "FATAL" if fatal else "degradation"
        first, last = worst[-1], worst[0]   # newest-first from Get-WinEvent
        detail = (f"{len(fatal)} fatal + {len(degraded)} degradation event(s) "
                  f"since the mount started; earliest {first['at']} "
                  f"[{first['id']}] {first['meaning']}; latest {last['at']} "
                  f"[{last['id']}] {last['meaning']}")
        msg = (f"LTFS reports drive/medium {kind}. Refusing to start a tape "
               f"write: {detail}. Writing into a failing drive is what froze "
               "Tape_02 permanently. Investigate before resuming — see "
               "docs/incidents/010-20260724-ltfs-write-perm-readonly.md.")
        print(f"\n[TAPE] {msg}")
        for ev in worst[:5]:
            print(f"[TAPE]   {ev['at']}  [{ev['id']}]  {ev['message'][:140]}")
        get_logger().error("ltfs_media_degraded: fatal=%d degraded=%d detail=%s",
                           len(fatal), len(degraded), detail)
        # The drive/medium state this cartridge was verified under no longer
        # holds, so the cached readiness must not be reused. Invalidated before
        # returning the block, so no later readiness decision can see stale
        # state. This discards cached state only — the StopResult below, and the
        # stop policy it carries, are unchanged.
        note_device_state_change(
            f"ltfs_media_health fatal={len(fatal)} degraded={len(degraded)}")
        send_best_effort(self.notifier, f"[PIPELINE] {msg}")
        return StopResult(
            exit_code=ExitCode.SAFETY_BLOCK,
            reason=REASON_LTFS_MEDIA_DEGRADED, resumable=False,
            source="gate", detailed_reason=msg)

    @staticmethod
    def _reboot_reason_slug(reasons, sccm):
        """Map a pending-reboot block to (reason_slug). SCCM if the Configuration
        Manager client is the cause (or is unreadable), else the Windows markers."""
        sccm = sccm or {}
        if (sccm.get("reboot_pending") or sccm.get("hard_reboot_pending")
                or not sccm.get("determinate", True)):
            return REASON_SCCM_REBOOT_PENDING
        # Corroborate against the reason text in case the caller's sccm dict is
        # sparse (e.g. the fallback path returns None for sccm).
        if any("SCCM" in r or "Configuration Manager" in r for r in reasons):
            return REASON_SCCM_REBOOT_PENDING
        return REASON_WINDOWS_REBOOT_PENDING

    def _record_reboot_stop(self, reasons, sccm=None):
        """Record a StopResult for a staged restart detected by the sentinel.

        The sentinel sets ``stop_pipeline`` in the background; recording here (as
        its ``on_detect``) is what lets the pre-write gate return the specific
        ``sccm_reboot_pending`` / ``windows_reboot_pending`` reason instead of the
        generic ``stopped_at_chunk_boundary``."""
        slug = self._reboot_reason_slug(reasons, sccm)
        self._record_stop(StopResult(
            exit_code=ExitCode.TRANSIENT_RESUMABLE, reason=slug, resumable=True,
            source="reboot-sentinel", detailed_reason="; ".join(reasons)))

    def _chunk_backing_from_prior_run(self, session_id, chunk_index):
        """True if this chunk is in 'backing' at gate entry — i.e. a PRIOR run
        left it mid-write. The current run only moves a chunk to 'backing' inside
        ``_write_chunk`` AFTER this gate, so a 'backing' status now is never the
        current run's own doing.

        **Fails CLOSED** (Plan 1, Task 3.2). An unreadable status used to be
        treated as "clear", which is the wrong direction for this specific
        question: the cost of a false "clear" is re-writing a chunk that may
        already be on tape, onto media that cannot be corrected in place. The
        cost of a false "ambiguous" is a stop the operator resolves in a minute.
        """
        try:
            return chunk_index in self.db.get_chunks_with_status(
                session_id, 'backing')
        except Exception:
            get_logger().error(
                "could not read chunk status for the ambiguity guard; "
                "treating the chunk as AMBIGUOUS rather than clear",
                exc_info=True)
            return True

    def _detect_prior_backing_chunks(self, session_id):
        """Before any producer/fetch/pack thread starts, refuse to resume a
        session that has a chunk left in 'backing' by a prior run.

        Such a chunk may already be on tape (the crash happened after the
        physical write but before it was marked 'done'); re-fetching and
        re-writing it blindly would double-write. Detect and stop — no
        auto-reconcile, no status flip. Returns a StopResult to stop, else None.

        **Fails CLOSED** (Plan 1, Task 3.2). If the database cannot be read, the
        run stops instead of proceeding: "we could not check for an ambiguous
        chunk" is not a state in which to start writing to a cartridge.
        """
        try:
            backing = self.db.get_chunks_with_status(session_id, 'backing')
        except Exception as exc:
            msg = (f"session {session_id}: could not read chunk status to "
                   f"check for an ambiguous 'backing' chunk ({exc}). Refusing "
                   "to start: a chunk left mid-write by a prior run may "
                   "already be on tape, and this run cannot rule that out.")
            print(f"\n[REMOTE] {msg}")
            get_logger().exception("backing_scan_unreadable: %s", msg)
            send_best_effort(self.notifier, f"[PIPELINE] SAFETY STOP: {msg}")
            return StopResult(
                exit_code=ExitCode.SAFETY_BLOCK,
                reason=REASON_AMBIGUOUS_BACKING_CHUNK, resumable=False,
                source="resume-precheck", session_id=session_id,
                detailed_reason=msg)
        if not backing:
            return None
        ci = backing[0]
        msg = (f"session {session_id}: chunk(s) {[c + 1 for c in backing]} were "
               "left in 'backing' by a prior run — they may already be on tape. "
               "Refusing to resume blindly; a human must reconcile the tape/DB "
               "state before continuing.")
        print(f"\n[REMOTE] {msg}")
        get_logger().error("ambiguous_backing_chunk: %s", msg)
        send_best_effort(self.notifier, f"[PIPELINE] SAFETY STOP: {msg}")
        return StopResult(
            exit_code=ExitCode.SAFETY_BLOCK, reason=REASON_AMBIGUOUS_BACKING_CHUNK,
            resumable=False, source="resume-precheck", session_id=session_id,
            chunk_index=ci, detailed_reason=msg)

    def _record_fetch_failure_stop(self, session_id, chunk_index):
        """Record the stop reason for a staging failure, using the precise fetch
        classification captured by ``_note_fetch_failure`` when available.

        Permanent auth/host-key/config → FATAL_CONFIG (no blind retry). An
        exhausted transient (DNS/timeout/…) → TRANSIENT_RESUMABLE with the
        generic ``network_retry_exhausted`` reason and the precise
        ``error_classification``. Anything else stays a resumable generic stop.
        """
        info = getattr(self, "_last_fetch_failure", None) or {}
        kind = info.get("kind")
        detail = info.get("detail") or f"chunk {chunk_index + 1} could not be staged"
        if kind == "permanent":
            return self._record_stop(StopResult(
                exit_code=ExitCode.FATAL_CONFIG,
                reason=info.get("permanent_reason") or REASON_BAD_CONFIG,
                resumable=False, source="fetch", session_id=session_id,
                chunk_index=chunk_index, detailed_reason=detail))
        if kind == "transient":
            return self._record_stop(StopResult(
                exit_code=ExitCode.TRANSIENT_RESUMABLE,
                reason=REASON_NETWORK_RETRY_EXHAUSTED,
                error_classification=info.get("classification"),
                resumable=True, source="fetch", session_id=session_id,
                chunk_index=chunk_index, detailed_reason=detail))
        return self._record_stop(StopResult(
            exit_code=ExitCode.TRANSIENT_RESUMABLE,
            reason=REASON_STOPPED_AT_CHUNK_BOUNDARY, resumable=True,
            source="fetch", session_id=session_id, chunk_index=chunk_index,
            detailed_reason=detail))

    def _pre_write_safety_gate(self, session_id, desc, tape_label, stop_pipeline):
        """The single authority that permits or blocks the START of a tape write.

        Both writer loops reach the tape only through ``_write_chunk``, and call
        this gate immediately before it. The gate — and only the gate — checks
        the stop flags, the live mount, a staged restart, and the ambiguity
        guard, so no set flag can slip past into a new write and no two places
        can disagree. It never interrupts a write already in progress; it only
        blocks the start of the next one.

        Returns None to permit the write, or a StopResult (already recorded as
        the winning reason) to block it. On a block the caller preserves the
        staged pack for resume.
        """
        ci = desc.chunk_index

        # 1. An ALREADY-RECORDED stop wins, returned unchanged. This is the
        #    required precedence: a later CANCEL must never replace an earlier
        #    20/ltfs_mount_unverifiable or 10/network_retry_exhausted. Only when
        #    nothing is recorded do the flag checks below create a new reason.
        recorded = self._get_recorded_stop()
        if recorded is not None:
            return recorded

        # 2. No prior stop: an operator cancel is the first stop source.
        if CANCEL.is_set():
            return self._record_stop(StopResult(
                exit_code=ExitCode.USER_STOP, reason=REASON_USER_REQUESTED_STOP,
                resumable=True, source="gate", session_id=session_id,
                chunk_index=ci, detailed_reason="cancel requested before write"))

        # 3. stop_pipeline set with nothing recorded — only now is the generic
        #    boundary reason correct (no setter left a specific one).
        if stop_pipeline.is_set():
            return self._record_stop(StopResult(
                exit_code=ExitCode.TRANSIENT_RESUMABLE,
                reason=REASON_STOPPED_AT_CHUNK_BOUNDARY, resumable=True,
                source="gate", session_id=session_id, chunk_index=ci))

        # 4. The live mount must be time@5, bound to the running LTFS instance.
        mount_block = self._verify_current_mount_time5()
        if mount_block is not None:
            mount_block.session_id = session_id
            mount_block.chunk_index = ci
            return self._record_stop(mount_block)

        # 4b. The drive/medium must not be reporting faults on this mount.
        media_block = self._verify_ltfs_media_health()
        if media_block is not None:
            media_block.session_id = session_id
            media_block.chunk_index = ci
            return self._record_stop(media_block)

        # 4c. The cartridge in the drive must be the one this session writes to.
        cartridge_block = self._verify_mounted_cartridge(tape_label)
        if cartridge_block is not None:
            cartridge_block.session_id = session_id
            cartridge_block.chunk_index = ci
            return self._record_stop(cartridge_block)

        # 5. SCCM + Windows pending reboot (the synchronous, current re-check).
        reasons, sccm = self._pre_tape_write_reboot_check(
            session_id, desc, tape_label)
        if reasons:
            return self._record_stop(StopResult(
                exit_code=ExitCode.TRANSIENT_RESUMABLE,
                reason=self._reboot_reason_slug(reasons, sccm), resumable=True,
                source="gate", session_id=session_id, chunk_index=ci,
                detailed_reason="; ".join(reasons)))

        # 6. Ambiguity guard: a 'backing' status now is a prior run's, never ours.
        if self._chunk_backing_from_prior_run(session_id, ci):
            msg = (f"chunk {ci + 1} is in 'backing' from a prior run — it may "
                   "already be on tape; refusing to re-write it blindly.")
            print(f"\n[REMOTE] {msg}")
            get_logger().error("ambiguous_backing_chunk: %s", msg)
            return self._record_stop(StopResult(
                exit_code=ExitCode.SAFETY_BLOCK,
                reason=REASON_AMBIGUOUS_BACKING_CHUNK, resumable=False,
                source="gate", session_id=session_id, chunk_index=ci,
                detailed_reason=msg))

        # 7. All clear — permit the write. (Fit-to-tape stays inside
        #    _write_chunk, which marks the chunk backup_failed on a miss.)
        return None

    def _verify_session_tape_generation(self, session_row):
        """Block an old session from silently crossing a physical format.

        **PostgreSQL only** (Plan 1 Task 1.4): it compares the session's
        persisted generation with the catalog's and needs no tape, no mount and
        no ownership. It never infers or advances a generation from what is
        loaded in the drive — the mounted volume is checked separately, once per
        finite write group, under ownership.

        Fails closed on a missing, null or non-active generation. Before Task
        1.4, ``register_tape`` could leave ``tapes.current_generation`` set with
        no matching ``tape_generations`` row; resuming against that state means
        resuming against a cartridge history nobody can reconstruct.
        """
        # Unit-test/legacy injected rows may predate migration 013.  A real
        # manager applies 013 at startup and makes this column NOT NULL, so the
        # absence is not a production bypass.
        if 'tape_generation' not in session_row:
            return None
        tape_label = session_row.get('tape_label')
        tape = self.db.get_tape(tape_label)
        session_generation = session_row.get('tape_generation')
        current_generation = tape.get('current_generation') if tape else None
        if session_generation is None or current_generation is None:
            msg = (
                f"Session {session_row.get('session_id')} targets "
                f"{tape_label!r}, but its tape generation is not established "
                f"(session={session_generation}, catalog={current_generation}). "
                "Refusing to resume: without a generation there is no way to "
                "tell whether the cartridge has been reformatted since this "
                "session planned its chunks. Re-register the tape (which "
                "creates its active generation row) and review the session.")
            print(f"\n[TAPE] {msg}")
            get_logger().error("session_tape_generation_missing: session=%s "
                               "tape=%s session_generation=%s catalog=%s",
                               session_row.get('session_id'), tape_label,
                               session_generation, current_generation)
            return StopResult(
                exit_code=ExitCode.SAFETY_BLOCK,
                reason=REASON_UNEXPECTED_TAPE_OR_DB_STATE, resumable=False,
                source="gate", session_id=session_row.get('session_id'),
                detailed_reason=msg)
        if session_generation == current_generation:
            active_block = self._verify_generation_is_active(
                session_row, tape, current_generation)
            if active_block is not None:
                return active_block
            return None
        msg = (
            f"Session {session_row.get('session_id')} targets {tape_label!r} "
            f"generation {session_generation}, but the registered cartridge "
            f"is generation {current_generation}. Refusing to resume an old "
            "session onto newly formatted media. An operator must explicitly "
            "re-point the session generation after reviewing its pending chunks.")
        print(f"\n[TAPE] {msg}")
        get_logger().error(
            "session_tape_generation_mismatch: session=%s tape=%s "
            "session_generation=%s current_generation=%s",
            session_row.get('session_id'), tape_label, session_generation,
            current_generation)
        return StopResult(
            exit_code=ExitCode.SAFETY_BLOCK,
            reason=REASON_UNEXPECTED_TAPE_OR_DB_STATE, resumable=False,
            source="gate", session_id=session_row.get('session_id'),
            detailed_reason=msg)

    def _announce_target_cartridge(self, session_id, tape_label):
        """Say which cartridge this run targets, WITHOUT reading the drive.

        Task 1.4 moved the mounted-cartridge check into the finite write group,
        which is correct for the drive (no access while idle) but costs the
        operator something real: before, a wrong cartridge was caught at
        startup; now it is caught at the first write, after a whole fetch+pack
        cycle. The staged pack is preserved either way, so nothing is lost but
        time — yet incident 011 is precisely about a wrong cartridge that
        *announced nothing*, so silence for ~40 minutes is the wrong default.

        This is the honest middle: state the expectation loudly and up front,
        and say exactly when it will be enforced. It reads no device, holds no
        ownership and touches no mount — it prints what the database already
        says.
        """
        message = (
            f"[TAPE] This run writes to cartridge '{tape_label}' "
            f"(session {session_id}).\n"
            "       The cartridge physically in the drive is NOT checked yet: "
            "that happens once,\n"
            "       under LTFS ownership, immediately before the first tape "
            "write — so the drive\n"
            "       stays untouched while the first chunk is fetched and "
            "packed.\n"
            "       If the wrong cartridge is loaded the write is REFUSED "
            "(nothing is written and\n"
            "       nothing is mis-cataloged), but you will not find out until "
            "then. If you are not\n"
            f"       sure '{tape_label}' is loaded, check now rather than "
            "after the first fetch.")
        print(f"\n{message}")
        get_logger().info("target_cartridge_announced: session=%s tape=%s",
                          session_id, tape_label)
        return message

    def _verify_generation_is_active(self, session_row, tape, generation):
        """Refuse a generation that the catalog does not record as active.

        Read-only and PostgreSQL-only. A database that predates migration 013
        (no ``tape_generations`` reader) is not treated as a failure — the
        column-presence check above already established there is nothing to
        compare — but a reader that answers "retired" or "no such row" is.
        """
        reader = getattr(self.db, 'get_active_tape_generation', None)
        if reader is None:
            return None
        try:
            active = reader(session_row.get('tape_label'))
        except Exception:
            get_logger().warning(
                "could not read the active tape generation; treating the "
                "state as unusable", exc_info=True)
            active = None
            indeterminate = True
        else:
            indeterminate = False
        if active is not None and int(active) == int(generation):
            return None
        msg = (
            f"Session {session_row.get('session_id')} targets "
            f"{session_row.get('tape_label')!r} generation {generation}, but "
            + ("that generation could not be read from the catalog."
               if indeterminate else
               f"the catalog's ACTIVE generation is {active!r}.")
            + " Refusing to resume: a non-active generation means the "
              "cartridge was reformatted or retired since this session planned "
              "its chunks, and its catalog rows point at media that no longer "
              "holds them.")
        print(f"\n[TAPE] {msg}")
        get_logger().error("session_tape_generation_not_active: session=%s "
                           "generation=%s active=%s",
                           session_row.get('session_id'), generation, active)
        return StopResult(
            exit_code=ExitCode.SAFETY_BLOCK,
            reason=REASON_UNEXPECTED_TAPE_OR_DB_STATE, resumable=False,
            source="gate", session_id=session_row.get('session_id'),
            detailed_reason=msg)

    def _verify_mounted_cartridge(self, tape_label):
        """Gate check: the mounted cartridge must be the session's tape.

        Returns a blocking StopResult, or None when it is safe. Read-only — the
        label comes from the already-mounted volume, never from the drive.

        A resumed session takes its ``tape_label`` from the session row, and
        until 2026-07-26 nothing compared that to the cartridge physically in
        the drive. After the Tape_02 freeze the operator loaded Tape_03 and
        resumed; the pipeline would have written the remaining chunks to
        Tape_03 while cataloging every one of them under ``Tape_02``. Nothing
        would have failed, and the catalog would have pointed a future restore
        at the wrong cartridge — the read-only one that cannot be rewritten.

        Fails **closed**: an unreadable label blocks the write, because "we
        cannot tell which cartridge this is" is not a state in which to commit
        files to a catalog keyed by cartridge.
        """
        try:
            mounted = get_volume_label(self.cfg.lto_drive)
        except Exception as e:
            msg = (f"Cannot read the mounted volume label ({e}). Refusing to "
                   "start a tape write: the catalog is keyed by cartridge and "
                   "must not record a guess.")
            print(f"\n[TAPE] {msg}")
            get_logger().exception("mounted_cartridge_unreadable")
            send_best_effort(self.notifier, f"[PIPELINE] {msg}")
            return StopResult(
                exit_code=ExitCode.SAFETY_BLOCK,
                reason=REASON_UNEXPECTED_TAPE_OR_DB_STATE, resumable=False,
                source="gate", detailed_reason=msg)

        if not mounted:
            msg = ("The mounted volume reports no label. Refusing to start a "
                   f"tape write for session tape '{tape_label}'.")
            print(f"\n[TAPE] {msg}")
            get_logger().error("mounted_cartridge_unlabelled: expected=%s",
                               tape_label)
            send_best_effort(self.notifier, f"[PIPELINE] {msg}")
            return StopResult(
                exit_code=ExitCode.SAFETY_BLOCK,
                reason=REASON_UNEXPECTED_TAPE_OR_DB_STATE, resumable=False,
                source="gate", detailed_reason=msg)

        if mounted != tape_label:
            msg = (f"The mounted cartridge is '{mounted}' but this session "
                   f"writes to '{tape_label}'. Refusing to write: the chunks "
                   f"would land on '{mounted}' and be cataloged under "
                   f"'{tape_label}', sending a future restore to the wrong "
                   "cartridge. Load the right tape, or re-point the session.")
            print(f"\n[TAPE] {msg}")
            get_logger().error(
                "mounted_cartridge_mismatch: mounted=%s session=%s",
                mounted, tape_label)
            send_best_effort(self.notifier, f"[PIPELINE] SAFETY STOP: {msg}")
            return StopResult(
                exit_code=ExitCode.SAFETY_BLOCK,
                reason=REASON_UNEXPECTED_TAPE_OR_DB_STATE, resumable=False,
                source="gate", detailed_reason=msg)

        get_logger().info("mounted_cartridge_ok: %s", mounted)
        return None

    def _block_on_soft_reboot_marker(self):
        """Deprecated: severity now decides what blocks, not this flag.

        ``PendingFileRenameOperations`` is classified warning-only in
        ``windows_update_guard``, so it can never stop the pipeline on its own
        regardless of what this returns. Kept because
        ``[WINDOWS_UPDATE] block_on_pending_reboot`` still means "let a real,
        critical pending restart block a write", and an absent key must fail
        safe (block).
        """
        return bool(getattr(
            self.cfg, "windows_update_block_on_pending_reboot", True))

    def _pre_tape_write_reboot_check(self, session_id, desc, tape_label):
        """Refuse a new tape write while a restart is staged. Returns reasons.

        Called synchronously on the writer thread immediately before each write,
        deliberately duplicating the sentinel's background poll. The sentinel
        answers "has a restart appeared in the last 60s"; this answers "is it
        safe to start a write *right now*", and on 2026-07-15 the gap between
        those two questions was the whole failure — SCCM announced the restart
        60 seconds before taking it.
        """
        log = get_logger()
        log.info("pre_tape_write_reboot_check: session=%s chunk=%s tape=%s "
                 "staging=%s", session_id, desc.chunk_index + 1, tape_label,
                 desc.pack_dir)
        try:
            assessment = assess_reboot_state(block_on_unknown=True)
        except Exception:
            # The gate itself must never take the pipeline down. Fall back to
            # the critical Windows markers alone rather than blocking forever.
            log.exception("pre_tape_write_reboot_check failed; "
                          "falling back to Windows markers")
            return list(pending_reboot_reasons(include_soft=False)), None

        sccm = assessment.sccm
        # Warning-only indicators are never discarded: they are recorded here
        # and on the console so a stale rename queue stays visible in
        # diagnostics without stopping a tape write.
        if assessment.warnings:
            summary = assessment.warning_summary()
            log.info("pre_tape_write_reboot_warning: session=%s chunk=%s %s",
                     session_id, desc.chunk_index + 1, summary)
            print(f"[WU] {summary}")

        reasons = assessment.blocking_reasons
        if reasons and not self._block_on_soft_reboot_marker():
            # Explicit operator override of a real, critical pending restart.
            log.warning(
                "tape_write_reboot_block_overridden: session=%s chunk=%s "
                "reasons=%s — [WINDOWS_UPDATE] block_on_pending_reboot=false",
                session_id, desc.chunk_index + 1, "; ".join(reasons))
            print("[WU] block_on_pending_reboot = false — proceeding despite: "
                  + "; ".join(reasons))
            return [], sccm

        if reasons:
            detail = "; ".join(reasons)
            log.warning(
                "tape_write_blocked_by_reboot: session=%s chunk=%s tape=%s "
                "staging=%s sccm=%s reasons=%s",
                session_id, desc.chunk_index + 1, tape_label, desc.pack_dir,
                sccm, detail)
            print(f"\n[WU] Not starting the tape write for chunk "
                  f"{desc.chunk_index + 1}: {detail}")
            print("[WU] The pack is kept in staging. Let the host restart, then "
                  "re-run option 6 — it resumes from this pack without "
                  "re-fetching.")
            send_best_effort(
                self.notifier,
                f"[PIPELINE] Tape write for chunk {desc.chunk_index + 1} "
                f"blocked: {detail}. Pack kept in staging; stopping cleanly.")
        return reasons, sccm

    # ------------------------------------------------------------------
    # Consumer: write a staged chunk to tape  (runs on the main thread)
    # ------------------------------------------------------------------

    def _write_chunk(self, session_id, desc: StagedChunk, tape_label,
                     eject_after, stop_pipeline):
        """Authorize and (if permitted) write one chunk to tape.

        Returns None on success. Returns a :class:`StopResult` when the write did
        not complete — a gate block (``preserve_pack=True``: the write never
        started, keep the pack for a direct resume) or a mid-attempt failure
        (``preserve_pack=False``: re-fetchable). This is the single boundary
        where a tape write is authorized and started.

        The authorization (recorded-stop check → safety gate → final
        recorded-stop check → fits-tape → set 'backing' → launch the external
        writer) all runs under ``_TAPE_IO_LOCK``, so no stop can slip in between
        approving the write and starting it, and no other in-process tape op can
        interleave. ``_TAPE_IO_LOCK`` is reentrant and this runs on the consumer
        thread, so ``LTOBackup.run``'s own acquire nests correctly. The lock is
        released *before* the 'done' commit, staging flush, and cleanup — it is
        never held during status writes, fetch/pack, thread joins, or retries.
        """
        return self._write_chunk_group(
            session_id, [desc], tape_label, eject_after, stop_pipeline)

    def _start_pipeline_heartbeat(self, stop_evt, ready_q, total_chunks):
        """Print a periodic line showing the producer staying ahead of the tape.

        Every telegram_heartbeat_minutes it also sends an all-is-well Telegram
        message with the same pipeline state, so a long unattended run that
        stops making progress is noticed by silence-plus-alerts rather than by
        checking the console."""
        hb_secs = self.heartbeat_secs

        def _beat():
            last_msg = None
            last_print = 0
            quiet_interval = 30
            last_hb = time.time()
            while not stop_evt.wait(5):
                with self._staged_lock:
                    staged_gb = self._staged_bytes / 1024**3
                prod_c = ('-' if self._producer_chunk is None
                          else self._producer_chunk + 1)
                cons_c = ('-' if self._consumer_chunk is None
                          else self._consumer_chunk + 1)
                # ReadyQueue (Phase 4) reports bytes as well as count; the old
                # queue.Queue only had qsize(), which is why "queued=0/1" hid a
                # 99.6%-empty pipeline for weeks.
                if hasattr(ready_q, 'metrics'):
                    m = ready_q.metrics()
                    ready_desc = (
                        f"ready={m['ready_chunks']}ch/"
                        f"{m['ready_bytes'] / 1024**3:.1f}GiB "
                        f"(start@{m['ready_target_bytes'] / 1024**3:.0f}"
                        f"/max{m['ready_max_bytes'] / 1024**3:.0f}GiB) | "
                        f"writing={m['writing_chunks']}ch | "
                        f"groups={m['groups_started']} "
                        f"own={self._ownership_acquisitions} "
                        f"rdy={self._readiness_checks}"
                    )
                else:                                   # pragma: no cover
                    ready_desc = f"queued={ready_q.qsize()}/{self.prefetch_ahead} | "
                msg = (
                    f"{ready_desc} | "
                    f"staging={staged_gb:.0f}/"
                    f"{self.staging_max_bytes / 1024**3:.0f} GB | "
                    f"producer chunk {prod_c}/{total_chunks} | "
                    f"tape chunk {cons_c}/{total_chunks}"
                )
                now = time.time()
                if msg != last_msg or (now - last_print) >= quiet_interval:
                    _status('PIPELINE', msg)
                    last_msg = msg
                    last_print = now
                if hb_secs and (now - last_hb) >= hb_secs:
                    last_hb = now
                    try:
                        free_gb = (shutil.disk_usage(self.staging_dir).free
                                   / 1024**3)
                        free_txt = f" | staging free {free_gb:.0f} GB"
                    except OSError:
                        free_txt = ""
                    send_best_effort(
                        self.notifier,
                        f"[PIPELINE] heartbeat — running: {msg}{free_txt}")
        threading.Thread(target=_beat, name='pipeline-heartbeat',
                         daemon=True).start()

    # ------------------------------------------------------------------
    # Fetch helpers
    # ------------------------------------------------------------------

