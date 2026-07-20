"""RemoteOrchestrator: streaming remote-host -> staging -> tape pipeline."""
import gc
import json
import os
import time
import queue
import shutil
import threading
import posixpath
from datetime import datetime
from collections import defaultdict
from typing import Optional

from .backup import LTOBackup, _NoEjectBackup
from .constants import (DEFAULT_TAPE_CAPACITY_GB, LOCAL_STAGING_RESERVE_BYTES,
                        LTFS_WRITE_WARNING, tape_budget_bytes)
from .db import _apply_canonical_remote_paths
from .logsetup import get_logger
from .windows_update_guard import (RebootSentinel, ltfs_sync_mode_status,
                                   pending_reboot_reasons,
                                   reboot_block_reasons)
from .ltfs import _ensure_lto_drive_ready, get_volume_label
from .packer import LTOPacker
from .paths import (_LEGACY_PATH_LIMIT, _dir_tree_size,
                    _disambiguate_local_rel, _exceeds_legacy_path_limit,
                    _long, _remote_fetch_base_and_rel,
                    _reserved_name_component, _volume_cluster_size,
                    _winsafe_extracted_rel)
from .pipeline_types import StagedChunk, StreamState
from .planning import StreamingChunkBuilder
from .ram_telemetry import RamStageSampler
from .remote_transport import _remote_tar_fetch
from .resource_governor import ResourceGovernor
from .reporting import _write_source_missing_only_log
from .runtime import (CANCEL, _fmt_eta, _phase, _priority_class,
                      _progress_done, _progress_line, _status,
                      compute_affinity_sets, pin_current_process,
                      unpin_current_process)
from .scanning import StreamingRemoteScanner
from .skipped import SkippedFileTracker
from .telegram_notify import TelegramNotifier, send_best_effort
from .ui import ConsoleUI

# A fetch this far past its planned bytes gets one loud alert; the hard
# abort threshold is the configurable fetch_overrun_abort_factor.
_FETCH_OVERRUN_WARN_FACTOR = 1.10

# Written inside a pack dir when a stop preserves it, and the only thing that
# makes that pack reusable. Written last and atomically, so its presence means
# the pack is complete — a pack interrupted mid-write simply has no marker.
_RESUME_MARKER = "_resume_pack.json"

# Substrings that mark a fetch failure as transient — a network/DNS hiccup worth
# a retry, not a reason to abandon the chunk and stop the run. Drawn from the
# 2026-07-17 incident (a momentary "Could not resolve hostname so01" killed the
# session) and the usual ssh/tar transport failures. Matched case-insensitively.
_TRANSIENT_FETCH_SIGNATURES = (
    "could not resolve hostname",
    "name or service not known",
    "temporary failure in name resolution",
    "getaddrinfo",
    "exit 255",                 # ssh's catch-all for a connection it never made
    "connection reset",
    "connection timed out",
    "connection refused",
    "connection closed",
    "broken pipe",
    "network is unreachable",
    "no route to host",
    "kex_exchange_identification",
    "banner exchange",
)


def _is_transient_fetch_error(err):
    """True if a fetch error looks like a retryable network/DNS blip.

    Deliberately conservative: an error that is not clearly transient is treated
    as fatal, because retrying a genuine problem (missing file, permission,
    corrupt source) just wastes time. A false negative costs a re-fetch on
    resume; a false positive would spin on an unrecoverable error."""
    if not err:
        return False
    low = str(err).lower()
    return any(sig in low for sig in _TRANSIENT_FETCH_SIGNATURES)


class RemoteOrchestrator:
    """Orchestrates archiving files from a remote Linux host to LTO tape.

    Pipeline per chunk:
      1. SSH find  → file manifest (paths + sizes)
      2. Greedy bin-pack into staging-sized chunks
      3. Per chunk: SCP fetch → LTOPacker.run() → LTOBackup.run() → flush staging

    Sessions are persisted in remote_sessions / remote_manifest so an
    interrupted run can be resumed from the last completed chunk.
    """

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
        self.staging_padding   = cfg.staging_padding_factor
        self.fetch_abort_factor = cfg.fetch_overrun_abort_factor
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
        self.governor = ResourceGovernor(cfg, self.staging_dir)

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
    # Public entry point
    # ------------------------------------------------------------------

    def run(self):
        try:
            self._validate_config()

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
                    self._run_session(existing['session_id'])
                    return
                elif choice == '2':
                    print("[REMOTE] Starting a fresh-session scan. The current session "
                          "will remain resumable until the replacement is approved.")
                    self._start_new_session(replacing_session=existing)
                    return
                else:
                    return

            self._start_new_session()
        finally:
            self.skipped_tracker.print_summary(self.ui, self.cfg.backup_log_dir)

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

        tape_label = self._resolve_tape_label()
        if not tape_label:
            return
        if not _ensure_lto_drive_ready(self.cfg.lto_drive):
            return

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
            return

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

        self._run_streaming_session(session_id)

    def _resolve_tape_label(self):
        detected = get_volume_label(self.cfg.lto_drive)
        if detected:
            print(f"[TAPE] Detected label: {detected}")
            return detected
        print("[TAPE] Could not auto-detect tape label.")
        label = input("Enter tape Volume Label manually (or Enter to cancel): ").strip()
        return label if label else None

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
        used_bytes = self.db.recalculate_tape_used_space(tape_label)
        reserved_bytes = 0
        if session_id is not None and hasattr(
                self.db, 'get_pending_remote_reserved_bytes'):
            reserved_bytes = self.db.get_pending_remote_reserved_bytes(session_id)
        capacity_bytes, available_bytes = tape_budget_bytes(
            tape['total_capacity'], used_bytes, reserved_bytes)
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

    def _run_streaming_session(self, session_id):
        """Scan, persist, stage, and write chunks as one continuous pipeline."""
        session_row = self.db.get_remote_session(session_id)
        tape_label = session_row['tape_label']
        if not _ensure_lto_drive_ready(self.cfg.lto_drive):
            return

        self._staged_bytes = 0
        self._producer_err = None
        self._producer_chunk = None
        self._consumer_chunk = None

        if self.fetch_cores:
            pin_current_process(self.fetch_cores, label='fetch/pack')

        _phase('PIPELINE', "Continuous remote stream to tape | "
                           f"prefetch {self.prefetch_ahead} ahead | "
                           f"staging cap {self.staging_max_bytes / 1024**3:.0f} GB")
        print(f"[WARNING] {LTFS_WRITE_WARNING}")

        if not self._validate_ltfs_sync_mode():
            return False

        chunk_q = queue.Queue(maxsize=max(1, self.prefetch_ahead * 2))
        ready_q = queue.Queue(maxsize=self.prefetch_ahead)
        stop_pipeline = threading.Event()
        hb_stop = threading.Event()
        SENTINEL = object()

        # Same forced-update protection as the resume path: stop at a chunk
        # boundary while LTFS can still sync, rather than be killed mid-write.
        reboot_sentinel = RebootSentinel(
            stop_pipeline,
            on_detect=lambda reasons: send_best_effort(
                self.notifier,
                "[PIPELINE] Windows staged a restart — stopping at the next "
                "chunk boundary so the tape index is synced. Re-run option 6 "
                "to resume after the host restarts.")).start()

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

        def _queue_put(q, item):
            while not (CANCEL.is_set() or stop_pipeline.is_set()):
                try:
                    q.put(item, timeout=1)
                    return True
                except queue.Full:
                    continue
            return False

        def _force_put(q, item):
            while True:
                try:
                    q.put(item, timeout=1)
                    return
                except queue.Full:
                    if CANCEL.is_set():
                        return

        def _chunk_rows(chunk_index, chunk_files):
            return [
                (chunk_index, remote_fpath, os.path.basename(remote_fpath), fsize)
                for remote_fpath, fsize in chunk_files
            ]

        def _append_chunk(chunk_files):
            if hasattr(self.db, 'get_remote_existing_snapshot_paths'):
                existing = self.db.get_remote_existing_snapshot_paths(
                    session_id, [remote_fpath for remote_fpath, _ in chunk_files])
                chunk_files = [
                    (remote_fpath, fsize)
                    for remote_fpath, fsize in chunk_files
                    if remote_fpath.replace('\\', '/') not in existing
                ]
                if not chunk_files:
                    return True
            logical_bytes = sum(fsize for _, fsize in chunk_files)
            with remaining_lock:
                if logical_bytes > state.remaining_bytes:
                    msg = (
                        f"next remote chunk needs {logical_bytes / 1024**3:.2f} GiB, "
                        f"but only {state.remaining_bytes / 1024**3:.2f} GiB "
                        "remains on the mounted tape under the DB safety budget"
                    )
                    state.scan_error = msg
                    self.db.mark_remote_scan_error(session_id, msg)
                    print(f"[TAPE] {msg}. Stopping before overfill.")
                    stop_pipeline.set()
                    return False

            chunk_index = state.next_chunk_index
            result = self.db.append_remote_streaming_chunk(
                session_id, chunk_index, _chunk_rows(chunk_index, chunk_files))
            inserted_files = int(result.get('inserted_files', 0))
            inserted_bytes = int(result.get('inserted_bytes', 0))
            if inserted_files == 0:
                return True

            with remaining_lock:
                state.remaining_bytes = max(
                    0, state.remaining_bytes - inserted_bytes)
            state.next_chunk_index += 1
            state.chunks += 1
            state.files += inserted_files
            state.bytes += inserted_bytes
            _status('SCAN', f"Chunk {chunk_index + 1} planned: "
                            f"{inserted_files:,} file(s), "
                            f"{inserted_bytes / 1024**3:.2f} GiB")
            return _queue_put(chunk_q, chunk_index)

        def _scanner_planner():
            try:
                for ci in self.db.get_pending_chunks(session_id):
                    if not _queue_put(chunk_q, ci):
                        return

                budget = self._chunk_budget()
                builder = StreamingChunkBuilder(
                    budget,
                    alloc_unit=_volume_cluster_size(self.staging_dir),
                    padding_factor=self.staging_padding,
                    max_files=self.chunk_max_files,
                )
                scanner = StreamingRemoteScanner(
                    self.remote_user,
                    self.remote_host,
                    remote_password=self.remote_password,
                    skipped_tracker=self.skipped_tracker,
                    ui=self.ui,
                    cipher=self.ssh_cipher,
                )
                for remote_fpath, fsize in scanner.iter_scan(
                        self.remote_scan_paths, stop_evt=stop_pipeline):
                    if CANCEL.is_set() or stop_pipeline.is_set():
                        return
                    for chunk in builder.add(remote_fpath, fsize):
                        if not _append_chunk(chunk):
                            return
                for chunk in builder.flush():
                    if not _append_chunk(chunk):
                        return
                if not (CANCEL.is_set() or stop_pipeline.is_set()):
                    self.db.mark_remote_scan_complete(session_id)
                    _status('SCAN', f"Complete: {state.chunks:,} new "
                                    f"chunk(s), {state.files:,} file(s), "
                                    f"{state.bytes / 1024**3:.2f} GiB")
            except Exception as e:
                get_logger().exception("streaming scanner failed")
                state.scan_error = str(e)
                self.db.mark_remote_scan_error(session_id, str(e))
                self._producer_err = str(e)
                stop_pipeline.set()
            finally:
                _force_put(chunk_q, SENTINEL)

        def _stager():
            try:
                while not (CANCEL.is_set() or stop_pipeline.is_set()):
                    try:
                        # A bounded wait keeps this thread responsive to a
                        # cancellation that raced the scanner's sentinel put
                        # (a bare get() would block forever if the sentinel
                        # was dropped on a full queue during shutdown).
                        item = chunk_q.get(timeout=1)
                    except queue.Empty:
                        continue
                    if item is SENTINEL:
                        break
                    ci = item
                    summary = self.db.get_chunk_size_summary(
                        session_id, ci).get(ci, (0, 0, 0))
                    planned_bytes, _, planned_files = summary
                    self._validate_chunk_file_limit(
                        session_id, ci, planned_files)
                    self._await_staging_capacity(
                        planned_bytes, planned_files, stop_pipeline)
                    if CANCEL.is_set() or stop_pipeline.is_set():
                        break
                    desc = self._stage_chunk(
                        session_id, ci, self.db.get_chunk_files(session_id, ci))
                    if desc is None:
                        if not CANCEL.is_set():
                            self._producer_err = (
                                f"chunk {ci + 1} could not be staged")
                        stop_pipeline.set()
                        break
                    if not _queue_put(ready_q, desc):
                        self._discard_desc(desc)
                        break
            except Exception as e:
                get_logger().exception("chunk stager failed")
                self._producer_err = str(e)
                stop_pipeline.set()
            finally:
                _force_put(ready_q, SENTINEL)

        scanner_thread = threading.Thread(
            target=_scanner_planner, name='streaming-scanner', daemon=True)
        stager_thread = threading.Thread(
            target=_stager, name='streaming-stager', daemon=True)
        scanner_thread.start()
        stager_thread.start()
        self._start_pipeline_heartbeat(hb_stop, ready_q, "streaming")

        completed = 0
        failed = False
        blocked_by_reboot = False
        try:
            while True:
                desc = ready_q.get()
                if desc is SENTINEL:
                    break
                if CANCEL.is_set():
                    # Keep the pack. It cost a full fetch+pack cycle and it is
                    # exactly what a resume wants; deleting it here is why an
                    # interrupted run used to re-fetch work it already had.
                    self._preserve_desc(session_id, desc, "cancelled before write")
                    break
                # A stop asked for mid-write must end the pipeline at THIS
                # boundary, not after the queue drains. The sentinel sets
                # stop_pipeline precisely so the next write never starts.
                if stop_pipeline.is_set():
                    self._preserve_desc(session_id, desc, "stop requested before write")
                    break
                # Synchronous, current re-check. The sentinel's poll can be up
                # to 60s stale, and on 2026-07-15 SCCM gave 60s of warning —
                # a stale answer is not good enough at this boundary.
                reasons, sccm = self._pre_tape_write_reboot_check(
                    session_id, desc, tape_label)
                if reasons:
                    self._preserve_desc(session_id, desc, "blocked by pending restart")
                    blocked_by_reboot = True
                    stop_pipeline.set()
                    break
                if not self._write_chunk(
                        session_id, desc, tape_label, eject_after=False):
                    if not CANCEL.is_set():
                        self._discard_desc(desc)
                    failed = True
                    stop_pipeline.set()
                    break
                completed += 1
        finally:
            stop_pipeline.set()
            hb_stop.set()
            reboot_sentinel.stop()
            # Anything still queued is a fully staged pack. Preserve it for the
            # resume instead of deleting it.
            try:
                while True:
                    leftover = ready_q.get_nowait()
                    if leftover is not SENTINEL:
                        self._preserve_desc(session_id, leftover, "queued at shutdown")
            except queue.Empty:
                pass
            # Drain chunk_q too: the scanner's _force_put(SENTINEL) can spin
            # forever on a full queue once the stager has exited, leaking the
            # scanner thread past its join timeout.
            try:
                while True:
                    chunk_q.get_nowait()
            except queue.Empty:
                pass
            scanner_thread.join(timeout=15)
            stager_thread.join(timeout=15)
            if self.fetch_cores:
                unpin_current_process()

        if CANCEL.is_set():
            print("\n[ABORTED] Stopped by user. Session saved - re-run option 6 "
                  "to resume from the interrupted chunk.")
            return
        if failed or self._producer_err or state.scan_error:
            msg = self._producer_err or state.scan_error or (
                "a chunk failed during tape write")
            print(f"\n[REMOTE] Streaming pipeline stopped: {msg}. "
                  "Re-run to resume when the condition is fixed.")
            send_best_effort(
                self.notifier,
                f"[PIPELINE] STOPPED: {msg}. Re-run to resume.")
            return

        session_row = self.db.get_remote_session(session_id)
        if session_row and session_row.get('scan_complete'):
            pending = self.db.get_pending_chunks(session_id)
            if not pending:
                self.db.update_remote_session(
                    session_id, status='completed',
                    completed_at=datetime.now().isoformat()
                )
                self._backup_writer(LTOBackup).eject_tape(self.cfg.lto_drive)
                print("\n[REMOTE] Session complete. All streamed chunks archived.")
                send_best_effort(
                    self.notifier,
                    f"[PIPELINE] Session complete - {completed} chunk(s) "
                    "written in this run.")
            else:
                print(f"\n[REMOTE] Scan complete; {len(pending)} chunk(s) "
                      "remain pending. Re-run to resume.")

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
            self._run_streaming_session(session_id)
            return
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
            return

        if not pending_chunks:
            print("[REMOTE] All chunks already completed.")
            self.db.update_remote_session(
                session_id, status='completed',
                completed_at=datetime.now().isoformat()
            )
            return

        if not _ensure_lto_drive_ready(self.cfg.lto_drive):
            return

        # --- per-session pipeline state ---
        self._staged_bytes   = 0
        self._producer_err   = None
        self._producer_chunk = None
        self._consumer_chunk = None
        last_chunk = pending_chunks[-1]

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

        ready_q       = queue.Queue(maxsize=self.prefetch_ahead)
        stop_pipeline = threading.Event()
        SENTINEL      = object()

        # Race a forced update restart rather than trying to prevent one: stop
        # at the next chunk boundary so LTFS syncs its index while it still can.
        reboot_sentinel = RebootSentinel(
            stop_pipeline,
            on_detect=lambda reasons: send_best_effort(
                self.notifier,
                "[PIPELINE] Windows staged a restart — stopping at the next "
                "chunk boundary so the tape index is synced. Re-run option 6 "
                "to resume after the host restarts.")).start()

        def _producer():
            try:
                for ci in pending_chunks:
                    if CANCEL.is_set() or stop_pipeline.is_set():
                        break
                    planned_bytes, _, planned_files = planned[ci]
                    self._await_staging_capacity(
                        planned_bytes, planned_files, stop_pipeline)
                    if CANCEL.is_set() or stop_pipeline.is_set():
                        break
                    desc = self._stage_chunk(
                        session_id, ci, self.db.get_chunk_files(session_id, ci))
                    if desc is None:
                        if not CANCEL.is_set():
                            self._producer_err = f"chunk {ci + 1} could not be staged"
                        break
                    # Enqueue, staying responsive to pipeline shutdown.
                    queued = False
                    while not (CANCEL.is_set() or stop_pipeline.is_set()):
                        try:
                            ready_q.put(desc, timeout=1)
                            queued = True
                            break
                        except queue.Full:
                            continue
                    if not queued:
                        self._discard_desc(desc)
                        break
            except Exception as e:
                get_logger().exception("prefetch producer failed")
                self._producer_err = str(e)
            finally:
                ready_q.put(SENTINEL)

        prod = threading.Thread(target=_producer, name='prefetch-producer',
                                daemon=True)
        prod.start()

        hb_stop = threading.Event()
        self._start_pipeline_heartbeat(hb_stop, ready_q, total_chunks)

        completed = 0
        failed    = False
        try:
            while True:
                desc = ready_q.get()
                if desc is SENTINEL:
                    break
                if CANCEL.is_set():
                    self._preserve_desc(session_id, desc, "cancelled before write")
                    break
                if stop_pipeline.is_set():
                    self._preserve_desc(session_id, desc, "stop requested before write")
                    break
                ci          = desc.chunk_index
                eject_after = (ci == last_chunk)
                reasons, _sccm = self._pre_tape_write_reboot_check(
                    session_id, desc, tape_label)
                if reasons:
                    self._preserve_desc(session_id, desc, "blocked by pending restart")
                    stop_pipeline.set()
                    break
                if not self._write_chunk(session_id, desc, tape_label, eject_after):
                    if not CANCEL.is_set():
                        # A failed chunk is re-fetched and repacked on resume,
                        # so its staged copy only wastes staging space — free
                        # it now. On cancel, skip the rmtree so exit is quick.
                        self._discard_desc(desc)
                    failed = True
                    break
                completed += 1
        finally:
            stop_pipeline.set()
            hb_stop.set()
            reboot_sentinel.stop()
            # Drain the queue so a producer blocked on a full put() can exit.
            # Staged packs are kept, not deleted: each one is a whole fetch+pack
            # cycle and the resume can write it straight to tape.
            try:
                while True:
                    leftover = ready_q.get_nowait()
                    if leftover is not SENTINEL:
                        self._preserve_desc(session_id, leftover, "queued at shutdown")
            except queue.Empty:
                pass
            prod.join(timeout=15)
            if self.fetch_cores:
                unpin_current_process()

        if reboot_sentinel.triggered:
            print("\n[REMOTE] Stopped cleanly because Windows staged a "
                  "restart. The tape index was synced and the session is "
                  "resumable — re-run option 6 after the host restarts.")
            return
        if CANCEL.is_set():
            print("\n[ABORTED] Stopped by user. Session saved — "
                  "re-run option 6 to resume from the interrupted chunk.")
            return
        if failed or self._producer_err:
            msg = self._producer_err or "a chunk failed during tape write"
            print(f"\n[REMOTE] Pipeline stopped: {msg}. "
                  f"Re-run to resume from the failed chunk.")
            send_best_effort(
                self.notifier,
                f"[PIPELINE] STOPPED: {msg}. Re-run to resume from the "
                "failed chunk.")
            return
        if completed == len(pending_chunks):
            self.db.update_remote_session(
                session_id, status='completed',
                completed_at=datetime.now().isoformat()
            )
            print("\n[REMOTE] Session complete. All chunks archived to tape.")
            send_best_effort(
                self.notifier,
                f"[PIPELINE] Session complete — all {total_chunks} chunk(s) "
                "archived to tape.")

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

    def _await_staging_capacity(self, planned_bytes, planned_files, stop_evt):
        """Block until there is room to stage another chunk without breaching the
        staging cap or starving the disk. Accounts for the ~2x transient
        footprint while a chunk is packed (fetch_dir + pack_dir coexist),
        sized on the estimated physical (allocated) footprint rather than
        the plan's logical byte total."""
        # peak while fetch + pack dirs coexist
        need  = 2 * self._physical_estimate(planned_bytes, planned_files)
        floor = LOCAL_STAGING_RESERVE_BYTES
        warned = False
        while not (CANCEL.is_set() or stop_evt.is_set()):
            with self._staged_lock:
                resident = self._staged_bytes
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

    def _stage_chunk(self, session_id, chunk_index, chunk_files):
        """Fetch then pack one chunk. Returns a ready-descriptor or None."""
        self._producer_chunk = chunk_index
        # The session id is embedded so the on-tape root (basename(pack_dir),
        # see LTOBackup._run_locked) is unique per session — two sessions on the
        # same tape never collide on '_pack_NNN'. Resuming a session reuses the
        # same deterministic names, so robocopy still same-size-skips.
        fetch_dir = os.path.join(
            self.staging_dir, f"_fetch_s{session_id:04d}_{chunk_index:03d}")
        pack_dir  = os.path.join(
            self.staging_dir, f"_pack_s{session_id:04d}_{chunk_index:03d}")

        # A pack this session preserved on an earlier stop can go straight to
        # tape: it is the same deterministic path, and the marker proves it is
        # complete. This is what makes a restart-driven stop cheap.
        resumed = self._try_resume_pack(session_id, chunk_index, pack_dir)
        if resumed is not None:
            return resumed

        # No usable pack. Any leftover pack dir is now known-untrustworthy, and
        # packing into it would silently mix stale files into the chunk, so it
        # goes before the packer runs.
        if os.path.isdir(pack_dir):
            get_logger().warning(
                "clearing an unusable leftover pack dir before repacking "
                "chunk %s: %s", chunk_index + 1, pack_dir)
            self._cleanup_dir(pack_dir)

        # --- FETCH (remote -> PC) ---
        self.db.update_chunk_status(session_id, chunk_index, 'fetching')
        fetch_start = time.perf_counter()
        ram_stats = {}
        governor = getattr(self, 'governor', None)
        if governor:
            planned_bytes = sum(int(row['file_size_bytes'])
                                for row in chunk_files)
            governor.wait_or_pause(
                "fetch", "start", needed_bytes=planned_bytes)
            fetch_guard = governor.mark_fetch_active()
        else:
            fetch_guard = None
        with RamStageSampler(
                "fetch", self.ram_sample_interval) as fetch_sampler:
            if fetch_guard:
                with fetch_guard:
                    fetch_ok, source_missing_files, fetched_file_count = (
                        self._fetch_chunk(
                            session_id, chunk_index, chunk_files, fetch_dir))
            else:
                fetch_ok, source_missing_files, fetched_file_count = (
                    self._fetch_chunk(
                        session_id, chunk_index, chunk_files, fetch_dir))
        ram_stats.update(fetch_sampler.as_details("fetch"))
        if not fetch_ok:
            if not CANCEL.is_set():
                self.db.update_chunk_status(session_id, chunk_index, 'fetch_failed')
            self._cleanup_dir(fetch_dir)
            return None
        if CANCEL.is_set():
            self._cleanup_dir(fetch_dir)
            return None
        fetch_seconds = time.perf_counter() - fetch_start
        fetch_bytes   = _dir_tree_size(fetch_dir)   # raw remote->PC payload

        if fetched_file_count == 0:
            self._cleanup_dir(fetch_dir)
            self._cleanup_dir(pack_dir)
            _status('REMOTE', f"Chunk {chunk_index + 1}: all source files are "
                               "missing; no tape write is required.")
            return StagedChunk(
                chunk_index=chunk_index,
                fetch_dir=fetch_dir,
                pack_dir=pack_dir,
                metadata=[],
                staged_bytes=0,
                fetch_seconds=fetch_seconds,
                fetch_bytes=fetch_bytes,
                pack_seconds=0,
                pack_bytes=0,
                ram_stats=ram_stats,
                source_missing_files=source_missing_files,
                skip_tape=True,
            )

        # --- PACK (small files -> ZIP, large files staged loose) ---
        self.db.update_chunk_status(session_id, chunk_index, 'packing')
        self._cleanup_dir(pack_dir)
        _phase('PACK', f"Packing chunk {chunk_index + 1}: "
                       f"small files -> ZIP, large files staged loose")
        pack_start = time.perf_counter()
        try:
            if governor:
                governor.wait_or_pause(
                    "pack", "start", needed_bytes=fetch_bytes,
                    queued_bytes=getattr(self, '_staged_bytes', 0))
                pack_guard = governor.mark_pack_active()
            else:
                pack_guard = None
            # on_existing='clean': this runs on the producer thread, which must
            # never block on the packer's interactive stdin prompt. If the dest
            # cannot be cleaned, the packer raises and the chunk stays
            # resumable instead of the pipeline deadlocking.
            packer = LTOPacker(
                self.cfg.max_zip_size_gb,
                index_min_file_mb=self.cfg.index_min_file_mb,
                index_packed_small_files=self.cfg.index_packed_small_files,
                manifest_enabled=self.cfg.small_file_manifest_enabled,
                manifest_format=self.cfg.small_file_manifest_format,
                manifest_compression=(
                    self.cfg.small_file_manifest_compression),
            )
            with RamStageSampler(
                    "pack", self.ram_sample_interval) as pack_sampler:
                if pack_guard:
                    with pack_guard:
                        metadata = packer.run(
                            source=fetch_dir,
                            dest=pack_dir,
                            threshold_mb=self.cfg.zip_threshold_mb,
                            skipped_tracker=self.skipped_tracker,
                            source_name='remote',
                            session_id=session_id,
                            chunk_index=chunk_index,
                            on_existing='clean',
                            governor=governor,
                            pack_file_batch_size=self.pack_file_batch_size,
                            pack_parallel_workers=self.pack_parallel_workers,
                        )
                else:
                    metadata = packer.run(
                        source=fetch_dir,
                        dest=pack_dir,
                        threshold_mb=self.cfg.zip_threshold_mb,
                        skipped_tracker=self.skipped_tracker,
                        source_name='remote',
                        session_id=session_id,
                        chunk_index=chunk_index,
                        on_existing='clean',
                        governor=governor,
                        pack_file_batch_size=self.pack_file_batch_size,
                        pack_parallel_workers=self.pack_parallel_workers,
                    )
            ram_stats.update(pack_sampler.as_details("pack"))
        except Exception as e:
            print(f"[REMOTE] Packer error: {e}")
            if not CANCEL.is_set():
                self.db.update_chunk_status(session_id, chunk_index, 'fetch_failed')
            self._cleanup_dir(fetch_dir)
            self._cleanup_dir(pack_dir)
            return None

        if not metadata:
            if not CANCEL.is_set():
                print(f"[REMOTE] Chunk {chunk_index + 1}: nothing to pack "
                      f"(empty fetch). Marking failed.")
                self.db.update_chunk_status(session_id, chunk_index, 'fetch_failed')
            self._cleanup_dir(fetch_dir)
            self._cleanup_dir(pack_dir)
            return None

        # LTOPacker necessarily sees temporary Windows staging paths. Replace
        # them before logging/indexing with the durable canonical remote paths
        # persisted in the remote manifest.
        canonical_count = _apply_canonical_remote_paths(
            metadata, self.db.get_chunk_files(session_id, chunk_index))
        if canonical_count != len(metadata):
            self.db.update_chunk_status(session_id, chunk_index, 'fetch_failed')
            self._cleanup_dir(fetch_dir)
            self._cleanup_dir(pack_dir)
            raise RuntimeError(
                "[DB] Refusing to index temporary staging paths: canonical "
                f"SOURCE paths mapped for only {canonical_count:,}/"
                f"{len(metadata):,} staged file(s)."
            )

        pack_seconds = time.perf_counter() - pack_start

        # Free the raw fetched copy now that packing is done — this halves the
        # per-chunk staging footprint so the prefetch buffer stays under the cap.
        self._cleanup_dir(fetch_dir)
        # A chunk's packer metadata and per-file dicts are the largest transient
        # Python allocation in the pipeline; reclaim them now, before the next
        # chunk's fetch grows the process again.
        gc.collect()

        staged_bytes = _dir_tree_size(pack_dir)
        with self._staged_lock:
            self._staged_bytes += staged_bytes
        _status('PIPELINE', f"Chunk {chunk_index + 1} staged & ready "
                            f"({staged_bytes / 1024**3:.1f} GB) — queued for tape.")
        return StagedChunk(
            chunk_index=chunk_index,
            fetch_dir=fetch_dir,
            pack_dir=pack_dir,
            metadata=metadata,
            staged_bytes=staged_bytes,
            # Per-phase producer timings, surfaced in the per-pack log. Fetch and
            # pack overlap the *previous* chunk's tape write, so they need not sum
            # to the consumer-measured Total time.
            fetch_seconds=fetch_seconds,
            fetch_bytes=fetch_bytes,
            pack_seconds=pack_seconds,
            pack_bytes=staged_bytes,
            ram_stats=ram_stats,
            source_missing_files=source_missing_files,
            skip_tape=False,
        )

    def _discard_desc(self, desc):
        """Drop a staged-but-unused chunk: clean its dirs and free its budget."""
        self._cleanup_dir(desc.fetch_dir)
        self._cleanup_dir(desc.pack_dir)
        with self._staged_lock:
            self._staged_bytes = max(0, self._staged_bytes - desc.staged_bytes)

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

    def _pack_inventory(self, pack_dir):
        """(name, size) for every file under pack_dir, sorted. The integrity basis."""
        items = []
        for root, _dirs, files in os.walk(pack_dir):
            for name in files:
                if name == _RESUME_MARKER:
                    continue
                full = os.path.join(root, name)
                rel = os.path.relpath(full, pack_dir).replace("\\", "/")
                try:
                    items.append([rel, os.path.getsize(full)])
                except OSError:
                    return None
        return sorted(items)

    def _preserve_desc(self, session_id, desc, why):
        """Keep a staged pack on disk so the resume can write it directly.

        The counterpart to ``_discard_desc``. The chunk's DB row is left alone —
        it was never written, so it stays resumable — and the staging dirs are
        left in place. Only the in-memory budget is released, because this
        process is on its way out and the next one re-measures staging from disk.

        A marker recording the descriptor is written *last*, and its presence is
        what makes the pack reusable. A pack that was interrupted mid-write has
        no marker, so a later run cannot mistake a partial pack for a complete
        one — the failure mode that would put a truncated chunk on tape.
        """
        with self._staged_lock:
            self._staged_bytes = max(0, self._staged_bytes - desc.staged_bytes)

        marker_ok = False
        try:
            inventory = self._pack_inventory(desc.pack_dir)
            if inventory is None:
                raise OSError("pack directory is unreadable")
            payload = {
                "version": 1,
                "session_id": session_id,
                "chunk_index": desc.chunk_index,
                "fetch_dir": desc.fetch_dir,
                "pack_dir": desc.pack_dir,
                "staged_bytes": desc.staged_bytes,
                "skip_tape": desc.skip_tape,
                "metadata": desc.metadata,
                "source_missing_files": desc.source_missing_files,
                "fetch_seconds": desc.fetch_seconds,
                "fetch_bytes": desc.fetch_bytes,
                "pack_seconds": desc.pack_seconds,
                "pack_bytes": desc.pack_bytes,
                "ram_stats": desc.ram_stats,
                "pack_inventory": inventory,
                "preserved_at": datetime.now().isoformat(),
                "reason": why,
            }
            tmp = os.path.join(desc.pack_dir, _RESUME_MARKER + ".tmp")
            final = os.path.join(desc.pack_dir, _RESUME_MARKER)
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(payload, fh)
            os.replace(tmp, final)  # atomic: a half-written marker never appears
            marker_ok = True
        except Exception:
            get_logger().exception(
                "could not write the resume marker for chunk %s; the pack stays "
                "on disk but the resume will re-fetch it", desc.chunk_index + 1)

        msg = (f"pack_preserved_for_resume: chunk {desc.chunk_index + 1} "
               f"({why}) kept at {desc.pack_dir} reusable={marker_ok}")
        get_logger().warning(msg)
        suffix = ("" if marker_ok
                  else " (marker failed — resume will re-fetch this chunk)")
        print(f"\n[PIPELINE] Chunk {desc.chunk_index + 1}: pack kept in staging "
              f"for resume ({why}).{suffix}")

    def _try_resume_pack(self, session_id, chunk_index, pack_dir):
        """Return a StagedChunk for an intact preserved pack, else None.

        Only a pack carrying a marker whose recorded inventory still matches the
        directory byte-for-byte is reused. Anything else — no marker, changed
        sizes, extra or missing files — is treated as untrustworthy and the
        caller re-fetches. Being wrong here means writing a corrupt chunk to
        tape and recording it as good, so the bar is exact equality, not
        heuristics.
        """
        marker = os.path.join(pack_dir, _RESUME_MARKER)
        if not os.path.isfile(marker):
            return None
        try:
            with open(marker, encoding="utf-8") as fh:
                payload = json.load(fh)
        except Exception:
            get_logger().warning(
                "resume marker for chunk %s is unreadable; re-fetching",
                chunk_index + 1)
            return None

        if (payload.get("version") != 1
                or payload.get("chunk_index") != chunk_index
                or payload.get("session_id") != session_id):
            get_logger().warning(
                "resume marker at %s does not match session %s chunk %s; "
                "re-fetching", pack_dir, session_id, chunk_index + 1)
            return None

        expected = payload.get("pack_inventory")
        actual = self._pack_inventory(pack_dir)
        if actual is None or [list(x) for x in expected or []] != [
                list(x) for x in actual]:
            get_logger().warning(
                "preserved pack for chunk %s failed its integrity check "
                "(inventory changed); re-fetching", chunk_index + 1)
            print(f"[REMOTE] Preserved pack for chunk {chunk_index + 1} failed "
                  f"its integrity check — re-fetching it.")
            return None

        desc = StagedChunk(
            chunk_index=chunk_index,
            fetch_dir=payload["fetch_dir"],
            pack_dir=pack_dir,
            metadata=payload.get("metadata") or [],
            staged_bytes=int(payload.get("staged_bytes") or 0),
            fetch_seconds=payload.get("fetch_seconds"),
            fetch_bytes=payload.get("fetch_bytes"),
            pack_seconds=payload.get("pack_seconds"),
            pack_bytes=payload.get("pack_bytes"),
            ram_stats=payload.get("ram_stats") or {},
            source_missing_files=payload.get("source_missing_files") or [],
            skip_tape=bool(payload.get("skip_tape")),
        )
        with self._staged_lock:
            self._staged_bytes += desc.staged_bytes
        get_logger().warning(
            "resume_from_existing_pack: session=%s chunk=%s pack=%s bytes=%s "
            "preserved_at=%s", session_id, chunk_index + 1, pack_dir,
            desc.staged_bytes, payload.get("preserved_at"))
        print(f"[REMOTE] Chunk {chunk_index + 1}: reusing the pack preserved at "
              f"{payload.get('preserved_at')} — no re-fetch, no re-pack.")
        return desc

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
            reasons, sccm = reboot_block_reasons(block_on_unknown=True)
        except Exception:
            # The gate itself must never take the pipeline down. Fall back to
            # the Windows markers alone rather than blocking forever.
            log.exception("pre_tape_write_reboot_check failed; "
                          "falling back to Windows markers")
            return list(pending_reboot_reasons()), None

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
                     eject_after):
        chunk_index = desc.chunk_index
        self._consumer_chunk = chunk_index
        pack_dir = desc.pack_dir

        if desc.skip_tape:
            log_path = _write_source_missing_only_log(
                self.cfg.backup_log_dir,
                session_id,
                chunk_index,
                tape_label,
                desc.source_missing_files or [],
                source_host=self.remote_host.split('.', 1)[0],
                source_path=self.remote_session_path,
                notifier=self.notifier,
            )
            self.db.update_chunk_status(session_id, chunk_index, 'done')
            self._cleanup_dir(desc.fetch_dir)
            self._cleanup_dir(pack_dir)
            if log_path:
                print(f"[REMOTE] Source-missing CSV summary: {log_path}")
            if eject_after:
                self._backup_writer().eject_tape(self.cfg.lto_drive)
            return True

        # present_bytes excludes files marked source_missing during the fetch.
        _, planned_bytes, _ = self.db.get_chunk_size_summary(
            session_id, chunk_index).get(chunk_index, (0, 0, 0))
        if not self._ensure_remote_chunk_fits_tape(
                tape_label, planned_bytes, chunk_index):
            self.db.update_chunk_status(session_id, chunk_index, 'backup_failed')
            return False

        self.db.update_chunk_status(session_id, chunk_index, 'backing')
        # _NoEjectBackup keeps the tape mounted; eject only after the final chunk.
        backup_cls = LTOBackup if eject_after else _NoEjectBackup
        governor = getattr(self, 'governor', None)
        tape_pending = (
            governor.mark_tape_write_pending()
            if governor else None
        )
        try:
            if tape_pending:
                with tape_pending:
                    self._backup_writer(backup_cls).run(
                        source=pack_dir,
                        tape_drive=self.cfg.lto_drive,
                        tape_label=tape_label,
                        packer_metadata=desc.metadata,
                        stage_stats=desc,
                        source_host=self.remote_host.split('.', 1)[0],
                        skipped_tracker=self.skipped_tracker,
                        remote_session_id=session_id,
                        remote_chunk_index=chunk_index,
                    )
            else:
                self._backup_writer(backup_cls).run(
                    source=pack_dir,
                    tape_drive=self.cfg.lto_drive,
                    tape_label=tape_label,
                    packer_metadata=desc.metadata,
                    stage_stats=desc,
                    source_host=self.remote_host.split('.', 1)[0],
                    skipped_tracker=self.skipped_tracker,
                    remote_session_id=session_id,
                    remote_chunk_index=chunk_index,
                )
        except Exception as e:
            if CANCEL.is_set():
                # Robocopy was terminated by the stop request; leave the chunk
                # non-'done' (resumable) and skip eject.
                return False
            print(f"[REMOTE] Backup error: {e}")
            self.db.update_chunk_status(session_id, chunk_index, 'backup_failed')
            return False

        if CANCEL.is_set():
            return False

        self.db.update_chunk_status(session_id, chunk_index, 'done')

        # --- FLUSH staged files for this chunk ---
        _status('REMOTE', f"Flushing staged files for chunk {chunk_index + 1}...")
        self._cleanup_dir(desc.fetch_dir)   # already removed after packing
        self._cleanup_dir(pack_dir)
        with self._staged_lock:
            self._staged_bytes = max(0, self._staged_bytes - desc.staged_bytes)
        return True

    def _ensure_remote_chunk_fits_tape(self, tape_label, planned_bytes,
                                       chunk_index):
        tape = self.db.get_tape(tape_label)
        if not tape:
            print(f"[DB] Tape '{tape_label}' is not registered.")
            return False
        used_bytes = self.db.recalculate_tape_used_space(tape_label)
        _, available_bytes = tape_budget_bytes(
            tape['total_capacity'], used_bytes)
        if planned_bytes > available_bytes:
            print(f"[TAPE] Remote chunk {chunk_index + 1} does not fit on "
                  f"'{tape_label}' ({planned_bytes / 1024**3:.2f} GiB needed, "
                  f"{max(0, available_bytes) / 1024**3:.2f} GiB available "
                  "under the DB safety budget).")
            return False
        return True

    # ------------------------------------------------------------------
    # Pipeline status heartbeat
    # ------------------------------------------------------------------

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
                msg = (
                    f"queued={ready_q.qsize()}/{self.prefetch_ahead} | "
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

    def _fetch_chunk(self, session_id, chunk_index, chunk_files, fetch_dir):
        os.makedirs(_long(fetch_dir), exist_ok=True)
        total_chunks = self.db.count_chunks(session_id)
        source_missing_files = []
        fetched_file_count = 0
        records = []
        pending = []        # primary files: extracted at their sanitized path
        collisions = []     # renamed files: fetched individually, then moved
        claimed = {}        # case-folded local_rel -> remote rel that owns it
        fetching_ids = []

        for row in chunk_files:
            remote_fpath = row['remote_path']
            fsize        = row['file_size_bytes']
            manifest_id  = row['manifest_id']

            if row['status'] == 'source_missing':
                source_missing_files.append({
                    'manifest_id': manifest_id,
                    'remote_path': remote_fpath,
                    'file_size_bytes': fsize,
                })
                self.skipped_tracker.add(
                    'remote', remote_fpath, row['error_msg'] or 'source missing',
                    'fetch', session_id=session_id, chunk_index=chunk_index)
                print(f"[REMOTE] Skip (source already missing): {remote_fpath}")
                continue

            try:
                remote_base, rel = _remote_fetch_base_and_rel(
                    self.remote_path, remote_fpath
                )
            except ValueError as e:
                self.db.update_manifest_row(
                    manifest_id,
                    session_id=session_id,
                    status='fetch_failed',
                    error_msg=str(e),
                )
                print(f"[REMOTE] Invalid remote path: {e}")
                return False, source_missing_files, fetched_file_count

            # rel is the true remote path (sent verbatim to remote tar); the
            # local copy lands under the name the Windows extractor can write.
            local_rel = _winsafe_extracted_rel(rel)
            key = local_rel.casefold()
            collided = key in claimed
            if collided:
                # Two distinct remote names map to the same on-disk path —
                # rename this one so neither file is silently overwritten.
                clash_with = claimed[key]
                local_rel  = _disambiguate_local_rel(local_rel, claimed)
                key        = local_rel.casefold()
                print(f"[REMOTE] Name collision: '{rel}' and '{clash_with}' map "
                      f"to the same Windows path — fetching the former as "
                      f"'{local_rel}'.")
            claimed[key] = rel

            local_path = os.path.join(fetch_dir, local_rel.replace('/', os.sep))
            records.append((row, remote_base, rel, local_rel, local_path))

            # Skip if already fetched with matching size (resume support)
            if os.path.exists(_long(local_path)):
                try:
                    if os.path.getsize(_long(local_path)) == fsize:
                        print(f"[REMOTE] Skip (already fetched): {rel}")
                        continue
                    os.remove(_long(local_path))  # partial from interrupted run
                except OSError:
                    pass

            fetching_ids.append(manifest_id)
            (collisions if collided else pending).append(
                (row, remote_base, rel, local_rel, local_path))

        for start in range(0, len(fetching_ids), self.metadata_batch_size):
            if governor := getattr(self, 'governor', None):
                governor.wait_or_pause("fetch", "continue")
            batch_ids = fetching_ids[start:start + self.metadata_batch_size]
            self.db.update_manifest_rows_fetching(
                batch_ids, session_id=session_id)

        if pending or collisions:
            todo_bytes = sum(row['file_size_bytes']
                             for row, *_ in pending + collisions)
            todo_count = len(pending) + len(collisions)
            _phase('FETCH', f"Remote -> PC | chunk {chunk_index + 1}/{total_chunks} | "
                            f"{todo_count} file(s), {todo_bytes / 1024**3:.2f} GB")
            _status('SSH', f"Opening tar stream to "
                           f"{self.remote_user}@{self.remote_host} "
                           f"(cipher={self.ssh_cipher or 'default'}, "
                           f"mbuffer={'on' if self.use_mbuffer else 'off'})")

            fetch_stop  = threading.Event()
            fetch_abort = threading.Event()
            self._start_fetch_monitor(fetch_stop, fetch_abort, fetch_dir,
                                      todo_bytes)

            pending_by_base = defaultdict(list)
            for row, remote_base, rel, local_rel, local_path in pending:
                pending_by_base[remote_base].append((row, rel, local_path))

            # One work item per (base, metadata-sized batch). Streams > 1 run
            # these concurrently to overlap per-file stalls; the default (1)
            # keeps the exact legacy single-stream behaviour.
            work_items = []
            for remote_base, base_pending in pending_by_base.items():
                for start in range(
                        0, len(base_pending), self.metadata_batch_size):
                    work_items.append(
                        (remote_base,
                         base_pending[start:start + self.metadata_batch_size]))

            streams = max(1, int(getattr(self, 'fetch_parallel_streams', 1)))

            try:
                if streams <= 1 or len(work_items) <= 1:
                    for remote_base, base_batch in work_items:
                        if CANCEL.is_set():
                            return False, source_missing_files, fetched_file_count
                        if governor := getattr(self, 'governor', None):
                            governor.wait_or_pause("fetch", "continue")
                        ok, err = self._fetch_one_batch(
                            remote_base, base_batch, fetch_dir, fetch_abort)
                        if not ok:
                            if CANCEL.is_set():
                                return False, source_missing_files, fetched_file_count
                            print(f"\n[REMOTE] Tar fetch failed:\n{err}")
                            self.db.update_manifest_rows_fetch_failed(
                                (row['manifest_id'] for row, _, _ in base_batch),
                                err, session_id=session_id)
                            return False, source_missing_files, fetched_file_count
                elif not self._fetch_batches_parallel(
                        work_items, fetch_dir, fetch_abort, session_id, streams):
                    return False, source_missing_files, fetched_file_count

                # Renamed files can't ride the shared stream (bsdtar would
                # extract them onto the primary's path), so fetch each alone
                # into an isolated dir and move it to its disambiguated name.
                if collisions and not self._fetch_collisions(
                        session_id, collisions, fetch_dir,
                        source_missing_files, fetch_abort):
                    return False, source_missing_files, fetched_file_count
            finally:
                fetch_stop.set()
                _progress_done()
        else:
            print(f"[REMOTE] Chunk {chunk_index + 1}/{total_chunks}: "
                  "all files already fetched.")

        source_missing_ids = {
            item['manifest_id'] for item in source_missing_files
        }
        fetched_updates = []
        for row, _, rel, local_rel, local_path in records:
            fsize       = row['file_size_bytes']
            manifest_id = row['manifest_id']
            if manifest_id in source_missing_ids:
                continue
            if not os.path.exists(_long(local_path)):
                # An absent file is normally a genuine remote omission (the
                # remote tar emitted a tolerated "Cannot stat"). But if the
                # local target is unwritable by the non-long-path-aware
                # extractor — a reserved device name or an over-MAX_PATH target
                # — the absence is a LOCAL drop we must not record as
                # source_missing. Fail the chunk loudly; it stays resumable.
                reserved = _reserved_name_component(local_rel)
                too_long = _exceeds_legacy_path_limit(local_path)
                if reserved or too_long:
                    reason = (f"reserved Windows device name '{reserved}'"
                              if reserved else
                              f"target exceeds the {_LEGACY_PATH_LIMIT}-char "
                              "Windows path limit")
                    msg = (f"refusing to skip '{row['remote_path']}': it could "
                           f"not be written locally ({reason}). "
                           f"Target: {local_path}")
                    print(f"\n[REMOTE] {msg}")
                    self.db.update_manifest_row(
                        manifest_id, session_id=session_id,
                        status='fetch_failed', error_msg=msg[:500])
                    return False, source_missing_files, fetched_file_count
                detail = {
                    'manifest_id': manifest_id,
                    'remote_path': row['remote_path'],
                    'file_size_bytes': fsize,
                }
                source_missing_files.append(detail)
                source_missing_ids.add(manifest_id)
                self.skipped_tracker.add(
                    'remote', row['remote_path'], "missing after tar fetch",
                    'fetch', session_id=session_id, chunk_index=chunk_index)
                print(f"[REMOTE] Source missing; skipped: {row['remote_path']}")
                self.db.update_manifest_row(
                    manifest_id,
                    session_id=session_id,
                    status='source_missing',
                    local_rel_path=None,
                    error_msg="missing after tar fetch",
                )
                continue

            try:
                actual = os.path.getsize(_long(local_path))
            except OSError as e:
                self.db.update_manifest_row(
                    manifest_id,
                    session_id=session_id,
                    status='fetch_failed',
                    error_msg=f"stat failed: {e}",
                )
                return False, source_missing_files, fetched_file_count

            if actual != fsize:
                print(f"[REMOTE] Size mismatch for {rel}: "
                      f"expected {fsize} B, got {actual} B")
                try:
                    os.remove(_long(local_path))
                except OSError:
                    pass
                self.db.update_manifest_row(
                    manifest_id,
                    session_id=session_id,
                    status='fetch_failed',
                    error_msg=f"size mismatch: expected {fsize}, got {actual}",
                )
                return False, source_missing_files, fetched_file_count

            fetched_updates.append((local_rel, manifest_id))
        for start in range(0, len(fetched_updates), self.metadata_batch_size):
            if governor := getattr(self, 'governor', None):
                governor.wait_or_pause("fetch", "continue")
            self.db.update_manifest_rows_fetched(
                fetched_updates[start:start + self.metadata_batch_size],
                session_id=session_id)
        fetched_file_count = len(fetched_updates)
        return True, source_missing_files, fetched_file_count

    def _fetch_one_batch(self, remote_base, base_batch, fetch_dir, fetch_abort):
        """Fetch one metadata-sized batch as a single tar stream.

        Returns (ok, err) — the shared shape used by both fetch paths. A batch
        that fails on a transient network/DNS error is retried with exponential
        backoff (``fetch_transient_retries``) before giving up, so a momentary
        blip costs seconds rather than the whole streaming session. A cancel or
        a sibling-stream abort ends the retries immediately."""
        attempts = max(0, getattr(self, "fetch_transient_retries", 0))
        base = getattr(self, "fetch_transient_retry_base", 5.0)
        rel_paths = [rel for _, rel, _ in base_batch]

        for attempt in range(attempts + 1):
            if CANCEL.is_set() or fetch_abort.is_set():
                return False, "cancelled"
            ok, err = _remote_tar_fetch(
                self.remote_user,
                self.remote_host,
                remote_base,
                rel_paths,
                fetch_dir,
                password=self.remote_password,
                cipher=self.ssh_cipher,
                use_mbuffer=self.use_mbuffer,
                mbuffer_size=self.mbuffer_size,
                fetch_cores=self.fetch_cores,
                abort_evt=fetch_abort,
            )
            if ok or attempt >= attempts or not _is_transient_fetch_error(err):
                return ok, err
            if CANCEL.is_set() or fetch_abort.is_set():
                return False, "cancelled"
            delay = min(60.0, base * (2 ** attempt))
            msg = (f"transient fetch error (attempt {attempt + 1}/{attempts + 1}), "
                   f"retrying in {delay:.0f}s: {str(err).strip()[:160]}")
            get_logger().warning("fetch_transient_retry: %s", msg)
            print(f"\n[REMOTE] {msg}")
            # Interruptible wait: a cancel/abort during backoff returns at once.
            if fetch_abort.wait(delay) or CANCEL.is_set():
                return False, "cancelled"
        return False, "retries exhausted"

    def _fetch_batches_parallel(self, work_items, fetch_dir, fetch_abort,
                                session_id, streams):
        """Fetch work items with up to ``streams`` concurrent tar streams.

        Batches are disjoint file lists extracted into the same fetch dir, so
        concurrency is safe. On the first non-cancel failure the shared
        ``fetch_abort`` is set (killing the other streams' ssh/tar trees) and
        the failing batch's rows are marked fetch_failed. Returns True on full
        success, False on failure (caller re-fetches the chunk on resume)."""
        from concurrent.futures import ThreadPoolExecutor

        governor = getattr(self, 'governor', None)
        failure = {}
        failure_lock = threading.Lock()

        def _worker(item):
            remote_base, base_batch = item
            if CANCEL.is_set() or fetch_abort.is_set():
                return item, False, "cancelled"
            ok, err = self._fetch_one_batch(
                remote_base, base_batch, fetch_dir, fetch_abort)
            if not ok and not CANCEL.is_set():
                with failure_lock:
                    if not failure:
                        failure['err'] = err
                        failure['batch'] = base_batch
                        fetch_abort.set()  # stop the sibling streams
            return item, ok, err

        _status('FETCH', f"Parallel fetch: {streams} concurrent stream(s) over "
                         f"{len(work_items)} batch(es).")
        with ThreadPoolExecutor(max_workers=streams) as pool:
            futures = []
            for item in work_items:
                if CANCEL.is_set() or fetch_abort.is_set():
                    break
                if governor:
                    governor.wait_or_pause("fetch", "continue")
                futures.append(pool.submit(_worker, item))
            for fut in futures:
                fut.result()

        if failure:
            if CANCEL.is_set():
                return False
            print(f"\n[REMOTE] Tar fetch failed:\n{failure['err']}")
            self.db.update_manifest_rows_fetch_failed(
                (row['manifest_id'] for row, _, _ in failure['batch']),
                failure['err'], session_id=session_id)
            return False
        return not (CANCEL.is_set() or fetch_abort.is_set())

    def _fetch_collisions(self, session_id, collisions, fetch_dir,
                          source_missing_files, abort_evt=None):
        """Fetch files whose sanitized name clashed with another file's.

        Each is streamed alone into a private temp dir (where bsdtar writes it
        at its natural sanitized path) and then moved to the disambiguated
        local_path. Missing sources are accumulated and skipped; other failures
        leave the row marked fetch_failed for the caller to surface."""
        collide_root = os.path.join(fetch_dir, '_collide')
        try:
            for row, remote_base, rel, _local_rel, local_path in collisions:
                if CANCEL.is_set():
                    return False
                if governor := getattr(self, 'governor', None):
                    governor.wait_or_pause("fetch", "continue")
                tmp = os.path.join(collide_root, str(row['manifest_id']))
                shutil.rmtree(_long(tmp), ignore_errors=True)
                os.makedirs(_long(tmp), exist_ok=True)

                ok, err = _remote_tar_fetch(
                    self.remote_user,
                    self.remote_host,
                    remote_base,
                    [rel],
                    tmp,
                    password=self.remote_password,
                    cipher=self.ssh_cipher,
                    use_mbuffer=self.use_mbuffer,
                    mbuffer_size=self.mbuffer_size,
                    fetch_cores=self.fetch_cores,
                    abort_evt=abort_evt,
                )
                if not ok:
                    if CANCEL.is_set():
                        return False
                    print(f"\n[REMOTE] Tar fetch failed (renamed file):\n{err}")
                    self.db.update_manifest_row(
                        row['manifest_id'], session_id=session_id,
                        status='fetch_failed',
                        error_msg=err[:500])
                    return False

                # Alone in tmp, the file lands at its natural sanitized path.
                natural = os.path.join(
                    tmp, _winsafe_extracted_rel(rel).replace('/', os.sep))
                if not os.path.exists(_long(natural)):
                    # As in _fetch_chunk: a reserved-name or over-MAX_PATH
                    # target is a local write failure, not a remote omission —
                    # surface it loudly (resumable) instead of source_missing.
                    reserved = _reserved_name_component(_local_rel)
                    too_long = (_exceeds_legacy_path_limit(natural)
                                or _exceeds_legacy_path_limit(local_path))
                    if reserved or too_long:
                        reason = (f"reserved Windows device name '{reserved}'"
                                  if reserved else
                                  f"target exceeds the {_LEGACY_PATH_LIMIT}-char "
                                  "Windows path limit")
                        msg = (f"refusing to skip '{row['remote_path']}': it "
                               f"could not be written locally ({reason}).")
                        print(f"\n[REMOTE] {msg}")
                        self.db.update_manifest_row(
                            row['manifest_id'], session_id=session_id,
                            status='fetch_failed',
                            error_msg=msg[:500])
                        return False
                    detail = {
                        'manifest_id': row['manifest_id'],
                        'remote_path': row['remote_path'],
                        'file_size_bytes': row['file_size_bytes'],
                    }
                    source_missing_files.append(detail)
                    self.skipped_tracker.add(
                        'remote', row['remote_path'], "missing after tar fetch",
                        'fetch', session_id=session_id, chunk_index=None)
                    print(f"[REMOTE] Source missing; skipped: {row['remote_path']}")
                    self.db.update_manifest_row(
                        row['manifest_id'], session_id=session_id,
                        status='source_missing',
                        local_rel_path=None,
                        error_msg="missing after tar fetch")
                    continue

                os.makedirs(_long(os.path.dirname(local_path)), exist_ok=True)
                try:
                    os.replace(_long(natural), _long(local_path))
                except OSError as e:
                    print(f"[REMOTE] Could not place renamed file {rel}: {e}")
                    self.db.update_manifest_row(
                        row['manifest_id'], session_id=session_id,
                        status='fetch_failed',
                        error_msg=f"move failed: {e}")
                    return False
            return True
        finally:
            shutil.rmtree(_long(collide_root), ignore_errors=True)

    def _start_fetch_monitor(self, stop_evt, abort_evt, fetch_dir, total_bytes):
        """Live remote->PC throughput, plus a staging watchdog.

        Progress is the fetch dir's logical growth. The watchdog fires
        abort_evt — killing the tar stream — before an overrunning chunk can
        exhaust the staging disk and wedge the pipeline: either when free
        space on the staging volume reaches the reserve floor, or when the
        chunk exceeds its planned bytes by fetch_overrun_abort_factor. Any
        overrun past the warn threshold is reported loudly (the plan's sizes
        come from the scan, so a growing or sparse-expanded remote file shows
        up here first)."""
        abort_factor = self.fetch_abort_factor

        def _alarm(msg):
            print(f"\n[FETCH][ALERT] {msg}")
            send_best_effort(self.notifier, f"[FETCH] {msg}")

        def _mon():
            prev_bytes = 0
            prev_time  = time.time()
            interval   = 2
            overrun_warned = False
            while not stop_evt.wait(interval):
                walk_start = time.time()
                cur   = _dir_tree_size(fetch_dir)
                now   = time.time()
                # Rewalking a chunk with many small files is itself expensive;
                # keep the scan overhead under ~10% of the monitor's cycle.
                interval = min(30, max(2, (now - walk_start) * 10))
                dt    = now - prev_time
                speed = ((cur - prev_bytes) / 1024**2) / dt if dt > 0 else 0
                pct   = (cur / total_bytes * 100) if total_bytes else 0
                remaining = max(0, total_bytes - cur)
                eta = remaining / (speed * 1024**2) if speed > 0 else None
                _progress_line(
                    f"[FETCH] {pct:.1f}% | {speed:.1f} MB/s | "
                    f"{cur / 1024**3:.1f}/{total_bytes / 1024**3:.1f} GB | "
                    f"ETA {_fmt_eta(eta)}"
                )
                prev_bytes = cur
                prev_time  = now

                if (total_bytes and not overrun_warned
                        and cur > total_bytes * _FETCH_OVERRUN_WARN_FACTOR):
                    overrun_warned = True
                    _alarm(
                        f"chunk overrun: {cur / 1024**3:.1f} GB fetched of "
                        f"{total_bytes / 1024**3:.1f} GB planned — a remote "
                        "file likely grew after the scan. The fetch continues "
                        "but is watched for a hard overrun."
                    )

                try:
                    free = shutil.disk_usage(self.staging_dir).free
                except OSError:
                    free = None
                if free is not None and free <= LOCAL_STAGING_RESERVE_BYTES:
                    _alarm(
                        f"aborting fetch: staging free space is down to "
                        f"{free / 1024**3:.1f} GB (reserve floor "
                        f"{LOCAL_STAGING_RESERVE_BYTES / 1024**3:.0f} GB). "
                        "The chunk stays resumable."
                    )
                    abort_evt.set()
                    return
                if (total_bytes and abort_factor
                        and cur > total_bytes * abort_factor):
                    _alarm(
                        f"aborting fetch: {cur / 1024**3:.1f} GB fetched "
                        f"exceeds {abort_factor:.1f}x the planned "
                        f"{total_bytes / 1024**3:.1f} GB "
                        "(fetch_overrun_abort_factor). The chunk stays "
                        "resumable; re-scan so the plan matches the source."
                    )
                    abort_evt.set()
                    return
        threading.Thread(target=_mon, name='fetch-monitor', daemon=True).start()

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    def _cleanup_remote_staging_dirs(self):
        """Remove remote-session temp folders before a truly fresh run."""
        staging_root = os.path.abspath(self.staging_dir)
        try:
            names = os.listdir(staging_root)
        except OSError as e:
            print(f"[REMOTE] Warning - could not inspect staging directory: {e}")
            return

        for name in names:
            if not (name.startswith("_fetch_") or name.startswith("_pack_")):
                continue
            path = os.path.abspath(os.path.join(staging_root, name))
            if path == staging_root or not path.startswith(staging_root + os.sep):
                print(f"[REMOTE] Warning - refusing to clean suspicious path: {path}")
                continue
            self._cleanup_dir(path)

    def _cleanup_dir(self, path):
        if os.path.exists(_long(path)):
            governor = getattr(self, 'governor', None)
            if governor:
                governor.wait_until(governor.can_cleanup, "cleanup")
            try:
                if governor:
                    with governor.mark_cleanup_active():
                        shutil.rmtree(_long(path))
                else:
                    shutil.rmtree(_long(path))
                print(f"[REMOTE] Cleaned: {path}")
            except OSError as e:
                print(f"[REMOTE] Warning — could not clean {path}: {e}")
