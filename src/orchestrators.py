"""Local and remote archive orchestrators."""
import os
import time
import queue
import shutil
import threading
import subprocess
import codecs
import shlex
import posixpath
import re
from datetime import datetime
from collections import defaultdict
from typing import Dict, Optional, Type

try:
    import psutil
except ImportError:  # optional dependency — priority/affinity degrade gracefully
    psutil = None

from .backup import LTOBackup, _NoEjectBackup
from .constants import (DEFAULT_TAPE_CAPACITY_GB, LOCAL_STAGING_RESERVE_BYTES,
                        LOCAL_TAPE_BUDGET_BYTES, LTFS_WRITE_WARNING,
                        ROOT_FILES_GROUP, TAPE_BUDGET_LABEL,
                        _auto_pack_decision)
from .db import _apply_canonical_remote_paths
from .ltfs import _ensure_lto_drive_ready, get_volume_label
from .packer import LTOAnalyzer, LTOPacker, ensure_staging_space
from .paths import _LEGACY_PATH_LIMIT, _dir_tree_size, _disambiguate_local_rel, _exceeds_legacy_path_limit, _long, _remote_fetch_base_and_rel, _reserved_name_component, _volume_cluster_size, _winsafe_extracted_rel
from .remote_transport import _remote_tar_fetch, _ssh_run, _ssh_stream_command
from .reporting import _write_source_missing_only_log
from .runtime import CANCEL, _acquire_tape_io_lock, _fmt_eta, _phase, _priority_class, _progress_done, _progress_line, _release_tape_io_lock, _status, compute_affinity_sets, pin_current_process, unpin_current_process
from .skipped import SkippedFileTracker
from .telegram_notify import TelegramNotifier, send_best_effort
from .ui import ConsoleUI


_FIND_WARNING_RE = re.compile(r"^find:\s+[\"'`‘](.*?)[\"'`’]:\s*(.*)$")

# A fetch this far past its planned bytes gets one loud alert; the hard
# abort threshold is the configurable fetch_overrun_abort_factor.
_FETCH_OVERRUN_WARN_FACTOR = 1.10
_FIND_PATH_QUOTES = "\"'`\u2018\u2019\u201c\u201d\ufffd?"


class RemoteScanner:
    """Run remote find and preserve partial-scan omissions as skipped rows."""

    def __init__(self, remote_user, remote_host, remote_password='',
                 timeout=None, skipped_tracker=None, ui=None):
        self.remote_user = remote_user
        self.remote_host = remote_host
        self.remote_password = remote_password
        self.timeout = timeout
        self.skipped_tracker = skipped_tracker or SkippedFileTracker()
        self.ui = ui or ConsoleUI()

    def _record_find_warnings(self, stderr):
        def clean_find_path(path):
            path = (path or '').strip(_FIND_PATH_QUOTES)
            for marker in ("ג€˜", "ג€™", "ג€�", "ג€?"):
                path = path.removeprefix(marker).removesuffix(marker)
            return path.strip(_FIND_PATH_QUOTES)

        for line in (stderr or '').splitlines():
            line = line.strip()
            if not line:
                continue
            match = _FIND_WARNING_RE.match(line)
            if match:
                path, reason = match.groups()
                path = clean_find_path(path)
            elif line.startswith("find:") and ": " in line:
                path, reason = line[len("find:"):].strip().rsplit(": ", 1)
                path = clean_find_path(path)
            else:
                path, reason = line, "remote find warning"
            self.skipped_tracker.add('remote', path, reason, 'scan')

    def _scan_one(self, scan_path):
        find_cmd = f"find {shlex.quote(scan_path)} -type f -printf '%s %p\\0'"
        result = _ssh_run(
            self.remote_user,
            self.remote_host,
            find_cmd,
            capture=True,
            password=self.remote_password,
            timeout=self.timeout,
        )
        stdout = result.stdout or ''
        stderr = (result.stderr or '').strip()
        if result.returncode == 124:
            if stderr:
                self._record_find_warnings(stderr)
            raise RuntimeError(
                f"[REMOTE] SSH scan timed out while scanning {scan_path!r}. "
                "The partial find output was discarded so an incomplete backup "
                "session cannot be created. Increase "
                "[PERFORMANCE] ssh_command_timeout_seconds or split the "
                "selection into smaller runs, then start a fresh session."
            )
        if result.returncode == 255:
            raise RuntimeError(
                f"[REMOTE] SSH scan failed (exit {result.returncode}):\n{stderr}"
            )
        if result.returncode != 0 and stderr:
            self._record_find_warnings(stderr)
            self.ui.warning(
                f"[REMOTE] Scan completed with warnings (find exit {result.returncode}); "
                "inaccessible paths were recorded in the skipped-file report."
            )
            if not stdout.strip():
                return []
        elif result.returncode != 0 and not stdout.strip():
            raise RuntimeError(
                f"[REMOTE] SSH scan failed (exit {result.returncode}):\n{stderr}"
            )
        if stdout and not stdout.endswith('\0'):
            # find terminates every record with NUL; a different tail means the
            # stream was cut mid-record. The fragment is rejected below (its
            # truncated path cannot sit under a scan root), never planned.
            self.ui.warning(
                "[REMOTE] Scan stream ended mid-record (truncated transfer?); "
                "the partial record was discarded and recorded as skipped."
            )
        root = posixpath.normpath(scan_path.replace('\\', '/').strip())
        manifest = []
        for record in stdout.split('\0'):
            if not record:
                continue
            parts = record.split(' ', 1)
            if len(parts) != 2:
                continue
            size_s, path = parts
            try:
                size = int(size_s)
            except ValueError:
                self.skipped_tracker.add(
                    'remote', path, f"invalid find size token: {size_s}", 'scan')
                continue
            # Every legitimate record lies under (or is) a scan root. Anything
            # else is a corrupt/truncated record; planning it is dangerous — a
            # path like '/strg' names a directory and would make the fetch tar
            # stream an entire unplanned tree.
            norm = posixpath.normpath(path)
            if not (norm == root or norm.startswith(root + '/')):
                self.skipped_tracker.add(
                    'remote', path,
                    "scan record outside scan roots (truncated stream?)",
                    'scan')
                continue
            manifest.append((path, size))
        return manifest

    def scan(self, scan_paths):
        manifest = []
        for scan_path in scan_paths:
            self.ui.info(f"[REMOTE] Scanning {scan_path} ...")
            manifest.extend(self._scan_one(scan_path))
        return manifest


class StreamingRemoteScanner(RemoteScanner):
    """Yield remote find records as they arrive over a long-lived SSH stream."""

    def __init__(self, remote_user, remote_host, remote_password='',
                 skipped_tracker=None, ui=None, cipher=''):
        super().__init__(
            remote_user,
            remote_host,
            remote_password=remote_password,
            timeout=None,
            skipped_tracker=skipped_tracker,
            ui=ui,
        )
        self.cipher = cipher

    def _parse_record(self, record, root):
        parts = record.split(' ', 1)
        if len(parts) != 2:
            return None
        size_s, path = parts
        try:
            size = int(size_s)
        except ValueError:
            self.skipped_tracker.add(
                'remote', path, f"invalid find size token: {size_s}", 'scan')
            return None
        norm = posixpath.normpath(path)
        if not (norm == root or norm.startswith(root + '/')):
            self.skipped_tracker.add(
                'remote', path,
                "scan record outside scan roots (truncated stream?)",
                'scan')
            return None
        return path, size

    def iter_scan(self, scan_paths, stop_evt=None):
        for scan_path in scan_paths:
            if stop_evt is not None and stop_evt.is_set():
                return
            yield from self._iter_scan_one(scan_path, stop_evt=stop_evt)

    def _iter_scan_one(self, scan_path, stop_evt=None):
        self.ui.info(f"[REMOTE] Streaming scan {scan_path} ...")
        root = posixpath.normpath(scan_path.replace('\\', '/').strip())
        find_cmd = f"find {shlex.quote(scan_path)} -type f -printf '%s %p\\0'"
        ssh_cmd, env, err = _ssh_stream_command(
            self.remote_user,
            self.remote_host,
            find_cmd,
            password=self.remote_password,
            cipher=self.cipher,
        )
        if err:
            raise RuntimeError(f"[REMOTE] SSH scan failed: {err}")
        if ssh_cmd is None:
            raise RuntimeError("[REMOTE] SSH scan failed: no command produced")

        proc = subprocess.Popen(
            ssh_cmd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
        )
        stderr_chunks = []

        def _drain_stderr():
            try:
                while True:
                    chunk = proc.stderr.read(65536) if proc.stderr else b''
                    if not chunk:
                        return
                    stderr_chunks.append(chunk)
            except OSError:
                return

        stderr_thread = threading.Thread(
            target=_drain_stderr, name='streaming-find-stderr', daemon=True)
        stderr_thread.start()

        decoder = codecs.getincrementaldecoder('utf-8')('replace')
        buffer = ''
        saw_record = False
        try:
            while True:
                if stop_evt is not None and stop_evt.is_set():
                    proc.terminate()
                    break
                chunk = proc.stdout.read(65536) if proc.stdout else b''
                if not chunk:
                    break
                buffer += decoder.decode(chunk)
                while '\0' in buffer:
                    record, buffer = buffer.split('\0', 1)
                    if not record:
                        continue
                    parsed = self._parse_record(record, root)
                    if parsed is not None:
                        saw_record = True
                        yield parsed
            tail = decoder.decode(b'', final=True)
            if tail:
                buffer += tail
            if buffer:
                self.ui.warning(
                    "[REMOTE] Scan stream ended mid-record (truncated transfer?); "
                    "the partial record was discarded and recorded as skipped."
                )
                self._parse_record(buffer, root)
        finally:
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
            stderr_thread.join(timeout=5)

        stderr = b''.join(stderr_chunks).decode('utf-8', errors='replace').strip()
        if proc.returncode == 255:
            raise RuntimeError(
                f"[REMOTE] SSH scan failed (exit {proc.returncode}):\n{stderr}"
            )
        if proc.returncode != 0 and stderr:
            self._record_find_warnings(stderr)
            self.ui.warning(
                f"[REMOTE] Streaming scan completed with warnings "
                f"(find exit {proc.returncode}); inaccessible paths were "
                "recorded in the skipped-file report."
            )
        elif proc.returncode != 0 and not saw_record:
            raise RuntimeError(
                f"[REMOTE] SSH scan failed (exit {proc.returncode}):\n{stderr}"
            )


class ChunkPlanner:
    """Greedy largest-first chunk planner.

    Bin-packs on each file's estimated on-disk footprint — logical size
    rounded up to the staging volume's allocation unit, times a safety
    padding factor — so a chunk's *physical* staging footprint stays within
    budget even when logical sizes understate "size on disk" (cluster
    rounding on small files, filesystem metadata, files that grow after the
    scan). The manifest's logical sizes are preserved in the plan."""

    def __init__(self, budget_bytes, alloc_unit=1, padding_factor=1.0):
        self.budget_bytes = budget_bytes
        self.alloc_unit = max(1, int(alloc_unit))
        self.padding_factor = max(1.0, padding_factor)

    def footprint(self, fsize):
        """Estimated bytes a file of logical size fsize allocates on disk."""
        clusters = max(1, -(-int(fsize) // self.alloc_unit))
        return int(clusters * self.alloc_unit * self.padding_factor)

    def plan(self, manifest):
        chunks = []
        current = []
        cur_fp = 0
        for remote_path, fsize in sorted(manifest, key=lambda x: x[1], reverse=True):
            fp = self.footprint(fsize)
            if fp > self.budget_bytes:
                chunks.append([(remote_path, fsize)])
                continue
            if cur_fp + fp > self.budget_bytes and current:
                chunks.append(current)
                current = []
                cur_fp = 0
            current.append((remote_path, fsize))
            cur_fp += fp
        if current:
            chunks.append(current)
        return chunks


class StreamingChunkBuilder:
    """Build threshold-sized chunks in discovery order."""

    def __init__(self, budget_bytes, alloc_unit=1, padding_factor=1.0):
        self.planner = ChunkPlanner(
            budget_bytes, alloc_unit=alloc_unit, padding_factor=padding_factor)
        self.current = []
        self.current_fp = 0

    def add(self, remote_path, fsize):
        fp = self.planner.footprint(fsize)
        if fp > self.planner.budget_bytes:
            chunks = []
            if self.current:
                chunks.append(self.current)
                self.current = []
                self.current_fp = 0
            chunks.append([(remote_path, fsize)])
            return chunks
        if self.current and self.current_fp + fp > self.planner.budget_bytes:
            ready = self.current
            self.current = [(remote_path, fsize)]
            self.current_fp = fp
            return [ready]
        self.current.append((remote_path, fsize))
        self.current_fp += fp
        return []

    def flush(self):
        if not self.current:
            return []
        ready = self.current
        self.current = []
        self.current_fp = 0
        return [ready]


class LocalOrchestrator:
    """Persistent local multi-tape archive workflow."""

    def __init__(self, cfg, db, ui=None, skipped_tracker=None):
        self.cfg = cfg
        self.db = db
        self.ui = ui or ConsoleUI()
        self.skipped_tracker = skipped_tracker or SkippedFileTracker()
        self.notifier = TelegramNotifier.from_config(cfg)
        self.source_dir = cfg.source_dir
        self.staging_dir = cfg.staging_dir
        self.fill_pct = cfg.staging_fill_pct

    def _backup_writer(self, cls=_NoEjectBackup):
        # type: (Type[LTOBackup]) -> LTOBackup
        return cls(
            self.db,
            self.cfg.ibm_eject_cmd,
            log_dir=self.cfg.backup_log_dir,
            notifier=self.notifier,
        )

    def run(self):
        try:
            source_dir = os.path.abspath(self.source_dir)
            existing = self.db.get_active_local_session(source_dir)
            if existing:
                pending = self.db.get_local_pending_chunks(existing['session_id'])
                done = existing['total_chunks'] - len(pending)
                print(f"\n[LOCAL] Found active session: {existing['session_label']}")
                print(f"        Created : {existing['created_at']}")
                print(f"        Progress: {done}/{existing['total_chunks']} chunks completed.")
                print(f"        Mode    : {existing['backup_mode']}")
                print("1. Resume from first incomplete chunk")
                print("2. Abandon and start a fresh session")
                print("0. Cancel")
                choice = self.ui.prompt("Choose: ").strip()
                if choice == '1':
                    self._run_session(existing['session_id'])
                    return
                if choice == '2':
                    self.db.update_local_session(existing['session_id'], status='abandoned')
                else:
                    return

            self._start_new_session(source_dir)
        finally:
            self.skipped_tracker.print_summary(self.ui, self.cfg.backup_log_dir)

    def _start_new_session(self, source_dir):
        analyzer = LTOAnalyzer()
        recommended_pack = analyzer.analyze(source_dir, self.cfg.zip_threshold_mb)
        backup_mode = self._choose_backup_mode(recommended_pack)
        if backup_mode is None:
            print("[ABORTED] Local session was not created.")
            return

        plan = self._mounted_tape_plan_context()
        chunks = analyzer.build_local_allocation_plan(
            source_dir,
            first_tape_budget_bytes=plan['available_bytes'],
        )
        analyzer.render_allocation_plan(
            chunks,
            first_tape_used_bytes=plan['used_bytes'],
            first_tape_label=plan['tape_label'],
        )
        confirm = input("Create this local multi-tape session? Type YES to continue: ").strip()
        if confirm != 'YES':
            print("[ABORTED] Local session was not created.")
            return

        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        session_label = f"LOCAL_{os.path.basename(source_dir.rstrip(os.sep))}_{ts}"
        session_id = self.db.create_local_session(
            session_label, source_dir, chunks, backup_mode=backup_mode
        )
        print(f"[LOCAL] Session created: {session_label} (id {session_id}, mode {backup_mode})")
        self._run_session(session_id)

    def _mounted_tape_plan_context(self):
        """Return the first-tape budget after reconciling DB occupancy."""
        tape_label = get_volume_label(self.cfg.lto_drive)
        if not tape_label:
            print("[TAPE] No mounted volume label detected; the plan assumes the "
                  f"first tape has the full {TAPE_BUDGET_LABEL} safety budget.")
            return {
                'tape_label': None,
                'used_bytes': 0,
                'available_bytes': LOCAL_TAPE_BUDGET_BYTES,
            }

        tape = self.db.get_tape(tape_label)
        if not tape:
            print(f"[TAPE] Mounted tape '{tape_label}' has no DB record; the plan "
                  "treats it as a fresh tape. A non-empty unregistered tape will "
                  "still be rejected before writing.")
            return {
                'tape_label': tape_label,
                'used_bytes': 0,
                'available_bytes': LOCAL_TAPE_BUDGET_BYTES,
            }

        used_bytes = self.db.recalculate_tape_used_space(tape_label)
        capacity_bytes = LOCAL_TAPE_BUDGET_BYTES
        if tape['total_capacity']:
            capacity_bytes = min(
                capacity_bytes, int(tape['total_capacity'] * 1024**3))
        available_bytes = max(0, capacity_bytes - used_bytes)
        print(f"[TAPE] Mounted '{tape_label}': DB occupied "
              f"{used_bytes / 1024**4:.2f} TiB; safe remaining "
              f"{available_bytes / 1024**4:.2f} TiB.")
        if available_bytes <= 0:
            raise RuntimeError(
                f"[TAPE] Mounted tape '{tape_label}' has no capacity remaining "
                f"under the {TAPE_BUDGET_LABEL} safety budget."
            )
        return {
            'tape_label': tape_label,
            'used_bytes': used_bytes,
            'available_bytes': available_bytes,
        }

    def _choose_backup_mode(self, recommended_pack):
        recommended = 'pack' if recommended_pack else 'direct'
        labels = {
            'direct': 'Direct backup',
            'pack': 'AUTO-PILOT / staged packing',
        }
        choices = [recommended, 'direct' if recommended == 'pack' else 'pack']

        print("\n[LOCAL] Choose backup mode:")
        for idx, mode in enumerate(choices, 1):
            suffix = " (Recommended)" if mode == recommended else ""
            if mode == 'direct':
                detail = "copy selected top-level folders directly to tape"
            else:
                detail = f"pack files < {self.cfg.zip_threshold_mb:.0f} MB and stage the batch"
            print(f"{idx}. {labels[mode]}{suffix} - {detail}")
        print("0. Cancel")

        while True:
            choice = input("Choose backup mode: ").strip()
            if choice == '0':
                return None
            if choice in ('1', '2'):
                return choices[int(choice) - 1]
            print("[ERROR] Invalid selection.")

    def _run_session(self, session_id):
        session = self.db.get_local_session(session_id)
        if not session:
            print(f"[LOCAL] Session not found: {session_id}")
            return

        pending = self.db.get_local_pending_chunks(session_id)
        if not pending:
            self.db.update_local_session(
                session_id, status='completed',
                completed_at=datetime.now().isoformat()
            )
            print("[LOCAL] Session complete. All chunks archived.")
            return

        if not _ensure_lto_drive_ready(self.cfg.lto_drive):
            return

        print(f"\n[LOCAL] Processing {len(pending)} pending chunk(s).")
        for loop_idx, chunk_index in enumerate(pending):
            if loop_idx > 0:
                # The previous chunk finished and ejected its tape. A local
                # session uses one tape per chunk, so pause for a tape swap and
                # re-verify drive readiness before continuing.
                print("\n[LOCAL] The previous tape has been ejected.")
                input("Insert the NEXT blank/formatted tape, wait until ready, "
                      "then press Enter...")
                if not _ensure_lto_drive_ready(self.cfg.lto_drive):
                    print("[LOCAL] Drive not ready. Re-run option 1 to resume.")
                    return
            entries = self.db.get_local_chunk_entries(session_id, chunk_index)
            print(f"\n[LOCAL] === Tape {chunk_index + 1}/{session['total_chunks']} ===")
            ok = self._process_chunk(session, chunk_index, entries)
            if not ok:
                print(f"[LOCAL] Chunk {chunk_index + 1} stopped. Re-run option 1 to resume.")
                return

        self.db.update_local_session(
            session_id, status='completed',
            completed_at=datetime.now().isoformat()
        )
        print("\n[LOCAL] Session complete. All chunks archived.")

    def _process_chunk(self, session, chunk_index, entries):
        tape_label = self._prepare_tape_for_chunk(session, chunk_index, entries)
        if not tape_label:
            return False

        self.db.assign_local_chunk_tape(session['session_id'], chunk_index, tape_label)
        self.db.update_local_chunk_status(session['session_id'], chunk_index, 'staged')

        files = self._collect_chunk_files(session['source_dir'], entries)
        backup_mode = session['backup_mode'] if 'backup_mode' in session.keys() else 'auto'
        if backup_mode == 'direct':
            if self._can_direct_copy_entries(entries):
                return self._process_direct_chunk(session, chunk_index, entries, tape_label)
            print("[LOCAL] Direct mode cannot copy loose root-level files as a "
                  "separate tape chunk; using staged packing for this chunk.")
        elif backup_mode == 'auto':
            if self._can_direct_copy_entries(entries) and not self._should_pack_chunk(files):
                return self._process_direct_chunk(session, chunk_index, entries, tape_label)
        else:
            print("[LOCAL] AUTO-PILOT selected: staging and packing this chunk.")

        already = self.db.get_local_indexed_original_paths(
            session['session_id'], chunk_index, tape_label
        )
        batches = self._make_batches(files)
        if not batches:
            print("[LOCAL] No files to process for this chunk.")
            self.db.update_local_chunk_status(session['session_id'], chunk_index, 'backed_up')
            return True

        for batch_index, batch in enumerate(batches):
            pending = [f for f in batch if f['path'] not in already]
            if not pending:
                print(f"[LOCAL] Batch {batch_index + 1}/{len(batches)} already indexed; skipping.")
                continue

            batch_name = self._batch_name(session['session_id'], chunk_index, batch_index)
            pack_dir = os.path.join(self.staging_dir, batch_name)
            bundle_prefix = f"Bundle_s{session['session_id']:04d}_c{chunk_index + 1:03d}_b{batch_index + 1:03d}"
            batch_bytes = sum(f['size'] for f in pending)
            print(f"\n[LOCAL] Batch {batch_index + 1}/{len(batches)}: "
                  f"{len(pending)} file(s), {batch_bytes / 1024**3:.2f} GiB")

            try:
                self._cleanup_dir(pack_dir)
                ensure_staging_space(
                    self.staging_dir,
                    batch_bytes,
                    context=f"local batch {batch_index + 1}/{len(batches)}",
                )
                metadata = LTOPacker(self.cfg.max_zip_size_gb).run_manifest(
                    source_root=session['source_dir'],
                    dest=pack_dir,
                    threshold_mb=self.cfg.zip_threshold_mb,
                    file_entries=pending,
                    bundle_prefix=bundle_prefix,
                    skipped_tracker=self.skipped_tracker,
                    source_name='local',
                    session_id=session['session_id'],
                    chunk_index=chunk_index,
                )
                exclude_files, exclude_dirs = self._build_resume_excludes(
                    session['session_id'], chunk_index, tape_label,
                    batch_name, pack_dir
                )
                self._backup_writer().run(
                    source=pack_dir,
                    tape_drive=self.cfg.lto_drive,
                    tape_label=tape_label,
                    packer_metadata=metadata,
                    exclude_file_paths=exclude_files,
                    exclude_dir_paths=exclude_dirs,
                    local_session_id=session['session_id'],
                    local_chunk_index=chunk_index,
                    tape_parent_dir=self._session_tape_dir(session),
                    skipped_tracker=self.skipped_tracker,
                )
                already.update(m['original_path'] for m in metadata)
            except Exception as e:
                print(f"[LOCAL] Batch failed: {e}")
                self._cleanup_dir(pack_dir)
                return False
            finally:
                self._cleanup_dir(pack_dir)

        self.db.update_local_chunk_status(session['session_id'], chunk_index, 'backed_up')
        self._backup_writer(LTOBackup).eject_tape(self.cfg.lto_drive)
        return True

    def _prepare_tape_for_chunk(self, session, chunk_index, entries):
        assigned = next((e['tape_label'] for e in entries if e['tape_label']), None)
        detected = get_volume_label(self.cfg.lto_drive)
        if detected:
            print(f"[TAPE] Detected label: {detected}")
            tape_label = detected
        else:
            print("[TAPE] Could not auto-detect tape label.")
            tape_label = input("Enter tape Volume Label manually (or Enter to cancel): ").strip()
        if not tape_label:
            print("[ABORTED] No tape label provided.")
            return None

        if assigned and tape_label.upper() != assigned.upper():
            print(f"[TAPE] This chunk is assigned to '{assigned}', "
                  f"but '{tape_label}' is mounted.")
            return None

        if not assigned:
            root_empty = self._tape_root_is_empty()
            record_count = self.db.count_tape_file_records(tape_label)

            if not self.db.tape_exists(tape_label):
                if not root_empty:
                    print(f"[TAPE] Mounted tape '{tape_label}' is not registered "
                          "and is not empty. Register it first or use a blank tape.")
                    return None
                print(f"[TAPE] Registering fresh tape '{tape_label}'.")
                self.db.register_tape(tape_label, DEFAULT_TAPE_CAPACITY_GB)
            elif not root_empty or record_count > 0:
                print(f"[TAPE] Appending to registered tape '{tape_label}' "
                      f"({record_count} indexed file record(s) already present).")
            if not self._ensure_chunk_fits_tape(tape_label, entries):
                return None

        return tape_label

    def _tape_root_is_empty(self):
        _acquire_tape_io_lock(f"inspect tape root {self.cfg.lto_drive}")
        try:
            try:
                return len(os.listdir(self.cfg.lto_drive)) == 0
            except OSError as e:
                print(f"[TAPE] Cannot inspect tape root: {e}")
                return False
        finally:
            _release_tape_io_lock()

    def _ensure_chunk_fits_tape(self, tape_label, entries):
        planned_bytes = sum(e['dir_size_bytes'] for e in entries)

        # The LTFS free-space figure is advisory only (it varies with hardware
        # compression); LOCAL_TAPE_BUDGET_BYTES is the authoritative guard. This
        # probe is a best-effort early-out, so a read failure is non-fatal.
        _acquire_tape_io_lock(f"read free space {self.cfg.lto_drive}")
        try:
            disk_free = shutil.disk_usage(self.cfg.lto_drive).free
            if planned_bytes > disk_free:
                print(f"[TAPE] Warning: LTFS reports less free space than the "
                      f"planned chunk ({planned_bytes / 1024**3:.2f} GiB needed, "
                      f"{disk_free / 1024**3:.2f} GiB reported). The DB safety "
                      "budget remains authoritative because LTFS compression "
                      "makes this figure advisory.")
        except OSError as e:
            print(f"[TAPE] Cannot read LTFS free space: {e}")
        finally:
            _release_tape_io_lock()

        tape = self.db.get_tape(tape_label)
        if not tape:
            print(f"[DB] Tape '{tape_label}' is not registered.")
            return False

        used_bytes = self.db.recalculate_tape_used_space(tape_label)
        capacity_bytes = LOCAL_TAPE_BUDGET_BYTES
        if tape['total_capacity']:
            capacity_bytes = min(
                capacity_bytes, int(tape['total_capacity'] * 1024**3))
        available_bytes = capacity_bytes - used_bytes
        if planned_bytes > available_bytes:
            print(f"[TAPE] '{tape_label}' does not have enough indexed "
                  f"capacity for this chunk ({planned_bytes / 1024**3:.2f} "
                  f"GiB needed, {max(0, available_bytes) / 1024**3:.2f} "
                  "GiB available in DB).")
            return False

        return True

    def _collect_chunk_files(self, source_dir, entries):
        collected = []
        for entry in entries:
            top = entry['top_level_dir']
            if top == ROOT_FILES_GROUP:
                try:
                    scan = list(os.scandir(source_dir))
                except OSError as e:
                    raise RuntimeError(f"Cannot scan source root: {e}")
                for item in scan:
                    if item.is_file():
                        collected.append(self._file_entry(source_dir, item.path))
            else:
                root = os.path.join(source_dir, top)
                for cur, _, files in os.walk(root):
                    for file in files:
                        collected.append(self._file_entry(source_dir, os.path.join(cur, file)))
        return sorted(collected, key=lambda f: f['rel'].lower())

    def _file_entry(self, source_dir, path):
        size = os.path.getsize(path)
        return {
            'path': path,
            'rel': os.path.relpath(path, source_dir),
            'size': size,
        }

    def _can_direct_copy_entries(self, entries):
        return all(e['top_level_dir'] != ROOT_FILES_GROUP for e in entries)

    def _should_pack_chunk(self, files):
        total_files = len(files)
        total_bytes = sum(f['size'] for f in files)
        small_files = [
            f for f in files
            if (f['size'] / (1024 * 1024)) < self.cfg.zip_threshold_mb
        ]
        small_bytes = sum(f['size'] for f in small_files)
        should_pack, file_ratio, byte_ratio = _auto_pack_decision(
            total_files, total_bytes, len(small_files), small_bytes
        )
        if should_pack:
            print(f"[LOCAL] AUTO-PILOT: packing {len(small_files)} small file(s) "
                  f"({byte_ratio*100:.2f}% of chunk data).")
        else:
            print(f"[LOCAL] DIRECT: {len(small_files)} file(s) are under "
                  f"{self.cfg.zip_threshold_mb:.0f} MB, but only "
                  f"{byte_ratio*100:.2f}% of chunk data; skipping staging.")
        return should_pack

    def _process_direct_chunk(self, session, chunk_index, entries, tape_label):
        print("[LOCAL] Direct chunk copy: selected top-level directories will be "
              "copied from source to tape without staging large files.")
        try:
            for entry in sorted(entries, key=lambda e: e['top_level_dir'].lower()):
                source = os.path.join(session['source_dir'], entry['top_level_dir'])
                if not os.path.isdir(source):
                    raise RuntimeError(f"Direct source directory not found: {source}")
                print(f"\n[LOCAL] Direct backup: {entry['top_level_dir']} "
                      f"({entry['dir_size_bytes'] / 1024**3:.2f} GiB)")
                self._backup_writer().run(
                    source=source,
                    tape_drive=self.cfg.lto_drive,
                    tape_label=tape_label,
                    packer_metadata=None,
                    local_session_id=session['session_id'],
                    local_chunk_index=chunk_index,
                    tape_parent_dir=self._session_tape_dir(session),
                    skipped_tracker=self.skipped_tracker,
                )
        except Exception as e:
            print(f"[LOCAL] Direct chunk failed: {e}")
            return False

        self.db.update_local_chunk_status(session['session_id'], chunk_index, 'backed_up')
        self._backup_writer(LTOBackup).eject_tape(self.cfg.lto_drive)
        return True

    def _make_batches(self, files):
        target_budget, usable_bytes, free_bytes = self._staging_limits()
        batches = []
        current = []
        current_size = 0
        for entry in files:
            if entry['size'] > usable_bytes:
                raise RuntimeError(
                    f"File cannot fit safely in local staging "
                    f"({entry['size'] / 1024**3:.2f} GiB file; "
                    f"{usable_bytes / 1024**3:.2f} GiB usable): "
                    f"{entry['path']}"
                )
            if entry['size'] > target_budget:
                if current:
                    batches.append(current)
                    current = []
                    current_size = 0
                batches.append([entry])
                print(f"[LOCAL] Large file gets a dedicated staging chunk: "
                      f"{entry['size'] / 1024**3:.2f} GiB - {entry['rel']}")
                continue
            if current and current_size + entry['size'] > target_budget:
                batches.append(current)
                current = []
                current_size = 0
            current.append(entry)
            current_size += entry['size']
        if current:
            batches.append(current)
        print(f"[LOCAL] Staging: {free_bytes / 1024**3:.1f} GiB free; "
              f"{target_budget / 1024**3:.1f} GiB batch budget; "
              f"{len(batches)} sequential chunk(s).")
        return batches

    def _staging_limits(self):
        os.makedirs(self.staging_dir, exist_ok=True)
        free = shutil.disk_usage(self.staging_dir).free
        reserve = LOCAL_STAGING_RESERVE_BYTES
        usable = min(int(free * self.fill_pct), max(0, free - reserve))
        # Local staging is sequential. Unlike the SSH producer/consumer
        # pipeline, it does not benefit from small network-friendly chunks.
        configured_cap = int(self.cfg.staging_max_gb * 1024**3)
        target = min(usable, configured_cap)
        if target <= 0:
            raise RuntimeError(
                f"Not enough free staging space in '{self.staging_dir}'. "
                f"Free: {free / 1024**3:.2f} GiB; "
                f"required reserve: {reserve / 1024**3:.0f} GiB."
            )
        return target, usable, free

    def _batch_name(self, session_id, chunk_index, batch_index):
        return f"_local_s{session_id:04d}_c{chunk_index + 1:03d}_b{batch_index + 1:03d}"

    def _session_tape_dir(self, session):
        return session['session_label']

    def _build_resume_excludes(self, session_id, chunk_index, tape_label,
                               batch_name, pack_dir):
        session = self.db.get_local_session(session_id)
        tape_batch_root = os.path.abspath(os.path.join(
            self.cfg.lto_drive, self._session_tape_dir(session), batch_name
        ))
        exclude_files = []
        for tape_path in self.db.get_local_written_tape_paths(session_id, chunk_index, tape_label):
            try:
                abs_tape_path = os.path.abspath(tape_path)
                rel = os.path.relpath(abs_tape_path, tape_batch_root)
            except ValueError:
                continue
            if rel.startswith('..') or rel == '.':
                continue
            exclude_files.append(os.path.join(pack_dir, rel))
        if exclude_files:
            print(f"[LOCAL] Resume excludes: {len(exclude_files)} already indexed tape object(s).")
        return exclude_files, []

    def _cleanup_dir(self, path):
        if os.path.exists(path):
            try:
                shutil.rmtree(path)
                print(f"[LOCAL] Cleaned staging: {path}")
            except OSError as e:
                print(f"[LOCAL] Warning - could not clean {path}: {e}")


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
        self.notifier = TelegramNotifier.from_config(cfg)  # type: Optional[TelegramNotifier]
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

    def _backup_writer(self, cls=LTOBackup):
        return cls(
            self.db,
            self.cfg.ibm_eject_cmd,
            tape_priority=self.tape_priority,
            tape_affinity=self.tape_cores,
            log_dir=self.cfg.backup_log_dir,
            notifier=self.notifier,
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
        print(f"  Mode : continuous streaming scan -> fetch/pack -> tape")
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
        capacity_bytes = LOCAL_TAPE_BUDGET_BYTES
        if tape['total_capacity']:
            capacity_bytes = min(
                capacity_bytes, int(tape['total_capacity'] * 1024**3))
        reserved_bytes = 0
        if session_id is not None and hasattr(
                self.db, 'get_pending_remote_reserved_bytes'):
            reserved_bytes = self.db.get_pending_remote_reserved_bytes(session_id)
        available_bytes = max(0, capacity_bytes - used_bytes - reserved_bytes)
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

    def _scan_remote(self):
        """SSH find with -printf '%s %p\0' to get size + path for every file."""
        scanner = RemoteScanner(
            self.remote_user,
            self.remote_host,
            remote_password=self.remote_password,
            timeout=self.ssh_timeout,
            skipped_tracker=self.skipped_tracker,
            ui=self.ui,
        )
        return scanner.scan(self.remote_scan_paths)

    # ------------------------------------------------------------------
    # Bin-packing
    # ------------------------------------------------------------------

    def _chunk_budget(self):
        # Cap each chunk at chunk_cap_gb so the deep-prefetch pipeline can keep
        # 2+ chunks resident on the NVMe staging disk under the staging_max cap.
        os.makedirs(self.staging_dir, exist_ok=True)
        free = shutil.disk_usage(self.staging_dir).free
        usable = max(0, free - LOCAL_STAGING_RESERVE_BYTES)
        free_budget = int(usable * self.fill_pct)
        return min(free_budget, self.chunk_cap_bytes)

    def _bin_pack(self, manifest):
        """Greedy largest-first bin-packing into chunks that fit staging budget.
        Files larger than the budget get their own single-file chunk."""
        budget = self._chunk_budget()
        planner = ChunkPlanner(
            budget,
            alloc_unit=_volume_cluster_size(self.staging_dir),
            padding_factor=self.staging_padding,
        )
        for remote_path, fsize in sorted(manifest, key=lambda x: x[1], reverse=True):
            if planner.footprint(fsize) > budget:
                print(f"[WARN] File exceeds staging budget "
                      f"({fsize/1024**3:.2f} GB > {budget/1024**3:.2f} GB), "
                      f"placing in dedicated chunk: {os.path.basename(remote_path)}")
        return planner.plan(manifest)

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

        chunk_q = queue.Queue(maxsize=max(1, self.prefetch_ahead * 2))
        ready_q = queue.Queue(maxsize=self.prefetch_ahead)
        stop_pipeline = threading.Event()
        hb_stop = threading.Event()
        SENTINEL = object()

        tape_context = self._remote_tape_capacity_context(
            tape_label, session_id=session_id)
        remaining_lock = threading.Lock()
        remaining_bytes: Dict[str, int] = {'value': tape_context['available_bytes']}
        next_chunk_index: Dict[str, int] = {'value': (
            self.db.get_next_remote_chunk_index(session_id)
            if hasattr(self.db, 'get_next_remote_chunk_index')
            else self.db.count_chunks(session_id)
        )}
        scan_stats: Dict[str, int] = {'chunks': 0, 'files': 0, 'bytes': 0}
        scan_error: Dict[str, Optional[str]] = {'message': None}

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
                if logical_bytes > remaining_bytes['value']:
                    msg = (
                        f"next remote chunk needs {logical_bytes / 1024**3:.2f} GiB, "
                        f"but only {remaining_bytes['value'] / 1024**3:.2f} GiB "
                        "remains on the mounted tape under the DB safety budget"
                    )
                    scan_error['message'] = msg
                    self.db.mark_remote_scan_error(session_id, msg)
                    print(f"[TAPE] {msg}. Stopping before overfill.")
                    stop_pipeline.set()
                    return False

            chunk_index = next_chunk_index['value']
            result = self.db.append_remote_streaming_chunk(
                session_id, chunk_index, _chunk_rows(chunk_index, chunk_files))
            inserted_files = int(result.get('inserted_files', 0))
            inserted_bytes = int(result.get('inserted_bytes', 0))
            if inserted_files == 0:
                return True

            with remaining_lock:
                remaining_bytes['value'] = max(
                    0, remaining_bytes['value'] - inserted_bytes)
            next_chunk_index['value'] += 1
            scan_stats['chunks'] += 1
            scan_stats['files'] += inserted_files
            scan_stats['bytes'] += inserted_bytes
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
                    _status('SCAN', f"Complete: {scan_stats['chunks']:,} new "
                                    f"chunk(s), {scan_stats['files']:,} file(s), "
                                    f"{scan_stats['bytes'] / 1024**3:.2f} GiB")
            except Exception as e:
                scan_error['message'] = str(e)
                self.db.mark_remote_scan_error(session_id, str(e))
                self._producer_err = str(e)
                stop_pipeline.set()
            finally:
                _force_put(chunk_q, SENTINEL)

        def _stager():
            try:
                while not (CANCEL.is_set() or stop_pipeline.is_set()):
                    item = chunk_q.get()
                    if item is SENTINEL:
                        break
                    ci = item
                    summary = self.db.get_chunk_size_summary(
                        session_id, ci).get(ci, (0, 0, 0))
                    planned_bytes, _, planned_files = summary
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
        try:
            while True:
                desc = ready_q.get()
                if desc is SENTINEL:
                    break
                if CANCEL.is_set():
                    self._discard_desc(desc)
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
            try:
                while True:
                    leftover = ready_q.get_nowait()
                    if leftover is not SENTINEL:
                        self._discard_desc(leftover)
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
        if failed or self._producer_err or scan_error['message']:
            msg = self._producer_err or scan_error['message'] or (
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

        ready_q       = queue.Queue(maxsize=self.prefetch_ahead)
        stop_pipeline = threading.Event()
        SENTINEL      = object()

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
                    self._discard_desc(desc)
                    break
                ci          = desc['chunk_index']
                eject_after = (ci == last_chunk)
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
            # Drain the queue so a producer blocked on a full put() can exit,
            # and clean up any prefetched-but-unused chunks.
            try:
                while True:
                    leftover = ready_q.get_nowait()
                    if leftover is not SENTINEL:
                        self._discard_desc(leftover)
            except queue.Empty:
                pass
            prod.join(timeout=15)
            if self.fetch_cores:
                unpin_current_process()

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

        # --- FETCH (remote -> PC) ---
        self.db.update_chunk_status(session_id, chunk_index, 'fetching')
        fetch_start = time.perf_counter()
        fetch_ok, source_missing_files, fetched_file_count = self._fetch_chunk(
            session_id, chunk_index, chunk_files, fetch_dir
        )
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
            return {
                'chunk_index': chunk_index,
                'fetch_dir': fetch_dir,
                'pack_dir': pack_dir,
                'metadata': [],
                'staged_bytes': 0,
                'fetch_seconds': fetch_seconds,
                'fetch_bytes': fetch_bytes,
                'pack_seconds': 0,
                'pack_bytes': 0,
                'source_missing_files': source_missing_files,
                'skip_tape': True,
            }

        # --- PACK (small files -> ZIP, large files staged loose) ---
        self.db.update_chunk_status(session_id, chunk_index, 'packing')
        self._cleanup_dir(pack_dir)
        _phase('PACK', f"Packing chunk {chunk_index + 1}: "
                       f"small files -> ZIP, large files staged loose")
        pack_start = time.perf_counter()
        try:
            # on_existing='clean': this runs on the producer thread, which must
            # never block on the packer's interactive stdin prompt. If the dest
            # cannot be cleaned, the packer raises and the chunk stays
            # resumable instead of the pipeline deadlocking.
            metadata = LTOPacker(self.cfg.max_zip_size_gb).run(
                source=fetch_dir,
                dest=pack_dir,
                threshold_mb=self.cfg.zip_threshold_mb,
                skipped_tracker=self.skipped_tracker,
                source_name='remote',
                session_id=session_id,
                chunk_index=chunk_index,
                on_existing='clean',
            )
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

        staged_bytes = _dir_tree_size(pack_dir)
        with self._staged_lock:
            self._staged_bytes += staged_bytes
        _status('PIPELINE', f"Chunk {chunk_index + 1} staged & ready "
                            f"({staged_bytes / 1024**3:.1f} GB) — queued for tape.")
        return {
            'chunk_index':   chunk_index,
            'fetch_dir':     fetch_dir,
            'pack_dir':      pack_dir,
            'metadata':      metadata,
            'staged_bytes':  staged_bytes,
            # Per-phase producer timings, surfaced in the per-pack log. Fetch and
            # pack overlap the *previous* chunk's tape write, so they need not sum
            # to the consumer-measured Total time.
            'fetch_seconds': fetch_seconds,
            'fetch_bytes':   fetch_bytes,
            'pack_seconds':  pack_seconds,
            'pack_bytes':    staged_bytes,
            'source_missing_files': source_missing_files,
            'skip_tape': False,
        }

    def _discard_desc(self, desc):
        """Drop a staged-but-unused chunk: clean its dirs and free its budget."""
        self._cleanup_dir(desc['fetch_dir'])
        self._cleanup_dir(desc['pack_dir'])
        with self._staged_lock:
            self._staged_bytes = max(0, self._staged_bytes - desc['staged_bytes'])

    # ------------------------------------------------------------------
    # Consumer: write a staged chunk to tape  (runs on the main thread)
    # ------------------------------------------------------------------

    def _write_chunk(self, session_id, desc, tape_label, eject_after):
        chunk_index = desc['chunk_index']
        self._consumer_chunk = chunk_index
        pack_dir = desc['pack_dir']

        if desc.get('skip_tape'):
            log_path = _write_source_missing_only_log(
                self.cfg.backup_log_dir,
                session_id,
                chunk_index,
                tape_label,
                desc.get('source_missing_files') or [],
                source_host=self.remote_host.split('.', 1)[0],
                source_path=self.remote_session_path,
                notifier=self.notifier,
            )
            self.db.update_chunk_status(session_id, chunk_index, 'done')
            self._cleanup_dir(desc['fetch_dir'])
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
        try:
            self._backup_writer(backup_cls).run(
                source=pack_dir,
                tape_drive=self.cfg.lto_drive,
                tape_label=tape_label,
                packer_metadata=desc['metadata'],
                stage_stats=desc,
                source_host=self.remote_host.split('.', 1)[0],
                skipped_tracker=self.skipped_tracker,
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
        self._cleanup_dir(desc['fetch_dir'])   # already removed after packing
        self._cleanup_dir(pack_dir)
        with self._staged_lock:
            self._staged_bytes = max(0, self._staged_bytes - desc['staged_bytes'])
        return True

    def _ensure_remote_chunk_fits_tape(self, tape_label, planned_bytes,
                                       chunk_index):
        tape = self.db.get_tape(tape_label)
        if not tape:
            print(f"[DB] Tape '{tape_label}' is not registered.")
            return False
        used_bytes = self.db.recalculate_tape_used_space(tape_label)
        capacity_bytes = LOCAL_TAPE_BUDGET_BYTES
        if tape['total_capacity']:
            capacity_bytes = min(
                capacity_bytes, int(tape['total_capacity'] * 1024**3))
        available_bytes = capacity_bytes - used_bytes
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

        self.db.update_manifest_rows_fetching(fetching_ids, session_id=session_id)

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

            try:
                for remote_base, base_pending in pending_by_base.items():
                    if CANCEL.is_set():
                        return False, source_missing_files, fetched_file_count
                    ok, err = _remote_tar_fetch(
                        self.remote_user,
                        self.remote_host,
                        remote_base,
                        [rel for _, rel, _ in base_pending],
                        fetch_dir,
                        password=self.remote_password,
                        cipher=self.ssh_cipher,
                        use_mbuffer=self.use_mbuffer,
                        mbuffer_size=self.mbuffer_size,
                        fetch_cores=self.fetch_cores,
                        abort_evt=fetch_abort,
                    )
                    if not ok:
                        if CANCEL.is_set():
                            return False, source_missing_files, fetched_file_count
                        print(f"\n[REMOTE] Tar fetch failed:\n{err}")
                        self.db.update_manifest_rows_fetch_failed(
                            (row['manifest_id'] for row, _, _ in base_pending),
                            err,
                            session_id=session_id,
                        )
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
        self.db.update_manifest_rows_fetched(
            fetched_updates, session_id=session_id)
        fetched_file_count = len(fetched_updates)
        return True, source_missing_files, fetched_file_count

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
            try:
                shutil.rmtree(_long(path))
                print(f"[REMOTE] Cleaned: {path}")
            except OSError as e:
                print(f"[REMOTE] Warning — could not clean {path}: {e}")
