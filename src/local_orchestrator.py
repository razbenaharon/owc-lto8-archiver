"""LocalOrchestrator: persistent local multi-tape archive workflow."""
import os
import shutil
from datetime import datetime
from typing import Type

from .backup import LTOBackup, _NoEjectBackup
from .constants import (DEFAULT_TAPE_CAPACITY_GB, LOCAL_STAGING_RESERVE_BYTES,
                        LOCAL_TAPE_BUDGET_BYTES, ROOT_FILES_GROUP,
                        TAPE_BUDGET_LABEL, _auto_pack_decision,
                        tape_budget_bytes)
from .ltfs import _ensure_lto_drive_ready, get_volume_label
from .packer import LTOAnalyzer, LTOPacker, ensure_staging_space
from .resource_governor import ResourceGovernor
from .runtime import _acquire_tape_io_lock, _release_tape_io_lock
from .skipped import SkippedFileTracker
from .telegram_notify import TelegramNotifier
from .ui import ConsoleUI


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
        self.governor = ResourceGovernor(cfg, self.staging_dir)

    def _backup_writer(self, cls: Type[LTOBackup] = _NoEjectBackup) -> LTOBackup:
        return cls(
            self.db,
            self.cfg.ibm_eject_cmd,
            log_dir=self.cfg.backup_log_dir,
            notifier=self.notifier,
            governor=self.governor,
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
        _, available_bytes = tape_budget_bytes(
            tape['total_capacity'], used_bytes)
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
                self.governor.wait_until(
                    lambda: self.governor.can_start_pack(
                        needed_bytes=batch_bytes),
                    "local pack")
                with self.governor.mark_pack_active():
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
        _, available_bytes = tape_budget_bytes(
            tape['total_capacity'], used_bytes)
        if planned_bytes > available_bytes:
            print(f"[TAPE] '{tape_label}' does not have enough indexed "
                  f"capacity for this chunk ({planned_bytes / 1024**3:.2f} "
                  f"GiB needed, {max(0, available_bytes) / 1024**3:.2f} "
                  "GiB available in DB).")
            return False

        return True

    def _collect_chunk_files(self, source_dir, entries):
        collected = []

        def _append(path):
            # A file deleted between planning and this run should be recorded
            # as skipped, not crash the whole chunk on the stat call.
            try:
                collected.append(self._file_entry(source_dir, path))
            except OSError as e:
                print(f"[WARN] Cannot stat {path}: {e}")
                self.skipped_tracker.add('local', path, e, 'collect')

        for entry in entries:
            top = entry['top_level_dir']
            if top == ROOT_FILES_GROUP:
                try:
                    scan = list(os.scandir(source_dir))
                except OSError as e:
                    raise RuntimeError(f"Cannot scan source root: {e}")
                for item in scan:
                    if item.is_file():
                        _append(item.path)
            else:
                root = os.path.join(source_dir, top)
                for cur, _, files in os.walk(root):
                    for file in files:
                        _append(os.path.join(cur, file))
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
            self.governor.wait_until(self.governor.can_cleanup, "cleanup")
            try:
                shutil.rmtree(path)
                print(f"[LOCAL] Cleaned staging: {path}")
            except OSError as e:
                print(f"[LOCAL] Warning - could not clean {path}: {e}")
