"""LTOBackup and _NoEjectBackup tape writers."""
import contextlib
import os
import time
import threading
from datetime import datetime
from collections import defaultdict
from typing import TYPE_CHECKING

try:
    import psutil
except ImportError:  # optional dependency; priority/affinity degrade gracefully
    psutil = None

from .constants import BACKUP_LOG_DIR, LTFS_WRITE_WARNING
from .logsetup import get_logger
from .ltfs import _ensure_lto_drive_ready, eject_tape_drive
from .ram_telemetry import RamStageSampler, TapeWriteProfiler
from .reporting import append_backup_summary_row
from .robocopy import (
    _parse_robocopy_summary, _run_robocopy_tuned, classify_robocopy_result,
    RobocopyVerdict)
from .runtime import CANCEL, _acquire_tape_io_lock, _fmt_eta, _phase, _progress_done, _progress_line, _release_tape_io_lock
from .tape_write_log import TapeWriteRawLog
from .telegram_notify import notify_backup_summary

if TYPE_CHECKING:
    from .pg_db import PgDatabaseManager


def _tape_destination_root(source, tape_drive, tape_parent_dir=None):
    """Resolve a write root, optionally nested below a session directory."""
    if not tape_parent_dir:
        return os.path.join(tape_drive, os.path.basename(source))

    parent = os.path.normpath(tape_parent_dir)
    if (os.path.isabs(parent) or os.path.splitdrive(parent)[0] or
            parent == '..' or parent.startswith('..' + os.sep)):
        raise RuntimeError(f"Unsafe tape parent directory: {tape_parent_dir}")
    return os.path.join(tape_drive, parent, os.path.basename(source))


class LTOBackup:
    def __init__(self, db: "PgDatabaseManager", ibm_eject_cmd: str,
                 tape_priority=None, tape_affinity=None, log_dir=None,
                 notifier=None, governor=None, index_min_file_mb=10):
        self.db            = db
        self.ibm_eject_cmd = ibm_eject_cmd
        self.tape_priority = tape_priority   # psutil priority class for robocopy
        self.tape_affinity = tape_affinity   # consumer (tape-writer) core set
        self.log_dir       = log_dir or BACKUP_LOG_DIR
        self.notifier      = notifier
        self.governor      = governor
        self.index_min_file_mb = index_min_file_mb

    def _write_backup_log(self, details, packer_metadata, loose_map,
                          recovered_direct_existing, skipped_existing,
                          robocopy_cmd, robocopy_result):
        """Append one compact CSV row for a completed tape-write step."""
        try:
            details = dict(details)
            details.setdefault(
                'backup_mode',
                'staged/packed' if packer_metadata is not None else 'direct')
            path = append_backup_summary_row(
                self.log_dir or BACKUP_LOG_DIR, details, robocopy_result)
            notify_backup_summary(self.notifier, details, robocopy_result)
            return path
        except Exception as e:
            get_logger().exception("could not update CSV summary")
            print(f"[REPORT] Warning: could not update CSV summary: {e}")
            return None

    def eject_tape(self, tape_drive):
        return eject_tape_drive(tape_drive, self.ibm_eject_cmd)

    def run(self, source, tape_drive, tape_label, packer_metadata=None,
            exclude_file_paths=None, exclude_dir_paths=None,
            local_session_id=None, local_chunk_index=None, stage_stats=None,
            tape_parent_dir=None, source_host='local', skipped_tracker=None,
            remote_session_id=None, remote_chunk_index=None,
            on_write_start=None):
        print(f"[WARNING] {LTFS_WRITE_WARNING}")
        _acquire_tape_io_lock(f"backup write to {tape_drive}")
        try:
            return self._run_locked(
                source, tape_drive, tape_label,
                packer_metadata=packer_metadata,
                exclude_file_paths=exclude_file_paths,
                exclude_dir_paths=exclude_dir_paths,
                local_session_id=local_session_id,
                local_chunk_index=local_chunk_index,
                stage_stats=stage_stats,
                tape_parent_dir=tape_parent_dir,
                source_host=source_host,
                skipped_tracker=skipped_tracker,
                remote_session_id=remote_session_id,
                remote_chunk_index=remote_chunk_index,
                on_write_start=on_write_start,
            )
        finally:
            _release_tape_io_lock()

    def _run_locked(self, source, tape_drive, tape_label, packer_metadata=None,
                    exclude_file_paths=None, exclude_dir_paths=None,
                    local_session_id=None, local_chunk_index=None,
                    stage_stats=None, tape_parent_dir=None,
                    source_host='local', skipped_tracker=None,
                    remote_session_id=None, remote_chunk_index=None,
                    on_write_start=None):
        """
        Copy files from source to tape and commit to the database.

        packer_metadata:
            list of dicts  - staged backup with full metadata (from LTOPacker).
            []             - staged backup, existing staging, no per-file metadata.
            None           - direct backup from source directory.
        stage_stats:
            src.pipeline_types.StagedChunk from the remote pipeline (producer
            timings + source-missing list), or None for local/direct runs.
        """
        def _stat(name, default=None):
            return getattr(stage_stats, name, default) if stage_stats else default
        def _ram_interval():
            cfg = getattr(self.governor, "cfg", None)
            return getattr(cfg, "governor_memory_sample_interval_seconds", 5)
        def _stage_ram_details():
            details = {}
            if stage_stats and getattr(stage_stats, "ram_stats", None):
                details.update(stage_stats.ram_stats)
            if self.governor:
                details.update(self.governor.telemetry_details())
            return details
        print(f"\n[BACKUP] Starting... Tape: {tape_label} | Drive: {tape_drive}")
        if not _ensure_lto_drive_ready(tape_drive, prefix="[BACKUP]"):
            raise RuntimeError("LTO drive is not ready for backup.")
        if not self.db.tape_exists(tape_label):
            raise RuntimeError(
                f"[DB] Tape '{tape_label}' is not registered; cannot sync file records."
            )
        if packer_metadata == []:
            raise RuntimeError(
                "[DB] Cannot sync staged backup without packer metadata. "
                "Repack the staging data before backing up."
            )
        if (packer_metadata and
                any(item.get("is_packed") for item in packer_metadata) and
                hasattr(self.db, "directory_catalog_schema_installed") and
                not self.db.directory_catalog_schema_installed()):
            raise RuntimeError(
                "[DB] Directory catalog indexing is enabled for packed archive "
                "runs, but the explicit directory catalog schema migration is "
                "not installed on this database. Refusing to start the tape "
                "write. Create and verify a PostgreSQL backup, restore it to a "
                "separate migrated database, then apply "
                "scripts/sql/007_postgres_directory_catalog.sql. See "
                "docs/directory_catalog_migration_runbook.md."
            )

        exclude_file_paths = exclude_file_paths or []
        exclude_dir_paths  = exclude_dir_paths or []
        tape_root = _tape_destination_root(
            source, tape_drive, tape_parent_dir=tape_parent_dir)
        os.makedirs(tape_root, exist_ok=True)

        # Build lookup: staging-relative-path -> metadata dict (loose large files only)
        meta_by_rel = {}
        if packer_metadata:
            for m in packer_metadata:
                if not m['is_packed']:
                    meta_by_rel[m['stored_path']] = m

        started_at = datetime.now()
        total_start = time.time()
        record_counts = defaultdict(int)

        # ---------------------------------------------------------------
        # Phase 1 - Build loose-file map before any tape I/O.
        #   AUTO-PILOT : consume metadata from packer_metadata.
        #   DIRECT     : walk source_dir and stage every new/changed file.
        # ---------------------------------------------------------------
        loose_map = {}
        skipped  = 0
        skipped_existing = []
        recovered_direct_existing = []

        if packer_metadata is not None:
            # AUTO-PILOT path (metadata list, possibly empty).
            # Already-on-tape (same size) files: count as skipped, omit from loose_map.
            print("[BACKUP] Staged metadata loaded.")
            for m in packer_metadata:
                if m['is_packed']:
                    continue  # bundle ZIPs handled via packer_metadata directly
                rel_path = m['stored_path']
                src      = os.path.join(source, rel_path)
                dst      = os.path.join(tape_root, rel_path)
                if os.path.exists(dst):
                    try:
                        if os.path.getsize(src) == os.path.getsize(dst):
                            skipped += 1
                            skipped_existing.append((rel_path, m, dst))
                            continue
                    except OSError:
                        pass
                loose_map[rel_path] = {
                    'fsize': m['file_size_bytes'],
                    'src':   src,
                    'dst':   dst,
                }
        else:
            # DIRECT path - files are written loose to tape; skipping an extra
            # full-file read keeps the tape from waiting.
            print("[BACKUP] Walking source files...")
            # One indexed-paths query replaces a per-file record lookup on
            # resume; the per-file fallback remains for sessionless callers.
            direct_indexed = None
            if local_session_id is not None and local_chunk_index is not None:
                direct_indexed = self.db.get_local_indexed_original_paths(
                    local_session_id, local_chunk_index, tape_label)

            def _already_indexed(path):
                if direct_indexed is not None:
                    return path in direct_indexed
                return self.db.file_record_exists(
                    path, tape_label,
                    local_session_id=local_session_id,
                    local_chunk_index=local_chunk_index,
                    source_host=source_host,
                )

            for root, _, files in os.walk(source):
                rel_folder  = os.path.relpath(root, source)
                dest_folder = os.path.join(tape_root, rel_folder)
                for file in files:
                    src      = os.path.join(root, file)
                    dst      = os.path.join(dest_folder, file)
                    rel_path = os.path.relpath(src, source)
                    if os.path.exists(dst):
                        try:
                            if os.path.getsize(src) == os.path.getsize(dst):
                                skipped += 1
                                if not _already_indexed(src):
                                    recovered_direct_existing.append({
                                        'file_name': file,
                                        'original_path': src,
                                        'file_size_bytes': os.path.getsize(src),
                                        'stored_path': dst,
                                    })
                                continue
                        except OSError:
                            pass
                    try:
                        fsize = os.path.getsize(src)
                        loose_map[rel_path] = {'fsize': fsize,
                                              'src': src, 'dst': dst}
                    except Exception as e:
                        _progress_done()
                        print(f"\n[WARN] Cannot stat {file}: {e}")
                        if skipped_tracker is not None:
                            skipped_tracker.add(
                                source_host, src, e, "backup_scan",
                                session_id=local_session_id,
                                chunk_index=local_chunk_index)

        if packer_metadata is not None:
            # Bundle ZIPs aren't in loose_map; walk staging to size the progress
            # bar and count the files robocopy is expected to move. This is the
            # source-side accounting used to sanity-check robocopy's own reported
            # statistics — it never reads the tape.
            total_bytes = 0
            expected_files = 0
            for r, _, fs in os.walk(source):
                for f in fs:
                    expected_files += 1
                    try:
                        total_bytes += os.path.getsize(os.path.join(r, f))
                    except OSError:
                        pass
        else:
            total_bytes = sum(v['fsize'] for v in loose_map.values())
            # Direct path: the loose files newly submitted to this write.
            expected_files = len(loose_map)

        _progress_done()
        print(f"[BACKUP] {len(loose_map)} loose file(s) staged "
              f"({total_bytes / 1024**3:.2f} GB to copy) | {skipped} already on tape.")

        # ---------------------------------------------------------------
        # Phase 2 - Single robocopy call: source directory to tape
        # ---------------------------------------------------------------
        prio_label = (
            (self.tape_priority is not None) and
            {getattr(psutil, 'REALTIME_PRIORITY_CLASS', None): 'REALTIME',
             getattr(psutil, 'HIGH_PRIORITY_CLASS', None): 'HIGH',
             getattr(psutil, 'NORMAL_PRIORITY_CLASS', None): 'NORMAL'}.get(
                 self.tape_priority, 'custom')
        ) or 'default'
        cores_label = (f"cores {self.tape_affinity}"
                       if self.tape_affinity else "all cores")
        _phase('TAPE', f"PC -> Tape (LTFS) | {tape_label} | "
                       f"priority={prio_label}, {cores_label}")
        print("[BACKUP] Copying to tape via robocopy...")

        stop_evt = threading.Event()

        def _monitor():
            start_time = time.time()
            while not stop_evt.wait(15):
                elapsed = time.time() - start_time
                _progress_line(
                    f"[COPYING] robocopy active | elapsed {_fmt_eta(elapsed)} | "
                    f"chunk {total_bytes / 1024**3:.1f} GB"
                )

        mon = threading.Thread(target=_monitor, daemon=True)
        mon.start()

        robocopy_cmd = [
            'robocopy', source, tape_root,
             '/E',     # recurse subdirectories including empty ones
             '/J',     # unbuffered I/O; optimised for large files / tape
             '/R:3', '/W:10',
             '/NP',    # no per-file progress %
             '/NDL',   # no directory listing lines
             '/NFL',   # no per-file listing lines (keep job header+summary)
             '/BYTES', # report byte counts as integers for reliable parsing
        ]
        if exclude_file_paths:
            robocopy_cmd.extend(['/XF'] + exclude_file_paths)
        if exclude_dir_paths:
            robocopy_cmd.extend(['/XD'] + exclude_dir_paths)

        if self.governor:
            self.governor.wait_or_pause("tape", "start")
            tape_guard = self.governor.mark_tape_write_active()
        else:
            tape_guard = contextlib.nullcontext()

        # Durable per-write raw log: robocopy's stdout/stderr is streamed to disk
        # line by line AS IT RUNS (never only after exit), so the complete
        # evidence survives a killed process, a detached-console closure, a
        # Ctrl+C, an exception, or a summary-less failure. Closed in the finally.
        # Remote pipeline writes carry only remote_* ids (local_* are None), so
        # fall back to them to keep the log identified as session_<id>/chunk_<n>.
        log_session_id = (local_session_id if local_session_id is not None
                          else remote_session_id)
        log_chunk_index = (local_chunk_index if local_chunk_index is not None
                           else remote_chunk_index)
        raw_log = TapeWriteRawLog(
            self.log_dir or BACKUP_LOG_DIR, log_session_id, log_chunk_index,
            tape_label, source, tape_root, robocopy_cmd,
            expected_files=expected_files, expected_bytes=total_bytes)
        try:
            try:
                with RamStageSampler("tape", _ram_interval()) as tape_sampler:
                    # Passive per-second profiler: reads only robocopy's own I/O
                    # counter (never the tape) to isolate open/stream/close/stalls.
                    with TapeWriteProfiler(interval_seconds=1.0) as tape_profiler:
                        with tape_guard:
                            # The write boundary: everything above (drive checks,
                            # metadata, governor wait) is pre-write. From here the
                            # tape may be touched, so signal the caller that the
                            # write has STARTED — a failure past this point is
                            # physically ambiguous (data may be partly on tape).
                            if on_write_start is not None:
                                on_write_start()
                            rc = _run_robocopy_tuned(
                                robocopy_cmd,
                                priority=self.tape_priority,
                                affinity=self.tape_affinity,
                                on_start=tape_profiler.attach,
                                raw_sink=raw_log)
            finally:
                stop_evt.set()
                mon.join(timeout=2)
                _progress_done()

            rc_sum = _parse_robocopy_summary(rc.stdout)
            rc_output = (rc.stdout or '') + '\n' + (rc.stderr or '')

            # A cooperative Ctrl+C no longer implies the write was cut: the tape
            # write is protected, so robocopy runs to completion. Therefore a
            # COMPLETED, successful write must still commit — its data is on tape
            # and skipping the commit would itself create the ambiguity we avoid.
            # Only an INCOMPLETE write under cancel (a forced kill: no robocopy
            # summary) is skipped, and it stays ambiguous ('backing') for the
            # caller. Semantics unchanged from before the raw-log/classifier work.
            if CANCEL.is_set() and not rc_sum.get('summary_found'):
                raw_log.write_footer(rc.returncode, rc_sum, RobocopyVerdict(
                    False, 'interrupted',
                    'tape write cut mid-flight before completion (cancelled)',
                    []))
                raise RuntimeError(
                    "tape write cut mid-flight before completion (cancelled)")

            # Single authoritative verdict from robocopy's OWN evidence (return
            # code, complete output, parsed summary) plus source-side accounting.
            # No tape is read. A missing/malformed summary or an unexpected
            # zero-copy result is never success even when the return code is 0.
            verdict = classify_robocopy_result(
                rc.returncode, rc_sum, rc_output,
                expected_files=expected_files, expected_bytes=total_bytes)
            raw_log.write_footer(rc.returncode, rc_sum, verdict)

            if not verdict.is_success:
                copied_bytes = rc_sum.get('bytes_copied', 0)
                new_used = self.db.recalculate_tape_used_space(tape_label)
                log_path = self._write_backup_log(
                    {
                        'status': 'failed_critical',
                        'started_at': started_at,
                        'finished_at': datetime.now(),
                        'source': source,
                        'source_host': source_host,
                        'tape_drive': tape_drive,
                        'tape_label': tape_label,
                        'tape_root': tape_root,
                        'local_session_id': local_session_id,
                        'local_chunk_index': local_chunk_index,
                        'total_time_seconds': time.time() - total_start,
                        'total_bytes': total_bytes,
                        'copied_bytes': copied_bytes,
                        'skipped': skipped,
                        'new_used': new_used,
                        'rc_sum': rc_sum,
                        'record_counts': {},
                        'source_missing_files':
                            _stat('source_missing_files', []),
                        'skipped_files_count':
                            skipped_tracker.count() if skipped_tracker else '',
                        'skipped_files_report':
                            skipped_tracker.write_csv(self.log_dir or BACKUP_LOG_DIR)
                            if skipped_tracker and skipped_tracker.has_items() else '',
                        **_stage_ram_details(),
                        **tape_sampler.as_details("tape"),
                        **tape_profiler.as_details("tape"),
                    },
                    packer_metadata,
                    loose_map,
                    recovered_direct_existing,
                    skipped_existing,
                    robocopy_cmd,
                    rc,
                )
                msg = (
                    "Robocopy result is incomplete/untrustworthy: "
                    f"{verdict.detail} (category={verdict.category}). "
                    f"return_code={rc.returncode}. "
                    "No file records were committed to the database. "
                    f"Raw log: {raw_log.path}"
                )
                if log_path:
                    msg += f" | CSV summary: {log_path}"
                print(f"[ERROR] {msg}")
                raise RuntimeError(msg)
        except BaseException as exc:
            # Any propagating failure/interrupt: annotate the durable log so the
            # evidence is never lost, then re-raise unchanged. Classification
            # failures already wrote a footer, so this only fires for unexpected
            # exceptions (launch failure, interrupt, DB error building the log).
            if not raw_log.footer_written:
                raw_log.note(
                    f"tape write aborted by {type(exc).__name__}: {exc}")
            raise
        finally:
            raw_log.close()

        # ---------------------------------------------------------------
        # Phase 3 - DB inserts (new/recovered files from this run)
        # ---------------------------------------------------------------
        if self.governor:
            self.governor.wait_or_pause("db_sync", "start")
            db_guard = self.governor.mark_db_sync_active()
        else:
            db_guard = contextlib.nullcontext()
        db_sync_start = time.perf_counter()

        def catalog_record(file_name, original_path, file_size_bytes,
                           is_packed, container_name, stored_path,
                           canonical_source_path=None):
            return {
                'file_name': file_name,
                'original_path': canonical_source_path or original_path,
                'canonical_source_path': canonical_source_path,
                'file_size_bytes': file_size_bytes,
                'tape_label': tape_label,
                'source_host': source_host,
                'is_packed': is_packed,
                'container_name': container_name,
                'stored_path': stored_path,
                'local_session_id': local_session_id,
                'local_chunk_index': local_chunk_index,
                'remote_session_id': remote_session_id,
                'remote_chunk_index': remote_chunk_index,
            }

        def _db_checkpoint():
            if self.governor:
                self.governor.wait_or_pause("db_sync", "continue")

        # The db_sync guard is held across every catalog write below, including
        # recalculate_tape_used_space, and is released by ``with`` even if a
        # bulk upsert raises; otherwise a failed sync would leave
        # db_sync_active=True and block later fetch/pack/tape work.
        with RamStageSampler("db_sync", _ram_interval()) as db_sampler:
            with db_guard:
                if packer_metadata is None:
                    _db_checkpoint()
                    recovered_stats = self.db.bulk_upsert_files(
                        (catalog_record(
                            info['file_name'], info['original_path'],
                            info['file_size_bytes'], False, None,
                            info['stored_path'])
                         for info in recovered_direct_existing),
                    )
                    recovered_count = recovered_stats['inserted']
                    record_counts['direct_recovered_inserted'] += recovered_stats['inserted']
                    record_counts['direct_recovered_updated'] += recovered_stats['updated']
                    if recovered_count:
                        print(f"[DB] Recovered {recovered_count} existing tape file record(s).")
                    _db_checkpoint()
                    loose_stats = self.db.bulk_upsert_files(
                        (catalog_record(
                            os.path.basename(info['src']), info['src'], info['fsize'],
                            False, None, info['dst'])
                         for info in loose_map.values()),
                    )
                    record_counts['loose_records_inserted'] += loose_stats['inserted']
                    record_counts['loose_records_updated'] += loose_stats['updated']

                else:
                    # Preserve resume semantics while batching all indexed lookups and
                    # writes into bounded transactions.
                    _db_checkpoint()
                    directory_stats = (
                        self.db.bulk_upsert_directory_catalog(
                            packer_metadata,
                            tape_label,
                            source_host,
                            local_session_id=local_session_id,
                            local_chunk_index=local_chunk_index,
                            remote_session_id=remote_session_id,
                            tape_root=tape_root,
                            backup_date=started_at,
                            index_min_file_mb=self.index_min_file_mb,
                        )
                        if hasattr(self.db, 'bulk_upsert_directory_catalog')
                        else {"bundles": 0, "stats": 0, "tree_rows": 0}
                    )
                    record_counts['directory_bundles_upserted'] += (
                        directory_stats['bundles'])
                    record_counts['directory_stats_upserted'] += directory_stats['stats']
                    record_counts['directory_tree_rows_upserted'] += (
                        directory_stats['tree_rows'])

                    _db_checkpoint()
                    recovered_stats = self.db.bulk_upsert_files(
                        (catalog_record(
                            m['file_name'], m['original_path'], m['file_size_bytes'],
                            False, None, dst,
                            m.get('canonical_source_path'))
                         for _, m, dst in skipped_existing),
                        update_existing=False,
                    )
                    recovered_count = recovered_stats['inserted']
                    record_counts['loose_recovered_inserted'] += recovered_stats['inserted']
                    record_counts['loose_records_skipped_existing'] += recovered_stats['skipped']

                    def loose_staged_records():
                        # loose_map is built only from is_packed=False packer metadata,
                        # so generated bundle ZIPs can never appear here; no name
                        # filter is needed (one used to silently drop legitimate user
                        # files that happened to be named "Bundle_*.zip").
                        for rel_path, info in loose_map.items():
                            file = os.path.basename(info['src'])
                            m = meta_by_rel.get(rel_path)
                            if m:
                                yield catalog_record(
                                    file, m['original_path'], info['fsize'], False,
                                    None, info['dst'],
                                    m.get('canonical_source_path'))

                    _db_checkpoint()
                    loose_stats = self.db.bulk_upsert_files(
                        loose_staged_records(), update_existing=False)
                    record_counts['loose_records_inserted'] += loose_stats['inserted']
                    record_counts['loose_records_skipped_existing'] += loose_stats['skipped']

                    print("[DB] Recording packed file entries in transaction batches...")
                    _db_checkpoint()
                    packed_stats = self.db.bulk_upsert_files(
                        (catalog_record(
                            m['file_name'], m['original_path'], m['file_size_bytes'],
                            True, os.path.join(tape_root, m['container_name']),
                            m['stored_path'], m.get('canonical_source_path'))
                         for m in packer_metadata
                         if m['is_packed']
                         and m.get('catalog_policy') != 'manifest_only'),
                        update_existing=False,
                    )
                    packed_count = packed_stats['inserted']
                    record_counts['packed_records_inserted'] += packed_stats['inserted']
                    record_counts['packed_records_skipped_existing'] += packed_stats['skipped']
                    if recovered_count:
                        print(f"[DB] Recovered {recovered_count} existing loose file record(s).")
                    print(f"[DB] {packed_count} packed file record(s) inserted; "
                          f"{packed_stats['skipped']} already indexed.")

                db_sync_seconds = time.perf_counter() - db_sync_start

                # /BYTES keeps robocopy byte counts parseable without reading LTFS
                # immediately after a write.
                copied_bytes = rc_sum['bytes_copied']
                _db_checkpoint()
                new_used = int(self.db.recalculate_tape_used_space(tape_label) or 0)
                print(f"[DB] Tape used space reconciled to {new_used / 1024**3:.3f} GB.")

        # ---------------------------------------------------------------
        # Phase 4 - Print Robocopy job summary
        # ---------------------------------------------------------------
        total_time = time.time() - total_start
        finished_at = datetime.now()
        skipped_count = skipped_tracker.count() if skipped_tracker else 0
        skipped_report = (
            skipped_tracker.write_csv(self.log_dir or BACKUP_LOG_DIR)
            if skipped_tracker and skipped_tracker.has_items() else ''
        )
        log_status = 'completed_with_skips' if skipped_count else 'completed'
        log_path = self._write_backup_log(
            {
                'status': log_status,
                'started_at': started_at,
                'finished_at': finished_at,
                'source': source,
                'source_host': source_host,
                'tape_drive': tape_drive,
                'tape_label': tape_label,
                'tape_root': tape_root,
                'local_session_id': local_session_id,
                'local_chunk_index': local_chunk_index,
                'total_time_seconds': total_time,
                'total_bytes': total_bytes,
                'copied_bytes': copied_bytes,
                'skipped': skipped,
                'new_used': new_used,
                'rc_sum': rc_sum,
                'record_counts': dict(record_counts),
                'db_sync_seconds': db_sync_seconds,
                'fetch_seconds': _stat('fetch_seconds'),
                'fetch_bytes':   _stat('fetch_bytes'),
                'pack_seconds':  _stat('pack_seconds'),
                'pack_bytes':    _stat('pack_bytes'),
                'source_missing_files':
                    _stat('source_missing_files', []),
                'skipped_files_count': skipped_count,
                'skipped_files_report': skipped_report,
                **_stage_ram_details(),
                **tape_sampler.as_details("tape"),
                **tape_profiler.as_details("tape"),
                **db_sampler.as_details("db_sync"),
            },
            packer_metadata,
            loose_map,
            recovered_direct_existing,
            skipped_existing,
            robocopy_cmd,
            rc,
        )
        print("\n" + "=" * 60)
        print("BACKUP SESSION SUMMARY  [Robocopy]")
        print("=" * 60)
        print(f"Tape            : {tape_label}")
        print(f"Total Time      : {total_time / 60:.1f} minutes")
        print(f"Data Copied     : {copied_bytes / 1024**3:.2f} GB")
        print(f"Avg Speed       : {rc_sum['speed_mbs']:.1f} MB/s")
        print(f"Files Copied    : {rc_sum['files_copied']}")
        print(f"Files Skipped   : {rc_sum['files_skipped'] + skipped}")
        print(f"Source Missing  : "
              f"{len(_stat('source_missing_files', []))}")
        print(f"Files Failed    : {rc_sum['files_failed']}")
        if rc_sum['elapsed']:
            print(f"Robocopy Time   : {rc_sum['elapsed']}")
        if log_path:
            print(f"CSV Summary     : {log_path}")
        print("-" * 60)

        # A successful write under a cooperative Ctrl+C still commits (above), but
        # do NOT physically eject on cancel — the operator is stopping the run;
        # a surprise eject of a still-loaded cartridge cannot be reloaded
        # remotely. A clean (non-cancel) completion ejects as before.
        if not CANCEL.is_set():
            self.eject_tape(tape_drive)


class _NoEjectBackup(LTOBackup):
    """LTOBackup variant that suppresses the automatic post-backup tape eject.
    RemoteOrchestrator uses this for every chunk so the tape stays mounted,
    then calls eject_tape() once explicitly after the final chunk."""
    def eject_tape(self, tape_drive):
        pass
