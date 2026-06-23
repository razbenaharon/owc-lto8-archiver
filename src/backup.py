"""LTOBackup and _NoEjectBackup tape writers."""
import os
import re
import sys
import time
import queue
import signal
import shutil
import hashlib
import zipfile
import sqlite3
import threading
import configparser
import subprocess
import tempfile
import shlex
import posixpath
import atexit
from datetime import datetime
from collections import defaultdict

try:
    import psutil
except ImportError:  # optional dependency — priority/affinity degrade gracefully
    psutil = None

from .constants import BACKUP_LOG_DIR, LTFS_DIR, LTFS_WRITE_WARNING
from .db import DatabaseManager
from .ltfs import _ensure_lto_drive_ready
from .paths import _safe_log_token, _unique_path
from .reporting import generate_backup_summary
from .robocopy import _parse_robocopy_summary, _run_robocopy_tuned
from .runtime import CANCEL, _acquire_tape_io_lock, _fmt_bytes, _fmt_eta, _phase, _progress_done, _progress_line, _release_tape_io_lock, _speed_str


class LTOBackup:
    def __init__(self, db: DatabaseManager, ibm_eject_cmd: str,
                 tape_priority=None, tape_affinity=None, log_dir=None):
        self.db            = db
        self.ibm_eject_cmd = ibm_eject_cmd
        self.tape_priority = tape_priority   # psutil priority class for robocopy
        self.tape_affinity = tape_affinity   # consumer (tape-writer) core set
        self.log_dir       = log_dir or BACKUP_LOG_DIR

    def _write_backup_log(self, details, packer_metadata, hash_map,
                          recovered_direct_existing, skipped_existing,
                          robocopy_cmd, robocopy_result):
        """Write a reviewable text log for one completed tape-write step."""
        try:
            log_dir = os.path.abspath(self.log_dir or BACKUP_LOG_DIR)
            os.makedirs(log_dir, exist_ok=True)

            finished_at = details['finished_at']
            source_token = _safe_log_token(details.get('source'), 'source')
            tape_token = _safe_log_token(details.get('tape_label'), 'tape')
            name_parts = [finished_at.strftime('%Y%m%d_%H%M%S'),
                          tape_token, source_token]
            if details.get('local_session_id') is not None:
                name_parts.append(f"s{int(details['local_session_id']):04d}")
            if details.get('local_chunk_index') is not None:
                name_parts.append(f"c{int(details['local_chunk_index']) + 1:03d}")

            log_path = _unique_path(os.path.join(log_dir, '_'.join(name_parts) + '.log'))
            rc_sum = details.get('rc_sum') or {}
            counts = details.get('record_counts') or {}
            source_missing_files = details.get('source_missing_files') or []
            mode = 'staged/packed' if packer_metadata is not None else 'direct'

            def cell(value):
                text = '' if value is None else str(value)
                return text.replace('\t', ' ').replace('\r', ' ').replace('\n', ' ')

            def write_kv(f, key, value):
                f.write(f"{key:<28}: {value}\n")

            with open(log_path, 'w', encoding='utf-8', newline='\n') as f:
                f.write("LTO Backup Log\n")
                f.write("=" * 60 + "\n")
                write_kv(f, "Status", details.get('status', 'completed'))
                write_kv(f, "Started", details['started_at'].isoformat(timespec='seconds'))
                write_kv(f, "Finished", finished_at.isoformat(timespec='seconds'))
                write_kv(f, "Source", details.get('source'))
                write_kv(f, "Tape label", details.get('tape_label'))
                write_kv(f, "Tape drive", details.get('tape_drive'))
                write_kv(f, "Tape root", details.get('tape_root'))
                write_kv(f, "Backup mode", mode)
                if details.get('local_session_id') is not None:
                    write_kv(f, "Local session id", details.get('local_session_id'))
                if details.get('local_chunk_index') is not None:
                    write_kv(f, "Local chunk", int(details['local_chunk_index']) + 1)
                write_kv(f, "Robocopy exit code", robocopy_result.returncode)
                write_kv(f, "Robocopy command", subprocess.list2cmdline(robocopy_cmd))

                f.write("\nSummary\n")
                f.write("-" * 60 + "\n")
                write_kv(f, "Total time", f"{details['total_time_seconds'] / 60:.1f} minutes")
                write_kv(f, "Data copied", f"{_fmt_bytes(details['copied_bytes'])} ({details['copied_bytes']} bytes)")
                write_kv(f, "Planned copy size", f"{_fmt_bytes(details['total_bytes'])} ({details['total_bytes']} bytes)")
                write_kv(f, "Average speed", f"{rc_sum.get('speed_mbs', 0):.1f} MB/s")
                write_kv(f, "Files copied", rc_sum.get('files_copied', 0))
                write_kv(f, "Files skipped", rc_sum.get('files_skipped', 0) + details.get('skipped', 0))
                write_kv(f, "Files failed", rc_sum.get('files_failed', 0))
                if rc_sum.get('elapsed'):
                    write_kv(f, "Robocopy time", rc_sum.get('elapsed'))
                write_kv(f, "Loose files hashed", len(hash_map))
                write_kv(f, "Already on tape", details.get('skipped', 0))
                write_kv(f, "Source-missing files skipped",
                         len(source_missing_files))
                write_kv(f, "Tape used after backup", f"{_fmt_bytes(details['new_used'])} ({details['new_used']} bytes)")

                # Per-phase timing & throughput. Present only for the staged/packed
                # pipeline (omitted for direct/local backups). Fetch and pack run
                # in the producer and overlap the *previous* chunk's tape write, so
                # these phases need not sum to Total time.
                fetch_s = details.get('fetch_seconds')
                if fetch_s is not None:
                    fetch_b = details.get('fetch_bytes') or 0
                    write_kv(f, "Fetch time", f"{fetch_s / 60:.1f} minutes")
                    write_kv(f, "Fetched data", f"{_fmt_bytes(fetch_b)} ({fetch_b} bytes)")
                    write_kv(f, "Fetch speed (remote->PC)", _speed_str(fetch_b, fetch_s))
                pack_s = details.get('pack_seconds')
                if pack_s is not None:
                    pack_b = details.get('pack_bytes') or 0
                    write_kv(f, "Pack time", f"{pack_s / 60:.1f} minutes")
                    write_kv(f, "Pack speed", _speed_str(pack_b, pack_s))
                db_s = details.get('db_sync_seconds')
                if db_s is not None:
                    write_kv(f, "DB sync time", f"{db_s / 60:.1f} minutes")
                total_secs = details.get('total_time_seconds')
                if total_secs:
                    write_kv(f, "End-to-end speed",
                             _speed_str(details.get('copied_bytes'), total_secs, with_rate=True))
                if fetch_s is not None or pack_s is not None:
                    f.write("  (fetch/pack overlap the previous chunk's tape write; "
                            "phases need not sum to Total time)\n")

                if counts:
                    f.write("\nDatabase Records\n")
                    f.write("-" * 60 + "\n")
                    for key in sorted(counts):
                        write_kv(f, key.replace('_', ' ').title(), counts[key])

                if source_missing_files:
                    f.write("\nSource-Missing Files Skipped\n")
                    f.write("-" * 60 + "\n")
                    f.write("manifest_id\tremote_path\tsize\n")
                    for item in source_missing_files:
                        f.write('\t'.join(cell(value) for value in (
                            item.get('manifest_id'),
                            item.get('remote_path'),
                            item.get('file_size_bytes'),
                        )) + "\n")

                f.write("\nFile Manifest\n")
                f.write("-" * 60 + "\n")
                f.write("Status\tPacked\tSizeBytes\tSHA256\tOriginalPath\tTapePath\tContainer\tStoredPath\n")

                if packer_metadata is None:
                    for info in recovered_direct_existing:
                        f.write('\t'.join(cell(v) for v in (
                            'already_on_tape_recovered',
                            'no',
                            info.get('file_size_bytes'),
                            info.get('file_hash'),
                            info.get('original_path'),
                            info.get('stored_path'),
                            '',
                            info.get('stored_path'),
                        )) + "\n")
                    for rel_path, info in hash_map.items():
                        f.write('\t'.join(cell(v) for v in (
                            'copied',
                            'no',
                            info.get('fsize'),
                            info.get('hash'),
                            info.get('src'),
                            info.get('dst'),
                            '',
                            rel_path,
                        )) + "\n")
                    if details.get('skipped', 0) > len(recovered_direct_existing):
                        f.write("# Skipped files already present in the DB are counted above but not expanded here.\n")
                else:
                    skipped_originals = {
                        m.get('original_path')
                        for _, m, _ in skipped_existing
                        if isinstance(m, dict)
                    }
                    tape_root = details.get('tape_root')
                    for m in packer_metadata or []:
                        is_packed = bool(m.get('is_packed'))
                        container = m.get('container_name') if is_packed else ''
                        tape_path = (os.path.join(tape_root, container)
                                     if is_packed and container
                                     else os.path.join(tape_root, m.get('stored_path') or ''))
                        if not is_packed and m.get('original_path') in skipped_originals:
                            status = 'already_on_tape'
                        elif is_packed:
                            status = 'packed_file'
                        else:
                            status = 'copied'
                        f.write('\t'.join(cell(v) for v in (
                            status,
                            'yes' if is_packed else 'no',
                            m.get('file_size_bytes'),
                            m.get('file_hash'),
                            m.get('original_path'),
                            tape_path,
                            container,
                            m.get('stored_path'),
                        )) + "\n")

                if robocopy_result.stdout:
                    f.write("\nRobocopy Stdout\n")
                    f.write("-" * 60 + "\n")
                    f.write(robocopy_result.stdout)
                    if not robocopy_result.stdout.endswith("\n"):
                        f.write("\n")
                if robocopy_result.stderr:
                    f.write("\nRobocopy Stderr\n")
                    f.write("-" * 60 + "\n")
                    f.write(robocopy_result.stderr)
                    if not robocopy_result.stderr.endswith("\n"):
                        f.write("\n")

            return log_path
        except Exception as e:
            print(f"[LOG] Warning: could not write backup log: {e}")
            return None

    def _legacy_eject_tape_unlocked(self, tape_drive):
        print("\n" + "#" * 60)
        print("[LTO] FINALIZING: Ejecting tape...")
        print("[LTO] PLEASE WAIT — this can take 1-2 minutes.")
        print("#" * 60)

        drive_arg = tape_drive.rstrip(":\\")
        exe       = self.ibm_eject_cmd or os.path.join(LTFS_DIR, 'LtfsCmdEject.exe')
        exe_dir   = os.path.dirname(exe) or LTFS_DIR
        cmd       = [exe, drive_arg]

        try:
            result = subprocess.run(cmd, check=True, text=True, capture_output=True,
                                    cwd=exe_dir)
            print("[LTO] Tape ejected successfully!")
            if result.stdout:
                print(result.stdout)
        except subprocess.CalledProcessError as e:
            print(f"[ERROR] Eject failed: {e.stderr}")
            print(f"Try manually: cd /d \"{LTFS_DIR}\" && LtfsCmdEject.exe {drive_arg}")
        except FileNotFoundError:
            print(f"[ERROR] LtfsCmdEject.exe not found in: {LTFS_DIR}")

    def eject_tape(self, tape_drive):
        _acquire_tape_io_lock(f"eject {tape_drive}")
        try:
            return self._legacy_eject_tape_unlocked(tape_drive)
        finally:
            _release_tape_io_lock()

    def run(self, source, tape_drive, tape_label, packer_metadata=None,
            exclude_file_paths=None, exclude_dir_paths=None,
            local_session_id=None, local_chunk_index=None, stage_stats=None):
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
            )
        finally:
            _release_tape_io_lock()

    def _run_locked(self, source, tape_drive, tape_label, packer_metadata=None,
                    exclude_file_paths=None, exclude_dir_paths=None,
                    local_session_id=None, local_chunk_index=None,
                    stage_stats=None):
        """
        Copy files from source to tape and commit to the database.

        packer_metadata:
            list of dicts  — staged backup with full metadata (from LTOPacker).
                             Hashes already computed; live hashing is skipped.
            []             — staged backup, existing staging, no per-file metadata.
            None           — direct backup from source directory (files not hashed).
        """
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

        exclude_file_paths = exclude_file_paths or []
        exclude_dir_paths  = exclude_dir_paths or []
        tape_root = os.path.join(tape_drive, os.path.basename(source))
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
        # Phase 1 — Build hash_map *before* any tape I/O.
        #   AUTO-PILOT : consume pre-computed hashes from packer_metadata
        #                (packed small files only; loose large files unhashed).
        #   DIRECT     : walk source_dir and stage every new/changed file
        #                (no hashing) while the tape stays idle.
        # ---------------------------------------------------------------
        # hash_map: rel_path -> {'hash', 'fsize', 'src', 'dst'}
        hash_map = {}
        skipped  = 0
        skipped_existing = []
        recovered_direct_existing = []

        if packer_metadata is not None:
            # AUTO-PILOT path (metadata list, possibly empty).
            # Loose large files: pull hashes from packer_metadata.
            # Already-on-tape (same size) files: count as skipped, omit from hash_map.
            print("[BACKUP] Pre-hashed metadata loaded — no live hashing.")
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
                hash_map[rel_path] = {
                    'hash':  m.get('file_hash', ''),
                    'fsize': m['file_size_bytes'],
                    'src':   src,
                    'dst':   dst,
                }
        else:
            # DIRECT path — files are written loose to tape and not hashed;
            # skipping the extra full-file read keeps the tape from waiting.
            print("[BACKUP] Walking source files (no hashing)...")
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
                                if not self.db.file_record_exists(
                                    src, tape_label,
                                    local_session_id=local_session_id,
                                    local_chunk_index=local_chunk_index,
                                ):
                                    recovered_direct_existing.append({
                                        'file_name': file,
                                        'original_path': src,
                                        'file_size_bytes': os.path.getsize(src),
                                        'file_hash': '',
                                        'stored_path': dst,
                                    })
                                continue
                        except OSError:
                            pass
                    try:
                        fsize = os.path.getsize(src)
                        hash_map[rel_path] = {'hash': '', 'fsize': fsize,
                                              'src': src, 'dst': dst}
                    except Exception as e:
                        _progress_done()
                        print(f"\n[WARN] Cannot stat {file}: {e}")

        if packer_metadata is not None:
            # Bundle ZIPs aren't in hash_map; walk staging to size the progress bar.
            total_bytes = 0
            for r, _, fs in os.walk(source):
                for f in fs:
                    try:
                        total_bytes += os.path.getsize(os.path.join(r, f))
                    except OSError:
                        pass
        else:
            total_bytes = sum(v['fsize'] for v in hash_map.values())

        _progress_done()
        print(f"[BACKUP] {len(hash_map)} loose file(s) staged "
              f"({total_bytes / 1024**3:.2f} GB to copy) | {skipped} already on tape.")

        # ---------------------------------------------------------------
        # Phase 2 — Single robocopy call: source directory → tape
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
        _phase('TAPE', f"PC → Tape (LTFS) | {tape_label} | "
                       f"priority={prio_label}, {cores_label}")
        print("[BACKUP] Copying to tape via robocopy...")

        def _dir_size(path):
            total = 0
            try:
                for r, _, fs in os.walk(path):
                    for f in fs:
                        try:
                            total += os.path.getsize(os.path.join(r, f))
                        except OSError:
                            pass
            except OSError:
                pass
            return total

        initial_tape_bytes = _dir_size(tape_root)
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
             '/J',     # unbuffered I/O — optimised for large files / tape
             '/R:3', '/W:10',
             '/NP',    # no per-file progress %
             '/NDL',   # no directory listing lines
             '/NFL',   # no per-file listing lines (keep job header+summary)
        ]
        if exclude_file_paths:
            robocopy_cmd.extend(['/XF'] + exclude_file_paths)
        if exclude_dir_paths:
            robocopy_cmd.extend(['/XD'] + exclude_dir_paths)

        rc = _run_robocopy_tuned(robocopy_cmd,
                                 priority=self.tape_priority,
                                 affinity=self.tape_affinity)

        stop_evt.set()
        mon.join(timeout=2)
        _progress_done()

        # If the user pressed Ctrl+C, robocopy was terminated mid-write. Skip the
        # DB commit and the eject so the chunk stays resumable and the tape is
        # left mounted for the next run.
        if CANCEL.is_set():
            raise RuntimeError("tape write cancelled by user")

        rc_sum = _parse_robocopy_summary(rc.stdout)

        rc_output = (rc.stdout or '') + '\n' + (rc.stderr or '')
        critical_robocopy_failure = (
            rc.returncode >= 8 or
            rc_sum.get('files_failed', 0) > 0 or
            'ERROR ' in rc_output or
            'RETRY LIMIT EXCEEDED' in rc_output
        )
        if critical_robocopy_failure:
            copied_bytes = rc_sum.get('bytes_copied', 0)
            if copied_bytes <= 0:
                copied_bytes = max(0, _dir_size(tape_root) - initial_tape_bytes)
            new_used = self.db.recalculate_tape_used_space(tape_label)
            log_path = self._write_backup_log(
                {
                    'status': 'failed_critical',
                    'started_at': started_at,
                    'finished_at': datetime.now(),
                    'source': source,
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
                        (stage_stats or {}).get('source_missing_files', []),
                },
                packer_metadata,
                hash_map,
                recovered_direct_existing,
                skipped_existing,
                robocopy_cmd,
                rc,
            )
            msg = (
                f"CRITICAL: robocopy failed with exit code {rc.returncode}; "
                f"{rc_sum.get('files_failed', 0)} file(s) failed. "
                "No file records were committed to the database."
            )
            if log_path:
                msg += f" Log: {log_path}"
            print(f"[ERROR] {msg}")
            raise RuntimeError(msg)

        # ---------------------------------------------------------------
        # Phase 3 — DB inserts (only files that were hashed / new this run)
        # ---------------------------------------------------------------
        db_sync_start = time.perf_counter()

        def catalog_record(file_name, original_path, file_size_bytes, file_hash,
                           is_packed, container_name, stored_path):
            return {
                'file_name': file_name,
                'original_path': original_path,
                'file_size_bytes': file_size_bytes,
                'file_hash': file_hash,
                'tape_label': tape_label,
                'is_packed': is_packed,
                'container_name': container_name,
                'stored_path': stored_path,
                'local_session_id': local_session_id,
                'local_chunk_index': local_chunk_index,
            }

        if packer_metadata is None:
            recovered_stats = self.db.bulk_upsert_files(
                (catalog_record(
                    info['file_name'], info['original_path'],
                    info['file_size_bytes'], info['file_hash'], False, None,
                    info['stored_path'])
                 for info in recovered_direct_existing),
            )
            recovered_count = recovered_stats['inserted']
            record_counts['direct_recovered_inserted'] += recovered_stats['inserted']
            record_counts['direct_recovered_updated'] += recovered_stats['updated']
            if recovered_count:
                print(f"[DB] Recovered {recovered_count} existing tape file record(s).")
            loose_stats = self.db.bulk_upsert_files(
                (catalog_record(
                    os.path.basename(info['src']), info['src'], info['fsize'],
                    info['hash'], False, None, info['dst'])
                 for info in hash_map.values()),
            )
            record_counts['loose_records_inserted'] += loose_stats['inserted']
            record_counts['loose_records_updated'] += loose_stats['updated']

        else:
            # Preserve resume semantics while batching all indexed lookups and
            # writes into bounded transactions.
            recovered_stats = self.db.bulk_upsert_files(
                (catalog_record(
                    m['file_name'], m['original_path'], m['file_size_bytes'],
                    m.get('file_hash', ''), False, None, dst)
                 for _, m, dst in skipped_existing),
                update_existing=False,
            )
            recovered_count = recovered_stats['inserted']
            record_counts['loose_recovered_inserted'] += recovered_stats['inserted']
            record_counts['loose_records_skipped_existing'] += recovered_stats['skipped']

            def loose_staged_records():
                for rel_path, info in hash_map.items():
                    file = os.path.basename(info['src'])
                    if file.startswith("Bundle_") and file.endswith(".zip"):
                        continue
                    m = meta_by_rel.get(rel_path)
                    if m:
                        yield catalog_record(
                            file, m['original_path'], info['fsize'], info['hash'],
                            False, None, info['dst'])

            loose_stats = self.db.bulk_upsert_files(
                loose_staged_records(), update_existing=False)
            record_counts['loose_records_inserted'] += loose_stats['inserted']
            record_counts['loose_records_skipped_existing'] += loose_stats['skipped']

            print("[DB] Recording packed file entries in transaction batches...")
            packed_stats = self.db.bulk_upsert_files(
                (catalog_record(
                    m['file_name'], m['original_path'], m['file_size_bytes'],
                    m.get('file_hash', ''), True,
                    os.path.join(tape_root, m['container_name']),
                    m['stored_path'])
                 for m in packer_metadata if m['is_packed']),
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

        # The robocopy summary parser is English-only; on a localized console
        # it yields 0 bytes. Fall back to the measured growth of the tape
        # directory so used-space accounting still works.
        copied_bytes = rc_sum['bytes_copied']
        if copied_bytes <= 0:
            copied_bytes = max(0, _dir_size(tape_root) - initial_tape_bytes)
        new_used = self.db.recalculate_tape_used_space(tape_label)
        print(f"[DB] Tape used space reconciled to {new_used / 1024**3:.3f} GB.")

        # ---------------------------------------------------------------
        # Phase 4 — Print Robocopy job summary
        # ---------------------------------------------------------------
        total_time = time.time() - total_start
        finished_at = datetime.now()
        # Reaching here means robocopy did not fail critically: returncode < 8
        # and files_failed == 0 both raised earlier, so the status is always
        # 'completed' (the old 'completed_with_warnings' branch was unreachable).
        log_status = 'completed'
        log_path = self._write_backup_log(
            {
                'status': log_status,
                'started_at': started_at,
                'finished_at': finished_at,
                'source': source,
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
                'fetch_seconds': (stage_stats or {}).get('fetch_seconds'),
                'fetch_bytes':   (stage_stats or {}).get('fetch_bytes'),
                'pack_seconds':  (stage_stats or {}).get('pack_seconds'),
                'pack_bytes':    (stage_stats or {}).get('pack_bytes'),
                'source_missing_files':
                    (stage_stats or {}).get('source_missing_files', []),
            },
            packer_metadata,
            hash_map,
            recovered_direct_existing,
            skipped_existing,
            robocopy_cmd,
            rc,
        )
        try:
            summary_path = generate_backup_summary(self.log_dir or BACKUP_LOG_DIR)
            if summary_path:
                print(f"[REPORT] Backup summary updated: {summary_path}")
        except Exception as e:
            print(f"[REPORT] Could not update backup summary: {e}")
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
              f"{len((stage_stats or {}).get('source_missing_files', []))}")
        print(f"Files Failed    : {rc_sum['files_failed']}")
        if rc_sum['elapsed']:
            print(f"Robocopy Time   : {rc_sum['elapsed']}")
        if log_path:
            print(f"Backup Log      : {log_path}")
        print("-" * 60)

        self.eject_tape(tape_drive)


class _NoEjectBackup(LTOBackup):
    """LTOBackup variant that suppresses the automatic post-backup tape eject.
    RemoteOrchestrator uses this for every chunk so the tape stays mounted,
    then calls eject_tape() once explicitly after the final chunk."""
    def eject_tape(self, tape_drive):
        pass
