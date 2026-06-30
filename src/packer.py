"""LTOAnalyzer and LTOPacker."""
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

from .constants import (BUFFER_SIZE, LOCAL_STAGING_RESERVE_BYTES,
                        LOCAL_TAPE_BUDGET_BYTES, ROOT_FILES_GROUP,
                        _auto_pack_decision)
from .robocopy import _robocopy_file
from .runtime import _progress_done, _progress_line


def _gib(value):
    return value / 1024**3


def _staging_write_overhead(required_bytes):
    return max(1 * 1024**3, int(required_bytes * 0.01))


class StagingSpaceError(RuntimeError):
    pass


def ensure_staging_space(staging_dir, required_bytes, context="staging"):
    """Refuse a staging write unless the current disk free space is safe."""
    os.makedirs(staging_dir, exist_ok=True)
    required_bytes = max(0, int(required_bytes))
    overhead = _staging_write_overhead(required_bytes)
    floor = LOCAL_STAGING_RESERVE_BYTES
    free = shutil.disk_usage(staging_dir).free
    needed = required_bytes + overhead + floor
    if free < needed:
        raise StagingSpaceError(
            f"Insufficient local staging space for {context}. "
            f"Need {_gib(required_bytes):.2f} GiB + "
            f"{_gib(overhead):.2f} GiB overhead + "
            f"{_gib(floor):.0f} GiB reserve; "
            f"current free on '{staging_dir}': {_gib(free):.2f} GiB."
        )
    return free


class LTOAnalyzer:
    def analyze(self, folder_path, threshold_mb):
        print(f"\n[ANALYZER] Scanning: {folder_path}...")

        bins = {
            "Tiny (<1MB)":       0,
            "Small (1-10MB)":    0,
            "Medium (10-100MB)": 0,
            "Large (100MB-1GB)": 0,
            "Huge (>1GB)":       0,
        }
        total_files = 0
        total_size_bytes = 0
        files_under_threshold = 0
        bytes_under_threshold = 0

        for root, _, files in os.walk(folder_path):
            for file in files:
                try:
                    size_bytes = os.path.getsize(os.path.join(root, file))
                    size_mb    = size_bytes / (1024 * 1024)
                    total_files   += 1
                    total_size_bytes += size_bytes

                    if   size_mb < 1:    bins["Tiny (<1MB)"] += 1
                    elif size_mb < 10:   bins["Small (1-10MB)"] += 1
                    elif size_mb < 100:  bins["Medium (10-100MB)"] += 1
                    elif size_mb < 1024: bins["Large (100MB-1GB)"] += 1
                    else:                bins["Huge (>1GB)"] += 1

                    if size_mb < threshold_mb:
                        files_under_threshold += 1
                        bytes_under_threshold += size_bytes
                except OSError:
                    pass

        print("-" * 60)
        print(f"REPORT | Files: {total_files} | Total Size: {total_size_bytes/1024**3:.2f} GB")
        print("-" * 60)
        for cat, count in bins.items():
            pct = (count / total_files * 100) if total_files else 0
            bar = "#" * max(int(pct / 2), 1 if count else 0)
            print(f"{cat:20} : {count:6} ({pct:5.1f}%) | {bar}")
        print("-" * 60)

        should_pack, file_ratio, byte_ratio = _auto_pack_decision(
            total_files, total_size_bytes, files_under_threshold, bytes_under_threshold
        )
        if should_pack:
            print(f">>> ANALYSIS: {file_ratio*100:.1f}% of files are under {threshold_mb:.0f} MB "
                  f"and they account for {byte_ratio*100:.2f}% of the data.")
            print(f">>> RECOMMENDATION: AUTO-PILOT (Pack files < {threshold_mb:.0f} MB)")
            return True
        else:
            print(f">>> ANALYSIS: files under {threshold_mb:.0f} MB account for "
                  f"{byte_ratio*100:.2f}% of the data.")
            print(">>> RECOMMENDATION: DIRECT BACKUP (packing is not worth staging the large files)")
            return False

    def build_local_allocation_plan(self, source_dir,
                                    budget_bytes=LOCAL_TAPE_BUDGET_BYTES,
                                    first_tape_budget_bytes=None):
        print(f"\n[ANALYZER] Building local multi-tape plan: {source_dir}")
        if not os.path.isdir(source_dir):
            raise RuntimeError(f"[ANALYZER] Source directory not found: {source_dir}")

        top_entries = {}
        root_files_size = 0
        total_files = 0
        total_bytes = 0

        for root, _, files in os.walk(source_dir):
            rel_root = os.path.relpath(root, source_dir)
            top_name = None if rel_root == '.' else rel_root.split(os.sep, 1)[0]
            for file in files:
                path = os.path.join(root, file)
                try:
                    size = os.path.getsize(path)
                except OSError as e:
                    print(f"[WARN] Cannot stat {path}: {e}")
                    continue

                if size > budget_bytes:
                    raise RuntimeError(
                        "[FATAL] Single file exceeds the 11.5 TB tape safety "
                        "limit. Spanning one file across tapes is unsupported; "
                        "manually split it with CLI utilities before archiving:\n"
                        f"        {path}\n"
                        f"        Size: {size / 1000**4:.2f} TB"
                    )

                total_files += 1
                total_bytes += size
                if top_name is None:
                    root_files_size += size
                else:
                    top_entries[top_name] = top_entries.get(top_name, 0) + size

        entries = [
            {'name': name, 'size_bytes': size}
            for name, size in top_entries.items()
        ]
        if root_files_size:
            entries.append({'name': ROOT_FILES_GROUP, 'size_bytes': root_files_size})

        if not entries:
            raise RuntimeError("[ANALYZER] No files found in source directory.")

        for entry in entries:
            if entry['size_bytes'] > budget_bytes:
                raise RuntimeError(
                    "[FATAL] Top-level directory exceeds the 11.5 TB tape "
                    "budget and cannot be split automatically:\n"
                    f"        {entry['name']} ({entry['size_bytes'] / 1000**4:.2f} TB)"
                )

        if first_tape_budget_bytes is not None and not any(
                entry['size_bytes'] <= first_tape_budget_bytes
                for entry in entries):
            raise RuntimeError(
                "[TAPE] The mounted tape's remaining DB capacity cannot fit any "
                "top-level source directory. Mount a tape with more free capacity "
                "and create the session again."
            )

        chunks = self._bin_pack_top_level(
            entries, budget_bytes,
            first_tape_budget_bytes=first_tape_budget_bytes,
        )
        print(f"[ANALYZER] Files: {total_files} | Total: {total_bytes / 1024**4:.2f} TiB")
        return chunks

    def _bin_pack_top_level(self, entries, budget_bytes,
                            first_tape_budget_bytes=None):
        chunks = []
        capacities = []
        if first_tape_budget_bytes is not None:
            chunks.append([])
            capacities.append(max(0, first_tape_budget_bytes))
        for entry in sorted(entries, key=lambda e: (-e['size_bytes'], e['name'].lower())):
            placed = False
            for chunk, capacity in zip(chunks, capacities):
                used = sum(e['size_bytes'] for e in chunk)
                if used + entry['size_bytes'] <= capacity:
                    chunk.append(entry)
                    placed = True
                    break
            if not placed:
                chunks.append([entry])
                capacities.append(budget_bytes)
        return [chunk for chunk in chunks if chunk]

    def render_allocation_plan(self, chunks,
                               budget_bytes=LOCAL_TAPE_BUDGET_BYTES,
                               first_tape_used_bytes=0,
                               first_tape_label=None):
        print("\n" + "=" * 60)
        print("LOCAL MULTI-TAPE ALLOCATION PLAN")
        print("=" * 60)
        for idx, chunk in enumerate(chunks, 1):
            planned = sum(e['size_bytes'] for e in chunk)
            occupied = first_tape_used_bytes if idx == 1 else 0
            resulting = occupied + planned
            pct = (resulting / budget_bytes * 100) if budget_bytes else 0
            label = f" ({first_tape_label})" if idx == 1 and first_tape_label else ""
            if occupied:
                print(f"Tape {idx}{label}: {planned / 1024**4:.2f} TiB planned | "
                      f"DB occupied {occupied / 1024**4:.2f} TiB | "
                      f"after archive {resulting / 1024**4:.2f} TiB "
                      f"({pct:.1f}% of 11.5 TB budget)")
            else:
                print(f"Tape {idx}{label}: {planned / 1024**4:.2f} TiB "
                      f"({pct:.1f}% of 11.5 TB budget)")
            for entry in sorted(chunk, key=lambda e: e['name'].lower()):
                print(f"  - {entry['name']}  {entry['size_bytes'] / 1024**3:.2f} GiB")
        print("-" * 60)


class LTOPacker:
    def __init__(self, max_zip_size_gb):
        self.max_zip_size_gb = max_zip_size_gb

    def run_manifest(self, source_root, dest, threshold_mb, file_entries,
                     bundle_prefix="Bundle"):
        """Pack a selected list of source files into a staging directory.

        file_entries: iterable of {'path', 'rel', 'size'} dicts, where rel is
        relative to source_root and is used for restore metadata.
        """
        if os.path.exists(dest):
            shutil.rmtree(dest)
        os.makedirs(dest, exist_ok=True)

        metadata             = []
        zip_idx              = 1
        zip_path             = os.path.join(dest, f"{bundle_prefix}_{zip_idx:03d}.zip")
        zipf                 = zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_STORED, allowZip64=True)
        current_zip_size     = 0
        files_in_current_zip = 0
        total_packed         = 0
        total_loose          = 0
        errors               = []

        print(f"\n[PACKER] Local sub-chunk staging. "
              f"(Threshold: {threshold_mb:.0f} MB | Max ZIP: {self.max_zip_size_gb:.0f} GB)")

        for entry in file_entries:
            src = entry['path']
            rel = entry['rel']
            file = os.path.basename(src)
            try:
                fsize = entry.get('size')
                if fsize is None:
                    fsize = os.path.getsize(src)
                fsize_mb = fsize / (1024 * 1024)

                if fsize_mb < threshold_mb:
                    ensure_staging_space(dest, fsize, context=file)
                    zip_rel = rel.replace('\\', '/')
                    if current_zip_size + fsize > self.max_zip_size_gb * 1024**3 * 0.99:
                        zipf.close()
                        _progress_done()
                        print(f"\n -> Sealed {bundle_prefix}_{zip_idx:03d}.zip ({files_in_current_zip} files)")
                        zip_idx += 1
                        zip_path = os.path.join(dest, f"{bundle_prefix}_{zip_idx:03d}.zip")
                        zipf = zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_STORED, allowZip64=True)
                        current_zip_size     = 0
                        files_in_current_zip = 0

                    container = f"{bundle_prefix}_{zip_idx:03d}.zip"
                    hasher = hashlib.sha256()
                    with open(src, 'rb') as fsrc, zipf.open(zip_rel, 'w', force_zip64=True) as zdst:
                        while True:
                            buf = fsrc.read(BUFFER_SIZE)
                            if not buf:
                                break
                            hasher.update(buf)
                            zdst.write(buf)
                    file_hash            = hasher.hexdigest()
                    current_zip_size     += fsize
                    files_in_current_zip += 1
                    total_packed         += 1

                    metadata.append({
                        'file_name':       file,
                        'original_path':   src,
                        'file_size_bytes': fsize,
                        'file_hash':       file_hash,
                        'is_packed':       True,
                        'container_name':  container,
                        'stored_path':     zip_rel,
                    })

                    if total_packed % 500 == 0:
                        _progress_line(f"[PACKING] {total_packed} files packed")

                else:
                    # Large/loose files are not hashed — skipping the extra
                    # full-file read keeps the tape from waiting on Python I/O.
                    dst_path = os.path.join(dest, rel)
                    ensure_staging_space(dest, fsize, context=file)

                    if not _robocopy_file(src, dst_path, display_name=file):
                        raise RuntimeError(f"robocopy failed for: {src}")
                    total_loose += 1

                    metadata.append({
                        'file_name':       file,
                        'original_path':   src,
                        'file_size_bytes': fsize,
                        'file_hash':       '',
                        'is_packed':       False,
                        'container_name':  None,
                        'stored_path':     rel,
                    })

            except StagingSpaceError:
                _progress_done()
                try:
                    zipf.close()
                except Exception:
                    pass
                raise
            except Exception as e:
                _progress_done()
                print(f"\n[ERROR] {file}: {e}")
                errors.append((src, e))

        if files_in_current_zip > 0:
            zipf.close()
            _progress_done()
            print(f"\n -> Sealed {bundle_prefix}_{zip_idx:03d}.zip ({files_in_current_zip} files)")
        else:
            zipf.close()
            if os.path.exists(zip_path) and os.path.getsize(zip_path) < 100:
                os.remove(zip_path)

        if errors:
            raise RuntimeError(
                f"{len(errors)} file(s) failed during local staging; "
                f"first failure: {errors[0][0]} ({errors[0][1]})"
            )

        _progress_done()
        print(f"\n[PACKER] Sub-chunk done: {total_packed} packed | {total_loose} loose.")
        return metadata

    def run(self, source, dest, threshold_mb):
        """
        Pack small files into ZIP bundles; copy large files loose.

        Returns:
            list of dicts  — full metadata (staged backup ready for DB)
            []             — user chose to use existing staging (no new metadata)
            None           — user aborted
        """
        if os.path.exists(dest) and os.listdir(dest):
            print(f"\n[WARNING] Staging directory is not empty: {dest}")
            print("1. Delete staging and repack from scratch")
            print("2. Use existing staged files (packed-file DB records will be skipped)")
            choice = input("Choose (1/2): ").strip()
            if choice == '2':
                print("[PACKER] Using existing staging. DB metadata for packed files will not be generated.")
                return []
            elif choice == '1':
                print("[PACKER] Cleaning staging directory...")
                shutil.rmtree(dest)
            else:
                return None

        os.makedirs(dest, exist_ok=True)

        metadata             = []
        zip_idx              = 1
        zip_path             = os.path.join(dest, f"Bundle_{zip_idx:03d}.zip")
        zipf                 = zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_STORED, allowZip64=True)
        current_zip_size     = 0
        files_in_current_zip = 0
        total_packed         = 0
        total_loose          = 0

        print(f"\n[PACKER] Offline phase — tape idle. (Threshold: {threshold_mb:.0f} MB | Max ZIP: {self.max_zip_size_gb:.0f} GB)")

        for root, _, files in os.walk(source):
            for file in files:
                src = os.path.join(root, file)
                try:
                    fsize    = os.path.getsize(src)
                    fsize_mb = fsize / (1024 * 1024)
                    rel      = os.path.relpath(src, source)

                    if fsize_mb < threshold_mb:
                        ensure_staging_space(dest, fsize, context=file)
                        zip_rel = rel.replace('\\', '/')  # ZIP entries use POSIX separators
                        # Roll over to a new ZIP bundle if current one is full
                        if current_zip_size + fsize > self.max_zip_size_gb * 1024**3 * 0.99:
                            zipf.close()
                            _progress_done()
                            print(f"\n -> Sealed Bundle_{zip_idx:03d}.zip ({files_in_current_zip} files)")
                            zip_idx += 1
                            zip_path = os.path.join(dest, f"Bundle_{zip_idx:03d}.zip")
                            zipf = zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_STORED, allowZip64=True)
                            current_zip_size     = 0
                            files_in_current_zip = 0

                        container = f"Bundle_{zip_idx:03d}.zip"
                        hasher = hashlib.sha256()
                        with open(src, 'rb') as fsrc, zipf.open(zip_rel, 'w', force_zip64=True) as zdst:
                            while True:
                                buf = fsrc.read(BUFFER_SIZE)
                                if not buf:
                                    break
                                hasher.update(buf)
                                zdst.write(buf)
                        file_hash            = hasher.hexdigest()
                        current_zip_size     += fsize
                        files_in_current_zip += 1
                        total_packed         += 1

                        metadata.append({
                            'file_name':       file,
                            'original_path':   src,
                            'file_size_bytes': fsize,
                            'file_hash':       file_hash,
                            'is_packed':       True,
                            'container_name':  container,
                            'stored_path':     zip_rel,
                        })

                        if total_packed % 500 == 0:
                            _progress_line(f"[PACKING] {total_packed} files packed")

                    else:
                        # Large/loose files are not hashed — skipping the extra
                        # full-file read keeps the tape from waiting on Python I/O.
                        dst_path = os.path.join(dest, rel)
                        ensure_staging_space(dest, fsize, context=file)

                        if not _robocopy_file(src, dst_path, display_name=file):
                            raise RuntimeError(f"robocopy failed for: {src}")
                        total_loose += 1

                        metadata.append({
                            'file_name':       file,
                            'original_path':   src,
                            'file_size_bytes': fsize,
                            'file_hash':       '',
                            'is_packed':       False,
                            'container_name':  None,
                            'stored_path':     rel,
                        })

                except StagingSpaceError:
                    _progress_done()
                    try:
                        zipf.close()
                    except Exception:
                        pass
                    raise
                except Exception as e:
                    _progress_done()
                    print(f"\n[ERROR] {file}: {e}")

        if files_in_current_zip > 0:
            zipf.close()
            _progress_done()
            print(f"\n -> Sealed Bundle_{zip_idx:03d}.zip ({files_in_current_zip} files)")
        else:
            zipf.close()
            if os.path.exists(zip_path) and os.path.getsize(zip_path) < 100:
                os.remove(zip_path)

        _progress_done()
        print(f"\n[PACKER] Offline phase done: {total_packed} packed into ZIPs | {total_loose} large files staged (not hashed).")
        return metadata
