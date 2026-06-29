"""LTORetriever restore flows."""
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

from .db import DatabaseManager
from .ltfs import get_volume_label
from .paths import _verify_restored_hash
from .robocopy import _robocopy_file
from .runtime import _acquire_tape_io_lock, _release_tape_io_lock


class LTORetriever:
    def __init__(self, db: DatabaseManager, tape_drive: str,
                 staging_dir: str, restore_dir: str):
        self.db          = db
        self.tape_drive  = tape_drive
        self.staging_dir = staging_dir
        self.restore_dir = restore_dir

    def _unique_dest(self, file_name):
        """Return a restore path under restore_dir that won't overwrite an
        existing file. Distinct source files that share a basename would
        otherwise silently clobber each other when flattened into restore_dir."""
        base, ext = os.path.splitext(file_name)
        candidate = os.path.join(self.restore_dir, file_name)
        counter = 1
        while os.path.exists(candidate):
            candidate = os.path.join(self.restore_dir, f"{base}_{counter}{ext}")
            counter += 1
        return candidate

    def run(self):
        print("\n--- RETRIEVER: Search & Restore ---")
        print("1. Search by filename / wildcard  (e.g. *.mov, IMG_*)")
        print("2. Search by date range")
        print("3. Search by both")
        print("4. Restore full directory")
        print("5. Restore full backup session")
        opt = input("Option (1-5): ").strip()

        results = []

        if opt in ('1', '2', '3'):
            name_q = date_from = date_to = None
            if opt in ('1', '3'):
                name_q = input("Filename or pattern: ").strip() or None
            if opt in ('2', '3'):
                date_from = input("Backed-up from (YYYY-MM-DD, blank=any): ").strip() or None
                date_to   = input("Backed-up to   (YYYY-MM-DD, blank=any): ").strip() or None
            results = self.db.search_files(name_q, date_from, date_to)

        elif opt == '4':
            dir_q = input("Original directory path (partial ok): ").strip()
            if not dir_q:
                return
            results = self.db.search_by_directory(dir_q)

        elif opt == '5':
            sessions = self.db.list_backup_sessions()
            if not sessions:
                print("[RETRIEVER] No backup sessions found.")
                return
            print(f"\n{'#':>3}  {'Date':<12}  {'Tape':<25}  {'Files':>6}  Size")
            print("-" * 65)
            for i, s in enumerate(sessions, 1):
                size_s = f"{(s['total_bytes'] or 0) / 1024**3:.2f} GB"
                print(f"{i:>3}  {s['session_date']:<12}  {s['tape_label']:<25}  {s['file_count']:>6}  {size_s}")
            print()
            try:
                idx = int(input("Select session # (0 = cancel): ").strip())
            except ValueError:
                return
            if idx < 1 or idx > len(sessions):
                return
            s = sessions[idx - 1]
            results = self.db.search_by_session(s['session_date'], s['tape_label'])

        else:
            return

        if not results:
            print("[RETRIEVER] No matching files found.")
            return

        total_size = sum(r['file_size_bytes'] or 0 for r in results)
        print(f"\n{'ID':>7}  {'Filename':<42}  {'Size':>10}  {'Backup Date':<20}  Tape")
        print("-" * 100)
        for row in results:
            size_s = f"{row['file_size_bytes']/1024**2:.1f} MB"
            date_s = (row['backup_date'] or '')[:19]
            print(f"{row['file_id']:>7}  {row['file_name']:<42}  {size_s:>10}  {date_s:<20}  {row['tape_label']}")
        print(f"\n{len(results)} file(s)  |  {total_size/1024**3:.2f} GB total")

        print()
        sel_raw = input("Enter file ID to restore, ALL to restore all, or 0 to cancel: ").strip()

        if sel_raw == '0' or not sel_raw:
            return

        os.makedirs(self.restore_dir, exist_ok=True)

        if sel_raw.upper() == 'ALL':
            self._restore_many(list(results))
            return

        try:
            sel = int(sel_raw)
        except ValueError:
            print("[RETRIEVER] Invalid input.")
            return

        record = self.db.get_file_by_id(sel)
        if not record:
            print("[RETRIEVER] File ID not found.")
            return

        self._verify_tape(record['tape_label'])
        if record['is_packed']:
            self._restore_packed(record)
        else:
            self._restore_loose(record)

    def _restore_many(self, records):
        total = len(records)
        done  = 0

        # Group by tape so we only ask for each tape once
        by_tape = defaultdict(list)
        for r in records:
            by_tape[r['tape_label']].append(r)

        for tape_label, tape_records in by_tape.items():
            self._verify_tape(tape_label)

            loose  = [r for r in tape_records if not r['is_packed']]
            packed = [r for r in tape_records if r['is_packed']]

            for record in loose:
                self._restore_loose(record)
                done += 1
                print(f"[RESTORE] Progress: {done}/{total}")

            # Group packed files by ZIP bundle so each bundle is copied only once
            by_container = defaultdict(list)
            for r in packed:
                by_container[r['container_name']].append(r)

            for container_path, container_records in by_container.items():
                self._restore_packed_bulk(container_path, container_records)
                done += len(container_records)
                print(f"[RESTORE] Progress: {done}/{total}")

        print(f"\n[RESTORE] Complete. {total} file(s) restored to: {self.restore_dir}")

    def _verify_tape(self, required_label):
        while True:
            mounted = get_volume_label(self.tape_drive)
            if mounted and mounted.upper() == required_label.upper():
                return

            current = mounted or "not detected"
            print(f"\n[TAPE] Required: {required_label}  |  Currently mounted: {current}")
            choice = input(
                f"Insert tape '{required_label}', then press Enter to re-check "
                "or type CANCEL to abort: "
            ).strip()
            if choice.upper() == 'CANCEL':
                raise RuntimeError(
                    f"[RESTORE] Cancelled: tape '{required_label}' was not mounted.")

    def _restore_loose(self, record):
        src = record['stored_path']
        dst = self._unique_dest(record['file_name'])
        print(f"\n[RESTORE] Copying loose file: {record['file_name']}")
        _acquire_tape_io_lock(f"restore {record['file_name']}")
        try:
            ok = _robocopy_file(src, dst)
        finally:
            _release_tape_io_lock()
        if ok:
            print(f"[RESTORE] Saved to: {dst}")
            _verify_restored_hash(dst, record)
        else:
            print(f"[ERROR] Restore failed: robocopy error")

    @staticmethod
    def _resolve_zip_entry(zip_names, stored_in_zip, file_name):
        """Choose which ZIP entry to extract for a catalog record.

        Prefer the exact stored_path. Only fall back to a basename match when it
        is UNIQUE in the ZIP (returning a warning); refuse an ambiguous basename
        so a wrong same-named entry is never extracted. Returns
        (entry_name, warning, error) — entry_name is None when error is set."""
        if stored_in_zip and stored_in_zip in zip_names:
            return stored_in_zip, None, None
        base_matches = [n for n in zip_names
                        if os.path.basename(n) == file_name]
        if len(base_matches) == 1:
            warn = (f"exact stored path '{stored_in_zip}' not in ZIP; using the "
                    f"unique basename match '{base_matches[0]}'.")
            return base_matches[0], warn, None
        if not base_matches:
            return None, None, (f"'{file_name}' not found inside ZIP "
                                f"(stored path: {stored_in_zip}).")
        return None, None, (f"'{file_name}' is ambiguous inside ZIP "
                            f"({len(base_matches)} entries share this name) and "
                            f"the stored path '{stored_in_zip}' is absent — "
                            "refusing to guess.")

    def _restore_packed(self, record):
        tape_zip_path = record['container_name']   # full path of ZIP on tape
        stored_in_zip = record['stored_path']       # relative path inside the ZIP
        local_zip     = os.path.join(self.staging_dir, os.path.basename(tape_zip_path))

        print(f"\n[RESTORE] Packed file inside {os.path.basename(tape_zip_path)}")
        print(f"[RESTORE] Step 1/3: Copying ZIP from tape to staging...")

        os.makedirs(self.staging_dir, exist_ok=True)
        _acquire_tape_io_lock(f"restore {os.path.basename(tape_zip_path)}")
        try:
            ok = _robocopy_file(tape_zip_path, local_zip)
        finally:
            _release_tape_io_lock()
        if not ok:
            print(f"[ERROR] Could not copy ZIP from tape: robocopy error")
            return

        print(f"[RESTORE] Step 2/3: Extracting '{record['file_name']}' from ZIP...")
        dst = self._unique_dest(record['file_name'])
        try:
            with zipfile.ZipFile(local_zip, 'r') as zf:
                entry, warn, err = self._resolve_zip_entry(
                    zf.namelist(), stored_in_zip, record['file_name'])
                if err:
                    print(f"[ERROR] {err}")
                    return
                if warn:
                    print(f"[WARN] {warn}")
                with zf.open(entry) as zf_src, open(dst, 'wb') as out:
                    shutil.copyfileobj(zf_src, out)
            print(f"[RESTORE] Saved to: {dst}")
            _verify_restored_hash(dst, record)
        except Exception as e:
            print(f"[ERROR] Extraction failed: {e}")
        finally:
            print("[RESTORE] Step 3/3: Removing staging ZIP...")
            try:
                os.remove(local_zip)
            except OSError:
                pass

    def _restore_packed_bulk(self, tape_zip_path, records):
        """Extract multiple files from a single ZIP bundle in one pass."""
        local_zip = os.path.join(self.staging_dir, os.path.basename(tape_zip_path))
        print(f"\n[RESTORE] Copying {os.path.basename(tape_zip_path)} from tape to staging...")
        os.makedirs(self.staging_dir, exist_ok=True)
        _acquire_tape_io_lock(f"restore {os.path.basename(tape_zip_path)}")
        try:
            ok = _robocopy_file(tape_zip_path, local_zip)
        finally:
            _release_tape_io_lock()
        if not ok:
            print(f"[ERROR] Could not copy ZIP from tape: robocopy error")
            return
        print(f"[RESTORE] Extracting {len(records)} file(s)...")
        try:
            with zipfile.ZipFile(local_zip, 'r') as zf:
                zip_names = zf.namelist()
                for record in records:
                    stored_in_zip = record['stored_path']
                    dst = self._unique_dest(record['file_name'])
                    entry, warn, err = self._resolve_zip_entry(
                        zip_names, stored_in_zip, record['file_name'])
                    if err:
                        print(f"[ERROR] {err}")
                        continue
                    if warn:
                        print(f"[WARN] {warn}")
                    try:
                        with zf.open(entry) as zf_src, open(dst, 'wb') as out:
                            shutil.copyfileobj(zf_src, out)
                        print(f"[OK] {record['file_name']}")
                        _verify_restored_hash(dst, record)
                    except Exception as e:
                        print(f"[ERROR] {record['file_name']}: {e}")
        except Exception as e:
            print(f"[ERROR] ZIP extraction failed: {e}")
        finally:
            try:
                os.remove(local_zip)
            except OSError:
                pass
