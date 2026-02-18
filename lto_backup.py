import os
import sys
import time
import shutil
import hashlib
import zipfile
import sqlite3
import configparser
import subprocess
from datetime import datetime

# ==============================================================================
# LTO ARCHIVE MANAGEMENT SYSTEM (LAMS) v5.0
# ==============================================================================

BUFFER_SIZE = 1024 * 1024 * 16  # 16 MB read buffer
CONFIG_FILE  = "config.ini"
LTFS_DIR     = r'C:\Program Files\IBM\LTFS'  # IBM LTFS tools must run from this directory


def get_volume_label(drive_path):
    """Detect the volume label of a Windows drive (e.g. 'D:\\')."""
    try:
        drive_letter = drive_path.rstrip(":\\/")
        result = subprocess.run(
            ['vol', f'{drive_letter}:'],
            capture_output=True, text=True, shell=True
        )
        for line in result.stdout.splitlines():
            line = line.strip()
            if line.lower().startswith('volume in drive') and ' is ' in line:
                return line.rsplit(' is ', 1)[-1].strip()
    except Exception:
        pass
    return None


# ==============================================================================
# CONFIGURATION MANAGER
# ==============================================================================

class ConfigManager:
    def __init__(self, config_path=CONFIG_FILE):
        self.config      = configparser.ConfigParser()
        self.config_path = config_path

        if not os.path.exists(config_path):
            self._create_default()
            print(f"[CONFIG] Created default config file: {os.path.abspath(config_path)}")
            print("[CONFIG] Please review and edit it before running operations.")

        self.config.read(config_path, encoding='utf-8')

    def _create_default(self):
        self.config['PATHS'] = {
            'source_dir':  r'C:\Users\User\Desktop\Project_Raw',
            'staging_dir': r'C:\Users\User\Desktop\Project_Ready_For_LTO',
            'restore_dir': r'C:\Users\User\Desktop\Restored_Files',
            'db_path':     r'C:\Users\User\Desktop\lto_archive.db',
        }
        self.config['HARDWARE'] = {
            'lto_drive':     r'D:\\',
            'ibm_eject_cmd': r'C:\Program Files\IBM\LTFS\LtfsCmdEject.exe',
        }
        self.config['SETTINGS'] = {
            'zip_threshold_mb': '100',
            'max_zip_size_gb':  '100',
        }
        with open(self.config_path, 'w', encoding='utf-8') as f:
            self.config.write(f)

    @property
    def source_dir(self):    return self.config['PATHS']['source_dir']
    @property
    def staging_dir(self):   return self.config['PATHS']['staging_dir']
    @property
    def restore_dir(self):   return self.config['PATHS']['restore_dir']
    @property
    def db_path(self):       return self.config['PATHS']['db_path']
    @property
    def lto_drive(self):     return self.config['HARDWARE']['lto_drive']
    @property
    def ibm_eject_cmd(self): return self.config['HARDWARE'].get(
                                 'ibm_eject_cmd',
                                 r'C:\Program Files\IBM\LTFS\LtfsCmdEject.exe')
    @property
    def zip_threshold_mb(self): return float(self.config['SETTINGS']['zip_threshold_mb'])
    @property
    def max_zip_size_gb(self):  return float(self.config['SETTINGS']['max_zip_size_gb'])


# ==============================================================================
# DATABASE MANAGER
# ==============================================================================

class DatabaseManager:
    def __init__(self, db_path):
        db_dir = os.path.dirname(os.path.abspath(db_path))
        os.makedirs(db_dir, exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS tapes (
                tape_id        INTEGER PRIMARY KEY AUTOINCREMENT,
                volume_label   TEXT    UNIQUE NOT NULL,
                date_formatted DATETIME,
                total_capacity INTEGER
            );
            CREATE TABLE IF NOT EXISTS files_index (
                file_id         INTEGER PRIMARY KEY AUTOINCREMENT,
                file_name       TEXT,
                original_path   TEXT,
                file_size_bytes INTEGER,
                file_hash       TEXT,
                backup_date     DATETIME,
                tape_label      TEXT,
                is_packed       BOOLEAN,
                container_name  TEXT,
                stored_path     TEXT,
                FOREIGN KEY (tape_label) REFERENCES tapes(volume_label)
            );
        """)
        self.conn.commit()

    def register_tape(self, volume_label, capacity_gb=None):
        try:
            self.conn.execute(
                "INSERT INTO tapes (volume_label, date_formatted, total_capacity) VALUES (?, ?, ?)",
                (volume_label, datetime.now().isoformat(), capacity_gb)
            )
            self.conn.commit()
            print(f"[DB] Tape '{volume_label}' registered successfully.")
            return True
        except sqlite3.IntegrityError:
            print(f"[DB] Tape '{volume_label}' is already in the database.")
            return False

    def tape_exists(self, volume_label):
        return bool(self.conn.execute(
            "SELECT 1 FROM tapes WHERE volume_label = ?", (volume_label,)
        ).fetchone())

    def insert_file(self, file_name, original_path, file_size_bytes, file_hash,
                    tape_label, is_packed, container_name, stored_path):
        self.conn.execute(
            """INSERT INTO files_index
               (file_name, original_path, file_size_bytes, file_hash, backup_date,
                tape_label, is_packed, container_name, stored_path)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (file_name, original_path, file_size_bytes, file_hash,
             datetime.now().isoformat(), tape_label, is_packed, container_name, stored_path)
        )
        self.conn.commit()

    def search_files(self, name_query=None, date_from=None, date_to=None):
        sql    = "SELECT * FROM files_index WHERE 1=1"
        params = []
        if name_query:
            sql += " AND file_name LIKE ?"
            params.append(name_query.replace('*', '%').replace('?', '_'))
        if date_from:
            sql += " AND backup_date >= ?"
            params.append(date_from)
        if date_to:
            sql += " AND backup_date <= ?"
            params.append(date_to + " 23:59:59")
        sql += " ORDER BY backup_date DESC"
        return self.conn.execute(sql, params).fetchall()

    def get_file_by_id(self, file_id):
        return self.conn.execute(
            "SELECT * FROM files_index WHERE file_id = ?", (file_id,)
        ).fetchone()

    def list_tapes(self):
        return self.conn.execute(
            "SELECT * FROM tapes ORDER BY date_formatted DESC"
        ).fetchall()

    def close(self):
        self.conn.close()


# ==============================================================================
# MODULE A: ANALYZER
# ==============================================================================

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
        total_size_mb = 0
        files_under_threshold = 0

        for root, _, files in os.walk(folder_path):
            for file in files:
                try:
                    size_bytes = os.path.getsize(os.path.join(root, file))
                    size_mb    = size_bytes / (1024 * 1024)
                    total_files   += 1
                    total_size_mb += size_mb

                    if   size_mb < 1:    bins["Tiny (<1MB)"] += 1
                    elif size_mb < 10:   bins["Small (1-10MB)"] += 1
                    elif size_mb < 100:  bins["Medium (10-100MB)"] += 1
                    elif size_mb < 1024: bins["Large (100MB-1GB)"] += 1
                    else:                bins["Huge (>1GB)"] += 1

                    if size_mb < threshold_mb:
                        files_under_threshold += 1
                except OSError:
                    pass

        print("-" * 60)
        print(f"REPORT | Files: {total_files} | Total Size: {total_size_mb/1024:.2f} GB")
        print("-" * 60)
        for cat, count in bins.items():
            pct = (count / total_files * 100) if total_files else 0
            bar = "█" * max(int(pct / 2), 1 if count else 0)
            print(f"{cat:20} : {count:6} ({pct:5.1f}%) | {bar}")
        print("-" * 60)

        ratio = files_under_threshold / total_files if total_files else 0
        if ratio > 0.3:
            print(f">>> ANALYSIS: {ratio*100:.1f}% of files are under {threshold_mb:.0f} MB.")
            print(f">>> RECOMMENDATION: AUTO-PILOT (Pack files < {threshold_mb:.0f} MB)")
            return True
        else:
            print(">>> RECOMMENDATION: DIRECT BACKUP (Most files are large)")
            return False


# ==============================================================================
# MODULE B-1: SMART PACKER
# Returns a list of per-file metadata dicts for DB ingestion.
# ==============================================================================

class LTOPacker:
    def __init__(self, max_zip_size_gb):
        self.max_zip_size_gb = max_zip_size_gb

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

        print(f"\n[PACKER] Processing... (Threshold: {threshold_mb:.0f} MB | Max ZIP: {self.max_zip_size_gb:.0f} GB)")

        for root, _, files in os.walk(source):
            for file in files:
                src = os.path.join(root, file)
                try:
                    fsize    = os.path.getsize(src)
                    fsize_mb = fsize / (1024 * 1024)
                    rel      = os.path.relpath(src, source)

                    if fsize_mb < threshold_mb:
                        # Roll over to a new ZIP bundle if current one is full
                        if current_zip_size + fsize > self.max_zip_size_gb * 1024**3 * 0.99:
                            zipf.close()
                            print(f"\n -> Sealed Bundle_{zip_idx:03d}.zip ({files_in_current_zip} files)")
                            zip_idx += 1
                            zip_path = os.path.join(dest, f"Bundle_{zip_idx:03d}.zip")
                            zipf = zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_STORED, allowZip64=True)
                            current_zip_size     = 0
                            files_in_current_zip = 0

                        container = f"Bundle_{zip_idx:03d}.zip"
                        hasher = hashlib.sha256()
                        with open(src, 'rb') as fsrc, zipf.open(rel, 'w', force_zip64=True) as zdst:
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
                            'stored_path':     rel,
                        })

                        if total_packed % 500 == 0:
                            print(f"\r[PACKING] {total_packed} files packed...", end="", flush=True)

                    else:
                        dst_path = os.path.join(dest, rel)
                        os.makedirs(os.path.dirname(os.path.abspath(dst_path)), exist_ok=True)
                        shutil.copy2(src, dst_path)
                        total_loose += 1

                        metadata.append({
                            'file_name':       file,
                            'original_path':   src,
                            'file_size_bytes': fsize,
                            'is_packed':       False,
                            'container_name':  None,
                            'stored_path':     rel,
                        })

                        print(f"\r[LARGE]   {file[:55]}", end="", flush=True)

                except Exception as e:
                    print(f"\n[ERROR] {file}: {e}")

        if files_in_current_zip > 0:
            zipf.close()
            print(f"\n -> Sealed Bundle_{zip_idx:03d}.zip ({files_in_current_zip} files)")
        else:
            zipf.close()
            if os.path.exists(zip_path) and os.path.getsize(zip_path) < 100:
                os.remove(zip_path)

        print(f"\n[PACKER] Done: {total_packed} packed into ZIPs | {total_loose} large files staged.")
        return metadata


# ==============================================================================
# MODULE B-2: LTO BACKUP
# Copies staged/source files to tape and commits records to the DB.
# ==============================================================================

class LTOBackup:
    def __init__(self, db: DatabaseManager, ibm_eject_cmd: str):
        self.db           = db
        self.ibm_eject_cmd = ibm_eject_cmd

    def eject_tape(self, tape_drive):
        print("\n" + "#" * 60)
        print("[LTO] FINALIZING: Ejecting tape...")
        print("[LTO] PLEASE WAIT — this can take 1-2 minutes.")
        print("#" * 60)

        drive_arg = tape_drive.rstrip(":\\")
        exe       = os.path.join(LTFS_DIR, 'LtfsCmdEject.exe')
        cmd       = [exe, drive_arg]

        try:
            result = subprocess.run(cmd, check=True, text=True, capture_output=True,
                                    cwd=LTFS_DIR)
            print("[LTO] Tape ejected successfully!")
            if result.stdout:
                print(result.stdout)
        except subprocess.CalledProcessError as e:
            print(f"[ERROR] Eject failed: {e.stderr}")
            print(f"Try manually: cd /d \"{LTFS_DIR}\" && LtfsCmdEject.exe {drive_arg}")
        except FileNotFoundError:
            print(f"[ERROR] LtfsCmdEject.exe not found in: {LTFS_DIR}")

    def run(self, source, tape_drive, tape_label, packer_metadata=None):
        """
        Copy files from source to tape and commit to the database.

        packer_metadata:
            list of dicts  — staged backup with full metadata (from LTOPacker)
            []             — staged backup, existing staging, no per-file metadata
            None           — direct backup from source directory
        """
        print(f"\n[BACKUP] Starting... Tape: {tape_label} | Drive: {tape_drive}")

        tape_root = os.path.join(tape_drive, os.path.basename(source))
        os.makedirs(tape_root, exist_ok=True)

        # Build lookup: staging-relative-path -> metadata dict (loose large files only)
        meta_by_rel = {}
        if packer_metadata:
            for m in packer_metadata:
                if not m['is_packed']:
                    meta_by_rel[m['stored_path']] = m

        summary     = {'ok': 0, 'skip': 0, 'fail': 0, 'bytes': 0}
        total_start = time.time()

        for root, _, files in os.walk(source):
            rel_folder  = os.path.relpath(root, source)
            dest_folder = os.path.join(tape_root, rel_folder)
            os.makedirs(dest_folder, exist_ok=True)

            for file in files:
                src      = os.path.join(root, file)
                dst      = os.path.join(dest_folder, file)
                rel_path = os.path.relpath(src, source)

                # Skip if already on tape at the same size
                if os.path.exists(dst):
                    try:
                        if os.path.getsize(src) == os.path.getsize(dst):
                            print(f"\r[SKIP] {file[:55]}", end="")
                            summary['skip'] += 1
                            continue
                    except OSError:
                        pass

                try:
                    fsize  = os.path.getsize(src)
                    hasher = hashlib.sha256()
                    copied = 0
                    start  = time.time()

                    with open(src, 'rb') as fsrc, open(dst, 'wb') as fdst:
                        while True:
                            buf = fsrc.read(BUFFER_SIZE)
                            if not buf:
                                break
                            hasher.update(buf)
                            fdst.write(buf)
                            copied += len(buf)

                            elapsed = time.time() - start
                            if elapsed > 0.5:
                                speed = (copied / 1024**2) / elapsed
                                pct   = (copied / fsize * 100) if fsize else 100
                                disp  = (file[:15] + '..' + file[-5:]) if len(file) > 22 else file
                                sys.stdout.write(f"\r[WRITING] {disp} | {pct:.1f}% | {speed:.1f} MB/s")
                                sys.stdout.flush()

                        fdst.flush()
                        os.fsync(fdst.fileno())

                    file_hash = hasher.hexdigest()
                    summary['ok']    += 1
                    summary['bytes'] += fsize
                    sys.stdout.write(f"\r[OK] {file[:58]}\n")

                    # --- DB INSERT ---
                    is_bundle = file.startswith("Bundle_") and file.endswith(".zip")

                    if packer_metadata is None:
                        # Direct backup: every file is a loose tape record
                        self.db.insert_file(
                            file_name=file, original_path=src,
                            file_size_bytes=fsize, file_hash=file_hash,
                            tape_label=tape_label, is_packed=False,
                            container_name=None, stored_path=dst,
                        )

                    elif packer_metadata:
                        # Staged backup with full metadata
                        if is_bundle:
                            pass  # packed-file records handled in batch below
                        elif rel_path in meta_by_rel:
                            m = meta_by_rel[rel_path]
                            self.db.insert_file(
                                file_name=file, original_path=m['original_path'],
                                file_size_bytes=fsize, file_hash=file_hash,
                                tape_label=tape_label, is_packed=False,
                                container_name=None, stored_path=dst,
                            )
                    # else: packer_metadata == [] (existing staging) -> no DB records

                except Exception as e:
                    print(f"\n[FAIL] {file}: {e}")
                    summary['fail'] += 1

        # Batch-insert individual packed-file records
        if packer_metadata:
            print("[DB] Recording packed file entries...")
            packed_count = 0
            for m in packer_metadata:
                if m['is_packed']:
                    tape_zip_path = os.path.join(tape_root, m['container_name'])
                    self.db.insert_file(
                        file_name=m['file_name'], original_path=m['original_path'],
                        file_size_bytes=m['file_size_bytes'], file_hash=m.get('file_hash', ''),
                        tape_label=tape_label, is_packed=True,
                        container_name=tape_zip_path,
                        stored_path=m['stored_path'],
                    )
                    packed_count += 1
            print(f"[DB] {packed_count} packed file records committed.")

        total_time = time.time() - total_start
        print("\n" + "=" * 60)
        print("BACKUP SESSION SUMMARY")
        print("=" * 60)
        print(f"Tape            : {tape_label}")
        print(f"Total Time      : {total_time/60:.1f} minutes")
        print(f"Total Data      : {summary['bytes']/1024**3:.2f} GB")
        print(f"Files OK        : {summary['ok']}")
        print(f"Files Skipped   : {summary['skip']}")
        print(f"Files Failed    : {summary['fail']}")
        print("-" * 60)

        self.eject_tape(tape_drive)


# ==============================================================================
# MODULE C: RETRIEVER — Search DB & restore files from tape
# ==============================================================================

class LTORetriever:
    def __init__(self, db: DatabaseManager, tape_drive: str,
                 staging_dir: str, restore_dir: str):
        self.db          = db
        self.tape_drive  = tape_drive
        self.staging_dir = staging_dir
        self.restore_dir = restore_dir

    def run(self):
        print("\n--- RETRIEVER: Search & Restore ---")
        print("1. Search by filename / wildcard  (e.g. *.mov, IMG_*)")
        print("2. Search by date range")
        print("3. Search by both")
        opt = input("Option (1-3): ").strip()

        name_q = date_from = date_to = None

        if opt in ('1', '3'):
            name_q = input("Filename or pattern: ").strip() or None

        if opt in ('2', '3'):
            date_from = input("Backed-up from (YYYY-MM-DD, blank=any): ").strip() or None
            date_to   = input("Backed-up to   (YYYY-MM-DD, blank=any): ").strip() or None

        results = self.db.search_files(name_q, date_from, date_to)

        if not results:
            print("[RETRIEVER] No matching files found.")
            return

        print(f"\n{'ID':>7}  {'Filename':<42}  {'Size':>10}  {'Backup Date':<20}  Tape")
        print("-" * 100)
        for row in results:
            size_s = f"{row['file_size_bytes']/1024**2:.1f} MB"
            date_s = (row['backup_date'] or '')[:19]
            print(f"{row['file_id']:>7}  {row['file_name']:<42}  {size_s:>10}  {date_s:<20}  {row['tape_label']}")

        print()
        try:
            sel = int(input("Enter file ID to restore (0 = cancel): ").strip())
        except ValueError:
            print("[RETRIEVER] Invalid input.")
            return

        if sel == 0:
            return

        record = self.db.get_file_by_id(sel)
        if not record:
            print("[RETRIEVER] File ID not found.")
            return

        self._verify_tape(record['tape_label'])
        os.makedirs(self.restore_dir, exist_ok=True)

        if record['is_packed']:
            self._restore_packed(record)
        else:
            self._restore_loose(record)

    def _verify_tape(self, required_label):
        mounted = get_volume_label(self.tape_drive)
        if mounted and mounted.upper() != required_label.upper():
            print(f"\n[TAPE] Required: {required_label}  |  Currently mounted: {mounted}")
            input(f"Please insert tape '{required_label}' and press Enter to continue...")
        elif not mounted:
            print(f"\n[TAPE] Could not auto-detect tape label. Required tape: {required_label}")
            input("Ensure the correct tape is inserted, then press Enter...")

    def _restore_loose(self, record):
        src = record['stored_path']
        dst = os.path.join(self.restore_dir, record['file_name'])
        print(f"\n[RESTORE] Copying loose file: {record['file_name']}")
        try:
            shutil.copy2(src, dst)
            print(f"[RESTORE] Saved to: {dst}")
        except Exception as e:
            print(f"[ERROR] Restore failed: {e}")

    def _restore_packed(self, record):
        tape_zip_path = record['container_name']   # full path of ZIP on tape
        stored_in_zip = record['stored_path']       # relative path inside the ZIP
        local_zip     = os.path.join(self.staging_dir, os.path.basename(tape_zip_path))

        print(f"\n[RESTORE] Packed file inside {os.path.basename(tape_zip_path)}")
        print(f"[RESTORE] Step 1/3: Copying ZIP from tape to staging...")

        os.makedirs(self.staging_dir, exist_ok=True)
        try:
            shutil.copy2(tape_zip_path, local_zip)
        except Exception as e:
            print(f"[ERROR] Could not copy ZIP from tape: {e}")
            return

        print(f"[RESTORE] Step 2/3: Extracting '{record['file_name']}' from ZIP...")
        dst = os.path.join(self.restore_dir, record['file_name'])
        try:
            with zipfile.ZipFile(local_zip, 'r') as zf:
                candidates = [n for n in zf.namelist()
                              if n == stored_in_zip
                              or os.path.basename(n) == record['file_name']]
                if not candidates:
                    print(f"[ERROR] '{record['file_name']}' not found inside ZIP.")
                    print(f"        Searched stored path: {stored_in_zip}")
                    return
                with zf.open(candidates[0]) as zf_src, open(dst, 'wb') as out:
                    shutil.copyfileobj(zf_src, out)
            print(f"[RESTORE] Saved to: {dst}")
        except Exception as e:
            print(f"[ERROR] Extraction failed: {e}")
        finally:
            print("[RESTORE] Step 3/3: Removing staging ZIP...")
            try:
                os.remove(local_zip)
            except OSError:
                pass


# ==============================================================================
# MODULE D: TAPE MANAGER — Formatting & registration
# ==============================================================================

class TapeManager:
    def __init__(self, db: DatabaseManager, tape_drive: str):
        self.db         = db
        self.tape_drive = tape_drive

    def list_drives(self):
        if os.name == 'nt':
            try:
                result = subprocess.run(
                    ['wmic', 'logicaldisk', 'get', 'DeviceID,Description,VolumeName'],
                    capture_output=True, text=True
                )
                print("\n[DRIVES]\n" + result.stdout)
            except Exception as e:
                print(f"[ERROR] {e}")
        else:
            print("[INFO] Drive listing is only supported on Windows.")

    def format_tape(self):
        print("\n[TAPE MANAGER] Format / Initialize Tape")
        print(f"Target drive: {self.tape_drive}")
        print("=" * 60)
        print("WARNING: This will ERASE ALL DATA on the current tape.")
        print('Type  ERASE  to confirm (or press Enter to cancel):')
        if input(">> ").strip() != "ERASE":
            print("[ABORTED] Format cancelled.")
            return

        label = input("New Volume Label (e.g. ARCHIVE_2026_A): ").strip()
        if not label:
            print("[ABORTED] No label provided.")
            return

        drive_letter = self.tape_drive.rstrip(":\\/")
        exe          = os.path.join(LTFS_DIR, 'LtfsCmdFormat.exe')
        cmd          = [exe, drive_letter, f'/N:{label}']

        print(f"\n[FORMAT] Running: cd /d \"{LTFS_DIR}\" && LtfsCmdFormat.exe {drive_letter} /N:{label}")
        print("[FORMAT] This may take several minutes...")

        try:
            result = subprocess.run(cmd, check=True, text=True, capture_output=True,
                                    cwd=LTFS_DIR)
            print("[FORMAT] Complete.")
            if result.stdout:
                print(result.stdout)
            cap      = input("Tape capacity in GB (optional, Enter to skip): ").strip()
            capacity = int(cap) if cap.isdigit() else None
            self.db.register_tape(label, capacity)
        except subprocess.CalledProcessError as e:
            print(f"[ERROR] LtfsCmdFormat.exe failed:\n{e.stderr}")
        except FileNotFoundError:
            print(f"[ERROR] LtfsCmdFormat.exe not found in: {LTFS_DIR}")

    def register_tape(self):
        label = input("Volume label of tape to register: ").strip()
        if not label:
            return
        cap      = input("Capacity in GB (optional, Enter to skip): ").strip()
        capacity = int(cap) if cap.isdigit() else None
        self.db.register_tape(label, capacity)

    def check_tape(self):
        """Run ltfsck.exe to check and repair the tape filesystem."""
        drive_letter = self.tape_drive.rstrip(":\\/")
        exe          = os.path.join(LTFS_DIR, 'ltfsck.exe')
        cmd          = [exe, f'{drive_letter}:']
        print(f"\n[CHECK] Running: cd /d \"{LTFS_DIR}\" && ltfsck.exe {drive_letter}:")
        print("[CHECK] This may take several minutes...")
        try:
            result = subprocess.run(cmd, check=True, text=True, capture_output=True,
                                    cwd=LTFS_DIR)
            print("[CHECK] Complete.")
            if result.stdout:
                print(result.stdout)
        except subprocess.CalledProcessError as e:
            print(f"[ERROR] ltfsck.exe failed:\n{e.stderr}")
        except FileNotFoundError:
            print(f"[ERROR] ltfsck.exe not found in: {LTFS_DIR}")

    def tape_info(self):
        """Run ltfsinfo.exe to display current tape status."""
        drive_letter = self.tape_drive.rstrip(":\\/")
        exe          = os.path.join(LTFS_DIR, 'ltfsinfo.exe')
        cmd          = [exe, f'{drive_letter}:']
        print(f"\n[INFO] Running: cd /d \"{LTFS_DIR}\" && ltfsinfo.exe {drive_letter}:")
        try:
            result = subprocess.run(cmd, check=True, text=True, capture_output=True,
                                    cwd=LTFS_DIR)
            if result.stdout:
                print(result.stdout)
        except subprocess.CalledProcessError as e:
            print(f"[ERROR] ltfsinfo.exe failed:\n{e.stderr}")
        except FileNotFoundError:
            print(f"[ERROR] ltfsinfo.exe not found in: {LTFS_DIR}")


# ==============================================================================
# ARCHIVER WORKFLOW (ties together Analyzer, Packer, Backup)
# ==============================================================================

def run_archiver(cfg: ConfigManager, db: DatabaseManager):
    LTOAnalyzer().analyze(cfg.source_dir, cfg.zip_threshold_mb)

    # Identify the tape currently in the drive
    detected_label = get_volume_label(cfg.lto_drive)
    if detected_label:
        print(f"\n[TAPE] Detected label: {detected_label}")
        tape_label = detected_label
    else:
        print("[TAPE] Could not auto-detect tape label.")
        tape_label = input("Enter Volume Label manually: ").strip()

    if not tape_label:
        print("[ABORTED] No tape label provided.")
        return

    if not db.tape_exists(tape_label):
        print(f"[TAPE] '{tape_label}' is not registered in the database.")
        if input("Register now? (y/n): ").strip().lower() == 'y':
            cap = input("Capacity in GB (optional): ").strip()
            db.register_tape(tape_label, int(cap) if cap.isdigit() else None)
        else:
            print("[ABORTED] Cannot backup to an unregistered tape.")
            return

    print(f"\nBackup Mode:")
    print(f"1. AUTO-PILOT  (Pack files < {cfg.zip_threshold_mb:.0f} MB into ZIPs, then backup)")
    print("2. DIRECT BACKUP (Copy files as-is, no packing)")
    print("0. Cancel")
    mode = input("Choose: ").strip()

    backup = LTOBackup(db, cfg.ibm_eject_cmd)

    if mode == '1':
        packer   = LTOPacker(cfg.max_zip_size_gb)
        metadata = packer.run(cfg.source_dir, cfg.staging_dir, cfg.zip_threshold_mb)
        if metadata is None:
            print("[ABORTED]")
            return
        print("\n>>> Staging complete. Starting backup in 3 seconds...")
        time.sleep(3)
        backup.run(cfg.staging_dir, cfg.lto_drive, tape_label, packer_metadata=metadata)

    elif mode == '2':
        print("\n>>> Starting direct backup...")
        time.sleep(2)
        backup.run(cfg.source_dir, cfg.lto_drive, tape_label, packer_metadata=None)


# ==============================================================================
# MAIN MENU — persistent loop
# ==============================================================================

def main():
    os.system('cls' if os.name == 'nt' else 'clear')
    print("=" * 60)
    print("   LTO ARCHIVE MANAGEMENT SYSTEM (LAMS) v5.0")
    print("=" * 60)

    cfg       = ConfigManager()
    db        = DatabaseManager(cfg.db_path)
    tape_mgr  = TapeManager(db, cfg.lto_drive)
    retriever = LTORetriever(db, cfg.lto_drive, cfg.staging_dir, cfg.restore_dir)

    while True:
        print("\n" + "=" * 60)
        print("  MAIN MENU")
        print("=" * 60)
        print("  1. Archive   — Backup files to LTO tape")
        print("  2. Retrieve  — Search database & restore files")
        print("  3. Tape Maintenance — Format / Register tapes")
        print("  4. List Registered Tapes")
        print("  5. Open config.ini")
        print("  0. Exit")
        print("-" * 60)

        choice = input("Choose: ").strip()

        if choice == '1':
            run_archiver(cfg, db)

        elif choice == '2':
            retriever.run()

        elif choice == '3':
            print("\n--- Tape Maintenance ---")
            print("1. Format tape        (LtfsCmdFormat.exe — ERASES ALL DATA)")
            print("2. Register tape manually")
            print("3. List available drives")
            print("4. Check tape         (ltfsck.exe — repair filesystem errors)")
            print("5. Tape info          (ltfsinfo.exe — show tape status)")
            print("0. Back")
            sub = input("Choose: ").strip()
            if sub == '1':
                tape_mgr.format_tape()
            elif sub == '2':
                tape_mgr.register_tape()
            elif sub == '3':
                tape_mgr.list_drives()
            elif sub == '4':
                tape_mgr.check_tape()
            elif sub == '5':
                tape_mgr.tape_info()

        elif choice == '4':
            tapes = db.list_tapes()
            if not tapes:
                print("[DB] No tapes registered yet.")
            else:
                print(f"\n{'ID':>4}  {'Volume Label':<25}  {'Initialized':<20}  Capacity (GB)")
                print("-" * 65)
                for t in tapes:
                    cap_s  = str(t['total_capacity']) if t['total_capacity'] else 'N/A'
                    date_s = (t['date_formatted'] or '')[:19]
                    print(f"{t['tape_id']:>4}  {t['volume_label']:<25}  {date_s:<20}  {cap_s}")

        elif choice == '5':
            cfg_abs = os.path.abspath(CONFIG_FILE)
            print(f"\n[INFO] Config path: {cfg_abs}")
            if os.name == 'nt':
                os.startfile(cfg_abs)

        elif choice == '0':
            print("Goodbye.")
            db.close()
            break

        else:
            print("[ERROR] Invalid selection.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n[ABORTED] User stopped the script.")
