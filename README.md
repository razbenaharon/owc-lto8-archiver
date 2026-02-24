# OWC LTO-8 Archiver

A Python CLI for archiving files to LTO tape using an OWC Mercury Pro LTO-8 drive and IBM LTFS on Windows.

## Features

- **Smart packing** — small files (under a configurable threshold) are bundled into ZIP archives to minimize tape fragmentation; large files are copied directly
- **SQLite index** — every archived file is recorded with its original path, SHA-256 hash, tape label, backup date, and ZIP container (if packed)
- **Restore** — search by filename/wildcard, date range, original directory, or full backup session; restore individual files or entire sets
- **Tape management** — format, register, check, and inspect tapes via IBM LTFS command-line tools
- **Multi-tape support** — tracks multiple tapes; prompts you to swap tapes during restore when needed

## Requirements

- Windows (uses `vol`, `wmic`, and IBM LTFS executables)
- Python 3.8+
- [IBM LTFS SDE](https://www.ibm.com/support/pages/ibm-linear-tape-file-system-ltfs) installed to `C:\Program Files\IBM\LTFS\`
- OWC Mercury Pro LTO-8 (or compatible LTFS-formatted LTO drive)

The `Framework & Drivers` folder in this repo contains the installers used during setup:
- `IBM_LTFS_SDE_2.4.8.1.10519_x64.exe`
- ThunderLink SH-3128 HBA driver + release notes
- Visual C++ redistributable and .NET 4.0 (LTFS dependencies)

## Setup

1. Install IBM LTFS SDE and the HBA driver from `Framework & Drivers\`.
2. Format a tape and mount it so it appears as a drive letter (e.g. `E:\`).
3. Run the script once to generate a default `config.ini`, then edit it:

```ini
[PATHS]
source_dir  = C:\path\to\your\source\files
staging_dir = C:\path\to\staging\area
restore_dir = C:\path\to\restored\files
db_path     = C:\path\to\lto_archive.db

[HARDWARE]
lto_drive     = E:\\
ibm_eject_cmd = C:\Program Files\IBM\LTFS\LtfsCmdEject.exe

[SETTINGS]
zip_threshold_mb = 100   ; files smaller than this are packed into ZIPs
max_zip_size_gb  = 100   ; maximum size per ZIP bundle
```

## Usage

```
python lto_archive_manager.py
```

### Main Menu

| Option | Action |
|--------|--------|
| 1 | **Archive** — analyze source folder and back up to tape |
| 2 | **Retrieve** — search DB and restore files from tape |
| 3 | **Tape Maintenance** — format, register, check, or inspect tapes |
| 4 | **List Registered Tapes** — show all tapes with used/total space |
| 5 | **Open config.ini** |
| 0 | Exit |

### Archive Workflow

1. The analyzer scans your source folder and reports file-size distribution.
2. You choose **AUTO-PILOT** or **DIRECT BACKUP**:
   - **AUTO-PILOT** — files under `zip_threshold_mb` are packed into `Bundle_NNN.zip` archives in the staging directory; large files are staged as-is. The staged tree is then copied to tape.
   - **DIRECT BACKUP** — files are copied from source to tape without packing.
3. The backup runs in three phases:
   - **Hash scan** — SHA-256 each file not already on tape at the same size.
   - **Robocopy** — transfers the full directory tree to tape with live MB/s progress display (`/J` unbuffered I/O, `/R:3 /W:10` retry).
   - **DB insert** — records every file (hash, size, tape label, container) to the SQLite index.
4. After copying, a session summary is printed and the tape is ejected automatically via `LtfsCmdEject.exe`.

### Retrieve Workflow

Choose a search mode:

| Option | Search |
|--------|--------|
| 1 | Filename / wildcard (e.g. `*.mov`, `IMG_*`) |
| 2 | Date range (backed-up from / to, YYYY-MM-DD) |
| 3 | Both filename and date range |
| 4 | Restore full directory — partial path match against original paths |
| 5 | Restore full backup session — select from a dated session list |

Results are displayed in a table showing file ID, filename, size, backup date, and tape label.

- Enter a **file ID** to restore a single file.
- Enter **ALL** to restore every result.
- Enter **0** to cancel.

**Tape handling** — before each restore the script checks the mounted tape label. If the wrong tape is inserted you'll be prompted to swap it before copying begins.

**Packed files (ZIP bundles)** — files that were archived via AUTO-PILOT are stored inside `Bundle_NNN.zip` containers on tape. The restore process:
1. Copies the ZIP from tape to the staging directory via robocopy.
2. Extracts the target file(s) from the ZIP to the restore directory.
3. Deletes the staging ZIP automatically.

When restoring multiple files from the same ZIP bundle, the bundle is copied from tape only once.

**Hash verification** — after each file is restored its SHA-256 hash is verified against the value stored in the database. A `[VERIFY] OK` or `[VERIFY] FAIL` line is printed for each file.

### Tape Maintenance Sub-menu

| Option | IBM LTFS tool |
|--------|---------------|
| Format tape | `LtfsCmdFormat.exe` — **erases all data** |
| Register tape manually | DB only (for tapes already formatted) |
| List drives | `wmic logicaldisk` |
| Check tape | `LtfsCmdCheck.exe` — repair filesystem errors |
| Tape drives info | `LtfsCmdDrives.exe` — list connected drives |
| Eject tape | `LtfsCmdEject.exe` — safely eject without archiving |

## Database Schema

`lto_archive.db` (SQLite) contains two tables:

**`tapes`** — one row per tape
- `volume_label`, `date_formatted`, `total_capacity`, `used_space`

**`files_index`** — one row per file
- `file_name`, `original_path`, `file_size_bytes`, `file_hash` (SHA-256)
- `backup_date`, `tape_label`, `is_packed`, `container_name`, `stored_path`

## Important

**Run the script as Administrator.** Windows Defender exclusions are added automatically during both archive and retrieval operations (covering the LTO drive, staging directory, and restore directory) and require elevated privileges. Exclusions are removed automatically when each operation completes.
