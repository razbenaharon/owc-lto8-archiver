# OWC LTO-8 Archiver — LAMS v5.0

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
max_zip_size_gb  = 20    ; maximum size per ZIP bundle
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
3. After copying, the tape is ejected automatically via `LtfsCmdEject.exe`.

### Retrieve Workflow

Search options:
1. Filename / wildcard (e.g. `*.mov`, `IMG_*`)
2. Date range
3. Both
4. Restore full directory (by original path prefix)
5. Restore full backup session

After search results are shown, enter a file ID to restore one file, or `ALL` to restore everything. If the required tape is not mounted, you'll be prompted to swap it.

### Tape Maintenance Sub-menu

| Option | IBM LTFS tool |
|--------|---------------|
| Format tape | `LtfsCmdFormat.exe` — **erases all data** |
| Register tape manually | DB only (for tapes already formatted) |
| List drives | `wmic logicaldisk` |
| Check tape | `LtfsCmdCheck.exe` — repair filesystem errors |
| Tape drives info | `LtfsCmdDrives.exe` — list connected drives |

## Database Schema

`lto_archive.db` (SQLite) contains two tables:

**`tapes`** — one row per tape
- `volume_label`, `date_formatted`, `total_capacity`, `used_space`

**`files_index`** — one row per file
- `file_name`, `original_path`, `file_size_bytes`, `file_hash` (SHA-256)
- `backup_date`, `tape_label`, `is_packed`, `container_name`, `stored_path`

## Notes

- The script hashes every file during packing/copying (SHA-256). Hashes are stored in the DB but not automatically verified on restore — manual spot-checks are recommended for long-term archival.
- Staging ZIPs use `ZIP_STORED` (no compression) to maximize tape write speed and LTO hardware compression compatibility.
- `os.fsync()` is called after each file write to ensure data is flushed before moving on.
