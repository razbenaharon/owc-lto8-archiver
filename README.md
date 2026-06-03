# OWC LTO-8 Archiver

A Python CLI for archiving files to LTO tape using an OWC Mercury Pro LTO-8 drive and IBM LTFS on Windows.

## Features

- **Smart packing** — small files (under a configurable threshold) are bundled into ZIP archives to minimize tape fragmentation; large files are copied directly
- **SQLite index** — every archived file is recorded with its original path, SHA-256 hash, tape label, backup date, and ZIP container (if packed)
- **Restore** — search by filename/wildcard, date range, original directory, or full backup session; restore individual files or entire sets
- **Tape management** — format, register, check, and inspect tapes via IBM LTFS command-line tools
- **Multi-tape support** — tracks multiple tapes; prompts you to swap tapes during restore when needed
- **Remote archive** — fetch files from a remote SSH host into local staging, pack them, and stream them to LTO
- **Database Inspector GUI** — standalone CustomTkinter app for browsing and editing the tape/file index without touching the CLI

## Requirements

- Windows (uses `vol`, `wmic`, and IBM LTFS executables)
- Python 3.8+
- [IBM LTFS SDE](https://www.ibm.com/support/pages/ibm-linear-tape-file-system-ltfs) installed to `C:\Program Files\IBM\LTFS\`
- OWC Mercury Pro LTO-8 (or compatible LTFS-formatted LTO drive)
- OpenSSH client tools for remote archive mode
- `customtkinter` (required only for the DB Inspector GUI): `pip install customtkinter`

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

[REMOTE]
remote_host = example.host.local
remote_user = archive-user
remote_path = /path/to/remote/source
staging_fill_pct = 0.80
```

## Usage

```
# CLI (no extra dependencies)
python lto_archive_manager.py

# Database Inspector GUI (requires customtkinter)
python db_inspector.py
```

### Main Menu

| Option | Action |
|--------|--------|
| 1 | **Archive** — analyze source folder and back up to tape |
| 2 | **Retrieve** — search DB and restore files from tape |
| 3 | **Tape Maintenance** — format, register, check, or inspect tapes |
| 4 | **List Registered Tapes** — show all tapes with used/total space |
| 5 | **Open config.ini** |
| 6 | **Remote Archive** — fetch from a remote host and back up to LTO |
| 7 | **Database Management** — edit or delete tape and file records |
| 0 | Exit |

### Archive Workflow

1. The analyzer scans `source_dir`, reports file-size distribution, and builds a local multi-tape allocation plan.
2. Files under `zip_threshold_mb` are packed into session-specific ZIP bundles; large files are staged as loose files.
3. The app creates a resumable local session in SQLite. If a previous local session is active, you can resume it or abandon it.
4. Before each chunk is written, the mounted tape label is detected and assigned to that chunk.
5. New blank tapes are registered automatically. Registered non-empty LTFS tapes can also be used for append backups when both the LTFS free-space check and the database capacity check show enough room.
6. Robocopy streams the staged batch to tape with `/J` unbuffered I/O, retry settings, live progress, and tuned priority/affinity when available.
7. After copying, file records are written to SQLite, tape used-space is reconciled, and the tape is ejected automatically via `LtfsCmdEject.exe`.

If a write is interrupted, re-run option 1 and choose **Resume from first incomplete chunk**. The app skips records that are already indexed for the same local session/chunk/tape.

### Remote Archive Workflow

Option 6 scans `remote_path` over SSH, splits the remote file list into staging-sized chunks, fetches each chunk to local staging, packs it, and writes it to the selected tape. Remote sessions are resumable; if a fetch or tape write fails, re-run option 6 and resume the active session.

The remote pipeline can prefetch chunks ahead of the tape writer so the drive keeps streaming while network fetch and packing continue in the background. Tune `chunk_cap_gb`, `prefetch_chunks_ahead`, `staging_max_gb`, `robocopy_priority`, `cpu_affinity`, `ssh_cipher`, and `use_mbuffer` in the `[PERFORMANCE]` section.

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

## Database Inspector GUI

`db_inspector.py` is a standalone dark-theme GUI (CustomTkinter) for browsing and editing the SQLite archive index without using the CLI.

```
python db_inspector.py
```

**Tapes tab** — lists all registered tapes with capacity bars and file counts. Select a tape to enable:
- **Rename** — update the volume label (cascades to all file records)
- **Set Capacity** — manually set the tape's total capacity in GB
- **Recalculate Used** — recompute used space from the files_index
- **Wipe File Records** — delete all file records for the tape (tape entry kept); type the label to confirm
- **Delete Tape** — permanently remove the tape and all its file records; type the label to confirm

**Files tab** — searchable view of the `files_index` table. Filter by name fragment, tape label, and date range. Select one or more rows to **Delete Selected** or double-click a row to open a **View Details** panel showing all fields (including the full SHA-256 hash).

## Database Schema

`lto_archive.db` (SQLite) contains two tables:

**`tapes`** — one row per tape
- `volume_label`, `date_formatted`, `total_capacity`, `used_space`

**`files_index`** — one row per file
- `file_name`, `original_path`, `file_size_bytes`, `file_hash` (SHA-256)
- `backup_date`, `tape_label`, `is_packed`, `container_name`, `stored_path`
- `local_session_id`, `local_chunk_index`

The CLI also creates session tables for resumable work:

- **`local_sessions` / `local_chunks_manifest`** — local multi-tape plans and per-chunk status
- **`remote_sessions` / `remote_manifest`** — remote archive sessions, fetched files, and per-chunk status

## Important

**Run the script as Administrator.** Windows Defender exclusions are added automatically during both archive and retrieval operations (covering the LTO drive, staging directory, and restore directory) and require elevated privileges. Exclusions are removed automatically when each operation completes.
