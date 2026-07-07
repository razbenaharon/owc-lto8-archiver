# OWC LTO-8 Archiver

A Python CLI for archiving files to LTO tape using an OWC Mercury Pro LTO-8 drive and IBM LTFS on Windows.

Licensed under the [MIT License](LICENSE) — © 2026 Raz Ben Aharon. Free to use, modify, and distribute with attribution.

## Features

- **Smart packing** — small files (under a configurable threshold) are bundled into ZIP archives to minimize tape fragmentation; large files are copied directly
- **PostgreSQL catalog** — every archived file is recorded with its original path, source host, tape label, backup date, and ZIP container (if packed)
- **Restore** — search by filename/wildcard, date range, original directory, or full backup session; restore individual files or entire sets
- **Tape management** — format, register, check, and inspect tapes via IBM LTFS command-line tools
- **Multi-tape support** — tracks multiple tapes; prompts you to swap tapes during restore when needed
- **Remote archive** — fetch files from a remote SSH host into local staging, pack them, and stream them to LTO
- **Database Inspector GUI** — standalone PySide6 app for lazy browsing, searching, and managing the tape/file index without touching the CLI
- **Storage Map** - run `python storage_map/create_dashboard.py` to launch a scan, then `python storage_map/check_status_create_dashboard.py --open` later to create `storage_map/index.html`
- **Storage Map v2 (interactive)** - run `python storage_map/serve.py --open` for a local web dashboard with in-app scan/status/fetch buttons and a **tape-coverage** view matching each server's top-level directories, plus depth 2 inside `shared-data`, against the archive database

## Requirements

- Windows (uses `vol`, `wmic`, and IBM LTFS executables)
- Python 3.8+
- [IBM LTFS SDE](https://www.ibm.com/support/pages/ibm-linear-tape-file-system-ltfs) installed to `C:\Program Files\IBM\LTFS\`
- OWC Mercury Pro LTO-8 (or compatible LTFS-formatted LTO drive)
- OpenSSH client tools for remote archive mode
- PostgreSQL 17, either local via `docker compose up -d db` or an existing server
- `PySide6` (required only for the DB Inspector GUI): `pip install PySide6`

These installers are **not** bundled in this repository (they are proprietary). Download
them from the vendors and, if you wish, keep them in a local `Framework & Drivers\`
folder (gitignored):

- IBM LTFS SDE — from IBM support (link above)
- ThunderLink SH-3128 HBA driver + release notes — from ATTO
- Visual C++ redistributable and .NET Framework 4.0 — from Microsoft (LTFS dependencies)

## Setup

1. Install IBM LTFS SDE and the HBA driver (download links above).
2. Format a tape and mount it so it appears as a drive letter (e.g. `E:\`).
3. Copy `config.example.ini` to `config.ini` and edit it (your `config.ini` is
   gitignored). For remote archive, also copy `.env.example` to `.env` and set
   `PGPASSWORD` and `REMOTE_PASSWORD` there. Key fields:

```ini
[PATHS]
source_dir  = C:\path\to\your\source\files
staging_dir = C:\path\to\staging\area
restore_dir = C:\path\to\restored\files

[DATABASE]
host = localhost
port = 5432
dbname = lto_archive
user = lto
sslmode = prefer

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
python run.py

# Create a PostgreSQL catalog backup
python run.py --backup-db

# Database Inspector GUI (requires PySide6)
python inspect_db.py

# Storage Map dashboard (optional rich/plotly visualizers)
python storage_map/create_dashboard.py
python storage_map/check_status_create_dashboard.py --open

# Storage Map v2 — interactive web dashboard (requires fastapi/uvicorn)
python storage_map/serve.py --open
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
| 8 | **Backup Summary** — ensure `backup_logs/SUMMARY.csv` exists |
| 9 | **Database Backup** — dump the PostgreSQL catalog to `db_backups/` |
| 0 | Exit |

### Archive Workflow

1. The analyzer scans `source_dir`, reports file-size distribution, and builds a local multi-tape allocation plan.
2. Files under `zip_threshold_mb` are packed into session-specific ZIP bundles; large files are staged as loose files.
3. The app creates a resumable local session in PostgreSQL. If a previous local session is active, you can resume it or abandon it.
4. Before each chunk is written, the mounted tape label is detected and assigned to that chunk.
5. New blank tapes are registered automatically. Registered non-empty LTFS tapes can also be used for append backups when both the LTFS free-space check and the database capacity check show enough room.
6. Robocopy streams the staged batch to tape with `/J` unbuffered I/O, retry settings, a simple active heartbeat, and tuned priority/affinity when available.
7. After copying, file records are written to PostgreSQL, tape used-space is reconciled, and the tape is ejected automatically via `LtfsCmdEject.exe`.
8. A compact aggregate CSV row is appended to `backup_logs/SUMMARY.csv`; per-file manifests are not written to logs.

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

Results are displayed in bounded pages showing file ID, filename, size, backup date, source host, and tape label.

- Enter a **file ID** to restore a single file.
- Enter **N** / **P** to move between result pages.
- Enter **ALL** to restore every result; large result sets require typing `RESTORE ALL` to confirm.
- Enter **0** to cancel.

**Tape handling** — before each restore the script checks the mounted tape label. If the wrong tape is inserted you'll be prompted to swap it before copying begins.

**Packed files (ZIP bundles)** — files that were archived via AUTO-PILOT are stored inside `Bundle_NNN.zip` containers on tape. The restore process:
1. Copies the ZIP from tape to the staging directory via robocopy.
2. Extracts the target file(s) from the ZIP to the restore directory.
3. Deletes the staging ZIP automatically.

When restoring multiple files from the same ZIP bundle in one page, the bundle is copied from tape only once.

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

`inspect_db.py` launches a PySide6 GUI, implemented in `src/db_inspector_qt.py`, for lazy browsing, trigram search, and management of the PostgreSQL archive catalog without using the CLI.

```
python inspect_db.py
```

**Tapes tab** — lists all registered tapes with capacity bars and file counts. Select a tape to enable:
- **Rename** — update the volume label (cascades to all file records)
- **Set Capacity** — manually set the tape's total capacity in GB
- **Recalculate Used** — recompute used space from the files_index
- **Wipe File Records** — delete all file records for the tape (tape entry kept); type the label to confirm
- **Delete Tape** — permanently remove the tape and all its file records; type the label to confirm

**Files tab** — lazy tape/directory browser backed by PostgreSQL catalog indexes. Select one or more rows to **Delete Selected** or double-click a row to open a **View Details** panel showing all fields, including the source host.

**Search tab** — PostgreSQL trigram substring search over catalog names and original paths, with bounded result pages and source-host filtering.

**Manage tab** — tape and session management actions, including rename, capacity, recalculation, wipe/delete, and unused session-data cleanup.

## PostgreSQL Setup

```
docker compose up -d db
```

PostgreSQL artifacts:

- `docker-compose.yml` — Postgres 17 dev service with bulk-load tuning
- `scripts/pg_init/00_extensions.sql` — `pg_trgm` and `btree_gin`
- `scripts/sql/001_postgres_schema.sql` — normalized non-partitioned schema
- `scripts/sql/002_postgres_indexes.sql` — unique record key, browse B-trees,
  and trigram GIN search indexes
- `src/pg_bulk.py` — reusable psycopg 3 COPY/staging upsert helper

## Database Schema

PostgreSQL contains the permanent archive catalog plus normalized session
tables. Local credentials live in `.env`, which remains gitignored.

### Database Backups

Use menu option 9 or run `python run.py --backup-db` to create a PostgreSQL
custom-format dump in `db_backups/`. The helper uses the local Docker container
when available, otherwise it falls back to `pg_dump` from PostgreSQL client
tools on PATH.

**`tapes`** — one row per tape
- `volume_label`, `date_formatted`, `total_capacity`, `used_space`

**`files_index`** — one row per file
- `original_path`, `file_size_bytes`, `source_host`, `tape_label`
- `is_packed`, `stored_path`, `local_session_id`, `local_chunk_index`
- `record_key`, `archive_run_id`, `directory_id`, `catalog_name`,
  `catalog_backup_date`
- ZIP bundle and run metadata are normalized through `archive_bundles` and
  `archive_runs`.

The CLI also creates session tables for resumable work:

- **`local_sessions` / `local_chunks_manifest`** — local multi-tape plans and per-chunk status
- **`remote_sessions` / `remote_snapshots` / `remote_plans` / `remote_chunks` / `remote_file_state`** — normalized remote archive sessions, reusable source snapshots, plans, and per-file exception state

## Important

**Run the script as Administrator for best tape throughput.** When elevated, the app temporarily adds a Windows Defender process exclusion for `robocopy.exe` during archive and retrieval operations, then removes only the exclusion it added. It does not add drive, staging-directory, or restore-directory path exclusions.

## License

Copyright © 2026 Raz Ben Aharon.

Released under the [MIT License](LICENSE). You are free to use, copy, modify, and distribute this software, including for commercial purposes, provided the copyright notice and license text are retained.
