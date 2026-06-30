# Repository Guidelines

This repository is a Windows-focused Python utility for archiving data to LTO/LTFS
tapes. It fetches data from a remote host over SSH, packs many small files into ZIP
bundles, writes them to an LTFS-mounted tape via `robocopy`, and indexes every file
in a local SQLite database.

## Project Structure & Module Organization

- `run.py` — root runner for the main CLI (chdir to the project root, then
  `src.cli.main()`).
- `inspect_db.py` — root runner for the GUI database inspector.
- `src/` — internal package holding the application code, split into modules with
  strictly downward dependencies: `constants` → `runtime` → `paths` →
  `reporting`/`config`/`db` → `robocopy`/`remote_transport` → `ltfs` → `packer`
  → `backup`/`retriever` → `orchestrators` → `cli`; `src/db_inspector_qt.py`
  holds the GUI. Data files (`config.ini`, `.env`, `lto_archive.db`, `backup_logs/`)
  stay in the project root; `src/constants.py` anchors paths to `PROJECT_ROOT`.
- `config.ini` — local paths, tape drive settings, remote archive settings, and
  performance tuning. `.env` stores secrets (e.g. `remote_password`); keep it
  untracked and use `.env.example` as the template.
- `backup_logs/` — holds the single `SUMMARY.csv`: the one statistics file for
  the whole system (backup/tape-write sessions and database-maintenance runs).
  No per-run log files and no per-file manifests are written, so it never
  contains individual file names.
- `Framework & Drivers/` — installer assets and hardware documentation.
- `lto_archive.db` — local SQLite archive index; treat as runtime data, not source.

## Build, Test, and Development Commands

```powershell
python -m pip install -r requirements.txt          # set up / update the environment
$files = @(Get-ChildItem src -Filter *.py | ForEach-Object { $_.FullName }) + @((Resolve-Path run.py).Path, (Resolve-Path inspect_db.py).Path); python -m py_compile @files  # syntax check before handoff
python run.py                                        # run the main application
python inspect_db.py                                # run the database inspector
```

## Coding Style & Naming Conventions

Python 3, four-space indentation, descriptive `snake_case` for functions, variables,
and methods. Keep the existing module shape: procedural helpers near the top, larger
workflow classes (`LTOPacker`, `LTOBackup`, `LTORetriever`, `DatabaseManager`) below.
Prefer small helpers for hardware, LTFS, robocopy, database, and path-safety
behavior. Avoid broad refactors during operational fixes.

## Testing Guidelines

Run `python -m pytest -q` and the PowerShell-expanded `py_compile` command above
after edits. Pure parsing, config, database, path-normalization, and reporting changes do
**not** require real tape hardware — validate them directly where possible. For
tape-related changes, reason carefully about hardware side effects and verify with a
small staged dataset before a full remote archive run.

## Logging & Reports

- **One statistics file** (`backup_logs/SUMMARY.csv`) holds every system
  statistic. A leading `record_type` column distinguishes the two row kinds and
  `operation` names the specific activity; columns not relevant to a row are left
  blank. No per-file manifests or robocopy stdout dumps are written — the CSV is
  file-name-free by construction.
  - `record_type=backup` rows are appended by `LTOBackup._write_backup_log` via
    `reporting.append_backup_summary_row`. Each reports `total_time_seconds`,
    `copied_bytes`, final robocopy stats, source host, tape label, and — for the
    staged/packed pipeline — per-phase timing and throughput (`fetch_seconds`,
    `pack_seconds`, `db_sync_seconds`; see Performance characteristics). Fetch and
    pack run in the producer and overlap the *previous* chunk's tape write, so
    these phases need not sum to `total_time_seconds`.
  - `record_type=maintenance` rows are appended by the database optimizers
    (`DatabaseOptimizer`, `HashlessOriginOptimizer`, `CatalogV3Optimizer`) via
    `reporting.append_maintenance_summary_row`, reporting `operation`,
    `started_at`/`finished_at`, `total_time_seconds`, and `before_bytes` /
    `after_bytes` / `reduction_pct`. These replace the former per-run
    `DB_*.json` reports.
- Main-menu option **8** ensures `backup_logs/SUMMARY.csv` exists.

## Performance Characteristics

For staged/packed runs the dominant cost is usually **not** fetch bandwidth — it
is the **Phase-3 DB sync** in `LTOBackup._run_locked`. This scales with file
*count*, not bytes: packs with more, smaller files take far longer for the same
data volume. The `db_sync_seconds` CSV field makes this visible.

Recommended future optimization (not yet implemented): batch Phase-3 inserts into a
single transaction / `executemany`, and add an index on
`(original_path, tape_label, local_session_id, local_chunk_index)` to speed the
dedup lookups.

## Commit & Pull Request Guidelines

Use concise, imperative commit messages, optionally prefixed `fix:`, `feat:`,
`refactor:`, or `chore:` (e.g. `fix: handle Windows-illegal chars in remote fetch
paths + collision guard`). PRs should state: purpose, risk level, commands run,
hardware/manual verification if relevant, and any database/config changes. For
`src/db_inspector_qt.py` UI changes, include a screenshot or short behavior description.

## Security & Operations Notes

- Never commit `.env`, real credentials, generated logs with sensitive paths, or
  large runtime databases (`lto_archive.db`).
- **During archive writes, avoid browsing the LTFS drive or starting separate copy
  jobs.** Internal tape access is serialized (`_acquire_tape_io_lock`), but external
  processes can still degrade tape throughput. This is enforced as guidance only —
  it is printed as a `[WARNING]` (`LTFS_WRITE_WARNING`, defined in
  `src/constants.py`) at the start of every tape-write run and at remote-pipeline
  start.
- **No Independent Write Verification.** Never add code that reads from the tape
  immediately after a write/copy operation just for verification purposes.
  Unnecessary reading right after writing causes wear and tear and damages the
  tape. Rely solely on copy tool success reports (for example, `robocopy`) and
  minimize tape reads as a critical architectural rule.
