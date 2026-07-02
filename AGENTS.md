# Repository Guidelines

This repository is a Windows-focused Python utility for archiving data to LTO/LTFS
tapes. It fetches data from a remote host over SSH, packs many small files into ZIP
bundles, writes them to an LTFS-mounted tape via `robocopy`, and indexes every file
in a PostgreSQL catalog (see `docker-compose.yml` and `scripts/sql/`).

## Project Structure & Module Organization

- `run.py` — root runner for the main CLI (chdir to the project root, then
  `src.cli.main()`).
- `inspect_db.py` — root runner for the GUI database inspector.
- `src/` — internal package holding the application code, split into modules with
  strictly downward dependencies: `constants` → `runtime` → `paths` →
  `reporting`/`config`/`db` → `robocopy`/`remote_transport` → `ltfs` → `packer`
  → `backup`/`retriever` → `orchestrators` → `cli`; `src/db_inspector_qt.py`
  holds the GUI. Data files (`config.ini`, `.env`, `backup_logs/`) stay in the
  project root; `src/constants.py` anchors paths to `PROJECT_ROOT`. The archive
  catalog itself lives in PostgreSQL, not in a repo file.
- `config.ini` — local paths, tape drive settings, remote archive settings, and
  performance tuning. `.env` stores secrets (e.g. `remote_password`); keep it
  untracked and use `.env.example` as the template.
- `backup_logs/` — holds the single `SUMMARY.csv`: the one statistics file for
  backup/tape-write sessions. No per-run log files and no per-file manifests are
  written, so it never contains individual file names.
- `Framework & Drivers/` — installer assets and hardware documentation.
- `scripts/sql/` — PostgreSQL schema/index/constraint migrations applied on
  startup by `PgDatabaseManager._init_schema`; `docker-compose.yml` runs the
  local database. Catalog rows are runtime data, not source.

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

- **One statistics file** (`backup_logs/SUMMARY.csv`) holds every backup/
  tape-write statistic. `record_type` and `operation` are always `backup`;
  columns not relevant to a row are left blank. No per-file manifests or
  robocopy stdout dumps are written — the CSV is file-name-free by construction.
  - Rows are appended by `LTOBackup._write_backup_log` via
    `reporting.append_backup_summary_row`. Each reports `total_time_seconds`,
    `copied_bytes`, final robocopy stats, source host, tape label, and — for the
    staged/packed pipeline — per-phase timing and throughput (`fetch_seconds`,
    `pack_seconds`, `db_sync_seconds`; see Performance characteristics). Fetch and
    pack run in the producer and overlap the *previous* chunk's tape write, so
    these phases need not sum to `total_time_seconds`.
- Main-menu option **8** ensures `backup_logs/SUMMARY.csv` exists.

## Performance Characteristics

For staged/packed runs the dominant cost is usually **not** fetch bandwidth — it
is the **Phase-3 DB sync** in `LTOBackup._run_locked`. This scales with file
*count*, not bytes: packs with more, smaller files take far longer for the same
data volume. The `db_sync_seconds` CSV field makes this visible.

Phase-3 inserts are batched: rows are streamed via PostgreSQL `COPY` into a temp
table and applied with a single `INSERT ... ON CONFLICT (record_key)` upsert
(`PgDatabaseManager._bulk_upsert_batch`), and the per-file directory chain is
resolved with one multi-row upsert per tree depth
(`PgDatabaseManager._ensure_directories`) instead of one round-trip per file.
Dedup lookups use the unique `record_key` index.

## Storage Map & Analytics (`storage_map.py`)

A self-contained, two-stage remote disk-usage mapper for the lab servers,
**decoupled from the tape pipeline** (it does not touch `src/cli.py`,
`lto_archive.db`, or the LTFS drive). It lives at the top of the package
dependency graph in `src/storage_map.py` with a root runner `storage_map.py`.

- **Stage 1 — `scan` (fire-and-forget).** Connects to each configured server
  over SSH (reusing `remote_transport._ssh_run`), launches a low-priority
  `ionice -c3 nice -n19 du -x -h --max-depth=2` per mount under
  `nohup`/`setsid`, then exits immediately. The scan keeps running on the server
  after the SSH session closes — no live connection is held for the ~hours-long
  `du`.
- **Stage 1.5 — `status` / `fetch`.** `status` checks each server's remote
  completion sentinel; `fetch` SCPs the finished raw log to
  `storage_map_logs/<server>_<ts>.rawlog` (+ a `<server>_latest.rawlog` pointer)
  via `remote_transport._scp_fetch_file`.
- **Stage 2 — `view` / `treemap`.** Parse a *local* raw log only (never the
  disks again): `parse_size` normalizes `du -h` units to bytes and `parse_raw_log`
  builds a Mount → user/project → sub-folder tree, rendered as a Rich terminal
  dashboard (`view`) or an interactive Plotly HTML treemap (`treemap`). `rich`
  and `plotly` are **optional** — each visualizer prints a `pip install` hint if
  its library is missing.

Mount points and servers are **config-driven, never hardcoded** — see the
`[STORAGE_MAP]` / `[STORAGE_MAP:<name>]` sections in `config.ini`
(`config.example.ini` documents them). SSH user/password default to the
`[REMOTE]` account (secret still in `.env`). Output lands in
`storage_map_logs/` (gitignored like `backup_logs/`).

Typical nightly use (Windows Task Scheduler): schedule `python storage_map.py
scan` at night and, hours later, `python storage_map.py fetch --treemap`. Unit
tests: `tests/test_storage_map.py` (pure parser + launcher-script coverage, no
hardware/network).

## Commit & Pull Request Guidelines

Use concise, imperative commit messages, optionally prefixed `fix:`, `feat:`,
`refactor:`, or `chore:` (e.g. `fix: handle Windows-illegal chars in remote fetch
paths + collision guard`). PRs should state: purpose, risk level, commands run,
hardware/manual verification if relevant, and any database/config changes. For
`src/db_inspector_qt.py` UI changes, include a screenshot or short behavior description.

## Security & Operations Notes

- Never commit `.env`, real credentials, or generated logs with sensitive paths.
  The PostgreSQL catalog lives in a Docker volume, never in the repo.
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
