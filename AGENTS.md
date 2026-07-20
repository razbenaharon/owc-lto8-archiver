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
  strictly downward dependencies: `constants`/`pipeline_types` → `logsetup` →
  `runtime` → `paths` → `reporting`/`config`/`db` →
  `robocopy`/`remote_transport` → `ltfs` → `packer` → `scanning`/`planning` →
  `backup`/`retriever` → `local_orchestrator`/`remote_orchestrator` →
  `orchestrators` (re-export facade) → `cli`; `src/db_inspector_qt.py` holds
  the GUI. The PostgreSQL layer is split the same way: `pg_bulk` → `pg_core`
  → `pg_catalog`/`pg_sessions`/`pg_tapes` → `pg_db` (facade assembling
  `PgDatabaseManager` from the mixins). Import the facades (`orchestrators`,
  `pg_db`) from application code; in tests, `mock.patch` targets must name the
  module a symbol is *used* in (e.g. `src.scanning._ssh_run`). Data files
  (`config.ini`, `.env`, `backup_logs/`) stay in the project root;
  `src/constants.py` anchors paths to `PROJECT_ROOT`. The archive catalog
  itself lives in PostgreSQL, not in a repo file.
- `config.ini` — local paths, tape drive settings, remote archive settings, and
  performance tuning. `.env` stores secrets (e.g. `remote_password`); keep it
  untracked and use `.env.example` as the template.
- `backup_logs/` — holds the single `SUMMARY.csv`: the one statistics file for
  backup/tape-write sessions. No per-run log files and no per-file manifests are
  written, so it never contains individual file names. A rotating diagnostic
  trace (`archiver.log`, via `src/logsetup.py`) also lives here: status lines
  and full exception tracebacks tee into it, console output unchanged. It is
  a debugging trace, not a statistics file — never parse it for reports.
- `Framework & Drivers/` — installer assets and hardware documentation.
- `scripts/sql/` — PostgreSQL schema/index/constraint migrations applied on
  startup by `PgDatabaseManager._init_schema`; `docker-compose.yml` runs the
  local database. Catalog rows are runtime data, not source.

## Build, Test, and Development Commands

```powershell
python -m pip install -r requirements.txt          # set up / update the environment
$files = @(Get-ChildItem src -Filter *.py | ForEach-Object { $_.FullName }) + @(Get-ChildItem storage_map -Filter *.py | ForEach-Object { $_.FullName }) + @((Resolve-Path run.py).Path, (Resolve-Path inspect_db.py).Path); python -m py_compile @files  # syntax check before handoff
python run.py                                        # run the main application
python inspect_db.py                                # run the database inspector
```

## Coding Style & Naming Conventions

Keep the existing module shape: procedural helpers near the top, larger
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

### The three real-world limiters (measured 2026-07-10) and how to reason about them

The remote pipeline has three independent limiters. Full analysis, numbers, the
fix stack, measurement methodology, and future tuning live in
**`docs/performance_insights_and_recommendations.md`** — read it before any
perf work. In brief:

1. **RAM — phantom file cache.** Buffered `tar` extraction into staging fills the
   Windows file cache; `psutil` counts that *reclaimable* cache as "used", so the
   governor sees ~90-94 % and can deadlock on nothing. The archiver process
   itself is tiny (RSS 44-585 MB). `ResourceGovernor._drain_stage_relaxed` lets
   the low-RAM drains (`pack`/`db_sync`) proceed past the ceiling; the real
   consumers (`fetch`/`tape`) are never relaxed. `config.ini [PERFORMANCE]` is
   host-calibrated (soft 90 / hard 95, low floors) because the psutil signal is
   cache, not crash risk; the 8.8 GB pagefile is the true OOM guard.
2. **Tape write — LTFS index sync.** robocopy transfers at full LTO-8 speed
   (100-320 MB/s) but IBM LTFS default `sync_type=time@5` re-syncs the index
   every 5 min, and each sync seeks across a filling tape → *effective* per-chunk
   speed collapses to 8-46 MB/s and the collapse worsens as the cartridge fills.
   Two fixes: `sync_type=unmount` in `ltfs.conf.local` (syncs once at the single
   end-of-session eject; applies only on the next physical remount), and **bigger
   chunks** (`chunk_max_files`) that amortise the fixed overhead — a 135 GB chunk
   wrote at **208.6 MB/s effective**. After the remount, dial chunks back to
   ~150-200k files (huge chunks then only add fetch latency / RAM / staging).
3. **Fetch — single-stream small-file latency.** One SSH/tar stream over 100k
   tiny files is per-file-latency bound at ~15 MB/s. `[PERFORMANCE]
   fetch_parallel_streams` (default 1) runs N concurrent tar streams
   (`RemoteOrchestrator._fetch_batches_parallel`); 3 measured ~30 MB/s. Once the
   tape is fast (unmount), fetch becomes the binding constraint.

## Storage Map & Analytics (`storage_map/`)

A self-contained remote disk-usage mapper for the lab servers,
**decoupled from the tape pipeline** (it does not touch `src/cli.py`,
`lto_archive.db`, or the LTFS drive). `storage_map/run_app.py` is its single
top-level Python entrypoint; internal code lives in `storage_map/lib/` and
`storage_map/webapp/`.

- **Stage 1 — `scan` (fire-and-forget).** Connects to each configured server
  over SSH (reusing `remote_transport._ssh_run`), launches a low-priority
  `ionice -c3 nice -n19 du -x -B1 --max-depth=2` per mount under
  `nohup`/`setsid`, then exits immediately. The scan keeps running on the server
  after the SSH session closes — no live connection is held for the ~hours-long
  `du`.
- **Stage 1.5 — `status` / `fetch`.** `status` checks each server's remote
  completion sentinel; `fetch` SCPs the finished raw log to
  `storage_map/logs/<server>_<ts>.rawlog` (+ a `<server>_latest.rawlog` pointer)
  via `remote_transport._scp_fetch_file`.
- **Stage 2 — `view` / `treemap`.** Parse a *local* raw log only (never the
  disks again): `parse_size` normalizes du size tokens (byte-exact `-B1`
  integers, plus legacy `-h` units) to bytes and `parse_raw_log`
  builds a Mount → user/project → sub-folder tree, rendered as a Rich terminal
  dashboard (`view`), full HTML dashboard (`dashboard`), or interactive Plotly
  HTML treemap (`treemap`). `rich`
  and `plotly` are **optional** — each visualizer prints a `pip install` hint if
  its library is missing.
- **Interactive web dashboard (`python storage_map/run_app.py [--open-chrome]`).**
  A FastAPI + uvicorn app (`storage_map/webapp/`) that serves the same
  overview live from the fetched raw logs, adds in-browser action
  buttons (start scan / check status / refresh servers), static HTML/PDF state
  exports, and a **tape-coverage
  table** matching each mount's directories (mount + `match_depth` levels,
  default 2 — never individual files) against the PostgreSQL catalog: one
  read-only aggregation of `files_index.original_path` prefixes
  (`webapp/coverage.py`), cached in `storage_map/logs/coverage_cache.json`
  and refreshed only via the "Refresh DB coverage" button. Binds to
  `127.0.0.1:8765` (`web_port`/`match_depth`/`host_map`
  keys in `[STORAGE_MAP]`, all optional). The app is intentionally local-only
  and binds to `127.0.0.1`. `fastapi`/`uvicorn` are optional
  dependencies used only by the web app.
  Tests: `tests/test_storage_map_webapp.py`.

Mount points and servers are **config-driven, never hardcoded** — see the
`[STORAGE_MAP]` / `[STORAGE_MAP:<name>]` sections in `config.ini`
(`config.example.ini` documents them). SSH user/password default to the
`[REMOTE]` account (secret still in `.env`). Output lands in
`storage_map/logs/` and generated `storage_map/index.html` (gitignored like
`backup_logs/`).

Run `python storage_map/run_app.py --open-chrome`, then use the in-app Start
scan, Check status, and Refresh servers controls. Unit tests:
`tests/test_storage_map.py` (pure parser + launcher coverage, no hardware/network).

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

### Operating a live run (assistant/operator playbook)

- **Never eject the tape remotely.** `LtfsCmdEject` is physical; a cartridge
  ejected with nobody at the drive cannot be reloaded remotely. Changes that need
  a remount (e.g. LTFS `sync_type`) must be *staged* and applied when someone is
  physically present — never force-eject to apply them.
- **Never kill `Code.exe` to free RAM** — the assistant session may run inside
  VS Code; killing it ends the session. Ask the operator to close windows.
- **Before stopping `run.py`, confirm no tape write is active** (`tasklist` for
  `robocopy` — case-insensitive; a WMI `Name='robocopy.exe'` filter *misses* it
  because it is `Robocopy.exe`). Sessions are resumable and nothing is committed
  to tape mid-write, but interrupting a live tape write is not acceptable.
- **Stopping a *detached* run: use `scripts/graceful_stop.py <pid>`, never
  `taskkill /F`.** A run launched detached (`printf '6\n1\n' | nohup python
  run.py &`) has no terminal to press Ctrl+C in. The helper attaches to the
  target's console and raises `CTRL_C_EVENT` there, which is the signal the
  pipeline already handles — the writer finishes its current chunk, packs are
  preserved, and the session stays resumable. Notes learned the hard way:
  - Pass the PID of the **real** interpreter, not the launcher shim. A detached
    run shows two `python.exe`: a ~1 MB `.venv\Scripts\python.exe run.py` parent
    and the actual several-hundred-MB child. `Get-CimInstance Win32_Process`
    shows both with their `ParentProcessId`.
  - `CTRL_C_EVENT`, not `CTRL_BREAK_EVENT`.
  - **Confirm `robocopy` is not running first.** The signal is only safe at a
    chunk boundary.
  - If the process is wedged in a native tape call, `CTRL_C` does nothing. Only
    then is force-killing Python acceptable — the LTFS driver is a separate
    process and the cartridge stays mounted. See the tape-stage deadlock notes.
- **A stop preserves staged packs; it does not throw them away.** `_preserve_desc`
  keeps the pack directory and writes an atomic `_resume_pack.json` marker
  recording its exact file inventory. The next run reuses that pack with no
  re-fetch and no re-pack (`_try_resume_pack`), but *only* on exact inventory
  equality — a pack with no marker, or whose contents changed, is deleted and
  re-fetched rather than risk writing a truncated chunk to tape and recording it
  as good.
- **Measure with kernel perf counters only, never by reading the tape or walking
  the LTFS drive.** `backup_logs/_tape_sampler.ps1` samples per-process I/O/CPU +
  NIC + RAM every 10 s with negligible overhead; `du`/`ls`/`Get-Volume` on `E:`
  touches the media and can trigger shoe-shining.
- **`wsl --shutdown` frees WSL-ballooned RAM but bounces the shared hot DB** — do
  it only with the operator's OK and only when `run.py` is stopped. Verify the
  port + `current_database()` afterwards.
- **Chunk-size / config changes apply to newly-scanned chunks only** (already-
  planned chunks keep their old plan) and need a `run.py` restart to be read.
  LTFS `sync_type` needs a physical remount. Neither is retroactive.
- **A transient network/DNS blip retries; it no longer kills the run.** On
  2026-07-17 a momentary `ssh: Could not resolve hostname so01` (a Technion DNS
  hiccup — Telegram failed the same instant with `getaddrinfo failed`, and the
  machine never rebooted) stopped the streaming session at chunk 25. Because the
  monitor was offline **on the same host that lost the network**, nothing
  relaunched it and the run sat idle ~3 days. `_fetch_one_batch` now retries a
  transient failure with exponential backoff before giving up
  (`_is_transient_fetch_error`; `[PERFORMANCE] fetch_transient_retries` default
  5, `fetch_transient_retry_base_seconds` default 5). Genuine errors (missing
  file, permission) still fail fast. Two lessons that are *not* fixed in code and
  still bite: (1) don't run the only monitor on the host doing the work — if that
  host loses the network you lose the watchdog exactly when you need it; (2) there
  is still no auto-relaunch, so a hard stop needs a human to re-run `6\n1\n`.
- **The forced restart on this host comes from SCCM, not WSUS.** Evidence from
  the 2026-07-15 loss (System log 1074): the initiator was `CcmExec.exe` —
  *"Your computer will restart at 15/07/2026 10:39:01 to complete the
  installation of applications and software updates"* — the Software Center
  notification, **60 seconds** before the restart. WSUS/GPO deadline settings are
  real on this host but were not the trigger. Consequences:
  - Pausing Windows Update cannot influence an SCCM restart. Different control
    plane. Escalate for an **SCCM maintenance window / deployment exemption**,
    not a WSUS deadline exemption.
  - 60 s of warning against a ~70 min chunk cycle means the sentinel's poll can
    never save the chunk in flight. The protection that matters is
    `_pre_tape_write_reboot_check`, which runs synchronously immediately before
    each write and refuses to *start* one — so the loss is a deferred chunk
    rather than a killed write.
  - Query SCCM via `root\ccm\ClientSDK` →
    `CCM_ClientUtilities.DetermineIfRebootPending`. Treat "could not ask" as
    distinct from "no restart" (`sccm_reboot_status()['determinate']`).
  - **The `RebootData` registry key's existence proves nothing** — verified
    2026-07-17: present but empty while SCCM reported no pending restart. Read
    its *values* (`RebootBy`, `HardReboot`), never its presence.
- **Verify LTFS `sync_type` from the mount, not the config file.** The two drift
  silently: an MSI reinstall of IBM Storage Archive SDE on 2026-07-16 13:52 reset
  `ltfs.conf.local` to its packaged contents (which is why that file is dated
  2024 and matches its own `.bak`). The mount states its own truth once, in the
  LTFS Windows event log, event **61259**: `Sync type is "time", Sync time is
  300 sec`. `ltfs_sync_mode_status()` reads that, and the pipeline refuses to
  start tape writes under anything but `time@5` — under `sync_type=unmount` the
  index is written only at unmount, so a forced restart loses every chunk since
  the mount, which is exactly how chunks 18-91 (~126 GB) died.
