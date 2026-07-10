# CLAUDE.md

AI-assistant guidance for this repository lives in **[AGENTS.md](AGENTS.md)** — read it
first. It covers project structure, build/run commands, coding style, testing,
logging/reports, performance characteristics, and security/operations.

This file is intentionally thin so the guidance has a single source of truth.

## Do not miss

> **During archive writes, avoid browsing the LTFS drive or starting separate copy
> jobs.** Internal tape access is serialized (`_acquire_tape_io_lock`), but external
> processes can still degrade tape throughput. This warning is also printed at the
> start of every tape-write run (`LTFS_WRITE_WARNING`, defined in `src/constants.py`).

### Operational best practices (learned the hard way — read before touching a live run)

- **Never eject the tape remotely.** `LtfsCmdEject` is physical; a cartridge
  ejected with nobody at the drive cannot be reloaded remotely (no software
  "load" for a tape out of the slot). LTFS `sync_type` changes need a physical
  remount — stage them, never force-eject to apply them.
- **Never kill `Code.exe` to free RAM.** This assistant session runs *inside*
  VS Code; killing it terminates the session. Ask the operator to close spare
  windows instead.
- **Detect `robocopy` with `tasklist` (case-insensitive), not a WMI
  `Name='robocopy.exe'` filter** — it registers as `Robocopy.exe` (capital R)
  and the case-sensitive filter misses it, faking a "hung/gone" alarm.
- **Measure the pipeline with kernel perf counters only — never read the tape.**
  A read-only perf-counter sampler is safe; walking the LTFS drive or `du`-ing
  it is not (tape wear + shoe-shining). See `backup_logs/_tape_sampler.ps1`.
- **`config.ini [PERFORMANCE]` is host-calibrated** for the 15.6 GB box (psutil
  reports reclaimable file cache as "used"). Do NOT "restore defaults" — the
  tuned RAM thresholds + the 8.8 GB pagefile are the correct, crash-safe config.
- Before stopping a run, **verify no tape write is active** (`tasklist` for
  robocopy); sessions are resumable but interrupting a live tape write is not OK.
- Full detail, measurements, and future recommendations:
  **[docs/performance_insights_and_recommendations.md](docs/performance_insights_and_recommendations.md)**.

## Layout

Run the app with `python run.py` (CLI) or `python inspect_db.py` (GUI). The code
lives in the `src/` package; data files (`config.ini`, `.env`, `lto_archive.db`,
`backup_logs/`) stay in the project root.
