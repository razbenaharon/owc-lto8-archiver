# Repository Guidelines

This repository is a Windows-focused Python utility for archiving data to LTO/LTFS
tapes. It fetches data from a remote host over SSH, packs many small files into ZIP
bundles, writes them to an LTFS-mounted tape via `robocopy`, and indexes every file
in a local SQLite database.

## Project Structure & Module Organization

- `lto_archive_manager.py` — main CLI: local archive, remote archive, restore, tape
  maintenance, packing, database updates, and the backup-summary report.
- `db_inspector.py` — GUI database inspector/editor.
- `config.ini` — local paths, tape drive settings, remote archive settings, and
  performance tuning. `.env` stores secrets (e.g. `remote_password`); keep it
  untracked and use `.env.example` as the template.
- `backup_logs/` — generated per-pack logs plus the cross-pack `SUMMARY.md` report.
- `Framework & Drivers/` — installer assets and hardware documentation.
- `lto_archive.db` — local SQLite archive index; treat as runtime data, not source.

## Build, Test, and Development Commands

```powershell
python -m pip install -r requirements.txt          # set up / update the environment
python -m py_compile lto_archive_manager.py db_inspector.py   # syntax check before handoff
python lto_archive_manager.py                       # run the main application
python db_inspector.py                              # run the database inspector
```

## Coding Style & Naming Conventions

Python 3, four-space indentation, descriptive `snake_case` for functions, variables,
and methods. Keep the existing module shape: procedural helpers near the top, larger
workflow classes (`LTOPacker`, `LTOBackup`, `LTORetriever`, `DatabaseManager`) below.
Prefer small helpers for hardware, LTFS, robocopy, database, and path-safety
behavior. Avoid broad refactors during operational fixes.

## Testing Guidelines

There is no formal test suite yet. At minimum run `python -m py_compile ...` after
edits. Pure parsing, config, database, path-normalization, and reporting changes do
**not** require real tape hardware — validate them directly (e.g. run
`generate_backup_summary()` against the existing logs in `backup_logs/`). For
tape-related changes, reason carefully about hardware side effects and verify with a
small staged dataset before a full remote archive run.

## Logging & Reports

- **Per-pack logs** (`backup_logs/<timestamp>_<tape>_<source>.log`) are written by
  `LTOBackup._write_backup_log`. The Summary section reports `Total time`,
  `Data copied`, robocopy `Average speed`/`Robocopy time`, and — for the
  staged/packed pipeline — per-phase timing and throughput:
  - `Fetch time` / `Fetched data` / `Fetch speed (remote->PC)` — the remote→PC
    ("internet") throughput, measured in `_stage_chunk`.
  - `Pack time` / `Pack speed` — local ZIP packing.
  - `DB sync time` — the Phase-3 database indexing (see Performance characteristics).
  - `End-to-end speed` — `Data copied ÷ Total time`.
  Fetch and pack run in the producer and overlap the *previous* chunk's tape write,
  so these phases need not sum to `Total time`.
- **Cross-pack summary** (`backup_logs/SUMMARY.md`) is a Markdown table across all
  per-pack logs, produced by `generate_backup_summary()`. It regenerates
  automatically after each pack and on demand via main-menu option **8**.

## Performance Characteristics

For staged/packed runs the dominant cost is usually **not** fetch bandwidth or
SHA-256 hashing — it is the **Phase-3 DB sync** in `LTOBackup._run_locked`, which
calls `file_record_exists` + `insert_file` per file, and `insert_file` issues a
`commit()` (fsync) **per row**. This scales with file *count*, not bytes: packs with
more, smaller files take far longer for the same data volume (observed: ~3.4× longer
end-to-end from 89k to 157k files at a constant ~100 GiB). The new `DB sync time` log
field makes this visible.

Recommended future optimization (not yet implemented): batch Phase-3 inserts into a
single transaction / `executemany`, and add an index on
`(original_path, tape_label, local_session_id, local_chunk_index)` to speed the
dedup lookups.

## Commit & Pull Request Guidelines

Use concise, imperative commit messages, optionally prefixed `fix:`, `feat:`,
`refactor:`, or `chore:` (e.g. `fix: handle Windows-illegal chars in remote fetch
paths + collision guard`). PRs should state: purpose, risk level, commands run,
hardware/manual verification if relevant, and any database/config changes. For
`db_inspector.py` UI changes, include a screenshot or short behavior description.

## Security & Operations Notes

- Never commit `.env`, real credentials, generated logs with sensitive paths, or
  large runtime databases (`lto_archive.db`).
- **During archive writes, avoid browsing the LTFS drive or starting separate copy
  jobs.** Internal tape access is serialized (`_acquire_tape_io_lock`), but external
  processes can still degrade tape throughput. This is enforced as guidance only —
  it is printed as a `[WARNING]` (`LTFS_WRITE_WARNING`) at the start of every
  tape-write run and at remote-pipeline start.
