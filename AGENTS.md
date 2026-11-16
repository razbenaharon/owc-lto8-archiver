# Repository guidelines

Guidance for contributors and AI assistants working on this repository: a
Windows Python tool that archives data to LTO/LTFS tape. It reads from a
local folder or a remote host over SSH, packs small files into bundles,
writes them to an LTFS-mounted cartridge with `robocopy`, and records every
file in a catalog on SQLite or PostgreSQL.

Start with the [documentation map](docs/README.md). The tape-safety rules
below and in [docs/safety.md](docs/safety.md) are non-negotiable.

**This version has not been validated on a real tape drive.** Tape-path
behaviour is verified with simulated devices only.

## Sources of truth

- Behaviour: the current code and tests.
- Settings: `CONFIG_SCHEMA` in `src/config.py`. The active configuration of
  an installation is its untracked `config.ini`, never `config.example.ini`.
- Session and chunk state: the catalog database.
- Mounted LTFS sync mode: the mount's own declaration (LTFS event 61259),
  never `ltfs.conf.local` alone.
- Drive and medium health: the IBM LTFS `LogFile.csv`, scoped to the current
  mount.
- Statistics: `backup_logs/SUMMARY.csv`. `archiver.log` is a debugging trace;
  never parse it for reports.
- A Markdown statement about the state of an installation is a dated
  snapshot. Verify live state on the host that runs the archive; a clone on
  another computer proves nothing about it.

When sources conflict, prefer live evidence over a snapshot and code over a
design note. Never relax a tape-safety rule to reconcile a conflict.

## Project structure

- `run.py`: runner for the CLI (menu, `--version`, `--backup-db`,
  `remote-archive`, `tape-reset`).
- `inspect_db.py`: runner for the inspector GUI and the catalog report and
  maintenance commands.
- `src/`: the application package. Layers and module roles:
  [docs/architecture.md](docs/architecture.md). Import the facades
  (`orchestrators`, `db.create_database_manager`) from application code.
- `scripts/`: the privacy gate, release builder, install smoke test,
  graceful stop, and helpers from the original deployment.
- `scripts/sql/`: PostgreSQL migrations; `scripts/sql/sqlite/`: the SQLite
  schema. Applied migrations are never edited (the SQLite schema file is
  checksum-pinned).
- `tests/`: the test suite. `tests/db_backends.py` runs a catalog suite on
  both backends.
- `docs/`: guides. `docs/history/`: dated records of the original
  deployment, not instructions.
- `examples/`: JSON Schemas and synthetic samples of the manifest formats.
- Data files stay in the project root and are never committed:
  `config.ini`, `.env`, the SQLite catalog, `backup_logs/`, `db_backups/`.

## Commands

```powershell
python -m pip install -r requirements.txt
python -m pip install pytest
python -m pytest tests/ -q
python -m compileall -q src scripts run.py inspect_db.py
python scripts/check_public_repo.py
python run.py --version
python run.py
python inspect_db.py
```

More, including the PostgreSQL suites, the install smoke test and the
mutation check: [docs/testing-and-validation.md](docs/testing-and-validation.md).

## Coding style

- Keep the existing module shape: small helpers for hardware, LTFS,
  `robocopy`, database and path-safety behaviour; workflow classes
  (`LTOPacker`, `LTOBackup`, `LTORetriever`, the orchestrators) above them.
- Avoid broad refactoring in the same change as an operational fix.
- **Two catalog backends.** `[DATABASE] backend` selects `sqlite` or
  `postgresql`; a config with no `backend` key stays on PostgreSQL. The
  shared method groups (`src/pg_*.py` mixins) must never branch on the
  backend: put an engine difference behind a hook in `src/pg_core.py` and
  override it in `src/sqlite_core.py`. SQL is written in PostgreSQL syntax
  and must stay valid on both engines. The PostgreSQL-only list is
  `src/db_backend.POSTGRESQL_ONLY_FEATURES`. Details:
  [docs/database-backends.md](docs/database-backends.md).
- A new `config.ini` key goes in `CONFIG_SCHEMA`, `config.example.ini` and
  `docs/configuration.md`.
- An expected refusal raises `OperationalError` with one readable line.
  Failures end in a non-zero exit code (`src/exit_codes.py`); a new stop
  reason must be classified there as restartable or not.
- In tests, `mock.patch` targets name the module a symbol is *used* in (for
  example `src.scanning._ssh_run`).

## Testing

- Run `python -m pytest tests/ -q` and the `compileall` sweep after every
  edit. Parsing, config, database, path and reporting changes need no tape
  hardware; validate them directly.
- The suite is mock-only by policy. `tests/conftest.py` blocks any call that
  would reach a real LTFS mount. Do not weaken it. A test that needs a
  drive-shaped path uses a temporary directory.
- The PostgreSQL suites run only against a disposable server named in
  `LTO_TEST_PG_DSN`. `tests/pg_test_guard.py` refuses port 5432, a
  non-loopback host and any server hosting a database named `lto_archive`.
  **Never point tests at a real catalog.** Do not weaken the guard.
- The PostgreSQL archiver-lock probe counts advisory locks for the whole
  server; a test on a throwaway database must patch it.
- Documented commands, links and the configuration reference are tested
  (`tests/test_docs_commands.py`). Update the docs with the code.
- For a tape-path change, reason about hardware side effects and verify
  with a small staged data set on a scratch cartridge before any real run.

## Logging and reports

- `backup_logs/SUMMARY.csv` is the one statistics file. It holds no file
  names. Rows are appended by `LTOBackup._write_backup_log` through
  `reporting.append_backup_summary_row`, with per-phase timing
  (`fetch_seconds`, `pack_seconds`, `db_sync_seconds`). Phases overlap, so
  they need not sum to the total.
- `backup_logs/archiver.log` is a rotating diagnostic trace.
- `backup_logs/tape_write/` holds the raw `robocopy` log of each tape write
  and its classification. This, not the return code, decides success.

## Performance notes

- The catalog write after a tape copy scales with file *count*, not bytes.
  It is batched (PostgreSQL `COPY` into a staging table, one upsert).
- Buffered extraction fills the Windows file cache, which `psutil` reports
  as used memory. The governor therefore lets low-memory stages proceed
  after a wait; do not "fix" that by tightening the memory limits.
- **An installation's `[PERFORMANCE]` values are host-calibrated.** Do not
  "restore defaults" in a working `config.ini`, and do not shrink its
  pagefile: the tuned memory thresholds and the pagefile are what keep a
  long run from stalling or crashing.
- The required `sync_type=time@5` costs throughput through periodic index
  writes. Recover it only through bounded chunk size, never by changing the
  sync mode.
- One SSH/tar stream over many tiny files is latency-bound;
  `fetch_parallel_streams` helps.
- Measurements from the original hardware:
  [docs/history/performance_insights_and_recommendations.md](docs/history/performance_insights_and_recommendations.md).

## Commits and pull requests

Concise imperative messages, optionally prefixed `fix:`, `feat:`, `test:`,
`docs:`, `refactor:`, `chore:`. A pull request states purpose, risk to tape
state, commands run, hardware verification if any, and database or
configuration changes. See [CONTRIBUTING.md](CONTRIBUTING.md).

## Security and privacy

- Never commit `.env`, credentials, catalogs, dumps, logs, real manifests,
  or real host, user, site or path names. Use placeholders
  (`source-host.example`, user `archive`, `D:\staging`).
  `python scripts/check_public_repo.py` must pass.
- Secrets live in `.env`, not in `config.ini`.

## Tape-safety rules (non-negotiable)

The full statement, with reasons, is [docs/safety.md](docs/safety.md). The
question behind all of them: *if this fails at the worst possible moment,
can it be recovered without somebody standing at the drive?* If not,
redesign or reject. This outranks throughput.

### Never, without the operator asking for it each time

- Never eject. `LtfsCmdEject` is physical; a cartridge out of the slot
  cannot be loaded by software. The remote pipeline never ejects, whatever
  `eject_after_session` says.
- Never remount, format, reset the drive or run `ltfsck` / `LtfsCmdCheck`.
  Each can strand a recoverable volume.
- Never force a remount to apply a setting. Stage LTFS setting changes for
  when somebody is at the drive.
- Never use `sync_type=unmount`. The required mode is `time@5`, verified
  from the mount.

### No independent write verification

Never add code that reads from the tape after a write to verify it. Reading
right after writing wears the tape and the drive. Rely on the classified
result of the copy tool, and keep tape reads to a minimum.

### While idle, do not touch LTFS

After a write group finishes and ownership is released, the application
performs zero tape operations until the next finite group is ready. No
readiness call, no label read, no access to the tape drive letter, no
`os.listdir()` or `os.path.isdir()` on the LTFS root, no IBM LTFS helper, no
`vol`, `Test-Path`, `Get-Volume` or `Get-PSDrive` on it, no `robocopy`
against it. Only passive log and telemetry observation is allowed. This
applies to diagnostics and to assistants as well: measure with process and
disk performance counters, never by reading or walking the tape.

### Write only in finite groups

```text
prepare several chunks locally
-> select one finite group
-> acquire LTFS ownership once
-> run readiness once
-> verify the cartridge once
-> write all selected chunks consecutively
-> release ownership
```

No readiness check or label read between chunks. Ownership is not released
between chunks and is not held while waiting for future chunks. Every chunk
keeps its own status: if a later chunk fails, earlier ones stay completed
and packs that were not started stay reusable.
`RemoteChunkWriter.write_chunk_group` is the only path by which a remote
copy reaches the tape.

### On a hard tape failure, stop

Read-only, write-protect, servo error, SCSI timeout, ownership loss,
cartridge mismatch, LTFS instability or a hard `robocopy` failure: stop. Do
not retry and do not start another group. Do not eject, remount, format or
run a filesystem check. Keep packs, logs and dumps. Record the last
successful chunk. Classify the outcome conservatively. Treat every hard
write error as potentially latching: one can make a cartridge read-only for
good.

### `backing` has no automatic exit

A chunk is marked `backing` before the first touch of the tape and may only
become `done`. Never clear a `backing` chunk in code or by hand without
comparing the cartridge with the catalog; its outcome on tape cannot be
known from the catalog. An unreadable chunk status stops the run.

### Stopping and starting

- Stop only at a chunk boundary. Before stopping `run.py`, confirm no tape
  write is active with `tasklist` (case-insensitive). A WMI filter on
  `Name='robocopy.exe'` misses it: the process is `Robocopy.exe`.
- Stop a detached run with `python scripts/graceful_stop.py <pid>`, never
  `taskkill /F`. Pass the PID of the real interpreter, not the launcher. A
  stop keeps staged packs; the next run reuses a pack only when its
  inventory matches exactly.
- Refuse to *start* work likely to be interrupted (the
  `_pre_tape_write_reboot_check` pattern) rather than aborting it midway. A
  managed host (SCCM) can force a restart with 60 seconds of warning;
  pausing Windows Update does not control that, and the reboot sentinel
  cannot win such a countdown. The durable fix is a maintenance window or
  deployment exemption for the archive host.
- Never infer success from a `robocopy` return code of 0.
- The LTFS drive letter is not stable. Verify `[HARDWARE] lto_drive` before
  every run; `Test-Path` returning true does not prove a live mount.
- Prefer a smaller blast radius over bigger batches.
- Retiring a cartridge is a status (`tapes.status = 'full'`), not an edited
  byte counter; used space is recalculated from the catalog.
- A session row marked `active` is not proof that anything runs, and the
  cleanup guard refuses while one exists. Clear it only with
  `python inspect_db.py --reconcile-stale-sessions`, which proves staleness.
  `python inspect_db.py --session-forensics` shows the same evidence
  read-only and is safe during a live run.
- An assistant session may run inside the editor: never kill the editor
  process to free memory. Ask the operator to close windows.

### After code changes

```text
code change -> full offline tests -> PostgreSQL tests on a disposable server
-> small synthetic pilot on a scratch cartridge -> one bounded write group
-> review -> normal operation
```

Do not combine a major refactoring, a database change, a scheduling change,
a hardware pilot and a return to real data in one step.

## History

The site-specific production notes that used to be in this file (session
numbers, tape labels, host events, the SCCM restart, cartridge swaps) are in
[docs/history/agents-production-notes.md](docs/history/agents-production-notes.md).
The incident records are in [docs/history/incidents/](docs/history/incidents/README.md).
They are dated records, not current instructions.
