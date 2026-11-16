# Architecture

An overview of how the program is put together. For operation see
[user-guide.md](user-guide.md); for the rules the design serves see
[safety.md](safety.md).

## The flow: local first

```text
source (local folder, or remote host over SSH/tar)
  -> local staging
  -> pack + validate (ZIP bundles, or Stored TAR parsed to its trailer)
  -> finite write group: robocopy to the LTFS tape
  -> classify the copy result (the tape is never read back)
  -> commit to the catalog
```

There is no code path that streams source data directly to tape. The tape
writer's only input is validated local staging. That ordering is what makes a
fetch or packing failure cost local disk and never tape state.

## Module map

`run.py` and `inspect_db.py` in the project root are thin runners: they change
to the project folder and call into the `src` package. Data files
(`config.ini`, `.env`, the SQLite catalog, `backup_logs/`, `db_backups/`) stay
in the project root and are ignored by git.

Dependencies point downward in this list:

| Layer | Modules | Role |
| --- | --- | --- |
| Entry | `cli`, `ui` | Main menu, sub-menus, the headless remote-archive entry |
| Orchestration | `local_orchestrator`, `remote_orchestrator`, `orchestrators` (re-export facade) | One archive session: plan, resume, pre-write gates |
| Remote pipeline | `scan_frontier`, `remote_staging`, `remote_writer`, `remote_pipeline`, `ready_queue`, `resource_governor` | Scan, fetch + pack, write, and the loop that schedules them |
| Write and restore | `backup`, `retriever`, `packer`, `packer_parallel`, `tar_container`, `stored_tar_planning` | Copy to tape and record it; search and restore; containers |
| Artifacts | `archive_artifacts`, `plan_manifest`, `terminal_manifest`, `scan_state_manifest`, `local_manifest_archive`, `manifest_first` | Versioned local files that hold per-file detail |
| Tape | `ltfs`, `ltfs_ownership`, `robocopy`, `tape_write_log`, `tape_reset` | LTFS tools, the drive ownership mutex, the copy and its classifier |
| Host | `windows_update_guard`, `runtime`, `status_file`, `exit_codes`, `telegram_notify` (retired) | Restart guard, cancel handling, status file, exit codes |
| Catalog | `db` (factory), `db_backend`, `pg_db` / `sqlite_db` (managers), `pg_core` / `sqlite_core` (engine hooks), `pg_catalog`, `pg_sessions`, `pg_tapes`, `pg_scan`, `pg_containers`, `pg_directory_catalog`, `pg_tape_reset` (shared method groups), `sqlite_adapter`, `sqlite_dialect`, `sqlite_lock`, `pg_bulk` | Everything that talks to the database |
| Catalog tools | `db_backup`, `pg_backup`, `session_reconcile`, `session_health`, `schema_audit`, `directory_catalog_validation`, `inspector_repository`, `db_inspector_qt` | Backups, reports, maintenance, the GUI |
| Base | `config`, `constants`, `paths`, `pipeline_types`, `logsetup`, `reporting`, `cli_errors` | Configuration schema, shared types, logging |

Application code imports the facades (`orchestrators`, and
`db.create_database_manager`). In tests, a `mock.patch` target names the
module a symbol is used in.

### The remote pipeline

`remote_orchestrator.py` is a facade over four modules, each owning one
invariant:

| Module | Owns | Never does |
| --- | --- | --- |
| `scan_frontier.py` | The only scanner: discovery, sealing chunks | touch the tape |
| `remote_staging.py` | SSH/tar fetch, retry classification, staging watchdog, packing, planned = fetched = packed reconciliation | touch the tape |
| `remote_writer.py` | The finite write group: the only path by which a remote copy reaches the tape | eject; retry into a failing drive; clear `backing` |
| `remote_pipeline.py` | The single scheduling loop | decide anything about the tape |

Cartridge selection and the pre-run checks live in `remote_orchestrator.py`,
so a review of all cartridge access must include it and `remote_writer.py`.

## The two-backend seam

`[DATABASE] backend` selects SQLite or PostgreSQL.
`db.create_database_manager(cfg)` returns `SqliteDatabaseManager` or
`PgDatabaseManager`. Both are assembled from the **same** method-group
classes; those classes never test which backend they run on.

- Engine differences are hooks in `pg_core.PgConnectionCore`, overridden in
  `sqlite_core.SqliteConnectionCore`.
- SQL is written once in PostgreSQL syntax. `sqlite_dialect` applies a closed
  list of lexical rewrites; anything left over raises.
- Code that opens its own short-lived connection (inspector, reconcile, tape
  reset, reports) goes through `db_backend.connect()`.
- PostgreSQL-only features are one list, `db_backend.POSTGRESQL_ONLY_FEATURES`,
  and one refusal, `PostgresOnlyFeature`.
- Importing the package never imports psycopg, so a SQLite-only install works
  without it.

Details and the guard-by-guard mapping:
[database-backends.md](database-backends.md).

### Schema

PostgreSQL: ordered files in `scripts/sql/`. Start-up applies the additive
ones (001-006, 008-011, 013, 020). 007, 014-019 are installed automatically
only on a database that was completely empty; on an existing catalog they are
explicit commands. 012 (sealed batches) is always manual. See
[installation.md](installation.md#schema-on-postgresql).

SQLite: `scripts/sql/sqlite/001_schema.sql` is the fully migrated schema,
applied in one transaction and recorded with its checksum. The catalog
refuses to open if that file, or the definitions it created, changed.

## Locks

Three locks, each for a different thing:

| Lock | Protects | Mechanism |
| --- | --- | --- |
| Archiver lock | One archive run at a time per catalog | PostgreSQL: session advisory lock. SQLite: operating-system lock on `<database>.archiver.lock` plus an owner row. Verified before every write group and by the move to `backing`; never re-acquired silently. Lost lock = stop (`archiver_lock_lost`) |
| LTFS ownership | One process touching the tape drive | Named Windows mutex `Global\OWC_LTO8_LTFS_OWNERSHIP_<ltfs_ownership_id>`, keyed on the physical drive, not the drive letter. Unset or malformed identity = no tape access |
| Tape I/O lock | Tape operations inside one process | In-process reentrant lock (`_acquire_tape_io_lock`) |

Processes outside the program (Explorer, antivirus, another copy job) are not
covered by any lock.

## Chunk state machine

A remote session is a list of chunks. Chunk status lives in the catalog.
Allowed moves (`CHUNK_TRANSITIONS` in `src/pipeline_types.py`):

```text
pending       -> fetching | fetch_failed | backing | backup_failed | done
fetching      -> packing | fetch_failed | pending | done
packing       -> backing | backup_failed | fetch_failed | pending | fetching | done
backing       -> done
done          -> (final)
fetch_failed  -> pending | fetching
backup_failed -> pending | fetching | packing
```

- A chunk is set to `backing` **before the first touch of the tape**. From
  there it can only become `done`. If a run dies in between, the chunk stays
  `backing`: whether it is on tape cannot be known from the catalog, so the
  next run stops (`ambiguous_backing_chunk`) instead of rewriting it.
- The same rule is enforced twice: in the status setter, and in the database
  by a row trigger (PostgreSQL migration 020; same-named trigger on SQLite).
- Leaving `backing` or `done` needs a **named override**, recorded with who
  authorised it and why in `remote_chunk_transition_overrides`:
  `tape_reset` (the cartridge generation was destroyed by an authorised
  reset) or `operator_reconciled` (a person compared the cartridge with the
  catalog). There is no blanket bypass.

## Safety gates before a tape write

Evaluated before each write group; any gate that fails, or cannot be
evaluated, blocks the write and stops the run with a named reason:

| Gate | Stop reason |
| --- | --- |
| Archiver lock still held | `archiver_lock_lost` |
| LTFS ownership acquired | `ltfs_ownership_unavailable` |
| Mount is real and its sync mode is `time@5` (LTFS event 61259) | `ltfs_mount_unverifiable`, `ltfs_sync_mode_not_time5` |
| No fatal or degrading drive/medium event in the IBM LTFS log since the mount; re-checked from the log before each chunk of the group | `ltfs_media_degraded` |
| No pending Windows or SCCM restart | `sccm_reboot_pending`, `windows_reboot_pending` |
| Mounted cartridge and tape generation match the session; tape not marked full; chunk fits the budget | `unexpected_tape_or_db_state` |
| No chunk of the session left in `backing` | `ambiguous_backing_chunk` |
| Planned, fetched and packed file sets agree | chunk is not handed to the writer |

The local (menu 1) path runs the same sync-mode, health and restart gates.
During the group, the first hard write error stops the run; there is no
retry. The copy result is decided by a classifier over the raw `robocopy`
log, never by the return code and never by reading the tape.

Exit codes and stop reasons: `src/exit_codes.py`,
[troubleshooting.md](troubleshooting.md#exit-codes).

## Where each fact lives

| Store | Authoritative for |
| --- | --- |
| Catalog database | Sessions, chunks, bundles and containers, large-file rows, folder totals, tape accounting |
| `jsonl.zst` manifests under `[LOCAL_MANIFEST_ARCHIVE] root` | Per-file inventory of packed small files; scan segments |
| Receipts and sidecars beside containers | Container evidence: sizes, SHA-256, member counts |
| Tape | Payload only. Nothing needed to interpret the archive lives only on tape |

Formats of the public artifacts, with synthetic samples:
[examples/](../examples/README.md).

## Local artifacts and how they are published

Three versioned schemas carry per-file detail without one catalog row per
file: `plan-manifest-v1` (the sealed plan of a chunk), `tar-sidecar-v1` (the
validated parse of a Stored TAR container) and `terminal-state-v1` (one
final disposition per planned file).

All three are published the same way: write to a uniquely named `.part` file,
flush and fsync, re-read and validate the finished file against the sealed
plan, then publish atomically without ever overwriting an existing final
name. A crash leaves an orphan `.part`, never a truncated artifact that a
reader would take as complete. A catalog locator may never name a `.part`
(checked in code and by a schema constraint).

These artifacts carry no content hashes of source files: hashing would mean
reading every source byte a second time over SSH.

## History

The design decisions above were made in response to real failures. The dated
records are in [history/](history/README.md).
