# SQLite catalog backend

The catalog runs on one of two backends, chosen in `config.ini`:

```ini
[DATABASE]
backend = sqlite            ; or: postgresql
sqlite_path = lto_archive.sqlite3
```

- **`sqlite`** — one local file, created automatically with the complete
  schema on first run. Nothing to install, no server, no psycopg.
- **`postgresql`** — the external server the project has always used.

**A `config.ini` with no `backend` line keeps using PostgreSQL.** An existing
installation is never switched by an upgrade. Only the generated default
config and `config.example.ini` (new installs) select SQLite.

There is no tool that copies data between the two backends in v1.0.

## One code base

The method groups (`src/pg_catalog.py`, `pg_sessions.py`, `pg_tapes.py`,
`pg_scan.py`, `pg_containers.py`, `pg_directory_catalog.py`,
`pg_tape_reset.py`) are the *same classes* on both backends and never test
which one they run on. Everything engine-specific is a hook in the core:

| Piece | PostgreSQL | SQLite |
| --- | --- | --- |
| Manager | `src/pg_db.PgDatabaseManager` | `src/sqlite_db.SqliteDatabaseManager` |
| Core hooks | `src/pg_core.PgConnectionCore` | `src/sqlite_core.SqliteConnectionCore` |
| Driver | psycopg 3 | `src/sqlite_adapter.py` (psycopg-shaped) + `src/sqlite_dialect.py` |
| Standalone connections | `src/db_backend.connect()` | `src/db_backend.connect()` |
| Backup | `src/pg_backup.py` | `src/db_backup.py` |

SQL is written once, in PostgreSQL syntax. On SQLite every statement passes
through a small, closed list of lexical rewrites; anything PostgreSQL-only that
is left over **raises** instead of running half-translated.

## Schema

`scripts/sql/sqlite/001_schema.sql` is the fully migrated PostgreSQL schema
(migrations 001–011, 013, 007, 014 + finalize, 015–020): same tables, same
columns in the same order, same uniqueness, CHECK and foreign-key constraints,
and the guard triggers. It is applied in one transaction and recorded in
`schema_migrations` (file checksum + definition fingerprint). Later changes go
in `002_*.sql`, never by editing `001`.

The catalog refuses to open when a migration file changed after it was
applied, when the definitions of its own objects drifted (a dropped or added
trigger, an altered table), or when the file is not a catalog at all. The same
fingerprint backs the pre-write gate (`validate_container_format_authority`).

The explicit PostgreSQL migration commands (`inspect_db.py --apply-*-schema`)
answer *installed at creation, schema vN* on SQLite and change nothing.

`tests/test_sqlite_schema_parity.py` compares the SQLite schema with a
committed snapshot of the migrated PostgreSQL schema, and the snapshot with a
live PostgreSQL when a test server is configured.

### Guards: how each PostgreSQL guard is enforced on SQLite

| PostgreSQL guard | SQLite |
| --- | --- |
| Chunk status may not leave `backing`/`done` without a named override (020) | Trigger `trg_remote_chunks_status_transition`; the override row is consumed in the same statement |
| Write-once packaging format, container identity, artifact identity, bundle format (015) | Triggers of the same names |
| Initial chunk format must match the session's durable format (015) | Trigger `trg_remote_chunks_initial_format` |
| Session packaging default immutable; new sessions cannot default to Stored TAR (015) | Triggers `trg_remote_sessions_packaging_default[__upd]` |
| Container tape generation must match the session/tape (015) | Triggers `trg_archive_containers_tape_generation[__upd]` |
| Schema-authority rows immutable (015/018/019) | Triggers; the one bootstrap insert is allowed only into an empty table while the creating transaction carries the schema checksum |
| Packaging boundary rows immutable (015) | Triggers refuse **every** write (boundaries are PostgreSQL-only, see below) |
| Directory parent/depth rules, coverage scope, sealed artifacts, directory-part routing (018) | Triggers of the same names |
| Chunk plan-source rules and the audited `legacy_db → manifest` transition (018) | Triggers `trg_remote_chunks_plan_source_guard[__upd/__audit]`; the audit row is written by the trigger |
| Session plan-transition state ladder and boundary history (018) | Triggers `trg_remote_session_plan_transitions_guard[__upd/__del]` |
| `lto_assign_new_chunk_format()`, `lto_transition_chunk_plan_source()` | The same rules in Python, in the same transaction (`SqliteConnectionCore._sql_*`); the schema triggers still check the resulting write |

Differences that are deliberate and safe (each is the stricter side):

- **Trigger messages are static text** (no interpolated ids).
- **Guards that read a PostgreSQL GUC read `lto_txn_settings`.** Its rows
  exist only inside the write transaction that set them: they are deleted in
  that transaction before COMMIT and vanish on ROLLBACK.
- **The manifest-authority check runs immediately**, not at COMMIT. The only
  writers create the ready artifact before pointing a chunk at it.
- **`pg_trigger_depth()`** (audit rows only from inside the guard trigger) is
  replaced by a setting the guard trigger itself sets and removes.
- **The sealed-batch active-chunk probe** inside the plan-source guard is
  omitted: sealed batches do not exist on SQLite.
- **No trigram indexes.** Substring search scans; it is correct, not fast.
- **`ILIKE` / `LIKE` are case-insensitive for ASCII only.**
- **Timestamps** are stored as UTC text `YYYY-MM-DD HH:MM:SS.ffffff+00:00`.

## Single-writer archiver lock

PostgreSQL uses a session advisory lock. SQLite uses two layers, both
required and both fail-closed:

1. **Authority** — an exclusive operating-system lock on
   `<database>.archiver.lock`, held on an open handle for the whole run
   (`src/sqlite_lock.py`). The OS drops it when the process exits, crashes or
   is killed, so a dead archiver can never leave the lock stuck: nobody has to
   go to the machine to clear it.
2. **Loss detection** — an owner-token row in `archiver_lock`.

`verify_archiver_lock()` is called before every tape write and by the
`backing` transition, exactly as on PostgreSQL, and never re-acquires. "Is an
archiver running?" (`archiver_lock_status`, the inspector, maintenance
commands) and "does this session own the lock?" each go through one core hook
on both backends.

The database file must be on a **local disk**; a network path is refused.

## Backup

`python run.py --backup-db` (and CLI menu 9) copy the live catalog with the
SQLite online backup API to `db_backups/`, then run `PRAGMA integrity_check`
and `PRAGMA foreign_key_check` on the copy. A copy that fails is discarded and
never renamed into place. `src/db_backup.verify_backup()` returns the same
`BackupVerification` shape as the PostgreSQL verifier.

## PostgreSQL-only in v1.0

The single list is `src/db_backend.POSTGRESQL_ONLY_FEATURES`. Each entry
refuses on SQLite with `PostgresOnlyFeature` — one readable line, exit code 1,
never a traceback, before doing any work:

| Key | What |
| --- | --- |
| `explicit_migrations` | Explicit schema migration commands |
| `legacy_backfills` | Legacy backfill / transition / rehearsal commands (directory-catalog backfill, session-37 transition, frontier bootstrap, membership backfill) |
| `database_compare` | `--compare-db`, `--db` (another database on the server) |
| `schema_audit` | Schema provenance audit |
| `sealed_batches` | Sealed tape-write batches and their observation mode |
| `small_file_manifest_maintenance` | Manifest export / validate / prune. The file-based manifest search used by restore still works |
| `stored_tar_boundary` | Stored-TAR packaging boundaries. New Stored-TAR containers exist only behind such a boundary, so **SQLite sessions always use ZIP packaging** |
| `pg_dump_backup` | `pg_dump` backup, restore-verification, migrated-database creation |

## Tests

`tests/db_backends.py` runs one behavioural suite per backend: SQLite always
(a temporary file, no server), PostgreSQL when `LTO_TEST_PG_DSN` names a
disposable server (see `tests/pg_test_guard.py`).

```powershell
python -m pytest tests/ -q        # SQLite suites run; PostgreSQL suites skip
```
