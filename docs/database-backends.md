# Database backends

The catalog (tapes, files, sessions, chunks) lives in one of two databases,
chosen in `config.ini`:

```ini
[DATABASE]
backend = sqlite
sqlite_path = lto_archive.sqlite3
```

| | SQLite | PostgreSQL |
| --- | --- | --- |
| Setup | None. One local file, created with its complete schema on first run | A server: `docker compose up -d db`, or your own |
| Python packages | `requirements-core.txt` | `requirements-postgres.txt` (adds psycopg 3) |
| Choose it for | A new installation on one archive host | An existing installation; a very large catalog; fast substring search |
| Name search | Scans the table. Case-insensitive for ASCII letters only | Trigram indexes (`pg_trgm`) |
| Packaging of small files | ZIP only | ZIP, or Stored TAR behind a packaging boundary |
| Backup | `python run.py --backup-db` (online copy + integrity check) | `python run.py --backup-db` (`pg_dump`, custom format) |
| Single-writer lock | Operating-system file lock on `<database>.archiver.lock` | Session advisory lock |
| Features | Everything except the [PostgreSQL-only list](#postgresql-only-features) | Everything |

**A `config.ini` with no `backend` line keeps using PostgreSQL.** An existing
installation is never switched by an upgrade. Only `config.example.ini` and
the config generated on first run select SQLite.

**There is no tool that copies a catalog from one backend to the other.**
Choose before the first archive run.

## SQLite

- The file and the whole schema (`scripts/sql/sqlite/001_schema.sql`) are
  created automatically the first time the program starts.
- The file must be on a **local disk**. A network path is refused.
- The catalog refuses to open when its schema was changed behind the
  program's back: a migration file edited after it was applied, a dropped or
  added trigger, an altered table, a file that is not a catalog, or a catalog
  written by a newer version.
- The explicit PostgreSQL migration commands (`inspect_db.py --apply-*-schema`)
  answer `"schema": "installed at creation"` and change nothing.
- Timestamps are stored as UTC text.

### Single-writer lock

Only one archive run may write at a time. On SQLite this has two layers, both
required:

1. An exclusive operating-system lock on `<database>.archiver.lock`, held for
   the whole run. The operating system releases it when the process exits,
   crashes or is killed, so a dead run cannot leave the lock stuck.
2. An owner record in the `archiver_lock` table, used to detect a lost lock.

The lock is verified before every tape write and never re-acquired silently.
A second run started while one is active stops with
`[LOCK] Another archiver instance already holds the tape-writer lock`.

### Backup

`python run.py --backup-db` (menu option 9) copies the live catalog with the
SQLite online backup API into `db_backups/`, then runs
`PRAGMA integrity_check` and `PRAGMA foreign_key_check` on the copy. A copy
that fails is discarded and never given its final name.

## PostgreSQL

- `docker compose up -d db` starts PostgreSQL 17 in a container named
  `lto_pg`, published on `127.0.0.1:5432` only, with the `pg_trgm` and
  `btree_gin` extensions (`scripts/pg_init/00_extensions.sql`). The password
  comes from `PGPASSWORD` in `.env`.
- **A new, empty database gets the complete schema on first start**, and the
  program prints one line saying so. "Empty" means the database held no table
  or view at all before that start. "Complete" means everything an archive
  run needs; the optional sealed-batch tables (migration 012) are never
  installed automatically.
- **An existing catalog is never migrated implicitly** beyond the additive
  start-up migrations. See
  [installation.md](installation.md#schema-on-postgresql) for the list and
  the explicit commands.
- Backup: `python run.py --backup-db` writes a custom-format `pg_dump` to
  `db_backups/`, using the local Docker container when it exists and
  otherwise `pg_dump` from `PATH`.

## PostgreSQL-only features

The single source is `POSTGRESQL_ONLY_FEATURES` in `src/db_backend.py`. On
SQLite each one stops before doing any work, with one line and exit code 1:

```text
[DB] Not available on the SQLite backend: <feature>. This feature needs PostgreSQL ([DATABASE] backend = postgresql). Nothing was changed.
```

| Key | What refuses on SQLite |
| --- | --- |
| `explicit_migrations` | Explicit schema migration commands. `inspect_db.py --create-migrated-db` refuses; the `--apply-*-schema` commands do not refuse but report that the schema was installed at creation |
| `legacy_backfills` | Legacy backfill, transition and rehearsal commands: directory-catalog backfill, session transition, frontier bootstrap, membership backfill, plan-1 rehearsal |
| `database_compare` | Another database on the same server: `inspect_db.py --db`, `--compare-db` with `--with-db` |
| `schema_audit` | `inspect_db.py --schema-provenance-audit` |
| `sealed_batches` | Sealed tape-write batches and their observation mode (`[FEATURES]`) |
| `small_file_manifest_maintenance` | Manifest export, validation and pruning (`--manifest-status`, `--export-small-file-manifests`, `--validate-local-manifest-export`, `--prune-exported-small-files`). The file-based manifest search used by restore still works |
| `stored_tar_boundary` | Stored TAR packaging boundaries. New Stored TAR containers exist only behind such a boundary, so **SQLite sessions always use ZIP** |
| `pg_dump_backup` | `pg_dump` based backup, restore verification and migrated-database creation (`inspect_db.py --backup-postgres`, `--create-migrated-db`) |

## How the two backends share one code base

For contributors. The catalog method groups (`src/pg_catalog.py`,
`pg_sessions.py`, `pg_tapes.py`, `pg_scan.py`, `pg_containers.py`,
`pg_directory_catalog.py`, `pg_tape_reset.py`) are the same classes on both
backends and never test which one they run on. Everything engine-specific is a
hook in the core:

| Piece | PostgreSQL | SQLite |
| --- | --- | --- |
| Manager | `src/pg_db.PgDatabaseManager` | `src/sqlite_db.SqliteDatabaseManager` |
| Core hooks | `src/pg_core.PgConnectionCore` | `src/sqlite_core.SqliteConnectionCore` |
| Driver | psycopg 3 | `src/sqlite_adapter.py` (psycopg-shaped) and `src/sqlite_dialect.py` |
| Stand-alone connections | `src/db_backend.connect()` | `src/db_backend.connect()` |
| Backup | `src/pg_backup.py` | `src/db_backup.py` |

SQL is written once, in PostgreSQL syntax. On SQLite every statement passes
through a small, closed list of lexical rewrites; anything PostgreSQL-only
that is left over raises instead of running half-translated.
`tests/test_sqlite_prepare_all.py` checks every literal statement on SQLite.

The SQLite schema is the fully migrated PostgreSQL schema: same tables,
columns, uniqueness, CHECK and foreign-key constraints. Later changes go in a
new `scripts/sql/sqlite/002_*.sql`, never by editing `001`.
`tests/test_sqlite_schema_parity.py` compares the two.

### How each PostgreSQL guard is enforced on SQLite

| PostgreSQL guard | SQLite |
| --- | --- |
| Chunk status may not leave `backing` or `done` without a named override (020) | Trigger `trg_remote_chunks_status_transition`; the override row is consumed in the same statement |
| Write-once packaging format, container identity, artifact identity, bundle format (015) | Triggers of the same names |
| Initial chunk format must match the session's format (015) | Trigger `trg_remote_chunks_initial_format` |
| Session packaging default is immutable; a new session cannot default to Stored TAR (015) | Triggers `trg_remote_sessions_packaging_default[__upd]` |
| Container tape generation must match the session and tape (015) | Triggers `trg_archive_containers_tape_generation[__upd]` |
| Schema-authority rows are immutable (015/018/019) | Triggers; the one bootstrap insert is allowed only into an empty table while the creating transaction carries the schema checksum |
| Packaging boundary rows are immutable (015) | Triggers refuse every write (boundaries are PostgreSQL-only) |
| Directory parent/depth rules, coverage scope, sealed artifacts, directory-part routing (018) | Triggers of the same names |
| Chunk plan-source rules and the audited `legacy_db` to `manifest` transition (018) | Triggers `trg_remote_chunks_plan_source_guard[__upd/__audit]` |
| Session plan-transition state ladder and boundary history (018) | Triggers `trg_remote_session_plan_transitions_guard[__upd/__del]` |
| `lto_assign_new_chunk_format()`, `lto_transition_chunk_plan_source()` | The same rules in Python, in the same transaction (`SqliteConnectionCore._sql_*`); the triggers still check the resulting write |

Deliberate differences, each on the stricter side:

- Trigger messages are static text (no interpolated ids).
- Guards that read a PostgreSQL setting read the table `lto_txn_settings`,
  whose rows exist only inside the write transaction that set them.
- The manifest-authority check runs immediately, not at COMMIT.
- The sealed-batch probe inside the plan-source guard is omitted: sealed
  batches do not exist on SQLite.

## Tests

`tests/db_backends.py` runs each behavioural catalog suite once per backend:
SQLite always (a temporary file, no server), PostgreSQL when
`LTO_TEST_PG_DSN` names a disposable server. See
[testing-and-validation.md](testing-and-validation.md).
