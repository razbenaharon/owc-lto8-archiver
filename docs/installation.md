# Installation

There is one package for both catalog backends. Pick the dependency set that
matches the backend you want. Read [database-backends.md](database-backends.md)
if you have not chosen yet; there is no tool that moves a catalog from one
backend to the other.

## Before you start

- Windows, Python 3.11 to 3.14 (`python --version`).
- For tape work: an LTO drive, its driver, and IBM LTFS for Windows installed
  in `C:\Program Files\IBM\LTFS\`, with a formatted cartridge mounted as a
  drive letter. These are vendor products and are not part of this
  repository. You can install and try everything except tape writes without
  them.
- For remote archive: the OpenSSH client on this PC (`ssh -V`), and on the
  source host `bash` and GNU `tar` 1.23 or newer (`tar --version`).
- For PostgreSQL with the bundled compose file: Docker Desktop.

Get the code (a release zip, or `git clone`), open PowerShell in its folder,
and create a virtual environment:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

## Option 1: SQLite

```powershell
python -m pip install -r requirements-core.txt
python run.py --version
Copy-Item config.example.ini config.ini
notepad config.ini
```

Edit `config.ini`:

1. `[PATHS]`: set `source_dir`, `staging_dir` and `restore_dir` to real
   folders. Delete the `backup_log_dir` line to keep logs in `backup_logs\`
   inside the project folder, or point it at a real folder. The placeholder
   paths are used literally if you leave them.
2. `[DATABASE]`: keep `backend = sqlite`. `sqlite_path` must be on a local
   disk.
3. `[HARDWARE]`: set `lto_drive` to the LTFS drive letter and
   `ltfs_ownership_id` to a stable name for the physical drive (its serial
   number is a good choice). With no tape drive attached, leave `lto_drive`
   empty.
4. `[LOCAL_MANIFEST_ARCHIVE]`: set `root` to a real folder on durable storage.
5. `[REMOTE]`: fill in only if you will archive from a remote host.

Start the program:

```powershell
python run.py
```

The first start creates `lto_archive.sqlite3` with the complete schema.
Nothing else needs to be installed.

## Option 2: PostgreSQL

```powershell
python -m pip install -r requirements-postgres.txt
python run.py --version
Copy-Item config.example.ini config.ini
Copy-Item .env.example .env
notepad .env
notepad config.ini
```

1. In `.env`, set `PGPASSWORD`. The same value is used by the container and
   by the program.
2. In `config.ini`, set `[DATABASE] backend = postgresql` and check `host`,
   `port`, `dbname`, `user`. Then edit `[PATHS]`, `[HARDWARE]`,
   `[LOCAL_MANIFEST_ARCHIVE]` and `[REMOTE]` as in Option 1.
3. Start the bundled server and the program:

```powershell
docker compose up -d db
docker ps --filter name=lto_pg
python run.py
```

On a **new, empty** database the first start installs the complete schema
and prints:

```text
[DB] New, empty PostgreSQL database: installed the complete catalog schema (migrations 007, 014 + finalize, 015, 016 + 017, 018, 019).
```

### The bundled server

`docker-compose.yml` runs PostgreSQL 17 as container `lto_pg`:

- published on `127.0.0.1:5432` only (not reachable from the network);
- database `lto_archive`, user `lto`, password from `PGPASSWORD`
  (`change_me_local` if unset: set your own);
- data in the Docker volume `lto_pgdata`;
- extensions `pg_trgm` and `btree_gin` created by
  `scripts/pg_init/00_extensions.sql` when the volume is first initialised.

### Your own server

Set `host`, `port`, `dbname`, `user` and `sslmode` in `[DATABASE]`. Use
`sslmode = require` or stricter for any server that is not on the loopback
interface. The database user needs to:

- connect to the database and create tables, indexes, functions and triggers
  in its schema (the program installs and upgrades its own schema);
- have the extensions `pg_trgm` and `btree_gin` available. Create them once
  as a superuser if the user may not:

```sql
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS btree_gin;
```

The simplest arrangement is a dedicated database owned by a dedicated user.

### Schema on PostgreSQL

The migrations are the files in `scripts/sql/`.

| When | Migrations | How |
| --- | --- | --- |
| Every start, any database | 001-006, 008-011, 013, 020 | Automatic. Additive and idempotent |
| First start on a database with no table or view at all | 007, 014 + finalize, 015, 016 + 017, 018, 019 | Automatic, once. If it is interrupted, the next start continues |
| An existing catalog that lacks them | 007, 014, 015, 016 + 017, 018, 019 | **Explicit commands only** (below) |
| Only if you enable sealed write batches | 012 | Not applied by any command: run `scripts/sql/012_sealed_tape_write_batches.sql` yourself |

An existing catalog is never migrated implicitly beyond the first row. A
packed archive run on a catalog without migration 007 stops with
`[DB] Directory catalog indexing is enabled ... not installed on this
database`.

Each explicit command prints a read-only preflight when run on its own:

```powershell
python inspect_db.py --apply-incremental-scan-schema
python inspect_db.py --apply-container-format-schema
python inspect_db.py --apply-stored-tar-plan-schema
python inspect_db.py --apply-manifest-directory-catalog-schema
python inspect_db.py --apply-container-format-schema-authority-v2
```

To apply, stop the archiver, take a verified backup, and add
`--execute --yes --backup-file <DUMP>` (the command refuses without all
three, and refuses while an archiver holds the lock). In order:

```powershell
python inspect_db.py --backup-postgres
python inspect_db.py --apply-directory-catalog-schema
python inspect_db.py --apply-incremental-scan-schema --execute --yes --backup-file <DUMP>
python inspect_db.py --apply-incremental-scan-schema --finalize --execute --yes --backup-file <DUMP>
python inspect_db.py --apply-container-format-schema --execute --yes --backup-file <DUMP>
python inspect_db.py --apply-stored-tar-plan-schema --execute --yes --backup-file <DUMP>
python inspect_db.py --apply-manifest-directory-catalog-schema --execute --yes --backup-file <DUMP>
python inspect_db.py --apply-container-format-schema-authority-v2 --execute --yes --backup-file <DUMP>
```

`--apply-directory-catalog-schema` (migration 007) has no preflight mode: it
applies at once. `--backup-postgres` writes the dump to `db_backups\` and
prints its path; it uses the Docker container `lto_pg` when one exists.

Read-only checks afterwards:

```powershell
python inspect_db.py --validate-stored-tar-plan-schema
python inspect_db.py --validate-manifest-directory-catalog-schema
python inspect_db.py --validate-container-format-schema-authority-v2
```

`--validate-container-format-schema` checks migration 015 **as it was before
016/017**. On a complete schema it stops with
`Container-format schema missing or drifted: ... fingerprint drift`. That
message is expected there and is not a fault of the catalog.

What was verified for this version: the automatic installation on an empty
database, the preflights and the three checks above, on a disposable
PostgreSQL 17 server. The explicit `--execute` route on a populated catalog
is covered by the test suite only, and `--backup-postgres` was not run for
this documentation.

## Optional: the inspector GUI

```powershell
python -m pip install -r requirements-gui.txt
python inspect_db.py
```

## Verify the installation

```powershell
python run.py --version
python inspect_db.py --print-db-target
python run.py --backup-db
```

`--print-db-target` prints which database the configuration points at and
how many archivers hold the lock. `--backup-db` proves the catalog can be
read and copied. Neither touches the tape drive.

## Upgrading an existing PostgreSQL installation

1. Stop the archiver. Take a catalog backup with your current version.
2. Replace the code, then refresh the dependencies:

   ```powershell
   python -m pip install -r requirements-postgres.txt
   ```

3. Start the program once and read the output.

What to expect:

- **Your backend does not change.** A `config.ini` with no `backend` line
  keeps using PostgreSQL.
- **One new migration is applied automatically:**
  `scripts/sql/020_postgres_chunk_transition_guard.sql`. It adds one small
  table, one index, one function and one row trigger, rewrites no data and is
  idempotent. After it, the database itself refuses to move a chunk out of
  `backing` (except to `done`) or out of `done`.
- **Strict configuration parsing may reject a `config.ini` that used to
  load.** The message names the section, key and value. The usual causes: an
  inline comment after a value (move it to its own line), a yes/no value
  that is not one of `true false yes no on off 1 0`, a number outside its
  range. Unknown keys are warnings, not errors. Compare your file with the
  new `config.example.ini`.
- **Exit codes are non-zero on failure.** Scheduled tasks and scripts that
  ignored the exit code will now see failures
  ([troubleshooting.md](troubleshooting.md#exit-codes)).
- **The local archive path no longer ejects after each chunk.** It ejects
  only at the end of a completed session, only with
  `[HARDWARE] eject_after_session = true`, and only after `EJECT` is typed.
- **Restore may write a slightly different file name** when the catalogued
  name is unsafe on Windows.
- **The watchdog script refuses to restart after more kinds of stop.**

The complete list is in the [changelog](../CHANGELOG.md) and the
[release notes](releases/v1.0.0-rc.1.md).

## Uninstall

Delete the project folder. With PostgreSQL, also stop and remove the
container:

```powershell
docker compose down
```

Adding `-v` to that command also deletes the volume `lto_pgdata`, which is
the catalog. Without the catalog, the files on tape can no longer be located
by this program.
