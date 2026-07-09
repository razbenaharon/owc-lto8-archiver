# Directory Catalog Migration Runbook

This runbook creates and validates a separate PostgreSQL database with the
directory-first catalog schema. It does not cut production over automatically.

## Preconditions

- Work from the project root: `C:\owc-lto8-archiver`.
- PostgreSQL is running and reachable with the existing `[DATABASE]` config,
  `.env`, and/or `PG*` environment variables.
- No tape-write/archive run is active.
- Do not browse, copy from, or independently read-verify the LTFS tape as part
  of this database migration.

Confirm the current target and advisory tape-writer lock:

```powershell
python inspect_db.py --print-db-target
```

Proceed only when `archiver_lock_holders` is `0` and the operator has confirmed
that no `python run.py` archive/write operation is active.

## Back Up Production

Create a verified custom-format dump of the current production DB:

```powershell
python inspect_db.py --backup-postgres
```

The command prints:

- redacted PostgreSQL target
- backup dump path in `db_backups/`
- sibling `*.restore_list.txt` created by `pg_restore --list`

Stop if the backup command fails or if either file is missing or empty.

## Create the Migrated Database

Choose a timestamped database name, for example:

```powershell
$NEW_DB = "lto_archive_directory_catalog_YYYYMMDD_HHMMSS"
```

Create, restore, and apply the explicit directory catalog schema to the new DB:

```powershell
python inspect_db.py --create-migrated-db --backup-file db_backups\prod_before_directory_catalog_lto_archive_YYYYMMDD_HHMMSS.dump --new-db $NEW_DB
```

This creates a separate PostgreSQL database, restores the verified production
dump into it, and applies only `scripts/sql/007_postgres_directory_catalog.sql`
there.

To apply the schema manually to an already-restored target DB:

```powershell
python inspect_db.py --db $NEW_DB --apply-directory-catalog-schema
```

## Backfill

Run dry-run mode first:

```powershell
python inspect_db.py --db $NEW_DB --backfill-directory-catalog --dry-run
```

If dry-run output is acceptable, run execute mode:

```powershell
python inspect_db.py --db $NEW_DB --backfill-directory-catalog --execute
```

Backfill is best-effort for legacy data. It does not delete from `files_index`,
does not purge small-file rows, and does not invent manifest paths when the old
catalog did not record enough information.

## Validate

Validate the migrated directory catalog:

```powershell
python inspect_db.py --db $NEW_DB --validate-directory-catalog
```

Compare production to the migrated DB:

```powershell
python inspect_db.py --compare-db lto_archive --with-db $NEW_DB
```

Required validation outcome:

- legacy row counts match, especially `files_index`
- directory catalog tables exist and are populated
- bundle/tree/stat totals have no mismatches
- tape used-space calculation does not double count bundle rows and legacy rows
- a second execute backfill run reports no additional rows created

## Manual Cutover

Cut over only after manual approval. Update `[DATABASE] dbname` in `config.ini`
or the relevant `PGDATABASE` environment variable to the migrated database name.

Before a real archive run after cutover, run:

```powershell
python inspect_db.py --print-db-target
python inspect_db.py --db $NEW_DB --validate-directory-catalog
```

## Rollback

Production is left untouched throughout this workflow. To roll back before or
after manual cutover, point the application back to the original production DB
name, typically:

```ini
[DATABASE]
dbname = lto_archive
```

Do not drop the migrated DB until validation records and operator approval say
it is no longer needed.
