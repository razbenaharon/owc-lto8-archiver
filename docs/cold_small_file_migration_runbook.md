# Cold Small-File Migration Runbook

This migration copies existing small rows from the hot `files_index` table into
the separate cold PostgreSQL database. The cold payload table is intentionally
unindexed and is used for small-file documentation/search, not tape accounting.

## Required Order

1. Back up the hot production database and verify the dump:

   ```powershell
   python inspect_db.py --backup-postgres
   ```

2. Start the cold database if needed:

   ```powershell
   docker compose up -d cold_manifest_db
   ```

3. Dry-run the migration:

   ```powershell
   python inspect_db.py --cold-migrate-small-files --dry-run
   ```

4. Execute the copy using the verified hot backup path:

   ```powershell
   python inspect_db.py --cold-migrate-small-files --execute --yes --hot-backup-path <hot.dump>
   ```

5. Validate the exact migration snapshot:

   ```powershell
   python inspect_db.py --cold-validate-small-file-migration --heavy --migration-id <id>
   ```

6. Back up the cold database after validation succeeds:

   ```powershell
   python inspect_db.py --backup-cold-postgres
   ```

7. Dry-run hot-row removal:

   ```powershell
   python inspect_db.py --hot-remove-migrated-small-files --dry-run --migration-id <id>
   ```

8. Execute hot-row removal only with both verified backups:

   ```powershell
   python inspect_db.py --hot-remove-migrated-small-files --execute --yes --migration-id <id> --hot-backup-path <hot.dump> --cold-backup-path <cold.dump>
   ```

## Safety Rules

- Removal uses the hot-local migration snapshot table, not a fresh size query.
- Removal deletes only rows with `covered_by_hot_accounting=true`.
- Rows not covered by `directory_archive_bundles` or equivalent hot accounting
  stay in `files_index`.
- The cold DB is never used for tape used-space accounting.
- Per-tape used-space estimates must match before and after removal.
- `VACUUM FULL` and `REINDEX` are manual maintenance only and must not run
  during tape writes.

