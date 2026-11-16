# Local Small-File Manifest Export and Prune Runbook

This workflow moves completed per-file rows smaller than exactly 10 MiB from
the hot PostgreSQL catalog into permanent local `jsonl.zst` manifests. It does
not delete session, plan, chunk, snapshot, or resume-state tables.

No command below was designed to be combined with a live archive run. The
maintenance CLI refuses to proceed when the archiver advisory lock is held or
when a local fetch, pack, tar, SSH/SCP, robocopy, or archive process is found.

## 1. Configure durable storage

Set `[LOCAL_MANIFEST_ARCHIVE] root` in `config.ini` to durable local storage.
It must be outside `staging_dir` and every directory removed by cleanup.

## 2. Back up the hot database first

```powershell
python inspect_db.py --backup-postgres
```

Keep both the `.dump` and its generated `.restore_list.txt`.

## 3. Review eligibility without writing

```powershell
python inspect_db.py --export-small-file-manifests --dry-run
```

Rows whose ownership is unknown or whose run/session/chunks are not terminal
are reported by reason and remain in `files_index`.

## 4. Export, then validate

```powershell
python inspect_db.py --export-small-file-manifests --execute --yes --hot-backup-path <hot.dump>
python inspect_db.py --validate-local-manifest-export --heavy --export-id <id>
```

Validation re-hashes and decompresses every segment, checks exact source
`file_id` membership, compares row and byte totals, and checks both folder
aggregate representations.

## 5. Dry-run and execute the prune

```powershell
python inspect_db.py --prune-exported-small-files --dry-run --export-id <id>
python inspect_db.py --prune-exported-small-files --execute --yes --export-id <id> --hot-backup-path <hot.dump>
```

The execute path validates again and deletes the eligible immutable snapshot in
guarded, resumable transactions (100,000 rows per batch by default). Before
every batch it takes the same PostgreSQL advisory lock used by archive runs,
checks local archive/transfer processes, rechecks terminal session/chunk
ownership, and verifies stable `file_id` + `record_key` + size identities. Use
`--prune-batch-size` to lower the batch size. A failed current batch rolls back;
committed progress remains marked in the export snapshot for a safe resume. On
successful finalization it removes the temporary per-file snapshot rows from
PostgreSQL. Permanent folder aggregates and segment checksums remain.

## 6. Search and restore

Use retriever option 7, or:

```powershell
python inspect_db.py --manifest-search "*.mov" --limit 100
```

Manifest records contain the tape label, bundle path, ZIP member path, and
original path needed by the normal restore code.

## 7. Validate the end state

After a prune completes, run the reconciliation validator; it must pass
before any run that changes tape state:

```powershell
python scripts/validate_archive_reconciliation.py --heavy
```

(The one-time `--export-legacy-cold-db` helper used to retire the historical
`lto_cold_manifest` database was removed in 2026-08 after that retirement
completed; its export artifact lives with the local manifest archive.)
