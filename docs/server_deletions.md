# Server deletions — reclaiming space after tape archival

When a directory is deleted from a lab server, the tape becomes the **only**
remaining copy. This file defines the procedure. It deliberately contains **no
deletion records**: this repository is public, and the records name real lab
users and their file paths.

The actual log lives only on the archiver host:

- `storage_map/logs/deletions/ledger.json` — one record per deletion, carrying
  the verification numbers that justified it.
- `storage_map/logs/deletions/*_inventory.tsv.gz` — every removed file with
  size, mtime, owner and mode.
- Both are surfaced in the Storage Map dashboard as the **"Reclaimed from
  servers"** panel (`GET /api/deletions`), and both are gitignored.

Those files cannot be regenerated once the data is gone — **keep an off-repo
backup of them**.

## Procedure

1. **Verify coverage exactly — count *and* bytes must match.** Use the
   `storage_map/webapp/coverage.py` merge. Two traps:
   - `files_index` alone under-reports. Packed small files have no
     `files_index` row and live only in `directory_tree_index`.
   - `SUM(direct_file_count)` over `directory_tree_index` over-reports: that
     table holds repeat rows per chunk, so a naive sum double-counts. (This
     produced a convincing phantom 2 GB "gap" once — the arithmetic even
     matched the size of a real file.)
2. **No file may be newer than the backup.** Compare the newest mtime in the
   tree against the catalog's `catalog_backup_date`.
3. **Capture the inventory before deleting.** A `find -printf` TSV of every
   entry. It is unrecoverable afterwards.
4. **Check the tape is healthy.** `tapes.status` must be `active`; a retired or
   read-only cartridge is not a safe sole custodian.
5. **Confirm the directory is not in flight** — not queued in
   `remote_selected_paths`, not being written by a live session, and ideally on
   a different server than the one an archive is currently fetching from.
6. **Get the operator's explicit go-ahead, per directory.** This is someone
   else's research data.
7. **Record the deletion** via `storage_map.lib.deletions.append_record`.

## Permissions

The archiver account **cannot** delete these directories directly: the
`shared-data` trees are owned by individual lab users, mode `drwxr-xr-x`, with
no group write. Deletion needs `sudo`, which *is* available — `sudo -l` on both
servers returns:

```text
(root) NOPASSWD: /usr/bin/du, /usr/bin/ncdu, /usr/bin/rsync, /bin/ls, /bin/rm, /bin/chmod
```

**Test sudo with `sudo -l`, never with an arbitrary command.** A command outside
that list (e.g. `sudo -n true`) makes sudo demand a password, which reads as
"no sudo at all" and is wrong.

The parent `shared-data` directory is group-writable with no sticky bit, so the
account can *rename* a whole tree without `sudo` — but renaming reclaims no
space, and the contents still cannot be removed.
