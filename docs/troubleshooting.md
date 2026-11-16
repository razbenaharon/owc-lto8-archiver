# Troubleshooting

Messages the program prints, what they mean, and what to do. The texts are
taken from the code; `...` stands for a value that varies.

First steps for any problem:

```powershell
python run.py --version
python inspect_db.py --print-db-target
python inspect_db.py --session-forensics
```

The full traceback of an error is in `backup_logs\archiver.log`. To see it on
the console, set `LTO_DEBUG=1` for one run:

```powershell
$env:LTO_DEBUG = "1"; python run.py
```

Before you post a log or a config in a bug report, remove host names, user
names, paths and file names. Never post `.env`.

**After any error during a tape write: stop. Do not retry, eject, remount,
format or run a filesystem check until you know what happened**
([safety.md](safety.md)).

## Exit codes

Defined in `src/exit_codes.py`. The values are a stable contract.

| Code | Name | Meaning | May a supervisor restart the run? |
| --- | --- | --- | --- |
| 0 | `COMPLETED` | Finished; nothing failed | - |
| 10 | `TRANSIENT_RESUMABLE` | Stopped for a reason that clears by itself | Yes, except after `tape_write_failed` |
| 20 | `SAFETY_BLOCK` | A person must look before anything continues | No |
| 30 | `FATAL_CONFIG` | Configuration, credentials or usage error; also any other error that stops the program, a failed catalog backup, and a failed or incomplete restore | No |
| 40 | `USER_STOP` | Ctrl+C or a deliberate stop | No |
| 2 | (argparse) | A command-line flag was not accepted | No |
| 1 | (`inspect_db.py`) | The command refused or failed; one line on standard error | No |

From the interactive menu, the exit code is that of the last failed archive,
restore or backup action (0 if none failed). A mistyped menu choice or a
failed option 8 prints an error but does not change the exit code.

A remote run also records a *stop reason*:

| Code | Reason | What happened | What to do |
| --- | --- | --- | --- |
| 10 | `network_retry_exhausted` | Fetch retries ran out on a network error | Check the network and the source host; resume |
| 10 | `sccm_reboot_pending`, `windows_reboot_pending` | A restart is pending; the run refused to start a write | Restart the PC, then resume |
| 10 | `stopped_at_chunk_boundary` | A clean stop between chunks | Resume when ready |
| 10 | `tape_write_failed` | A tape copy failed | **Do not resume yet.** Read the log in `backup_logs\tape_write\`, check the drive and cartridge, then decide |
| 20 | `ambiguous_backing_chunk` | A chunk was left in `backing` by an earlier run | Establish whether it is on tape. Never clear it blindly |
| 20 | `ltfs_sync_mode_not_time5` | The mount is not `sync_type=time@5` | Fix the LTFS setting; it takes effect at the next mount, with somebody at the drive |
| 20 | `ltfs_mount_unverifiable` | The mount or its sync mode could not be confirmed | Check that the cartridge is mounted and `lto_drive` is right |
| 20 | `ltfs_media_degraded` | The IBM LTFS log shows a drive or medium fault since the mount | Diagnose the drive and cartridge before any write |
| 20 | `ltfs_ownership_unavailable` | Another process holds the tape drive, or `ltfs_ownership_id` is missing. Nothing was written | Find the other process; set the identity |
| 20 | `unexpected_tape_or_db_state` | Cartridge, tape generation or catalog do not match the session | Compare the mounted label with the session's tape |
| 20 | `ambiguous_active_sessions` | More than one active session matches | Resolve with `--session-forensics` and `--reconcile-stale-sessions` |
| 20 | `archiver_lock_lost` | The single-writer lock can no longer be proven | Make sure no other archiver runs; start again |
| 20 | `scan_frontier_unavailable` | The scanner's schema is missing or incomplete (PostgreSQL, existing catalog) | Apply migration 014 ([installation.md](installation.md#schema-on-postgresql)) |
| 20 | `sealed_batch_feature_unavailable` | Sealed batches are enabled but not installed | Set the `[FEATURES]` flag back to `false`, or apply migration 012 |
| 30 | `bad_config` | Incomplete `[REMOTE]`, lock not acquired, or another start-up error | Read the message printed with it |
| 30 | `no_active_session` | `remote-archive --resume` found no session to resume | Start a session from menu option 6 |
| 30 | `noninteractive_requires_resume` | `remote-archive` without `--resume` | Add `--resume`, or use the menu |
| 30 | `ssh_authentication_failed`, `ssh_permission_denied`, `ssh_host_key_mismatch`, `missing_noninteractive_credential` | SSH login problem | See [Remote host](#remote-host) |
| 40 | `user_requested_stop` | Ctrl+C | Resume when ready |

## Configuration

```text
[CONFIG] ...\config.ini has 3 invalid setting(s); refusing to start rather than guess:
  [DATABASE] backend = 'sqlit': must be one of sqlite / postgresql.
  [WINDOWS_UPDATE] guard = 'true ; keep on': not a yes/no value. Use one of true/false/yes/no/on/off/1/0. Inline comments are not supported - put the comment on its own line.
  [WINDOWS_UPDATE] pause_days = '-3': must be at least 1.
```

Fix each line named. Exit code 30.

| Message | Cause | Fix |
| --- | --- | --- |
| `Inline comments are not supported - put the comment on its own line.` | Text after a value, starting with `;` or `#` | Move the comment to the line above |
| `not a yes/no value. Use one of true/false/yes/no/on/off/1/0.` | A typo in a yes/no setting | Correct it. A typo is never read as "false" |
| `not a number.` / `must be a whole number.` / `must be at least ...` / `must be greater than ...` / `must be at most ...` / `must be a finite number (not inf/nan).` | A number outside what the key allows | See the Type column in [configuration.md](configuration.md) |
| `must be one of ... / ...` | A value outside the listed choices | Use one of the listed words |
| `[CONFIG] WARNING: [X] key is not a known setting and is ignored. Did you mean '...'?` | A misspelled or obsolete key. The setting you meant is **not** applied | Correct the name |
| `[CONFIG] WARNING: [x] is not a known section; everything in it is ignored. ... Section names are case-sensitive.` | A misspelled or lower-case section | Use the upper-case name |
| `[CONFIG] WARNING: [X] key has no effect (...)` | An inert key | Remove it |
| `[CONFIG] WARNING: [X] key is retired and ignored (...). Remove it.` | A key from an older version | Remove it |
| `[CONFIG] Created default config file: ...` | There was no `config.ini`; one was written with defaults | Edit it before any operation |
| `[CONFIG] Invalid [PIPELINE] ready-queue limits (...); using defaults ...` | `min` <= `target` <= `max` does not hold | Correct the three values |
| `[PIPELINE] ready-queue limits cannot coexist with staging: ...` | The ready queue plus its reserve is larger than `staging_max_gb` | Raise `staging_max_gb` or lower the reserve |
| `[REMOTE] The [REMOTE] section in config.ini is incomplete.` | `remote_host`, `remote_user` or `remote_path` is empty | Fill them in |
| A folder such as `C:\path\to\backup_logs` appears | Placeholder paths from `config.example.ini` were left in `config.ini` | Set real paths; delete the stray folder |

## Database

| Message | Cause | Fix |
| --- | --- | --- |
| `[DB] Could not connect to PostgreSQL at host:port (database '...', user '...').` | Server not running or not reachable; wrong host, port or password. The lines after it give hints | Local container: `docker compose up -d db`, then `docker ps --filter name=lto_pg`. Check `PGPASSWORD` in `.env` |
| `[DB] No PostgreSQL password is configured. Copy .env.example to .env or set PGPASSWORD ...` | No password found | Create `.env` |
| `[PG] psycopg 3 is not installed. Run ...` | `backend = postgresql` (or no `backend` line) with only the core packages installed | `python -m pip install -r requirements-postgres.txt`, or set `backend = sqlite` for a new catalog |
| `[DB] This PostgreSQL database was empty, and installing the complete catalog schema stopped at migration ...` | The first-start installation failed part way | Fix the cause it names and start again; it continues where it stopped |
| `[DB] Directory catalog indexing is enabled for packed archive runs, but the explicit directory catalog schema migration is not installed on this database.` | An existing PostgreSQL catalog without migration 007 | [installation.md](installation.md#schema-on-postgresql) |
| `[DB] The folder for the SQLite catalog does not exist: ...` | `sqlite_path` points into a missing folder | Create the folder or correct the path |
| `[DB] The SQLite catalog schema does not match ...` / `[DB] SQLite migration ... was changed after ...` | The schema file or the database was modified outside the program | Restore the original `scripts/sql/sqlite/*.sql`; if the database itself was altered, restore a catalog backup |
| `[DB] This SQLite catalog was written by a NEWER version ...` | An older program on a newer catalog | Use the newer program |
| `[DB] Not available on the SQLite backend: .... This feature needs PostgreSQL ([DATABASE] backend = postgresql). Nothing was changed.` | A PostgreSQL-only feature on SQLite. Exit code 1 | [The list](database-backends.md#postgresql-only-features) |
| `--validate-container-format-schema checks migration 015 on its own and cannot pass on this database` | Expected on a complete schema (migrations 016/017 extend the same tables); exit code 1 | Run `python inspect_db.py --validate-container-format-schema-authority-v2` |
| `Container-format schema missing or drifted: ...` from `--validate-container-format-schema` | The schema differs from migration 015 (016/017 not installed, or another issue is listed) | Read the listed issues; restore from a backup or re-apply the migration |

## Lock

| Message | Cause | Fix |
| --- | --- | --- |
| `[LOCK] Another archiver instance already holds the tape-writer lock. Only one tape-write run may be active at a time; finish or close the other instance and retry.` | Another `run.py` is archiving on this catalog | Wait, or stop the other run safely. On SQLite the lock is released by the operating system when that process ends, so there is nothing to clear by hand. On PostgreSQL a holder may also be a session on another database of the same server |
| `[LOCK] Refusing the archiver lock on a network or non-local path: ...` | The SQLite catalog is on a network share | Move it to a local disk |
| `[LOCK] Cannot open the archiver lock file ...` | The folder of the catalog is not writable | Fix the permissions |
| `[LOCK] The archiver lock is not held by this process.` / `... can no longer be proven ...` | The lock was lost during a run (database session died, lock file replaced). The run stops before the next write | Make sure only one archiver runs; start again |
| `[MIGRATION 014] Refusing to migrate while the archiver lock is held` | A migration command during an archive run | Stop the run first |

To see who holds the lock:

```powershell
python inspect_db.py --print-db-target
```

A session that is marked active although nothing runs blocks some
maintenance. Prove it stale and close it:

```powershell
python inspect_db.py --reconcile-stale-sessions --dry-run
python inspect_db.py --reconcile-stale-sessions --execute --yes
```

## Tape and drive

| Message | Cause | Fix |
| --- | --- | --- |
| `[HARDWARE] ltfs_ownership_id is not set. It is required ...` | No drive identity configured; every tape operation is refused | Set `[HARDWARE] ltfs_ownership_id` |
| `[HARDWARE] ltfs_ownership_id=... is malformed.` | Characters outside `A-Z a-z 0-9 . _ -`, or longer than 64 | Correct it |
| `[TAPE] LTFS mount declared sync mode ..., expected time@300s. Refusing to start tape writes ...` | The mount is not `time@5` | Change `sync_type` in the LTFS configuration; it applies at the next mount |
| `[TAPE] LTFS reports drive/medium .... Refusing to start a tape write ...` | A fault event in the IBM LTFS log since the mount | Diagnose before writing. A retry into a failing drive can make the cartridge read-only for good |
| `[WU] Windows has a restart pending:` | A restart is staged | Restart the PC first. `block_on_pending_reboot = false` overrides, at the risk of losing the run |
| `[WU] Running without Administrator privileges - cannot pause Windows Update.` | Not elevated | Run PowerShell as Administrator for tape work |
| `[DEFENDER] Running without Administrator privileges. Cannot verify or modify Windows Defender exclusions.` | Not elevated | As above, or exclude `robocopy.exe` yourself |
| `[CANCELLED] Standard input is not interactive, so 'EJECT' cannot be confirmed. Nothing was done.` | Eject or repair was reached with piped input | Run it from an interactive console, at the drive |
| `[REFUSED] Formatting an existing labelled cartridge is no longer available from the ordinary maintenance menu.` | The cartridge already has a label | See *Tape maintenance* in [user-guide.md](user-guide.md#tape-maintenance-option-3) |
| `[TAPE RESET] Both --delete-catalog and --yes are mandatory` | `tape-reset` without both flags | Add them, once you are sure |
| `[DB BACKUP] Refusing to read or write a backup artifact on the LTFS drive` | `db_backups\` is on the drive letter configured as `lto_drive` | Keep the project folder on another drive. If no tape drive is attached, leave `lto_drive` empty |

`robocopy` problems are reported by the classifier, not by the exit code.
The raw log of every tape write is kept in `backup_logs\tape_write\`.

## Remote host

| Symptom | Cause | Fix |
| --- | --- | --- |
| `ssh_authentication_failed`, `ssh_permission_denied` | Wrong user or password; key not accepted | Test `ssh <user>@<host>` by hand. Set `REMOTE_PASSWORD` in `.env`, or install a key |
| `ssh_host_key_mismatch` | The host key changed | Verify the new key with the host's administrator, then correct `known_hosts`. Do not just delete the entry |
| `missing_noninteractive_credential` | A headless run needs a password and none is configured | Set `REMOTE_PASSWORD`, or use a key |
| `network_retry_exhausted` | Repeated DNS, timeout or reset errors | Fix the network; raise `fetch_transient_retries` if short outages are normal |
| Fetch aborts with a stall or overrun message | No bytes for `fetch_stall_timeout_seconds`, or more bytes than planned times `fetch_overrun_abort_factor` | The retry takes over. If it repeats, the source changed during the run |
| Files reported missing at the source | The source host positively reported them missing | They were deleted after the scan; they are recorded as missing, not archived |

The source host needs `bash` and GNU `tar` 1.23 or newer. `use_mbuffer = true`
needs `mbuffer` installed there.

## Restore

| Message | Cause | Fix |
| --- | --- | --- |
| `[RESTORE] INCOMPLETE. N of M file(s) restored ...` followed by `NOT restored:` | Some files could not be restored; each has a reason | Fix the cause (wrong cartridge, read error, full disk) and restore those files again. Exit code is non-zero |
| `[ERROR] Could not copy ZIP from tape: robocopy error` | The copy from tape failed | Check the mount and the cartridge. Do not loop retries on a failing drive |
| `[RETRIEVER] No directory-catalog bundle found for that path.` | Option 6 found no bundle for the directory | Try option 4, or search by name |
| `[RETRIEVER] Local manifest archive is not configured.` | Option 7 without `[LOCAL_MANIFEST_ARCHIVE] root` | Set it |
| A restored file has a different name | The catalogued name is not legal on Windows, or would have left `restore_dir` | Expected; see [user-guide.md](user-guide.md#restore-option-2) |

## Inspector GUI

| Symptom | Cause | Fix |
| --- | --- | --- |
| `ModuleNotFoundError: No module named 'PySide6'` | GUI packages not installed | `python -m pip install -r requirements-gui.txt` |
| A dialog says the database cannot be reached | Server down or wrong settings | As under *Database* |
| Deleting a session is refused | The session is active | Stop the run; if it is stale, reconcile it (see *Lock*) |

## Commands that no longer exist

`python run.py` with `--optimize-db`, `--catalog-v3-preflight`,
`--catalog-v3-migrate`, `--catalog-rebuild`, `--hashless-origin-migrate` or
`--dry-run` stops with `These legacy maintenance flags were removed long
ago.` They belonged to a storage layout that no longer exists.
