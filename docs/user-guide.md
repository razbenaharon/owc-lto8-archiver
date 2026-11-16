# User guide

How to operate the program. Read [safety.md](safety.md) before the first tape
write.

> This version has not been validated on a real tape drive and cartridge.
> The tape steps below describe the designed behaviour, verified with
> simulated devices only. Start with a small pilot
> ([testing-and-validation.md](testing-and-validation.md#hardware-pilot)).

## Starting

```powershell
python run.py
```

Run it from an elevated PowerShell (Run as Administrator) for tape work. Not
elevated, the program still runs, but it cannot pause Windows Update for the
run and cannot add its temporary Windows Defender process exclusion for
`robocopy.exe`; it says so at the start of an archive run.

| Option | Action |
| --- | --- |
| 1 | Archive: back up `[PATHS] source_dir` to tape |
| 2 | Retrieve: search the catalog and restore files |
| 3 | Tape maintenance |
| 4 | List registered tapes, with used space against the planning budget |
| 5 | Open `config.ini` in the default editor |
| 6 | Remote archive: fetch from `[REMOTE]` and write to tape |
| 7 | Database management |
| 8 | Make sure `backup_logs\SUMMARY.csv` exists and print its path |
| 9 | Back up the catalog |
| 0 | Exit |

Other ways to start:

```powershell
python run.py --version
python run.py --backup-db
python run.py remote-archive --resume --non-interactive
```

Changes to `config.ini` take effect at the next start.

## Before every tape run

1. The cartridge is mounted and `[HARDWARE] lto_drive` is the letter it is
   mounted on. The letter can change between mounts.
2. The LTFS mount uses `sync_type=time@5`. The program checks this from LTFS
   event 61259 and refuses to write otherwise.
3. No Windows restart is pending. With the default `[WINDOWS_UPDATE]`
   settings the program refuses to start a run, and each tape write, when
   one is. `block_on_pending_reboot = false` turns the refusal off; do not
   use it.
4. Nothing else uses the tape drive: no Explorer window on it, no antivirus
   scan, no second copy job.
5. No other archive run is active:

```powershell
python inspect_db.py --print-db-target
python inspect_db.py --session-forensics
```

`--session-forensics` is read-only and safe during a live run. It reports
every active session, the archiver lock holders and the archiver processes.
`--idle-seconds <N>` changes the silence after which a session counts as
stale in the report (default 86400).

To see what the restart guard sees, without starting anything:

```powershell
python -c "import json; from src.windows_update_guard import assess_reboot_state; print(json.dumps(assess_reboot_state().as_dict(), indent=2, default=str))"
```

`"blocking": true` means an archive run would refuse to start (with the
default settings).

## Archive a local folder (option 1)

1. The program scans `source_dir`, reports the size distribution, and
   proposes a backup mode: *direct* (copy the top-level folders as they are)
   or *AUTO-PILOT* (pack files smaller than `[SETTINGS] zip_threshold_mb`
   into bundles, stage, then write).
2. It shows the plan, which can span several tapes. Type `YES` to create the
   session.
3. For each chunk it checks the mounted cartridge, runs the safety gates and
   writes. If the wrong cartridge is mounted, it asks you to change it and
   press Enter.
4. After each chunk the files are recorded in the catalog and a row is added
   to `backup_logs\SUMMARY.csv`.
5. The cartridge stays loaded. It is ejected at the end of a completed
   session only if `[HARDWARE] eject_after_session = true`, and only after
   you type `EJECT`.

If an active session exists, option 1 offers: resume from the first
incomplete chunk, abandon it and start a fresh one, or cancel.

The local path refuses to write when the cartridge label cannot be read, when
the tape is marked full, or when the chunk does not fit the tape's budget.

## Archive from a remote host (option 6)

Set `[REMOTE] remote_host`, `remote_user` and `remote_path` first. Put the
password in `.env` as `REMOTE_PASSWORD`, or use key-based SSH login.
`remote_selected_paths` limits the run to some folders under `remote_path`.

A new session:

1. Names the target cartridge: from `[REMOTE] tape_label`, or it asks. A tape
   that is not in the catalog yet is registered (it asks for the capacity).
2. Shows host, tape, paths, chunk size and free tape space, and waits for a
   typed `yes` (`confirm_before_backup = true`).
3. Scans the source, plans chunks, and runs three stages side by side: fetch
   over SSH into `staging_dir`, pack and reconcile, write ready chunks to
   tape in finite groups.

The tape is touched only inside a write group. The remote pipeline never
ejects.

If an active session exists for the same host and path, option 6 offers:
resume, abandon and start a fresh scan, or cancel.

### Without the menu

```powershell
python run.py remote-archive --resume --non-interactive
```

This continues the single active session for the configured host and path. It
never prompts and never reads standard input, so it can run detached or from
Task Scheduler. `--non-interactive` is implied and accepted for clarity.
A *new* session cannot be started this way; without `--resume` the command
stops with exit code 30:

```text
[REMOTE] --non-interactive requires --resume: a fresh session needs interactive tape selection and confirmation.
```

The exit code says why the run ended
([troubleshooting.md](troubleshooting.md#exit-codes)).

`[PIPELINE] max_write_groups_per_run = 1` makes a run stop by itself after
one write group, at a chunk boundary. Use it for a first bounded run.

## Stopping a run safely

A run may be stopped only at a chunk boundary.

**In the foreground:** press Ctrl+C once. The tape copy in flight is not
killed; it finishes, the chunk is committed, and the run stops. Staged packs
are kept and reused by the next run. The session stays resumable.

**Detached run:** find the process ID of the real interpreter (a run started
from a virtual environment shows a small launcher and a larger child; use
the child), then:

```powershell
tasklist | findstr /i robocopy
python scripts\graceful_stop.py <pid>
```

`graceful_stop.py` sends the same Ctrl+C signal. It refuses while a
`robocopy` process is running, and when it cannot read the process list.
`--allow-active-tape-write` overrides the refusal; use it only with this
version of the archiver, which isolates its tape copy from the signal.

Do not use `taskkill /F` or close the window during a tape write. If the
process is truly hung (no CPU use, no progress for a long time) and you must
kill it, kill only `python.exe`, never the LTFS service, and expect the chunk
that was being written to be left in state `backing`.

After any stop:

```powershell
python inspect_db.py --session-forensics
```

## Restore (option 2)

| Option | Search |
| --- | --- |
| 1 | File name or wildcard, for example `*.mov`, `IMG_*` |
| 2 | Date range (backed up from / to, `YYYY-MM-DD`) |
| 3 | Name and date range |
| 4 | Full directory: catalogued files at or above the index threshold only |
| 5 | Full backup session, chosen from a dated list |
| 6 | Full directory, complete: whole bundles, including packed small files |
| 7 | Small files that were exported to local manifests and pruned from the catalog |

Results are shown in pages. Enter a file ID to restore one file, `N` / `P` to
change page, `ALL` to restore everything that matched (a large result asks
you to type `RESTORE ALL`), or `0` to cancel.

For each restore the program:

1. checks the mounted cartridge and asks you to change it if another tape is
   needed;
2. copies the bundle, or the loose file, from tape to `staging_dir`;
3. extracts the wanted files into `restore_dir` and removes the staged
   bundle. Files from the same bundle on one page cost one tape copy.

At the end it prints what happened:

```text
[RESTORE] Complete. 12 file(s) restored to: D:\staging\restored
```

or, when anything is missing:

```text
[RESTORE] INCOMPLETE. 10 of 12 file(s) restored to: D:\staging\restored  (failed: 2, skipped: 0)
[RESTORE] NOT restored:
  - <path>: <reason>
```

An incomplete restore makes the program exit with a non-zero code.

Restored names can differ from the catalogued ones. Every destination is
kept inside `restore_dir`, and characters Windows does not allow in a file
name (including `:`), trailing dots and spaces, and reserved device names
such as `CON` and `NUL` are neutralised, usually by replacing the character
with `_`. Legacy ZIP members are extracted atomically; one bad member is
reported and the rest continue.

## Tape maintenance (option 3)

| Option | What it does | Confirmation |
| --- | --- | --- |
| 1 Format tape | Runs `LtfsCmdFormat.exe`. **Erases the cartridge.** Refused for a cartridge that already has a label (see below) | `y`, then a new label |
| 2 Register tape manually | Adds a tape to the catalog (label and capacity). Does not touch the drive | - |
| 3 List available drives | `wmic logicaldisk` | - |
| 4 Check tape | Runs `LtfsCmdCheck.exe`, which **repairs** the LTFS filesystem and can roll back the index. Last resort | type `REPAIR` |
| 5 Tape drives info | `LtfsCmdDrives.exe` | - |
| 6 Eject tape | `LtfsCmdEject.exe`. **Physical.** A cartridge cannot be loaded again by software | type `EJECT` |

`REPAIR` and `EJECT` must be typed exactly, in capitals, on an interactive
console. Anything else, including Enter, a piped answer or a closed standard
input, cancels and does nothing.

**Re-formatting a cartridge that already has a label** is refused from this
menu and directed to the destructive reset command, which takes two steps.

Step 1 writes the impact report. It changes nothing:

```powershell
python run.py --backup-db
python run.py tape-reset --tape <LABEL> --prepare-impact-report <FILE> --backup-file <BACKUP>
```

It reads the catalog through a read-only connection, the IBM LTFS log and
the label of the mounted cartridge (the reset later insists on that same
cartridge; no file on it is opened), verifies the backup, and writes
`<FILE>`, which must not exist yet. It never formats, ejects or writes to a
cartridge and never writes to the catalog. The printed summary lists what a
reset would delete and how many chunks would return to `pending`. On
PostgreSQL the backup is a dump (`python inspect_db.py --backup-postgres`)
and must be proven restorable: name a scratch server and the dump is
restored there into a new database, checked, and that database is dropped:

```powershell
python run.py tape-reset --tape <LABEL> --prepare-impact-report <FILE> --backup-file <DUMP> --scratch-host <HOST> --scratch-port <PORT> --scratch-user <USER>
```

The password comes from `PGPASSWORD` or the PostgreSQL password file;
`--scratch-docker-container <NAME>` uses the `pg_restore` of a container.

Step 2 is the reset itself:

```powershell
python run.py tape-reset --tape <LABEL> --impact-report <FILE> --delete-catalog --yes
```

All four are mandatory. It formats the mounted cartridge, removes its
catalog records and returns its chunks to `pending`; it never ejects. It
refuses if the catalog, the backup or the mounted cartridge differ from what
the report recorded; prepare a new report then.
Treat it as an expert operation that has not been run on hardware. If a
reset fails, the error file in `tape_reset_audits\` holds the command that
resumes it (`--resume-operation`). A reset interrupted during the format
itself needs a person at the drive. Stored TAR chunks cannot be written
again after a reset; ZIP chunks can.

**Retiring a tape.** To take a tape out of the write rotation (full, or
read-only after an error), mark it full: option 7 (Database management), then
7 (Mark tape FULL / re-activate), or *Mark Full / Re-activate* in the
inspector. A tape marked full is refused by both archive paths. Do not edit
the used-space number instead; it is recalculated from the catalog.

## Database management (option 7)

| Option | Action | Confirmation |
| --- | --- | --- |
| 1 | Delete a tape and all its file records | type `yes` |
| 2 | Delete one file record by ID | type `yes` |
| 3 | Rename a tape label. Refused for a tape that has loose-file restore routes | - |
| 4 | Set tape capacity (GB) | - |
| 5 | Recalculate a tape's used space from the catalog | - |
| 6 | Delete all file records of a tape, keep the tape entry | type `yes` |
| 7 | Mark a tape FULL, or re-activate it | type `yes` |

These change the catalog only. They never touch the tape.

## Catalog backups (option 9)

The tape holds the payload only. Without the catalog, the program cannot say
which cartridge holds a file. Back it up after every run:

```powershell
python run.py --backup-db
```

- SQLite: an online copy to `db_backups\lto_archive_<timestamp>.sqlite3`,
  then an integrity check and a foreign-key check of the copy.
- PostgreSQL: a custom-format `pg_dump` in `db_backups\`.
- The backup is refused when `db_backups\` is on the drive letter configured
  as `lto_drive`. Keep the project folder on a different drive from the
  tape. With `lto_drive` empty the refusal does not apply.
- Copy the backups, and the `[LOCAL_MANIFEST_ARCHIVE] root` folder, to
  another machine or medium. A backup next to the catalog protects against
  corruption, not against losing the disk.

**Restoring a catalog backup.** There is no restore command.

- SQLite: stop every run, then put a copy of the backup file at the
  `sqlite_path` location (keep the damaged file under another name first).
  Start the program; it checks the schema when it opens the file.
- PostgreSQL: restore the dump into an empty database with `pg_restore`, and
  point `[DATABASE] dbname` at it.

Neither restore path was exercised for this release beyond the SQLite backup
verification itself. Test a restore of your own backup before you depend on
it.

## Reports and logs

| File | Content |
| --- | --- |
| `backup_logs\SUMMARY.csv` | One row per backup: bytes, times, throughput, tape, source host. No file names |
| `backup_logs\archiver.log` | Rotating diagnostic trace with full error tracebacks |
| `backup_logs\tape_write\` | The raw `robocopy` log of each tape write and its classification |
| `db_backups\` | Catalog backups |

Set `LTO_DEBUG=1` in the environment to see the full traceback of an error
on the console.

## Inspector GUI

```powershell
python -m pip install -r requirements-gui.txt
python inspect_db.py
```

| Tab | Use |
| --- | --- |
| Files | Browse by tape and directory; details; delete selected records |
| Search | Search names and paths, with a source-host filter and paged results |
| Directory Catalog | Bundles, directory statistics, find a directory, manifest search, validate |
| Manage | *Tapes*: rename, set capacity, recalculate used space, mark full / re-activate, wipe file records, delete tape. *Sessions*: delete a session, clean unused session data |

Destructive actions ask for a typed confirmation. Deleting an active session
is refused. The tape usage bar shows one percentage, measured against the
planning budget. The GUI changes the catalog only; it never touches the tape.

`inspect_db.py` also has command-line reports and maintenance commands; with
any of them no window opens:

```powershell
python inspect_db.py --print-db-target
python inspect_db.py --session-forensics
python inspect_db.py --all-session-health
python inspect_db.py --validate-directory-catalog
python inspect_db.py --manifest-search "*.csv" --limit 20
python inspect_db.py --cleanup-session-data
python inspect_db.py --reconcile-stale-sessions --dry-run
python inspect_db.py --reconcile-stale-sessions --execute --yes
```

`--reconcile-stale-sessions` closes sessions that are marked active but
provably dead: it stops if any archiver lock holder or archiver process
exists, needs the session to have been silent for `--idle-seconds`, and
leaves alone any session with a chunk in `fetching`, `packing` or `backing`.
Run `--dry-run` first. `python inspect_db.py --help` lists every command;
many of them are PostgreSQL schema and migration tools described in
[installation.md](installation.md#schema-on-postgresql).

## Supervising a long run

`scripts\archive_watchdog.ps1` is an optional Task Scheduler helper from the
original deployment. It restarts a stopped remote session only when the
recorded stop reason and exit code allow it, and refuses otherwise. It was
written for a PostgreSQL catalog and one specific host, and was not
exercised for this release. Read its header and
[troubleshooting.md](troubleshooting.md#exit-codes) before using it. Do not
run the only monitor of a run on the same host as the run.
