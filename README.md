# Tape Ledger

*Formerly OWC LTO-8 Archiver.*

A Windows command-line tool that archives data to LTO/LTFS tape and keeps a
searchable catalog of what is on which cartridge.

It reads files from a local folder or from a remote host over SSH, packs small
files into bundles, writes them to an LTFS-mounted cartridge with `robocopy`,
and records every file in a catalog (SQLite or PostgreSQL) so it can be found
and restored later. Long runs are resumable: an interrupted run continues
from the first incomplete chunk.

Version `1.0.0rc1` (tag `v1.0.0-rc.1`). **This is a pre-release.**

> **Not validated on real tape hardware.** No working drive was available
> while this version was prepared. All tape-path behaviour (writing, LTFS
> readiness, cartridge checks, eject, restore from tape) was verified with
> simulated devices and temporary directories only. Do not treat it as proven
> safe for production tape writing until you have run a small pilot on your
> own drive and cartridge. See
> [docs/testing-and-validation.md](docs/testing-and-validation.md#hardware-pilot)
> and [Known limitations](#known-limitations).

## The problem it solves

Tape is cheap to keep and awkward to use. A cartridge is linear, slow to seek
and expensive to rewrite; a drive can turn a cartridge read-only in the middle
of a write; `robocopy` can exit `0` on a copy that did not land; and millions
of small files destroy tape throughput. The hard part is not writing bytes.
It is being able to say, months later, which cartridge holds a given file, and
being able to continue a multi-day run from the exact point where it stopped.

## What it does

- **Local and remote archive.** Archive a local folder, or fetch from a
  remote host over SSH into local staging first.
- **Local-first staging.** Source data never streams straight to tape. The
  tape writer's only input is validated data on local disk, so a network or
  packing failure costs local disk, never tape state.
- **Packing.** Files under a size threshold are packed into bundles; large
  files are copied as they are.
- **Resumable sessions.** Sessions, chunks and their states are stored in the
  catalog. A chunk whose tape write was interrupted is never rewritten
  automatically.
- **Catalog.** One row per large file, per-bundle manifests for small files,
  folder totals, and per-tape space accounting. SQLite or PostgreSQL.
- **Restore.** Search by name, date, directory or session; restore single
  files, whole directories or whole sessions.
- **Safety gates before every tape write.** LTFS sync mode, drive and medium
  health, pending Windows restart, cartridge identity, single-writer lock.
  A gate that cannot be evaluated blocks the write.
- **Inspector GUI** for browsing and maintaining the catalog (optional).

## Requirements

| | |
| --- | --- |
| Operating system | Windows (developed and tested on Windows 11). The program uses `robocopy`, `vol`, `wmic` and Windows drive semantics. |
| Python | 3.11 to 3.14 |
| Tape | An LTO drive and [IBM LTFS](https://www.ibm.com/products/linear-tape-file-system) for Windows (single-drive edition), with a cartridge mounted as a drive letter. Developed for an OWC Mercury Pro LTO-8. Not needed to try the catalog, the menus or a backup. |
| Catalog | SQLite (built in, nothing to install) **or** PostgreSQL 17 (Docker Desktop for the bundled `docker-compose.yml`, or your own server) |
| Remote archive only | The OpenSSH client (`ssh`) on this PC. On the source host: `bash` and GNU `tar` 1.23 or newer; `mbuffer` only if you enable `use_mbuffer` |
| Inspector GUI only | PySide6 |

Vendor software (IBM LTFS, drive firmware, HBA drivers, diagnostic tools) is
not included here and is not redistributed. Get it from the vendor.

## Installation

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements-core.txt
```

| File | Installs | For |
| --- | --- | --- |
| `requirements-core.txt` | psutil, zstandard | Command-line program with the SQLite catalog |
| `requirements-postgres.txt` | core + psycopg 3 | The PostgreSQL catalog |
| `requirements-gui.txt` | PySide6 | The inspector GUI (install on top of one of the above) |
| `requirements.txt` | all of the above | Everything |

Step-by-step instructions for both backends, and upgrade notes for an
existing PostgreSQL installation: [docs/installation.md](docs/installation.md).

## Quick start (SQLite, no tape drive needed)

```powershell
python run.py --version
Copy-Item config.example.ini config.ini
notepad config.ini
```

In `config.ini`, before the first start:

- set the three folders in `[PATHS]` to real folders, and delete the
  `backup_log_dir` line (the placeholder paths are used literally otherwise);
- set `[LOCAL_MANIFEST_ARCHIVE] root` to a real folder;
- leave `lto_drive` empty if no tape drive is attached: `lto_drive =`
- keep `backend = sqlite`.

Then:

```powershell
python run.py
```

The first start creates the catalog file `lto_archive.sqlite3` with its
complete schema and shows the main menu. Choose `4` to list tapes (none yet)
and `0` to exit. Back up the catalog:

```powershell
python run.py --backup-db
```

This writes a verified copy to `db_backups\`.

## SQLite or PostgreSQL

| | SQLite | PostgreSQL |
| --- | --- | --- |
| Setup | none | a server (`docker compose up -d db`) |
| Choose it when | new installation, one archive host | you already run it, or the catalog is very large |
| Name search | scans the table; case-insensitive for ASCII only | trigram indexes |
| Small-file packaging | ZIP | ZIP, or Stored TAR |
| Not available | the [PostgreSQL-only features](docs/database-backends.md#postgresql-only-features) | - |

A `config.ini` with no `backend` line keeps using PostgreSQL, so an upgrade
never switches an existing installation. There is no tool that moves a
catalog from one backend to the other. Details:
[docs/database-backends.md](docs/database-backends.md).

## Configuration

`config.ini` is checked strictly at start-up. A bad value stops the start with
a message naming the section, key and value. Inline comments are not allowed.

```ini
[PATHS]
source_dir = D:\staging\source
staging_dir = D:\staging\work
restore_dir = D:\staging\restored

[DATABASE]
backend = sqlite
sqlite_path = lto_archive.sqlite3

[HARDWARE]
lto_drive = T:\\
ltfs_ownership_id = drive_0000000000
eject_after_session = false
tape_generation = LTO-8

[SETTINGS]
zip_threshold_mb = 100
max_zip_size_gb = 100

[REMOTE]
remote_host = source-host.example
remote_user = archive
remote_path = /data/projects
```

Passwords go in `.env` (copy `.env.example`), never in `config.ini`:
`REMOTE_PASSWORD` for the source host, `PGPASSWORD` for PostgreSQL.

Every key, with type, default and meaning:
[docs/configuration.md](docs/configuration.md).

## Use

```powershell
python run.py                                  # interactive menu
python run.py --version
python run.py --backup-db                      # back up the catalog
python run.py remote-archive --resume          # continue the active remote session, no prompts
python inspect_db.py                           # inspector GUI (needs PySide6)
python inspect_db.py --session-forensics       # read-only report on active sessions
```

Main menu:

| Option | Action |
| --- | --- |
| 1 | Archive a local folder to tape |
| 2 | Retrieve: search the catalog and restore files |
| 3 | Tape maintenance: format, register, check, drive info, eject |
| 4 | List registered tapes |
| 5 | Open `config.ini` |
| 6 | Remote archive: fetch from a remote host and write to tape |
| 7 | Database management: edit or delete tape and file records |
| 8 | Make sure `backup_logs\SUMMARY.csv` exists |
| 9 | Back up the catalog |
| 0 | Exit |

The process exit code is non-zero when an archive run, a restore, a catalog
backup or the start-up itself failed
([docs/troubleshooting.md](docs/troubleshooting.md#exit-codes)).

### Archive flow

```text
source (local folder, or remote host over SSH)
  -> local staging
  -> pack and validate (planned = fetched = packed)
  -> finite write group: robocopy to the LTFS tape
  -> classify the copy result (the tape is never read back)
  -> commit to the catalog
```

1. The source is scanned and split into chunks of bounded size.
2. Each chunk is fetched to local staging, packed and reconciled.
3. Ready chunks are written to tape in a finite group, under one ownership of
   the drive. Before the group, the safety gates run.
4. Each chunk is marked `backing` before the first touch of the tape and
   `done` after its copy is classified clean. A chunk left in `backing` is
   never retried automatically.
5. Re-running the same option resumes the session.

### Restore flow

Menu option 2: search (name or wildcard, date range, directory, session), then
pick a file ID or `ALL`. The program checks the mounted cartridge, copies the
bundle or file from tape to staging, extracts into `restore_dir`, and prints
what was and was not restored. An incomplete restore sets a non-zero exit
code. Every restored file stays inside `restore_dir`; characters that are
illegal in Windows file names are replaced with `_`.

Full instructions: [docs/user-guide.md](docs/user-guide.md).

## Known limitations

- **No real-hardware validation** (see the warning at the top).
- **Windows only.** The POSIX code path of the SQLite archiver lock exists
  but has never been run.
- **PostgreSQL-only features** refuse on SQLite with one line:
  explicit schema migrations, legacy backfills, comparing databases, the
  schema audit, sealed write batches, manifest export/prune, Stored TAR
  packaging, `pg_dump` backups.
  [Full list](docs/database-backends.md#postgresql-only-features).
- **No SQLite to PostgreSQL data migration**, in either direction.
- **SQLite name search** is case-insensitive for ASCII letters only and has
  no trigram index.
- **An existing PostgreSQL catalog is not migrated automatically** beyond the
  additive start-up migrations. Only a completely empty database gets the full
  schema on first start.
- **Re-formatting a labelled cartridge is not available from the menu.** It
  is refused there in favour of the `tape-reset` subcommand of `run.py`, which
  requires an `--impact-report` file. That file is written by the read-only
  `python run.py tape-reset --tape <LABEL> --prepare-impact-report <FILE> --backup-file <BACKUP>`.
  The reset has not been run on hardware.
  See [the user guide](docs/user-guide.md#tape-maintenance-option-3).
- **A cartridge holding Stored TAR chunks (PostgreSQL only) cannot be
  reset**: such a chunk could not be written again afterwards, so the reset
  refuses before changing anything. ZIP chunks are reset normally.
- **A reset interrupted during the format itself needs a person at the
  drive.**
- **Renaming a tape that has loose-file restore routes is refused.**
- **One drive.** There is no drive pool.
- **No content hashes for fetched files.** A source file replaced by another
  of the same size between scan and fetch is not detected.
- **The release workflow has not yet been run on GitHub.** The check workflow
  (privacy gate, tests on Python 3.11 / 3.13 / 3.14, PostgreSQL suites, GUI,
  clean installs) has passed there.

## Documentation

| | |
| --- | --- |
| [docs/installation.md](docs/installation.md) | Install, both backends, upgrade notes |
| [docs/configuration.md](docs/configuration.md) | Every `config.ini` key |
| [docs/user-guide.md](docs/user-guide.md) | Menus, archive, restore, tape maintenance, backups, GUI, stopping a run |
| [docs/safety.md](docs/safety.md) | Tape safety rules. Read before the first tape write |
| [docs/troubleshooting.md](docs/troubleshooting.md) | Error messages, causes, fixes, exit codes |
| [docs/database-backends.md](docs/database-backends.md) | SQLite and PostgreSQL in detail |
| [docs/architecture.md](docs/architecture.md) | Modules, state machine, locks, safety gates |
| [docs/testing-and-validation.md](docs/testing-and-validation.md) | Running the tests; what is and is not verified |
| [CHANGELOG.md](CHANGELOG.md), [release notes](docs/releases/v1.0.0-rc.1.md) | What changed |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Development setup and rules for changes |
| [docs/history/](docs/history/README.md) | Dated records from the original deployment |

## Licence

MIT, copyright (c) 2026 Raz Ben Aharon. See [LICENSE](LICENSE).
Third-party components: [THIRD_PARTY_LICENSES.md](THIRD_PARTY_LICENSES.md).
