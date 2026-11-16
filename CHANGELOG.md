# Changelog

All notable changes to this project are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Versions follow
[PEP 440](https://peps.python.org/pep-0440/) in the program (`1.0.0rc1`) and
SemVer spelling in tags (`v1.0.0-rc.1`).

## 1.0.0-rc.1 - unreleased

Pre-release. First version prepared for public use. Release notes with
installation and upgrade instructions:
[docs/releases/v1.0.0-rc.1.md](docs/releases/v1.0.0-rc.1.md).

**Not validated on a real tape drive and cartridge.** All tape-path
behaviour in this version was verified with simulated devices and temporary
directories only.

### Added

- **SQLite catalog backend.** `[DATABASE] backend = sqlite` keeps the catalog
  in one local file, created with its complete schema on first run. No
  server and no database driver to install. It is the default for new
  configurations; a `config.ini` with no `backend` line keeps PostgreSQL.
- Single-writer archiver lock for SQLite: an operating-system file lock on
  `<database>.archiver.lock`, released by the system when the process ends.
- SQLite catalog backup with integrity and foreign-key checks
  (`python run.py --backup-db`, menu option 9).
- `python run.py tape-reset --tape <LABEL> --prepare-impact-report <FILE>
  --backup-file <BACKUP>`: the read-only step that writes the impact report
  a tape reset requires. Nothing produced that file before.
- A new, empty PostgreSQL database gets the complete schema on first start.
- `--version` for `run.py` and `inspect_db.py`; one version source
  (`src/__init__.py`).
- Split dependency sets: `requirements-core.txt`, `requirements-postgres.txt`,
  `requirements-gui.txt`. `requirements.txt` still installs everything.
- Database guard for chunk state transitions (PostgreSQL migration 020,
  applied at start-up; same-named trigger on SQLite), with named, audited
  overrides (`tape_reset`, `operator_reconciled`).
- `scripts/install_smoke_test.py`: clean-install test for both backends.
- `scripts/build_release.py`: release package and checksums.
- `tests/mutation_check.py`: opt-in mutation check for the safety gates.
- Tests proving both tape writers obey their pre-write gates; every
  behavioural catalog suite runs on SQLite and on PostgreSQL.
- Documentation set: installation, configuration reference, user guide,
  safety rules, troubleshooting, database backends, contributing guide, and
  a test that keeps documented commands, links and the configuration
  reference in step with the code.
- CI workflows: Python 3.11, 3.13 and 3.14, PostgreSQL suites, clean-install
  smoke tests, headless GUI tests, privacy gate, draft release. Not yet
  executed on GitHub.

### Changed

- **Strict configuration parsing.** `config.ini` is validated at start-up; a
  bad value stops the start with a message naming section, key and value.
  Inline comments after a value are an error. Yes/no values accept only
  `true false yes no on off 1 0`. Unknown keys are reported as warnings.
- **Exit codes are non-zero on failure** in `run.py`: a failed backup, a
  headless run that could not reach the database, a failed archive action
  from the menu, an incomplete restore.
- **The local archive path no longer ejects the cartridge after each
  chunk.** It ejects only at the end of a completed session, only with
  `[HARDWARE] eject_after_session = true`, and only after `EJECT` is typed.
  It now runs the same pre-write gates as the remote pipeline and refuses to
  write when the cartridge label cannot be read.
- **Eject and filesystem repair require a typed confirmation** (`EJECT`,
  `REPAIR`) on an interactive console.
- **Ctrl+C no longer kills a tape copy in flight.** The copy finishes and
  the run stops at the next chunk boundary. `scripts/graceful_stop.py`
  refuses to signal while a tape write is active.
- A chunk is marked `backing` before the first touch of the tape, and its
  staged pack is kept on every writer error.
- Chunk state transitions are enforced in the status setter as well as in
  the database: `backing` may only become `done`; `done` is final.
- The archiver lock is verified before every write group and is never
  re-acquired silently.
- Planned, fetched and packed files are reconciled before a chunk may reach
  the writer.
- Drive and medium health are re-checked from the IBM LTFS log before each
  chunk of a write group.
- The scanner fills chunks across directories instead of sealing one chunk
  per directory.
- Restore prints what was really restored and returns a failure status when
  it was incomplete.
- Restore keeps every destination inside the restore directory. Characters
  that are illegal in Windows file names are replaced with `_`, so a
  restored name can differ from the catalogued one.
- One tape-usage percentage in the CLI and the GUI, measured against the
  planning budget.
- The inspector GUI refuses to delete an active session; destructive actions
  need a typed confirmation.
- The watchdog script decides on the stop reason and the exit code, refuses
  after more kinds of hard stop (including a lost archiver lock), and fails
  closed on an unknown code.
- The pending-restart check fails closed without blocking hosts that have no
  SCCM client.
- The `robocopy` result classifier is stricter: it rejects a failed
  directory, unaccounted bytes, a killed process and a grossly short byte
  summary.
- Catalog backups work with no tape drive configured (`lto_drive` empty).
- Catalog-backup verification reads the whole dump and states what it did
  not prove.
- The PostgreSQL test guard is fail-closed: it refuses port 5432, a
  non-loopback host, redirecting connection parameters, and a server hosting
  a production catalog. Tests no longer read `config.ini` in the project
  folder.
- `tests/test_pg_integration.py` was renamed `tests/test_db_integration.py`.
- Site-specific operational records moved to `docs/history/`.

### Fixed

- Remote files are requested by their exact name; trailing spaces are no
  longer stripped.
- Extracted names are matched across Unicode normalisation forms (NFC/NFD);
  a file is recorded as missing only when the source host reports it
  missing.
- Stored TAR carries sparse files with trailing holes and hard-linked names;
  loose large files are carried through a mixed Stored TAR chunk.
- Legacy ZIP members are extracted atomically; a bad member no longer stops
  the rest; restore never picks one of several same-named members.
- A directory-catalog error during a directory restore is a failed restore.
- Per-file manifests are never overwritten and are protected from lost
  writes and trailing data.
- The fetch watchdog no longer cancels healthy fetches; the remote scan is
  bounded.
- The local writer refuses a tape marked full, or over its budget, also for
  a chunk already assigned to it.
- Tape reset, rename and delete work against the current container schema.
- The campaign-store verifier fails when it verified nothing; a receipted
  container must be a regular file; `--resume` trusts an earlier SHA-256
  only on recorded evidence.
- Inspector background workers are kept alive until their reply is
  delivered; paging is safe; a database that is down gives a readable error.
- Two catalog statements that failed on both backends.
- A new PostgreSQL database could not take a packed archive run.
- Binary sizes in the backup summary are labelled GiB and MiB/s.
- Documentation: commands that the program rejected
  (`--session-forensics --session`, `--reboot-sentinel-status`), the claim
  that every migration is applied at start-up, and differing test counts.

### Removed

- The `DatabaseManager` placeholder class.
- Accessors of seven configuration keys that nothing read. The keys are
  still accepted, with a notice that they have no effect.
- `scripts/await_drive_and_evacuate.ps1`.

### Security

- Restore destinations are contained inside the restore directory
  (path-traversal hardening for names coming from the catalog or from
  archive members).
- The privacy gate (`scripts/check_public_repo.py`) covers naming schemes
  rather than single literals, and runs in CI over the full history.
- Tests can reach neither a real tape drive nor a real catalog.
