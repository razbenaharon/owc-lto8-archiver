# Testing and validation

How to run the tests, and what they do and do not prove.

## What is verified, and what is not

| Area | How it is checked | Status |
| --- | --- | --- |
| Catalog logic, both backends | Unit and integration tests; every behavioural catalog suite runs on SQLite, and on PostgreSQL when a disposable server is configured | Tested |
| Planning, packing, reconciliation, manifests, restore logic | Tests with temporary directories | Tested |
| Safety gates and chunk state machine | Tests that prove both tape writers obey their gates; mutation check | Tested |
| Clean install, first run, archive to catalog to restore round trip | `scripts/install_smoke_test.py`, both backends. A temporary directory stands in for the cartridge and an in-process copier stands in for `robocopy.exe` | Tested, simulated tape |
| Inspector GUI | Headless GUI tests | Tested |
| Documented commands, links and the configuration reference | `tests/test_docs_commands.py` | Tested |
| **Writing to a real cartridge with `robocopy`** | - | **Not verified** |
| **LTFS readiness, sync-mode detection, drive and medium health from the IBM log, on a real drive** | Parsers are tested on recorded and synthetic text | **Not verified on hardware** |
| **Cartridge label check, eject, format, filesystem check** | Simulated devices | **Not verified on hardware** |
| **Restore from a real cartridge** | Simulated | **Not verified** |
| **Tape reset** | Simulated | **Not verified** |
| POSIX path of the SQLite archiver lock | - | Not run |
| CI workflows on GitHub | Written, not yet executed | Not run |

No working tape drive was available while this version was prepared. Treat
the first runs on your hardware as a pilot (see [Hardware pilot](#hardware-pilot)).

## Setup

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m pip install pytest
```

`requirements-core.txt` is enough for the SQLite suites; the PostgreSQL and
GUI suites then skip.

## Offline suite

```powershell
python -m pytest tests/ -q
```

Needs no tape drive, no network and no database server. The catalog suites
run on SQLite in temporary files. The PostgreSQL suites **skip**. As of
2026-11-16: about 2,600 passed and about 440 skipped.

The suite cannot reach a real tape: `tests/conftest.py` blocks every
filesystem and subprocess call aimed at the configured `lto_drive`, at `Z:`,
or at an IBM LTFS tool, before any I/O happens. The suite must not leave a
`config.ini` behind in the project folder.

Syntax check of everything that ships:

```powershell
python -m compileall -q src scripts run.py inspect_db.py
```

## PostgreSQL suites: disposable server only

The PostgreSQL suites create and drop databases. They run only against a
server named in `LTO_TEST_PG_DSN`, and `tests/pg_test_guard.py` refuses,
before any test runs, a DSN that:

- uses port 5432 (where a real catalog normally listens);
- names a host that is not loopback, or carries any parameter that could
  redirect the connection;
- reaches a server that hosts a database called `lto_archive`.

An unsafe DSN fails at collection. It never degrades to a skip. Do not weaken
this guard.

```powershell
docker run -d --name lto_pg_test -e POSTGRES_DB=postgres -e POSTGRES_USER=lto `
  -e POSTGRES_PASSWORD=<password> -p 127.0.0.1:15432:5432 `
  --tmpfs /var/lib/postgresql/data:rw,size=2g --shm-size=1g postgres:17

$env:LTO_TEST_PG_DSN = "postgresql://lto:<password>@127.0.0.1:15432/postgres"
$env:LTO_PG_SEALED_BATCH_IT = "1"
python -m pytest tests/ -q

docker rm -f lto_pg_test
```

The container keeps its data in memory and disappears with `docker rm`. Every
test database is named `ltotest_<run-id>_...`; cleanup drops only names that
carry the current run's marker. As of 2026-11-16 this run is about 2,990
passed and about 55 skipped (the skips need things this recipe does not
provide).

The archiver-lock probe on PostgreSQL counts advisory locks for the whole
server, not per database. A test against a throwaway database must patch it,
or an archiver running on another database of that server will block it.

## One test file, one backend

```powershell
python -m pytest tests/test_docs_commands.py -q
python -m pytest tests/test_config_strict.py tests/test_sqlite_schema_parity.py -q
```

`tests/db_backends.py` is the harness that runs one suite per backend; a new
catalog test should use it rather than open a database itself.

## Clean-install smoke test

Proves that a fresh installation of one backend works, from the committed
tree, in a new virtual environment, inside one temporary directory:

```powershell
python scripts/install_smoke_test.py --backend sqlite
python scripts/install_smoke_test.py --backend postgresql --pg-dsn <DSN>
```

It exports `HEAD` with `git archive` (uncommitted work is not tested),
installs exactly one dependency set, starts the program for the first time,
checks that the catalog and its schema were created, runs an archive to
catalog to restore round trip on synthetic files and compares every restored
file byte for byte, then takes and verifies a catalog backup. The PostgreSQL
variant creates its own database on the given disposable server and drops
it; the rules of `pg_test_guard` apply. `--keep` leaves the temporary
directory for inspection. No tape is involved.

## Mutation check

Shows that the tests really fail when a safety rule is broken. It copies
`src/`, `scripts/` and `tests/` to a temporary directory, injects one
deliberate fault at a time (for example: skip a pre-write gate), runs the
relevant tests, and reports `KILLED`, `SURVIVED` or `ERROR` per fault. The
real tree is never modified.

```powershell
python tests/mutation_check.py --list
python tests/mutation_check.py -k gate
python tests/mutation_check.py
```

It is not collected by pytest. A full run starts one pytest process per
mutation and takes several minutes. It exits non-zero unless every mutation
was killed. Run it after changing a safety gate or its tests.

## Privacy gate and release build

```powershell
python scripts/check_public_repo.py
python scripts/build_release.py
python scripts/build_release.py --expect-tag v1.0.0-rc.1
```

`check_public_repo.py` fails if anything tracked by git looks like private
or operational data (a database, a dump, a log, a real manifest, a secret, a
real host name). Run it before every push. `build_release.py` builds
`dist\tape-ledger-<version>.zip` and `SHA256SUMS.txt` from `HEAD`; it
refuses a dirty working tree, a failing privacy gate, or a package that
lacks a required file. `dist\` is not committed.

## Hardware pilot

Do this before trusting the program with real data. Use a **scratch
cartridge** and synthetic or unimportant files, and stay at the drive.

1. Read [safety.md](safety.md).
2. Mount the scratch cartridge with `sync_type=time@5`. Set `lto_drive` and
   `ltfs_ownership_id`. Start from an elevated console.
3. Prepare a small source: a few gigabytes, with a mix of many small files
   and a few large ones, odd file names included.
4. Set `[PIPELINE] max_write_groups_per_run = 1` and a small
   `[PERFORMANCE] chunk_cap_gb`, so the first run writes one bounded group.
5. Run the archive. Watch for: the gates passing, the chunk going
   `backing` then `done`, a clean classification in
   `backup_logs\tape_write\`, a row in `SUMMARY.csv`.
6. Run `python inspect_db.py --session-forensics` and
   `python run.py --backup-db`.
7. Restore a sample (one file, one directory, one whole session) into an
   empty `restore_dir` and compare it with the source.
8. Stop a run with Ctrl+C during fetch and during a tape write, and resume
   each time.
9. Only then raise the limits and archive real data. Keep the first real
   cartridges' source data until you have restored from them.

If anything unexpected happens at the tape: stop, keep the logs and the
staged data, and do not retry.

## After a code change

```text
full offline tests -> PostgreSQL tests on a disposable server
-> install smoke test -> mutation check (if a gate or its tests changed)
-> small pilot on a scratch cartridge -> one bounded write group -> review
```

Do not combine a refactoring, a schema change and a return to real data in
one step.
