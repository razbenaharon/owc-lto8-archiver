# Contributing

This project is maintained with little spare time. Small, focused changes
with tests are welcome; large rewrites are unlikely to be reviewed.

Please note before you start: **this version has not been validated on a
real tape drive.** A change to the tape path cannot be proven by the test
suite alone. Say in the pull request what you ran on hardware, if anything.

## Setup

Windows, Python 3.11 to 3.14.

```powershell
git clone <your fork>
cd owc-lto8-archiver
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m pip install pytest
```

Run the program from a scratch copy or with your own untracked `config.ini`.
`config.ini`, `.env`, catalogs, logs and backups are ignored by git and must
stay that way.

## Before you open a pull request

```powershell
python -m pytest tests/ -q
python -m compileall -q src scripts run.py inspect_db.py
python scripts/check_public_repo.py
```

All three must pass. Also, depending on the change:

| You changed | Also run |
| --- | --- |
| Catalog code or SQL | The PostgreSQL suites on a disposable server |
| A safety gate, the chunk state machine, the tape writers, or their tests | `python tests/mutation_check.py` |
| Installation, requirements, first-run behaviour | `python scripts/install_smoke_test.py --backend sqlite` |
| A command, a flag, or `CONFIG_SCHEMA` | Update the docs; `tests/test_docs_commands.py` fails otherwise |

How to run each: [docs/testing-and-validation.md](docs/testing-and-validation.md).

## Safety rules a change must respect

The full list is [docs/safety.md](docs/safety.md). A pull request is not
merged if it:

- reads from the tape to verify a write;
- touches LTFS outside the finite write group, or adds an automatic eject,
  remount, format or filesystem check;
- retries automatically after a hard tape error, or adds an automatic way
  out of the `backing` state;
- makes a pre-write gate, the archiver lock, the LTFS ownership mutex or the
  chunk-transition guard weaker, or makes it pass when it cannot be
  evaluated;
- infers success from a `robocopy` return code;
- weakens `tests/conftest.py` (no test may reach a real tape) or
  `tests/pg_test_guard.py` (no test may reach a real catalog).

Ask of every change: *if this fails at the worst possible moment, can it be
recovered without somebody standing at the drive?*

## Code conventions

- Keep the module layering in [docs/architecture.md](docs/architecture.md);
  dependencies point downward.
- **Two backends, one code base.** The shared catalog classes must not test
  which backend they run on. Put an engine difference behind a hook in
  `src/pg_core.py` and override it in `src/sqlite_core.py`. Write SQL in
  PostgreSQL syntax that also runs on SQLite
  (`tests/test_sqlite_prepare_all.py` checks every literal statement). A new
  catalog test goes through `tests/db_backends.py` so it runs on both.
- **Never edit an applied migration**, not even a comment in it.
  `scripts/sql/sqlite/001_schema.sql` is checksum-pinned: an installed
  SQLite catalog refuses to open if the file changes. Several PostgreSQL
  migrations record their checksum too. Add a new numbered file instead.
- A new `config.ini` key needs: a line in `CONFIG_SCHEMA`, a line in
  `config.example.ini`, and a row in `docs/configuration.md`. Tests keep the
  three in step.
- An expected refusal is an `OperationalError` with one readable line. Do
  not catch broad exceptions to hide a bug.
- A failure must end in a non-zero exit code (`src/exit_codes.py`). A new
  stop reason must be classified there as restartable or not.
- Prefer small helpers. Avoid broad refactoring in the same change as a fix.
- In tests, `mock.patch` targets name the module a symbol is used in.

## Commits and pull requests

Short imperative subject, optionally prefixed: `fix:`, `feat:`, `test:`,
`docs:`, `refactor:`, `chore:`.

```text
fix: refuse a tape marked full for an already-assigned chunk
```

In the pull request, state: what and why, the risk to tape state, the
commands you ran, any hardware verification, and any database or
configuration change. For a GUI change add a screenshot or a short
description of the behaviour. Add a line to `CHANGELOG.md` under the
unreleased entry for anything a user can notice.

## Privacy

The repository is public. Never commit:

- `config.ini`, `.env`, credentials or tokens;
- catalogs, dumps, logs, real manifests or receipts (they list real file
  names and paths);
- real host names, user names, lab or site names, or internal paths, in
  code, tests, comments or documentation.

Use placeholders: host `source-host.example`, user `archive`, paths such as
`D:\staging`. Everything under `examples/` must be synthetic.
`python scripts/check_public_repo.py` enforces this for tracked files, and CI
runs it over the full history. Remove such details from logs you attach to
an issue, too.

## Reporting a problem

Open an issue with: the version (`python run.py --version`), the backend,
the command or menu path, the message, and the exit code. For anything that
happened during a tape write, describe the state of the drive and cartridge
and **do not retry before you understand it**.

## Licence

By contributing you agree that your contribution is licensed under the
[MIT License](LICENSE).
