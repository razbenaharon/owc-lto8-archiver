# Repository Guidelines

## Project Structure & Module Organization

This repository is a Windows-focused Python utility for archiving data to LTO/LTFS tapes.

- `lto_archive_manager.py` is the main CLI application: local archive, remote archive, restore, tape maintenance, packing, and database updates.
- `db_inspector.py` is the GUI database inspector/editor.
- `config.ini` contains local paths, tape drive settings, remote archive settings, and performance tuning.
- `.env` stores secrets such as remote passwords; keep it untracked. Use `.env.example` as the template.
- `backup_logs/` contains generated robocopy/session logs.
- `Framework & Drivers/` stores installer assets and hardware documentation.
- `lto_archive.db` is the local SQLite archive index and should be treated as runtime data, not source.

## Build, Test, and Development Commands

Create or update the Python environment:

```powershell
python -m pip install -r requirements.txt
```

Run syntax checks before handing off changes:

```powershell
python -m py_compile lto_archive_manager.py db_inspector.py
```

Run the main application:

```powershell
python lto_archive_manager.py
```

Run the database inspector:

```powershell
python db_inspector.py
```

## Coding Style & Naming Conventions

Use Python 3, four-space indentation, and descriptive `snake_case` names for functions, variables, and methods. Keep existing module style: procedural helpers near the top, larger workflow classes below. Prefer small helper functions for hardware, LTFS, robocopy, database, and path-safety behavior. Avoid broad refactors during operational fixes.

## Testing Guidelines

There is no formal test suite yet. At minimum, run `python -m py_compile ...` after edits. For tape-related changes, reason carefully about hardware side effects and verify with a small staged dataset before full remote archive runs. Do not require real tape hardware for pure parsing, config, database, or path-normalization fixes.

## Commit & Pull Request Guidelines

Recent history uses concise imperative messages, sometimes with prefixes such as `fix:`, `feat:`, and `refactor:`. Examples: `fix: handle Windows-illegal chars in remote fetch paths + collision guard`, `feat: continuous-streaming remote->tape pipeline with status + graceful stop`.

PRs should include: purpose, risk level, commands run, hardware/manual verification if relevant, and notes about database/config changes. For UI changes in `db_inspector.py`, include a screenshot or short behavior description.

## Security & Operations Notes

Never commit `.env`, real credentials, generated logs with sensitive paths, or large runtime databases. During archive writes, avoid browsing the LTFS drive or starting separate copy jobs. Internal script tape access is serialized, but external processes can still interfere with tape throughput.
