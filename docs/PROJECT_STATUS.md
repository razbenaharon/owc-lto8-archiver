# Project status

**The one-page answer to "where is this project?"** Updated 2026-09-23 on the
operator workstation. Counts and tape state are not restated here; they live
in [`tape-and-archive-state.md`](tape-and-archive-state.md). This file records
what is **done**, what is **open**, and what is **blocked**, so nobody has to
reconstruct it from 180 commits and 15 incidents.

Every line carries one of three evidence labels:

- **Verified**: checked against live evidence on the date given.
- **Documented**: recorded in an incident or state snapshot, not re-checked.
- **Unknown**: nobody has checked. Treat it as unknown, never as done.

## Bottom line

1. **The backup that exists is safe.** Tape_01 and Tape_02 are closed
   production tapes. Their pre-July content (13.8 TB, about 4.27M files) has a local per-file
   inventory that passed reconciliation. *Verified 2026-09-18.*
2. **All tape work is frozen.** The LTO-8 drive failed (servo fault) and is
   under RMA. No write is allowed until a replacement passes a synthetic pilot
   on a scratch cartridge. *Documented, incidents 013/014.*
3. **Session 37 is abandoned.** Operator decision, 2026-09-23. Chunks 0–48
   stay on Tape_02 as written, without a local per-file inventory. Nothing
   more will be fetched, and no further Session 37 metadata is kept. The local
   catalog already holds zero Session 37 rows. *Verified 2026-09-23.*
4. **The code is finished for the current scope and green in CI.** *Verified
   2026-09-04 (PR #2).*

## Achieved — milestones

| # | When | Milestone | Evidence |
| --- | --- | --- | --- |
| M1 | 2026-02 → 05 | Local archive tool: robocopy staging, hash verification on retrieval, remote tar-stream fetch | `2304b56`, `92f80b0` |
| M2 | 2026-06 | Continuous remote→tape streaming pipeline; monolith split into the `src/` package; serialized tape access | `aefc18c`, `f3cfd9f`, `3b39370` |
| M3 | 2026-07-02 | PostgreSQL catalog backend (Docker `lto_pg`) with backup command | `74006b1`, `8f714da` |
| M4 | 2026-07 | Production hardening from live runs: RAM governor, pack parallelism, deadlock fix, SCCM/reboot guards, robocopy evidence classifier, cartridge verification | incidents 001–011 |
| M5 | 2026-07 | **Tape_01 closed** (10.62 TB) and **Tape_02 closed read-only** (3.20 TB + Session 37 chunks 0–48) | [state](tape-and-archive-state.md) |
| M6 | 2026-08-03 | Plan 1: manifest-first frontier scanner as the only production scanner | `Close Plan 1…` commits |
| M7 | 2026-08-04 | Plan 2: Stored-TAR container format (writer, strict reader, restore routing, receipts) | `Plan 2 Task …` commits |
| M8 | 2026-08-05 → 06 | Plan 3: directory catalog, Session 37 boundary, guarded transitions, migration 019 | `Plan 3 …` commits |
| M9 | 2026-08-10 → 12 | Session 37 chunks 49–216 localized as verified TAR containers | incident 014 |
| M10 | 2026-08-20 | Plan 4: per-file inventory exported to JSONL.zst manifests; PostgreSQL pruned of per-small-file rows | [plan 4](archive-modernization-plans/04_LEGACY_EXPORT_AND_POSTGRESQL_PRUNING.md) |
| M11 | 2026-08-20 → 21 | Repository made public: history rewritten, infrastructure identities replaced, privacy gate in CI | `refactor: replace real infrastructure identities…` |
| M12 | 2026-08-21 | Campaign-store verifier and failing-drive evacuation tooling | `feat: verify campaign containers…` |
| M13 | 2026-09-04 | Clean-clone green: 1,752 tests offline, CI on Python 3.11 and 3.13 | PR #1, PR #2 |
| M14 | 2026-09-18 | Manifests moved off the Desktop into `LTO_METADATA/`; reconciliation `--heavy` PASS: disk = ledger = aggregates, 4,240,566 rows | [state](tape-and-archive-state.md) |

## Open — ordered by what unblocks what

| ID | Item | Blocked by | Label |
| --- | --- | --- | --- |
| O1 | Replacement LTO-8 drive, then a synthetic pilot on a scratch cartridge | RMA (external) | Documented |
| O4 | Tape_03 / Tape_04: reformat as scratch. Their partial Session 37 copies are no longer wanted | O1 | Documented |
| O5 | **Second copy of `LTO_METADATA/` and `db_backups/`**. Both exist only in this repo folder on one drive, and a `git clean -x` would delete them | Operator decision on the target | Verified 2026-09-23 |
| O6 | Fresh `pg_dump`. The newest dump is dated 2026-08-21, and the catalog is 139 MB live | — | Verified 2026-09-23 |
| O7 | SCCM maintenance window or deployment exemption for the archive host | IT | Documented, incident 005 |
| O8 | Revert the incident-008 `config.ini` overrides; Session 37 is abandoned, so nothing needs them | — | Documented |
| O9 | Off-host monitoring. A restart watchdog exists (`scripts/archive_watchdog.ps1`), but the only monitor still runs on the host doing the work | — | Documented, incident 007 |
| O10 | Two `[STORAGE_MAP:*]` host sections in the local `config.ini` still hold TODO placeholders | Real host details | Verified 2026-09-23 |

## Decisions

- 2026-09-23: Session 37 abandoned. Closes the former items "re-fetch
  chunks 49–216" and "copy the production host's metadata".

Deliberately **not** planned: physical PostgreSQL compaction (plan 4 task 6.2)
stays separately approved future work.

## What is not in git (and why)

The repository is public. The following stay local on purpose (see
`.gitignore`) and are not reproducible from git:

| Path | What it is | Can it be regenerated? |
| --- | --- | --- |
| `LTO_METADATA/` | Per-file manifests for Tape_01/Tape_02 (145 segments, 241 MiB) | **No.** This is the only copy (see O5) |
| `db_backups/` | `pg_dump` files, 2026-07-02 → 2026-08-21 | **No** |
| PostgreSQL volume (Docker `lto_pg`) | The live catalog | Only from `db_backups/` |
| `config.ini`, `.env` | Host configuration and secrets | By hand, from `config.example.ini` / `.env.example` |
| `private/` | Pre-rewrite git bundle, archived design docs, retired scripts, vendor support logs | No (the bundle holds the unscrubbed history) |
| `backup_logs/` | `SUMMARY.csv` performance ledger | No |
| `venv/`, `__pycache__/`, `.pytest_cache/` | Build and test caches | Yes |

## Keeping this file honest

- Close an item only with evidence (commit, report, or dated check), and move
  it to *Achieved* with that evidence.
- When a new incident opens, add an O-row that links to it.
- Update the date at the top on every edit. A stale "Verified" label is worse
  than an honest "Unknown".
