# Current system

Consolidated architecture reference for the OWC LTO-8 archiver. It describes
what the code does today; for live production state see
[tape-and-archive-state.md](tape-and-archive-state.md), and for how to operate
a run see [operations.md](operations.md).

## What the system is

A Windows-focused Python utility that archives lab-server data to LTO-8/LTFS
tape. It fetches data from remote hosts over SSH, packs many small files into
containers (ZIP bundles or stored-TAR), writes them to an LTFS-mounted tape via
`robocopy`, and indexes the result in a PostgreSQL catalog.

## The canonical archive flow — LOCAL-FIRST

```text
remote source (SSH/tar fetch)
  -> local staging on the internal NVMe
  -> validation (stored-TAR parse to trailer + sidecar, or ZIP pack)
  -> finite write group: robocopy to the LTFS tape
  -> verify via copy-tool classification (never read-back from tape)
  -> commit to catalog
```

There is **no code path that streams source data directly to tape**. The tape
writer's only input is validated local staging (`src/remote_writer.py`;
`ReadyQueue`). This ordering is what makes every failure recoverable without a
person at the drive: a fetch or validation failure costs local disk, never
tape state.

## Project structure & module organization

- `run.py` — root runner for the main CLI (chdir to the project root, then
  `src.cli.main()`).
- `inspect_db.py` — root runner for the GUI database inspector.
- `src/` — internal package, split into modules with strictly downward
  dependencies: `constants`/`pipeline_types` → `logsetup` → `runtime` →
  `paths` → `reporting`/`config`/`db` → `robocopy`/`remote_transport` →
  `ltfs` → `packer` → `scanning`/`planning` → `archive_artifacts` →
  `backup`/`retriever` → `scan_frontier`/`remote_staging`/`remote_writer` →
  `remote_pipeline` → `local_orchestrator`/`remote_orchestrator` →
  `orchestrators` (re-export facade) → `cli`; `src/db_inspector_qt.py` holds
  the GUI. The PostgreSQL layer is split the same way: `pg_bulk` → `pg_core` →
  `pg_catalog`/`pg_scan`/`pg_sessions`/`pg_tapes` → `pg_db` (facade assembling
  `PgDatabaseManager` from the mixins). Import the facades (`orchestrators`,
  `pg_db`) from application code; in tests, `mock.patch` targets must name the
  module a symbol is *used* in (e.g. `src.scanning._ssh_run`).
- `config.ini` — local paths, tape drive settings, remote archive settings,
  performance tuning. `.env` stores secrets; keep it untracked
  (`.env.example` is the template). Data files (`config.ini`, `.env`,
  `backup_logs/`) stay in the project root; `src/constants.py` anchors paths
  to `PROJECT_ROOT`.
- `backup_logs/` — the single `SUMMARY.csv` statistics file plus the rotating
  diagnostic trace `archiver.log`. No per-run log files, no per-file
  manifests; the CSV is file-name-free by construction.
- `scripts/sql/` — PostgreSQL schema/index/constraint migrations applied on
  startup by `PgDatabaseManager._init_schema`; `docker-compose.yml` runs the
  local database. Catalog rows are runtime data, not source.

## The remote pipeline, after Plan 1

`remote_orchestrator.py` used to be one 3,657-line class. It is now a
**façade** over four modules, each owning one invariant:

| module | owns | never does |
| --- | --- | --- |
| `scan_frontier.py` | the sole production scanner; discovery; sealing chunks | touch the tape |
| `remote_staging.py` | SSH/tar fetch, retry classification, the staging watchdog, packing | touch the tape |
| `remote_writer.py` | **the finite write group — the sole tape-writing entry path** | eject; retry into a failing drive; clear `backing` |
| `remote_pipeline.py` | the single scheduling loop for BOTH session kinds | decide anything about the tape |

`remote_writer.py` is the only place a remote pipeline copy enters the tape.
Cartridge selection, announcement, and pre-run checks also exist in
`remote_orchestrator.py`, so a review of all cartridge access must include
both modules.

Three rules the code enforces rather than documents:

- **No LTFS access outside a finite write group.** Not at startup, not while
  waiting, not between group members, not at completion. Both loops announce
  the target cartridge up front (verified once per group, under ownership).
- **The remote pipeline never ejects**, even with
  `[HARDWARE] eject_after_session = true` (that setting applies only to the
  attended local orchestrator).
- **`backing` has no automatic retry.** `CHUNK_TRANSITIONS` allows only
  `backing → done`; an unreadable chunk status *stops the run* instead of
  being treated as clear.

## Source-of-truth boundaries

Each store answers exactly one kind of question; none duplicates another:

| store | is authoritative for |
| --- | --- |
| JSONL.zst manifests under the `[LOCAL_MANIFEST_ARCHIVE]` root | Per-file inventory of packed small files (after a validated + pruned export) |
| PostgreSQL | Operational state + coarse aggregates: sessions, chunks, bundles, containers, folder aggregates, tape accounting |
| Receipts + sidecars | Campaign container evidence (SHA-256s, member counts, logical bytes, plan-manifest locators) |
| Tapes | Payload only — never metadata anyone must read back to interpret the archive |

## Artifact schemas and the publication protocol

Three versioned local artifact schemas carry the per-file detail that would
otherwise cost one PostgreSQL row per file:

- **`plan-manifest-v1`** — the sealed plan of a chunk: canonical path,
  expected size, stable plan ordinal, chunk/container identity, storage class
  (`container` or `loose`), container format, routing precision.
- **`tar-sidecar-v1`** — the validated parse of a stored-TAR container:
  `record_type='member'` rows for actual TAR members and
  `record_type='source_exception'` rows for absent planned ordinals; together
  they account for every plan ordinal exactly once.
- **`terminal-state-v1`** — the terminal manifest of a chunk: exactly one
  disposition per plan ordinal, written when the chunk reaches its final
  state.

All three publish through the same protocol: write to a uniquely-named
**`.part`** file → flush and **fsync** → **reparse** the completed file and
validate it against the sealed plan → **atomic publish** (`os.replace`, never
clobbering an existing final name). A crash leaves an orphan `.part`, never a
truncated artifact a reader would parse as complete; a ready catalog locator
is never allowed to name a `.part` (enforced in code, in `pg_scan`, and by a
schema CHECK). No content hashes exist in these artifacts — hashing would mean
reading every source byte over SSH, so the residual same-size-replacement risk
is documented rather than papered over.

## Storage Map & analytics (`storage_map/`)

A self-contained remote disk-usage mapper for the lab servers, **decoupled
from the tape pipeline** (it does not touch `src/cli.py`, the archive catalog,
or the LTFS drive). `storage_map/run_app.py` is its single entrypoint;
internals live in `storage_map/lib/` and `storage_map/webapp/`.

- **Stage 1 — `scan` (fire-and-forget):** connects to each configured server
  over SSH, launches a low-priority `du` per mount under `nohup`/`setsid`,
  then exits; no live connection is held during the hours-long scan.
- **Stage 1.5 — `status` / `fetch`:** checks the remote completion sentinel;
  SCPs the finished raw log to `storage_map/logs/`.
- **Stage 2 — `view` / `dashboard` / `treemap`:** parses a *local* raw log
  only (never the disks again) into a Rich terminal dashboard, HTML dashboard,
  or Plotly treemap.
- **Interactive web dashboard:** a FastAPI + uvicorn app bound to
  `127.0.0.1:8765`, adding a tape-coverage table (directory prefixes matched
  read-only against the PostgreSQL catalog, cached) and a **deletion ledger**
  — the permanent record of server directories deleted after their archive was
  verified on tape. The module only records deletions, never performs one;
  rules live in [server_deletions.md](server_deletions.md).

Mount points and servers are config-driven (`[STORAGE_MAP]` sections), never
hardcoded.

## Database migrations (scripts/sql/, 001–019)

| # | Adds |
| --- | --- |
| 001 | Base PostgreSQL schema (files index, directories, tapes, bundles) |
| 002 | Indexes for the base schema |
| 003 | Constraints for the base schema |
| 004 | `archive_runs` / session linkage |
| 005 | Unique session labels |
| 006 | Remote streaming sessions and chunks |
| 007 | Directory catalog tables |
| 008 | Remote provenance columns |
| 009 | Remote session foreign keys |
| 010 | `[LOCAL_MANIFEST_ARCHIVE]` small-file manifest export tracking |
| 011 | Durable tape status (`tapes.status='full'` retirement) |
| 012 | Sealed tape write batches (+ rollback script) |
| 013 | Tape reset safety guards |
| 014 | Incremental scan / persistent frontier (+ finalize and rollback scripts) |
| 015 | Container formats (ZIP vs stored-TAR axis) |
| 016 | Stored-TAR plans |
| 017 | Stored-TAR publication state (sidecar/TAR locator readiness) |
| 018 | Manifest-first directory catalog |
| 019 | Container-format schema authority v2 |

Migrations are applied automatically on startup; migration 014 is applied and
finalized on the authoritative catalog, and the persistent frontier scanner is
the sole production scanner (no legacy scan mode, no fallback — an unusable
migration-014 schema stops the run rather than downgrading).
