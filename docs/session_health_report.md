# All-session health report — 2026-08-03

Produced read-only by `python inspect_db.py --all-session-health` against
`lto_archive_directory_catalog_20260710_103359`. It opens no LTFS path, reads no
tape, starts nothing and writes no row. Every fact below is derived from
PostgreSQL at run time; no session id or status is hard-coded in the tool.

Regenerate it before acting on anything here — it is a measurement, not a
memory.

## Summary

| Session | Verdict | Status | Scan | Chunks | Tape (gen) | Action |
|---|---|---|---|---|---|---|
| 34 | **terminal** | completed | complete | 6 done | Tape_02 (1, active) | none |
| 35 | **terminal** | abandoned | never ran | none | Tape_02 (1, active) | none |
| 36 | **blocked** | active | incomplete | 1 done · 1 fetch_failed · 9 pending | Tape_02 (1, active) | **defer** — see below |
| 37 | **blocked** | active | incomplete | 49 done (**on Tape_02**) · 64 pending | targets Tape_03 (**gen 1, reformatted**) | conservative bootstrap **done**; next-cartridge decision outstanding |

Classification vocabulary, and the distinction that drives it:

* **terminal** — finished, and self-consistent. Nothing to do.
* **partial** — unfinished but consistent. *Unfinished is not unsafe*: it is the
  normal input to a conservative frontier bootstrap.
* **blocked** — needs an operator decision before any run.
* **ambiguous** — a physical or worker outcome the catalog cannot describe
  (`backing` chunks, live workers, held locks). Nothing automated may act.
* **orphaned** / **inconsistent** — references something gone, or facts that
  contradict each other.

## Session 34 — terminal, healthy

`completed`, scan complete, 6 chunks all `done`, on Tape_02 generation 1 which is
still active. Counts agree with the session row. No action.

## Session 35 — terminal, abandoned

`abandoned` with **zero** chunks and a recorded scan error: its scan died before
planning anything. There is nothing to preserve and nothing to migrate. No
action.

## Session 36 — partial and SUPERSEDED. No Plan 1 intervention.

**Evidence, measured against PostgreSQL:**

| Fact | Value |
|---|---|
| Planned paths (plan 36) | 5,720,920 |
| ...also planned in plan 37 | 2,414,473 (42%) |
| Chunk 0 files | 3,306,447 — status `done` |
| Chunk 0 paths also in plan 37 | **0** |
| Chunks 1–10 files | 2,414,473 — 1 `fetch_failed`, 9 `pending` |
| Chunks 1–10 paths also in plan 37 | **all of them** |

So the split is exact and not a coincidence:

* **Chunk 0 is unique to session 36.** 3.3M files, already written, on Tape_02
  generation 1 — which is still the active generation. That work is real, is the
  only copy, and must be preserved.
* **Chunks 1–10 are entirely superseded by session 37.** Every path in the one
  `fetch_failed` chunk and the nine `pending` chunks is also planned in plan 37.

**Verdict: partial, superseded, consistent.** It is *not* terminal (its scan
never finished and it still holds pending chunks), and it is *not* ambiguous —
no `backing` chunk, no owner token, no lease, no worker attempt, no frontier
state, no shared plan.

**Intervention: NONE, deliberately.** Session 36 gets no conservative bootstrap
and no metadata correction, because:

* it has no outstanding work the new scanner needs to reach — session 37 covers
  all of it, so bootstrapping 36 would queue a second exploration of the same
  source;
* the `fetch_failed` chunk must **not** be reset. It is superseded, and clearing
  it would invite re-fetching bytes plan 37 already plans;
* marking it `completed` would be inventing completion: its scan genuinely never
  finished, and no catalog row can establish otherwise;
* its `active` status is stale but harmless — it targets Tape_02 while 37 targets
  Tape_03. Retiring a superseded session's lifecycle is a Plan 3/Plan 4
  decision, not a Plan 1 consistency defect.

The one thing to know before a future run: session 36 is still `active`, so
session-selection logic that matches on host/path could see it. It has never
collided in practice because its selection differs from 37's, but a future
Plan 3/4 task should retire it explicitly rather than leave two active sessions.

## Session 37 — blocked: it targets a cartridge that was reformatted

### CORRECTION (2026-08-03, evidence-based — read this before the rest)

An earlier revision of this report claimed session 37's 49 `done` chunks existed
"in the catalog only" because the session is bound to Tape_03 generation 1,
which was retired as *destroyed*. **That was wrong**, and the inference was made
from the session header rather than from where the work actually landed.

Measured:

| Check | Result |
|---|---|
| `archive_runs` where `remote_session_id = 37` | **9 runs, every one `Tape_02`** (2026-07-10 → 2026-07-24) |
| Any archive run ever on Tape_03 | **none** |
| `files_index` rows from session-37 runs | **2,036 stored objects, 710 GB, all on Tape_02** (2,018 ZIP containers, the rest loose) |
| `files_index` rows on Tape_03 | **0** |
| `tapes.used_space` for Tape_03 | **0** |

So session 37's completed work is on **Tape_02, generation 1, which is still
active**. It was never reset and nothing of session 37's was destroyed.
**Nothing was ever written to Tape_03.**

### What the generation mismatch actually means

```
Tape_03 generation 1  formatted 2026-06-28  RETIRED 2026-08-02 09:56
Tape_03 generation 2  formatted 2026-08-02  RETIRED 2026-08-02 12:07
Tape_03 generation 3  formatted 2026-08-02  ACTIVE
retired_reason: "physical contents intentionally destroyed by tape reset"
```

`remote_sessions.tape_label = Tape_03` is the session's **next write target**,
not where its finished work lives. It was re-pointed at Tape_03 when Tape_02
filled, but no write to Tape_03 ever completed.

The mismatch (session holds generation 1, active is 3) is real and still blocks
`--resume` via `_verify_session_tape_generation` — correctly, and for the
forward-looking reason: the session is planned to continue writing onto a
cartridge that has been reformatted twice since. It is **not** evidence of loss.

**What an operator still has to decide:** whether the remaining 64 pending
chunks should target the current Tape_03 generation 3, a different cartridge, or
be re-planned. That is a capacity and lifecycle decision, out of Plan 1 scope.
The 49 `done` chunks need no decision — they are on Tape_02 and restorable.

**Intervention: the conservative frontier bootstrap, and nothing else.** It
creates scope rows and queues each configured root as `pending`. It changes no
chunk, no ordinal, no status, no membership, no ZIP metadata and no tape
locator, and it does not mark the scan complete.

**Executed against production on 2026-08-03**, after a shadow rehearsal on a
restored copy proved zero invariant violations. It wrote 65 scope rows (all
`provisional`), 65 directory rows (**all `pending`**) and one bootstrap record
(`running`, `coverage_final=false`) — and nothing else. Session 37's 113
chunks, 23,214,474 plan members, ZIP metadata and tape locators are byte-for-
byte what they were. Evidence: §14.7 of
[`plan1_completion_gate.md`](plan1_completion_gate.md).

## What the report itself found wrong

Running it exposed a defect in the shared quiescence probe:
`active_archive_processes()` matched **any** process whose command line
contained `remote_orchestrator` or `run.py`, so a code-review tool invoked with a
prompt naming those files was reported as a running archiver — turning every
session's verdict into `ambiguous`. The probe now additionally requires the
process to be a Python interpreter, which keeps every true positive (the
archiver is always Python) and drops the impostors. `robocopy`/`scp`/`tar` are
still matched by executable name and are unaffected.
