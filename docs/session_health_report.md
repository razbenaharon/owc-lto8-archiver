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
| 37 | **blocked** | active | incomplete | 49 done · 64 pending | Tape_03 (**1, RETIRED**) | conservative bootstrap **done**; generation decision outstanding |

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

## Session 37 — blocked: its tape generation was destroyed

This is the most serious finding in the catalog, and it was **not** previously
recorded anywhere.

```
Tape_03 generation 1  formatted 2026-06-28  RETIRED 2026-08-02 09:56
Tape_03 generation 2  formatted 2026-08-02  RETIRED 2026-08-02 12:07
Tape_03 generation 3  formatted 2026-08-02  ACTIVE
retired_reason: "physical contents intentionally destroyed by tape reset"
```

Session 37 is bound to **generation 1**. Its 49 `done` chunks therefore describe
data on a medium that was deliberately wiped — twice — and `tapes.used_space`
for Tape_03 is `0`, which corroborates it exactly.

**Consequences an operator must accept before any resume:**

* The 49 `done` chunks are done *in the catalog only*. Their bytes are not on the
  cartridge.
* `_verify_session_tape_generation` compares the session's persisted generation
  against the catalog's active one and blocks the run, so a `--resume` stops
  before touching the drive. That guard is working; do not bypass it.
* Deciding what to do about those 49 chunks — re-plan, abandon, or accept the
  loss — is an operator decision, and a large one. It is explicitly **out of
  Plan 1 scope**: Plan 1 must not change chunk membership.

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
