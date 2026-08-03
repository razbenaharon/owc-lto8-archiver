# Plan 1 — operational handoff

**Read this before the next archive run.** One page, plain language.
Full evidence: [`plan1_completion_gate.md`](plan1_completion_gate.md).

## In one sentence

The remote pipeline was one 3,657-line file that did everything; it is now a
façade over four focused modules, `remote_writer.py` is the sole entry path for a
finite tape-write group, and the persistent frontier scanner is the only scanner
a production run can build. Cartridge checks also remain in the façade.

## What an operator will actually notice on the next run

Nothing about how you start a run changes. Five behaviours differ:

1. **You name the cartridge; the software no longer guesses it.**
   Set `[REMOTE] tape_label` in `config.ini`, or answer the prompt. Previously
   a new session read the drive to find out which tape was loaded.
2. **Nothing touches the drive at startup.** The cartridge is verified once per
   write group, while holding LTFS ownership. Both loops now print a loud
   announcement of the target cartridge first, so a wrong tape is caught up
   front instead of ~40 minutes in, silently.
3. **The remote pipeline never ejects** — not even with
   `eject_after_session = true`. An eject with nobody at the drive cannot be
   undone remotely, so the option is now ignored on this path.
4. **A resumed, scan-complete session writes multi-chunk groups.** The old
   group-of-one shortcut is gone.
5. **`SUMMARY.csv` has 17 new `scan_*` columns**, appended at the end.
   Aggregates only — existing columns and their order are unchanged.

## Production activation — 2026-08-03

Two passes. The first applied migration 014; the second made the persistent
frontier the sole production scanner and migrated Session 37. The former
`incremental_scan` key was never a runtime selector and is no longer a feature
flag. If the old key remains in an untracked live config, it is deprecated and
ignored.

| Step | Result |
|---|---|
| Plan 1 code on `origin/main` | **Done** |
| Verified production backup | 647 MB dump + `pg_restore --list` verified |
| Migration 014 on production | **Applied and finalized**; existing data unchanged |
| All-session health report | 4 sessions audited — [`session_health_report.md`](session_health_report.md) |
| Production scanner | **Persistent frontier only** — no legacy scan mode or feature flag |
| Session 37 conservative bootstrap | **done** — 65 scopes, 65 pending roots, nothing traversed |
| Shadow rehearsal on a restored copy | **PASS** — 12/12 invariants unchanged |
| Any session resumed | **No.** No run, no scan, no tape operation |

Migration 014 changed no existing data: 113 chunks / 49 done / 64 pending /
23,214,474 plan member rows identical before and after.

### Session 37: conservative bootstrap, and a serious finding

**A correction first.** Session 37 is bound to **Tape_03 generation 1**, which
was retired as *destroyed*. An earlier revision concluded its 49 `done` chunks
were therefore lost. They are not. All 9 of the session's archive runs wrote to
**Tape_02**, `files_index` records 2,036 stored objects / 710 GB from them on
Tape_02, zero archive runs reference Tape_03, and `files_index` has zero rows on
Tape_03. No Session 37 archive work was written to Tape_03. That cartridge was
nevertheless written for a separate 24 GiB Phase 5E synthetic pilot and was
reformatted twice, so it must not be described as never written. The finished
Session 37 work sits on Tape_02 generation 1, still active, and is restorable.

`tape_label = Tape_03` is the session's **next** target, set when Tape_02
filled. `_verify_session_tape_generation` still blocks a resume, correctly —
the session would continue writing onto a cartridge reformatted twice since it
was planned. Where the 64 pending chunks should go is an operator decision.

**What the bootstrap did.** The gate used to refuse any session whose scan had
not finished — which is every session it exists for. That was wrong: an
unfinished scan is the *expected input*, and `backing` chunks, mid-flight work,
a shared plan or a live worker are what must actually block. The conservative
bootstrap now creates the scope rows and queues each configured root as
`pending`, and stops. It lists no directory, publishes no segment, imports no
membership, finalizes nothing, and does not mark the scan complete.

Nothing is skipped (every root starts `pending`, so the whole source is queued)
and nothing is duplicated (when the next run lists a directory, its segment is
reconciled once against the 23.2M existing rows and only genuinely new entries
reach the chunk builder). Repeating it writes nothing new, and a completed
bootstrap refuses a second one. The durable `remote_frontier_bootstraps` row is
also what identifies this as a migrated legacy session after bootstrap. With no
row, only a definite empty snapshot is treated as frontier-born; existing
membership or missing, exceptional, or indeterminate evidence fails toward
reconciliation, never toward replay.

### The persistent frontier is the sole production scanner

The first activation pass exposed that the old `incremental_scan` property and
`decide_scan_mode()` helper had zero effect on the run path. They were not a
working mode gate. The production streaming path now builds
[`FrontierScanCoordinator`](../src/scan_frontier.py) unconditionally. The dead
decision API and feature property are gone: there is no legacy scan mode and no
incremental-scan feature flag.

**The new architecture, in one picture:**

```text
DirectoryFrontierScanner   lists ONE directory over SSH
        |
DirectoryFrontierCoordinator   claims a directory, writes its listing as a
        |                      JSONL.zst segment, queues its children, commits
        |                      -> a crash replays at most that one directory
        |
SegmentChunkPublisher      reads the segment artifact; for a migrated session,
        |                  reconciles it ONCE against the legacy snapshot
        |                  (path AND size, one
        |                  set-based query per segment), and passes ONLY the
        |                  genuinely new entries to the chunk builder
        |
StreamingChunkBuilder      chooses chunk boundaries -- from survivors only
        |
sealed chunk -> stager -> ready queue -> finite write group (sole tape-writing path)
```

The ordering is the point. The old whole-root path filtered already-planned files
*after* they had moved the chunk boundary, so a resumed scan produced different
boundaries from the original run for the same source. Now a known file never
reaches the builder at all.

**No runtime fallback, deliberately.** An unusable migration-014 schema stops the
run (`SAFETY_BLOCK` / `scan_frontier_unavailable`) instead of quietly picking the
old scanner. A fallback is exactly how two scanners end up running against one
frontier. Git history and the verified PostgreSQL backup are the rollback path.

**Scan finality now needs traversal evidence.** `remote_sessions.scan_complete`
is set only when *every* scope reports final coverage, which requires each
directory terminal, every descendant subtree final, and the mutation sweep
finding nothing changed. A permission error or a partial directory leaves the
session resumable — which is why the report can honestly say "the scan never
completed" about session 37 rather than guessing from its 23M catalog rows.

## Two bugs this work found and fixed

- **Two different Linux files could collapse into one catalog entry.** A
  backslash is an ordinary filename character on Linux, but
  `_canonical_remote_path` rewrites every `\` to `/`, so `a\b` and `a/b` became
  the same key. The existing catalog is built on that rewrite and cannot be
  changed under it, so such paths are now **detected and held back** from
  planning with an `unrepresentable_path` error rather than silently merged.
- **An unreadable `backing` status used to be treated as "clear".** It now
  stops the run. `backing` means a tape write whose outcome is unknown — the
  data may already be on the cartridge. Guessing wrong means writing it twice.
  See [incident 010](incidents/010-20260724-ltfs-write-perm-readonly.md).

## Running the tests

PostgreSQL tests now **require** an explicitly configured disposable server.
Previously a bare `python -m pytest` on this host connected to the production
catalog server, because the defaults are `localhost:5432` and that is exactly
where `lto_pg` listens.

```powershell
docker run -d --name lto_pg_test -e POSTGRES_DB=postgres -e POSTGRES_USER=lto `
  -e POSTGRES_PASSWORD=<pw> -p 127.0.0.1:15432:5432 `
  --tmpfs /var/lib/postgresql/data:rw,size=2g --shm-size=1g postgres:17

$env:LTO_TEST_PG_DSN = "postgresql://lto:<pw>@127.0.0.1:15432/postgres"
$env:LTO_PG_SEALED_BATCH_IT = "1"
python -m pytest tests/ -q            # 1461 passed, 0 skipped; 12 subtests passed

docker rm -f lto_pg_test              # tmpfs: the server vanishes with it
```

Without `LTO_TEST_PG_DSN` the suite still runs (1312 passed, 149 skipped,
12 subtests passed; 2 warnings) and opens no database connection. Point it at
port 5432 and it **fails loudly** at
collection rather than skipping — an unsafe run must never look green. Details:
[`tests/pg_test_guard.py`](../tests/pg_test_guard.py).

## Still outstanding

| # | Item | Who |
|---|---|---|
| 1 | **Operator-supervised tape rehearsal** — stage 3 of `scripts/plan1_rehearsal.py`. Nine of its ten claims are already proven against fakes; this confirms a real drive agrees. Status: **NOT RUN**. | operator, at the drive |
| 2 | Remove the obsolete `incremental_scan` line from the untracked live `config.ini`. It is ignored and no longer selects runtime behaviour. | operator |
| 3 | Three pending `config.ini` changes from session 37, all unrelated to Plan 1: revert `allow_resume_oversized_chunks` to `false`, revert `fetch_overrun_abort_factor` to `2.0`, and apply the narrowed 5-root selection staged in `backup_logs/next_session_selected_paths.txt`. **All three only after session 37 reaches `status='completed'`** — editing `remote_selected_paths` in flight changes the session key and makes `--resume` fail to find the session. | operator |

Item 1 must **not** include an ambiguous-write test on real media: provoking a
latching write error can leave a cartridge permanently read-only.

## Where things live now

| File | Role |
|---|---|
| [`src/remote_orchestrator.py`](../src/remote_orchestrator.py) | Façade. Session setup, cartridge checks, and delegation. 3,657 → 2,506 lines. |
| [`src/remote_writer.py`](../src/remote_writer.py) | The sole finite write-group entry path; owns remote tape-copy execution. |
| [`src/remote_staging.py`](../src/remote_staging.py) | Fetch and pack. |
| [`src/remote_pipeline.py`](../src/remote_pipeline.py) | One scheduling loop for both session kinds. |
| [`src/scan_frontier.py`](../src/scan_frontier.py) | Sole production scanner and frontier coordinator; schema readiness fails closed. |
| [`src/startup_reconcile.py`](../src/startup_reconcile.py) | What to do about an interrupted previous run. |
| [`tests/pg_test_guard.py`](../tests/pg_test_guard.py) | Refuses any PostgreSQL connection that is not provably disposable. |

Rationale for the boundaries:
[`plan1_module_boundary_audit.md`](plan1_module_boundary_audit.md).
