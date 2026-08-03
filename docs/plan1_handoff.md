# Plan 1 — operational handoff

**Read this before the next archive run.** One page, plain language.
Full evidence: [`plan1_completion_gate.md`](plan1_completion_gate.md).

## In one sentence

The remote pipeline was one 3,657-line file that did everything; it is now a
small façade over four focused modules, the tape is touched from exactly one of
them, and a faster "only scan what changed" scanner is built but **switched
off**.

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

## Production activation — what actually happened on 2026-08-03

| Step | Result |
|---|---|
| Plan 1 code on `origin/main` | **Done** (`3b810f1`) |
| Verified production backup | **Done** — 647 MB dump + `pg_restore --list` verified |
| Session 37 frontier report (read-only) | **`verdict: blocked`** |
| Migration 014 on production | **Applied and finalized**, invariants validated |
| Frontier bootstrap dry run | **`would_proceed: false`** |
| Frontier bootstrap execution | **NOT RUN** — gate failed, executor refuses by design |
| `incremental_scan = true` | **Set** (see the caveat below) |
| Session 37 resumed | **No.** No run, no tape operation, no scan |

Migration 014 changed no existing data: 113 chunks / 49 done / 64 pending /
23,214,474 plan member rows were identical before and after, and all seven new
tables are empty.

### Session 37 cannot be bootstrapped, and that is correct

Its scan never finished — `scan_complete = false`, killed by an SSH reset — so
the plan's full membership is unknown and "all chunks done" cannot mean
"finished". Both the read-only report and the bootstrap dry run block on
exactly that one condition, and `FrontierBootstrap.execute()` re-runs the dry
run and raises `BootstrapRefused` rather than proceeding. Nothing here is
broken; the gate is doing its job.

To unblock it later, Session 37's scan has to complete first. That is a run,
and a run is a separate operator decision.

### `incremental_scan = true` is set — and is currently a no-op

Measured, not assumed. `decide_scan_mode()` does return `MODE_FRONTIER`, but
**nothing consumes that decision**:

- `RemoteOrchestrator._resolve_scan_mode()` sets `self._scan_mode`, and
  `self._scan_mode` is never read anywhere.
- The streaming path builds its scanner with `build_legacy_scanner_factory()`
  unconditionally ([remote_orchestrator.py:1097](../src/remote_orchestrator.py#L1097)).
- `build_frontier_scanner_factory()` has **zero callers** in `src/` or `tests/`.
- `DirectoryFrontierCoordinator` is constructed in exactly one place —
  `FrontierBootstrap.execute()` ([frontier_bootstrap.py:166](../src/frontier_bootstrap.py#L166)),
  reachable only via `--bootstrap-frontier --execute`.

So the flag is safe to leave true — it cannot hand a session to a scanner it
was never bound to — but it does not yet deliver the speed-up either. **Wiring
the run path to the scan-mode decision is remaining Plan 1 work**, and it is
the next thing to do on this plan. The frontier machinery itself is built and
tested; only the connection from decision to scanner is missing.

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
python -m pytest tests/ -q            # 1408 passed, 0 skipped

docker rm -f lto_pg_test              # tmpfs: the server vanishes with it
```

Without `LTO_TEST_PG_DSN` the suite still runs (1259 passed, 149 skipped) and
opens no database connection. Point it at port 5432 and it **fails loudly** at
collection rather than skipping — an unsafe run must never look green. Details:
[`tests/pg_test_guard.py`](../tests/pg_test_guard.py).

## Still outstanding

| # | Item | Who |
|---|---|---|
| 1 | **Operator-supervised tape rehearsal** — stage 3 of `scripts/plan1_rehearsal.py`. Nine of its ten claims are already proven against fakes; this confirms a real drive agrees. Status: **NOT RUN**. | operator, at the drive |
| 2 | Read-only session-frontier report against production session 37 (`inspect_db.py --session-frontier-report --session-id 37`). | operator |
| 3 | Migration 014 on production, after a verified backup, with no archiver running. | operator |
| 4 | Three pending `config.ini` changes from session 37, all unrelated to Plan 1: revert `allow_resume_oversized_chunks` to `false`, revert `fetch_overrun_abort_factor` to `2.0`, and apply the narrowed 5-root selection staged in `backup_logs/next_session_selected_paths.txt`. **All three only after session 37 reaches `status='completed'`** — editing `remote_selected_paths` in flight changes the session key and makes `--resume` fail to find the session. | operator |

Item 1 must **not** include an ambiguous-write test on real media: provoking a
latching write error can leave a cartridge permanently read-only.

## Where things live now

| File | Role |
|---|---|
| [`src/remote_orchestrator.py`](../src/remote_orchestrator.py) | Façade. Session setup and delegation. 3,657 → 2,330 lines. |
| [`src/remote_writer.py`](../src/remote_writer.py) | **The only module that touches tape.** If a change cannot affect the drive, it does not belong here. |
| [`src/remote_staging.py`](../src/remote_staging.py) | Fetch and pack. |
| [`src/remote_pipeline.py`](../src/remote_pipeline.py) | One scheduling loop for both session kinds. |
| [`src/scan_frontier.py`](../src/scan_frontier.py) | Scan-mode gate and the frontier coordinator. |
| [`src/startup_reconcile.py`](../src/startup_reconcile.py) | What to do about an interrupted previous run. |
| [`tests/pg_test_guard.py`](../tests/pg_test_guard.py) | Refuses any PostgreSQL connection that is not provably disposable. |

Rationale for the boundaries:
[`plan1_module_boundary_audit.md`](plan1_module_boundary_audit.md).
