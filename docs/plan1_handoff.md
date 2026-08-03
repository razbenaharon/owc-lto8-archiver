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

## What is built but OFF

The incremental scan frontier. It replays only the directories that changed
instead of re-walking the whole root (measured: 6 entries replayed vs 89,430).
It is off because turning it on is an operator decision with a database
migration behind it:

- `incremental_scan = false` in `config.ini`.
- Migration 014 is applied to **no** database — not production, not anywhere.
- Session 37 has not been read or modified.

`decide_scan_mode()` in [`src/scan_frontier.py`](../src/scan_frontier.py) is the
single gate. If anything is missing or unreadable it returns `MODE_LEGACY` or
`MODE_BLOCKED` — it never guesses its way into the new path.

To enable it later, work through §10 of the completion gate in order. Do not
skip the dry run.

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
