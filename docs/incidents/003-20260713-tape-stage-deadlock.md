# 003 — Pipeline hard-hangs mid-chunk (governor deadlock)

- **When:** 2026-07-13 (~1 h 48 m, tape-write stage), 2026-07-15 (~2.5 h,
  fetch/stage boundary, session 37); root-caused 2026-07-23
- **Physical intervention required:** no
- **Status:** fixed (`0552a52`, regression test in `7a5253b`)

## Symptom

The pipeline stops making progress mid-chunk with **all threads at 0% CPU**. The
tell is that the governor and heartbeat lines *keep printing* while no `[FETCH]`
or tape progress ever appears again.

> Beware the false alarm: the heartbeat also sits flat for ~25 minutes during a
> perfectly **normal slow PACK**. Confirm against `[FETCH]` / `[PACK]` / `[TAPE]`
> log lines before declaring a hang.

## Root cause (confirmed 2026-07-23, first py-spy dump)

A **`db_sync` ⇄ `pack` deadly embrace** in `ResourceGovernor.decision()`
([src/resource_governor.py](../../src/resource_governor.py)):

- The tape thread finishes robocopy (`tape_active` clears), then does its DB
  catalog write via `wait_or_pause("db_sync", "continue")` — but the `db_sync`
  branch blocks while `pack_active`.
- Meanwhile the **next** chunk's pack has entered `mark_pack_active()` and calls
  `wait_or_pause("pack", …)` — but the `pack` branch blocks while `db_sync_active`.

Each holds its own active flag while waiting for the other's to clear → permanent
circular wait. Governor stdout proves it, looping every 30 s:

```text
db_sync continue ... tape_active=false, reason=pack_active
pack    continue ... tape_active=false, reason=db_sync_active
```

It is intermittent because it needs a timing race between chunk N's tape write
finishing and chunk N+1's pack starting.

## Recovery (if an old build ever hangs again)

1. Confirm no `robocopy` is running (`tasklist`, case-insensitive — it registers
   as `Robocopy.exe`).
2. `CTRL_C` is useless when wedged in a native tape call; force-killing
   `python.exe` is safe — the LTFS driver is a separate process and the cartridge
   stays mounted.
3. Reconcile the `backing` chunk, then resume. The preserved pack is reused with
   no re-fetch and no re-pack.

## Note

This was **not** fixed by the earlier connection-retry patch (incident
[004](004-20260714-postgres-pool-timeout.md)) — that addressed a different
failure. This one is a pure lock/queue deadlock.
