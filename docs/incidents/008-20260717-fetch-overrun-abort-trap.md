# 008 — Partial resume trips the fetch-overrun hard abort

- **When:** 2026-07-17 (session 37, chunk 19)
- **Physical intervention required:** no
- **Status:** worked around; **config revert still pending**

## Symptom

```text
[FETCH][ALERT] aborting fetch: 3.0 GB fetched exceeds 2.0x the planned 1.5 GB
```

The crash surfaces as a **red-herring `EOFError` traceback**, which sends you
looking in the wrong place.

## Root cause

A *partial* resume plans only the **remaining** files' bytes, but the tar stream
re-pulls **whole batches**. Actual bytes therefore blow past planned bytes and the
watchdog hard-aborts at `fetch_overrun_abort_factor` (default 2.0).

The `[FETCH] 170.0% | 2.5/1.5 GB` overrun that earlier sessions documented as
"cosmetic — not data loss" is the **same phenomenon**. It is cosmetic as to data,
but it is *not* harmless: past 2.0× it kills the run.

## Fix / workaround

The abort is **self-healing**: it cleans staging, so a plain relaunch replans the
chunk whole and succeeds. **Try a plain relaunch first** before changing config.

On 2026-07-17 `fetch_overrun_abort_factor` was overridden to unblock chunk 19.

## ACTION REQUIRED — pending reverts

Two `config.ini` overrides are commented in place and must be reverted **together
at session 37's end**:

| Key | Current | Revert to |
| ----- | --------- |-----------|
| `[PERFORMANCE] fetch_overrun_abort_factor` | override | `2.0` |
| `[PERFORMANCE] allow_resume_oversized_chunks` | `true` | `false` |

`allow_resume_oversized_chunks=true` was set on 2026-07-12 to resume session 37,
whose chunks were planned at 400k files against a 200k limit (session 37 is
**96 chunks**, not 21).

Neither value hot-reloads — they are read once at startup into
`RemoteOrchestrator`, so reverting requires a restart. The operator decided on
2026-07-17 not to restart purely to restore them.
