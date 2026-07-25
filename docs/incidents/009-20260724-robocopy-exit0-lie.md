# 009 — Robocopy reported success (exit 0) having copied nothing

- **When:** 2026-07-24 ~07:00 (session 37, chunk 49)
- **Physical intervention required:** no
- **Status:** fixed (`efda427`, `04dc841`, `19106f3`)

## Symptom

Robocopy ran for ~10 minutes, copied **0 files**, and the application reported:

```text
robocopy exit 0
copied 0
failed 0
```

Robocopy had in fact emitted an `ERROR` and never produced a trustworthy final
summary. The application still refused to commit the chunk and left it `backing`
(ambiguous) — correct behaviour — but the reported numbers were misleading, and
**the raw Robocopy output was lost because it was not durably logged**, which is
what made the first failure un-diagnosable.

## Root cause

Three separate weaknesses:

1. **Trusting the return code.** Robocopy can exit **0** after
   `ERROR: RETRY LIMIT EXCEEDED`. Return code alone can never establish success.
2. **No durable raw log.** The evidence existed only in a stream nobody kept.
3. **Summary parsing bug.** Robocopy's options header contains the line
   `Files : *.*`, which the parser mistook for the final `Files` summary row.

## Fix

- **Durable per-write raw logging** to
  `backup_logs/tape_write/session_<id>/chunk_<idx>_<timestamp>.log`, written
  live, including the exact command, expected files/bytes, the full output, and
  the classification block. When local session/chunk ids are unavailable the log
  is labelled from the remote ids instead of being dropped (`04dc841`).
- **Conservative classifier** — a write counts as successful only on *complete*
  evidence. It accounts for: return code, retry-limit exhaustion, missing or
  malformed summary, failed file counts, zero-copy when source work was expected,
  interruption, and source-side expected work.
- **Parser fix** so the `Files : *.*` options echo is not mistaken for the
  summary (`19106f3`).

## Proof it works

The very next attempt (2026-07-24 10:16) captured the real error that the first
failure had hidden — see
[incident 010](010-20260724-ltfs-write-perm-readonly.md). Raw log:
`backup_logs/tape_write/session_na/chunk_na_20260724_101636_203.log`, classified
`FAILURE (retry_limit_exceeded)`, nothing committed to the database.

## Standing rule

**Never infer that `returncode == 0` means Robocopy succeeded.**
