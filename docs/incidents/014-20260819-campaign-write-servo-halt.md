# 014 — 2026-08-18/19: Campaign one-copy tape writes halt on servo fault at chunk 82 — PHYSICAL

**Status: OPEN — campaign tape writes are suspended pending a healthy drive.**

## Background: the localization campaign (2026-08-10..12)

After incident 013 left no healthy write path, Session 37's remaining work
was **localized instead of streamed**: chunks 49–216 were fetched from the
remote source and materialized as stored-TAR containers on a local campaign
drive, each chunk with a `receipt.json` recording container SHA-256, sidecar
SHA-256, member count, logical bytes, and the plan-manifest locator
(`campaign-localizer/1.0`, receipts dated 2026-08-10T11:45Z through
2026-08-11T22:59Z). This is the local-first flow the architecture now
mandates: **remote → local disk → validated staging → tape**.

- 168 chunks (49–216) have complete containers + receipts.
- Chunk 217 has a container TAR but **no receipt** — its localization or its
  copy to the campaign drive was interrupted. It is unverifiable and must be
  re-localized; do not treat it as evidence.

## What happened at the tape

1. **2026-08-18** — robocopy one-copy run (options `/COPY:DT /J /XO /R:1`)
   from the local one-copy staging tree to the mounted LTFS volume. Chunks
   49–81 copied; at `chunk_000082` the copy failed with ERROR 31 ("device not
   functioning") followed by ERROR 19 ("media is write protected") — the
   write-protect latch signature of a servo PWE. LTFS then failed directory
   creation for chunks 83–85 (-1126, read-only).
2. **2026-08-19** — a fresh cartridge (Tape_04, barcode AAAAAV, generation 1)
   was mounted on the same drive. The copy reached the same file —
   `chunk_000082/chunk_000082_stored_tar_0000.tar` — and the drive raised
   **-20301 Track Following Error (Servo)** again; LTFS dropped to read-only
   and persisted a 68-file index ("Write perm").

Two different cartridges failing identically at the same write point on the
same drive confirms incident 013's verdict: **the drive is at fault**. The
runbook's own escalation rule ("if a second cartridge fails, escalate
hardware rather than loading a third") applies from this point.

## Current state (as of 2026-08-20)

- Tape_03 (gen 3) and Tape_04 (gen 1) each hold a partial, **unverified**
  copy of campaign chunks 49–81/82. Neither copy was committed to any
  catalog: `archive_runs`, `files_index`, and the tape accounting carry
  **zero** rows for them. They are scratch until a healthy drive can verify
  or rewrite them; neither is production.
- Tape_01 and Tape_02 are untouched by the campaign and remain the only
  closed production tapes.
- The campaign containers and receipts remain on the local campaign drive —
  see [incident 015](015-20260820-campaign-drive-instability.md) for the
  urgent caveat about that drive's health.
- LTFS/robocopy logs and drive dumps for both attempts are kept off-repo in
  the local diagnostics store (`LTO_DIAG/tape04_servo_20260819/`).

## What prevents a repeat

- No tape write is attempted until a replacement drive passes a synthetic
  pilot on a scratch cartridge.
- When writes resume, the receipts' SHA-256s let every already-localized
  container be re-verified locally before it goes to tape — no re-fetch and
  no tape reads are needed.
