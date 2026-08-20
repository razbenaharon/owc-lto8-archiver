# 015 — 2026-08-20: Campaign drive (external E:) intermittent disconnects — OPEN

**Status: OPEN — the drive holding the only copy of the campaign containers
is unstable. Evacuate before anything else touches it.**

## What happened

During post-campaign cleanup work, the external 2 TB campaign drive (mounted
`E:`) disconnected twice within one hour under sustained I/O:

- Writes failed with "The device is not ready" / "No medium found" while the
  volume still listed directories.
- The Windows System log recorded a storm of **disk event 51** ("error
  detected during a paging operation") and **NTFS event 140** ("The system
  failed to flush data to the transaction log. **Corruption may occur** in
  VolumeId: E:").

## Why this is urgent

That drive currently holds the **only complete copy** of the localization
campaign: stored-TAR containers + receipts for Session 37 chunks 49–216
(~2 TB class data), the abandoned-fetch-state export artifact, and the
tape03/tape04 diagnostic packages. The tapes written from it (incident 014)
are partial and unverified, and the source-side data may not remain
available indefinitely.

## Decisions taken during the 2026-08-20 cleanup

- **Nothing was deleted from E:.** Two items that would normally be cleanup
  targets were deliberately kept, because redundant copies on a failing
  drive are an asset:
  - `LTO_CAMPAIGN_STAGING/chunk_000057/` — an apparent duplicate of campaign
    chunk 57 (same container size; hash comparison was aborted when the
    drive dropped mid-read).
  - `campaign_tape03/chunk_000217/` — a receiptless, unverifiable container.
- The permanent manifest root and the next run's staging were **moved off
  E:** to the internal NVMe (`C:\LTO_METADATA\LOCAL_MANIFEST_ARCHIVE`,
  `C:\LTO_STAGING`) so no operational state depends on the flaky drive.
- The pre-cleanup PostgreSQL backup was stored on `C:` after two write
  attempts to E: failed mid-copy.

## Required next steps (operator)

1. Check the physical link first (cable, port, powered hub) — event-51
   storms are classically enclosure/cable, but treat the data as at-risk
   either way.
2. Copy the campaign tree (`LTO_METADATA/LOCAL_MANIFEST_ARCHIVE/
   campaign_tape03/` + `abandoned_fetch_state/` + `LTO_DIAG/`) to a stable
   disk, verifying every container against its receipt SHA-256 during the
   copy.
3. Only after a verified second copy exists, retire or repurpose the drive.
