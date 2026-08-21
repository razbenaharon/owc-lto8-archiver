# 015 — 2026-08-20/21: Campaign drive failing; evacuation required — OPEN, URGENT

**Status: OPEN and escalating. The drive holding the only copy of the
campaign store is failing and is currently absent from the system.**

## Evidence

**2026-08-20** — the external 2 TB campaign drive disconnected twice within an
hour under sustained I/O. Writes failed with "device is not ready" / "no
medium found" while the volume still listed directories. The Windows System
log recorded a storm of **disk event 51** ("error detected during a paging
operation") and **NTFS event 140** ("The system failed to flush data to the
transaction log. **Corruption may occur**").

**2026-08-21** — the drive degraded further:

- The operator reports **audible mechanical noise** from the enclosure.
- **292 disk-51 events in a single hour.**
- The drive's **physical disk is no longer enumerated at all**: `Get-Disk`
  lists only the internal NVMe and the healthy target drive, and
  `Get-Partition -DriveLetter E` returns nothing.
- `Get-Volume` nevertheless still reports the volume `Healthy / OK` with a
  size and free space, and directory listings still succeed.

That last pair is the trap this repository has hit before (see
[incident 011](011-20260726-tape-swap-blockers.md) for the LTFS equivalent):
**a volume that drops while mounted keeps serving cached directory metadata.**
`Test-Path`, `os.listdir` and `os.path.getsize` all succeed against a drive
that is physically gone; only an actual read of file *content* tells the
truth. Tooling that classifies failures must therefore re-probe the root
rather than trust an errno — a vanished medium surfaces as `EACCES`
("permission denied"), which otherwise reads as data corruption.

## What is at risk

The drive holds the **only complete copy** of the localization campaign:
stored-TAR containers and receipts for Session 37 chunks 49–216, plus the
abandoned-fetch-state artifact and the tape03/tape04 diagnostic packages. The
tapes written from it are partial and unverified
([incident 014](014-20260819-campaign-write-servo-halt.md)).

## What was proven while the drive was still readable

A structural verification pass completed against every chunk directory
(`scripts/verify_campaign_store.py --mode structure`):

| Result | Count |
| --- | --- |
| Chunks whose every receipted container is present at its recorded size | **168** |
| Containers verified | **184** (695.3 GiB) |
| Size mismatches / truncation | **0** |
| I/O errors during the pass | **0** |
| Chunks without a receipt | **1** (`chunk_000217`) |

So as of the last readable moment the store was structurally intact. The full
SHA-256 pass began but **could not complete** — the drive vanished partway
through, and no container has yet been content-verified.

## Evacuation findings (2026-08-21)

Evacuation to healthy storage began while the drive was intermittently
responsive. The receipt group — a few hundred kilobytes total, copied first
precisely because it is what makes everything else provable — produced the
most diagnostic result of the whole incident:

- **147 of 168 receipts copied. 21 failed, and failed identically on every
  retry.** Deterministic failure on a fixed set of files is damaged media,
  not the transient bus dropouts seen on 2026-08-20.
- **The failures are contiguous:** `chunk_000090`, then an unbroken run from
  `chunk_000196` through `chunk_000216`. A contiguous failure set means one
  damaged *region* of the platter rather than scattered bad sectors — and
  chunks 196–216 were the last written (2026-08-11/12), so they are
  physically adjacent.
- A 20 GiB container in that region appeared to copy out while neighbouring
  receipts could not be read. **That appearance was false** — see the outcome
  section: the file was a pre-allocated placeholder containing nothing. No
  container was recovered from the damaged region or anywhere else.

Consequence: for any chunk whose receipt is lost, the container cannot be
content-verified locally — its plan manifest and TAR sidecar live on the
production host's metadata root, which is not reachable from this
workstation. If both the receipt and the container are lost for a chunk, that
chunk is recoverable only by re-fetching from the remote source and
re-localizing.

Recovery doctrine that follows: **copy the healthy region first and in full,
then attempt the damaged region.** Reads against damaged sectors hang for a
long time and stall everything queued behind them — which is exactly what the
first evacuation attempt did when a small, already-duplicated diagnostics
folder blocked 695 GiB of irreplaceable containers.

## Outcome — what survived and what did not (2026-08-21)

The drive left the bus for good partway through the evacuation and no longer
resolves at all. Final accounting, measured rather than estimated:

| | |
| --- | --- |
| Real data rescued | **198.3 KB** — 147 of 168 `receipt.json` files, plus 2 small diagnostic text files |
| Containers rescued | **0 of 184** |
| Archive payload lost from this drive | **695.3 GiB** |
| Receipts lost | 21 (`chunk_000090`, `chunk_000196`–`chunk_000216`) |

**A 20 GiB file on the rescue target was NOT rescued data.** Robocopy's
restartable mode (`/Z`) pre-allocates the destination at full size, so the
interrupted copy of `chunk_000217/container_0000.tar` left a full-length file
containing nothing. A byte scan of all 21,474,574,336 bytes found **zero
non-zero bytes**, and a streaming TAR parse reached end-of-archive with zero
members in under three seconds. It has been renamed
`…tar.EMPTY-PLACEHOLDER-NOT-DATA` so it cannot be mistaken for a rescued
container. **Size is not evidence of content** — check bytes, not lengths,
before recording anything as recovered.

### What the 147 surviving receipts are actually worth

They are 198 KB that describe **317,074,964,480 bytes (295.3 GiB) of
containers and 29,354,756 archived files**, each with its container SHA-256,
sidecar SHA-256, membership fingerprint and plan-manifest locator. So for 147
of 169 chunks the exact expected content is still known: a re-localized copy
can be proven byte-identical to what was lost. For the 21 chunks whose
receipt is gone, even the inventory is gone.

That is the entire argument for copying receipts first, and it held: the
cheapest thing on the drive was the only thing worth saving that we managed
to save.

### What was never at risk

The production archive itself is untouched. Tape_01 and Tape_02 are closed
and physically elsewhere; their per-file manifests (145 `JSONL.zst` segments,
242 MB) live on the internal NVMe; the PostgreSQL catalog and 2.5 GB of
verified dumps are on the internal drive; the source code and documentation
are in git. The vendor diagnostic package also survives in the repository's
private area.

**This was the loss of a staging copy, not of the backup.** Session 37 chunks
49–216 were fetched from the remote source and had not yet been committed to
tape. If that source data still exists, the recovery path is to re-fetch and
re-localize — which makes *confirming the remote source is still intact* the
most urgent action after the drive itself.

## Required procedure (operator)

1. **Stop using the drive. Power it down.** Mechanical noise plus bus
   dropouts means every additional spin-up risks more data. Do not run
   `chkdsk`, do not "repair" it, do not defragment it, do not let a backup
   tool retry against it — all of those write to, or hammer, failing media.
2. **Rule out the cheap causes first**, since an enclosure or cable failure
   presents identically and is trivially fixable: try a different cable, a
   different port (rear/motherboard port, not a hub), and mains power rather
   than bus power. Re-check `Get-Disk` after each change — the drive is back
   only when it appears there, not when `Test-Path` says so.
3. **If it returns, evacuate immediately and do nothing else with it:**

   ```powershell
   powershell -ExecutionPolicy Bypass -File scripts\evacuate_campaign_store.ps1 `
     -Source E:\OWC-LTO -Destination G:\OWC-LTO-RESCUE
   ```

   The script copies irreplaceable-first (receipts before the containers they
   verify), retries minimally, restarts large files mid-transfer, and skips
   what already exists — so it can simply be re-run after another dropout.
4. **Verify the rescued copy**, not the original:

   ```powershell
   python scripts\verify_campaign_store.py `
     --root G:\OWC-LTO-RESCUE\LTO_METADATA\LOCAL_MANIFEST_ARCHIVE\campaign_tape03 `
     --mode full --report private\campaign_full_report.json --resume
   ```
5. **If it does not return**, the containers are recoverable only by
   professional data recovery, or by re-fetching from the remote source and
   re-localizing — the plan manifests and remote source, not the tapes, are
   what make that possible.

## Decisions already taken

- The permanent manifest root and the next run's staging were moved **off**
  this drive to the internal NVMe (`C:\LTO_METADATA`, `C:\LTO_STAGING`), so
  no operational state depends on it.
- **Nothing was deleted from the drive**, including two items that would
  otherwise be cleanup candidates — a duplicate of campaign chunk 57 under
  `LTO_CAMPAIGN_STAGING`, and the receiptless `chunk_000217` container. On
  failing media a redundant copy is an asset and the deletion itself is a
  risk.
- The PostgreSQL backups were written to the internal drive after two copy
  attempts to this one failed mid-write.

## Lessons that outlive this incident

- **Metadata is not evidence of readability.** Any health check against
  removable media must read file content; `Test-Path`/`Get-Volume`/`getsize`
  will confirm a drive that is physically absent.
- **A vanished medium can surface as a permission error.** Classify by
  re-probing the root, never by errno alone, or a dead drive gets reported as
  corrupt data.
- **One copy is not a copy.** The campaign was localized to a single external
  drive with no second copy for ten days. The receipts made the store
  *verifiable*, but verifiability is not redundancy.
