# Runbook — drive cleaning + ITDT diagnostics (at the drive)

Written 2026-07-25 for the Tape_02 permanent-write-error investigation
([incident 010](incidents/010-20260724-ltfs-write-perm-readonly.md)). Everything
here needs somebody **physically at the drive** — the assistant cannot do any of
it remotely, and must not try.

## Why an assistant cannot do this remotely

- **Cleaning requires swapping cartridges.** Tape_02 must come out and the
  cleaning cartridge must go in. There is no software path.
- **Ejecting remotely is forbidden.** `LtfsCmdEject` is physical; a cartridge
  ejected with nobody at the drive cannot be reloaded — there is no software
  "load" for a tape sitting out of the slot.
- **ITDT is installed here as the Graphical Edition only** (`itdt-ge.exe`,
  `itdt-gec.exe` — no CLI binary), so it cannot be driven headlessly, and
  pointing it at the drive takes the device away from LTFS.

## ⚠️ Order matters — diagnose BEFORE cleaning

**Run ITDT first, while the drive is still dirty.** Cleaning clears the
TapeAlert "clean now" flag and resets the condition that would tell us whether
this was debris on the head (drive) or damaged servo bands (medium). Cleaning
first destroys the evidence we are trying to collect.

## ⚠️ ITDT has destructive tests — do not run them

Tape_02 holds ~3.6 TB of archive and 49 committed chunks. Inside ITDT run **only
read-only functions**:

| Safe (read-only) | NEVER run |
|---|---|
| Device / Drive Information | **Full Write Test** |
| **Tape Usage / TapeAlert flags** ← the goal | **Test Drive / Write-Read test** |
| Dump (retrieve drive dump) | **Format / Unformat / mkltfs** |
| Error / Log Sense counters | Anything writing to the cartridge |

If a prompt mentions writing, overwriting, formatting, or "the cartridge will be
overwritten" — **cancel**.

## Facts you will need

These are the last documented production facts from `LAB-HPLB-09`, as observed
2026-07-25; re-verify them at handover.

| Item | Value |
|---|---|
| Drive | IBM ULTRIUM-HH8, serial **1097008774** |
| Device ID | `SCSI\SEQUENTIAL&VEN_IBM&PROD_ULTRIUM-HH8\7&1EDBF2CE&0&000000` |
| Cartridge | Tape_02, last confirmed mounted read-only on `Z:` |
| ITDT | `C:\Program Files\IBM\ITDT Graphical Edition\itdt-ge.exe` |
| LTFS log | `C:\Program Files\IBM\LTFS\log\LogFile.csv` |

## Procedure

### 0. Confirm nothing is running

Verified clear at handover on 2026-07-25: no `python.exe`, no `robocopy`. Re-check
if any time has passed — never do this while a write is live.

### 1. ITDT, before cleaning — capture the evidence

1. Close the LTFS GUI if open. ITDT needs the device; LTFS holding it can block
   ITDT, and vice versa.
2. Launch `itdt-ge.exe`, scan for devices, select the ULTRIUM-HH8 (serial
   `1097008774`).
3. Record, and keep the output:
   - **TapeAlert flags** — the decisive ones are *Clean now / Clean periodic*
     (points at the drive/head), *Media / Read-write failure*, *Hard error*,
     *Degraded media*.
   - Drive error counters / log sense.
   - Optionally save a drive dump.
4. **This is the drive-vs-medium answer.** A "clean now" or head-related flag
   means the drive; a media/degraded-media flag means the cartridge.

### 2. Clean the drive

1. Unload Tape_02 (from the LTFS GUI, or eject at the drive) — **note this is the
   point of no return for remote work**; the cartridge is now out.
2. Insert the **LTO cleaning cartridge**. The drive runs the cycle automatically
   and ejects it, typically in a few minutes.
3. If the cleaning cartridge ejects immediately, it is expired — LTO cleaning
   cartridges are good for a limited number of uses. Use a fresh one.

### 3. ITDT, after cleaning — confirm

Re-run the TapeAlert read. The cleaning flag should now be clear. Compare against
step 1 and keep both.

### 4. Reload Tape_02 and hand back

1. Re-insert Tape_02 and let LTFS mount it.
2. **Re-check the drive letter** — it is *not* stable across remounts (it was
   `E:` for months, then `Z:`). Verify `Test-Path Z:\` and that
   `config.ini [HARDWARE] lto_drive` matches. `Get-Volume` cannot see LTFS.
3. Tell the assistant it is back. It can then verify remotely, read-only:
   - the new mount's `sync_type` (must be `time@5`, LTFS event 61259),
   - whether the cartridge still mounts read-only (the PWE bit is stored on the
     cartridge — cleaning the drive does **not** clear it),
   - a fresh `ltfs_media_health()` scoped to the new mount.

## What this does and does not fix

Cleaning addresses the **drive**. It does **not** clear Tape_02's PWE bit — that
lives in the cartridge's MAM chip, so Tape_02 will still mount read-only
afterwards. Cleaning is what makes it safe to trust the drive with the *next*
cartridge, and it is why it must happen before any new media is committed.

After this, the remaining decision for Tape_02 itself is unchanged: attempt
`ltfsck` / SDE Check-Repair, or retire it read-only (the data is fully readable)
and continue the remaining **73 GB** of session 37 on a new cartridge.
