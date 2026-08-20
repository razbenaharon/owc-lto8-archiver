# 012 — 2026-08-05 — No LTFS volume mounted, and a duplicated `Z:` device symlink blocks database backups

**Status:** **RESOLVED 2026-08-05 17:55** by a full host reboot. No data loss.
Session 37 untouched. **Two of this document's original conclusions were wrong**
— see "Resolution and corrections" at the end before acting on anything above it.
**Impact while open:** Plan 3 Approvals A (apply migrations) and C (hardware
pilot) were blocked.

## What was observed

Two independent symptoms, one cause.

### 1. `Z:` is a drive letter with no volume behind it

```powershell
Get-CimInstance Win32_LogicalDisk -Filter "DeviceID='Z:'"
# DriveType  : 2        (removable)
# FileSystem : <empty>
# Size       : <empty>
# FreeSpace  : <empty>
```

`Test-Path Z:\` still returns **True**. That is the
[incident 011](011-20260726-tape-swap-blockers.md) trap: a live-looking drive
letter over a mount that is not there. `Get-PSDrive` reports 0 used / 0 free.

There are **no LTFS mount events (61259)** anywhere in the recent Application
log, and no cartridge is loaded.

### 2. `Z:` has a duplicated device symlink, which stops every backup

```
QueryDosDeviceW("Z:") -> 2 mappings:
      \Device\UfsIoDev_3B2EFE49LTFS
      \Device\UfsIoDev_3B2EFE49LTFS      <- the SAME device, listed twice
QueryDosDeviceW("C:") -> 1 mapping:
      \Device\HarddiskVolume3
```

`src/pg_backup.py::_windows_device_target` refuses any mapping count other than
one:

```
RuntimeError: [DB BACKUP] A drive has multiple mappings;
              local-path safety is indeterminate
```

So `inspect_db.py --backup-postgres` **fails outright**, because the guard
resolves the configured `lto_drive` in order to prove the backup directory is
not on it.

## Why the guard is right

The check exists so a PostgreSQL dump can never be written onto the LTFS drive.
It refuses to *guess* what a drive letter points at. Two identical targets are
arguably unambiguous, but the guard is a safety device and this is a symptom of
a sick mount, not a false alarm to code around. **Do not relax it to unblock a
backup.** Fix the mount.

## Probable cause

The drive letter churn on 2026-08-02: Tape_03 was reset twice
(`tape_generations` generation 2 retired 09:56, generation 3 active 12:07) and
`mkltfs.exe` **crashed twice** the same day (Application Error, 12:53 and
15:07). A crashed format/mount cycle plausibly left a duplicate DOS-device
symlink and no mounted volume.

Background: two cartridges failed within seven days — Tape_02's PWE bit latched
2026-07-24 ([incident 010](010-20260724-ltfs-write-perm-readonly.md)) and
Tape_03 froze on a Track Following (Servo) error 2026-07-31. The drive device
itself currently reports `Status: OK`.

## What is NOT affected

- Production PostgreSQL is intact and quiescent: Session 37 `done=49`,
  `pending=64`, 29,085,495 plan and snapshot rows, 3,336,421 `files_index` rows.
- The last production mutation was **2026-08-02 09:56**, so the existing
  `..._20260803_151701.dump` post-dates it and is a faithful image of the
  current catalog.
- Scheduled tasks remain **Disabled**; no worker, Robocopy, advisory lock or
  tape I/O is active.

## Recovery (needs someone at the drive)

1. Load a cartridge and mount it through the LTFS manager.
2. Confirm the mount is **real**, not just a letter:
   ```powershell
   Get-CimInstance Win32_LogicalDisk -Filter "DeviceID='Z:'" |
       Select-Object FileSystem, Size, FreeSpace     # all must be non-empty
   ```
3. Confirm the duplicate symlink is gone:
   ```powershell
   python -c "import ctypes; b=ctypes.create_unicode_buffer(32768); n=ctypes.windll.kernel32.QueryDosDeviceW('Z:',b,len(b)); print([t for t in b[:n].split(chr(0)) if t])"
   ```
   Expect exactly **one** target. If two remain after a clean remount, the
   stale symlink must be cleared before any backup or write.
4. Verify `sync_type=time@5` from the **mount** (LTFS event 61259), never from
   `ltfs.conf.local` — an MSI reinstall silently reset it once.
5. Re-check `config.ini [HARDWARE] lto_drive`; the letter has moved before
   (`E:` → `Z:`).
6. Only then retry `python inspect_db.py --backup-postgres`.

**Do not** eject, format, reset the drive or run `ltfsck` to "clear" this.
Those are physical-recovery routes and this is not yet a media fault.

## Hardware escalation

If a cartridge fails again after a clean, **escalate the drive** rather than
loading a third. Two dead cartridges in seven days plus two `mkltfs` crashes is
a drive-level pattern, not bad luck.

---

## Resolution and corrections (2026-08-05, evidence-based)

A **full host reboot at 17:54:34** fixed it. At 17:55:57 the LTFS log recorded:

```text
Tape attribute: Medium Label = TAPE_03.
Tape attribute: Volume Lock Status = 0x00.
11031 Volume mounted successfully. AAAAAA : Gen = 1 / (a,5) -> (b,5) / (drive identity: see private records).
```

`GetDiskFreeSpaceEx('Z:\')` then returned 11,711,538,003,968 total /
11,711,529,615,360 free (11.712 TB, 8 MiB of index) and event 61259 confirmed
`Sync type is "time", Sync time is 300 sec`. The drive is healthy; no cartridge
was harmed and nothing was formatted, ejected, or `ltfsck`ed.

### Correction 1 — `Win32_LogicalDisk` cannot see an LTFS volume

Symptom 1 above ("`Z:` is a drive letter with no volume behind it") was a
**misdiagnosis**. With the cartridge mounted and fully working, WMI *still*
reports empty `FileSystem`, `Size`, `FreeSpace`, `VolumeName` and
`VolumeSerialNumber`, and `Get-PSDrive` still reports 0 used / blank free.
Emptiness there proves nothing.

This is the exact mirror of the [incident 011](011-20260726-tape-swap-blockers.md)
trap: there, `Test-Path` said *alive* about a dead mount; here, WMI said *dead*
about a live one. Use `GetDiskFreeSpaceEx` plus LTFS event `11031`, never WMI.
The Session 37 continuation runbook (retired; its operative content now lives
in [docs/operations.md](../operations.md)) was corrected — it previously
instructed the operator to stop on exactly this false negative.

### Correction 2 — the duplicated symlink is normal, and did not block the mount

Symptom 2 was real but its cause and severity were both wrong. A full reboot
clears every DOS device symlink, yet `Z:` came back with **two identical
mappings again**, under a new device id each time:

```text
3B2EFE49  ->  39613629  ->  4EEB10F8      (three boots, always duplicated)
```

The cartridge mounted successfully **with the duplicate present**. So the
"probable cause" above — a crashed `mkltfs` leaving a stale symlink — is
falsified: the IBM LTFS driver publishes its device symlink twice as a matter
of course, and this never had anything to do with the mount failure.

Consequently `src/pg_backup.py::_windows_device_target` was rejecting a
**normal** condition, permanently, on every run. Its `len(targets) != 1` test
now collapses identical targets before counting: repeated copies of one target
resolve to exactly one device and leave nothing to guess, while genuinely
*different* targets remain indeterminate and are still refused. The guard's
purpose — a dump can never land on the LTFS drive — is unchanged.
Covered by `tests/test_pg_backup.py::WindowsDeviceTargetTests`.

This supersedes the "Why the guard is right" section above, which told the
reader not to relax the check. That instruction was correct *given* the belief
that the duplicate signalled a sick mount; that belief is now disproven.

### What remains true

The hardware-escalation note still stands. Before the reboot, a mount attempt
at 17:02 failed with `readpos: Initializing Command Required (-20203)`,
`rewind: Diagnostic Failure (-20400)`, four forced drive dumps in 42 seconds,
and `Device 2.0.0.0 has been removed while drive letter Z assigned`. Those
dumps are preserved in `C:\Program Files\IBM\LTFS\log\` and are the evidence to
hand to support if the pattern repeats.
