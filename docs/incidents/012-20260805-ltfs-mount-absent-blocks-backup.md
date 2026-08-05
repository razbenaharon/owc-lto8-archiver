# 012 — 2026-08-05 — No LTFS volume mounted, and a duplicated `Z:` device symlink blocks database backups

**Status:** open. Needs physical attention at the drive.
**Impact:** Plan 3 Approvals A (apply migrations) and C (hardware pilot) are
both blocked. No data loss. Session 37 is untouched and resumable.

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
