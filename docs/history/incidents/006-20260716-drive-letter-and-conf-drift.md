# 006 — Drive letter moved and `ltfs.conf.local` was silently reset **PHYSICAL**

- **When:** 2026-07-16 (MSI reinstall 13:52); config corrected 2026-07-17
- **Physical intervention required:** **yes** (a remount is physical)
- **Status:** documented; no code fix — this is a pre-flight check

## Two separate drifts, same day

### 1. The LTFS mount letter is not stable

It was `E:` for months. After the 2026-07-16 tape restore it came back as **`Z:`**
and `E:` stopped existing entirely, while `config.ini [HARDWARE] lto_drive` still
said `E:\\`.

**Why it hurts:** a stale `lto_drive` fails the run at the drive-ready check —
*after* the operator has already committed to a multi-day resume.

**How to check, before every run:**

- `Test-Path Z:\` — this works.
- `Get-Volume -DriveLetter Z` does **not** see it. LTFS is a user-space
  filesystem with no `MSFT_Volume` object. Do not use it.
- A plain directory listing of the mount root is cheap — it is served from the
  in-memory LTFS index and does not move tape.

Docs and older notes that say `E:` are describing the letter of the day, not a
constant.

### 2. An MSI reinstall reset the sync type

The IBM Storage Archive SDE reinstall reset `ltfs.conf.local`, discarding the
staged `sync_type` line. **After the 2026-07-16 reinstall,
`sync_type=unmount` was not active** — do not cite it as the setting of that
post-reinstall mount. This does not contradict incident 005: `unmount` was active
on the mount interrupted on 2026-07-15 and amplified that restart into the
~126 GB index loss. The later mount is proven `time@5` from LTFS event **61259**
(`Sync type is "time", Sync time is 300 sec`).

**Rule: verify `sync_type` from the mount (event 61259 in
`C:\Program Files\IBM\LTFS\log\LogFile.csv`), never from the config file.** The
two drift silently, and only the mount is authoritative.

## Prevention

Pre-flight checklist before launching any resume:

1. `Test-Path` the configured `lto_drive`.
2. Confirm event 61259 in the current mount's log shows `time@5`.
3. Confirm the label is the expected cartridge (`Tape_02`).
