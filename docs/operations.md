# Operations runbook

How to operate, stop, and recover a live archive run without ever creating a
state that needs somebody standing at the drive. The prime directive
([CLAUDE.md](../CLAUDE.md), [incident 000](incidents/000-no-physical-intervention-policy.md))
outranks throughput: *if this fails at the worst possible moment, can it be
recovered without somebody at the drive?* If no — stop.

Companion runbooks this document links instead of restating:

- [drive_cleaning_and_itdt_runbook.md](drive_cleaning_and_itdt_runbook.md) —
  drive cleaning and ITDT diagnostics (read-only ITDT use only).
- [local_small_file_manifest_runbook.md](local_small_file_manifest_runbook.md)
  — small-file manifest export/prune.
- [server_deletions.md](server_deletions.md) — reclaiming server space after
  verified archival.

## Standing rules (non-negotiable)

- **Never eject the tape remotely.** `LtfsCmdEject` is physical; a cartridge
  ejected with nobody at the drive cannot be reloaded remotely. Changes that
  need a remount (e.g. LTFS `sync_type`) are *staged* and applied when someone
  is physically present.
- **Stop only at chunk boundaries, never mid-write.** Confirm no tape write is
  active first: `tasklist | findstr /i robocopy` (case-insensitive — a WMI
  `Name='robocopy.exe'` filter misses `Robocopy.exe`).
- **`robocopy` exit 0 is not success.** It exits 0 even after
  `ERROR: RETRY LIMIT EXCEEDED` having copied nothing
  ([incident 009](incidents/009-20260724-robocopy-exit0-lie.md)). Trust only
  the durable raw log + classifier in `backup_logs/tape_write/`.
- **Verify `sync_type` from the mount (LTFS event 61259), never from
  `ltfs.conf.local`.** The required mode is `time@5`; the pipeline refuses to
  write under anything else. `sync_type=unmount` is forbidden — it is how a
  forced restart lost ~126 GB ([incident 005](incidents/005-20260715-sccm-forced-restart-data-loss.md)).
- **Stopping a detached run: `python scripts\graceful_stop.py <pid>`, never
  `taskkill /F`.** Pass the PID of the real interpreter (the
  several-hundred-MB child), not the ~1 MB launcher shim; the helper raises
  `CTRL_C_EVENT` so the writer finishes its chunk and the session stays
  resumable. Only if the process is wedged in a native tape call is
  force-killing Python acceptable (the LTFS driver is a separate process).
- **Measure with kernel perf counters only** (`backup_logs/_tape_sampler.ps1`)
  — never read the tape or walk the LTFS drive for diagnostics.
- **No independent write verification.** Never read from the tape after a
  write just to verify; rely on the copy tool's classified report.
- **Treat every hard write error as potentially latching** — one unrecoverable
  write set a cartridge's PWE bit permanently
  ([incident 010](incidents/010-20260724-ltfs-write-perm-readonly.md)). Stop
  and diagnose after the first one; never retry into a failing drive.
- **A stop preserves staged packs.** `_resume_pack.json` records the exact
  inventory; the next run reuses the pack only on exact inventory equality.
- **`config.ini [PERFORMANCE]` is host-calibrated** — do not "restore
  defaults"; psutil counts reclaimable file cache as "used".
- Chunk-size / config changes apply to newly-scanned chunks only and need a
  `run.py` restart; LTFS `sync_type` needs a physical remount.

## Preflight — every time, before anything

Run all of these and stop on the first failure.

```powershell
# 1. nothing is running; scheduled tasks disabled
Get-Process | Where-Object { $_.ProcessName -imatch 'robocopy|python' }   # expect none
Get-ScheduledTask | Where-Object { $_.TaskName -like '*LTO*' } |
    Select-Object TaskName, State                                        # expect Disabled

# 2. no advisory lock, no application connection
python inspect_db.py --session-forensics --session <N>

# 3. the LTFS mount is REAL, not just a drive letter (one stat call, no tape walk)
python -c "import ctypes; f=ctypes.c_ulonglong(); t=ctypes.c_ulonglong(); tf=ctypes.c_ulonglong(); ok=ctypes.windll.kernel32.GetDiskFreeSpaceExW('Z:\\', ctypes.byref(f), ctypes.byref(t), ctypes.byref(tf)); print('ok', ok, 'total', t.value, 'free', f.value)"
Get-Content "C:\Program Files\IBM\LTFS\log\LogFile.csv" |
    Select-String -Pattern '"(11031|17227)"' | Select-Object -Last 12

# 4. drive letter still matches config (it has moved before: E: -> Z:)
(Select-String -Path config.ini -Pattern '^lto_drive').Line

# 5. sync_type is time@5, read from the MOUNT (event 61259)
Get-WinEvent -LogName Application -MaxEvents 200 |
    Where-Object { $_.Id -eq 61259 } | Select-Object -First 1 TimeCreated, Message

# 6. restart risk: refuse to START a write a restart could interrupt (SCCM guard)
python inspect_db.py --reboot-sentinel-status
```

Mount-verification traps, both seen on this host
([incident 011](incidents/011-20260726-tape-swap-blockers.md),
[incident 012](incidents/012-20260805-ltfs-mount-absent-blocks-backup.md)):

- `Test-Path Z:\` returns **True** on a dead mount.
- `Win32_LogicalDisk` (WMI) reports **empty** on a perfectly mounted LTFS
  volume. The mount is real when `GetDiskFreeSpaceEx` succeeds with a non-zero
  total **and** the LTFS log shows `11031 Volume mounted successfully` with
  the intended `Medium Label`. Two *identical* DOS device symlinks for `Z:`
  are normal; only genuinely different targets are ambiguous.
- **Cartridge identity:** a session's `tape_label` is the **next** target, not
  where completed work lives. `_verify_session_tape_generation` blocks a
  resume whose generation does not match — do not bypass it. After a swap,
  `UPDATE remote_sessions SET tape_label=...`; already-written chunks keep
  their attribution per-bundle.

## Hardware health gate

Before any write group: confirm the drive has been cleaned and the cartridge
mounts; confirm no Track Following / servo error in the Application log since
the last successful write; if a **second** cartridge fails after a clean,
**escalate hardware** rather than loading a third. Never eject, remount,
format, reset the drive, or run `ltfsck` unprompted — all are physical-recovery
routes. Cleaning and ITDT procedure:
[drive_cleaning_and_itdt_runbook.md](drive_cleaning_and_itdt_runbook.md).

## Running one bounded group

A group is a **finite** list of chunk indexes. There is no "run until done"
mode and none may be added. Start with `python run.py` (menu → remote archive →
resume session). Rules the code enforces, listed so a deviation is
recognisable:

- `RemoteChunkWriter.write_chunk_group` is the only path a remote copy reaches
  tape; no LTFS access at startup, while waiting, between group members, or at
  completion.
- The remote pipeline never ejects.
- `backing` has no automatic retry; an unreadable chunk status stops the run.
- The first hard write error stops the run.

During writes, avoid browsing the LTFS drive or starting separate copy jobs —
external processes degrade tape throughput (printed as `LTFS_WRITE_WARNING` at
every tape-write start).

## After every group

```powershell
python inspect_db.py --session-forensics --session <N>
python inspect_db.py --schema-provenance-audit
```

Verify in order: every chunk in the group is `done`; each written container is
`writer_state='copied'` + `catalog_state='committed'`; each chunk's terminal
manifest exists locally with exactly one disposition per plan ordinal; no
chunk outside the group changed; closed chunks are byte-identical in the
catalog. Then stop, review, and **ask before the next group**.

## What must never happen

- No container is ever rewritten; ZIP stays ZIP and TAR stays TAR.
- No membership or ordinal changes for already-planned chunks.
- No PostgreSQL pruning, legacy row deletion, or physical compaction outside
  Plan 4 ([archive-modernization-plans/04_LEGACY_EXPORT_AND_POSTGRESQL_PRUNING.md](archive-modernization-plans/04_LEGACY_EXPORT_AND_POSTGRESQL_PRUNING.md)).
- No tape read for verification after a write.
- No `robocopy` return-code-0 inference.
- No clearing of a `backing` chunk — its on-tape outcome is unknowable from
  the catalog.

## Recovery quick table

| symptom | action |
|---|---|
| chunk stuck `backing` | **Do not clear it.** Stop, preserve evidence, decide with a human. |
| hard write error | Stop. Do not retry. Check for a PWE latch before touching the cartridge again. |
| heartbeat flat ~25 min | Usually a normal slow PACK, not a hang. Confirm via FETCH/PACK/TAPE log lines before acting. |
| all threads 0 % CPU, no progress | Tape-stage deadlock. Force-killing `python` is safe (LTFS driver is separate); resume afterwards. |
| transient SSH/DNS failure | Retries automatically with backoff; genuine errors still fail fast. |
| restart staged mid-run | `RebootSentinel` stops at the next chunk boundary; the session stays resumable. |
| stale `active` session row | `inspect_db.py --reconcile-stale-sessions` proves staleness; ambiguous cases are left alone. |

## After code changes

Do not resume production immediately. Progression:

```text
code change -> full offline tests -> isolated PostgreSQL tests
-> small synthetic hardware pilot -> one bounded production group
-> review -> broader resume
```

Offline suite and the disposable-PostgreSQL recipe (one canonical copy, port
15432): [testing-and-validation.md](testing-and-validation.md). Never point
tests at the production database.

## Current blockers

Current tape, drive, session, and campaign state — including why no tape
operation is currently permitted — lives in
**[tape-and-archive-state.md](tape-and-archive-state.md)** (snapshot
2026-08-20). The governing open incidents are
[013](incidents/013-20260809-tape03-servo-drive-failure.md) (drive servo
fault, RMA), [014](incidents/014-20260819-campaign-write-servo-halt.md)
(campaign writes halted, drive fault confirmed), and
[015](incidents/015-20260820-campaign-drive-instability.md) (campaign store
drive unstable — evacuate first).
