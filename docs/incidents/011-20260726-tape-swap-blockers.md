# 011 — Three defects surfaced by the first cartridge swap (Tape_02 → Tape_03)

- **When:** 2026-07-26, resuming session 37 after the Tape_02 freeze
  ([incident 010](010-20260724-ltfs-write-perm-readonly.md))
- **Physical intervention required:** no — the swap itself was physical, but every
  blocker below was diagnosed and fixed remotely
- **Status:** fixed. Session 37 was re-pointed to Tape_03, but no Session 37
  archive run completed there.

**Later evidence correction (2026-08-03):** all 9 Session 37 `archive_runs` name
Tape_02; zero `archive_runs` reference Tape_03; and `files_index` has zero rows
for Tape_03. Tape_03 was still written for a separate 24 GiB Phase 5E synthetic
pilot and was reformatted twice. The incident fixes and retargeting below are
real, but "re-pointed" is not evidence that Session 37 data landed there.

## Context

Tape_02 was frozen read-only by a permanent write error on 2026-07-24. The
operator loaded Tape_03 and asked to continue. Session 37 stood at chunks 0–48
`done`, 49 `backing`, ~73 GB remaining. Three separate defects had to be cleared
before a single byte could be written, and **none of them announced itself
honestly** — two presented as a healthy system.

## Defect 1 — `Test-Path Z:\` returned True against a dead mount

The drive dropped off the SAS bus at 13:45:17 and re-enumerated as a new PnP
instance:

```text
esas4hba  13:45:17  PHY 6 link down (Connector C)
esas4hba  13:45:23  PHY 6 link up in SAS mode (Connector C)
LTFS      13:45:19  "Device 2.0.0.0 has been removed while drive letter Z assigned"
```

`\\.\tape0` ceased to exist; the drive became `\\.\tape1`. But `LtfsMain`
(PID 6040, from the previous day) still held the dead handle and kept the `Z:`
mount point alive, so `Test-Path Z:\` — the check AGENTS.md prescribes before
every run — returned **True** while the device was unusable. The real state was
only visible in the LTFS log, repeating every 10 s:

```text
"12029","Error","2026/07/26 13:50:57" ... "Device is not ready (-21702)."
```

**Resolution:** restart the LTFS services and terminate the wedged `LtfsMain`;
the stack re-bound to `\\.\tape1` and mounted Tape_03 cleanly. Safe here because
the stale volume was read-only with its sync thread already dead, so there was
no dirty index to lose.

> **Lesson:** `Test-Path` proves a mount point exists, not that a drive is
> alive. Cross-check the LTFS log when a run has been idle across a drive event.

## Defect 2 — the mount-health window was anchored to the wrong process

`ltfs_current_mount_status()` anchored the drive-health evidence window to the
earliest running process matching `%ltfs%`
(`windows_update_guard.py`). That matched the GUI helper
`LtfsGuiCancelShutdown`, which survives service restarts — it had been running
since 2026-07-25 10:17. So the window opened **~28 h too early**, swept in
Tape_02's LOCATE faults from that morning, and refused to write to a brand-new,
provably clean cartridge:

```text
mount_started_at = 2026-07-25T10:17:39   ← the GUI helper, not the mount
degraded = 6      ← all of them Tape_02's, hours before the swap
```

The safety property was never at risk (the anchor is always *at or before* the
true mount, so the window can only be too wide, never too narrow) — but the
false block was absolute: no amount of waiting clears a fault that already
happened.

**Fix:** anchor on `LtfsMain`, the process that performs the mount and emits the
61259 declaration, falling back to the broad match for SDE builds that name it
differently. Regression tests: `LtfsMountAnchorTests`.

After the fix, scoped to the real mount: `fatal = 0, degraded = 0`.

## Defect 3 — a resumed session never checked which cartridge was loaded

The most dangerous of the three, because **nothing would have failed.**

`_run_streaming_session` takes `tape_label` from the session row
(`remote_orchestrator.py`). Session 37's row still said `Tape_02`. Nothing
compared that to the volume physically in the drive. The run would have written
chunks 49–95 to **Tape_03** while cataloging every one of them under
**Tape_02** — a silent split-brain in the catalog, pointing any future restore
at the wrong cartridge, and specifically at the one that is read-only and cannot
be corrected in place.

It was caught only because the capacity line looked wrong:

```text
[TAPE] 'Tape_02': DB occupied 4656.39 GiB; ... streaming available 5971.58 GiB.
```

on a cartridge that was empty.

**Fix:** `_verify_mounted_cartridge()` compares the mounted volume label to the
session's tape and fails **closed** — an unreadable or absent label blocks the
write, because "we cannot tell which cartridge this is" is not a state in which
to commit rows to a catalog keyed by cartridge. Wired in twice: at
resume-precheck (so a mismatch stops immediately instead of after a ~40 min
fetch+pack) and as step 4c of `_pre_write_safety_gate` (the authoritative
per-chunk gate). Regression tests: `MountedCartridgeGuardTests`.

Session 37's row was re-pointed to `Tape_03`. Per-bundle attribution is stored
on `archive_bundles.tape_label`, so chunks 0–48 correctly remain on Tape_02:

```text
Tape_01  295 bundles
Tape_02   73 bundles   ← unchanged by the re-point
```

No later Session 37 archive run completed on Tape_03. The session row names the
next target; completed Session 37 work remains on Tape_02.

## Also fixed — the reboot override only worked at the start gate

`block_on_pending_reboot = false` let the run *start* past a pending-restart
marker, but `RebootSentinel` polled the same markers and stopped the run 60 s
later — so the override could never produce a written chunk. The marker in
question was `PendingFileRenameOperations`, holding ten entries of Edge update
leftovers and Defender ATP temp files.

That marker is a **soft** signal: it lists file moves to apply *if* a restart
happens and never causes one. The hard markers (`RebootRequired`,
`RebootPending`) and SCCM's own intent were all clear. `pending_reboot_reasons`
and `reboot_block_reasons` now take `include_soft`, the sentinel and the
pre-write gate honour the operator's setting consistently, and the hard markers
stay unconditional. Regression tests: `SoftRebootMarkerTests` and
`test_include_soft_is_forwarded_so_the_override_actually_holds`.

## Operational finding — something read Tape_02 for nine hours

Between 04:13 and 13:19, with no `python.exe` or `robocopy` running, something
walked the LTFS volume and read Bundle zips:

```text
09:54:47  Read data fail (path:/LOCAL_Recordings_.../Bundle_s0011_c001_b001_002.zip) (-1201)
11:54:52  Read data fail (path:/LOCAL_Recordings_.../Bundle_s0011_c001_b001_003.zip) (-1201)
13:19:34  Read data fail (path:/LOCAL_Recordings_.../Bundle_s0011_c001_b001_003.zip) (-1201)
```

Each triggered a **49-minute** LOCATE timeout (`cmd 92h, difftime 2938.00`) —
ten in total — plus three forced drive dumps, on an already-damaged cartridge.
Defender had `DisableRemovableDriveScanning = False` and the Windows Search
indexer was running. `Z:\` was added to Defender's `ExclusionPath`.

> **Note:** `Set-MpPreference -DisableRemovableDriveScanning $true` **silently
> did not take** — it still reads `False`. Policy-managed, exactly like the
> Windows Update pause. The path exclusion is what actually holds. Verify the
> value after writing it; do not trust the call's success.

## Lesson

A cartridge swap is not a routine event on this system — it was the first one,
and it exercised three code paths that had never run before. Two of the three
defects presented as a *healthy* system (`Test-Path` True; a plausible capacity
line), and the third would have corrupted the catalog without any error at all.
Prefer checks that fail closed and that name the specific object they are
asserting about — the drive, the mount, the cartridge — rather than a proxy for
it.
