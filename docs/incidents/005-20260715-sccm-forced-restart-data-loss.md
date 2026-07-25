# 005 — Forced restart destroyed the LTFS index (~126 GB lost) **PHYSICAL**

- **When:** 2026-07-15
- **Physical intervention required:** **yes** (cartridge had to be re-handled;
  the span had to be re-fetched)
- **Status:** mitigated in code; the durable fix is organisational and **still open**

## What was lost

An interrupted session 37 silently lost **chunks 18–91 (~126 GB)** from Tape_02.
The writes had been acknowledged and counted in `tape_used_after`, but the
cartridge came back with only chunks 0–17. The whole span had to be re-fetched.

## Root cause — corrected 2026-07-17, read this carefully

The original diagnosis blamed the WSUS/GPO update deadline. **That was wrong**,
and acting on it would have targeted the wrong system.

**Trigger.** System log event 1074 names the initiator: **`CcmExec.exe` (SCCM)** —
*"Your computer will restart at 15/07/2026 10:39:01 to complete the installation
of applications and software updates"* — i.e. the Software Center notification,
**60 seconds** of warning against a ~70-minute chunk cycle. The WSUS/GPO deadline
settings on this host are real but were **not** the trigger.

**Loss mechanism.** The `~126 GB` did not vanish because of the restart as such.
The mount was running `sync_type=unmount`, and **LTFS writes its index only at
unmount**. The last write had ended **17 minutes before** shutdown, so under
`time@5` the restart would have lost *nothing*.

So this incident is really two independent defects: an uncontrollable 60-second
restart, and an index-durability setting that turned it into data loss.

## What was done

- **`_pre_tape_write_reboot_check`** — runs synchronously immediately before each
  write and **refuses to start one** when a restart is staged. This is the guard
  that actually holds; the loss becomes a deferred chunk instead of a killed write.
- **`RebootSentinel`** (`src/windows_update_guard.py`) polls during the run and
  sets `stop_pipeline`, stopping at the next chunk boundary. Useful, but it can
  never win a 60-second race on its own.
- **`managed_update_policy()`** detects the WSUS/`SetDisablePauseUXAccess=1`
  situation and refuses to print a false "paused" line — the pause registry writes
  *succeed* while doing nothing, which is the trap.
- **Mount switched to `time@5`**, verified from the mount itself (event 61259),
  bounding worst-case index loss to ~5 minutes.
- Query SCCM via `root\ccm\ClientSDK` → `CCM_ClientUtilities.DetermineIfRebootPending`.
  Treat *"could not ask"* as distinct from *"no restart"*
  (`sccm_reboot_status()['determinate']`). The `RebootData` registry key's mere
  **existence proves nothing** — verified 2026-07-17 present but empty while SCCM
  reported no pending restart. Read its *values* (`RebootBy`, `HardReboot`).

## Still open — the durable fix

Ask IT for an **SCCM maintenance window / deployment exemption** for this host.
Pausing Windows Update cannot influence an SCCM restart — different control plane.
