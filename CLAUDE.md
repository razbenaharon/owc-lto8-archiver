# CLAUDE.md

AI-assistant guidance for this repository lives in **[AGENTS.md](AGENTS.md)** — read it
first. It covers project structure, build/run commands, coding style, testing,
logging/reports, performance characteristics, and security/operations.

This file is intentionally thin so the guidance has a single source of truth.

## Prime directive — minimise physical intervention at the drive

> **The operator must physically travel to the machine to touch the drive or the
> cartridge. Avoiding that outranks throughput.** Standing requirement set
> 2026-07-25. Every design, config and recovery decision must prefer an outcome
> that can be resolved *remotely* over one that is faster but risks a state only a
> human at the drive can clear. A change that makes the pipeline a few percent
> faster while adding any chance of a wedged cartridge, a read-only mount, or a
> lost LTFS index **is a bad trade and must be rejected.**

Before proposing or doing anything on this system, ask:
**"if this fails at the worst possible moment, can it be recovered without
somebody standing at the drive?"** If no — redesign it or reject it.

Hard rules that follow (full list, with rationale:
[docs/incidents/000-no-physical-intervention-policy.md](docs/incidents/000-no-physical-intervention-policy.md)):

- **Never eject, remount, format, reset the drive, or run `ltfsck` unprompted.**
  All are last-resort and can strand a recoverable volume. Ask first, every time.
- **Keep the LTFS index durable at all times** — `sync_type=time@5`, never
  `unmount`, which is what turned a restart into ~126 GB of loss.
- **Stop only at chunk boundaries, never mid-write.** An interrupted write is the
  main route to a state needing physical recovery.
- **Refuse to *start* work likely to be interrupted** (the
  `_pre_tape_write_reboot_check` pattern) rather than aborting it midway.
- **Treat every hard write error as potentially latching.** One unrecoverable
  write set the cartridge's PWE bit and made Tape_02 permanently read-only
  ([incident 010](docs/incidents/010-20260724-ltfs-write-perm-readonly.md)).
  Stop and diagnose after the first one — never retry blindly into a failing drive.
- **Prefer a smaller blast radius over bigger batches.** Long single operations
  widen the window for a crash or drive fault to strand the volume. This is why
  the ~500 GB-per-transfer idea was rejected:
  [docs/tape_transfer_size_analysis.md](docs/tape_transfer_size_analysis.md).

**Incident history — read before touching a live run:
[docs/incidents/](docs/incidents/README.md)** (what broke, when, why, and what fixed it).

## Do not miss

> **During archive writes, avoid browsing the LTFS drive or starting separate copy
> jobs.** Internal tape access is serialized (`_acquire_tape_io_lock`), but external
> processes can still degrade tape throughput. This warning is also printed at the
> start of every tape-write run (`LTFS_WRITE_WARNING`, defined in `src/constants.py`).

### Operational best practices (learned the hard way — read before touching a live run)

- **A forced Windows Update restart mid-write destroys the LTFS index.** On
  2026-07-15 one interrupted session 37 and silently lost chunks 18-91 (~126 GB)
  from Tape_02 — the writes had been acknowledged and counted in
  `tape_used_after`, but the cartridge came back with only chunks 0-17 and the
  span had to be re-fetched. **This host cannot pause its way out of it**: it is
  domain-joined to `iem.technion.ac.il`, served by WSUS
  (`dds-wsus.iem.technion.ac.il`), and policy sets `SetDisablePauseUXAccess=1`
  (pause is removed) plus `SetComplianceDeadline` +
  `ConfigureDeadlineForQualityUpdates=2` — a 2-day deadline restart that
  overrides **both** ActiveHours and `NoAutoRebootWithLoggedOnUsers` (which was
  already `1` here and did not help). The pause registry writes still *succeed*,
  which is the trap.
  `src/windows_update_guard.py` therefore has two layers: `managed_update_policy()`
  detects the above and refuses to print a false "paused" line, and
  **`RebootSentinel` is the guard that actually works** — it polls for a staged
  restart during the run and sets the pipeline's `stop_pipeline` event, stopping
  at the next chunk boundary so LTFS syncs its index and the session stays
  resumable. It never kills the writer itself. Configure via `[WINDOWS_UPDATE]`
  in `config.ini`; needs Administrator for the pause layer only — the sentinel
  works unelevated. **The durable fix is organizational: ask IT to exempt this
  host from the update deadline policy.**
  > **CORRECTION (2026-07-17, evidence-based — read before acting on the bullet
  > above).** The 2026-07-15 restart was **not** a WSUS deadline restart. System
  > log event 1074 names the initiator: `CcmExec.exe` (SCCM) — *"Your computer
  > will restart at 15/07/2026 10:39:01 to complete the installation of
  > applications and software updates"*, i.e. **60 seconds** of warning against a
  > ~70 min chunk cycle. The WSUS/GPO settings above are real but were not the
  > trigger, so **"exempt this host from the update deadline policy" targets the
  > wrong system** — ask IT for an **SCCM maintenance window / deployment
  > exemption**. The sentinel alone cannot win a 60 s race either; the guard that
  > holds is `_pre_tape_write_reboot_check`, which refuses to *start* a write.
  > And the ~126 GB loss mechanism was `sync_type=unmount` (LTFS writes the index
  > only at unmount), **not** the restart itself: the last write ended 17 min
  > before shutdown, so `time@5` would have lost nothing. The current mount is
  > verified `time@5`. Evidence and specifics: **AGENTS.md → "Operating a live
  > run"**.
- **Never eject the tape remotely.** `LtfsCmdEject` is physical; a cartridge
  ejected with nobody at the drive cannot be reloaded remotely (no software
  "load" for a tape out of the slot). LTFS `sync_type` changes need a physical
  remount — stage them, never force-eject to apply them.
- **Never kill `Code.exe` to free RAM.** This assistant session runs *inside*
  VS Code; killing it terminates the session. Ask the operator to close spare
  windows instead.
- **Detect `robocopy` with `tasklist` (case-insensitive), not a WMI
  `Name='robocopy.exe'` filter** — it registers as `Robocopy.exe` (capital R)
  and the case-sensitive filter misses it, faking a "hung/gone" alarm.
- **Measure the pipeline with kernel perf counters only — never read the tape.**
  A read-only perf-counter sampler is safe; walking the LTFS drive or `du`-ing
  it is not (tape wear + shoe-shining). See `backup_logs/_tape_sampler.ps1`.
- **`config.ini [PERFORMANCE]` is host-calibrated** for the 15.6 GB box (psutil
  reports reclaimable file cache as "used"). Do NOT "restore defaults" — the
  tuned RAM thresholds + the 8.8 GB pagefile are the correct, crash-safe config.
- Before stopping a run, **verify no tape write is active** (`tasklist` for
  robocopy); sessions are resumable but interrupting a live tape write is not OK.
- **Never infer that `robocopy` returncode 0 means success.** It exits 0 even
  after `ERROR: RETRY LIMIT EXCEEDED` having copied nothing. Trust only the
  durable raw log + classifier in `backup_logs/tape_write/`
  ([incident 009](docs/incidents/009-20260724-robocopy-exit0-lie.md)).
- **The LTFS mount letter is not stable** (was `E:`, now `Z:`). Verify
  `config.ini [HARDWARE] lto_drive` with `Test-Path` before every run —
  `Get-Volume` cannot see LTFS. Verify `sync_type` from the *mount* (LTFS event
  61259), never from `ltfs.conf.local`; an MSI reinstall silently reset it once.
- Full detail, measurements, and future recommendations:
  **[docs/performance_insights_and_recommendations.md](docs/performance_insights_and_recommendations.md)**.

## Layout

Run the app with `python run.py` (CLI) or `python inspect_db.py` (GUI). The code
lives in the `src/` package; data files (`config.ini`, `.env`, `lto_archive.db`,
`backup_logs/`) stay in the project root.
