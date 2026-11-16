# Tape safety rules

These rules apply to everyone who operates the program and to every change
made to it. They exist because a tape drive is a physical device: some states
can only be cleared by a person standing at the drive, and a few mistakes
make a cartridge permanently read-only.

> **This version has not been validated on a real tape drive and cartridge.**
> All tape-path behaviour was verified with simulated devices and temporary
> directories only. The rules below describe what the code is designed to do;
> prove it on your own hardware with a small pilot before you trust it with
> data you care about ([testing-and-validation.md](testing-and-validation.md#hardware-pilot)).

## The one question

Before any design, configuration or recovery decision, ask:

> If this fails at the worst possible moment, can it be recovered without
> somebody standing at the drive?

If the answer is no, redesign it or do not do it. This ranks above
throughput: a change that is a few percent faster but adds any chance of a
wedged cartridge, a read-only mount or a lost LTFS index is a bad trade.

## Rules for operating

1. **Never eject unless somebody is at the drive.** `LtfsCmdEject` is a
   physical action. There is no software "load" for a cartridge that is out
   of the slot. The remote pipeline never ejects; the local path ejects only
   with `[HARDWARE] eject_after_session = true` and a typed `EJECT`.
2. **Never eject, remount, format, reset the drive or run a filesystem check
   (`ltfsck`, `LtfsCmdCheck`) unprompted.** All are last-resort and each can
   strand a volume that was still recoverable. They need an explicit decision
   by the operator for that specific case, after a diagnosis. A script, a
   supervisor or an AI assistant must ask first, every time, and must never
   force a remount just to apply a setting.
3. **Keep the LTFS index durable.** Mount with `sync_type=time@5` (index
   written every 5 minutes). Never use `sync_type=unmount`: the index is then
   written only at unmount, and a forced restart loses every file written
   since the mount. The program refuses to start a tape write on any other
   mode. Read the mode from the mount (LTFS event 61259 in the Windows
   Application log), not from `ltfs.conf.local`; a reinstall of the LTFS
   software can reset that file.
4. **Stop only at a chunk boundary, never in the middle of a write.** Before
   stopping a run, check that no tape write is active:

   ```powershell
   tasklist | findstr /i robocopy
   ```

   No output means no copy is running. Use `tasklist`: a WMI filter on
   `Name='robocopy.exe'` is case-sensitive
   and misses the process, which registers as `Robocopy.exe`.
   See [user-guide.md](user-guide.md#stopping-a-run-safely).
5. **Do not start work that is likely to be interrupted.** The program checks
   for a pending Windows or SCCM restart immediately before each tape write
   and refuses to start one. Do not override that to "get one more chunk in".
   This check is not a guarantee. A centrally managed PC (SCCM, Intune,
   WSUS) can force a restart with as little as 60 seconds of warning, which
   no check can outrun once a write has started, and pausing Windows Update
   does not control such a restart. The durable fix is organisational: a
   maintenance window or an exemption for the archive host.
6. **Treat every hard write error as possibly permanent.** One unrecoverable
   write can set the cartridge's permanent-write-error flag and make the
   volume read-only for good. After the first hard write error: stop, keep
   the staged data and the logs, and diagnose. Never retry into a failing
   drive.
7. **Never read the tape to check a write.** Do not browse the LTFS drive, run
   `dir`, `du`, an antivirus scan or a second copy job on it during or right
   after a write. Reads between writes make the drive seek back and forth
   and wear the tape. Measure with process and disk performance counters
   instead.
8. **Never trust `robocopy` exit code 0.** It exits 0 even after
   `ERROR: RETRY LIMIT EXCEEDED` with nothing copied. Trust only the
   classified result the program records in `backup_logs/tape_write/`.
9. **Prefer a small blast radius.** Long single operations widen the window
   in which a crash or drive fault can strand the volume. Keep
   `[PERFORMANCE] chunk_cap_gb` moderate.
10. **The drive letter is not stable.** Check `[HARDWARE] lto_drive` against
    the real mount before every run. `Test-Path` returning true does not
    prove the mount is alive, and `Get-Volume` cannot see an LTFS volume.
11. **A chunk in state `backing` is never cleared automatically.** Its
    outcome on tape cannot be known from the catalog. A person must compare
    the cartridge with the catalog before it is written again.
12. **Do not reset a working installation's tuning to defaults.** The
    `[PERFORMANCE]` memory and staging limits, and the Windows pagefile size,
    are calibrated to the host. "Restoring defaults" on a machine that runs
    reliably can bring back stalls or out-of-memory crashes in the middle of
    a run.

## What the program enforces

| Rule | Where it is enforced |
| --- | --- |
| Source data never streams straight to tape; the writer's only input is validated local staging | `src/remote_writer.py`, `src/ready_queue.py` |
| No LTFS access outside a finite write group (not at start-up, not while waiting, not between groups) | `src/remote_writer.py`, `src/remote_orchestrator.py` |
| Only one process may touch LTFS, keyed on the physical drive, never on the drive letter; no configured identity means no tape access | `src/ltfs_ownership.py` (`[HARDWARE] ltfs_ownership_id`) |
| Only one archive run may write at a time; the lock is re-verified before every tape write | archiver lock, [database-backends.md](database-backends.md#single-writer-lock) |
| Pre-write gates: LTFS sync mode is `time@5`, drive and medium health from the IBM log, no pending restart, mounted cartridge is the expected one. A gate that cannot be evaluated blocks | `src/remote_orchestrator.py`, `src/local_orchestrator.py` |
| A chunk is marked `backing` before the first touch of the tape, and may only move to `done` | `src/pipeline_types.py`, database trigger (migration 020) |
| The first hard write error stops the run; there is no automatic retry | `src/remote_writer.py` |
| Ctrl+C does not kill a tape copy in flight; the run stops at the next chunk boundary | `src/robocopy.py`, `scripts/graceful_stop.py` |
| Eject and filesystem repair need the words `EJECT` / `REPAIR` typed in capitals, on an interactive console | `src/ltfs.py` |
| A supervisor may restart a stopped run only after a self-clearing stop | `src/exit_codes.py`, `scripts/archive_watchdog.ps1` |
| The test suite cannot reach a real LTFS mount | `tests/conftest.py` |

## Rules for changes

A pull request must not:

- add a code path that reads from tape to verify a write;
- add tape access outside the finite write group in `src/remote_writer.py`,
  or any automatic eject, remount, format or filesystem check;
- add an automatic retry after a hard tape error, or an automatic way out of
  the `backing` state;
- weaken a pre-write gate, the archiver lock, the LTFS ownership mutex or the
  chunk-transition guard, or make any of them fail open;
- infer success from a `robocopy` return code;
- weaken `tests/conftest.py` or `tests/pg_test_guard.py`.

After a change to the tape path, do not go straight back to real data:

```text
code change -> full offline tests -> PostgreSQL tests on a disposable server
-> small synthetic pilot on a scratch cartridge -> one bounded write group
-> review -> normal operation
```

## Where these rules come from

The rules were written after real incidents at the installation this project
was built for. The dated records are kept in
[history/incidents/](history/incidents/README.md); the standing policy they
led to is
[history/incidents/000-no-physical-intervention-policy.md](history/incidents/000-no-physical-intervention-policy.md).
The three most worth reading:
[a forced restart under `sync_type=unmount`](history/incidents/005-20260715-sccm-forced-restart-data-loss.md),
[`robocopy` exiting 0 on a failed copy](history/incidents/009-20260724-robocopy-exit0-lie.md)
and [a write error that made a cartridge permanently read-only](history/incidents/010-20260724-ltfs-write-perm-readonly.md).
