# 000 — Policy: minimise physical intervention at the drive

**Status:** standing requirement, set by the operator 2026-07-25.

## The requirement

The operator has to physically travel to the machine to touch the drive or the
cartridge. That trip is expensive and sometimes slow to arrange, and during it
the archive is stopped. Therefore:

> **Every design, config and recovery decision must prefer an outcome that can be
> resolved remotely over one that is faster but risks a state only a human at the
> drive can clear.**

This ranks *above* throughput. A change that makes the pipeline 5% faster but
adds any chance of a wedged cartridge, a read-only mount, or a lost LTFS index is
a bad trade and must be rejected.

## Rules that follow from it

1. **Never eject remotely.** `LtfsCmdEject` is physical and irreversible from
   here — there is no software "load" for a cartridge sitting out of the slot.
2. **Never force a remount to apply a setting.** Stage `sync_type` and similar
   changes for the next time someone is physically present.
3. **Keep the LTFS index durable at all times.** `sync_type=time@5` (not
   `unmount`) bounds worst-case loss to ~5 minutes. Incident
   [005](005-20260715-sccm-forced-restart-data-loss.md) is what `unmount` costs.
4. **Stop at chunk boundaries, never mid-write.** A write interrupted in flight
   is the main way this system reaches a state needing physical recovery.
5. **Refuse to start work that is likely to be interrupted.**
   `_pre_tape_write_reboot_check` is the model: check *before* committing to a
   write, not after.
6. **Prefer smaller blast radius over larger batches.** Long single operations
   widen the window in which a crash, restart or drive fault can strand the
   volume. See [docs/tape_transfer_size_analysis.md](../tape_transfer_size_analysis.md).
7. **Treat every write error as potentially latching.** A single failed write can
   flip an LTFS volume read-only for good (incident
   [010](010-20260724-ltfs-write-perm-readonly.md)); do not retry blindly into a
   failing drive, and stop to diagnose after the first hard write error.
8. **Never run `ltfsck`, format, or drive resets unprompted.** They are
   last-resort and can destroy a recoverable volume. Ask first, always.
9. **Escalate the environment, not just the code.** The SCCM restart policy on
   this host is an organisational risk that no amount of code fully removes —
   an SCCM maintenance-window exemption is the durable fix.

## Review question for any proposed change

*"If this fails at the worst possible moment, can it be recovered without
somebody standing at the drive?"* If the answer is no, redesign it or reject it.
