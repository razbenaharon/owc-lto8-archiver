# 013 — 2026-08-09/10: Servo write failure on Tape_03, drive cannot reload — PHYSICAL

**Status: CLOSED as a drive-hardware verdict; drive replacement/RMA required.**

## What happened

During a normal stored-TAR write to Tape_03 (generation 3), the LTO-8 drive
raised **-20301 Track Following Error (Servo)** — a Permanent Write Error —
and LTFS dropped the volume to read-only (event 62173 → 12045,
2026-08-09 18:26). After unload, the drive could no longer thread any
cartridge: LOAD returned **Sense Key 0x04 HARDWARE ERROR, ASC/ASCQ 0x40/0x80**
(diagnostic failure on component).

## Root cause

Drive mechanism failure, not media. The same servo fault class recurred on
this one drive across multiple cartridges within weeks (Tape_02 PWE latch in
[incident 010](010-20260724-ltfs-write-perm-readonly.md), Tape_03 servo on
2026-07-31 and again 2026-08-09, and later two more cartridges in
[incident 014](014-20260819-campaign-write-servo-halt.md)). One drive ×
many cartridges × identical fault ⇒ drive.

## What was done

- No write/format/erase/media-test was run after the fault (policy 000).
- Read-only ITDT diagnostics plus a drive memory dump were collected into a
  vendor support package, and drive replacement was requested.
- The full diagnostic package (ITDT logs, drive dumps, LTFS logs, drive
  identity) is **operational evidence and is kept off-repo** on the local
  diagnostics store (`LTO_DIAG/tape03_20260810_122227/` on the campaign
  drive), indexed by its own `README_OWC.md`.
- The cartridge remained physically in the drive, unthreaded; its data is
  recoverable on a healthy drive.

## What prevents a repeat

- The escalation rule from the continuation runbook stands: after a second
  cartridge fails post-cleaning, escalate hardware — never load a third.
- No production tape writes are attempted until a replacement drive passes a
  synthetic pilot.
