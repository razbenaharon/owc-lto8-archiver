# 010 — LTFS latched Tape_02 read-only after a WRITE-PERM error **OPEN**

- **When:** first failure 2026-07-24 ~07:00; confirmed still read-only
  2026-07-25 16:36 (session 37, chunk 49)
- **Physical intervention required:** **yes so far** — and the physical attempt
  on 2026-07-25 **did not fix it**, because the cause is not the WP switch
- **Status:** **OPEN.** Diagnosed; recovery not yet applied. Production is stopped.

## Symptom

Every write to `Z:` fails with:

```text
ERROR 19 (0x00000013) Changing File Attributes Z:\_pack_s0037_049\
The media is write protected.
```

Robocopy exhausts its retries and still exits **0** (see incident
[009](009-20260724-robocopy-exit0-lie.md)).

## The decisive test

A **bare native** `System.IO.Directory.CreateDirectory` — no Robocopy, no
attributes, a brand-new unique path unrelated to chunk 49 — fails identically:

```text
Target : Z:\_ltfs_write_test_20260725_163609
Result : System.IO.IOException
HRESULT: 0x80070013  (Win32 = 19, ERROR_WRITE_PROTECT)
Message: The media is write protected.
```

That rules out every chunk-49-specific and Robocopy-specific hypothesis. It also
rules out a directory-attribute anomaly: `Z:\_pack_s0037_049` and the known-good
`Z:\_pack_s0037_048` carry **identical** attributes (plain `Directory`, no
ReadOnly/Hidden/System).

## Root cause

From `C:\Program Files\IBM\LTFS\log\`:

**1. The drive stopped responding first** (`ScsiLib.log`) — five consecutive
timeouts on SCSI command `34h` (READ POSITION), ~58 s each against a 60 s limit:

```text
SCSI0019E 'Fri Jul 24 07:03:05 2026' TIMEOUT on cmd 34h, difftime 58.00 (sec)
… 07:04:03, 07:05:01, 07:05:59, 07:06:57
```

**2. LTFS then failed a write and latched the volume read-only** (`LogFile.csv`):

```text
"12045","Error","2026/07/24 07:08:18.709",…,"Z",
  "Cannot write block: backend call failed (-20301). Dropping to read-only mode."
```

**3. The state is persistent on the cartridge.** After the physical handling and
a fresh mount on 2026-07-25 10:14, LTFS reported:

```text
11333  A cartridge with write-perm error is detected on DP. Seek the newest index
       (IP: Gen = 179, VCR = 1232) (DP: Gen = 178, VCR = 1232)
17287  Making R/O mount from the location (a, 21).
17228  Tape attribute: Volume Lock Status = 0x04
61223  Medium is write protected. Mounting medium as read-only.
11030  Failed to sync volume (-1126). Stop the periodic sync thread.
```

**`Volume Lock Status = 0x04` is the permanent-write-error (PWE) bit in the
cartridge's MAM chip.** LTFS sets it after an unrecoverable write and then mounts
that cartridge read-only **on every subsequent mount**. It is stored on the
cartridge itself, which is precisely why reseating it, power-cycling, or moving
the physical write-protect switch **cannot** clear it.

## This was NOT sudden — the drive degraded for 4 days first

Aggregating `LogFile.csv` by day shows a clear escalating trend that nobody was
watching:

| Date | Event | Count |
| ------ | ------- | ------- |
| 07-20 19:41 → 07-23 11:52 | `Error on locate: Recorded Entity Not Found (-20301)` | **45** |
| same | `Locate command returns write-perm error (-20301). Replace a return code to -1201.` | **45** |
| 07-24 07:07:41 | `Error on write: Track Following Error (Servo) (-20301)` | 1 → fatal |

Per-day locate failures rose **4 → 13 → 18** across 07-20/21/22. For three days
the drive repeatedly failed to find recorded data where it expected it, LTFS
**masked each failure** (`Replace a return code to -1201`) and carried on, and
then the servo lost track following during a write and the cartridge was frozen.

`Recorded Entity Not Found` on LOCATE plus `Track Following Error (Servo)` is a
**servo/positioning** fault — either damaged servo bands on the medium or a
dirty/failing head-positioning system in the drive. It is a degradation trend,
not a one-off event.

> **Note:** this is exactly the LOCATE-command write-perm case IBM warns about,
> where the reported position may not indicate the true failure location. The
> associated 0-byte truncation defect affects SDE **2.4.0.0–2.4.3.0**; this host
> runs **2.4.8.1**, so that defect does not apply — but the hardware symptom is
> the same one.

### The real operational gap

These 45 errors sat in the LTFS log for four days and **nothing was monitoring
it**. An alert on the first `17267` / `62173` event on 07-20 would very likely
have saved the cartridge, since the fatal write was still three days away. The
archiver watches Windows Update, RAM, fetch stalls and robocopy — but not the
one log that predicted this failure.

**Fixed 2026-07-25.** `ltfs_media_health()` in `src/windows_update_guard.py`
now reads the LTFS trace and classifies drive/medium faults, and
`_verify_ltfs_media_health()` gates every tape write (step 4b of
`_pre_write_safety_gate`, beside the mount-mode check). It fails **closed**: an
unreadable log, or a mount whose start time cannot be determined, blocks the
write. Evidence is scoped to the current mount so a previous cartridge's faults
cannot block a good one.

> **Trap worth knowing:** the obvious source — the Windows **`LTFS` event log** —
> is the wrong one. Verified 2026-07-25: it contains **no `17267` events at all**
> (the single most valuable early warning) and had already rotated away
> everything before 07-23, while `LogFile.csv` still held history back to 07-16.
> A guard built on the event log would have missed this very failure. Read the
> CSV.

Verified against the real log: scoped to the current mount it reports the
fatal `11333`; scoped to 07-20 it flags **97 events, the earliest at
2026-07-20 19:41:34** — four days before the freeze. Benign `62173` mode-sense
chatter is not flagged. Regression tests in
`tests/test_windows_update_guard.py::LtfsMediaHealthTests`.

## Drive or medium? Leading hypothesis: the drive

Not yet proven, but the timing is the strongest evidence available without
touching hardware: the LOCATE failures recurred **once per chunk cycle for three
days**, matching the drive-dump timestamps exactly. A localised media defect
would not re-trigger every cycle as the append point advanced into fresh tape —
a systematic per-cycle failure across advancing positions points at the
head/servo mechanism rather than one bad spot on the cartridge.

`Track Following Error (Servo)` is also most commonly caused by debris on the
head, which is why **a cleaning cartridge is the cheapest first move** before any
new media is committed. If the drive is at fault, a fresh cartridge will simply
acquire the same PWE.

ITDT (`IBM Tape Diagnostic Tool`) is installed on this host but only as the
**Graphical Edition** — `itdt-ge.exe` / `itdt-gec.exe`, no command-line binary —
so it cannot be driven headlessly, and pointing it at the drive would take the
device while a frozen cartridge is still mounted. Run it interactively at the
drive, together with the cleaning cartridge, to read TapeAlert flags and settle
drive-vs-media conclusively.

**Step-by-step procedure:
[docs/drive_cleaning_and_itdt_runbook.md](../drive_cleaning_and_itdt_runbook.md).**
Two things in it are easy to get wrong: run ITDT **before** cleaning (cleaning
clears the TapeAlert flag that answers drive-vs-media), and never run ITDT's
write/format tests against this cartridge.

## Data integrity — verified good

No data was lost. A single root metadata listing shows all **50** `_pack_s0037_*`
directories present (0–49, none missing), plus the older `s0034`/`s0036` sets.
Chunk 48 still holds its 6 files / 1,731,200,224 bytes. Chunk 49's directory
exists but is **empty** — consistent with the earlier reconciliation (0 files,
0 bytes on tape) and with the write having failed before any data landed.

The 0-byte-truncation defect IBM documents for Spectrum Archive SDE
**2.4.0.0–2.4.3.0** does not apply here: this host runs **2.4.8.1**
(tape attribute `Application Version`), past the affected range.

The preserved local pack is intact and reusable — 6 files,
**1,731,382,179 bytes**, matching the expected accounting exactly, plus the
local-only `_resume_pack.json` marker.

## Current state

| Item | State |
| ------ | ------- |
| `remote_chunks` session 37 | 0–48 `done`, **49 `backing`**, 50 `fetching` (stale), 51–95 `pending` |
| Tape_02 | mounted `Z:`, **read-only**, ~3.6 TB used of ~10.7 TB |
| Preserved pack | intact at `C:\temp_for_disk\staging\_pack_s0037_049` |
| Production run | **stopped** — not resumed |

## Recovery options (not yet applied — needs a decision)

1. **Software repair first (remote-friendly).** `ltfsck` / the SDE Check-Repair
   function can roll the volume back to a consistent index and may clear the PWE
   state. This is the only option that does not need somebody at the drive, so
   under [policy 000](000-no-physical-intervention-policy.md) it is tried first —
   but it is a repair operation and must be explicitly authorised.
2. **Retire Tape_02 read-only and continue on a new cartridge.** The data is
   fully readable; only writing is blocked. Requires a physical cartridge swap.
3. **Check the drive before trusting any cartridge.** The READ POSITION timeouts
   point at the *drive*, not only the medium. If the drive is at fault, a fresh
   cartridge will acquire the same PWE. A cleaning cartridge plus a review of the
   drive dumps (`ltfs_2026_0724_070741_*.dmp`) should precede committing new media.

## Outcome (2026-07-26)

Option 2 was initiated: Tape_02 was retired read-only and Session 37's next
target was changed to Tape_03. No Session 37 archive work was ultimately written
there: all 9 Session 37 `archive_runs` name Tape_02, zero `archive_runs` reference
Tape_03, and `files_index` has zero Tape_03 rows. Tape_03 did receive the separate
24 GiB Phase 5E synthetic pilot and was reformatted twice. The Tape_02 retirement
is recorded in the catalog rather than kept in the operator's head —
`tapes.status = 'full'` with
`status_reason = 'read-only: PWE bit latched 2026-07-24 (incident 010); retired
2026-07-26'` (schema: `scripts/sql/011_postgres_tape_status.sql`).

`status` exists because the byte counters cannot express this. Tape_02 holds
~5.0 TB of 12 TB, and `recalculate_tape_used_space` rewrites `used_space` from
the catalog on every mount, so a hand-edited counter would be silently undone at
the next run. With the flag set, `tape_budget_bytes` reports zero available
bytes, both orchestrators refuse to start or continue a write on the tape, and
the CLI/GUI show `FULL` instead of `5000/12288 GB` — which would otherwise
invite a write that can never land. Clear it from Database Management → option 7
if a cartridge is ever genuinely returned to service.

## Lesson

A single unrecoverable write can permanently flip a cartridge to read-only. That
makes "stop and diagnose after the first hard write error" a safety rule, not a
nicety — and it is a strong argument against very large single tape writes, which
widen the window in which such an error can strike. See
[docs/tape_transfer_size_analysis.md](../tape_transfer_size_analysis.md).
