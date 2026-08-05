# Session 37 continuation runbook

**Purpose.** Take Session 37 from "stopped and ready" to a finished archive, in
finite bounded groups, without ever creating a state that needs somebody
standing at the drive.

Read [CLAUDE.md](../CLAUDE.md) and
[docs/incidents/](incidents/README.md) first. The prime directive outranks
throughput: *if this fails at the worst possible moment, can it be recovered
without somebody at the drive?* If no — stop.

---

## 0. The fixed architecture (do not redesign it)

| chunks | plan_source | packaging_format | status |
|---|---|---|---|
| 0–48 | `legacy_db` | `zip` | done, on Tape_02 — **never rewritten** |
| 49–112 | `legacy_db` | `stored_tar` | pending; membership and ordinals **unchanged** |
| 113+ | `manifest` | `stored_tar` | future only |

`plan_source` and `packaging_format` are independent axes. Chunks 49–112 keep
their PostgreSQL membership (so they stay `legacy_db`) but are written as
Stored TAR. Only chunks allocated after the persisted boundary are `manifest`.

**Chunks 0–48 are never touched.** No container is rewritten, no tape locator
changes, no membership or ordinal moves.

---

## 1. Preflight — every time, before anything

Run all of these and stop on the first failure.

```powershell
# 1.1 nothing is running
Get-Process | Where-Object { $_.ProcessName -imatch 'robocopy|python' }   # expect none
Get-ScheduledTask | Where-Object { $_.TaskName -like '*LTO*' } |
    Select-Object TaskName, State                                        # expect Disabled

# 1.2 no advisory lock, no application connection
python inspect_db.py --session-forensics --session 37

# 1.3 the LTFS mount is REAL, not just a drive letter
Get-CimInstance Win32_LogicalDisk -Filter "DeviceID='Z:'" |
    Select-Object DeviceID, FileSystem, Size, FreeSpace
```

> **The trap that has bitten this system before.** `Test-Path Z:\` returns
> **True** on a dead mount ([incident 011](incidents/011-20260726-tape-swap-blockers.md)).
> The mount is only real when `Win32_LogicalDisk` reports a non-empty
> `FileSystem` **and** a non-zero `Size`. Empty/zero means **no cartridge is
> loaded** — stop and get someone to the drive.

```powershell
# 1.4 the drive letter still matches config (it has moved before: E: -> Z:)
(Select-String -Path config.ini -Pattern '^lto_drive').Line

# 1.5 sync_type is time@5, read from the MOUNT (LTFS event 61259), never
#     from ltfs.conf.local -- an MSI reinstall silently reset it once
Get-WinEvent -LogName Application -MaxEvents 200 |
    Where-Object { $_.Id -eq 61259 } | Select-Object -First 1 TimeCreated, Message
```

```powershell
# 1.6 restart risk: refuse to START a write that a restart could interrupt
#     (the SCCM guard; a 60 s warning cannot be survived mid-chunk)
python inspect_db.py --reboot-sentinel-status
```

**Cartridge identity.** Session 37's `tape_label` is the **next** target, not
where completed work lives. All 49 done chunks and every `files_index` row are
on **Tape_02**, which is `status='full'` and read-only after its PWE bit
latched ([incident 010](incidents/010-20260724-ltfs-write-perm-readonly.md)).
Continuation writes go to the **currently active generation of Tape_03**.
`_verify_session_tape_generation` will block a resume whose generation does not
match — **do not bypass it.**

---

## 2. Hardware health gate

Two cartridges failed within seven days (Tape_02 PWE latch 2026-07-24;
Tape_03 Track Following Error / Servo 2026-07-31), and `mkltfs.exe` crashed
twice on 2026-08-02. Before any write group:

- confirm the drive has been cleaned and the cartridge mounts (§1.3);
- confirm no `Track Following` / servo error in the Application log since the
  last successful write;
- if a **second** cartridge fails after a clean, **escalate hardware** rather
  than loading a third.

**Never** eject, remount, format, reset the drive, or run `ltfsck` unprompted.
All are physical-recovery routes.

---

## 3. Starting one bounded group

A group is a **finite** list of chunk indexes. There is no "run until done"
mode and none may be added.

```powershell
python run.py            # menu -> remote archive -> resume session 37
```

Rules the code enforces, listed so a deviation is recognisable:

- `RemoteChunkWriter.write_chunk_group` is the **only** path a remote copy
  reaches tape. No LTFS access happens at startup, while waiting, between group
  members, or at completion.
- The remote pipeline **never ejects**, even with
  `[HARDWARE] eject_after_session = true`.
- `backing` has **no automatic retry**. An unreadable chunk status stops the
  run; it is never treated as clear.
- The first hard write error **stops the run**. Treat every hard write error as
  potentially latching — never retry into a failing drive.

**Stopping.** Only at a chunk boundary, and only when `robocopy` is not running:

```powershell
tasklist | findstr /i robocopy          # must be empty
python scripts\graceful_stop.py <pid>   # CTRL_C_EVENT, not taskkill /F
```

Pass the PID of the **real** interpreter (the several-hundred-MB child), not
the ~1 MB launcher shim.

---

## 4. After every group

```powershell
python inspect_db.py --session-forensics --session 37
python inspect_db.py --schema-provenance-audit
```

Verify, in this order:

1. every chunk in the group is `done` — none left `backing`, `fetching`,
   `packing`;
2. `archive_containers.writer_state='copied'` and
   `catalog_state='committed'` for each container written;
3. the terminal manifest for each chunk exists locally and every plan ordinal
   has exactly one disposition;
4. no chunk outside the group changed;
5. **chunks 0–48 are byte-identical in the catalog** — same containers, same
   tape locators, same membership.

Then stop, review the evidence, and **ask before the next group.** Broader
continuation is a separate decision every time.

---

## 5. What must never happen

- No `remote_snapshot_files` / `remote_plan_files` rows are created for a
  manifest chunk. That absence is the point of manifest-first planning.
- No container is ever rewritten; ZIP stays ZIP and TAR stays TAR.
- No membership or ordinal changes for chunks 49–112.
- No PostgreSQL pruning, no legacy row deletion, no physical compaction —
  that is Plan 4, which is **deferred**.
- No tape read for verification after a write. Trust the copy tool's report;
  reading after writing wears the media.
- No `robocopy` return-code-0 inference. It exits 0 after
  `ERROR: RETRY LIMIT EXCEEDED` having copied nothing
  ([incident 009](incidents/009-20260724-robocopy-exit0-lie.md)); trust only
  the durable raw log and classifier in `backup_logs/tape_write/`.

---

## 6. Recovery

| symptom | action |
|---|---|
| chunk stuck `backing` | **Do not clear it.** The on-tape outcome is unknowable from the catalog. Stop, preserve evidence, decide with a human. |
| hard write error | Stop. Do not retry. Check for a PWE latch before touching the cartridge again. |
| heartbeat flat ~25 min | Usually a **normal slow PACK**, not a hang. Confirm via FETCH/PACK/TAPE log lines before acting. |
| all threads 0 % CPU, no progress | Tape-stage deadlock. Force-killing `python` is safe (the LTFS driver is a separate process); resume afterwards. |
| transient SSH/DNS failure | Retries automatically. Genuine errors still fail fast. |
| restart staged mid-run | `RebootSentinel` stops at the next chunk boundary so LTFS syncs its index and the session stays resumable. |

A stop **preserves** staged packs: `_resume_pack.json` records the exact
inventory, and the next run reuses the pack only on exact inventory equality.

---

## 7. Rollback

Nothing here rewrites data, so rollback is always "stop allocating", never
"undo".

- **Stop future manifest chunks:** persist a new `legacy_db` transition epoch.
  Already-sealed manifest chunks keep their format and finish normally.
- **Disable Stored TAR for future writes:** set
  `stored_tar_write_enabled = false`. Existing TAR containers are unaffected.
- **Never** relabel an existing manifest chunk, move the original boundary, or
  rewrite a container.

---

## 8. Residual risk

No mandatory content hashes exist anywhere in Plans 1–3. A **same-size,
same-structure** replacement of a source file between scan and fetch is
undetectable by plan manifests, TAR sidecars, terminal manifests, the directory
catalog, or the semantic comparison. Hashing would require reading every byte
over SSH — the very fetch the pipeline exists to schedule. The risk is recorded
rather than papered over with a hash nobody can afford to compute.

Session 37's legacy ZIP routing is additionally **coarse**: all 134
`directory_archive_bundles` rows have `chunk_index` NULL and no
`archive_containers` row, so per-member routing cannot be proven. Coarse
evidence can never become authoritative, drive row-free restore, or qualify
anything for pruning.
