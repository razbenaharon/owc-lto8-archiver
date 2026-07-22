# Performance Insights & Future Recommendations — RAM, Tape, Fetch

> Self-contained context document for an LLM or engineer picking up performance
> work on this repo. It captures everything measured and reasoned about during a
> 2026-07-10 deep-dive on the 15.6 GB Windows host while archiving remote session
> 37 to LTO-8. Every number here was measured live, not estimated. Companion to
> `docs/pipeline_ram_context.md` (RAM deep-dive) and the memory files
> `archive-pipeline-ram-phantom-cache` and `tape-write-speed-ltfs-sync`.

---

## 0. One-paragraph summary

The remote-archive pipeline has **three independent performance limiters**, and
all three were diagnosed and (mostly) fixed here: (1) **RAM** — the governor was
gating on *phantom reclaimable file cache* that psutil reports as "used", and it
gated low-RAM drain stages the same as the real memory consumers, causing
deadlocks; (2) **tape write** — robocopy transfers at full LTO-8 speed
(100-320 MB/s) but the *effective* per-chunk rate collapsed to 8-46 MB/s because
IBM LTFS syncs its index every 5 min (`sync_type=time@5`) and each sync seeks
across a filling 3.6 TB tape; (3) **fetch** — a single SSH/tar stream over 100k
tiny files is per-file-latency bound at ~15 MB/s. The archiver process itself is
never the memory hog (RSS 44-585 MB throughout). The two decisive fixes are
`sync_type=unmount` (eliminates the tape overhead; needs a physical remount) and
**bigger chunks** (amortise the overhead: a 135 GB chunk wrote at **208.6 MB/s
effective**), plus parallel fetch (3 streams → ~30 MB/s).

---

## 1. Host hardware profile (measured)

| Component | Value | Implication |
|---|---|---|
| Physical RAM | 15.62 GB | Tight: runs hot PG + Docker/WSL + VS Code + Defender + pipeline together |
| Pagefile | `C:\pagefile.sys` 8.8 GB, auto-managed | Real OOM backstop; commit limit ~24 GB, ~4 GB free |
| CPU | i7-1165G7, **4 cores / 8 logical**, 2.8 GHz (mobile) | Modest; limits fetch parallelism to ~3 streams |
| Staging disk | **WD_BLACK SN750 2TB NVMe** (`C:\temp_for_disk\staging`) | ~3000 MB/s — never the bottleneck; concurrent tape-read + fetch-write is free |
| Tape | LTO-8 on `E:`, IBM LTFS, cartridge Tape_02 (~3.6 TB used of ~12 TB) | Append speed degrades as index-sync seeks grow with fullness |
| DB | PostgreSQL 17 in Docker Desktop (WSL2) | Shares host RAM; `vmmemWSL` was 4.6 GB → 1.8 GB after tuning |
| Env note | **This Claude/LLM session runs INSIDE VS Code** | Never kill `Code.exe` to free RAM — it kills the session |

---

## 2. Bottleneck A — RAM

### 2.1 Root causes
1. **Phantom file cache.** Local `tar -xf` extraction of a chunk into staging
   fills the Windows file cache as *active* pages. `psutil.virtual_memory()`
   counts that reclaimable cache as "used", so `available` reads ~1 GB and
   `percent` ~90-94 % within seconds of activity, while real committed memory is
   ~20/24 GB. **Proof:** governor blocked at `memory=93.8% available=0.97GB` with
   the archiver at 44 MB RSS; the instant packing did real work, Windows evicted
   the cache and it dropped to 75 % / 3.9 GB free on its own.
2. **Drain stages gated like consumers.** `pack`/`db_sync` are low-RAM (tens of
   MB) *drains* that read staged data and write a ZIP / stream a COPY. Blocking
   them on the global RAM ceiling (meant to throttle `fetch`'s 44 GB page cache
   and the tape writer) deadlocks the pipeline: the consumer filled RAM, and the
   drain that would free staging is refused.

### 2.2 Fixes applied (all committed / configured)
- **`ResourceGovernor._drain_stage_relaxed`** (src/resource_governor.py): after
  the soft-relax window, `pack`/`db_sync` proceed despite the ceiling if ≥512 MB
  is free. `fetch`/`tape` (real consumers) are never relaxed. +5 regression tests.
- **`_tape_blocks(action)`**: a *pending* tape write blocks only new stage
  *starts*; mid-stage *continue* checkpoints drain (fixed a separate
  producer/consumer deadlock).
- **Periodic `gc.collect()`** in the pack loop and after each chunk's fetch dir
  is freed (defensive; the archiver heap is small anyway).
- **Hot PostgreSQL sized down** (docker-compose.yml): `shared_buffers` 2GB→1GB,
  `mem_limit` 6g→4g, `maintenance_work_mem` 1GB→512MB.
- **`config.ini` [PERFORMANCE] host-calibrated** (gitignored — per host):
  `ram_soft_limit_pct=90 ram_hard_limit_pct=95 fetch_min_free_ram_gb=1
  governor_fetch_target_free_ram_gb=0.8 governor_fetch_min_free_floor_gb=0.5
  governor_tape_min_free_ram_gb=1.0`. **Rationale:** psutil percent/available
  measure reclaimable cache on this box, not crash risk; the 8.8 GB pagefile is
  the real OOM guard. Stock defaults (soft 70/hard 85, floors 4.0/2.5/3.0) are
  unreachable here and deadlock on phantom cache.
- **One-time ops:** `wsl --shutdown` freed ~2.8 GB (needs the operator's OK; it
  bounces the shared hot DB and is safe only when run.py is stopped). A one-off
  ~2.5 GB Python "cache-buster" allocation forces
  Windows to trim the file cache when the fetch gate is stuck on phantom cache.

### 2.3 Verified behaviour
- A **400k-file pack** (the aggressive chunk size) held RAM fine: peaked ~86-93 %
  (mostly reclaimable cache), recovered to 77 % / 3.5 GB free. RSS peaked ~585 MB.
  So 400k is tolerable on this box, but it is the aggressive end.
- **Rule of thumb:** a *tiny* `process_rss` (tens/hundreds of MB) next to a high
  `memory%` means the pressure is reclaimable cache, not the pipeline — let it
  proceed.

---

## 3. Bottleneck B — Tape write speed (the big one)

### 3.1 The measurement that cracked it
robocopy's own summary reports `Speed 104-322 MB/s` and `Robocopy Time 1-2 min`,
but the **total backup-step time was ~16-17 min per chunk regardless of size**.
The `[COPYING] robocopy active` monitor line climbed to ~15 min while robocopy's
internal transfer was only 1-2 min → robocopy was **blocked ~13-15 min in the
LTFS file close/index sync**, not transferring.

### 3.2 Root cause
IBM LTFS default `sync_type=time@5` (sync the index every 5 min). Each sync seeks
from EOD (now ~3.6 TB deep) to the index partition and back; these seeks grow as
the tape fills. robocopy's `close()` blocks on them. **Not the archiver** — fetch,
RAM, CPU, and the robocopy transfer are all healthy. Drive itself is healthy
(`LtfsCmdDrives` = LTFS_MEDIA, no error). `ltfs.conf.local` had no override, so
the documented default `time@5` was active. Physical object count on Tape_02 is
small (59 bundles + 524 loose files + manifests ≈ hundreds), so it is the *seek
distance on a filling tape*, not index size, that dominates.

### 3.3 Effective per-chunk tape speed — the data trail
```
date        size        total   effective   note
2026-07-06  100.01 GiB   9.7min  176.6 MB/s  tape emptier
2026-07-06    5.27 GiB   0.3min  268.4 MB/s
2026-07-09   80.65 GiB  22.3min   61.7 MB/s  degrading as tape fills
2026-07-10   43.45 GiB  16.1min   45.9 MB/s
2026-07-10    7.77 GiB  16.1min    8.2 MB/s  small chunk, overhead dominates
2026-07-10    9.29 GiB  17.0min    9.3 MB/s
2026-07-10    8.68 GiB   7.4min   19.9 MB/s  variance at time@5 (8-20 MB/s)
2026-07-10  135.82 GiB  11.1min  208.6 MB/s  ← 400k BIG CHUNK: overhead amortised
```
The 135 GB chunk wrote the **entire 135 GB in 11 min — less wall-time than a
single 9 GB small chunk (16 min)** — because the ~15 min overhead is paid once
and 135 GB then streams at full LTO speed.

> **CORRECTION (2026-07-22) — `sync_type=unmount` is NOT compatible with this
> pipeline's safety model. Do not re-enable it.** `unmount` is a supported,
> valid LTFS mode — it is not "broken" or universally dangerous. But it does not
> fit *this system's* recovery requirements: LTFS then writes its index **only at
> unmount**, and a clean pipeline stop does **not** necessarily unmount, while
> SCCM or Windows Update can force a reboot before that final unmount ever
> happens. Under `unmount`, such a restart loses every chunk written since the
> mount — this is exactly the ~126 GB Tape_02 loss of 2026-07-15 (see AGENTS.md /
> CLAUDE.md). Therefore the pipeline **requires the current mount to be `time@5`**
> before it will start a write, enforced in code by
> `RemoteOrchestrator._verify_current_mount_time5` /
> `windows_update_guard.ltfs_current_mount_status`, which read the mount's own
> event-log declaration and bind it to the running LTFS process. Note precisely
> what that check proves and does not: it verifies the **current mount** declared
> `time@5`; it does **not** assert that each per-chunk periodic index sync
> actually succeeded (there is no per-sync success event that we read or trust).
> The
> two staged edits below are kept only as a historical record of the 2026-07-10
> throughput investigation; treat item 1 as **superseded**.

### 3.4 Two fixes (item 1 SUPERSEDED — see the correction above)

1. **`sync_type=unmount`** *(superseded — do not use; see the 2026-07-22
   correction above)* — appended `option single-drive sync_type=unmount` to
   `C:\Program Files\IBM\LTFS\ltfs\ltfs.conf.local` (backup: `.bak_20260710`).
   The pipeline uses `_NoEjectBackup` for every chunk and ejects ONCE at session
   end, so `unmount` syncs the index only at that final eject → the per-chunk
   overhead vanishes entirely. **Takes effect only on the NEXT mount** (current
   mount stays time@5 until the tape is ejected + reloaded). The accepted
   trade-off — "a crash before the final eject leaves the session's index
   unwritten" — is precisely the incompatibility the correction describes: on
   this managed host the crash is a *forced update restart*, not a rare event.
   **Never eject remotely to force this** — eject is physical; a tape ejected
   with nobody present cannot be reloaded remotely.
2. **Bigger chunks** (workaround that works even under time@5):
   `chunk_max_files` 100k→400k, `chunk_cap_gb` 50→250. The *actual* limiter was
   file count (100k tiny files ≈ 8 GB), so raising it produced the 135 GB chunk
   that hit 208 MB/s.

---

## 4. Bottleneck C — Fetch (download from server)

### 4.1 Root cause
Single SSH/tar stream over 100k tiny files is **per-file-latency bound**:
`net_recv` avg 14.7 / max 25 MB/s, highly bursty (drops to 0.5 MB/s between
files); `ssh_cpu` avg 48 % / max 85 % (not consistently pegged → not local-CPU
bound); net never plateaus (not raw-bandwidth bound). The server stat/opens each
tiny file and tar framing stalls the single stream.

### 4.2 Fix — parallel fetch
New config `[PERFORMANCE] fetch_parallel_streams` (default 1 = legacy).
`RemoteOrchestrator._fetch_batches_parallel` runs N concurrent tar streams over
disjoint metadata-sized batches into the same fetch dir (safe: disjoint files),
sharing one `fetch_abort`; first non-cancel failure aborts the siblings and marks
those rows fetch_failed. Set to **3**; measured **~25-30 MB/s (2× the 15 baseline)**
with `tar=3` confirmed live. `use_mbuffer=true` also enabled to smooth the bursty
tar→ssh handoff (falls back if mbuffer absent on the server).

### 4.3 Ceiling
Peaks ~30 MB/s; the WAN to Technion / server small-file read is likely the real
ceiling near there. More streams may help marginally if the server allows; the
mobile 4C/8T CPU and the fetch-core split (0-5) comfortably run 3.

---

## 5. Measurement methodology (reusable, tape-safe)

- **Never read the LTFS tape (E:) for diagnostics** (AGENTS.md rule: no
  independent write verification, minimise tape reads). All measurement used
  **kernel perf counters only** — never touches the tape, never walks a disk.
- Sampler: `backup_logs/_tape_sampler.ps1` — every 10 s logs per-process I/O+CPU
  (robocopy / ssh / tar / python), NIC bytes/sec, and RAM to a CSV. One
  `Win32_PerfFormattedData_PerfProc_Process` + one NIC + one OS query per tick;
  negligible overhead, so it cannot itself cause tape shoe-shining.
- **Detect robocopy with `tasklist` (case-insensitive), NOT a WMI
  `Name='robocopy.exe'` filter** — the process registers as `Robocopy.exe`
  (capital R) and the case-sensitive WQL/`Get-Process` filter MISSES it (cost a
  false "robocopy is gone / hung" alarm during this session).
- Per-chunk truth comes from `backup_logs/SUMMARY.csv`
  (`total_time_seconds`, `copied_bytes`, `fetch_seconds`, `pack_seconds`,
  `db_sync_seconds`) — the authoritative effective-speed source.
- To free phantom cache without touching the tape: a bounded Python allocation
  (~2.5 GB, touch pages, then free) forces Windows to trim the file cache.

---

## 6. Config knobs reference (what to tune, and why)

All in `config.ini [PERFORMANCE]` unless noted. Values shown are the tuned
2026-07-10 state.

| Knob | Tuned | Effect / when to change |
|---|---|---|
| `chunk_max_files` | 400000 | The real chunk-size limiter (small files). Big → amortise time@5 tape overhead. **Drop to 150-200k after remount** (see §7). |
| `chunk_cap_gb` | 250 | GB ceiling per chunk; rarely binds vs file count. |
| `fetch_parallel_streams` | 3 | Concurrent SSH/tar streams. The fetch fix. |
| `use_mbuffer` | true | Smooths bursty small-file fetch. |
| `prefetch_chunks_ahead` | 1 | Chunks staged ahead of the tape writer. |
| `ram_soft_limit_pct` / `ram_hard_limit_pct` | 90 / 95 | Host-calibrated for phantom cache; do NOT lower on this box. |
| `governor_fetch_*` / `governor_tape_min_free_ram_gb` | 0.8 / 0.5 / 1.0 | Low floors because psutil available under-reports here. |
| `robocopy_priority` / `cpu_affinity` | high / auto (fetch 0-5, tape 6-7) | Already optimal; isolates tape cores. |
| LTFS `sync_type` (ltfs.conf.local) | **time@5 (required)** | Must stay `time@5` — the pipeline refuses to write otherwise. `unmount` is incompatible with this pipeline's crash/restart recovery (see the §3.4 correction). |

---

## 7. Future recommendations (ranked)

1. **Chunk sizing under the required `time@5` mount.** The 208 MB/s win from
   400 k chunks came from amortising the `time@5` per-chunk index-sync overhead,
   which is real and stays (the mount must be `time@5` — see the §3.4
   correction; `unmount` is not an option here). Bigger chunks trade that tape
   overhead against ~75-90 min fetch latency before the tape is fed, 270 GB+
   staging, ~585 MB pack RAM, and coarse resume granularity. Tune the file-count
   ceiling against those, not against a hypothetical `unmount` future.
2. **The steady-state bottleneck is fetch (~30 MB/s).** The pipeline is
   fetch-bound end to end. To go faster, invest in fetch: more parallel streams
   if the WAN/server allow, or a faster link. The durable protection against the
   forced-restart data-loss risk is organizational — an **SCCM maintenance-window
   / deployment exemption** for this host (see AGENTS.md) — plus the in-code
   `time@5` gate; it is *not* an LTFS `sync_type` change.
3. **Keep the RAM governor host-calibrated.** Do not "restore defaults" — on this
   box the psutil signal is dominated by reclaimable cache; the tuned thresholds
   + pagefile are the correct, crash-safe configuration. The `_drain_stage_relaxed`
   logic is essential and covered by tests.
4. **Long-term RAM headroom:** the box is ~2 GB short for hot-PG + full IDE +
   pipeline together. Options: more RAM, run archives with the IDE closed, or (if
   the archiver ran elevated) a Defender robocopy exclusion is already handled.
5. **Root-cause fetch cache pollution (optional):** the buffered `tar -xf`
   extraction is what fills the phantom cache. Write-through extraction or gating
   the governor on *commit* memory (not physical %) on Windows would remove the
   need for per-host RAM recalibration. Design note only; not implemented.
6. **On a fresh/empty tape** the time@5 overhead is small (2026-07-06 hit
   176-268 MB/s) — the degradation is specifically appending to a filling tape.
   A fresh tape is a temporary reset; there is no `sync_type` fix available here,
   because the mount must remain `time@5` for restart safety.

---

## 8. Quick decision tree for the next operator/LLM

- *Tape writes slow (single chunk 8-45 MB/s effective, robocopy Speed high)?* →
  LTFS time@5 index-sync on a filling tape. This is the accepted cost of the
  restart-safe `time@5` mount the pipeline requires; do **not** switch to
  `sync_type=unmount` (incompatible — see §3.4 correction). Mitigate with chunk
  sizing and faster fetch, not a `sync_type` change.
- *Pipeline stalled at "producer chunk N", governor loops `hard_ram_limit` with
  tiny process_rss?* → phantom cache. Check §2; the tuned thresholds should let
  it run; if truly stuck, cache-buster (§5) or free desktop RAM (NOT VS Code).
- *Fetch slow (~15 MB/s, bursty, 100k tiny files)?* → per-file latency; raise
  `fetch_parallel_streams`.
- *"robocopy is gone / hung"?* → verify with `tasklist` (case-insensitive), not a
  WMI `Name=` filter. Likely a false alarm.
- *Want to change LTFS sync or chunk size?* → sync needs a physical remount;
  chunk size needs a run restart (already-planned chunks keep their old size;
  only newly-scanned chunks use the new limit).
