# Pipeline RAM Context — the 15.6 GB Windows host

> Living reference for how the remote/local archive pipeline behaves under
> memory pressure on this workstation, what actually goes wrong, and how to get
> unstuck. Started 2026-07-10 during a stalled resume of remote session 36.

---

## 1. TL;DR

The pipeline **does not run out of RAM** in the dangerous sense. It **stalls on
*phantom* memory pressure**: buffered `tar` extraction of a chunk into the
staging disk fills the Windows **file cache**, `psutil` counts that
*reclaimable* cache as "used", and the Resource Governor — which gates on
physical-memory percent — refuses to proceed even though real committed memory
is healthy and the pagefile backstops any true pressure. The instant a stage
does real work, Windows evicts the cache and the numbers fall on their own.

**Proof, observed live:** governor blocked at `memory=93.8%`, `available=0.97 GB`
with the archiver process using only 44 MB RSS; the moment packing started,
`percent` dropped to **75.1% / 3.9 GB free** with no other change.

---

## 2. Host profile

| Property | Value |
|---|---|
| Physical RAM | 15.62 GB |
| Pagefile | `C:\pagefile.sys`, 8.8 GB allocated (auto-managed) |
| Commit limit | ~24.2 GB (RAM + pagefile) |
| Typical commit in use | ~20 GB (≈4 GB commit headroom) |
| DB backend | PostgreSQL 17 in Docker Desktop (WSL2 backend) |
| WSL memory cap | 5 GB (`%USERPROFILE%\.wslconfig`, `autoMemoryReclaim=gradual`) |
| Staging disk | `C:\temp_for_disk\staging` (chunks fetched/packed here) |
| Desktop load | VS Code (hosts this Claude Code session — **do not kill**), Defender/MsMpEng, MsSense, Docker Desktop |

The box runs the hot catalog DB **and** a full IDE **and** the fetch/pack/tape
pipeline simultaneously. It is ~2 GB short of comfortable for that combination;
everything below is about making the governor measure *real* pressure instead
of reclaimable cache, and freeing the slack that is freeable.

---

## 3. Root causes (two, independent)

### 3.1 Phantom file cache fools `psutil` (host/OS level)

- A chunk is fetched with a local `tar -xf -` that extracts through **buffered**
  I/O. Writing ~44 GB into staging fills the Windows **file cache** with
  *active* (not standby) pages.
- `psutil.virtual_memory().available` does **not** count that active file cache
  as available, so within seconds of any activity `available` reads ~1 GB and
  `percent` ~90–94%.
- Real **committed** memory is ~20 / 24 GB (healthy). The cache is fully
  reclaimable: when a process actually asks for RAM, Windows evicts it
  instantly. So the 90–94% is **not** crash risk — it is the wrong signal.
- Net effect: the governor's physical-percent gates trip on cache that would
  vanish the moment it mattered, and the pipeline deadlocks.

### 3.2 Drain stages gated like consumers (code level)

- The RAM ceiling exists to throttle the **consumers**: `fetch` (fills page
  cache with a whole chunk) and the `tape` writer.
- `pack` and `db_sync` are **drains**: they read staged data off disk and write
  a ZIP / stream a COPY. Their own footprint is tens of MB (`process_rss` was
  44–175 MB throughout).
- Blocking a drain on global RAM% can **never lower** RAM (the drain is not the
  consumer) and **stalls the pipeline**: the consumer filled memory, and the
  drain that would free the 44 GB of staging is refused. Permanent deadlock,
  visible as `[GOVERNOR] pack start: ... reason=hard_ram_limit,ram_soft_limit`
  looping every 30 s while `_pack_*` stays at 0 files.

---

## 4. Symptom → what you actually see

- Telegram heartbeats repeat `producer chunk 1/streaming | staging=0/700 GB` for
  a long time (observed >1 h).
- Log loops `[GOVERNOR] pack start: available≈1 GB memory≈90-94%
  reason=hard_ram_limit,ram_soft_limit`.
- On disk: `_fetch_s<sess>_<chunk>` holds the full chunk (e.g. 44 GB, 13 607
  files); `_pack_s<sess>_<chunk>` is empty.
- `process_rss` in the governor lines is tiny (tens of MB) — the archiver is not
  the memory consumer, which is the giveaway that pressure is external/cache.

---

## 5. Fix stack applied (2026-07-10)

### 5.1 Code (committed, durable)

| Commit | Change |
|---|---|
| `de7eb84` | `ResourceGovernor._drain_stage_relaxed`: after the soft-relax window, let **pack/db_sync** proceed despite the RAM ceiling **iff** ≥512 MB is still free. `fetch`/`tape` (real consumers) are **never** relaxed. +5 regression tests. |
| `772b0d8` | Periodic `gc.collect()` at each pack batch checkpoint and after a chunk's fetch dir is freed — keeps the Python heap from drifting between checkpoints. |
| `679d7a7` | Hot PostgreSQL sized for this host: `shared_buffers` 2 GB→1 GB, `mem_limit` 6g→4g, `maintenance_work_mem` 1 GB→512 MB (`docker-compose.yml`). |

### 5.2 `config.ini` (host-local, gitignored — not in the repo)

Recalibrated the governor so it gates on **genuine** pressure rather than
reclaimable cache. These are safe here because fetch streams to disk
(reclaimable), pack spools only 64 MB and is drain-relaxed, and any real
overflow pages to the 8.8 GB pagefile (slow, never fatal):

```ini
ram_soft_limit_pct    = 90
ram_hard_limit_pct    = 95
fetch_min_free_ram_gb = 1
governor_fetch_target_free_ram_gb = 0.8
governor_fetch_min_free_floor_gb  = 0.5
governor_tape_min_free_ram_gb     = 1.0
```

Stock defaults (soft 70 / hard 85, fetch 4.0/2.5, tape 3.0) are *unreachable*
on this box and deadlock the pipeline on phantom cache. Keep these host values
in `config.ini`; they are intentionally **not** committed (per-host).

### 5.3 One-time ops (to recover from a stalled pipeline)

1. **Stop the stuck `run.py`** (resumable — nothing is committed to tape until
   the write succeeds; the fetched chunk on disk is same-size-skipped on resume).
2. **`wsl --shutdown`** — resets the WSL memory balloon. Freed ~2.8 GB here
   (`vmmemWSL` 4.6 GB→1.8 GB). **Needs explicit user approval** (auto-mode blocks
   it: it bounces the shared hot DB). Safe when `run.py` is stopped. Recreate
   and verify the database afterwards: `docker compose up
   -d db`, then confirm `docker port lto_pg` = `127.0.0.1:5432` and
   `SELECT current_database()` = `lto_archive`.
3. **Kill leaked `pytest` processes** — repeated backgrounded test runs on this
   box can leak python processes that hang at exit (thread teardown) and quietly
   eat RAM. `Get-CimInstance Win32_Process -Filter "Name='python.exe'" | ? {
   $_.CommandLine -like '*pytest*' } | % { Stop-Process $_.ProcessId -Force }`.
4. **Cache-buster (last resort, pagefile required):** a one-off ~2.5 GB Python
   allocation that touches its pages forces Windows to trim the file cache;
   `available` jumps and the fetch gate opens. Do it immediately before
   launching `run.py` so it grabs the window:
   ```python
   import gc
   chunks=[]; step=256*1024*1024
   try:
       while sum(len(c) for c in chunks) < int(2.5*1024**3):
           b=bytearray(step)
           for i in range(0,len(b),4096): b[i]=1
           chunks.append(b)
   except MemoryError: pass
   del chunks; gc.collect()
   ```
   Note: the recalibrated `config.ini` usually makes this unnecessary.

**Do NOT** force-kill `Code.exe` to free RAM — this Claude Code session runs
*inside* VS Code; killing it terminates the session. Ask the user to close
spare VS Code windows themselves if desktop RAM must come down.

---

## 6. What "healthy" looks like once moving

After the fixes, resume got past the fetch gate (the 44 GB chunk was
same-size-skipped in seconds), and **packing ran at 75–79% RAM / 3.2–3.9 GB
available** — Windows evicted the phantom cache under the real workload exactly
as predicted. `vmmemWSL` settled at ~1.8 GB with `shared_buffers=1GB`.

Rule of thumb: if the governor line shows a *tiny* `process_rss` (tens of MB)
next to a high `memory%`, the pressure is reclaimable cache, not the pipeline —
the recalibrated thresholds are correct to let it proceed.

---

## 7. Open items / things still to watch (update as discovered)

- **Buffered fetch extraction is the cache source.** A cleaner root fix would be
  to extract with less cache pollution (write-through / `FILE_FLAG_NO_BUFFERING`
  where practical) or to gate the governor on **commit headroom** instead of
  physical-percent on Windows. Not yet done — the recalibration + drain-relax
  are the current mitigation.
- **Governor signal choice.** Consider teaching the governor to treat reclaimable
  Windows file cache as available (e.g. read standby/cache counters) so stock
  thresholds work without per-host recalibration. Design note only.
- **Throughput vs. paging.** With hard=95 the box may page during a very large
  chunk (slow, not a crash). If throughput matters more than desktop use, run
  archives with spare apps closed, or add RAM.
- **(2026-07-10) Non-RAM blocker discovered downstream:** after chunk 1 packed
  cleanly, the tape write was refused because the **directory-catalog schema
  (`007`) is not installed** on this DB (`directory_archive_bundles` absent).
  That is a separate migration decision (see
  `docs/directory_catalog_migration_runbook.md`), unrelated to memory — noted
  here only so the RAM fix is not mistaken for the reason the run later stopped.

---

## 8. Cross-references

- `docs/local_small_file_manifest_runbook.md` — permanent local manifest
  export, validation, prune, and legacy database retirement.
- `src/resource_governor.py` — `_drain_stage_relaxed`, `_tape_blocks`, the
  decision logic and thresholds.
- `docker-compose.yml` — hot database container memory sizing.
- `config.ini` `[PERFORMANCE]` — the host-local governor thresholds (not in git).
