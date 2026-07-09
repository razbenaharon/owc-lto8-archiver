# Cold Small-File Migration — Host Tuning & Operations

Companion to [`cold_small_file_migration_runbook.md`](cold_small_file_migration_runbook.md).
That runbook lists the command sequence; **this document captures the host-level
tuning and the operational gotchas** discovered running the migration on the
production Windows/WSL2 workstation, so the next run does not rediscover them.

> Scope: a 15.6 GB-RAM Windows 11 host running PostgreSQL in Docker Desktop
> (WSL2 backend). Two containers share one WSL VM: `lto_pg` (hot, `lto_archive`)
> and `lto_cold_manifest_pg` (cold, `lto_cold_manifest`).

---

## 1. Why the host needs tuning

The tape-archive pipeline runs on **Windows** (SSH fetch → ZIP pack → robocopy)
and its `ResourceGovernor` requires free RAM headroom:

- `fetch_min_free_ram_gb = 4`
- `governor_tape_min_free_ram_gb = 3`

PostgreSQL runs inside the **WSL2 VM**. A heavy job (the cold migration's 7.7M-row
snapshot INSERT + streamed COPY) grows the VM's Linux **page cache by ~4 GB**.
WSL2 holds that allocation and does **not** promptly return it to Windows, so
`vmmemWSL` balloons (observed 2.7 GB → 6.8 GB), free Windows RAM collapses
(~5 GB → ~1.3 GB), and the cold governor aborts the copy mid-run. On a 15.6 GB
host this makes the migration effectively impossible **until the VM is capped**.

Measured committed footprint of PostgreSQL is only **~2.7 GB** (hot
`shared_buffers = 2 GB` + backends); everything above that is reclaimable cache.

---

## 2. Permanent fix — cap the WSL VM (`%USERPROFILE%\.wslconfig`)

```ini
# WSL2 / Docker Desktop memory tuning for the LTO archiver host (15.6 GB RAM).
[wsl2]
memory=5GB
swap=12GB
autoMemoryReclaim=gradual
```

**Why 5 GB is optimal (not just enough for the migration, but right for tape runs):**

| Consumer (during a tape run) | Reserve |
| --- | --- |
| Windows OS + Defender + Docker Desktop UI | ~4.0 GB |
| Archiver process (fetch / pack / robocopy) | ~2.0 GB |
| Governor free-RAM target during fetch | ~4.0 GB |
| **→ leaves for WSL/PostgreSQL** | **~5.5 GB** |

PostgreSQL needs ~2.7 GB committed + ~2 GB useful cache, so **5 GB** keeps
`shared_buffers` and a healthy cache **and** guarantees ~10.5 GB stays on Windows
for the archiver and its 4 GB-free target. The hard cap also means PostgreSQL can
**never** balloon and starve a tape write. `swap=12 GB` (on the vhdx) is the
safety net for rare heavy maintenance; it is untouched during normal tape runs.

**Apply / change the cap** (requires a VM restart, which bounces both DB
containers — they are `restart: unless-stopped`, so they return automatically):

```powershell
wsl --shutdown
# Docker Desktop restarts the VM and both containers within ~1-2 min.
```

**Effect confirmed:** `vmmemWSL` 6.8 GB → ~2 GB; free Windows RAM stable ~6 GB;
migration then runs to completion under normal governor thresholds.

---

## 3. Gotcha — Docker port cross-wiring after a simultaneous restart

After `wsl --shutdown`, Docker Desktop **may swap the published port mappings**
when both containers start at once:

- `127.0.0.1:5432` → **cold** DB (should be hot)
- `127.0.0.1:55432` → **hot** DB (should be cold)

Symptom: every app connection fails auth (`password authentication failed`),
because the hot user reaches the cold DB and vice-versa. Confusingly,
`docker port` still reports the **intended** mapping.

**Fix — restart the containers sequentially, then verify:**

```powershell
docker restart lto_pg
docker restart lto_cold_manifest_pg
```

```python
# verify each maps to the expected database
psycopg.connect(host="127.0.0.1", port=5432,  dbname="lto_archive",       user="lto",      password=...)      # -> lto_archive
psycopg.connect(host="127.0.0.1", port=55432, dbname="lto_cold_manifest", user="lto_cold", password=...)      # -> lto_cold_manifest
```

Avoid restarting both containers simultaneously; the race is what shuffles the ports.

---

## 4. Cold DB credential

`cold_pg_password` resolves from `COLD_PGPASSWORD` (process env → `.env` →
`config.ini`). If it is unset, cold TCP auth fails. Ensure `.env` contains:

```ini
COLD_PGPASSWORD=<the cold container's POSTGRES_PASSWORD>
```

(The docker-compose default for local dev is `change_me_cold_local`.) `.env` is
gitignored and must never be committed.

---

## 5. Cold-migration governor settings (`config.ini [COLD_MANIFEST_DB]`)

The defaults are unusable on this host and were tuned to work under the 5 GB cap:

| Key | Old (unusable here) | Tuned (durable) | Reason |
| --- | --- | --- | --- |
| `min_free_ram_gb` | 16 | **2** | 16 GB free is impossible on a 15.6 GB host; the 5 GB WSL cap keeps free RAM ≥ ~2.6 GB during a run |
| `max_ram_pct` | 60 | **85** | matches the pipeline's RAM hard limit |
| `max_local_disk_io_mbs` | 200 | **5000** | the migration's own COPY + WAL bursts exceed 200 MB/s and would trip the guard against itself; cold ops are already gated off tape writes by the archiver lock + activity flags |

These are safe because a cold migration only ever runs when no tape write / fetch /
pack / DB-sync / cleanup is active (enforced by `can_start_cold_migration()` and
the archiver advisory lock).

---

## 6. Deletion prerequisite — why hot rows may be 0-removable

Hot-row removal (`--hot-remove-migrated-small-files`) deletes **only** rows with
`covered_by_hot_accounting = TRUE` — i.e. rows whose tape data is *also* accounted
for in **`directory_archive_bundles`**. This protects the invariant that
`files_index` remains the tape used-space source of truth unless another table
accounts for the same bytes. The cold DB is **never** used for tape accounting.

If `directory_archive_bundles` does not exist (or does not match), **every row is
uncovered → 0 removable**, and the migration is purely a search/documentation copy
in the cold DB; `files_index` is left intact.

**To actually free hot space you must first build the directory catalog:**

```powershell
python inspect_db.py --apply-directory-catalog-schema
python inspect_db.py --backfill-directory-catalog --dry-run     # review
python inspect_db.py --backfill-directory-catalog --execute
```

…then run a **fresh** cold migration whose snapshot computes coverage against the
now-populated `directory_archive_bundles`, validate it, and only then run the
removal dry-run / execute.

---

## 7. Recovery — mid-copy abort behavior

If the copy aborts mid-run (e.g. a governor guard trips), `execute_migration`'s
error handler rolls back the **entire hot snapshot** (`small_file_cold_migrations`
and `small_file_cold_migration_sources` return to 0 rows — no failed record
survives on the hot side), while the **cold** DB keeps the partial
`small_file_manifest_cold` rows plus a `failed` `small_file_cold_loads` record.

Before retrying, clear the cold orphans (the cold DB is dedicated to this
migration):

```sql
TRUNCATE small_file_manifest_cold;
DELETE FROM small_file_cold_loads;
```

`files_index` is only ever **read** by the copy, so it is never at risk during the
copy or validation phases. A retry mints a new `migration_id`; validation compares
only that id.

---

## 8. Reference run (2026-07-09, `migration_id = 35`)

| Step | Result |
| --- | --- |
| Eligible / copied | 7,696,437 rows / 2,068,342,231,422 bytes (~1.88 TiB, < 10 MB) — **100 %** |
| Validation (`--heavy`) | **PASSED** — exact row/byte, per-tape, per-archive-run match; zero null critical fields |
| Removal dry-run | **0 eligible** (all uncovered; `directory_archive_bundles` absent) |
| Cold backup | verified `cold_small_file_catalog_*.dump` |
| `files_index` | **unchanged** (7,725,051 rows); no destructive command run |

Execution order used: hot backup (`--backup-postgres`) → migrate dry-run →
migrate `--execute` → validate `--heavy` → backup cold (`--backup-cold-postgres`)
→ removal dry-run → **stop** (operator accepted no deletion).
