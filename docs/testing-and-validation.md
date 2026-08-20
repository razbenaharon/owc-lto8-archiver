# Testing and validation

How to validate a change before it goes anywhere near hardware. The required
progression after code changes is in
[operations.md](operations.md#after-code-changes); this page covers the test
mechanics.

## Offline suite

```powershell
python -m pytest -q
```

Safe and complete on its own, **except the PostgreSQL suites, which skip**
without an explicit server (expected shape: on the order of ~1312 passed,
~149 skipped — the skips are the PG suites refusing to guess a server).
No hardware, network, or database is required.

## Syntax sweep before handoff

```powershell
$files = @(Get-ChildItem src -Filter *.py | ForEach-Object { $_.FullName }) + @(Get-ChildItem storage_map -Filter *.py | ForEach-Object { $_.FullName }) + @((Resolve-Path run.py).Path, (Resolve-Path inspect_db.py).Path); python -m py_compile @files
```

## PostgreSQL tests — disposable server only

The one canonical recipe (port **15432**; port 55432 is inside a Windows
excluded-port range on the production host):

```powershell
docker run -d --name lto_pg_test -e POSTGRES_DB=postgres -e POSTGRES_USER=lto `
  -e POSTGRES_PASSWORD=<pw> -p 127.0.0.1:15432:5432 `
  --tmpfs /var/lib/postgresql/data:rw,size=2g --shm-size=1g postgres:17

$env:LTO_TEST_PG_DSN = "postgresql://lto:<pw>@127.0.0.1:15432/postgres"
$env:LTO_PG_SEALED_BATCH_IT = "1"
python -m pytest tests/ -q            # full run, 0 skipped

docker rm -f lto_pg_test              # tmpfs: the server vanishes with it
```

Every test database is named `ltotest_<run-id>_…`; cleanup drops only names
carrying the current run's marker, and `pytest_unconfigure` sweeps leaks.

### The `pg_test_guard` hazard

**Never point tests at the production `lto_pg` on port 5432.** The PG suites
used to fall back to `build_conninfo` defaults — `localhost:5432`, exactly
where the production container listens. `tests/pg_test_guard.py` now forbids
that, fail-closed: no implicit defaults, no port 5432, no non-loopback host,
and no server hosting `lto_archive`. A DSN that is set but unsafe **fails at
collection**; it never degrades to a skip. Do not weaken this guard.

Note: `archiver_lock_status()` counts advisory locks cluster-wide, not per
database — tests against a throwaway DB must patch it or a live archiver on
another database will block them.

## Performance reference

Full numbers, methodology, and tuning history:
[performance_insights_and_recommendations.md](performance_insights_and_recommendations.md).
The three real-world limiters, in brief:

- **RAM — phantom file cache:** buffered tar extraction fills the Windows file
  cache, which `psutil` counts as "used"; the governor relaxes low-RAM drain
  stages rather than deadlocking on a reclaimable cache. The tuned
  `[PERFORMANCE]` thresholds are host-calibrated — do not restore defaults.
- **Tape write — LTFS index sync:** robocopy runs at full LTO-8 speed, but the
  required `sync_type=time@5` index re-sync collapses *effective* per-chunk
  speed; amortise only through bounded chunk sizing
  ([tape_transfer_size_analysis.md](tape_transfer_size_analysis.md)).
- **Fetch — single-stream small-file latency:** one SSH/tar stream over many
  tiny files is latency-bound (~15 MB/s); `fetch_parallel_streams = 3`
  measured ~30 MB/s, near the WAN/server ceiling.
