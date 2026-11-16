# 004 — `[PIPELINE] STOPPED: couldn't get a connection after 5.00 sec`

- **When:** 2026-07-14
- **Physical intervention required:** no
- **Status:** fixed

## Symptom

```text
[PIPELINE] STOPPED: couldn't get a connection after 5.00 sec. Re-run to resume.
```

That string is a psycopg_pool `PoolTimeout` (`make_pool` sets `pool_timeout=5`).

## Root cause — two parts

**Operational:** Docker Desktop was not running, so the `lto_pg` container was
down and nothing was listening on `127.0.0.1:5432`.

**Code bug (the real one):** writes went through `_transaction`, which retries
`PoolTimeout` / `OperationalError` with backoff — but streaming-thread **reads**
(`get_chunk_files`, `get_chunk_size_summary`, `get_pending_chunks`,
`get_remote_existing_snapshot_paths`, `get_remote_session`, …) used a bare
`with self._pool.connection()` with **no retry**. So a merely *transient* DB blip
on a read path escaped into a pipeline thread's generic `except Exception`, set
`stop_pipeline`, and tore down the whole run.

## Fix

- Added `PgConnectionCore._run_read` so read paths get the same retry/backoff as
  writes.
- Operational recovery: start
  `C:\Program Files\Docker\Docker\Docker Desktop.exe`; the Linux engine comes up
  in ~10 s and `lto_pg` **auto-starts** (restart policy). Wait for
  `docker inspect -f '{{.State.Health.Status}}' lto_pg` to report `healthy`.

## Notes

The live database is the migrated
`lto_archive_directory_catalog_20260710_103359` (from `cfg.db_dsn`, password read
from `.env`), **not** the empty `lto_archive.db` sqlite file left in the project
root. Query it as user `lto`.
