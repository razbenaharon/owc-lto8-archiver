# 001 — Pipeline stalls forever at "producer chunk 1" (phantom RAM)

- **When:** 2026-07-10
- **Physical intervention required:** no
- **Status:** fixed

## Summary

A remote-archive run fetched a chunk fine (~30 MB/s) then sat forever on
`producer chunk 1/streaming`. Telegram heartbeats repeated `staging=0/700 GB`,
the `_pack_*` directory stayed at 0 files while `_fetch_*` held the full 44 GB,
and the governor logged on a 30 s loop:

```text
[GOVERNOR] pack start: ... memory=90-94% reason=hard_ram_limit,ram_soft_limit
```

## Root cause

**Not** real memory exhaustion. The local `tar -xf` extraction of a chunk into
staging fills the Windows file cache as *active* buffered pages.
`psutil.virtual_memory()` counts that reclaimable cache as "used", so `available`
reads ~1 GB and `percent` ~90–94% within seconds of any activity — while real
committed memory was ~20/24 GB with 4 GB free, backstopped by the pagefile.

The governor gated on physical percent, so it blocked on **phantom cache**, not
on crash risk. Proof: the moment packing actually did work, Windows evicted the
cache and psutil dropped to ~75% / 3.9 GB free on its own. The archiver process
itself was tiny (`process_rss` 44 MB) — it was never the consumer.

## Fix

- Governor drain-relax logic so a stalled state cannot self-perpetuate.
- Recalibrated RAM thresholds in `config.ini [PERFORMANCE]`, **host-calibrated
  for this 15.6 GB box**.
- PostgreSQL capped at 1 GB.
- Cache-buster to force eviction.

## Prevention / notes

`config.ini [PERFORMANCE]` must **not** be "restored to defaults" — the tuned
thresholds plus the 8.8 GB pagefile are the correct, crash-safe configuration for
this host. See [docs/pipeline_ram_context.md](../pipeline_ram_context.md).
