# 002 — PACK was the pipeline bottleneck (single-threaded)

- **When:** identified and fixed 2026-07-13 (commit `957c30d`)
- **Physical intervention required:** no
- **Status:** fixed

## Summary

Roughly 90% of each chunk cycle was a single thread packing ~200k tiny files at
~58 files/s — about 57 minutes per chunk — while the tape sat idle.

## Root cause

The packer was serial by design. With this workload (millions of ~4 KB files) the
per-file overhead dominates, and one thread cannot saturate either the disk or
the tape.

## Fix

`pack_parallel_workers = 3` in `config.ini [PERFORMANCE]`: the chunk's file list
is split across N worker **threads**, each writing its own uniquely named bundle.
Threads (not processes) keep RSS flat on this 15.6 GB host.

Measured: pack time **3425 s → 1397 s (~2.4× faster)**, tape stream stayed clean.

Validated byte-for-byte against the serial packer offline before enabling.
`pack_parallel_workers = 1` restores the legacy serial path instantly.

## Do not retry

A 4-worker experiment on 2026-07-13 was **rolled back** — the run became
RAM-governor-bound and 4 workers were ~10% *slower* than 3. **3 is the sweet
spot; do not retry 4 or 5** on this host.

## Residual

PACK is still the dominant phase (~1500 s of a ~1800–2000 s chunk cycle). Any
future throughput work should target PACK, not the tape write — see
[docs/tape_transfer_size_analysis.md](../tape_transfer_size_analysis.md).
