# Incident log — OWC LTO-8 archiver

One file per incident. Each entry records **when**, **root cause**, **what was done**,
and **what prevents a repeat**. Written in English to match the rest of the repo
(`AGENTS.md`, `CLAUDE.md`) since assistants read these files.

**Prime directive for every entry here:** see
[docs/incidents/000-no-physical-intervention-policy.md](000-no-physical-intervention-policy.md).
Any incident whose recovery required someone to physically walk to the drive is
tagged **PHYSICAL** and is treated as a design defect, not just an outage.

## Last documented production state

**Snapshot: `LAB-HPLB-09`, as observed 2026-07-25. This is not live status.**
Before acting, verify the production host directly with read-only process,
PostgreSQL, current-mount, and LTFS-log checks. Do not infer production state
from a synchronized clone on another computer.

| Item | Last documented state |
| --- | --- |
| Production run | **Stopped; not resumed** |
| Tape_02 | Last confirmed mounted as `Z:`, **read-only**, about 3.6 TB used |
| Remote session 37 | Chunks 0–48 `done`; 49 `backing`; 50 stale `fetching`; 51–95 `pending` |
| Preserved chunk 49 pack | Verified intact on the production host: 6 files, 1,731,382,179 bytes, with resume marker |
| Remaining work | About 73 GB |
| Data integrity in incident 010 | No loss found; chunk 49 was empty on tape and was not committed |

Open decisions and risks:

- Incident [010](010-20260724-ltfs-write-perm-readonly.md): diagnose the drive,
  then explicitly choose repair or read-only retirement for Tape_02.
- Incident [005](005-20260715-sccm-forced-restart-data-loss.md): an SCCM
  maintenance window/deployment exemption remains the durable restart fix.
- Incident [007](007-20260717-dns-blip-3-day-idle.md): external monitoring and
  automatic relaunch remain unimplemented.
- Incident [008](008-20260717-fetch-overrun-abort-trap.md): the documented
  host-local config overrides still require re-verification/revert at session
  37 completion.

When production state changes, update this section's date and facts, then update
the relevant open incident. Preserve older evidence inside the incident file.

## Summary table

| # | Date | Incident | Root cause (one line) | Fix | Physical? |
| --- | ------ |----------| ---------------------- |-----| ----------- |
| [001](001-20260710-ram-phantom-cache-stall.md) | 2026-07-10 | Pipeline stalls forever at "producer chunk 1" | Governor gated on `psutil` percent, which counts reclaimable Windows file cache as *used* | Drain-relax + recalibrated thresholds + 8.8 GB pagefile + cache-buster | no |
| [002](002-20260713-pack-singlethread-bottleneck.md) | 2026-07-13 | Chunk cycle ~90% single-thread PACK | One thread zipping ~200k tiny files at ~58 files/s | `pack_parallel_workers=3` (2.4× faster); 4 workers tested and rolled back | no |
| [003](003-20260713-tape-stage-deadlock.md) | 2026-07-13, 07-15, 07-23 | Pipeline hard-hangs mid-chunk, all threads 0% CPU | `db_sync` ⇄ `pack` deadly embrace in `ResourceGovernor.decision()` | Root-caused 2026-07-23 via py-spy; fixed in `0552a52` + regression test | no |
| [004](004-20260714-postgres-pool-timeout.md) | 2026-07-14 | `[PIPELINE] STOPPED: couldn't get a connection after 5.00 sec` | Docker Desktop down → `lto_pg` gone; **plus** streaming-thread reads had no retry | Start Docker; added `PgConnectionCore._run_read` retry | no |
| [005](005-20260715-sccm-forced-restart-data-loss.md) | 2026-07-15 | ~126 GB lost (chunks 18–91 of Tape_02) | SCCM (`CcmExec.exe`) restart with 60 s warning **+** `sync_type=unmount` (index only written at unmount) | `_pre_tape_write_reboot_check`; mount verified `time@5`; escalate to IT for SCCM window | **yes** |
| [006](006-20260716-drive-letter-and-conf-drift.md) | 2026-07-16 | Run fails at drive-ready check | LTFS mount letter moved `E:` → `Z:`; MSI reinstall silently reset `ltfs.conf.local` | Verify `lto_drive` with `Test-Path` before every run; verify `sync_type` from the *mount*, not the file | **yes** |
| [007](007-20260717-dns-blip-3-day-idle.md) | 2026-07-17 | Run stopped at chunk 25, sat idle ~3 days | Momentary Technion DNS failure (`could not resolve so01`); monitor ran on the host that lost the network; no auto-relaunch | `_fetch_one_batch` transient retry + backoff. Monitor placement & auto-relaunch still open | no |
| [008](008-20260717-fetch-overrun-abort-trap.md) | 2026-07-17 | Run aborts: "fetched exceeds 2.0x the planned" | Partial resume plans only remaining bytes, but tar re-pulls whole batches | `fetch_overrun_abort_factor` override (**pending revert**); abort is self-healing — plain relaunch replans | no |
| [009](009-20260724-robocopy-exit0-lie.md) | 2026-07-24 | Chunk 49 "succeeded" with 0 files copied | Robocopy returned exit 0 after `RETRY LIMIT EXCEEDED`, emitting no trustworthy summary | Durable raw logs + conservative classifier (`efda427`, `04dc841`, `19106f3`) | no |
| [010](010-20260724-ltfs-write-perm-readonly.md) | 2026-07-24 → **OPEN** | All writes fail `ERROR 19 — media is write protected` | Drive/media WRITE-PERM error → LTFS latched the volume read-only; **not** the WP switch | Diagnosed 2026-07-25. Recovery not yet applied — see the file | **yes (so far)** |

## Conventions

- Dates are local host time (`LAB-HPLB-09`, Asia/Jerusalem) unless a UTC stamp is
  quoted from Postgres, which stores `timestamptz` in UTC. The DB is ~3 h behind
  local wall-clock in summer — `07:16 UTC` in `remote_chunks.updated_at` is
  `10:16` on the console.
- "Chunk N" always means `remote_chunks.chunk_index = N`, i.e. the (N+1)-th chunk.
- Evidence paths (`backup_logs/…`, LTFS `LogFile.csv`) are quoted verbatim so the
  claim can be re-checked later.
- "Current", "now", and "mounted" are valid only inside an explicitly dated,
  host-named snapshot. Otherwise use "last documented" or historical wording.
