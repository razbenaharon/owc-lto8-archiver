# Should every tape transfer be ~500 GB instead of ~1 GB?

**Question (operator, 2026-07-25):** the tape writes look like small ~1 GB
transfers; wouldn't one big ~500 GB transfer per write be better?

**Short answer:** the *instinct* is sound — tape rewards long sequential writes —
but the premise is inaccurate for this system, and 500 GB is the wrong number by
a wide margin. **Recommendation: do not change chunk sizing.** The measured
upside is ~2–3% of wall-clock, and the cost is a much larger blast radius, which
is the exact thing that has hurt this project repeatedly.

---

## 1. The premise is not what the data shows

Chunks are **not** ~1 GB. They are bounded by **file count**
(`chunk_max_files = 200000`), not by bytes, so their size follows whatever files
land in them. For session 37 (plan 37, 96 chunks, 19.8 M files, 1.16 TB):

| Metric | Value |
| -------- | ------- |
| Smallest chunk | **1,587 MB** |
| **Average chunk** | **12 GB** |
| **Largest chunk** | **217 GB** |
| Total session | 1,160 GB |
| **Remaining (chunks 49–95)** | **73 GB** |
| Existing byte safety cap | `chunk_cap_gb = 250` |

Two consequences:

- A **217 GB** single transfer has *already happened* in this very session. The
  system is not doing "small 1 GB transfers" as a rule — the recent chunks are
  small only because the job is currently crossing a region of tiny files
  (~8.6 KB average in chunk 49).
- **The entire remaining job is 73 GB.** A 500 GB transfer size would make all
  47 remaining chunks into **one single all-or-nothing write** — and still not
  reach 500 GB. The proposal is larger than the work that is left.

## 2. It would not affect session 37 at all

Chunk sizing applies to **newly-scanned chunks only**. Chunks already in the plan
keep their planned composition, and the value is read once at startup. To make a
new size take effect on session 37 you would have to **re-plan the session**,
which throws away the resume position — far more expensive than anything it saves.

So for the job actually in front of us, this change is a **no-op with a migration
cost**.

## 3. What the measurements say about the upside

From `backup_logs/SUMMARY.csv`, the last four successful chunks (~1.73 GB each):

| Phase | Measured |
| ------- | ---------- |
| `pack_seconds` | **1468 – 1594 s** ← dominant |
| `fetch_seconds` | 218 – 371 s |
| `robocopy_elapsed` | 25 – 66 s |
| `tape_open_seconds` | 15 – 30 s (fixed per write) |
| `tape_close_seconds` | 0 s |
| `tape_stream_mbs` | 204 – 260 MB/s (peak 391 – 420) |

At ~250 MB/s, 1.73 GB is about **7 seconds** of actual streaming, sitting inside
a tape phase of ~40–95 s. So per-write fixed overhead really is most of the tape
phase — the instinct is correct *at that level*.

But the tape phase is **~3–5% of a ~1,800–2,000 s chunk cycle**. Even driving
per-write overhead to zero saves ~45 s per chunk: across all 47 remaining chunks
that is **~35 minutes out of a ~23-hour job (~2.5%)**.

Meanwhile **PACK is ~75–80% of every cycle**. Any real throughput work belongs
there, not on the tape write. That is where the last big win came from
([incident 002](incidents/002-20260713-pack-singlethread-bottleneck.md): 2.4×).

## 4. Why the tape penalty is already largely solved

The classic reason small transfers destroy tape performance is millions of tiny
files hitting the drive, forcing per-file overhead, index churn and shoe-shining.
**That does not happen here.** The packer already consolidates each chunk into
~3 zip bundles of ~577 MB plus 3 small manifests — the drive sees **6 large
sequential files**, which is exactly what tape wants. LTFS's known pathological
case (files under 4 KB written directly to tape) is fully avoided.

The remaining per-write overhead is LTFS index sync and positioning, and
**index sync is time-driven here (`sync_type=time@5`, every 5 minutes), not
per-write** — so a longer single write does not avoid index syncs, it just
contains more of them. The theoretical gain from larger writes is therefore
smaller than it first appears.

## 5. Why 500 GB is actively dangerous on this host

| Risk | Detail |
| ------ | -------- |
| **Staging capacity** | `C:` has ~1.31 TB free. A 500 GB chunk plus `prefetch_chunks_ahead=1` puts ~1 TB in staging *before* pack output. Disk-full during a multi-hour chunk. |
| **RAM** | 15.6 GB host, already RAM-governor-bound ([001](incidents/001-20260710-ram-phantom-cache-stall.md)); 4 pack workers were *slower* than 3 for this reason ([002](incidents/002-20260713-pack-singlethread-bottleneck.md)). |
| **Blast radius** | Chunk 49 failed after ~30 min of work and the 1.73 GB pack was preserved and reused with no re-fetch. At 500 GB a late failure discards ~10 h of fetch+pack. |
| **Resume granularity** | `remote_chunks` tracks state **per chunk**. Fewer, larger chunks = coarser resume = more re-work after any stop. |
| **Exposure to forced restarts** | SCCM gives **60 seconds** of warning ([005](incidents/005-20260715-sccm-forced-restart-data-loss.md)). A 33-minute write is a 33-minute window in which that can land. |
| **Latching write errors** | [Incident 010](incidents/010-20260724-ltfs-write-perm-readonly.md): one unrecoverable write set the PWE bit and made the cartridge **permanently read-only**. Longer writes mean more time exposed to that, and a bigger loss when it happens. |
| **Time to first durable data** | Nothing reaches tape for ~10 h. Under `time@5` the index is safe, but the *work* is not. |

The last three items conflict directly with the operator's own standing
requirement to
[minimise physical intervention](incidents/000-no-physical-intervention-policy.md).
Larger transfers trade a ~2.5% speed gain for a materially higher chance of the
one outcome we are trying hardest to avoid.

## 6. Verdict and what to do instead

**Do not adopt 500 GB.** Keep `chunk_max_files = 200000` and `chunk_cap_gb = 250`.

If throughput becomes the priority, in order of value:

1. **Attack PACK** — it is 75–80% of the cycle. This is the only lever with a
   large payoff left.
2. **Reduce fixed per-write overhead**, not chunk size — e.g. avoid re-issuing
   directory-attribute operations Robocopy does not need (`/DCOPY:DA` is what
   surfaced ERROR 19 in incident 010).
3. **Consider *lowering* `chunk_cap_gb`.** A 217 GB chunk already exists in this
   plan; capping nearer ~50 GB would bound worst-case re-work without measurably
   hurting tape efficiency (a 50 GB write is ~200 s of streaming against ~30 s of
   overhead — already ~87% efficient).

**Where the operator is right:** transfers in the low-GB range *are* inefficient
at the tape layer, and if this were the bottleneck it would be worth fixing. It
isn't — packing is. The efficiency curve also flattens fast: most of the benefit
of "bigger" is captured by ~20–50 GB, and everything beyond that buys single-digit
percentages while multiplying risk.

---

### Sources

- [Why LTO Tape Drives Never Reach Their Rated Speed — Archiware](https://blog.archiware.com/blog/why-lto-tape-drives-never-reach-their-rated-speed/)
- [Linear Tape File System — Grokipedia](https://grokipedia.com/page/Linear_Tape_File_System)
- [Extremely slow write performance for files <4kb — LTFS issue #496](https://github.com/LinearTapeFileSystem/ltfs/issues/496)
- [YoYotta — LTO + LTFS FAQ](https://yoyotta.com/help/LTO_FAQ.html)
