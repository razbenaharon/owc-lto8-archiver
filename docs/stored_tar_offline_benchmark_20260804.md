# Stored TAR Offline Benchmark 2026-08-04

Task 2.8 sample run for `stored-tar-benchmark-v1`.

Run date: 2026-08-04 15:49:48 Jerusalem Daylight Time.
Raw harness output: `storage_map_logs/benchmark_stored_tar/20260804T124948Z/`.

This is one local sample on one machine, not a benchmark-suite average.

## Machine

- Platform: `Windows-11-10.0.26200-SP0`
- Python: `3.14.5`
- CPU: `Intel64 Family 6 Model 140 Stepping 1, GenuineIntel`
- Logical CPUs: `8`
- Physical CPUs: `4`
- RAM: `15.62 GiB`

## Method

- Both flows consumed the same locally generated TAR byte stream per profile.
- `current` means local TAR extraction to staging followed by `LTOPacker` ZIP_STORED packaging.
- `stored_tar` means direct `.tar.part` creation, full `validate_stored_tar_part()` validation, and sidecar publication through `publish_stored_tar_pair()`.
- Profiles run: `small`, `medium`, `large`, and `sparse`.
- No PostgreSQL, SSH, LTFS, tape, `Z:\`, `config.ini`, `.env`, or `backup_logs/SUMMARY.csv` writes were used.

## Results

| Profile | Flow | Files | Logical MiB | Output MiB | Wall s | CPU s | Peak RSS MiB | Peak staging MiB | Final staging MiB | Peak entries | Final entries | Ready s |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| small | current | 512 | 2.00 | 2.07 | 5.688 | 5.359 | 53.92 | 6.57 | 6.57 | 525 | 525 | 5.667 |
| small | stored_tar | 512 | 2.00 | 2.50 | 0.115 | 0.094 | 38.38 | 2.50 | 2.50 | 3 | 3 | 0.106 |
| medium | current | 4096 | 8.00 | 8.59 | 93.092 | 107.516 | 71.69 | 26.84 | 26.84 | 4133 | 4133 | 92.915 |
| medium | stored_tar | 4096 | 8.00 | 10.28 | 0.666 | 0.656 | 58.65 | 10.28 | 10.28 | 3 | 3 | 0.658 |
| large | current | 16384 | 16.00 | 18.34 | 346.917 | 409.359 | 117.91 | 58.59 | 58.59 | 16453 | 16453 | 346.609 |
| large | stored_tar | 16384 | 16.00 | 24.35 | 2.845 | 2.750 | 106.23 | 24.35 | 24.35 | 3 | 3 | 2.836 |
| sparse | current | 1 | 128.00 | 128.00 | 0.539 | 0.422 | 79.31 | 368.25 | 256.25 | 8 | 7 | 0.529 |
| sparse | stored_tar | 1 | 128.00 | 0.25 | 0.069 | 0.031 | 79.28 | 0.25 | 0.25 | 3 | 3 | 0.058 |

## Interpretation

- `small`, `medium`, and `large` all show the same shape: direct Stored TAR reaches writer-ready output dramatically faster and with materially lower staging footprint and CPU time than extract-and-repack.
- For those three cardinality profiles, Stored TAR produces a larger final artifact than ZIP_STORED:
  - `small`: +0.43 MiB
  - `medium`: +1.69 MiB
  - `large`: +6.01 MiB
- The sparse profile changes the decision materially. The current path expands the sparse member into a large staged file and then stores it in ZIP, while Stored TAR preserves the sparse TAR structure:
  - Output bytes: `128.00 MiB` current vs `0.25 MiB` Stored TAR
  - Peak staging: `368.25 MiB` current vs `0.25 MiB` Stored TAR
  - Ready time: `0.529 s` current vs `0.058 s` Stored TAR

## Decision

For small-file-heavy chunks, this sample strongly favors enabling the TAR writer from a staging-footprint, CPU, and time-to-ready perspective. The cost in this offline sample is larger final artifact size for non-sparse profiles, so the decision is:

- If writer readiness, local staging pressure, and CPU overhead dominate, Stored TAR wins decisively.
- If minimizing final artifact bytes dominates and the corpus is entirely regular small files, ZIP_STORED still wins on output size.
- Sparse files are the clearest case for Stored TAR; the current materialization path is structurally wasteful there.

## Limits

- One host, one run, one set of deterministic profiles.
- No SSH transport variance, no PostgreSQL work, and no LTFS/tape interaction were included.
- The benchmark is intentionally offline; no conclusion here relies on a hardware tape read.
