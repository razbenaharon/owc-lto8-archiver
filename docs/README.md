# Documentation map

This is the entry point for engineers and LLMs working on the repository.
Use it to choose the smallest authoritative document set for the task instead
of reading every historical note.

## Read first

1. [`AGENTS.md`](../AGENTS.md) — current repository rules, architecture,
   validation commands, and non-negotiable tape-safety constraints.
2. [`incidents/README.md`](incidents/README.md) — incident index, the latest
   dated production snapshot, and links to open operational risks.
3. [`incidents/000-no-physical-intervention-policy.md`](incidents/000-no-physical-intervention-policy.md)
   — the operator's standing safety policy.

For live operations, also read the newest incident marked **OPEN**. As of
2026-07-25 that is
[`010-20260724-ltfs-write-perm-readonly.md`](incidents/010-20260724-ltfs-write-perm-readonly.md).

## Source-of-truth hierarchy

Use the first applicable source in this table. A dated document is a snapshot,
not a live status endpoint.

| Question | Authoritative source |
| --- | --- |
| Safety or permitted recovery action | `AGENTS.md`, then policy 000; use the safer rule if wording differs |
| What the current code does | Current source and tests |
| Current production state | Read-only checks on **`EXAMPLE-HOST`**: processes, PostgreSQL, LTFS event/log evidence |
| Last known production state when the host is unavailable | The explicitly dated snapshot in `incidents/README.md`, then the newest open incident |
| Active configuration | Production host's untracked `config.ini` and `.env`; never infer it from `config.example.ini` or a synced clone |
| Mounted LTFS sync mode | The live mount declaration, event 61259; never `ltfs.conf.local` alone |
| Drive/media health | `C:\Program Files\IBM\LTFS\log\LogFile.csv`, scoped to the current mount |
| Session/chunk state | Production PostgreSQL catalog |
| Performance statistics | `backup_logs/SUMMARY.csv`; do not parse `archiver.log` for reports |
| Historical cause and evidence | The corresponding incident document |

When sources conflict, prefer live evidence over a dated snapshot, current code
over a design document, and a later explicit correction over an older
recommendation. Never relax a tape-safety rule merely to reconcile a conflict.

## Environment identity

The repository may be synchronized to more than one Windows computer. Before
reporting "current status", record `$env:COMPUTERNAME`.

- Production archive host: **`EXAMPLE-HOST`**.
- A check on another computer describes only that clone. Missing Docker,
  `run.py`, staging files, or an LTFS drive there does **not** update production
  status.
- If production cannot be queried, say that explicitly and label the result
  "last documented state as of YYYY-MM-DD".

Do not browse or walk the LTFS drive to establish status. During a write, use
process state, PostgreSQL, kernel counters, and LTFS logs only.

## Task routing

| Task | Read |
| --- | --- |
| Code change or review | `AGENTS.md`, relevant source/tests |
| Live archive operation or recovery | `AGENTS.md` → operating playbook, incident index, newest open incident |
| Performance work | [`performance_insights_and_recommendations.md`](performance_insights_and_recommendations.md), then [`pipeline_ram_context.md`](pipeline_ram_context.md) when RAM is involved |
| Tape/drive WRITE-PERM investigation | Incident 010 and [`drive_cleaning_and_itdt_runbook.md`](drive_cleaning_and_itdt_runbook.md) |
| Transfer/chunk sizing | [`tape_transfer_size_analysis.md`](tape_transfer_size_analysis.md) |
| Directory catalog migration | [`directory_catalog_migration_runbook.md`](directory_catalog_migration_runbook.md) |
| Small-file manifest export/prune | [`local_small_file_manifest_runbook.md`](local_small_file_manifest_runbook.md) |
| Proposed directory completeness feature | [`directory_completeness_feature_design.md`](directory_completeness_feature_design.md); it is a design, not implemented behavior |

## Documentation maintenance rules

- Every operational state statement must name the host and include an
  `as observed` date/time.
- Mark historical settings and measurements as historical; do not call them
  "current" without live verification.
- Put corrections at the top summary as well as beside the historical detail.
- Update `incidents/README.md` when an incident opens, closes, or changes the
  production recovery plan.
- Add a new incident rather than rewriting evidence from an old one. Cross-link
  the correction from both entries.
