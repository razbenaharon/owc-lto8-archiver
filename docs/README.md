# Documentation map

This is the entry point for engineers and LLMs working on the repository.
Use it to choose the smallest authoritative document set for the task instead
of reading every historical note.

## Read first

1. [`AGENTS.md`](../AGENTS.md) — current repository rules, architecture,
   validation commands, and non-negotiable tape-safety constraints.
2. [`incidents/README.md`](incidents/README.md) — incident index and links to
   open operational risks.
3. [`incidents/000-no-physical-intervention-policy.md`](incidents/000-no-physical-intervention-policy.md)
   — the operator's standing safety policy.
4. [`tape-and-archive-state.md`](tape-and-archive-state.md) — the current
   dated snapshot of tape, drive, session, and campaign state.

For live operations, also read the newest incidents marked **OPEN**. As of
2026-08-20 those are
[`014-20260819-campaign-write-servo-halt.md`](incidents/014-20260819-campaign-write-servo-halt.md)
and
[`015-20260820-campaign-drive-instability.md`](incidents/015-20260820-campaign-drive-instability.md).

## Source-of-truth hierarchy

Use the first applicable source in this table. A dated document is a snapshot,
not a live status endpoint.

| Question | Authoritative source |
| --- | --- |
| Safety or permitted recovery action | `AGENTS.md`, then policy 000; use the safer rule if wording differs |
| What the current code does | Current source and tests |
| Per-file inventory of packed small files | The JSONL.zst manifests under the `[LOCAL_MANIFEST_ARCHIVE]` root |
| Session/chunk state | The local PostgreSQL catalog |
| Tape/media health | IBM LTFS `LogFile.csv`, scoped to the current mount |
| Mounted LTFS sync mode | The live mount declaration, event 61259; never `ltfs.conf.local` alone |
| Active configuration | The production host's untracked `config.ini` and `.env`; never infer it from `config.example.ini` or a synced clone |
| Last documented production state | [`tape-and-archive-state.md`](tape-and-archive-state.md), then the newest open incident |
| Performance statistics | `backup_logs/SUMMARY.csv`; do not parse `archiver.log` for reports |
| Historical cause and evidence | The corresponding incident document |

When sources conflict, prefer live evidence over a dated snapshot, current code
over a design document, and a later explicit correction over an older
recommendation. Never relax a tape-safety rule merely to reconcile a conflict.

## Environment identity

The repository may be synchronized to more than one Windows computer. Before
reporting "current status", record `$env:COMPUTERNAME`. A check on a computer
other than the production archive host describes only that clone; if
production cannot be queried, say so explicitly and label the result
"last documented state as of YYYY-MM-DD". Do not browse or walk the LTFS
drive to establish status.

## Task routing

| Task | Read |
| --- | --- |
| Understand the system, module boundaries, data flow, schemas | [`architecture.md`](architecture.md) |
| Live archive operation, stop, or recovery | [`operations.md`](operations.md), then the newest open incidents |
| Run or extend the tests; validate a change | [`testing-and-validation.md`](testing-and-validation.md) |
| Current tape/session/campaign state | [`tape-and-archive-state.md`](tape-and-archive-state.md) |
| Legacy export & PostgreSQL pruning (deferred Plan 4) | [`archive-modernization-plans/04_LEGACY_EXPORT_AND_POSTGRESQL_PRUNING.md`](archive-modernization-plans/04_LEGACY_EXPORT_AND_POSTGRESQL_PRUNING.md) |
| Small-file manifest export/prune | [`local_small_file_manifest_runbook.md`](local_small_file_manifest_runbook.md) |
| Drive cleaning / ITDT diagnostics | [`drive_cleaning_and_itdt_runbook.md`](drive_cleaning_and_itdt_runbook.md) |
| Performance work | [`performance_insights_and_recommendations.md`](performance_insights_and_recommendations.md) |
| Transfer/chunk sizing | [`tape_transfer_size_analysis.md`](tape_transfer_size_analysis.md) |
| Reclaiming server space after archival | [`server_deletions.md`](server_deletions.md) |

Retired historical documents (completed plan handoffs, one-off migration
runbooks, superseded design notes, benchmarks) live **outside the public
tree** in the operator's private records; their still-operative content was
folded into the documents above.

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
