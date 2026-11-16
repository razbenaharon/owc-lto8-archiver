# History: records from the original deployment

Everything in this folder is a **dated record** from the single installation
this program was first built for: one LTO-8 drive on one Windows workstation,
archiving data from lab servers to tape during 2026.

**These files are not instructions for a new user.** They describe one
site's hardware, hosts, cartridges, sessions and decisions at the time they
were written. Tape labels, session numbers, drive letters, measurements and
"current state" statements in them refer to that site and that date. Some
commands in them no longer exist. Names of hosts were redacted before
publication.

They are kept because the safety rules of this project were learned from
them, and a rule is easier to respect when you can read what it cost.

If you are setting up or operating the program, use the guides:
[installation](../installation.md), [user guide](../user-guide.md),
[safety rules](../safety.md), [troubleshooting](../troubleshooting.md).

**The safety policy is the exception:** it is still in force. Its current,
general statement is [../safety.md](../safety.md); the original wording is
[incidents/000-no-physical-intervention-policy.md](incidents/000-no-physical-intervention-policy.md).

## Contents

| File | What it is |
| --- | --- |
| [incidents/](incidents/README.md) | One post-mortem per incident (001-015) and the standing policy (000): what broke, why, what was done, what prevents a repeat |
| [agents-production-notes.md](agents-production-notes.md) | Site-specific operating notes and lessons that used to live in `AGENTS.md` |
| [operations.md](operations.md) | The site's operations runbook as of August 2026. Superseded by the [user guide](../user-guide.md) and [troubleshooting](../troubleshooting.md); two commands in it never worked (noted at its top) |
| [PROJECT_STATUS.md](PROJECT_STATUS.md) | Status page of the original project, last updated 2026-10-14 |
| [tape-and-archive-state.md](tape-and-archive-state.md) | Snapshot of the site's tapes, sessions and catalog, 2026-08-20 to 2026-09-23 |
| [performance_insights_and_recommendations.md](performance_insights_and_recommendations.md) | Throughput measurements and tuning history on the site's hardware |
| [tape_transfer_size_analysis.md](tape_transfer_size_analysis.md) | Why very large single transfers were rejected |
| [drive_cleaning_and_itdt_runbook.md](drive_cleaning_and_itdt_runbook.md) | Drive cleaning and read-only ITDT diagnostics, as done at the site |
| [local_small_file_manifest_runbook.md](local_small_file_manifest_runbook.md) | Export and pruning of small-file rows from the site's PostgreSQL catalog |
| [archive-modernization-plans/](archive-modernization-plans/04_LEGACY_EXPORT_AND_POSTGRESQL_PRUNING.md) | A deferred plan for legacy export and PostgreSQL pruning |

## What still applies in general

Lessons from these records that hold for any installation have been folded
into the guides:

| Lesson | Now in |
| --- | --- |
| Prefer anything recoverable remotely over anything faster | [safety.md](../safety.md) |
| `sync_type=time@5`, verified from the mount; never `unmount` | [safety.md](../safety.md), incident 005 |
| `robocopy` exit code 0 is not success | [safety.md](../safety.md), incident 009 |
| A hard write error can make a cartridge read-only for good | [safety.md](../safety.md), incident 010 |
| The LTFS drive letter is not stable; `Test-Path` does not prove a live mount | [safety.md](../safety.md), incidents 006, 011, 012 |
| Stop only at a chunk boundary; how to stop a detached run | [user-guide.md](../user-guide.md#stopping-a-run-safely) |
| A transient network error should cost a retry, not the run | `fetch_transient_retries` in [configuration.md](../configuration.md), incident 007 |
| Do not run the only monitor on the host that does the work | [user-guide.md](../user-guide.md#supervising-a-long-run), incident 007 |
| Never point tests at a real catalog | [testing-and-validation.md](../testing-and-validation.md) |
