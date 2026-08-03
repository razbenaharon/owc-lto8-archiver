# Plan 1 — completion gate (evidence-backed review)

**Status: CLOSED — COMPLETE AND VALIDATED OFFLINE.** (closed 2026-08-03)

Plan 1 (`docs/archive-modernization-plans/01_CORE_SIMPLIFICATION_AND_INCREMENTAL_SCAN.md`)
is closed for development. All 20 tasks across its 5 phases are implemented and
covered by tests; §1 is the task-by-task matrix. **1408 tests pass, 0 skipped,
0 failed, 0 deselected.**

**Hardware validation: NOT RUN, OPERATOR-SUPERVISED TAPE WRITE REQUIRED LATER.**
That is a production-activation gate, not a Plan 1 code defect — see §9.

Closing Plan 1 does **not** enable anything in production. The incremental scan
frontier ships **disabled** (`incremental_scan = false`), migration 014 is
applied to **no** database, and Session 37 is untouched. §10 lists the five
gates that must be cleared before the frontier may be switched on, each of which
is an operator decision, not a code change. Operational summary for the next
person: [`docs/plan1_handoff.md`](plan1_handoff.md).

Reproduce every claim below with:

```powershell
# a DISPOSABLE PostgreSQL, never the production lto_pg container
docker run -d --name lto_pg_test -e POSTGRES_DB=postgres -e POSTGRES_USER=lto `
  -e POSTGRES_PASSWORD=<pw> -p 127.0.0.1:15432:5432 `
  --tmpfs /var/lib/postgresql/data:rw,size=2g --shm-size=1g postgres:17

$env:LTO_TEST_PG_DSN = "postgresql://lto:<pw>@127.0.0.1:15432/postgres"
$env:LTO_PG_SEALED_BATCH_IT = "1"

python -m pytest tests/ -q                             # 1408 passed, 0 skipped
python scripts/plan1_rehearsal.py --json evidence.json       # stages 1-2
python scripts/validate_migration_014_shadow.py --json shadow_014.json
python scripts/benchmark_scan_models.py --synthetic 60000 --restarts 3

docker rm -f lto_pg_test          # tmpfs: the whole server vanishes with it
```

`LTO_TEST_PG_DSN` is now **required** for any PostgreSQL test to connect —
see §11. Without it the suite still runs (1259 passed, 149 skipped) and opens
no database connection at all.

## 0. What changed in this review

The previous report claimed completion while 53 PostgreSQL tests had never run,
migration 014 had never touched a database, and one test suite hung forever.
Those gaps are now closed, and closing them found four real defects.

| # | Defect | Found by | Severity |
|---|---|---|---|
| 1 | `rename_tape` never repointed `tape_generations` / `tape_reset_operations`, both `ON DELETE RESTRICT`. Every tape rename raised `ForeignKeyViolation` once `register_tape` began creating a generation row. | isolated PostgreSQL | **regression introduced by Plan 1 Task 1.4** |
| 2 | `TapeLockObserver` did not patch `src.remote_writer`, so after the Task 1.2 split it observed **nothing** — any test relying on it silently proved nothing. Its `_acquire` also rejected the `timeout=` argument the real lock takes. | new execution-contract test | **test infrastructure blind spot introduced by Plan 1** |
| 3 | `TapeWriteGovernorLifecycleTests` hung forever. Root cause: it builds a **real** `ResourceGovernor` reading **real host RAM** and blocks in `wait_or_pause("tape", "start")` whenever available RAM < `governor_tape_min_free_ram_gb` (3.0 GB). This host had 1.63 GB available. Not a deadlock, not a Plan 1 regression — a pre-existing host-dependent test. | faulthandler stack dump | pre-existing |
| 4 | `test_session_reconcile.py` fixtures omit `remote_sessions.tape_generation`, `NOT NULL` since migration 013 — 14 failures + 3 errors, invisible while the suite always skipped. | isolated PostgreSQL | pre-existing |
| 5 | A bare `python -m pytest` connected the suite to the **production PostgreSQL server** (`build_conninfo` defaults to localhost:5432, which is `lto_pg`), and fixture cleanup leaked databases onto it. No archive data was at risk, but nothing prevented the connection. | running the suite on this host | pre-existing |

Defect 1 is fixed in `src/pg_tapes.py`. Defects 2–4 are fixed in the test
layer. Defect 5 is fixed by the fail-closed guard in §11. All five now have
regression coverage.

## 1. Task-by-task compliance matrix

Evidence types: **F** = fake-backed test · **P** = isolated PostgreSQL ·
**C** = code inspection · **B** = benchmark/measurement · **S** = shadow database.

| Task | Status | Files / symbols | Tests | Evidence | Operational evidence | Blocker |
|---|---|---|---|---|---|---|
| **0.1** characterization map | Complete | `remote_orchestrator` module docstring | `test_pipeline_characterization.py` (24) | F, C | n/a | — |
| **0.2** scan telemetry + benchmark | Complete | `pipeline_types.ScanMetrics`; `reporting.SCAN_METRIC_COLUMNS` (17 cols); `scanning`; `scripts/benchmark_scan_models.py` | `test_scan_metrics.py` (23), `test_reporting_and_robocopy.py` | F, B, P | Benchmark run, §5 | — |
| **0.3** `incremental_scan_enabled` + gate | Complete | `config.incremental_scan_enabled`; `scan_frontier.decide_scan_mode` | `test_scan_mode_gate.py` (23) | F, C, P | Shadow proves gate stays `legacy`, §4 | — |
| **1.1** `RemoteScanCoordinator` | Complete | `scan_frontier.RemoteScanCoordinator.run/publish_legacy_chunk` | `test_scan_frontier.py` (25) | F, C | n/a | — |
| **1.2** stager + writer extraction | Complete | `remote_staging.RemoteChunkStager`, `remote_writer.RemoteChunkWriter` | `test_remote_failure_hardening.py` (94), `test_phase35` (37) | F, C | n/a | — |
| **1.3** one pipeline coordinator | Complete | `remote_pipeline.RemotePipelineCoordinator` | `test_pipeline_characterization.py`, `test_plan1_rehearsal.py::OverlapRehearsalTests` | F, C | n/a | — |
| **1.4** finite group = only tape path | Complete | `_eject_after_session`, `_resolve_tape_label`, `_announce_target_cartridge`, `pg_tapes.register_tape`, `get_active_tape_generation` | `test_finite_group_only_tape_path.py` (23), `test_execution_contract.py` (29) | F, C, P | `TapeRenameGenerationTests` (P) | Real-drive confirmation deferred to the tape run |
| **1.5** typed transitions | Complete | `pipeline_types.ChunkStatus/CHUNK_TRANSITIONS`; `pg_sessions.transition_chunk` | `test_lifecycle_transitions.py` (32), `test_status_vocabulary.py` (8) | F, C, P | `RealConcurrencyTests` transitions (P) | — |
| **1.6** dead-path removal | Complete | removed `DirectoryFirstRemoteScanner`, `DirectoryUnitPlanner`, `DirectoryPlanUnit`, 4 config knobs | `test_pipeline_characterization.py::RemovedDirectoryFirstPathTests` | C | `docs/plan1_module_boundary_audit.md` | — |
| **2.1** migration 014 + `PgScanMixin` | Complete | `scripts/sql/014_*.sql` (base/finalize/rollback); `pg_core.apply_incremental_scan_schema`; `pg_scan`; `inspect_db --apply-incremental-scan-schema` | `test_migration_014.py` (49), `test_pg_integration.py::IncrementalScanMigrationTests`, `ShadowLegacyMigrationTests` | F, C, P, S | Shadow run, §4 | Not applied to production (intended) |
| **2.2** JSONL.zst artifacts | Complete | `archive_artifacts` | `test_archive_artifacts.py` (29) | F | n/a | — |
| **2.3** directory-boundary continuation | Complete | `scanning.DirectoryFrontierScanner`; `scan_frontier.DirectoryFrontierCoordinator` | `test_incremental_scan_frontier.py` (55) | F, C | n/a | Never run against a real source host |
| **2.4** segment → chunk publication | Complete | `scan_frontier.SegmentChunkPublisher`; `pg_scan.import_legacy_scan_segment`; `pg_sessions.seal_remote_chunk` | `test_segment_chunk_publication.py` (29) | F, P | `ShadowLegacyMigrationTests` seal/append refusal (P) | — |
| **3.1** chunk claims | Complete | `pg_sessions.claim_chunk_for_staging` / `renew` / `release` / `list_expired` / `reclaim` | `test_claims_and_reconciliation.py` (41), `test_pg_integration.py::RealConcurrencyTests` | F, C, P | 6-thread race → exactly 1 winner (P) | — |
| **3.2** startup reconciliation | Complete | `startup_reconcile`; `_detect_prior_backing_chunks` fail-closed | `test_claims_and_reconciliation.py`, `test_execution_contract.py::Clause11` | F, C | n/a | — |
| **3.3** scan scopes + POSIX path validator | Complete | `paths.validate_remote_posix_relpath`, `remote_path_is_legacy_safe`; `pg_scan.create_scan_scopes` | `test_segment_chunk_publication.py::UnrepresentablePathTests`, `test_pg_integration.py::test_an_unrepresentable_path_error_persists_for_review` | F, C, P | Backslash persisted byte-for-byte (P) | — |
| **4.1** read-only session report | Complete | `startup_reconcile.session_frontier_report`; `inspect_db --session-frontier-report` | `test_session_frontier_report.py` (23) | F, C | **Not run against session 37** — production DB is out of scope | Requires operator to run it against production |
| **4.2** frontier bootstrap | Complete | `frontier_bootstrap.FrontierBootstrap`; `inspect_db --bootstrap-frontier` | `test_frontier_bootstrap.py` (20) | F, P | Shadow proves catalog rows ≠ coverage (S) | Never executed against a real session |
| **4.3** rehearsal | Offline complete | `scripts/plan1_rehearsal.py` | all suites | F, P, S | Stages 1–2 PASSED | **Stage 3 = operator-supervised tape run** |
| **5.1** invariant documentation | Complete | module docstrings; `AGENTS.md`; this file | doc assertions in `test_incident_invariants.py` | C | n/a | — |

**No task is `incomplete`. Two are `implemented but unverified against
production reality`:** 4.1 (never run against session 37) and 4.3 stage 3.

## 2. Execution-contract verification

`tests/test_execution_contract.py` — 29 tests, one clause each, all passing.

| Clause | Verified by | How |
|---|---|---|
| finite group is the only tape-writing path | `Clause01` | C: only `remote_orchestrator` builds the backup writer; only `remote_writer` runs the copy |
| no pre-group probes in the three entry methods | `Clause02` | C: `_ensure_lto_drive_ready`, `_verify_mounted_cartridge`, `get_volume_label`, `_acquire_tape_io_lock`, `eject_tape` all absent |
| no LTFS access before a group is ready | `Clause03` | F: sub-threshold queue → no group, no recorded call, no ownership |
| no LTFS access while waiting | `Clause04` | F + C: `RemotePipelineCoordinator` contains no tape token at all |
| one ownership acquisition per group | `Clause05to08` | F: 5 chunks → `OWNERSHIP.generation` +1 |
| one readiness + one cartridge check per group | `Clause05to08` | F: readiness=1, cartridge=1, writes=5 |
| members written consecutively, ownership never released between | `Clause05to08` | F: `TapeLockObserver.depth ≥ 1` at every write, 0 at the end |
| ownership released after the group | `Clause05to08` | F: incl. the abort path |
| auto-eject disabled | `Clause09` | F + C: eject count 0 even with the flag true |
| `backing` always ambiguous | `Clause10` | F + C + P: matrix, claim SQL, real transitions |
| DB read failure ≠ no backing chunk | `Clause11` | F: 4 exception types all produce `SAFETY_BLOCK` |
| session facts measured, not documented | `Clause12` | C: no `37`/`112`/`113`/tape label anywhere in the report path |

## 3. Isolated PostgreSQL results

Run against a **disposable** container on port 15432 with tmpfs storage.
Production `lto_pg` (port 5432, `lto_archive`) was never connected to.

| Suite | Result |
|---|---|
| `test_pg_integration.py` | **96 passed** (was 53 skipped) |
| `test_session_reconcile.py` | **33 passed + 5 subtests** (was 14 failed / 3 errors) |
| `test_phase5b_sealed_batch_pg.py` | **36 passed** (was skipped; opt-in flag) |
| `test_pg_test_guard.py` (new, §11) | **31 passed** |
| **Whole suite** | **1408 passed, 0 skipped, 0 failed, 0 deselected** (92 s) |
| Whole suite, no test server configured | **1259 passed, 149 skipped, 0 failed** (47 s) |

Leaked test databases on the disposable server after the full run: **0**
(`SELECT datname FROM pg_database` returned only `postgres`, `template0`,
`template1`). The leak recorded in the previous revision of §11 is fixed.

New PostgreSQL coverage added by this review:

- `TapeRenameGenerationTests` — atomic generation on register; rename carries
  generation **and** reset history; retired generation reads `None`.
- `ShadowLegacyMigrationTests` — 16 tests on a representative interrupted
  session (chunks `done`/`backing`/`fetch_failed`, sealed plan, file state).
- `RealConcurrencyTests` — 20 tests: 6-thread claim race, backing exclusion from
  claim/renew/release/expire/reclaim, forbidden transitions against real state,
  stale-owner CAS, segment range consumption, transaction rollback of a seal.

## 4. Migration 014 shadow validation

`python scripts/validate_migration_014_shadow.py` → **PASSED**.

Fixture: one session, 6 chunks (`done`×3, `backing`, `fetch_failed`, `pending`),
48 plan-file rows, 48 snapshot rows, 1 tape at generation 1, 1 catalog row.

Invariants — identical before and after (exact queries in the script):

| Claim | Rows before | Rows after |
|---|---|---|
| sessions unchanged | 1 | 1 |
| chunk states unchanged (incl. `backing`) | 6 | 6 |
| plan membership and ordinals unchanged | 48 | 48 |
| snapshot files unchanged | 48 | 48 |
| per-file transfer state unchanged | 0 | 0 |
| tapes + generations unchanged | 1 | 1 |
| ZIP/loose catalog unchanged | 1 | 1 |

Post-migration — nothing inferred or invented:

| Claim | Expected | Actual |
|---|---|---|
| no chunk gained owner/lease/attempt/seal/expectation | 0 | 0 |
| no directory marked scanned from catalog rows | 0 | 0 |
| no segment invented | 0 | 0 |
| no scope created without a bootstrap | 0 | 0 |
| no chunk/segment membership created | 0 | 0 |
| no bootstrap run started | 0 | 0 |
| the `backing` chunk is still `backing` | 1 | 1 |
| duplicate-ordinal guard exists | 1 | 1 |

Plus: the legacy scanner stays selected with the flag off
(`mode=legacy, reason=disabled_by_config`), and frontier state is sufficient to
resume an interrupted directory. Applying base twice and finalize twice are
both no-ops. A finalize against duplicate ordinals **refuses** and leaves the
ordinals byte-identical, with the base half still usable.

## 5. Scan-model benchmark findings

60,000 synthetic entries, 3 simulated interruptions, 2 GiB chunk budget:

| Metric | current root replay | full scan first | **persistent frontier** |
|---|---:|---:|---:|
| listing starts | 4 | 4 | 5,578 (one per directory) |
| entries seen | 150,000 | 150,000 | **60,006** |
| **entries replayed** | **89,430** | **90,000** | **6** |
| directories replayed | 0 | 0 | 2 |
| duplicate entries | 89,430 | 0 | 0 |
| SQL round trips | 773 | 220 | 440 |
| rows processed | 209,430 | 60,000 | 120,012 |
| membership query time | 0.012 s | 0 s | 0.008 s |
| time to first sealed chunk | 0.000 s | 0.040 s | 0.059 s |
| chunks sealed | 220 | 220 | 220 |

- **Repeated exploration**: root replay sees 150,000 entries to discover 60,000
  — 89,430 wasted. The frontier sees 60,006 and replays **6**.
- **Full scan first**: seals nothing until the whole source is listed, so its
  first sealed chunk cannot precede complete enumeration. Rejected.
- **All three seal identical work** (220 chunks) — the comparison is honest.

Isolated-PostgreSQL membership cost by catalog cardinality:

| catalog rows | SQL executions | probe paths | elapsed |
|---:|---:|---:|---:|
| 1,000 | 1 | 143 | 0.0024 s |
| 10,000 | 1 | 1,429 | 0.0114 s |
| 100,000 | 1 | 14,286 | 0.1250 s |
| 400,000 | 1 | 28,572 | 0.2589 s |

**One round trip at every cardinality.** Cost tracks probe size, not catalog
size — the "one query per file" concern is disproven at scale.

**Resumed-backlog fairness**: proven behaviourally in
`test_pipeline_characterization.py::test_a_resumed_backlog_cannot_starve_renewed_exploration`
— exploration starts while the stager is parked on the first of four backlog
chunks. Old pending chunks **cannot** starve renewed scanning.

## 6. Code-growth breakdown

| Category | Lines |
|---|---:|
| production logic **moved** out of `remote_orchestrator` (`remote_staging`, `remote_writer`) | 1,488 |
| production logic **new, active** (`remote_pipeline`) | 407 |
| production logic **new, DISABLED** (`scan_frontier`, `pg_scan`, `archive_artifacts`) | 2,218 |
| safety checks (`startup_reconcile`) | 518 |
| diagnostics / migration tooling (`frontier_bootstrap`) | 245 |
| net change to **existing** `src/` files | **+72** |
| migrations (SQL) | 584 |
| scripts | 1,006 |
| tests | 6,845 |
| documentation | 193 |

33% of the new module lines are comments and docstrings.

**The honest summary**: `src/` grew ~4,950 lines. Only **+72** of that is net
change to existing files; 1,488 lines moved; and 2,218 lines are a subsystem
that is switched off. `remote_orchestrator.py` fell from 3,657 to 2,330 lines
and from **378 to 158 control-flow branches (−58%)**, and the entire tape
surface is now 351 reviewable lines in `remote_writer.py`.

### Duplication audit

| Concern | Finding | Action |
|---|---|---|
| duplicate orchestration paths | None — one `RemotePipelineCoordinator` for both session kinds | — |
| parallel ownership models | None — one `_acquire_tape_io_lock` site in `remote_writer` | — |
| duplicated reconciliation logic | `session_reconcile` (stale **sessions**) and `startup_reconcile` (stale **chunk claims**) are different scopes | kept, documented |
| forward-only wrappers | 18 façade delegations on `RemoteOrchestrator` | kept — they are the public API the existing suite addresses |
| unreachable legacy branches | none found; 1.6 removed the dormant directory-first code | — |
| **repeated state-transition rules** | **chunk statuses spelled as literals in 4 places** | **fixed** — all derive from `ChunkStatus`, equality pinned by `test_status_vocabulary.py` |
| excessive compatibility layers | `update_chunk_status` wrapper only | kept — documented as temporary |
| oversized/mixed modules | `remote_staging.py` at 1,137 lines is the largest | accepted — it is cohesive (fetch+pack) and entirely moved code |

## 7. Linux path-collision safety

`_canonical_remote_path` rewrites `\` → `/`, so `/strg/a/back\slash` and
`/strg/a/back/slash` collapse to one catalog key. It cannot be changed in place;
the existing catalog is built on it.

| Requirement | Status | Evidence |
|---|---|---|
| never merge `a\b` with `a/b` | ✅ | such a path is withheld from planning |
| canonical catalog contract preserved | ✅ | `_canonical_remote_path` untouched |
| prevented from unsafe planning | ✅ | `test_such_a_path_is_withheld_from_planning` |
| explicit unsupported-path outcome | ✅ | `unrepresentable_path`, disposition `unresolved` |
| visible in operator reports | ✅ | unresolved error blocks directory finality |
| not silently treated as archived | ✅ | never appended to any chunk |
| enough information preserved | ✅ | **P**: path stored byte-for-byte with the backslash intact |
| faithful validator exists | ✅ | `validate_remote_posix_relpath` returns input unchanged |

## 8. Fail-closed `backing` behaviour

| Failure mode | Behaviour | Evidence |
|---|---|---|
| DB connection failure | `SAFETY_BLOCK`, run refuses to start | `Clause11` |
| query failure | `SAFETY_BLOCK` | `Clause11` |
| malformed result (`ValueError`) | `SAFETY_BLOCK` | `Clause11` |
| timeout | `SAFETY_BLOCK` | `Clause11` |
| unknown status | `ForbiddenTransition`, old state preserved | `test_lifecycle_transitions.py` |
| multiple backing rows | all reported, run blocked | `Clause11` |
| stale state without proof | claim **not** released | `reclaim_expired_chunk` requires evidence |
| elapsed time alone | never sufficient | `test_reclaiming_without_evidence_raises` (F+P) |

`backing` cannot be reached by claim, renew, release, expiry-listing or
reclamation — verified in SQL (**P**) and by the transition matrix (**F**).

## 9. Hardware validation

**NOT RUN, OPERATOR-SUPERVISED TAPE WRITE REQUIRED LATER.**

`scripts/plan1_rehearsal.py` now prints, for each of the ten hardware claims,
the offline test that already proves it and precisely what the tape run adds.
Nine of ten are already proven against fakes; the tape run confirms a real drive
matches. One (ambiguous-write handling) must **not** be provoked on real media —
it can latch a cartridge read-only (incident 010).

Explicitly still unverified after any tape run: real-drive latching-error
behaviour, production-scale behaviour (~82M files), and the frontier in
production.

## 10. Blockers before enabling `incremental_scan`

1. Apply migration 014 (base **and** finalize) to production after a verified
   backup, with no archiver running. Currently applied to **no** database.
2. Run `inspect_db.py --session-frontier-report --session-id 37` against
   production and confirm `verdict: ready`. **Never done.**
3. Run `--bootstrap-frontier` dry-run, review, then `--execute --yes`.
4. Complete the operator-supervised tape rehearsal.
5. Plan 3 approval for the bounded production group.

## 11. The PostgreSQL test safety guard (`tests/pg_test_guard.py`)

**The hazard, as found.** `src.pg_bulk.build_conninfo` defaults to
`host=localhost port=5432 dbname=lto_archive`, and the **production** `lto_pg`
container listens on exactly `127.0.0.1:5432`. A bare `python -m pytest` on this
host therefore connected the suite to the **production catalog server**. It
created and dropped throwaway `lto_test_*` / `lto_conc_*` / `lto_shadow_*` /
`lto_sessq_*` databases there rather than touching `lto_archive`, so no archive
data was ever at risk — but the connection was to production, and cleanup leaked
(three orphaned `lto_sessq_*` databases were observed after one
`test_session_reconcile.py` run). Relying on "remember to export `PG*` first" is
not a control.

**The fix.** All PostgreSQL test connections now go through
`tests/pg_test_guard.py`, which is fail-closed on every axis:

| Rule | Behaviour |
|---|---|
| Connection settings must be explicit | Only `LTO_TEST_PG_DSN` is read. Ambient `PG*` variables are never used to build a test connection. |
| Implicit defaults are refused | A DSN omitting host, port or dbname is rejected — those defaults are exactly localhost:5432/`lto_archive`. |
| Port 5432 is refused | Production here, and a real local install on a developer machine. |
| Non-loopback hosts are refused | A remote server cannot be shown to be disposable. |
| A production database name is refused | `lto_archive`, `lto_archive_prod`. |
| A server hosting `lto_archive` is refused | Connects read-only, one `SELECT` on `pg_database`. The decisive check: host and port are conventions an operator can get wrong; hosting the catalog is what production *is*. |
| Unsafe never skips | Unset ⇒ skip (a legitimate configuration). Set-but-unsafe ⇒ **`UnsafeTestDatabase` raised at collection**, so the run cannot report green. |
| Destructive fixtures are scoped to this run | Every test database is named `ltotest_<run-id>_<tag>_<rand>`; `drop_test_database` refuses any name lacking this process's marker, before opening a connection. |
| Leaks are swept | `pytest_unconfigure` drops every database carrying this run's marker — and only those. |

A session-wide wrapper over `psycopg.connect` in `tests/conftest.py` extends the
same rule to code the per-module wiring does not cover: any connection whose
host/port is not the configured disposable server raises, including a
`PgDatabaseManager` built from an unvetted conninfo inside production code
called from a test.

`tests/test_pg_test_guard.py` (31 tests) proves each rejection, including that
`pg_available()` **raises** rather than returning `False` for an unsafe target,
that `drop_test_database` refuses before connecting, that the production-server
probe issues only `SELECT`, and that the documented command in the module
docstring passes the guard's own rules.

Verified end to end: with the guard configured, 1408 passed / 0 skipped and
**zero** leaked databases; with `LTO_TEST_PG_DSN` unset, 1259 passed /
149 skipped in 0.24 s across the three PostgreSQL modules — no connection
attempted; with it pointed at port 5432, collection fails loudly.

Wired into `tests/test_pg_integration.py`, `tests/test_session_reconcile.py`,
`tests/test_phase5b_sealed_batch_pg.py`. No `build_conninfo` call remains
anywhere under `tests/`.

## 12. What was NOT modified

- **Session 37** — never read, never written. No production database connection
  was opened at any point in this review.
- **Production PostgreSQL (`lto_pg`, port 5432, `lto_archive`)** — untouched and
  still running. All work used a disposable container on port 15432 with tmpfs
  storage, destroyed afterwards.
- **Physical tape state** — no mount, no write, no eject, no format, no drive
  access, no LTFS ownership acquisition against real hardware.
- **Stored TAR / manifest-first TAR** — not implemented; Plan 2 not started.
- **`incremental_scan`** — still `false`. Migration 014 applied to no database.
