# Plan 1 — Core Simplification and Incremental Scan

## Execution contract

1. Implement this plan before Plans 2–4.
2. Keep Stored TAR creation disabled throughout this plan.
3. Do not prune PostgreSQL rows in this plan.
4. Make the finite write-group sequence the only active tape path: select a finite ready group, acquire LTFS ownership once, run readiness and cartridge verification once, write consecutive chunks, then release ownership. Preserve the correct core of `RemoteOrchestrator._write_chunk_group`, while removing current pre-group probes from `_start_new_session()`, `_run_streaming_session()`, and `_run_session()`.
5. Enforce the idle rule across the full lifecycle: no LTFS access before a finite group is ready, while waiting for future chunks, between group members, or after release. Disable remote-pipeline auto-eject paths.
6. Treat `remote_chunks.status='backing'` as ambiguous and never reset it automatically.
7. Treat the Session 37 facts in `AGENTS.md` as the current written baseline, but require a later read-only database/forensics report before relying on any chunk number or state.

## Phase 0 — Freeze the current behavioral contract and measure it

### Task 0.1 — Add a current-flow characterization map

- **Change:** Add concise developer documentation beside the relevant modules and characterization tests that pin both entry flows: `run.py` → `src.cli.main()` → `run_remote_archiver()` and `src.cli.run_remote_archiver_headless()` → `RemoteOrchestrator.run()`. Map new-session/incomplete-scan `_run_streaming_session()` and scan-complete `_run_session()` through scan/planning, fetch, ZIP/loose container handling, tape write, catalog, and restore.
- **Current facts to preserve in the characterization:**
  - `RemoteOrchestrator._start_new_session()` calls `PgSessionMixin.create_remote_streaming_session()` and creates growable `remote_snapshots`/`remote_plans` rows with `remote_sessions.scan_complete=FALSE`.
  - `_run_session()` sends every incomplete session back through `_run_streaming_session()`.
  - `_scanner_planner()` queues all existing non-`done` chunks before new exploration; the bounded queue can postpone renewed scanning behind a large pending backlog.
  - `StreamingRemoteScanner.iter_scan()` runs `find <root> -type f -printf '%s %p\0'` from every configured root on every incomplete-session run.
  - `ConfigManager.remote_scan_mode`, `DirectoryFirstRemoteScanner`, and `DirectoryUnitPlanner` are not wired into the production new-session path; their presence does not mean directory-first continuation exists.
  - `StreamingChunkBuilder` selects boundaries in discovery order using `ChunkPlanner.footprint()`, the current dynamic byte budget, and `chunk_max_files`.
  - `_append_chunk()` filters known paths only after those rediscovered paths have influenced `StreamingChunkBuilder` boundaries. `_append_chunk()` and `PgSessionMixin.append_remote_streaming_chunk()` each perform a chunk-bulk membership query; this is not one SQL round trip per file, but the latter still inserts snapshot/plan rows with `executemany`.
  - Surviving exploration state is the session-wide `scan_complete`/`scan_error`, growing snapshot/plan file rows, chunk rows, and totals. The partial in-memory builder, current root/directory, traversal stack, continuation, empty directories, and directory finality do not survive.
  - Duplicate membership currently relies on `UNIQUE(snapshot_id, remote_path)`, `UNIQUE(plan_id, snapshot_file_id)`, `PRIMARY KEY(session_id, chunk_index)`, and application filters. There is no membership seal or unique `(plan_id, chunk_index, ordinal)` constraint.
  - Recoverable `find` warnings can coexist with global `scan_complete`; no row proves a directory was fully scanned. A crash replays all roots and a source can change during the live traversal; same-size changes are not detectable.
  - Scanner, stager, and finite-group writer can overlap through `chunk_q` and `ReadyQueue`, but resumed pending chunks are queued first and can delay renewed scanning.
  - The current data boundaries are `src.packer.LTOPacker` for ZIP/loose metadata, `src.backup.LTOBackup` for Robocopy plus catalog sync, `src.pg_catalog.PgCatalogMixin` for file/directory rows, and `src.retriever.LTORetriever` for ZIP/loose restore.
- **Exact files/symbols:** `run.py`; `src/cli.py::main`, `run_remote_archiver`, `run_remote_archiver_headless`; `src/remote_orchestrator.py::RemoteOrchestrator`, `_start_new_session`, `_run_session`, `_run_streaming_session`, `_write_chunk_group`; `src/scanning.py::StreamingRemoteScanner`; `src/planning.py::StreamingChunkBuilder`, `ChunkPlanner`; `src/pg_sessions.py::PgSessionMixin.create_remote_streaming_session`, `append_remote_streaming_chunk`, `get_remote_existing_snapshot_paths`; `src/packer.py::LTOPacker`; `src/backup.py::LTOBackup`; `src/pg_catalog.py::PgCatalogMixin`; `src/retriever.py::LTORetriever`.
- **Database:** No schema change.
- **Dependencies:** None.
- **Tests:** Extend fake-only `tests/test_remote_hardening.py`, `tests/test_phase4_ready_queue.py`, `tests/test_phase45_control_signals.py`, `tests/test_staging_space.py`, and `tests/test_retriever_restore.py`; keep `test_queue_works_with_an_incomplete_scan`, `test_partial_scan_is_never_mistaken_for_a_complete_plan`, `test_packer_does_not_create_empty_zip_for_loose_only_batch`, `test_packed_and_unpacked_manifest_records_use_normal_restore_paths`, and `test_extracts_small_files_and_only_the_requested_subtree`. Separately extend isolated-PostgreSQL `tests/test_pg_integration.py::test_remote_streaming_session_appends_chunks_idempotently`.
- **Failure/recovery:** Fake characterization tests never reach configured staging, PostgreSQL, LTFS, or SSH. PostgreSQL characterization runs only against the isolated test database.
- **Acceptance gate:** One test trace shows scan publication preceding scan completion and prepared chunks reaching the writer while scanning continues; a resumed-backlog fairness test proves renewed scanning cannot be starved indefinitely by old pending chunks.
- **Rollback:** Documentation/test-only commit can be reverted without schema or runtime effects.

### Task 0.2 — Instrument and compare the three scan models

- **Change:** Add aggregate scan telemetry that measures root/directory enumeration time, entries seen, new entries, duplicate entries, PostgreSQL duplicate-query elapsed time and path count, SQL execution/round-trip count separately from rows processed, plan insert elapsed time and row count, repeated directory-listing-start events, discarded partial-buffer entries, time to first sealed chunk, time to first staged chunk, and time to first writer-ready group. Extend the existing backup row/columns in `backup_logs/SUMMARY.csv`; do not create a new record type or statistics file. The new metric fields contain no individual file or directory names; preserve the existing `source_host`/`source_path` backup columns. Add both an offline listing-replay harness and an isolated-PostgreSQL cardinality benchmark populated at increasing snapshot/plan sizes.
- **Models to compare:**

  | Model | Required experiment | Decision criterion |
  |---|---|---|
  | Current root replay | Resume an incomplete scan multiple times with an increasing known-path set | Establish repeat enumeration and database cost; do not infer one SQL round trip per file because current calls are chunk-bulk queries |
  | Full scan before processing | Persist a complete synthetic inventory before releasing any chunk | Reject if time-to-first-write or metadata footprint is worse without a compensating safety benefit |
  | Persistent directory frontier | Resume from committed directory/segment state while staging earlier sealed chunks | Select when it eliminates completed-directory replay and retains overlap |

- **Exact files/symbols:** `src/scanning.py::StreamingRemoteScanner`; `src/remote_orchestrator.py::RemoteOrchestrator._run_streaming_session`; `src/reporting.py::append_backup_summary_row`; `src/pipeline_types.py::StreamState`; new `scripts/benchmark_scan_models.py`.
- **Database:** No production metrics table and no per-file metric rows. The isolated benchmark creates/discards only test-database fixtures.
- **Dependencies:** Task 0.1.
- **Tests:** Add `tests/test_scan_metrics.py`; update `tests/test_reporting_and_robocopy.py` for appended backup-row columns while asserting `record_type`/`operation` remain `backup`, counters do not change control flow, and no source filename appears. Add an isolated-PostgreSQL benchmark smoke test that reports catalog cardinality, SQL executions, rows, and elapsed time separately.
- **Failure/recovery:** Metrics failures are non-fatal and must not alter scan, tape, or session state.
- **Acceptance gate:** The benchmark report separately reports exploration, database membership work, replay work, and time-to-first-chunk for all three models.
- **Rollback:** Remove the counters/harness without changing persistence or orchestration.

### Task 0.3 — Record the target decision

- **Change:** Adopt the persistent incremental directory frontier unless Task 0.2 disproves either restart safety or overlap. Retain full-scan mode only as an offline diagnostic, not the production prerequisite. Add `ConfigManager.incremental_scan_enabled`, default false in code and `config.example.ini`; the existing `StreamingRemoteScanner` remains the production default through all Plan 1 implementation/rehearsal gates.
- **Rationale that must be asserted by tests, not prose:** current root replay is recovery by visited-file replay; no tape invariant requires it; a persisted frontier can publish chunks before global scan completion.
- **Exact files/symbols:** `src/config.py::ConfigManager`; `config.example.ini`; `src/remote_orchestrator.py::RemoteOrchestrator._run_streaming_session`; new `src/scan_frontier.py::RemoteScanCoordinator`.
- **Database:** The flag cannot activate unless migration 014 and its finalized constraints validate; it does not backfill or migrate a session implicitly.
- **Dependencies:** Tasks 0.1–0.2.
- **Tests:** Default-off, malformed-config fallback, missing/drifted-schema refusal, explicit test-only activation, and legacy-default flow.
- **Failure/recovery:** Any configuration/schema uncertainty keeps the legacy scanner active for a new session and blocks activation for a session already explicitly bound to frontier state; never mix two scanners for the same active frontier.
- **Acceptance gate:** The chosen model has measured time-to-first-chunk, bounded restart replay, and no completed-directory re-enumeration.
- **Rollback:** Turn off the new frontier for future/unmigrated sessions; keep the legacy scanner as default. A session already publishing frontier state uses explicit rollback/reconciliation from Task 4.2 rather than silently switching scanners.

## Phase 1 — Establish focused module and state boundaries

### Task 1.1 — Extract scan/frontier coordination without changing behavior

- **Change:** Keep `RemoteOrchestrator` as the public façade, but move its nested scanner/planner coordination into `src/scan_frontier.py::RemoteScanCoordinator`. Keep remote listing/parsing in `src/scanning.py`, chunk sizing in `src/planning.py`, and database operations behind `PgDatabaseManager`.
- **Exact files/symbols:**
  - Move the logic currently nested in `RemoteOrchestrator._run_streaming_session::_scanner_planner` and `_append_chunk` into `RemoteScanCoordinator.run()` and `publish_legacy_chunk()`.
  - Add scan/frontier dataclasses and string-backed enums to `src/pipeline_types.py`: `ScanCoverageState`, `ScanDirectoryState`, `ScanSegmentState`, `ScanScope`, and `ScanSegmentRef`.
  - Preserve exports from `src/orchestrators.py` and callers in `src/cli.py`.
- **Database:** None in this task.
- **Dependencies:** Phase 0 characterization tests.
- **Tests:** Move no test responsibility silently. Add `tests/test_scan_frontier.py` for the coordinator and keep existing orchestration tests unchanged.
- **Failure/recovery:** The extraction must preserve cancellation through `src.runtime.CANCEL`, existing queue closure, and error propagation.
- **Acceptance gate:** Existing fake-based scan/queue tests pass with identical state transitions, and `RemoteOrchestrator._run_streaming_session()` reads as pipeline wiring rather than a second scanner implementation.
- **Rollback:** Revert the extraction while retaining characterization tests.

### Task 1.2 — Extract staging and writer coordination in two behavior-preserving steps

- **Change:**
  1. Move `_fetch_chunk`, `_fetch_one_batch`, `_fetch_batches_parallel`, `_stage_chunk`, `_preserve_desc`, `_try_resume_pack`, and their local cleanup helpers into `src/remote_staging.py::RemoteChunkStager`.
  2. Move `_write_chunk_group`, `_write_one_chunk_owned`, and capacity admission into `src/remote_writer.py::RemoteChunkWriter`; continue delegating the actual copy/catalog operation to `src.backup.LTOBackup`.
- **Exact files/symbols:** `src/remote_orchestrator.py::RemoteOrchestrator`; new `src/remote_staging.py::RemoteChunkStager`; new `src/remote_writer.py::RemoteChunkWriter`; `src/pipeline_types.py::StagedChunk`; `src/backup.py::LTOBackup`; `src/remote_transport.py::_remote_tar_fetch`.
- **Database:** No schema change; call the existing `PgDatabaseManager` façade.
- **Dependencies:** Task 1.1.
- **Tests:** Preserve `tests/test_remote_failure_hardening.py`, `tests/test_phase4_ready_queue.py`, `tests/test_phase45_control_signals.py`, `tests/test_staging_space.py`, and `tests/test_packer_on_existing.py`. Update patch targets to the module where each symbol is used.
- **Failure/recovery:** Do not alter protected-Robocopy cancellation, backing ambiguity, preserved `_resume_pack.json`, finite-group ownership, or no-idle-LTFS behavior.
- **Acceptance gate:** `test_no_ltfs_access_while_the_queue_is_filled`, `test_no_device_work_between_chunks`, `test_later_failure_preserves_earlier_successes`, `test_forced_interruption_leaves_chunk_ambiguous_backing`, and `test_cancel_during_active_write_finishes_and_commits_done` retain their behavior.
- **Rollback:** Revert each extraction independently; do not combine extraction with Stored TAR or catalog redesign.

### Task 1.3 — Consolidate complete and incomplete sessions into one pipeline loop

- **Change:** Replace the separate scheduling in `_run_streaming_session()` and scan-complete `_run_session()` with `src/remote_pipeline.py::RemotePipelineCoordinator`. Feed it persisted pending chunk identities plus an optional `RemoteScanCoordinator`; use one `RemoteChunkStager`, one `ReadyQueue`, and one `RemoteChunkWriter`. Before migration 014 exists, scanner publication uses a configured in-memory backlog limit and derives the current sealed-but-unstaged count from authoritative chunk status on every scheduling decision, so old pending staging cannot indefinitely block renewed exploration. The coordinator selects work in authoritative index/order; durable claims arrive only in Task 3.1.
- **Exact files/symbols:** `src/remote_orchestrator.py::RemoteOrchestrator._run_streaming_session`, `_run_session`; new `src/remote_pipeline.py::RemotePipelineCoordinator`; `src/scan_frontier.py::RemoteScanCoordinator`; `src/remote_staging.py::RemoteChunkStager`; `src/ready_queue.py::ReadyQueue`; `src/remote_writer.py::RemoteChunkWriter`.
- **Database:** No new schema in this task. One coordinator owns current state transitions; scanner completion remains independent of chunk completion. Derive the sealed-but-unstaged backlog from authoritative `remote_chunks` rows and keep the limit in configuration rather than persisting a second counter that can drift. Task 3.1 later adds claims after migration 014 is available.
- **Dependencies:** Tasks 1.1–1.2.
- **Tests:** Existing incomplete and complete session fixtures must traverse the same coordinator. Add resumed-backlog fairness, scanner optional, producer failure, final partial group, safe cancellation, and ordering tests.
- **Failure/recovery:** A scanner failure stops new publication but preserves sealed chunks; a stager/writer failure stops the pipeline per existing severity rules; neither loop invents a second transition path.
- **Acceptance gate:** There is one producer/stager/ReadyQueue/writer orchestration loop and no group-of-one legacy bypass.
- **Rollback:** Keep a compatibility adapter that feeds legacy complete plans into the same coordinator; do not restore duplicate scheduling loops.

### Task 1.4 — Make finite-group ownership the only tape-access path

- **Change:** Remove active drive/volume/readiness/cartridge calls from `_start_new_session()`, `_run_streaming_session()`, `_run_session()`, skip-tape completion, and post-group idle/completion paths. Add optional `[REMOTE] tape_label` exposed as `ConfigManager.remote_tape_label`; for an interactive new session, `_prompt_remote_tape_label()` asks for the intended label when that setting is blank, before any device access. Preserve the current promptless rule in `_run_non_interactive()`: a headless invocation without `--resume` fails before scanning or staging whether the setting is blank or populated; it never prompts, reads the mount, or infers a target. A resumed session always uses its persisted label/generation and never the current config. Validate the chosen label as a nonempty legal catalog volume label, show it in `_confirm_start()`, and register an absent catalog tape only through the existing explicit capacity prompt. Make `PgTapeMixin.register_tape()` atomically create both the `tapes` row at generation 1 and its matching active `tape_generations` row. At session creation, pin `tapes.current_generation` exactly as `PgSessionMixin._upsert_remote_session()` does today and refuse a missing/null or non-active generation; never infer or advance a generation from the mount. Verify the physically mounted label only after a finite group is ready and Global LTFS ownership is held. Disable/refuse `eject_after_session` for the remote pipeline.
- **Exact files/symbols:** `src/config.py::ConfigManager.remote_tape_label`; `config.example.ini` `[REMOTE] tape_label`; `src/remote_orchestrator.py::_run_non_interactive`, `_start_new_session`, `_resolve_tape_label`, new `_prompt_remote_tape_label`, `_confirm_start`, `_run_streaming_session`, `_run_session`, `_write_skip_tape_chunk`, `_eject_after_session`, `_verify_mounted_cartridge`, `_verify_session_tape_generation`; `src/pg_sessions.py::PgSessionMixin._upsert_remote_session`; `src/pg_tapes.py::PgTapeMixin.register_tape`; `src/ltfs.py::_ensure_lto_drive_ready`; `src/ltfs_ownership.py`; `src/remote_writer.py::RemoteChunkWriter.write_chunk_group`; `src/backup.py::LTOBackup`.
- **Database:** Session target tape label and current generation are explicit operational state. New-session registration/capacity selection is catalog-only and atomically establishes the matching active generation; a missing, null, or non-active catalog generation blocks creation/resume. `_verify_session_tape_generation()` compares the persisted session generation with `tapes.current_generation` using PostgreSQL only and may run without tape ownership. The owned physical gate verifies the mounted volume label once per finite group; it does not claim to observe a catalog generation. Neither check uses `Test-Path`/mount availability during idle.
- **Dependencies:** Task 1.3.
- **Tests:** Full-lifecycle fake traces from interactive new-session startup, headless fresh-start refusal with blank/populated configured label, complete/incomplete resume, empty/all-source-missing session, queue wait, finite group, post-group idle, completion, cancellation, and hard failure. Add atomic tape/generation registration and null/retired/mismatched-generation refusal cases. Assert the generation comparison is database-only, exactly one physical readiness/cartridge-label check occurs per group, zero tape calls occur outside ownership, and there is no remote eject, inter-chunk probe, or later group after hard failure.
- **Failure/recovery:** Read-only/write-protect/servo/SCSI timeout/ownership loss/cartridge mismatch/LTFS instability/hard Robocopy failure stops immediately; preserve packs/logs/dumps and last success; do not retry, eject, remount, format, or run `ltfsck`.
- **Acceptance gate:** `TapeOperationLog`/`FakeLtfsAdapter` proves every active tape call occurs inside one finite owned group and nowhere else.
- **Rollback:** Retain the fail-closed no-idle/no-eject behavior; rollback may route future work to the legacy ZIP stager but may not restore out-of-group tape probes.

### Task 1.5 — Centralize persisted lifecycle transitions

- **Change:** Add string-backed `SessionStatus`, `ChunkStatus`, `FileTransferStatus`, and `MembershipState` enums plus an allowed-transition matrix. Replace scattered string updates with one compare-and-swap repository transition method that validates owner/attempt, timestamps the transition, and writes/clears existing `remote_chunks.error_msg` deliberately. Keep `update_chunk_status()` only as a compatibility wrapper until all callers migrate.
- **Exact files/symbols:** `src/pipeline_types.py`; `src/pg_sessions.py::PgSessionMixin.update_chunk_status` and new `transition_chunk`; `src/remote_pipeline.py`; `src/remote_staging.py`; `src/remote_writer.py`; `src/session_reconcile.py`.
- **Database:** First centralize current status strings and use existing `remote_chunks.error_msg`; Task 2.1 then expands `remote_file_state.status` for `source_permission_denied`, `source_unreadable`, `source_changed`, and `unresolved` plus claim/membership fields. Define future `expected_bytes` as logical planned source bytes, distinct from Plan 2 actual staged bytes. Preserve explicit error category/message without putting file names in summary statistics.
- **Dependencies:** Task 1.3.
- **Tests:** Table-driven current/future allowed/forbidden transitions, error persistence, done cleanup, source-outcome mapping, restart idempotence, and every existing cancellation/backing ambiguity test. Add stale owner/attempt CAS cases after Task 2.1.
- **Failure/recovery:** An unknown/forbidden transition fails closed and leaves the old state; `backing` has no automatic outbound retry transition.
- **Acceptance gate:** Every runtime session/chunk/file/membership transition has one typed owner and test matrix.
- **Rollback:** Compatibility wrapper reads the same persisted strings; never downgrade a richer source outcome or ambiguous writer state.

### Task 1.6 — Remove only verified dead scan/planning paths

- **Change:** Run a repository-wide import/call audit before deleting or retaining compatibility shims for `src.scanning.DirectoryFirstRemoteScanner`, `RemoteScanner`, `src.planning.DirectoryPlanUnit`, `DirectoryUnitPlanner`, and the unused `ConfigManager.remote_scan_mode`, `remote_scan_depth`, `directory_chunk_max_gb`, and `directory_chunk_max_files` properties. Do not reuse the dormant directory-first code without proving it avoids recursive duplicate walks and uses the real loose threshold.
- **Exact files/symbols:** `src/scanning.py`; `src/planning.py`; `src/config.py`; `src/orchestrators.py`; `config.example.ini`; references in `tests/test_remote_hardening.py`.
- **Database:** None.
- **Dependencies:** New frontier implementation must exist before removing the old production scanner.
- **Tests:** Delete tests only when the corresponding implementation is proven unreachable; migrate still-valid parser/path tests to the new scanner.
- **Failure/recovery:** Preserve public façade imports for one release if external callers cannot be disproved.
- **Acceptance gate:** Every retained scanner/planner has a production caller or an explicitly documented compatibility purpose; a before/after module/branch/line report shows `remote_orchestrator.py` materially reduced, no duplicate scheduling implementation, and no new wrapper-only module.
- **Rollback:** Restore the shim exports without restoring duplicate production paths.

## Phase 2 — Persist the minimum scan frontier

### Task 2.1 — Add migration 014 and explicit schema application

- **Change:** Add `scripts/sql/014_postgres_incremental_scan.sql` plus explicit `PgConnectionCore.apply_incremental_scan_schema()` and `inspect_db.py --apply-incremental-scan-schema --execute --yes`. Split base table/nullable-column creation from legacy membership audit/final constraint creation. Require a read-only preflight, verified PostgreSQL backup, exact database identity, no archiver process/advisory lock, and explicit execution; never resequence an ambiguous legacy plan automatically. Do not reuse migration number 010 and do not assume optional migrations 007 or 012 are installed.
- **Database:** Apply these additive changes:
  - `remote_scan_scopes`: `scan_scope_id`, `session_id`, `scope_ordinal`, `scope_kind` (`directory` or `file`), canonical `source_root`, coverage/final-observation state, timestamps, and unique `(session_id, scope_ordinal)`/`(session_id, source_root)` constraints.
  - `remote_scan_directories`: `scan_directory_id`, `scan_scope_id`, canonical path, parent path/ID, stable traversal ordinal, separate `listing_state` (`pending`, `scanning`, `partial`, `complete`, `error`, `invalidated`), traversal-only `subtree_coverage_state` (`provisional`, `final`, `error`, `invalidated`), and independent `planning_state` (`unplanned`, `partially_allocated`, `fully_allocated`, `blocked`), plus last committed segment, before/after/final source observations, direct file/byte/child counts, error count, attempt/owner fields, and timestamps; unique scope/path and scope/ordinal constraints. Add scope/session `planning_complete` separately from scan finality.
  - `remote_scan_segments`: directory/segment identity, stable scan-ordinal range, `next_unconsumed_ordinal`, local `JSONL.zst` locator, artifact version, recorded artifact size, file/byte counts, first/last canonical path, readiness state (`writing`, `ready`, `partially_consumed`, `consumed`, `invalidated`), and timestamps. `.part` paths are never persisted as ready locators.
  - `remote_chunk_scan_segments`: FKs to chunk and segment plus first/last scan ordinal and unique range identity. Consume only the segment row's locked `next_unconsumed_ordinal`, advance it transactionally, reject gaps/overlap, and mark `consumed` only after its entire ready range is allocated. One chunk may consume several segments and one segment may split across chunks without duplicate membership.
  - `remote_scan_errors`: compact exceptional-entry/directory records with scope/directory, category, path when representable, message, and disposition; do not create success rows per file.
  - `remote_worker_attempts`: owner token, session/chunk/directory, attempt kind, local PID and process creation identity, remote command token/process-group evidence, lease/heartbeat, and terminal cleanup state.
  - Add nullable-for-legacy `owner_token`, `lease_expires_at`, `attempt_id`, `membership_state`, `expected_file_count`, and logical-source `expected_bytes` to `remote_chunks`; new chunks require `building`/`sealed`. Expand `remote_file_state.status` with the richer source outcomes from Task 1.5.
  - Audit `remote_plan_files` for duplicate/mismatched chunk ordinals and only then create unique `(plan_id, chunk_index, ordinal)`. Fail and report ambiguous rows; do not auto-resequence them.
- **Exact files/symbols:** `scripts/sql/001_postgres_schema.sql` for current references only; new migration 014; `src/pg_core.py::PgConnectionCore`; new `src/pg_scan.py::PgScanMixin`; `src/pg_db.py::PgDatabaseManager`; `inspect_db.py`.
- **Dependencies:** Phase 1 boundary and a schema audit command that is read-only by default.
- **Tests:** Add migration unit tests plus isolated PostgreSQL tests in `tests/test_pg_integration.py` for preflight/backup/quiescence/confirmation, idempotent base apply, legacy null semantics, finalized constraints, claim compare-and-swap, atomic segment readiness/range consumption/chunk seal, duplicate ordinal audit refusal, and unchanged optional-migration behavior.
- **Failure/recovery:** Base DDL is additive, but legacy repair/final unique constraints are a separate explicit operation. Refuse feature enablement on missing/drifted/unfinalized schema. A failed transaction leaves the old scanner active and retains its audit report.
- **Acceptance gate:** Schema can be applied twice, old sessions remain readable, and no existing chunk is marked sealed or assigned ownership without an explicit backfill report.
- **Rollback:** Disable the feature and retain populated tables/columns/audit. A reviewed rollback may drop only an unused finalized index or empty frontier tables; restore legacy data from the verified backup rather than guessing a reverse resequence.

### Task 2.2 — Add local scan-segment artifacts

- **Change:** Add a versioned `scan-segment-v1` `JSONL.zst` writer/reader under the existing permanent local manifest root, outside staging and LTFS. Use namespaced subdirectories so operational artifacts cannot collide with migration-010 exports. Each record contains canonical source path, observed size, stable scan ordinal, source scope, directory identity, and entry/storage hint. Keep path/size/count/byte totals; do not require content hashes.
- **Exact files/symbols:** new `src/archive_artifacts.py::JsonlZstArtifactWriter`, `parse_jsonl_zst_artifact`; reuse `src/config.py::ConfigManager.local_manifest_archive_root` and `src/local_manifest_archive.py::validate_archive_root`; update the `[LOCAL_MANIFEST_ARCHIVE]` description in `config.example.ini`; use `src/paths.py` for containment checks.
- **Database:** Persist only a validated root-relative artifact locator, version, size/readiness, and aggregate counts in `remote_scan_segments`; resolve it against `ConfigManager.local_manifest_archive_root` at read time so metadata backups can relocate safely.
- **Dependencies:** Task 2.1.
- **Tests:** Add `tests/test_archive_artifacts.py` covering unique `.part`, flush/close, reopen/full parse, aggregate validation, atomic no-clobber publish, interrupted publication, conflicting final file, and path edge cases. Keep artifacts hashless.
- **Failure/recovery:** Orphan `.part` files are ignored as evidence; reconcile or remove them only when their owner is absent/expired. A ready artifact must be reusable idempotently after complete equivalence validation.
- **Acceptance gate:** A ready segment can reconstruct its exact ordered path/size list without PostgreSQL per-file rows.
- **Rollback:** Stop publishing new segments; preserve ready artifacts and database locators for forward recovery.

### Task 2.3 — Implement directory-boundary continuation

- **Change:** Replace whole-root recursive `find` with an immediate-directory work queue. Canonicalize and reject overlapping configured roots rather than coalescing silently. On resume, use persisted scope order when the configured set is identical and refuse an added/removed root; a reordered config only warns. Support a single-file scope as one explicit scope entry. Use one deterministic scan publisher in v1: scope ordinal, directory traversal ordinal, and bounded external canonical-path ordering determine entry ordinals; fetching/packing/writing still overlap independently.
- **Coverage rule:** Mark `listing_state='complete'` only when that directory's immediate listing/segments are valid. Mark traversal-only `subtree_coverage_state='final'` after every descendant listing is terminal, the final mutation observations agree, and no unresolved traversal/error state remains; segment allocation is deliberately irrelevant to this fact. Advance independent `planning_state` as ready segment ranges are sealed/consumed. Mark scan finality in one transaction when the directory queue is empty and all traversal subtrees are final; mark `planning_complete` only when every ready segment range is fully allocated. This lets operators distinguish “the source tree was explored” from “all discovered entries were assigned to plans.”
- **Final mutation sweep:** Before global finality, re-read lightweight source observation tokens for every covered directory. A changed token invalidates that directory and its ancestors and requeues the bounded subtree. Stable path/size/structure checks still cannot detect same-size content replacement; record that residual risk.
- **Continuation rule:** A crash may replay only the current `partial` directory, never a completed directory or all source roots. Already-ready segments are matched by stable canonical path/ordinal from local artifacts, not by PostgreSQL path lookups. If exact within-directory continuation cannot be proved, restart that directory and perform a deterministic artifact merge; do not guess an opaque GNU `find` cursor.
- **Exact files/symbols:** `src/scanning.py` add an immediate-child NUL-framed iterator and bounded deterministic ordering; `src/scan_frontier.py::RemoteScanCoordinator`; `src/remote_transport.py::_ssh_run`/streaming process helpers; `src/pg_scan.py::PgScanMixin.claim_next_directory`, `publish_scan_segment`, `complete_directory_listing`, `finalize_directory_subtree`, `finalize_scan_scope`.
- **Database:** Transactionally claim one directory, publish ready segment metadata, enqueue child directories, and advance directory state. Store no successful per-file scan rows.
- **Dependencies:** Tasks 2.1–2.2.
- **Tests:** Add `tests/test_incremental_scan_frontier.py` for root normalization, overlapping-root rejection, reordered-identical scope reuse, added/removed scope refusal, single-file roots, deterministic ordinals independent of worker timing, empty directory listing/subtree coverage, partial-directory restart, no completed-directory replay, final observation sweep/mutation invalidation, multiple roots, spaces/tabs/newlines/Unicode, literal Linux backslash handling, invalid UTF-8/error coverage, and cancellation at each commit boundary.
- **Failure/recovery:**
  - Directory membership/source changes invalidate finality and schedule that directory for reconciliation.
  - Size changes after planning become `source_changed`; deletion becomes `source_missing`; same-size content changes remain an explicitly documented residual risk.
  - Permission/unreadable errors leave the directory provisional/error-qualified; they cannot silently produce global final coverage.
  - PostgreSQL restart retries use idempotency keys; an ambiguous commit is reread before any re-publication.
- **Acceptance gate:** Kill/restart injection at every segment boundary resumes from the persisted directory frontier, publishes no duplicate membership, distinguishes immediate listing from recursive subtree finality, and never marks changed/incomplete/error coverage final.
- **Rollback:** For a session that has not published any frontier artifact/state, keep or return future exploration to the compatibility scanner. Once a session has published frontier state, stop new claims, drain already sealed legacy chunks, retain frontier records/artifacts, and require the explicit Task 4.2 reconciliation path; never switch that session back to root replay.

### Task 2.4 — Preserve legacy chunk publication while removing replay lookups

- **Change:** During Plan 1, keep `PgSessionMixin.get_chunk_files()` as the production planning source, but feed it from ready scan segments. Normal frontier progress must never call `get_remote_existing_snapshot_paths()` for every rediscovered file. For a migrated incomplete legacy session, use a one-time set-based comparison per imported segment against `remote_snapshot_files`, matching both canonical path and expected size, then record that segment as imported so it is never repeated. A path match with a different size is `source_changed`: preserve any existing sealed membership, block automatic replanning or duplicate append, keep the affected directory/scope coverage provisional, and require an explicit operator reconciliation that chooses retain-old, abandon, or create a separately approved later plan. It is never silently anti-joined away.
- **Exact files/symbols:** `src/scan_frontier.py::RemoteScanCoordinator.publish_legacy_chunk`; `src/pg_sessions.py::append_remote_streaming_chunk`; `src/pg_scan.py::import_legacy_scan_segment`; `src/planning.py::StreamingChunkBuilder` or its narrowed replacement.
- **Database:** Seal chunk membership atomically with expected count/bytes and the ready scan-segment reference. Do not allow later membership append to a sealed chunk. Allocate chunk indexes under the session lock and reread after ambiguous commits.
- **Dependencies:** Tasks 2.1–2.3.
- **Tests:** Extend `tests/test_pg_integration.py` for immutable sealed membership, ambiguous-commit retry, no duplicate ordinals, and one-time legacy anti-join. Add an end-to-end fake test showing scan, staging, and write overlap.
- **Failure/recovery:** If publication fails, leave the segment ready/unconsumed and allocate no replacement chunk until the transaction outcome is known. `source_changed` fails closed: retain the observation and old sealed membership, publish no replacement automatically, and require explicit reconciliation before planning or coverage can advance.
- **Acceptance gate:** A resumed frontier consumes each ready segment once; database membership work is proportional to newly published legacy chunks, not repeated source exploration.
- **Rollback:** Existing sealed DB-backed chunks remain consumable; disable new frontier claims and retain artifacts.

## Phase 3 — Make ownership, cancellation, and reconciliation explicit

### Task 3.1 — Add chunk claims without weakening writer ambiguity

- **Change:** Add compare-and-swap claims for scan publication and fetch/pack work. Keep the cluster-wide archiver advisory lock as the outer duplicate-process guard and LTFS ownership as the tape guard. A lease expiry may return pre-write fetch/pack work to retry only after process/staging evidence is reconciled; it must never reset `backing`.
- **Exact files/symbols:** `src/pg_sessions.py::PgSessionMixin.update_chunk_status`; new claim methods in `src/pg_scan.py` or `src/pg_sessions.py`; `src/remote_staging.py::RemoteChunkStager`; `src/remote_writer.py::RemoteChunkWriter`; `src/pg_core.py::PgConnectionCore.ARCHIVER_LOCK_KEY`.
- **Database:** Use `owner_token`, `lease_expires_at`, and `attempt_id`; add compare-and-swap predicates to state transitions. Persist writer-start evidence separately from claim expiry.
- **Dependencies:** Task 2.1.
- **Tests:** Add concurrent isolated-PostgreSQL tests for single claimant, expired pre-write lease, lost advisory-lock connection, stale owner refusal, and permanent backing ambiguity.
- **Failure/recovery:** Loss of the advisory-lock connection stops new work; it does not assume another worker is absent. Existing protected Robocopy finishes under current policy, then state is reconciled conservatively.
- **Acceptance gate:** Two workers cannot stage or publish the same chunk, and no timeout can turn an ambiguous tape write into a retry.
- **Rollback:** Disable new claims only after returning all non-writer claims to a neutral state; never mutate backing rows during rollback.

### Task 3.2 — Reconcile frontier, artifacts, staging, and child processes at startup

- **Change:** Add a no-tape startup reconciliation pass that inventories database claims, durable `remote_worker_attempts`, local metadata `.part` files, ready artifacts, `_resume_pack.json`, and local/remote SSH/find/tar process evidence. The in-memory `src.runtime` registry is only current-process cleanup, not post-crash proof: match a local PID with process creation time and unique attempt token, and tag the non-detached remote process group with the same token. If the remote host is reachable, use a read-only token/process check and terminate only the proven orphan; if it is unreachable or identity is uncertain, keep the claim blocked. Keep `src/session_reconcile.py` as the explicit operator path for stale session classification. Change `RemoteOrchestrator._detect_prior_backing_chunks()` and `_chunk_backing_from_prior_run()` to fail closed when their database read is indeterminate; the current warning-and-proceed behavior is not safe.
- **Exact files/symbols:** new `src/startup_reconcile.py`; `src/session_reconcile.py::_classify`; `src/remote_orchestrator.py::_detect_prior_backing_chunks`, `_chunk_backing_from_prior_run`; `src/runtime.py::register_proc`, `unregister_proc`, `_kill_proc_tree`; `src/remote_transport.py`; `src/scan_frontier.py`; `src/remote_staging.py`.
- **Database:** Reconciliation may release only provably stale pre-write claims and records process cleanup evidence in `remote_worker_attempts`; it may mark scan artifacts invalid/provisional. It may not classify a session or writer outcome without the existing strict evidence.
- **Dependencies:** Tasks 2.2–3.1.
- **Tests:** Add crash-injection cases for scan `.part`, ready artifact before DB commit, DB commit before queue publish, partial directory, matching/reused PID, matching/missing/indeterminate remote token, fetch child termination, packing interruption, staging full, database restart, and cancellation. Extend `tests/test_session_reconcile.py` for frontier evidence and retain transient/backing refusal.
- **Failure/recovery:** Refuse startup on conflicting ready artifacts, duplicate published identities, unknown active owners, or ambiguous writer results. Never touch LTFS during reconciliation.
- **Acceptance gate:** Repeated reconciliation is idempotent and all recovery decisions cite local/database evidence.
- **Rollback:** Disable automatic repair, retain a read-only report command, and leave ambiguous records unchanged.

### Task 3.3 — Fix scan-scope and source-change correctness

- **Change:** Persist selected roots as ordered scope rows instead of relying on `_remote_session_key()`'s newline-concatenated `remote_sessions.remote_path`. Validate containment, duplicate/overlapping roots, and source-host identity. Define remote Linux path preservation separately from Windows storage-path normalization so literal backslashes are not silently rewritten for new artifacts. Add `src.paths.validate_remote_posix_relpath()` as the new byte/character-faithful validator; retain `_safe_remote_relpath()` only as an explicitly legacy compatibility path until callers migrate.
- **Exact files/symbols:** `src/remote_orchestrator.py::_remote_session_key`, `_validate_config`; `src/config.py::ConfigManager.remote_scan_paths`; `src/paths.py::_safe_remote_relpath` and new `validate_remote_posix_relpath`; `src/pg_sessions.py::_canonical_remote_path`; `src/pg_catalog.py::PgCatalogMixin._normalize_file_records`; new `src/scan_frontier.py` path contract.
- **Database:** `remote_scan_scopes` becomes authoritative for new scanner scope; keep `remote_sessions.remote_path` unchanged for legacy identity compatibility.
- **Dependencies:** Task 2.1.
- **Tests:** Add identical-set reorder compatibility using persisted order, overlapping-root rejection, added/removed/root-rename refusal, single-file scope, backslash, newline, Unicode, invalid UTF-8, and source mutation cases.
- **Failure/recovery:** Scope drift blocks continuation until an explicit reconcile/import operation; it never silently expands or shrinks source coverage.
- **Acceptance gate:** Restart uses persisted scope order when the configured canonical set is identical and refuses any set/content drift with an actionable mismatch.
- **Rollback:** Re-enable legacy session-key lookup for old sessions; do not delete scope rows.

## Phase 4 — Validate Session 37 frontier bootstrap without changing Session 37

### Task 4.1 — Build a read-only Session 37 frontier/membership report

- **Change:** Add a report-only command that later reads the authoritative catalog, session/chunk states, plan membership, active owners/advisory locks, and local artifact/staging evidence without touching LTFS. It must report current `scan_complete`, selected-scope evidence, maximum existing chunk, membership counts/bytes, transient/ambiguous states, and whether any snapshot/plan is shared.
- **Exact files/symbols:** `inspect_db.py`; `src/session_reconcile.py`; `src/pg_sessions.py`; `src/directory_catalog_validation.py::archiver_lock_status`; new report helper in `src/scan_frontier.py` or `src/startup_reconcile.py`.
- **Database:** Read-only.
- **Dependencies:** Phases 1–3 implemented and offline-tested.
- **Tests:** Fake reports for the current written baseline, conflicting stale-doc states, incomplete scan, shared plan, active process, and ambiguous backing.
- **Failure/recovery:** Any indeterminate lock/process/staging or scope evidence produces `blocked`, not a guessed boundary.
- **Acceptance gate:** The report does not hardcode 112/113 or any historical Session 37 status.
- **Rollback:** Remove the report command; it creates no state.

### Task 4.2 — Define the one-time conservative frontier bootstrap

- **Change:** Plan an operator-approved bootstrap that imports existing Session 37 membership as immutable legacy-covered paths, creates persisted scope rows, and performs a controlled read-only source traversal to establish directory coverage. Existing path rows alone must not mark any directory complete. Unresolved/error directories remain provisional and queued.
- **Exact files/symbols:** `src/scan_frontier.py::RemoteScanCoordinator`; `src/pg_scan.py`; `src/pg_sessions.py::get_remote_existing_snapshot_paths`; artifact helpers from Task 2.2; `inspect_db.py` dry-run/execute command pair.
- **Database:** Bootstrap state must be transactional and resumable; record its run ID, source/session IDs, imported segment counts, and coverage status. Do not change chunk format in Plan 1.
- **Dependencies:** Task 4.1 reports a quiescent, reconcilable state.
- **Tests:** Rehearse against an isolated clone with missing roots, partial current inventory, overlapping roots, changed sizes, deleted paths, and restart during bootstrap.
- **Failure/recovery:** Existing sealed chunk membership wins. Rediscovered path-and-size matches are recorded as covered and never appended again; a same-path/different-size observation is `source_changed`, remains provisional, and is never silently suppressed. Uncovered paths become future chunks. A failed pre-activation bootstrap leaves legacy scanning available and coverage non-final.
- **Acceptance gate:** A shadow rehearsal proves no existing member is duplicated, no newly discovered path is skipped, and no directory becomes final without traversal evidence.
- **Rollback:** Before activation/publication, abandon the rehearsal and leave Session 37 on its legacy scanner. After any frontier-backed publication, stop and preserve frontier/artifacts for explicit reconciliation; never run both scanners or blindly return to root replay. Do not delete or rewrite completed work.

### Task 4.3 — Rehearse the frontier rollout without resuming Session 37

- **Change:** Execute the future implementation progression through full offline tests, isolated PostgreSQL tests, and a small synthetic hardware pilot using ZIP/loose behavior and one finite group. Review scan frontier, no-idle-LTFS, writer, restore, and catalog evidence. Keep `incremental_scan_enabled=false` in production and defer the one bounded Session 37 production group to Plan 3 after format/catalog changes are also ready.
- **Exact files/symbols:** all Plan 1 test files; `tests/lto_fakes.py::FakeLtfsAdapter`, `TapeOperationLog`, `TapeLockObserver`; `src/remote_pipeline.py`; `src/remote_writer.py`; `src/scan_frontier.py`.
- **Database:** Synthetic pilot uses an isolated catalog/session. Session 37 report/bootstrap remains dry-run against restored evidence only.
- **Dependencies:** Tasks 4.1–4.2 and all prior Plan 1 gates.
- **Tests:** Full offline suite, isolated migration/frontier suite, synthetic single-file/directory/error/restart dataset, one finite hardware group, and local ZIP/loose restore.
- **Failure/recovery:** A hard tape failure stops the pilot immediately under Task 1.4 rules. Preserve evidence; do not resume Session 37 or automatically recover hardware.
- **Acceptance gate:** Pilot/review evidence is complete and the production feature remains disabled pending Plan 3's bounded-group approval.
- **Rollback:** Disable the test/pilot flag; preserve pilot artifacts/catalog and no-tape diagnostics.

## Phase 5 — Documentation, typing, and focused cleanup

### Task 5.1 — Document invariants at their owners

- **Change:** Add concise module/class/public-function documentation for scan-segment atomicity, chunk sealing, claim transitions, provisional/final coverage, finite write groups, no-idle-LTFS, backing ambiguity, artifact-root safety, and the path/size-only residual corruption risk. Update the stale `RemoteOrchestrator` docstring that mentions greedy bin-packing and a nonexistent `remote_manifest` table. If implementation creates a major source subdirectory, add one short README there that maps its public entrypoints and dependencies; keep the flat `src/` layout without a README when no new subdirectory exists.
- **Exact files/symbols:** `src/remote_orchestrator.py::RemoteOrchestrator`; new modules from this plan; `src/pipeline_types.py`; `src/pg_sessions.py`; `src/pg_scan.py`.
- **Database:** None.
- **Dependencies:** Final implemented module boundaries.
- **Tests:** Static import/type/syntax checks; documentation assertions only for safety-critical text already protected by tests.
- **Failure/recovery:** Documentation must not claim optional schema is installed or Session 37 live state was verified.
- **Acceptance gate:** Each state transition has one owning method and one documented source of truth.
- **Rollback:** Documentation follows code rollback; no separate runtime consequence.

## Plan 1 completion gate

Proceed to Plan 2 only when all software items below pass in offline and isolated-PostgreSQL environments and the separately identified synthetic hardware pilot has been completed and reviewed:

- [ ] Existing ZIP packing and restore regression tests pass unchanged.
- [ ] Existing loose large-file staging, indexing, and restore tests pass unchanged.
- [ ] Scanner, stager, and writer overlap is demonstrated end to end with fakes.
- [ ] Complete-plan and incomplete-scan sessions use the same pipeline coordinator and transition owner.
- [ ] A restart does not invoke remote enumeration for any completed directory.
- [ ] A crash in one partial directory repeats at most that bounded directory and never the entire scope.
- [ ] Completed directory coverage is durable, provisional/error coverage cannot become final, and empty directories are represented.
- [ ] Sealed chunks reject later membership changes; duplicate paths and duplicate ordinals are database-constrained.
- [ ] Normal frontier continuation does not use PostgreSQL as the per-file visited set.
- [ ] Source changes and scan errors produce explicit provisional/error state; same-size content changes are documented as undetectable without hashes.
- [ ] Chunk claims prevent duplicate workers; ambiguous `backing` remains blocked.
- [ ] Cancellation, PostgreSQL restart, staging exhaustion, SSH child cleanup, and orphan `.part` recovery are idempotent and no-tape.
- [ ] Full-lifecycle tests prove zero LTFS access before/after/between finite groups, exactly one owned readiness/cartridge gate per group, no remote auto-eject, and immediate stop after a hard tape failure.
- [ ] `RemoteOrchestrator` is a pipeline façade; scan, staging, and writer responsibilities have one implementation each.
- [ ] A source-size/control-flow report shows reduced mixed-responsibility and duplicate code; new modules own behavior rather than forwarding through unnecessary wrappers.
- [ ] A read-only Session 37 report and isolated bootstrap rehearsal exist; Session 37 itself has not been modified.
- [ ] The synthetic ZIP/loose hardware pilot is reviewed; `incremental_scan_enabled` remains false in production until Plan 3's bounded rollout.
- [ ] Stored TAR creation remains disabled and no PostgreSQL pruning has occurred.

### Plan 1 rollback gate

- [ ] Disable incremental-scan claims and future segment publication.
- [ ] Allow already sealed legacy DB-backed chunks to drain through the unchanged ZIP path.
- [ ] Preserve all ready scan artifacts and additive database state.
- [ ] Return future exploration to the legacy scanner only for unmigrated sessions that never published frontier state; preserve and explicitly reconcile every frontier-publishing session.
- [ ] Do not reset `backing`, delete coverage, rewrite chunks, or touch tape during rollback.
