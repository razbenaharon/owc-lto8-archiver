# Plan 3 — Manifest-First Directory Catalog and Session 37

## Execution contract

1. Start only after Plan 1 frontier/restart gates and Plan 2 reader/writer/local-restore gates pass.
2. Keep `PgSessionMixin.get_chunk_files()` available for every legacy DB-backed chunk.
3. Do not rewrite an existing ZIP or TAR container.
4. Do not activate a Session 37 transition without a quiescent read-only report, isolated rehearsal, persisted frontier, and explicit operator approval.
5. Do not prune rows in this plan; Plan 4 owns deletion.
6. Keep detailed artifacts in versioned `JSONL.zst`, use no mandatory content hashes, and record the same-size/same-structure corruption residual risk.
7. Rebuild, completeness, validation, and startup reconciliation must use local metadata and PostgreSQL only; never probe tape automatically.

## Phase 0 — Verify and repair the current catalog boundary

### Task 0.1 — Add an installed-schema/current-data audit

- **Change:** Add a read-only report that distinguishes repository DDL from schema actually installed in a selected database and reports current directory/container provenance quality.
- **Facts the report must test rather than assume:**
  - `catalog_directories` exists in base migration 001 and is a tape-specific navigation tree for `files_index`.
  - Optional migration 007 defines `directory_archive_stats`, `directory_archive_bundles`, and `directory_tree_index`; `PgConnectionCore._init_schema()` does not auto-apply it.
  - `directory_completeness` and a unified directory view do not exist in the current repository.
  - `docs/directory_completeness_feature_design.md` is explicitly unimplemented, and its proposed migration number 010 conflicts with the real `010_postgres_local_manifest_archive.sql`; none of its boolean/session-wide assumptions are schema truth.
  - `directory_tree_index` is a per-container contribution table, not one canonical row per directory.
  - `archive_bundles` and `directory_archive_bundles` are unlinked registries with independent `bundle_id` namespaces.
  - `PgCatalogMixin.bulk_upsert_directory_catalog()` currently receives no `remote_chunk_index` and writes `chunk_index=local_chunk_index`, so remote rows can have null/wrong chunk attribution.
  - Remote pack metadata can leave `directory_archive_bundles.original_dir_path` rooted at transient fetch staging, while item paths in `directory_tree_index` have been canonicalized.
  - Current ZIP manifest locators commonly point to tape after successful staging cleanup and are not local rebuild evidence.
- **Exact files/symbols:** `inspect_db.py`; `src/pg_core.py::PgConnectionCore._init_schema`, `apply_directory_catalog_schema`; `src/pg_catalog.py::PgCatalogMixin.bulk_upsert_directory_catalog`, `find_directory_restore_bundles`, `_derive_bundle_base_path`; `src/backup.py::LTOBackup._run_locked`; `src/packer.py::LTOPacker`; migrations 001 and 007.
- **Database:** Read-only report of table/column/constraint presence, null provenance, duplicate/conflicting contributions, local-vs-tape locators, and counts/bytes.
- **Dependencies:** Plans 1–2 complete.
- **Tests:** Add fake-schema and isolated-PostgreSQL cases to `tests/test_pg_integration.py`; cover 007 absent/present and each provenance defect.
- **Failure/recovery:** An absent optional table or unverified locator is reported as unavailable, never synthesized as proof.
- **Acceptance gate:** The report identifies every row set that can and cannot support Session 37 export, directory completeness, restore, and rebuild.
- **Rollback:** Report-only.

### Task 0.2 — Verify the Plan 2 provenance fix and audit historical rows

- **Change:** Verify that Plan 2 passes canonical source root, `remote_session_id`, and `remote_chunk_index` explicitly from the staged chunk through `LTOBackup` to `PgCatalogMixin.bulk_upsert_directory_catalog()`, never derives remote chunk identity from `local_chunk_index`, and never uses a local fetch directory as `original_root_dir`. Add only the historical dry-run audit/repair here. Also verify that Plan 2 restore routing has eliminated `_derive_bundle_base_path()`'s multiline `remote_sessions.remote_path` fallback in favor of persisted scan scopes.
- **Exact files/symbols:** `src/pipeline_types.py::StagedChunk`, `FileRecord`; `src/remote_staging.py::RemoteChunkStager`; `src/backup.py::LTOBackup._run_locked`; `src/pg_catalog.py::PgCatalogMixin.bulk_upsert_directory_catalog`; `src/packer.py::LTOPacker`; `src/db.py::_apply_canonical_remote_paths` as used by `src/remote_orchestrator.py` until fully extracted.
- **Database:** Add no destructive backfill. Create an explicit dry-run repair report for historical rows; only repair rows whose chunk/root can be proven by stable session/container/member evidence.
- **Dependencies:** Task 0.1 and Plan 2 Task 3.1/3.3 provenance work.
- **Tests:** Extend `tests/test_pg_integration.py::test_directory_catalog_counts_bundle_without_double_counting` and `test_directory_backfill_dry_run_and_execute_are_idempotent`; add remote chunk/root, multi-tape, and all-small-bundle cases.
- **Failure/recovery:** Conflicting historical provenance remains flagged and blocks completeness/pruning for affected directories; never guess from `_pack_sNNNN_CCC` alone.
- **Acceptance gate:** Every new contribution has canonical source path plus exact remote session/chunk/container/tape identity.
- **Rollback:** New fields remain additive; legacy reader continues to support old rows as incomplete evidence.

## Phase 1 — Publish immutable manifest-first chunk plans

### Task 1.1 — Add migration 016 for plan source, transition epochs, and artifact authority

- **Change:** Add `scripts/sql/016_postgres_manifest_directory_catalog.sql` with an explicit apply/validate path. Keep current tables and foreign keys intact.
- **Database:** Apply these planning-source and transition changes:
  - Use existing unique `remote_sessions.session_label` plus `remote_chunks.chunk_index` as the stable logical chunk identity in artifacts and canonical comparison; retain `(session_id, chunk_index)` as the database FK/operational key. Do not depend on generated numeric IDs surviving rebuild.
  - Add `plan_source` (`legacy_db` or `manifest`), `plan_manifest_artifact_id`, `terminal_manifest_artifact_id`, `plan_ordinal_scope='chunk'`, and final disposition aggregate columns to `remote_chunks`.
  - Backfill all existing chunks to `plan_source='legacy_db'`; do not change their format backfill from Plan 2.
  - Add `remote_session_plan_transitions` with session, transition epoch, state (`draft`, `rehearsed`, `approved`, `active`, `rolled_back`), prior/new plan source, last chunk before transition, first chunk after transition, scan-frontier generation, evidence/report locator, approval identity/time, and timestamps.
  - Add `remote_chunk_plan_source_transitions` as an append-only audit for the single permitted terminal authority migration `legacy_db` → `manifest`. Require a ready equivalent plan artifact, terminal/unowned chunk, Plan 4 gate/evidence IDs, and an atomic update of chunk source/artifact FK plus audit row. No `manifest` → `legacy_db` transition exists for an already created chunk.
  - Add uniqueness/checks preventing two active transition epochs at one next-chunk boundary and preventing locator/readiness changes that would mutate sealed membership.
- **Exact files/symbols:** new migration 016; add `src/pg_core.py::PgConnectionCore.apply_manifest_directory_catalog_schema`; `src/pg_db.py`; `src/pg_sessions.py`; `src/pg_scan.py`; `src/pg_containers.py`; new `src/pg_directory_catalog.py`; add `inspect_db.py --apply-manifest-directory-catalog-schema` and validation/report flags.
- **Dependencies:** Migrations 014–015.
- **Tests:** Isolated PostgreSQL tests for idempotent apply, stable session-label/chunk-index identity, legacy backfill, normal source immutability, the one audited terminal `legacy_db`→`manifest` authority transition, shared-plan refusal, transition state machine, boundary uniqueness, and coexistence with optional migrations 007/012.
- **Failure/recovery:** Feature startup fails closed if a manifest chunk lacks one ready plan artifact or a transition boundary is inconsistent.
- **Acceptance gate:** Existing sessions/chunks read identically through `legacy_db`; no artifact or format is inferred.
- **Rollback:** Set the default for future, not-yet-created chunks back to `legacy_db` through a new transition epoch. Never relabel existing manifest chunks.

### Task 1.2 — Define the three versioned chunk artifacts

- **Change:** Implement the following `JSONL.zst` schemas using one header record, ordered detail records, and one trailer/aggregate record. Version and validate every field; unknown required versions fail closed.

  | Artifact | Required detail fields | Publication point |
  |---|---|---|
  | `plan-manifest-v1` | canonical planned path, expected size, stable chunk-local plan ordinal, session label/chunk index, `provenance_kind` (`frontier` or `legacy_db_export`), conditional source mapping, storage class (`container` or `loose`), container format (`zip` or `stored_tar`), routing precision, and conditional container ordinal | Before a new chunk is sealed/published, or as a non-destructive legacy export |
  | `tar-sidecar-v1` | exact TAR member name, original canonical source path, expected/observed size, plan ordinal, container ordinal, chunk/container identity | After full local TAR parse; implemented in Plan 2 |
  | `terminal-state-v1` | plan ordinal/path plus one of `archived`, `source_missing`, `source_permission_denied`, `source_unreadable`, `source_changed`, `unresolved`; final evidence; observed archived size; and, for archived loose files, exact tape label, tape generation identity/value, stored path, archive date/run logical identity, source/session/chunk identity, and restore locator | After all source outcomes are known and writer/catalog completion is durable |

- **Plan-manifest conditional rules:** A frontier-created record requires source scan-segment identity/ordinal, `routing_precision='exact'`, and a non-null stable container ordinal for every container member. A loose record has null container format/ordinal. A legacy export instead requires `legacy_source_plan_file_id`, original legacy ordinal, normalized chunk-local ordinal, and source snapshot identity; its source scan-segment fields may be null because the current database cannot prove that provenance. A legacy container member may have a null container ordinal only with `routing_precision='coarse'`. Such a coarse artifact is schema-valid auxiliary evidence but cannot become authoritative, drive row-free restore, or qualify for pruning until a later exact-routing generation supersedes it.
- **Additional artifact discovery metadata:** Put stable session label/chunk index, persisted source scopes, format, artifact version, counts/bytes, container/tape label and generation locators where known, and prior artifact linkage in headers/trailers. Use a deterministic namespaced layout below `ConfigManager.local_manifest_archive_root`, organized by stable session label and chunk index; filenames include artifact kind, schema version, and generation/container ordinal. Publish append-only generations of a small `session-descriptor-v1.jsonl.zst` at session creation/update so an empty or partially planned session can be rebuilt without the source database.
- **Frontier rebuild artifacts:** Publish append-only `scan-state-segment-v1.jsonl.zst` generations from the same committed frontier evidence. Records preserve scan scopes; directory/parent/child queue topology including empty directories; listing, traversal-only recursive coverage, and independent planning state; partial continuation; before/after/final source observations; exclusions/errors; coverage finality; scan-segment locators and ordinal ranges; and every `remote_chunk_scan_segments` consumption range. The latest ready session descriptor names the complete ordered scan-state generation set.
- **Publication protocol for every artifact:** unique `.part` → flush/close/fsync → reopen and parse completely → validate path/order/count/byte totals and cross-artifact identity → atomic no-clobber final publication → persist locator/version/actual size/readiness. Never treat `.part` as evidence.
- **Exact files/symbols:** `src/archive_artifacts.py`; `src/pipeline_types.py`; `src/pg_containers.py`; `src/pg_scan.py`; new schema-specific helpers in `src/plan_manifest.py`, `src/terminal_manifest.py`, and `src/scan_state_manifest.py` only where keeping them separate makes the core artifact writer simpler.
- **Database:** `archive_artifacts` plus new chunk locator columns. Store local locators root-relative to `ConfigManager.local_manifest_archive_root`; a valid local locator is mandatory for readiness, while a tape locator is optional and never used for routine verification.
- **Dependencies:** Task 1.1 and Plan 2 sidecar implementation.
- **Tests:** Extend `tests/test_archive_artifacts.py`; add `tests/test_manifest_schemas.py` for version compatibility, order, duplicate ordinal/path, exact frontier provenance, legacy source-to-export ordinal mapping with null scan-segment identity, exact/coarse container routing and conditional null ordinal rules, coarse-authority refusal, cross-artifact mismatch, every terminal disposition/loose locator, scan-state generation gaps, empty/partial/error directories, traversal/planning-state independence, segment-consumption range mismatch, truncated Zstandard/JSONL, header/trailer mismatch, no-clobber retry, and `.part` exclusion.
- **Failure/recovery:** A plan artifact published before its DB transaction is adopted only after full validation; a DB row without a ready final artifact is not sealed. A terminal manifest is local durable evidence and is not automatically written to tape after the data write.
- **Acceptance gate:** The three artifacts independently parse and collectively prove the exact plan, TAR membership, final outcomes, and aggregate totals without content hashes.
- **Rollback:** Preserve all ready artifacts. Reader support remains even if future publication is disabled.

### Task 1.3 — Add a minimal `PlanSource` boundary

- **Change:** Add a narrow iterator interface, not a framework: `PlanSource.iter_chunk_entries(chunk_ref)` and `summary(chunk_ref)`. Implement `LegacyDbPlanSource` and `ManifestPlanSource` and select by persisted `remote_chunks.plan_source`.
- **Exact files/symbols:** new `src/plan_source.py::PlanSource`, `LegacyDbPlanSource`, `ManifestPlanSource`; `src/pg_sessions.py::PgSessionMixin.get_chunk_files`, `get_chunk_size_summary`; `src/archive_artifacts.py`; `src/remote_staging.py::RemoteChunkStager`.
- **Database:** Legacy adapter joins `remote_plan_files`/`remote_snapshot_files`; manifest adapter reads the ready local plan artifact. Neither adapter decides packaging format.
- **Dependencies:** Tasks 1.1–1.2.
- **Tests:** Add `tests/test_plan_source.py` asserting identical ordered entries/summaries from legacy and manifest fixtures, missing artifact refusal, duplicate membership rejection, and mixed adapters within one session.
- **Failure/recovery:** Plan source is immutable for active/nonterminal work and permanently immutable for a manifest-first chunk. The only exception is the audited terminal `legacy_db`→`manifest` authority migration defined in Task 1.1 and executed only after Plan 4 gates; it atomically installs the verified artifact authority for every affected plan reference before source rows are removed. If a local manifest is unavailable, a manifest chunk blocks and never falls back to a possibly stale database inventory.
- **Acceptance gate:** Staging code has no direct knowledge of snapshot/plan tables and processes either source through one typed stream.
- **Rollback:** A new transition epoch may assign `legacy_db` to future chunks; existing manifest chunks continue through `ManifestPlanSource`.

### Task 1.4 — Seal and publish new manifest-first chunks from the frontier

- **Change:** Implement the target flow: consume ready scan segments from Plan 1 → accumulate one bounded chunk → assign stable chunk-local plan ordinals and stable container ordinals → publish/validate the plan manifest → atomically insert the chunk with `plan_source='manifest'`, format, aggregate counts/bytes, artifact locator, and `membership_state='sealed'` → enqueue it → continue from the persisted frontier.
- **Exact files/symbols:** `src/scan_frontier.py::RemoteScanCoordinator`; `src/planning.py`; `src/plan_manifest.py`; `src/pg_scan.py`; `src/pg_sessions.py`; `src/ready_queue.py::ReadyQueue`; `src/remote_orchestrator.py::RemoteOrchestrator._run_streaming_session`.
- **Database:** Do not insert new small-file rows into `remote_snapshot_files` or `remote_plan_files` for a manifest chunk. Keep only chunk aggregates, source scope/frontier state, directory coverage, transition/source, artifact locators/readiness, and claims. Large files receive `files_index` rows only through the existing successful catalog path.
- **Dependencies:** Tasks 1.1–1.3.
- **Tests:** Add `tests/test_manifest_first_scan.py` for first-chunk-before-scan-complete, restart after artifact/DB/queue boundaries, no permanent small-file planning rows, stable ordinals, immutable membership, duplicate scan publication, source mutation, and DB restart.
- **Failure/recovery:**
  - An uncommitted partial chunk is rebuilt from ready scan segments.
  - A ready plan artifact with unknown DB outcome is looked up by stable `(session_label, chunk_index)` plus artifact-generation idempotency key before allocating another chunk.
  - A sealed chunk never gains members; late discoveries enter a later chunk.
  - Directory coverage remains provisional until every scope/directory is complete and error policy is resolved.
- **Acceptance gate:** New small-file chunks process before full scan completion without snapshot/plan file rows or per-file PostgreSQL visited checks.
- **Rollback:** Stop manifest chunk creation at a new transition boundary; preserve and finish already sealed manifest chunks.

### Task 1.5 — Publish terminal state without a second tape write

- **Change:** Collect source exceptions during fetch/TAR creation, then publish `terminal-state-v1` only when every plan ordinal has one final disposition and all writer/catalog states needed for `archived` are durable. Keep the final terminal artifact locally; include it in PostgreSQL/database-backup procedures, not an automatic post-write tape copy.
- **Exact files/symbols:** `src/remote_staging.py::RemoteChunkStager`; `src/remote_writer.py::RemoteChunkWriter`; `src/backup.py::LTOBackup`; `src/terminal_manifest.py`; `src/pg_containers.py`; `src/pg_sessions.py::update_chunk_status`.
- **Database:** Persist aggregate disposition counts and the ready terminal artifact locator. Do not delete `remote_file_state` exceptions until Plan 4 eligibility passes.
- **Dependencies:** Task 1.4 and Plan 2 writer/catalog states.
- **Tests:** Every outcome, all-source-missing no-tape chunk, copy-success/catalog-failure, backing ambiguity, terminal artifact crash points, and exactly-one-disposition-per-ordinal.
- **Failure/recovery:** `archived` is impossible before writer plus catalog completion. Ambiguous writer state leaves affected entries `unresolved` and blocks final readiness/pruning.
- **Acceptance gate:** A terminal manifest can be joined to the plan by ordinal with no missing/duplicate entry and matches chunk aggregates.
- **Rollback:** Keep local terminal artifacts; disabling new publication does not change completed outcomes.

## Phase 2 — Build the directory-first PostgreSQL catalog

### Task 2.1 — Add canonical directories, coverage, parts, and completeness

- **Change:** Extend migration 016 with normalized directory-level truth while preserving `catalog_directories`, `directory_tree_index`, `directory_archive_bundles`, and `directory_archive_stats`.
- **Database:** Apply these directory-catalog changes:
  - `archive_directories`: stable directory ID, source host, canonical path, parent directory ID, name, depth; unique source host/path.
  - `directory_scan_coverage`: directory/session/scope, coverage state (`provisional`, `final`, `error`), direct and recursive discovered file/byte/directory counts, excluded/error counts, frontier generation, and timestamps.
  - `directory_archive_parts`: many-to-many directory ↔ session/chunk/container-or-loose/tape-generation/storage-class contribution with direct expected/archived/outcome counts/bytes, plan/sidecar/terminal locators, local-validation state, writer state, catalog state, and restore-routing data. Add FKs to `archive_directories`, composite `remote_chunks`, `archive_containers`, `tape_generations`, and `archive_artifacts`; enforce exactly one container or loose-record identity. Add separate partial unique keys for `(directory_id, session_id, chunk_index, container_id, evidence_generation)` and `(directory_id, session_id, chunk_index, loose_record_key, evidence_generation)` so duplicate contributions cannot hide behind nulls. A directory may have many parts and a container may contribute to many directories.
  - `directory_completeness`: one row per session/directory with persisted expected/archive/exception aggregates; booleans for `scan_is_final`, `all_planned_items_terminal`, `all_required_items_archived`, `all_parts_written`, `all_writer_completions_succeeded`, `all_parts_cataloged`, and `all_local_validation_succeeded`; derived status; pinned frontier/artifact evidence generation and calculation time.
  - `directory_catalog_status_v`: canonical parent/child data plus relevant chunk indexes, containers, tape labels and generation identities, artifact locators, coverage/finality, aggregates, completeness status, and restore routes. Use arrays/JSON aggregates only in the view; keep base rows normalized.
- **Exact files/symbols:** migration 016; new `src/pg_directory_catalog.py::PgDirectoryCatalogMixin`; `src/pg_db.py`; `src/scan_frontier.py`; `src/remote_writer.py`; `src/backup.py`; existing `src/pg_catalog.py` compatibility readers.
- **Dependencies:** Phase 1 artifacts and corrected future provenance.
- **Tests:** Isolated PostgreSQL tests for one directory spanning chunks, ZIP/TAR/loose containers, multiple tapes, multiple sessions, empty directories, deep ancestors, duplicate contribution rejection, and idempotent recomputation.
- **Failure/recovery:** Contribution upserts use stable chunk/container/artifact identities. Conflicting counts or locators mark the directory ambiguous and block completeness.
- **Acceptance gate:** Every known directory is queryable even when it has no files, is still scanning, or spans several archive parts.
- **Rollback:** Keep compatibility reads from existing tables; do not delete new contributions or legacy aggregates.

### Task 2.2 — Define and persist directory status semantics

- **Change:** Compute status only after expected counts have been materialized from scan/plan artifacts and before any per-file rows used for those counts can be pruned.
- **Required status rules:**

  | Status | Required conditions |
  |---|---|
  | `provisional` | Source coverage is not final; archive progress may be partial or complete for currently known entries |
  | `complete` | Scan final; every planned entry terminal and archived; every required part locally validated, written, and cataloged; no source exceptions |
  | `complete_with_source_exceptions` | Scan final; every planned entry terminal; all non-exception entries archived/written/cataloged; only explicit missing/permission/unreadable/changed outcomes remain; no unresolved outcome |
  | `incomplete` | Scan final with known planned/unwritten/unvalidated/catalog-pending work and no writer ambiguity |
  | `ambiguous` | Conflicting evidence, unresolved disposition, unknown owner, or writer result that might have reached tape |

- **Deterministic precedence:** `ambiguous` overrides every other state; otherwise non-final source coverage is `provisional`; otherwise known planned/unwritten/unvalidated/catalog-pending work is `incomplete`; otherwise explicit source exceptions select `complete_with_source_exceptions`; otherwise the directory is `complete`.
- **Required independent booleans:** entire directory scanned; every planned item terminal; every planned item archived; all archive parts written; every writer completion successful; all parts cataloged; all local validations successful. Never collapse these into one count comparison and never accept `archived_count >= expected_count` as duplicate-safe proof.
- **Exact files/symbols:** `src/pg_directory_catalog.py::recalculate_directory_completeness`, `get_directory_status`; `src/pipeline_types.py::DirectoryBackupStatus`; `src/scan_frontier.py`; `src/terminal_manifest.py`.
- **Database:** Pin one frontier/artifact high-water generation, compute direct counts, recursive aggregates, booleans, and status from that snapshot in one transaction, then publish the generation. Store separate source outcome counts: missing, permission denied, unreadable, changed, unresolved.
- **Dependencies:** Task 2.1.
- **Tests:** Add `tests/test_directory_completeness.py` for every state transition, duplicate overcount, provisional-to-final, exception-qualified completion, multi-part writer failure, catalog-pending, and ancestor aggregation.
- **Failure/recovery:** Recalculation is idempotent. Any missing generation/artifact/contribution degrades to incomplete/ambiguous, never complete.
- **Acceptance gate:** Status answers each required question independently and remains stable after deletion of a synthetic per-file source table.
- **Rollback:** Recompute from preserved artifacts/contributions; retain the last known generation for audit.

### Task 2.3 — Integrate legacy ZIP, new TAR, and loose contributions

- **Change:** Add adapters that populate the same directory parts from:
  - existing `directory_tree_index`/`directory_archive_bundles` and `files_index` for legacy ZIP/loose data;
  - manifest-first plan, TAR sidecar, terminal manifest, `archive_containers`, and large-file `files_index` for new data.
- **Exact files/symbols:** `src/pg_directory_catalog.py`; `src/pg_catalog.py`; `src/local_manifest_archive.py`; `src/pg_containers.py`; `src/plan_source.py`.
- **Database:** Never force a directory into one bundle row. Record direct contribution identities and compute ancestors. Retain legacy rows if per-member/container routing cannot be proven.
- **Dependencies:** Tasks 2.1–2.2.
- **Tests:** Legacy-only, manifest-only, mixed Session 37, directory spanning Tape_02/Tape_03, indexed/unindexed small members, loose large files, and incomplete historical metadata.
- **Failure/recovery:** Unprovable historical mapping is recorded as coarse candidate routing and blocks row pruning that needs exact routing; it does not trigger a tape read.
- **Acceptance gate:** The unified view reports old ZIP, new TAR, and loose parts together without altering legacy tables.
- **Rollback:** Disable new adapter writes; preserve all source tables/artifacts.

## Phase 3 — Controlled Session 37 transition

### Task 3.1 — Rehearse and persist the migration boundary

- **Change:** Use the Plan 2 classification report plus Plan 1 frontier bootstrap to create a transition proposal. Treat `AGENTS.md` as the latest written operational baseline, not live database proof; treat conflicting chunk counts/states in `config.ini` comments, `docs/PHASE5_SEALED_BATCH_DESIGN.md`, incident snapshots, and older tests as historical only. In execute mode, atomically persist the approved initial boundary and backfill all chunks that actually exist at that instant as `plan_source='legacy_db'`, `packaging_format='zip'`, and sealed only after membership/ordinal audit passes.
- **Required boundary fields:** verified maximum existing chunk (`last_legacy_planned_chunk`), next allocated index (`first_manifest_first_chunk`), frontier generation, evidence report/artifact locator, source-scope identity, approval, and transition timestamp.
- **Exact files/symbols:** `inspect_db.py`; `src/pg_sessions.py`; `src/pg_scan.py`; `src/pg_containers.py`; `src/session_reconcile.py`; `src/startup_reconcile.py`; `src/scan_frontier.py`.
- **Database:** Insert one approved/active `remote_session_plan_transitions` row. Derive indexes from a locked database audit; do not hardcode 112/113. Preserve `remote_sessions.tape_generation` and all per-container tape labels.
- **Dependencies:** Plan 1 conservative frontier bootstrap, Plan 2 category rules, Tasks 1.1–1.4.
- **Tests:** Add `tests/test_session37_transition.py` covering current written baseline as a fixture, changed maximum index, scan complete/incomplete, shared plan, active owner, transient/backing, duplicate ordinal, format conflict, tape-generation mismatch, and idempotent execute.
- **Failure/recovery:** Any active/ambiguous chunk, unproven scope, incomplete bootstrap, conflicting artifact, or ownership uncertainty blocks activation. A transaction failure leaves no active transition.
- **Acceptance gate:** Existing chunks retain their identities/memberships/ZIP format and future allocation starts at the persisted next index only if proven uncovered source scope remains. If the authoritative scan is already final with no future work, persist/report the boundary but do not fabricate TAR chunks merely to make Session 37 mixed-format.
- **Rollback:** Mark the transition rolled back only for future allocation and create a new `legacy_db` transition epoch. Never alter already created manifest/TAR chunks.

### Task 3.2 — Export fixed legacy membership and rebuild evidence

- **Change:** For each existing legacy chunk, stream the `remote_plan_files` → `remote_snapshot_files` join in chunk-local order into the `provenance_kind='legacy_db_export'` branch of `plan-manifest-v1`. Preserve `plan_file_id`, snapshot identity, original ordinal, and deterministic normalized chunk-local ordinal; set scan-segment identity null rather than inventing a frontier origin. Validate exact paths/sizes/counts/bytes/ordinal mapping and persist it as an auxiliary export while keeping `plan_source='legacy_db'`. For the Session 37 rehearsal scope, also export locally readable terminal outcomes, proven ZIP container/member routing, and exact loose-file tape label/generation/stored locators needed to rebuild large-file `files_index` rows. Mark absent historical disposition or container evidence unresolved/coarse; use a null container ordinal only with coarse routing, and retain the source rows. Plan 4 generalizes this non-destructive exporter before any pruning.
- **Exact files/symbols:** `src/pg_sessions.py::get_chunk_files`; `src/plan_manifest.py`; `src/archive_artifacts.py`; `src/pg_containers.py`; `inspect_db.py` dry-run/execute/verify flags.
- **Database:** Record export artifacts separately from the authoritative plan locator until Plan 4 eligibility can switch a chunk safely. Audit shared plan/snapshot references and preserve every existing container/tape locator.
- **Dependencies:** Task 3.1.
- **Tests:** Missing/duplicate/conflicting legacy rows, global-vs-chunk-local ordinal normalization, large streaming export, archived/source-missing reconstruction, exact/coarse ZIP routing, loose-file locator export, interrupted publish, already-equivalent artifact, and active/ambiguous refusal.
- **Failure/recovery:** Do not invent container membership missing from PostgreSQL. Mark routing precision and keep source rows when exact equivalence cannot be proven.
- **Acceptance gate:** Every exportable fixed legacy chunk has a fully parsed plan manifest equal to its current DB membership; the chosen rebuild rehearsal scope also has enough local terminal/container/loose-locator evidence to reconstruct it without reading the source database or tape.
- **Rollback:** Delete no source rows; ready exports remain harmless auxiliary evidence.

### Task 3.3 — Continue Session 37 from the proven frontier

- **Change:** Allocate only post-boundary chunks through the manifest-first flow and assign `stored_tar` at seal. Keep old chunks on `LegacyDbPlanSource`/ZIP and new chunks on `ManifestPlanSource`/Stored TAR; keep loose large files in both eras.
- **Exact files/symbols:** `src/scan_frontier.py`; `src/plan_source.py`; `src/remote_staging.py`; `src/remote_writer.py`; `src/retriever.py`; `src/pg_directory_catalog.py`.
- **Database:** Persist source/format per chunk/container. Do not update `remote_sessions.default_packaging_format` as a substitute for chunk truth.
- **Dependencies:** Tasks 3.1–3.2 and explicit operator approval.
- **Tests:** One synthetic mixed session with completed ZIP, pending legacy ZIP, future TAR, old/new loose files, multiple tapes, restart between modes, and restore across all routes.
- **Failure/recovery:** Startup dispatches each chunk independently. A failed future transition can revert only uncreated future chunks to legacy planning; existing TAR stays TAR and existing ZIP stays ZIP.
- **Acceptance gate:** No already covered path is replanned, no uncovered path is skipped, and old/new chunks can progress independently.
- **Rollback:** Persist a new future-allocation transition epoch; do not move the original boundary or rewrite containers.

### Task 3.4 — Gate the post-change Session 37 rollout

- **Change:** After the Plan 3 planning/catalog/startup changes, repeat the repository's required rollout progression: full offline tests → isolated PostgreSQL tests → small synthetic hardware pilot → one finite bounded production group → evidence review → only then broader Session 37 continuation.
- **Exact files/symbols:** test suites named throughout this plan; `tests/lto_fakes.py::FakeLtfsAdapter`, `TapeLockObserver`; `src/remote_writer.py::RemoteChunkWriter.write_chunk_group`; `src/scan_frontier.py`; `src/startup_reconcile.py`; operator runbook section in the existing appropriate operations document.
- **Database:** Use a restored isolated copy for rehearsal. Persist pilot/bounded-group evidence and transition approval; do not activate the boundary from test fixtures.
- **Dependencies:** Tasks 3.1–3.3 and all Plan 3 offline/isolated gates.
- **Tests:** Full relevant offline suite, isolated migration/rebuild/compare suite, synthetic mixed-format/frontier pilot, and finite-group/no-idle/hard-failure assertions.
- **Failure/recovery:** A hard tape failure stops immediately with no retry/eject/remount/format/ltfsck; preserve packs, artifacts, dumps, last successful chunk, and ambiguous state. No tape probe occurs while idle or between group chunks.
- **Acceptance gate:** The single bounded production group is reviewed and explicitly approved before broader Session 37 work.
- **Rollback:** Disable future manifest/TAR allocation through a new transition epoch; retain readers/artifacts and do not rewrite existing containers.

## Phase 4 — Restore and local artifact-driven catalog rebuild

### Task 4.1 — Complete directory restore routing

- **Change:** Query `directory_catalog_status_v`, expand the selected directory into exact archive parts, then dispatch each part to ZIP, Stored TAR, or loose restore. Rare small-file lookup scans only relevant local sidecar/legacy manifest candidates.
- **Exact files/symbols:** `src/retriever.py::LTORetriever._restore_directory_complete`; `src/pg_directory_catalog.py::find_directory_restore_parts`; `src/container_restore.py` if added in Plan 2; `src/plan_source.py`.
- **Database:** Return exact format, tape, stored path, source-base path, container/member candidate, and artifact locator. A provisional/incomplete/ambiguous directory requires explicit partial-restore confirmation.
- **Dependencies:** Phase 2 and Plan 2 restore router.
- **Tests:** Extend `tests/test_retriever_restore.py`; add mixed directory, multi-tape, coarse legacy routing, missing local metadata, partial status, conflict, and no-automatic-tape-access cases.
- **Failure/recovery:** Never claim full restore for a non-complete directory. Explicit user restore may read the selected tape artifacts; background validation may not.
- **Acceptance gate:** A complete mixed directory restores with expected paths/sizes/counts and no permanent small-file DB rows for manifest chunks.
- **Rollback:** Legacy file/bundle restore remains available.

### Task 4.2 — Build an empty shadow catalog from local artifacts

- **Change:** Add a rebuild command that discovers ready `session-descriptor`, scan-entry segments, `scan-state-segment`, plan, ZIP/TAR sidecar/export, and terminal artifacts under the permanent metadata root, validates their ordered generation chain and cross-links, and writes an empty shadow PostgreSQL database through idempotent repository methods.
- **Rebuild scope:** sessions identified by stable session label; scan scopes, directory queue/frontier/continuation/empty-directory/error/finality state; scan-segment and chunk-consumption ranges; chunks and transition epochs; ZIP/TAR containers; tape labels/generation identities/locators; directory tree/parts/statistics/completeness; exact loose large-file `files_index` rows from terminal locator evidence; and source outcome summaries. Regenerate internal numeric IDs only.
- **Exact files/symbols:** new `src/catalog_rebuild.py::CatalogRebuilder`; `inspect_db.py` add rebuild command/flags; `src/pg_db.py`; `src/pg_scan.py`; `src/pg_containers.py`; `src/pg_directory_catalog.py`; `src/archive_artifacts.py`.
- **Database:** Require an explicitly named empty shadow database and refuse the configured production database. Apply required migrations in dependency order. Do not copy transient owners/leases/retry state.
- **Dependencies:** Tasks 1.2, 2.1–2.3, 3.2.
- **Tests:** Add `tests/test_catalog_rebuild.py`; isolated PostgreSQL roundtrip for legacy ZIP, new TAR, loose large, provisional scan, source exceptions, multi-tape directory, interrupted rebuild, and idempotent resume.
- **Failure/recovery:** Stop on missing/conflicting/unready artifacts; checkpoint completed artifact IDs in the shadow database. Never fall back to tape.
- **Acceptance gate:** For an artifact-complete candidate scope, a clean shadow database supports session/chunk/frontier/directory/restore queries without reading the original database during rebuild. Coarse legacy ZIP routing is reportable but never satisfies exact row-independent restore, rebuild equivalence, or pruning eligibility.
- **Rollback:** Drop only the explicitly named shadow database after verifying its identity; production remains untouched.

### Task 4.3 — Compare rebuilt and original catalogs semantically

- **Change:** Add canonical comparison that ignores generated numeric IDs, timestamps, transient states, owners/leases, and row ordering but compares stable session label/chunk index, formats, plan paths/sizes/ordinals and scan-segment ranges, disposition counts, container/tape-generation locators, directory frontier/coverage/counts/status/routes, and large-file index records.
- **Exact files/symbols:** `src/catalog_rebuild.py::compare_catalogs`; `inspect_db.py`; existing `src/directory_catalog_validation.py::compare_databases` only as a legacy count-level helper.
- **Database:** Read-only against original and shadow; output a local report outside staging/LTFS.
- **Dependencies:** Task 4.2.
- **Tests:** Equal-with-different-IDs, missing path, same-size path substitution, duplicate contribution, wrong tape, status difference, expected approved coarse-legacy exception, and large-file mismatch.
- **Failure/recovery:** Any unexplained difference fails the comparison and blocks Plan 4 pruning. Same-size content corruption remains outside this hashless comparison and must be reported as residual risk.
- **Acceptance gate:** Canonical comparison is exact for all modeled logical fields and produces machine-readable discrepancies.
- **Rollback:** Comparison is read-only.

## Plan 3 completion gate

Proceed to Plan 4 only when all items pass:

- [ ] Existing chunks are explicitly `legacy_db`/ZIP; new manifest chunks have immutable format/source and ready local plan artifacts.
- [ ] A new manifest-first chunk creates no permanent small-file `remote_snapshot_files`, `remote_plan_files`, or `files_index` rows.
- [ ] Scanner/stager/writer overlap remains intact and restart resumes from the persisted frontier without completed-root replay.
- [ ] Plan, TAR sidecar, and terminal artifacts pass two-phase publication and cross-equivalence tests.
- [ ] Every known directory has queryable provisional/final coverage and many-to-many archive parts.
- [ ] Directory status distinguishes scan finality, terminal disposition, successful archive, part write completion, and local validation.
- [ ] Legacy ZIP, new TAR, and loose large-file restore work in one session.
- [ ] Session 37's boundary is derived from authoritative quiescent evidence, persisted, rehearsed in isolation, and explicitly approved; all pre-existing fixed chunks remain ZIP.
- [ ] Existing ZIP tape locators are unchanged and no container has been rewritten.
- [ ] A manifest-first Session 37 continuation can add future TAR chunks without duplicating covered paths or skipping uncovered scope.
- [ ] After all offline and isolated-PostgreSQL gates, a new small synthetic hardware pilot and one finite bounded Session 37 production group have completed; their no-idle-LTFS, hard-failure, restore, catalog, and artifact evidence has been reviewed and explicitly approved before broader continuation or Plan 4.
- [ ] A clean shadow database rebuilds each declared artifact-complete candidate scope from local artifacts with no automatic tape access; coarse historical ZIP evidence remains ineligible rather than being accepted as exact.
- [ ] Canonical comparison against the original catalog passes for the candidate scope.
- [ ] No PostgreSQL pruning has occurred.

### Plan 3 rollback gate

- [ ] Disable creation of future manifest chunks and persist a new future `legacy_db` transition epoch.
- [ ] Continue reading all already sealed manifest chunks through `ManifestPlanSource`.
- [ ] Preserve ready artifacts, directory contributions, transition evidence, and formats.
- [ ] Never rewrite TAR as ZIP, ZIP as TAR, or change completed tape locators.
- [ ] Never mark provisional coverage final merely to permit rollback.
