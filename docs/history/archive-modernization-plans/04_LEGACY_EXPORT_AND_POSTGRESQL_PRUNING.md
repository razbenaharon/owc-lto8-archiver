# Plan 4 — Legacy Export and PostgreSQL Pruning

## Execution contract

1. Start only after Plan 3 restore, directory completeness, scan continuation, local artifact publication, shadow rebuild, and canonical comparison pass for the candidate chunk.
2. Keep pruning optional, disabled by default, explicit, dry-run first, audited, batched, resumable, and cancellable between batches.
3. Never infer eligibility from TAR/ZIP creation, Robocopy launch, Robocopy success alone, or `remote_chunks.status='done'` alone.
4. Never prune an active, owned, retrying, fetching, packing, backing, ambiguous, or unresolved chunk.
5. Never prune information still needed by an incomplete scan, duplicate prevention, expected directory counts, restore routing, or tape accounting.
6. Keep every loose-file `files_index` row and every file at or above the configured loose-file threshold. Determine storage class from sealed plan/container evidence, not from the existing hard-coded 10 MiB local-export threshold.
7. Use exact paths, sizes, ordinals, counts, total bytes, full artifact parsing, local restore-route rehearsal, shadow rebuild, and semantic comparison. Existing optional checksums may remain as diagnostics, but content hashes are not a mandatory correctness or deletion gate.
8. Record the residual risk that same-size/same-structure corruption is not detected.
9. Do not read tape automatically and do not execute pruning as part of implementing or rehearsing export-only gates.

## Phase 0 — Pin the current pruning boundary

### Task 0.1 — Characterize the existing local manifest exporter

- **Change:** Add tests/report output that distinguish the current `src/local_manifest_archive.py` feature from the new operational export required here.
- **Current behavior to pin:**
  - Migration 010 creates `local_manifest_exports`, `local_manifest_export_rows`, `local_manifest_segments`, `local_manifest_folder_aggregates`, and `local_manifest_catalog_aggregates`.
  - Current export is `files_index`-only, uses `JSONL.zst`, uses an exact hard-coded 10 MiB threshold, requires the entire remote session and all chunks to be terminal, and deliberately never deletes planning/session tables.
  - Current pruning rechecks identity, uses bounded transactions, checkpoints progress, and preserves aggregate accounting; these mechanics are reusable.
  - Current successful ZIP sidecars usually have tape locators after staging cleanup and are not automatically local evidence.
  - `cleanup_unreferenced_remote_data()` is global orphan cleanup that refuses while any active session exists; it is not a chunk-pruning implementation.
- **Exact files/symbols:** `src/local_manifest_archive.py`; `src/pg_sessions.py::cleanup_unreferenced_remote_data`; `inspect_db.py` current `--export-small-file-manifests`, `--validate-local-manifest-export`, `--prune-exported-small-files`; migration 010.
- **Database:** Read-only characterization queries.
- **Dependencies:** Plan 3 complete.
- **Tests:** Preserve `tests/test_local_manifest_archive.py::test_classification_requires_terminal_sessions_and_chunks` and `test_prune_never_touches_operational_tables`; preserve `tests/test_pg_integration.py::test_zz_local_manifest_export_validate_prune_preserves_operations` as the legacy contract.
- **Failure/recovery:** Do not broaden the old exporter silently; introduce explicit new scope/state.
- **Acceptance gate:** Operators can distinguish legacy `files_index` export status from new chunk operational export/prune eligibility.
- **Rollback:** Report/test-only.

### Task 0.2 — Encode the actual table dependency boundaries

- **Change:** Add a read-only eligibility report that classifies candidate rows and all inbound/outbound references before export.
- **Required table rules:**

  | Table | Current unique information/dependency | Earliest safe pruning scope |
  |---|---|---|
  | `remote_file_state` | Surviving terminal rows are primarily `source_missing`; FK references `remote_plan_files.plan_file_id`; current done transition already deletes other states | Per final chunk, after `terminal-state-v1` preserves every disposition and all readers use it for final chunks |
  | `remote_plan_files` | Maps plan/snapshot file to chunk and ordinal; runtime legacy plan reader depends on it; may belong to a plan referenced by multiple sessions | Per final chunk, after exact plan export, manifest `PlanSource` activation for that chunk, all file-state references removed, and all referencing sessions audited |
  | `remote_snapshot_files` | Sole current normalized path/expected-size/snapshot truth; no chunk column; may be referenced by several plans; current incomplete scanner uses it as the visited set | Session/snapshot or proven finalized scan-segment scope only; default first implementation waits for final full scan coverage, no plan references, and replacement frontier/dedup artifacts |
  | small packed rows in `files_index` | Search/restore/catalog/tape-accounting evidence for indexed members; modern rows have session/chunk provenance, legacy rows may not | Per final chunk after exact local manifest/sidecar routing, directory contribution/completeness, accounting, restore-route, rebuild, and comparison gates |

- **Additional constraints:**
  - `remote_plan_files` cannot be removed before referencing `remote_file_state` rows.
  - A `remote_plan_files` row cannot be removed while any session still uses its plan through `LegacyDbPlanSource`, even if the candidate Session 37 chunk has switched to manifests. Every session referencing the plan/chunk must be terminal and independently eligible for that membership, have an equivalent ready manifest, have all referencing `remote_file_state` resolved, and switch authority atomically as one candidate group; otherwise retain the plan row.
  - `remote_snapshot_files` cannot be removed while any `remote_plan_files` row references it.
  - `remote_plans` remain while `remote_sessions.plan_id` references them; keep plan/snapshot parent summary rows in the first pruning version.
  - Streaming snapshots/plans can be session-specific by convention, but reference checks must prove they are not shared.
  - `files_index.remote_session_id/remote_chunk_index` supports exact modern scoping; legacy inference from pack names is evidence only, never sole proof.
- **Reader audit:** Before plan-row eligibility, audit every direct reader of `remote_plan_files`/`remote_snapshot_files`, including `PgSessionMixin.get_chunk_files()`, `get_chunk_size_summary()`, pending-size/tape reservations, resume/reconciliation, reporting, inspector queries, and cleanup. Route terminal promoted chunks through `ManifestPlanSource` or persisted chunk aggregates; changing staging alone is insufficient.
- **Exact files/symbols:** migrations 001, 003, 004, 005, 006, 008, 009, 010, and 013; inspect live FK actions through `pg_constraint`; `src/pg_sessions.py::get_chunk_files`, `get_remote_existing_snapshot_paths`, `get_chunk_size_summary`, `update_chunk_status`, `cleanup_unreferenced_remote_data`; `src/session_reconcile.py`; `src/inspector_repository.py`; `src/pg_catalog.py`; `src/pg_tapes.py::PgTapeMixin._calculate_tape_used_space_conn`.
- **Database:** Read-only dependency graph and candidate counts/bytes by table/chunk.
- **Dependencies:** Task 0.1.
- **Tests:** Shared plan/snapshot across multiple sessions, incomplete scan, modern/legacy provenance, every direct-reader compatibility path, dangling-reference prevention, installed FK-action audit, and tape-accounting fixtures.
- **Failure/recovery:** Unknown/sharing/conflicting provenance makes the affected table scope ineligible; it does not broaden to session deletion.
- **Acceptance gate:** The report gives a fail-closed reason for each candidate/noncandidate table and row range.
- **Rollback:** Read-only.

### Task 0.3 — Pin the rows and metadata that always remain

- **Change:** Make the eligibility report and delete repositories reject every object outside the four explicitly candidate row classes. Preserve sessions, chunks, per-chunk/per-container packaging format, scan scopes/frontier/coverage, transition epochs, container/bundle records, tape labels/generations, local and tape locators, artifact versions/sizes/publication/readiness, directory tree/parts/summaries/completeness, restore routes, all loose and large-file `files_index` rows, source outcome aggregates, pruning audits/certificates, rebuild validations, PostgreSQL backup evidence, and rehearsal reports. “Preserve” means these parent/evidence rows are never deletion candidates. The only allowed mutation to a preserved `remote_chunks` row is the narrowly audited terminal authority update from `plan_source='legacy_db'` to `manifest` plus its verified plan-artifact FK/transition timestamp in the same transaction as the append-only transition audit; every other column remains unchanged.
- **Exact files/symbols:** `src/catalog_pruning.py`; `src/pg_pruning.py`; `src/pg_sessions.py`; `src/pg_scan.py`; `src/pg_containers.py`; `src/pg_directory_catalog.py`; migrations 013–017.
- **Database:** Add an allow-list of deletable table names and exact scope builders; no generic table-name delete API. Keep `remote_sessions`, `remote_chunks`, `remote_plans`, and `remote_snapshots` in the first implementation even after their detailed child rows become eligible.
- **Dependencies:** Task 0.2.
- **Tests:** Assert every preserved row remains byte-for-byte/logically unchanged after a synthetic completed run except the explicit whitelisted `remote_chunks` authority columns, whose exact before/after values and matching transition audit must be asserted; assert unknown table requests fail closed.
- **Failure/recovery:** Encountering a dependency outside the allow-list stops the plan and records the blocker.
- **Acceptance gate:** Dry-run output has separate `will_delete` and `will_preserve` sections covering every relevant table/metadata class.
- **Rollback:** Read-only until Phase 5; preserved objects are never rollback candidates.

## Phase 1 — Add generalized export, audit, and pruning state

### Task 1.1 — Add migration 017

- **Change:** Add `scripts/sql/017_postgres_manifest_pruning.sql` through an explicit operator-applied path. Reuse `archive_artifacts` for artifact identities; do not overload legacy migration-010 export states.
- **Database:** Apply these additive audit/pruning changes:
  - `archive_manifest_exports`: export ID, session/chunk or scan scope, export kind, state (`planned`, `writing`, `ready`, `verified`, `failed`, `superseded`), source table snapshot bounds, expected counts/bytes, artifact IDs, evidence version, creator/timestamps, and eligibility result.
  - `archive_export_table_counts`: export/table, candidate rows/bytes, exported rows/bytes, duplicate/conflict/error counts, minimum/maximum stable key, and verification state.
  - `archive_rebuild_validations`: export/scope, shadow database identity, artifact-set identity, canonical comparison status/counts, report locator, performed time, and operator.
  - `archive_prune_runs`: run ID, exact scope, state (`planned`, `approved`, `running`, `cancel_requested`, `paused`, `cancelled`, `blocked`, `completed`, `failed`), dry-run flag, batch size, approval/evidence references, database and metadata backup evidence, start/end timestamps, and error.
  - `archive_metadata_backups`: append-only backup ID/run ID, phase (`pre_prune` or `post_certificate`), source artifact-set generation, independent destination/inventory locator, copied/parsed counts and bytes, relocation/restore verification state, certificate ID when applicable, and timestamps. The pre-prune row proves prerequisite evidence; the post-certificate row proves the final certificate was independently copied and verified.
  - `archive_prune_tables`: run/table ordering, snapshotted stable-key bounds/counts, checkpoint key, deleted/skipped/conflict counts, state, and timestamps.
  - `archive_prune_batches`: run/table/batch ordinal, inclusive/exclusive key range, expected/deleted/skipped counts, transaction outcome, and timestamps.
  - `archive_prune_accounting`: one authoritative post-prune accounting contribution per tape-generation/container or packed-small contribution scope not already represented by directory-bundle/container/migration-010 accounting, with source system (`migration_010` or `manifest_prune`), logical bytes, actual artifact bytes when known, row/file counts, and uniqueness that prevents the same source row/container from being owned by both export systems. Loose/large-file storage is always preserved and is never a pruning or replacement-accounting candidate in this plan.
  - `archive_prune_certificates`: immutable logical summary of gates, before/after counts/bytes, artifacts, restore-route rehearsal, rebuild/compare evidence, operator approvals, residual-risk acknowledgement, vacuum status, and local certificate artifact locator. Link post-certificate copies through `archive_metadata_backups` rather than mutating the certificate.
- **Concurrency guard:** Define a dedicated cluster-wide `PRUNE_LOCK_KEY`, require the archiver advisory lock/process set to be clear for execution, enforce one active pruning executor with a partial unique singleton row, and reject overlapping approved candidate identities/ranges. A logically `active` session row alone remains allowed when no process/owner is working.
- **Exact files/symbols:** new migration 017; add `src/pg_core.py::PgConnectionCore.apply_manifest_pruning_schema`; `src/pg_db.py`; new `src/pg_pruning.py::PgPruningMixin`; add `inspect_db.py --apply-manifest-pruning-schema` and schema validation/report flags.
- **Dependencies:** Migrations 014–016 and Plan 3 artifact schema.
- **Tests:** Isolated PostgreSQL tests for idempotent migration, state constraints including pause/cancel/block, exact scope, immutable approved candidate bounds, global lock/single executor, candidate overlap refusal, batch checkpoints, cancellation state, and certificate uniqueness.
- **Failure/recovery:** Schema is additive and pruning flags remain false. No migration statement deletes source rows.
- **Acceptance gate:** A dry-run can persist a complete candidate/audit plan without granting execute authority.
- **Rollback:** Disable commands; retain audit records. Never drop audit tables after any execution.

### Task 1.2 — Add compatible administration commands

- **Change:** Keep `inspect_db.py` as the root administration entry point and add argparse subcommands while preserving existing flat flags as compatibility wrappers.
- **Required commands:**

  ```text
  python inspect_db.py manifests export --session-id <id> --chunk-id <id> --dry-run
  python inspect_db.py manifests export --session-id <id> --chunk-id <id> --execute
  python inspect_db.py manifests verify --session-id <id> --chunk-id <id>

  python inspect_db.py catalog rebuild --shadow-database <name>
  python inspect_db.py catalog compare --session-id <id>

  python inspect_db.py prune plan --session-id <id> [--chunk-id <id>]
  python inspect_db.py prune execute --session-id <id> [--chunk-id <id>]
  python inspect_db.py prune resume --run-id <id>
  python inspect_db.py prune status --run-id <id>
  python inspect_db.py prune cancel --run-id <id>
  ```

- **Exact files/symbols:** `inspect_db.py::main`; new `src/legacy_export.py`; new `src/catalog_pruning.py`; Plan 3 `src/catalog_rebuild.py`; `src/pg_pruning.py`.
- **Database:** Dry-run is default for export/prune plan. `--execute` requires exact scope, recorded gates, and interactive confirmation unless a separate explicit noninteractive approval token is supplied.
- **Dependencies:** Task 1.1.
- **Tests:** Add `tests/test_archive_admin_cli.py` for parser compatibility, dry-run default, missing chunk/session refusal, production-vs-shadow database guard, cancellation, and no accidental execution from legacy flags.
- **Failure/recovery:** Commands refuse ambiguous target database/configuration and print exact database identity before work. No command accesses LTFS.
- **Acceptance gate:** Every mutating command has a separate plan/status path and an auditable run ID.
- **Rollback:** Hide/disable new subcommands; legacy inspect commands remain.

## Phase 2 — Export complete local evidence

### Task 2.1 — Export each legacy chunk plan

- **Change:** Stream exact legacy membership through `LegacyDbPlanSource` into the `provenance_kind='legacy_db_export'` branch of `plan-manifest-v1`, retaining canonical path, expected size, stable normalized chunk-local ordinal, chunk identity, source snapshot identity, storage class (`container` or `loose`), container format (`zip` or `stored_tar`), and routing precision. Preserve both `source_plan_file_id`/original `remote_plan_files.ordinal` and exported chunk-local ordinal so globally numbered legacy plans and chunk-reset streaming plans have an auditable deterministic mapping. Set source scan-segment identity null rather than inventing it. A container ordinal may be null only with `routing_precision='coarse'`; that artifact remains auxiliary and ineligible for authority promotion or pruning until exact routing is proven.
- **Exact files/symbols:** `src/legacy_export.py::export_chunk_plan`; `src/plan_source.py::LegacyDbPlanSource`; `src/plan_manifest.py`; `src/pg_sessions.py::get_chunk_files`; `src/archive_artifacts.py`.
- **Database:** Snapshot source `plan_file_id` bounds and counts in `archive_manifest_exports`; persist the ready artifact through `archive_artifacts`. Do not switch `plan_source` until verification and restoration gates pass.
- **Dependencies:** Phase 1.
- **Tests:** Millions-row streaming fake, duplicate path/ordinal, inconsistent plan/snapshot relationship, shared plan, interrupted export, equivalent retry, and exact counts/bytes.
- **Failure/recovery:** Conflicting or incomplete membership fails export. Keep all DB rows and any valid ready artifact for investigation.
- **Acceptance gate:** Exported path/size/original-ordinal/export-ordinal mapping equals the source join exactly in both directions.
- **Rollback:** Artifact remains auxiliary; no source state changes.

### Task 2.2 — Export terminal outcomes and container routing

- **Change:** Build `terminal-state-v1` and verified legacy ZIP/TAR sidecar/container evidence from current state.
- **Historical rules:**
  - For a `done` legacy chunk, classify a non-exception planned member as `archived` only when robust writer completion, catalog/container evidence, plan equivalence, and directory contribution evidence agree.
  - Preserve explicit surviving `source_missing` state.
  - Do not fabricate historical permission/unreadable/changed distinctions absent from current schema; classify uncertain entries `unresolved` and refuse eligibility.
  - Existing TAR chunks use their ready local `tar-sidecar-v1`.
  - Existing ZIP manifests may be exported from local/DB evidence only when exact member-to-container routing can be proved. A tape-only manifest locator is not local validation evidence.
- **Exact files/symbols:** `src/legacy_export.py::export_terminal_state`, `export_container_evidence`; `src/terminal_manifest.py`; `src/local_manifest_archive.py`; `src/pg_catalog.py`; `src/pg_containers.py`; `src/pg_directory_catalog.py`.
- **Database:** Persist artifact readiness and aggregate archived/missing/permission/unreadable/changed/unresolved counts. Record routing precision (`exact` or `coarse`); coarse evidence blocks pruning that requires exact row-free restore.
- **Dependencies:** Task 2.1 and Plan 3 directory parts.
- **Tests:** Done-but-catalog-mismatch, source missing, absent historical status, tape-only manifest, exact/coarse ZIP mapping, TAR sidecar equivalence, large loose member, and multi-container chunk.
- **Failure/recovery:** Any unresolved member, unknown writer state, or missing local routing metadata leaves the chunk ineligible. Do not read tape to improve the result automatically.
- **Acceptance gate:** Every plan ordinal has exactly one justified terminal disposition and every archived packed member has an independently usable route.
- **Rollback:** Keep source rows; mark export failed/superseded if a corrected export is needed.

### Task 2.3 — Export scan/snapshot semantics before considering snapshot-row deletion

- **Change:** Create `legacy-scan-segment-v1` artifacts when current `remote_snapshot_files` carries source path/size truth not already captured by Plan 1 scan segments. Combine it only with proven Plan 1 scope/frontier/coverage evidence; snapshot file presence alone never proves a directory was traversed completely.
- **Required content:** covered directory and scope identities, provisional/final state, persisted frontier/continuation, path/size entries where needed, excluded/invalid entries, scan errors, coverage boundaries/generation, scan completion state, and the data used by duplicate prevention.
- **Exact files/symbols:** `src/legacy_export.py::export_scan_segments`; `src/scan_frontier.py`; `src/pg_scan.py`; `src/pg_sessions.py::get_remote_existing_snapshot_paths`; `src/archive_artifacts.py`.
- **Database:** Record which snapshot IDs/row ranges and frontier generation each artifact covers. Verify all plan references before classifying snapshot rows.
- **Dependencies:** Plan 1 Session 37 bootstrap and Task 2.1.
- **Tests:** Incomplete scan, partial directory, completed directory, excluded/error entries, snapshot rows absent from plan, plan rows from shared snapshot, duplicate roots, and restart/dedup without snapshot rows in a shadow fixture.
- **Failure/recovery:** If scan coverage is incomplete, retain `remote_snapshot_files` even when completed chunk plan rows later become eligible. Never mark uncovered scope final.
- **Acceptance gate:** A shadow scanner can resume and prevent duplicate planning from artifacts/frontier without consulting candidate snapshot rows.
- **Rollback:** Retain all snapshot rows; auxiliary scan artifacts remain.

### Task 2.4 — Persist directory summaries and completeness before row removal

- **Change:** Recalculate and freeze a directory completeness generation for each export scope using Plan 3 coverage, plan, terminal, container, writer, and local validation evidence.
- **Exact files/symbols:** `src/pg_directory_catalog.py::recalculate_directory_completeness`; `src/legacy_export.py`; `src/catalog_rebuild.py`; `src/pg_tapes.py`.
- **Database:** Persist direct/recursive expected counts/bytes, archived and exception counts, archive parts, restore routes, actual artifact bytes, and tape-accounting aggregates. Link the generation to the export run.
- **Dependencies:** Tasks 2.1–2.3.
- **Tests:** Recalculate after candidate per-file rows are hidden in a transaction/shadow database and prove identical directory/tape summaries.
- **Failure/recovery:** Any ambiguous or missing contribution blocks the affected chunk/directory; do not substitute global session totals.
- **Acceptance gate:** Directory and tape-accounting queries no longer require the candidate small-file rows.
- **Rollback:** Recompute from retained source rows/artifacts; do not delete the frozen generation.

### Task 2.5 — Preserve one authoritative tape-accounting contribution

- **Change:** Reconcile the candidate set with existing `local_manifest_export_rows` before export/pruning. Refuse double ownership, or import/link an already verified migration-010 export instead of exporting it again. Update tape accounting to select each physical/logical contribution exactly once: actual `archive_containers.artifact_size_bytes` for new format-aware containers, existing legacy directory-bundle accounting where actual size is unavailable, migration-010 folder aggregates for rows already pruned there, and `archive_prune_accounting` for newly pruned packed-small contributions not covered by those sources. Loose and large-file rows remain live and continue through their existing accounting path.
- **Exact files/symbols:** `src/pg_tapes.py::PgTapeMixin._calculate_tape_used_space_conn`; `src/local_manifest_archive.py`; `src/pg_pruning.py`; `src/pg_directory_catalog.py`; `src/pg_containers.py`.
- **Database:** Key accounting by tape generation as well as label/container. Persist authority/source and prevent overlap with live `files_index`, migration-010 aggregates, directory-bundle rows, and new prune aggregates.
- **Dependencies:** Tasks 2.1–2.4.
- **Tests:** Exact before/after tape used space for legacy unpruned, migration-010 pruned, new manifest-pruned, mixed old/new exports, actual-size TAR, logical-size legacy ZIP, loose rows, tape reset generation, and deliberate double-ownership conflicts.
- **Failure/recovery:** Any overlap or unexplained accounting drift blocks export verification/pruning. Never “fix” usage by deleting an older aggregate.
- **Acceptance gate:** `_calculate_tape_used_space_conn()` returns the same physical/logical accounting result before and after hiding candidate `files_index` rows, with every byte owned by one source.
- **Rollback:** Keep the prior accounting path active until the new comparison passes; retain all aggregates.

### Task 2.6 — Verify export equivalence without mandatory hashes

- **Change:** Implement a bidirectional streaming comparison between PostgreSQL source rows and artifacts: exact path, expected/observed size, ordinal, storage class, container identity/routing, disposition, counts, and byte totals. Fully parse every `JSONL.zst` and TAR sidecar to its trailer/end.
- **Exact files/symbols:** `src/legacy_export.py::verify_export`; `src/archive_artifacts.py`; `src/tar_container.py`; `src/plan_source.py`.
- **Database:** Mark export `verified` only after all table counts and artifact comparisons pass. Existing SHA-256 fields in migration 010 may be recorded as optional diagnostics but are not required gates.
- **Dependencies:** Tasks 2.1–2.5.
- **Tests:** Missing, unexpected, duplicate, reordered ordinal, wrong size, same totals/different paths, truncated compressed artifact, wrong actual file size, conflicting artifact version, and optional-checksum absence.
- **Failure/recovery:** Verification failure changes no source row and invalidates eligibility.
- **Acceptance gate:** Export equivalence is exact for modeled metadata in both directions; residual same-size content risk is printed and acknowledged.
- **Rollback:** Mark the export superseded after a corrected new export; never overwrite a ready conflicting artifact.

## Phase 3 — Prove row-independent restore and rebuild

### Task 3.1 — Rehearse restore with candidate rows hidden

- **Change:** In an isolated database transaction or shadow database, hide/delete only candidate rows, resolve exact directory/file restore routes through artifacts and durable directory/container rows, and run local extraction tests against representative locally readable ZIP/TAR/loose fixtures.
- **Exact files/symbols:** `src/retriever.py`; `src/container_restore.py`; `src/plan_source.py::ManifestPlanSource`; `src/pg_directory_catalog.py`; `src/legacy_export.py`.
- **Database:** Never perform this rehearsal in production. Compare restore plan paths/sizes/counts/container/tape routes before/after hiding rows.
- **Dependencies:** Verified export and directory generation.
- **Tests:** Exact file, subtree, complete directory, mixed ZIP/TAR/loose, multi-tape route, coarse legacy route refusal, and no automatic tape access.
- **Failure/recovery:** If exact routing or expected output cannot be produced without candidate rows, the chunk is ineligible.
- **Acceptance gate:** The actual candidate has exact locally readable routing metadata with candidate rows absent, and the same code path successfully extracts representative local ZIP/TAR/loose fixtures. A representative fixture never substitutes for missing candidate metadata.
- **Rollback:** Discard the transaction/shadow database.

### Task 3.2 — Rebuild and compare the candidate scope

- **Change:** Run Plan 3 `CatalogRebuilder` into an explicitly named empty shadow database and canonical comparison for the candidate session/chunk, then persist `archive_rebuild_validations` evidence.
- **Exact files/symbols:** `src/catalog_rebuild.py::CatalogRebuilder`, `compare_catalogs`; `inspect_db.py catalog rebuild/compare`; `src/pg_pruning.py`.
- **Database:** Rebuild from local ready artifacts, not source candidate rows or tape. Compare sessions, chunks, formats, containers/tape generations, frontier/coverage, directory stats/status/routes, terminal outcomes, and large-file rows while ignoring generated IDs/transient state.
- **Dependencies:** Task 3.1.
- **Tests:** Roundtrip plus deliberately missing artifact/route/large-file/tape/contribution cases.
- **Failure/recovery:** Any unexplained difference blocks pruning. Store the report outside staging/LTFS and link it to the export.
- **Acceptance gate:** Shadow canonical comparison passes for the exact candidate scope.
- **Rollback:** Drop only the verified shadow database; source remains untouched.

### Task 3.3 — Require database, metadata, and operator evidence

- **Change:** Require a fresh verified PostgreSQL backup using the existing `inspect_db.py --backup-postgres` path plus a verified independent pre-prune copy/inventory of the permanent metadata root. The pre-prune metadata backup must preserve relocation-safe root-relative locators, recorded sizes/versions/counts, parse every prerequisite artifact after copy, and include the candidate's scan/plan/sidecar/terminal/session, export, comparison, and approval evidence. It cannot contain the pruning certificate, which does not exist yet. Require the documented shadow rehearsal, exact export/rebuild IDs, and explicit operator scope approval before creating an executable prune run. Task 5.4 separately publishes, copies, and verifies the post-prune certificate.
- **Exact files/symbols:** `inspect_db.py`; `src/pg_backup.py::create_verified_production_backup`; new `src/metadata_backup.py::create_verified_metadata_backup`; `src/archive_artifacts.py`; `src/pg_pruning.py`.
- **Database:** Store the database backup evidence on the prune plan and append a verified `archive_metadata_backups(phase='pre_prune')` row with path/size/time/source identity, artifact inventory result, and restore-verification result. Do not store credentials.
- **Dependencies:** Task 3.2.
- **Tests:** Missing/stale/wrong-database backup, missing/corrupt/non-relocatable metadata copy, unverified restore, scope mismatch, approval mismatch, candidate mutation after capture, and unrelated active-session mutation.
- **Failure/recovery:** Revalidate the exact candidate identities, chunk state/owner, shared-plan references, artifact version/readiness/size, directory completeness generation, and tape-accounting generation. A mutation to those dependencies invalidates the run; unrelated future chunks/catalog rows do not.
- **Acceptance gate:** Execute can cite verified database and metadata backups plus matching rehearsal/approval for the exact immutable candidate set.
- **Rollback:** No pruning run is approved.

## Phase 4 — Enforce the safe deletion gate

### Task 4.1 — Implement one authoritative eligibility evaluator

- **Change:** Add `src/catalog_pruning.py::PruneEligibilityEvaluator` and make dry-run, execute, resume, and status use the same result. It must require all applicable conditions below.
- **Chunk/state gates:** terminal chunk; sealed membership; immutable known format; no active owner/lease/process; not `fetching`, `packing`, `backing`, retrying, or unresolved; writer not ambiguous; every plan ordinal terminal; every required archive part written/cataloged.
- **Artifact gates:** final ready plan/sidecar/terminal/scan artifacts as applicable; no relevant `.part`; actual artifact size matches recorded size; every artifact parses fully; paths/sizes/ordinals/counts/bytes agree; duplicate/unexpected/missing entries absent.
- **Directory/restore gates:** directory contributions and frozen completeness generation persisted; exact restore route works without candidate rows; normal small-file lookup for the scope works through local artifacts; tape accounting remains equivalent.
- **Rebuild/operations gates:** empty-shadow rebuild and canonical comparison pass; metadata exists outside primary PostgreSQL and is locally readable; a matching independent `pre_prune` metadata backup is verified and recorded; a verified matching PostgreSQL backup exists; rehearsal documented; future scan continuation and duplicate prevention do not depend on candidates; explicit operator approval and residual-risk acknowledgement exist.
- **Exact files/symbols:** new `src/catalog_pruning.py`; `src/session_reconcile.py`; `src/directory_catalog_validation.py::archiver_lock_status`; `src/startup_reconcile.py`; `src/pg_pruning.py`; `src/pg_directory_catalog.py`; `src/catalog_rebuild.py`.
- **Database:** Persist every gate as pass/fail/not-applicable with evidence locator/version. A failed gate cannot be overridden by `--force`; require the underlying evidence to be repaired and re-planned.
- **Dependencies:** Phase 3.
- **Tests:** Table-driven unit tests for every gate plus concurrent state/owner changes between plan and execute.
- **Failure/recovery:** Re-evaluate volatile gates immediately before every table phase and batch. Stop on change; keep checkpoints.
- **Acceptance gate:** No code path can call delete without a current successful evaluator result and approved run.
- **Rollback:** Disable execute; status/dry-run remain available.

### Task 4.2 — Enable Session 37 chunk-scoped eligibility

- **Change:** Remove the old whole-session-terminal requirement only from the new evaluator. `remote_sessions.status='active'` alone is not disqualifying: permit an individually final Session 37 chunk while the logical session/scan remains incomplete, but retain snapshot rows and any scan evidence required by the unfinished frontier. Any process, advisory lock, owner, lease, or retry touching the candidate is disqualifying; require Session 37 to be process-quiescent/paused for the first production prune pilot.
- **Exact files/symbols:** `src/catalog_pruning.py`; `src/legacy_export.py`; `src/plan_source.py`; `src/scan_frontier.py`; do not alter legacy `src/local_manifest_archive.py` classification semantics without compatibility tests.
- **Database:** Candidate plan/file-state/small-index rows are chunk-scoped. Candidate `remote_snapshot_files` remain zero until full scan coverage and reference gates pass.
- **Dependencies:** Task 4.1.
- **Tests:** Completed Session 37 chunk eligible; pending/fetching/packing/backing/ambiguous chunks ineligible; active scanner with independent frontier; chunk plan pruned while snapshot rows retained; future rediscovery prevented by frontier artifacts.
- **Failure/recovery:** Session activity alone does not block an independent final chunk, but any active ownership or scan dependency for that chunk does.
- **Acceptance gate:** Report demonstrates safe chunk candidates without proposing active/ambiguous chunks or snapshot deletion during incomplete coverage.
- **Rollback:** Disable Session 37 scope; no source rows changed by planning.

## Phase 5 — Execute batched, resumable logical pruning

### Task 5.1 — Snapshot stable candidate keys and table order

- **Change:** At approval, materialize immutable candidate primary-key ranges/identities and expected counts for the exact run. Use keyset ranges, never `OFFSET` or a changing threshold query.
- **Required first-version order:**
  1. `remote_file_state` rows for the final chunk, keyed by `(session_id, plan_file_id)`, after terminal equivalence.
  2. `remote_plan_files` rows, keyed by `plan_file_id`, after every session/chunk sharing that plan membership passes the gates, all its file-state references are resolved, and one transaction records each permitted terminal `legacy_db`→`manifest` transition plus the ready artifact FK. If any sharing session cannot switch, retain the shared plan rows.
  3. Eligible packed-small `files_index` rows, keyed by `file_id`, after directory/restore/accounting gates. This phase is independent of plan FKs but stays after plan-source activation for simpler rollback evidence.
  4. `remote_snapshot_files`, keyed by `snapshot_file_id`, only in a separate session/snapshot-wide run after final scan coverage, no remaining plan reference, exported scan semantics, and shared-reference audit.
- **Exact files/symbols:** `src/pg_pruning.py::plan_prune_run`; `src/catalog_pruning.py`; use the installed primary-key/FK/unique-constraint audit from Task 0.2 across migrations 001, 004, 006, 008, and 009 rather than relying on a two-migration shortcut.
- **Database:** Store min/max keys plus exact candidate identity evidence (`record_key`/session/chunk/snapshot relation and expected size) in audit/artifacts. Do not materialize millions of duplicate audit rows in PostgreSQL if a ready local candidate `JSONL.zst` artifact suffices.
- **Dependencies:** Phase 4.
- **Tests:** Stable range under concurrent inserts, sparse keys, row changed after plan, shared references, empty phase, and exact storage-class filter.
- **Failure/recovery:** A row that no longer matches its snapshotted identity is skipped/conflict-counted and stops certification.
- **Acceptance gate:** Before counts equal immutable candidate counts and every range belongs only to the approved scope.
- **Rollback:** Discard an unexecuted run and plan a new one.

### Task 5.2 — Delete in short, checkpointed transactions

- **Change:** Implement bounded keyset batches with a configurable conservative batch size and per-batch eligibility recheck. In one PostgreSQL transaction: delete by exact IDs plus identity predicates, write the `archive_prune_batches` result/counts, and advance the `archive_prune_tables` checkpoint; commit them atomically. Perform post-commit verification before the next batch. Check cancellation only between batches.
- **Exact files/symbols:** `src/catalog_pruning.py::PruneExecutor`; `src/pg_pruning.py::delete_prune_batch`, `checkpoint_prune_batch`; `src/runtime.py::CANCEL`; `inspect_db.py prune execute/resume/cancel/status`.
- **Database:** Lock only the current bounded candidate rows. Record attempted/deleted/skipped/conflict counts and transaction outcome. Resume strictly after the last committed key.
- **Dependencies:** Task 5.1.
- **Tests:** Cancellation before/after the atomic transaction, process crash before commit/after commit/before post-check, deadlock/retry, PostgreSQL restart, row identity drift, zero-row retry, and repeated resume idempotence.
- **Failure/recovery:** Atomic audit/checkpoint state makes a committed deletion discoverable. If the client still cannot determine commit outcome, reread the batch row and exact candidate keys before retry; never rerun a range blindly. Stop on active owner/state change.
- **Acceptance gate:** Kill/restart at every boundary reaches the same final logical result without deleting out-of-scope rows.
- **Rollback:** Logical deletion is restored only from the verified backup or artifact-driven rebuild into a replacement database; do not promise transactional rollback after committed batches.

### Task 5.3 — Verify after every table phase

- **Change:** After each table finishes, verify deleted/remaining counts, referential integrity, manifest plan/terminal equivalence, directory/tape summaries, restore-route output, and shadow comparison for the changed scope before advancing.
- **Exact files/symbols:** `src/catalog_pruning.py::verify_prune_phase`; `src/catalog_rebuild.py::compare_catalogs`; `src/pg_directory_catalog.py`; `src/pg_tapes.py`.
- **Database:** Persist phase verification and before/after sizes/counts. Do not advance on skipped/conflicting rows without a new approved plan.
- **Dependencies:** Task 5.2.
- **Tests:** Deliberate mismatch at each phase prevents the next phase; successful phase resume is idempotent.
- **Failure/recovery:** Stop with the last completed checkpoint and keep the database usable through legacy/manifest adapters. Do not delete a wider table to “clean up” partial progress.
- **Acceptance gate:** Each completed phase is independently certified before the next FK/dependency layer is touched.
- **Rollback:** Restore/rebuild only the failed scope if needed; preserve audit evidence.

### Task 5.4 — Issue a durable pruning certificate

- **Change:** After final verification, publish a local `pruning-certificate-v1.jsonl.zst` plus `archive_prune_certificates` row containing scope, table phases/ranges, artifacts and versions/sizes, before/after counts/bytes, pre-prune backup/rebuild/compare/restore evidence, directory generation, operator approval, cancellation/resume history, residual-risk acknowledgement, and final result. Then create an independent metadata-backup generation containing that certificate and its referenced final artifacts, parse/verify it at the destination, and append `archive_metadata_backups(phase='post_certificate', certificate_id=...)`. Wider pruning remains blocked until that copy is verified.
- **Exact files/symbols:** `src/catalog_pruning.py::publish_pruning_certificate`; `src/metadata_backup.py::create_verified_metadata_backup`; `src/archive_artifacts.py`; `src/pg_pruning.py`.
- **Database:** Certificate is append-only; corrections create a superseding certificate. Post-certificate backup evidence is a separate append-only row so verification never mutates the certificate.
- **Dependencies:** Task 5.3.
- **Tests:** Full parse/cross-check, interrupted certificate publication, equivalent retry, supersession, missing evidence refusal, failed/partial independent copy, wrong certificate generation, corrupt copied certificate, and idempotent verified post-certificate backup linkage.
- **Failure/recovery:** A completed logical prune without a certificate, or with a certificate that lacks a verified independent post-certificate copy, remains operationally incomplete and blocks wider pruning until reconciled.
- **Acceptance gate:** Certificate plus local artifacts can explain and rebuild every removed logical fact, and a separately located verified backup contains the same certificate/evidence generation.
- **Rollback:** Preserve certificates even if the database is restored from backup.

## Phase 6 — PostgreSQL maintenance after logical pruning

### Task 6.1 — Offer ordinary statistics/dead-tuple maintenance separately

- **Change:** Add an explicit post-prune option for `VACUUM (ANALYZE)` on affected tables after the logical prune certificate exists. Report dead/live tuple and relation size observations before/after.
- **Exact files/symbols:** `inspect_db.py`; `src/pg_pruning.py`; existing database maintenance helpers.
- **Database:** Run outside the row-deletion transactions. Do not claim it returns relation files to the operating system.
- **Dependencies:** Completed certified logical prune.
- **Tests:** Command generation/scope, transaction-mode refusal, cancellation/reporting, and no implicit execution.
- **Failure/recovery:** Vacuum failure does not invalidate logical pruning; record it separately.
- **Acceptance gate:** Statistics are refreshed and reporting distinguishes reusable internal space from OS-reclaimed space.
- **Rollback:** None required for ordinary vacuum; it is optional.

### Task 6.2 — Keep physical compaction as separately approved future work

- **Change:** Document and gate `VACUUM FULL`, `pg_repack`, table rewrites, physical compaction, and broad `REINDEX` as independent maintenance with downtime/disk-space/rollback planning. Do not invoke any from prune execute/resume.
- **Exact files/symbols:** Administration documentation and `inspect_db.py` refusal/confirmation paths only; no automatic hook.
- **Database:** Separate approval, backup, maintenance window, capacity check, and post-operation verification.
- **Dependencies:** Logical pruning stability reviewed over time.
- **Tests:** Assert prune commands never emit these statements.
- **Failure/recovery:** Use PostgreSQL-specific recovery plan prepared for the selected operation.
- **Acceptance gate:** Physical reclamation cannot be mistaken for or bundled with logical eligibility.
- **Rollback:** Operation-specific restore/failover plan required before approval.

## Plan 4 staged completion gates

### Gate 1 — Legacy export only

- [ ] New export schema/commands are installed in an isolated database with pruning disabled.
- [ ] One terminal legacy chunk exports plan/terminal/container evidence without source-row changes.
- [ ] Source path/size/ordinal/count/byte totals match exactly.

### Gate 2 — Session 37 final-chunk export

- [ ] A read-only current-state report proves the selected Session 37 chunk is final, unowned, unambiguous, and independent of active scan continuation.
- [ ] Export leaves all Session 37 rows untouched.
- [ ] Snapshot rows remain ineligible while full scan coverage is incomplete.

### Gate 3 — Export verification

- [ ] Every artifact parses completely, has recorded actual size, has no relevant `.part`, and is mutually equivalent.
- [ ] Exact path/size/order/count/byte comparison passes without requiring hashes.
- [ ] Residual same-size corruption risk is recorded.

### Gate 4 — Shadow rebuild

- [ ] Empty shadow rebuild uses only local ready artifacts.
- [ ] Restore routes, directory status, tape accounting, and large-file rows compare canonically with the original.
- [ ] The actual candidate has exact local routing evidence; coarse or tape-only historical ZIP metadata fails this gate.
- [ ] No automatic tape access occurs.

### Gate 5 — Chunk-scoped pruning dry-run

- [ ] The evaluator records every gate and immutable candidate range.
- [ ] `remote_file_state`, `remote_plan_files`, and small packed `files_index` candidates are separated by phase.
- [ ] `remote_snapshot_files` candidates are zero unless whole-scan/reference proof exists.

### Gate 6 — One completed Session 37 chunk pruning pilot

- [ ] Explicit operator approval names one completed chunk and a verified backup/rehearsal.
- [ ] Session 37 may remain logically `active`, but the archiver is paused/quiescent: no archiver process/advisory lock and no candidate owner/lease exists.
- [ ] Both the PostgreSQL backup and independent verified metadata-root backup are current for the candidate generation.
- [ ] Batches are small, cancellable, and verified after every table phase.
- [ ] Active/ambiguous Session 37 chunks and all required frontier/snapshot state remain untouched.
- [ ] A pruning certificate is published and the chunk remains restorable/rebuildable through artifacts.
- [ ] The published certificate and its final referenced artifacts have a verified independent post-certificate metadata backup.

### Gate 7 — Small terminal-session pruning pilot

- [ ] Select a small completed session with final scan coverage and no shared plan/snapshot references.
- [ ] Exercise the separate snapshot-row phase only after all scan-segment/frontier gates pass.
- [ ] Review database performance, audit, restore routing, rebuild, and tape accounting.

### Gate 8 — Wider logical pruning

- [ ] Expand only after both pilots are reviewed and no unexplained comparison/accounting drift exists.
- [ ] Keep per-chunk/session approval, dry-run, batch checkpoints, and certificates.
- [ ] Never convert “session is done” into blanket eligibility.

### Gate 9 — Optional later physical space reclamation

- [ ] Evaluate relation bloat and filesystem capacity after logical pruning.
- [ ] Ordinary `VACUUM (ANALYZE)` remains separate from OS space reclamation.
- [ ] `VACUUM FULL`, `pg_repack`, rewrites, compaction, or broad `REINDEX` require a new explicit maintenance plan and approval.

## Plan 4 rollback gate

- [ ] Before execution, rollback is cancellation of the unexecuted approved run.
- [ ] Between batches, rollback is stop-and-preserve-checkpoints; do not reverse already committed deletions ad hoc.
- [ ] After committed logical deletion, recover only through the verified PostgreSQL backup or artifact-driven rebuild into a replacement database.
- [ ] Preserve export artifacts, audit rows, comparison reports, and certificates across every recovery path.
- [ ] Never use tape reads, container rewrites, or deletion of a broader scope as rollback.
