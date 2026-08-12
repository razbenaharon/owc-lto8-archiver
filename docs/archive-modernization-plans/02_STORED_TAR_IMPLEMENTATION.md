# Plan 2 — Stored TAR Implementation

## Execution contract

1. Start only after the Plan 1 completion gate passes.
2. Implement TAR read/restore before TAR creation.
3. Keep the legacy PostgreSQL chunk-plan reader in use; manifest-first planning is Plan 3.
4. Keep files at or above `ConfigManager.zip_threshold_mb` on the existing loose-file path. Do not confuse this storage threshold with `index_min_file_mb`/`large_file_min_mb`.
5. Existing ZIP containers and tape locators are immutable.
6. Use path, size, ordinal, count, byte-total, full TAR parse, sidecar equivalence, restore, and rebuild evidence; do not require content hashes.
7. Record the residual risk that corruption preserving the same path, size, and readable structure is not detected.
8. Never add automatic tape reads. All validation in this plan uses staging or permanent local metadata.

> **Operator-approved Session 37 override (authoritative):** The generic rule
> below says pre-existing chunks backfill to ZIP. The operator explicitly
> approved the Task 4.2 exception for Session 37 after individual evidence
> review: completed chunks 0-48 remain ZIP, while never-started pending chunks
> 49-112 are assigned `stored_tar` by migration 015. This is not a conversion of
> written data. It is the one proved existing-identity exception allowed by Task
> 4.2's acceptance clause. Do not revert it or weaken its eligibility predicate.
> Persisting that boundary additionally requires
> `stored_tar_write_enabled=true`; rehearsal and ordinary ZIP backfill do not.

## Phase 0 — Add explicit, immutable format and artifact state

### Task 0.1 — Add migration 015 for chunk/container format truth

- **Change:** Add `scripts/sql/015_postgres_container_formats.sql` and an explicit schema apply/validate command. Normally backfill every pre-existing remote chunk as ZIP; the operator-approved Session 37 exception above assigns its individually proved never-started suffix directly to Stored TAR. Do not derive format from a filename at restore time. Do not treat optional/disabled migration 012 or `src.sealed_batch_repository.SealedBatchRepository.FORMAT_GENERATION=1` as per-chunk format authority; it models legacy ZIP batches and is not wired into scheduling.
- **Database:** Apply these additive changes:
  - Add `remote_sessions.default_packaging_format` with values `zip` or `stored_tar`, default/backfill `zip`.
  - Add `remote_chunks.packaging_format`, `packaging_assigned_at`, `writer_started_at`, `writer_completed_at`, and `catalog_committed_at`; backfill existing rows to `zip` and make format non-null.
  - Make a non-null chunk format write-once. Reject every later update, including an update before fetch/pack/write. Existing chunks normally become permanently ZIP at backfill. The Session 37 exception assigns the evidence-proven pending suffix on its first assignment; future chunks after the approved boundary may then receive TAR on their initial insert.
  - Add `archive_containers` keyed by `(session_id, chunk_index, container_ordinal)` with immutable `container_format`, persisted TAR dialect/version, storage class, container name, temporary data-staging locator, permanent local metadata locator, tape label/path and `tape_generation_id` when written, expected/observed member counts and bytes, actual artifact bytes, validation state, writer state, and catalog state. Link `tape_generation_id` to migration-013 `tape_generations.generation_id` and verify it matches the session/tape at write time.
  - Add `archive_artifacts` keyed by container/chunk plus artifact kind/version, with distinct local and tape locators, recorded artifact size, readiness state, and publication timestamps. Reserve kinds for `zip_manifest`, `tar_sidecar`, `plan_manifest`, and `terminal_manifest` without requiring Plans 3–4 to populate all kinds yet.
  - Add nullable `container_id` and explicit legacy `container_format='zip'` compatibility columns to `archive_bundles`; do not merge or renumber existing bundle identities.
  - Add nullable `remote_chunk_index` and `tape_generation_id` to `archive_runs` plus a partial unique identity for new remote runs. Preserve historical date/tape run labels; new remote run identity/labels include stable session label, chunk index, and tape generation so same-day chunks cannot collapse into one provenance row.
  - If migration 007 is installed, add nullable container linkage/format/tape-generation/actual-artifact-byte columns to `directory_archive_bundles` through guarded DDL. If it is absent, do not silently install migration 007.
- **Exact files/symbols:** new migration 015; add `src/pg_core.py::PgConnectionCore.apply_container_format_schema`; `src/pg_db.py::PgDatabaseManager`; new `src/pg_containers.py::PgContainerMixin`; `src/pipeline_types.py` add `ContainerFormat`, `ArtifactKind`, `ArtifactReadiness`, `ContainerWriterState` and typed records; add `inspect_db.py --apply-container-format-schema` plus validate/report flags consistent with its current flat CLI.
- **Dependencies:** Plan 1 migration 014 and sealed-membership semantics.
- **Tests:** Add isolated PostgreSQL tests to `tests/test_pg_integration.py` for idempotent apply, ZIP backfill, immutable format, unique container ordinal, artifact readiness, optional-007 behavior, and legacy `archive_bundles` compatibility.
- **Failure/recovery:** Schema is additive. Feature startup fails closed on missing/drifted schema. A failed backfill cannot leave null or inferred formats.
- **Acceptance gate:** Every existing chunk reports a durable, write-once format; all are ZIP except an individually proved and approved Task 4.2 exception such as Session 37 chunks 49-112. No existing tape/container row changes location or identity; every attempted format mutation is rejected.
- **Rollback:** Disable all new format-aware paths and continue reading backfilled ZIP rows. Do not drop populated container/artifact tables.

### Task 0.2 — Extend the in-memory staging contract

- **Change:** Extend `StagedChunk` and staged file/container metadata so one chunk can contain one immutable small-file format plus loose large files, ready local artifacts, actual staged bytes, and writer/catalog state.
- **Exact files/symbols:** `src/pipeline_types.py::StagedChunk`, `FileRecord`; `src/remote_staging.py::RemoteChunkStager`; `src/remote_writer.py::RemoteChunkWriter`; `src/backup.py::LTOBackup`.
- **Database:** Map staged identities to `archive_containers` and `archive_artifacts`; do not create a second unlinked identity namespace in memory.
- **Dependencies:** Task 0.1.
- **Tests:** Extend `tests/test_phase4_ready_queue.py::test_prepared_bytes_come_from_the_pack_not_the_remote_size` for TAR, sidecars, plan/terminal manifests, and loose files.
- **Failure/recovery:** A chunk is writer-ready only when every required artifact is final/readable and its database readiness record matches.
- **Acceptance gate:** ZIP-only `StagedChunk` fixtures remain valid; TAR fields cannot be omitted for a TAR chunk.
- **Rollback:** Retain optional fields while disabling TAR construction.

### Task 0.3 — Add one fail-closed writer feature gate

- **Change:** Add `ConfigManager.stored_tar_write_enabled`, default false in code and `config.example.ini`. Keep session default format `zip`; do not add overlapping writer/default booleans. TAR reader/restore remains available independently once implemented. Every new-chunk format assignment and every direct-TAR producer entrypoint checks the flag plus migration/version/reader readiness.
- **Exact files/symbols:** `src/config.py::ConfigManager`; `config.example.ini`; `src/pg_containers.py::PgContainerMixin.assign_new_chunk_format`; `src/scan_frontier.py::RemoteScanCoordinator`; `src/remote_staging.py::RemoteChunkStager`.
- **Database:** A disabled flag cannot insert `packaging_format='stored_tar'`. Existing TAR rows remain readable/stageable for recovery after the flag is turned off.
- **Dependencies:** Tasks 0.1–0.2.
- **Tests:** Defaults false, malformed config false, missing/drifted schema refusal, reader-version mismatch refusal, all format-assignment call sites guarded, existing TAR restore/recovery allowed while writer is disabled.
- **Failure/recovery:** Configuration uncertainty chooses ZIP for new sessions/chunks and refuses a transition that explicitly requires TAR; it never changes an existing assignment.
- **Acceptance gate:** No code path can create a TAR-assigned chunk while the flag is false.
- **Rollback:** Turn off new TAR assignment/production only; retain TAR reader, routing, sidecar lookup, and restart support permanently after the first TAR artifact exists.

## Phase 1 — Implement the TAR consumer and restore router first

### Task 1.1 — Add a strict Stored TAR parser

- **Change:** Add a streaming TAR reader that parses to end-of-archive without extraction, validates exact member names/sizes/order against an expected sidecar or plan, and returns typed member records.
- **Dialect:** Define and persist one first-version producer dialect, `gnu-pax-sparse-v1`, with explicit GNU TAR flags `--format=pax --sparse --sparse-version=1.0`; never inherit a host default. Gate creation on remote GNU TAR capability and reader fixtures for that exact dialect. If the target cannot produce/read it, leave TAR writing disabled rather than silently changing format.
- **Safety rules:**
  - Accept only planned regular-file members and explicitly supported GNU/PAX metadata needed to represent those regular files; test sparse-file representation before enabling it.
  - Reject absolute paths, drive-qualified paths, `.`/`..` traversal, NUL-invalid names, duplicate normalized names, case-fold collisions on Windows destinations, links, devices, FIFOs, directories, and every unrecognized member type.
  - Do not reuse `src.retriever._safe_restore_relpath` as-is; it currently drops unsafe components instead of rejecting the member.
  - Parse to the two zero end blocks, allow only all-zero blocking padding required by GNU TAR `-b 512`, and reject truncation, nonzero trailing bytes, concatenated archives, missing members without an explicit source exception, unexpected members, wrong size, wrong ordinal, duplicate member, and aggregate mismatch.
- **Exact files/symbols:** new `src/tar_container.py::StoredTarReader`, `validate_tar_member_name`, `validate_stored_tar`; `src/archive_artifacts.py` from Plan 1; `src/pipeline_types.py` member/container records.
- **Database:** None beyond reading persisted format/artifact locators.
- **Dependencies:** Task 0.1.
- **Tests:** Add `tests/test_stored_tar.py` with exact dialect/version, regular, sparse, GNU/PAX Unicode/long name, spaces, tabs, newlines, literal backslash, leading dash, duplicate, missing with/without exception, unexpected, unsafe, link/device/FIFO, wrong-size, corrupt-header, truncated-end, valid zero padding, nonzero trailing bytes, and concatenated-archive fixtures.
- **Failure/recovery:** Any structural uncertainty fails closed and leaves the container unvalidated. Do not attempt repair or tape access.
- **Acceptance gate:** The parser reads a multi-gigabyte synthetic TAR with bounded memory and catches every required malformed fixture.
- **Rollback:** Parser remains dormant before the first TAR exists. After any TAR pilot, retain the parser and its version support permanently; disable only new TAR creation.

### Task 1.2 — Add format-aware single-file and directory restore

- **Change:** Route restore by persisted `archive_containers.container_format`/legacy ZIP backfill, not `files_index.is_packed` alone or filename extension. Add strict TAR member extraction to a unique temporary destination, validate the exact expected output size, then atomically publish without clobbering unrelated files.
- **Exact files/symbols:**
  - `src/retriever.py::LTORetriever.run`, `_restore_many`, `_restore_packed`, `_restore_packed_bulk`, `_restore_directory_complete`, `_extract_bundle_subtree`.
  - New `src/retriever.py::_restore_container`, `_restore_tar_members`, `_route_container_format` or a focused `src/container_restore.py` if that removes ZIP/TAR duplication.
  - `src/pg_catalog.py::PgCatalogMixin.find_directory_restore_bundles`, `_derive_bundle_base_path`.
- **Required compatibility fixes:**
  - Keep ZIP extraction behavior and loose-file restore behavior unchanged.
  - Return explicit format/version, container identity, source base path, member name, tape label plus generation identity, and local/tape locator from catalog queries.
  - Stop using newline-composite `remote_sessions.remote_path` as the all-small-bundle base-path fallback; persist/return canonical source roots instead.
  - Permit one restore request to route across ZIP, Stored TAR, and loose files from different chunks/tapes.
- **Database:** Query the new container linkage; legacy rows without linkage use the explicit `zip` backfill adapter.
- **Dependencies:** Task 1.1.
- **Tests:** Extend `tests/test_retriever_restore.py` and add `tests/test_mixed_container_restore.py` for ZIP-only regression, TAR-only local restore, ZIP+TAR+loose mixed restore, multi-container directory restore, rename collision, cancellation, unsafe path refusal, and no-clobber publication.
- **Failure/recovery:** Restore copies a requested tape container only after explicit user restore routing; validation/rebuild/startup never follows a tape locator. Cancellation removes only the unique temporary destination.
- **Acceptance gate:** Reader and mixed restore tests pass before any TAR writer flag can be enabled.
- **Rollback:** Before any TAR exists, routing may remain disabled. After the first TAR pilot, retain TAR restore routing and ZIP/loose compatibility permanently; disable only new TAR assignment/creation and never rewrite existing artifacts.

### Task 1.3 — Add rare small-file lookup through local sidecars

- **Change:** Add a bounded sequential `JSONL.zst` sidecar search for an explicitly selected container/directory route. Interactive global per-file search is not a target.
- **Exact files/symbols:** `src/retriever.py`; `src/archive_artifacts.py`; `src/pg_containers.py`; retain `src/local_manifest_archive.py::search_manifests` for legacy exports.
- **Database:** Directory/container queries select the small set of relevant local sidecars; never open `directory_archive_bundles.manifest_path` if it is an LTFS locator.
- **Dependencies:** Task 1.2.
- **Tests:** Local-only search fixtures, missing local sidecar refusal, and a guard proving no `Z:\`/LTFS operation occurs.
- **Failure/recovery:** Missing local metadata produces an actionable refusal, not an automatic tape scan.
- **Acceptance gate:** An archived TAR member can be located and restored without a permanent `files_index` row in a synthetic fixture.
- **Rollback:** Retain legacy PostgreSQL/ZIP search behavior and, after the first TAR exists, retain local TAR-sidecar lookup for TAR records.

## Phase 2 — Produce Stored TAR directly from the remote stream

### Task 2.1 — Split small-container members from loose large files before transfer

- **Change:** Use the sealed legacy DB plan from `PgSessionMixin.get_chunk_files()` to partition each chunk before remote transfer. Files below `ConfigManager.zip_threshold_mb` become one or more Stored TAR container plans; files at/above it use the existing loose fetch/stage/index path. Add one `stored_tar_max_size_gb` setting defaulted from the current ZIP bundle cap and persist the resolved byte cap with the sealed chunk.
- **Deterministic boundary rule:** Traverse stable plan ordinal order. Estimate each PAX/sparse member's 512-byte headers/data padding plus end/block padding and start a new container before the configured cap; persist every ordinal→container assignment before workers start. `fetch_parallel_streams` changes concurrency only and can never change membership or ordinals. The actual finalized size remains authoritative for staging/tape admission.
- **Exact files/symbols:** `src/remote_staging.py::RemoteChunkStager`; `src/pg_sessions.py::PgSessionMixin.get_chunk_files`, `get_chunk_size_summary`; `src/planning.py::ChunkPlanner`; `src/config.py::ConfigManager.zip_threshold_mb` and new `stored_tar_max_size_gb`; `src/pipeline_types.py` container plan records.
- **Database:** Persist stable container ordinals and expected counts/bytes before generation; the chunk format must already be `stored_tar`. Keep large-file `files_index` rows and loose locators.
- **Dependencies:** Phase 1 and Task 0.2.
- **Tests:** Boundary tests at exactly the loose threshold and container cap, PAX/header/padding overhead, TAR-only, loose-only, mixed, empty/source-missing, oversized singleton, stable assignment across restart/config change, and identical assignment at different `fetch_parallel_streams` values.
- **Failure/recovery:** A changed configuration cannot repartition a sealed chunk. Retry reads its persisted format and container plan.
- **Acceptance gate:** The same sealed membership always produces the same storage-class/container assignment.
- **Rollback:** Never change an assigned chunk format. An unstarted TAR-assigned chunk remains TAR and may wait while future chunks/sessions default to ZIP.

### Task 2.2 — Add direct, injection-safe TAR streaming

- **Change:** Add a transport operation that sends the existing validated NUL-delimited relative-name list to GNU TAR over SSH and writes stdout directly to a unique local `.tar.part` file instead of piping it into local extraction.
- **Command contract:** Retain the safe foundation in `src.remote_transport._remote_tar_fetch`: validated relative paths on stdin, `LC_ALL=C tar -C <quoted base> -b 512 --format=pax --sparse --sparse-version=1.0 --no-recursion --ignore-failed-read -cf - --null -T -`, no filename interpolation, and existing SSH credential/keepalive behavior. Make output/stderr/exit classification explicit in a new result type.
- **Path/error contract:** Use Plan 1's new `src.paths.validate_remote_posix_relpath`, which preserves a legal literal Linux backslash; do not reuse the current converting `_safe_remote_relpath`. A line-oriented TAR diagnostic is never sufficient to attribute an exception to a filename containing newlines/escaped bytes. Add a machine-readable NUL-framed ordinal/path status probe for absent planned members, or classify an unassignable diagnostic `unresolved`.
- **Mbuffer contract:** For v1, use plain TAR unless a capability-checked `bash -o pipefail` wrapper preserves TAR's exit through `mbuffer`; never accept the pipe's last-process status as TAR success. Fall back to plain TAR, not an unverifiable pipeline.
- **Exact files/symbols:** `src/remote_transport.py` add `_remote_tar_store`, a structured diagnostic result, and a machine-safe source-status helper; reuse `_ssh_stream_command`, `_is_recoverable_remote_tar_warning`, and `shlex.quote`; add/use `src/paths.py::validate_remote_posix_relpath`; `src/remote_staging.py::RemoteChunkStager`; `src/runtime.py::register_proc`, `unregister_proc`, `_kill_proc_tree`, `_apply_proc_tuning`.
- **Database:** Set container build state/owner before launch; no ready artifact record until Task 2.3 completes.
- **Dependencies:** Task 2.1.
- **Tests:** Extend `tests/test_remote_hardening.py` for exact dialect/command construction, spaces/tabs/newlines/Unicode/literal backslash/leading dash, shell metacharacters, absolute/traversal rejection, path-attributed missing/permission/unreadable outcomes for special names, unassignable diagnostics→`unresolved`, TAR failure with successful `mbuffer`, permanent diagnostics, transient network classification, and sibling cancellation. Add a fake stdout streaming test proving bounded memory.
- **Failure/recovery:**
  - Transient SSH/network failures use the existing bounded exponential retry classification.
  - A first-version retry restarts the TAR at byte zero; no byte-range resume.
  - Cancellation terminates registered local/SSH children and leaves only an unready `.part` for startup reconciliation.
  - Unknown TAR warnings/outcomes become unresolved and prevent readiness.
- **Acceptance gate:** No small member is extracted locally, no source filename enters the remote shell command, and failure classes remain as precise as the current fetch path.
- **Rollback:** Disable `_remote_tar_store`; legacy ZIP staging remains available for future ZIP-assigned chunks.

### Task 2.3 — Validate the completed TAR while it remains unpublished

- **Change:** After streaming, flush and `fsync`, close, reopen, parse the full TAR, and compare it with the sealed container plan while the data file still has its unique `.tar.part` name. Do not publish the final `.tar` in this task: the trusted source-exception diagnostics captured by Task 2.2 must first be incorporated into and durably published with the sidecar in Task 2.4.
- **Validation:** Exact member name, expected/observed size, stable plan ordinal, stable container ordinal, file count, logical byte total, actual TAR byte size, supported member type, duplicate/unexpected/missing detection, end-of-archive readability, and observable source change. The expected archived-member set is the sealed plan minus members supported by explicit `source_missing`, `source_permission_denied`, or `source_unreadable` diagnostics; an unclassified missing member blocks readiness.
- **Exact files/symbols:** `src/tar_container.py::validate_stored_tar`; `src/archive_artifacts.py::publish_no_clobber`; `src/remote_staging.py::RemoteChunkStager`; `src/pg_containers.py` state methods.
- **Database:** Persist only an owner-scoped validation summary/part identity and `validation_state='validated_part'` after successful reopen/parse. Neither a final TAR locator nor a ready artifact state exists yet; TAR and sidecar readiness are coupled in Task 2.4.
- **Dependencies:** Tasks 1.1 and 2.2.
- **Tests:** Crash injection before close, after close, during parse, and after validated-part state; validate two concurrent build owners cannot adopt the same part. Cover same-size conflicting data, unsupported members, and complete plan/diagnostic equivalence. Final-name publication tests belong to Task 2.4.
- **Failure/recovery:** Invalid/truncated TAR is quarantined or deleted only while ownership is proven; it is never reused. A valid `.tar.part` may be revalidated and adopted idempotently only after owner reconciliation. Never rename it final before the sidecar protocol completes.
- **Acceptance gate:** Only fully parsed TAR parts equivalent to the plan plus the captured explicit source-exception set can proceed to paired publication.
- **Rollback:** Preserve owner-reconciled validated parts and diagnostics; mark them unconsumed if downstream activation is disabled.

### Task 2.4 — Build and publish a versioned TAR sidecar

- **Change:** While the validated data file is still `.tar.part`, stream a `tar-sidecar-v1` `JSONL.zst` artifact from its full parse plus Task 2.2's trusted source diagnostics under the permanent `ConfigManager.local_manifest_archive_root`, never under the disposable pack tree. Use `record_type='member'` for an actual TAR member and `record_type='source_exception'` for an absent planned ordinal. Member records contain exact TAR member name, original canonical source path, expected size, observed archived size, stable plan ordinal, container ordinal, session/chunk/container identity, and the observable disposition; exception records have no invented TAR member name and carry the trusted diagnostic/status evidence. Together the records account for every plan ordinal exactly once.
- **Disposition mapping:**
  - Present exact member: candidate `archived` after writer/catalog completion.
  - Planned but absent with explicit remote diagnostic: `source_missing`, `source_permission_denied`, or `source_unreadable`.
  - Present with size mismatch or plan/source observation drift: `source_changed`; reject the TAR as ready for that plan.
  - Unclassifiable warning, parse discrepancy, or interrupted observation: `unresolved`; block processing.
- **Exact files/symbols:** `src/archive_artifacts.py`; `src/tar_container.py`; `src/remote_transport.py` diagnostic result; `src/remote_staging.py`; `src/pipeline_types.py` add `SourceDisposition`.
- **Publication order:** Complete and fully validate the sidecar `.part`; atomically publish the permanent sidecar first; then atomically rename the already validated `.tar.part` to its final local TAR name without clobber; then use one owner-checked database transaction to persist the sidecar root-relative locator/version/size, final temporary TAR data locator/size, aggregate dispositions, and readiness of the pair. A writer can see the pair only after that transaction. Plan 3 will add the final terminal-state artifact.
- **Database:** Persist both readiness records in one compare-and-swap transaction after the ordered filesystem publication. Keep the TAR data locator explicitly local/temporary until it is written to tape; the permanent sidecar remains rooted at `ConfigManager.local_manifest_archive_root`.
- **Dependencies:** Task 2.3.
- **Tests:** Sidecar roundtrip/equivalence, all dispositions, count/byte mismatch, duplicate ordinal, invalid `.part`, two concurrent publishers, no-clobber behavior on Windows, and crash injection after sidecar validation, after sidecar publication, after TAR publication, and before/after the paired DB commit. Cover sidecar-final/TAR-part adoption, equivalent final reuse, and missing-sidecar refusal when source exceptions existed.
- **Failure/recovery:** A final sidecar plus matching validated TAR part is revalidated, the TAR publication is completed, and the pair is adopted idempotently after owner checks. A final sidecar plus final TAR but missing DB commit is fully revalidated and adopted. A final TAR with no sidecar is never writer-ready: a sidecar may be reconstructed from plan plus TAR only when every planned member is present and exact, so no absent-member disposition must be inferred. If any planned member is absent or prior diagnostics are needed, refuse adoption and, only before writer start and with proven ownership, rebuild from the source rather than inventing exception evidence. Never regenerate from or inspect tape. Copy/link the ready sidecar into the Robocopy pack for tape co-location, but pack cleanup never removes the authoritative permanent copy or changes its database locator.
- **Acceptance gate:** TAR and sidecar are mutually equivalent and locally readable without extraction or tape access, and no crash window can publish a TAR while discarding its only trusted source-exception evidence.
- **Rollback:** Preserve ready sidecars/TARs; block writing rather than converting them to ZIP.

### Task 2.5 — Replace extraction/ZIP staging only for TAR-assigned chunks

- **Change:** In `RemoteChunkStager`, route `zip` chunks through the current `_remote_tar_fetch` → local extraction → `LTOPacker` flow and route `stored_tar` chunks through direct TAR storage. For a TAR chunk, keep the current loose-large sequence: large-only `_remote_tar_fetch` extraction, Windows-safe collision mapping, exact local-size validation, `_robocopy_file` staging, canonical source-path remap, and later `files_index` insertion. Stage TAR, a copy of the permanent sidecar, any available plan/terminal artifacts, and loose large files in the same pack directory consumed by Robocopy.
- **Exact files/symbols:** `src/remote_staging.py::RemoteChunkStager.stage_chunk`; `src/remote_transport.py::_remote_tar_fetch`, `_remote_tar_store`; `src/packer.py::LTOPacker`; `src/pipeline_types.py::StagedChunk`.
- **Database:** Format dispatch reads the persisted chunk format. Store artifact/container records before writer readiness.
- **Dependencies:** Tasks 2.1–2.4.
- **Tests:** ZIP regression, TAR-small no-extraction assertion, unchanged large-only/mixed loose path, Windows collision mapping, exact loose size, mixed TAR+loose pack directory, all-source-missing `StagedChunk.skip_tape` outside LTFS ownership, no empty TAR, `_resume_pack.json` exact inventory, cancellation, staging full, retry/reuse, and cleanup ownership.
- **Failure/recovery:** Resume markers include format, container identity, artifact names/sizes, and readiness. A marker mismatch deletes/rebuilds only locally owned staging; never reinterpret ZIP as TAR or vice versa.
- **Acceptance gate:** A TAR-assigned chunk reaches `ReadyQueue` without creating its small files on disk.
- **Rollback:** Stop producing new TAR-assigned chunks; already ready TAR chunks remain resumable and format-aware.

### Task 2.6 — Extend startup reconciliation for TAR artifact pairs

- **Change:** Extend Plan 1 startup reconciliation with explicit TAR/container cases: orphan `.tar.part`; expired/unknown build owner; validated TAR part before sidecar; final sidecar plus TAR part; final sidecar plus final TAR before DB commit; final TAR with absent sidecar; ready pair with missing/invalid member; final/part collision; ready `_resume_pack.json` after restart; and database artifact state inconsistent with local files.
- **Exact files/symbols:** `src/startup_reconcile.py::reconcile_tar_artifacts`; `src/tar_container.py`; `src/archive_artifacts.py`; `src/pg_containers.py`; `src/remote_staging.py::RemoteChunkStager`.
- **Database:** Adopt an existing final pair only after full plan/TAR/sidecar equivalence and compare-and-swap owner/state checks. Delete/quarantine a local `.part` only when no live owner can hold it. A final TAR without a sidecar follows Task 2.4's all-members-present reconstruction rule; it is never enough to reconstruct absent-member outcomes.
- **Dependencies:** Tasks 2.3–2.5.
- **Tests:** One crash/restart fixture for every listed state combination, plus concurrent owner and indeterminate owner. Assert reconciliation is idempotent and performs no tape operation.
- **Failure/recovery:** Conflicting finals, unknown ownership, or mismatched ready records block the chunk. Never regenerate from or inspect tape.
- **Acceptance gate:** Every TAR/sidecar/DB/staging combination has one deterministic no-tape recovery or fail-closed outcome.
- **Rollback:** Retain read/reconcile support for every TAR already created; disable only new creation.

### Task 2.7 — Adapt staging admission and progress monitoring

- **Change:** Replace the current two-materialized-tree estimate and extracted-directory growth signal for TAR-assigned chunks. Reserve aggregate space for concurrent `.tar.part` streams, 512-byte/PAX/sparse overhead, permanent sidecar temporary/final files, loose extraction/staging, pack copies, and the staging reserve. Monitor TAR `.part` byte growth plus loose-file progress and aggregate all parallel containers for stall/hard-overrun decisions.
- **Exact files/symbols:** move/adapt `src/remote_orchestrator.py::_await_staging_capacity`, `_start_fetch_monitor` into `src/remote_staging.py::RemoteChunkStager`; `src/resource_governor.py::ResourceGovernor`; `src/ready_queue.py::ReadyQueue`; `src/config.py` staging limits.
- **Database:** None beyond actual artifact size updates.
- **Dependencies:** Tasks 2.1–2.6.
- **Tests:** Add direct-TAR admission at boundary, multi-stream aggregate overrun, stalled `.part`, progressing sparse TAR, loose+TAR reserve, sidecar overhead, cancellation, and unchanged ZIP estimate tests in `tests/test_staging_space.py`, `tests/test_remote_hardening.py`, and `tests/test_resource_governor.py`.
- **Failure/recovery:** Refuse before launch when the conservative reservation cannot fit. A mid-stream hard-overrun cancels producers, keeps the chunk non-ready, and leaves recoverable owned `.part` evidence.
- **Acceptance gate:** Direct TAR no longer reserves space for millions of extracted small files, yet cannot exceed the configured staging safety envelope.
- **Rollback:** Route future work to ZIP; preserve TAR-aware monitoring for resumable TAR chunks.

### Task 2.8 — Benchmark direct Stored TAR against the current materialization path

- **Change:** Add a reproducible, offline performance harness that feeds the same generated small-file corpus and byte stream through (a) current remote-TAR extraction plus `LTOPacker` ZIP_STORED and (b) direct Stored TAR plus sidecar publication. Report file count, logical bytes, output bytes, wall time, process CPU time, peak RSS, peak/final staging footprint, temporary file/entry count, and time to writer-ready output. Include small/medium/large cardinality profiles and a sparse-file profile; keep optional controlled-lab SSH input separate from the required offline comparison and never access LTFS.
- **Exact files/symbols:** new `scripts/benchmark_stored_tar.py`; `src/remote_transport.py::_remote_tar_fetch`, `_remote_tar_store`; `src/packer.py::LTOPacker`; `src/tar_container.py`; `src/archive_artifacts.py`; reuse the aggregate resource-sampling helpers without writing filenames to `SUMMARY.csv`.
- **Database:** None. Store benchmark output outside production statistics and generated output paths already ignored by the repository; do not create per-file catalog rows.
- **Dependencies:** Tasks 2.2–2.7.
- **Tests:** Add a small deterministic smoke fixture that checks both flows receive identical path/size inputs, validate equivalent logical membership, clean up only harness-owned temporary data, and report every required metric. The benchmark itself is not a pass/fail timing unit test.
- **Failure/recovery:** A failed or interrupted benchmark removes only its uniquely owned temporary workspace, records the incomplete sample, and never changes configuration, PostgreSQL, staging used by a live run, or tape state.
- **Acceptance gate:** A versioned comparison report makes the CPU, staging-footprint, throughput, and time-to-ready tradeoff explicit enough to decide whether to enable the TAR writer; no claimed benefit relies on a hardware tape read.
- **Rollback:** Remove the harness/report; runtime format and artifact state are unchanged.

## Phase 3 — Integrate with writer, capacity, and catalog commits

### Task 3.1 — Adapt current catalog inputs for TAR without extraction

- **Change:** Add a streaming TAR catalog adapter that joins the sealed legacy DB plan with the validated sidecar and produces the canonical metadata consumed by `LTOBackup._run_locked`, without materializing member files. Create/link `archive_containers` and compatibility `archive_bundles` rows, satisfy `files_index.ck_packed_has_bundle` for every TAR member still selected by `catalog_policy`, populate directory contributions from plan/sidecar data, and pass loose records through unchanged.
- **Exact files/symbols:** new `src/container_catalog.py::TarCatalogAdapter`; `src/backup.py::LTOBackup._run_locked`; `src/pg_catalog.py::PgCatalogMixin._normalize_file_records`, `bulk_upsert_files`, `bulk_upsert_directory_catalog`; `src/db.py::_apply_canonical_remote_paths`; `src/pg_containers.py`; `src/pipeline_types.py::FileRecord`.
- **Database:** Link the independent legacy `archive_bundles` identity to `archive_containers`; persist container format and tape generation. Create/link the exact remote session/chunk/tape-generation `archive_runs` row instead of the current date+tape row with null remote session. Use canonical remote source root/path, never a transient fetch/pack `original_root_dir`. Preserve index-threshold behavior until Plan 3 removes new small rows.
- **Dependencies:** Phase 2.
- **Tests:** Extend `tests/test_postgres_only_helpers.py`, `tests/test_pg_integration.py`, and `tests/lto_fakes.py::MinimalBackupDB` for TAR indexed/unindexed members, packed-bundle check, catalog policy, canonical root, directory contributions, tape generation, loose pass-through, and idempotent retry.
- **Failure/recovery:** Catalog adapter disagreement with plan/sidecar blocks catalog commit and leaves post-writer state ambiguous under existing rules; it never reads or rewrites tape.
- **Acceptance gate:** A TAR chunk can complete every current `LTOBackup` catalog call with the same logical member/large-file truth as a ZIP chunk.
- **Rollback:** Keep the adapter/read path for existing TAR; disable new TAR creation.

### Task 3.2 — Gate tape capacity on actual staged bytes

- **Change:** Replace the pre-write use of planned logical bytes in `RemoteChunkWriter._ensure_remote_chunk_fits_tape` with an admission check over the sum of `StagedChunk.staged_bytes` for the selected finite group, including TAR/ZIP bytes, sidecars/manifests, loose files, and required safety headroom. Alternatively persist/decrement one cumulative reservation under the same ownership period; never compare each chunk independently against an unchanged available value. Keep scanner logical-byte reservation as planning/backpressure only, not tape authority.
- **Exact files/symbols:** `src/remote_writer.py`; `src/remote_orchestrator.py::_ensure_remote_chunk_fits_tape` until extraction completes; `src/ready_queue.py::ReadyQueue`; `src/pg_tapes.py::PgTapeMixin._calculate_tape_used_space_conn`; `src/backup.py::LTOBackup`.
- **Database:** Record both logical member bytes and actual artifact bytes. Update tape used-space calculation to include actual new artifact sizes exactly once while retaining legacy `files_index`, `directory_archive_bundles`, and `local_manifest_folder_aggregates` compatibility.
- **Dependencies:** Task 3.1.
- **Tests:** Extend `tests/test_operational_hardening.py`, `tests/test_phase4_ready_queue.py`, and isolated PostgreSQL tape-accounting tests for sidecar overhead, mixed loose/TAR, legacy ZIP, pruned legacy aggregates, and no double count.
- **Failure/recovery:** If actual staged bytes exceed the available tape budget, fail before acquiring/starting Robocopy and preserve the pack. Never probe LTFS during queue wait.
- **Acceptance gate:** The writer cannot admit a chunk using a smaller logical-byte estimate than its staged footprint.
- **Rollback:** Fall back only to a conservative larger estimate; never fall back to undercounting actual staged bytes.

### Task 3.3 — Separate copy completion, catalog commit, and final chunk completion

- **Change:** Make the existing ordering explicit: writer starts → Robocopy result classified → container/tape locator persisted → directory/file catalog committed idempotently → chunk `done`. Record an ambiguous state if the process stops after writer start and before durable completion evidence.
- **Exact files/symbols:** `src/remote_writer.py::RemoteChunkWriter.write_one_chunk_owned`; `src/backup.py::LTOBackup._run_locked`; `src/robocopy.py::classify_robocopy_result`; `src/pg_containers.py`; `src/pg_catalog.py::PgCatalogMixin.bulk_upsert_directory_catalog`; `src/pg_sessions.py::update_chunk_status`.
- **Database:** Use `archive_containers.writer_state/catalog_state` and chunk timestamps from migration 015. Upserts must be idempotent by stable session/chunk/container identity. Fix remote directory-catalog chunk propagation by passing an explicit `remote_chunk_index`; do not write remote chunk provenance from `local_chunk_index`.
- **Dependencies:** Task 3.2.
- **Tests:** Extend `tests/test_remote_failure_hardening.py` with success, failure before writer start, Robocopy hard failure, copy-success/DB-failure, partial catalog commit, restart reconciliation, and no automatic retry from ambiguous backing.
- **Failure/recovery:** Never infer a completed tape write from a local TAR or Robocopy launch. A copy-success/catalog-failure remains blocked for no-tape evidence reconciliation; do not rewrite automatically.
- **Acceptance gate:** `done` means robust Robocopy success plus all required catalog/container commits; every earlier failure remains distinguishable.
- **Rollback:** Existing ZIP completion semantics remain supported; do not downgrade recorded TAR writer evidence.

### Task 3.4 — Preserve finite-group tape behavior

- **Change:** Feed TAR-ready chunks through the existing finite `ReadyQueue` grouping without adding readiness, label, directory, or capacity probes between chunks or while idle.
- **Exact files/symbols:** `src/remote_writer.py::RemoteChunkWriter.write_chunk_group`; `src/ready_queue.py::ReadyQueue`; `src/ltfs.py`; `src/ltfs_ownership.py`; `src/backup.py`.
- **Database:** No additional schema.
- **Dependencies:** Tasks 3.1–3.3.
- **Tests:** Keep `tests/test_phase4_ready_queue.py::test_group_writes_all_chunks_consecutively`, `test_ownership_released_after_the_group`, `test_later_failure_preserves_earlier_successes`, `test_unstarted_chunks_are_not_written_after_a_failure`; keep `tests/test_phase45_control_signals.py::test_no_device_work_between_chunks` and `test_no_ltfs_while_waiting_for_the_next_group`. Extend `tests/lto_fakes.py::MinimalBackupDB`, `TapeLockObserver`, and patch targets for new artifact/container calls and the relocated writer.
- **Failure/recovery:** A hard tape failure stops the group and pipeline, preserves unstarted packs, records last success, and performs no automatic recovery/eject/remount/ltfsck.
- **Acceptance gate:** Container format is invisible to LTFS ownership sequencing.
- **Rollback:** Disable TAR enqueueing; no tape-state rollback.

## Phase 4 — Session 37 migration-boundary classification and rehearsal

### Task 4.1 — Produce a quiescent, read-only classification report

- **Change:** Extend the Plan 1 report to classify every current Session 37 chunk from authoritative database, process/advisory-lock, staging marker, container/catalog, and failure evidence. Do not touch LTFS.
- **Exact files/symbols:** `inspect_db.py`; `src/session_reconcile.py`; `src/pg_sessions.py`; `src/pg_containers.py`; `src/startup_reconcile.py`; `src/remote_staging.py` resume-marker parser.
- **Database:** Read-only. Report `membership_state`, current status, owner/lease, fixed membership count/bytes, existing pack/container evidence, writer/catalog evidence, and inferred legacy ZIP format with confidence/blocker.
- **Dependencies:** Plan 1 Task 4.1 and migration 015 available in an isolated rehearsal database.
- **Tests:** Fixtures for every category below and contradictory historical evidence.
- **Failure/recovery:** Indeterminate evidence blocks conversion. Do not hardcode the written 0–48/49–112 baseline.
- **Acceptance gate:** Operator can see why each existing chunk is immutable ZIP, retryable as ZIP, or blocked; only not-yet-created post-boundary chunks are TAR candidates.
- **Rollback:** Report-only.

### Task 4.2 — Apply fail-closed category rules

- **Change:** Classify every existing Session 37 chunk with the following fail-closed table; status alone never authorizes a format conversion or identity reuse.

| Observed category | Required format/action | Migration effect |
|---|---|---|
| `done` with written/catalog evidence | Immutable ZIP | Backfill `zip`; preserve tape/container locators |
| Copy may have succeeded but catalog finality is absent | Manual reconciliation; no rewrite | `backing`/ambiguous; blocks boundary activation |
| `fetching` | Reconcile owner/staging; retry as ZIP only | No automatic format change |
| `packing` | Reconcile owner/pack marker; retry as ZIP only | No automatic format change |
| `backing` | Ambiguous hard block | No retry or conversion |
| Existing `pending` with fixed membership | ZIP by default | Seal/backfill as legacy ZIP |
| Existing `pending` proven never owned, staged, packed, written, or cataloged | Retry/continue as ZIP | Do not repurpose the existing chunk identity; future post-boundary chunks may use TAR |
| `fetch_failed`/`backup_failed` | Reconcile exact attempt; retry as ZIP if safe | No format change |
| Existing chunk with absent/conflicting membership | Manual reconciliation; do not reuse its identity | Blocks activation until resolved; create only later chunk identities under the approved boundary |
| Stale/conflicting evidence | Manual reconciliation | Blocks migration |
| Future chunk not yet created after persisted boundary | Stored TAR eligible | Assign TAR at creation/seal |

- **Exact files/symbols:** `src/pg_containers.py::PgContainerMixin.assign_new_chunk_format`; `src/session_reconcile.py`; `src/scan_frontier.py`; `inspect_db.py` rehearsal output.
- **Database:** No update in rehearsal mode. Execute mode in Plan 3 will persist the boundary and planning-source transition.
- **Dependencies:** Task 4.1.
- **Tests:** A table-driven `tests/test_session37_format_boundary.py` asserting every rule, including no conversion on status alone.
- **Failure/recovery:** Existing membership or artifact evidence always wins over a desired default. Never rewrite tape to standardize formats.
- **Acceptance gate:** The derived safest boundary is after the verified maximum existing fixed-membership chunk; any exception is individually proven and approved.
- **Rollback:** No state change during Plan 2 rehearsal.

## Phase 5 — Activation sequence

### Gate 5.1 — TAR reader support

- [ ] Migration 015 is validated in an isolated PostgreSQL database.
- [ ] All existing rows route from persisted format metadata: ZIP by normal backfill, with only the documented, individually proved Session 37 suffix exception routing to Stored TAR.
- [ ] Strict TAR parser handles all supported fixtures and rejects unsafe/unsupported members.
- [ ] TAR creation flags remain off.

### Gate 5.2 — Local restore

- [ ] TAR restore from a local synthetic container passes exact path/size/count validation.
- [ ] Restore lands through a unique temporary path and never clobbers a conflict.
- [ ] ZIP and loose restore regressions pass.
- [ ] No test or startup path accesses LTFS automatically.

### Gate 5.3 — Mixed routing

- [ ] One synthetic session restores ZIP, TAR, and loose members by persisted format.
- [ ] Directory routing spans multiple containers and tapes without using `remote_sessions.remote_path` as a composite-path fallback.

### Gate 5.4 — Lab-only TAR creation

- [ ] `stored_tar_write_enabled` exists and defaults to false in `src/config.py` and `config.example.ini`; the persisted session default remains `zip` until an approved transition changes the default for future chunk creation.
- [ ] Direct remote-stream fixtures create no extracted small files.
- [ ] `.part`, retry, source-error, validation, sidecar, staging-space, capacity, and crash-injection tests pass.
- [ ] Benchmarks compare current remote-TAR/extract/ZIP with direct Stored TAR for file count, wall time, CPU, memory, and staging footprint.

### Gate 5.5 — Session 37 boundary rehearsal

- [ ] Run only against a restored isolated database and copied local metadata/staging fixtures.
- [ ] Produce the complete category report and proposed boundary without modifying Session 37.
- [ ] Verify every pre-boundary chunk stays ZIP and every ambiguous chunk blocks activation.

### Gate 5.6 — Limited TAR pilot

- [ ] Follow the repository progression: offline tests → isolated PostgreSQL tests → small synthetic hardware pilot.
- [ ] Use a new bounded pilot group and review Robocopy, catalog, actual-byte accounting, restore, and no-idle-LTFS evidence.
- [ ] Do not perform an independent tape read after the write.

### Gate 5.7 — Future Session 37 TAR continuation

- [ ] Requires explicit operator approval after Plan 3 persists the migration boundary and manifest-first plan source.
- [ ] No existing fixed ZIP chunk is silently reinterpreted.
- [ ] A single bounded production group is reviewed before broader continuation.

### Gate 5.8 — Production default for new chunks

- [ ] Enable only after reader, local restore, mixed routing, pilot, writer ambiguity, capacity, and catalog tests pass.
- [ ] Keep a per-session ZIP default override and immutable per-chunk format.
- [ ] Rollback disables new TAR assignment only; it never converts or deletes an existing TAR or ZIP container.
