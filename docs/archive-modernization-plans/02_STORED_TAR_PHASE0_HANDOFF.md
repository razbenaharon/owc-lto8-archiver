# Plan 2 implementation handoff

Status date: 2026-08-04. Plan 2 Phases 0-4 are implemented through the final
read-only Session 37 boundary rehearsal. The authoritative task list remains
`02_STORED_TAR_IMPLEMENTATION.md`.

## Current-path map (Plan 2 execution step 1)

### Discovery and chunk identity

- Production constructs `FrontierScanCoordinator` in
  `RemoteOrchestrator._run_streaming_session`; there is no legacy-scanner
  fallback.
- `SegmentChunkPublisher._seal_counted` is the production publication seam. It
  calls `PgSessionMixin.append_remote_streaming_chunk`, then seals membership.
- Remote chunk rows are created in exactly two database paths:
  `append_remote_streaming_chunk` for the frontier and `_persist_remote_plan`
  for a fixed plan. Migration-015 format selection is called inside the same
  transaction and its result is included in the initial `INSERT`. A later
  format update is never used.
- The Plan 2 Task 0.3 reference to `scan_frontier.RemoteScanCoordinator` names
  the legacy coordinator, not the sole production scanner. Both coordinators
  pass the gate inputs, but the production `SegmentChunkPublisher` path is the
  load-bearing one.

### Fetch, ZIP staging, retry and resume

- `RemotePipelineCoordinator._run_stager` reads pending chunk membership in
  persisted ordinal order and calls `RemoteChunkStager._stage_chunk`.
- The legacy ZIP route remains remote GNU TAR to local extraction followed by
  `LTOPacker` ZIP/loose splitting. `is_packed` is retained only as the adapter
  for genuinely pre-migration rows; persisted container format is authoritative.
- `_resume_pack.json` version 1 records the packaging format, staged
  container/artifact identities, and an exact path/size inventory. The shared
  read-only parser validates marker identity and actual pack contents without
  consuming the marker; production consumes it only after validation.
- `stored_tar` chunks use the direct GNU TAR producer and never fall back to
  extraction/ZIP. Startup reconciliation covers Stored TAR claims and
  TAR/sidecar publication states.

### Finite writer and catalog

- `RemoteChunkWriter._write_chunk_group` remains the sole remote tape entry and
  still owns one finite group under one LTFS ownership period. Phase 0 adds
  local descriptor/database-readiness validation before ownership; it does not
  add a readiness check between group members.
- `LTOBackup` validates and catalogs explicit container/artifact identities;
  actual staged artifact bytes drive admission/accounting. It does not infer TAR
  from `is_packed` or a filename extension.
- `archive_bundles.container_format` is backfilled for legacy ZIPs and linked to
  `archive_containers` when an explicit container exists. Stable remote run
  identity includes session, chunk, and tape generation without renumbering
  either legacy table.
- Directory-catalog remote provenance currently derives its chunk value from
  `local_chunk_index`. Legacy directory rows therefore cannot, by themselves,
  prove an exact remote chunk. The migration exception never guesses that link.

### Restore, lookup, accounting and cleanup

- `PgCatalogMixin` returns persisted/backfilled format, version, generation,
  container, member, sidecar, and locator metadata. `LTORetriever` routes from
  that durable format; only rows that genuinely lack migration-015 metadata use
  the legacy `is_packed` ZIP adapter.
- Directory restore uses canonical source roots and mixed ZIP/TAR/loose routing.
  TAR sidecars use the distinct permanent local artifact locator and never
  follow a tape locator.
- Tape admission/accounting uses actual staged artifact bytes. Cleanup includes
  container/artifact reachability and remains protected by restrictive foreign
  keys.

The authoritative plan names public methods `RemoteChunkStager.stage_chunk`
and `RemoteChunkWriter.write_chunk_group` / `write_one_chunk_owned`; the current
symbols are underscored. It also names
`archive_artifacts.publish_no_clobber`, which does not yet exist. Those symbol
discrepancies are recorded here rather than silently substituting unrelated
helpers.

## Deliberate Session 37 migration exception

The authoritative Task 0.1 text says all existing chunks become ZIP, and the
Task 4.2 default table keeps an existing never-started pending identity as ZIP.
The fixed operator decision overrides that default for one individually proved
case: completed chunks 0-48 remain immutable ZIP, while existing pending chunks
49-112 and future chunks of the same session are assigned Stored TAR.

Migration 015 implements this as a generic, evidence-gated exception, not a
session/chunk literal:

1. A read-only preflight derives a candidate boundary from the real catalog.
2. Execute requires the expected derived boundary, an approval identifier and
   reason, a verified backup, no archiver lock/process, and a readable local
   staging root with no deterministic fetch/pack path for any candidate.
3. The migration locks the relevant tables and repeats the database evidence
   query in the assignment transaction. Every suffix chunk must be pending,
    have exact contiguous fixed membership/ordinals, and have no owner, lease,
    attempt, error, file-state, worker-attempt, container, artifact, sealed-batch,
    catalog, run or measured attributable directory evidence. Fully
    unattributable legacy directory rows are counted and reported explicitly;
    they do not claim evidence about the TAR suffix. This Stored TAR eligibility
    rule is unchanged. A prefix chunk must be `done` with exact contiguous fixed
   membership, but it no longer needs positive written/catalog corroboration to
   receive the conservative legacy ZIP default.
4. The first assignment writes ZIP directly to the proved prefix and Stored TAR
   directly to the approved suffix. It never temporarily assigns ZIP and then
   mutates a format. The boundary and per-chunk evidence summary are persisted.
   Each prefix audit row durably records `prefix_evidence_basis=corroborated`
   when real written/catalog/container/artifact evidence exists, or
   `prefix_evidence_basis=status_only` when classification rests on status plus
   membership alone. Report and preflight output expose both counts.
5. Any missing, unreadable, partial or contradictory evidence aborts the whole
   transaction. Applying the normal ZIP backfill first permanently closes the
   exception; a later conversion is rejected by the write-once trigger.

This reconciles the operator decision with Task 4.2's acceptance clause that an
exception may be individually proved and approved. Membership, ordinals,
statuses, source selection, legacy ZIP identities and tape locators are not
changed. No completed ZIP is converted or rewritten.

### 2026-08-04 prefix-evidence relaxation

The first evidence rule was too strict for the verified production shape. The
following numbers came from an operator-approved read-only production query and
were not re-derived during this change:

- Session 37 has 113 chunks: 49 `done` (0-48) and 64 `pending` (49-112).
- Every pending chunk has 200,000 members with ordinals 0-199,999 and no owner,
  lease, attempt or error. `membership_state`, `expected_file_count`, and
  `expected_bytes` are NULL on every Session 37 chunk.
- Only 6 of the 49 done chunks have any `files_index` rows; 43 have none.
- Migration 015 adds `archive_runs.remote_chunk_index`, so all legacy run rows
  begin NULL and provide no per-chunk evidence.
- Session 37 has 134 `directory_archive_bundles` rows; all have NULL
  `chunk_index` (`count(distinct chunk_index)=0`).
- No Session 37 `files_index` row has NULL `remote_chunk_index`.

The approved relaxation applies only to the ZIP prefix. ZIP is the conservative
legacy default, and a `done` chunk cannot be selected for TAR because the
independent, unchanged `eligible_stored_tar` rule requires `status='pending'`.
Demanding positive written evidence merely to assign that safe default added no
safety while making the approved boundary impossible. Likewise, a legacy
directory row with no provable per-chunk provenance no longer blocks the ZIP
prefix; any directory row that does prove provenance at or after the boundary
still aborts.

The explicit residual risk is durable and operator-visible: 43 completed chunks
are classified ZIP from status plus fixed/contiguous membership rather than
corroborating written evidence. They are recorded as `status_only`; the other 6
prefix chunks are `corroborated`. This relaxation cannot grant Stored TAR and
does not soften suffix evidence, expected-boundary equality, provenance
consistency, write-once formats, immutable audit rows, or the rule that a normal
ZIP backfill permanently closes the exception.

## Plan 2 safety state

- `stored_tar_write_enabled` defaults to false in code and the example config.
- A new Stored TAR assignment additionally requires migration schema version 1
  and reader contract version 1. The reader contract is implemented; the false
  writer flag remains the independent default-off creation gate.
- Existing TAR recovery authorization does not depend on the creation flag;
  reader compatibility remains mandatory. Recovery uses the direct producer /
  validated resume paths and never reinterprets TAR as ZIP.
- ZIP-only `StagedChunk` callers remain valid. A non-empty TAR handoff requires
  database container/artifact identities, one ready sidecar per container,
  exact actual-byte accounting, matching database readiness and readable final
  local files.
- Migration 015 is explicit-only and is not part of startup schema
  initialization. Report/preflight/validation commands construct
  `PgDatabaseManager(init_schema=False)`.
- The execute path may persist a Stored TAR exception boundary only while
  `stored_tar_write_enabled=true`. Read-only rehearsal/preflight and ordinary ZIP
  backfill continue to work while the flag is false.
- No code in Phase 0 reads a tape or performs post-write verification.

As required by the authoritative plan, validation deliberately uses path, size,
ordinal, count, byte-total, structure, sidecar and restore/rebuild evidence, not
file-content or TAR-content hashes. Residual risk remains: corruption that
preserves paths, sizes and readable container structure is not detected.

## Phase 4 / Gate 5.5 completion

The final Plan 2 phase is report-only. `inspect_db.py` now exposes:

```powershell
python inspect_db.py --session37-boundary-rehearsal --db <isolated_restore>
```

The command opens PostgreSQL with `default_transaction_read_only=on`, defaults
to session 37 unless `--session-id` is supplied, reads liveness/process evidence,
calls the same database classifier used by migration-015 preflight, and inspects
the configured local staging root through the shared read-only resume-marker
parser. It reports marker identity and actual path/size inventory agreement. An
absent or unreadable staging root is `unknown`, never proof of absence. It does
not touch LTFS, consume a marker, resume work, or persist the boundary.

Each chunk report now includes the raw state and grouped evidence needed by the
operator:

- membership: `membership_state`, fixed-membership result, count, bytes, and
  ordinal range.
- owner/lease: whether owner, lease, or attempt evidence exists.
- pack/resume: file-state rows, worker attempts, sealed-batch evidence, local
  fetch/pack entry presence, resume-marker state, and actual inventory state.
- container: container/artifact rows and written-container evidence.
- writer/catalog: file catalog, archive-run, directory, writer-start,
  writer-complete, and catalog-commit evidence.
- category rule: the exact Task-4.2 category, required action, assigned-format
  inference, confidence, and blocker.
- legacy ZIP inference: ZIP confidence and blocker for chunks that must remain
  ZIP.

The category logic is table-driven in
`tests/test_session37_format_boundary.py`. It asserts every Task-4.2 rule and
explicitly proves that status alone cannot authorize format conversion or
identity reuse.

The reduced rehearsal fixture in `tests/test_pg_integration.py` preserves the
measured Session 37 shape: 113 chunks, 49 done, 64 pending, NULL membership
columns, no owner/lease/attempt/error rows, only 6 done chunks with
`files_index` evidence, 134 directory bundle rows with NULL `chunk_index`, and
zero ambiguous `files_index.remote_chunk_index` rows. It verifies:

- derived boundary is 49;
- chunks 0-48 report ZIP, with 6 `corroborated` and 43 `status_only`;
- chunks 49-112 report Stored TAR eligibility under the approved strict suffix
  rule;
- the `remote_sessions` row is unchanged;
- no boundary table is created during rehearsal.

Execute mode exists but remains an explicit guarded operator action. Persisting
the Session 37 boundary requires the writer flag to be deliberately enabled
first; the default remains `stored_tar_write_enabled=false`.
