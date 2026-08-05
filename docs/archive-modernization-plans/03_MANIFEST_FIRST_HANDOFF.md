# Plan 3 implementation handoff

Status date: **2026-08-05**. Plan 3's **implementation is complete** - Phases 0-4 and Tasks 3.1-3.3 are written and tested. What is **not** done is the production rollout: Approvals A, B and C are blocked by a hardware fault (see "Blocked: the LTFS mount" below), and Task 3.4's pilot and bounded group have not run. The authoritative task list remains
`03_MANIFEST_FIRST_DIRECTORY_CATALOG_AND_SESSION_37.md`.

**Nothing has been applied to production.** No migration, no session resume, no
tape or LTFS access, no PostgreSQL pruning. Approvals A–E have not been
requested. Plan 4 has not been started.

## Migration numbering

The plan text calls its migration "016". **That number belongs to Plan 2.**
Plan 3's migration is **`scripts/sql/018_postgres_manifest_directory_catalog.sql`**
and carries both Task 1.1 (planning source, transition epochs, artifact
authority) and Task 2.1 (the normalized directory catalog), because Task 2.1
says "extend migration 016" and one atomic explicit migration is safer than
extending an applied one.

## What is implemented

| Task | Modules | Tests |
|---|---|---|
| 0.1 / 0.2 audit | `src/schema_audit.py`, `inspect_db.py --schema-provenance-audit` | `tests/test_schema_audit.py` |
| 1.1 + 2.1 schema | `scripts/sql/018_*.sql`, `PgConnectionCore.apply_manifest_directory_catalog_schema` | `tests/test_migration_018.py` |
| 1.2 artifacts | `src/plan_manifest.py`, `src/terminal_manifest.py`, `src/scan_state_manifest.py`, `Plan3ArtifactWriter` in `src/archive_artifacts.py` | `tests/test_manifest_schemas.py` |
| 1.3 plan source | `src/plan_source.py`, wired through `src/remote_pipeline.py` | `tests/test_plan_source.py` |
| 1.4 + 1.5 | `src/manifest_first.py`, `PgSessionMixin.create_manifest_chunk` / `record_terminal_manifest` | `tests/test_manifest_first_scan.py` |
| 2.2 status | `src/directory_status.py`, `PgDirectoryCatalogMixin.recalculate_directory_completeness` | `tests/test_directory_completeness.py`, `tests/test_directory_catalog_pg.py` |
| 2.3 adapters | `PgDirectoryCatalogMixin.ingest_legacy_directory_parts` | `tests/test_directory_catalog_pg.py` |
| 3.1 boundary | `src/session_transition.py`, `PgSessionMixin.audit_session_transition_evidence` | `tests/test_session37_transition.py` |
| 3.2 export | `src/legacy_export.py`, `PgSessionMixin.iter_legacy_chunk_membership` | `tests/test_legacy_export.py` |
| 3.3 continuation | `SegmentChunkPublisher._seal_manifest_first`, `active_manifest_boundary` | `tests/test_scan_frontier.py` |
| 3.1 execute | `PgDirectoryCatalogMixin.persist_session_transition` | rehearsal + `tests/test_session37_transition.py` |
| 4.1 restore routing | `PgDirectoryCatalogMixin.find_directory_restore_parts` | `tests/test_directory_catalog_pg.py` |
| 4.2 + 4.3 rebuild/compare | `src/catalog_rebuild.py` | `tests/test_catalog_rebuild.py` |

Suite: **1,654 at baseline → 1,892 passing, 0 failed.**

## Blocked: the LTFS mount (2026-08-05)

Approvals A and C are blocked by one physical fault, documented in
[incident 012](../incidents/012-20260805-ltfs-mount-absent-blocks-backup.md):

* **No cartridge is mounted.** `Z:` is a drive letter with an empty
  `FileSystem`, `Size` and `FreeSpace`; `Test-Path` on it still returns True,
  which is the incident-011 trap. There are no LTFS mount events (61259).
* **`Z:` has a duplicated device symlink** (the same LTFS device listed
  twice), so `src/pg_backup.py::_windows_device_target` refuses to resolve it
  and **every `--backup-postgres` run fails**. The guard is correct - it
  exists to prove a dump never lands on the tape drive - and must not be
  relaxed to work around this.

Consequences: **Approval A** cannot take a fresh pre-migration backup;
**Approval C** cannot run a pilot against an unmounted drive; **Approval B**
is database-only and technically unblocked, but is deliberately not exercised
without A.

Production is unaffected: the last mutation was **2026-08-02 09:56**, so the
existing `..._20260803_151701.dump` is a faithful image of the current
catalog and was used for the isolated rehearsal.

## What is NOT done

- **Task 3.4** - no synthetic pilot, no bounded production group.
- Approvals A, B and C - see above.
- The independent adversarial review of the whole plan.

## Findings that changed the plan

### Migration 015's exception gate was unsatisfiable

`ambiguous_run_count` required every archive run to own at least one
`files_index` row naming a chunk. `files_index` indexes only files at or above
`index_min_file_mb`, so a chunk built entirely from small files produces none —
by design. On production this flagged **7 of session 37's 9 archive runs** and
made the approved Stored-TAR exception impossible to apply.

Forensics (read-only, 2026-08-04) decomposed the 7:

| runs | why flagged | verdict |
|---|---|---|
| 931, 969, 988 | small-file-only chunks; 1.8M / 3.6M / 1.4M files in `directory_archive_bundles` | false positive |
| 792, 801, 830, 889 | no catalog evidence of any kind | output destroyed and redone |

**No chunk carries a completion timestamp from 2026-07-12 through 07-15**, and
the timeline resumes at chunk 18 — exactly where incident 010's lost span
begins. Those four runs are the SCCM-restart window; their work was superseded.

The predicate now asks the question it meant to ask: *does output exist that
cannot be attributed to this session?* It accepts `directory_archive_bundles`
evidence, treats a run with no output at all as superseded, and still refuses a
run whose output names a foreign session or carries no chunk identity. Verified
against production: **7 → 0**, with five tests proving it still rejects.

**Not proven, and not claimable:** that those four runs left nothing physically
on Tape_02. `used_space` is recalculated from the catalog, so it cannot
corroborate media contents, and confirming it would require reading the tape.

### Session 37 legacy routing is coarse

All 134 `directory_archive_bundles` rows for session 37 have `chunk_index`
NULL, and legacy bundles have no `archive_containers` row. Per-member ZIP
routing therefore cannot be proven, so legacy exports are `routing_precision =
'coarse'`. `plan_manifest.is_authoritative()` refuses coarse evidence as
authority: it cannot drive row-free restore or qualify anything for pruning
until an exact-routing generation supersedes it. This is the correct
conservative outcome, not a gap to close by guessing.

## Verified production evidence (read-only)

Transition proposal, derived entirely from the live catalog:

```
session             : REMOTE_srv01_20260709_234150 (id 37)
existing chunks     : 113 (0..112)
last legacy chunk   : 112     first manifest chunk : 113
scan complete       : False   uncovered scope      : True
source scopes       : 65      tape generation      : 1
chunk states        : done=49, pending=64
packaging formats   : none recorded  (migration 015 not applied)
no blockers: the boundary is activatable
```

Bounded legacy export at real scale:

| chunk | records | export | verify | artifact |
|---|---|---|---|---|
| 0 | 13,608 | 14.8 s | equivalent | 254 KB |
| 48 | 200,000 | 6.6 s | equivalent | 1.42 MB |

Extrapolated: the whole session is ~13 minutes and ~160 MB of artifacts.

## Design decisions worth keeping

- **Artifact before row.** Sealing publishes the plan manifest, then commits the
  chunk. A crash between them leaves a ready artifact and no chunk, which the
  idempotency lookup finishes. The other order would leave a chunk whose
  membership is unknowable.
- **The schema is the authority for directory status.**
  `resolve_directory_status()` mirrors
  `directory_completeness_derived_status_ck` line for line. Three bugs in the
  Python were caught by that constraint during development: missing recursive
  aggregates, an `ambiguous`/`incomplete` divergence, and
  `all_planned_items_terminal` computed as "nothing unresolved" instead of
  "every entry accounted for".
- **A loose part is a contribution that exists.** `loose_record_key` is a
  foreign key into `files_index`. Planned-but-unwritten work is modelled as a
  container part, since `archive_containers` exists from `planned` onward.
- **A manifest chunk with no readable artifact blocks.** It never falls back to
  the database, whose per-file rows are absent by design.
- **`archived` is derived in one place** and requires durable writer completion
  *and* catalog commit. An ambiguous writer result becomes `unresolved`.

## Residual risk

No mandatory content hashes anywhere. A same-size, same-structure replacement of
a source file is **undetectable** by any check in Plan 3 — plan manifests, TAR
sidecars, terminal manifests and the semantic comparison all compare path, size,
ordinal and counts. Hashing would mean reading every byte over SSH, which is the
fetch the pipeline exists to schedule. The risk is recorded rather than papered
over.

## Operating notes

```powershell
# read-only audits (safe against production)
python inspect_db.py --schema-provenance-audit [--json]
```

PostgreSQL tests still require an explicit disposable server; see
`AGENTS.md`. `tests/pg_test_guard.py` remains fail-closed.

**Do not edit `src/` while a full suite is running.** Several tests use
`inspect.getsource()`; `linecache` re-reads the changed file and reports shifted
source, producing failures that vanish on re-run.
