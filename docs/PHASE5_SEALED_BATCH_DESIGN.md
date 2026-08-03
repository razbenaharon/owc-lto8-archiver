# Phase 5 — Sealed Physical-Write-Batch Design

**Status: DESIGN ONLY (Phase 5A). No production schema migration has been
approved or executed.** This document is the authoritative Phase 5 design. It is
proved by the executable reference model in
[`tests/sealed_batch_model.py`](../tests/sealed_batch_model.py) and
[`tests/test_phase5a_sealed_batch_model.py`](../tests/test_phase5a_sealed_batch_model.py),
and the proposed DDL lives (non-executable) in
[`docs/phase5_sealed_batch_schema.sql`](phase5_sealed_batch_schema.sql).

---

## 1. Purpose

Phase 4 made the streaming writer hold LTFS ownership once and write several
prepared chunks consecutively, collapsing ~124 index-dirty transitions per run
toward roughly one per group. That grouping is **in-memory only**
(`ReadyQueue`): a crash loses the group boundary, and recovery re-derives work
per chunk. Phase 5 gives the physical write group a **durable, sealed identity**
in PostgreSQL so that a crash at any point can be reconciled deterministically —
without ever weakening per-chunk truth and without any chance of a wedged
cartridge, a double write, or a lost index.

## 2. Non-goals

- Not replacing `remote_chunks` as the unit of file identity, write result, or
  catalog durability. Chunks stay authoritative.
- Not changing the archive format. No ZIP→TAR conversion; `format_generation=1`
  is the existing legacy ZIP pack.
- Not changing LTFS sync policy (`time@5` stays). Phase 5A only *defines* the
  synchronization contract; it issues no explicit LTFS sync.
- Not introducing automatic retries for hard tape failures.
- Not a group-level durability shortcut: "all robocopy returned 0" is explicitly
  **not** durability.
- Not resuming Session 37, not touching any production chunk, and not converting
  anything in-place.

## 3. Existing Phase 4 behavior (baseline)

```
prepare several existing ZIP chunks on local NVMe
select a finite ready group   (ReadyQueue, byte+count bounded)
acquire Global LTFS ownership once
one readiness verification
one cartridge verification
write N chunks consecutively   (each chunk: backing -> done, per-chunk commit)
release ownership
no application LTFS access while preparing the next group
```

Per-chunk failure isolation is already correct: a chunk enters `backing` only
when its own write begins; earlier `done` chunks stay `done`; a later failure is
classified conservatively (`ambiguous_backing_chunk` if the write had started);
unstarted chunks keep their packs. Phase 5 preserves every one of these rules and
only adds a durable object *around* them.

## 4. Terminology

| Term | Meaning |
|---|---|
| **chunk** | The Phase-4 unit: an ordered set of files, one legacy-ZIP pack, its own `remote_chunks` lifecycle. Authoritative. |
| **pack identity** | Stable, deterministic pack-dir basename (`_pack_sNNNN_III`). Survives restarts; robocopy same-size-skips on it. |
| **sealed batch** | A durable, immutable record of an ordered group of chunks selected to be written under one ownership period. A scheduling + recovery object, not a file-identity object. |
| **batch fingerprint** | SHA-256 over the ordered membership (ordinal, chunk_index, pack_identity, prepared_bytes, file_count, pack_fingerprint) + expected tape + format generation. Detects any mutation. |
| **generation** | Monotonic per-batch counter for compare-and-set transitions and stale-claim rejection. |
| **writer lease** | A claim token + owner + expiry recorded when a batch is claimed for writing; complements (does not replace) the Global LTFS mutex. |
| **durable** | The selected tape durability condition has succeeded (see §13). |
| **externally_durable** | A *separate, future* gate for off-tape copies / verification. Not implemented. |
| **prune_safe** | Source data may be deleted. Requires `durable` (and, when defined, `externally_durable`). |

## 5. Lifecycle definitions

Batch states (terminal: `durable`, `failed`, `cancelled`):

```
building      accumulating a candidate group; NOT authoritative
   | seal (atomic; fingerprint written)
sealed        immutable ordered group; the recovery authority
   | claim_for_write (CAS: sealed -> writing, lease taken)
writing       chunks written consecutively; each chunk keeps its own state
   | request_sync (all chunks done)
sync_pending  copies complete; LTFS synchronization requested
   | confirm_sync + mark_durable
durable       selected tape durability condition satisfied
```

`failed` / `cancelled` may be entered from `building`, `sealed`, `writing`, or
`sync_pending`. Per-chunk states are unchanged from Phase 4:
`pending → backing → done` (or `failed`), and a chunk enters `backing` only when
its own write begins.

## 6. Invariants

**Completeness.** A set of prepared chunks in staging is *not* a batch. A batch
becomes authoritative only after an **atomic seal** records: batch id, session
id, ordered chunk ids, pack identities, exact prepared bytes, exact file count,
expected tape identity, per-pack manifest/resume hashes, aggregate fingerprint,
created/sealed timestamps, and format generation. A partial building operation
interrupted by a crash is discarded on recovery and can never be mistaken for a
sealed batch (proof: model tests 1, 6).

**Immutability.** After `sealed`: no chunk added or removed, no reordering, no
pack-content change, no silent tape change. The fingerprint detects any of these.
A required change creates a **new** batch (proof: tests 3, 5, and the
reconstruction tamper test).

**Per-chunk authority.** For a sealed group (e.g. chunks 108–119): each chunk
enters `backing` only when its own write begins; each success follows the
existing commit + staging-flush rules; an earlier `done` chunk stays `done` if a
later one fails; the failing chunk follows the existing conservative
classification; later unstarted chunks stay prepared and reusable; **no
batch-level state may turn a completed chunk ambiguous** (proof: tests 8, 9, 18).

**Tape durability.** Distinct, non-conflated conditions:

```
chunk copy completed          (robocopy for one chunk finished — NOT success by exit code alone)
all selected copies completed  (every chunk in the batch reached 'done')
LTFS synchronization requested (sync_pending)
LTFS synchronization confirmed (sync_confirmed_at set)
batch durable                  (durable; selected tape condition met)
externally durable             (separate future gate; not implemented)
prune safe                     (durable + all authoritative durability conditions)
```

`durable` is never reached before sync is confirmed (DB CHECK + model test 10),
and `prune_safe` requires `durable` (test 10, 12-adjacent).

## 7. Proposed PostgreSQL schema

Normalized: `tape_write_batches` (one row per batch) and
`tape_write_batch_chunks` (ordered membership), plus a `tape_write_active_chunk`
claims table whose primary key enforces "one chunk in at most one active batch"
without triggers. Full DDL, constraints, and indexes:
[`docs/phase5_sealed_batch_schema.sql`](phase5_sealed_batch_schema.sql).

Key constraints:
- `PRIMARY KEY (batch_id, chunk_index)` and `UNIQUE (batch_id, ordinal)` on
  membership → deterministic, gap-checked ordering.
- Fingerprint CHECK: NULL iff `building`; present for every post-seal active
  state; unconstrained for terminal `failed`/`cancelled`.
- Durable CHECK: `state <> 'durable' OR sync_confirmed_at IS NOT NULL`.
- `tape_write_active_chunk (session_id, chunk_index)` PK → the "two active
  batches" impossibility (model test 16).
- FKs to `remote_sessions`, `tapes`, `remote_chunks` keep the batch anchored to
  existing truth; `remote_chunks` stays the authoritative per-chunk row (the
  `chunk_state` column on membership is a shadow for the model).

## 8. Transaction boundaries

| Operation | Transaction | Locks / concurrency control |
|---|---|---|
| build (begin) | single INSERT (`building`) | none |
| add chunk | one txn: INSERT claim + INSERT membership + UPDATE aggregates | claims PK rejects a chunk already active; whole txn rolls back on conflict |
| **seal** | single guarded UPDATE `... WHERE batch_id=? AND state='building'` + fingerprint + `generation+1` | atomic CAS; `rowcount<>1` ⇒ abort. No double-seal (test 2) |
| claim for write | single guarded UPDATE `... WHERE state='sealed' [AND generation=?]` setting lease + `generation+1` | CAS on state (+ optional generation); complements the Global mutex |
| record chunk write start | UPDATE membership → `backing` (mirrors `remote_chunks`) | requires batch `writing` |
| commit chunk done | UPDATE membership → `done` (+ existing per-chunk `remote_chunks` commit) | per-chunk, unchanged |
| record copies complete → request sync | UPDATE `writing → sync_pending` only if all chunks `done` | guarded by a `done`-count check |
| confirm sync | UPDATE `sync_confirmed_at` | requires `sync_pending` |
| mark durable | guarded UPDATE `sync_pending → durable` requiring `sync_confirmed_at`; idempotent no-op if already `durable` | CHECK + idempotency (test 11) |
| fail / cancel | UPDATE to terminal + DELETE claims for non-`done` chunks | releases unwritten chunks for re-batching |
| recover abandoned writer | UPDATE `writing → sealed` where lease expired, `generation+1` | generation bump invalidates the dead writer's stale token (test 17) |

**Idempotency keys:** the batch `generation` (CAS) and the unique
`tape_write_active_chunk` PK. Advisory locks are *not* required for correctness —
the guarded CAS UPDATEs are sufficient — but a per-session advisory lock is a
reasonable optimization to serialize batch-building on one session.

## 9. Crash-recovery matrix

For each crash point: DB state on restart, batch state, chunk state, packs,
resumability, what to reverify, operator need.

| Crash point | DB / batch | Chunks | Packs | Resume? | Reverify | Operator? |
|---|---|---|---|---|---|---|
| building, before seal | `building` row (or none) | `pending` | retained | yes | none | no — discard building batch, re-enqueue packs |
| during the atomic seal | seal committed or not (no partial) | `pending` | retained | yes | fingerprint on reconstruct | no |
| after seal, before ownership | `sealed` | `pending` | retained | yes | reconstruct + validate pack paths + recompute fingerprint | no |
| during readiness verification | `sealed` (not yet `writing`) | `pending` | retained | yes | full readiness + cartridge on next claim | no |
| after claim, before first write | `writing`, lease live | `pending` | retained | yes, after lease recovery | readiness + cartridge; re-claim | no |
| during chunk N write | `writing` | N `backing` (ambiguous) | retained | **conservative**: N stays `backing` | resume precheck classifies N | no (matches Phase 4) |
| after chunk N copied, before its commit | `writing` | N `backing` | retained | yes | N is physically ambiguous → stays `backing` until verified | no |
| after several chunks committed | `writing` | some `done`, rest `pending` | retained | yes | `done` chunks are final; continue remaining | no |
| after all copies, before sync | `writing`/`sync_pending` | all `done` | retained | yes | request/confirm sync idempotently | no |
| during sync | `sync_pending`, `sync_confirmed_at` NULL | all `done` | retained | yes | re-request/confirm sync; not durable yet | no |
| after sync, before durable commit | `sync_pending`, `sync_confirmed_at` set | all `done` | retained | yes | `mark_durable` idempotently (test 11) | no |
| during cancellation | terminal or not (single UPDATE) | unaffected | retained | n/a | none | no |
| process death holding the Global mutex | mutex reported *abandoned* to next waiter; batch `writing`, lease expired | per above | retained | yes | `recover_abandoned_writer` → `sealed`, generation bumped; readiness re-verified from scratch | no |

No crash point permits a double write, a silent index loss, or turning a `done`
chunk ambiguous. None requires physical intervention.

## 10. Cancellation matrix

| Cancellation point | Batch result | Chunks | Packs |
|---|---|---|---|
| before sealing | `cancelled` from `building` (fingerprint NULL) | `pending`, released | retained, reusable |
| after sealing, before writing | `cancelled` from `sealed` | `pending`, released | retained, reusable (test 12) |
| between chunks | `cancelled`; earlier `done` stay `done` | unstarted released | retained |
| during a chunk | current chunk keeps existing semantics (`backing` if started); no later chunk starts | later released | retained |
| safe stop, scan active | batch may seal + drain the finite group, or cancel building; scan continues | per above | retained |
| producer failure | building batch discarded; sealed batch untouched (a failure never forces a write) | preserved | retained |
| ownership timeout | batch stays `sealed`, unclaimed; chunks pre-write, unambiguous (test 18) | `pending` | retained |
| hard LTFS failure / permanent write error / read-only transition | batch `failed`; **no automatic retry**; failing chunk classified conservatively | earlier `done` preserved | retained |

## 11. Session 37 compatibility

Current corrected baseline: Session 37 is `active`, `scan_complete=false`, and
legacy ZIP. Chunks 0–48 are `done` on Tape_02; chunks 49–112 are pending. Its
`tape_label = Tape_03` names the next target, not completed work: all 9 Session
37 `archive_runs` name Tape_02, zero `archive_runs` reference Tape_03, and
`files_index` has zero Tape_03 rows. Tape_03 separately received the 24 GiB
Phase 5E synthetic pilot and was reformatted twice. The design **explicitly
supports introducing sealed batches for future prepared chunks only**, leaving
prior completed chunks untouched:

- No conversion to TAR; `format_generation=1`.
- Chunks 0–48 are **not** rewritten, regenerated, or re-batched.
- Pending chunks retain their existing identities and may join a future batch
  only after the repository's production rollout gates are satisfied.
- Sealing a physical group does **not** claim the session scan/plan is complete
  (§12; model tests 13, 14). `scan_complete=false` never blocks sealing.
- The currently discovered chunk set is never treated as the final plan.

Sealed batches are additive: they can start being formed for *newly prepared*
chunks on the streaming path without any in-place migration of Session 37.

## 12. Session-plan lifecycle (separate from a sealed write batch)

A sealed physical write batch must not imply session completeness. If a
session-level plan lifecycle is later added (`building/sealed/active/terminal`),
it must keep these distinct:

- **scan plan completeness** (`scan_complete`) — the scanner finished discovering
  work;
- **a sealed physical write batch** — a finite group scheduled for one ownership
  period;
- **session terminality** — the session is closed;
- **final chunk / file / byte counts** and an **aggregate plan fingerprint** —
  only meaningful once the scan is complete.

Phase 5 requires none of these to seal a batch. A sealed batch is valid mid-scan.

## 13. LTFS synchronization contract (definition only)

Phase 5A defines, and does not implement, the boundary between "copied" and
"durable":

1. `chunk copy completed` — a chunk's robocopy finished **and passed the durable
   raw-log classifier** (exit code 0 is never trusted alone — incident 009).
2. `all selected copies completed` — every chunk in the batch is `done`.
3. `LTFS synchronization requested` — batch → `sync_pending`.
4. `LTFS synchronization confirmed` — the index is known synced. Under the
   current `time@5` policy this is satisfied by the periodic time-based sync; the
   contract leaves room for an explicit, gated sync later **without changing the
   policy now**.
5. `batch durable` — `durable`; `durability_kind='ltfs_time5_confirmed'`.
6. `externally_durable` — reserved for off-tape verification; **separate**.
7. `prune_safe` — requires all authoritative durability conditions.

No explicit LTFS synchronization is issued in Phase 5A.

## 14. ReadyQueue interaction

**Principle:** `ReadyQueue` is *scheduling* state; the PostgreSQL sealed batch is
*recovery* authority. The system must never assume the in-memory queue survived a
crash.

- Ready items enter a **building** batch as the group snapshot is taken (the same
  finite snapshot Phase 4 already forms under the queue lock).
- The group snapshot becomes authoritative at **seal**, before ownership is
  acquired. Items leave the queue when the batch is sealed (or preserved on stop).
- A restart **reconstructs** the sealed batch from PostgreSQL, not from the queue:
  read ordered membership, **validate each pack path exists and matches its
  pack_identity/size**, recompute the fingerprint, and compare. A mismatch fails
  closed (no write).
- An **incomplete building batch** found on restart is discarded (its packs stay
  on disk and are re-enqueued); it is never promoted to sealed.

## 15. Migration and rollout sequence

Introduced behind a disabled feature flag; no behavior change until explicitly
activated:

```
5A  design + executable model + tests            (this phase — no production change)
5B  schema + repository layer behind a DISABLED flag; apply-schema is manual,
    never in the normal startup path
5C  dual-write OBSERVATION: record building/sealed batches alongside the existing
    per-chunk flow WITHOUT changing tape scheduling; compare, do not depend
5D  sealed-batch SCHEDULING on scratch tapes only
5E  crash/recovery drills on scratch tapes
5F  production activation (explicit approval; Session 37 unaffected unless chosen)
```

## 16. Rollback strategy

- The feature flag defaults **off**; disabling it reverts to Phase 4 behavior
  with no data change.
- Batch tables are **additive** and never the source of per-chunk truth, so they
  can be dropped without affecting `remote_chunks`, `remote_sessions`, catalog
  durability, or resumability.
- Because a sealed batch is reconstructed from (and validated against) existing
  chunk/pack state, discarding all batch rows leaves the system in a valid,
  Phase-4-resumable state.

## 17. Test plan

The 18 required proofs are implemented in
[`tests/test_phase5a_sealed_batch_model.py`](../tests/test_phase5a_sealed_batch_model.py)
against the SQLite model (plus reconstruction-tamper, prune-safe, claim-CAS, and
released-chunk-rejoin extras). Later phases add: repository-layer tests against a
throwaway PostgreSQL database (never production), dual-write reconciliation tests,
and scratch-tape recovery drills.

## 18. Open questions

- Should `remote_chunks` gain a nullable `active_batch_id` back-reference, or is
  the claims table sufficient? (Model uses the claims table.)
- Exact `durability_kind` taxonomy once an explicit LTFS sync is offered.
- Whether the writer lease should be a DB row (as proposed) or derived purely
  from the Global mutex generation. Proposal keeps both: the mutex is the
  physical boundary, the lease is the *recoverable* record.
- Session-plan lifecycle: needed now, or deferred until multi-tape sessions?

## 19. Phase 5B.5 addendum — authority, migration integrity, rollback, shadow

### 19.1 Per-chunk authority decision (Option B, resolved)

`remote_chunks.status` is the **sole authority** for the per-chunk archive
result. The batch member column was renamed `chunk_state` → **`member_write_phase`**
with a deliberately distinct vocabulary so it can never be read as the archive
status: `not_started | writing | copied | failed`. It records only THIS batch's
execution attempt. `reconcile_member` / `reconcile_batch` fail closed on
impossible combinations and never downgrade a chunk that `remote_chunks` reports
`done` (the authority always wins).

### 19.2 Compatibility matrix (`member_write_phase` × `remote_chunks.status`)

| member_write_phase | remote_chunks.status | valid? | reconcile outcome / action |
|---|---|---|---|
| not_started | pending / fetching / packing | valid | `RC_OK` — reusable, may be written |
| not_started | done | valid | `RC_RELEASE` — already done elsewhere; release claim, do not rewrite |
| not_started | backing | **invalid** | fail closed (another writer started it) |
| writing | backing | valid | `RC_AMBIGUOUS` — resume precheck classifies |
| writing | done | valid | `RC_AUTHORITY_DONE` — promote member to `copied`; never revert |
| writing | pending / fetching / packing | **invalid** | fail closed (started but authority never saw backing) |
| copied | done | valid | `RC_OK` — terminal success |
| copied | pending / fetching / packing / backing | **invalid** | fail closed (member claims copied, authority not done) |
| failed | backup_failed / fetch_failed / backing | valid | `RC_OK` — conservative; no blind rewrite |
| failed | done | valid | `RC_AUTHORITY_DONE` — authority wins |
| any | (no remote_chunks row) | **invalid** | fail closed (dangling membership) |

Reconciliation is idempotent (`apply=True` promotes `RC_AUTHORITY_DONE`→`copied`
and releases `RC_RELEASE` claims; re-running yields `RC_OK`). A batch failure
never writes `remote_chunks`, so a completed chunk stays completed.

### 19.3 Migration integrity

`apply_schema` records a SHA-256 of the 012 file in `schema_migrations.checksum`
and **fails closed** if a different checksum is already recorded for the version.
It then runs **exact schema-drift validation** (`assert_schema_valid`): every
expected table, column, data type, nullability, primary key, named constraint
(state/fp/durable/member-phase CHECKs + FKs), and index. An incompatible
pre-existing table, a missing column, a wrong type, a missing constraint, or a
partial schema all raise `SchemaDriftError` — "the tables already exist" is never
by itself treated as success. Do not edit `scripts/sql/012` after its checksum
is recorded anywhere.

### 19.4 Rollback safety

`rollback_schema` **refuses** (raising `RollbackRefused`) while any batch, active
claim, or durable record exists; only `force=True` (test / explicit operator
confirmation) overrides. It never touches `remote_chunks`, `remote_sessions`,
`tapes`, or catalog data.

### 19.5 Feature gate wired into startup (disabled)

`assert_feature_ready` is called from `_assert_feature_gate` in both remote
session runners, after the ownership preflight and before any worker thread.
Flag **false** (default): no repository is constructed, no batch table queried,
no schema check runs, no behaviour changes. Flag **true**: it requires schema
applied + checksum valid + exact validation, and fails closed with
`sealed_batch_feature_unavailable` (SAFETY_BLOCK) before workers start — no
fallback to legacy behaviour after opt-in. Passing the gate creates/schedules no
batch in Phase 5B.5.

### 19.6 Production-schema shadow validation

012 was validated against a shadow restored from a schema-only `pg_dump` of the
real production catalog (`lto_archive_directory_catalog_20260710_103359`, PG
17.10): the shadow's `remote_chunks` matched production exactly, 012 added only
its four tables (removed none), a full seal→claim→reconstruct lifecycle
succeeded, and production remained untouched (row counts unchanged, no
sealed-batch tables). A full-data backup/restore was intentionally NOT run
unprompted on this constrained host — the operator commands are in the 5B.5
report.

## 20. Phase 5C addendum — observation-only shadow

`src/sealed_batch_observer.py` computes what a sealed batch WOULD be for the group
the Phase 4.5 scheduler selects and compares it to the ReadyQueue selection and
authoritative `remote_chunks.status`, with **zero** production influence.
Constructed only via `maybe_build_observer(cfg)` when
`[FEATURES] sealed_tape_write_batches_observation_enabled` is true (default
false); it opens no DB connection during computation (the caller passes a copied
snapshot + a read-only status map), holds no ReadyQueue lock, touches no LTFS,
and its (or its sink's) failure never propagates to the writer. Records are
diagnostic/append-only — never prune, restore, mark durable, or override
`remote_chunks`.

**Mismatch taxonomy** (per member vs authority): `prepared_chunk_already_done`
→ DATA_INCONSISTENCY; `backing_chunk_unresolved_ambiguity` → RECONCILIATION_REQUIRED
(the historical shadow fixture's chunk-108 case — flagged from PostgreSQL truth alone, never inferred
reusable from outside evidence); `chunk_eligible_in_queue_but_not_in_db`,
`duplicate_active_claim`, `byte_count_mismatch`, `missing_pack_metadata`,
`chunk_order_mismatch`, `expected_tape_mismatch`, `fingerprint_mismatch`.
Classifications: MATCH, EXPECTED_DIFFERENCE, OBSERVER_ERROR, SCHEDULER_ERROR,
DATA_INCONSISTENCY, RECONCILIATION_REQUIRED. The original validation used a
**historical full-data production shadow** with the then-recorded state (done
0–107, backing 108, pending 3, packing 1); it is not a current Session 37 status
report. In that fixture: 108→RECONCILIATION_REQUIRED, done→DATA_INCONSISTENCY,
`reconcile_member(not_started, done)`→release. Observer cost ~62 µs per 12-chunk
group (1000 groups ≈ 62 ms) — the basis for a future async observation budget.

## 21. Approval status

**No production migration has been approved.** Phase 5A is design + model + tests
only. Phases 5B–5F must each be approved explicitly before any schema or runtime
change.
