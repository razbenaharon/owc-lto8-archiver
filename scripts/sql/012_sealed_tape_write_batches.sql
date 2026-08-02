-- =============================================================================
-- 012 — Sealed tape-write batches (Phase 5B / 5B.5).
-- =============================================================================
-- FEATURE-FLAG GATED / EXPLICIT-ONLY. Deliberately NOT in the startup schema
-- list in src/pg_core.py::_init_schema. Applied only explicitly by
-- SealedBatchRepository.apply_schema against a chosen, verified database — like
-- 007_postgres_directory_catalog.sql — because the feature ships disabled behind
-- [FEATURES] sealed_tape_write_batches_enabled (default false).
--
-- IMMUTABLE MIGRATION IDENTITY (Phase 5B.5): the repository records a SHA-256 of
-- THIS file in schema_migrations.checksum on first apply and fails closed if a
-- later apply sees a different checksum for the same version. Do NOT edit this
-- file after it has been applied anywhere its checksum is recorded. The DDL is
-- idempotent (CREATE ... IF NOT EXISTS) but "the tables already exist" is NOT
-- treated as success: apply_schema also runs exact schema-drift validation.
--
-- Authority: docs/PHASE5_SEALED_BATCH_DESIGN.md.
-- =============================================================================
BEGIN;

CREATE TABLE IF NOT EXISTS schema_migrations (
    version    TEXT PRIMARY KEY,
    checksum   TEXT,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- Smallest additive mechanism for a pre-existing tracking table (isolated /
-- shadow environments only): give it a checksum column if it lacks one.
ALTER TABLE schema_migrations ADD COLUMN IF NOT EXISTS checksum TEXT;

CREATE TABLE IF NOT EXISTS tape_write_batches (
    batch_id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    session_id          BIGINT  NOT NULL
                            REFERENCES remote_sessions(session_id) ON DELETE CASCADE,
    state               TEXT    NOT NULL DEFAULT 'building',
    generation          BIGINT  NOT NULL DEFAULT 0 CHECK (generation >= 0),
    expected_tape_label TEXT    NOT NULL
                            REFERENCES tapes(volume_label) ON UPDATE CASCADE,
    format_generation   INTEGER NOT NULL,

    chunk_count         INTEGER NOT NULL DEFAULT 0 CHECK (chunk_count >= 0),
    prepared_bytes      BIGINT  NOT NULL DEFAULT 0 CHECK (prepared_bytes >= 0),
    file_count          BIGINT  NOT NULL DEFAULT 0 CHECK (file_count >= 0),

    batch_fingerprint   TEXT,

    writer_lease_id         TEXT,
    writer_lease_owner      TEXT,
    writer_lease_expires_at TIMESTAMPTZ,

    durability_kind     TEXT,

    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    sealed_at           TIMESTAMPTZ,
    write_started_at    TIMESTAMPTZ,
    copies_completed_at TIMESTAMPTZ,
    sync_requested_at   TIMESTAMPTZ,
    sync_confirmed_at   TIMESTAMPTZ,
    durable_at          TIMESTAMPTZ,
    failed_at           TIMESTAMPTZ,
    cancelled_at        TIMESTAMPTZ,

    CONSTRAINT tape_write_batches_state_ck CHECK (state IN
        ('building','sealed','writing','sync_pending','durable',
         'failed','cancelled')),

    -- Fingerprint invariant: NULL while building; present for every post-seal
    -- active state; unconstrained for terminal failed/cancelled.
    CONSTRAINT tape_write_batches_fp_ck CHECK (
        (state = 'building' AND batch_fingerprint IS NULL)
        OR (state IN ('sealed','writing','sync_pending','durable')
            AND batch_fingerprint IS NOT NULL)
        OR (state IN ('failed','cancelled'))
    ),

    CONSTRAINT tape_write_batches_durable_ck CHECK (
        state <> 'durable' OR sync_confirmed_at IS NOT NULL
    )
);

CREATE INDEX IF NOT EXISTS ix_twb_session_state
    ON tape_write_batches (session_id, state);
CREATE INDEX IF NOT EXISTS ix_twb_active_lease
    ON tape_write_batches (writer_lease_expires_at) WHERE state = 'writing';


CREATE TABLE IF NOT EXISTS tape_write_batch_chunks (
    batch_id        BIGINT  NOT NULL
                        REFERENCES tape_write_batches(batch_id) ON DELETE CASCADE,
    session_id      BIGINT  NOT NULL,
    chunk_index     INTEGER NOT NULL,
    ordinal         INTEGER NOT NULL CHECK (ordinal >= 0),
    pack_identity   TEXT    NOT NULL,
    prepared_bytes  BIGINT  NOT NULL CHECK (prepared_bytes >= 0),
    file_count      BIGINT  NOT NULL CHECK (file_count >= 0),
    pack_fingerprint TEXT,

    -- BATCH-EXECUTION phase for THIS batch's attempt to write the chunk. It is
    -- NOT the archive result: remote_chunks.status is the sole authority for
    -- whether the chunk is durably archived (Phase 5B.5 authority decision,
    -- Option B). Deliberately distinct vocabulary so it can never be mistaken
    -- for remote_chunks.status. Reconciliation (SealedBatchRepository.reconcile_*)
    -- fails closed on impossible combinations and never downgrades a chunk that
    -- remote_chunks says is done.
    member_write_phase TEXT NOT NULL DEFAULT 'not_started',

    PRIMARY KEY (batch_id, chunk_index),
    UNIQUE (batch_id, ordinal),
    CONSTRAINT twbc_member_phase_ck CHECK (member_write_phase IN
        ('not_started','writing','copied','failed')),
    CONSTRAINT twbc_chunk_fk FOREIGN KEY (session_id, chunk_index)
        REFERENCES remote_chunks(session_id, chunk_index)
);


CREATE TABLE IF NOT EXISTS tape_write_active_chunk (
    session_id  BIGINT  NOT NULL,
    chunk_index INTEGER NOT NULL,
    batch_id    BIGINT  NOT NULL REFERENCES tape_write_batches(batch_id)
                    ON DELETE CASCADE,
    PRIMARY KEY (session_id, chunk_index),
    CONSTRAINT twac_chunk_fk FOREIGN KEY (session_id, chunk_index)
        REFERENCES remote_chunks(session_id, chunk_index)
);

-- NOTE: the (version, checksum) row is recorded by
-- SealedBatchRepository.apply_schema (Python), which owns checksum computation
-- and the fail-closed drift guard. This file intentionally does NOT insert it.

COMMIT;
