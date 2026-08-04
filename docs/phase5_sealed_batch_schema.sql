-- =============================================================================
-- Phase 5 sealed-batch schema — PROPOSAL ONLY. DO NOT EXECUTE.
-- =============================================================================
-- This file is documentation, not a migration. It is deliberately NOT wired
-- into any startup or apply-schema path. No Phase 5 production migration has
-- been approved. The executable proof of these constraints lives in the
-- SQLite reference model at tests/sealed_batch_model.py and its test suite
-- tests/test_phase5a_sealed_batch_model.py.
--
-- The sealed batch is a physical SCHEDULING and RECOVERY object. It never
-- replaces remote_chunks as the authoritative unit of file identity, write
-- result, or catalog durability. Every per-chunk rule from Phase 4 still holds.
-- =============================================================================

-- Enum-like domains kept as TEXT + CHECK for portability and easy evolution.

CREATE TABLE tape_write_batches (
    batch_id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    session_id          BIGINT NOT NULL
                            REFERENCES remote_sessions(session_id),
    state               TEXT   NOT NULL DEFAULT 'building',
    generation          BIGINT NOT NULL DEFAULT 0,   -- optimistic-lock / CAS
    expected_tape_label TEXT   NOT NULL
                            REFERENCES tapes(tape_label),
    format_generation   INTEGER NOT NULL,            -- pack/schema format id (1 = legacy ZIP)

    -- Aggregates recorded at seal (advisory while building).
    chunk_count         INTEGER NOT NULL DEFAULT 0,
    prepared_bytes      BIGINT  NOT NULL DEFAULT 0,
    file_count          BIGINT  NOT NULL DEFAULT 0,

    -- Integrity: aggregate fingerprint over ordered membership + tape identity.
    batch_fingerprint   TEXT,                        -- NULL while building

    -- Writer lease (cross-process claim; complements the Global LTFS mutex).
    writer_lease_id         TEXT,
    writer_lease_owner      TEXT,                     -- host/pid
    writer_lease_expires_at TIMESTAMPTZ,

    durability_kind     TEXT,                         -- e.g. 'ltfs_time5_confirmed'

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

    -- Fingerprint invariant: NULL iff building; present for every post-seal
    -- active state; unconstrained for terminal failed/cancelled (a batch
    -- cancelled from 'building' was never sealed).
    CONSTRAINT tape_write_batches_fp_ck CHECK (
        (state = 'building' AND batch_fingerprint IS NULL)
        OR (state IN ('sealed','writing','sync_pending','durable')
            AND batch_fingerprint IS NOT NULL)
        OR (state IN ('failed','cancelled'))
    ),

    -- Durable requires a confirmed sync (the durability contract, below).
    CONSTRAINT tape_write_batches_durable_ck CHECK (
        state <> 'durable' OR sync_confirmed_at IS NOT NULL
    )
);

CREATE INDEX ix_twb_session_state ON tape_write_batches (session_id, state);
CREATE INDEX ix_twb_active_lease  ON tape_write_batches (writer_lease_expires_at)
    WHERE state = 'writing';


CREATE TABLE tape_write_batch_chunks (
    batch_id        BIGINT  NOT NULL
                        REFERENCES tape_write_batches(batch_id) ON DELETE CASCADE,
    session_id      BIGINT  NOT NULL,                -- denormalized for the claim rule
    chunk_index     BIGINT  NOT NULL,               -- remote_chunks chunk index
    ordinal         INTEGER NOT NULL,               -- 0..N-1 write order in the batch
    pack_identity   TEXT    NOT NULL,               -- stable pack dir basename
    prepared_bytes  BIGINT  NOT NULL,
    file_count      BIGINT  NOT NULL,
    pack_fingerprint TEXT,                           -- manifest / resume-marker hash

    -- Per-chunk state mirrors remote_chunks and stays authoritative; the batch
    -- never overrides it. (Kept here for the model; production reads the
    -- authoritative value from remote_chunks and this column is a shadow.)
    chunk_state     TEXT    NOT NULL DEFAULT 'pending',

    PRIMARY KEY (batch_id, chunk_index),
    UNIQUE (batch_id, ordinal),
    CONSTRAINT twbc_chunk_state_ck CHECK (chunk_state IN
        ('pending','backing','done','failed')),
    FOREIGN KEY (session_id, chunk_index)
        REFERENCES remote_chunks(session_id, chunk_index)
);


-- One chunk may belong to at most ONE active batch. Expressed as a plain
-- primary key on a claims table rather than a cross-table trigger: a claim row
-- exists while a chunk is held by an active batch and is removed when the batch
-- releases it unwritten (fail/cancel). A 'done' chunk keeps its association in
-- tape_write_batch_chunks (historical, on tape) and is never re-batched.
CREATE TABLE tape_write_active_chunk (
    session_id  BIGINT NOT NULL,
    chunk_index BIGINT NOT NULL,
    batch_id    BIGINT NOT NULL REFERENCES tape_write_batches(batch_id),
    PRIMARY KEY (session_id, chunk_index)
);

-- Alternative to the claims table (documented, not chosen): a partial unique
-- index would need the batch state denormalized into the child row and kept in
-- sync by triggers:
--   ALTER TABLE tape_write_batch_chunks ADD COLUMN is_active BOOLEAN ...;
--   CREATE UNIQUE INDEX uq_active_chunk
--       ON tape_write_batch_chunks (session_id, chunk_index) WHERE is_active;
-- The claims table was preferred because the invariant becomes a single PK the
-- database enforces without trigger drift.
