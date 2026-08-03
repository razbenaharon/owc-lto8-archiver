-- 014_postgres_incremental_scan_finalize.sql  —  FINALIZE (can FAIL on real data)
--
-- Plan 1, Task 2.1.  The second, separately-applied half of migration 014.
--
-- The base file is additive and always safe.  This file is NOT: it audits
-- existing legacy plan membership and then creates the unique constraint that
-- makes duplicate chunk ordinals impossible.  On a database whose
-- remote_plan_files already contains duplicate or mismatched ordinals it
-- FAILS and REPORTS — it never auto-resequences them.
--
-- Why refusing is the right behaviour: an ordinal identifies a file's position
-- within a chunk that may ALREADY BE ON TAPE.  Renumbering to make a
-- constraint pass would silently change what the catalog claims a written
-- chunk contained, and a future restore would be pointed at the wrong bytes.
-- Restoring from the verified backup and reconciling by hand is recoverable;
-- a guessed reverse-resequence is not.
--
-- Apply order: 014_postgres_incremental_scan.sql FIRST, then this file, and
-- only after a verified PostgreSQL backup on the intended database with no
-- archiver process holding the advisory lock.

-- ===========================================================================
-- 1. Preconditions
-- ===========================================================================
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema='public' AND table_name='remote_scan_scopes'
    ) THEN
        RAISE EXCEPTION
            'migration 014 BASE is not installed; apply '
            '014_postgres_incremental_scan.sql first';
    END IF;
END $$;

-- ===========================================================================
-- 2. Audit: duplicate or mismatched plan ordinals
-- ===========================================================================
-- Reported as a temp table so the operator can read the offending rows out of
-- the same session that ran the audit, then FAIL loudly.
DROP TABLE IF EXISTS remote_plan_ordinal_audit;
CREATE TEMP TABLE remote_plan_ordinal_audit AS
SELECT plan_id, chunk_index, ordinal, COUNT(*) AS duplicate_rows
FROM remote_plan_files
GROUP BY plan_id, chunk_index, ordinal
HAVING COUNT(*) > 1;

DO $$
DECLARE
    offending BIGINT;
    sample    TEXT;
BEGIN
    SELECT COUNT(*) INTO offending FROM remote_plan_ordinal_audit;
    IF offending > 0 THEN
        SELECT string_agg(
                   format('plan %s chunk %s ordinal %s (x%s)',
                          plan_id, chunk_index, ordinal, duplicate_rows),
                   '; ')
        INTO sample
        FROM (SELECT * FROM remote_plan_ordinal_audit LIMIT 10) s;

        RAISE EXCEPTION
            'REFUSING to finalize migration 014: % duplicate (plan_id, '
            'chunk_index, ordinal) group(s) already exist in '
            'remote_plan_files. These rows are AMBIGUOUS legacy membership '
            'and are NOT auto-resequenced, because an ordinal positions a file '
            'inside a chunk that may already be on tape. Review them, decide '
            'per chunk, and re-run. First offenders: %',
            offending, sample;
    END IF;
END $$;

-- ===========================================================================
-- 3. The final constraint
-- ===========================================================================
-- With the audit passed, duplicate membership becomes structurally impossible
-- rather than application-enforced.
CREATE UNIQUE INDEX IF NOT EXISTS uq_remote_plan_files_chunk_ordinal
    ON remote_plan_files (plan_id, chunk_index, ordinal);

-- New chunks must declare their membership state.  Legacy rows keep NULL, so
-- this is expressed as "a chunk that has any of the new columns set must have
-- a legal membership_state", not as a retroactive NOT NULL.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'remote_chunks_sealed_expectations_check'
          AND conrelid = 'remote_chunks'::regclass
    ) THEN
        ALTER TABLE remote_chunks
            ADD CONSTRAINT remote_chunks_sealed_expectations_check
            CHECK (membership_state IS DISTINCT FROM 'sealed'
                   OR (expected_file_count IS NOT NULL
                       AND expected_bytes IS NOT NULL))
            NOT VALID;
    END IF;
END $$;

ALTER TABLE remote_chunks
    VALIDATE CONSTRAINT remote_chunks_sealed_expectations_check;

-- A ready segment must carry the consumption cursor it will be advanced
-- through; a NULL there would make "which ordinals are still unconsumed"
-- unanswerable at exactly the moment a chunk asks.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'remote_scan_segments_ready_cursor_check'
          AND conrelid = 'remote_scan_segments'::regclass
    ) THEN
        ALTER TABLE remote_scan_segments
            ADD CONSTRAINT remote_scan_segments_ready_cursor_check
            CHECK (state IN ('writing','invalidated')
                   OR next_unconsumed_ordinal IS NOT NULL)
            NOT VALID;
    END IF;
END $$;

ALTER TABLE remote_scan_segments
    VALIDATE CONSTRAINT remote_scan_segments_ready_cursor_check;

-- ===========================================================================
-- 4. Marker
-- ===========================================================================
-- PgConnectionCore.incremental_scan_schema_finalized() checks for this comment
-- plus the unique index above.  Both halves must be present before
-- [REMOTE] incremental_scan can activate.
COMMENT ON INDEX uq_remote_plan_files_chunk_ordinal IS
    'Plan 1 migration 014 (finalize): duplicate chunk membership is '
    'structurally impossible. Audited before creation; never auto-resequenced.';
