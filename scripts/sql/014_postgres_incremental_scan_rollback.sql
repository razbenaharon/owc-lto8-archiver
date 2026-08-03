-- 014_postgres_incremental_scan_rollback.sql  —  REVIEWED rollback only
--
-- Plan 1, Task 2.1.  This is NOT a general undo.  The supported rollback for
-- the incremental scan is to DISABLE the feature ([REMOTE] incremental_scan =
-- false) and leave every table, column and artifact in place: they are
-- additive, they cost nothing while unused, and they are the only record of
-- what a frontier-publishing session already covered.
--
-- What this file may safely drop, after review:
--   * the FINALIZED unique index, if it is not being relied on;
--   * the frontier tables, ONLY while they are still empty.
--
-- What it deliberately does NOT do:
--   * drop a populated frontier table (that is coverage evidence);
--   * drop the remote_chunks / remote_file_state columns (a chunk may carry a
--     claim or an expectation another process is mid-way through validating);
--   * touch remote_plan_files rows in any way;
--   * reverse a resequence — there is none to reverse, because the finalize
--     step refuses rather than resequences.  Restore from the verified backup.

-- ===========================================================================
-- 1. Refuse to drop anything that holds data
-- ===========================================================================
DO $$
DECLARE
    populated TEXT;
BEGIN
    SELECT string_agg(name, ', ') INTO populated FROM (
        SELECT 'remote_scan_scopes' AS name
        WHERE EXISTS (SELECT 1 FROM remote_scan_scopes)
        UNION ALL SELECT 'remote_scan_directories'
        WHERE EXISTS (SELECT 1 FROM remote_scan_directories)
        UNION ALL SELECT 'remote_scan_segments'
        WHERE EXISTS (SELECT 1 FROM remote_scan_segments)
        UNION ALL SELECT 'remote_chunk_scan_segments'
        WHERE EXISTS (SELECT 1 FROM remote_chunk_scan_segments)
        UNION ALL SELECT 'remote_scan_errors'
        WHERE EXISTS (SELECT 1 FROM remote_scan_errors)
        UNION ALL SELECT 'remote_worker_attempts'
        WHERE EXISTS (SELECT 1 FROM remote_worker_attempts)
        UNION ALL SELECT 'remote_frontier_bootstraps'
        WHERE EXISTS (SELECT 1 FROM remote_frontier_bootstraps)
    ) t;

    IF populated IS NOT NULL THEN
        RAISE EXCEPTION
            'REFUSING to roll back migration 014: these tables hold data (%). '
            'They are the record of what a frontier-publishing session already '
            'covered, and dropping them would make that coverage unknowable. '
            'Disable [REMOTE] incremental_scan instead and reconcile the '
            'session explicitly (Plan 1 Task 4.2).', populated;
    END IF;
END $$;

-- ===========================================================================
-- 2. Drop the empty frontier tables, children first
-- ===========================================================================
DROP TABLE IF EXISTS remote_frontier_bootstraps;
DROP TABLE IF EXISTS remote_chunk_scan_segments;
DROP TABLE IF EXISTS remote_scan_errors;
DROP TABLE IF EXISTS remote_worker_attempts;
DROP TABLE IF EXISTS remote_scan_segments;
DROP TABLE IF EXISTS remote_scan_directories;
DROP TABLE IF EXISTS remote_scan_scopes;

-- ===========================================================================
-- 3. The finalized index
-- ===========================================================================
-- Dropped only as part of a reviewed rollback: without it, duplicate chunk
-- ordinals become application-enforced again rather than structurally
-- impossible.
DROP INDEX IF EXISTS uq_remote_plan_files_chunk_ordinal;

ALTER TABLE remote_chunks
    DROP CONSTRAINT IF EXISTS remote_chunks_sealed_expectations_check;

-- The remote_chunks and remote_file_state COLUMNS are intentionally kept.
-- They are nullable, unused when the feature is off, and a populated
-- owner_token or expectation may be exactly what a reconciliation needs to
-- read next.
