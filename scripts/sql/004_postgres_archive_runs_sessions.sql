-- Resolve the polymorphic archive_runs.session_id (review §2.3).
--
-- The old design stored a bare `session_id BIGINT` with no foreign key and a
-- `session_kind` discriminator, so a run could reference a nonexistent — or the
-- wrong kind of — session with nothing to stop it. This migration replaces it
-- with two typed, foreign-keyed columns and a CHECK that keeps them consistent
-- with `session_kind`. It is idempotent: safe to run on a fresh database (where
-- 001 already created the new columns) and on an existing one (where it adds
-- the columns, backfills from the legacy `session_id`, and drops it).
BEGIN;

ALTER TABLE archive_runs
    ADD COLUMN IF NOT EXISTS local_session_id  BIGINT,
    ADD COLUMN IF NOT EXISTS remote_session_id BIGINT;

DO $$
BEGIN
    -- Referential integrity: each column points at its own session table.
    -- ON DELETE SET NULL keeps a run row (and its files) even if the session
    -- provenance is later deleted; the run simply becomes 'legacy'-like.
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'archive_runs_local_session_id_fkey'
          AND conrelid = 'archive_runs'::regclass
    ) THEN
        ALTER TABLE archive_runs
            ADD CONSTRAINT archive_runs_local_session_id_fkey
            FOREIGN KEY (local_session_id) REFERENCES local_sessions(session_id)
            ON DELETE SET NULL;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'archive_runs_remote_session_id_fkey'
          AND conrelid = 'archive_runs'::regclass
    ) THEN
        ALTER TABLE archive_runs
            ADD CONSTRAINT archive_runs_remote_session_id_fkey
            FOREIGN KEY (remote_session_id) REFERENCES remote_sessions(session_id)
            ON DELETE SET NULL;
    END IF;

    -- Backfill from the legacy polymorphic column, then drop it. The EXISTS
    -- guards demote any dangling reference (the exact bug this fixes) to NULL
    -- rather than violating the new foreign keys.
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'archive_runs' AND column_name = 'session_id'
    ) THEN
        UPDATE archive_runs r
           SET local_session_id = r.session_id
         WHERE r.session_kind = 'local'
           AND r.session_id IS NOT NULL
           AND r.local_session_id IS NULL
           AND EXISTS (SELECT 1 FROM local_sessions ls
                       WHERE ls.session_id = r.session_id);

        UPDATE archive_runs r
           SET remote_session_id = r.session_id
         WHERE r.session_kind = 'remote'
           AND r.session_id IS NOT NULL
           AND r.remote_session_id IS NULL
           AND EXISTS (SELECT 1 FROM remote_sessions rs
                       WHERE rs.session_id = r.session_id);

        ALTER TABLE archive_runs DROP COLUMN session_id;
    END IF;

    -- Kind consistency: at most one session reference is set, and it matches
    -- session_kind. A 'remote' run may legitimately carry no session id (the
    -- catalog layer does not thread one through), so references are optional —
    -- the FKs above guarantee that any reference present is valid.
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'archive_runs_session_kind_ck'
          AND conrelid = 'archive_runs'::regclass
    ) THEN
        ALTER TABLE archive_runs
            ADD CONSTRAINT archive_runs_session_kind_ck CHECK (
                NOT (local_session_id IS NOT NULL AND remote_session_id IS NOT NULL)
                AND (session_kind <> 'legacy'
                     OR (local_session_id IS NULL AND remote_session_id IS NULL))
                AND (session_kind <> 'local'  OR remote_session_id IS NULL)
                AND (session_kind <> 'remote' OR local_session_id IS NULL)
            );
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_archive_runs_local_session
    ON archive_runs(local_session_id) WHERE local_session_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_archive_runs_remote_session
    ON archive_runs(remote_session_id) WHERE remote_session_id IS NOT NULL;

COMMIT;
