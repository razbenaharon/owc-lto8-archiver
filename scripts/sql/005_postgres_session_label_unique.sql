-- Make session creation idempotent under ambiguous-commit retries (review §1.4).
--
-- The connection-loss retry loop in PgDatabaseManager._transaction can re-run a
-- session INSERT whose first COMMIT actually landed (the client cannot tell).
-- Session labels embed a second-resolution timestamp, so they uniquely identify
-- one create attempt; a UNIQUE constraint lets the create become an upsert that
-- converges instead of duplicating the session and its manifest.
--
-- Idempotent: any historical duplicate labels (possible only via that same
-- retry bug) are renamed with a "_dupN" suffix before the constraint attaches.
BEGIN;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'uq_local_sessions_label'
          AND conrelid = 'local_sessions'::regclass
    ) THEN
        UPDATE local_sessions s
           SET session_label = s.session_label || '_dup' || s.session_id
         WHERE EXISTS (
                   SELECT 1 FROM local_sessions o
                   WHERE o.session_label = s.session_label
                     AND o.session_id < s.session_id
               );
        ALTER TABLE local_sessions
            ADD CONSTRAINT uq_local_sessions_label UNIQUE (session_label);
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'uq_remote_sessions_label'
          AND conrelid = 'remote_sessions'::regclass
    ) THEN
        UPDATE remote_sessions s
           SET session_label = s.session_label || '_dup' || s.session_id
         WHERE EXISTS (
                   SELECT 1 FROM remote_sessions o
                   WHERE o.session_label = s.session_label
                     AND o.session_id < s.session_id
               );
        ALTER TABLE remote_sessions
            ADD CONSTRAINT uq_remote_sessions_label UNIQUE (session_label);
    END IF;
END $$;

COMMIT;
