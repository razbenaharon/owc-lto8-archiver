BEGIN;

-- Referential integrity for the remote provenance columns added by 008.
--
-- files_index.local_session_id has had a foreign key since 001, but 008
-- added remote_session_id/remote_chunk_index bare: deleting a remote session
-- left dangling provenance with nothing to demote it to NULL. ON DELETE SET
-- NULL mirrors the archive_runs session FKs — the file row (and its
-- record_key identity, which is immutable by design) survives; only the live
-- session reference is cleared.
--
-- NOT VALID so a legacy dangling reference cannot brick schema init (same
-- pattern as fk_files_bundle_tape in 008); new/updated rows are enforced
-- immediately. Operators can VALIDATE CONSTRAINT fk_files_remote_session
-- after auditing existing data.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'fk_files_remote_session'
          AND conrelid = 'files_index'::regclass
    ) THEN
        ALTER TABLE files_index
            ADD CONSTRAINT fk_files_remote_session
            FOREIGN KEY (remote_session_id)
            REFERENCES remote_sessions (session_id)
            ON DELETE SET NULL
            NOT VALID;
    END IF;
END $$;

COMMIT;
