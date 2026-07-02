BEGIN;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'local_chunks_manifest_tape_label_fkey'
          AND conrelid = 'local_chunks_manifest'::regclass
    ) THEN
        ALTER TABLE local_chunks_manifest
            ADD CONSTRAINT local_chunks_manifest_tape_label_fkey
            FOREIGN KEY (tape_label) REFERENCES tapes(volume_label)
            ON UPDATE CASCADE ON DELETE SET NULL
            NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'remote_sessions_tape_label_fkey'
          AND conrelid = 'remote_sessions'::regclass
    ) THEN
        ALTER TABLE remote_sessions
            ADD CONSTRAINT remote_sessions_tape_label_fkey
            FOREIGN KEY (tape_label) REFERENCES tapes(volume_label)
            ON UPDATE CASCADE ON DELETE CASCADE
            NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'remote_chunks_status_check'
          AND conrelid = 'remote_chunks'::regclass
    ) THEN
        ALTER TABLE remote_chunks
            ADD CONSTRAINT remote_chunks_status_check
            CHECK (status IN ('pending','fetching','packing','backing','done',
                              'fetch_failed','backup_failed'))
            NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'remote_file_state_status_check'
          AND conrelid = 'remote_file_state'::regclass
    ) THEN
        ALTER TABLE remote_file_state
            ADD CONSTRAINT remote_file_state_status_check
            CHECK (status IS NULL OR status IN
                   ('pending','fetching','fetched','fetch_failed','source_missing'))
            NOT VALID;
    END IF;
END $$;

ALTER TABLE local_chunks_manifest
    VALIDATE CONSTRAINT local_chunks_manifest_tape_label_fkey;
ALTER TABLE remote_sessions
    VALIDATE CONSTRAINT remote_sessions_tape_label_fkey;
ALTER TABLE remote_chunks
    VALIDATE CONSTRAINT remote_chunks_status_check;
ALTER TABLE remote_file_state
    VALIDATE CONSTRAINT remote_file_state_status_check;

COMMIT;
