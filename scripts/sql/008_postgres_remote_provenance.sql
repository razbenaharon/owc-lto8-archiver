BEGIN;

-- Remote provenance for files_index.
--
-- Remote archive runs previously recorded no session/chunk identity, so
-- files_index.record_key fell back to the -1/-1 local placeholders. Two
-- distinct remote archives of the same source_host + original_path +
-- tape_label therefore collided on one key, and re-archiving changed remote
-- content could be skipped when update_existing=False.
--
-- These columns let _file_record_key() fold remote session/chunk identity into
-- the key for remote writes. The key stays byte-identical for local rows and
-- for every row written before this migration, so no existing row is re-keyed.
ALTER TABLE files_index
    ADD COLUMN IF NOT EXISTS remote_session_id BIGINT;

ALTER TABLE files_index
    ADD COLUMN IF NOT EXISTS remote_chunk_index INTEGER;

CREATE INDEX IF NOT EXISTS idx_files_remote_chunk
    ON files_index(remote_session_id, remote_chunk_index, tape_label)
    WHERE remote_session_id IS NOT NULL;

-- Bundle/tape consistency: a packed file row must reference a bundle on the
-- SAME tape. bundle_id is already the PK of archive_bundles, so the composite
-- UNIQUE is trivially satisfiable and cannot fail on existing data.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'uq_archive_bundles_id_tape'
          AND conrelid = 'archive_bundles'::regclass
    ) THEN
        ALTER TABLE archive_bundles
            ADD CONSTRAINT uq_archive_bundles_id_tape UNIQUE (bundle_id, tape_label);
    END IF;
END $$;

-- Added NOT VALID so a legacy row with a cross-tape bundle reference cannot
-- brick schema init; new/updated rows are enforced immediately. Operators can
-- VALIDATE CONSTRAINT fk_files_bundle_tape after auditing existing data.
-- MATCH SIMPLE (the default) skips the check when bundle_id IS NULL, so loose
-- (unpacked) file rows are unaffected.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'fk_files_bundle_tape'
          AND conrelid = 'files_index'::regclass
    ) THEN
        ALTER TABLE files_index
            ADD CONSTRAINT fk_files_bundle_tape
            FOREIGN KEY (bundle_id, tape_label)
            REFERENCES archive_bundles (bundle_id, tape_label)
            ON UPDATE CASCADE
            NOT VALID;
    END IF;
END $$;

COMMIT;
