-- =============================================================================
-- 016 - Stored TAR sealed container plan assignments.
-- =============================================================================
--
-- Additive Task 2.1 schema.  It records the resolved per-chunk byte cap and the
-- stable plan ordinal -> storage/container assignment before any TAR worker can
-- start.  It does not create TAR data.
-- =============================================================================

DO $$
BEGIN
    IF to_regclass('public.archive_containers') IS NULL
       OR to_regclass('public.remote_chunks') IS NULL
       OR to_regclass('public.remote_plan_files') IS NULL THEN
        RAISE EXCEPTION 'migration 016 requires migration 015';
    END IF;
END $$;

ALTER TABLE remote_chunks
    ADD COLUMN IF NOT EXISTS stored_tar_max_size_bytes BIGINT CHECK (
        stored_tar_max_size_bytes IS NULL OR stored_tar_max_size_bytes > 0);

ALTER TABLE archive_containers
    ADD COLUMN IF NOT EXISTS estimated_archive_bytes BIGINT CHECK (
        estimated_archive_bytes IS NULL OR estimated_archive_bytes >= 0);

CREATE TABLE IF NOT EXISTS archive_container_members (
    session_id             BIGINT NOT NULL,
    chunk_index            INTEGER NOT NULL CHECK (chunk_index >= 0),
    plan_file_id           BIGINT NOT NULL,
    plan_ordinal           BIGINT NOT NULL CHECK (plan_ordinal >= 0),
    storage_class          TEXT NOT NULL CHECK (
                              storage_class IN (
                                  'small_files','loose_files',
                                  'source_missing')),
    container_id           BIGINT,
    container_ordinal      INTEGER CHECK (container_ordinal >= 0),
    remote_path            TEXT NOT NULL CHECK (length(remote_path) > 0),
    expected_logical_bytes BIGINT NOT NULL CHECK (expected_logical_bytes >= 0),
    estimated_tar_bytes    BIGINT NOT NULL DEFAULT 0 CHECK (
                              estimated_tar_bytes >= 0),
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (session_id, chunk_index, plan_file_id),
    UNIQUE (session_id, chunk_index, plan_ordinal),
    FOREIGN KEY (session_id, chunk_index)
        REFERENCES remote_chunks(session_id, chunk_index) ON DELETE RESTRICT,
    FOREIGN KEY (plan_file_id)
        REFERENCES remote_plan_files(plan_file_id) ON DELETE RESTRICT,
    FOREIGN KEY (container_id, session_id, chunk_index)
        REFERENCES archive_containers(container_id, session_id, chunk_index)
        ON DELETE RESTRICT,
    CONSTRAINT archive_container_members_container_ck CHECK (
        (storage_class='small_files'
         AND container_id IS NOT NULL
         AND container_ordinal IS NOT NULL
         AND estimated_tar_bytes > 0)
        OR
        (storage_class='loose_files'
         AND container_id IS NULL
         AND container_ordinal IS NULL
         AND estimated_tar_bytes = 0)
        OR
        (storage_class='source_missing'
         AND container_id IS NULL
         AND container_ordinal IS NULL
         AND estimated_tar_bytes = 0))
);

CREATE INDEX IF NOT EXISTS idx_archive_container_members_container
    ON archive_container_members(container_id)
    WHERE container_id IS NOT NULL;
