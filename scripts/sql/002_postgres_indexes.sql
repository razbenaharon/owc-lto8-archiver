BEGIN;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'uq_files_record_key'
          AND conrelid = 'files_index'::regclass
    ) THEN
        ALTER TABLE files_index
            ADD CONSTRAINT uq_files_record_key UNIQUE (record_key);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_catalog_dirs_parent
    ON catalog_directories(tape_label, parent_id, name, directory_id);

CREATE INDEX IF NOT EXISTS idx_catalog_dirs_parent_id
    ON catalog_directories(parent_id, name, directory_id);

CREATE INDEX IF NOT EXISTS idx_local_manifest_session_chunk
    ON local_chunks_manifest(session_id, chunk_index);

CREATE INDEX IF NOT EXISTS idx_local_manifest_tape_label
    ON local_chunks_manifest(tape_label)
    WHERE tape_label IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_archive_runs_tape_label
    ON archive_runs(tape_label);

CREATE INDEX IF NOT EXISTS idx_remote_sessions_tape_label
    ON remote_sessions(tape_label);

CREATE INDEX IF NOT EXISTS idx_remote_sessions_plan_id
    ON remote_sessions(plan_id)
    WHERE plan_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_remote_plans_snapshot_id
    ON remote_plans(snapshot_id);

CREATE INDEX IF NOT EXISTS idx_remote_plan_chunk
    ON remote_plan_files(plan_id, chunk_index, ordinal);

CREATE INDEX IF NOT EXISTS idx_remote_plan_files_snapshot_file_id
    ON remote_plan_files(snapshot_file_id);

CREATE INDEX IF NOT EXISTS idx_remote_file_state_plan_file_id
    ON remote_file_state(plan_file_id);

CREATE INDEX IF NOT EXISTS idx_files_dir_name
    ON files_index(directory_id, catalog_name, file_id);

CREATE INDEX IF NOT EXISTS idx_files_dir_size
    ON files_index(directory_id, file_size_bytes, catalog_name, file_id);

CREATE INDEX IF NOT EXISTS idx_files_dir_date
    ON files_index(directory_id, catalog_backup_date, catalog_name, file_id);

CREATE INDEX IF NOT EXISTS idx_files_tape
    ON files_index(tape_label) INCLUDE (file_size_bytes);

CREATE INDEX IF NOT EXISTS idx_files_src_host
    ON files_index(source_host, tape_label, original_path);

CREATE INDEX IF NOT EXISTS idx_files_bundle
    ON files_index(bundle_id) WHERE bundle_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_files_run
    ON files_index(archive_run_id);

CREATE INDEX IF NOT EXISTS idx_files_backup_date_tape
    ON files_index(catalog_backup_date, tape_label, file_id);

CREATE INDEX IF NOT EXISTS idx_files_local_chunk
    ON files_index(local_session_id, local_chunk_index, tape_label)
    WHERE local_session_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_local_sessions_active_source
    ON local_sessions(source_dir, session_id DESC)
    WHERE status = 'active';

CREATE INDEX IF NOT EXISTS idx_remote_sessions_active_source
    ON remote_sessions(remote_host, remote_path, session_id DESC)
    WHERE status = 'active';

-- Trigram GIN builds can briefly exceed their maintenance budget before
-- spilling. Keep this below Docker's /dev/shm ceiling so PostgreSQL spills to
-- temp files before the container reaches a hard shared-memory limit.
SET LOCAL maintenance_work_mem = '512MB';

CREATE INDEX IF NOT EXISTS idx_files_catalog_name_trgm
    ON files_index USING gin (catalog_name gin_trgm_ops);

CREATE INDEX IF NOT EXISTS idx_files_original_path_trgm
    ON files_index USING gin (original_path gin_trgm_ops);

RESET maintenance_work_mem;

COMMIT;
