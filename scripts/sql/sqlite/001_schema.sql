-- =============================================================================
-- SQLite catalog schema, version 1.
-- =============================================================================
--
-- The complete catalog schema for the SQLite backend. It is equivalent to the
-- FULLY MIGRATED PostgreSQL schema (scripts/sql/001-011, 013, 007, 014 +
-- finalize, 015-020): same tables, same column names in the same order, same
-- uniqueness, CHECK and FOREIGN KEY constraints, and the guard triggers ported
-- from plpgsql. Migration 012 (sealed tape-write batches) is PostgreSQL-only
-- and deliberately absent.
--
-- Applied automatically, in ONE transaction, the first time the SQLite backend
-- opens a new database file (src/sqlite_core.py). Never edit this file after a
-- release: add scripts/sql/sqlite/002_*.sql instead - the runner records each
-- applied file and its checksum in schema_migrations and refuses a database
-- whose recorded checksum no longer matches.
--
-- Type conventions (src/sqlite_adapter.py picks a converter from the FIRST
-- word of the declared type; the second word gives the storage affinity):
--     TIMESTAMPTZ TEXT   'YYYY-MM-DD HH:MM:SS.ffffff+00:00' (UTC)
--     BOOLEAN INTEGER    0 / 1
--     JSONB TEXT         JSON text
--     BLOB               bytes
--
-- Differences from PostgreSQL, all documented in docs/sqlite_backend.md:
--   * Trigger messages are static text (no interpolated ids).
--   * Guards that read a PostgreSQL GUC read the lto_txn_settings table, whose
--     rows exist only inside the transaction that wrote them.
--   * The manifest-authority check runs immediately, not at COMMIT.
--   * Trigram (GIN) search indexes do not exist; substring search scans.
--   * One PostgreSQL trigger for several events is one SQLite trigger per
--     event: <name>, <name>__upd, <name>__del, <name>__touch, <name>__audit.

-- SQLite-only bookkeeping -------------------------------------------------------

CREATE TABLE schema_migrations (
    version INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    checksum TEXT NOT NULL CHECK (length(checksum) = 64),
    schema_fingerprint TEXT NOT NULL CHECK (length(schema_fingerprint) = 64),
    applied_at TIMESTAMPTZ TEXT NOT NULL
);

-- Transaction-scoped settings read by guard triggers (the counterpart of
-- PostgreSQL set_config(name, value, true)). Rows are written and removed
-- inside one write transaction and are never committed.
CREATE TABLE lto_txn_settings (
    name TEXT PRIMARY KEY,
    value TEXT
) WITHOUT ROWID;

-- Owner token of the running archiver (loss detection). The authority is the
-- operating-system lock on <database>.archiver.lock; see src/sqlite_lock.py.
CREATE TABLE archiver_lock (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    owner_token TEXT NOT NULL,
    pid INTEGER NOT NULL,
    host TEXT NOT NULL,
    acquired_at TIMESTAMPTZ TEXT NOT NULL
);

-- Tables (column order identical to the migrated PostgreSQL schema)

CREATE TABLE archive_artifacts (
    artifact_id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER NOT NULL,
    chunk_index INTEGER NOT NULL,
    container_id INTEGER,
    artifact_kind TEXT NOT NULL,
    artifact_version TEXT NOT NULL,
    local_locator TEXT,
    tape_locator TEXT,
    artifact_size_bytes INTEGER,
    readiness_state TEXT NOT NULL DEFAULT 'planned',
    publication_started_at TIMESTAMPTZ TEXT,
    published_at TIMESTAMPTZ TEXT,
    tape_published_at TIMESTAMPTZ TEXT,
    created_at TIMESTAMPTZ TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')||'000+00:00'),
    updated_at TIMESTAMPTZ TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')||'000+00:00'),
    CONSTRAINT archive_artifacts_artifact_kind_check CHECK ((artifact_kind IN ('zip_manifest', 'tar_sidecar', 'plan_manifest', 'terminal_manifest'))),
    CONSTRAINT archive_artifacts_artifact_size_bytes_check CHECK ((artifact_size_bytes >= 0)),
    CONSTRAINT archive_artifacts_artifact_version_check CHECK ((length(artifact_version) > 0)),
    CONSTRAINT archive_artifacts_chunk_index_check CHECK ((chunk_index >= 0)),
    CONSTRAINT archive_artifacts_locator_distinct_ck CHECK (((local_locator IS NULL) OR (tape_locator IS NULL) OR (local_locator <> tape_locator))),
    CONSTRAINT archive_artifacts_readiness_state_check CHECK ((readiness_state IN ('planned', 'writing', 'validated', 'ready', 'failed', 'blocked'))),
    CONSTRAINT archive_artifacts_ready_ck CHECK (((readiness_state <> 'ready') OR ((local_locator IS NOT NULL) AND (lower(substr(local_locator, -5)) <> '.part') AND (artifact_size_bytes IS NOT NULL) AND (published_at IS NOT NULL)))),
    CONSTRAINT archive_artifacts_scope_ck CHECK ((((artifact_kind IN ('zip_manifest', 'tar_sidecar')) AND (container_id IS NOT NULL)) OR ((artifact_kind IN ('plan_manifest', 'terminal_manifest')) AND (container_id IS NULL)))),
    CONSTRAINT archive_artifacts_chunk_fk FOREIGN KEY (session_id, chunk_index) REFERENCES remote_chunks(session_id, chunk_index) ON DELETE RESTRICT,
    CONSTRAINT archive_artifacts_container_fk FOREIGN KEY (container_id, session_id, chunk_index) REFERENCES archive_containers(container_id, session_id, chunk_index) ON DELETE RESTRICT
);

CREATE TABLE archive_bundles (
    bundle_id INTEGER PRIMARY KEY AUTOINCREMENT,
    tape_label TEXT NOT NULL,
    tape_path TEXT NOT NULL,
    container_id INTEGER,
    container_format TEXT NOT NULL DEFAULT 'zip',
    CONSTRAINT archive_bundles_container_format_ck CHECK ((container_format IN ('zip', 'stored_tar'))),
    CONSTRAINT archive_bundles_container_fk FOREIGN KEY (container_id, container_format) REFERENCES archive_containers(container_id, container_format) ON DELETE RESTRICT,
    CONSTRAINT archive_bundles_tape_label_fkey FOREIGN KEY (tape_label) REFERENCES tapes(volume_label) ON UPDATE CASCADE ON DELETE RESTRICT,
    CONSTRAINT archive_bundles_tape_label_tape_path_key UNIQUE (tape_label, tape_path),
    CONSTRAINT uq_archive_bundles_id_tape UNIQUE (bundle_id, tape_label)
);

CREATE TABLE archive_container_members (
    session_id INTEGER NOT NULL,
    chunk_index INTEGER NOT NULL,
    plan_file_id INTEGER NOT NULL,
    plan_ordinal INTEGER NOT NULL,
    storage_class TEXT NOT NULL,
    container_id INTEGER,
    container_ordinal INTEGER,
    remote_path TEXT NOT NULL,
    expected_logical_bytes INTEGER NOT NULL,
    estimated_tar_bytes INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')||'000+00:00'),
    CONSTRAINT archive_container_members_chunk_index_check CHECK ((chunk_index >= 0)),
    CONSTRAINT archive_container_members_container_ck CHECK ((((storage_class = 'small_files') AND (container_id IS NOT NULL) AND (container_ordinal IS NOT NULL) AND (estimated_tar_bytes > 0)) OR ((storage_class = 'loose_files') AND (container_id IS NULL) AND (container_ordinal IS NULL) AND (estimated_tar_bytes = 0)) OR ((storage_class = 'source_missing') AND (container_id IS NULL) AND (container_ordinal IS NULL) AND (estimated_tar_bytes = 0)))),
    CONSTRAINT archive_container_members_container_ordinal_check CHECK ((container_ordinal >= 0)),
    CONSTRAINT archive_container_members_estimated_tar_bytes_check CHECK ((estimated_tar_bytes >= 0)),
    CONSTRAINT archive_container_members_expected_logical_bytes_check CHECK ((expected_logical_bytes >= 0)),
    CONSTRAINT archive_container_members_plan_ordinal_check CHECK ((plan_ordinal >= 0)),
    CONSTRAINT archive_container_members_remote_path_check CHECK ((length(remote_path) > 0)),
    CONSTRAINT archive_container_members_storage_class_check CHECK ((storage_class IN ('small_files', 'loose_files', 'source_missing'))),
    CONSTRAINT archive_container_members_container_id_session_id_chunk_in_fkey FOREIGN KEY (container_id, session_id, chunk_index) REFERENCES archive_containers(container_id, session_id, chunk_index) ON DELETE RESTRICT,
    CONSTRAINT archive_container_members_plan_file_id_fkey FOREIGN KEY (plan_file_id) REFERENCES remote_plan_files(plan_file_id) ON DELETE RESTRICT,
    CONSTRAINT archive_container_members_session_id_chunk_index_fkey FOREIGN KEY (session_id, chunk_index) REFERENCES remote_chunks(session_id, chunk_index) ON DELETE RESTRICT,
    CONSTRAINT archive_container_members_pkey PRIMARY KEY (session_id, chunk_index, plan_file_id),
    CONSTRAINT archive_container_members_session_id_chunk_index_plan_ordin_key UNIQUE (session_id, chunk_index, plan_ordinal)
);

CREATE TABLE archive_containers (
    container_id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER NOT NULL,
    chunk_index INTEGER NOT NULL,
    container_ordinal INTEGER NOT NULL,
    container_format TEXT NOT NULL,
    format_version TEXT NOT NULL,
    tar_dialect TEXT,
    storage_class TEXT NOT NULL,
    container_name TEXT NOT NULL,
    temporary_data_locator TEXT,
    permanent_local_metadata_locator TEXT,
    tape_label TEXT,
    tape_path TEXT,
    tape_generation_id INTEGER,
    expected_member_count INTEGER NOT NULL DEFAULT 0,
    expected_logical_bytes INTEGER NOT NULL DEFAULT 0,
    observed_member_count INTEGER,
    observed_logical_bytes INTEGER,
    actual_artifact_bytes INTEGER,
    validation_state TEXT NOT NULL DEFAULT 'planned',
    writer_state TEXT NOT NULL DEFAULT 'not_started',
    catalog_state TEXT NOT NULL DEFAULT 'not_started',
    owner_token TEXT,
    lease_expires_at TIMESTAMPTZ TEXT,
    validation_started_at TIMESTAMPTZ TEXT,
    validated_at TIMESTAMPTZ TEXT,
    writer_started_at TIMESTAMPTZ TEXT,
    writer_completed_at TIMESTAMPTZ TEXT,
    catalog_committed_at TIMESTAMPTZ TEXT,
    created_at TIMESTAMPTZ TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')||'000+00:00'),
    updated_at TIMESTAMPTZ TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')||'000+00:00'),
    estimated_archive_bytes INTEGER,
    validated_part_locator TEXT,
    validation_summary JSONB TEXT,
    disposition_counts JSONB TEXT,
    CONSTRAINT archive_containers_actual_artifact_bytes_check CHECK ((actual_artifact_bytes >= 0)),
    CONSTRAINT archive_containers_catalog_ck CHECK (((catalog_state <> 'committed') OR ((writer_state = 'copied') AND (catalog_committed_at IS NOT NULL)))),
    CONSTRAINT archive_containers_catalog_state_check CHECK ((catalog_state IN ('not_started', 'committing', 'committed', 'failed', 'ambiguous'))),
    CONSTRAINT archive_containers_chunk_index_check CHECK ((chunk_index >= 0)),
    CONSTRAINT archive_containers_container_format_check CHECK ((container_format IN ('zip', 'stored_tar'))),
    CONSTRAINT archive_containers_container_name_check CHECK ((length(container_name) > 0)),
    CONSTRAINT archive_containers_container_ordinal_check CHECK ((container_ordinal >= 0)),
    CONSTRAINT archive_containers_dialect_ck CHECK ((((container_format = 'stored_tar') AND (tar_dialect IS NOT NULL) AND (length(tar_dialect) > 0)) OR ((container_format = 'zip') AND (tar_dialect IS NULL)))),
    CONSTRAINT archive_containers_estimated_archive_bytes_check CHECK (((estimated_archive_bytes IS NULL) OR (estimated_archive_bytes >= 0))),
    CONSTRAINT archive_containers_expected_logical_bytes_check CHECK ((expected_logical_bytes >= 0)),
    CONSTRAINT archive_containers_expected_member_count_check CHECK ((expected_member_count >= 0)),
    CONSTRAINT archive_containers_format_version_check CHECK ((length(format_version) > 0)),
    CONSTRAINT archive_containers_observed_ck CHECK (((validation_state NOT IN ('validated_part', 'ready')) OR ((observed_member_count IS NOT NULL) AND (observed_logical_bytes IS NOT NULL) AND (actual_artifact_bytes IS NOT NULL) AND (((container_format = 'zip') AND (observed_member_count = expected_member_count) AND (observed_logical_bytes = expected_logical_bytes)) OR ((container_format = 'stored_tar') AND (observed_member_count <= expected_member_count) AND (observed_logical_bytes <= expected_logical_bytes)))))),
    CONSTRAINT archive_containers_observed_logical_bytes_check CHECK ((observed_logical_bytes >= 0)),
    CONSTRAINT archive_containers_observed_member_count_check CHECK ((observed_member_count >= 0)),
    CONSTRAINT archive_containers_ready_pair_ck CHECK (((container_format <> 'stored_tar') OR (validation_state <> 'ready') OR ((temporary_data_locator IS NOT NULL) AND (lower(substr(temporary_data_locator, -5)) <> '.part') AND (permanent_local_metadata_locator IS NOT NULL) AND (lower(substr(permanent_local_metadata_locator, -5)) <> '.part') AND (disposition_counts IS NOT NULL)))),
    CONSTRAINT archive_containers_storage_class_check CHECK ((storage_class IN ('small_files', 'loose_files'))),
    CONSTRAINT archive_containers_tape_locator_ck CHECK ((((tape_label IS NULL) AND (tape_path IS NULL) AND (tape_generation_id IS NULL)) OR ((tape_label IS NOT NULL) AND (tape_path IS NOT NULL) AND (tape_generation_id IS NOT NULL)))),
    CONSTRAINT archive_containers_validated_part_ck CHECK (((container_format <> 'stored_tar') OR (validation_state NOT IN ('validated_part', 'ready')) OR ((validated_part_locator IS NOT NULL) AND (lower(substr(validated_part_locator, -5)) = '.part') AND (validation_summary IS NOT NULL)))),
    CONSTRAINT archive_containers_validation_state_check CHECK ((validation_state IN ('planned', 'building', 'validated_part', 'ready', 'failed', 'blocked'))),
    CONSTRAINT archive_containers_writer_ck CHECK (((writer_state <> 'copied') OR ((tape_path IS NOT NULL) AND (writer_started_at IS NOT NULL) AND (writer_completed_at IS NOT NULL)))),
    CONSTRAINT archive_containers_writer_state_check CHECK ((writer_state IN ('not_started', 'writing', 'copied', 'failed', 'ambiguous'))),
    CONSTRAINT archive_containers_chunk_format_fk FOREIGN KEY (session_id, chunk_index, container_format) REFERENCES remote_chunks(session_id, chunk_index, packaging_format) ON DELETE RESTRICT,
    CONSTRAINT archive_containers_tape_generation_id_fkey FOREIGN KEY (tape_generation_id) REFERENCES tape_generations(generation_id) ON DELETE RESTRICT,
    CONSTRAINT archive_containers_tape_label_fkey FOREIGN KEY (tape_label) REFERENCES tapes(volume_label) ON UPDATE CASCADE ON DELETE RESTRICT,
    CONSTRAINT archive_containers_id_format_uq UNIQUE (container_id, container_format),
    CONSTRAINT archive_containers_identity_uq UNIQUE (container_id, session_id, chunk_index),
    CONSTRAINT archive_containers_ordinal_uq UNIQUE (session_id, chunk_index, container_ordinal)
);

CREATE TABLE archive_directories (
    directory_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_host TEXT NOT NULL,
    canonical_path TEXT NOT NULL,
    parent_directory_id INTEGER,
    name TEXT NOT NULL,
    depth INTEGER NOT NULL,
    created_at TIMESTAMPTZ TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')||'000+00:00'),
    updated_at TIMESTAMPTZ TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')||'000+00:00'),
    CONSTRAINT archive_directories_canonical_path_check CHECK ((length(trim(canonical_path)) > 0)),
    CONSTRAINT archive_directories_depth_check CHECK ((depth >= 0)),
    CONSTRAINT archive_directories_name_check CHECK ((length(name) > 0)),
    CONSTRAINT archive_directories_parent_ck CHECK (((parent_directory_id IS NULL) OR (parent_directory_id <> directory_id))),
    CONSTRAINT archive_directories_root_depth_ck CHECK ((((parent_directory_id IS NULL) AND (depth = 0)) OR ((parent_directory_id IS NOT NULL) AND (depth > 0)))),
    CONSTRAINT archive_directories_source_host_check CHECK ((length(trim(source_host)) > 0)),
    CONSTRAINT archive_directories_parent_directory_id_fkey FOREIGN KEY (parent_directory_id) REFERENCES archive_directories(directory_id) ON DELETE RESTRICT,
    CONSTRAINT archive_directories_source_path_uq UNIQUE (source_host, canonical_path)
);

CREATE TABLE archive_runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_label TEXT NOT NULL,
    tape_label TEXT NOT NULL,
    session_kind TEXT NOT NULL DEFAULT 'legacy',
    local_session_id INTEGER,
    remote_session_id INTEGER,
    started_at TIMESTAMPTZ TEXT NOT NULL,
    completed_at TIMESTAMPTZ TEXT,
    remote_chunk_index INTEGER,
    tape_generation_id INTEGER,
    CONSTRAINT archive_runs_remote_chunk_index_ck CHECK (((remote_chunk_index IS NULL) OR (remote_chunk_index >= 0))),
    CONSTRAINT archive_runs_session_kind_check CHECK ((session_kind IN ('legacy', 'local', 'remote'))),
    CONSTRAINT archive_runs_session_kind_ck CHECK (((NOT ((local_session_id IS NOT NULL) AND (remote_session_id IS NOT NULL))) AND ((session_kind <> 'legacy') OR ((local_session_id IS NULL) AND (remote_session_id IS NULL))) AND ((session_kind <> 'local') OR (remote_session_id IS NULL)) AND ((session_kind <> 'remote') OR (local_session_id IS NULL)))),
    CONSTRAINT archive_runs_local_session_id_fkey FOREIGN KEY (local_session_id) REFERENCES local_sessions(session_id) ON DELETE SET NULL,
    CONSTRAINT archive_runs_remote_session_id_fkey FOREIGN KEY (remote_session_id) REFERENCES remote_sessions(session_id) ON DELETE SET NULL,
    CONSTRAINT archive_runs_tape_generation_fk FOREIGN KEY (tape_generation_id) REFERENCES tape_generations(generation_id) ON DELETE RESTRICT,
    CONSTRAINT archive_runs_tape_label_fkey FOREIGN KEY (tape_label) REFERENCES tapes(volume_label) ON UPDATE CASCADE,
    CONSTRAINT archive_runs_run_label_tape_label_key UNIQUE (run_label, tape_label)
);

CREATE TABLE catalog_directories (
    directory_id INTEGER PRIMARY KEY AUTOINCREMENT,
    tape_label TEXT NOT NULL,
    parent_id INTEGER,
    name TEXT NOT NULL,
    normalized_path TEXT NOT NULL,
    CONSTRAINT catalog_directories_parent_id_fkey FOREIGN KEY (parent_id) REFERENCES catalog_directories(directory_id) ON DELETE CASCADE,
    CONSTRAINT catalog_directories_tape_label_fkey FOREIGN KEY (tape_label) REFERENCES tapes(volume_label) ON UPDATE CASCADE ON DELETE RESTRICT,
    CONSTRAINT catalog_directories_tape_label_normalized_path_key UNIQUE (tape_label, normalized_path)
);

CREATE TABLE container_format_schema_authority (
    authority_id INTEGER PRIMARY KEY AUTOINCREMENT,
    authority_version INTEGER NOT NULL,
    schema_fingerprint TEXT NOT NULL,
    migration_checksums JSONB TEXT NOT NULL,
    contract_definition JSONB TEXT NOT NULL,
    applied_at TIMESTAMPTZ TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')||'000+00:00'),
    CONSTRAINT container_format_schema_authority_authority_version_check CHECK ((authority_version >= 2)),
    CONSTRAINT container_format_schema_authority_schema_fingerprint_check CHECK ((length(schema_fingerprint) = 32)),
    CONSTRAINT container_format_schema_authority_authority_version_key UNIQUE (authority_version)
);

CREATE TABLE container_format_schema_metadata (
    singleton BOOLEAN INTEGER NOT NULL DEFAULT 1,
    schema_version INTEGER NOT NULL,
    required_reader_contract_version INTEGER NOT NULL,
    stored_tar_dialect TEXT NOT NULL,
    migration_checksum TEXT NOT NULL,
    schema_fingerprint TEXT NOT NULL,
    applied_at TIMESTAMPTZ TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')||'000+00:00'),
    CONSTRAINT container_format_schema_metadata_migration_checksum_check CHECK ((length(migration_checksum) = 64)),
    CONSTRAINT container_format_schema_metadata_schema_fingerprint_check CHECK ((length(schema_fingerprint) = 32)),
    CONSTRAINT container_format_schema_metadata_singleton_check CHECK (singleton),
    CONSTRAINT container_format_schema_metadata_pkey PRIMARY KEY (singleton)
);

CREATE TABLE directory_archive_bundles (
    bundle_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_host TEXT NOT NULL,
    original_dir_path TEXT NOT NULL,
    tape_label TEXT NOT NULL,
    archive_run_id INTEGER,
    local_session_id INTEGER,
    remote_session_id INTEGER,
    chunk_index INTEGER,
    stored_bundle_path TEXT NOT NULL,
    manifest_path TEXT,
    manifest_format TEXT,
    manifest_compression TEXT,
    file_count INTEGER NOT NULL,
    byte_count INTEGER NOT NULL,
    small_file_count INTEGER NOT NULL,
    small_file_bytes INTEGER NOT NULL,
    large_file_count INTEGER NOT NULL,
    large_file_bytes INTEGER NOT NULL,
    backup_date TIMESTAMPTZ TEXT NOT NULL,
    record_key BLOB NOT NULL,
    container_id INTEGER,
    container_format TEXT,
    tape_generation_id INTEGER,
    actual_artifact_bytes INTEGER,
    CONSTRAINT directory_archive_bundles_artifact_bytes_ck CHECK (((actual_artifact_bytes IS NULL) OR (actual_artifact_bytes >= 0))),
    CONSTRAINT directory_archive_bundles_byte_count_check CHECK ((byte_count >= 0)),
    CONSTRAINT directory_archive_bundles_container_format_ck CHECK (((container_format IS NULL) OR (container_format IN ('zip', 'stored_tar')))),
    CONSTRAINT directory_archive_bundles_file_count_check CHECK ((file_count >= 0)),
    CONSTRAINT directory_archive_bundles_large_file_bytes_check CHECK ((large_file_bytes >= 0)),
    CONSTRAINT directory_archive_bundles_large_file_count_check CHECK ((large_file_count >= 0)),
    CONSTRAINT directory_archive_bundles_record_key_check CHECK ((typeof(record_key) = 'blob' AND length(record_key) = 32)),
    CONSTRAINT directory_archive_bundles_small_file_bytes_check CHECK ((small_file_bytes >= 0)),
    CONSTRAINT directory_archive_bundles_small_file_count_check CHECK ((small_file_count >= 0)),
    CONSTRAINT directory_archive_bundles_archive_run_id_fkey FOREIGN KEY (archive_run_id) REFERENCES archive_runs(run_id) ON DELETE SET NULL,
    CONSTRAINT directory_archive_bundles_container_fk FOREIGN KEY (container_id) REFERENCES archive_containers(container_id) ON DELETE RESTRICT,
    CONSTRAINT directory_archive_bundles_generation_fk FOREIGN KEY (tape_generation_id) REFERENCES tape_generations(generation_id) ON DELETE RESTRICT,
    CONSTRAINT directory_archive_bundles_local_session_id_fkey FOREIGN KEY (local_session_id) REFERENCES local_sessions(session_id) ON DELETE SET NULL,
    CONSTRAINT directory_archive_bundles_remote_session_id_fkey FOREIGN KEY (remote_session_id) REFERENCES remote_sessions(session_id) ON DELETE SET NULL,
    CONSTRAINT directory_archive_bundles_tape_label_fkey FOREIGN KEY (tape_label) REFERENCES tapes(volume_label) ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT directory_archive_bundles_record_key_key UNIQUE (record_key),
    CONSTRAINT uq_dir_bundles_id_tape UNIQUE (bundle_id, tape_label)
);

CREATE TABLE directory_archive_parts (
    part_id INTEGER PRIMARY KEY AUTOINCREMENT,
    directory_id INTEGER NOT NULL,
    session_id INTEGER NOT NULL,
    chunk_index INTEGER NOT NULL,
    container_id INTEGER,
    loose_record_key BLOB,
    tape_generation_id INTEGER,
    storage_class TEXT NOT NULL,
    evidence_generation INTEGER NOT NULL DEFAULT 1,
    direct_expected_count INTEGER NOT NULL DEFAULT 0,
    direct_expected_bytes INTEGER NOT NULL DEFAULT 0,
    direct_archived_count INTEGER NOT NULL DEFAULT 0,
    direct_archived_bytes INTEGER NOT NULL DEFAULT 0,
    direct_source_missing_count INTEGER NOT NULL DEFAULT 0,
    direct_source_missing_bytes INTEGER NOT NULL DEFAULT 0,
    direct_source_permission_denied_count INTEGER NOT NULL DEFAULT 0,
    direct_source_permission_denied_bytes INTEGER NOT NULL DEFAULT 0,
    direct_source_unreadable_count INTEGER NOT NULL DEFAULT 0,
    direct_source_unreadable_bytes INTEGER NOT NULL DEFAULT 0,
    direct_source_changed_count INTEGER NOT NULL DEFAULT 0,
    direct_source_changed_bytes INTEGER NOT NULL DEFAULT 0,
    direct_unresolved_count INTEGER NOT NULL DEFAULT 0,
    direct_unresolved_bytes INTEGER NOT NULL DEFAULT 0,
    plan_manifest_artifact_id INTEGER,
    sidecar_artifact_id INTEGER,
    terminal_manifest_artifact_id INTEGER,
    plan_artifact_locator TEXT,
    sidecar_artifact_locator TEXT,
    terminal_artifact_locator TEXT,
    local_validation_state TEXT NOT NULL DEFAULT 'not_validated',
    writer_state TEXT NOT NULL DEFAULT 'not_started',
    catalog_state TEXT NOT NULL DEFAULT 'not_started',
    restore_format TEXT NOT NULL,
    tape_label TEXT,
    stored_path TEXT,
    source_base_path TEXT NOT NULL,
    container_member_candidate TEXT,
    routing_precision TEXT NOT NULL,
    created_at TIMESTAMPTZ TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')||'000+00:00'),
    updated_at TIMESTAMPTZ TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')||'000+00:00'),
    CONSTRAINT directory_archive_parts_catalog_state_check CHECK ((catalog_state IN ('not_started', 'committing', 'committed', 'failed', 'ambiguous'))),
    CONSTRAINT directory_archive_parts_catalog_writer_ck CHECK (((catalog_state <> 'committed') OR (writer_state = 'copied'))),
    CONSTRAINT directory_archive_parts_chunk_index_check CHECK ((chunk_index >= 0)),
    CONSTRAINT directory_archive_parts_direct_archived_bytes_check CHECK ((direct_archived_bytes >= 0)),
    CONSTRAINT directory_archive_parts_direct_archived_count_check CHECK ((direct_archived_count >= 0)),
    CONSTRAINT directory_archive_parts_direct_expected_bytes_check CHECK ((direct_expected_bytes >= 0)),
    CONSTRAINT directory_archive_parts_direct_expected_count_check CHECK ((direct_expected_count >= 0)),
    CONSTRAINT directory_archive_parts_direct_source_changed_bytes_check CHECK ((direct_source_changed_bytes >= 0)),
    CONSTRAINT directory_archive_parts_direct_source_changed_count_check CHECK ((direct_source_changed_count >= 0)),
    CONSTRAINT directory_archive_parts_direct_source_missing_bytes_check CHECK ((direct_source_missing_bytes >= 0)),
    CONSTRAINT directory_archive_parts_direct_source_missing_count_check CHECK ((direct_source_missing_count >= 0)),
    CONSTRAINT directory_archive_parts_direct_source_permission_denied_b_check CHECK ((direct_source_permission_denied_bytes >= 0)),
    CONSTRAINT directory_archive_parts_direct_source_permission_denied_c_check CHECK ((direct_source_permission_denied_count >= 0)),
    CONSTRAINT directory_archive_parts_direct_source_unreadable_bytes_check CHECK ((direct_source_unreadable_bytes >= 0)),
    CONSTRAINT directory_archive_parts_direct_source_unreadable_count_check CHECK ((direct_source_unreadable_count >= 0)),
    CONSTRAINT directory_archive_parts_direct_unresolved_bytes_check CHECK ((direct_unresolved_bytes >= 0)),
    CONSTRAINT directory_archive_parts_direct_unresolved_count_check CHECK ((direct_unresolved_count >= 0)),
    CONSTRAINT directory_archive_parts_evidence_generation_check CHECK ((evidence_generation >= 1)),
    CONSTRAINT directory_archive_parts_exact_route_ck CHECK (((routing_precision <> 'exact') OR (container_id IS NULL) OR ((container_member_candidate IS NOT NULL) AND (length(container_member_candidate) > 0)))),
    CONSTRAINT directory_archive_parts_exactly_one_identity_ck CHECK ((((container_id IS NOT NULL) + (loose_record_key IS NOT NULL)) = 1)),
    CONSTRAINT directory_archive_parts_local_validation_state_check CHECK ((local_validation_state IN ('not_validated', 'pending', 'succeeded', 'failed', 'ambiguous'))),
    CONSTRAINT directory_archive_parts_locator_ck CHECK ((((plan_artifact_locator IS NULL) OR (lower(substr(plan_artifact_locator, -5)) <> '.part')) AND ((sidecar_artifact_locator IS NULL) OR (lower(substr(sidecar_artifact_locator, -5)) <> '.part')) AND ((terminal_artifact_locator IS NULL) OR (lower(substr(terminal_artifact_locator, -5)) <> '.part')))),
    CONSTRAINT directory_archive_parts_loose_key_ck CHECK (((loose_record_key IS NULL) OR (typeof(loose_record_key) = 'blob' AND length(loose_record_key) = 32))),
    CONSTRAINT directory_archive_parts_outcome_bytes_ck CHECK (((((((direct_archived_bytes + direct_source_missing_bytes) + direct_source_permission_denied_bytes) + direct_source_unreadable_bytes) + direct_source_changed_bytes) + direct_unresolved_bytes) <= direct_expected_bytes)),
    CONSTRAINT directory_archive_parts_outcome_total_ck CHECK (((((((direct_archived_count + direct_source_missing_count) + direct_source_permission_denied_count) + direct_source_unreadable_count) + direct_source_changed_count) + direct_unresolved_count) <= direct_expected_count)),
    CONSTRAINT directory_archive_parts_restore_format_check CHECK ((restore_format IN ('zip', 'stored_tar', 'loose'))),
    CONSTRAINT directory_archive_parts_route_tuple_ck CHECK (((((tape_generation_id IS NOT NULL) + (tape_label IS NOT NULL) + (stored_path IS NOT NULL)) IN (0, 3)) AND (((direct_archived_count = 0) AND (direct_archived_bytes = 0)) OR (writer_state = 'copied')) AND (((writer_state <> 'copied') AND (direct_archived_count = 0)) OR (((tape_generation_id IS NOT NULL) + (tape_label IS NOT NULL) + (stored_path IS NOT NULL)) = 3)))),
    CONSTRAINT directory_archive_parts_routing_precision_check CHECK ((routing_precision IN ('exact', 'coarse'))),
    CONSTRAINT directory_archive_parts_source_base_path_check CHECK ((length(trim(source_base_path)) > 0)),
    CONSTRAINT directory_archive_parts_storage_class_check CHECK ((storage_class IN ('container', 'loose'))),
    CONSTRAINT directory_archive_parts_storage_identity_ck CHECK ((((container_id IS NOT NULL) AND (loose_record_key IS NULL) AND (storage_class = 'container') AND (restore_format IN ('zip', 'stored_tar'))) OR ((container_id IS NULL) AND (loose_record_key IS NOT NULL) AND (storage_class = 'loose') AND (restore_format = 'loose')))),
    CONSTRAINT directory_archive_parts_stored_path_check CHECK (((stored_path IS NULL) OR (length(trim(stored_path)) > 0))),
    CONSTRAINT directory_archive_parts_writer_state_check CHECK ((writer_state IN ('not_started', 'writing', 'copied', 'failed', 'ambiguous'))),
    CONSTRAINT directory_archive_parts_chunk_fk FOREIGN KEY (session_id, chunk_index) REFERENCES remote_chunks(session_id, chunk_index) ON DELETE RESTRICT,
    CONSTRAINT directory_archive_parts_container_fk FOREIGN KEY (container_id, session_id, chunk_index) REFERENCES archive_containers(container_id, session_id, chunk_index) ON DELETE RESTRICT,
    CONSTRAINT directory_archive_parts_directory_id_fkey FOREIGN KEY (directory_id) REFERENCES archive_directories(directory_id) ON DELETE RESTRICT,
    CONSTRAINT directory_archive_parts_loose_record_fk FOREIGN KEY (loose_record_key) REFERENCES files_index(record_key) ON DELETE RESTRICT,
    CONSTRAINT directory_archive_parts_plan_manifest_artifact_id_fkey FOREIGN KEY (plan_manifest_artifact_id) REFERENCES archive_artifacts(artifact_id) ON DELETE RESTRICT,
    CONSTRAINT directory_archive_parts_sidecar_artifact_id_fkey FOREIGN KEY (sidecar_artifact_id) REFERENCES archive_artifacts(artifact_id) ON DELETE RESTRICT,
    CONSTRAINT directory_archive_parts_tape_generation_id_fkey FOREIGN KEY (tape_generation_id) REFERENCES tape_generations(generation_id) ON DELETE RESTRICT,
    CONSTRAINT directory_archive_parts_tape_label_fkey FOREIGN KEY (tape_label) REFERENCES tapes(volume_label) ON UPDATE CASCADE ON DELETE RESTRICT,
    CONSTRAINT directory_archive_parts_terminal_manifest_artifact_id_fkey FOREIGN KEY (terminal_manifest_artifact_id) REFERENCES archive_artifacts(artifact_id) ON DELETE RESTRICT
);

CREATE TABLE directory_archive_stats (
    stat_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_host TEXT NOT NULL,
    original_dir_path TEXT NOT NULL,
    tape_label TEXT NOT NULL,
    archive_run_id INTEGER,
    local_session_id INTEGER,
    remote_session_id INTEGER,
    chunk_index INTEGER,
    stored_root_path TEXT,
    direct_file_count INTEGER NOT NULL,
    direct_bytes INTEGER NOT NULL,
    recursive_file_count INTEGER NOT NULL,
    recursive_bytes INTEGER NOT NULL,
    small_file_count INTEGER NOT NULL,
    small_file_bytes INTEGER NOT NULL,
    large_file_count INTEGER NOT NULL,
    large_file_bytes INTEGER NOT NULL,
    packed_bundle_count INTEGER NOT NULL,
    backup_date TIMESTAMPTZ TEXT NOT NULL,
    record_key BLOB NOT NULL,
    CONSTRAINT directory_archive_stats_direct_bytes_check CHECK ((direct_bytes >= 0)),
    CONSTRAINT directory_archive_stats_direct_file_count_check CHECK ((direct_file_count >= 0)),
    CONSTRAINT directory_archive_stats_large_file_bytes_check CHECK ((large_file_bytes >= 0)),
    CONSTRAINT directory_archive_stats_large_file_count_check CHECK ((large_file_count >= 0)),
    CONSTRAINT directory_archive_stats_packed_bundle_count_check CHECK ((packed_bundle_count >= 0)),
    CONSTRAINT directory_archive_stats_record_key_check CHECK ((typeof(record_key) = 'blob' AND length(record_key) = 32)),
    CONSTRAINT directory_archive_stats_recursive_bytes_check CHECK ((recursive_bytes >= 0)),
    CONSTRAINT directory_archive_stats_recursive_file_count_check CHECK ((recursive_file_count >= 0)),
    CONSTRAINT directory_archive_stats_small_file_bytes_check CHECK ((small_file_bytes >= 0)),
    CONSTRAINT directory_archive_stats_small_file_count_check CHECK ((small_file_count >= 0)),
    CONSTRAINT directory_archive_stats_archive_run_id_fkey FOREIGN KEY (archive_run_id) REFERENCES archive_runs(run_id) ON DELETE SET NULL,
    CONSTRAINT directory_archive_stats_local_session_id_fkey FOREIGN KEY (local_session_id) REFERENCES local_sessions(session_id) ON DELETE SET NULL,
    CONSTRAINT directory_archive_stats_remote_session_id_fkey FOREIGN KEY (remote_session_id) REFERENCES remote_sessions(session_id) ON DELETE SET NULL,
    CONSTRAINT directory_archive_stats_tape_label_fkey FOREIGN KEY (tape_label) REFERENCES tapes(volume_label) ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT directory_archive_stats_record_key_key UNIQUE (record_key)
);

CREATE TABLE directory_completeness (
    session_id INTEGER NOT NULL,
    directory_id INTEGER NOT NULL,
    direct_expected_file_count INTEGER NOT NULL DEFAULT 0,
    direct_expected_bytes INTEGER NOT NULL DEFAULT 0,
    direct_archived_file_count INTEGER NOT NULL DEFAULT 0,
    direct_archived_bytes INTEGER NOT NULL DEFAULT 0,
    recursive_expected_file_count INTEGER NOT NULL DEFAULT 0,
    recursive_expected_bytes INTEGER NOT NULL DEFAULT 0,
    recursive_archived_file_count INTEGER NOT NULL DEFAULT 0,
    recursive_archived_bytes INTEGER NOT NULL DEFAULT 0,
    direct_source_missing_count INTEGER NOT NULL DEFAULT 0,
    direct_source_permission_denied_count INTEGER NOT NULL DEFAULT 0,
    direct_source_unreadable_count INTEGER NOT NULL DEFAULT 0,
    direct_source_changed_count INTEGER NOT NULL DEFAULT 0,
    direct_unresolved_count INTEGER NOT NULL DEFAULT 0,
    recursive_source_missing_count INTEGER NOT NULL DEFAULT 0,
    recursive_source_permission_denied_count INTEGER NOT NULL DEFAULT 0,
    recursive_source_unreadable_count INTEGER NOT NULL DEFAULT 0,
    recursive_source_changed_count INTEGER NOT NULL DEFAULT 0,
    recursive_unresolved_count INTEGER NOT NULL DEFAULT 0,
    scan_is_final BOOLEAN INTEGER NOT NULL DEFAULT 0,
    all_planned_items_terminal BOOLEAN INTEGER NOT NULL DEFAULT 0,
    all_required_items_archived BOOLEAN INTEGER NOT NULL DEFAULT 0,
    all_parts_written BOOLEAN INTEGER NOT NULL DEFAULT 0,
    all_writer_completions_succeeded BOOLEAN INTEGER NOT NULL DEFAULT 0,
    all_parts_cataloged BOOLEAN INTEGER NOT NULL DEFAULT 0,
    all_local_validation_succeeded BOOLEAN INTEGER NOT NULL DEFAULT 0,
    has_ambiguous_evidence BOOLEAN INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'provisional',
    pinned_frontier_generation INTEGER NOT NULL DEFAULT 0,
    pinned_artifact_evidence_generation INTEGER NOT NULL DEFAULT 0,
    calculated_at TIMESTAMPTZ TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')||'000+00:00'),
    CONSTRAINT directory_completeness_derived_status_ck CHECK ((status =
CASE
    WHEN (has_ambiguous_evidence OR (recursive_unresolved_count > 0)) THEN 'ambiguous'
    WHEN (NOT scan_is_final) THEN 'provisional'
    WHEN (NOT (all_planned_items_terminal AND all_required_items_archived AND all_parts_written AND all_writer_completions_succeeded AND all_parts_cataloged AND all_local_validation_succeeded)) THEN 'incomplete'
    WHEN ((((recursive_source_missing_count + recursive_source_permission_denied_count) + recursive_source_unreadable_count) + recursive_source_changed_count) > 0) THEN 'complete_with_source_exceptions'
    ELSE 'complete'
END)),
    CONSTRAINT directory_completeness_direct_archived_bytes_check CHECK ((direct_archived_bytes >= 0)),
    CONSTRAINT directory_completeness_direct_archived_file_count_check CHECK ((direct_archived_file_count >= 0)),
    CONSTRAINT directory_completeness_direct_expected_bytes_check CHECK ((direct_expected_bytes >= 0)),
    CONSTRAINT directory_completeness_direct_expected_file_count_check CHECK ((direct_expected_file_count >= 0)),
    CONSTRAINT directory_completeness_direct_source_changed_count_check CHECK ((direct_source_changed_count >= 0)),
    CONSTRAINT directory_completeness_direct_source_missing_count_check CHECK ((direct_source_missing_count >= 0)),
    CONSTRAINT directory_completeness_direct_source_permission_denied_co_check CHECK ((direct_source_permission_denied_count >= 0)),
    CONSTRAINT directory_completeness_direct_source_unreadable_count_check CHECK ((direct_source_unreadable_count >= 0)),
    CONSTRAINT directory_completeness_direct_unresolved_count_check CHECK ((direct_unresolved_count >= 0)),
    CONSTRAINT directory_completeness_pinned_artifact_evidence_generatio_check CHECK ((pinned_artifact_evidence_generation >= 0)),
    CONSTRAINT directory_completeness_pinned_frontier_generation_check CHECK ((pinned_frontier_generation >= 0)),
    CONSTRAINT directory_completeness_recursive_archived_bytes_check CHECK ((recursive_archived_bytes >= 0)),
    CONSTRAINT directory_completeness_recursive_archived_file_count_check CHECK ((recursive_archived_file_count >= 0)),
    CONSTRAINT directory_completeness_recursive_ck CHECK (((recursive_expected_file_count >= direct_expected_file_count) AND (recursive_expected_bytes >= direct_expected_bytes) AND (recursive_archived_file_count >= direct_archived_file_count) AND (recursive_archived_bytes >= direct_archived_bytes) AND (recursive_source_missing_count >= direct_source_missing_count) AND (recursive_source_permission_denied_count >= direct_source_permission_denied_count) AND (recursive_source_unreadable_count >= direct_source_unreadable_count) AND (recursive_source_changed_count >= direct_source_changed_count) AND (recursive_unresolved_count >= direct_unresolved_count))),
    CONSTRAINT directory_completeness_recursive_expected_bytes_check CHECK ((recursive_expected_bytes >= 0)),
    CONSTRAINT directory_completeness_recursive_expected_file_count_check CHECK ((recursive_expected_file_count >= 0)),
    CONSTRAINT directory_completeness_recursive_source_changed_count_check CHECK ((recursive_source_changed_count >= 0)),
    CONSTRAINT directory_completeness_recursive_source_missing_count_check CHECK ((recursive_source_missing_count >= 0)),
    CONSTRAINT directory_completeness_recursive_source_permission_denied_check CHECK ((recursive_source_permission_denied_count >= 0)),
    CONSTRAINT directory_completeness_recursive_source_unreadable_count_check CHECK ((recursive_source_unreadable_count >= 0)),
    CONSTRAINT directory_completeness_recursive_unresolved_count_check CHECK ((recursive_unresolved_count >= 0)),
    CONSTRAINT directory_completeness_required_archive_ck CHECK (((NOT all_required_items_archived) OR ((recursive_unresolved_count = 0) AND (((((recursive_archived_file_count + recursive_source_missing_count) + recursive_source_permission_denied_count) + recursive_source_unreadable_count) + recursive_source_changed_count) = recursive_expected_file_count)))),
    CONSTRAINT directory_completeness_status_check CHECK ((status IN ('ambiguous', 'provisional', 'incomplete', 'complete_with_source_exceptions', 'complete'))),
    CONSTRAINT directory_completeness_terminal_partition_ck CHECK (((NOT all_planned_items_terminal) OR ((((((recursive_archived_file_count + recursive_source_missing_count) + recursive_source_permission_denied_count) + recursive_source_unreadable_count) + recursive_source_changed_count) + recursive_unresolved_count) = recursive_expected_file_count))),
    CONSTRAINT directory_completeness_directory_id_fkey FOREIGN KEY (directory_id) REFERENCES archive_directories(directory_id) ON DELETE RESTRICT,
    CONSTRAINT directory_completeness_session_id_fkey FOREIGN KEY (session_id) REFERENCES remote_sessions(session_id) ON DELETE RESTRICT,
    CONSTRAINT directory_completeness_pkey PRIMARY KEY (session_id, directory_id)
);

CREATE TABLE directory_scan_coverage (
    coverage_id INTEGER PRIMARY KEY AUTOINCREMENT,
    directory_id INTEGER NOT NULL,
    session_id INTEGER NOT NULL,
    scan_scope_id INTEGER NOT NULL,
    coverage_state TEXT NOT NULL,
    direct_discovered_file_count INTEGER NOT NULL DEFAULT 0,
    direct_discovered_bytes INTEGER NOT NULL DEFAULT 0,
    direct_discovered_directory_count INTEGER NOT NULL DEFAULT 0,
    recursive_discovered_file_count INTEGER NOT NULL DEFAULT 0,
    recursive_discovered_bytes INTEGER NOT NULL DEFAULT 0,
    recursive_discovered_directory_count INTEGER NOT NULL DEFAULT 0,
    excluded_count INTEGER NOT NULL DEFAULT 0,
    error_count INTEGER NOT NULL DEFAULT 0,
    frontier_generation INTEGER NOT NULL,
    created_at TIMESTAMPTZ TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')||'000+00:00'),
    updated_at TIMESTAMPTZ TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')||'000+00:00'),
    CONSTRAINT directory_scan_coverage_coverage_state_check CHECK ((coverage_state IN ('provisional', 'final', 'error'))),
    CONSTRAINT directory_scan_coverage_direct_discovered_bytes_check CHECK ((direct_discovered_bytes >= 0)),
    CONSTRAINT directory_scan_coverage_direct_discovered_directory_count_check CHECK ((direct_discovered_directory_count >= 0)),
    CONSTRAINT directory_scan_coverage_direct_discovered_file_count_check CHECK ((direct_discovered_file_count >= 0)),
    CONSTRAINT directory_scan_coverage_error_ck CHECK (((coverage_state <> 'error') OR (error_count > 0))),
    CONSTRAINT directory_scan_coverage_error_count_check CHECK ((error_count >= 0)),
    CONSTRAINT directory_scan_coverage_excluded_count_check CHECK ((excluded_count >= 0)),
    CONSTRAINT directory_scan_coverage_frontier_generation_check CHECK ((frontier_generation >= 0)),
    CONSTRAINT directory_scan_coverage_recursive_ck CHECK (((recursive_discovered_file_count >= direct_discovered_file_count) AND (recursive_discovered_bytes >= direct_discovered_bytes) AND (recursive_discovered_directory_count >= direct_discovered_directory_count))),
    CONSTRAINT directory_scan_coverage_recursive_discovered_bytes_check CHECK ((recursive_discovered_bytes >= 0)),
    CONSTRAINT directory_scan_coverage_recursive_discovered_directory_co_check CHECK ((recursive_discovered_directory_count >= 0)),
    CONSTRAINT directory_scan_coverage_recursive_discovered_file_count_check CHECK ((recursive_discovered_file_count >= 0)),
    CONSTRAINT directory_scan_coverage_directory_id_fkey FOREIGN KEY (directory_id) REFERENCES archive_directories(directory_id) ON DELETE RESTRICT,
    CONSTRAINT directory_scan_coverage_scan_scope_id_fkey FOREIGN KEY (scan_scope_id) REFERENCES remote_scan_scopes(scan_scope_id) ON DELETE RESTRICT,
    CONSTRAINT directory_scan_coverage_session_id_fkey FOREIGN KEY (session_id) REFERENCES remote_sessions(session_id) ON DELETE RESTRICT,
    CONSTRAINT directory_scan_coverage_generation_uq UNIQUE (directory_id, session_id, scan_scope_id, frontier_generation)
);

CREATE TABLE directory_tree_index (
    dir_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_host TEXT NOT NULL,
    original_dir_path TEXT NOT NULL,
    parent_original_dir_path TEXT,
    dir_name TEXT NOT NULL,
    depth INTEGER NOT NULL,
    tape_label TEXT NOT NULL,
    archive_run_id INTEGER,
    local_session_id INTEGER,
    remote_session_id INTEGER,
    chunk_index INTEGER,
    bundle_id INTEGER,
    stored_bundle_path TEXT,
    manifest_path TEXT,
    direct_file_count INTEGER NOT NULL,
    direct_bytes INTEGER NOT NULL,
    recursive_file_count INTEGER NOT NULL,
    recursive_bytes INTEGER NOT NULL,
    direct_small_file_count INTEGER NOT NULL,
    direct_small_file_bytes INTEGER NOT NULL,
    recursive_small_file_count INTEGER NOT NULL,
    recursive_small_file_bytes INTEGER NOT NULL,
    direct_large_file_count INTEGER NOT NULL,
    direct_large_file_bytes INTEGER NOT NULL,
    recursive_large_file_count INTEGER NOT NULL,
    recursive_large_file_bytes INTEGER NOT NULL,
    backup_date TIMESTAMPTZ TEXT NOT NULL,
    record_key BLOB NOT NULL,
    CONSTRAINT directory_tree_index_depth_check CHECK ((depth >= 0)),
    CONSTRAINT directory_tree_index_direct_bytes_check CHECK ((direct_bytes >= 0)),
    CONSTRAINT directory_tree_index_direct_file_count_check CHECK ((direct_file_count >= 0)),
    CONSTRAINT directory_tree_index_direct_large_file_bytes_check CHECK ((direct_large_file_bytes >= 0)),
    CONSTRAINT directory_tree_index_direct_large_file_count_check CHECK ((direct_large_file_count >= 0)),
    CONSTRAINT directory_tree_index_direct_small_file_bytes_check CHECK ((direct_small_file_bytes >= 0)),
    CONSTRAINT directory_tree_index_direct_small_file_count_check CHECK ((direct_small_file_count >= 0)),
    CONSTRAINT directory_tree_index_record_key_check CHECK ((typeof(record_key) = 'blob' AND length(record_key) = 32)),
    CONSTRAINT directory_tree_index_recursive_bytes_check CHECK ((recursive_bytes >= 0)),
    CONSTRAINT directory_tree_index_recursive_file_count_check CHECK ((recursive_file_count >= 0)),
    CONSTRAINT directory_tree_index_recursive_large_file_bytes_check CHECK ((recursive_large_file_bytes >= 0)),
    CONSTRAINT directory_tree_index_recursive_large_file_count_check CHECK ((recursive_large_file_count >= 0)),
    CONSTRAINT directory_tree_index_recursive_small_file_bytes_check CHECK ((recursive_small_file_bytes >= 0)),
    CONSTRAINT directory_tree_index_recursive_small_file_count_check CHECK ((recursive_small_file_count >= 0)),
    CONSTRAINT directory_tree_index_archive_run_id_fkey FOREIGN KEY (archive_run_id) REFERENCES archive_runs(run_id) ON DELETE SET NULL,
    CONSTRAINT directory_tree_index_bundle_id_fkey FOREIGN KEY (bundle_id) REFERENCES directory_archive_bundles(bundle_id) ON DELETE SET NULL,
    CONSTRAINT directory_tree_index_local_session_id_fkey FOREIGN KEY (local_session_id) REFERENCES local_sessions(session_id) ON DELETE SET NULL,
    CONSTRAINT directory_tree_index_remote_session_id_fkey FOREIGN KEY (remote_session_id) REFERENCES remote_sessions(session_id) ON DELETE SET NULL,
    CONSTRAINT directory_tree_index_tape_label_fkey FOREIGN KEY (tape_label) REFERENCES tapes(volume_label) ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT fk_dir_tree_bundle_tape FOREIGN KEY (bundle_id, tape_label) REFERENCES directory_archive_bundles(bundle_id, tape_label) ON UPDATE CASCADE,
    CONSTRAINT directory_tree_index_record_key_key UNIQUE (record_key)
);

CREATE TABLE files_index (
    file_id INTEGER PRIMARY KEY AUTOINCREMENT,
    original_path TEXT NOT NULL,
    file_size_bytes INTEGER NOT NULL,
    tape_label TEXT NOT NULL,
    source_host TEXT NOT NULL DEFAULT 'srv02',
    is_packed BOOLEAN INTEGER NOT NULL DEFAULT 0,
    stored_path TEXT NOT NULL,
    local_session_id INTEGER,
    local_chunk_index INTEGER,
    bundle_id INTEGER,
    record_key BLOB NOT NULL,
    archive_run_id INTEGER NOT NULL,
    directory_id INTEGER NOT NULL,
    catalog_name TEXT NOT NULL,
    catalog_backup_date TIMESTAMPTZ TEXT NOT NULL,
    remote_session_id INTEGER,
    remote_chunk_index INTEGER,
    CONSTRAINT ck_packed_has_bundle CHECK (((NOT is_packed) OR (bundle_id IS NOT NULL))),
    CONSTRAINT files_index_file_size_bytes_check CHECK ((file_size_bytes >= 0)),
    CONSTRAINT files_index_record_key_check CHECK ((typeof(record_key) = 'blob' AND length(record_key) = 32)),
    CONSTRAINT files_index_archive_run_id_fkey FOREIGN KEY (archive_run_id) REFERENCES archive_runs(run_id),
    CONSTRAINT files_index_bundle_id_fkey FOREIGN KEY (bundle_id) REFERENCES archive_bundles(bundle_id),
    CONSTRAINT files_index_directory_id_fkey FOREIGN KEY (directory_id) REFERENCES catalog_directories(directory_id),
    CONSTRAINT files_index_local_session_id_fkey FOREIGN KEY (local_session_id) REFERENCES local_sessions(session_id),
    CONSTRAINT files_index_tape_label_fkey FOREIGN KEY (tape_label) REFERENCES tapes(volume_label) ON UPDATE CASCADE,
    CONSTRAINT fk_files_bundle_tape FOREIGN KEY (bundle_id, tape_label) REFERENCES archive_bundles(bundle_id, tape_label) ON UPDATE CASCADE,
    CONSTRAINT fk_files_remote_session FOREIGN KEY (remote_session_id) REFERENCES remote_sessions(session_id) ON DELETE SET NULL,
    CONSTRAINT uq_files_record_key UNIQUE (record_key)
);

CREATE TABLE local_chunks_manifest (
    manifest_id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER NOT NULL,
    chunk_index INTEGER NOT NULL,
    top_level_dir TEXT NOT NULL,
    dir_size_bytes INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    tape_label TEXT,
    started_at TIMESTAMPTZ TEXT,
    completed_at TIMESTAMPTZ TEXT,
    updated_at TIMESTAMPTZ TEXT,
    CONSTRAINT local_chunks_manifest_chunk_index_check CHECK ((chunk_index >= 0)),
    CONSTRAINT local_chunks_manifest_dir_size_bytes_check CHECK ((dir_size_bytes >= 0)),
    CONSTRAINT local_chunks_manifest_status_check CHECK ((status IN ('pending', 'staged', 'backed_up'))),
    CONSTRAINT local_chunks_manifest_session_id_fkey FOREIGN KEY (session_id) REFERENCES local_sessions(session_id) ON DELETE CASCADE,
    CONSTRAINT local_chunks_manifest_tape_label_fkey FOREIGN KEY (tape_label) REFERENCES tapes(volume_label) ON UPDATE CASCADE ON DELETE RESTRICT
);

CREATE TABLE local_manifest_catalog_aggregates (
    export_id INTEGER NOT NULL,
    directory_id INTEGER NOT NULL,
    direct_file_count INTEGER NOT NULL,
    direct_bytes INTEGER NOT NULL,
    recursive_file_count INTEGER NOT NULL,
    recursive_bytes INTEGER NOT NULL,
    CONSTRAINT local_manifest_catalog_aggregates_direct_bytes_check CHECK ((direct_bytes >= 0)),
    CONSTRAINT local_manifest_catalog_aggregates_direct_file_count_check CHECK ((direct_file_count >= 0)),
    CONSTRAINT local_manifest_catalog_aggregates_recursive_bytes_check CHECK ((recursive_bytes >= 0)),
    CONSTRAINT local_manifest_catalog_aggregates_recursive_file_count_check CHECK ((recursive_file_count >= 0)),
    CONSTRAINT local_manifest_catalog_aggregates_directory_id_fkey FOREIGN KEY (directory_id) REFERENCES catalog_directories(directory_id) ON DELETE CASCADE,
    CONSTRAINT local_manifest_catalog_aggregates_export_id_fkey FOREIGN KEY (export_id) REFERENCES local_manifest_exports(export_id) ON DELETE RESTRICT,
    CONSTRAINT local_manifest_catalog_aggregates_pkey PRIMARY KEY (export_id, directory_id)
);

CREATE TABLE local_manifest_export_rows (
    export_id INTEGER NOT NULL,
    source_file_id INTEGER NOT NULL,
    source_record_key BLOB NOT NULL,
    tape_label TEXT NOT NULL,
    source_host TEXT NOT NULL,
    original_path TEXT NOT NULL,
    file_size_bytes INTEGER NOT NULL,
    archive_run_id INTEGER NOT NULL,
    local_session_id INTEGER,
    remote_session_id INTEGER,
    chunk_index INTEGER,
    bundle_id INTEGER,
    covered_by_directory_catalog BOOLEAN INTEGER NOT NULL DEFAULT 0,
    eligible BOOLEAN INTEGER NOT NULL,
    skip_reason TEXT,
    manifest_relpath TEXT,
    pruned_at TIMESTAMPTZ TEXT,
    CONSTRAINT local_manifest_export_rows_check CHECK (((eligible AND (skip_reason IS NULL)) OR ((NOT eligible) AND (skip_reason IS NOT NULL)))),
    CONSTRAINT local_manifest_export_rows_file_size_bytes_check CHECK ((file_size_bytes >= 0)),
    CONSTRAINT local_manifest_export_rows_source_record_key_check CHECK ((typeof(source_record_key) = 'blob' AND length(source_record_key) = 32)),
    CONSTRAINT local_manifest_export_rows_export_id_fkey FOREIGN KEY (export_id) REFERENCES local_manifest_exports(export_id) ON DELETE RESTRICT,
    CONSTRAINT local_manifest_export_rows_pkey PRIMARY KEY (export_id, source_file_id)
);

CREATE TABLE local_manifest_exports (
    export_id INTEGER PRIMARY KEY AUTOINCREMENT,
    threshold_bytes INTEGER NOT NULL,
    archive_root TEXT NOT NULL,
    hot_backup_path TEXT NOT NULL,
    status TEXT NOT NULL,
    candidate_rows INTEGER NOT NULL DEFAULT 0,
    candidate_bytes INTEGER NOT NULL DEFAULT 0,
    eligible_rows INTEGER NOT NULL DEFAULT 0,
    eligible_bytes INTEGER NOT NULL DEFAULT 0,
    skipped_rows INTEGER NOT NULL DEFAULT 0,
    segment_count INTEGER NOT NULL DEFAULT 0,
    validation_passed BOOLEAN INTEGER NOT NULL DEFAULT 0,
    validation_report JSONB TEXT,
    prune_report JSONB TEXT,
    started_at TIMESTAMPTZ TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')||'000+00:00'),
    exported_at TIMESTAMPTZ TEXT,
    validated_at TIMESTAMPTZ TEXT,
    pruned_at TIMESTAMPTZ TEXT,
    error_message TEXT,
    CONSTRAINT local_manifest_exports_status_check CHECK ((status IN ('snapshotting', 'exporting', 'exported', 'pruning', 'validated', 'validation_failed', 'pruned', 'failed'))),
    CONSTRAINT local_manifest_exports_threshold_bytes_check CHECK ((threshold_bytes > 0))
);

CREATE TABLE local_manifest_folder_aggregates (
    aggregate_id INTEGER PRIMARY KEY AUTOINCREMENT,
    export_id INTEGER NOT NULL,
    source_host TEXT NOT NULL,
    tape_label TEXT NOT NULL,
    original_dir_path TEXT NOT NULL,
    direct_file_count INTEGER NOT NULL,
    direct_bytes INTEGER NOT NULL,
    direct_uncovered_file_count INTEGER NOT NULL,
    direct_uncovered_bytes INTEGER NOT NULL,
    recursive_file_count INTEGER NOT NULL,
    recursive_bytes INTEGER NOT NULL,
    recursive_uncovered_file_count INTEGER NOT NULL,
    recursive_uncovered_bytes INTEGER NOT NULL,
    backup_date TIMESTAMPTZ TEXT NOT NULL,
    CONSTRAINT local_manifest_folder_aggreg_recursive_uncovered_file_cou_check CHECK ((recursive_uncovered_file_count >= 0)),
    CONSTRAINT local_manifest_folder_aggrega_direct_uncovered_file_count_check CHECK ((direct_uncovered_file_count >= 0)),
    CONSTRAINT local_manifest_folder_aggregate_recursive_uncovered_bytes_check CHECK ((recursive_uncovered_bytes >= 0)),
    CONSTRAINT local_manifest_folder_aggregates_direct_bytes_check CHECK ((direct_bytes >= 0)),
    CONSTRAINT local_manifest_folder_aggregates_direct_file_count_check CHECK ((direct_file_count >= 0)),
    CONSTRAINT local_manifest_folder_aggregates_direct_uncovered_bytes_check CHECK ((direct_uncovered_bytes >= 0)),
    CONSTRAINT local_manifest_folder_aggregates_recursive_bytes_check CHECK ((recursive_bytes >= 0)),
    CONSTRAINT local_manifest_folder_aggregates_recursive_file_count_check CHECK ((recursive_file_count >= 0)),
    CONSTRAINT local_manifest_folder_aggregates_export_id_fkey FOREIGN KEY (export_id) REFERENCES local_manifest_exports(export_id) ON DELETE RESTRICT,
    CONSTRAINT local_manifest_folder_aggregates_tape_label_fkey FOREIGN KEY (tape_label) REFERENCES tapes(volume_label) ON UPDATE CASCADE ON DELETE RESTRICT,
    CONSTRAINT local_manifest_folder_aggrega_export_id_source_host_tape_la_key UNIQUE (export_id, source_host, tape_label, original_dir_path)
);

CREATE TABLE local_manifest_segments (
    export_id INTEGER NOT NULL,
    manifest_relpath TEXT NOT NULL,
    row_count INTEGER NOT NULL,
    original_bytes INTEGER NOT NULL,
    covered_rows INTEGER NOT NULL,
    covered_bytes INTEGER NOT NULL,
    uncovered_rows INTEGER NOT NULL,
    uncovered_bytes INTEGER NOT NULL,
    compressed_bytes INTEGER NOT NULL,
    sha256_hex TEXT NOT NULL,
    created_at TIMESTAMPTZ TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')||'000+00:00'),
    CONSTRAINT local_manifest_segments_compressed_bytes_check CHECK ((compressed_bytes >= 0)),
    CONSTRAINT local_manifest_segments_covered_bytes_check CHECK ((covered_bytes >= 0)),
    CONSTRAINT local_manifest_segments_covered_rows_check CHECK ((covered_rows >= 0)),
    CONSTRAINT local_manifest_segments_original_bytes_check CHECK ((original_bytes >= 0)),
    CONSTRAINT local_manifest_segments_row_count_check CHECK ((row_count >= 0)),
    CONSTRAINT local_manifest_segments_sha256_hex_check CHECK ((length(sha256_hex) = 64)),
    CONSTRAINT local_manifest_segments_uncovered_bytes_check CHECK ((uncovered_bytes >= 0)),
    CONSTRAINT local_manifest_segments_uncovered_rows_check CHECK ((uncovered_rows >= 0)),
    CONSTRAINT local_manifest_segments_export_id_fkey FOREIGN KEY (export_id) REFERENCES local_manifest_exports(export_id) ON DELETE RESTRICT,
    CONSTRAINT local_manifest_segments_pkey PRIMARY KEY (export_id, manifest_relpath)
);

CREATE TABLE local_sessions (
    session_id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_label TEXT NOT NULL,
    source_dir TEXT NOT NULL,
    total_chunks INTEGER NOT NULL,
    backup_mode TEXT NOT NULL DEFAULT 'auto',
    created_at TIMESTAMPTZ TEXT NOT NULL,
    completed_at TIMESTAMPTZ TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    CONSTRAINT local_sessions_backup_mode_check CHECK ((backup_mode IN ('auto', 'direct', 'pack'))),
    CONSTRAINT local_sessions_status_check CHECK ((status IN ('active', 'completed', 'abandoned'))),
    CONSTRAINT local_sessions_total_chunks_check CHECK ((total_chunks >= 0)),
    CONSTRAINT uq_local_sessions_label UNIQUE (session_label)
);

CREATE TABLE manifest_directory_catalog_schema_metadata (
    singleton BOOLEAN INTEGER NOT NULL DEFAULT 1,
    schema_version INTEGER NOT NULL,
    migration_checksum TEXT NOT NULL,
    schema_fingerprint TEXT NOT NULL,
    applied_at TIMESTAMPTZ TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')||'000+00:00'),
    CONSTRAINT manifest_directory_catalog_schema_meta_migration_checksum_check CHECK ((length(migration_checksum) = 64)),
    CONSTRAINT manifest_directory_catalog_schema_meta_schema_fingerprint_check CHECK ((length(schema_fingerprint) = 32)),
    CONSTRAINT manifest_directory_catalog_schema_metadata_singleton_check CHECK (singleton),
    CONSTRAINT manifest_directory_catalog_schema_metadata_pkey PRIMARY KEY (singleton)
);

CREATE TABLE remote_chunk_plan_source_transitions (
    chunk_plan_transition_id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER NOT NULL,
    chunk_index INTEGER NOT NULL,
    from_plan_source TEXT NOT NULL,
    to_plan_source TEXT NOT NULL,
    prior_plan_manifest_artifact_id INTEGER,
    new_plan_manifest_artifact_id INTEGER NOT NULL,
    plan4_gate_id TEXT NOT NULL,
    plan4_evidence_id TEXT NOT NULL,
    approval_identity TEXT NOT NULL,
    equivalence_confirmed BOOLEAN INTEGER NOT NULL,
    transitioned_at TIMESTAMPTZ TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')||'000+00:00'),
    CONSTRAINT remote_chunk_plan_source_transition_direction_ck CHECK (((from_plan_source = 'legacy_db') AND (to_plan_source = 'manifest'))),
    CONSTRAINT remote_chunk_plan_source_transition_equivalence_confirmed_check CHECK (equivalence_confirmed),
    CONSTRAINT remote_chunk_plan_source_transitions_approval_identity_check CHECK ((length(trim(approval_identity)) > 0)),
    CONSTRAINT remote_chunk_plan_source_transitions_chunk_index_check CHECK ((chunk_index >= 0)),
    CONSTRAINT remote_chunk_plan_source_transitions_plan4_evidence_id_check CHECK ((length(trim(plan4_evidence_id)) > 0)),
    CONSTRAINT remote_chunk_plan_source_transitions_plan4_gate_id_check CHECK ((length(trim(plan4_gate_id)) > 0)),
    CONSTRAINT remote_chunk_plan_source_tran_new_plan_manifest_artifact_i_fkey FOREIGN KEY (new_plan_manifest_artifact_id) REFERENCES archive_artifacts(artifact_id) ON DELETE RESTRICT,
    CONSTRAINT remote_chunk_plan_source_tran_prior_plan_manifest_artifact_fkey FOREIGN KEY (prior_plan_manifest_artifact_id) REFERENCES archive_artifacts(artifact_id) ON DELETE RESTRICT,
    CONSTRAINT remote_chunk_plan_source_transition_chunk_fk FOREIGN KEY (session_id, chunk_index) REFERENCES remote_chunks(session_id, chunk_index) ON DELETE RESTRICT
);

CREATE TABLE remote_chunk_scan_segments (
    chunk_segment_id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER NOT NULL,
    chunk_index INTEGER NOT NULL,
    scan_segment_id INTEGER NOT NULL,
    first_scan_ordinal INTEGER NOT NULL,
    last_scan_ordinal INTEGER NOT NULL,
    created_at TIMESTAMPTZ TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')||'000+00:00'),
    CONSTRAINT remote_chunk_scan_segments_check CHECK ((last_scan_ordinal >= first_scan_ordinal)),
    CONSTRAINT remote_chunk_scan_segments_chunk_index_check CHECK ((chunk_index >= 0)),
    CONSTRAINT remote_chunk_scan_segments_first_scan_ordinal_check CHECK ((first_scan_ordinal >= 0)),
    CONSTRAINT remote_chunk_scan_segments_last_scan_ordinal_check CHECK ((last_scan_ordinal >= 0)),
    CONSTRAINT remote_chunk_scan_segments_scan_segment_id_fkey FOREIGN KEY (scan_segment_id) REFERENCES remote_scan_segments(scan_segment_id) ON DELETE RESTRICT,
    CONSTRAINT remote_chunk_scan_segments_session_id_chunk_index_fkey FOREIGN KEY (session_id, chunk_index) REFERENCES remote_chunks(session_id, chunk_index) ON DELETE CASCADE,
    CONSTRAINT remote_chunk_scan_segments_scan_segment_id_first_scan_ordin_key UNIQUE (scan_segment_id, first_scan_ordinal, last_scan_ordinal)
);

CREATE TABLE remote_chunk_transition_overrides (
    override_id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER NOT NULL,
    chunk_index INTEGER NOT NULL,
    from_status TEXT NOT NULL,
    to_status TEXT NOT NULL,
    override_name TEXT NOT NULL,
    reason TEXT NOT NULL,
    authorized_by TEXT NOT NULL,
    authorized_at TIMESTAMPTZ TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')||'000+00:00'),
    consumed_at TIMESTAMPTZ TEXT,
    CONSTRAINT remote_chunk_transition_overrides_authorized_by_check CHECK ((length(trim(authorized_by)) > 0)),
    CONSTRAINT remote_chunk_transition_overrides_chunk_index_check CHECK ((chunk_index >= 0)),
    CONSTRAINT remote_chunk_transition_overrides_distinct_ck CHECK ((from_status <> to_status)),
    CONSTRAINT remote_chunk_transition_overrides_from_status_check CHECK ((from_status IN ('backing', 'done'))),
    CONSTRAINT remote_chunk_transition_overrides_override_name_check CHECK ((length(trim(override_name)) > 0)),
    CONSTRAINT remote_chunk_transition_overrides_reason_check CHECK ((length(trim(reason)) > 0)),
    CONSTRAINT remote_chunk_transition_overrides_to_status_check CHECK ((to_status IN ('pending', 'fetching', 'packing', 'backing', 'done', 'fetch_failed', 'backup_failed')))
);

CREATE TABLE remote_chunks (
    session_id INTEGER NOT NULL,
    chunk_index INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    error_msg TEXT,
    updated_at TIMESTAMPTZ TEXT,
    owner_token TEXT,
    lease_expires_at TIMESTAMPTZ TEXT,
    attempt_id TEXT,
    membership_state TEXT,
    expected_file_count INTEGER,
    expected_bytes INTEGER,
    packaging_format TEXT NOT NULL,
    packaging_assigned_at TIMESTAMPTZ TEXT NOT NULL,
    writer_started_at TIMESTAMPTZ TEXT,
    writer_completed_at TIMESTAMPTZ TEXT,
    catalog_committed_at TIMESTAMPTZ TEXT,
    stored_tar_max_size_bytes INTEGER,
    plan_source TEXT NOT NULL,
    plan_manifest_artifact_id INTEGER,
    terminal_manifest_artifact_id INTEGER,
    plan_ordinal_scope TEXT NOT NULL DEFAULT 'chunk',
    final_archived_count INTEGER,
    final_source_missing_count INTEGER,
    final_source_permission_denied_count INTEGER,
    final_source_unreadable_count INTEGER,
    final_source_changed_count INTEGER,
    final_unresolved_count INTEGER,
    final_archived_bytes INTEGER,
    CONSTRAINT remote_chunks_chunk_index_check CHECK ((chunk_index >= 0)),
    CONSTRAINT remote_chunks_final_disposition_complete_ck CHECK (((((final_archived_count IS NOT NULL) + (final_source_missing_count IS NOT NULL) + (final_source_permission_denied_count IS NOT NULL) + (final_source_unreadable_count IS NOT NULL) + (final_source_changed_count IS NOT NULL) + (final_unresolved_count IS NOT NULL) + (final_archived_bytes IS NOT NULL)) IN (0, 7)) AND ((final_archived_count IS NULL) OR (expected_file_count IS NULL) OR ((((((final_archived_count + final_source_missing_count) + final_source_permission_denied_count) + final_source_unreadable_count) + final_source_changed_count) + final_unresolved_count) = expected_file_count)))),
    CONSTRAINT remote_chunks_final_disposition_nonnegative_ck CHECK ((((final_archived_count IS NULL) OR (final_archived_count >= 0)) AND ((final_source_missing_count IS NULL) OR (final_source_missing_count >= 0)) AND ((final_source_permission_denied_count IS NULL) OR (final_source_permission_denied_count >= 0)) AND ((final_source_unreadable_count IS NULL) OR (final_source_unreadable_count >= 0)) AND ((final_source_changed_count IS NULL) OR (final_source_changed_count >= 0)) AND ((final_unresolved_count IS NULL) OR (final_unresolved_count >= 0)) AND ((final_archived_bytes IS NULL) OR (final_archived_bytes >= 0)))),
    CONSTRAINT remote_chunks_manifest_authority_ck CHECK ((((plan_source = 'legacy_db') AND (plan_manifest_artifact_id IS NULL)) OR ((plan_source = 'manifest') AND (plan_manifest_artifact_id IS NOT NULL)))),
    CONSTRAINT remote_chunks_membership_state_check CHECK (((membership_state IS NULL) OR (membership_state IN ('building', 'sealed')))),
    CONSTRAINT remote_chunks_packaging_format_ck CHECK ((packaging_format IN ('zip', 'stored_tar'))),
    CONSTRAINT remote_chunks_plan_ordinal_scope_ck CHECK ((plan_ordinal_scope = 'chunk')),
    CONSTRAINT remote_chunks_plan_source_ck CHECK ((plan_source IN ('legacy_db', 'manifest'))),
    CONSTRAINT remote_chunks_sealed_expectations_check CHECK (((membership_state IS NOT 'sealed') OR ((expected_file_count IS NOT NULL) AND (expected_bytes IS NOT NULL)))),
    CONSTRAINT remote_chunks_status_check CHECK ((status IN ('pending', 'fetching', 'packing', 'backing', 'done', 'fetch_failed', 'backup_failed'))),
    CONSTRAINT remote_chunks_stored_tar_max_size_bytes_check CHECK (((stored_tar_max_size_bytes IS NULL) OR (stored_tar_max_size_bytes > 0))),
    CONSTRAINT remote_chunks_plan_manifest_artifact_fk FOREIGN KEY (plan_manifest_artifact_id) REFERENCES archive_artifacts(artifact_id) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED,
    CONSTRAINT remote_chunks_session_id_fkey FOREIGN KEY (session_id) REFERENCES remote_sessions(session_id) ON DELETE CASCADE,
    CONSTRAINT remote_chunks_terminal_manifest_artifact_fk FOREIGN KEY (terminal_manifest_artifact_id) REFERENCES archive_artifacts(artifact_id) ON DELETE RESTRICT,
    CONSTRAINT remote_chunks_pkey PRIMARY KEY (session_id, chunk_index),
    CONSTRAINT remote_chunks_identity_format_uq UNIQUE (session_id, chunk_index, packaging_format)
);

CREATE TABLE remote_file_state (
    session_id INTEGER NOT NULL,
    plan_file_id INTEGER NOT NULL,
    status TEXT,
    local_rel_path TEXT,
    error_msg TEXT,
    updated_at TIMESTAMPTZ TEXT,
    CONSTRAINT remote_file_state_status_check CHECK (((status IS NULL) OR (status IN ('pending', 'fetching', 'fetched', 'fetch_failed', 'source_missing', 'source_permission_denied', 'source_unreadable', 'source_changed', 'unresolved')))),
    CONSTRAINT remote_file_state_plan_file_id_fkey FOREIGN KEY (plan_file_id) REFERENCES remote_plan_files(plan_file_id),
    CONSTRAINT remote_file_state_session_id_fkey FOREIGN KEY (session_id) REFERENCES remote_sessions(session_id) ON DELETE CASCADE,
    CONSTRAINT remote_file_state_pkey PRIMARY KEY (session_id, plan_file_id)
);

CREATE TABLE remote_frontier_bootstraps (
    bootstrap_id TEXT NOT NULL,
    session_id INTEGER NOT NULL,
    source_host TEXT,
    state TEXT NOT NULL DEFAULT 'planned',
    scopes_created INTEGER NOT NULL DEFAULT 0,
    directories_listed INTEGER NOT NULL DEFAULT 0,
    segments_published INTEGER NOT NULL DEFAULT 0,
    segments_imported INTEGER NOT NULL DEFAULT 0,
    entries_covered INTEGER NOT NULL DEFAULT 0,
    entries_new INTEGER NOT NULL DEFAULT 0,
    entries_changed INTEGER NOT NULL DEFAULT 0,
    coverage_final BOOLEAN INTEGER NOT NULL DEFAULT 0,
    detail TEXT,
    created_at TIMESTAMPTZ TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')||'000+00:00'),
    updated_at TIMESTAMPTZ TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')||'000+00:00'),
    CONSTRAINT remote_frontier_bootstraps_state_check CHECK ((state IN ('planned', 'running', 'completed', 'failed', 'abandoned'))),
    CONSTRAINT remote_frontier_bootstraps_session_id_fkey FOREIGN KEY (session_id) REFERENCES remote_sessions(session_id) ON DELETE CASCADE,
    CONSTRAINT remote_frontier_bootstraps_pkey PRIMARY KEY (bootstrap_id)
);

CREATE TABLE remote_packaging_boundaries (
    session_id INTEGER NOT NULL,
    first_stored_tar_chunk_index INTEGER NOT NULL,
    last_existing_chunk_index INTEGER NOT NULL,
    approval_id TEXT NOT NULL,
    approval_reason TEXT NOT NULL,
    database_evidence JSONB TEXT NOT NULL,
    local_staging_evidence JSONB TEXT NOT NULL,
    evidence_checked_at TIMESTAMPTZ TEXT NOT NULL,
    created_at TIMESTAMPTZ TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')||'000+00:00'),
    CONSTRAINT remote_packaging_boundaries_approval_id_check CHECK ((length(approval_id) > 0)),
    CONSTRAINT remote_packaging_boundaries_approval_reason_check CHECK ((length(approval_reason) > 0)),
    CONSTRAINT remote_packaging_boundaries_check CHECK ((last_existing_chunk_index >= first_stored_tar_chunk_index)),
    CONSTRAINT remote_packaging_boundaries_first_stored_tar_chunk_index_check CHECK ((first_stored_tar_chunk_index >= 0)),
    CONSTRAINT remote_packaging_boundaries_session_id_fkey FOREIGN KEY (session_id) REFERENCES remote_sessions(session_id) ON DELETE RESTRICT,
    CONSTRAINT remote_packaging_boundaries_pkey PRIMARY KEY (session_id)
);

CREATE TABLE remote_packaging_boundary_chunks (
    session_id INTEGER NOT NULL,
    chunk_index INTEGER NOT NULL,
    classification TEXT NOT NULL,
    assigned_format TEXT NOT NULL,
    prefix_evidence_basis TEXT,
    plan_member_count INTEGER NOT NULL,
    plan_logical_bytes INTEGER NOT NULL,
    first_ordinal INTEGER,
    last_ordinal INTEGER,
    evidence JSONB TEXT NOT NULL,
    CONSTRAINT remote_packaging_boundary_chunks_assigned_format_check CHECK ((assigned_format IN ('zip', 'stored_tar'))),
    CONSTRAINT remote_packaging_boundary_chunks_check CHECK ((((classification = 'immutable_zip') AND (assigned_format = 'zip') AND (prefix_evidence_basis IS NOT NULL)) OR ((classification = 'approved_stored_tar_exception') AND (assigned_format = 'stored_tar') AND (prefix_evidence_basis IS NULL)))),
    CONSTRAINT remote_packaging_boundary_chunks_chunk_index_check CHECK ((chunk_index >= 0)),
    CONSTRAINT remote_packaging_boundary_chunks_classification_check CHECK ((classification IN ('immutable_zip', 'approved_stored_tar_exception'))),
    CONSTRAINT remote_packaging_boundary_chunks_plan_logical_bytes_check CHECK ((plan_logical_bytes >= 0)),
    CONSTRAINT remote_packaging_boundary_chunks_plan_member_count_check CHECK ((plan_member_count >= 0)),
    CONSTRAINT remote_packaging_boundary_chunks_prefix_evidence_basis_check CHECK ((prefix_evidence_basis IN ('corroborated', 'status_only'))),
    CONSTRAINT remote_packaging_boundary_chunks_session_id_chunk_index_fkey FOREIGN KEY (session_id, chunk_index) REFERENCES remote_chunks(session_id, chunk_index) ON DELETE RESTRICT,
    CONSTRAINT remote_packaging_boundary_chunks_session_id_fkey FOREIGN KEY (session_id) REFERENCES remote_packaging_boundaries(session_id) ON DELETE RESTRICT,
    CONSTRAINT remote_packaging_boundary_chunks_pkey PRIMARY KEY (session_id, chunk_index)
);

CREATE TABLE remote_plan_files (
    plan_file_id INTEGER PRIMARY KEY AUTOINCREMENT,
    plan_id INTEGER NOT NULL,
    snapshot_file_id INTEGER NOT NULL,
    chunk_index INTEGER NOT NULL,
    ordinal INTEGER NOT NULL,
    CONSTRAINT remote_plan_files_chunk_index_check CHECK ((chunk_index >= 0)),
    CONSTRAINT remote_plan_files_ordinal_check CHECK ((ordinal >= 0)),
    CONSTRAINT remote_plan_files_plan_id_fkey FOREIGN KEY (plan_id) REFERENCES remote_plans(plan_id) ON DELETE CASCADE,
    CONSTRAINT remote_plan_files_snapshot_file_id_fkey FOREIGN KEY (snapshot_file_id) REFERENCES remote_snapshot_files(snapshot_file_id),
    CONSTRAINT remote_plan_files_plan_id_snapshot_file_id_key UNIQUE (plan_id, snapshot_file_id)
);

CREATE TABLE remote_plans (
    plan_id INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_id INTEGER NOT NULL,
    fingerprint BLOB NOT NULL,
    chunk_count INTEGER NOT NULL,
    created_at TIMESTAMPTZ TEXT NOT NULL,
    CONSTRAINT remote_plans_chunk_count_check CHECK ((chunk_count >= 0)),
    CONSTRAINT remote_plans_snapshot_id_fkey FOREIGN KEY (snapshot_id) REFERENCES remote_snapshots(snapshot_id) ON DELETE CASCADE,
    CONSTRAINT remote_plans_fingerprint_key UNIQUE (fingerprint)
);

CREATE TABLE remote_scan_directories (
    scan_directory_id INTEGER PRIMARY KEY AUTOINCREMENT,
    scan_scope_id INTEGER NOT NULL,
    canonical_path TEXT NOT NULL,
    parent_path TEXT,
    parent_directory_id INTEGER,
    traversal_ordinal INTEGER NOT NULL,
    listing_state TEXT NOT NULL DEFAULT 'pending',
    subtree_coverage_state TEXT NOT NULL DEFAULT 'provisional',
    planning_state TEXT NOT NULL DEFAULT 'unplanned',
    last_committed_segment_id INTEGER,
    observation_before TEXT,
    observation_after TEXT,
    observation_final TEXT,
    direct_file_count INTEGER NOT NULL DEFAULT 0,
    direct_byte_count INTEGER NOT NULL DEFAULT 0,
    child_directory_count INTEGER NOT NULL DEFAULT 0,
    error_count INTEGER NOT NULL DEFAULT 0,
    owner_token TEXT,
    attempt_id TEXT,
    lease_expires_at TIMESTAMPTZ TEXT,
    created_at TIMESTAMPTZ TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')||'000+00:00'),
    updated_at TIMESTAMPTZ TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')||'000+00:00'),
    CONSTRAINT remote_scan_directories_child_directory_count_check CHECK ((child_directory_count >= 0)),
    CONSTRAINT remote_scan_directories_direct_byte_count_check CHECK ((direct_byte_count >= 0)),
    CONSTRAINT remote_scan_directories_direct_file_count_check CHECK ((direct_file_count >= 0)),
    CONSTRAINT remote_scan_directories_error_count_check CHECK ((error_count >= 0)),
    CONSTRAINT remote_scan_directories_listing_state_check CHECK ((listing_state IN ('pending', 'scanning', 'partial', 'complete', 'error', 'invalidated'))),
    CONSTRAINT remote_scan_directories_planning_state_check CHECK ((planning_state IN ('unplanned', 'partially_allocated', 'fully_allocated', 'blocked'))),
    CONSTRAINT remote_scan_directories_subtree_coverage_state_check CHECK ((subtree_coverage_state IN ('provisional', 'final', 'error', 'invalidated'))),
    CONSTRAINT remote_scan_directories_parent_directory_id_fkey FOREIGN KEY (parent_directory_id) REFERENCES remote_scan_directories(scan_directory_id) ON DELETE CASCADE,
    CONSTRAINT remote_scan_directories_scan_scope_id_fkey FOREIGN KEY (scan_scope_id) REFERENCES remote_scan_scopes(scan_scope_id) ON DELETE CASCADE,
    CONSTRAINT remote_scan_directories_scan_scope_id_canonical_path_key UNIQUE (scan_scope_id, canonical_path),
    CONSTRAINT remote_scan_directories_scan_scope_id_traversal_ordinal_key UNIQUE (scan_scope_id, traversal_ordinal)
);

CREATE TABLE remote_scan_errors (
    scan_error_id INTEGER PRIMARY KEY AUTOINCREMENT,
    scan_scope_id INTEGER,
    scan_directory_id INTEGER,
    category TEXT NOT NULL,
    path TEXT,
    message TEXT,
    disposition TEXT NOT NULL DEFAULT 'unresolved',
    created_at TIMESTAMPTZ TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')||'000+00:00'),
    CONSTRAINT remote_scan_errors_disposition_check CHECK ((disposition IN ('unresolved', 'skipped', 'retried', 'accepted'))),
    CONSTRAINT remote_scan_errors_scan_directory_id_fkey FOREIGN KEY (scan_directory_id) REFERENCES remote_scan_directories(scan_directory_id) ON DELETE CASCADE,
    CONSTRAINT remote_scan_errors_scan_scope_id_fkey FOREIGN KEY (scan_scope_id) REFERENCES remote_scan_scopes(scan_scope_id) ON DELETE CASCADE
);

CREATE TABLE remote_scan_scopes (
    scan_scope_id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER NOT NULL,
    scope_ordinal INTEGER NOT NULL,
    scope_kind TEXT NOT NULL,
    source_root TEXT NOT NULL,
    coverage_state TEXT NOT NULL DEFAULT 'provisional',
    planning_complete BOOLEAN INTEGER NOT NULL DEFAULT 0,
    observation_final TEXT,
    created_at TIMESTAMPTZ TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')||'000+00:00'),
    updated_at TIMESTAMPTZ TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')||'000+00:00'),
    CONSTRAINT remote_scan_scopes_coverage_state_check CHECK ((coverage_state IN ('provisional', 'final', 'error', 'invalidated'))),
    CONSTRAINT remote_scan_scopes_scope_kind_check CHECK ((scope_kind IN ('directory', 'file'))),
    CONSTRAINT remote_scan_scopes_scope_ordinal_check CHECK ((scope_ordinal >= 0)),
    CONSTRAINT remote_scan_scopes_session_id_fkey FOREIGN KEY (session_id) REFERENCES remote_sessions(session_id) ON DELETE CASCADE,
    CONSTRAINT remote_scan_scopes_session_id_scope_ordinal_key UNIQUE (session_id, scope_ordinal),
    CONSTRAINT remote_scan_scopes_session_id_source_root_key UNIQUE (session_id, source_root)
);

CREATE TABLE remote_scan_segments (
    scan_segment_id INTEGER PRIMARY KEY AUTOINCREMENT,
    scan_directory_id INTEGER NOT NULL,
    first_scan_ordinal INTEGER NOT NULL,
    last_scan_ordinal INTEGER NOT NULL,
    next_unconsumed_ordinal INTEGER,
    locator TEXT NOT NULL,
    artifact_version TEXT NOT NULL DEFAULT 'scan-segment-v1',
    artifact_size_bytes INTEGER,
    file_count INTEGER NOT NULL DEFAULT 0,
    byte_count INTEGER NOT NULL DEFAULT 0,
    first_canonical_path TEXT,
    last_canonical_path TEXT,
    state TEXT NOT NULL DEFAULT 'writing',
    legacy_import_state TEXT NOT NULL DEFAULT 'not_imported',
    legacy_imported_at TIMESTAMPTZ TEXT,
    created_at TIMESTAMPTZ TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')||'000+00:00'),
    updated_at TIMESTAMPTZ TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')||'000+00:00'),
    CONSTRAINT remote_scan_segments_artifact_size_bytes_check CHECK ((artifact_size_bytes >= 0)),
    CONSTRAINT remote_scan_segments_byte_count_check CHECK ((byte_count >= 0)),
    CONSTRAINT remote_scan_segments_check CHECK ((last_scan_ordinal >= first_scan_ordinal)),
    CONSTRAINT remote_scan_segments_file_count_check CHECK ((file_count >= 0)),
    CONSTRAINT remote_scan_segments_first_scan_ordinal_check CHECK ((first_scan_ordinal >= 0)),
    CONSTRAINT remote_scan_segments_last_scan_ordinal_check CHECK ((last_scan_ordinal >= 0)),
    CONSTRAINT remote_scan_segments_legacy_import_state_check CHECK ((legacy_import_state IN ('not_imported', 'imported', 'blocked'))),
    CONSTRAINT remote_scan_segments_locator_check CHECK ((locator NOT LIKE '%.part')),
    CONSTRAINT remote_scan_segments_ready_cursor_check CHECK (((state IN ('writing', 'invalidated')) OR (next_unconsumed_ordinal IS NOT NULL))),
    CONSTRAINT remote_scan_segments_state_check CHECK ((state IN ('writing', 'ready', 'partially_consumed', 'consumed', 'invalidated'))),
    CONSTRAINT remote_scan_segments_scan_directory_id_fkey FOREIGN KEY (scan_directory_id) REFERENCES remote_scan_directories(scan_directory_id) ON DELETE CASCADE,
    CONSTRAINT remote_scan_segments_scan_directory_id_first_scan_ordinal_key UNIQUE (scan_directory_id, first_scan_ordinal)
);

CREATE TABLE remote_session_plan_transitions (
    transition_id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER NOT NULL,
    transition_epoch INTEGER NOT NULL,
    state TEXT NOT NULL DEFAULT 'draft',
    prior_plan_source TEXT NOT NULL,
    new_plan_source TEXT NOT NULL,
    last_chunk_before_transition INTEGER,
    first_chunk_after_transition INTEGER NOT NULL,
    scan_frontier_generation INTEGER,
    evidence_report_locator TEXT,
    approval_identity TEXT,
    approved_at TIMESTAMPTZ TEXT,
    created_at TIMESTAMPTZ TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')||'000+00:00'),
    updated_at TIMESTAMPTZ TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')||'000+00:00'),
    CONSTRAINT remote_session_plan_transiti_first_chunk_after_transition_check CHECK ((first_chunk_after_transition >= 0)),
    CONSTRAINT remote_session_plan_transiti_last_chunk_before_transition_check CHECK (((last_chunk_before_transition IS NULL) OR (last_chunk_before_transition >= 0))),
    CONSTRAINT remote_session_plan_transitions_approval_ck CHECK ((((state IN ('draft', 'rehearsed')) AND (approval_identity IS NULL) AND (approved_at IS NULL)) OR ((state IN ('approved', 'active', 'rolled_back')) AND (approval_identity IS NOT NULL) AND (length(trim(approval_identity)) > 0) AND (approved_at IS NOT NULL)))),
    CONSTRAINT remote_session_plan_transitions_boundary_ck CHECK ((((last_chunk_before_transition IS NULL) AND (first_chunk_after_transition = 0)) OR (first_chunk_after_transition = (last_chunk_before_transition + 1)))),
    CONSTRAINT remote_session_plan_transitions_evidence_ck CHECK (((state = 'draft') OR ((scan_frontier_generation IS NOT NULL) AND (evidence_report_locator IS NOT NULL) AND (length(trim(evidence_report_locator)) > 0) AND (lower(substr(evidence_report_locator, -5)) <> '.part')))),
    CONSTRAINT remote_session_plan_transitions_new_plan_source_check CHECK ((new_plan_source IN ('legacy_db', 'manifest'))),
    CONSTRAINT remote_session_plan_transitions_prior_plan_source_check CHECK ((prior_plan_source IN ('legacy_db', 'manifest'))),
    CONSTRAINT remote_session_plan_transitions_scan_frontier_generation_check CHECK (((scan_frontier_generation IS NULL) OR (scan_frontier_generation >= 0))),
    CONSTRAINT remote_session_plan_transitions_sources_ck CHECK ((prior_plan_source <> new_plan_source)),
    CONSTRAINT remote_session_plan_transitions_state_check CHECK ((state IN ('draft', 'rehearsed', 'approved', 'active', 'rolled_back'))),
    CONSTRAINT remote_session_plan_transitions_transition_epoch_check CHECK ((transition_epoch >= 1)),
    CONSTRAINT remote_session_plan_transitions_session_id_fkey FOREIGN KEY (session_id) REFERENCES remote_sessions(session_id) ON DELETE RESTRICT,
    CONSTRAINT remote_session_plan_transitions_epoch_uq UNIQUE (session_id, transition_epoch)
);

CREATE TABLE remote_sessions (
    session_id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_label TEXT NOT NULL,
    remote_host TEXT NOT NULL,
    remote_user TEXT NOT NULL,
    remote_path TEXT NOT NULL,
    tape_label TEXT NOT NULL,
    staging_dir TEXT NOT NULL,
    total_files INTEGER DEFAULT 0,
    total_bytes INTEGER DEFAULT 0,
    chunk_count INTEGER DEFAULT 0,
    plan_id INTEGER,
    scan_complete BOOLEAN INTEGER NOT NULL DEFAULT 1,
    scan_error TEXT,
    created_at TIMESTAMPTZ TEXT NOT NULL,
    completed_at TIMESTAMPTZ TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    tape_generation INTEGER NOT NULL,
    default_packaging_format TEXT NOT NULL DEFAULT 'zip',
    CONSTRAINT remote_sessions_chunk_count_check CHECK ((chunk_count >= 0)),
    CONSTRAINT remote_sessions_default_packaging_format_ck CHECK ((default_packaging_format IN ('zip', 'stored_tar'))),
    CONSTRAINT remote_sessions_status_check CHECK ((status IN ('active', 'completed', 'abandoned'))),
    CONSTRAINT remote_sessions_total_bytes_check CHECK ((total_bytes >= 0)),
    CONSTRAINT remote_sessions_total_files_check CHECK ((total_files >= 0)),
    CONSTRAINT remote_sessions_plan_id_fkey FOREIGN KEY (plan_id) REFERENCES remote_plans(plan_id),
    CONSTRAINT remote_sessions_tape_label_fkey FOREIGN KEY (tape_label) REFERENCES tapes(volume_label) ON UPDATE CASCADE ON DELETE RESTRICT,
    CONSTRAINT uq_remote_sessions_label UNIQUE (session_label)
);

CREATE TABLE remote_snapshot_files (
    snapshot_file_id INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_id INTEGER NOT NULL,
    remote_path TEXT NOT NULL,
    file_size_bytes INTEGER NOT NULL,
    CONSTRAINT remote_snapshot_files_file_size_bytes_check CHECK ((file_size_bytes >= 0)),
    CONSTRAINT remote_snapshot_files_snapshot_id_fkey FOREIGN KEY (snapshot_id) REFERENCES remote_snapshots(snapshot_id) ON DELETE CASCADE,
    CONSTRAINT remote_snapshot_files_snapshot_id_remote_path_key UNIQUE (snapshot_id, remote_path)
);

CREATE TABLE remote_snapshots (
    snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
    remote_host TEXT NOT NULL,
    remote_path TEXT NOT NULL,
    fingerprint BLOB NOT NULL,
    total_files INTEGER NOT NULL,
    total_bytes INTEGER NOT NULL,
    created_at TIMESTAMPTZ TEXT NOT NULL,
    CONSTRAINT remote_snapshots_total_bytes_check CHECK ((total_bytes >= 0)),
    CONSTRAINT remote_snapshots_total_files_check CHECK ((total_files >= 0)),
    CONSTRAINT remote_snapshots_fingerprint_key UNIQUE (fingerprint)
);

CREATE TABLE remote_worker_attempts (
    attempt_id TEXT NOT NULL,
    owner_token TEXT NOT NULL,
    session_id INTEGER,
    chunk_index INTEGER,
    scan_directory_id INTEGER,
    attempt_kind TEXT NOT NULL,
    local_pid INTEGER,
    local_process_started_at TIMESTAMPTZ TEXT,
    remote_command_token TEXT,
    remote_process_group TEXT,
    lease_expires_at TIMESTAMPTZ TEXT,
    heartbeat_at TIMESTAMPTZ TEXT,
    terminal_state TEXT,
    created_at TIMESTAMPTZ TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')||'000+00:00'),
    updated_at TIMESTAMPTZ TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')||'000+00:00'),
    CONSTRAINT remote_worker_attempts_attempt_kind_check CHECK ((attempt_kind IN ('scan', 'fetch', 'pack', 'write'))),
    CONSTRAINT remote_worker_attempts_chunk_index_check CHECK ((chunk_index >= 0)),
    CONSTRAINT remote_worker_attempts_terminal_state_check CHECK (((terminal_state IS NULL) OR (terminal_state IN ('completed', 'failed', 'abandoned', 'orphan_terminated', 'indeterminate')))),
    CONSTRAINT remote_worker_attempts_scan_directory_id_fkey FOREIGN KEY (scan_directory_id) REFERENCES remote_scan_directories(scan_directory_id) ON DELETE SET NULL,
    CONSTRAINT remote_worker_attempts_session_id_fkey FOREIGN KEY (session_id) REFERENCES remote_sessions(session_id) ON DELETE CASCADE,
    CONSTRAINT remote_worker_attempts_pkey PRIMARY KEY (attempt_id)
);

CREATE TABLE tape_generations (
    generation_id INTEGER PRIMARY KEY AUTOINCREMENT,
    tape_id INTEGER NOT NULL,
    volume_label TEXT NOT NULL,
    generation INTEGER NOT NULL,
    state TEXT NOT NULL,
    formatted_at TIMESTAMPTZ TEXT,
    retired_at TIMESTAMPTZ TEXT,
    retired_reason TEXT,
    reset_operation_id TEXT,
    CONSTRAINT tape_generations_generation_check CHECK ((generation >= 1)),
    CONSTRAINT tape_generations_state_check CHECK ((state IN ('active', 'retired'))),
    CONSTRAINT tape_generations_reset_operation_id_fkey FOREIGN KEY (reset_operation_id) REFERENCES tape_reset_operations(operation_id) ON DELETE RESTRICT,
    CONSTRAINT tape_generations_tape_id_fkey FOREIGN KEY (tape_id) REFERENCES tapes(tape_id) ON DELETE RESTRICT,
    CONSTRAINT tape_generations_tape_id_generation_key UNIQUE (tape_id, generation)
);

CREATE TABLE tape_reset_operations (
    operation_id TEXT NOT NULL,
    target_tape_id INTEGER NOT NULL,
    target_label TEXT NOT NULL,
    old_generation INTEGER NOT NULL,
    new_generation INTEGER NOT NULL,
    state TEXT NOT NULL,
    expected_drive TEXT NOT NULL,
    expected_drive_serial TEXT NOT NULL,
    expected_ltfs_gen INTEGER,
    expected_vcr INTEGER,
    backup_path TEXT NOT NULL,
    backup_sha256 TEXT NOT NULL,
    restore_list_path TEXT NOT NULL,
    restore_list_sha256 TEXT NOT NULL,
    shadow_database TEXT NOT NULL,
    impact_report_path TEXT NOT NULL,
    impact_report_sha256 TEXT NOT NULL,
    impact JSONB TEXT NOT NULL,
    hardware_evidence JSONB TEXT,
    cleanup_evidence JSONB TEXT,
    error_message TEXT,
    created_at TIMESTAMPTZ TEXT NOT NULL DEFAULT (strftime('%Y-%m-%d %H:%M:%f','now')||'000+00:00'),
    hardware_started_at TIMESTAMPTZ TEXT,
    hardware_succeeded_at TIMESTAMPTZ TEXT,
    cleanup_succeeded_at TIMESTAMPTZ TEXT,
    verified_at TIMESTAMPTZ TEXT,
    CONSTRAINT tape_reset_generation_ck CHECK ((new_generation = (old_generation + 1))),
    CONSTRAINT tape_reset_operations_backup_sha256_check CHECK ((length(backup_sha256) = 64)),
    CONSTRAINT tape_reset_operations_impact_report_sha256_check CHECK ((length(impact_report_sha256) = 64)),
    CONSTRAINT tape_reset_operations_new_generation_check CHECK ((new_generation >= 2)),
    CONSTRAINT tape_reset_operations_old_generation_check CHECK ((old_generation >= 1)),
    CONSTRAINT tape_reset_operations_restore_list_sha256_check CHECK ((length(restore_list_sha256) = 64)),
    CONSTRAINT tape_reset_state_ck CHECK ((state IN ('intent_recorded', 'hardware_started', 'hardware_succeeded', 'cleanup_succeeded', 'verified', 'failed'))),
    CONSTRAINT tape_reset_operations_target_tape_id_fkey FOREIGN KEY (target_tape_id) REFERENCES tapes(tape_id) ON DELETE RESTRICT,
    CONSTRAINT tape_reset_operations_pkey PRIMARY KEY (operation_id)
);

CREATE TABLE tapes (
    tape_id INTEGER PRIMARY KEY AUTOINCREMENT,
    volume_label TEXT NOT NULL,
    date_formatted TIMESTAMPTZ TEXT,
    total_capacity INTEGER,
    used_space INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'active',
    status_reason TEXT,
    status_changed_at TIMESTAMPTZ TEXT,
    current_generation INTEGER NOT NULL DEFAULT 1,
    CONSTRAINT tapes_status_check CHECK ((status IN ('active', 'full'))),
    CONSTRAINT tapes_used_space_check CHECK ((used_space >= 0)),
    CONSTRAINT tapes_volume_label_key UNIQUE (volume_label)
);

-- Indexes

CREATE UNIQUE INDEX uq_archive_artifacts_chunk_kind_version ON archive_artifacts (session_id, chunk_index, artifact_kind, artifact_version) WHERE (container_id IS NULL);
CREATE UNIQUE INDEX uq_archive_artifacts_container_kind_version ON archive_artifacts (container_id, artifact_kind, artifact_version) WHERE (container_id IS NOT NULL);
CREATE UNIQUE INDEX uq_archive_artifacts_ready_plan_chunk ON archive_artifacts (session_id, chunk_index) WHERE ((artifact_kind = 'plan_manifest') AND (container_id IS NULL) AND (readiness_state = 'ready'));
CREATE INDEX idx_archive_container_members_container ON archive_container_members (container_id) WHERE (container_id IS NOT NULL);
CREATE INDEX ix_archive_directories_parent ON archive_directories (parent_directory_id, name);
CREATE INDEX idx_archive_runs_local_session ON archive_runs (local_session_id) WHERE (local_session_id IS NOT NULL);
CREATE INDEX idx_archive_runs_remote_session ON archive_runs (remote_session_id) WHERE (remote_session_id IS NOT NULL);
CREATE INDEX idx_archive_runs_tape_label ON archive_runs (tape_label);
CREATE UNIQUE INDEX uq_archive_runs_remote_chunk_generation ON archive_runs (remote_session_id, remote_chunk_index, tape_generation_id) WHERE ((remote_session_id IS NOT NULL) AND (remote_chunk_index IS NOT NULL) AND (tape_generation_id IS NOT NULL));
CREATE INDEX idx_catalog_dirs_parent ON catalog_directories (tape_label, parent_id, name, directory_id);
CREATE INDEX idx_catalog_dirs_parent_id ON catalog_directories (parent_id, name, directory_id);
CREATE INDEX idx_directory_bundles_manifest ON directory_archive_bundles (manifest_path) WHERE (manifest_path IS NOT NULL);
CREATE INDEX idx_directory_bundles_tape_path ON directory_archive_bundles (tape_label, source_host, original_dir_path);
CREATE INDEX ix_directory_archive_parts_restore ON directory_archive_parts (directory_id, tape_label, tape_generation_id, restore_format);
CREATE UNIQUE INDEX uq_directory_archive_parts_container ON directory_archive_parts (directory_id, session_id, chunk_index, container_id, evidence_generation) WHERE (container_id IS NOT NULL);
CREATE UNIQUE INDEX uq_directory_archive_parts_loose ON directory_archive_parts (directory_id, session_id, chunk_index, loose_record_key, evidence_generation) WHERE (loose_record_key IS NOT NULL);
CREATE INDEX idx_directory_stats_tape_path ON directory_archive_stats (tape_label, source_host, original_dir_path);
CREATE INDEX ix_directory_completeness_status ON directory_completeness (session_id, status, directory_id);
CREATE INDEX ix_directory_scan_coverage_session_generation ON directory_scan_coverage (session_id, frontier_generation, directory_id);
CREATE INDEX idx_directory_tree_bundle ON directory_tree_index (bundle_id) WHERE (bundle_id IS NOT NULL);
CREATE INDEX idx_directory_tree_parent ON directory_tree_index (tape_label, source_host, parent_original_dir_path, dir_name);
CREATE INDEX idx_directory_tree_path ON directory_tree_index (source_host, tape_label, original_dir_path);
CREATE INDEX idx_files_backup_date_tape ON files_index (catalog_backup_date, tape_label, file_id);
CREATE INDEX idx_files_bundle ON files_index (bundle_id) WHERE (bundle_id IS NOT NULL);
CREATE INDEX idx_files_dir_date ON files_index (directory_id, catalog_backup_date, catalog_name, file_id);
CREATE INDEX idx_files_dir_name ON files_index (directory_id, catalog_name, file_id);
CREATE INDEX idx_files_dir_size ON files_index (directory_id, file_size_bytes, catalog_name, file_id);
CREATE INDEX idx_files_local_chunk ON files_index (local_session_id, local_chunk_index, tape_label) WHERE (local_session_id IS NOT NULL);
CREATE INDEX idx_files_remote_chunk ON files_index (remote_session_id, remote_chunk_index, tape_label) WHERE (remote_session_id IS NOT NULL);
CREATE INDEX idx_files_run ON files_index (archive_run_id);
CREATE INDEX idx_files_src_host ON files_index (source_host, tape_label, original_path);
CREATE INDEX idx_files_tape ON files_index (tape_label);
CREATE INDEX idx_local_manifest_session_chunk ON local_chunks_manifest (session_id, chunk_index);
CREATE INDEX idx_local_manifest_tape_label ON local_chunks_manifest (tape_label) WHERE (tape_label IS NOT NULL);
CREATE INDEX idx_local_manifest_catalog_directory ON local_manifest_catalog_aggregates (directory_id, export_id);
CREATE INDEX idx_local_manifest_rows_eligible ON local_manifest_export_rows (export_id, eligible, source_file_id);
CREATE INDEX idx_local_manifest_rows_prune_pending ON local_manifest_export_rows (export_id, source_file_id) WHERE (eligible AND (pruned_at IS NULL));
CREATE INDEX idx_local_manifest_rows_skip_reason ON local_manifest_export_rows (export_id, skip_reason) WHERE (NOT eligible);
CREATE INDEX idx_local_manifest_folder_path ON local_manifest_folder_aggregates (source_host, tape_label, original_dir_path);
CREATE INDEX idx_local_sessions_active_source ON local_sessions (source_dir, session_id DESC) WHERE (status = 'active');
CREATE UNIQUE INDEX uq_remote_chunk_plan_source_transition ON remote_chunk_plan_source_transitions (session_id, chunk_index);
CREATE UNIQUE INDEX uq_remote_chunk_plan_transition_artifact ON remote_chunk_plan_source_transitions (new_plan_manifest_artifact_id);
CREATE INDEX ix_remote_chunk_scan_segments_chunk ON remote_chunk_scan_segments (session_id, chunk_index);
CREATE INDEX idx_remote_chunk_transition_overrides_open ON remote_chunk_transition_overrides (session_id, chunk_index) WHERE (consumed_at IS NULL);
CREATE UNIQUE INDEX uq_remote_chunks_plan_manifest_artifact ON remote_chunks (plan_manifest_artifact_id) WHERE (plan_manifest_artifact_id IS NOT NULL);
CREATE UNIQUE INDEX uq_remote_chunks_terminal_manifest_artifact ON remote_chunks (terminal_manifest_artifact_id) WHERE (terminal_manifest_artifact_id IS NOT NULL);
CREATE INDEX idx_remote_file_state_plan_file_id ON remote_file_state (plan_file_id);
CREATE UNIQUE INDEX uq_remote_frontier_bootstrap_open ON remote_frontier_bootstraps (session_id) WHERE (state IN ('planned', 'running'));
CREATE INDEX idx_remote_plan_chunk ON remote_plan_files (plan_id, chunk_index, ordinal);
CREATE INDEX idx_remote_plan_files_snapshot_file_id ON remote_plan_files (snapshot_file_id);
CREATE UNIQUE INDEX uq_remote_plan_files_chunk_ordinal ON remote_plan_files (plan_id, chunk_index, ordinal);
CREATE INDEX idx_remote_plans_snapshot_id ON remote_plans (snapshot_id);
CREATE INDEX ix_remote_scan_directories_claimable ON remote_scan_directories (scan_scope_id, traversal_ordinal) WHERE (listing_state IN ('pending', 'partial'));
CREATE INDEX ix_remote_scan_errors_directory ON remote_scan_errors (scan_directory_id);
CREATE INDEX ix_remote_scan_segments_consumable ON remote_scan_segments (scan_directory_id, first_scan_ordinal) WHERE (state IN ('ready', 'partially_consumed'));
CREATE INDEX ix_remote_session_plan_effective_boundary ON remote_session_plan_transitions (session_id, first_chunk_after_transition DESC, transition_epoch DESC) WHERE (state IN ('active', 'rolled_back'));
CREATE UNIQUE INDEX uq_remote_session_plan_active_boundary ON remote_session_plan_transitions (session_id, first_chunk_after_transition) WHERE (state = 'active');
CREATE INDEX idx_remote_sessions_active_source ON remote_sessions (remote_host, remote_path, session_id DESC) WHERE (status = 'active');
CREATE INDEX idx_remote_sessions_plan_id ON remote_sessions (plan_id) WHERE (plan_id IS NOT NULL);
CREATE INDEX idx_remote_sessions_tape_label ON remote_sessions (tape_label);
CREATE INDEX ix_remote_worker_attempts_live ON remote_worker_attempts (session_id, attempt_kind) WHERE (terminal_state IS NULL);
CREATE UNIQUE INDEX uq_tape_generations_one_active ON tape_generations (tape_id) WHERE (state = 'active');
CREATE UNIQUE INDEX uq_tape_reset_open_operation ON tape_reset_operations (target_tape_id) WHERE (state IN ('intent_recorded', 'hardware_started', 'hardware_succeeded', 'cleanup_succeeded'));

-- PostgreSQL trigram (GIN) indexes with no SQLite counterpart: idx_directory_tree_path_trgm, idx_files_catalog_name_trgm, idx_files_original_path_trgm

-- View (018). PostgreSQL uses LATERAL / DISTINCT ON / jsonb_agg; this is the
-- same result set with window functions, correlated subqueries and JSON1.
-- JSON columns (child_directories, coverage_generations, restore_route) are
-- JSON text and boolean expressions are 0/1: untyped expression results.
CREATE VIEW directory_catalog_status_v AS
WITH directory_session_keys AS (
    SELECT directory_id, session_id FROM directory_scan_coverage
    UNION
    SELECT directory_id, session_id FROM directory_archive_parts
    UNION
    SELECT directory_id, session_id FROM directory_completeness
),
ranked_coverage AS (
    SELECT cov.directory_id, cov.session_id, cov.coverage_state,
           cov.frontier_generation,
           ROW_NUMBER() OVER (
               PARTITION BY cov.directory_id, cov.session_id, cov.scan_scope_id
               ORDER BY cov.frontier_generation DESC, cov.coverage_id DESC
           ) AS rn
    FROM directory_scan_coverage cov
),
coverage AS (
    SELECT directory_id, session_id,
           CASE
               WHEN max(coverage_state = 'error') THEN 'error'
               WHEN min(coverage_state = 'final') THEN 'final'
               ELSE 'provisional'
           END AS coverage_state,
           min(coverage_state = 'final') AS coverage_is_final,
           max(frontier_generation) AS maximum_frontier_generation
    FROM ranked_coverage
    WHERE rn = 1
    GROUP BY directory_id, session_id
)
SELECT d.directory_id,
    d.source_host,
    d.canonical_path,
    d.name AS directory_name,
    d.depth,
    d.parent_directory_id,
    parent.canonical_path AS parent_canonical_path,
    parent.name AS parent_name,
    keys.session_id,
    session_row.session_label,
    (SELECT count(*) FROM archive_directories child
     WHERE child.parent_directory_id = d.directory_id) AS child_count,
    (SELECT COALESCE(json_group_array(json_object(
                'directory_id', child.directory_id,
                'canonical_path', child.canonical_path,
                'name', child.name,
                'depth', child.depth)), '[]')
     FROM (SELECT c.directory_id, c.canonical_path, c.name, c.depth
           FROM archive_directories c
           WHERE c.parent_directory_id = d.directory_id
           ORDER BY c.name, c.directory_id) child) AS child_directories,
    COALESCE(coverage.coverage_state, 'provisional') AS coverage_state,
    COALESCE(coverage.coverage_is_final, 0) AS coverage_is_final,
    coverage.maximum_frontier_generation,
    (SELECT COALESCE(json_group_array(json_object(
                'coverage_id', history.coverage_id,
                'scan_scope_id', history.scan_scope_id,
                'coverage_state', history.coverage_state,
                'frontier_generation', history.frontier_generation,
                'direct_discovered_file_count',
                    history.direct_discovered_file_count,
                'direct_discovered_bytes', history.direct_discovered_bytes,
                'direct_discovered_directory_count',
                    history.direct_discovered_directory_count,
                'recursive_discovered_file_count',
                    history.recursive_discovered_file_count,
                'recursive_discovered_bytes',
                    history.recursive_discovered_bytes,
                'recursive_discovered_directory_count',
                    history.recursive_discovered_directory_count,
                'excluded_count', history.excluded_count,
                'error_count', history.error_count)), '[]')
     FROM (SELECT h.* FROM directory_scan_coverage h
           WHERE h.directory_id = d.directory_id
             AND h.session_id = keys.session_id
           ORDER BY h.frontier_generation, h.scan_scope_id, h.coverage_id
          ) history) AS coverage_generations,
    completeness.direct_expected_file_count,
    completeness.direct_expected_bytes,
    completeness.direct_archived_file_count,
    completeness.direct_archived_bytes,
    completeness.recursive_expected_file_count,
    completeness.recursive_expected_bytes,
    completeness.recursive_archived_file_count,
    completeness.recursive_archived_bytes,
    completeness.recursive_source_missing_count,
    completeness.recursive_source_permission_denied_count,
    completeness.recursive_source_unreadable_count,
    completeness.recursive_source_changed_count,
    completeness.recursive_unresolved_count,
    completeness.scan_is_final,
    completeness.all_planned_items_terminal,
    completeness.all_required_items_archived,
    completeness.all_parts_written,
    completeness.all_writer_completions_succeeded,
    completeness.all_parts_cataloged,
    completeness.all_local_validation_succeeded,
    completeness.has_ambiguous_evidence,
    completeness.status AS completeness_status,
    completeness.pinned_frontier_generation,
    completeness.pinned_artifact_evidence_generation,
    completeness.calculated_at,
    part.part_id,
    part.chunk_index,
    chunk.plan_source,
    chunk.packaging_format,
    part.container_id,
    container.container_ordinal,
    container.container_name,
    container.container_format,
    part.loose_record_key,
    part.storage_class,
    part.evidence_generation,
    part.direct_expected_count,
    part.direct_expected_bytes AS part_direct_expected_bytes,
    part.direct_archived_count,
    part.direct_archived_bytes AS part_direct_archived_bytes,
    part.direct_source_missing_count AS part_source_missing_count,
    part.direct_source_missing_bytes AS part_source_missing_bytes,
    part.direct_source_permission_denied_count
        AS part_source_permission_denied_count,
    part.direct_source_permission_denied_bytes
        AS part_source_permission_denied_bytes,
    part.direct_source_unreadable_count AS part_source_unreadable_count,
    part.direct_source_unreadable_bytes AS part_source_unreadable_bytes,
    part.direct_source_changed_count AS part_source_changed_count,
    part.direct_source_changed_bytes AS part_source_changed_bytes,
    part.direct_unresolved_count AS part_unresolved_count,
    part.direct_unresolved_bytes AS part_unresolved_bytes,
    part.plan_manifest_artifact_id,
    part.sidecar_artifact_id,
    part.terminal_manifest_artifact_id,
    COALESCE(part.plan_artifact_locator, plan_artifact.local_locator)
        AS plan_artifact_locator,
    COALESCE(part.sidecar_artifact_locator, sidecar_artifact.local_locator)
        AS sidecar_artifact_locator,
    COALESCE(part.terminal_artifact_locator, terminal_artifact.local_locator)
        AS terminal_artifact_locator,
    part.local_validation_state,
    part.writer_state,
    part.catalog_state,
    part.restore_format,
    part.tape_label,
    part.tape_generation_id,
    tape_generation.generation AS tape_generation,
    tape_generation.state AS tape_generation_state,
    part.stored_path,
    part.source_base_path,
    part.container_member_candidate,
    part.routing_precision,
    CASE
        WHEN part.part_id IS NULL THEN NULL
        ELSE json_object(
            'format', part.restore_format,
            'tape_label', part.tape_label,
            'tape_generation_id', part.tape_generation_id,
            'tape_generation', tape_generation.generation,
            'stored_path', part.stored_path,
            'source_base_path', part.source_base_path,
            'container_member_candidate', part.container_member_candidate,
            'routing_precision', part.routing_precision)
    END AS restore_route
FROM archive_directories d
LEFT JOIN archive_directories parent
       ON parent.directory_id = d.parent_directory_id
LEFT JOIN directory_session_keys keys ON keys.directory_id = d.directory_id
LEFT JOIN remote_sessions session_row
       ON session_row.session_id = keys.session_id
LEFT JOIN directory_completeness completeness
       ON completeness.directory_id = d.directory_id
      AND completeness.session_id = keys.session_id
LEFT JOIN directory_archive_parts part
       ON part.directory_id = d.directory_id
      AND part.session_id = keys.session_id
LEFT JOIN remote_chunks chunk
       ON chunk.session_id = part.session_id
      AND chunk.chunk_index = part.chunk_index
LEFT JOIN archive_containers container
       ON container.container_id = part.container_id
LEFT JOIN tape_generations tape_generation
       ON tape_generation.generation_id = part.tape_generation_id
LEFT JOIN archive_artifacts plan_artifact
       ON plan_artifact.artifact_id = part.plan_manifest_artifact_id
LEFT JOIN archive_artifacts sidecar_artifact
       ON sidecar_artifact.artifact_id = part.sidecar_artifact_id
LEFT JOIN archive_artifacts terminal_artifact
       ON terminal_artifact.artifact_id = part.terminal_manifest_artifact_id
LEFT JOIN coverage
       ON coverage.directory_id = d.directory_id
      AND coverage.session_id = keys.session_id;

-- Guard triggers ---------------------------------------------------------------

-- ===== Migration 015: container formats =====

-- lto_guard_artifact_container_format
CREATE TRIGGER trg_archive_artifacts_container_format
BEFORE INSERT ON archive_artifacts
FOR EACH ROW
WHEN NEW.container_id IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'container-scoped artifact kind conflicts with durable format')
    WHERE (NEW.artifact_kind = 'tar_sidecar' AND (SELECT container_format FROM archive_containers WHERE container_id = NEW.container_id) <> 'stored_tar')
       OR (NEW.artifact_kind = 'zip_manifest' AND (SELECT container_format FROM archive_containers WHERE container_id = NEW.container_id) <> 'zip');
END;

CREATE TRIGGER trg_archive_artifacts_container_format__upd
BEFORE UPDATE OF container_id, artifact_kind ON archive_artifacts
FOR EACH ROW
WHEN NEW.container_id IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'container-scoped artifact kind conflicts with durable format')
    WHERE (NEW.artifact_kind = 'tar_sidecar' AND (SELECT container_format FROM archive_containers WHERE container_id = NEW.container_id) <> 'stored_tar')
       OR (NEW.artifact_kind = 'zip_manifest' AND (SELECT container_format FROM archive_containers WHERE container_id = NEW.container_id) <> 'zip');
END;

-- lto_reject_artifact_identity_change
CREATE TRIGGER trg_archive_artifacts_identity_write_once
BEFORE UPDATE OF session_id, chunk_index, container_id, artifact_kind, artifact_version ON archive_artifacts
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'archive artifact identity is immutable')
    WHERE (OLD.session_id IS NOT NEW.session_id
          OR OLD.chunk_index IS NOT NEW.chunk_index
          OR OLD.container_id IS NOT NEW.container_id
          OR OLD.artifact_kind IS NOT NEW.artifact_kind
          OR OLD.artifact_version IS NOT NEW.artifact_version);
END;

-- lto_reject_container_format_change
CREATE TRIGGER trg_archive_bundles_format_write_once
BEFORE UPDATE OF container_format ON archive_bundles
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'container format is immutable for container')
    WHERE OLD.container_format IS NOT NEW.container_format;
END;

-- lto_reject_container_identity_change
CREATE TRIGGER trg_archive_containers_format_write_once
BEFORE UPDATE OF session_id, chunk_index, container_ordinal, container_format, format_version, tar_dialect, storage_class, container_name, expected_member_count, expected_logical_bytes ON archive_containers
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'archive container identity is immutable')
    WHERE (OLD.session_id IS NOT NEW.session_id
          OR OLD.chunk_index IS NOT NEW.chunk_index
          OR OLD.container_ordinal IS NOT NEW.container_ordinal
          OR OLD.container_format IS NOT NEW.container_format
          OR OLD.format_version IS NOT NEW.format_version
          OR OLD.tar_dialect IS NOT NEW.tar_dialect
          OR OLD.storage_class IS NOT NEW.storage_class
          OR OLD.container_name IS NOT NEW.container_name
          OR OLD.expected_member_count IS NOT NEW.expected_member_count
          OR OLD.expected_logical_bytes IS NOT NEW.expected_logical_bytes);
END;

-- lto_guard_container_tape_generation
CREATE TRIGGER trg_archive_containers_tape_generation
BEFORE INSERT ON archive_containers
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'container write/catalog state requires exact tape generation')
    WHERE NEW.tape_generation_id IS NULL
      AND (NEW.tape_label IS NOT NULL OR NEW.tape_path IS NOT NULL
           OR NEW.writer_state <> 'not_started'
           OR NEW.catalog_state <> 'not_started');
    SELECT RAISE(ABORT, 'container tape generation does not match session/tape assignment')
    WHERE NEW.tape_generation_id IS NOT NULL
      AND ((SELECT g.volume_label FROM tape_generations g WHERE g.generation_id = NEW.tape_generation_id) IS NOT NEW.tape_label
           OR (SELECT s.tape_label FROM remote_sessions s WHERE s.session_id = NEW.session_id) IS NOT NEW.tape_label
           OR (SELECT s.tape_generation FROM remote_sessions s WHERE s.session_id = NEW.session_id) IS NOT
              (SELECT g.generation FROM tape_generations g WHERE g.generation_id = NEW.tape_generation_id));
END;

CREATE TRIGGER trg_archive_containers_tape_generation__upd
BEFORE UPDATE OF tape_label, tape_path, tape_generation_id, writer_state, catalog_state ON archive_containers
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'container write/catalog state requires exact tape generation')
    WHERE NEW.tape_generation_id IS NULL
      AND (NEW.tape_label IS NOT NULL OR NEW.tape_path IS NOT NULL
           OR NEW.writer_state <> 'not_started'
           OR NEW.catalog_state <> 'not_started');
    SELECT RAISE(ABORT, 'container tape generation does not match session/tape assignment')
    WHERE NEW.tape_generation_id IS NOT NULL
      AND ((SELECT g.volume_label FROM tape_generations g WHERE g.generation_id = NEW.tape_generation_id) IS NOT NEW.tape_label
           OR (SELECT s.tape_label FROM remote_sessions s WHERE s.session_id = NEW.session_id) IS NOT NEW.tape_label
           OR (SELECT s.tape_generation FROM remote_sessions s WHERE s.session_id = NEW.session_id) IS NOT
              (SELECT g.generation FROM tape_generations g WHERE g.generation_id = NEW.tape_generation_id));
END;

CREATE TRIGGER trg_container_format_schema_metadata_immutable
BEFORE INSERT ON container_format_schema_metadata
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'container format schema authority is immutable')
    WHERE EXISTS (SELECT 1 FROM container_format_schema_metadata)
       OR NEW.migration_checksum IS NOT (SELECT value FROM lto_txn_settings WHERE name='lto.sqlite_schema_bootstrap_checksum');
END;

CREATE TRIGGER trg_container_format_schema_metadata_immutable__upd
BEFORE UPDATE ON container_format_schema_metadata
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'container format schema authority is immutable');
END;

CREATE TRIGGER trg_container_format_schema_metadata_immutable__del
BEFORE DELETE ON container_format_schema_metadata
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'container format schema authority is immutable');
END;

CREATE TRIGGER trg_manifest_directory_catalog_schema_metadata_immutable
BEFORE INSERT ON manifest_directory_catalog_schema_metadata
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'manifest directory catalog schema authority is immutable')
    WHERE EXISTS (SELECT 1 FROM manifest_directory_catalog_schema_metadata)
       OR NEW.migration_checksum IS NOT (SELECT value FROM lto_txn_settings WHERE name='lto.sqlite_schema_bootstrap_checksum');
END;

CREATE TRIGGER trg_manifest_directory_catalog_schema_metadata_immutable__upd
BEFORE UPDATE ON manifest_directory_catalog_schema_metadata
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'manifest directory catalog schema authority is immutable');
END;

CREATE TRIGGER trg_manifest_directory_catalog_schema_metadata_immutable__del
BEFORE DELETE ON manifest_directory_catalog_schema_metadata
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'manifest directory catalog schema authority is immutable');
END;

CREATE TRIGGER trg_container_format_schema_authority_v2_immutable
BEFORE UPDATE ON container_format_schema_authority
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'container format schema authority v2 rows are immutable');
END;

CREATE TRIGGER trg_container_format_schema_authority_v2_immutable__del
BEFORE DELETE ON container_format_schema_authority
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'container format schema authority v2 rows are immutable');
END;

CREATE TRIGGER trg_remote_packaging_boundaries_immutable
BEFORE INSERT ON remote_packaging_boundaries
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'packaging boundary/audit evidence is immutable');
END;

CREATE TRIGGER trg_remote_packaging_boundaries_immutable__upd
BEFORE UPDATE ON remote_packaging_boundaries
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'packaging boundary/audit evidence is immutable');
END;

CREATE TRIGGER trg_remote_packaging_boundaries_immutable__del
BEFORE DELETE ON remote_packaging_boundaries
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'packaging boundary/audit evidence is immutable');
END;

CREATE TRIGGER trg_remote_packaging_boundary_chunks_immutable
BEFORE INSERT ON remote_packaging_boundary_chunks
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'packaging boundary/audit evidence is immutable');
END;

CREATE TRIGGER trg_remote_packaging_boundary_chunks_immutable__upd
BEFORE UPDATE ON remote_packaging_boundary_chunks
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'packaging boundary/audit evidence is immutable');
END;

CREATE TRIGGER trg_remote_packaging_boundary_chunks_immutable__del
BEFORE DELETE ON remote_packaging_boundary_chunks
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'packaging boundary/audit evidence is immutable');
END;

-- lto_reject_packaging_format_change
CREATE TRIGGER trg_remote_chunks_packaging_write_once
BEFORE UPDATE OF packaging_format, packaging_assigned_at ON remote_chunks
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'remote chunk packaging format is write-once')
    WHERE (OLD.packaging_format IS NOT NEW.packaging_format
          OR OLD.packaging_assigned_at IS NOT NEW.packaging_assigned_at);
END;

-- lto_guard_session_packaging_default
CREATE TRIGGER trg_remote_sessions_packaging_default
BEFORE INSERT ON remote_sessions
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'new sessions default to zip; Stored TAR needs an approved boundary')
    WHERE NEW.default_packaging_format = 'stored_tar';
END;

CREATE TRIGGER trg_remote_sessions_packaging_default__upd
BEFORE UPDATE OF default_packaging_format ON remote_sessions
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'remote session packaging default is immutable after initial assignment')
    WHERE OLD.default_packaging_format IS NOT NEW.default_packaging_format;
END;

-- lto_guard_new_chunk_format
CREATE TRIGGER trg_remote_chunks_initial_format
BEFORE INSERT ON remote_chunks
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'initial chunk format conflicts with durable format')
    WHERE NEW.packaging_format IS NOT (CASE
            WHEN (SELECT first_stored_tar_chunk_index
                  FROM remote_packaging_boundaries
                  WHERE session_id = NEW.session_id) IS NOT NULL
                 AND NEW.chunk_index >= (
                     SELECT first_stored_tar_chunk_index
                     FROM remote_packaging_boundaries
                     WHERE session_id = NEW.session_id) THEN 'stored_tar'
            WHEN (SELECT first_stored_tar_chunk_index
                  FROM remote_packaging_boundaries
                  WHERE session_id = NEW.session_id) IS NOT NULL THEN 'zip'
            ELSE (SELECT default_packaging_format FROM remote_sessions
                  WHERE session_id = NEW.session_id)
          END)
       OR NEW.packaging_assigned_at IS NULL;
END;

-- ===== Migration 018: manifest-first directory catalog =====

-- lto_guard_archive_directory
CREATE TRIGGER trg_archive_directories_parent_guard
BEFORE INSERT ON archive_directories
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'root archive directory depth must be zero')
    WHERE NEW.parent_directory_id IS NULL AND NEW.depth <> 0;
    SELECT RAISE(ABORT, 'archive directory parent must share source host and be one depth above')
    WHERE NEW.parent_directory_id IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM archive_directories p
          WHERE p.directory_id = NEW.parent_directory_id
            AND p.source_host IS NEW.source_host
            AND NEW.depth = p.depth + 1);
END;

CREATE TRIGGER trg_archive_directories_parent_guard__upd
BEFORE UPDATE OF source_host, canonical_path, parent_directory_id, name, depth ON archive_directories
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'canonical archive directory identity is immutable')
    WHERE (OLD.source_host IS NOT NEW.source_host
          OR OLD.canonical_path IS NOT NEW.canonical_path
          OR OLD.parent_directory_id IS NOT NEW.parent_directory_id
          OR OLD.name IS NOT NEW.name
          OR OLD.depth IS NOT NEW.depth);
END;

CREATE TRIGGER trg_archive_directories_parent_guard__touch
AFTER UPDATE OF source_host, canonical_path, parent_directory_id, name, depth ON archive_directories
FOR EACH ROW
BEGIN
    UPDATE archive_directories SET updated_at = (strftime('%Y-%m-%d %H:%M:%f','now')||'000+00:00')
    WHERE directory_id = NEW.directory_id;
END;

-- lto_guard_directory_scan_coverage
CREATE TRIGGER trg_directory_scan_coverage_scope_guard
BEFORE INSERT ON directory_scan_coverage
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'directory coverage scope does not belong to its session')
    WHERE (SELECT session_id FROM remote_scan_scopes
        WHERE scan_scope_id = NEW.scan_scope_id) IS NOT NEW.session_id;
END;

CREATE TRIGGER trg_directory_scan_coverage_scope_guard__upd
BEFORE UPDATE OF session_id, scan_scope_id ON directory_scan_coverage
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'directory coverage scope does not belong to its session')
    WHERE (SELECT session_id FROM remote_scan_scopes
        WHERE scan_scope_id = NEW.scan_scope_id) IS NOT NEW.session_id;
END;

CREATE TRIGGER trg_directory_scan_coverage_scope_guard__touch
AFTER UPDATE OF session_id, scan_scope_id ON directory_scan_coverage
FOR EACH ROW
BEGIN
    UPDATE directory_scan_coverage SET updated_at = (strftime('%Y-%m-%d %H:%M:%f','now')||'000+00:00')
    WHERE coverage_id = NEW.coverage_id;
END;

-- lto_guard_sealed_artifact
CREATE TRIGGER trg_archive_artifacts_sealed_immutable
BEFORE UPDATE OF session_id, chunk_index, container_id, artifact_kind, artifact_version, readiness_state, local_locator, artifact_size_bytes, published_at ON archive_artifacts
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'ready archive artifact is immutable')
    WHERE OLD.readiness_state = 'ready' AND (OLD.session_id IS NOT NEW.session_id
          OR OLD.chunk_index IS NOT NEW.chunk_index
          OR OLD.container_id IS NOT NEW.container_id
          OR OLD.artifact_kind IS NOT NEW.artifact_kind
          OR OLD.artifact_version IS NOT NEW.artifact_version
          OR OLD.readiness_state IS NOT NEW.readiness_state
          OR OLD.local_locator IS NOT NEW.local_locator
          OR OLD.artifact_size_bytes IS NOT NEW.artifact_size_bytes
          OR OLD.published_at IS NOT NEW.published_at);
    SELECT RAISE(ABORT, 'authoritative archive artifact is sealed')
    WHERE EXISTS (SELECT 1 FROM remote_chunks c
              WHERE c.plan_manifest_artifact_id = OLD.artifact_id
                 OR c.terminal_manifest_artifact_id = OLD.artifact_id)
      AND (OLD.session_id IS NOT NEW.session_id
          OR OLD.chunk_index IS NOT NEW.chunk_index
          OR OLD.container_id IS NOT NEW.container_id
          OR OLD.artifact_kind IS NOT NEW.artifact_kind
          OR OLD.artifact_version IS NOT NEW.artifact_version
          OR OLD.readiness_state IS NOT NEW.readiness_state
          OR OLD.local_locator IS NOT NEW.local_locator
          OR OLD.artifact_size_bytes IS NOT NEW.artifact_size_bytes
          OR OLD.published_at IS NOT NEW.published_at);
END;

CREATE TRIGGER trg_archive_artifacts_sealed_immutable__del
BEFORE DELETE ON archive_artifacts
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'ready archive artifact is immutable')
    WHERE OLD.readiness_state = 'ready';
END;

-- lto_reject_chunk_plan_transition_mutation
CREATE TRIGGER trg_remote_chunk_plan_source_transitions_immutable
BEFORE INSERT ON remote_chunk_plan_source_transitions
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'chunk plan-source transition audit is append-only')
    WHERE (SELECT value FROM lto_txn_settings WHERE name='lto.chunk_plan_transition_audit_write') IS NOT 'on'
       OR NEW.plan4_gate_id IS NOT (SELECT value FROM lto_txn_settings WHERE name='lto.chunk_plan_transition_gate_id')
       OR NEW.plan4_evidence_id IS NOT (SELECT value FROM lto_txn_settings WHERE name='lto.chunk_plan_transition_evidence_id')
       OR NEW.approval_identity IS NOT (SELECT value FROM lto_txn_settings WHERE name='lto.chunk_plan_transition_approval_identity');
END;

CREATE TRIGGER trg_remote_chunk_plan_source_transitions_immutable__upd
BEFORE UPDATE ON remote_chunk_plan_source_transitions
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'chunk plan-source transition audit is append-only');
END;

CREATE TRIGGER trg_remote_chunk_plan_source_transitions_immutable__del
BEFORE DELETE ON remote_chunk_plan_source_transitions
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'chunk plan-source transition audit is append-only');
END;

-- lto_guard_chunk_plan_source (INSERT). plan_source has a column DEFAULT of legacy_db where PostgreSQL assigns NEW.
CREATE TRIGGER trg_remote_chunks_plan_source_guard
BEFORE INSERT ON remote_chunks
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'initial chunk plan source conflicts with durable transition boundary')
    WHERE NEW.plan_source IS NOT COALESCE(
              (SELECT t.new_plan_source
               FROM remote_session_plan_transitions t
               WHERE t.session_id = NEW.session_id
                 AND t.state IN ('active','rolled_back')
                 AND t.first_chunk_after_transition <= NEW.chunk_index
               ORDER BY t.first_chunk_after_transition DESC,
                        t.transition_epoch DESC
               LIMIT 1),
              'legacy_db');
    SELECT RAISE(ABORT, 'manifest chunk requires exactly one ready plan artifact')
    WHERE NEW.plan_source = 'manifest' AND NEW.plan_manifest_artifact_id IS NULL;
    SELECT RAISE(ABORT, 'legacy database chunk cannot install manifest authority')
    WHERE NEW.plan_source IS NOT 'manifest' AND NEW.plan_manifest_artifact_id IS NOT NULL;
    SELECT RAISE(ABORT, 'artifact is not the ready terminal_manifest authority for its chunk')
    WHERE NEW.terminal_manifest_artifact_id IS NOT NULL
      AND NOT EXISTS (
              SELECT 1 FROM archive_artifacts a
              WHERE a.artifact_id = NEW.terminal_manifest_artifact_id
                AND a.session_id IS NEW.session_id
                AND a.chunk_index IS NEW.chunk_index
                AND a.artifact_kind = 'terminal_manifest'
                AND a.container_id IS NULL
                AND a.readiness_state = 'ready'
                AND a.local_locator IS NOT NULL
                AND lower(substr(a.local_locator, -5)) <> '.part'
                AND a.artifact_size_bytes IS NOT NULL
                AND a.published_at IS NOT NULL);
END;

-- lto_guard_chunk_plan_source (UPDATE). The sealed-batch tape_write_active_chunk probe is omitted: sealed batches are PostgreSQL-only, the table never exists here.
CREATE TRIGGER trg_remote_chunks_plan_source_guard__upd
BEFORE UPDATE OF plan_source, plan_manifest_artifact_id, terminal_manifest_artifact_id, plan_ordinal_scope, final_archived_count, final_source_missing_count, final_source_permission_denied_count, final_source_unreadable_count, final_source_changed_count, final_unresolved_count, final_archived_bytes ON remote_chunks
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'chunk plan ordinal scope is immutable')
    WHERE OLD.plan_ordinal_scope IS NOT NEW.plan_ordinal_scope;
    SELECT RAISE(ABORT, 'terminal chunk artifact and aggregates are immutable')
    WHERE OLD.terminal_manifest_artifact_id IS NOT NULL
      AND (OLD.terminal_manifest_artifact_id IS NOT NEW.terminal_manifest_artifact_id
          OR OLD.final_archived_count IS NOT NEW.final_archived_count
          OR OLD.final_source_missing_count IS NOT NEW.final_source_missing_count
          OR OLD.final_source_permission_denied_count IS NOT NEW.final_source_permission_denied_count
          OR OLD.final_source_unreadable_count IS NOT NEW.final_source_unreadable_count
          OR OLD.final_source_changed_count IS NOT NEW.final_source_changed_count
          OR OLD.final_unresolved_count IS NOT NEW.final_unresolved_count
          OR OLD.final_archived_bytes IS NOT NEW.final_archived_bytes);
    SELECT RAISE(ABORT, 'terminal manifest requires complete terminal unowned chunk aggregates')
    WHERE OLD.terminal_manifest_artifact_id IS NULL AND NEW.terminal_manifest_artifact_id IS NOT NULL
      AND (NEW.status <> 'done'
           OR NEW.owner_token IS NOT NULL
           OR NEW.lease_expires_at IS NOT NULL
           OR NEW.attempt_id IS NOT NULL
           OR NEW.expected_file_count IS NULL
           OR NEW.expected_bytes IS NULL
           OR NEW.final_archived_count IS NULL
           OR NEW.final_source_missing_count IS NULL
           OR NEW.final_source_permission_denied_count IS NULL
           OR NEW.final_source_unreadable_count IS NULL
           OR NEW.final_source_changed_count IS NULL
           OR NEW.final_unresolved_count IS NULL
           OR NEW.final_archived_bytes IS NULL);
    SELECT RAISE(ABORT, 'archived terminal outcomes require durable writer and catalog completion')
    WHERE OLD.terminal_manifest_artifact_id IS NULL AND NEW.terminal_manifest_artifact_id IS NOT NULL
      AND NEW.plan_source = 'manifest'
      AND NEW.final_archived_count > 0
      AND (NEW.writer_completed_at IS NULL
           OR NEW.catalog_committed_at IS NULL);
    SELECT RAISE(ABORT, 'terminal manifest refuses an active chunk worker')
    WHERE OLD.terminal_manifest_artifact_id IS NULL AND NEW.terminal_manifest_artifact_id IS NOT NULL
      AND EXISTS (
              SELECT 1 FROM remote_worker_attempts wa
              WHERE wa.session_id = NEW.session_id
                AND wa.chunk_index = NEW.chunk_index
                AND wa.terminal_state IS NULL);
    SELECT RAISE(ABORT, 'artifact is not the ready terminal_manifest authority for its chunk')
    WHERE OLD.terminal_manifest_artifact_id IS NULL AND NEW.terminal_manifest_artifact_id IS NOT NULL
      AND NOT EXISTS (
              SELECT 1 FROM archive_artifacts a
              WHERE a.artifact_id = NEW.terminal_manifest_artifact_id
                AND a.session_id IS NEW.session_id
                AND a.chunk_index IS NEW.chunk_index
                AND a.artifact_kind = 'terminal_manifest'
                AND a.container_id IS NULL
                AND a.readiness_state = 'ready'
                AND a.local_locator IS NOT NULL
                AND lower(substr(a.local_locator, -5)) <> '.part'
                AND a.artifact_size_bytes IS NOT NULL
                AND a.published_at IS NOT NULL);
    SELECT RAISE(ABORT, 'chunk plan artifact authority changes only with audited source transition')
    WHERE OLD.plan_source IS NEW.plan_source
      AND OLD.plan_manifest_artifact_id IS NOT NEW.plan_manifest_artifact_id;
    SELECT RAISE(ABORT, 'chunk plan source is immutable except audited legacy_db to manifest transition')
    WHERE OLD.plan_source IS NOT NEW.plan_source
      AND (OLD.plan_source IS NOT 'legacy_db'
           OR NEW.plan_source IS NOT 'manifest'
           OR OLD.plan_manifest_artifact_id IS NOT NULL
           OR NEW.plan_manifest_artifact_id IS NULL);
    SELECT RAISE(ABORT, 'audited plan authority transition requires Plan 4 gate, evidence, and approval IDs')
    WHERE OLD.plan_source IS NOT NEW.plan_source
      AND (COALESCE((SELECT value FROM lto_txn_settings WHERE name='lto.chunk_plan_transition_gate_id'), '') = ''
           OR COALESCE((SELECT value FROM lto_txn_settings WHERE name='lto.chunk_plan_transition_evidence_id'), '') = ''
           OR COALESCE((SELECT value FROM lto_txn_settings WHERE name='lto.chunk_plan_transition_approval_identity'), '') = '');
    SELECT RAISE(ABORT, 'plan authority transition requires an unchanged terminal unowned chunk')
    WHERE OLD.plan_source IS NOT NEW.plan_source
      AND (OLD.status <> 'done'
           OR OLD.owner_token IS NOT NULL
           OR OLD.lease_expires_at IS NOT NULL
           OR OLD.attempt_id IS NOT NULL
           OR NEW.status <> 'done'
           OR NEW.owner_token IS NOT NULL
           OR NEW.lease_expires_at IS NOT NULL
           OR NEW.attempt_id IS NOT NULL);
    SELECT RAISE(ABORT, 'plan authority transition refuses an active chunk worker')
    WHERE OLD.plan_source IS NOT NEW.plan_source
      AND EXISTS (
              SELECT 1 FROM remote_worker_attempts wa
              WHERE wa.session_id = OLD.session_id
                AND wa.chunk_index = OLD.chunk_index
                AND wa.terminal_state IS NULL);
    SELECT RAISE(ABORT, 'plan authority transition refuses nonterminal container ownership or state')
    WHERE OLD.plan_source IS NOT NEW.plan_source
      AND EXISTS (
          SELECT 1 FROM archive_containers ac
          WHERE ac.session_id = OLD.session_id
            AND ac.chunk_index = OLD.chunk_index
            AND (ac.owner_token IS NOT NULL
                 OR ac.lease_expires_at IS NOT NULL
                 OR ac.validation_state IN
                      ('building','validated_part','failed','blocked')
                 OR ac.writer_state IN ('writing','failed','ambiguous')
                 OR ac.catalog_state IN
                      ('committing','failed','ambiguous')));
    SELECT RAISE(ABORT, 'shared or missing legacy plan authority refuses transition')
    WHERE OLD.plan_source IS NOT NEW.plan_source
      AND ((SELECT plan_id FROM remote_sessions WHERE session_id = OLD.session_id) IS NULL
           OR EXISTS (
               SELECT 1 FROM remote_sessions s
               WHERE s.plan_id = (SELECT plan_id FROM remote_sessions WHERE session_id = OLD.session_id)
                 AND s.session_id <> OLD.session_id)
           OR NOT EXISTS (
               SELECT 1 FROM remote_plan_files pf
               WHERE pf.plan_id = (SELECT plan_id FROM remote_sessions WHERE session_id = OLD.session_id)
                 AND pf.chunk_index = OLD.chunk_index));
    SELECT RAISE(ABORT, 'artifact is not the ready plan_manifest authority for its chunk')
    WHERE OLD.plan_source IS NOT NEW.plan_source
      AND NOT EXISTS (
              SELECT 1 FROM archive_artifacts a
              WHERE a.artifact_id = NEW.plan_manifest_artifact_id
                AND a.session_id IS NEW.session_id
                AND a.chunk_index IS NEW.chunk_index
                AND a.artifact_kind = 'plan_manifest'
                AND a.container_id IS NULL
                AND a.readiness_state = 'ready'
                AND a.local_locator IS NOT NULL
                AND lower(substr(a.local_locator, -5)) <> '.part'
                AND a.artifact_size_bytes IS NOT NULL
                AND a.published_at IS NOT NULL);
END;

-- audit row for the legacy_db -> manifest transition
CREATE TRIGGER trg_remote_chunks_plan_source_guard__audit
AFTER UPDATE OF plan_source ON remote_chunks
FOR EACH ROW
WHEN OLD.plan_source IS NOT NEW.plan_source
BEGIN
    INSERT OR REPLACE INTO lto_txn_settings (name, value)
    VALUES ('lto.chunk_plan_transition_audit_write', 'on');
    INSERT INTO remote_chunk_plan_source_transitions
        (session_id, chunk_index, from_plan_source, to_plan_source,
         prior_plan_manifest_artifact_id, new_plan_manifest_artifact_id,
         plan4_gate_id, plan4_evidence_id, approval_identity,
         equivalence_confirmed)
    VALUES
        (OLD.session_id, OLD.chunk_index, OLD.plan_source,
         NEW.plan_source, OLD.plan_manifest_artifact_id,
         NEW.plan_manifest_artifact_id,
         (SELECT value FROM lto_txn_settings WHERE name='lto.chunk_plan_transition_gate_id'),
         (SELECT value FROM lto_txn_settings WHERE name='lto.chunk_plan_transition_evidence_id'),
         (SELECT value FROM lto_txn_settings WHERE name='lto.chunk_plan_transition_approval_identity'),
         1);
    DELETE FROM lto_txn_settings WHERE name = 'lto.chunk_plan_transition_audit_write';
END;

-- lto_validate_manifest_chunk_authority (immediate, not deferred)
CREATE TRIGGER trg_remote_chunks_manifest_authority_deferred
AFTER INSERT ON remote_chunks
FOR EACH ROW
WHEN NEW.plan_source = 'manifest'
BEGIN
    SELECT RAISE(ABORT, 'artifact is not the ready plan_manifest authority for its chunk')
    WHERE NOT EXISTS (
              SELECT 1 FROM archive_artifacts a
              WHERE a.artifact_id = NEW.plan_manifest_artifact_id
                AND a.session_id IS NEW.session_id
                AND a.chunk_index IS NEW.chunk_index
                AND a.artifact_kind = 'plan_manifest'
                AND a.container_id IS NULL
                AND a.readiness_state = 'ready'
                AND a.local_locator IS NOT NULL
                AND lower(substr(a.local_locator, -5)) <> '.part'
                AND a.artifact_size_bytes IS NOT NULL
                AND a.published_at IS NOT NULL);
END;

CREATE TRIGGER trg_remote_chunks_manifest_authority_deferred__upd
AFTER UPDATE OF plan_source, plan_manifest_artifact_id ON remote_chunks
FOR EACH ROW
WHEN NEW.plan_source = 'manifest'
BEGIN
    SELECT RAISE(ABORT, 'artifact is not the ready plan_manifest authority for its chunk')
    WHERE NOT EXISTS (
              SELECT 1 FROM archive_artifacts a
              WHERE a.artifact_id = NEW.plan_manifest_artifact_id
                AND a.session_id IS NEW.session_id
                AND a.chunk_index IS NEW.chunk_index
                AND a.artifact_kind = 'plan_manifest'
                AND a.container_id IS NULL
                AND a.readiness_state = 'ready'
                AND a.local_locator IS NOT NULL
                AND lower(substr(a.local_locator, -5)) <> '.part'
                AND a.artifact_size_bytes IS NOT NULL
                AND a.published_at IS NOT NULL);
END;

-- lto_guard_session_plan_transition
CREATE TRIGGER trg_remote_session_plan_transitions_guard
BEFORE INSERT ON remote_session_plan_transitions
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'remote session does not exist')
    WHERE NOT EXISTS (SELECT 1 FROM remote_sessions WHERE session_id = NEW.session_id);
    SELECT RAISE(ABORT, 'a session plan transition must begin as draft')
    WHERE NEW.state <> 'draft';
    SELECT RAISE(ABORT, 'transition epoch is not the next epoch for its session')
    WHERE NEW.transition_epoch <> (
        SELECT COALESCE(max(transition_epoch), 0) + 1
        FROM remote_session_plan_transitions
        WHERE session_id = NEW.session_id);
END;

CREATE TRIGGER trg_remote_session_plan_transitions_guard__del
BEFORE DELETE ON remote_session_plan_transitions
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'session plan transition epochs are append-preserved');
END;

CREATE TRIGGER trg_remote_session_plan_transitions_guard__upd
BEFORE UPDATE ON remote_session_plan_transitions
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'remote session does not exist')
    WHERE NOT EXISTS (SELECT 1 FROM remote_sessions WHERE session_id = NEW.session_id);
    SELECT RAISE(ABORT, 'session plan transition identity is immutable')
    WHERE (OLD.session_id IS NOT NEW.session_id
          OR OLD.transition_epoch IS NOT NEW.transition_epoch
          OR OLD.prior_plan_source IS NOT NEW.prior_plan_source
          OR OLD.new_plan_source IS NOT NEW.new_plan_source
          OR OLD.last_chunk_before_transition IS NOT NEW.last_chunk_before_transition
          OR OLD.first_chunk_after_transition IS NOT NEW.first_chunk_after_transition
          OR OLD.created_at IS NOT NEW.created_at);
    SELECT RAISE(ABORT, 'rehearsed transition evidence is immutable')
    WHERE OLD.state IN ('rehearsed','approved','active','rolled_back')
      AND (OLD.scan_frontier_generation IS NOT NEW.scan_frontier_generation
          OR OLD.evidence_report_locator IS NOT NEW.evidence_report_locator);
    SELECT RAISE(ABORT, 'approved transition identity is immutable')
    WHERE OLD.state IN ('approved','active','rolled_back')
      AND (OLD.approval_identity IS NOT NEW.approval_identity
          OR OLD.approved_at IS NOT NEW.approved_at);
    SELECT RAISE(ABORT, 'illegal session plan transition state change')
    WHERE NEW.state <> OLD.state
      AND NOT ((OLD.state = 'draft' AND NEW.state = 'rehearsed')
            OR (OLD.state = 'rehearsed' AND NEW.state = 'approved')
            OR (OLD.state = 'approved' AND NEW.state = 'active')
            OR (OLD.state = 'active' AND NEW.state = 'rolled_back'));
    SELECT RAISE(ABORT, 'active plan transition boundary does not follow the current maximum chunk')
    WHERE OLD.state = 'approved' AND NEW.state = 'active'
      AND (SELECT max(chunk_index) FROM remote_chunks
           WHERE session_id = NEW.session_id)
          IS NOT NEW.last_chunk_before_transition;
    SELECT RAISE(ABORT, 'active transition boundary must advance monotonically')
    WHERE OLD.state = 'approved' AND NEW.state = 'active'
      AND NEW.first_chunk_after_transition < (
          SELECT max(first_chunk_after_transition)
          FROM remote_session_plan_transitions
          WHERE session_id = NEW.session_id
            AND transition_id <> NEW.transition_id
            AND state IN ('active','rolled_back'));
    SELECT RAISE(ABORT, 'transition prior plan source conflicts with active boundary history')
    WHERE OLD.state = 'approved' AND NEW.state = 'active'
      AND COALESCE(
          (SELECT t.new_plan_source
           FROM remote_session_plan_transitions t
           WHERE t.session_id = NEW.session_id
             AND t.transition_id <> NEW.transition_id
             AND t.state IN ('active','rolled_back')
             AND t.first_chunk_after_transition <=
                 NEW.first_chunk_after_transition
           ORDER BY t.first_chunk_after_transition DESC,
                    t.transition_epoch DESC
           LIMIT 1),
          'legacy_db') IS NOT NEW.prior_plan_source;
    SELECT RAISE(ABORT, 'transition boundary history conflicts with existing chunk plan source(s)')
    WHERE OLD.state = 'approved' AND NEW.state = 'active'
      AND EXISTS (
          SELECT 1 FROM remote_chunks c
          WHERE c.session_id = NEW.session_id
            AND c.chunk_index < NEW.first_chunk_after_transition
            AND c.plan_source IS NOT COALESCE(
                (SELECT history.new_plan_source
                 FROM remote_session_plan_transitions history
                 WHERE history.session_id = c.session_id
                   AND history.transition_id <> NEW.transition_id
                   AND history.state IN ('active','rolled_back')
                   AND history.first_chunk_after_transition <= c.chunk_index
                 ORDER BY history.first_chunk_after_transition DESC,
                          history.transition_epoch DESC
                 LIMIT 1),
                'legacy_db')
            AND NOT EXISTS (
                SELECT 1 FROM remote_chunk_plan_source_transitions audit
                WHERE audit.session_id = c.session_id
                  AND audit.chunk_index = c.chunk_index
                  AND audit.to_plan_source = c.plan_source
                  AND audit.new_plan_manifest_artifact_id =
                      c.plan_manifest_artifact_id));
END;

CREATE TRIGGER trg_remote_session_plan_transitions_guard__touch
AFTER UPDATE ON remote_session_plan_transitions
FOR EACH ROW
WHEN NEW.updated_at IS OLD.updated_at
BEGIN
    UPDATE remote_session_plan_transitions SET updated_at = (strftime('%Y-%m-%d %H:%M:%f','now')||'000+00:00')
    WHERE transition_id = NEW.transition_id;
END;

-- lto_guard_directory_archive_part
CREATE TRIGGER trg_directory_archive_parts_guard
BEFORE INSERT ON directory_archive_parts
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'directory part tape label does not match its tape generation')
    WHERE (SELECT volume_label FROM tape_generations
        WHERE generation_id = NEW.tape_generation_id) IS NOT NEW.tape_label;
    SELECT RAISE(ABORT, 'directory part route conflicts with container format')
    WHERE NEW.container_id IS NOT NULL
      AND (NOT EXISTS (SELECT 1 FROM archive_containers c
                       WHERE c.container_id = NEW.container_id)
           OR (SELECT c.container_format FROM archive_containers c WHERE c.container_id = NEW.container_id) IS NOT NEW.restore_format);
    SELECT RAISE(ABORT, 'directory part route conflicts with container tape label')
    WHERE NEW.container_id IS NOT NULL
      AND (SELECT c.tape_label FROM archive_containers c WHERE c.container_id = NEW.container_id) IS NOT NULL
      AND (SELECT c.tape_label FROM archive_containers c WHERE c.container_id = NEW.container_id) IS NOT NEW.tape_label;
    SELECT RAISE(ABORT, 'directory part route conflicts with container tape generation')
    WHERE NEW.container_id IS NOT NULL
      AND (SELECT c.tape_generation_id FROM archive_containers c WHERE c.container_id = NEW.container_id) IS NOT NULL
      AND (SELECT c.tape_generation_id FROM archive_containers c WHERE c.container_id = NEW.container_id) IS NOT NEW.tape_generation_id;
    SELECT RAISE(ABORT, 'exact directory part route conflicts with container stored path')
    WHERE NEW.container_id IS NOT NULL
      AND NEW.routing_precision = 'exact'
      AND (SELECT c.tape_path FROM archive_containers c WHERE c.container_id = NEW.container_id) IS NOT NULL
      AND (SELECT c.tape_path FROM archive_containers c WHERE c.container_id = NEW.container_id) IS NOT NEW.stored_path;
    SELECT RAISE(ABORT, 'directory loose part conflicts with its catalog record')
    WHERE NEW.container_id IS NULL
      AND (NOT EXISTS (SELECT 1 FROM files_index f
                       WHERE f.record_key = NEW.loose_record_key)
           OR (SELECT f.tape_label FROM files_index f WHERE f.record_key = NEW.loose_record_key) IS NOT NEW.tape_label);
    SELECT RAISE(ABORT, 'directory loose part conflicts with remote chunk provenance')
    WHERE NEW.container_id IS NULL
      AND (SELECT f.remote_session_id FROM files_index f WHERE f.record_key = NEW.loose_record_key) IS NOT NULL
      AND ((SELECT f.remote_session_id FROM files_index f WHERE f.record_key = NEW.loose_record_key) IS NOT NEW.session_id
           OR (SELECT f.remote_chunk_index FROM files_index f WHERE f.record_key = NEW.loose_record_key) IS NOT NEW.chunk_index);
    SELECT RAISE(ABORT, 'exact loose restore route conflicts with catalog stored path')
    WHERE NEW.container_id IS NULL
      AND NEW.routing_precision = 'exact'
      AND (SELECT f.stored_path FROM files_index f WHERE f.record_key = NEW.loose_record_key) IS NOT NEW.stored_path;
    SELECT RAISE(ABORT, 'artifact is not the ready plan_manifest authority for its chunk')
    WHERE NEW.plan_manifest_artifact_id IS NOT NULL
      AND NOT EXISTS (
              SELECT 1 FROM archive_artifacts a
              WHERE a.artifact_id = NEW.plan_manifest_artifact_id
                AND a.session_id IS NEW.session_id
                AND a.chunk_index IS NEW.chunk_index
                AND a.artifact_kind = 'plan_manifest'
                AND a.container_id IS NULL
                AND a.readiness_state = 'ready'
                AND a.local_locator IS NOT NULL
                AND lower(substr(a.local_locator, -5)) <> '.part'
                AND a.artifact_size_bytes IS NOT NULL
                AND a.published_at IS NOT NULL);
    SELECT RAISE(ABORT, 'directory part plan artifact locator conflicts with artifact authority')
    WHERE NEW.plan_manifest_artifact_id IS NOT NULL
      AND NEW.plan_artifact_locator IS NOT NULL
      AND NEW.plan_artifact_locator IS NOT (SELECT a.local_locator FROM archive_artifacts a WHERE a.artifact_id = NEW.plan_manifest_artifact_id);
    SELECT RAISE(ABORT, 'artifact is not the ready terminal_manifest authority for its chunk')
    WHERE NEW.terminal_manifest_artifact_id IS NOT NULL
      AND NOT EXISTS (
              SELECT 1 FROM archive_artifacts a
              WHERE a.artifact_id = NEW.terminal_manifest_artifact_id
                AND a.session_id IS NEW.session_id
                AND a.chunk_index IS NEW.chunk_index
                AND a.artifact_kind = 'terminal_manifest'
                AND a.container_id IS NULL
                AND a.readiness_state = 'ready'
                AND a.local_locator IS NOT NULL
                AND lower(substr(a.local_locator, -5)) <> '.part'
                AND a.artifact_size_bytes IS NOT NULL
                AND a.published_at IS NOT NULL);
    SELECT RAISE(ABORT, 'directory part terminal artifact locator conflicts with artifact authority')
    WHERE NEW.terminal_manifest_artifact_id IS NOT NULL
      AND NEW.terminal_artifact_locator IS NOT NULL
      AND NEW.terminal_artifact_locator IS NOT (SELECT a.local_locator FROM archive_artifacts a WHERE a.artifact_id = NEW.terminal_manifest_artifact_id);
    SELECT RAISE(ABORT, 'directory part sidecar is not ready for its container')
    WHERE NEW.sidecar_artifact_id IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM archive_artifacts a
          WHERE a.artifact_id = NEW.sidecar_artifact_id
            AND a.session_id IS NEW.session_id
            AND a.chunk_index IS NEW.chunk_index
            AND a.container_id IS NEW.container_id
            AND a.artifact_kind IN ('zip_manifest','tar_sidecar')
            AND a.readiness_state = 'ready'
            AND a.local_locator IS NOT NULL
            AND lower(substr(a.local_locator, -5)) <> '.part');
    SELECT RAISE(ABORT, 'directory part sidecar kind conflicts with restore format')
    WHERE NEW.sidecar_artifact_id IS NOT NULL
      AND ((NEW.restore_format = 'zip'
            AND (SELECT a.artifact_kind FROM archive_artifacts a WHERE a.artifact_id = NEW.sidecar_artifact_id) <> 'zip_manifest')
           OR (NEW.restore_format = 'stored_tar'
               AND (SELECT a.artifact_kind FROM archive_artifacts a WHERE a.artifact_id = NEW.sidecar_artifact_id) <> 'tar_sidecar'));
    SELECT RAISE(ABORT, 'directory part sidecar locator conflicts with artifact authority')
    WHERE NEW.sidecar_artifact_id IS NOT NULL
      AND NEW.sidecar_artifact_locator IS NOT NULL
      AND NEW.sidecar_artifact_locator IS NOT (SELECT a.local_locator FROM archive_artifacts a WHERE a.artifact_id = NEW.sidecar_artifact_id);
END;

CREATE TRIGGER trg_directory_archive_parts_guard__upd
BEFORE UPDATE ON directory_archive_parts
FOR EACH ROW
BEGIN
    SELECT RAISE(ABORT, 'directory part tape label does not match its tape generation')
    WHERE (SELECT volume_label FROM tape_generations
        WHERE generation_id = NEW.tape_generation_id) IS NOT NEW.tape_label;
    SELECT RAISE(ABORT, 'directory part route conflicts with container format')
    WHERE NEW.container_id IS NOT NULL
      AND (NOT EXISTS (SELECT 1 FROM archive_containers c
                       WHERE c.container_id = NEW.container_id)
           OR (SELECT c.container_format FROM archive_containers c WHERE c.container_id = NEW.container_id) IS NOT NEW.restore_format);
    SELECT RAISE(ABORT, 'directory part route conflicts with container tape label')
    WHERE NEW.container_id IS NOT NULL
      AND (SELECT c.tape_label FROM archive_containers c WHERE c.container_id = NEW.container_id) IS NOT NULL
      AND (SELECT c.tape_label FROM archive_containers c WHERE c.container_id = NEW.container_id) IS NOT NEW.tape_label;
    SELECT RAISE(ABORT, 'directory part route conflicts with container tape generation')
    WHERE NEW.container_id IS NOT NULL
      AND (SELECT c.tape_generation_id FROM archive_containers c WHERE c.container_id = NEW.container_id) IS NOT NULL
      AND (SELECT c.tape_generation_id FROM archive_containers c WHERE c.container_id = NEW.container_id) IS NOT NEW.tape_generation_id;
    SELECT RAISE(ABORT, 'exact directory part route conflicts with container stored path')
    WHERE NEW.container_id IS NOT NULL
      AND NEW.routing_precision = 'exact'
      AND (SELECT c.tape_path FROM archive_containers c WHERE c.container_id = NEW.container_id) IS NOT NULL
      AND (SELECT c.tape_path FROM archive_containers c WHERE c.container_id = NEW.container_id) IS NOT NEW.stored_path;
    SELECT RAISE(ABORT, 'directory loose part conflicts with its catalog record')
    WHERE NEW.container_id IS NULL
      AND (NOT EXISTS (SELECT 1 FROM files_index f
                       WHERE f.record_key = NEW.loose_record_key)
           OR (SELECT f.tape_label FROM files_index f WHERE f.record_key = NEW.loose_record_key) IS NOT NEW.tape_label);
    SELECT RAISE(ABORT, 'directory loose part conflicts with remote chunk provenance')
    WHERE NEW.container_id IS NULL
      AND (SELECT f.remote_session_id FROM files_index f WHERE f.record_key = NEW.loose_record_key) IS NOT NULL
      AND ((SELECT f.remote_session_id FROM files_index f WHERE f.record_key = NEW.loose_record_key) IS NOT NEW.session_id
           OR (SELECT f.remote_chunk_index FROM files_index f WHERE f.record_key = NEW.loose_record_key) IS NOT NEW.chunk_index);
    SELECT RAISE(ABORT, 'exact loose restore route conflicts with catalog stored path')
    WHERE NEW.container_id IS NULL
      AND NEW.routing_precision = 'exact'
      AND (SELECT f.stored_path FROM files_index f WHERE f.record_key = NEW.loose_record_key) IS NOT NEW.stored_path;
    SELECT RAISE(ABORT, 'artifact is not the ready plan_manifest authority for its chunk')
    WHERE NEW.plan_manifest_artifact_id IS NOT NULL
      AND NOT EXISTS (
              SELECT 1 FROM archive_artifacts a
              WHERE a.artifact_id = NEW.plan_manifest_artifact_id
                AND a.session_id IS NEW.session_id
                AND a.chunk_index IS NEW.chunk_index
                AND a.artifact_kind = 'plan_manifest'
                AND a.container_id IS NULL
                AND a.readiness_state = 'ready'
                AND a.local_locator IS NOT NULL
                AND lower(substr(a.local_locator, -5)) <> '.part'
                AND a.artifact_size_bytes IS NOT NULL
                AND a.published_at IS NOT NULL);
    SELECT RAISE(ABORT, 'directory part plan artifact locator conflicts with artifact authority')
    WHERE NEW.plan_manifest_artifact_id IS NOT NULL
      AND NEW.plan_artifact_locator IS NOT NULL
      AND NEW.plan_artifact_locator IS NOT (SELECT a.local_locator FROM archive_artifacts a WHERE a.artifact_id = NEW.plan_manifest_artifact_id);
    SELECT RAISE(ABORT, 'artifact is not the ready terminal_manifest authority for its chunk')
    WHERE NEW.terminal_manifest_artifact_id IS NOT NULL
      AND NOT EXISTS (
              SELECT 1 FROM archive_artifacts a
              WHERE a.artifact_id = NEW.terminal_manifest_artifact_id
                AND a.session_id IS NEW.session_id
                AND a.chunk_index IS NEW.chunk_index
                AND a.artifact_kind = 'terminal_manifest'
                AND a.container_id IS NULL
                AND a.readiness_state = 'ready'
                AND a.local_locator IS NOT NULL
                AND lower(substr(a.local_locator, -5)) <> '.part'
                AND a.artifact_size_bytes IS NOT NULL
                AND a.published_at IS NOT NULL);
    SELECT RAISE(ABORT, 'directory part terminal artifact locator conflicts with artifact authority')
    WHERE NEW.terminal_manifest_artifact_id IS NOT NULL
      AND NEW.terminal_artifact_locator IS NOT NULL
      AND NEW.terminal_artifact_locator IS NOT (SELECT a.local_locator FROM archive_artifacts a WHERE a.artifact_id = NEW.terminal_manifest_artifact_id);
    SELECT RAISE(ABORT, 'directory part sidecar is not ready for its container')
    WHERE NEW.sidecar_artifact_id IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM archive_artifacts a
          WHERE a.artifact_id = NEW.sidecar_artifact_id
            AND a.session_id IS NEW.session_id
            AND a.chunk_index IS NEW.chunk_index
            AND a.container_id IS NEW.container_id
            AND a.artifact_kind IN ('zip_manifest','tar_sidecar')
            AND a.readiness_state = 'ready'
            AND a.local_locator IS NOT NULL
            AND lower(substr(a.local_locator, -5)) <> '.part');
    SELECT RAISE(ABORT, 'directory part sidecar kind conflicts with restore format')
    WHERE NEW.sidecar_artifact_id IS NOT NULL
      AND ((NEW.restore_format = 'zip'
            AND (SELECT a.artifact_kind FROM archive_artifacts a WHERE a.artifact_id = NEW.sidecar_artifact_id) <> 'zip_manifest')
           OR (NEW.restore_format = 'stored_tar'
               AND (SELECT a.artifact_kind FROM archive_artifacts a WHERE a.artifact_id = NEW.sidecar_artifact_id) <> 'tar_sidecar'));
    SELECT RAISE(ABORT, 'directory part sidecar locator conflicts with artifact authority')
    WHERE NEW.sidecar_artifact_id IS NOT NULL
      AND NEW.sidecar_artifact_locator IS NOT NULL
      AND NEW.sidecar_artifact_locator IS NOT (SELECT a.local_locator FROM archive_artifacts a WHERE a.artifact_id = NEW.sidecar_artifact_id);
END;

CREATE TRIGGER trg_directory_archive_parts_guard__touch
AFTER UPDATE ON directory_archive_parts
FOR EACH ROW
WHEN NEW.updated_at IS OLD.updated_at
BEGIN
    UPDATE directory_archive_parts SET updated_at = (strftime('%Y-%m-%d %H:%M:%f','now')||'000+00:00')
    WHERE part_id = NEW.part_id;
END;

-- ===== Migration 020: chunk status transition guard =====

-- lto_guard_chunk_status_transition
CREATE TRIGGER trg_remote_chunks_status_transition
BEFORE UPDATE OF status ON remote_chunks
FOR EACH ROW
WHEN OLD.status IS NOT NEW.status
     AND (OLD.status = 'done'
          OR (OLD.status = 'backing' AND NEW.status <> 'done'))
BEGIN
    SELECT RAISE(ABORT, 'remote chunk may not move backwards from backing/done: the chunk may already be on tape. An operator recovery must record a named override in remote_chunk_transition_overrides in the same transaction.')
    WHERE (SELECT min(o.override_id)
         FROM remote_chunk_transition_overrides o
         WHERE o.session_id = OLD.session_id
           AND o.chunk_index = OLD.chunk_index
           AND o.from_status = OLD.status
           AND o.to_status = NEW.status
           AND o.consumed_at IS NULL) IS NULL;
    UPDATE remote_chunk_transition_overrides
    SET consumed_at = (strftime('%Y-%m-%d %H:%M:%f','now')||'000+00:00')
    WHERE override_id = (SELECT min(o.override_id)
         FROM remote_chunk_transition_overrides o
         WHERE o.session_id = OLD.session_id
           AND o.chunk_index = OLD.chunk_index
           AND o.from_status = OLD.status
           AND o.to_status = NEW.status
           AND o.consumed_at IS NULL);
END;

