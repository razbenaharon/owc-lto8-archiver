-- =============================================================================
-- 018 - Manifest plan authority and normalized directory catalog.
-- =============================================================================
--
-- EXPLICIT-ONLY. PgConnectionCore.apply_manifest_directory_catalog_schema()
-- owns one transaction, supplies the SHA-256 identity of this file through a
-- transaction-local setting, and executes every DDL and backfill statement.
-- This migration is deliberately absent from PgConnectionCore._init_schema().
--
-- The migration is additive. Existing plan membership, chunk ordinals,
-- packaging formats, containers, tape locators, and the legacy directory
-- catalog are neither inferred nor rewritten.
-- =============================================================================

-- The authorization check precedes every schema mutation. Direct execution is
-- refused so a caller cannot accidentally apply only part of the file.
DO $$
DECLARE
    supplied_checksum TEXT := nullif(current_setting(
        'lto.manifest_directory_catalog_migration_checksum', true), '');
BEGIN
    IF supplied_checksum IS NULL
       OR supplied_checksum !~ '^[0-9a-f]{64}$' THEN
        RAISE EXCEPTION
            'migration 018 must be applied through the explicit guarded command';
    END IF;
END $$;

-- Required Plan 1/Plan 2 authority. Missing prerequisites fail; nothing is
-- silently skipped or synthesized.
DO $$
DECLARE
    optional_007_count INTEGER;
    optional_012_count INTEGER;
    required_marker_count INTEGER;
BEGIN
    IF to_regclass('public.remote_sessions') IS NULL
       OR to_regclass('public.remote_chunks') IS NULL
       OR to_regclass('public.remote_plan_files') IS NULL
       OR to_regclass('public.remote_snapshot_files') IS NULL
       OR to_regclass('public.remote_scan_scopes') IS NULL
       OR to_regclass('public.remote_worker_attempts') IS NULL
       OR to_regclass('public.archive_containers') IS NULL
       OR to_regclass('public.archive_artifacts') IS NULL
       OR to_regclass('public.archive_container_members') IS NULL
       OR to_regclass('public.tape_generations') IS NULL
       OR to_regclass('public.files_index') IS NULL THEN
        RAISE EXCEPTION
            'migration 018 requires finalized migration 014 and migrations 015-017';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE schemaname='public'
          AND indexname='uq_remote_plan_files_chunk_ordinal'
    ) THEN
        RAISE EXCEPTION
            'migration 018 requires finalized migration 014 membership';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='remote_sessions'::regclass
          AND conname='uq_remote_sessions_label'
    ) THEN
        RAISE EXCEPTION
            'migration 018 requires stable unique remote session labels';
    END IF;

    IF to_regclass('public.container_format_schema_metadata') IS NULL
       OR (SELECT count(*) FROM container_format_schema_metadata) <> 1
       OR to_regclass('public.remote_packaging_boundaries') IS NULL
       OR to_regclass('public.remote_packaging_boundary_chunks') IS NULL THEN
        RAISE EXCEPTION 'migration 018 requires guarded migration 015';
    END IF;

    -- Do not compare migration 015's original broad fingerprint here. The
    -- legitimate 016/017 migrations add archive_containers columns and replace
    -- constraints that 015 included wholesale in that fingerprint. Instead,
    -- the prerequisite checks above and below require every 015-017 authority
    -- object that 018 consumes.

    SELECT count(*) INTO required_marker_count
    FROM information_schema.columns
    WHERE table_schema='public' AND (
        (table_name='remote_sessions'
         AND column_name='default_packaging_format')
        OR (table_name='remote_chunks' AND column_name IN
            ('packaging_format','packaging_assigned_at',
             'writer_started_at','writer_completed_at',
             'catalog_committed_at'))
        OR (table_name='archive_bundles' AND column_name IN
            ('container_id','container_format'))
        OR (table_name='archive_runs' AND column_name IN
            ('remote_chunk_index','tape_generation_id')));
    IF required_marker_count <> 10
       OR NOT EXISTS (
            SELECT 1 FROM pg_trigger
            WHERE tgrelid='remote_chunks'::regclass
              AND tgname='trg_remote_chunks_packaging_write_once'
              AND tgenabled<>'D')
       OR NOT EXISTS (
            SELECT 1 FROM pg_trigger
            WHERE tgrelid='remote_chunks'::regclass
              AND tgname='trg_remote_chunks_initial_format'
              AND tgenabled<>'D') THEN
        RAISE EXCEPTION
            'migration 018 refuses a partial migration 015 installation';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='public' AND table_name='remote_chunks'
          AND column_name='stored_tar_max_size_bytes'
    ) OR NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='public' AND table_name='archive_containers'
          AND column_name='estimated_archive_bytes'
    ) THEN
        RAISE EXCEPTION 'migration 018 requires migration 016';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='archive_container_members'::regclass
          AND conname='archive_container_members_pkey')
       OR NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='archive_container_members'::regclass
          AND contype='u'
          AND pg_get_constraintdef(oid, true)=
              'UNIQUE (session_id, chunk_index, plan_ordinal)')
       OR NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='archive_container_members'::regclass
          AND conname='archive_container_members_container_ck')
       OR to_regclass(
            'public.idx_archive_container_members_container') IS NULL THEN
        RAISE EXCEPTION
            'migration 018 refuses a partial migration 016 installation';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='public' AND table_name='archive_containers'
          AND column_name='validated_part_locator'
    ) OR NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='public' AND table_name='archive_containers'
          AND column_name='validation_summary'
    ) OR NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='public' AND table_name='archive_containers'
          AND column_name='disposition_counts'
    ) THEN
        RAISE EXCEPTION 'migration 018 requires migration 017';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='archive_containers'::regclass
          AND conname='archive_containers_observed_ck')
       OR NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='archive_containers'::regclass
          AND conname='archive_containers_validated_part_ck')
       OR NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='archive_containers'::regclass
          AND conname='archive_containers_ready_pair_ck') THEN
        RAISE EXCEPTION
            'migration 018 refuses a partial migration 017 installation';
    END IF;

    SELECT count(*) INTO optional_007_count
    FROM (VALUES ('directory_archive_stats'),
                 ('directory_archive_bundles'),
                 ('directory_tree_index')) AS wanted(name)
    WHERE to_regclass('public.' || wanted.name) IS NOT NULL;
    IF optional_007_count NOT IN (0, 3) THEN
        RAISE EXCEPTION
            'migration 007 is partially installed (% of 3 tables); refusing migration 018',
            optional_007_count;
    END IF;
    IF optional_007_count=3 THEN
        SELECT count(*) INTO required_marker_count
        FROM information_schema.columns
        WHERE table_schema='public'
          AND table_name='directory_archive_bundles'
          AND column_name IN
              ('container_id','container_format','tape_generation_id',
               'actual_artifact_bytes');
        IF required_marker_count <> 4 THEN
            RAISE EXCEPTION
                'migration 007 exists without migration 015 compatibility columns; refusing migration 018';
        END IF;
    END IF;

    SELECT count(*) INTO optional_012_count
    FROM (VALUES ('tape_write_batches'),
                 ('tape_write_batch_chunks'),
                 ('tape_write_active_chunk')) AS wanted(name)
    WHERE to_regclass('public.' || wanted.name) IS NOT NULL;
    IF optional_012_count NOT IN (0, 3) THEN
        RAISE EXCEPTION
            'migration 012 is partially installed (% of 3 tables); refusing migration 018',
            optional_012_count;
    END IF;
END $$;

-- A first application starts only from the pristine pre-018 shape. IF NOT
-- EXISTS supports reapplying an already-authorized installation; it never
-- adopts hand-created fragments.
DO $$
DECLARE
    marker_count INTEGER;
    metadata_rows INTEGER;
    fingerprint_matches BOOLEAN;
BEGIN
    marker_count :=
        (CASE WHEN to_regclass('public.remote_session_plan_transitions')
                   IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN to_regclass('public.remote_chunk_plan_source_transitions')
                   IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN to_regclass('public.archive_directories')
                   IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN to_regclass('public.directory_scan_coverage')
                   IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN to_regclass('public.directory_archive_parts')
                   IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN to_regclass('public.directory_completeness')
                   IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN to_regclass('public.directory_catalog_status_v')
                   IS NOT NULL THEN 1 ELSE 0 END) +
        (SELECT count(*) FROM information_schema.columns
         WHERE table_schema='public' AND table_name='remote_chunks'
           AND column_name IN
               ('plan_source','plan_manifest_artifact_id',
                'terminal_manifest_artifact_id','plan_ordinal_scope',
                'final_archived_count','final_source_missing_count',
                'final_source_permission_denied_count',
                'final_source_unreadable_count','final_source_changed_count',
                'final_unresolved_count','final_archived_bytes'));

    IF to_regclass(
            'public.manifest_directory_catalog_schema_metadata') IS NULL THEN
        IF marker_count <> 0
           OR EXISTS (
                SELECT 1 FROM pg_proc p
                JOIN pg_namespace n ON n.oid=p.pronamespace
                WHERE n.nspname='public' AND p.proname LIKE 'lto_%'
                  AND p.proname IN
                    ('lto_assert_ready_chunk_artifact',
                     'lto_effective_chunk_plan_source',
                     'lto_guard_chunk_plan_source',
                     'lto_transition_chunk_plan_source',
                     'lto_validate_manifest_chunk_authority',
                     'lto_guard_session_plan_transition',
                     'lto_reject_chunk_plan_transition_mutation',
                     'lto_guard_sealed_artifact',
                     'lto_guard_archive_directory',
                     'lto_guard_directory_scan_coverage',
                     'lto_guard_directory_archive_part',
                     'lto_manifest_directory_catalog_schema_fingerprint',
                     'lto_guard_manifest_directory_catalog_metadata'))
           OR EXISTS (
                SELECT 1 FROM pg_trigger t
                JOIN pg_class c ON c.oid=t.tgrelid
                JOIN pg_namespace n ON n.oid=c.relnamespace
                WHERE n.nspname='public' AND NOT t.tgisinternal
                  AND t.tgname IN
                    ('trg_archive_directories_parent_guard',
                     'trg_directory_scan_coverage_scope_guard',
                     'trg_directory_archive_parts_guard',
                     'trg_archive_artifacts_sealed_immutable',
                     'trg_remote_session_plan_transitions_guard',
                     'trg_remote_chunk_plan_source_transitions_immutable',
                     'trg_remote_chunks_plan_source_guard',
                     'trg_remote_chunks_manifest_authority_deferred',
                     'trg_manifest_directory_catalog_schema_metadata_immutable'))
           OR EXISTS (
                SELECT 1 FROM pg_class c
                JOIN pg_namespace n ON n.oid=c.relnamespace
                WHERE n.nspname='public' AND c.relname IN
                    ('uq_remote_chunks_plan_manifest_artifact',
                     'uq_remote_chunks_terminal_manifest_artifact',
                     'uq_remote_session_plan_active_boundary',
                     'uq_remote_chunk_plan_source_transition',
                     'uq_directory_archive_parts_container',
                     'uq_directory_archive_parts_loose',
                     'uq_archive_artifacts_ready_plan_chunk')) THEN
            RAISE EXCEPTION
                'partial migration 018 markers exist without schema metadata';
        END IF;
    ELSE
        EXECUTE
            'SELECT count(*) FROM manifest_directory_catalog_schema_metadata'
            INTO metadata_rows;
        IF metadata_rows <> 1 THEN
            RAISE EXCEPTION
                'migration 018 metadata exists without exactly one authority row';
        END IF;
        IF marker_count <> 18 THEN
            RAISE EXCEPTION
                'installed migration 018 is partial (% of 18 core markers)',
                marker_count;
        END IF;
        IF to_regprocedure(
              'public.lto_manifest_directory_catalog_schema_fingerprint()')
              IS NULL THEN
            RAISE EXCEPTION
                'installed migration 018 lacks schema fingerprint authority';
        END IF;
        EXECUTE $sql$
            SELECT schema_fingerprint =
                   lto_manifest_directory_catalog_schema_fingerprint()
            FROM manifest_directory_catalog_schema_metadata WHERE singleton
        $sql$ INTO fingerprint_matches;
        IF fingerprint_matches IS NOT TRUE THEN
            RAISE EXCEPTION
                'installed migration 018 catalog definition has drifted';
        END IF;
    END IF;
END $$;

-- Serialize the backfill and transition-policy installation with all mutable
-- evidence tables. The explicit command separately performs process/lock
-- preflight; these locks close the database race inside that preflight.
LOCK TABLE remote_sessions IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE remote_chunks IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE remote_plan_files IN SHARE MODE;
LOCK TABLE remote_snapshot_files IN SHARE MODE;
LOCK TABLE remote_scan_scopes IN SHARE MODE;
LOCK TABLE remote_worker_attempts IN SHARE MODE;
LOCK TABLE archive_containers IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE archive_artifacts IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE archive_container_members IN SHARE MODE;
LOCK TABLE tape_generations IN SHARE MODE;
LOCK TABLE files_index IN SHARE MODE;

DO $$
BEGIN
    IF to_regclass(
            'public.manifest_directory_catalog_schema_metadata') IS NOT NULL THEN
        LOCK TABLE manifest_directory_catalog_schema_metadata
            IN SHARE ROW EXCLUSIVE MODE;
        LOCK TABLE remote_session_plan_transitions
            IN SHARE ROW EXCLUSIVE MODE;
        LOCK TABLE remote_chunk_plan_source_transitions
            IN SHARE ROW EXCLUSIVE MODE;
        LOCK TABLE archive_directories IN SHARE ROW EXCLUSIVE MODE;
        LOCK TABLE directory_scan_coverage IN SHARE ROW EXCLUSIVE MODE;
        LOCK TABLE directory_archive_parts IN SHARE ROW EXCLUSIVE MODE;
        LOCK TABLE directory_completeness IN SHARE ROW EXCLUSIVE MODE;
    END IF;
    IF to_regclass('public.tape_write_batches') IS NOT NULL THEN
        LOCK TABLE tape_write_batches IN SHARE MODE;
        LOCK TABLE tape_write_batch_chunks IN SHARE MODE;
        LOCK TABLE tape_write_active_chunk IN SHARE MODE;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS manifest_directory_catalog_schema_metadata (
    singleton          BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    schema_version     INTEGER NOT NULL,
    migration_checksum TEXT NOT NULL CHECK (length(migration_checksum)=64),
    schema_fingerprint TEXT NOT NULL CHECK (length(schema_fingerprint)=32),
    applied_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

DO $$
DECLARE
    supplied_checksum TEXT := current_setting(
        'lto.manifest_directory_catalog_migration_checksum');
    installed_checksum TEXT;
BEGIN
    SELECT migration_checksum INTO installed_checksum
    FROM manifest_directory_catalog_schema_metadata WHERE singleton;
    IF installed_checksum IS NOT NULL
       AND installed_checksum <> supplied_checksum THEN
        RAISE EXCEPTION
            'migration 018 checksum drift: installed %, supplied %',
            installed_checksum, supplied_checksum;
    END IF;
END $$;

-- ---------------------------------------------------------------------------
-- Chunk plan authority and terminal-disposition aggregates.
-- ---------------------------------------------------------------------------
ALTER TABLE remote_chunks
    ADD COLUMN IF NOT EXISTS plan_source TEXT,
    ADD COLUMN IF NOT EXISTS plan_manifest_artifact_id BIGINT,
    ADD COLUMN IF NOT EXISTS terminal_manifest_artifact_id BIGINT,
    ADD COLUMN IF NOT EXISTS plan_ordinal_scope TEXT NOT NULL DEFAULT 'chunk',
    ADD COLUMN IF NOT EXISTS final_archived_count BIGINT,
    ADD COLUMN IF NOT EXISTS final_source_missing_count BIGINT,
    ADD COLUMN IF NOT EXISTS final_source_permission_denied_count BIGINT,
    ADD COLUMN IF NOT EXISTS final_source_unreadable_count BIGINT,
    ADD COLUMN IF NOT EXISTS final_source_changed_count BIGINT,
    ADD COLUMN IF NOT EXISTS final_unresolved_count BIGINT,
    ADD COLUMN IF NOT EXISTS final_archived_bytes BIGINT;

-- This is the only legacy backfill in migration 018. Packaging format and all
-- membership/ordinal evidence are deliberately untouched.
UPDATE remote_chunks
SET plan_source='legacy_db'
WHERE plan_source IS NULL;

ALTER TABLE remote_chunks
    ALTER COLUMN plan_source SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='remote_chunks'::regclass
          AND conname='remote_chunks_plan_source_ck'
    ) THEN
        ALTER TABLE remote_chunks ADD CONSTRAINT remote_chunks_plan_source_ck
            CHECK (plan_source IN ('legacy_db','manifest'));
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='remote_chunks'::regclass
          AND conname='remote_chunks_plan_ordinal_scope_ck'
    ) THEN
        ALTER TABLE remote_chunks
            ADD CONSTRAINT remote_chunks_plan_ordinal_scope_ck
            CHECK (plan_ordinal_scope='chunk');
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='remote_chunks'::regclass
          AND conname='remote_chunks_final_disposition_nonnegative_ck'
    ) THEN
        ALTER TABLE remote_chunks
            ADD CONSTRAINT remote_chunks_final_disposition_nonnegative_ck
            CHECK (
                (final_archived_count IS NULL OR final_archived_count >= 0)
                AND (final_source_missing_count IS NULL
                     OR final_source_missing_count >= 0)
                AND (final_source_permission_denied_count IS NULL
                     OR final_source_permission_denied_count >= 0)
                AND (final_source_unreadable_count IS NULL
                     OR final_source_unreadable_count >= 0)
                AND (final_source_changed_count IS NULL
                     OR final_source_changed_count >= 0)
                AND (final_unresolved_count IS NULL
                     OR final_unresolved_count >= 0)
                AND (final_archived_bytes IS NULL
                     OR final_archived_bytes >= 0));
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='remote_chunks'::regclass
          AND conname='remote_chunks_final_disposition_complete_ck'
    ) THEN
        ALTER TABLE remote_chunks
            ADD CONSTRAINT remote_chunks_final_disposition_complete_ck
            CHECK (
                num_nonnulls(
                    final_archived_count, final_source_missing_count,
                    final_source_permission_denied_count,
                    final_source_unreadable_count, final_source_changed_count,
                    final_unresolved_count, final_archived_bytes) IN (0, 7)
                AND (final_archived_count IS NULL
                     OR expected_file_count IS NULL
                     OR final_archived_count + final_source_missing_count
                        + final_source_permission_denied_count
                        + final_source_unreadable_count
                        + final_source_changed_count
                        + final_unresolved_count = expected_file_count));
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='remote_chunks'::regclass
          AND conname='remote_chunks_plan_manifest_artifact_fk'
    ) THEN
        ALTER TABLE remote_chunks
            ADD CONSTRAINT remote_chunks_plan_manifest_artifact_fk
            FOREIGN KEY (plan_manifest_artifact_id)
            REFERENCES archive_artifacts(artifact_id) ON DELETE RESTRICT
            DEFERRABLE INITIALLY DEFERRED;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='remote_chunks'::regclass
          AND conname='remote_chunks_terminal_manifest_artifact_fk'
    ) THEN
        ALTER TABLE remote_chunks
            ADD CONSTRAINT remote_chunks_terminal_manifest_artifact_fk
            FOREIGN KEY (terminal_manifest_artifact_id)
            REFERENCES archive_artifacts(artifact_id) ON DELETE RESTRICT;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='remote_chunks'::regclass
          AND conname='remote_chunks_manifest_authority_ck'
    ) THEN
        ALTER TABLE remote_chunks
            ADD CONSTRAINT remote_chunks_manifest_authority_ck
            CHECK ((plan_source='legacy_db'
                    AND plan_manifest_artifact_id IS NULL)
                   OR (plan_source='manifest'
                       AND plan_manifest_artifact_id IS NOT NULL));
    END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS uq_remote_chunks_plan_manifest_artifact
    ON remote_chunks(plan_manifest_artifact_id)
    WHERE plan_manifest_artifact_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_remote_chunks_terminal_manifest_artifact
    ON remote_chunks(terminal_manifest_artifact_id)
    WHERE terminal_manifest_artifact_id IS NOT NULL;

-- ---------------------------------------------------------------------------
-- Session allocation boundaries and terminal legacy-authority transitions.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS remote_session_plan_transitions (
    transition_id                 BIGINT GENERATED BY DEFAULT AS IDENTITY
                                      PRIMARY KEY,
    session_id                    BIGINT NOT NULL
                                      REFERENCES remote_sessions(session_id)
                                      ON DELETE RESTRICT,
    transition_epoch              BIGINT NOT NULL CHECK (transition_epoch >= 1),
    state                         TEXT NOT NULL DEFAULT 'draft' CHECK (
                                      state IN ('draft','rehearsed','approved',
                                                'active','rolled_back')),
    prior_plan_source             TEXT NOT NULL CHECK (
                                      prior_plan_source IN
                                      ('legacy_db','manifest')),
    new_plan_source               TEXT NOT NULL CHECK (
                                      new_plan_source IN
                                      ('legacy_db','manifest')),
    last_chunk_before_transition  INTEGER CHECK (
                                      last_chunk_before_transition IS NULL
                                      OR last_chunk_before_transition >= 0),
    first_chunk_after_transition  INTEGER NOT NULL CHECK (
                                      first_chunk_after_transition >= 0),
    scan_frontier_generation      BIGINT CHECK (
                                      scan_frontier_generation IS NULL
                                      OR scan_frontier_generation >= 0),
    evidence_report_locator       TEXT,
    approval_identity             TEXT,
    approved_at                   TIMESTAMPTZ,
    created_at                    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT remote_session_plan_transitions_sources_ck CHECK (
        prior_plan_source <> new_plan_source),
    CONSTRAINT remote_session_plan_transitions_boundary_ck CHECK (
        (last_chunk_before_transition IS NULL
         AND first_chunk_after_transition=0)
        OR first_chunk_after_transition=last_chunk_before_transition+1),
    CONSTRAINT remote_session_plan_transitions_evidence_ck CHECK (
        state='draft'
        OR (scan_frontier_generation IS NOT NULL
            AND evidence_report_locator IS NOT NULL
            AND length(btrim(evidence_report_locator)) > 0
            AND lower(right(evidence_report_locator, 5)) <> '.part')),
    CONSTRAINT remote_session_plan_transitions_approval_ck CHECK (
        (state IN ('draft','rehearsed')
         AND approval_identity IS NULL AND approved_at IS NULL)
        OR (state IN ('approved','active','rolled_back')
            AND approval_identity IS NOT NULL
            AND length(btrim(approval_identity)) > 0
            AND approved_at IS NOT NULL)),
    CONSTRAINT remote_session_plan_transitions_epoch_uq
        UNIQUE (session_id, transition_epoch)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_remote_session_plan_active_boundary
    ON remote_session_plan_transitions(
        session_id, first_chunk_after_transition)
    WHERE state='active';

CREATE INDEX IF NOT EXISTS ix_remote_session_plan_effective_boundary
    ON remote_session_plan_transitions(
        session_id, first_chunk_after_transition DESC, transition_epoch DESC)
    WHERE state IN ('active','rolled_back');

CREATE TABLE IF NOT EXISTS remote_chunk_plan_source_transitions (
    chunk_plan_transition_id       BIGINT GENERATED BY DEFAULT AS IDENTITY
                                       PRIMARY KEY,
    session_id                     BIGINT NOT NULL,
    chunk_index                    INTEGER NOT NULL CHECK (chunk_index >= 0),
    from_plan_source               TEXT NOT NULL,
    to_plan_source                 TEXT NOT NULL,
    prior_plan_manifest_artifact_id BIGINT
                                       REFERENCES archive_artifacts(artifact_id)
                                       ON DELETE RESTRICT,
    new_plan_manifest_artifact_id  BIGINT NOT NULL
                                       REFERENCES archive_artifacts(artifact_id)
                                       ON DELETE RESTRICT,
    plan4_gate_id                  TEXT NOT NULL CHECK (
                                       length(btrim(plan4_gate_id)) > 0),
    plan4_evidence_id              TEXT NOT NULL CHECK (
                                       length(btrim(plan4_evidence_id)) > 0),
    approval_identity              TEXT NOT NULL CHECK (
                                       length(btrim(approval_identity)) > 0),
    equivalence_confirmed          BOOLEAN NOT NULL CHECK (
                                       equivalence_confirmed),
    transitioned_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT remote_chunk_plan_source_transition_direction_ck CHECK (
        from_plan_source='legacy_db' AND to_plan_source='manifest'),
    CONSTRAINT remote_chunk_plan_source_transition_chunk_fk
        FOREIGN KEY (session_id, chunk_index)
        REFERENCES remote_chunks(session_id, chunk_index) ON DELETE RESTRICT
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_remote_chunk_plan_source_transition
    ON remote_chunk_plan_source_transitions(session_id, chunk_index);

CREATE UNIQUE INDEX IF NOT EXISTS uq_remote_chunk_plan_transition_artifact
    ON remote_chunk_plan_source_transitions(new_plan_manifest_artifact_id);

-- ---------------------------------------------------------------------------
-- Canonical directories and scan-coverage generations.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS archive_directories (
    directory_id        BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    source_host         TEXT NOT NULL CHECK (length(btrim(source_host)) > 0),
    canonical_path      TEXT NOT NULL CHECK (length(btrim(canonical_path)) > 0),
    parent_directory_id BIGINT REFERENCES archive_directories(directory_id)
                            ON DELETE RESTRICT,
    name                TEXT NOT NULL CHECK (length(name) > 0),
    depth               INTEGER NOT NULL CHECK (depth >= 0),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT archive_directories_source_path_uq
        UNIQUE (source_host, canonical_path),
    CONSTRAINT archive_directories_parent_ck CHECK (
        parent_directory_id IS NULL OR parent_directory_id <> directory_id),
    CONSTRAINT archive_directories_root_depth_ck CHECK (
        (parent_directory_id IS NULL AND depth=0)
        OR (parent_directory_id IS NOT NULL AND depth>0))
);

CREATE INDEX IF NOT EXISTS ix_archive_directories_parent
    ON archive_directories(parent_directory_id, name);

CREATE TABLE IF NOT EXISTS directory_scan_coverage (
    coverage_id                          BIGINT GENERATED BY DEFAULT AS IDENTITY
                                             PRIMARY KEY,
    directory_id                         BIGINT NOT NULL
                                             REFERENCES archive_directories(
                                                 directory_id)
                                             ON DELETE RESTRICT,
    session_id                           BIGINT NOT NULL
                                             REFERENCES remote_sessions(session_id)
                                             ON DELETE RESTRICT,
    scan_scope_id                        BIGINT NOT NULL
                                             REFERENCES remote_scan_scopes(
                                                 scan_scope_id)
                                             ON DELETE RESTRICT,
    coverage_state                       TEXT NOT NULL CHECK (
                                             coverage_state IN
                                             ('provisional','final','error')),
    direct_discovered_file_count         BIGINT NOT NULL DEFAULT 0 CHECK (
                                             direct_discovered_file_count >= 0),
    direct_discovered_bytes              BIGINT NOT NULL DEFAULT 0 CHECK (
                                             direct_discovered_bytes >= 0),
    direct_discovered_directory_count    BIGINT NOT NULL DEFAULT 0 CHECK (
                                             direct_discovered_directory_count >= 0),
    recursive_discovered_file_count      BIGINT NOT NULL DEFAULT 0 CHECK (
                                             recursive_discovered_file_count >= 0),
    recursive_discovered_bytes           BIGINT NOT NULL DEFAULT 0 CHECK (
                                             recursive_discovered_bytes >= 0),
    recursive_discovered_directory_count BIGINT NOT NULL DEFAULT 0 CHECK (
                                             recursive_discovered_directory_count >= 0),
    excluded_count                       BIGINT NOT NULL DEFAULT 0 CHECK (
                                             excluded_count >= 0),
    error_count                          BIGINT NOT NULL DEFAULT 0 CHECK (
                                             error_count >= 0),
    frontier_generation                  BIGINT NOT NULL CHECK (
                                             frontier_generation >= 0),
    created_at                           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                           TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT directory_scan_coverage_generation_uq UNIQUE (
        directory_id, session_id, scan_scope_id, frontier_generation),
    CONSTRAINT directory_scan_coverage_recursive_ck CHECK (
        recursive_discovered_file_count >= direct_discovered_file_count
        AND recursive_discovered_bytes >= direct_discovered_bytes
        AND recursive_discovered_directory_count >=
            direct_discovered_directory_count),
    CONSTRAINT directory_scan_coverage_error_ck CHECK (
        coverage_state <> 'error' OR error_count > 0)
);

CREATE INDEX IF NOT EXISTS ix_directory_scan_coverage_session_generation
    ON directory_scan_coverage(session_id, frontier_generation, directory_id);

-- ---------------------------------------------------------------------------
-- Normalized directory contributions. A part is one directory's direct
-- contribution to one container or one loose catalog record, never both.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS directory_archive_parts (
    part_id                                  BIGINT GENERATED BY DEFAULT AS IDENTITY
                                                 PRIMARY KEY,
    directory_id                            BIGINT NOT NULL
                                                 REFERENCES archive_directories(
                                                     directory_id)
                                                 ON DELETE RESTRICT,
    session_id                              BIGINT NOT NULL,
    chunk_index                             INTEGER NOT NULL CHECK (
                                                 chunk_index >= 0),
    container_id                            BIGINT,
    loose_record_key                        BYTEA,
    tape_generation_id                      BIGINT
                                                 REFERENCES tape_generations(
                                                     generation_id)
                                                 ON DELETE RESTRICT,
    storage_class                           TEXT NOT NULL CHECK (
                                                 storage_class IN
                                                 ('container','loose')),
    evidence_generation                     BIGINT NOT NULL DEFAULT 1 CHECK (
                                                 evidence_generation >= 1),

    direct_expected_count                   BIGINT NOT NULL DEFAULT 0 CHECK (
                                                 direct_expected_count >= 0),
    direct_expected_bytes                   BIGINT NOT NULL DEFAULT 0 CHECK (
                                                 direct_expected_bytes >= 0),
    direct_archived_count                   BIGINT NOT NULL DEFAULT 0 CHECK (
                                                 direct_archived_count >= 0),
    direct_archived_bytes                   BIGINT NOT NULL DEFAULT 0 CHECK (
                                                 direct_archived_bytes >= 0),
    direct_source_missing_count             BIGINT NOT NULL DEFAULT 0 CHECK (
                                                 direct_source_missing_count >= 0),
    direct_source_missing_bytes             BIGINT NOT NULL DEFAULT 0 CHECK (
                                                 direct_source_missing_bytes >= 0),
    direct_source_permission_denied_count   BIGINT NOT NULL DEFAULT 0 CHECK (
                                                 direct_source_permission_denied_count >= 0),
    direct_source_permission_denied_bytes   BIGINT NOT NULL DEFAULT 0 CHECK (
                                                 direct_source_permission_denied_bytes >= 0),
    direct_source_unreadable_count          BIGINT NOT NULL DEFAULT 0 CHECK (
                                                 direct_source_unreadable_count >= 0),
    direct_source_unreadable_bytes          BIGINT NOT NULL DEFAULT 0 CHECK (
                                                 direct_source_unreadable_bytes >= 0),
    direct_source_changed_count             BIGINT NOT NULL DEFAULT 0 CHECK (
                                                 direct_source_changed_count >= 0),
    direct_source_changed_bytes             BIGINT NOT NULL DEFAULT 0 CHECK (
                                                 direct_source_changed_bytes >= 0),
    direct_unresolved_count                 BIGINT NOT NULL DEFAULT 0 CHECK (
                                                 direct_unresolved_count >= 0),
    direct_unresolved_bytes                 BIGINT NOT NULL DEFAULT 0 CHECK (
                                                 direct_unresolved_bytes >= 0),

    plan_manifest_artifact_id               BIGINT
                                                 REFERENCES archive_artifacts(
                                                     artifact_id)
                                                 ON DELETE RESTRICT,
    sidecar_artifact_id                     BIGINT
                                                 REFERENCES archive_artifacts(
                                                     artifact_id)
                                                 ON DELETE RESTRICT,
    terminal_manifest_artifact_id           BIGINT
                                                 REFERENCES archive_artifacts(
                                                     artifact_id)
                                                 ON DELETE RESTRICT,
    plan_artifact_locator                   TEXT,
    sidecar_artifact_locator                TEXT,
    terminal_artifact_locator               TEXT,

    local_validation_state                  TEXT NOT NULL DEFAULT 'not_validated'
                                                 CHECK (local_validation_state IN
                                                 ('not_validated','pending',
                                                  'succeeded','failed','ambiguous')),
    writer_state                            TEXT NOT NULL DEFAULT 'not_started'
                                                 CHECK (writer_state IN
                                                 ('not_started','writing','copied',
                                                  'failed','ambiguous')),
    catalog_state                           TEXT NOT NULL DEFAULT 'not_started'
                                                 CHECK (catalog_state IN
                                                 ('not_started','committing',
                                                  'committed','failed','ambiguous')),

    restore_format                          TEXT NOT NULL CHECK (
                                                 restore_format IN
                                                 ('zip','stored_tar','loose')),
    tape_label                              TEXT
                                                 REFERENCES tapes(volume_label)
                                                 ON UPDATE CASCADE
                                                 ON DELETE RESTRICT,
    stored_path                             TEXT CHECK (
                                                 stored_path IS NULL
                                                 OR length(btrim(stored_path)) > 0),
    source_base_path                        TEXT NOT NULL CHECK (
                                                 length(btrim(source_base_path)) > 0),
    container_member_candidate              TEXT,
    routing_precision                       TEXT NOT NULL CHECK (
                                                 routing_precision IN
                                                 ('exact','coarse')),
    created_at                              TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                              TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT directory_archive_parts_chunk_fk
        FOREIGN KEY (session_id, chunk_index)
        REFERENCES remote_chunks(session_id, chunk_index) ON DELETE RESTRICT,
    CONSTRAINT directory_archive_parts_container_fk
        FOREIGN KEY (container_id, session_id, chunk_index)
        REFERENCES archive_containers(container_id, session_id, chunk_index)
        ON DELETE RESTRICT,
    CONSTRAINT directory_archive_parts_loose_record_fk
        FOREIGN KEY (loose_record_key)
        REFERENCES files_index(record_key) ON DELETE RESTRICT,
    CONSTRAINT directory_archive_parts_exactly_one_identity_ck CHECK (
        num_nonnulls(container_id, loose_record_key)=1),
    CONSTRAINT directory_archive_parts_loose_key_ck CHECK (
        loose_record_key IS NULL OR octet_length(loose_record_key)=32),
    CONSTRAINT directory_archive_parts_storage_identity_ck CHECK (
        (container_id IS NOT NULL AND loose_record_key IS NULL
         AND storage_class='container'
         AND restore_format IN ('zip','stored_tar'))
        OR
        (container_id IS NULL AND loose_record_key IS NOT NULL
         AND storage_class='loose' AND restore_format='loose')),
    CONSTRAINT directory_archive_parts_exact_route_ck CHECK (
        routing_precision <> 'exact'
        OR container_id IS NULL
        OR (container_member_candidate IS NOT NULL
            AND length(container_member_candidate) > 0)),
    CONSTRAINT directory_archive_parts_outcome_total_ck CHECK (
        direct_archived_count + direct_source_missing_count
        + direct_source_permission_denied_count
        + direct_source_unreadable_count + direct_source_changed_count
        + direct_unresolved_count <= direct_expected_count),
    CONSTRAINT directory_archive_parts_outcome_bytes_ck CHECK (
        direct_archived_bytes + direct_source_missing_bytes
        + direct_source_permission_denied_bytes
        + direct_source_unreadable_bytes + direct_source_changed_bytes
        + direct_unresolved_bytes <= direct_expected_bytes),
    CONSTRAINT directory_archive_parts_route_tuple_ck CHECK (
        num_nonnulls(tape_generation_id, tape_label, stored_path) IN (0, 3)
        AND ((direct_archived_count=0 AND direct_archived_bytes=0)
             OR writer_state='copied')
        AND ((writer_state<>'copied' AND direct_archived_count=0)
             OR num_nonnulls(
                    tape_generation_id, tape_label, stored_path)=3)),
    CONSTRAINT directory_archive_parts_catalog_writer_ck CHECK (
        catalog_state<>'committed' OR writer_state='copied'),
    CONSTRAINT directory_archive_parts_locator_ck CHECK (
        (plan_artifact_locator IS NULL
         OR lower(right(plan_artifact_locator, 5)) <> '.part')
        AND (sidecar_artifact_locator IS NULL
             OR lower(right(sidecar_artifact_locator, 5)) <> '.part')
        AND (terminal_artifact_locator IS NULL
             OR lower(right(terminal_artifact_locator, 5)) <> '.part'))
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_directory_archive_parts_container
    ON directory_archive_parts(
        directory_id, session_id, chunk_index, container_id,
        evidence_generation)
    WHERE container_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_directory_archive_parts_loose
    ON directory_archive_parts(
        directory_id, session_id, chunk_index, loose_record_key,
        evidence_generation)
    WHERE loose_record_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_directory_archive_parts_restore
    ON directory_archive_parts(
        directory_id, tape_label, tape_generation_id, restore_format);

-- ---------------------------------------------------------------------------
-- One persisted completeness result per session/directory. Direct and
-- recursive aggregates remain separate, as do all seven proof booleans.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS directory_completeness (
    session_id                              BIGINT NOT NULL
                                                REFERENCES remote_sessions(
                                                    session_id)
                                                ON DELETE RESTRICT,
    directory_id                            BIGINT NOT NULL
                                                REFERENCES archive_directories(
                                                    directory_id)
                                                ON DELETE RESTRICT,

    direct_expected_file_count              BIGINT NOT NULL DEFAULT 0 CHECK (
                                                direct_expected_file_count >= 0),
    direct_expected_bytes                   BIGINT NOT NULL DEFAULT 0 CHECK (
                                                direct_expected_bytes >= 0),
    direct_archived_file_count              BIGINT NOT NULL DEFAULT 0 CHECK (
                                                direct_archived_file_count >= 0),
    direct_archived_bytes                   BIGINT NOT NULL DEFAULT 0 CHECK (
                                                direct_archived_bytes >= 0),
    recursive_expected_file_count           BIGINT NOT NULL DEFAULT 0 CHECK (
                                                recursive_expected_file_count >= 0),
    recursive_expected_bytes                BIGINT NOT NULL DEFAULT 0 CHECK (
                                                recursive_expected_bytes >= 0),
    recursive_archived_file_count           BIGINT NOT NULL DEFAULT 0 CHECK (
                                                recursive_archived_file_count >= 0),
    recursive_archived_bytes                BIGINT NOT NULL DEFAULT 0 CHECK (
                                                recursive_archived_bytes >= 0),

    direct_source_missing_count             BIGINT NOT NULL DEFAULT 0 CHECK (
                                                direct_source_missing_count >= 0),
    direct_source_permission_denied_count   BIGINT NOT NULL DEFAULT 0 CHECK (
                                                direct_source_permission_denied_count >= 0),
    direct_source_unreadable_count          BIGINT NOT NULL DEFAULT 0 CHECK (
                                                direct_source_unreadable_count >= 0),
    direct_source_changed_count             BIGINT NOT NULL DEFAULT 0 CHECK (
                                                direct_source_changed_count >= 0),
    direct_unresolved_count                 BIGINT NOT NULL DEFAULT 0 CHECK (
                                                direct_unresolved_count >= 0),
    recursive_source_missing_count          BIGINT NOT NULL DEFAULT 0 CHECK (
                                                recursive_source_missing_count >= 0),
    recursive_source_permission_denied_count BIGINT NOT NULL DEFAULT 0 CHECK (
                                                recursive_source_permission_denied_count >= 0),
    recursive_source_unreadable_count       BIGINT NOT NULL DEFAULT 0 CHECK (
                                                recursive_source_unreadable_count >= 0),
    recursive_source_changed_count          BIGINT NOT NULL DEFAULT 0 CHECK (
                                                recursive_source_changed_count >= 0),
    recursive_unresolved_count              BIGINT NOT NULL DEFAULT 0 CHECK (
                                                recursive_unresolved_count >= 0),

    scan_is_final                           BOOLEAN NOT NULL DEFAULT FALSE,
    all_planned_items_terminal              BOOLEAN NOT NULL DEFAULT FALSE,
    all_required_items_archived             BOOLEAN NOT NULL DEFAULT FALSE,
    all_parts_written                       BOOLEAN NOT NULL DEFAULT FALSE,
    all_writer_completions_succeeded        BOOLEAN NOT NULL DEFAULT FALSE,
    all_parts_cataloged                     BOOLEAN NOT NULL DEFAULT FALSE,
    all_local_validation_succeeded          BOOLEAN NOT NULL DEFAULT FALSE,
    has_ambiguous_evidence                  BOOLEAN NOT NULL DEFAULT FALSE,
    status                                  TEXT NOT NULL DEFAULT 'provisional'
                                                CHECK (status IN
                                                ('ambiguous','provisional',
                                                 'incomplete',
                                                 'complete_with_source_exceptions',
                                                 'complete')),
    pinned_frontier_generation              BIGINT NOT NULL DEFAULT 0 CHECK (
                                                pinned_frontier_generation >= 0),
    pinned_artifact_evidence_generation     BIGINT NOT NULL DEFAULT 0 CHECK (
                                                pinned_artifact_evidence_generation >= 0),
    calculated_at                           TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (session_id, directory_id),

    CONSTRAINT directory_completeness_recursive_ck CHECK (
        recursive_expected_file_count >= direct_expected_file_count
        AND recursive_expected_bytes >= direct_expected_bytes
        AND recursive_archived_file_count >= direct_archived_file_count
        AND recursive_archived_bytes >= direct_archived_bytes
        AND recursive_source_missing_count >= direct_source_missing_count
        AND recursive_source_permission_denied_count >=
            direct_source_permission_denied_count
        AND recursive_source_unreadable_count >=
            direct_source_unreadable_count
        AND recursive_source_changed_count >= direct_source_changed_count
        AND recursive_unresolved_count >= direct_unresolved_count),
    CONSTRAINT directory_completeness_terminal_partition_ck CHECK (
        NOT all_planned_items_terminal
        OR recursive_archived_file_count + recursive_source_missing_count
           + recursive_source_permission_denied_count
           + recursive_source_unreadable_count
           + recursive_source_changed_count + recursive_unresolved_count
           = recursive_expected_file_count),
    CONSTRAINT directory_completeness_required_archive_ck CHECK (
        NOT all_required_items_archived
        OR (recursive_unresolved_count=0
            AND recursive_archived_file_count
                + recursive_source_missing_count
                + recursive_source_permission_denied_count
                + recursive_source_unreadable_count
                + recursive_source_changed_count
                = recursive_expected_file_count)),
    -- Status is constrained by every independent proof bit. Counts alone can
    -- never select a complete state.
    CONSTRAINT directory_completeness_derived_status_ck CHECK (
        status = CASE
            WHEN has_ambiguous_evidence OR recursive_unresolved_count > 0
                THEN 'ambiguous'
            WHEN NOT scan_is_final THEN 'provisional'
            WHEN NOT (all_planned_items_terminal
                      AND all_required_items_archived
                      AND all_parts_written
                      AND all_writer_completions_succeeded
                      AND all_parts_cataloged
                      AND all_local_validation_succeeded)
                THEN 'incomplete'
            WHEN recursive_source_missing_count
                 + recursive_source_permission_denied_count
                 + recursive_source_unreadable_count
                 + recursive_source_changed_count > 0
                THEN 'complete_with_source_exceptions'
            ELSE 'complete'
        END)
);

CREATE INDEX IF NOT EXISTS ix_directory_completeness_status
    ON directory_completeness(session_id, status, directory_id);

-- ---------------------------------------------------------------------------
-- Cross-row guards.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lto_assert_ready_chunk_artifact(
    p_artifact_id BIGINT,
    p_session_id BIGINT,
    p_chunk_index INTEGER,
    p_artifact_kind TEXT,
    p_container_id BIGINT DEFAULT NULL
) RETURNS VOID LANGUAGE plpgsql STABLE AS $$
DECLARE
    artifact RECORD;
BEGIN
    SELECT * INTO artifact
    FROM archive_artifacts
    WHERE artifact_id=p_artifact_id;
    IF NOT FOUND
       OR artifact.session_id IS DISTINCT FROM p_session_id
       OR artifact.chunk_index IS DISTINCT FROM p_chunk_index
       OR artifact.artifact_kind IS DISTINCT FROM p_artifact_kind
       OR artifact.container_id IS DISTINCT FROM p_container_id
       OR artifact.readiness_state <> 'ready'
       OR artifact.local_locator IS NULL
       OR lower(right(artifact.local_locator, 5))='.part'
       OR artifact.artifact_size_bytes IS NULL
       OR artifact.published_at IS NULL THEN
        RAISE EXCEPTION
            'artifact % is not the ready % authority for session %, chunk %',
            p_artifact_id, p_artifact_kind, p_session_id, p_chunk_index;
    END IF;
END $$;

CREATE OR REPLACE FUNCTION lto_guard_archive_directory()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    parent RECORD;
BEGIN
    IF TG_OP='UPDATE'
       AND ROW(OLD.source_host, OLD.canonical_path,
               OLD.parent_directory_id, OLD.name, OLD.depth)
           IS DISTINCT FROM
           ROW(NEW.source_host, NEW.canonical_path,
               NEW.parent_directory_id, NEW.name, NEW.depth) THEN
        RAISE EXCEPTION 'canonical archive directory identity is immutable';
    END IF;
    IF NEW.parent_directory_id IS NULL THEN
        IF NEW.depth <> 0 THEN
            RAISE EXCEPTION 'root archive directory depth must be zero';
        END IF;
    ELSE
        SELECT source_host, depth INTO parent
        FROM archive_directories
        WHERE directory_id=NEW.parent_directory_id;
        IF NOT FOUND
           OR parent.source_host IS DISTINCT FROM NEW.source_host
           OR NEW.depth <> parent.depth + 1 THEN
            RAISE EXCEPTION
                'archive directory parent must share source host and be one depth above';
        END IF;
    END IF;
    IF TG_OP='UPDATE' THEN
        NEW.updated_at := now();
    END IF;
    RETURN NEW;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgrelid='archive_directories'::regclass
          AND tgname='trg_archive_directories_parent_guard'
          AND NOT tgisinternal
    ) THEN
        CREATE TRIGGER trg_archive_directories_parent_guard
        BEFORE INSERT OR UPDATE OF source_host, canonical_path,
                                   parent_directory_id, name, depth
        ON archive_directories
        FOR EACH ROW EXECUTE FUNCTION lto_guard_archive_directory();
    END IF;
END $$;

CREATE OR REPLACE FUNCTION lto_guard_directory_scan_coverage()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    scope_session_id BIGINT;
BEGIN
    SELECT session_id INTO scope_session_id
    FROM remote_scan_scopes WHERE scan_scope_id=NEW.scan_scope_id;
    IF scope_session_id IS DISTINCT FROM NEW.session_id THEN
        RAISE EXCEPTION
            'directory coverage scope does not belong to its session';
    END IF;
    IF TG_OP='UPDATE' THEN
        NEW.updated_at := now();
    END IF;
    RETURN NEW;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgrelid='directory_scan_coverage'::regclass
          AND tgname='trg_directory_scan_coverage_scope_guard'
          AND NOT tgisinternal
    ) THEN
        CREATE TRIGGER trg_directory_scan_coverage_scope_guard
        BEFORE INSERT OR UPDATE OF session_id, scan_scope_id
        ON directory_scan_coverage
        FOR EACH ROW EXECUTE FUNCTION lto_guard_directory_scan_coverage();
    END IF;
END $$;

CREATE OR REPLACE FUNCTION lto_guard_directory_archive_part()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    generation_label TEXT;
    container RECORD;
    loose RECORD;
    artifact RECORD;
BEGIN
    SELECT volume_label INTO generation_label
    FROM tape_generations WHERE generation_id=NEW.tape_generation_id;
    IF generation_label IS DISTINCT FROM NEW.tape_label THEN
        RAISE EXCEPTION
            'directory part tape label does not match its tape generation';
    END IF;

    IF NEW.container_id IS NOT NULL THEN
        SELECT container_format, tape_label, tape_generation_id, tape_path
        INTO container
        FROM archive_containers WHERE container_id=NEW.container_id;
        IF NOT FOUND OR container.container_format IS DISTINCT FROM
                NEW.restore_format THEN
            RAISE EXCEPTION
                'directory part route conflicts with container format';
        END IF;
        IF container.tape_label IS NOT NULL
           AND container.tape_label IS DISTINCT FROM NEW.tape_label THEN
            RAISE EXCEPTION
                'directory part route conflicts with container tape label';
        END IF;
        IF container.tape_generation_id IS NOT NULL
           AND container.tape_generation_id IS DISTINCT FROM
               NEW.tape_generation_id THEN
            RAISE EXCEPTION
                'directory part route conflicts with container tape generation';
        END IF;
        IF NEW.routing_precision='exact'
           AND container.tape_path IS NOT NULL
           AND container.tape_path IS DISTINCT FROM NEW.stored_path THEN
            RAISE EXCEPTION
                'exact directory part route conflicts with container stored path';
        END IF;
    ELSE
        -- A loose part is a contribution that EXISTS: its
        -- loose_record_key carries a foreign key into files_index, so the
        -- catalog row is structurally required.  Work that is planned but not
        -- yet written is represented as a container part (archive_containers
        -- exists from 'planned' onward) or simply as unarchived plan entries -
        -- never as a loose part with no catalog record.
        SELECT tape_label, stored_path, remote_session_id, remote_chunk_index
        INTO loose
        FROM files_index WHERE record_key=NEW.loose_record_key;
        IF NOT FOUND OR loose.tape_label IS DISTINCT FROM NEW.tape_label THEN
            RAISE EXCEPTION
                'directory loose part conflicts with its catalog record';
        END IF;
        IF loose.remote_session_id IS NOT NULL
           AND (loose.remote_session_id IS DISTINCT FROM NEW.session_id
                OR loose.remote_chunk_index IS DISTINCT FROM NEW.chunk_index) THEN
            RAISE EXCEPTION
                'directory loose part conflicts with remote chunk provenance';
        END IF;
        IF NEW.routing_precision='exact'
           AND loose.stored_path IS DISTINCT FROM NEW.stored_path THEN
            RAISE EXCEPTION
                'exact loose restore route conflicts with catalog stored path';
        END IF;
    END IF;

    IF NEW.plan_manifest_artifact_id IS NOT NULL THEN
        PERFORM lto_assert_ready_chunk_artifact(
            NEW.plan_manifest_artifact_id, NEW.session_id, NEW.chunk_index,
            'plan_manifest', NULL);
        SELECT * INTO artifact FROM archive_artifacts
        WHERE artifact_id=NEW.plan_manifest_artifact_id;
        IF NEW.plan_artifact_locator IS NOT NULL
           AND NEW.plan_artifact_locator IS DISTINCT FROM
               artifact.local_locator THEN
            RAISE EXCEPTION
                'directory part plan artifact locator conflicts with artifact authority';
        END IF;
    END IF;
    IF NEW.terminal_manifest_artifact_id IS NOT NULL THEN
        PERFORM lto_assert_ready_chunk_artifact(
            NEW.terminal_manifest_artifact_id, NEW.session_id,
            NEW.chunk_index, 'terminal_manifest', NULL);
        SELECT * INTO artifact FROM archive_artifacts
        WHERE artifact_id=NEW.terminal_manifest_artifact_id;
        IF NEW.terminal_artifact_locator IS NOT NULL
           AND NEW.terminal_artifact_locator IS DISTINCT FROM
               artifact.local_locator THEN
            RAISE EXCEPTION
                'directory part terminal artifact locator conflicts with artifact authority';
        END IF;
    END IF;
    IF NEW.sidecar_artifact_id IS NOT NULL THEN
        SELECT * INTO artifact FROM archive_artifacts
        WHERE artifact_id=NEW.sidecar_artifact_id;
        IF NOT FOUND
           OR artifact.session_id IS DISTINCT FROM NEW.session_id
           OR artifact.chunk_index IS DISTINCT FROM NEW.chunk_index
           OR artifact.container_id IS DISTINCT FROM NEW.container_id
           OR artifact.artifact_kind NOT IN ('zip_manifest','tar_sidecar')
           OR artifact.readiness_state <> 'ready'
           OR artifact.local_locator IS NULL
           OR lower(right(artifact.local_locator, 5))='.part' THEN
            RAISE EXCEPTION
                'directory part sidecar is not ready for its container';
        END IF;
        IF (NEW.restore_format='zip'
            AND artifact.artifact_kind<>'zip_manifest')
           OR (NEW.restore_format='stored_tar'
               AND artifact.artifact_kind<>'tar_sidecar') THEN
            RAISE EXCEPTION
                'directory part sidecar kind conflicts with restore format';
        END IF;
        IF NEW.sidecar_artifact_locator IS NOT NULL
           AND NEW.sidecar_artifact_locator IS DISTINCT FROM
               artifact.local_locator THEN
            RAISE EXCEPTION
                'directory part sidecar locator conflicts with artifact authority';
        END IF;
    END IF;

    IF TG_OP='UPDATE' THEN
        NEW.updated_at := now();
    END IF;
    RETURN NEW;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgrelid='directory_archive_parts'::regclass
          AND tgname='trg_directory_archive_parts_guard'
          AND NOT tgisinternal
    ) THEN
        CREATE TRIGGER trg_directory_archive_parts_guard
        BEFORE INSERT OR UPDATE ON directory_archive_parts
        FOR EACH ROW EXECUTE FUNCTION lto_guard_directory_archive_part();
    END IF;
END $$;

-- A ready artifact is sealed local evidence. Its locator, readiness, size and
-- publication time cannot later be repointed. This uses a trigger rather than
-- new archive_artifacts columns/constraints so the Plan 2 schema remains
-- byte-for-byte compatible with migration 015's original authority surface.
CREATE OR REPLACE FUNCTION lto_guard_sealed_artifact()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' THEN
        IF OLD.readiness_state='ready' THEN
            RAISE EXCEPTION 'ready archive artifact % is immutable',
                OLD.artifact_id;
        END IF;
        RETURN OLD;
    END IF;

    IF OLD.readiness_state='ready'
       AND ROW(OLD.session_id, OLD.chunk_index, OLD.container_id,
               OLD.artifact_kind, OLD.artifact_version,
               OLD.readiness_state, OLD.local_locator,
               OLD.artifact_size_bytes, OLD.published_at)
           IS DISTINCT FROM
           ROW(NEW.session_id, NEW.chunk_index, NEW.container_id,
               NEW.artifact_kind, NEW.artifact_version,
               NEW.readiness_state, NEW.local_locator,
               NEW.artifact_size_bytes, NEW.published_at) THEN
        RAISE EXCEPTION 'ready archive artifact % is immutable',
            OLD.artifact_id;
    END IF;

    IF EXISTS (
        SELECT 1 FROM remote_chunks c
        WHERE c.plan_manifest_artifact_id=OLD.artifact_id
           OR c.terminal_manifest_artifact_id=OLD.artifact_id
    ) AND ROW(OLD.session_id, OLD.chunk_index, OLD.container_id,
              OLD.artifact_kind, OLD.artifact_version,
              OLD.readiness_state, OLD.local_locator,
              OLD.artifact_size_bytes, OLD.published_at)
          IS DISTINCT FROM
          ROW(NEW.session_id, NEW.chunk_index, NEW.container_id,
              NEW.artifact_kind, NEW.artifact_version,
              NEW.readiness_state, NEW.local_locator,
              NEW.artifact_size_bytes, NEW.published_at) THEN
        RAISE EXCEPTION 'authoritative archive artifact % is sealed',
            OLD.artifact_id;
    END IF;
    RETURN NEW;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgrelid='archive_artifacts'::regclass
          AND tgname='trg_archive_artifacts_sealed_immutable'
          AND NOT tgisinternal
    ) THEN
        CREATE TRIGGER trg_archive_artifacts_sealed_immutable
        BEFORE UPDATE OF session_id, chunk_index, container_id, artifact_kind,
                         artifact_version, readiness_state, local_locator,
                         artifact_size_bytes, published_at OR DELETE
        ON archive_artifacts
        FOR EACH ROW EXECUTE FUNCTION lto_guard_sealed_artifact();
    END IF;
END $$;

-- ---------------------------------------------------------------------------
-- Session transition epoch lifecycle and effective future-allocation source.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lto_guard_session_plan_transition()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    expected_epoch BIGINT;
    maximum_chunk_index INTEGER;
    prior_effective_source TEXT;
    maximum_activated_boundary INTEGER;
    inconsistent_chunk_count BIGINT;
BEGIN
    IF TG_OP='DELETE' THEN
        RAISE EXCEPTION 'session plan transition epochs are append-preserved';
    END IF;

    PERFORM 1 FROM remote_sessions
    WHERE session_id=NEW.session_id FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'remote session % does not exist', NEW.session_id;
    END IF;

    IF TG_OP='INSERT' THEN
        IF NEW.state <> 'draft' THEN
            RAISE EXCEPTION 'a session plan transition must begin as draft';
        END IF;
        SELECT COALESCE(max(transition_epoch), 0) + 1
        INTO expected_epoch
        FROM remote_session_plan_transitions
        WHERE session_id=NEW.session_id;
        IF NEW.transition_epoch <> expected_epoch THEN
            RAISE EXCEPTION
                'transition epoch % is not the next epoch % for session %',
                NEW.transition_epoch, expected_epoch, NEW.session_id;
        END IF;
        RETURN NEW;
    END IF;

    IF ROW(OLD.session_id, OLD.transition_epoch, OLD.prior_plan_source,
           OLD.new_plan_source, OLD.last_chunk_before_transition,
           OLD.first_chunk_after_transition, OLD.created_at)
       IS DISTINCT FROM
       ROW(NEW.session_id, NEW.transition_epoch, NEW.prior_plan_source,
           NEW.new_plan_source, NEW.last_chunk_before_transition,
           NEW.first_chunk_after_transition, NEW.created_at) THEN
        RAISE EXCEPTION 'session plan transition identity is immutable';
    END IF;

    IF OLD.state IN ('rehearsed','approved','active','rolled_back')
       AND ROW(OLD.scan_frontier_generation, OLD.evidence_report_locator)
           IS DISTINCT FROM
           ROW(NEW.scan_frontier_generation, NEW.evidence_report_locator) THEN
        RAISE EXCEPTION 'rehearsed transition evidence is immutable';
    END IF;
    IF OLD.state IN ('approved','active','rolled_back')
       AND ROW(OLD.approval_identity, OLD.approved_at)
           IS DISTINCT FROM
           ROW(NEW.approval_identity, NEW.approved_at) THEN
        RAISE EXCEPTION 'approved transition identity is immutable';
    END IF;

    IF NEW.state <> OLD.state
       AND ROW(OLD.state, NEW.state) NOT IN
           (ROW('draft','rehearsed'), ROW('rehearsed','approved'),
            ROW('approved','active'), ROW('active','rolled_back')) THEN
        RAISE EXCEPTION 'illegal session plan transition state change % -> %',
            OLD.state, NEW.state;
    END IF;

    IF OLD.state='approved' AND NEW.state='active' THEN
        SELECT max(chunk_index) INTO maximum_chunk_index
        FROM remote_chunks WHERE session_id=NEW.session_id;
        IF maximum_chunk_index IS DISTINCT FROM
                NEW.last_chunk_before_transition THEN
            RAISE EXCEPTION
                'active plan transition boundary does not follow the current maximum chunk';
        END IF;

        SELECT max(first_chunk_after_transition)
        INTO maximum_activated_boundary
        FROM remote_session_plan_transitions
        WHERE session_id=NEW.session_id
          AND transition_id<>NEW.transition_id
          AND state IN ('active','rolled_back');
        IF maximum_activated_boundary IS NOT NULL
           AND NEW.first_chunk_after_transition <
               maximum_activated_boundary THEN
            RAISE EXCEPTION
                'active transition boundary must advance monotonically';
        END IF;

        SELECT t.new_plan_source INTO prior_effective_source
        FROM remote_session_plan_transitions t
        WHERE t.session_id=NEW.session_id
          AND t.transition_id<>NEW.transition_id
          AND t.state IN ('active','rolled_back')
          AND t.first_chunk_after_transition <=
              NEW.first_chunk_after_transition
        ORDER BY t.first_chunk_after_transition DESC,
                 t.transition_epoch DESC
        LIMIT 1;
        prior_effective_source := COALESCE(
            prior_effective_source, 'legacy_db');
        IF prior_effective_source IS DISTINCT FROM NEW.prior_plan_source THEN
            RAISE EXCEPTION
                'transition prior plan source conflicts with active boundary history';
        END IF;

        SELECT count(*) INTO inconsistent_chunk_count
        FROM remote_chunks c
        WHERE c.session_id=NEW.session_id
          AND c.chunk_index < NEW.first_chunk_after_transition
          AND c.plan_source IS DISTINCT FROM COALESCE(
              (SELECT history.new_plan_source
               FROM remote_session_plan_transitions history
               WHERE history.session_id=c.session_id
                 AND history.transition_id<>NEW.transition_id
                 AND history.state IN ('active','rolled_back')
                 AND history.first_chunk_after_transition<=c.chunk_index
               ORDER BY history.first_chunk_after_transition DESC,
                        history.transition_epoch DESC
               LIMIT 1),
              'legacy_db')
          AND NOT EXISTS (
              SELECT 1 FROM remote_chunk_plan_source_transitions audit
              WHERE audit.session_id=c.session_id
                AND audit.chunk_index=c.chunk_index
                AND audit.to_plan_source=c.plan_source
                AND audit.new_plan_manifest_artifact_id=
                    c.plan_manifest_artifact_id);
        IF inconsistent_chunk_count <> 0 THEN
            RAISE EXCEPTION
                'transition boundary history conflicts with % existing chunk plan source(s)',
                inconsistent_chunk_count;
        END IF;
    END IF;

    NEW.updated_at := now();
    RETURN NEW;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgrelid='remote_session_plan_transitions'::regclass
          AND tgname='trg_remote_session_plan_transitions_guard'
          AND NOT tgisinternal
    ) THEN
        CREATE TRIGGER trg_remote_session_plan_transitions_guard
        BEFORE INSERT OR UPDATE OR DELETE
        ON remote_session_plan_transitions
        FOR EACH ROW EXECUTE FUNCTION lto_guard_session_plan_transition();
    END IF;
END $$;

CREATE OR REPLACE FUNCTION lto_effective_chunk_plan_source(
    p_session_id BIGINT,
    p_chunk_index INTEGER
) RETURNS TEXT LANGUAGE SQL STABLE AS $$
SELECT COALESCE(
    (SELECT t.new_plan_source
     FROM remote_session_plan_transitions t
     WHERE t.session_id=p_session_id
       AND t.state IN ('active','rolled_back')
       AND t.first_chunk_after_transition <= p_chunk_index
     ORDER BY t.first_chunk_after_transition DESC,
              t.transition_epoch DESC
     LIMIT 1),
    'legacy_db')
$$;

-- ---------------------------------------------------------------------------
-- Audited terminal legacy_db -> manifest authority migration.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lto_reject_chunk_plan_transition_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='INSERT'
       AND pg_trigger_depth() > 1
       AND current_setting('lto.chunk_plan_transition_audit_write', true)='on'
       AND NEW.plan4_gate_id=current_setting(
           'lto.chunk_plan_transition_gate_id', true)
       AND NEW.plan4_evidence_id=current_setting(
           'lto.chunk_plan_transition_evidence_id', true)
       AND NEW.approval_identity=current_setting(
           'lto.chunk_plan_transition_approval_identity', true) THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'chunk plan-source transition audit is append-only';
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgrelid='remote_chunk_plan_source_transitions'::regclass
          AND tgname='trg_remote_chunk_plan_source_transitions_immutable'
          AND NOT tgisinternal
    ) THEN
        CREATE TRIGGER trg_remote_chunk_plan_source_transitions_immutable
        BEFORE INSERT OR UPDATE OR DELETE
        ON remote_chunk_plan_source_transitions
        FOR EACH ROW EXECUTE FUNCTION
            lto_reject_chunk_plan_transition_mutation();
    END IF;
END $$;

CREATE OR REPLACE FUNCTION lto_guard_chunk_plan_source()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    expected_source TEXT;
    gate_id TEXT;
    evidence_id TEXT;
    approval TEXT;
    session_plan_id BIGINT;
    active_attempt BOOLEAN;
    active_tape_owner BOOLEAN := FALSE;
    active_container BOOLEAN := FALSE;
BEGIN
    IF TG_OP='INSERT' THEN
        expected_source := lto_effective_chunk_plan_source(
            NEW.session_id, NEW.chunk_index);
        IF NEW.plan_source IS NULL THEN
            NEW.plan_source := expected_source;
        ELSIF NEW.plan_source IS DISTINCT FROM expected_source THEN
            RAISE EXCEPTION
                'initial chunk plan source conflicts with durable transition boundary';
        END IF;
        IF NEW.plan_source='manifest' THEN
            IF NEW.plan_manifest_artifact_id IS NULL THEN
                RAISE EXCEPTION
                    'manifest chunk requires exactly one ready plan artifact';
            END IF;
        ELSIF NEW.plan_manifest_artifact_id IS NOT NULL THEN
            RAISE EXCEPTION
                'legacy database chunk cannot install manifest authority';
        END IF;
        IF NEW.terminal_manifest_artifact_id IS NOT NULL THEN
            PERFORM lto_assert_ready_chunk_artifact(
                NEW.terminal_manifest_artifact_id, NEW.session_id,
                NEW.chunk_index, 'terminal_manifest', NULL);
        END IF;
        RETURN NEW;
    END IF;

    IF OLD.plan_ordinal_scope IS DISTINCT FROM NEW.plan_ordinal_scope THEN
        RAISE EXCEPTION 'chunk plan ordinal scope is immutable';
    END IF;

    IF OLD.terminal_manifest_artifact_id IS NOT NULL
       AND (OLD.terminal_manifest_artifact_id IS DISTINCT FROM
                NEW.terminal_manifest_artifact_id
            OR ROW(OLD.final_archived_count,
                   OLD.final_source_missing_count,
                   OLD.final_source_permission_denied_count,
                   OLD.final_source_unreadable_count,
                   OLD.final_source_changed_count,
                   OLD.final_unresolved_count,
                   OLD.final_archived_bytes)
               IS DISTINCT FROM
               ROW(NEW.final_archived_count,
                   NEW.final_source_missing_count,
                   NEW.final_source_permission_denied_count,
                   NEW.final_source_unreadable_count,
                   NEW.final_source_changed_count,
                   NEW.final_unresolved_count,
                   NEW.final_archived_bytes)) THEN
        RAISE EXCEPTION 'terminal chunk artifact and aggregates are immutable';
    END IF;
    IF OLD.terminal_manifest_artifact_id IS NULL
       AND NEW.terminal_manifest_artifact_id IS NOT NULL THEN
        IF NEW.status<>'done'
           OR NEW.owner_token IS NOT NULL
           OR NEW.lease_expires_at IS NOT NULL
           OR NEW.attempt_id IS NOT NULL
           OR NEW.expected_file_count IS NULL
           OR NEW.expected_bytes IS NULL
           OR num_nonnulls(
                NEW.final_archived_count,
                NEW.final_source_missing_count,
                NEW.final_source_permission_denied_count,
                NEW.final_source_unreadable_count,
                NEW.final_source_changed_count,
                NEW.final_unresolved_count,
                NEW.final_archived_bytes) <> 7 THEN
            RAISE EXCEPTION
                'terminal manifest requires complete terminal unowned chunk aggregates';
        END IF;
        IF NEW.plan_source='manifest'
           AND NEW.final_archived_count > 0
           AND (NEW.writer_completed_at IS NULL
                OR NEW.catalog_committed_at IS NULL) THEN
            RAISE EXCEPTION
                'archived terminal outcomes require durable writer and catalog completion';
        END IF;
        SELECT EXISTS (
            SELECT 1 FROM remote_worker_attempts wa
            WHERE wa.session_id=NEW.session_id
              AND wa.chunk_index=NEW.chunk_index
              AND wa.terminal_state IS NULL)
        INTO active_attempt;
        IF active_attempt THEN
            RAISE EXCEPTION
                'terminal manifest refuses an active chunk worker';
        END IF;
        PERFORM lto_assert_ready_chunk_artifact(
            NEW.terminal_manifest_artifact_id, NEW.session_id,
            NEW.chunk_index, 'terminal_manifest', NULL);
    END IF;

    IF OLD.plan_source IS NOT DISTINCT FROM NEW.plan_source THEN
        IF OLD.plan_manifest_artifact_id IS DISTINCT FROM
                NEW.plan_manifest_artifact_id THEN
            RAISE EXCEPTION
                'chunk plan artifact authority changes only with audited source transition';
        END IF;
        RETURN NEW;
    END IF;

    IF OLD.plan_source<>'legacy_db' OR NEW.plan_source<>'manifest'
       OR OLD.plan_manifest_artifact_id IS NOT NULL
       OR NEW.plan_manifest_artifact_id IS NULL THEN
        RAISE EXCEPTION
            'chunk plan source is immutable except audited legacy_db to manifest transition';
    END IF;

    gate_id := nullif(current_setting(
        'lto.chunk_plan_transition_gate_id', true), '');
    evidence_id := nullif(current_setting(
        'lto.chunk_plan_transition_evidence_id', true), '');
    approval := nullif(current_setting(
        'lto.chunk_plan_transition_approval_identity', true), '');
    IF gate_id IS NULL OR evidence_id IS NULL OR approval IS NULL THEN
        RAISE EXCEPTION
            'audited plan authority transition requires Plan 4 gate, evidence, and approval IDs';
    END IF;

    IF OLD.status<>'done'
       OR OLD.owner_token IS NOT NULL
       OR OLD.lease_expires_at IS NOT NULL
       OR OLD.attempt_id IS NOT NULL
       OR NEW.status<>'done'
       OR NEW.owner_token IS NOT NULL
       OR NEW.lease_expires_at IS NOT NULL
       OR NEW.attempt_id IS NOT NULL
       OR ROW(OLD.status, OLD.owner_token, OLD.lease_expires_at,
              OLD.attempt_id)
          IS DISTINCT FROM
          ROW(NEW.status, NEW.owner_token, NEW.lease_expires_at,
              NEW.attempt_id) THEN
        RAISE EXCEPTION
            'plan authority transition requires an unchanged terminal unowned chunk';
    END IF;
    SELECT EXISTS (
        SELECT 1 FROM remote_worker_attempts wa
        WHERE wa.session_id=OLD.session_id
          AND wa.chunk_index=OLD.chunk_index
          AND wa.terminal_state IS NULL)
    INTO active_attempt;
    IF active_attempt THEN
        RAISE EXCEPTION
            'plan authority transition refuses an active chunk worker';
    END IF;

    SELECT EXISTS (
        SELECT 1 FROM archive_containers ac
        WHERE ac.session_id=OLD.session_id
          AND ac.chunk_index=OLD.chunk_index
          AND (ac.owner_token IS NOT NULL
               OR ac.lease_expires_at IS NOT NULL
               OR ac.validation_state IN
                    ('building','validated_part','failed','blocked')
               OR ac.writer_state IN ('writing','failed','ambiguous')
               OR ac.catalog_state IN
                    ('committing','failed','ambiguous')))
    INTO active_container;
    IF active_container THEN
        RAISE EXCEPTION
            'plan authority transition refuses nonterminal container ownership or state';
    END IF;

    IF to_regclass('public.tape_write_active_chunk') IS NOT NULL THEN
        EXECUTE
            'SELECT EXISTS (SELECT 1 FROM tape_write_active_chunk '
            'WHERE session_id=$1 AND chunk_index=$2)'
        INTO active_tape_owner USING OLD.session_id, OLD.chunk_index;
    END IF;
    IF active_tape_owner THEN
        RAISE EXCEPTION
            'plan authority transition refuses an active tape-write owner';
    END IF;

    SELECT plan_id INTO session_plan_id
    FROM remote_sessions WHERE session_id=OLD.session_id;
    IF session_plan_id IS NULL
       OR EXISTS (
            SELECT 1 FROM remote_sessions s
            WHERE s.plan_id=session_plan_id
              AND s.session_id<>OLD.session_id)
       OR NOT EXISTS (
            SELECT 1 FROM remote_plan_files pf
            WHERE pf.plan_id=session_plan_id
              AND pf.chunk_index=OLD.chunk_index) THEN
        RAISE EXCEPTION
            'shared or missing legacy plan authority refuses transition';
    END IF;

    PERFORM lto_assert_ready_chunk_artifact(
        NEW.plan_manifest_artifact_id, NEW.session_id, NEW.chunk_index,
        'plan_manifest', NULL);

    PERFORM set_config('lto.chunk_plan_transition_audit_write', 'on', true);
    INSERT INTO remote_chunk_plan_source_transitions
        (session_id, chunk_index, from_plan_source, to_plan_source,
         prior_plan_manifest_artifact_id, new_plan_manifest_artifact_id,
         plan4_gate_id, plan4_evidence_id, approval_identity,
         equivalence_confirmed)
    VALUES
        (OLD.session_id, OLD.chunk_index, OLD.plan_source, NEW.plan_source,
         OLD.plan_manifest_artifact_id, NEW.plan_manifest_artifact_id,
         gate_id, evidence_id, approval, TRUE);
    PERFORM set_config('lto.chunk_plan_transition_audit_write', 'off', true);
    RETURN NEW;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgrelid='remote_chunks'::regclass
          AND tgname='trg_remote_chunks_plan_source_guard'
          AND NOT tgisinternal
    ) THEN
        CREATE TRIGGER trg_remote_chunks_plan_source_guard
        BEFORE INSERT OR UPDATE OF plan_source, plan_manifest_artifact_id,
                                terminal_manifest_artifact_id,
                                plan_ordinal_scope, final_archived_count,
                                final_source_missing_count,
                                final_source_permission_denied_count,
                                final_source_unreadable_count,
                                final_source_changed_count,
                                final_unresolved_count, final_archived_bytes
        ON remote_chunks
        FOR EACH ROW EXECUTE FUNCTION lto_guard_chunk_plan_source();
    END IF;
END $$;

CREATE OR REPLACE FUNCTION lto_transition_chunk_plan_source(
    p_session_id BIGINT,
    p_chunk_index INTEGER,
    p_plan_manifest_artifact_id BIGINT,
    p_plan4_gate_id TEXT,
    p_plan4_evidence_id TEXT,
    p_approval_identity TEXT
) RETURNS BIGINT LANGUAGE plpgsql AS $$
DECLARE
    existing RECORD;
    audit_id BIGINT;
BEGIN
    IF nullif(btrim(p_plan4_gate_id), '') IS NULL
       OR nullif(btrim(p_plan4_evidence_id), '') IS NULL
       OR nullif(btrim(p_approval_identity), '') IS NULL THEN
        RAISE EXCEPTION
            'Plan 4 gate, evidence, and approval IDs are required';
    END IF;

    SELECT c.plan_source, c.plan_manifest_artifact_id
    INTO existing
    FROM remote_chunks c
    WHERE c.session_id=p_session_id AND c.chunk_index=p_chunk_index
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'remote chunk does not exist';
    END IF;

    IF existing.plan_source='manifest' THEN
        SELECT chunk_plan_transition_id INTO audit_id
        FROM remote_chunk_plan_source_transitions
        WHERE session_id=p_session_id AND chunk_index=p_chunk_index
          AND new_plan_manifest_artifact_id=p_plan_manifest_artifact_id
          AND plan4_gate_id=p_plan4_gate_id
          AND plan4_evidence_id=p_plan4_evidence_id
          AND approval_identity=p_approval_identity;
        IF audit_id IS NULL THEN
            RAISE EXCEPTION
                'manifest chunk authority differs from requested audited transition';
        END IF;
        RETURN audit_id;
    END IF;

    PERFORM set_config('lto.chunk_plan_transition_gate_id',
                       p_plan4_gate_id, true);
    PERFORM set_config('lto.chunk_plan_transition_evidence_id',
                       p_plan4_evidence_id, true);
    PERFORM set_config('lto.chunk_plan_transition_approval_identity',
                       p_approval_identity, true);

    UPDATE remote_chunks
    SET plan_source='manifest',
        plan_manifest_artifact_id=p_plan_manifest_artifact_id
    WHERE session_id=p_session_id AND chunk_index=p_chunk_index;

    SELECT chunk_plan_transition_id INTO STRICT audit_id
    FROM remote_chunk_plan_source_transitions
    WHERE session_id=p_session_id AND chunk_index=p_chunk_index;
    RETURN audit_id;
END $$;

-- The Plan 2 artifact FK points from archive_artifacts to remote_chunks and is
-- intentionally preserved. The new reverse authority FK is deferred, allowing
-- one transaction to reserve an artifact identity, insert the manifest chunk,
-- then insert its ready artifact. This deferred trigger proves readiness and
-- scope at commit; no committed manifest chunk can ever expose a missing or
-- unready plan artifact.
CREATE OR REPLACE FUNCTION lto_validate_manifest_chunk_authority()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.plan_source='manifest' THEN
        PERFORM lto_assert_ready_chunk_artifact(
            NEW.plan_manifest_artifact_id, NEW.session_id, NEW.chunk_index,
            'plan_manifest', NULL);
    END IF;
    RETURN NEW;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgrelid='remote_chunks'::regclass
          AND tgname='trg_remote_chunks_manifest_authority_deferred'
          AND NOT tgisinternal
    ) THEN
        CREATE CONSTRAINT TRIGGER
            trg_remote_chunks_manifest_authority_deferred
        AFTER INSERT OR UPDATE OF plan_source, plan_manifest_artifact_id
        ON remote_chunks
        DEFERRABLE INITIALLY DEFERRED
        FOR EACH ROW EXECUTE FUNCTION
            lto_validate_manifest_chunk_authority();
    END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS uq_archive_artifacts_ready_plan_chunk
    ON archive_artifacts(session_id, chunk_index)
    WHERE artifact_kind='plan_manifest'
      AND container_id IS NULL
      AND readiness_state='ready';

-- ---------------------------------------------------------------------------
-- Unified read view. Base tables remain normalized; aggregation is confined
-- to this view so no duplicate contribution can disappear into an array.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW directory_catalog_status_v AS
WITH directory_session_keys AS (
    SELECT directory_id, session_id FROM directory_scan_coverage
    UNION
    SELECT directory_id, session_id FROM directory_archive_parts
    UNION
    SELECT directory_id, session_id FROM directory_completeness
)
SELECT
    d.directory_id,
    d.source_host,
    d.canonical_path,
    d.name AS directory_name,
    d.depth,
    d.parent_directory_id,
    parent.canonical_path AS parent_canonical_path,
    parent.name AS parent_name,
    keys.session_id,
    session_row.session_label,
    children.child_count,
    children.child_directories,
    coverage.coverage_state,
    coverage.coverage_is_final,
    coverage.maximum_frontier_generation,
    coverage.coverage_generations,

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
    COALESCE(part.plan_artifact_locator,
             plan_artifact.local_locator) AS plan_artifact_locator,
    COALESCE(part.sidecar_artifact_locator,
             sidecar_artifact.local_locator) AS sidecar_artifact_locator,
    COALESCE(part.terminal_artifact_locator,
             terminal_artifact.local_locator) AS terminal_artifact_locator,
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
    CASE WHEN part.part_id IS NULL THEN NULL ELSE jsonb_build_object(
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
       ON parent.directory_id=d.parent_directory_id
LEFT JOIN directory_session_keys keys
       ON keys.directory_id=d.directory_id
LEFT JOIN remote_sessions session_row
       ON session_row.session_id=keys.session_id
LEFT JOIN directory_completeness completeness
       ON completeness.directory_id=d.directory_id
      AND completeness.session_id=keys.session_id
LEFT JOIN directory_archive_parts part
       ON part.directory_id=d.directory_id
      AND part.session_id=keys.session_id
LEFT JOIN remote_chunks chunk
       ON chunk.session_id=part.session_id
      AND chunk.chunk_index=part.chunk_index
LEFT JOIN archive_containers container
       ON container.container_id=part.container_id
LEFT JOIN tape_generations tape_generation
       ON tape_generation.generation_id=part.tape_generation_id
LEFT JOIN archive_artifacts plan_artifact
       ON plan_artifact.artifact_id=part.plan_manifest_artifact_id
LEFT JOIN archive_artifacts sidecar_artifact
       ON sidecar_artifact.artifact_id=part.sidecar_artifact_id
LEFT JOIN archive_artifacts terminal_artifact
       ON terminal_artifact.artifact_id=part.terminal_manifest_artifact_id
LEFT JOIN LATERAL (
    SELECT count(*)::BIGINT AS child_count,
           COALESCE(jsonb_agg(jsonb_build_object(
               'directory_id', child.directory_id,
               'canonical_path', child.canonical_path,
               'name', child.name,
               'depth', child.depth)
               ORDER BY child.name, child.directory_id), '[]'::JSONB)
               AS child_directories
    FROM archive_directories child
    WHERE child.parent_directory_id=d.directory_id
) children ON TRUE
LEFT JOIN LATERAL (
    WITH latest_coverage AS (
        SELECT DISTINCT ON (cov.scan_scope_id) cov.*
        FROM directory_scan_coverage cov
        WHERE cov.directory_id=d.directory_id
          AND cov.session_id=keys.session_id
        ORDER BY cov.scan_scope_id,
                 cov.frontier_generation DESC,
                 cov.coverage_id DESC
    )
    SELECT CASE
               WHEN bool_or(latest.coverage_state='error') THEN 'error'
               WHEN bool_and(latest.coverage_state='final') THEN 'final'
               ELSE 'provisional'
           END AS coverage_state,
           COALESCE(bool_and(latest.coverage_state='final'), FALSE)
               AS coverage_is_final,
           max(latest.frontier_generation) AS maximum_frontier_generation,
           (
               SELECT COALESCE(jsonb_agg(jsonb_build_object(
                   'coverage_id', history.coverage_id,
                   'scan_scope_id', history.scan_scope_id,
                   'coverage_state', history.coverage_state,
                   'frontier_generation', history.frontier_generation,
                   'direct_discovered_file_count',
                       history.direct_discovered_file_count,
                   'direct_discovered_bytes',
                       history.direct_discovered_bytes,
                   'direct_discovered_directory_count',
                       history.direct_discovered_directory_count,
                   'recursive_discovered_file_count',
                       history.recursive_discovered_file_count,
                   'recursive_discovered_bytes',
                       history.recursive_discovered_bytes,
                   'recursive_discovered_directory_count',
                       history.recursive_discovered_directory_count,
                   'excluded_count', history.excluded_count,
                   'error_count', history.error_count)
                   ORDER BY history.frontier_generation,
                            history.scan_scope_id,
                            history.coverage_id), '[]'::JSONB)
               FROM directory_scan_coverage history
               WHERE history.directory_id=d.directory_id
                 AND history.session_id=keys.session_id
           ) AS coverage_generations
    FROM latest_coverage latest
) coverage ON TRUE;

COMMENT ON VIEW directory_catalog_status_v IS
    'Migration 018 normalized directory status and restore routes; metadata-only and never tape-probing.';

-- ---------------------------------------------------------------------------
-- Canonical migration-018 fingerprint and immutable authority row.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lto_manifest_directory_catalog_schema_fingerprint()
RETURNS TEXT LANGUAGE SQL STABLE AS $$
WITH object_defs(object_key, object_definition) AS (
    SELECT 'column:' || c.relname || '.' || a.attname,
           concat_ws('|', format_type(a.atttypid, a.atttypmod),
                     a.attnotnull::text, a.attidentity,
                     COALESCE(pg_get_expr(d.adbin, d.adrelid), ''))
    FROM pg_attribute a
    JOIN pg_class c ON c.oid=a.attrelid
    JOIN pg_namespace n ON n.oid=c.relnamespace
    LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
    WHERE n.nspname='public' AND a.attnum>0 AND NOT a.attisdropped
      AND (
        c.relname IN
            ('manifest_directory_catalog_schema_metadata',
             'remote_session_plan_transitions',
             'remote_chunk_plan_source_transitions',
             'archive_directories','directory_scan_coverage',
             'directory_archive_parts','directory_completeness')
        OR (c.relname='remote_chunks' AND a.attname IN
            ('plan_source','plan_manifest_artifact_id',
             'terminal_manifest_artifact_id','plan_ordinal_scope',
             'final_archived_count','final_source_missing_count',
             'final_source_permission_denied_count',
             'final_source_unreadable_count','final_source_changed_count',
             'final_unresolved_count','final_archived_bytes')))
    UNION ALL
    SELECT 'constraint:' || rel.relname || '.' || con.conname,
           concat_ws('|', con.contype, con.convalidated::text,
                     pg_get_constraintdef(con.oid, true))
    FROM pg_constraint con
    JOIN pg_class rel ON rel.oid=con.conrelid
    JOIN pg_namespace n ON n.oid=rel.relnamespace
    WHERE n.nspname='public' AND (
        rel.relname IN
            ('manifest_directory_catalog_schema_metadata',
             'remote_session_plan_transitions',
             'remote_chunk_plan_source_transitions',
             'archive_directories','directory_scan_coverage',
             'directory_archive_parts','directory_completeness')
        OR con.conname IN
            ('remote_chunks_plan_source_ck',
             'remote_chunks_plan_ordinal_scope_ck',
             'remote_chunks_final_disposition_nonnegative_ck',
             'remote_chunks_final_disposition_complete_ck',
             'remote_chunks_plan_manifest_artifact_fk',
             'remote_chunks_terminal_manifest_artifact_fk',
             'remote_chunks_manifest_authority_ck'))
    UNION ALL
    SELECT 'index:' || idx.relname,
           concat_ws('|', i.indisvalid::text, i.indisready::text,
                     pg_get_indexdef(i.indexrelid, 0, true))
    FROM pg_index i
    JOIN pg_class idx ON idx.oid=i.indexrelid
    JOIN pg_namespace n ON n.oid=idx.relnamespace
    WHERE n.nspname='public' AND idx.relname IN
        ('uq_remote_chunks_plan_manifest_artifact',
         'uq_remote_chunks_terminal_manifest_artifact',
         'uq_remote_session_plan_active_boundary',
         'ix_remote_session_plan_effective_boundary',
         'uq_remote_chunk_plan_source_transition',
         'uq_remote_chunk_plan_transition_artifact',
         'ix_archive_directories_parent',
         'ix_directory_scan_coverage_session_generation',
         'uq_directory_archive_parts_container',
         'uq_directory_archive_parts_loose',
         'ix_directory_archive_parts_restore',
         'ix_directory_completeness_status',
         'uq_archive_artifacts_ready_plan_chunk')
    UNION ALL
    SELECT 'trigger:' || rel.relname || '.' || t.tgname,
           concat_ws('|', t.tgenabled, pg_get_triggerdef(t.oid, true))
    FROM pg_trigger t
    JOIN pg_class rel ON rel.oid=t.tgrelid
    JOIN pg_namespace n ON n.oid=rel.relnamespace
    WHERE n.nspname='public' AND NOT t.tgisinternal AND t.tgname IN
        ('trg_archive_directories_parent_guard',
         'trg_directory_scan_coverage_scope_guard',
         'trg_directory_archive_parts_guard',
         'trg_archive_artifacts_sealed_immutable',
         'trg_remote_session_plan_transitions_guard',
         'trg_remote_chunk_plan_source_transitions_immutable',
         'trg_remote_chunks_plan_source_guard',
         'trg_remote_chunks_manifest_authority_deferred',
         'trg_manifest_directory_catalog_schema_metadata_immutable')
    UNION ALL
    SELECT 'function:' || p.proname || '(' ||
               pg_get_function_identity_arguments(p.oid) || ')',
           concat_ws('|', p.provolatile, p.prosecdef::text,
                     pg_get_functiondef(p.oid))
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid=p.pronamespace
    WHERE n.nspname='public' AND p.proname IN
        ('lto_assert_ready_chunk_artifact',
         'lto_guard_archive_directory',
         'lto_guard_directory_scan_coverage',
         'lto_guard_directory_archive_part',
         'lto_guard_sealed_artifact',
         'lto_guard_session_plan_transition',
         'lto_effective_chunk_plan_source',
         'lto_reject_chunk_plan_transition_mutation',
         'lto_guard_chunk_plan_source',
         'lto_transition_chunk_plan_source',
         'lto_validate_manifest_chunk_authority',
         'lto_manifest_directory_catalog_schema_fingerprint',
         'lto_guard_manifest_directory_catalog_metadata')
    UNION ALL
    SELECT 'view:directory_catalog_status_v',
           pg_get_viewdef('directory_catalog_status_v'::regclass, true)
)
SELECT md5(COALESCE(string_agg(
    object_key || '=' || object_definition, E'\n' ORDER BY object_key), ''))
FROM object_defs
$$;

CREATE OR REPLACE FUNCTION lto_guard_manifest_directory_catalog_metadata()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='INSERT'
       AND NOT EXISTS (
           SELECT 1 FROM manifest_directory_catalog_schema_metadata)
       AND NEW.migration_checksum=current_setting(
           'lto.manifest_directory_catalog_migration_checksum', true) THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION
        'manifest directory catalog schema authority is immutable';
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_trigger
        WHERE tgrelid='manifest_directory_catalog_schema_metadata'::regclass
          AND tgname=
              'trg_manifest_directory_catalog_schema_metadata_immutable'
          AND NOT tgisinternal
    ) THEN
        CREATE TRIGGER
            trg_manifest_directory_catalog_schema_metadata_immutable
        BEFORE INSERT OR UPDATE OR DELETE
        ON manifest_directory_catalog_schema_metadata
        FOR EACH ROW EXECUTE FUNCTION
            lto_guard_manifest_directory_catalog_metadata();
    END IF;
END $$;

DO $$
DECLARE
    supplied_checksum TEXT := current_setting(
        'lto.manifest_directory_catalog_migration_checksum');
    fingerprint TEXT :=
        lto_manifest_directory_catalog_schema_fingerprint();
    installed RECORD;
BEGIN
    SELECT * INTO installed
    FROM manifest_directory_catalog_schema_metadata WHERE singleton;
    IF NOT FOUND THEN
        INSERT INTO manifest_directory_catalog_schema_metadata
            (singleton, schema_version, migration_checksum,
             schema_fingerprint)
        VALUES (TRUE, 1, supplied_checksum, fingerprint);
    ELSIF installed.schema_version<>1
       OR installed.migration_checksum<>supplied_checksum
       OR installed.schema_fingerprint<>fingerprint THEN
        RAISE EXCEPTION
            'installed migration 018 authority differs from current schema';
    END IF;
END $$;

COMMENT ON TABLE manifest_directory_catalog_schema_metadata IS
    'Explicit migration 018 checksum and catalog-definition authority.';
COMMENT ON TABLE remote_session_plan_transitions IS
    'Append-preserved future chunk plan-source boundaries by session epoch.';
COMMENT ON TABLE remote_chunk_plan_source_transitions IS
    'Atomic terminal legacy_db to manifest authority audit; no reverse transition.';
COMMENT ON TABLE archive_directories IS
    'Canonical source-host directory identity independent of tape navigation.';
