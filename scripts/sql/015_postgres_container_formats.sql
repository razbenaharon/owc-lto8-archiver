-- =============================================================================
-- 015 - Durable per-chunk packaging format and archive-container truth.
-- =============================================================================
--
-- EXPLICIT-ONLY.  PgConnectionCore.apply_container_format_schema() owns the
-- transaction and supplies the migration checksum plus any individually
-- approved legacy-session exception through transaction-local settings.  This
-- migration is deliberately absent from PgConnectionCore._init_schema().
--
-- The normal backfill is ZIP.  A legacy-session exception is possible only on
-- the FIRST application, while packaging_format is still NULL, and only when
-- the evidence query below proves a contiguous, never-started suffix.  The
-- exception never changes a non-NULL format and never contains a session or
-- chunk-number literal.
-- =============================================================================

DO $$
BEGIN
    IF to_regclass('public.remote_chunks') IS NULL
       OR to_regclass('public.remote_sessions') IS NULL
       OR to_regclass('public.remote_plan_files') IS NULL
       OR to_regclass('public.remote_snapshot_files') IS NULL
       OR to_regclass('public.remote_worker_attempts') IS NULL
       OR to_regclass('public.tape_generations') IS NULL THEN
        RAISE EXCEPTION
            'migration 015 requires migrations 013 and 014 (base)';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE schemaname='public'
          AND indexname='uq_remote_plan_files_chunk_ordinal'
    ) THEN
        RAISE EXCEPTION
            'migration 015 requires finalized migration 014 membership';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='public' AND table_name='files_index'
          AND column_name='remote_chunk_index'
    ) THEN
        RAISE EXCEPTION
            'migration 015 requires migration 008 remote provenance';
    END IF;
END $$;

-- A first application must start from a pristine pre-015 shape.  IF NOT EXISTS
-- is for idempotent re-application of a *validated* installation, never for
-- repairing or adopting an unrecorded partial schema.
DO $$
DECLARE
    metadata_rows INTEGER := 0;
    marker_count INTEGER := 0;
    fingerprint_matches BOOLEAN := FALSE;
BEGIN
    marker_count :=
        (CASE WHEN to_regclass('public.remote_packaging_boundaries')
                   IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN to_regclass('public.remote_packaging_boundary_chunks')
                   IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN to_regclass('public.archive_containers')
                   IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN to_regclass('public.archive_artifacts')
                   IS NOT NULL THEN 1 ELSE 0 END) +
        (SELECT count(*) FROM information_schema.columns
         WHERE table_schema='public' AND (
             (table_name='remote_sessions'
              AND column_name='default_packaging_format') OR
             (table_name='remote_chunks' AND column_name IN
                 ('packaging_format','packaging_assigned_at',
                  'writer_started_at','writer_completed_at',
                  'catalog_committed_at')) OR
             (table_name='archive_bundles' AND column_name IN
                 ('container_id','container_format')) OR
             (table_name='archive_runs' AND column_name IN
                 ('remote_chunk_index','tape_generation_id'))));

    IF to_regclass('public.container_format_schema_metadata') IS NULL THEN
        IF marker_count <> 0
           OR EXISTS (
                SELECT 1 FROM pg_proc p
                JOIN pg_namespace n ON n.oid=p.pronamespace
                WHERE n.nspname='public' AND p.proname IN
                    ('lto_reject_packaging_boundary_mutation',
                     'lto_reject_packaging_format_change',
                     'lto_reject_container_format_change',
                     'lto_reject_container_identity_change',
                     'lto_reject_artifact_identity_change',
                     'lto_guard_session_packaging_default',
                     'lto_assign_new_chunk_format',
                     'lto_guard_new_chunk_format',
                     'lto_guard_container_tape_generation',
                     'lto_guard_artifact_container_format',
                     'lto_guard_container_format_metadata',
                     'lto_container_format_schema_fingerprint'))
           OR EXISTS (
                SELECT 1 FROM pg_class c
                JOIN pg_namespace n ON n.oid=c.relnamespace
                WHERE n.nspname='public' AND c.relname IN
                    ('uq_archive_artifacts_container_kind_version',
                     'uq_archive_artifacts_chunk_kind_version',
                     'uq_archive_runs_remote_chunk_generation')) THEN
            RAISE EXCEPTION
                'partial migration 015 markers exist without schema metadata';
        END IF;
    ELSE
        EXECUTE 'SELECT count(*) FROM container_format_schema_metadata'
            INTO metadata_rows;
        IF metadata_rows <> 1 THEN
            RAISE EXCEPTION
                'migration 015 metadata exists without exactly one authority row';
        END IF;
        IF marker_count <> 14 THEN
            RAISE EXCEPTION
                'installed migration 015 is partial (% of 14 core markers)',
                marker_count;
        END IF;
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema='public'
              AND table_name='container_format_schema_metadata'
              AND column_name='schema_fingerprint')
           OR to_regprocedure(
                'public.lto_container_format_schema_fingerprint()') IS NULL THEN
            RAISE EXCEPTION
                'installed migration 015 lacks schema fingerprint authority';
        END IF;
        EXECUTE $sql$
            SELECT schema_fingerprint =
                   lto_container_format_schema_fingerprint()
            FROM container_format_schema_metadata WHERE singleton
        $sql$ INTO fingerprint_matches;
        IF fingerprint_matches IS NOT TRUE THEN
            RAISE EXCEPTION
                'installed migration 015 catalog definition has drifted';
        END IF;
    END IF;
END $$;

-- Block concurrent session/chunk/catalog mutations for the evidence snapshot
-- and initial assignment.  The CLI also refuses a live archiver/process; these
-- locks close the database race between that preflight and this transaction.
LOCK TABLE remote_sessions IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE remote_chunks IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE remote_plan_files IN SHARE MODE;
LOCK TABLE remote_snapshot_files IN SHARE MODE;
LOCK TABLE remote_file_state IN SHARE MODE;
LOCK TABLE remote_worker_attempts IN SHARE MODE;
LOCK TABLE files_index IN SHARE MODE;
LOCK TABLE archive_bundles IN SHARE ROW EXCLUSIVE MODE;
LOCK TABLE archive_runs IN SHARE ROW EXCLUSIVE MODE;
DO $$
BEGIN
    IF to_regclass('public.container_format_schema_metadata') IS NOT NULL THEN
        LOCK TABLE container_format_schema_metadata IN SHARE ROW EXCLUSIVE MODE;
        LOCK TABLE remote_packaging_boundaries IN SHARE ROW EXCLUSIVE MODE;
        LOCK TABLE remote_packaging_boundary_chunks IN SHARE ROW EXCLUSIVE MODE;
        LOCK TABLE archive_containers IN SHARE ROW EXCLUSIVE MODE;
        LOCK TABLE archive_artifacts IN SHARE ROW EXCLUSIVE MODE;
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS container_format_schema_metadata (
    singleton                        BOOLEAN PRIMARY KEY DEFAULT TRUE
                                         CHECK (singleton),
    schema_version                   INTEGER NOT NULL,
    required_reader_contract_version INTEGER NOT NULL,
    stored_tar_dialect               TEXT NOT NULL,
    migration_checksum               TEXT NOT NULL
                                         CHECK (length(migration_checksum)=64),
    schema_fingerprint               TEXT NOT NULL
                                         CHECK (length(schema_fingerprint)=32),
    applied_at                       TIMESTAMPTZ NOT NULL DEFAULT now()
);

DO $$
DECLARE
    supplied_checksum TEXT := nullif(current_setting(
        'lto.container_format_migration_checksum', true), '');
    installed_checksum TEXT;
BEGIN
    IF supplied_checksum IS NULL OR supplied_checksum !~ '^[0-9a-f]{64}$' THEN
        RAISE EXCEPTION
            'migration 015 must be applied through the explicit guarded command';
    END IF;
    SELECT migration_checksum INTO installed_checksum
    FROM container_format_schema_metadata WHERE singleton;
    IF installed_checksum IS NOT NULL
       AND installed_checksum <> supplied_checksum THEN
        RAISE EXCEPTION
            'migration 015 checksum drift: installed %, supplied %',
            installed_checksum, supplied_checksum;
    END IF;
END $$;

ALTER TABLE remote_sessions
    ADD COLUMN IF NOT EXISTS default_packaging_format TEXT;

ALTER TABLE remote_chunks
    ADD COLUMN IF NOT EXISTS packaging_format TEXT,
    ADD COLUMN IF NOT EXISTS packaging_assigned_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS writer_started_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS writer_completed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS catalog_committed_at TIMESTAMPTZ;

ALTER TABLE archive_bundles
    ADD COLUMN IF NOT EXISTS container_id BIGINT,
    ADD COLUMN IF NOT EXISTS container_format TEXT;

ALTER TABLE archive_runs
    ADD COLUMN IF NOT EXISTS remote_chunk_index INTEGER,
    ADD COLUMN IF NOT EXISTS tape_generation_id BIGINT;

CREATE TABLE IF NOT EXISTS remote_packaging_boundaries (
    session_id                       BIGINT PRIMARY KEY
                                         REFERENCES remote_sessions(session_id)
                                         ON DELETE RESTRICT,
    first_stored_tar_chunk_index     INTEGER NOT NULL CHECK (
                                         first_stored_tar_chunk_index >= 0),
    last_existing_chunk_index        INTEGER NOT NULL CHECK (
                                         last_existing_chunk_index >=
                                         first_stored_tar_chunk_index),
    approval_id                      TEXT NOT NULL CHECK (length(approval_id) > 0),
    approval_reason                  TEXT NOT NULL CHECK (length(approval_reason) > 0),
    database_evidence                JSONB NOT NULL,
    local_staging_evidence           JSONB NOT NULL,
    evidence_checked_at              TIMESTAMPTZ NOT NULL,
    created_at                       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS remote_packaging_boundary_chunks (
    session_id              BIGINT NOT NULL,
    chunk_index             INTEGER NOT NULL CHECK (chunk_index >= 0),
    classification          TEXT NOT NULL CHECK (classification IN
                                ('immutable_zip',
                                 'approved_stored_tar_exception')),
    assigned_format         TEXT NOT NULL CHECK (assigned_format IN
                                ('zip','stored_tar')),
    prefix_evidence_basis   TEXT CHECK (prefix_evidence_basis IN
                                ('corroborated','status_only')),
    plan_member_count       BIGINT NOT NULL CHECK (plan_member_count >= 0),
    plan_logical_bytes      BIGINT NOT NULL CHECK (plan_logical_bytes >= 0),
    first_ordinal           BIGINT,
    last_ordinal            BIGINT,
    evidence                JSONB NOT NULL,
    CHECK ((classification='immutable_zip'
            AND assigned_format='zip'
            AND prefix_evidence_basis IS NOT NULL)
           OR (classification='approved_stored_tar_exception'
               AND assigned_format='stored_tar'
               AND prefix_evidence_basis IS NULL)),
    PRIMARY KEY (session_id, chunk_index),
    FOREIGN KEY (session_id)
        REFERENCES remote_packaging_boundaries(session_id) ON DELETE RESTRICT,
    FOREIGN KEY (session_id, chunk_index)
        REFERENCES remote_chunks(session_id, chunk_index) ON DELETE RESTRICT
);

-- Boundary/approval evidence is a one-time migration record. The trigger is
-- installed before assignment and permits INSERT only while the authoritative
-- metadata row is still absent in this same first-application transaction.
CREATE OR REPLACE FUNCTION lto_reject_packaging_boundary_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='INSERT'
       AND NOT EXISTS (SELECT 1 FROM container_format_schema_metadata
                       WHERE singleton)
       AND current_setting('lto.container_format_migration_checksum', true)
             ~ '^[0-9a-f]{64}$' THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'packaging boundary/audit evidence is immutable';
END $$;

DROP TRIGGER IF EXISTS trg_remote_packaging_boundaries_immutable
    ON remote_packaging_boundaries;
CREATE TRIGGER trg_remote_packaging_boundaries_immutable
BEFORE INSERT OR UPDATE OR DELETE ON remote_packaging_boundaries
FOR EACH ROW EXECUTE FUNCTION lto_reject_packaging_boundary_mutation();

DROP TRIGGER IF EXISTS trg_remote_packaging_boundary_chunks_immutable
    ON remote_packaging_boundary_chunks;
CREATE TRIGGER trg_remote_packaging_boundary_chunks_immutable
BEFORE INSERT OR UPDATE OR DELETE ON remote_packaging_boundary_chunks
FOR EACH ROW EXECUTE FUNCTION lto_reject_packaging_boundary_mutation();

-- Required by the composite container->chunk format FK below.  NULL legacy
-- values are still legal at this point; the initial assignment fills them
-- before the migration commits.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='remote_chunks'::regclass
          AND conname='remote_chunks_identity_format_uq'
    ) THEN
        ALTER TABLE remote_chunks
            ADD CONSTRAINT remote_chunks_identity_format_uq
            UNIQUE (session_id, chunk_index, packaging_format);
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS archive_containers (
    container_id                      BIGINT GENERATED BY DEFAULT AS IDENTITY
                                          PRIMARY KEY,
    session_id                       BIGINT NOT NULL,
    chunk_index                      INTEGER NOT NULL CHECK (chunk_index >= 0),
    container_ordinal                INTEGER NOT NULL CHECK (container_ordinal >= 0),
    container_format                 TEXT NOT NULL CHECK (container_format IN
                                          ('zip','stored_tar')),
    format_version                   TEXT NOT NULL CHECK (length(format_version)>0),
    tar_dialect                      TEXT,
    storage_class                    TEXT NOT NULL CHECK (storage_class IN
                                          ('small_files','loose_files')),
    container_name                   TEXT NOT NULL CHECK (length(container_name)>0),
    temporary_data_locator           TEXT,
    permanent_local_metadata_locator TEXT,
    tape_label                       TEXT REFERENCES tapes(volume_label)
                                          ON UPDATE CASCADE ON DELETE RESTRICT,
    tape_path                        TEXT,
    tape_generation_id               BIGINT REFERENCES tape_generations(generation_id)
                                          ON DELETE RESTRICT,
    expected_member_count            BIGINT NOT NULL DEFAULT 0 CHECK (
                                          expected_member_count >= 0),
    expected_logical_bytes           BIGINT NOT NULL DEFAULT 0 CHECK (
                                          expected_logical_bytes >= 0),
    observed_member_count            BIGINT CHECK (observed_member_count >= 0),
    observed_logical_bytes           BIGINT CHECK (observed_logical_bytes >= 0),
    actual_artifact_bytes            BIGINT CHECK (actual_artifact_bytes >= 0),
    validation_state                 TEXT NOT NULL DEFAULT 'planned' CHECK (
                                          validation_state IN
                                          ('planned','building','validated_part',
                                           'ready','failed','blocked')),
    writer_state                     TEXT NOT NULL DEFAULT 'not_started' CHECK (
                                          writer_state IN
                                          ('not_started','writing','copied',
                                           'failed','ambiguous')),
    catalog_state                    TEXT NOT NULL DEFAULT 'not_started' CHECK (
                                          catalog_state IN
                                          ('not_started','committing','committed',
                                           'failed','ambiguous')),
    owner_token                      TEXT,
    lease_expires_at                 TIMESTAMPTZ,
    validation_started_at            TIMESTAMPTZ,
    validated_at                     TIMESTAMPTZ,
    writer_started_at                TIMESTAMPTZ,
    writer_completed_at              TIMESTAMPTZ,
    catalog_committed_at             TIMESTAMPTZ,
    created_at                       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT archive_containers_chunk_format_fk
        FOREIGN KEY (session_id, chunk_index, container_format)
        REFERENCES remote_chunks(session_id, chunk_index, packaging_format)
        ON DELETE RESTRICT,
    CONSTRAINT archive_containers_ordinal_uq
        UNIQUE (session_id, chunk_index, container_ordinal),
    CONSTRAINT archive_containers_identity_uq
        UNIQUE (container_id, session_id, chunk_index),
    CONSTRAINT archive_containers_id_format_uq
        UNIQUE (container_id, container_format),
    CONSTRAINT archive_containers_dialect_ck CHECK (
        (container_format='stored_tar' AND tar_dialect IS NOT NULL
         AND length(tar_dialect)>0)
        OR (container_format='zip' AND tar_dialect IS NULL)),
    CONSTRAINT archive_containers_tape_locator_ck CHECK (
        (tape_label IS NULL AND tape_path IS NULL AND tape_generation_id IS NULL)
        OR (tape_label IS NOT NULL AND tape_path IS NOT NULL
            AND tape_generation_id IS NOT NULL)),
    CONSTRAINT archive_containers_observed_ck CHECK (
        validation_state NOT IN ('validated_part','ready')
         OR (observed_member_count IS NOT NULL
             AND observed_logical_bytes IS NOT NULL
             AND actual_artifact_bytes IS NOT NULL
             AND observed_member_count=expected_member_count
             AND observed_logical_bytes=expected_logical_bytes)),
    CONSTRAINT archive_containers_writer_ck CHECK (
        writer_state <> 'copied'
        OR (tape_path IS NOT NULL AND writer_started_at IS NOT NULL
            AND writer_completed_at IS NOT NULL)),
    CONSTRAINT archive_containers_catalog_ck CHECK (
        catalog_state <> 'committed'
        OR (writer_state='copied' AND catalog_committed_at IS NOT NULL))
);

CREATE TABLE IF NOT EXISTS archive_artifacts (
    artifact_id          BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    session_id           BIGINT NOT NULL,
    chunk_index          INTEGER NOT NULL CHECK (chunk_index >= 0),
    container_id         BIGINT,
    artifact_kind        TEXT NOT NULL CHECK (artifact_kind IN
                             ('zip_manifest','tar_sidecar',
                              'plan_manifest','terminal_manifest')),
    artifact_version     TEXT NOT NULL CHECK (length(artifact_version)>0),
    local_locator        TEXT,
    tape_locator         TEXT,
    artifact_size_bytes  BIGINT CHECK (artifact_size_bytes >= 0),
    readiness_state      TEXT NOT NULL DEFAULT 'planned' CHECK (
                             readiness_state IN
                             ('planned','writing','validated','ready',
                              'failed','blocked')),
    publication_started_at TIMESTAMPTZ,
    published_at         TIMESTAMPTZ,
    tape_published_at    TIMESTAMPTZ,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT archive_artifacts_chunk_fk
        FOREIGN KEY (session_id, chunk_index)
        REFERENCES remote_chunks(session_id, chunk_index) ON DELETE RESTRICT,
    CONSTRAINT archive_artifacts_container_fk
        FOREIGN KEY (container_id, session_id, chunk_index)
        REFERENCES archive_containers(container_id, session_id, chunk_index)
        ON DELETE RESTRICT,
    CONSTRAINT archive_artifacts_scope_ck CHECK (
        (artifact_kind IN ('zip_manifest','tar_sidecar')
         AND container_id IS NOT NULL)
        OR (artifact_kind IN ('plan_manifest','terminal_manifest')
            AND container_id IS NULL)),
    CONSTRAINT archive_artifacts_locator_distinct_ck CHECK (
        local_locator IS NULL OR tape_locator IS NULL
        OR local_locator <> tape_locator),
    CONSTRAINT archive_artifacts_ready_ck CHECK (
        readiness_state <> 'ready'
        OR (local_locator IS NOT NULL
            AND lower(right(local_locator, 5)) <> '.part'
            AND artifact_size_bytes IS NOT NULL
            AND published_at IS NOT NULL))
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_archive_artifacts_container_kind_version
    ON archive_artifacts(container_id, artifact_kind, artifact_version)
    WHERE container_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_archive_artifacts_chunk_kind_version
    ON archive_artifacts(session_id, chunk_index, artifact_kind, artifact_version)
    WHERE container_id IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_archive_runs_remote_chunk_generation
    ON archive_runs(remote_session_id, remote_chunk_index, tape_generation_id)
    WHERE remote_session_id IS NOT NULL
      AND remote_chunk_index IS NOT NULL
      AND tape_generation_id IS NOT NULL;

-- Optional migration 007 compatibility.  Zero tables means "not installed";
-- one or two means schema drift and must not be guessed through.
DO $$
DECLARE
    installed_count INTEGER;
BEGIN
    SELECT count(*) INTO installed_count
    FROM (VALUES ('directory_archive_stats'),
                 ('directory_archive_bundles'),
                 ('directory_tree_index')) AS wanted(name)
    WHERE to_regclass('public.' || wanted.name) IS NOT NULL;

    IF installed_count NOT IN (0, 3) THEN
        RAISE EXCEPTION
            'migration 007 is partially installed (% of 3 tables); refusing migration 015',
            installed_count;
    END IF;

    IF installed_count = 3 THEN
        LOCK TABLE directory_archive_stats IN SHARE ROW EXCLUSIVE MODE;
        LOCK TABLE directory_archive_bundles IN SHARE ROW EXCLUSIVE MODE;
        LOCK TABLE directory_tree_index IN SHARE ROW EXCLUSIVE MODE;
        ALTER TABLE directory_archive_bundles
            ADD COLUMN IF NOT EXISTS container_id BIGINT,
            ADD COLUMN IF NOT EXISTS container_format TEXT,
            ADD COLUMN IF NOT EXISTS tape_generation_id BIGINT,
            ADD COLUMN IF NOT EXISTS actual_artifact_bytes BIGINT;

        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conrelid='directory_archive_bundles'::regclass
              AND conname='directory_archive_bundles_container_fk'
        ) THEN
            ALTER TABLE directory_archive_bundles
                ADD CONSTRAINT directory_archive_bundles_container_fk
                FOREIGN KEY (container_id) REFERENCES archive_containers(container_id)
                ON DELETE RESTRICT NOT VALID;
        END IF;
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conrelid='directory_archive_bundles'::regclass
              AND conname='directory_archive_bundles_generation_fk'
        ) THEN
            ALTER TABLE directory_archive_bundles
                ADD CONSTRAINT directory_archive_bundles_generation_fk
                FOREIGN KEY (tape_generation_id)
                REFERENCES tape_generations(generation_id)
                ON DELETE RESTRICT NOT VALID;
        END IF;
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conrelid='directory_archive_bundles'::regclass
              AND conname='directory_archive_bundles_container_format_ck'
        ) THEN
            ALTER TABLE directory_archive_bundles
                ADD CONSTRAINT directory_archive_bundles_container_format_ck
                CHECK (container_format IS NULL
                       OR container_format IN ('zip','stored_tar')) NOT VALID;
        END IF;
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint
            WHERE conrelid='directory_archive_bundles'::regclass
              AND conname='directory_archive_bundles_artifact_bytes_ck'
        ) THEN
            ALTER TABLE directory_archive_bundles
                ADD CONSTRAINT directory_archive_bundles_artifact_bytes_ck
                CHECK (actual_artifact_bytes IS NULL
                       OR actual_artifact_bytes >= 0) NOT VALID;
        END IF;
    END IF;
END $$;

-- Foreign keys/uniques added after both sides exist.  Named DO blocks keep a
-- re-application idempotent while exact validation in Python detects drift.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='remote_chunks'::regclass
          AND conname='remote_chunks_identity_format_uq'
    ) THEN
        ALTER TABLE remote_chunks
            ADD CONSTRAINT remote_chunks_identity_format_uq
            UNIQUE (session_id, chunk_index, packaging_format);
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='archive_bundles'::regclass
          AND conname='archive_bundles_container_fk'
    ) THEN
        ALTER TABLE archive_bundles
            ADD CONSTRAINT archive_bundles_container_fk
            FOREIGN KEY (container_id, container_format)
            REFERENCES archive_containers(container_id, container_format)
            ON DELETE RESTRICT NOT VALID;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='archive_bundles'::regclass
          AND conname='archive_bundles_container_format_ck'
    ) THEN
        ALTER TABLE archive_bundles
            ADD CONSTRAINT archive_bundles_container_format_ck
            CHECK (container_format IN ('zip','stored_tar')) NOT VALID;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='archive_runs'::regclass
          AND conname='archive_runs_remote_chunk_index_ck'
    ) THEN
        ALTER TABLE archive_runs
            ADD CONSTRAINT archive_runs_remote_chunk_index_ck
            CHECK (remote_chunk_index IS NULL OR remote_chunk_index >= 0)
            NOT VALID;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='archive_runs'::regclass
          AND conname='archive_runs_tape_generation_fk'
    ) THEN
        ALTER TABLE archive_runs
            ADD CONSTRAINT archive_runs_tape_generation_fk
            FOREIGN KEY (tape_generation_id)
            REFERENCES tape_generations(generation_id)
            ON DELETE RESTRICT NOT VALID;
    END IF;
END $$;

-- ---------------------------------------------------------------------------
-- Initial evidence-gated assignment.
-- ---------------------------------------------------------------------------
DO $$
DECLARE
    exception_text       TEXT := nullif(current_setting(
                               'lto.container_format_exception_session_id', true), '');
    expected_text        TEXT := nullif(current_setting(
                               'lto.container_format_expected_boundary', true), '');
    approval_id_text     TEXT := nullif(current_setting(
                               'lto.container_format_approval_id', true), '');
    approval_reason_text TEXT := nullif(current_setting(
                               'lto.container_format_approval_reason', true), '');
    staging_text         TEXT := nullif(current_setting(
                               'lto.container_format_staging_evidence', true), '');
    exception_session    BIGINT;
    expected_boundary    INTEGER;
    derived_boundary     INTEGER;
    maximum_index        INTEGER;
    minimum_index        INTEGER;
    chunk_count          BIGINT;
    existing_boundary    RECORD;
    staging_evidence     JSONB;
    candidate_indexes    INTEGER[];
    checked_indexes      INTEGER[];
    blocked_indexes      INTEGER[];
    optional_012_count   INTEGER;
    optional_007_count   INTEGER;
    ambiguous_file_count BIGINT;
    ambiguous_run_count  BIGINT;
    ambiguous_directory_count BIGINT := 0;
BEGIN
    IF exception_text IS NULL THEN
        IF expected_text IS NOT NULL OR approval_id_text IS NOT NULL
           OR approval_reason_text IS NOT NULL OR staging_text IS NOT NULL THEN
            RAISE EXCEPTION
                'migration exception settings are incomplete';
        END IF;
        UPDATE remote_sessions
        SET default_packaging_format='zip'
        WHERE default_packaging_format IS NULL;
        UPDATE remote_chunks
        SET packaging_format='zip',
            packaging_assigned_at=COALESCE(packaging_assigned_at, now())
        WHERE packaging_format IS NULL;
    ELSE
        IF exception_text !~ '^[1-9][0-9]*$'
           OR expected_text IS NULL OR expected_text !~ '^[0-9]+$'
           OR approval_id_text IS NULL OR approval_reason_text IS NULL
           OR staging_text IS NULL THEN
            RAISE EXCEPTION
                'approved migration exception requires session, expected boundary, approval id/reason, and staging evidence';
        END IF;
        exception_session := exception_text::BIGINT;
        expected_boundary := expected_text::INTEGER;
        staging_evidence := staging_text::JSONB;

        PERFORM 1 FROM remote_sessions
        WHERE session_id=exception_session FOR UPDATE;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'approved exception session % does not exist',
                exception_session;
        END IF;

        SELECT * INTO existing_boundary
        FROM remote_packaging_boundaries
        WHERE session_id=exception_session FOR UPDATE;

        IF FOUND THEN
            IF existing_boundary.first_stored_tar_chunk_index <> expected_boundary
               OR existing_boundary.approval_id <> approval_id_text THEN
                RAISE EXCEPTION
                    'persisted packaging boundary/approval differs for session %',
                    exception_session;
            END IF;
            IF EXISTS (
                SELECT 1 FROM remote_chunks c
                WHERE c.session_id=exception_session
                  AND c.packaging_format <>
                      CASE WHEN c.chunk_index >= expected_boundary
                           THEN 'stored_tar' ELSE 'zip' END
            ) THEN
                RAISE EXCEPTION
                    'persisted packaging boundary conflicts with chunk formats for session %',
                    exception_session;
            END IF;
            -- Idempotent re-application: no format assignment occurs here.
        ELSE
            IF EXISTS (
                SELECT 1 FROM remote_chunks
                WHERE session_id=exception_session
                  AND packaging_format IS NOT NULL
            ) THEN
                RAISE EXCEPTION
                    'session % was already backfilled without an approved boundary; formats are write-once',
                    exception_session;
            END IF;

            DROP TABLE IF EXISTS pg_temp.plan2_format_evidence;
            CREATE TEMP TABLE plan2_format_evidence ON COMMIT DROP AS
            SELECT
                c.session_id,
                c.chunk_index,
                c.status,
                c.error_msg,
                c.owner_token,
                c.lease_expires_at,
                c.attempt_id,
                c.membership_state,
                c.expected_file_count,
                c.expected_bytes,
                COALESCE(m.member_count, 0)::BIGINT AS member_count,
                COALESCE(m.distinct_ordinal_count, 0)::BIGINT
                    AS distinct_ordinal_count,
                m.first_ordinal::BIGINT AS first_ordinal,
                m.last_ordinal::BIGINT AS last_ordinal,
                COALESCE(m.logical_bytes, 0)::BIGINT AS logical_bytes,
                (SELECT count(*) FROM remote_file_state fs
                 JOIN remote_plan_files pfs
                   ON pfs.plan_file_id=fs.plan_file_id
                 WHERE fs.session_id=c.session_id
                   AND pfs.plan_id=s.plan_id
                   AND pfs.chunk_index=c.chunk_index)::BIGINT
                    AS file_state_count,
                (SELECT count(*) FROM remote_worker_attempts wa
                 WHERE wa.session_id=c.session_id
                   AND wa.chunk_index=c.chunk_index)::BIGINT
                    AS worker_attempt_count,
                (SELECT count(*) FROM files_index fi
                 WHERE fi.remote_session_id=c.session_id
                   AND fi.remote_chunk_index=c.chunk_index)::BIGINT
                    AS catalog_file_count,
                (SELECT count(*) FROM archive_runs ar
                 WHERE ar.remote_session_id=c.session_id
                   AND ar.remote_chunk_index=c.chunk_index)::BIGINT
                    AS archive_run_count,
                (SELECT count(*) FROM archive_containers ac
                 WHERE ac.session_id=c.session_id
                   AND ac.chunk_index=c.chunk_index)::BIGINT
                    AS container_count,
                (SELECT count(*) FROM archive_containers ac
                 WHERE ac.session_id=c.session_id
                   AND ac.chunk_index=c.chunk_index
                   AND (ac.tape_path IS NOT NULL
                        OR ac.writer_state IN ('writing','copied','ambiguous')
                        OR ac.catalog_state IN ('committing','committed','ambiguous'))
                )::BIGINT AS written_container_count,
                (SELECT count(*) FROM archive_artifacts aa
                 WHERE aa.session_id=c.session_id
                   AND aa.chunk_index=c.chunk_index)::BIGINT
                    AS artifact_count,
                0::BIGINT AS directory_evidence_count,
                0::BIGINT AS sealed_batch_evidence_count,
                0::BIGINT AS sealed_batch_written_count,
                FALSE AS fixed_membership,
                FALSE AS eligible_stored_tar,
                FALSE AS immutable_zip
            FROM remote_chunks c
            JOIN remote_sessions s ON s.session_id=c.session_id
            LEFT JOIN LATERAL (
                SELECT count(*) AS member_count,
                       count(DISTINCT pf.ordinal) AS distinct_ordinal_count,
                       min(pf.ordinal) AS first_ordinal,
                       max(pf.ordinal) AS last_ordinal,
                       COALESCE(sum(sf.file_size_bytes), 0) AS logical_bytes
                FROM remote_plan_files pf
                JOIN remote_snapshot_files sf
                  ON sf.snapshot_file_id=pf.snapshot_file_id
                WHERE pf.plan_id=s.plan_id
                  AND pf.chunk_index=c.chunk_index
            ) m ON TRUE
            WHERE c.session_id=exception_session;

            SELECT count(*) INTO optional_007_count
            FROM (VALUES ('directory_archive_stats'),
                         ('directory_archive_bundles'),
                         ('directory_tree_index')) AS wanted(name)
            WHERE to_regclass('public.' || wanted.name) IS NOT NULL;
            -- Migration-007 chunk_index came from the historical local-chunk
            -- field and is not remote provenance.  It is deliberately never
            -- joined to a remote chunk here.  After the boundary is derived,
            -- directory rows are reconciled only through archive_run_id plus
            -- exact files_index remote provenance; anything else blocks.

            SELECT count(*) INTO optional_012_count
            FROM (VALUES ('tape_write_batches'),
                         ('tape_write_batch_chunks'),
                         ('tape_write_active_chunk')) AS wanted(name)
            WHERE to_regclass('public.' || wanted.name) IS NOT NULL;
            IF optional_012_count NOT IN (0, 3) THEN
                RAISE EXCEPTION
                    'migration 012 is partially installed (% of 3 tables); evidence is indeterminate',
                    optional_012_count;
            END IF;
            IF optional_012_count = 3 THEN
                LOCK TABLE tape_write_batches IN SHARE MODE;
                LOCK TABLE tape_write_batch_chunks IN SHARE MODE;
                LOCK TABLE tape_write_active_chunk IN SHARE MODE;
                EXECUTE $sql$
                    UPDATE pg_temp.plan2_format_evidence e
                    SET sealed_batch_evidence_count =
                        (SELECT count(*) FROM tape_write_batch_chunks bc
                         WHERE bc.session_id=e.session_id
                           AND bc.chunk_index=e.chunk_index)
                        +
                        (SELECT count(*) FROM tape_write_active_chunk ac
                         WHERE ac.session_id=e.session_id
                           AND ac.chunk_index=e.chunk_index),
                        sealed_batch_written_count =
                        (SELECT count(*)
                         FROM tape_write_batch_chunks bc
                         JOIN tape_write_batches b ON b.batch_id=bc.batch_id
                         WHERE bc.session_id=e.session_id
                           AND bc.chunk_index=e.chunk_index
                           AND (bc.member_write_phase='copied'
                                OR b.state='durable'))
                $sql$;
            END IF;

            UPDATE plan2_format_evidence e
            SET fixed_membership =
                    (e.membership_state IS NULL
                         OR e.membership_state='sealed')
                    AND e.member_count > 0
                    AND e.distinct_ordinal_count=e.member_count
                    AND e.first_ordinal=0
                    AND e.last_ordinal=e.member_count-1
                    AND (e.expected_file_count IS NULL
                         OR e.expected_file_count=e.member_count)
                    AND (e.expected_bytes IS NULL
                         OR e.expected_bytes=e.logical_bytes),
                eligible_stored_tar =
                    e.status='pending'
                    AND e.error_msg IS NULL
                    AND e.owner_token IS NULL
                    AND e.lease_expires_at IS NULL
                    AND e.attempt_id IS NULL
                    AND (e.membership_state IS NULL
                         OR e.membership_state='sealed')
                    AND e.member_count > 0
                    AND e.distinct_ordinal_count=e.member_count
                    AND e.first_ordinal=0
                    AND e.last_ordinal=e.member_count-1
                    AND (e.expected_file_count IS NULL
                         OR e.expected_file_count=e.member_count)
                    AND (e.expected_bytes IS NULL
                         OR e.expected_bytes=e.logical_bytes)
                    AND e.file_state_count=0
                    AND e.worker_attempt_count=0
                    AND e.catalog_file_count=0
                    AND e.archive_run_count=0
                    AND e.container_count=0
                    AND e.artifact_count=0
                    AND e.directory_evidence_count=0
                    AND e.sealed_batch_evidence_count=0,
                immutable_zip =
                    (e.catalog_file_count > 0
                     OR e.archive_run_count > 0
                     OR e.container_count > 0
                     OR e.artifact_count > 0
                     OR e.written_container_count > 0
                     OR e.sealed_batch_written_count > 0);

            SELECT count(*) INTO ambiguous_file_count
            FROM files_index fi
            WHERE fi.remote_session_id=exception_session
              AND (fi.remote_chunk_index IS NULL OR NOT EXISTS (
                    SELECT 1 FROM remote_chunks c
                    WHERE c.session_id=exception_session
                      AND c.chunk_index=fi.remote_chunk_index));
            IF ambiguous_file_count <> 0 THEN
                RAISE EXCEPTION
                    'session % has % catalog rows without trustworthy remote chunk provenance',
                    exception_session, ambiguous_file_count;
            END IF;

            SELECT count(*) INTO ambiguous_run_count
            FROM archive_runs ar
            WHERE ar.remote_session_id=exception_session
              AND (
                (ar.remote_chunk_index IS NOT NULL AND (
                    NOT EXISTS (SELECT 1 FROM remote_chunks c
                                WHERE c.session_id=exception_session
                                  AND c.chunk_index=ar.remote_chunk_index)
                    OR EXISTS (SELECT 1 FROM files_index fi
                               WHERE fi.archive_run_id=ar.run_id
                                 AND (fi.remote_session_id IS DISTINCT FROM
                                          exception_session
                                      OR fi.remote_chunk_index IS DISTINCT FROM
                                          ar.remote_chunk_index))))
                OR
                (ar.remote_chunk_index IS NULL AND (
                    NOT EXISTS (SELECT 1 FROM files_index fi
                                WHERE fi.archive_run_id=ar.run_id
                                  AND fi.remote_session_id=exception_session
                                  AND fi.remote_chunk_index IS NOT NULL)
                    OR EXISTS (SELECT 1 FROM files_index fi
                               WHERE fi.archive_run_id=ar.run_id
                                 AND (fi.remote_session_id IS DISTINCT FROM
                                          exception_session
                                      OR fi.remote_chunk_index IS NULL)))));
            IF ambiguous_run_count <> 0 THEN
                RAISE EXCEPTION
                    'session % has % archive runs without trustworthy remote chunk provenance',
                    exception_session, ambiguous_run_count;
            END IF;

            SELECT min(chunk_index), max(chunk_index), count(*)
            INTO minimum_index, maximum_index, chunk_count
            FROM plan2_format_evidence;
            IF chunk_count=0 OR minimum_index<>0
               OR chunk_count<>maximum_index+1 THEN
                RAISE EXCEPTION
                    'session % chunk identities are absent or non-contiguous; refusing format assignment',
                    exception_session;
            END IF;

            SELECT min(chunk_index) INTO derived_boundary
            FROM plan2_format_evidence WHERE eligible_stored_tar;
            IF derived_boundary IS NULL THEN
                RAISE EXCEPTION
                    'session % has no evidence-proven never-started suffix',
                    exception_session;
            END IF;
            IF derived_boundary <> expected_boundary THEN
                RAISE EXCEPTION
                    'derived boundary % differs from approved expected boundary % for session %',
                    derived_boundary, expected_boundary, exception_session;
            END IF;

            IF optional_007_count = 3 THEN
                -- Migration-007's historical chunk_index is not remote
                -- provenance.  Missing archive_run/files_index linkage is an
                -- expected legacy gap and cannot grant TAR, so it does not
                -- block the conservative ZIP prefix.  A linked file that does
                -- positively place a directory row in the TAR suffix (or a
                -- different session) remains contradictory and aborts.
                EXECUTE $sql$
                    SELECT count(*) FROM (
                        SELECT d.archive_run_id
                        FROM directory_archive_stats d
                        WHERE d.remote_session_id=$1
                        UNION ALL
                        SELECT d.archive_run_id
                        FROM directory_archive_bundles d
                        WHERE d.remote_session_id=$1
                        UNION ALL
                        SELECT d.archive_run_id
                        FROM directory_tree_index d
                        WHERE d.remote_session_id=$1
                    ) d
                    WHERE EXISTS (
                            SELECT 1 FROM files_index fi
                            WHERE fi.archive_run_id=d.archive_run_id
                              AND (fi.remote_session_id IS DISTINCT FROM $1
                                   OR (fi.remote_chunk_index IS NOT NULL
                                       AND fi.remote_chunk_index >= $2)))
                $sql$ INTO ambiguous_directory_count
                USING exception_session, derived_boundary;
                IF ambiguous_directory_count <> 0 THEN
                    RAISE EXCEPTION
                        'session % has % directory catalog rows with contradictory TAR-suffix provenance',
                        exception_session, ambiguous_directory_count;
                END IF;
            END IF;

            -- ZIP is the conservative legacy default.  Positive written
            -- evidence is retained as corroboration, but requiring it merely
            -- to choose that safe default buys no safety and would strand
            -- historical done chunks whose per-chunk provenance predates the
            -- catalog fields.  Fixed, contiguous membership plus status=done
            -- is sufficient for the prefix.  This cannot grant TAR: the
            -- unchanged eligible_stored_tar predicate independently requires
            -- status=pending and zero operational/catalog evidence.
            SELECT array_agg(chunk_index ORDER BY chunk_index)
            INTO blocked_indexes
            FROM plan2_format_evidence
            WHERE (chunk_index < derived_boundary
                   AND (status<>'done' OR NOT fixed_membership))
               OR (chunk_index >= derived_boundary AND NOT eligible_stored_tar);
            IF blocked_indexes IS NOT NULL THEN
                RAISE EXCEPTION
                    'session % has contradictory/indeterminate chunk evidence at indexes %',
                    exception_session, blocked_indexes;
            END IF;

            SELECT array_agg(chunk_index ORDER BY chunk_index)
            INTO candidate_indexes
            FROM plan2_format_evidence
            WHERE chunk_index >= derived_boundary;

            IF COALESCE((staging_evidence->>'root_accessible')::BOOLEAN, FALSE)
                   IS NOT TRUE
               OR COALESCE((staging_evidence->>'entry_count')::BIGINT, -1) <> 0
               OR COALESCE((staging_evidence->>'unreadable_count')::BIGINT, -1) <> 0
               OR staging_evidence->>'checked_at' IS NULL
               OR (SELECT count(*) FROM jsonb_object_keys(staging_evidence)) <> 5
               OR EXISTS (
                    SELECT 1
                    FROM jsonb_object_keys(staging_evidence) AS keys(key_name)
                    WHERE key_name NOT IN
                        ('root_accessible','checked_chunk_indexes',
                         'entry_count','unreadable_count','checked_at'))
               OR (staging_evidence->>'checked_at')::TIMESTAMPTZ <
                    clock_timestamp() - interval '5 minutes'
               OR (staging_evidence->>'checked_at')::TIMESTAMPTZ >
                    clock_timestamp() + interval '5 seconds' THEN
                RAISE EXCEPTION
                    'local staging evidence is missing, stale, unreadable, non-empty, or non-canonical';
            END IF;
            SELECT array_agg(value::INTEGER ORDER BY value::INTEGER)
            INTO checked_indexes
            FROM jsonb_array_elements_text(
                COALESCE(staging_evidence->'checked_chunk_indexes', '[]'::JSONB));
            IF checked_indexes IS DISTINCT FROM candidate_indexes THEN
                RAISE EXCEPTION
                    'local staging evidence covered %, expected %',
                    checked_indexes, candidate_indexes;
            END IF;

            INSERT INTO remote_packaging_boundaries
                (session_id, first_stored_tar_chunk_index,
                 last_existing_chunk_index, approval_id, approval_reason,
                 database_evidence, local_staging_evidence,
                 evidence_checked_at)
            VALUES
                (exception_session, derived_boundary, maximum_index,
                 approval_id_text, approval_reason_text,
                 jsonb_build_object(
                      'chunk_count', chunk_count,
                      'immutable_zip_count', (SELECT count(*)
                          FROM plan2_format_evidence WHERE immutable_zip),
                      'corroborated_prefix_count', (SELECT count(*)
                          FROM plan2_format_evidence
                          WHERE chunk_index < derived_boundary
                            AND immutable_zip),
                      'status_only_prefix_count', (SELECT count(*)
                          FROM plan2_format_evidence
                          WHERE chunk_index < derived_boundary
                            AND NOT immutable_zip),
                     'approved_stored_tar_count', (SELECT count(*)
                         FROM plan2_format_evidence WHERE eligible_stored_tar),
                     'first_stored_tar_chunk_index', derived_boundary,
                     'last_existing_chunk_index', maximum_index),
                 staging_evidence,
                 (staging_evidence->>'checked_at')::TIMESTAMPTZ);

            INSERT INTO remote_packaging_boundary_chunks
                (session_id, chunk_index, classification, assigned_format,
                 prefix_evidence_basis,
                 plan_member_count, plan_logical_bytes,
                 first_ordinal, last_ordinal, evidence)
            SELECT session_id, chunk_index,
                   CASE WHEN chunk_index < derived_boundary
                        THEN 'immutable_zip'
                        ELSE 'approved_stored_tar_exception' END,
                   CASE WHEN chunk_index < derived_boundary
                        THEN 'zip' ELSE 'stored_tar' END,
                   CASE WHEN chunk_index >= derived_boundary THEN NULL
                        WHEN immutable_zip THEN 'corroborated'
                        ELSE 'status_only' END,
                   member_count, logical_bytes, first_ordinal, last_ordinal,
                   jsonb_build_object(
                       'status', status,
                       'fixed_membership', fixed_membership,
                       'prefix_evidence_basis',
                           CASE WHEN chunk_index >= derived_boundary THEN NULL
                                WHEN immutable_zip THEN 'corroborated'
                                ELSE 'status_only' END,
                       'membership_state', membership_state,
                       'file_state_count', file_state_count,
                       'worker_attempt_count', worker_attempt_count,
                       'catalog_file_count', catalog_file_count,
                       'archive_run_count', archive_run_count,
                       'container_count', container_count,
                       'artifact_count', artifact_count,
                       'directory_evidence_count', directory_evidence_count,
                       'sealed_batch_evidence_count',
                           sealed_batch_evidence_count,
                       'written_container_count', written_container_count,
                       'sealed_batch_written_count',
                           sealed_batch_written_count)
            FROM plan2_format_evidence
            ORDER BY chunk_index;

            UPDATE remote_sessions
            SET default_packaging_format =
                CASE WHEN session_id=exception_session
                     THEN 'stored_tar' ELSE 'zip' END
            WHERE default_packaging_format IS NULL
               OR session_id=exception_session;

            UPDATE remote_chunks
            SET packaging_format =
                    CASE WHEN session_id=exception_session
                              AND chunk_index>=derived_boundary
                         THEN 'stored_tar' ELSE 'zip' END,
                packaging_assigned_at=COALESCE(packaging_assigned_at, now())
            WHERE packaging_format IS NULL;
        END IF;

        UPDATE remote_sessions
        SET default_packaging_format='zip'
        WHERE default_packaging_format IS NULL;
        UPDATE remote_chunks
        SET packaging_format='zip',
            packaging_assigned_at=COALESCE(packaging_assigned_at, now())
        WHERE packaging_format IS NULL;
    END IF;

    IF EXISTS (SELECT 1 FROM remote_chunks WHERE packaging_format IS NULL
               OR packaging_assigned_at IS NULL) THEN
        RAISE EXCEPTION
            'migration 015 assignment left NULL chunk format evidence';
    END IF;
END $$;

UPDATE archive_bundles
SET container_format='zip'
WHERE container_format IS NULL;

ALTER TABLE remote_sessions
    ALTER COLUMN default_packaging_format SET DEFAULT 'zip',
    ALTER COLUMN default_packaging_format SET NOT NULL;

ALTER TABLE remote_chunks
    ALTER COLUMN packaging_format SET NOT NULL,
    ALTER COLUMN packaging_assigned_at SET NOT NULL;

ALTER TABLE archive_bundles
    ALTER COLUMN container_format SET DEFAULT 'zip',
    ALTER COLUMN container_format SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='remote_sessions'::regclass
          AND conname='remote_sessions_default_packaging_format_ck'
    ) THEN
        ALTER TABLE remote_sessions
            ADD CONSTRAINT remote_sessions_default_packaging_format_ck
            CHECK (default_packaging_format IN ('zip','stored_tar'));
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='remote_chunks'::regclass
          AND conname='remote_chunks_packaging_format_ck'
    ) THEN
        ALTER TABLE remote_chunks
            ADD CONSTRAINT remote_chunks_packaging_format_ck
            CHECK (packaging_format IN ('zip','stored_tar'));
    END IF;
END $$;

-- Write-once format fields.  Same-value idempotent updates are allowed.
CREATE OR REPLACE FUNCTION lto_reject_packaging_format_change()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.packaging_format IS DISTINCT FROM NEW.packaging_format
       OR OLD.packaging_assigned_at IS DISTINCT FROM NEW.packaging_assigned_at THEN
        RAISE EXCEPTION
            'remote chunk packaging format is write-once (session %, chunk %)',
            OLD.session_id, OLD.chunk_index;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_remote_chunks_packaging_write_once ON remote_chunks;
CREATE TRIGGER trg_remote_chunks_packaging_write_once
BEFORE UPDATE OF packaging_format, packaging_assigned_at ON remote_chunks
FOR EACH ROW EXECUTE FUNCTION lto_reject_packaging_format_change();

CREATE OR REPLACE FUNCTION lto_reject_container_format_change()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.container_format IS DISTINCT FROM NEW.container_format THEN
        RAISE EXCEPTION 'container format is immutable for container %',
            OLD.container_id;
    END IF;
    RETURN NEW;
END $$;

CREATE OR REPLACE FUNCTION lto_reject_container_identity_change()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(OLD.session_id, OLD.chunk_index, OLD.container_ordinal,
           OLD.container_format, OLD.format_version, OLD.tar_dialect,
           OLD.storage_class, OLD.container_name,
           OLD.expected_member_count, OLD.expected_logical_bytes)
       IS DISTINCT FROM
       ROW(NEW.session_id, NEW.chunk_index, NEW.container_ordinal,
           NEW.container_format, NEW.format_version, NEW.tar_dialect,
           NEW.storage_class, NEW.container_name,
           NEW.expected_member_count, NEW.expected_logical_bytes) THEN
        RAISE EXCEPTION 'archive container identity is immutable for container %',
            OLD.container_id;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_archive_containers_format_write_once
    ON archive_containers;
CREATE TRIGGER trg_archive_containers_format_write_once
BEFORE UPDATE OF session_id, chunk_index, container_ordinal, container_format,
                 format_version, tar_dialect, storage_class, container_name,
                 expected_member_count, expected_logical_bytes
ON archive_containers
FOR EACH ROW EXECUTE FUNCTION lto_reject_container_identity_change();

DROP TRIGGER IF EXISTS trg_archive_bundles_format_write_once ON archive_bundles;
CREATE TRIGGER trg_archive_bundles_format_write_once
BEFORE UPDATE OF container_format ON archive_bundles
FOR EACH ROW EXECUTE FUNCTION lto_reject_container_format_change();

-- A session default may become Stored TAR only after a durable boundary exists;
-- it cannot be changed back while TAR chunk identities exist.
CREATE OR REPLACE FUNCTION lto_guard_session_packaging_default()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='INSERT' THEN
        IF NEW.default_packaging_format='stored_tar' THEN
            RAISE EXCEPTION
                'new sessions default to zip; Stored TAR needs an approved boundary';
        END IF;
        RETURN NEW;
    END IF;
    IF OLD.default_packaging_format IS NOT DISTINCT FROM
       NEW.default_packaging_format THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION
        'remote session packaging default is immutable after initial assignment';
END $$;

DROP TRIGGER IF EXISTS trg_remote_sessions_packaging_default
    ON remote_sessions;
CREATE TRIGGER trg_remote_sessions_packaging_default
BEFORE INSERT OR UPDATE OF default_packaging_format ON remote_sessions
FOR EACH ROW EXECUTE FUNCTION lto_guard_session_packaging_default();

-- Database-side policy used by PgContainerMixin.assign_new_chunk_format().
-- It returns the only legal initial value; the INSERT and this function run in
-- the same transaction.  Existing rows are never changed by it.
CREATE OR REPLACE FUNCTION lto_assign_new_chunk_format(
    p_session_id BIGINT,
    p_chunk_index INTEGER,
    p_stored_tar_write_enabled BOOLEAN,
    p_schema_version INTEGER,
    p_reader_contract_version INTEGER
) RETURNS TEXT LANGUAGE plpgsql STABLE AS $$
DECLARE
    session_default TEXT;
    boundary_index INTEGER;
    installed_version INTEGER;
    required_reader_version INTEGER;
    result_format TEXT;
BEGIN
    SELECT schema_version, required_reader_contract_version
    INTO installed_version, required_reader_version
    FROM container_format_schema_metadata WHERE singleton;
    IF installed_version IS NULL OR installed_version<>p_schema_version THEN
        RAISE EXCEPTION
            'container-format schema missing or version mismatch (installed %, caller %)',
            installed_version, p_schema_version;
    END IF;

    SELECT default_packaging_format INTO session_default
    FROM remote_sessions WHERE session_id=p_session_id;
    IF session_default IS NULL THEN
        RAISE EXCEPTION 'remote session % does not exist', p_session_id;
    END IF;
    SELECT first_stored_tar_chunk_index INTO boundary_index
    FROM remote_packaging_boundaries WHERE session_id=p_session_id;

    result_format := CASE
        WHEN boundary_index IS NOT NULL AND p_chunk_index>=boundary_index
            THEN 'stored_tar'
        WHEN boundary_index IS NOT NULL AND p_chunk_index<boundary_index
            THEN 'zip'
        ELSE session_default
    END;

    IF result_format<>session_default THEN
        RAISE EXCEPTION
            'session default/boundary conflict for session %, chunk %',
            p_session_id, p_chunk_index;
    END IF;
    IF result_format='stored_tar' THEN
        IF NOT COALESCE(p_stored_tar_write_enabled, FALSE) THEN
            RAISE EXCEPTION
                'Stored TAR assignment is disabled for session %, chunk %',
                p_session_id, p_chunk_index;
        END IF;
        IF p_reader_contract_version IS NULL
           OR p_reader_contract_version<>required_reader_version THEN
            RAISE EXCEPTION
                'Stored TAR reader contract mismatch (required %, caller %)',
                required_reader_version, p_reader_contract_version;
        END IF;
    END IF;
    RETURN result_format;
END $$;

-- A direct SQL insert cannot contradict the durable session boundary.  The
-- application gate above supplies config/reader readiness; this trigger supplies
-- the database identity invariant.
CREATE OR REPLACE FUNCTION lto_guard_new_chunk_format()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    session_default TEXT;
    boundary_index INTEGER;
    expected_format TEXT;
BEGIN
    SELECT default_packaging_format INTO session_default
    FROM remote_sessions WHERE session_id=NEW.session_id;
    SELECT first_stored_tar_chunk_index INTO boundary_index
    FROM remote_packaging_boundaries WHERE session_id=NEW.session_id;
    expected_format := CASE
        WHEN boundary_index IS NOT NULL
             AND NEW.chunk_index>=boundary_index THEN 'stored_tar'
        WHEN boundary_index IS NOT NULL THEN 'zip'
        ELSE session_default
    END;
    IF NEW.packaging_format IS DISTINCT FROM expected_format
       OR NEW.packaging_assigned_at IS NULL THEN
        RAISE EXCEPTION
            'initial chunk format % conflicts with durable format % for session %, chunk %',
            NEW.packaging_format, expected_format,
            NEW.session_id, NEW.chunk_index;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_remote_chunks_initial_format ON remote_chunks;
CREATE TRIGGER trg_remote_chunks_initial_format
BEFORE INSERT ON remote_chunks
FOR EACH ROW EXECUTE FUNCTION lto_guard_new_chunk_format();

-- Tape attribution belongs to the exact generation selected when the container
-- enters a write.  A later change to remote_sessions.tape_label cannot rewrite
-- this history.
CREATE OR REPLACE FUNCTION lto_guard_container_tape_generation()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    generation_label TEXT;
    generation_number INTEGER;
    session_label TEXT;
    session_generation INTEGER;
BEGIN
    IF NEW.tape_generation_id IS NULL THEN
        IF NEW.tape_label IS NOT NULL OR NEW.tape_path IS NOT NULL
           OR NEW.writer_state <> 'not_started'
           OR NEW.catalog_state <> 'not_started' THEN
            RAISE EXCEPTION
                'container write/catalog state requires exact tape generation';
        END IF;
        RETURN NEW;
    END IF;
    SELECT g.volume_label, g.generation
    INTO generation_label, generation_number
    FROM tape_generations g WHERE g.generation_id=NEW.tape_generation_id;
    SELECT s.tape_label, s.tape_generation
    INTO session_label, session_generation
    FROM remote_sessions s WHERE s.session_id=NEW.session_id;
    IF generation_label IS DISTINCT FROM NEW.tape_label
       OR session_label IS DISTINCT FROM NEW.tape_label
       OR session_generation IS DISTINCT FROM generation_number THEN
        RAISE EXCEPTION
            'container tape generation does not match session/tape assignment';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_archive_containers_tape_generation
    ON archive_containers;
CREATE TRIGGER trg_archive_containers_tape_generation
BEFORE INSERT OR UPDATE OF tape_label, tape_path, tape_generation_id,
                           writer_state, catalog_state
ON archive_containers
FOR EACH ROW EXECUTE FUNCTION lto_guard_container_tape_generation();

CREATE OR REPLACE FUNCTION lto_guard_artifact_container_format()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    durable_format TEXT;
BEGIN
    IF NEW.container_id IS NULL THEN
        RETURN NEW;
    END IF;
    SELECT container_format INTO durable_format
    FROM archive_containers WHERE container_id=NEW.container_id;
    IF (NEW.artifact_kind='tar_sidecar' AND durable_format<>'stored_tar')
       OR (NEW.artifact_kind='zip_manifest' AND durable_format<>'zip') THEN
        RAISE EXCEPTION
            'container-scoped artifact kind conflicts with durable format';
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_archive_artifacts_container_format
    ON archive_artifacts;
CREATE TRIGGER trg_archive_artifacts_container_format
BEFORE INSERT OR UPDATE OF container_id, artifact_kind ON archive_artifacts
FOR EACH ROW EXECUTE FUNCTION lto_guard_artifact_container_format();

CREATE OR REPLACE FUNCTION lto_reject_artifact_identity_change()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(OLD.session_id, OLD.chunk_index, OLD.container_id,
           OLD.artifact_kind, OLD.artifact_version)
       IS DISTINCT FROM
       ROW(NEW.session_id, NEW.chunk_index, NEW.container_id,
           NEW.artifact_kind, NEW.artifact_version) THEN
        RAISE EXCEPTION 'archive artifact identity is immutable for artifact %',
            OLD.artifact_id;
    END IF;
    RETURN NEW;
END $$;

DROP TRIGGER IF EXISTS trg_archive_artifacts_identity_write_once
    ON archive_artifacts;
CREATE TRIGGER trg_archive_artifacts_identity_write_once
BEFORE UPDATE OF session_id, chunk_index, container_id, artifact_kind,
                 artifact_version
ON archive_artifacts
FOR EACH ROW EXECUTE FUNCTION lto_reject_artifact_identity_change();

-- Canonical catalog fingerprint. It covers all columns/constraints/indexes on
-- the new tables, every additive existing-table column/constraint/index, and
-- all migration-015 triggers/functions including enabled/validated state.
CREATE OR REPLACE FUNCTION lto_container_format_schema_fingerprint()
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
        c.relname IN ('container_format_schema_metadata',
                      'remote_packaging_boundaries',
                      'remote_packaging_boundary_chunks',
                      'archive_containers','archive_artifacts')
        OR (c.relname='remote_sessions'
            AND a.attname='default_packaging_format')
        OR (c.relname='remote_chunks' AND a.attname IN
            ('packaging_format','packaging_assigned_at','writer_started_at',
             'writer_completed_at','catalog_committed_at'))
        OR (c.relname='archive_bundles'
            AND a.attname IN ('container_id','container_format'))
        OR (c.relname='archive_runs'
            AND a.attname IN ('remote_chunk_index','tape_generation_id'))
        OR (c.relname='directory_archive_bundles' AND a.attname IN
            ('container_id','container_format','tape_generation_id',
             'actual_artifact_bytes')))
    UNION ALL
    SELECT 'constraint:' || rel.relname || '.' || con.conname,
           concat_ws('|', con.contype, con.convalidated::text,
                     pg_get_constraintdef(con.oid, true))
    FROM pg_constraint con
    JOIN pg_class rel ON rel.oid=con.conrelid
    JOIN pg_namespace n ON n.oid=rel.relnamespace
    WHERE n.nspname='public' AND (
        rel.relname IN ('container_format_schema_metadata',
                        'remote_packaging_boundaries',
                        'remote_packaging_boundary_chunks',
                        'archive_containers','archive_artifacts')
        OR con.conname IN
            ('remote_chunks_identity_format_uq',
             'remote_chunks_packaging_format_ck',
             'remote_sessions_default_packaging_format_ck',
             'archive_bundles_container_fk',
             'archive_bundles_container_format_ck',
             'archive_runs_remote_chunk_index_ck',
             'archive_runs_tape_generation_fk',
             'directory_archive_bundles_container_fk',
             'directory_archive_bundles_generation_fk',
             'directory_archive_bundles_container_format_ck',
             'directory_archive_bundles_artifact_bytes_ck'))
    UNION ALL
    SELECT 'index:' || idx.relname,
           concat_ws('|', i.indisvalid::text, i.indisready::text,
                     pg_get_indexdef(i.indexrelid, 0, true))
    FROM pg_index i
    JOIN pg_class idx ON idx.oid=i.indexrelid
    JOIN pg_namespace n ON n.oid=idx.relnamespace
    WHERE n.nspname='public' AND idx.relname IN
        ('uq_archive_artifacts_container_kind_version',
         'uq_archive_artifacts_chunk_kind_version',
         'uq_archive_runs_remote_chunk_generation')
    UNION ALL
    SELECT 'trigger:' || rel.relname || '.' || t.tgname,
           concat_ws('|', t.tgenabled, pg_get_triggerdef(t.oid, true))
    FROM pg_trigger t
    JOIN pg_class rel ON rel.oid=t.tgrelid
    JOIN pg_namespace n ON n.oid=rel.relnamespace
    WHERE n.nspname='public' AND NOT t.tgisinternal AND t.tgname IN
        ('trg_remote_packaging_boundaries_immutable',
         'trg_remote_packaging_boundary_chunks_immutable',
         'trg_remote_chunks_packaging_write_once',
         'trg_archive_containers_format_write_once',
         'trg_archive_bundles_format_write_once',
         'trg_remote_sessions_packaging_default',
         'trg_remote_chunks_initial_format',
         'trg_archive_containers_tape_generation',
         'trg_archive_artifacts_container_format',
         'trg_archive_artifacts_identity_write_once',
         'trg_container_format_schema_metadata_immutable')
    UNION ALL
    SELECT 'function:' || p.proname || '(' ||
               pg_get_function_identity_arguments(p.oid) || ')',
           concat_ws('|', p.provolatile, p.prosecdef::text,
                     pg_get_functiondef(p.oid))
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid=p.pronamespace
    WHERE n.nspname='public' AND p.proname IN
        ('lto_reject_packaging_boundary_mutation',
         'lto_reject_packaging_format_change',
         'lto_reject_container_format_change',
         'lto_reject_container_identity_change',
         'lto_reject_artifact_identity_change',
         'lto_guard_session_packaging_default',
         'lto_assign_new_chunk_format',
         'lto_guard_new_chunk_format',
         'lto_guard_container_tape_generation',
         'lto_guard_artifact_container_format',
         'lto_guard_container_format_metadata',
         'lto_container_format_schema_fingerprint')
)
SELECT md5(COALESCE(string_agg(
    object_key || '=' || object_definition, E'\n' ORDER BY object_key), ''))
FROM object_defs
$$;

CREATE OR REPLACE FUNCTION lto_guard_container_format_metadata()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='INSERT'
       AND NOT EXISTS (SELECT 1 FROM container_format_schema_metadata)
       AND NEW.migration_checksum = current_setting(
           'lto.container_format_migration_checksum', true) THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'container format schema authority is immutable';
END $$;

DROP TRIGGER IF EXISTS trg_container_format_schema_metadata_immutable
    ON container_format_schema_metadata;
CREATE TRIGGER trg_container_format_schema_metadata_immutable
BEFORE INSERT OR UPDATE OR DELETE ON container_format_schema_metadata
FOR EACH ROW EXECUTE FUNCTION lto_guard_container_format_metadata();

DO $$
DECLARE
    supplied_checksum TEXT := current_setting(
        'lto.container_format_migration_checksum');
    fingerprint TEXT := lto_container_format_schema_fingerprint();
    installed RECORD;
BEGIN
    SELECT * INTO installed FROM container_format_schema_metadata
    WHERE singleton;
    IF NOT FOUND THEN
        INSERT INTO container_format_schema_metadata
            (singleton, schema_version, required_reader_contract_version,
             stored_tar_dialect, migration_checksum, schema_fingerprint)
        VALUES
            (TRUE, 1, 1, 'gnu-pax-sparse-v1', supplied_checksum, fingerprint);
    ELSIF installed.schema_version<>1
       OR installed.required_reader_contract_version<>1
       OR installed.stored_tar_dialect<>'gnu-pax-sparse-v1'
       OR installed.migration_checksum<>supplied_checksum
       OR installed.schema_fingerprint<>fingerprint THEN
        RAISE EXCEPTION
            'installed migration 015 authority differs from current schema';
    END IF;
END $$;
