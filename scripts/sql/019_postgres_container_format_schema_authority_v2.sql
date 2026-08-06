-- =============================================================================
-- 019 - Additive v2 container-format schema authority (covers 015+016+017).
-- =============================================================================
--
-- Migration 015 recorded a `schema_fingerprint` computed over an UNFILTERED
-- column wildcard on `archive_containers`/`archive_artifacts`
-- (`c.relname IN (...)` with no column filter), and its computation itself
-- included its own function definition in the hash.  015's authority row is
-- also permanently immutable (INSERT-only, one bootstrap row, no UPDATE or
-- DELETE ever).  Migrations 016 and 017 legitimately ALTER `archive_containers`
-- to add columns -- that is their entire purpose -- so once 016/017 are
-- applied anywhere, 015's fingerprint can never match again: editing the
-- fingerprint function to re-scope it changes the function's own body, which
-- changes the very hash it computes, which still can never match the frozen
-- original.  This is a genuine, unfixable-in-place defect, discovered
-- 2026-08-06 on the first real archive write attempted after 015+016+017 were
-- all installed together for the first time in this codebase's history.
--
-- 015's row and its `lto_container_format_schema_fingerprint()` function are
-- UNCHANGED here and remain the sole authority for a database that has ONLY
-- 015 installed.  This migration is PURELY ADDITIVE:
--
--   * a new, separately-versioned authority table
--     (`container_format_schema_authority`), append-only and immutable the
--     same way 015's row is (no UPDATE, no DELETE, ever);
--   * a new fingerprint function scoped to an EXPLICIT, hand-enumerated
--     object list -- every table, every column (never a table-wide wildcard),
--     every named constraint, every index, every trigger, every protective
--     function -- so it never silently re-scopes itself as the schema grows;
--   * that function does NOT include its own definition, nor the definitions
--     of the new v2 bookkeeping objects this migration adds, in what it
--     hashes -- so a future, correctly-scoped v3 can be added the same way
--     without ever hitting this same trap again;
--   * one completeness check, run BEFORE the authority row is created, that
--     verifies every required table/column/constraint/index/trigger/function
--     exists and names exactly what is missing if it does not.
--
-- Idempotent: re-running this file when the authority_version=2 row already
-- exists changes nothing (`ON CONFLICT DO NOTHING`).
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 0. Prerequisites: 015, 016 and 017 must already be installed.
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF to_regclass('public.container_format_schema_metadata') IS NULL
       OR to_regclass('public.archive_containers') IS NULL
       OR to_regclass('public.archive_artifacts') IS NULL
       OR to_regclass('public.archive_container_members') IS NULL THEN
        RAISE EXCEPTION
            'migration 019 requires migrations 015 and 016 first';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema='public' AND table_name='archive_containers'
          AND column_name='validated_part_locator') THEN
        RAISE EXCEPTION 'migration 019 requires migration 017 first';
    END IF;
END $$;

-- ---------------------------------------------------------------------------
-- 1. Completeness check: every object the v2 contract will fingerprint must
--    actually exist, named individually, BEFORE any authority row is
--    written.  This is what proves "the exact required columns, constraints,
--    triggers and functions exist" rather than assuming it.
-- ---------------------------------------------------------------------------
DO $$
DECLARE
    missing_tables      TEXT[];
    missing_columns     TEXT[];
    missing_constraints TEXT[];
    missing_indexes     TEXT[];
    missing_triggers    TEXT[];
    missing_functions   TEXT[];
    directory_catalog_table_count INTEGER;
    missing_optional_directory_columns     TEXT[];
    missing_optional_directory_constraints TEXT[];
BEGIN
    -- directory_archive_bundles's container-format compatibility columns are
    -- added by migration 015 ONLY when migration 007's directory catalog
    -- (directory_archive_stats, directory_archive_bundles,
    -- directory_tree_index) was already installed -- the exact same
    -- optional_007_state trichotomy PgContainerMixin's v1 report uses.  A
    -- database with 015+016+017 but no 007 is a real, tested, supported
    -- shape (see tests/test_migration_015_preflight_parity.py), so these
    -- cannot be unconditionally required the way the rest of the contract is.
    SELECT count(*) INTO directory_catalog_table_count
    FROM unnest(ARRAY['directory_archive_stats', 'directory_archive_bundles',
                       'directory_tree_index']) AS t
    WHERE to_regclass('public.' || t) IS NOT NULL;
    IF directory_catalog_table_count NOT IN (0, 3) THEN
        RAISE EXCEPTION 'migration 019 contract incomplete: optional '
            'directory catalog (migration 007) is partially installed (% '
            'of 3 tables)', directory_catalog_table_count;
    END IF;

    IF directory_catalog_table_count = 3 THEN
        SELECT array_agg(req.table_name || '.' || req.column_name)
          INTO missing_optional_directory_columns
        FROM (VALUES
            ('directory_archive_bundles','container_id'),
            ('directory_archive_bundles','container_format'),
            ('directory_archive_bundles','tape_generation_id'),
            ('directory_archive_bundles','actual_artifact_bytes')
        ) AS req(table_name, column_name)
        WHERE NOT EXISTS (
            SELECT 1 FROM information_schema.columns c
            WHERE c.table_schema='public' AND c.table_name=req.table_name
              AND c.column_name=req.column_name);
        IF missing_optional_directory_columns IS NOT NULL THEN
            RAISE EXCEPTION
                'migration 019 contract incomplete: missing column(s): %',
                array_to_string(missing_optional_directory_columns, ', ');
        END IF;

        SELECT array_agg(req) INTO missing_optional_directory_constraints
        FROM unnest(ARRAY[
            'directory_archive_bundles_container_fk',
            'directory_archive_bundles_generation_fk',
            'directory_archive_bundles_container_format_ck',
            'directory_archive_bundles_artifact_bytes_ck']) AS req
        WHERE NOT EXISTS (
            SELECT 1 FROM pg_constraint con
            JOIN pg_class rel ON rel.oid=con.conrelid
            JOIN pg_namespace n ON n.oid=rel.relnamespace
            WHERE n.nspname='public' AND con.conname=req);
        IF missing_optional_directory_constraints IS NOT NULL THEN
            RAISE EXCEPTION
                'migration 019 contract incomplete: missing constraint(s): %',
                array_to_string(missing_optional_directory_constraints, ', ');
        END IF;
    END IF;

    SELECT array_agg(t) INTO missing_tables
    FROM unnest(ARRAY[
        'container_format_schema_metadata', 'remote_packaging_boundaries',
        'remote_packaging_boundary_chunks', 'archive_containers',
        'archive_artifacts', 'archive_container_members']) AS t
    WHERE to_regclass('public.' || t) IS NULL;
    IF missing_tables IS NOT NULL THEN
        RAISE EXCEPTION 'migration 019 contract incomplete: missing table(s): %',
            array_to_string(missing_tables, ', ');
    END IF;

    SELECT array_agg(req.table_name || '.' || req.column_name)
      INTO missing_columns
    FROM (VALUES
        ('container_format_schema_metadata','singleton'),
        ('container_format_schema_metadata','schema_version'),
        ('container_format_schema_metadata','required_reader_contract_version'),
        ('container_format_schema_metadata','stored_tar_dialect'),
        ('container_format_schema_metadata','migration_checksum'),
        ('container_format_schema_metadata','schema_fingerprint'),
        ('container_format_schema_metadata','applied_at'),
        ('remote_packaging_boundaries','session_id'),
        ('remote_packaging_boundaries','first_stored_tar_chunk_index'),
        ('remote_packaging_boundaries','last_existing_chunk_index'),
        ('remote_packaging_boundaries','approval_id'),
        ('remote_packaging_boundaries','approval_reason'),
        ('remote_packaging_boundaries','database_evidence'),
        ('remote_packaging_boundaries','local_staging_evidence'),
        ('remote_packaging_boundaries','evidence_checked_at'),
        ('remote_packaging_boundaries','created_at'),
        ('remote_packaging_boundary_chunks','session_id'),
        ('remote_packaging_boundary_chunks','chunk_index'),
        ('remote_packaging_boundary_chunks','classification'),
        ('remote_packaging_boundary_chunks','assigned_format'),
        ('remote_packaging_boundary_chunks','prefix_evidence_basis'),
        ('remote_packaging_boundary_chunks','plan_member_count'),
        ('remote_packaging_boundary_chunks','plan_logical_bytes'),
        ('remote_packaging_boundary_chunks','first_ordinal'),
        ('remote_packaging_boundary_chunks','last_ordinal'),
        ('remote_packaging_boundary_chunks','evidence'),
        ('archive_containers','container_id'),
        ('archive_containers','session_id'),
        ('archive_containers','chunk_index'),
        ('archive_containers','container_ordinal'),
        ('archive_containers','container_format'),
        ('archive_containers','format_version'),
        ('archive_containers','tar_dialect'),
        ('archive_containers','storage_class'),
        ('archive_containers','container_name'),
        ('archive_containers','temporary_data_locator'),
        ('archive_containers','permanent_local_metadata_locator'),
        ('archive_containers','tape_label'),
        ('archive_containers','tape_path'),
        ('archive_containers','tape_generation_id'),
        ('archive_containers','expected_member_count'),
        ('archive_containers','expected_logical_bytes'),
        ('archive_containers','observed_member_count'),
        ('archive_containers','observed_logical_bytes'),
        ('archive_containers','actual_artifact_bytes'),
        ('archive_containers','validation_state'),
        ('archive_containers','writer_state'),
        ('archive_containers','catalog_state'),
        ('archive_containers','owner_token'),
        ('archive_containers','lease_expires_at'),
        ('archive_containers','validation_started_at'),
        ('archive_containers','validated_at'),
        ('archive_containers','writer_started_at'),
        ('archive_containers','writer_completed_at'),
        ('archive_containers','catalog_committed_at'),
        ('archive_containers','created_at'),
        ('archive_containers','updated_at'),
        ('archive_containers','estimated_archive_bytes'),
        ('archive_containers','validated_part_locator'),
        ('archive_containers','validation_summary'),
        ('archive_containers','disposition_counts'),
        ('archive_artifacts','artifact_id'),
        ('archive_artifacts','session_id'),
        ('archive_artifacts','chunk_index'),
        ('archive_artifacts','container_id'),
        ('archive_artifacts','artifact_kind'),
        ('archive_artifacts','artifact_version'),
        ('archive_artifacts','local_locator'),
        ('archive_artifacts','tape_locator'),
        ('archive_artifacts','artifact_size_bytes'),
        ('archive_artifacts','readiness_state'),
        ('archive_artifacts','publication_started_at'),
        ('archive_artifacts','published_at'),
        ('archive_artifacts','tape_published_at'),
        ('archive_artifacts','created_at'),
        ('archive_artifacts','updated_at'),
        ('archive_container_members','session_id'),
        ('archive_container_members','chunk_index'),
        ('archive_container_members','plan_file_id'),
        ('archive_container_members','plan_ordinal'),
        ('archive_container_members','storage_class'),
        ('archive_container_members','container_id'),
        ('archive_container_members','container_ordinal'),
        ('archive_container_members','remote_path'),
        ('archive_container_members','expected_logical_bytes'),
        ('archive_container_members','estimated_tar_bytes'),
        ('archive_container_members','created_at'),
        ('remote_sessions','default_packaging_format'),
        ('remote_chunks','packaging_format'),
        ('remote_chunks','packaging_assigned_at'),
        ('remote_chunks','writer_started_at'),
        ('remote_chunks','writer_completed_at'),
        ('remote_chunks','catalog_committed_at'),
        ('remote_chunks','stored_tar_max_size_bytes'),
        ('archive_bundles','container_id'),
        ('archive_bundles','container_format'),
        ('archive_runs','remote_chunk_index'),
        ('archive_runs','tape_generation_id')
    ) AS req(table_name, column_name)
    WHERE NOT EXISTS (
        SELECT 1 FROM information_schema.columns c
        WHERE c.table_schema='public' AND c.table_name=req.table_name
          AND c.column_name=req.column_name);
    IF missing_columns IS NOT NULL THEN
        RAISE EXCEPTION 'migration 019 contract incomplete: missing column(s): %',
            array_to_string(missing_columns, ', ');
    END IF;

    SELECT array_agg(req) INTO missing_constraints
    FROM unnest(ARRAY[
        'remote_chunks_identity_format_uq', 'remote_chunks_packaging_format_ck',
        'remote_sessions_default_packaging_format_ck',
        'archive_bundles_container_fk', 'archive_bundles_container_format_ck',
        'archive_runs_remote_chunk_index_ck', 'archive_runs_tape_generation_fk',
        'archive_containers_chunk_format_fk', 'archive_containers_ordinal_uq',
        'archive_containers_identity_uq', 'archive_containers_id_format_uq',
        'archive_containers_dialect_ck', 'archive_containers_tape_locator_ck',
        'archive_containers_observed_ck', 'archive_containers_writer_ck',
        'archive_containers_catalog_ck', 'archive_containers_validated_part_ck',
        'archive_containers_ready_pair_ck',
        'archive_artifacts_chunk_fk', 'archive_artifacts_container_fk',
        'archive_artifacts_scope_ck', 'archive_artifacts_locator_distinct_ck',
        'archive_artifacts_ready_ck', 'archive_container_members_container_ck'
    ]) AS req
    WHERE NOT EXISTS (
        SELECT 1 FROM pg_constraint con
        JOIN pg_namespace n ON n.oid=con.connamespace
        WHERE n.nspname='public' AND con.conname=req);
    IF missing_constraints IS NOT NULL THEN
        RAISE EXCEPTION
            'migration 019 contract incomplete: missing constraint(s): %',
            array_to_string(missing_constraints, ', ');
    END IF;

    SELECT array_agg(req) INTO missing_indexes
    FROM unnest(ARRAY[
        'uq_archive_artifacts_container_kind_version',
        'uq_archive_artifacts_chunk_kind_version',
        'uq_archive_runs_remote_chunk_generation',
        'idx_archive_container_members_container']) AS req
    WHERE NOT EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE schemaname='public' AND indexname=req);
    IF missing_indexes IS NOT NULL THEN
        RAISE EXCEPTION 'migration 019 contract incomplete: missing index(es): %',
            array_to_string(missing_indexes, ', ');
    END IF;

    SELECT array_agg(req) INTO missing_triggers
    FROM unnest(ARRAY[
        'trg_remote_packaging_boundaries_immutable',
        'trg_remote_packaging_boundary_chunks_immutable',
        'trg_remote_chunks_packaging_write_once',
        'trg_archive_containers_format_write_once',
        'trg_archive_bundles_format_write_once',
        'trg_remote_sessions_packaging_default',
        'trg_remote_chunks_initial_format',
        'trg_archive_containers_tape_generation',
        'trg_archive_artifacts_container_format',
        'trg_archive_artifacts_identity_write_once',
        'trg_container_format_schema_metadata_immutable']) AS req
    WHERE NOT EXISTS (
        SELECT 1 FROM pg_trigger t
        JOIN pg_class rel ON rel.oid=t.tgrelid
        JOIN pg_namespace n ON n.oid=rel.relnamespace
        WHERE n.nspname='public' AND NOT t.tgisinternal AND t.tgname=req);
    IF missing_triggers IS NOT NULL THEN
        RAISE EXCEPTION 'migration 019 contract incomplete: missing trigger(s): %',
            array_to_string(missing_triggers, ', ');
    END IF;

    -- Every entry here is a trigger function (RETURNS trigger, implicitly
    -- zero declared args) except lto_assign_new_chunk_format, which is called
    -- directly from Python with five explicit arguments and is checked below
    -- with its real signature.
    SELECT array_agg(req) INTO missing_functions
    FROM unnest(ARRAY[
        'lto_reject_packaging_boundary_mutation',
        'lto_reject_packaging_format_change',
        'lto_reject_container_format_change',
        'lto_reject_container_identity_change',
        'lto_reject_artifact_identity_change',
        'lto_guard_session_packaging_default',
        'lto_guard_new_chunk_format', 'lto_guard_container_tape_generation',
        'lto_guard_artifact_container_format',
        'lto_guard_container_format_metadata',
        'lto_container_format_schema_fingerprint']) AS req
    WHERE to_regprocedure('public.' || req || '()') IS NULL;
    IF missing_functions IS NOT NULL THEN
        RAISE EXCEPTION 'migration 019 contract incomplete: missing function(s): %',
            array_to_string(missing_functions, ', ');
    END IF;
    IF to_regprocedure(
            'public.lto_assign_new_chunk_format'
            '(bigint,integer,boolean,integer,integer)') IS NULL THEN
        RAISE EXCEPTION 'migration 019 contract incomplete: missing '
            'function(s): lto_assign_new_chunk_format';
    END IF;
END $$;

-- ---------------------------------------------------------------------------
-- 2. The append-only v2 authority table.  Never a "singleton" row like v1 --
--    each authority_version gets its own permanent row, so a future v3 can
--    be added the same way without touching this one.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS container_format_schema_authority (
    authority_id        BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    authority_version    INTEGER NOT NULL CHECK (authority_version >= 2),
    schema_fingerprint   TEXT NOT NULL CHECK (length(schema_fingerprint)=32),
    migration_checksums  JSONB NOT NULL,
    contract_definition  JSONB NOT NULL,
    applied_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (authority_version)
);

-- ---------------------------------------------------------------------------
-- 3. The v2 object enumeration and fingerprint.  EXPLICIT (table, column)
--    pairs only -- no `c.relname IN (...)` without a column filter anywhere
--    in this query, for any table.  Excludes ITS OWN definition and every
--    new v2 bookkeeping object (the guard trigger function and this helper
--    itself) so it can never repeat 015's self-referential trap.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lto_container_format_schema_v2_object_defs()
RETURNS TABLE(object_key TEXT, object_definition TEXT)
LANGUAGE SQL STABLE AS $$
    SELECT 'table:' || c.relname, c.relkind::text
    FROM pg_class c
    JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE n.nspname='public' AND c.relname IN (
        'container_format_schema_metadata', 'remote_packaging_boundaries',
        'remote_packaging_boundary_chunks', 'archive_containers',
        'archive_artifacts', 'archive_container_members')
    UNION ALL
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
        (c.relname='container_format_schema_metadata' AND a.attname IN
            ('singleton','schema_version','required_reader_contract_version',
             'stored_tar_dialect','migration_checksum','schema_fingerprint',
             'applied_at'))
        OR (c.relname='remote_packaging_boundaries' AND a.attname IN
            ('session_id','first_stored_tar_chunk_index',
             'last_existing_chunk_index','approval_id','approval_reason',
             'database_evidence','local_staging_evidence',
             'evidence_checked_at','created_at'))
        OR (c.relname='remote_packaging_boundary_chunks' AND a.attname IN
            ('session_id','chunk_index','classification','assigned_format',
             'prefix_evidence_basis','plan_member_count','plan_logical_bytes',
             'first_ordinal','last_ordinal','evidence'))
        OR (c.relname='archive_containers' AND a.attname IN
            ('container_id','session_id','chunk_index','container_ordinal',
             'container_format','format_version','tar_dialect',
             'storage_class','container_name','temporary_data_locator',
             'permanent_local_metadata_locator','tape_label','tape_path',
             'tape_generation_id','expected_member_count',
             'expected_logical_bytes','observed_member_count',
             'observed_logical_bytes','actual_artifact_bytes',
             'validation_state','writer_state','catalog_state','owner_token',
             'lease_expires_at','validation_started_at','validated_at',
             'writer_started_at','writer_completed_at','catalog_committed_at',
             'created_at','updated_at','estimated_archive_bytes',
             'validated_part_locator','validation_summary',
             'disposition_counts'))
        OR (c.relname='archive_artifacts' AND a.attname IN
            ('artifact_id','session_id','chunk_index','container_id',
             'artifact_kind','artifact_version','local_locator',
             'tape_locator','artifact_size_bytes','readiness_state',
             'publication_started_at','published_at','tape_published_at',
             'created_at','updated_at'))
        OR (c.relname='archive_container_members' AND a.attname IN
            ('session_id','chunk_index','plan_file_id','plan_ordinal',
             'storage_class','container_id','container_ordinal',
             'remote_path','expected_logical_bytes','estimated_tar_bytes',
             'created_at'))
        OR (c.relname='remote_sessions' AND a.attname='default_packaging_format')
        OR (c.relname='remote_chunks' AND a.attname IN
            ('packaging_format','packaging_assigned_at','writer_started_at',
             'writer_completed_at','catalog_committed_at',
             'stored_tar_max_size_bytes'))
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
    WHERE n.nspname='public' AND con.conname IN
        ('remote_chunks_identity_format_uq', 'remote_chunks_packaging_format_ck',
         'remote_sessions_default_packaging_format_ck',
         'archive_bundles_container_fk', 'archive_bundles_container_format_ck',
         'archive_runs_remote_chunk_index_ck', 'archive_runs_tape_generation_fk',
         'directory_archive_bundles_container_fk',
         'directory_archive_bundles_generation_fk',
         'directory_archive_bundles_container_format_ck',
         'directory_archive_bundles_artifact_bytes_ck',
         'archive_containers_chunk_format_fk', 'archive_containers_ordinal_uq',
         'archive_containers_identity_uq', 'archive_containers_id_format_uq',
         'archive_containers_dialect_ck', 'archive_containers_tape_locator_ck',
         'archive_containers_observed_ck', 'archive_containers_writer_ck',
         'archive_containers_catalog_ck', 'archive_containers_validated_part_ck',
         'archive_containers_ready_pair_ck',
         'archive_artifacts_chunk_fk', 'archive_artifacts_container_fk',
         'archive_artifacts_scope_ck', 'archive_artifacts_locator_distinct_ck',
         'archive_artifacts_ready_ck', 'archive_container_members_container_ck')
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
         'uq_archive_runs_remote_chunk_generation',
         'idx_archive_container_members_container')
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
    -- Deliberately excludes lto_container_format_schema_v2_object_defs,
    -- lto_container_format_schema_fingerprint_v2 and
    -- lto_guard_container_format_schema_authority_v2 (this migration's own
    -- new objects).  lto_container_format_schema_fingerprint (v1's, a
    -- DIFFERENT function) is included: checking a third function's
    -- definition is not self-reference, and proves v1's own mechanism is
    -- also still intact.
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
         'lto_guard_session_packaging_default', 'lto_assign_new_chunk_format',
         'lto_guard_new_chunk_format', 'lto_guard_container_tape_generation',
         'lto_guard_artifact_container_format',
         'lto_guard_container_format_metadata',
         'lto_container_format_schema_fingerprint')
$$;

CREATE OR REPLACE FUNCTION lto_container_format_schema_fingerprint_v2()
RETURNS TEXT LANGUAGE SQL STABLE AS $$
    SELECT md5(COALESCE(string_agg(
        object_key || '=' || object_definition, E'\n' ORDER BY object_key), ''))
    FROM lto_container_format_schema_v2_object_defs()
$$;

-- ---------------------------------------------------------------------------
-- 4. Immutability: v2 authority rows can never be updated or deleted, same
--    guarantee as v1's row, so no later process can quietly "fix" a v2
--    fingerprint either.  A future v3's remediation for drift under v2 must
--    be another additive migration, the same way this one is for v1.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION lto_guard_container_format_schema_authority_v2()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'container format schema authority v2 rows are immutable';
END $$;

DROP TRIGGER IF EXISTS trg_container_format_schema_authority_v2_immutable
    ON container_format_schema_authority;
CREATE TRIGGER trg_container_format_schema_authority_v2_immutable
BEFORE UPDATE OR DELETE ON container_format_schema_authority
FOR EACH ROW EXECUTE FUNCTION lto_guard_container_format_schema_authority_v2();

-- ---------------------------------------------------------------------------
-- 5. The one row.  Explicit-only: requires the three migration checksums as
--    GUCs, set by the guarded Python apply path before this file runs (same
--    idiom as migrations 015 and 018).  Idempotent via ON CONFLICT DO
--    NOTHING keyed on authority_version.
-- ---------------------------------------------------------------------------
DO $$
DECLARE
    checksum_015 TEXT := nullif(current_setting(
        'lto.container_format_authority_v2_checksum_015', true), '');
    checksum_016 TEXT := nullif(current_setting(
        'lto.container_format_authority_v2_checksum_016', true), '');
    checksum_017 TEXT := nullif(current_setting(
        'lto.container_format_authority_v2_checksum_017', true), '');
BEGIN
    IF checksum_015 IS NULL OR checksum_015 !~ '^[0-9a-f]{64}$'
       OR checksum_016 IS NULL OR checksum_016 !~ '^[0-9a-f]{64}$'
       OR checksum_017 IS NULL OR checksum_017 !~ '^[0-9a-f]{64}$' THEN
        RAISE EXCEPTION
            'migration 019 must be applied through the explicit guarded command';
    END IF;

    INSERT INTO container_format_schema_authority
        (authority_version, schema_fingerprint, migration_checksums,
         contract_definition)
    SELECT 2, lto_container_format_schema_fingerprint_v2(),
           jsonb_build_object('015', checksum_015, '016', checksum_016,
                               '017', checksum_017),
           jsonb_build_object(
               'objects', (SELECT jsonb_agg(object_key ORDER BY object_key)
                           FROM lto_container_format_schema_v2_object_defs()),
               'object_count',
                   (SELECT count(*)
                    FROM lto_container_format_schema_v2_object_defs()))
    WHERE NOT EXISTS (
        SELECT 1 FROM container_format_schema_authority
        WHERE authority_version=2);
END $$;
