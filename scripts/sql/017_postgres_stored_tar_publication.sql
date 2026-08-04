-- =============================================================================
-- 017 - Stored TAR build ownership, validated parts, and paired publication.
-- =============================================================================

DO $$
BEGIN
    IF to_regclass('public.archive_containers') IS NULL
       OR to_regclass('public.archive_artifacts') IS NULL
       OR to_regclass('public.archive_container_members') IS NULL THEN
        RAISE EXCEPTION 'migration 017 requires migrations 015 and 016';
    END IF;
END $$;

ALTER TABLE archive_containers
    ADD COLUMN IF NOT EXISTS validated_part_locator TEXT,
    ADD COLUMN IF NOT EXISTS validation_summary JSONB,
    ADD COLUMN IF NOT EXISTS disposition_counts JSONB;

ALTER TABLE archive_containers
    DROP CONSTRAINT IF EXISTS archive_containers_observed_ck;

ALTER TABLE archive_containers
    ADD CONSTRAINT archive_containers_observed_ck CHECK (
        validation_state NOT IN ('validated_part','ready')
        OR (observed_member_count IS NOT NULL
            AND observed_logical_bytes IS NOT NULL
            AND actual_artifact_bytes IS NOT NULL
            AND ((container_format='zip'
                  AND observed_member_count=expected_member_count
                  AND observed_logical_bytes=expected_logical_bytes)
                 OR (container_format='stored_tar'
                     AND observed_member_count <= expected_member_count
                     AND observed_logical_bytes <= expected_logical_bytes))));

ALTER TABLE archive_containers
    DROP CONSTRAINT IF EXISTS archive_containers_validated_part_ck;

ALTER TABLE archive_containers
    ADD CONSTRAINT archive_containers_validated_part_ck CHECK (
        container_format <> 'stored_tar'
        OR validation_state NOT IN ('validated_part','ready')
        OR (validated_part_locator IS NOT NULL
            AND lower(right(validated_part_locator, 5)) = '.part'
            AND validation_summary IS NOT NULL));

ALTER TABLE archive_containers
    DROP CONSTRAINT IF EXISTS archive_containers_ready_pair_ck;

ALTER TABLE archive_containers
    ADD CONSTRAINT archive_containers_ready_pair_ck CHECK (
        container_format <> 'stored_tar'
        OR validation_state <> 'ready'
        OR (temporary_data_locator IS NOT NULL
            AND lower(right(temporary_data_locator, 5)) <> '.part'
            AND permanent_local_metadata_locator IS NOT NULL
            AND lower(right(permanent_local_metadata_locator, 5)) <> '.part'
            AND disposition_counts IS NOT NULL));
