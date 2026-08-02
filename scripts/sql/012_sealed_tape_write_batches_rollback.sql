-- =============================================================================
-- 012 ROLLBACK — remove the sealed tape-write batch objects (Phase 5B).
-- =============================================================================
-- Safe to run ONLY against an isolated throwaway database. Never run against
-- production. The sealed-batch tables are additive and are never the source of
-- per-chunk truth, so dropping them leaves remote_chunks / remote_sessions /
-- tapes and all catalog durability untouched (see PHASE5_SEALED_BATCH_DESIGN.md
-- §16 Rollback strategy).
--
-- schema_migrations is intentionally NOT dropped (other migrations may record
-- there); only this feature's version row is removed.
-- =============================================================================
BEGIN;

DROP TABLE IF EXISTS tape_write_active_chunk;
DROP TABLE IF EXISTS tape_write_batch_chunks;
DROP TABLE IF EXISTS tape_write_batches;

DELETE FROM schema_migrations WHERE version = '012_sealed_tape_write_batches';

COMMIT;
