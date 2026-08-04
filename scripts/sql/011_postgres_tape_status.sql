-- 011: retire a cartridge from writing without lying about its numbers.
--
-- `used_space` is recomputed from the catalog on every mount
-- (recalculate_tape_used_space), so "mark a tape full" cannot be expressed by
-- editing the byte counters — the next run would silently undo it. A tape can
-- also be unwritable while far from full: Tape_02 latched its PWE bit after an
-- unrecoverable write and came back read-only with ~5 TB of 12 TB used
-- (docs/incidents/010-20260724-ltfs-write-perm-readonly.md).
--
-- status='full' means "no further writes are planned for this cartridge",
-- whatever the reason; status_reason records the why for the operator.
-- Existing rows default to 'active', so applying this migration changes no
-- behaviour until a tape is explicitly marked.

ALTER TABLE tapes
    ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'active';
ALTER TABLE tapes
    ADD COLUMN IF NOT EXISTS status_reason TEXT;
ALTER TABLE tapes
    ADD COLUMN IF NOT EXISTS status_changed_at TIMESTAMPTZ;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'tapes_status_check'
    ) THEN
        ALTER TABLE tapes
            ADD CONSTRAINT tapes_status_check
            CHECK (status IN ('active', 'full'));
    END IF;
END
$$;
