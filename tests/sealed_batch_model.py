"""Executable reference model for the Phase 5 sealed-batch design.

This is a TEST-ONLY artifact. It is imported by ``test_phase5a_sealed_batch_model``
and by nothing in ``src/``; it never touches the production PostgreSQL database,
runs no migration, and issues no tape or LTFS operation. It exists so the Phase 5
design can be *proved* — the lifecycle transitions, the immutability guarantees,
the fingerprint, the crash-recovery reconstruction and the "one chunk in one
active batch" constraint — before any of it is proposed for production.

It is backed by an in-memory SQLite database whose DDL mirrors the proposed
PostgreSQL schema closely enough to exercise the real constraints (primary keys,
the active-chunk uniqueness rule, CHECK-style guards enforced in the model, and
compare-and-set generation checks). SQLite is stdlib-only: no server, no
production data, nothing durable.

Chunks remain individually authoritative here exactly as in production: a batch
is a physical *scheduling and recovery* object layered on top of per-chunk truth,
never a replacement for it.
"""
import hashlib
import json
import sqlite3
import time
import uuid

# Batch lifecycle (§ design doc). Terminal states: durable, failed, cancelled.
BUILDING = "building"
SEALED = "sealed"
WRITING = "writing"
SYNC_PENDING = "sync_pending"
DURABLE = "durable"
FAILED = "failed"
CANCELLED = "cancelled"

ACTIVE_STATES = (BUILDING, SEALED, WRITING, SYNC_PENDING)
TERMINAL_STATES = (DURABLE, FAILED, CANCELLED)

# Per-chunk states mirror remote_chunks: a chunk enters 'backing' only when its
# own write begins, and stays authoritative regardless of batch state.
CHUNK_PENDING = "pending"
CHUNK_BACKING = "backing"
CHUNK_DONE = "done"
CHUNK_FAILED = "failed"

FORMAT_GENERATION = 1          # legacy-ZIP pack format; unchanged by Phase 5


class SealedBatchError(RuntimeError):
    pass


class ImmutableBatchError(SealedBatchError):
    pass


class GenerationConflict(SealedBatchError):
    pass


_SCHEMA = """
CREATE TABLE tape_write_batches (
    batch_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id          INTEGER NOT NULL,
    state               TEXT NOT NULL,
    generation          INTEGER NOT NULL DEFAULT 0,
    expected_tape_label TEXT NOT NULL,
    format_generation   INTEGER NOT NULL,
    chunk_count         INTEGER NOT NULL DEFAULT 0,
    prepared_bytes      INTEGER NOT NULL DEFAULT 0,
    file_count          INTEGER NOT NULL DEFAULT 0,
    batch_fingerprint   TEXT,
    writer_lease_id     TEXT,
    writer_lease_owner  TEXT,
    writer_lease_expires_at REAL,
    durability_kind     TEXT,
    created_at          REAL NOT NULL,
    sealed_at           REAL,
    write_started_at    REAL,
    copies_completed_at REAL,
    sync_requested_at   REAL,
    sync_confirmed_at   REAL,
    durable_at          REAL,
    failed_at           REAL,
    cancelled_at        REAL,
    CHECK (state IN ('building','sealed','writing','sync_pending',
                     'durable','failed','cancelled')),
    -- Fingerprint invariant: NULL while building; NOT NULL once sealed and for
    -- every post-seal active state; unconstrained for terminal failed/cancelled
    -- (a batch cancelled *from building* was never sealed and has no
    -- fingerprint, while one cancelled after sealing keeps its fingerprint).
    CHECK (
        (state = 'building' AND batch_fingerprint IS NULL)
        OR (state IN ('sealed','writing','sync_pending','durable')
            AND batch_fingerprint IS NOT NULL)
        OR (state IN ('failed','cancelled'))
    )
);

CREATE TABLE tape_write_batch_chunks (
    batch_id        INTEGER NOT NULL REFERENCES tape_write_batches(batch_id)
                        ON DELETE CASCADE,
    session_id      INTEGER NOT NULL,
    chunk_index     INTEGER NOT NULL,
    ordinal         INTEGER NOT NULL,
    pack_identity   TEXT NOT NULL,
    prepared_bytes  INTEGER NOT NULL,
    file_count      INTEGER NOT NULL,
    pack_fingerprint TEXT,
    chunk_state     TEXT NOT NULL DEFAULT 'pending',
    PRIMARY KEY (batch_id, chunk_index),
    UNIQUE (batch_id, ordinal),
    CHECK (chunk_state IN ('pending','backing','done','failed'))
);

-- One chunk may belong to at most ONE *active* batch. A dedicated claims table
-- makes that a plain PRIMARY-KEY invariant the database enforces, rather than a
-- cross-table trigger. The claim is inserted when a chunk joins a building batch
-- and removed only when the batch releases the chunk unwritten (fail/cancel) or
-- keeps it forever (durable).
CREATE TABLE tape_write_active_chunk (
    session_id  INTEGER NOT NULL,
    chunk_index INTEGER NOT NULL,
    batch_id    INTEGER NOT NULL REFERENCES tape_write_batches(batch_id),
    PRIMARY KEY (session_id, chunk_index)
);
"""


def compute_batch_fingerprint(expected_tape_label, format_generation, members):
    """Aggregate fingerprint over the ORDERED membership + tape identity.

    ``members`` is an iterable of dicts with ordinal/chunk_index/pack_identity/
    prepared_bytes/file_count/pack_fingerprint. Any change to membership,
    ordering, a pack identity, a byte count or a file count changes the hash.
    """
    canonical = {
        "expected_tape_label": expected_tape_label,
        "format_generation": format_generation,
        "members": [
            [m["ordinal"], m["chunk_index"], m["pack_identity"],
             m["prepared_bytes"], m["file_count"], m.get("pack_fingerprint")]
            for m in sorted(members, key=lambda m: m["ordinal"])
        ],
    }
    blob = json.dumps(canonical, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


class SealedBatchModel:
    def __init__(self, now=None):
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA foreign_keys = ON")
        self.db.executescript(_SCHEMA)
        self._clock = now or time.time

    # -- helpers ----------------------------------------------------------
    def _row(self, batch_id):
        r = self.db.execute(
            "SELECT * FROM tape_write_batches WHERE batch_id=?",
            (batch_id,)).fetchone()
        if r is None:
            raise SealedBatchError(f"no batch {batch_id}")
        return r

    def _members(self, batch_id):
        return [dict(r) for r in self.db.execute(
            "SELECT * FROM tape_write_batch_chunks WHERE batch_id=? "
            "ORDER BY ordinal", (batch_id,)).fetchall()]

    def state(self, batch_id):
        return self._row(batch_id)["state"]

    def generation(self, batch_id):
        return self._row(batch_id)["generation"]

    def chunk_state(self, batch_id, chunk_index):
        r = self.db.execute(
            "SELECT chunk_state FROM tape_write_batch_chunks "
            "WHERE batch_id=? AND chunk_index=?",
            (batch_id, chunk_index)).fetchone()
        return None if r is None else r["chunk_state"]

    # -- building ---------------------------------------------------------
    def begin_batch(self, session_id, expected_tape_label,
                    format_generation=FORMAT_GENERATION):
        cur = self.db.execute(
            "INSERT INTO tape_write_batches "
            "(session_id, state, expected_tape_label, format_generation, "
            " created_at) VALUES (?,?,?,?,?)",
            (session_id, BUILDING, expected_tape_label, format_generation,
             self._clock()))
        self.db.commit()
        return cur.lastrowid

    def add_chunk(self, batch_id, chunk_index, pack_identity, prepared_bytes,
                  file_count, pack_fingerprint=None):
        row = self._row(batch_id)
        if row["state"] != BUILDING:
            raise ImmutableBatchError(
                f"cannot add chunk to a {row['state']} batch")
        ordinal = row["chunk_count"]
        try:
            # The active-chunk PK rejects a chunk already held by another active
            # batch. Both statements are in ONE transaction: a rejected claim
            # rolls the membership row back too.
            self.db.execute(
                "INSERT INTO tape_write_active_chunk "
                "(session_id, chunk_index, batch_id) VALUES (?,?,?)",
                (row["session_id"], chunk_index, batch_id))
            self.db.execute(
                "INSERT INTO tape_write_batch_chunks "
                "(batch_id, session_id, chunk_index, ordinal, pack_identity, "
                " prepared_bytes, file_count, pack_fingerprint) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (batch_id, row["session_id"], chunk_index, ordinal,
                 pack_identity, prepared_bytes, file_count, pack_fingerprint))
        except sqlite3.IntegrityError as e:
            self.db.rollback()
            raise SealedBatchError(
                f"chunk {chunk_index} already belongs to an active batch: {e}")
        self.db.execute(
            "UPDATE tape_write_batches SET chunk_count=chunk_count+1, "
            "prepared_bytes=prepared_bytes+?, file_count=file_count+? "
            "WHERE batch_id=?",
            (prepared_bytes, file_count, batch_id))
        self.db.commit()
        return ordinal

    # -- seal (atomic) ----------------------------------------------------
    def seal(self, batch_id):
        row = self._row(batch_id)
        if row["state"] != BUILDING:
            raise ImmutableBatchError(f"cannot seal a {row['state']} batch")
        members = self._members(batch_id)
        if not members:
            raise SealedBatchError("refusing to seal an empty batch")
        fp = compute_batch_fingerprint(
            row["expected_tape_label"], row["format_generation"], members)
        # ATOMIC compare-and-set: the state flips building->sealed and the
        # fingerprint is written in a single guarded UPDATE. A concurrent sealer
        # (or a crash between the check and here) cannot double-seal.
        cur = self.db.execute(
            "UPDATE tape_write_batches SET state=?, batch_fingerprint=?, "
            "sealed_at=?, generation=generation+1 "
            "WHERE batch_id=? AND state=?",
            (SEALED, fp, self._clock(), batch_id, BUILDING))
        if cur.rowcount != 1:
            self.db.rollback()
            raise GenerationConflict("batch was not building at seal time")
        self.db.commit()
        return fp

    # -- claim for writing (CAS + lease) ----------------------------------
    def claim_for_write(self, batch_id, lease_owner, lease_ttl=300,
                        expected_generation=None):
        row = self._row(batch_id)
        if expected_generation is not None and \
                row["generation"] != expected_generation:
            raise GenerationConflict(
                f"generation {row['generation']} != {expected_generation}")
        lease = uuid.uuid4().hex
        now = self._clock()
        cur = self.db.execute(
            "UPDATE tape_write_batches SET state=?, writer_lease_id=?, "
            "writer_lease_owner=?, writer_lease_expires_at=?, "
            "write_started_at=COALESCE(write_started_at,?), "
            "generation=generation+1 "
            "WHERE batch_id=? AND state=?",
            (WRITING, lease, lease_owner, now + lease_ttl, now,
             batch_id, SEALED))
        if cur.rowcount != 1:
            self.db.rollback()
            raise GenerationConflict(
                f"batch {batch_id} was not sealed/claimable")
        self.db.commit()
        return lease

    # -- per-chunk authority ---------------------------------------------
    def record_chunk_write_start(self, batch_id, chunk_index):
        self._require_state(batch_id, WRITING, "write a chunk")
        self.db.execute(
            "UPDATE tape_write_batch_chunks SET chunk_state=? "
            "WHERE batch_id=? AND chunk_index=?",
            (CHUNK_BACKING, batch_id, chunk_index))
        self.db.commit()

    def commit_chunk_done(self, batch_id, chunk_index):
        self.db.execute(
            "UPDATE tape_write_batch_chunks SET chunk_state=? "
            "WHERE batch_id=? AND chunk_index=?",
            (CHUNK_DONE, batch_id, chunk_index))
        self.db.commit()

    def fail_chunk(self, batch_id, chunk_index, leave_backing=True):
        """Conservative classification: a chunk whose write started stays
        'backing' (physically ambiguous); one that never started is failed and
        remains re-fetchable."""
        new = CHUNK_BACKING if leave_backing else CHUNK_FAILED
        self.db.execute(
            "UPDATE tape_write_batch_chunks SET chunk_state=? "
            "WHERE batch_id=? AND chunk_index=?",
            (new, batch_id, chunk_index))
        self.db.commit()

    # -- sync / durability contract --------------------------------------
    def request_sync(self, batch_id):
        row = self._require_state(batch_id, WRITING, "request sync")
        undone = [m for m in self._members(batch_id)
                  if m["chunk_state"] != CHUNK_DONE]
        if undone:
            raise SealedBatchError(
                f"cannot request sync: {len(undone)} chunk(s) not done")
        self.db.execute(
            "UPDATE tape_write_batches SET state=?, copies_completed_at=?, "
            "sync_requested_at=? WHERE batch_id=?",
            (SYNC_PENDING, self._clock(), self._clock(), batch_id))
        self.db.commit()

    def confirm_sync(self, batch_id):
        """LTFS synchronization CONFIRMED. Phase 5A defines the contract only;
        no explicit LTFS sync is issued here."""
        self._require_state(batch_id, SYNC_PENDING, "confirm sync")
        self.db.execute(
            "UPDATE tape_write_batches SET sync_confirmed_at=? WHERE batch_id=?",
            (self._clock(), batch_id))
        self.db.commit()

    def mark_durable(self, batch_id, durability_kind="ltfs_time5_confirmed"):
        row = self._row(batch_id)
        if row["state"] == DURABLE:                       # idempotent
            return
        if row["state"] != SYNC_PENDING:
            raise SealedBatchError(
                f"cannot mark durable from {row['state']}")
        if row["sync_confirmed_at"] is None:
            raise SealedBatchError(
                "refusing durable: LTFS synchronization not confirmed")
        self.db.execute(
            "UPDATE tape_write_batches SET state=?, durable_at=?, "
            "durability_kind=? WHERE batch_id=? AND state=?",
            (DURABLE, self._clock(), durability_kind, batch_id, SYNC_PENDING))
        self.db.commit()

    def is_prune_safe(self, batch_id):
        """Pruning source data requires the authoritative durability condition.
        externally_durable is intentionally a SEPARATE future gate."""
        return self._row(batch_id)["state"] == DURABLE

    # -- failure / cancellation ------------------------------------------
    def fail_batch(self, batch_id, release_unwritten=True):
        self._terminate(batch_id, FAILED, "failed_at", release_unwritten)

    def cancel_batch(self, batch_id, release_unwritten=True):
        self._terminate(batch_id, CANCELLED, "cancelled_at", release_unwritten)

    def _terminate(self, batch_id, state, ts_col, release_unwritten):
        row = self._row(batch_id)
        # Release only chunks that never became durable (done). A 'done' chunk is
        # physically on tape and keeps its claim; unwritten chunks are freed so
        # their reusable packs can join a future batch.
        if release_unwritten:
            self.db.execute(
                "DELETE FROM tape_write_active_chunk WHERE batch_id=? AND "
                "chunk_index IN (SELECT chunk_index FROM "
                "tape_write_batch_chunks WHERE batch_id=? AND chunk_state!=?)",
                (batch_id, batch_id, CHUNK_DONE))
        self.db.execute(
            f"UPDATE tape_write_batches SET state=?, {ts_col}=? WHERE batch_id=?",
            (state, self._clock(), batch_id))
        self.db.commit()

    # -- crash recovery ---------------------------------------------------
    def discard_unsealed(self, session_id):
        """A crash while BUILDING leaves no authoritative batch: the design
        discards building batches on recovery (their packs stay on disk and are
        re-enqueued)."""
        ids = [r["batch_id"] for r in self.db.execute(
            "SELECT batch_id FROM tape_write_batches WHERE session_id=? "
            "AND state=?", (session_id, BUILDING)).fetchall()]
        for bid in ids:
            self.db.execute(
                "DELETE FROM tape_write_active_chunk WHERE batch_id=?", (bid,))
            self.db.execute(
                "DELETE FROM tape_write_batch_chunks WHERE batch_id=?", (bid,))
            self.db.execute(
                "DELETE FROM tape_write_batches WHERE batch_id=?", (bid,))
        self.db.commit()
        return ids

    def reconstruct(self, batch_id):
        """Rebuild the exact ordered group from durable rows and re-verify the
        fingerprint. Returns (members, fingerprint_ok)."""
        row = self._row(batch_id)
        if row["state"] == BUILDING:
            raise SealedBatchError("a building batch is not authoritative")
        members = self._members(batch_id)
        recomputed = compute_batch_fingerprint(
            row["expected_tape_label"], row["format_generation"], members)
        return members, (recomputed == row["batch_fingerprint"])

    def recover_abandoned_writer(self, now=None):
        """A writer that died holding the mutex leaves a 'writing' batch with an
        expired lease. Recovery returns it to 'sealed' (re-claimable) with a
        generation bump, so a stale claim token can never win a later CAS."""
        now = now if now is not None else self._clock()
        recovered = []
        for r in self.db.execute(
                "SELECT batch_id FROM tape_write_batches WHERE state=? AND "
                "writer_lease_expires_at IS NOT NULL AND "
                "writer_lease_expires_at < ?", (WRITING, now)).fetchall():
            self.db.execute(
                "UPDATE tape_write_batches SET state=?, writer_lease_id=NULL, "
                "writer_lease_owner=NULL, writer_lease_expires_at=NULL, "
                "generation=generation+1 WHERE batch_id=? AND state=?",
                (SEALED, r["batch_id"], WRITING))
            recovered.append(r["batch_id"])
        self.db.commit()
        return recovered

    def _require_state(self, batch_id, expected, action):
        row = self._row(batch_id)
        if row["state"] != expected:
            raise SealedBatchError(
                f"cannot {action}: batch is {row['state']}, not {expected}")
        return row
