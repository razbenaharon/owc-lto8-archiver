"""Repository layer for Phase 5 sealed tape-write batches.

**NOT wired into tape scheduling.** Phase 5B.5 wires only the disabled feature
GATE into startup (``assert_feature_ready``); the repository is never connected
to the ReadyQueue, the chunk writer, or Session 37. It is exercised against an
isolated throwaway / shadow PostgreSQL database behind the fail-closed flag
``[FEATURES] sealed_tape_write_batches_enabled`` (default false).

Authority model (Phase 5B.5, Option B):

* ``remote_chunks.status`` is the SOLE authority for the per-chunk archive
  result. The sealed-batch tables are authoritative for membership, ordering,
  the writer claim, synchronization, and batch recovery — never for whether a
  chunk is durably archived.
* ``tape_write_batch_chunks.member_write_phase`` is BATCH-EXECUTION bookkeeping
  with a deliberately distinct vocabulary (``not_started/writing/copied/failed``)
  so it cannot be mistaken for ``remote_chunks.status``. ``reconcile_*`` fails
  closed on impossible combinations and never downgrades a chunk that
  ``remote_chunks`` says is done.

Migration integrity (Phase 5B.5): ``apply_schema`` records a SHA-256 of the 012
file, fails closed if a different checksum is already recorded for the version,
and runs exact schema-drift validation — "the tables already exist" is never by
itself treated as success. Authority: ``docs/PHASE5_SEALED_BATCH_DESIGN.md``.
"""
import hashlib
import json
from pathlib import Path

from .constants import PROJECT_ROOT

FORMAT_GENERATION = 1          # legacy-ZIP pack format; unchanged by Phase 5
MIGRATION_VERSION = "012_sealed_tape_write_batches"
_MIGRATION_FILE = (Path(PROJECT_ROOT) / "scripts" / "sql"
                   / "012_sealed_tape_write_batches.sql")
_ROLLBACK_FILE = (Path(PROJECT_ROOT) / "scripts" / "sql"
                  / "012_sealed_tape_write_batches_rollback.sql")

# Batch lifecycle.
BUILDING = "building"
SEALED = "sealed"
WRITING = "writing"
SYNC_PENDING = "sync_pending"
DURABLE = "durable"
FAILED = "failed"
CANCELLED = "cancelled"
ACTIVE_STATES = (BUILDING, SEALED, WRITING, SYNC_PENDING)

# Batch-execution phase for a member (NOT the archive result — that is
# remote_chunks.status). Distinct vocabulary on purpose.
PHASE_NOT_STARTED = "not_started"
PHASE_WRITING = "writing"
PHASE_COPIED = "copied"
PHASE_FAILED = "failed"

# reconcile_member outcomes.
RC_OK = "ok"                       # member and remote authority agree
RC_AUTHORITY_DONE = "authority_done"  # remote says done; promote member to copied
RC_RELEASE = "release"             # unstarted member already done elsewhere; release
RC_AMBIGUOUS = "ambiguous"         # writing + backing: physically ambiguous


class SealedBatchError(RuntimeError):
    pass


class ImmutableBatchError(SealedBatchError):
    pass


class GenerationConflict(SealedBatchError):
    pass


class SchemaDriftError(SealedBatchError):
    pass


class ReconcileError(SealedBatchError):
    pass


class RollbackRefused(SealedBatchError):
    pass


def migration_checksum():
    """SHA-256 of the 012 migration file — its immutable identity."""
    return hashlib.sha256(_MIGRATION_FILE.read_bytes()).hexdigest()


def compute_batch_fingerprint(expected_tape_label, format_generation, members):
    """SHA-256 over ORDERED membership + tape identity + format generation."""
    canonical = {
        "expected_tape_label": expected_tape_label,
        "format_generation": format_generation,
        "members": [
            [m["ordinal"], int(m["chunk_index"]), m["pack_identity"],
             int(m["prepared_bytes"]), int(m["file_count"]),
             m.get("pack_fingerprint")]
            for m in sorted(members, key=lambda m: m["ordinal"])
        ],
    }
    blob = json.dumps(canonical, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


# --- Exact expected schema (Phase 5B.5 drift validation) --------------------
# data_type is exactly what information_schema.columns reports on PostgreSQL.
_EXPECTED_COLUMNS = {
    "tape_write_batches": {
        "batch_id": ("bigint", False),
        "session_id": ("bigint", False),
        "state": ("text", False),
        "generation": ("bigint", False),
        "expected_tape_label": ("text", False),
        "format_generation": ("integer", False),
        "chunk_count": ("integer", False),
        "prepared_bytes": ("bigint", False),
        "file_count": ("bigint", False),
        "batch_fingerprint": ("text", True),
        "writer_lease_id": ("text", True),
        "writer_lease_owner": ("text", True),
        "writer_lease_expires_at": ("timestamp with time zone", True),
        "durability_kind": ("text", True),
        "created_at": ("timestamp with time zone", False),
        "sealed_at": ("timestamp with time zone", True),
        "write_started_at": ("timestamp with time zone", True),
        "copies_completed_at": ("timestamp with time zone", True),
        "sync_requested_at": ("timestamp with time zone", True),
        "sync_confirmed_at": ("timestamp with time zone", True),
        "durable_at": ("timestamp with time zone", True),
        "failed_at": ("timestamp with time zone", True),
        "cancelled_at": ("timestamp with time zone", True),
    },
    "tape_write_batch_chunks": {
        "batch_id": ("bigint", False),
        "session_id": ("bigint", False),
        "chunk_index": ("integer", False),
        "ordinal": ("integer", False),
        "pack_identity": ("text", False),
        "prepared_bytes": ("bigint", False),
        "file_count": ("bigint", False),
        "pack_fingerprint": ("text", True),
        "member_write_phase": ("text", False),
    },
    "tape_write_active_chunk": {
        "session_id": ("bigint", False),
        "chunk_index": ("integer", False),
        "batch_id": ("bigint", False),
    },
}
# Named CHECK/constraint identities that must be present.
_EXPECTED_NAMED_CONSTRAINTS = {
    "tape_write_batches_state_ck", "tape_write_batches_fp_ck",
    "tape_write_batches_durable_ck", "twbc_member_phase_ck",
    "twbc_chunk_fk", "twac_chunk_fk",
}
_EXPECTED_INDEXES = {"ix_twb_session_state", "ix_twb_active_lease"}
# Expected primary-key column sets (order-insensitive).
_EXPECTED_PK = {
    "tape_write_batches": {"batch_id"},
    "tape_write_batch_chunks": {"batch_id", "chunk_index"},
    "tape_write_active_chunk": {"session_id", "chunk_index"},
}


def reconcile_member(member_phase, remote_status):
    """Pure per-member reconciliation of batch-execution phase against the
    authoritative ``remote_chunks.status``. Returns an ``RC_*`` outcome and
    raises :class:`ReconcileError` (fail closed) on an impossible combination.
    Idempotent: it never mutates, so callers can run it repeatedly.

    The authority always wins: a chunk that ``remote_chunks`` reports ``done``
    is treated as done regardless of the member phase; member state can never
    downgrade it."""
    if remote_status is None:
        raise ReconcileError(
            "member references a chunk with no remote_chunks row")
    if member_phase == PHASE_COPIED:
        if remote_status == "done":
            return RC_OK
        raise ReconcileError(
            f"member 'copied' but remote status {remote_status!r} is not done")
    if member_phase == PHASE_WRITING:
        if remote_status == "backing":
            return RC_AMBIGUOUS
        if remote_status == "done":
            return RC_AUTHORITY_DONE
        raise ReconcileError(
            f"member 'writing' but remote status {remote_status!r} "
            "(never entered backing)")
    if member_phase == PHASE_FAILED:
        if remote_status in ("backup_failed", "fetch_failed", "backing"):
            return RC_OK
        if remote_status == "done":
            return RC_AUTHORITY_DONE
        raise ReconcileError(
            f"member 'failed' but remote status {remote_status!r}")
    if member_phase == PHASE_NOT_STARTED:
        if remote_status in ("pending", "fetching", "packing"):
            return RC_OK
        if remote_status == "done":
            return RC_RELEASE
        raise ReconcileError(
            f"member 'not_started' but remote status {remote_status!r}")
    raise ReconcileError(f"unknown member phase {member_phase!r}")


def assert_feature_ready(cfg, repo):
    """Fail-closed feature gate for the startup path.

    Returns False and touches NO batch table when the flag is disabled. When
    enabled it requires: the 012 schema applied, the recorded checksum valid, and
    exact schema validation to pass — raising otherwise so a mis-enabled or
    drifted feature fails at startup rather than mid-run."""
    if not cfg.sealed_tape_write_batches_enabled:
        return False
    if not repo.schema_applied():
        raise RuntimeError(
            "[FEATURES] sealed_tape_write_batches_enabled=true but the sealed-"
            "batch schema (scripts/sql/012) is not applied to this database.")
    repo.assert_schema_valid()          # checksum + exact drift validation
    return True


class SealedBatchRepository:
    """PostgreSQL repository for sealed tape-write batches."""

    def __init__(self, conninfo):
        self.conninfo = conninfo

    # -- connection / transaction helpers --------------------------------
    def _connect(self):
        import psycopg
        return psycopg.connect(self.conninfo)

    def _tx(self, fn):
        conn = self._connect()
        try:
            with conn.transaction():
                return fn(conn)
        finally:
            conn.close()

    def _read(self, fn):
        conn = self._connect()
        try:
            return fn(conn)
        finally:
            conn.close()

    # -- schema apply / checksum / drift ---------------------------------
    def apply_schema(self):
        """Apply 012 idempotently, then record/verify its checksum and run exact
        schema-drift validation. Never reports success just because the tables
        exist; a recorded-but-different checksum fails closed."""
        import psycopg
        ddl = _MIGRATION_FILE.read_text(encoding="utf-8")
        checksum = migration_checksum()
        conn = self._connect()
        try:
            pre_exists = conn.execute(
                "SELECT to_regclass('public.tape_write_batches') IS NOT NULL"
            ).fetchone()[0]
            if pre_exists:
                # An incompatible pre-existing table must be REJECTED, not
                # silently extended (running the DDL would otherwise fail deep in
                # a CREATE INDEX with a confusing UndefinedColumn). Validate the
                # existing objects first; a clean idempotent re-apply passes here
                # and simply re-records/verifies the checksum below.
                self._verify_schema_conn(conn)
                conn.rollback()            # close the read-only transaction
            else:
                conn.execute(ddl)
                conn.commit()
            # Checksum guard (own transaction).
            with conn.transaction():
                row = conn.execute(
                    "SELECT checksum FROM schema_migrations WHERE version=%s "
                    "FOR UPDATE", (MIGRATION_VERSION,)).fetchone()
                if row is None:
                    conn.execute(
                        "INSERT INTO schema_migrations (version, checksum) "
                        "VALUES (%s, %s)", (MIGRATION_VERSION, checksum))
                elif row[0] is None:
                    conn.execute(
                        "UPDATE schema_migrations SET checksum=%s "
                        "WHERE version=%s", (checksum, MIGRATION_VERSION))
                elif row[0] != checksum:
                    raise SchemaDriftError(
                        f"migration {MIGRATION_VERSION} recorded checksum "
                        f"{row[0]!r} != current file checksum {checksum!r}; "
                        "refusing to proceed (the migration definition changed "
                        "after it was applied).")
        finally:
            conn.close()
        self.assert_schema_valid()
        return checksum

    def assert_schema_valid(self):
        """Checksum match + exact schema-drift validation. Raises on any
        mismatch, missing/incompatible object, or unrecorded checksum."""
        def _q(conn):
            row = conn.execute(
                "SELECT checksum FROM schema_migrations WHERE version=%s",
                (MIGRATION_VERSION,)).fetchone()
            if row is None:
                raise SchemaDriftError(
                    f"migration {MIGRATION_VERSION} is not recorded")
            if row[0] is None or row[0] != migration_checksum():
                raise SchemaDriftError(
                    f"migration {MIGRATION_VERSION} checksum mismatch "
                    f"(recorded {row[0]!r} != {migration_checksum()!r})")
            self._verify_schema_conn(conn)
        return self._read(_q)

    def _verify_schema_conn(self, conn):
        for table, cols in _EXPECTED_COLUMNS.items():
            actual = {
                r[0]: (r[1], r[2] == "YES")
                for r in conn.execute(
                    "SELECT column_name, data_type, is_nullable "
                    "FROM information_schema.columns "
                    "WHERE table_schema='public' AND table_name=%s",
                    (table,)).fetchall()}
            if not actual:
                raise SchemaDriftError(f"expected table {table!r} is missing")
            for col, (dtype, nullable) in cols.items():
                if col not in actual:
                    raise SchemaDriftError(
                        f"{table}.{col} is missing (schema drift)")
                got_type, got_nullable = actual[col]
                if got_type != dtype:
                    raise SchemaDriftError(
                        f"{table}.{col} type {got_type!r} != expected {dtype!r}")
                if got_nullable != nullable:
                    raise SchemaDriftError(
                        f"{table}.{col} nullability {got_nullable} != expected "
                        f"{nullable}")
        # Named constraints (state/fp/durable/member-phase CHECKs + FKs).
        present = {r[0] for r in conn.execute(
            "SELECT conname FROM pg_constraint WHERE connamespace = "
            "'public'::regnamespace").fetchall()}
        missing = _EXPECTED_NAMED_CONSTRAINTS - present
        if missing:
            raise SchemaDriftError(f"missing constraints: {sorted(missing)}")
        # Indexes.
        idx = {r[0] for r in conn.execute(
            "SELECT indexname FROM pg_indexes WHERE schemaname='public'"
        ).fetchall()}
        missing_idx = _EXPECTED_INDEXES - idx
        if missing_idx:
            raise SchemaDriftError(f"missing indexes: {sorted(missing_idx)}")
        # Primary-key column sets.
        for table, expected_pk in _EXPECTED_PK.items():
            pk = {r[0] for r in conn.execute(
                "SELECT a.attname FROM pg_constraint c "
                "JOIN pg_attribute a ON a.attrelid=c.conrelid "
                "AND a.attnum = ANY(c.conkey) "
                "WHERE c.conrelid = %s::regclass AND c.contype='p'",
                (table,)).fetchall()}
            if pk != expected_pk:
                raise SchemaDriftError(
                    f"{table} primary key {sorted(pk)} != {sorted(expected_pk)}")

    def rollback_schema(self, force=False):
        """Remove the sealed-batch objects. REFUSES on real batch history unless
        ``force=True`` (test/operator-confirmed only).

        Never touches remote_chunks / remote_sessions / tapes / catalog data."""
        def _guard(conn):
            if not force:
                n = conn.execute(
                    "SELECT COUNT(*) FROM tape_write_batches").fetchone()[0]
                claims = conn.execute(
                    "SELECT COUNT(*) FROM tape_write_active_chunk").fetchone()[0]
                durable = conn.execute(
                    "SELECT COUNT(*) FROM tape_write_batches WHERE state="
                    "'durable'").fetchone()[0]
                if n or claims or durable:
                    raise RollbackRefused(
                        f"refusing to roll back: {n} batch(es), {claims} active "
                        f"claim(s), {durable} durable — real batch history would "
                        "be destroyed. Pass force=True to override (test / "
                        "explicit operator confirmation only).")
        # Guard runs only when the tables exist.
        if self.schema_applied():
            self._read(_guard)
        sql = _ROLLBACK_FILE.read_text(encoding="utf-8")
        conn = self._connect()
        try:
            conn.execute(sql)
            conn.commit()
        finally:
            conn.close()

    def schema_applied(self):
        def _q(conn):
            row = conn.execute(
                "SELECT to_regclass('public.tape_write_batches') IS NOT NULL"
            ).fetchone()
            return bool(row[0])
        return self._read(_q)

    # -- building --------------------------------------------------------
    def create_building_batch(self, session_id, expected_tape_label,
                              format_generation=FORMAT_GENERATION):
        def _op(conn):
            return conn.execute(
                "INSERT INTO tape_write_batches "
                "(session_id, state, expected_tape_label, format_generation) "
                "VALUES (%s, 'building', %s, %s) RETURNING batch_id",
                (session_id, expected_tape_label, format_generation)
            ).fetchone()[0]
        return self._tx(_op)

    def add_chunk(self, batch_id, chunk_index, pack_identity, prepared_bytes,
                  file_count, pack_fingerprint=None):
        import psycopg

        def _op(conn):
            row = conn.execute(
                "SELECT session_id, state, chunk_count FROM tape_write_batches "
                "WHERE batch_id=%s FOR UPDATE", (batch_id,)).fetchone()
            if row is None:
                raise SealedBatchError(f"no batch {batch_id}")
            session_id, state, chunk_count = row
            if state != BUILDING:
                raise ImmutableBatchError(
                    f"cannot add a chunk to a {state} batch")
            ordinal = chunk_count
            try:
                conn.execute(
                    "INSERT INTO tape_write_active_chunk "
                    "(session_id, chunk_index, batch_id) VALUES (%s,%s,%s)",
                    (session_id, chunk_index, batch_id))
                conn.execute(
                    "INSERT INTO tape_write_batch_chunks "
                    "(batch_id, session_id, chunk_index, ordinal, pack_identity,"
                    " prepared_bytes, file_count, pack_fingerprint) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                    (batch_id, session_id, chunk_index, ordinal, pack_identity,
                     prepared_bytes, file_count, pack_fingerprint))
            except psycopg.errors.UniqueViolation as e:
                raise SealedBatchError(
                    f"chunk {chunk_index} already belongs to an active batch") \
                    from e
            conn.execute(
                "UPDATE tape_write_batches SET chunk_count=chunk_count+1, "
                "prepared_bytes=prepared_bytes+%s, file_count=file_count+%s "
                "WHERE batch_id=%s", (prepared_bytes, file_count, batch_id))
            return ordinal
        return self._tx(_op)

    # -- seal (atomic CAS) -----------------------------------------------
    def seal_batch(self, batch_id):
        def _op(conn):
            row = conn.execute(
                "SELECT state, expected_tape_label, format_generation "
                "FROM tape_write_batches WHERE batch_id=%s FOR UPDATE",
                (batch_id,)).fetchone()
            if row is None:
                raise SealedBatchError(f"no batch {batch_id}")
            state, tape, fmt = row
            if state != BUILDING:
                raise ImmutableBatchError(f"cannot seal a {state} batch")
            members = self._members_conn(conn, batch_id)
            if not members:
                raise SealedBatchError("refusing to seal an empty batch")
            fp = compute_batch_fingerprint(tape, fmt, members)
            cur = conn.execute(
                "UPDATE tape_write_batches SET state='sealed', "
                "batch_fingerprint=%s, sealed_at=now(), generation=generation+1 "
                "WHERE batch_id=%s AND state='building'", (fp, batch_id))
            if cur.rowcount != 1:
                raise GenerationConflict("batch was not building at seal time")
            return fp
        return self._tx(_op)

    # -- claim for write (CAS + lease) -----------------------------------
    def claim_for_write(self, batch_id, lease_owner, lease_ttl_seconds=300,
                        expected_generation=None):
        def _op(conn):
            row = conn.execute(
                "SELECT state, generation FROM tape_write_batches "
                "WHERE batch_id=%s FOR UPDATE", (batch_id,)).fetchone()
            if row is None:
                raise SealedBatchError(f"no batch {batch_id}")
            state, generation = row
            if expected_generation is not None and \
                    generation != expected_generation:
                raise GenerationConflict(
                    f"generation {generation} != {expected_generation}")
            if state != SEALED:
                raise GenerationConflict(f"batch is {state}, not sealed")
            import uuid
            lease = uuid.uuid4().hex
            conn.execute(
                "UPDATE tape_write_batches SET state='writing', "
                "writer_lease_id=%s, writer_lease_owner=%s, "
                "writer_lease_expires_at=now() + make_interval(secs => %s), "
                "write_started_at=COALESCE(write_started_at, now()), "
                "generation=generation+1 WHERE batch_id=%s AND state='sealed'",
                (lease, lease_owner, lease_ttl_seconds, batch_id))
            return lease
        return self._tx(_op)

    # -- per-chunk batch-execution phase ---------------------------------
    def record_chunk_write_start(self, batch_id, chunk_index):
        def _op(conn):
            self._require_state_conn(conn, batch_id, WRITING, "start a write")
            conn.execute(
                "UPDATE tape_write_batch_chunks SET member_write_phase='writing'"
                " WHERE batch_id=%s AND chunk_index=%s", (batch_id, chunk_index))
        return self._tx(_op)

    def commit_chunk_copied(self, batch_id, chunk_index):
        """Record that THIS batch's copy of the chunk completed. The archive
        result stays authoritative in remote_chunks.status."""
        def _op(conn):
            conn.execute(
                "UPDATE tape_write_batch_chunks SET member_write_phase='copied' "
                "WHERE batch_id=%s AND chunk_index=%s", (batch_id, chunk_index))
        return self._tx(_op)

    def fail_chunk(self, batch_id, chunk_index, leave_ambiguous=True):
        new = PHASE_WRITING if leave_ambiguous else PHASE_FAILED
        def _op(conn):
            conn.execute(
                "UPDATE tape_write_batch_chunks SET member_write_phase=%s "
                "WHERE batch_id=%s AND chunk_index=%s",
                (new, batch_id, chunk_index))
        return self._tx(_op)

    # -- sync / durability contract --------------------------------------
    def request_sync(self, batch_id):
        def _op(conn):
            self._require_state_conn(conn, batch_id, WRITING, "request sync")
            undone = conn.execute(
                "SELECT COUNT(*) FROM tape_write_batch_chunks "
                "WHERE batch_id=%s AND member_write_phase <> 'copied'",
                (batch_id,)).fetchone()[0]
            if undone:
                raise SealedBatchError(
                    f"cannot request sync: {undone} chunk(s) not copied")
            conn.execute(
                "UPDATE tape_write_batches SET state='sync_pending', "
                "copies_completed_at=now(), sync_requested_at=now() "
                "WHERE batch_id=%s", (batch_id,))
        return self._tx(_op)

    def confirm_sync(self, batch_id):
        def _op(conn):
            self._require_state_conn(conn, batch_id, SYNC_PENDING,
                                     "confirm sync")
            conn.execute(
                "UPDATE tape_write_batches SET sync_confirmed_at=now() "
                "WHERE batch_id=%s", (batch_id,))
        return self._tx(_op)

    def mark_durable(self, batch_id, durability_kind="ltfs_time5_confirmed"):
        def _op(conn):
            row = conn.execute(
                "SELECT state, sync_confirmed_at FROM tape_write_batches "
                "WHERE batch_id=%s FOR UPDATE", (batch_id,)).fetchone()
            if row is None:
                raise SealedBatchError(f"no batch {batch_id}")
            state, sync_confirmed = row
            if state == DURABLE:
                return
            if state != SYNC_PENDING:
                raise SealedBatchError(f"cannot mark durable from {state}")
            if sync_confirmed is None:
                raise SealedBatchError(
                    "refusing durable: LTFS synchronization not confirmed")
            conn.execute(
                "UPDATE tape_write_batches SET state='durable', durable_at=now(),"
                " durability_kind=%s WHERE batch_id=%s AND state='sync_pending'",
                (durability_kind, batch_id))
        return self._tx(_op)

    def is_prune_safe(self, batch_id):
        return self.get_batch(batch_id)["state"] == DURABLE

    # -- failure / cancellation ------------------------------------------
    def fail_batch(self, batch_id, release_unwritten=True):
        return self._terminate(batch_id, FAILED, "failed_at", release_unwritten)

    def cancel_batch(self, batch_id, release_unwritten=True):
        return self._terminate(batch_id, CANCELLED, "cancelled_at",
                               release_unwritten)

    def _terminate(self, batch_id, state, ts_col, release_unwritten):
        def _op(conn):
            if release_unwritten:
                # Release members that were never copied (never durably written
                # by this batch). A 'copied' member keeps its claim.
                conn.execute(
                    "DELETE FROM tape_write_active_chunk a "
                    "USING tape_write_batch_chunks c "
                    "WHERE a.batch_id=%s AND c.batch_id=a.batch_id "
                    "AND c.chunk_index=a.chunk_index "
                    "AND c.member_write_phase <> 'copied'", (batch_id,))
            conn.execute(
                f"UPDATE tape_write_batches SET state=%s, {ts_col}=now() "
                "WHERE batch_id=%s", (state, batch_id))
        return self._tx(_op)

    # -- crash recovery ---------------------------------------------------
    def discard_unsealed(self, session_id):
        def _op(conn):
            return [r[0] for r in conn.execute(
                "DELETE FROM tape_write_batches WHERE session_id=%s "
                "AND state='building' RETURNING batch_id", (session_id,)
            ).fetchall()]
        return self._tx(_op)

    def recover_abandoned_writer(self, as_of=None):
        def _op(conn):
            return [r[0] for r in conn.execute(
                "UPDATE tape_write_batches SET state='sealed', "
                "writer_lease_id=NULL, writer_lease_owner=NULL, "
                "writer_lease_expires_at=NULL, generation=generation+1 "
                "WHERE state='writing' AND writer_lease_expires_at IS NOT NULL "
                "AND writer_lease_expires_at < COALESCE(%s, now()) "
                "RETURNING batch_id", (as_of,)).fetchall()]
        return self._tx(_op)

    # -- reconciliation with authoritative remote_chunks.status ----------
    def reconcile_batch(self, batch_id, apply=False):
        """Reconcile every member's batch-execution phase against the
        authoritative remote_chunks.status. Returns {chunk_index: RC_*}. Raises
        :class:`ReconcileError` on any impossible combination (fail closed).

        With ``apply=True`` it idempotently promotes RC_AUTHORITY_DONE members to
        'copied' (authority wins) and releases RC_RELEASE members' active claims.
        It NEVER writes remote_chunks and never downgrades a done chunk."""
        def _op(conn):
            rows = conn.execute(
                "SELECT c.chunk_index, c.member_write_phase, r.status "
                "FROM tape_write_batch_chunks c "
                "LEFT JOIN remote_chunks r ON r.session_id=c.session_id "
                "AND r.chunk_index=c.chunk_index WHERE c.batch_id=%s "
                "ORDER BY c.ordinal", (batch_id,)).fetchall()
            outcomes = {}
            for chunk_index, phase, remote_status in rows:
                outcome = reconcile_member(phase, remote_status)
                outcomes[chunk_index] = outcome
                if apply and outcome == RC_AUTHORITY_DONE:
                    conn.execute(
                        "UPDATE tape_write_batch_chunks SET "
                        "member_write_phase='copied' WHERE batch_id=%s "
                        "AND chunk_index=%s", (batch_id, chunk_index))
                elif apply and outcome == RC_RELEASE:
                    conn.execute(
                        "DELETE FROM tape_write_active_chunk WHERE session_id="
                        "(SELECT session_id FROM tape_write_batch_chunks WHERE "
                        "batch_id=%s AND chunk_index=%s) AND chunk_index=%s",
                        (batch_id, chunk_index, chunk_index))
            return outcomes
        return self._tx(_op)

    # -- reads ------------------------------------------------------------
    def get_batch(self, batch_id):
        def _q(conn):
            from psycopg.rows import dict_row
            with conn.cursor(row_factory=dict_row) as cur:
                row = cur.execute(
                    "SELECT * FROM tape_write_batches WHERE batch_id=%s",
                    (batch_id,)).fetchone()
            if row is None:
                raise SealedBatchError(f"no batch {batch_id}")
            return row
        return self._read(_q)

    def get_members(self, batch_id):
        def _q(conn):
            from psycopg.rows import dict_row
            with conn.cursor(row_factory=dict_row) as cur:
                return cur.execute(
                    "SELECT * FROM tape_write_batch_chunks WHERE batch_id=%s "
                    "ORDER BY ordinal", (batch_id,)).fetchall()
        return self._read(_q)

    def reconstruct(self, batch_id):
        batch = self.get_batch(batch_id)
        if batch["state"] == BUILDING:
            raise SealedBatchError("a building batch is not authoritative")
        members = self.get_members(batch_id)
        recomputed = compute_batch_fingerprint(
            batch["expected_tape_label"], batch["format_generation"], members)
        return members, (recomputed == batch["batch_fingerprint"])

    def member_phase(self, batch_id, chunk_index):
        def _q(conn):
            row = conn.execute(
                "SELECT member_write_phase FROM tape_write_batch_chunks "
                "WHERE batch_id=%s AND chunk_index=%s",
                (batch_id, chunk_index)).fetchone()
            return None if row is None else row[0]
        return self._read(_q)

    # -- internal helpers -------------------------------------------------
    @staticmethod
    def _members_conn(conn, batch_id):
        from psycopg.rows import dict_row
        with conn.cursor(row_factory=dict_row) as cur:
            return cur.execute(
                "SELECT * FROM tape_write_batch_chunks WHERE batch_id=%s "
                "ORDER BY ordinal", (batch_id,)).fetchall()

    @staticmethod
    def _require_state_conn(conn, batch_id, expected, action):
        row = conn.execute(
            "SELECT state FROM tape_write_batches WHERE batch_id=%s FOR UPDATE",
            (batch_id,)).fetchone()
        if row is None:
            raise SealedBatchError(f"no batch {batch_id}")
        if row[0] != expected:
            raise SealedBatchError(
                f"cannot {action}: batch is {row[0]}, not {expected}")
        return row[0]
