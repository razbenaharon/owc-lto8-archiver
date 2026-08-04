"""Phase 5B / 5B.5: sealed-batch schema + repository against ISOLATED PostgreSQL.

Safety model (must hold before any DDL runs):
* Opt-in only: skips unless ``LTO_PG_SEALED_BATCH_IT=1``.
* The SERVER is chosen by ``tests/pg_test_guard.py``, which refuses anything
  that is not provably disposable — no implicit defaults, no port 5432, no
  non-loopback host, and no server hosting the production catalog. Formerly
  this module inherited ``build_conninfo``'s defaults, which point at the
  production container on this workstation.
* Throwaway database names carry the guard's per-run marker, and are asserted
  here to differ from every production database name as a second layer.
* ``DROP DATABASE`` runs only for a name THIS run created.
* PostgreSQL-specific: no SQLite fallback.

No production database is created, modified, or dropped. Parent tables
(``tapes`` / ``remote_sessions`` / ``remote_chunks``) are created minimally so the
sealed-batch foreign keys resolve; validation against the REAL production schema
is done separately by the Phase 5B.5 shadow (docs / scratch script).
"""
import os
import threading
import time
import unittest
import uuid

from src.config import ConfigManager
from pg_test_guard import (RUN_PREFIX, create_test_database,
                           drop_test_database, pg_available)
from src import sealed_batch_repository as sbr
from src.sealed_batch_repository import (
    SealedBatchRepository, SealedBatchError, ImmutableBatchError,
    GenerationConflict, SchemaDriftError, ReconcileError, RollbackRefused,
    migration_checksum, reconcile_member,
    BUILDING, SEALED, WRITING, SYNC_PENDING, DURABLE, FAILED, CANCELLED,
    PHASE_NOT_STARTED, PHASE_WRITING, PHASE_COPIED, PHASE_FAILED,
    RC_OK, RC_AUTHORITY_DONE, RC_RELEASE, RC_AMBIGUOUS, MIGRATION_VERSION)

try:
    import psycopg
    from psycopg import errors as pg_errors
except ImportError:                          # pragma: no cover
    psycopg = None

#: Supplied by the guard, unique per pytest run.
THROWAWAY_PREFIX = RUN_PREFIX
_OPT_IN = os.environ.get("LTO_PG_SEALED_BATCH_IT") == "1"
GiB = 1024 ** 3

_MINIMAL_PARENTS = """
CREATE TABLE IF NOT EXISTS tapes (
    volume_label   TEXT PRIMARY KEY,
    total_capacity BIGINT,
    used_space     BIGINT NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS remote_sessions (
    session_id    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    session_label TEXT NOT NULL,
    tape_label    TEXT NOT NULL REFERENCES tapes(volume_label),
    scan_complete BOOLEAN NOT NULL DEFAULT TRUE,
    status        TEXT NOT NULL DEFAULT 'active'
);
CREATE TABLE IF NOT EXISTS remote_chunks (
    session_id  BIGINT NOT NULL REFERENCES remote_sessions(session_id),
    chunk_index INTEGER NOT NULL,
    status      TEXT NOT NULL DEFAULT 'pending',
    PRIMARY KEY (session_id, chunk_index)
);
"""


def _production_dbnames():
    cfg = ConfigManager()
    return {cfg.pg_dbname, "lto_archive"}


def _pg_reachable():
    """Guard-mediated: an UNSAFE configured target raises rather than skips."""
    if psycopg is None or not _OPT_IN:
        return False
    return pg_available()


def _create_throwaway(with_parents=True, with_012=True):
    """Create an isolated throwaway DB (verified name) and return (name, repo)."""
    name, conninfo = create_test_database("sealed_batch")
    assert name.startswith(THROWAWAY_PREFIX)
    assert name not in _production_dbnames()
    if with_parents:
        with psycopg.connect(conninfo) as conn:
            conn.execute(_MINIMAL_PARENTS)
            conn.commit()
    repo = SealedBatchRepository(conninfo)
    if with_012:
        repo.apply_schema()
    return name, conninfo, repo


def _drop_throwaway(name):
    if name in _production_dbnames():                    # pragma: no cover
        print(f"[SAFETY] refusing to drop {name!r}")
        return
    # Refuses on its own if the name lacks this run's marker.
    drop_test_database(name)


def _seed_session(conninfo, n_chunks=12, tape="Tape_5BTEST", scan_complete=False):
    with psycopg.connect(conninfo, autocommit=True) as conn:
        conn.execute("INSERT INTO tapes (volume_label, total_capacity) VALUES "
                     "(%s, 12000000000000) ON CONFLICT (volume_label) DO NOTHING",
                     (tape,))
        sid = conn.execute(
            "INSERT INTO remote_sessions (session_label, tape_label, "
            "scan_complete, status) VALUES (%s,%s,%s,'active') RETURNING "
            "session_id", (f"s_{uuid.uuid4().hex[:8]}", tape, scan_complete)
        ).fetchone()[0]
        for ci in range(n_chunks):
            conn.execute("INSERT INTO remote_chunks (session_id, chunk_index, "
                         "status) VALUES (%s,%s,'pending')", (sid, ci))
    return sid, tape


def _set_remote_status(conninfo, sid, chunk_index, status):
    with psycopg.connect(conninfo, autocommit=True) as conn:
        conn.execute("UPDATE remote_chunks SET status=%s WHERE session_id=%s "
                     "AND chunk_index=%s", (status, sid, chunk_index))


# ===========================================================================
# Migration identity, checksum, and exact schema-drift validation (Part 2)
# ===========================================================================
@unittest.skipUnless(_OPT_IN, "set LTO_PG_SEALED_BATCH_IT=1 to run PG integration")
@unittest.skipUnless(_pg_reachable(), "PostgreSQL server not reachable")
class MigrationIntegrityTests(unittest.TestCase):
    """Each test gets its own DB it may corrupt/roll back."""

    def setUp(self):
        self.name, self.conninfo, self.repo = _create_throwaway(with_012=False)
        with psycopg.connect(self.conninfo) as c:
            c.execute(_MINIMAL_PARENTS)
            c.commit()

    def tearDown(self):
        _drop_throwaway(self.name)

    def _cols(self):
        with psycopg.connect(self.conninfo) as c:
            return c

    def test_01_clean_migration_succeeds_and_records_checksum(self):
        self.repo.apply_schema()
        self.assertTrue(self.repo.schema_applied())
        with psycopg.connect(self.conninfo) as c:
            row = c.execute("SELECT checksum FROM schema_migrations WHERE "
                            "version=%s", (MIGRATION_VERSION,)).fetchone()
        self.assertEqual(row[0], migration_checksum())

    def test_02_reapplying_identical_migration_is_safe(self):
        self.repo.apply_schema()
        self.repo.apply_schema()                 # no raise
        with psycopg.connect(self.conninfo) as c:
            n = c.execute("SELECT COUNT(*) FROM schema_migrations WHERE "
                          "version=%s", (MIGRATION_VERSION,)).fetchone()[0]
        self.assertEqual(n, 1)

    def test_03_modified_checksum_is_rejected(self):
        self.repo.apply_schema()
        with psycopg.connect(self.conninfo, autocommit=True) as c:
            c.execute("UPDATE schema_migrations SET checksum='deadbeef' WHERE "
                      "version=%s", (MIGRATION_VERSION,))
        with self.assertRaises(SchemaDriftError):
            self.repo.apply_schema()
        with self.assertRaises(SchemaDriftError):
            self.repo.assert_schema_valid()

    def test_04_missing_column_detected(self):
        self.repo.apply_schema()
        with psycopg.connect(self.conninfo, autocommit=True) as c:
            c.execute("ALTER TABLE tape_write_batches DROP COLUMN durability_kind")
        with self.assertRaises(SchemaDriftError):
            self.repo.assert_schema_valid()

    def test_05_wrong_data_type_detected(self):
        self.repo.apply_schema()
        with psycopg.connect(self.conninfo, autocommit=True) as c:
            c.execute("ALTER TABLE tape_write_batches "
                      "ALTER COLUMN prepared_bytes TYPE numeric")
        with self.assertRaises(SchemaDriftError):
            self.repo.assert_schema_valid()

    def test_06_missing_constraint_detected(self):
        self.repo.apply_schema()
        with psycopg.connect(self.conninfo, autocommit=True) as c:
            c.execute("ALTER TABLE tape_write_batches "
                      "DROP CONSTRAINT tape_write_batches_durable_ck")
        with self.assertRaises(SchemaDriftError):
            self.repo.assert_schema_valid()

    def test_07_incompatible_pre_existing_table_rejected(self):
        # A table with the same name but the wrong shape must NOT be accepted.
        with psycopg.connect(self.conninfo, autocommit=True) as c:
            c.execute("CREATE TABLE tape_write_batches (batch_id INTEGER "
                      "PRIMARY KEY, note TEXT)")
        with self.assertRaises(SchemaDriftError):
            self.repo.apply_schema()

    def test_08_partial_migration_not_reported_successful(self):
        self.repo.apply_schema()
        # Simulate a partial/incompatible state: drop a whole table.
        with psycopg.connect(self.conninfo, autocommit=True) as c:
            c.execute("DROP TABLE tape_write_active_chunk CASCADE")
        with self.assertRaises(SchemaDriftError):
            self.repo.assert_schema_valid()

    def test_09_nullability_change_detected(self):
        self.repo.apply_schema()
        with psycopg.connect(self.conninfo, autocommit=True) as c:
            c.execute("ALTER TABLE tape_write_batches "
                      "ALTER COLUMN state DROP NOT NULL")
        with self.assertRaises(SchemaDriftError):
            self.repo.assert_schema_valid()


# ===========================================================================
# Rollback safety (Part 7) — isolated DB per test (rollback drops tables)
# ===========================================================================
@unittest.skipUnless(_OPT_IN, "set LTO_PG_SEALED_BATCH_IT=1 to run PG integration")
@unittest.skipUnless(_pg_reachable(), "PostgreSQL server not reachable")
class RollbackSafetyTests(unittest.TestCase):
    def setUp(self):
        self.name, self.conninfo, self.repo = _create_throwaway()

    def tearDown(self):
        _drop_throwaway(self.name)

    def test_empty_schema_rolls_back(self):
        self.repo.rollback_schema()
        self.assertFalse(self.repo.schema_applied())

    def test_non_empty_schema_refuses_normal_rollback(self):
        sid, tape = _seed_session(self.conninfo)
        self.repo.create_building_batch(sid, tape)
        with self.assertRaises(RollbackRefused):
            self.repo.rollback_schema()
        self.assertTrue(self.repo.schema_applied())      # still there

    def test_forced_rollback_overrides(self):
        sid, tape = _seed_session(self.conninfo)
        self.repo.create_building_batch(sid, tape)
        self.repo.rollback_schema(force=True)
        self.assertFalse(self.repo.schema_applied())

    def test_rollback_never_touches_parent_data(self):
        sid, tape = _seed_session(self.conninfo, n_chunks=5)
        self.repo.rollback_schema(force=True)
        with psycopg.connect(self.conninfo) as c:
            tapes = c.execute("SELECT COUNT(*) FROM tapes").fetchone()[0]
            sess = c.execute("SELECT COUNT(*) FROM remote_sessions").fetchone()[0]
            chunks = c.execute("SELECT COUNT(*) FROM remote_chunks").fetchone()[0]
        self.assertEqual((tapes, sess, chunks), (1, 1, 5))


# ===========================================================================
# Lifecycle, reconciliation, concurrency, crash (Parts 3 & 6) — shared DB
# ===========================================================================
@unittest.skipUnless(_OPT_IN, "set LTO_PG_SEALED_BATCH_IT=1 to run PG integration")
@unittest.skipUnless(_pg_reachable(), "PostgreSQL server not reachable")
class SealedBatchLifecyclePgTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.name, cls.conninfo, cls.repo = _create_throwaway()

    @classmethod
    def tearDownClass(cls):
        _drop_throwaway(cls.name)

    def _session(self, n=12, scan_complete=False):
        return _seed_session(self.conninfo, n_chunks=n, scan_complete=scan_complete)

    def _build(self, sid, tape, chunks):
        bid = self.repo.create_building_batch(sid, tape)
        for ci in chunks:
            self.repo.add_chunk(bid, ci, f"_pack_{sid}_{ci:03d}",
                                int(1.7 * GiB), 200000, f"fp-{ci}")
        return bid

    # -- lifecycle sanity ------------------------------------------------
    def test_seal_reconstruct_ordered(self):
        sid, tape = self._session()
        bid = self._build(sid, tape, [2, 0, 1])
        self.repo.seal_batch(bid)
        members, ok = self.repo.reconstruct(bid)
        self.assertTrue(ok)
        self.assertEqual([m["chunk_index"] for m in members], [2, 0, 1])

    def test_durable_requires_sync_confirmation(self):
        sid, tape = self._session()
        bid = self._build(sid, tape, [0])
        self.repo.seal_batch(bid)
        self.repo.claim_for_write(bid, "host")
        self.repo.record_chunk_write_start(bid, 0)
        self.repo.commit_chunk_copied(bid, 0)
        self.repo.request_sync(bid)
        with self.assertRaises(SealedBatchError):
            self.repo.mark_durable(bid)
        self.repo.confirm_sync(bid)
        self.repo.mark_durable(bid)
        self.assertEqual(self.repo.get_batch(bid)["state"], DURABLE)

    def test_incomplete_scan_does_not_block_sealing(self):
        sid, tape = self._session(scan_complete=False)
        bid = self._build(sid, tape, [0, 1])
        self.assertTrue(self.repo.seal_batch(bid))
        with psycopg.connect(self.conninfo) as c:
            sc = c.execute("SELECT scan_complete FROM remote_sessions WHERE "
                           "session_id=%s", (sid,)).fetchone()[0]
        self.assertFalse(sc, "sealing changed session scan completeness")

    # -- Part 3: chunk authority + reconciliation ------------------------
    def test_reconcile_member_matrix(self):
        # Pure matrix (no DB): valid rows classify, impossible rows raise.
        self.assertEqual(reconcile_member(PHASE_NOT_STARTED, "pending"), RC_OK)
        self.assertEqual(reconcile_member(PHASE_WRITING, "backing"), RC_AMBIGUOUS)
        self.assertEqual(reconcile_member(PHASE_WRITING, "done"), RC_AUTHORITY_DONE)
        self.assertEqual(reconcile_member(PHASE_COPIED, "done"), RC_OK)
        self.assertEqual(reconcile_member(PHASE_NOT_STARTED, "done"), RC_RELEASE)
        for phase, status in [(PHASE_COPIED, "pending"),
                              (PHASE_WRITING, "packing"),
                              (PHASE_NOT_STARTED, "backing")]:
            with self.assertRaises(ReconcileError):
                reconcile_member(phase, status)
        with self.assertRaises(ReconcileError):
            reconcile_member(PHASE_NOT_STARTED, None)   # no remote row

    def test_completed_remote_chunk_authoritative_after_batch_failure(self):
        sid, tape = self._session()
        bid = self._build(sid, tape, [0])
        self.repo.seal_batch(bid)
        self.repo.claim_for_write(bid, "host")
        self.repo.record_chunk_write_start(bid, 0)
        self.repo.commit_chunk_copied(bid, 0)
        _set_remote_status(self.conninfo, sid, 0, "done")   # authoritative
        self.repo.fail_batch(bid)                            # batch fails after
        # remote_chunks stays done; reconcile agrees; batch failure never
        # de-authorized the completed chunk.
        self.assertEqual(self.repo.reconcile_batch(bid), {0: RC_OK})
        with psycopg.connect(self.conninfo) as c:
            st = c.execute("SELECT status FROM remote_chunks WHERE session_id=%s "
                           "AND chunk_index=0", (sid,)).fetchone()[0]
        self.assertEqual(st, "done")

    def test_unstarted_member_reusable(self):
        sid, tape = self._session()
        bid = self._build(sid, tape, [0, 1])
        self.repo.seal_batch(bid)
        self.repo.claim_for_write(bid, "host")
        self.repo.record_chunk_write_start(bid, 0)
        self.repo.commit_chunk_copied(bid, 0)
        _set_remote_status(self.conninfo, sid, 0, "done")
        self.repo.fail_batch(bid, release_unwritten=True)
        # chunk 1 unstarted -> claim released -> re-batchable
        b2 = self.repo.create_building_batch(sid, tape)
        self.repo.add_chunk(b2, 1, f"_pack_{sid}_001", 1, 1)
        self.assertEqual(self.repo.member_phase(b2, 1), PHASE_NOT_STARTED)

    def test_impossible_mismatch_detected(self):
        sid, tape = self._session()
        bid = self._build(sid, tape, [0])
        self.repo.seal_batch(bid)
        self.repo.claim_for_write(bid, "host")
        self.repo.record_chunk_write_start(bid, 0)
        self.repo.commit_chunk_copied(bid, 0)      # member says copied
        # authority still 'pending' -> impossible
        with self.assertRaises(ReconcileError):
            self.repo.reconcile_batch(bid)

    def test_stale_batch_state_cannot_overwrite_remote_truth(self):
        sid, tape = self._session()
        bid = self._build(sid, tape, [0])
        self.repo.seal_batch(bid)
        self.repo.claim_for_write(bid, "host")
        self.repo.record_chunk_write_start(bid, 0)  # member 'writing'
        _set_remote_status(self.conninfo, sid, 0, "done")   # authority: done
        outcomes = self.repo.reconcile_batch(bid, apply=True)
        self.assertEqual(outcomes, {0: RC_AUTHORITY_DONE})
        # authority won: member promoted to copied; remote stays done.
        self.assertEqual(self.repo.member_phase(bid, 0), PHASE_COPIED)
        with psycopg.connect(self.conninfo) as c:
            st = c.execute("SELECT status FROM remote_chunks WHERE session_id=%s "
                           "AND chunk_index=0", (sid,)).fetchone()[0]
        self.assertEqual(st, "done")

    def test_reconciliation_is_idempotent(self):
        sid, tape = self._session()
        bid = self._build(sid, tape, [0])
        self.repo.seal_batch(bid)
        self.repo.claim_for_write(bid, "host")
        self.repo.record_chunk_write_start(bid, 0)
        _set_remote_status(self.conninfo, sid, 0, "done")
        first = self.repo.reconcile_batch(bid, apply=True)
        second = self.repo.reconcile_batch(bid, apply=True)   # now RC_OK
        self.assertEqual(first, {0: RC_AUTHORITY_DONE})
        self.assertEqual(second, {0: RC_OK})
        self.assertEqual(self.repo.reconcile_batch(bid), {0: RC_OK})  # stable

    # -- Part 6: concurrency + crash -------------------------------------
    def test_ten_concurrent_claims_one_winner(self):
        sid, tape = self._session()
        bid = self._build(sid, tape, [0])
        self.repo.seal_batch(bid)
        results = []
        lock = threading.Lock()
        barrier = threading.Barrier(10)

        def claimer(k):
            repo = SealedBatchRepository(self.conninfo)
            barrier.wait()
            try:
                repo.claim_for_write(bid, f"host-{k}")
                out = "won"
            except (GenerationConflict, SealedBatchError):
                out = "lost"
            with lock:
                results.append(out)
        ts = [threading.Thread(target=claimer, args=(k,)) for k in range(10)]
        for t in ts:
            t.start()
        for t in ts:
            t.join(timeout=30)
        self.assertEqual(results.count("won"), 1)
        self.assertEqual(results.count("lost"), 9)

    def test_concurrent_add_same_chunk_one_winner(self):
        sid, tape = self._session()
        b1 = self.repo.create_building_batch(sid, tape)
        b2 = self.repo.create_building_batch(sid, tape)
        outcomes = []
        lock = threading.Lock()
        barrier = threading.Barrier(2)

        def adder(bid):
            repo = SealedBatchRepository(self.conninfo)
            barrier.wait()
            try:
                repo.add_chunk(bid, 5, f"_pack_{sid}_005", 1, 1)
                out = "won"
            except SealedBatchError:
                out = "lost"
            with lock:
                outcomes.append(out)
        ts = [threading.Thread(target=adder, args=(b,)) for b in (b1, b2)]
        for t in ts:
            t.start()
        for t in ts:
            t.join(timeout=20)
        self.assertEqual(sorted(outcomes), ["lost", "won"])

    def test_writer_loss_after_claim_then_recovery(self):
        sid, tape = self._session()
        bid = self._build(sid, tape, [0])
        self.repo.seal_batch(bid)
        self.repo.claim_for_write(bid, "dead", lease_ttl_seconds=1)
        from datetime import datetime, timedelta, timezone
        gen = self.repo.get_batch(bid)["generation"]
        recovered = self.repo.recover_abandoned_writer(
            as_of=datetime.now(timezone.utc) + timedelta(hours=1))
        # recover_abandoned_writer is global (it sweeps every expired writer);
        # this batch must be among those recovered.
        self.assertIn(bid, recovered)
        b = self.repo.get_batch(bid)
        self.assertEqual(b["state"], SEALED)
        self.assertGreater(b["generation"], gen)
        # A stale token cannot re-claim at the old generation.
        with self.assertRaises(GenerationConflict):
            self.repo.claim_for_write(bid, "new", expected_generation=gen)
        self.repo.claim_for_write(bid, "new")            # fresh claim ok

    def test_rollback_during_seal_leaves_building(self):
        # Prove seal is atomic: a transaction that performs the seal UPDATE then
        # raises leaves the batch building, uncommitted.
        sid, tape = self._session()
        bid = self._build(sid, tape, [0])
        try:
            with psycopg.connect(self.conninfo) as conn:
                with conn.transaction():
                    conn.execute("UPDATE tape_write_batches SET state='sealed', "
                                 "batch_fingerprint='x', sealed_at=now() "
                                 "WHERE batch_id=%s", (bid,))
                    raise RuntimeError("crash before commit")
        except RuntimeError:
            pass
        self.assertEqual(self.repo.get_batch(bid)["state"], BUILDING)

    def test_rollback_during_chunk_completion(self):
        sid, tape = self._session()
        bid = self._build(sid, tape, [0])
        self.repo.seal_batch(bid)
        self.repo.claim_for_write(bid, "host")
        self.repo.record_chunk_write_start(bid, 0)
        try:
            with psycopg.connect(self.conninfo) as conn:
                with conn.transaction():
                    conn.execute("UPDATE tape_write_batch_chunks SET "
                                 "member_write_phase='copied' WHERE batch_id=%s",
                                 (bid,))
                    raise RuntimeError("crash before commit")
        except RuntimeError:
            pass
        self.assertEqual(self.repo.member_phase(bid, 0), PHASE_WRITING)

    def test_connection_loss_before_commit(self):
        sid, tape = self._session()
        bid = self._build(sid, tape, [0])
        conn = psycopg.connect(self.conninfo)
        conn.execute("UPDATE tape_write_batches SET chunk_count=999 "
                     "WHERE batch_id=%s", (bid,))
        conn.close()                                      # closed WITHOUT commit
        self.assertEqual(self.repo.get_batch(bid)["chunk_count"], 1)

    def test_idempotent_reexecution_after_uncertain_outcome(self):
        sid, tape = self._session()
        bid = self._build(sid, tape, [0])
        self.repo.seal_batch(bid)
        self.repo.claim_for_write(bid, "host")
        self.repo.record_chunk_write_start(bid, 0)
        self.repo.commit_chunk_copied(bid, 0)
        self.repo.request_sync(bid)
        self.repo.confirm_sync(bid)
        self.repo.mark_durable(bid)
        at = self.repo.get_batch(bid)["durable_at"]
        self.repo.mark_durable(bid)                       # retry after uncertainty
        self.assertEqual(self.repo.get_batch(bid)["durable_at"], at)

    def test_lock_timeout_is_clean(self):
        sid, tape = self._session()
        bid = self._build(sid, tape, [0])
        self.repo.seal_batch(bid)
        holder = psycopg.connect(self.conninfo)          # non-autocommit
        holder.execute("SELECT 1 FROM tape_write_batches WHERE batch_id=%s "
                       "FOR UPDATE", (bid,))              # implicit tx holds lock
        try:
            other = psycopg.connect(self.conninfo)
            other.autocommit = True
            other.execute("SET lock_timeout = '500ms'")  # session-level
            other.autocommit = False
            with self.assertRaises(pg_errors.LockNotAvailable):
                with other.transaction():
                    other.execute("SELECT 1 FROM tape_write_batches WHERE "
                                  "batch_id=%s FOR UPDATE", (bid,))
            other.close()
        finally:
            holder.rollback()
            holder.close()
        # After the holder releases, a normal claim still works cleanly.
        self.repo.claim_for_write(bid, "host")

    def test_deadlock_detected_and_rolled_back(self):
        sid, tape = self._session()
        b1 = self._build(sid, tape, [0])
        b2 = self._build(sid, tape, [1])
        self.repo.seal_batch(b1)
        self.repo.seal_batch(b2)
        errors_seen = []
        start = threading.Barrier(2)                     # only the two workers

        def worker(first, second):
            conn = psycopg.connect(self.conninfo)        # non-autocommit
            try:
                conn.execute("SELECT 1 FROM tape_write_batches WHERE batch_id=%s "
                             "FOR UPDATE", (first,))      # lock own row first
                start.wait()                              # both hold their first
                conn.execute("SELECT 1 FROM tape_write_batches WHERE batch_id=%s "
                             "FOR UPDATE", (second,))      # cross -> deadlock
                conn.commit()
            except pg_errors.DeadlockDetected:
                errors_seen.append("deadlock")
                conn.rollback()
            except Exception as e:                        # pragma: no cover
                errors_seen.append(type(e).__name__)
                conn.rollback()
            finally:
                conn.close()
        t1 = threading.Thread(target=worker, args=(b1, b2))
        t2 = threading.Thread(target=worker, args=(b2, b1))
        t1.start(); t2.start()
        t1.join(timeout=20); t2.join(timeout=20)
        # PostgreSQL detected and aborted exactly one side cleanly.
        self.assertIn("deadlock", errors_seen)
        # DB remains consistent and usable.
        self.assertTrue(self.repo.schema_applied())

    # -- Phase 5D read-only observation status reader --------------------
    def test_readonly_status_reader_reads_exact_chunks(self):
        from src.sealed_batch_observation import _StatusReader
        sid, tape = self._session(n=6)
        _set_remote_status(self.conninfo, sid, 2, "backing")
        reader = _StatusReader(self.conninfo, statement_timeout_seconds=5)
        statuses, secs, iso, err = reader.read(sid, [0, 2, 5])
        self.assertIsNone(err)
        self.assertEqual(statuses, {0: "pending", 2: "backing", 5: "pending"})
        self.assertIsNotNone(iso)                    # a real isolation level
        self.assertGreaterEqual(secs, 0.0)

    def test_readonly_status_reader_does_not_mutate(self):
        from src.sealed_batch_observation import _StatusReader
        sid, tape = self._session(n=3)
        with psycopg.connect(self.conninfo) as c:
            before = c.execute("SELECT status FROM remote_chunks WHERE "
                               "session_id=%s ORDER BY chunk_index", (sid,)
                               ).fetchall()
        _StatusReader(self.conninfo, 5).read(sid, [0, 1, 2])
        with psycopg.connect(self.conninfo) as c:
            after = c.execute("SELECT status FROM remote_chunks WHERE "
                              "session_id=%s ORDER BY chunk_index", (sid,)
                              ).fetchall()
        self.assertEqual(before, after)              # read-only: nothing changed

    def test_status_reader_bad_dsn_is_unavailable(self):
        from src.sealed_batch_observation import _StatusReader
        statuses, secs, iso, err = _StatusReader(
            "postgresql://nouser@127.0.0.1:1/nodb", 2).read(37, [0])
        self.assertIsNone(statuses)                  # -> DB_OBSERVATION_UNAVAILABLE
        self.assertIsNotNone(err)

    # -- Part 5 feature gate ---------------------------------------------
    def test_feature_flag_false_no_activity(self):
        class _Cfg:
            sealed_tape_write_batches_enabled = False

        class _Tripwire:
            def schema_applied(self):
                raise AssertionError("touched while disabled")
        self.assertFalse(sbr.assert_feature_ready(_Cfg(), _Tripwire()))

    def test_feature_flag_true_requires_valid_schema(self):
        class _CfgOn:
            sealed_tape_write_batches_enabled = True
        self.assertTrue(sbr.assert_feature_ready(_CfgOn(), self.repo))

        class _NoSchema:
            def schema_applied(self):
                return False
        with self.assertRaises(RuntimeError):
            sbr.assert_feature_ready(_CfgOn(), _NoSchema())


if __name__ == "__main__":
    unittest.main()
