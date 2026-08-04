"""Isolated-PostgreSQL coverage for explicit migration 018.

These tests deliberately use the same fail-closed disposable-server guard as
the rest of the PostgreSQL integration suite.  They never infer a PostgreSQL
target and therefore cannot fall back to the production listener.
"""

from contextlib import contextmanager
from datetime import datetime, timezone
import os
import tempfile
import unittest
from typing import Any, TYPE_CHECKING, cast

if TYPE_CHECKING:
    import psycopg
    from psycopg import errors
    from psycopg.rows import dict_row
else:
    try:
        import psycopg
        from psycopg import errors
        from psycopg.rows import dict_row
    except ImportError:  # pragma: no cover - skipped without psycopg
        psycopg = None
        errors = None
        dict_row = None

from pg_test_guard import (SKIP_REASON, create_test_database,
                           drop_test_database, pg_available)


def _connect(*args, **kwargs) -> Any:
    return cast(Any, psycopg.connect(*args, **kwargs))


def _pg_available():
    if psycopg is None:
        return False
    return pg_available()


@unittest.skipUnless(_pg_available(), SKIP_REASON)
class ManifestDirectoryCatalogMigrationTests(unittest.TestCase):
    """Migration 018 against a fresh disposable database per test."""

    def setUp(self):
        from src.pg_db import PgDatabaseManager

        self.dbname, self.conninfo = create_test_database("lto_migration_018")
        self.db = PgDatabaseManager(self.conninfo)
        self.staging = tempfile.TemporaryDirectory()
        self.addCleanup(self._drop_primary)

    def _drop_primary(self):
        try:
            self.db.close()
        finally:
            try:
                self.staging.cleanup()
            finally:
                drop_test_database(self.dbname)

    def _additional_database(self, tag):
        from src.pg_db import PgDatabaseManager

        dbname, conninfo = create_test_database(tag)
        db = PgDatabaseManager(conninfo)

        def cleanup():
            try:
                db.close()
            finally:
                drop_test_database(dbname)

        self.addCleanup(cleanup)
        return db, conninfo

    def _query(self, sql, params=(), *, conninfo=None):
        target = conninfo or self.conninfo
        with _connect(target, autocommit=True,
                      row_factory=cast(Any, dict_row)) as conn:
            return conn.execute(sql, params).fetchall()

    def _exec(self, sql, params=(), *, conninfo=None):
        target = conninfo or self.conninfo
        with _connect(target, autocommit=True) as conn:
            if params:
                conn.execute(sql, params)
            else:
                conn.execute(sql)

    def _session(self, label="MIGRATION_018_SESSION",
                 tape="MIGRATION_018_TAPE", chunks=(0,), *, db=None):
        target = db or self.db
        target.register_tape(tape, 12000)
        session_id = target.create_remote_streaming_session(
            session_label=label,
            remote_host="fixture.example",
            remote_user="fixture",
            remote_path=f"/fixture/{label.lower()}",
            tape_label=tape,
            staging_dir=self.staging.name,
        )
        for chunk_index in chunks:
            target.append_remote_streaming_chunk(
                session_id,
                int(chunk_index),
                [(int(chunk_index),
                  f"/fixture/{label.lower()}/chunk_{chunk_index:03d}.bin",
                  f"chunk_{chunk_index:03d}.bin",
                  int(chunk_index) + 1)],
            )
        return session_id

    @staticmethod
    def _staging_evidence(indexes):
        return {
            "root_accessible": True,
            "checked_chunk_indexes": list(indexes),
            "entry_count": 0,
            "unreadable_count": 0,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }

    def _catalog_file(self, session_id, chunk_index, *,
                      tape="MIGRATION_018_TAPE", db=None,
                      original_path=None):
        target = db or self.db
        path = original_path or (
            f"/fixture/catalog/chunk_{int(chunk_index):03d}.bin")
        target.bulk_upsert_files([{
            "original_path": path,
            "canonical_source_path": path,
            "file_size_bytes": int(chunk_index) + 1,
            "tape_label": tape,
            "source_host": "fixture",
            "is_packed": False,
            "container_name": None,
            "stored_path": f"catalog/chunk_{int(chunk_index):03d}.bin",
            "remote_session_id": session_id,
            "remote_chunk_index": int(chunk_index),
        }])
        return path

    def _apply_plan2(self, *, db=None, exception_session_id=None,
                     expected_boundary=None):
        target = db or self.db
        target.apply_incremental_scan_schema(finalize=True)
        kwargs = {}
        if exception_session_id is not None:
            kwargs = {
                "exception_session_id": exception_session_id,
                "expected_boundary": expected_boundary,
                "approval_id": "migration-018-test-boundary",
                "approval_reason": "synthetic never-started suffix fixture",
                "staging_evidence": self._staging_evidence(
                    [expected_boundary]),
            }
        target.apply_container_format_schema(**kwargs)
        self.assertEqual(
            target.apply_stored_tar_plan_schema(),
            ["016_postgres_stored_tar_plans.sql",
             "017_postgres_stored_tar_publication.sql"],
        )

    def _apply_018(self, *, db=None):
        target = db or self.db
        self.assertEqual(
            target.apply_manifest_directory_catalog_schema(),
            ["018_postgres_manifest_directory_catalog.sql"],
        )

    def _ready_artifact(self, session_id, chunk_index, *,
                        version="plan-v1", readiness="ready",
                        kind="plan_manifest", locator=None):
        locator = locator or (
            f"manifests/{session_id}/{chunk_index}/{version}.jsonl.zst")
        published_at = (datetime.now(timezone.utc)
                        if readiness == "ready" else None)
        size = 101 if readiness == "ready" else None
        return self._query(
            """INSERT INTO archive_artifacts
                   (session_id,chunk_index,artifact_kind,artifact_version,
                    local_locator,artifact_size_bytes,readiness_state,
                    published_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
               RETURNING *""",
            (session_id, chunk_index, kind, version, locator, size,
             readiness, published_at),
        )[0]

    def _mark_done(self, session_id, chunk_index):
        self._exec(
            """UPDATE remote_chunks
               SET status='done', owner_token=NULL, lease_expires_at=NULL,
                   attempt_id=NULL, updated_at=now()
               WHERE session_id=%s AND chunk_index=%s""",
            (session_id, chunk_index),
        )

    def _transition_chunk(self, session_id, chunk_index, artifact_id, *,
                          gate="plan4-gate", evidence="plan4-evidence",
                          approval="operator@example.test"):
        return self._query(
            """SELECT lto_transition_chunk_plan_source(
                       %s,%s,%s,%s,%s,%s) AS audit_id""",
            (session_id, chunk_index, artifact_id, gate, evidence, approval),
        )[0]["audit_id"]

    def _insert_epoch(self, session_id, epoch, prior_source, new_source,
                      last_chunk, first_chunk):
        return self._query(
            """INSERT INTO remote_session_plan_transitions
                   (session_id,transition_epoch,state,prior_plan_source,
                    new_plan_source,last_chunk_before_transition,
                    first_chunk_after_transition)
               VALUES (%s,%s,'draft',%s,%s,%s,%s)
               RETURNING transition_id""",
            (session_id, epoch, prior_source, new_source,
             last_chunk, first_chunk),
        )[0]["transition_id"]

    def _approve_epoch(self, transition_id, *, activate=False):
        self._exec(
            """UPDATE remote_session_plan_transitions
               SET state='rehearsed', scan_frontier_generation=7,
                   evidence_report_locator='reports/transition.json'
               WHERE transition_id=%s""",
            (transition_id,),
        )
        self._exec(
            """UPDATE remote_session_plan_transitions
               SET state='approved', approval_identity='operator@example.test',
                   approved_at=now()
               WHERE transition_id=%s""",
            (transition_id,),
        )
        if activate:
            self._exec(
                """UPDATE remote_session_plan_transitions SET state='active'
                   WHERE transition_id=%s""",
                (transition_id,),
            )

    def _reserve_artifact_id(self):
        return self._query(
            """SELECT nextval(pg_get_serial_sequence(
                       'archive_artifacts','artifact_id')) AS artifact_id"""
        )[0]["artifact_id"]

    def _insert_manifest_chunk_transaction(self, session_id, chunk_index, *,
                                           readiness="ready",
                                           artifact_kind="plan_manifest",
                                           include_artifact=True):
        artifact_id = self._reserve_artifact_id()
        with _connect(self.conninfo,
                      row_factory=cast(Any, dict_row)) as conn:
            with conn.transaction():
                conn.execute(
                    """INSERT INTO remote_chunks
                           (session_id,chunk_index,status,updated_at,
                            packaging_format,packaging_assigned_at,
                            plan_source,plan_manifest_artifact_id)
                       VALUES (%s,%s,'pending',now(),'zip',now(),
                               'manifest',%s)""",
                    (session_id, chunk_index, artifact_id),
                )
                if include_artifact:
                    ready = readiness == "ready"
                    conn.execute(
                        """INSERT INTO archive_artifacts
                               (artifact_id,session_id,chunk_index,artifact_kind,
                                artifact_version,local_locator,
                                artifact_size_bytes,readiness_state,published_at)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        (artifact_id, session_id, chunk_index, artifact_kind,
                         f"initial-{chunk_index}",
                         f"manifests/initial-{chunk_index}.jsonl.zst",
                         13 if ready else None, readiness,
                         datetime.now(timezone.utc) if ready else None),
                    )
        return artifact_id

    def _object_snapshot(self):
        """Definitions of every pre-existing public-schema object."""
        return {
            "relations": {
                (row["relation_name"], row["relation_kind"])
                for row in self._query(
                    """SELECT c.relname AS relation_name,
                              c.relkind::text AS relation_kind
                       FROM pg_class c
                       JOIN pg_namespace n ON n.oid=c.relnamespace
                       WHERE n.nspname='public'
                         AND c.relkind IN ('r','p','v','m','S')""")
            },
            "columns": {
                (row["table_name"], row["column_name"]):
                    (row["data_type"], row["udt_name"], row["is_nullable"],
                     row["column_default"], row["is_identity"])
                for row in self._query(
                    """SELECT table_name,column_name,data_type,udt_name,
                              is_nullable,column_default,is_identity
                       FROM information_schema.columns
                       WHERE table_schema='public'""")
            },
            "constraints": {
                (row["table_name"], row["constraint_name"]): row["definition"]
                for row in self._query(
                    """SELECT c.relname AS table_name,
                              con.conname AS constraint_name,
                              pg_get_constraintdef(con.oid, true) AS definition
                       FROM pg_constraint con
                       JOIN pg_class c ON c.oid=con.conrelid
                       JOIN pg_namespace n ON n.oid=c.relnamespace
                       WHERE n.nspname='public'""")
            },
            "indexes": {
                (row["table_name"], row["index_name"]): row["definition"]
                for row in self._query(
                    """SELECT tablename AS table_name,indexname AS index_name,
                              indexdef AS definition
                       FROM pg_indexes WHERE schemaname='public'""")
            },
            "triggers": {
                (row["table_name"], row["trigger_name"]): row["definition"]
                for row in self._query(
                    """SELECT c.relname AS table_name,t.tgname AS trigger_name,
                              pg_get_triggerdef(t.oid, true) AS definition
                       FROM pg_trigger t
                       JOIN pg_class c ON c.oid=t.tgrelid
                       JOIN pg_namespace n ON n.oid=c.relnamespace
                       WHERE n.nspname='public' AND NOT t.tgisinternal""")
            },
            "functions": {
                row["identity"]: row["definition"]
                for row in self._query(
                    """SELECT p.oid::regprocedure::text AS identity,
                              pg_get_functiondef(p.oid) AS definition
                       FROM pg_proc p
                       JOIN pg_namespace n ON n.oid=p.pronamespace
                       WHERE n.nspname='public'""")
            },
            "views": {
                row["view_name"]: row["definition"]
                for row in self._query(
                    """SELECT c.relname AS view_name,
                              pg_get_viewdef(c.oid, true) AS definition
                       FROM pg_class c
                       JOIN pg_namespace n ON n.oid=c.relnamespace
                       WHERE n.nspname='public' AND c.relkind IN ('v','m')""")
            },
        }

    @staticmethod
    def _assert_snapshot_preserved(testcase, before, after):
        testcase.assertTrue(before["relations"].issubset(after["relations"]))
        for category in (
                "columns", "constraints", "indexes", "triggers",
                "functions", "views"):
            for key, definition in before[category].items():
                testcase.assertIn(key, after[category], (category, key))
                testcase.assertEqual(
                    after[category][key], definition, (category, key))

    # -- explicit application, upgrade and backfill ---------------------

    def test_explicit_only_requires_complete_plan2_and_is_transactional(self):
        self.assertFalse(
            self.db.manifest_directory_catalog_schema_installed())
        self.assertEqual(self._query(
            """SELECT count(*) AS n FROM information_schema.columns
               WHERE table_schema='public' AND table_name='remote_chunks'
                 AND column_name='plan_source'""")[0]["n"], 0)

        with self.assertRaisesRegex(Exception, "requires finalized migration 014"):
            self.db.apply_manifest_directory_catalog_schema()
        self.assertIsNone(self._query(
            "SELECT to_regclass('manifest_directory_catalog_schema_metadata') AS r"
        )[0]["r"])
        self.assertEqual(self._query(
            """SELECT count(*) AS n FROM information_schema.columns
               WHERE table_schema='public' AND table_name='remote_chunks'
                 AND column_name='plan_source'""")[0]["n"], 0)

        self._apply_plan2()
        migration_sql = (
            self.db.manifest_directory_catalog_migration_path()
            .read_text(encoding="utf-8"))
        with self.assertRaisesRegex(Exception, "explicit guarded command"):
            self._exec(migration_sql)
        self.assertIsNone(self._query(
            "SELECT to_regclass('manifest_directory_catalog_schema_metadata') AS r"
        )[0]["r"])

    def test_legacy_backfill_and_second_apply_are_noops(self):
        session_id = self._session(chunks=(0, 1))
        self._catalog_file(session_id, 0)
        self._exec(
            "UPDATE remote_chunks SET status='done' "
            "WHERE session_id=%s AND chunk_index=0", (session_id,))
        self._apply_plan2(
            exception_session_id=session_id, expected_boundary=1)
        before = self._query(
            """SELECT c.chunk_index,c.status,c.packaging_format,
                      c.packaging_assigned_at,c.membership_state,
                      c.expected_file_count,c.expected_bytes,
                      array_agg(pf.plan_file_id ORDER BY pf.ordinal) AS members
               FROM remote_chunks c
               LEFT JOIN remote_sessions s ON s.session_id=c.session_id
               LEFT JOIN remote_plan_files pf
                 ON pf.plan_id=s.plan_id AND pf.chunk_index=c.chunk_index
               WHERE c.session_id=%s
               GROUP BY c.chunk_index,c.status,c.packaging_format,
                        c.packaging_assigned_at,c.membership_state,
                        c.expected_file_count,c.expected_bytes
               ORDER BY c.chunk_index""", (session_id,))
        self.assertEqual(
            [row["packaging_format"] for row in before],
            ["zip", "stored_tar"],
        )

        self._apply_018()
        backfilled = self._query(
            """SELECT chunk_index,plan_source,plan_manifest_artifact_id,
                      terminal_manifest_artifact_id,plan_ordinal_scope,
                      packaging_format
               FROM remote_chunks WHERE session_id=%s ORDER BY chunk_index""",
            (session_id,),
        )
        self.assertEqual(
            [(row["plan_source"], row["plan_manifest_artifact_id"],
              row["terminal_manifest_artifact_id"],
              row["plan_ordinal_scope"])
             for row in backfilled],
            [("legacy_db", None, None, "chunk")] * 2,
        )
        self.assertEqual(
            [row["packaging_format"] for row in backfilled],
            ["zip", "stored_tar"],
        )
        after = self._query(
            """SELECT c.chunk_index,c.status,c.packaging_format,
                      c.packaging_assigned_at,c.membership_state,
                      c.expected_file_count,c.expected_bytes,
                      array_agg(pf.plan_file_id ORDER BY pf.ordinal) AS members
               FROM remote_chunks c
               LEFT JOIN remote_sessions s ON s.session_id=c.session_id
               LEFT JOIN remote_plan_files pf
                 ON pf.plan_id=s.plan_id AND pf.chunk_index=c.chunk_index
               WHERE c.session_id=%s
               GROUP BY c.chunk_index,c.status,c.packaging_format,
                        c.packaging_assigned_at,c.membership_state,
                        c.expected_file_count,c.expected_bytes
               ORDER BY c.chunk_index""", (session_id,))
        self.assertEqual(after, before)

        metadata_before = self._query(
            "SELECT * FROM manifest_directory_catalog_schema_metadata")[0]
        fingerprint_before = self._query(
            "SELECT lto_manifest_directory_catalog_schema_fingerprint() AS f"
        )[0]["f"]
        counts_before = self._query(
            """SELECT (SELECT count(*) FROM remote_session_plan_transitions) AS e,
                      (SELECT count(*) FROM remote_chunk_plan_source_transitions) AS a,
                      (SELECT count(*) FROM archive_directories) AS d""")[0]
        self._apply_018()
        self.assertEqual(
            self._query(
                "SELECT * FROM manifest_directory_catalog_schema_metadata")[0],
            metadata_before,
        )
        self.assertEqual(self._query(
            "SELECT lto_manifest_directory_catalog_schema_fingerprint() AS f"
        )[0]["f"], fingerprint_before)
        self.assertEqual(self._query(
            """SELECT (SELECT count(*) FROM remote_session_plan_transitions) AS e,
                      (SELECT count(*) FROM remote_chunk_plan_source_transitions) AS a,
                      (SELECT count(*) FROM archive_directories) AS d""")[0],
            counts_before)
        self.assertTrue(
            self.db.validate_manifest_directory_catalog_schema()["ready"])

    def test_logical_identity_is_session_label_plus_chunk_index(self):
        session_id = self._session(label="STABLE_LOGICAL_SESSION", chunks=(0,))
        self._apply_plan2()
        self._apply_018()

        db2, conninfo2 = self._additional_database("lto_018_identity")
        self._session(label="IDENTITY_SEQUENCE_OFFSET", tape="OFFSET_TAPE",
                      chunks=(), db=db2)
        session_id_2 = self._session(
            label="STABLE_LOGICAL_SESSION", tape="STABLE_TAPE_2",
            chunks=(0,), db=db2)
        self._apply_plan2(db=db2)
        self._apply_018(db=db2)

        self.assertNotEqual(session_id, session_id_2)
        identity1 = self._query(
            """SELECT s.session_label,c.chunk_index
               FROM remote_chunks c JOIN remote_sessions s USING(session_id)
               WHERE s.session_label='STABLE_LOGICAL_SESSION'""")[0]
        identity2 = self._query(
            """SELECT s.session_label,c.chunk_index
               FROM remote_chunks c JOIN remote_sessions s USING(session_id)
               WHERE s.session_label='STABLE_LOGICAL_SESSION'""",
            conninfo=conninfo2)[0]
        self.assertEqual(identity1, identity2)
        self.assertEqual(
            (identity1["session_label"], identity1["chunk_index"]),
            ("STABLE_LOGICAL_SESSION", 0),
        )

        constraints = {
            row["conname"]: row["definition"]
            for row in self._query(
                """SELECT conname,pg_get_constraintdef(oid,true) AS definition
                   FROM pg_constraint
                   WHERE conrelid IN ('remote_sessions'::regclass,
                                      'remote_chunks'::regclass,
                                      'archive_artifacts'::regclass)""")
        }
        self.assertIn("UNIQUE (session_label)",
                      constraints["uq_remote_sessions_label"])
        self.assertTrue(any(
            "PRIMARY KEY (session_id, chunk_index)" in definition
            for definition in constraints.values()))
        self.assertIn(
            "FOREIGN KEY (session_id, chunk_index)",
            constraints["archive_artifacts_chunk_fk"],
        )

    # -- authority and transition epochs --------------------------------

    def test_plan_source_is_immutable_except_atomic_audited_terminal_transition(self):
        session_id = self._session(chunks=(0,))
        self._apply_plan2()
        self._apply_018()
        self._mark_done(session_id, 0)
        artifact = self._ready_artifact(session_id, 0)

        with self.assertRaisesRegex(Exception, "audited plan authority transition"):
            self._exec(
                """UPDATE remote_chunks SET plan_source='manifest',
                          plan_manifest_artifact_id=%s
                   WHERE session_id=%s AND chunk_index=0""",
                (artifact["artifact_id"], session_id),
            )
        self.assertEqual(self._query(
            """SELECT plan_source,plan_manifest_artifact_id FROM remote_chunks
               WHERE session_id=%s AND chunk_index=0""", (session_id,))[0],
            {"plan_source": "legacy_db", "plan_manifest_artifact_id": None})
        self.assertEqual(self._query(
            "SELECT count(*) AS n FROM remote_chunk_plan_source_transitions"
        )[0]["n"], 0)

        audit_id = self._transition_chunk(
            session_id, 0, artifact["artifact_id"])
        chunk = self._query(
            """SELECT plan_source,plan_manifest_artifact_id FROM remote_chunks
               WHERE session_id=%s AND chunk_index=0""", (session_id,))[0]
        audit = self._query(
            """SELECT * FROM remote_chunk_plan_source_transitions
               WHERE chunk_plan_transition_id=%s""", (audit_id,))[0]
        self.assertEqual(chunk["plan_source"], "manifest")
        self.assertEqual(chunk["plan_manifest_artifact_id"],
                         artifact["artifact_id"])
        self.assertEqual(
            (audit["from_plan_source"], audit["to_plan_source"],
             audit["plan4_gate_id"], audit["plan4_evidence_id"],
             audit["approval_identity"], audit["equivalence_confirmed"]),
            ("legacy_db", "manifest", "plan4-gate", "plan4-evidence",
             "operator@example.test", True),
        )
        self.assertEqual(
            self._transition_chunk(session_id, 0, artifact["artifact_id"]),
            audit_id,
        )
        self.assertEqual(self._query(
            "SELECT count(*) AS n FROM remote_chunk_plan_source_transitions"
        )[0]["n"], 1)

        with self.assertRaisesRegex(Exception, "immutable except audited"):
            self._exec(
                """UPDATE remote_chunks
                   SET plan_source='legacy_db',plan_manifest_artifact_id=NULL
                   WHERE session_id=%s AND chunk_index=0""", (session_id,))
        for statement in (
                "UPDATE remote_chunk_plan_source_transitions "
                "SET plan4_gate_id='changed' WHERE chunk_plan_transition_id=%s",
                "DELETE FROM remote_chunk_plan_source_transitions "
                "WHERE chunk_plan_transition_id=%s"):
            with self.subTest(statement=statement):
                with self.assertRaisesRegex(Exception, "append-only"):
                    self._exec(statement, (audit_id,))
        with self.assertRaisesRegex(Exception, "immutable"):
            self._exec(
                "UPDATE archive_artifacts SET local_locator='changed' "
                "WHERE artifact_id=%s", (artifact["artifact_id"],))

    def test_audited_transition_refuses_unready_nonterminal_owned_and_missing_gate(self):
        session_id = self._session(chunks=(0,))
        self._apply_plan2()
        self._apply_018()
        planned = self._ready_artifact(
            session_id, 0, version="planned-v1", readiness="planned")

        def assert_atomic_failure(pattern, artifact_id, **kwargs):
            before = self._query(
                """SELECT plan_source,plan_manifest_artifact_id
                   FROM remote_chunks WHERE session_id=%s AND chunk_index=0""",
                (session_id,))[0]
            audit_count = self._query(
                "SELECT count(*) AS n FROM remote_chunk_plan_source_transitions"
            )[0]["n"]
            with self.assertRaisesRegex(Exception, pattern):
                self._transition_chunk(
                    session_id, 0, artifact_id, **kwargs)
            self.assertEqual(self._query(
                """SELECT plan_source,plan_manifest_artifact_id
                   FROM remote_chunks WHERE session_id=%s AND chunk_index=0""",
                (session_id,))[0], before)
            self.assertEqual(self._query(
                "SELECT count(*) AS n FROM remote_chunk_plan_source_transitions"
            )[0]["n"], audit_count)

        assert_atomic_failure("terminal unowned", planned["artifact_id"])
        self._mark_done(session_id, 0)
        assert_atomic_failure("not the ready plan_manifest authority",
                              planned["artifact_id"])
        ready = self._ready_artifact(
            session_id, 0, version="ready-v2", readiness="ready")
        self._exec(
            "UPDATE remote_chunks SET owner_token='worker' "
            "WHERE session_id=%s AND chunk_index=0", (session_id,))
        assert_atomic_failure("terminal unowned", ready["artifact_id"])
        self._exec(
            "UPDATE remote_chunks SET owner_token=NULL "
            "WHERE session_id=%s AND chunk_index=0", (session_id,))
        assert_atomic_failure("gate, evidence, and approval IDs are required",
                              ready["artifact_id"], gate="")

    def test_shared_legacy_plan_refuses_authority_transition_atomically(self):
        session_id = self._session(label="SHARED_PLAN_ONE", chunks=(0,))
        other = self._session(label="SHARED_PLAN_TWO", tape="SHARED_TAPE_TWO",
                              chunks=())
        self._apply_plan2()
        self._apply_018()
        self._mark_done(session_id, 0)
        plan_id = self._query(
            "SELECT plan_id FROM remote_sessions WHERE session_id=%s",
            (session_id,))[0]["plan_id"]
        self._exec(
            "UPDATE remote_sessions SET plan_id=%s WHERE session_id=%s",
            (plan_id, other),
        )
        artifact = self._ready_artifact(session_id, 0)
        with self.assertRaisesRegex(Exception, "shared or missing legacy plan"):
            self._transition_chunk(session_id, 0, artifact["artifact_id"])
        self.assertEqual(self._query(
            """SELECT plan_source,plan_manifest_artifact_id FROM remote_chunks
               WHERE session_id=%s AND chunk_index=0""", (session_id,))[0],
            {"plan_source": "legacy_db", "plan_manifest_artifact_id": None})
        self.assertEqual(self._query(
            "SELECT count(*) AS n FROM remote_chunk_plan_source_transitions"
        )[0]["n"], 0)

    def test_transition_state_machine_boundary_uniqueness_and_same_boundary_rollback(self):
        session_id = self._session(chunks=(0,))
        self._apply_plan2()
        self._apply_018()
        first = self._insert_epoch(
            session_id, 1, "legacy_db", "manifest", 0, 1)

        with self.assertRaisesRegex(Exception, "illegal session plan transition"):
            self._exec(
                """UPDATE remote_session_plan_transitions
                   SET state='approved',scan_frontier_generation=1,
                       evidence_report_locator='reports/illegal.json',
                       approval_identity='operator',approved_at=now()
                   WHERE transition_id=%s""", (first,))
        with self.assertRaisesRegex(Exception, "next epoch"):
            self._insert_epoch(
                session_id, 3, "legacy_db", "manifest", 0, 1)

        self._approve_epoch(first, activate=True)
        with self.assertRaisesRegex(Exception, "evidence is immutable"):
            self._exec(
                """UPDATE remote_session_plan_transitions
                   SET evidence_report_locator='reports/changed.json'
                   WHERE transition_id=%s""", (first,))

        second = self._insert_epoch(
            session_id, 2, "manifest", "legacy_db", 0, 1)
        self._approve_epoch(second)
        with self.assertRaises(Exception):
            self._exec(
                "UPDATE remote_session_plan_transitions SET state='active' "
                "WHERE transition_id=%s", (second,))
        states = self._query(
            """SELECT transition_epoch,state FROM remote_session_plan_transitions
               WHERE session_id=%s ORDER BY transition_epoch""", (session_id,))
        self.assertEqual(
            [(row["transition_epoch"], row["state"]) for row in states],
            [(1, "active"), (2, "approved")],
        )

        self._exec(
            "UPDATE remote_session_plan_transitions SET state='rolled_back' "
            "WHERE transition_id=%s", (first,))
        self._exec(
            "UPDATE remote_session_plan_transitions SET state='active' "
            "WHERE transition_id=%s", (second,))
        self.assertEqual(self._query(
            "SELECT lto_effective_chunk_plan_source(%s,1) AS source",
            (session_id,))[0]["source"], "legacy_db")
        self.db.append_remote_streaming_chunk(
            session_id, 1,
            [(1, "/fixture/rollback/future.bin", "future.bin", 1)],
        )
        self.assertEqual(self._query(
            """SELECT chunk_index,plan_source FROM remote_chunks
               WHERE session_id=%s ORDER BY chunk_index""", (session_id,)),
            [{"chunk_index": 0, "plan_source": "legacy_db"},
             {"chunk_index": 1, "plan_source": "legacy_db"}],
        )

    def test_deferred_atomic_initial_manifest_chunk_and_future_only_rollback(self):
        session_id = self._session(chunks=(0,))
        self._apply_plan2()
        self._apply_018()
        first = self._insert_epoch(
            session_id, 1, "legacy_db", "manifest", 0, 1)
        self._approve_epoch(first, activate=True)

        artifact_id = self._insert_manifest_chunk_transaction(session_id, 1)
        chunk = self._query(
            """SELECT plan_source,plan_manifest_artifact_id
               FROM remote_chunks WHERE session_id=%s AND chunk_index=1""",
            (session_id,))[0]
        self.assertEqual(chunk,
                         {"plan_source": "manifest",
                          "plan_manifest_artifact_id": artifact_id})

        constraint = self._query(
            """SELECT condeferrable,condeferred
               FROM pg_constraint
               WHERE conrelid='remote_chunks'::regclass
                 AND conname='remote_chunks_plan_manifest_artifact_fk'""")[0]
        self.assertEqual(constraint,
                         {"condeferrable": True, "condeferred": True})
        old_fk = self._query(
            """SELECT condeferrable,condeferred
               FROM pg_constraint
               WHERE conrelid='archive_artifacts'::regclass
                 AND conname='archive_artifacts_chunk_fk'""")[0]
        self.assertEqual(old_fk,
                         {"condeferrable": False, "condeferred": False})

        for index, kwargs, pattern in (
                (2, {"include_artifact": False},
                 "violates foreign key constraint|not the ready"),
                (3, {"readiness": "planned"},
                 "not the ready plan_manifest authority"),
                (4, {"artifact_kind": "terminal_manifest"},
                 "not the ready plan_manifest authority")):
            with self.subTest(chunk_index=index):
                with self.assertRaisesRegex(Exception, pattern):
                    self._insert_manifest_chunk_transaction(
                        session_id, index, **kwargs)
                self.assertEqual(self._query(
                    """SELECT count(*) AS n FROM remote_chunks
                       WHERE session_id=%s AND chunk_index=%s""",
                    (session_id, index))[0]["n"], 0)
                self.assertEqual(self._query(
                    """SELECT count(*) AS n FROM archive_artifacts
                       WHERE session_id=%s AND chunk_index=%s""",
                    (session_id, index))[0]["n"], 0)

        reversal = self._insert_epoch(
            session_id, 2, "manifest", "legacy_db", 1, 2)
        self._approve_epoch(reversal, activate=True)
        self.db.append_remote_streaming_chunk(
            session_id, 2,
            [(2, "/fixture/rollback/later.bin", "later.bin", 2)],
        )
        self.assertEqual(self._query(
            """SELECT chunk_index,plan_source FROM remote_chunks
               WHERE session_id=%s ORDER BY chunk_index""", (session_id,)),
            [{"chunk_index": 0, "plan_source": "legacy_db"},
             {"chunk_index": 1, "plan_source": "manifest"},
             {"chunk_index": 2, "plan_source": "legacy_db"}],
        )

    # -- optional migrations and real Plan-2 upgrade ---------------------

    def test_optional_007_and_012_absent_and_fully_installed(self):
        self._apply_plan2()
        self._apply_018()
        report = self.db.manifest_directory_catalog_schema_report()
        self.assertEqual(report["optional_007_state"], "absent")
        self.assertEqual(report["optional_012_state"], "absent")
        self.assertTrue(report["ready"])

        from src.sealed_batch_repository import SealedBatchRepository

        db2, conninfo2 = self._additional_database("lto_018_optional_full")
        db2.apply_directory_catalog_schema()
        SealedBatchRepository(conninfo2).apply_schema()
        self._apply_plan2(db=db2)
        self._apply_018(db=db2)
        report2 = db2.manifest_directory_catalog_schema_report()
        self.assertEqual(report2["optional_007_state"], "installed")
        self.assertEqual(report2["optional_012_state"], "installed")
        self.assertTrue(report2["ready"])
        SealedBatchRepository(conninfo2).assert_schema_valid()

    def test_partial_optional_007_and_012_are_refused_without_partial_018(self):
        for tag, table_name, pattern in (
                ("lto_018_partial_007", "directory_archive_stats",
                 "migration 007 is partially installed"),
                ("lto_018_partial_012", "tape_write_batches",
                 "migration 012 is partially installed")):
            with self.subTest(table=table_name):
                db, conninfo = self._additional_database(tag)
                self._apply_plan2(db=db)
                self._exec(
                    f"CREATE TABLE {table_name}(dummy INTEGER)",
                    conninfo=conninfo)
                with self.assertRaisesRegex(Exception, pattern):
                    db.apply_manifest_directory_catalog_schema()
                self.assertIsNone(self._query(
                    """SELECT to_regclass(
                           'manifest_directory_catalog_schema_metadata') AS r""",
                    conninfo=conninfo)[0]["r"])
                self.assertEqual(self._query(
                    """SELECT count(*) AS n FROM information_schema.columns
                       WHERE table_schema='public'
                         AND table_name='remote_chunks'
                         AND column_name='plan_source'""",
                    conninfo=conninfo)[0]["n"], 0)

    def test_real_plan2_upgrade_preserves_all_prior_objects_constraints_and_rows(self):
        from src.sealed_batch_repository import SealedBatchRepository

        self.db.apply_directory_catalog_schema()
        SealedBatchRepository(self.conninfo).apply_schema()
        session_id = self._session(chunks=(0,))
        self._apply_plan2()
        self._exec(
            """INSERT INTO directory_archive_stats
                   (source_host,original_dir_path,tape_label,remote_session_id,
                    chunk_index,direct_file_count,direct_bytes,
                    recursive_file_count,recursive_bytes,small_file_count,
                    small_file_bytes,large_file_count,large_file_bytes,
                    packed_bundle_count,backup_date,record_key)
               VALUES ('fixture','/fixture/legacy/stats','MIGRATION_018_TAPE',
                       %s,0,0,0,0,0,0,0,0,0,0,now(),decode(repeat('11',32),'hex'))""",
            (session_id,))
        self._exec(
            """INSERT INTO directory_archive_bundles
                   (source_host,original_dir_path,tape_label,remote_session_id,
                    chunk_index,stored_bundle_path,file_count,byte_count,
                    small_file_count,small_file_bytes,large_file_count,
                    large_file_bytes,backup_date,record_key)
               VALUES ('fixture','/fixture/legacy/bundle','MIGRATION_018_TAPE',
                       %s,0,'legacy.zip',0,0,0,0,0,0,now(),
                       decode(repeat('22',32),'hex'))""", (session_id,))
        self._exec(
            """INSERT INTO directory_tree_index
                   (source_host,original_dir_path,dir_name,depth,tape_label,
                    remote_session_id,chunk_index,direct_file_count,direct_bytes,
                    recursive_file_count,recursive_bytes,
                    direct_small_file_count,direct_small_file_bytes,
                    recursive_small_file_count,recursive_small_file_bytes,
                    direct_large_file_count,direct_large_file_bytes,
                    recursive_large_file_count,recursive_large_file_bytes,
                    backup_date,record_key)
               VALUES ('fixture','/fixture/legacy/tree','tree',0,
                       'MIGRATION_018_TAPE',%s,0,0,0,0,0,0,0,0,0,0,0,0,0,
                       now(),decode(repeat('33',32),'hex'))""", (session_id,))
        before_objects = self._object_snapshot()
        row_tables = (
            "remote_sessions", "remote_chunks", "remote_plan_files",
            "archive_containers", "archive_artifacts",
            "archive_container_members", "directory_archive_stats",
            "directory_archive_bundles", "directory_tree_index",
            "tape_write_batches", "tape_write_batch_chunks",
            "tape_write_active_chunk")
        rows_before = {
            table: self._query(f"SELECT * FROM {table} ORDER BY 1")
            for table in row_tables
        }

        self._apply_018()
        after_objects = self._object_snapshot()
        self._assert_snapshot_preserved(self, before_objects, after_objects)
        for table, expected_rows in rows_before.items():
            actual_rows = self._query(f"SELECT * FROM {table} ORDER BY 1")
            # Migration 018 is additive: remote_chunks intentionally receives
            # new columns. Compare every pre-018 field and value without
            # pretending that additive columns violate row preservation.
            if expected_rows:
                prior_columns = tuple(expected_rows[0])
                actual_rows = [
                    {column: row[column] for column in prior_columns}
                    for row in actual_rows
                ]
            self.assertEqual(
                actual_rows, expected_rows, table)
        SealedBatchRepository(self.conninfo).assert_schema_valid()

        with self.assertRaisesRegex(Exception, "write-once"):
            self._exec(
                "UPDATE remote_chunks SET packaging_format='stored_tar' "
                "WHERE session_id=%s AND chunk_index=0", (session_id,))
        with self.assertRaises(errors.CheckViolation):
            self._exec(
                """INSERT INTO archive_artifacts
                       (session_id,chunk_index,artifact_kind,artifact_version,
                        readiness_state)
                   VALUES (%s,0,'plan_manifest','bad-ready','ready')""",
                (session_id,))
        plan_file_id = self._query(
            """SELECT pf.plan_file_id FROM remote_plan_files pf
               JOIN remote_sessions s ON s.plan_id=pf.plan_id
               WHERE s.session_id=%s AND pf.chunk_index=0""",
            (session_id,))[0]["plan_file_id"]
        with self.assertRaises(errors.CheckViolation):
            self._exec(
                """INSERT INTO archive_container_members
                       (session_id,chunk_index,plan_file_id,plan_ordinal,
                        storage_class,remote_path,expected_logical_bytes,
                        estimated_tar_bytes)
                   VALUES (%s,0,%s,0,'small_files','/fixture/member',1,1)""",
                (session_id, plan_file_id))
        container = self.db.create_archive_container({
            "session_id": session_id,
            "chunk_index": 0,
            "container_ordinal": 0,
            "container_format": "zip",
            "format_version": "zip-v1",
            "storage_class": "small_files",
            "container_name": "plan2-preserved.zip",
            "expected_member_count": 1,
            "expected_logical_bytes": 1,
        })
        with self.assertRaises(errors.CheckViolation):
            self._exec(
                """UPDATE archive_containers SET validation_state='ready'
                   WHERE container_id=%s""", (container["container_id"],))

    # -- normalized directory truth -------------------------------------

    def test_directory_catalog_uniqueness_exactly_one_completeness_and_view_routes(self):
        session_id = self._session(chunks=(0,))
        self._apply_plan2()
        self._apply_018()
        root = self._query(
            """INSERT INTO archive_directories
                   (source_host,canonical_path,name,depth)
               VALUES ('fixture','/fixture','fixture',0)
               RETURNING directory_id""")[0]["directory_id"]
        child = self._query(
            """INSERT INTO archive_directories
                   (source_host,canonical_path,parent_directory_id,name,depth)
               VALUES ('fixture','/fixture/child',%s,'child',1)
               RETURNING directory_id""", (root,))[0]["directory_id"]
        scope = self._query(
            """INSERT INTO remote_scan_scopes
                   (session_id,scope_ordinal,scope_kind,source_root,
                    coverage_state,planning_complete)
               VALUES (%s,0,'directory','/fixture','final',TRUE)
               RETURNING scan_scope_id""", (session_id,))[0]["scan_scope_id"]
        self._exec(
            """INSERT INTO directory_scan_coverage
                   (directory_id,session_id,scan_scope_id,coverage_state,
                    direct_discovered_file_count,direct_discovered_bytes,
                    recursive_discovered_file_count,recursive_discovered_bytes,
                    direct_discovered_directory_count,
                    recursive_discovered_directory_count,frontier_generation)
               VALUES (%s,%s,%s,'final',1,1,1,1,0,0,7)""",
            (child, session_id, scope),
        )
        container = self.db.create_archive_container({
            "session_id": session_id,
            "chunk_index": 0,
            "container_ordinal": 0,
            "container_format": "zip",
            "format_version": "zip-v1",
            "storage_class": "small_files",
            "container_name": "directory-part.zip",
            "expected_member_count": 1,
            "expected_logical_bytes": 1,
        })
        container_part = (
            """INSERT INTO directory_archive_parts
                   (directory_id,session_id,chunk_index,container_id,
                    storage_class,evidence_generation,direct_expected_count,
                    direct_expected_bytes,restore_format,source_base_path,
                    routing_precision)
               VALUES (%s,%s,0,%s,'container',1,1,1,'zip','/fixture','coarse')""")
        self._exec(container_part,
                   (root, session_id, container["container_id"]))
        with self.assertRaises(errors.UniqueViolation):
            self._exec(container_part,
                       (root, session_id, container["container_id"]))
        self._exec(container_part,
                   (child, session_id, container["container_id"]))

        loose_path = self._catalog_file(
            session_id, 0, original_path="/fixture/child/loose.bin")
        loose = self._query(
            """SELECT record_key,tape_label,stored_path FROM files_index
               WHERE original_path=%s""", (loose_path,))[0]
        generation_id = self._query(
            """SELECT generation_id FROM tape_generations
               WHERE volume_label='MIGRATION_018_TAPE' AND state='active'"""
        )[0]["generation_id"]
        loose_part = (
            """INSERT INTO directory_archive_parts
                   (directory_id,session_id,chunk_index,loose_record_key,
                    tape_generation_id,storage_class,evidence_generation,
                    direct_expected_count,direct_expected_bytes,
                    tape_label,stored_path,restore_format,source_base_path,
                    routing_precision)
               VALUES (%s,%s,0,%s,%s,'loose',1,1,1,%s,%s,'loose',
                       '/fixture/child','exact')""")
        loose_params = (
            child, session_id, loose["record_key"], generation_id,
            loose["tape_label"], loose["stored_path"])
        self._exec(loose_part, loose_params)
        with self.assertRaises(errors.UniqueViolation):
            self._exec(loose_part, loose_params)

        with self.assertRaises(Exception):
            self._exec(
                """INSERT INTO directory_archive_parts
                       (directory_id,session_id,chunk_index,storage_class,
                        restore_format,source_base_path,routing_precision)
                   VALUES (%s,%s,0,'loose','loose','/fixture','coarse')""",
                (root, session_id))
        with self.assertRaises(errors.CheckViolation):
            self._exec(
                """INSERT INTO directory_archive_parts
                       (directory_id,session_id,chunk_index,container_id,
                        loose_record_key,storage_class,restore_format,
                        source_base_path,routing_precision)
                   VALUES (%s,%s,0,%s,%s,'container','zip','/fixture','coarse')""",
                (root, session_id, container["container_id"],
                 loose["record_key"]))
        exactly_one = self._query(
            """SELECT pg_get_constraintdef(oid,true) AS definition
               FROM pg_constraint
               WHERE conrelid='directory_archive_parts'::regclass
                 AND conname='directory_archive_parts_exactly_one_identity_ck'"""
        )[0]["definition"]
        self.assertIn("num_nonnulls(container_id, loose_record_key) = 1",
                      exactly_one)

        self._exec(
            """INSERT INTO directory_completeness
                   (session_id,directory_id,direct_expected_file_count,
                    direct_expected_bytes,direct_archived_file_count,
                    direct_archived_bytes,recursive_expected_file_count,
                    recursive_expected_bytes,recursive_archived_file_count,
                    recursive_archived_bytes,status)
               VALUES (%s,%s,1,1,1,1,1,1,1,1,'provisional')""",
            (session_id, child),
        )
        with self.assertRaises(errors.CheckViolation):
            self._exec(
                """UPDATE directory_completeness SET status='complete'
                   WHERE session_id=%s AND directory_id=%s""",
                (session_id, child))
        self._exec(
            """UPDATE directory_completeness
               SET scan_is_final=TRUE,all_planned_items_terminal=TRUE,
                   all_required_items_archived=TRUE,all_parts_written=TRUE,
                   all_writer_completions_succeeded=TRUE,
                   all_parts_cataloged=TRUE,
                   all_local_validation_succeeded=TRUE,status='complete',
                   pinned_frontier_generation=7,
                   pinned_artifact_evidence_generation=1
               WHERE session_id=%s AND directory_id=%s""",
            (session_id, child),
        )

        view_rows = self.db.get_directory_catalog_status(
            session_id=session_id)
        child_loose = next(
            row for row in view_rows
            if row["directory_id"] == child
            and row["loose_record_key"] is not None)
        self.assertEqual(child_loose["parent_canonical_path"], "/fixture")
        self.assertEqual(child_loose["coverage_state"], "final")
        self.assertTrue(child_loose["coverage_is_final"])
        self.assertEqual(child_loose["completeness_status"], "complete")
        self.assertEqual(child_loose["restore_route"]["format"], "loose")
        self.assertEqual(child_loose["restore_route"]["tape_generation_id"],
                         generation_id)
        root_row = next(
            row for row in view_rows
            if row["directory_id"] == root and row["container_id"] is not None)
        self.assertEqual(root_row["child_count"], 1)
        self.assertEqual(
            root_row["child_directories"][0]["canonical_path"],
            "/fixture/child",
        )

    def test_startup_validation_fails_closed_on_manifest_authority_corruption(self):
        session_id = self._session(chunks=(0,))
        self._apply_plan2()
        self._apply_018()
        self._mark_done(session_id, 0)
        ready = self._ready_artifact(session_id, 0)
        self._transition_chunk(session_id, 0, ready["artifact_id"])
        planned = self._ready_artifact(
            session_id, 0, version="planned-corruption", readiness="planned")

        self._exec(
            "ALTER TABLE remote_chunks DISABLE TRIGGER "
            "trg_remote_chunks_plan_source_guard")
        self._exec(
            "ALTER TABLE remote_chunks DISABLE TRIGGER "
            "trg_remote_chunks_manifest_authority_deferred")
        try:
            self._exec(
                """UPDATE remote_chunks SET plan_manifest_artifact_id=%s
                   WHERE session_id=%s AND chunk_index=0""",
                (planned["artifact_id"], session_id),
            )
        finally:
            self._exec(
                "ALTER TABLE remote_chunks ENABLE TRIGGER "
                "trg_remote_chunks_manifest_authority_deferred")
            self._exec(
                "ALTER TABLE remote_chunks ENABLE TRIGGER "
                "trg_remote_chunks_plan_source_guard")

        report = self.db.manifest_directory_catalog_schema_report()
        self.assertFalse(report["ready"])
        self.assertEqual(report["invalid_manifest_chunk_count"], 1)
        self.assertGreaterEqual(
            report["invalid_chunk_plan_source_transition_count"], 1)
        with self.assertRaisesRegex(RuntimeError, "missing or inconsistent"):
            self.db.validate_manifest_directory_catalog_schema()


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
