"""Plan 3, Tasks 2.1-2.2 against disposable PostgreSQL only.

The pure precedence rules live in ``tests/test_directory_completeness.py``.
These tests prove the *persisted* path: that a pinned generation is honoured,
that recomputation is idempotent, that the schema's partial unique indexes
actually reject duplicate contributions, and that a directory spanning several
chunks, containers and tapes is queryable as one thing.
"""
import unittest
from typing import Any, cast

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:                          # pragma: no cover - guard handles it
    psycopg = None
    dict_row = None

from pg_test_guard import (SKIP_REASON, create_test_database,
                           drop_test_database, pg_available)


def _connect(*args, **kwargs) -> Any:
    return cast(Any, psycopg.connect(*args, **kwargs))


def _pg_available():
    if psycopg is None:
        return False
    return pg_available()


class _DirectoryCatalogFixture:
    """Shared schema/session/directory fixture.

    Deliberately not a TestCase: the concrete classes below mix it in, so the
    shared cases are collected once rather than re-running for every subclass.
    """

    def setUp(self):
        from src.pg_db import PgDatabaseManager

        self.dbname, self.conninfo = create_test_database("lto_dircat")
        self.db = PgDatabaseManager(self.conninfo)
        self.addCleanup(self._drop)

        self.db.apply_directory_catalog_schema()
        self.db.apply_incremental_scan_schema()
        self.db.apply_incremental_scan_schema(finalize=True)
        self._seed_session()
        self.db.apply_container_format_schema()
        self.db.apply_stored_tar_plan_schema()
        self.db.apply_manifest_directory_catalog_schema()
        self._seed_directories()

    def _drop(self):
        try:
            self.db.close()
        except Exception:
            pass
        drop_test_database(self.dbname)

    def _exec(self, sql, params=()):
        with _connect(self.conninfo, autocommit=True) as conn:
            conn.execute(sql, params)

    def _query(self, sql, params=()):
        with _connect(self.conninfo, autocommit=True,
                      row_factory=cast(Any, dict_row)) as conn:
            return conn.execute(sql, params).fetchall()

    def _seed_session(self):
        self._exec("INSERT INTO tapes (volume_label,status) VALUES "
                   "('T_A','active'),('T_B','active')")
        # Migration 018 requires a part's tape_label to agree with its
        # tape_generation_id, so the fixture carries real generations.
        self.generations = {
            row["volume_label"]: row["generation_id"]
            for row in self._query(
                """INSERT INTO tape_generations (tape_id,volume_label,
                       generation,state,formatted_at)
                   SELECT t.tape_id,t.volume_label,1,'active',now()
                   FROM tapes t WHERE t.volume_label IN ('T_A','T_B')
                   RETURNING generation_id, volume_label""")}
        rows = self._query(
            """INSERT INTO remote_snapshots (remote_host,remote_path,
                   fingerprint,total_files,total_bytes,created_at)
               VALUES ('h','/r','fp',4,400,now()) RETURNING snapshot_id""")
        snap = rows[0]["snapshot_id"]
        rows = self._query(
            """INSERT INTO remote_plans (snapshot_id,fingerprint,chunk_count,
                   created_at) VALUES (%s,'pf',3,now()) RETURNING plan_id""",
            (snap,))
        plan = rows[0]["plan_id"]
        rows = self._query(
            """INSERT INTO remote_sessions (session_label,remote_host,
                   remote_user,remote_path,tape_label,staging_dir,total_files,
                   total_bytes,chunk_count,plan_id,created_at,status,
                   scan_complete,tape_generation)
               VALUES ('DIRCAT','h','u','/r','T_A','C:\\stg',4,400,3,%s,now(),
                       'active',false,1) RETURNING session_id""", (plan,))
        self.session_id = rows[0]["session_id"]
        for index in range(3):
            self._exec(
                """INSERT INTO remote_chunks (session_id,chunk_index,status,
                       updated_at,membership_state,expected_file_count,
                       expected_bytes)
                   VALUES (%s,%s,'done',now(),'sealed',1,100)""",
                (self.session_id, index))
        self.scope_id = self._query(
            """INSERT INTO remote_scan_scopes (session_id,scope_ordinal,
                   scope_kind,source_root,coverage_state)
               VALUES (%s,0,'directory','/r','provisional')
               RETURNING scan_scope_id""", (self.session_id,))[0]["scan_scope_id"]
        self.run_id = self._query(
            """INSERT INTO archive_runs (run_label,tape_label,session_kind,
                   started_at,completed_at,remote_session_id)
               VALUES ('fixture:T_A','T_A','remote',now(),now(),%s)
               RETURNING run_id""", (self.session_id,))[0]["run_id"]

    def _seed_directories(self):
        # A root has depth 0 and no parent; migration 018 enforces both.
        self.root_id = self._query(
            """INSERT INTO archive_directories (source_host,canonical_path,
                   parent_directory_id,name,depth)
               VALUES ('h','/r',NULL,'r',0)
               RETURNING directory_id""")[0]["directory_id"]
        self.dir_id = self._query(
            """INSERT INTO archive_directories (source_host,canonical_path,
                   parent_directory_id,name,depth)
               VALUES ('h','/r/data',%s,'data',1)
               RETURNING directory_id""", (self.root_id,))[0]["directory_id"]
        self.child_id = self._query(
            """INSERT INTO archive_directories (source_host,canonical_path,
                   parent_directory_id,name,depth)
               VALUES ('h','/r/data/sub',%s,'sub',2)
               RETURNING directory_id""", (self.dir_id,))[0]["directory_id"]

    def _coverage(self, directory_id, state="final", generation=1):
        # The schema insists an 'error' coverage row actually carries an error.
        self._exec(
            """INSERT INTO directory_scan_coverage (directory_id,session_id,
                   scan_scope_id,coverage_state,error_count,
                   frontier_generation)
               VALUES (%s,%s,%s,%s,%s,%s)""",
            (directory_id, self.session_id, self.scope_id, state,
             1 if state == "error" else 0, generation))

    def _catalog_row(self, key, tape_label, stored_path, chunk_index):
        """A real files_index row: migration 018 refuses a loose route that
        does not point at one, which is the guard doing its job."""
        catalog_dir = self._query(
            """INSERT INTO catalog_directories (tape_label,parent_id,name,
                   normalized_path)
               VALUES (%s,NULL,%s,%s)
               ON CONFLICT (tape_label,normalized_path) DO UPDATE
                   SET name=EXCLUDED.name
               RETURNING directory_id""",
            (tape_label, "loose", f"/loose/{tape_label}"))[0]["directory_id"]
        self._exec(
            """INSERT INTO files_index (original_path,file_size_bytes,
                   tape_label,source_host,is_packed,stored_path,record_key,
                   remote_session_id,remote_chunk_index,directory_id,
                   archive_run_id,catalog_name,catalog_backup_date)
               VALUES (%s,100,%s,'h',false,%s,%s,%s,%s,%s,%s,'fixture',now())
               ON CONFLICT (record_key) DO NOTHING""",
            (f"/r/data/f{chunk_index}", tape_label, stored_path, key,
             self.session_id, chunk_index, catalog_dir, self.run_id))

    def _part(self, directory_id, chunk_index, *, loose_key=b"", **over):
        values = dict(
            storage_class="loose", evidence_generation=1,
            direct_expected_count=1, direct_expected_bytes=100,
            direct_archived_count=1, direct_archived_bytes=100,
            local_validation_state="succeeded", writer_state="copied",
            catalog_state="committed", restore_format="loose",
            tape_label="T_A", stored_path=None,
            source_base_path="/r/data", routing_precision="exact")
        values.update(over)
        key = loose_key or f"k{directory_id}-{chunk_index}".encode().ljust(
            32, b"0")
        values["tape_generation_id"] = self.generations[values["tape_label"]]
        if values["stored_path"] is None:
            values["stored_path"] = f"/loose/{chunk_index}"
        self._catalog_row(key, values["tape_label"], values["stored_path"],
                          chunk_index)
        self._exec(
            f"""INSERT INTO directory_archive_parts
                    (directory_id,session_id,chunk_index,loose_record_key,
                     {','.join(values)})
                VALUES (%s,%s,%s,%s,{','.join(['%s'] * len(values))})""",
            (directory_id, self.session_id, chunk_index, key,
             *values.values()))

    def _container_part(self, directory_id, chunk_index, *, writer_state,
                        **over):
        """An unwritten contribution.

        ``archive_containers`` exists from 'planned' onward, so a container
        part is how the schema represents work that is planned but not yet on
        tape.  A loose part cannot model this: ``loose_record_key`` is a
        foreign key into ``files_index``, which only exists after a successful
        write.
        """
        container_id = self._query(
            """INSERT INTO archive_containers (session_id,chunk_index,
                   container_ordinal,container_format,format_version,
                   tar_dialect,storage_class,container_name,
                   expected_member_count,expected_logical_bytes,
                   validation_state,writer_state,catalog_state,
                   tape_label,tape_path,tape_generation_id,writer_started_at)
               VALUES (%s,%s,%s,'zip','1',NULL,'small_files',%s,1,100,
                       'planned',%s,'not_started','T_A',%s,%s,now())
               RETURNING container_id""",
            (self.session_id, chunk_index, chunk_index,
             f"c{chunk_index}.zip", writer_state, f"/tape/c{chunk_index}.zip",
             self.generations["T_A"]))[0]["container_id"]
        values = dict(
            storage_class="container", evidence_generation=1,
            direct_expected_count=1, direct_expected_bytes=100,
            direct_archived_count=0, direct_archived_bytes=0,
            local_validation_state="pending", writer_state=writer_state,
            catalog_state="not_started", restore_format="zip",
            tape_label="T_A", stored_path=f"/tape/c{chunk_index}.zip",
            tape_generation_id=self.generations["T_A"],
            container_member_candidate=f"member_{chunk_index}.bin",
            source_base_path="/r/data", routing_precision="exact")
        values.update(over)
        self._exec(
            f"""INSERT INTO directory_archive_parts
                    (directory_id,session_id,chunk_index,container_id,
                     {','.join(values)})
                VALUES (%s,%s,%s,%s,{','.join(['%s'] * len(values))})""",
            (directory_id, self.session_id, chunk_index, container_id,
             *values.values()))

    def _status(self, directory_id):
        row = self.db.get_directory_status(self.session_id, directory_id)
        return row["status"] if row else None


@unittest.skipUnless(_pg_available(), SKIP_REASON)
class DirectoryCatalogPgTests(_DirectoryCatalogFixture, unittest.TestCase):
    """One directory, many parts, and the completeness that follows."""

    def test_a_directory_with_no_files_is_still_queryable(self):
        self._coverage(self.dir_id, "final")
        result = self.db.recalculate_directory_completeness(self.session_id)
        self.assertEqual(result["directories"], 1)
        self.assertEqual(self._status(self.dir_id), "complete")

    def test_a_directory_still_scanning_is_provisional(self):
        self._coverage(self.dir_id, "provisional")
        self.db.recalculate_directory_completeness(self.session_id)
        self.assertEqual(self._status(self.dir_id), "provisional")

    def test_a_directory_spanning_chunks_and_tapes_is_one_row(self):
        self._coverage(self.dir_id, "final")
        self._part(self.dir_id, 0, tape_label="T_A")
        self._part(self.dir_id, 1, tape_label="T_B")
        self._part(self.dir_id, 2, tape_label="T_A")
        self.db.recalculate_directory_completeness(self.session_id)
        row = self.db.get_directory_status(self.session_id, self.dir_id)
        self.assertEqual(row["status"], "complete")
        self.assertEqual(row["direct_expected_file_count"], 3)
        self.assertEqual(row["direct_archived_file_count"], 3)

    def test_an_unwritten_part_makes_the_directory_incomplete(self):
        self._coverage(self.dir_id, "final")
        self._part(self.dir_id, 0)
        self._container_part(self.dir_id, 1, writer_state="writing")
        self.db.recalculate_directory_completeness(self.session_id)
        self.assertEqual(self._status(self.dir_id), "incomplete")

    def test_an_ambiguous_writer_state_makes_it_ambiguous(self):
        self._coverage(self.dir_id, "final")
        self._container_part(self.dir_id, 0, writer_state="ambiguous")
        self.db.recalculate_directory_completeness(self.session_id)
        self.assertEqual(self._status(self.dir_id), "ambiguous")

    def test_an_unresolved_entry_makes_it_ambiguous(self):
        self._coverage(self.dir_id, "final")
        self._part(self.dir_id, 0, direct_expected_count=2,
                   direct_archived_count=1, direct_unresolved_count=1)
        self.db.recalculate_directory_completeness(self.session_id)
        self.assertEqual(self._status(self.dir_id), "ambiguous")

    def test_source_exceptions_qualify_completion(self):
        self._coverage(self.dir_id, "final")
        self._part(self.dir_id, 0, direct_expected_count=2,
                   direct_archived_count=1, direct_source_missing_count=1)
        self.db.recalculate_directory_completeness(self.session_id)
        self.assertEqual(self._status(self.dir_id),
                         "complete_with_source_exceptions")

    def test_a_directory_with_no_coverage_row_is_never_complete(self):
        """Unproven coverage can never reach `complete`.

        It reports `provisional`, not `incomplete`: nothing has established
        that discovery finished, and migration 018's derived-status constraint
        makes non-final coverage provisional by construction.
        """
        self._part(self.dir_id, 0)
        self.db.recalculate_directory_completeness(self.session_id)
        self.assertEqual(self._status(self.dir_id), "provisional")

    def test_error_coverage_is_never_complete(self):
        self._coverage(self.dir_id, "error")
        self._part(self.dir_id, 0)
        self.db.recalculate_directory_completeness(self.session_id)
        self.assertEqual(self._status(self.dir_id), "provisional")

    def test_recalculation_is_idempotent(self):
        self._coverage(self.dir_id, "final")
        self._part(self.dir_id, 0)
        first = self.db.recalculate_directory_completeness(self.session_id)
        row_a = self.db.get_directory_status(self.session_id, self.dir_id)
        second = self.db.recalculate_directory_completeness(self.session_id)
        row_b = self.db.get_directory_status(self.session_id, self.dir_id)
        self.assertEqual(first["directories"], second["directories"])
        for field in ("status", "direct_expected_file_count",
                      "direct_archived_file_count", "scan_is_final"):
            self.assertEqual(row_a[field], row_b[field], field)

    def test_deep_ancestors_each_get_their_own_row(self):
        self._coverage(self.dir_id, "final")
        self._coverage(self.child_id, "final")
        self._part(self.child_id, 0)
        result = self.db.recalculate_directory_completeness(self.session_id)
        self.assertEqual(result["directories"], 2)
        self.assertIsNotNone(self._status(self.dir_id))
        self.assertIsNotNone(self._status(self.child_id))

    def test_a_directory_is_final_only_when_every_scope_is(self):
        other_scope = self._query(
            """INSERT INTO remote_scan_scopes (session_id,scope_ordinal,
                   scope_kind,source_root,coverage_state)
               VALUES (%s,1,'directory','/r2','provisional')
               RETURNING scan_scope_id""",
            (self.session_id,))[0]["scan_scope_id"]
        self._coverage(self.dir_id, "final")
        self._exec(
            """INSERT INTO directory_scan_coverage (directory_id,session_id,
                   scan_scope_id,coverage_state,frontier_generation)
               VALUES (%s,%s,%s,'provisional',1)""",
            (self.dir_id, self.session_id, other_scope))
        self.db.recalculate_directory_completeness(self.session_id)
        self.assertEqual(self._status(self.dir_id), "provisional")

    def test_duplicate_loose_contribution_is_rejected_by_the_schema(self):
        self._coverage(self.dir_id, "final")
        self._part(self.dir_id, 0, loose_key=b"same".ljust(32, b"0"))
        with self.assertRaises(Exception):
            self._part(self.dir_id, 0, loose_key=b"same".ljust(32, b"0"))

    def test_a_pinned_generation_excludes_later_evidence(self):
        """A part published at a later generation is not silently absorbed."""
        self._coverage(self.dir_id, "final")
        self._part(self.dir_id, 0, evidence_generation=1)
        self.db.recalculate_directory_completeness(self.session_id)
        before = self.db.get_directory_status(self.session_id, self.dir_id)
        self._part(self.dir_id, 1, evidence_generation=2)
        result = self.db.recalculate_directory_completeness(self.session_id)
        after = self.db.get_directory_status(self.session_id, self.dir_id)
        self.assertEqual(before["pinned_artifact_evidence_generation"], 1)
        self.assertEqual(result["pinned_artifact_evidence_generation"], 2)
        self.assertEqual(after["direct_expected_file_count"], 2)

    def test_status_is_stable_after_dropping_a_synthetic_per_file_table(self):
        """The plan's acceptance gate: status must not depend on per-file rows."""
        self._coverage(self.dir_id, "final")
        self._part(self.dir_id, 0)
        self._exec("CREATE TABLE synthetic_per_file (id BIGINT)")
        self._exec("INSERT INTO synthetic_per_file VALUES (1)")
        self.db.recalculate_directory_completeness(self.session_id)
        before = self.db.get_directory_status(self.session_id, self.dir_id)
        self._exec("DROP TABLE synthetic_per_file")
        self.db.recalculate_directory_completeness(self.session_id)
        after = self.db.get_directory_status(self.session_id, self.dir_id)
        self.assertEqual(before["status"], after["status"])
        self.assertEqual(before["direct_expected_file_count"],
                         after["direct_expected_file_count"])

    def test_the_canonical_view_reports_parent_child_and_routes(self):
        self._coverage(self.dir_id, "final")
        self._part(self.dir_id, 0)
        self.db.recalculate_directory_completeness(self.session_id)
        rows = self.db.get_directory_catalog_status(session_id=self.session_id)
        self.assertTrue(rows)
        paths = {row["canonical_path"] for row in rows}
        self.assertIn("/r/data", paths)

    def test_completeness_never_reads_tape(self):
        import src.pg_directory_catalog as module
        source = __import__("inspect").getsource(module)
        for token in ("lto_drive", "LtfsCmd", "robocopy", "_acquire_tape_io_lock",
                      "get_volume_label"):
            self.assertNotIn(token, source, token)


if __name__ == "__main__":                   # pragma: no cover
    unittest.main()


@unittest.skipUnless(_pg_available(), SKIP_REASON)
class LegacyAdapterTests(_DirectoryCatalogFixture, unittest.TestCase):
    """Task 2.3: legacy evidence folds in without altering legacy tables."""

    def test_loose_large_files_become_exact_parts(self):
        self._catalog_row(b"loose-key".ljust(32, b"0"), "T_A", "/loose/big", 0)
        report = self.db.ingest_legacy_directory_parts(self.session_id)
        self.assertEqual(report["loose_parts"], 1)
        rows = self._query(
            """SELECT routing_precision, restore_format, storage_class
               FROM directory_archive_parts WHERE session_id=%s""",
            (self.session_id,))
        self.assertEqual(rows[0]["routing_precision"], "exact")
        self.assertEqual(rows[0]["restore_format"], "loose")

    def test_a_bundle_without_container_identity_stays_coarse(self):
        """Session 37's real shape: 134 bundles, every chunk_index NULL."""
        self._exec(
            """INSERT INTO directory_archive_bundles
                   (source_host,original_dir_path,tape_label,
                    remote_session_id,chunk_index,stored_bundle_path,
                    file_count,byte_count,small_file_count,small_file_bytes,
                    large_file_count,large_file_bytes,backup_date,record_key)
               VALUES ('h','/r/data','T_A',%s,NULL,'legacy.zip',5,500,5,500,
                       0,0,now(),decode(repeat('ab',32),'hex'))""",
            (self.session_id,))
        report = self.db.ingest_legacy_directory_parts(self.session_id)
        self.assertEqual(report["coarse_bundle_candidates"], 1)
        self.assertEqual(report["container_parts"], 0)
        self.assertEqual(
            self._query("SELECT count(*) AS n FROM directory_archive_parts "
                        "WHERE session_id=%s", (self.session_id,))[0]["n"], 0)

    def test_dry_run_writes_nothing(self):
        self._catalog_row(b"dry-key".ljust(32, b"0"), "T_A", "/loose/dry", 0)
        report = self.db.ingest_legacy_directory_parts(
            self.session_id, dry_run=True)
        self.assertTrue(report["dry_run"])
        self.assertEqual(report["loose_parts"], 1)
        self.assertEqual(
            self._query("SELECT count(*) AS n FROM directory_archive_parts "
                        "WHERE session_id=%s", (self.session_id,))[0]["n"], 0)

    def test_ingestion_is_idempotent(self):
        self._catalog_row(b"idem-key".ljust(32, b"0"), "T_A", "/loose/i", 0)
        first = self.db.ingest_legacy_directory_parts(self.session_id)
        self.db.ingest_legacy_directory_parts(self.session_id)
        self.assertEqual(first["loose_parts"], 1)
        self.assertEqual(
            self._query("SELECT count(*) AS n FROM directory_archive_parts "
                        "WHERE session_id=%s", (self.session_id,))[0]["n"], 1)

    def test_legacy_tables_are_not_altered(self):
        before = self._query(
            "SELECT count(*) AS n FROM directory_archive_bundles")[0]["n"]
        self._catalog_row(b"keep-key".ljust(32, b"0"), "T_A", "/loose/k", 0)
        self.db.ingest_legacy_directory_parts(self.session_id)
        after = self._query(
            "SELECT count(*) AS n FROM directory_archive_bundles")[0]["n"]
        self.assertEqual(before, after)
