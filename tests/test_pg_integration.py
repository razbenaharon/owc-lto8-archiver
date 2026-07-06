"""Live-PostgreSQL integration tests for the archive catalog.

These exercise the real query paths that pure unit tests cannot: ILIKE escaping
against a real planner (§1.1), backslash-safe remote manifests (§1.2), batched
directory-chain resolution (§2.1), and RETURNING-based upsert stats (§2.4).

They run against a throwaway database created on the configured server and are
skipped automatically when no server is reachable (e.g. CI without Docker), so
they never touch the operator's live ``lto_archive`` catalog. Point them at a
server with the standard PG* environment variables (PGHOST/PGPORT/PGUSER/
PGPASSWORD); the local ``docker compose up -d db`` default works out of the box
with ``PGPASSWORD=change_me_local``.
"""
import unittest
import uuid
from pathlib import Path

try:
    import psycopg
    from psycopg import errors
    from psycopg.rows import dict_row
except ImportError:  # pragma: no cover - skipped when psycopg is absent
    psycopg = None
    errors = None
    dict_row = None

from src.pg_bulk import build_conninfo


def _pg_available():
    if psycopg is None:
        return False
    try:
        with psycopg.connect(build_conninfo(dbname="postgres"),
                             connect_timeout=3):
            return True
    except Exception:
        return False


@unittest.skipUnless(_pg_available(), "PostgreSQL server not reachable")
class PgIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from src.pg_db import PgDatabaseManager

        cls.dbname = f"lto_test_{uuid.uuid4().hex[:12]}"
        with psycopg.connect(build_conninfo(dbname="postgres"),
                             autocommit=True) as conn:
            conn.execute(f'CREATE DATABASE "{cls.dbname}"')
        cls.conninfo = build_conninfo(dbname=cls.dbname)
        cls.db = PgDatabaseManager(cls.conninfo)

    @classmethod
    def tearDownClass(cls):
        try:
            cls.db.close()
        except Exception:
            pass
        with psycopg.connect(build_conninfo(dbname="postgres"),
                             autocommit=True) as conn:
            conn.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = %s AND pid <> pg_backend_pid()",
                (cls.dbname,))
            conn.execute(f'DROP DATABASE IF EXISTS "{cls.dbname}"')

    # -- helpers -------------------------------------------------------------

    def _query(self, sql, params=()):
        with psycopg.connect(self.conninfo, autocommit=True,
                             row_factory=dict_row) as conn:
            return conn.execute(sql, params).fetchall()

    def _exec(self, sql, params=()):
        with psycopg.connect(self.conninfo, autocommit=True) as conn:
            conn.execute(sql, params)

    @staticmethod
    def _loose(original_path, tape, size=10, host="so02"):
        return {
            "original_path": original_path,
            "file_size_bytes": size,
            "tape_label": tape,
            "source_host": host,
            "is_packed": False,
            "container_name": None,
            "stored_path": original_path,
        }

    # -- §1.1 ILIKE escaping -------------------------------------------------

    def test_search_treats_underscore_as_literal(self):
        self.db.register_tape("TA")
        self.db.bulk_upsert_files([
            self._loose("/srv/data/report_2024.txt", "TA"),
            self._loose("/srv/data/reportX2024.txt", "TA"),
        ])
        hits = self.db.search_catalog(name_query="report_2024", tape_label="TA")
        self.assertEqual([h["file_name"] for h in hits], ["report_2024.txt"])
        self.assertEqual(self.db.count_search_files("report_2024"), 1)

    def test_wildcards_translate_to_ilike(self):
        self.db.register_tape("TB")
        self.db.bulk_upsert_files([
            self._loose("/b/clip_alpha.mov", "TB"),
            self._loose("/b/clip_beta.mov", "TB"),
            self._loose("/b/notes_alpha.txt", "TB"),
        ])
        movs = self.db.search_catalog(name_query="*.mov", tape_label="TB")
        self.assertEqual(len(movs), 2)
        clips = self.db.search_catalog(name_query="clip_*", tape_label="TB")
        self.assertEqual(
            sorted(m["file_name"] for m in clips),
            ["clip_alpha.mov", "clip_beta.mov"])

    def test_search_by_directory_literal_underscore(self):
        self.db.register_tape("TF")
        self.db.bulk_upsert_files([
            self._loose("/mnt/data_2024/a.txt", "TF"),
            self._loose("/mnt/dataX2024/b.txt", "TF"),
        ])
        hits = self.db.search_by_directory("/mnt/data_2024")
        self.assertEqual([h["file_name"] for h in hits], ["a.txt"])
        self.assertEqual(self.db.count_by_directory("/mnt/data_2024"), 1)

    # -- §2.1 batched directory chain ---------------------------------------

    def test_directory_chain_is_built_and_linked(self):
        self.db.register_tape("TC")
        self.db.bulk_upsert_files([
            self._loose("/srv/proj/sub/f1.txt", "TC"),
            self._loose("/srv/proj/sub/f2.txt", "TC"),
            self._loose("/srv/proj/other/g.txt", "TC"),
        ])
        rows = self._query(
            "SELECT directory_id, parent_id, name, normalized_path "
            "FROM catalog_directories WHERE tape_label = %s", ("TC",))
        by_path = {r["normalized_path"]: r for r in rows}
        id_to_path = {r["directory_id"]: r["normalized_path"] for r in rows}
        self.assertEqual(set(by_path), {
            "so02", "so02/srv", "so02/srv/proj",
            "so02/srv/proj/sub", "so02/srv/proj/other",
        })
        self.assertIsNone(by_path["so02"]["parent_id"])
        self.assertEqual(
            id_to_path[by_path["so02/srv"]["parent_id"]], "so02")
        self.assertEqual(
            id_to_path[by_path["so02/srv/proj/sub"]["parent_id"]],
            "so02/srv/proj")
        # Files land in their own leaf directory, and siblings share a parent.
        f1 = self._query(
            "SELECT directory_id FROM files_index WHERE original_path = %s",
            ("/srv/proj/sub/f1.txt",))[0]
        self.assertEqual(id_to_path[f1["directory_id"]], "so02/srv/proj/sub")

    def test_multi_tape_batch_keeps_directories_isolated(self):
        self.db.register_tape("TC1")
        self.db.register_tape("TC2")
        self.db.bulk_upsert_files([
            self._loose("/shared/dir/x.txt", "TC1"),
            self._loose("/shared/dir/y.txt", "TC2"),
        ])
        for tape in ("TC1", "TC2"):
            paths = {r["normalized_path"] for r in self._query(
                "SELECT normalized_path FROM catalog_directories "
                "WHERE tape_label = %s", (tape,))}
            self.assertEqual(paths, {"so02", "so02/shared", "so02/shared/dir"})

    def test_root_fallback_for_bare_name(self):
        self.db.register_tape("TR")
        self.db.bulk_upsert_files([self._loose("standalone.dat", "TR")])
        rows = self._query(
            "SELECT normalized_path FROM catalog_directories "
            "WHERE tape_label = %s", ("TR",))
        self.assertEqual({r["normalized_path"] for r in rows}, {"ROOT"})
        frow = self._query(
            "SELECT d.normalized_path FROM files_index f "
            "JOIN catalog_directories d ON d.directory_id = f.directory_id "
            "WHERE f.original_path = %s", ("standalone.dat",))[0]
        self.assertEqual(frow["normalized_path"], "ROOT")

    def test_windows_drive_path_uses_local_root(self):
        self.db.register_tape("TW")
        self.db.bulk_upsert_files([
            self._loose(r"C:\Users\me\clip.mov", "TW", host="local"),
        ])
        paths = {r["normalized_path"] for r in self._query(
            "SELECT normalized_path FROM catalog_directories "
            "WHERE tape_label = %s", ("TW",))}
        self.assertEqual(paths, {"LOCAL", "LOCAL/Users", "LOCAL/Users/me"})

    # -- §2.4 upsert stats via RETURNING ------------------------------------

    def test_upsert_stats_insert_update_skip(self):
        self.db.register_tape("TD")
        recs = [self._loose(f"/d/file{i}.bin", "TD") for i in range(5)]
        first = self.db.bulk_upsert_files(recs)
        self.assertEqual(
            (first["inserted"], first["updated"], first["skipped"]), (5, 0, 0))
        second = self.db.bulk_upsert_files(recs, update_existing=True)
        self.assertEqual(
            (second["inserted"], second["updated"], second["skipped"]),
            (0, 5, 0))
        third = self.db.bulk_upsert_files(recs, update_existing=False)
        self.assertEqual(
            (third["inserted"], third["updated"], third["skipped"]), (0, 0, 5))

    def test_upsert_mixed_insert_and_update_counts(self):
        self.db.register_tape("TD2")
        self.db.bulk_upsert_files([self._loose("/d2/a.bin", "TD2")])
        stats = self.db.bulk_upsert_files([
            self._loose("/d2/a.bin", "TD2"),   # existing -> update
            self._loose("/d2/b.bin", "TD2"),   # new -> insert
        ], update_existing=True)
        self.assertEqual(stats["inserted"], 1)
        self.assertEqual(stats["updated"], 1)
        self.assertEqual(stats["skipped"], 0)

    # -- §2.3 typed, foreign-keyed archive_runs.session refs -----------------

    def test_archive_runs_columns_are_typed(self):
        cols = {r["column_name"] for r in self._query(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'archive_runs'")}
        self.assertIn("local_session_id", cols)
        self.assertIn("remote_session_id", cols)
        # The legacy polymorphic column must be gone after migration 004.
        self.assertNotIn("session_id", cols)

    def test_local_run_links_local_session_and_remote_run_has_none(self):
        self.db.register_tape("TSL")
        self.db.register_tape("TSR")
        session_id = self.db.create_local_session(
            "L1", "/src", [[{"name": "top", "size_bytes": 10}]], "pack")
        self.db.bulk_upsert_files([{
            "original_path": "/src/top/f.txt", "file_size_bytes": 10,
            "tape_label": "TSL", "source_host": "so02", "is_packed": False,
            "container_name": None, "stored_path": "/src/top/f.txt",
            "local_session_id": session_id, "local_chunk_index": 0,
        }])
        self.db.bulk_upsert_files([self._loose("/data/r.txt", "TSR")])

        local_run = self._query(
            "SELECT ar.session_kind, ar.local_session_id, ar.remote_session_id "
            "FROM files_index f JOIN archive_runs ar ON ar.run_id = f.archive_run_id "
            "WHERE f.original_path = %s", ("/src/top/f.txt",))[0]
        self.assertEqual(local_run["session_kind"], "local")
        self.assertEqual(local_run["local_session_id"], session_id)
        self.assertIsNone(local_run["remote_session_id"])

        remote_run = self._query(
            "SELECT ar.session_kind, ar.local_session_id, ar.remote_session_id "
            "FROM files_index f JOIN archive_runs ar ON ar.run_id = f.archive_run_id "
            "WHERE f.original_path = %s", ("/data/r.txt",))[0]
        self.assertEqual(remote_run["session_kind"], "remote")
        self.assertIsNone(remote_run["local_session_id"])
        self.assertIsNone(remote_run["remote_session_id"])

    def test_archive_runs_fk_rejects_unknown_local_session(self):
        self.db.register_tape("TXFK")
        with self.assertRaises(errors.ForeignKeyViolation):
            self._exec(
                "INSERT INTO archive_runs "
                "(run_label, tape_label, session_kind, local_session_id, started_at) "
                "VALUES ('fk-run', 'TXFK', 'local', %s, now())",
                (999_999_999,))

    def test_archive_runs_check_rejects_kind_mismatch(self):
        self.db.register_tape("TXCK")
        good_local = self.db.create_local_session(
            "LCK", "/s", [[{"name": "t", "size_bytes": 1}]], "pack")
        # A 'remote' run must not carry a local session reference.
        with self.assertRaises(errors.CheckViolation):
            self._exec(
                "INSERT INTO archive_runs "
                "(run_label, tape_label, session_kind, local_session_id, started_at) "
                "VALUES ('ck-run', 'TXCK', 'remote', %s, now())",
                (good_local,))

    # -- review fixes: catalog write-path hardening ---------------------------

    def test_rename_tape_repoints_local_chunk_assignments(self):
        # local_chunks_manifest.tape_label is ON DELETE SET NULL; before the
        # fix, rename_tape forgot this table and the old tape's DELETE silently
        # wiped every in-flight chunk assignment.
        self.db.register_tape("TRN1")
        session_id = self.db.create_local_session(
            "RN_SESSION", "/rn", [[{"name": "top", "size_bytes": 5}]], "pack")
        self.db.assign_local_chunk_tape(session_id, 0, "TRN1")
        self.db.rename_tape("TRN1", "TRN2")
        rows = self._query(
            "SELECT tape_label FROM local_chunks_manifest WHERE session_id=%s",
            (session_id,))
        self.assertEqual([r["tape_label"] for r in rows], ["TRN2"])

    def test_create_local_session_is_idempotent_on_label(self):
        # An ambiguous-commit retry re-runs the create; the label upsert must
        # converge on the committed session without duplicating the manifest.
        chunks = [[{"name": "top", "size_bytes": 5}]]
        first = self.db.create_local_session("IDEM_L", "/idem", chunks, "pack")
        second = self.db.create_local_session("IDEM_L", "/idem", chunks, "pack")
        self.assertEqual(first, second)
        count = self._query(
            "SELECT COUNT(*) AS n FROM local_chunks_manifest "
            "WHERE session_id=%s", (first,))[0]["n"]
        self.assertEqual(count, 1)

    def test_create_remote_session_with_plan_is_atomic_and_idempotent(self):
        self.db.register_tape("TRP")
        rows = [
            (0, "/plan/a.bin", "a.bin", 10),
            (1, "/plan/b.bin", "b.bin", 20),
        ]
        sid = self.db.create_remote_session_with_plan(
            "PLAN_S", "host.example", "user", "/plan", "TRP", "C:/stage",
            rows=rows)
        session = self.db.get_remote_session(sid)
        self.assertEqual(session["total_files"], 2)
        self.assertEqual(session["total_bytes"], 30)
        self.assertEqual(session["chunk_count"], 2)
        self.assertEqual(self.db.get_pending_chunks(sid), [0, 1])
        # Retrying the same create converges instead of duplicating.
        again = self.db.create_remote_session_with_plan(
            "PLAN_S", "host.example", "user", "/plan", "TRP", "C:/stage",
            rows=rows)
        self.assertEqual(sid, again)
        self.assertEqual(self.db.count_chunks(sid), 2)

    def test_chunk_size_summary_matches_rows(self):
        self.db.register_tape("TCS")
        rows = [
            (0, "/cs/a.bin", "a.bin", 7),
            (0, "/cs/b.bin", "b.bin", 5),
            (1, "/cs/c.bin", "c.bin", 11),
        ]
        sid = self.db.create_remote_session_with_plan(
            "CS_S", "host.example", "user", "/cs", "TCS", "C:/stage",
            rows=rows)
        summary = self.db.get_chunk_size_summary(sid)
        self.assertEqual(summary[0], (12, 12))
        self.assertEqual(summary[1], (11, 11))
        # source_missing files drop out of present_bytes, not planned_bytes.
        manifest_id = self.db.get_chunk_files(sid, 0)[0]["manifest_id"]
        self.db.update_manifest_row(
            manifest_id, session_id=sid, status="source_missing")
        planned, present = self.db.get_chunk_size_summary(sid, 0)[0]
        self.assertEqual(planned, 12)
        self.assertEqual(present, 5)

    def test_delete_files_batch_reconciles_used_space(self):
        self.db.register_tape("TDEL")
        self.db.bulk_upsert_files([
            self._loose("/del/a.bin", "TDEL", size=100),
            self._loose("/del/b.bin", "TDEL", size=50),
        ])
        self.db.recalculate_tape_used_space("TDEL")
        ids = [r["file_id"] for r in self.db.search_catalog(tape_label="TDEL")]
        self.assertEqual(self.db.delete_files([ids[0]]), 1)
        tape = self.db.get_tape("TDEL")
        remaining = self._query(
            "SELECT COALESCE(SUM(file_size_bytes),0) AS n FROM files_index "
            "WHERE tape_label=%s", ("TDEL",))[0]["n"]
        self.assertEqual(tape["used_space"], remaining)
        with self.assertRaisesRegex(RuntimeError, "File record not found"):
            self.db.delete_file(999_999_999)

    def test_search_catalog_keyset_pagination(self):
        self.db.register_tape("TKS")
        self.db.bulk_upsert_files(
            [self._loose(f"/ks/f{i:02d}.bin", "TKS") for i in range(5)])
        seen = []
        after = 0
        while True:
            page = self.db.search_catalog(
                tape_label="TKS", limit=2, after_id=after)
            if not page:
                break
            ids = [r["file_id"] for r in page]
            self.assertEqual(ids, sorted(ids))
            seen.extend(ids)
            after = ids[-1]
        self.assertEqual(len(seen), 5)
        self.assertEqual(len(set(seen)), 5)

    # -- §1.2 backslash-safe remote manifest --------------------------------

    def test_remote_manifest_accepts_backslash_in_path(self):
        self.db.register_tape("TE")
        session_id = self.db.create_remote_session(
            "REMOTE_TEST", "host.example", "user", "/data", "TE", "C:/stage")
        # A backslash in a remote (Linux) filename previously raised KeyError
        # during plan-file insertion.
        self.db.insert_remote_manifest_batch(session_id, [
            (0, "/data/plain.txt", "plain.txt", 11),
            (0, "/data/weird\\name.txt", "name.txt", 22),
        ])
        files = self.db.get_chunk_files(session_id, 0)
        self.assertEqual(
            sorted(r["remote_path"] for r in files),
            ["/data/plain.txt", "/data/weird/name.txt"])


@unittest.skipUnless(_pg_available(), "PostgreSQL server not reachable")
class PgArchiveRunsMigrationTests(unittest.TestCase):
    """Exercise the production upgrade path of migration 004 on legacy data."""

    def setUp(self):
        self.dbname = f"lto_mig_{uuid.uuid4().hex[:12]}"
        with psycopg.connect(build_conninfo(dbname="postgres"),
                             autocommit=True) as conn:
            conn.execute(f'CREATE DATABASE "{self.dbname}"')
        self.conninfo = build_conninfo(dbname=self.dbname)

    def tearDown(self):
        with psycopg.connect(build_conninfo(dbname="postgres"),
                             autocommit=True) as conn:
            conn.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = %s AND pid <> pg_backend_pid()",
                (self.dbname,))
            conn.execute(f'DROP DATABASE IF EXISTS "{self.dbname}"')

    @staticmethod
    def _migration_sql():
        from src.constants import PROJECT_ROOT
        return (Path(PROJECT_ROOT) / "scripts" / "sql"
                / "004_postgres_archive_runs_sessions.sql").read_text(
                    encoding="utf-8")

    @staticmethod
    def _build_legacy_schema(conn):
        # The pre-migration shape: a bare, FK-less polymorphic session_id.
        conn.execute("""
            CREATE TABLE tapes (volume_label TEXT PRIMARY KEY);
            CREATE TABLE local_sessions (session_id BIGINT PRIMARY KEY);
            CREATE TABLE remote_sessions (session_id BIGINT PRIMARY KEY);
            CREATE TABLE archive_runs (
                run_id       BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                run_label    TEXT NOT NULL,
                tape_label   TEXT NOT NULL REFERENCES tapes(volume_label),
                session_kind TEXT NOT NULL DEFAULT 'legacy',
                session_id   BIGINT,
                started_at   TIMESTAMPTZ NOT NULL,
                completed_at TIMESTAMPTZ,
                UNIQUE (run_label, tape_label)
            );
            INSERT INTO tapes VALUES ('T');
            INSERT INTO local_sessions VALUES (1), (2);
            INSERT INTO archive_runs
                (run_label, tape_label, session_kind, session_id, started_at)
            VALUES
                ('valid',    'T', 'local',  1,    now()),
                ('dangling', 'T', 'local',  999,  now()),
                ('remote',   'T', 'remote', NULL, now()),
                ('legacy',   'T', 'legacy', NULL, now());
        """)

    def test_migration_backfills_and_drops_legacy_column(self):
        with psycopg.connect(self.conninfo, autocommit=True,
                             row_factory=dict_row) as conn:
            self._build_legacy_schema(conn)
            conn.execute(self._migration_sql())

            cols = {r["column_name"] for r in conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'archive_runs'").fetchall()}
            self.assertNotIn("session_id", cols)
            self.assertIn("local_session_id", cols)
            self.assertIn("remote_session_id", cols)

            rows = {r["run_label"]: r for r in conn.execute(
                "SELECT run_label, session_kind, local_session_id, "
                "remote_session_id FROM archive_runs").fetchall()}
            # Valid reference is backfilled into the typed column.
            self.assertEqual(rows["valid"]["local_session_id"], 1)
            self.assertIsNone(rows["valid"]["remote_session_id"])
            # The dangling reference (the bug this fixes) is demoted to NULL
            # rather than violating the new foreign key.
            self.assertIsNone(rows["dangling"]["local_session_id"])
            self.assertIsNone(rows["remote"]["local_session_id"])
            self.assertIsNone(rows["remote"]["remote_session_id"])

        # The foreign key is now enforced for future writes.
        with self.assertRaises(errors.ForeignKeyViolation):
            self._exec_on(
                self.conninfo,
                "INSERT INTO archive_runs (run_label, tape_label, session_kind, "
                "local_session_id, started_at) "
                "VALUES ('bad', 'T', 'local', 424242, now())")

    def test_migration_is_idempotent(self):
        with psycopg.connect(self.conninfo, autocommit=True) as conn:
            self._build_legacy_schema(conn)
            conn.execute(self._migration_sql())
            # Re-applying on the already-migrated schema must be a no-op.
            conn.execute(self._migration_sql())
        rows = self._query_on(
            self.conninfo,
            "SELECT local_session_id FROM archive_runs WHERE run_label = 'valid'")
        self.assertEqual(rows[0]["local_session_id"], 1)

    @staticmethod
    def _exec_on(conninfo, sql, params=()):
        with psycopg.connect(conninfo, autocommit=True) as conn:
            conn.execute(sql, params)

    @staticmethod
    def _query_on(conninfo, sql, params=()):
        with psycopg.connect(conninfo, autocommit=True,
                             row_factory=dict_row) as conn:
            return conn.execute(sql, params).fetchall()


if __name__ == "__main__":
    unittest.main()
