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
import os
import tempfile
import threading
import unittest
import uuid
from pathlib import Path
from typing import Any, TYPE_CHECKING, cast
from unittest import mock

if TYPE_CHECKING:
    import psycopg
    from psycopg import errors
    from psycopg.rows import dict_row
else:
    try:
        import psycopg
        from psycopg import errors
        from psycopg.rows import dict_row
    except ImportError:  # pragma: no cover - skipped when psycopg is absent
        psycopg = None
        errors = None
        dict_row = None

from src.inspector_repository import InspectorRepository
from src.local_manifest_archive import (
    dry_run_export, execute_export, prune_export, validate_export)
from pg_test_guard import (SKIP_REASON, create_test_database,
                           drop_test_database, pg_available)


def _connect(*args, **kwargs) -> Any:
    return cast(Any, psycopg.connect(*args, **kwargs))


def _pg_available():
    """Delegates to the fail-closed guard: an UNSAFE target raises here."""
    if psycopg is None:
        return False
    return pg_available()


@unittest.skipUnless(_pg_available(), SKIP_REASON)
class PgIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from src.pg_db import PgDatabaseManager

        cls.dbname, cls.conninfo = create_test_database("lto_test")
        cls.db = PgDatabaseManager(cls.conninfo)
        cls.directory_schema_was_auto_installed = cls.db.directory_catalog_schema_installed()
        cls.db.apply_directory_catalog_schema()

    @classmethod
    def tearDownClass(cls):
        try:
            cls.db.close()
        except Exception:
            pass
        drop_test_database(cls.dbname)

    # -- helpers -------------------------------------------------------------

    def _query(self, sql, params=()):
        with _connect(self.conninfo, autocommit=True,
                      row_factory=cast(Any, dict_row)) as conn:
            return conn.execute(sql, params).fetchall()

    def _exec(self, sql, params=()):
        with _connect(self.conninfo, autocommit=True) as conn:
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

    def test_directory_catalog_schema_is_not_auto_applied_on_startup(self):
        self.assertFalse(self.directory_schema_was_auto_installed)

    # -- §1.1 ILIKE escaping -------------------------------------------------

    def test_search_treats_underscore_as_literal(self):
        self.db.register_tape("TA")
        self.db.bulk_upsert_files([
            self._loose("/srv/data/report_2024.txt", "TA"),
            self._loose("/srv/data/reportX2024.txt", "TA"),
        ])
        hits = cast(Any, self.db.search_catalog(
            name_query="report_2024", tape_label="TA"))
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
        clips = cast(Any, self.db.search_catalog(
            name_query="clip_*", tape_label="TB"))
        self.assertEqual(
            sorted(m["file_name"] for m in clips),
            ["clip_alpha.mov", "clip_beta.mov"])

    def test_search_by_directory_literal_underscore(self):
        self.db.register_tape("TF")
        self.db.bulk_upsert_files([
            self._loose("/mnt/data_2024/a.txt", "TF"),
            self._loose("/mnt/dataX2024/b.txt", "TF"),
        ])
        hits = cast(Any, self.db.search_by_directory("/mnt/data_2024"))
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

    def test_inspector_subtree_sizes_are_recursive(self):
        self.db.register_tape("TSS")
        self.db.bulk_upsert_files([
            self._loose("/srv/proj/a.txt", "TSS", size=100),
            self._loose("/srv/proj/sub/b.txt", "TSS", size=20),
            self._loose("/srv/proj/sub/c.txt", "TSS", size=5),
            self._loose("/srv/other/d.txt", "TSS", size=1),
        ])
        by_path = {
            r["normalized_path"]: r["directory_id"]
            for r in self._query(
                "SELECT directory_id, normalized_path FROM catalog_directories "
                "WHERE tape_label = %s", ("TSS",))
        }
        with InspectorRepository(self.conninfo) as repo:
            sizes = repo.subtree_sizes([
                by_path["so02/srv/proj"],
                by_path["so02/srv/proj/sub"],
                by_path["so02/srv/other"],
            ])
        # A directory's total rolls up every descendant, not just direct files.
        proj = sizes[by_path["so02/srv/proj"]]
        self.assertEqual(proj["recursive_bytes"], 125)
        self.assertEqual(proj["recursive_file_count"], 3)
        sub = sizes[by_path["so02/srv/proj/sub"]]
        self.assertEqual(sub["recursive_bytes"], 25)
        self.assertEqual(sub["recursive_file_count"], 2)
        other = sizes[by_path["so02/srv/other"]]
        self.assertEqual(other["recursive_bytes"], 1)
        self.assertEqual(other["recursive_file_count"], 1)

    def test_inspector_subtree_sizes_empty_input(self):
        with InspectorRepository(self.conninfo) as repo:
            self.assertEqual(repo.subtree_sizes([]), {})

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

    def test_local_manifest_eligibility_query_accepts_only_terminal_local_work(self):
        self.db.register_tape("TMAN")
        session_id = self.db.create_local_session(
            "MANIFEST_DONE", "/manifest",
            [[{"name": "top", "size_bytes": 5}]], "pack")
        self.db.bulk_upsert_files([{
            "original_path": "/manifest/top/tiny.txt",
            "file_size_bytes": 5, "tape_label": "TMAN",
            "source_host": "so02", "is_packed": False,
            "container_name": None,
            "stored_path": "/manifest/top/tiny.txt",
            "local_session_id": session_id, "local_chunk_index": 0,
        }])
        before = dry_run_export(self.conninfo)
        before_eligible = before["eligible_rows"]
        self.db.update_local_chunk_status(session_id, 0, "backed_up")
        self.db.update_local_session(
            session_id, status="completed", completed_at="2026-07-16T00:00:00Z")
        after = dry_run_export(self.conninfo)
        self.assertEqual(after["eligible_rows"], before_eligible + 1)

    def test_zz_local_manifest_export_validate_prune_preserves_operations(self):
        eligible_before = dry_run_export(self.conninfo)["eligible_rows"]
        self.assertGreaterEqual(eligible_before, 1)
        operational_tables = (
            "remote_snapshot_files", "remote_plan_files", "remote_file_state",
            "remote_chunks", "remote_sessions", "local_chunks_manifest",
            "local_sessions",
        )
        before = {table: self._query(
            f"SELECT COUNT(*) AS n FROM {table}")[0]["n"]
            for table in operational_tables}
        with tempfile.TemporaryDirectory() as root:
            backup = os.path.join(root, "verified_hot.dump")
            with open(backup, "wb") as handle:
                handle.write(b"test-only throwaway database backup marker")
            exported = execute_export(self.conninfo, root, backup)
            export_id = exported["export_id"]
            validation = validate_export(self.conninfo, export_id)
            self.assertTrue(validation["passed"])
            owned = self._query(
                "SELECT local_session_id FROM local_manifest_export_rows "
                "WHERE export_id=%s AND eligible "
                "AND local_session_id IS NOT NULL LIMIT 1", (export_id,))[0]
            self._exec(
                "UPDATE local_sessions SET status='active', completed_at=NULL "
                "WHERE session_id=%s", (owned["local_session_id"],))
            with self.assertRaisesRegex(RuntimeError, "terminal ownership"):
                prune_export(
                    self.conninfo, export_id, backup, execute=True,
                    batch_size=1)
            partial = self._query(
                "SELECT COUNT(*) AS n FROM local_manifest_export_rows "
                "WHERE export_id=%s AND eligible AND pruned_at IS NOT NULL",
                (export_id,))[0]["n"]
            # The batch containing the newly active session is rolled back.
            self.assertEqual(partial, 0)
            self._exec(
                "UPDATE local_sessions SET status='completed', "
                "completed_at=now() WHERE session_id=%s",
                (owned["local_session_id"],))
            fake_process = {
                "pid": 999, "name": "robocopy.exe", "command": "test"}
            with mock.patch(
                    "src.local_manifest_archive.active_archive_processes",
                    side_effect=[[], [], [fake_process]]):
                with self.assertRaisesRegex(
                        RuntimeError, "process appeared"):
                    prune_export(
                        self.conninfo, export_id, backup, execute=True,
                        batch_size=1)
            partial = self._query(
                "SELECT COUNT(*) AS n FROM local_manifest_export_rows "
                "WHERE export_id=%s AND eligible AND pruned_at IS NOT NULL",
                (export_id,))[0]["n"]
            # The completed first batch remains durable when the next batch is
            # blocked before it starts.
            self.assertEqual(partial, 1)
            pruned = prune_export(
                self.conninfo, export_id, backup, execute=True,
                batch_size=1)
            self.assertEqual(pruned["deleted_rows"], eligible_before)
            self.assertGreaterEqual(len(pruned["batches"]), 1)
            self.assertEqual(self._query(
                "SELECT COUNT(*) AS n FROM local_manifest_export_rows "
                "WHERE export_id=%s", (export_id,))[0]["n"], 0)
            self.assertEqual(self._query(
                "SELECT status FROM local_manifest_exports WHERE export_id=%s",
                (export_id,))[0]["status"], "pruned")
        after = {table: self._query(
            f"SELECT COUNT(*) AS n FROM {table}")[0]["n"]
            for table in operational_tables}
        self.assertEqual(after, before)

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
        session = cast(Any, self.db.get_remote_session(sid))
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
        self.assertEqual(summary[0], (12, 12, 2))
        self.assertEqual(summary[1], (11, 11, 1))
        # source_missing files drop out of present_bytes, not planned_bytes
        # or file_count.
        manifest_id = self.db.get_chunk_files(sid, 0)[0]["manifest_id"]
        self.db.update_manifest_row(
            manifest_id, session_id=sid, status="source_missing")
        planned, present, count = self.db.get_chunk_size_summary(sid, 0)[0]
        self.assertEqual(planned, 12)
        self.assertEqual(present, 5)
        self.assertEqual(count, 2)

    def test_remote_streaming_session_appends_chunks_idempotently(self):
        self.db.register_tape("TSTR")
        sid = self.db.create_remote_streaming_session(
            "STREAM_S", "host.example", "user", "/stream", "TSTR",
            "C:/stage")
        session = cast(Any, self.db.get_remote_session(sid))
        self.assertFalse(session["scan_complete"])
        self.assertEqual(session["total_files"], 0)
        self.assertEqual(self.db.count_chunks(sid), 0)

        first = self.db.append_remote_streaming_chunk(sid, 0, [
            (0, "/stream/a.bin", "a.bin", 10),
            (0, "/stream/b.bin", "b.bin", 20),
        ])
        self.assertEqual(first, {"inserted_files": 2, "inserted_bytes": 30})
        self.assertEqual(self.db.get_pending_chunks(sid), [0])
        self.assertEqual(self.db.get_next_remote_chunk_index(sid), 1)
        self.assertEqual(self.db.get_chunk_size_summary(sid)[0], (30, 30, 2))

        dup = self.db.append_remote_streaming_chunk(sid, 1, [
            (1, "/stream/a.bin", "a.bin", 10),
            (1, "/stream/c.bin", "c.bin", 5),
        ])
        self.assertEqual(dup, {"inserted_files": 1, "inserted_bytes": 5})
        self.assertEqual(self.db.get_pending_chunks(sid), [0, 1])
        self.assertEqual(self.db.get_chunk_size_summary(sid)[1], (5, 5, 1))

        files = self.db.get_chunk_files(sid, 1)
        self.assertEqual([row["remote_path"] for row in files],
                         ["/stream/c.bin"])
        self.assertEqual(self.db.get_pending_remote_reserved_bytes(sid), 35)
        self.db.mark_remote_scan_complete(sid)
        session = cast(Any, self.db.get_remote_session(sid))
        self.assertTrue(session["scan_complete"])

    def test_delete_files_batch_reconciles_used_space(self):
        self.db.register_tape("TDEL")
        self.db.bulk_upsert_files([
            self._loose("/del/a.bin", "TDEL", size=100),
            self._loose("/del/b.bin", "TDEL", size=50),
        ])
        self.db.recalculate_tape_used_space("TDEL")
        ids = [r["file_id"] for r in cast(
            Any, self.db.search_catalog(tape_label="TDEL"))]
        self.assertEqual(self.db.delete_files([ids[0]]), 1)
        tape = cast(Any, self.db.get_tape("TDEL"))
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
            page = cast(Any, self.db.search_catalog(
                tape_label="TKS", limit=2, after_id=after))
            if not page:
                break
            ids = [r["file_id"] for r in page]
            self.assertEqual(ids, sorted(ids))
            seen.extend(ids)
            after = ids[-1]
        self.assertEqual(len(seen), 5)
        self.assertEqual(len(set(seen)), 5)

    def test_directory_catalog_counts_bundle_without_double_counting(self):
        self.db.register_tape("TDC")
        bundle_path = os.path.join("TROOT", "Bundle_001.zip")
        large_size = 12 * 1024 * 1024
        records = [
            {
                "file_name": "small.txt",
                "original_path": "/src/project/sub/small.txt",
                "file_size_bytes": 5,
                "tape_label": "TDC",
                "source_host": "so02",
                "is_packed": True,
                "container_name": "Bundle_001.zip",
                "stored_path": "sub/small.txt",
                "catalog_policy": "manifest_only",
                "manifest_name": "Bundle_001.manifest.jsonl.zst",
                "manifest_format": "jsonl",
                "manifest_compression": "zstd",
            },
            {
                "file_name": "large.bin",
                "original_path": "/src/project/sub/large.bin",
                "file_size_bytes": large_size,
                "tape_label": "TDC",
                "source_host": "so02",
                "is_packed": True,
                "container_name": "Bundle_001.zip",
                "stored_path": "sub/large.bin",
                "catalog_policy": "index",
                "manifest_name": "Bundle_001.manifest.jsonl.zst",
                "manifest_format": "jsonl",
                "manifest_compression": "zstd",
            },
        ]
        stats = self.db.bulk_upsert_directory_catalog(
            records, "TDC", "so02", tape_root="TROOT",
            index_min_file_mb=10)
        self.assertEqual(stats["bundles"], 1)
        self.db.bulk_upsert_files([
            dict(records[1], container_name=bundle_path)
        ])
        self.assertEqual(
            self._query(
                "SELECT COUNT(*) AS n FROM files_index WHERE tape_label=%s",
                ("TDC",))[0]["n"],
            1)
        used = self.db.recalculate_tape_used_space("TDC")
        self.assertEqual(used, large_size + 5)
        tree = self._query(
            """SELECT original_dir_path, recursive_file_count, recursive_bytes
               FROM directory_tree_index WHERE tape_label=%s""",
            ("TDC",))
        by_path = {row["original_dir_path"]: row for row in tree}
        self.assertEqual(by_path["/src/project/sub"]["recursive_file_count"], 2)
        self.assertEqual(
            by_path["/src/project/sub"]["recursive_bytes"], large_size + 5)

    def test_directory_backfill_dry_run_and_execute_are_idempotent(self):
        self.db.register_tape("TBF")
        records = [
            {
                "original_path": "/legacy/project/a.txt",
                "file_size_bytes": 7,
                "tape_label": "TBF",
                "source_host": "so02",
                "is_packed": True,
                "container_name": "LegacyBundle.zip",
                "stored_path": "project/a.txt",
            },
            {
                "original_path": "/legacy/project/sub/b.txt",
                "file_size_bytes": 11,
                "tape_label": "TBF",
                "source_host": "so02",
                "is_packed": True,
                "container_name": "LegacyBundle.zip",
                "stored_path": "project/sub/b.txt",
            },
        ]
        self.db.bulk_upsert_files(records)
        dry = self.db.backfill_directory_catalog_from_files_index(
            tape_label="TBF", dry_run=True)
        self.assertEqual(dry["bundles_pending"], 1)
        self.assertEqual(
            self._query(
                "SELECT COUNT(*) AS n FROM directory_archive_bundles "
                "WHERE tape_label=%s", ("TBF",))[0]["n"],
            0)
        first = self.db.backfill_directory_catalog_from_files_index(
            tape_label="TBF", dry_run=False)
        self.assertEqual(first["bundles_backfilled"], 1)
        counts_after_first = {
            table: self._query(
                f"SELECT COUNT(*) AS n FROM {table} WHERE tape_label=%s",
                ("TBF",))[0]["n"]
            for table in (
                "directory_archive_bundles",
                "directory_archive_stats",
                "directory_tree_index",
            )
        }
        second = self.db.backfill_directory_catalog_from_files_index(
            tape_label="TBF", dry_run=False)
        self.assertEqual(second["bundles_backfilled"], 0)
        counts_after_second = {
            table: self._query(
                f"SELECT COUNT(*) AS n FROM {table} WHERE tape_label=%s",
                ("TBF",))[0]["n"]
            for table in counts_after_first
        }
        self.assertEqual(counts_after_first, counts_after_second)

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


@unittest.skipUnless(_pg_available(), SKIP_REASON)
class PgArchiveRunsMigrationTests(unittest.TestCase):
    """Exercise the production upgrade path of migration 004 on legacy data."""

    def setUp(self):
        self.dbname, self.conninfo = create_test_database("lto_mig")

    def tearDown(self):
        drop_test_database(self.dbname)

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
        with _connect(self.conninfo, autocommit=True,
                      row_factory=cast(Any, dict_row)) as conn:
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
        with _connect(self.conninfo, autocommit=True) as conn:
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
        with _connect(conninfo, autocommit=True) as conn:
            conn.execute(sql, params)

    @staticmethod
    def _query_on(conninfo, sql, params=()):
        with _connect(conninfo, autocommit=True,
                      row_factory=cast(Any, dict_row)) as conn:
            return conn.execute(sql, params).fetchall()


@unittest.skipUnless(_pg_available(), SKIP_REASON)
class IncrementalScanMigrationTests(unittest.TestCase):
    """Plan 1 / Task 2.1 — migration 014 against a real PostgreSQL.

    Each test gets its own throwaway database, because the interesting cases
    are about the *transition* between schema states (not installed -> base ->
    finalized) and about a FINALIZE that must fail.
    """

    def setUp(self):
        from src.pg_db import PgDatabaseManager

        self.dbname, self.conninfo = create_test_database("lto_scan")
        self.db = PgDatabaseManager(self.conninfo)
        self.addCleanup(self._drop)

    def _drop(self):
        try:
            self.db.close()
        except Exception:
            pass
        drop_test_database(self.dbname)

    # -- helpers -----------------------------------------------------------
    def _query(self, sql, params=()):
        with _connect(self.conninfo, autocommit=True,
                      row_factory=cast(Any, dict_row)) as conn:
            return conn.execute(sql, params).fetchall()

    def _exec(self, sql, params=()):
        with _connect(self.conninfo, autocommit=True) as conn:
            conn.execute(sql, params)

    def _session(self, label="S1", tape="Tape_MIG"):
        """A minimal tape + session + plan the frontier can hang off."""
        self.db.register_tape(tape, 12000)
        session_id = self.db.create_remote_streaming_session(
            session_label=label, remote_host="h", remote_user="u",
            remote_path="/strg", tape_label=tape, staging_dir="C:\\stage")
        return session_id

    # -- state transitions -------------------------------------------------
    def test_the_schema_is_not_auto_applied_at_startup(self):
        """014 is explicit: a fresh manager must NOT have installed it."""
        self.assertFalse(self.db.incremental_scan_schema_installed())
        self.assertFalse(self.db.incremental_scan_schema_finalized())

    def test_the_base_half_is_idempotent(self):
        self.db.apply_incremental_scan_schema()
        self.assertTrue(self.db.incremental_scan_schema_installed())
        self.assertFalse(self.db.incremental_scan_schema_finalized())
        # Applying twice must be a no-op, not an error.
        self.db.apply_incremental_scan_schema()
        self.assertTrue(self.db.incremental_scan_schema_installed())

    def test_old_sessions_stay_readable_after_the_base_half(self):
        session_id = self._session()
        self.db.append_remote_streaming_chunk(
            session_id, 0, [(0, "/strg/a", "a", 10)])
        before = self.db.get_pending_chunks(session_id)

        self.db.apply_incremental_scan_schema()

        self.assertEqual(self.db.get_pending_chunks(session_id), before)
        rows = self._query(
            """SELECT owner_token, membership_state, expected_bytes
               FROM remote_chunks WHERE session_id=%s""", (session_id,))
        # Legacy rows keep NULL in every added column.
        self.assertEqual(rows[0]["owner_token"], None)
        self.assertEqual(rows[0]["membership_state"], None)
        self.assertEqual(rows[0]["expected_bytes"], None)

    def test_the_finalize_half_creates_the_unique_index(self):
        self._session()
        self.db.apply_incremental_scan_schema(finalize=True)
        self.assertTrue(self.db.incremental_scan_schema_finalized())

    def test_finalize_refuses_on_duplicate_plan_ordinals(self):
        """The refusal that protects a chunk which may already be on tape."""
        session_id = self._session()
        self.db.append_remote_streaming_chunk(
            session_id, 0, [(0, "/strg/a", "a", 10), (0, "/strg/b", "b", 20)])
        # Force the ambiguity migration 014 must refuse to repair.
        self._exec(
            """UPDATE remote_plan_files SET ordinal = 0
               WHERE plan_id = (SELECT plan_id FROM remote_sessions
                                WHERE session_id=%s)""",
            (session_id,))

        self.db.apply_incremental_scan_schema()          # base is fine
        with self.assertRaises(Exception) as caught:
            self.db.apply_incremental_scan_schema(finalize=True)
        self.assertIn("REFUSING to finalize", str(caught.exception))
        # ...and nothing was resequenced.
        ordinals = [r["ordinal"] for r in self._query(
            "SELECT ordinal FROM remote_plan_files ORDER BY plan_file_id")]
        self.assertEqual(ordinals, [0, 0])
        self.assertFalse(self.db.incremental_scan_schema_finalized())

    def test_the_preflight_reports_duplicates_without_changing_anything(self):
        session_id = self._session()
        self.db.append_remote_streaming_chunk(
            session_id, 0, [(0, "/strg/a", "a", 10), (0, "/strg/b", "b", 20)])
        self._exec(
            """UPDATE remote_plan_files SET ordinal = 0
               WHERE plan_id = (SELECT plan_id FROM remote_sessions
                                WHERE session_id=%s)""",
            (session_id,))
        report = self.db.incremental_scan_schema_preflight()
        self.assertEqual(report["database"], self.dbname)
        self.assertEqual(report["duplicate_ordinal_groups"], 1)
        self.assertTrue(report["blocking"])
        self.assertFalse(report["installed"])
        # Read-only: the schema is still absent.
        self.assertFalse(self.db.incremental_scan_schema_installed())

    def test_optional_migrations_are_unaffected(self):
        """014 must not require or disturb 007 / 012."""
        installed_before = self.db.directory_catalog_schema_installed()
        self.db.apply_incremental_scan_schema(finalize=True)
        self.assertEqual(self.db.directory_catalog_schema_installed(),
                         installed_before)

    # -- frontier behaviour ------------------------------------------------
    def _frontier_session(self):
        session_id = self._session()
        self.db.apply_incremental_scan_schema(finalize=True)
        self.db.create_scan_scopes(session_id, ["/strg/a", "/strg/b"])
        scopes = self.db.get_scan_scopes(session_id)
        return session_id, scopes

    def test_conservative_bootstrap_keeps_legacy_members_out_of_the_builder(self):
        """The migration record must survive the scopes the bootstrap creates.

        This follows the production construction path far enough to create the
        real ``SegmentChunkPublisher`` owned by ``FrontierScanCoordinator``.
        The recording builder is intentional: PostgreSQL also rejects duplicate
        snapshot paths while appending, which could otherwise hide a filter that
        ran too late.
        """
        from types import SimpleNamespace

        from src.archive_artifacts import (JsonlZstArtifactWriter,
                                           resolve_locator, segment_locator)
        from src.frontier_bootstrap import FrontierBootstrap
        from src.pipeline_types import ScanMetrics
        from src.planning import StreamingChunkBuilder
        from src.remote_orchestrator import RemoteOrchestrator
        from src.scan_frontier import FrontierScanCoordinator

        known = [(f"/strg/known{i}", 100) for i in range(6)]
        new_only = [(f"/strg/new{i}", 100) for i in range(6)]
        mixed = [entry for pair in zip(known, new_only) for entry in pair]

        session_id = self._session(label="LEGACY_BOOTSTRAP")
        self.assertFalse(self.db.session_has_snapshot_membership(session_id))
        self.db.append_remote_streaming_chunk(
            session_id, 0,
            [(0, path, f"known{i}", size)
             for i, (path, size) in enumerate(known)])
        self.assertTrue(self.db.session_has_snapshot_membership(session_id))
        self._exec(
            """UPDATE remote_chunks SET status='done'
               WHERE session_id=%s AND chunk_index=0""", (session_id,))
        self.db.apply_incremental_scan_schema(finalize=True)
        self.db.apply_container_format_schema()

        existing_chunk_before = self._query(
            """SELECT * FROM remote_chunks
               WHERE session_id=%s AND chunk_index=0""", (session_id,))
        existing_members_before = self._query(
            """SELECT pf.*, sf.remote_path, sf.file_size_bytes
               FROM remote_sessions s
               JOIN remote_plan_files pf ON pf.plan_id=s.plan_id
               JOIN remote_snapshot_files sf
                    ON sf.snapshot_file_id=pf.snapshot_file_id
               WHERE s.session_id=%s AND pf.chunk_index=0
               ORDER BY pf.ordinal""", (session_id,))

        with tempfile.TemporaryDirectory() as archive_root:
            bootstrap = FrontierBootstrap(
                db=self.db, session_id=session_id, scan_paths=["/strg"],
                archive_root=archive_root,
                scanner_factory=lambda _metrics: mock.Mock(),
                stop_event=threading.Event(),
                active_processes_probe=lambda: [],
                lock_holders_probe=lambda: [])
            result = bootstrap.execute(approved=True, conservative=True)
            self.assertEqual(result["mode"], "conservative")
            self.assertIsNotNone(self.db.get_frontier_bootstrap(session_id))
            self.assertTrue(self.db.session_has_frontier_state(session_id))

            scope = self.db.get_scan_scopes(session_id)[0]
            directory_id = self._query(
                """SELECT scan_directory_id FROM remote_scan_directories
                   WHERE scan_scope_id=%s AND canonical_path='/strg'""",
                (scope["scan_scope_id"],))[0]["scan_directory_id"]
            locator = segment_locator(session_id, directory_id, 0)
            with JsonlZstArtifactWriter(
                    archive_root, locator, scope="/strg",
                    scan_directory_id=directory_id,
                    session_id=session_id) as writer:
                for ordinal, (path, size) in enumerate(mixed):
                    writer.add(path=path, size=size, ordinal=ordinal)
            segment = self.db.publish_scan_segment(
                directory_id, first_scan_ordinal=0,
                last_scan_ordinal=len(mixed) - 1, locator=locator,
                file_count=writer.file_count, byte_count=writer.byte_count,
                artifact_size_bytes=os.path.getsize(
                    resolve_locator(archive_root, locator)),
                first_canonical_path=mixed[0][0],
                last_canonical_path=mixed[-1][0])

            added = []

            class RecordingBuilder(StreamingChunkBuilder):
                def add(self, remote_path, size):
                    added.append((remote_path, size))
                    return super().add(remote_path, size)

            state = SimpleNamespace(
                metrics=ScanMetrics(), remaining_bytes=10 ** 9,
                next_chunk_index=1, chunks=0, files=0, bytes=0,
                scan_error=None)
            legacy_session = RemoteOrchestrator._session_predates_frontier(
                SimpleNamespace(db=self.db), session_id)
            self.assertTrue(legacy_session)
            coordinator = FrontierScanCoordinator(
                db=self.db, session_id=session_id, scan_paths=["/strg"],
                archive_root=archive_root, state=state,
                remaining_lock=threading.Lock(), stop_event=threading.Event(),
                scanner_factory=lambda _metrics: mock.Mock(),
                builder_factory=lambda: RecordingBuilder(250),
                legacy_session=legacy_session)

            sealed = coordinator.publisher.publish_ready_segments(
                next_chunk_index=1)

        baseline_builder = StreamingChunkBuilder(250)
        expected_chunks = []
        for path, size in new_only:
            expected_chunks.extend(baseline_builder.add(path, size))
        expected_chunks.extend(baseline_builder.flush())

        planned_rows = self._query(
            """SELECT pf.chunk_index, pf.ordinal, sf.remote_path,
                      sf.file_size_bytes
               FROM remote_sessions s
               JOIN remote_plan_files pf ON pf.plan_id=s.plan_id
               JOIN remote_snapshot_files sf
                    ON sf.snapshot_file_id=pf.snapshot_file_id
               WHERE s.session_id=%s AND pf.chunk_index > 0
               ORDER BY pf.chunk_index, pf.ordinal""", (session_id,))
        actual_by_chunk = {}
        for row in planned_rows:
            actual_by_chunk.setdefault(row["chunk_index"], []).append(
                (row["remote_path"], row["file_size_bytes"]))

        self.assertEqual(added, new_only,
                         "known membership reached StreamingChunkBuilder.add")
        self.assertEqual(sealed, [1, 2, 3])
        self.assertEqual(list(actual_by_chunk.values()), expected_chunks)
        self.assertEqual(
            self._query(
                """SELECT * FROM remote_chunks
                   WHERE session_id=%s AND chunk_index=0""", (session_id,)),
            existing_chunk_before)
        self.assertEqual(
            self._query(
                """SELECT pf.*, sf.remote_path, sf.file_size_bytes
                   FROM remote_sessions s
                   JOIN remote_plan_files pf ON pf.plan_id=s.plan_id
                   JOIN remote_snapshot_files sf
                        ON sf.snapshot_file_id=pf.snapshot_file_id
                   WHERE s.session_id=%s AND pf.chunk_index=0
                   ORDER BY pf.ordinal""", (session_id,)),
            existing_members_before)
        import_state = self._query(
            """SELECT legacy_import_state FROM remote_scan_segments
               WHERE scan_segment_id=%s""",
            (segment["scan_segment_id"],))[0]["legacy_import_state"]
        self.assertEqual(import_state, "imported")

    def test_scopes_are_persisted_in_order_and_are_idempotent(self):
        session_id, scopes = self._frontier_session()
        self.assertEqual([s["source_root"] for s in scopes],
                         ["/strg/a", "/strg/b"])
        self.assertTrue(self.db.session_has_frontier_state(session_id))
        # Same set, reordered in config: persisted order wins, no error.
        self.db.create_scan_scopes(session_id, ["/strg/b", "/strg/a"])
        self.assertEqual(
            [s["source_root"] for s in self.db.get_scan_scopes(session_id)],
            ["/strg/a", "/strg/b"])

    def test_an_added_or_removed_root_is_refused(self):
        from src.pg_scan import ScanFrontierError
        session_id, _ = self._frontier_session()
        for drifted in (["/strg/a"], ["/strg/a", "/strg/b", "/strg/c"]):
            with self.assertRaises(ScanFrontierError):
                self.db.create_scan_scopes(session_id, drifted)

    def test_only_one_claimant_wins_a_directory(self):
        session_id, scopes = self._frontier_session()
        self.db.enqueue_scan_directories(
            scopes[0]["scan_scope_id"], [("/strg/a", 0)])

        first = self.db.claim_next_directory(session_id, "owner-1", "att-1")
        self.assertIsNotNone(first)
        self.assertEqual(first["listing_state"], "scanning")
        # A live lease blocks a second claimant.
        self.assertIsNone(
            self.db.claim_next_directory(session_id, "owner-2", "att-2"))

    def test_a_completed_directory_is_never_re_claimed(self):
        session_id, scopes = self._frontier_session()
        self.db.enqueue_scan_directories(
            scopes[0]["scan_scope_id"], [("/strg/a", 0)])
        claimed = self.db.claim_next_directory(session_id, "o", "a")
        self.assertTrue(self.db.complete_directory_listing(
            claimed["scan_directory_id"], "o",
            direct_file_count=3, direct_byte_count=30))
        self.assertIsNone(self.db.claim_next_directory(session_id, "o2", "a2"))

    def test_a_partial_directory_is_the_only_one_replayed(self):
        session_id, scopes = self._frontier_session()
        self.db.enqueue_scan_directories(
            scopes[0]["scan_scope_id"], [("/strg/a", 0), ("/strg/b", 1)])
        claimed = self.db.claim_next_directory(session_id, "o", "a")
        self.assertTrue(self.db.mark_directory_partial(
            claimed["scan_directory_id"], "o"))
        again = self.db.claim_next_directory(session_id, "o2", "a2")
        self.assertEqual(again["scan_directory_id"],
                         claimed["scan_directory_id"])

    def test_a_ready_segment_is_consumed_exactly_once(self):
        from src.pg_scan import SegmentRangeConflict
        session_id, scopes = self._frontier_session()
        self.db.enqueue_scan_directories(
            scopes[0]["scan_scope_id"], [("/strg/a", 0)])
        claimed = self.db.claim_next_directory(session_id, "o", "a")
        segment = self.db.publish_scan_segment(
            claimed["scan_directory_id"], first_scan_ordinal=0,
            last_scan_ordinal=9, locator="seg/0.jsonl.zst",
            file_count=10, byte_count=1000)
        self.assertEqual(segment["state"], "ready")
        self.assertEqual(segment["next_unconsumed_ordinal"], 0)

        self.db.append_remote_streaming_chunk(
            session_id, 0, [(0, "/strg/a/f", "f", 10)])
        first, last = self.db.consume_segment_range(
            segment["scan_segment_id"], session_id, 0, 4)
        self.assertEqual((first, last), (0, 3))

        # The cursor advanced, so the next chunk continues where this stopped.
        self.db.append_remote_streaming_chunk(
            session_id, 1, [(1, "/strg/a/g", "g", 10)])
        first, last = self.db.consume_segment_range(
            segment["scan_segment_id"], session_id, 1, 100)
        self.assertEqual((first, last), (4, 9))

        rows = self._query(
            "SELECT state, next_unconsumed_ordinal FROM remote_scan_segments")
        self.assertEqual(rows[0]["state"], "consumed")
        # Exhausted: a third attempt is a conflict, never a silent re-issue.
        self.db.append_remote_streaming_chunk(
            session_id, 2, [(2, "/strg/a/h", "h", 10)])
        with self.assertRaises(SegmentRangeConflict):
            self.db.consume_segment_range(
                segment["scan_segment_id"], session_id, 2, 1)

    def test_membership_ranges_cannot_overlap(self):
        session_id, scopes = self._frontier_session()
        self.db.enqueue_scan_directories(
            scopes[0]["scan_scope_id"], [("/strg/a", 0)])
        claimed = self.db.claim_next_directory(session_id, "o", "a")
        segment = self.db.publish_scan_segment(
            claimed["scan_directory_id"], first_scan_ordinal=0,
            last_scan_ordinal=9, locator="seg/0.jsonl.zst",
            file_count=10, byte_count=1000)
        self.db.append_remote_streaming_chunk(
            session_id, 0, [(0, "/strg/a/f", "f", 10)])
        self.db.consume_segment_range(
            segment["scan_segment_id"], session_id, 0, 4)
        # The unique range identity makes a duplicate insert impossible.
        with self.assertRaises(Exception):
            self._exec(
                """INSERT INTO remote_chunk_scan_segments
                   (session_id, chunk_index, scan_segment_id,
                    first_scan_ordinal, last_scan_ordinal, created_at)
                   VALUES (%s, 0, %s, 0, 3, now())""",
                (session_id, segment["scan_segment_id"]))

    def test_subtree_finality_needs_every_descendant(self):
        session_id, scopes = self._frontier_session()
        scope_id = scopes[0]["scan_scope_id"]
        self.db.enqueue_scan_directories(scope_id, [("/strg/a", 0)])
        parent = self.db.claim_next_directory(session_id, "o", "a")
        self.db.enqueue_scan_directories(
            scope_id, [("/strg/a/child", 1)],
            parent_directory_id=parent["scan_directory_id"])
        self.db.complete_directory_listing(
            parent["scan_directory_id"], "o",
            direct_file_count=1, direct_byte_count=10)

        finalized, reason = self.db.finalize_directory_subtree(
            parent["scan_directory_id"])
        self.assertFalse(finalized)
        self.assertIn("descendant", reason)

        child = self.db.claim_next_directory(session_id, "o", "a")
        self.db.complete_directory_listing(
            child["scan_directory_id"], "o",
            direct_file_count=0, direct_byte_count=0)
        self.assertTrue(
            self.db.finalize_directory_subtree(child["scan_directory_id"])[0])
        self.assertTrue(
            self.db.finalize_directory_subtree(parent["scan_directory_id"])[0])

    def test_an_error_directory_never_becomes_final(self):
        session_id, scopes = self._frontier_session()
        self.db.enqueue_scan_directories(
            scopes[0]["scan_scope_id"], [("/strg/a", 0)])
        claimed = self.db.claim_next_directory(session_id, "o", "a")
        self.db.complete_directory_listing(
            claimed["scan_directory_id"], "o", direct_file_count=0,
            direct_byte_count=0, error_count=1)
        finalized, reason = self.db.finalize_directory_subtree(
            claimed["scan_directory_id"])
        self.assertFalse(finalized)
        self.assertIn("listing_state", reason)

    def test_an_unresolved_error_blocks_finality(self):
        session_id, scopes = self._frontier_session()
        self.db.enqueue_scan_directories(
            scopes[0]["scan_scope_id"], [("/strg/a", 0)])
        claimed = self.db.claim_next_directory(session_id, "o", "a")
        self.db.complete_directory_listing(
            claimed["scan_directory_id"], "o",
            direct_file_count=1, direct_byte_count=1)
        self.db.record_scan_error(
            scan_directory_id=claimed["scan_directory_id"],
            category="permission_denied", path="/strg/a/secret")
        finalized, reason = self.db.finalize_directory_subtree(
            claimed["scan_directory_id"])
        self.assertFalse(finalized)
        self.assertIn("unresolved", reason)

    def test_a_source_change_invalidates_the_ancestor_chain(self):
        session_id, scopes = self._frontier_session()
        scope_id = scopes[0]["scan_scope_id"]
        self.db.enqueue_scan_directories(scope_id, [("/strg/a", 0)])
        parent = self.db.claim_next_directory(session_id, "o", "a")
        self.db.enqueue_scan_directories(
            scope_id, [("/strg/a/child", 1)],
            parent_directory_id=parent["scan_directory_id"])
        self.db.complete_directory_listing(
            parent["scan_directory_id"], "o",
            direct_file_count=1, direct_byte_count=1)
        child = self.db.claim_next_directory(session_id, "o", "a")
        self.db.complete_directory_listing(
            child["scan_directory_id"], "o",
            direct_file_count=0, direct_byte_count=0)
        self.db.finalize_directory_subtree(child["scan_directory_id"])
        self.db.finalize_directory_subtree(parent["scan_directory_id"])

        self.db.invalidate_directory(child["scan_directory_id"], "size changed")
        states = {r["canonical_path"]: r["subtree_coverage_state"]
                  for r in self._query(
                      "SELECT canonical_path, subtree_coverage_state "
                      "FROM remote_scan_directories")}
        self.assertEqual(states["/strg/a/child"], "invalidated")
        self.assertEqual(states["/strg/a"], "invalidated")

    def test_coverage_and_planning_are_independent(self):
        session_id, scopes = self._frontier_session()
        scope_id = scopes[0]["scan_scope_id"]
        self.db.enqueue_scan_directories(scope_id, [("/strg/a", 0)])
        claimed = self.db.claim_next_directory(session_id, "o", "a")
        segment = self.db.publish_scan_segment(
            claimed["scan_directory_id"], first_scan_ordinal=0,
            last_scan_ordinal=4, locator="seg/0.jsonl.zst",
            file_count=5, byte_count=50)
        self.db.complete_directory_listing(
            claimed["scan_directory_id"], "o",
            direct_file_count=5, direct_byte_count=50)
        self.assertTrue(
            self.db.finalize_directory_subtree(claimed["scan_directory_id"])[0])
        self.assertTrue(self.db.finalize_scan_scope(scope_id)[0])
        # Explored, but NOT planned: the ready segment is still unallocated.
        done, reason = self.db.mark_scope_planning_complete(scope_id)
        self.assertFalse(done)
        self.assertIn("unallocated", reason)

        self.db.append_remote_streaming_chunk(
            session_id, 0, [(0, "/strg/a/f", "f", 10)])
        self.db.consume_segment_range(
            segment["scan_segment_id"], session_id, 0, 5)
        self.assertTrue(self.db.mark_scope_planning_complete(scope_id)[0])

    def test_a_worker_attempt_records_process_identity(self):
        session_id, _ = self._frontier_session()
        attempt_id = self.db.start_worker_attempt(
            owner_token="owner-1", attempt_kind="scan", session_id=session_id,
            local_pid=4242, remote_command_token="tok-abc")
        live = self.db.list_live_worker_attempts(session_id)
        self.assertEqual([a["attempt_id"] for a in live], [attempt_id])
        self.assertEqual(live[0]["local_pid"], 4242)
        self.assertEqual(live[0]["remote_command_token"], "tok-abc")
        self.assertTrue(self.db.finish_worker_attempt(attempt_id, "completed"))
        self.assertEqual(self.db.list_live_worker_attempts(session_id), [])

    def test_a_sealed_chunk_must_declare_its_expectations(self):
        session_id, _ = self._frontier_session()
        self.db.append_remote_streaming_chunk(
            session_id, 0, [(0, "/strg/a", "a", 10)])
        with self.assertRaises(Exception):
            self._exec(
                """UPDATE remote_chunks SET membership_state='sealed'
                   WHERE session_id=%s AND chunk_index=0""", (session_id,))
        # With the expectations set, sealing is allowed.
        self._exec(
            """UPDATE remote_chunks
               SET membership_state='sealed', expected_file_count=1,
                   expected_bytes=10
               WHERE session_id=%s AND chunk_index=0""", (session_id,))

    def test_the_richer_file_outcomes_are_writable_after_014(self):
        session_id, _ = self._frontier_session()
        self.db.append_remote_streaming_chunk(
            session_id, 0, [(0, "/strg/a", "a", 10)])
        plan_file_id = self._query(
            "SELECT plan_file_id FROM remote_plan_files LIMIT 1"
        )[0]["plan_file_id"]
        for status in ("source_permission_denied", "source_unreadable",
                       "source_changed", "unresolved"):
            self._exec(
                """INSERT INTO remote_file_state
                   (session_id, plan_file_id, status, updated_at)
                   VALUES (%s, %s, %s, now())
                   ON CONFLICT (session_id, plan_file_id)
                       DO UPDATE SET status=EXCLUDED.status""",
                (session_id, plan_file_id, status))

    def test_the_rollback_refuses_to_drop_a_populated_frontier(self):
        session_id, _ = self._frontier_session()
        self.assertTrue(self.db.session_has_frontier_state(session_id))
        rollback = (Path(__file__).resolve().parent.parent / "scripts" / "sql"
                    / "014_postgres_incremental_scan_rollback.sql")
        # Executed with NO parameters, exactly as apply_incremental_scan_schema
        # does. Passing even an empty tuple makes psycopg parse the SQL for
        # placeholders, and the RAISE EXCEPTION format strings contain '%'.
        with self.assertRaises(Exception) as caught:
            with _connect(self.conninfo, autocommit=True) as conn:
                conn.execute(rollback.read_text(encoding="utf-8"))
        self.assertIn("REFUSING to roll back", str(caught.exception))

    def test_the_rollback_drops_an_empty_frontier_cleanly(self):
        """With nothing published, the rollback is allowed to remove it."""
        self._session()
        self.db.apply_incremental_scan_schema(finalize=True)
        self.assertTrue(self.db.incremental_scan_schema_installed())
        rollback = (Path(__file__).resolve().parent.parent / "scripts" / "sql"
                    / "014_postgres_incremental_scan_rollback.sql")
        with _connect(self.conninfo, autocommit=True) as conn:
            conn.execute(rollback.read_text(encoding="utf-8"))
        self.assertFalse(self.db.incremental_scan_schema_installed())
        # The nullable remote_chunks columns are deliberately KEPT.
        columns = {r["column_name"] for r in self._query(
            """SELECT column_name FROM information_schema.columns
               WHERE table_name='remote_chunks'""")}
        self.assertIn("owner_token", columns)
        self.assertIn("membership_state", columns)


@unittest.skipUnless(_pg_available(), SKIP_REASON)
class TapeRenameGenerationTests(unittest.TestCase):
    """A relabel is the SAME cartridge, so its generation history follows it.

    Migration 013 made every FK into ``tapes`` restrictive, and ``rename_tape``
    creates a new row and deletes the old one. Anything still pointing at the
    old ``tape_id`` therefore blocks the delete outright. This was latent until
    ``register_tape`` began creating a generation row for every tape (Plan 1
    Task 1.4), at which point every rename broke.
    """

    def setUp(self):
        from src.pg_db import PgDatabaseManager

        self.dbname, self.conninfo = create_test_database("lto_ren")
        self.db = PgDatabaseManager(self.conninfo)
        self.addCleanup(self._drop)

    def _drop(self):
        try:
            self.db.close()
        except Exception:
            pass
        drop_test_database(self.dbname)

    def _query(self, sql, params=()):
        with _connect(self.conninfo, autocommit=True,
                      row_factory=cast(Any, dict_row)) as conn:
            return conn.execute(sql, params).fetchall()

    def test_register_tape_creates_the_active_generation_atomically(self):
        self.db.register_tape("GEN1", 12000)
        rows = self._query(
            """SELECT g.generation, g.state, g.volume_label
               FROM tape_generations g JOIN tapes t ON t.tape_id = g.tape_id
               WHERE t.volume_label='GEN1'""")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["generation"], 1)
        self.assertEqual(rows[0]["state"], "active")
        self.assertEqual(self.db.get_active_tape_generation("GEN1"), 1)

    def test_register_tape_is_idempotent(self):
        self.db.register_tape("GEN1", 12000)
        self.db.register_tape("GEN1", 12000)
        rows = self._query(
            """SELECT 1 FROM tape_generations g
               JOIN tapes t ON t.tape_id = g.tape_id
               WHERE t.volume_label='GEN1'""")
        self.assertEqual(len(rows), 1)

    def test_a_rename_carries_the_generation_history_across(self):
        self.db.register_tape("OLD1", 12000)
        before = self._query(
            """SELECT g.generation_id, g.generation, g.state
               FROM tape_generations g JOIN tapes t ON t.tape_id = g.tape_id
               WHERE t.volume_label='OLD1'""")
        self.assertEqual(len(before), 1)

        self.db.rename_tape("OLD1", "NEW1")

        after = self._query(
            """SELECT g.generation_id, g.generation, g.state, g.volume_label
               FROM tape_generations g JOIN tapes t ON t.tape_id = g.tape_id
               WHERE t.volume_label='NEW1'""")
        self.assertEqual(len(after), 1)
        # The SAME generation row followed the cartridge — not a new one.
        self.assertEqual(after[0]["generation_id"], before[0]["generation_id"])
        self.assertEqual(after[0]["generation"], before[0]["generation"])
        self.assertEqual(after[0]["state"], "active")
        # The denormalised label was repointed too.
        self.assertEqual(after[0]["volume_label"], "NEW1")
        # Nothing is left behind on the old label.
        self.assertEqual(
            self._query("SELECT 1 FROM tapes WHERE volume_label='OLD1'"), [])
        self.assertEqual(self.db.get_active_tape_generation("NEW1"), 1)

    def test_a_rename_carries_reset_operations_across(self):
        self.db.register_tape("OLD2", 12000)
        tape_id = self._query(
            "SELECT tape_id FROM tapes WHERE volume_label='OLD2'"
        )[0]["tape_id"]
        with _connect(self.conninfo, autocommit=True) as conn:
            sha = "0" * 64
            conn.execute(
                """INSERT INTO tape_reset_operations
                   (operation_id, target_tape_id, target_label,
                    old_generation, new_generation, state, expected_drive,
                    expected_drive_serial, backup_path, backup_sha256,
                    restore_list_path, restore_list_sha256, shadow_database,
                    impact_report_path, impact_report_sha256, impact,
                    created_at)
                   VALUES ('op-1', %s, 'OLD2', 1, 2, 'intent_recorded',
                           'X:', 'drive_TEST', 'b.dump', %s, 'r.txt', %s,
                           'shadow_db', 'i.json', %s, '{}'::jsonb, now())""",
                (tape_id, sha, sha, sha))

        self.db.rename_tape("OLD2", "NEW2")

        rows = self._query(
            """SELECT o.operation_id FROM tape_reset_operations o
               JOIN tapes t ON t.tape_id = o.target_tape_id
               WHERE t.volume_label='NEW2'""")
        self.assertEqual([r["operation_id"] for r in rows], ["op-1"])

    def test_the_active_generation_reader_returns_none_when_retired(self):
        self.db.register_tape("GEN2", 12000)
        with _connect(self.conninfo, autocommit=True) as conn:
            conn.execute(
                """UPDATE tape_generations SET state='retired'
                   WHERE volume_label='GEN2'""")
        self.assertIsNone(self.db.get_active_tape_generation("GEN2"))


@unittest.skipUnless(_pg_available(), SKIP_REASON)
class ShadowLegacyMigrationTests(unittest.TestCase):
    """Migration 014 against a database holding REPRESENTATIVE legacy data.

    The database is built to look like a session interrupted mid-flight, which
    is the state migration 014 will actually meet: chunks in every status
    including ``backing``, a sealed plan, file-state rows, and a tape at
    generation 1. Every assertion is a before/after comparison, because the
    only claim that matters is "nothing that already existed changed".
    """

    def setUp(self):
        from src.pg_db import PgDatabaseManager

        self.dbname, self.conninfo = create_test_database("lto_shadow")
        self.db = PgDatabaseManager(self.conninfo)
        self.addCleanup(self._drop)
        self.session_id = self._build_legacy_session()

    def _drop(self):
        try:
            self.db.close()
        except Exception:
            pass
        drop_test_database(self.dbname)

    def _query(self, sql, params=()):
        with _connect(self.conninfo, autocommit=True,
                      row_factory=cast(Any, dict_row)) as conn:
            return conn.execute(sql, params).fetchall()

    def _exec(self, sql, params=()):
        with _connect(self.conninfo, autocommit=True) as conn:
            conn.execute(sql, params)

    def _build_legacy_session(self):
        """A session shaped like a real interrupted one."""
        self.db.register_tape("SHADOW_T1", 12000)
        session_id = self.db.create_remote_streaming_session(
            session_label="REMOTE_shadow_legacy", remote_host="so02",
            remote_user="lto", remote_path="/strg/a\n/strg/b",
            tape_label="SHADOW_T1", staging_dir="C:\\stage")
        for chunk_index in range(5):
            self.db.append_remote_streaming_chunk(
                session_id, chunk_index,
                [(chunk_index, f"/strg/a/f{chunk_index}_{i}",
                  f"f{chunk_index}_{i}", 100 + i) for i in range(4)])
        # A realistic spread of terminal and in-flight states.
        for chunk_index, status in ((0, "done"), (1, "done"), (2, "backing"),
                                    (3, "fetch_failed")):
            self._exec(
                """UPDATE remote_chunks SET status=%s
                   WHERE session_id=%s AND chunk_index=%s""",
                (status, session_id, chunk_index))
        plan_file_id = self._query(
            "SELECT plan_file_id FROM remote_plan_files ORDER BY plan_file_id "
            "LIMIT 1")[0]["plan_file_id"]
        self._exec(
            """INSERT INTO remote_file_state
               (session_id, plan_file_id, status, updated_at)
               VALUES (%s, %s, 'source_missing', now())""",
            (session_id, plan_file_id))
        self.db.mark_remote_scan_complete(session_id)
        return session_id

    def _snapshot(self):
        """Everything that must survive the migration, as one comparable dict."""
        return {
            "session": self._query(
                """SELECT session_id, session_label, remote_path, tape_label,
                          scan_complete, status, plan_id, total_files,
                          total_bytes, chunk_count, tape_generation
                   FROM remote_sessions ORDER BY session_id"""),
            "chunks": self._query(
                """SELECT session_id, chunk_index, status, error_msg
                   FROM remote_chunks ORDER BY session_id, chunk_index"""),
            "plan_files": self._query(
                """SELECT plan_id, snapshot_file_id, chunk_index, ordinal
                   FROM remote_plan_files ORDER BY plan_file_id"""),
            "snapshot_files": self._query(
                """SELECT snapshot_id, remote_path, file_size_bytes
                   FROM remote_snapshot_files ORDER BY snapshot_file_id"""),
            "file_state": self._query(
                """SELECT session_id, plan_file_id, status
                   FROM remote_file_state ORDER BY plan_file_id"""),
            "tapes": self._query(
                """SELECT volume_label, total_capacity, status,
                          current_generation
                   FROM tapes ORDER BY volume_label"""),
            "generations": self._query(
                """SELECT volume_label, generation, state
                   FROM tape_generations ORDER BY generation_id"""),
        }

    # -- the migration is additive ---------------------------------------
    def test_migration_014_changes_no_existing_row(self):
        before = self._snapshot()
        self.db.apply_incremental_scan_schema(finalize=True)
        after = self._snapshot()
        for table in before:
            self.assertEqual(before[table], after[table],
                             f"migration 014 altered {table}")

    def test_the_backing_chunk_is_untouched(self):
        before = self._query(
            """SELECT status FROM remote_chunks
               WHERE session_id=%s AND chunk_index=2""", (self.session_id,))
        self.db.apply_incremental_scan_schema(finalize=True)
        after = self._query(
            """SELECT status, owner_token, lease_expires_at
               FROM remote_chunks
               WHERE session_id=%s AND chunk_index=2""", (self.session_id,))
        self.assertEqual(before[0]["status"], "backing")
        self.assertEqual(after[0]["status"], "backing")
        # It did NOT acquire an owner or a lease as a side effect.
        self.assertIsNone(after[0]["owner_token"])
        self.assertIsNone(after[0]["lease_expires_at"])

    def test_no_chunk_gains_ownership_or_a_membership_seal(self):
        self.db.apply_incremental_scan_schema(finalize=True)
        rows = self._query(
            """SELECT owner_token, attempt_id, lease_expires_at,
                      membership_state, expected_file_count, expected_bytes
               FROM remote_chunks""")
        self.assertTrue(rows)
        for row in rows:
            for column, value in row.items():
                self.assertIsNone(value,
                                  f"{column} was populated by the migration")

    def test_existing_zip_plans_still_resolve(self):
        before = self.db.get_chunk_files(self.session_id, 0)
        self.db.apply_incremental_scan_schema(finalize=True)
        after = self.db.get_chunk_files(self.session_id, 0)
        self.assertTrue(before)
        self.assertEqual(before, after)

    def test_pending_chunks_are_unchanged(self):
        before = self.db.get_pending_chunks(self.session_id)
        self.db.apply_incremental_scan_schema(finalize=True)
        self.assertEqual(self.db.get_pending_chunks(self.session_id), before)

    def test_applying_the_base_half_twice_is_a_no_op(self):
        self.db.apply_incremental_scan_schema()
        snapshot = self._snapshot()
        self.db.apply_incremental_scan_schema()
        self.assertEqual(self._snapshot(), snapshot)
        self.assertTrue(self.db.incremental_scan_schema_installed())

    def test_applying_the_finalize_half_twice_is_a_no_op(self):
        self.db.apply_incremental_scan_schema(finalize=True)
        snapshot = self._snapshot()
        self.db.apply_incremental_scan_schema(finalize=True)
        self.assertEqual(self._snapshot(), snapshot)
        self.assertTrue(self.db.incremental_scan_schema_finalized())

    # -- coverage is never inferred from catalog rows ---------------------
    def test_catalog_rows_do_not_become_scan_coverage(self):
        """THE bootstrap invariant, on real data.

        The session has 20 plan-file rows describing /strg/a. After the
        migration there is still NO directory, NO segment and NO coverage —
        because nothing has been traversed.
        """
        self.db.apply_incremental_scan_schema(finalize=True)
        self.assertEqual(
            self._query("SELECT * FROM remote_scan_directories"), [])
        self.assertEqual(
            self._query("SELECT * FROM remote_scan_segments"), [])
        self.assertEqual(self._query("SELECT * FROM remote_scan_scopes"), [])
        self.assertFalse(self.db.session_has_frontier_state(self.session_id))

    def test_the_frontier_schema_gate_requires_both_migration_halves(self):
        from src.scan_frontier import incremental_scan_schema_ready
        # Nothing applied.
        self.assertEqual(
            incremental_scan_schema_ready(self.db),
            (False, "migration_014_not_installed"),
        )
        # Base only.
        self.db.apply_incremental_scan_schema()
        self.assertEqual(
            incremental_scan_schema_ready(self.db),
            (False, "migration_014_not_finalized"),
        )
        # Finalized.
        self.db.apply_incremental_scan_schema(finalize=True)
        self.assertEqual(
            incremental_scan_schema_ready(self.db),
            (True, "schema_ready"),
        )

    # -- the finalize audit on real duplicate data ------------------------
    def test_finalize_refuses_and_leaves_the_ordinals_untouched(self):
        self._exec(
            """UPDATE remote_plan_files SET ordinal = 0
               WHERE chunk_index = 0""")
        before = self._query(
            """SELECT plan_file_id, ordinal FROM remote_plan_files
               ORDER BY plan_file_id""")
        self.db.apply_incremental_scan_schema()
        with self.assertRaises(Exception) as caught:
            self.db.apply_incremental_scan_schema(finalize=True)
        self.assertIn("REFUSING to finalize", str(caught.exception))
        self.assertEqual(
            self._query("""SELECT plan_file_id, ordinal FROM remote_plan_files
                           ORDER BY plan_file_id"""), before)
        self.assertFalse(self.db.incremental_scan_schema_finalized())

    def test_a_failed_finalize_leaves_the_base_half_usable(self):
        self._exec("UPDATE remote_plan_files SET ordinal = 0 "
                   "WHERE chunk_index = 0")
        self.db.apply_incremental_scan_schema()
        with self.assertRaises(Exception):
            self.db.apply_incremental_scan_schema(finalize=True)
        self.assertTrue(self.db.incremental_scan_schema_installed())
        self.assertEqual(self.db.get_pending_chunks(self.session_id),
                         [2, 3, 4])

    # -- sealed membership is immutable, enforced by the database ---------
    def test_the_unique_index_makes_duplicate_ordinals_impossible(self):
        self.db.apply_incremental_scan_schema(finalize=True)
        row = self._query(
            """SELECT plan_id, snapshot_file_id, chunk_index, ordinal
               FROM remote_plan_files ORDER BY plan_file_id LIMIT 1""")[0]
        other = self._query(
            """SELECT snapshot_file_id FROM remote_snapshot_files
               WHERE snapshot_file_id <> %s LIMIT 1""",
            (row["snapshot_file_id"],))[0]
        with self.assertRaises(Exception):
            self._exec(
                """INSERT INTO remote_plan_files
                   (plan_id, snapshot_file_id, chunk_index, ordinal)
                   VALUES (%s, %s, %s, %s)""",
                (row["plan_id"], other["snapshot_file_id"],
                 row["chunk_index"], row["ordinal"]))

    def test_appending_to_a_sealed_chunk_is_refused(self):
        self.db.apply_incremental_scan_schema(finalize=True)
        self.db.seal_remote_chunk(self.session_id, 4,
                                  expected_file_count=4, expected_bytes=406)
        with self.assertRaises(Exception) as caught:
            self.db.append_remote_streaming_chunk(
                self.session_id, 4, [(4, "/strg/a/late", "late", 1)])
        self.assertIn("SEALED", str(caught.exception))

    def test_a_sealed_chunk_must_carry_its_expectation(self):
        self.db.apply_incremental_scan_schema(finalize=True)
        with self.assertRaises(Exception):
            self._exec(
                """UPDATE remote_chunks SET membership_state='sealed'
                   WHERE session_id=%s AND chunk_index=4""",
                (self.session_id,))


@unittest.skipUnless(_pg_available(), SKIP_REASON)
class RealConcurrencyTests(unittest.TestCase):
    """Claims, transitions and segment consumption under REAL concurrency.

    A fake cannot prove these: they depend on PostgreSQL's row locks and on
    ``UPDATE ... WHERE`` seeing a consistent snapshot. Two connections race
    here, deliberately.
    """

    def setUp(self):
        from src.pg_db import PgDatabaseManager

        self.dbname, self.conninfo = create_test_database("lto_conc")
        self.db = PgDatabaseManager(self.conninfo)
        self.other = PgDatabaseManager(self.conninfo)
        self.addCleanup(self._drop)

        self.db.register_tape("CONC_T", 12000)
        self.session_id = self.db.create_remote_streaming_session(
            session_label="REMOTE_conc", remote_host="so02", remote_user="lto",
            remote_path="/strg/a", tape_label="CONC_T", staging_dir="C:\\s")
        self.db.append_remote_streaming_chunk(
            self.session_id, 0, [(0, "/strg/a/f0", "f0", 10)])
        self.db.append_remote_streaming_chunk(
            self.session_id, 1, [(1, "/strg/a/f1", "f1", 20)])
        self.db.apply_incremental_scan_schema(finalize=True)

    def _drop(self):
        for manager in (getattr(self, "db", None), getattr(self, "other", None)):
            try:
                manager.close()
            except Exception:
                pass
        drop_test_database(self.dbname)

    def _query(self, sql, params=()):
        with _connect(self.conninfo, autocommit=True,
                      row_factory=cast(Any, dict_row)) as conn:
            return conn.execute(sql, params).fetchall()

    # -- chunk claims -----------------------------------------------------
    def test_only_one_of_two_workers_claims_a_chunk(self):
        first = self.db.claim_chunk_for_staging(
            self.session_id, 0, "owner-A", "att-A", lease_seconds=600)
        second = self.other.claim_chunk_for_staging(
            self.session_id, 0, "owner-B", "att-B", lease_seconds=600)
        self.assertTrue(first)
        self.assertFalse(second, "two workers claimed the same chunk")
        row = self._query(
            """SELECT owner_token, status FROM remote_chunks
               WHERE session_id=%s AND chunk_index=0""",
            (self.session_id,))[0]
        self.assertEqual(row["owner_token"], "owner-A")
        self.assertEqual(row["status"], "fetching")

    def test_parallel_claims_from_many_threads_yield_exactly_one_winner(self):
        import threading
        from src.pg_db import PgDatabaseManager

        winners = []
        lock = threading.Lock()

        def attempt(index):
            manager = PgDatabaseManager(self.conninfo)
            try:
                if manager.claim_chunk_for_staging(
                        self.session_id, 1, f"owner-{index}", f"att-{index}"):
                    with lock:
                        winners.append(index)
            finally:
                manager.close()

        threads = [threading.Thread(target=attempt, args=(i,))
                   for i in range(6)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(60)
        self.assertEqual(len(winners), 1, f"winners: {winners}")

    def test_a_claim_cannot_be_taken_from_a_backing_chunk(self):
        self.db.transition_chunk(self.session_id, 0, "fetching",
                                 validate=False)
        self.db.transition_chunk(self.session_id, 0, "packing")
        self.db.transition_chunk(self.session_id, 0, "backing")
        self.assertFalse(
            self.db.claim_chunk_for_staging(self.session_id, 0, "o", "a"))
        self.assertEqual(
            self._query("""SELECT status FROM remote_chunks
                           WHERE session_id=%s AND chunk_index=0""",
                        (self.session_id,))[0]["status"], "backing")

    def test_renewing_and_releasing_require_the_owner(self):
        self.db.claim_chunk_for_staging(self.session_id, 0, "owner-A", "att-A")
        self.assertFalse(self.db.renew_chunk_claim(
            self.session_id, 0, "owner-B"))
        self.assertFalse(self.db.release_chunk_claim(
            self.session_id, 0, "owner-B"))
        self.assertTrue(self.db.renew_chunk_claim(
            self.session_id, 0, "owner-A"))
        self.assertTrue(self.db.release_chunk_claim(
            self.session_id, 0, "owner-A", to_status="pending"))

    def test_a_backing_chunk_can_never_be_released_or_renewed(self):
        self.db.claim_chunk_for_staging(self.session_id, 0, "owner-A", "att-A")
        self.db.transition_chunk(self.session_id, 0, "packing")
        self.db.transition_chunk(self.session_id, 0, "backing")
        self.assertFalse(self.db.renew_chunk_claim(
            self.session_id, 0, "owner-A"))
        self.assertFalse(self.db.release_chunk_claim(
            self.session_id, 0, "owner-A", to_status="pending"))

    # -- stale-lease reporting and reclamation ----------------------------
    def test_an_expired_lease_is_reported_but_not_reclaimed(self):
        self.db.claim_chunk_for_staging(self.session_id, 0, "owner-A", "att-A",
                                        lease_seconds=-1)
        expired = self.db.list_expired_chunk_claims(self.session_id)
        self.assertEqual([row["chunk_index"] for row in expired], [0])
        # Reporting changed nothing.
        self.assertEqual(
            self._query("""SELECT owner_token FROM remote_chunks
                           WHERE session_id=%s AND chunk_index=0""",
                        (self.session_id,))[0]["owner_token"], "owner-A")

    def test_a_backing_chunk_never_appears_as_an_expired_claim(self):
        self.db.claim_chunk_for_staging(self.session_id, 0, "owner-A", "att-A",
                                        lease_seconds=-1)
        self.db.transition_chunk(self.session_id, 0, "packing")
        self.db.transition_chunk(self.session_id, 0, "backing")
        self.assertEqual(self.db.list_expired_chunk_claims(self.session_id), [])

    def test_reclaiming_requires_evidence_and_returns_the_chunk(self):
        self.db.claim_chunk_for_staging(self.session_id, 0, "owner-A", "att-A",
                                        lease_seconds=-1)
        with self.assertRaises(Exception):
            self.db.reclaim_expired_chunk(self.session_id, 0, "owner-A", "")
        self.assertTrue(self.db.reclaim_expired_chunk(
            self.session_id, 0, "owner-A", "local PID 4242 no longer exists"))
        row = self._query(
            """SELECT status, owner_token, error_msg FROM remote_chunks
               WHERE session_id=%s AND chunk_index=0""",
            (self.session_id,))[0]
        self.assertEqual(row["status"], "pending")
        self.assertIsNone(row["owner_token"])
        self.assertIn("4242", row["error_msg"])

    def test_reclaiming_a_backing_chunk_does_nothing(self):
        self.db.claim_chunk_for_staging(self.session_id, 0, "owner-A", "att-A",
                                        lease_seconds=-1)
        self.db.transition_chunk(self.session_id, 0, "packing")
        self.db.transition_chunk(self.session_id, 0, "backing")
        self.assertFalse(self.db.reclaim_expired_chunk(
            self.session_id, 0, "owner-A", "process gone"))
        self.assertEqual(
            self._query("""SELECT status FROM remote_chunks
                           WHERE session_id=%s AND chunk_index=0""",
                        (self.session_id,))[0]["status"], "backing")

    # -- transitions ------------------------------------------------------
    def test_a_forbidden_transition_is_refused_against_real_state(self):
        from src.pipeline_types import ForbiddenTransition
        self.db.transition_chunk(self.session_id, 0, "fetching",
                                 validate=False)
        self.db.transition_chunk(self.session_id, 0, "packing")
        self.db.transition_chunk(self.session_id, 0, "backing")
        for target in ("pending", "fetching", "packing", "backup_failed"):
            with self.assertRaises(ForbiddenTransition, msg=target):
                self.db.transition_chunk(self.session_id, 0, target)
        self.assertEqual(
            self._query("""SELECT status FROM remote_chunks
                           WHERE session_id=%s AND chunk_index=0""",
                        (self.session_id,))[0]["status"], "backing")

    def test_backing_may_still_move_forward_to_done(self):
        self.db.transition_chunk(self.session_id, 0, "fetching",
                                 validate=False)
        self.db.transition_chunk(self.session_id, 0, "packing")
        self.db.transition_chunk(self.session_id, 0, "backing")
        self.assertTrue(self.db.transition_chunk(self.session_id, 0, "done"))

    def test_a_lost_compare_and_swap_returns_false(self):
        """The chunk is 'fetching'; a caller that believed it was 'pending'
        must LOSE the swap rather than overwrite someone else's progress."""
        self.db.transition_chunk(self.session_id, 0, "fetching",
                                 validate=False)
        # pending -> fetching is a legal transition, so the matrix permits the
        # attempt; it is the compare-and-swap that must fail.
        self.assertFalse(self.db.transition_chunk(
            self.session_id, 0, "fetching", expected_from="pending"))
        self.assertTrue(self.db.transition_chunk(
            self.session_id, 0, "packing", expected_from="fetching"))

    def test_declaring_an_impossible_source_state_is_refused_outright(self):
        """A caller asserting `backing -> packing` is a bug, not a lost race."""
        from src.pipeline_types import ForbiddenTransition
        with self.assertRaises(ForbiddenTransition):
            self.db.transition_chunk(self.session_id, 0, "packing",
                                     expected_from="backing")

    def test_a_stale_owner_token_loses_the_swap(self):
        self.db.claim_chunk_for_staging(self.session_id, 0, "owner-A", "att-A")
        self.assertFalse(self.db.transition_chunk(
            self.session_id, 0, "packing", owner_token="owner-STALE",
            validate=False))
        self.assertTrue(self.db.transition_chunk(
            self.session_id, 0, "packing", owner_token="owner-A",
            validate=False))

    # -- segment consumption ----------------------------------------------
    def _ready_segment(self, first=0, last=9):
        self.db.create_scan_scopes(self.session_id, ["/strg/a"])
        scope = self.db.get_scan_scopes(self.session_id)[0]
        self.db.enqueue_scan_directories(
            scope["scan_scope_id"], [("/strg/a", 0)])
        claimed = self.db.claim_next_directory(self.session_id, "o", "a")
        return self.db.publish_scan_segment(
            claimed["scan_directory_id"], first_scan_ordinal=first,
            last_scan_ordinal=last, locator="scan_segments/s/d/seg.jsonl.zst",
            file_count=last - first + 1, byte_count=100)

    def test_two_chunks_cannot_consume_the_same_ordinals(self):
        segment = self._ready_segment()
        first_a, last_a = self.db.consume_segment_range(
            segment["scan_segment_id"], self.session_id, 0, 5)
        first_b, last_b = self.db.consume_segment_range(
            segment["scan_segment_id"], self.session_id, 1, 5)
        self.assertEqual((first_a, last_a), (0, 4))
        self.assertEqual((first_b, last_b), (5, 9))
        self.assertEqual(len(set(range(first_a, last_a + 1))
                             & set(range(first_b, last_b + 1))), 0)

    def test_an_exhausted_segment_refuses_further_consumption(self):
        from src.pg_scan import SegmentRangeConflict
        segment = self._ready_segment(0, 2)
        self.db.consume_segment_range(
            segment["scan_segment_id"], self.session_id, 0, 3)
        with self.assertRaises(SegmentRangeConflict):
            self.db.consume_segment_range(
                segment["scan_segment_id"], self.session_id, 1, 1)

    def test_the_range_identity_blocks_a_duplicate_membership_row(self):
        segment = self._ready_segment()
        self.db.consume_segment_range(
            segment["scan_segment_id"], self.session_id, 0, 5)
        with self.assertRaises(Exception):
            with _connect(self.conninfo, autocommit=True) as conn:
                conn.execute(
                    """INSERT INTO remote_chunk_scan_segments
                       (session_id, chunk_index, scan_segment_id,
                        first_scan_ordinal, last_scan_ordinal, created_at)
                       VALUES (%s, 0, %s, 0, 4, now())""",
                    (self.session_id, segment["scan_segment_id"]))

    def test_only_one_worker_claims_a_directory(self):
        self.db.create_scan_scopes(self.session_id, ["/strg/a"])
        scope = self.db.get_scan_scopes(self.session_id)[0]
        self.db.enqueue_scan_directories(
            scope["scan_scope_id"], [("/strg/a", 0)])
        first = self.db.claim_next_directory(self.session_id, "o1", "a1")
        second = self.other.claim_next_directory(self.session_id, "o2", "a2")
        self.assertIsNotNone(first)
        self.assertIsNone(second)

    def test_a_transaction_failure_rolls_back_the_whole_seal(self):
        """seal_remote_chunk writes the expectation AND the segment reference
        in one transaction; a failure must leave neither."""
        segment = self._ready_segment()
        before = self._query(
            """SELECT membership_state FROM remote_chunks
               WHERE session_id=%s AND chunk_index=0""",
            (self.session_id,))[0]["membership_state"]
        with self.assertRaises(Exception):
            # An impossible segment reference aborts the transaction after the
            # remote_chunks UPDATE has already been issued.
            self.db.seal_remote_chunk(
                self.session_id, 0, expected_file_count=1, expected_bytes=10,
                scan_segment_id=9_999_999, first_scan_ordinal=0,
                last_scan_ordinal=0)
        after = self._query(
            """SELECT membership_state FROM remote_chunks
               WHERE session_id=%s AND chunk_index=0""",
            (self.session_id,))[0]["membership_state"]
        self.assertEqual(after, before)
        self.assertIsNone(after)

    # -- richer file outcomes, persisted ----------------------------------
    def test_the_richer_source_outcomes_persist(self):
        plan_file_id = self._query(
            "SELECT plan_file_id FROM remote_plan_files LIMIT 1"
        )[0]["plan_file_id"]
        for status in ("source_permission_denied", "source_unreadable",
                       "source_changed", "unresolved"):
            with _connect(self.conninfo, autocommit=True) as conn:
                conn.execute(
                    """INSERT INTO remote_file_state
                       (session_id, plan_file_id, status, updated_at)
                       VALUES (%s, %s, %s, now())
                       ON CONFLICT (session_id, plan_file_id)
                           DO UPDATE SET status=EXCLUDED.status""",
                    (self.session_id, plan_file_id, status))
            self.assertEqual(
                self._query("""SELECT status FROM remote_file_state
                               WHERE plan_file_id=%s""",
                            (plan_file_id,))[0]["status"], status)

    def test_an_unrepresentable_path_error_persists_for_review(self):
        """Task 3.3: a literal backslash cannot be planned, and the record of
        that decision must survive in the database for an operator."""
        backslash_path = "/strg/a/back" + chr(92) + "slash"
        scope = None
        self.db.create_scan_scopes(self.session_id, ["/strg/a"])
        scope = self.db.get_scan_scopes(self.session_id)[0]
        error_id = self.db.record_scan_error(
            scan_scope_id=scope["scan_scope_id"],
            category="unrepresentable_path", path=backslash_path,
            message="literal backslash would merge with a different file")
        row = self._query(
            "SELECT path, category, disposition FROM remote_scan_errors "
            "WHERE scan_error_id=%s", (error_id,))[0]
        # Stored byte-for-byte — NOT normalised into a separator.
        self.assertEqual(row["path"], backslash_path)
        self.assertIn(chr(92), row["path"])
        self.assertEqual(row["category"], "unrepresentable_path")
        self.assertEqual(row["disposition"], "unresolved")

    def test_an_unresolved_error_keeps_a_directory_from_finality(self):
        self.db.create_scan_scopes(self.session_id, ["/strg/a"])
        scope = self.db.get_scan_scopes(self.session_id)[0]
        self.db.enqueue_scan_directories(
            scope["scan_scope_id"], [("/strg/a", 0)])
        claimed = self.db.claim_next_directory(self.session_id, "o", "a")
        self.db.complete_directory_listing(
            claimed["scan_directory_id"], "o",
            direct_file_count=1, direct_byte_count=10)
        self.db.record_scan_error(
            scan_directory_id=claimed["scan_directory_id"],
            category="unrepresentable_path", path="/strg/a/x")
        final, reason = self.db.finalize_directory_subtree(
            claimed["scan_directory_id"])
        self.assertFalse(final)
        self.assertIn("unresolved", reason)


@unittest.skipUnless(_pg_available(), SKIP_REASON)
class ContainerFormatMigrationTests(unittest.TestCase):
    """Plan 2 Phase 0 / migration 015 against disposable PostgreSQL only."""

    def setUp(self):
        from src.pg_db import PgDatabaseManager

        self.dbname, self.conninfo = create_test_database("lto_formats")
        self.db = PgDatabaseManager(self.conninfo)
        self.staging = tempfile.TemporaryDirectory()
        self.addCleanup(self._drop)

    def _drop(self):
        try:
            self.db.close()
        except Exception:
            pass
        try:
            self.staging.cleanup()
        except Exception:
            pass
        drop_test_database(self.dbname)

    def _query(self, sql, params=()):
        with _connect(self.conninfo, autocommit=True,
                      row_factory=cast(Any, dict_row)) as conn:
            return conn.execute(sql, params).fetchall()

    def _exec(self, sql, params=()):
        with _connect(self.conninfo, autocommit=True) as conn:
            conn.execute(sql, params)

    def _ready_014(self, *, finalize=True):
        self.db.apply_incremental_scan_schema(finalize=finalize)

    def _session(self, label="FORMAT_SESSION", tape="FORMAT_TAPE"):
        self.db.register_tape(tape, 12000)
        return self.db.create_remote_streaming_session(
            session_label=label, remote_host="fixture.example",
            remote_user="fixture", remote_path="/fixture",
            tape_label=tape, staging_dir=self.staging.name)

    def _append(self, session_id, chunk_index, member_count=2, **gate):
        rows = [
            (chunk_index,
             f"/fixture/chunk_{chunk_index:03d}/member_{ordinal:03d}.bin",
             f"member_{ordinal:03d}.bin", ordinal + 1)
            for ordinal in range(member_count)
        ]
        return self.db.append_remote_streaming_chunk(
            session_id, chunk_index, rows, **gate)

    def _catalog_chunks(self, session_id, indexes, tape="FORMAT_TAPE", *,
                        archive_run_id=None):
        records = []
        for chunk_index in indexes:
            path = f"/fixture/chunk_{chunk_index:03d}/member_000.bin"
            record = {
                "original_path": path,
                "canonical_source_path": path,
                "file_size_bytes": 1,
                "tape_label": tape,
                "source_host": "fixture",
                "is_packed": False,
                "container_name": None,
                "stored_path": f"chunks/{chunk_index:03d}/member_000.bin",
                "remote_session_id": session_id,
                "remote_chunk_index": chunk_index,
            }
            if archive_run_id is not None:
                record["archive_run_id"] = archive_run_id
            records.append(record)
        self.db.bulk_upsert_files(records)

    def _catalog_done_prefix(self, session_id, last_index, tape="FORMAT_TAPE"):
        self._catalog_chunks(session_id, range(last_index + 1), tape)
        self._exec(
            """UPDATE remote_chunks SET status='done', updated_at=now()
               WHERE session_id=%s AND chunk_index<=%s""",
            (session_id, last_index))

    @staticmethod
    def _staging_evidence(indexes):
        from datetime import datetime, timezone
        return {
            "root_accessible": True,
            "checked_chunk_indexes": list(indexes),
            "entry_count": 0,
            "unreadable_count": 0,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }

    def _stored_tar_planning_session(self, rows):
        session_id = self._session(label="TAR_PLAN_FIXTURE")
        self._append(session_id, 0, member_count=1)
        self.db.append_remote_streaming_chunk(session_id, 1, rows)
        self._catalog_done_prefix(session_id, 0)
        self._ready_014()
        self.db.apply_container_format_schema(
            exception_session_id=session_id,
            expected_boundary=1,
            approval_id="task-2-1-plan-test",
            approval_reason="synthetic never-started suffix for planning",
            staging_evidence=self._staging_evidence([1]))
        self.db.apply_stored_tar_plan_schema()
        self.db.seal_remote_chunk(
            session_id, 1, expected_file_count=len(rows),
            expected_bytes=sum(int(row[3]) for row in rows))
        return session_id

    def test_015_is_explicit_and_requires_finalized_014(self):
        self.assertFalse(self.db.container_format_schema_installed())
        with self.assertRaisesRegex(Exception, "requires migrations 013 and 014"):
            self.db.apply_container_format_schema()
        self.assertEqual(self._query(
            """SELECT count(*) AS n FROM information_schema.columns
               WHERE table_schema='public' AND table_name='remote_chunks'
                 AND column_name='packaging_format'""")[0]["n"], 0)

        self._ready_014(finalize=False)
        with self.assertRaisesRegex(Exception, "requires finalized migration 014"):
            self.db.apply_container_format_schema()
        self.assertFalse(self.db.container_format_schema_installed())

    def test_zip_backfill_is_idempotent_immutable_and_preserves_legacy_bundle(self):
        session_id = self._session()
        self._append(session_id, 0)
        bundle = self._query(
            """INSERT INTO archive_bundles(tape_label,tape_path)
               VALUES ('FORMAT_TAPE','legacy/original.zip')
               RETURNING bundle_id,tape_label,tape_path""")[0]
        self._ready_014()

        self.assertEqual(
            self.db.apply_container_format_schema(),
            ["015_postgres_container_formats.sql"])
        report = self.db.validate_container_format_schema()
        self.assertTrue(report["ready"])
        self.assertEqual(report["format_counts"], {"zip": 1, "stored_tar": 0})
        chunk = self._query(
            """SELECT packaging_format, packaging_assigned_at
               FROM remote_chunks WHERE session_id=%s AND chunk_index=0""",
            (session_id,))[0]
        self.assertEqual(chunk["packaging_format"], "zip")
        self.assertIsNotNone(chunk["packaging_assigned_at"])
        after = self._query(
            """SELECT bundle_id,tape_label,tape_path,container_id,container_format
               FROM archive_bundles WHERE bundle_id=%s""",
            (bundle["bundle_id"],))[0]
        self.assertEqual(
            (after["bundle_id"], after["tape_label"], after["tape_path"]),
            (bundle["bundle_id"], bundle["tape_label"], bundle["tape_path"]))
        self.assertIsNone(after["container_id"])
        self.assertEqual(after["container_format"], "zip")

        with self.assertRaisesRegex(Exception, "write-once"):
            self._exec(
                """UPDATE remote_chunks SET packaging_format='stored_tar'
                   WHERE session_id=%s AND chunk_index=0""", (session_id,))
        with self.assertRaisesRegex(Exception, "immutable"):
            self._exec(
                """UPDATE archive_bundles SET container_format='stored_tar'
                   WHERE bundle_id=%s""", (bundle["bundle_id"],))

        # Re-apply with the same checksum/schema: no identities or formats move.
        self.db.apply_container_format_schema()
        self.assertTrue(self.db.validate_container_format_schema()["ready"])
        self.assertEqual(self._query(
            "SELECT count(*) AS n FROM archive_bundles")[0]["n"], 1)

    def test_016_persists_stored_tar_partition_cap_and_member_ordinals(self):
        from src.stored_tar_planning import GNU_TAR_RECORD_SIZE

        rows = [
            (1, "/fixture/chunk_001/small_a.bin", "small_a.bin", 9),
            (1, "/fixture/chunk_001/large.bin", "large.bin", 10),
            (1, "/fixture/chunk_001/small_b.bin", "small_b.bin", 8),
        ]
        session_id = self._stored_tar_planning_session(rows)
        chunk_files = self.db.get_chunk_files(session_id, 1)
        plan = self.db.get_or_create_stored_tar_chunk_plan(
            session_id, 1, chunk_files, loose_threshold_bytes=10,
            max_size_bytes=GNU_TAR_RECORD_SIZE)

        self.assertEqual(plan.max_size_bytes, GNU_TAR_RECORD_SIZE)
        self.assertEqual(
            [(m.plan_ordinal, m.storage_class, m.container_ordinal)
             for m in plan.members],
            [(0, "small_files", 0), (1, "loose_files", None),
             (2, "small_files", 1)])
        self.assertEqual(
            [(c.container_ordinal, c.expected_member_count,
              c.expected_logical_bytes, c.max_size_bytes)
             for c in plan.containers],
            [(0, 1, 9, GNU_TAR_RECORD_SIZE),
             (1, 1, 8, GNU_TAR_RECORD_SIZE)])
        row = self._query(
            """SELECT stored_tar_max_size_bytes FROM remote_chunks
               WHERE session_id=%s AND chunk_index=1""",
            (session_id,))[0]
        self.assertEqual(row["stored_tar_max_size_bytes"], GNU_TAR_RECORD_SIZE)
        stored = self._query(
            """SELECT plan_ordinal,storage_class,container_ordinal
               FROM archive_container_members
               WHERE session_id=%s AND chunk_index=1
               ORDER BY plan_ordinal""", (session_id,))
        self.assertEqual(
            [(r["plan_ordinal"], r["storage_class"], r["container_ordinal"])
             for r in stored],
            [(0, "small_files", 0), (1, "loose_files", None),
             (2, "small_files", 1)])

    def test_stored_tar_plan_is_reused_across_restart_and_config_change(self):
        rows = [
            (1, f"/fixture/chunk_001/small_{i}.bin", f"small_{i}.bin", 1)
            for i in range(5)
        ]
        session_id = self._stored_tar_planning_session(rows)
        chunk_files = self.db.get_chunk_files(session_id, 1)
        first = self.db.get_or_create_stored_tar_chunk_plan(
            session_id, 1, chunk_files, loose_threshold_bytes=10,
            max_size_bytes=512 * 512)
        self.db.close()
        from src.pg_db import PgDatabaseManager
        self.db = PgDatabaseManager(self.conninfo)
        second = self.db.get_or_create_stored_tar_chunk_plan(
            session_id, 1, self.db.get_chunk_files(session_id, 1),
            loose_threshold_bytes=2, max_size_bytes=512 * 512 * 9)
        self.assertEqual(second.max_size_bytes, first.max_size_bytes)
        self.assertEqual(second.members, first.members)
        self.assertEqual(second.containers, first.containers)

    def test_stored_tar_plan_records_source_missing_without_container(self):
        rows = [
            (1, "/fixture/chunk_001/missing.bin", "missing.bin", 1),
            (1, "/fixture/chunk_001/small.bin", "small.bin", 1),
        ]
        session_id = self._stored_tar_planning_session(rows)
        missing_id = self._query(
            """SELECT pf.plan_file_id FROM remote_sessions s
               JOIN remote_plan_files pf ON pf.plan_id=s.plan_id
               WHERE s.session_id=%s AND pf.chunk_index=1
               ORDER BY pf.ordinal LIMIT 1""", (session_id,))[0]["plan_file_id"]
        self.db.update_manifest_row(
            missing_id, session_id=session_id, status="source_missing",
            error_msg="fixture")
        plan = self.db.get_or_create_stored_tar_chunk_plan(
            session_id, 1, self.db.get_chunk_files(session_id, 1),
            loose_threshold_bytes=10, max_size_bytes=512 * 512)
        self.assertEqual(
            [(m.plan_ordinal, m.storage_class, m.container_ordinal)
             for m in plan.members],
            [(0, "source_missing", None), (1, "small_files", 0)])

    def test_approved_session37_shape_assigns_only_never_started_suffix(self):
        """Session-37's provenance gaps still yield the approved boundary."""
        session_id = self._session(label="MIXED_FORMAT_FIXTURE")
        self.db.apply_directory_catalog_schema()
        for chunk_index in range(113):
            self._append(session_id, chunk_index)
        # Real shape: all 49 prefix chunks are done, but only six have catalog
        # rows; every legacy chunk expectation/membership marker remains NULL.
        self._catalog_chunks(session_id, range(6))
        self._exec(
            """UPDATE remote_chunks SET status='done', updated_at=now()
               WHERE session_id=%s AND chunk_index<=48""", (session_id,))
        # Real shape: 134 migration-007 rows carry no trustworthy remote chunk
        # or run provenance. Their absence cannot grant TAR and is accepted only
        # as a conservative ZIP-prefix provenance gap.
        self._exec(
            """INSERT INTO directory_archive_bundles
                   (source_host,original_dir_path,tape_label,archive_run_id,
                    remote_session_id,chunk_index,stored_bundle_path,
                    file_count,byte_count,small_file_count,small_file_bytes,
                    large_file_count,large_file_bytes,backup_date,record_key)
               SELECT 'fixture', '/fixture/legacy_' || g, 'FORMAT_TAPE', NULL,
                      %s, NULL, 'legacy/' || g || '.zip', 1, 1, 1, 1, 0, 0,
                      now(), decode(md5(g::text) || md5(g::text), 'hex')
               FROM generate_series(1,134) AS g""", (session_id,))
        self._ready_014()

        shape = self._query(
            """SELECT count(*) AS chunks,
                      count(*) FILTER (WHERE status='done') AS done,
                      count(*) FILTER (WHERE status='pending') AS pending,
                      count(*) FILTER (WHERE membership_state IS NULL
                                        AND expected_file_count IS NULL
                                        AND expected_bytes IS NULL) AS null_shape,
                      count(*) FILTER (WHERE owner_token IS NULL
                                        AND lease_expires_at IS NULL
                                        AND attempt_id IS NULL
                                        AND error_msg IS NULL) AS unclaimed
               FROM remote_chunks WHERE session_id=%s""", (session_id,))[0]
        self.assertEqual(tuple(shape.values()), (113, 49, 64, 113, 113))
        self.assertEqual(self._query(
            """SELECT count(DISTINCT remote_chunk_index) AS chunks
               FROM files_index WHERE remote_session_id=%s""",
            (session_id,))[0]["chunks"], 6)
        self.assertEqual(self._query(
            """SELECT count(*) AS rows,
                      count(*) FILTER (WHERE chunk_index IS NULL) AS null_chunks
               FROM directory_archive_bundles WHERE remote_session_id=%s""",
            (session_id,))[0], {"rows": 134, "null_chunks": 134})

        before = self._query(
            """SELECT pf.chunk_index,pf.ordinal,sf.remote_path,
                      sf.file_size_bytes
               FROM remote_sessions s
               JOIN remote_plan_files pf ON pf.plan_id=s.plan_id
               JOIN remote_snapshot_files sf
                 ON sf.snapshot_file_id=pf.snapshot_file_id
               WHERE s.session_id=%s
               ORDER BY pf.chunk_index,pf.ordinal""", (session_id,))
        classification = self.db.classify_format_boundary(session_id)
        self.assertEqual(classification["derived_boundary"], 49)
        self.assertEqual(classification["blocking"], [])
        self.assertEqual(classification["prefix_evidence_counts"], {
            "corroborated": 6, "status_only": 43})

        self.db.apply_container_format_schema(
            exception_session_id=session_id,
            expected_boundary=49,
            approval_id="phase0-test-approval",
            approval_reason=(
                "synthetic fixture proves fixed membership and no operational "
                "evidence for the pending suffix"),
            staging_evidence=self._staging_evidence(range(49, 113)))

        formats = self._query(
            """SELECT chunk_index,packaging_format,status
               FROM remote_chunks WHERE session_id=%s ORDER BY chunk_index""",
            (session_id,))
        self.assertEqual(len(formats), 113)
        self.assertTrue(all(row["packaging_format"] == "zip"
                            for row in formats[:49]))
        self.assertTrue(all(row["packaging_format"] == "stored_tar"
                            for row in formats[49:]))
        self.assertTrue(all(row["status"] == "done" for row in formats[:49]))
        self.assertTrue(all(row["status"] == "pending"
                            for row in formats[49:]))
        session = self.db.get_remote_session(session_id)
        self.assertEqual(session["default_packaging_format"], "stored_tar")

        after = self._query(
            """SELECT pf.chunk_index,pf.ordinal,sf.remote_path,
                      sf.file_size_bytes
               FROM remote_sessions s
               JOIN remote_plan_files pf ON pf.plan_id=s.plan_id
               JOIN remote_snapshot_files sf
                 ON sf.snapshot_file_id=pf.snapshot_file_id
               WHERE s.session_id=%s
               ORDER BY pf.chunk_index,pf.ordinal""", (session_id,))
        self.assertEqual(after, before)
        suffix_ordinals = self._query(
            """SELECT chunk_index,count(*) AS members,min(ordinal) AS first,
                      max(ordinal) AS last,count(DISTINCT ordinal) AS distinct_n
               FROM remote_plan_files
               WHERE plan_id=(SELECT plan_id FROM remote_sessions
                              WHERE session_id=%s)
                 AND chunk_index BETWEEN 49 AND 112
               GROUP BY chunk_index ORDER BY chunk_index""", (session_id,))
        self.assertEqual(len(suffix_ordinals), 64)
        self.assertTrue(all((row["members"], row["first"], row["last"],
                             row["distinct_n"]) == (2, 0, 1, 2)
                            for row in suffix_ordinals))
        evidence = self._query(
            """SELECT classification,assigned_format,prefix_evidence_basis,
                      count(*) AS n
               FROM remote_packaging_boundary_chunks WHERE session_id=%s
               GROUP BY classification,assigned_format,prefix_evidence_basis""",
            (session_id,))
        self.assertEqual({
            (row["classification"], row["assigned_format"],
             row["prefix_evidence_basis"]): row["n"]
            for row in evidence}, {
                ("immutable_zip", "zip", "corroborated"): 6,
                ("immutable_zip", "zip", "status_only"): 43,
                ("approved_stored_tar_exception", "stored_tar", None): 64,
            })
        report = self.db.validate_container_format_schema()
        self.assertEqual(report["prefix_evidence_counts"], {
            "corroborated": 6, "status_only": 43})
        boundary_evidence = self._query(
            """SELECT database_evidence FROM remote_packaging_boundaries
               WHERE session_id=%s""", (session_id,))[0]["database_evidence"]
        self.assertEqual(boundary_evidence["corroborated_prefix_count"], 6)
        self.assertEqual(boundary_evidence["status_only_prefix_count"], 43)

        # Same approved application is a no-op: neither format nor immutable
        # audit evidence changes.
        formats_before_reapply = [(row["chunk_index"], row["packaging_format"])
                                  for row in formats]
        self.db.apply_container_format_schema(
            exception_session_id=session_id, expected_boundary=49,
            approval_id="phase0-test-approval",
            approval_reason=(
                "synthetic fixture proves fixed membership and no operational "
                "evidence for the pending suffix"),
            staging_evidence=self._staging_evidence(range(49, 113)))
        formats_after_reapply = self._query(
            """SELECT chunk_index,packaging_format FROM remote_chunks
               WHERE session_id=%s ORDER BY chunk_index""", (session_id,))
        self.assertEqual(
            [(row["chunk_index"], row["packaging_format"])
             for row in formats_after_reapply], formats_before_reapply)
        with self.assertRaisesRegex(Exception, "write-once"):
            self._exec(
                """UPDATE remote_chunks SET packaging_format='stored_tar'
                   WHERE session_id=%s AND chunk_index=0""", (session_id,))

        # Future chunks inherit TAR, but disabled/mismatched gates cannot create
        # one and their plan membership rolls back with the failed transaction.
        with self.assertRaisesRegex(Exception, "assignment is disabled"):
            self._append(session_id, 113)
        with self.assertRaisesRegex(Exception, "reader contract mismatch"):
            self._append(
                session_id, 113, stored_tar_write_enabled=True,
                reader_contract_version=99)
        with self.assertRaisesRegex(Exception, "assignment is disabled"):
            self._append(
                session_id, 113, stored_tar_write_enabled="true",
                reader_contract_version=1)
        self.assertEqual(self._query(
            """SELECT count(*) AS n FROM remote_chunks
               WHERE session_id=%s AND chunk_index=113""",
            (session_id,))[0]["n"], 0)
        self._append(
            session_id, 113, stored_tar_write_enabled=True,
            reader_contract_version=1)
        future = self._query(
            """SELECT packaging_format FROM remote_chunks
               WHERE session_id=%s AND chunk_index=113""", (session_id,))[0]
        self.assertEqual(future["packaging_format"], "stored_tar")
        self.assertTrue(self.db.require_existing_stored_tar_recovery(
            session_id, 49, reader_contract_version=1))

    def test_contradictory_evidence_rolls_back_the_entire_migration(self):
        session_id = self._session(label="CONTRADICTORY_FIXTURE")
        self._append(session_id, 0)
        self._append(session_id, 1)
        self._catalog_done_prefix(session_id, 0)
        self._ready_014()
        plan_file_id = self._query(
            """SELECT pf.plan_file_id FROM remote_sessions s
               JOIN remote_plan_files pf ON pf.plan_id=s.plan_id
               WHERE s.session_id=%s AND pf.chunk_index=1 LIMIT 1""",
            (session_id,))[0]["plan_file_id"]
        self._exec(
            """INSERT INTO remote_file_state
                   (session_id,plan_file_id,status,updated_at)
               VALUES (%s,%s,'pending',now())""", (session_id, plan_file_id))

        with self.assertRaisesRegex(Exception, "no evidence-proven"):
            self.db.apply_container_format_schema(
                exception_session_id=session_id,
                expected_boundary=1,
                approval_id="must-rollback",
                approval_reason="contradictory synthetic evidence",
                staging_evidence=self._staging_evidence([1]))
        columns = self._query(
            """SELECT count(*) AS n FROM information_schema.columns
               WHERE table_schema='public' AND table_name='remote_chunks'
                 AND column_name='packaging_format'""")[0]["n"]
        self.assertEqual(columns, 0)
        self.assertEqual(self._query(
            "SELECT count(*) AS n FROM remote_packaging_boundaries")[0]["n"]
            if self._query("SELECT to_regclass('remote_packaging_boundaries') AS r")[0]["r"]
            else 0, 0)

    def test_pending_chunk_with_any_claim_evidence_blocks_migration(self):
        self._ready_014()
        cases = (
            ("owner", "owner_token='fixture-owner'"),
            ("lease", "lease_expires_at=now() + interval '1 hour'"),
            ("attempt", "attempt_id='fixture-attempt'"),
            ("error", "error_msg='fixture-error'"),
        )
        for case_index, (name, assignment) in enumerate(cases):
            with self.subTest(evidence=name):
                session_id = self._session(
                    label=f"PENDING_EVIDENCE_{name}_{case_index}")
                self._append(session_id, 0)
                self._append(session_id, 1)
                self._exec(
                    """UPDATE remote_chunks SET status='done', updated_at=now()
                       WHERE session_id=%s AND chunk_index=0""", (session_id,))
                self._exec(
                    f"""UPDATE remote_chunks SET {assignment}, updated_at=now()
                        WHERE session_id=%s AND chunk_index=1""", (session_id,))
                classification = self.db.classify_format_boundary(session_id)
                self.assertIsNone(classification["derived_boundary"])
                with self.assertRaisesRegex(Exception, "no evidence-proven"):
                    self.db.apply_container_format_schema(
                        exception_session_id=session_id, expected_boundary=1,
                        approval_id=f"pending-evidence-{name}",
                        approval_reason=(
                            "every pending claim/error evidence kind must keep "
                            "the Stored TAR suffix fail-closed"),
                        staging_evidence=self._staging_evidence([1]))
                self.assertIsNone(self._query(
                    "SELECT to_regclass('archive_containers') AS r")[0]["r"])

    def test_non_done_fixed_prefix_still_blocks_exception(self):
        session_id = self._session(label="NON_DONE_PREFIX_FIXTURE")
        self._append(session_id, 0)
        self._append(session_id, 1)
        self._exec(
            """UPDATE remote_chunks SET status='packing', updated_at=now()
               WHERE session_id=%s AND chunk_index=0""", (session_id,))
        self._ready_014()

        classification = self.db.classify_format_boundary(session_id)
        self.assertEqual(classification["derived_boundary"], 1)
        self.assertTrue(any("indeterminate chunks: 0" in item
                            for item in classification["blocking"]))
        with self.assertRaisesRegex(Exception, "indexes.*0"):
            self.db.apply_container_format_schema(
                exception_session_id=session_id, expected_boundary=1,
                approval_id="non-done-prefix",
                approval_reason="a non-done prefix cannot receive ZIP by exception",
                staging_evidence=self._staging_evidence([1]))
        self.assertIsNone(self._query(
            "SELECT to_regclass('archive_containers') AS r")[0]["r"])

    def test_directory_row_proven_for_suffix_still_blocks_exception(self):
        session_id = self._session(label="DIRECTORY_SUFFIX_FIXTURE")
        self.db.apply_directory_catalog_schema()
        for chunk_index in range(3):
            self._append(session_id, chunk_index)
        self._exec(
            """UPDATE remote_chunks SET status='done', updated_at=now()
               WHERE session_id=%s AND chunk_index=0""", (session_id,))
        run_id = self._query(
            """INSERT INTO archive_runs
                   (run_label,tape_label,session_kind,remote_session_id,started_at)
               VALUES ('directory-suffix-run','FORMAT_TAPE','remote',%s,now())
               RETURNING run_id""", (session_id,))[0]["run_id"]
        self._catalog_chunks(session_id, [2], archive_run_id=run_id)
        self._exec(
            """INSERT INTO directory_archive_bundles
                   (source_host,original_dir_path,tape_label,archive_run_id,
                    remote_session_id,chunk_index,stored_bundle_path,
                    file_count,byte_count,small_file_count,small_file_bytes,
                    large_file_count,large_file_bytes,backup_date,record_key)
               VALUES ('fixture','/fixture/suffix','FORMAT_TAPE',%s,%s,NULL,
                       'legacy/suffix.zip',1,1,1,1,0,0,now(),decode(
                       md5('directory-suffix') || md5('directory-suffix'),'hex'))""",
            (run_id, session_id))
        self._ready_014()

        classification = self.db.classify_format_boundary(session_id)
        self.assertEqual(classification["derived_boundary"], 1)
        self.assertTrue(any("TAR-suffix provenance" in item
                            for item in classification["blocking"]))
        with self.assertRaisesRegex(Exception, "TAR-suffix provenance"):
            self.db.apply_container_format_schema(
                exception_session_id=session_id, expected_boundary=1,
                approval_id="directory-suffix",
                approval_reason=(
                    "linked directory provenance at or after the boundary "
                    "must abort"),
                staging_evidence=self._staging_evidence([1, 2]))
        self.assertIsNone(self._query(
            "SELECT to_regclass('archive_containers') AS r")[0]["r"])

    def test_plain_zip_backfill_permanently_closes_exception(self):
        session_id = self._session(label="ZIP_FIRST_FIXTURE")
        self._append(session_id, 0)
        self._append(session_id, 1)
        self._exec(
            """UPDATE remote_chunks SET status='done', updated_at=now()
               WHERE session_id=%s AND chunk_index=0""", (session_id,))
        self._ready_014()
        self.db.apply_container_format_schema()

        with self.assertRaisesRegex(Exception, "already backfilled"):
            self.db.apply_container_format_schema(
                exception_session_id=session_id, expected_boundary=1,
                approval_id="too-late",
                approval_reason="normal ZIP assignment is permanently write-once",
                staging_evidence=self._staging_evidence([1]))
        self.assertEqual(self._query(
            """SELECT array_agg(packaging_format ORDER BY chunk_index) AS formats
               FROM remote_chunks WHERE session_id=%s""", (session_id,)
        )[0]["formats"], ["zip", "zip"])
        self.assertEqual(self._query(
            """SELECT count(*) AS n FROM remote_packaging_boundaries
               WHERE session_id=%s""", (session_id,))[0]["n"], 0)

    def test_container_artifact_and_generation_constraints_fail_closed(self):
        session_id = self._session()
        self._append(session_id, 0)
        self._ready_014()
        self.db.apply_container_format_schema()

        container = self.db.create_archive_container({
            "session_id": session_id, "chunk_index": 0,
            "container_ordinal": 0, "container_format": "zip",
            "format_version": "zip-v1", "tar_dialect": None,
            "storage_class": "small_files", "container_name": "legacy.zip",
            "expected_member_count": 2, "expected_logical_bytes": 3,
        })
        self.assertGreater(container["container_id"], 0)
        with self.assertRaises(Exception):
            self.db.create_archive_container({
                "session_id": session_id, "chunk_index": 0,
                "container_ordinal": 0, "container_format": "zip",
                "format_version": "zip-v2", "tar_dialect": None,
                "storage_class": "small_files",
                "container_name": "conflict.zip",
                "expected_member_count": 2, "expected_logical_bytes": 3,
            })
        with self.assertRaises(Exception):
            self.db.create_archive_container({
                "session_id": session_id, "chunk_index": 0,
                "container_ordinal": 1, "container_format": "stored_tar",
                "format_version": "stored-tar-v1",
                "tar_dialect": "gnu-pax-sparse-v1",
                "storage_class": "small_files", "container_name": "bad.tar",
                "expected_member_count": 2, "expected_logical_bytes": 3,
            })

        with self.assertRaises(errors.CheckViolation):
            self._exec(
                """INSERT INTO archive_artifacts
                       (session_id,chunk_index,container_id,artifact_kind,
                        artifact_version,readiness_state)
                   VALUES (%s,0,%s,'zip_manifest','v1','ready')""",
                (session_id, container["container_id"]))
        artifact = self.db.create_archive_artifact({
            "session_id": session_id, "chunk_index": 0,
            "container_id": container["container_id"],
            "artifact_kind": "zip_manifest", "artifact_version": "v1",
            "local_locator": "metadata/legacy.jsonl.zst",
            "artifact_size_bytes": 7, "readiness_state": "ready",
            "published_at": self._query("SELECT now() AS n")[0]["n"],
        })
        self.assertEqual(artifact["readiness_state"], "ready")
        with self.assertRaisesRegex(Exception, "conflicting fields"):
            self.db.create_archive_artifact({
                "session_id": session_id, "chunk_index": 0,
                "container_id": container["container_id"],
                "artifact_kind": "zip_manifest", "artifact_version": "v1",
                "local_locator": "metadata/conflict.jsonl.zst",
                "artifact_size_bytes": 7,
            })
        with self.assertRaisesRegex(Exception, "identity is immutable"):
            self._exec(
                """UPDATE archive_containers SET expected_member_count=3
                   WHERE container_id=%s""", (container["container_id"],))

        self.db.register_tape("OTHER_TAPE", 12000)
        other_generation = self._query(
            """SELECT generation_id FROM tape_generations
               WHERE volume_label='OTHER_TAPE' AND state='active'""")[0]
        with self.assertRaisesRegex(Exception, "does not match"):
            self._exec(
                """INSERT INTO archive_containers
                       (session_id,chunk_index,container_ordinal,
                        container_format,format_version,storage_class,
                        container_name,tape_label,tape_path,tape_generation_id,
                        expected_member_count,expected_logical_bytes)
                   VALUES (%s,0,2,'zip','zip-v1','small_files','other.zip',
                           'OTHER_TAPE','other.zip',%s,1,1)""",
                (session_id, other_generation["generation_id"]))

    def test_optional_007_is_augmented_but_never_silently_installed(self):
        self._ready_014()
        self.db.apply_container_format_schema()
        self.assertFalse(self.db.directory_catalog_schema_installed())
        self.assertEqual(
            self.db.container_format_schema_report()["optional_007_state"],
            "absent")

    def test_installed_007_receives_nullable_compatibility_columns(self):
        self.db.apply_directory_catalog_schema()
        self._ready_014()
        self.db.apply_container_format_schema()
        columns = {row["column_name"] for row in self._query(
            """SELECT column_name FROM information_schema.columns
               WHERE table_schema='public'
                 AND table_name='directory_archive_bundles'""")}
        self.assertTrue({
            "container_id", "container_format", "tape_generation_id",
            "actual_artifact_bytes"}.issubset(columns))
        self.assertEqual(
            self.db.container_format_schema_report()["optional_007_state"],
            "installed")

    def test_partial_007_blocks_and_rolls_back_015(self):
        self._exec("CREATE TABLE directory_archive_stats(dummy INTEGER)")
        self._ready_014()
        with self.assertRaisesRegex(Exception, "partially installed"):
            self.db.apply_container_format_schema()
        self.assertIsNone(self._query(
            "SELECT to_regclass('archive_containers') AS r")[0]["r"])

    def test_remote_archive_run_identity_and_schema_drift_validation(self):
        session_id = self._session()
        self._append(session_id, 0)
        self._ready_014()
        self.db.apply_container_format_schema()
        generation = self._query(
            """SELECT generation_id FROM tape_generations
               WHERE volume_label='FORMAT_TAPE' AND state='active'""")[0]
        self._exec(
            """INSERT INTO archive_runs
                   (run_label,tape_label,session_kind,remote_session_id,
                    remote_chunk_index,tape_generation_id,started_at)
               VALUES ('stable-run-1','FORMAT_TAPE','remote',%s,0,%s,now())""",
            (session_id, generation["generation_id"]))
        with self.assertRaises(errors.UniqueViolation):
            self._exec(
                """INSERT INTO archive_runs
                       (run_label,tape_label,session_kind,remote_session_id,
                        remote_chunk_index,tape_generation_id,started_at)
                   VALUES ('stable-run-2','FORMAT_TAPE','remote',%s,0,%s,now())""",
                (session_id, generation["generation_id"]))

        self._exec(
            "DROP TRIGGER trg_remote_chunks_packaging_write_once "
            "ON remote_chunks")
        report = self.db.container_format_schema_report()
        self.assertFalse(report["ready"])
        self.assertIn(
            "missing trigger: trg_remote_chunks_packaging_write_once",
            report["issues"])
        with self.assertRaisesRegex(RuntimeError, "missing or drifted"):
            self.db.validate_container_format_schema()

    def test_partial_015_is_rejected_instead_of_adopted(self):
        self._ready_014()
        self._exec(
            "ALTER TABLE remote_chunks ADD COLUMN packaging_format TEXT")
        report = self.db.container_format_schema_report()
        self.assertEqual(report["installation_state"], "partial")
        with self.assertRaisesRegex(RuntimeError, "partial or drifted"):
            self.db.apply_container_format_schema()
        self.assertIsNone(self._query(
            "SELECT to_regclass('container_format_schema_metadata') AS r"
        )[0]["r"])

    def test_boundary_approval_and_audit_rows_are_immutable(self):
        session_id = self._session(label="IMMUTABLE_BOUNDARY_FIXTURE")
        self._append(session_id, 0)
        self._append(session_id, 1)
        self._catalog_done_prefix(session_id, 0)
        self._ready_014()
        self.db.apply_container_format_schema(
            exception_session_id=session_id, expected_boundary=1,
            approval_id="immutable-boundary",
            approval_reason="synthetic immutable boundary fixture",
            staging_evidence=self._staging_evidence([1]))

        for statement, params in (
            ("UPDATE remote_packaging_boundaries SET approval_id='changed' "
             "WHERE session_id=%s", (session_id,)),
            ("DELETE FROM remote_packaging_boundary_chunks "
             "WHERE session_id=%s AND chunk_index=1", (session_id,)),
            ("UPDATE remote_sessions SET default_packaging_format='zip' "
             "WHERE session_id=%s", (session_id,)),
        ):
            with self.subTest(statement=statement):
                with self.assertRaisesRegex(Exception, "immutable"):
                    self._exec(statement, params)

        other_session = self._session(
            label="FABRICATED_BOUNDARY_FIXTURE", tape="FORMAT_TAPE_2")
        with self.assertRaisesRegex(Exception, "immutable"):
            self._exec(
                """INSERT INTO remote_packaging_boundaries
                       (session_id,first_stored_tar_chunk_index,
                        last_existing_chunk_index,approval_id,approval_reason,
                        database_evidence,local_staging_evidence,
                        evidence_checked_at)
                   VALUES (%s,0,0,'fabricated','fabricated','{}','{}',now())""",
                (other_session,))

    def test_stale_or_noncanonical_staging_attestation_is_rejected(self):
        from datetime import datetime, timedelta, timezone

        session_id = self._session(label="STALE_EVIDENCE_FIXTURE")
        self._append(session_id, 0)
        self._ready_014()
        stale = self._staging_evidence([0])
        stale["checked_at"] = (
            datetime.now(timezone.utc) - timedelta(minutes=6)).isoformat()
        with self.assertRaisesRegex(ValueError, "stale"):
            self.db.apply_container_format_schema(
                exception_session_id=session_id, expected_boundary=0,
                approval_id="stale-evidence",
                approval_reason="synthetic stale evidence fixture",
                staging_evidence=stale)
        extra = self._staging_evidence([0])
        extra["local_path"] = "must-not-be-persisted"
        with self.assertRaisesRegex(ValueError, "aggregate fields"):
            self.db.apply_container_format_schema(
                exception_session_id=session_id, expected_boundary=0,
                approval_id="extra-evidence",
                approval_reason="synthetic extra evidence fixture",
                staging_evidence=extra)

    def test_ambiguous_catalog_provenance_blocks_exception(self):
        session_id = self._session(label="AMBIGUOUS_PROVENANCE_FIXTURE")
        self._append(session_id, 0)
        self._append(session_id, 1)
        self._catalog_done_prefix(session_id, 0)
        self._exec(
            """UPDATE files_index SET remote_chunk_index=NULL
               WHERE remote_session_id=%s""", (session_id,))
        self._ready_014()
        classification = self.db.classify_format_boundary(session_id)
        self.assertTrue(any("lack trustworthy remote chunk provenance" in item
                            for item in classification["blocking"]))
        with self.assertRaisesRegex(Exception, "trustworthy remote chunk"):
            self.db.apply_container_format_schema(
                exception_session_id=session_id, expected_boundary=1,
                approval_id="ambiguous-provenance",
                approval_reason="synthetic ambiguous provenance fixture",
                staging_evidence=self._staging_evidence([1]))

    def test_definition_fingerprint_detects_disabled_trigger(self):
        session_id = self._session(label="FINGERPRINT_FIXTURE")
        self._append(session_id, 0)
        self._ready_014()
        self.db.apply_container_format_schema()
        self._exec(
            "ALTER TABLE remote_chunks DISABLE TRIGGER "
            "trg_remote_chunks_packaging_write_once")
        report = self.db.container_format_schema_report()
        self.assertFalse(report["ready"])
        self.assertIn(
            "container format catalog-definition fingerprint drift",
            report["issues"])

    def _validated_tar_publication_fixture(self):
        rows = [
            (1, "/fixture/a.bin", "a.bin", 3),
            (1, "/fixture/missing.bin", "missing.bin", 5),
        ]
        session_id = self._stored_tar_planning_session(rows)
        plan = self.db.get_or_create_stored_tar_chunk_plan(
            session_id, 1, self.db.get_chunk_files(session_id, 1),
            loose_threshold_bytes=100, max_size_bytes=512 * 512)
        container = self.db.get_archive_containers(session_id, 1)[0]
        part = os.path.join(
            self.staging.name, "container.tar.owner.part")
        owner = "task-2-4-owner"
        self.db.claim_stored_tar_container_build(
            container["container_id"], owner, part)
        summary = {
            "container_ordinal": 0,
            "member_count": 1,
            "logical_bytes": 3,
            "archive_size": 262144,
            "plan_ordinal_count": 2,
            "disposition_counts": {
                "archived": 1, "source_missing": 1,
                "source_permission_denied": 0, "source_unreadable": 0,
                "source_changed": 0, "unresolved": 0,
            },
            "members": [],
        }
        diagnostics = [{
            "plan_ordinal": 1, "path": "missing.bin",
            "disposition": "source_missing", "evidence": "lstat_missing",
        }]
        self.db.mark_stored_tar_validated_part(
            container["container_id"], owner, part, summary, diagnostics)
        return session_id, plan, container, part, owner, summary

    def test_017_two_build_owners_cannot_adopt_the_same_part(self):
        rows = [(1, "/fixture/a.bin", "a.bin", 3)]
        session_id = self._stored_tar_planning_session(rows)
        self.db.get_or_create_stored_tar_chunk_plan(
            session_id, 1, self.db.get_chunk_files(session_id, 1),
            loose_threshold_bytes=100, max_size_bytes=512 * 512)
        container = self.db.get_archive_containers(session_id, 1)[0]
        part = os.path.join(self.staging.name, "same.tar.part")
        first = self.db.claim_stored_tar_container_build(
            container["container_id"], "owner-one", part)
        self.assertEqual(first["owner_token"], "owner-one")
        with self.assertRaisesRegex(RuntimeError, "another builder"):
            self.db.claim_stored_tar_container_build(
                container["container_id"], "owner-two", part)
        replay = self.db.claim_stored_tar_container_build(
            container["container_id"], "owner-one", part)
        self.assertEqual(replay["validated_part_locator"], part)

    def test_017_preserves_preexisting_ready_zip_container_contract(self):
        session_id = self._session(label="READY_ZIP_BEFORE_017")
        self._append(session_id, 0, member_count=1)
        self._ready_014()
        self.db.apply_container_format_schema()
        created = self.db.create_archive_container({
            "session_id": session_id, "chunk_index": 0,
            "container_ordinal": 0, "container_format": "zip",
            "format_version": "zip-v1", "tar_dialect": None,
            "storage_class": "small_files", "container_name": "one.zip",
            "expected_member_count": 1, "expected_logical_bytes": 1,
            "observed_member_count": 1, "observed_logical_bytes": 1,
            "actual_artifact_bytes": 10, "validation_state": "ready",
        })
        self.db.apply_stored_tar_plan_schema()
        row = self.db.get_archive_containers(session_id, 0)[0]
        self.assertEqual(row["container_id"], created["container_id"])
        self.assertEqual(row["validation_state"], "ready")
        self.assertIsNone(row["validation_summary"])
        self.assertIsNone(row["disposition_counts"])

    def test_017_validated_part_persists_no_final_or_ready_locator(self):
        _session, _plan, container, part, owner, summary = (
            self._validated_tar_publication_fixture())
        row = self.db.get_archive_containers(
            container["session_id"], container["chunk_index"])[0]
        self.assertEqual(row["validation_state"], "validated_part")
        self.assertEqual(row["validated_part_locator"], part)
        self.assertIsNone(row["temporary_data_locator"])
        self.assertIsNone(row["permanent_local_metadata_locator"])
        self.assertEqual(row["validation_summary"]["member_count"], 1)
        replay = self.db.mark_stored_tar_validated_part(
            container["container_id"], owner, part, summary,
            [{"plan_ordinal": 1, "path": "missing.bin",
              "disposition": "source_missing", "evidence": "lstat_missing"}])
        self.assertEqual(replay["validation_state"], "validated_part")

    def test_017_pair_becomes_ready_in_one_owner_checked_transaction(self):
        session, _plan, container, _part, owner, _summary = (
            self._validated_tar_publication_fixture())
        values = {
            "container_id": container["container_id"],
            "owner_token": owner,
            "sidecar_locator": "tar_sidecars/s/c.jsonl.zst",
            "sidecar_version": "tar-sidecar-v1",
            "sidecar_size_bytes": 123,
            "temporary_data_locator": os.path.join(
                self.staging.name, "container.tar"),
            "tar_size_bytes": 262144,
            "observed_member_count": 1,
            "observed_logical_bytes": 3,
            "disposition_counts": {
                "archived": 1, "source_missing": 1,
                "source_permission_denied": 0, "source_unreadable": 0,
                "source_changed": 0, "unresolved": 0,
            },
        }
        published = self.db.publish_stored_tar_pair(**values)
        self.assertEqual(
            published["container"]["validation_state"], "ready")
        self.assertEqual(
            published["artifact"]["readiness_state"], "ready")
        self.assertIsNone(published["container"]["owner_token"])
        artifacts = self.db.get_archive_artifacts(session, 1)
        self.assertEqual(len(artifacts), 1)
        self.assertEqual(artifacts[0]["local_locator"], values["sidecar_locator"])
        replay = self.db.publish_stored_tar_pair(**{
            **values, "owner_token": "recovery-owner"})
        self.assertEqual(replay["container"]["validation_state"], "ready")

    def test_017_conflicting_sidecar_rolls_back_the_pair(self):
        session, _plan, container, _part, owner, _summary = (
            self._validated_tar_publication_fixture())
        self.db.create_archive_artifact({
            "session_id": session, "chunk_index": 1,
            "container_id": container["container_id"],
            "artifact_kind": "tar_sidecar",
            "artifact_version": "tar-sidecar-v1",
            "local_locator": "conflicting/sidecar.jsonl.zst",
        })
        with self.assertRaisesRegex(RuntimeError, "conflicts on local_locator"):
            self.db.publish_stored_tar_pair(
                container_id=container["container_id"], owner_token=owner,
                sidecar_locator="tar_sidecars/s/c.jsonl.zst",
                sidecar_version="tar-sidecar-v1", sidecar_size_bytes=123,
                temporary_data_locator=os.path.join(
                    self.staging.name, "container.tar"),
                tar_size_bytes=262144, observed_member_count=1,
                observed_logical_bytes=3,
                disposition_counts={
                    "archived": 1, "source_missing": 1,
                    "source_permission_denied": 0, "source_unreadable": 0,
                    "source_changed": 0, "unresolved": 0})
        row = self.db.get_archive_containers(session, 1)[0]
        self.assertEqual(row["validation_state"], "validated_part")
        artifact = self.db.get_archive_artifacts(session, 1)[0]
        self.assertEqual(artifact["readiness_state"], "planned")
        self.assertEqual(
            artifact["local_locator"], "conflicting/sidecar.jsonl.zst")

    def test_017_reconciliation_owner_reset_and_ready_block_are_cas_guarded(self):
        session, _plan, container, part, owner, summary = (
            self._validated_tar_publication_fixture())
        adopted = self.db.reconcile_stored_tar_build_owner(
            container["container_id"], owner, "validated_part",
            "startup-reconciler")
        self.assertEqual(adopted["owner_token"], "startup-reconciler")
        with self.assertRaisesRegex(RuntimeError, "owner/state CAS failed"):
            self.db.reconcile_stored_tar_build_owner(
                container["container_id"], owner, "validated_part",
                "racing-reconciler")
        reset = self.db.reset_reconciled_stored_tar_build(
            container["container_id"], "startup-reconciler",
            "validated_part")
        self.assertEqual(reset["validation_state"], "planned")
        self.assertIsNone(reset["owner_token"])
        self.assertIsNone(reset["validated_part_locator"])

        ready_session = session
        ready_container = container
        ready_owner = "ready-after-reconcile"
        self.db.claim_stored_tar_container_build(
            ready_container["container_id"], ready_owner, part)
        self.db.mark_stored_tar_validated_part(
            ready_container["container_id"], ready_owner, part, summary,
            [{"plan_ordinal": 1, "path": "missing.bin",
              "disposition": "source_missing",
              "evidence": "lstat_missing"}])
        values = {
            "container_id": ready_container["container_id"],
            "owner_token": ready_owner,
            "sidecar_locator": "tar_sidecars/reconcile/ready.jsonl.zst",
            "sidecar_version": "tar-sidecar-v1",
            "sidecar_size_bytes": 123,
            "temporary_data_locator": os.path.join(
                self.staging.name, "ready-container.tar"),
            "tar_size_bytes": 262144,
            "observed_member_count": 1,
            "observed_logical_bytes": 3,
            "disposition_counts": {
                "archived": 1, "source_missing": 1,
                "source_permission_denied": 0,
                "source_unreadable": 0, "source_changed": 0,
                "unresolved": 0,
            },
        }
        self.db.publish_stored_tar_pair(**values)
        blocked = self.db.block_stored_tar_container(
            ready_container["container_id"], "ready", None)
        self.assertEqual(blocked["validation_state"], "blocked")
        artifacts = self.db.get_archive_artifacts(ready_session, 1)
        self.assertEqual(artifacts[0]["readiness_state"], "blocked")
        replay = self.db.block_stored_tar_container(
            ready_container["container_id"], "ready", None)
        self.assertEqual(replay["validation_state"], "blocked")


if __name__ == "__main__":
    unittest.main()
