"""The SQLite backend as a product: configuration, automatic schema, backup,
the PostgreSQL-only refusals, and process-level guarantees.

Everything here runs without a PostgreSQL server and creates its database
files only in private temporary directories.
"""
import configparser
import io
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import textwrap
import time
import unittest
from contextlib import redirect_stderr, redirect_stdout
from types import SimpleNamespace
from unittest import mock

from db_backends import SQLITE, create_test_db

from src import db_backend
from src.db_backend import POSTGRESQL_ONLY_FEATURES, PostgresOnlyFeature

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _real_python():
    """The base interpreter, not the venv launcher stub (which starts the
    real interpreter as a CHILD: killing the stub would leave it running)."""
    return getattr(sys, "_base_executable", None) or sys.executable


def _subprocess_env():
    env = dict(os.environ)
    paths = [REPO_ROOT] + [p for p in sys.path if p]
    env["PYTHONPATH"] = os.pathsep.join(paths)
    env.pop("LTO_TEST_PG_DSN", None)
    return env


def _cfg(tdb, **extra):
    """A config stand-in for the code under test (no config.ini involved)."""
    values = dict(db_backend="sqlite", db_dsn=tdb.conninfo,
                  sqlite_path=tdb.path, db_display_ref=tdb.conninfo,
                  lto_drive="", pg_dbname="unused")
    values.update(extra)
    return SimpleNamespace(**values)


# ---------------------------------------------------------------------------
# Configuration and dispatch
# ---------------------------------------------------------------------------

class ConfigBackendSelectionTests(unittest.TestCase):
    def _config(self, text):
        from src.config import ConfigManager
        manager = ConfigManager.__new__(ConfigManager)
        manager.config = configparser.ConfigParser()
        manager.config.read_string(text)
        manager.env = {}
        return manager

    def test_a_config_without_a_backend_key_stays_on_postgresql(self):
        """An existing install has PostgreSQL settings and no backend key."""
        cfg = self._config(
            "[DATABASE]\nhost = localhost\nport = 5432\ndbname = lto_archive\n"
            "user = lto\nsslmode = prefer\n")
        self.assertEqual(cfg.db_backend, "postgresql")
        self.assertTrue(cfg.db_dsn.startswith("postgresql://"))
        self.assertEqual(db_backend.backend_of(cfg), "postgresql")

    def test_a_config_with_no_database_section_stays_on_postgresql(self):
        cfg = self._config("[PATHS]\nsource_dir = x\n")
        self.assertEqual(cfg.db_backend, "postgresql")

    def test_backend_sqlite_selects_a_file_in_the_project_root_by_default(self):
        from src.constants import PROJECT_ROOT
        cfg = self._config("[DATABASE]\nbackend = sqlite\n")
        self.assertEqual(cfg.db_backend, "sqlite")
        self.assertEqual(
            cfg.sqlite_path,
            os.path.abspath(os.path.join(PROJECT_ROOT, "lto_archive.sqlite3")))
        self.assertTrue(cfg.db_dsn.startswith("sqlite:///"))
        self.assertEqual(cfg.db_display_ref, cfg.db_dsn)
        self.assertEqual(db_backend.backend_of(cfg), "sqlite")

    def test_sqlite_path_may_be_absolute(self):
        target = os.path.join(tempfile.gettempdir(), "elsewhere.sqlite3")
        cfg = self._config(
            f"[DATABASE]\nbackend = sqlite\nsqlite_path = {target}\n")
        self.assertEqual(cfg.sqlite_path, os.path.abspath(target))

    def test_an_unknown_backend_is_a_configuration_error(self):
        cfg = self._config("[DATABASE]\nbackend = mysql\n")
        with self.assertRaises(Exception) as caught:
            cfg.db_backend
        self.assertIn("backend", str(caught.exception))

    def test_the_generated_default_and_the_example_select_sqlite(self):
        import inspect
        from src.config import ConfigManager
        source = inspect.getsource(ConfigManager._create_default)
        self.assertIn("'backend': 'sqlite'", source)
        example = configparser.ConfigParser()
        example.read(os.path.join(REPO_ROOT, "config.example.ini"),
                     encoding="utf-8")
        self.assertEqual(example.get("DATABASE", "backend"), "sqlite")
        self.assertEqual(example.get("DATABASE", "sqlite_path"),
                         "lto_archive.sqlite3")

    def test_the_default_sqlite_file_is_gitignored(self):
        with open(os.path.join(REPO_ROOT, ".gitignore"),
                  encoding="utf-8") as handle:
            patterns = {line.strip() for line in handle}
        self.assertIn("*.sqlite3", patterns)


class AutomaticSchemaTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="lto_sqlite_auto_")
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.path = os.path.join(self.tmp, "fresh.sqlite3")
        self.dsn = "sqlite:///" + self.path.replace("\\", "/")

    def test_create_database_manager_creates_file_and_complete_schema(self):
        from src.db import create_database_manager
        from src.sqlite_db import SqliteDatabaseManager

        self.assertFalse(os.path.exists(self.path))
        db = create_database_manager(SimpleNamespace(
            db_backend="sqlite", db_dsn=self.dsn))
        try:
            self.assertIsInstance(db, SqliteDatabaseManager)
            self.assertTrue(os.path.isfile(self.path))
            status = db.sqlite_schema_status()
            self.assertEqual(status["schema_version"], 1)
            self.assertFalse(status["drift"])
            # Every "explicit migration" of the PostgreSQL history is present.
            self.assertTrue(db.directory_catalog_schema_installed())
            self.assertTrue(db.incremental_scan_schema_installed())
            self.assertTrue(db.incremental_scan_schema_finalized())
            self.assertTrue(db.container_format_schema_report()["ready"])
            self.assertTrue(db.stored_tar_plan_schema_report()["ready"])
            self.assertTrue(
                db.manifest_directory_catalog_schema_report()["ready"])
            self.assertTrue(
                db.container_format_schema_authority_v2_report()["ready"])
            self.assertTrue(db.validate_container_format_authority())
            for apply in (db.apply_directory_catalog_schema,
                          db.apply_container_format_schema,
                          db.apply_stored_tar_plan_schema,
                          db.apply_manifest_directory_catalog_schema,
                          db.apply_container_format_schema_authority_v2):
                self.assertIn("installed at creation", apply()[0])
            self.assertIn(
                "installed at creation",
                db.apply_incremental_scan_schema(finalize=True)[0])
        finally:
            db.close()

    def test_reopening_applies_nothing_and_keeps_the_data(self):
        from src.sqlite_db import SqliteDatabaseManager
        db = SqliteDatabaseManager(self.dsn)
        db.register_tape("KEEP_ME", 100)
        before = db.sqlite_schema_status()["migrations"]
        db.close()
        db = SqliteDatabaseManager(self.dsn)
        try:
            self.assertTrue(db.tape_exists("KEEP_ME"))
            self.assertEqual(db.sqlite_schema_status()["migrations"], before)
        finally:
            db.close()

    def test_bootstrap_settings_are_never_left_in_the_database(self):
        from src.sqlite_db import SqliteDatabaseManager
        SqliteDatabaseManager(self.dsn).close()
        raw = sqlite3.connect(self.path)
        try:
            self.assertEqual(
                raw.execute("SELECT count(*) FROM lto_txn_settings"
                            ).fetchone()[0], 0)
        finally:
            raw.close()

    def test_schema_drift_is_refused_at_open_and_by_the_write_gate(self):
        from src.sqlite_db import SqliteDatabaseManager
        db = SqliteDatabaseManager(self.dsn)
        raw = sqlite3.connect(self.path)
        try:
            raw.execute("DROP TRIGGER trg_remote_chunks_status_transition")
            raw.commit()
        finally:
            raw.close()
        try:
            self.assertTrue(db.sqlite_schema_status()["drift"])
            report = db.container_format_schema_report()
            self.assertFalse(report["ready"])
            with self.assertRaisesRegex(RuntimeError, "drift"):
                db.validate_container_format_authority()
        finally:
            db.close()
        with self.assertRaisesRegex(RuntimeError, "fingerprint drift"):
            SqliteDatabaseManager(self.dsn)

    def test_an_unrelated_table_in_the_file_is_not_drift(self):
        from src.sqlite_db import SqliteDatabaseManager
        SqliteDatabaseManager(self.dsn).close()
        raw = sqlite3.connect(self.path)
        raw.execute("CREATE TABLE operator_notes (note TEXT)")
        raw.commit()
        raw.close()
        db = SqliteDatabaseManager(self.dsn)
        try:
            self.assertFalse(db.sqlite_schema_status()["drift"])
        finally:
            db.close()

    def test_a_foreign_database_file_is_refused_not_adopted(self):
        from src.sqlite_db import SqliteDatabaseManager
        raw = sqlite3.connect(self.path)
        raw.execute("PRAGMA journal_mode=WAL")
        raw.execute("CREATE TABLE something_else (x)")
        raw.commit()
        raw.close()
        with self.assertRaisesRegex(RuntimeError, "not a catalog"):
            SqliteDatabaseManager(self.dsn)

    def test_a_changed_migration_file_is_refused(self):
        from src import sqlite_core
        from src.sqlite_db import SqliteDatabaseManager
        SqliteDatabaseManager(self.dsn).close()
        with mock.patch.object(sqlite_core, "_file_checksum",
                               return_value="0" * 64):
            with self.assertRaisesRegex(RuntimeError, "checksum mismatch"):
                SqliteDatabaseManager(self.dsn)

    def test_init_schema_false_never_creates_a_database(self):
        from src.sqlite_db import SqliteDatabaseManager
        with self.assertRaisesRegex(RuntimeError, "does not exist"):
            SqliteDatabaseManager(self.dsn, init_schema=False)
        self.assertFalse(os.path.exists(self.path))

    def test_a_read_only_manager_cannot_write(self):
        from src.sqlite_db import SqliteDatabaseManager
        SqliteDatabaseManager(self.dsn).close()
        db = SqliteDatabaseManager(self.dsn, read_only=True)
        try:
            self.assertEqual(db.list_tapes(), [])
            with self.assertRaises(Exception):
                db.register_tape("NOPE", 1)
        finally:
            db.close()
        check = SqliteDatabaseManager(self.dsn)
        try:
            self.assertFalse(check.tape_exists("NOPE"))
        finally:
            check.close()


# ---------------------------------------------------------------------------
# Guards that PostgreSQL enforces in plpgsql
# ---------------------------------------------------------------------------

class PortedGuardTests(unittest.TestCase):
    def setUp(self):
        self.tdb = create_test_db(SQLITE, "guards")
        self.addCleanup(self.tdb.drop)
        self.db = self.tdb.manager()
        self.db.register_tape("G1", 100)
        self.session_id = self.tdb.run(
            """INSERT INTO remote_sessions (session_label,remote_host,
                   remote_user,remote_path,tape_label,staging_dir,total_files,
                   total_bytes,chunk_count,created_at,status,tape_generation)
               VALUES ('G','h','u','/r','G1','C:\\stg',1,1,1,now(),'active',1)
               RETURNING session_id""")[0]["session_id"]
        self.tdb.insert_remote_chunk(self.session_id, 0, "pending")

    def _refused(self, sql, params=(), message=None):
        with self.assertRaises(self.tdb.errors.GuardViolation) as caught:
            self.tdb.run(sql, params)
        if message:
            self.assertIn(message, str(caught.exception))

    def test_packaging_format_is_write_once(self):
        self._refused(
            "UPDATE remote_chunks SET packaging_format='stored_tar' "
            "WHERE session_id=%s", (self.session_id,), "write-once")

    def test_a_new_chunk_must_carry_the_sessions_durable_format(self):
        self._refused(
            """INSERT INTO remote_chunks (session_id,chunk_index,status,
                   updated_at,packaging_format,packaging_assigned_at)
               VALUES (%s,5,'pending',now(),'stored_tar',now())""",
            (self.session_id,), "initial chunk format")

    def test_a_session_cannot_default_to_stored_tar(self):
        self._refused(
            """INSERT INTO remote_sessions (session_label,remote_host,
                   remote_user,remote_path,tape_label,staging_dir,created_at,
                   status,tape_generation,default_packaging_format)
               VALUES ('S2','h','u','/r','G1','C:\\stg',now(),'active',1,
                       'stored_tar')""", (), "default to zip")
        self._refused(
            "UPDATE remote_sessions SET default_packaging_format='stored_tar' "
            "WHERE session_id=%s", (self.session_id,), "immutable")

    def test_schema_authority_rows_are_immutable(self):
        for table in ("container_format_schema_metadata",
                      "manifest_directory_catalog_schema_metadata",
                      "container_format_schema_authority"):
            with self.subTest(table=table):
                self._refused(f"UPDATE {table} SET applied_at=now()")
                self._refused(f"DELETE FROM {table}")
        self._refused(
            """INSERT INTO container_format_schema_metadata
               (singleton,schema_version,required_reader_contract_version,
                stored_tar_dialect,migration_checksum,schema_fingerprint)
               VALUES (1,1,1,'x',%s,%s)""", ("a" * 64, "b" * 32))

    def test_packaging_boundaries_refuse_every_write(self):
        self._refused(
            """INSERT INTO remote_packaging_boundaries
               (session_id,first_stored_tar_chunk_index,
                last_existing_chunk_index,approval_id,approval_reason,
                database_evidence,local_staging_evidence,evidence_checked_at)
               VALUES (%s,1,0,'a','r','{}','{}',now())""",
            (self.session_id,), "immutable")

    def test_plan_source_cannot_change_without_the_audited_transition(self):
        self._refused(
            "UPDATE remote_chunks SET plan_source='manifest' "
            "WHERE session_id=%s", (self.session_id,))
        self._refused(
            """INSERT INTO remote_chunk_plan_source_transitions
               (session_id,chunk_index,from_plan_source,to_plan_source,
                new_plan_manifest_artifact_id,plan4_gate_id,plan4_evidence_id,
                approval_identity,equivalence_confirmed)
               VALUES (%s,0,'legacy_db','manifest',1,'g','e','a',1)""",
            (self.session_id,), "append-only")

    def test_a_manifest_chunk_needs_a_ready_plan_artifact(self):
        self._refused(
            """INSERT INTO remote_chunks (session_id,chunk_index,status,
                   updated_at,packaging_format,packaging_assigned_at,
                   plan_source)
               VALUES (%s,7,'pending',now(),'zip',now(),'manifest')""",
            (self.session_id,))

    def test_assign_new_chunk_format_follows_the_plpgsql_rules(self):
        from src.pipeline_types import ContainerFormat
        self.assertIs(
            self.db.assign_new_chunk_format(self.session_id, 3),
            ContainerFormat.ZIP)
        with self.assertRaisesRegex(Exception, "does not exist"):
            self.db.assign_new_chunk_format(987654, 0)

    def test_transition_chunk_plan_source_needs_a_real_chunk_and_ids(self):
        with self.assertRaises(ValueError):
            self.db.transition_chunk_plan_source(
                self.session_id, 0, 1, "", "e", "a")
        with self.assertRaises(Exception):
            # legacy_db chunk that is not terminal / has no ready artifact
            self.db.transition_chunk_plan_source(
                self.session_id, 0, 1, "gate", "evidence", "approver")
        self.assertEqual(self.tdb.scalar(
            "SELECT plan_source FROM remote_chunks WHERE session_id=%s",
            (self.session_id,)), "legacy_db")
        self.assertEqual(self.tdb.scalar(
            "SELECT count(*) AS n FROM lto_txn_settings"), 0)


# ---------------------------------------------------------------------------
# PostgreSQL-only features refuse with ONE error class
# ---------------------------------------------------------------------------

class PostgresOnlyRefusalTests(unittest.TestCase):
    def setUp(self):
        self.tdb = create_test_db(SQLITE, "pgonly")
        self.addCleanup(self.tdb.drop)
        self.db = self.tdb.manager()
        self.cfg = _cfg(self.tdb)

    def test_the_refusal_is_an_operator_facing_error_not_a_crash(self):
        from src.cli_errors import OperationalError
        error = PostgresOnlyFeature("schema_audit")
        self.assertIsInstance(error, OperationalError)
        self.assertIsInstance(error, RuntimeError)
        self.assertIn("Not available on the SQLite backend", str(error))
        self.assertIn("Nothing was changed", str(error))
        with self.assertRaises(KeyError):
            db_backend.require_postgresql(self.cfg, "no_such_feature")

    def test_every_listed_feature_refuses_on_sqlite_only(self):
        pg_cfg = SimpleNamespace(db_backend="postgresql",
                                 db_dsn="postgresql://u@h:1/d")
        for feature in POSTGRESQL_ONLY_FEATURES:
            with self.subTest(feature=feature):
                with self.assertRaises(PostgresOnlyFeature) as caught:
                    db_backend.require_postgresql(self.cfg, feature)
                self.assertEqual(caught.exception.feature, feature)
                db_backend.require_postgresql(pg_cfg, feature)

    def test_schema_audit_refuses(self):
        from src.schema_audit import audit_manager, audit_schema_provenance
        with self.assertRaises(PostgresOnlyFeature):
            audit_manager(self.db)
        with self.tdb.connect() as conn:
            with self.assertRaises(PostgresOnlyFeature):
                audit_schema_provenance(conn)

    def test_sealed_batches_refuse_and_observation_stays_off(self):
        from src.sealed_batch_observation import maybe_build_observation_worker
        from src.sealed_batch_repository import SealedBatchRepository
        with self.assertRaises(PostgresOnlyFeature):
            SealedBatchRepository(self.tdb.conninfo)
        out = io.StringIO()
        with redirect_stdout(out):
            worker = maybe_build_observation_worker(
                SimpleNamespace(
                    sealed_tape_write_batches_observation_enabled=True),
                self.tdb.conninfo)
        self.assertIsNone(worker)
        self.assertIn("Not available on the SQLite backend", out.getvalue())

    def test_manifest_maintenance_refuses_but_file_search_still_works(self):
        from src import local_manifest_archive as archive
        for call in (lambda: archive.dry_run_export(self.tdb.conninfo),
                     lambda: archive.export_status(self.tdb.conninfo),
                     lambda: archive.validate_export(self.tdb.conninfo, 1),
                     lambda: archive.prune_export(
                         self.tdb.conninfo, 1, "x.dump"),
                     lambda: archive.execute_export(
                         self.tdb.conninfo, "root", "x.dump")):
            with self.assertRaises(PostgresOnlyFeature):
                call()
        # What restore relies on: no database export, so nothing is pruned,
        # and the file-based search answers (empty) without an error.
        self.assertEqual(archive.pruned_manifest_paths(self.tdb.conninfo), [])
        self.assertEqual(list(self.db.list_pruned_manifest_segments()), [])
        root = tempfile.mkdtemp(prefix="lto_manifest_root_")
        self.addCleanup(shutil.rmtree, root, ignore_errors=True)
        self.assertEqual(archive.search_manifests(
            root, "anything", allowed_paths=[]), [])

    def test_stored_tar_boundary_paths_refuse(self):
        with self.assertRaises(PostgresOnlyFeature):
            self.db.classify_format_boundary(1)
        with self.assertRaises(PostgresOnlyFeature):
            self.db.apply_container_format_schema(
                exception_session_id=1, expected_boundary=1, approval_id="a",
                approval_reason="r", staging_evidence={})

    def test_pg_dump_options_refuse_through_the_backup_facade(self):
        from src import db_backup
        with self.assertRaises(PostgresOnlyFeature):
            db_backup.create_database_backup(self.cfg, snapshot_id="x")
        with self.assertRaises(PostgresOnlyFeature):
            db_backup.verify_backup(self.cfg, "x", deep_target=object())


class InspectDbOnSqliteTests(unittest.TestCase):
    def setUp(self):
        import inspect_db
        self.inspect_db = inspect_db
        self.tdb = create_test_db(SQLITE, "inspect")
        self.addCleanup(self.tdb.drop)
        self.tdb.manager().close()
        self.cfg = _cfg(self.tdb, local_manifest_archive_root="",
                        staging_dir="")

    def _run(self, *argv):
        out, err = io.StringIO(), io.StringIO()
        with mock.patch.object(self.inspect_db, "ConfigManager",
                               return_value=self.cfg), \
                redirect_stdout(out), redirect_stderr(err):
            code = self.inspect_db.main(list(argv))
        return code, out.getvalue(), err.getvalue()

    def test_postgresql_only_commands_exit_1_with_one_line_and_no_traceback(
            self):
        commands = (
            ["--backup-postgres"], ["--schema-provenance-audit"],
            ["--create-migrated-db", "--backup-file", "x.dump"],
            ["--compare-db", "a", "--with-db", "b"],
            ["--bootstrap-frontier", "--session-id", "1"],
            ["--backfill-legacy-chunk-membership", "--session-id", "1"],
            ["--persist-session-transition", "--session-id", "1"],
            ["--session37-boundary-rehearsal"],
            ["--backfill-directory-catalog", "--dry-run"],
            ["--manifest-status"], ["--export-small-file-manifests"],
            ["--validate-local-manifest-export"],
            ["--prune-exported-small-files"],
            ["--apply-container-format-schema",
             "--stored-tar-exception-session-id", "37"],
            ["--db", "another_database", "--print-db-target"],
        )
        for argv in commands:
            with self.subTest(command=argv[0]):
                code, out, err = self._run(*argv)
                self.assertEqual(code, 1)
                self.assertIn("Not available on the SQLite backend", err)
                self.assertNotIn("Traceback", err + out)
                self.assertEqual(len(err.strip().splitlines()), 1)

    def test_explicit_migration_commands_report_installed_at_creation(self):
        for flag in ("--apply-directory-catalog-schema",
                     "--apply-incremental-scan-schema",
                     "--apply-container-format-schema",
                     "--apply-container-format-schema-authority-v2",
                     "--apply-stored-tar-plan-schema",
                     "--apply-manifest-directory-catalog-schema"):
            with self.subTest(flag=flag):
                code, out, err = self._run(flag)
                self.assertEqual((code, err), (0, ""))
                payload = json.loads(out)
                self.assertEqual(payload["schema"], "installed at creation")
                self.assertEqual(payload["schema_version"], 1)
                self.assertEqual(payload["applied"], [])

    def test_read_only_reports_and_target_identity_work(self):
        code, out, _err = self._run("--print-db-target")
        self.assertEqual(code, 0)
        payload = json.loads(out)
        self.assertEqual(payload["archiver_lock_holders"], 0)
        self.assertEqual(
            os.path.normcase(payload["identity"]["database"]),
            os.path.normcase(self.tdb.path))
        for flag in ("--validate-container-format-schema",
                     "--container-format-schema-report",
                     "--validate-stored-tar-plan-schema",
                     "--validate-manifest-directory-catalog-schema",
                     "--validate-container-format-schema-authority-v2",
                     "--validate-directory-catalog", "--all-session-health"):
            with self.subTest(flag=flag):
                code, out, err = self._run(flag)
                self.assertEqual(code, 0, err)
                json.loads(out)


# ---------------------------------------------------------------------------
# Backup
# ---------------------------------------------------------------------------

class SqliteBackupTests(unittest.TestCase):
    def setUp(self):
        self.tdb = create_test_db(SQLITE, "backup")
        self.addCleanup(self.tdb.drop)
        self.db = self.tdb.manager()
        self.db.register_tape("BK1", 100)
        self.db.bulk_upsert_files([{
            "original_path": "/data/a.bin", "file_size_bytes": 10,
            "tape_label": "BK1", "source_host": "srv02", "is_packed": False,
            "container_name": None, "stored_path": "/data/a.bin"}])
        self.out = tempfile.mkdtemp(prefix="lto_sqlite_backup_")
        self.addCleanup(shutil.rmtree, self.out, ignore_errors=True)
        self.cfg = _cfg(self.tdb)

    def test_backup_of_a_live_catalog_is_a_complete_verified_copy(self):
        from src import db_backup
        from src.pg_backup import BACKUP_LEVEL_READ_THROUGH, BackupVerification

        self.db.acquire_archiver_lock()            # the catalog is in use
        path = db_backup.create_database_backup(self.cfg, output_dir=self.out)
        self.assertTrue(path.endswith(".sqlite3"))
        self.assertFalse(os.path.exists(path + ".part"))
        result = db_backup.verify_backup(self.cfg, path)
        self.assertIsInstance(result, BackupVerification)
        self.assertEqual(result.backend, "sqlite")
        self.assertTrue(result.ok, result.summary())
        self.assertEqual(result.level, BACKUP_LEVEL_READ_THROUGH)
        self.assertEqual(
            {check.name for check in result.checks},
            {"non_empty", "integrity_check", "foreign_key_check",
             "catalog_schema_present"})
        self.assertEqual(len(result.sha256), 64)
        self.assertTrue(result.not_verified)
        self.assertEqual(result.details["files_index_rows"], 1)
        self.assertIn("PASSED", result.summary())
        self.assertTrue(result.as_dict()["ok"])
        # The copy holds the data.
        raw = sqlite3.connect(path)
        try:
            self.assertEqual(raw.execute(
                "SELECT volume_label FROM tapes").fetchall(), [("BK1",)])
            self.assertEqual(raw.execute(
                "SELECT count(*) FROM files_index").fetchone()[0], 1)
        finally:
            raw.close()

    def test_a_damaged_backup_fails_verification(self):
        from src import db_backup
        path = db_backup.create_database_backup(self.cfg, output_dir=self.out)
        size = os.path.getsize(path)
        with open(path, "r+b") as handle:
            handle.seek(size // 2)
            handle.write(b"\xff" * 4096)
        result = db_backup.verify_backup(self.cfg, path)
        self.assertFalse(result.ok)
        self.assertIn("FAILED", result.summary())
        with self.assertRaises(RuntimeError):
            result.raise_if_failed()

    def test_an_empty_file_is_not_a_backup(self):
        from src import db_backup
        path = os.path.join(self.out, "empty.sqlite3")
        open(path, "wb").close()
        self.assertFalse(db_backup.verify_backup(self.cfg, path).ok)

    def test_the_menu_and_messages_do_not_say_postgresql_on_sqlite(self):
        from src import db_backup
        self.assertNotIn("PostgreSQL", db_backup.backup_menu_text(self.cfg))
        self.assertNotIn("PostgreSQL", db_backup.backup_noun(self.cfg))
        pg = SimpleNamespace(db_backend="postgresql",
                             db_dsn="postgresql://u@h:1/d")
        self.assertIn("PostgreSQL", db_backup.backup_menu_text(pg))
        self.assertEqual(db_backup.backup_noun(pg), "PostgreSQL dump")

    def test_a_missing_catalog_is_reported_clearly(self):
        from src import db_backup
        cfg = _cfg(self.tdb, sqlite_path=os.path.join(self.out, "none.db"))
        with self.assertRaisesRegex(RuntimeError, "does not exist yet"):
            db_backup.create_database_backup(cfg, output_dir=self.out)


# ---------------------------------------------------------------------------
# Standalone connections (GUI inspector, validation) are read-only
# ---------------------------------------------------------------------------

class StandaloneConnectionTests(unittest.TestCase):
    def setUp(self):
        self.tdb = create_test_db(SQLITE, "standalone")
        self.addCleanup(self.tdb.drop)
        self.db = self.tdb.manager()
        self.db.register_tape("RO1", 100)

    def test_the_inspector_repository_reads_and_cannot_write(self):
        from src.inspector_repository import InspectorRepository
        with InspectorRepository(self.tdb.conninfo) as repo:
            self.assertEqual(
                repo._execute("SELECT volume_label FROM tapes"
                              ).fetchone()["volume_label"], "RO1")
            with self.assertRaises(Exception):
                repo._execute("DELETE FROM tapes")
        self.assertTrue(self.db.tape_exists("RO1"))

    def test_the_lock_probe_sees_a_running_archiver(self):
        from src.directory_catalog_validation import archiver_lock_status
        self.assertEqual(archiver_lock_status(self.tdb.conninfo), 0)
        self.db.acquire_archiver_lock()
        self.assertEqual(archiver_lock_status(self.tdb.conninfo), 1)
        with self.db._pool.connection() as conn:
            self.assertTrue(self.db._archiver_lock_held_conn(conn))
            self.assertFalse(self.db._owns_archiver_lock_conn(conn))
        self.assertTrue(
            self.db._owns_archiver_lock_conn(self.db._lock_conn))
        self.db.release_archiver_lock()
        self.assertEqual(archiver_lock_status(self.tdb.conninfo), 0)

    def test_validation_reports_run_read_only(self):
        from src.directory_catalog_validation import (
            describe_database, row_counts, validate_directory_catalog)
        self.assertEqual(
            row_counts(self.tdb.conninfo, ("tapes", "no_such_table")),
            {"tapes": 1, "no_such_table": None})
        self.assertIsNone(
            describe_database(self.tdb.conninfo)["server_port"])
        self.assertIsInstance(
            validate_directory_catalog(self.tdb.conninfo), dict)


# ---------------------------------------------------------------------------
# Process-level guarantees
# ---------------------------------------------------------------------------

_HOLDER = textwrap.dedent("""
    import sys, time
    from src.sqlite_db import SqliteDatabaseManager
    db = SqliteDatabaseManager(sys.argv[1])
    db.acquire_archiver_lock()
    db.verify_archiver_lock()
    print("HELD", flush=True)
    time.sleep(float(sys.argv[2]))
""")

_CONTENDER = textwrap.dedent("""
    import sys
    from src.sqlite_db import SqliteDatabaseManager
    db = SqliteDatabaseManager(sys.argv[1])
    try:
        db.acquire_archiver_lock()
    except RuntimeError as exc:
        print("REFUSED:" + str(exc).splitlines()[0], flush=True)
        sys.exit(3)
    db.verify_archiver_lock()
    db.register_tape("SECOND_WRITER", 1)
    print("ACQUIRED", flush=True)
""")


class TwoProcessWriterTests(unittest.TestCase):
    def setUp(self):
        self.tdb = create_test_db(SQLITE, "twoproc")
        self.addCleanup(self.tdb.drop)
        self.tdb.manager().close()
        self.children = []
        self.addCleanup(self._reap)

    def _reap(self):
        for child in self.children:
            if child.poll() is None:
                child.kill()
            child.communicate()

    def _spawn(self, script, *args):
        child = subprocess.Popen(
            [_real_python(), "-c", script, self.tdb.conninfo, *args],
            cwd=REPO_ROOT, env=_subprocess_env(), stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True)
        self.children.append(child)
        return child

    def _holder(self, seconds=60):
        holder = self._spawn(_HOLDER, str(seconds))
        line = holder.stdout.readline().strip()
        if line != "HELD":
            self.fail("holder did not take the lock: "
                      + line + holder.stderr.read())
        return holder

    def test_a_second_archiver_process_is_refused(self):
        self._holder()
        contender = self._spawn(_CONTENDER)
        out, err = contender.communicate(timeout=60)
        self.assertEqual(contender.returncode, 3, out + err)
        self.assertIn("REFUSED:[LOCK] Another archiver instance", out)
        # The refused process wrote nothing.
        self.assertEqual(self.tdb.scalar(
            "SELECT count(*) AS n FROM tapes "
            "WHERE volume_label='SECOND_WRITER'"), 0)
        # ...and every "is an archiver running?" probe sees the holder.
        self.assertEqual(
            db_backend.archiver_lock_holders(self.tdb.conninfo), 1)
        db = self.tdb.manager(init_schema=False)
        with self.assertRaisesRegex(RuntimeError, "Another archiver"):
            db.acquire_archiver_lock()

    def test_a_killed_holder_never_leaves_the_lock_stuck(self):
        """Nobody has to walk to the machine: the OS drops the lock."""
        holder = self._holder()
        holder.kill()
        holder.communicate(timeout=30)
        deadline = time.monotonic() + 15
        while (db_backend.archiver_lock_holders(self.tdb.conninfo)
               and time.monotonic() < deadline):
            time.sleep(0.1)
        contender = self._spawn(_CONTENDER)
        out, err = contender.communicate(timeout=60)
        self.assertEqual(contender.returncode, 0, out + err)
        self.assertIn("ACQUIRED", out)
        self.assertEqual(self.tdb.scalar(
            "SELECT count(*) AS n FROM tapes "
            "WHERE volume_label='SECOND_WRITER'"), 1)

    def test_a_clean_exit_releases_the_lock(self):
        holder = self._holder(seconds=0.2)
        holder.communicate(timeout=30)
        self.assertEqual(holder.returncode, 0)
        self.assertEqual(
            db_backend.archiver_lock_holders(self.tdb.conninfo), 0)

    def test_concurrent_writers_in_two_processes_serialise_cleanly(self):
        """Ordinary catalog writes (no archiver lock) from two processes at
        once: nothing is lost and nothing is corrupted."""
        script = textwrap.dedent("""
            import sys
            from src.sqlite_db import SqliteDatabaseManager
            db = SqliteDatabaseManager(sys.argv[1])
            for index in range(40):
                db.register_tape(f"{sys.argv[2]}_{index:03d}", 1)
            db.close()
            print("DONE", flush=True)
        """)
        children = [self._spawn(script, name) for name in ("PA", "PB")]
        for child in children:
            out, err = child.communicate(timeout=120)
            self.assertEqual(child.returncode, 0, out + err)
        self.assertEqual(
            self.tdb.scalar("SELECT count(*) AS n FROM tapes"), 80)
        self.assertEqual(
            self.tdb.scalar("SELECT count(*) AS n FROM tape_generations"), 80)
        raw = sqlite3.connect(self.tdb.path)
        try:
            self.assertEqual(
                raw.execute("PRAGMA integrity_check").fetchone()[0], "ok")
        finally:
            raw.close()


_NO_PSYCOPG = textwrap.dedent("""
    import importlib.abc, os, sys, tempfile

    class _Block(importlib.abc.MetaPathFinder):
        def find_spec(self, name, path=None, target=None):
            if name.split(".")[0] in ("psycopg", "psycopg_pool",
                                      "psycopg_binary", "psycopg_c"):
                raise ImportError("psycopg is blocked for this test: " + name)
            return None

    sys.meta_path.insert(0, _Block())
    try:
        import psycopg
    except ImportError:
        pass
    else:
        raise SystemExit("the psycopg import block is not effective")

    from types import SimpleNamespace
    from src.db import create_database_manager
    from src import db_backup, db_backend
    from src.inspector_repository import InspectorRepository
    from src.session_reconcile import session_forensics
    import src.cli, src.retriever, src.tape_reset, inspect_db   # all import

    root = tempfile.mkdtemp(prefix="lto_nopg_")
    path = os.path.join(root, "catalog.sqlite3")
    dsn = "sqlite:///" + path.replace(os.sep, "/")
    cfg = SimpleNamespace(db_backend="sqlite", db_dsn=dsn, sqlite_path=path,
                          lto_drive="")
    db = create_database_manager(cfg)
    db.register_tape("NOPG_T1", 100)
    db.acquire_archiver_lock()
    db.verify_archiver_lock()
    stats = db.bulk_upsert_files([{
        "original_path": "/src/d/a.txt", "file_size_bytes": 3,
        "tape_label": "NOPG_T1", "source_host": "local", "is_packed": False,
        "container_name": None, "stored_path": "d/a.txt"}])
    assert stats["inserted"] == 1, stats
    found = db.search_catalog(name_query="a.txt")
    assert [row["original_path"] for row in found] == ["/src/d/a.txt"], found
    assert db.count_tape_file_records("NOPG_T1") == 1
    db.release_archiver_lock()
    with InspectorRepository(dsn) as repo:
        assert repo._execute("SELECT count(*) AS n FROM tapes"
                             ).fetchone()["n"] == 1
    assert session_forensics(dsn, log_dir=root) is not None
    backup = db_backup.create_database_backup(cfg, output_dir=root)
    assert db_backup.verify_backup(cfg, backup).ok
    db.close()
    assert "psycopg" not in sys.modules, sorted(
        name for name in sys.modules if name.startswith("psycopg"))
    print("NO_PSYCOPG_OK")
""")


class NoPsycopgInstallTests(unittest.TestCase):
    def test_sqlite_archive_and_catalog_round_trip_without_psycopg(self):
        """A SQLite-only install has no psycopg at all."""
        result = subprocess.run(
            [_real_python(), "-c", _NO_PSYCOPG], cwd=REPO_ROOT,
            env=_subprocess_env(), capture_output=True, text=True,
            timeout=180)
        self.assertEqual(result.returncode, 0,
                         result.stdout + "\n" + result.stderr)
        self.assertIn("NO_PSYCOPG_OK", result.stdout)

    def test_the_block_really_hides_psycopg_from_the_postgresql_path(self):
        probe = _NO_PSYCOPG.split("from types import SimpleNamespace")[0] + (
            "import src.pg_bulk as bulk\n"
            "try:\n"
            "    bulk.require_psycopg()\n"
            "except RuntimeError:\n"
            "    print('REQUIRE_REFUSED')\n")
        result = subprocess.run(
            [_real_python(), "-c", probe], cwd=REPO_ROOT,
            env=_subprocess_env(), capture_output=True, text=True,
            timeout=180)
        self.assertIn("REQUIRE_REFUSED", result.stdout,
                      result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()
