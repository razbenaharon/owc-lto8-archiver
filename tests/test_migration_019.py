"""Migration 019: additive v2 container-format schema authority.

Covers the requirements from the guarded rollout: v1 keeps validating alone,
015+016+017+019 validates through v2, unexpected schema drift fails closed,
the v2 authority row is immutable, re-applying is idempotent, and the real
pre-write readiness path (``require_existing_stored_tar_recovery``) passes
once v2 is installed. A second class covers the guarded CLI entry point
(``inspect_db._apply_container_format_schema_authority_v2``) with mocks only,
mirroring ``tests/test_container_format_cli.py``.
"""
from datetime import datetime, timezone
from typing import Any, TYPE_CHECKING, cast
import tempfile
import unittest
from unittest import mock
from types import SimpleNamespace

if TYPE_CHECKING:
    import psycopg
else:
    try:
        import psycopg
    except ImportError:  # pragma: no cover - skipped without psycopg
        psycopg = None

from pg_test_guard import (SKIP_REASON, create_test_database,
                           drop_test_database, pg_available)

import inspect_db
from src.cli_errors import OperationalError


def _connect(*args, **kwargs) -> Any:
    return cast(Any, psycopg.connect(*args, **kwargs))


def _pg_available():
    if psycopg is None:
        return False
    return pg_available()


@unittest.skipUnless(_pg_available(), SKIP_REASON)
class Migration019SchemaAuthorityTests(unittest.TestCase):
    """Migration 019 against a fresh disposable database per test."""

    def setUp(self):
        from src.pg_db import PgDatabaseManager

        self.dbname, self.conninfo = create_test_database("lto_migration_019")
        self.db = PgDatabaseManager(self.conninfo)
        self.staging = tempfile.TemporaryDirectory()
        self.addCleanup(self._teardown)

    def _teardown(self):
        try:
            self.db.close()
        finally:
            try:
                self.staging.cleanup()
            finally:
                drop_test_database(self.dbname)

    def _exec(self, sql, params=()):
        with _connect(self.conninfo, autocommit=True) as conn:
            if params:
                conn.execute(sql, params)
            else:
                conn.execute(sql)

    def _apply_015(self):
        self.db.apply_incremental_scan_schema(finalize=True)
        applied = self.db.apply_container_format_schema()
        self.assertEqual(applied, ["015_postgres_container_formats.sql"])

    def _apply_016_017(self):
        applied = self.db.apply_stored_tar_plan_schema()
        self.assertEqual(applied, [
            "016_postgres_stored_tar_plans.sql",
            "017_postgres_stored_tar_publication.sql"])

    def _apply_019(self):
        return self.db.apply_container_format_schema_authority_v2()

    # ---- v1-only still validates -----------------------------------------

    def test_v1_only_validates(self):
        self._apply_015()
        v1 = self.db.validate_container_format_schema()
        self.assertTrue(v1["ready"])

        v2_report = self.db.container_format_schema_authority_v2_report()
        self.assertFalse(v2_report["installed"])

        # The dispatcher used by the pre-write readiness path must take the
        # v1 branch and succeed -- 016/017 are not installed.
        authority = self.db.validate_container_format_authority()
        self.assertEqual(authority, v1)

    # ---- the known defect: 016/017 without 019 fails closed ---------------

    def test_016_017_without_019_fails_closed(self):
        self._apply_015()
        self._apply_016_017()

        v1_after = self.db.container_format_schema_report()
        self.assertFalse(
            v1_after["ready"],
            "v1's own fingerprint must show drift once 016/017 exist")

        with self.assertRaisesRegex(RuntimeError, "v2 schema authority"):
            self.db.validate_container_format_authority()

    # ---- 015+016+017+019 validates through v2 -----------------------------

    def test_full_stack_validates_through_v2(self):
        self._apply_015()
        self._apply_016_017()
        applied = self._apply_019()
        self.assertEqual(applied, [
            "019_postgres_container_format_schema_authority_v2.sql"])

        v2 = self.db.validate_container_format_schema_authority_v2()
        self.assertTrue(v2["ready"])
        self.assertEqual(v2["issues"], [])

        authority = self.db.validate_container_format_authority()
        self.assertTrue(authority["installed"])
        self.assertTrue(authority["ready"])

        # v1's own row and function are untouched.
        v1_after = self.db.container_format_schema_report()
        self.assertFalse(
            v1_after["ready"],
            "015's row stays immutable/unchanged -- it does not start "
            "matching again just because 019 exists")

    # ---- unexpected schema changes fail ------------------------------------

    def test_unexpected_column_change_fails_closed(self):
        self._apply_015()
        self._apply_016_017()
        self._apply_019()
        self.assertTrue(
            self.db.validate_container_format_schema_authority_v2()["ready"])

        self._exec(
            "ALTER TABLE remote_chunks "
            "ALTER COLUMN stored_tar_max_size_bytes TYPE NUMERIC")

        report = self.db.container_format_schema_authority_v2_report()
        self.assertFalse(report["ready"])
        self.assertIn("drift", " ".join(report["issues"]))
        with self.assertRaisesRegex(RuntimeError, "missing or drifted"):
            self.db.validate_container_format_schema_authority_v2()
        with self.assertRaisesRegex(RuntimeError, "missing or drifted"):
            self.db.validate_container_format_authority()

    def test_dropped_constraint_fails_closed(self):
        self._apply_015()
        self._apply_016_017()
        self._apply_019()
        self._exec(
            "ALTER TABLE archive_containers "
            "DROP CONSTRAINT archive_containers_dialect_ck")

        report = self.db.container_format_schema_authority_v2_report()
        self.assertFalse(report["ready"])

    # ---- authority rows cannot be updated or deleted -----------------------

    def test_authority_row_is_immutable(self):
        self._apply_015()
        self._apply_016_017()
        self._apply_019()

        with self.assertRaisesRegex(Exception, "immutable"):
            self._exec(
                "UPDATE container_format_schema_authority "
                "SET schema_fingerprint=repeat('0', 32) "
                "WHERE authority_version=2")
        with self.assertRaisesRegex(Exception, "immutable"):
            self._exec(
                "DELETE FROM container_format_schema_authority "
                "WHERE authority_version=2")

        # Untouched by the rejected attempts.
        self.assertTrue(
            self.db.validate_container_format_schema_authority_v2()["ready"])

    # ---- repeated application is idempotent --------------------------------

    def test_reapply_is_idempotent(self):
        self._apply_015()
        self._apply_016_017()
        first = self._apply_019()
        v2_first = self.db.validate_container_format_schema_authority_v2()

        second = self._apply_019()
        self.assertEqual(first, second)
        v2_second = self.db.validate_container_format_schema_authority_v2()
        self.assertEqual(
            v2_second["row"]["authority_id"], v2_first["row"]["authority_id"])
        self.assertEqual(
            v2_second["row"]["applied_at"], v2_first["row"]["applied_at"])

    # ---- explicit-only: cannot be applied without the guarded checksums ---

    def test_raw_sql_without_checksums_refuses(self):
        self._apply_015()
        self._apply_016_017()
        sql_path = self.db.container_format_authority_v2_migration_path()
        with self.assertRaisesRegex(
                Exception, "explicit guarded command"):
            self._exec(sql_path.read_text(encoding="utf-8"))

    def test_apply_refuses_without_016_017(self):
        self._apply_015()
        with self.assertRaises(Exception):
            self._apply_019()
        self.assertFalse(
            self.db.container_format_schema_authority_v2_report()[
                "installed"])

    # ---- archiver-lock guard, same idiom as 016/017 ------------------------

    def test_apply_refuses_without_a_pinned_archiver_lock(self):
        self._apply_015()
        self._apply_016_017()
        with self.assertRaisesRegex(RuntimeError, "pinned archiver lock"):
            self.db.apply_container_format_schema_authority_v2(
                require_archiver_lock=True)
        self.assertFalse(
            self.db.container_format_schema_authority_v2_report()[
                "installed"])

    def test_apply_succeeds_on_the_pinned_lock_connection(self):
        self._apply_015()
        self._apply_016_017()
        self.db.acquire_archiver_lock()
        applied = self.db.apply_container_format_schema_authority_v2(
            require_archiver_lock=True)
        self.assertEqual(applied, [
            "019_postgres_container_format_schema_authority_v2.sql"])
        self.assertTrue(
            self.db.validate_container_format_schema_authority_v2()["ready"])

    # ---- the real pre-write readiness path passes --------------------------

    def test_real_pre_write_readiness_path_passes(self):
        """``require_existing_stored_tar_recovery`` is what the writer calls
        before touching tape for an already-assigned Stored TAR chunk. It must
        succeed end-to-end once 015+016+017+019 are installed and a chunk is
        assigned Stored TAR through the real approved-boundary path -- the
        same shape as production's Session 37 (an immutable ZIP prefix and an
        approved, never-started Stored TAR suffix) -- not just the schema
        report checked in isolation.

        ``default_packaging_format`` can never become 'stored_tar' directly
        (write-once, guarded by ``trg_remote_sessions_packaging_default``):
        the only legal route is an approved ``remote_packaging_boundaries``
        exception applied as part of migration 015 itself, so the fixture has
        to build the prefix/suffix session shape *before* calling 015.
        """
        self.db.apply_incremental_scan_schema(finalize=True)
        tape = "MIGRATION_019_TAPE"
        self.db.register_tape(tape, 12000)
        session_id = self.db.create_remote_streaming_session(
            session_label="MIGRATION_019_SESSION",
            remote_host="fixture.example",
            remote_user="fixture",
            remote_path="/fixture/migration019",
            tape_label=tape,
            staging_dir=self.staging.name,
        )
        for chunk_index in (0, 1):
            self.db.append_remote_streaming_chunk(
                session_id, chunk_index,
                [(chunk_index,
                  f"/fixture/migration019/chunk_{chunk_index:03d}.bin",
                  f"chunk_{chunk_index:03d}.bin", chunk_index + 1)])
        self._exec(
            """UPDATE remote_chunks SET status='done'
               WHERE session_id=%s AND chunk_index=0""", (session_id,))

        self.db.apply_container_format_schema(
            exception_session_id=session_id,
            expected_boundary=1,
            approval_id="migration-019-readiness-fixture",
            approval_reason="synthetic never-started suffix fixture",
            staging_evidence={
                "root_accessible": True,
                "checked_chunk_indexes": [1],
                "entry_count": 0,
                "unreadable_count": 0,
                "checked_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        self._apply_016_017()
        self._apply_019()

        self.assertEqual(
            self.db.get_chunk_packaging_format(session_id, 1).value,
            "stored_tar")
        self.assertTrue(
            self.db.require_existing_stored_tar_recovery(session_id, 1))

        # And drift still blocks it, exactly like the schema-only checks.
        self._exec(
            "ALTER TABLE archive_containers "
            "DROP CONSTRAINT archive_containers_dialect_ck")
        with self.assertRaisesRegex(RuntimeError, "missing or drifted"):
            self.db.require_existing_stored_tar_recovery(session_id, 1)


class Migration019CliDispatchTests(unittest.TestCase):
    """Local-only guarded-CLI tests; never opens LTFS or PostgreSQL."""

    def _args(self, **overrides):
        base = dict(dry_run=False, execute=False, yes=False, backup_file=None)
        base.update(overrides)
        return SimpleNamespace(**base)

    def test_dry_run_and_execute_together_are_rejected_before_database_open(
            self):
        args = self._args(dry_run=True, execute=True)
        parser = mock.Mock()
        parser.error.side_effect = RuntimeError("exclusive modes")
        with mock.patch.object(inspect_db, "_open_read_only_db") as open_db:
            with self.assertRaisesRegex(RuntimeError, "exclusive modes"):
                inspect_db._apply_container_format_schema_authority_v2(
                    SimpleNamespace(), args, parser)
        open_db.assert_not_called()

    def test_read_only_preflight_reports_without_mutating(self):
        db = mock.Mock()
        db.container_format_schema_report.return_value = {
            "installation_state": "installed", "ready": False}
        db.stored_tar_plan_schema_report.return_value = {
            "installation_state": "installed"}
        db.container_format_schema_authority_v2_report.return_value = {
            "installed": False, "ready": False}
        captured = []
        with mock.patch.object(
                inspect_db, "_open_read_only_db", return_value=db), \
                mock.patch.object(inspect_db, "_print_json",
                                  side_effect=captured.append):
            result = inspect_db._apply_container_format_schema_authority_v2(
                SimpleNamespace(pg_dbname="fixture"),
                self._args(execute=False), mock.Mock())
        self.assertEqual(result, 0)
        db.close.assert_called_once()
        self.assertEqual(captured[0]["applied"], [])
        self.assertNotIn("backup", captured[0])

    def test_execute_requires_yes(self):
        db = mock.Mock()
        db.container_format_schema_report.return_value = {
            "installation_state": "installed", "ready": False}
        db.stored_tar_plan_schema_report.return_value = {
            "installation_state": "installed"}
        db.container_format_schema_authority_v2_report.return_value = {
            "installed": False, "ready": False}
        parser = mock.Mock()
        parser.error.side_effect = RuntimeError("needs --yes")
        with mock.patch.object(
                inspect_db, "_open_read_only_db", return_value=db):
            with self.assertRaisesRegex(RuntimeError, "needs --yes"):
                inspect_db._apply_container_format_schema_authority_v2(
                    SimpleNamespace(pg_dbname="fixture"),
                    self._args(execute=True, yes=False), parser)

    def test_execute_requires_backup_file(self):
        db = mock.Mock()
        db.container_format_schema_report.return_value = {
            "installation_state": "installed", "ready": False}
        db.stored_tar_plan_schema_report.return_value = {
            "installation_state": "installed"}
        db.container_format_schema_authority_v2_report.return_value = {
            "installed": False, "ready": False}
        parser = mock.Mock()
        parser.error.side_effect = RuntimeError("needs --backup-file")
        with mock.patch.object(
                inspect_db, "_open_read_only_db", return_value=db):
            with self.assertRaisesRegex(RuntimeError, "needs --backup-file"):
                inspect_db._apply_container_format_schema_authority_v2(
                    SimpleNamespace(pg_dbname="fixture"),
                    self._args(execute=True, yes=True, backup_file=None),
                    parser)

    def test_execute_refuses_when_prerequisites_are_missing(self):
        db = mock.Mock()
        db.container_format_schema_report.return_value = {
            "installation_state": "absent", "ready": False}
        db.stored_tar_plan_schema_report.return_value = {
            "installation_state": "absent"}
        db.container_format_schema_authority_v2_report.return_value = {
            "installed": False, "ready": False}
        with mock.patch.object(
                inspect_db, "_open_read_only_db", return_value=db):
            with self.assertRaisesRegex(
                    OperationalError, "migration 015 is not installed"):
                inspect_db._apply_container_format_schema_authority_v2(
                    SimpleNamespace(pg_dbname="fixture"),
                    self._args(execute=True, yes=True,
                               backup_file="backup.dump"),
                    mock.Mock())

    def test_execute_refuses_while_archiver_lock_held(self):
        db = mock.Mock()
        db.container_format_schema_report.return_value = {
            "installation_state": "installed", "ready": False}
        db.stored_tar_plan_schema_report.return_value = {
            "installation_state": "installed"}
        db.container_format_schema_authority_v2_report.return_value = {
            "installed": False, "ready": False}
        with mock.patch.object(
                inspect_db, "_open_read_only_db", return_value=db), \
                mock.patch.object(inspect_db, "_conninfo",
                                  return_value="fixture"), \
                mock.patch.object(inspect_db, "archiver_lock_status",
                                  return_value=["pid=123"]):
            with self.assertRaisesRegex(
                    OperationalError, "archiver lock is held"):
                inspect_db._apply_container_format_schema_authority_v2(
                    SimpleNamespace(pg_dbname="fixture"),
                    self._args(execute=True, yes=True,
                               backup_file="backup.dump"),
                    mock.Mock())

    def test_execute_refuses_while_archive_processes_running(self):
        db = mock.Mock()
        db.container_format_schema_report.return_value = {
            "installation_state": "installed", "ready": False}
        db.stored_tar_plan_schema_report.return_value = {
            "installation_state": "installed"}
        db.container_format_schema_authority_v2_report.return_value = {
            "installed": False, "ready": False}
        with mock.patch.object(
                inspect_db, "_open_read_only_db", return_value=db), \
                mock.patch.object(inspect_db, "_conninfo",
                                  return_value="fixture"), \
                mock.patch.object(inspect_db, "archiver_lock_status",
                                  return_value=[]), \
                mock.patch.object(inspect_db, "active_archive_processes",
                                  return_value=["pid=456"]):
            with self.assertRaisesRegex(
                    OperationalError, "archive/transfer processes"):
                inspect_db._apply_container_format_schema_authority_v2(
                    SimpleNamespace(pg_dbname="fixture"),
                    self._args(execute=True, yes=True,
                               backup_file="backup.dump"),
                    mock.Mock())

    def test_execute_applies_and_validates_when_everything_ready(self):
        read_db = mock.Mock()
        read_db.container_format_schema_report.return_value = {
            "installation_state": "installed", "ready": False}
        read_db.stored_tar_plan_schema_report.return_value = {
            "installation_state": "installed"}
        read_db.container_format_schema_authority_v2_report.return_value = {
            "installed": False, "ready": False}
        write_db = mock.Mock()
        write_db.apply_container_format_schema_authority_v2.return_value = [
            "019_postgres_container_format_schema_authority_v2.sql"]
        write_db.validate_container_format_schema_authority_v2.return_value = {
            "ready": True}
        receipt = {"receipt_id": "fixture-receipt"}
        captured = []
        with mock.patch.object(
                inspect_db, "_open_read_only_db", return_value=read_db), \
                mock.patch.object(
                    inspect_db, "_open_no_init_db", return_value=write_db), \
                mock.patch.object(inspect_db, "_conninfo",
                                  return_value="fixture"), \
                mock.patch.object(inspect_db, "archiver_lock_status",
                                  return_value=[]), \
                mock.patch.object(inspect_db, "active_archive_processes",
                                  return_value=[]), \
                mock.patch.object(inspect_db, "verify_backup_receipt",
                                  return_value=receipt), \
                mock.patch.object(inspect_db, "_print_json",
                                  side_effect=captured.append):
            result = inspect_db._apply_container_format_schema_authority_v2(
                SimpleNamespace(pg_dbname="fixture"),
                self._args(execute=True, yes=True,
                           backup_file="backup.dump"),
                mock.Mock())
        self.assertEqual(result, 0)
        write_db.acquire_archiver_lock.assert_called_once()
        write_db.apply_container_format_schema_authority_v2\
            .assert_called_once_with(require_archiver_lock=True)
        write_db.validate_container_format_schema_authority_v2\
            .assert_called_once()
        write_db.close.assert_called_once()
        self.assertEqual(
            captured[0]["applied"],
            ["019_postgres_container_format_schema_authority_v2.sql"])
        self.assertEqual(captured[0]["validation"], {"ready": True})

    def test_execute_refuses_when_backup_receipt_changes_before_locked_apply(
            self):
        read_db = mock.Mock()
        read_db.container_format_schema_report.return_value = {
            "installation_state": "installed", "ready": False}
        read_db.stored_tar_plan_schema_report.return_value = {
            "installation_state": "installed"}
        read_db.container_format_schema_authority_v2_report.return_value = {
            "installed": False, "ready": False}
        write_db = mock.Mock()
        with mock.patch.object(
                inspect_db, "_open_read_only_db", return_value=read_db), \
                mock.patch.object(
                    inspect_db, "_open_no_init_db", return_value=write_db), \
                mock.patch.object(inspect_db, "_conninfo",
                                  return_value="fixture"), \
                mock.patch.object(inspect_db, "archiver_lock_status",
                                  return_value=[]), \
                mock.patch.object(inspect_db, "active_archive_processes",
                                  return_value=[]), \
                mock.patch.object(
                    inspect_db, "verify_backup_receipt",
                    side_effect=[{"receipt_id": "first"},
                                 {"receipt_id": "second"}]):
            with self.assertRaisesRegex(
                    OperationalError, "Backup receipt changed"):
                inspect_db._apply_container_format_schema_authority_v2(
                    SimpleNamespace(pg_dbname="fixture"),
                    self._args(execute=True, yes=True,
                               backup_file="backup.dump"),
                    mock.Mock())
        write_db.apply_container_format_schema_authority_v2.assert_not_called()
        write_db.close.assert_called_once()


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
