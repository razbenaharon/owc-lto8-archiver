"""Plan 1 / Task 2.1 — migration 014 shape, guards and refusals (offline).

The dangerous half of a migration is not the DDL, it is what the DDL is allowed
to do to data that already exists. These tests assert the properties that make
014 safe to run against the production catalog:

* the BASE half is additive and idempotent, and every column it adds to an
  existing table is NULLABLE — old sessions stay readable;
* the FINALIZE half is a SEPARATE file that can fail, and it **refuses** rather
  than resequencing ambiguous legacy plan ordinals;
* "installed" and "finalized" are different questions, and a half-applied
  migration reads as neither;
* the ``inspect_db.py`` entry point is read-only unless an operator supplies an
  explicit execute + confirmation + verified backup, with no archiver running;
* the rollback refuses to drop a populated frontier table.

Isolated-PostgreSQL behaviour lives in ``tests/test_pg_integration.py``.
"""
import os
import re
import unittest
from types import SimpleNamespace
from unittest import mock

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SQL_DIR = os.path.join(PROJECT_ROOT, "scripts", "sql")

BASE = "014_postgres_incremental_scan.sql"
FINALIZE = "014_postgres_incremental_scan_finalize.sql"
ROLLBACK = "014_postgres_incremental_scan_rollback.sql"


def _sql(name):
    with open(os.path.join(SQL_DIR, name), encoding="utf-8") as handle:
        return handle.read()


# =============================================================================
# A. File layout and numbering
# =============================================================================
class MigrationLayoutTests(unittest.TestCase):
    def test_all_three_files_exist(self):
        for name in (BASE, FINALIZE, ROLLBACK):
            self.assertTrue(os.path.exists(os.path.join(SQL_DIR, name)), name)

    def test_migration_number_010_is_not_reused(self):
        names = [n for n in os.listdir(SQL_DIR) if n.endswith(".sql")]
        tens = [n for n in names if n.startswith("010_")]
        self.assertEqual(tens, ["010_postgres_local_manifest_archive.sql"])

    def test_no_dependency_on_the_optional_migrations(self):
        """007 (directory catalog) and 012 (sealed batches) are optional."""
        for name in (BASE, FINALIZE):
            sql = _sql(name)
            for optional in ("directory_archive_stats", "directory_tree_index",
                             "sealed_tape_write_batches",
                             "tape_write_batches"):
                self.assertNotIn(optional, sql, f"{name} references {optional}")


# =============================================================================
# B. The BASE half is additive and legacy-safe
# =============================================================================
class BaseMigrationSafetyTests(unittest.TestCase):
    def setUp(self):
        self.sql = _sql(BASE)

    def test_every_new_table_is_created_if_not_exists(self):
        creates = re.findall(r"CREATE TABLE (\w*\s*)*?(\w+)", self.sql)
        self.assertEqual(self.sql.count("CREATE TABLE"),
                         self.sql.count("CREATE TABLE IF NOT EXISTS"),
                         "a CREATE TABLE without IF NOT EXISTS is not idempotent")
        self.assertTrue(creates)

    def test_every_index_is_created_if_not_exists(self):
        self.assertEqual(self.sql.count("CREATE INDEX"),
                         self.sql.count("CREATE INDEX IF NOT EXISTS"))

    def test_every_frontier_table_is_created(self):
        from src.pg_core import PgConnectionCore
        for table in PgConnectionCore.INCREMENTAL_SCAN_TABLES:
            self.assertIn(f"CREATE TABLE IF NOT EXISTS {table}", self.sql)

    def test_added_chunk_columns_are_nullable_for_legacy(self):
        block = self.sql[self.sql.index("ALTER TABLE remote_chunks"):]
        block = block[:block.index(";")]
        for column in ("owner_token", "lease_expires_at", "attempt_id",
                       "membership_state", "expected_file_count",
                       "expected_bytes"):
            self.assertIn(f"ADD COLUMN IF NOT EXISTS {column}", block, column)
        # Nothing added here may be NOT NULL: a legacy row must stay legal.
        self.assertNotIn("NOT NULL", block)

    def test_the_base_half_creates_no_unique_constraint_over_legacy_data(self):
        """A unique index over remote_plan_files belongs to FINALIZE only."""
        self.assertNotIn("uq_remote_plan_files_chunk_ordinal", self.sql)
        self.assertNotIn("remote_plan_files", self.sql)

    def test_the_base_half_repairs_nothing(self):
        for mutation in ("UPDATE remote_plan_files", "DELETE FROM remote_",
                         "row_number()", "RESEQUENCE"):
            self.assertNotIn(mutation, self.sql, mutation)

    def test_the_three_directory_states_are_independent_columns(self):
        for column, values in (
            ("listing_state",
             ("pending", "scanning", "partial", "complete", "error",
              "invalidated")),
            ("subtree_coverage_state",
             ("provisional", "final", "error", "invalidated")),
            ("planning_state",
             ("unplanned", "partially_allocated", "fully_allocated",
              "blocked")),
        ):
            self.assertIn(column, self.sql)
            for value in values:
                self.assertIn(f"'{value}'", self.sql, f"{column}:{value}")

    def test_a_part_locator_can_never_be_a_ready_segment(self):
        self.assertIn("CHECK (locator NOT LIKE '%.part')", self.sql)

    def test_segment_membership_has_a_unique_range_identity(self):
        self.assertIn(
            "UNIQUE (scan_segment_id, first_scan_ordinal, last_scan_ordinal)",
            self.sql)

    def test_expected_bytes_is_documented_as_logical_source_bytes(self):
        self.assertRegex(self.sql, r"LOGICAL PLANNED SOURCE BYTES")

    def test_the_richer_file_outcomes_are_added(self):
        for status in ("source_permission_denied", "source_unreadable",
                       "source_changed", "unresolved"):
            self.assertIn(f"'{status}'", self.sql, status)
        # ...without dropping any legacy value.
        for status in ("pending", "fetching", "fetched", "fetch_failed",
                       "source_missing"):
            self.assertIn(f"'{status}'", self.sql, status)

    def test_errors_are_recorded_but_successes_are_not(self):
        self.assertIn("remote_scan_errors", self.sql)
        self.assertRegex(self.sql, r"no success row per file")

    def test_worker_attempts_carry_process_creation_identity(self):
        """A PID alone proves nothing — PIDs are reused."""
        for column in ("local_pid", "local_process_started_at",
                       "remote_command_token", "remote_process_group"):
            self.assertIn(column, self.sql, column)


# =============================================================================
# C. The FINALIZE half refuses rather than repairs
# =============================================================================
class FinalizeMigrationTests(unittest.TestCase):
    def setUp(self):
        self.sql = _sql(FINALIZE)

    def test_it_requires_the_base_half_first(self):
        self.assertIn("remote_scan_scopes", self.sql)
        self.assertIn("BASE is not installed", self.sql)

    def test_it_audits_before_creating_the_unique_index(self):
        audit_at = self.sql.index("remote_plan_ordinal_audit")
        index_at = self.sql.index("uq_remote_plan_files_chunk_ordinal")
        self.assertLess(audit_at, index_at,
                        "the unique index is created before the audit runs")

    def test_it_raises_on_duplicate_ordinals(self):
        self.assertIn("RAISE EXCEPTION", self.sql)
        self.assertIn("REFUSING to finalize", self.sql)

    def test_it_never_resequences(self):
        for mutation in ("UPDATE remote_plan_files", "DELETE FROM remote_plan_files",
                         "row_number()"):
            self.assertNotIn(mutation, self.sql, mutation)

    def test_the_refusal_explains_the_tape_consequence(self):
        self.assertIn("may already be on tape", self.sql)

    def test_a_sealed_chunk_must_declare_its_expectations(self):
        self.assertIn("remote_chunks_sealed_expectations_check", self.sql)
        self.assertIn("expected_file_count IS NOT NULL", self.sql)
        self.assertIn("expected_bytes IS NOT NULL", self.sql)

    def test_a_ready_segment_must_carry_its_consumption_cursor(self):
        self.assertIn("remote_scan_segments_ready_cursor_check", self.sql)
        self.assertIn("next_unconsumed_ordinal IS NOT NULL", self.sql)


# =============================================================================
# D. Rollback refuses to destroy evidence
# =============================================================================
class RollbackSafetyTests(unittest.TestCase):
    def setUp(self):
        self.sql = _sql(ROLLBACK)

    def test_it_refuses_when_a_frontier_table_holds_data(self):
        self.assertIn("REFUSING to roll back", self.sql)
        from src.pg_core import PgConnectionCore
        for table in PgConnectionCore.INCREMENTAL_SCAN_TABLES:
            self.assertIn(f"EXISTS (SELECT 1 FROM {table})", self.sql, table)

    def test_it_never_touches_plan_membership_rows(self):
        self.assertNotIn("DROP TABLE IF EXISTS remote_plan_files", self.sql)
        self.assertNotIn("DELETE FROM remote_plan_files", self.sql)
        self.assertNotIn("UPDATE remote_plan_files", self.sql)

    def test_it_keeps_the_nullable_chunk_columns(self):
        self.assertNotIn("DROP COLUMN", self.sql)


# =============================================================================
# E. installed != finalized
# =============================================================================
class _FakeCursor:
    def __init__(self, row=None):
        self._row = row

    def fetchone(self):
        return self._row


class _FakeConn:
    def __init__(self, tables=(), columns=(), indexes=(), duplicates=0,
                 lock_held=0, database="lto_test"):
        self.tables = set(tables)
        self.columns = set(columns)
        self.indexes = set(indexes)
        self.duplicates = duplicates
        self.lock_held = lock_held
        self.database = database

    def execute(self, sql, params=()):
        flat = " ".join(sql.split())
        if "current_database()" in flat:
            return _FakeCursor({"db": self.database})
        if "information_schema.tables" in flat:
            return _FakeCursor({"x": 1} if params[0] in self.tables else None)
        if "information_schema.columns" in flat:
            return _FakeCursor(
                {"x": 1} if (params[0], params[1]) in self.columns else None)
        if "pg_indexes" in flat:
            return _FakeCursor({"x": 1} if params[0] in self.indexes else None)
        if "pg_locks" in flat:
            return _FakeCursor({"n": self.lock_held})
        if "HAVING COUNT(*) > 1" in flat and "LIMIT 10" in flat:
            return _FakeRows([{"plan_id": 1, "chunk_index": 0, "ordinal": 0,
                               "n": 2}] * min(self.duplicates, 10))
        if "HAVING COUNT(*) > 1" in flat:
            return _FakeCursor({"n": self.duplicates})
        raise AssertionError(f"unexpected SQL: {flat}")


class _FakeRows:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class _FakePool:
    def __init__(self, conn):
        self._conn = conn

    def connection(self):
        outer = self

        class _Ctx:
            def __enter__(self):
                return outer._conn

            def __exit__(self, *exc):
                return False
        return _Ctx()


def _core(**kwargs):
    from src.pg_core import PgConnectionCore
    core = PgConnectionCore.__new__(PgConnectionCore)
    core._pool = _FakePool(_FakeConn(**kwargs))
    return core


ALL_TABLES = ("remote_scan_scopes", "remote_scan_directories",
              "remote_scan_segments", "remote_chunk_scan_segments",
              "remote_scan_errors", "remote_worker_attempts",
              "remote_frontier_bootstraps")
ALL_COLUMNS = tuple(("remote_chunks", c) for c in
                    ("owner_token", "lease_expires_at", "attempt_id",
                     "membership_state", "expected_file_count",
                     "expected_bytes"))


class SchemaStatePredicateTests(unittest.TestCase):
    def test_a_clean_database_is_neither_installed_nor_finalized(self):
        core = _core()
        self.assertFalse(core.incremental_scan_schema_installed())
        self.assertFalse(core.incremental_scan_schema_finalized())

    def test_a_half_applied_base_does_not_read_as_installed(self):
        # All tables, but one column missing.
        core = _core(tables=ALL_TABLES, columns=ALL_COLUMNS[:-1])
        self.assertFalse(core.incremental_scan_schema_installed())
        # One table missing, all columns present.
        core = _core(tables=ALL_TABLES[:-1], columns=ALL_COLUMNS)
        self.assertFalse(core.incremental_scan_schema_installed())

    def test_a_complete_base_reads_as_installed_but_not_finalized(self):
        core = _core(tables=ALL_TABLES, columns=ALL_COLUMNS)
        self.assertTrue(core.incremental_scan_schema_installed())
        self.assertFalse(core.incremental_scan_schema_finalized())

    def test_finalized_requires_the_unique_index(self):
        core = _core(tables=ALL_TABLES, columns=ALL_COLUMNS,
                     indexes=("uq_remote_plan_files_chunk_ordinal",))
        self.assertTrue(core.incremental_scan_schema_finalized())

    def test_the_scan_mode_gate_sees_both_answers(self):
        from src.scan_frontier import decide_scan_mode, MODE_FRONTIER, MODE_LEGACY
        cfg = SimpleNamespace(incremental_scan_enabled=True)
        base_only = _core(tables=ALL_TABLES, columns=ALL_COLUMNS)
        self.assertEqual(decide_scan_mode(cfg, base_only).mode, MODE_LEGACY)
        ready = _core(tables=ALL_TABLES, columns=ALL_COLUMNS,
                      indexes=("uq_remote_plan_files_chunk_ordinal",))
        self.assertEqual(decide_scan_mode(cfg, ready).mode, MODE_FRONTIER)


class PreflightTests(unittest.TestCase):
    def test_it_is_read_only(self):
        import inspect
        from src.pg_core import PgConnectionCore
        source = inspect.getsource(
            PgConnectionCore.incremental_scan_schema_preflight)
        for mutation in ("INSERT", "UPDATE", "DELETE", "CREATE", "ALTER",
                         "DROP", "commit()"):
            self.assertNotIn(mutation, source, mutation)

    def test_it_reports_a_live_archiver_as_blocking(self):
        core = _core(tables=ALL_TABLES, columns=ALL_COLUMNS, lock_held=1)
        report = core.incremental_scan_schema_preflight()
        self.assertTrue(report["archiver_lock_held"])
        self.assertTrue(any("advisory lock" in b for b in report["blocking"]))

    def test_it_reports_duplicate_ordinals_as_blocking(self):
        core = _core(tables=ALL_TABLES, columns=ALL_COLUMNS, duplicates=3)
        report = core.incremental_scan_schema_preflight()
        self.assertEqual(report["duplicate_ordinal_groups"], 3)
        self.assertTrue(any("resequence" in b for b in report["blocking"]))

    def test_a_clean_database_has_nothing_blocking(self):
        core = _core(tables=ALL_TABLES, columns=ALL_COLUMNS)
        report = core.incremental_scan_schema_preflight()
        self.assertEqual(report["blocking"], [])
        self.assertEqual(report["database"], "lto_test")

    def test_it_names_the_exact_database(self):
        core = _core(database="lto_archive_prod")
        self.assertEqual(
            core.incremental_scan_schema_preflight()["database"],
            "lto_archive_prod")


# =============================================================================
# F. The inspect_db entry point is read-only unless told otherwise
# =============================================================================
class ApplyCommandGuardTests(unittest.TestCase):
    def _args(self, **kwargs):
        base = {"execute": False, "yes": False, "finalize": False,
                "backup_file": None}
        base.update(kwargs)
        return SimpleNamespace(**base)

    def _cfg(self):
        return SimpleNamespace(pg_dbname="lto_test", db_display_ref="lto_test")

    def _patched(self, preflight, applied=None):
        import inspect_db
        db = mock.MagicMock()
        db.incremental_scan_schema_preflight.return_value = dict(preflight)
        db.apply_incremental_scan_schema.return_value = applied or []
        db.incremental_scan_schema_installed.return_value = True
        db.incremental_scan_schema_finalized.return_value = bool(applied and
                                                                 len(applied) > 1)
        return mock.patch.object(inspect_db, "_open_db", return_value=db), db

    def test_without_execute_it_only_reports(self):
        import inspect_db
        patch, db = self._patched(
            {"blocking": [], "archiver_lock_held": False,
             "duplicate_ordinal_groups": 0, "duplicate_ordinal_sample": []})
        with patch, mock.patch.object(inspect_db, "_print_json") as printed:
            rc = inspect_db._apply_incremental_scan_schema(
                self._cfg(), self._args(), mock.MagicMock())
        self.assertEqual(rc, 0)
        db.apply_incremental_scan_schema.assert_not_called()
        self.assertIn("preflight", printed.call_args.args[0]["note"])

    def test_execute_without_confirmation_is_an_error(self):
        import inspect_db
        parser = mock.MagicMock()
        parser.error.side_effect = SystemExit(2)
        patch, _db = self._patched(
            {"blocking": [], "archiver_lock_held": False,
             "duplicate_ordinal_groups": 0, "duplicate_ordinal_sample": []})
        with patch, self.assertRaises(SystemExit):
            inspect_db._apply_incremental_scan_schema(
                self._cfg(), self._args(execute=True), parser)

    def test_execute_without_a_backup_is_an_error(self):
        import inspect_db
        parser = mock.MagicMock()
        parser.error.side_effect = SystemExit(2)
        patch, _db = self._patched(
            {"blocking": [], "archiver_lock_held": False,
             "duplicate_ordinal_groups": 0, "duplicate_ordinal_sample": []})
        with patch, self.assertRaises(SystemExit):
            inspect_db._apply_incremental_scan_schema(
                self._cfg(), self._args(execute=True, yes=True), parser)

    def test_a_live_archiver_blocks_execution(self):
        import inspect_db
        from src.cli_errors import OperationalError
        patch, db = self._patched(
            {"blocking": [], "archiver_lock_held": False,
             "duplicate_ordinal_groups": 0, "duplicate_ordinal_sample": []})
        with patch, \
                mock.patch.object(inspect_db, "archiver_lock_status",
                                  return_value=[{"pid": 1}]), \
                mock.patch.object(inspect_db, "_conninfo", return_value=""), \
                self.assertRaises(OperationalError):
            inspect_db._apply_incremental_scan_schema(
                self._cfg(),
                self._args(execute=True, yes=True, backup_file="b.dump"),
                mock.MagicMock())
        db.apply_incremental_scan_schema.assert_not_called()

    def test_running_archive_processes_block_execution(self):
        import inspect_db
        from src.cli_errors import OperationalError
        patch, db = self._patched(
            {"blocking": [], "archiver_lock_held": False,
             "duplicate_ordinal_groups": 0, "duplicate_ordinal_sample": []})
        with patch, \
                mock.patch.object(inspect_db, "archiver_lock_status",
                                  return_value=[]), \
                mock.patch.object(inspect_db, "active_archive_processes",
                                  return_value=["robocopy.exe"]), \
                mock.patch.object(inspect_db, "_conninfo", return_value=""), \
                self.assertRaises(OperationalError):
            inspect_db._apply_incremental_scan_schema(
                self._cfg(),
                self._args(execute=True, yes=True, backup_file="b.dump"),
                mock.MagicMock())
        db.apply_incremental_scan_schema.assert_not_called()

    def test_finalize_refuses_on_duplicate_ordinals(self):
        import inspect_db
        from src.cli_errors import OperationalError
        patch, db = self._patched(
            {"blocking": [], "archiver_lock_held": False,
             "duplicate_ordinal_groups": 4,
             "duplicate_ordinal_sample": [{"plan_id": 1}]})
        with patch, \
                mock.patch.object(inspect_db, "archiver_lock_status",
                                  return_value=[]), \
                mock.patch.object(inspect_db, "active_archive_processes",
                                  return_value=[]), \
                mock.patch.object(inspect_db, "_conninfo", return_value=""), \
                mock.patch.object(inspect_db, "_verify_hot_backup",
                                  return_value={"verified": True}), \
                self.assertRaises(OperationalError) as caught:
            inspect_db._apply_incremental_scan_schema(
                self._cfg(),
                self._args(execute=True, yes=True, finalize=True,
                           backup_file="b.dump"),
                mock.MagicMock())
        self.assertIn("NOT auto-resequenced", str(caught.exception))
        db.apply_incremental_scan_schema.assert_not_called()

    def test_a_clean_execute_applies_the_base_half(self):
        import inspect_db
        patch, db = self._patched(
            {"blocking": [], "archiver_lock_held": False,
             "duplicate_ordinal_groups": 0, "duplicate_ordinal_sample": []},
            applied=[BASE])
        with patch, \
                mock.patch.object(inspect_db, "archiver_lock_status",
                                  return_value=[]), \
                mock.patch.object(inspect_db, "active_archive_processes",
                                  return_value=[]), \
                mock.patch.object(inspect_db, "_conninfo", return_value=""), \
                mock.patch.object(inspect_db, "_verify_hot_backup",
                                  return_value={"verified": True}), \
                mock.patch.object(inspect_db, "_print_json") as printed:
            rc = inspect_db._apply_incremental_scan_schema(
                self._cfg(),
                self._args(execute=True, yes=True, backup_file="b.dump"),
                mock.MagicMock())
        self.assertEqual(rc, 0)
        db.apply_incremental_scan_schema.assert_called_once_with(finalize=False)
        self.assertEqual(printed.call_args.args[0]["applied"], [BASE])


# =============================================================================
# G. The frontier repository refuses without the schema
# =============================================================================
class FrontierRepositoryGuardTests(unittest.TestCase):
    def _mixin(self, installed):
        from src.pg_scan import PgScanMixin

        class Repo(PgScanMixin):
            def incremental_scan_schema_installed(self_inner):
                return installed
        return Repo()

    def test_every_write_path_refuses_without_migration_014(self):
        from src.pg_scan import ScanFrontierError
        repo = self._mixin(installed=False)
        with self.assertRaises(ScanFrontierError):
            repo.create_scan_scopes(37, ["/strg"])
        with self.assertRaises(ScanFrontierError):
            repo.claim_next_directory(37, "tok", "att")
        with self.assertRaises(ScanFrontierError):
            repo.publish_scan_segment(
                1, first_scan_ordinal=0, last_scan_ordinal=1, locator="a.zst",
                file_count=1, byte_count=1)
        with self.assertRaises(ScanFrontierError):
            repo.consume_segment_range(1, 37, 0, 5)

    def test_frontier_state_is_definitively_absent_without_the_schema(self):
        self.assertFalse(
            self._mixin(installed=False).session_has_frontier_state(37))

    def test_a_part_locator_is_refused_before_any_sql(self):
        from src.pg_scan import ScanFrontierError
        repo = self._mixin(installed=True)
        with self.assertRaises(ScanFrontierError) as caught:
            repo.publish_scan_segment(
                1, first_scan_ordinal=0, last_scan_ordinal=1,
                locator="seg.jsonl.zst.part", file_count=1, byte_count=1)
        self.assertIn(".part", str(caught.exception))

    def test_an_inverted_range_is_refused(self):
        from src.pg_scan import ScanFrontierError
        repo = self._mixin(installed=True)
        with self.assertRaises(ScanFrontierError):
            repo.publish_scan_segment(
                1, first_scan_ordinal=9, last_scan_ordinal=2, locator="a.zst",
                file_count=1, byte_count=1)

    def test_a_non_positive_consumption_is_refused(self):
        from src.pg_scan import ScanFrontierError
        repo = self._mixin(installed=True)
        for count in (0, -1):
            with self.assertRaises(ScanFrontierError):
                repo.consume_segment_range(1, 37, 0, count)

    def test_the_mixin_is_assembled_into_the_manager(self):
        from src.pg_db import PgDatabaseManager
        from src.pg_scan import PgScanMixin
        self.assertTrue(issubclass(PgDatabaseManager, PgScanMixin))
        for name in ("create_scan_scopes", "claim_next_directory",
                     "publish_scan_segment", "consume_segment_range",
                     "finalize_directory_subtree", "finalize_scan_scope",
                     "session_has_frontier_state"):
            self.assertTrue(hasattr(PgDatabaseManager, name), name)


if __name__ == "__main__":
    unittest.main()
