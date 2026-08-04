import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from src.schema_audit import (
    BASE_TABLES, _code_provenance_facts, audit_schema_provenance)


class _Result:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return list(self._rows)


class _FakeSchemaConnection:
    """Small information_schema fake for optional-schema shape tests."""

    def __init__(self, tables, columns=()):
        self.tables = set(tables)
        self.columns = list(columns)

    def execute(self, sql, params=()):
        if "information_schema.tables" in sql:
            return _Result([{"table_name": name} for name in sorted(self.tables)])
        if "information_schema.views" in sql:
            return _Result([])
        if "information_schema.columns" in sql:
            return _Result([
                {"table_name": table, "column_name": column,
                 "ordinal_position": index}
                for table, column, index in self.columns
            ])
        if "information_schema.table_constraints" in sql:
            return _Result([])
        if "FROM pg_indexes" in sql:
            return _Result([])
        if "COUNT(*)" in sql:
            if "FROM directory_archive_bundles" in sql:
                return _Result([{"n": 0}])
            if "directory_tree_index" in sql:
                return _Result([{"total": 0, "null_chunk": 0, "no_bundle": 0}])
            return _Result([{"row_count": 0, "byte_total": 0}])
        return _Result([])


class SchemaAuditFakeSchemaTests(unittest.TestCase):
    def test_007_absent_is_unavailable(self):
        report = audit_schema_provenance(_FakeSchemaConnection(BASE_TABLES))
        tables = {
            item["object"]: item for item in report["schema_checks"]
            if item["kind"] == "table"
        }
        self.assertEqual(tables["directory_archive_bundles"]["status"],
                         "UNAVAILABLE")

    def test_007_present_is_pass_only_for_present_table(self):
        report = audit_schema_provenance(
            _FakeSchemaConnection(BASE_TABLES + ("directory_archive_bundles",)))
        tables = {
            item["object"]: item for item in report["schema_checks"]
            if item["kind"] == "table"
        }
        self.assertEqual(tables["directory_archive_bundles"]["status"], "PASS")
        self.assertEqual(tables["directory_tree_index"]["status"],
                         "UNAVAILABLE")

    def test_007_partial_columns_remain_unavailable(self):
        report = audit_schema_provenance(
            _FakeSchemaConnection(
                BASE_TABLES + ("directory_archive_bundles",),
                columns=[("directory_archive_bundles", "bundle_id", 1)]))
        columns = {
            item["object"]: item for item in report["schema_checks"]
            if item["kind"] == "column"
        }
        self.assertEqual(
            columns["directory_archive_bundles.original_dir_path"]["status"],
            "UNAVAILABLE")


class SchemaAuditCodeProvenanceAstTests(unittest.TestCase):
    @staticmethod
    def _root_with_catalog(source):
        temp = TemporaryDirectory()
        root = Path(temp.name)
        (root / "src").mkdir()
        (root / "src" / "pg_catalog.py").write_text(source, encoding="utf-8")
        return temp, root

    def test_docstring_remote_path_warning_does_not_fail_no_fallback(self):
        temp, root = self._root_with_catalog(
            '''
class Catalog:
    @staticmethod
    def _derive_bundle_base_path(conn, stored_bundle_path, remote_session_id):
        """remote_sessions.remote_path is deliberately never consulted."""
        return ""
''')
        try:
            facts = _code_provenance_facts(root)
            self.assertTrue(facts["no_remote_path_root_fallback"])
        finally:
            temp.cleanup()

    def test_executable_remote_sessions_query_fails_no_fallback(self):
        temp, root = self._root_with_catalog(
            '''
class Catalog:
    @staticmethod
    def _derive_bundle_base_path(conn, stored_bundle_path, remote_session_id):
        row = conn.execute("SELECT remote_path FROM remote_sessions").fetchone()
        return row["remote_path"] if row else ""
''')
        try:
            facts = _code_provenance_facts(root)
            self.assertFalse(facts["no_remote_path_root_fallback"])
            self.assertTrue(facts["reasons"]["no_remote_path_root_fallback"])
        finally:
            temp.cleanup()

    def test_missing_target_symbols_never_pass_any_code_fact(self):
        cases = {
            "explicit_remote_identity": {
                "src/backup.py": "def unrelated(): pass\n",
                "src/remote_writer.py": "def unrelated(): pass\n",
            },
            "remote_chunk_not_derived_from_local": {
                "src/pg_catalog.py": "def unrelated(): pass\n",
            },
            "no_remote_path_root_fallback": {
                "src/pg_catalog.py": "def unrelated(): pass\n",
            },
            "persisted_scan_scope_guard": {
                "src/pg_catalog.py": "def unrelated(): pass\n",
            },
        }
        for fact_name, files in cases.items():
            with self.subTest(fact=fact_name), TemporaryDirectory() as temp_name:
                root = Path(temp_name)
                for relative, source in files.items():
                    path = root / relative
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text(source, encoding="utf-8")
                facts = _code_provenance_facts(root)
                self.assertNotEqual(facts[fact_name], True)
                self.assertTrue(facts["reasons"].get(fact_name))

    def test_scope_query_without_empty_root_gate_fails(self):
        temp, root = self._root_with_catalog(
            '''
class Catalog:
    @staticmethod
    def _derive_bundle_base_path(conn, stored_bundle_path, remote_session_id):
        scopes = conn.execute(
            "SELECT root_path FROM remote_scan_scopes WHERE session_id=%s",
            (remote_session_id,),
        ).fetchall()
        return "/candidate"
''')
        try:
            facts = _code_provenance_facts(root)
            self.assertFalse(facts["persisted_scan_scope_guard"])
        finally:
            temp.cleanup()
