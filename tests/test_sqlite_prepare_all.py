"""Every literal SQL statement in the application prepares on SQLite.

The catalog layer is written once, in PostgreSQL SQL, and the SQLite backend
runs the same text through the dialect rewriter. This test finds every SQL
string literal passed to ``execute`` / ``executemany`` in ``src/`` and
``inspect_db.py`` (by walking the syntax tree), rewrites it, and asks SQLite
to ``EXPLAIN`` it against the real SQLite schema - which proves the statement
parses and that every table and column it names exists.

A statement is exempt only when it lives in code that never runs on SQLite:

* a PostgreSQL-only module (:data:`POSTGRESQL_ONLY_MODULES`, the features in
  ``src.db_backend.POSTGRESQL_ONLY_FEATURES``), or
* a named PostgreSQL implementation of a backend hook
  (:data:`POSTGRESQL_ONLY_FUNCTIONS`), each of which
  ``SqliteConnectionCore`` / ``SqliteSchemaReports`` overrides or
  ``db_backend`` branches around.

Not covered (and said so here rather than implied): SQL assembled at run time
from f-strings or concatenation. Those statements are exercised by the
behavioural suites that run on both backends.
"""
import ast
import os
import re
import sqlite3
import unittest

from db_backends import SQLITE, create_test_db

from src.sqlite_dialect import SqliteDialectError, rewrite

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

EXECUTE_NAMES = {"execute", "executemany", "_execute"}

#: Whole modules that never run against SQLite (v1.0 PostgreSQL-only).
POSTGRESQL_ONLY_MODULES = {
    "pg_backup.py": "pg_dump_backup",
    "pg_bulk.py": "PostgreSQL pool / COPY helpers",
    "schema_audit.py": "schema_audit",
    "sealed_batch_repository.py": "sealed_batches",
    "sealed_batch_observation.py": "sealed_batches",
    "sealed_batch_observer.py": "sealed_batches",
    "local_manifest_archive.py": "small_file_manifest_maintenance",
    # The SQLite engine itself: native SQLite SQL, not PostgreSQL SQL.
    "sqlite_adapter.py": "native SQLite",
    "sqlite_core.py": "native SQLite / hook implementations",
    "sqlite_lock.py": "native SQLite",
}

#: PostgreSQL implementations of backend hooks, by (module, function).
POSTGRESQL_ONLY_FUNCTIONS = {
    "pg_core.py": {
        # schema: PostgreSQL migration files and their explicit entry points
        "_init_schema", "_ensure_runtime_constraints",
        "apply_directory_catalog_schema", "apply_incremental_scan_schema",
        "apply_on",
        # completing an EMPTY PostgreSQL database at start-up; reached only
        # through PgDatabaseManager(complete_fresh_schema=True), which the
        # SQLite core never passes (SQLite installs everything at creation)
        "_init_schema_completing_a_fresh_database", "_fresh_install_state",
        "_complete_fresh_schema",
        # hooks overridden by SqliteConnectionCore
        "_set_local_setting_conn", "_index_exists_conn", "_view_exists_conn",
        "_constraint_exists_conn", "_create_stage_table_conn",
        "_upsert_from_stage_counts_conn", "_insert_local_session_conn",
        "_vacuum_analyze_conn", "_single_column_references_conn",
        "_sql_assign_new_chunk_format_conn",
        "_sql_transition_chunk_plan_source_conn",
        "acquire_archiver_lock", "verify_archiver_lock",
        "release_archiver_lock",
    },
    # Both branches of a backend-neutral helper live in one function.
    "db_backend.py": {
        "connect", "set_session_read_only", "table_exists", "column_exists",
        "column_names", "describe_database", "database_identity",
        "foreign_keys_referencing", "database_name",
        "pg_archiver_lock_holders", "pg_session_owns_archiver_lock",
    },
    # Replaced on SQLite by SqliteSchemaReports.
    "pg_containers.py": {
        "_container_format_schema_report_conn",
        "_container_format_schema_authority_v2_report_conn",
        "operation:classify_format_boundary",
    },
    "pg_directory_catalog.py": {
        "_manifest_directory_catalog_schema_report_conn",
        "_manifest_directory_catalog_prerequisite_issues_conn",
    },
}


#: Statements that are wrong on BOTH backends: ``(module, function) -> the
#: PostgreSQL error``. Empty since the two original entries were fixed
#: (tests/test_catalog_defect_regressions.py). An entry must quote the error
#: a real PostgreSQL gives; the test fails when a listed statement starts
#: preparing, so an entry cannot outlive its defect.
KNOWN_DEFECTS_ON_BOTH_BACKENDS = {}

#: Tables named only inside a table-exists guard for a pre-PostgreSQL layout.
#: They exist in neither schema, so the guarded statement never runs.
LEGACY_ABSENT_TABLES = {"remote_manifest"}

#: Transaction-scoped stage tables the bulk paths create first.
STAGE_TABLES = {
    "_stage": "files_index", "_dir_bundles": "directory_archive_bundles",
    "_dir_stats": "directory_archive_stats",
    "_dir_tree": "directory_tree_index",
    "_stage_acm": "archive_container_members",
}


def _constant_sql(node):
    """The string value of a literal (or concatenation of literals)."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left, right = _constant_sql(node.left), _constant_sql(node.right)
        if left is not None and right is not None:
            return left + right
    return None


class _Harvester(ast.NodeVisitor):
    def __init__(self):
        self.stack = []
        self.found = []        # (function path, lineno, sql)
        self.dynamic = 0

    def _visit_function(self, node):
        self.stack.append(node.name)
        self.generic_visit(node)
        self.stack.pop()

    visit_FunctionDef = _visit_function
    visit_AsyncFunctionDef = _visit_function

    def visit_Call(self, node):
        func = node.func
        name = getattr(func, "attr", None) or getattr(func, "id", None)
        if name in EXECUTE_NAMES and node.args:
            sql = _constant_sql(node.args[0])
            if sql is None:
                self.dynamic += 1
            elif re.match(r"\s*(SELECT|INSERT|UPDATE|DELETE|WITH|CREATE|DROP"
                          r"|ALTER|SET|VACUUM|ANALYZE|DO)\b", sql, re.I):
                self.found.append((tuple(self.stack), node.lineno, sql))
        self.generic_visit(node)


def harvest():
    """``[(module, function path, lineno, sql)]`` plus the dynamic count."""
    files = [os.path.join(REPO_ROOT, "src", name)
             for name in sorted(os.listdir(os.path.join(REPO_ROOT, "src")))
             if name.endswith(".py")]
    files.append(os.path.join(REPO_ROOT, "inspect_db.py"))
    statements, dynamic = [], 0
    for path in files:
        with open(path, encoding="utf-8") as handle:
            tree = ast.parse(handle.read(), filename=path)
        harvester = _Harvester()
        harvester.visit(tree)
        dynamic += harvester.dynamic
        for stack, lineno, sql in harvester.found:
            statements.append((os.path.basename(path), stack, lineno, sql))
    return statements, dynamic


def _is_postgresql_only(module, stack):
    if module in POSTGRESQL_ONLY_MODULES:
        return True
    allowed = POSTGRESQL_ONLY_FUNCTIONS.get(module, ())
    for depth, name in enumerate(stack):
        if name in allowed:
            return True
        if depth and f"{name}:{stack[depth - 1]}" in allowed:
            return True
    return False


class PrepareAllStatementsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tdb = create_test_db(SQLITE, "prepare_all")
        cls.tdb.manager().close()
        cls.raw = sqlite3.connect(cls.tdb.path)
        for stage, source in STAGE_TABLES.items():
            cls.raw.execute(f"CREATE TEMP TABLE {stage} AS "
                            f"SELECT * FROM {source} WHERE 0")
        cls.statements, cls.dynamic = harvest()

    @classmethod
    def tearDownClass(cls):
        cls.raw.close()
        cls.tdb.drop()

    def _explain(self, sql):
        statement = rewrite(sql)
        if statement.keyword in ("VACUUM", "ANALYZE"):
            return
        if statement.param_style == "named":
            params = {name: None
                      for name in re.findall(r":(\w+)", statement.sql)}
        else:
            params = [None] * statement.param_count
        self.raw.execute("EXPLAIN " + statement.sql, params)

    def test_the_harvest_is_not_empty(self):
        """A refactor that hides the SQL from this test must be noticed."""
        portable = [item for item in self.statements
                    if not _is_postgresql_only(item[0], item[1])]
        self.assertGreater(len(self.statements), 400)
        self.assertGreater(len(portable), 300)

    def test_every_portable_literal_statement_prepares_on_sqlite(self):
        failures = []
        checked = 0
        defects_seen = set()
        for module, stack, lineno, sql in self.statements:
            if _is_postgresql_only(module, stack):
                continue
            checked += 1
            defect = next((key for key in KNOWN_DEFECTS_ON_BOTH_BACKENDS
                           if key[0] == module and key[1] in stack), None)
            try:
                self._explain(sql)
            except (SqliteDialectError, sqlite3.Error) as exc:
                if defect:
                    defects_seen.add(defect)
                    continue
                missing = re.match(r"no such table: (\w+)", str(exc))
                if missing and missing.group(1) in LEGACY_ABSENT_TABLES:
                    continue
                failures.append(
                    f"{module}:{lineno} in {'.'.join(stack) or '<module>'}: "
                    f"{type(exc).__name__}: {str(exc)[:200]}")
        self.assertEqual(
            failures, [],
            f"{len(failures)} of {checked} statements do not prepare on "
            "SQLite. Make the SQL portable or move it behind a backend "
            "hook:\n  " + "\n  ".join(failures))
        self.assertEqual(
            defects_seen, set(KNOWN_DEFECTS_ON_BOTH_BACKENDS),
            "a listed known defect now prepares: remove it from "
            "KNOWN_DEFECTS_ON_BOTH_BACKENDS")

    def test_legacy_absent_tables_are_absent_from_postgresql_too(self):
        import json
        with open(os.path.join(REPO_ROOT, "tests", "data",
                               "pg_schema_expectation.json"),
                  encoding="utf-8") as handle:
            tables = set(json.load(handle)["tables"])
        self.assertFalse(LEGACY_ABSENT_TABLES & tables)

    def test_the_allow_list_names_only_things_that_exist(self):
        """An allow-list entry for code that was renamed would silently stop
        protecting anything."""
        present = {}
        for module, stack, _lineno, _sql in self.statements:
            names = present.setdefault(module, set())
            for depth, name in enumerate(stack):
                names.add(name)
                if depth:
                    names.add(f"{name}:{stack[depth - 1]}")
        src = os.path.join(REPO_ROOT, "src")
        for module in POSTGRESQL_ONLY_MODULES:
            self.assertTrue(os.path.isfile(os.path.join(src, module)), module)
        for module, functions in POSTGRESQL_ONLY_FUNCTIONS.items():
            with open(os.path.join(src, module), encoding="utf-8") as handle:
                text = handle.read()
            for function in functions:
                name = function.split(":")[0]
                self.assertRegex(text, rf"def {re.escape(name)}\(",
                                 f"{module}: {function}")

    def test_postgresql_only_modules_match_the_feature_list(self):
        from src.db_backend import POSTGRESQL_ONLY_FEATURES
        for module, feature in POSTGRESQL_ONLY_MODULES.items():
            if feature in POSTGRESQL_ONLY_FEATURES:
                continue
            self.assertTrue(
                feature.startswith(("native SQLite", "PostgreSQL pool")),
                f"{module} is exempt for an undeclared reason: {feature}")


if __name__ == "__main__":
    unittest.main()
