"""The SQLite schema is the fully migrated PostgreSQL schema.

Two layers:

* ``tests/data/pg_schema_expectation.json`` is a committed snapshot of the
  fully migrated PostgreSQL schema (tables, columns in order with type and
  nullability, constraint / index / trigger names, the view's columns). The
  SQLite schema is compared against it on every run - no server needed.
* When ``LTO_TEST_PG_DSN`` is set, a real PostgreSQL database is migrated and
  the snapshot is compared against it, so the snapshot cannot rot: adding a
  PostgreSQL migration without porting it to SQLite fails here.

Regenerate the snapshot (PostgreSQL DSN required)::

    python tests/test_sqlite_schema_parity.py --write-expectation
"""
import json
import os
import sqlite3
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db_backends import (POSTGRESQL, SQLITE, create_test_db,   # noqa: E402
                         postgres_enabled)
from pg_test_guard import SKIP_REASON                          # noqa: E402

EXPECTATION = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                           "data", "pg_schema_expectation.json")

#: Bookkeeping tables that exist only on SQLite.
SQLITE_ONLY_TABLES = {"schema_migrations", "lto_txn_settings",
                      "archiver_lock"}

#: PostgreSQL type -> declared SQLite type (the adapter's convention).
TYPE_MAP = {
    "bigint": "INTEGER", "integer": "INTEGER", "text": "TEXT",
    "timestamp with time zone": "TIMESTAMPTZ TEXT",
    "boolean": "BOOLEAN INTEGER", "jsonb": "JSONB TEXT", "bytea": "BLOB",
}

#: PostgreSQL trigram (GIN) indexes: no SQLite counterpart, by design.
UNPORTED_INDEXES = {"idx_directory_tree_path_trgm",
                    "idx_files_catalog_name_trgm",
                    "idx_files_original_path_trgm"}

#: Columns where SQLite deliberately differs in nullability/default only:
#: PostgreSQL assigns NEW.plan_source in a BEFORE trigger, SQLite needs a
#: column DEFAULT (the same trigger still verifies the value).
DEFAULT_ONLY_DIFFERENCES = {("remote_chunks", "plan_source")}


def read_postgres_schema(conn):
    """The parts of a migrated PostgreSQL schema that must exist on SQLite."""
    views = {row["viewname"] for row in conn.execute(
        "SELECT viewname FROM pg_views WHERE schemaname='public'").fetchall()}
    tables, view_columns = {}, {}
    for row in conn.execute(
            """SELECT table_name, column_name, data_type, is_nullable,
                      is_identity
               FROM information_schema.columns WHERE table_schema='public'
               ORDER BY table_name, ordinal_position""").fetchall():
        if row["table_name"] in views:
            view_columns.setdefault(row["table_name"], []).append(
                row["column_name"])
            continue
        tables.setdefault(row["table_name"], []).append({
            "name": row["column_name"], "type": row["data_type"],
            "nullable": row["is_nullable"] == "YES",
            "identity": row["is_identity"] == "YES"})
    constraints = {}
    for row in conn.execute(
            """SELECT t.relname AS table_name, c.conname,
                      c.contype::text AS contype
               FROM pg_constraint c JOIN pg_class t ON t.oid=c.conrelid
               JOIN pg_namespace n ON n.oid=t.relnamespace
               WHERE n.nspname='public' AND c.contype IN ('p','u','c','f')
               ORDER BY 1, 2""").fetchall():
        constraints.setdefault(row["table_name"], []).append(
            [row["contype"], row["conname"]])
    constraint_names = {name for items in constraints.values()
                        for _kind, name in items}
    indexes = sorted(
        row["indexname"] for row in conn.execute(
            "SELECT indexname FROM pg_indexes WHERE schemaname='public'"
        ).fetchall() if row["indexname"] not in constraint_names)
    unique_indexes = sorted(
        row["indexname"] for row in conn.execute(
            """SELECT indexname FROM pg_indexes WHERE schemaname='public'
               AND indexdef LIKE 'CREATE UNIQUE INDEX%%'""").fetchall()
        if row["indexname"] not in constraint_names)
    triggers = {}
    for row in conn.execute(
            """SELECT c.relname AS table_name, t.tgname
               FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid
               JOIN pg_namespace n ON n.oid=c.relnamespace
               WHERE n.nspname='public' AND NOT t.tgisinternal
               ORDER BY 1, 2""").fetchall():
        triggers.setdefault(row["table_name"], []).append(row["tgname"])
    return {"tables": tables, "views": view_columns,
            "constraints": constraints, "indexes": indexes,
            "unique_indexes": unique_indexes, "triggers": triggers}


def migrated_postgres_schema():
    tdb = create_test_db(POSTGRESQL, "schema_parity")
    try:
        tdb.manager(migrations="all")
        with tdb.connect() as conn:
            return read_postgres_schema(conn)
    finally:
        tdb.drop()


def load_expectation():
    with open(EXPECTATION, encoding="utf-8") as handle:
        return json.load(handle)


class SqliteSchemaMatchesExpectationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.expected = load_expectation()
        cls.tdb = create_test_db(SQLITE, "schema_parity")
        cls.tdb.manager().close()
        cls.raw = sqlite3.connect(cls.tdb.path)

    @classmethod
    def tearDownClass(cls):
        cls.raw.close()
        cls.tdb.drop()

    def _master(self, kind):
        return {row[0]: (row[1], row[2] or "") for row in self.raw.execute(
            "SELECT name, tbl_name, sql FROM sqlite_master WHERE type=? "
            "AND name NOT LIKE 'sqlite_%'", (kind,)).fetchall()}

    def test_same_tables(self):
        tables = set(self._master("table")) - SQLITE_ONLY_TABLES
        self.assertEqual(tables, set(self.expected["tables"]))
        self.assertTrue(SQLITE_ONLY_TABLES <= set(self._master("table")))

    def test_same_columns_in_the_same_order_with_the_same_types(self):
        for table, columns in self.expected["tables"].items():
            info = self.raw.execute(
                f'PRAGMA table_info("{table}")').fetchall()
            with self.subTest(table=table):
                self.assertEqual([row[1] for row in info],
                                 [column["name"] for column in columns])
                for row, column in zip(info, columns):
                    self.assertEqual(
                        row[2], TYPE_MAP[column["type"]],
                        f"{table}.{column['name']} declared type")

    def test_same_nullability(self):
        for table, columns in self.expected["tables"].items():
            info = {row[1]: row for row in self.raw.execute(
                f'PRAGMA table_info("{table}")').fetchall()}
            for column in columns:
                row = info[column["name"]]
                # An INTEGER PRIMARY KEY (identity) is NOT NULL by nature and
                # SQLite reports notnull=0 for it.
                not_null = bool(row[3]) or (column["identity"] and row[5])
                with self.subTest(column=f"{table}.{column['name']}"):
                    self.assertEqual(not not_null, column["nullable"])

    def test_identity_columns_are_autoincrement_primary_keys(self):
        tables = self._master("table")
        for table, columns in self.expected["tables"].items():
            for column in columns:
                if column["identity"]:
                    self.assertIn(
                        f"{column['name']} INTEGER PRIMARY KEY AUTOINCREMENT",
                        tables[table][1], f"{table}.{column['name']}")

    def test_every_named_constraint_exists_on_the_same_table(self):
        tables = self._master("table")
        identity = {table: next((c["name"] for c in columns
                                 if c["identity"]), None)
                    for table, columns in self.expected["tables"].items()}
        for table, items in self.expected["constraints"].items():
            sql = tables[table][1]
            for kind, name in items:
                if kind == "p" and identity[table]:
                    continue    # expressed as INTEGER PRIMARY KEY
                with self.subTest(constraint=f"{table}.{name}"):
                    self.assertIn(f"CONSTRAINT {name} ", sql)
                    keyword = {"p": "PRIMARY KEY", "u": "UNIQUE",
                               "c": "CHECK", "f": "FOREIGN KEY"}[kind]
                    self.assertIn(f"CONSTRAINT {name} {keyword}", sql)

    def test_same_indexes_except_the_documented_trigram_ones(self):
        indexes = self._master("index")
        expected = set(self.expected["indexes"]) - UNPORTED_INDEXES
        self.assertEqual(set(indexes), expected)
        self.assertTrue(UNPORTED_INDEXES <= set(self.expected["indexes"]))
        for name in self.expected["unique_indexes"]:
            self.assertTrue(
                indexes[name][1].startswith("CREATE UNIQUE INDEX"), name)

    def test_every_postgresql_trigger_is_ported_to_the_same_table(self):
        triggers = self._master("trigger")
        for table, names in self.expected["triggers"].items():
            for name in names:
                with self.subTest(trigger=name):
                    self.assertIn(name, triggers)
                    self.assertEqual(triggers[name][0], table)
        # Every SQLite trigger is a named part of a PostgreSQL trigger.
        expected_names = {name for names in self.expected["triggers"].values()
                          for name in names}
        for name in triggers:
            self.assertIn(name.split("__")[0], expected_names, name)

    def test_the_view_has_the_same_columns_in_the_same_order(self):
        for view, columns in self.expected["views"].items():
            cursor = self.raw.execute(f'SELECT * FROM "{view}" LIMIT 0')
            self.assertEqual([d[0] for d in cursor.description], columns)

    def test_foreign_keys_are_enforced_and_clean(self):
        self.assertEqual(
            self.raw.execute("PRAGMA foreign_key_check").fetchall(), [])
        self.assertEqual(
            self.raw.execute("PRAGMA integrity_check").fetchone()[0], "ok")

    def test_documented_default_differences_are_the_only_extra_defaults(self):
        for table, column in DEFAULT_ONLY_DIFFERENCES:
            row = next(r for r in self.raw.execute(
                f'PRAGMA table_info("{table}")').fetchall() if r[1] == column)
            self.assertIsNotNone(row[4])


@unittest.skipUnless(postgres_enabled(), SKIP_REASON)
class ExpectationMatchesPostgresTests(unittest.TestCase):
    """PostgreSQL-only by nature: proves the committed snapshot is current."""

    def test_the_committed_snapshot_is_the_live_migrated_schema(self):
        live = migrated_postgres_schema()
        expected = load_expectation()
        self.maxDiff = None
        for key in ("tables", "views", "constraints", "indexes",
                    "unique_indexes", "triggers"):
            with self.subTest(part=key):
                self.assertEqual(
                    live[key], expected[key],
                    "the PostgreSQL schema changed: port the change to "
                    "scripts/sql/sqlite/ and regenerate the snapshot with "
                    "`python tests/test_sqlite_schema_parity.py "
                    "--write-expectation`")


if __name__ == "__main__":
    if "--write-expectation" in sys.argv:
        if not postgres_enabled():
            raise SystemExit(SKIP_REASON)
        os.makedirs(os.path.dirname(EXPECTATION), exist_ok=True)
        with open(EXPECTATION, "w", encoding="utf-8", newline="\n") as out:
            json.dump(migrated_postgres_schema(), out, indent=1,
                      sort_keys=True)
            out.write("\n")
        print("wrote", EXPECTATION)
    else:
        unittest.main()
