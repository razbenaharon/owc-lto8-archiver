"""A brand-new PostgreSQL database must be able to archive after first run.

PostgreSQL-specific by nature (SQLite always installs the complete schema at
creation): start-up used to create the base tables only, a packed archive then
refused for want of the directory catalog, and the explicit migration commands
demand a verified ``pg_dump`` backup - of an empty database.

The rule under test is strict in both directions:

* a database that held NO catalog before start-up gets the complete schema;
* a database with ANY pre-existing table is opened exactly as before - the
  explicit migrations are never applied implicitly to an existing catalog.
"""
import contextlib
import io
import threading
import unittest
from types import SimpleNamespace
from unittest import mock

from db_backends import BackendTestDb, postgres_enabled
from pg_test_guard import (SKIP_REASON, create_test_database,
                           drop_test_database)
from src.db import create_database_manager

ALL_STEPS = ("007", "014 + finalize", "015", "016 + 017", "018", "019")
MARKER = "lto_fresh_schema_install"


def _cfg(conninfo):
    return SimpleNamespace(db_backend="postgresql", db_dsn=conninfo)


def _migration_state(db):
    return {
        "directory_catalog": db.directory_catalog_schema_installed(),
        "incremental_scan": db.incremental_scan_schema_finalized(),
        # The v2 authority covers 015+016+017 together; "ready" also proves
        # the installed definitions match its recorded fingerprint.
        "container_format":
            db.container_format_schema_authority_v2_report()["ready"],
        "stored_tar_plan": db.stored_tar_plan_schema_report()[
            "installation_state"] == "installed",
        "manifest_directory_catalog":
            db.manifest_directory_catalog_schema_report()[
                "installation_state"] == "installed",
        "authority_v2":
            db.container_format_schema_authority_v2_report()["installed"],
    }


@unittest.skipUnless(postgres_enabled(), SKIP_REASON)
class FreshPostgresDatabaseTests(unittest.TestCase):
    def setUp(self):
        self.name, self.conninfo = create_test_database("fresh_schema")
        self.managers = []

    def tearDown(self):
        for db in self.managers:
            try:
                db.close()
            except Exception:
                pass
        drop_test_database(self.name)

    def _open_as_the_application(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            db = create_database_manager(_cfg(self.conninfo))
        self.managers.append(db)
        return db, out.getvalue()

    def _open_directly(self, **kwargs):
        from src.pg_db import PgDatabaseManager
        db = PgDatabaseManager(self.conninfo, **kwargs)
        self.managers.append(db)
        return db

    def _tables(self):
        import psycopg
        with psycopg.connect(self.conninfo, autocommit=True) as conn:
            return {row[0] for row in conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = current_schema()").fetchall()}

    # -- a database that held nothing --------------------------------------
    def test_an_empty_database_gets_the_complete_schema_and_says_so(self):
        db, printed = self._open_as_the_application()

        state = _migration_state(db)
        self.assertEqual(state, dict.fromkeys(state, True))
        self.assertEqual(db.fresh_schema_installed, ALL_STEPS)
        self.assertIn("New, empty PostgreSQL database: installed", printed)
        self.assertIn(", ".join(ALL_STEPS), printed)
        self.assertNotIn(MARKER, self._tables())

    def test_it_matches_what_the_explicit_migrations_produce(self):
        """The result has exactly the tables, and the same migration state,
        as a database migrated explicitly one command at a time."""
        db, _printed = self._open_as_the_application()
        automatic_state = _migration_state(db)
        automatic = self._tables()

        other_name, other_conninfo = create_test_database("fresh_explicit")
        try:
            from src.pg_db import PgDatabaseManager
            explicit = PgDatabaseManager(other_conninfo)
            try:
                BackendTestDb.apply_all_migrations(explicit)
                self.assertEqual(_migration_state(explicit), automatic_state)
                rows = explicit._run_read(
                    lambda conn: conn.execute(
                        "SELECT table_name FROM information_schema.tables "
                        "WHERE table_schema = current_schema()").fetchall(),
                    "list tables")
            finally:
                explicit.close()
        finally:
            drop_test_database(other_name)
        self.assertEqual(automatic, {row["table_name"] for row in rows})

    def test_a_packed_archive_can_be_cataloged_and_found_again(self):
        """The exact precondition a packed archive run checks before its
        tape write, then the catalog writes and reads such a run performs."""
        db, _printed = self._open_as_the_application()
        self.assertTrue(db.directory_catalog_schema_installed())

        db.register_tape("TFRESH")
        records = [{
            "original_path": "/data/project/dir/a.txt",
            "file_size_bytes": 7,
            "tape_label": "TFRESH",
            "source_host": "host",
            "is_packed": True,
            "container_name": "Bundle_A.zip",
            "stored_path": "dir/a.txt",
        }, {
            "original_path": "/data/project/big.bin",
            "file_size_bytes": 20 * 1024 * 1024,
            "tape_label": "TFRESH",
            "source_host": "host",
            "is_packed": False,
            "container_name": None,
            "stored_path": "/data/project/big.bin",
        }]
        totals = db.bulk_upsert_files([dict(item) for item in records])
        self.assertEqual(totals["inserted"], 2)
        db.bulk_upsert_directory_catalog(
            [dict(item) for item in records], "TFRESH", "host",
            local_session_id=None, local_chunk_index=0, tape_root="ROOT")

        with db._pool.connection() as conn:
            files = conn.execute(
                """SELECT original_path, is_packed, stored_path
                   FROM files_index WHERE tape_label='TFRESH'
                   ORDER BY original_path""").fetchall()
            bundles = conn.execute(
                "SELECT count(*) AS n FROM directory_archive_bundles "
                "WHERE tape_label='TFRESH'").fetchone()["n"]
            conn.rollback()
        self.assertEqual(
            [(row["original_path"], row["is_packed"], row["stored_path"])
             for row in files],
            [("/data/project/big.bin", False, "/data/project/big.bin"),
             ("/data/project/dir/a.txt", True, "dir/a.txt")])
        self.assertEqual(bundles, 1)

    def test_a_second_start_installs_nothing_more(self):
        self._open_as_the_application()
        again, printed = self._open_as_the_application()
        self.assertEqual(again.fresh_schema_installed, ())
        self.assertNotIn("New, empty PostgreSQL database", printed)

    def test_two_programs_starting_together_install_it_once(self):
        results, errors = [], []

        def start():
            try:
                results.append(self._open_as_the_application()[0])
            except Exception as exc:            # pragma: no cover - reported
                errors.append(exc)

        threads = [threading.Thread(target=start) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(120)
        self.assertEqual(errors, [])
        self.assertEqual(
            sorted(db.fresh_schema_installed for db in results),
            [(), ALL_STEPS])
        state = _migration_state(results[0])
        self.assertEqual(state, dict.fromkeys(state, True))

    # -- an installation that was interrupted ------------------------------
    def test_an_interrupted_installation_continues_on_the_next_start(self):
        from src.pg_db import PgDatabaseManager
        with mock.patch.object(
                PgDatabaseManager, "apply_container_format_schema",
                side_effect=RuntimeError("disk full")):
            with self.assertRaisesRegex(
                    RuntimeError, "stopped at migration 015") as caught:
                self._open_as_the_application()
        self.assertIn("installed in this run: 007, 014 + finalize",
                      str(caught.exception))
        self.assertIn("start the program again", str(caught.exception))
        self.assertIn(MARKER, self._tables())

        db, printed = self._open_as_the_application()

        self.assertEqual(db.fresh_schema_installed,
                         ("015", "016 + 017", "018", "019"))
        self.assertIn("finished installing", printed)
        state = _migration_state(db)
        self.assertEqual(state, dict.fromkeys(state, True))
        self.assertNotIn(MARKER, self._tables())

    def test_an_interrupted_installation_is_not_resumed_once_data_exists(self):
        """A marked database that has since been used is an existing catalog:
        nothing is applied to it implicitly."""
        from src.pg_db import PgDatabaseManager
        with mock.patch.object(
                PgDatabaseManager, "apply_directory_catalog_schema",
                side_effect=RuntimeError("stopped")):
            with self.assertRaises(RuntimeError):
                self._open_as_the_application()
        used = self._open_directly()
        with contextlib.redirect_stdout(io.StringIO()):
            used.register_tape("TUSED")
        used.close()

        db, printed = self._open_as_the_application()

        self.assertEqual(db.fresh_schema_installed, ())
        self.assertNotIn("New, empty PostgreSQL database", printed)
        self.assertFalse(db.directory_catalog_schema_installed())

    # -- a catalog that already existed ------------------------------------
    def test_an_existing_base_only_catalog_is_left_exactly_as_it_was(self):
        base = self._open_directly()                 # start-up schema only
        self.assertFalse(base.directory_catalog_schema_installed())
        base.close()
        before = self._tables()

        db, printed = self._open_as_the_application()

        self.assertEqual(db.fresh_schema_installed, ())
        self.assertNotIn("New, empty PostgreSQL database", printed)
        self.assertFalse(db.directory_catalog_schema_installed())
        self.assertFalse(db.incremental_scan_schema_finalized())
        self.assertFalse(db.container_format_schema_report()["ready"])
        self.assertEqual(self._tables(), before)

    def test_one_unrelated_table_already_makes_the_database_existing(self):
        import psycopg
        with psycopg.connect(self.conninfo, autocommit=True) as conn:
            conn.execute("CREATE TABLE something_of_the_operators (id int)")

        db, printed = self._open_as_the_application()

        self.assertEqual(db.fresh_schema_installed, ())
        self.assertNotIn("New, empty PostgreSQL database", printed)
        self.assertFalse(db.directory_catalog_schema_installed())

    def test_constructing_the_manager_directly_never_completes_a_schema(self):
        """Tools and tests that build a manager themselves keep the old
        behaviour; only the application's start-up opts in."""
        db = self._open_directly()
        self.assertEqual(db.fresh_schema_installed, ())
        self.assertFalse(db.directory_catalog_schema_installed())


if __name__ == "__main__":
    unittest.main()
