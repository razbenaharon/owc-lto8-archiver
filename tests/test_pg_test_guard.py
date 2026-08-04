"""The PostgreSQL test guard must reject every unsafe target — proven here.

A guard nobody exercises is a guard nobody can trust, and this one is protecting
the production archive catalog. Every rule in ``tests/pg_test_guard.py`` gets a
test that a *wrong* configuration is refused, and refused by RAISING rather than
skipping: a skip would let an unsafe run look green.

None of these tests opens a connection. The DSN checks are static by design
(:func:`assert_safe_dsn` never connects), and the one rule that must consult a
server — "does this server host the production catalog?" — is exercised against
a fake connection object.
"""
import os
import unittest
from unittest import mock

import pg_test_guard as guard
from pg_test_guard import TEST_DSN_ENV, UnsafeTestDatabase

SAFE = "postgresql://lto:pw@127.0.0.1:15432/postgres"


class UnsafeDsnIsRejectedTests(unittest.TestCase):
    """Each of these is a configuration an operator could plausibly type."""

    def _refused(self, dsn, expected_fragment):
        with self.assertRaises(UnsafeTestDatabase) as caught:
            guard.assert_safe_dsn(dsn)
        self.assertIn(expected_fragment, str(caught.exception).lower())

    def test_an_empty_dsn_is_refused(self):
        self._refused("", "explicit disposable server")
        self._refused("   ", "explicit disposable server")

    def test_the_production_port_is_refused(self):
        """5432 is the production container on this host."""
        self._refused("postgresql://lto:pw@127.0.0.1:5432/postgres",
                      "standard postgresql port")

    def test_the_production_port_is_refused_in_keyword_form(self):
        self._refused("host=127.0.0.1 port=5432 dbname=postgres",
                      "standard postgresql port")

    def test_a_missing_port_is_refused_rather_than_defaulted(self):
        """An omitted port means 5432 — the exact thing being guarded against."""
        self._refused("postgresql://lto:pw@127.0.0.1/postgres",
                      "does not name a port")

    def test_a_missing_host_is_refused_rather_than_defaulted(self):
        self._refused("dbname=postgres port=15432", "does not name a host")

    def test_a_non_loopback_host_is_refused(self):
        self._refused("postgresql://lto:pw@10.0.0.5:15432/postgres",
                      "only a loopback address")
        self._refused("postgresql://lto:pw@db.example.org:15432/postgres",
                      "only a loopback address")

    def test_a_missing_database_is_refused_rather_than_defaulted(self):
        """The implicit dbname is 'lto_archive' — the production catalog."""
        self._refused("host=127.0.0.1 port=15432", "does not name a database")

    def test_naming_the_production_catalog_is_refused(self):
        for name in sorted(guard.PRODUCTION_DATABASE_NAMES):
            self._refused(f"postgresql://lto:pw@127.0.0.1:15432/{name}",
                          "production")

    def test_a_malformed_dsn_is_refused(self):
        self._refused("this is not a dsn at all", "not a valid dsn")

    def test_a_safe_dsn_is_accepted(self):
        self.assertEqual(guard.assert_safe_dsn(SAFE),
                         ("127.0.0.1", 15432, "postgres"))

    def test_the_docstring_command_is_itself_safe(self):
        """The command the module tells operators to run must pass its own rules.

        Documented instructions drift out of step with code; this ties them.
        """
        import re
        match = re.search(r'LTO_TEST_PG_DSN = "([^"]+)"', guard.__doc__)
        self.assertIsNotNone(match, "the documented DSN example disappeared")
        guard.assert_safe_dsn(match.group(1))


class UnsafeConfigurationFailsRatherThanSkipsTests(unittest.TestCase):
    """The distinction that matters: not configured skips, MISconfigured fails."""

    def test_an_unset_variable_means_not_configured(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop(TEST_DSN_ENV, None)
            self.assertIsNone(guard.configured_dsn())
            self.assertFalse(guard.pg_available())

    def test_a_configured_but_unsafe_target_raises_from_configured_dsn(self):
        with mock.patch.dict(
                os.environ,
                {TEST_DSN_ENV: "postgresql://lto:pw@127.0.0.1:5432/lto_archive"}):
            with self.assertRaises(UnsafeTestDatabase):
                guard.configured_dsn()

    def test_a_configured_but_unsafe_target_raises_from_pg_available(self):
        """pg_available must NOT return False for an unsafe target.

        Returning False would turn a dangerous misconfiguration into a quiet
        skip, and the suite would report green while pointing at production.
        """
        with mock.patch.dict(
                os.environ,
                {TEST_DSN_ENV: "postgresql://lto:pw@127.0.0.1:5432/postgres"}):
            with self.assertRaises(UnsafeTestDatabase):
                guard.pg_available()

    def test_an_empty_string_is_misconfiguration_not_absence(self):
        with mock.patch.dict(os.environ, {TEST_DSN_ENV: ""}):
            with self.assertRaises(UnsafeTestDatabase):
                guard.configured_dsn()


class ProductionServerDetectionTests(unittest.TestCase):
    """Host and port are conventions; hosting ``lto_archive`` is what production
    IS. This check is the one that cannot be talked around by renaming a port."""

    @staticmethod
    def _fake_psycopg(datnames):
        cursor = mock.Mock()
        cursor.fetchall.return_value = [(name,) for name in datnames]
        conn = mock.MagicMock()
        conn.__enter__.return_value = conn
        conn.execute.return_value = cursor
        module = mock.Mock()
        module.connect.return_value = conn
        return module

    def test_a_server_hosting_the_production_catalog_is_refused(self):
        fake = self._fake_psycopg(["lto_archive"])
        with mock.patch.dict("sys.modules", {"psycopg": fake}):
            with self.assertRaises(UnsafeTestDatabase) as caught:
                guard.assert_server_is_not_production(SAFE)
        self.assertIn("PRODUCTION", str(caught.exception))

    def test_a_server_hosting_a_DATED_production_catalog_is_refused(self):
        """The live catalog is not named 'lto_archive'.

        On this host it is ``lto_archive_directory_catalog_20260710_103359``,
        and ``--create-migrated-db`` mints a new dated name each time. Matching
        only the exact name would stop recognising production the day
        ``lto_archive`` is dropped.
        """
        fake = self._fake_psycopg(
            ["lto_archive_directory_catalog_20260710_103359"])
        with mock.patch.dict("sys.modules", {"psycopg": fake}):
            with self.assertRaises(UnsafeTestDatabase) as caught:
                guard.assert_server_is_not_production(SAFE)
        self.assertIn("PRODUCTION", str(caught.exception))

    def test_a_server_without_the_production_catalog_is_accepted(self):
        fake = self._fake_psycopg([])
        with mock.patch.dict("sys.modules", {"psycopg": fake}):
            self.assertTrue(guard.assert_server_is_not_production(SAFE))

    def test_the_run_prefix_does_not_look_like_production(self):
        """This run's own databases must not trip the production matcher."""
        import fnmatch
        name = guard.test_database_name("db")
        for pattern in guard.PRODUCTION_MARKER_PATTERNS:
            self.assertFalse(
                fnmatch.fnmatch(name, pattern.replace("%", "*")),
                f"{name!r} matches production pattern {pattern!r}")

    def test_the_check_only_reads(self):
        """It must not create, drop or write anything while deciding."""
        fake = self._fake_psycopg([])
        with mock.patch.dict("sys.modules", {"psycopg": fake}):
            guard.assert_server_is_not_production(SAFE)
        statements = [str(call.args[0]).upper()
                      for call in fake.connect.return_value.execute.call_args_list]
        self.assertTrue(statements)
        for statement in statements:
            self.assertTrue(statement.lstrip().startswith("SELECT"), statement)
            for forbidden in ("DROP", "CREATE", "DELETE", "UPDATE", "INSERT",
                              "ALTER", "TRUNCATE"):
                self.assertNotIn(forbidden, statement)


class CleanupIsScopedToThisRunTests(unittest.TestCase):
    """Cleanup may delete only what this run created."""

    def test_generated_names_carry_this_runs_marker(self):
        name = guard.test_database_name("example")
        self.assertTrue(name.startswith(guard.RUN_PREFIX))
        self.assertTrue(guard.assert_created_by_this_run(name))

    def test_generated_names_are_unique(self):
        names = {guard.test_database_name("x") for _ in range(50)}
        self.assertEqual(len(names), 50)

    def test_generated_names_contain_no_quoting_hazard(self):
        name = guard.test_database_name('evil"; DROP DATABASE lto_archive; --')
        self.assertRegex(name, r"^[A-Za-z0-9_]+$")

    def test_dropping_a_foreign_database_is_refused(self):
        for name in ("lto_archive", "postgres", "lto_test_deadbeef",
                     "ltotest_someotherrun_db_1234"):
            with self.assertRaises(UnsafeTestDatabase) as caught:
                guard.assert_created_by_this_run(name)
            self.assertIn("does not carry this run's marker",
                          str(caught.exception))

    def test_drop_test_database_refuses_before_connecting(self):
        """The refusal must happen without a connection being opened."""
        fake = mock.Mock()
        with mock.patch.dict("sys.modules", {"psycopg": fake}):
            with self.assertRaises(UnsafeTestDatabase):
                guard.drop_test_database("lto_archive")
        fake.connect.assert_not_called()

    def test_dsn_for_refuses_a_foreign_database(self):
        with self.assertRaises(UnsafeTestDatabase):
            guard.dsn_for("lto_archive")

    def test_create_refuses_when_nothing_is_configured(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop(TEST_DSN_ENV, None)
            with self.assertRaises(UnsafeTestDatabase):
                guard.create_test_database("x")


class ConftestConnectionGuardTests(unittest.TestCase):
    """The session-wide net over ``psycopg.connect``.

    The per-module wiring protects the modules that use it; this protects the
    ones that forget, including production code called from a test.
    """

    def setUp(self):
        import conftest
        self.wrapper = conftest._guarded_connect(mock.Mock(name="real_connect"))

    def test_connecting_with_nothing_configured_is_blocked(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop(TEST_DSN_ENV, None)
            with self.assertRaises(UnsafeTestDatabase) as caught:
                self.wrapper("host=127.0.0.1 port=5432 dbname=lto_archive")
        self.assertIn("is not set", str(caught.exception))

    def test_connecting_with_implicit_defaults_is_blocked(self):
        """``psycopg.connect()`` with no DSN resolves to localhost:5432."""
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop(TEST_DSN_ENV, None)
            with self.assertRaises(UnsafeTestDatabase):
                self.wrapper()

    def test_a_connection_to_another_server_is_blocked(self):
        with mock.patch.dict(os.environ, {TEST_DSN_ENV: SAFE}):
            with self.assertRaises(UnsafeTestDatabase) as caught:
                self.wrapper("host=127.0.0.1 port=5432 dbname=postgres")
        self.assertIn("not the configured disposable test server",
                      str(caught.exception))

    def test_a_connection_to_the_configured_server_passes_through(self):
        with mock.patch.dict(os.environ, {TEST_DSN_ENV: SAFE}):
            self.wrapper("host=127.0.0.1 port=15432 dbname=ltotest_x")
        self.wrapper.__wrapped__.assert_called_once()

    def test_keyword_form_is_checked_too(self):
        with mock.patch.dict(os.environ, {TEST_DSN_ENV: SAFE}):
            with self.assertRaises(UnsafeTestDatabase):
                self.wrapper(host="127.0.0.1", port=5432, dbname="lto_archive")

    def test_the_guard_is_actually_installed_during_this_run(self):
        """Proves the hook ran — not merely that the wrapper works in isolation."""
        import psycopg
        self.assertTrue(hasattr(psycopg.connect, "__wrapped__"),
                        "conftest.pytest_configure did not wrap psycopg.connect")


if __name__ == "__main__":
    unittest.main()
