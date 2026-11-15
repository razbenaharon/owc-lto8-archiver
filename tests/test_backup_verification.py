"""Catalog-backup verification depth (audit M13).

``pg_restore --list`` reads only the table of contents at the front of a dump,
so a dump truncated or corrupted afterwards used to "verify". These tests pin
the full read-through, the opt-in scratch restore, and the honesty of the
structured result.

The real-dump tests need the disposable PostgreSQL test server AND its Docker
container (for pg_dump/pg_restore); they never touch any other server.
"""
import os
import shutil
import subprocess
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from typing import Any, cast
from unittest import mock

from src import pg_backup
import pg_test_guard
from pg_test_guard import (SKIP_REASON, TEST_DSN_ENV, create_test_database,
                           drop_test_database, pg_available)

TEST_CONTAINER = os.environ.get("LTO_TEST_PG_DOCKER_CONTAINER", "lto_pg_test")


def _dsn_parts():
    from psycopg.conninfo import conninfo_to_dict
    return conninfo_to_dict(os.environ[TEST_DSN_ENV])


def _container_serves_test_dsn():
    """True only if TEST_CONTAINER publishes exactly the test DSN's port."""
    if not pg_available() or not shutil.which("docker"):
        return False
    if TEST_CONTAINER == pg_backup.DOCKER_DB_CONTAINER:
        return False                     # never the production container
    try:
        result = subprocess.run(
            ["docker", "port", TEST_CONTAINER, "5432"],
            text=True, capture_output=True, timeout=30)
    except (OSError, subprocess.SubprocessError):
        return False
    port = str(_dsn_parts().get("port") or "")
    return result.returncode == 0 and any(
        line.strip().endswith(":" + port)
        for line in result.stdout.splitlines())


class _FakeTools:
    """Stand-in for pg_restore: lists a TOC; optionally fails the read."""

    def __init__(self, read_error=None):
        self.read_error = read_error
        self.commands = []

    def __call__(self, args, env=None):
        self.commands.append(list(args))
        if "--list" in args:
            return SimpleNamespace(
                stdout="; Archive\n1; 0 1 TABLE DATA public t lto\n")
        if self.read_error:
            raise RuntimeError(self.read_error)
        return SimpleNamespace(stdout="")


class StructuredResultTests(unittest.TestCase):
    """No server needed: the contract and the honesty of the messages."""

    def setUp(self):
        tmp = TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.backup = Path(tmp.name) / "fixture.dump"
        self.backup.write_bytes(b"not-a-real-dump")
        self.cfg = SimpleNamespace(pg_host="db.example.invalid", lto_drive="")

    def _verify(self, tools, **kw):
        with mock.patch.object(pg_backup, "_run_command", tools), \
                mock.patch.object(pg_backup.shutil, "which",
                                  return_value="pg_restore"):
            return pg_backup.verify_backup(self.cfg, self.backup, **kw)

    def test_toc_alone_is_not_enough_to_pass(self):
        tools = _FakeTools(read_error="could not read from input file")
        result = self._verify(tools)
        self.assertFalse(result.ok)
        self.assertEqual([c.name for c in result.failures],
                         ["full_read_through"])
        self.assertEqual(result.level, pg_backup.BACKUP_LEVEL_STRUCTURE)
        with mock.patch.object(pg_backup, "_run_command", tools), \
                mock.patch.object(pg_backup.shutil, "which",
                                  return_value="pg_restore"):
            with self.assertRaisesRegex(RuntimeError, "full_read_through"):
                pg_backup.verify_backup_file(self.cfg, self.backup)

    def test_default_verification_reads_the_whole_archive(self):
        tools = _FakeTools()
        result = self._verify(tools)
        self.assertTrue(result.ok)
        self.assertEqual(result.level, pg_backup.BACKUP_LEVEL_READ_THROUGH)
        self.assertEqual([c.name for c in result.checks],
                         ["non_empty", "table_of_contents",
                          "full_read_through"])
        self.assertTrue(any("--list" not in cmd for cmd in tools.commands))
        self.assertTrue(Path(result.artifacts["restore_list_path"]).exists())

    def test_passing_result_still_states_what_was_not_verified(self):
        result = self._verify(_FakeTools())
        text = result.summary()
        self.assertIn("PASSED", text)
        self.assertIn("NOT restored", text)
        self.assertIn(pg_backup.NOT_RESTORED_STATEMENT, result.not_verified)
        shallow = self._verify(_FakeTools(), read_through=False)
        self.assertEqual(shallow.level, pg_backup.BACKUP_LEVEL_STRUCTURE)
        self.assertIn(pg_backup.NOT_READ_THROUGH_STATEMENT,
                      shallow.not_verified)

    def test_result_shape_is_backend_neutral(self):
        payload = self._verify(_FakeTools()).as_dict()
        self.assertEqual(
            set(payload),
            {"backend", "backup_path", "size_bytes", "sha256", "level",
             "checks", "not_verified", "artifacts", "details", "ok"})
        self.assertEqual(payload["backend"], "postgresql")
        self.assertEqual(len(payload["sha256"]), 64)
        for check in payload["checks"]:
            self.assertEqual(set(check), {"name", "passed", "detail"})

    def test_empty_backup_fails_without_running_any_tool(self):
        self.backup.write_bytes(b"")
        tools = _FakeTools()
        result = self._verify(tools)
        self.assertFalse(result.ok)
        self.assertEqual(tools.commands, [])
        self.assertEqual(result.level, pg_backup.BACKUP_LEVEL_NONE)

    def test_deep_verification_refuses_to_pick_a_server(self):
        with self.assertRaisesRegex(RuntimeError, "explicitly named"):
            pg_backup.deep_verify_backup(self.cfg, self.backup, None)
        with self.assertRaisesRegex(RuntimeError, "explicit host"):
            pg_backup.scratch_server(host="", port="", user="")

    def test_scratch_server_never_inherits_the_production_container(self):
        target = pg_backup.scratch_server(
            host="127.0.0.1", port=15432, user="lto")
        self.assertEqual(pg_backup._docker_container(target), "")
        self.assertNotEqual(pg_backup._docker_container(target),
                            pg_backup.DOCKER_DB_CONTAINER)

    def test_receipt_refuses_a_failed_verification(self):
        failed = self._verify(_FakeTools(read_error="boom"))
        with self.assertRaisesRegex(RuntimeError, "unverified"):
            pg_backup.create_backup_receipt(
                self.cfg, self.backup, verification=failed)


@unittest.skipUnless(_container_serves_test_dsn(),
                     SKIP_REASON + " (and its Docker container)")
class RealDumpVerificationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg
        cls.dbname, cls.conninfo = create_test_database("m13_backup")
        with cast(Any, psycopg.connect(cls.conninfo, autocommit=True)) as conn:
            conn.execute(
                "CREATE TABLE payload (id int PRIMARY KEY, body text)")
            conn.execute(
                "INSERT INTO payload SELECT g, md5(g::text) || md5((g*7)::text)"
                " FROM generate_series(1, 60000) g")
            conn.execute("CREATE TABLE small (id int PRIMARY KEY)")
            conn.execute("INSERT INTO small VALUES (1), (2), (3)")
        parts = _dsn_parts()
        cls.cfg = SimpleNamespace(
            pg_host=parts["host"], pg_port=str(parts["port"]),
            pg_dbname=cls.dbname, pg_user=parts["user"],
            pg_password=parts.get("password"), pg_sslmode="prefer",
            pg_docker_container=TEST_CONTAINER, lto_drive="")
        cls._tmp = TemporaryDirectory()
        cls.dump = Path(pg_backup.create_database_backup(
            cls.cfg, output_dir=cls._tmp.name))
        cls.good = cls.dump.read_bytes()

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()
        drop_test_database(cls.dbname)

    def _variant(self, name, data):
        path = Path(self._tmp.name) / name
        path.write_bytes(data)
        return path

    def test_intact_dump_passes_at_read_through_level(self):
        result = pg_backup.verify_backup(self.cfg, self.dump)
        self.assertTrue(result.ok, result.summary())
        self.assertEqual(result.level, pg_backup.BACKUP_LEVEL_READ_THROUGH)
        self.assertIn(pg_backup.NOT_RESTORED_STATEMENT, result.not_verified)

    def test_dump_truncated_after_its_table_of_contents_fails(self):
        self.assertGreater(len(self.good), 200_000)
        truncated = self._variant(
            "truncated.dump", self.good[:int(len(self.good) * 0.6)])
        # The table of contents alone still lists: this is the shallow check
        # that used to be the whole verification.
        shallow = pg_backup.verify_backup(
            self.cfg, truncated, read_through=False)
        self.assertTrue(shallow.ok, shallow.summary())
        with self.assertRaisesRegex(RuntimeError, "full_read_through"):
            pg_backup.verify_backup_file(self.cfg, truncated)

    def test_dump_corrupted_inside_its_table_data_fails(self):
        data = bytearray(self.good)
        at = int(len(data) * 0.7)
        for offset in range(at, at + 256):
            data[offset] ^= 0xFF
        corrupt = self._variant("corrupt.dump", bytes(data))
        result = pg_backup.verify_backup(self.cfg, corrupt)
        self.assertFalse(result.ok)
        self.assertEqual([c.name for c in result.failures],
                         ["full_read_through"])

    def _scratch(self):
        parts = _dsn_parts()
        return pg_backup.scratch_server(
            host=parts["host"], port=parts["port"], user=parts["user"],
            password=parts.get("password"), docker_container=TEST_CONTAINER)

    def _database_exists(self, name):
        import psycopg
        with cast(Any, psycopg.connect(self.conninfo, autocommit=True)) as conn:
            return conn.execute(
                "SELECT 1 FROM pg_database WHERE datname=%s",
                (name,)).fetchone() is not None

    def test_deep_verification_restores_and_drops_a_scratch_database(self):
        scratch = pg_test_guard.test_database_name("m13_scratch")
        result = pg_backup.deep_verify_backup(
            self.cfg, self.dump, self._scratch(), scratch_dbname=scratch)
        self.assertTrue(result.ok, result.summary())
        self.assertEqual(result.level, pg_backup.BACKUP_LEVEL_RESTORED)
        restored = result.details["scratch_restore"]
        self.assertEqual(restored["tables"],
                         {"public.payload": 60000, "public.small": 3})
        self.assertTrue(restored["dropped"])
        self.assertFalse(self._database_exists(scratch))
        self.assertNotIn(pg_backup.NOT_RESTORED_STATEMENT, result.not_verified)

    def test_deep_verification_never_reuses_an_existing_database(self):
        result = pg_backup.deep_verify_backup(
            self.cfg, self.dump, self._scratch(), scratch_dbname=self.dbname)
        self.assertFalse(result.ok)
        self.assertEqual(result.failures[0].name, "scratch_restore")
        # The pre-existing database was neither restored into nor dropped.
        self.assertTrue(self._database_exists(self.dbname))


if __name__ == "__main__":
    unittest.main()
