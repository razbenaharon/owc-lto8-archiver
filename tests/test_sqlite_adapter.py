"""Behaviour of the psycopg-shaped SQLite adapter.  Pure offline."""
import json
import os
import shutil
import sqlite3
import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

from src import sqlite_adapter as sa
from src.sqlite_dialect import SqliteDialectError

SCHEMA = """
CREATE TABLE items (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    qty INTEGER NOT NULL DEFAULT 0 CHECK (qty >= 0),
    created_at TIMESTAMPTZ TEXT,
    active BOOLEAN INTEGER,
    meta JSONB TEXT,
    blob BLOB
);
CREATE TABLE children (
    id INTEGER PRIMARY KEY,
    item_id INTEGER NOT NULL REFERENCES items(id)
);
CREATE TABLE counter (id INTEGER PRIMARY KEY, n INTEGER NOT NULL);
INSERT INTO counter VALUES (1, 0);
"""


class AdapterTestCase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="lto_sqlite_")
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)
        self.path = os.path.join(self.dir, "catalog.db")
        self.conn = sa.connect(self.path)
        self.addCleanup(self.conn.close)
        self.conn.executescript(SCHEMA)

    def insert(self, name, qty=0, conn=None):
        return (conn or self.conn).execute(
            "INSERT INTO items (name, qty) VALUES (%s, %s) RETURNING id",
            (name, qty)).fetchone()["id"]

    def names(self, conn=None):
        return [r["name"] for r in (conn or self.conn).execute(
            "SELECT name FROM items ORDER BY name").fetchall()]


class ConnectionSetupTests(AdapterTestCase):
    def test_pragmas_are_applied(self):
        raw = self.conn.raw
        self.assertEqual(
            raw.execute("PRAGMA journal_mode").fetchone()[0], "wal")
        self.assertEqual(raw.execute("PRAGMA synchronous").fetchone()[0], 2)
        self.assertEqual(raw.execute("PRAGMA foreign_keys").fetchone()[0], 1)
        self.assertEqual(
            raw.execute("PRAGMA busy_timeout").fetchone()[0], 30000)
        self.assertEqual(
            raw.execute("PRAGMA trusted_schema").fetchone()[0], 0)

    def test_old_sqlite_is_refused(self):
        with self.assertRaises(sa.SqliteVersionError):
            sa.require_sqlite_version((3, 37, 2))
        self.assertGreaterEqual(sa.require_sqlite_version(), (3, 38, 0))
        with mock.patch.object(sa.sqlite3, "sqlite_version_info", (3, 31, 0)):
            with self.assertRaises(sa.SqliteVersionError):
                sa.connect(os.path.join(self.dir, "x.db"))

    def test_unc_and_network_paths_are_refused(self):
        self.assertFalse(sa.is_network_path(self.path))
        if os.name == "nt":
            self.assertTrue(sa.is_network_path("//server/share/catalog.db"))
            self.assertTrue(sa.is_network_path(r"\\server\share\c.db"))
            self.assertTrue(sa.is_network_path(r"\\?\UNC\server\share\c.db"))
            with self.assertRaises(sa.NonLocalPathError):
                sa.connect("//server/share/catalog.db")
            with mock.patch.object(sa, "_windows_drive_type", return_value=4):
                self.assertTrue(sa.is_network_path(self.path))
                with self.assertRaises(sa.NonLocalPathError):
                    sa.connect(self.path)
            with mock.patch.object(sa, "_windows_drive_type",
                                   side_effect=OSError("x")):
                self.assertTrue(sa.is_network_path(self.path))
        else:
            with mock.patch.object(sa, "_linux_mount_fstype",
                                   return_value="nfs4"):
                self.assertTrue(sa.is_network_path(self.path))
                with self.assertRaises(sa.NonLocalPathError):
                    sa.connect(self.path)

    def test_wal_refusal_when_it_does_not_take_effect(self):
        # A read-only open of a non-WAL database must be refused.
        plain = os.path.join(self.dir, "plain.db")
        raw = sqlite3.connect(plain)
        raw.execute("CREATE TABLE t (a)")
        raw.commit()
        raw.close()
        with self.assertRaises(sa.FatalError) as ctx:
            sa.connect(plain, read_only=True)
        self.assertIn("WAL", str(ctx.exception))

    def test_missing_directory_is_fatal_not_retryable(self):
        with self.assertRaises(sa.FatalError) as ctx:
            sa.connect(os.path.join(self.dir, "nope", "c.db"))
        self.assertFalse(ctx.exception.retryable)

    def test_dsn_form(self):
        self.assertEqual(sa.path_from_dsn("plain.db"), "plain.db")
        dsn = "sqlite:///" + self.path.replace("\\", "/")
        with sa.connect(dsn) as conn:
            self.assertEqual(conn.execute(
                "SELECT n FROM counter").fetchone()["n"], 0)
        self.assertTrue(conn.closed)
        with self.assertRaises(sa.ProgrammingError):
            sa.path_from_dsn("sqlite://host/x")


class RowAndCursorTests(AdapterTestCase):
    def test_rows_are_dicts_with_positional_access(self):
        self.insert("a", 3)
        row = self.conn.execute(
            "SELECT id, name, qty FROM items WHERE name=%s", ("a",)).fetchone()
        self.assertIsInstance(row, dict)
        self.assertEqual(row["name"], "a")
        self.assertEqual(row[1], "a")
        self.assertEqual(row.keys(), ("id", "name", "qty"))
        self.assertEqual(dict(row), {"id": row["id"], "name": "a", "qty": 3})
        self.assertEqual(row.get("missing", 9), 9)
        from src.pg_core import _row, _rows
        self.assertEqual(_row(row)[2], 3)
        self.assertEqual(_rows([row])[0]["qty"], 3)

    def test_fetch_methods_and_iteration(self):
        for i in range(5):
            self.insert(f"n{i}", i)
        cur = self.conn.execute("SELECT qty FROM items ORDER BY qty")
        self.assertEqual(cur.rowcount, 5)
        self.assertEqual(cur.fetchone()["qty"], 0)
        self.assertEqual([r["qty"] for r in cur.fetchmany(2)], [1, 2])
        self.assertEqual([r["qty"] for r in cur.fetchall()], [3, 4])
        self.assertIsNone(cur.fetchone())
        cur = self.conn.execute("SELECT qty FROM items ORDER BY qty")
        self.assertEqual([r["qty"] for r in cur], [0, 1, 2, 3, 4])
        self.assertEqual(cur.description[0].name, "qty")

    def test_cursor_context_manager_and_named_parameters(self):
        with self.conn.cursor() as cur:
            cur.execute("INSERT INTO items (name, qty) VALUES (%(n)s, %(q)s)",
                        {"n": "x", "q": 2, "unused": 1})
            self.assertEqual(cur.rowcount, 1)
            cur.execute("SELECT qty FROM items WHERE name=%(n)s", {"n": "x"})
            self.assertEqual(cur.fetchone()["qty"], 2)
        self.assertTrue(cur.closed)
        with self.assertRaises(sa.InterfaceError):
            cur.execute("SELECT 1")

    def test_rowcount_for_dml_and_returning(self):
        for name in "abc":
            self.insert(name)
        cur = self.conn.execute("UPDATE items SET qty=qty+1 WHERE name<>%s",
                                ("a",))
        self.assertEqual(cur.rowcount, 2)
        with self.assertRaises(sa.ProgrammingError):
            cur.fetchone()
        self.assertEqual(self.conn.execute(
            "UPDATE items SET qty=5 WHERE name=%s", ("zz",)).rowcount, 0)
        cur = self.conn.execute(
            "UPDATE items SET qty=9 WHERE name = ANY(%s) RETURNING name",
            (["a", "c", "nope"],))
        self.assertEqual(cur.rowcount, 2)       # known before any fetch
        self.assertEqual(sorted(r["name"] for r in cur.fetchall()), ["a", "c"])
        cur = self.conn.execute(
            "INSERT INTO items (name) VALUES (%s) ON CONFLICT (name) "
            "DO NOTHING RETURNING id", ("a",))
        self.assertEqual((cur.rowcount, cur.fetchall()), (0, []))
        self.assertEqual(
            self.conn.execute("DELETE FROM items").rowcount, 3)

    def test_unfetched_returning_does_not_block_commit(self):
        self.conn.execute("INSERT INTO items (name) VALUES (%s) RETURNING id",
                          ("a",))
        self.conn.commit()
        self.assertFalse(self.conn.in_transaction)

    def test_executemany(self):
        with self.conn.cursor() as cur:
            cur.executemany("INSERT INTO items (name, qty) VALUES (%s, %s)",
                            [("a", 1), ("b", 2), ("c", 3)])
            self.assertEqual(cur.rowcount, 3)
            cur.executemany(
                "UPDATE items SET qty=%s WHERE name=%s RETURNING id",
                [(7, "a"), (8, "zz")])
            self.assertEqual(cur.rowcount, 1)
        self.conn.commit()
        self.assertEqual(self.names(), ["a", "b", "c"])

    def test_named_cursor_iterates_unbuffered(self):
        with self.conn.cursor() as cur:
            cur.executemany("INSERT INTO items (name) VALUES (%s)",
                            [(f"n{i:03d}",) for i in range(50)])
        self.conn.commit()
        with self.conn.cursor(name="export_1") as cur:
            cur.itersize = 7
            cur.execute("SELECT name FROM items ORDER BY name")
            self.assertEqual(cur.rowcount, -1)
            seen = [dict(row)["name"] for row in cur]
        self.assertEqual(len(seen), 50)
        self.assertEqual(seen[0], "n000")
        self.conn.commit()

    def test_copy_is_refused(self):
        with self.assertRaises(sa.NotSupportedError):
            self.conn.cursor().copy("COPY t FROM STDIN")

    def test_every_statement_goes_through_the_dialect(self):
        with self.assertRaises(SqliteDialectError):
            self.conn.execute("SELECT 1 FROM pg_locks")
        with self.assertRaises(SqliteDialectError):
            self.conn.execute("SELECT %s::date", ("2020-01-01",))
        self.assertFalse(self.conn.in_transaction)
        with self.assertRaises(sa.ProgrammingError):
            self.conn.execute("COMMIT")

    def test_parameter_count_and_shape_errors(self):
        with self.assertRaises(sa.ProgrammingError):
            self.conn.execute("SELECT %s, %s", (1,))
        with self.assertRaises(sa.ProgrammingError):
            self.conn.execute("SELECT %(a)s", (1,))
        with self.assertRaises(sa.ProgrammingError):
            self.conn.execute("SELECT %s", {"a": 1})
        with self.assertRaises(sa.InterfaceError):
            self.conn.execute("SELECT %s", ([1, 2],))


class TransactionTests(AdapterTestCase):
    def other(self, **kw):
        conn = sa.connect(self.path, **kw)
        self.addCleanup(conn.close)
        return conn

    def test_implicit_transaction_until_commit(self):
        self.insert("a")
        self.assertTrue(self.conn.in_transaction)
        self.assertEqual(self.names(self.other()), [])
        self.conn.commit()
        self.assertEqual(self.names(self.other()), ["a"])
        self.insert("b")
        self.conn.rollback()
        self.assertEqual(self.names(), ["a"])

    def test_transaction_block_commits_and_rolls_back(self):
        with self.conn.transaction():
            self.insert("a")
        self.assertFalse(self.conn.in_transaction)
        with self.assertRaises(RuntimeError):
            with self.conn.transaction():
                self.insert("b")
                raise RuntimeError("boom")
        self.assertFalse(self.conn.in_transaction)
        self.assertFalse(self.conn._has_write_lock)
        self.assertEqual(self.names(self.other()), ["a"])

    def test_transaction_uses_begin_immediate(self):
        seen = []
        self.conn.raw.set_trace_callback(seen.append)
        with self.conn.transaction():
            pass
        self.conn.raw.set_trace_callback(None)
        self.assertEqual(seen, ["BEGIN IMMEDIATE", "COMMIT"])

    def test_nested_savepoints(self):
        with self.conn.transaction():
            self.insert("outer")
            with self.assertRaises(sa.UniqueViolation):
                with self.conn.transaction():
                    self.insert("inner")
                    self.insert("outer")      # duplicate
            with self.conn.transaction():
                self.insert("kept")
                with self.assertRaises(ValueError):
                    with self.conn.transaction():
                        self.insert("deep")
                        raise ValueError("x")
            self.assertTrue(self.conn.in_transaction)
        self.assertEqual(self.names(self.other()), ["kept", "outer"])

    def test_transaction_inside_implicit_is_a_savepoint(self):
        self.insert("a")                      # opens the implicit transaction
        with self.conn.transaction():
            self.insert("b")
        self.assertTrue(self.conn.in_transaction)   # outer still open
        self.conn.rollback()
        self.assertEqual(self.names(), [])

    def test_force_rollback(self):
        with self.conn.transaction(force_rollback=True):
            self.insert("a")
        self.assertEqual(self.names(), [])
        self.conn.commit()

    def test_commit_inside_block_is_refused(self):
        with self.conn.transaction():
            with self.assertRaises(sa.ProgrammingError):
                self.conn.commit()
            with self.assertRaises(sa.ProgrammingError):
                self.conn.rollback()

    def test_autocommit(self):
        self.conn.autocommit = True
        self.insert("a")
        self.assertFalse(self.conn.in_transaction)
        self.assertFalse(self.conn._has_write_lock)
        self.assertEqual(self.names(self.other()), ["a"])
        with self.conn.transaction():
            self.insert("b")
            with self.assertRaises(sa.ProgrammingError):
                self.conn.autocommit = False
        self.conn.execute("VACUUM")
        self.conn.autocommit = False
        self.assertEqual(self.names(), ["a", "b"])
        self.conn.commit()

    def test_read_then_write_upgrades_the_transaction(self):
        self.conn.execute("SELECT n FROM counter").fetchone()
        self.assertTrue(self.conn.in_transaction)
        self.assertFalse(self.conn._has_write_lock)
        self.insert("a")
        self.assertTrue(self.conn._has_write_lock)
        self.conn.commit()
        self.assertFalse(self.conn._has_write_lock)
        self.assertEqual(self.names(self.other()), ["a"])

    def test_for_update_takes_the_write_lock(self):
        self.conn.execute(
            "SELECT n FROM counter WHERE id=%s FOR UPDATE", (1,)).fetchone()
        self.assertTrue(self.conn._has_write_lock)
        self.conn.rollback()
        self.assertFalse(self.conn._has_write_lock)

    def test_strict_mode_refuses_read_then_write(self):
        conn = self.other(strict_read_then_write=True)
        conn.execute("SELECT n FROM counter").fetchone()
        with self.assertRaises(sa.ProgrammingError):
            conn.execute("UPDATE counter SET n=n+1")
        conn.rollback()
        with conn.transaction():
            conn.execute("SELECT n FROM counter").fetchone()
            conn.execute("UPDATE counter SET n=n+1")

    def test_failed_statement_leaves_transaction_usable(self):
        with self.conn.transaction():
            self.insert("a")
            with self.assertRaises(sa.UniqueViolation):
                self.insert("a")
            self.insert("b")
        self.assertEqual(self.names(), ["a", "b"])
        self.conn.commit()

    def test_close_rolls_back_and_releases_the_lock(self):
        conn = self.other()
        self.insert("a", conn=conn)
        conn.close()
        self.assertTrue(conn.closed)
        self.insert("b")              # would block if the lock had leaked
        self.conn.commit()
        self.assertEqual(self.names(), ["b"])
        with self.assertRaises(sa.InterfaceError):
            conn.execute("SELECT 1")

    def test_settings_udf_is_cleared_at_transaction_end(self):
        with self.conn.transaction():
            self.conn.set_setting("lto.checksum", "abc")
            row = self.conn.execute(
                "SELECT lto_setting(%s) AS v, lto_setting('nope') AS w",
                ("lto.checksum",)).fetchone()
            self.assertEqual((row["v"], row["w"]), ("abc", None))
        self.assertEqual(self.conn.settings, {})
        self.assertIsNone(self.conn.execute(
            "SELECT lto_setting('lto.checksum') AS v").fetchone()["v"])
        self.conn.set_setting("k", 1)
        self.conn.rollback()
        self.assertEqual(self.conn.settings, {})

    def test_executescript_is_atomic(self):
        with self.assertRaises(sa.SqliteError):
            self.conn.executescript(
                "CREATE TABLE s1 (a); CREATE TABLE s1 (a);")
        self.assertFalse(self.conn.in_transaction)
        self.assertFalse(self.conn._has_write_lock)
        self.assertIsNone(self.conn.execute(
            "SELECT name FROM sqlite_master WHERE name='s1'").fetchone())
        self.conn.commit()

    def test_statement_timeout_cancels(self):
        self.conn.statement_timeout = 0.05
        with self.assertRaises(sa.QueryCanceled):
            self.conn.execute(
                "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 "
                "FROM c) SELECT COUNT(*) FROM c")
        self.conn.statement_timeout = None
        self.conn.rollback()
        self.assertEqual(self.conn.execute("SELECT 1 AS a").fetchone()["a"], 1)
        self.conn.commit()


class TypeRoundTripTests(AdapterTestCase):
    seq = 0

    def roundtrip(self, column, value):
        TypeRoundTripTests.seq += 1
        item = self.conn.execute(
            f"INSERT INTO items (name, {column}) VALUES (%s, %s) "
            "RETURNING id", (f"row{TypeRoundTripTests.seq}", value)
        ).fetchone()["id"]
        return self.conn.execute(
            f"SELECT {column} AS v FROM items WHERE id=%s", (item,)
        ).fetchone()["v"]

    def test_aware_datetimes(self):
        utc = datetime(2026, 11, 15, 12, 30, 45, 123456, tzinfo=timezone.utc)
        back = self.roundtrip("created_at", utc)
        self.assertEqual(back, utc)
        self.assertEqual(back.utcoffset(), timedelta(0))
        other = datetime(2026, 1, 2, 3, 4, 5,
                         tzinfo=timezone(timedelta(hours=2)))
        back = self.roundtrip("created_at", other)
        self.assertEqual(back, other)
        self.assertEqual(back.tzinfo, timezone.utc)
        self.assertEqual(back.hour, 1)

    def test_timestamp_storage_is_canonical_and_orderable(self):
        value = datetime(2026, 11, 15, 12, 0, 0, tzinfo=timezone.utc)
        self.assertEqual(sa.format_timestamptz(value),
                         "2026-11-15 12:00:00.000000+00:00")
        self.roundtrip("created_at", value)
        stored = self.conn.raw.execute(
            "SELECT CAST(created_at AS TEXT) FROM items").fetchone()[0]
        self.assertEqual(stored, "2026-11-15 12:00:00.000000+00:00")
        # now() and Python-written values compare correctly as text.
        row = self.conn.execute(
            "SELECT COUNT(*) AS n FROM items WHERE created_at < now() "
            "AND created_at > %s", (value - timedelta(days=1),)).fetchone()
        self.assertEqual(row["n"], 1)
        self.conn.execute("UPDATE items SET created_at=now()")
        fresh = self.conn.execute(
            "SELECT created_at FROM items").fetchone()["created_at"]
        self.assertLess(abs((datetime.now(timezone.utc) - fresh)
                            .total_seconds()), 60)

    def test_naive_datetime_is_taken_as_local_time(self):
        naive = datetime(2026, 6, 1, 8, 0, 0)
        self.assertEqual(self.roundtrip("created_at", naive),
                         naive.astimezone(timezone.utc))

    def test_boolean(self):
        self.assertIs(self.roundtrip("active", True), True)
        self.assertIs(self.roundtrip("active", False), False)
        self.assertIsNone(self.roundtrip("active", None))
        self.assertEqual(self.conn.execute(
            "SELECT COUNT(*) AS n FROM items WHERE active IS TRUE"
        ).fetchone()["n"], 1)

    def test_json(self):
        payload = {"a": [1, 2, {"b": None}], "text": "\u05e9\u05dc\u05d5\u05dd"}
        self.assertEqual(self.roundtrip("meta", json.dumps(payload)), payload)
        self.assertEqual(self.roundtrip("meta", json.dumps([1, "x"])),
                         [1, "x"])
        row = self.conn.execute(
            "SELECT COALESCE((meta->>'n')::bigint, 0) AS n FROM items "
            "WHERE meta->>'text' IS NOT NULL").fetchone()
        self.assertEqual(row["n"], 0)

    def test_bytes(self):
        data = bytes(range(256))
        self.assertEqual(self.roundtrip("blob", data), data)
        self.assertEqual(self.roundtrip("blob", bytearray(b"\x00\x01")),
                         b"\x00\x01")
        self.assertEqual(self.roundtrip("blob", memoryview(b"xy")), b"xy")

    def test_list_parameter_for_any(self):
        ids = [self.insert(n, q) for n, q in (("a", 1), ("b", 2), ("c", 3))]
        rows = self.conn.execute(
            "SELECT name FROM items WHERE id = ANY(%s) ORDER BY name",
            (ids[:2],)).fetchall()
        self.assertEqual([r["name"] for r in rows], ["a", "b"])
        rows = self.conn.execute(
            "SELECT name FROM items WHERE qty > %s AND name = ANY(%s)",
            (1, ("b", "c", "zz"))).fetchall()
        self.assertEqual(sorted(r["name"] for r in rows), ["b", "c"])
        self.assertEqual(self.conn.execute(
            "SELECT name FROM items WHERE name = ANY(%(n)s)",
            {"n": []}).fetchall(), [])
        self.assertEqual(self.conn.execute(
            "DELETE FROM items WHERE id = ANY(%s)", (ids,)).rowcount, 3)
        with self.assertRaises(sa.InterfaceError):
            self.conn.execute(
                "SELECT 1 FROM items WHERE blob = ANY(%s)", ([b"x"],))
        with self.assertRaises(sa.InterfaceError):
            self.conn.execute(
                "SELECT 1 FROM items WHERE name = ANY(%s)", ("abc",))


class ErrorMappingTests(AdapterTestCase):
    def test_constraint_classes(self):
        self.insert("a")
        cases = [
            (sa.UniqueViolation,
             "INSERT INTO items (name) VALUES (%s)", ("a",)),
            (sa.NotNullViolation,
             "INSERT INTO items (name) VALUES (%s)", (None,)),
            (sa.CheckViolation,
             "INSERT INTO items (name, qty) VALUES (%s, -1)", ("b",)),
            (sa.ForeignKeyViolation,
             "INSERT INTO children (id, item_id) VALUES (1, %s)", (999,)),
        ]
        for cls, sql, params in cases:
            with self.subTest(cls=cls.__name__):
                with self.assertRaises(cls) as ctx:
                    self.conn.execute(sql, params)
                self.assertIsInstance(ctx.exception, sa.IntegrityError)
                self.assertFalse(ctx.exception.retryable)
                self.assertIsInstance(ctx.exception.__cause__, sqlite3.Error)

    def test_trigger_abort(self):
        self.conn.execute(
            "CREATE TRIGGER no_rename BEFORE UPDATE OF name ON items "
            "BEGIN SELECT RAISE(ABORT, 'name is write-once'); END")
        self.insert("a")
        with self.assertRaises(sa.TriggerAbort) as ctx:
            self.conn.execute("UPDATE items SET name='b'")
        self.assertIn("write-once", str(ctx.exception))

    def test_syntax_and_missing_table_are_programming_errors(self):
        for sql in ("SELEKT 1", "SELECT * FROM missing_table"):
            with self.assertRaises(sa.ProgrammingError):
                self.conn.execute(sql)

    def test_retryable_versus_never_retried(self):
        def fake(code, name, cls=sqlite3.OperationalError):
            exc = cls("x")
            exc.sqlite_errorcode = code
            exc.sqlite_errorname = name
            return exc
        for code, name in ((5, "SQLITE_BUSY"), (6, "SQLITE_LOCKED"),
                           (517, "SQLITE_BUSY_SNAPSHOT"),
                           (262, "SQLITE_LOCKED_SHAREDCACHE")):
            mapped = sa.map_error(fake(code, name))
            self.assertIsInstance(mapped, sa.BusyError, name)
            self.assertTrue(mapped.retryable)
            self.assertEqual(mapped.sqlite_errorcode, code)
        for code, name in ((13, "SQLITE_FULL"), (10, "SQLITE_IOERR"),
                           (778, "SQLITE_IOERR_WRITE"), (11, "SQLITE_CORRUPT"),
                           (8, "SQLITE_READONLY"), (26, "SQLITE_NOTADB"),
                           (14, "SQLITE_CANTOPEN")):
            mapped = sa.map_error(fake(code, name))
            self.assertIsInstance(mapped, sa.FatalError, name)
            self.assertNotIsInstance(mapped, sa.RetryableError)
            self.assertFalse(mapped.retryable)
        self.assertTrue(sa.PoolTimeout("x").retryable)

    def test_real_busy_is_retryable(self):
        # A foreign writer (no shared in-process lock) holds the database.
        foreign = sqlite3.connect(self.path, isolation_level=None)
        self.addCleanup(foreign.close)
        foreign.execute("BEGIN IMMEDIATE")
        quick = sa.connect(self.path, busy_timeout_ms=50)
        self.addCleanup(quick.close)
        with self.assertRaises(sa.BusyError) as ctx:
            with quick.transaction():
                pass
        self.assertTrue(ctx.exception.retryable)
        self.assertFalse(quick._has_write_lock)
        foreign.execute("ROLLBACK")
        with quick.transaction():
            quick.execute("UPDATE counter SET n=n+1")


class ReadOnlyTests(AdapterTestCase):
    def test_read_only_connection_reads_and_rejects_writes(self):
        self.insert("a")
        self.conn.commit()
        ro = sa.connect(self.path, read_only=True)
        self.addCleanup(ro.close)
        self.assertEqual(self.names(ro), ["a"])
        for sql in ("INSERT INTO items (name) VALUES ('b')",
                    "UPDATE items SET qty=1", "DELETE FROM items",
                    "CREATE TABLE z (a)"):
            with self.subTest(sql=sql):
                with self.assertRaises(sa.ReadOnlyError) as ctx:
                    ro.execute(sql)
                self.assertFalse(ctx.exception.retryable)
        ro.rollback()
        with self.assertRaises(sa.ReadOnlyError):
            with ro.transaction():
                ro.execute("DELETE FROM items")
        with self.assertRaises(sa.ReadOnlyError):
            ro.executescript("CREATE TABLE z (a);")
        self.assertFalse(ro._has_write_lock)
        self.assertEqual(self.names(), ["a"])

    def test_reader_is_not_blocked_by_a_writer(self):
        ro = sa.connect(self.path, read_only=True)
        self.addCleanup(ro.close)
        with self.conn.transaction():
            self.insert("pending")
            self.assertEqual(self.names(ro), [])
            ro.rollback()
        self.assertEqual(self.names(ro), ["pending"])

    def test_read_only_open_of_missing_file_fails(self):
        with self.assertRaises(sa.FatalError):
            sa.connect(os.path.join(self.dir, "absent.db"), read_only=True)


class PoolTests(AdapterTestCase):
    def pool(self, **kw):
        pool = sa.SqlitePool(self.path, **kw)
        self.addCleanup(pool.close)
        return pool

    def test_connection_commits_on_success_and_rolls_back_on_error(self):
        pool = self.pool()
        with pool.connection() as conn:
            self.insert("a", conn=conn)
        with self.assertRaises(RuntimeError):
            with pool.connection() as conn:
                self.insert("b", conn=conn)
                raise RuntimeError("boom")
        with pool.connection() as conn:
            self.assertEqual(self.names(conn), ["a"])
            self.assertFalse(conn._has_write_lock)

    def test_getconn_putconn_reuse_and_reset(self):
        pool = self.pool(max_size=2)
        first = pool.getconn()
        first.autocommit = True
        pool.putconn(first)
        again = pool.getconn()
        self.assertIs(again, first)
        self.assertFalse(again.autocommit)
        self.insert("lost", conn=again)        # never committed
        pool.putconn(again)
        with pool.connection() as conn:
            self.assertEqual(self.names(conn), [])

    def test_size_limit_and_timeout(self):
        with self.assertRaises(ValueError):
            sa.SqlitePool(self.path, max_size=9)
        pool = self.pool(max_size=2, timeout=0.1)
        a, b = pool.getconn(), pool.getconn()
        with self.assertRaises(sa.PoolTimeout) as ctx:
            pool.getconn()
        self.assertTrue(ctx.exception.retryable)
        pool.putconn(a)
        self.assertIs(pool.getconn(), a)
        pool.putconn(a)
        pool.putconn(b)

    def test_closed_connection_is_replaced(self):
        pool = self.pool(max_size=1)
        conn = pool.getconn()
        conn.close()
        pool.putconn(conn)
        with pool.connection() as fresh:
            self.assertIsNot(fresh, conn)
            self.assertEqual(fresh.execute("SELECT 1 AS a").fetchone()["a"], 1)

    def test_close(self):
        pool = self.pool()
        held = pool.getconn()
        pool.close()
        with self.assertRaises(sa.PoolClosed):
            pool.getconn()
        pool.putconn(held)
        self.assertTrue(held.closed)

    def test_pinned_connection_with_autocommit(self):
        # The archiver-lock pattern: getconn, autocommit, keep, transaction().
        pool = self.pool()
        pinned = pool.getconn()
        pinned.autocommit = True
        with pinned.transaction():
            self.insert("a", conn=pinned)
        with pool.connection() as conn:
            self.assertEqual(self.names(conn), ["a"])
        pinned.autocommit = False
        pool.putconn(pinned)

    def test_eight_thread_write_stress(self):
        pool = self.pool(max_size=8)
        threads, per_thread = 8, 60
        errors = []
        start = threading.Barrier(threads)

        def work(index):
            try:
                start.wait()
                for i in range(per_thread):
                    if i % 3 == 0:
                        # read-then-write on a bare pool connection
                        with pool.connection() as conn:
                            conn.execute("SELECT n FROM counter").fetchone()
                            conn.execute(
                                "UPDATE counter SET n=n+1 WHERE id=%s", (1,))
                    elif i % 3 == 1:
                        with pool.connection() as conn:
                            with conn.transaction():
                                n = conn.execute(
                                    "SELECT n FROM counter WHERE id=%s "
                                    "FOR UPDATE", (1,)).fetchone()["n"]
                                conn.execute(
                                    "UPDATE counter SET n=%s WHERE id=%s",
                                    (n + 1, 1))
                    else:
                        with pool.connection() as conn:
                            conn.execute(
                                "INSERT INTO items (name) VALUES (%s)",
                                (f"t{index}-{i}",))
                            conn.execute(
                                "UPDATE counter SET n=n+1 WHERE id=%s", (1,))
                    with pool.connection() as conn:     # concurrent reader
                        conn.execute("SELECT COUNT(*) AS c FROM items"
                                     ).fetchone()
            except BaseException as exc:    # noqa: BLE001 - reported below
                errors.append(repr(exc))

        workers = [threading.Thread(target=work, args=(i,))
                   for i in range(threads)]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(180)
        self.assertFalse(any(w.is_alive() for w in workers))
        self.assertEqual(errors, [])
        with pool.connection() as conn:
            self.assertEqual(
                conn.execute("SELECT n FROM counter").fetchone()["n"],
                threads * per_thread)
            self.assertEqual(
                conn.execute("SELECT COUNT(*) AS c FROM items").fetchone()["c"],
                threads * (per_thread // 3))


if __name__ == "__main__":
    unittest.main()
