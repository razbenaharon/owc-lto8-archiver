"""Golden pairs, residue refusals and literal/comment safety for the
PostgreSQL -> SQLite lexical rewriter.  Pure offline: no database server."""
import sqlite3
import unittest

from src.sqlite_dialect import (
    NOW_SQL,
    SqliteDialectError,
    clear_rewrite_cache,
    rewrite,
    rewrite_cache_info,
    tokenize,
)


def rw(sql):
    return rewrite(sql).sql


class GoldenPairTests(unittest.TestCase):
    """One or more exact input -> output pairs for every rule."""

    PAIRS = [
        # placeholders
        ("SELECT * FROM t WHERE a=%s AND b = %s",
         "SELECT * FROM t WHERE a=? AND b = ?"),
        ("SELECT * FROM t WHERE a=%(x)s AND b=%(y_2)s AND c=%(x)s",
         "SELECT * FROM t WHERE a=:x AND b=:y_2 AND c=:x"),
        ("SELECT 10 %% 3", "SELECT 10 % 3"),
        ("SELECT a % b FROM t", "SELECT a % b FROM t"),
        # FOR UPDATE
        ("SELECT * FROM t WHERE id=%s FOR UPDATE",
         "SELECT * FROM t WHERE id=?"),
        ("SELECT * FROM t s WHERE id=%s\n   FOR UPDATE OF s",
         "SELECT * FROM t s WHERE id=?"),
        ("SELECT 1 FROM a, b FOR UPDATE OF a, b SKIP LOCKED LIMIT 1",
         "SELECT 1 FROM a, b LIMIT 1"),
        ("SELECT 1 FROM d for update of d skip locked",
         "SELECT 1 FROM d"),
        # IS [NOT] DISTINCT FROM
        ("SELECT 1 WHERE a IS DISTINCT FROM b", "SELECT 1 WHERE a IS NOT b"),
        ("SELECT 1 WHERE a IS NOT DISTINCT FROM %s",
         "SELECT 1 WHERE a IS ?"),
        ("SELECT 1 WHERE a IS\n DISTINCT\n FROM 'x' AND b IS NOT NULL",
         "SELECT 1 WHERE a IS\n NOT 'x' AND b IS NOT NULL"),
        # ILIKE
        ("SELECT 1 WHERE n ILIKE %s ESCAPE '\\'",
         "SELECT 1 WHERE n LIKE ? ESCAPE '\\'"),
        ("SELECT 1 WHERE n NOT ilike %s", "SELECT 1 WHERE n NOT LIKE ?"),
        # now()
        ("UPDATE t SET u=now() WHERE id=%s",
         f"UPDATE t SET u={NOW_SQL} WHERE id=?"),
        ("SELECT COALESCE(x, %s, NOW ( ))", f"SELECT COALESCE(x, ?, {NOW_SQL})"),
        # dropped casts
        ("UPDATE t SET j=%s::jsonb, k=%s::json", "UPDATE t SET j=?, k=?"),
        ("SELECT NULL::text, NULL::timestamptz, a.b ::TEXT",
         "SELECT NULL, NULL, a.b"),
        # integer casts on simple operands
        ("SELECT %s::bigint AS session_id",
         "SELECT CAST(? AS INTEGER) AS session_id"),
        ("SELECT 1 WHERE x=%(session)s::BIGINT",
         "SELECT 1 WHERE x=CAST(:session AS INTEGER)"),
        ("SELECT NULL::integer, a.b::int, \"c\"::smallint, 5::int8, '7'::int4",
         "SELECT CAST(NULL AS INTEGER), CAST(a.b AS INTEGER), "
         "CAST(\"c\" AS INTEGER), CAST(5 AS INTEGER), CAST('7' AS INTEGER)"),
        ("SELECT COALESCE((a.e->>'n')::bigint,0)>0",
         "SELECT COALESCE(CAST((a.e->>'n') AS INTEGER),0)>0"),
        ("SELECT 1 WHERE (%s::bigint IS NULL OR s = %s)",
         "SELECT 1 WHERE (CAST(? AS INTEGER) IS NULL OR s = ?)"),
        ("SELECT x::text::bigint, y::bigint::text",
         "SELECT CAST(x AS INTEGER), CAST(y AS INTEGER)"),
        ("SELECT ((a+b)::bigint + 1)::integer",
         "SELECT CAST((CAST((a+b) AS INTEGER) + 1) AS INTEGER)"),
        # = ANY(%s)
        ("SELECT 1 FROM t WHERE p = ANY(%s)",
         "SELECT 1 FROM t WHERE p IN (SELECT value FROM json_each(?))"),
        ("DELETE FROM t WHERE s=%s AND p=ANY(%s)",
         "DELETE FROM t WHERE s=? AND p IN (SELECT value FROM json_each(?))"),
        ("SELECT 1 WHERE p = any ( %(names)s )",
         "SELECT 1 WHERE p IN (SELECT value FROM json_each ( :names ))"),
        # GREATEST / LEAST
        ("UPDATE p SET n=GREATEST(n, %s)", "UPDATE p SET n=max(n, ?)"),
        ("SELECT least(a, b, f(c, d))", "SELECT min(a, b, f(c, d))"),
        # native constructs pass through untouched
        ("SELECT a->>'k', COUNT(*) FILTER (WHERE x) FROM t WHERE b IS TRUE",
         "SELECT a->>'k', COUNT(*) FILTER (WHERE x) FROM t WHERE b IS TRUE"),
        ("INSERT INTO t (a) VALUES (%s) ON CONFLICT (a) DO UPDATE "
         "SET a=EXCLUDED.a RETURNING id",
         "INSERT INTO t (a) VALUES (?) ON CONFLICT (a) DO UPDATE "
         "SET a=EXCLUDED.a RETURNING id"),
        ("SELECT CAST(x AS bigint), CAST(y AS TEXT)",
         "SELECT CAST(x AS bigint), CAST(y AS TEXT)"),
    ]

    def test_golden_pairs(self):
        for source, expected in self.PAIRS:
            with self.subTest(source=source):
                self.assertEqual(rw(source), expected)

    def test_every_golden_output_is_parseable_by_sqlite(self):
        """The rewritten text must at least be valid SQLite syntax."""
        conn = sqlite3.connect(":memory:")
        conn.executescript(
            "CREATE TABLE t (id, a UNIQUE, b, c, e, j, k, n, p, s, u, x, y);"
            "CREATE TABLE p (n); CREATE TABLE d (x);"
            "CREATE TABLE a (b, e); CREATE TABLE b (x);")
        skip_tables = ("f(c, d)", "(a+b)", "SELECT a % b")
        for source, expected in self.PAIRS:
            if any(marker in source for marker in skip_tables):
                continue
            with self.subTest(sql=expected):
                try:
                    conn.execute("EXPLAIN " + expected, self._dummy(source))
                except sqlite3.OperationalError as exc:
                    # Unknown columns are fine; syntax errors are not.
                    self.assertNotIn("syntax error", str(exc))
        conn.close()

    @staticmethod
    def _dummy(source):
        result = rewrite(source)
        if result.param_style == "named":
            names = {tok.value for tok in tokenize(source)
                     if tok.kind == "nparam"}
            return {name: "[]" if name in result.json_names else None
                    for name in names}
        return ["[]" if i in result.json_positions else None
                for i in range(result.param_count)]


class StatementMetadataTests(unittest.TestCase):
    def test_param_styles_and_counts(self):
        self.assertEqual(rewrite("SELECT 1").param_style, "none")
        result = rewrite("SELECT %s, %s, %s")
        self.assertEqual((result.param_style, result.param_count),
                         ("positional", 3))
        result = rewrite("SELECT %(a)s, %(b)s")
        self.assertEqual((result.param_style, result.param_count),
                         ("named", 0))

    def test_json_parameters_are_reported(self):
        result = rewrite(
            "SELECT 1 FROM t WHERE a=%s AND b = ANY(%s) AND c=%s "
            "AND d = ANY(%s)")
        self.assertEqual(result.json_positions, frozenset({1, 3}))
        self.assertTrue(result.needs_json)
        result = rewrite("SELECT 1 WHERE a=%(a)s AND b = ANY(%(many)s)")
        self.assertEqual(result.json_names, frozenset({"many"}))
        self.assertFalse(rewrite("SELECT %s").needs_json)

    def test_classification(self):
        cases = {
            "SELECT 1": ("SELECT", False, False),
            "  -- c\n select 1": ("SELECT", False, False),
            "(SELECT 1) UNION ALL (SELECT 2)": ("SELECT", False, False),
            "INSERT INTO t VALUES (%s) RETURNING id": ("INSERT", True, True),
            "UPDATE t SET a=1": ("UPDATE", True, False),
            "DELETE FROM t": ("DELETE", True, False),
            "CREATE TABLE x (a)": ("CREATE", True, False),
            "WITH q AS (SELECT 1) SELECT * FROM q": ("WITH", False, False),
            "WITH q AS (SELECT 1) INSERT INTO t SELECT * FROM q":
                ("WITH", True, False),
            "PRAGMA journal_mode": ("PRAGMA", False, False),
            "VACUUM": ("VACUUM", True, False),
        }
        for sql, (keyword, is_write, returning) in cases.items():
            with self.subTest(sql=sql):
                result = rewrite(sql)
                self.assertEqual(
                    (result.keyword, result.is_write, result.has_returning),
                    (keyword, is_write, returning))

    def test_for_update_reports_write_intent(self):
        result = rewrite("SELECT * FROM t WHERE id=%s FOR UPDATE")
        self.assertFalse(result.is_write)
        self.assertTrue(result.locks_rows)
        self.assertTrue(result.write_intent)
        self.assertFalse(rewrite("SELECT 1").write_intent)

    def test_returning_inside_a_literal_is_not_a_returning_clause(self):
        self.assertFalse(
            rewrite("UPDATE t SET a='RETURNING id'").has_returning)


class LiteralAndCommentSafetyTests(unittest.TestCase):
    def assert_unchanged(self, sql):
        self.assertEqual(rw(sql), sql)

    def test_string_literals_are_never_touched(self):
        self.assert_unchanged(
            "SELECT 'a ILIKE b', 'x::text', 'now()', ' = ANY(%s) ', "
            "'FOR UPDATE', 'IS DISTINCT FROM', 'GREATEST(1)', '%s %(n)s %%'")

    def test_placeholders_in_literals_are_not_counted(self):
        result = rewrite("SELECT '%s', %s")
        self.assertEqual(result.sql, "SELECT '%s', ?")
        self.assertEqual(result.param_count, 1)

    def test_doubled_quotes_do_not_end_a_literal(self):
        self.assert_unchanged("SELECT 'it''s now() ::text', 1")
        self.assert_unchanged('SELECT "we""ird now() ::x" FROM t')

    def test_backslash_is_an_ordinary_character(self):
        self.assert_unchanged("SELECT 1 WHERE a LIKE 'x\\' AND b = 'now()'")

    def test_quoted_identifiers_are_never_touched(self):
        self.assert_unchanged(
            'SELECT "now()", "ILIKE", "a::text", "pg_class", "xmax" FROM t')

    def test_comments_are_never_touched(self):
        self.assert_unchanged(
            "SELECT 1 -- now() ::text ILIKE %s pg_locks LATERAL\n FROM t")
        self.assert_unchanged(
            "SELECT 1 /* = ANY(%s) /* nested :: */ FOR UPDATE */ FROM t")

    def test_comment_between_rule_tokens(self):
        result = rw("SELECT a FROM t WHERE a IS /* c */ DISTINCT FROM b")
        self.assertEqual(result, "SELECT a FROM t WHERE a IS /* c */ NOT b")

    def test_literal_adjacent_to_rules(self):
        self.assertEqual(
            rw("SELECT 1 WHERE a ILIKE 'ILIKE' AND b = ANY(%s) "
               "AND c='= ANY(%s)'"),
            "SELECT 1 WHERE a LIKE 'ILIKE' AND b IN (SELECT value FROM "
            "json_each(?)) AND c='= ANY(%s)'")

    def test_tokenizer_is_lossless(self):
        sql = ("SELECT a::text, 'x''y', \"q\"\"r\" /* c */ -- d\n"
               " FROM t WHERE a=%s AND b=%(n)s AND c ->> 'k' <> 1.5e3 %% 2")
        self.assertEqual("".join(tok.text for tok in tokenize(sql)), sql)

    def test_unterminated_tokens_are_refused(self):
        for sql in ("SELECT 'abc", 'SELECT "abc', "SELECT 1 /* never",
                    "SELECT $$ never"):
            with self.subTest(sql=sql):
                with self.assertRaises(SqliteDialectError):
                    rewrite(sql)


class ResidueRefusalTests(unittest.TestCase):
    REFUSED = [
        # every token the design lists
        "SELECT %s::date",
        "SELECT x::regclass",
        "SELECT 1 FROM t WHERE a = ANY(ARRAY(SELECT 1))",
        "SELECT 1 FROM t WHERE a <> ANY(%s)",
        "SELECT 1 FROM t WHERE a = ANY(%s::bigint[])",
        "SELECT * FROM a LEFT JOIN LATERAL (SELECT 1) b ON TRUE",
        "SELECT 1 FROM pg_locks",
        "SELECT pg_try_advisory_lock(%s) AS locked",
        "SELECT 1 FROM information_schema.tables WHERE table_name=%s",
        "UPDATE t SET l=now() + make_interval(secs => %s)",
        "SELECT %s::date + INTERVAL '1 day'",
        "UPDATE t SET l=now()+(%s * interval '1 second')",
        "INSERT INTO t SELECT * FROM s RETURNING (xmax = 0) AS inserted",
        "SELECT set_config(%s, %s, true)",
        "SELECT current_setting('lto.x', true)",
        "SELECT to_regclass(%s)",
        "DO $$ BEGIN PERFORM 1; END $$",
        "SELECT $tag$ body $tag$",
        # additional fail-closed cases
        "SELECT f(x)::bigint",
        "SELECT CASE WHEN a THEN 1 END::bigint",
        "SELECT x::bigint::integer",
        "SELECT x::numeric(10,2)",
        "SELECT x::varchar(10)",
        "SELECT x::timestamp with time zone",
        "SELECT CAST(x AS timestamptz)",
        "SELECT CAST(x AS jsonb)",
        "SELECT GREATEST(a)",
        "SELECT a[1] FROM t",
        "SELECT ARRAY[1,2]",
        "SELECT a ? 'k' FROM t",
        "SELECT a FROM t WHERE b = :name",
        "SELECT a FROM t WHERE b = $1",
        "SELECT a FROM t WHERE b ~ 'x'",
        "SELECT a FROM t WHERE b @> c",
        "SELECT a #>> '{x}' FROM t",
        "SELECT 1 WHERE a = ALL(%s)",
        "SELECT 1 WHERE a = SOME(%s)",
        "SELECT 1 FROM t FOR SHARE",
        "SELECT 1 FROM t FOR UPDATE NOWAIT",
        "SELECT 1 FROM t FOR NO KEY UPDATE",
        "SELECT CURRENT_TIMESTAMP",
        "SELECT 1 WHERE a SIMILAR TO 'x'",
        "SELECT E'a\\nb'",
        "SELECT %b",
        "SELECT %(bad name)s",
        "SELECT %(n)b",
        "SELECT %s, %(n)s",
    ]

    def test_refusals(self):
        for sql in self.REFUSED:
            with self.subTest(sql=sql):
                with self.assertRaises(SqliteDialectError):
                    rewrite(sql)

    def test_error_names_the_construct_and_statement(self):
        with self.assertRaises(SqliteDialectError) as ctx:
            rewrite("SELECT 1 FROM pg_locks WHERE pid=%s")
        self.assertIn("pg_locks", str(ctx.exception))
        self.assertIn("SELECT 1 FROM pg_locks", str(ctx.exception))
        self.assertEqual(ctx.exception.sql,
                         "SELECT 1 FROM pg_locks WHERE pid=%s")

    def test_non_string_sql_is_refused(self):
        with self.assertRaises(SqliteDialectError):
            rewrite(b"SELECT 1")

    def test_union_all_subquery_is_not_mistaken_for_all(self):
        self.assertEqual(rw("SELECT 1 UNION ALL (SELECT 2)"),
                         "SELECT 1 UNION ALL (SELECT 2)")

    def test_trigger_for_each_row_is_not_a_locking_clause(self):
        sql = ("CREATE TRIGGER g BEFORE UPDATE OF a ON t FOR EACH ROW "
               "BEGIN SELECT RAISE(ABORT, 'no'); END")
        self.assertEqual(rw(sql), sql)


class CacheTests(unittest.TestCase):
    def test_results_are_cached_and_errors_are_not(self):
        clear_rewrite_cache()
        first = rewrite("SELECT %s -- cache probe")
        hits = rewrite_cache_info().hits
        second = rewrite("SELECT %s -- cache probe")
        self.assertIs(first, second)
        self.assertEqual(rewrite_cache_info().hits, hits + 1)
        size = rewrite_cache_info().currsize
        for _ in range(2):
            with self.assertRaises(SqliteDialectError):
                rewrite("SELECT 1 FROM pg_class -- cache probe")
        self.assertEqual(rewrite_cache_info().currsize, size)

    def test_result_is_immutable(self):
        result = rewrite("SELECT %s")
        with self.assertRaises(Exception):
            result.sql = "tampered"


class SemanticsAgainstSqliteTests(unittest.TestCase):
    """The rewritten SQL must *mean* the same thing where the design says so."""

    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.addCleanup(self.conn.close)

    def one(self, sql, params=()):
        return self.conn.execute(rw(sql), params).fetchone()[0]

    def test_is_distinct_from_is_null_safe(self):
        self.assertEqual(self.one("SELECT NULL IS DISTINCT FROM NULL"), 0)
        self.assertEqual(self.one("SELECT 1 IS DISTINCT FROM NULL"), 1)
        self.assertEqual(self.one("SELECT 1 IS NOT DISTINCT FROM 1"), 1)
        self.assertEqual(self.one("SELECT NULL IS NOT DISTINCT FROM 1"), 0)

    def test_any_matches_list_membership(self):
        self.assertEqual(self.one("SELECT 2 = ANY(%s)", ("[1, 2, 3]",)), 1)
        self.assertEqual(self.one("SELECT 5 = ANY(%s)", ("[1, 2, 3]",)), 0)
        self.assertEqual(self.one("SELECT 5 = ANY(%s)", ("[]",)), 0)
        self.assertEqual(self.one("SELECT 'b' = ANY(%s)", ('["a", "b"]',)), 1)

    def test_now_is_canonical_fixed_width_utc_text(self):
        value = self.one("SELECT now()")
        self.assertRegex(
            value, r"^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d{6}\+00:00$")

    def test_integer_cast_and_minmax(self):
        self.assertEqual(self.one("SELECT %s::bigint + 1", ("41",)), 42)
        self.assertEqual(self.one("SELECT GREATEST(3, %s)", (7,)), 7)
        self.assertEqual(self.one("SELECT LEAST(3, %s)", (7,)), 3)

    def test_ilike_is_case_insensitive(self):
        self.assertEqual(
            self.one("SELECT 'Report_A' ILIKE %s ESCAPE '\\'",
                     ("report\\_a",)), 1)


if __name__ == "__main__":
    unittest.main()
