"""Lexical PostgreSQL -> SQLite SQL rewriter (closed rule list, fails closed).

The catalog layer is written in PostgreSQL SQL for psycopg 3.  The SQLite
backend runs the *same* statements through :func:`rewrite`, which applies a
deliberately small, closed list of purely lexical rules:

====================================  =====================================
PostgreSQL                            SQLite
====================================  =====================================
``%s`` / ``%(name)s`` / ``%%``        ``?`` / ``:name`` / ``%``
``FOR UPDATE [OF t,..] [SKIP LOCKED]``  removed (``locks_rows`` is reported;
                                      the adapter takes the write lock)
``a IS DISTINCT FROM b``              ``a IS NOT b``
``a IS NOT DISTINCT FROM b``          ``a IS b``
``ILIKE``                             ``LIKE``
``now()``                             canonical UTC text via ``strftime``
``x::jsonb|json|text|timestamptz``    cast dropped
``x::bigint|integer|int|smallint``    ``CAST(x AS INTEGER)`` (simple operand)
``= ANY(%s)``                         ``IN (SELECT value FROM json_each(?))``
``GREATEST(a,b..)`` / ``LEAST(..)``   ``max(a,b..)`` / ``min(..)``
====================================  =====================================

Nothing else is translated.  After rewriting, a residue scanner raises
:class:`SqliteDialectError` on any PostgreSQL-only token that is still present
(``::``, ``ANY(``, ``LATERAL``, ``pg_*``, ``information_schema``,
``make_interval``, ``INTERVAL``, ``xmax``, ``set_config``, ``to_reg*``,
``DO $$`` ...), so a statement can never run half-translated.

The tokenizer is a real one: string literals, quoted identifiers and comments
are opaque and are never modified.  (psycopg itself *does* look inside string
literals for ``%`` placeholders; working code therefore never has a bare ``%``
in a literal of a parameterised statement, and this module leaves literals
alone.)

Known semantic caveats of the rules (accepted by the design):

* ``ILIKE`` -> ``LIKE`` is case-insensitive for ASCII only.
* ``now()`` is statement time, not transaction start time.
* ``max()/min()`` return NULL if any argument is NULL; ``GREATEST/LEAST``
  ignore NULLs.
* A stripped ``FOR UPDATE`` locks the whole database for writing, not a row.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from functools import lru_cache

__all__ = [
    "SqliteDialectError",
    "RewrittenStatement",
    "NOW_SQL",
    "rewrite",
    "tokenize",
    "rewrite_cache_info",
    "clear_rewrite_cache",
]

#: Canonical UTC timestamp text, identical in shape to what the adapter
#: stores for an aware ``datetime``: ``YYYY-MM-DD HH:MM:SS.ffffff+00:00``.
#: Fixed width, so plain text comparison orders chronologically.
NOW_SQL = "(strftime('%Y-%m-%d %H:%M:%f','now')||'000+00:00')"


class SqliteDialectError(Exception):
    """A statement cannot be translated to SQLite safely.

    Never retryable: the statement itself has to change (a "portable edit")
    or be routed through a backend hook.
    """

    def __init__(self, message, *, construct=None, sql=None):
        self.construct = construct
        self.sql = sql
        detail = message
        if sql is not None:
            snippet = " ".join(sql.split())
            if len(snippet) > 240:
                snippet = snippet[:237] + "..."
            detail = f"{message} | statement: {snippet}"
        super().__init__(f"[SQLITE-DIALECT] {detail}")


@dataclass(frozen=True)
class RewrittenStatement:
    """Result of :func:`rewrite` for one statement."""

    sql: str
    #: ``"none"``, ``"positional"`` (``?``) or ``"named"`` (``:name``).
    param_style: str
    #: Number of positional placeholders (0 for named statements).
    param_count: int
    #: 0-based positional parameter indexes that must be JSON-encoded lists.
    json_positions: frozenset
    #: Named parameters that must be JSON-encoded lists.
    json_names: frozenset
    #: First keyword, upper-cased (``SELECT``, ``INSERT``, ``WITH`` ...).
    keyword: str
    #: The statement itself modifies the database.
    is_write: bool
    #: A ``FOR UPDATE`` clause was stripped: needs the write lock.
    locks_rows: bool
    #: The statement has a ``RETURNING`` clause.
    has_returning: bool

    @property
    def write_intent(self):
        """True when the statement must run under the write lock."""
        return self.is_write or self.locks_rows

    @property
    def needs_json(self):
        return bool(self.json_positions or self.json_names)


# ---------------------------------------------------------------------------
# Tokenizer
# ---------------------------------------------------------------------------

WS = "ws"
COMMENT = "comment"
STR = "str"
QIDENT = "qident"
DOLLAR = "dollar"
NUM = "num"
WORD = "word"
PARAM = "param"
NPARAM = "nparam"
PCT = "pct"
OP = "op"
RAW = "raw"      # text emitted by a rewrite rule; never re-examined
DEL = "del"      # removed by a rewrite rule

_INSIGNIFICANT = frozenset((WS, COMMENT, DEL))


class Token:
    __slots__ = ("kind", "text", "value")

    def __init__(self, kind, text, value=None):
        self.kind = kind
        self.text = text
        self.value = value   # PARAM: 0-based index; NPARAM: name

    @property
    def upper(self):
        return self.text.upper() if self.kind == WORD else None

    def __repr__(self):  # pragma: no cover - debugging aid
        return f"Token({self.kind}, {self.text!r})"


_WORD_RE = re.compile(r"[^\W\d][\w$]*")
_NUM_RE = re.compile(r"\d+\.?\d*(?:[eE][+-]?\d+)?|\.\d+(?:[eE][+-]?\d+)?")
_NPARAM_RE = re.compile(r"%\(([^)]*)\)(.?)")
_NAME_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
_DOLLAR_RE = re.compile(r"\$(?:[^\W\d]\w*)?\$")
_OPERATORS = ("->>", "::", "->", "<>", "<=", ">=", "!=", "||", "<<", ">>",
              "==")


def _scan_quoted(sql, start, quote):
    """Index just past a quote-delimited token (doubled quote = escape)."""
    i = start + 1
    n = len(sql)
    while i < n:
        if sql[i] == quote:
            if i + 1 < n and sql[i + 1] == quote:
                i += 2
                continue
            return i + 1
        i += 1
    raise SqliteDialectError(
        f"unterminated {quote} quoted token", construct="quote", sql=sql)


def _scan_block_comment(sql, start):
    depth = 0
    i = start
    n = len(sql)
    while i < n:
        if sql.startswith("/*", i):
            depth += 1
            i += 2
        elif sql.startswith("*/", i):
            depth -= 1
            i += 2
            if depth == 0:
                return i
        else:
            i += 1
    raise SqliteDialectError(
        "unterminated block comment", construct="comment", sql=sql)


def tokenize(sql):
    """Split PostgreSQL/psycopg SQL into tokens.  Lossless: joining the token
    texts gives the input back."""
    tokens = []
    i = 0
    n = len(sql)
    params = 0
    while i < n:
        c = sql[i]
        if c.isspace():
            j = i + 1
            while j < n and sql[j].isspace():
                j += 1
            tokens.append(Token(WS, sql[i:j]))
        elif c == "-" and sql.startswith("--", i):
            j = sql.find("\n", i)
            j = n if j < 0 else j
            tokens.append(Token(COMMENT, sql[i:j]))
        elif c == "/" and sql.startswith("/*", i):
            j = _scan_block_comment(sql, i)
            tokens.append(Token(COMMENT, sql[i:j]))
        elif c == "'":
            j = _scan_quoted(sql, i, "'")
            tokens.append(Token(STR, sql[i:j]))
        elif c == '"':
            j = _scan_quoted(sql, i, '"')
            tokens.append(Token(QIDENT, sql[i:j]))
        elif c == "$":
            m = _DOLLAR_RE.match(sql, i)
            if m:
                end = sql.find(m.group(), m.end())
                if end < 0:
                    raise SqliteDialectError(
                        "unterminated dollar-quoted string",
                        construct="$$", sql=sql)
                j = end + len(m.group())
                tokens.append(Token(DOLLAR, sql[i:j]))
            else:
                j = i + 1
                tokens.append(Token(OP, c))
        elif c == "%":
            nxt = sql[i + 1] if i + 1 < n else ""
            after = sql[i + 2] if i + 2 < n else ""
            if nxt == "%":
                j = i + 2
                tokens.append(Token(PCT, "%%"))
            elif nxt == "(":
                m = _NPARAM_RE.match(sql, i)
                if not m or m.group(2) != "s" or not _NAME_RE.match(
                        m.group(1)):
                    raise SqliteDialectError(
                        "unsupported named placeholder (only %(name)s with "
                        "an identifier name is translated)",
                        construct="%(", sql=sql)
                j = m.end()
                tokens.append(Token(NPARAM, sql[i:j], m.group(1)))
            elif nxt == "s":
                j = i + 2
                tokens.append(Token(PARAM, "%s", params))
                params += 1
            elif nxt in ("b", "t") and not (after.isalnum() or after == "_"):
                raise SqliteDialectError(
                    f"unsupported psycopg placeholder %{nxt}",
                    construct="%" + nxt, sql=sql)
            else:
                j = i + 1
                tokens.append(Token(OP, "%"))
        elif c.isdigit() or (c == "." and i + 1 < n and sql[i + 1].isdigit()):
            m = _NUM_RE.match(sql, i)
            j = m.end()
            tokens.append(Token(NUM, sql[i:j]))
        elif c == "_" or c.isalpha():
            m = _WORD_RE.match(sql, i)
            j = m.end()
            word = sql[i:j]
            if j < n and sql[j] == "'" and word.upper() == "E":
                raise SqliteDialectError(
                    "E'' escape-string literals are PostgreSQL-only",
                    construct="E''", sql=sql)
            tokens.append(Token(WORD, word))
        else:
            for op in _OPERATORS:
                if sql.startswith(op, i):
                    j = i + len(op)
                    tokens.append(Token(OP, op))
                    break
            else:
                j = i + 1
                tokens.append(Token(OP, c))
        i = j
    return tokens


# ---------------------------------------------------------------------------
# Token-list helpers
# ---------------------------------------------------------------------------

def _next(tokens, i):
    """Index of the next significant token after ``i`` (or None)."""
    i += 1
    n = len(tokens)
    while i < n:
        if tokens[i].kind not in _INSIGNIFICANT:
            return i
        i += 1
    return None


def _prev(tokens, i):
    i -= 1
    while i >= 0:
        if tokens[i].kind not in _INSIGNIFICANT:
            return i
        i -= 1
    return None


def _is_word(tokens, i, *words):
    return (i is not None and tokens[i].kind == WORD
            and tokens[i].text.upper() in words)


def _is_op(tokens, i, *ops):
    return i is not None and tokens[i].kind == OP and tokens[i].text in ops


def _delete(tokens, i):
    """Remove token ``i`` together with the whitespace just before it."""
    tokens[i].kind = DEL
    tokens[i].text = ""
    if i > 0 and tokens[i - 1].kind == WS:
        tokens[i - 1].kind = DEL
        tokens[i - 1].text = ""


def _matching_close(tokens, open_i):
    depth = 0
    for j in range(open_i, len(tokens)):
        if _is_op(tokens, j, "("):
            depth += 1
        elif _is_op(tokens, j, ")"):
            depth -= 1
            if depth == 0:
                return j
    return None


def _matching_open(tokens, close_i):
    depth = 0
    for j in range(close_i, -1, -1):
        if _is_op(tokens, j, ")"):
            depth += 1
        elif _is_op(tokens, j, "("):
            depth -= 1
            if depth == 0:
                return j
    return None


# ---------------------------------------------------------------------------
# Rewrite rules (the closed list)
# ---------------------------------------------------------------------------

def _rule_for_update(tokens, sql):
    """Strip ``FOR UPDATE [OF name[, name]] [SKIP LOCKED]``."""
    stripped = False
    i = _next(tokens, -1)
    while i is not None:
        if _is_word(tokens, i, "FOR"):
            j = _next(tokens, i)
            if _is_word(tokens, j, "UPDATE"):
                doomed = [i, j]
                k = _next(tokens, j)
                if _is_word(tokens, k, "OF"):
                    doomed.append(k)
                    k = _next(tokens, k)
                    while True:
                        if k is None or tokens[k].kind not in (WORD, QIDENT):
                            raise SqliteDialectError(
                                "malformed FOR UPDATE OF list",
                                construct="FOR UPDATE", sql=sql)
                        doomed.append(k)
                        k = _next(tokens, k)
                        if _is_op(tokens, k, ","):
                            doomed.append(k)
                            k = _next(tokens, k)
                            continue
                        break
                if _is_word(tokens, k, "SKIP"):
                    k2 = _next(tokens, k)
                    if not _is_word(tokens, k2, "LOCKED"):
                        raise SqliteDialectError(
                            "malformed SKIP LOCKED",
                            construct="SKIP LOCKED", sql=sql)
                    doomed.extend((k, k2))
                    k = _next(tokens, k2)
                for d in doomed:
                    _delete(tokens, d)
                stripped = True
                i = k
                continue
        i = _next(tokens, i)
    return stripped


def _rule_distinct_from(tokens, sql):
    i = _next(tokens, -1)
    while i is not None:
        if _is_word(tokens, i, "IS"):
            j = _next(tokens, i)
            negated = _is_word(tokens, j, "NOT")
            d = _next(tokens, j) if negated else j
            f = _next(tokens, d) if d is not None else None
            if _is_word(tokens, d, "DISTINCT") and _is_word(tokens, f, "FROM"):
                if negated:
                    # IS NOT DISTINCT FROM  ->  IS
                    _delete(tokens, j)
                    _delete(tokens, d)
                else:
                    # IS DISTINCT FROM  ->  IS NOT
                    tokens[d].text = "NOT"
                _delete(tokens, f)
                i = _next(tokens, f)
                continue
        i = _next(tokens, i)


def _rule_ilike(tokens, sql):
    for tok in tokens:
        if tok.kind == WORD and tok.text.upper() == "ILIKE":
            tok.text = "LIKE"


def _rule_now(tokens, sql):
    i = _next(tokens, -1)
    while i is not None:
        if _is_word(tokens, i, "NOW"):
            o = _next(tokens, i)
            c = _next(tokens, o) if o is not None else None
            p = _prev(tokens, i)
            if (_is_op(tokens, o, "(") and _is_op(tokens, c, ")")
                    and not _is_op(tokens, p, ".")):
                tokens[i].kind = RAW
                tokens[i].text = NOW_SQL
                _delete(tokens, o)
                _delete(tokens, c)
                i = _next(tokens, c)
                continue
        i = _next(tokens, i)


_MINMAX = {"GREATEST": "max", "LEAST": "min"}


def _rule_greatest_least(tokens, sql):
    i = _next(tokens, -1)
    while i is not None:
        if tokens[i].kind == WORD and tokens[i].text.upper() in _MINMAX:
            name = tokens[i].text.upper()
            o = _next(tokens, i)
            if not _is_op(tokens, o, "("):
                raise SqliteDialectError(
                    f"{name} without an argument list",
                    construct=name, sql=sql)
            close = _matching_close(tokens, o)
            if close is None:
                raise SqliteDialectError(
                    f"unbalanced parentheses after {name}",
                    construct=name, sql=sql)
            depth = 0
            commas = 0
            for j in range(o, close + 1):
                if _is_op(tokens, j, "("):
                    depth += 1
                elif _is_op(tokens, j, ")"):
                    depth -= 1
                elif depth == 1 and _is_op(tokens, j, ","):
                    commas += 1
            if commas == 0:
                # A one-argument max()/min() is an AGGREGATE in SQLite.
                raise SqliteDialectError(
                    f"{name} with a single argument would become an "
                    "aggregate in SQLite", construct=name, sql=sql)
            tokens[i].text = _MINMAX[name]
        i = _next(tokens, i)


def _rule_any(tokens, sql, json_positions, json_names):
    """``= ANY(<placeholder>)`` -> ``IN (SELECT value FROM json_each(?))``."""
    i = _next(tokens, -1)
    while i is not None:
        if _is_word(tokens, i, "ANY"):
            eq = _prev(tokens, i)
            o = _next(tokens, i)
            p = _next(tokens, o) if o is not None else None
            c = _next(tokens, p) if p is not None else None
            if (_is_op(tokens, eq, "=") and _is_op(tokens, o, "(")
                    and p is not None
                    and tokens[p].kind in (PARAM, NPARAM)
                    and _is_op(tokens, c, ")")):
                tokens[eq].kind = RAW
                tokens[eq].text = "IN"
                # Keep "x IN (" readable when the source had "x=ANY(".
                if eq > 0 and tokens[eq - 1].kind not in (WS, DEL):
                    tokens[eq].text = " IN"
                tokens[i].kind = RAW
                tokens[i].text = "(SELECT value FROM json_each"
                if tokens[i - 1].kind != WS:
                    tokens[i].text = " " + tokens[i].text
                tokens[c].kind = RAW
                tokens[c].text = "))"
                if tokens[p].kind == PARAM:
                    json_positions.add(tokens[p].value)
                else:
                    json_names.add(tokens[p].value)
                i = _next(tokens, c)
                continue
            # Anything else stays and is refused by the residue scanner.
        i = _next(tokens, i)


_DROP_CASTS = frozenset(("JSONB", "JSON", "TEXT", "TIMESTAMPTZ"))
_INT_CASTS = frozenset(("BIGINT", "INTEGER", "INT", "INT8", "INT4",
                        "SMALLINT", "INT2"))
#: Keywords that may directly precede a parenthesised *expression* (as
#: opposed to an identifier, which would make it a function call).
_EXPR_KEYWORDS = frozenset((
    "AND", "OR", "NOT", "WHEN", "THEN", "ELSE", "SELECT", "WHERE", "ON",
    "CASE", "BY", "HAVING", "SET", "RETURNING", "DISTINCT", "LIKE", "IS",
    "BETWEEN", "VALUES", "LIMIT", "OFFSET",
))
_NON_OPERAND_WORDS = frozenset(("END", "AS", "FROM", "WHERE", "AND", "OR"))


def _cast_operand_start(tokens, cast_i, sql):
    """Index of the first token of the simple operand left of ``::``."""
    p = _prev(tokens, cast_i)

    def refuse(why):
        return SqliteDialectError(
            f"integer cast on an operand that is not simple ({why}); use a "
            "portable edit", construct="::", sql=sql)

    if p is None:
        raise refuse("nothing to cast")
    tok = tokens[p]
    if tok.kind in (PARAM, NPARAM, NUM, STR):
        return p
    if tok.kind in (WORD, QIDENT):
        if tok.kind == WORD and tok.text.upper() in _NON_OPERAND_WORDS:
            raise refuse(f"keyword {tok.text}")
        start = p
        while True:   # qualified name: a.b.c
            dot = _prev(tokens, start)
            if not _is_op(tokens, dot, "."):
                break
            q = _prev(tokens, dot)
            if q is None or tokens[q].kind not in (WORD, QIDENT):
                raise refuse("malformed qualified name")
            start = q
        return start
    if _is_op(tokens, p, ")"):
        o = _matching_open(tokens, p)
        if o is None:
            raise refuse("unbalanced parentheses")
        before = _prev(tokens, o)
        if before is not None:
            b = tokens[before]
            if b.kind == QIDENT or (
                    b.kind == WORD
                    and b.text.upper() not in _EXPR_KEYWORDS):
                raise refuse(f"function call {b.text}(...)")
        return o
    raise refuse(f"after {tok.text!r}")


def _rule_casts(tokens, sql):
    i = 0
    while i < len(tokens):
        if not _is_op(tokens, i, "::"):
            i += 1
            continue
        t = _next(tokens, i)
        if t is None or tokens[t].kind != WORD:
            raise SqliteDialectError(
                "cast to an unrecognised type", construct="::", sql=sql)
        type_name = tokens[t].text.upper()
        known = type_name in _DROP_CASTS or type_name in _INT_CASTS
        if known and _is_op(tokens, _next(tokens, t), "(", "["):
            raise SqliteDialectError(
                f"cast to parameterised/array type {tokens[t].text}",
                construct="::", sql=sql)
        if type_name in _DROP_CASTS:
            _delete(tokens, t)
            _delete(tokens, i)
            i += 1
            continue
        if type_name in _INT_CASTS:
            start = _cast_operand_start(tokens, i, sql)
            tokens[i].kind = RAW
            tokens[i].text = " AS INTEGER)"
            _delete(tokens, t)
            tokens.insert(start, Token(RAW, "CAST("))
            i += 2
            continue
        raise SqliteDialectError(
            f"cast to {tokens[t].text} has no automatic SQLite translation; "
            "use a portable edit", construct="::" + tokens[t].text, sql=sql)


# ---------------------------------------------------------------------------
# Residue scanner
# ---------------------------------------------------------------------------

_RESIDUE_WORDS = {
    "LATERAL": "LATERAL join",
    "INFORMATION_SCHEMA": "information_schema introspection",
    "MAKE_INTERVAL": "make_interval()",
    "INTERVAL": "INTERVAL arithmetic",
    "XMAX": "xmax system column",
    "CTID": "ctid system column",
    "SET_CONFIG": "set_config() GUC",
    "CURRENT_SETTING": "current_setting() GUC",
    "ILIKE": "ILIKE",
    "SIMILAR": "SIMILAR TO",
    "GREATEST": "GREATEST",
    "LEAST": "LEAST",
    "NOWAIT": "NOWAIT",
    # Valid in both engines but a naive local-format text in SQLite: a silent
    # type drift against timestamptz columns.  now() is the supported form.
    "CURRENT_TIMESTAMP": "CURRENT_TIMESTAMP (use now())",
    "CURRENT_DATE": "CURRENT_DATE",
    "CURRENT_TIME": "CURRENT_TIME",
    "LOCALTIMESTAMP": "LOCALTIMESTAMP",
    "LOCALTIME": "LOCALTIME",
}
_RESIDUE_PREFIXES = (("PG_", "pg_* catalog/function"),
                     ("TO_REG", "to_reg*() lookup"))
_RESIDUE_OPS = {
    "::": "PostgreSQL :: cast",
    "[": "array syntax",
    "]": "array syntax",
    "?": "raw ? (jsonb operator / foreign placeholder)",
    ":": "raw : (slice / foreign placeholder)",
    "$": "$n placeholder",
    "~": "regex operator",
    "@": "containment operator",
    "#": "json path operator",
}
_COMPARISONS = frozenset(("=", "<>", "!=", "<", ">", "<=", ">=", "=="))
_SQLITE_CAST_HINTS = ("INT", "CHAR", "CLOB", "TEXT", "BLOB", "REAL", "FLOA",
                      "DOUB")


def _cast_type_ok(type_name):
    upper = type_name.upper()
    return upper == "NUMERIC" or any(h in upper for h in _SQLITE_CAST_HINTS)


def _scan_residue(tokens, sql):
    def refuse(what):
        raise SqliteDialectError(
            f"PostgreSQL-only construct left after rewriting: {what}",
            construct=what, sql=sql)

    first = _next(tokens, -1)
    if _is_word(tokens, first, "DO"):
        refuse("DO block")
    for i, tok in enumerate(tokens):
        kind = tok.kind
        if kind == DOLLAR:
            refuse("dollar-quoted string ($$)")
        elif kind == OP:
            if tok.text in _RESIDUE_OPS:
                refuse(_RESIDUE_OPS[tok.text])
        elif kind == WORD:
            upper = tok.text.upper()
            if upper in _RESIDUE_WORDS:
                refuse(_RESIDUE_WORDS[upper])
            for prefix, label in _RESIDUE_PREFIXES:
                if upper.startswith(prefix):
                    refuse(f"{label} ({tok.text})")
            nxt = _next(tokens, i)
            if upper in ("ANY", "SOME") and _is_op(tokens, nxt, "("):
                refuse(f"{upper}(")
            if upper == "ALL" and _is_op(tokens, nxt, "("):
                prev = _prev(tokens, i)
                if prev is not None and tokens[prev].kind == OP \
                        and tokens[prev].text in _COMPARISONS:
                    refuse("ALL(")
            if upper == "FOR" and _is_word(
                    tokens, nxt, "UPDATE", "SHARE", "NO", "KEY"):
                refuse("row-locking clause")
            if upper == "SKIP" and _is_word(tokens, nxt, "LOCKED"):
                refuse("SKIP LOCKED")
            if upper == "CAST" and _is_op(tokens, nxt, "("):
                close = _matching_close(tokens, nxt)
                if close is None:
                    refuse("unbalanced CAST(")
                depth = 0
                for j in range(nxt, close):
                    if _is_op(tokens, j, "("):
                        depth += 1
                    elif _is_op(tokens, j, ")"):
                        depth -= 1
                    elif depth == 1 and _is_word(tokens, j, "AS"):
                        t = _next(tokens, j)
                        if t is None or tokens[t].kind != WORD \
                                or not _cast_type_ok(tokens[t].text):
                            name = tokens[t].text if t is not None else "?"
                            refuse(f"CAST(.. AS {name})")
                        break


# ---------------------------------------------------------------------------
# Statement classification and rendering
# ---------------------------------------------------------------------------

_READ_KEYWORDS = frozenset(("SELECT", "VALUES", "EXPLAIN", "PRAGMA"))
_DML_KEYWORDS = frozenset(("INSERT", "UPDATE", "DELETE", "REPLACE"))


def _classify(tokens):
    first = _next(tokens, -1)
    while first is not None and _is_op(tokens, first, "("):
        first = _next(tokens, first)
    keyword = ""
    if first is not None and tokens[first].kind == WORD:
        keyword = tokens[first].text.upper()
    has_returning = False
    top_level_dml = False
    depth = 0
    for tok in tokens:
        if tok.kind == OP:
            if tok.text == "(":
                depth += 1
            elif tok.text == ")":
                depth -= 1
        elif tok.kind == WORD:
            upper = tok.text.upper()
            if upper == "RETURNING":
                has_returning = True
            elif depth == 0 and upper in _DML_KEYWORDS:
                top_level_dml = True
    if keyword in _READ_KEYWORDS:
        is_write = False
    elif keyword == "WITH":
        is_write = top_level_dml
    else:
        is_write = True   # unknown keywords are treated as writes
    return keyword, is_write, has_returning


def _render(tokens, sql):
    parts = []
    positional = 0
    named = False
    for tok in tokens:
        if tok.kind == PARAM:
            parts.append("?")
            positional += 1
        elif tok.kind == NPARAM:
            parts.append(":" + tok.value)
            named = True
        elif tok.kind == PCT:
            parts.append("%")
        else:
            parts.append(tok.text)
    if positional and named:
        raise SqliteDialectError(
            "statement mixes %s and %(name)s placeholders",
            construct="placeholders", sql=sql)
    style = "positional" if positional else ("named" if named else "none")
    return "".join(parts), style, positional


def _rewrite(sql):
    if not isinstance(sql, str):
        raise SqliteDialectError(
            f"SQL must be str, got {type(sql).__name__}")
    tokens = tokenize(sql)
    json_positions = set()
    json_names = set()
    locks_rows = _rule_for_update(tokens, sql)
    _rule_distinct_from(tokens, sql)
    _rule_ilike(tokens, sql)
    _rule_now(tokens, sql)
    _rule_greatest_least(tokens, sql)
    _rule_any(tokens, sql, json_positions, json_names)
    _rule_casts(tokens, sql)
    _scan_residue(tokens, sql)
    keyword, is_write, has_returning = _classify(tokens)
    text, style, count = _render(tokens, sql)
    return RewrittenStatement(
        sql=text,
        param_style=style,
        param_count=count,
        json_positions=frozenset(json_positions),
        json_names=frozenset(json_names),
        keyword=keyword,
        is_write=is_write,
        locks_rows=locks_rows,
        has_returning=has_returning,
    )


_rewrite_cached = lru_cache(maxsize=4096)(_rewrite)


def rewrite(sql):
    """Translate one PostgreSQL/psycopg statement to SQLite.

    Returns a cached :class:`RewrittenStatement`; raises
    :class:`SqliteDialectError` (never cached) if the statement cannot be
    translated completely.
    """
    return _rewrite_cached(sql)


def rewrite_cache_info():
    return _rewrite_cached.cache_info()


def clear_rewrite_cache():
    _rewrite_cached.cache_clear()
