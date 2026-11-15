"""Facts about source CODE, read from the syntax tree instead of raw text.

Audit M11 counted 85 tests that assert a substring appears in source text, one
of them satisfied by a word that occurs only in a comment. A substring search
cannot tell a call from a comment, a docstring, a log message or a longer
identifier that merely contains the word.

Where an invariant can be exercised, it now is (see
``test_write_gate_obeyed_*``). What remains here is for invariants that are
genuinely properties of the source - "this module must never CALL eject",
"this loop calls no tape probe" - and those are answered from the AST, so:

* comments and docstrings can never satisfy or violate a check;
* a string literal is not a call (``print("never ejects")`` calls ``print``);
* names match exactly (``_write_chunk`` is not ``_write_chunk_group``).

Not collected by pytest (no ``test_`` prefix).
"""
import ast
import copy
import inspect
import os
import textwrap

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def tree_of(target):
    """AST for a function, method, class, module object, ``src`` module name
    (``"remote_writer"``), or a path to a ``.py`` file."""
    if isinstance(target, ast.AST):
        return target
    if isinstance(target, str):
        path = target
        if not target.endswith(".py"):
            path = os.path.join(PROJECT_ROOT, "src", f"{target}.py")
        elif not os.path.isabs(target):
            path = os.path.join(PROJECT_ROOT, target)
        with open(path, encoding="utf-8") as handle:
            return ast.parse(handle.read(), path)
    return ast.parse(textwrap.dedent(inspect.getsource(target)))


def _name_of(func):
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return None


def calls(target):
    """Every call in ``target`` as a list of names, in source order.

    ``foo(...)`` -> ``"foo"``; ``self.host._gate(...)`` -> ``"_gate"``.
    """
    found = []
    for node in ast.walk(tree_of(target)):
        if isinstance(node, ast.Call):
            name = _name_of(node.func)
            if name:
                found.append((node.lineno, node.col_offset, name))
    return [name for _line, _col, name in sorted(found)]


def called(target):
    """The set of names ``target`` calls."""
    return set(calls(target))


def call_count(target, name):
    return calls(target).count(name)


def names(target):
    """Every identifier the code REFERENCES: bare names, attribute names,
    imported names, and keyword-argument names. Comments, docstrings and
    string literals contribute nothing."""
    found = set()
    for node in ast.walk(tree_of(target)):
        if isinstance(node, ast.Name):
            found.add(node.id)
        elif isinstance(node, ast.Attribute):
            found.add(node.attr)
        elif isinstance(node, ast.alias):
            found.add(node.name.split(".")[-1])
            if node.asname:
                found.add(node.asname)
        elif isinstance(node, ast.keyword) and node.arg:
            found.add(node.arg)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef,
                               ast.ClassDef)):
            found.add(node.name)
        elif isinstance(node, ast.arg):
            found.add(node.arg)
    return found


def identifiers_matching(target, fragment, ignore_case=False):
    """Referenced identifiers that CONTAIN ``fragment``.

    The AST replacement for ``assertNotIn(fragment, source_text)``: it keeps
    the substring reach (``eject`` still finds ``eject_tape_drive``) but only
    over real identifiers, so prose about ejecting cannot trip it."""
    if ignore_case:
        fragment = fragment.lower()
        return {n for n in names(target) if fragment in n.lower()}
    return {n for n in names(target) if fragment in n}


def imported_names(target):
    """Names bound by ``import`` / ``from ... import`` statements."""
    found = set()
    for node in ast.walk(tree_of(target)):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            for alias in node.names:
                found.add(alias.name.split(".")[-1])
                if alias.asname:
                    found.add(alias.asname)
    return found


def _docstring_nodes(tree):
    nodes = set()
    for node in ast.walk(tree):
        body = getattr(node, "body", None)
        if not isinstance(body, list) or not body:
            continue
        first = body[0]
        if (isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef,
                              ast.AsyncFunctionDef))
                and isinstance(first, ast.Expr)
                and isinstance(first.value, ast.Constant)
                and isinstance(first.value.value, str)):
            nodes.add(id(first.value))
    return nodes


def string_literals(target):
    """Every string constant in the code, docstrings excluded.

    f-string fragments are included as their literal pieces. Use this for SQL
    text or an executable name embedded in code."""
    tree = tree_of(target)
    skip = _docstring_nodes(tree)
    return [node.value for node in ast.walk(tree)
            if isinstance(node, ast.Constant) and isinstance(node.value, str)
            and id(node) not in skip]


def literal_text(target):
    """All non-docstring string constants joined - for 'this SQL mentions X'."""
    return "\n".join(string_literals(target))


class _Scrub(ast.NodeTransformer):
    """Drop docstrings; optionally blank every string literal."""

    def __init__(self, strings):
        self.strings = strings

    def _strip_docstring(self, node):
        body = node.body
        if (body and isinstance(body[0], ast.Expr)
                and isinstance(body[0].value, ast.Constant)
                and isinstance(body[0].value.value, str)):
            node.body = body[1:] or [ast.Pass()]
        self.generic_visit(node)
        return node

    visit_Module = visit_ClassDef = _strip_docstring
    visit_FunctionDef = visit_AsyncFunctionDef = _strip_docstring

    def visit_Constant(self, node):
        if not self.strings and isinstance(node.value, str):
            return ast.copy_location(ast.Constant(value=""), node)
        return node


def code_text(target, strings=True):
    """``target`` re-rendered from its syntax tree: CODE ONLY.

    The drop-in replacement for ``inspect.getsource`` in a test that must
    look at source. Comments and docstrings do not survive the round trip, so
    neither can satisfy (or trip) an assertion. String literals are kept by
    default because embedded SQL is code in this project; pass
    ``strings=False`` (or use :func:`code_only`) when the assertion is about
    identifiers and calls, so a log message cannot stand in for a call. The
    expressions inside an f-string are code and always survive.

    Formatting is normalised by ``ast.unparse`` (single-quoted strings, one
    statement per line), so match on names and short fragments, not layout.
    """
    tree = tree_of(target)
    if tree is target:                 # never mutate a caller's tree
        tree = copy.deepcopy(tree)
    tree = _Scrub(strings).visit(tree)
    return ast.unparse(ast.fix_missing_locations(tree))


def code_only(target):
    """:func:`code_text` with every string literal blanked."""
    return code_text(target, strings=False)


def function(target, name):
    """The ``FunctionDef`` called ``name`` anywhere inside ``target``."""
    for node in ast.walk(tree_of(target)):
        if (isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                and node.name == name):
            return node
    raise LookupError(f"no function {name!r} in {target!r}")


def first_call_line(target, name):
    """Line (within ``target``) of the first call to ``name``; LookupError if
    it is never called."""
    for node in sorted((n for n in ast.walk(tree_of(target))
                        if isinstance(n, ast.Call)),
                       key=lambda n: (n.lineno, n.col_offset)):
        if _name_of(node.func) == name:
            return node.lineno
    raise LookupError(f"{name} is never called")
