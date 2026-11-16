"""The documentation must match the program (audit M12).

Four facts are checked, all offline:

1. Every ``python run.py``, ``python inspect_db.py``, ``python scripts/*.py``
   and ``python tests/mutation_check.py`` invocation written in the user
   documentation names a script that exists and is accepted by that program:

   * ``inspect_db.py`` lines are parsed by its real argument parser;
   * ``run.py`` subcommand lines are parsed by the subcommand's real parser
     (so a missing required argument fails too), and its top-level flags are
     the ones its source tests ``sys.argv`` for;
   * script lines may only use flags the script's source declares.

   A documented flag the program rejects fails the build.
2. Every relative link in the Markdown files resolves to an existing file
   (and, when it carries a ``#fragment``, to an existing heading).
3. ``docs/configuration.md`` lists exactly the keys of
   ``src.config.CONFIG_SCHEMA``, with the type the schema declares.
4. Every ``ini`` example in the user documentation passes the strict start-up
   check of ``config.ini``.

Scope of (1) and (4): README.md, CONTRIBUTING.md, AGENTS.md and
``docs/*.md``. The dated records under ``docs/history/`` are not instructions
and are not checked for commands; their links are checked.

Known limits of (1): only flags and argparse-level rules are checked, not
rules a command applies after parsing (for example "exactly one of --dry-run
or --execute"); a script's flags are the union of every parser in its file.
"""
import ast
import configparser
import contextlib
import functools
import glob
import io
import os
import re
import shlex
import tempfile
import unittest
from unittest import mock

import inspect_db
import run as run_py
from src.config import CONFIG_SCHEMA, validate_config

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

#: Files whose commands are instructions to a user.
COMMAND_DOCS = sorted(
    [os.path.join(ROOT, name)
     for name in ("README.md", "CONTRIBUTING.md", "AGENTS.md")]
    + glob.glob(os.path.join(ROOT, "docs", "*.md")))

#: Files whose links must resolve.
LINK_DOCS = sorted(
    glob.glob(os.path.join(ROOT, "*.md"))
    + glob.glob(os.path.join(ROOT, "examples", "*.md"))
    + glob.glob(os.path.join(ROOT, "docs", "**", "*.md"), recursive=True))

_FENCE = re.compile(
    r"^[ \t]*```([A-Za-z0-9_+-]*)[ \t]*\n(.*?)^[ \t]*```[ \t]*$",
    re.MULTILINE | re.DOTALL)        # also a fence indented inside a list
#: Fenced blocks in these languages are data or code, never a command line.
_NOT_COMMAND_LANGUAGES = {"ini", "json", "jsonl", "python", "sql", "yaml",
                          "yml", "mermaid", "toml", "diff"}
_SEPARATOR = re.compile(r"\s(?:&&|\|\||;)\s")
_QUOTED = re.compile(r"\"[^\"]*\"|'[^']*'")
_MASK = str.maketrans({";": "\x01", "&": "\x02", "|": "\x03"})
_UNMASK = str.maketrans({"\x01": ";", "\x02": "&", "\x03": "|"})
_INLINE = re.compile(r"`([^`\n]+)`")
_INVOCATION = re.compile(
    r"python(?:\.exe)?\s+"
    r"(run\.py|inspect_db\.py|(?:scripts|tests)[\\/][A-Za-z0-9_]+\.py)"
    r"(.*)$")
_HELP = {"-h", "--help"}


# ---------------------------------------------------------------------------
# Extracting invocations from Markdown
# ---------------------------------------------------------------------------

def _logical_lines(block):
    """Lines of a code block with PowerShell/bash continuations joined."""
    joined, pending = [], ""
    for line in block.splitlines():
        stripped = line.rstrip()
        if stripped.endswith("`") or stripped.endswith("\\"):
            pending += stripped[:-1] + " "
            continue
        joined.append(pending + stripped)
        pending = ""
    if pending:
        joined.append(pending)
    return joined


_SHELL_TAIL = re.compile(r"#.*|\||\d?>>?.*|<")


def _tokens(arguments):
    """Argument tokens of one documented invocation.

    Usage notation is normalised: a ``<placeholder>`` becomes the value ``1``
    (valid for both integer and text parameters), ``[--optional]`` brackets
    are removed and an ellipsis is dropped. Whatever follows the command
    itself - a trailing comment, a pipe, a redirection - is cut off, but only
    where it stands as a token of its own: ``"a # b"`` in quotes is a value.
    """
    text = re.sub(r"<[^<>\s][^<>]*>", "1", arguments)
    text = text.replace("[", " ").replace("]", " ")
    try:
        parts = shlex.split(text, posix=False)
    except ValueError:
        parts = text.split()
    tokens = []
    for part in parts:
        if _SHELL_TAIL.fullmatch(part):
            break
        if part not in ("...", "…"):
            tokens.append(part.strip("\"'"))
    return tokens


def documented_invocations(path):
    """``[(script, tokens, source_line)]`` for one Markdown file."""
    with open(path, encoding="utf-8") as handle:
        text = handle.read()
    found = []

    def scan(line):
        # Several commands may share a line (a && b, a ; b): check each one.
        # A separator inside quotes is part of a value, not a separator.
        masked = _QUOTED.sub(
            lambda found: found.group(0).translate(_MASK), line)
        for part in _SEPARATOR.split(masked):
            part = part.translate(_UNMASK)
            match = _INVOCATION.search(part)
            if match:
                script = match.group(1).replace("\\", "/")
                found.append((script, _tokens(match.group(2)), line.strip()))

    for language, block in _FENCE.findall(text):
        if language.lower() not in _NOT_COMMAND_LANGUAGES:
            for line in _logical_lines(block):
                scan(line)
    for span in _INLINE.findall(_FENCE.sub("", text)):
        if span.startswith("python"):
            scan(span)
    return found


# ---------------------------------------------------------------------------
# What each program really accepts
# ---------------------------------------------------------------------------

@functools.lru_cache(maxsize=None)
def _tree(relative):
    with open(os.path.join(ROOT, relative), encoding="utf-8") as handle:
        return ast.parse(handle.read())


@functools.lru_cache(maxsize=None)
def script_flags(relative):
    """Flags a script declares: its ``add_argument`` option strings plus any
    module-level ``SOMETHING_FLAG = "--flag"`` constant (the hand-rolled flag
    of a script that reads ``sys.argv`` itself, e.g. graceful_stop.py)."""
    tree = _tree(relative)
    flags = set()
    for call in ast.walk(tree):
        if (isinstance(call, ast.Call)
                and isinstance(call.func, ast.Attribute)
                and call.func.attr == "add_argument"):
            for argument in call.args:
                if (isinstance(argument, ast.Constant)
                        and isinstance(argument.value, str)
                        and argument.value.startswith("-")):
                    flags.add(argument.value)
    for node in tree.body:
        if (isinstance(node, ast.Assign)
                and all(isinstance(target, ast.Name)
                        and target.id.endswith("_FLAG")
                        for target in node.targets)
                and isinstance(node.value, ast.Constant)
                and isinstance(node.value.value, str)
                and node.value.value.startswith("--")):
            flags.add(node.value.value)
    return frozenset(flags | _HELP)


@functools.lru_cache(maxsize=None)
def run_py_interface():
    """``(top_level_flags, {subcommand: function name})`` read from run.py.

    * a top-level flag is a string run.py tests with ``"--x" in sys.argv``;
    * a subcommand is a string compared with ``sys.argv[1]``; its function is
      the one whose parser is created with ``prog="run.py <subcommand>"``.
    """
    tree = _tree("run.py")
    top, subcommands, functions = set(), set(), {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Compare) and len(node.ops) == 1:
            left, right = node.left, node.comparators[0]
            if "sys.argv" not in ast.unparse(node):
                continue
            if (isinstance(node.ops[0], ast.In)
                    and isinstance(left, ast.Constant)
                    and isinstance(left.value, str)):
                top.add(left.value)
            if (isinstance(node.ops[0], ast.Eq)
                    and isinstance(right, ast.Constant)
                    and isinstance(right.value, str)):
                subcommands.add(right.value)
        if isinstance(node, ast.FunctionDef):
            for call in ast.walk(node):
                if (isinstance(call, ast.Call)
                        and getattr(call.func, "attr", "") == "ArgumentParser"):
                    for keyword in call.keywords:
                        if (keyword.arg == "prog"
                                and isinstance(keyword.value, ast.Constant)
                                and str(keyword.value.value).startswith(
                                    "run.py ")):
                            name = keyword.value.value.split(" ", 1)[1]
                            functions[name] = node.name
    return frozenset(top), {name: functions[name] for name in subcommands}


class _ParsedFine(Exception):
    """Raised in place of the first thing a run.py subcommand does after its
    own ``parse_args`` succeeded, so nothing beyond parsing ever runs."""


def _run_py_subcommand_problem(function_name, arguments):
    """Feed ``arguments`` to the subcommand's REAL parser, and stop there."""
    stderr = io.StringIO()
    with mock.patch.object(run_py, "ConfigManager", side_effect=_ParsedFine), \
            contextlib.redirect_stderr(stderr), \
            contextlib.redirect_stdout(io.StringIO()):
        try:
            getattr(run_py, function_name)(arguments)
        except _ParsedFine:
            return []
        except SystemExit as exc:
            if exc.code in (0, None):
                return []                # --help
            lines = stderr.getvalue().strip().splitlines()
            return [lines[-1] if lines else "rejected by the parser"]
    return ["the subcommand did not stop after parsing"]


def _is_flag(token):
    return token.startswith("-") and not re.fullmatch(r"-?\d+(\.\d+)?", token)


def _flag_names(tokens):
    return [token.split("=", 1)[0] for token in tokens
            if _is_flag(token) and "*" not in token]


def problems_with(script, tokens):
    """Why the program would reject this documented invocation ([] = fine)."""
    if not os.path.isfile(os.path.join(ROOT, script)):
        return [f"{script} does not exist"]
    # A flag-shaped pattern such as --apply-*-schema names a family of flags,
    # not a runnable command: the remaining flags are still checked by name.
    is_family = any(_is_flag(token) and "*" in token for token in tokens)

    if script == "inspect_db.py":
        parser = inspect_db._build_parser()
        if is_family:
            known = set(parser._option_string_actions)
            return [f"inspect_db.py does not accept {flag}"
                    for flag in _flag_names(tokens) if flag not in known]
        stderr = io.StringIO()
        try:
            with contextlib.redirect_stderr(stderr), \
                    contextlib.redirect_stdout(io.StringIO()):
                parser.parse_args(tokens)
        except SystemExit as exc:
            if exc.code not in (0, None):
                lines = stderr.getvalue().strip().splitlines()
                return [lines[-1] if lines else "rejected by the parser"]
        return []

    if script == "run.py":
        top, subcommands = run_py_interface()
        if tokens and not _is_flag(tokens[0]):
            # run.py dispatches on sys.argv[1] only.
            if tokens[0] not in subcommands:
                return [f"run.py has no subcommand {tokens[0]!r}"]
            return _run_py_subcommand_problem(
                subcommands[tokens[0]], tokens[1:])
        problems = [f"run.py does not accept {flag}"
                    for flag in _flag_names(tokens) if flag not in top]
        problems += [f"run.py ignores {token!r} here: a subcommand must be "
                     "the first argument"
                     for token in tokens if not _is_flag(token)]
        return problems

    allowed = script_flags(script)
    return [f"{script} does not accept {flag}"
            for flag in _flag_names(tokens) if flag not in allowed]


# ---------------------------------------------------------------------------
# Links
# ---------------------------------------------------------------------------

_LINK = re.compile(r"!?\[[^\]]*\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")
_REFERENCE = re.compile(r"^\[[^\]]+\]:\s*(\S+)", re.MULTILINE)
_REFERENCE_LABEL = re.compile(r"^\[([^\]]+)\]:\s*\S+", re.MULTILINE)
_REFERENCE_USE = re.compile(r"\[[^\]\n]+\]\[([^\]\n]+)\]")
_HEADING = re.compile(r"^#{1,6}\s+(.*?)\s*#*\s*$", re.MULTILINE)


def _slug(heading):
    """GitHub's anchor for a heading."""
    text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", heading)   # [text](url)
    text = re.sub(r"[^\w\- ]", "", text.strip().lower())
    return text.replace(" ", "-")


@functools.lru_cache(maxsize=None)
def _anchors(path):
    with open(path, encoding="utf-8") as handle:
        text = _FENCE.sub("", handle.read())
    anchors, seen = set(), {}
    for heading in _HEADING.findall(text):
        slug = _slug(heading)
        count = seen.get(slug, 0)
        seen[slug] = count + 1
        anchors.add(slug if not count else f"{slug}-{count}")  # GitHub's rule
    return frozenset(anchors)


def broken_links(path):
    with open(path, encoding="utf-8") as handle:
        text = _FENCE.sub("", handle.read())
    # A link whose TEXT is code keeps its target; a link-shaped string inside
    # a code span is not a link.
    text = _INLINE.sub("", text)
    broken = []
    for target in _LINK.findall(text) + _REFERENCE.findall(text):
        if re.match(r"[a-zA-Z][a-zA-Z0-9+.-]*:", target):
            continue                      # http:, https:, mailto: ...
        file_part, _, fragment = target.partition("#")
        resolved = path if not file_part else os.path.normpath(
            os.path.join(os.path.dirname(path), file_part))
        if not os.path.exists(resolved):
            broken.append(f"{target} -> missing {resolved}")
        elif (fragment and resolved.endswith(".md")
              and fragment not in _anchors(resolved)):
            broken.append(f"{target} -> no heading #{fragment}")
    # A reference-style use, [text][label], needs its "[label]: target" line.
    defined = {label.lower() for label in _REFERENCE_LABEL.findall(text)}
    for label in _REFERENCE_USE.findall(text):
        if label.lower() not in defined:
            broken.append(f"[{label}] -> no reference definition")
    return broken


# ---------------------------------------------------------------------------
# Configuration reference and examples
# ---------------------------------------------------------------------------

def schema_type_text(option):
    """How docs/configuration.md must describe one schema entry's type."""
    if option.status == "retired":
        return "retired"
    if option.kind == "choice":
        text = "choice: " + " / ".join(option.choices)
    elif option.kind in ("int", "float"):
        bounds = []
        if option.above is not None:
            bounds.append(f"> {option.above:g}")
        if option.minimum is not None:
            bounds.append(f">= {option.minimum:g}")
        if option.maximum is not None:
            bounds.append(f"<= {option.maximum:g}")
        text = option.kind + (", " + ", ".join(bounds) if bounds else "")
    else:
        text = option.kind
    return text + (" (inert)" if option.status == "inert" else "")


def documented_config_keys(path):
    """``{(section, key): type_cell}`` from the reference's tables."""
    with open(path, encoding="utf-8") as handle:
        text = handle.read()
    keys, section = {}, None
    for line in text.splitlines():
        heading = re.match(r"^##\s+`\[([A-Z_]+)\]`", line)
        if heading:
            section = heading.group(1)
            continue
        if line.startswith("## "):
            section = None
            continue
        row = re.match(r"^\|\s*`([a-z0-9_]+)`\s*\|\s*([^|]*?)\s*\|", line)
        if row and section:
            keys[(section, row.group(1))] = row.group(2).replace("`", "")
    return keys


def ini_examples(path):
    with open(path, encoding="utf-8") as handle:
        text = handle.read()
    return [block for language, block in _FENCE.findall(text)
            if language.lower() == "ini"]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class DocumentedCommandsTests(unittest.TestCase):
    def test_every_documented_command_is_accepted(self):
        failures, checked = [], 0
        for path in COMMAND_DOCS:
            for script, tokens, line in documented_invocations(path):
                checked += 1
                for problem in problems_with(script, tokens):
                    failures.append(
                        f"{os.path.relpath(path, ROOT)}: `{line}`: {problem}")
        self.assertGreater(checked, 20, "the extractor found almost nothing")
        self.assertEqual(failures, [], "\n" + "\n".join(failures))

    def test_the_checker_rejects_wrong_commands(self):
        # The two lines audit M12 found.
        for arguments in ("--session-forensics --session <N>",
                          "--reboot-sentinel-status"):
            self.assertTrue(
                problems_with("inspect_db.py", _tokens(arguments)), arguments)
        self.assertTrue(problems_with("run.py", ["--no-such-flag"]))
        self.assertTrue(problems_with("run.py", ["no-such-subcommand"]))
        self.assertTrue(problems_with(
            "run.py", ["remote-archive", "--delete-catalog"]))
        # Required arguments are enforced by the subcommand's real parser.
        self.assertTrue(problems_with("run.py", ["tape-reset", "--yes"]))
        # run.py only looks at sys.argv[1] for a subcommand.
        self.assertTrue(problems_with(
            "run.py", ["--backup-db", "remote-archive"]))
        # A wildcard VALUE does not switch the check off.
        self.assertTrue(problems_with(
            "inspect_db.py", ["--manifest-search", "*.txt", "--nope"]))
        # A flag family is still checked for its other flags.
        self.assertTrue(problems_with(
            "inspect_db.py", ["--apply-*-schema", "--nope"]))
        self.assertTrue(problems_with(
            "scripts/install_smoke_test.py", ["--no-such-flag"]))
        self.assertTrue(problems_with(
            "scripts/graceful_stop.py", ["1", "--force"]))
        self.assertTrue(problems_with("scripts/no_such_script.py", []))

    def test_the_checker_accepts_real_commands(self):
        for script, arguments in (
                ("run.py", ""),
                ("run.py", "--version"),
                ("run.py", "--backup-db"),
                ("run.py", "remote-archive [--resume] [--non-interactive]"),
                ("run.py", "tape-reset --tape <LABEL> --impact-report <FILE> "
                           "--delete-catalog --yes"),
                ("inspect_db.py", ""),
                ("inspect_db.py", "--session-forensics --idle-seconds <N>"),
                ("inspect_db.py", "--apply-*-schema --execute"),
                ("inspect_db.py", '--manifest-search "*.txt" --limit 5'),
                ("scripts/graceful_stop.py", "<pid> --allow-active-tape-write"),
                ("scripts/install_smoke_test.py", "--backend sqlite"),
                ("tests/mutation_check.py", "-k gate")):
            self.assertEqual(
                problems_with(script, _tokens(arguments)), [], arguments)

    def test_run_py_interface_is_read_from_the_source(self):
        top, subcommands = run_py_interface()
        self.assertEqual(top, {"--version", "--backup-db"})
        self.assertEqual(set(subcommands), {"remote-archive", "tape-reset"})
        for function_name in subcommands.values():
            self.assertTrue(callable(getattr(run_py, function_name)))

    def test_several_commands_on_one_line_are_all_checked(self):
        with tempfile.TemporaryDirectory() as folder:
            path = os.path.join(folder, "page.md")
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(
                    "```console\n"
                    "python run.py --version && python inspect_db.py --nope\n"
                    "python inspect_db.py --manifest-search \"a ; b\" --nope\n"
                    "```\n")
            found = documented_invocations(path)
        self.assertEqual([script for script, _, _ in found],
                         ["run.py", "inspect_db.py", "inspect_db.py"])
        self.assertTrue(problems_with(found[1][0], found[1][1]))
        # The separator inside the quoted value did not hide the bad flag.
        self.assertEqual(found[2][1],
                         ["--manifest-search", "a ; b", "--nope"])
        self.assertTrue(problems_with(found[2][0], found[2][1]))


class DocumentationLinksTests(unittest.TestCase):
    def test_every_relative_link_resolves(self):
        failures = []
        for path in LINK_DOCS:
            for problem in broken_links(path):
                failures.append(f"{os.path.relpath(path, ROOT)}: {problem}")
        self.assertGreater(len(LINK_DOCS), 20)
        self.assertEqual(failures, [], "\n" + "\n".join(failures))

    def test_the_link_checker_sees_broken_links(self):
        with tempfile.TemporaryDirectory() as folder:
            path = os.path.join(folder, "page.md")
            with open(path, "w", encoding="utf-8") as handle:
                handle.write(
                    "# Title\n\n## Part\n\n## Part\n\n"
                    "[ok](#part) [ok](#part-1) [ok](page.md#title) "
                    "[web](https://example.org/x)\n"
                    "[`code`](missing.md) [bad](#nope) [case](#PART) "
                    "[ref][r] [text][undefined]\n\n"
                    "[r]: also-missing.md\n")
            problems = broken_links(path)
        self.assertEqual(len(problems), 5, problems)

    def test_shell_tail_is_cut_but_quoted_values_are_kept(self):
        self.assertEqual(_tokens('--manifest-search "a # b" --limit 5  # note'),
                         ["--manifest-search", "a # b", "--limit", "5"])
        self.assertEqual(_tokens("--print-db-target | Out-File x.json"),
                         ["--print-db-target"])
        self.assertEqual(_tokens("--backup-db > log.txt 2>&1"),
                         ["--backup-db"])


class ConfigurationReferenceTests(unittest.TestCase):
    PATH = os.path.join(ROOT, "docs", "configuration.md")

    def test_reference_lists_exactly_the_schema_keys(self):
        documented = documented_config_keys(self.PATH)
        schema = {(section, key): option
                  for section, options in CONFIG_SCHEMA.items()
                  for key, option in options.items()}
        self.assertEqual(
            sorted(set(schema) - set(documented)), [],
            "in CONFIG_SCHEMA but missing from docs/configuration.md")
        self.assertEqual(
            sorted(set(documented) - set(schema)), [],
            "in docs/configuration.md but not in CONFIG_SCHEMA")

    def test_reference_states_the_schema_type_of_every_key(self):
        documented = documented_config_keys(self.PATH)
        wrong = []
        for section, options in CONFIG_SCHEMA.items():
            for key, option in options.items():
                expected = schema_type_text(option)
                actual = documented.get((section, key))
                if actual != expected:
                    wrong.append(
                        f"[{section}] {key}: documented {actual!r}, "
                        f"schema says {expected!r}")
        self.assertEqual(wrong, [], "\n" + "\n".join(wrong))

    def test_every_ini_example_passes_the_strict_startup_check(self):
        failures, checked = [], 0
        for path in COMMAND_DOCS:
            for block in ini_examples(path):
                checked += 1
                parser = configparser.ConfigParser(interpolation=None)
                parser.read_string(block)
                errors, notes = validate_config(parser)
                for message in errors + [
                        note for note in notes if "not a known" in note]:
                    failures.append(
                        f"{os.path.relpath(path, ROOT)}: {message}")
        self.assertGreater(checked, 3)
        self.assertEqual(failures, [], "\n" + "\n".join(failures))


if __name__ == "__main__":
    unittest.main()
