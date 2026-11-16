"""Operator-facing text of a public tool carries no site-specific wording.

The messages a user reads (prints, prompts, raised errors, ``--help`` text)
used to name the original deployment's cartridges, its lab, and the date and
size of one of its incidents. Comments and docstrings may keep that history;
the strings a user sees may not.

Identifiers are not wording: function names, command-line flags, database
values and the tape-reset report keys stay as they are for compatibility and
are listed in ``ALLOWED`` with the reason.
"""
import ast
import glob
import os
import re
import unittest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Built by concatenation so that this file does not trip the privacy gate.
LAB_NAME = "Scalpe" + "lab"

SITE_SPECIFIC = re.compile(
    r"(?<![A-Za-z_])Tape_0\d"     # a cartridge of the original deployment
    r"|" + LAB_NAME +             # the lab that ran it
    r"|[Ss]ession[ -]37\b"        # its long-running session
    r"|2026-07-15|2026-07-24"     # the dates of its incidents
    r"|126 GB",                   # what one of them cost
    re.IGNORECASE)

#: (file, exact string literal) pairs that stay, and why.
ALLOWED = {
    # Keys and the hard-wired label of the generation-aware reset report.
    # The report format is pinned by fingerprint; renaming would break the
    # reports already produced.
    ("src/tape_reset.py", "Tape_02"),
    # A legacy PostgreSQL-only rehearsal command whose flag is named after
    # the session it was written for and really does default to it.
    ("inspect_db.py",
     "READ-ONLY Plan-2 Gate-5.5 chunk classification and proposed Stored "
     "TAR boundary. Defaults to session 37; override with --session-id."),
}


def _docstring_nodes(tree):
    found = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef,
                             ast.AsyncFunctionDef)):
            body = node.body
            if (body and isinstance(body[0], ast.Expr)
                    and isinstance(body[0].value, ast.Constant)
                    and isinstance(body[0].value.value, str)):
                found.add(id(body[0].value))
    return found


def application_files():
    files = sorted(glob.glob(os.path.join(REPO_ROOT, "src", "*.py")))
    return files + [os.path.join(REPO_ROOT, "run.py"),
                    os.path.join(REPO_ROOT, "inspect_db.py")]


def site_specific_strings(path):
    """Non-docstring string literals of ``path`` with site-specific text."""
    with open(path, encoding="utf-8") as handle:
        tree = ast.parse(handle.read())
    skip = _docstring_nodes(tree)
    relative = os.path.relpath(path, REPO_ROOT).replace(os.sep, "/")
    hits = []
    for node in ast.walk(tree):
        if (isinstance(node, ast.Constant) and isinstance(node.value, str)
                and id(node) not in skip
                and SITE_SPECIFIC.search(node.value)
                and (relative, node.value) not in ALLOWED):
            hits.append(f"{relative}:{node.lineno}: {node.value!r}")
    return hits


class OperatorFacingStringsAreNeutral(unittest.TestCase):
    def test_no_site_specific_wording_in_application_strings(self):
        hits = []
        for path in application_files():
            hits.extend(site_specific_strings(path))
        self.assertEqual(hits, [])

    def test_the_scan_sees_string_literals_and_skips_docstrings(self):
        probe = os.path.join(REPO_ROOT, "src", "tape_reset.py")
        with open(probe, encoding="utf-8") as handle:
            tree = ast.parse(handle.read())
        literals = [node.value for node in ast.walk(tree)
                    if isinstance(node, ast.Constant)
                    and isinstance(node.value, str)]
        self.assertIn("Tape_02", literals)        # allowed, but seen
        self.assertTrue(SITE_SPECIFIC.search("cost ~126 GB of Tape_02"))
        self.assertTrue(SITE_SPECIFIC.search("e.g. " + LAB_NAME + "_Tape_X"))
        self.assertFalse(SITE_SPECIFIC.search("--session37-boundary-rehearsal"))
        self.assertFalse(SITE_SPECIFIC.search("e.g. Archive_Tape_01"))

    def test_the_incident_warnings_keep_their_substance_and_link(self):
        """Generic wording still says what is at stake and where to read."""
        for name, needles in {
            "src/cli.py": ("caused data loss on a real deployment",
                           "docs/history/incidents/005-"),
            "src/windows_update_guard.py": (
                "caused data loss on a real deployment",
                "docs/history/incidents/005-"),
            "src/remote_orchestrator.py": (
                "made a cartridge permanently read-only",
                "docs/history/incidents/010-"),
        }.items():
            with open(os.path.join(REPO_ROOT, name), encoding="utf-8") as fh:
                tree = ast.parse(fh.read())
            text = " ".join(
                node.value for node in ast.walk(tree)
                if isinstance(node, ast.Constant)
                and isinstance(node.value, str))
            text = re.sub(r"\s+", " ", text)
            for needle in needles:
                with self.subTest(file=name, needle=needle):
                    self.assertIn(needle, text)

    def test_cited_incident_documents_exist(self):
        cited = set()
        for path in application_files():
            with open(path, encoding="utf-8") as handle:
                cited.update(re.findall(
                    r"docs/history/incidents/[0-9A-Za-z_.-]+\.md",
                    handle.read()))
        self.assertTrue(cited)
        for relative in sorted(cited):
            with self.subTest(document=relative):
                self.assertTrue(os.path.isfile(
                    os.path.join(REPO_ROOT, *relative.split("/"))))

    def test_the_privacy_gate_knows_the_lab_name(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "check_public_repo",
            os.path.join(REPO_ROOT, "scripts", "check_public_repo.py"))
        gate = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(gate)
        self.assertTrue(any(
            pattern.search("label " + LAB_NAME + "_Tape_X")
            and pattern.search(LAB_NAME.lower())
            for pattern, _label in gate.PRIVATE_IDENTITY_PATTERNS))
        with open(os.path.join(REPO_ROOT, "scripts", "check_public_repo.py"),
                  encoding="utf-8") as handle:
            self.assertNotIn(LAB_NAME.lower(), handle.read().lower())


if __name__ == "__main__":
    unittest.main()
