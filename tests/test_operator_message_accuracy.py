"""Operator-facing messages must name things that exist.

* Every ``[SECTION] key`` cited in a string of ``src/db_backend.py`` is a real
  setting. The sealed-batch refusal used to cite
  ``[PERFORMANCE] tape_write_batch_mode``, a key that never existed.
* The "psycopg is not installed" message names the requirements file that
  actually holds the PostgreSQL driver, and says that SQLite needs no server.
"""
import ast
import os
import re
import unittest
from unittest import mock

from src import db_backend, pg_bulk
from src.config import CONFIG_SCHEMA

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_SETTING = re.compile(r"\[([A-Z][A-Z_]*)\]\s+([a-z][a-z0-9_]*)")


def _string_constants(path):
    with open(path, encoding="utf-8") as handle:
        tree = ast.parse(handle.read())
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            yield node.value


def cited_settings(path):
    found = set()
    for text in _string_constants(path):
        found.update(_SETTING.findall(text))
    return found


class DbBackendCitesRealSettings(unittest.TestCase):
    def test_every_cited_setting_is_in_the_schema(self):
        cited = cited_settings(os.path.join(REPO_ROOT, "src", "db_backend.py"))
        self.assertIn(("DATABASE", "backend"), cited)   # the scan sees them
        unknown = sorted(
            f"[{section}] {key}" for section, key in cited
            if key not in CONFIG_SCHEMA.get(section, {}))
        self.assertEqual(unknown, [])

    def test_the_sealed_batch_refusal_names_the_real_switch(self):
        message = str(db_backend.PostgresOnlyFeature("sealed_batches"))
        self.assertIn("[FEATURES] sealed_tape_write_batches_enabled", message)
        self.assertIn("sealed_tape_write_batches_enabled",
                      CONFIG_SCHEMA["FEATURES"])
        self.assertNotIn("tape_write_batch_mode", message)

    def test_the_scan_would_catch_an_invented_key(self):
        self.assertEqual(
            _SETTING.findall("([PERFORMANCE] tape_write_batch_mode)"),
            [("PERFORMANCE", "tape_write_batch_mode")])
        self.assertNotIn("tape_write_batch_mode",
                         CONFIG_SCHEMA["PERFORMANCE"])


class DocumentsNamedInCodeExist(unittest.TestCase):
    """A comment or message that sends the reader to ``docs/x.md`` must point
    at a file that is there. (``scripts/sql`` is left out: applied migrations
    are never edited.)"""

    def _sources(self):
        import glob
        files = [os.path.join(REPO_ROOT, "run.py"),
                 os.path.join(REPO_ROOT, "inspect_db.py")]
        for pattern in (("src", "*.py"), ("scripts", "*.py"),
                        ("scripts", "*.ps1")):
            files += glob.glob(os.path.join(REPO_ROOT, *pattern))
        return sorted(files)

    def test_every_docs_path_in_code_resolves(self):
        missing = set()
        seen = 0
        for path in self._sources():
            with open(path, encoding="utf-8", errors="replace") as handle:
                text = handle.read()
            for cited in re.findall(r"docs/[A-Za-z0-9_./-]+\.md", text):
                seen += 1
                if not os.path.isfile(
                        os.path.join(REPO_ROOT, *cited.split("/"))):
                    missing.add(
                        f"{os.path.basename(path)} -> {cited}")
        self.assertGreater(seen, 3)
        self.assertEqual(sorted(missing), [])


class MissingPsycopgMessage(unittest.TestCase):
    def _message(self):
        with mock.patch.object(pg_bulk, "psycopg", None):
            with self.assertRaises(RuntimeError) as caught:
                pg_bulk.require_psycopg()
        return str(caught.exception)

    def test_it_names_the_postgresql_requirements_file(self):
        message = self._message()
        self.assertIn("requirements-postgres.txt", message)
        self.assertNotIn("-r requirements.txt", message)
        self.assertTrue(os.path.isfile(
            os.path.join(REPO_ROOT, "requirements-postgres.txt")))

    def test_it_says_sqlite_needs_no_server(self):
        message = self._message()
        self.assertIn("[DATABASE] backend = sqlite", message)
        self.assertIn("no server", message)


if __name__ == "__main__":
    unittest.main()
