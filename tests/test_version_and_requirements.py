"""The version has one source, and the dependency sets stay consistent.

``python run.py --version`` and ``python inspect_db.py --version`` must answer
on a machine that has nothing but the source tree: no config.ini, no database
and no psycopg. Each is run as a real child process from an empty working
directory, with ``psycopg`` and ``PySide6`` made unimportable.
"""
import os
import re
import subprocess
import sys
import tempfile
import unittest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

import src  # noqa: E402

#: Runs an entry point as ``__main__`` with the optional drivers blocked. A
#: ``None`` entry in ``sys.modules`` makes ``import psycopg`` raise
#: ImportError, exactly as on a machine where it is not installed.
_RUNNER = (
    "import runpy, sys\n"
    "for name in ('psycopg', 'psycopg_pool', 'PySide6'):\n"
    "    sys.modules[name] = None\n"
    "script = sys.argv[1]\n"
    "sys.argv = [script] + sys.argv[2:]\n"
    "runpy.run_path(script, run_name='__main__')\n"
)


def _requirements(name):
    """``(includes, packages)`` of one requirements file."""
    includes, packages = [], []
    with open(os.path.join(REPO_ROOT, name), encoding="utf-8") as handle:
        for raw in handle:
            line = raw.split("#", 1)[0].strip()
            if not line:
                continue
            if line.startswith("-r"):
                includes.append(line[2:].strip())
            else:
                packages.append(line)
    return includes, packages


def _resolved(name):
    """Every package line a requirements file installs, includes followed."""
    includes, packages = _requirements(name)
    found = set(packages)
    for include in includes:
        found |= _resolved(include)
    return found


class VersionFlag(unittest.TestCase):
    def _version_of(self, script):
        with tempfile.TemporaryDirectory(prefix="lto_version_") as cwd:
            env = {key: value for key, value in os.environ.items()
                   if key not in ("PYTHONPATH", "PYTHONSTARTUP")}
            result = subprocess.run(
                [sys.executable, "-c", _RUNNER,
                 os.path.join(REPO_ROOT, script), "--version"],
                cwd=cwd, env=env, capture_output=True, text=True, timeout=120)
            # Nothing may be created where the command was run: a first-run
            # config.ini or database here would mean it did real start-up.
            leftovers = os.listdir(cwd)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(leftovers, [])
        return result.stdout.strip()

    def test_run_py_prints_the_version_without_config_or_database(self):
        self.assertEqual(self._version_of("run.py"),
                         f"owc-lto8-archiver {src.__version__}")

    def test_inspect_db_prints_the_version_without_config_or_database(self):
        self.assertEqual(self._version_of("inspect_db.py"),
                         f"owc-lto8-archiver {src.__version__}")

    def test_inspect_db_parser_lists_the_flag(self):
        import inspect_db
        self.assertIn("--version", inspect_db._build_parser().format_help())

    def test_version_is_a_single_literal_in_the_package(self):
        """The release builder reads the version without importing ``src``."""
        with open(os.path.join(REPO_ROOT, "src", "__init__.py"),
                  encoding="utf-8") as handle:
            found = re.findall(
                r'^__version__\s*=\s*"([^"]+)"\s*$', handle.read(), re.M)
        self.assertEqual(found, [src.__version__])

    def test_version_is_pep440(self):
        self.assertIsNotNone(
            re.fullmatch(r"\d+\.\d+\.\d+(?:(?:a|b|rc)\d+)?", src.__version__),
            src.__version__)


class RequirementSets(unittest.TestCase):
    def test_core_has_no_database_driver_and_no_gui_toolkit(self):
        includes, packages = _requirements("requirements-core.txt")
        self.assertEqual(includes, [])
        self.assertEqual(sorted(packages), ["psutil>=5.9", "zstandard>=0.23"])

    def test_postgres_is_core_plus_psycopg(self):
        includes, packages = _requirements("requirements-postgres.txt")
        self.assertEqual(includes, ["requirements-core.txt"])
        self.assertEqual(packages, ["psycopg[binary,pool]>=3.2"])

    def test_gui_is_pyside_only(self):
        self.assertEqual(_requirements("requirements-gui.txt"),
                         ([], ["PySide6>=6.7"]))

    def test_full_file_is_exactly_the_three_sets(self):
        """requirements.txt keeps installing what it always installed."""
        self.assertEqual(
            _resolved("requirements.txt"),
            {"psutil>=5.9", "zstandard>=0.23", "psycopg[binary,pool]>=3.2",
             "PySide6>=6.7"})
        self.assertEqual(
            _resolved("requirements.txt"),
            _resolved("requirements-core.txt")
            | _resolved("requirements-postgres.txt")
            | _resolved("requirements-gui.txt"))


if __name__ == "__main__":
    unittest.main()
