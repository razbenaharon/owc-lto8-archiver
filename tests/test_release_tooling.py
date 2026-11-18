"""Release tooling: the package builder, the install smoke test's offline
parts, and the shape of the CI workflows.

Nothing here needs a network, a database server or a tape. The package
builder is exercised against small throwaway git repositories; the real
``dist/`` of this checkout is never written.
"""
import configparser
import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
import zipfile
from unittest import mock

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO_ROOT, "scripts"))

import build_release            # noqa: E402
import install_smoke_test       # noqa: E402

import src                      # noqa: E402
from src.config import ConfigManager  # noqa: E402

WORKFLOWS = os.path.join(REPO_ROOT, ".github", "workflows")


def _git(repo, *args):
    subprocess.run(
        ["git", "-C", repo, "-c", "user.name=t", "-c", "user.email=t@t.invalid",
         "-c", "commit.gpgsign=false", "-c", "core.autocrlf=false", *args],
        check=True, capture_output=True, text=True)


class _ThrowawayRepo(unittest.TestCase):
    """A minimal committed tree that satisfies the package rules."""

    VERSION = "9.8.7rc2"
    TAG = "v9.8.7-rc.2"

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="lto_release_")
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.repo = os.path.join(self.tmp, "repo")
        self.out = os.path.join(self.tmp, "out")
        files = {name: f"{name}\n" for name in build_release.REQUIRED_FILES}
        files["src/__init__.py"] = f'__version__ = "{self.VERSION}"\n'
        files[f"docs/releases/{self.TAG}.md"] = "# notes\n"
        # The real privacy gate, run against this throwaway tree.
        with open(os.path.join(REPO_ROOT, "scripts", "check_public_repo.py"),
                  encoding="utf-8") as handle:
            files["scripts/check_public_repo.py"] = handle.read()
        files[".gitignore"] = "config.ini\n.env\ndist/\n"
        for rel, content in files.items():
            self._write(rel, content)
        _git(self.tmp, "init", "-q", self.repo)
        self._commit("initial")

    def _write(self, rel, content):
        path = os.path.join(self.repo, *rel.split("/"))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(content)

    def _commit(self, message, *add_args):
        _git(self.repo, "add", "-A", *add_args)
        _git(self.repo, "commit", "-q", "-m", message)

    def _build(self, **kwargs):
        return build_release.build(self.repo, self.out, **kwargs)


class BuildRelease(_ThrowawayRepo):
    def test_builds_one_zip_of_tracked_files_and_a_checksum(self):
        # Local files that exist but are not tracked must not be packaged.
        self._write("config.ini", "[PATHS]\n")
        self._write(".env", "X=1\n")
        result = self._build(expect_tag=self.TAG)

        self.assertEqual(result["zip_name"],
                         f"tape-ledger-{self.VERSION}.zip")
        self.assertTrue(result["prerelease"])
        self.assertEqual(sorted(os.listdir(self.out)),
                         ["SHA256SUMS.txt", result["zip_name"]])
        prefix = f"tape-ledger-{self.VERSION}/"
        with zipfile.ZipFile(result["zip"]) as bundle:
            names = bundle.namelist()
        self.assertTrue(all(name.startswith(prefix) for name in names))
        members = {name[len(prefix):] for name in names}
        self.assertNotIn("config.ini", members)
        self.assertNotIn(".env", members)
        for required in ("requirements-core.txt", "requirements-postgres.txt",
                         "config.example.ini"):
            self.assertIn(required, members)
        self.assertEqual(result["file_count"],
                         len([n for n in names if not n.endswith("/")]))
        with open(result["checksums"], encoding="utf-8") as handle:
            line = handle.read()
        self.assertEqual(
            line, f"{build_release.sha256_of(result['zip'])}  "
                  f"{result['zip_name']}\n")

    def test_same_commit_gives_the_same_bytes(self):
        first = self._build()["sha256"]
        self.assertEqual(self._build()["sha256"], first)

    def test_refuses_a_dirty_tree_and_writes_nothing(self):
        self._write("run.py", "changed\n")
        with self.assertRaisesRegex(build_release.BuildRefused, "not clean"):
            self._build()
        self.assertFalse(os.path.exists(self.out))

    def test_refuses_an_untracked_file(self):
        self._write("notes_to_self.txt", "x\n")
        with self.assertRaisesRegex(build_release.BuildRefused, "not clean"):
            self._build()

    def test_refuses_when_the_tag_does_not_match_the_version(self):
        with self.assertRaisesRegex(build_release.BuildRefused,
                                    "v9.8.7-rc.2"):
            self._build(expect_tag="v9.8.7")
        self.assertFalse(os.path.exists(self.out))

    def test_refuses_without_release_notes(self):
        os.remove(os.path.join(self.repo, "docs", "releases",
                               self.TAG + ".md"))
        self._commit("drop notes")
        with self.assertRaisesRegex(build_release.BuildRefused,
                                    "release notes are missing"):
            self._build()

    def test_refuses_when_the_privacy_gate_fails(self):
        # A tracked config.ini is exactly what the gate exists to stop.
        self._write("config.ini", "[PATHS]\n")
        self._commit("oops", "-f")
        with self.assertRaisesRegex(build_release.BuildRefused,
                                    "privacy gate failed"):
            self._build()
        self.assertFalse(os.path.exists(self.out))

    def test_a_private_file_in_the_zip_deletes_the_package(self):
        """The zip check is independent of the gate: with the gate silenced,
        a packaged config.ini still stops the build and leaves no zip."""
        self._write("config.ini", "[PATHS]\n")
        self._commit("oops", "-f")
        with mock.patch.object(build_release, "run_privacy_gate",
                               return_value="(gate skipped)"):
            with self.assertRaisesRegex(build_release.BuildRefused,
                                        "operator configuration"):
                self._build()
        self.assertEqual(os.listdir(self.out), [])

    def test_a_missing_requirement_set_deletes_the_package(self):
        os.remove(os.path.join(self.repo, "requirements-core.txt"))
        self._commit("drop core requirements")
        with self.assertRaisesRegex(build_release.BuildRefused,
                                    "missing requirements-core.txt"):
            self._build()
        self.assertEqual(os.listdir(self.out), [])

    def test_main_reports_a_refusal_as_exit_code_1(self):
        self._write("run.py", "changed\n")
        with mock.patch("sys.stderr"):
            code = build_release.main(
                ["--repo", self.repo, "--output-dir", self.out])
        self.assertEqual(code, 1)


class PackageRules(unittest.TestCase):
    def test_forbidden_patterns(self):
        def label(path):
            for pattern, name in build_release.FORBIDDEN_PATTERNS:
                if re.search(pattern, path.lower()):
                    return name
            return None

        for bad in ("config.ini", "sub/config.ini", "config.ini.bak2", ".env",
                    ".env.local", "lto_archive.sqlite3", "x/catalog.db",
                    "catalog.sqlite3-wal", "db_backups/a.txt", "dump.dump",
                    "backup_logs/SUMMARY.csv", "archiver.log", "private/x.md",
                    "src/__pycache__/a.pyc"):
            self.assertIsNotNone(label(bad), bad)
        for good in ("config.example.ini", ".env.example", "src/db_backup.py",
                     "scripts/sql/sqlite/001_schema.sql", "src/logsetup.py",
                     "docs/releases/v1.0.0-rc.1.md", "src/config.py"):
            self.assertIsNone(label(good), good)

    def test_version_and_tag_spelling(self):
        self.assertEqual(build_release.tag_for("1.0.0rc1"), "v1.0.0-rc.1")
        self.assertEqual(build_release.tag_for("1.2.3"), "v1.2.3")
        self.assertEqual(build_release.tag_for("2.0.0b3"), "v2.0.0-beta.3")
        self.assertTrue(build_release.is_prerelease("1.0.0rc1"))
        self.assertFalse(build_release.is_prerelease("1.0.0"))
        with self.assertRaises(build_release.BuildRefused):
            build_release.tag_for("1.0")

    def test_this_checkout_has_notes_for_its_own_version(self):
        self.assertEqual(build_release.read_version(), src.__version__)
        tag = build_release.tag_for(src.__version__)
        notes = os.path.join(REPO_ROOT, "docs", "releases", tag + ".md")
        self.assertTrue(os.path.isfile(notes), notes)
        with open(notes, encoding="utf-8") as handle:
            text = handle.read()
        self.assertIn(tag, text)
        self.assertIn(src.__version__, text)

    def test_release_notes_list_every_postgresql_only_feature_key(self):
        """The notes promise the list comes from the code; hold them to it."""
        from src.db_backend import POSTGRESQL_ONLY_FEATURES
        notes = os.path.join(
            REPO_ROOT, "docs", "releases",
            build_release.tag_for(src.__version__) + ".md")
        with open(notes, encoding="utf-8") as handle:
            section = handle.read().split("PostgreSQL-only features", 1)[1]
        bullets = re.findall(r"^  - ", section.split("\n- **", 1)[0], re.M)
        self.assertEqual(len(bullets), len(POSTGRESQL_ONLY_FEATURES))

    def test_dist_is_gitignored(self):
        with open(os.path.join(REPO_ROOT, ".gitignore"),
                  encoding="utf-8") as handle:
            self.assertIn("dist/", handle.read().split())


class InstallSmokeOffline(unittest.TestCase):
    """The parts of the smoke test that need no network and no server."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="lto_smoke_unit_")
        self.addCleanup(shutil.rmtree, self.tmp, ignore_errors=True)
        self.tree = os.path.join(self.tmp, "tree")
        os.makedirs(self.tree)
        shutil.copy(os.path.join(REPO_ROOT, "config.example.ini"), self.tree)
        self.data = os.path.join(self.tmp, "data")

    def _config(self, backend, pg=None):
        paths, sqlite_file = install_smoke_test._write_config(
            self.tree, self.data, backend, pg)
        return paths, sqlite_file, os.path.join(self.tree, "config.ini")

    def test_generated_sqlite_config_passes_the_strict_parser(self):
        paths, sqlite_file, config_path = self._config("sqlite")
        with mock.patch("builtins.print"):
            cfg = ConfigManager(config_path)
        self.assertEqual(cfg.warnings, [])
        self.assertEqual(cfg.db_backend, "sqlite")
        self.assertEqual(os.path.normcase(cfg.sqlite_path),
                         os.path.normcase(sqlite_file))
        root = os.path.normcase(os.path.realpath(self.data))
        for value in (cfg.source_dir, cfg.staging_dir, cfg.restore_dir,
                      cfg.backup_log_dir, cfg.lto_drive, cfg.sqlite_path,
                      cfg.local_manifest_archive_root):
            inside = os.path.normcase(os.path.realpath(value))
            self.assertTrue(inside.startswith(root + os.sep), value)
        self.assertFalse(cfg.eject_after_session)
        self.assertFalse(os.path.exists(sqlite_file))

    def test_generated_postgresql_config_names_only_the_given_server(self):
        _paths, _sqlite, config_path = self._config("postgresql", {
            "host": "127.0.0.1", "port": 15432,
            "dbname": "ltotest_unit_x", "user": "lto"})
        parser = configparser.ConfigParser(interpolation=None)
        parser.read(config_path, encoding="utf-8")
        self.assertEqual(parser["DATABASE"]["backend"], "postgresql")
        self.assertEqual(parser["DATABASE"]["port"], "15432")
        self.assertEqual(parser["DATABASE"]["dbname"], "ltotest_unit_x")
        self.assertNotIn("password", parser["DATABASE"])

    def test_a_path_left_outside_the_work_directory_is_refused(self):
        with open(os.path.join(self.tree, "config.example.ini"), "a",
                  encoding="utf-8") as handle:
            handle.write("\n[EXTRA]\nstray_dir = C:\\somewhere\\else\n")
        with self.assertRaisesRegex(install_smoke_test.SmokeFailure,
                                    "points outside"):
            self._config("sqlite")

    def test_port_5432_is_refused_before_anything_runs(self):
        for dsn in ("postgresql://lto@127.0.0.1:5432/postgres",
                    "host=127.0.0.1 port=5432 dbname=postgres user=lto"):
            with self.assertRaisesRegex(install_smoke_test.SmokeFailure,
                                        "5432"):
                install_smoke_test.refuse_obviously_unsafe_dsn(dsn)

    def test_missing_port_and_remote_host_are_refused(self):
        with self.assertRaisesRegex(install_smoke_test.SmokeFailure,
                                    "no port"):
            install_smoke_test.refuse_obviously_unsafe_dsn(
                "postgresql://lto@127.0.0.1/postgres")
        with self.assertRaisesRegex(install_smoke_test.SmokeFailure,
                                    "loopback"):
            install_smoke_test.refuse_obviously_unsafe_dsn(
                "postgresql://lto@db.example.invalid:15432/postgres")
        install_smoke_test.refuse_obviously_unsafe_dsn(
            "host=127.0.0.1 port=15432 dbname=postgres user=lto")

    def test_refusals_happen_before_git_venv_or_network(self):
        """An unsafe or missing DSN ends the run with no side effect."""
        with mock.patch.object(install_smoke_test, "export_head") as export, \
                mock.patch.object(install_smoke_test, "create_venv") as venv, \
                mock.patch.dict(os.environ, {}, clear=False), \
                mock.patch("sys.stderr"):
            os.environ.pop(install_smoke_test.PG_DSN_ENV, None)
            self.assertEqual(install_smoke_test.main(
                ["--backend", "postgresql"]), 2)
            self.assertEqual(install_smoke_test.main(
                ["--backend", "postgresql", "--pg-dsn",
                 "postgresql://lto@127.0.0.1:5432/postgres"]), 1)
        export.assert_not_called()
        venv.assert_not_called()

    def test_child_environment_drops_ambient_database_settings(self):
        ambient = {"PGHOST": "prod.example.invalid", "PGPORT": "5432",
                   "PGPASSWORD": "x", "PGSERVICE": "prod", "PYTHONPATH": "x",
                   install_smoke_test.PG_DSN_ENV: "ambient"}
        with mock.patch.dict(os.environ, ambient):
            env = install_smoke_test._clean_env(
                {install_smoke_test.PG_DSN_ENV: "explicit"})
        for name in ("PGHOST", "PGPORT", "PGPASSWORD", "PGSERVICE",
                     "PYTHONPATH"):
            self.assertNotIn(name, env)
        self.assertEqual(env[install_smoke_test.PG_DSN_ENV], "explicit")

    def test_each_backend_installs_its_own_requirements_file(self):
        self.assertEqual(install_smoke_test.REQUIREMENT_FILES, {
            "sqlite": "requirements-core.txt",
            "postgresql": "requirements-postgres.txt"})


class WorkflowShape(unittest.TestCase):
    """Text-level facts about the workflows. They cannot be executed here;
    these tests only stop the load-bearing lines from being lost."""

    def _read(self, name):
        with open(os.path.join(WORKFLOWS, name), encoding="utf-8") as handle:
            return handle.read()

    def test_release_only_ever_creates_a_draft_prerelease(self):
        text = self._read("release.yml")
        creates = [line for line in text.splitlines()
                   if "gh release" in line and not line.lstrip().startswith("#")]
        self.assertEqual(len(creates), 1, creates)
        self.assertIn("gh release create", creates[0])
        self.assertIn("--draft", text)
        self.assertIn("--prerelease", text)
        self.assertIn("--verify-tag", text)
        for forbidden in ("gh release edit", "--draft=false", "git tag",
                          "git push", "--latest"):
            self.assertNotIn(forbidden, text)

    def test_release_triggers_are_tag_push_and_manual_only(self):
        text = self._read("release.yml")
        block = text.split("\non:\n", 1)[1].split("\npermissions:", 1)[0]
        self.assertEqual(
            [line.strip() for line in block.splitlines() if line.strip()],
            ["push:", "tags: ['v*']", "workflow_dispatch:"])

    def test_write_permission_exists_in_exactly_one_job(self):
        release = self._read("release.yml")
        self.assertEqual(release.count("contents: write"), 1)
        self.assertLess(release.index("draft-release:"),
                        release.index("contents: write"))
        self.assertNotIn("contents: write",
                         self._read("public-repo-guard.yml"))

    def test_release_runs_the_checks_before_building(self):
        release = self._read("release.yml")
        self.assertIn("uses: ./.github/workflows/public-repo-guard.yml",
                      release)
        self.assertIn("needs: checks", release)
        self.assertIn("scripts/build_release.py --expect-tag", release)
        self.assertIn("workflow_call:", self._read("public-repo-guard.yml"))

    def test_ci_never_names_the_standard_postgresql_port(self):
        for name in os.listdir(WORKFLOWS):
            text = self._read(name)
            code = "\n".join(line for line in text.splitlines()
                             if not line.lstrip().startswith("#"))
            self.assertNotIn("5432:", code, name)
            self.assertIsNone(re.search(r"(?<!1)5432", code), name)

    def test_ci_runs_both_install_smoke_tests_and_the_postgres_suites(self):
        text = self._read("public-repo-guard.yml")
        self.assertIn("backend: [sqlite, postgresql]", text)
        self.assertIn("scripts/install_smoke_test.py --backend", text)
        self.assertIn("LTO_PG_SEALED_BATCH_IT: '1'", text)
        self.assertIn("LTO_TEST_PG_DSN=", text)
        self.assertIn("python-version: ['3.11', '3.13', '3.14']", text)
        self.assertIn("python scripts/check_public_repo.py", text)


if __name__ == "__main__":
    unittest.main()
