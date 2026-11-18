"""Clean-install smoke test: prove a fresh install of one backend works.

    python scripts/install_smoke_test.py --backend sqlite
    python scripts/install_smoke_test.py --backend postgresql --pg-dsn <DSN>

What it does, entirely inside one temporary directory that is removed
afterwards:

1. Exports the committed source tree with ``git archive HEAD``. Untracked
   local files - the operator's real ``config.ini``, ``.env``, databases,
   logs - are therefore never part of the test, and neither is uncommitted
   work: this tests what a release built from HEAD would ship.
2. Creates a brand-new virtual environment and installs exactly one
   dependency set into it: ``requirements-core.txt`` for ``sqlite``,
   ``requirements-postgres.txt`` for ``postgresql``.
3. Inside that environment, from the exported tree:
   * ``sqlite``: checks psycopg is NOT installed; writes a ``config.ini``
     from ``config.example.ini`` with every path pointing into the temporary
     directory; starts the program non-interactively and checks that this
     first run created the SQLite catalog file and its schema; runs a small
     archive -> catalog -> restore round trip on synthetic files and compares
     every restored file byte for byte; takes and verifies a catalog backup.
   * ``postgresql``: the same first run and round trip against a database it
     creates for itself on the given server and drops afterwards. The server
     must be disposable: the rules of ``tests/pg_test_guard.py`` apply
     unchanged (loopback only, never port 5432, never a server that hosts an
     ``lto_archive`` database).

No tape is involved. A temporary directory stands in for the cartridge and an
in-process copier stands in for ``robocopy.exe`` (the approach of
``tests/test_sqlite_end_to_end.py``); the two hardware questions - "is the
drive ready" and "which cartridge is mounted" - are answered by the test.
This proves the install and the catalog, not tape behaviour.

Exit code 0 = passed, 1 = a check failed, 2 = the environment could not be
prepared (for example pip could not reach the package index).
"""
import argparse
import hashlib
import os
import shutil
import subprocess
import sys
import tempfile
import zipfile

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

BACKENDS = ("sqlite", "postgresql")
REQUIREMENT_FILES = {
    "sqlite": "requirements-core.txt",
    "postgresql": "requirements-postgres.txt",
}
#: The same variable the test suite uses, so CI can hand the DSN over without
#: putting a password on a command line.
PG_DSN_ENV = "LTO_TEST_PG_DSN"

#: Never inherited by the child processes: an ambient value could point a
#: connection somewhere nobody validated, or make the fresh environment import
#: packages from outside itself.
_SCRUBBED_ENV = (
    "PYTHONPATH", "PYTHONHOME", "PYTHONSTARTUP", "VIRTUAL_ENV",
    "PGHOST", "PGHOSTADDR", "PGPORT", "PGDATABASE", "PGUSER", "PGPASSWORD",
    "PGSERVICE", "PGSERVICEFILE", "PGPASSFILE", "PGOPTIONS",
    "REMOTE_PASSWORD", PG_DSN_ENV,
)

#: Local files that must never appear in an exported tree.
_PRIVATE_NAMES = ("config.ini", ".env")
_PRIVATE_SUFFIXES = (".db", ".sqlite", ".sqlite3", ".dump", ".log")

TAPE_LABEL = "SMOKE_T1"


class SmokeFailure(Exception):
    """A check failed. The message is the whole report."""


class EnvironmentFailure(Exception):
    """The test could not be set up (network, missing tool)."""


def say(message):
    print(f"[SMOKE] {message}", flush=True)


def check(condition, message):
    if not condition:
        raise SmokeFailure(message)


# ---------------------------------------------------------------------------
# Outer part: runs on any Python, standard library only
# ---------------------------------------------------------------------------

def _run(cmd, **kwargs):
    return subprocess.run(cmd, capture_output=True, text=True,
                          encoding="utf-8", errors="replace", **kwargs)


def _clean_env(extra=None):
    env = {key: value for key, value in os.environ.items()
           if key.upper() not in _SCRUBBED_ENV}
    env["PYTHONUTF8"] = "1"
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env.update(extra or {})
    return env


def _tail(text, lines=25):
    return "\n".join((text or "").strip().splitlines()[-lines:])


def export_head(repo, work):
    """``git archive HEAD`` -> ``<work>/tree``. Returns (tree, commit)."""
    probe = _run(["git", "-C", repo, "rev-parse", "HEAD"])
    if probe.returncode != 0:
        raise EnvironmentFailure(
            f"{repo} is not a git checkout (git rev-parse HEAD failed): "
            + _tail(probe.stderr, 3))
    commit = probe.stdout.strip()
    archive = os.path.join(work, "source.zip")
    # core.autocrlf=false: the export is byte-identical to what is committed,
    # whatever line-ending conversion this checkout uses.
    made = _run(["git", "-C", repo, "-c", "core.autocrlf=false", "archive",
                 "--format=zip", "-o", archive, "HEAD"])
    if made.returncode != 0:
        raise EnvironmentFailure("git archive HEAD failed: "
                                 + _tail(made.stderr, 5))
    tree = os.path.join(work, "tree")
    with zipfile.ZipFile(archive) as bundle:
        bundle.extractall(tree)
    os.remove(archive)
    leaked = []
    for current, _dirs, names in os.walk(tree):
        for name in names:
            if (name.lower() in _PRIVATE_NAMES
                    or name.lower().endswith(_PRIVATE_SUFFIXES)):
                leaked.append(os.path.relpath(os.path.join(current, name),
                                              tree))
    check(not leaked,
          "the exported tree contains local/private files, which means they "
          f"are tracked by git: {sorted(leaked)}")
    return tree, commit


def create_venv(python, work):
    venv_dir = os.path.join(work, "venv")
    made = _run([python, "-m", "venv", venv_dir], timeout=600)
    if made.returncode != 0:
        raise EnvironmentFailure(
            f"could not create a virtual environment with {python}:\n"
            + _tail(made.stdout + made.stderr))
    sub = "Scripts" if os.name == "nt" else "bin"
    exe = os.path.join(venv_dir, sub,
                       "python.exe" if os.name == "nt" else "python")
    if not os.path.isfile(exe):
        raise EnvironmentFailure(f"the new environment has no interpreter "
                                 f"at {exe}")
    return exe


def pip_install(venv_python, requirements):
    cmd = [venv_python, "-m", "pip", "install", "--disable-pip-version-check",
           "--no-input", "-r", requirements]
    done = _run(cmd, env=_clean_env(), timeout=1800)
    if done.returncode != 0:
        raise EnvironmentFailure(
            f"pip could not install {os.path.basename(requirements)} "
            f"(exit {done.returncode}). If the lines below show a connection "
            "or name-resolution error, pip could not reach the package index "
            "(PyPI): this test needs network access for that one step.\n"
            + _tail(done.stdout + "\n" + done.stderr))
    listed = _run([venv_python, "-m", "pip", "list", "--format=freeze",
                   "--disable-pip-version-check"], env=_clean_env())
    return sorted(line.strip() for line in listed.stdout.splitlines()
                  if line.strip() and not line.lower().startswith("pip=="))


def dsn_host_port(dsn):
    """Best-effort ``(host, port)`` of a DSN, standard library only.

    Used to refuse an obviously wrong server before any time is spent and
    before anything connects. It is NOT the safety check: the authoritative,
    fail-closed validation is ``tests/pg_test_guard.py``, run with libpq's own
    parser inside the fresh environment.
    """
    text = dsn.strip()
    if "://" in text:
        from urllib.parse import urlsplit
        try:
            parts = urlsplit(text)
            return parts.hostname or "", str(parts.port or "")
        except ValueError:
            return "", ""
    found = {}
    for item in text.split():
        key, _sep, value = item.partition("=")
        found[key.strip().lower()] = value.strip().strip("'")
    return found.get("host", ""), found.get("port", "")


def refuse_obviously_unsafe_dsn(dsn):
    host, port = dsn_host_port(dsn)
    check(port != "5432",
          "refusing this PostgreSQL server: port 5432 is the standard port, "
          "where a real catalog would listen. Start a throwaway server on "
          "another port (see tests/pg_test_guard.py).")
    check(port != "",
          "refusing this PostgreSQL server: the DSN names no port, and the "
          "implicit default is 5432.")
    check(host in ("127.0.0.1", "::1", "localhost"),
          f"refusing this PostgreSQL server: host {host!r} is not a loopback "
          "address, so it cannot be shown to be disposable.")


def outer(args):
    repo = os.path.abspath(args.repo)
    python = args.python or sys.executable
    dsn = args.pg_dsn or os.environ.get(PG_DSN_ENV) or ""
    if args.backend == "postgresql":
        if not dsn.strip():
            raise EnvironmentFailure(
                "--backend postgresql needs a disposable PostgreSQL server: "
                f"pass --pg-dsn or set {PG_DSN_ENV} (see "
                "tests/pg_test_guard.py for the rules and a docker command).")
        refuse_obviously_unsafe_dsn(dsn)

    # The operator's own files in the checkout must come out untouched.
    watched = [os.path.join(repo, name) for name in ("config.ini", ".env")]
    before = {path: (os.path.getmtime(path), os.path.getsize(path))
              if os.path.exists(path) else None for path in watched}

    work = tempfile.mkdtemp(prefix="owc_install_smoke_")
    say(f"backend            : {args.backend}")
    say(f"work directory     : {work}")
    try:
        tree, commit = export_head(repo, work)
        say(f"source             : git archive of {commit[:12]} "
            "(tracked files only)")
        requirements = os.path.join(tree, REQUIREMENT_FILES[args.backend])
        check(os.path.isfile(requirements),
              f"{REQUIREMENT_FILES[args.backend]} is not in the committed "
              "tree")

        base = _run([python, "-c", "import sys; print(sys.version.split()[0])"])
        if base.returncode != 0:
            raise EnvironmentFailure(f"cannot run the interpreter {python}")
        say(f"base interpreter   : Python {base.stdout.strip()} ({python})")
        venv_python = create_venv(python, work)
        say("virtual environment: created (empty)")
        installed = pip_install(venv_python, requirements)
        say(f"installed from {REQUIREMENT_FILES[args.backend]}: "
            + ", ".join(installed))

        # The driver is this file, copied next to (not into) the tree so the
        # run does not depend on the script being part of the commit.
        driver = os.path.join(work, "smoke_driver.py")
        shutil.copyfile(os.path.abspath(__file__), driver)
        extra = {PG_DSN_ENV: dsn} if args.backend == "postgresql" else {}
        inner_run = subprocess.run(
            [venv_python, driver, "--inner", "--backend", args.backend,
             "--tree", tree, "--data", os.path.join(work, "data")],
            cwd=tree, env=_clean_env(extra), timeout=1800)
        check(inner_run.returncode == 0,
              f"the checks inside the fresh environment failed "
              f"(exit {inner_run.returncode}); see the output above")

        for path, state in before.items():
            now = ((os.path.getmtime(path), os.path.getsize(path))
                   if os.path.exists(path) else None)
            check(now == state,
                  f"{path} was created or changed by the smoke test")
        say(f"PASSED: clean install with the {args.backend} backend works "
            f"(commit {commit[:12]}, Python {base.stdout.strip()})")
    finally:
        if args.keep:
            say(f"kept for inspection: {work}")
        else:
            shutil.rmtree(work, ignore_errors=True)


# ---------------------------------------------------------------------------
# Inner part: runs inside the fresh environment, cwd = the exported tree
# ---------------------------------------------------------------------------

def _module_missing(name):
    import importlib.util
    try:
        return importlib.util.find_spec(name) is None
    except (ImportError, ValueError):
        return True


def _check_environment(backend, tree):
    check(sys.prefix != sys.base_prefix,
          "the inner checks are not running inside a virtual environment")
    for name in ("psutil", "zstandard"):
        check(not _module_missing(name),
              f"{name} is missing from the fresh environment")
    check(_module_missing("PySide6"),
          "PySide6 is installed, but neither the core nor the PostgreSQL "
          "dependency set asks for it")
    if backend == "sqlite":
        for name in ("psycopg", "psycopg_pool", "psycopg_binary"):
            check(_module_missing(name),
                  f"{name} is installed in the SQLite-only environment: the "
                  "core dependency set must not pull in the PostgreSQL driver")
        say("psycopg            : not installed (as required for SQLite)")
    else:
        for name in ("psycopg", "psycopg_pool"):
            check(not _module_missing(name),
                  f"{name} is missing after installing the PostgreSQL set")
        import psycopg
        say(f"psycopg            : {psycopg.__version__}")
    import src
    origin = os.path.normcase(os.path.realpath(os.path.dirname(src.__file__)))
    expected = os.path.normcase(os.path.realpath(os.path.join(tree, "src")))
    check(origin == expected,
          f"the program was imported from {origin}, not from the exported "
          f"tree {expected}")
    version = subprocess.run(
        [sys.executable, "run.py", "--version"], cwd=tree,
        capture_output=True, text=True, timeout=120)
    check(version.returncode == 0 and version.stdout.strip()
          == f"tape-ledger {src.__version__}",
          f"run.py --version failed: {version.stdout!r} {version.stderr!r}")
    say(f"program version    : {src.__version__}")
    return src.__version__


def _write_config(tree, data, backend, pg=None):
    """config.ini from config.example.ini, every path inside ``data``."""
    import configparser
    import uuid

    paths = {name: os.path.join(data, name) for name in (
        "source", "staging", "restored", "logs", "tape", "manifests",
        "observations", "catalog")}
    for path in paths.values():
        os.makedirs(path, exist_ok=True)
    parser = configparser.ConfigParser(interpolation=None)
    with open(os.path.join(tree, "config.example.ini"),
              encoding="utf-8") as handle:
        parser.read_file(handle)

    parser["PATHS"]["source_dir"] = paths["source"]
    parser["PATHS"]["staging_dir"] = paths["staging"]
    parser["PATHS"]["restore_dir"] = paths["restored"]
    parser["PATHS"]["backup_log_dir"] = paths["logs"]
    parser["HARDWARE"]["lto_drive"] = paths["tape"]
    parser["HARDWARE"]["ltfs_ownership_id"] = "smoke_" + uuid.uuid4().hex[:12]
    parser["HARDWARE"]["ibm_eject_cmd"] = os.path.join(
        data, "no_such_eject_command.exe")
    parser["HARDWARE"]["eject_after_session"] = "false"
    parser["LOCAL_MANIFEST_ARCHIVE"]["root"] = paths["manifests"]
    parser["OBSERVATION"]["sealed_batch_observation_log_path"] = os.path.join(
        paths["observations"], "sealed_batch_observations.jsonl")
    parser["TELEGRAM"]["enabled"] = "false"
    parser["WINDOWS_UPDATE"]["guard"] = "false"
    parser["DATABASE"]["backend"] = backend
    sqlite_file = os.path.join(paths["catalog"], "lto_archive.sqlite3")
    parser["DATABASE"]["sqlite_path"] = sqlite_file
    if pg:
        parser["DATABASE"]["host"] = pg["host"]
        parser["DATABASE"]["port"] = str(pg["port"])
        parser["DATABASE"]["dbname"] = pg["dbname"]
        parser["DATABASE"]["user"] = pg["user"]

    # "Every path points at the temp dir" is checked, not assumed: any value
    # that is an absolute Windows or POSIX path must live under ``data``.
    root = os.path.normcase(os.path.realpath(data))
    for section in parser.sections():
        for key, value in parser[section].items():
            text = value.strip().strip('"')
            if "\n" in text or key in ("remote_path", "remote_selected_paths"):
                continue                       # paths on the REMOTE host
            looks_absolute = (len(text) > 2 and text[1] == ":"
                              and text[2] in "\\/") or text.startswith("/")
            if not looks_absolute:
                continue
            inside = os.path.normcase(os.path.realpath(text))
            check(inside == root or inside.startswith(root + os.sep),
                  f"config [{section}] {key} = {value!r} points outside the "
                  "temporary directory")

    target = os.path.join(tree, "config.ini")
    check(not os.path.exists(target),
          "the exported tree already contains a config.ini")
    with open(target, "w", encoding="utf-8") as handle:
        parser.write(handle)
    return paths, sqlite_file


def _first_run(tree, backend, sqlite_file, pg_conninfo):
    """Start the program with no terminal; the catalog must appear."""
    if backend == "sqlite":
        check(not os.path.exists(sqlite_file),
              "the SQLite catalog already exists before the first run")
    # Menu 4 (list registered tapes: a catalog read) and 0 (exit). Neither
    # touches a drive, and stdin is a pipe, so nothing can wait for a human.
    started = subprocess.run(
        [sys.executable, "run.py"], cwd=tree, input="4\n0\n",
        capture_output=True, text=True, encoding="utf-8", errors="replace",
        timeout=600)
    check(started.returncode == 0,
          f"the first run exited {started.returncode}:\n"
          + _tail(started.stdout + "\n" + started.stderr, 40))
    check("Goodbye." in started.stdout,
          "the first run did not reach the menu and exit normally:\n"
          + _tail(started.stdout, 20))
    wanted = {"tapes", "files_index", "local_sessions", "remote_sessions",
              "remote_chunks"}
    if backend == "sqlite":
        wanted.add("schema_migrations")
        import sqlite3
        check(os.path.isfile(sqlite_file),
              f"the first run did not create the SQLite catalog {sqlite_file}")
        uri = "file:" + sqlite_file.replace("\\", "/") + "?mode=ro"
        conn = sqlite3.connect(uri, uri=True)
        try:
            tables = {row[0] for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
            triggers = conn.execute(
                "SELECT count(*) FROM sqlite_master WHERE type='trigger'"
            ).fetchone()[0]
            applied = conn.execute(
                "SELECT count(*) FROM schema_migrations").fetchone()[0]
        finally:
            conn.close()
        check(applied >= 1, "schema_migrations is empty after the first run")
        detail = (f"{len(tables)} tables, {triggers} guard triggers, "
                  f"{applied} recorded schema file(s)")
    else:
        import psycopg
        with psycopg.connect(pg_conninfo, autocommit=True,
                             connect_timeout=10) as conn:
            tables = {row[0] for row in conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'public'")}
        detail = f"{len(tables)} tables"
    missing = sorted(wanted - tables)
    check(not missing,
          f"the first run left the catalog without table(s) {missing}")
    say(f"first run          : exit 0, catalog and schema created ({detail})")


def _check_complete_postgres_schema():
    """The first run on an EMPTY PostgreSQL database installs everything.

    Start-up completes the schema (the directory catalog and migrations 014
    to 019) when - and only when - the database held no catalog before, so a
    new install can archive straight away. This test applies nothing itself:
    it only checks what the first run left behind.
    """
    from src.config import ConfigManager
    from src.db import create_database_manager

    db = create_database_manager(ConfigManager())
    try:
        check(db.directory_catalog_schema_installed(),
              "the first run on an empty PostgreSQL database did not install "
              "the directory catalog schema")
        check(db.incremental_scan_schema_finalized(),
              "the first run did not install the incremental-scan schema")
        check(db.container_format_schema_authority_v2_report()["ready"],
              "the first run did not install the container-format schema")
        check(db.fresh_schema_installed == (),
              "a second start-up installed schema again: "
              f"{db.fresh_schema_installed}")
    finally:
        db.close()
    say("complete schema    : installed by the first run on the empty "
        "database (nothing applied by this test)")


def _synthetic_files():
    """Small files (packed into a ZIP bundle) and files above the 1 MiB pack
    threshold (written loose), in two top-level folders."""
    return {
        os.path.join("project_a", "notes.txt"): b"alpha\n" * 50,
        os.path.join("project_a", "sub", "data.csv"):
            b"id,value\n" + b"1,2\n" * 300,
        os.path.join("project_a", "sub", "empty.bin"): b"",
        os.path.join("project_a", "unicode_שלום.txt"):
            "shalom שלום".encode("utf-8"),
        os.path.join("project_a", "big", "volume.raw"):
            bytes(range(256)) * 5000,
        os.path.join("project_b", "model.bin"):
            hashlib.sha256(b"seed").digest() * 40000,
        os.path.join("project_b", "readme.md"): b"# readme\n",
    }


def _robocopy_stand_in(cmd, priority=None, affinity=None, on_start=None,
                       raw_sink=None):
    """``robocopy <src> <dst> /E ...`` in process, reporting in robocopy's
    own summary format so the production result classifier still decides
    whether the write succeeded. No robocopy process is started."""
    from types import SimpleNamespace

    source, target = str(cmd[1]), str(cmd[2])
    files = dirs = copied = 0
    for current, _subdirs, names in os.walk(source):
        relative = os.path.relpath(current, source)
        destination = os.path.normpath(os.path.join(target, relative))
        os.makedirs(destination, exist_ok=True)
        dirs += 1
        for name in names:
            shutil.copy2(os.path.join(current, name),
                         os.path.join(destination, name))
            files += 1
            copied += os.path.getsize(os.path.join(current, name))
    report = "\n".join((
        "", "               Total    Copied   Skipped  Mismatch    FAILED    "
        "Extras",
        f"    Dirs : {dirs:9d} {dirs:9d}         0         0         0"
        "         0",
        f"   Files : {files:9d} {files:9d}         0         0         0"
        "         0",
        f"   Bytes : {copied:9d} {copied:9d}         0         0"
        "         0         0",
        "   Times :   0:00:01   0:00:01                       0:00:00   "
        "0:00:00",
        "   Speed :            1000000 Bytes/sec.", ""))
    if raw_sink is not None:
        raw_sink.write(report)
    return SimpleNamespace(stdout=report, stderr="",
                           returncode=1 if files else 0)


def _copy_file_stand_in(src_path, dst_path, display_name=None):
    """``robocopy`` for one file: it lands at dirname(dst)/basename(src)."""
    target_dir = os.path.dirname(os.path.abspath(dst_path))
    os.makedirs(target_dir, exist_ok=True)
    shutil.copy2(src_path, os.path.join(target_dir,
                                        os.path.basename(src_path)))
    return True


def _digest(path):
    sha = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(65536), b""):
            sha.update(block)
    return sha.hexdigest()


def _round_trip(paths, data):
    """archive -> catalog -> restore with the real pipeline classes."""
    from unittest import mock

    from src.backup import _NoEjectBackup
    from src.config import ConfigManager
    from src.db import create_database_manager
    from src.packer import LTOAnalyzer, LTOPacker
    from src.retriever import LTORetriever
    from src.runtime import CANCEL

    CANCEL.clear()
    cfg = ConfigManager()
    for key, value in (("source", cfg.source_dir), ("staging", cfg.staging_dir),
                       ("restored", cfg.restore_dir), ("tape", cfg.lto_drive)):
        check(os.path.normcase(os.path.normpath(value))
              == os.path.normcase(os.path.normpath(paths[key])),
              f"the program resolved the {key} path to {value!r}, not to the "
              f"temporary directory {paths[key]!r}")

    files = _synthetic_files()
    for rel, content in files.items():
        path = os.path.join(paths["source"], rel)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as handle:
            handle.write(content)

    def chunk_files(entries):
        collected = []
        for entry in entries:
            root = os.path.join(paths["source"], entry["top_level_dir"])
            for current, _dirs, names in os.walk(root):
                for name in names:
                    path = os.path.join(current, name)
                    collected.append({
                        "path": path,
                        "rel": os.path.relpath(path, paths["source"]),
                        "size": os.path.getsize(path)})
        return sorted(collected, key=lambda item: item["rel"].lower())

    db = create_database_manager(cfg)
    try:
        db.register_tape(TAPE_LABEL, 12000)
        db.acquire_archiver_lock()
        try:
            chunks = LTOAnalyzer().build_local_allocation_plan(
                paths["source"], tape_profile=cfg.tape_profile,
                first_tape_budget_bytes=10 * 1024 ** 4)
            session_id = db.create_local_session(
                "LOCAL_install_smoke", paths["source"], chunks,
                backup_mode="pack")
            session = db.get_local_session(session_id)
            pending = db.get_local_pending_chunks(session_id)
            check(pending, "the allocation plan produced no chunks")
            for chunk_index in pending:
                db.verify_archiver_lock()
                entries = db.get_local_chunk_entries(session_id, chunk_index)
                db.assign_local_chunk_tape(session_id, chunk_index, TAPE_LABEL)
                db.update_local_chunk_status(session_id, chunk_index, "staged")
                pack_dir = os.path.join(
                    paths["staging"],
                    f"_local_s{session_id:04d}_c{chunk_index:03d}")
                metadata = LTOPacker(
                    1, index_min_file_mb=0, index_packed_small_files=True,
                ).run_manifest(
                    source_root=paths["source"], dest=pack_dir,
                    threshold_mb=1, file_entries=chunk_files(entries),
                    bundle_prefix=(f"Bundle_s{session_id:04d}"
                                   f"_c{chunk_index:03d}"),
                    source_name="local", session_id=session_id,
                    chunk_index=chunk_index)
                with mock.patch("src.backup._ensure_lto_drive_ready",
                                return_value=True), \
                        mock.patch("src.backup._run_robocopy_tuned",
                                   _robocopy_stand_in):
                    _NoEjectBackup(
                        db, "", log_dir=paths["logs"], index_min_file_mb=0,
                        index_packed_small_files=True,
                    ).run(source=pack_dir, tape_drive=paths["tape"],
                          tape_label=TAPE_LABEL, packer_metadata=metadata,
                          local_session_id=session_id,
                          local_chunk_index=chunk_index,
                          tape_parent_dir=session["session_label"])
                shutil.rmtree(pack_dir, ignore_errors=True)
                db.update_local_chunk_status(session_id, chunk_index,
                                             "backed_up")
            check(db.get_local_pending_chunks(session_id) == [],
                  "chunks are still pending after the archive")
            db.update_local_session(session_id, status="completed")
        finally:
            db.release_archiver_lock()

        # -- catalog -------------------------------------------------------
        records = db.search_catalog(tape_label=TAPE_LABEL)
        check(len(records) == len(files),
              f"the catalog holds {len(records)} file records, expected "
              f"{len(files)}")
        by_source = {os.path.normcase(os.path.normpath(r["original_path"])): r
                     for r in records}
        for rel, content in files.items():
            key = os.path.normcase(os.path.normpath(
                os.path.join(paths["source"], rel)))
            check(key in by_source, f"{rel} is not in the catalog")
            check(by_source[key]["file_size_bytes"] == len(content),
                  f"{rel} is catalogued with the wrong size")
        packed = sum(1 for r in records if r["is_packed"])
        loose = len(records) - packed
        check(packed and loose,
              "expected both packed and loose files in the catalog, got "
              f"{packed} packed / {loose} loose")
        hit = db.search_catalog(name_query="volume.raw")
        check([r["file_name"] for r in hit] == ["volume.raw"],
              "catalog search by name did not find exactly volume.raw")
        check(db.count_tape_file_records(TAPE_LABEL) == len(files),
              "count_tape_file_records disagrees with the search")
        check(db.get_local_session(session_id)["status"] == "completed",
              "the local session is not marked completed")
        say(f"archive + catalog  : {len(files)} files catalogued "
            f"({packed} packed in a ZIP bundle, {loose} loose), search works")

        # -- restore -------------------------------------------------------
        drive = os.path.splitdrive(os.path.abspath(data))[0] + os.sep
        retriever = LTORetriever(
            db, tape_drive=drive,
            staging_dir=os.path.join(data, "restore_staging"),
            restore_dir=paths["restored"])
        with mock.patch("src.retriever.get_volume_label",
                        return_value=TAPE_LABEL), \
                mock.patch("src.retriever._robocopy_file",
                           _copy_file_stand_in):
            outcome = retriever._restore_many(records)
        check(outcome.failed_count == 0,
              f"restore reported failures: {outcome.failures}")
        check(outcome.restored_count == len(files),
              f"restore reported {outcome.restored_count} files, expected "
              f"{len(files)}")
        restored = {}
        for current, _dirs, names in os.walk(paths["restored"]):
            for name in names:
                path = os.path.join(current, name)
                restored.setdefault(_digest(path), []).append(path)
        for rel, content in files.items():
            check(hashlib.sha256(content).hexdigest() in restored,
                  f"{rel} was not restored with identical content")
        total = sum(len(found) for found in restored.values())
        check(total == len(files),
              f"{total} files were restored, expected {len(files)}")
        say(f"restore            : {total} files restored, every one "
            "byte-identical to its source")
    finally:
        db.close()
    return cfg


def _set_tape_drive(tree, value):
    """Rewrite ``[HARDWARE] lto_drive`` in the exported tree's config.ini."""
    import configparser

    target = os.path.join(tree, "config.ini")
    parser = configparser.ConfigParser(interpolation=None)
    parser.read(target, encoding="utf-8")
    parser["HARDWARE"]["lto_drive"] = value
    with open(target, "w", encoding="utf-8") as handle:
        parser.write(handle)


def _backup_command(tree):
    return subprocess.run(
        [sys.executable, "run.py", "--backup-db"], cwd=tree,
        stdin=subprocess.DEVNULL, capture_output=True, text=True,
        encoding="utf-8", errors="replace", timeout=600)


def _backup(tree):
    """Take and verify a SQLite catalog backup through the real command.

    The program refuses to put a catalog backup on the drive letter
    configured as the tape drive. Here the stand-in "tape" is a folder on the
    same disk as everything else, so the command must first REFUSE (non-zero
    exit, nothing written). An install with no tape drive configured - a
    blank ``lto_drive`` - has nothing to protect: there the same command must
    succeed and leave a verified copy.
    """
    from src.config import ConfigManager
    from src.db_backup import verify_backup

    target = os.path.join(tree, "db_backups")
    refused = _backup_command(tree)
    check(refused.returncode != 0 and "LTFS drive" in refused.stdout,
          "run.py --backup-db was expected to refuse a backup on the tape "
          f"drive's letter, but exited {refused.returncode}:\n"
          + _tail(refused.stdout + "\n" + refused.stderr, 30))
    check(not os.path.isdir(target) or not os.listdir(target),
          "a refused backup still left files in db_backups")
    say("backup guard       : run.py --backup-db refused to write onto the "
        f"configured tape drive letter (exit {refused.returncode})")

    _set_tape_drive(tree, "")
    done = _backup_command(tree)
    check(done.returncode == 0,
          "run.py --backup-db failed with no tape drive configured "
          f"(exit {done.returncode}):\n"
          + _tail(done.stdout + "\n" + done.stderr, 30))
    backups = [name for name in (os.listdir(target)
                                 if os.path.isdir(target) else [])
               if name.endswith(".sqlite3")]
    check(len(backups) == 1,
          f"expected exactly one backup file in db_backups, found {backups}")
    path = os.path.join(target, backups[0])
    check(os.path.basename(path) in done.stdout,
          "run.py --backup-db did not name the backup it created:\n"
          + _tail(done.stdout, 15))
    result = verify_backup(ConfigManager(), path)
    check(result.ok, "the catalog backup failed verification:\n"
          + result.summary())
    say(f"catalog backup     : run.py --backup-db wrote "
        f"{os.path.basename(path)} ({os.path.getsize(path)} bytes), "
        "integrity verified")


def inner(args):
    tree = os.path.abspath(args.tree)
    data = os.path.abspath(args.data)
    os.makedirs(data, exist_ok=True)
    os.chdir(tree)
    sys.path.insert(0, tree)
    _check_environment(args.backend, tree)

    pg, pg_conninfo, pg_name, guard = None, None, None, None
    if args.backend == "postgresql":
        sys.path.insert(0, os.path.join(tree, "tests"))
        import pg_test_guard as guard
        from psycopg.conninfo import conninfo_to_dict

        dsn = os.environ.get(PG_DSN_ENV, "")
        try:
            host, port, _dbname = guard.assert_safe_dsn(dsn)
            reachable = guard.pg_available()
        except guard.UnsafeTestDatabase as exc:
            raise SmokeFailure(f"refusing this PostgreSQL server: {exc}")
        check(reachable,
              f"cannot connect to the PostgreSQL server at {host}:{port}")
        say(f"PostgreSQL server  : {host}:{port} - loopback, not port 5432, "
            "hosts no lto_archive database")
        pg_name, pg_conninfo = guard.create_test_database("install_smoke")
        parsed = conninfo_to_dict(pg_conninfo)
        pg = {"host": parsed["host"], "port": int(parsed["port"]),
              "dbname": pg_name, "user": parsed.get("user") or ""}
        check(pg["user"], "the PostgreSQL DSN does not name a user")
        # The program reads the password from the environment (or .env),
        # never from config.ini.
        os.environ["PGPASSWORD"] = parsed.get("password") or ""
        say(f"test database      : created {pg_name}")
    try:
        paths, sqlite_file = _write_config(tree, data, args.backend, pg)
        say("config.ini         : written from config.example.ini, every "
            "path inside the work directory")
        _first_run(tree, args.backend, sqlite_file, pg_conninfo)
        if args.backend == "postgresql":
            _check_complete_postgres_schema()
        cfg = _round_trip(paths, data)
        check(cfg.db_backend == args.backend,
              f"the program used the {cfg.db_backend} backend")
        if args.backend == "sqlite":
            _backup(tree)
        else:
            check(not os.path.exists(sqlite_file),
                  "a SQLite catalog file appeared during a PostgreSQL run")
            say("catalog backup     : not run for PostgreSQL (it needs the "
                "pg_dump client program, which is not a Python dependency)")
    finally:
        if pg_name:
            guard.drop_test_database(pg_name)
            import psycopg
            with psycopg.connect(os.environ[PG_DSN_ENV], autocommit=True,
                                 connect_timeout=10) as conn:
                left = conn.execute(
                    "SELECT count(*) FROM pg_database WHERE datname = %s",
                    (pg_name,)).fetchone()[0]
            check(left == 0, f"the test database {pg_name} was not dropped")
            say(f"test database      : dropped {pg_name}")


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Prove that a clean install of one catalog backend "
                    "works: fresh virtualenv, committed source only, first "
                    "run, archive -> catalog -> restore round trip. No tape "
                    "is used.")
    parser.add_argument("--backend", required=True, choices=BACKENDS)
    parser.add_argument("--python", help="Interpreter the virtual environment "
                        "is created from (default: the one running this "
                        "script).")
    parser.add_argument("--pg-dsn", help="Disposable PostgreSQL server for "
                        f"--backend postgresql (default: ${PG_DSN_ENV}). "
                        "Loopback only, never port 5432.")
    parser.add_argument("--repo", default=REPO_ROOT,
                        help="Git checkout to export (default: this one).")
    parser.add_argument("--keep", action="store_true",
                        help="Keep the work directory for inspection.")
    parser.add_argument("--inner", action="store_true",
                        help=argparse.SUPPRESS)
    parser.add_argument("--tree", help=argparse.SUPPRESS)
    parser.add_argument("--data", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    try:
        if args.inner:
            inner(args)
        else:
            outer(args)
    except SmokeFailure as exc:
        print(f"[SMOKE] FAILED: {exc}", file=sys.stderr, flush=True)
        return 1
    except EnvironmentFailure as exc:
        print(f"[SMOKE] COULD NOT RUN: {exc}", file=sys.stderr, flush=True)
        return 2
    except subprocess.TimeoutExpired as exc:
        print(f"[SMOKE] FAILED: a step timed out: {exc}", file=sys.stderr,
              flush=True)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
