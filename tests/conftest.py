"""Fail-closed guard: the test suite must never touch the production LTFS mount.

Every test in this repository is mock-only by policy, but nothing enforced that
policy — and several tests configure ``lto_drive="Z:"``, the *same* letter as
production. A single un-mocked call in a test (or a future refactor that adds
one) would therefore reach the live cartridge.

This autouse guard closes that hole. It wraps the filesystem and subprocess
entry points and raises **before** any I/O is issued when the target resolves to
a forbidden drive (the configured ``[HARDWARE] lto_drive`` plus ``Z:`` always),
or when the command is an IBM LTFS helper. Anything pointing elsewhere — temp
dirs, the repo, fake drive letters — passes straight through untouched.

The guard is deliberately *not* configurable from a test: a test that wants to
exercise drive-shaped behaviour must use a temporary directory as its fake
drive, never a real letter.
"""
import configparser
import os
import shutil
import subprocess
import sys

import pytest

# ``pg_test_guard`` lives beside this file. pytest normally puts this directory
# on sys.path itself; make it certain, because the PostgreSQL guard below is
# useless if importing it can fail.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# TEST ISOLATION. Give every pytest PROCESS a unique LTFS ownership identity so
# two accidentally-parallel test runs never contend on the real Global mutex and
# hang for the writer timeout — the failure mode seen during Phase 4.5. This is
# an EXPLICIT, pytest-gated call (Phase 5B hardening): no environment variable
# can repoint the production ownership mutex on its own. Tests that must prove
# genuine cross-process contention still pass an explicit shared name and are
# unaffected. Optionally honours LTO_TEST_OWNERSHIP_ID for a deterministic id.
from src import ltfs_ownership as _ltfs_ownership
_ltfs_ownership.activate_test_ownership_isolation()

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# LTFS helper executables that must never run from a test, whatever their args.
FORBIDDEN_EXECUTABLES = (
    "ltfscmddrives", "ltfscmdeject", "ltfscmdformat", "ltfscmdload",
    "ltfscmdcheck", "ltfscmdrollback", "ltfscmdunformat", "ltfscmdassign",
    "ltfscmdunassign", "ltfsck", "mkltfs", "ltfsmain", "itdt", "itdt-ge",
    "itdt-gec",
)


class ProductionDriveAccessError(AssertionError):
    """Raised when a test attempts to reach the real tape mount."""


def _configured_drive_letters():
    """Forbidden drive letters: the configured mount plus 'Z' as a backstop."""
    letters = {"Z"}
    parser = configparser.ConfigParser()
    try:
        # utf-8: config.ini carries non-ASCII comment text.
        parser.read(os.path.join(PROJECT_ROOT, "config.ini"), encoding="utf-8")
        raw = parser.get("HARDWARE", "lto_drive", fallback="") or ""
    except (configparser.Error, OSError, UnicodeDecodeError):
        raw = ""
    drive = os.path.splitdrive(raw.strip().strip('"').replace("\\\\", "\\"))[0]
    if drive:
        letters.add(drive.rstrip(":").upper())
    return frozenset(letters)


FORBIDDEN_DRIVES = _configured_drive_letters()


def _drive_of(value):
    """Return the upper-case drive letter of ``value``, or None."""
    if isinstance(value, int):          # file descriptor, never a path
        return None
    try:
        text = os.fsdecode(value)
    except (TypeError, ValueError):
        return None
    drive = os.path.splitdrive(text)[0]
    return drive.rstrip(":").upper() if drive else None


def _reject_path(value, origin):
    drive = _drive_of(value)
    if drive and drive in FORBIDDEN_DRIVES:
        raise ProductionDriveAccessError(
            f"BLOCKED: {origin} attempted to access production tape drive "
            f"{drive}: via {value!r}. Tests must use a temporary directory as a "
            f"fake drive. Forbidden drives: {sorted(FORBIDDEN_DRIVES)}"
        )


def _reject_command(cmd, origin):
    parts = [cmd] if isinstance(cmd, (str, bytes, os.PathLike)) else list(cmd or ())
    for part in parts:
        try:
            text = os.fsdecode(part)
        except (TypeError, ValueError):
            continue
        _reject_path(text, origin)
        stem = os.path.splitext(os.path.basename(text))[0].lower()
        if stem in FORBIDDEN_EXECUTABLES:
            raise ProductionDriveAccessError(
                f"BLOCKED: {origin} attempted to run IBM LTFS helper "
                f"{stem!r}. Tests must inject a fake adapter instead."
            )
        # `cmd /c vol Z:` arrives as separate argv items; catch the bare form.
        bare = text.rstrip(":\\/").upper()
        if len(bare) == 1 and bare in FORBIDDEN_DRIVES:
            raise ProductionDriveAccessError(
                f"BLOCKED: {origin} referenced production drive {bare}: "
                f"in a subprocess argument."
            )


@pytest.fixture(autouse=True)
def block_production_drive(monkeypatch):
    """Autouse: fail closed on any access to the production tape mount."""

    def guard_path(func, name, argindex=0):
        def wrapper(*args, **kwargs):
            if len(args) > argindex:
                _reject_path(args[argindex], name)
            return func(*args, **kwargs)
        return wrapper

    def guard_cmd(func, name):
        def wrapper(*args, **kwargs):
            if args:
                _reject_command(args[0], name)
            return func(*args, **kwargs)
        return wrapper

    for module, attr in (
        (os, "listdir"), (os, "scandir"), (os, "stat"), (os, "lstat"),
        (os, "open"), (os, "mkdir"), (os, "makedirs"), (os, "remove"),
        (os, "rmdir"), (os, "unlink"), (os, "rename"), (os, "walk"),
        (os.path, "isdir"), (os.path, "isfile"), (os.path, "exists"),
        (os.path, "getsize"),
        (shutil, "disk_usage"), (shutil, "rmtree"), (shutil, "copy2"),
    ):
        original = getattr(module, attr)
        monkeypatch.setattr(
            module, attr, guard_path(original, f"{module.__name__}.{attr}"))

    import builtins
    monkeypatch.setattr(builtins, "open", guard_path(builtins.open, "open"))

    for attr in ("run", "Popen", "call", "check_call", "check_output"):
        original = getattr(subprocess, attr)
        monkeypatch.setattr(
            subprocess, attr, guard_cmd(original, f"subprocess.{attr}"))

    yield


# ===========================================================================
# The same idea, for PostgreSQL: no test may open a connection to a server
# that has not been proven disposable.
#
# The individual PostgreSQL test modules already route through
# ``tests/pg_test_guard.py``, but that only protects the modules that remember
# to use it. This hook wraps ``psycopg.connect`` for the whole session so a new
# test — or a production code path a test calls into, such as
# ``PgDatabaseManager`` built from an unvetted conninfo — cannot reach the
# production catalog server either. It compares the resolved host/port against
# the one configured test server; with no test server configured, every real
# connection is refused.
# ===========================================================================
_PSYCOPG_ORIGINAL_CONNECT = None


def _guarded_connect(original):
    from psycopg.conninfo import conninfo_to_dict

    import pg_test_guard as guard

    def wrapper(conninfo="", **kwargs):
        try:
            parsed = conninfo_to_dict(conninfo, **{
                k: v for k, v in kwargs.items()
                if k in ("host", "port", "dbname", "user", "password")})
        except Exception:
            parsed = {}
        target = ((parsed.get("host") or "").strip(),
                  str(parsed.get("port") or "").strip())

        dsn = guard.configured_dsn()          # raises on an UNSAFE setting
        if dsn is None:
            raise guard.UnsafeTestDatabase(
                f"BLOCKED: a test tried to open a PostgreSQL connection to "
                f"host={target[0] or '<default>'} port={target[1] or '<default>'} "
                f"but {guard.TEST_DSN_ENV} is not set. With no configured test "
                f"server the implicit default is localhost:5432 — the "
                f"production catalog container. {guard.SKIP_REASON}")

        allowed = guard.assert_safe_dsn(dsn)  # (host, port, dbname)
        if target != (allowed[0], str(allowed[1])):
            raise guard.UnsafeTestDatabase(
                f"BLOCKED: a test tried to open a PostgreSQL connection to "
                f"host={target[0] or '<default>'} port={target[1] or '<default>'}, "
                f"which is not the configured disposable test server "
                f"({allowed[0]}:{allowed[1]}). Tests must derive every "
                f"connection from tests/pg_test_guard.py.")
        return original(conninfo, **kwargs)

    wrapper.__wrapped__ = original
    return wrapper


@pytest.fixture(autouse=True, scope="session")
def _config_file_outside_the_repo(tmp_path_factory):
    """Point ConfigManager's default at a scratch directory for the whole run.

    Several suites construct a bare ``ConfigManager()``. Without this, the first
    of them writes a real ``config.ini`` into the repository root (it is
    gitignored, so nothing complains) and every later test that reads that file
    sees generated defaults instead of the operator's configuration. The result
    was an order-dependent suite. Tests get their own throwaway config; the
    operator's file is never read or written.
    """
    import src.config as _config
    original = _config.CONFIG_FILE
    _config.CONFIG_FILE = str(tmp_path_factory.mktemp("config") / "config.ini")
    try:
        yield _config.CONFIG_FILE
    finally:
        _config.CONFIG_FILE = original


def pytest_configure(config):
    global _PSYCOPG_ORIGINAL_CONNECT
    try:
        import psycopg
    except ImportError:                       # pragma: no cover
        return
    _PSYCOPG_ORIGINAL_CONNECT = psycopg.connect
    psycopg.connect = _guarded_connect(_PSYCOPG_ORIGINAL_CONNECT)


def pytest_unconfigure(config):
    """Restore psycopg, then sweep away every database THIS run created."""
    global _PSYCOPG_ORIGINAL_CONNECT
    if _PSYCOPG_ORIGINAL_CONNECT is None:
        return
    import psycopg
    guarded = psycopg.connect
    try:
        import pg_test_guard as guard
        leaked = guard.drop_all_run_databases()
        if leaked:
            print(f"\n[pg_test_guard] swept {len(leaked)} leaked test "
                  f"database(s): {sorted(leaked)}")
    except Exception as exc:                  # pragma: no cover - best effort
        print(f"\n[pg_test_guard] sweep failed: {exc}")
    finally:
        if psycopg.connect is guarded:
            psycopg.connect = _PSYCOPG_ORIGINAL_CONNECT
        _PSYCOPG_ORIGINAL_CONNECT = None
