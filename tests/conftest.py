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

import pytest

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
