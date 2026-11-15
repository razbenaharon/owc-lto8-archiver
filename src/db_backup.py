"""Backend-neutral catalog backup: create one, verify one.

``create_database_backup(cfg)`` and ``verify_backup(cfg, path)`` dispatch on
``[DATABASE] backend``:

* PostgreSQL - ``pg_dump`` custom-format dump (:mod:`src.pg_backup`).
* SQLite - a consistent copy made with the SQLite *online backup API* while
  the catalog stays usable, then proven with ``PRAGMA integrity_check`` and
  ``PRAGMA foreign_key_check`` on the copy.

Both return / describe the same :class:`src.pg_backup.BackupVerification`
shape, so callers print one honest summary whatever the backend.

Importing this module never imports psycopg.
"""
from __future__ import annotations

import hashlib
import os
import sqlite3
import urllib.parse
from datetime import datetime
from pathlib import Path

from . import db_backend
from .constants import DB_BACKUP_DIR

SQLITE_BACKUP_SUFFIX = ".sqlite3"

NOT_STATE_STATEMENT_SQLITE = (
    "Verification says nothing about WHICH moment of the catalog the backup "
    "holds beyond its own timestamp; later changes are not in it.")


def backup_noun(cfg):
    """Operator-facing name of what a backup of this catalog is."""
    if db_backend.is_sqlite(cfg):
        return "SQLite catalog backup"
    return "PostgreSQL dump"


def backup_menu_text(cfg):
    if db_backend.is_sqlite(cfg):
        return "Database Backup — Copy the SQLite catalog to db_backups"
    return "Database Backup — Dump PostgreSQL catalog to db_backups"


def _sha256_file(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _read_only_uri(path):
    quoted = urllib.parse.quote(os.path.abspath(path).replace("\\", "/"),
                                safe="/:")
    return ("file://" + ("" if quoted.startswith("/") else "/") + quoted
            + "?mode=ro")


def _local_path(cfg, path, *, expected_kind, allow_missing):
    # Same fail-closed rule as PostgreSQL backups: a plain local path that is
    # provably not the tape drive, checked before touching the filesystem.
    from .pg_backup import validate_backup_local_path
    return validate_backup_local_path(
        cfg, path, expected_kind=expected_kind, allow_missing=allow_missing)


def _safe_part(value, fallback):
    text = "".join(ch if ch.isalnum() or ch in "-_." else "_"
                   for ch in str(value or "")).strip("._")
    return text or fallback


def verify_sqlite_backup(cfg, backup_path):
    """Prove a SQLite catalog backup is a sound database file.

    Opens the copy read-only and runs ``integrity_check`` and
    ``foreign_key_check``. A failed check is reported in the result (``ok``
    False); only an unsafe path raises.
    """
    from .pg_backup import (BACKUP_LEVEL_NONE, BACKUP_LEVEL_READ_THROUGH,
                            BackupCheck, BackupVerification)
    backup_path = _local_path(cfg, backup_path, expected_kind="file",
                              allow_missing=False)
    size = backup_path.stat().st_size
    checks = [BackupCheck(
        "non_empty", size > 0,
        f"{size:,} bytes" if size > 0 else "backup is missing or empty")]
    details = {}
    level = BACKUP_LEVEL_NONE
    digest = ""
    if size > 0:
        digest = _sha256_file(backup_path)
        try:
            conn = sqlite3.connect(_read_only_uri(backup_path), uri=True)
        except sqlite3.Error as exc:
            checks.append(BackupCheck("opens_as_sqlite", False, str(exc)))
        else:
            try:
                try:
                    rows = [row[0] for row in conn.execute(
                        "PRAGMA integrity_check").fetchall()]
                    ok = rows == ["ok"]
                    checks.append(BackupCheck(
                        "integrity_check", ok,
                        "ok" if ok else "; ".join(map(str, rows[:5]))))
                    violations = conn.execute(
                        "PRAGMA foreign_key_check").fetchall()
                    checks.append(BackupCheck(
                        "foreign_key_check", not violations,
                        "no violations" if not violations
                        else f"{len(violations)} violation(s)"))
                    migrations = conn.execute(
                        "SELECT version, name FROM schema_migrations "
                        "ORDER BY version").fetchall()
                    checks.append(BackupCheck(
                        "catalog_schema_present", bool(migrations),
                        f"schema v{migrations[-1][0]}" if migrations
                        else "schema_migrations is empty"))
                    details["schema_migrations"] = [
                        {"version": row[0], "name": row[1]}
                        for row in migrations]
                    details["files_index_rows"] = conn.execute(
                        "SELECT count(*) FROM files_index").fetchone()[0]
                except sqlite3.Error as exc:
                    checks.append(BackupCheck(
                        "readable_catalog", False, str(exc)))
            finally:
                conn.close()
        if all(check.passed for check in checks):
            # integrity_check reads every page of every table and index.
            level = BACKUP_LEVEL_READ_THROUGH
    return BackupVerification(
        backend=db_backend.BACKEND_SQLITE, backup_path=str(backup_path),
        size_bytes=size, sha256=digest, level=level, checks=tuple(checks),
        not_verified=(NOT_STATE_STATEMENT_SQLITE,), artifacts={},
        details=details)


def create_sqlite_backup(cfg, output_dir=None, prefix=None, verify=True):
    """Copy the live SQLite catalog with the online backup API.

    The copy is written to a ``.part`` file, verified, and only then renamed
    into place - a failed or half-written backup never looks like a good one.
    Returns the backup path.
    """
    source = Path(cfg.sqlite_path)
    if not source.is_file():
        raise RuntimeError(
            f"[DB BACKUP] The SQLite catalog does not exist yet: {source}")
    target_dir = _local_path(cfg, output_dir or DB_BACKUP_DIR,
                             expected_kind="directory", allow_missing=True)
    target_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    name = f"{_safe_part(source.stem, 'catalog')}_{stamp}{SQLITE_BACKUP_SUFFIX}"
    if prefix:
        name = f"{_safe_part(prefix, 'backup')}_{name}"
    output_path = _local_path(cfg, target_dir / name, expected_kind="file",
                              allow_missing=True)
    if output_path.exists():
        raise RuntimeError(
            "[DB BACKUP] Refusing to overwrite an existing backup")
    if os.path.normcase(str(output_path)) == os.path.normcase(str(source)):
        raise RuntimeError(
            "[DB BACKUP] The backup target is the live catalog itself")
    temporary = output_path.with_name(output_path.name + ".part")
    temporary.unlink(missing_ok=True)
    try:
        try:
            live = sqlite3.connect(_read_only_uri(source), uri=True,
                                   timeout=30.0)
        except sqlite3.Error as exc:
            raise RuntimeError(
                f"[DB BACKUP] Could not open the SQLite catalog: {exc}"
            ) from exc
        try:
            copy = sqlite3.connect(str(temporary))
            try:
                live.backup(copy)
            finally:
                copy.close()
        except sqlite3.Error as exc:
            raise RuntimeError(
                f"[DB BACKUP] SQLite online backup failed: {exc}") from exc
        finally:
            live.close()
        if verify:
            result = verify_sqlite_backup(cfg, temporary)
            if not result.ok:
                raise RuntimeError(
                    "[DB BACKUP] The new backup failed verification and was "
                    "discarded.\n" + result.summary())
        os.replace(temporary, output_path)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise
    return str(output_path)


def create_database_backup(cfg, output_dir=None, prefix=None, verify=None,
                           **pg_options):
    """Create a catalog backup for the configured backend; return its path.

    SQLite verifies by default (it is cheap and local). PostgreSQL keeps its
    historical default (``verify=False``) unless asked.
    """
    if db_backend.is_sqlite(cfg):
        if pg_options:
            raise db_backend.PostgresOnlyFeature("pg_dump_backup")
        return create_sqlite_backup(
            cfg, output_dir=output_dir, prefix=prefix,
            verify=True if verify is None else bool(verify))
    from .pg_backup import create_database_backup as create_pg_backup
    return create_pg_backup(
        cfg, output_dir=output_dir, prefix=prefix, verify=bool(verify),
        **pg_options)


def verify_backup(cfg, backup_path, **pg_options):
    """Verify a catalog backup; returns a ``BackupVerification``."""
    if db_backend.is_sqlite(cfg):
        if pg_options.get("deep_target") is not None:
            raise db_backend.PostgresOnlyFeature("pg_dump_backup")
        return verify_sqlite_backup(cfg, backup_path)
    from .pg_backup import verify_backup as verify_pg_backup
    return verify_pg_backup(cfg, backup_path, **pg_options)
