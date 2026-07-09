"""Root runner for PostgreSQL inspector and catalog maintenance commands."""
import argparse
import json
import os
import sys
from urllib.parse import quote

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.constants import PROJECT_ROOT
os.chdir(PROJECT_ROOT)

from src.config import ConfigManager
from src.db import create_database_manager
from src.directory_catalog_validation import (
    archiver_lock_status,
    compare_databases,
    describe_database,
    validate_directory_catalog,
)
from src.pg_backup import (
    apply_directory_catalog_schema_to_database,
    create_migrated_database_from_backup,
    create_verified_production_backup,
)
from src.pg_bulk import build_conninfo


class _DbOverrideConfig:
    def __init__(self, base, dbname):
        self._base = base
        self._dbname = dbname

    def __getattr__(self, name):
        return getattr(self._base, name)

    @property
    def pg_dbname(self):
        return self._dbname or self._base.pg_dbname

    @property
    def db_dsn(self):
        user = quote(self.pg_user, safe='')
        password = quote(self.pg_password, safe='')
        auth = f"{user}:{password}@" if password else f"{user}@"
        return (
            f"postgresql://{auth}{self.pg_host}:{self.pg_port}/"
            f"{quote(self.pg_dbname, safe='')}?sslmode={quote(self.pg_sslmode, safe='')}"
        )

    @property
    def db_display_ref(self):
        user = quote(self.pg_user, safe='')
        auth = f"{user}:***@" if self.pg_password else f"{user}@"
        return (
            f"postgresql://{auth}{self.pg_host}:{self.pg_port}/"
            f"{quote(self.pg_dbname, safe='')}?sslmode={quote(self.pg_sslmode, safe='')}"
        )


def _config(args):
    cfg = ConfigManager()
    return _DbOverrideConfig(cfg, args.db) if args.db else cfg


def _conninfo(cfg, dbname=None):
    return build_conninfo(
        host=cfg.pg_host,
        port=cfg.pg_port,
        dbname=dbname or cfg.pg_dbname,
        user=cfg.pg_user,
        password=cfg.pg_password,
        sslmode=cfg.pg_sslmode,
    )


def _print_json(payload):
    print(json.dumps(payload, indent=2, default=str))


def _open_db(cfg):
    try:
        return create_database_manager(cfg)
    except RuntimeError as exc:
        print(f"\n{exc}")
        raise SystemExit(1) from exc


def _cleanup_session_data(db, assume_yes):
    try:
        summary = db.get_unreferenced_remote_data_summary()
        print("[DB] Unreferenced remote session data:")
        _print_json(summary)
        if summary['active_sessions']:
            raise RuntimeError("Refusing cleanup while a remote session is active.")
        if not summary['plans'] and not summary['snapshots']:
            print("[DB] Nothing to clean.")
            return 0
        if not assume_yes:
            confirm = input(
                "Type CLEAN to delete only this unreferenced session data "
                "and compact the database: ").strip()
            if confirm != 'CLEAN':
                print("[ABORTED]")
                return 1
        result = db.cleanup_unreferenced_remote_data(compact=True)
        print("[DB] Cleanup and compaction complete:")
        _print_json(result)
        return 0
    finally:
        db.close()


def _run_backfill(db, args):
    try:
        if not args.dry_run and not args.execute:
            raise RuntimeError(
                "--backfill-directory-catalog requires --dry-run or --execute")
        if args.dry_run and args.execute:
            raise RuntimeError("Choose only one of --dry-run or --execute")
        mode = "dry-run" if args.dry_run else "execute"
        print(f"[DB] Directory catalog backfill ({mode}) on target database...")
        result = db.backfill_directory_catalog_from_files_index(
            tape_label=args.tape,
            dry_run=args.dry_run,
            batch_size=args.batch_size,
            progress=True,
        )
        _print_json(result)
        return 0
    finally:
        db.close()


def _build_parser():
    parser = argparse.ArgumentParser(
        description="Inspect and safely maintain the PostgreSQL archive catalog.")
    parser.add_argument("--db", help="Override target database name.")
    parser.add_argument("--tape", help="Limit an operation to one tape label.")
    parser.add_argument("--batch-size", type=int, default=100,
                        help="Directory backfill bundle batch size.")
    parser.add_argument("--yes", action="store_true",
                        help="Skip supported interactive confirmations.")
    parser.add_argument("--print-db-target", action="store_true",
                        help="Print configured target and read-only DB identity.")
    parser.add_argument("--backup-postgres", action="store_true",
                        help="Create and verify a custom-format PostgreSQL dump.")
    parser.add_argument("--create-migrated-db", action="store_true",
                        help="Create, restore, and schema-migrate a new DB.")
    parser.add_argument("--backup-file",
                        help="Backup dump path for --create-migrated-db.")
    parser.add_argument("--new-db",
                        help="Explicit DB name for --create-migrated-db.")
    parser.add_argument("--apply-directory-catalog-schema", action="store_true",
                        help="Apply scripts/sql/007 to the selected DB.")
    parser.add_argument("--backfill-directory-catalog", action="store_true",
                        help="Backfill directory catalog from legacy files_index.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Report backfill work without writing.")
    parser.add_argument("--execute", action="store_true",
                        help="Execute the explicit backfill.")
    parser.add_argument("--validate-directory-catalog", action="store_true",
                        help="Print read-only directory catalog validation.")
    parser.add_argument("--compare-db", help="Source DB for row-count comparison.")
    parser.add_argument("--with-db", help="Target DB for row-count comparison.")
    parser.add_argument("--cleanup-session-data", action="store_true",
                        help="Clean unreferenced remote session data.")
    return parser


def main(argv=None):
    parser = _build_parser()
    args = parser.parse_args(argv)
    cfg = _config(args)

    if args.print_db_target:
        payload = {
            "configured_target": cfg.db_display_ref,
            "identity": describe_database(_conninfo(cfg)),
            "archiver_lock_holders": archiver_lock_status(_conninfo(cfg)),
        }
        _print_json(payload)
        return 0

    if args.backup_postgres:
        print(f"[DB BACKUP] Target: {cfg.db_display_ref}")
        _print_json(create_verified_production_backup(cfg))
        return 0

    if args.create_migrated_db:
        if not args.backup_file:
            parser.error("--create-migrated-db requires --backup-file")
        target_db = create_migrated_database_from_backup(
            cfg, args.backup_file, dbname=args.new_db)
        _print_json({"migrated_database": target_db})
        return 0

    if args.apply_directory_catalog_schema:
        apply_directory_catalog_schema_to_database(cfg, cfg.pg_dbname)
        _print_json({
            "database": cfg.pg_dbname,
            "schema": "directory_catalog",
            "applied": True,
        })
        return 0

    if args.validate_directory_catalog:
        _print_json(validate_directory_catalog(_conninfo(cfg)))
        return 0

    if args.compare_db or args.with_db:
        if not args.compare_db or not args.with_db:
            parser.error("--compare-db requires --with-db")
        source = _conninfo(cfg, dbname=args.compare_db)
        target = _conninfo(cfg, dbname=args.with_db)
        _print_json(compare_databases(source, target))
        return 0

    if args.cleanup_session_data:
        return _cleanup_session_data(_open_db(cfg), assume_yes=args.yes)

    if args.backfill_directory_catalog:
        return _run_backfill(_open_db(cfg), args)

    db = _open_db(cfg)
    from src.db_inspector_qt import run_qt_inspector
    try:
        return run_qt_inspector(db, cfg.db_dsn, display_ref=cfg.db_display_ref)
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
