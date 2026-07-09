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
from src.cold_migration import (
    ColdBackupConfig,
    backup_is_after_validation,
    cold_conninfo,
    dry_run_migration,
    execute_migration,
    migration_info,
    migration_status,
    remove_migrated_hot_rows,
    search_cold,
    validate_migration,
)
from src.db import create_database_manager
from src.directory_catalog_validation import (
    archiver_lock_status,
    compare_databases,
    describe_database,
    validate_directory_catalog,
)
from src.pg_backup import (
    apply_directory_catalog_schema_to_database,
    create_database_backup,
    create_migrated_database_from_backup,
    create_verified_production_backup,
    verify_backup_file,
)
from src.pg_bulk import build_conninfo
from src.resource_governor import ResourceGovernor


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


def _cold_backup_cfg(cfg):
    return ColdBackupConfig(cfg)


def _require_maintenance_safe(cfg):
    holders = archiver_lock_status(_conninfo(cfg))
    if holders:
        raise RuntimeError(
            "[COLD] Refusing maintenance while the archiver lock is held.")
    governor = ResourceGovernor(cfg, staging_dir=cfg.staging_dir,
                                sleep_seconds=0.25)
    if not governor.can_start_cold_migration():
        raise RuntimeError(
            "[COLD] Resource Governor refused cold maintenance; archive "
            "activity is active or local RAM/disk/I/O is unsafe.")
    return governor


def _verify_hot_backup(cfg, path):
    restore_list = verify_backup_file(cfg, path)
    return {"backup_path": os.path.abspath(path),
            "restore_list_path": restore_list,
            "verified": True}


def _verify_cold_backup(cfg, path):
    cold_cfg = _cold_backup_cfg(cfg)
    restore_list = verify_backup_file(cold_cfg, path)
    return {"backup_path": os.path.abspath(path),
            "restore_list_path": restore_list,
            "verified": True}


def _backup_cold_postgres(cfg):
    cold_cfg = _cold_backup_cfg(cfg)
    path = create_database_backup(
        cold_cfg, prefix="cold_small_file_catalog", verify=True)
    return {
        "database": cold_cfg.pg_dbname,
        "target": cold_cfg.db_display_ref,
        "backup_path": path,
        "restore_list_path": os.path.splitext(path)[0] + ".restore_list.txt",
    }


def _run_cold_migrate(cfg, args):
    governor = _require_maintenance_safe(cfg)
    hot = _conninfo(cfg)
    cold = cold_conninfo(cfg)
    if args.dry_run == args.execute:
        raise RuntimeError(
            "--cold-migrate-small-files requires exactly one of --dry-run "
            "or --execute")
    if args.dry_run:
        _print_json(dry_run_migration(hot, cfg))
        return 0
    if not args.yes:
        raise RuntimeError("--execute requires --yes")
    if not args.hot_backup_path:
        raise RuntimeError("--execute requires --hot-backup-path")
    hot_backup = _verify_hot_backup(cfg, args.hot_backup_path)
    result = execute_migration(
        hot, cold, cfg, hot_backup_path=args.hot_backup_path,
        governor=governor)
    result["hot_backup_verification"] = hot_backup
    _print_json(result)
    return 0


def _run_cold_validate(cfg, args):
    if not args.heavy:
        raise RuntimeError(
            "--cold-validate-small-file-migration requires --heavy")
    if args.migration_id is None:
        raise RuntimeError("--migration-id is required")
    governor = _require_maintenance_safe(cfg)
    _print_json(validate_migration(
        _conninfo(cfg), cold_conninfo(cfg), args.migration_id,
        governor=governor))
    return 0


def _run_cold_search(cfg, args):
    governor = _require_maintenance_safe(cfg)
    print("[COLD] Warning: cold small-file search is unindexed and may be slow.")
    rows = search_cold(
        cold_conninfo(cfg), args.cold_search, limit=args.limit,
        fetch_rows=cfg.cold_fetch_rows, governor=governor)
    _print_json({"limit": args.limit, "rows": rows})
    return 0


def _run_hot_remove(cfg, args):
    governor = _require_maintenance_safe(cfg)
    if args.migration_id is None:
        raise RuntimeError("--migration-id is required")
    if args.dry_run == args.execute:
        raise RuntimeError(
            "--hot-remove-migrated-small-files requires exactly one of "
            "--dry-run or --execute")
    hot_backup = None
    cold_backup = None
    cold_verified = False
    if args.execute:
        if not args.yes:
            raise RuntimeError("--execute requires --yes")
        if not args.hot_backup_path or not args.cold_backup_path:
            raise RuntimeError(
                "--execute requires --hot-backup-path and --cold-backup-path")
        hot_backup = _verify_hot_backup(cfg, args.hot_backup_path)
        cold_backup = _verify_cold_backup(cfg, args.cold_backup_path)
        info = migration_info(_conninfo(cfg), args.migration_id)
        if not info or not info.get("validation_passed"):
            raise RuntimeError(
                "[COLD] Migration has not passed validation.")
        if not backup_is_after_validation(
                args.cold_backup_path, info.get("validated_at")):
            raise RuntimeError(
                "[COLD] Cold backup must be created after successful "
                "migration validation.")
        cold_verified = True
    result = remove_migrated_hot_rows(
        _conninfo(cfg), cfg, args.migration_id,
        hot_backup_path=args.hot_backup_path,
        cold_backup_path=args.cold_backup_path,
        cold_backup_verified=cold_verified,
        execute=args.execute,
        governor=governor,
    )
    result["hot_backup_verification"] = hot_backup
    result["cold_backup_verification"] = cold_backup
    _print_json(result)
    return 0


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
    parser.add_argument("--backup-cold-postgres", action="store_true",
                        help="Create and verify a cold catalog PostgreSQL dump.")
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
    parser.add_argument("--heavy", action="store_true",
                        help="Allow heavy explicit validation commands.")
    parser.add_argument("--validate-directory-catalog", action="store_true",
                        help="Print read-only directory catalog validation.")
    parser.add_argument("--compare-db", help="Source DB for row-count comparison.")
    parser.add_argument("--with-db", help="Target DB for row-count comparison.")
    parser.add_argument("--cleanup-session-data", action="store_true",
                        help="Clean unreferenced remote session data.")
    parser.add_argument("--cold-status", action="store_true",
                        help="Print recent cold small-file migrations.")
    parser.add_argument("--cold-migrate-small-files", action="store_true",
                        help="Copy hot small files into the cold DB.")
    parser.add_argument("--cold-validate-small-file-migration",
                        action="store_true",
                        help="Validate a cold small-file migration.")
    parser.add_argument("--cold-search",
                        help="Search the cold small-file DB with a LIMIT.")
    parser.add_argument("--hot-remove-migrated-small-files",
                        action="store_true",
                        help="Remove validated migrated small rows from hot DB.")
    parser.add_argument("--migration-id", type=int,
                        help="Cold migration id for validation/removal.")
    parser.add_argument("--hot-backup-path",
                        help="Verified hot DB backup path.")
    parser.add_argument("--cold-backup-path",
                        help="Verified cold DB backup path.")
    parser.add_argument("--limit", type=int, default=100,
                        help="Result limit for cold search.")
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

    if args.backup_cold_postgres:
        print(f"[DB BACKUP] Cold target: {cfg.cold_db_display_ref}")
        _print_json(_backup_cold_postgres(cfg))
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

    if args.cold_status:
        _print_json(migration_status(_conninfo(cfg)))
        return 0

    if args.cold_migrate_small_files:
        return _run_cold_migrate(cfg, args)

    if args.cold_validate_small_file_migration:
        return _run_cold_validate(cfg, args)

    if args.cold_search:
        return _run_cold_search(cfg, args)

    if args.hot_remove_migrated_small_files:
        return _run_hot_remove(cfg, args)

    db = _open_db(cfg)
    from src.db_inspector_qt import run_qt_inspector
    try:
        return run_qt_inspector(db, cfg.db_dsn, display_ref=cfg.db_display_ref)
    finally:
        db.close()


if __name__ == "__main__":
    raise SystemExit(main())
