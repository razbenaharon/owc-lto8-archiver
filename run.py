"""Root runner — launch the LTO Archive Management System CLI.

The application code lives in the ``src/`` package; data files (config.ini,
.env, backup_logs/) live here in the project root. We chdir to
the root so a relative ``config.ini`` resolves exactly as it did before the
package split.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.constants import PROJECT_ROOT
os.chdir(PROJECT_ROOT)

from src.cli import main, run_remote_archiver_headless
from src.config import ConfigManager
from src.db import create_database_manager
from src.logsetup import configure_file_logging, get_logger
from src.pg_backup import create_database_backup


_REMOVED_SQLITE_FLAGS = {
    "--optimize-db",
    "--catalog-v3-preflight",
    "--catalog-v3-migrate",
    "--catalog-rebuild",
    "--hashless-origin-migrate",
    "--dry-run",
}


def _run_headless_remote_archive(argv):
    """`python run.py remote-archive [--resume] [--non-interactive]`.

    A promptless, menu-less launch for a stable manual/detached run. It never
    reads stdin, so a closed stdin cannot crash it; it exits with a stable code
    (see src/exit_codes.py). Absence of this subcommand leaves the interactive
    menu (``main()``) completely unchanged.
    """
    import argparse
    parser = argparse.ArgumentParser(
        prog="run.py remote-archive",
        description="Headless remote archive to LTO tape (no interactive menu).")
    parser.add_argument(
        "--resume", action="store_true",
        help="Resume the single active session for the configured host/path.")
    parser.add_argument(
        "--non-interactive", action="store_true",
        help="Never prompt (implied by this subcommand; accepted for clarity).")
    args = parser.parse_args(argv)

    cfg = ConfigManager()
    configure_file_logging(cfg.backup_log_dir)
    db = create_database_manager(cfg)
    try:
        code = run_remote_archiver_headless(cfg, db, resume=args.resume)
    finally:
        try:
            db.close()
        except Exception:
            pass
    raise SystemExit(code)


if __name__ == "__main__":
    try:
        if len(sys.argv) > 1 and sys.argv[1] == "remote-archive":
            _run_headless_remote_archive(sys.argv[2:])

        if "--backup-db" in sys.argv:
            cfg = ConfigManager()
            configure_file_logging(cfg.backup_log_dir)
            path = create_database_backup(cfg)
            print(f"[DB BACKUP] PostgreSQL dump created: {path}")
            raise SystemExit(0)

        removed = sorted(_REMOVED_SQLITE_FLAGS.intersection(sys.argv))
        if removed:
            raise RuntimeError(
                "SQLite maintenance commands have been removed. The archive "
                "catalog is PostgreSQL-only; manage schema with scripts/sql/*.sql "
                f"instead. Unsupported flag(s): {', '.join(removed)}"
            )
        main()
    except SystemExit:
        raise
    except RuntimeError as e:
        # Operator-facing errors are raised as RuntimeError with a readable
        # message; the full traceback goes to backup_logs/archiver.log, and
        # LTO_DEBUG=1 re-raises it onto the console for bug reports.
        get_logger().exception("run.py stopped on error")
        if os.environ.get('LTO_DEBUG'):
            raise
        print(f"\n{e}")
    except KeyboardInterrupt:
        print("\n\n[ABORTED] User stopped the script.")
