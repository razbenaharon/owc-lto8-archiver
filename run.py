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

from src.cli import main
from src.config import ConfigManager
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

if __name__ == "__main__":
    try:
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
