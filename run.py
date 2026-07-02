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
        removed = sorted(_REMOVED_SQLITE_FLAGS.intersection(sys.argv))
        if removed:
            raise RuntimeError(
                "SQLite maintenance commands have been removed. The archive "
                "catalog is PostgreSQL-only; manage schema with scripts/sql/*.sql "
                f"instead. Unsupported flag(s): {', '.join(removed)}"
            )
        main()
    except RuntimeError as e:
        print(f"\n{e}")
    except KeyboardInterrupt:
        print("\n\n[ABORTED] User stopped the script.")
