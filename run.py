"""Root runner — launch the LTO Archive Management System CLI.

The application code lives in the ``src/`` package; data files (config.ini,
.env, lto_archive.db, backup_logs/) live here in the project root. We chdir to
the root so a relative ``config.ini`` resolves exactly as it did before the
package split.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.constants import PROJECT_ROOT
os.chdir(PROJECT_ROOT)

from src.cli import main


def _run_maintenance():
    from src.config import ConfigManager
    from src.maintenance import (
        CatalogV3Optimizer,
        DatabaseOptimizer,
        HashlessOriginOptimizer,
        inspect_catalog_database,
        inspect_legacy_database,
    )

    cfg = ConfigManager()
    if '--catalog-v3-preflight' in sys.argv:
        import json
        print(json.dumps(inspect_catalog_database(cfg.db_path), indent=2))
        return
    if '--catalog-v3-migrate' in sys.argv:
        CatalogV3Optimizer(cfg.db_path).run()
        return
    if '--hashless-origin-migrate' in sys.argv:
        HashlessOriginOptimizer(cfg.db_path).run()
        return
    dry_run = '--dry-run' in sys.argv
    if dry_run:
        import json
        print(json.dumps(inspect_legacy_database(cfg.db_path), indent=2))
        return
    DatabaseOptimizer(cfg.db_path).run()

if __name__ == "__main__":
    try:
        if any(arg in sys.argv for arg in (
                '--optimize-db', '--catalog-v3-preflight',
                '--catalog-v3-migrate', '--hashless-origin-migrate')):
            _run_maintenance()
        else:
            main()
    except RuntimeError as e:
        print(f"\n{e}")
    except KeyboardInterrupt:
        print("\n\n[ABORTED] User stopped the script.")
