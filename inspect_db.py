"""Root runner — launch the PostgreSQL database inspector GUI.

Mirrors run.py's path anchoring: the GUI code lives in src/db_inspector_qt.py,
so we chdir to the project root before reading config.ini / .env.
"""
import os
import sys
import json

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.constants import PROJECT_ROOT
os.chdir(PROJECT_ROOT)

from src.config import ConfigManager
from src.db import create_database_manager

if __name__ == "__main__":
    cfg = ConfigManager()
    try:
        db = create_database_manager(cfg)
    except RuntimeError as e:
        print(f"\n{e}")
        sys.exit(1)

    if '--cleanup-session-data' in sys.argv:
        try:
            summary = db.get_unreferenced_remote_data_summary()
            print("[DB] Unreferenced remote session data:")
            print(json.dumps(summary, indent=2))
            if summary['active_sessions']:
                raise RuntimeError(
                    "Refusing cleanup while a remote session is active.")
            if not summary['plans'] and not summary['snapshots']:
                print("[DB] Nothing to clean.")
                sys.exit(0)
            if '--yes' not in sys.argv:
                confirm = input(
                    "Type CLEAN to delete only this unreferenced session data "
                    "and compact the database: ").strip()
                if confirm != 'CLEAN':
                    print("[ABORTED]")
                    sys.exit(1)
            result = db.cleanup_unreferenced_remote_data(compact=True)
            print("[DB] Cleanup and compaction complete:")
            print(json.dumps(result, indent=2))
        finally:
            db.close()
        sys.exit(0)

    from src.db_inspector_qt import run_qt_inspector

    try:
        raise SystemExit(run_qt_inspector(
            db, cfg.db_dsn, display_ref=cfg.db_display_ref))
    finally:
        db.close()
