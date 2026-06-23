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

if __name__ == "__main__":
    try:
        main()
    except RuntimeError as e:
        print(f"\n{e}")
    except KeyboardInterrupt:
        print("\n\n[ABORTED] User stopped the script.")
