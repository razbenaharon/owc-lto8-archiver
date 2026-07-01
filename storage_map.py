"""Root runner — Storage Map & Analytics.

Two-stage, decoupled remote disk-usage mapper (see ``src/storage_map.py``).
The application code lives in the ``src/`` package; data files (config.ini,
.env, storage_map_logs/) live here in the project root. We chdir to the root so
a relative ``config.ini`` resolves exactly as the rest of the app expects.

Usage (run from this project directory)::

    python storage_map.py scan                 # Stage 1: fire-and-forget launch
    python storage_map.py status               # check if the remote scans ended
    python storage_map.py fetch --view         # pull logs + Rich dashboard
    python storage_map.py fetch --treemap --open   # pull logs + HTML treemap
    python storage_map.py view                 # re-render locally (no SSH)
    python storage_map.py treemap --open       # rebuild the HTML (no SSH)

    python storage_map.py --server so01 scan   # limit to one server

Nightly automation (Windows Task Scheduler): schedule the 'scan' at night and,
some hours later, a 'fetch --treemap' — the scan never keeps an SSH session
open while ``du`` runs on the server.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.constants import PROJECT_ROOT
os.chdir(PROJECT_ROOT)

from src.storage_map import main


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv[1:]))
    except RuntimeError as exc:
        print(f"\n[ERROR] {exc}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n\n[ABORTED] User stopped the script.")
        sys.exit(130)
