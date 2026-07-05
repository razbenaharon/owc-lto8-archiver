"""Start a fresh Storage Map scan, then exit immediately.

Run this when you want the servers to begin scanning in the background. It does
not wait for the scan to finish and it does not create the dashboard.
"""
import argparse
import os
import sys


sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.config import ConfigManager
from src.telegram_notify import TelegramNotifier
from storage_map.lib.core import _select_servers, load_storage_map_config, scan


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Launch remote storage-map scans and exit immediately.")
    parser.add_argument("--server", default="all",
                        help="Server name(s) from config, comma-separated, or all.")
    args = parser.parse_args(argv)

    cfg = ConfigManager()
    smcfg = load_storage_map_config(cfg)
    servers = _select_servers(smcfg, args.server)
    rc = scan(smcfg, servers, notifier=TelegramNotifier.from_config(cfg))
    if rc == 0:
        print("[STARTED] Remote scan launched. This process is done.")
        print("          Later, run: python storage_map/check_status_create_dashboard.py --open")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
