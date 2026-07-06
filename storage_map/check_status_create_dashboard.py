"""Check Storage Map scan status and create the dashboard if finished."""
import argparse
import os
import sys


sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.constants import PROJECT_ROOT
from src.config import ConfigManager
from src.telegram_notify import TelegramNotifier
from storage_map.lib.core import (
    _notify_status_change,
    _remote_status,
    _select_servers,
    fetch,
    load_storage_map_config,
)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Check scan status; fetch logs and build dashboard when done.")
    parser.add_argument("--server", default="all",
                        help="Server name(s) from config, comma-separated, or all.")
    parser.add_argument("--open", action="store_true",
                        help="Open storage_map/index.html when finished.")
    args = parser.parse_args(argv)

    os.chdir(PROJECT_ROOT)
    cfg = ConfigManager()
    notifier = TelegramNotifier.from_config(cfg)
    smcfg = load_storage_map_config(cfg)
    servers = _select_servers(smcfg, args.server)
    states = [(srv.name, _remote_status(srv)) for srv in servers]

    by_name = {srv.name: srv for srv in servers}
    for name, state in states:
        print(f"[STATUS] {name:<8} {state}")
        _notify_status_change(smcfg, by_name[name], state, notifier)

    if not all(state == "DONE" for _, state in states):
        print("[WAIT] Scan is not finished yet. Run this file again later.")
        return 1

    rc = fetch(smcfg, servers, do_dashboard=True, open_html=args.open,
               notifier=notifier)
    if rc == 0:
        print(f"[DONE] Dashboard is ready: {os.path.join(smcfg.dashboard_dir, 'index.html')}")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
