"""Web-app settings for storage_map v2, read from ``[STORAGE_MAP]``.

Every key is optional with a safe default, so the web dashboard runs without
any config.ini edits. Kept separate from :func:`storage_map.lib.core.
load_storage_map_config` so the v1 CLI config loader stays untouched.
"""
import os

from src.paths import _config_list
from storage_map.lib.core import (
    CONFIG_SECTION,
    SHARED_DATA_DEPTH,
    _safe_int,
)


class WebAppConfig:
    """Parsed web-app settings: bind address, port and DB-match options."""

    def __init__(self, host, port, match_depth, host_map, auth_token=''):
        self.host = host
        self.port = port
        self.match_depth = match_depth
        # db source_host (short, lowercase) -> configured server name.
        self.host_map = host_map
        # Shared secret required (X-Auth-Token header) on every /api/ request
        # when set. Mandatory for a non-loopback bind: the API launches SSH
        # commands on the lab servers and must never be reachable
        # unauthenticated from the network.
        self.auth_token = auth_token


LOOPBACK_HOSTS = ('127.0.0.1', 'localhost', '::1')


def ensure_bind_safe(host, webcfg):
    """Refuse a non-loopback bind unless an auth token is configured."""
    if host in LOOPBACK_HOSTS or webcfg.auth_token:
        return
    raise RuntimeError(
        f"[WEB] Refusing to bind to {host!r} without authentication: the "
        "storage-map API launches SSH scans on the configured servers. Set "
        "STORAGE_MAP_WEB_TOKEN in .env (clients must send it in the "
        "X-Auth-Token header), or bind to 127.0.0.1.")


def load_webapp_config(cfg, smcfg):
    """Build a :class:`WebAppConfig` from a :class:`~src.config.ConfigManager`.

    ``match_depth`` is clamped to the du ``scan_depth`` — the coverage table
    cannot show directories deeper than the shared-data exception reports.
    """
    conf = cfg.config
    get = lambda key, fb: conf.get(CONFIG_SECTION, key, fallback=fb)  # noqa: E731

    host = (get('web_host', '127.0.0.1') or '').strip() or '127.0.0.1'
    port = _safe_int(get('web_port', '8765'), 8765)
    requested_match_depth = _safe_int(
        get('match_depth', str(SHARED_DATA_DEPTH)), SHARED_DATA_DEPTH)
    match_depth = min(requested_match_depth, SHARED_DATA_DEPTH, smcfg.depth)
    if requested_match_depth != match_depth:
        print(f"[WEB] match_depth={requested_match_depth} exceeds "
              f"scan_depth={smcfg.depth} or the shared-data view; clamping to "
              f"{match_depth}.")

    host_map = {}
    for pair in _config_list(get('host_map', '')):
        db_host, sep, server_name = pair.partition(':')
        if sep and db_host.strip() and server_name.strip():
            host_map[db_host.strip().lower()] = server_name.strip()

    auth_token = (os.environ.get('STORAGE_MAP_WEB_TOKEN')
                  or getattr(cfg, 'env', {}).get('STORAGE_MAP_WEB_TOKEN')
                  or get('web_auth_token', '') or '').strip()

    return WebAppConfig(host, port, match_depth, host_map, auth_token)
