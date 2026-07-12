"""Storage Map & Analytics — a two-stage, decoupled remote disk-usage mapper.

The goal is to give lab admins an at-a-glance picture of how disk space is
distributed across the critical mount points of the lab servers (so01, so02),
so a filling disk is caught before it crashes a shared machine — *without*
loading those slow, shared disks during working hours.

Two stages, mediated by an on-disk raw log so they never run at the same time:

  STAGE 1 — SCANNER (fire-and-forget)
      ``scan`` connects to each configured server over SSH, launches a
      low-priority ``du`` scan under ``nohup``/``setsid`` on the *remote* host,
      then returns immediately. The heavy scan keeps running on the server even
      after this laptop's SSH session closes or the network drops. Nothing is
      streamed back live.

  STAGE 2 — FETCH / VISUALIZE (run later)
      ``status`` checks whether each server's scan has finished. ``fetch``
      SCP-copies the finished raw log to this machine. ``view`` / ``treemap``
      parse a *local* raw log and render it as a Rich terminal dashboard and/or
      an interactive Plotly HTML treemap. Parsing and rendering touch only the
      local log file — the physical server disks are never read again.

Design rules honoured here:
  * Mount points are NEVER hardcoded — they are loaded from the ``[STORAGE_MAP]``
    section of ``config.ini`` via :class:`~src.config.ConfigManager`.
  * The scanner does not block on the scan: launch-and-exit is a distinct
    command from fetch, so a 2-hour scan does not require a live SSH session.

This module sits at the very top of the package dependency graph (it imports
``config``/``constants``/``paths``/``remote_transport`` and nothing imports it),
so it stays fully decoupled from the tape pipeline.
"""
import argparse
import json
import os
import posixpath
import re
import shlex
import sys
from datetime import datetime

from src.config import ConfigManager
from src.constants import PROJECT_ROOT
from src.paths import _clean_remote_path, _config_list, _safe_log_token
from src.remote_transport import _scp_fetch_file, _ssh_run
from src.telegram_notify import (
    TelegramNotifier,
    format_storage_dashboard_summary,
    format_storage_fetch_summary,
    format_storage_scan_summary,
    format_storage_status_summary,
    send_best_effort,
)

# Optional visualization dependencies — the scanner and parser work without
# either; each visualizer degrades to a helpful "pip install ..." message.
try:
    from rich.console import Console
    from rich.table import Table
    from rich.tree import Tree
    _HAVE_RICH = True
except ImportError:  # pragma: no cover - exercised only when rich is absent
    _HAVE_RICH = False

try:
    import plotly.graph_objects as go
    _HAVE_PLOTLY = True
except ImportError:  # pragma: no cover - exercised only when plotly is absent
    _HAVE_PLOTLY = False


# Remote working directory (relative to the SSH user's home) where each scan
# writes its output, error log, launcher script, and completion sentinel.
REMOTE_SCAN_DIR = '.storage_map/current'
CONFIG_SECTION = 'STORAGE_MAP'
TOP_LAYER_DEPTH = 1
SHARED_DATA_DEPTH = 2
SHARED_DATA_DIR = 'shared-data'

# Human-readable size suffixes emitted by ``du -h`` (binary / base-1024).
_UNIT_FACTORS = {
    '': 1, 'B': 1,
    'K': 1024, 'M': 1024**2, 'G': 1024**3,
    'T': 1024**4, 'P': 1024**5, 'E': 1024**6,
}
_SIZE_RE = re.compile(r'^\s*([0-9]+(?:[.,][0-9]+)?)\s*([KMGTPEB]?)(?:i?B)?\s*$', re.I)


# --------------------------------------------------------------------------- #
# Configuration                                                               #
# --------------------------------------------------------------------------- #
class ServerConfig:
    """One scan target: name, SSH host/user/password and its mount list."""

    def __init__(self, name, host, user, password, mounts):
        self.name = name
        self.host = host
        self.user = user
        self.password = password
        self.mounts = mounts

    def __repr__(self):  # pragma: no cover - debugging aid
        return f"ServerConfig(name={self.name!r}, host={self.host!r}, mounts={self.mounts!r})"


class StorageMapConfig:
    """Parsed ``[STORAGE_MAP]`` settings plus the per-server targets."""

    def __init__(self, output_dir, dashboard_dir, depth, poll_timeout, servers):
        self.output_dir = output_dir
        self.dashboard_dir = dashboard_dir
        self.depth = depth
        self.poll_timeout = poll_timeout
        self.servers = servers

    def server(self, name):
        for srv in self.servers:
            if srv.name == name:
                return srv
        return None


def load_storage_map_config(cfg):
    """Build a :class:`StorageMapConfig` from a :class:`ConfigManager`.

    Mounts, servers, depth and output directory all come from config — nothing
    about the scan targets is hardcoded here. SSH user/password fall back to the
    existing ``[REMOTE]`` credentials (same ``ben.raz`` account, secret still in
    ``.env``) so no new secret handling is introduced.
    """
    conf = cfg.config
    if not conf.has_section(CONFIG_SECTION):
        raise RuntimeError(
            f"config.ini has no [{CONFIG_SECTION}] section. Add one describing "
            f"the servers and mount points to map (see config.example.ini)."
        )

    raw_out = conf.get(CONFIG_SECTION, 'output_dir',
                       fallback='storage_map/logs').strip()
    output_dir = raw_out if os.path.isabs(raw_out) else os.path.join(PROJECT_ROOT, raw_out)

    raw_dash = conf.get(CONFIG_SECTION, 'dashboard_dir',
                        fallback='storage_map').strip()
    dashboard_dir = raw_dash if os.path.isabs(raw_dash) else os.path.join(PROJECT_ROOT, raw_dash)

    requested_depth = _safe_int(
        conf.get(CONFIG_SECTION, 'scan_depth', fallback=str(SHARED_DATA_DEPTH)),
        SHARED_DATA_DEPTH)
    depth = max(TOP_LAYER_DEPTH, min(requested_depth, SHARED_DATA_DEPTH))
    if requested_depth != depth:
        print(f"[STORAGE_MAP] scan_depth={requested_depth} is outside the "
              f"supported top-layer/shared-data view; using scan_depth={depth}.")
    poll_timeout = _safe_int(
        conf.get(CONFIG_SECTION, 'poll_timeout_seconds', fallback='14400'), 14400)

    shared_mounts = _mount_list(conf.get(CONFIG_SECTION, 'mounts', fallback=''))
    server_names = _config_list(conf.get(CONFIG_SECTION, 'servers', fallback=''))

    servers = []
    for name in server_names:
        sec = f'{CONFIG_SECTION}:{name}'
        if not conf.has_section(sec):
            raise RuntimeError(
                f"[{CONFIG_SECTION}] lists server '{name}' but there is no "
                f"[{sec}] section defining its host.")
        host = conf.get(sec, 'host', fallback='').strip()
        if not host:
            raise RuntimeError(f"[{sec}] is missing a 'host' value.")
        user = conf.get(sec, 'user', fallback='').strip() or cfg.remote_user
        # A per-server 'mounts' override wins; otherwise use the shared list.
        mounts = _mount_list(conf.get(sec, 'mounts', fallback='')) or shared_mounts
        if not mounts:
            raise RuntimeError(
                f"No mount points configured for server '{name}'. Set 'mounts' "
                f"in [{CONFIG_SECTION}] or [{sec}].")
        servers.append(ServerConfig(name, host, user, cfg.remote_password, mounts))

    if not servers:
        raise RuntimeError(
            f"[{CONFIG_SECTION}] defines no servers. Set e.g. 'servers = so01, so02'.")
    return StorageMapConfig(output_dir, dashboard_dir, depth, poll_timeout, servers)


def _mount_list(raw):
    """Config text -> list of clean POSIX mount paths (order preserved)."""
    return [p for p in (_clean_remote_path(m) for m in _config_list(raw)) if p]


def _safe_int(value, default):
    try:
        return max(1, int(float(str(value).strip())))
    except (TypeError, ValueError):
        return default


def _shared_data_child(mount):
    """Return the shared-data child path for a configured POSIX mount."""
    mount = (mount or '').rstrip('/') or '/'
    if mount == '/':
        return '/' + SHARED_DATA_DIR
    return mount + '/' + SHARED_DATA_DIR


# --------------------------------------------------------------------------- #
# Stage 1 — Scanner (fire-and-forget)                                         #
# --------------------------------------------------------------------------- #
def path_segments(path):
    """``/strg/D`` -> 2 segments; ``/`` -> 0. Used to size the awk rollup."""
    return len([p for p in (path or '').strip('/').split('/') if p])


def _dir_count_line(target, rel_depth):
    """``find -type d | awk`` pipeline emitting recursive dir counts per dir."""
    qt = shlex.quote(target)
    return (
        f"$PRE find {qt} -xdev -type d -not -path '/proc/*' "
        f"-not -path '/sys/*' -printf '%p\\n' 2>>\"$DIR/scan.err\" | "
        f'awk -F/ -v a={path_segments(target)} -v r={rel_depth} '
        f'{shlex.quote(ROLLUP_AWK)} 2>>"$DIR/scan.err" || true'
    )


def _remote_launcher_script(server, depth):
    """Return the shell script (run.sh) executed on the remote host.

    It writes a self-describing raw log with one section per mount, running each
    ``du`` at the lowest I/O and CPU priority available so it yields to real lab
    workloads. ``du`` only ``stat()``s entries (metadata) and never reads file
    contents; ``--max-depth`` limits *reporting* depth and ``-x`` keeps ``/`` on
    its own filesystem so the other mounts are not double-counted.
    """
    q = shlex.quote
    depth = max(TOP_LAYER_DEPTH,
                min(_safe_int(depth, SHARED_DATA_DEPTH), SHARED_DATA_DEPTH))
    lines = [
        'DIR="$HOME/' + REMOTE_SCAN_DIR + '"',
        'mkdir -p "$DIR"',
        'rm -f "$DIR/scan.out" "$DIR/scan.err" "$DIR/scan.sentinel"',
        # Best-effort low-priority prefix; degrade gracefully if the tools are
        # missing so the scan still runs on a minimal server.
        'PRE=""',
        'command -v nice >/dev/null 2>&1 && PRE="nice -n19"',
        'command -v ionice >/dev/null 2>&1 && PRE="ionice -c3 $PRE"',
        '{',
        r'printf "# storage-map raw log\n"',
        'printf "# server: %s\\n" ' + q(server.host),
        'printf "# generated_at: %s\\n" "$(date -Iseconds 2>/dev/null || date)"',
        'printf "# depth: %s\\n" ' + q(str(depth)),
    ]
    for mount in server.mounts:
        qm = q(mount)
        lines.append('printf "##### MOUNT: %s #####\\n" ' + qm)
        lines.append(
            f"df -B1 -P {qm} 2>>\"$DIR/scan.err\" | "
            "awk 'NR==2 {printf \"##### DF: %s %s %s %s #####\\n\", "
            "$2, $3, $4, $5}' || true")
        lines.append(
            # -B1 emits exact byte counts (parse_size handles bare integers);
            # human-readable rounding (-h) is too lossy for tape-coverage math.
            f'$PRE du -x -B1 --max-depth={TOP_LAYER_DEPTH} '
            f"--exclude='/proc/*' --exclude='/sys/*' {qm} "
            f'2>>"$DIR/scan.err" || true')
        if depth >= SHARED_DATA_DEPTH:
            shared = q(_shared_data_child(mount))
            lines.append(
                f'if [ -d {shared} ]; then $PRE du -x -B1 '
                f'--max-depth={TOP_LAYER_DEPTH} '
                f"--exclude='/proc/*' --exclude='/sys/*' {shared} "
                f'2>>"$DIR/scan.err" || true; fi')
        # Inode (file+dir) counts for the same tree. Same cheap metadata walk as
        # the byte du; lets coverage require a per-directory file-count match
        # instead of trusting the byte ratio (block allocation inflates du bytes
        # for directories of many tiny files). Older logs omit this block, so the
        # parser and coverage treat a missing count as unknown.
        lines.append('printf "##### INODES #####\\n"')
        lines.append(
            f'$PRE du -x --inodes --max-depth={TOP_LAYER_DEPTH} '
            f"--exclude='/proc/*' --exclude='/sys/*' {qm} "
            f'2>>"$DIR/scan.err" || true')
        if depth >= SHARED_DATA_DEPTH:
            shared = q(_shared_data_child(mount))
            lines.append(
                f'if [ -d {shared} ]; then $PRE du -x --inodes '
                f'--max-depth={TOP_LAYER_DEPTH} '
                f"--exclude='/proc/*' --exclude='/sys/*' {shared} "
                f'2>>"$DIR/scan.err" || true; fi')
        # Directory-only counts (find -type d, no per-file enumeration). The
        # exact recursive file count is inodes − dirs, which lets coverage drop
        # the du --inodes subdirectory tolerance and certify large directories
        # precisely. -type d reads directory entries but does not stat every
        # file, so it is lighter than the du passes.
        lines.append('printf "##### DIRS #####\\n"')
        lines.append(_dir_count_line(mount, TOP_LAYER_DEPTH))
        if depth >= SHARED_DATA_DEPTH:
            child = _shared_data_child(mount)
            lines.append(
                f'if [ -d {q(child)} ]; then '
                + _dir_count_line(child, TOP_LAYER_DEPTH) + '; fi')
    lines.append(r'printf "##### END #####\n"')
    lines.append('} > "$DIR/scan.out" 2>>"$DIR/scan.err"')
    # The sentinel is written last, so its presence means the whole run finished.
    lines.append('echo DONE > "$DIR/scan.sentinel"')
    return '\n'.join(lines)


def _remote_launch_command(server, depth):
    """Full remote command: write run.sh then detach it from the SSH session."""
    q = shlex.quote
    script = _remote_launcher_script(server, depth)
    run_sh = REMOTE_SCAN_DIR + '/run.sh'
    # A *quoted* heredoc terminator stores the script literally, so $DIR / $PRE /
    # $(date) are expanded when run.sh executes on the server, not now.
    return (
        f'mkdir -p {q(REMOTE_SCAN_DIR)} && '
        f'cat > {q(run_sh)} <<\'__SM_EOF__\'\n'
        f'{script}\n'
        f'__SM_EOF__\n'
        f'chmod +x {q(run_sh)}\n'
        f'if command -v setsid >/dev/null 2>&1; then '
        f'setsid sh {q(run_sh)} </dev/null >/dev/null 2>&1 & '
        f'else nohup sh {q(run_sh)} </dev/null >/dev/null 2>&1 & fi\n'
        f'echo LAUNCHED'
    )


def scan(smcfg, servers, notifier=None):
    """Stage 1: launch the remote scan on each server and return immediately."""
    os.makedirs(smcfg.output_dir, exist_ok=True)
    started_at = datetime.now().isoformat(timespec='seconds')
    failures = 0
    launched = []
    failed = []
    for srv in servers:
        print(f"[SCAN] Launching background scan on {srv.name} ({srv.host}) "
              f"for {len(srv.mounts)} mount(s)...")
        cmd = _remote_launch_command(srv, smcfg.depth)
        result = _ssh_run(srv.user, srv.host, cmd,
                          password=srv.password, timeout=120)
        if result.returncode != 0 or 'LAUNCHED' not in (result.stdout or ''):
            failures += 1
            failed.append(srv.name)
            err = (result.stderr or '').strip() or (result.stdout or '').strip()
            print(f"[SCAN] FAILED to launch on {srv.name}: {err}")
            continue
        _write_manifest(smcfg, srv, started_at)
        launched.append(srv.name)
        print(f"[SCAN] {srv.name}: scan running detached under nohup/setsid. "
              f"SSH session no longer required.")

    print("\n[SCAN] Fire-and-forget complete. The scan(s) continue on the "
          "server(s) after this process exits.")
    print("[SCAN] Use the Storage Map web app later to check progress and "
          "refresh completed server results.")
    send_best_effort(notifier, format_storage_scan_summary(launched, failed))
    return 1 if failures else 0


def _manifest_path(smcfg, server):
    return os.path.join(smcfg.output_dir, f'{_safe_log_token(server.name)}.pending.json')


def _write_manifest(smcfg, server, started_at):
    """Record when/what was launched (never the password) for later fetch."""
    data = {
        'server': server.name,
        'host': server.host,
        'user': server.user,
        'mounts': server.mounts,
        'depth': smcfg.depth,
        'remote_dir': REMOTE_SCAN_DIR,
        'started_at': started_at,
    }
    try:
        with open(_manifest_path(smcfg, server), 'w', encoding='utf-8') as fh:
            json.dump(data, fh, indent=2)
    except OSError as exc:  # pragma: no cover - disk/permission edge
        print(f"[SCAN] Warning: could not write manifest for {server.name}: {exc}")


def _read_manifest(smcfg, server):
    try:
        with open(_manifest_path(smcfg, server), encoding='utf-8') as fh:
            return json.load(fh)
    except (OSError, ValueError):
        return None


def _update_manifest(smcfg, server, data):
    if not data:
        return
    try:
        with open(_manifest_path(smcfg, server), 'w', encoding='utf-8') as fh:
            json.dump(data, fh, indent=2)
    except OSError as exc:  # pragma: no cover - disk/permission edge
        print(f"[STATUS] Warning: could not update manifest for {server.name}: {exc}")


# --------------------------------------------------------------------------- #
# Stage 1.5 — status / fetch                                                  #
# --------------------------------------------------------------------------- #
def _remote_status(server):
    """Return 'DONE', 'PENDING', or 'MISSING' for a server's remote scan."""
    sentinel = f'$HOME/{REMOTE_SCAN_DIR}/scan.sentinel'
    run_sh = f'$HOME/{REMOTE_SCAN_DIR}/run.sh'
    cmd = (f'if [ -f "{sentinel}" ]; then echo DONE; '
           f'elif [ -f "{run_sh}" ]; then echo PENDING; '
           f'else echo MISSING; fi')
    result = _ssh_run(server.user, server.host, cmd,
                      password=server.password, timeout=60)
    out = (result.stdout or '').strip().splitlines()
    if result.returncode != 0 or not out:
        return 'UNREACHABLE'
    return out[-1].strip()


def status(smcfg, servers, notifier=None):
    """Report per-server scan status without fetching anything."""
    any_pending = False
    for srv in servers:
        state = _remote_status(srv)
        started = _manifest_started(smcfg, srv)
        suffix = f" (launched {started})" if started else ""
        print(f"[STATUS] {srv.name:<8} {state}{suffix}")
        _notify_status_change(smcfg, srv, state, notifier)
        if state in ('PENDING', 'UNREACHABLE'):
            any_pending = True
    return 1 if any_pending else 0


def _notify_status_change(smcfg, server, state, notifier=None):
    if notifier is None or not getattr(notifier, 'enabled', True):
        return False
    if state not in ('DONE', 'UNREACHABLE', 'MISSING'):
        return False
    manifest = _read_manifest(smcfg, server)
    if not manifest:
        return False
    if manifest.get('last_notified_status') == state:
        return False
    manifest['last_notified_status'] = state
    _update_manifest(smcfg, server, manifest)
    return send_best_effort(
        notifier,
        format_storage_status_summary(
            server.name, state, manifest.get('started_at')))


def _manifest_started(smcfg, server):
    manifest = _read_manifest(smcfg, server)
    return (manifest or {}).get('started_at')


def fetch(smcfg, servers, do_view=False, do_treemap=False, do_dashboard=False,
          top=15, open_html=False, notifier=None):
    """SCP the finished raw log(s) locally; optionally render afterwards."""
    os.makedirs(smcfg.output_dir, exist_ok=True)
    fetched = []
    skipped = []
    failed = []
    for srv in servers:
        state = _remote_status(srv)
        if state != 'DONE':
            skipped.append(f"{srv.name}({state})")
            print(f"[FETCH] {srv.name}: scan not finished (status={state}); "
                  f"skipping. Try again later.")
            continue
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        dated = os.path.join(smcfg.output_dir,
                             f'{_safe_log_token(srv.name)}_{ts}.rawlog')
        remote_out = f'{REMOTE_SCAN_DIR}/scan.out'
        rc = _scp_fetch_file(srv.user, srv.host, remote_out, dated,
                             password=srv.password)
        if rc != 0 or not os.path.exists(dated) or os.path.getsize(dated) == 0:
            failed.append(srv.name)
            print(f"[FETCH] {srv.name}: failed to retrieve raw log (scp rc={rc}).")
            continue
        latest = _latest_path(smcfg, srv.name)
        _copy_file(dated, latest)
        print(f"[FETCH] {srv.name}: raw log saved -> {dated}")
        fetched.append(srv.name)

    if not fetched:
        print("[FETCH] No completed scans were retrieved.")
        send_best_effort(
            notifier,
            format_storage_fetch_summary(fetched, skipped, failed))
        return 1

    rendered = [s for s in servers if s.name in fetched]
    render_results = {}
    if do_view:
        render_results['view'] = view(smcfg, rendered, top=top)
    if do_treemap:
        render_results['treemap'] = treemap(smcfg, rendered, open_html=open_html)
    if do_dashboard:
        render_results['dashboard'] = dashboard(
            smcfg, rendered, open_html=open_html, notifier=None)
    send_best_effort(
        notifier,
        format_storage_fetch_summary(fetched, skipped, failed, render_results))
    return 0


def _latest_path(smcfg, server_name):
    return os.path.join(smcfg.output_dir,
                        f'{_safe_log_token(server_name)}_latest.rawlog')


def _copy_file(src, dst):
    with open(src, 'rb') as rf, open(dst, 'wb') as wf:
        wf.write(rf.read())


# --------------------------------------------------------------------------- #
# Stage 2 — Parser (pure; zero disk touch beyond the local raw log)           #
# --------------------------------------------------------------------------- #
def parse_size(token):
    """Normalize a ``du -h`` size token (e.g. '4.0K', '1.5G', '512') to bytes.

    This is the "mathematical normalization" step: diverse human-readable units
    become a single integer baseline so sizes can be summed and sorted.
    """
    match = _SIZE_RE.match(token or '')
    if not match:
        return 0
    number = float(match.group(1).replace(',', '.'))
    unit = (match.group(2) or '').upper()
    return int(round(number * _UNIT_FACTORS.get(unit, 1)))


# awk that rolls a list of paths into per-directory recursive counts at the
# reporting depth (same tab format as du). Shared by the exact directory-count
# pass here and the file-count baseline in :mod:`storage_map.lib.baseline`:
# given ``a`` (the argument's path-segment count) and ``r`` (relative report
# depth), each input path bumps every ancestor prefix from depth ``a`` to
# ``a+r``. ``-F/`` makes ``NF-1`` the path's segment count; the empty key (mount
# root ``/``) is normalized to ``/``.
ROLLUP_AWK = (
    '{segs=NF-1; maxd=a+r; '
    'for(d=a; d<=maxd && d<=segs; d++){key=""; '
    'for(i=2;i<=d+1;i++)key=key"/"$i; if(key=="")key="/"; c[key]++}} '
    'END{for(k in c)printf "%d\\t%s\\n", c[k], k}'
)


class Node:
    """A folder in the hierarchy with its byte size and children.

    ``count`` is the recursive inode count from the ``du --inodes`` pass (files
    plus subdirectories); ``dir_count`` is the recursive directory count from the
    ``find -type d`` pass. Their difference is the exact recursive *file* count.
    Both are ``None`` for logs written before those passes existed, so consumers
    must treat a missing value as "unknown" rather than zero.
    """

    __slots__ = ('path', 'name', 'size', 'count', 'dir_count', 'children')

    def __init__(self, path, size, count=None, dir_count=None):
        self.path = path
        self.name = posixpath.basename(path.rstrip('/')) or path
        self.size = size
        self.count = count
        self.dir_count = dir_count
        self.children = []

    def sorted_children(self):
        return sorted(self.children, key=lambda n: n.size, reverse=True)


class MountTree:
    """One mount hierarchy: top layer, plus one extra level in shared-data."""

    def __init__(self, mount, root, capacity_bytes=None, used_bytes=None,
                 free_bytes=None):
        self.mount = mount
        self.root = root
        self.capacity_bytes = capacity_bytes
        self.used_bytes = used_bytes
        self.free_bytes = free_bytes

    @property
    def total(self):
        return self.root.size if self.root else 0

    @property
    def has_capacity(self):
        return bool(self.capacity_bytes and self.free_bytes is not None)

    @property
    def free_percent(self):
        capacity_bytes = self.capacity_bytes
        free_bytes = self.free_bytes
        if not capacity_bytes or free_bytes is None:
            return None
        return free_bytes / capacity_bytes * 100

    @property
    def used_percent(self):
        free_percent = self.free_percent
        if free_percent is None:
            return None
        return 100 - free_percent


class ScanResult:
    """Parsed contents of a single server's raw log."""

    def __init__(self, server, host, generated_at, depth, mounts):
        self.server = server
        self.host = host
        self.generated_at = generated_at
        self.depth = depth
        self.mounts = mounts  # list[MountTree]

    @property
    def total(self):
        return sum(m.total for m in self.mounts)


def parse_raw_log(path, server_name=None):
    """Read a raw log file and build a :class:`ScanResult`."""
    host = generated_at = ''
    depth = 0
    sections = []  # list[dict(mount, entries, capacity_bytes, used_bytes, free_bytes)]
    current = None

    with open(path, encoding='utf-8', errors='replace') as fh:
        for line in fh:
            line = line.rstrip('\n')
            # Section markers also start with '#', so match them before the
            # generic comment-header branch below.
            marker = re.match(r'#####\s*MOUNT:\s*(.+?)\s*#####$', line)
            if marker:
                current = {
                    'mount': marker.group(1).strip(),
                    'entries': [],
                    'inode_entries': [],
                    'dir_entries': [],
                    'mode': 'bytes',
                    'capacity_bytes': None,
                    'used_bytes': None,
                    'free_bytes': None,
                }
                sections.append(current)
                continue
            if line.strip() == '##### INODES #####':
                if current is not None:
                    current['mode'] = 'inodes'
                continue
            if line.strip() == '##### DIRS #####':
                if current is not None:
                    current['mode'] = 'dirs'
                continue
            df_marker = re.match(
                r'#####\s*DF:\s*(\d+)\s+(\d+)\s+(\d+)\s+([0-9]+%?)\s*#####$',
                line)
            if df_marker and current is not None:
                current['capacity_bytes'] = int(df_marker.group(1))
                current['used_bytes'] = int(df_marker.group(2))
                current['free_bytes'] = int(df_marker.group(3))
                continue
            if line.strip() == '##### END #####':
                continue
            if line.startswith('#'):
                if line.startswith('# server:'):
                    host = line.split(':', 1)[1].strip()
                elif line.startswith('# generated_at:'):
                    generated_at = line.split(':', 1)[1].strip()
                elif line.startswith('# depth:'):
                    depth = _safe_int(line.split(':', 1)[1], 0)
                continue
            if not line.strip():
                continue
            if current is None:
                continue
            # du lines are "<size>\t<path>" (tab); tolerate spaces as a fallback.
            parts = line.split('\t', 1)
            if len(parts) != 2:
                parts = line.split(None, 1)
            if len(parts) != 2:
                continue
            size_token, entry_path = parts[0].strip(), parts[1].strip()
            # ``du``/``find`` emit "<count>\t<path>"; reuse parse_size to tolerate
            # any thousands-formatting, though counts are integers.
            bucket = {'inodes': 'inode_entries', 'dirs': 'dir_entries'}.get(
                current['mode'], 'entries')
            current[bucket].append((parse_size(size_token), entry_path))

    effective_depth = (SHARED_DATA_DEPTH if depth <= 0
                       else min(depth, SHARED_DATA_DEPTH))
    mounts = []
    for sec in sections:
        root = _build_tree(sec['entries'], sec['mount'])
        _apply_counts(root, sec['inode_entries'], 'count')
        _apply_counts(root, sec['dir_entries'], 'dir_count')
        _limit_tree_depth(root, effective_depth)
        mounts.append(MountTree(sec['mount'], root,
                                sec['capacity_bytes'], sec['used_bytes'],
                                sec['free_bytes']))
    name = server_name or (host.split('.')[0] if host else os.path.basename(path))
    return ScanResult(name, host, generated_at, effective_depth, mounts)


def _build_tree(entries, mount):
    """Turn flat ``(size, path)`` du entries into a nested tree under ``mount``."""
    def norm(p):
        p = p.rstrip('/')
        return p or '/'

    mount_key = norm(mount)
    nodes = {}
    root = Node(mount_key, 0)
    nodes[mount_key] = root

    # Shallowest paths first so a parent always exists before its child.
    for size, entry in sorted(entries, key=lambda sp: sp[1].count('/')):
        key = norm(entry)
        if key == mount_key:
            root.size = size
            continue
        node = nodes.get(key)
        if node is None:
            node = Node(key, size)
            nodes[key] = node
            parent = nodes.get(norm(posixpath.dirname(key)), root)
            parent.children.append(node)
        else:
            node.size = size
    return root


def _apply_counts(root, count_entries, attr):
    """Overlay recursive counts (inodes or dirs) onto the byte tree by path.

    Counts are only set on nodes that already exist in the byte tree; the byte
    ``du`` and the count passes walk the same tree at the same depth, so the
    paths line up. Nodes with no matching line keep ``attr`` at ``None``.
    """
    def norm(p):
        p = p.rstrip('/')
        return p or '/'

    by_path = {}
    stack = [root]
    while stack:
        node = stack.pop()
        by_path[node.path] = node
        stack.extend(node.children)

    for count, entry in count_entries:
        node = by_path.get(norm(entry))
        if node is not None:
            setattr(node, attr, int(count))


def _limit_tree_depth(node, max_depth, depth=0):
    """Keep top-level dirs, plus one extra level inside shared-data."""
    path_limit = _visible_depth_limit(node.path, max_depth)
    if depth >= path_limit:
        node.children = []
        return
    for child in node.children:
        _limit_tree_depth(child, max_depth, depth + 1)


def _visible_depth_limit(path, max_depth):
    limit = min(max_depth, SHARED_DATA_DEPTH)
    parts = [p for p in (path or '').strip('/').split('/') if p]
    if SHARED_DATA_DIR in parts:
        return limit
    return min(limit, TOP_LAYER_DEPTH)


# --------------------------------------------------------------------------- #
# Stage 2 — Visualizers                                                       #
# --------------------------------------------------------------------------- #
def human(num_bytes):
    """Bytes -> compact binary string (e.g. '1.4 TiB')."""
    value = float(num_bytes)
    for unit in ('B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB'):
        if value < 1024 or unit == 'PiB':
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} PiB"


def _size_style(num_bytes):
    if num_bytes >= 1024**4:        # >= 1 TiB
        return 'bold red'
    if num_bytes >= 100 * 1024**3:  # >= 100 GiB
        return 'yellow'
    return 'green'


def _load_results(smcfg, servers):
    """Load the latest local raw log for each server, skipping missing ones."""
    results = []
    for srv in servers:
        path = _latest_path(smcfg, srv.name)
        if not os.path.exists(path):
            print(f"[VIEW] No local raw log for {srv.name} "
                  f"(expected {path}). Run 'fetch' first.")
            continue
        results.append(parse_raw_log(path, server_name=srv.name))
    return results


def view(smcfg, servers, top=15):
    """Render a Rich terminal dashboard from local raw logs."""
    results = _load_results(smcfg, servers)
    if not results:
        return 1
    if not _HAVE_RICH:
        _view_plain(results, top)
        print("\n[VIEW] Install 'rich' for the color dashboard: pip install rich")
        return 0

    console = Console()
    for res in results:
        console.rule(f"[bold cyan]{res.server}[/]  ({res.host or 'unknown host'})")
        summary = Table(title=f"Mount usage — scanned {res.generated_at or 'n/a'}",
                        title_style="dim")
        summary.add_column("Mount", style="bold")
        summary.add_column("Used", justify="right")
        summary.add_column("Free", justify="right")
        summary.add_column("Left", justify="right")
        grand = res.total or 1
        for mt in sorted(res.mounts, key=lambda m: m.total, reverse=True):
            if mt.has_capacity:
                used_bytes = mt.used_bytes
                if used_bytes is None:
                    used_bytes = max(0, mt.capacity_bytes - mt.free_bytes)
                used_text = human(used_bytes)
                free_text = human(mt.free_bytes)
                pct_text = f"{mt.free_percent:5.1f}%"
            else:
                used_text = human(mt.total)
                free_text = "n/a"
                pct_text = f"{mt.total / grand * 100:5.1f}% of scanned"
            summary.add_row(mt.mount,
                            f"[{_size_style(mt.total)}]{used_text}[/]",
                            free_text,
                            pct_text)
        console.print(summary)

        for mt in sorted(res.mounts, key=lambda m: m.total, reverse=True):
            tree = Tree(
                f"[{_size_style(mt.total)}]{mt.mount}[/]  "
                f"[dim]{human(mt.total)}[/]")
            _rich_children(tree, mt.root, mt.total or 1, top)
            console.print(tree)
        console.print()
    return 0


def _rich_children(tree_node, folder, mount_total, top):
    for child in folder.sorted_children()[:top]:
        pct = child.size / mount_total * 100
        branch = tree_node.add(
            f"[{_size_style(child.size)}]{child.name}[/]  "
            f"[dim]{human(child.size)} ({pct:.1f}%)[/]")
        _rich_children(branch, child, mount_total, top)


def _view_plain(results, top):
    """Plain-text fallback used when Rich is not installed."""
    for res in results:
        print(f"\n=== {res.server} ({res.host}) — scanned {res.generated_at} ===")
        for mt in sorted(res.mounts, key=lambda m: m.total, reverse=True):
            print(f"  {mt.mount}: {human(mt.total)}")
            _plain_children(mt.root, top, indent=2)


def _plain_children(folder, top, indent):
    for child in folder.sorted_children()[:top]:
        print(f"{'  ' * indent}- {child.name}: {human(child.size)}")
        _plain_children(child, top, indent + 1)


# Categorical hue per top-level mount (validated data-viz palette). Area encodes
# magnitude; color encodes *identity* (which mount), so a mount and all its
# descendants share one hue and the drill-down stays visually coherent.
MOUNT_HUES = ['#2a78d6', '#1baf7a', '#eda100', '#008300',
              '#4a3aa7', '#e34948', '#e87ba4', '#eb6834']
_SERVER_HUE = '#52514e'  # neutral ink for the per-server root tile


def build_treemap_figure(results):
    """Build the interactive Plotly treemap :class:`~plotly.graph_objects.Figure`.

    Shared by ``treemap`` (standalone HTML) and the dashboard so both render an
    identical, consistently-coloured drill-down. Returns ``None`` when Plotly is
    unavailable so callers can degrade gracefully.
    """
    if not _HAVE_PLOTLY:
        return None

    ids, labels, parents, values, customdata, colors = [], [], [], [], [], []

    def add(node_id, label, parent_id, size, color):
        ids.append(node_id)
        labels.append(label)
        parents.append(parent_id)
        values.append(size)
        customdata.append(human(size))
        colors.append(color)

    def walk(server_id, folder, parent_id, color, label=None):
        # branchvalues='total' requires parent >= sum(children); du rounding can
        # violate that by a hair, so reconcile upward.
        child_sum = 0
        node_id = f"{server_id}\x1f{folder.path}"
        add(node_id, label or folder.name, parent_id, max(folder.size, 0), color)
        for child in folder.children:
            child_sum += walk(server_id, child, node_id, color)
        reconciled = max(folder.size, child_sum)
        values[ids.index(node_id)] = reconciled
        return reconciled

    for res in results:
        server_id = f"srv\x1f{res.server}"
        add(server_id, res.server, "", 0, _SERVER_HUE)
        server_total = 0
        for idx, mt in enumerate(res.mounts):
            hue = MOUNT_HUES[idx % len(MOUNT_HUES)]
            # Label the mount root with its full path (e.g. '/strg/E'), not just
            # the basename, so mounts are recognizable in the treemap.
            server_total += walk(server_id, mt.root, server_id, hue, label=mt.mount)
        values[ids.index(server_id)] = max(server_total, 1)

    fig = go.Figure(go.Treemap(
        ids=ids, labels=labels, parents=parents, values=values,
        customdata=customdata, branchvalues='total',
        marker=dict(colors=colors, line=dict(width=1, color='rgba(255,255,255,0.28)')),
        tiling=dict(pad=2),
        textfont=dict(color='#ffffff', size=13,
                      family='system-ui, -apple-system, "Segoe UI", sans-serif'),
        texttemplate="%{label}<br>%{customdata}",
        hovertemplate="%{label}<br>%{customdata}<extra></extra>",
        maxdepth=3,
        pathbar=dict(visible=True, textfont=dict(color='#898781', size=13)),
    ))
    fig.update_layout(
        margin=dict(t=8, l=8, r=8, b=8),
        paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
        height=560,
    )
    return fig


def treemap(smcfg, servers, open_html=False):
    """Build an interactive Plotly treemap HTML from local raw logs."""
    results = _load_results(smcfg, servers)
    if not results:
        return 1
    fig = build_treemap_figure(results)
    if fig is None:
        print("[TREEMAP] Plotly is not installed. Run: pip install plotly")
        return 1

    servers_label = ', '.join(r.server for r in results)
    fig.update_layout(
        title=f"Storage Map — {servers_label}  "
              f"(scanned {results[0].generated_at or 'n/a'})",
        margin=dict(t=60, l=10, r=10, b=10))

    os.makedirs(smcfg.output_dir, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    tag = _safe_log_token(results[0].server) if len(results) == 1 else 'all'
    out_html = os.path.join(smcfg.output_dir, f'{tag}_treemap_{ts}.html')
    fig.write_html(out_html, include_plotlyjs='cdn')
    print(f"[TREEMAP] Interactive treemap written -> {out_html}")
    if open_html and os.name == 'nt':
        os.startfile(out_html)  # noqa: S606 - opening our own generated report
    return 0


def dashboard(smcfg, servers, open_html=False, notifier=None):
    """Build the self-contained HTML dashboard from local raw logs."""
    from .dashboard import render_dashboard

    results = _load_results(smcfg, servers)
    if not results:
        send_best_effort(
            notifier,
            format_storage_dashboard_summary([s.name for s in servers], failed=True))
        return 1
    fig = build_treemap_figure(results)  # None if Plotly is absent -> no treemap
    out_html = render_dashboard(results, fig, smcfg.dashboard_dir)
    print(f"[DASHBOARD] Storage dashboard written -> {out_html}")
    if open_html and os.name == 'nt':
        os.startfile(out_html)  # noqa: S606 - opening our own generated report
    send_best_effort(
        notifier,
        format_storage_dashboard_summary([r.server for r in results], out_html))
    return 0


# --------------------------------------------------------------------------- #
# CLI                                                                         #
# --------------------------------------------------------------------------- #
def _select_servers(smcfg, requested):
    if not requested or requested == 'all':
        return smcfg.servers
    chosen = []
    for name in _config_list(requested):
        srv = smcfg.server(name)
        if srv is None:
            raise RuntimeError(f"Unknown server '{name}'. Configured: "
                               f"{', '.join(s.name for s in smcfg.servers)}")
        chosen.append(srv)
    return chosen


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog='storage_map',
        description="Storage Map & Analytics — scan lab servers' disk usage "
                    "(fire-and-forget) then fetch and visualize later.")
    parser.add_argument('--server', default='all',
                        help="Comma-separated server name(s) from config, or "
                             "'all' (default).")
    sub = parser.add_subparsers(dest='command')

    sub.add_parser('scan', help="Stage 1: launch background scans, then exit.")
    sub.add_parser('status', help="Check whether remote scans have finished.")

    p_fetch = sub.add_parser('fetch', help="SCP finished raw logs locally.")
    p_fetch.add_argument('--view', action='store_true',
                         help="Render the Rich dashboard after fetching.")
    p_fetch.add_argument('--treemap', action='store_true',
                         help="Build the Plotly HTML treemap after fetching.")
    p_fetch.add_argument('--dashboard', action='store_true',
                         help="Build the full HTML dashboard after fetching.")
    p_fetch.add_argument('--top', type=int, default=15)
    p_fetch.add_argument('--open', action='store_true',
                         help="Open the generated HTML report(s).")

    p_view = sub.add_parser('view', help="Stage 2: Rich terminal dashboard.")
    p_view.add_argument('--top', type=int, default=15)

    p_tree = sub.add_parser('treemap', help="Stage 2: Plotly HTML treemap.")
    p_tree.add_argument('--open', action='store_true')

    p_dash = sub.add_parser('dashboard',
                            help="Stage 2: build the self-contained HTML dashboard.")
    p_dash.add_argument('--open', action='store_true',
                        help="Open the generated dashboard in the browser.")

    args = parser.parse_args(argv)

    cfg = ConfigManager()
    smcfg = load_storage_map_config(cfg)
    servers = _select_servers(smcfg, args.server)
    notifier = TelegramNotifier.from_config(cfg)

    command = args.command or 'scan'  # default action for Task Scheduler
    if command == 'scan':
        return scan(smcfg, servers, notifier=notifier)
    if command == 'status':
        return status(smcfg, servers, notifier=notifier)
    if command == 'fetch':
        return fetch(smcfg, servers, do_view=args.view, do_treemap=args.treemap,
                     do_dashboard=args.dashboard, top=args.top,
                     open_html=args.open, notifier=notifier)
    if command == 'view':
        return view(smcfg, servers, top=args.top)
    if command == 'treemap':
        return treemap(smcfg, servers, open_html=args.open)
    if command == 'dashboard':
        return dashboard(smcfg, servers, open_html=args.open, notifier=notifier)
    parser.print_help()
    return 2
