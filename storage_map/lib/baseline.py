"""Exact per-directory file-count baseline (``find -type f``).

The routine scan uses ``du --inodes`` (fast, but counts subdirectories too, so
coverage compares within a 2% tolerance). For directories that are mostly
static once archived, that tolerance is looser than it needs to be. This module
produces a **one-time, 100%-accurate** baseline with ``find -type f`` and stores
it in the repository (:data:`BASELINE_PATH`). Coverage then prefers the exact
baseline count where one exists and falls back to ``du --inodes`` for anything
the baseline does not cover (see :func:`storage_map.webapp.coverage._row_status`).

Generation mirrors the ``du`` scan flow in :mod:`storage_map.lib.core`: a
fire-and-forget remote ``find`` writes a sentinel-guarded output file, then
:func:`collect_baseline` scp's and parses it. The remote command emits one
``<recursive_file_count>\\t<path>`` line per directory at the same reporting
depth the ``du`` scan uses, so the byte scan and the baseline line up path for
path.
"""
import json
import os
import shlex
from datetime import datetime

from src.paths import _safe_log_token
from src.remote_transport import _scp_fetch_file, _ssh_run

from .core import (
    PROJECT_ROOT,
    ROLLUP_AWK,
    SHARED_DATA_DEPTH,
    TOP_LAYER_DEPTH,
    _shared_data_child,
    _safe_int,
    path_segments,
)

# Committed in the repo: it is small (one line per directory prefix) and static
# between deliberate re-baselines, so it belongs in version control next to the
# code that reads it.
BASELINE_PATH = os.path.join(PROJECT_ROOT, 'storage_map', 'coverage_baseline.json')

# Kept separate from the du scan's REMOTE_SCAN_DIR so a baseline run and a
# routine scan never clobber each other's output/sentinel.
REMOTE_BASELINE_DIR = '.storage_map/baseline'

def _find_line(pre, target, arg_segs, rel_depth, err):
    """One ``find -type f | awk`` pipeline rolling files up per directory.

    Emits ``%h`` (each file's parent dir); the shared :data:`ROLLUP_AWK` bumps
    every ancestor prefix, giving exact recursive file counts per directory.
    """
    qt = shlex.quote(target)
    return (
        f'{pre}find {qt} -xdev -type f '
        f"-not -path '/proc/*' -not -path '/sys/*' -printf '%h\\n' "
        f'2>>{err} | awk -F/ -v a={arg_segs} -v r={rel_depth} '
        f'{shlex.quote(ROLLUP_AWK)} 2>>{err} || true'
    )


def _remote_baseline_script(server, depth):
    """Shell script (run.sh) that writes the exact-count baseline remotely."""
    q = shlex.quote
    depth = max(TOP_LAYER_DEPTH,
                min(_safe_int(depth, SHARED_DATA_DEPTH), SHARED_DATA_DEPTH))
    err = '"$DIR/baseline.err"'
    lines = [
        'DIR="$HOME/' + REMOTE_BASELINE_DIR + '"',
        'mkdir -p "$DIR"',
        'rm -f "$DIR/baseline.out" "$DIR/baseline.err" "$DIR/baseline.sentinel"',
        # Same yield-to-real-work priority prefix as the du scan.
        'PRE=""',
        'command -v nice >/dev/null 2>&1 && PRE="nice -n19"',
        'command -v ionice >/dev/null 2>&1 && PRE="ionice -c3 $PRE"',
        '{',
        r'printf "# storage-map exact-count baseline\n"',
        'printf "# server: %s\\n" ' + q(server.host),
        'printf "# generated_at: %s\\n" "$(date -Iseconds 2>/dev/null || date)"',
        'printf "# method: find -type f\\n"',
    ]
    for mount in server.mounts:
        lines.append('printf "##### MOUNT: %s #####\\n" ' + q(mount))
        lines.append(_find_line(
            '$PRE ', mount, path_segments(mount), TOP_LAYER_DEPTH, err))
        if depth >= SHARED_DATA_DEPTH:
            child = _shared_data_child(mount)
            lines.append(
                f'if [ -d {q(child)} ]; then '
                + _find_line('$PRE ', child, path_segments(child),
                             TOP_LAYER_DEPTH, err)
                + '; fi')
    lines.append(r'printf "##### END #####\n"')
    lines.append('} > "$DIR/baseline.out" 2>>"$DIR/baseline.err"')
    lines.append('echo DONE > "$DIR/baseline.sentinel"')
    return '\n'.join(lines)


def _remote_baseline_launch_command(server, depth):
    """Write run.sh remotely then detach it from the SSH session."""
    q = shlex.quote
    script = _remote_baseline_script(server, depth)
    run_sh = REMOTE_BASELINE_DIR + '/run.sh'
    return (
        f'mkdir -p {q(REMOTE_BASELINE_DIR)} && '
        f'cat > {q(run_sh)} <<\'__SM_EOF__\'\n'
        f'{script}\n'
        f'__SM_EOF__\n'
        f'chmod +x {q(run_sh)}\n'
        f'if command -v setsid >/dev/null 2>&1; then '
        f'setsid sh {q(run_sh)} </dev/null >/dev/null 2>&1 & '
        f'else nohup sh {q(run_sh)} </dev/null >/dev/null 2>&1 & fi\n'
        f'echo LAUNCHED'
    )


def launch_baseline(smcfg, servers):
    """Fire-and-forget: launch the exact-count ``find`` on each server.

    One-time and deliberate. Do not run it while an archive/backup is fetching
    from the same server — ``find`` walks every inode and competes for metadata
    I/O even at ``ionice -c3``.
    """
    failures = 0
    for srv in servers:
        print(f"[BASELINE] Launching exact find-count on {srv.name} "
              f"({srv.host})...")
        cmd = _remote_baseline_launch_command(srv, smcfg.depth)
        result = _ssh_run(srv.user, srv.host, cmd,
                          password=srv.password, timeout=120)
        if result.returncode != 0 or 'LAUNCHED' not in (result.stdout or ''):
            failures += 1
            err = (result.stderr or '').strip() or (result.stdout or '').strip()
            print(f"[BASELINE] FAILED to launch on {srv.name}: {err}")
            continue
        print(f"[BASELINE] {srv.name}: find running detached. Collect later.")
    return 1 if failures else 0


def _baseline_status(server):
    """'DONE', 'PENDING', 'MISSING', or 'UNREACHABLE' for a server's baseline."""
    sentinel = f'$HOME/{REMOTE_BASELINE_DIR}/baseline.sentinel'
    run_sh = f'$HOME/{REMOTE_BASELINE_DIR}/run.sh'
    cmd = (f'if [ -f "{sentinel}" ]; then echo DONE; '
           f'elif [ -f "{run_sh}" ]; then echo PENDING; '
           f'else echo MISSING; fi')
    result = _ssh_run(server.user, server.host, cmd,
                      password=server.password, timeout=60)
    out = (result.stdout or '').strip().splitlines()
    if result.returncode != 0 or not out:
        return 'UNREACHABLE'
    return out[-1].strip()


def parse_baseline_counts(text):
    """Parse ``<count>\\t<path>`` lines into ``{path: file_count}``.

    Comment/marker lines (``#``/``#####``) are ignored, matching the raw-log
    format the du scan uses.
    """
    counts = {}
    for line in (text or '').splitlines():
        line = line.rstrip('\n')
        if not line.strip() or line.startswith('#'):
            continue
        parts = line.split('\t', 1)
        if len(parts) != 2:
            parts = line.split(None, 1)
        if len(parts) != 2:
            continue
        token, path = parts[0].strip(), parts[1].strip()
        try:
            counts[path.rstrip('/') or '/'] = int(token)
        except ValueError:
            continue
    return counts


def load_baseline(path=None):
    """Return ``{server_name: {path: file_count}}`` from the baseline file.

    Missing or malformed files yield an empty mapping so coverage degrades to
    the ``du --inodes`` / byte rules rather than failing.
    """
    path = path or BASELINE_PATH
    try:
        with open(path, encoding='utf-8') as fh:
            data = json.load(fh)
    except (OSError, ValueError):
        return {}
    out = {}
    for name, entry in (data.get('servers') or {}).items():
        counts = (entry or {}).get('counts') or {}
        out[name] = {p: int(c) for p, c in counts.items()}
    return out


def write_baseline(server_counts, path=None, generated_at=None):
    """Persist ``{server_name: {path: count}}`` to the baseline JSON file."""
    path = path or BASELINE_PATH
    payload = {
        'method': 'find -type f',
        'generated_at': generated_at or datetime.now().isoformat(
            timespec='seconds'),
        'note': ('Exact per-directory recursive file counts. Regenerate with '
                 'the coverage baseline job during a quiet window; coverage '
                 'prefers these over du --inodes.'),
        'servers': {
            name: {
                'generated_at': datetime.now().isoformat(timespec='seconds'),
                'counts': counts,
            }
            for name, counts in server_counts.items()
        },
    }
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as fh:
        json.dump(payload, fh, indent=1, sort_keys=True)
        fh.write('\n')
    return path


def collect_baseline(smcfg, servers, path=None):
    """Fetch finished baseline output, merge with any prior file, and write it.

    Servers whose ``find`` is still running (or unreachable) keep their existing
    baseline entry, so collecting one server never drops another's counts.
    """
    os.makedirs(smcfg.output_dir, exist_ok=True)
    merged = {}
    prior_raw = {}
    try:
        with open(path or BASELINE_PATH, encoding='utf-8') as fh:
            prior_raw = (json.load(fh).get('servers') or {})
    except (OSError, ValueError):
        prior_raw = {}
    for name, entry in prior_raw.items():
        merged[name] = dict((entry or {}).get('counts') or {})

    fetched, skipped, failed = [], [], []
    for srv in servers:
        state = _baseline_status(srv)
        if state != 'DONE':
            skipped.append(f"{srv.name}({state})")
            print(f"[BASELINE] {srv.name}: not finished (status={state}); "
                  f"skipping.")
            continue
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        dest = os.path.join(
            smcfg.output_dir, f'{_safe_log_token(srv.name)}_{ts}.baseline')
        rc = _scp_fetch_file(srv.user, srv.host,
                             f'{REMOTE_BASELINE_DIR}/baseline.out', dest,
                             password=srv.password)
        if rc != 0 or not os.path.exists(dest) or os.path.getsize(dest) == 0:
            failed.append(srv.name)
            print(f"[BASELINE] {srv.name}: failed to retrieve (scp rc={rc}).")
            continue
        with open(dest, encoding='utf-8') as fh:
            merged[srv.name] = parse_baseline_counts(fh.read())
        fetched.append(srv.name)
        print(f"[BASELINE] {srv.name}: {len(merged[srv.name]):,} directory "
              f"counts recorded.")

    if fetched:
        write_baseline(merged, path)
    else:
        print("[BASELINE] No completed baseline runs were retrieved.")
    return {'fetched': fetched, 'skipped': skipped, 'failed': failed}
