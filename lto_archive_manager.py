import os
import re
import sys
import time
import queue
import signal
import shutil
import hashlib
import zipfile
import sqlite3
import threading
import configparser
import subprocess
import tempfile
import shlex
import posixpath
import atexit
from datetime import datetime
from collections import defaultdict

try:
    import psutil
except ImportError:  # optional dependency — priority/affinity degrade gracefully
    psutil = None

# ==============================================================================
# LTO ARCHIVE MANAGEMENT SYSTEM
# ==============================================================================

BUFFER_SIZE = 1024 * 1024 * 16  # 16 MB read buffer
CONFIG_FILE  = "config.ini"
LTFS_DIR     = r'C:\Program Files\IBM\LTFS'  # IBM LTFS tools must run from this directory
APP_DIR      = os.path.dirname(os.path.abspath(__file__))
BACKUP_LOG_DIR = os.path.join(APP_DIR, 'backup_logs')
LOCAL_TAPE_BUDGET_BYTES = int(11.5 * 1000**4)
ROOT_FILES_GROUP = "_ROOT_FILES"
AUTO_PACK_FILE_RATIO = 0.30
AUTO_PACK_MIN_SMALL_BYTES = 1 * 1024**3
AUTO_PACK_MIN_SMALL_BYTE_RATIO = 0.01


def _auto_pack_decision(total_files, total_bytes, small_files, small_bytes):
    file_ratio = (small_files / total_files) if total_files else 0.0
    byte_ratio = (small_bytes / total_bytes) if total_bytes else 0.0
    meaningful_size = (
        small_bytes >= AUTO_PACK_MIN_SMALL_BYTES or
        byte_ratio >= AUTO_PACK_MIN_SMALL_BYTE_RATIO
    )
    should_pack = file_ratio > AUTO_PACK_FILE_RATIO and meaningful_size
    return should_pack, file_ratio, byte_ratio


# ==============================================================================
# RUNTIME INFRASTRUCTURE — status output, cancellation, CPU priority/affinity
# These helpers are shared by the streaming pipeline so the user always sees
# which phase is active and can stop the job cleanly with Ctrl+C.
# ==============================================================================

def _ts():
    """Wall-clock timestamp prefix for status lines."""
    return time.strftime('%H:%M:%S')


_PRINT_LOCK = threading.Lock()
_PROGRESS_ACTIVE = False


def _fmt_eta(seconds):
    if seconds is None or seconds < 0:
        return "--:--"
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h:d}:{m:02d}:{s:02d}"
    return f"{m:02d}:{s:02d}"


def _fmt_bytes(num):
    try:
        value = float(num or 0)
    except (TypeError, ValueError):
        value = 0.0
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    for unit in units:
        if abs(value) < 1024.0 or unit == units[-1]:
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.2f} {unit}"
        value /= 1024.0


def _safe_log_token(value, default='item'):
    text = str(value or '').strip()
    if text:
        text = os.path.basename(os.path.normpath(text))
    text = re.sub(r'[^A-Za-z0-9._-]+', '_', text or default)
    text = text.strip('._-')
    return (text or default)[:80]


def _unique_path(path):
    if not os.path.exists(path):
        return path
    root, ext = os.path.splitext(path)
    for idx in range(2, 1000):
        candidate = f"{root}_{idx}{ext}"
        if not os.path.exists(candidate):
            return candidate
    return f"{root}_{int(time.time() * 1000)}{ext}"


def _progress_line(text):
    """Render a live progress line without fighting normal status output."""
    global _PROGRESS_ACTIVE
    with _PRINT_LOCK:
        _PROGRESS_ACTIVE = True
        sys.stdout.write("\r" + text + "   ")
        sys.stdout.flush()


def _progress_done():
    """Terminate the current live progress line, if one is active."""
    global _PROGRESS_ACTIVE
    with _PRINT_LOCK:
        if _PROGRESS_ACTIVE:
            sys.stdout.write("\n")
            sys.stdout.flush()
            _PROGRESS_ACTIVE = False


def _phase(tag, msg):
    """Print a prominent phase-transition banner (e.g. SSH -> FETCH -> TAPE)."""
    global _PROGRESS_ACTIVE
    with _PRINT_LOCK:
        if _PROGRESS_ACTIVE:
            sys.stdout.write("\n")
            _PROGRESS_ACTIVE = False
        print(f"\n[{_ts()}] ===== {tag}: {msg} =====")
        sys.stdout.flush()


def _status(tag, msg):
    """Print a timestamped one-line status update."""
    global _PROGRESS_ACTIVE
    with _PRINT_LOCK:
        if _PROGRESS_ACTIVE:
            sys.stdout.write("\n")
            _PROGRESS_ACTIVE = False
        print(f"[{_ts()}] [{tag}] {msg}")
        sys.stdout.flush()


# --- Graceful cancellation (Ctrl+C) -------------------------------------------

CANCEL = threading.Event()        # set by the SIGINT handler; polled by workers
_PROCS = set()                    # live child Popen objects we may need to kill
# RLock (not Lock): the SIGINT handler runs in the main thread and may fire while
# that thread is already inside register/unregister, so it must be re-entrant.
_PROCS_LOCK = threading.RLock()
_TAPE_IO_LOCK = threading.RLock()
_SIGNAL_INSTALLED = False
_PREV_SIGINT = None
_PREV_SIGBREAK = None
_PERF_WARNED = False


def _acquire_tape_io_lock(reason):
    """Serialize in-process LTFS reads/writes so robocopy owns the tape alone."""
    if _TAPE_IO_LOCK.acquire(blocking=False):
        return
    _status('TAPE', f"Waiting for exclusive tape access: {reason}")
    _TAPE_IO_LOCK.acquire()


def _release_tape_io_lock():
    _TAPE_IO_LOCK.release()


def register_proc(proc):
    """Track a live subprocess so Ctrl+C can terminate it (and its children)."""
    if proc is not None:
        with _PROCS_LOCK:
            _PROCS.add(proc)


def unregister_proc(proc):
    with _PROCS_LOCK:
        _PROCS.discard(proc)


def _kill_proc_tree(proc):
    """Terminate a process and every child it spawned, escalating to kill()."""
    if proc is None:
        return
    if psutil is not None:
        try:
            parent = psutil.Process(proc.pid)
            targets = parent.children(recursive=True) + [parent]
        except psutil.Error:
            targets = []
        if targets:
            for p in targets:
                try:
                    p.terminate()
                except psutil.Error:
                    pass
            _, alive = psutil.wait_procs(targets, timeout=3)
            for p in alive:
                try:
                    p.kill()
                except psutil.Error:
                    pass
            return
    # Fallback when psutil is unavailable
    try:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except Exception:
            proc.kill()
    except Exception:
        pass


def _terminate_all_procs():
    with _PROCS_LOCK:
        procs = list(_PROCS)
        _PROCS.clear()
    for p in procs:
        _kill_proc_tree(p)


def _cancel_handler(signum, frame):
    """First Ctrl+C: cancel gracefully + kill active transfers. Second: hard exit."""
    if CANCEL.is_set():
        print("\n[ABORTED] Second interrupt — forcing immediate exit.")
        _terminate_all_procs()
        raise KeyboardInterrupt
    CANCEL.set()
    print("\n\n[STOP] Cancellation requested — stopping safely "
          "(terminating active transfers).")
    print("[STOP] The session is saved; re-run to resume. "
          "Press Ctrl+C again to force-quit.")
    _terminate_all_procs()


def install_cancel_handler():
    """Install SIGINT/SIGBREAK handlers for graceful cancellation (idempotent)."""
    global _SIGNAL_INSTALLED, _PREV_SIGINT, _PREV_SIGBREAK
    if _SIGNAL_INSTALLED:
        return
    try:
        _PREV_SIGINT = signal.signal(signal.SIGINT, _cancel_handler)
    except (ValueError, OSError):
        return  # not on the main thread / unsupported
    if hasattr(signal, 'SIGBREAK'):
        try:
            _PREV_SIGBREAK = signal.signal(signal.SIGBREAK, _cancel_handler)
        except (ValueError, OSError):
            pass
    _SIGNAL_INSTALLED = True


def uninstall_cancel_handler():
    """Restore the previous SIGINT/SIGBREAK handlers (so menu input() behaves
    normally again after an operation finishes)."""
    global _SIGNAL_INSTALLED, _PREV_SIGINT, _PREV_SIGBREAK
    if not _SIGNAL_INSTALLED:
        return
    try:
        if _PREV_SIGINT is not None:
            signal.signal(signal.SIGINT, _PREV_SIGINT)
    except (ValueError, OSError):
        pass
    if hasattr(signal, 'SIGBREAK') and _PREV_SIGBREAK is not None:
        try:
            signal.signal(signal.SIGBREAK, _PREV_SIGBREAK)
        except (ValueError, OSError):
            pass
    _SIGNAL_INSTALLED = False
    _PREV_SIGINT = None
    _PREV_SIGBREAK = None


def reset_cancel():
    """Clear the cancellation flag at the start of a fresh operation."""
    CANCEL.clear()


# --- CPU priority / affinity (psutil, best-effort, no admin needed for HIGH) ---

def _priority_class(name):
    """Map a config priority name to a Windows psutil priority class constant."""
    if psutil is None:
        return None
    return {
        'realtime': getattr(psutil, 'REALTIME_PRIORITY_CLASS', None),
        'high':     getattr(psutil, 'HIGH_PRIORITY_CLASS', None),
        'normal':   getattr(psutil, 'NORMAL_PRIORITY_CLASS', None),
    }.get((name or 'normal').strip().lower())


def _warn_no_psutil(what):
    global _PERF_WARNED
    if not _PERF_WARNED:
        _status('PERF', f"psutil not installed — skipping {what}. "
                        "Run 'pip install psutil' to enable priority/affinity.")
        _PERF_WARNED = True


def _apply_proc_tuning(proc, priority=None, affinity=None, label=''):
    """Best-effort: set a child process CPU priority class and core affinity.
    Never fatal — logs a [PERF] note and continues (e.g. REALTIME needs admin)."""
    if proc is None:
        return
    if psutil is None:
        if priority or affinity:
            _warn_no_psutil("CPU priority/affinity")
        return
    try:
        ps = psutil.Process(proc.pid)
    except psutil.Error:
        return
    if priority is not None:
        try:
            ps.nice(priority)
        except (psutil.Error, OSError, PermissionError) as e:
            _status('PERF', f"Could not set {label} priority: {e}")
    if affinity:
        try:
            ps.cpu_affinity(list(affinity))
        except (psutil.Error, OSError, ValueError) as e:
            _status('PERF', f"Could not set {label} affinity: {e}")


def _parse_core_list(spec):
    """Parse '0-5' or '0,1,4-7' into a sorted list of logical core indices."""
    cores = set()
    for part in str(spec).split(','):
        part = part.strip()
        if not part:
            continue
        if '-' in part:
            a, b = part.split('-', 1)
            cores.update(range(int(a), int(b) + 1))
        else:
            cores.add(int(part))
    return sorted(cores)


def compute_affinity_sets(spec):
    """Return (producer_cores, consumer_cores), or (None, None) to skip pinning.

    spec='auto' -> consumer (tape writer) gets the last 2 logical cores so it
                   never competes with SSH decryption / Python packing.
    spec='fetch=0-5;tape=6-7' -> explicit split (groups separated by ';').
    spec='off' -> disabled.
    """
    n = os.cpu_count() or 1
    spec = (spec or 'auto').strip().lower()
    if spec in ('off', 'none', 'disabled'):
        return None, None
    if spec in ('', 'auto'):
        if n <= 3:
            return None, None  # too few cores to bother isolating
        return list(range(0, n - 2)), list(range(n - 2, n))
    fetch_cores = tape_cores = None
    for group in spec.split(';'):
        group = group.strip()
        if group.startswith('fetch='):
            fetch_cores = _parse_core_list(group[len('fetch='):])
        elif group.startswith('tape='):
            tape_cores = _parse_core_list(group[len('tape='):])
    if fetch_cores and tape_cores:
        return fetch_cores, tape_cores
    return None, None


def pin_current_process(cores, label='main'):
    """Pin the current (Python) process to a core set — hashing/packing run here."""
    if psutil is None or not cores:
        return
    try:
        psutil.Process().cpu_affinity(list(cores))
        _status('PERF', f"Pinned {label} process to cores {cores}.")
    except (psutil.Error, OSError, ValueError) as e:
        _status('PERF', f"Could not pin {label} process: {e}")


def unpin_current_process():
    """Restore the current process to all cores (after a tuned session)."""
    if psutil is None:
        return
    try:
        psutil.Process().cpu_affinity(list(range(os.cpu_count() or 1)))
    except (psutil.Error, OSError, ValueError):
        pass


def _clean_config_path(value):
    """Return a filesystem path from config text, tolerating optional quotes."""
    value = (value or '').strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
        value = value[1:-1]
    return os.path.normpath(os.path.expandvars(os.path.expanduser(value)))


def _clean_remote_path(value):
    """Return a POSIX remote path from config text, tolerating optional quotes."""
    value = (value or '').strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
        value = value[1:-1]
    value = value.replace('\\', '/').strip()
    return posixpath.normpath(value) if value else ''


def _config_list(value):
    """Parse a newline/comma/semicolon config list without splitting spaces."""
    value = (value or '').replace('\r', '\n')
    items = []
    for line in value.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = re.split(r'[;,]', line) if not line.startswith(('"', "'")) else [line]
        for part in parts:
            part = part.strip()
            if part:
                items.append(part)
    return items


def _load_env_file(path):
    """Parse a simple KEY=VALUE .env file into a dict.

    Keeps secrets (e.g. REMOTE_PASSWORD) out of the git-tracked config.ini.
    Blank lines and '#' comments are ignored; an optional leading 'export ' and
    surrounding quotes are stripped. Missing/unreadable files yield {}."""
    data = {}
    try:
        with open(path, encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#') or '=' not in line:
                    continue
                if line.startswith('export '):
                    line = line[len('export '):]
                key, val = line.split('=', 1)
                key, val = key.strip(), val.strip()
                if len(val) >= 2 and val[0] == val[-1] and val[0] in ('"', "'"):
                    val = val[1:-1]
                data[key] = val
    except OSError:
        pass
    return data


def get_volume_label(drive_path):
    """Detect the volume label of a Windows drive (e.g. 'D:\\')."""
    _acquire_tape_io_lock(f"read volume label {drive_path}")
    try:
        try:
            drive_letter = drive_path.rstrip(":\\/")
            result = subprocess.run(
                ['vol', f'{drive_letter}:'],
                capture_output=True, text=True, shell=True
            )
            for line in result.stdout.splitlines():
                line = line.strip()
                if line.lower().startswith('volume in drive') and ' is ' in line:
                    return line.rsplit(' is ', 1)[-1].strip()
        except Exception:
            pass
        return None
    finally:
        _release_tape_io_lock()


def _hash_file(path):
    """Compute SHA-256 hash of a file, reading in chunks."""
    hasher = hashlib.sha256()
    with open(path, 'rb') as f:
        while True:
            buf = f.read(BUFFER_SIZE)
            if not buf:
                break
            hasher.update(buf)
    return hasher.hexdigest()


def _verify_restored_hash(local_path, record):
    """Verify a restored file against the stored DB hash.
    Runs entirely on local disk after tape transfer is complete — no impact on tape speed."""
    try:
        stored_hash = record['file_hash']
    except (KeyError, IndexError):
        stored_hash = None
    if not stored_hash:
        print(f"[VERIFY] No stored hash for {record['file_name']} — skipping.")
        return
    actual_hash = _hash_file(local_path)
    if actual_hash == stored_hash:
        print(f"[VERIFY] OK  {record['file_name']}")
    else:
        print(f"[VERIFY] FAIL  {record['file_name']}")
        print(f"         expected: {stored_hash}")
        print(f"         got:      {actual_hash}")


def _robocopy_file(src, dst, display_name=None):
    """
    Copy a single file using robocopy with unbuffered I/O.
    Streams live transfer speed and progress to stdout while copying.
    Returns True on success (robocopy exit code < 8).
    """
    src_dir  = os.path.dirname(os.path.abspath(src))
    dst_dir  = os.path.dirname(os.path.abspath(dst))
    filename = os.path.basename(src)
    os.makedirs(dst_dir, exist_ok=True)

    try:
        fsize = os.path.getsize(src)
    except OSError as e:
        print(f"\n[ERROR] Cannot access source file: {src} ({e})")
        return False
    label = display_name or filename
    disp  = (label[:15] + '..' + label[-5:]) if len(label) > 22 else label

    proc = subprocess.Popen(
        ['robocopy', src_dir, dst_dir, filename,
         '/J',    # unbuffered I/O — optimized for large files / tape
         '/IS',   # include same files (always copy)
         '/IT',   # include tweaked files (always copy)
         '/R:3',  # retry 3 times on failure
         '/W:10', # wait 10 s between retries
         '/NP', '/NDL', '/NJH', '/NJS', '/NFL',
        ],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    register_proc(proc)  # so Ctrl+C can terminate a long restore copy

    # Monitor destination file growth to compute live MB/s
    stop_evt = threading.Event()

    def _monitor():
        prev_size = 0
        prev_time = time.time()
        while not stop_evt.is_set():
            time.sleep(0.5)
            try:
                cur_size = os.path.getsize(dst) if os.path.exists(dst) else 0
            except OSError:
                cur_size = 0
            now     = time.time()
            delta_t = now - prev_time
            speed   = ((cur_size - prev_size) / 1024**2) / delta_t if delta_t > 0 else 0
            pct     = (cur_size / fsize * 100) if fsize else 100
            remaining = max(0, fsize - cur_size)
            eta = remaining / (speed * 1024**2) if speed > 0 else None
            _progress_line(
                f"[COPYING] {disp} | {min(pct, 100):.1f}% | "
                f"{speed:.1f} MB/s | ETA {_fmt_eta(eta)}"
            )
            prev_size = cur_size
            prev_time = now

    t = threading.Thread(target=_monitor, daemon=True)
    t.start()
    try:
        proc.wait()
    finally:
        unregister_proc(proc)
    stop_evt.set()
    t.join(timeout=2)
    _progress_done()

    # robocopy exit codes < 8 indicate success (0=nothing done, 1=ok, 2-7=ok+extras)
    return proc.returncode < 8


def _parse_robocopy_bytes(tokens, idx):
    """
    Consume one bytes value from a robocopy summary token list.
    Handles both '4.52 g' (two tokens) and '1234567890' (one token).
    Returns (bytes_int, next_idx).
    """
    if idx >= len(tokens):
        return 0, idx
    val = tokens[idx].replace(',', '')
    idx += 1
    if idx < len(tokens) and tokens[idx].lower() in ('k', 'm', 'g', 't'):
        mult = {'k': 1024, 'm': 1024**2, 'g': 1024**3, 't': 1024**4}[tokens[idx].lower()]
        idx += 1
        try:
            return int(float(val) * mult), idx
        except ValueError:
            return 0, idx
    try:
        return int(float(val)), idx
    except ValueError:
        return 0, idx


def _parse_robocopy_summary(output):
    """
    Parse robocopy's captured stdout and return a dict with:
      files_copied, files_skipped, files_failed,
      bytes_copied, speed_mbs, elapsed
    """
    result = {
        'files_copied': 0, 'files_skipped': 0, 'files_failed': 0,
        'bytes_copied': 0, 'speed_mbs': 0.0, 'elapsed': '',
    }
    output = output or ''
    for line in output.splitlines():
        parts = line.split()
        if not parts:
            continue

        # "Files :  5  5  0  0  0  0"  (Total Copied Skipped Mismatch Failed Extras)
        if parts[0] == 'Files' and len(parts) >= 7 and parts[1] == ':':
            try:
                result['files_copied']  = int(parts[3])
                result['files_skipped'] = int(parts[4])
                result['files_failed']  = int(parts[6])
            except (ValueError, IndexError):
                pass

        # "Bytes :  4.52 g  4.52 g  0  0  0  0"

        elif parts[0] == 'Bytes' and len(parts) >= 4 and parts[1] == ':':
            _,          i = _parse_robocopy_bytes(parts, 2)  # total (skip)
            bytes_copied, _ = _parse_robocopy_bytes(parts, i)
            result['bytes_copied'] = bytes_copied

        # "Speed :  59993856 Bytes/Sec."
        elif parts[0] == 'Speed' and len(parts) >= 4 and parts[1] == ':' and 'bytes/sec' in parts[3].lower():
            try:
                result['speed_mbs'] = float(parts[2].replace(',', '')) / 1024**2
            except (ValueError, IndexError):
                pass

        # "Times :  0:01:18  0:01:18  ..."
        elif parts[0] == 'Times' and len(parts) >= 3 and parts[1] == ':':
            result['elapsed'] = parts[2]

    return result


def _run_robocopy_capture(cmd):
    """Run robocopy and decode its localized console output without crashing."""
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        encoding='utf-8',
        errors='replace',
    )


def _run_robocopy_tuned(cmd, priority=None, affinity=None):
    """Run robocopy as a tracked, cancellable child with optional CPU priority
    and core affinity (the tape-write step). Returns a CompletedProcess with
    .stdout/.returncode so it is a drop-in for _run_robocopy_capture.

    Registering the process lets Ctrl+C terminate the live tape write; the HIGH
    priority + dedicated cores keep the LTO drive streaming without contention."""
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding='utf-8',
        errors='replace',
    )
    register_proc(proc)
    _apply_proc_tuning(proc, priority=priority, affinity=affinity, label='robocopy-tape')
    try:
        out, err = proc.communicate()
    finally:
        unregister_proc(proc)
    return subprocess.CompletedProcess(cmd, proc.returncode, out, err)


def _dir_tree_size(path):
    """Total size in bytes of every file under path (0 if missing/unreadable)."""
    total = 0
    try:
        for root, _, files in os.walk(path):
            for f in files:
                try:
                    total += os.path.getsize(os.path.join(root, f))
                except OSError:
                    pass
    except OSError:
        pass
    return total


def _has_command(name):
    return shutil.which(name) is not None


_ASKPASS_HELPERS = set()


@atexit.register
def _cleanup_askpass_helpers():
    """Remove any SSH askpass helper scripts created during this run."""
    for helper_path in _ASKPASS_HELPERS:
        try:
            os.remove(helper_path)
        except OSError:
            pass


def _openssh_askpass_env(password):
    """Build an environment that lets OpenSSH read a configured password."""
    helper_path = os.path.join(tempfile.gettempdir(), 'lto_ssh_askpass.cmd')
    helper_body = (
        "@echo off\r\n"
        "powershell -NoProfile -ExecutionPolicy Bypass "
        "-Command \"[Console]::Out.Write($env:LTO_REMOTE_PASSWORD)\"\r\n"
    )
    try:
        with open(helper_path, 'w', encoding='utf-8', newline='') as f:
            f.write(helper_body)
    except OSError as e:
        raise RuntimeError(f"Could not create SSH askpass helper: {e}") from e
    _ASKPASS_HELPERS.add(helper_path)

    env = os.environ.copy()
    env['LTO_REMOTE_PASSWORD'] = password
    env['SSH_ASKPASS'] = helper_path
    env['SSH_ASKPASS_REQUIRE'] = 'force'
    env['DISPLAY'] = env.get('DISPLAY') or 'lto-archive-manager'
    return env


def _ssh_run(remote_user, remote_host, command, capture=True, password=''):
    """Run a command on the remote host.

    Blank password uses normal OpenSSH key auth. A configured password uses
    sshpass when available, or PuTTY plink on Windows-style installations.
    """
    password = password or ''
    if password:
        if _has_command('sshpass'):
            ssh_cmd = [
                'sshpass', '-e',
                'ssh',
                '-o', 'BatchMode=no',
                '-o', 'PubkeyAuthentication=no',
                '-o', 'StrictHostKeyChecking=accept-new',
                f'{remote_user}@{remote_host}',
                command,
            ]
            env = os.environ.copy()
            env['SSHPASS'] = password
            if capture:
                return subprocess.run(
                    ssh_cmd,
                    capture_output=True,
                    text=True,
                    encoding='utf-8',
                    errors='replace',
                    env=env,
                )
            return subprocess.run(ssh_cmd, env=env)
        if _has_command('ssh'):
            ssh_cmd = [
                'ssh',
                '-o', 'BatchMode=no',
                '-o', 'PubkeyAuthentication=no',
                '-o', 'NumberOfPasswordPrompts=1',
                '-o', 'StrictHostKeyChecking=accept-new',
                f'{remote_user}@{remote_host}',
                command,
            ]
            env = _openssh_askpass_env(password)
            if capture:
                return subprocess.run(
                    ssh_cmd,
                    stdin=subprocess.DEVNULL,
                    capture_output=True,
                    text=True,
                    encoding='utf-8',
                    errors='replace',
                    env=env,
                )
            return subprocess.run(ssh_cmd, stdin=subprocess.DEVNULL, env=env)
        if _has_command('plink'):
            ssh_cmd = [
                'plink',
                '-batch',
                '-pw', password,
                f'{remote_user}@{remote_host}',
                command,
            ]
            if capture:
                return subprocess.run(
                    ssh_cmd,
                    capture_output=True,
                    text=True,
                    encoding='utf-8',
                    errors='replace',
                )
            return subprocess.run(ssh_cmd)
        return subprocess.CompletedProcess(
            args=['ssh'],
            returncode=255,
            stdout='',
            stderr=(
                "remote_password is set, but no password-capable SSH helper was found. "
                "Install OpenSSH, sshpass, or PuTTY plink/pscp; or configure SSH key auth."
            ),
        )

    ssh_cmd = [
        'ssh',
        '-o', 'BatchMode=yes',
        '-o', 'StrictHostKeyChecking=accept-new',
        f'{remote_user}@{remote_host}',
        command,
    ]
    if capture:
        return subprocess.run(
            ssh_cmd,
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace',
        )
    return subprocess.run(ssh_cmd)


def _scp_fetch_file(remote_user, remote_host, remote_file_path, local_dest_path, password=''):
    """Copy a single file from remote_user@remote_host:remote_file_path to
    local_dest_path using SCP.  stdout/stderr are NOT redirected so SCP's
    native progress output is visible in the terminal.
    Returns SCP's exit code (0 = success).
    """
    os.makedirs(os.path.dirname(os.path.abspath(local_dest_path)), exist_ok=True)
    remote_spec = f'{remote_user}@{remote_host}:{remote_file_path}'

    password = password or ''
    if password:
        if _has_command('sshpass'):
            env = os.environ.copy()
            env['SSHPASS'] = password
            proc = subprocess.Popen(
                ['sshpass', '-e', 'scp', '-p', remote_spec, local_dest_path],
                env=env
            )
            return proc.wait()
        if _has_command('scp'):
            env = _openssh_askpass_env(password)
            proc = subprocess.Popen(
                [
                    'scp',
                    '-o', 'BatchMode=no',
                    '-o', 'PubkeyAuthentication=no',
                    '-o', 'NumberOfPasswordPrompts=1',
                    '-p',
                    remote_spec,
                    local_dest_path,
                ],
                stdin=subprocess.DEVNULL,
                env=env,
            )
            return proc.wait()
        if _has_command('pscp'):
            proc = subprocess.Popen([
                'pscp',
                '-scp',
                '-p',
                '-pw', password,
                remote_spec,
                local_dest_path,
            ])
            return proc.wait()
        print("[REMOTE] remote_password is set, but scp, sshpass, or PuTTY pscp was not found.")
        return 255

    proc = subprocess.Popen(['scp', '-p', remote_spec, local_dest_path])
    return proc.wait()


def _ssh_stream_command(remote_user, remote_host, command, password='', cipher=''):
    """Return a command/env pair for an SSH process that streams stdin/stdout.

    cipher: optional OpenSSH cipher name (e.g. aes128-gcm@openssh.com). When set,
            it is requested with SSH-level compression disabled — a fast AES-NI
            cipher keeps the fetch stream from being CPU-bound on incompressible
            media. Ignored for PuTTY plink (different cipher naming)."""
    password = password or ''
    # OpenSSH cipher/compression tuning, inserted right after the 'ssh' binary.
    cipher_opts = (['-c', cipher, '-o', 'Compression=no'] if cipher else [])
    if password:
        if _has_command('sshpass'):
            env = os.environ.copy()
            env['SSHPASS'] = password
            return [
                'sshpass', '-e',
                'ssh', *cipher_opts,
                '-o', 'BatchMode=no',
                '-o', 'PubkeyAuthentication=no',
                '-o', 'StrictHostKeyChecking=accept-new',
                f'{remote_user}@{remote_host}',
                command,
            ], env, None
        if _has_command('ssh'):
            return [
                'ssh', *cipher_opts,
                '-o', 'BatchMode=no',
                '-o', 'PubkeyAuthentication=no',
                '-o', 'NumberOfPasswordPrompts=1',
                '-o', 'StrictHostKeyChecking=accept-new',
                f'{remote_user}@{remote_host}',
                command,
            ], _openssh_askpass_env(password), None
        if _has_command('plink'):
            return [
                'plink',
                '-batch',
                '-pw', password,
                f'{remote_user}@{remote_host}',
                command,
            ], None, None
        return None, None, (
            "remote_password is set, but no password-capable SSH helper was found. "
            "Install OpenSSH, sshpass, or PuTTY plink/pscp; or configure SSH key auth."
        )

    if not _has_command('ssh'):
        return None, None, "ssh was not found on PATH."
    return [
        'ssh', *cipher_opts,
        '-o', 'BatchMode=yes',
        '-o', 'StrictHostKeyChecking=accept-new',
        f'{remote_user}@{remote_host}',
        command,
    ], None, None


def _safe_remote_relpath(path):
    """Return a tar-safe remote relative path using forward slashes."""
    rel = (path or '').replace('\\', '/')
    if rel.startswith('/') or re.match(r'^[A-Za-z]:/', rel):
        raise ValueError(f"unsafe relative path: {path}")
    rel = rel.strip('/')
    raw_parts = [part for part in rel.split('/') if part]
    if any(part in ('.', '..') for part in raw_parts):
        raise ValueError(f"unsafe relative path: {path}")
    normalized = posixpath.normpath(rel)
    if normalized in ('', '.'):
        raise ValueError("empty relative path")
    parts = normalized.split('/')
    if normalized.startswith('/') or any(part in ('', '.', '..') for part in parts):
        raise ValueError(f"unsafe relative path: {path}")
    return normalized


def _remote_fetch_base_and_rel(configured_remote_path, remote_fpath):
    """Map a scanned remote path to a tar -C base and safe relative path.

    configured_remote_path may be either a directory or a single file.
    """
    remote_root = (configured_remote_path or '').replace('\\', '/').strip()
    scanned_path = (remote_fpath or '').replace('\\', '/').strip()
    if not remote_root:
        raise ValueError("empty configured remote path")
    if not scanned_path:
        raise ValueError("empty scanned remote path")

    remote_root = posixpath.normpath(remote_root)
    scanned_path = posixpath.normpath(scanned_path)

    if scanned_path == remote_root:
        base = posixpath.dirname(remote_root) or '.'
        rel = posixpath.basename(remote_root)
    elif remote_root == '/':
        base = '/'
        rel = scanned_path.lstrip('/')
    elif scanned_path.startswith(remote_root.rstrip('/') + '/'):
        base = remote_root
        rel = scanned_path[len(remote_root.rstrip('/') + '/'):]
    else:
        raise ValueError(f"remote path outside base: {remote_fpath}")

    return base, _safe_remote_relpath(rel)


# Characters and trailing forms that NTFS (and therefore the Windows system
# bsdtar that extracts the fetch stream) cannot write verbatim. bsdtar does NOT
# fail on these — it silently rewrites the name on extraction: each reserved
# character becomes '_' and trailing dots/spaces are stripped per component.
# We must reproduce that mapping exactly so the post-fetch existence/size check
# looks for the file where bsdtar actually wrote it (e.g. a remote dir named
# "26-06T16:07:40" lands on disk as "26-06T16_07_40").
_WIN_RESERVED_TABLE = {ord(c): '_' for c in '<>:"|?*'}
_WIN_RESERVED_TABLE.update({i: '_' for i in range(32)})  # ASCII control chars


def _winsafe_extracted_rel(rel):
    """Map a POSIX remote rel-path to the on-disk path bsdtar produces on
    Windows. No-op for names that are already NTFS-legal, so chunks without
    reserved characters are byte-for-byte unaffected. On non-Windows hosts the
    extractor keeps names verbatim, so this is a pass-through there."""
    if os.name != 'nt':
        return rel
    out = []
    for part in rel.split('/'):
        if not part:
            continue
        part = part.translate(_WIN_RESERVED_TABLE).rstrip(' .')
        out.append(part or '_')
    return '/'.join(out)


def _disambiguate_local_rel(local_rel, claimed):
    """Return a variant of local_rel whose case-folded form is not in `claimed`.

    Two distinct remote names can sanitize to the same on-disk path (e.g.
    "a:b" and "a?b" both become "a_b"); NTFS is also case-insensitive. When
    that happens the second file would overwrite the first, so we insert a
    "~N" tag before the extension of the final component until the full path
    is unique. `claimed` holds the case-folded paths already taken."""
    head, _, tail = local_rel.rpartition('/')
    dot = tail.rfind('.')
    stem, ext = (tail[:dot], tail[dot:]) if dot > 0 else (tail, '')
    n = 1
    while True:
        cand_tail = f"{stem}~{n}{ext}"
        cand = f"{head}/{cand_tail}" if head else cand_tail
        if cand.casefold() not in claimed:
            return cand
        n += 1


def _remote_tar_fetch(remote_user, remote_host, remote_base, rel_paths, local_dest_dir,
                      password='', cipher='', use_mbuffer=False, mbuffer_size='2G',
                      fetch_cores=None):
    """Fetch many remote files in one tar stream over SSH.

    rel_paths must be relative to remote_base and use POSIX separators.
    Returns (ok, error_message).

    Performance knobs:
      cipher       — fast OpenSSH cipher for the stream (AES-NI, low CPU).
      use_mbuffer  — wrap the remote tar in mbuffer (if installed remotely) so a
                     large RAM ring smooths the tar->ssh handoff against jitter.
      fetch_cores  — pin the ssh/tar children to these cores, isolating SSH
                     decryption from the tape-writer's cores.
    """
    if not rel_paths:
        return True, ''
    if not _has_command('tar'):
        return False, "local tar executable was not found on PATH"
    if CANCEL.is_set():
        return False, "cancelled"

    os.makedirs(local_dest_dir, exist_ok=True)
    safe_paths = []
    try:
        for rel in rel_paths:
            safe_paths.append(_safe_remote_relpath(rel))
    except ValueError as e:
        return False, str(e)

    # -b 512 -> 256 KiB records: fewer syscalls than tar's tiny default block.
    tar_core = f"tar -C {shlex.quote(remote_base)} -b 512 -cf - --null -T -"
    if use_mbuffer:
        # Use mbuffer only if it exists on the remote; otherwise fall back to a
        # plain tar so a missing binary never fails the fetch. stdin (the NUL
        # file list) flows to whichever tar runs.
        remote_cmd = (
            f"if command -v mbuffer >/dev/null 2>&1; then "
            f"{tar_core} | mbuffer -q -m {shlex.quote(mbuffer_size)}; "
            f"else {tar_core}; fi"
        )
    else:
        remote_cmd = tar_core

    ssh_cmd, ssh_env, err = _ssh_stream_command(
        remote_user, remote_host, remote_cmd, password=password, cipher=cipher
    )
    if err:
        return False, err

    # Local extract: read the stream as-is. A tar stream is a 512-byte-record
    # byte stream over the pipe, so the remote -b 512 does not need to be matched
    # here — and this stays compatible with Windows' bsdtar (no GNU -b/-B flags).
    tar_cmd = ['tar', '-C', local_dest_dir, '-xf', '-']
    ssh_proc = None
    tar_proc = None
    ssh_stderr = []

    def _drain_stderr(pipe):
        try:
            while True:
                chunk = pipe.read(65536)
                if not chunk:
                    break
                ssh_stderr.append(chunk)
        except OSError:
            pass

    try:
        ssh_proc = subprocess.Popen(
            ssh_cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=ssh_env,
        )
        register_proc(ssh_proc)
        _apply_proc_tuning(ssh_proc, affinity=fetch_cores, label='ssh-fetch')
        stderr_thread = threading.Thread(
            target=_drain_stderr, args=(ssh_proc.stderr,), daemon=True
        )
        stderr_thread.start()

        tar_proc = subprocess.Popen(
            tar_cmd,
            stdin=ssh_proc.stdout,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        register_proc(tar_proc)
        _apply_proc_tuning(tar_proc, affinity=fetch_cores, label='tar-extract')
        ssh_proc.stdout.close()

        file_list = ''.join(f'{rel}\0' for rel in safe_paths).encode('utf-8')
        try:
            ssh_proc.stdin.write(file_list)
            ssh_proc.stdin.close()
        except OSError:
            pass

        tar_stdout, tar_stderr = tar_proc.communicate()
        ssh_rc = ssh_proc.wait()
        stderr_thread.join(timeout=2)
        tar_rc = tar_proc.returncode
    except OSError as e:
        for proc in (tar_proc, ssh_proc):
            if proc and proc.poll() is None:
                proc.kill()
        return False, str(e)
    finally:
        unregister_proc(ssh_proc)
        unregister_proc(tar_proc)

    if CANCEL.is_set():
        return False, "cancelled"

    ssh_err_text = b''.join(ssh_stderr).decode('utf-8', errors='replace').strip()
    tar_err_text = (tar_stderr or b'').decode('utf-8', errors='replace').strip()
    if ssh_rc != 0 or tar_rc != 0:
        parts = []
        if ssh_rc != 0:
            parts.append(f"remote tar/ssh exit {ssh_rc}: {ssh_err_text}")
        if tar_rc != 0:
            parts.append(f"local tar exit {tar_rc}: {tar_err_text}")
        return False, '\n'.join(parts)
    return True, ''


def _drive_letter(drive_path):
    return (drive_path or '').rstrip(":\\/")


def _ltfs_drive_status(drive_path):
    """Return (status, full_output, error) from IBM LTFS drive info."""
    exe = os.path.join(LTFS_DIR, 'LtfsCmdDrives.exe')
    try:
        result = subprocess.run([exe], text=True, capture_output=True, cwd=LTFS_DIR)
    except FileNotFoundError:
        return None, None, f"LtfsCmdDrives.exe not found in: {LTFS_DIR}"

    output = ((result.stdout or '') + (result.stderr or '')).strip()
    drive_letter = _drive_letter(drive_path).upper()
    for line in output.splitlines():
        parts = line.split()
        if len(parts) >= 4 and parts[0].upper() == drive_letter:
            return parts[-1], output, None
    return None, output, None


def _ensure_lto_drive_ready_unlocked(tape_drive, prefix="[TAPE]"):
    """Check that the configured LTFS drive is mounted and writable enough to use."""
    status, output, error = _ltfs_drive_status(tape_drive)
    if error:
        print(f"{prefix} {error}")

    if status:
        print(f"{prefix} IBM LTFS drive status for {_drive_letter(tape_drive)}: {status}")
        blocking_statuses = {
            "LTFS_UNFORMATTED",
            "NO_LTFS_MEDIA",
            "NO_MEDIA",
            "NOT_MOUNTED",
            "UNFORMATTED",
        }
        if status.upper() in blocking_statuses:
            print(f"{prefix} Drive {tape_drive} is not mounted as a writable LTFS filesystem.")
            if status.upper() == "LTFS_UNFORMATTED":
                print(f"{prefix} Format the cartridge from Tape Maintenance before archiving.")
            elif "MEDIA" in status.upper():
                print(f"{prefix} Load a writable LTFS data cartridge, wait until ready, then retry.")
            else:
                print(f"{prefix} Mount or reload the cartridge, then retry.")
            return False
    elif output:
        print(f"{prefix} Could not identify drive {_drive_letter(tape_drive)} in LtfsCmdDrives.exe output:")
        print(output)

    try:
        if not os.path.isdir(tape_drive):
            print(f"{prefix} Drive path is not available: {tape_drive}")
            return False
        os.listdir(tape_drive)
    except OSError as e:
        print(f"{prefix} Cannot access LTFS drive {tape_drive}: {e}")
        print(f"{prefix} Check that the cartridge is formatted, loaded, and mounted.")
        return False

    return True


def _ensure_lto_drive_ready(tape_drive, prefix="[TAPE]"):
    _acquire_tape_io_lock(f"check drive readiness {tape_drive}")
    try:
        return _ensure_lto_drive_ready_unlocked(tape_drive, prefix=prefix)
    finally:
        _release_tape_io_lock()


# ==============================================================================
# CONFIGURATION MANAGER
# ==============================================================================

class ConfigManager:
    def __init__(self, config_path=CONFIG_FILE):
        self.config      = configparser.ConfigParser(interpolation=None)
        self.config_path = config_path

        if not os.path.exists(config_path):
            self._create_default()
            print(f"[CONFIG] Created default config file: {os.path.abspath(config_path)}")
            print("[CONFIG] Please review and edit it before running operations.")

        self.config.read(config_path, encoding='utf-8')

        # Secrets live in a gitignored .env next to the app, never in config.ini.
        self.env = _load_env_file(os.path.join(APP_DIR, '.env'))

    def _create_default(self):
        self.config['PATHS'] = {
            'source_dir':  os.path.join(APP_DIR, 'source'),
            'staging_dir': os.path.join(APP_DIR, 'staging'),
            'restore_dir': os.path.join(APP_DIR, 'restored'),
            'db_path':     os.path.join(APP_DIR, 'lto_archive.db'),
            'backup_log_dir': BACKUP_LOG_DIR,
        }
        self.config['HARDWARE'] = {
            'lto_drive':     r'D:\\',
            'ibm_eject_cmd': r'C:\Program Files\IBM\LTFS\LtfsCmdEject.exe',
        }
        self.config['SETTINGS'] = {
            'zip_threshold_mb': '100',
            'max_zip_size_gb':  '100',
        }
        self.config['REMOTE'] = {
            'remote_host':      'your.remote.host',
            'remote_user':      '',
            'remote_password':  '',
            'remote_path':      '',
            'remote_selected_paths': '',
            'confirm_before_backup': 'true',
            'staging_fill_pct': '0.80',
        }
        self.config['PERFORMANCE'] = {
            'chunk_cap_gb':          '100',
            'prefetch_chunks_ahead': '2',
            'eject_after_pack':      'off',
            'staging_max_gb':        '350',
            'robocopy_priority':     'high',
            'cpu_affinity':          'auto',
            'ssh_cipher':            'aes128-gcm@openssh.com',
            'use_mbuffer':           'true',
            'mbuffer_size':          '2G',
        }
        with open(self.config_path, 'w', encoding='utf-8') as f:
            self.config.write(f)

    @property
    def source_dir(self):    return _clean_config_path(self.config['PATHS']['source_dir'])
    @property
    def staging_dir(self):   return _clean_config_path(self.config['PATHS']['staging_dir'])
    @property
    def restore_dir(self):   return _clean_config_path(self.config['PATHS']['restore_dir'])
    @property
    def db_path(self):       return _clean_config_path(self.config['PATHS']['db_path'])
    @property
    def backup_log_dir(self):
        return _clean_config_path(self.config.get('PATHS', 'backup_log_dir',
                                                  fallback=BACKUP_LOG_DIR))
    @property
    def lto_drive(self):     return _clean_config_path(self.config['HARDWARE']['lto_drive'])
    @property
    def ibm_eject_cmd(self): return _clean_config_path(self.config['HARDWARE'].get(
                                 'ibm_eject_cmd',
                                 r'C:\Program Files\IBM\LTFS\LtfsCmdEject.exe'))
    @property
    def zip_threshold_mb(self): return float(self.config['SETTINGS']['zip_threshold_mb'])
    @property
    def max_zip_size_gb(self):  return float(self.config['SETTINGS']['max_zip_size_gb'])
    @property
    def remote_host(self):      return self.config.get('REMOTE', 'remote_host', fallback='')
    @property
    def remote_user(self):      return self.config.get('REMOTE', 'remote_user', fallback='')
    @property
    def remote_password(self):
        # Priority: process env var > .env file > config.ini (kept empty in git).
        value = (os.environ.get('REMOTE_PASSWORD')
                 or self.env.get('REMOTE_PASSWORD')
                 or self.config.get('REMOTE', 'remote_password', fallback='', raw=True))
        value = (value or '').strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
            value = value[1:-1]
        return value
    @property
    def remote_path(self):      return _clean_remote_path(self.config.get('REMOTE', 'remote_path', fallback=''))
    @property
    def remote_selected_paths(self):
        paths = [_clean_remote_path(p)
                 for p in _config_list(self.config.get('REMOTE', 'remote_selected_paths', fallback='', raw=True))]
        return [p for p in paths if p]
    @property
    def remote_scan_paths(self):
        return self.remote_selected_paths or ([self.remote_path] if self.remote_path else [])
    @property
    def confirm_before_backup(self):
        return self.config.get('REMOTE', 'confirm_before_backup', fallback='true').strip().lower() in ('1', 'true', 'yes', 'on')
    @property
    def staging_fill_pct(self): return float(self.config.get('REMOTE', 'staging_fill_pct', fallback='0.80'))

    # --- [PERFORMANCE] : continuous-streaming pipeline tuning -----------------
    @property
    def chunk_cap_gb(self):
        return float(self.config.get('PERFORMANCE', 'chunk_cap_gb', fallback='100'))
    @property
    def prefetch_chunks_ahead(self):
        return max(1, int(float(self.config.get('PERFORMANCE', 'prefetch_chunks_ahead', fallback='2'))))
    @property
    def eject_after_pack(self):
        raw = self.config.get('PERFORMANCE', 'eject_after_pack', fallback='off').strip().lower()
        if raw in ('', 'off', 'none', 'false', 'no'):
            return None
        try:
            value = int(float(raw))
        except ValueError:
            return None
        return value if value >= 0 else None
    @property
    def staging_max_gb(self):
        return float(self.config.get('PERFORMANCE', 'staging_max_gb', fallback='350'))
    @property
    def robocopy_priority(self):
        return self.config.get('PERFORMANCE', 'robocopy_priority', fallback='high').strip().lower()
    @property
    def cpu_affinity(self):
        return self.config.get('PERFORMANCE', 'cpu_affinity', fallback='auto')
    @property
    def ssh_cipher(self):
        return self.config.get('PERFORMANCE', 'ssh_cipher', fallback='aes128-gcm@openssh.com').strip()
    @property
    def use_mbuffer(self):
        return self.config.get('PERFORMANCE', 'use_mbuffer', fallback='true').strip().lower() in ('1', 'true', 'yes', 'on')
    @property
    def mbuffer_size(self):
        return self.config.get('PERFORMANCE', 'mbuffer_size', fallback='2G').strip()


# ==============================================================================
# DATABASE MANAGER
# ==============================================================================

class DatabaseManager:
    def __init__(self, db_path):
        db_path = _clean_config_path(db_path)
        db_dir = os.path.dirname(os.path.abspath(db_path))
        try:
            os.makedirs(db_dir, exist_ok=True)
            # check_same_thread=False: the streaming pipeline updates the DB from
            # both the producer (fetch/pack) thread and the consumer (tape) thread.
            # self.lock serialises every write so the shared connection stays safe.
            self.conn = sqlite3.connect(db_path, check_same_thread=False)
        except (OSError, sqlite3.Error) as e:
            raise RuntimeError(
                f"[DB] Cannot open database at: {db_path}\n"
                f"     Directory: {db_dir}\n"
                f"     Reason: {e}\n"
                f"     Edit {CONFIG_FILE} and set [PATHS] db_path to a writable location."
            ) from e
        self.conn.row_factory = sqlite3.Row
        self.lock = threading.RLock()
        self._init_schema()
        self._init_remote_schema()
        self._init_local_schema()

    def _require_updated(self, cur, message):
        """Raise if the preceding UPDATE/DELETE matched no rows (target missing).

        sqlite3 reports an accurate rowcount for UPDATE/DELETE, so a value of 0
        means the WHERE clause matched nothing — i.e. the row we expected to
        change does not exist. Callers pass a '[DB] ... not found' message."""
        if cur.rowcount == 0:
            raise RuntimeError(message)

    def _init_schema(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS tapes (
                tape_id        INTEGER PRIMARY KEY AUTOINCREMENT,
                volume_label   TEXT    UNIQUE NOT NULL,
                date_formatted DATETIME,
                total_capacity INTEGER,
                used_space     INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS files_index (
                file_id         INTEGER PRIMARY KEY AUTOINCREMENT,
                file_name       TEXT,
                original_path   TEXT,
                file_size_bytes INTEGER,
                file_hash       TEXT,
                backup_date     DATETIME,
                tape_label      TEXT,
                is_packed       BOOLEAN,
                container_name  TEXT,
                stored_path     TEXT,
                FOREIGN KEY (tape_label) REFERENCES tapes(volume_label)
            );
        """)
        self.conn.commit()
        # Migrate existing DB: add used_space if missing
        try:
            self.conn.execute("ALTER TABLE tapes ADD COLUMN used_space INTEGER DEFAULT 0")
            self.conn.commit()
        except sqlite3.OperationalError:
            pass  # column already exists
        for column, col_type in (
            ('local_session_id', 'INTEGER'),
            ('local_chunk_index', 'INTEGER'),
        ):
            try:
                self.conn.execute(f"ALTER TABLE files_index ADD COLUMN {column} {col_type}")
                self.conn.commit()
            except sqlite3.OperationalError:
                pass  # column already exists

    def _init_local_schema(self):
        """Create local multi-tape session tables if they don't exist."""
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS local_sessions (
                session_id    INTEGER PRIMARY KEY AUTOINCREMENT,
                session_label TEXT    NOT NULL,
                source_dir    TEXT    NOT NULL,
                total_chunks  INTEGER NOT NULL,
                backup_mode   TEXT NOT NULL DEFAULT 'auto'
                    CHECK(backup_mode IN ('auto','direct','pack')),
                created_at    DATETIME NOT NULL,
                completed_at  DATETIME,
                status        TEXT NOT NULL DEFAULT 'active'
                    CHECK(status IN ('active','completed','abandoned'))
            );
            CREATE TABLE IF NOT EXISTS local_chunks_manifest (
                manifest_id     INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id      INTEGER NOT NULL
                    REFERENCES local_sessions(session_id),
                chunk_index     INTEGER NOT NULL,
                top_level_dir   TEXT    NOT NULL,
                dir_size_bytes  INTEGER NOT NULL,
                status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK(status IN ('pending','staged','backed_up')),
                tape_label      TEXT,
                started_at      DATETIME,
                completed_at    DATETIME,
                updated_at      DATETIME
            );
            CREATE INDEX IF NOT EXISTS idx_local_manifest_session_chunk
                ON local_chunks_manifest(session_id, chunk_index);
            CREATE INDEX IF NOT EXISTS idx_files_local_session_chunk
                ON files_index(local_session_id, local_chunk_index, tape_label);
        """)
        self.conn.commit()
        try:
            self.conn.execute(
                """ALTER TABLE local_sessions
                   ADD COLUMN backup_mode TEXT NOT NULL DEFAULT 'auto'
                   CHECK(backup_mode IN ('auto','direct','pack'))"""
            )
            self.conn.commit()
        except sqlite3.OperationalError:
            pass  # column already exists

    def _init_remote_schema(self):
        """Create remote_sessions and remote_manifest tables if they don't exist.
        Safe to call on existing databases — uses CREATE TABLE IF NOT EXISTS."""
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS remote_sessions (
                session_id    INTEGER PRIMARY KEY AUTOINCREMENT,
                session_label TEXT    NOT NULL,
                remote_host   TEXT    NOT NULL,
                remote_user   TEXT    NOT NULL,
                remote_path   TEXT    NOT NULL,
                tape_label    TEXT    NOT NULL,
                staging_dir   TEXT    NOT NULL,
                total_files   INTEGER DEFAULT 0,
                total_bytes   INTEGER DEFAULT 0,
                chunk_count   INTEGER DEFAULT 0,
                created_at    DATETIME NOT NULL,
                completed_at  DATETIME,
                status        TEXT NOT NULL DEFAULT 'active'
                    CHECK(status IN ('active','completed','abandoned'))
            );
            CREATE TABLE IF NOT EXISTS remote_manifest (
                manifest_id     INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id      INTEGER NOT NULL
                    REFERENCES remote_sessions(session_id),
                chunk_index     INTEGER NOT NULL,
                remote_path     TEXT    NOT NULL,
                file_name       TEXT    NOT NULL,
                file_size_bytes INTEGER NOT NULL,
                local_rel_path  TEXT,
                status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK(status IN (
                        'pending','fetching','fetched','packing','packed',
                        'backing','backed','done','fetch_failed','backup_failed'
                    )),
                chunk_status    TEXT NOT NULL DEFAULT 'pending'
                    CHECK(chunk_status IN (
                        'pending','fetching','packing','backing','done',
                        'fetch_failed','backup_failed'
                    )),
                error_msg       TEXT,
                updated_at      DATETIME
            );
            CREATE INDEX IF NOT EXISTS idx_remote_manifest_session_chunk
                ON remote_manifest(session_id, chunk_index);
        """)
        self.conn.commit()

    def create_remote_session(self, session_label, remote_host, remote_user,
                               remote_path, tape_label, staging_dir):
        with self.lock:
            cur = self.conn.execute(
                """INSERT INTO remote_sessions
                   (session_label, remote_host, remote_user, remote_path,
                    tape_label, staging_dir, created_at, status)
                   VALUES (?, ?, ?, ?, ?, ?, ?, 'active')""",
                (session_label, remote_host, remote_user, remote_path,
                 tape_label, staging_dir, datetime.now().isoformat())
            )
            self.conn.commit()
            return cur.lastrowid

    def update_remote_session(self, session_id, **kwargs):
        if not kwargs:
            return
        sets = ', '.join(f"{k} = ?" for k in kwargs)
        vals = list(kwargs.values()) + [session_id]
        with self.lock:
            cur = self.conn.execute(
                f"UPDATE remote_sessions SET {sets} WHERE session_id = ?", vals
            )
            self._require_updated(cur, f"[DB] Remote session not found: {session_id}")
            self.conn.commit()

    def get_active_remote_session(self, remote_host, remote_path):
        return self.conn.execute(
            """SELECT * FROM remote_sessions
               WHERE remote_host = ? AND remote_path = ? AND status = 'active'
               ORDER BY session_id DESC LIMIT 1""",
            (remote_host, remote_path)
        ).fetchone()

    def insert_remote_manifest_batch(self, session_id, rows):
        """rows: list of (chunk_index, remote_path, file_name, file_size_bytes)"""
        with self.lock:
            self.conn.executemany(
                """INSERT INTO remote_manifest
                   (session_id, chunk_index, remote_path, file_name, file_size_bytes,
                    status, chunk_status, updated_at)
                   VALUES (?, ?, ?, ?, ?, 'pending', 'pending', ?)""",
                [(session_id, r[0], r[1], r[2], r[3], datetime.now().isoformat())
                 for r in rows]
            )
            self.conn.commit()

    def get_chunk_files(self, session_id, chunk_index):
        return self.conn.execute(
            """SELECT * FROM remote_manifest
               WHERE session_id = ? AND chunk_index = ?
               ORDER BY manifest_id""",
            (session_id, chunk_index)
        ).fetchall()

    def update_manifest_row(self, manifest_id, **kwargs):
        kwargs['updated_at'] = datetime.now().isoformat()
        sets = ', '.join(f"{k} = ?" for k in kwargs)
        vals = list(kwargs.values()) + [manifest_id]
        with self.lock:
            cur = self.conn.execute(
                f"UPDATE remote_manifest SET {sets} WHERE manifest_id = ?", vals
            )
            self._require_updated(cur, f"[DB] Remote manifest row not found: {manifest_id}")
            self.conn.commit()

    def update_chunk_status(self, session_id, chunk_index, status):
        with self.lock:
            cur = self.conn.execute(
                """UPDATE remote_manifest SET chunk_status = ?, updated_at = ?
                   WHERE session_id = ? AND chunk_index = ?""",
                (status, datetime.now().isoformat(), session_id, chunk_index)
            )
            self._require_updated(
                cur,
                f"[DB] Remote chunk not found: session {session_id}, chunk {chunk_index}"
            )
            self.conn.commit()

    def get_pending_chunks(self, session_id):
        rows = self.conn.execute(
            """SELECT DISTINCT chunk_index FROM remote_manifest
               WHERE session_id = ? AND chunk_status NOT IN ('done')
               ORDER BY chunk_index""",
            (session_id,)
        ).fetchall()
        return [r[0] for r in rows]

    def count_chunks(self, session_id):
        return self.conn.execute(
            "SELECT COUNT(DISTINCT chunk_index) FROM remote_manifest WHERE session_id = ?",
            (session_id,)
        ).fetchone()[0]

    def create_local_session(self, session_label, source_dir, chunks,
                             backup_mode='auto'):
        """Persist a local multi-tape allocation plan.

        chunks: list of lists containing allocation dicts with name/size_bytes.
        """
        now = datetime.now().isoformat()
        with self.lock:
            with self.conn:
                cur = self.conn.execute(
                    """INSERT INTO local_sessions
                       (session_label, source_dir, total_chunks, backup_mode,
                        created_at, status)
                       VALUES (?, ?, ?, ?, ?, 'active')""",
                    (session_label, source_dir, len(chunks), backup_mode, now)
                )
                session_id = cur.lastrowid
                rows = []
                for chunk_index, entries in enumerate(chunks):
                    for entry in entries:
                        rows.append((
                            session_id, chunk_index, entry['name'],
                            entry['size_bytes'], 'pending', now
                        ))
                self.conn.executemany(
                    """INSERT INTO local_chunks_manifest
                       (session_id, chunk_index, top_level_dir, dir_size_bytes,
                        status, updated_at)
                       VALUES (?, ?, ?, ?, ?, ?)""",
                    rows
                )
            return session_id

    def update_local_session(self, session_id, **kwargs):
        if not kwargs:
            return
        sets = ', '.join(f"{k} = ?" for k in kwargs)
        vals = list(kwargs.values()) + [session_id]
        with self.lock:
            cur = self.conn.execute(
                f"UPDATE local_sessions SET {sets} WHERE session_id = ?", vals
            )
            self._require_updated(cur, f"[DB] Local session not found: {session_id}")
            self.conn.commit()

    def get_active_local_session(self, source_dir):
        return self.conn.execute(
            """SELECT * FROM local_sessions
               WHERE source_dir = ? AND status = 'active'
               ORDER BY session_id DESC LIMIT 1""",
            (source_dir,)
        ).fetchone()

    def get_local_session(self, session_id):
        return self.conn.execute(
            "SELECT * FROM local_sessions WHERE session_id = ?", (session_id,)
        ).fetchone()

    def get_local_pending_chunks(self, session_id):
        rows = self.conn.execute(
            """SELECT chunk_index FROM local_chunks_manifest
               WHERE session_id = ?
               GROUP BY chunk_index
               HAVING SUM(CASE WHEN status != 'backed_up' THEN 1 ELSE 0 END) > 0
               ORDER BY chunk_index""",
            (session_id,)
        ).fetchall()
        return [r[0] for r in rows]

    def get_local_chunk_entries(self, session_id, chunk_index):
        return self.conn.execute(
            """SELECT * FROM local_chunks_manifest
               WHERE session_id = ? AND chunk_index = ?
               ORDER BY manifest_id""",
            (session_id, chunk_index)
        ).fetchall()

    def assign_local_chunk_tape(self, session_id, chunk_index, tape_label):
        now = datetime.now().isoformat()
        with self.lock:
            cur = self.conn.execute(
                """UPDATE local_chunks_manifest
                   SET tape_label = COALESCE(tape_label, ?),
                       started_at = COALESCE(started_at, ?),
                       updated_at = ?
                   WHERE session_id = ? AND chunk_index = ?""",
                (tape_label, now, now, session_id, chunk_index)
            )
            self._require_updated(
                cur,
                f"[DB] Local chunk not found: session {session_id}, chunk {chunk_index}"
            )
            self.conn.commit()

    def update_local_chunk_status(self, session_id, chunk_index, status):
        kwargs = {
            'status': status,
            'updated_at': datetime.now().isoformat(),
        }
        if status == 'backed_up':
            kwargs['completed_at'] = datetime.now().isoformat()
        sets = ', '.join(f"{k} = ?" for k in kwargs)
        vals = list(kwargs.values()) + [session_id, chunk_index]
        with self.lock:
            cur = self.conn.execute(
                f"""UPDATE local_chunks_manifest SET {sets}
                    WHERE session_id = ? AND chunk_index = ?""",
                vals
            )
            self._require_updated(
                cur,
                f"[DB] Local chunk not found: session {session_id}, chunk {chunk_index}"
            )
            self.conn.commit()

    def update_local_manifest_row(self, manifest_id, **kwargs):
        if not kwargs:
            return
        kwargs['updated_at'] = datetime.now().isoformat()
        sets = ', '.join(f"{k} = ?" for k in kwargs)
        vals = list(kwargs.values()) + [manifest_id]
        with self.lock:
            cur = self.conn.execute(
                f"UPDATE local_chunks_manifest SET {sets} WHERE manifest_id = ?",
                vals
            )
            self._require_updated(cur, f"[DB] Local manifest row not found: {manifest_id}")
            self.conn.commit()

    def count_tape_file_records(self, tape_label):
        return self.conn.execute(
            "SELECT COUNT(*) FROM files_index WHERE tape_label = ?",
            (tape_label,)
        ).fetchone()[0]

    def get_local_indexed_original_paths(self, session_id, chunk_index, tape_label):
        rows = self.conn.execute(
            """SELECT original_path FROM files_index
               WHERE local_session_id = ?
                 AND local_chunk_index = ?
                 AND tape_label = ?""",
            (session_id, chunk_index, tape_label)
        ).fetchall()
        return {r[0] for r in rows}

    def get_local_written_tape_paths(self, session_id, chunk_index, tape_label):
        rows = self.conn.execute(
            """SELECT DISTINCT COALESCE(container_name, stored_path) AS tape_path
               FROM files_index
               WHERE local_session_id = ?
                 AND local_chunk_index = ?
                 AND tape_label = ?
                 AND COALESCE(container_name, stored_path) IS NOT NULL""",
            (session_id, chunk_index, tape_label)
        ).fetchall()
        return [r['tape_path'] for r in rows if r['tape_path']]

    def file_record_exists(self, original_path, tape_label, local_session_id=None,
                           local_chunk_index=None):
        with self.lock:
            return bool(self.conn.execute(
                """SELECT 1 FROM files_index
                   WHERE COALESCE(original_path, '') = COALESCE(?, '')
                     AND COALESCE(tape_label, '') = COALESCE(?, '')
                     AND COALESCE(local_session_id, -1) = COALESCE(?, -1)
                     AND COALESCE(local_chunk_index, -1) = COALESCE(?, -1)""",
                (original_path, tape_label, local_session_id, local_chunk_index)
            ).fetchone())

    def register_tape(self, volume_label, capacity_gb=None):
        with self.lock:
            try:
                self.conn.execute(
                    "INSERT INTO tapes (volume_label, date_formatted, total_capacity) VALUES (?, ?, ?)",
                    (volume_label, datetime.now().isoformat(), capacity_gb)
                )
                self.conn.commit()
                print(f"[DB] Tape '{volume_label}' registered successfully.")
                return True
            except sqlite3.IntegrityError:
                print(f"[DB] Tape '{volume_label}' is already in the database.")
                return False

    def delete_tape(self, volume_label):
        with self.lock:
            self.conn.execute("DELETE FROM files_index WHERE tape_label = ?", (volume_label,))
            cur = self.conn.execute("DELETE FROM tapes WHERE volume_label = ?", (volume_label,))
            self._require_updated(cur, f"[DB] Tape not found: {volume_label}")
            self.conn.commit()
            print(f"[DB] Tape '{volume_label}' and its file records removed from database.")

    def tape_exists(self, volume_label):
        with self.lock:
            return bool(self.conn.execute(
                "SELECT 1 FROM tapes WHERE volume_label = ?", (volume_label,)
            ).fetchone())

    def get_tape(self, volume_label):
        return self.conn.execute(
            "SELECT * FROM tapes WHERE volume_label = ?", (volume_label,)
        ).fetchone()

    def insert_file(self, file_name, original_path, file_size_bytes, file_hash,
                    tape_label, is_packed, container_name, stored_path,
                    local_session_id=None, local_chunk_index=None):
        with self.lock:
            if not self.conn.execute(
                "SELECT 1 FROM tapes WHERE volume_label = ?", (tape_label,)
            ).fetchone():
                raise RuntimeError(
                    f"[DB] Cannot index file for unregistered tape: {tape_label}"
                )

            now = datetime.now().isoformat()
            existing = self.conn.execute(
                """SELECT file_id FROM files_index
                   WHERE COALESCE(original_path, '') = COALESCE(?, '')
                     AND COALESCE(tape_label, '') = COALESCE(?, '')
                     AND COALESCE(local_session_id, -1) = COALESCE(?, -1)
                     AND COALESCE(local_chunk_index, -1) = COALESCE(?, -1)""",
                (original_path, tape_label, local_session_id, local_chunk_index)
            ).fetchone()

            if existing:
                self.conn.execute(
                    """UPDATE files_index
                       SET file_name = ?,
                           file_size_bytes = ?,
                           file_hash = ?,
                           backup_date = ?,
                           is_packed = ?,
                           container_name = ?,
                           stored_path = ?
                       WHERE file_id = ?""",
                    (file_name, file_size_bytes, file_hash, now, is_packed,
                     container_name, stored_path, existing['file_id'])
                )
                self.conn.commit()
                return False

            self.conn.execute(
                """INSERT INTO files_index
                   (file_name, original_path, file_size_bytes, file_hash, backup_date,
                    tape_label, is_packed, container_name, stored_path,
                    local_session_id, local_chunk_index)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (file_name, original_path, file_size_bytes, file_hash, now,
                 tape_label, is_packed, container_name, stored_path,
                 local_session_id, local_chunk_index)
            )
            self.conn.commit()
            return True

    def search_files(self, name_query=None, date_from=None, date_to=None):
        sql    = "SELECT * FROM files_index WHERE 1=1"
        params = []
        if name_query:
            sql += " AND file_name LIKE ?"
            pattern = name_query.replace('*', '%').replace('?', '_')
            if '%' not in pattern and '_' not in pattern:
                pattern = f'%{pattern}%'
            params.append(pattern)
        if date_from:
            sql += " AND DATE(backup_date) >= ?"
            params.append(date_from)
        if date_to:
            sql += " AND DATE(backup_date) <= ?"
            params.append(date_to)
        sql += " ORDER BY backup_date DESC"
        return self.conn.execute(sql, params).fetchall()

    def get_file_by_id(self, file_id):
        return self.conn.execute(
            "SELECT * FROM files_index WHERE file_id = ?", (file_id,)
        ).fetchone()

    def search_by_directory(self, dir_path):
        pattern = dir_path.rstrip('/\\') + '%'
        return self.conn.execute(
            "SELECT * FROM files_index WHERE original_path LIKE ? ORDER BY original_path",
            (pattern,)
        ).fetchall()

    def list_backup_sessions(self):
        return self.conn.execute("""
            SELECT DATE(backup_date) as session_date, tape_label,
                   COUNT(*)          as file_count,
                   SUM(file_size_bytes) as total_bytes
            FROM files_index
            GROUP BY DATE(backup_date), tape_label
            ORDER BY session_date DESC
        """).fetchall()

    def search_by_session(self, session_date, tape_label):
        return self.conn.execute(
            "SELECT * FROM files_index WHERE DATE(backup_date) = ? AND tape_label = ? ORDER BY original_path",
            (session_date, tape_label)
        ).fetchall()

    def update_tape_used_space(self, volume_label, bytes_added):
        with self.lock:
            cur = self.conn.execute(
                "UPDATE tapes SET used_space = COALESCE(used_space, 0) + ? WHERE volume_label = ?",
                (bytes_added, volume_label)
            )
            self._require_updated(cur, f"[DB] Tape not found: {volume_label}")
            self.conn.commit()

    def list_tapes(self):
        return self.conn.execute(
            "SELECT * FROM tapes ORDER BY date_formatted DESC"
        ).fetchall()

    def delete_file(self, file_id):
        with self.lock:
            cur = self.conn.execute("DELETE FROM files_index WHERE file_id = ?", (file_id,))
            self._require_updated(cur, f"[DB] File record not found: {file_id}")
            self.conn.commit()

    def rename_tape(self, old_label, new_label):
        with self.lock:
            try:
                self.conn.execute("BEGIN")
                self.conn.execute("PRAGMA defer_foreign_keys = ON")
                cur = self.conn.execute(
                    "UPDATE tapes SET volume_label = ? WHERE volume_label = ?",
                    (new_label, old_label)
                )
                self._require_updated(cur, f"[DB] Tape not found: {old_label}")
                self.conn.execute(
                    "UPDATE files_index SET tape_label = ? WHERE tape_label = ?",
                    (new_label, old_label)
                )
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise
        print(f"[DB] Tape '{old_label}' renamed to '{new_label}'.")

    def update_tape_capacity(self, volume_label, capacity_gb):
        with self.lock:
            cur = self.conn.execute(
                "UPDATE tapes SET total_capacity = ? WHERE volume_label = ?",
                (capacity_gb, volume_label)
            )
            self._require_updated(cur, f"[DB] Tape not found: {volume_label}")
            self.conn.commit()
        print(f"[DB] Tape '{volume_label}' capacity set to {capacity_gb} GB.")

    def recalculate_tape_used_space(self, volume_label):
        with self.lock:
            row = self.conn.execute(
                "SELECT COALESCE(SUM(file_size_bytes), 0) FROM files_index WHERE tape_label = ?",
                (volume_label,)
            ).fetchone()
            new_used = row[0]
            cur = self.conn.execute(
                "UPDATE tapes SET used_space = ? WHERE volume_label = ?",
                (new_used, volume_label)
            )
            self._require_updated(cur, f"[DB] Tape not found: {volume_label}")
            self.conn.commit()
            return new_used

    def delete_files_for_tape(self, volume_label):
        with self.lock:
            if not self.conn.execute(
                "SELECT 1 FROM tapes WHERE volume_label = ?", (volume_label,)
            ).fetchone():
                raise RuntimeError(f"[DB] Tape not found: {volume_label}")
            cur = self.conn.execute("DELETE FROM files_index WHERE tape_label = ?", (volume_label,))
            removed = cur.rowcount
            self.conn.execute("UPDATE tapes SET used_space = 0 WHERE volume_label = ?", (volume_label,))
            self.conn.commit()
            print(f"[DB] Removed {removed} file record(s) for tape '{volume_label}' (tape entry kept).")

    def close(self):
        self.conn.close()


# ==============================================================================
# MODULE A: ANALYZER
# ==============================================================================

class LTOAnalyzer:
    def analyze(self, folder_path, threshold_mb):
        print(f"\n[ANALYZER] Scanning: {folder_path}...")

        bins = {
            "Tiny (<1MB)":       0,
            "Small (1-10MB)":    0,
            "Medium (10-100MB)": 0,
            "Large (100MB-1GB)": 0,
            "Huge (>1GB)":       0,
        }
        total_files = 0
        total_size_bytes = 0
        files_under_threshold = 0
        bytes_under_threshold = 0

        for root, _, files in os.walk(folder_path):
            for file in files:
                try:
                    size_bytes = os.path.getsize(os.path.join(root, file))
                    size_mb    = size_bytes / (1024 * 1024)
                    total_files   += 1
                    total_size_bytes += size_bytes

                    if   size_mb < 1:    bins["Tiny (<1MB)"] += 1
                    elif size_mb < 10:   bins["Small (1-10MB)"] += 1
                    elif size_mb < 100:  bins["Medium (10-100MB)"] += 1
                    elif size_mb < 1024: bins["Large (100MB-1GB)"] += 1
                    else:                bins["Huge (>1GB)"] += 1

                    if size_mb < threshold_mb:
                        files_under_threshold += 1
                        bytes_under_threshold += size_bytes
                except OSError:
                    pass

        print("-" * 60)
        print(f"REPORT | Files: {total_files} | Total Size: {total_size_bytes/1024**3:.2f} GB")
        print("-" * 60)
        for cat, count in bins.items():
            pct = (count / total_files * 100) if total_files else 0
            bar = "#" * max(int(pct / 2), 1 if count else 0)
            print(f"{cat:20} : {count:6} ({pct:5.1f}%) | {bar}")
        print("-" * 60)

        should_pack, file_ratio, byte_ratio = _auto_pack_decision(
            total_files, total_size_bytes, files_under_threshold, bytes_under_threshold
        )
        if should_pack:
            print(f">>> ANALYSIS: {file_ratio*100:.1f}% of files are under {threshold_mb:.0f} MB "
                  f"and they account for {byte_ratio*100:.2f}% of the data.")
            print(f">>> RECOMMENDATION: AUTO-PILOT (Pack files < {threshold_mb:.0f} MB)")
            return True
        else:
            print(f">>> ANALYSIS: files under {threshold_mb:.0f} MB account for "
                  f"{byte_ratio*100:.2f}% of the data.")
            print(">>> RECOMMENDATION: DIRECT BACKUP (packing is not worth staging the large files)")
            return False

    def build_local_allocation_plan(self, source_dir,
                                    budget_bytes=LOCAL_TAPE_BUDGET_BYTES):
        print(f"\n[ANALYZER] Building local multi-tape plan: {source_dir}")
        if not os.path.isdir(source_dir):
            raise RuntimeError(f"[ANALYZER] Source directory not found: {source_dir}")

        top_entries = {}
        root_files_size = 0
        total_files = 0
        total_bytes = 0

        for root, _, files in os.walk(source_dir):
            rel_root = os.path.relpath(root, source_dir)
            top_name = None if rel_root == '.' else rel_root.split(os.sep, 1)[0]
            for file in files:
                path = os.path.join(root, file)
                try:
                    size = os.path.getsize(path)
                except OSError as e:
                    print(f"[WARN] Cannot stat {path}: {e}")
                    continue

                if size > budget_bytes:
                    raise RuntimeError(
                        "[FATAL] Single file exceeds the 11.5 TB tape safety "
                        "limit. Spanning one file across tapes is unsupported; "
                        "manually split it with CLI utilities before archiving:\n"
                        f"        {path}\n"
                        f"        Size: {size / 1000**4:.2f} TB"
                    )

                total_files += 1
                total_bytes += size
                if top_name is None:
                    root_files_size += size
                else:
                    top_entries[top_name] = top_entries.get(top_name, 0) + size

        entries = [
            {'name': name, 'size_bytes': size}
            for name, size in top_entries.items()
        ]
        if root_files_size:
            entries.append({'name': ROOT_FILES_GROUP, 'size_bytes': root_files_size})

        if not entries:
            raise RuntimeError("[ANALYZER] No files found in source directory.")

        for entry in entries:
            if entry['size_bytes'] > budget_bytes:
                raise RuntimeError(
                    "[FATAL] Top-level directory exceeds the 11.5 TB tape "
                    "budget and cannot be split automatically:\n"
                    f"        {entry['name']} ({entry['size_bytes'] / 1000**4:.2f} TB)"
                )

        chunks = self._bin_pack_top_level(entries, budget_bytes)
        print(f"[ANALYZER] Files: {total_files} | Total: {total_bytes / 1024**4:.2f} TiB")
        return chunks

    def _bin_pack_top_level(self, entries, budget_bytes):
        chunks = []
        for entry in sorted(entries, key=lambda e: (-e['size_bytes'], e['name'].lower())):
            placed = False
            for chunk in chunks:
                used = sum(e['size_bytes'] for e in chunk)
                if used + entry['size_bytes'] <= budget_bytes:
                    chunk.append(entry)
                    placed = True
                    break
            if not placed:
                chunks.append([entry])
        return chunks

    def render_allocation_plan(self, chunks,
                               budget_bytes=LOCAL_TAPE_BUDGET_BYTES):
        print("\n" + "=" * 60)
        print("LOCAL MULTI-TAPE ALLOCATION PLAN")
        print("=" * 60)
        for idx, chunk in enumerate(chunks, 1):
            used = sum(e['size_bytes'] for e in chunk)
            pct = (used / budget_bytes * 100) if budget_bytes else 0
            print(f"Tape {idx}: {used / 1024**4:.2f} TiB ({pct:.1f}% of 11.5 TB budget)")
            for entry in sorted(chunk, key=lambda e: e['name'].lower()):
                print(f"  - {entry['name']}  {entry['size_bytes'] / 1024**3:.2f} GiB")
        print("-" * 60)


# ==============================================================================
# MODULE B-1: SMART PACKER  (OFFLINE PHASE)
# Packs small files into ZIPs and stages large files; pre-hashes everything
# while the tape drive is idle so the online phase can stream uninterrupted.
# Returns a list of per-file metadata dicts for DB ingestion.
# ==============================================================================

class LTOPacker:
    def __init__(self, max_zip_size_gb):
        self.max_zip_size_gb = max_zip_size_gb

    def run_manifest(self, source_root, dest, threshold_mb, file_entries,
                     bundle_prefix="Bundle"):
        """Pack a selected list of source files into a staging directory.

        file_entries: iterable of {'path', 'rel', 'size'} dicts, where rel is
        relative to source_root and is used for restore metadata.
        """
        if os.path.exists(dest):
            shutil.rmtree(dest)
        os.makedirs(dest, exist_ok=True)

        metadata             = []
        zip_idx              = 1
        zip_path             = os.path.join(dest, f"{bundle_prefix}_{zip_idx:03d}.zip")
        zipf                 = zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_STORED, allowZip64=True)
        current_zip_size     = 0
        files_in_current_zip = 0
        total_packed         = 0
        total_loose          = 0
        errors               = []

        print(f"\n[PACKER] Local sub-chunk staging. "
              f"(Threshold: {threshold_mb:.0f} MB | Max ZIP: {self.max_zip_size_gb:.0f} GB)")

        for entry in file_entries:
            src = entry['path']
            rel = entry['rel']
            file = os.path.basename(src)
            try:
                fsize = entry.get('size')
                if fsize is None:
                    fsize = os.path.getsize(src)
                fsize_mb = fsize / (1024 * 1024)

                if fsize_mb < threshold_mb:
                    zip_rel = rel.replace('\\', '/')
                    if current_zip_size + fsize > self.max_zip_size_gb * 1024**3 * 0.99:
                        zipf.close()
                        _progress_done()
                        print(f"\n -> Sealed {bundle_prefix}_{zip_idx:03d}.zip ({files_in_current_zip} files)")
                        zip_idx += 1
                        zip_path = os.path.join(dest, f"{bundle_prefix}_{zip_idx:03d}.zip")
                        zipf = zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_STORED, allowZip64=True)
                        current_zip_size     = 0
                        files_in_current_zip = 0

                    container = f"{bundle_prefix}_{zip_idx:03d}.zip"
                    hasher = hashlib.sha256()
                    with open(src, 'rb') as fsrc, zipf.open(zip_rel, 'w', force_zip64=True) as zdst:
                        while True:
                            buf = fsrc.read(BUFFER_SIZE)
                            if not buf:
                                break
                            hasher.update(buf)
                            zdst.write(buf)
                    file_hash            = hasher.hexdigest()
                    current_zip_size     += fsize
                    files_in_current_zip += 1
                    total_packed         += 1

                    metadata.append({
                        'file_name':       file,
                        'original_path':   src,
                        'file_size_bytes': fsize,
                        'file_hash':       file_hash,
                        'is_packed':       True,
                        'container_name':  container,
                        'stored_path':     zip_rel,
                    })

                    if total_packed % 500 == 0:
                        _progress_line(f"[PACKING] {total_packed} files packed")

                else:
                    # Large/loose files are not hashed — skipping the extra
                    # full-file read keeps the tape from waiting on Python I/O.
                    dst_path = os.path.join(dest, rel)

                    if not _robocopy_file(src, dst_path, display_name=file):
                        raise RuntimeError(f"robocopy failed for: {src}")
                    total_loose += 1

                    metadata.append({
                        'file_name':       file,
                        'original_path':   src,
                        'file_size_bytes': fsize,
                        'file_hash':       '',
                        'is_packed':       False,
                        'container_name':  None,
                        'stored_path':     rel,
                    })

            except Exception as e:
                _progress_done()
                print(f"\n[ERROR] {file}: {e}")
                errors.append((src, e))

        if files_in_current_zip > 0:
            zipf.close()
            _progress_done()
            print(f"\n -> Sealed {bundle_prefix}_{zip_idx:03d}.zip ({files_in_current_zip} files)")
        else:
            zipf.close()
            if os.path.exists(zip_path) and os.path.getsize(zip_path) < 100:
                os.remove(zip_path)

        if errors:
            raise RuntimeError(
                f"{len(errors)} file(s) failed during local staging; "
                f"first failure: {errors[0][0]} ({errors[0][1]})"
            )

        _progress_done()
        print(f"\n[PACKER] Sub-chunk done: {total_packed} packed | {total_loose} loose.")
        return metadata

    def run(self, source, dest, threshold_mb):
        """
        Pack small files into ZIP bundles; copy large files loose.

        Returns:
            list of dicts  — full metadata (staged backup ready for DB)
            []             — user chose to use existing staging (no new metadata)
            None           — user aborted
        """
        if os.path.exists(dest) and os.listdir(dest):
            print(f"\n[WARNING] Staging directory is not empty: {dest}")
            print("1. Delete staging and repack from scratch")
            print("2. Use existing staged files (packed-file DB records will be skipped)")
            choice = input("Choose (1/2): ").strip()
            if choice == '2':
                print("[PACKER] Using existing staging. DB metadata for packed files will not be generated.")
                return []
            elif choice == '1':
                print("[PACKER] Cleaning staging directory...")
                shutil.rmtree(dest)
            else:
                return None

        os.makedirs(dest, exist_ok=True)

        metadata             = []
        zip_idx              = 1
        zip_path             = os.path.join(dest, f"Bundle_{zip_idx:03d}.zip")
        zipf                 = zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_STORED, allowZip64=True)
        current_zip_size     = 0
        files_in_current_zip = 0
        total_packed         = 0
        total_loose          = 0

        print(f"\n[PACKER] Offline phase — tape idle. (Threshold: {threshold_mb:.0f} MB | Max ZIP: {self.max_zip_size_gb:.0f} GB)")

        for root, _, files in os.walk(source):
            for file in files:
                src = os.path.join(root, file)
                try:
                    fsize    = os.path.getsize(src)
                    fsize_mb = fsize / (1024 * 1024)
                    rel      = os.path.relpath(src, source)

                    if fsize_mb < threshold_mb:
                        zip_rel = rel.replace('\\', '/')  # ZIP entries use POSIX separators
                        # Roll over to a new ZIP bundle if current one is full
                        if current_zip_size + fsize > self.max_zip_size_gb * 1024**3 * 0.99:
                            zipf.close()
                            _progress_done()
                            print(f"\n -> Sealed Bundle_{zip_idx:03d}.zip ({files_in_current_zip} files)")
                            zip_idx += 1
                            zip_path = os.path.join(dest, f"Bundle_{zip_idx:03d}.zip")
                            zipf = zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_STORED, allowZip64=True)
                            current_zip_size     = 0
                            files_in_current_zip = 0

                        container = f"Bundle_{zip_idx:03d}.zip"
                        hasher = hashlib.sha256()
                        with open(src, 'rb') as fsrc, zipf.open(zip_rel, 'w', force_zip64=True) as zdst:
                            while True:
                                buf = fsrc.read(BUFFER_SIZE)
                                if not buf:
                                    break
                                hasher.update(buf)
                                zdst.write(buf)
                        file_hash            = hasher.hexdigest()
                        current_zip_size     += fsize
                        files_in_current_zip += 1
                        total_packed         += 1

                        metadata.append({
                            'file_name':       file,
                            'original_path':   src,
                            'file_size_bytes': fsize,
                            'file_hash':       file_hash,
                            'is_packed':       True,
                            'container_name':  container,
                            'stored_path':     zip_rel,
                        })

                        if total_packed % 500 == 0:
                            _progress_line(f"[PACKING] {total_packed} files packed")

                    else:
                        # Large/loose files are not hashed — skipping the extra
                        # full-file read keeps the tape from waiting on Python I/O.
                        dst_path = os.path.join(dest, rel)

                        if not _robocopy_file(src, dst_path, display_name=file):
                            raise RuntimeError(f"robocopy failed for: {src}")
                        total_loose += 1

                        metadata.append({
                            'file_name':       file,
                            'original_path':   src,
                            'file_size_bytes': fsize,
                            'file_hash':       '',
                            'is_packed':       False,
                            'container_name':  None,
                            'stored_path':     rel,
                        })

                except Exception as e:
                    _progress_done()
                    print(f"\n[ERROR] {file}: {e}")

        if files_in_current_zip > 0:
            zipf.close()
            _progress_done()
            print(f"\n -> Sealed Bundle_{zip_idx:03d}.zip ({files_in_current_zip} files)")
        else:
            zipf.close()
            if os.path.exists(zip_path) and os.path.getsize(zip_path) < 100:
                os.remove(zip_path)

        _progress_done()
        print(f"\n[PACKER] Offline phase done: {total_packed} packed into ZIPs | {total_loose} large files staged (not hashed).")
        return metadata


# ==============================================================================
# MODULE B-2: LTO BACKUP  (ONLINE PHASE)
# Streams staged/source files to tape and commits records to the DB.
# All hashing is done up-front — before robocopy to tape starts — so the
# drive never sits idle waiting on Python disk I/O.
# ==============================================================================

class LTOBackup:
    def __init__(self, db: DatabaseManager, ibm_eject_cmd: str,
                 tape_priority=None, tape_affinity=None, log_dir=None):
        self.db            = db
        self.ibm_eject_cmd = ibm_eject_cmd
        self.tape_priority = tape_priority   # psutil priority class for robocopy
        self.tape_affinity = tape_affinity   # consumer (tape-writer) core set
        self.log_dir       = log_dir or BACKUP_LOG_DIR

    def _write_backup_log(self, details, packer_metadata, hash_map,
                          recovered_direct_existing, skipped_existing,
                          robocopy_cmd, robocopy_result):
        """Write a reviewable text log for one completed tape-write step."""
        try:
            log_dir = os.path.abspath(self.log_dir or BACKUP_LOG_DIR)
            os.makedirs(log_dir, exist_ok=True)

            finished_at = details['finished_at']
            source_token = _safe_log_token(details.get('source'), 'source')
            tape_token = _safe_log_token(details.get('tape_label'), 'tape')
            name_parts = [finished_at.strftime('%Y%m%d_%H%M%S'),
                          tape_token, source_token]
            if details.get('local_session_id') is not None:
                name_parts.append(f"s{int(details['local_session_id']):04d}")
            if details.get('local_chunk_index') is not None:
                name_parts.append(f"c{int(details['local_chunk_index']) + 1:03d}")

            log_path = _unique_path(os.path.join(log_dir, '_'.join(name_parts) + '.log'))
            rc_sum = details.get('rc_sum') or {}
            counts = details.get('record_counts') or {}
            mode = 'staged/packed' if packer_metadata is not None else 'direct'

            def cell(value):
                text = '' if value is None else str(value)
                return text.replace('\t', ' ').replace('\r', ' ').replace('\n', ' ')

            def write_kv(f, key, value):
                f.write(f"{key:<28}: {value}\n")

            with open(log_path, 'w', encoding='utf-8', newline='\n') as f:
                f.write("LTO Backup Log\n")
                f.write("=" * 60 + "\n")
                write_kv(f, "Status", details.get('status', 'completed'))
                write_kv(f, "Started", details['started_at'].isoformat(timespec='seconds'))
                write_kv(f, "Finished", finished_at.isoformat(timespec='seconds'))
                write_kv(f, "Source", details.get('source'))
                write_kv(f, "Tape label", details.get('tape_label'))
                write_kv(f, "Tape drive", details.get('tape_drive'))
                write_kv(f, "Tape root", details.get('tape_root'))
                write_kv(f, "Backup mode", mode)
                if details.get('local_session_id') is not None:
                    write_kv(f, "Local session id", details.get('local_session_id'))
                if details.get('local_chunk_index') is not None:
                    write_kv(f, "Local chunk", int(details['local_chunk_index']) + 1)
                write_kv(f, "Robocopy exit code", robocopy_result.returncode)
                write_kv(f, "Robocopy command", subprocess.list2cmdline(robocopy_cmd))

                f.write("\nSummary\n")
                f.write("-" * 60 + "\n")
                write_kv(f, "Total time", f"{details['total_time_seconds'] / 60:.1f} minutes")
                write_kv(f, "Data copied", f"{_fmt_bytes(details['copied_bytes'])} ({details['copied_bytes']} bytes)")
                write_kv(f, "Planned copy size", f"{_fmt_bytes(details['total_bytes'])} ({details['total_bytes']} bytes)")
                write_kv(f, "Average speed", f"{rc_sum.get('speed_mbs', 0):.1f} MB/s")
                write_kv(f, "Files copied", rc_sum.get('files_copied', 0))
                write_kv(f, "Files skipped", rc_sum.get('files_skipped', 0) + details.get('skipped', 0))
                write_kv(f, "Files failed", rc_sum.get('files_failed', 0))
                if rc_sum.get('elapsed'):
                    write_kv(f, "Robocopy time", rc_sum.get('elapsed'))
                write_kv(f, "Loose files hashed", len(hash_map))
                write_kv(f, "Already on tape", details.get('skipped', 0))
                write_kv(f, "Tape used after backup", f"{_fmt_bytes(details['new_used'])} ({details['new_used']} bytes)")

                if counts:
                    f.write("\nDatabase Records\n")
                    f.write("-" * 60 + "\n")
                    for key in sorted(counts):
                        write_kv(f, key.replace('_', ' ').title(), counts[key])

                f.write("\nFile Manifest\n")
                f.write("-" * 60 + "\n")
                f.write("Status\tPacked\tSizeBytes\tSHA256\tOriginalPath\tTapePath\tContainer\tStoredPath\n")

                if packer_metadata is None:
                    for info in recovered_direct_existing:
                        f.write('\t'.join(cell(v) for v in (
                            'already_on_tape_recovered',
                            'no',
                            info.get('file_size_bytes'),
                            info.get('file_hash'),
                            info.get('original_path'),
                            info.get('stored_path'),
                            '',
                            info.get('stored_path'),
                        )) + "\n")
                    for rel_path, info in hash_map.items():
                        f.write('\t'.join(cell(v) for v in (
                            'copied',
                            'no',
                            info.get('fsize'),
                            info.get('hash'),
                            info.get('src'),
                            info.get('dst'),
                            '',
                            rel_path,
                        )) + "\n")
                    if details.get('skipped', 0) > len(recovered_direct_existing):
                        f.write("# Skipped files already present in the DB are counted above but not expanded here.\n")
                else:
                    skipped_originals = {
                        m.get('original_path')
                        for _, m, _ in skipped_existing
                        if isinstance(m, dict)
                    }
                    tape_root = details.get('tape_root')
                    for m in packer_metadata or []:
                        is_packed = bool(m.get('is_packed'))
                        container = m.get('container_name') if is_packed else ''
                        tape_path = (os.path.join(tape_root, container)
                                     if is_packed and container
                                     else os.path.join(tape_root, m.get('stored_path') or ''))
                        if not is_packed and m.get('original_path') in skipped_originals:
                            status = 'already_on_tape'
                        elif is_packed:
                            status = 'packed_file'
                        else:
                            status = 'copied'
                        f.write('\t'.join(cell(v) for v in (
                            status,
                            'yes' if is_packed else 'no',
                            m.get('file_size_bytes'),
                            m.get('file_hash'),
                            m.get('original_path'),
                            tape_path,
                            container,
                            m.get('stored_path'),
                        )) + "\n")

                if robocopy_result.stdout:
                    f.write("\nRobocopy Stdout\n")
                    f.write("-" * 60 + "\n")
                    f.write(robocopy_result.stdout)
                    if not robocopy_result.stdout.endswith("\n"):
                        f.write("\n")
                if robocopy_result.stderr:
                    f.write("\nRobocopy Stderr\n")
                    f.write("-" * 60 + "\n")
                    f.write(robocopy_result.stderr)
                    if not robocopy_result.stderr.endswith("\n"):
                        f.write("\n")

            return log_path
        except Exception as e:
            print(f"[LOG] Warning: could not write backup log: {e}")
            return None

    def _legacy_eject_tape_unlocked(self, tape_drive):
        print("\n" + "#" * 60)
        print("[LTO] FINALIZING: Ejecting tape...")
        print("[LTO] PLEASE WAIT — this can take 1-2 minutes.")
        print("#" * 60)

        drive_arg = tape_drive.rstrip(":\\")
        exe       = self.ibm_eject_cmd or os.path.join(LTFS_DIR, 'LtfsCmdEject.exe')
        exe_dir   = os.path.dirname(exe) or LTFS_DIR
        cmd       = [exe, drive_arg]

        try:
            result = subprocess.run(cmd, check=True, text=True, capture_output=True,
                                    cwd=exe_dir)
            print("[LTO] Tape ejected successfully!")
            if result.stdout:
                print(result.stdout)
        except subprocess.CalledProcessError as e:
            print(f"[ERROR] Eject failed: {e.stderr}")
            print(f"Try manually: cd /d \"{LTFS_DIR}\" && LtfsCmdEject.exe {drive_arg}")
        except FileNotFoundError:
            print(f"[ERROR] LtfsCmdEject.exe not found in: {LTFS_DIR}")

    def eject_tape(self, tape_drive):
        _acquire_tape_io_lock(f"eject {tape_drive}")
        try:
            return self._legacy_eject_tape_unlocked(tape_drive)
        finally:
            _release_tape_io_lock()

    def run(self, source, tape_drive, tape_label, packer_metadata=None,
            exclude_file_paths=None, exclude_dir_paths=None,
            local_session_id=None, local_chunk_index=None):
        _acquire_tape_io_lock(f"backup write to {tape_drive}")
        try:
            return self._run_locked(
                source, tape_drive, tape_label,
                packer_metadata=packer_metadata,
                exclude_file_paths=exclude_file_paths,
                exclude_dir_paths=exclude_dir_paths,
                local_session_id=local_session_id,
                local_chunk_index=local_chunk_index,
            )
        finally:
            _release_tape_io_lock()

    def _run_locked(self, source, tape_drive, tape_label, packer_metadata=None,
                    exclude_file_paths=None, exclude_dir_paths=None,
                    local_session_id=None, local_chunk_index=None):
        """
        Copy files from source to tape and commit to the database.

        packer_metadata:
            list of dicts  — staged backup with full metadata (from LTOPacker).
                             Hashes already computed; live hashing is skipped.
            []             — staged backup, existing staging, no per-file metadata.
            None           — direct backup from source directory (files not hashed).
        """
        print(f"\n[BACKUP] Starting... Tape: {tape_label} | Drive: {tape_drive}")
        if not _ensure_lto_drive_ready(tape_drive, prefix="[BACKUP]"):
            raise RuntimeError("LTO drive is not ready for backup.")
        if not self.db.tape_exists(tape_label):
            raise RuntimeError(
                f"[DB] Tape '{tape_label}' is not registered; cannot sync file records."
            )
        if packer_metadata == []:
            raise RuntimeError(
                "[DB] Cannot sync staged backup without packer metadata. "
                "Repack the staging data before backing up."
            )

        exclude_file_paths = exclude_file_paths or []
        exclude_dir_paths  = exclude_dir_paths or []
        tape_root = os.path.join(tape_drive, os.path.basename(source))
        os.makedirs(tape_root, exist_ok=True)

        # Build lookup: staging-relative-path -> metadata dict (loose large files only)
        meta_by_rel = {}
        if packer_metadata:
            for m in packer_metadata:
                if not m['is_packed']:
                    meta_by_rel[m['stored_path']] = m

        started_at = datetime.now()
        total_start = time.time()
        record_counts = defaultdict(int)

        # ---------------------------------------------------------------
        # Phase 1 — Build hash_map *before* any tape I/O.
        #   AUTO-PILOT : consume pre-computed hashes from packer_metadata
        #                (packed small files only; loose large files unhashed).
        #   DIRECT     : walk source_dir and stage every new/changed file
        #                (no hashing) while the tape stays idle.
        # ---------------------------------------------------------------
        # hash_map: rel_path -> {'hash', 'fsize', 'src', 'dst'}
        hash_map = {}
        skipped  = 0
        skipped_existing = []
        recovered_direct_existing = []

        if packer_metadata is not None:
            # AUTO-PILOT path (metadata list, possibly empty).
            # Loose large files: pull hashes from packer_metadata.
            # Already-on-tape (same size) files: count as skipped, omit from hash_map.
            print("[BACKUP] Pre-hashed metadata loaded — no live hashing.")
            for m in packer_metadata:
                if m['is_packed']:
                    continue  # bundle ZIPs handled via packer_metadata directly
                rel_path = m['stored_path']
                src      = os.path.join(source, rel_path)
                dst      = os.path.join(tape_root, rel_path)
                if os.path.exists(dst):
                    try:
                        if os.path.getsize(src) == os.path.getsize(dst):
                            skipped += 1
                            skipped_existing.append((rel_path, m, dst))
                            continue
                    except OSError:
                        pass
                hash_map[rel_path] = {
                    'hash':  m.get('file_hash', ''),
                    'fsize': m['file_size_bytes'],
                    'src':   src,
                    'dst':   dst,
                }
        else:
            # DIRECT path — files are written loose to tape and not hashed;
            # skipping the extra full-file read keeps the tape from waiting.
            print("[BACKUP] Walking source files (no hashing)...")
            for root, _, files in os.walk(source):
                rel_folder  = os.path.relpath(root, source)
                dest_folder = os.path.join(tape_root, rel_folder)
                for file in files:
                    src      = os.path.join(root, file)
                    dst      = os.path.join(dest_folder, file)
                    rel_path = os.path.relpath(src, source)
                    if os.path.exists(dst):
                        try:
                            if os.path.getsize(src) == os.path.getsize(dst):
                                skipped += 1
                                if not self.db.file_record_exists(
                                    src, tape_label,
                                    local_session_id=local_session_id,
                                    local_chunk_index=local_chunk_index,
                                ):
                                    recovered_direct_existing.append({
                                        'file_name': file,
                                        'original_path': src,
                                        'file_size_bytes': os.path.getsize(src),
                                        'file_hash': '',
                                        'stored_path': dst,
                                    })
                                continue
                        except OSError:
                            pass
                    try:
                        fsize = os.path.getsize(src)
                        hash_map[rel_path] = {'hash': '', 'fsize': fsize,
                                              'src': src, 'dst': dst}
                    except Exception as e:
                        _progress_done()
                        print(f"\n[WARN] Cannot stat {file}: {e}")

        if packer_metadata is not None:
            # Bundle ZIPs aren't in hash_map; walk staging to size the progress bar.
            total_bytes = 0
            for r, _, fs in os.walk(source):
                for f in fs:
                    try:
                        total_bytes += os.path.getsize(os.path.join(r, f))
                    except OSError:
                        pass
        else:
            total_bytes = sum(v['fsize'] for v in hash_map.values())

        _progress_done()
        print(f"[BACKUP] {len(hash_map)} loose file(s) staged "
              f"({total_bytes / 1024**3:.2f} GB to copy) | {skipped} already on tape.")

        # ---------------------------------------------------------------
        # Phase 2 — Single robocopy call: source directory → tape
        # ---------------------------------------------------------------
        prio_label = (
            (self.tape_priority is not None) and
            {getattr(psutil, 'REALTIME_PRIORITY_CLASS', None): 'REALTIME',
             getattr(psutil, 'HIGH_PRIORITY_CLASS', None): 'HIGH',
             getattr(psutil, 'NORMAL_PRIORITY_CLASS', None): 'NORMAL'}.get(
                 self.tape_priority, 'custom')
        ) or 'default'
        cores_label = (f"cores {self.tape_affinity}"
                       if self.tape_affinity else "all cores")
        _phase('TAPE', f"PC → Tape (LTFS) | {tape_label} | "
                       f"priority={prio_label}, {cores_label}")
        print("[BACKUP] Copying to tape via robocopy...")

        def _dir_size(path):
            total = 0
            try:
                for r, _, fs in os.walk(path):
                    for f in fs:
                        try:
                            total += os.path.getsize(os.path.join(r, f))
                        except OSError:
                            pass
            except OSError:
                pass
            return total

        initial_tape_bytes = _dir_size(tape_root)
        stop_evt = threading.Event()

        def _monitor():
            start_time = time.time()
            while not stop_evt.wait(15):
                elapsed = time.time() - start_time
                _progress_line(
                    f"[COPYING] robocopy active | elapsed {_fmt_eta(elapsed)} | "
                    f"chunk {total_bytes / 1024**3:.1f} GB"
                )

        mon = threading.Thread(target=_monitor, daemon=True)
        mon.start()

        robocopy_cmd = [
            'robocopy', source, tape_root,
             '/E',     # recurse subdirectories including empty ones
             '/J',     # unbuffered I/O — optimised for large files / tape
             '/R:3', '/W:10',
             '/NP',    # no per-file progress %
             '/NDL',   # no directory listing lines
             '/NFL',   # no per-file listing lines (keep job header+summary)
        ]
        if exclude_file_paths:
            robocopy_cmd.extend(['/XF'] + exclude_file_paths)
        if exclude_dir_paths:
            robocopy_cmd.extend(['/XD'] + exclude_dir_paths)

        rc = _run_robocopy_tuned(robocopy_cmd,
                                 priority=self.tape_priority,
                                 affinity=self.tape_affinity)

        stop_evt.set()
        mon.join(timeout=2)
        _progress_done()

        # If the user pressed Ctrl+C, robocopy was terminated mid-write. Skip the
        # DB commit and the eject so the chunk stays resumable and the tape is
        # left mounted for the next run.
        if CANCEL.is_set():
            raise RuntimeError("tape write cancelled by user")

        rc_sum = _parse_robocopy_summary(rc.stdout)

        rc_output = (rc.stdout or '') + '\n' + (rc.stderr or '')
        critical_robocopy_failure = (
            rc.returncode >= 8 or
            rc_sum.get('files_failed', 0) > 0 or
            'ERROR ' in rc_output or
            'RETRY LIMIT EXCEEDED' in rc_output
        )
        if critical_robocopy_failure:
            copied_bytes = rc_sum.get('bytes_copied', 0)
            if copied_bytes <= 0:
                copied_bytes = max(0, _dir_size(tape_root) - initial_tape_bytes)
            new_used = self.db.recalculate_tape_used_space(tape_label)
            log_path = self._write_backup_log(
                {
                    'status': 'failed_critical',
                    'started_at': started_at,
                    'finished_at': datetime.now(),
                    'source': source,
                    'tape_drive': tape_drive,
                    'tape_label': tape_label,
                    'tape_root': tape_root,
                    'local_session_id': local_session_id,
                    'local_chunk_index': local_chunk_index,
                    'total_time_seconds': time.time() - total_start,
                    'total_bytes': total_bytes,
                    'copied_bytes': copied_bytes,
                    'skipped': skipped,
                    'new_used': new_used,
                    'rc_sum': rc_sum,
                    'record_counts': {},
                },
                packer_metadata,
                hash_map,
                recovered_direct_existing,
                skipped_existing,
                robocopy_cmd,
                rc,
            )
            msg = (
                f"CRITICAL: robocopy failed with exit code {rc.returncode}; "
                f"{rc_sum.get('files_failed', 0)} file(s) failed. "
                "No file records were committed to the database."
            )
            if log_path:
                msg += f" Log: {log_path}"
            print(f"[ERROR] {msg}")
            raise RuntimeError(msg)

        if rc.returncode >= 8:
            print(f"[WARN] Robocopy finished with exit code {rc.returncode} "
                  f"— check for errors above.")

        # ---------------------------------------------------------------
        # Phase 3 — DB inserts (only files that were hashed / new this run)
        # ---------------------------------------------------------------
        if packer_metadata is None:
            # Direct backup: every hashed file is a loose tape record
            recovered_count = 0
            for info in recovered_direct_existing:
                inserted = self.db.insert_file(
                    file_name=info['file_name'],
                    original_path=info['original_path'],
                    file_size_bytes=info['file_size_bytes'],
                    file_hash=info['file_hash'],
                    tape_label=tape_label, is_packed=False,
                    container_name=None, stored_path=info['stored_path'],
                    local_session_id=local_session_id,
                    local_chunk_index=local_chunk_index,
                )
                if inserted:
                    recovered_count += 1
                    record_counts['direct_recovered_inserted'] += 1
                else:
                    record_counts['direct_recovered_updated'] += 1
            if recovered_count:
                print(f"[DB] Recovered {recovered_count} existing tape file record(s).")
            for rel_path, info in hash_map.items():
                inserted = self.db.insert_file(
                    file_name=os.path.basename(info['src']),
                    original_path=info['src'],
                    file_size_bytes=info['fsize'],
                    file_hash=info['hash'],
                    tape_label=tape_label, is_packed=False,
                    container_name=None, stored_path=info['dst'],
                    local_session_id=local_session_id,
                    local_chunk_index=local_chunk_index,
                )
                if inserted:
                    record_counts['loose_records_inserted'] += 1
                else:
                    record_counts['loose_records_updated'] += 1

        else:
            # Staged backup: loose large files + batch-insert packed-file records
            recovered_count = 0
            for rel_path, m, dst in skipped_existing:
                if self.db.file_record_exists(
                    m['original_path'], tape_label,
                    local_session_id=local_session_id,
                    local_chunk_index=local_chunk_index,
                ):
                    record_counts['loose_records_skipped_existing'] += 1
                    continue
                inserted = self.db.insert_file(
                    file_name=m['file_name'],
                    original_path=m['original_path'],
                    file_size_bytes=m['file_size_bytes'],
                    file_hash=m.get('file_hash', ''),
                    tape_label=tape_label,
                    is_packed=False,
                    container_name=None,
                    stored_path=dst,
                    local_session_id=local_session_id,
                    local_chunk_index=local_chunk_index,
                )
                if inserted:
                    recovered_count += 1
                    record_counts['loose_recovered_inserted'] += 1
                else:
                    record_counts['loose_recovered_updated'] += 1
            for rel_path, info in hash_map.items():
                file = os.path.basename(info['src'])
                if file.startswith("Bundle_") and file.endswith(".zip"):
                    continue  # bundle records handled below
                if rel_path in meta_by_rel:
                    m = meta_by_rel[rel_path]
                    if self.db.file_record_exists(
                        m['original_path'], tape_label,
                        local_session_id=local_session_id,
                        local_chunk_index=local_chunk_index,
                    ):
                        record_counts['loose_records_skipped_existing'] += 1
                        continue
                    inserted = self.db.insert_file(
                        file_name=file,
                        original_path=m['original_path'],
                        file_size_bytes=info['fsize'],
                        file_hash=info['hash'],
                        tape_label=tape_label, is_packed=False,
                        container_name=None, stored_path=info['dst'],
                        local_session_id=local_session_id,
                        local_chunk_index=local_chunk_index,
                    )
                    if inserted:
                        record_counts['loose_records_inserted'] += 1
                    else:
                        record_counts['loose_records_updated'] += 1
            print("[DB] Recording packed file entries...")
            packed_count = 0
            for m in packer_metadata:
                if m['is_packed']:
                    if self.db.file_record_exists(
                        m['original_path'], tape_label,
                        local_session_id=local_session_id,
                        local_chunk_index=local_chunk_index,
                    ):
                        record_counts['packed_records_skipped_existing'] += 1
                        continue
                    tape_zip_path = os.path.join(tape_root, m['container_name'])
                    inserted = self.db.insert_file(
                        file_name=m['file_name'], original_path=m['original_path'],
                        file_size_bytes=m['file_size_bytes'],
                        file_hash=m.get('file_hash', ''),
                        tape_label=tape_label, is_packed=True,
                        container_name=tape_zip_path,
                        stored_path=m['stored_path'],
                        local_session_id=local_session_id,
                        local_chunk_index=local_chunk_index,
                    )
                    if inserted:
                        record_counts['packed_records_inserted'] += 1
                    else:
                        record_counts['packed_records_updated'] += 1
                    packed_count += 1
            if recovered_count:
                print(f"[DB] Recovered {recovered_count} existing loose file record(s).")
            print(f"[DB] {packed_count} packed file record(s) synchronized.")

        # The robocopy summary parser is English-only; on a localized console
        # it yields 0 bytes. Fall back to the measured growth of the tape
        # directory so used-space accounting still works.
        copied_bytes = rc_sum['bytes_copied']
        if copied_bytes <= 0:
            copied_bytes = max(0, _dir_size(tape_root) - initial_tape_bytes)
        new_used = self.db.recalculate_tape_used_space(tape_label)
        print(f"[DB] Tape used space reconciled to {new_used / 1024**3:.3f} GB.")

        # ---------------------------------------------------------------
        # Phase 4 — Print Robocopy job summary
        # ---------------------------------------------------------------
        total_time = time.time() - total_start
        finished_at = datetime.now()
        log_status = 'completed'
        if rc.returncode >= 8 or rc_sum.get('files_failed', 0):
            log_status = 'completed_with_warnings'
        log_path = self._write_backup_log(
            {
                'status': log_status,
                'started_at': started_at,
                'finished_at': finished_at,
                'source': source,
                'tape_drive': tape_drive,
                'tape_label': tape_label,
                'tape_root': tape_root,
                'local_session_id': local_session_id,
                'local_chunk_index': local_chunk_index,
                'total_time_seconds': total_time,
                'total_bytes': total_bytes,
                'copied_bytes': copied_bytes,
                'skipped': skipped,
                'new_used': new_used,
                'rc_sum': rc_sum,
                'record_counts': dict(record_counts),
            },
            packer_metadata,
            hash_map,
            recovered_direct_existing,
            skipped_existing,
            robocopy_cmd,
            rc,
        )
        print("\n" + "=" * 60)
        print("BACKUP SESSION SUMMARY  [Robocopy]")
        print("=" * 60)
        print(f"Tape            : {tape_label}")
        print(f"Total Time      : {total_time / 60:.1f} minutes")
        print(f"Data Copied     : {copied_bytes / 1024**3:.2f} GB")
        print(f"Avg Speed       : {rc_sum['speed_mbs']:.1f} MB/s")
        print(f"Files Copied    : {rc_sum['files_copied']}")
        print(f"Files Skipped   : {rc_sum['files_skipped'] + skipped}")
        print(f"Files Failed    : {rc_sum['files_failed']}")
        if rc_sum['elapsed']:
            print(f"Robocopy Time   : {rc_sum['elapsed']}")
        if log_path:
            print(f"Backup Log      : {log_path}")
        print("-" * 60)

        self.eject_tape(tape_drive)


# ==============================================================================
# MODULE C: RETRIEVER — Search DB & restore files from tape
# ==============================================================================

class LTORetriever:
    def __init__(self, db: DatabaseManager, tape_drive: str,
                 staging_dir: str, restore_dir: str):
        self.db          = db
        self.tape_drive  = tape_drive
        self.staging_dir = staging_dir
        self.restore_dir = restore_dir

    def _unique_dest(self, file_name):
        """Return a restore path under restore_dir that won't overwrite an
        existing file. Distinct source files that share a basename would
        otherwise silently clobber each other when flattened into restore_dir."""
        base, ext = os.path.splitext(file_name)
        candidate = os.path.join(self.restore_dir, file_name)
        counter = 1
        while os.path.exists(candidate):
            candidate = os.path.join(self.restore_dir, f"{base}_{counter}{ext}")
            counter += 1
        return candidate

    def run(self):
        print("\n--- RETRIEVER: Search & Restore ---")
        print("1. Search by filename / wildcard  (e.g. *.mov, IMG_*)")
        print("2. Search by date range")
        print("3. Search by both")
        print("4. Restore full directory")
        print("5. Restore full backup session")
        opt = input("Option (1-5): ").strip()

        results = []

        if opt in ('1', '2', '3'):
            name_q = date_from = date_to = None
            if opt in ('1', '3'):
                name_q = input("Filename or pattern: ").strip() or None
            if opt in ('2', '3'):
                date_from = input("Backed-up from (YYYY-MM-DD, blank=any): ").strip() or None
                date_to   = input("Backed-up to   (YYYY-MM-DD, blank=any): ").strip() or None
            results = self.db.search_files(name_q, date_from, date_to)

        elif opt == '4':
            dir_q = input("Original directory path (partial ok): ").strip()
            if not dir_q:
                return
            results = self.db.search_by_directory(dir_q)

        elif opt == '5':
            sessions = self.db.list_backup_sessions()
            if not sessions:
                print("[RETRIEVER] No backup sessions found.")
                return
            print(f"\n{'#':>3}  {'Date':<12}  {'Tape':<25}  {'Files':>6}  Size")
            print("-" * 65)
            for i, s in enumerate(sessions, 1):
                size_s = f"{(s['total_bytes'] or 0) / 1024**3:.2f} GB"
                print(f"{i:>3}  {s['session_date']:<12}  {s['tape_label']:<25}  {s['file_count']:>6}  {size_s}")
            print()
            try:
                idx = int(input("Select session # (0 = cancel): ").strip())
            except ValueError:
                return
            if idx < 1 or idx > len(sessions):
                return
            s = sessions[idx - 1]
            results = self.db.search_by_session(s['session_date'], s['tape_label'])

        else:
            return

        if not results:
            print("[RETRIEVER] No matching files found.")
            return

        total_size = sum(r['file_size_bytes'] or 0 for r in results)
        print(f"\n{'ID':>7}  {'Filename':<42}  {'Size':>10}  {'Backup Date':<20}  Tape")
        print("-" * 100)
        for row in results:
            size_s = f"{row['file_size_bytes']/1024**2:.1f} MB"
            date_s = (row['backup_date'] or '')[:19]
            print(f"{row['file_id']:>7}  {row['file_name']:<42}  {size_s:>10}  {date_s:<20}  {row['tape_label']}")
        print(f"\n{len(results)} file(s)  |  {total_size/1024**3:.2f} GB total")

        print()
        sel_raw = input("Enter file ID to restore, ALL to restore all, or 0 to cancel: ").strip()

        if sel_raw == '0' or not sel_raw:
            return

        os.makedirs(self.restore_dir, exist_ok=True)

        if sel_raw.upper() == 'ALL':
            self._restore_many(list(results))
            return

        try:
            sel = int(sel_raw)
        except ValueError:
            print("[RETRIEVER] Invalid input.")
            return

        record = self.db.get_file_by_id(sel)
        if not record:
            print("[RETRIEVER] File ID not found.")
            return

        self._verify_tape(record['tape_label'])
        if record['is_packed']:
            self._restore_packed(record)
        else:
            self._restore_loose(record)

    def _restore_many(self, records):
        total = len(records)
        done  = 0

        # Group by tape so we only ask for each tape once
        by_tape = defaultdict(list)
        for r in records:
            by_tape[r['tape_label']].append(r)

        for tape_label, tape_records in by_tape.items():
            self._verify_tape(tape_label)

            loose  = [r for r in tape_records if not r['is_packed']]
            packed = [r for r in tape_records if r['is_packed']]

            for record in loose:
                self._restore_loose(record)
                done += 1
                print(f"[RESTORE] Progress: {done}/{total}")

            # Group packed files by ZIP bundle so each bundle is copied only once
            by_container = defaultdict(list)
            for r in packed:
                by_container[r['container_name']].append(r)

            for container_path, container_records in by_container.items():
                self._restore_packed_bulk(container_path, container_records)
                done += len(container_records)
                print(f"[RESTORE] Progress: {done}/{total}")

        print(f"\n[RESTORE] Complete. {total} file(s) restored to: {self.restore_dir}")

    def _verify_tape(self, required_label):
        mounted = get_volume_label(self.tape_drive)
        if mounted and mounted.upper() != required_label.upper():
            print(f"\n[TAPE] Required: {required_label}  |  Currently mounted: {mounted}")
            input(f"Please insert tape '{required_label}' and press Enter to continue...")
        elif not mounted:
            print(f"\n[TAPE] Could not auto-detect tape label. Required tape: {required_label}")
            input("Ensure the correct tape is inserted, then press Enter...")

    def _restore_loose(self, record):
        src = record['stored_path']
        dst = self._unique_dest(record['file_name'])
        print(f"\n[RESTORE] Copying loose file: {record['file_name']}")
        _acquire_tape_io_lock(f"restore {record['file_name']}")
        try:
            ok = _robocopy_file(src, dst)
        finally:
            _release_tape_io_lock()
        if ok:
            print(f"[RESTORE] Saved to: {dst}")
            _verify_restored_hash(dst, record)
        else:
            print(f"[ERROR] Restore failed: robocopy error")

    def _restore_packed(self, record):
        tape_zip_path = record['container_name']   # full path of ZIP on tape
        stored_in_zip = record['stored_path']       # relative path inside the ZIP
        local_zip     = os.path.join(self.staging_dir, os.path.basename(tape_zip_path))

        print(f"\n[RESTORE] Packed file inside {os.path.basename(tape_zip_path)}")
        print(f"[RESTORE] Step 1/3: Copying ZIP from tape to staging...")

        os.makedirs(self.staging_dir, exist_ok=True)
        _acquire_tape_io_lock(f"restore {os.path.basename(tape_zip_path)}")
        try:
            ok = _robocopy_file(tape_zip_path, local_zip)
        finally:
            _release_tape_io_lock()
        if not ok:
            print(f"[ERROR] Could not copy ZIP from tape: robocopy error")
            return

        print(f"[RESTORE] Step 2/3: Extracting '{record['file_name']}' from ZIP...")
        dst = self._unique_dest(record['file_name'])
        try:
            with zipfile.ZipFile(local_zip, 'r') as zf:
                candidates = [n for n in zf.namelist()
                              if n == stored_in_zip
                              or os.path.basename(n) == record['file_name']]
                if not candidates:
                    print(f"[ERROR] '{record['file_name']}' not found inside ZIP.")
                    print(f"        Searched stored path: {stored_in_zip}")
                    return
                with zf.open(candidates[0]) as zf_src, open(dst, 'wb') as out:
                    shutil.copyfileobj(zf_src, out)
            print(f"[RESTORE] Saved to: {dst}")
            _verify_restored_hash(dst, record)
        except Exception as e:
            print(f"[ERROR] Extraction failed: {e}")
        finally:
            print("[RESTORE] Step 3/3: Removing staging ZIP...")
            try:
                os.remove(local_zip)
            except OSError:
                pass

    def _restore_packed_bulk(self, tape_zip_path, records):
        """Extract multiple files from a single ZIP bundle in one pass."""
        local_zip = os.path.join(self.staging_dir, os.path.basename(tape_zip_path))
        print(f"\n[RESTORE] Copying {os.path.basename(tape_zip_path)} from tape to staging...")
        os.makedirs(self.staging_dir, exist_ok=True)
        _acquire_tape_io_lock(f"restore {os.path.basename(tape_zip_path)}")
        try:
            ok = _robocopy_file(tape_zip_path, local_zip)
        finally:
            _release_tape_io_lock()
        if not ok:
            print(f"[ERROR] Could not copy ZIP from tape: robocopy error")
            return
        print(f"[RESTORE] Extracting {len(records)} file(s)...")
        try:
            with zipfile.ZipFile(local_zip, 'r') as zf:
                zip_names = zf.namelist()
                for record in records:
                    stored_in_zip = record['stored_path']
                    dst = self._unique_dest(record['file_name'])
                    candidates = [n for n in zip_names
                                  if n == stored_in_zip
                                  or os.path.basename(n) == record['file_name']]
                    if not candidates:
                        print(f"[ERROR] '{record['file_name']}' not found in ZIP.")
                        continue
                    try:
                        with zf.open(candidates[0]) as zf_src, open(dst, 'wb') as out:
                            shutil.copyfileobj(zf_src, out)
                        print(f"[OK] {record['file_name']}")
                        _verify_restored_hash(dst, record)
                    except Exception as e:
                        print(f"[ERROR] {record['file_name']}: {e}")
        except Exception as e:
            print(f"[ERROR] ZIP extraction failed: {e}")
        finally:
            try:
                os.remove(local_zip)
            except OSError:
                pass


# ==============================================================================
# MODULE E: REMOTE ORCHESTRATOR
# SSH-scan → chunk → SCP fetch → LTOPacker → LTOBackup → flush
# Supports resumeable sessions via remote_sessions + remote_manifest tables.
# ==============================================================================

class _NoEjectBackup(LTOBackup):
    """LTOBackup variant that suppresses the automatic post-backup tape eject.
    RemoteOrchestrator uses this for every chunk so the tape stays mounted,
    then calls eject_tape() once explicitly after the final chunk."""
    def eject_tape(self, tape_drive):
        pass


class LocalOrchestrator:
    """Persistent local multi-tape archive workflow."""

    def __init__(self, cfg, db):
        self.cfg = cfg
        self.db = db
        self.source_dir = cfg.source_dir
        self.staging_dir = cfg.staging_dir
        self.fill_pct = cfg.staging_fill_pct

    def run(self):
        source_dir = os.path.abspath(self.source_dir)
        existing = self.db.get_active_local_session(source_dir)
        if existing:
            pending = self.db.get_local_pending_chunks(existing['session_id'])
            done = existing['total_chunks'] - len(pending)
            print(f"\n[LOCAL] Found active session: {existing['session_label']}")
            print(f"        Created : {existing['created_at']}")
            print(f"        Progress: {done}/{existing['total_chunks']} chunks completed.")
            print(f"        Mode    : {existing['backup_mode']}")
            print("1. Resume from first incomplete chunk")
            print("2. Abandon and start a fresh session")
            print("0. Cancel")
            choice = input("Choose: ").strip()
            if choice == '1':
                self._run_session(existing['session_id'])
                return
            if choice == '2':
                self.db.update_local_session(existing['session_id'], status='abandoned')
            else:
                return

        self._start_new_session(source_dir)

    def _start_new_session(self, source_dir):
        analyzer = LTOAnalyzer()
        recommended_pack = analyzer.analyze(source_dir, self.cfg.zip_threshold_mb)
        backup_mode = self._choose_backup_mode(recommended_pack)
        if backup_mode is None:
            print("[ABORTED] Local session was not created.")
            return

        chunks = analyzer.build_local_allocation_plan(source_dir)
        analyzer.render_allocation_plan(chunks)
        confirm = input("Create this local multi-tape session? Type YES to continue: ").strip()
        if confirm != 'YES':
            print("[ABORTED] Local session was not created.")
            return

        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        session_label = f"LOCAL_{os.path.basename(source_dir.rstrip(os.sep))}_{ts}"
        session_id = self.db.create_local_session(
            session_label, source_dir, chunks, backup_mode=backup_mode
        )
        print(f"[LOCAL] Session created: {session_label} (id {session_id}, mode {backup_mode})")
        self._run_session(session_id)

    def _choose_backup_mode(self, recommended_pack):
        recommended = 'pack' if recommended_pack else 'direct'
        labels = {
            'direct': 'Direct backup',
            'pack': 'AUTO-PILOT / staged packing',
        }
        choices = [recommended, 'direct' if recommended == 'pack' else 'pack']

        print("\n[LOCAL] Choose backup mode:")
        for idx, mode in enumerate(choices, 1):
            suffix = " (Recommended)" if mode == recommended else ""
            if mode == 'direct':
                detail = "copy selected top-level folders directly to tape"
            else:
                detail = f"pack files < {self.cfg.zip_threshold_mb:.0f} MB and stage the batch"
            print(f"{idx}. {labels[mode]}{suffix} - {detail}")
        print("0. Cancel")

        while True:
            choice = input("Choose backup mode: ").strip()
            if choice == '0':
                return None
            if choice in ('1', '2'):
                return choices[int(choice) - 1]
            print("[ERROR] Invalid selection.")

    def _run_session(self, session_id):
        session = self.db.get_local_session(session_id)
        if not session:
            print(f"[LOCAL] Session not found: {session_id}")
            return

        pending = self.db.get_local_pending_chunks(session_id)
        if not pending:
            self.db.update_local_session(
                session_id, status='completed',
                completed_at=datetime.now().isoformat()
            )
            print("[LOCAL] Session complete. All chunks archived.")
            return

        if not _ensure_lto_drive_ready(self.cfg.lto_drive):
            return

        print(f"\n[LOCAL] Processing {len(pending)} pending chunk(s).")
        for loop_idx, chunk_index in enumerate(pending):
            if loop_idx > 0:
                # The previous chunk finished and ejected its tape. A local
                # session uses one tape per chunk, so pause for a tape swap and
                # re-verify drive readiness before continuing.
                print("\n[LOCAL] The previous tape has been ejected.")
                input("Insert the NEXT blank/formatted tape, wait until ready, "
                      "then press Enter...")
                if not _ensure_lto_drive_ready(self.cfg.lto_drive):
                    print("[LOCAL] Drive not ready. Re-run option 1 to resume.")
                    return
            entries = self.db.get_local_chunk_entries(session_id, chunk_index)
            print(f"\n[LOCAL] === Tape {chunk_index + 1}/{session['total_chunks']} ===")
            ok = self._process_chunk(session, chunk_index, entries)
            if not ok:
                print(f"[LOCAL] Chunk {chunk_index + 1} stopped. Re-run option 1 to resume.")
                return

        self.db.update_local_session(
            session_id, status='completed',
            completed_at=datetime.now().isoformat()
        )
        print("\n[LOCAL] Session complete. All chunks archived.")

    def _process_chunk(self, session, chunk_index, entries):
        tape_label = self._prepare_tape_for_chunk(session, chunk_index, entries)
        if not tape_label:
            return False

        self.db.assign_local_chunk_tape(session['session_id'], chunk_index, tape_label)
        self.db.update_local_chunk_status(session['session_id'], chunk_index, 'staged')

        files = self._collect_chunk_files(session['source_dir'], entries)
        backup_mode = session['backup_mode'] if 'backup_mode' in session.keys() else 'auto'
        if backup_mode == 'direct':
            if self._can_direct_copy_entries(entries):
                return self._process_direct_chunk(session, chunk_index, entries, tape_label)
            print("[LOCAL] Direct mode cannot copy loose root-level files as a "
                  "separate tape chunk; using staged packing for this chunk.")
        elif backup_mode == 'auto':
            if self._can_direct_copy_entries(entries) and not self._should_pack_chunk(files):
                return self._process_direct_chunk(session, chunk_index, entries, tape_label)
        else:
            print("[LOCAL] AUTO-PILOT selected: staging and packing this chunk.")

        already = self.db.get_local_indexed_original_paths(
            session['session_id'], chunk_index, tape_label
        )
        batches = self._make_batches(files)
        if not batches:
            print("[LOCAL] No files to process for this chunk.")
            self.db.update_local_chunk_status(session['session_id'], chunk_index, 'backed_up')
            return True

        for batch_index, batch in enumerate(batches):
            pending = [f for f in batch if f['path'] not in already]
            if not pending:
                print(f"[LOCAL] Batch {batch_index + 1}/{len(batches)} already indexed; skipping.")
                continue

            batch_name = self._batch_name(session['session_id'], chunk_index, batch_index)
            pack_dir = os.path.join(self.staging_dir, batch_name)
            bundle_prefix = f"Bundle_s{session['session_id']:04d}_c{chunk_index + 1:03d}_b{batch_index + 1:03d}"
            print(f"\n[LOCAL] Batch {batch_index + 1}/{len(batches)}: "
                  f"{len(pending)} file(s), {sum(f['size'] for f in pending) / 1024**3:.2f} GiB")

            try:
                metadata = LTOPacker(self.cfg.max_zip_size_gb).run_manifest(
                    source_root=session['source_dir'],
                    dest=pack_dir,
                    threshold_mb=self.cfg.zip_threshold_mb,
                    file_entries=pending,
                    bundle_prefix=bundle_prefix,
                )
                exclude_files, exclude_dirs = self._build_resume_excludes(
                    session['session_id'], chunk_index, tape_label,
                    batch_name, pack_dir
                )
                _NoEjectBackup(
                    self.db,
                    self.cfg.ibm_eject_cmd,
                    log_dir=self.cfg.backup_log_dir,
                ).run(
                    source=pack_dir,
                    tape_drive=self.cfg.lto_drive,
                    tape_label=tape_label,
                    packer_metadata=metadata,
                    exclude_file_paths=exclude_files,
                    exclude_dir_paths=exclude_dirs,
                    local_session_id=session['session_id'],
                    local_chunk_index=chunk_index,
                )
                already.update(m['original_path'] for m in metadata)
            except Exception as e:
                print(f"[LOCAL] Batch failed: {e}")
                self._cleanup_dir(pack_dir)
                return False
            finally:
                self._cleanup_dir(pack_dir)

        self.db.update_local_chunk_status(session['session_id'], chunk_index, 'backed_up')
        LTOBackup(
            self.db,
            self.cfg.ibm_eject_cmd,
            log_dir=self.cfg.backup_log_dir,
        ).eject_tape(self.cfg.lto_drive)
        return True

    def _prepare_tape_for_chunk(self, session, chunk_index, entries):
        assigned = next((e['tape_label'] for e in entries if e['tape_label']), None)
        detected = get_volume_label(self.cfg.lto_drive)
        if detected:
            print(f"[TAPE] Detected label: {detected}")
            tape_label = detected
        else:
            print("[TAPE] Could not auto-detect tape label.")
            tape_label = input("Enter tape Volume Label manually (or Enter to cancel): ").strip()
        if not tape_label:
            print("[ABORTED] No tape label provided.")
            return None

        if assigned and tape_label.upper() != assigned.upper():
            print(f"[TAPE] This chunk is assigned to '{assigned}', "
                  f"but '{tape_label}' is mounted.")
            return None

        if not assigned:
            root_empty = self._tape_root_is_empty()
            record_count = self.db.count_tape_file_records(tape_label)

            if not self.db.tape_exists(tape_label):
                if not root_empty:
                    print(f"[TAPE] Mounted tape '{tape_label}' is not registered "
                          "and is not empty. Register it first or use a blank tape.")
                    return None
                print(f"[TAPE] Registering fresh tape '{tape_label}'.")
                self.db.register_tape(tape_label, 12288)
            elif not root_empty or record_count > 0:
                if not self._ensure_chunk_fits_tape(tape_label, entries):
                    return None
                print(f"[TAPE] Appending to registered tape '{tape_label}' "
                      f"({record_count} indexed file record(s) already present).")

        return tape_label

    def _tape_root_is_empty(self):
        _acquire_tape_io_lock(f"inspect tape root {self.cfg.lto_drive}")
        try:
            try:
                return len(os.listdir(self.cfg.lto_drive)) == 0
            except OSError as e:
                print(f"[TAPE] Cannot inspect tape root: {e}")
                return False
        finally:
            _release_tape_io_lock()

    def _ensure_chunk_fits_tape(self, tape_label, entries):
        planned_bytes = sum(e['dir_size_bytes'] for e in entries)

        _acquire_tape_io_lock(f"read free space {self.cfg.lto_drive}")
        try:
            disk_free = shutil.disk_usage(self.cfg.lto_drive).free
            if planned_bytes > disk_free:
                print(f"[TAPE] '{tape_label}' does not have enough LTFS free "
                      f"space for this chunk ({planned_bytes / 1024**3:.2f} GiB "
                      f"needed, {disk_free / 1024**3:.2f} GiB free).")
                return False
        except OSError as e:
            print(f"[TAPE] Cannot read LTFS free space: {e}")
        finally:
            _release_tape_io_lock()

        tape = self.db.get_tape(tape_label)
        if not tape:
            print(f"[DB] Tape '{tape_label}' is not registered.")
            return False

        used_bytes = self.db.recalculate_tape_used_space(tape_label)
        capacity_gb = tape['total_capacity']
        if capacity_gb:
            capacity_bytes = int(capacity_gb * 1024**3)
            available_bytes = capacity_bytes - used_bytes
            if planned_bytes > available_bytes:
                print(f"[TAPE] '{tape_label}' does not have enough indexed "
                      f"capacity for this chunk ({planned_bytes / 1024**3:.2f} "
                      f"GiB needed, {max(0, available_bytes) / 1024**3:.2f} "
                      "GiB available in DB).")
                return False

        return True

    def _collect_chunk_files(self, source_dir, entries):
        collected = []
        for entry in entries:
            top = entry['top_level_dir']
            if top == ROOT_FILES_GROUP:
                try:
                    scan = list(os.scandir(source_dir))
                except OSError as e:
                    raise RuntimeError(f"Cannot scan source root: {e}")
                for item in scan:
                    if item.is_file():
                        collected.append(self._file_entry(source_dir, item.path))
            else:
                root = os.path.join(source_dir, top)
                for cur, _, files in os.walk(root):
                    for file in files:
                        collected.append(self._file_entry(source_dir, os.path.join(cur, file)))
        return sorted(collected, key=lambda f: f['rel'].lower())

    def _file_entry(self, source_dir, path):
        size = os.path.getsize(path)
        return {
            'path': path,
            'rel': os.path.relpath(path, source_dir),
            'size': size,
        }

    def _can_direct_copy_entries(self, entries):
        return all(e['top_level_dir'] != ROOT_FILES_GROUP for e in entries)

    def _should_pack_chunk(self, files):
        total_files = len(files)
        total_bytes = sum(f['size'] for f in files)
        small_files = [
            f for f in files
            if (f['size'] / (1024 * 1024)) < self.cfg.zip_threshold_mb
        ]
        small_bytes = sum(f['size'] for f in small_files)
        should_pack, file_ratio, byte_ratio = _auto_pack_decision(
            total_files, total_bytes, len(small_files), small_bytes
        )
        if should_pack:
            print(f"[LOCAL] AUTO-PILOT: packing {len(small_files)} small file(s) "
                  f"({byte_ratio*100:.2f}% of chunk data).")
        else:
            print(f"[LOCAL] DIRECT: {len(small_files)} file(s) are under "
                  f"{self.cfg.zip_threshold_mb:.0f} MB, but only "
                  f"{byte_ratio*100:.2f}% of chunk data; skipping staging.")
        return should_pack

    def _process_direct_chunk(self, session, chunk_index, entries, tape_label):
        print("[LOCAL] Direct chunk copy: selected top-level directories will be "
              "copied from source to tape without staging large files.")
        try:
            for entry in sorted(entries, key=lambda e: e['top_level_dir'].lower()):
                source = os.path.join(session['source_dir'], entry['top_level_dir'])
                if not os.path.isdir(source):
                    raise RuntimeError(f"Direct source directory not found: {source}")
                print(f"\n[LOCAL] Direct backup: {entry['top_level_dir']} "
                      f"({entry['dir_size_bytes'] / 1024**3:.2f} GiB)")
                _NoEjectBackup(
                    self.db,
                    self.cfg.ibm_eject_cmd,
                    log_dir=self.cfg.backup_log_dir,
                ).run(
                    source=source,
                    tape_drive=self.cfg.lto_drive,
                    tape_label=tape_label,
                    packer_metadata=None,
                    local_session_id=session['session_id'],
                    local_chunk_index=chunk_index,
                )
        except Exception as e:
            print(f"[LOCAL] Direct chunk failed: {e}")
            return False

        self.db.update_local_chunk_status(session['session_id'], chunk_index, 'backed_up')
        LTOBackup(
            self.db,
            self.cfg.ibm_eject_cmd,
            log_dir=self.cfg.backup_log_dir,
        ).eject_tape(self.cfg.lto_drive)
        return True

    def _make_batches(self, files):
        batches = []
        current = []
        current_size = 0
        for entry in files:
            budget = self._staging_budget()
            if entry['size'] > budget:
                raise RuntimeError(
                    f"File exceeds current staging budget "
                    f"({entry['size'] / 1024**3:.2f} GiB > {budget / 1024**3:.2f} GiB): "
                    f"{entry['path']}"
                )
            if current and current_size + entry['size'] > budget:
                batches.append(current)
                current = []
                current_size = 0
            current.append(entry)
            current_size += entry['size']
        if current:
            batches.append(current)
        return batches

    def _staging_budget(self):
        os.makedirs(self.staging_dir, exist_ok=True)
        free = shutil.disk_usage(self.staging_dir).free
        return max(1, int(free * self.fill_pct))

    def _batch_name(self, session_id, chunk_index, batch_index):
        return f"_local_s{session_id:04d}_c{chunk_index + 1:03d}_b{batch_index + 1:03d}"

    def _build_resume_excludes(self, session_id, chunk_index, tape_label,
                               batch_name, pack_dir):
        tape_batch_root = os.path.abspath(os.path.join(self.cfg.lto_drive, batch_name))
        exclude_files = []
        for tape_path in self.db.get_local_written_tape_paths(session_id, chunk_index, tape_label):
            try:
                abs_tape_path = os.path.abspath(tape_path)
                rel = os.path.relpath(abs_tape_path, tape_batch_root)
            except ValueError:
                continue
            if rel.startswith('..') or rel == '.':
                continue
            exclude_files.append(os.path.join(pack_dir, rel))
        if exclude_files:
            print(f"[LOCAL] Resume excludes: {len(exclude_files)} already indexed tape object(s).")
        return exclude_files, []

    def _cleanup_dir(self, path):
        if os.path.exists(path):
            try:
                shutil.rmtree(path)
                print(f"[LOCAL] Cleaned staging: {path}")
            except OSError as e:
                print(f"[LOCAL] Warning - could not clean {path}: {e}")


class RemoteOrchestrator:
    """Orchestrates archiving files from a remote Linux host to LTO tape.

    Pipeline per chunk:
      1. SSH find  → file manifest (paths + sizes)
      2. Greedy bin-pack into staging-sized chunks
      3. Per chunk: SCP fetch → LTOPacker.run() → LTOBackup.run() → flush staging

    Sessions are persisted in remote_sessions / remote_manifest so an
    interrupted run can be resumed from the last completed chunk.
    """

    def __init__(self, cfg, db):
        self.cfg          = cfg
        self.db           = db
        self.remote_host  = cfg.remote_host
        self.remote_user  = cfg.remote_user
        self.remote_password = cfg.remote_password
        self.remote_path  = cfg.remote_path
        self.remote_scan_paths = cfg.remote_scan_paths
        self.remote_session_path = self._remote_session_key()
        self.confirm_before_backup = cfg.confirm_before_backup
        self.staging_dir  = cfg.staging_dir
        self.fill_pct     = cfg.staging_fill_pct

        # --- continuous-streaming pipeline tuning (from [PERFORMANCE]) --------
        self.chunk_cap_bytes   = int(cfg.chunk_cap_gb * 1024**3)
        self.staging_max_bytes = int(cfg.staging_max_gb * 1024**3)
        self.prefetch_ahead    = cfg.prefetch_chunks_ahead
        self.eject_after_pack  = cfg.eject_after_pack
        self.ssh_cipher        = cfg.ssh_cipher
        self.use_mbuffer       = cfg.use_mbuffer
        self.mbuffer_size      = cfg.mbuffer_size
        self.tape_priority     = _priority_class(cfg.robocopy_priority)
        self.fetch_cores, self.tape_cores = compute_affinity_sets(cfg.cpu_affinity)

        # Producer/consumer coordination (initialised per session).
        self._staged_bytes = 0                 # bytes currently resident in staging
        self._staged_lock  = threading.Lock()
        self._producer_err = None              # first fatal producer error, if any

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def run(self):
        self._validate_config()

        existing = self.db.get_active_remote_session(self.remote_host, self.remote_session_path)
        if existing:
            pending = self.db.get_pending_chunks(existing['session_id'])
            total   = self.db.count_chunks(existing['session_id'])
            done    = total - len(pending)
            print(f"\n[REMOTE] Found active session: {existing['session_label']}")
            print(f"         Created : {existing['created_at']}")
            print(f"         Progress: {done}/{total} chunks completed.")
            print("1. Resume from last completed chunk")
            print("2. Abandon and start a fresh session")
            print("0. Cancel")
            choice = input("Choose: ").strip()
            if choice == '1':
                self._run_session(existing['session_id'])
                return
            elif choice == '2':
                print("[REMOTE] Starting a fresh-session scan. The current session "
                      "will remain resumable until the replacement is approved.")
                self._start_new_session(replacing_session=existing)
                return
            else:
                return

        self._start_new_session()

    # ------------------------------------------------------------------
    # Session lifecycle
    # ------------------------------------------------------------------

    def _validate_config(self):
        missing = [k for k in ('remote_host', 'remote_user', 'remote_path')
                   if not getattr(self.cfg, k)]
        if not self.remote_scan_paths:
            missing.append('remote_selected_paths')
        if missing:
            raise RuntimeError(
                f"[REMOTE] Missing values in [REMOTE] config section: "
                f"{', '.join(missing)}\n"
                f"Edit config.ini and fill in remote_host, remote_user, remote_path."
            )

    def _remote_session_key(self):
        if not self.remote_scan_paths or self.remote_scan_paths == [self.remote_path]:
            return self.remote_path
        return self.remote_path + '\n' + '\n'.join(self.remote_scan_paths)

    def _start_new_session(self, replacing_session=None):
        self._cleanup_remote_staging_dirs()

        tape_label = self._resolve_tape_label()
        if not tape_label:
            return
        if not _ensure_lto_drive_ready(self.cfg.lto_drive):
            return

        ts            = datetime.now().strftime('%Y%m%d_%H%M%S')
        session_label = f"REMOTE_{self.remote_host.split('.')[0]}_{ts}"

        print(f"\n[REMOTE] Session : {session_label}")
        print(f"[REMOTE] Base    : {self.remote_user}@{self.remote_host}:{self.remote_path}")
        if self.remote_scan_paths == [self.remote_path]:
            print(f"[REMOTE] Scanning {self.remote_path} ...")
        else:
            print("[REMOTE] Selected paths:")
            for path in self.remote_scan_paths:
                print(f"  - {path}")

        manifest = self._scan_remote()
        if not manifest:
            print("[REMOTE] No files found on remote host. Aborting.")
            return

        total_bytes = sum(sz for _, sz in manifest)
        print(f"[REMOTE] Found {len(manifest)} file(s) "
              f"({total_bytes / 1024**3:.2f} GB total).")

        chunks = self._bin_pack(manifest)
        print(f"[REMOTE] Split into {len(chunks)} chunk(s) "
              f"(staging budget: {self._chunk_budget() / 1024**3:.2f} GB each).")

        if not self._confirm_start(tape_label, len(manifest), total_bytes, len(chunks)):
            if replacing_session:
                print("[REMOTE] Cancelled before creating backup session. "
                      f"Previous session remains resumable: "
                      f"{replacing_session['session_label']}")
            else:
                print("[REMOTE] Cancelled before creating backup session.")
            return

        if replacing_session:
            self.db.update_remote_session(
                replacing_session['session_id'],
                status='abandoned',
            )
            print(f"[REMOTE] Abandoned session: {replacing_session['session_label']}")

        session_id = self.db.create_remote_session(
            session_label=session_label,
            remote_host=self.remote_host,
            remote_user=self.remote_user,
            remote_path=self.remote_session_path,
            tape_label=tape_label,
            staging_dir=self.staging_dir,
        )
        self.db.update_remote_session(
            session_id,
            total_files=len(manifest),
            total_bytes=total_bytes,
            chunk_count=len(chunks),
        )

        rows = []
        for chunk_idx, chunk_files in enumerate(chunks):
            for remote_fpath, fsize in chunk_files:
                rows.append((chunk_idx, remote_fpath,
                              os.path.basename(remote_fpath), fsize))
        self.db.insert_remote_manifest_batch(session_id, rows)

        if not self.db.tape_exists(tape_label):
            print(f"[TAPE] '{tape_label}' not in database. Registering...")
            cap = input("Tape capacity in GB (default 12288 for 12 TB, Enter to skip): ").strip()
            self.db.register_tape(tape_label, int(cap) if cap.isdigit() else 12288)

        self._run_session(session_id)

    def _resolve_tape_label(self):
        detected = get_volume_label(self.cfg.lto_drive)
        if detected:
            print(f"[TAPE] Detected label: {detected}")
            return detected
        print("[TAPE] Could not auto-detect tape label.")
        label = input("Enter tape Volume Label manually (or Enter to cancel): ").strip()
        return label if label else None

    def _confirm_start(self, tape_label, file_count, total_bytes, chunk_count):
        if not self.confirm_before_backup:
            return True
        print("\n[REMOTE] Approval required before backup starts.")
        print(f"  Host : {self.remote_user}@{self.remote_host}")
        print(f"  Tape : {tape_label}")
        print(f"  Base : {self.remote_path}")
        print(f"  Files: {file_count} ({total_bytes / 1024**3:.2f} GB)")
        print(f"  Plan : {chunk_count} chunk(s)")
        print("  Paths:")
        for path in self.remote_scan_paths:
            print(f"    - {path}")
        choice = input("Type 'yes' to start writing to tape: ").strip().lower()
        return choice == 'yes'

    # ------------------------------------------------------------------
    # Remote scanning
    # ------------------------------------------------------------------

    def _scan_remote(self):
        """SSH find with -printf '%s %p\n' to get size + path for every file."""
        quoted_paths = ' '.join(shlex.quote(path) for path in self.remote_scan_paths)
        find_cmd = f"find {quoted_paths} -type f -printf '%s %p\\n'"
        result   = _ssh_run(
            self.remote_user,
            self.remote_host,
            find_cmd,
            capture=True,
            password=self.remote_password,
        )
        stdout = result.stdout or ''
        stderr = (result.stderr or '').strip()
        # `find` returns a non-zero exit (typically 1) when it cannot descend
        # into some directories (e.g. "Permission denied"), even though it has
        # still listed every file it *could* read. Only abort when SSH itself
        # failed (exit 255) or nothing usable came back; otherwise warn and use
        # the partial listing.
        if result.returncode == 255 or (result.returncode != 0 and not stdout.strip()):
            raise RuntimeError(
                f"[REMOTE] SSH scan failed (exit {result.returncode}):\n{stderr}"
            )
        if result.returncode != 0 and stderr:
            print(
                f"[REMOTE] Scan completed with warnings (find exit {result.returncode}); "
                f"some paths were skipped:\n{stderr}"
            )
        manifest = []
        for line in stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            parts = line.split(' ', 1)
            if len(parts) != 2:
                continue
            try:
                manifest.append((parts[1].strip(), int(parts[0])))
            except ValueError:
                continue
        return manifest

    # ------------------------------------------------------------------
    # Bin-packing
    # ------------------------------------------------------------------

    def _chunk_budget(self):
        # Cap each chunk at chunk_cap_gb so the deep-prefetch pipeline can keep
        # 2+ chunks resident on the NVMe staging disk under the staging_max cap.
        free_budget = int(shutil.disk_usage(self.staging_dir).free * self.fill_pct)
        return min(free_budget, self.chunk_cap_bytes)

    def _bin_pack(self, manifest):
        """Greedy largest-first bin-packing into chunks that fit staging budget.
        Files larger than the budget get their own single-file chunk."""
        budget  = self._chunk_budget()
        chunks  = []
        current = []
        cur_sz  = 0

        for remote_path, fsize in sorted(manifest, key=lambda x: x[1], reverse=True):
            if fsize > budget:
                print(f"[WARN] File exceeds staging budget "
                      f"({fsize/1024**3:.2f} GB > {budget/1024**3:.2f} GB), "
                      f"placing in dedicated chunk: {os.path.basename(remote_path)}")
                chunks.append([(remote_path, fsize)])
                continue
            if cur_sz + fsize > budget and current:
                chunks.append(current)
                current = []
                cur_sz  = 0
            current.append((remote_path, fsize))
            cur_sz += fsize

        if current:
            chunks.append(current)
        return chunks

    # ------------------------------------------------------------------
    # Pipeline execution
    # ------------------------------------------------------------------

    def _run_session(self, session_id):
        """Stream pending chunks to tape with a deep-prefetch pipeline.

        A background producer fetches + packs chunks onto NVMe staging up to
        `prefetch_ahead` chunks in front of the tape writer, while this thread
        (the consumer) keeps robocopy streaming to the LTO drive. The staging
        footprint is capped (backpressure) so the disk never overruns, and the
        tape never starves on the network (no shoe-shining)."""
        session_row    = self.db.conn.execute(
            "SELECT * FROM remote_sessions WHERE session_id = ?", (session_id,)
        ).fetchone()
        tape_label     = session_row['tape_label']
        pending_chunks = self.db.get_pending_chunks(session_id)
        total_chunks   = self.db.count_chunks(session_id)
        done_count     = total_chunks - len(pending_chunks)

        if not pending_chunks:
            print("[REMOTE] All chunks already completed.")
            self.db.update_remote_session(
                session_id, status='completed',
                completed_at=datetime.now().isoformat()
            )
            return

        if not _ensure_lto_drive_ready(self.cfg.lto_drive):
            return

        # --- per-session pipeline state ---
        self._staged_bytes   = 0
        self._producer_err   = None
        self._producer_chunk = None
        self._consumer_chunk = None
        last_chunk = pending_chunks[-1]

        # Pin hashing/packing (this process) to the fetch cores so the tape
        # writer's cores stay free of SSH decryption + Python packing.
        if self.fetch_cores:
            pin_current_process(self.fetch_cores, label='fetch/pack')

        _phase('PIPELINE', f"Streaming {len(pending_chunks)} chunk(s) to tape "
                           f"({done_count}/{total_chunks} already done) | prefetch "
                           f"{self.prefetch_ahead} ahead | staging cap "
                           f"{self.staging_max_bytes / 1024**3:.0f} GB")

        chunk_files_map = {ci: self.db.get_chunk_files(session_id, ci)
                           for ci in pending_chunks}
        planned = {ci: sum(r['file_size_bytes'] for r in chunk_files_map[ci])
                   for ci in pending_chunks}

        ready_q       = queue.Queue(maxsize=self.prefetch_ahead)
        stop_pipeline = threading.Event()
        SENTINEL      = object()

        def _producer():
            try:
                for ci in pending_chunks:
                    if CANCEL.is_set() or stop_pipeline.is_set():
                        break
                    self._await_staging_capacity(planned[ci], stop_pipeline)
                    if CANCEL.is_set() or stop_pipeline.is_set():
                        break
                    desc = self._stage_chunk(session_id, ci, chunk_files_map[ci])
                    if desc is None:
                        if not CANCEL.is_set():
                            self._producer_err = f"chunk {ci + 1} could not be staged"
                        break
                    # Enqueue, staying responsive to pipeline shutdown.
                    queued = False
                    while not (CANCEL.is_set() or stop_pipeline.is_set()):
                        try:
                            ready_q.put(desc, timeout=1)
                            queued = True
                            break
                        except queue.Full:
                            continue
                    if not queued:
                        self._discard_desc(desc)
                        break
            except Exception as e:
                self._producer_err = str(e)
            finally:
                ready_q.put(SENTINEL)

        prod = threading.Thread(target=_producer, name='prefetch-producer',
                                daemon=True)
        prod.start()

        hb_stop = threading.Event()
        self._start_pipeline_heartbeat(hb_stop, ready_q, total_chunks)

        completed = 0
        failed    = False
        try:
            while True:
                desc = ready_q.get()
                if desc is SENTINEL:
                    break
                if CANCEL.is_set():
                    self._discard_desc(desc)
                    break
                ci          = desc['chunk_index']
                eject_after = (ci == last_chunk)
                if not self._write_chunk(session_id, desc, tape_label, eject_after):
                    failed = True
                    break
                completed += 1
                if (self.eject_after_pack is not None and
                        ci == self.eject_after_pack and
                        ci != last_chunk):
                    stop_pipeline.set()
                    _status('REMOTE',
                            f"Checkpoint reached after pack {ci:03d}; "
                            "ejecting tape and saving session.")
                    LTOBackup(
                        self.db,
                        self.cfg.ibm_eject_cmd,
                        log_dir=self.cfg.backup_log_dir,
                    ).eject_tape(self.cfg.lto_drive)
                    print("\n[REMOTE] Checkpoint complete. Session saved - "
                          "re-run option 6 to resume from the next pack.")
                    return
        finally:
            stop_pipeline.set()
            hb_stop.set()
            # Drain the queue so a producer blocked on a full put() can exit,
            # and clean up any prefetched-but-unused chunks.
            try:
                while True:
                    leftover = ready_q.get_nowait()
                    if leftover is not SENTINEL:
                        self._discard_desc(leftover)
            except queue.Empty:
                pass
            prod.join(timeout=15)
            if self.fetch_cores:
                unpin_current_process()

        if CANCEL.is_set():
            print("\n[ABORTED] Stopped by user. Session saved — "
                  "re-run option 6 to resume from the interrupted chunk.")
            return
        if failed or self._producer_err:
            msg = self._producer_err or "a chunk failed during tape write"
            print(f"\n[REMOTE] Pipeline stopped: {msg}. "
                  f"Re-run to resume from the failed chunk.")
            return
        if completed == len(pending_chunks):
            self.db.update_remote_session(
                session_id, status='completed',
                completed_at=datetime.now().isoformat()
            )
            print("\n[REMOTE] Session complete. All chunks archived to tape.")

    # ------------------------------------------------------------------
    # Producer: fetch + pack a chunk onto staging  (runs off the main thread)
    # ------------------------------------------------------------------

    def _await_staging_capacity(self, planned_bytes, stop_evt):
        """Block until there is room to stage another chunk without breaching the
        staging cap or starving the disk. Accounts for the ~2x transient
        footprint while a chunk is packed (fetch_dir + pack_dir coexist)."""
        need  = 2 * planned_bytes          # peak while fetch + pack dirs coexist
        floor = 20 * 1024**3               # keep >=20 GB free on the staging volume
        warned = False
        while not (CANCEL.is_set() or stop_evt.is_set()):
            with self._staged_lock:
                resident = self._staged_bytes
            try:
                free = shutil.disk_usage(self.staging_dir).free
            except OSError:
                free = need + floor
            room_cap  = (resident + need) <= self.staging_max_bytes
            room_disk = (free - need) >= floor
            alone     = (resident == 0)    # nothing else resident: must proceed
            if (room_cap and room_disk) or alone:
                return
            if not warned:
                _status('PIPELINE',
                        f"Backpressure — {resident / 1024**3:.0f} GB staged, "
                        f"waiting for the tape to drain before fetching the next "
                        f"chunk (cap {self.staging_max_bytes / 1024**3:.0f} GB).")
                warned = True
            time.sleep(2)

    def _stage_chunk(self, session_id, chunk_index, chunk_files):
        """Fetch then pack one chunk. Returns a ready-descriptor or None."""
        self._producer_chunk = chunk_index
        fetch_dir = os.path.join(self.staging_dir, f"_fetch_{chunk_index:03d}")
        pack_dir  = os.path.join(self.staging_dir, f"_pack_{chunk_index:03d}")

        # --- FETCH (remote -> PC) ---
        self.db.update_chunk_status(session_id, chunk_index, 'fetching')
        if not self._fetch_chunk(session_id, chunk_index, chunk_files, fetch_dir):
            if not CANCEL.is_set():
                self.db.update_chunk_status(session_id, chunk_index, 'fetch_failed')
            self._cleanup_dir(fetch_dir)
            return None
        if CANCEL.is_set():
            self._cleanup_dir(fetch_dir)
            return None

        # --- PACK (small files -> ZIP, large files staged loose) ---
        self.db.update_chunk_status(session_id, chunk_index, 'packing')
        # Hand the packer a clean dest so it never hits its interactive prompt
        # from this worker thread.
        self._cleanup_dir(pack_dir)
        _phase('PACK', f"Packing chunk {chunk_index + 1}: "
                       f"small files -> ZIP, large files staged loose")
        try:
            metadata = LTOPacker(self.cfg.max_zip_size_gb).run(
                source=fetch_dir,
                dest=pack_dir,
                threshold_mb=self.cfg.zip_threshold_mb,
            )
        except Exception as e:
            print(f"[REMOTE] Packer error: {e}")
            if not CANCEL.is_set():
                self.db.update_chunk_status(session_id, chunk_index, 'fetch_failed')
            self._cleanup_dir(fetch_dir)
            self._cleanup_dir(pack_dir)
            return None

        if not metadata:
            if not CANCEL.is_set():
                print(f"[REMOTE] Chunk {chunk_index + 1}: nothing to pack "
                      f"(empty fetch). Marking failed.")
                self.db.update_chunk_status(session_id, chunk_index, 'fetch_failed')
            self._cleanup_dir(fetch_dir)
            self._cleanup_dir(pack_dir)
            return None

        # Free the raw fetched copy now that packing is done — this halves the
        # per-chunk staging footprint so the prefetch buffer stays under the cap.
        self._cleanup_dir(fetch_dir)

        staged_bytes = _dir_tree_size(pack_dir)
        with self._staged_lock:
            self._staged_bytes += staged_bytes
        _status('PIPELINE', f"Chunk {chunk_index + 1} staged & ready "
                            f"({staged_bytes / 1024**3:.1f} GB) — queued for tape.")
        return {
            'chunk_index':  chunk_index,
            'fetch_dir':    fetch_dir,
            'pack_dir':     pack_dir,
            'metadata':     metadata,
            'staged_bytes': staged_bytes,
        }

    def _discard_desc(self, desc):
        """Drop a staged-but-unused chunk: clean its dirs and free its budget."""
        self._cleanup_dir(desc['fetch_dir'])
        self._cleanup_dir(desc['pack_dir'])
        with self._staged_lock:
            self._staged_bytes = max(0, self._staged_bytes - desc['staged_bytes'])

    # ------------------------------------------------------------------
    # Consumer: write a staged chunk to tape  (runs on the main thread)
    # ------------------------------------------------------------------

    def _write_chunk(self, session_id, desc, tape_label, eject_after):
        chunk_index = desc['chunk_index']
        self._consumer_chunk = chunk_index
        pack_dir = desc['pack_dir']

        self.db.update_chunk_status(session_id, chunk_index, 'backing')
        # _NoEjectBackup keeps the tape mounted; eject only after the final chunk.
        backup_cls = LTOBackup if eject_after else _NoEjectBackup
        try:
            backup_cls(self.db, self.cfg.ibm_eject_cmd,
                       tape_priority=self.tape_priority,
                       tape_affinity=self.tape_cores,
                       log_dir=self.cfg.backup_log_dir).run(
                source=pack_dir,
                tape_drive=self.cfg.lto_drive,
                tape_label=tape_label,
                packer_metadata=desc['metadata'],
            )
        except Exception as e:
            if CANCEL.is_set():
                # Robocopy was terminated by the stop request; leave the chunk
                # non-'done' (resumable) and skip eject.
                return False
            print(f"[REMOTE] Backup error: {e}")
            self.db.update_chunk_status(session_id, chunk_index, 'backup_failed')
            return False

        if CANCEL.is_set():
            return False

        self.db.update_chunk_status(session_id, chunk_index, 'done')

        # --- FLUSH staged files for this chunk ---
        _status('REMOTE', f"Flushing staged files for chunk {chunk_index + 1}...")
        self._cleanup_dir(desc['fetch_dir'])   # already removed after packing
        self._cleanup_dir(pack_dir)
        with self._staged_lock:
            self._staged_bytes = max(0, self._staged_bytes - desc['staged_bytes'])
        return True

    # ------------------------------------------------------------------
    # Pipeline status heartbeat
    # ------------------------------------------------------------------

    def _start_pipeline_heartbeat(self, stop_evt, ready_q, total_chunks):
        """Print a periodic line showing the producer staying ahead of the tape."""
        def _beat():
            last_msg = None
            last_print = 0
            quiet_interval = 30
            while not stop_evt.wait(5):
                with self._staged_lock:
                    staged_gb = self._staged_bytes / 1024**3
                prod_c = ('-' if self._producer_chunk is None
                          else self._producer_chunk + 1)
                cons_c = ('-' if self._consumer_chunk is None
                          else self._consumer_chunk + 1)
                msg = (
                    f"queued={ready_q.qsize()}/{self.prefetch_ahead} | "
                    f"staging={staged_gb:.0f}/"
                    f"{self.staging_max_bytes / 1024**3:.0f} GB | "
                    f"producer chunk {prod_c}/{total_chunks} | "
                    f"tape chunk {cons_c}/{total_chunks}"
                )
                now = time.time()
                if msg != last_msg or (now - last_print) >= quiet_interval:
                    _status('PIPELINE', msg)
                    last_msg = msg
                    last_print = now
        threading.Thread(target=_beat, name='pipeline-heartbeat',
                         daemon=True).start()

    # ------------------------------------------------------------------
    # Fetch helpers
    # ------------------------------------------------------------------

    def _fetch_chunk(self, session_id, chunk_index, chunk_files, fetch_dir):
        os.makedirs(fetch_dir, exist_ok=True)
        total_chunks = self.db.count_chunks(session_id)
        records = []
        pending = []        # primary files: extracted at their sanitized path
        collisions = []     # renamed files: fetched individually, then moved
        claimed = {}        # case-folded local_rel -> remote rel that owns it

        for row in chunk_files:
            remote_fpath = row['remote_path']
            fsize        = row['file_size_bytes']
            manifest_id  = row['manifest_id']

            try:
                remote_base, rel = _remote_fetch_base_and_rel(
                    self.remote_path, remote_fpath
                )
            except ValueError as e:
                self.db.update_manifest_row(
                    manifest_id,
                    status='fetch_failed',
                    error_msg=str(e),
                )
                print(f"[REMOTE] Invalid remote path: {e}")
                return False

            # rel is the true remote path (sent verbatim to remote tar); the
            # local copy lands under the name the Windows extractor can write.
            local_rel = _winsafe_extracted_rel(rel)
            key = local_rel.casefold()
            collided = key in claimed
            if collided:
                # Two distinct remote names map to the same on-disk path —
                # rename this one so neither file is silently overwritten.
                clash_with = claimed[key]
                local_rel  = _disambiguate_local_rel(local_rel, claimed)
                key        = local_rel.casefold()
                print(f"[REMOTE] Name collision: '{rel}' and '{clash_with}' map "
                      f"to the same Windows path — fetching the former as "
                      f"'{local_rel}'.")
            claimed[key] = rel

            local_path = os.path.join(fetch_dir, local_rel.replace('/', os.sep))
            records.append((row, remote_base, rel, local_rel, local_path))

            # Skip if already fetched with matching size (resume support)
            if os.path.exists(local_path):
                try:
                    if os.path.getsize(local_path) == fsize:
                        print(f"[REMOTE] Skip (already fetched): {rel}")
                        self.db.update_manifest_row(manifest_id, status='fetched',
                                                    local_rel_path=local_rel,
                                                    error_msg=None)
                        continue
                    os.remove(local_path)  # partial file from interrupted run
                except OSError:
                    pass

            self.db.update_manifest_row(manifest_id, status='fetching')
            (collisions if collided else pending).append(
                (row, remote_base, rel, local_rel, local_path))

        if pending or collisions:
            todo_bytes = sum(row['file_size_bytes']
                             for row, *_ in pending + collisions)
            todo_count = len(pending) + len(collisions)
            _phase('FETCH', f"Remote -> PC | chunk {chunk_index + 1}/{total_chunks} | "
                            f"{todo_count} file(s), {todo_bytes / 1024**3:.2f} GB")
            _status('SSH', f"Opening tar stream to "
                           f"{self.remote_user}@{self.remote_host} "
                           f"(cipher={self.ssh_cipher or 'default'}, "
                           f"mbuffer={'on' if self.use_mbuffer else 'off'})")

            fetch_stop = threading.Event()
            self._start_fetch_monitor(fetch_stop, fetch_dir, todo_bytes)

            pending_by_base = defaultdict(list)
            for row, remote_base, rel, local_rel, local_path in pending:
                pending_by_base[remote_base].append((row, rel, local_path))

            try:
                for remote_base, base_pending in pending_by_base.items():
                    if CANCEL.is_set():
                        return False
                    ok, err = _remote_tar_fetch(
                        self.remote_user,
                        self.remote_host,
                        remote_base,
                        [rel for _, rel, _ in base_pending],
                        fetch_dir,
                        password=self.remote_password,
                        cipher=self.ssh_cipher,
                        use_mbuffer=self.use_mbuffer,
                        mbuffer_size=self.mbuffer_size,
                        fetch_cores=self.fetch_cores,
                    )
                    if not ok:
                        if CANCEL.is_set():
                            return False
                        print(f"\n[REMOTE] Tar fetch failed:\n{err}")
                        for row, rel, _ in base_pending:
                            self.db.update_manifest_row(
                                row['manifest_id'],
                                status='fetch_failed',
                                error_msg=err[:500],
                            )
                        return False

                # Renamed files can't ride the shared stream (bsdtar would
                # extract them onto the primary's path), so fetch each alone
                # into an isolated dir and move it to its disambiguated name.
                if collisions and not self._fetch_collisions(
                        collisions, fetch_dir):
                    return False
            finally:
                fetch_stop.set()
                _progress_done()
        else:
            print(f"[REMOTE] Chunk {chunk_index + 1}/{total_chunks}: "
                  "all files already fetched.")

        for row, _, rel, local_rel, local_path in records:
            fsize       = row['file_size_bytes']
            manifest_id = row['manifest_id']
            if not os.path.exists(local_path):
                print(f"[REMOTE] Missing after fetch: {rel}")
                self.db.update_manifest_row(
                    manifest_id,
                    status='fetch_failed',
                    error_msg="missing after tar fetch",
                )
                return False

            try:
                actual = os.path.getsize(local_path)
            except OSError as e:
                self.db.update_manifest_row(
                    manifest_id,
                    status='fetch_failed',
                    error_msg=f"stat failed: {e}",
                )
                return False

            if actual != fsize:
                print(f"[REMOTE] Size mismatch for {rel}: "
                      f"expected {fsize} B, got {actual} B")
                try:
                    os.remove(local_path)
                except OSError:
                    pass
                self.db.update_manifest_row(
                    manifest_id,
                    status='fetch_failed',
                    error_msg=f"size mismatch: expected {fsize}, got {actual}",
                )
                return False

            self.db.update_manifest_row(manifest_id, status='fetched',
                                        local_rel_path=local_rel,
                                        error_msg=None)
        return True

    def _fetch_collisions(self, collisions, fetch_dir):
        """Fetch files whose sanitized name clashed with another file's.

        Each is streamed alone into a private temp dir (where bsdtar writes it
        at its natural sanitized path) and then moved to the disambiguated
        local_path. Returns False on the first failure, leaving the row marked
        fetch_failed for the caller to surface."""
        collide_root = os.path.join(fetch_dir, '_collide')
        try:
            for row, remote_base, rel, _local_rel, local_path in collisions:
                if CANCEL.is_set():
                    return False
                tmp = os.path.join(collide_root, str(row['manifest_id']))
                shutil.rmtree(tmp, ignore_errors=True)
                os.makedirs(tmp, exist_ok=True)

                ok, err = _remote_tar_fetch(
                    self.remote_user,
                    self.remote_host,
                    remote_base,
                    [rel],
                    tmp,
                    password=self.remote_password,
                    cipher=self.ssh_cipher,
                    use_mbuffer=self.use_mbuffer,
                    mbuffer_size=self.mbuffer_size,
                    fetch_cores=self.fetch_cores,
                )
                if not ok:
                    if CANCEL.is_set():
                        return False
                    print(f"\n[REMOTE] Tar fetch failed (renamed file):\n{err}")
                    self.db.update_manifest_row(
                        row['manifest_id'], status='fetch_failed',
                        error_msg=err[:500])
                    return False

                # Alone in tmp, the file lands at its natural sanitized path.
                natural = os.path.join(
                    tmp, _winsafe_extracted_rel(rel).replace('/', os.sep))
                if not os.path.exists(natural):
                    print(f"[REMOTE] Missing after fetch (renamed file): {rel}")
                    self.db.update_manifest_row(
                        row['manifest_id'], status='fetch_failed',
                        error_msg="missing after tar fetch")
                    return False

                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                try:
                    os.replace(natural, local_path)
                except OSError as e:
                    print(f"[REMOTE] Could not place renamed file {rel}: {e}")
                    self.db.update_manifest_row(
                        row['manifest_id'], status='fetch_failed',
                        error_msg=f"move failed: {e}")
                    return False
            return True
        finally:
            shutil.rmtree(collide_root, ignore_errors=True)

    def _start_fetch_monitor(self, stop_evt, fetch_dir, total_bytes):
        """Live remote->PC throughput: watch the fetch dir grow on disk."""
        def _mon():
            prev_bytes = 0
            prev_time  = time.time()
            while not stop_evt.wait(2):
                cur   = _dir_tree_size(fetch_dir)
                now   = time.time()
                dt    = now - prev_time
                speed = ((cur - prev_bytes) / 1024**2) / dt if dt > 0 else 0
                pct   = (cur / total_bytes * 100) if total_bytes else 0
                remaining = max(0, total_bytes - cur)
                eta = remaining / (speed * 1024**2) if speed > 0 else None
                _progress_line(
                    f"[FETCH] {min(pct, 100):.1f}% | {speed:.1f} MB/s | "
                    f"{cur / 1024**3:.1f}/{total_bytes / 1024**3:.1f} GB | "
                    f"ETA {_fmt_eta(eta)}"
                )
                prev_bytes = cur
                prev_time  = now
        threading.Thread(target=_mon, name='fetch-monitor', daemon=True).start()

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    def _cleanup_remote_staging_dirs(self):
        """Remove remote-session temp folders before a truly fresh run."""
        staging_root = os.path.abspath(self.staging_dir)
        try:
            names = os.listdir(staging_root)
        except OSError as e:
            print(f"[REMOTE] Warning - could not inspect staging directory: {e}")
            return

        for name in names:
            if not (name.startswith("_fetch_") or name.startswith("_pack_")):
                continue
            path = os.path.abspath(os.path.join(staging_root, name))
            if path == staging_root or not path.startswith(staging_root + os.sep):
                print(f"[REMOTE] Warning - refusing to clean suspicious path: {path}")
                continue
            self._cleanup_dir(path)

    def _cleanup_dir(self, path):
        if os.path.exists(path):
            try:
                shutil.rmtree(path)
                print(f"[REMOTE] Cleaned: {path}")
            except OSError as e:
                print(f"[REMOTE] Warning — could not clean {path}: {e}")


# ==============================================================================
# MODULE D: TAPE MANAGER - Formatting & registration
# ==============================================================================

class TapeManager:
    def __init__(self, db: DatabaseManager, tape_drive: str, ibm_eject_cmd=None):
        self.db            = db
        self.tape_drive    = tape_drive
        self.ibm_eject_cmd = ibm_eject_cmd

    def _drive_letter(self):
        return _drive_letter(self.tape_drive)

    def _ltfs_drive_status(self):
        """Return the current IBM LTFS status for this drive, if available."""
        return _ltfs_drive_status(self.tape_drive)

    def _print_drive_status(self, prefix="[INFO]"):
        status, output, error = self._ltfs_drive_status()
        if error:
            print(f"{prefix} {error}")
            return status
        if status:
            print(f"{prefix} IBM LTFS drive status for {self._drive_letter()}: {status}")
        elif output:
            print(f"{prefix} Could not identify drive {self._drive_letter()} in LtfsCmdDrives.exe output:")
            print(output)
        return status

    def _print_invalid_medium_hint(self, operation, output):
        if "LTFS60233E" not in (output or ""):
            return
        status = self._print_drive_status("[HINT]")
        print(f"[HINT] IBM LTFS says the medium is not valid for {operation}.")
        if status == "NO_LTFS_MEDIA":
            print("[HINT] The drive currently reports NO_LTFS_MEDIA.")
            print("       Check that a writable data cartridge is fully loaded, not a cleaning/WORM cartridge,")
            print("       then wait for the drive to become ready and try again.")
        elif status:
            print(f"[HINT] Current medium status is {status}.")
            print("       Eject/reload the tape, confirm the cartridge is writable, and close any app using the drive.")
        else:
            print("[HINT] Run Tape Maintenance -> Tape drives info, then confirm the cartridge is loaded and writable.")

    def list_drives(self):
        if os.name == 'nt':
            try:
                result = subprocess.run(
                    ['wmic', 'logicaldisk', 'get', 'DeviceID,Description,VolumeName'],
                    capture_output=True, text=True
                )
                print("\n[DRIVES]\n" + result.stdout)
            except Exception as e:
                print(f"[ERROR] {e}")
        else:
            print("[INFO] Drive listing is only supported on Windows.")

    def format_tape(self):
        print("\n[TAPE MANAGER] Format / Initialize Tape")
        print(f"Target drive: {self.tape_drive}")
        self._print_drive_status()
        print("=" * 60)
        print("WARNING: This will ERASE ALL DATA on the current tape.")
        print('Type  y  to confirm (or press Enter to cancel):')
        if input(">> ").strip().lower() != "y":
            print("[ABORTED] Format cancelled.")
            return

        old_label = get_volume_label(self.tape_drive)
        if old_label:
            print(f"[INFO] Current tape label detected: {old_label}")

        label = input("New Volume Label (e.g. Scalpelab_Tape_X): ").strip()
        if not label:
            print("[ABORTED] No label provided.")
            return

        drive_letter = self._drive_letter()
        exe          = os.path.join(LTFS_DIR, 'LtfsCmdFormat.exe')
        cmd          = [exe, drive_letter, f'/N:{label}']

        print(f"\n[FORMAT] Running: cd /d \"{LTFS_DIR}\" && LtfsCmdFormat.exe {drive_letter} /N:{label}")
        print("[FORMAT] This may take several minutes...")

        _acquire_tape_io_lock(f"format {self.tape_drive}")
        try:
            result = subprocess.run(cmd, check=True, text=True, capture_output=True,
                                    cwd=LTFS_DIR)
            print("[FORMAT] Complete.")
            if result.stdout:
                print(result.stdout)

            labels_to_delete = []
            for existing_label in (old_label, label):
                if existing_label and existing_label not in labels_to_delete:
                    labels_to_delete.append(existing_label)
            for existing_label in labels_to_delete:
                if self.db.tape_exists(existing_label):
                    self.db.delete_tape(existing_label)

            cap      = input("Tape capacity in GB (default 12288 for 12 TB, Enter to skip): ").strip()
            capacity = int(cap) if cap.isdigit() else 12288
            self.db.register_tape(label, capacity)
        except subprocess.CalledProcessError as e:
            output = ((e.stdout or '') + (e.stderr or '')).strip()
            print(f"[ERROR] LtfsCmdFormat.exe failed:\n{output}")
            self._print_invalid_medium_hint("Format", output)
        except FileNotFoundError:
            print(f"[ERROR] LtfsCmdFormat.exe not found in: {LTFS_DIR}")
        finally:
            _release_tape_io_lock()

    def register_tape(self):
        label = input("Volume label of tape to register: ").strip()
        if not label:
            return
        cap      = input("Capacity in GB (default 12288 for 12 TB, Enter to skip): ").strip()
        capacity = int(cap) if cap.isdigit() else 12288
        self.db.register_tape(label, capacity)

    def check_tape(self):
        """Run LtfsCmdCheck.exe to check and repair the tape filesystem."""
        drive_letter = self._drive_letter()
        exe          = os.path.join(LTFS_DIR, 'LtfsCmdCheck.exe')
        cmd          = [exe, drive_letter]
        print(f"\n[CHECK] Running: LtfsCmdCheck.exe {drive_letter}")
        self._print_drive_status("[CHECK]")
        print("[CHECK] This may take several minutes...")
        _acquire_tape_io_lock(f"check {self.tape_drive}")
        try:
            result = subprocess.run(cmd, text=True, capture_output=True, cwd=LTFS_DIR)
            output = (result.stdout or '') + (result.stderr or '')
            if output.strip():
                print(output.strip())
            if result.returncode == 0:
                print("[CHECK] Complete — no errors found.")
            else:
                print(f"[CHECK] Finished with code {result.returncode}.")
                self._print_invalid_medium_hint("Check", output)
        except FileNotFoundError:
            print(f"[ERROR] LtfsCmdCheck.exe not found in: {LTFS_DIR}")
        finally:
            _release_tape_io_lock()

    def tape_info(self):
        """Run LtfsCmdDrives.exe to display connected tape drives and status."""
        exe = os.path.join(LTFS_DIR, 'LtfsCmdDrives.exe')
        print(f"\n[INFO] Running: LtfsCmdDrives.exe")
        try:
            result = subprocess.run([exe], text=True, capture_output=True, cwd=LTFS_DIR)
            output = (result.stdout or '') + (result.stderr or '')
            if output.strip():
                print(output.strip())
            if result.returncode != 0:
                print(f"[INFO] Finished with code {result.returncode}.")
        except FileNotFoundError:
            print(f"[ERROR] LtfsCmdDrives.exe not found in: {LTFS_DIR}")

    def _legacy_eject_tape_unlocked(self):
        """Run LtfsCmdEject.exe to safely eject the tape."""
        drive_arg = self.tape_drive.rstrip(":\\")
        exe       = self.ibm_eject_cmd or os.path.join(LTFS_DIR, 'LtfsCmdEject.exe')
        exe_dir   = os.path.dirname(exe) or LTFS_DIR
        cmd       = [exe, drive_arg]
        print("\n" + "#" * 60)
        print("[LTO] Ejecting tape...")
        print("[LTO] PLEASE WAIT — this can take 1-2 minutes.")
        print("#" * 60)
        try:
            result = subprocess.run(cmd, check=True, text=True, capture_output=True,
                                    cwd=exe_dir)
            print("[LTO] Tape ejected successfully!")
            if result.stdout:
                print(result.stdout)
        except subprocess.CalledProcessError as e:
            print(f"[ERROR] Eject failed: {e.stderr}")
            print(f"Try manually: cd /d \"{LTFS_DIR}\" && LtfsCmdEject.exe {drive_arg}")
        except FileNotFoundError:
            print(f"[ERROR] LtfsCmdEject.exe not found in: {LTFS_DIR}")


    def eject_tape(self):
        """Run LtfsCmdEject.exe to safely eject the tape."""
        drive_arg = self.tape_drive.rstrip(":\\")
        exe       = self.ibm_eject_cmd or os.path.join(LTFS_DIR, 'LtfsCmdEject.exe')
        exe_dir   = os.path.dirname(exe) or LTFS_DIR
        cmd       = [exe, drive_arg]
        print("\n" + "#" * 60)
        print("[LTO] Ejecting tape...")
        print("[LTO] PLEASE WAIT - this can take 1-2 minutes.")
        print("#" * 60)
        _acquire_tape_io_lock(f"eject {self.tape_drive}")
        try:
            result = subprocess.run(cmd, check=True, text=True, capture_output=True,
                                    cwd=exe_dir)
            print("[LTO] Tape ejected successfully!")
            if result.stdout:
                print(result.stdout)
        except subprocess.CalledProcessError as e:
            print(f"[ERROR] Eject failed: {e.stderr}")
            print(f"Try manually: cd /d \"{LTFS_DIR}\" && LtfsCmdEject.exe {drive_arg}")
        except FileNotFoundError:
            print(f"[ERROR] LtfsCmdEject.exe not found in: {LTFS_DIR}")
        finally:
            _release_tape_io_lock()


# ==============================================================================
# WINDOWS DEFENDER PROCESS-EXCLUSION HELPERS
# ==============================================================================
#
# Rationale: a process-based exclusion on `robocopy.exe` lets the copy stream
# bypass real-time AV scanning entirely without exposing any filesystem path
# from scans. If the user has already excluded robocopy.exe globally, we leave
# their settings alone. Otherwise we add it temporarily and remove it on the
# way out via a try/finally at the call site.
#
# All Defender mutations require Administrator. If the script is not elevated,
# every PowerShell call here will fail — we catch those failures and let the
# workflow continue, since the backup itself still works (just slower and
# with shoe-shining risk).

ROBOCOPY_PROCESS_NAME = 'robocopy.exe'


def _is_admin():
    """True if the current process is running with Administrator privileges.

    Defender's exclusion list is only *readable* from an elevated context —
    Get-MpPreference succeeds for non-elevated callers but returns empty
    exclusion arrays, so we can't trust a negative result from there.
    Checking elevation up-front is the only reliable way to decide whether
    to touch Defender at all.
    """
    try:
        import ctypes
        return bool(ctypes.windll.shell32.IsUserAnAdmin())
    except Exception:
        return False


def _run_powershell(ps_command):
    """Invoke a PowerShell command. Returns CompletedProcess.
    Raises FileNotFoundError if PowerShell is missing,
    subprocess.CalledProcessError on non-zero exit."""
    return subprocess.run(
        ['powershell', '-NonInteractive', '-NoProfile', '-Command', ps_command],
        capture_output=True, text=True, check=True,
    )


def _robocopy_already_excluded():
    """Return True if robocopy.exe is already in Defender's ExclusionProcess list.

    Returns False if it isn't excluded, or if the lookup itself failed
    (insufficient privilege, Defender unavailable, etc.) — the caller will then
    attempt to add the exclusion and surface any error from that attempt.
    """
    ps = (
        "$p = (Get-MpPreference).ExclusionProcess; "
        "if ($p -and ($p -contains 'robocopy.exe')) { 'YES' } else { 'NO' }"
    )
    try:
        result = _run_powershell(ps)
    except FileNotFoundError:
        print("[DEFENDER] PowerShell not found — cannot check existing exclusions.")
        return False
    except subprocess.CalledProcessError as e:
        print("[DEFENDER] WARNING: could not query Defender exclusions "
              f"(exit {e.returncode}). Real-time scanning may still be active, "
              "which can trigger LTO shoe-shining due to hardware buffer drops. "
              "Continuing without process exclusion.")
        return False
    return (result.stdout or '').strip().upper() == 'YES'


def _add_robocopy_exclusion():
    """Attempt to add robocopy.exe as a Defender process exclusion.

    Returns True if the exclusion was added by us (and should be removed
    later). Returns False on any failure — typically lack of Administrator
    privileges — after printing a warning.
    """
    try:
        _run_powershell("Add-MpPreference -ExclusionProcess 'robocopy.exe'")
    except FileNotFoundError:
        print("[DEFENDER] PowerShell not found — skipping process exclusion.")
        return False
    except subprocess.CalledProcessError as e:
        stderr = (e.stderr or '').strip()
        print("!" * 60)
        print("[DEFENDER] WARNING: failed to add robocopy.exe process exclusion "
              f"(exit {e.returncode}).")
        if stderr:
            print(f"[DEFENDER] stderr: {stderr}")
        print("[DEFENDER] Without Administrator rights, Defender real-time "
              "scanning remains ACTIVE. This can intercept robocopy I/O and "
              "trigger LTO shoe-shining due to hardware buffer drops.")
        print("[DEFENDER] Continuing anyway — backup will proceed at reduced speed.")
        print("!" * 60)
        return False
    print("[DEFENDER] Added temporary process exclusion: robocopy.exe")
    return True


def _remove_robocopy_exclusion():
    """Remove the robocopy.exe process exclusion. Errors are logged, not raised."""
    try:
        _run_powershell("Remove-MpPreference -ExclusionProcess 'robocopy.exe'")
    except FileNotFoundError:
        return
    except subprocess.CalledProcessError as e:
        stderr = (e.stderr or '').strip()
        print(f"[DEFENDER] WARNING: failed to remove robocopy.exe exclusion "
              f"(exit {e.returncode}). You may want to remove it manually.")
        if stderr:
            print(f"[DEFENDER] stderr: {stderr}")
        return
    print("[DEFENDER] Removed temporary process exclusion: robocopy.exe")


def _prepare_robocopy_exclusion():
    """Ensure robocopy.exe is excluded from Defender for this run.

    Returns True if WE added the exclusion (caller must remove it in finally).
    Returns False if it was already excluded, if we can't read/modify Defender
    state without admin, or if the add itself failed.
    """
    if not _is_admin():
        print("[DEFENDER] Running without Administrator privileges. Cannot "
              "verify or modify Windows Defender exclusions. If 'robocopy.exe' "
              "is already globally excluded on this system, the backup will "
              "still run at full speed. Otherwise, real-time scanning may "
              "trigger LTO shoe-shining due to hardware buffer drops.")
        return False

    if _robocopy_already_excluded():
        print("[DEFENDER] robocopy.exe is already excluded. "
              "Proceeding to backup at max speed.")
        return False
    return _add_robocopy_exclusion()


# ==============================================================================
# ARCHIVER WORKFLOW (ties together Analyzer, Packer, Backup)
# ==============================================================================

def run_archiver(cfg: ConfigManager, db: DatabaseManager):
    added_exclusion = _prepare_robocopy_exclusion()
    try:
        LocalOrchestrator(cfg, db).run()
    finally:
        if added_exclusion:
            _remove_robocopy_exclusion()


def run_remote_archiver(cfg, db):
    """Menu option 6: pull files from a remote host and archive to LTO tape."""
    if not cfg.remote_host or not cfg.remote_user or not cfg.remote_path:
        print("\n[REMOTE] The [REMOTE] section in config.ini is incomplete.")
        print("  Required: remote_host, remote_user, remote_path")
        print("  Optional: remote_password, staging_fill_pct  (default 0.80)")
        cfg_abs = os.path.abspath(CONFIG_FILE)
        print(f"\n[INFO] Config path: {cfg_abs}")
        if os.name == 'nt':
            os.startfile(cfg_abs)
        return

    added_exclusion = _prepare_robocopy_exclusion()
    reset_cancel()
    install_cancel_handler()
    print("[REMOTE] Press Ctrl+C at any time to stop safely "
          "(the session is saved and can be resumed).")
    try:
        RemoteOrchestrator(cfg, db).run()
    except RuntimeError as e:
        print(str(e))
    except KeyboardInterrupt:
        print("\n[REMOTE] Interrupted. Session state saved — re-run to resume.")
    finally:
        # Make sure no fetch/tape child survives, restore CPU affinity and the
        # default Ctrl+C behaviour, then drop the robocopy Defender exclusion.
        _terminate_all_procs()
        unpin_current_process()
        uninstall_cancel_handler()
        reset_cancel()
        if added_exclusion:
            _remove_robocopy_exclusion()


# ==============================================================================
# DATABASE MANAGEMENT SUBMENU
# ==============================================================================

def _print_tapes_table(db):
    tapes = db.list_tapes()
    if not tapes:
        print("[DB] No tapes registered.")
        return tapes
    BAR_W = 20
    print(f"\n{'ID':>4}  {'Volume Label':<25}  {'Initialized':<19}  Space")
    print("-" * 80)
    for t in tapes:
        date_s  = (t['date_formatted'] or '')[:19]
        cap_gb  = t['total_capacity']
        used_b  = t['used_space'] or 0
        used_gb = used_b / 1024**3
        if cap_gb:
            pct     = min(used_gb / cap_gb, 1.0)
            filled  = round(pct * BAR_W)
            bar     = '█' * filled + '░' * (BAR_W - filled)
            space_s = f"[{bar}] {pct*100:.1f}%  {used_gb:.1f}/{cap_gb:.0f} GiB"
        else:
            space_s = f"{used_gb:.1f} GiB used  (no capacity set)"
        print(f"{t['tape_id']:>4}  {t['volume_label']:<25}  {date_s:<19}  {space_s}")
    return tapes


def _db_management_menu(db):
    while True:
        print("\n--- Database Management ---")
        print("  1. Delete tape & all file records")
        print("  2. Delete single file record by ID")
        print("  3. Rename tape label")
        print("  4. Set tape capacity (GB)")
        print("  5. Recalculate tape used space")
        print("  6. Wipe file records for tape (keep tape entry)")
        print("  0. Back")
        print("-" * 40)
        sub = input("Choose: ").strip()

        if sub == '0':
            break

        elif sub == '1':
            tapes = _print_tapes_table(db)
            if not tapes:
                continue
            label = input("Enter volume label to DELETE (tape + all file records): ").strip()
            if not db.tape_exists(label):
                print(f"[ERROR] Tape '{label}' not found.")
                continue
            confirm = input(f"Type 'yes' to permanently delete tape '{label}' and ALL its file records: ").strip()
            if confirm.lower() == 'yes':
                db.delete_tape(label)
            else:
                print("[ABORTED]")

        elif sub == '2':
            file_id_s = input("Enter file ID to delete: ").strip()
            if not file_id_s.isdigit():
                print("[ERROR] Invalid file ID.")
                continue
            file_id = int(file_id_s)
            rec = db.get_file_by_id(file_id)
            if not rec:
                print(f"[ERROR] No file record with ID {file_id}.")
                continue
            print(f"\n  ID:        {rec['file_id']}")
            print(f"  Name:      {rec['file_name']}")
            print(f"  Path:      {rec['original_path']}")
            print(f"  Size:      {rec['file_size_bytes']:,} bytes")
            print(f"  Tape:      {rec['tape_label']}")
            print(f"  Backed up: {(rec['backup_date'] or '')[:19]}")
            confirm = input("Type 'yes' to delete this record: ").strip()
            if confirm.lower() == 'yes':
                db.delete_file(file_id)
                print(f"[DB] File record {file_id} deleted.")
            else:
                print("[ABORTED]")

        elif sub == '3':
            tapes = _print_tapes_table(db)
            if not tapes:
                continue
            old_label = input("Enter current volume label: ").strip()
            if not db.tape_exists(old_label):
                print(f"[ERROR] Tape '{old_label}' not found.")
                continue
            new_label = input("Enter new volume label: ").strip()
            if not new_label:
                print("[ERROR] New label cannot be empty.")
                continue
            if db.tape_exists(new_label):
                print(f"[ERROR] Label '{new_label}' already exists.")
                continue
            db.rename_tape(old_label, new_label)

        elif sub == '4':
            tapes = _print_tapes_table(db)
            if not tapes:
                continue
            label = input("Enter volume label: ").strip()
            if not db.tape_exists(label):
                print(f"[ERROR] Tape '{label}' not found.")
                continue
            cap_s = input("Enter capacity in GB (e.g. 12000): ").strip()
            try:
                cap_gb = float(cap_s)
            except ValueError:
                print("[ERROR] Invalid number.")
                continue
            db.update_tape_capacity(label, cap_gb)

        elif sub == '5':
            tapes = _print_tapes_table(db)
            if not tapes:
                continue
            label = input("Enter volume label: ").strip()
            if not db.tape_exists(label):
                print(f"[ERROR] Tape '{label}' not found.")
                continue
            tape_row = next((t for t in tapes if t['volume_label'] == label), None)
            old_used = (tape_row['used_space'] or 0) if tape_row else 0
            new_used = db.recalculate_tape_used_space(label)
            print(f"[DB] Used space updated: {old_used/1024**3:.2f} GB → {new_used/1024**3:.2f} GB")

        elif sub == '6':
            tapes = _print_tapes_table(db)
            if not tapes:
                continue
            label = input("Enter volume label to wipe file records for: ").strip()
            if not db.tape_exists(label):
                print(f"[ERROR] Tape '{label}' not found.")
                continue
            count = db.count_tape_file_records(label)
            confirm = input(f"Type 'yes' to delete {count} file record(s) for '{label}' (tape entry kept): ").strip()
            if confirm.lower() == 'yes':
                db.delete_files_for_tape(label)
            else:
                print("[ABORTED]")

        else:
            print("[ERROR] Invalid selection.")


# ==============================================================================
# MAIN MENU — persistent loop
# ==============================================================================

def main():
    os.system('cls' if os.name == 'nt' else 'clear')
    print("=" * 60)
    print("   LTO ARCHIVE MANAGEMENT SYSTEM")
    print("=" * 60)

    cfg       = ConfigManager()
    db        = DatabaseManager(cfg.db_path)
    tape_mgr  = TapeManager(db, cfg.lto_drive, cfg.ibm_eject_cmd)
    retriever = LTORetriever(db, cfg.lto_drive, cfg.staging_dir, cfg.restore_dir)

    while True:
        print("\n" + "=" * 60)
        print("  MAIN MENU")
        print("=" * 60)
        print("  1. Archive   — Backup files to LTO tape")
        print("  2. Retrieve  — Search database & restore files")
        print("  3. Tape Maintenance — Format / Register tapes")
        print("  4. List Registered Tapes")
        print("  5. Open config.ini")
        print("  6. Remote Archive — Fetch from remote host & backup to LTO")
        print("  7. Database Management — Edit / delete tape & file records")
        print("  0. Exit")
        print("-" * 60)

        choice = input("Choose: ").strip()

        if choice == '1':
            run_archiver(cfg, db)

        elif choice == '2':
            added_exclusion = _prepare_robocopy_exclusion()
            try:
                retriever.run()
            finally:
                if added_exclusion:
                    _remove_robocopy_exclusion()

        elif choice == '3':
            print("\n--- Tape Maintenance ---")
            print("1. Format tape        (LtfsCmdFormat.exe — ERASES ALL DATA)")
            print("2. Register tape manually")
            print("3. List available drives")
            print("4. Check tape         (LtfsCmdCheck.exe — repair filesystem errors)")
            print("5. Tape drives info   (LtfsCmdDrives.exe — list drives & status)")
            print("6. Eject tape         (LtfsCmdEject.exe — safely eject tape)")
            print("0. Back")
            sub = input("Choose: ").strip()
            if sub == '1':
                tape_mgr.format_tape()
            elif sub == '2':
                tape_mgr.register_tape()
            elif sub == '3':
                tape_mgr.list_drives()
            elif sub == '4':
                tape_mgr.check_tape()
            elif sub == '5':
                tape_mgr.tape_info()
            elif sub == '6':
                tape_mgr.eject_tape()

        elif choice == '4':
            tapes = db.list_tapes()
            if not tapes:
                print("[DB] No tapes registered yet.")
            else:
                BAR_W = 24
                print(f"\n{'ID':>4}  {'Volume Label':<25}  {'Initialized':<19}  {'Used / Capacity':<22}  Space")
                print("-" * 95)
                for t in tapes:
                    date_s  = (t['date_formatted'] or '')[:19]
                    cap_gb  = t['total_capacity']
                    used_b  = t['used_space'] or 0
                    used_gb = used_b / 1024**3

                    if cap_gb:
                        pct    = min(used_gb / cap_gb, 1.0)
                        filled = round(pct * BAR_W)
                        bar    = '█' * filled + '░' * (BAR_W - filled)
                        space_s = f"[{bar}] {pct*100:.1f}%  {used_gb:.1f}/{cap_gb:.0f} GiB"
                    else:
                        space_s = f"{used_gb:.1f} GiB used  (no capacity set)"

                    print(f"{t['tape_id']:>4}  {t['volume_label']:<25}  {date_s:<19}  {space_s}")

        elif choice == '5':
            cfg_abs = os.path.abspath(CONFIG_FILE)
            print(f"\n[INFO] Config path: {cfg_abs}")
            if os.name == 'nt':
                os.startfile(cfg_abs)

        elif choice == '6':
            run_remote_archiver(cfg, db)

        elif choice == '7':
            _db_management_menu(db)

        elif choice == '0':
            print("Goodbye.")
            db.close()
            break

        else:
            print("[ERROR] Invalid selection.")


if __name__ == "__main__":
    try:
        main()
    except RuntimeError as e:
        print(f"\n{e}")
    except KeyboardInterrupt:
        print("\n\n[ABORTED] User stopped the script.")
