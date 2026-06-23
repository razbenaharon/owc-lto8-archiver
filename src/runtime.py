"""Status output, cancellation, subprocess registry, CPU tuning."""
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


def _speed_str(num_bytes, seconds, with_rate=False):
    """Human throughput like '220.4 MiB/s'; with_rate also appends GiB/h."""
    try:
        seconds = float(seconds or 0)
    except (TypeError, ValueError):
        seconds = 0.0
    if seconds <= 0:
        return "n/a"
    mib_s = (float(num_bytes or 0) / 1024**2) / seconds
    if not with_rate:
        return f"{mib_s:.1f} MiB/s"
    gib_h = (float(num_bytes or 0) / 1024**3) / (seconds / 3600)
    return f"{mib_s:.1f} MiB/s | {gib_h:.1f} GiB/h"


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


CANCEL = threading.Event()        # set by the SIGINT handler; polled by workers


_PROCS = set()                    # live child Popen objects we may need to kill


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
