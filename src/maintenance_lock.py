"""Shared stale-lock handling for database maintenance operations."""
import json
import os
import socket
import time
from contextlib import contextmanager


STALE_LOCK_SECONDS = 24 * 60 * 60


def _pid_exists(pid):
    try:
        pid = int(pid)
    except (TypeError, ValueError):
        return False
    if pid <= 0:
        return False
    try:
        import psutil
        return psutil.pid_exists(pid)
    except ImportError:
        pass
    if os.name == 'nt':
        try:
            import ctypes
            handle = ctypes.windll.kernel32.OpenProcess(0x1000, False, pid)
            if handle:
                ctypes.windll.kernel32.CloseHandle(handle)
                return True
            return False
        except Exception:
            return True
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def _read_lock(path):
    try:
        with open(path, encoding='utf-8') as handle:
            raw = handle.read().strip()
    except OSError:
        return None
    if not raw:
        return {'raw': raw}
    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            return data
    except json.JSONDecodeError:
        pass
    if raw.isdigit():
        return {'pid': int(raw), 'legacy': True}
    return {'raw': raw}


def clear_stale_maintenance_lock(lock_path, stale_seconds=STALE_LOCK_SECONDS):
    """Remove a maintenance lock only when it is provably stale."""
    if not os.path.exists(lock_path):
        return False
    data = _read_lock(lock_path) or {}
    pid = data.get('pid')
    if pid is not None:
        if _pid_exists(pid):
            return False
        try:
            os.remove(lock_path)
            return True
        except OSError:
            return False
    try:
        age = time.time() - os.path.getmtime(lock_path)
    except OSError:
        return False
    if age < stale_seconds:
        return False
    try:
        os.remove(lock_path)
        return True
    except OSError:
        return False


def ensure_no_active_maintenance_lock(lock_path):
    if clear_stale_maintenance_lock(lock_path):
        print(f"[DB] Cleared stale maintenance lock: {lock_path}")
    if os.path.exists(lock_path):
        raise RuntimeError(
            "[DB] Database optimization is in progress; archive and "
            "inspector access is temporarily disabled."
        )


@contextmanager
def maintenance_lock(lock_path, operation):
    ensure_no_active_maintenance_lock(lock_path)
    payload = {
        'pid': os.getpid(),
        'host': socket.gethostname(),
        'operation': operation,
        'created_at': time.strftime('%Y-%m-%dT%H:%M:%S'),
    }
    try:
        with open(lock_path, 'x', encoding='utf-8') as lock:
            json.dump(payload, lock, sort_keys=True)
    except FileExistsError:
        ensure_no_active_maintenance_lock(lock_path)
        raise RuntimeError(f"maintenance lock already exists: {lock_path}")
    try:
        yield
    finally:
        try:
            os.remove(lock_path)
        except OSError:
            pass
