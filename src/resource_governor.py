"""Resource Governor for heavy local archive pipeline work."""
import contextlib
import os
import shutil
import threading
import time

try:
    import psutil
except ImportError:  # pragma: no cover - requirements includes psutil
    psutil = None

from .constants import LOCAL_STAGING_RESERVE_BYTES


def _bool_config(cfg, name, default=False):
    value = getattr(cfg, name, default)
    if isinstance(value, str):
        return value.strip().lower() in ("1", "true", "yes", "on")
    return bool(value)


class ResourceGovernor:
    """Central go/no-go checks for heavy local pipeline resources."""

    def __init__(self, cfg, staging_dir=None, sleep_seconds=2.0):
        self.cfg = cfg
        self.staging_dir = staging_dir or getattr(cfg, "staging_dir", ".")
        self.sleep_seconds = sleep_seconds
        self._lock = threading.RLock()
        self.tape_write_active = False
        self.tape_write_pending = False
        self.fetch_active = False
        self.pack_active = False
        self.db_sync_active = False

    @property
    def ram_soft_limit_pct(self):
        return float(getattr(self.cfg, "ram_soft_limit_pct", 70))

    @property
    def ram_hard_limit_pct(self):
        return float(getattr(self.cfg, "ram_hard_limit_pct", 85))

    @property
    def fetch_min_free_ram_bytes(self):
        gb = float(getattr(self.cfg, "fetch_min_free_ram_gb", 16))
        return int(gb * 1024**3)

    @property
    def tape_write_exclusive(self):
        return _bool_config(self.cfg, "tape_write_exclusive", True)

    def _virtual_memory(self):
        if psutil is None:
            return None
        return psutil.virtual_memory()

    def _disk_free(self):
        os.makedirs(self.staging_dir, exist_ok=True)
        return shutil.disk_usage(self.staging_dir).free

    def _memory_pct(self):
        vm = self._virtual_memory()
        return 0.0 if vm is None else float(vm.percent)

    def _memory_available(self):
        vm = self._virtual_memory()
        return None if vm is None else int(vm.available)

    def _hard_memory_ok(self):
        return self._memory_pct() < self.ram_hard_limit_pct

    def _soft_memory_ok(self):
        return self._memory_pct() < self.ram_soft_limit_pct

    def _tape_allows(self, flag_name):
        if not self.tape_write_exclusive:
            return True
        if not (self.tape_write_active or self.tape_write_pending):
            return True
        return _bool_config(self.cfg, flag_name, False)

    def _staging_ok(self, needed_bytes=0, queued_bytes=0):
        needed = max(0, int(needed_bytes or 0)) + max(0, int(queued_bytes or 0))
        return (self._disk_free() - needed) >= LOCAL_STAGING_RESERVE_BYTES

    def can_start_fetch(self, needed_bytes=0, queued_bytes=0):
        with self._lock:
            if not self._tape_allows("allow_fetch_during_tape_write"):
                return False
            if self.db_sync_active:
                return False
            if not self._hard_memory_ok():
                return False
            available = self._memory_available()
            if (available is not None and
                    available < self.fetch_min_free_ram_bytes):
                return False
            return self._staging_ok(needed_bytes, queued_bytes)

    def can_start_pack(self, needed_bytes=0, queued_bytes=0):
        with self._lock:
            if not self._tape_allows("allow_pack_during_tape_write"):
                return False
            if self.db_sync_active:
                return False
            if (self.fetch_active and
                    str(getattr(self.cfg, "allow_pack_during_fetch",
                                "conditional")).strip().lower() == "false"):
                return False
            if not self._hard_memory_ok():
                return False
            if (not self._soft_memory_ok() and
                    not _bool_config(self.cfg, "allow_pack_above_ram_soft",
                                     False)):
                return False
            return self._staging_ok(needed_bytes, queued_bytes)

    def can_start_tape_write(self):
        with self._lock:
            if not self._hard_memory_ok():
                return False
            return not (
                self.tape_write_active or self.fetch_active or
                self.pack_active or self.db_sync_active
            )

    def can_start_db_sync(self):
        with self._lock:
            if not self._tape_allows("allow_db_sync_during_tape_write"):
                return False
            if not _bool_config(self.cfg, "allow_db_sync_during_fetch", False):
                if self.fetch_active:
                    return False
            return self._hard_memory_ok() and not self.pack_active

    def can_cleanup(self):
        with self._lock:
            if self.tape_write_active or self.tape_write_pending:
                return False
            return not (self.fetch_active or self.pack_active)

    def wait_for_memory(self, stop_evt=None):
        while not (stop_evt is not None and stop_evt.is_set()):
            if self._hard_memory_ok():
                return True
            time.sleep(self.sleep_seconds)
        return False

    def wait_until(self, predicate, label, stop_evt=None):
        warned = False
        while not (stop_evt is not None and stop_evt.is_set()):
            if predicate():
                return True
            if not warned:
                print(f"[GOVERNOR] Waiting to start {label}: resource guard active.")
                warned = True
            time.sleep(self.sleep_seconds)
        return False

    @contextlib.contextmanager
    def mark_fetch_active(self):
        with self._mark("fetch_active"):
            yield

    @contextlib.contextmanager
    def mark_pack_active(self):
        with self._mark("pack_active"):
            yield

    @contextlib.contextmanager
    def mark_tape_write_active(self):
        with self._lock:
            self.tape_write_pending = False
        with self._mark("tape_write_active"):
            yield

    @contextlib.contextmanager
    def mark_db_sync_active(self):
        with self._mark("db_sync_active"):
            yield

    @contextlib.contextmanager
    def mark_tape_write_pending(self):
        with self._lock:
            self.tape_write_pending = True
        try:
            yield
        finally:
            with self._lock:
                self.tape_write_pending = False

    @contextlib.contextmanager
    def _mark(self, attr):
        with self._lock:
            setattr(self, attr, True)
        try:
            yield
        finally:
            with self._lock:
                setattr(self, attr, False)
