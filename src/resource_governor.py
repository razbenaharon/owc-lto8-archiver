"""Resource Governor for heavy local archive pipeline work."""
import contextlib
from dataclasses import dataclass, field
import os
import shutil
import threading
import time

try:
    import psutil
except ImportError:  # pragma: no cover - requirements includes psutil
    psutil = None

from .constants import LOCAL_STAGING_RESERVE_BYTES


GB = 1024**3


def _bool_config(cfg, name, default=False):
    value = getattr(cfg, name, default)
    if isinstance(value, str):
        return value.strip().lower() in ("1", "true", "yes", "on")
    return bool(value)


@dataclass
class GovernorDecision:
    allowed: bool
    action: str
    stage: str
    reasons: list = field(default_factory=list)
    memory_pct: float = 0.0
    available_gb: float = 0.0
    process_tree_rss_mb: float = 0.0
    effective_min_free_gb: float = 0.0
    hard_limit_pct: float = 0.0
    tape_active: bool = False
    wait_seconds: float = 0.0

    def reason_text(self):
        return ",".join(self.reasons)


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
        self._wait_started = {}
        self._last_status = {}
        self.total_wait_seconds = 0.0
        self.wait_reasons = set()
        self.cleanup_active = False

    @property
    def ram_soft_limit_pct(self):
        return float(getattr(self.cfg, "ram_soft_limit_pct", 70))

    @property
    def ram_hard_limit_pct(self):
        return float(getattr(self.cfg, "ram_hard_limit_pct", 85))

    @property
    def fetch_min_free_ram_bytes(self):
        gb = float(getattr(
            self.cfg, "governor_fetch_target_free_ram_gb",
            getattr(self.cfg, "fetch_min_free_ram_gb", 16)))
        return int(gb * GB)

    @property
    def fetch_floor_bytes(self):
        gb = float(getattr(self.cfg, "governor_fetch_min_free_floor_gb", 2.5))
        return int(max(0.5, gb) * GB)

    @property
    def tape_min_free_ram_bytes(self):
        gb = float(getattr(self.cfg, "governor_tape_min_free_ram_gb", 3.0))
        return int(max(0.5, gb) * GB)

    @property
    def status_interval_seconds(self):
        return float(getattr(self.cfg, "governor_status_interval_seconds", 60))

    @property
    def soft_relax_after_seconds(self):
        return float(getattr(self.cfg, "governor_soft_relax_after_seconds", 120))

    @property
    def soft_relax_factor(self):
        return float(getattr(self.cfg, "governor_soft_relax_factor", 0.75))

    @property
    def cold_min_free_ram_bytes(self):
        gb = float(getattr(self.cfg, "cold_min_free_ram_gb", 16))
        return int(gb * 1024**3)

    @property
    def cold_max_ram_pct(self):
        return float(getattr(self.cfg, "cold_max_ram_pct", 60))

    @property
    def cold_max_local_disk_io_mbs(self):
        return float(getattr(self.cfg, "cold_max_local_disk_io_mbs", 200))

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

    def _memory_total(self):
        vm = self._virtual_memory()
        return None if vm is None else int(vm.total)

    def _process_tree_rss_mb(self):
        if psutil is None:
            return 0.0
        try:
            proc = psutil.Process()
            procs = [proc] + proc.children(recursive=True)
            total = 0
            for item in procs:
                try:
                    total += item.memory_info().rss
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue
            return total / 1024**2
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return 0.0

    def _cold_memory_ok(self):
        available = self._memory_available()
        if self._memory_pct() >= self.cold_max_ram_pct:
            return False
        if (available is not None and
                available < self.cold_min_free_ram_bytes):
            return False
        return True

    def _local_disk_io_busy(self):
        if psutil is None:
            return False
        try:
            first = psutil.disk_io_counters()
            if first is None:
                return False
            time.sleep(0.05)
            second = psutil.disk_io_counters()
        except Exception:
            return False
        if second is None:
            return False
        delta = (
            (second.read_bytes - first.read_bytes) +
            (second.write_bytes - first.write_bytes)
        )
        mbs = delta / 0.05 / 1024**2
        return mbs > self.cold_max_local_disk_io_mbs

    def _hard_memory_ok(self):
        return self._memory_pct() < self.ram_hard_limit_pct

    def _soft_memory_ok(self):
        return self._memory_pct() < self.ram_soft_limit_pct

    def _tape_blocks(self, action):
        """Tape-gate blocking state for a stage decision.

        A *pending* tape write must only block new stage STARTS. A stage that
        is already running ("continue" checkpoints) must be allowed to drain
        and finish, because the tape's own start gate waits for the active
        stage flags to clear — blocking mid-stage checkpoints on `pending`
        creates a circular wait (producer waits for pending to clear, tape
        waits for fetch/pack_active to clear: deadlock). An ACTIVE tape write
        still pauses mid-stage work, protecting RAM/IO while robocopy streams.
        """
        if action == "start":
            return self.tape_write_active or self.tape_write_pending
        return self.tape_write_active

    def _tape_allows(self, flag_name, action="start"):
        if not self.tape_write_exclusive:
            return True
        if not self._tape_blocks(action):
            return True
        return _bool_config(self.cfg, flag_name, False)

    def _staging_ok(self, needed_bytes=0, queued_bytes=0):
        needed = max(0, int(needed_bytes or 0)) + max(0, int(queued_bytes or 0))
        return (self._disk_free() - needed) >= LOCAL_STAGING_RESERVE_BYTES

    def _effective_fetch_min_free_bytes(self, wait_seconds=0):
        target = self.fetch_min_free_ram_bytes
        total = self._memory_total()
        if total:
            cap_pct = float(getattr(
                self.cfg, "governor_fetch_total_ram_cap_pct", 25))
            target = min(target, int(total * max(1.0, cap_pct) / 100.0))
        floor = self.fetch_floor_bytes
        if wait_seconds > 0 and self.soft_relax_after_seconds > 0:
            steps = int(wait_seconds // self.soft_relax_after_seconds)
            if steps > 0:
                factor = min(0.99, max(0.10, self.soft_relax_factor))
                target = int(target * (factor ** steps))
        return max(floor, target)

    # A drain stage (pack, db_sync) reads staged data off disk and writes a ZIP
    # or streams a COPY: its own footprint is tiny (tens of MB), and blocking it
    # can never lower host RAM because it is not the consumer. The RAM ceiling
    # exists to throttle the real consumers (fetch fills page cache with a whole
    # chunk; the tape writer). Gating drains on global RAM% therefore deadlocks
    # the pipeline: the consumer filled memory, and the drain that would free
    # the staging disk is refused. After the soft-relax window a drain is let
    # through despite the ceiling, provided a small absolute floor of memory is
    # still free so we never push the box into hard thrashing.
    _DRAIN_STAGE_MIN_FREE_BYTES = 512 * 1024**2

    def _drain_stage_relaxed(self, stage, wait_seconds):
        if stage not in ("pack", "db_sync"):
            return False
        window = self.soft_relax_after_seconds
        if window <= 0 or wait_seconds < window:
            return False
        available = self._memory_available()
        return (available is None or
                available >= self._DRAIN_STAGE_MIN_FREE_BYTES)

    def _base_decision(self, stage, action, wait_seconds=0):
        available = self._memory_available()
        available_gb = 0.0 if available is None else available / GB
        return GovernorDecision(
            allowed=True,
            action=action,
            stage=stage,
            memory_pct=self._memory_pct(),
            available_gb=available_gb,
            process_tree_rss_mb=self._process_tree_rss_mb(),
            hard_limit_pct=self.ram_hard_limit_pct,
            tape_active=self.tape_write_active or self.tape_write_pending,
            wait_seconds=wait_seconds,
        )

    def decision(self, stage, action="start", needed_bytes=0, queued_bytes=0,
                 wait_seconds=0):
        with self._lock:
            stage = str(stage)
            action = str(action)
            dec = self._base_decision(stage, action, wait_seconds)
            available = self._memory_available()
            drain_relaxed = self._drain_stage_relaxed(stage, wait_seconds)
            if not self._hard_memory_ok() and not drain_relaxed:
                dec.reasons.append("hard_ram_limit")

            if stage == "fetch":
                min_free = self._effective_fetch_min_free_bytes(wait_seconds)
                dec.effective_min_free_gb = min_free / GB
                if available is not None and available < min_free:
                    dec.reasons.append("fetch_min_free_ram")
                if not self._tape_allows("allow_fetch_during_tape_write",
                                         action):
                    dec.reasons.append("tape_active")
                if self.db_sync_active:
                    dec.reasons.append("db_sync_active")
                if self.pack_active and not self._soft_memory_ok():
                    dec.reasons.append("pack_memory_pressure")
            elif stage == "pack":
                if not self._tape_allows("allow_pack_during_tape_write",
                                         action):
                    dec.reasons.append("tape_active")
                if self.db_sync_active:
                    dec.reasons.append("db_sync_active")
                if (self.fetch_active and
                        str(getattr(self.cfg, "allow_pack_during_fetch",
                                    "conditional")).strip().lower() == "false"):
                    dec.reasons.append("fetch_active")
                if (not self._soft_memory_ok() and
                        not _bool_config(self.cfg, "allow_pack_above_ram_soft",
                                         False) and
                        not drain_relaxed):
                    dec.reasons.append("ram_soft_limit")
            elif stage == "tape":
                dec.effective_min_free_gb = self.tape_min_free_ram_bytes / GB
                if available is not None and available < self.tape_min_free_ram_bytes:
                    dec.reasons.append("tape_min_free_ram")
                if self.tape_write_active or self.fetch_active or self.pack_active or self.db_sync_active:
                    dec.reasons.append("heavy_stage_active")
            elif stage == "db_sync":
                if not self._tape_allows("allow_db_sync_during_tape_write",
                                         action):
                    dec.reasons.append("tape_active")
                if not _bool_config(self.cfg, "allow_db_sync_during_fetch", False):
                    if self.fetch_active:
                        dec.reasons.append("fetch_active")
                if self.pack_active:
                    dec.reasons.append("pack_active")
            elif stage == "cleanup":
                if self.tape_write_active or self.tape_write_pending:
                    dec.reasons.append("tape_active")
                if self.fetch_active or self.pack_active:
                    dec.reasons.append("producer_active")

            if (stage in ("fetch", "pack", "db_sync") and
                    _bool_config(self.cfg, "governor_tape_exclusive_heavy_stages",
                                 False) and
                    self._tape_blocks(action)):
                if "tape_active" not in dec.reasons:
                    dec.reasons.append("tape_active")
            # RAM reserve for the tape writer: enforced whenever a tape write
            # is actually running (any action), but a merely *pending* write
            # only blocks new starts — a mid-stage checkpoint must drain so
            # the tape's heavy_stage_active wait can resolve. The hard RAM
            # limit above still applies unconditionally to every action.
            if (stage in ("fetch", "pack", "db_sync") and
                    _bool_config(self.cfg, "governor_tape_pause_other_stages",
                                 True) and
                    self._tape_blocks(action) and
                    available is not None and
                    available < self.tape_min_free_ram_bytes):
                dec.effective_min_free_gb = max(
                    dec.effective_min_free_gb,
                    self.tape_min_free_ram_bytes / GB)
                dec.reasons.append("tape_ram_reserve")

            if not self._staging_ok(needed_bytes, queued_bytes):
                dec.reasons.append("staging_reserve")

            dec.allowed = not dec.reasons
            return dec

    def can_start_fetch(self, needed_bytes=0, queued_bytes=0):
        return self.decision(
            "fetch", "start", needed_bytes, queued_bytes).allowed

    def can_start_pack(self, needed_bytes=0, queued_bytes=0):
        return self.decision(
            "pack", "start", needed_bytes, queued_bytes).allowed

    def can_start_tape_write(self):
        return self.decision("tape", "start").allowed

    def can_start_db_sync(self):
        return self.decision("db_sync", "start").allowed

    def can_cleanup(self):
        return self.decision("cleanup", "start").allowed

    def can_start_cold_migration(self):
        with self._lock:
            if (self.tape_write_active or self.tape_write_pending or
                    self.fetch_active or self.pack_active or
                    self.db_sync_active or self.cleanup_active):
                return False
            if not self._cold_memory_ok():
                return False
            if not self._staging_ok():
                return False
            if self._local_disk_io_busy():
                return False
            return True

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

    def wait_or_pause(self, stage, action="start", needed_bytes=0,
                      queued_bytes=0, stop_evt=None):
        key = (stage, action)
        while not (stop_evt is not None and stop_evt.is_set()):
            now = time.time()
            started = self._wait_started.setdefault(key, now)
            wait_seconds = now - started
            dec = self.decision(
                stage, action, needed_bytes=needed_bytes,
                queued_bytes=queued_bytes, wait_seconds=wait_seconds)
            if dec.allowed:
                with self._lock:
                    self._wait_started.pop(key, None)
                return True

            self.wait_reasons.update(dec.reasons)
            last = self._last_status.get(key, 0)
            if now - last >= self.status_interval_seconds:
                print(
                    f"[GOVERNOR] {stage} {action}: "
                    f"available={dec.available_gb:.2f} GB, "
                    f"min={dec.effective_min_free_gb:.2f} GB, "
                    f"memory={dec.memory_pct:.1f}%, "
                    f"process_rss={dec.process_tree_rss_mb:.0f} MB, "
                    f"tape_active={str(dec.tape_active).lower()}, "
                    f"reason={dec.reason_text() or 'unknown'}"
                )
                self._last_status[key] = now
            sleep_for = min(self.sleep_seconds, 2.0)
            self.total_wait_seconds += sleep_for
            time.sleep(sleep_for)
        return False

    def telemetry_details(self):
        return {
            "governor_wait_seconds": f"{self.total_wait_seconds:.3f}",
            "governor_wait_reasons": ";".join(sorted(self.wait_reasons)),
        }

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
    def mark_cleanup_active(self):
        with self._mark("cleanup_active"):
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
