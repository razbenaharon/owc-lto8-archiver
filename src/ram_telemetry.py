"""Lightweight RAM sampling for archive pipeline stages."""
import threading
import time

try:
    import psutil
except ImportError:  # pragma: no cover - requirements includes psutil
    psutil = None


def _process_tree_rss_mb():
    if psutil is None:
        return None
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
        return None


class RamStageSampler:
    """Sample system/process memory while one pipeline stage is active."""

    def __init__(self, stage, interval_seconds=5.0):
        self.stage = stage
        self.interval_seconds = max(0.5, float(interval_seconds or 5.0))
        self._stop = threading.Event()
        self._thread = None
        self.peak_pct = None
        self.min_available_gb = None
        self.process_peak_mb = None

    def __enter__(self):
        self._sample()
        self._thread = threading.Thread(
            target=self._run, name=f"ram-sampler-{self.stage}", daemon=True)
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc, tb):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2)
        self._sample()
        return False

    def _run(self):
        while not self._stop.wait(self.interval_seconds):
            self._sample()

    def _sample(self):
        if psutil is not None:
            try:
                vm = psutil.virtual_memory()
                pct = float(vm.percent)
                avail_gb = int(vm.available) / 1024**3
                self.peak_pct = pct if self.peak_pct is None else max(
                    self.peak_pct, pct)
                self.min_available_gb = (
                    avail_gb if self.min_available_gb is None else
                    min(self.min_available_gb, avail_gb)
                )
            except Exception:
                pass
        rss_mb = _process_tree_rss_mb()
        if rss_mb is not None:
            self.process_peak_mb = (
                rss_mb if self.process_peak_mb is None else
                max(self.process_peak_mb, rss_mb)
            )

    def as_details(self, prefix):
        return {
            f"{prefix}_ram_peak_pct": _fmt(self.peak_pct),
            f"{prefix}_ram_min_available_gb": _fmt(self.min_available_gb),
            f"{prefix}_process_peak_mb": _fmt(self.process_peak_mb),
        }


def _fmt(value):
    if value is None:
        return ''
    try:
        return f"{float(value):.3f}"
    except (TypeError, ValueError):
        return ''
