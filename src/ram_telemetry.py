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

    def __exit__(self, _exc_type, _exc, _tb):
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


class TapeWriteProfiler:
    """Decompose a single robocopy tape write into phases, tape-safely.

    Samples ONLY the robocopy process's own kernel I/O counter
    (``psutil.Process(pid).io_counters().write_bytes``) once per interval.
    It never opens, stats, reads, or walks the LTFS tape (E:), and it never
    touches the robocopy process's execution — it is a passive read of OS
    per-process accounting, so it cannot slow, interrupt, or interfere with the
    write. (The tape write is CPU-pinned to its own cores; this thread is not.)

    From the per-second write-rate series it isolates:
      * ``open_seconds``   - leading time before robocopy streams data (LTFS file
                             create + locate to end-of-data on a filling tape).
      * ``stream_mbs``     - the CLEAN streaming rate = bytes moved while actively
                             writing / seconds spent actively writing. This is the
                             real drive throughput, free of the open/close/flush
                             overhead that drags down bytes/total-time.
      * ``close_seconds``  - trailing time after the last data write while robocopy
                             is still alive (buffer flush + file mark).
      * ``stall_seconds`` / ``stall_count`` - below-floor gaps *within* the
                             streaming window. Repeated stalls = back-hitch =
                             shoe-shining; an isolated one is benign buffer priming.

    psutil-absent or no-samples degrade to blank metrics (callers fall back).
    """

    def __init__(self, interval_seconds=1.0, stream_floor_mbs=5.0):
        self.interval_seconds = max(0.25, float(interval_seconds or 1.0))
        self.stream_floor_bps = max(0.0, float(stream_floor_mbs)) * 1024**2
        self._stop = threading.Event()
        self._attached = threading.Event()
        self._thread = None
        self._proc = None            # psutil.Process handle for robocopy
        self._start_t = None         # monotonic time robocopy was attached
        self._samples = []           # list of (monotonic_t, cumulative_write_bytes)
        # computed results (None until __exit__)
        self.stream_mbs = None
        self.stream_peak_mbs = None
        self.open_seconds = None
        self.close_seconds = None
        self.stall_seconds = None
        self.stall_count = None
        self.streamed_bytes = None

    def attach(self, proc):
        """Called by _run_robocopy_tuned with the live robocopy Popen.

        Grabs a psutil handle for that exact PID so sampling targets only this
        write. Best-effort: any failure leaves the profiler inert.
        """
        if psutil is None:
            return
        try:
            self._proc = psutil.Process(proc.pid)
            self._start_t = time.monotonic()
            self._attached.set()
        except Exception:
            pass

    def __enter__(self):
        self._thread = threading.Thread(
            target=self._run, name="tape-write-profiler", daemon=True)
        self._thread.start()
        return self

    def __exit__(self, _exc_type, _exc, _tb):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=3)
        try:
            self._compute()
        except Exception:
            pass
        return False

    def _read_write_bytes(self):
        try:
            return int(self._proc.io_counters().write_bytes)
        except Exception:
            return None

    def _run(self):
        # Wait until robocopy is attached (or the write is over before it started).
        while not self._stop.is_set():
            if self._attached.wait(0.2):
                break
        if self._proc is None:
            return
        # Sample the robocopy write counter every interval until the stage ends.
        while not self._stop.is_set():
            wb = self._read_write_bytes()
            if wb is not None:
                self._samples.append((time.monotonic(), wb))
            if self._stop.wait(self.interval_seconds):
                break
        wb = self._read_write_bytes()
        if wb is not None:
            self._samples.append((time.monotonic(), wb))

    def _compute(self):
        s = self._samples
        if self._start_t is None or len(s) < 2:
            return
        floor = self.stream_floor_bps
        # Per-interval write rates: (interval_end_t, bytes_delta, dt, bytes_per_sec)
        rates = []
        for i in range(1, len(s)):
            t0, b0 = s[i - 1]
            t1, b1 = s[i]
            dt = t1 - t0
            if dt <= 0:
                continue
            db = max(0, b1 - b0)
            rates.append((t1, db, dt, db / dt))
        if not rates:
            return

        active = [r for r in rates if r[3] >= floor]
        if not active:
            # Never crossed the streaming floor: report the overall average so the
            # metric is still populated, with no phase split.
            total_b = s[-1][1] - s[0][1]
            total_t = s[-1][0] - s[0][0]
            self.stream_mbs = (total_b / total_t) / 1024**2 if total_t > 0 else 0.0
            self.stream_peak_mbs = max(r[3] for r in rates) / 1024**2
            self.open_seconds = 0.0
            self.close_seconds = 0.0
            self.stall_seconds = 0.0
            self.stall_count = 0
            self.streamed_bytes = total_b
            return

        first_active_t = active[0][0]
        last_active_t = active[-1][0]
        # Open = robocopy start -> start of the first active interval.
        first_active_start = active[0][0] - active[0][2]
        self.open_seconds = max(0.0, first_active_start - self._start_t)
        # Close = end of last active interval -> last observed sample (flush tail).
        self.close_seconds = max(0.0, s[-1][0] - last_active_t)

        streamed_bytes = sum(r[1] for r in active)
        active_seconds = sum(r[2] for r in active)
        self.streamed_bytes = streamed_bytes
        self.stream_mbs = (
            (streamed_bytes / active_seconds) / 1024**2 if active_seconds > 0 else 0.0)
        self.stream_peak_mbs = max(r[3] for r in active) / 1024**2

        # Stalls = below-floor intervals strictly within the streaming window.
        window = [r for r in rates if first_active_t <= r[0] <= last_active_t]
        self.stall_seconds = sum(r[2] for r in window if r[3] < floor)
        count = 0
        prev_stall = False
        for r in window:
            is_stall = r[3] < floor
            if is_stall and not prev_stall:
                count += 1
            prev_stall = is_stall
        self.stall_count = count

    def as_details(self, prefix):
        return {
            f"{prefix}_stream_mbs": _fmt(self.stream_mbs),
            f"{prefix}_stream_peak_mbs": _fmt(self.stream_peak_mbs),
            f"{prefix}_open_seconds": _fmt(self.open_seconds),
            f"{prefix}_close_seconds": _fmt(self.close_seconds),
            f"{prefix}_stall_seconds": _fmt(self.stall_seconds),
            f"{prefix}_stall_count": (
                '' if self.stall_count is None else str(self.stall_count)),
        }


def _fmt(value):
    if value is None:
        return ''
    try:
        return f"{float(value):.3f}"
    except (TypeError, ValueError):
        return ''
