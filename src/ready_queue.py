"""Byte-bounded ready queue for the remote streaming pipeline.

Phase 4. The pipeline used ``queue.Queue(maxsize=prefetch_chunks_ahead)`` with
``prefetch_chunks_ahead = 1``: the packer handed a single ~1.7 GB chunk to an
empty writer queue, the writer took LTFS ownership, wrote for 20-40 s, released,
and the drive then sat idle for 25-37 minutes while the next chunk was packed.
Measured over the Tape_03 mount: the ready queue was empty in 22,860 of 22,945
heartbeat samples (99.6%), the tape duty cycle was 1.90%, and 698 GB of the
700 GB staging allowance went unused.

This queue instead bounds readiness by **bytes and count together**, so several
prepared chunks accumulate on NVMe and are then written consecutively inside a
single ownership period.

Two invariants shape the design:

* The limits describe **files already prepared on the staging disk**, never
  in-memory payloads. Nothing here allocates memory proportional to
  ``max_ready_bytes``; items hold a path and a byte count.
* A write group is **finite at acquisition time**. The writer snapshots what is
  ready, takes ownership, drains exactly that snapshot and releases. It never
  holds LTFS ownership while waiting for the producer to make more work.
"""
import threading
import time

# Item lifecycle. Deliberately not persisted anywhere: Phase 4 introduces no
# group-level durable state, and each chunk keeps its own PostgreSQL lifecycle.
STATE_READY = "ready"
STATE_WRITING = "writing"
STATE_WRITTEN = "written"
STATE_FAILED = "failed"
STATE_PRESERVED = "preserved"


class ReadyQueueLimits:
    """Validated byte/count bounds for the ready queue."""

    def __init__(self, min_start_bytes, target_bytes, max_bytes, max_chunks):
        self.min_start_bytes = int(min_start_bytes)
        self.target_bytes = int(target_bytes)
        self.max_bytes = int(max_bytes)
        self.max_chunks = int(max_chunks)
        self.validate()

    def validate(self):
        if self.min_start_bytes <= 0:
            raise ValueError(
                "min_ready_bytes_before_writer_start must be > 0")
        if self.min_start_bytes > self.target_bytes:
            raise ValueError(
                "min_ready_bytes_before_writer_start must be <= "
                "target_ready_bytes")
        if self.target_bytes > self.max_bytes:
            raise ValueError("target_ready_bytes must be <= max_ready_bytes")
        if self.max_chunks <= 0:
            raise ValueError("max_ready_chunks must be > 0")
        return True

    def as_dict(self):
        return {
            "min_ready_bytes_before_writer_start": self.min_start_bytes,
            "target_ready_bytes": self.target_bytes,
            "max_ready_bytes": self.max_bytes,
            "max_ready_chunks": self.max_chunks,
        }


class ReadyItem:
    """One prepared chunk waiting to be written."""

    __slots__ = ("chunk_index", "pack_dir", "prepared_bytes", "file_count",
                 "state", "desc", "enqueued_at")

    def __init__(self, chunk_index, pack_dir, prepared_bytes, file_count=None,
                 desc=None):
        self.chunk_index = int(chunk_index)
        self.pack_dir = pack_dir
        self.prepared_bytes = max(0, int(prepared_bytes or 0))
        self.file_count = file_count
        self.state = STATE_READY
        self.desc = desc
        self.enqueued_at = time.monotonic()

    def as_dict(self):
        return {
            "chunk_index": self.chunk_index,
            "prepared_bytes": self.prepared_bytes,
            "file_count": self.file_count,
            "state": self.state,
        }

    def __repr__(self):
        return (f"<ReadyItem chunk={self.chunk_index} "
                f"bytes={self.prepared_bytes} state={self.state}>")


class ReadyQueue:
    """Thread-safe, byte-bounded queue of prepared chunks.

    Counters stay correct across enqueue, blocked enqueue, group selection,
    success, failure, cancellation, preservation and shutdown, because every
    transition moves an item between exactly two counted sets under one lock.
    """

    def __init__(self, limits, name="ready"):
        self.limits = limits
        self.name = name
        self._cv = threading.Condition(threading.RLock())
        self._ready = []            # list[ReadyItem], FIFO by chunk order
        self._writing = []          # list[ReadyItem]
        self._closed = False        # producer finished (no more work coming)
        self._producer_exhausted = False   # temporarily out of input
        self._staging_pressure = False
        self._drain_requested = False
        # metrics
        self.producer_blocked_seconds = 0.0
        self.writer_waiting_seconds = 0.0
        self.chunks_written = 0
        self.bytes_written = 0
        self.chunks_failed = 0
        self.groups_started = 0

    # -- counters ---------------------------------------------------------
    @property
    def ready_chunks(self):
        with self._cv:
            return len(self._ready)

    @property
    def ready_bytes(self):
        with self._cv:
            return sum(i.prepared_bytes for i in self._ready)

    @property
    def writing_chunks(self):
        with self._cv:
            return len(self._writing)

    @property
    def writing_bytes(self):
        with self._cv:
            return sum(i.prepared_bytes for i in self._writing)

    @property
    def closed(self):
        with self._cv:
            return self._closed

    def metrics(self):
        with self._cv:
            return {
                "ready_chunks": len(self._ready),
                "ready_bytes": sum(i.prepared_bytes for i in self._ready),
                "ready_target_bytes": self.limits.target_bytes,
                "ready_max_bytes": self.limits.max_bytes,
                "ready_max_chunks": self.limits.max_chunks,
                "writing_chunks": len(self._writing),
                "writing_bytes": sum(i.prepared_bytes for i in self._writing),
                "producer_blocked_seconds": round(self.producer_blocked_seconds, 1),
                "writer_waiting_seconds": round(self.writer_waiting_seconds, 1),
                "chunks_written": self.chunks_written,
                "bytes_written": self.bytes_written,
                "chunks_failed": self.chunks_failed,
                "groups_started": self.groups_started,
                "producer_closed": self._closed,
            }

    # -- producer side ----------------------------------------------------
    def _has_capacity(self, item):
        if len(self._ready) + len(self._writing) >= self.limits.max_chunks:
            return False
        if not self._ready and not self._writing:
            # Never deadlock on a single chunk larger than the whole budget.
            return True
        current = sum(i.prepared_bytes for i in self._ready + self._writing)
        return current + item.prepared_bytes <= self.limits.max_bytes

    def put(self, item, stop_event=None, timeout=None):
        """Enqueue a prepared chunk, blocking while the queue is full.

        Returns True when enqueued, False when a stop was requested first.
        """
        started = time.monotonic()
        blocked = False
        with self._cv:
            while not self._has_capacity(item):
                if stop_event is not None and stop_event.is_set():
                    if blocked:
                        self.producer_blocked_seconds += (
                            time.monotonic() - started)
                    return False
                blocked = True
                if not self._cv.wait(timeout=timeout or 1.0):
                    continue
            if blocked:
                self.producer_blocked_seconds += time.monotonic() - started
            item.state = STATE_READY
            self._ready.append(item)
            self._ready.sort(key=lambda i: i.chunk_index)
            self._cv.notify_all()
            return True

    def close(self, reason="producer finished"):
        """Producer will add nothing further; unblocks a waiting writer."""
        with self._cv:
            self._closed = True
            self._cv.notify_all()

    def set_producer_exhausted(self, value=True):
        """Producer temporarily has no input (scan paused/stalled)."""
        with self._cv:
            self._producer_exhausted = bool(value)
            self._cv.notify_all()

    def set_staging_pressure(self, value=True):
        """Staging is filling; the writer should drain to release space."""
        with self._cv:
            self._staging_pressure = bool(value)
            self._cv.notify_all()

    def request_drain(self, value=True):
        """Explicit final-partial-drain (safe stop, session end, operator)."""
        with self._cv:
            self._drain_requested = bool(value)
            self._cv.notify_all()

    # -- writer side ------------------------------------------------------
    def start_reason(self):
        """Why the writer may start now, or None if it should keep waiting.

        Caller must hold nothing; this takes the queue lock itself.
        """
        with self._cv:
            return self._start_reason_locked()

    def _start_reason_locked(self):
        if not self._ready:
            return None
        ready_bytes = sum(i.prepared_bytes for i in self._ready)
        if ready_bytes >= self.limits.min_start_bytes:
            return "min_ready_bytes_reached"
        if len(self._ready) >= self.limits.max_chunks:
            return "max_ready_chunks_reached"
        if ready_bytes >= self.limits.max_bytes:
            return "max_ready_bytes_reached"
        if self._closed:
            return "producer_finished_final_group"
        if self._producer_exhausted:
            return "producer_input_exhausted"
        if self._staging_pressure:
            return "staging_pressure_drain"
        if self._drain_requested:
            return "explicit_drain_requested"
        return None

    def wait_for_group(self, stop_event=None, poll=0.5, timeout=None):
        """Block until a write group may start.

        Returns ``(items, reason)`` with a FINITE snapshot, or ``([], reason)``
        when stopping. The snapshot is taken under the lock and never grows
        afterwards: the writer must not hold LTFS ownership waiting for more.
        """
        started = time.monotonic()
        deadline = None if timeout is None else started + timeout
        with self._cv:
            while True:
                if stop_event is not None and stop_event.is_set():
                    # A stop still permits draining what is already prepared,
                    # but the caller decides; report the state honestly.
                    self.writer_waiting_seconds += time.monotonic() - started
                    return [], "stop_requested"
                reason = self._start_reason_locked()
                if reason is not None:
                    items = list(self._ready)
                    self._ready.clear()
                    for it in items:
                        it.state = STATE_WRITING
                    self._writing.extend(items)
                    self.groups_started += 1
                    self.writer_waiting_seconds += time.monotonic() - started
                    self._cv.notify_all()
                    return items, reason
                if self._closed and not self._ready and not self._writing:
                    self.writer_waiting_seconds += time.monotonic() - started
                    return [], "producer_closed_empty"
                if deadline is not None and time.monotonic() >= deadline:
                    self.writer_waiting_seconds += time.monotonic() - started
                    return [], "timeout"
                self._cv.wait(timeout=poll)

    # -- completion transitions ------------------------------------------
    def _remove_from_writing(self, item):
        try:
            self._writing.remove(item)
        except ValueError:
            pass

    def mark_written(self, item):
        with self._cv:
            self._remove_from_writing(item)
            item.state = STATE_WRITTEN
            self.chunks_written += 1
            self.bytes_written += item.prepared_bytes
            self._cv.notify_all()

    def mark_failed(self, item):
        with self._cv:
            self._remove_from_writing(item)
            item.state = STATE_FAILED
            self.chunks_failed += 1
            self._cv.notify_all()

    def mark_preserved(self, item):
        """A selected-but-unstarted chunk: its pack stays reusable."""
        with self._cv:
            self._remove_from_writing(item)
            item.state = STATE_PRESERVED
            self._cv.notify_all()

    def preserve_remaining(self, items):
        """Bulk-preserve every not-yet-written item of an aborted group."""
        preserved = []
        with self._cv:
            for it in items:
                if it.state in (STATE_WRITING, STATE_READY):
                    self._remove_from_writing(it)
                    it.state = STATE_PRESERVED
                    preserved.append(it)
            self._cv.notify_all()
        return preserved

    def drain_ready(self):
        """Remove and return everything still queued (shutdown paths)."""
        with self._cv:
            items = list(self._ready)
            self._ready.clear()
            for it in items:
                it.state = STATE_PRESERVED
            self._cv.notify_all()
            return items
