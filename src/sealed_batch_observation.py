"""Asynchronous, non-authoritative observation of the real write-group boundary.

Phase 5D wires :class:`~src.sealed_batch_observer.SealedBatchObserver` into the
production scheduler as a **diagnostic-only** observer. The writer thread does the
absolute minimum at the boundary — copy an immutable snapshot and a single
non-blocking enqueue — and returns to writing. A dedicated bounded worker thread
does everything expensive (one read-only PostgreSQL query for the selected chunk
ids, the observer computation, JSON serialization, and bounded log rotation).

Hard guarantees:

* The observer never influences ReadyQueue selection, group membership/ordering,
  writer-start, LTFS ownership, readiness/cartridge, robocopy, chunk status,
  retries, cancellation, safe-stop, pack cleanup, durability, or pruning.
* A full queue drops the observation (never the write group) and bumps a metric.
* Observer/worker/DB/log failures are contained and never reach the writer.
* Shutdown is bounded; a safe stop never waits indefinitely for diagnostics.
* No sealed-batch table is queried; no production row is mutated; Migration 012
  is not required. Records are append-only, disposable, and non-authoritative.
"""
import json
import logging
import os
import queue
import threading
import time
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from typing import Optional, Tuple

from .constants import PROJECT_ROOT
from .logsetup import get_logger
from .sealed_batch_observer import (SealedBatchObserver, ReadyGroupSnapshot,
                                    SnapshotMember, OBSERVER_VERSION,
                                    OBSERVATION_SCHEMA_VERSION)

# Consistency-window diagnostic classifications (never stop/delay the writer).
STALE_OBSERVATION = "STALE_OBSERVATION"
MISSING_CHUNK_ROW = "MISSING_CHUNK_ROW"
STATUS_CHANGED_DURING_WINDOW = "STATUS_CHANGED_DURING_WINDOW"
DB_OBSERVATION_UNAVAILABLE = "DB_OBSERVATION_UNAVAILABLE"

# When the scheduler-to-DB-read gap exceeds this, the read is a stale view.
_DEFAULT_STALE_WINDOW_SECONDS = 30.0


@dataclass(frozen=True)
class WriteGroupSnapshot:
    """Immutable snapshot captured at the finite-group boundary. Frozen + only
    immutable fields (tuples), so it cannot be mutated after submission."""
    correlation_id: str
    session_id: int
    chunk_ids: Tuple[int, ...]
    prepared_bytes: Tuple[int, ...]
    total_prepared_bytes: int
    pack_identities: Tuple[Optional[str], ...]
    expected_tape: str
    selection_reason: str
    ready_queue_generation: int
    scan_complete: bool
    producer_state: str
    staging_pressure: bool
    safe_stop: bool
    snapshot_ts_utc: str
    writer_path: str                      # 'streaming' | 'legacy'
    software_version: str = OBSERVER_VERSION
    observation_schema_version: int = OBSERVATION_SCHEMA_VERSION


@dataclass(frozen=True)
class WriteGroupOutcome:
    """Immutable actual-outcome event, correlated by ``correlation_id``."""
    correlation_id: str
    session_id: int
    selected_chunk_ids: Tuple[int, ...]
    started_chunk_ids: Tuple[int, ...]
    completed_chunk_ids: Tuple[int, ...]
    failing_chunk: Optional[int]
    stop_reason: Optional[str]
    group_start_ts: float
    group_finish_ts: float
    ownership_acquisitions: int
    readiness_checks: int
    cartridge_verifications: int
    writer_invocations: int


@dataclass
class ObservationConfig:
    enabled: bool = False
    queue_max: int = 100
    shutdown_timeout_seconds: float = 5.0
    statement_timeout_seconds: float = 5.0
    log_path: str = ""
    log_max_bytes: int = 10 * 1024 * 1024
    log_backups: int = 5
    stale_window_seconds: float = _DEFAULT_STALE_WINDOW_SECONDS

    @classmethod
    def from_cfg(cls, cfg):
        return cls(
            enabled=cfg.sealed_tape_write_batches_observation_enabled,
            queue_max=cfg.sealed_batch_observation_queue_max,
            shutdown_timeout_seconds=cfg.sealed_batch_observation_shutdown_timeout_seconds,
            statement_timeout_seconds=cfg.sealed_batch_observation_statement_timeout_seconds,
            log_path=cfg.sealed_batch_observation_log_path,
            log_max_bytes=cfg.sealed_batch_observation_log_max_bytes,
            log_backups=cfg.sealed_batch_observation_log_backups)


class ObservationLogPathError(RuntimeError):
    """Configured log path is inside a forbidden location."""


def _validate_log_path(path, cfg):
    """The diagnostic log must not live inside the repo, the LTFS mount, an
    active pack/staging dir, or the diagnostic-evidence tree."""
    if not path:
        raise ObservationLogPathError("no observation log path configured")
    ap = os.path.abspath(path)
    lower = ap.lower()
    forbidden = [os.path.abspath(PROJECT_ROOT)]
    for attr in ("staging_dir", "lto_drive", "backup_log_dir"):
        try:
            v = getattr(cfg, attr, None)
            if v:
                forbidden.append(os.path.abspath(v))
        except Exception:
            pass
    forbidden.append(os.path.abspath(r"C:\lto8-evidence"))
    for root in forbidden:
        r = root.lower().rstrip("\\/")
        if lower == r or lower.startswith(r + os.sep) or lower.startswith(r + "/"):
            raise ObservationLogPathError(
                f"observation log path {ap!r} is inside a forbidden location "
                f"{root!r}")
    return ap


class _StatusReader:
    """One bounded, read-only PostgreSQL lookup for the selected chunk ids.

    Never writes, never locks for write, never touches a sealed-batch table, and
    always closes/rolls back. Any failure -> (None, ...) so the caller degrades
    to DB_OBSERVATION_UNAVAILABLE."""

    def __init__(self, db_dsn, statement_timeout_seconds):
        self._dsn = db_dsn
        self._timeout_ms = max(1, int(statement_timeout_seconds * 1000))

    def read(self, session_id, chunk_ids):
        import psycopg
        t0 = time.perf_counter()
        try:
            conn = psycopg.connect(self._dsn, connect_timeout=5)
        except Exception as e:
            return None, time.perf_counter() - t0, None, str(e)
        try:
            conn.autocommit = False
            with conn.transaction():
                conn.execute("SET TRANSACTION READ ONLY")
                conn.execute(f"SET LOCAL statement_timeout = {self._timeout_ms}")
                iso = conn.execute("SHOW transaction_isolation").fetchone()[0]
                rows = conn.execute(
                    "SELECT chunk_index, status FROM remote_chunks "
                    "WHERE session_id=%s AND chunk_index = ANY(%s)",
                    (session_id, list(chunk_ids))).fetchall()
            return ({int(r[0]): r[1] for r in rows},
                    time.perf_counter() - t0, iso, None)
        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            return None, time.perf_counter() - t0, None, str(e)
        finally:
            conn.close()


class ObservationWorker:
    """Bounded queue + one worker thread. All expensive work runs here."""

    def __init__(self, obs_cfg, observer, status_reader, record_logger,
                 stale_window_seconds=_DEFAULT_STALE_WINDOW_SECONDS):
        self._cfg = obs_cfg
        self._observer = observer
        self._reader = status_reader
        self._log = record_logger
        self._stale = stale_window_seconds
        self._q = queue.Queue(maxsize=obs_cfg.queue_max)
        self._stop = threading.Event()
        self._thread = None
        self._last_warn = 0.0
        self._snapshots = {}          # correlation_id -> snapshot dict (bounded by queue flow)
        # counters (read for diagnostics/tests)
        self.submitted = 0
        self.completed = 0
        self.failed = 0
        self.dropped = 0

    # -- writer-thread side (non-blocking only) --------------------------
    def start(self):
        self._thread = threading.Thread(
            target=self._run, name="sealed-batch-observer", daemon=True)
        self._thread.start()
        return self

    def submit_snapshot(self, snapshot):
        return self._offer(("snapshot", snapshot))

    def submit_outcome(self, outcome):
        return self._offer(("outcome", outcome))

    def _offer(self, item):
        try:
            self._q.put_nowait(item)
            self.submitted += 1
            return True
        except queue.Full:
            self.dropped += 1
            now = time.monotonic()
            if now - self._last_warn > 30:
                self._last_warn = now
                get_logger().warning(
                    "sealed_batch_observation_dropped: queue full "
                    "(dropped_total=%d); the write group is unaffected",
                    self.dropped)
            return False

    # -- worker thread ----------------------------------------------------
    def _run(self):
        while not (self._stop.is_set() and self._q.empty()):
            try:
                kind, payload = self._q.get(timeout=0.25)
            except queue.Empty:
                continue
            try:
                if kind == "snapshot":
                    self._process_snapshot(payload)
                else:
                    self._process_outcome(payload)
                self.completed += 1
            except Exception:
                # Contained: an observer failure must never affect the writer.
                self.failed += 1
                get_logger().exception("sealed_batch_observation_worker_error")

    def _process_snapshot(self, snap):
        remote_status, db_secs, iso, db_err = ({}, 0.0, None, None)
        window_codes = []
        if self._reader is not None:
            remote_status, db_secs, iso, db_err = self._reader.read(
                snap.session_id, snap.chunk_ids)
        db_read_ts = datetime.now(timezone.utc)
        if remote_status is None:
            window_codes.append(DB_OBSERVATION_UNAVAILABLE)
            remote_status = {}
        else:
            for ci in snap.chunk_ids:
                if ci not in remote_status:
                    window_codes.append(MISSING_CHUNK_ROW)
                elif remote_status[ci] in ("done", "backing"):
                    # Prepared-at-selection but already written/ambiguous at read.
                    window_codes.append(STATUS_CHANGED_DURING_WINDOW)
        try:
            snap_ts = datetime.fromisoformat(snap.snapshot_ts_utc)
            window_secs = (db_read_ts - snap_ts).total_seconds()
        except Exception:
            window_secs = None
        if window_secs is not None and window_secs > self._stale:
            window_codes.append(STALE_OBSERVATION)

        members = [
            SnapshotMember(chunk_index=ci, prepared_bytes=pb,
                           pack_identity=pk, file_count=None,
                           pack_fingerprint=None)
            for ci, pb, pk in zip(snap.chunk_ids, snap.prepared_bytes,
                                  snap.pack_identities)]
        rgs = ReadyGroupSnapshot(
            session_id=snap.session_id,
            ready_queue_generation=snap.ready_queue_generation,
            selection_reason=snap.selection_reason, members=members,
            expected_tape=snap.expected_tape, scan_complete=snap.scan_complete,
            producer_state=snap.producer_state,
            staging_pressure=snap.staging_pressure, safe_stop=snap.safe_stop,
            aggregate_prepared_bytes=snap.total_prepared_bytes)
        record = self._observer.observe_group(rgs, remote_status)
        record.update({
            "group_correlation_id": snap.correlation_id,
            "writer_path": snap.writer_path,
            "scheduler_selection_timestamp": snap.snapshot_ts_utc,
            "database_read_timestamp": db_read_ts.isoformat(),
            "consistency_window_seconds": window_secs,
            "database_query_seconds": round(db_secs, 6),
            "transaction_isolation": iso,
            "db_error": db_err,
            "consistency_window_codes": sorted(set(window_codes)),
            "record_kind": "observation",
        })
        self._snapshots[snap.correlation_id] = {
            "chunk_ids": list(snap.chunk_ids)}
        self._emit(record)

    def _process_outcome(self, out):
        record = {
            "record_kind": "outcome",
            "group_correlation_id": out.correlation_id,
            "session_id": out.session_id,
            "selected_chunk_ids": list(out.selected_chunk_ids),
            "started_chunk_ids": list(out.started_chunk_ids),
            "completed_chunk_ids": list(out.completed_chunk_ids),
            "failing_chunk": out.failing_chunk,
            "stop_reason": out.stop_reason,
            "group_start_ts": out.group_start_ts,
            "group_finish_ts": out.group_finish_ts,
            "group_duration_seconds": round(
                out.group_finish_ts - out.group_start_ts, 3),
            "ownership_acquisitions": out.ownership_acquisitions,
            "readiness_checks": out.readiness_checks,
            "cartridge_verifications": out.cartridge_verifications,
            "writer_invocations": out.writer_invocations,
            "observer_version": OBSERVER_VERSION,
        }
        self._snapshots.pop(out.correlation_id, None)
        self._emit(record)

    def _emit(self, record):
        # Bounded append-only JSONL. A log/disk failure is contained.
        try:
            self._log.info(json.dumps(record, default=str))
        except Exception:
            get_logger().exception("sealed_batch_observation_log_error")

    # -- shutdown (bounded) ----------------------------------------------
    def shutdown(self, timeout=None):
        if self._thread is not None:
            self._stop.set()
            t = (self._cfg.shutdown_timeout_seconds if timeout is None
                 else timeout)
            self._thread.join(timeout=max(0.0, t))
            # Never wait indefinitely; leftover items are simply dropped.
        self.close()

    def close(self):
        """Release the rotating log file handle (so the file can be rotated by
        the OS or removed by a test). Safe to call repeatedly."""
        for h in list(getattr(self._log, "handlers", [])):
            try:
                h.close()
                self._log.removeHandler(h)
            except Exception:
                pass

    def counters(self):
        return {"submitted": self.submitted, "completed": self.completed,
                "failed": self.failed, "dropped": self.dropped,
                "queue_size": self._q.qsize()}


def _build_record_logger(log_path, max_bytes, backups):
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    logger = logging.getLogger(f"sealed_batch_observation.{id(log_path)}")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    for h in list(logger.handlers):
        logger.removeHandler(h)
    handler = RotatingFileHandler(
        log_path, maxBytes=max_bytes, backupCount=backups, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    return logger


def maybe_build_observation_worker(cfg, db_dsn):
    """Construct the observation worker ONLY when observation mode is enabled.

    Returns ``None`` when the flag is false (the default): no queue, no thread,
    no DB, no log, no observer construction. An initialization failure disables
    observation only (returns ``None``); it never propagates to the writer.
    """
    if not getattr(cfg, "sealed_tape_write_batches_observation_enabled", False):
        return None
    try:
        obs_cfg = ObservationConfig.from_cfg(cfg)
        log_path = _validate_log_path(obs_cfg.log_path, cfg)
        record_logger = _build_record_logger(
            log_path, obs_cfg.log_max_bytes, obs_cfg.log_backups)
        reader = _StatusReader(db_dsn, obs_cfg.statement_timeout_seconds)
        worker = ObservationWorker(
            obs_cfg, SealedBatchObserver(), reader, record_logger,
            stale_window_seconds=obs_cfg.stale_window_seconds)
        return worker.start()
    except Exception:
        get_logger().exception(
            "sealed_batch_observation_init_failed: observation disabled, "
            "writer unaffected")
        return None
