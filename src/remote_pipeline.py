"""RemotePipelineCoordinator: ONE scheduling loop for every remote session.

Plan 1, Task 1.3. Until now there were two loops — ``_run_streaming_session``
for a session still scanning, and ``_run_session`` for a scan-complete resume —
with two different queues, two producer shapes and two writer paths (the resume
path wrote a *group of one* per ownership period). Two loops meant two places
for a stop, a cancellation or a preserved pack to be handled differently, and a
scan-complete resume never got the multi-chunk write group Phase 4 exists for.

This module replaces both with one coordinator over one
:class:`~src.ready_queue.ReadyQueue`, one
:class:`~src.remote_staging.RemoteChunkStager`, one
:class:`~src.remote_writer.RemoteChunkWriter`, and an **optional**
:class:`~src.scan_frontier.FrontierScanCoordinator`. A scan-complete session is
simply the case where the scanner is ``None``.

Fairness — the reason work selection changed
--------------------------------------------
The old scanner thread pushed **every** resumed pending chunk into a bounded
hand-off queue *before* it explored anything. With a large backlog and a small
queue, renewed exploration could be postponed for as long as the stager needed
to drain — the source was not being looked at while old work trickled through.

The fix is not a bigger queue. Work selection is now **authoritative**: the
stager asks PostgreSQL which chunks are still ``pending`` and takes them in
index order, so the backlog never has to pass through the scanner at all. The
scanner is admitted to publish while the count of sealed-but-unstaged chunks is
under a configured limit, and that count is re-derived from chunk status on
every scheduling decision rather than tracked in a second counter that can
drift. Old pending staging therefore bounds how far ahead the scanner may run;
it can never stop it from running.

Durable per-chunk claims are deliberately NOT here — they arrive in Task 3.1
once migration 014 provides the owner/lease columns. Until then the only
in-process guard is the taken-set below, and the cross-process guard remains the
archiver advisory lock.
"""
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import List, Optional

from .exit_codes import ExitCode, StopResult, REASON_USER_REQUESTED_STOP
from .logsetup import get_logger
from .ready_queue import ReadyItem
from .runtime import CANCEL, _status

#: How long the stager waits before re-asking for work while the scanner is
#: still running. Short enough that a freshly sealed chunk is picked up
#: promptly, long enough that an idle pipeline does not busy-poll PostgreSQL.
DEFAULT_POLL_SECONDS = 1.0


@dataclass
class PipelineOutcome:
    """What the single loop did, for the caller's terminal classification."""

    completed_chunks: int = 0
    #: The stop that ended a write group, if any.
    stop_block: Optional[StopResult] = None
    #: True when the run ended for a reason other than a user stop.
    failed: bool = False
    scanner_finished: bool = False
    groups_written: List[tuple] = field(default_factory=list)
    #: Set when a bounded run ended itself at a group boundary.
    stopped_reason: Optional[str] = None


class RemotePipelineCoordinator:
    """Scanner (optional) -> stager -> ready queue -> finite write group.

    ``host`` is the :class:`~src.remote_orchestrator.RemoteOrchestrator` façade:
    it owns configuration, session state, the stop record and the staging
    capacity gate, and every hook routes back through it so an override still
    wins.
    """

    def __init__(self, *, host, session_id, tape_label, ready_q, stop_event,
                 metrics, scan_coordinator=None, backlog_limit=64,
                 poll_seconds=DEFAULT_POLL_SECONDS, observation_worker=None,
                 writer_path="pipeline", scan_complete=False,
                 max_write_groups=0):
        self.host = host
        self.session_id = session_id
        self.tape_label = tape_label
        self.ready_q = ready_q
        self.stop_event = stop_event
        self.metrics = metrics
        self.scan_coordinator = scan_coordinator
        self.backlog_limit = max(1, int(backlog_limit))
        self.poll_seconds = poll_seconds
        self.observation_worker = observation_worker
        self.writer_path = writer_path
        self.scan_complete = scan_complete
        #: Stop after this many successfully committed write groups; 0 means
        #: drain normally. A bounded run uses this rather than a mid-run kill,
        #: so the stop always lands on a chunk boundary.
        self.max_write_groups = max(0, int(max_write_groups or 0))
        #: Write-only resume: drain already-planned, already-sealed legacy_db
        #: chunks and do no discovery. Set by the orchestrator, which also
        #: declines to construct a scan coordinator at all.
        self.write_only = False

        #: Chunks this process has already picked up for staging. Purely
        #: in-process de-duplication on top of authoritative status — never a
        #: substitute for it, and never persisted (Task 3.1 adds real claims).
        self._taken = set()
        self._taken_lock = threading.Lock()
        self._scanner_done = threading.Event()
        self._scanner_thread = None
        self._stager_thread = None
        self.outcome = PipelineOutcome()

    # ------------------------------------------------------------------
    # Work selection — authoritative, in chunk-index order
    # ------------------------------------------------------------------
    def _pending_chunk_indices(self):
        """Chunks that are sealed but not yet staged, from chunk status.

        ``pending`` is exactly "planned, and no worker has started fetching
        it": ``RemoteChunkStager`` moves a chunk to ``fetching`` as its first
        act. Deriving the backlog from this instead of a counter means a
        restart, a crash or a second process cannot leave the figure wrong.
        """
        db = self.host.db
        reader = getattr(db, 'get_chunks_with_status', None)
        if reader is not None:
            return list(reader(self.session_id, 'pending'))
        # A database double without the status reader still schedules work,
        # it just cannot distinguish staged-in-flight from sealed.
        return list(db.get_pending_chunks(self.session_id))

    def sealed_but_unstaged(self):
        """The backlog figure the publication gate is measured against."""
        try:
            pending = self._pending_chunk_indices()
        except Exception:
            get_logger().warning(
                "could not read the sealed-but-unstaged backlog; holding "
                "publication until it can be read", exc_info=True)
            # Fail towards NOT publishing more: an unknown backlog must not be
            # treated as an empty one.
            return self.backlog_limit
        with self._taken_lock:
            return sum(1 for ci in pending if ci not in self._taken)

    def _write_only_admits(self, chunk_index):
        """In write-only resume, a chunk must already be sealed legacy_db work.

        Write-only exists to drain chunks planned by an earlier run. It must
        never be a back door into writing something whose membership is not
        already frozen and persisted, so a chunk is admitted only when its
        durable state says so. Anything unreadable, unsealed, or
        manifest-planned is refused and left alone: a manifest chunk's
        membership lives in a local artifact, which is the scanner's business,
        not this mode's.
        """
        reader = getattr(self.host.db, 'get_remote_chunk', None)
        if not callable(reader):
            return True
        try:
            chunk = reader(self.session_id, chunk_index)
        except Exception:
            get_logger().warning(
                "write_only_chunk_unreadable: chunk=%s -- refusing",
                chunk_index, exc_info=True)
            return False
        if not chunk:
            return False
        if chunk.get('membership_state') != 'sealed':
            get_logger().info(
                "write_only_refused_unsealed: chunk=%s membership_state=%r",
                chunk_index, chunk.get('membership_state'))
            return False
        plan_source = chunk.get('plan_source')
        # A pre-015/018 row may predate the column entirely; absent means the
        # only planning source that existed then, which is the legacy one.
        if plan_source is not None and plan_source != 'legacy_db':
            get_logger().info(
                "write_only_refused_plan_source: chunk=%s plan_source=%r",
                chunk_index, plan_source)
            return False
        return True

    def next_chunk_to_stage(self):
        """The lowest-indexed sealed chunk this process has not taken yet."""
        try:
            pending = sorted(self._pending_chunk_indices())
        except Exception:
            get_logger().warning("could not read pending chunks", exc_info=True)
            return None
        with self._taken_lock:
            for chunk_index in pending:
                if chunk_index in self._taken:
                    continue
                if self.write_only and not self._write_only_admits(chunk_index):
                    # Mark it taken so the loop does not re-check it forever;
                    # it is skipped for this run, not altered in any way.
                    self._taken.add(chunk_index)
                    continue
                self._taken.add(chunk_index)
                return chunk_index
        return None

    # ------------------------------------------------------------------
    # Scanner admission
    # ------------------------------------------------------------------
    def publication_gate(self):
        """Block while sealed-but-unstaged work is at the configured limit.

        Returns False only when the run is stopping. This is a *bound*, not a
        barrier: as soon as the stager takes one chunk the scanner is admitted
        again, so exploration resumes without the whole backlog draining first.
        """
        waited = False
        while not (CANCEL.is_set() or self.stop_event.is_set()):
            if self.sealed_but_unstaged() < self.backlog_limit:
                if waited:
                    get_logger().info(
                        "scan_publication_resumed: backlog fell below %d",
                        self.backlog_limit)
                return True
            if not waited:
                waited = True
                get_logger().info(
                    "scan_publication_paused: %d sealed chunk(s) still "
                    "unstaged (limit %d); exploration will resume as the "
                    "stager drains", self.sealed_but_unstaged(),
                    self.backlog_limit)
            self.stop_event.wait(self.poll_seconds)
        return False

    # ------------------------------------------------------------------
    # Threads
    # ------------------------------------------------------------------
    def _run_scanner(self):
        self.scan_coordinator.run()

    def _stopping(self):
        return CANCEL.is_set() or self.stop_event.is_set()

    def _more_work_possible(self):
        """True while the scanner might still seal another chunk."""
        return (self.scan_coordinator is not None
                and not self._scanner_done.is_set())

    def _plan_source_for(self, chunk_index):
        """The membership reader for one chunk (Plan 3, Task 1.3).

        Resolved here rather than through a host hook so that every caller -
        production orchestrator and test double alike - gets the same selection
        rule without having to reimplement it.
        """
        from .plan_source import plan_source_for_chunk

        host = self.host
        cfg = getattr(host, "cfg", None)
        return plan_source_for_chunk(
            host.db, self.session_id, chunk_index,
            archive_root=getattr(cfg, "local_manifest_archive_root", None),
            exception_states=getattr(host, "_chunk_exception_states", None))

    def _run_stager(self):
        """Take sealed chunks in index order, stage them, enqueue them."""
        host = self.host
        try:
            while not self._stopping():
                chunk_index = self.next_chunk_to_stage()
                if chunk_index is None:
                    if not self._more_work_possible():
                        break
                    # Nothing sealed yet, but the scanner is still exploring.
                    # A bounded wait keeps this responsive to a stop.
                    self.stop_event.wait(self.poll_seconds)
                    continue

                # Plan 3, Task 1.3: membership arrives through ONE typed
                # stream. Which adapter answers is decided by the chunk's
                # persisted plan_source and nothing else, so a legacy chunk and
                # a manifest chunk in the same session are both just "a chunk".
                plan_source, chunk_ref = self._plan_source_for(chunk_index)
                summary = plan_source.summary(chunk_ref)
                planned_bytes, _, planned_files = summary.as_tuple()
                host._validate_chunk_file_limit(
                    self.session_id, chunk_index, planned_files)
                chunk_files = plan_source.iter_chunk_entries(chunk_ref)
                host._await_staging_capacity(
                    planned_bytes, planned_files, self.stop_event,
                    ready_q=self.ready_q, session_id=self.session_id,
                    chunk_index=chunk_index, chunk_files=chunk_files)
                if self._stopping():
                    break

                desc = host._stage_chunk(
                    self.session_id, chunk_index, chunk_files)
                if desc is None:
                    if not CANCEL.is_set():
                        host._producer_err = (
                            f"chunk {chunk_index + 1} could not be staged")
                        host._record_fetch_failure_stop(
                            self.session_id, chunk_index)
                    self.stop_event.set()
                    break
                self.metrics.mark_first_staged_chunk()
                desc.scan_stats = self.metrics.snapshot()

                item = ReadyItem(
                    chunk_index=chunk_index,
                    pack_dir=desc.pack_dir,
                    prepared_bytes=int(getattr(desc, 'staged_bytes', 0) or 0),
                    file_count=planned_files,
                    desc=desc)
                # Blocks (without touching LTFS) at the queue's byte/count
                # ceiling; that backpressure is what keeps staging bounded.
                if not self.ready_q.put(item, stop_event=self.stop_event):
                    host._discard_desc(desc)
                    break
        except Exception as exc:
            get_logger().exception("chunk stager failed")
            host._producer_err = str(exc)
            host._record_stop(StopResult(
                exit_code=ExitCode.TRANSIENT_RESUMABLE,
                reason=_boundary_reason(), resumable=True, source="stager",
                session_id=self.session_id, detailed_reason=str(exc)))
            self.stop_event.set()
        finally:
            # Translate WHY the producer stopped into ready-queue completion
            # state: only a genuine end-of-work may force a final partial group.
            host._signal_producer_completion(self.ready_q, self.stop_event)

    # ------------------------------------------------------------------
    # The single writer loop
    # ------------------------------------------------------------------
    def run(self):
        """Start the producers and drain finite write groups until done."""
        host = self.host
        if self.write_only and self.scan_coordinator is not None:
            # Belt and braces: the orchestrator does not build a coordinator in
            # this mode, so reaching here means the two disagree. Refuse rather
            # than start discovery a write-only run promised not to do.
            raise RuntimeError(
                "[PIPELINE] write-only resume was given a scan coordinator; "
                "refusing to start the scanner")
        if self.scan_coordinator is not None:
            self._scanner_thread = threading.Thread(
                target=self._run_scanner, name='streaming-scanner', daemon=True)
            self._scanner_thread.start()
        else:
            # No scanner: every chunk that will ever exist is already sealed.
            self._scanner_done.set()
        self._stager_thread = threading.Thread(
            target=self._run_stager, name='pipeline-stager', daemon=True)
        self._stager_thread.start()

        outcome = self.outcome
        try:
            while True:
                if self._stopping():
                    break
                items, reason = self.ready_q.wait_for_group(
                    stop_event=self.stop_event)
                if not items:
                    if reason in ("producer_closed_empty", "stop_requested"):
                        break
                    continue
                self.metrics.mark_first_writer_group()
                group_bytes = sum(i.prepared_bytes for i in items)
                _status('TAPE', f"Write group: {len(items)} chunk(s), "
                                f"{group_bytes / 1024**3:.2f} GiB ({reason})")
                get_logger().info(
                    "ready_group_selected: chunks=%d bytes=%d reason=%s "
                    "ready_after=%d", len(items), group_bytes, reason,
                    self.ready_q.ready_chunks)
                outcome.groups_written.append(
                    tuple(i.chunk_index for i in items))

                written_before = outcome.completed_chunks
                correlation_id, counts0, group_start = self._observe_start(
                    items, reason)

                stop_block = host._write_chunk_group(
                    self.session_id, [i.desc for i in items], self.tape_label,
                    False, self.stop_event)

                self._observe_finish(items, stop_block, correlation_id,
                                     counts0, group_start)

                if stop_block is None:
                    for item in items:
                        self.ready_q.mark_written(item)
                    outcome.completed_chunks += len(items)
                    # A bounded run stops after N successful groups instead of
                    # draining every ready chunk.  The runbook requires that
                    # the next group never starts automatically, and the stop
                    # lands here -- AFTER a group committed and BEFORE the next
                    # is selected -- which is exactly a chunk boundary, so LTFS
                    # has synced and the session stays resumable.
                    if (self.max_write_groups
                            and len(outcome.groups_written)
                            >= self.max_write_groups):
                        _status('TAPE',
                                f"Bounded run: {self.max_write_groups} write "
                                "group(s) completed; stopping instead of "
                                "starting another.")
                        get_logger().info(
                            "bounded_run_group_limit_reached: groups=%d",
                            len(outcome.groups_written))
                        outcome.stopped_reason = "max_write_groups_reached"
                        break
                    continue

                self._settle_aborted_group(items, stop_block)
                get_logger().info(
                    "ready_group_aborted: reason=%s failing_chunk=%s "
                    "committed_in_group=%d", stop_block.reason,
                    None if stop_block.chunk_index is None
                    else stop_block.chunk_index + 1,
                    outcome.completed_chunks - written_before)
                outcome.stop_block = stop_block
                outcome.failed = stop_block.exit_code != ExitCode.USER_STOP
                self.stop_event.set()
                break
        finally:
            self.shutdown()
        outcome.scanner_finished = self._scanner_done.is_set()
        return outcome

    def _settle_aborted_group(self, items, stop_block):
        """Per-chunk failure isolation, unchanged from the streaming loop."""
        failing = stop_block.chunk_index
        for item in items:
            if failing is not None and item.chunk_index < failing:
                self.ready_q.mark_written(item)
                self.outcome.completed_chunks += 1
            elif failing is not None and item.chunk_index == failing:
                self.ready_q.mark_failed(item)
                if stop_block.preserve_pack:
                    self.host._preserve_desc(
                        self.session_id, item.desc, stop_block.reason)
                elif not CANCEL.is_set():
                    self.host._discard_desc(item.desc)
            else:
                # Never started: its pack stays reusable, untouched.
                self.ready_q.mark_preserved(item)
                self.host._preserve_desc(
                    self.session_id, item.desc, "not started in aborted group")

    # ------------------------------------------------------------------
    # Non-authoritative observation
    # ------------------------------------------------------------------
    def _observe_start(self, items, reason):
        if self.observation_worker is None:
            return None, None, None
        correlation_id = uuid.uuid4().hex
        self.host._obs_correlation_id = correlation_id
        self.observation_worker.submit_snapshot(
            self.host._capture_write_group_snapshot(
                correlation_id, self.session_id, [i.desc for i in items],
                reason, self.tape_label, self.ready_q.groups_started,
                bool(self.scan_complete),
                ('failed' if self.host._producer_err else
                 ('closed' if self.ready_q.closed else 'active')),
                CANCEL.is_set() or self.stop_event.is_set(),
                self.writer_path))
        counts0 = (self.host._ownership_acquisitions,
                   self.host._readiness_checks,
                   self.host._cartridge_verifications)
        return correlation_id, counts0, time.time()

    def _observe_finish(self, items, stop_block, correlation_id, counts0,
                        group_start):
        if self.observation_worker is None:
            return
        self.observation_worker.submit_outcome(
            self.host._capture_group_outcome(
                correlation_id, self.session_id, [i.desc for i in items],
                stop_block, counts0, group_start, time.time()))

    # ------------------------------------------------------------------
    def shutdown(self):
        """Stop the producers and preserve anything still prepared.

        Shutdown-only: it records no stop reason. The loop above already
        recorded one (or finished cleanly), so nothing is inferred here.
        """
        self.stop_event.set()
        for leftover in self.ready_q.drain_ready():
            self.host._preserve_desc(
                self.session_id, leftover.desc, "queued at shutdown")
        get_logger().info("ready_queue_final_metrics: %s",
                          self.ready_q.metrics())
        for thread in (self._scanner_thread, self._stager_thread):
            if thread is not None:
                thread.join(timeout=15)

    def note_scanner_finished(self):
        self._scanner_done.set()


def _boundary_reason():
    # Imported lazily to keep this module's import graph shallow.
    from .exit_codes import REASON_STOPPED_AT_CHUNK_BOUNDARY
    return REASON_STOPPED_AT_CHUNK_BOUNDARY


__all__ = ["RemotePipelineCoordinator", "PipelineOutcome",
           "DEFAULT_POLL_SECONDS", "REASON_USER_REQUESTED_STOP"]
