"""RemoteChunkWriter: the finite write group — the ONLY path to the tape.

Plan 1, Task 1.2. Moved out of ``RemoteOrchestrator`` unchanged, so that the
one place a tape write can start is a single, small, readable module.

The invariants this module exists to protect (see
``docs/incidents/000-no-physical-intervention-policy.md``):

* **One ownership period per group, not per chunk.** Ownership and the safety
  gate run once; between chunks there is deliberately no readiness probe, no
  cartridge read and no filesystem access on the mount.
* **The group is finite at acquisition time.** Ownership is never held while
  waiting for the producer to prepare more work.
* **Failure isolation is per chunk.** An earlier chunk that committed stays
  committed; a chunk that never started keeps its pack.
* **``backing`` means the physical write has begun, and is never cleared here.**
  Any failure at or after that point is physically ambiguous and must stay
  ambiguous — no retry, no eject, no remount, no ``ltfsck``.

The actual copy and catalog commit stay delegated to
:class:`~src.backup.LTOBackup`; this module decides *whether* and *when*.

``host`` is the :class:`~src.remote_orchestrator.RemoteOrchestrator` façade;
every cross-method call routes back through it so an injected hook still wins.
"""
import time

from .backup import LTOBackup, _NoEjectBackup
from .constants import (tape_budget_bytes, tape_is_full,
                        tape_status_reason_suffix)
from .exit_codes import (
    ExitCode, StopResult, REASON_AMBIGUOUS_BACKING_CHUNK,
    REASON_LTFS_OWNERSHIP_UNAVAILABLE, REASON_TAPE_WRITE_FAILED,
    REASON_UNEXPECTED_TAPE_OR_DB_STATE, REASON_USER_REQUESTED_STOP)
from .logsetup import get_logger
from .pipeline_types import ChunkStatus
from .ltfs_ownership import LtfsOwnershipError, writer_timeout_seconds
from .reporting import _write_source_missing_only_log
from .runtime import CANCEL, _acquire_tape_io_lock, _release_tape_io_lock, _status
from .telegram_notify import send_best_effort


class RemoteChunkWriter:
    """Write a finite group of prepared chunks under ONE LTFS ownership period."""

    def __init__(self, host):
        #: The RemoteOrchestrator façade that owns the gate, stop state and DB.
        self.host = host

    def _write_chunk_group(self, session_id, descs, tape_label, eject_after,
                           stop_pipeline):
        """Write a FINITE group of prepared chunks under ONE ownership period.

        Phase 4. Ownership and the safety gate run once for the whole group;
        between chunks there is deliberately no readiness probe, no cartridge
        read, no ``LtfsCmdDrives.exe`` and no filesystem access — the Phase 1
        readiness cache stays valid precisely because Phase 3 ownership is never
        released in between (a nested acquire/release does not invalidate).

        The group is fixed at acquisition time. Chunks prepared later wait for
        the next group: ownership is never held waiting for the producer.

        Failure isolation is unchanged and per-chunk. An earlier chunk that
        committed stays committed if a later one fails; a chunk that never
        started keeps its pack and is returned to the caller as preserved.
        """
        descs = list(descs)
        if not descs:
            return None

        # skip_tape chunks involve no tape at all — handle them outside
        # ownership so an all-skip group never touches LTFS.
        tape_descs = []
        for desc in descs:
            if desc.skip_tape:
                block = self.host._write_skip_tape_chunk(
                    session_id, desc, tape_label, eject_after)
                if block is not None:
                    return block
            else:
                tape_descs.append(desc)
        if not tape_descs:
            return None

        group_started = time.time()
        first = tape_descs[0]
        group_bytes = sum(int(getattr(d, 'staged_bytes', 0) or 0)
                          for d in tape_descs)
        get_logger().info(
            "tape_write_group_start: chunks=%d indices=%s bytes=%d "
            "group_correlation_id=%s",
            len(tape_descs), [d.chunk_index + 1 for d in tape_descs],
            group_bytes, getattr(self.host, '_obs_correlation_id', None))

        try:
            _acquire_tape_io_lock(
                f"authorize+write remote chunk group of {len(tape_descs)}",
                timeout=writer_timeout_seconds())
        except LtfsOwnershipError as e:
            msg = (f"could not acquire exclusive LTFS ownership for a group of "
                   f"{len(tape_descs)} chunk(s) ({e.kind}): {e}")
            print(f"\n[TAPE] {msg}")
            get_logger().error("ltfs_ownership_stop: kind=%s chunks=%s detail=%s",
                               e.kind, [d.chunk_index + 1 for d in tape_descs], e)
            send_best_effort(self.host.notifier, f"[PIPELINE] {msg}")
            return self.host._record_stop(StopResult(
                exit_code=ExitCode.SAFETY_BLOCK,
                reason=REASON_LTFS_OWNERSHIP_UNAVAILABLE, resumable=True,
                source="write", session_id=session_id,
                chunk_index=first.chunk_index, preserve_pack=True,
                detailed_reason=msg))

        self.host._ownership_acquisitions += 1
        try:
            # ONE gate for the whole group: mount mode, media health, cartridge
            # identity and pending-reboot. Nothing below re-runs it.
            recorded = self.host._get_recorded_stop()
            if recorded is not None:
                return recorded
            gate_block = self.host._pre_write_safety_gate(
                session_id, first, tape_label, stop_pipeline)
            if gate_block is not None:
                return gate_block
            self.host._readiness_checks += 1
            self.host._cartridge_verifications += 1

            for desc in tape_descs:
                # Cheap, non-LTFS stop checks between chunks. A staged reboot,
                # a recorded stop or a cancel must still end the group at a
                # chunk boundary — but none of these touches the drive.
                recorded = self.host._get_recorded_stop()
                if recorded is not None:
                    return recorded
                if CANCEL.is_set() or stop_pipeline.is_set():
                    return self.host._record_stop(StopResult(
                        exit_code=ExitCode.USER_STOP,
                        reason=REASON_USER_REQUESTED_STOP, resumable=True,
                        source="write", session_id=session_id,
                        chunk_index=desc.chunk_index, preserve_pack=True,
                        detailed_reason="stop requested between group chunks"))
                block = self.host._write_one_chunk_owned(
                    session_id, desc, tape_label, eject_after)
                if block is not None:
                    return block
            return None
        finally:
            _release_tape_io_lock()
            get_logger().info(
                "tape_write_group_end: chunks=%d bytes=%d duration_s=%.1f "
                "group_correlation_id=%s",
                len(tape_descs), group_bytes, time.time() - group_started,
                getattr(self.host, '_obs_correlation_id', None))

    def _write_skip_tape_chunk(self, session_id, desc, tape_label, eject_after):
        """A chunk whose files were all source-missing: no tape I/O at all."""
        chunk_index = desc.chunk_index
        self.host._consumer_chunk = chunk_index
        log_path = _write_source_missing_only_log(
            self.host.cfg.backup_log_dir, session_id, chunk_index, tape_label,
            desc.source_missing_files or [],
            source_host=self.host.remote_host.split('.', 1)[0],
            source_path=self.host.remote_session_path, notifier=self.host.notifier)
        self.host.db.update_chunk_status(session_id, chunk_index,
                ChunkStatus.DONE.value)
        self.host._cleanup_dir(desc.fetch_dir)
        self.host._cleanup_dir(desc.pack_dir)
        if log_path:
            print(f"[REMOTE] Source-missing CSV summary: {log_path}")
        # Task 1.4: a skip-tape chunk involves no tape at all, so it certainly
        # must not end in a mechanical eject. `eject_after` is accepted for
        # signature compatibility and deliberately ignored.
        if eject_after:
            get_logger().warning(
                "skip_tape_eject_refused: chunk %s wrote nothing to tape; "
                "refusing to eject", chunk_index + 1)
        return None

    def _write_one_chunk_owned(self, session_id, desc, tape_label, eject_after):
        """Write ONE chunk. Ownership is already held and the gate has passed."""
        chunk_index = desc.chunk_index
        self.host._consumer_chunk = chunk_index
        pack_dir = desc.pack_dir

        # Ownership is already held by the group and the gate has already
        # passed for this group: no readiness probe, cartridge read or
        # LtfsCmdDrives call happens here or between chunks.
        try:
            # (d) Fits-tape. A miss is re-fetchable — discard, do not preserve.
            _, planned_bytes, _ = self.host.db.get_chunk_size_summary(
                session_id, chunk_index).get(chunk_index, (0, 0, 0))
            if not self.host._ensure_remote_chunk_fits_tape(
                    tape_label, planned_bytes, chunk_index):
                self.host.db.update_chunk_status(
                    session_id, chunk_index,
                ChunkStatus.BACKUP_FAILED.value)
                return self.host._record_stop(StopResult(
                    exit_code=ExitCode.SAFETY_BLOCK,
                    reason=REASON_UNEXPECTED_TAPE_OR_DB_STATE, resumable=False,
                    source="write", session_id=session_id,
                    chunk_index=chunk_index, preserve_pack=False,
                    detailed_reason="chunk does not fit the mounted tape"))

            # (e) Launch the external writer. The chunk is moved to 'backing'
            #     ONLY when the writer actually starts (via on_write_start), so
            #     'backing' means exactly "the tape write has physically begun".
            #     A failure BEFORE that (drive not ready, bad metadata) never set
            #     'backing', so it is safely re-fetchable; a failure AT OR AFTER
            #     it is physically ambiguous and must stay 'backing'.
            backup_cls = LTOBackup if eject_after else _NoEjectBackup
            governor = getattr(self.host, 'governor', None)
            tape_pending = (
                governor.mark_tape_write_pending() if governor else None)
            write_started = {'v': False}

            def _mark_write_started():
                write_started['v'] = True
                self.host.db.update_chunk_status(session_id, chunk_index,
                ChunkStatus.BACKING.value)

            try:
                if tape_pending:
                    with tape_pending:
                        self.host._backup_writer(backup_cls).run(
                            source=pack_dir,
                            tape_drive=self.host.cfg.lto_drive,
                            tape_label=tape_label,
                            packer_metadata=desc.metadata,
                            stage_stats=desc,
                            source_host=self.host.remote_host.split('.', 1)[0],
                            skipped_tracker=self.host.skipped_tracker,
                            remote_session_id=session_id,
                            remote_chunk_index=chunk_index,
                            on_write_start=_mark_write_started,
                        )
                else:
                    self.host._backup_writer(backup_cls).run(
                        source=pack_dir,
                        tape_drive=self.host.cfg.lto_drive,
                        tape_label=tape_label,
                        packer_metadata=desc.metadata,
                        stage_stats=desc,
                        source_host=self.host.remote_host.split('.', 1)[0],
                        skipped_tracker=self.host.skipped_tracker,
                        remote_session_id=session_id,
                        remote_chunk_index=chunk_index,
                        on_write_start=_mark_write_started,
                    )
            except Exception as e:
                if write_started['v']:
                    # ANY failure once the write has started — cancel or not,
                    # exception, non-zero robocopy, killed subprocess, LTFS/I/O
                    # error, DB-commit failure — is PHYSICALLY AMBIGUOUS: data
                    # may be partly on tape. Leave the chunk 'backing' (never
                    # done / backup_failed / pending). This is 20/ambiguous even
                    # when CANCEL is set (a forced kill): physical ambiguity
                    # ESCALATES over a user stop, so the recorded reason wins over
                    # any 40/user_requested_stop. The resume precheck returns
                    # 20/ambiguous_backing_chunk; staging is preserved.
                    get_logger().warning(
                        "tape write for chunk %s failed AFTER it started (%s); "
                        "left 'backing' (ambiguous) for resume",
                        chunk_index + 1, e)
                    print(f"[REMOTE] Backup error after write started: {e}")
                    return self.host._record_stop(StopResult(
                        exit_code=ExitCode.SAFETY_BLOCK,
                        reason=REASON_AMBIGUOUS_BACKING_CHUNK, resumable=False,
                        source="write", session_id=session_id,
                        chunk_index=chunk_index, preserve_pack=True,
                        detailed_reason=f"tape write failed after starting: {e}"),
                        escalate=True)
                # Failure BEFORE the write started: nothing reached the tape, so
                # the chunk is safely re-fetchable. 'backing' was never set.
                if CANCEL.is_set():
                    return self.host._record_stop(StopResult(
                        exit_code=ExitCode.USER_STOP,
                        reason=REASON_USER_REQUESTED_STOP, resumable=True,
                        source="write", session_id=session_id,
                        chunk_index=chunk_index, preserve_pack=True,
                        detailed_reason="cancelled before the write started"))
                print(f"[REMOTE] Backup error (before write started): {e}")
                self.host.db.update_chunk_status(
                    session_id, chunk_index,
                ChunkStatus.BACKUP_FAILED.value)
                return self.host._record_stop(StopResult(
                    exit_code=ExitCode.TRANSIENT_RESUMABLE,
                    reason=REASON_TAPE_WRITE_FAILED, resumable=True,
                    source="write", session_id=session_id,
                    chunk_index=chunk_index, preserve_pack=False,
                    detailed_reason=str(e)))
        except BaseException:
            raise

        # --- The physical write completed -----------------------------------
        # Ownership stays held by the GROUP across this commit (Phase 4): the
        # commit is PostgreSQL plus local staging cleanup and touches no tape,
        # and releasing here would invalidate the readiness cache and force a
        # re-verification before the next chunk — the exact per-chunk probe this
        # phase removes.
        # The write finished normally (a cooperative Ctrl+C lets the protected
        # robocopy complete), so ALWAYS run the normal commit: mark the chunk
        # 'done' and flush its staging. A cooperative cancel does NOT leave this
        # chunk ambiguous — it is committed.
        self.host.db.update_chunk_status(session_id, chunk_index,
                ChunkStatus.DONE.value)

        # --- FLUSH staged files for this chunk ---
        _status('REMOTE', f"Flushing staged files for chunk {chunk_index + 1}...")
        self.host._cleanup_dir(desc.fetch_dir)   # already removed after packing
        self.host._cleanup_dir(pack_dir)
        with self.host._staged_lock:
            self.host._staged_bytes = max(0, self.host._staged_bytes - desc.staged_bytes)

        # The chunk is committed. If a cooperative cancel is active, record the
        # user-stop NOW — on the final chunk there is no next gate invocation to
        # record it, and it must remain authoritative at process exit rather than
        # letting the terminal fall through to 0/completed. We still return None
        # so this chunk counts as done; the loop then stops before any next one.
        if CANCEL.is_set():
            self.host._record_stop(StopResult(
                exit_code=ExitCode.USER_STOP, reason=REASON_USER_REQUESTED_STOP,
                resumable=True, source="write", session_id=session_id,
                chunk_index=chunk_index,
                detailed_reason="cancel requested during the final write; "
                                "chunk committed, stopping before any next"))
        return None

    def _ensure_remote_chunk_fits_tape(self, tape_label, planned_bytes,
                                       chunk_index):
        tape = self.host.db.get_tape(tape_label)
        if not tape:
            print(f"[DB] Tape '{tape_label}' is not registered.")
            return False
        if tape_is_full(tape.get('status')):
            print(f"[TAPE] '{tape_label}' is marked FULL in the database"
                  f"{tape_status_reason_suffix(tape)}; refusing to write "
                  f"remote chunk {chunk_index + 1}.")
            return False
        used_bytes = self.host.db.recalculate_tape_used_space(tape_label)
        _, available_bytes = tape_budget_bytes(
            tape['total_capacity'], used_bytes, status=tape.get('status'))
        if planned_bytes > available_bytes:
            print(f"[TAPE] Remote chunk {chunk_index + 1} does not fit on "
                  f"'{tape_label}' ({planned_bytes / 1024**3:.2f} GiB needed, "
                  f"{max(0, available_bytes) / 1024**3:.2f} GiB available "
                  "under the DB safety budget).")
            return False
        return True

    # ------------------------------------------------------------------
    # Pipeline status heartbeat
    # ------------------------------------------------------------------
