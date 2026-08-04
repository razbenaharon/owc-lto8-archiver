"""RemoteChunkStager: fetch a planned chunk from the source host and pack it.

Plan 1, Task 1.2. This is the producer half of the remote pipeline, moved out
of ``RemoteOrchestrator`` unchanged: SSH/tar fetch (serial or parallel), the
staging watchdog, collision handling, packing via :class:`~src.packer.LTOPacker`,
and the preserve/discard/resume rules for a staged pack.

Boundaries this module must keep, because they are what make an interrupted run
recoverable *without anybody standing at the drive*:

* **It never touches the tape.** No LTFS path, no mount probe, no IBM helper.
  Staging failures are local and re-fetchable by construction.
* **A preserved pack is only reusable once its marker exists.** ``_RESUME_MARKER``
  is written last and atomically, so a pack interrupted mid-write simply has no
  marker and is re-fetched rather than half-written to tape.
* **A fetch error is classified before it is retried.** An auth, host-key or
  configuration failure is permanent and must not be retried into; only a
  clearly transient transport failure is.

``host`` is the :class:`~src.remote_orchestrator.RemoteOrchestrator` façade.
Every cross-method call routes back through it, so the orchestrator stays the
single place session state, configuration and stop decisions live — and so a
caller (or a test) that overrides one hook still sees that override honoured.
"""
import gc
import json
import os
import random
import shutil
import threading
import time
from collections import defaultdict
from datetime import datetime

from .constants import LOCAL_STAGING_RESERVE_BYTES
from .db import _apply_canonical_remote_paths
from .exit_codes import (
    CLASS_CONNECTION_REFUSED, CLASS_CONNECTION_RESET, CLASS_CONNECTION_TIMEOUT,
    CLASS_DNS_RESOLUTION_FAILURE, CLASS_NETWORK_UNREACHABLE,
    CLASS_TEMPORARY_TRANSPORT_FAILURE, REASON_BAD_CONFIG,
    REASON_MISSING_NONINTERACTIVE_CREDENTIAL, REASON_SSH_AUTHENTICATION_FAILED,
    REASON_SSH_HOST_KEY_MISMATCH, REASON_SSH_PERMISSION_DENIED)
from .logsetup import get_logger
from .packer import LTOPacker
from .paths import (_LEGACY_PATH_LIMIT, _dir_tree_size,
                    _disambiguate_local_rel, _exceeds_legacy_path_limit,
                    _long, _remote_fetch_base_and_rel,
                    _reserved_name_component, _winsafe_extracted_rel)
from .pipeline_types import ChunkStatus, ContainerFormat, StagedChunk
from .ram_telemetry import RamStageSampler
from .remote_transport import _remote_tar_fetch
from .runtime import (CANCEL, _fmt_eta, _phase, _progress_done, _progress_line,
                      _status)
from .telegram_notify import send_best_effort


# A fetch this far past its planned bytes gets one loud alert; the hard
# abort threshold is the configurable fetch_overrun_abort_factor.
_FETCH_OVERRUN_WARN_FACTOR = 1.10


def _fetch_watchdog_action(*, cur, last_growth_at, now, total_bytes,
                           abort_factor, stall_timeout, free_bytes,
                           reserve_bytes):
    """Pure decision for the staging watchdog — returns a reason or ``None``.

    Split out from the monitor thread so every abort branch is unit-testable
    without spawning ssh/tar. Order is by urgency: a full staging disk wedges
    everything, a hard overrun is a runaway file, and a stall is a stream that
    stays connected but delivers no data (``cur`` has not grown for
    ``stall_timeout`` seconds). Each reason maps to ``abort_evt.set()``; the
    fetch retry/backoff then re-drives the chunk, which stays resumable.
    """
    if free_bytes is not None and free_bytes <= reserve_bytes:
        return 'diskfull'
    if total_bytes and abort_factor and cur > total_bytes * abort_factor:
        return 'overrun'
    if stall_timeout and (now - last_growth_at) >= stall_timeout:
        return 'stall'
    return None

# Written inside a pack dir when a stop preserves it, and the only thing that
# makes that pack reusable. Written last and atomically, so its presence means
# the pack is complete — a pack interrupted mid-write simply has no marker.
_RESUME_MARKER = "_resume_pack.json"

# Permanent fetch-failure signatures, checked FIRST. An auth/permission/host-key/
# config failure is never a retryable network blip: retrying it just spins on an
# unrecoverable error and — worse — could mask a real access problem behind a
# "transient DNS" story. Each maps to the exit_codes reason the terminal uses.
_PERMANENT_FETCH_SIGNATURES = (
    ("host key verification failed", REASON_SSH_HOST_KEY_MISMATCH),
    ("remote host identification has changed", REASON_SSH_HOST_KEY_MISMATCH),
    ("no password-capable ssh helper", REASON_MISSING_NONINTERACTIVE_CREDENTIAL),
    ("no non-interactive credential", REASON_MISSING_NONINTERACTIVE_CREDENTIAL),
    ("permission denied", REASON_SSH_PERMISSION_DENIED),
    ("too many authentication failures", REASON_SSH_AUTHENTICATION_FAILED),
    ("no supported authentication methods", REASON_SSH_AUTHENTICATION_FAILED),
    ("authentication failed", REASON_SSH_AUTHENTICATION_FAILED),
    ("authentication failure", REASON_SSH_AUTHENTICATION_FAILED),
    ("bad configuration option", REASON_BAD_CONFIG),
    ("bad configuration", REASON_BAD_CONFIG),
)

# Transient network/transport signatures, checked only after no permanent one
# matched. Each carries the precise error_classification (the operational
# ``reason`` for all of these stays the generic ``network_retry_exhausted`` —
# not every transient failure is DNS). Note that a bare ``exit 255`` is
# deliberately NOT here: on its own it is ssh's catch-all and could hide an
# auth failure, so a 255 is transient only when accompanied by one of these.
_TRANSIENT_FETCH_CLASSES = (
    ("could not resolve hostname", CLASS_DNS_RESOLUTION_FAILURE),
    ("name or service not known", CLASS_DNS_RESOLUTION_FAILURE),
    ("temporary failure in name resolution", CLASS_DNS_RESOLUTION_FAILURE),
    ("nodename nor servname", CLASS_DNS_RESOLUTION_FAILURE),
    ("getaddrinfo", CLASS_DNS_RESOLUTION_FAILURE),
    ("connection timed out", CLASS_CONNECTION_TIMEOUT),
    ("operation timed out", CLASS_CONNECTION_TIMEOUT),
    ("timed out", CLASS_CONNECTION_TIMEOUT),
    ("connection reset", CLASS_CONNECTION_RESET),
    ("reset by peer", CLASS_CONNECTION_RESET),
    ("connection refused", CLASS_CONNECTION_REFUSED),
    ("network is unreachable", CLASS_NETWORK_UNREACHABLE),
    ("no route to host", CLASS_NETWORK_UNREACHABLE),
    ("connection closed", CLASS_TEMPORARY_TRANSPORT_FAILURE),
    ("broken pipe", CLASS_TEMPORARY_TRANSPORT_FAILURE),
    ("kex_exchange_identification", CLASS_TEMPORARY_TRANSPORT_FAILURE),
    ("banner exchange", CLASS_TEMPORARY_TRANSPORT_FAILURE),
)


def _classify_fetch_error(err):
    """Classify a fetch error as (kind, classification, permanent_reason).

    ``kind`` is ``"permanent"``, ``"transient"`` or ``"unknown"``. Permanent
    signatures are matched FIRST, so stderr that mixes a network hiccup with an
    auth/host-key/config failure is classified permanent (no blind retry) — a
    momentary network blip does not excuse an auth failure. ``classification`` is
    the precise transport diagnosis for a transient error; ``permanent_reason``
    is the exit_codes reason slug for a permanent one.
    """
    if not err:
        return ("unknown", None, None)
    low = str(err).lower()
    for sig, reason in _PERMANENT_FETCH_SIGNATURES:
        if sig in low:
            return ("permanent", None, reason)
    for sig, classification in _TRANSIENT_FETCH_CLASSES:
        if sig in low:
            return ("transient", classification, None)
    return ("unknown", None, None)


def _is_transient_fetch_error(err):
    """True if a fetch error is a retryable network/transport blip.

    Deliberately conservative: an error that is not clearly transient is treated
    as non-retryable, because retrying a genuine problem (missing file,
    permission, corrupt source) just wastes time. A false negative costs a
    re-fetch on resume; a false positive would spin on an unrecoverable error."""
    return _classify_fetch_error(err)[0] == "transient"


class RemoteChunkStager:
    """Fetch + pack one chunk onto local staging. Never touches the tape."""

    def __init__(self, host):
        #: The RemoteOrchestrator façade that owns session state and config.
        self.host = host

    def _stage_chunk(self, session_id, chunk_index, chunk_files):
        """Fetch then pack one chunk. Returns a ready-descriptor or None."""
        self.host._producer_chunk = chunk_index
        format_reader = getattr(
            self.host.db, "get_chunk_packaging_format", None)
        if not callable(format_reader):
            raise RuntimeError(
                "[STAGING] No durable chunk-format reader is available; "
                "refusing to guess ZIP")
        chunk_format = ContainerFormat(
            format_reader(session_id, chunk_index))
        if chunk_format is ContainerFormat.STORED_TAR:
            recovery_guard = getattr(
                self.host.db, "require_existing_stored_tar_recovery", None)
            if not callable(recovery_guard):
                raise RuntimeError(
                    "[STAGING] Stored TAR chunk has no format-aware recovery "
                    "reader; refusing to reinterpret it as ZIP")
            # This deliberately does not inspect stored_tar_write_enabled:
            # disabling NEW creation must not strand an existing immutable TAR.
            recovery_guard(session_id, chunk_index)
            planner = getattr(
                self.host.db, "get_or_create_stored_tar_chunk_plan", None)
            if not callable(planner):
                raise RuntimeError(
                    "[STAGING] Stored TAR chunk has no persisted container-plan "
                    "repository; refusing to start a producer")
            planner(
                session_id, chunk_index, chunk_files,
                loose_threshold_bytes=int(
                    self.host.cfg.zip_threshold_mb * 1024 * 1024),
                max_size_bytes=int(
                    self.host.cfg.stored_tar_max_size_gb * 1024**3),
            )
            raise RuntimeError(
                "[STAGING] Stored TAR is durably assigned and its container "
                "plan is persisted, but TAR production is outside Task 2.1; "
                "preserving the chunk unchanged")
        # The session id is embedded so the on-tape root (basename(pack_dir),
        # see LTOBackup._run_locked) is unique per session — two sessions on the
        # same tape never collide on '_pack_NNN'. Resuming a session reuses the
        # same deterministic names, so robocopy still same-size-skips.
        fetch_dir = os.path.join(
            self.host.staging_dir, f"_fetch_s{session_id:04d}_{chunk_index:03d}")
        pack_dir  = os.path.join(
            self.host.staging_dir, f"_pack_s{session_id:04d}_{chunk_index:03d}")

        # A pack this session preserved on an earlier stop can go straight to
        # tape: it is the same deterministic path, and the marker proves it is
        # complete. This is what makes a restart-driven stop cheap.
        resumed = self.host._try_resume_pack(session_id, chunk_index, pack_dir)
        if resumed is not None:
            return resumed

        # No usable pack. Any leftover pack dir is now known-untrustworthy, and
        # packing into it would silently mix stale files into the chunk, so it
        # goes before the packer runs.
        if os.path.isdir(pack_dir):
            get_logger().warning(
                "clearing an unusable leftover pack dir before repacking "
                "chunk %s: %s", chunk_index + 1, pack_dir)
            self.host._cleanup_dir(pack_dir)

        # --- FETCH (remote -> PC) ---
        self.host.db.update_chunk_status(session_id, chunk_index,
                ChunkStatus.FETCHING.value)
        fetch_start = time.perf_counter()
        ram_stats = {}
        governor = getattr(self.host, 'governor', None)
        if governor:
            planned_bytes = sum(int(row['file_size_bytes'])
                                for row in chunk_files)
            governor.wait_or_pause(
                "fetch", "start", needed_bytes=planned_bytes)
            fetch_guard = governor.mark_fetch_active()
        else:
            fetch_guard = None
        with RamStageSampler(
                "fetch", self.host.ram_sample_interval) as fetch_sampler:
            if fetch_guard:
                with fetch_guard:
                    fetch_ok, source_missing_files, fetched_file_count = (
                        self.host._fetch_chunk(
                            session_id, chunk_index, chunk_files, fetch_dir))
            else:
                fetch_ok, source_missing_files, fetched_file_count = (
                    self.host._fetch_chunk(
                        session_id, chunk_index, chunk_files, fetch_dir))
        ram_stats.update(fetch_sampler.as_details("fetch"))
        if not fetch_ok:
            if not CANCEL.is_set():
                self.host.db.update_chunk_status(session_id, chunk_index,
                ChunkStatus.FETCH_FAILED.value)
            self.host._cleanup_dir(fetch_dir)
            return None
        if CANCEL.is_set():
            self.host._cleanup_dir(fetch_dir)
            return None
        fetch_seconds = time.perf_counter() - fetch_start
        fetch_bytes   = _dir_tree_size(fetch_dir)   # raw remote->PC payload

        if fetched_file_count == 0:
            self.host._cleanup_dir(fetch_dir)
            self.host._cleanup_dir(pack_dir)
            _status('REMOTE', f"Chunk {chunk_index + 1}: all source files are "
                               "missing; no tape write is required.")
            return StagedChunk(
                chunk_index=chunk_index,
                fetch_dir=fetch_dir,
                pack_dir=pack_dir,
                metadata=[],
                staged_bytes=0,
                fetch_seconds=fetch_seconds,
                fetch_bytes=fetch_bytes,
                pack_seconds=0,
                pack_bytes=0,
                ram_stats=ram_stats,
                source_missing_files=source_missing_files,
                skip_tape=True,
                session_id=session_id,
                packaging_format=chunk_format,
            )

        # --- PACK (small files -> ZIP, large files staged loose) ---
        self.host.db.update_chunk_status(session_id, chunk_index,
                ChunkStatus.PACKING.value)
        self.host._cleanup_dir(pack_dir)
        _phase('PACK', f"Packing chunk {chunk_index + 1}: "
                       f"small files -> ZIP, large files staged loose")
        pack_start = time.perf_counter()
        try:
            if governor:
                governor.wait_or_pause(
                    "pack", "start", needed_bytes=fetch_bytes,
                    queued_bytes=getattr(self.host, '_staged_bytes', 0))
                pack_guard = governor.mark_pack_active()
            else:
                pack_guard = None
            # on_existing='clean': this runs on the producer thread, which must
            # never block on the packer's interactive stdin prompt. If the dest
            # cannot be cleaned, the packer raises and the chunk stays
            # resumable instead of the pipeline deadlocking.
            packer = LTOPacker(
                self.host.cfg.max_zip_size_gb,
                index_min_file_mb=self.host.cfg.index_min_file_mb,
                index_packed_small_files=self.host.cfg.index_packed_small_files,
                manifest_enabled=self.host.cfg.small_file_manifest_enabled,
                manifest_format=self.host.cfg.small_file_manifest_format,
                manifest_compression=(
                    self.host.cfg.small_file_manifest_compression),
            )
            with RamStageSampler(
                    "pack", self.host.ram_sample_interval) as pack_sampler:
                if pack_guard:
                    with pack_guard:
                        metadata = packer.run(
                            source=fetch_dir,
                            dest=pack_dir,
                            threshold_mb=self.host.cfg.zip_threshold_mb,
                            skipped_tracker=self.host.skipped_tracker,
                            source_name='remote',
                            session_id=session_id,
                            chunk_index=chunk_index,
                            on_existing='clean',
                            governor=governor,
                            pack_file_batch_size=self.host.pack_file_batch_size,
                            pack_parallel_workers=self.host.pack_parallel_workers,
                        )
                else:
                    metadata = packer.run(
                        source=fetch_dir,
                        dest=pack_dir,
                        threshold_mb=self.host.cfg.zip_threshold_mb,
                        skipped_tracker=self.host.skipped_tracker,
                        source_name='remote',
                        session_id=session_id,
                        chunk_index=chunk_index,
                        on_existing='clean',
                        governor=governor,
                        pack_file_batch_size=self.host.pack_file_batch_size,
                        pack_parallel_workers=self.host.pack_parallel_workers,
                    )
            ram_stats.update(pack_sampler.as_details("pack"))
        except Exception as e:
            print(f"[REMOTE] Packer error: {e}")
            if not CANCEL.is_set():
                self.host.db.update_chunk_status(session_id, chunk_index,
                ChunkStatus.FETCH_FAILED.value)
            self.host._cleanup_dir(fetch_dir)
            self.host._cleanup_dir(pack_dir)
            return None

        if not metadata:
            if not CANCEL.is_set():
                print(f"[REMOTE] Chunk {chunk_index + 1}: nothing to pack "
                      f"(empty fetch). Marking failed.")
                self.host.db.update_chunk_status(session_id, chunk_index,
                ChunkStatus.FETCH_FAILED.value)
            self.host._cleanup_dir(fetch_dir)
            self.host._cleanup_dir(pack_dir)
            return None

        # LTOPacker necessarily sees temporary Windows staging paths. Replace
        # them before logging/indexing with the durable canonical remote paths
        # persisted in the remote manifest.
        canonical_count = _apply_canonical_remote_paths(
            metadata, self.host.db.get_chunk_files(session_id, chunk_index))
        if canonical_count != len(metadata):
            self.host.db.update_chunk_status(session_id, chunk_index,
                ChunkStatus.FETCH_FAILED.value)
            self.host._cleanup_dir(fetch_dir)
            self.host._cleanup_dir(pack_dir)
            raise RuntimeError(
                "[DB] Refusing to index temporary staging paths: canonical "
                f"SOURCE paths mapped for only {canonical_count:,}/"
                f"{len(metadata):,} staged file(s)."
            )

        pack_seconds = time.perf_counter() - pack_start

        # Free the raw fetched copy now that packing is done — this halves the
        # per-chunk staging footprint so the prefetch buffer stays under the cap.
        self.host._cleanup_dir(fetch_dir)
        # A chunk's packer metadata and per-file dicts are the largest transient
        # Python allocation in the pipeline; reclaim them now, before the next
        # chunk's fetch grows the process again.
        gc.collect()

        staged_bytes = _dir_tree_size(pack_dir)
        with self.host._staged_lock:
            self.host._staged_bytes += staged_bytes
        _status('PIPELINE', f"Chunk {chunk_index + 1} staged & ready "
                            f"({staged_bytes / 1024**3:.1f} GB) — queued for tape.")
        return StagedChunk(
            chunk_index=chunk_index,
            fetch_dir=fetch_dir,
            pack_dir=pack_dir,
            metadata=metadata,
            staged_bytes=staged_bytes,
            # Per-phase producer timings, surfaced in the per-pack log. Fetch and
            # pack overlap the *previous* chunk's tape write, so they need not sum
            # to the consumer-measured Total time.
            fetch_seconds=fetch_seconds,
            fetch_bytes=fetch_bytes,
            pack_seconds=pack_seconds,
            pack_bytes=staged_bytes,
            ram_stats=ram_stats,
            source_missing_files=source_missing_files,
            skip_tape=False,
            session_id=session_id,
            packaging_format=chunk_format,
        )

    def _discard_desc(self, desc):
        """Drop a staged-but-unused chunk: clean its dirs and free its budget."""
        self.host._cleanup_dir(desc.fetch_dir)
        self.host._cleanup_dir(desc.pack_dir)
        with self.host._staged_lock:
            self.host._staged_bytes = max(0, self.host._staged_bytes - desc.staged_bytes)

    def _pack_inventory(self, pack_dir):
        """(name, size) for every file under pack_dir, sorted. The integrity basis."""
        items = []
        for root, _dirs, files in os.walk(pack_dir):
            for name in files:
                if name == _RESUME_MARKER:
                    continue
                full = os.path.join(root, name)
                rel = os.path.relpath(full, pack_dir).replace("\\", "/")
                try:
                    items.append([rel, os.path.getsize(full)])
                except OSError:
                    return None
        return sorted(items)

    def _preserve_desc(self, session_id, desc, why):
        """Keep a staged pack on disk so the resume can write it directly.

        The counterpart to ``_discard_desc``. The chunk's DB row is left alone —
        it was never written, so it stays resumable — and the staging dirs are
        left in place. Only the in-memory budget is released, because this
        process is on its way out and the next one re-measures staging from disk.

        A marker recording the descriptor is written *last*, and its presence is
        what makes the pack reusable. A pack that was interrupted mid-write has
        no marker, so a later run cannot mistake a partial pack for a complete
        one — the failure mode that would put a truncated chunk on tape.
        """
        with self.host._staged_lock:
            self.host._staged_bytes = max(0, self.host._staged_bytes - desc.staged_bytes)

        marker_ok = False
        try:
            inventory = self.host._pack_inventory(desc.pack_dir)
            if inventory is None:
                raise OSError("pack directory is unreadable")
            payload = {
                "version": 1,
                "session_id": session_id,
                "chunk_index": desc.chunk_index,
                "fetch_dir": desc.fetch_dir,
                "pack_dir": desc.pack_dir,
                "staged_bytes": desc.staged_bytes,
                "skip_tape": desc.skip_tape,
                "metadata": desc.metadata,
                "source_missing_files": desc.source_missing_files,
                "fetch_seconds": desc.fetch_seconds,
                "fetch_bytes": desc.fetch_bytes,
                "pack_seconds": desc.pack_seconds,
                "pack_bytes": desc.pack_bytes,
                "ram_stats": desc.ram_stats,
                "pack_inventory": inventory,
                "preserved_at": datetime.now().isoformat(),
                "reason": why,
            }
            tmp = os.path.join(desc.pack_dir, _RESUME_MARKER + ".tmp")
            final = os.path.join(desc.pack_dir, _RESUME_MARKER)
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(payload, fh)
            os.replace(tmp, final)  # atomic: a half-written marker never appears
            marker_ok = True
        except Exception:
            get_logger().exception(
                "could not write the resume marker for chunk %s; the pack stays "
                "on disk but the resume will re-fetch it", desc.chunk_index + 1)

        msg = (f"pack_preserved_for_resume: chunk {desc.chunk_index + 1} "
               f"({why}) kept at {desc.pack_dir} reusable={marker_ok}")
        get_logger().warning(msg)
        suffix = ("" if marker_ok
                  else " (marker failed — resume will re-fetch this chunk)")
        print(f"\n[PIPELINE] Chunk {desc.chunk_index + 1}: pack kept in staging "
              f"for resume ({why}).{suffix}")

    def _try_resume_pack(self, session_id, chunk_index, pack_dir):
        """Return a StagedChunk for an intact preserved pack, else None.

        Only a pack carrying a marker whose recorded inventory still matches the
        directory byte-for-byte is reused. Anything else — no marker, changed
        sizes, extra or missing files — is treated as untrustworthy and the
        caller re-fetches. Being wrong here means writing a corrupt chunk to
        tape and recording it as good, so the bar is exact equality, not
        heuristics.
        """
        marker = os.path.join(pack_dir, _RESUME_MARKER)
        if not os.path.isfile(marker):
            return None
        try:
            with open(marker, encoding="utf-8") as fh:
                payload = json.load(fh)
        except Exception:
            get_logger().warning(
                "resume marker for chunk %s is unreadable; re-fetching",
                chunk_index + 1)
            return None

        if (payload.get("version") != 1
                or payload.get("chunk_index") != chunk_index
                or payload.get("session_id") != session_id):
            get_logger().warning(
                "resume marker at %s does not match session %s chunk %s; "
                "re-fetching", pack_dir, session_id, chunk_index + 1)
            return None

        expected = payload.get("pack_inventory")
        actual = self.host._pack_inventory(pack_dir)
        if actual is None or [list(x) for x in expected or []] != [
                list(x) for x in actual]:
            get_logger().warning(
                "preserved pack for chunk %s failed its integrity check "
                "(inventory changed); re-fetching", chunk_index + 1)
            print(f"[REMOTE] Preserved pack for chunk {chunk_index + 1} failed "
                  f"its integrity check — re-fetching it.")
            return None

        desc = StagedChunk(
            chunk_index=chunk_index,
            fetch_dir=payload["fetch_dir"],
            pack_dir=pack_dir,
            metadata=payload.get("metadata") or [],
            staged_bytes=int(payload.get("staged_bytes") or 0),
            fetch_seconds=payload.get("fetch_seconds"),
            fetch_bytes=payload.get("fetch_bytes"),
            pack_seconds=payload.get("pack_seconds"),
            pack_bytes=payload.get("pack_bytes"),
            ram_stats=payload.get("ram_stats") or {},
            source_missing_files=payload.get("source_missing_files") or [],
            skip_tape=bool(payload.get("skip_tape")),
            session_id=session_id,
            packaging_format=ContainerFormat.ZIP,
        )
        with self.host._staged_lock:
            self.host._staged_bytes += desc.staged_bytes
        get_logger().warning(
            "resume_from_existing_pack: session=%s chunk=%s pack=%s bytes=%s "
            "preserved_at=%s", session_id, chunk_index + 1, pack_dir,
            desc.staged_bytes, payload.get("preserved_at"))
        print(f"[REMOTE] Chunk {chunk_index + 1}: reusing the pack preserved at "
              f"{payload.get('preserved_at')} — no re-fetch, no re-pack.")
        return desc

    def _fetch_chunk(self, session_id, chunk_index, chunk_files, fetch_dir):
        os.makedirs(_long(fetch_dir), exist_ok=True)
        total_chunks = self.host.db.count_chunks(session_id)
        source_missing_files = []
        fetched_file_count = 0
        records = []
        pending = []        # primary files: extracted at their sanitized path
        collisions = []     # renamed files: fetched individually, then moved
        claimed = {}        # case-folded local_rel -> remote rel that owns it
        fetching_ids = []

        for row in chunk_files:
            remote_fpath = row['remote_path']
            fsize        = row['file_size_bytes']
            manifest_id  = row['manifest_id']

            if row['status'] == 'source_missing':
                source_missing_files.append({
                    'manifest_id': manifest_id,
                    'remote_path': remote_fpath,
                    'file_size_bytes': fsize,
                })
                self.host.skipped_tracker.add(
                    'remote', remote_fpath, row['error_msg'] or 'source missing',
                    'fetch', session_id=session_id, chunk_index=chunk_index)
                print(f"[REMOTE] Skip (source already missing): {remote_fpath}")
                continue

            try:
                remote_base, rel = _remote_fetch_base_and_rel(
                    self.host.remote_path, remote_fpath
                )
            except ValueError as e:
                self.host.db.update_manifest_row(
                    manifest_id,
                    session_id=session_id,
                    status='fetch_failed',
                    error_msg=str(e),
                )
                print(f"[REMOTE] Invalid remote path: {e}")
                return False, source_missing_files, fetched_file_count

            # rel is the true remote path (sent verbatim to remote tar); the
            # local copy lands under the name the Windows extractor can write.
            local_rel = _winsafe_extracted_rel(rel)
            key = local_rel.casefold()
            collided = key in claimed
            if collided:
                # Two distinct remote names map to the same on-disk path —
                # rename this one so neither file is silently overwritten.
                clash_with = claimed[key]
                local_rel  = _disambiguate_local_rel(local_rel, claimed)
                key        = local_rel.casefold()
                print(f"[REMOTE] Name collision: '{rel}' and '{clash_with}' map "
                      f"to the same Windows path — fetching the former as "
                      f"'{local_rel}'.")
            claimed[key] = rel

            local_path = os.path.join(fetch_dir, local_rel.replace('/', os.sep))
            records.append((row, remote_base, rel, local_rel, local_path))

            # Skip if already fetched with matching size (resume support)
            if os.path.exists(_long(local_path)):
                try:
                    if os.path.getsize(_long(local_path)) == fsize:
                        print(f"[REMOTE] Skip (already fetched): {rel}")
                        continue
                    os.remove(_long(local_path))  # partial from interrupted run
                except OSError:
                    pass

            fetching_ids.append(manifest_id)
            (collisions if collided else pending).append(
                (row, remote_base, rel, local_rel, local_path))

        for start in range(0, len(fetching_ids), self.host.metadata_batch_size):
            if governor := getattr(self.host, 'governor', None):
                governor.wait_or_pause("fetch", "continue")
            batch_ids = fetching_ids[start:start + self.host.metadata_batch_size]
            self.host.db.update_manifest_rows_fetching(
                batch_ids, session_id=session_id)

        if pending or collisions:
            todo_bytes = sum(row['file_size_bytes']
                             for row, *_ in pending + collisions)
            todo_count = len(pending) + len(collisions)
            _phase('FETCH', f"Remote -> PC | chunk {chunk_index + 1}/{total_chunks} | "
                            f"{todo_count} file(s), {todo_bytes / 1024**3:.2f} GB")
            _status('SSH', f"Opening tar stream to "
                           f"{self.host.remote_user}@{self.host.remote_host} "
                           f"(cipher={self.host.ssh_cipher or 'default'}, "
                           f"mbuffer={'on' if self.host.use_mbuffer else 'off'})")

            fetch_stop  = threading.Event()
            fetch_abort = threading.Event()
            self.host._start_fetch_monitor(fetch_stop, fetch_abort, fetch_dir,
                                      todo_bytes)

            pending_by_base = defaultdict(list)
            for row, remote_base, rel, local_rel, local_path in pending:
                pending_by_base[remote_base].append((row, rel, local_path))

            # One work item per (base, metadata-sized batch). Streams > 1 run
            # these concurrently to overlap per-file stalls; the default (1)
            # keeps the exact legacy single-stream behaviour.
            work_items = []
            for remote_base, base_pending in pending_by_base.items():
                for start in range(
                        0, len(base_pending), self.host.metadata_batch_size):
                    work_items.append(
                        (remote_base,
                         base_pending[start:start + self.host.metadata_batch_size]))

            streams = max(1, int(getattr(self.host, 'fetch_parallel_streams', 1)))

            try:
                if streams <= 1 or len(work_items) <= 1:
                    for remote_base, base_batch in work_items:
                        if CANCEL.is_set():
                            return False, source_missing_files, fetched_file_count
                        if governor := getattr(self.host, 'governor', None):
                            governor.wait_or_pause("fetch", "continue")
                        ok, err = self.host._fetch_one_batch(
                            remote_base, base_batch, fetch_dir, fetch_abort)
                        if not ok:
                            if CANCEL.is_set():
                                return False, source_missing_files, fetched_file_count
                            print(f"\n[REMOTE] Tar fetch failed:\n{err}")
                            self.host.db.update_manifest_rows_fetch_failed(
                                (row['manifest_id'] for row, _, _ in base_batch),
                                err, session_id=session_id)
                            return False, source_missing_files, fetched_file_count
                elif not self.host._fetch_batches_parallel(
                        work_items, fetch_dir, fetch_abort, session_id, streams):
                    return False, source_missing_files, fetched_file_count

                # Renamed files can't ride the shared stream (bsdtar would
                # extract them onto the primary's path), so fetch each alone
                # into an isolated dir and move it to its disambiguated name.
                if collisions and not self.host._fetch_collisions(
                        session_id, collisions, fetch_dir,
                        source_missing_files, fetch_abort):
                    return False, source_missing_files, fetched_file_count
            finally:
                fetch_stop.set()
                _progress_done()
        else:
            print(f"[REMOTE] Chunk {chunk_index + 1}/{total_chunks}: "
                  "all files already fetched.")

        source_missing_ids = {
            item['manifest_id'] for item in source_missing_files
        }
        fetched_updates = []
        for row, _, rel, local_rel, local_path in records:
            fsize       = row['file_size_bytes']
            manifest_id = row['manifest_id']
            if manifest_id in source_missing_ids:
                continue
            if not os.path.exists(_long(local_path)):
                # An absent file is normally a genuine remote omission (the
                # remote tar emitted a tolerated "Cannot stat"). But if the
                # local target is unwritable by the non-long-path-aware
                # extractor — a reserved device name or an over-MAX_PATH target
                # — the absence is a LOCAL drop we must not record as
                # source_missing. Fail the chunk loudly; it stays resumable.
                reserved = _reserved_name_component(local_rel)
                too_long = _exceeds_legacy_path_limit(local_path)
                if reserved or too_long:
                    reason = (f"reserved Windows device name '{reserved}'"
                              if reserved else
                              f"target exceeds the {_LEGACY_PATH_LIMIT}-char "
                              "Windows path limit")
                    msg = (f"refusing to skip '{row['remote_path']}': it could "
                           f"not be written locally ({reason}). "
                           f"Target: {local_path}")
                    print(f"\n[REMOTE] {msg}")
                    self.host.db.update_manifest_row(
                        manifest_id, session_id=session_id,
                        status='fetch_failed', error_msg=msg[:500])
                    return False, source_missing_files, fetched_file_count
                detail = {
                    'manifest_id': manifest_id,
                    'remote_path': row['remote_path'],
                    'file_size_bytes': fsize,
                }
                source_missing_files.append(detail)
                source_missing_ids.add(manifest_id)
                self.host.skipped_tracker.add(
                    'remote', row['remote_path'], "missing after tar fetch",
                    'fetch', session_id=session_id, chunk_index=chunk_index)
                print(f"[REMOTE] Source missing; skipped: {row['remote_path']}")
                self.host.db.update_manifest_row(
                    manifest_id,
                    session_id=session_id,
                    status='source_missing',
                    local_rel_path=None,
                    error_msg="missing after tar fetch",
                )
                continue

            try:
                actual = os.path.getsize(_long(local_path))
            except OSError as e:
                self.host.db.update_manifest_row(
                    manifest_id,
                    session_id=session_id,
                    status='fetch_failed',
                    error_msg=f"stat failed: {e}",
                )
                return False, source_missing_files, fetched_file_count

            if actual != fsize:
                print(f"[REMOTE] Size mismatch for {rel}: "
                      f"expected {fsize} B, got {actual} B")
                try:
                    os.remove(_long(local_path))
                except OSError:
                    pass
                self.host.db.update_manifest_row(
                    manifest_id,
                    session_id=session_id,
                    status='fetch_failed',
                    error_msg=f"size mismatch: expected {fsize}, got {actual}",
                )
                return False, source_missing_files, fetched_file_count

            fetched_updates.append((local_rel, manifest_id))
        for start in range(0, len(fetched_updates), self.host.metadata_batch_size):
            if governor := getattr(self.host, 'governor', None):
                governor.wait_or_pause("fetch", "continue")
            self.host.db.update_manifest_rows_fetched(
                fetched_updates[start:start + self.host.metadata_batch_size],
                session_id=session_id)
        fetched_file_count = len(fetched_updates)
        return True, source_missing_files, fetched_file_count

    def _fetch_one_batch(self, remote_base, base_batch, fetch_dir, fetch_abort):
        """Fetch one metadata-sized batch as a single tar stream.

        Returns (ok, err) — the shared shape used by both fetch paths. A batch
        that fails on a transient network/DNS error is retried with exponential
        backoff (``fetch_transient_retries``) before giving up, so a momentary
        blip costs seconds rather than the whole streaming session. A cancel or
        a sibling-stream abort ends the retries immediately."""
        attempts = max(0, getattr(self.host, "fetch_transient_retries", 0))
        base = getattr(self.host, "fetch_transient_retry_base", 5.0)
        rel_paths = [rel for _, rel, _ in base_batch]

        for attempt in range(attempts + 1):
            if CANCEL.is_set() or fetch_abort.is_set():
                return False, "cancelled"
            ok, err = _remote_tar_fetch(
                self.host.remote_user,
                self.host.remote_host,
                remote_base,
                rel_paths,
                fetch_dir,
                password=self.host.remote_password,
                cipher=self.host.ssh_cipher,
                use_mbuffer=self.host.use_mbuffer,
                mbuffer_size=self.host.mbuffer_size,
                fetch_cores=self.host.fetch_cores,
                abort_evt=fetch_abort,
            )
            if ok:
                return ok, err
            # Permanent-first classification: an auth/host-key/config failure is
            # never retried, however "network-ish" a co-occurring line looks.
            if attempt >= attempts or not _is_transient_fetch_error(err):
                self.host._note_fetch_failure(err)
                return ok, err
            if CANCEL.is_set() or fetch_abort.is_set():
                return False, "cancelled"
            delay = self.host._fetch_backoff_delay(attempt, base)
            _cls = _classify_fetch_error(err)[1]
            self.host._note_fetch_failure(err, retry_attempt=attempt + 1,
                                     next_retry_delay=delay)
            self.host._write_status_snapshot(
                phase="fetch-retry", retry_attempt=attempt + 1,
                error_classification=_cls, error_message=str(err).strip()[:300],
                next_retry_delay=delay, resumable=True)
            msg = (f"transient fetch error (attempt {attempt + 1}/{attempts + 1}), "
                   f"retrying in {delay:.0f}s: {str(err).strip()[:160]}")
            get_logger().warning("fetch_transient_retry: %s", msg)
            print(f"\n[REMOTE] {msg}")
            # Interruptible wait: a cancel/abort during backoff returns at once.
            if fetch_abort.wait(delay) or CANCEL.is_set():
                return False, "cancelled"
        self.host._note_fetch_failure(err)
        return False, "retries exhausted"

    @staticmethod
    def _fetch_backoff_delay(attempt, base):
        """Exponential backoff with jitter, capped at 60s.

        Jitter (``0.5 + random``) spreads retries so several streams that failed
        on the same blip do not re-hammer the host in lockstep. The cap is
        applied after jitter so a delay is always bounded and non-negative."""
        raw = float(base) * (2 ** attempt)
        jittered = raw * (0.5 + random.random())
        return max(0.0, min(60.0, jittered))

    def _note_fetch_failure(self, err, retry_attempt=None, next_retry_delay=None):
        """Record the classification of a fetch failure for the terminal path.

        The bubbled-up producer error is a generic "chunk N could not be staged";
        this keeps the *precise* diagnosis (permanent auth vs transient DNS, etc.)
        so the final StopResult carries the right exit code and reason."""
        kind, classification, permanent_reason = _classify_fetch_error(err)
        self.host._last_fetch_failure = {
            "kind": kind,
            "classification": classification,
            "permanent_reason": permanent_reason,
            "detail": str(err).strip()[:500],
            "retry_attempt": retry_attempt,
            "next_retry_delay": next_retry_delay,
        }

    def _fetch_batches_parallel(self, work_items, fetch_dir, fetch_abort,
                                session_id, streams):
        """Fetch work items with up to ``streams`` concurrent tar streams.

        Batches are disjoint file lists extracted into the same fetch dir, so
        concurrency is safe. On the first non-cancel failure the shared
        ``fetch_abort`` is set (killing the other streams' ssh/tar trees) and
        the failing batch's rows are marked fetch_failed. Returns True on full
        success, False on failure (caller re-fetches the chunk on resume)."""
        from concurrent.futures import ThreadPoolExecutor

        governor = getattr(self.host, 'governor', None)
        failure = {}
        failure_lock = threading.Lock()

        def _worker(item):
            remote_base, base_batch = item
            if CANCEL.is_set() or fetch_abort.is_set():
                return item, False, "cancelled"
            ok, err = self.host._fetch_one_batch(
                remote_base, base_batch, fetch_dir, fetch_abort)
            if not ok and not CANCEL.is_set():
                with failure_lock:
                    if not failure:
                        failure['err'] = err
                        failure['batch'] = base_batch
                        fetch_abort.set()  # stop the sibling streams
            return item, ok, err

        _status('FETCH', f"Parallel fetch: {streams} concurrent stream(s) over "
                         f"{len(work_items)} batch(es).")
        with ThreadPoolExecutor(max_workers=streams) as pool:
            futures = []
            for item in work_items:
                if CANCEL.is_set() or fetch_abort.is_set():
                    break
                if governor:
                    governor.wait_or_pause("fetch", "continue")
                futures.append(pool.submit(_worker, item))
            for fut in futures:
                fut.result()

        if failure:
            if CANCEL.is_set():
                return False
            print(f"\n[REMOTE] Tar fetch failed:\n{failure['err']}")
            self.host.db.update_manifest_rows_fetch_failed(
                (row['manifest_id'] for row, _, _ in failure['batch']),
                failure['err'], session_id=session_id)
            return False
        return not (CANCEL.is_set() or fetch_abort.is_set())

    def _fetch_collisions(self, session_id, collisions, fetch_dir,
                          source_missing_files, abort_evt=None):
        """Fetch files whose sanitized name clashed with another file's.

        Each is streamed alone into a private temp dir (where bsdtar writes it
        at its natural sanitized path) and then moved to the disambiguated
        local_path. Missing sources are accumulated and skipped; other failures
        leave the row marked fetch_failed for the caller to surface."""
        collide_root = os.path.join(fetch_dir, '_collide')
        try:
            for row, remote_base, rel, _local_rel, local_path in collisions:
                if CANCEL.is_set():
                    return False
                if governor := getattr(self.host, 'governor', None):
                    governor.wait_or_pause("fetch", "continue")
                tmp = os.path.join(collide_root, str(row['manifest_id']))
                shutil.rmtree(_long(tmp), ignore_errors=True)
                os.makedirs(_long(tmp), exist_ok=True)

                ok, err = _remote_tar_fetch(
                    self.host.remote_user,
                    self.host.remote_host,
                    remote_base,
                    [rel],
                    tmp,
                    password=self.host.remote_password,
                    cipher=self.host.ssh_cipher,
                    use_mbuffer=self.host.use_mbuffer,
                    mbuffer_size=self.host.mbuffer_size,
                    fetch_cores=self.host.fetch_cores,
                    abort_evt=abort_evt,
                )
                if not ok:
                    if CANCEL.is_set():
                        return False
                    print(f"\n[REMOTE] Tar fetch failed (renamed file):\n{err}")
                    self.host.db.update_manifest_row(
                        row['manifest_id'], session_id=session_id,
                        status='fetch_failed',
                        error_msg=err[:500])
                    return False

                # Alone in tmp, the file lands at its natural sanitized path.
                natural = os.path.join(
                    tmp, _winsafe_extracted_rel(rel).replace('/', os.sep))
                if not os.path.exists(_long(natural)):
                    # As in _fetch_chunk: a reserved-name or over-MAX_PATH
                    # target is a local write failure, not a remote omission —
                    # surface it loudly (resumable) instead of source_missing.
                    reserved = _reserved_name_component(_local_rel)
                    too_long = (_exceeds_legacy_path_limit(natural)
                                or _exceeds_legacy_path_limit(local_path))
                    if reserved or too_long:
                        reason = (f"reserved Windows device name '{reserved}'"
                                  if reserved else
                                  f"target exceeds the {_LEGACY_PATH_LIMIT}-char "
                                  "Windows path limit")
                        msg = (f"refusing to skip '{row['remote_path']}': it "
                               f"could not be written locally ({reason}).")
                        print(f"\n[REMOTE] {msg}")
                        self.host.db.update_manifest_row(
                            row['manifest_id'], session_id=session_id,
                            status='fetch_failed',
                            error_msg=msg[:500])
                        return False
                    detail = {
                        'manifest_id': row['manifest_id'],
                        'remote_path': row['remote_path'],
                        'file_size_bytes': row['file_size_bytes'],
                    }
                    source_missing_files.append(detail)
                    self.host.skipped_tracker.add(
                        'remote', row['remote_path'], "missing after tar fetch",
                        'fetch', session_id=session_id, chunk_index=None)
                    print(f"[REMOTE] Source missing; skipped: {row['remote_path']}")
                    self.host.db.update_manifest_row(
                        row['manifest_id'], session_id=session_id,
                        status='source_missing',
                        local_rel_path=None,
                        error_msg="missing after tar fetch")
                    continue

                os.makedirs(_long(os.path.dirname(local_path)), exist_ok=True)
                try:
                    os.replace(_long(natural), _long(local_path))
                except OSError as e:
                    print(f"[REMOTE] Could not place renamed file {rel}: {e}")
                    self.host.db.update_manifest_row(
                        row['manifest_id'], session_id=session_id,
                        status='fetch_failed',
                        error_msg=f"move failed: {e}")
                    return False
            return True
        finally:
            shutil.rmtree(_long(collide_root), ignore_errors=True)

    def _start_fetch_monitor(self, stop_evt, abort_evt, fetch_dir, total_bytes):
        """Live remote->PC throughput, plus a staging watchdog.

        Progress is the fetch dir's logical growth. The watchdog fires
        abort_evt — killing the tar stream — before an overrunning chunk can
        exhaust the staging disk and wedge the pipeline: either when free
        space on the staging volume reaches the reserve floor, or when the
        chunk exceeds its planned bytes by fetch_overrun_abort_factor. Any
        overrun past the warn threshold is reported loudly (the plan's sizes
        come from the scan, so a growing or sparse-expanded remote file shows
        up here first)."""
        abort_factor = self.host.fetch_abort_factor
        stall_timeout = self.host.fetch_stall_timeout

        def _alarm(msg):
            print(f"\n[FETCH][ALERT] {msg}")
            send_best_effort(self.host.notifier, f"[FETCH] {msg}")

        def _mon():
            prev_bytes = 0
            prev_time  = time.time()
            interval   = 2
            overrun_warned = False
            # Stall tracking: the last time the staging dir grew by a byte. A
            # wedged SSH/tar stream stays connected and delivers nothing, so
            # ``cur`` never advances and tar's communicate() blocks forever
            # (there is no idle timeout on the stream itself). The project has
            # hit exactly this — a ~2.5h fetch/stage hang on 2026-07-15 — so the
            # watchdog is the safety net that turns an infinite hang into a
            # bounded, resumable retry.
            max_seen = 0
            last_growth_at = time.time()
            while not stop_evt.wait(interval):
                walk_start = time.time()
                cur   = _dir_tree_size(fetch_dir)
                now   = time.time()
                # Rewalking a chunk with many small files is itself expensive;
                # keep the scan overhead under ~10% of the monitor's cycle.
                interval = min(30, max(2, (now - walk_start) * 10))
                dt    = now - prev_time
                speed = ((cur - prev_bytes) / 1024**2) / dt if dt > 0 else 0
                pct   = (cur / total_bytes * 100) if total_bytes else 0
                remaining = max(0, total_bytes - cur)
                eta = remaining / (speed * 1024**2) if speed > 0 else None
                _progress_line(
                    f"[FETCH] {pct:.1f}% | {speed:.1f} MB/s | "
                    f"{cur / 1024**3:.1f}/{total_bytes / 1024**3:.1f} GB | "
                    f"ETA {_fmt_eta(eta)}"
                )
                prev_bytes = cur
                prev_time  = now
                if cur > max_seen:
                    max_seen = cur
                    last_growth_at = now

                if (total_bytes and not overrun_warned
                        and cur > total_bytes * _FETCH_OVERRUN_WARN_FACTOR):
                    overrun_warned = True
                    _alarm(
                        f"chunk overrun: {cur / 1024**3:.1f} GB fetched of "
                        f"{total_bytes / 1024**3:.1f} GB planned — a remote "
                        "file likely grew after the scan. The fetch continues "
                        "but is watched for a hard overrun."
                    )

                try:
                    free = shutil.disk_usage(self.host.staging_dir).free
                except OSError:
                    free = None
                action = _fetch_watchdog_action(
                    cur=cur, last_growth_at=last_growth_at, now=now,
                    total_bytes=total_bytes, abort_factor=abort_factor,
                    stall_timeout=stall_timeout, free_bytes=free,
                    reserve_bytes=LOCAL_STAGING_RESERVE_BYTES)
                if action == 'diskfull':
                    _alarm(
                        f"aborting fetch: staging free space is down to "
                        f"{(free or 0) / 1024**3:.1f} GB (reserve floor "
                        f"{LOCAL_STAGING_RESERVE_BYTES / 1024**3:.0f} GB). "
                        "The chunk stays resumable."
                    )
                    abort_evt.set()
                    return
                if action == 'overrun':
                    _alarm(
                        f"aborting fetch: {cur / 1024**3:.1f} GB fetched "
                        f"exceeds {abort_factor:.1f}x the planned "
                        f"{total_bytes / 1024**3:.1f} GB "
                        "(fetch_overrun_abort_factor). The chunk stays "
                        "resumable; re-scan so the plan matches the source."
                    )
                    abort_evt.set()
                    return
                if action == 'stall':
                    _alarm(
                        f"aborting fetch: staging has not grown "
                        f"({cur / 1024**3:.1f} GB) for {stall_timeout}s — the "
                        "remote stream is wedged (connected but delivering no "
                        "data). The chunk stays resumable and will be retried."
                    )
                    abort_evt.set()
                    return
        threading.Thread(target=_mon, name='fetch-monitor', daemon=True).start()

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    def _cleanup_remote_staging_dirs(self):
        """Remove remote-session temp folders before a truly fresh run."""
        staging_root = os.path.abspath(self.host.staging_dir)
        try:
            names = os.listdir(staging_root)
        except OSError as e:
            print(f"[REMOTE] Warning - could not inspect staging directory: {e}")
            return

        for name in names:
            if not (name.startswith("_fetch_") or name.startswith("_pack_")):
                continue
            path = os.path.abspath(os.path.join(staging_root, name))
            if path == staging_root or not path.startswith(staging_root + os.sep):
                print(f"[REMOTE] Warning - refusing to clean suspicious path: {path}")
                continue
            self.host._cleanup_dir(path)

    def _cleanup_dir(self, path):
        if os.path.exists(_long(path)):
            governor = getattr(self.host, 'governor', None)
            if governor:
                governor.wait_until(governor.can_cleanup, "cleanup")
            try:
                if governor:
                    with governor.mark_cleanup_active():
                        shutil.rmtree(_long(path))
                else:
                    shutil.rmtree(_long(path))
                print(f"[REMOTE] Cleaned: {path}")
            except OSError as e:
                print(f"[REMOTE] Warning — could not clean {path}: {e}")
