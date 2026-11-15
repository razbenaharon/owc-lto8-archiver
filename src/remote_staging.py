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
import uuid
import zipfile
from collections import defaultdict
from dataclasses import dataclass
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
from .archive_artifacts import (
    publish_stored_tar_pair, resolve_locator, tar_sidecar_locator)
from .packer import LTOPacker, StagingSpaceError
from .paths import (_LEGACY_PATH_LIMIT, _dir_tree_size,
                     _disambiguate_local_rel, _exceeds_legacy_path_limit,
                     _local_collision_key, _long, _remote_fetch_base_and_rel,
                     remote_store_base_and_rel,
                    _reserved_name_component, _resolve_extracted_path,
                    _winsafe_extracted_rel)
from .pipeline_types import (
    ArtifactKind, ArtifactReadiness, ChunkStatus, ContainerFormat,
    ContainerValidationState, SourceDisposition, StagedArtifact, StagedChunk,
    StagedContainer)
from .ram_telemetry import RamStageSampler
from .remote_transport import (_remote_source_status_probe,
                               _remote_tar_fetch, _remote_tar_store)
from .tar_container import validate_stored_tar_part
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


@dataclass(frozen=True)
class StagingReservation:
    """Local staging bytes reserved before a producer is launched."""

    packaging_format: ContainerFormat
    needed_bytes: int
    tar_stream_bytes: int = 0
    sidecar_bytes: int = 0
    loose_bytes: int = 0
    pack_copy_bytes: int = 0


def _sidecar_reserve_bytes(member_count, cfg):
    fixed = int(getattr(cfg, "stored_tar_sidecar_fixed_reserve_bytes", 4096))
    per_member = int(getattr(
        cfg, "stored_tar_sidecar_member_reserve_bytes", 1024))
    return max(0, fixed) + max(0, per_member) * max(0, int(member_count or 0))


def stored_tar_staging_reservation(chunk_plan, host):
    """Conservative peak footprint for direct Stored TAR staging.

    Direct TAR never extracts its small-file members.  The reserve therefore
    covers the aggregate unfinished/final TAR artifacts, permanent sidecar
    temp+final files, the sidecar copy placed in the pack tree, loose large-file
    fetch+pack coexistence, and the writer-visible pack copy.
    """
    tar_stream_bytes = sum(
        int(item.estimated_archive_bytes) for item in chunk_plan.containers)
    small_members = len(chunk_plan.small_members)
    sidecar_final = _sidecar_reserve_bytes(small_members, host.cfg)
    sidecar_bytes = 3 * sidecar_final
    loose_logical = sum(
        int(item.file_size_bytes) for item in chunk_plan.loose_members)
    loose_count = len(chunk_plan.loose_members)
    if loose_count:
        loose_physical = host._physical_estimate(loose_logical, loose_count)
    else:
        loose_physical = 0
    loose_bytes = 2 * loose_physical
    pack_copy_bytes = tar_stream_bytes
    return StagingReservation(
        packaging_format=ContainerFormat.STORED_TAR,
        needed_bytes=tar_stream_bytes + sidecar_bytes + loose_bytes
        + pack_copy_bytes,
        tar_stream_bytes=tar_stream_bytes,
        sidecar_bytes=sidecar_bytes,
        loose_bytes=loose_bytes,
        pack_copy_bytes=pack_copy_bytes,
    )


def zip_staging_reservation(planned_bytes, planned_files, host):
    need = 2 * host._physical_estimate(planned_bytes, planned_files)
    return StagingReservation(
        packaging_format=ContainerFormat.ZIP, needed_bytes=need)


def _file_size_or_zero(path):
    try:
        return os.path.getsize(_long(path))
    except OSError:
        return 0


def _staging_progress_snapshot(part_paths=(), loose_dirs=()):
    parts = {
        str(path): _file_size_or_zero(path)
        for path in part_paths or ()
        if path and os.path.isfile(_long(path))
    }
    loose = sum(_dir_tree_size(path) for path in loose_dirs or ()
                if path and os.path.isdir(_long(path)))
    return {"parts": parts, "loose_bytes": loose,
            "total_bytes": sum(parts.values()) + loose}


def _tar_progress_watchdog_action(
        *, snapshot, previous_snapshot, part_last_growth, aggregate_last_growth,
        now, total_bytes, abort_factor, stall_timeout, free_bytes,
        reserve_bytes):
    action = _fetch_watchdog_action(
        cur=int(snapshot["total_bytes"]),
        last_growth_at=aggregate_last_growth,
        now=now,
        total_bytes=total_bytes,
        abort_factor=abort_factor,
        stall_timeout=stall_timeout,
        free_bytes=free_bytes,
        reserve_bytes=reserve_bytes)
    if action is not None:
        return action
    if not stall_timeout:
        return None
    previous_parts = (previous_snapshot or {}).get("parts", {})
    for path, size in snapshot["parts"].items():
        if size > int(previous_parts.get(path, -1)):
            part_last_growth[path] = now
        elif now - part_last_growth.get(path, now) >= stall_timeout:
            return "part_stall"
    return None

# Written inside a pack dir when a stop preserves it, and the only thing that
# makes that pack reusable. Written last and atomically, so its presence means
# the pack is complete — a pack interrupted mid-write simply has no marker.
_RESUME_MARKER = "_resume_pack.json"


def _pack_inventory_readonly(pack_dir):
    """Return the marker-excluded name/size inventory, or ``None`` unreadable.

    This is deliberately path/size based.  Plan 2 does not use content hashes,
    and callers must be able to inspect a preserved pack without changing it.
    """
    items = []
    walk_errors = []

    def onerror(exc):
        walk_errors.append(exc)

    try:
        for root, _dirs, files in os.walk(pack_dir, onerror=onerror):
            for name in files:
                if name == _RESUME_MARKER:
                    continue
                full = os.path.join(root, name)
                rel = os.path.relpath(full, pack_dir).replace("\\", "/")
                items.append([rel, os.path.getsize(_long(full))])
    except OSError:
        return None
    return None if walk_errors else sorted(items)


def _read_resume_pack_marker(pack_dir, session_id, chunk_index):
    """Parse and inventory-check one resume marker without modifying it."""
    result = {
        "marker_state": "unknown",
        "inventory_state": "unknown",
        "pack_file_count": None,
        "packaging_format": None,
    }
    try:
        os.stat(_long(pack_dir))
    except FileNotFoundError:
        result["marker_state"] = "pack_directory_absent"
        return result, None
    except OSError:
        result["marker_state"] = "pack_directory_unreadable"
        return result, None
    if not os.path.isdir(_long(pack_dir)):
        result["marker_state"] = "pack_directory_unreadable"
        return result, None

    actual = _pack_inventory_readonly(pack_dir)
    if actual is None:
        result["marker_state"] = "pack_directory_unreadable"
        return result, None
    result["pack_file_count"] = len(actual)
    result["inventory_state"] = "readable"

    marker = os.path.join(pack_dir, _RESUME_MARKER)
    try:
        with open(_long(marker), encoding="utf-8") as fh:
            payload = json.load(fh)
    except FileNotFoundError:
        result["marker_state"] = "absent"
        return result, None
    except (OSError, ValueError, TypeError):
        result["marker_state"] = "unreadable"
        return result, None

    if not isinstance(payload, dict):
        result["marker_state"] = "unreadable"
        return result, None
    result["packaging_format"] = payload.get(
        "packaging_format", ContainerFormat.ZIP.value)
    if (payload.get("version") != 1
            or payload.get("chunk_index") != int(chunk_index)
            or payload.get("session_id") != int(session_id)):
        result["marker_state"] = "identity_mismatch"
        return result, None

    expected = payload.get("pack_inventory")
    if not isinstance(expected, list):
        result["marker_state"] = "unreadable"
        return result, None
    try:
        expected = sorted([list(item) for item in expected])
    except (TypeError, ValueError):
        result["marker_state"] = "unreadable"
        return result, None
    if expected != actual:
        result["marker_state"] = "inventory_mismatch"
        result["inventory_state"] = "mismatch"
        return result, None

    result["marker_state"] = "valid"
    result["inventory_state"] = "matched"
    return result, payload


def inspect_resume_pack_marker(pack_dir, session_id, chunk_index):
    """Return path-free, read-only resume-marker and pack-content evidence."""
    report, _payload = _read_resume_pack_marker(
        pack_dir, session_id, chunk_index)
    return report

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


def _enum_value(value):
    return getattr(value, "value", value)


class ChunkReconciliationError(RuntimeError):
    """A packed chunk does not hold exactly the files that were fetched."""


def _slash(path):
    return str(path or "").replace("\\", "/")


def reconcile_packed_chunk(planned_rows, source_missing_files,
                           fetched_file_count, manifest_rows, metadata,
                           pack_dir=None):
    """Compare plan, fetch and pack; return a list of discrepancies.

    The packer records a per-file failure in the skip list and carries on, so
    its return value alone cannot show that a file was dropped (audit H7).
    This is the gate between packing and the tape writer: an empty list means
    every planned file is either positively source-missing or present in the
    pack exactly once at its fetched size, and nothing else is in the pack.

    ``manifest_rows`` are the chunk's rows re-read after the fetch (they carry
    ``local_rel_path``); ``metadata`` is the packer's output. With
    ``pack_dir`` the staged artifacts themselves are inspected too: each ZIP's
    central directory and each loose file's size must agree with the metadata.
    """
    problems = []
    planned = list(planned_rows)
    missing_paths = {item['remote_path'] for item in source_missing_files}
    expected_count = len(planned) - len(missing_paths)
    if len({row['remote_path'] for row in planned}) != len(planned):
        problems.append("the plan lists a source path more than once")
    if len(missing_paths) != len(source_missing_files):
        problems.append("a source-missing file is listed more than once")
    if not missing_paths <= {row['remote_path'] for row in planned}:
        problems.append("a source-missing file is not part of the plan")
    if int(fetched_file_count) != expected_count:
        problems.append(
            f"planned {len(planned):,} file(s), {len(missing_paths):,} "
            f"confirmed missing at source, but {int(fetched_file_count):,} "
            "fetched")
    if len(metadata) != int(fetched_file_count):
        problems.append(
            f"{int(fetched_file_count):,} file(s) fetched but "
            f"{len(metadata):,} packed")

    packed = {}
    for item in metadata:
        stored = _slash(item.get('stored_path'))
        if stored in packed:
            problems.append(f"packed twice: {stored}")
        packed[stored] = item

    local_by_remote = {
        row['remote_path']: row.get('local_rel_path') for row in manifest_rows}
    expected_local = set()
    for row in planned:
        remote_path = row['remote_path']
        if remote_path in missing_paths:
            continue
        local_rel = local_by_remote.get(remote_path)
        if not local_rel:
            problems.append(f"no staged path recorded for: {remote_path}")
            continue
        local_rel = _slash(local_rel)
        expected_local.add(local_rel)
        item = packed.get(local_rel)
        if item is None:
            problems.append(f"fetched but not packed: {remote_path}")
            continue
        if int(item.get('file_size_bytes', -1)) != int(row['file_size_bytes']):
            problems.append(
                f"packed size {item.get('file_size_bytes')} differs from "
                f"planned {row['file_size_bytes']}: {remote_path}")
    for stored in packed:
        if stored not in expected_local:
            problems.append(f"packed but not planned: {stored}")

    if pack_dir is not None and not problems:
        problems.extend(_pack_artifact_discrepancies(pack_dir, metadata))
    return problems


def _pack_artifact_discrepancies(pack_dir, metadata):
    """Check the staged ZIPs and loose files against the packer metadata."""
    problems = []
    by_container = defaultdict(dict)
    for item in metadata:
        stored = _slash(item.get('stored_path'))
        size = int(item.get('file_size_bytes', -1))
        if item.get('is_packed'):
            by_container[item.get('container_name')][stored] = size
            continue
        path = os.path.join(pack_dir, stored.replace('/', os.sep))
        try:
            actual = os.path.getsize(_long(path))
        except OSError as exc:
            problems.append(f"loose file is not in the pack: {stored} ({exc})")
            continue
        if actual != size:
            problems.append(
                f"loose file is {actual} B in the pack, expected {size} B: "
                f"{stored}")
    for container, members in by_container.items():
        path = os.path.join(pack_dir, str(container))
        try:
            with zipfile.ZipFile(_long(path)) as archive:
                actual = {}
                for info in archive.infolist():
                    if info.filename in actual:
                        problems.append(
                            f"{container} holds {info.filename} twice")
                    actual[info.filename] = info.file_size
        except (OSError, zipfile.BadZipFile) as exc:
            problems.append(f"bundle {container} is unreadable: {exc}")
            continue
        if actual != members:
            absent = sorted(set(members) - set(actual))
            extra = sorted(set(actual) - set(members))
            wrong = sorted(name for name in set(members) & set(actual)
                           if members[name] != actual[name])
            problems.append(
                f"bundle {container} disagrees with its metadata: "
                f"{len(absent)} absent, {len(extra)} undeclared, "
                f"{len(wrong)} wrong-sized "
                f"(e.g. {(absent + extra + wrong)[:3]})")
    return problems


# The only remote observations that may excuse a planned file from a chunk.
# Each is a positive statement by the source host about that exact name; a
# file that merely failed to appear locally is never one of them.
_CONFIRMED_ABSENT_DISPOSITIONS = frozenset({
    SourceDisposition.SOURCE_MISSING,
    SourceDisposition.SOURCE_PERMISSION_DENIED,
    SourceDisposition.SOURCE_UNREADABLE,
})


def _serialize_staged_container(item):
    data = dict(item.__dict__)
    for key in ("container_format", "validation_state",
                "database_validation_state"):
        data[key] = _enum_value(data[key])
    return data


def _serialize_staged_artifact(item):
    data = dict(item.__dict__)
    for key in ("artifact_kind", "readiness_state",
                "database_readiness_state"):
        data[key] = _enum_value(data[key])
    return data


class RemoteChunkStager:
    """Fetch + pack one chunk onto local staging. Never touches the tape."""

    def __init__(self, host):
        #: The RemoteOrchestrator façade that owns session state and config.
        self.host = host

    def _start_stored_tar_attempt(self, owner, session_id, chunk_index):
        """Persist process identity so a restart can prove this owner dead."""
        starter = getattr(self.host.db, "start_worker_attempt", None)
        if not callable(starter):
            return None
        started_at = None
        try:
            import psutil
            started_at = psutil.Process(os.getpid()).create_time()
        except Exception:
            # PID absence after a crash remains useful evidence.  If the PID is
            # later reused, missing creation identity correctly blocks reclaim.
            pass
        try:
            return starter(
                owner_token=owner, attempt_kind="stored_tar_build",
                session_id=session_id, chunk_index=chunk_index,
                local_pid=os.getpid(),
                local_process_started_at=started_at)
        except Exception:
            get_logger().warning(
                "could not record a durable Stored TAR build attempt; a crash "
                "will require fail-closed owner reconciliation", exc_info=True)
            return None

    def _finish_stored_tar_attempt(self, attempt_id):
        if attempt_id is None:
            return
        try:
            self.host.db.finish_worker_attempt(attempt_id, "completed")
        except Exception:
            get_logger().warning(
                "could not close Stored TAR build attempt %s", attempt_id,
                exc_info=True)

    def _build_stored_tar_container(
            self, session_id, chunk_index, container_plan, plan_members,
            pack_dir, *, owner_token=None, abort_evt=None, crash_hook=None,
            progress_part_paths=None, progress_lock=None):
        """Build and publish one Task-2.2/2.3/2.4 container pair.

        Task 2.5 routes TAR-assigned chunks here while ZIP chunks stay on the
        existing extraction/packer path.
        """
        owner = str(owner_token or uuid.uuid4().hex)
        members = sorted(plan_members, key=lambda item: int(item.plan_ordinal))
        remote_bases = []
        sidecar_plan = []
        for item in members:
            base, rel = remote_store_base_and_rel(
                self.host.remote_path, item.remote_path)
            remote_bases.append(base)
            sidecar_plan.append({
                "member_name": rel,
                "canonical_source_path": item.remote_path,
                "expected_size": int(item.file_size_bytes),
                "plan_ordinal": int(item.plan_ordinal),
                "container_ordinal": int(container_plan.container_ordinal),
            })
        if not sidecar_plan:
            raise RuntimeError("Stored TAR container plan is empty")
        if len(set(remote_bases)) != 1:
            raise RuntimeError(
                "Stored TAR container members do not share one remote base")

        os.makedirs(_long(pack_dir), exist_ok=True)
        final_tar = os.path.join(pack_dir, container_plan.container_name)
        container_rows = self.host.db.get_archive_containers(
            session_id, chunk_index)
        container_row = next(
            (row for row in container_rows
             if int(row["container_ordinal"])
             == int(container_plan.container_ordinal)), None)
        if container_row is None:
            raise RuntimeError("Stored TAR container identity is absent")
        container_id = int(container_row["container_id"])
        existing_state = container_row.get("validation_state")
        existing_owner = container_row.get("owner_token")
        existing_part = container_row.get("validated_part_locator")
        if (existing_part and container_row.get("validation_state") in (
                "building", "validated_part", "ready")
                and (existing_owner in (None, owner))):
            part_path = existing_part
        else:
            part_path = f"{final_tar}.{uuid.uuid4().hex[:12]}.part"
        claimed = self.host.db.claim_stored_tar_container_build(
            container_id, owner, part_path)
        if progress_part_paths is not None:
            lock = progress_lock or threading.Lock()
            with lock:
                if part_path not in progress_part_paths:
                    progress_part_paths.append(part_path)
        claimed_state = (claimed.get("validation_state")
                         if isinstance(claimed, dict) else existing_state)
        attempt_id = (None if claimed_state == "ready"
                      else self._start_stored_tar_attempt(
                          owner, session_id, chunk_index))
        if existing_state in ("validated_part", "ready"):
            summary = container_row.get("validation_summary") or {}
            diagnostics = tuple(summary.get("source_diagnostics") or ())
            sidecar_path = resolve_locator(
                self.host.cfg.local_manifest_archive_root,
                tar_sidecar_locator(
                    session_id, chunk_index,
                    container_plan.container_ordinal))
            if (existing_state == "validated_part" and diagnostics
                    and os.path.isfile(_long(final_tar))
                    and not os.path.isfile(_long(sidecar_path))):
                # The only durable exception evidence should have preceded the
                # final TAR.  Preserve the anomalous local data as evidence,
                # reset only under the still-current pre-writer owner, and
                # recapture source outcomes instead of inventing them.
                quarantine = (
                    f"{final_tar}.{uuid.uuid4().hex[:12]}.unready")
                os.rename(_long(final_tar), _long(quarantine))
                part_path = f"{final_tar}.{uuid.uuid4().hex[:12]}.part"
                self.host.db.restart_stored_tar_build_from_source(
                    container_id, owner, part_path)
                existing_state = "building"
            else:
                publication = publish_stored_tar_pair(
                    self.host.cfg.local_manifest_archive_root,
                    part_path, final_tar, sidecar_plan, diagnostics,
                    validation=None, session_id=session_id,
                    chunk_index=chunk_index, container_id=container_id,
                    container_ordinal=container_plan.container_ordinal,
                    owner_token=owner, db=self.host.db, pack_dir=pack_dir,
                    crash_hook=crash_hook)
                self._finish_stored_tar_attempt(attempt_id)
                return publication

        retries = max(0, int(getattr(
            self.host, "fetch_transient_retries", 0)))
        retry_base = float(getattr(
            self.host, "fetch_transient_retry_base", 5.0))
        result = None
        for attempt in range(retries + 1):
            if attempt or (existing_state == "building"
                           and os.path.exists(_long(part_path))):
                # Ownership was established before the first byte.  A transient
                # retry discards only that owner's unpublished partial stream and
                # restarts at byte zero.
                try:
                    os.remove(_long(part_path))
                except FileNotFoundError:
                    pass
            result = _remote_tar_store(
                self.host.remote_user, self.host.remote_host, remote_bases[0],
                [item["member_name"] for item in sidecar_plan], part_path,
                plan_ordinals=[item["plan_ordinal"] for item in sidecar_plan],
                password=self.host.remote_password, cipher=self.host.ssh_cipher,
                use_mbuffer=self.host.use_mbuffer,
                mbuffer_size=self.host.mbuffer_size,
                fetch_cores=self.host.fetch_cores, abort_evt=abort_evt)
            if result.ok:
                break
            detail = "\n".join(filter(None, (result.error, result.stderr)))
            if (attempt >= retries or result.cancelled
                    or not _is_transient_fetch_error(detail)):
                self.host._note_fetch_failure(detail)
                raise RuntimeError(f"direct Stored TAR stream failed: {detail}")
            delay = self.host._fetch_backoff_delay(attempt, retry_base)
            self.host._note_fetch_failure(
                detail, retry_attempt=attempt + 1,
                next_retry_delay=delay)
            if abort_evt is not None and abort_evt.wait(delay):
                raise RuntimeError("direct Stored TAR stream cancelled")
            if CANCEL.is_set():
                raise RuntimeError("direct Stored TAR stream cancelled")
        assert result is not None and result.ok

        validation = validate_stored_tar_part(
            part_path, sidecar_plan,
            container_ordinal=container_plan.container_ordinal,
            source_diagnostics=result.diagnostics,
            tar_dialect=container_plan.tar_dialect,
            format_version=container_plan.format_version)
        self.host.db.mark_stored_tar_validated_part(
            container_id, owner, part_path, validation,
            result.diagnostics)
        if crash_hook is not None:
            crash_hook("after_validated_part_state")
        publication = publish_stored_tar_pair(
            self.host.cfg.local_manifest_archive_root,
            part_path, final_tar, sidecar_plan, result.diagnostics,
            validation=validation, session_id=session_id,
            chunk_index=chunk_index, container_id=container_id,
            container_ordinal=container_plan.container_ordinal,
            owner_token=owner, db=self.host.db, pack_dir=pack_dir,
            crash_hook=crash_hook)
        self._finish_stored_tar_attempt(attempt_id)
        return publication

    @staticmethod
    def _source_diagnostic_ordinals(diagnostics):
        ordinals = set()
        for item in diagnostics or ():
            if isinstance(item, dict):
                ordinal = item.get("plan_ordinal")
            else:
                ordinal = getattr(item, "plan_ordinal", None)
            if ordinal is not None:
                ordinals.add(int(ordinal))
        return ordinals

    @staticmethod
    def _safe_artifact_stage_name(row):
        kind = str(row["artifact_kind"])
        artifact_id = int(row["artifact_id"])
        locator = str(row.get("local_locator") or "")
        base = os.path.basename(locator.replace("\\", "/")) or kind
        return f"_artifact_{artifact_id:08d}_{base}"

    def _copy_ready_artifact_to_pack(self, row, pack_dir):
        src = resolve_locator(
            self.host.cfg.local_manifest_archive_root, row["local_locator"])
        if not os.path.isfile(_long(src)):
            raise RuntimeError(
                f"ready artifact is missing locally: {row['local_locator']}")
        dst = os.path.join(pack_dir, self._safe_artifact_stage_name(row))
        if os.path.exists(_long(dst)):
            if os.path.getsize(_long(dst)) != os.path.getsize(_long(src)):
                raise RuntimeError(f"pack artifact conflicts: {dst}")
            return dst
        part = f"{dst}.{uuid.uuid4().hex[:12]}.part"
        os.makedirs(_long(os.path.dirname(dst)), exist_ok=True)
        try:
            with open(_long(src), "rb") as source, open(_long(part), "xb") as target:
                shutil.copyfileobj(source, target, 1024 * 1024)
                target.flush()
                os.fsync(target.fileno())
            os.replace(_long(part), _long(dst))
        finally:
            if os.path.exists(_long(part)):
                try:
                    os.remove(_long(part))
                except OSError:
                    pass
        return dst

    def _stored_tar_metadata_for_container(
            self, container_plan, plan_members, publication, container_row,
            artifact_row):
        excluded = self._source_diagnostic_ordinals(
            getattr(publication, "source_diagnostics", ()))
        records = []
        threshold_bytes = int(float(
            getattr(self.host.cfg, 'index_min_file_mb', 10) or 10)
                              * 1024 * 1024)
        index_all = bool(getattr(
            self.host.cfg, 'index_packed_small_files', False))
        for member in sorted(plan_members, key=lambda item: int(item.plan_ordinal)):
            if int(member.plan_ordinal) in excluded:
                continue
            records.append({
                "file_name": os.path.basename(str(member.remote_path)),
                "original_path": str(member.remote_path),
                "file_size_bytes": int(member.file_size_bytes),
                "is_packed": True,
                "container_name": container_plan.container_name,
                "stored_path": str(member.remote_path),
                "canonical_source_path": str(member.remote_path),
                "catalog_policy": (
                    "index" if index_all
                    or int(member.file_size_bytes) >= threshold_bytes
                    else "manifest_only"),
                "container_id": int(container_row["container_id"]),
                "container_format": ContainerFormat.STORED_TAR.value,
                "container_ordinal": int(container_plan.container_ordinal),
                "artifact_id": int(artifact_row["artifact_id"]),
                "artifact_kind": ArtifactKind.TAR_SIDECAR.value,
                "artifact_version": str(artifact_row["artifact_version"]),
            })
        return records

    def _stored_tar_rows_after_publication(
            self, session_id, chunk_index, container_id):
        containers = {
            int(row["container_id"]): row
            for row in self.host.db.get_archive_containers(session_id, chunk_index)
        }
        artifacts = {
            int(row["container_id"]): row
            for row in self.host.db.get_archive_artifacts(session_id, chunk_index)
            if row.get("container_id") is not None
            and row["artifact_kind"] == ArtifactKind.TAR_SIDECAR.value
            and row["readiness_state"] == ArtifactReadiness.READY.value
        }
        container = containers.get(int(container_id))
        artifact = artifacts.get(int(container_id))
        if container is None or artifact is None:
            raise RuntimeError(
                "Stored TAR publication did not produce durable ready rows")
        return container, artifact

    def _stored_tar_rows_after_publication_by_ordinal(
            self, session_id, chunk_index, container_ordinal):
        rows = self.host.db.get_archive_containers(session_id, chunk_index)
        match = next(
            (row for row in rows
             if int(row["container_ordinal"]) == int(container_ordinal)),
            None)
        if match is None:
            raise RuntimeError("Stored TAR container identity is absent")
        return self._stored_tar_rows_after_publication(
            session_id, chunk_index, int(match["container_id"]))

    def _stage_stored_tar_chunk(
            self, session_id, chunk_index, chunk_files, chunk_plan,
            fetch_dir, pack_dir, ram_stats, fetch_seconds, fetch_bytes,
            abort_evt=None, progress_part_paths=None, progress_lock=None):
        self.host.db.update_chunk_status(
            session_id, chunk_index, ChunkStatus.PACKING.value)
        # Task 2.6 may have adopted a crash-published TAR/sidecar pair in this
        # deterministic pack.  Reconciliation has already validated every
        # surviving local member; deleting the directory here would discard
        # the very no-refetch recovery it just proved safe.
        os.makedirs(_long(pack_dir), exist_ok=True)
        _phase('PACK', f"Staging Stored TAR chunk {chunk_index + 1}: "
                       "small files -> TAR, large files staged loose")
        pack_start = time.perf_counter()

        source_missing_files = [
            {
                "manifest_id": item.manifest_id,
                "remote_path": item.remote_path,
                "file_size_bytes": item.file_size_bytes,
            }
            for item in chunk_plan.source_missing_members
        ]
        for item in source_missing_files:
            self.host.skipped_tracker.add(
                'remote', item["remote_path"], "source missing",
                'fetch', session_id=session_id, chunk_index=chunk_index)

        metadata = []
        staged_containers = []
        staged_artifacts = []
        members_by_container = defaultdict(list)
        for member in chunk_plan.small_members:
            members_by_container[int(member.container_ordinal)].append(member)

        try:
            for container_plan in chunk_plan.containers:
                members = members_by_container[int(container_plan.container_ordinal)]
                publication = self._build_stored_tar_container(
                    session_id, chunk_index, container_plan, members, pack_dir,
                    abort_evt=abort_evt,
                    progress_part_paths=progress_part_paths,
                    progress_lock=progress_lock)
                container_row, artifact_row = (
                    self._stored_tar_rows_after_publication_by_ordinal(
                        session_id, chunk_index,
                        container_plan.container_ordinal))
                metadata.extend(self._stored_tar_metadata_for_container(
                    container_plan, members, publication, container_row,
                    artifact_row))
                staged_containers.append(StagedContainer(
                    container_id=int(container_row["container_id"]),
                    session_id=session_id,
                    chunk_index=chunk_index,
                    container_ordinal=int(container_row["container_ordinal"]),
                    container_format=ContainerFormat.STORED_TAR,
                    format_version=str(container_row["format_version"]),
                    storage_class=str(container_row["storage_class"]),
                    container_name=str(container_row["container_name"]),
                    data_path=str(container_row["temporary_data_locator"]),
                    temporary_data_locator=str(
                        container_row["temporary_data_locator"]),
                    data_size_bytes=int(container_row["actual_artifact_bytes"]),
                    expected_member_count=int(
                        container_row["expected_member_count"]),
                    expected_logical_bytes=int(
                        container_row["expected_logical_bytes"]),
                    observed_member_count=int(
                        container_row["observed_member_count"]),
                    observed_logical_bytes=int(
                        container_row["observed_logical_bytes"]),
                    permanent_local_metadata_locator=(
                        container_row["permanent_local_metadata_locator"]),
                    validation_state=ContainerValidationState.READY,
                    database_validation_state=ContainerValidationState.READY,
                    tar_dialect=str(container_row["tar_dialect"]),
                ))
                staged_artifacts.append(StagedArtifact(
                    artifact_id=int(artifact_row["artifact_id"]),
                    session_id=session_id,
                    chunk_index=chunk_index,
                    container_id=int(container_row["container_id"]),
                    artifact_kind=ArtifactKind.TAR_SIDECAR,
                    artifact_version=str(artifact_row["artifact_version"]),
                    staged_path=publication.pack_sidecar_path,
                    local_locator=str(artifact_row["local_locator"]),
                    staged_size_bytes=int(artifact_row["artifact_size_bytes"]),
                    readiness_state=ArtifactReadiness.READY,
                    database_readiness_state=ArtifactReadiness.READY,
                ))

            for artifact_row in self.host.db.get_archive_artifacts(
                    session_id, chunk_index):
                if artifact_row.get("container_id") is not None:
                    continue
                if artifact_row["artifact_kind"] not in {
                        ArtifactKind.PLAN_MANIFEST.value,
                        ArtifactKind.TERMINAL_MANIFEST.value}:
                    continue
                if artifact_row["readiness_state"] != ArtifactReadiness.READY.value:
                    continue
                staged_path = self._copy_ready_artifact_to_pack(
                    artifact_row, pack_dir)
                staged_artifacts.append(StagedArtifact(
                    artifact_id=int(artifact_row["artifact_id"]),
                    session_id=session_id,
                    chunk_index=chunk_index,
                    container_id=None,
                    artifact_kind=ArtifactKind(artifact_row["artifact_kind"]),
                    artifact_version=str(artifact_row["artifact_version"]),
                    staged_path=staged_path,
                    local_locator=str(artifact_row["local_locator"]),
                    staged_size_bytes=int(artifact_row["artifact_size_bytes"]),
                    readiness_state=ArtifactReadiness.READY,
                    database_readiness_state=ArtifactReadiness.READY,
                ))

            loose_members = list(chunk_plan.loose_members)
            if loose_members:
                loose_ids = {int(item.manifest_id) for item in loose_members}
                loose_rows = [
                    row for row in chunk_files
                    if int(row["manifest_id"]) in loose_ids]
                fetch_ok, loose_missing, loose_count = self.host._fetch_chunk(
                    session_id, chunk_index, loose_rows, fetch_dir)
                source_missing_files.extend(loose_missing)
                if not fetch_ok:
                    raise RuntimeError("large-file fetch failed")
                # Re-read the rows: only the durable copy carries the
                # local_rel_path the fetch just recorded.
                reader = getattr(self.host.db, "get_chunk_files", None)
                fresh_rows = (
                    [row for row in reader(session_id, chunk_index)
                     if row['manifest_id'] is not None
                     and int(row['manifest_id']) in loose_ids]
                    if callable(reader) else loose_rows)
                loose_metadata = []
                if loose_count:
                    self._assert_loose_targets_free(fetch_dir, pack_dir)
                    packer = LTOPacker(
                        self.host.cfg.max_zip_size_gb,
                        index_min_file_mb=self.host.cfg.index_min_file_mb,
                        index_packed_small_files=(
                            self.host.cfg.index_packed_small_files),
                        manifest_enabled=False,
                    )
                    # 'merge', not 'reuse': the pack already holds this
                    # chunk's TAR/sidecar/manifest artifacts. 'reuse' returned
                    # an empty list for any non-empty pack, so the large files
                    # were never copied and were then deleted with the fetch
                    # dir (audit H12).
                    loose_metadata = packer.run(
                        source=fetch_dir,
                        dest=pack_dir,
                        threshold_mb=0,
                        skipped_tracker=self.host.skipped_tracker,
                        source_name='remote',
                        session_id=session_id,
                        chunk_index=chunk_index,
                        on_existing='merge',
                        governor=getattr(self.host, 'governor', None),
                        pack_file_batch_size=self.host.pack_file_batch_size,
                        pack_parallel_workers=1,
                    )
                # Same gate as the ZIP path (audit H7): every planned large
                # file is positively missing or in the pack at its size.
                problems = reconcile_packed_chunk(
                    loose_rows, loose_missing, loose_count, fresh_rows,
                    loose_metadata or [], pack_dir=pack_dir)
                if problems:
                    raise ChunkReconciliationError(
                        f"[PACK] Chunk {chunk_index + 1} large files failed "
                        "reconciliation and were NOT queued for tape: "
                        + "; ".join(problems[:5]))
                if loose_count:
                    canonical_count = _apply_canonical_remote_paths(
                        loose_metadata, fresh_rows)
                    if canonical_count != len(loose_metadata):
                        raise RuntimeError(
                            "[DB] Refusing to index temporary loose staging "
                            "paths: canonical SOURCE paths mapped for only "
                            f"{canonical_count:,}/{len(loose_metadata):,} "
                            "staged file(s).")
                    metadata.extend(loose_metadata)

            ready_object_count = len(staged_containers) + sum(
                1 for item in metadata if not item.get("is_packed"))
            if ready_object_count == 0:
                if source_missing_files:
                    self.host._cleanup_dir(fetch_dir)
                    self.host._cleanup_dir(pack_dir)
                    _status('REMOTE', f"Chunk {chunk_index + 1}: all source "
                                      "files are missing; no tape write is "
                                      "required.")
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
                        packaging_format=ContainerFormat.STORED_TAR,
                    )
                raise RuntimeError("Stored TAR chunk produced no ready objects")

            pack_seconds = time.perf_counter() - pack_start
            self.host._cleanup_dir(fetch_dir)
            gc.collect()
            staged_bytes = _dir_tree_size(pack_dir)
            desc = StagedChunk(
                chunk_index=chunk_index,
                fetch_dir=fetch_dir,
                pack_dir=pack_dir,
                metadata=metadata,
                staged_bytes=staged_bytes,
                fetch_seconds=fetch_seconds,
                fetch_bytes=fetch_bytes,
                pack_seconds=pack_seconds,
                pack_bytes=staged_bytes,
                ram_stats=ram_stats,
                source_missing_files=source_missing_files,
                skip_tape=False,
                session_id=session_id,
                packaging_format=ContainerFormat.STORED_TAR,
                containers=staged_containers,
                artifacts=staged_artifacts,
            )
            desc.assert_writer_ready()
            with self.host._staged_lock:
                self.host._staged_bytes += staged_bytes
            _status('PIPELINE', f"Chunk {chunk_index + 1} staged & ready "
                                f"({staged_bytes / 1024**3:.1f} GB) - queued for tape.")
            return desc
        except StagingSpaceError:
            raise
        except Exception:
            if not CANCEL.is_set():
                self.host.db.update_chunk_status(
                    session_id, chunk_index, ChunkStatus.FETCH_FAILED.value)
            if abort_evt is not None and abort_evt.is_set():
                self.host._cleanup_dir(fetch_dir)
            else:
                self.host._cleanup_dir(fetch_dir)
                self.host._cleanup_dir(pack_dir)
            raise

    @staticmethod
    def _assert_loose_targets_free(fetch_dir, pack_dir):
        """Refuse to copy a large file over an object already in the pack.

        Loose files keep their source-relative path inside the pack, next to
        the chunk's containers and metadata artifacts. A source file whose
        path equals one of those names must stop the chunk, not replace it."""
        for root, _dirs, files in os.walk(_long(fetch_dir)):
            for name in files:
                rel = os.path.relpath(os.path.join(root, name),
                                      _long(fetch_dir))
                target = os.path.join(pack_dir, rel)
                if os.path.lexists(_long(target)):
                    raise RuntimeError(
                        "[PACK] large file would overwrite an existing pack "
                        f"object: {rel}")

    def _reservation_for_chunk(
            self, planned_bytes, planned_files, *, session_id=None,
            chunk_index=None, chunk_files=None):
        packaging_format = ContainerFormat.ZIP
        if session_id is not None and chunk_index is not None:
            reader = getattr(self.host.db, "get_chunk_packaging_format", None)
            if callable(reader):
                packaging_format = ContainerFormat(
                    reader(session_id, chunk_index))
        if packaging_format is not ContainerFormat.STORED_TAR:
            return zip_staging_reservation(
                planned_bytes, planned_files, self.host)

        if chunk_files is None:
            chunk_files = self.host.db.get_chunk_files(session_id, chunk_index)
        planner = getattr(
            self.host.db, "get_or_create_stored_tar_chunk_plan", None)
        if not callable(planner):
            raise RuntimeError(
                "[STAGING] Stored TAR chunk has no persisted container-plan "
                "repository; refusing admission")
        chunk_plan = planner(
            session_id, chunk_index, chunk_files,
            loose_threshold_bytes=int(
                self.host.cfg.zip_threshold_mb * 1024 * 1024),
            max_size_bytes=int(
                self.host.cfg.stored_tar_max_size_gb * 1024**3),
        )
        return stored_tar_staging_reservation(chunk_plan, self.host)

    def _await_staging_capacity(
            self, planned_bytes, planned_files, stop_evt, ready_q=None,
            session_id=None, chunk_index=None, chunk_files=None,
            reservation=None):
        """Block until a ZIP or TAR chunk fits the local staging envelope."""
        if reservation is None:
            reservation = self._reservation_for_chunk(
                planned_bytes, planned_files, session_id=session_id,
                chunk_index=chunk_index, chunk_files=chunk_files)
        need = int(reservation.needed_bytes)
        floor = LOCAL_STAGING_RESERVE_BYTES
        warned = False
        while not (CANCEL.is_set() or stop_evt.is_set()):
            with self.host._staged_lock:
                resident = self.host._staged_bytes
            self.host._apply_staging_pressure(ready_q, resident, need)
            try:
                free = shutil.disk_usage(self.host.staging_dir).free
            except OSError:
                free = need + floor
            governor = getattr(self.host, 'governor', None)
            if governor:
                if not warned:
                    _status('PIPELINE',
                            "Backpressure - waiting for RAM/staging/tape "
                            "governor before fetching the next chunk.")
                    warned = True
                if not governor.wait_or_pause(
                        "fetch", "start", needed_bytes=need,
                        queued_bytes=resident, stop_evt=stop_evt):
                    return
            room_cap = (resident + need) <= self.host.staging_max_bytes
            room_disk = (free - need) >= floor
            if not room_disk:
                raise RuntimeError(
                    "Insufficient local staging space for remote chunk. "
                    f"Need {need / 1024**3:.2f} GiB peak staging + "
                    f"{floor / 1024**3:.0f} GiB reserve; current free on "
                    f"'{self.host.staging_dir}': {free / 1024**3:.2f} GiB."
                )
            alone = (resident == 0)
            if room_cap or alone:
                return reservation
            if not warned:
                _status('PIPELINE',
                        f"Backpressure - {resident / 1024**3:.0f} GB staged, "
                        f"waiting for the tape to drain before fetching the next "
                        f"chunk (cap {self.host.staging_max_bytes / 1024**3:.0f} GB).")
                warned = True
            time.sleep(2)
        return reservation

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
        fetch_dir = os.path.join(
            self.host.staging_dir, f"_fetch_s{session_id:04d}_{chunk_index:03d}")
        pack_dir  = os.path.join(
            self.host.staging_dir, f"_pack_s{session_id:04d}_{chunk_index:03d}")
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
            chunk_plan = planner(
                session_id, chunk_index, chunk_files,
                loose_threshold_bytes=int(
                    self.host.cfg.zip_threshold_mb * 1024 * 1024),
                max_size_bytes=int(
                    self.host.cfg.stored_tar_max_size_gb * 1024**3),
            )
            from .startup_reconcile import (TAR_RECONCILE_BLOCKED,
                                            reconcile_tar_artifacts)
            if chunk_plan.containers:
                reconcile_report = reconcile_tar_artifacts(
                    self.host.db, session_id, chunk_index,
                    archive_root=self.host.cfg.local_manifest_archive_root,
                    pack_dir=pack_dir,
                    remote_base=getattr(self.host, "remote_path", None),
                    owner_probe=getattr(
                        self.host, "stored_tar_owner_probe", None),
                    plan=chunk_plan)
            else:
                # All-source-missing/loose-only plans have no TAR artifact pair
                # to reconcile.  Their existing skip/loose path remains the
                # authority and performs no LTFS operation.
                reconcile_report = {
                    "verdict": "ready", "blocking": [], "cases": []}
            if reconcile_report["verdict"] == TAR_RECONCILE_BLOCKED:
                raise RuntimeError(
                    "[STAGING] Stored TAR startup reconciliation blocked the "
                    "chunk: " + "; ".join(reconcile_report["blocking"]))
            preserve_reconciled = any(
                item.get("action") in (
                    "adopted_pair", "none", "quarantined_equivalent_part")
                and item.get("container_id") is not None
                for item in reconcile_report["cases"])
            if (not preserve_reconciled and os.path.isdir(pack_dir)):
                self.host._cleanup_dir(pack_dir)
            resumed = self.host._try_resume_pack(
                session_id, chunk_index, pack_dir)
            if resumed is not None:
                return resumed
            self.host.db.update_chunk_status(session_id, chunk_index,
                    ChunkStatus.FETCHING.value)
            fetch_start = time.perf_counter()
            ram_stats = {}
            reservation = stored_tar_staging_reservation(chunk_plan, self.host)
            governor = getattr(self.host, 'governor', None)
            if governor:
                governor.wait_or_pause(
                    "fetch", "start",
                    needed_bytes=reservation.needed_bytes)
                fetch_guard = governor.mark_fetch_active()
            else:
                fetch_guard = None
            fetch_stop = threading.Event()
            fetch_abort = threading.Event()
            progress_lock = threading.Lock()
            progress_part_paths = []
            self._start_fetch_monitor(
                fetch_stop, fetch_abort, fetch_dir,
                reservation.tar_stream_bytes + reservation.loose_bytes,
                tar_part_paths=progress_part_paths,
                tar_part_paths_lock=progress_lock,
                label="STORED_TAR")
            with RamStageSampler(
                    "fetch", self.host.ram_sample_interval) as fetch_sampler:
                try:
                    if fetch_guard:
                        with fetch_guard:
                            desc = self._stage_stored_tar_chunk(
                                session_id, chunk_index, chunk_files, chunk_plan,
                                fetch_dir, pack_dir, ram_stats, 0, 0,
                                abort_evt=fetch_abort,
                                progress_part_paths=progress_part_paths,
                                progress_lock=progress_lock)
                    else:
                        desc = self._stage_stored_tar_chunk(
                            session_id, chunk_index, chunk_files, chunk_plan,
                            fetch_dir, pack_dir, ram_stats, 0, 0,
                            abort_evt=fetch_abort,
                            progress_part_paths=progress_part_paths,
                            progress_lock=progress_lock)
                finally:
                    fetch_stop.set()
            ram_stats.update(fetch_sampler.as_details("fetch"))
            if desc is not None:
                desc.fetch_seconds = time.perf_counter() - fetch_start
                desc.fetch_bytes = _dir_tree_size(fetch_dir)
                desc.ram_stats.update(ram_stats)
            return desc
        resumed = self.host._try_resume_pack(session_id, chunk_index, pack_dir)
        if resumed is not None:
            return resumed
        if os.path.isdir(pack_dir):
            get_logger().warning(
                "clearing an unusable leftover pack dir before repacking "
                "chunk %s: %s", chunk_index + 1, pack_dir)
            self.host._cleanup_dir(pack_dir)
        # The session id is embedded so the on-tape root (basename(pack_dir),
        # see LTOBackup._run_locked) is unique per session — two sessions on the
        # same tape never collide on '_pack_NNN'. Resuming a session reuses the
        # same deterministic names, so robocopy still same-size-skips.
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
        manifest_rows = self.host.db.get_chunk_files(session_id, chunk_index)
        # The gate between packing and the tape writer (audit H7): the pack
        # must hold exactly the fetched files. Checked before the staging
        # paths are rewritten, while stored_path still names the fetched file.
        self._require_reconciled_pack(
            session_id, chunk_index, chunk_files, source_missing_files,
            fetched_file_count, manifest_rows, metadata, fetch_dir, pack_dir)
        canonical_count = _apply_canonical_remote_paths(
            metadata, manifest_rows)
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

    def _require_reconciled_pack(
            self, session_id, chunk_index, planned_rows, source_missing_files,
            fetched_file_count, manifest_rows, metadata, fetch_dir, pack_dir):
        """Fail the chunk unless plan, fetch and pack agree file for file.

        On a mismatch the chunk is marked ``fetch_failed`` (resumable), the
        raw fetch is dropped, and the pack is set aside under a
        ``.reconcile_failed`` name for diagnosis: it is never handed to the
        writer and can never be picked up as a resumable pack."""
        problems = reconcile_packed_chunk(
            planned_rows, source_missing_files, fetched_file_count,
            manifest_rows, metadata, pack_dir=pack_dir)
        if not problems:
            return
        kept = self._quarantine_failed_pack(pack_dir)
        if not CANCEL.is_set():
            self.host.db.update_chunk_status(
                session_id, chunk_index, ChunkStatus.FETCH_FAILED.value)
        self.host._cleanup_dir(fetch_dir)
        shown = "; ".join(problems[:5])
        more = (f" (+{len(problems) - 5} more)" if len(problems) > 5 else "")
        message = (
            f"[PACK] Chunk {chunk_index + 1} failed reconciliation and was "
            f"NOT queued for tape: {shown}{more}. "
            + (f"The pack is kept for diagnosis at {kept}."
               if kept else "The pack could not be set aside and was left "
                            f"at {pack_dir}."))
        get_logger().error("chunk_reconciliation_failed: %s", message)
        print(f"\n{message}")
        raise ChunkReconciliationError(message)

    def _quarantine_failed_pack(self, pack_dir):
        """Rename a rejected pack out of the writer's and resume's reach."""
        if not os.path.isdir(_long(pack_dir)):
            return None
        kept = pack_dir + ".reconcile_failed"
        try:
            if os.path.exists(_long(kept)):
                # Keep at most one rejected pack per chunk so repeated
                # failures cannot fill the staging volume.
                shutil.rmtree(_long(kept))
            os.rename(_long(pack_dir), _long(kept))
        except OSError:
            get_logger().warning(
                "could not set aside the rejected pack %s", pack_dir,
                exc_info=True)
            return None
        return kept

    def _discard_desc(self, desc):
        """Drop a staged-but-unused chunk: clean its dirs and free its budget."""
        self.host._cleanup_dir(desc.fetch_dir)
        self.host._cleanup_dir(desc.pack_dir)
        with self.host._staged_lock:
            self.host._staged_bytes = max(0, self.host._staged_bytes - desc.staged_bytes)

    def _pack_inventory(self, pack_dir):
        """(name, size) for every file under pack_dir, sorted. The integrity basis."""
        return _pack_inventory_readonly(pack_dir)

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
                "packaging_format": desc.packaging_format.value,
                "metadata": desc.metadata,
                "containers": [
                    _serialize_staged_container(item)
                    for item in desc.containers
                ],
                "artifacts": [
                    _serialize_staged_artifact(item)
                    for item in desc.artifacts
                ],
                "artifact_inventory": {
                    "containers": [
                        {
                            "container_id": item.container_id,
                            "container_ordinal": item.container_ordinal,
                            "container_name": item.container_name,
                            "data_path": item.data_path,
                            "data_size_bytes": item.data_size_bytes,
                            "validation_state": item.validation_state.value,
                        }
                        for item in desc.containers
                    ],
                    "artifacts": [
                        {
                            "artifact_id": item.artifact_id,
                            "container_id": item.container_id,
                            "artifact_kind": item.artifact_kind.value,
                            "staged_path": item.staged_path,
                            "local_locator": item.local_locator,
                            "staged_size_bytes": item.staged_size_bytes,
                            "readiness_state": item.readiness_state.value,
                        }
                        for item in desc.artifacts
                    ],
                },
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
        marker_report, payload = _read_resume_pack_marker(
            pack_dir, session_id, chunk_index)
        if marker_report["marker_state"] in ("unreadable",
                                               "pack_directory_unreadable"):
            get_logger().warning(
                "resume marker for chunk %s is unreadable; re-fetching",
                chunk_index + 1)
            return None
        if marker_report["marker_state"] == "identity_mismatch":
            get_logger().warning(
                "resume marker at %s does not match session %s chunk %s; "
                "re-fetching", pack_dir, session_id, chunk_index + 1)
            return None
        if marker_report["marker_state"] == "inventory_mismatch":
            get_logger().warning(
                "preserved pack for chunk %s failed its integrity check "
                "(inventory changed); re-fetching", chunk_index + 1)
            print(f"[REMOTE] Preserved pack for chunk {chunk_index + 1} failed "
                  f"its integrity check — re-fetching it.")
            return None
        if marker_report["marker_state"] != "valid" or payload is None:
            return None

        try:
            marker_format = ContainerFormat(
                payload.get("packaging_format", ContainerFormat.ZIP.value))
        except ValueError:
            get_logger().warning(
                "resume marker for chunk %s has an unknown format; re-fetching",
                chunk_index + 1)
            return None
        reader = getattr(
            getattr(self.host, "db", None), "get_chunk_packaging_format", None)
        if not callable(reader):
            get_logger().warning(
                "resume marker for chunk %s cannot be checked against durable "
                "format; re-fetching", chunk_index + 1)
            return None
        try:
            durable_format = ContainerFormat(reader(session_id, chunk_index))
        except (TypeError, ValueError):
            get_logger().warning(
                "resume marker for chunk %s found no durable format; re-fetching",
                chunk_index + 1)
            return None
        if durable_format is not marker_format:
            get_logger().warning(
                "resume marker for chunk %s is %s but durable format is %s; "
                "re-fetching", chunk_index + 1, marker_format.value,
                durable_format.value)
            return None

        # The marker authorizes reuse but is not archive payload.  Consume it
        # before the descriptor reaches the writer's exact-pack validator, so
        # it can never be copied to tape as an undeclared extra file.
        try:
            os.remove(marker)
        except OSError:
            get_logger().warning(
                "validated resume marker for chunk %s could not be consumed; "
                "refusing reuse", chunk_index + 1, exc_info=True)
            return None

        containers = [
            StagedContainer(**item)
            for item in payload.get("containers") or []
        ]
        artifacts = [
            StagedArtifact(**item)
            for item in payload.get("artifacts") or []
        ]

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
            packaging_format=marker_format,
            containers=containers,
            artifacts=artifacts,
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
        claimed = {}        # collision key of local_rel -> owning remote rel
        fetching_ids = []
        chunk_files = list(chunk_files)

        # A row already marked source_missing is only an earlier run's opinion.
        # Ask the source again before letting the chunk go on without the
        # file: an older build recorded files as missing that were merely
        # extracted under a different Unicode form (audit H9).
        recorded_missing = [
            (index, row) for index, row in enumerate(chunk_files)
            if row['status'] == 'source_missing']
        still_missing = set()
        if recorded_missing:
            by_base = defaultdict(list)
            for index, row in recorded_missing:
                try:
                    remote_base, rel = _remote_fetch_base_and_rel(
                        self.host.remote_path, row['remote_path'])
                except ValueError:
                    # Unaddressable by the fetch path: nothing to re-ask.
                    still_missing.add(index)
                    continue
                by_base[remote_base].append((index, rel))
            for remote_base, items in by_base.items():
                verdicts, error = self._confirm_source_absence(
                    remote_base, [rel for _, rel in items])
                if error:
                    print(f"\n[REMOTE] Could not re-confirm recorded missing "
                          f"source files: {error}")
                    self.host._note_fetch_failure(error)
                    return False, source_missing_files, fetched_file_count
                for position, (index, rel) in enumerate(items):
                    if verdicts.get(position) in _CONFIRMED_ABSENT_DISPOSITIONS:
                        still_missing.add(index)
                    else:
                        print(f"[REMOTE] Recorded as missing but present at "
                              f"the source - fetching it now: {rel}")

        for row_index, row in enumerate(chunk_files):
            remote_fpath = row['remote_path']
            fsize        = row['file_size_bytes']
            manifest_id  = row['manifest_id']

            if row['status'] == 'source_missing' and row_index in still_missing:
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
            key = _local_collision_key(local_rel)
            collided = key in claimed
            if collided:
                # Two distinct remote names map to the same on-disk path —
                # rename this one so neither file is silently overwritten.
                clash_with = claimed[key]
                local_rel  = _disambiguate_local_rel(local_rel, claimed)
                key        = _local_collision_key(local_rel)
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
        listing_cache = {}
        absent = []         # locally absent; needs the source's confirmation
        for row, remote_base, rel, local_rel, local_path in records:
            fsize       = row['file_size_bytes']
            manifest_id = row['manifest_id']
            if manifest_id is not None and manifest_id in source_missing_ids:
                continue
            if not os.path.exists(_long(local_path)):
                # The extractor may have written the name in another Unicode
                # normalisation form (libarchive composes to NFC on Windows;
                # macOS-origin names are decomposed). Look for that before
                # believing the file is gone.
                found, found_rel, ambiguous = _resolve_extracted_path(
                    fetch_dir, local_rel, listing_cache)
                if ambiguous:
                    msg = (f"refusing to guess which staged file is "
                           f"'{row['remote_path']}': several local names "
                           "differ from it only by Unicode normalisation.")
                    print(f"\n[REMOTE] {msg}")
                    self.host.db.update_manifest_row(
                        manifest_id, session_id=session_id,
                        status='fetch_failed', error_msg=msg[:500])
                    return False, source_missing_files, fetched_file_count
                if found is not None:
                    local_path, local_rel = found, found_rel
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
                absent.append((row, remote_base, rel))
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

        # "Not on disk" is not evidence about the source. A file is recorded
        # source_missing only when the remote host itself says so; otherwise
        # the chunk fails loudly and stays resumable, because completing it
        # would archive the chunk without a file that still exists.
        if absent and not self._record_confirmed_absent(
                session_id, chunk_index, absent, source_missing_files):
            return False, source_missing_files, fetched_file_count

        for start in range(0, len(fetched_updates), self.host.metadata_batch_size):
            if governor := getattr(self.host, 'governor', None):
                governor.wait_or_pause("fetch", "continue")
            self.host.db.update_manifest_rows_fetched(
                fetched_updates[start:start + self.host.metadata_batch_size],
                session_id=session_id)
        fetched_file_count = len(fetched_updates)
        return True, source_missing_files, fetched_file_count

    def _confirm_source_absence(self, remote_base, rel_paths, abort_evt=None):
        """Ask the source host what became of ``rel_paths``.

        Returns ``(verdicts, error)``. ``verdicts`` maps the position of a
        path in ``rel_paths`` to its :class:`SourceDisposition` for every path
        the remote reports as anything other than a present, readable regular
        file; a position that is absent from the mapping is *present*.
        ``error`` is a message when the question could not be answered, in
        which case nothing may be concluded."""
        rel_paths = list(rel_paths)
        if not rel_paths:
            return {}, None
        try:
            diagnostics, unresolved = _remote_source_status_probe(
                self.host.remote_user, self.host.remote_host, remote_base,
                list(enumerate(rel_paths)),
                password=self.host.remote_password,
                cipher=self.host.ssh_cipher,
                fetch_cores=self.host.fetch_cores, abort_evt=abort_evt)
        except Exception as exc:  # noqa: BLE001 - reported, never guessed
            return {}, f"source status probe failed: {exc}"
        if unresolved:
            return {}, "; ".join(str(item) for item in unresolved)
        verdicts = {}
        for item in diagnostics:
            position = int(item.plan_ordinal)
            if not 0 <= position < len(rel_paths) \
                    or item.path != rel_paths[position]:
                return {}, ("source status probe answered for a path that "
                            "was not asked about")
            verdicts[position] = item.disposition
        return verdicts, None

    def _record_confirmed_absent(self, session_id, chunk_index, absent,
                                 source_missing_files):
        """Record locally-absent files as source_missing only on proof.

        ``absent`` is ``[(row, remote_base, rel), ...]``. Returns False (after
        marking the offending rows ``fetch_failed``) when the source could not
        be asked or reports any of them as still present."""
        by_base = defaultdict(list)
        for row, remote_base, rel in absent:
            by_base[remote_base].append((row, rel))
        confirmed = []
        for remote_base, items in by_base.items():
            verdicts, error = self._confirm_source_absence(
                remote_base, [rel for _, rel in items])
            if error:
                msg = (f"{len(items)} file(s) did not arrive and the source "
                       f"could not confirm they are gone: {error}")
                print(f"\n[REMOTE] {msg}")
                self.host._note_fetch_failure(error)
                self.host.db.update_manifest_rows_fetch_failed(
                    (row['manifest_id'] for row, _ in items), msg[:500],
                    session_id=session_id)
                return False
            for position, (row, rel) in enumerate(items):
                disposition = verdicts.get(position)
                if disposition not in _CONFIRMED_ABSENT_DISPOSITIONS:
                    state = (disposition.value if disposition is not None
                             else "present")
                    msg = (f"refusing to skip '{row['remote_path']}': it was "
                           f"not staged locally but the source reports it "
                           f"as {state}.")
                    print(f"\n[REMOTE] {msg}")
                    self.host.db.update_manifest_row(
                        row['manifest_id'], session_id=session_id,
                        status='fetch_failed', error_msg=msg[:500])
                    return False
                confirmed.append((row, disposition))
        for row, disposition in confirmed:
            reason = ("missing after tar fetch"
                      if disposition is SourceDisposition.SOURCE_MISSING
                      else f"{disposition.value} after tar fetch")
            source_missing_files.append({
                'manifest_id': row['manifest_id'],
                'remote_path': row['remote_path'],
                'file_size_bytes': row['file_size_bytes'],
            })
            self.host.skipped_tracker.add(
                'remote', row['remote_path'], reason,
                'fetch', session_id=session_id, chunk_index=chunk_index)
            print(f"[REMOTE] Source missing (confirmed by the source host); "
                  f"skipped: {row['remote_path']}")
            self.host.db.update_manifest_row(
                row['manifest_id'],
                session_id=session_id,
                status='source_missing',
                local_rel_path=None,
                error_msg=reason,
            )
        return True

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
                    # Alone in tmp, a name that differs only by Unicode form
                    # can only be this file.
                    found, _found_rel, ambiguous = _resolve_extracted_path(
                        tmp, _winsafe_extracted_rel(rel))
                    if found is not None and not ambiguous:
                        natural = found
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
                    if not self._record_confirmed_absent(
                            session_id, None, [(row, remote_base, rel)],
                            source_missing_files):
                        return False
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

    def _start_fetch_monitor(
            self, stop_evt, abort_evt, fetch_dir, total_bytes,
            tar_part_paths=None, tar_part_paths_lock=None, label="FETCH"):
        """Live remote->PC throughput, plus a staging watchdog.

        Progress is the fetch dir's logical growth. The watchdog fires
        abort_evt — killing the tar stream — before an overrunning chunk can
        exhaust the staging disk and wedge the pipeline: either when free
        space on the staging volume reaches the reserve floor, or when the
        chunk exceeds its planned bytes by fetch_overrun_abort_factor. Any
        overrun past the warn threshold is reported loudly (the plan's sizes
        come from the scan, so a growing or sparse-expanded remote file shows
        up here first)."""
        abort_factor = getattr(self.host, "fetch_abort_factor", 2.0)
        stall_timeout = getattr(self.host, "fetch_stall_timeout", 600)

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
            previous_snapshot = None
            part_last_growth = {}
            while not stop_evt.wait(interval):
                walk_start = time.time()
                if tar_part_paths is None:
                    cur = _dir_tree_size(fetch_dir)
                    snapshot = None
                else:
                    lock = tar_part_paths_lock or threading.Lock()
                    with lock:
                        parts = list(tar_part_paths)
                    snapshot = _staging_progress_snapshot(
                        parts, (fetch_dir,))
                    cur = int(snapshot["total_bytes"])
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
                if snapshot is None:
                    action = _fetch_watchdog_action(
                        cur=cur, last_growth_at=last_growth_at, now=now,
                        total_bytes=total_bytes, abort_factor=abort_factor,
                        stall_timeout=stall_timeout, free_bytes=free,
                        reserve_bytes=LOCAL_STAGING_RESERVE_BYTES)
                else:
                    action = _tar_progress_watchdog_action(
                        snapshot=snapshot,
                        previous_snapshot=previous_snapshot,
                        part_last_growth=part_last_growth,
                        aggregate_last_growth=last_growth_at,
                        now=now, total_bytes=total_bytes,
                        abort_factor=abort_factor,
                        stall_timeout=stall_timeout, free_bytes=free,
                        reserve_bytes=LOCAL_STAGING_RESERVE_BYTES)
                    previous_snapshot = snapshot
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
                if action == 'part_stall':
                    _alarm(
                        f"aborting fetch: a Stored TAR .part stream has not "
                        f"grown for {stall_timeout}s while sibling progress "
                        "may still be occurring. The chunk stays unready and "
                        "owned .part evidence is preserved."
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
