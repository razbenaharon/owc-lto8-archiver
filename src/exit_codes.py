"""Stable process exit codes and the structured stop-result they travel with.

The remote-archive pipeline can stop for many reasons that a human — or a thin
supervisor — needs to tell apart *without* reading the log. A bare boolean
``stop_pipeline`` flag cannot: an SCCM restart, a DNS outage that exhausted its
retries, and a not-``time@5`` mount are all "stopped", but the correct response
to each is different (relaunch after the host settles / relaunch now / do not
relaunch until a human looks). So every terminal path returns two things that
are recorded identically to the log, the status file, and the caller:

  * an **exit code** — the coarse class of outcome (:class:`ExitCode`);
  * a structured **reason** slug — the specific operational decision.

The reason is deliberately *generic* for network failures
(``network_retry_exhausted``, never ``dns_retry_exhausted``): not every transient
failure is DNS. The precise transport diagnosis goes in ``error_classification``
and the raw text in ``detailed_reason``.

This module is import-light on purpose (stdlib only) so it can be imported from
``run.py`` before the heavier package is pulled in.
"""
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import IntEnum
from typing import Optional


class ExitCode(IntEnum):
    """Process exit codes. Values are a stable contract — do not renumber."""

    #: Session finished; every planned chunk is archived.
    COMPLETED = 0
    #: Self-healing / re-launchable WITHOUT human judgement — network retry
    #: exhausted, an SCCM/Windows pending-reboot safe stop, a stop at a chunk
    #: boundary, a clean user-less stop. A pending reboot is transient and
    #: self-clearing, so it lives here, not in SAFETY_BLOCK.
    TRANSIENT_RESUMABLE = 10
    #: A human must inspect before continuing: mount not time@5, mount
    #: unverifiable, a prior-run 'backing' chunk, an ambiguous tape/DB state.
    SAFETY_BLOCK = 20
    #: Config/auth/host-key/missing-credential/code error — no blind retry.
    FATAL_CONFIG = 30
    #: Ctrl+C / an intentional operator stop.
    USER_STOP = 40


# --- reason slugs: the operational decision (kept generic for network) --------
# 10 — TRANSIENT_RESUMABLE
REASON_NETWORK_RETRY_EXHAUSTED = "network_retry_exhausted"
REASON_SCCM_REBOOT_PENDING = "sccm_reboot_pending"
REASON_WINDOWS_REBOOT_PENDING = "windows_reboot_pending"
REASON_STOPPED_AT_CHUNK_BOUNDARY = "stopped_at_chunk_boundary"  # fallback only
# 10 — TRANSIENT_RESUMABLE (a tape write that failed mid-attempt; re-fetchable)
REASON_TAPE_WRITE_FAILED = "tape_write_failed"
# 20 — SAFETY_BLOCK
REASON_AMBIGUOUS_BACKING_CHUNK = "ambiguous_backing_chunk"
REASON_LTFS_SYNC_MODE_NOT_TIME5 = "ltfs_sync_mode_not_time5"
REASON_LTFS_MOUNT_UNVERIFIABLE = "ltfs_mount_unverifiable"
REASON_LTFS_MEDIA_DEGRADED = "ltfs_media_degraded"
REASON_UNEXPECTED_TAPE_OR_DB_STATE = "unexpected_tape_or_db_state"
#: Authoritative cross-process LTFS ownership could not be acquired, so NO tape,
#: mount, readiness, cartridge or robocopy operation was attempted. Nothing was
#: written, so no chunk is ambiguous and the staged pack stays reusable.
REASON_LTFS_OWNERSHIP_UNAVAILABLE = "ltfs_ownership_unavailable"
REASON_AMBIGUOUS_ACTIVE_SESSIONS = "ambiguous_active_sessions"
#: The cross-process single-writer lock is no longer provably held (its
#: PostgreSQL session died). Never retried: another writer may hold it.
REASON_ARCHIVER_LOCK_LOST = "archiver_lock_lost"
#: The sealed-batch feature flag was explicitly enabled, but the feature is not
#: ready (schema not applied, checksum mismatch, or exact schema drift). Startup
#: refuses BEFORE any worker thread rather than fall back to legacy behaviour
#: after the operator opted in. No tape or batch operation was attempted.
REASON_SEALED_BATCH_FEATURE_UNAVAILABLE = "sealed_batch_feature_unavailable"
#: The sole production frontier scanner cannot rely on migration 014 because
#: the schema is missing, unfinalized, or indeterminate. There is no legacy
#: scan mode to select instead, so the run stops before scan, staging, or tape
#: work starts.
REASON_SCAN_FRONTIER_UNAVAILABLE = "scan_frontier_unavailable"
# 30 — FATAL_CONFIG
REASON_SSH_AUTHENTICATION_FAILED = "ssh_authentication_failed"
REASON_SSH_PERMISSION_DENIED = "ssh_permission_denied"
REASON_SSH_HOST_KEY_MISMATCH = "ssh_host_key_mismatch"
REASON_MISSING_NONINTERACTIVE_CREDENTIAL = "missing_noninteractive_credential"
REASON_BAD_CONFIG = "bad_config"
REASON_NO_ACTIVE_SESSION = "no_active_session"
REASON_NONINTERACTIVE_REQUIRES_RESUME = "noninteractive_requires_resume"
# 40 / 0
REASON_USER_REQUESTED_STOP = "user_requested_stop"
REASON_COMPLETED = "completed"

# --- auto-restart policy: the single source of truth --------------------------
# scripts/archive_watchdog.ps1 may relaunch a stopped run ONLY when the recorded
# stop is one a machine can safely resume without a human. The PowerShell lists
# there are a copy of the sets below; tests/test_watchdog_hard_stop_sync.py
# fails when the copy drifts, when a reason emitted with a hard exit code is
# missing here, or when a new REASON_* constant is left unclassified.

#: Exit codes after which NOTHING may restart the archiver automatically: a
#: human must inspect (20), fix configuration (30), or chose to stop (40).
NO_AUTO_RESTART_EXIT_CODES = frozenset({
    ExitCode.SAFETY_BLOCK, ExitCode.FATAL_CONFIG, ExitCode.USER_STOP})

#: The only exit code a supervisor may relaunch after. Anything else that is
#: not listed here — including a code this module does not define — refuses.
AUTO_RESTARTABLE_EXIT_CODES = frozenset({ExitCode.TRANSIENT_RESUMABLE})

#: Stop reasons that must never be auto-restarted.
NO_AUTO_RESTART_REASONS = frozenset({
    # 20 — SAFETY_BLOCK
    REASON_AMBIGUOUS_BACKING_CHUNK,
    REASON_LTFS_SYNC_MODE_NOT_TIME5,
    REASON_LTFS_MOUNT_UNVERIFIABLE,
    REASON_LTFS_MEDIA_DEGRADED,
    REASON_UNEXPECTED_TAPE_OR_DB_STATE,
    REASON_LTFS_OWNERSHIP_UNAVAILABLE,
    REASON_AMBIGUOUS_ACTIVE_SESSIONS,
    REASON_SEALED_BATCH_FEATURE_UNAVAILABLE,
    REASON_SCAN_FRONTIER_UNAVAILABLE,
    REASON_ARCHIVER_LOCK_LOST,
    # 30 — FATAL_CONFIG
    REASON_SSH_AUTHENTICATION_FAILED,
    REASON_SSH_PERMISSION_DENIED,
    REASON_SSH_HOST_KEY_MISMATCH,
    REASON_MISSING_NONINTERACTIVE_CREDENTIAL,
    REASON_BAD_CONFIG,
    REASON_NO_ACTIVE_SESSION,
    REASON_NONINTERACTIVE_REQUIRES_RESUME,
    # 40 — USER_STOP: restarting ten minutes after the operator stopped the
    # run on purpose would simply undo the stop.
    REASON_USER_REQUESTED_STOP,
    # 10 — but a tape write failed. Transient by exit code, yet never retried
    # into automatically (AGENTS.md: after a tape failure, stop and diagnose).
    REASON_TAPE_WRITE_FAILED,
})

#: Stop reasons a supervisor may relaunch after (exit code 10, self-clearing).
AUTO_RESTARTABLE_REASONS = frozenset({
    REASON_NETWORK_RETRY_EXHAUSTED,
    REASON_SCCM_REBOOT_PENDING,
    REASON_WINDOWS_REBOOT_PENDING,
    REASON_STOPPED_AT_CHUNK_BOUNDARY,
})

# ``stopped_at_chunk_boundary`` is the only reason that may be replaced by a more
# specific one after the fact; every other reason, once recorded, wins.
GENERIC_REASONS = frozenset({REASON_STOPPED_AT_CHUNK_BOUNDARY})

# --- error_classification: the precise transport diagnosis --------------------
CLASS_DNS_RESOLUTION_FAILURE = "dns_resolution_failure"
CLASS_CONNECTION_TIMEOUT = "connection_timeout"
CLASS_CONNECTION_RESET = "connection_reset"
CLASS_CONNECTION_REFUSED = "connection_refused"
CLASS_NETWORK_UNREACHABLE = "network_unreachable"
CLASS_TEMPORARY_TRANSPORT_FAILURE = "temporary_transport_failure"

#: The transport diagnoses that justify a retry. Auth/permission/host-key/config
#: are NOT here — a password requirement is never treated as a DNS blip.
TRANSIENT_CLASSIFICATIONS = frozenset({
    CLASS_DNS_RESOLUTION_FAILURE,
    CLASS_CONNECTION_TIMEOUT,
    CLASS_CONNECTION_RESET,
    CLASS_CONNECTION_REFUSED,
    CLASS_NETWORK_UNREACHABLE,
    CLASS_TEMPORARY_TRANSPORT_FAILURE,
})


@dataclass
class StopResult:
    """Why a terminal path stopped, carried unchanged to log/status/caller.

    Recorded at the *same point* a component sets the stop flag, so the reason
    is the specific one that component knows — never a generic reason re-derived
    from the bare flag downstream.
    """

    exit_code: int
    reason: str
    error_classification: Optional[str] = None
    detailed_reason: Optional[str] = None
    resumable: bool = True
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    source: Optional[str] = None
    session_id: Optional[int] = None
    chunk_index: Optional[int] = None
    #: Whether the caller should keep the staged pack for a direct resume.
    #: Every writer stop keeps it: the pack is intact local data whatever
    #: happened at the tape. Not serialized to the status files.
    preserve_pack: bool = True

    @property
    def is_generic(self) -> bool:
        return self.reason in GENERIC_REASONS

    def as_dict(self) -> dict:
        d = asdict(self)
        d["exit_code"] = int(self.exit_code)
        d.pop("preserve_pack", None)  # internal staging hint, not a report field
        return d


def completed(session_id=None, source="pipeline") -> StopResult:
    return StopResult(
        exit_code=ExitCode.COMPLETED, reason=REASON_COMPLETED,
        resumable=False, source=source, session_id=session_id)
