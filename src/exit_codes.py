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
REASON_UNEXPECTED_TAPE_OR_DB_STATE = "unexpected_tape_or_db_state"
REASON_AMBIGUOUS_ACTIVE_SESSIONS = "ambiguous_active_sessions"
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
    #: Whether the caller should keep the staged pack for a direct resume. A gate
    #: block (the write never started) keeps it; a mid-attempt write failure
    #: (re-fetchable) does not. Not serialized to the status files.
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
