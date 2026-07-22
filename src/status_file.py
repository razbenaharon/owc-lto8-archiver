"""Best-effort local status + last-failure JSON for the remote pipeline.

Purpose is narrow and deliberately un-ambitious: leave two small files behind so
a network failure can be diagnosed after the fact without tailing the log, and
without a background heartbeat daemon. It is NOT orchestration.

Hard rules (see the plan, item 11 / refinement):
  * writing these files must never take the pipeline down;
  * it must never change the original exit code or stop reason;
  * it must never hide or swallow the original exception.

So every public function here is wrapped so that any failure — a full disk, a
locked file, a serialization error — is logged locally and otherwise ignored.
Writes are atomic (temp file + ``os.replace``) so a reader never sees a
half-written file, and the atomic step itself is inside the best-effort guard.

``status.json``       — the live snapshot, rewritten at chunk boundaries and
                        during fetch retry/backoff.
``last_failure.json`` — the final snapshot written once at an abnormal exit.
"""
import json
import os
from datetime import datetime

from .logsetup import get_logger

STATUS_FILENAME = "status.json"
LAST_FAILURE_FILENAME = "last_failure.json"


def _atomic_write_json(path, payload):
    """Temp-file + os.replace, so no reader ever sees a partial file.

    The whole thing is best-effort: a failure here is logged and swallowed —
    the caller's control flow (and its exit code) must be unaffected.
    """
    tmp = f"{path}.tmp"
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, default=str)
        os.replace(tmp, path)
        return True
    except Exception:
        get_logger().warning(
            "status file write failed (ignored): %s", path, exc_info=True)
        # Do not leave a stray temp file behind if we can help it.
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except OSError:
            pass
        return False


def write_status(log_dir, *, session_id=None, chunk_id=None, phase=None,
                 retry_attempt=None, error_classification=None,
                 error_message=None, next_retry_delay=None, resumable=None,
                 exit_code=None, reason=None, detailed_reason=None):
    """Rewrite ``status.json`` with the live pipeline state. Best-effort."""
    if not log_dir:
        return False
    payload = {
        "timestamp": datetime.now().isoformat(),
        "session_id": session_id,
        "chunk_id": chunk_id,
        "phase": phase,
        "retry_attempt": retry_attempt,
        "error_classification": error_classification,
        "error_message": error_message,
        "next_retry_delay": next_retry_delay,
        "resumable": resumable,
        "exit_code": None if exit_code is None else int(exit_code),
        "reason": reason,
        "detailed_reason": detailed_reason,
    }
    return _atomic_write_json(
        os.path.join(log_dir, STATUS_FILENAME), payload)


def write_last_failure(log_dir, stop_result, *, phase=None):
    """Write ``last_failure.json`` from a StopResult at an abnormal exit.

    Best-effort: a failure to write must not change the exit code the caller is
    about to return, and must not raise.
    """
    if not log_dir or stop_result is None:
        return False
    payload = dict(stop_result.as_dict())
    payload.setdefault("timestamp", datetime.now().isoformat())
    if phase is not None:
        payload["phase"] = phase
    return _atomic_write_json(
        os.path.join(log_dir, LAST_FAILURE_FILENAME), payload)
