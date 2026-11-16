"""Fail-closed check of the single-writer lock before a tape write (audit H6).

The cross-process archiver lock is a PostgreSQL session-level advisory lock on
one pinned connection. If that session dies mid-run (a PostgreSQL restart, a
terminated backend) the lock is released while the connection pool reconnects
and the rest of the application keeps working, so a second archiver or a tape
reset can take the lock concurrently. Every write entry therefore asks the
database layer to *prove* ownership first.

This module only translates the proof into the pipeline's vocabulary. It never
touches LTFS and never re-acquires the lock.
"""
from .exit_codes import ExitCode, REASON_ARCHIVER_LOCK_LOST, StopResult
from .logsetup import get_logger


def assert_archiver_lock_held(db):
    """Raise unless ``db`` proves this process still owns the archiver lock.

    A database object that cannot answer the question at all is refused too:
    "there is no way to check" is not evidence that the lock is held.
    """
    verify = getattr(db, "verify_archiver_lock", None)
    if not callable(verify):
        raise RuntimeError(
            "[LOCK] This database layer cannot prove ownership of the "
            "archiver lock; refusing to start a tape write.")
    verify()


def archiver_lock_stop(db, *, session_id=None, chunk_index=None,
                       source="write"):
    """``None`` when the lock is proven held, else a hard-stop ``StopResult``.

    Any failure to prove ownership - including an unexpected error while
    asking - is a non-resumable safety block. Staged packs are preserved: no
    write started, so nothing is ambiguous on tape.
    """
    try:
        assert_archiver_lock_held(db)
    except Exception as exc:
        get_logger().error(
            "archiver_lock_lost: session=%s chunk=%s detail=%s",
            session_id, None if chunk_index is None else chunk_index + 1, exc,
            exc_info=True)
        print(f"\n[LOCK] {exc}")
        return StopResult(
            exit_code=ExitCode.SAFETY_BLOCK,
            reason=REASON_ARCHIVER_LOCK_LOST, resumable=False,
            source=source, session_id=session_id, chunk_index=chunk_index,
            preserve_pack=True, detailed_reason=str(exc))
    return None
