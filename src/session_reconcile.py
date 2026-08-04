"""Evidence-driven reconciliation of stale ``active`` remote-session rows.

A remote session row is marked ``active`` the moment it is created and is moved
to a terminal status by the orchestrator that owns it.  When that orchestrator
dies without getting the chance — a crash, a forced restart, an operator who
started a fresh session over a differently-scoped path list — the row stays
``active`` forever.  Nothing reaps it, because the only component that knows a
session is finished is the process that was running it.

That stuck row is not harmless.  ``cleanup_unreferenced_remote_data`` refuses
while *any* remote session is ``active``, so one dead session blocks catalog
maintenance permanently.  The refusal itself is correct and stays: a row marked
active is treated as live until proven otherwise.  What was missing is the
"proven otherwise" half.

The distinction this module draws, and never blurs:

``a row marked active``
    A database fact.  Says nothing about whether anything is running.
``a running archiver``
    A local process (``active_archive_processes``).
``a held archiver lock``
    A PostgreSQL advisory lock, released automatically when the owning
    connection dies — the most trustworthy liveness signal available, because
    it cannot outlive the process the way a status file or a row can.
``a stale session``
    A row marked active with *no* archiver lock, *no* archiver process, and no
    recorded activity for ``idle_seconds``.

Reconciliation is fail-safe by construction:

* any lock holder or archiver process aborts the whole run — a live archiver is
  never raced, and because a process cannot be mapped to a session id with
  certainty, *one* live archiver protects *every* session row;
* a session whose evidence is ambiguous is reported and left alone, never
  guessed at;
* the terminal status is derived from chunk state using the same rules the
  orchestrator applies to itself, not chosen by the operator;
* the UPDATE is conditional on the row still being ``active``, so re-running
  changes nothing and reports nothing changed.
"""
from datetime import datetime, timezone
import json
import os
from typing import Any, cast

from .cli_errors import OperationalError
from .directory_catalog_validation import archiver_lock_status
from .local_manifest_archive import active_archive_processes
from .pipeline_types import ChunkStatus

try:  # keep import-light; the caller already proved psycopg is available
    import psycopg
    from psycopg.rows import dict_row
except ImportError:  # pragma: no cover - mirrors the sibling modules
    psycopg = None
    dict_row = None


#: Statuses ``remote_sessions_status_check`` permits.
ACTIVE_STATUS = "active"
COMPLETED_STATUS = "completed"
ABANDONED_STATUS = "abandoned"

#: A chunk in one of these states was being worked on when the process died.
#: Its on-tape/on-staging outcome is unknown, so the session is never classified
#: automatically — this is the ``ambiguous_backing_chunk`` rule from
#: ``exit_codes``, applied to reconciliation.
# Derived from the single ChunkStatus vocabulary rather than re-spelled as
# literals. These four names, pg_sessions.RECLAIMABLE_CHUNK_STATES and
# startup_reconcile's constants all have to agree; when they were four
# independent tuples of strings, nothing made them agree except care.
# tests/test_status_vocabulary.py pins the derived values to the exact literals
# these used to hold, so the simplification is proven, not assumed.
TRANSIENT_CHUNK_STATES = (ChunkStatus.FETCHING.value,
                          ChunkStatus.PACKING.value,
                          ChunkStatus.BACKING.value)
FAILED_CHUNK_STATES = (ChunkStatus.FETCH_FAILED.value,
                       ChunkStatus.BACKUP_FAILED.value)
DONE_CHUNK_STATE = ChunkStatus.DONE.value
PENDING_CHUNK_STATE = ChunkStatus.PENDING.value

#: How long a session must have been silent before it can be called stale.
#: A full chunk cycle on this host is ~70 minutes and a slow PACK can sit
#: apparently idle for ~25 of those, so 24h is many multiples of the longest
#: legitimate silence.
DEFAULT_IDLE_SECONDS = 24 * 3600

VERDICT_LIVE = "live"
VERDICT_AMBIGUOUS = "ambiguous"
VERDICT_STALE_COMPLETED = "stale_completed"
VERDICT_STALE_ABANDONED = "stale_abandoned"

#: Verdicts that reconciliation will act on, mapped to the status they take.
ACTIONABLE_VERDICTS = {
    VERDICT_STALE_COMPLETED: COMPLETED_STATUS,
    VERDICT_STALE_ABANDONED: ABANDONED_STATUS,
}


def _positive(value):
    return int(value or 0) > 0


def classify_session37_format_boundary_category(evidence):
    """Apply the Plan-2 Task-4.2 chunk category table.

    This helper is intentionally pure and status is never enough: a chunk may
    be granted a concrete format only when the category also has membership or
    writer/catalog evidence.  The approved Session-37 suffix exception is still
    stricter than "pending": it requires fixed membership and zero ownership,
    staging, worker, container, artifact, catalog, run, directory, or sealed
    batch evidence.
    """
    row = dict(evidence or {})
    status = str(row.get("status") or "")
    fixed_membership = bool(row.get("fixed_membership"))
    has_owner = bool(row.get("has_owner_evidence")
                     or row.get("owner_token") is not None)
    has_lease = bool(row.get("has_lease_evidence")
                     or row.get("lease_expires_at") is not None)
    has_attempt = bool(row.get("has_attempt_evidence")
                       or row.get("attempt_id") is not None)
    has_error = bool(row.get("has_error_evidence") or row.get("error_msg"))
    owner_or_failure = has_owner or has_lease or has_attempt or has_error
    counts = {
        "file_state": int(row.get("file_state_count") or 0),
        "worker_attempt": int(row.get("worker_attempt_count") or 0),
        "catalog_file": int(row.get("catalog_file_count") or 0),
        "archive_run": int(row.get("archive_run_count") or 0),
        "container": int(row.get("container_count") or 0),
        "written_container": int(row.get("written_container_count") or 0),
        "artifact": int(row.get("artifact_count") or 0),
        "directory": int(row.get("directory_evidence_count") or 0),
        "sealed_batch": int(row.get("sealed_batch_evidence_count") or 0),
        "sealed_batch_written": int(row.get("sealed_batch_written_count") or 0),
    }
    written_or_catalog = any(_positive(counts[name]) for name in (
        "catalog_file", "archive_run", "written_container",
        "sealed_batch_written"))
    operational = owner_or_failure or any(_positive(value)
                                          for value in counts.values())
    writer_started = bool(row.get("writer_started_at")
                          or row.get("writer_completed_at")
                          or _positive(counts["written_container"])
                          or status == ChunkStatus.BACKING.value)
    catalog_final = bool(row.get("catalog_committed_at")
                         or _positive(counts["catalog_file"])
                         or _positive(counts["archive_run"]))

    def result(category, action, assigned_format=None, confidence="none",
               blocker=None, eligible=False):
        return {
            "observed_category": category,
            "required_action": action,
            "assigned_format": assigned_format,
            "format_confidence": confidence,
            "blocker": blocker,
            "eligible_stored_tar_exception": bool(eligible),
        }

    if row.get("future_chunk"):
        return result(
            "future_chunk_after_persisted_boundary",
            "stored_tar_eligible_at_creation", "stored_tar", "high",
            eligible=True)
    if not fixed_membership:
        return result(
            "absent_or_conflicting_membership",
            "manual_reconciliation_do_not_reuse_identity",
            blocker="fixed contiguous membership is not proven")
    if status == ChunkStatus.DONE.value and written_or_catalog:
        return result(
            "done_with_written_or_catalog_evidence",
            "immutable_zip_preserve_locators", "zip", "high")
    if status == ChunkStatus.DONE.value:
        return result(
            "done_status_only_with_fixed_membership",
            "zip_prefix_only_with_operator_visible_residual_risk",
            "zip", "medium",
            blocker="no corroborating written/catalog evidence")
    if status == ChunkStatus.BACKING.value:
        return result(
            "backing",
            "ambiguous_hard_block_no_retry_or_conversion",
            blocker="physical tape write may have started")
    if writer_started and not catalog_final:
        return result(
            "copy_may_have_succeeded_without_catalog_finality",
            "manual_reconciliation_no_rewrite",
            blocker="writer evidence exists without catalog finality")
    if status == ChunkStatus.FETCHING.value:
        return result(
            "fetching",
            "reconcile_owner_and_staging_retry_zip_only", "zip", "low",
            blocker="fetch ownership/staging outcome is not final")
    if status == ChunkStatus.PACKING.value:
        return result(
            "packing",
            "reconcile_owner_and_pack_marker_retry_zip_only", "zip", "low",
            blocker="pack ownership/marker outcome is not final")
    if status in (ChunkStatus.FETCH_FAILED.value,
                  ChunkStatus.BACKUP_FAILED.value):
        return result(
            status,
            "reconcile_exact_attempt_retry_zip_if_safe", "zip", "low",
            blocker="failed attempt evidence must be reconciled")
    if status == ChunkStatus.PENDING.value:
        if not operational:
            return result(
                "pending_never_owned_fixed_membership",
                "eligible_for_approved_boundary_rehearsal", "stored_tar",
                "high", eligible=True)
        return result(
            "pending_with_existing_evidence",
            "zip_default_no_identity_repurpose", "zip", "low",
            blocker="pending chunk already has ownership/staging/catalog evidence")
    return result(
        "stale_or_conflicting_evidence",
        "manual_reconciliation_blocks_migration",
        blocker=f"unrecognized or contradictory status {status!r}")


def _connect(conninfo, *, autocommit=False):
    if psycopg is None:  # pragma: no cover - environment guard
        raise OperationalError(
            "[RECONCILE] psycopg is required to inspect session state.")
    return cast(Any, psycopg).connect(
        conninfo, autocommit=autocommit, row_factory=cast(Any, dict_row))


def _now(now=None):
    return now or datetime.now(timezone.utc)


def status_file_session(log_dir):
    """Session id named by ``status.json``, or None. Corroboration only.

    The file is written best-effort and is never treated as proof that anything
    is running — it can trivially outlive the process. It is reported so an
    operator can see *which* session a live archiver most likely belongs to.
    """
    if not log_dir:
        return None
    path = os.path.join(str(log_dir), "status.json")
    try:
        with open(path, encoding="utf-8") as fh:
            payload = json.load(fh)
    except (OSError, ValueError):
        return None
    session_id = payload.get("session_id")
    return int(session_id) if isinstance(session_id, (int, str)) and str(
        session_id).lstrip("-").isdigit() else None


def _chunk_states(conn, session_id):
    rows = conn.execute(
        """SELECT status, COUNT(*) AS chunks, MAX(updated_at) AS last_update
           FROM remote_chunks WHERE session_id=%s
           GROUP BY status ORDER BY status""", (session_id,)).fetchall()
    return [dict(row) for row in rows]


def _catalog_artifacts(conn, session_id):
    """Rows this session actually contributed to the catalog.

    A session that produced nothing is provably not ``completed``, and it also
    means marking it terminal cannot orphan catalog provenance.
    """
    return dict(conn.execute(
        """SELECT
             (SELECT COUNT(*) FROM files_index
              WHERE remote_session_id=%s) AS files_index,
             (SELECT COUNT(*) FROM archive_runs
              WHERE remote_session_id=%s) AS archive_runs,
             (SELECT COUNT(*) FROM directory_archive_bundles
              WHERE remote_session_id=%s) AS directory_bundles,
             (SELECT COUNT(*) FROM directory_tree_index
              WHERE remote_session_id=%s) AS directory_tree_rows""",
        (session_id, session_id, session_id, session_id)).fetchone())


def _classify(session, chunk_states, idle_seconds, now):
    """Derive the terminal status from chunk state. Ambiguity wins.

    Mirrors ``RemoteOrchestrator._run_session``: a session with no pending work
    is ``completed``; a session that planned nothing, or still has unfinished
    or failed chunks, is ``abandoned``. Anything the orchestrator itself would
    not decide unattended is left to a human.
    """
    by_state = {row["status"]: int(row["chunks"]) for row in chunk_states}
    updates = [row["last_update"] for row in chunk_states if row["last_update"]]
    last_activity = max(updates) if updates else session["created_at"]
    idle = (now - last_activity).total_seconds() if last_activity else None

    transient = {s: by_state[s] for s in TRANSIENT_CHUNK_STATES if s in by_state}
    failed = {s: by_state[s] for s in FAILED_CHUNK_STATES if s in by_state}
    done = by_state.get(DONE_CHUNK_STATE, 0)
    pending = by_state.get(PENDING_CHUNK_STATE, 0)
    total_chunk_rows = sum(by_state.values())

    evidence = {
        "last_activity": last_activity,
        "idle_seconds": None if idle is None else int(idle),
        "chunk_rows": total_chunk_rows,
        "chunks_by_state": by_state,
        "transient_chunks": transient,
        "failed_chunks": failed,
    }

    if idle is None or idle < idle_seconds:
        return VERDICT_AMBIGUOUS, (
            "recorded activity is more recent than the "
            f"{idle_seconds}s idle threshold"), evidence
    if transient:
        # 'backing' in particular means a tape write was in flight; whether it
        # landed cannot be known from the database alone.
        return VERDICT_AMBIGUOUS, (
            "chunks left in a transient state "
            f"({', '.join(sorted(transient))}); the on-tape outcome is "
            "unknown from the catalog alone"), evidence
    if total_chunk_rows == 0:
        return VERDICT_STALE_ABANDONED, (
            "no chunk rows were ever planned, so the session archived "
            "nothing and cannot be completed"), evidence
    if not session.get("scan_complete", True):
        # The plan is still short of its full membership: more chunks could
        # legitimately have been added, so 'all done' cannot mean 'finished'.
        if pending or failed:
            return VERDICT_STALE_ABANDONED, (
                f"scan never completed and {pending} chunk(s) are still "
                f"pending, {sum(failed.values())} failed - unfinishable"), evidence
        return VERDICT_AMBIGUOUS, (
            "scan never completed; every existing chunk is done but the plan "
            "membership is unknown, so completion cannot be proven"), evidence
    if pending or failed:
        return VERDICT_STALE_ABANDONED, (
            f"{pending} chunk(s) pending and {sum(failed.values())} failed "
            "after the session went silent"), evidence
    if done and done == total_chunk_rows == int(session.get("chunk_count") or 0):
        return VERDICT_STALE_COMPLETED, (
            f"all {done} planned chunks are done and the scan completed; the "
            "owning process died before recording it"), evidence
    return VERDICT_AMBIGUOUS, (
        f"chunk bookkeeping does not add up: {done} done of "
        f"{total_chunk_rows} rows against chunk_count="
        f"{session.get('chunk_count')}"), evidence


def liveness_evidence(conninfo, log_dir=None):
    """Everything known about whether an archiver is actually running."""
    holders = archiver_lock_status(conninfo)
    processes = active_archive_processes()
    return {
        "archiver_lock_holders": holders,
        "archive_processes": processes,
        "status_file_session_id": status_file_session(log_dir),
        "archiver_running": bool(holders or processes),
    }


def session_forensics(conninfo, *, idle_seconds=DEFAULT_IDLE_SECONDS,
                      log_dir=None, now=None):
    """Read-only evidence + verdict for every ``active`` remote session."""
    now = _now(now)
    live = liveness_evidence(conninfo, log_dir)
    with _connect(conninfo) as conn:
        conn.execute("SET default_transaction_read_only = on")
        sessions = [dict(row) for row in conn.execute(
            """SELECT session_id, session_label, remote_host, tape_label,
                      status, scan_complete, scan_error, total_files,
                      total_bytes, chunk_count, plan_id, created_at,
                      completed_at
               FROM remote_sessions WHERE status=%s
               ORDER BY session_id""", (ACTIVE_STATUS,)).fetchall()]
        report = []
        for session in sessions:
            chunk_states = _chunk_states(conn, session["session_id"])
            verdict, why, evidence = _classify(
                session, chunk_states, idle_seconds, now)
            if live["archiver_running"]:
                # One live archiver protects every row: a process cannot be
                # mapped to a session id with certainty, so nothing is stale
                # while anything is running.
                verdict, why = VERDICT_LIVE, (
                    "an archiver lock/process is live; no active session may "
                    "be reclassified while anything is running")
            report.append({
                "session": session,
                "verdict": verdict,
                "reason": why,
                "evidence": evidence,
                "catalog_artifacts": _catalog_artifacts(
                    conn, session["session_id"]),
                "proposed_status": ACTIONABLE_VERDICTS.get(verdict),
            })
    return {"checked_at": now, "idle_seconds": idle_seconds,
            "liveness": live, "sessions": report}


def reconcile_stale_remote_sessions(conninfo, *, execute=False,
                                    idle_seconds=DEFAULT_IDLE_SECONDS,
                                    session_ids=None, log_dir=None, now=None):
    """Move provably-dead ``active`` sessions to their terminal status.

    Refuses outright while any archiver lock or archiver process exists. Only
    ``active`` rows are ever touched, and the UPDATE re-checks that status, so
    a second run is a no-op.
    """
    forensics = session_forensics(
        conninfo, idle_seconds=idle_seconds, log_dir=log_dir, now=now)
    live = forensics["liveness"]
    if live["archiver_running"]:
        raise OperationalError(
            "[RECONCILE] Refusing to reconcile while an archiver is live "
            f"(lock holders={live['archiver_lock_holders']}, "
            f"processes={len(live['archive_processes'])}). "
            "Reconciliation is only ever safe when nothing is running.")

    wanted = None if session_ids is None else {int(s) for s in session_ids}
    planned, skipped = [], []
    for entry in forensics["sessions"]:
        session_id = int(entry["session"]["session_id"])
        if wanted is not None and session_id not in wanted:
            continue
        if entry["verdict"] in ACTIONABLE_VERDICTS:
            planned.append(entry)
        else:
            skipped.append(entry)

    if wanted is not None:
        missing = wanted - {int(e["session"]["session_id"])
                            for e in forensics["sessions"]}
        if missing:
            raise OperationalError(
                "[RECONCILE] Not an active remote session (nothing to "
                f"reconcile): {sorted(missing)}")

    changed = []
    if execute and planned:
        with _connect(conninfo) as conn:
            for entry in planned:
                session_id = int(entry["session"]["session_id"])
                target = ACTIONABLE_VERDICTS[entry["verdict"]]
                # completed_at records when the session *stopped*, which is the
                # last activity the catalog witnessed — not the time an
                # operator got around to reconciling it.
                stopped_at = (entry["evidence"]["last_activity"]
                              or entry["session"]["created_at"])
                updated = conn.execute(
                    """UPDATE remote_sessions
                       SET status=%s, completed_at=COALESCE(completed_at,%s)
                       WHERE session_id=%s AND status=%s""",
                    (target, stopped_at, session_id, ACTIVE_STATUS)).rowcount
                changed.append({
                    "session_id": session_id,
                    "session_label": entry["session"]["session_label"],
                    "from_status": ACTIVE_STATUS,
                    "to_status": target,
                    "rows_updated": int(updated or 0),
                    "completed_at": stopped_at,
                    "reason": entry["reason"],
                })
            conn.commit()

    return {
        "executed": bool(execute),
        "checked_at": forensics["checked_at"],
        "idle_seconds": idle_seconds,
        "liveness": live,
        "planned": [{
            "session_id": e["session"]["session_id"],
            "session_label": e["session"]["session_label"],
            "verdict": e["verdict"],
            "proposed_status": e["proposed_status"],
            "reason": e["reason"],
            "evidence": e["evidence"],
            "catalog_artifacts": e["catalog_artifacts"],
        } for e in planned],
        "left_alone": [{
            "session_id": e["session"]["session_id"],
            "session_label": e["session"]["session_label"],
            "verdict": e["verdict"],
            "reason": e["reason"],
        } for e in skipped],
        "changed": changed,
    }


def format_report(result):
    """Operator-readable summary: what changed, what did not, and why."""
    lines = []
    live = result["liveness"]
    lines.append(
        f"[RECONCILE] archiver lock holders={live['archiver_lock_holders']} "
        f"processes={len(live['archive_processes'])} "
        f"status.json session={live['status_file_session_id']}")
    mode = "EXECUTE" if result["executed"] else "DRY-RUN"
    lines.append(f"[RECONCILE] mode={mode} "
                 f"idle_threshold={result['idle_seconds']}s")
    if not result["planned"]:
        lines.append("[RECONCILE] No session qualifies for reconciliation.")
    for entry in result["planned"]:
        lines.append(
            f"  session {entry['session_id']} "
            f"({entry['session_label']}): active -> "
            f"{entry['proposed_status']} - {entry['reason']}")
        lines.append(f"      chunks: {entry['evidence']['chunks_by_state']} "
                     f"idle={entry['evidence']['idle_seconds']}s")
        lines.append(f"      catalog rows: {entry['catalog_artifacts']}")
    for entry in result["left_alone"]:
        lines.append(
            f"  session {entry['session_id']} "
            f"({entry['session_label']}): LEFT ACTIVE [{entry['verdict']}] - "
            f"{entry['reason']}")
    for entry in result["changed"]:
        verb = "updated" if entry["rows_updated"] else "already reconciled"
        lines.append(
            f"[RECONCILE] session {entry['session_id']} {verb}: "
            f"{entry['from_status']} -> {entry['to_status']}")
    return "\n".join(lines)
