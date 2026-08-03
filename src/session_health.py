"""READ-ONLY health report across EVERY session in the catalog.

Plan 1 completion. :func:`~src.startup_reconcile.session_frontier_report`
answers a deep question about *one* session that an operator already suspects.
This answers a shallower question about *all* of them: which sessions are fine,
which are merely unfinished, and which are in a state the new scanner must not
be pointed at.

The distinction that drives every classification here:

* **unfinished is not unsafe.** A session whose scan stopped half way is the
  normal input to a conservative frontier bootstrap. It is ``partial``.
* **unknown is unsafe.** A chunk in ``backing`` means a tape write began and
  the catalog cannot say whether the bytes landed; a lease that has not expired
  means a worker may still be alive. Those are ``ambiguous`` and nothing
  automated may act on them (incident 010).
* **contradictory is unsafe.** Counts that disagree with each other, a chunk
  whose ordinals are duplicated, a session pointing at a tape generation that is
  no longer active — those are ``inconsistent`` and need a human.

Nothing here writes. It opens no LTFS path, reads no tape, and starts nothing.
Every fact is derived from PostgreSQL at call time; no session id, chunk number
or historical status is hard-coded, because a report that already knows the
answer cannot notice that the answer changed.
"""
from .logsetup import get_logger
from .pipeline_types import ChunkStatus

#: Classifications, most severe last. ``verdict`` is the worst that applies.
HEALTHY = "healthy"           # terminal and self-consistent
TERMINAL = "terminal"         # finished (completed or deliberately abandoned)
PARTIAL = "partial"           # unfinished, but consistent — safe to migrate
BLOCKED = "blocked"           # needs an operator decision before any run
AMBIGUOUS = "ambiguous"       # physical/worker outcome unknowable from catalog
ORPHANED = "orphaned"         # references something that no longer exists
INCONSISTENT = "inconsistent"  # facts contradict each other

SEVERITY = {HEALTHY: 0, TERMINAL: 0, PARTIAL: 1, BLOCKED: 2,
            AMBIGUOUS: 3, ORPHANED: 3, INCONSISTENT: 4}

AMBIGUOUS_CHUNK_STATE = ChunkStatus.BACKING.value
#: Mid-flight states: a worker may hold them right now.
MID_FLIGHT = tuple(s for s in ("fetching", "packing")
                   if s in {c.value for c in ChunkStatus})
FAILED_STATES = ("fetch_failed", "backup_failed")
TERMINAL_SESSION_STATES = ("completed", "abandoned")


def _safe(report, name, call, default=None):
    """Run one probe; a failure becomes a recorded finding, never an exception.

    A health report that dies on the first unreadable session is useless
    precisely when it is most needed.
    """
    try:
        return call()
    except Exception as exc:
        report["errors"].append(f"{name}: {exc}")
        report["findings"].append({
            "severity": INCONSISTENT,
            "detail": f"{name} could not be read, so this report is incomplete",
        })
        return default


def session_health(db, session_id, *, active_processes=None,
                   lock_holders=None):
    """Classify ONE session. Read-only; never raises for a bad session."""
    report = {
        "session_id": session_id,
        "verdict": HEALTHY,
        "findings": [],
        "errors": [],
        "session": None,
        "chunk_states": {},
        "membership": None,
        "frontier": {"bound": None, "scopes": 0, "bootstrap": None},
        "leases": {"expired": 0, "held": 0},
        "file_state": {},
        "tape": {},
        "shared_plan_sessions": [],
        "supersession": {},
    }

    def note(severity, detail, **extra):
        report["findings"].append(dict(severity=severity, detail=detail,
                                       **extra))

    session = _safe(report, "session row",
                    lambda: db.get_remote_session(session_id))
    if not session:
        report["verdict"] = ORPHANED
        note(ORPHANED, "the session row could not be read")
        return report
    report["session"] = {k: session.get(k) for k in (
        "session_id", "session_label", "remote_host", "tape_label",
        "tape_generation", "status", "scan_complete", "total_files",
        "total_bytes", "chunk_count", "plan_id", "created_at", "completed_at")}
    report["session"]["has_scan_error"] = bool(session.get("scan_error"))

    status = session.get("status")
    scan_complete = bool(session.get("scan_complete"))

    # -- chunk states --------------------------------------------------
    states = _safe(report, "chunk states",
                   lambda: db.get_chunk_state_counts(session_id), []) or []
    counts = {row["status"]: int(row["n"]) for row in states}
    report["chunk_states"] = counts

    ambiguous = counts.get(AMBIGUOUS_CHUNK_STATE, 0)
    if ambiguous:
        note(AMBIGUOUS,
             f"{ambiguous} chunk(s) are '{AMBIGUOUS_CHUNK_STATE}': a tape write "
             "began and the catalog cannot say whether the bytes landed. Only a "
             "human who has compared the tape against the catalog may move it "
             "(incident 010).", chunks=ambiguous)
    mid = {s: counts[s] for s in MID_FLIGHT if counts.get(s)}
    if mid:
        note(AMBIGUOUS,
             f"chunks are mid-flight ({mid}); a worker may still hold them",
             chunks=sum(mid.values()))
    failed = {s: counts[s] for s in FAILED_STATES if counts.get(s)}
    if failed:
        note(BLOCKED,
             f"chunk(s) in a failed state ({failed}) need an operator decision "
             "before the session runs again", chunks=sum(failed.values()))

    # -- membership ----------------------------------------------------
    membership = _safe(report, "plan membership",
                       lambda: db.get_session_membership_summary(session_id))
    report["membership"] = membership
    if membership:
        planned_chunks = membership.get("distinct_chunks")
        declared = session.get("chunk_count")
        total_chunks = sum(counts.values())
        if declared is not None and total_chunks and declared != total_chunks:
            note(INCONSISTENT,
                 f"the session declares {declared} chunk(s) but "
                 f"{total_chunks} chunk row(s) exist")
        if (planned_chunks is not None and total_chunks
                and planned_chunks > total_chunks):
            note(INCONSISTENT,
                 f"plan membership references {planned_chunks} chunk(s) but "
                 f"only {total_chunks} chunk row(s) exist")

    shared = _safe(report, "shared plan",
                   lambda: db.find_sessions_sharing_plan(session_id), []) or []
    report["shared_plan_sessions"] = list(shared)
    if shared:
        note(BLOCKED,
             f"this session shares its plan with {list(shared)}; changing one "
             "changes what the others see")

    # -- ownership and leases ------------------------------------------
    expired = _safe(report, "expired claims",
                    lambda: db.list_expired_chunk_claims(session_id), []) or []
    report["leases"]["expired"] = len(expired)
    if expired:
        note(BLOCKED,
             f"{len(expired)} chunk claim(s) have expired leases; startup "
             "reconciliation must release them before a run")

    # -- frontier ------------------------------------------------------
    bound = _safe(report, "frontier state",
                  lambda: db.session_has_frontier_state(session_id))
    report["frontier"]["bound"] = bound
    scopes = _safe(report, "scan scopes",
                   lambda: db.get_scan_scopes(session_id), []) or []
    report["frontier"]["scopes"] = len(scopes)
    boot = _safe(report, "bootstrap record",
                 lambda: db.get_frontier_bootstrap(session_id))
    if boot:
        report["frontier"]["bootstrap"] = {
            k: boot.get(k) for k in ("bootstrap_id", "state", "coverage_final")}

    # -- tape and generation -------------------------------------------
    label = session.get("tape_label")
    if label:
        active = _safe(report, "tape generation",
                       lambda: db.get_active_tape_generation(label))
        report["tape"] = {"label": label,
                          "session_generation": session.get("tape_generation"),
                          "active_generation": (active or {}).get("generation")
                          if isinstance(active, dict) else active}
        got = report["tape"]["active_generation"]
        mine = report["tape"]["session_generation"]
        if got is not None and mine is not None and got != mine:
            note(BLOCKED,
                 f"the session was planned against {label} generation {mine}, "
                 f"but the active generation is {got}: the cartridge was reset "
                 "or replaced, so its existing locators no longer describe the "
                 "loaded medium")

    # -- lifecycle -----------------------------------------------------
    if status in TERMINAL_SESSION_STATES:
        if status == "completed" and not scan_complete:
            note(INCONSISTENT,
                 "the session is 'completed' but its scan never completed, so "
                 "'all chunks done' cannot mean 'finished'")
        pending_left = counts.get("pending", 0)
        if status == "completed" and pending_left:
            note(INCONSISTENT,
                 f"the session is 'completed' but {pending_left} chunk(s) are "
                 "still pending")
        report["verdict"] = _worst(report, TERMINAL)
    else:
        if not scan_complete:
            note(PARTIAL,
                 "the scan never completed, so the plan's full membership is "
                 "unknown; this is the normal input to a conservative frontier "
                 "bootstrap, not a defect")
        elif counts.get("pending"):
            note(PARTIAL,
                 f"{counts['pending']} chunk(s) are still pending")
        report["verdict"] = _worst(report, PARTIAL if not scan_complete
                                   else HEALTHY)

    if lock_holders:
        note(AMBIGUOUS, f"the archiver lock is held by {lock_holders}")
    if active_processes:
        note(AMBIGUOUS, f"archive processes are running: {active_processes}")

    report["verdict"] = _worst(report, report["verdict"])
    return report


def _worst(report, baseline):
    worst = baseline
    for finding in report["findings"]:
        if SEVERITY.get(finding["severity"], 0) > SEVERITY.get(worst, 0):
            worst = finding["severity"]
    return worst


def all_session_health(db, *, active_processes=None, lock_holders=None):
    """Classify EVERY session in the catalog. Read-only.

    Sessions are discovered from PostgreSQL, never from a list in code, so a
    session created after this was written is still audited.
    """
    report = {"sessions": [], "summary": {}, "errors": []}
    try:
        rows = db.list_remote_sessions()
    except Exception as exc:
        report["errors"].append(f"could not list sessions: {exc}")
        get_logger().exception("all_session_health could not list sessions")
        return report

    for row in rows:
        report["sessions"].append(session_health(
            db, row["session_id"], active_processes=active_processes,
            lock_holders=lock_holders))

    summary = {}
    for entry in report["sessions"]:
        summary[entry["verdict"]] = summary.get(entry["verdict"], 0) + 1
    report["summary"] = {
        "sessions": len(report["sessions"]),
        "by_verdict": summary,
        "needs_attention": sorted(
            entry["session_id"] for entry in report["sessions"]
            if SEVERITY.get(entry["verdict"], 0) >= SEVERITY[BLOCKED]),
        "safe_to_bootstrap": sorted(
            entry["session_id"] for entry in report["sessions"]
            if entry["verdict"] in (PARTIAL,)),
    }
    return report
