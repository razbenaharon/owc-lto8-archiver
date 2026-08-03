"""Startup reconciliation: decide what a previous run actually left behind.

Plan 1, Task 3.2. Runs before any worker thread and **never touches LTFS**.

The problem it exists for: after a crash, several independent records disagree
about what was happening. PostgreSQL says a chunk is ``fetching`` with an
expired lease. Staging holds a half-written pack. A ``.part`` scan artifact sits
next to a published one. And ``src.runtime``'s process registry — the thing that
looks like it should answer this — is **current-process cleanup only**: it is an
in-memory set that dies with the process it was protecting, so it proves nothing
about a run that already ended.

The rule that makes this safe is that **elapsed time is not evidence**. A lapsed
lease means "nobody renewed this", which is equally consistent with a worker
alive on a wedged SSH call. So a claim is released only when the previous owner
is *provably* gone:

* the local PID must be absent, **or** present with a different process creation
  time (PIDs are reused, and reclaiming on a recycled PID would run two workers
  against one chunk);
* if the attempt tagged a remote process group, that token must be provably
  absent on the source host — checked **read-only**, terminating only a proven
  orphan;
* if the source host is unreachable, or identity is uncertain in any way, the
  claim stays **blocked**. Not released, not assumed dead.

And one thing is out of reach entirely: a chunk in ``backing`` is never
reconciled here, whatever the evidence says. It means the physical write began,
so bytes may be on the cartridge; only a human who has compared the tape against
the catalog may move it (incident 010).
"""
import os

from .logsetup import get_logger
from .pipeline_types import ChunkStatus

#: Verdicts. ``BLOCKED`` is the safe default for anything not proven.
RELEASABLE = "releasable"
BLOCKED = "blocked"
LIVE = "live"

#: A chunk in this state is never a reconciliation candidate, at any age.
AMBIGUOUS_CHUNK_STATE = ChunkStatus.BACKING.value


class ProcessEvidence:
    """What can be established about one recorded worker attempt."""

    __slots__ = ("attempt_id", "owner_token", "local_pid",
                 "local_process_started_at", "remote_command_token",
                 "verdict", "reasons")

    def __init__(self, attempt_id=None, owner_token=None, local_pid=None,
                 local_process_started_at=None, remote_command_token=None):
        self.attempt_id = attempt_id
        self.owner_token = owner_token
        self.local_pid = local_pid
        self.local_process_started_at = local_process_started_at
        self.remote_command_token = remote_command_token
        self.verdict = BLOCKED
        self.reasons = []

    def as_dict(self):
        return {
            "attempt_id": self.attempt_id,
            "owner_token": self.owner_token,
            "local_pid": self.local_pid,
            "remote_command_token": self.remote_command_token,
            "verdict": self.verdict,
            "reasons": list(self.reasons),
        }

    def __repr__(self):
        return f"<ProcessEvidence {self.attempt_id} {self.verdict}>"


def local_process_evidence(evidence, psutil_module=None):
    """Is the recorded local PID still the SAME process?

    Three outcomes, and the middle one is the reason this function exists:

    * the PID is absent               -> that worker is gone
    * the PID exists, SAME creation time -> it is still running, hands off
    * the PID exists, DIFFERENT creation time -> the PID was recycled by an
      unrelated process; the worker is gone, but only because we checked

    A missing ``psutil`` yields ``BLOCKED``: we cannot look, so we do not guess.
    """
    if psutil_module is None:
        try:
            import psutil as psutil_module
        except ImportError:
            evidence.reasons.append(
                "psutil is unavailable, so no local process evidence could be "
                "gathered")
            return BLOCKED
    if not evidence.local_pid:
        evidence.reasons.append("the attempt recorded no local PID")
        return BLOCKED
    try:
        process = psutil_module.Process(int(evidence.local_pid))
        created = process.create_time()
    except Exception as exc:
        name = type(exc).__name__
        if name in ("NoSuchProcess", "ProcessLookupError"):
            evidence.reasons.append(
                f"local PID {evidence.local_pid} no longer exists")
            return RELEASABLE
        evidence.reasons.append(
            f"could not inspect local PID {evidence.local_pid}: {exc}")
        return BLOCKED

    recorded = evidence.local_process_started_at
    if recorded is None:
        evidence.reasons.append(
            f"local PID {evidence.local_pid} exists and the attempt recorded "
            "no creation time, so it cannot be told from a recycled PID")
        return BLOCKED
    if _same_creation_time(recorded, created):
        evidence.reasons.append(
            f"local PID {evidence.local_pid} is still the original process")
        return LIVE
    evidence.reasons.append(
        f"local PID {evidence.local_pid} was recycled by a different process "
        "(creation time differs), so the original worker is gone")
    return RELEASABLE


def _same_creation_time(recorded, observed, tolerance=2.0):
    """Compare a stored creation time with a live one, tolerantly.

    The stored value may be a datetime (PostgreSQL) or an epoch float; the
    tolerance absorbs clock/round-trip skew without being wide enough to
    confuse two different processes on a machine that reuses PIDs slowly.
    """
    try:
        recorded_epoch = (recorded.timestamp() if hasattr(recorded, "timestamp")
                          else float(recorded))
    except (TypeError, ValueError):
        return False
    return abs(recorded_epoch - float(observed)) <= tolerance


def remote_process_evidence(evidence, remote_probe=None):
    """Is the tagged remote process group gone?

    ``remote_probe(token)`` must be READ-ONLY and return:
      ``True``  the token's process group is still running
      ``False`` it is provably absent
      ``None``  the host is unreachable, or the answer is uncertain

    ``None`` and an exception both mean BLOCKED. An unreachable source host is
    exactly when guessing is most tempting and least safe: the remote ``tar``
    may still be streaming into staging.
    """
    if not evidence.remote_command_token:
        evidence.reasons.append(
            "the attempt tagged no remote process group, so there is nothing "
            "remote to prove absent")
        return RELEASABLE
    if remote_probe is None:
        evidence.reasons.append(
            "no remote probe was supplied; the remote process group could not "
            "be checked")
        return BLOCKED
    try:
        alive = remote_probe(evidence.remote_command_token)
    except Exception as exc:
        evidence.reasons.append(f"the remote probe failed: {exc}")
        return BLOCKED
    if alive is None:
        evidence.reasons.append(
            "the source host could not be reached, so the remote process "
            "group's state is unknown")
        return BLOCKED
    if alive:
        evidence.reasons.append(
            f"remote process group {evidence.remote_command_token} is still "
            "running on the source host")
        return LIVE
    evidence.reasons.append(
        f"remote process group {evidence.remote_command_token} is provably "
        "absent")
    return RELEASABLE


def classify_attempt(attempt, psutil_module=None, remote_probe=None):
    """Combine local and remote evidence into ONE verdict.

    Both sides must agree that the worker is gone. ``LIVE`` on either side wins
    over ``RELEASABLE`` on the other, and ``BLOCKED`` wins over everything
    except ``LIVE`` — because "it is still running" and "we cannot tell" both
    mean do not touch it, and the first is more informative to report.
    """
    evidence = ProcessEvidence(
        attempt_id=attempt.get("attempt_id"),
        owner_token=attempt.get("owner_token"),
        local_pid=attempt.get("local_pid"),
        local_process_started_at=attempt.get("local_process_started_at"),
        remote_command_token=attempt.get("remote_command_token"))

    local = local_process_evidence(evidence, psutil_module=psutil_module)
    remote = remote_process_evidence(evidence, remote_probe=remote_probe)

    if LIVE in (local, remote):
        evidence.verdict = LIVE
    elif local == RELEASABLE and remote == RELEASABLE:
        evidence.verdict = RELEASABLE
    else:
        evidence.verdict = BLOCKED
    return evidence


class StartupReconciler:
    """Inventory what a previous run left, and release only what is proven.

    Read-mostly. It releases pre-write claims and records cleanup evidence; it
    never classifies a session or a writer outcome, never deletes an artifact
    it cannot account for, and — the invariant that matters — never touches
    LTFS or a ``backing`` chunk.
    """

    def __init__(self, db, session_id, *, archive_root=None,
                 psutil_module=None, remote_probe=None):
        self.db = db
        self.session_id = session_id
        self.archive_root = archive_root
        self.psutil_module = psutil_module
        self.remote_probe = remote_probe

    # -- inventory --------------------------------------------------------
    def inventory(self):
        """Everything a reconciliation decision could rest on. Read-only."""
        report = {
            "session_id": self.session_id,
            "expired_claims": [],
            "ambiguous_chunks": [],
            "live_attempts": [],
            "orphan_parts": [],
            "errors": [],
        }
        try:
            report["ambiguous_chunks"] = list(
                self.db.get_chunks_with_status(
                    self.session_id, AMBIGUOUS_CHUNK_STATE))
        except Exception as exc:
            report["errors"].append(f"could not read ambiguous chunks: {exc}")

        for name, call in (
            ("expired_claims",
             lambda: self.db.list_expired_chunk_claims(self.session_id)),
            ("live_attempts",
             lambda: self.db.list_live_worker_attempts(self.session_id)),
        ):
            try:
                report[name] = [dict(row) for row in call()]
            except Exception as exc:
                report["errors"].append(f"could not read {name}: {exc}")

        if self.archive_root:
            try:
                from .archive_artifacts import find_orphan_parts
                report["orphan_parts"] = find_orphan_parts(self.archive_root)
            except Exception as exc:
                report["errors"].append(f"could not scan for .part files: {exc}")
        return report

    # -- decisions --------------------------------------------------------
    def plan(self):
        """What reconciliation WOULD do. Changes nothing.

        Every expired claim is matched to its recorded attempt and classified.
        A claim with no recorded attempt is BLOCKED: without an attempt row
        there is no PID and no remote token, so nothing can be proven.
        """
        report = self.inventory()
        attempts = {a.get("attempt_id"): a for a in report["live_attempts"]}
        decisions = []
        for claim in report["expired_claims"]:
            if claim.get("status") == AMBIGUOUS_CHUNK_STATE:
                # Belt and braces: the query already excludes it.
                continue
            attempt = attempts.get(claim.get("attempt_id"))
            if attempt is None:
                evidence = ProcessEvidence(
                    attempt_id=claim.get("attempt_id"),
                    owner_token=claim.get("owner_token"))
                evidence.reasons.append(
                    "no durable worker-attempt row was recorded for this "
                    "claim, so the previous owner cannot be identified")
                evidence.verdict = BLOCKED
            else:
                evidence = classify_attempt(
                    attempt, psutil_module=self.psutil_module,
                    remote_probe=self.remote_probe)
            decisions.append({
                "chunk_index": claim.get("chunk_index"),
                "status": claim.get("status"),
                "owner_token": claim.get("owner_token"),
                "evidence": evidence.as_dict(),
            })
        report["decisions"] = decisions
        report["releasable"] = [d for d in decisions
                                if d["evidence"]["verdict"] == RELEASABLE]
        report["blocked"] = [d for d in decisions
                             if d["evidence"]["verdict"] != RELEASABLE]
        return report

    def apply(self):
        """Release ONLY what :meth:`plan` proved abandoned.

        Idempotent: a second run finds nothing left to release. Returns the
        plan, annotated with what was actually released.
        """
        report = self.plan()
        released = []
        for decision in report["releasable"]:
            reasons = "; ".join(decision["evidence"]["reasons"])
            try:
                if self.db.reclaim_expired_chunk(
                        self.session_id, decision["chunk_index"],
                        decision["owner_token"], reasons):
                    released.append(decision["chunk_index"])
                    self._close_attempt(decision["evidence"]["attempt_id"],
                                        "orphan_terminated")
            except Exception as exc:
                report["errors"].append(
                    f"could not reclaim chunk "
                    f"{decision['chunk_index']}: {exc}")
        for decision in report["blocked"]:
            get_logger().warning(
                "reconcile_blocked: session=%s chunk=%s verdict=%s reasons=%s",
                self.session_id, decision["chunk_index"],
                decision["evidence"]["verdict"],
                "; ".join(decision["evidence"]["reasons"]))
        report["released"] = released
        return report

    def _close_attempt(self, attempt_id, terminal_state):
        if not attempt_id:
            return
        try:
            self.db.finish_worker_attempt(attempt_id, terminal_state)
        except Exception:
            get_logger().warning("could not close attempt %s", attempt_id,
                                 exc_info=True)


#: A session state that must never be resolved by a report or a tool.
TRANSIENT_CHUNK_STATES = (ChunkStatus.FETCHING.value,
                          ChunkStatus.PACKING.value,
                          AMBIGUOUS_CHUNK_STATE)

VERDICT_READY = "ready"
VERDICT_BLOCKED = "blocked"


def session_frontier_report(db, session_id, *, archive_root=None,
                            lock_holders=None, active_processes=None):
    """READ-ONLY assessment of one session's frontier readiness (Task 4.1).

    Answers the questions an operator needs before deciding anything about a
    session, from the authoritative catalog rather than from documentation:
    is its scan complete, what does its plan actually contain, which chunks are
    in a transient or ambiguous state, does anything else share its plan, has it
    published frontier state, and is a process still holding it.

    Deliberately **generic**. It hardcodes no session id, no chunk number and no
    historical status: a report that already knows the answer cannot detect that
    the answer changed. Written notes about a session are a hypothesis; this is
    the measurement.

    Any indeterminate evidence produces ``blocked``, never a guessed boundary.
    Touches no LTFS, no mount, no drive.
    """
    report = {
        "session_id": session_id,
        "verdict": VERDICT_BLOCKED,
        "blocking": [],
        "errors": [],
        "session": None,
        "scan_complete": None,
        "membership": None,
        "chunk_states": [],
        "transient_chunks": {},
        "max_chunk_index": None,
        "shared_plan_sessions": [],
        "frontier": {"bound": None, "scopes": []},
        "artifacts": {"orphan_parts": []},
        "liveness": {"lock_holders": lock_holders,
                     "active_processes": active_processes},
    }

    def _try(name, call, target=None, key=None):
        try:
            value = call()
        except Exception as exc:
            report["errors"].append(f"{name}: {exc}")
            report["blocking"].append(
                f"{name} could not be read, so this report is incomplete")
            return None
        if key is not None:
            (target if target is not None else report)[key] = value
        return value

    session = _try("session row",
                   lambda: db.get_remote_session(session_id), key="session")
    if session is None:
        report["blocking"].append("the session row could not be read")
        return report
    report["scan_complete"] = session.get("scan_complete")
    if not session.get("scan_complete", False):
        report["blocking"].append(
            "the scan never completed, so the plan's full membership is "
            "unknown and 'all chunks done' cannot mean 'finished'")

    membership = _try("plan membership",
                      lambda: db.get_session_membership_summary(session_id),
                      key="membership")
    if membership:
        report["max_chunk_index"] = membership.get("max_chunk_index")

    states = _try("chunk states",
                  lambda: db.get_chunk_state_counts(session_id),
                  key="chunk_states") or []
    transient = {row["status"]: int(row["n"]) for row in states
                 if row["status"] in TRANSIENT_CHUNK_STATES}
    report["transient_chunks"] = transient
    if AMBIGUOUS_CHUNK_STATE in transient:
        report["blocking"].append(
            f"{transient[AMBIGUOUS_CHUNK_STATE]} chunk(s) are '"
            f"{AMBIGUOUS_CHUNK_STATE}': a tape write began and its on-tape "
            "outcome cannot be known from the catalog alone")
    other_transient = {k: v for k, v in transient.items()
                       if k != AMBIGUOUS_CHUNK_STATE}
    if other_transient:
        report["blocking"].append(
            f"chunks are mid-flight ({other_transient}); a worker may still "
            "be running")

    shared = _try("shared plan check",
                  lambda: db.find_sessions_sharing_plan(session_id),
                  key="shared_plan_sessions") or []
    if shared:
        report["blocking"].append(
            f"{len(shared)} other session(s) share this plan or snapshot, so "
            "any decision here is also a decision about them")

    bound = _try("frontier state",
                 lambda: db.session_has_frontier_state(session_id))
    report["frontier"]["bound"] = bound
    if bound:
        report["frontier"]["scopes"] = _try(
            "frontier scopes", lambda: [dict(s) for s in
                                        db.get_scan_scopes(session_id)]) or []

    if archive_root:
        try:
            from .archive_artifacts import find_orphan_parts
            orphans = find_orphan_parts(archive_root)
            report["artifacts"]["orphan_parts"] = orphans
            if orphans:
                report["blocking"].append(
                    f"{len(orphans)} interrupted scan artifact(s) (.part) are "
                    "present and unreconciled")
        except Exception as exc:
            report["errors"].append(f"artifact scan: {exc}")
            report["blocking"].append(
                "local scan artifacts could not be inspected")

    if lock_holders:
        report["blocking"].append(
            f"the archiver advisory lock is held by {lock_holders}")
    if active_processes:
        report["blocking"].append(
            f"archive/transfer processes are running: {active_processes}")
    if lock_holders is None or active_processes is None:
        report["blocking"].append(
            "process/lock liveness was not established, so nothing can be "
            "assumed about a running worker")

    report["verdict"] = (VERDICT_READY if not report["blocking"]
                         else VERDICT_BLOCKED)
    return report


def remote_token_probe(remote_user, remote_host, remote_password="",
                       timeout=30):
    """A READ-ONLY probe for a tagged remote process group.

    Returns ``True`` (running), ``False`` (provably absent) or ``None``
    (unreachable/uncertain). It only ever runs ``pgrep``; terminating a proven
    orphan is a separate, explicit decision by the caller.
    """
    from .remote_transport import _ssh_run

    def probe(token):
        if not token:
            return False
        command = f"pgrep -f {token!r} >/dev/null 2>&1 && echo RUNNING || echo ABSENT"
        try:
            result = _ssh_run(remote_user, remote_host, command, capture=True,
                              password=remote_password, timeout=timeout)
        except Exception:
            return None
        if result.returncode == 255 or result.returncode == 124:
            return None                     # unreachable / timed out
        output = (result.stdout or "").strip()
        if output.endswith("RUNNING"):
            return True
        if output.endswith("ABSENT"):
            return False
        return None
    return probe


__all__ = ["AMBIGUOUS_CHUNK_STATE", "BLOCKED", "LIVE", "ProcessEvidence",
           "RELEASABLE", "StartupReconciler", "TRANSIENT_CHUNK_STATES",
           "VERDICT_BLOCKED", "VERDICT_READY", "classify_attempt",
           "local_process_evidence", "remote_process_evidence",
           "remote_token_probe", "session_frontier_report"]
