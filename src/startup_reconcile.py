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
import json
import os
import uuid

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


# ---------------------------------------------------------------------------
# Plan 2, Task 2.6: Stored-TAR artifact-pair startup reconciliation
# ---------------------------------------------------------------------------

TAR_RECONCILE_READY = "ready"
TAR_RECONCILE_REBUILD = "rebuild"
TAR_RECONCILE_BLOCKED = "blocked"


def _tar_value(record, name, default=None):
    if isinstance(record, dict):
        return record.get(name, default)
    return getattr(record, name, default)


def _tar_local_path(path, root, label):
    """Resolve a local artifact path without ever following a DB locator away.

    In particular, a corrupt/old locator naming the LTFS drive is rejected
    before any ``stat``/open call can touch it.
    """
    value = os.path.abspath(os.fspath(path))
    boundary = os.path.abspath(os.fspath(root))
    try:
        inside = os.path.commonpath((value, boundary)) == boundary
    except ValueError:
        inside = False
    if not inside:
        raise RuntimeError(f"{label} is outside its local reconciliation root")
    return value


def _tar_files_equal(left, right, chunk_size=1024 * 1024):
    """Bounded byte comparison; deliberately no content hashes."""
    try:
        if os.path.getsize(left) != os.path.getsize(right):
            return False
        with open(left, "rb") as one, open(right, "rb") as two:
            while True:
                a = one.read(chunk_size)
                b = two.read(chunk_size)
                if a != b:
                    return False
                if not a:
                    return True
    except OSError:
        return False


def _quarantine_tar_part(path, suffix=".reconciled-unready", *,
                         quarantine_root=None):
    """Move one ownerless part aside without replacing earlier evidence."""
    if quarantine_root is None:
        target = f"{path}{suffix}"
    else:
        os.makedirs(quarantine_root, exist_ok=True)
        target = os.path.join(
            quarantine_root, os.path.basename(path) + suffix)
    if not os.path.exists(path):
        return target
    if os.path.exists(target):
        raise RuntimeError(
            f"quarantine destination already exists for {path!r}")
    os.rename(path, target)
    return target


def _normalise_owner_verdict(value):
    if value is True:
        return LIVE
    if value is False:
        return RELEASABLE
    if value is None:
        return BLOCKED
    text = str(value).strip().lower()
    if text in (LIVE, RELEASABLE, BLOCKED):
        return text
    return BLOCKED


def _stored_tar_owner_verdict(db, session_id, row, *, owner_probe=None,
                              psutil_module=None, remote_probe=None):
    """Tri-state owner evidence.  Missing evidence is never absence evidence."""
    owner = _tar_value(row, "owner_token")
    if not owner:
        return BLOCKED, "the build state has no owner token"
    if owner_probe is not None:
        try:
            verdict = _normalise_owner_verdict(owner_probe(owner, row))
        except Exception as exc:
            return BLOCKED, f"the build-owner probe failed: {exc}"
        return verdict, f"the build-owner probe returned {verdict}"

    reader = getattr(db, "list_live_worker_attempts", None)
    if not callable(reader):
        return BLOCKED, "no durable build-owner evidence reader is available"
    try:
        attempts = [dict(item) for item in reader(session_id)
                    if item.get("owner_token") == owner]
    except Exception as exc:
        return BLOCKED, f"build-owner evidence could not be read: {exc}"
    if len(attempts) != 1:
        return BLOCKED, (
            "the build owner has no unique live worker-attempt record")
    evidence = classify_attempt(
        attempts[0], psutil_module=psutil_module, remote_probe=remote_probe)
    return evidence.verdict, "; ".join(evidence.reasons)


def _stored_tar_plan_for_container(plan, container, remote_base):
    from .paths import remote_store_base_and_rel

    ordinal = int(_tar_value(container, "container_ordinal"))
    members = []
    for item in _tar_value(plan, "members", ()):
        assigned = _tar_value(item, "container_ordinal")
        if assigned is None or int(assigned) != ordinal:
            continue
        remote_path = _tar_value(item, "remote_path")
        member_name = _tar_value(item, "member_name")
        if member_name is None:
            if remote_base is None:
                raise RuntimeError(
                    "the Stored TAR plan needs its configured remote base to "
                    "recover member names")
            _base, member_name = remote_store_base_and_rel(
                remote_base, remote_path)
        members.append({
            "member_name": str(member_name),
            "canonical_source_path": str(
                _tar_value(item, "canonical_source_path", remote_path)),
            "expected_size": int(_tar_value(
                item, "file_size_bytes",
                _tar_value(item, "expected_size"))),
            "plan_ordinal": int(_tar_value(
                item, "plan_ordinal", _tar_value(item, "ordinal"))),
            "container_ordinal": ordinal,
        })
    if not members:
        raise RuntimeError("Stored TAR container has no persisted plan members")
    return sorted(members, key=lambda item: item["plan_ordinal"])


def _stored_tar_diagnostics(container, sidecar_path, *, session_id,
                            chunk_index):
    from .archive_artifacts import read_tar_sidecar_diagnostics

    summary = _tar_value(container, "validation_summary") or {}
    if summary.get("source_diagnostics") is not None:
        return tuple(summary.get("source_diagnostics") or ())
    if os.path.isfile(sidecar_path):
        return read_tar_sidecar_diagnostics(
            sidecar_path, session_id=session_id, chunk_index=chunk_index,
            container_id=int(_tar_value(container, "container_id")),
            container_ordinal=int(_tar_value(
                container, "container_ordinal")))
    return ()


def _validate_stored_tar_pair(container, artifacts, plan_members, tar_path,
                              sidecar_path, diagnostics, *, session_id,
                              chunk_index, require_db_ready):
    from .archive_artifacts import (TAR_SIDECAR_VERSION,
                                    tar_sidecar_locator,
                                    validate_tar_sidecar)
    from .tar_container import validate_stored_tar_part

    ordinal = int(_tar_value(container, "container_ordinal"))
    container_id = int(_tar_value(container, "container_id"))
    validation = validate_stored_tar_part(
        tar_path, plan_members, container_ordinal=ordinal,
        source_diagnostics=diagnostics, require_part=tar_path.endswith(".part"))
    counts = validate_tar_sidecar(
        sidecar_path, plan_members, validation, diagnostics,
        session_id=session_id, chunk_index=chunk_index,
        container_id=container_id, container_ordinal=ordinal,
        tar_size=validation.archive_size)

    if require_db_ready:
        locator = tar_sidecar_locator(session_id, chunk_index, ordinal)
        sidecars = [row for row in artifacts
                    if _tar_value(row, "container_id") == container_id
                    and _tar_value(row, "artifact_kind") == "tar_sidecar"]
        if len(sidecars) != 1:
            raise RuntimeError("ready pair lacks one authoritative sidecar row")
        artifact = sidecars[0]
        checks = {
            "validation_state": "ready",
            "temporary_data_locator": tar_path,
            "permanent_local_metadata_locator": locator,
            "actual_artifact_bytes": validation.archive_size,
            "observed_member_count": validation.member_count,
            "observed_logical_bytes": validation.logical_bytes,
            "disposition_counts": dict(counts),
        }
        mismatch = [key for key, expected in checks.items()
                    if _tar_value(container, key) != expected]
        artifact_checks = {
            "artifact_version": TAR_SIDECAR_VERSION,
            "local_locator": locator,
            "artifact_size_bytes": os.path.getsize(sidecar_path),
            "readiness_state": "ready",
        }
        mismatch.extend(
            f"artifact.{key}" for key, expected in artifact_checks.items()
            if _tar_value(artifact, key) != expected)
        if mismatch:
            raise RuntimeError(
                "ready database pair disagrees with local artifacts: "
                + ", ".join(mismatch))
    return validation, counts


def _pack_inventory_for_reconcile(pack_dir):
    items = []
    for root, _dirs, files in os.walk(pack_dir):
        for name in files:
            if name == "_resume_pack.json":
                continue
            full = os.path.join(root, name)
            rel = os.path.relpath(full, pack_dir).replace("\\", "/")
            try:
                items.append([rel, os.path.getsize(full)])
            except OSError as exc:
                raise RuntimeError(
                    "resume pack inventory is unreadable") from exc
    return sorted(items)


def _validate_tar_resume_marker(db, marker_path, *, session_id, chunk_index,
                                pack_dir):
    from .pipeline_types import (ContainerFormat, StagedArtifact, StagedChunk,
                                 StagedContainer)

    try:
        with open(marker_path, encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, ValueError) as exc:
        raise RuntimeError("ready _resume_pack.json is unreadable") from exc
    if (payload.get("version") != 1
            or payload.get("session_id") != int(session_id)
            or payload.get("chunk_index") != int(chunk_index)
            or payload.get("packaging_format") !=
            ContainerFormat.STORED_TAR.value):
        raise RuntimeError(
            "ready _resume_pack.json identity/format is inconsistent")
    if payload.get("pack_dir") and os.path.abspath(payload["pack_dir"]) != pack_dir:
        raise RuntimeError("ready _resume_pack.json names another pack")
    if payload.get("pack_inventory") != _pack_inventory_for_reconcile(pack_dir):
        raise RuntimeError("ready _resume_pack.json inventory changed")
    try:
        desc = StagedChunk(
            chunk_index=int(chunk_index),
            fetch_dir=payload["fetch_dir"], pack_dir=pack_dir,
            metadata=payload.get("metadata") or [],
            staged_bytes=int(payload.get("staged_bytes") or 0),
            fetch_seconds=payload.get("fetch_seconds"),
            fetch_bytes=payload.get("fetch_bytes"),
            pack_seconds=payload.get("pack_seconds"),
            pack_bytes=payload.get("pack_bytes"),
            ram_stats=payload.get("ram_stats") or {},
            source_missing_files=payload.get("source_missing_files") or [],
            skip_tape=bool(payload.get("skip_tape")),
            session_id=int(session_id),
            packaging_format=ContainerFormat.STORED_TAR,
            containers=[StagedContainer(**item)
                        for item in payload.get("containers") or []],
            artifacts=[StagedArtifact(**item)
                       for item in payload.get("artifacts") or []],
        )
        # The marker is control state, not archive payload.  Move it outside
        # Robocopy's source tree while applying the exact pack validator, then
        # restore it so RemoteChunkStager can consume it atomically on resume.
        check_marker = os.path.join(
            os.path.dirname(pack_dir),
            f".{os.path.basename(pack_dir)}._resume_pack.reconcile-check")
        if os.path.exists(check_marker):
            raise RuntimeError("resume-marker check path already exists")
        os.rename(marker_path, check_marker)
        try:
            desc.assert_writer_ready()
            validate_db = getattr(db, "validate_staged_chunk_readiness", None)
            if not callable(validate_db):
                raise RuntimeError(
                    "resume pack has no database readiness validator")
            validate_db(desc)
        finally:
            os.rename(check_marker, marker_path)
    except (KeyError, TypeError, ValueError, RuntimeError) as exc:
        raise RuntimeError(f"ready _resume_pack.json is invalid: {exc}") from exc
    return desc


def reconcile_tar_artifacts(
        db, session_id, chunk_index, *, archive_root, pack_dir,
        remote_base=None, owner_probe=None, psutil_module=None,
        remote_probe=None, recovery_owner_token=None, plan=None):
    """Reconcile every Stored-TAR local/DB combination for one sealed chunk.

    The function is intentionally local-filesystem + PostgreSQL only.  All
    paths are constrained below the supplied staging/manifest roots before
    they are inspected, and no LTFS module or tape locator is consulted.

    ``owner_probe(token, container_row)`` is tri-state: ``True``/``live``;
    ``False``/``releasable``; or ``None``/``blocked``.  Without conclusive
    absence evidence, an owned part is left untouched and the chunk blocks.
    """
    from .archive_artifacts import (resolve_locator, tar_sidecar_locator,
                                    publish_stored_tar_pair)
    from .tar_container import StoredTarError, validate_stored_tar_part

    session_id = int(session_id)
    chunk_index = int(chunk_index)
    archive_root = os.path.abspath(os.fspath(archive_root))
    pack_dir = os.path.abspath(os.fspath(pack_dir))
    recovery_owner = str(recovery_owner_token or (
        f"reconcile-{uuid.uuid4().hex}"))
    pack_quarantine = os.path.join(
        os.path.dirname(pack_dir), "_reconcile_quarantine",
        os.path.basename(pack_dir))
    sidecar_quarantine = os.path.join(
        archive_root, "_reconcile_quarantine",
        f"session_{session_id:06d}_chunk_{chunk_index:06d}")
    report = {
        "session_id": session_id,
        "chunk_index": chunk_index,
        "verdict": TAR_RECONCILE_READY,
        "cases": [],
        "blocking": [],
        "changed": False,
        "resume_pack": None,
    }

    def record(case, verdict, detail, *, container_id=None, action=None):
        item = {"case": case, "verdict": verdict, "detail": detail}
        if container_id is not None:
            item["container_id"] = int(container_id)
        if action is not None:
            item["action"] = action
        report["cases"].append(item)
        if verdict == TAR_RECONCILE_BLOCKED:
            report["blocking"].append(detail)

    try:
        containers = [dict(row) for row in
                      db.get_archive_containers(session_id, chunk_index)
                      if _tar_value(row, "container_format") == "stored_tar"]
        artifacts = [dict(row) for row in
                     db.get_archive_artifacts(session_id, chunk_index)]
        if plan is None:
            plan = db.get_stored_tar_chunk_plan(session_id, chunk_index)
    except Exception as exc:
        record("database_artifact_state_inconsistent",
               TAR_RECONCILE_BLOCKED,
               f"Stored TAR reconciliation inventory failed: {exc}")
        report["verdict"] = TAR_RECONCILE_BLOCKED
        return report

    referenced_parts = set()
    for container in containers:
        container_id = int(container["container_id"])
        ordinal = int(container["container_ordinal"])
        state = str(container.get("validation_state") or "")
        starting_state = state
        owner = container.get("owner_token")
        try:
            plan_members = _stored_tar_plan_for_container(
                plan, container, remote_base)
            final_tar = _tar_local_path(
                os.path.join(pack_dir, container["container_name"]), pack_dir,
                "final TAR")
            stored_final = container.get("temporary_data_locator")
            if stored_final is not None:
                stored_final = _tar_local_path(
                    stored_final, pack_dir, "database final TAR locator")
                if stored_final != final_tar:
                    raise RuntimeError(
                        "database final TAR locator conflicts with its plan name")
            part = container.get("validated_part_locator")
            if part is not None:
                part = _tar_local_path(part, pack_dir, "validated TAR part")
                if not part.lower().endswith(".part"):
                    raise RuntimeError("validated TAR locator is not a .part")
                referenced_parts.add(part)
            sidecar_locator = tar_sidecar_locator(
                session_id, chunk_index, ordinal)
            sidecar_path = _tar_local_path(
                resolve_locator(archive_root, sidecar_locator), archive_root,
                "TAR sidecar")
            part_exists = bool(part and os.path.isfile(part))
            final_exists = os.path.isfile(final_tar)
            sidecar_exists = os.path.isfile(sidecar_path)

            sidecar_rows = [row for row in artifacts
                            if row.get("container_id") == container_id
                            and row.get("artifact_kind") == "tar_sidecar"]
            if len(sidecar_rows) > 1:
                raise RuntimeError("multiple TAR sidecar database rows exist")
            if (state != "ready" and sidecar_rows
                    and sidecar_rows[0].get("readiness_state") == "ready"):
                raise RuntimeError(
                    "ready sidecar database row has a non-ready container")

            if part_exists and final_exists:
                if not _tar_files_equal(part, final_tar):
                    blocker = getattr(db, "block_stored_tar_container", None)
                    if (callable(blocker)
                            and container.get("writer_state") == "not_started"
                            and container.get("catalog_state") == "not_started"):
                        blocker(container_id, state, owner)
                        report["changed"] = True
                    record("final_part_name_collision",
                           TAR_RECONCILE_BLOCKED,
                           "final TAR conflicts byte-for-byte with its part",
                           container_id=container_id)
                    continue

            if state == "ready":
                if owner is not None:
                    raise RuntimeError("ready Stored TAR retains a build owner")
                if not final_exists or not sidecar_exists:
                    missing = "final TAR" if not final_exists else "sidecar"
                    raise RuntimeError(f"ready pair is missing its {missing}")
                diagnostics = _stored_tar_diagnostics(
                    container, sidecar_path, session_id=session_id,
                    chunk_index=chunk_index)
                _validate_stored_tar_pair(
                    container, artifacts, plan_members, final_tar, sidecar_path,
                    diagnostics, session_id=session_id,
                    chunk_index=chunk_index, require_db_ready=True)
                if part_exists:
                    _quarantine_tar_part(
                        part, ".reconciled-equivalent",
                        quarantine_root=pack_quarantine)
                    report["changed"] = True
                    record("final_part_name_collision", TAR_RECONCILE_READY,
                           "equivalent ownerless part quarantined after full "
                           "ready-pair validation", container_id=container_id,
                           action="quarantined_equivalent_part")
                else:
                    record("ready_pair", TAR_RECONCILE_READY,
                           "ready pair and database records are fully equivalent",
                           container_id=container_id, action="none")
                continue

            local_evidence = part_exists or final_exists or sidecar_exists
            if state == "planned":
                if local_evidence or sidecar_rows:
                    raise RuntimeError(
                        "planned database state conflicts with local/final "
                        "artifact evidence")
                record("database_artifact_state_inconsistent",
                       TAR_RECONCILE_REBUILD,
                       "planned container has no published local artifacts",
                       container_id=container_id, action="rebuild")
                continue
            if state not in ("building", "validated_part"):
                raise RuntimeError(
                    f"database container state {state!r} is not reconcilable")

            verdict, owner_detail = _stored_tar_owner_verdict(
                db, session_id, container, owner_probe=owner_probe,
                psutil_module=psutil_module, remote_probe=remote_probe)
            if verdict != RELEASABLE:
                case = ("concurrent_build_owner" if verdict == LIVE
                        else "expired_or_unknown_build_owner")
                record(case, TAR_RECONCILE_BLOCKED,
                       f"build owner {owner!r} is {verdict}: {owner_detail}",
                       container_id=container_id)
                continue
            db.reconcile_stored_tar_build_owner(
                container_id, owner, state, recovery_owner)
            report["changed"] = True
            container["owner_token"] = recovery_owner

            if not part_exists and not final_exists:
                if sidecar_exists:
                    raise RuntimeError(
                        "final sidecar exists without either TAR part or final")
                db.reset_reconciled_stored_tar_build(
                    container_id, recovery_owner, state)
                record("orphan_tar_part", TAR_RECONCILE_REBUILD,
                       "dead build owner left no reusable TAR; DB reset for "
                       "source rebuild", container_id=container_id,
                       action="reset_for_rebuild")
                continue

            diagnostics = _stored_tar_diagnostics(
                container, sidecar_path, session_id=session_id,
                chunk_index=chunk_index)
            validation_source = part if part_exists else final_tar
            try:
                validation = validate_stored_tar_part(
                    validation_source, plan_members,
                    container_ordinal=ordinal,
                    source_diagnostics=diagnostics,
                    require_part=validation_source.endswith(".part"))
            except StoredTarError as exc:
                if part_exists and not final_exists and not sidecar_exists:
                    db.reset_reconciled_stored_tar_build(
                        container_id, recovery_owner, state)
                    quarantined = _quarantine_tar_part(
                        part, quarantine_root=pack_quarantine)
                    report["changed"] = True
                    record("orphan_tar_part", TAR_RECONCILE_REBUILD,
                           f"invalid ownerless part quarantined at {quarantined}; "
                           "source rebuild required", container_id=container_id,
                           action="quarantine_and_rebuild")
                    continue
                raise RuntimeError(f"local TAR is invalid: {exc}") from exc

            if state == "building":
                db.mark_stored_tar_validated_part(
                    container_id, recovery_owner,
                    part or f"{final_tar}.reconcile.part", validation,
                    diagnostics)
                state = "validated_part"
                container["validation_state"] = state
            publication = publish_stored_tar_pair(
                archive_root, part or f"{final_tar}.reconcile.part", final_tar,
                plan_members, diagnostics, validation=validation,
                session_id=session_id, chunk_index=chunk_index,
                container_id=container_id, container_ordinal=ordinal,
                owner_token=recovery_owner, db=db, pack_dir=pack_dir)
            state = "ready"
            container["validation_state"] = state
            container["owner_token"] = None
            report["changed"] = True
            if sidecar_exists and part_exists and not final_exists:
                case = "final_sidecar_plus_tar_part"
            elif sidecar_exists and final_exists:
                case = "final_pair_before_db_commit"
            elif final_exists and not sidecar_exists:
                case = "final_tar_absent_sidecar"
            elif starting_state == "validated_part":
                case = "validated_tar_part_before_sidecar"
            else:
                case = "orphan_tar_part"
            record(case, TAR_RECONCILE_READY,
                   "local artifacts fully revalidated and adopted by owner/state "
                   "CAS", container_id=container_id, action="adopted_pair")

            # A crash after sidecar validation but before publication can leave
            # only owner-scoped sidecar .parts.  Once adoption succeeds, that
            # owner is proven dead and these unpublished remnants are safe to
            # quarantine.
            sidecar_dir = os.path.dirname(publication.sidecar_path)
            sidecar_prefix = os.path.basename(publication.sidecar_path) + "."
            if os.path.isdir(sidecar_dir):
                for name in os.listdir(sidecar_dir):
                    candidate = os.path.join(sidecar_dir, name)
                    if (name.startswith(sidecar_prefix)
                            and name.endswith(".part")
                            and os.path.isfile(candidate)):
                        _quarantine_tar_part(
                            candidate, quarantine_root=sidecar_quarantine)
        except Exception as exc:
            detail = (f"container {container_id} reconciliation blocked: {exc}")
            blocker = getattr(db, "block_stored_tar_container", None)
            if (callable(blocker) and state in (
                    "planned", "building", "validated_part", "ready")
                    and _tar_value(container, "writer_state") == "not_started"
                    and _tar_value(container, "catalog_state") == "not_started"):
                try:
                    expected_owner = container.get("owner_token")
                    # If ownership was successfully reclaimed above, the local
                    # row carries that new token.
                    blocker(container_id, state, expected_owner)
                    report["changed"] = True
                except Exception as block_exc:
                    detail += f"; durable block CAS also failed: {block_exc}"
            record("database_artifact_state_inconsistent",
                   TAR_RECONCILE_BLOCKED, detail,
                   container_id=container_id)

    # Unreferenced TAR parts cannot belong to a conforming live builder: the
    # protocol persists owner + unique part locator before streaming byte one.
    if os.path.isdir(pack_dir):
        for root, _dirs, files in os.walk(pack_dir):
            for name in files:
                lower = name.lower()
                if not lower.endswith(".part") or ".tar" not in lower:
                    continue
                path = os.path.abspath(os.path.join(root, name))
                if path in referenced_parts:
                    continue
                try:
                    quarantined = _quarantine_tar_part(
                        path, quarantine_root=pack_quarantine)
                    report["changed"] = True
                    record("orphan_tar_part", TAR_RECONCILE_REBUILD,
                           f"unreferenced TAR part quarantined at {quarantined}",
                           action="quarantined_orphan")
                except Exception as exc:
                    record("orphan_tar_part", TAR_RECONCILE_BLOCKED,
                           f"unreferenced TAR part could not be quarantined: {exc}")

    marker = os.path.join(pack_dir, "_resume_pack.json")
    if os.path.isfile(marker):
        try:
            _validate_tar_resume_marker(
                db, marker, session_id=session_id, chunk_index=chunk_index,
                pack_dir=pack_dir)
            report["resume_pack"] = "ready"
            record("ready_resume_pack", TAR_RECONCILE_READY,
                   "resume marker, exact inventory, local members, and database "
                   "records are equivalent", action="reuse")
        except Exception as exc:
            report["resume_pack"] = "blocked"
            record("ready_resume_pack", TAR_RECONCILE_BLOCKED,
                   f"ready resume pack is invalid: {exc}")

    if report["blocking"]:
        report["verdict"] = TAR_RECONCILE_BLOCKED
    elif any(item["verdict"] == TAR_RECONCILE_REBUILD
             for item in report["cases"]):
        report["verdict"] = TAR_RECONCILE_REBUILD
    return report


__all__ = ["AMBIGUOUS_CHUNK_STATE", "BLOCKED", "LIVE", "ProcessEvidence",
           "RELEASABLE", "StartupReconciler", "TRANSIENT_CHUNK_STATES",
           "TAR_RECONCILE_BLOCKED", "TAR_RECONCILE_READY",
           "TAR_RECONCILE_REBUILD", "reconcile_tar_artifacts",
           "VERDICT_BLOCKED", "VERDICT_READY", "classify_attempt",
           "local_process_evidence", "remote_process_evidence",
           "remote_token_probe", "session_frontier_report"]
