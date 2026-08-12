"""Propose, rehearse and persist a session's planning-source boundary.

Plan 3, Task 3.1. A session that has been running under database-backed
planning needs one durable instant at which future chunks start being planned
from local manifests instead. This module derives that instant **from the
database**, never from a literal.

Evidence discipline, stated once because it is the whole point:

* ``AGENTS.md`` is the latest written operational baseline, **not** live proof.
* Chunk counts in ``config.ini`` comments, design documents, incident snapshots
  and older tests are **historical only**.
* Every index here comes from a locked audit of the live catalog. There is no
  session number, chunk index or tape label literal in this module.

Report mode is read-only and is what runs against production. Execute mode
persists the approved boundary, and refuses on any of the blockers below.
"""
from dataclasses import dataclass, field

from .logsetup import get_logger

#: Chunk states that mean work is in flight. A boundary drawn while any of
#: these exists could split a chunk that is being written.
ACTIVE_CHUNK_STATES = ("fetching", "packing", "backing", "writing")

#: States that are ambiguous about what reached the tape.
AMBIGUOUS_CHUNK_STATES = ("backing",)


class TransitionBlocked(RuntimeError):
    """The boundary cannot be activated; the reason is the message."""


@dataclass
class TransitionProposal:
    """What the operator is being asked to approve."""

    session_id: int
    session_label: str
    last_legacy_planned_chunk: int
    first_manifest_first_chunk: int
    existing_chunk_count: int
    frontier_generation: int
    scan_complete: bool
    uncovered_scope_remains: bool
    source_scopes: tuple = ()
    tape_generation: int = 0
    blockers: tuple = ()
    warnings: tuple = ()
    chunk_states: dict = field(default_factory=dict)
    formats: dict = field(default_factory=dict)

    @property
    def activatable(self):
        return not self.blockers

    def as_dict(self):
        return {
            "session_id": self.session_id,
            "session_label": self.session_label,
            "last_legacy_planned_chunk": self.last_legacy_planned_chunk,
            "first_manifest_first_chunk": self.first_manifest_first_chunk,
            "existing_chunk_count": self.existing_chunk_count,
            "frontier_generation": self.frontier_generation,
            "scan_complete": self.scan_complete,
            "uncovered_scope_remains": self.uncovered_scope_remains,
            "source_scopes": list(self.source_scopes),
            "tape_generation": self.tape_generation,
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "chunk_states": dict(self.chunk_states),
            "formats": dict(self.formats),
            "activatable": self.activatable,
        }


def build_transition_proposal(db, session_id):
    """Derive the boundary from a locked read of the live catalog.

    Read-only. Returns a :class:`TransitionProposal` whose ``blockers`` list is
    empty only when every activation precondition holds.
    """
    audit = db.audit_session_transition_evidence(session_id)
    if audit is None:
        raise TransitionBlocked(f"remote session {session_id} does not exist")

    blockers = []
    warnings = []

    states = audit["chunk_states"]
    for state in ACTIVE_CHUNK_STATES:
        if states.get(state):
            blockers.append(
                f"{states[state]} chunk(s) are {state}; a boundary must not be "
                "drawn while work is in flight")
    for state in AMBIGUOUS_CHUNK_STATES:
        if states.get(state):
            blockers.append(
                f"{states[state]} chunk(s) are {state}: whether they reached "
                "the tape is unknowable from the catalog")

    if audit["owned_chunks"]:
        blockers.append(
            f"{audit['owned_chunks']} chunk(s) still carry an owner token or "
            "an unexpired lease")
    if audit["duplicate_ordinals"]:
        blockers.append(
            f"{audit['duplicate_ordinals']} chunk(s) have duplicate plan "
            "ordinals")
    if audit["sessions_sharing_plan"] > 1:
        blockers.append(
            f"this plan is shared by {audit['sessions_sharing_plan']} sessions; "
            "a boundary would silently move the others too")
    if not audit["chunks_contiguous"]:
        blockers.append(
            "chunk indexes are absent or non-contiguous, so 'the next index' "
            "is not well defined")
    if audit["bootstrap_state"] not in ("complete", "running"):
        blockers.append(
            f"the frontier bootstrap is {audit['bootstrap_state']!r}; an "
            "incomplete bootstrap cannot prove what is already covered")
    if audit["conflicting_formats"]:
        blockers.append(
            f"{audit['conflicting_formats']} chunk(s) disagree with their "
            "container about packaging format")
    if audit["tape_generation_mismatch"]:
        blockers.append(
            "the session's tape generation does not match its containers'")

    # Not blockers, but the operator must see them.
    if audit["null_membership_state"]:
        warnings.append(
            f"{audit['null_membership_state']} chunk(s) have no "
            "membership_state; their membership is fixed by status and plan "
            "rows rather than by an explicit seal")
    if audit["scan_complete"] and not audit["uncovered_scope_remains"]:
        warnings.append(
            "the authoritative scan is already final with no uncovered scope: "
            "persist and report the boundary, but do NOT fabricate chunks "
            "merely to make the session mixed-format")

    proposal = TransitionProposal(
        session_id=int(audit["session_id"]),
        session_label=audit["session_label"],
        last_legacy_planned_chunk=int(audit["max_chunk_index"]),
        first_manifest_first_chunk=int(audit["max_chunk_index"]) + 1,
        existing_chunk_count=int(audit["chunk_count"]),
        frontier_generation=int(audit["frontier_generation"]),
        scan_complete=bool(audit["scan_complete"]),
        uncovered_scope_remains=bool(audit["uncovered_scope_remains"]),
        source_scopes=tuple(audit["source_scopes"]),
        tape_generation=int(audit["tape_generation"] or 0),
        blockers=tuple(blockers),
        warnings=tuple(warnings),
        chunk_states=dict(states),
        formats=dict(audit["formats"]))

    get_logger().info(
        "session_transition_proposal: session=%s label=%s last_legacy=%s "
        "first_manifest=%s blockers=%d",
        proposal.session_id, proposal.session_label,
        proposal.last_legacy_planned_chunk, proposal.first_manifest_first_chunk,
        len(proposal.blockers))
    return proposal


def render_proposal(proposal):
    """A plain-text report an operator can read before approving."""
    lines = [
        "Session planning-source transition proposal (READ-ONLY)",
        f"  session            : {proposal.session_label} "
        f"(id {proposal.session_id})",
        f"  existing chunks    : {proposal.existing_chunk_count} "
        f"(0..{proposal.last_legacy_planned_chunk})",
        f"  last legacy chunk  : {proposal.last_legacy_planned_chunk}",
        f"  first manifest chunk: {proposal.first_manifest_first_chunk}",
        f"  frontier generation: {proposal.frontier_generation}",
        f"  scan complete      : {proposal.scan_complete}",
        f"  uncovered scope    : {proposal.uncovered_scope_remains}",
        f"  source scopes      : {len(proposal.source_scopes)}",
        f"  tape generation    : {proposal.tape_generation}",
        "  chunk states       : " + ", ".join(
            f"{k}={v}" for k, v in sorted(proposal.chunk_states.items())),
        "  packaging formats  : " + (", ".join(
            f"{k or 'unset'}={v}"
            for k, v in sorted(proposal.formats.items(),
                               key=lambda kv: str(kv[0]))) or "none recorded"),
    ]
    if proposal.warnings:
        lines.append("  warnings:")
        lines.extend(f"    - {w}" for w in proposal.warnings)
    if proposal.blockers:
        lines.append("  BLOCKERS (activation refused):")
        lines.extend(f"    - {b}" for b in proposal.blockers)
    else:
        lines.append("  no blockers: the boundary is activatable")
    lines.append("  No tape or LTFS access was performed.")
    return "\n".join(lines)
