"""Observation-only shadow of the sealed-batch scheduler (Phase 5C).

The observer computes what a sealed batch WOULD be for the group the existing
Phase 4.5 scheduler selects, and compares that proposal against the ReadyQueue
selection and the authoritative ``remote_chunks.status`` — WITHOUT participating
in any production decision.

It is constructed only when ``[FEATURES] sealed_tape_write_batches_observation_enabled``
is true (via :func:`maybe_build_observer`); the default is false, so no observer
exists in production. Even when constructed it is pure computation over a COPIED
snapshot plus a read-only status map: it never mutates a batch table, never
touches the ReadyQueue, never acquires LTFS ownership, never runs readiness /
cartridge / robocopy, never changes chunk status, never deletes packs, and never
influences stop / resume / retry. Its records are diagnostic and append-only;
they never prune, restore, mark durable, or override ``remote_chunks``.

The observer holds no lock while computing: it operates on a plain
:class:`ReadyGroupSnapshot` that the caller copies out of the ReadyQueue, so it
can later run fully asynchronously from a scheduling snapshot.
"""
import time
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Optional

from .sealed_batch_repository import (compute_batch_fingerprint,
                                      FORMAT_GENERATION)

OBSERVER_VERSION = "5c.1"
OBSERVATION_SCHEMA_VERSION = 1

# Proposed sealed-batch lifecycle state the observer would assign (never written).
PROPOSED_BUILDING = "building"
PROPOSED_SEALABLE = "sealable"          # a finite group ready to seal
PROPOSED_BLOCKED = "blocked"            # cannot be sealed as-is (needs reconcile)

# Comparison classifications.
MATCH = "MATCH"
EXPECTED_DIFFERENCE = "EXPECTED_DIFFERENCE"
OBSERVER_ERROR = "OBSERVER_ERROR"
SCHEDULER_ERROR = "SCHEDULER_ERROR"
DATA_INCONSISTENCY = "DATA_INCONSISTENCY"
RECONCILIATION_REQUIRED = "RECONCILIATION_REQUIRED"

# Mismatch codes.
MM_INELIGIBLE_IN_DB = "chunk_eligible_in_queue_but_not_in_db"
MM_DUPLICATE_CLAIM = "duplicate_active_claim"
MM_FINGERPRINT = "fingerprint_mismatch"
MM_MISSING_PACK_META = "missing_pack_metadata"
MM_BYTE_COUNT = "byte_count_mismatch"
MM_ORDER = "chunk_order_mismatch"
MM_TAPE = "expected_tape_mismatch"
MM_ALREADY_DONE = "prepared_chunk_already_done"
MM_BACKING_AMBIGUOUS = "backing_chunk_unresolved_ambiguity"

# Authoritative statuses under which a prepared (ready) chunk is legitimately
# awaiting its tape write. A ready chunk in any OTHER status is a mismatch.
_ELIGIBLE_STATUSES = ("pending", "fetching", "packing")

# Severity order for choosing the record classification (highest wins).
_SEVERITY = {
    MATCH: 0,
    EXPECTED_DIFFERENCE: 1,
    SCHEDULER_ERROR: 2,
    OBSERVER_ERROR: 3,
    RECONCILIATION_REQUIRED: 4,
    DATA_INCONSISTENCY: 5,
}


@dataclass
class SnapshotMember:
    """One prepared chunk as the scheduler sees it (copied from the ReadyQueue)."""
    chunk_index: int
    prepared_bytes: int
    pack_identity: Optional[str]
    file_count: Optional[int] = None
    pack_fingerprint: Optional[str] = None


@dataclass
class ReadyGroupSnapshot:
    """A COPY of the group the Phase 4.5 scheduler would select. The observer
    never holds the ReadyQueue lock; the caller fills this out and hands it over."""
    session_id: int
    ready_queue_generation: int
    selection_reason: str
    members: list                       # list[SnapshotMember], in write order
    expected_tape: str
    scan_complete: bool
    producer_state: str                 # 'active' | 'exhausted' | 'closed' | 'failed'
    staging_pressure: bool
    safe_stop: bool
    aggregate_prepared_bytes: Optional[int] = None   # scheduler's own total, if any


class SealedBatchObserver:
    """Pure, observation-only calculator. No production side effects."""

    def __init__(self, sink=None, now=None):
        # sink(record: dict) -> None : append-only diagnostic emitter (local log
        # or shadow-only table). Never a production table. Optional.
        self._sink = sink
        self._now = now or (lambda: datetime.now(timezone.utc))

    # -- eligibility -----------------------------------------------------
    @staticmethod
    def _member_eligibility(member, remote_status):
        """Return (mismatch_codes, classification) for one member vs authority."""
        codes = []
        cls = MATCH
        if member.pack_identity is None or member.prepared_bytes is None:
            codes.append(MM_MISSING_PACK_META)
            cls = OBSERVER_ERROR
        if remote_status is None:
            codes.append(MM_INELIGIBLE_IN_DB)
            return codes, DATA_INCONSISTENCY
        if remote_status == "done":
            # A ready item must never already be archived-done: authoritative
            # state contradicts the scheduler. Never silently treat as reusable.
            codes.append(MM_ALREADY_DONE)
            cls = DATA_INCONSISTENCY
        elif remote_status == "backing":
            # Chunk 108's exact case: physically ambiguous. The observer flags it
            # for explicit reconciliation; it does NOT infer reusability from any
            # evidence outside PostgreSQL.
            codes.append(MM_BACKING_AMBIGUOUS)
            cls = RECONCILIATION_REQUIRED
        elif remote_status not in _ELIGIBLE_STATUSES:
            codes.append(MM_INELIGIBLE_IN_DB)
            cls = DATA_INCONSISTENCY
        return codes, cls

    # -- observation -----------------------------------------------------
    def observe_group(self, snapshot, remote_status):
        """Compute an observation record for one selected group.

        ``remote_status``: mapping chunk_index -> authoritative
        remote_chunks.status (fetched by the CALLER via a read-only query,
        outside any writer transaction). Returns a plain dict (append-only).
        """
        members = list(snapshot.members)
        ordered_ids = [m.chunk_index for m in members]
        per_chunk_bytes = {m.chunk_index: m.prepared_bytes for m in members}
        aggregate = sum(int(m.prepared_bytes or 0) for m in members)

        codes = set()
        classifications = [MATCH]

        # Duplicate membership within the proposed group.
        if len(set(ordered_ids)) != len(ordered_ids):
            codes.add(MM_DUPLICATE_CLAIM)
            classifications.append(DATA_INCONSISTENCY)

        # The scheduler's own aggregate, if supplied, must agree.
        if snapshot.aggregate_prepared_bytes is not None and \
                snapshot.aggregate_prepared_bytes != aggregate:
            codes.add(MM_BYTE_COUNT)
            classifications.append(SCHEDULER_ERROR)

        # Ordering must be the write order the scheduler handed us (stable).
        if ordered_ids != [m.chunk_index for m in snapshot.members]:
            codes.add(MM_ORDER)
            classifications.append(OBSERVER_ERROR)

        per_member_status = {}
        for m in members:
            st = remote_status.get(m.chunk_index)
            per_member_status[m.chunk_index] = st
            mcodes, mcls = self._member_eligibility(m, st)
            codes.update(mcodes)
            classifications.append(mcls)

        # Fingerprint over the ordered membership + tape + format. Deterministic:
        # computed twice to assert stability (observer sanity, never production).
        fp_members = [
            {"ordinal": i, "chunk_index": m.chunk_index,
             "pack_identity": m.pack_identity or "",
             "prepared_bytes": int(m.prepared_bytes or 0),
             "file_count": int(m.file_count or 0),
             "pack_fingerprint": m.pack_fingerprint}
            for i, m in enumerate(members)]
        fp1 = compute_batch_fingerprint(
            snapshot.expected_tape, FORMAT_GENERATION, fp_members)
        fp2 = compute_batch_fingerprint(
            snapshot.expected_tape, FORMAT_GENERATION, fp_members)
        if fp1 != fp2:                                   # pragma: no cover
            codes.add(MM_FINGERPRINT)
            classifications.append(OBSERVER_ERROR)

        classification = max(classifications, key=lambda c: _SEVERITY[c])
        # Proposed lifecycle: a group with a blocking condition (reconcile /
        # inconsistency) is NOT sealable as-is.
        if classification in (RECONCILIATION_REQUIRED, DATA_INCONSISTENCY,
                              OBSERVER_ERROR):
            proposed_state = PROPOSED_BLOCKED
        elif not members:
            proposed_state = PROPOSED_BUILDING
        else:
            proposed_state = PROPOSED_SEALABLE

        record = {
            "observation_id": uuid.uuid4().hex,
            "session_id": snapshot.session_id,
            "observation_timestamp": self._now().isoformat(),
            "ready_queue_generation": snapshot.ready_queue_generation,
            "selection_reason": snapshot.selection_reason,
            "ordered_chunk_ids": ordered_ids,
            "prepared_bytes_per_chunk": per_chunk_bytes,
            "aggregate_prepared_bytes": aggregate,
            "pack_identities": {m.chunk_index: m.pack_identity for m in members},
            "expected_tape": snapshot.expected_tape,
            "proposed_fingerprint": fp1,
            "scan_complete": snapshot.scan_complete,
            "producer_state": snapshot.producer_state,
            "staging_pressure_state": snapshot.staging_pressure,
            "safe_stop_state": snapshot.safe_stop,
            "remote_status_per_member": per_member_status,
            "proposed_lifecycle_state": proposed_state,
            "validation_result": ("ok" if classification == MATCH else "flagged"),
            "classification": classification,
            "mismatch_codes": sorted(codes),
            "observer_version": OBSERVER_VERSION,
            "observation_schema_version": OBSERVATION_SCHEMA_VERSION,
        }
        if self._sink is not None:
            try:
                self._sink(record)
            except Exception:
                # An observer/sink failure must NEVER propagate into the writer.
                pass
        return record

    # -- comparison to Phase 4.5 selection -------------------------------
    def compare_to_scheduler(self, snapshot, remote_status,
                             readyqueue_selected_ids):
        """Compare the observer's proposal to the ReadyQueue's actual selection.

        Returns (record, comparison). ``comparison`` explains chunk-set, order,
        byte, reason and tape agreement, plus the final classification. The
        observer FAILS CLOSED in its report (a flag is a flag) but returns
        normally — it never signals the scheduler to stop."""
        record = self.observe_group(snapshot, remote_status)
        proposed = record["ordered_chunk_ids"]
        selected = list(readyqueue_selected_ids)
        comparison = {
            "readyqueue_selected_chunks": selected,
            "observer_proposed_chunks": proposed,
            "chunks_match": proposed == selected,
            "order_match": proposed == selected,
            "byte_total": record["aggregate_prepared_bytes"],
            "selection_reason": snapshot.selection_reason,
            "expected_tape": snapshot.expected_tape,
            "classification": record["classification"],
            "mismatch_codes": record["mismatch_codes"],
        }
        # A benign set/order difference with no data problem is EXPECTED.
        if not comparison["chunks_match"] and \
                record["classification"] == MATCH:
            comparison["classification"] = EXPECTED_DIFFERENCE
            record["classification"] = EXPECTED_DIFFERENCE
        return record, comparison


def maybe_build_observer(cfg, sink=None):
    """Construct the observer ONLY when observation mode is explicitly enabled.

    Returns ``None`` when the flag is false (the default), guaranteeing no
    observer object — and therefore no observation activity — exists in
    production. Enabling this flag never enables the sealed-batch feature itself.
    """
    if not getattr(cfg, "sealed_tape_write_batches_observation_enabled", False):
        return None
    return SealedBatchObserver(sink=sink)
