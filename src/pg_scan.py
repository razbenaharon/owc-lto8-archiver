"""Incremental-scan frontier persistence (migration 014).

Plan 1, Task 2.1. Mixin over :class:`src.pg_core.PgConnectionCore`; assembled
into :class:`src.pg_db.PgDatabaseManager` alongside the other method groups.

Three rules shape every method here, and each one exists because its opposite
is a way to lose data or to claim coverage that was never established:

1. **A successful scan writes no per-file row.** The per-file detail lives in a
   local ``JSONL.zst`` artifact; PostgreSQL stores the range, the aggregate
   counts and a root-relative locator. A scan of 82 million files must not cost
   82 million catalog rows to say it succeeded.

2. **Consumption advances one cursor, transactionally, under the segment row's
   lock.** ``next_unconsumed_ordinal`` is the only thing that decides what a
   chunk may take. Gaps and overlaps are *rejected*, never repaired: a repaired
   overlap is duplicate membership in a chunk that may already be on tape.

3. **Coverage is never inferred.** ``complete_directory_listing`` marks only
   this directory's own listing. ``finalize_directory_subtree`` additionally
   requires every descendant to be terminal, the before/after observations to
   agree, and no unresolved error — and it is deliberately blind to whether the
   discovered entries were ever assigned to chunks, which is
   ``planning_state``'s separate job.

Every method refuses on a database without migration 014 rather than degrading.
"""
import json
import uuid

from .logsetup import get_logger
from .pg_core import _now_utc, _row, _rows
from .pipeline_types import (SCOPE_KIND_DIRECTORY, SCOPE_KIND_FILE,
                             ScanCoverageState, ScanDirectoryState,
                             ScanPlanningState, ScanSegmentState)


class ScanFrontierError(RuntimeError):
    """A frontier operation that must not be papered over."""


class SegmentRangeConflict(ScanFrontierError):
    """A chunk asked for ordinals that are not the segment's next unconsumed."""


class PgScanMixin:
    """Scopes, directories, segments, chunk membership and worker attempts."""

    # ------------------------------------------------------------------
    # Guard
    # ------------------------------------------------------------------
    def _require_incremental_scan_schema(self):
        if not self.incremental_scan_schema_installed():
            raise ScanFrontierError(
                "[DB] The incremental-scan schema (migration 014) is not "
                "installed on this database. Apply it explicitly with "
                "inspect_db.py --apply-incremental-scan-schema after creating "
                "and verifying a PostgreSQL backup.")

    # ------------------------------------------------------------------
    # Scopes
    # ------------------------------------------------------------------
    def create_scan_scopes(self, session_id, roots):
        """Persist the ordered source roots for a session.

        ``roots`` is an iterable of ``(source_root, scope_kind)`` or plain
        paths (treated as directories). Idempotent: re-running with the SAME
        set is a no-op, and a DIFFERENT set raises rather than silently
        expanding or shrinking what the session covers.
        """
        self._require_incremental_scan_schema()
        wanted = []
        for ordinal, entry in enumerate(roots):
            if isinstance(entry, (tuple, list)):
                root, kind = entry
            else:
                root, kind = entry, SCOPE_KIND_DIRECTORY
            if kind not in (SCOPE_KIND_DIRECTORY, SCOPE_KIND_FILE):
                raise ScanFrontierError(f"unknown scope kind: {kind!r}")
            wanted.append((ordinal, str(root), kind))
        now = _now_utc()

        def operation(conn):
            existing = conn.execute(
                """SELECT scope_ordinal, source_root, scope_kind
                   FROM remote_scan_scopes
                   WHERE session_id=%s ORDER BY scope_ordinal""",
                (session_id,),
            ).fetchall()
            if existing:
                have = {(r["source_root"], r["scope_kind"]) for r in existing}
                want = {(root, kind) for _o, root, kind in wanted}
                if have != want:
                    raise ScanFrontierError(
                        f"[DB] Session {session_id} already has persisted scan "
                        f"scopes that differ from the configured set. Added or "
                        f"removed roots are refused, never absorbed: "
                        f"persisted={sorted(have)} configured={sorted(want)}. "
                        "Reconcile explicitly before continuing.")
                # Same set, possibly reordered in config: persisted order wins.
                return [r["scope_ordinal"] for r in existing]

            for ordinal, root, kind in wanted:
                conn.execute(
                    """INSERT INTO remote_scan_scopes
                       (session_id, scope_ordinal, scope_kind, source_root,
                        created_at, updated_at)
                       VALUES (%s, %s, %s, %s, %s, %s)
                       ON CONFLICT (session_id, scope_ordinal) DO NOTHING""",
                    (session_id, ordinal, kind, root, now, now),
                )
            return [o for o, _r, _k in wanted]

        return self._transaction(operation, f"create scan scopes {session_id}")

    def get_scan_scopes(self, session_id):
        return self._run_read(
            lambda conn: _rows(conn.execute(
                """SELECT * FROM remote_scan_scopes
                   WHERE session_id=%s ORDER BY scope_ordinal""",
                (session_id,),
            ).fetchall()),
            f"get_scan_scopes({session_id})")

    def session_has_frontier_state(self, session_id):
        """Has this session published ANY frontier state?

        Used by the scan-mode gate: a session that has published cannot be
        handed back to whole-root replay. Returns False only on a definite
        "nothing here"; a database without the schema has definitively nothing.
        """
        if not self.incremental_scan_schema_installed():
            return False
        return self._run_read(
            lambda conn: conn.execute(
                "SELECT 1 FROM remote_scan_scopes WHERE session_id=%s LIMIT 1",
                (session_id,),
            ).fetchone() is not None,
            f"session_has_frontier_state({session_id})")

    # ------------------------------------------------------------------
    # Directories
    # ------------------------------------------------------------------
    def enqueue_scan_directories(self, scan_scope_id, entries,
                                 parent_directory_id=None):
        """Add child directories to the frontier.

        ``entries`` is an iterable of ``(canonical_path, traversal_ordinal)``.
        Idempotent by ``(scan_scope_id, canonical_path)``: re-listing a parent
        after an invalidation must not duplicate its children.
        """
        self._require_incremental_scan_schema()
        entries = list(entries)
        now = _now_utc()

        def operation(conn):
            inserted = 0
            for canonical_path, traversal_ordinal in entries:
                cur = conn.execute(
                    """INSERT INTO remote_scan_directories
                       (scan_scope_id, canonical_path, parent_directory_id,
                        traversal_ordinal, listing_state,
                        subtree_coverage_state, planning_state,
                        created_at, updated_at)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (scan_scope_id, canonical_path)
                           DO NOTHING""",
                    (scan_scope_id, canonical_path, parent_directory_id,
                     int(traversal_ordinal), ScanDirectoryState.PENDING.value,
                     ScanCoverageState.PROVISIONAL.value,
                     ScanPlanningState.UNPLANNED.value, now, now),
                )
                inserted += cur.rowcount
            if parent_directory_id is not None:
                conn.execute(
                    """UPDATE remote_scan_directories
                       SET child_directory_count=%s, updated_at=%s
                       WHERE scan_directory_id=%s""",
                    (len(entries), now, parent_directory_id),
                )
            return inserted

        return self._transaction(
            operation, f"enqueue {len(entries)} scan directories")

    def get_covered_directories(self, scan_scope_id):
        """Directories whose own listing is terminal, in traversal order.

        The final mutation sweep and the bottom-up finalization both walk this;
        reversing it gives deepest-first, which is the order subtree finality
        has to be established in.
        """
        self._require_incremental_scan_schema()
        return self._run_read(
            lambda conn: _rows(conn.execute(
                """SELECT * FROM remote_scan_directories
                   WHERE scan_scope_id=%s AND listing_state IN (%s, %s)
                   ORDER BY traversal_ordinal""",
                (scan_scope_id, ScanDirectoryState.COMPLETE.value,
                 ScanDirectoryState.ERROR.value),
            ).fetchall()),
            f"get_covered_directories({scan_scope_id})")

    def claim_next_directory(self, session_id, owner_token, attempt_id,
                             lease_seconds=900):
        """Claim ONE directory to list, in stable traversal order.

        A compare-and-swap over ``listing_state``: only a ``pending`` or
        ``partial`` directory with no live lease can be taken, and exactly one
        claimant wins. Returns the claimed row or ``None`` when the frontier
        has nothing available.

        ``partial`` is claimable on purpose — it is the single directory a
        crash may replay. ``complete`` never is.
        """
        self._require_incremental_scan_schema()
        now = _now_utc()

        def operation(conn):
            row = conn.execute(
                """SELECT d.scan_directory_id
                   FROM remote_scan_directories d
                   JOIN remote_scan_scopes s
                       ON s.scan_scope_id = d.scan_scope_id
                   WHERE s.session_id=%s
                     AND d.listing_state IN (%s, %s)
                     AND (d.lease_expires_at IS NULL OR d.lease_expires_at < %s)
                   ORDER BY s.scope_ordinal, d.traversal_ordinal
                   FOR UPDATE OF d SKIP LOCKED
                   LIMIT 1""",
                (session_id, ScanDirectoryState.PENDING.value,
                 ScanDirectoryState.PARTIAL.value, now),
            ).fetchone()
            if row is None:
                return None
            cur = conn.execute(
                """UPDATE remote_scan_directories
                   SET listing_state=%s, owner_token=%s, attempt_id=%s,
                       lease_expires_at=%s + make_interval(secs => %s),
                       updated_at=%s
                   WHERE scan_directory_id=%s
                     AND listing_state IN (%s, %s)
                   RETURNING *""",
                (ScanDirectoryState.SCANNING.value, owner_token, attempt_id,
                 now, float(lease_seconds), now, row["scan_directory_id"],
                 ScanDirectoryState.PENDING.value,
                 ScanDirectoryState.PARTIAL.value),
            )
            claimed = cur.fetchone()
            return _row(claimed) if claimed else None

        return self._transaction(
            operation, f"claim next scan directory (session {session_id})")

    def complete_directory_listing(self, scan_directory_id, owner_token, *,
                                   direct_file_count, direct_byte_count,
                                   observation_after=None, error_count=0):
        """Mark THIS directory's immediate listing complete.

        Says nothing about descendants — that is
        :meth:`finalize_directory_subtree`. A directory that recorded any error
        becomes ``error``, not ``complete``: an unreadable entry is missing
        coverage, and calling it complete is how a partial tree gets archived
        as if it were whole.
        """
        self._require_incremental_scan_schema()
        now = _now_utc()
        state = (ScanDirectoryState.ERROR.value if error_count
                 else ScanDirectoryState.COMPLETE.value)

        def operation(conn):
            cur = conn.execute(
                """UPDATE remote_scan_directories
                   SET listing_state=%s, direct_file_count=%s,
                       direct_byte_count=%s, observation_after=%s,
                       error_count=%s, owner_token=NULL, attempt_id=NULL,
                       lease_expires_at=NULL, updated_at=%s
                   WHERE scan_directory_id=%s AND owner_token=%s""",
                (state, int(direct_file_count), int(direct_byte_count),
                 observation_after, int(error_count), now,
                 scan_directory_id, owner_token),
            )
            return cur.rowcount == 1

        return self._transaction(
            operation, f"complete directory listing {scan_directory_id}")

    def mark_directory_partial(self, scan_directory_id, owner_token,
                               last_committed_segment_id=None):
        """Release a directory that was interrupted mid-listing.

        This is the ONE state a crash may replay. Releasing the claim (rather
        than leaving a dead lease) is what lets the next run pick it up without
        waiting out a timeout.
        """
        self._require_incremental_scan_schema()
        now = _now_utc()

        def operation(conn):
            cur = conn.execute(
                """UPDATE remote_scan_directories
                   SET listing_state=%s, last_committed_segment_id=
                           COALESCE(%s, last_committed_segment_id),
                       owner_token=NULL, attempt_id=NULL,
                       lease_expires_at=NULL, updated_at=%s
                   WHERE scan_directory_id=%s AND owner_token=%s""",
                (ScanDirectoryState.PARTIAL.value, last_committed_segment_id,
                 now, scan_directory_id, owner_token),
            )
            return cur.rowcount == 1

        return self._transaction(
            operation, f"mark directory partial {scan_directory_id}")

    def invalidate_directory(self, scan_directory_id, reason):
        """A source change invalidates a directory AND its ancestors.

        Coverage that was ``final`` becomes ``invalidated`` all the way up: if
        a child changed, the parent's claim that its whole subtree was final is
        no longer true.
        """
        self._require_incremental_scan_schema()
        now = _now_utc()

        def operation(conn):
            conn.execute(
                """WITH RECURSIVE ancestry AS (
                       SELECT scan_directory_id, parent_directory_id
                       FROM remote_scan_directories
                       WHERE scan_directory_id=%s
                     UNION ALL
                       SELECT d.scan_directory_id, d.parent_directory_id
                       FROM remote_scan_directories d
                       JOIN ancestry a
                           ON d.scan_directory_id = a.parent_directory_id
                   )
                   UPDATE remote_scan_directories
                   SET subtree_coverage_state=%s, updated_at=%s
                   WHERE scan_directory_id IN (SELECT scan_directory_id
                                               FROM ancestry)""",
                (scan_directory_id, ScanCoverageState.INVALIDATED.value, now),
            )
            conn.execute(
                """UPDATE remote_scan_directories
                   SET listing_state=%s, updated_at=%s
                   WHERE scan_directory_id=%s""",
                (ScanDirectoryState.INVALIDATED.value, now, scan_directory_id),
            )
            conn.execute(
                """INSERT INTO remote_scan_errors
                   (scan_directory_id, category, message, disposition,
                    created_at)
                   VALUES (%s, 'source_changed', %s, 'unresolved', %s)""",
                (scan_directory_id, str(reason), now),
            )

        return self._transaction(
            operation, f"invalidate directory {scan_directory_id}")

    def finalize_directory_subtree(self, scan_directory_id):
        """Mark a subtree final — only when nothing unresolved remains.

        Requires, in one transaction: this directory's listing is ``complete``,
        every descendant listing is terminal and every descendant subtree is
        ``final``, the before/after observations agree, and there is no
        unresolved error row. Returns ``(finalized, reason)``.

        Segment allocation is deliberately NOT consulted. "The tree was
        explored" and "everything found was planned" are separate facts.
        """
        self._require_incremental_scan_schema()
        now = _now_utc()

        def operation(conn):
            row = conn.execute(
                """SELECT * FROM remote_scan_directories
                   WHERE scan_directory_id=%s FOR UPDATE""",
                (scan_directory_id,),
            ).fetchone()
            if row is None:
                return False, "directory not found"
            if row["listing_state"] != ScanDirectoryState.COMPLETE.value:
                return False, f"listing_state is {row['listing_state']!r}"
            if (row["observation_before"] is not None
                    and row["observation_after"] is not None
                    and row["observation_before"] != row["observation_after"]):
                return False, "the source changed while it was being listed"

            pending = conn.execute(
                """WITH RECURSIVE descendants AS (
                       SELECT scan_directory_id FROM remote_scan_directories
                       WHERE parent_directory_id=%s
                     UNION ALL
                       SELECT d.scan_directory_id
                       FROM remote_scan_directories d
                       JOIN descendants x
                           ON d.parent_directory_id = x.scan_directory_id
                   )
                   SELECT COUNT(*) AS n FROM remote_scan_directories
                   WHERE scan_directory_id IN (SELECT scan_directory_id
                                               FROM descendants)
                     AND (listing_state <> %s OR subtree_coverage_state <> %s)""",
                (scan_directory_id, ScanDirectoryState.COMPLETE.value,
                 ScanCoverageState.FINAL.value),
            ).fetchone()["n"]
            if pending:
                return False, f"{pending} descendant(s) are not yet final"

            unresolved = conn.execute(
                """SELECT COUNT(*) AS n FROM remote_scan_errors
                   WHERE scan_directory_id=%s AND disposition='unresolved'""",
                (scan_directory_id,),
            ).fetchone()["n"]
            if unresolved:
                return False, f"{unresolved} unresolved error(s)"

            conn.execute(
                """UPDATE remote_scan_directories
                   SET subtree_coverage_state=%s, updated_at=%s
                   WHERE scan_directory_id=%s""",
                (ScanCoverageState.FINAL.value, now, scan_directory_id),
            )
            return True, "final"

        return self._transaction(
            operation, f"finalize subtree {scan_directory_id}")

    def finalize_scan_scope(self, scan_scope_id):
        """Mark a whole scope final. Same rule, one level up."""
        self._require_incremental_scan_schema()
        now = _now_utc()

        def operation(conn):
            outstanding = conn.execute(
                """SELECT COUNT(*) AS n FROM remote_scan_directories
                   WHERE scan_scope_id=%s
                     AND (listing_state <> %s OR subtree_coverage_state <> %s)""",
                (scan_scope_id, ScanDirectoryState.COMPLETE.value,
                 ScanCoverageState.FINAL.value),
            ).fetchone()["n"]
            if outstanding:
                return False, f"{outstanding} directory/directories not final"
            conn.execute(
                """UPDATE remote_scan_scopes
                   SET coverage_state=%s, updated_at=%s WHERE scan_scope_id=%s""",
                (ScanCoverageState.FINAL.value, now, scan_scope_id),
            )
            return True, "final"

        return self._transaction(
            operation, f"finalize scan scope {scan_scope_id}")

    def mark_scope_planning_complete(self, scan_scope_id):
        """Every ready segment range in this scope is fully allocated.

        Separate from coverage on purpose, so an operator can tell "the source
        tree was explored" from "all discovered entries were assigned to plans".
        """
        self._require_incremental_scan_schema()
        now = _now_utc()

        def operation(conn):
            unallocated = conn.execute(
                """SELECT COUNT(*) AS n
                   FROM remote_scan_segments sg
                   JOIN remote_scan_directories d
                       ON d.scan_directory_id = sg.scan_directory_id
                   WHERE d.scan_scope_id=%s AND sg.state IN (%s, %s)""",
                (scan_scope_id, ScanSegmentState.READY.value,
                 ScanSegmentState.PARTIALLY_CONSUMED.value),
            ).fetchone()["n"]
            if unallocated:
                return False, f"{unallocated} ready segment(s) unallocated"
            conn.execute(
                """UPDATE remote_scan_scopes
                   SET planning_complete=TRUE, updated_at=%s
                   WHERE scan_scope_id=%s""",
                (now, scan_scope_id),
            )
            return True, "planning complete"

        return self._transaction(
            operation, f"mark scope planning complete {scan_scope_id}")

    # ------------------------------------------------------------------
    # Segments
    # ------------------------------------------------------------------
    def publish_scan_segment(self, scan_directory_id, *, first_scan_ordinal,
                             last_scan_ordinal, locator, file_count,
                             byte_count, artifact_size_bytes=None,
                             first_canonical_path=None,
                             last_canonical_path=None,
                             artifact_version="scan-segment-v1"):
        """Record a READY artifact range.

        The artifact must already be published atomically on disk: a ``.part``
        locator is refused here as well as by the schema, because a locator
        that names a file still being written is a promise the reader cannot
        keep.
        """
        self._require_incremental_scan_schema()
        if str(locator).endswith(".part"):
            raise ScanFrontierError(
                f"refusing to persist a .part locator as ready: {locator!r}")
        if last_scan_ordinal < first_scan_ordinal:
            raise ScanFrontierError("segment ordinal range is inverted")
        now = _now_utc()

        def operation(conn):
            row = conn.execute(
                """INSERT INTO remote_scan_segments
                   (scan_directory_id, first_scan_ordinal, last_scan_ordinal,
                    next_unconsumed_ordinal, locator, artifact_version,
                    artifact_size_bytes, file_count, byte_count,
                    first_canonical_path, last_canonical_path, state,
                    created_at, updated_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                           %s, %s)
                   ON CONFLICT (scan_directory_id, first_scan_ordinal)
                       DO UPDATE SET
                           last_scan_ordinal = EXCLUDED.last_scan_ordinal,
                           locator = EXCLUDED.locator,
                           file_count = EXCLUDED.file_count,
                           byte_count = EXCLUDED.byte_count,
                           updated_at = EXCLUDED.updated_at
                       WHERE remote_scan_segments.state = %s
                   RETURNING *""",
                (scan_directory_id, int(first_scan_ordinal),
                 int(last_scan_ordinal), int(first_scan_ordinal), str(locator),
                 artifact_version, artifact_size_bytes, int(file_count),
                 int(byte_count), first_canonical_path, last_canonical_path,
                 ScanSegmentState.READY.value, now, now,
                 ScanSegmentState.WRITING.value),
            ).fetchone()
            if row is None:
                # A ready/consumed segment already occupies this range. That is
                # idempotent re-publication, not a conflict to resolve.
                row = conn.execute(
                    """SELECT * FROM remote_scan_segments
                       WHERE scan_directory_id=%s AND first_scan_ordinal=%s""",
                    (scan_directory_id, int(first_scan_ordinal)),
                ).fetchone()
            conn.execute(
                """UPDATE remote_scan_directories
                   SET last_committed_segment_id=%s, updated_at=%s
                   WHERE scan_directory_id=%s""",
                (row["scan_segment_id"], now, scan_directory_id),
            )
            return _row(row)

        return self._transaction(
            operation, f"publish scan segment for dir {scan_directory_id}")

    def get_ready_segments(self, session_id, limit=50):
        """Ready (or partially consumed) segments, in stable traversal order."""
        self._require_incremental_scan_schema()
        return self._run_read(
            lambda conn: _rows(conn.execute(
                """SELECT sg.*, d.scan_scope_id, d.canonical_path
                   FROM remote_scan_segments sg
                   JOIN remote_scan_directories d
                       ON d.scan_directory_id = sg.scan_directory_id
                   JOIN remote_scan_scopes s
                       ON s.scan_scope_id = d.scan_scope_id
                   WHERE s.session_id=%s AND sg.state IN (%s, %s)
                   ORDER BY s.scope_ordinal, d.traversal_ordinal,
                            sg.first_scan_ordinal
                   LIMIT %s""",
                (session_id, ScanSegmentState.READY.value,
                 ScanSegmentState.PARTIALLY_CONSUMED.value, int(limit)),
            ).fetchall()),
            f"get_ready_segments({session_id})")

    def consume_segment_range(self, scan_segment_id, session_id, chunk_index,
                              ordinal_count):
        """Allocate the NEXT ``ordinal_count`` ordinals of a segment to a chunk.

        This is the one place segment ordinals become chunk membership, and it
        is deliberately narrow:

        * the segment row is locked, so two chunks cannot both read the same
          cursor;
        * only ordinals starting exactly at ``next_unconsumed_ordinal`` may be
          taken — a caller asking for anything else raises
          :class:`SegmentRangeConflict` rather than being accommodated;
        * the cursor advances in the SAME transaction as the membership row, so
          a crash cannot leave a chunk holding a range the segment does not
          know it gave away;
        * ``consumed`` is set only when the whole ready range is allocated.

        Returns ``(first_ordinal, last_ordinal)``.
        """
        self._require_incremental_scan_schema()
        if int(ordinal_count) <= 0:
            raise ScanFrontierError("ordinal_count must be positive")
        now = _now_utc()

        def operation(conn):
            segment = conn.execute(
                """SELECT * FROM remote_scan_segments
                   WHERE scan_segment_id=%s FOR UPDATE""",
                (scan_segment_id,),
            ).fetchone()
            if segment is None:
                raise SegmentRangeConflict(
                    f"segment {scan_segment_id} does not exist")
            if segment["state"] not in (ScanSegmentState.READY.value,
                                        ScanSegmentState.PARTIALLY_CONSUMED.value):
                raise SegmentRangeConflict(
                    f"segment {scan_segment_id} is {segment['state']!r}; only a "
                    "ready or partially-consumed segment may be allocated")
            cursor = segment["next_unconsumed_ordinal"]
            if cursor is None:
                raise SegmentRangeConflict(
                    f"segment {scan_segment_id} has no consumption cursor")
            last_available = segment["last_scan_ordinal"]
            if cursor > last_available:
                raise SegmentRangeConflict(
                    f"segment {scan_segment_id} is exhausted")
            first = int(cursor)
            last = min(int(last_available), first + int(ordinal_count) - 1)

            conn.execute(
                """INSERT INTO remote_chunk_scan_segments
                   (session_id, chunk_index, scan_segment_id,
                    first_scan_ordinal, last_scan_ordinal, created_at)
                   VALUES (%s, %s, %s, %s, %s, %s)""",
                (session_id, int(chunk_index), scan_segment_id, first, last,
                 now),
            )
            exhausted = last >= int(last_available)
            conn.execute(
                """UPDATE remote_scan_segments
                   SET next_unconsumed_ordinal=%s, state=%s, updated_at=%s
                   WHERE scan_segment_id=%s""",
                (last + 1,
                 (ScanSegmentState.CONSUMED.value if exhausted
                  else ScanSegmentState.PARTIALLY_CONSUMED.value),
                 now, scan_segment_id),
            )
            return first, last

        return self._transaction(
            operation, f"consume segment {scan_segment_id} range")

    def import_legacy_scan_segment(self, session_id, scan_segment_id, entries):
        """Reconcile ONE segment against a legacy session's snapshot, once.

        Plan 1, Task 2.4. A session migrated onto the frontier already has
        ``remote_snapshot_files`` rows from its whole-root scanning. Those must
        not be re-queried for every rediscovered file — that is exactly the
        per-file database cost the frontier removes. So each segment is
        compared **once**, in one set-based query, and then marked.

        The comparison matches **canonical path AND size**, and the three
        outcomes are deliberately different:

        ``covered``        same path, same size — already in the session's plan.
                           Skipped, never appended again.
        ``new``            not in the snapshot at all. Available for planning.
        ``source_changed`` same path, DIFFERENT size. This is the one that must
                           not be silently anti-joined away: the old membership
                           may already be on tape under the old size, and the
                           source now holds something else. It is recorded as an
                           unresolved error (which keeps the directory's
                           coverage provisional), the segment is marked
                           ``blocked``, and NOTHING is replanned automatically.
                           An operator chooses retain-old, abandon, or a
                           separately approved later plan.

        Returns ``{"covered": [...], "new": [...], "source_changed": [...],
        "already_imported": bool}``.
        """
        self._require_incremental_scan_schema()
        entries = [(str(path), int(size)) for path, size in entries]

        def operation(conn):
            segment = conn.execute(
                """SELECT scan_segment_id, scan_directory_id,
                          legacy_import_state
                   FROM remote_scan_segments
                   WHERE scan_segment_id=%s FOR UPDATE""",
                (scan_segment_id,),
            ).fetchone()
            if segment is None:
                raise ScanFrontierError(
                    f"segment {scan_segment_id} does not exist")
            if segment["legacy_import_state"] != 'not_imported':
                # Idempotent by construction: a segment is compared once.
                return {"covered": [], "new": [], "source_changed": [],
                        "already_imported": True}

            snapshot_id_row = conn.execute(
                """SELECT p.snapshot_id
                   FROM remote_sessions s
                   JOIN remote_plans p ON p.plan_id = s.plan_id
                   WHERE s.session_id=%s""",
                (session_id,),
            ).fetchone()

            existing = {}
            if snapshot_id_row is not None and entries:
                # ONE set-based query for the whole segment — not one per file.
                for row in conn.execute(
                    """SELECT remote_path, file_size_bytes
                       FROM remote_snapshot_files
                       WHERE snapshot_id=%s AND remote_path = ANY(%s)""",
                    (snapshot_id_row["snapshot_id"],
                     [path for path, _size in entries]),
                ).fetchall():
                    existing[row["remote_path"]] = int(row["file_size_bytes"])

            covered, fresh, changed = [], [], []
            for path, size in entries:
                known = existing.get(path.replace("\\", "/"))
                if known is None:
                    fresh.append((path, size))
                elif known == size:
                    covered.append((path, size))
                else:
                    changed.append((path, known, size))

            now = _now_utc()
            for path, planned_size, observed_size in changed:
                conn.execute(
                    """INSERT INTO remote_scan_errors
                       (scan_directory_id, category, path, message,
                        disposition, created_at)
                       VALUES (%s, 'source_changed', %s, %s, 'unresolved', %s)""",
                    (segment["scan_directory_id"], path,
                     f"planned {planned_size} bytes, source now reports "
                     f"{observed_size}; existing membership retained and no "
                     "replacement was planned",
                     now),
                )
            conn.execute(
                """UPDATE remote_scan_segments
                   SET legacy_import_state=%s, legacy_imported_at=%s,
                       updated_at=%s
                   WHERE scan_segment_id=%s""",
                ('blocked' if changed else 'imported', now, now,
                 scan_segment_id),
            )
            if changed:
                get_logger().error(
                    "legacy_import_blocked: segment=%s changed_entries=%d; "
                    "existing membership retained, nothing replanned",
                    scan_segment_id, len(changed))
            return {"covered": covered, "new": fresh,
                    "source_changed": changed, "already_imported": False}

        return self._transaction(
            operation, f"import legacy scan segment {scan_segment_id}")

    def mark_segment_fully_allocated(self, scan_segment_id):
        """Mark a segment ``consumed`` once the publisher has accounted for ALL
        of its entries — including the ones it did not plan.

        ``consume_segment_range`` advances the cursor by the number of entries
        that became chunk membership, and sets ``consumed`` only when the cursor
        reaches the end of the ready range. That is correct when every entry is
        planned, and wrong the moment filtering removes some: a segment whose
        entries were mostly already planned never reaches its end ordinal, stays
        ``ready`` for ever, and is handed back to the publisher on the next pass
        — which re-plans its new entries and duplicates them.

        Allocation is therefore two facts, not one: how many ordinals became
        membership (the cursor), and whether the segment still has anything left
        to offer (this). A segment the publisher has fully classified has
        nothing left to offer regardless of how much of it was planned.
        """
        self._require_incremental_scan_schema()
        now = _now_utc()

        def operation(conn):
            row = conn.execute(
                """UPDATE remote_scan_segments
                   SET state=%s, updated_at=%s
                   WHERE scan_segment_id=%s AND state IN (%s, %s)
                   RETURNING scan_segment_id, state""",
                (ScanSegmentState.CONSUMED.value, now, scan_segment_id,
                 ScanSegmentState.READY.value,
                 ScanSegmentState.PARTIALLY_CONSUMED.value),
            ).fetchone()
            return bool(row)

        return self._transaction(
            operation, f"mark segment {scan_segment_id} fully allocated")

    def classify_segment_entries(self, session_id, entries):
        """Classify entries against the legacy snapshot WITHOUT writing.

        The same set-based comparison :meth:`import_legacy_scan_segment`
        performs, with none of its side effects — no error rows, no state
        change, no marking. It exists because the import is deliberately
        once-only, which left a hole:

        A segment whose import already committed returns
        ``already_imported=True`` with **empty** lists, and the caller then had
        nothing to filter with. On any restart between "segment imported" and
        "chunk sealed" it therefore fed the FULL rediscovered set back into the
        chunk builder — re-planning files that were already planned and moving
        chunk boundaries, which is precisely what the frontier exists to
        prevent. Recomputing the classification is safe (it is a pure read),
        bounded (one query per segment against the ``(snapshot_id,
        remote_path)`` unique index) and idempotent.

        Returns ``{"covered": [...], "new": [...], "source_changed": [...]}``
        with the same shapes as the import.
        """
        self._require_incremental_scan_schema()
        entries = [(str(path), int(size)) for path, size in entries]
        if not entries:
            return {"covered": [], "new": [], "source_changed": []}

        def operation(conn):
            snapshot_id_row = conn.execute(
                """SELECT p.snapshot_id
                   FROM remote_sessions s
                   JOIN remote_plans p ON p.plan_id = s.plan_id
                   WHERE s.session_id=%s""",
                (session_id,),
            ).fetchone()
            existing = {}
            if snapshot_id_row is not None:
                for row in conn.execute(
                    """SELECT remote_path, file_size_bytes
                       FROM remote_snapshot_files
                       WHERE snapshot_id=%s AND remote_path = ANY(%s)""",
                    (snapshot_id_row["snapshot_id"],
                     [path for path, _size in entries]),
                ).fetchall():
                    existing[row["remote_path"]] = int(row["file_size_bytes"])

            covered, fresh, changed = [], [], []
            for path, size in entries:
                known = existing.get(path.replace("\\", "/"))
                if known is None:
                    fresh.append((path, size))
                elif known == size:
                    covered.append((path, size))
                else:
                    changed.append((path, known, size))
            return {"covered": covered, "new": fresh, "source_changed": changed}

        return self._run_read(
            operation, f"classify segment entries for session {session_id}")

    def invalidate_segment(self, scan_segment_id, reason):
        """Mark a segment unusable. Never deletes: the row is evidence."""
        self._require_incremental_scan_schema()
        now = _now_utc()

        def operation(conn):
            cur = conn.execute(
                """UPDATE remote_scan_segments
                   SET state=%s, updated_at=%s
                   WHERE scan_segment_id=%s AND state <> %s""",
                (ScanSegmentState.INVALIDATED.value, now, scan_segment_id,
                 ScanSegmentState.CONSUMED.value),
            )
            get_logger().warning("scan_segment_invalidated: id=%s reason=%s",
                                 scan_segment_id, reason)
            return cur.rowcount == 1

        return self._transaction(
            operation, f"invalidate segment {scan_segment_id}")

    # ------------------------------------------------------------------
    # Errors
    # ------------------------------------------------------------------
    def record_scan_error(self, *, scan_scope_id=None, scan_directory_id=None,
                          category, path=None, message=None,
                          disposition="unresolved"):
        """One compact row per exceptional entry. Never one per success."""
        self._require_incremental_scan_schema()
        now = _now_utc()
        return self._transaction(
            lambda conn: conn.execute(
                """INSERT INTO remote_scan_errors
                   (scan_scope_id, scan_directory_id, category, path, message,
                    disposition, created_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)
                   RETURNING scan_error_id""",
                (scan_scope_id, scan_directory_id, category, path, message,
                 disposition, now),
            ).fetchone()["scan_error_id"],
            f"record scan error {category}")

    # ------------------------------------------------------------------
    # Worker attempts
    # ------------------------------------------------------------------
    def start_worker_attempt(self, *, owner_token, attempt_kind,
                             session_id=None, chunk_index=None,
                             scan_directory_id=None, local_pid=None,
                             local_process_started_at=None,
                             remote_command_token=None,
                             remote_process_group=None, lease_seconds=900):
        """Record durable evidence that a worker started.

        The in-memory registry in ``src.runtime`` is current-process cleanup
        only and proves nothing after a crash. Reconciliation needs the local
        PID *plus* its creation time (PIDs are reused) and a token the remote
        process group carries, so an orphan can be identified rather than
        guessed at.
        """
        self._require_incremental_scan_schema()
        attempt_id = uuid.uuid4().hex
        now = _now_utc()

        def operation(conn):
            conn.execute(
                """INSERT INTO remote_worker_attempts
                   (attempt_id, owner_token, session_id, chunk_index,
                    scan_directory_id, attempt_kind, local_pid,
                    local_process_started_at, remote_command_token,
                    remote_process_group,
                    lease_expires_at, heartbeat_at, created_at, updated_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                           %s + make_interval(secs => %s), %s, %s, %s)""",
                (attempt_id, owner_token, session_id, chunk_index,
                 scan_directory_id, attempt_kind, local_pid,
                 local_process_started_at, remote_command_token,
                 remote_process_group, now, float(lease_seconds), now, now,
                 now),
            )
            return attempt_id

        return self._transaction(operation, f"start {attempt_kind} attempt")

    def finish_worker_attempt(self, attempt_id, terminal_state):
        self._require_incremental_scan_schema()
        now = _now_utc()
        return self._transaction(
            lambda conn: conn.execute(
                """UPDATE remote_worker_attempts
                   SET terminal_state=%s, updated_at=%s
                   WHERE attempt_id=%s""",
                (terminal_state, now, attempt_id),
            ).rowcount == 1,
            f"finish attempt {attempt_id}")

    # ------------------------------------------------------------------
    # Bootstrap runs (Task 4.2)
    # ------------------------------------------------------------------
    def start_frontier_bootstrap(self, session_id, source_host=None):
        """Open (or resume) the ONE bootstrap run for a session.

        A session may have at most one unfinished bootstrap — enforced by a
        partial unique index, not by hoping. Re-calling returns the existing
        run, so an interrupted bootstrap resumes rather than starting over.
        """
        self._require_incremental_scan_schema()
        now = _now_utc()

        def operation(conn):
            existing = conn.execute(
                """SELECT * FROM remote_frontier_bootstraps
                   WHERE session_id=%s AND state IN ('planned','running')
                   FOR UPDATE""",
                (session_id,),
            ).fetchone()
            if existing is not None:
                conn.execute(
                    """UPDATE remote_frontier_bootstraps
                       SET state='running', updated_at=%s
                       WHERE bootstrap_id=%s""",
                    (now, existing["bootstrap_id"]),
                )
                return _row(existing)
            bootstrap_id = uuid.uuid4().hex
            row = conn.execute(
                """INSERT INTO remote_frontier_bootstraps
                   (bootstrap_id, session_id, source_host, state,
                    created_at, updated_at)
                   VALUES (%s, %s, %s, 'running', %s, %s)
                   RETURNING *""",
                (bootstrap_id, session_id, source_host, now, now),
            ).fetchone()
            return _row(row)

        return self._transaction(
            operation, f"start frontier bootstrap for session {session_id}")

    def update_frontier_bootstrap(self, bootstrap_id, **fields):
        """Record progress. Counters are SET, not incremented, so a resumed
        run reports the true total rather than double-counting."""
        self._require_incremental_scan_schema()
        allowed = {"state", "scopes_created", "directories_listed",
                   "segments_published", "segments_imported",
                   "entries_covered", "entries_new", "entries_changed",
                   "coverage_final", "detail"}
        unknown = set(fields) - allowed
        if unknown:
            raise ScanFrontierError(
                f"unknown bootstrap field(s): {sorted(unknown)}")
        if not fields:
            return False
        sets = ", ".join(f"{key}=%s" for key in fields)
        params = list(fields.values()) + [_now_utc(), bootstrap_id]
        return self._transaction(
            lambda conn: conn.execute(
                f"""UPDATE remote_frontier_bootstraps
                    SET {sets}, updated_at=%s WHERE bootstrap_id=%s""",
                params,
            ).rowcount == 1,
            f"update frontier bootstrap {bootstrap_id}")

    def get_frontier_bootstrap(self, session_id):
        self._require_incremental_scan_schema()
        return self._run_read(
            lambda conn: _row(conn.execute(
                """SELECT * FROM remote_frontier_bootstraps
                   WHERE session_id=%s
                   ORDER BY created_at DESC LIMIT 1""",
                (session_id,),
            ).fetchone()),
            f"get_frontier_bootstrap({session_id})")

    def list_live_worker_attempts(self, session_id=None):
        """Attempts with no terminal state — candidates for reconciliation."""
        self._require_incremental_scan_schema()
        return self._run_read(
            lambda conn: _rows(conn.execute(
                """SELECT * FROM remote_worker_attempts
                   WHERE terminal_state IS NULL
                     AND (%s::bigint IS NULL OR session_id = %s)
                   ORDER BY created_at""",
                (session_id, session_id),
            ).fetchall()),
            f"list_live_worker_attempts({session_id})")


__all__ = ["PgScanMixin", "ScanFrontierError", "SegmentRangeConflict"]
