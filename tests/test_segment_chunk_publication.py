"""Plan 1 / Task 2.4 — sealing chunks from ready scan segments.

Three properties matter here, and each protects a different failure:

1. **The per-file legacy lookup is gone.** Normal frontier progress must never
   ask the database "have I seen this path before?" once per rediscovered file.
   A migrated session is reconciled ONCE per segment, in one set-based query.

2. **A ``source_changed`` entry is never silently dropped.** Same path,
   different size means the source moved after planning. The old membership may
   already be on tape; anti-joining it away would leave the catalog describing
   bytes that are not there. It becomes an unresolved error and blocks the
   segment, and nothing is replanned without a human.

3. **A sealed chunk is immutable.** Appending to one would change what a
   written chunk is recorded as containing.
"""
import os
import tempfile
import unittest
from unittest import mock

from src.archive_artifacts import JsonlZstArtifactWriter, segment_locator
from src.planning import StreamingChunkBuilder
from src.scan_frontier import SegmentChunkPublisher


class PublisherDB:
    """In-memory double with the same refusals as the real repository."""

    def __init__(self, snapshot=None):
        #: The legacy session's existing membership: {path: size}.
        self.snapshot = dict(snapshot or {})
        self.segments = []
        self.appended = []
        self.sealed = []
        self.consumed = []
        self.errors = []
        self.import_calls = []
        self.membership_queries = 0

    # -- segments ---------------------------------------------------------
    def add_segment(self, segment_id, locator, first=0, last=0):
        row = {"scan_segment_id": segment_id, "locator": locator,
               "first_scan_ordinal": first, "last_scan_ordinal": last,
               "next_unconsumed_ordinal": first,
               "legacy_import_state": "not_imported"}
        self.segments.append(row)
        return row

    def get_ready_segments(self, session_id, limit=50):
        return list(self.segments[:limit])

    def import_legacy_scan_segment(self, session_id, segment_id, entries):
        self.import_calls.append(segment_id)
        segment = next(s for s in self.segments
                       if s["scan_segment_id"] == segment_id)
        if segment["legacy_import_state"] != "not_imported":
            # Already reconciled: return WITHOUT touching the snapshot, exactly
            # as the repository does (its early return precedes the query).
            return {"covered": [], "new": [], "source_changed": [],
                    "already_imported": True}
        self.membership_queries += 1          # ONE query for the whole segment
        covered, fresh, changed = [], [], []
        for path, size in entries:
            known = self.snapshot.get(path)
            if known is None:
                fresh.append((path, size))
            elif known == size:
                covered.append((path, size))
            else:
                changed.append((path, known, size))
        for path, planned, observed in changed:
            self.errors.append({"category": "source_changed", "path": path,
                                "disposition": "unresolved",
                                "planned": planned, "observed": observed})
        segment["legacy_import_state"] = "blocked" if changed else "imported"
        return {"covered": covered, "new": fresh, "source_changed": changed,
                "already_imported": False}

    def consume_segment_range(self, segment_id, session_id, chunk_index,
                              count):
        segment = next(s for s in self.segments
                       if s["scan_segment_id"] == segment_id)
        first = segment["next_unconsumed_ordinal"]
        last = min(segment["last_scan_ordinal"], first + count - 1)
        segment["next_unconsumed_ordinal"] = last + 1
        self.consumed.append((segment_id, chunk_index, first, last))
        return first, last

    # -- chunks -----------------------------------------------------------
    def append_remote_streaming_chunk(self, session_id, chunk_index, rows):
        rows = list(rows)
        if any(s["chunk_index"] == chunk_index for s in self.sealed):
            raise RuntimeError(
                f"[DB] Chunk {chunk_index + 1} is SEALED; refusing to append.")
        self.appended.append((chunk_index, rows))
        return {"inserted_files": len(rows),
                "inserted_bytes": sum(int(r[3]) for r in rows)}

    def seal_remote_chunk(self, session_id, chunk_index, *,
                          expected_file_count, expected_bytes,
                          scan_segment_id=None, first_scan_ordinal=None,
                          last_scan_ordinal=None):
        for existing in self.sealed:
            if existing["chunk_index"] == chunk_index:
                if (existing["expected_file_count"] == expected_file_count
                        and existing["expected_bytes"] == expected_bytes):
                    return False
                raise RuntimeError("already sealed with a different expectation")
        self.sealed.append({
            "chunk_index": chunk_index,
            "expected_file_count": expected_file_count,
            "expected_bytes": expected_bytes,
            "scan_segment_id": scan_segment_id,
            "first_scan_ordinal": first_scan_ordinal,
            "last_scan_ordinal": last_scan_ordinal})
        return True

    # -- the method that must NOT be called per file ----------------------
    def get_remote_existing_snapshot_paths(self, session_id, paths):
        raise AssertionError(
            "the per-file legacy membership lookup was used during normal "
            "frontier progress")


class _Publisher(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory()
        self.root = self._dir.name
        self.addCleanup(self._dir.cleanup)

    def _segment(self, db, segment_id, entries, directory_id=1):
        locator = segment_locator(37, directory_id, 0)
        with JsonlZstArtifactWriter(self.root, locator, session_id=37,
                                    scan_directory_id=directory_id) as writer:
            for ordinal, (path, size) in enumerate(entries):
                writer.add(path=path, size=size, ordinal=ordinal)
        return db.add_segment(segment_id, locator, 0, max(0, len(entries) - 1))

    def _publisher(self, db, legacy=False, max_files=1000,
                   budget=10 ** 12):
        return SegmentChunkPublisher(
            db=db, session_id=37, archive_root=self.root,
            builder_factory=lambda: StreamingChunkBuilder(
                budget, alloc_unit=1, padding_factor=1.0, max_files=max_files),
            legacy_session=legacy)


# =============================================================================
# A. A fresh frontier session
# =============================================================================
class FreshSessionTests(_Publisher):
    def test_entries_come_from_the_artifact_not_the_database(self):
        db = PublisherDB()
        self._segment(db, 1, [("/strg/a/f1", 10), ("/strg/a/f2", 20)])
        sealed = self._publisher(db).publish_ready_segments(0)
        self.assertEqual(sealed, [0])
        (chunk_index, rows), = db.appended
        self.assertEqual([r[1] for r in rows], ["/strg/a/f1", "/strg/a/f2"])

    def test_no_legacy_reconciliation_runs_for_a_fresh_session(self):
        db = PublisherDB()
        self._segment(db, 1, [("/strg/a/f", 10)])
        self._publisher(db).publish_ready_segments(0)
        self.assertEqual(db.import_calls, [])
        self.assertEqual(db.membership_queries, 0)

    def test_the_chunk_is_sealed_with_its_expectation(self):
        db = PublisherDB()
        self._segment(db, 1, [("/strg/a/f1", 10), ("/strg/a/f2", 20)])
        self._publisher(db).publish_ready_segments(0)
        self.assertEqual(len(db.sealed), 1)
        self.assertEqual(db.sealed[0]["expected_file_count"], 2)
        self.assertEqual(db.sealed[0]["expected_bytes"], 30)

    def test_the_sealed_chunk_records_the_segment_range_it_consumed(self):
        db = PublisherDB()
        self._segment(db, 1, [(f"/strg/a/f{i}", 10) for i in range(5)])
        self._publisher(db).publish_ready_segments(0)
        self.assertEqual(db.sealed[0]["scan_segment_id"], 1)
        self.assertEqual((db.sealed[0]["first_scan_ordinal"],
                          db.sealed[0]["last_scan_ordinal"]), (0, 4))
        self.assertEqual(db.consumed, [(1, 0, 0, 4)])

    def test_a_segment_splits_across_chunks_without_duplicate_membership(self):
        db = PublisherDB()
        self._segment(db, 1, [(f"/strg/a/f{i}", 10) for i in range(6)])
        sealed = self._publisher(db, max_files=2).publish_ready_segments(0)
        self.assertEqual(sealed, [0, 1, 2])
        # Contiguous, non-overlapping ordinal ranges.
        self.assertEqual([(c[2], c[3]) for c in db.consumed],
                         [(0, 1), (2, 3), (4, 5)])
        every_path = [r[1] for _ci, rows in db.appended for r in rows]
        self.assertEqual(len(every_path), len(set(every_path)))

    def test_chunk_indexes_advance_from_the_caller_supplied_start(self):
        db = PublisherDB()
        self._segment(db, 1, [(f"/strg/a/f{i}", 10) for i in range(4)])
        sealed = self._publisher(db, max_files=2).publish_ready_segments(108)
        self.assertEqual(sealed, [108, 109])

    def test_an_empty_segment_seals_nothing(self):
        db = PublisherDB()
        self._segment(db, 1, [])
        self.assertEqual(self._publisher(db).publish_ready_segments(0), [])
        self.assertEqual(db.sealed, [])


# =============================================================================
# B. A migrated legacy session
# =============================================================================
class LegacySessionTests(_Publisher):
    def test_reconciliation_is_one_query_per_segment_not_per_file(self):
        db = PublisherDB(snapshot={})
        self._segment(db, 1, [(f"/strg/a/f{i}", 10) for i in range(500)])
        self._publisher(db, legacy=True).publish_ready_segments(0)
        self.assertEqual(db.membership_queries, 1)
        self.assertEqual(db.import_calls, [1])

    def test_an_already_covered_path_is_never_appended_again(self):
        db = PublisherDB(snapshot={"/strg/a/old": 10})
        self._segment(db, 1, [("/strg/a/old", 10), ("/strg/a/new", 20)])
        self._publisher(db, legacy=True).publish_ready_segments(0)
        appended = [r[1] for _ci, rows in db.appended for r in rows]
        self.assertEqual(appended, ["/strg/a/new"])

    def test_a_segment_is_reconciled_exactly_once(self):
        db = PublisherDB(snapshot={"/strg/a/old": 10})
        self._segment(db, 1, [("/strg/a/old", 10), ("/strg/a/new", 20)])
        publisher = self._publisher(db, legacy=True)
        publisher.publish_ready_segments(0)
        publisher.publish_ready_segments(1)
        self.assertEqual(db.import_calls, [1, 1])
        # ...but the second call short-circuits as already imported.
        self.assertEqual(db.membership_queries, 1)

    def test_a_changed_size_blocks_the_segment_and_plans_nothing(self):
        """THE case: same path, different size. Never anti-joined away."""
        db = PublisherDB(snapshot={"/strg/a/f": 10})
        self._segment(db, 1, [("/strg/a/f", 999), ("/strg/a/other", 20)])
        publisher = self._publisher(db, legacy=True)
        sealed = publisher.publish_ready_segments(0)

        self.assertEqual(sealed, [], "a blocked segment still planned work")
        self.assertEqual(db.appended, [])
        self.assertEqual(db.sealed, [])
        self.assertEqual(publisher.blocked_segments, [1])

    def test_a_changed_size_is_recorded_as_an_unresolved_error(self):
        db = PublisherDB(snapshot={"/strg/a/f": 10})
        self._segment(db, 1, [("/strg/a/f", 999)])
        self._publisher(db, legacy=True).publish_ready_segments(0)
        self.assertEqual(len(db.errors), 1)
        error = db.errors[0]
        self.assertEqual(error["category"], "source_changed")
        self.assertEqual(error["disposition"], "unresolved")
        self.assertEqual(error["path"], "/strg/a/f")
        # Both sizes are preserved, so an operator can decide.
        self.assertEqual((error["planned"], error["observed"]), (10, 999))

    def test_the_existing_membership_is_retained(self):
        """The old plan rows are untouched — they may be on tape."""
        db = PublisherDB(snapshot={"/strg/a/f": 10})
        self._segment(db, 1, [("/strg/a/f", 999)])
        self._publisher(db, legacy=True).publish_ready_segments(0)
        self.assertEqual(db.snapshot, {"/strg/a/f": 10})


# =============================================================================
# C. A sealed chunk is immutable
# =============================================================================
class SealImmutabilityTests(_Publisher):
    def test_appending_to_a_sealed_chunk_is_refused(self):
        db = PublisherDB()
        db.seal_remote_chunk(37, 0, expected_file_count=1, expected_bytes=10)
        with self.assertRaises(RuntimeError) as caught:
            db.append_remote_streaming_chunk(37, 0, [(0, "/x", "x", 1)])
        self.assertIn("SEALED", str(caught.exception))

    def test_re_sealing_with_the_same_expectation_is_idempotent(self):
        db = PublisherDB()
        self.assertTrue(db.seal_remote_chunk(
            37, 0, expected_file_count=2, expected_bytes=30))
        self.assertFalse(db.seal_remote_chunk(
            37, 0, expected_file_count=2, expected_bytes=30))

    def test_re_sealing_with_a_different_expectation_is_refused(self):
        db = PublisherDB()
        db.seal_remote_chunk(37, 0, expected_file_count=2, expected_bytes=30)
        with self.assertRaises(RuntimeError):
            db.seal_remote_chunk(37, 0, expected_file_count=3,
                                 expected_bytes=40)

    def test_the_repository_refuses_an_append_to_a_sealed_chunk(self):
        """Structural: the real SQL path carries the same refusal."""
        import inspect
        from src.pg_sessions import PgSessionMixin
        source = inspect.getsource(
            PgSessionMixin.append_remote_streaming_chunk)
        self.assertIn("membership_state", source)
        self.assertIn("SEALED", source)

    def test_sealing_requires_migration_014(self):
        import inspect
        from src.pg_sessions import PgSessionMixin
        source = inspect.getsource(PgSessionMixin.seal_remote_chunk)
        self.assertIn("migration 014", source)
        # Expectation and segment reference are set in ONE transaction.
        self.assertEqual(source.count("self._transaction("), 1)


# =============================================================================
# D. The legacy format is unchanged
# =============================================================================
class UnchangedChunkFormatTests(_Publisher):
    def test_rows_keep_the_existing_four_column_shape(self):
        db = PublisherDB()
        self._segment(db, 1, [("/strg/a/deep/file.bin", 42)])
        self._publisher(db).publish_ready_segments(0)
        (_chunk_index, rows), = db.appended
        chunk_index, path, name, size = rows[0]
        self.assertEqual(chunk_index, 0)
        self.assertEqual(path, "/strg/a/deep/file.bin")
        self.assertEqual(name, "file.bin")
        self.assertEqual(size, 42)

    def test_get_chunk_files_remains_the_planning_source(self):
        """Plan 1 changes where entries COME FROM, not the chunk format."""
        from src.pg_sessions import PgSessionMixin
        self.assertTrue(hasattr(PgSessionMixin, "get_chunk_files"))


if __name__ == "__main__":
    unittest.main()


# =============================================================================
# E. Task 3.3 — a path the catalog key cannot represent is never mangled
# =============================================================================
class UnrepresentablePathTests(_Publisher):
    """On Linux a backslash is an ordinary filename character.

    ``_canonical_remote_path`` rewrites it into a separator, so
    ``/strg/a/back<backslash>slash`` and ``/strg/a/back/slash`` collapse to ONE catalog
    key. The canonicaliser cannot be changed in place — the existing catalog is
    built on it — so such a path must be withheld and reported, never stored
    under another file's name.
    """

    BACKSLASH_PATH = "/strg/a/back" + chr(92) + "slash"

    def test_the_legacy_canonicaliser_really_does_collapse_them(self):
        from src.pg_sessions import _canonical_remote_path
        self.assertEqual(_canonical_remote_path(self.BACKSLASH_PATH),
                         "/strg/a/back/slash")

    def test_such_a_path_is_withheld_from_planning(self):
        db = PublisherDB()
        self._segment(db, 1, [(self.BACKSLASH_PATH, 10),
                              ("/strg/a/normal", 20)])
        publisher = self._publisher(db)
        with mock.patch("src.scan_frontier.get_logger"):
            publisher.publish_ready_segments(0)
        appended = [r[1] for _ci, rows in db.appended for r in rows]
        self.assertEqual(appended, ["/strg/a/normal"])
        self.assertEqual(publisher.unrepresentable, [self.BACKSLASH_PATH])

    def test_it_is_recorded_as_an_unresolved_error(self):
        db = PublisherDB()
        db.record_scan_error = lambda **kwargs: db.errors.append(kwargs)
        self._segment(db, 1, [(self.BACKSLASH_PATH, 10)])
        with mock.patch("src.scan_frontier.get_logger"):
            self._publisher(db).publish_ready_segments(0)
        self.assertEqual(db.errors[0]["category"], "unrepresentable_path")
        self.assertEqual(db.errors[0]["disposition"], "unresolved")
        self.assertEqual(db.errors[0]["path"], self.BACKSLASH_PATH)

    def test_an_ordinary_path_is_unaffected(self):
        db = PublisherDB()
        self._segment(db, 1, [("/strg/a/ordinary", 10)])
        publisher = self._publisher(db)
        publisher.publish_ready_segments(0)
        self.assertEqual(publisher.unrepresentable, [])
        self.assertEqual(len(db.sealed), 1)


class RemotePosixPathValidatorTests(unittest.TestCase):
    """Task 3.3: a faithful validator, separate from Windows normalization."""

    #: One literal backslash — an ordinary Linux filename character.
    BACKSLASH = chr(92)

    def test_it_returns_the_path_completely_unchanged(self):
        from src.paths import validate_remote_posix_relpath
        b = self.BACKSLASH
        for path in ("a/b", f"back{b}slash", "with space", "יוניקוד",
                     "emoji-😀", f"a{b}b/c{b}d", "trailing.dot."):
            self.assertEqual(validate_remote_posix_relpath(path), path)

    def test_a_backslash_survives_where_the_legacy_helper_rewrites_it(self):
        from src.paths import (_safe_remote_relpath,
                               validate_remote_posix_relpath)
        name = f"a{self.BACKSLASH}b"
        self.assertEqual(validate_remote_posix_relpath(name), name)
        # The legacy helper deliberately does the opposite — for Windows
        # extraction, where a backslash IS a separator.
        self.assertEqual(_safe_remote_relpath(name), "a/b")

    def test_absolute_and_traversal_paths_are_refused(self):
        from src.paths import validate_remote_posix_relpath
        b = self.BACKSLASH
        for bad in ("/absolute", "C:/drive", f"C:{b}drive", "", "a//b",
                    "a/./b", "a/../b", "../escape", "a/"):
            with self.assertRaises(ValueError, msg=repr(bad)):
                validate_remote_posix_relpath(bad)

    def test_the_legacy_helper_is_documented_as_legacy(self):
        from src.paths import _safe_remote_relpath
        self.assertIn("Legacy compatibility path",
                      _safe_remote_relpath.__doc__)

    def test_legacy_safety_detection(self):
        from src.paths import remote_path_is_legacy_safe
        self.assertTrue(remote_path_is_legacy_safe("/strg/a/normal"))
        self.assertFalse(
            remote_path_is_legacy_safe(f"/strg/a/back{self.BACKSLASH}slash"))
