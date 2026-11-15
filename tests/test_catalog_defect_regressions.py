"""Behavioural regressions for two catalog defects that failed on BOTH backends.

Neither statement could run at all before the fix, and neither had a
behavioural test: the functions were reached only through fakes.

* ``_derive_bundle_base_path`` read ``remote_scan_scopes.root_path`` (the
  column is ``source_root``, migration 014) and called ``self`` from a
  ``@staticmethod``.
* ``record_terminal_manifest`` named a PARTIAL unique index
  (``WHERE container_id IS NULL``, migration 015) in ``ON CONFLICT`` without
  the index predicate.
"""
import unittest

from db_backends import create_test_db, each_backend


@each_backend
class DeriveBundleBasePathTests(unittest.TestCase):
    """The scan-scope guard on a legacy bundle's derived source root."""

    BUNDLE = "Bundle_A.zip"

    def setUp(self):
        self.tdb = create_test_db(self.BACKEND, "bundle_base")
        self.db = self.tdb.manager()
        self.db.register_tape("TB")
        self.session_id = self.db.create_remote_streaming_session(
            "S_BASE", "host", "user", "/remote/root", "TB", "C:\\stg")
        self.db.bulk_upsert_files([{
            "original_path": "/remote/root/dir/a.txt",
            "file_size_bytes": 7,
            "tape_label": "TB",
            "source_host": "host",
            "is_packed": True,
            "container_name": self.BUNDLE,
            "stored_path": "dir/a.txt",
        }])

    def tearDown(self):
        self.tdb.drop()

    def _derive(self, session_id):
        conn = self.tdb.connect()
        try:
            return self.db._derive_bundle_base_path(
                conn, self.BUNDLE, session_id)
        finally:
            conn.close()

    def test_candidate_inside_a_persisted_scope_is_returned(self):
        self.db.create_scan_scopes(self.session_id, ["/remote/root"])
        self.assertEqual(self._derive(self.session_id), "/remote/root")

    def test_candidate_below_a_persisted_scope_is_returned(self):
        self.db.create_scan_scopes(self.session_id, ["/remote/"])
        self.assertEqual(self._derive(self.session_id), "/remote/root")

    def test_candidate_outside_every_persisted_scope_is_refused(self):
        self.db.create_scan_scopes(
            self.session_id, ["/elsewhere", "/remote/rootling"])
        self.assertEqual(self._derive(self.session_id), "")

    def test_session_without_scopes_keeps_the_candidate(self):
        self.assertEqual(self._derive(self.session_id), "/remote/root")

    def test_no_remote_session_skips_the_scope_guard(self):
        self.db.create_scan_scopes(self.session_id, ["/elsewhere"])
        self.assertEqual(self._derive(None), "/remote/root")


@each_backend
class RecordTerminalManifestTests(unittest.TestCase):
    """Recording a chunk's terminal manifest, and recording it again."""

    LOCATOR = "S_TERM/chunk_000000/terminal-state-v1.jsonl.zst"
    COUNTS = {"archived": 2, "source_missing": 1}

    def setUp(self):
        self.tdb = create_test_db(self.BACKEND, "terminal")
        self.db = self.tdb.manager()
        self.db.register_tape("TT")
        self.session_id = self.db.create_remote_streaming_session(
            "S_TERM", "host", "user", "/remote/root", "TT", "C:\\stg")
        self.tdb.run(
            """INSERT INTO remote_chunks (session_id,chunk_index,status,
                   updated_at,membership_state,expected_file_count,
                   expected_bytes,packaging_format,packaging_assigned_at)
               VALUES (%s,0,'done',now(),'sealed',3,300,'zip',now())""",
            (self.session_id,))

    def tearDown(self):
        self.tdb.drop()

    def _record(self, **overrides):
        arguments = dict(
            terminal_locator=self.LOCATOR, disposition_counts=self.COUNTS,
            archived_bytes=200, artifact_size_bytes=41)
        arguments.update(overrides)
        return self.db.record_terminal_manifest(
            self.session_id, 0, **arguments)

    def _artifacts(self):
        return self.tdb.query(
            """SELECT artifact_id, local_locator, artifact_size_bytes,
                      readiness_state, published_at
               FROM archive_artifacts WHERE artifact_kind='terminal_manifest'
               ORDER BY artifact_id""")

    def _chunk(self):
        return self.tdb.query(
            """SELECT terminal_manifest_artifact_id, final_archived_count,
                      final_source_missing_count, final_unresolved_count,
                      final_archived_bytes
               FROM remote_chunks WHERE session_id=%s AND chunk_index=0""",
            (self.session_id,))[0]

    def test_first_publication_records_artifact_and_aggregates(self):
        artifact_id = self._record()

        artifacts = self._artifacts()
        self.assertEqual(len(artifacts), 1)
        self.assertEqual(artifacts[0]["artifact_id"], artifact_id)
        self.assertEqual(artifacts[0]["local_locator"], self.LOCATOR)
        self.assertEqual(artifacts[0]["artifact_size_bytes"], 41)
        self.assertEqual(artifacts[0]["readiness_state"], "ready")
        chunk = self._chunk()
        self.assertEqual(chunk["terminal_manifest_artifact_id"], artifact_id)
        self.assertEqual(chunk["final_archived_count"], 2)
        self.assertEqual(chunk["final_source_missing_count"], 1)
        self.assertEqual(chunk["final_unresolved_count"], 0)
        self.assertEqual(chunk["final_archived_bytes"], 200)

    def test_identical_republication_is_idempotent(self):
        first = self._record()
        before = self._artifacts()

        self.assertEqual(self._record(), first)

        after = self._artifacts()
        self.assertEqual(len(after), 1)
        self.assertEqual(after[0]["published_at"], before[0]["published_at"])
        self.assertEqual(
            self._chunk()["terminal_manifest_artifact_id"], first)

    def test_republication_with_a_different_artifact_is_refused(self):
        first = self._record()
        for change in ({"terminal_locator": self.LOCATOR + ".other"},
                       {"artifact_size_bytes": 42}):
            with self.subTest(change=change):
                with self.assertRaisesRegex(RuntimeError, "refusing"):
                    self._record(**change)
        artifacts = self._artifacts()
        self.assertEqual(len(artifacts), 1)
        self.assertEqual(artifacts[0]["local_locator"], self.LOCATOR)
        self.assertEqual(artifacts[0]["artifact_size_bytes"], 41)
        self.assertEqual(
            self._chunk()["terminal_manifest_artifact_id"], first)

    def test_republication_with_different_aggregates_is_refused(self):
        self._record()
        with self.assertRaisesRegex(RuntimeError, "refusing"):
            self._record(
                disposition_counts={"archived": 3}, archived_bytes=300)
        chunk = self._chunk()
        self.assertEqual(chunk["final_archived_count"], 2)
        self.assertEqual(chunk["final_archived_bytes"], 200)

    def test_the_schema_guard_refuses_any_rewrite_of_a_ready_artifact(self):
        """Why a repeat is answered before the upsert: migration 018 makes a
        ready artifact immutable, so re-stamping ``published_at`` is refused
        by the schema on both backends."""
        artifact_id = self._record()
        with self.assertRaises(self.tdb.errors.GuardViolation):
            self.tdb.run(
                "UPDATE archive_artifacts SET published_at=now() "
                "WHERE artifact_id=%s", (artifact_id,))

    def test_an_unknown_chunk_is_refused(self):
        with self.assertRaisesRegex(RuntimeError, "not found"):
            self.db.record_terminal_manifest(
                self.session_id, 99, terminal_locator=self.LOCATOR,
                disposition_counts=self.COUNTS, archived_bytes=200,
                artifact_size_bytes=41)
        self.assertEqual(self._artifacts(), [])

    def test_a_part_locator_is_never_recorded(self):
        with self.assertRaises(RuntimeError):
            self._record(terminal_locator=self.LOCATOR + ".part")
        self.assertEqual(self._artifacts(), [])


if __name__ == "__main__":
    unittest.main()
