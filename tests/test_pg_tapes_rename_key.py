"""Regression coverage: rename_tape must rebuild record_key with remote
provenance (remote_session_id/remote_chunk_index) instead of the legacy
5-field call. Rebuilding with the legacy call would collide
provenance-distinct rows and silently re-key remote records so later
resume/upsert lookups miss them and insert duplicates.

Pure-logic tests only: no live PostgreSQL connection is used.
"""
import inspect
import unittest

from src.db import _file_record_key
from src.pg_tapes import PgTapeMixin


class FileRecordKeyProvenanceTests(unittest.TestCase):
    def test_remote_provenance_changes_the_key(self):
        legacy = _file_record_key(
            "/data/file.bin", "TAPE001", 3, 1, "so02")
        with_provenance = _file_record_key(
            "/data/file.bin", "TAPE001", 3, 1, "so02",
            remote_session_id=5, remote_chunk_index=2)
        self.assertNotEqual(legacy, with_provenance)

    def test_omitting_remote_fields_matches_the_legacy_5_field_key(self):
        # No remote provenance supplied -> byte-identical to the legacy key,
        # so local archives (and pre-existing catalog rows) are unaffected.
        legacy = _file_record_key(
            "/data/file.bin", "TAPE001", 3, 1, "so02")
        omitted = _file_record_key(
            "/data/file.bin", "TAPE001", 3, 1, "so02",
            remote_session_id=None, remote_chunk_index=None)
        self.assertEqual(legacy, omitted)

    def test_provenance_key_changes_with_tape_label_but_is_deterministic(self):
        old_label_key = _file_record_key(
            "/data/file.bin", "TAPE001", 3, 1, "so02",
            remote_session_id=5, remote_chunk_index=2)
        new_label_key = _file_record_key(
            "/data/file.bin", "TAPE002", 3, 1, "so02",
            remote_session_id=5, remote_chunk_index=2)
        new_label_key_recomputed = _file_record_key(
            "/data/file.bin", "TAPE002", 3, 1, "so02",
            remote_session_id=5, remote_chunk_index=2)
        # A rename changes the key (new tape label folded into the digest)...
        self.assertNotEqual(old_label_key, new_label_key)
        # ...but recomputing with identical inputs (what a retried rename
        # transaction does) converges on the exact same key.
        self.assertEqual(new_label_key, new_label_key_recomputed)


class RenameTapeSourceGuardTests(unittest.TestCase):
    """Guards against regression to the legacy 5-field _file_record_key call.

    Reading the source (rather than exercising a live DB) keeps this test
    hardware/DB-free while still catching a future edit that drops the
    remote_session_id/remote_chunk_index threading described in the
    rename_tape docstring.
    """

    def test_rename_tape_selects_remote_provenance_columns(self):
        source = inspect.getsource(PgTapeMixin.rename_tape)
        self.assertIn("remote_session_id", source)
        self.assertIn("remote_chunk_index", source)

    def test_rename_tape_passes_remote_provenance_into_file_record_key(self):
        source = inspect.getsource(PgTapeMixin.rename_tape)
        self.assertIn("_file_record_key(", source)
        call_start = source.index("_file_record_key(")
        # The remote_session_id/remote_chunk_index kwargs must appear inside
        # the _file_record_key(...) call itself, not merely elsewhere in the
        # method (e.g. only in the SELECT).
        call_tail = source[call_start:call_start + 400]
        self.assertIn("remote_session_id=", call_tail)
        self.assertIn("remote_chunk_index=", call_tail)


if __name__ == "__main__":
    unittest.main()
