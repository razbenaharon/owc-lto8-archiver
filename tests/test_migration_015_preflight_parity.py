"""The Python migration-015 preflight must agree with the SQL, clause for clause.

Why this file exists
--------------------
``PgContainerMixin.classify_format_boundary`` is the read-only preflight an
operator reads *before* authorizing migration 015, and
``scripts/sql/015_postgres_container_formats.sql`` re-derives the same evidence
under table locks at assignment time.  If the two disagree, the preflight either
lies about a migration that will fail, or -- far worse -- reassures about one
that will not.

They diverged once.  The preflight kept the pre-correction run predicate, which
condemned a run for producing no ``files_index`` rows.  ``files_index`` indexes
only files at or above ``index_min_file_mb``, so a chunk built entirely from
small files produces none BY DESIGN; on production that flagged 7 of session
37's 9 archive runs while the corrected SQL flagged 0.  The directory clause
had the matching defect: it treated a run whose catalog rows span several chunk
indexes as contradictory, when archiving one finite *group* per run makes
min<>max the normal shape (2,731 rows blocked against the SQL's 0).

So every case below asserts **both** sides on one fixture: what the Python
preflight reports, and whether the SQL migration actually accepts it.  A future
edit to either one alone fails here.
"""
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import tempfile
import unittest
from typing import Any, TYPE_CHECKING, cast

if TYPE_CHECKING:
    import psycopg
    from psycopg.rows import dict_row
else:
    try:
        import psycopg
        from psycopg.rows import dict_row
    except ImportError:  # pragma: no cover - exercised by the skip below
        psycopg = None
        dict_row = None

from pg_test_guard import (SKIP_REASON, create_test_database,
                           drop_test_database, pg_available)


def _connect(*args, **kwargs) -> Any:
    return cast(Any, psycopg.connect(*args, **kwargs))


def _pg_available():
    if psycopg is None:
        return False
    return pg_available()


APPROVAL_ID = "migration-015-preflight-parity"
APPROVAL_REASON = "synthetic never-started suffix fixture"


@unittest.skipUnless(_pg_available(), SKIP_REASON)
class Migration015PreflightParityTests(unittest.TestCase):
    """Each test builds a disposable database and asserts preflight == SQL."""

    TAPE = "PARITY_TAPE"

    def setUp(self):
        from src.pg_db import PgDatabaseManager

        self.dbname, self.conninfo = create_test_database("lto_015_parity")
        self.db = PgDatabaseManager(self.conninfo)
        self.staging = tempfile.TemporaryDirectory()
        self.addCleanup(self._teardown)

    def _teardown(self):
        try:
            self.db.close()
        finally:
            try:
                self.staging.cleanup()
            finally:
                drop_test_database(self.dbname)

    # ---- fixture helpers -------------------------------------------------

    def _query(self, sql, params=()):
        with _connect(self.conninfo, autocommit=True,
                      row_factory=cast(Any, dict_row)) as conn:
            return conn.execute(sql, params).fetchall()

    def _exec(self, sql, params=()):
        with _connect(self.conninfo, autocommit=True) as conn:
            if params:
                conn.execute(sql, params)
            else:
                conn.execute(sql)

    def _base(self, *, with_007=True, chunk_count=6, done_prefix=3):
        """A session whose chunks 0..done_prefix-1 are done and the rest pending.

        This is the shape of the approved exception: an immutable ZIP prefix and
        a never-started Stored TAR suffix.
        """
        if with_007:
            self.db.apply_directory_catalog_schema()
        self.db.apply_incremental_scan_schema(finalize=True)
        self.db.register_tape(self.TAPE, 12000)
        session_id = self.db.create_remote_streaming_session(
            session_label="PARITY_SESSION",
            remote_host="fixture.example",
            remote_user="fixture",
            remote_path="/fixture/parity",
            tape_label=self.TAPE,
            staging_dir=self.staging.name,
        )
        for chunk_index in range(chunk_count):
            self.db.append_remote_streaming_chunk(
                session_id, chunk_index,
                [(chunk_index,
                  f"/fixture/parity/chunk_{chunk_index:03d}.bin",
                  f"chunk_{chunk_index:03d}.bin", chunk_index + 1)],
            )
        for chunk_index in range(done_prefix):
            self._exec(
                """UPDATE remote_chunks
                   SET status='done', owner_token=NULL, lease_expires_at=NULL,
                       attempt_id=NULL, updated_at=now()
                   WHERE session_id=%s AND chunk_index=%s""",
                (session_id, chunk_index))
        self.boundary = done_prefix
        return session_id

    def _run(self, session_id):
        """One archive run for this session, with no direct chunk identity.

        Pre-015 there is no ``archive_runs.remote_chunk_index`` column at all,
        so every run necessarily takes the "chunk identity is absent" branch --
        which is exactly the branch that regressed.
        """
        return self._query(
            """INSERT INTO archive_runs (run_label, tape_label,
                                         remote_session_id, session_kind,
                                         started_at)
               VALUES (%s,%s,%s,'remote',now()) RETURNING run_id""",
            (f"PARITY_RUN_{datetime.now(timezone.utc).timestamp():.6f}",
             self.TAPE, session_id))[0]["run_id"]

    def _catalog_file(self, run_id, session_id, chunk_index, *, tag):
        """A ``files_index`` row: the output a large-file chunk leaves behind."""
        path = f"/fixture/catalog/{tag}.bin"
        self.db.bulk_upsert_files([{
            "original_path": path,
            "canonical_source_path": path,
            "file_size_bytes": 1024,
            "tape_label": self.TAPE,
            "source_host": "fixture",
            "is_packed": False,
            "container_name": None,
            "stored_path": f"catalog/{tag}.bin",
            "remote_session_id": session_id,
            "remote_chunk_index": chunk_index,
        }])
        self._exec(
            "UPDATE files_index SET archive_run_id=%s WHERE original_path=%s",
            (run_id, path))
        return path

    def _bundle(self, run_id, session_id, *, chunk_index=None, tag="bundle"):
        """A ``directory_archive_bundles`` row: what a small-file run leaves."""
        return self._query(
            """INSERT INTO directory_archive_bundles
                   (source_host, original_dir_path, tape_label, archive_run_id,
                    remote_session_id, chunk_index, stored_bundle_path,
                    file_count, byte_count, small_file_count, small_file_bytes,
                    large_file_count, large_file_bytes, backup_date,
                    record_key)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now(),%s)
               RETURNING bundle_id""",
            ("fixture", f"/fixture/dir/{tag}", self.TAPE, run_id, session_id,
             chunk_index, f"bundles/{tag}.zip", 1800000, 4096, 1800000, 4096,
             0, 0,
             hashlib.sha256(
                 f"{self.TAPE}|/fixture/dir/{tag}|{tag}".encode()).digest(),
             ))[0]["bundle_id"]

    def _staging_evidence(self, indexes):
        return {
            "root_accessible": True,
            "checked_chunk_indexes": sorted(int(i) for i in indexes),
            "entry_count": 0,
            "unreadable_count": 0,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }

    # ---- the parity assertion -------------------------------------------

    def _preflight(self, session_id):
        return self.db.classify_format_boundary(session_id)

    def _apply_015(self, session_id, boundary, chunk_count):
        self.db.apply_container_format_schema(
            exception_session_id=session_id,
            expected_boundary=boundary,
            approval_id=APPROVAL_ID,
            approval_reason=APPROVAL_REASON,
            staging_evidence=self._staging_evidence(
                range(boundary, chunk_count)),
        )

    @contextmanager
    def _sql_refuses(self):
        with self.assertRaises(Exception) as caught:
            yield
        self.assertNotIsInstance(caught.exception, AssertionError)

    def assert_accepted(self, session_id, *, boundary, chunk_count):
        """Preflight reports no blocker AND the SQL migration applies."""
        report = self._preflight(session_id)
        self.assertEqual(report["blocking"], [],
                         "preflight blocked a case the SQL accepts")
        self.assertEqual(report["derived_boundary"], boundary)
        self._apply_015(session_id, boundary, chunk_count)
        rows = self._query(
            """SELECT chunk_index, status, packaging_format
               FROM remote_chunks WHERE session_id=%s ORDER BY chunk_index""",
            (session_id,))
        for row in rows:
            expected = ("stored_tar" if row["chunk_index"] >= boundary
                        else "zip")
            self.assertEqual(row["packaging_format"], expected)
            self.assertEqual(
                row["status"],
                "pending" if row["chunk_index"] >= boundary else "done")
        return report

    def assert_rejected(self, session_id, *, boundary, chunk_count,
                        blocker_fragment):
        """Preflight names the blocker AND the SQL migration refuses."""
        report = self._preflight(session_id)
        self.assertTrue(
            any(blocker_fragment in item for item in report["blocking"]),
            f"expected {blocker_fragment!r} in {report['blocking']!r}")
        with self._sql_refuses():
            self._apply_015(session_id, boundary, chunk_count)
        # A refused migration leaves no format behind at all.
        installed = self.db.container_format_schema_installed()
        if installed:
            formats = self._query(
                """SELECT count(*) AS n FROM remote_chunks
                   WHERE session_id=%s AND packaging_format IS NOT NULL""",
                (session_id,))[0]["n"]
            self.assertEqual(formats, 0)
        return report

    # ---- accepted: evidence that is healthy ------------------------------

    def test_small_file_only_run_is_accepted_through_bundle_evidence(self):
        """1.8M small files produce zero ``files_index`` rows, by design."""
        session_id = self._base()
        run_id = self._run(session_id)
        self._bundle(run_id, session_id, tag="small_only")
        self.assertEqual(
            self._query(
                "SELECT count(*) AS n FROM files_index WHERE archive_run_id=%s",
                (run_id,))[0]["n"], 0,
            "fixture must have no files_index rows to be meaningful")
        self.assert_accepted(session_id, boundary=3, chunk_count=6)

    def test_zero_output_superseded_run_is_accepted(self):
        """A run whose output was destroyed and redone misattributes nothing."""
        session_id = self._base()
        run_id = self._run(session_id)
        self.assertEqual(
            self._query(
                """SELECT (SELECT count(*) FROM files_index
                           WHERE archive_run_id=%s)
                        + (SELECT count(*) FROM directory_archive_bundles
                           WHERE archive_run_id=%s) AS n""",
                (run_id, run_id))[0]["n"], 0)
        self.assert_accepted(session_id, boundary=3, chunk_count=6)

    def test_multi_chunk_prefix_run_is_accepted(self):
        """One run archives a finite GROUP, so min<>max is the normal shape."""
        session_id = self._base()
        run_id = self._run(session_id)
        for chunk_index in range(3):          # the whole done prefix, one run
            self._catalog_file(run_id, session_id, chunk_index,
                               tag=f"group_{chunk_index}")
        self._bundle(run_id, session_id, chunk_index=0, tag="group_bundle")
        spread = self._query(
            """SELECT min(remote_chunk_index) AS lo, max(remote_chunk_index) AS hi
               FROM files_index WHERE archive_run_id=%s""", (run_id,))[0]
        self.assertLess(spread["lo"], spread["hi"],
                        "fixture must span several chunks to be meaningful")
        self.assert_accepted(session_id, boundary=3, chunk_count=6)

    def test_mixed_small_and_large_file_runs_are_accepted(self):
        """The three shapes above coexisting is production's actual shape."""
        session_id = self._base(chunk_count=8, done_prefix=5)
        large = self._run(session_id)
        for chunk_index in range(3):
            self._catalog_file(large, session_id, chunk_index,
                               tag=f"large_{chunk_index}")
        small = self._run(session_id)
        self._bundle(small, session_id, tag="small_run")
        self._run(session_id)                 # superseded, no output at all
        self.assert_accepted(session_id, boundary=5, chunk_count=8)

    # ---- rejected: evidence that reaches the suffix ----------------------

    def test_file_range_reaching_the_suffix_is_rejected(self):
        """Catalog output naming a suffix chunk means a suffix chunk started."""
        session_id = self._base()
        run_id = self._run(session_id)
        self._catalog_file(run_id, session_id, 0, tag="prefix")
        self._catalog_file(run_id, session_id, 4, tag="reaches_suffix")
        self.assert_rejected(
            session_id, boundary=3, chunk_count=6,
            blocker_fragment="contradictory/indeterminate chunks")

    def test_bundle_declaring_a_suffix_chunk_is_rejected(self):
        """A directory row placed directly in the suffix blocks."""
        session_id = self._base()
        run_id = self._run(session_id)
        self._catalog_file(run_id, session_id, 0, tag="prefix")
        self._bundle(run_id, session_id, chunk_index=5, tag="suffix_bundle")
        self.assert_rejected(
            session_id, boundary=3, chunk_count=6,
            blocker_fragment="contradictory TAR-suffix provenance")

    # ---- rejected: evidence that cannot be attributed --------------------

    def test_foreign_session_catalog_rows_are_rejected(self):
        """Output naming a different session is real uncertainty."""
        session_id = self._base()
        other = self.db.create_remote_streaming_session(
            session_label="PARITY_OTHER",
            remote_host="fixture.example",
            remote_user="fixture",
            remote_path="/fixture/other",
            tape_label=self.TAPE,
            staging_dir=self.staging.name,
        )
        self.db.append_remote_streaming_chunk(
            other, 0, [(0, "/fixture/other/chunk_000.bin",
                        "chunk_000.bin", 1)])
        run_id = self._run(session_id)
        self._catalog_file(run_id, other, 0, tag="foreign")
        self.assert_rejected(
            session_id, boundary=3, chunk_count=6,
            blocker_fragment="archive run(s) lack trustworthy")

    def test_foreign_session_bundle_rows_are_rejected(self):
        """The same rule applies to migration-007 bundle output."""
        session_id = self._base()
        other = self.db.create_remote_streaming_session(
            session_label="PARITY_OTHER_BUNDLE",
            remote_host="fixture.example",
            remote_user="fixture",
            remote_path="/fixture/other",
            tape_label=self.TAPE,
            staging_dir=self.staging.name,
        )
        self.db.append_remote_streaming_chunk(
            other, 0, [(0, "/fixture/other/chunk_000.bin",
                        "chunk_000.bin", 1)])
        run_id = self._run(session_id)
        self._bundle(run_id, other, tag="foreign_bundle")
        self.assert_rejected(
            session_id, boundary=3, chunk_count=6,
            blocker_fragment="archive run(s) lack trustworthy")

    def test_chunkless_catalog_rows_are_rejected(self):
        """Output that carries no chunk identity cannot be placed anywhere."""
        session_id = self._base()
        run_id = self._run(session_id)
        self._catalog_file(run_id, session_id, 0, tag="chunkless")
        self._exec(
            """UPDATE files_index SET remote_chunk_index=NULL
               WHERE archive_run_id=%s""", (run_id,))
        self.assert_rejected(
            session_id, boundary=3, chunk_count=6,
            blocker_fragment="lack trustworthy")

    def test_catalog_rows_naming_a_nonexistent_chunk_are_rejected(self):
        """A chunk index with no ``remote_chunks`` row is not provenance."""
        session_id = self._base()
        run_id = self._run(session_id)
        self._catalog_file(run_id, session_id, 0, tag="ghost")
        self._exec(
            """UPDATE files_index SET remote_chunk_index=99
               WHERE archive_run_id=%s""", (run_id,))
        self.assert_rejected(
            session_id, boundary=3, chunk_count=6,
            blocker_fragment="lack trustworthy")

    # ---- the migration-007-absent case -----------------------------------

    def test_parity_holds_when_migration_007_is_absent(self):
        """With no directory tables the bundle fragments must simply vanish.

        The SQL builds its three bundle expressions only when all three
        migration-007 tables exist; the preflight must collapse them the same
        way rather than, say, keeping the old predicate on that path.
        """
        session_id = self._base(with_007=False)
        self.assertEqual(
            self._query(
                """SELECT count(*) AS n FROM information_schema.tables
                   WHERE table_schema='public'
                     AND table_name IN ('directory_archive_stats',
                                        'directory_archive_bundles',
                                        'directory_tree_index')""")[0]["n"],
            0, "fixture must not have migration 007 installed")
        run_id = self._run(session_id)
        self._catalog_file(run_id, session_id, 0, tag="no007")
        report = self.assert_accepted(session_id, boundary=3, chunk_count=6)
        self.assertEqual(
            report["directory_evidence_summary"]["state"], "absent")

    def test_zero_output_run_is_accepted_when_migration_007_is_absent(self):
        """Without 007 a no-output run still has nothing to misattribute."""
        session_id = self._base(with_007=False)
        self._run(session_id)
        self.assert_accepted(session_id, boundary=3, chunk_count=6)

    def test_foreign_rows_still_rejected_when_migration_007_is_absent(self):
        """Collapsing the bundle fragments must not weaken the guard."""
        session_id = self._base(with_007=False)
        other = self.db.create_remote_streaming_session(
            session_label="PARITY_OTHER_NO_007",
            remote_host="fixture.example",
            remote_user="fixture",
            remote_path="/fixture/other",
            tape_label=self.TAPE,
            staging_dir=self.staging.name,
        )
        self.db.append_remote_streaming_chunk(
            other, 0, [(0, "/fixture/other/chunk_000.bin",
                        "chunk_000.bin", 1)])
        run_id = self._run(session_id)
        self._catalog_file(run_id, other, 0, tag="foreign_no007")
        self.assert_rejected(
            session_id, boundary=3, chunk_count=6,
            blocker_fragment="archive run(s) lack trustworthy")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
