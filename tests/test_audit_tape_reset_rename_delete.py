"""Audit M3 - tape reset, rename and delete against the real schema.

Three defects, each reproduced on disposable PostgreSQL with migrations
013-018 installed:

* the reset cleanup ignored the tables migrations 015/018 added, so it could
  not even delete the catalog (a restrictive foreign key from
  ``directory_archive_parts``), and containers copied to the erased generation
  kept counting as used space;
* a reset interrupted after its cleanup stage could never be resumed, which
  also blocked any new reset of that tape;
* renaming a tape reset its generation number to 1, and deleting a tape always
  failed.

No tape, drive or LTFS path is touched: every hardware call is patched.
"""
import contextlib
import os
import stat
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest import mock

try:
    import psycopg
    from psycopg.rows import dict_row
except ImportError:                          # pragma: no cover - guard handles it
    psycopg = None
    dict_row = None

from db_backends import (create_test_db, each_backend, postgresql_only,
                         sqlite_only)
from pg_test_guard import (SKIP_REASON, create_test_database,
                           drop_test_database, pg_available)
from test_directory_catalog_pg import _DirectoryCatalogFixture

from src import tape_reset


def _pg_available():
    return psycopg is not None and pg_available()


def _sha():
    return "0" * 64


class _ResetFixture(_DirectoryCatalogFixture):
    """Session on T_A with three 'done' chunks, a loose part and a container."""

    def setUp(self):
        super().setUp()
        self._part(self.dir_id, 0)                        # loose, files_index
        self._container_part(self.dir_id, 1, writer_state="copied")
        self._exec("UPDATE archive_containers SET actual_artifact_bytes=500")
        self.transitions = [
            {"session_id": self.session_id, "chunk_index": index,
             "before": "done", "after": "pending",
             "reason": "latest durable output erased"}
            for index in range(3)]

    def _operation(self, operation_id="op-m3"):
        self.db.create_tape_reset_intent(
            operation_id=operation_id, tape_label="T_A", expected_drive="X:",
            expected_drive_serial="SER", expected_ltfs_gen=1, expected_vcr=1,
            backup_path="b.dump", backup_sha256=_sha(),
            restore_list_path="r.txt", restore_list_sha256=_sha(),
            shadow_database="shadow", impact_report_path="i.json",
            impact_report_sha256=_sha(), impact={})
        self.db.mark_tape_reset_hardware_started(operation_id)
        self.db.mark_tape_reset_hardware_succeeded(operation_id, {"ok": True})
        return operation_id

    def _count(self, table, where="tape_label='T_A'"):
        return self._query(
            f"SELECT count(*) AS n FROM {table} WHERE {where}")[0]["n"]


@each_backend
class ResetCleanupContainerTablesTests(_ResetFixture, unittest.TestCase):
    def test_used_space_counts_a_copied_container_before_the_reset(self):
        self.assertEqual(self.db.recalculate_tape_used_space("T_A"), 600)

    def test_cleanup_clears_the_restore_routes_and_the_catalog(self):
        """Used to die on directory_archive_parts -> files_index RESTRICT."""
        evidence = self.db.apply_tape_reset_cleanup(
            self._operation(), self.transitions)

        self.assertEqual(self._count("directory_archive_parts"), 0)
        self.assertEqual(self._count("files_index"), 0)
        self.assertEqual(self._count("archive_runs"), 0)
        self.assertEqual(evidence["deleted"]["directory_archive_parts"], 2)
        self.assertEqual(
            evidence["before_counts"]["directory_archive_parts"], 2)
        self.assertEqual(
            evidence["after_counts"]["directory_archive_parts"], 0)
        statuses = [row["status"] for row in self._query(
            "SELECT status FROM remote_chunks WHERE session_id=%s "
            "ORDER BY chunk_index", (self.session_id,))]
        self.assertEqual(statuses, ["pending"] * 3)
        self.assertEqual(self.db.get_active_tape_generation("T_A"), 2)

    def test_the_erased_generation_does_not_return_to_the_accounting(self):
        self.db.apply_tape_reset_cleanup(self._operation(), self.transitions)
        # The container row is kept as history of the retired generation...
        row = self._query(
            """SELECT c.writer_state, g.state, g.generation
               FROM archive_containers c
               JOIN tape_generations g
                 ON g.generation_id=c.tape_generation_id""")[0]
        self.assertEqual((row["writer_state"], row["state"],
                          row["generation"]), ("copied", "retired", 1))
        # ...but a later mount's recalculation must not count it again.
        self.assertEqual(self.db.recalculate_tape_used_space("T_A"), 0)
        self.assertEqual(self.db.get_tape("T_A")["used_space"], 0)

    def test_cleanup_is_idempotent_once_it_has_succeeded(self):
        operation_id = self._operation()
        first = self.db.apply_tape_reset_cleanup(operation_id,
                                                 self.transitions)
        again = self.db.apply_tape_reset_cleanup(operation_id,
                                                 self.transitions)
        self.assertEqual(first["deleted"], again["deleted"])
        self.assertEqual(self.db.get_active_tape_generation("T_A"), 2)
        self.assertEqual(self._count(
            "tape_generations", "volume_label='T_A'"), 2)

    def test_another_tape_is_untouched(self):
        # The fixture's only archive run belongs to T_A; give T_B its own so
        # the T_B catalog row does not hang off a run the reset will delete.
        self.run_id = self._query(
            """INSERT INTO archive_runs (run_label,tape_label,session_kind,
                   started_at,completed_at,remote_session_id)
               VALUES ('fixture:T_B','T_B','remote',now(),now(),%s)
               RETURNING run_id""", (self.session_id,))[0]["run_id"]
        self._part(self.child_id, 2, tape_label="T_B")
        self.db.apply_tape_reset_cleanup(self._operation(), self.transitions)
        self.assertEqual(
            self._count("directory_archive_parts", "tape_label='T_B'"), 1)
        self.assertEqual(self._count("files_index", "tape_label='T_B'"), 1)


@each_backend
class ResetResumeTests(_ResetFixture, unittest.TestCase):
    """execute_reset end to end, with every hardware call patched."""

    PRE = {"label": "T_A", "drive": "X:\\", "drive_serial": "SER",
           "device": "tape0", "ltfs_generation": 7, "vcr": 70}
    POST = {"label": "T_A", "drive": "X:\\", "drive_serial": "SER",
            "device": "tape0", "ltfs_generation": 1, "vcr": 2}

    def setUp(self):
        super().setUp()
        self.tmp = Path(tempfile.mkdtemp(prefix="lto_m3_"))
        self.addCleanup(self._rmtree)
        backup = self.tmp / "backup.dump"
        backup.write_bytes(b"backup")
        restore = self.tmp / "restore.list"
        restore.write_text("1; entry\n", encoding="utf-8")
        self.identity = dict(self.PRE)
        self.write_test_error = None
        self.formats = 0
        self.cfg = SimpleNamespace(lto_drive="X:\\")
        self.operation_id = "op-resume"

        latest = {(self.session_id, index): {
            "tape_label": "T_A", "status": "completed",
            "finished_at": "2026-01-01T00:00:00"} for index in range(3)}

        def fake_format(drive, label, operation_id):
            self.formats += 1
            self.identity = dict(self.POST)
            return {"mounted_identity": dict(self.POST), "ejected": False}

        def fake_write_test(drive, operation_id):
            if self.write_test_error:
                raise RuntimeError(self.write_test_error)
            return {"bytes_written": 1}

        stack = contextlib.ExitStack()
        self.addCleanup(stack.close)
        for name, value in (
                ("AUDIT_ROOT", self.tmp / "audits"),
                ("mounted_identity", lambda drive: dict(self.identity)),
                ("latest_chunk_write_evidence", lambda *a, **k: (latest, {})),
                ("_forbidden_processes", lambda pid=None: []),
                ("_format_and_positive_evidence", fake_format),
                ("smallest_write_test", fake_write_test),
                ("_acquire_tape_io_lock", lambda *a, **k: None),
                ("_release_tape_io_lock", lambda *a, **k: None)):
            stack.enter_context(mock.patch.object(tape_reset, name, value))

        backup_info = tape_reset.verify_backup_inputs(backup, restore)
        backup_info["shadow_verified"] = True
        report = tape_reset.build_impact_report(
            self.db, operation_id=self.operation_id, tape_label="T_A",
            identity=dict(self.PRE), backup=backup_info,
            shadow_database="shadow")
        self.impact_path, _ = tape_reset.write_immutable_json(
            tape_reset.audit_dir(self.operation_id) / "impact.json", report)

    def _rmtree(self):
        for root, _dirs, files in os.walk(self.tmp):
            for name in files:
                os.chmod(os.path.join(root, name), stat.S_IWRITE)
        import shutil
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _run(self, resume=False):
        return tape_reset.execute_reset(
            self.db, self.cfg, tape_label="T_A", impact_path=self.impact_path,
            delete_catalog=True, confirmed=True,
            resume_operation=self.operation_id if resume else None)

    def _state(self):
        return self.db.get_tape_reset_operation(self.operation_id)["state"]

    def _interrupt_after_cleanup(self):
        self.write_test_error = "interrupted after cleanup"
        with self.assertRaisesRegex(RuntimeError, "interrupted after cleanup"):
            self._run()
        self.assertEqual(self._state(), "cleanup_succeeded")
        self.write_test_error = None

    def test_an_uninterrupted_reset_reaches_verified(self):
        final = self._run()
        self.assertEqual(final["state"], "verified")
        self.assertEqual(self.formats, 1)

    def test_a_reset_interrupted_after_cleanup_can_be_resumed(self):
        self._interrupt_after_cleanup()
        final = self._run(resume=True)
        self.assertEqual(final["state"], "verified")
        self.assertEqual(self.formats, 1, "a resume must never format again")
        self.assertEqual(self.db.get_active_tape_generation("T_A"), 2)
        # Running it yet again is a no-op that reports the same outcome.
        self.assertEqual(self._run(resume=True)["state"], "verified")
        self.assertEqual(self.formats, 1)

    def test_a_finished_reset_no_longer_blocks_a_new_one(self):
        self._interrupt_after_cleanup()
        self._run(resume=True)
        row = self.db.create_tape_reset_intent(
            operation_id="op-next", tape_label="T_A", expected_drive="X:",
            expected_drive_serial="SER", expected_ltfs_gen=1, expected_vcr=2,
            backup_path="b", backup_sha256=_sha(), restore_list_path="r",
            restore_list_sha256=_sha(), shadow_database="s",
            impact_report_path="i", impact_report_sha256=_sha(), impact={})
        self.assertEqual((row["old_generation"], row["new_generation"]),
                         (2, 3))

    def test_resume_survives_losing_the_audit_files(self):
        """The database copy of each stage's evidence is enough to converge."""
        self._interrupt_after_cleanup()
        directory = tape_reset.audit_dir(self.operation_id)
        for name in ("hardware_result.json", "cleanup_result.json"):
            os.chmod(directory / name, stat.S_IWRITE)
            os.remove(directory / name)
        final = self._run(resume=True)
        self.assertEqual(final["state"], "verified")
        self.assertEqual(self.formats, 1)
        for name in ("hardware_result.json", "cleanup_result.json",
                     "verification.json", "final_report.json"):
            self.assertTrue((directory / name).exists(), name)

    def test_resume_still_refuses_a_different_impact_report(self):
        self._interrupt_after_cleanup()
        self._exec("UPDATE tape_reset_operations SET impact_report_sha256=%s",
                   ("f" * 64,))
        with self.assertRaisesRegex(RuntimeError, "Impact report differs"):
            self._run(resume=True)
        self.assertEqual(self._state(), "cleanup_succeeded")

    def test_resume_still_refuses_a_different_cartridge(self):
        self._interrupt_after_cleanup()
        self.identity["drive_serial"] = "OTHER"
        with self.assertRaisesRegex(RuntimeError, "identity changed"):
            self._run(resume=True)
        self.assertEqual(self._state(), "cleanup_succeeded")

    def test_scope_drift_before_cleanup_is_still_refused(self):
        self._exec("UPDATE remote_chunks SET status='done' WHERE false")
        self._part(self.child_id, 2)        # the catalog changed after approval
        with self.assertRaisesRegex(RuntimeError, "scope differs"):
            self._run()
        self.assertEqual(self.formats, 0)


@each_backend
class PrepareImpactReportTests(_ResetFixture, unittest.TestCase):
    """``run.py tape-reset --prepare-impact-report``: the read-only step that
    writes the report the destructive reset requires.

    Nothing called it before, so the documented reset could not be started
    from the command line. Hardware calls are patched; the catalog is real.
    """

    IDENTITY = {"label": "T_A", "drive": "X:\\", "drive_serial": "SER",
                "device": "tape0", "ltfs_generation": 7, "vcr": 70}
    POST = {"label": "T_A", "drive": "X:\\", "drive_serial": "SER",
            "device": "tape0", "ltfs_generation": 1, "vcr": 2}

    def setUp(self):
        super().setUp()
        self.tmp = Path(tempfile.mkdtemp(prefix="lto_prepare_"))
        self.addCleanup(self._rmtree)
        self.identity = dict(self.IDENTITY)
        self.identity_reads = 0
        self.formats = 0
        self.cfg = SimpleNamespace(
            db_backend=self.BACKEND, db_dsn=self.conninfo,
            sqlite_path=self.tdb.path, lto_drive="",
            lto_drive_configured=False)
        self.latest = latest = {(self.session_id, index): {
            "tape_label": "T_A", "status": "completed",
            "finished_at": "2026-01-01T00:00:00"} for index in range(3)}

        def read_identity(drive):
            self.identity_reads += 1
            return dict(self.identity)

        def fake_format(drive, label, operation_id):
            self.formats += 1
            self.identity = dict(self.POST)
            return {"mounted_identity": dict(self.POST), "ejected": False}

        stack = contextlib.ExitStack()
        self.addCleanup(stack.close)
        for name, value in (
                ("AUDIT_ROOT", self.tmp / "audits"),
                ("mounted_identity", read_identity),
                ("latest_chunk_write_evidence", lambda *a, **k: (latest, {})),
                ("_forbidden_processes", lambda pid=None: []),
                ("_format_and_positive_evidence", fake_format),
                ("smallest_write_test",
                 lambda drive, operation_id: {"bytes_written": 1}),
                ("_acquire_tape_io_lock", lambda *a, **k: None),
                ("_release_tape_io_lock", lambda *a, **k: None)):
            stack.enter_context(mock.patch.object(tape_reset, name, value))
        self.scratch = None
        self.pg_verification_level = "restored"
        if self.BACKEND == "sqlite":
            from src import db_backup
            self.backup = db_backup.create_database_backup(
                self.cfg, output_dir=str(self.tmp / "backups"))
        else:
            # The pg_dump / pg_restore toolchain is not what is under test:
            # the verification result is supplied, the catalog work is real.
            self.backup = str(self.tmp / "catalog.dump")
            Path(self.backup).write_bytes(b"dump")
            restore_list = self.tmp / "catalog.restore_list.txt"
            restore_list.write_text("1; TABLE DATA tapes\n", encoding="utf-8")
            self.scratch = SimpleNamespace(pg_host="127.0.0.1")
            self.verify_calls = []

            def fake_verify(cfg, backup_path, **options):
                from src.pg_backup import BackupCheck, BackupVerification
                self.verify_calls.append(options)
                return BackupVerification(
                    backend="postgresql", backup_path=str(backup_path),
                    size_bytes=4, sha256="0" * 64,
                    level=self.pg_verification_level,
                    checks=(BackupCheck("scratch_restore", True, "ok"),),
                    artifacts={"restore_list_path": str(restore_list)},
                    details={"scratch_restore": {
                        "scratch_database": "ltoverify_fixture"}})

            stack.enter_context(mock.patch(
                "src.db_backup.verify_backup", fake_verify))
        self.out = self.tmp / "impact.json"

    def _rmtree(self):
        for root, _dirs, files in os.walk(self.tmp):
            for name in files:
                os.chmod(os.path.join(root, name), stat.S_IWRITE)
        import shutil
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _prepare(self, **overrides):
        options = dict(tape_label="T_A", output_path=str(self.out),
                       backup_path=self.backup, scratch_server=self.scratch)
        options.update(overrides)
        return tape_reset.prepare_impact_report(self.cfg, **options)

    def _reset(self, impact_path, delete_catalog=True, confirmed=True):
        return tape_reset.execute_reset(
            self.db, SimpleNamespace(lto_drive="X:\\"), tape_label="T_A",
            impact_path=impact_path, delete_catalog=delete_catalog,
            confirmed=confirmed)

    def _catalog_state(self):
        tables = ("tapes", "remote_sessions", "remote_chunks", "files_index",
                  "archive_runs", "archive_containers",
                  "directory_archive_parts", "tape_reset_operations",
                  "tape_generations")
        state = {table: self._count(table, "1=1") for table in tables}
        state["chunk_statuses"] = [
            (row["chunk_index"], row["status"]) for row in self._query(
                "SELECT chunk_index,status FROM remote_chunks "
                "ORDER BY session_id,chunk_index")]
        return state

    def test_prepare_writes_the_report_and_changes_nothing(self):
        before = self._catalog_state()
        result = self._prepare()
        self.assertEqual(self._catalog_state(), before)
        self.assertEqual(before["tape_reset_operations"], 0)
        self.assertEqual(self.formats, 0)
        self.assertEqual(self.identity_reads, 1)

        path = Path(result["impact_report"])
        self.assertEqual(path, self.out.resolve())
        self.assertFalse(os.access(path, os.W_OK), "the report is write-once")
        report = tape_reset.read_json(path)
        self.assertEqual(result["impact_report_sha256"],
                         tape_reset.sha256_file(path))
        self.assertEqual(report["target_label"], "T_A")
        self.assertEqual(report["operation_id"], result["operation_id"])
        self.assertEqual(report["mounted_identity"], self.IDENTITY)
        self.assertTrue(report["backup"]["shadow_verified"])
        self.assertTrue(report["backup"]["shadow_verification"]["ok"])
        self.assertEqual(report["backup"]["backup_sha256"],
                         tape_reset.sha256_file(self.backup))
        self.assertEqual(result["chunks_returning_to_pending"], 3)
        self.assertEqual(report["planned_chunk_transitions"],
                         self.transitions)
        self.assertIn("--impact-report", result["next_command"])
        self.assertIn("--delete-catalog --yes", result["next_command"])
        # A preparation creates no audit directory: only the report.
        self.assertFalse((self.tmp / "audits").exists())
        if self.BACKEND == "sqlite":
            self.assertEqual(
                report["backup"]["shadow_verification"]["level"],
                "read_through")
            self.assertEqual(report["shadow_database"],
                             report["backup"]["backup_path"])
        else:
            self.assertEqual(self.verify_calls,
                             [{"deep_target": self.scratch}])
            self.assertEqual(report["shadow_database"], "ltoverify_fixture")

    def test_the_prepared_report_authorises_the_unchanged_reset(self):
        result = self._prepare()
        final = self._reset(result["impact_report"])
        self.assertEqual(final["state"], "verified")
        self.assertEqual(final["operation_id"], result["operation_id"])
        self.assertEqual(self.formats, 1)
        self.assertEqual(self._count("files_index"), 0)

    def test_the_reset_still_refuses_without_its_two_confirmations(self):
        result = self._prepare()
        for delete_catalog, confirmed in ((True, False), (False, True),
                                          (False, False)):
            with self.subTest(delete=delete_catalog, yes=confirmed):
                with self.assertRaisesRegex(RuntimeError, "mandatory"):
                    self._reset(result["impact_report"],
                                delete_catalog=delete_catalog,
                                confirmed=confirmed)
        self.assertEqual(self.formats, 0)

    def test_a_report_for_another_catalog_state_is_still_refused(self):
        result = self._prepare()
        self._part(self.child_id, 2)        # the catalog changed afterwards
        with self.assertRaisesRegex(RuntimeError, "scope differs"):
            self._reset(result["impact_report"])
        self.assertEqual(self.formats, 0)

    def _make_container_stored_tar(self):
        """Add a done Stored-TAR chunk 3 on T_A that owns one container.

        Stored TAR needs an approved session boundary that this fixture does
        not model, so the throwaway catalog's new-chunk format guard is
        switched off for this one row.
        """
        if self.BACKEND == "sqlite":
            self._exec("DROP TRIGGER trg_remote_chunks_initial_format")
        else:
            self._exec("ALTER TABLE remote_chunks DISABLE TRIGGER USER")
        self._exec(
            """INSERT INTO remote_chunks (session_id,chunk_index,status,
                   updated_at,membership_state,expected_file_count,
                   expected_bytes,packaging_format,packaging_assigned_at,
                   plan_source)
               VALUES (%s,3,'done',now(),'sealed',1,100,'stored_tar',
                       now(),'legacy_db')""", (self.session_id,))
        if self.BACKEND != "sqlite":
            self._exec("ALTER TABLE remote_chunks ENABLE TRIGGER USER")
        self.latest[(self.session_id, 3)] = {
            "tape_label": "T_A", "status": "completed",
            "finished_at": "2026-01-01T00:00:00"}
        self._query(
            """INSERT INTO archive_containers (session_id,chunk_index,
                   container_ordinal,container_format,format_version,
                   tar_dialect,storage_class,container_name,
                   expected_member_count,expected_logical_bytes,
                   validation_state,writer_state,catalog_state,
                   tape_label,tape_path,tape_generation_id)
               VALUES (%s,3,3,'stored_tar','1','ustar','small_files',
                       'c3.tar',1,100,'planned','not_started','not_started',
                       'T_A','/tape/c3.tar',%s)
               RETURNING container_id""",
            (self.session_id, self.generations["T_A"]))

    def test_a_cartridge_with_stored_tar_chunks_gets_no_report(self):
        # Such a chunk cannot be written again after the reset, so erasing
        # its only tape copy is refused before anything is changed.
        self._make_container_stored_tar()
        before = self._catalog_state()
        with self.assertRaisesRegex(RuntimeError, "Stored-TAR"):
            self._prepare()
        self.assertEqual(self._catalog_state(), before)
        self.assertFalse(self.out.exists())
        self.assertEqual(self.formats, 0)

    def test_the_reset_itself_refuses_stored_tar_chunks_before_formatting(
            self):
        # A report written before this check existed.
        self._make_container_stored_tar()
        with mock.patch.object(tape_reset, "refuse_unrewritable_chunks",
                               lambda *a, **k: None):
            result = self._prepare()
        before = self._catalog_state()
        with self.assertRaisesRegex(RuntimeError, "Stored-TAR"):
            self._reset(result["impact_report"])
        self.assertEqual(self.formats, 0)
        self.assertEqual(self._catalog_state(), before)

    def test_a_resumed_reset_that_never_formatted_is_refused_too(self):
        # An operation recorded before this check, stopped before the
        # format, must not reach the formatter when it is resumed.
        self._make_container_stored_tar()

        def failed_format(drive, label, operation_id):
            raise RuntimeError("format interrupted")

        with mock.patch.object(tape_reset, "refuse_unrewritable_chunks",
                               lambda *a, **k: None):
            result = self._prepare()
            with mock.patch.object(tape_reset,
                                   "_format_and_positive_evidence",
                                   failed_format):
                with self.assertRaisesRegex(RuntimeError, "interrupted"):
                    self._reset(result["impact_report"])
        self.assertEqual(self._count("tape_reset_operations", "1=1"), 1)
        with self.assertRaisesRegex(RuntimeError, "Stored-TAR"):
            tape_reset.execute_reset(
                self.db, SimpleNamespace(lto_drive="X:\\"),
                tape_label="T_A", impact_path=result["impact_report"],
                delete_catalog=True, confirmed=True,
                resume_operation=result["operation_id"])
        self.assertEqual(self.formats, 0)

    def test_an_existing_file_is_never_overwritten(self):
        self.out.write_text("mine", encoding="utf-8")
        with self.assertRaisesRegex(RuntimeError, "Refusing to overwrite"):
            self._prepare()
        self.assertEqual(self.out.read_text(encoding="utf-8"), "mine")
        self.assertEqual(self.identity_reads, 0)

    def test_another_cartridge_in_the_drive_is_refused(self):
        self.identity["label"] = "T_B"
        with self.assertRaisesRegex(RuntimeError, "Mounted label"):
            self._prepare()
        self.assertFalse(self.out.exists())

    def test_an_unknown_tape_is_refused(self):
        self.identity["label"] = "NOPE"
        with self.assertRaisesRegex(RuntimeError, "Tape not found"):
            self._prepare(tape_label="NOPE")
        self.assertFalse(self.out.exists())

    def test_an_unproven_backup_writes_no_report(self):
        if self.BACKEND == "sqlite":
            with open(self.backup, "r+b") as handle:
                handle.seek(os.path.getsize(self.backup) // 2)
                handle.write(b"\xff" * 4096)
        else:
            self.pg_verification_level = "read_through"   # never restored
        with self.assertRaisesRegex(RuntimeError, "not proven restorable"):
            self._prepare()
        self.assertFalse(self.out.exists())
        self.assertEqual(self.identity_reads, 0)

    @sqlite_only("a PostgreSQL catalog is not a file")
    def test_the_live_catalog_is_not_accepted_as_its_own_backup(self):
        with self.assertRaisesRegex(RuntimeError, "live catalog itself"):
            self._prepare(backup_path=self.tdb.path)
        self.assertFalse(self.out.exists())
        self.assertEqual(self.identity_reads, 0)
        self.assertFalse(os.path.exists(
            os.path.splitext(self.tdb.path)[0] + ".restore_list.txt"))

    @sqlite_only("a write-ahead log is a SQLite file")
    def test_a_backup_that_depends_on_a_write_ahead_log_is_refused(self):
        """The reset pins the backup by the hash of ONE file. A copy whose
        latest data sits in a ``-wal`` beside it is not covered by that."""
        import shutil
        import sqlite3
        copied = str(self.tmp / "copied.sqlite3")
        shutil.copy(self.backup, copied)
        writer = sqlite3.connect(copied)
        reader = sqlite3.connect(copied)     # keeps the log from closing out
        try:
            writer.execute("PRAGMA wal_autocheckpoint=0")
            writer.execute("UPDATE tapes SET status='full'")
            writer.commit()
            reader.execute("SELECT count(*) FROM tapes").fetchone()
            self.assertGreater(os.path.getsize(copied + "-wal"), 0)
            with self.assertRaisesRegex(RuntimeError, "write-ahead log"):
                self._prepare(backup_path=copied)
        finally:
            writer.close()
            reader.close()
        self.assertFalse(self.out.exists())
        self.assertEqual(self.identity_reads, 0)

    @postgresql_only("a SQLite backup is itself the restorable database")
    def test_postgresql_needs_a_named_scratch_server(self):
        with self.assertRaisesRegex(RuntimeError, "--scratch-host"):
            self._prepare(scratch_server=None)
        self.assertEqual(self.verify_calls, [])
        self.assertFalse(self.out.exists())

    def test_the_catalog_connection_refuses_writes(self):
        db = tape_reset.open_read_only_catalog(self.cfg)
        try:
            with self.assertRaises(self.tdb.errors.ReadOnly):
                with db._pool.connection() as conn:
                    conn.execute("UPDATE tapes SET status='full'")
        finally:
            db.close()
        self.assertEqual(self._count("tapes", "status='full'"), 0)


@each_backend
class RenameWithContainersTests(_ResetFixture, unittest.TestCase):
    def test_a_rename_carries_containers_and_restore_routes(self):
        """Used to fail: the new tables still referenced the old label."""
        # Loose restore routes are out of scope here: they reference
        # files_index.record_key, which a rename rewrites (see the test below).
        self._exec("DELETE FROM directory_archive_parts "
                   "WHERE loose_record_key IS NOT NULL")
        self.db.rename_tape("T_A", "T_NEW")
        self.assertEqual(self._count("tapes", "volume_label='T_A'"), 0)
        self.assertEqual(
            self._count("archive_containers", "tape_label='T_NEW'"), 1)
        self.assertEqual(
            self._count("directory_archive_parts", "tape_label='T_NEW'"), 1)
        self.assertEqual(self.db.get_active_tape_generation("T_NEW"), 1)

    def test_a_rename_blocked_by_loose_routes_changes_nothing(self):
        """Known limit, pinned so it stays fail-closed.

        A rename rewrites files_index.record_key, and migration 018's loose
        restore routes hold a foreign key on that key. The rename is refused
        by the database and must roll back completely - never half-renamed.
        """
        with self.assertRaises(self.tdb.errors.ForeignKeyViolation):
            self.db.rename_tape("T_A", "T_NEW")
        self.assertEqual(self._count("tapes", "volume_label='T_NEW'"), 0)
        self.assertEqual(self._count("files_index"), 1)
        self.assertEqual(self._count("archive_containers"), 1)
        self.assertEqual(self.db.get_active_tape_generation("T_A"), 1)


@each_backend
class RenameAndDeleteTests(unittest.TestCase):
    def setUp(self):
        from src.pg_db import PgDatabaseManager

        self.tdb = create_test_db(getattr(self, 'BACKEND', None) or 'postgresql', "lto_m3c")
        self.dbname, self.conninfo = self.tdb.name, self.tdb.conninfo
        self.db = self.tdb.manager(migrations="startup")
        self.addCleanup(self._drop)

    def _drop(self):
        try:
            self.db.close()
        except Exception:
            pass
        self.tdb.drop()

    def _q(self, sql, params=()):
        return self.tdb.run(sql, params)

    def _reset_twice(self, label):
        """Leave ``label`` at generation 3 with two retired generations."""
        self._q("UPDATE tape_generations SET state='retired' "
                "WHERE volume_label=%s", (label,))
        self._q(
            """INSERT INTO tape_generations (tape_id,volume_label,generation,
                   state,formatted_at)
               SELECT tape_id,volume_label,g,
                      CASE WHEN g=3 THEN 'active' ELSE 'retired' END, now()
               FROM tapes, (SELECT 2 AS g UNION ALL SELECT 3) v
               WHERE volume_label=%s""",
            (label,))
        self._q("UPDATE tapes SET current_generation=3 WHERE volume_label=%s",
                (label,))

    def test_a_rename_keeps_the_generation_number(self):
        self.db.register_tape("GEN_OLD", 12000)
        self._reset_twice("GEN_OLD")
        self.assertEqual(self.db.get_active_tape_generation("GEN_OLD"), 3)

        self.db.rename_tape("GEN_OLD", "GEN_NEW")

        self.assertEqual(self.db.get_tape("GEN_NEW")["current_generation"], 3)
        self.assertEqual(self.db.get_active_tape_generation("GEN_NEW"), 3)
        # The exact-generation lookup the writer uses must still resolve.
        row = self._q(
            """SELECT g.generation FROM tape_generations g
               JOIN tapes t ON t.tape_id=g.tape_id
               WHERE g.volume_label='GEN_NEW' AND g.state='active'
                 AND g.generation=t.current_generation""")
        self.assertEqual([r["generation"] for r in row], [3])

    def test_an_unreferenced_tape_can_be_deleted(self):
        """Used to fail for every tape: its own generation row blocked it."""
        self.db.register_tape("DEL1", 12000)
        self.db.delete_tape("DEL1")
        self.assertFalse(self.db.tape_exists("DEL1"))
        self.assertEqual(self._q(
            "SELECT 1 FROM tape_generations WHERE volume_label='DEL1'"), [])

    def test_deleting_a_missing_tape_is_reported(self):
        with self.assertRaisesRegex(RuntimeError, "Tape not found"):
            self.db.delete_tape("NOPE")

    def test_a_tape_with_session_history_is_refused_and_left_intact(self):
        self.db.register_tape("DEL2", 12000)
        snap = self._q(
            """INSERT INTO remote_snapshots (remote_host,remote_path,
                   fingerprint,total_files,total_bytes,created_at)
               VALUES ('h','/r','fp-del',1,1,now()) RETURNING snapshot_id"""
        )[0]["snapshot_id"]
        plan = self._q(
            """INSERT INTO remote_plans (snapshot_id,fingerprint,chunk_count,
                   created_at) VALUES (%s,'pf-del',1,now())
               RETURNING plan_id""", (snap,))[0]["plan_id"]
        self._q(
            """INSERT INTO remote_sessions (session_label,remote_host,
                   remote_user,remote_path,tape_label,staging_dir,total_files,
                   total_bytes,chunk_count,plan_id,created_at,status,
                   tape_generation)
               VALUES ('DELS','h','u','/r','DEL2','C:\\stg',1,1,1,%s,now(),
                       'active',1)""", (plan,))
        self._q(
            """INSERT INTO archive_runs (run_label,tape_label,session_kind,
                   started_at) VALUES ('del-run','DEL2','local',now())""")
        with self.assertRaisesRegex(RuntimeError,
                                    "Refusing to delete.*remote_sessions"):
            self.db.delete_tape("DEL2")
        self.assertTrue(self.db.tape_exists("DEL2"))
        # The refusal rolled back the catalog deletes as well.
        self.assertEqual(len(self._q(
            "SELECT 1 FROM archive_runs WHERE tape_label='DEL2'")), 1)
        self.assertEqual(self.db.get_active_tape_generation("DEL2"), 1)

    def test_a_tape_with_reset_history_is_refused(self):
        self.db.register_tape("DEL3", 12000)
        self.db.create_tape_reset_intent(
            operation_id="op-del", tape_label="DEL3", expected_drive="X:",
            expected_drive_serial="S", expected_ltfs_gen=1, expected_vcr=1,
            backup_path="b", backup_sha256=_sha(), restore_list_path="r",
            restore_list_sha256=_sha(), shadow_database="s",
            impact_report_path="i", impact_report_sha256=_sha(), impact={})
        with self.assertRaisesRegex(RuntimeError, "tape_reset_operations"):
            self.db.delete_tape("DEL3")
        self.assertTrue(self.db.tape_exists("DEL3"))


if __name__ == "__main__":
    unittest.main()
