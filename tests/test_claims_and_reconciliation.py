"""Plan 1 / Tasks 3.1 + 3.2 — chunk claims and startup reconciliation.

The single most dangerous thing either feature could do is decide, on a timer,
that an ambiguous tape write is safe to retry. So the tests are built around
that: every path that could plausibly reach a ``backing`` chunk is checked, and
every "we cannot tell" is checked to fail closed.

The second theme is that **elapsed time is not evidence**. A lapsed lease is
equally consistent with a worker wedged on an SSH call, so a claim is released
only when the previous owner is provably gone — and a recycled PID must not be
mistaken for the original worker.
"""
import inspect
import os
import tempfile
import unittest
from types import SimpleNamespace
from unittest import mock

from src import remote_orchestrator as ro
from src.exit_codes import ExitCode, REASON_AMBIGUOUS_BACKING_CHUNK
from src.startup_reconcile import (AMBIGUOUS_CHUNK_STATE, BLOCKED, LIVE,
                                   ProcessEvidence, RELEASABLE,
                                   StartupReconciler, classify_attempt,
                                   local_process_evidence,
                                   remote_process_evidence, remote_token_probe)


# =============================================================================
# A. The claim SQL can never reach a 'backing' chunk
# =============================================================================
class ClaimSqlSafetyTests(unittest.TestCase):
    """Structural, because the guarantee lives in the WHERE clauses."""

    def _source(self, name):
        from src.pg_sessions import PgSessionMixin
        return inspect.getsource(getattr(PgSessionMixin, name))

    def test_backing_is_not_a_reclaimable_state(self):
        from src.pg_sessions import PgSessionMixin
        self.assertNotIn(AMBIGUOUS_CHUNK_STATE,
                         PgSessionMixin.RECLAIMABLE_CHUNK_STATES)
        self.assertEqual(set(PgSessionMixin.RECLAIMABLE_CHUNK_STATES),
                         {"fetching", "packing"})

    def test_claiming_only_takes_a_re_drivable_chunk(self):
        source = self._source("claim_chunk_for_staging")
        self.assertIn("status IN ('pending','fetch_failed','backup_failed')",
                      source)
        self.assertNotIn("'backing'", source)

    def test_claiming_respects_a_live_lease(self):
        source = self._source("claim_chunk_for_staging")
        self.assertIn("owner_token IS NULL OR lease_expires_at <", source)

    def test_renewing_and_releasing_exclude_backing(self):
        for name in ("renew_chunk_claim", "release_chunk_claim"):
            self.assertIn("status <> 'backing'", self._source(name), name)

    def test_the_expiry_query_excludes_backing(self):
        source = self._source("list_expired_chunk_claims")
        self.assertIn("RECLAIMABLE_CHUNK_STATES", source)
        self.assertIn("_run_read", source)          # read-only
        for mutation in ("UPDATE", "DELETE", "INSERT"):
            self.assertNotIn(mutation, source, mutation)

    def test_reclaiming_requires_evidence_and_excludes_backing(self):
        source = self._source("reclaim_expired_chunk")
        self.assertIn("RECLAIMABLE_CHUNK_STATES", source)
        self.assertIn("if not evidence", source)

    def test_a_claim_without_migration_014_is_refused(self):
        source = self._source("claim_chunk_for_staging")
        self.assertIn("migration 014", source)
        self.assertIn("pretend a claim is enforced", source)


class ReclaimEvidenceTests(unittest.TestCase):
    class _Repo:
        def __init__(self):
            self.updates = []

        @staticmethod
        def _column_exists_conn(conn, table, column):
            return True

        def _transaction(self, operation, description):
            return operation(self)

        def execute(self, sql, params=()):
            self.updates.append((" ".join(sql.split()), params))
            return SimpleNamespace(rowcount=1)

    def _repo(self):
        from src.pg_sessions import PgSessionMixin

        class Repo(ReclaimEvidenceTests._Repo, PgSessionMixin):
            pass
        return Repo()

    def test_reclaiming_without_evidence_raises(self):
        repo = self._repo()
        for empty in ("", None, []):
            with self.assertRaises(RuntimeError) as caught:
                repo.reclaim_expired_chunk(37, 4, "owner", empty)
            self.assertIn("not proof", str(caught.exception))
        self.assertEqual(repo.updates, [])

    def test_reclaiming_with_evidence_records_it(self):
        repo = self._repo()
        with mock.patch("src.pg_sessions.get_logger"):
            self.assertTrue(repo.reclaim_expired_chunk(
                37, 4, "owner", "local PID 1234 no longer exists"))
        sql, params = repo.updates[0]
        self.assertIn("error_msg=%s", sql)
        self.assertIn("reclaimed after abandonment", params[0])


# =============================================================================
# B. Local process evidence — a recycled PID is not the original worker
# =============================================================================
class _FakeProcess:
    def __init__(self, created):
        self._created = created

    def create_time(self):
        return self._created


class _FakePsutil:
    class NoSuchProcess(Exception):
        pass

    def __init__(self, processes=None, raises=None):
        self._processes = processes or {}
        self._raises = raises

    def Process(self, pid):                      # noqa: N802 - psutil's name
        if self._raises is not None:
            raise self._raises
        if pid not in self._processes:
            raise _FakePsutil.NoSuchProcess(pid)
        return _FakeProcess(self._processes[pid])


class LocalEvidenceTests(unittest.TestCase):
    def _evidence(self, pid=1234, started=1_700_000_000.0, token=None):
        return ProcessEvidence(attempt_id="a1", owner_token="o1",
                               local_pid=pid, local_process_started_at=started,
                               remote_command_token=token)

    def test_an_absent_pid_is_releasable(self):
        evidence = self._evidence()
        verdict = local_process_evidence(evidence, _FakePsutil(processes={}))
        self.assertEqual(verdict, RELEASABLE)
        self.assertIn("no longer exists", evidence.reasons[0])

    def test_the_same_pid_and_creation_time_is_live(self):
        evidence = self._evidence()
        verdict = local_process_evidence(
            evidence, _FakePsutil({1234: 1_700_000_000.0}))
        self.assertEqual(verdict, LIVE)

    def test_a_recycled_pid_is_releasable_but_only_after_checking(self):
        """THE reason creation time is recorded at all."""
        evidence = self._evidence()
        verdict = local_process_evidence(
            evidence, _FakePsutil({1234: 1_700_009_999.0}))
        self.assertEqual(verdict, RELEASABLE)
        self.assertIn("recycled", evidence.reasons[0])

    def test_small_clock_skew_still_counts_as_the_same_process(self):
        evidence = self._evidence()
        verdict = local_process_evidence(
            evidence, _FakePsutil({1234: 1_700_000_001.0}))
        self.assertEqual(verdict, LIVE)

    def test_a_missing_creation_time_is_blocked_not_assumed(self):
        evidence = self._evidence(started=None)
        verdict = local_process_evidence(
            evidence, _FakePsutil({1234: 1_700_000_000.0}))
        self.assertEqual(verdict, BLOCKED)
        self.assertIn("recycled PID", evidence.reasons[0])

    def test_no_recorded_pid_is_blocked(self):
        evidence = self._evidence(pid=None)
        self.assertEqual(local_process_evidence(evidence, _FakePsutil()),
                         BLOCKED)

    def test_an_inspection_failure_is_blocked(self):
        evidence = self._evidence()
        verdict = local_process_evidence(
            evidence, _FakePsutil(raises=PermissionError("access denied")))
        self.assertEqual(verdict, BLOCKED)

    def test_a_datetime_creation_time_is_compared_correctly(self):
        from datetime import datetime, timezone
        recorded = datetime.fromtimestamp(1_700_000_000.0, tz=timezone.utc)
        evidence = ProcessEvidence(local_pid=1234,
                                   local_process_started_at=recorded)
        self.assertEqual(
            local_process_evidence(evidence,
                                   _FakePsutil({1234: 1_700_000_000.0})),
            LIVE)


# =============================================================================
# C. Remote process evidence — unreachable means blocked
# =============================================================================
class RemoteEvidenceTests(unittest.TestCase):
    def _evidence(self, token="tok-abc"):
        return ProcessEvidence(local_pid=1, remote_command_token=token)

    def test_no_token_means_nothing_remote_to_prove(self):
        self.assertEqual(
            remote_process_evidence(self._evidence(token=None)), RELEASABLE)

    def test_a_running_group_is_live(self):
        self.assertEqual(
            remote_process_evidence(self._evidence(),
                                    remote_probe=lambda t: True), LIVE)

    def test_a_provably_absent_group_is_releasable(self):
        self.assertEqual(
            remote_process_evidence(self._evidence(),
                                    remote_probe=lambda t: False), RELEASABLE)

    def test_an_unreachable_host_is_blocked(self):
        evidence = self._evidence()
        self.assertEqual(
            remote_process_evidence(evidence, remote_probe=lambda t: None),
            BLOCKED)
        self.assertIn("could not be reached", evidence.reasons[0])

    def test_a_failing_probe_is_blocked(self):
        def boom(token):
            raise OSError("connection reset")
        self.assertEqual(
            remote_process_evidence(self._evidence(), remote_probe=boom),
            BLOCKED)

    def test_no_probe_supplied_is_blocked(self):
        self.assertEqual(remote_process_evidence(self._evidence()), BLOCKED)

    def test_the_probe_is_read_only(self):
        source = inspect.getsource(remote_token_probe)
        self.assertIn("pgrep", source)
        for destructive in ("pkill", "kill ", "rm ", "> ", "tar "):
            self.assertNotIn(destructive, source, destructive)

    def test_the_probe_maps_ssh_failures_to_uncertain(self):
        results = {255: None, 124: None}
        for returncode, expected in results.items():
            with mock.patch("src.remote_transport._ssh_run",
                            return_value=SimpleNamespace(
                                stdout="", stderr="", returncode=returncode)):
                probe = remote_token_probe("u", "h")
                self.assertIs(probe("tok"), expected, str(returncode))


# =============================================================================
# D. Combining evidence — both sides must agree
# =============================================================================
class CombinedVerdictTests(unittest.TestCase):
    def _attempt(self, pid=1234, started=1_700_000_000.0, token=None):
        return {"attempt_id": "a1", "owner_token": "o1", "local_pid": pid,
                "local_process_started_at": started,
                "remote_command_token": token}

    def test_both_gone_is_releasable(self):
        evidence = classify_attempt(
            self._attempt(token="tok"), psutil_module=_FakePsutil({}),
            remote_probe=lambda t: False)
        self.assertEqual(evidence.verdict, RELEASABLE)

    def test_a_live_local_process_wins(self):
        evidence = classify_attempt(
            self._attempt(token="tok"),
            psutil_module=_FakePsutil({1234: 1_700_000_000.0}),
            remote_probe=lambda t: False)
        self.assertEqual(evidence.verdict, LIVE)

    def test_a_live_remote_group_wins(self):
        evidence = classify_attempt(
            self._attempt(token="tok"), psutil_module=_FakePsutil({}),
            remote_probe=lambda t: True)
        self.assertEqual(evidence.verdict, LIVE)

    def test_an_unreachable_remote_blocks_even_when_the_pid_is_gone(self):
        evidence = classify_attempt(
            self._attempt(token="tok"), psutil_module=_FakePsutil({}),
            remote_probe=lambda t: None)
        self.assertEqual(evidence.verdict, BLOCKED)


# =============================================================================
# E. The reconciler
# =============================================================================
class ReconcilerDB:
    def __init__(self, expired=(), attempts=(), backing=()):
        self.expired = [dict(row) for row in expired]
        self.attempts = [dict(row) for row in attempts]
        self.backing = list(backing)
        self.reclaimed = []
        self.closed = []

    def get_chunks_with_status(self, session_id, status):
        return list(self.backing) if status == "backing" else []

    def list_expired_chunk_claims(self, session_id):
        return list(self.expired)

    def list_live_worker_attempts(self, session_id=None):
        return list(self.attempts)

    def reclaim_expired_chunk(self, session_id, chunk_index, owner_token,
                              evidence):
        self.reclaimed.append((chunk_index, owner_token, evidence))
        self.expired = [c for c in self.expired
                        if c["chunk_index"] != chunk_index]
        return True

    def finish_worker_attempt(self, attempt_id, terminal_state):
        self.closed.append((attempt_id, terminal_state))
        return True


class ReconcilerTests(unittest.TestCase):
    def _claim(self, chunk_index=4, status="fetching", attempt_id="a1"):
        return {"chunk_index": chunk_index, "status": status,
                "owner_token": "o1", "attempt_id": attempt_id}

    def _attempt(self, attempt_id="a1", pid=1234, started=1_700_000_000.0,
                 token=None):
        return {"attempt_id": attempt_id, "owner_token": "o1",
                "local_pid": pid, "local_process_started_at": started,
                "remote_command_token": token}

    def test_a_dead_worker_is_released(self):
        db = ReconcilerDB(expired=[self._claim()], attempts=[self._attempt()])
        reconciler = StartupReconciler(db, 37, psutil_module=_FakePsutil({}))
        report = reconciler.apply()
        self.assertEqual(report["released"], [4])
        self.assertEqual(db.reclaimed[0][0], 4)
        self.assertIn("no longer exists", db.reclaimed[0][2])
        self.assertEqual(db.closed, [("a1", "orphan_terminated")])

    def test_a_live_worker_is_left_alone(self):
        db = ReconcilerDB(expired=[self._claim()], attempts=[self._attempt()])
        reconciler = StartupReconciler(
            db, 37, psutil_module=_FakePsutil({1234: 1_700_000_000.0}))
        with mock.patch("src.startup_reconcile.get_logger"):
            report = reconciler.apply()
        self.assertEqual(report["released"], [])
        self.assertEqual(db.reclaimed, [])
        self.assertEqual(report["blocked"][0]["evidence"]["verdict"], LIVE)

    def test_a_claim_with_no_recorded_attempt_is_blocked(self):
        db = ReconcilerDB(expired=[self._claim(attempt_id="missing")],
                          attempts=[])
        reconciler = StartupReconciler(db, 37, psutil_module=_FakePsutil({}))
        with mock.patch("src.startup_reconcile.get_logger"):
            report = reconciler.apply()
        self.assertEqual(report["released"], [])
        self.assertIn("cannot be identified",
                      report["blocked"][0]["evidence"]["reasons"][0])

    def test_a_backing_chunk_is_never_a_candidate(self):
        """Even if a caller hands one in, it is skipped."""
        db = ReconcilerDB(
            expired=[self._claim(chunk_index=9, status="backing")],
            attempts=[self._attempt()], backing=[9])
        reconciler = StartupReconciler(db, 37, psutil_module=_FakePsutil({}))
        report = reconciler.apply()
        self.assertEqual(report["released"], [])
        self.assertEqual(db.reclaimed, [])
        self.assertEqual(report["decisions"], [])
        # ...and it IS reported, so an operator sees it.
        self.assertEqual(report["ambiguous_chunks"], [9])

    def test_planning_changes_nothing(self):
        db = ReconcilerDB(expired=[self._claim()], attempts=[self._attempt()])
        StartupReconciler(db, 37, psutil_module=_FakePsutil({})).plan()
        self.assertEqual(db.reclaimed, [])
        self.assertEqual(db.closed, [])

    def test_reconciliation_is_idempotent(self):
        db = ReconcilerDB(expired=[self._claim()], attempts=[self._attempt()])
        reconciler = StartupReconciler(db, 37, psutil_module=_FakePsutil({}))
        first = reconciler.apply()
        second = reconciler.apply()
        self.assertEqual(first["released"], [4])
        self.assertEqual(second["released"], [])

    def test_a_database_read_failure_is_reported_not_swallowed(self):
        class Broken(ReconcilerDB):
            def list_expired_chunk_claims(self, session_id):
                raise RuntimeError("connection lost")

        report = StartupReconciler(Broken(), 37,
                                   psutil_module=_FakePsutil({})).plan()
        self.assertTrue(any("expired_claims" in e for e in report["errors"]))
        self.assertEqual(report["decisions"], [])

    def test_orphan_part_files_are_inventoried_not_deleted(self):
        from src.archive_artifacts import JsonlZstArtifactWriter, segment_locator
        with tempfile.TemporaryDirectory() as root:
            writer = JsonlZstArtifactWriter(root, segment_locator(37, 1, 0))
            writer.open()
            writer.add(path="/strg/a/f", size=1, ordinal=0)
            writer.close(publish=False)

            report = StartupReconciler(ReconcilerDB(), 37,
                                       archive_root=root).inventory()
            self.assertEqual(report["orphan_parts"], [writer.part_path])
            self.assertTrue(os.path.exists(writer.part_path))

    def test_the_reconciler_never_touches_ltfs(self):
        source = inspect.getsource(
            __import__("src.startup_reconcile", fromlist=["x"]))
        for token in ("lto_drive", "_acquire_tape_io_lock", "get_volume_label",
                      "_ensure_lto_drive_ready", "LtfsCmd", "robocopy"):
            self.assertNotIn(token, source, token)


# =============================================================================
# F. The backing probes now fail closed
# =============================================================================
class BackingProbeFailClosedTests(unittest.TestCase):
    def _orch(self, exc):
        orch = ro.RemoteOrchestrator.__new__(ro.RemoteOrchestrator)
        orch.notifier = None
        orch.db = SimpleNamespace(
            get_chunks_with_status=mock.Mock(side_effect=exc))
        return orch

    def test_an_unreadable_status_is_treated_as_ambiguous(self):
        """Was 'treat as clear', which risks a double write."""
        orch = self._orch(RuntimeError("connection lost"))
        with mock.patch("src.remote_orchestrator.get_logger"):
            self.assertTrue(orch._chunk_backing_from_prior_run(37, 4))

    def test_an_unreadable_scan_blocks_the_run(self):
        orch = self._orch(RuntimeError("connection lost"))
        with mock.patch("builtins.print"), \
                mock.patch.object(ro, "send_best_effort", lambda *a, **k: None):
            block = orch._detect_prior_backing_chunks(37)
        self.assertIsNotNone(block)
        self.assertEqual(block.exit_code, ExitCode.SAFETY_BLOCK)
        self.assertEqual(block.reason, REASON_AMBIGUOUS_BACKING_CHUNK)
        self.assertFalse(block.resumable)

    def test_a_readable_clear_session_still_proceeds(self):
        orch = ro.RemoteOrchestrator.__new__(ro.RemoteOrchestrator)
        orch.notifier = None
        orch.db = SimpleNamespace(
            get_chunks_with_status=lambda sid, status: [])
        self.assertIsNone(orch._detect_prior_backing_chunks(37))
        self.assertFalse(orch._chunk_backing_from_prior_run(37, 4))


if __name__ == "__main__":
    unittest.main()
