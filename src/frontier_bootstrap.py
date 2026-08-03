"""One-time migration of a legacy session onto the incremental frontier.

Plan 1, Task 4.2. A session that was scanned by whole-root replay already has
millions of ``remote_snapshot_files`` rows and a plan whose chunks may already
be on tape. Turning the frontier on for it is not a switch — it is a migration,
and this module is the only supported way to perform it.

The rule that shapes everything here:

    **Existing path rows are not traversal evidence.**

It is tempting to derive coverage from the catalog: "we have rows for every file
under ``/vault/a``, therefore ``/vault/a`` is covered". That is exactly the wrong
inference. The rows say what a previous scan *found*, not what the directory
*contains* — a file added since, or one the old scan silently skipped on a
permission error, is invisible in them. A directory becomes ``complete`` only
after this bootstrap has actually listed it.

What the bootstrap therefore does, in order:

1. Create persisted scope rows from the configured roots (rejecting overlap).
2. Traverse the source **read-only**, one directory at a time, publishing a
   segment artifact per directory. This is the traversal evidence.
3. Reconcile each segment against the existing snapshot **once**:
   * same path, same size  -> covered; never appended again
   * not present           -> new; available for future chunks
   * same path, DIFFERENT size -> ``source_changed``: recorded as unresolved,
     the existing sealed membership is retained, and nothing is replanned
4. Leave coverage provisional wherever anything is unresolved.

Existing sealed chunk membership always wins. This module never rewrites a
chunk, never changes the chunk format, and never touches LTFS.

A failed bootstrap leaves coverage non-final and must be retried or reconciled;
it never makes a legacy scanner available to production.
"""
import os

from .logsetup import get_logger
from .pipeline_types import ChunkStatus
from .scan_frontier import (DirectoryFrontierCoordinator, canonicalize_scopes,
                            reconcile_scope_order)

#: Bootstrap run states, mirroring the schema's CHECK constraint.
STATE_RUNNING = "running"
STATE_COMPLETED = "completed"
STATE_FAILED = "failed"
STATE_ABANDONED = "abandoned"


class BootstrapRefused(RuntimeError):
    """The session is not in a state a bootstrap may start from."""


class FrontierBootstrap:
    """Plan and (on explicit approval) perform the one-time migration."""

    def __init__(self, *, db, session_id, scan_paths, archive_root,
                 scanner_factory, stop_event, source_host=None, ui=None,
                 max_directories=None, active_processes_probe=None,
                 lock_holders_probe=None):
        #: Liveness probes. Injected so tests can drive them, but NEVER
        #: defaulted to "nothing is running" — see :meth:`_session_report`.
        self.active_processes_probe = active_processes_probe
        self.lock_holders_probe = lock_holders_probe
        self.db = db
        self.session_id = session_id
        self.scan_paths = list(scan_paths)
        self.archive_root = archive_root
        self.scanner_factory = scanner_factory
        self.stop_event = stop_event
        self.source_host = source_host
        self.ui = ui
        #: Bound for a rehearsal; ``None`` means traverse everything.
        self.max_directories = max_directories

    # ------------------------------------------------------------------
    # Dry run — changes nothing
    # ------------------------------------------------------------------
    def dry_run(self):
        """What the bootstrap WOULD do, and whether it may run at all.

        Read-only. It validates the scope configuration, checks the session is
        quiescent enough to migrate, and reports the existing state — without
        creating a scope row, listing a directory or writing an artifact.
        """
        report = {
            "session_id": self.session_id,
            "would_proceed": False,
            "blocking": [],
            "scopes": [],
            "existing_bootstrap": None,
            "frontier_already_bound": None,
            "session_report": None,
        }
        try:
            report["scopes"] = canonicalize_scopes(self.scan_paths)
        except Exception as exc:
            report["blocking"].append(f"scope configuration: {exc}")
            return report

        try:
            persisted = [row["source_root"]
                         for row in self.db.get_scan_scopes(self.session_id)]
            reconcile_scope_order(report["scopes"], persisted, ui=self.ui)
        except Exception as exc:
            report["blocking"].append(f"scope reconciliation: {exc}")

        for name, call, key in (
            ("frontier state",
             lambda: self.db.session_has_frontier_state(self.session_id),
             "frontier_already_bound"),
            ("existing bootstrap",
             lambda: self.db.get_frontier_bootstrap(self.session_id),
             "existing_bootstrap"),
        ):
            try:
                report[key] = call()
            except Exception as exc:
                report["blocking"].append(f"{name}: {exc}")

        existing = report["existing_bootstrap"]
        if existing and existing.get("state") == STATE_COMPLETED:
            report["blocking"].append(
                "this session has already been bootstrapped; a second "
                "bootstrap would re-traverse a source the frontier already "
                "describes")

        report["session_report"] = self._session_report()
        report["blocking"].extend(
            self._bootstrap_blockers(report["session_report"]))

        report["would_proceed"] = not report["blocking"]
        return report

    @staticmethod
    def _bootstrap_blockers(session_report):
        """What must stop a bootstrap, derived from the report's FACTS.

        This deliberately does **not** adopt every blocking reason the
        read-only session report raises. The two answer different questions:

        * ``session_frontier_report`` answers "is this session finished, and is
          it safe to treat it as done?" — for which an unfinished scan is
          correctly blocking;
        * a bootstrap asks "can I safely establish a conservative frontier for
          this session?" — for which **an unfinished scan is the expected
          input**, not an obstacle. Refusing it there made the bootstrap
          unable to do the one job Task 4.2 defines for it: migrate an
          incomplete historical scan.

        So ``scan_complete = false`` is allowed through, and everything it
        implies is handled by initialising the frontier as pending/partial and
        never marking the scan complete. What still blocks is anything that
        makes the session's *current* state unsafe to build on. Each is read as
        a structured fact rather than by matching the report's prose, so a
        reworded message can never silently disable a gate.
        """
        AMBIGUOUS_CHUNK_STATE = ChunkStatus.BACKING.value
        blockers = []
        if session_report is None:
            return ["the session report could not be produced, so the "
                    "session's state is unknown"]

        for problem in session_report.get("errors", []):
            blockers.append(f"session state could not be read: {problem}")

        transient = session_report.get("transient_chunks") or {}
        ambiguous = transient.get(AMBIGUOUS_CHUNK_STATE)
        if ambiguous:
            blockers.append(
                f"{ambiguous} chunk(s) are '{AMBIGUOUS_CHUNK_STATE}': a tape "
                "write began and its on-tape outcome cannot be known from the "
                "catalog alone (incident 010). A human must compare the tape "
                "against the catalog first.")
        mid_flight = {k: v for k, v in transient.items()
                      if k != AMBIGUOUS_CHUNK_STATE}
        if mid_flight:
            blockers.append(
                f"chunks are mid-flight ({mid_flight}); a worker may still be "
                "running, and a bootstrap must observe a still session")

        shared = session_report.get("shared_plan_sessions") or []
        if shared:
            blockers.append(
                f"this session shares its plan with {shared}; migrating one of "
                "them would change what the others see")

        liveness = session_report.get("liveness") or {}
        if liveness.get("lock_holders"):
            blockers.append(
                f"the archiver lock is held by {liveness['lock_holders']}")
        if liveness.get("active_processes"):
            blockers.append(
                f"archive processes are running: {liveness['active_processes']}")

        orphans = (session_report.get("artifacts") or {}).get("orphan_parts")
        if orphans:
            blockers.append(
                f"{len(orphans)} orphaned .part artifact(s) exist; reconcile "
                "them before publishing new segments")
        return blockers

    def _session_report(self):
        """The session report, with REAL liveness evidence.

        This used to pass ``lock_holders=[]`` and ``active_processes=[]`` —
        hard-coded emptiness, which told the report "nothing is running"
        without looking. Every liveness gate downstream was therefore vacuous:
        a bootstrap could have been approved while an archiver held the lock.
        The evidence is now measured, and a probe that cannot answer returns
        the string it failed with, so the gate blocks rather than assuming
        quiet.
        """
        from .startup_reconcile import session_frontier_report
        try:
            return session_frontier_report(
                self.db, self.session_id, archive_root=self.archive_root,
                lock_holders=self._lock_holders(),
                active_processes=self._active_processes())
        except Exception as exc:
            get_logger().warning("bootstrap session report failed: %s", exc)
            return None

    def _active_processes(self):
        """Archive/transfer processes on this host, or a blocking marker."""
        if self.active_processes_probe is not None:
            probe = self.active_processes_probe
        else:
            from .local_manifest_archive import active_archive_processes
            probe = active_archive_processes
        try:
            return probe()
        except Exception as exc:
            return [{"probe_failed": str(exc)}]

    def _lock_holders(self):
        """Archiver advisory-lock holders, or a blocking marker."""
        if self.lock_holders_probe is None:
            # No conninfo to probe with. Unknown must not read as "nobody".
            return [{"probe_unavailable":
                     "no archiver-lock probe was supplied to the bootstrap"}]
        try:
            return self.lock_holders_probe()
        except Exception as exc:
            return [{"probe_failed": str(exc)}]

    # ------------------------------------------------------------------
    # Execute — explicit, transactional, resumable
    # ------------------------------------------------------------------
    def execute(self, approved=False, conservative=False):
        """Perform the migration. Requires explicit approval.

        Resumable: the run record and the directory frontier both persist, so
        an interrupted bootstrap continues from the directories it had already
        listed rather than starting over.

        ``conservative=True`` performs the **structure-only** bootstrap: it
        creates the scope rows and enqueues each configured root as a
        ``pending`` directory, and stops there. See
        :meth:`execute_conservative` for why that is the right shape for a
        session whose historical scan never finished.
        """
        if not approved:
            raise BootstrapRefused(
                "the frontier bootstrap must be explicitly approved; it "
                "traverses the whole source and creates persistent scope, "
                "directory and segment state")
        plan = self.dry_run()
        if not plan["would_proceed"]:
            raise BootstrapRefused(
                "refusing to bootstrap: " + "; ".join(plan["blocking"]))

        if conservative:
            return self.execute_conservative(plan)

        run = self.db.start_frontier_bootstrap(self.session_id,
                                               source_host=self.source_host)
        bootstrap_id = run["bootstrap_id"]
        coordinator = DirectoryFrontierCoordinator(
            db=self.db, session_id=self.session_id,
            scan_paths=self.scan_paths, archive_root=self.archive_root,
            scanner_factory=self.scanner_factory, stop_event=self.stop_event,
            ui=self.ui, max_directories=self.max_directories)
        try:
            coordinator.run()
        except Exception as exc:
            self._record(bootstrap_id, coordinator,
                         state=STATE_FAILED, detail=str(exc))
            get_logger().exception("frontier_bootstrap_failed: %s",
                                   bootstrap_id)
            raise

        imported = self._import_segments()
        final = self._coverage_is_final()
        self._record(bootstrap_id, coordinator,
                     state=STATE_COMPLETED if final else STATE_RUNNING,
                     coverage_final=final, **imported)
        result = dict(plan)
        result.update({
            "bootstrap_id": bootstrap_id,
            "directories_listed": coordinator.directories_listed,
            "segments_published": coordinator.segments_published,
            "coverage_final": final,
            **imported,
        })
        return result

    def execute_conservative(self, plan=None):
        """Structure-only bootstrap: scopes and pending roots, nothing else.

        This is the correct migration for a session whose historical scan never
        finished, and it is deliberately the *smallest* thing that can be true.

        What it writes
        --------------
        Scope rows for the configured roots, and one ``pending`` directory row
        per directory scope — its own root. That is all. It lists no directory,
        publishes no segment, imports no membership and finalizes nothing.

        Why not traverse here
        ---------------------
        A full traversal is a multi-hour SSH walk of the whole source, and it
        would be doing the *scanner's* job during a migration window. Worse, it
        would have to decide coverage for directories the historical scan may or
        may not have reached — and any such decision made now is a guess. By
        leaving every root ``pending`` the next approved run simply scans, and
        the frontier's own rules decide coverage from real traversal evidence.

        Why this cannot lose or duplicate work
        --------------------------------------
        * **Nothing is skipped.** Every root starts ``pending``, so the whole
          configured source is queued for exploration. There is no directory
          this bootstrap could mark covered without having looked at it,
          because it marks nothing covered.
        * **Nothing is duplicated.** When the next run lists a directory and
          publishes its segment, ``import_legacy_scan_segment`` reconciles that
          segment ONCE against the existing 23M snapshot rows — path AND size —
          and only the genuinely ``new`` entries reach the chunk builder. Files
          already planned by the historical scan are recorded as ``covered``
          and never appended again.
        * **Existing work is untouched.** No chunk, ordinal, status, membership
          row, ZIP container or tape locator is read for writing here.
        * **The scan is never marked complete.** ``finalize()`` is not called,
          and the session's ``scan_complete`` flag is not written.

        Idempotence
        -----------
        Provably identical rather than refused. ``establish_scopes()`` creates
        scopes only when none are persisted and reconciles the order otherwise,
        and enqueueing a root that already exists is a no-op on the
        ``(scope, path)`` unique constraint. A second conservative run over an
        unchanged configuration therefore writes nothing new. A *changed* root
        set is refused by ``reconcile_scope_order`` rather than silently
        re-shaped.
        """
        plan = plan if plan is not None else self.dry_run()
        run = self.db.start_frontier_bootstrap(self.session_id,
                                               source_host=self.source_host)
        bootstrap_id = run["bootstrap_id"]
        coordinator = DirectoryFrontierCoordinator(
            db=self.db, session_id=self.session_id,
            scan_paths=self.scan_paths, archive_root=self.archive_root,
            scanner_factory=self.scanner_factory, stop_event=self.stop_event,
            ui=self.ui, max_directories=0)
        try:
            scopes = coordinator.establish_scopes()
        except Exception as exc:
            self._record(bootstrap_id, coordinator,
                         state=STATE_FAILED, detail=str(exc))
            get_logger().exception("conservative_bootstrap_failed: %s",
                                   bootstrap_id)
            raise

        # RUNNING, never COMPLETED: coverage is not final and the source has
        # not been traversed. Recording it completed would be the same lie as
        # inferring coverage from catalog rows.
        self._record(bootstrap_id, coordinator, state=STATE_RUNNING,
                     coverage_final=False, segments_imported=0,
                     entries_covered=0, entries_new=0, entries_changed=0,
                     detail="conservative structure-only bootstrap: scopes "
                            "created, roots queued pending, no traversal")
        result = dict(plan)
        result.update({
            "bootstrap_id": bootstrap_id,
            "mode": "conservative",
            "scopes_established": len(scopes),
            "directories_listed": 0,
            "segments_published": 0,
            "coverage_final": False,
            "scan_marked_complete": False,
            "segments_imported": 0, "entries_covered": 0,
            "entries_new": 0, "entries_changed": 0,
        })
        return result

    def _import_segments(self):
        """Reconcile every ready segment against the legacy snapshot, once."""
        covered = new = changed = 0
        imported = 0
        from .archive_artifacts import parse_jsonl_zst_artifact

        for segment in self.db.get_ready_segments(self.session_id, limit=10000):
            try:
                _header, entries, _totals = parse_jsonl_zst_artifact(
                    self.archive_root, segment["locator"])
            except Exception as exc:
                get_logger().error("bootstrap could not read segment %s: %s",
                                   segment.get("scan_segment_id"), exc)
                continue
            outcome = self.db.import_legacy_scan_segment(
                self.session_id, segment["scan_segment_id"],
                [(e["path"], int(e["size"])) for e in entries])
            if outcome.get("already_imported"):
                continue
            imported += 1
            covered += len(outcome["covered"])
            new += len(outcome["new"])
            changed += len(outcome["source_changed"])
        return {"segments_imported": imported, "entries_covered": covered,
                "entries_new": new, "entries_changed": changed}

    def _coverage_is_final(self):
        """True only when every scope reports final coverage."""
        try:
            scopes = self.db.get_scan_scopes(self.session_id)
        except Exception:
            return False
        return bool(scopes) and all(
            scope.get("coverage_state") == "final" for scope in scopes)

    def _record(self, bootstrap_id, coordinator, **fields):
        payload = {
            "scopes_created": len(self.scan_paths),
            "directories_listed": coordinator.directories_listed,
            "segments_published": coordinator.segments_published,
        }
        payload.update(fields)
        try:
            self.db.update_frontier_bootstrap(bootstrap_id, **payload)
        except Exception:
            get_logger().warning("could not record bootstrap progress",
                                 exc_info=True)


__all__ = ["BootstrapRefused", "FrontierBootstrap", "STATE_ABANDONED",
           "STATE_COMPLETED", "STATE_FAILED", "STATE_RUNNING"]
