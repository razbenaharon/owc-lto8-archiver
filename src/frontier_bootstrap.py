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

A failed bootstrap leaves the legacy scanner available and coverage non-final,
so an abandoned attempt costs nothing but the traversal.
"""
import os

from .logsetup import get_logger
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
                 max_directories=None):
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
        if report["session_report"] is not None:
            for reason in report["session_report"].get("blocking", []):
                # A shared plan or an ambiguous chunk must be resolved by a
                # human before a migration, not during one.
                report["blocking"].append(f"session state: {reason}")

        report["would_proceed"] = not report["blocking"]
        return report

    def _session_report(self):
        from .startup_reconcile import session_frontier_report
        try:
            return session_frontier_report(
                self.db, self.session_id, archive_root=self.archive_root,
                lock_holders=[], active_processes=[])
        except Exception as exc:
            get_logger().warning("bootstrap session report failed: %s", exc)
            return None

    # ------------------------------------------------------------------
    # Execute — explicit, transactional, resumable
    # ------------------------------------------------------------------
    def execute(self, approved=False):
        """Perform the migration. Requires explicit approval.

        Resumable: the run record and the directory frontier both persist, so
        an interrupted bootstrap continues from the directories it had already
        listed rather than starting over.
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
