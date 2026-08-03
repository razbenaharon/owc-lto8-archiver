"""Scan-model selection and (from Task 1.1) scan/frontier coordination.

Task 0.3 records the target decision as code rather than prose:

**Adopt the persistent incremental directory frontier.** Task 0.2's harness
measures the three candidates and the frontier is the only one that removes
completed-directory replay *while keeping* the scanner/stager/writer overlap.
Full-scan-before-processing is retained only as an offline diagnostic in
``scripts/benchmark_scan_models.py``; it is not a production prerequisite,
because nothing about a tape write requires a complete inventory first — chunks
are sealed and written incrementally today and that must not regress.

The rationale that matters is a *safety* one, and it is asserted by tests, not
argued here: today's whole-root replay is recovery **by re-walking every file
already visited**. No tape invariant requires that. A persisted frontier can
publish chunks before global scan completion exactly as the current scanner
does, and it bounds a crash to the single directory that was mid-listing.

Activation is deliberately hard
-------------------------------
``decide_scan_mode`` is the single gate. It fails **towards the legacy
scanner** on every kind of uncertainty — flag off, migration 014 absent,
constraints not finalized, database that cannot answer — and it fails
**closed** (blocked, no scanner at all) for a session already bound to frontier
state, because silently returning such a session to root replay would re-walk a
source whose frontier rows say it was already covered.

Two scanners must never run against one active frontier. That is the whole
reason this returns a three-valued decision instead of a boolean.
"""
import os
import posixpath
import time
import uuid
from dataclasses import dataclass
from typing import Optional

from .archive_artifacts import (ArtifactConflict, ArtifactError,
                                JsonlZstArtifactWriter,
                                parse_jsonl_zst_artifact, resolve_locator,
                                segment_locator)
from .logsetup import get_logger
from .planning import StreamingChunkBuilder
from .paths import remote_path_is_legacy_safe
from .pipeline_types import SCOPE_KIND_DIRECTORY, SCOPE_KIND_FILE
from .runtime import CANCEL, _status
from .scanning import DirectoryFrontierScanner, StreamingRemoteScanner

#: The legacy whole-root ``find`` scanner (``StreamingRemoteScanner``).
MODE_LEGACY = "legacy"
#: The persistent incremental directory frontier.
MODE_FRONTIER = "frontier"
#: No scanner may run: the state is ambiguous and needs an operator.
MODE_BLOCKED = "blocked"


@dataclass(frozen=True)
class ScanModeDecision:
    """Which scanner may run, and the reason — always both."""

    mode: str
    reason: str
    #: True only for ``MODE_FRONTIER``; convenience for call sites.
    frontier_enabled: bool = False
    #: True only for ``MODE_BLOCKED``; the run must stop before scanning.
    blocked: bool = False
    detail: Optional[str] = None

    def __post_init__(self):
        object.__setattr__(self, "frontier_enabled", self.mode == MODE_FRONTIER)
        object.__setattr__(self, "blocked", self.mode == MODE_BLOCKED)


def _legacy(reason, detail=None):
    return ScanModeDecision(MODE_LEGACY, reason, detail=detail)


def _blocked(reason, detail=None):
    return ScanModeDecision(MODE_BLOCKED, reason, detail=detail)


def incremental_scan_schema_ready(db):
    """``(ready, reason)`` — can the frontier schema be relied on?

    Requires **both** halves of migration 014: the base tables/columns, and the
    separately-applied finalized constraints (the legacy membership audit plus
    the unique ``(plan_id, chunk_index, ordinal)`` index). A database that
    cannot answer either question is treated as not ready — never as ready.

    Task 2.1 implements the two predicates on ``PgConnectionCore``. Until then
    they are absent, so this correctly reports "not ready" and the frontier
    cannot activate, which is what every Plan 1 gate before the rehearsal
    expects.
    """
    for probe, missing_reason in (
        ("incremental_scan_schema_installed", "migration_014_not_installed"),
        ("incremental_scan_schema_finalized", "migration_014_not_finalized"),
    ):
        method = getattr(db, probe, None)
        if method is None:
            return False, missing_reason
        try:
            if not method():
                return False, missing_reason
        except Exception as exc:                # a DB that cannot answer
            get_logger().warning(
                "incremental_scan_schema_probe_failed: %s: %s", probe, exc)
            return False, "schema_state_indeterminate"
    return True, "schema_ready"


def decide_scan_mode(cfg, db, *, session_bound_to_frontier=False):
    """Choose the scanner for this run. The ONLY activation gate.

    ``session_bound_to_frontier`` must be true when the session has already
    published frontier state (scope/directory/segment rows or a ready
    artifact). Such a session can never be handed back to the legacy scanner:
    if the frontier cannot be used, the run is blocked for an operator instead.

    Returns a :class:`ScanModeDecision`; callers must honour ``blocked``.
    """
    enabled = bool(getattr(cfg, "incremental_scan_enabled", False))
    ready, schema_reason = incremental_scan_schema_ready(db)

    if enabled and ready:
        return ScanModeDecision(MODE_FRONTIER, "enabled_and_schema_ready")

    if session_bound_to_frontier:
        # Never mix scanners against one frontier, and never silently re-walk a
        # source the frontier rows already describe as covered.
        detail = ("this session has already published incremental-scan state, "
                  "so it cannot fall back to whole-root replay. "
                  + ("Re-enable [REMOTE] incremental_scan."
                     if not enabled else
                     f"Resolve the schema state first ({schema_reason})."))
        get_logger().error("scan_mode_blocked: enabled=%s schema=%s",
                           enabled, schema_reason)
        return _blocked(
            "frontier_session_cannot_use_legacy_scanner", detail=detail)

    if not enabled:
        return _legacy("disabled_by_config")
    get_logger().info("scan_mode_legacy: incremental scan requested but %s",
                      schema_reason)
    return _legacy(schema_reason,
                   detail="incremental_scan is enabled in config but the "
                          "frontier schema is not usable; the legacy scanner "
                          "stays active and nothing was migrated.")


class TapeBudgetExceeded(Exception):
    """The next sealed chunk does not fit under the tape's DB safety budget."""


class ScopeConfigurationError(ValueError):
    """The configured source roots cannot be turned into a valid scope set."""


def canonicalize_scopes(roots):
    """Canonicalize configured roots and REJECT overlap (Task 2.3).

    Returns ``[(canonical_root, scope_kind_hint), ...]`` in configured order.

    Overlapping roots are refused rather than coalesced. Silently absorbing
    ``/vault/a`` into ``/strg`` would make per-scope coverage meaningless — the
    operator asked about two things and would be told about one — and would
    make the resumed-scope comparison in
    :meth:`~src.pg_scan.PgScanMixin.create_scan_scopes` compare a set that no
    longer matches the configuration it came from. An exact duplicate is the
    same problem and is refused too.
    """
    canonical = []
    for raw in roots:
        text = str(raw or "").replace("\\", "/").strip()
        if not text:
            raise ScopeConfigurationError("a configured scan root is empty")
        if not text.startswith("/"):
            raise ScopeConfigurationError(
                f"a scan root must be an absolute POSIX path: {raw!r}")
        canonical.append(posixpath.normpath(text))

    for i, outer in enumerate(canonical):
        for j, inner in enumerate(canonical):
            if i == j:
                continue
            if outer == inner and i < j:
                raise ScopeConfigurationError(
                    f"duplicate scan root: {outer!r}")
            if inner != "/" and outer.startswith(inner.rstrip("/") + "/"):
                raise ScopeConfigurationError(
                    f"overlapping scan roots: {outer!r} lies under {inner!r}. "
                    "Overlap is refused rather than merged, because merged "
                    "scopes cannot report per-root coverage. Configure one or "
                    "the other.")
    return canonical


def reconcile_scope_order(configured, persisted, ui=None):
    """Decide how a resumed session's scopes relate to today's configuration.

    Returns the roots to traverse, in the order the run should use.

    * identical set, identical order  -> use it;
    * identical set, REORDERED        -> warn and use the PERSISTED order, so
      entry ordinals stay reproducible across restarts;
    * anything added or removed       -> refuse. Silently expanding coverage
      archives files the session never planned for; silently shrinking it
      leaves the operator believing a root was covered.
    """
    if not persisted:
        return list(configured)
    if set(configured) == set(persisted):
        if list(configured) != list(persisted):
            message = ("[SCAN] The configured scan-root ORDER differs from the "
                       "order this session recorded. Using the persisted order "
                       "so scan ordinals stay reproducible.")
            get_logger().warning("scan_scope_order_differs: configured=%s "
                                 "persisted=%s", configured, persisted)
            if ui is not None:
                ui.warning(message)
            else:
                print(message)
        return list(persisted)

    added = sorted(set(configured) - set(persisted))
    removed = sorted(set(persisted) - set(configured))
    raise ScopeConfigurationError(
        f"the configured scan roots differ from the ones this session "
        f"recorded. Added: {added or 'none'}; removed: {removed or 'none'}. "
        "Scope drift is refused, never absorbed: continuing would either "
        "archive files the session never planned for, or report a root as "
        "covered that is no longer being scanned. Reconcile explicitly.")


class RemoteScanCoordinator:
    """Owns discovery -> chunk publication for one remote session.

    Task 1.1 extraction. This is the scanner/planner half of the pipeline that
    used to live as two closures inside
    ``RemoteOrchestrator._run_streaming_session``: ``_scanner_planner`` (now
    :meth:`run`) and ``_append_chunk`` (now :meth:`publish_legacy_chunk`).
    The behaviour is deliberately unchanged — including the resumed-backlog
    ordering, which Task 1.3 is where the fairness rule replaces.

    What the extraction must preserve, and what its tests assert:

    * **Cancellation** through ``src.runtime.CANCEL`` and the caller's stop
      event, checked at the same points as before.
    * **Queue closure**: the sentinel is force-put exactly once, in a
      ``finally``, so the stager can never block forever waiting for it.
    * **Error propagation**: a scan failure records the session's scan error,
      hands the exception to the caller's callback, and stops publication —
      it never marks the scan complete.

    Every collaborator is injected, so the coordinator can be tested without
    SSH, PostgreSQL, staging or a tape.
    """

    def __init__(self, *, db, session_id, scan_paths, state, remaining_lock,
                 stop_event, budget_bytes, alloc_unit, padding_factor,
                 max_files, scanner_factory=None, on_budget_exceeded=None,
                 on_scan_error=None, on_chunk_published=None,
                 publication_gate=None, on_finished=None):
        self.db = db
        self.session_id = session_id
        self.scan_paths = list(scan_paths)
        self.state = state
        self.remaining_lock = remaining_lock
        self.stop_event = stop_event
        self.budget_bytes = budget_bytes
        self.alloc_unit = alloc_unit
        self.padding_factor = padding_factor
        self.max_files = max_files
        self._scanner_factory = scanner_factory
        self._on_budget_exceeded = on_budget_exceeded
        self._on_scan_error = on_scan_error
        self._on_chunk_published = on_chunk_published
        #: Called before each publication. Returns False to stop publishing.
        #: Task 1.3 uses it to bound sealed-but-unstaged work WITHOUT making
        #: renewed exploration queue behind the whole resumed backlog.
        self._publication_gate = publication_gate
        self._on_finished = on_finished

    def _stopping(self):
        return CANCEL.is_set() or self.stop_event.is_set()

    @staticmethod
    def _chunk_rows(chunk_index, chunk_files):
        return [
            (chunk_index, remote_fpath, os.path.basename(remote_fpath), fsize)
            for remote_fpath, fsize in chunk_files
        ]

    # -- publication ------------------------------------------------------
    def publish_legacy_chunk(self, chunk_files):
        """Seal one discovered chunk into the session plan.

        Returns True to continue scanning, False to stop. Raises
        :class:`TapeBudgetExceeded` only when no ``on_budget_exceeded``
        callback was supplied.

        Note the ORDER, which is load-bearing and unchanged: the membership
        filter runs *after* these paths have already moved the chunk boundary,
        so a resumed scan's boundaries differ from the original run's.
        """
        state = self.state
        if self._publication_gate is not None and not self._publication_gate():
            return False
        if hasattr(self.db, 'get_remote_existing_snapshot_paths'):
            # ONE bulk query per sealed chunk — never one round trip per file.
            paths = [remote_fpath for remote_fpath, _ in chunk_files]
            query_started = time.monotonic()
            existing = self.db.get_remote_existing_snapshot_paths(
                self.session_id, paths)
            before = len(chunk_files)
            chunk_files = [
                (remote_fpath, fsize)
                for remote_fpath, fsize in chunk_files
                if remote_fpath.replace('\\', '/') not in existing
            ]
            state.metrics.note_membership_query(
                time.monotonic() - query_started, len(paths),
                before - len(chunk_files))
            if not chunk_files:
                return True

        logical_bytes = sum(fsize for _, fsize in chunk_files)
        with self.remaining_lock:
            if logical_bytes > state.remaining_bytes:
                msg = (
                    f"next remote chunk needs {logical_bytes / 1024**3:.2f} GiB, "
                    f"but only {state.remaining_bytes / 1024**3:.2f} GiB "
                    "remains on the mounted tape under the DB safety budget"
                )
                state.scan_error = msg
                self.db.mark_remote_scan_error(self.session_id, msg)
                print(f"[TAPE] {msg}. Stopping before overfill.")
                if self._on_budget_exceeded is None:
                    raise TapeBudgetExceeded(msg)
                self._on_budget_exceeded(msg)
                return False

        chunk_index = state.next_chunk_index
        insert_started = time.monotonic()
        result = self.db.append_remote_streaming_chunk(
            self.session_id, chunk_index,
            self._chunk_rows(chunk_index, chunk_files))
        inserted_files = int(result.get('inserted_files', 0))
        inserted_bytes = int(result.get('inserted_bytes', 0))
        state.metrics.note_plan_insert(
            time.monotonic() - insert_started, inserted_files)
        if inserted_files == 0:
            return True
        state.metrics.mark_first_sealed_chunk()

        with self.remaining_lock:
            state.remaining_bytes = max(
                0, state.remaining_bytes - inserted_bytes)
        state.next_chunk_index += 1
        state.chunks += 1
        state.files += inserted_files
        state.bytes += inserted_bytes
        _status('SCAN', f"Chunk {chunk_index + 1} planned: "
                        f"{inserted_files:,} file(s), "
                        f"{inserted_bytes / 1024**3:.2f} GiB")
        if self._on_chunk_published is not None:
            self._on_chunk_published(chunk_index)
        return not self._stopping()

    # -- the scanner thread body -----------------------------------------
    def _build_scanner(self):
        if self._scanner_factory is not None:
            return self._scanner_factory(self.state.metrics)
        raise RuntimeError(
            "RemoteScanCoordinator needs a scanner_factory to explore")

    def run(self):
        """Explore the configured scope and publish sealed chunks.

        Runs on its own thread. It no longer hands the resumed backlog through
        a bounded queue before exploring (Task 1.3): the stager reads pending
        chunks from authoritative status instead, so old pending work can never
        sit in front of renewed exploration. ``on_finished`` always fires.
        """
        state = self.state
        try:
            builder = StreamingChunkBuilder(
                self.budget_bytes,
                alloc_unit=self.alloc_unit,
                padding_factor=self.padding_factor,
                max_files=self.max_files,
            )
            scanner = self._build_scanner()
            for remote_fpath, fsize in scanner.iter_scan(
                    self.scan_paths, stop_evt=self.stop_event):
                if self._stopping():
                    return
                for chunk in builder.add(remote_fpath, fsize):
                    if not self.publish_legacy_chunk(chunk):
                        return
            for chunk in builder.flush():
                if not self.publish_legacy_chunk(chunk):
                    return
            if not self._stopping():
                self.db.mark_remote_scan_complete(self.session_id)
                _status('SCAN', f"Complete: {state.chunks:,} new "
                                f"chunk(s), {state.files:,} file(s), "
                                f"{state.bytes / 1024**3:.2f} GiB")
        except Exception as exc:
            get_logger().exception("streaming scanner failed")
            state.scan_error = str(exc)
            self.db.mark_remote_scan_error(self.session_id, str(exc))
            if self._on_scan_error is None:
                raise
            self._on_scan_error(exc)
        finally:
            if self._on_finished is not None:
                self._on_finished()


class DirectoryFrontierCoordinator:
    """Traverse a source scope one DIRECTORY at a time, resumably.

    Plan 1, Task 2.3. This is the model Task 0.2 measured and Task 0.3 adopted.
    Where :class:`RemoteScanCoordinator` re-enumerates whole roots on every
    run, this one claims a single directory, lists its immediate children,
    publishes them as a ready segment artifact, enqueues its subdirectories,
    and commits — so a crash replays **at most that one directory**.

    Coverage rules, enforced here and asserted by tests:

    * ``listing_state='complete'`` means only that THIS directory's own
      immediate listing succeeded. A directory that recorded any exceptional
      entry becomes ``error`` instead.
    * ``subtree_coverage_state='final'`` additionally requires every descendant
      to be terminal and the before/after observations to agree. Segment
      allocation is deliberately irrelevant to it.
    * Before global finality a **final mutation sweep** re-reads each covered
      directory's observation token; a changed token invalidates that directory
      and its ancestors and requeues the bounded subtree.

    Residual risk, stated rather than hidden: the observation token is
    mtime/ctime/inode and the entries carry path+size. A **same-size content
    replacement** with a preserved mtime is undetectable without hashing, and
    hashing would mean reading every source byte over SSH — which is the fetch,
    not the scan.
    """

    def __init__(self, *, db, session_id, scan_paths, archive_root,
                 scanner_factory, stop_event, owner_token=None, ui=None,
                 metrics=None, max_directories=None):
        self.db = db
        self.session_id = session_id
        self.scan_paths = list(scan_paths)
        self.archive_root = archive_root
        self._scanner_factory = scanner_factory
        self.stop_event = stop_event
        self.owner_token = owner_token or uuid.uuid4().hex
        self.ui = ui
        self.metrics = metrics
        #: Test/pilot bound on how many directories one run may process.
        self.max_directories = max_directories

        self.attempt_id = None
        self.directories_listed = 0
        self.segments_published = 0
        self._scanner = None

    # -- setup ------------------------------------------------------------
    def _stopping(self):
        return CANCEL.is_set() or self.stop_event.is_set()

    def scanner(self):
        if self._scanner is None:
            self._scanner = self._scanner_factory(self.metrics)
        return self._scanner

    def establish_scopes(self):
        """Canonicalize, reject overlap, reconcile against persisted order."""
        canonical = canonicalize_scopes(self.scan_paths)
        persisted = [row["source_root"]
                     for row in self.db.get_scan_scopes(self.session_id)]
        ordered = reconcile_scope_order(canonical, persisted, ui=self.ui)
        if not persisted:
            self.db.create_scan_scopes(
                self.session_id,
                [(root, self._scope_kind(root)) for root in ordered])
            scopes = self.db.get_scan_scopes(self.session_id)
            # Seed each directory scope with its own root as the first
            # directory to list. A single-file scope has no directory to list.
            for scope in scopes:
                if scope["scope_kind"] == SCOPE_KIND_DIRECTORY:
                    self.db.enqueue_scan_directories(
                        scope["scan_scope_id"], [(scope["source_root"], 0)])
        return self.db.get_scan_scopes(self.session_id)

    def _scope_kind(self, root):
        """A scope is a file only when the caller said so.

        Determining it by probing the source would be another round trip per
        root; the configuration knows, and an explicit single-file selection is
        exactly the case Plan 1 asks to support.
        """
        hints = getattr(self, "file_scope_hints", None) or ()
        return SCOPE_KIND_FILE if root in hints else SCOPE_KIND_DIRECTORY

    # -- one directory ----------------------------------------------------
    def process_one_directory(self):
        """Claim, list, publish and commit ONE directory.

        Returns ``True`` when a directory was processed, ``False`` when the
        frontier had nothing claimable.
        """
        claimed = self.db.claim_next_directory(
            self.session_id, self.owner_token, self.attempt_id)
        if claimed is None:
            return False

        directory_id = claimed["scan_directory_id"]
        path = claimed["canonical_path"]
        scanner = self.scanner()
        try:
            listing = scanner.list_directory(path)
        except Exception as exc:
            # The listing as a whole failed: release the claim as PARTIAL so
            # the next run retries exactly this directory, and record why.
            self.db.mark_directory_partial(directory_id, self.owner_token)
            self.db.record_scan_error(
                scan_directory_id=directory_id, category="listing_failed",
                path=path, message=str(exc))
            raise

        # Publish the entries as ONE ready segment artifact before any state
        # says this directory is covered: a segment the database points at must
        # already exist on disk, never the other way round.
        if listing.files:
            self._publish_segment(directory_id, listing)

        if listing.directories:
            base = int(claimed["traversal_ordinal"]) + 1
            self.db.enqueue_scan_directories(
                claimed["scan_scope_id"],
                [(child, base + offset)
                 for offset, child in enumerate(listing.directories)],
                parent_directory_id=directory_id)

        for category, error_path, message in listing.errors:
            self.db.record_scan_error(
                scan_directory_id=directory_id, category=category,
                path=error_path, message=message)

        self.db.complete_directory_listing(
            directory_id, self.owner_token,
            direct_file_count=listing.file_count,
            direct_byte_count=listing.byte_count,
            observation_after=listing.observation,
            error_count=len(listing.errors))
        self.directories_listed += 1
        return True

    def _publish_segment(self, directory_id, listing):
        """Write the artifact, then record it. In that order, always.

        A directory that was invalidated and re-listed will find its previous
        artifact already published. That is reused **only after complete
        equivalence validation** — same ordered paths, sizes and ordinals. An
        artifact that describes something else is a genuine conflict and is
        surfaced, never overwritten: two workers producing different "same"
        segments is a frontier defect, and letting the later one win would
        silently change what a chunk was planned from.
        """
        locator = segment_locator(self.session_id, directory_id, 0)
        expected = [(path, size, ordinal)
                    for ordinal, (path, size) in enumerate(listing.files)]

        reused = self._reuse_equivalent_artifact(locator, expected)
        if reused is not None:
            first_ordinal, last_ordinal, file_count, byte_count = reused
            first_path = expected[0][0] if expected else None
            last_path = expected[-1][0] if expected else None
        else:
            try:
                with JsonlZstArtifactWriter(
                        self.archive_root, locator, session_id=self.session_id,
                        scan_directory_id=directory_id,
                        scope=listing.path) as writer:
                    for path, size, ordinal in expected:
                        writer.add(path=path, size=size, ordinal=ordinal)
            except ArtifactError as exc:
                self.db.record_scan_error(
                    scan_directory_id=directory_id, category="artifact_failed",
                    path=listing.path, message=str(exc))
                raise
            first_ordinal = writer.first_ordinal or 0
            last_ordinal = (writer.last_ordinal
                            if writer.last_ordinal is not None else 0)
            file_count = writer.file_count
            byte_count = writer.byte_count
            first_path = writer.first_path
            last_path = writer.last_path

        self.db.publish_scan_segment(
            directory_id, first_scan_ordinal=first_ordinal,
            last_scan_ordinal=last_ordinal, locator=locator,
            file_count=file_count, byte_count=byte_count,
            first_canonical_path=first_path, last_canonical_path=last_path)
        self.segments_published += 1

    def _reuse_equivalent_artifact(self, locator, expected):
        """``(first, last, files, bytes)`` if an identical artifact exists.

        Returns ``None`` when there is nothing to reuse. Raises when an
        artifact exists but describes different content — that is the conflict
        the caller must not paper over.
        """
        path = resolve_locator(self.archive_root, locator)
        if not os.path.exists(path):
            return None
        _header, entries, totals = parse_jsonl_zst_artifact(
            self.archive_root, locator)
        actual = [(e["path"], e["size"], e["ordinal"]) for e in entries]
        if actual != expected:
            raise ArtifactConflict(
                f"an artifact already exists at {locator!r} but describes "
                f"different content ({len(actual)} entries vs "
                f"{len(expected)}). Refusing to overwrite it: reconcile the "
                "frontier rather than letting a re-listing silently replace "
                "what a chunk may already have been planned from.")
        get_logger().info(
            "scan_segment_reused: locator=%s files=%d (identical content)",
            locator, totals["file_count"])
        return (totals["first_scan_ordinal"] or 0,
                totals["last_scan_ordinal"] or 0,
                totals["file_count"], totals["byte_count"])

    # -- the run ----------------------------------------------------------
    def run(self):
        """Drain the directory frontier, then sweep and finalize."""
        self.attempt_id = self._start_attempt()
        try:
            self.establish_scopes()
            while not self._stopping():
                if (self.max_directories is not None
                        and self.directories_listed >= self.max_directories):
                    break
                if not self.process_one_directory():
                    break
            if not self._stopping():
                self.final_mutation_sweep()
                self.finalize()
        finally:
            if self.attempt_id is not None:
                try:
                    self.db.finish_worker_attempt(
                        self.attempt_id,
                        "completed" if not self._stopping() else "abandoned")
                except Exception:
                    get_logger().warning("could not close the scan attempt",
                                         exc_info=True)

    def _start_attempt(self):
        try:
            return self.db.start_worker_attempt(
                owner_token=self.owner_token, attempt_kind="scan",
                session_id=self.session_id, local_pid=os.getpid())
        except Exception:
            get_logger().warning(
                "could not record a durable scan attempt; continuing without "
                "one (reconciliation will have less evidence)", exc_info=True)
            return None

    def final_mutation_sweep(self):
        """Re-read every covered directory's observation before finality.

        A changed token means the source moved under us after we listed it, so
        that directory and its ancestors are invalidated and the bounded
        subtree is requeued. Cheap: one ``stat`` per directory, no listing.
        """
        scanner = self.scanner()
        invalidated = 0
        for scope in self.db.get_scan_scopes(self.session_id):
            for row in self._covered_directories(scope["scan_scope_id"]):
                if self._stopping():
                    return invalidated
                current = scanner.observe(row["canonical_path"])
                recorded = row.get("observation_after")
                if recorded is None or current is None:
                    # Nothing to compare: leave coverage provisional rather
                    # than asserting a finality we cannot support.
                    continue
                if current != recorded:
                    self.db.invalidate_directory(
                        row["scan_directory_id"],
                        f"observation changed after listing "
                        f"({recorded} -> {current})")
                    invalidated += 1
        return invalidated

    def _covered_directories(self, scan_scope_id):
        reader = getattr(self.db, "get_covered_directories", None)
        if reader is None:
            return []
        return reader(scan_scope_id)

    def finalize(self):
        """Finalize subtrees bottom-up, then each scope."""
        finalized = 0
        for scope in self.db.get_scan_scopes(self.session_id):
            for row in reversed(list(
                    self._covered_directories(scope["scan_scope_id"]))):
                done, _reason = self.db.finalize_directory_subtree(
                    row["scan_directory_id"])
                finalized += bool(done)
            self.db.finalize_scan_scope(scope["scan_scope_id"])
        return finalized


class SegmentChunkPublisher:
    """Turn ready scan segments into sealed chunks (Plan 1, Task 2.4).

    Plan 1 deliberately keeps the EXISTING chunk format and
    ``PgSessionMixin.get_chunk_files()`` as the production planning source —
    only where the entries come from changes. Chunk contents are read from
    local segment artifacts instead of being re-discovered over SSH.

    Two things this must never do, and the reason for each:

    * **Never call ``get_remote_existing_snapshot_paths`` per rediscovered
      file.** That is the per-file database cost the frontier exists to remove.
      A migrated legacy session is reconciled ONCE per segment, in one
      set-based query, and the segment is then marked as imported.
    * **Never silently drop a ``source_changed`` entry.** Same path, different
      size means the source moved after planning. The existing membership may
      already be on tape; anti-joining the entry away would quietly leave the
      catalog describing bytes that are no longer there. It becomes an
      unresolved error (keeping coverage provisional) and nothing is replanned
      without an operator.
    """

    def __init__(self, *, db, session_id, archive_root, builder_factory,
                 legacy_session=False):
        self.db = db
        self.session_id = session_id
        self.archive_root = archive_root
        self._builder_factory = builder_factory
        #: True when this session pre-dates the frontier and still has
        #: whole-root snapshot rows to reconcile against.
        self.legacy_session = legacy_session
        self.blocked_segments = []
        #: Paths the legacy catalog key cannot represent faithfully.
        self.unrepresentable = []
        self.chunks_sealed = 0

    def entries_for_segment(self, segment):
        """The segment's entries, after any one-time legacy reconciliation."""
        _header, entries, _totals = parse_jsonl_zst_artifact(
            self.archive_root, segment["locator"])
        pairs = [(e["path"], int(e["size"])) for e in entries]
        pairs = self._drop_unrepresentable(segment, pairs)
        if not self.legacy_session:
            return pairs

        outcome = self.db.import_legacy_scan_segment(
            self.session_id, segment["scan_segment_id"], pairs)
        if outcome.get("already_imported"):
            return pairs
        if outcome["source_changed"]:
            # Fail closed. The segment is marked 'blocked' by the repository;
            # nothing here plans a replacement.
            self.blocked_segments.append(segment["scan_segment_id"])
            get_logger().error(
                "segment_blocked_by_source_change: segment=%s entries=%d",
                segment["scan_segment_id"], len(outcome["source_changed"]))
            return []
        return outcome["new"]

    def _drop_unrepresentable(self, segment, pairs):
        """Withhold paths the legacy catalog key cannot represent faithfully.

        Plan 1, Task 3.3. ``_canonical_remote_path`` rewrites every backslash
        into a forward slash, so ``/vault/a/back\\slash`` and
        ``/vault/a/back/slash`` become the SAME catalog key. On Linux those are
        two different files.

        The canonicaliser cannot be changed in place — the existing catalog was
        built with it, and millions of rows depend on the rule. So such a path
        is **recorded as an exceptional entry and withheld from planning**,
        which keeps the directory's coverage provisional until a human decides,
        rather than archiving one file under another's name.

        Rare in practice; loud whenever it happens.
        """
        safe, unsafe = [], []
        for path, size in pairs:
            (safe if remote_path_is_legacy_safe(path) else unsafe).append(
                (path, size))
        for path, _size in unsafe:
            get_logger().error(
                "unrepresentable_remote_path: %r contains a literal backslash, "
                "which the legacy catalog key would rewrite into a separator; "
                "withheld from planning", path)
            try:
                self.db.record_scan_error(
                    category="unrepresentable_path", path=path,
                    message="the path contains a literal backslash, which the "
                            "catalog's canonical key would turn into a "
                            "separator and merge with a different file; it was "
                            "NOT planned",
                    disposition="unresolved")
            except Exception:
                get_logger().warning(
                    "could not record an unrepresentable-path error",
                    exc_info=True)
        self.unrepresentable.extend(path for path, _size in unsafe)
        return safe

    def publish_ready_segments(self, next_chunk_index, limit=50):
        """Seal chunks from whatever is ready. Returns the sealed indices."""
        sealed = []
        builder = self._builder_factory()
        pending = []                      # (files, segment, first, last)
        for segment in self.db.get_ready_segments(self.session_id, limit=limit):
            entries = self.entries_for_segment(segment)
            if not entries:
                continue
            pending.append((entries, segment))

        for entries, segment in pending:
            chunks = []
            for path, size in entries:
                chunks.extend(builder.add(path, size))
            for chunk in chunks:
                next_chunk_index = self._seal(chunk, next_chunk_index, segment)
                sealed.append(next_chunk_index - 1)
        for chunk in builder.flush():
            next_chunk_index = self._seal(chunk, next_chunk_index,
                                          pending[-1][1] if pending else None)
            sealed.append(next_chunk_index - 1)
        self.chunks_sealed += len(sealed)
        return sealed

    def _seal(self, chunk_files, chunk_index, segment):
        """Append the members, then seal — atomically expectation-first."""
        rows = [(chunk_index, path, os.path.basename(path), size)
                for path, size in chunk_files]
        result = self.db.append_remote_streaming_chunk(
            self.session_id, chunk_index, rows)
        inserted = int(result.get("inserted_files", 0))
        if inserted == 0:
            return chunk_index

        first = last = None
        if segment is not None:
            allocated = self.db.consume_segment_range(
                segment["scan_segment_id"], self.session_id, chunk_index,
                inserted)
            first, last = allocated
        self.db.seal_remote_chunk(
            self.session_id, chunk_index,
            expected_file_count=inserted,
            expected_bytes=int(result.get("inserted_bytes", 0)),
            scan_segment_id=(segment or {}).get("scan_segment_id"),
            first_scan_ordinal=first, last_scan_ordinal=last)
        return chunk_index + 1


def build_frontier_scanner_factory(*, remote_user, remote_host,
                                   remote_password, skipped_tracker, ui,
                                   timeout=None):
    """A ``scanner_factory`` producing the immediate-child directory scanner."""

    def factory(metrics):
        return DirectoryFrontierScanner(
            remote_user, remote_host, remote_password=remote_password,
            timeout=timeout, skipped_tracker=skipped_tracker, ui=ui,
            metrics=metrics)
    return factory


def build_legacy_scanner_factory(*, remote_user, remote_host, remote_password,
                                 skipped_tracker, ui, cipher):
    """A ``scanner_factory`` producing today's whole-root streaming scanner."""

    def factory(metrics):
        return StreamingRemoteScanner(
            remote_user,
            remote_host,
            remote_password=remote_password,
            skipped_tracker=skipped_tracker,
            ui=ui,
            cipher=cipher,
            metrics=metrics,
        )
    return factory
