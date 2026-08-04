"""Root runner for PostgreSQL inspector and catalog maintenance commands."""
import argparse
import json
import os
import stat
import sys
from datetime import datetime, timezone
from urllib.parse import quote

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.constants import PROJECT_ROOT
os.chdir(PROJECT_ROOT)

from src.cli_errors import OperationalError
from src.config import ConfigManager
from src.local_manifest_archive import (
    active_archive_processes,
    dry_run_export,
    execute_export,
    export_legacy_cold_database,
    export_status,
    prune_export,
    pruned_manifest_paths,
    search_manifests,
    validate_archive_root,
    validate_export,
)
from src.db import create_database_manager
from src.directory_catalog_validation import (
    archiver_lock_status,
    compare_databases,
    describe_database,
    validate_directory_catalog,
)
from src.pg_backup import (
    apply_directory_catalog_schema_to_database,
    create_migrated_database_from_backup,
    create_verified_production_backup,
    verify_backup_file,
    verify_backup_receipt,
)
from src.pg_bulk import build_conninfo, make_conninfo
from src.remote_staging import inspect_resume_pack_marker
from src.session_reconcile import (
    DEFAULT_IDLE_SECONDS,
    format_report,
    liveness_evidence,
    reconcile_stale_remote_sessions,
    session_forensics,
)


class _DbOverrideConfig:
    def __init__(self, base, dbname):
        self._base = base
        self._dbname = dbname

    def __getattr__(self, name):
        return getattr(self._base, name)

    @property
    def pg_dbname(self):
        return self._dbname or self._base.pg_dbname

    @property
    def db_dsn(self):
        user = quote(self.pg_user, safe='')
        password = quote(self.pg_password, safe='')
        auth = f"{user}:{password}@" if password else f"{user}@"
        return (
            f"postgresql://{auth}{self.pg_host}:{self.pg_port}/"
            f"{quote(self.pg_dbname, safe='')}?sslmode={quote(self.pg_sslmode, safe='')}"
        )

    @property
    def db_display_ref(self):
        user = quote(self.pg_user, safe='')
        auth = f"{user}:***@" if self.pg_password else f"{user}@"
        return (
            f"postgresql://{auth}{self.pg_host}:{self.pg_port}/"
            f"{quote(self.pg_dbname, safe='')}?sslmode={quote(self.pg_sslmode, safe='')}"
        )


def _config(args):
    cfg = ConfigManager()
    return _DbOverrideConfig(cfg, args.db) if args.db else cfg


def _conninfo(cfg, dbname=None):
    return build_conninfo(
        host=cfg.pg_host,
        port=cfg.pg_port,
        dbname=dbname or cfg.pg_dbname,
        user=cfg.pg_user,
        password=cfg.pg_password,
        sslmode=cfg.pg_sslmode,
    )


def _print_json(payload):
    print(json.dumps(payload, indent=2, default=str))


def _open_db(cfg):
    try:
        return create_database_manager(cfg)
    except RuntimeError as exc:
        print(f"\n{exc}")
        raise SystemExit(1) from exc


def _open_no_init_db(cfg):
    """Open PostgreSQL without implicit startup DDL."""
    from src.pg_db import PgDatabaseManager
    try:
        return PgDatabaseManager(_conninfo(cfg), init_schema=False)
    except RuntimeError as exc:
        print(f"\n{exc}")
        raise SystemExit(1) from exc


def _open_read_only_db(cfg):
    """Open a manager whose server sessions reject every write."""
    from src.pg_db import PgDatabaseManager
    conninfo = make_conninfo(
        _conninfo(cfg), options="-c default_transaction_read_only=on")
    try:
        return PgDatabaseManager(conninfo, init_schema=False)
    except RuntimeError as exc:
        print(f"\n{exc}")
        raise SystemExit(1) from exc


def _require_maintenance_safe(cfg):
    holders = archiver_lock_status(_conninfo(cfg))
    if holders:
        raise OperationalError(
            "[MANIFEST] Refusing maintenance while the archiver lock is held.")
    processes = active_archive_processes()
    if processes:
        raise OperationalError(
            "[MANIFEST] Refusing maintenance while archive/transfer processes "
            f"are running: {processes}")
    return validate_archive_root(
        cfg.local_manifest_archive_root, (cfg.staging_dir,))


def _run_all_session_health(cfg):
    """READ-ONLY health classification across every session (Plan 1).

    Opens no LTFS path, reads no tape, starts nothing, and writes no row.

    ``init_schema=False`` is load-bearing, not a micro-optimisation: the default
    ``PgDatabaseManager`` constructor APPLIES PENDING MIGRATIONS and commits
    them. A command advertised as read-only that quietly migrates the catalog
    would be a lie, and would make this report unusable as pre-change evidence —
    the thing it exists to be. The liveness inputs are host-wide, so a run while
    the archiver is active is reported rather than silently ignored.
    """
    from src.pg_db import PgDatabaseManager
    from src.session_health import all_session_health

    conninfo = _conninfo(cfg)
    db = PgDatabaseManager(conninfo, init_schema=False)
    try:
        report = all_session_health(
            db,
            active_processes=active_archive_processes(),
            lock_holders=archiver_lock_status(conninfo))
    finally:
        db.close()
    report["database"] = cfg.pg_dbname
    _print_json(report)
    return 0


def _apply_incremental_scan_schema(cfg, args, parser):
    """Guarded entry point for migration 014 (Plan 1, Task 2.1).

    Read-only by default. ``--dry-run`` (or neither flag) prints the preflight
    and changes nothing; ``--execute --yes`` applies the BASE half, and adding
    ``--finalize`` also applies the audit + final constraints.

    Everything that could make this unsafe is checked BEFORE any DDL:

    * the exact database identity is printed and must be confirmed, so a
      migration cannot land on the wrong catalog;
    * ``--backup-file`` must name a backup this tool can verify — a migration
      whose only recovery path is "hope" is not a migration;
    * no archiver process may hold the cluster advisory lock, because migrating
      under a live run changes a session's schema mid-write;
    * duplicate legacy plan ordinals are reported, and the finalize step will
      REFUSE rather than resequence them.
    """
    db = _open_db(cfg)
    try:
        preflight = db.incremental_scan_schema_preflight()
    finally:
        db.close()
    preflight["requested"] = {
        "execute": bool(args.execute),
        "finalize": bool(args.finalize),
        "database": cfg.pg_dbname,
    }

    if not args.execute:
        preflight["applied"] = []
        preflight["note"] = (
            "read-only preflight; re-run with --execute --yes "
            "--backup-file <verified backup> to apply")
        _print_json(preflight)
        return 0

    if not args.yes:
        parser.error("--apply-incremental-scan-schema --execute requires --yes")
    if not args.backup_file:
        parser.error(
            "--apply-incremental-scan-schema --execute requires --backup-file "
            "naming a PostgreSQL backup this tool can verify")

    holders = archiver_lock_status(_conninfo(cfg))
    if holders:
        raise OperationalError(
            "[MIGRATION 014] Refusing to migrate while the archiver lock is "
            f"held: {holders}. Stop the archive run first.")
    processes = active_archive_processes()
    if processes:
        raise OperationalError(
            "[MIGRATION 014] Refusing to migrate while archive/transfer "
            f"processes are running: {processes}")
    if preflight["archiver_lock_held"]:
        raise OperationalError(
            "[MIGRATION 014] The cluster advisory lock is held; refusing.")

    preflight["backup"] = _verify_hot_backup(cfg, args.backup_file)

    if args.finalize and preflight["duplicate_ordinal_groups"]:
        raise OperationalError(
            "[MIGRATION 014] Refusing to finalize: "
            f"{preflight['duplicate_ordinal_groups']} duplicate "
            "(plan_id, chunk_index, ordinal) group(s) exist in "
            "remote_plan_files. They are NOT auto-resequenced — an ordinal "
            "positions a file inside a chunk that may already be on tape. "
            "Review them and decide per chunk. Sample: "
            f"{preflight['duplicate_ordinal_sample']}")

    db = _open_db(cfg)
    try:
        preflight["applied"] = db.apply_incremental_scan_schema(
            finalize=bool(args.finalize))
        preflight["installed"] = db.incremental_scan_schema_installed()
        preflight["finalized"] = db.incremental_scan_schema_finalized()
    finally:
        db.close()
    _print_json(preflight)
    return 0


def _windows_drive_device_target(drive):
    """Resolve a DOS drive mapping without opening the mounted filesystem."""
    if os.name != "nt":
        return drive.casefold()
    import ctypes

    buffer = ctypes.create_unicode_buffer(32768)
    result = ctypes.windll.kernel32.QueryDosDeviceW(
        drive.rstrip("\\/"), buffer, len(buffer))
    if not result:
        raise OperationalError(
            "[MIGRATION 015] A drive mapping could not be resolved; staging "
            "location evidence is indeterminate")
    targets = [item for item in buffer[:result].split("\0") if item]
    if len(targets) != 1:
        raise OperationalError(
            "[MIGRATION 015] A drive has multiple device mappings; staging "
            "location evidence is indeterminate")
    target = targets[0].casefold()
    # SUBST commonly returns ``\??\Z:\...``.  Treat every namespace alias
    # as indeterminate rather than recursively resolving a path that may lead
    # to LTFS.  Plain local and LTFS drive letters both resolve canonically
    # through the device namespace; redirected network drives are not local.
    if (not target.startswith("\\device\\")
            or target.startswith(("\\device\\mup\\",
                                  "\\device\\lanmanredirector\\"))):
        raise OperationalError(
            "[MIGRATION 015] A drive mapping is aliased or non-local; staging "
            "location evidence is indeterminate")
    return target


def _assert_plain_local_directory(path):
    """Reject aliases/reparse components without following any of them."""
    if (path.startswith(("\\\\", "//"))
            or path.casefold().startswith(("\\\\?\\", "\\\\.\\"))):
        raise OperationalError(
            "[MIGRATION 015] Staging must be a plain local drive path; aliases "
            "and device/UNC paths are refused")
    drive, tail = os.path.splitdrive(path)
    if not drive or not tail.startswith(("\\", "/")):
        raise OperationalError(
            "[MIGRATION 015] Staging is not an absolute local drive path")

    current = drive + os.sep
    components = [part for part in tail.replace("/", "\\").split("\\")
                  if part]
    reparse_flag = getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400)
    try:
        for component in components:
            current = os.path.join(current, component)
            info = os.lstat(current)
            if (not stat.S_ISDIR(info.st_mode)
                    or getattr(info, "st_file_attributes", 0) & reparse_flag):
                raise OperationalError(
                    "[MIGRATION 015] Staging contains a non-directory or "
                    "reparse component; evidence is indeterminate")
    except OperationalError:
        raise
    except OSError as exc:
        raise OperationalError(
            "[MIGRATION 015] Staging path metadata is unreadable; evidence is "
            "indeterminate") from exc
    return drive


def _inspect_container_format_staging(
        cfg, db, session_id, chunk_indexes, *, strict=True,
        include_details=False):
    """Inspect deterministic candidate staging paths without changing them.

    Execute mode uses the five path-free aggregate fields as an attestation.
    The read-only rehearsal additionally requests per-chunk marker/inventory
    observations.  Missing or unreadable roots are ``unknown`` in that report;
    strict execute mode still fails closed.
    """
    session = db.get_remote_session(int(session_id))
    if not session:
        raise OperationalError(
            f"[MIGRATION 015] Session {session_id} does not exist")
    root_text = str(session.get("staging_dir") or "").strip()
    if not root_text:
        if strict:
            raise OperationalError(
                "[MIGRATION 015] Session staging root is absent; evidence is "
                "indeterminate")
        return {
            "root_accessible": False,
            "root_state": "absent",
            "evidence_state": "unknown",
            "checked_chunk_indexes": sorted(
                {int(index) for index in chunk_indexes}),
            "entry_count": None,
            "unreadable_count": None,
            "chunks": [],
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }
    root = os.path.abspath(root_text)
    lto_text = str(getattr(cfg, "lto_drive", "") or "").strip()
    if not lto_text:
        raise OperationalError(
            "[MIGRATION 015] LTFS drive configuration is absent; staging "
            "separation cannot be proved")
    lto_root = os.path.abspath(lto_text)
    if (lto_root.startswith(("\\\\", "//"))
            or lto_root.casefold().startswith(("\\\\?\\", "\\\\.\\"))):
        raise OperationalError(
            "[MIGRATION 015] LTFS drive configuration is aliased; staging "
            "separation cannot be proved")
    lto_drive = os.path.splitdrive(lto_root)[0]
    if not lto_drive:
        raise OperationalError(
            "[MIGRATION 015] LTFS drive configuration is indeterminate")
    if (root.startswith(("\\\\", "//"))
            or root.casefold().startswith(("\\\\?\\", "\\\\.\\"))):
        raise OperationalError(
            "[MIGRATION 015] Staging must be a plain local drive path")
    root_drive = os.path.splitdrive(root)[0]
    if not root_drive:
        raise OperationalError(
            "[MIGRATION 015] Staging is not an absolute local drive path")

    # Resolve and compare drive mappings before *any* lstat/scandir call.  A
    # literal LTFS path or SUBST alias must be refused without opening it.
    if (root_drive.casefold() == lto_drive.casefold()
            or _windows_drive_device_target(root_drive)
            == _windows_drive_device_target(lto_drive)):
        raise OperationalError(
            "[MIGRATION 015] Refusing staging inspection because the configured "
            "session staging root is on the LTFS drive")
    try:
        _assert_plain_local_directory(root)
    except OperationalError:
        if strict:
            raise
        return {
            "root_accessible": False,
            "root_state": "unreadable",
            "evidence_state": "unknown",
            "checked_chunk_indexes": sorted(
                {int(index) for index in chunk_indexes}),
            "entry_count": None,
            "unreadable_count": None,
            "chunks": [],
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }

    try:
        with os.scandir(root) as entries:
            root_entries = {entry.name.casefold(): entry for entry in entries}
    except OSError as exc:
        if strict:
            raise OperationalError(
                "[MIGRATION 015] Session staging root is unreadable; evidence "
                "is indeterminate") from exc
        return {
            "root_accessible": False,
            "root_state": "unreadable",
            "evidence_state": "unknown",
            "checked_chunk_indexes": sorted(
                {int(index) for index in chunk_indexes}),
            "entry_count": None,
            "unreadable_count": None,
            "chunks": [],
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }

    checked = sorted({int(index) for index in chunk_indexes})
    observations = []
    found = 0
    unreadable = 0
    for chunk_index in checked:
        fetch_name = f"_fetch_s{int(session_id):04d}_{chunk_index:03d}"
        pack_name = f"_pack_s{int(session_id):04d}_{chunk_index:03d}"
        fetch_present = fetch_name.casefold() in root_entries
        pack_entry = root_entries.get(pack_name.casefold())
        pack_present = pack_entry is not None
        found += int(fetch_present) + int(pack_present)
        marker = {
            "marker_state": "unknown_pack_absent",
            "inventory_state": "unknown",
            "pack_file_count": None,
            "packaging_format": None,
        }
        if pack_present:
            try:
                if not pack_entry.is_dir(follow_symlinks=False):
                    marker["marker_state"] = "pack_directory_unreadable"
                else:
                    marker = inspect_resume_pack_marker(
                        os.path.join(root, pack_entry.name),
                        int(session_id), chunk_index)
            except OSError:
                marker["marker_state"] = "pack_directory_unreadable"
            if marker["marker_state"] in (
                    "unreadable", "pack_directory_unreadable"):
                unreadable += 1
        observations.append({
            "chunk_index": chunk_index,
            "fetch_entry_present": fetch_present,
            "pack_entry_present": pack_present,
            "resume_marker": marker,
        })

    evidence = {
        "root_accessible": True,
        "checked_chunk_indexes": checked,
        "entry_count": found,
        "unreadable_count": unreadable,
        "checked_at": datetime.now(timezone.utc).isoformat(),
    }
    if include_details:
        evidence.update({
            "root_state": "accessible",
            "evidence_state": "observed",
            "chunks": observations,
        })
    return evidence


def _run_container_format_schema_report(cfg, *, validate=False):
    """Read-only migration-015 report/validation."""
    db = _open_read_only_db(cfg)
    try:
        report = (db.validate_container_format_schema() if validate
                  else db.container_format_schema_report())
    finally:
        db.close()
    _print_json(report)
    return 0


def _run_session37_boundary_rehearsal(cfg, args):
    """READ-ONLY Plan-2 Gate-5.5 format-boundary report.

    The target is normally an isolated restored database selected with
    ``--db``.  This command opens PostgreSQL read-only, does not inspect local
    staging (through the read-only resume-marker parser), never LTFS, and cannot
    persist the boundary.
    """
    session_id = int(args.session_id[0] if args.session_id else 37)
    conninfo = _conninfo(cfg)
    db = _open_read_only_db(cfg)
    try:
        report = db.classify_format_boundary(session_id)
        boundary = report.get("derived_boundary")
        indexes = ([row["chunk_index"] for row in report.get("chunks", [])
                    if boundary is not None
                    and row["chunk_index"] >= boundary])
        staging_report = _inspect_container_format_staging(
            cfg, db, session_id, indexes, strict=False,
            include_details=True)
    finally:
        db.close()
    _print_json({
        "database": cfg.pg_dbname,
        "session_id": session_id,
        "mode": "read_only_rehearsal",
        "liveness": liveness_evidence(
            conninfo, getattr(cfg, "backup_log_dir", None)),
        "stored_tar_write_enabled": bool(
            getattr(cfg, "stored_tar_write_enabled", False)),
        "boundary_report": report,
        "staging_evidence": staging_report,
        "session37_row_unchanged_by_command": True,
    })
    return 0


def _apply_container_format_schema(cfg, args, parser):
    """Guarded, explicit migration-015 entry point (Plan 2 Task 0.1)."""
    if args.dry_run and args.execute:
        parser.error(
            "--apply-container-format-schema accepts only one of --dry-run "
            "or --execute")
    exception_session_id = args.stored_tar_exception_session_id
    if (args.execute and exception_session_id is not None
            and getattr(cfg, "stored_tar_write_enabled", False) is not True):
        raise OperationalError(
            "[MIGRATION 015] Refusing Stored TAR boundary persistence while "
            "stored_tar_write_enabled is false")
    db = _open_read_only_db(cfg)
    try:
        preflight = db.container_format_schema_preflight(exception_session_id)
        staging_evidence = None
        if exception_session_id is not None:
            exception = preflight.get("exception") or {}
            boundary = exception.get("derived_boundary")
            if boundary is not None:
                indexes = [
                    row["chunk_index"] for row in exception.get("chunks", [])
                    if row["chunk_index"] >= boundary]
                staging_evidence = _inspect_container_format_staging(
                    cfg, db, exception_session_id, indexes)
                if staging_evidence["entry_count"]:
                    preflight["blocking"].append(
                        "candidate chunks have deterministic fetch/pack staging "
                        "evidence")
                if staging_evidence["unreadable_count"]:
                    preflight["blocking"].append(
                        "candidate staging evidence is unreadable")
    finally:
        db.close()

    preflight["requested"] = {
        "execute": bool(args.execute),
        "database": cfg.pg_dbname,
        "stored_tar_exception_session_id": exception_session_id,
        "expected_format_boundary": args.expected_format_boundary,
        "format_approval_id": args.format_approval_id,
    }
    preflight["staging_evidence"] = staging_evidence

    if not args.execute:
        preflight["applied"] = []
        preflight["note"] = (
            "read-only preflight; execute requires --execute --yes and a "
            "verified --backup-file")
        _print_json(preflight)
        return 0

    if not args.yes:
        parser.error("--apply-container-format-schema --execute requires --yes")
    if not args.backup_file:
        parser.error(
            "--apply-container-format-schema --execute requires --backup-file "
            "naming a PostgreSQL backup this tool can verify")
    if preflight["blocking"]:
        raise OperationalError(
            "[MIGRATION 015] Refusing migration: "
            + "; ".join(preflight["blocking"]))

    if exception_session_id is None:
        extras = (args.expected_format_boundary, args.format_approval_id,
                  args.format_approval_reason)
        if any(value is not None for value in extras):
            parser.error(
                "boundary/approval flags require "
                "--stored-tar-exception-session-id")
    else:
        required = (args.expected_format_boundary, args.format_approval_id,
                    args.format_approval_reason)
        if any(value is None for value in required):
            parser.error(
                "an approved Stored TAR exception requires "
                "--expected-format-boundary, --format-approval-id, and "
                "--format-approval-reason")
        derived = preflight["exception"]["derived_boundary"]
        if int(args.expected_format_boundary) != int(derived):
            raise OperationalError(
                f"[MIGRATION 015] Expected boundary "
                f"{args.expected_format_boundary} differs from the read-only "
                f"derived boundary {derived}")
        if staging_evidence is None:
            raise OperationalError(
                "[MIGRATION 015] Local staging evidence is absent")

    holders = archiver_lock_status(_conninfo(cfg))
    if holders:
        raise OperationalError(
            "[MIGRATION 015] Refusing while the archiver lock is held: "
            f"{holders}")
    processes = active_archive_processes()
    if processes:
        raise OperationalError(
            "[MIGRATION 015] Refusing while archive/transfer processes are "
            f"running ({len(processes)} detected)")
    preflight["backup"] = verify_backup_receipt(cfg, args.backup_file)

    # Pin the same cluster-wide lock used by production for the final evidence
    # window. A worker starting after this point cannot acquire its run lock.
    # The final DB classification and root-only staging scan are intentionally
    # repeated under that lock immediately before the single SQL transaction.
    db = _open_no_init_db(cfg)
    try:
        db.acquire_archiver_lock()
        processes = active_archive_processes()
        if processes:
            raise OperationalError(
                "[MIGRATION 015] Refusing after final liveness check "
                f"({len(processes)} archive/transfer process(es) detected)")
        locked_backup = verify_backup_receipt(cfg, args.backup_file)
        if locked_backup["receipt_id"] != preflight["backup"]["receipt_id"]:
            raise OperationalError(
                "[MIGRATION 015] Backup receipt changed before locked apply")
        processes = active_archive_processes()
        if processes:
            raise OperationalError(
                "[MIGRATION 015] A transfer process appeared during final "
                f"backup verification ({len(processes)} detected)")
        final_preflight = db.container_format_schema_preflight(
            exception_session_id, ignore_archiver_lock=True)
        if final_preflight["blocking"]:
            raise OperationalError(
                "[MIGRATION 015] Final locked preflight refused migration: "
                + "; ".join(final_preflight["blocking"]))

        staging_evidence = None
        if exception_session_id is not None:
            final_exception = final_preflight["exception"]
            final_boundary = final_exception["derived_boundary"]
            if int(args.expected_format_boundary) != int(final_boundary):
                raise OperationalError(
                    "[MIGRATION 015] Evidence boundary changed before apply")
            indexes = [
                row["chunk_index"] for row in final_exception["chunks"]
                if row["chunk_index"] >= final_boundary]
            staging_evidence = _inspect_container_format_staging(
                cfg, db, exception_session_id, indexes)
            if (staging_evidence["entry_count"]
                    or staging_evidence["unreadable_count"]):
                raise OperationalError(
                    "[MIGRATION 015] Final staging evidence is not empty and "
                    "readable")

        preflight["applied"] = db.apply_container_format_schema(
            exception_session_id=exception_session_id,
            expected_boundary=args.expected_format_boundary,
            approval_id=args.format_approval_id,
            approval_reason=args.format_approval_reason,
            staging_evidence=staging_evidence,
            require_archiver_lock=True)
        preflight["validation"] = db.validate_container_format_schema()
        preflight["final_locked_preflight"] = final_preflight
        preflight["staging_evidence"] = staging_evidence
    finally:
        db.close()
    _print_json(preflight)
    return 0


def _run_frontier_bootstrap(cfg, args, parser):
    """Dry-run / execute pair for the one-time frontier bootstrap (Task 4.2).

    Dry run by default and always read-only: it validates the scope
    configuration and the session's state and reports what would happen.
    ``--execute --yes`` performs the migration, which traverses the source
    read-only and creates persistent scope/directory/segment state.

    It never rewrites a chunk, never changes the chunk format, and never
    touches LTFS.
    """
    from src.frontier_bootstrap import FrontierBootstrap
    from src.scan_frontier import build_frontier_scanner_factory
    from src.skipped import SkippedFileTracker
    from src.ui import ConsoleUI
    import threading

    if not args.session_id or len(args.session_id) != 1:
        parser.error("--bootstrap-frontier requires exactly one --session-id")
    session_id = args.session_id[0]

    db = _open_db(cfg)
    try:
        bootstrap = FrontierBootstrap(
            db=db, session_id=session_id,
            scan_paths=cfg.remote_scan_paths,
            archive_root=cfg.local_manifest_archive_root,
            scanner_factory=build_frontier_scanner_factory(
                remote_user=cfg.remote_user, remote_host=cfg.remote_host,
                remote_password=cfg.remote_password,
                skipped_tracker=SkippedFileTracker(), ui=ConsoleUI(),
                timeout=cfg.ssh_command_timeout_seconds),
            stop_event=threading.Event(), source_host=cfg.remote_host,
            ui=ConsoleUI(),
            # Real liveness evidence. Without these the bootstrap's
            # quiescence gate would be told 'nothing is running' without
            # anything having looked.
            active_processes_probe=active_archive_processes,
            lock_holders_probe=lambda: archiver_lock_status(_conninfo(cfg)))
        if not args.execute:
            report = bootstrap.dry_run()
            report["note"] = ("dry run; nothing was created. Re-run with "
                              "--execute --yes to perform the migration.")
            _print_json(report)
            return 0
        if not args.yes:
            parser.error("--bootstrap-frontier --execute requires --yes")
        _print_json(bootstrap.execute(approved=True,
                                      conservative=args.conservative))
        return 0
    finally:
        db.close()


def _run_session_frontier_report(cfg, args, parser):
    """READ-ONLY frontier/membership report for one session (Task 4.1).

    Creates no state, changes no row, and never touches LTFS. Liveness is
    gathered here (the advisory lock and local archive processes) so the report
    can say "a worker may still be running" instead of assuming it is not.
    """
    from src.startup_reconcile import session_frontier_report

    if not args.session_id:
        parser.error("--session-frontier-report requires --session-id")

    try:
        holders = archiver_lock_status(_conninfo(cfg))
    except Exception:
        holders = None                       # unknown, not "none"
    try:
        processes = active_archive_processes()
    except Exception:
        processes = None

    db = _open_db(cfg)
    try:
        reports = [
            session_frontier_report(
                db, session_id,
                archive_root=cfg.local_manifest_archive_root,
                lock_holders=holders, active_processes=processes)
            for session_id in args.session_id
        ]
    finally:
        db.close()
    _print_json({"database": cfg.pg_dbname, "reports": reports})
    return 0


def _verify_hot_backup(cfg, path):
    restore_list = verify_backup_file(cfg, path)
    return {"backup_path": os.path.abspath(path),
            "restore_list_path": restore_list,
            "verified": True}


def _run_manifest_export(cfg, args):
    root = _require_maintenance_safe(cfg)
    if args.dry_run == args.execute:
        raise OperationalError(
            "--export-small-file-manifests requires exactly one of --dry-run "
            "or --execute")
    if args.dry_run:
        _print_json(dry_run_export(_conninfo(cfg)))
        return 0
    if not args.yes:
        raise OperationalError("--execute requires --yes")
    if not args.hot_backup_path:
        raise OperationalError("--execute requires --hot-backup-path")
    hot_backup = _verify_hot_backup(cfg, args.hot_backup_path)
    result = execute_export(
        _conninfo(cfg), root, args.hot_backup_path)
    result["hot_backup_verification"] = hot_backup
    _print_json(result)
    return 0


def _run_manifest_validate(cfg, args):
    if not args.heavy:
        raise OperationalError(
            "--validate-local-manifest-export requires --heavy")
    if args.export_id is None:
        raise OperationalError("--export-id is required")
    _require_maintenance_safe(cfg)
    _print_json(validate_export(_conninfo(cfg), args.export_id))
    return 0


def _run_manifest_search(cfg, args):
    root = validate_archive_root(
        cfg.local_manifest_archive_root, (cfg.staging_dir,))
    rows = search_manifests(
        root, args.manifest_search, limit=args.limit,
        allowed_paths=pruned_manifest_paths(_conninfo(cfg)))
    _print_json({"limit": args.limit, "rows": rows})
    return 0


def _run_manifest_prune(cfg, args):
    _require_maintenance_safe(cfg)
    if args.export_id is None:
        raise OperationalError("--export-id is required")
    if args.dry_run == args.execute:
        raise OperationalError(
            "--prune-exported-small-files requires exactly one of "
            "--dry-run or --execute")
    hot_backup = None
    if args.execute:
        if not args.yes:
            raise OperationalError("--execute requires --yes")
        if not args.hot_backup_path:
            raise OperationalError("--execute requires --hot-backup-path")
        hot_backup = _verify_hot_backup(cfg, args.hot_backup_path)
    result = prune_export(
        _conninfo(cfg), args.export_id,
        hot_backup_path=args.hot_backup_path,
        execute=args.execute,
        batch_size=args.prune_batch_size,
    )
    result["hot_backup_verification"] = hot_backup
    _print_json(result)
    return 0


def _run_legacy_cold_export(cfg, args):
    root = _require_maintenance_safe(cfg)
    if not args.execute or not args.yes:
        raise OperationalError(
            "--export-legacy-cold-db requires --execute --yes")
    if not args.legacy_cold_dsn or not args.cold_backup_path:
        raise OperationalError(
            "--legacy-cold-dsn and --cold-backup-path are required")
    _print_json(export_legacy_cold_database(
        args.legacy_cold_dsn, root, args.cold_backup_path))
    return 0


def _cleanup_session_data(db, assume_yes, cfg=None):
    try:
        summary = db.get_unreferenced_remote_data_summary()
        print("[DB] Unreferenced remote session data:")
        _print_json(summary)
        if summary['active_sessions']:
            # A row marked active is treated as live until proven otherwise —
            # that refusal never softens. Print the liveness evidence beside it
            # so the operator can tell a running archiver from a crashed
            # session that nobody ever reaped.
            if cfg is not None:
                _print_json({"liveness": liveness_evidence(
                    _conninfo(cfg), getattr(cfg, "backup_log_dir", None))})
            raise OperationalError(
                "[DB] Refusing cleanup while a remote session is active. "
                "If nothing is running, --reconcile-stale-sessions --dry-run "
                "shows whether those rows are stale and what they would "
                "become.")
        if not summary['plans'] and not summary['snapshots']:
            print("[DB] Nothing to clean.")
            return 0
        if not assume_yes:
            confirm = input(
                "Type CLEAN to delete only this unreferenced session data "
                "and compact the database: ").strip()
            if confirm != 'CLEAN':
                print("[ABORTED]")
                return 1
        result = db.cleanup_unreferenced_remote_data(compact=True)
        print("[DB] Cleanup and compaction complete:")
        _print_json(result)
        return 0
    finally:
        db.close()


def _run_backfill(db, args):
    try:
        if not args.dry_run and not args.execute:
            raise OperationalError(
                "--backfill-directory-catalog requires --dry-run or --execute")
        if args.dry_run and args.execute:
            raise OperationalError("Choose only one of --dry-run or --execute")
        mode = "dry-run" if args.dry_run else "execute"
        print(f"[DB] Directory catalog backfill ({mode}) on target database...")
        result = db.backfill_directory_catalog_from_files_index(
            tape_label=args.tape,
            dry_run=args.dry_run,
            batch_size=args.batch_size,
            progress=True,
        )
        _print_json(result)
        return 0
    finally:
        db.close()


def _build_parser():
    parser = argparse.ArgumentParser(
        description="Inspect and safely maintain the PostgreSQL archive catalog.")
    parser.add_argument("--db", help="Override target database name.")
    parser.add_argument("--tape", help="Limit an operation to one tape label.")
    parser.add_argument("--batch-size", type=int, default=100,
                        help="Directory backfill bundle batch size.")
    parser.add_argument("--yes", action="store_true",
                        help="Skip supported interactive confirmations.")
    parser.add_argument("--print-db-target", action="store_true",
                        help="Print configured target and read-only DB identity.")
    parser.add_argument("--backup-postgres", action="store_true",
                        help="Create and verify a custom-format PostgreSQL dump.")
    parser.add_argument("--create-migrated-db", action="store_true",
                        help="Create, restore, and schema-migrate a new DB.")
    parser.add_argument("--backup-file",
                        help="Backup dump path for --create-migrated-db.")
    parser.add_argument("--new-db",
                        help="Explicit DB name for --create-migrated-db.")
    parser.add_argument("--apply-directory-catalog-schema", action="store_true",
                        help="Apply scripts/sql/007 to the selected DB.")
    parser.add_argument("--bootstrap-frontier", action="store_true",
                        help="One-time migration of a session onto the "
                             "incremental frontier. Dry run unless --execute "
                             "--yes; traverses the source READ-ONLY and never "
                             "rewrites a chunk.")
    parser.add_argument("--all-session-health", action="store_true",
                        help="READ-ONLY health classification for EVERY "
                             "session. Creates no state, touches no LTFS.")
    parser.add_argument("--conservative", action="store_true",
                        help="With --bootstrap-frontier --execute: the "
                             "STRUCTURE-ONLY migration. Creates scope rows "
                             "and queues each root pending; lists no "
                             "directory, imports no membership and never "
                             "marks the scan complete. This is the correct "
                             "shape for a session whose historical scan "
                             "never finished.")
    parser.add_argument("--session-frontier-report", action="store_true",
                        help="READ-ONLY frontier/membership report for one or "
                             "more sessions (--session-id). Creates no state "
                             "and never touches LTFS.")
    parser.add_argument("--apply-incremental-scan-schema", action="store_true",
                        help="Migration 014 (incremental scan frontier). "
                             "Read-only preflight unless --execute --yes "
                             "--backup-file are all given; add --finalize for "
                             "the audit + final constraints.")
    parser.add_argument("--finalize", action="store_true",
                        help="With --apply-incremental-scan-schema: also apply "
                             "the legacy membership audit and the final unique "
                             "constraints. Refuses on duplicate ordinals.")
    parser.add_argument("--apply-container-format-schema", action="store_true",
                        help="Migration 015. Read-only preflight unless "
                             "--execute --yes --backup-file are supplied.")
    parser.add_argument("--validate-container-format-schema",
                        action="store_true",
                        help="Fail-closed read-only validation of migration 015.")
    parser.add_argument("--container-format-schema-report",
                        action="store_true",
                        help="Read-only migration-015 schema/format report.")
    parser.add_argument("--session37-boundary-rehearsal", action="store_true",
                        help="READ-ONLY Plan-2 Gate-5.5 chunk classification "
                             "and proposed Stored TAR boundary. Defaults to "
                             "session 37; override with --session-id.")
    parser.add_argument("--stored-tar-exception-session-id", type=int,
                        help="Individually approved legacy mixed-format session "
                             "for the migration-015 evidence gate.")
    parser.add_argument("--expected-format-boundary", type=int,
                        help="Expected first Stored TAR chunk index; execute "
                             "refuses if the evidence-derived value differs.")
    parser.add_argument("--format-approval-id",
                        help="Durable operator/review approval identifier for "
                             "the legacy-session exception.")
    parser.add_argument("--format-approval-reason",
                        help="Durable explanation for the approved exception.")
    parser.add_argument("--backfill-directory-catalog", action="store_true",
                        help="Backfill directory catalog from legacy files_index.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Report backfill work without writing.")
    parser.add_argument("--execute", action="store_true",
                        help="Execute the explicit backfill.")
    parser.add_argument("--heavy", action="store_true",
                        help="Allow heavy explicit validation commands.")
    parser.add_argument("--validate-directory-catalog", action="store_true",
                        help="Print read-only directory catalog validation.")
    parser.add_argument("--compare-db", help="Source DB for row-count comparison.")
    parser.add_argument("--with-db", help="Target DB for row-count comparison.")
    parser.add_argument("--cleanup-session-data", action="store_true",
                        help="Clean unreferenced remote session data.")
    parser.add_argument("--manifest-status", action="store_true",
                        help="Print recent local-manifest exports.")
    parser.add_argument("--export-small-file-manifests", action="store_true",
                        help="Export terminal small-file rows to local manifests.")
    parser.add_argument("--validate-local-manifest-export",
                        action="store_true",
                        help="Hash and exactly validate a local manifest export.")
    parser.add_argument("--manifest-search",
                        help="Search permanent local small-file manifests.")
    parser.add_argument("--prune-exported-small-files",
                        action="store_true",
                        help="Prune only a validated immutable export snapshot.")
    parser.add_argument("--export-id", type=int,
                        help="Local manifest export id for validation/pruning.")
    parser.add_argument("--export-legacy-cold-db", action="store_true",
                        help="One-time read-only export before cold DB retirement.")
    parser.add_argument("--legacy-cold-dsn",
                        help="Explicit DSN for the legacy cold database.")
    parser.add_argument("--hot-backup-path",
                        help="Verified hot DB backup path.")
    parser.add_argument("--cold-backup-path",
                        help="Verified legacy cold DB backup path.")
    parser.add_argument("--limit", type=int, default=100,
                        help="Result limit for local-manifest search.")
    parser.add_argument("--prune-batch-size", type=int, default=100000,
                        help="Maximum files_index rows committed per prune batch.")
    parser.add_argument("--session-forensics", action="store_true",
                        help="Read-only evidence for every active session.")
    parser.add_argument("--reconcile-stale-sessions", action="store_true",
                        help="Move provably-dead active sessions to a "
                             "terminal status (needs --dry-run or --execute).")
    parser.add_argument("--session-id", type=int, action="append",
                        help="Limit reconciliation to this session id "
                             "(repeatable).")
    parser.add_argument("--idle-seconds", type=int,
                        default=DEFAULT_IDLE_SECONDS,
                        help="Silence required before a session may be "
                             "called stale.")
    return parser


def _run_session_forensics(cfg, args):
    _print_json(session_forensics(
        _conninfo(cfg), idle_seconds=args.idle_seconds,
        log_dir=getattr(cfg, "backup_log_dir", None)))
    return 0


def _run_reconcile_sessions(cfg, args):
    if args.dry_run == args.execute:
        raise OperationalError(
            "--reconcile-stale-sessions requires exactly one of --dry-run "
            "or --execute")
    if args.execute and not args.yes:
        raise OperationalError("--execute requires --yes")
    result = reconcile_stale_remote_sessions(
        _conninfo(cfg), execute=args.execute,
        idle_seconds=args.idle_seconds, session_ids=args.session_id,
        log_dir=getattr(cfg, "backup_log_dir", None))
    print(format_report(result))
    _print_json(result)
    return 0


def _dispatch(parser, args):
    cfg = _config(args)

    if args.print_db_target:
        payload = {
            "configured_target": cfg.db_display_ref,
            "identity": describe_database(_conninfo(cfg)),
            "archiver_lock_holders": archiver_lock_status(_conninfo(cfg)),
        }
        _print_json(payload)
        return 0

    if args.backup_postgres:
        print(f"[DB BACKUP] Target: {cfg.db_display_ref}")
        if active_archive_processes():
            raise OperationalError(
                "[DB BACKUP] Refusing while archive/transfer processes run")
        lock_db = _open_no_init_db(cfg)
        try:
            lock_db.acquire_archiver_lock()
            if active_archive_processes():
                raise OperationalError(
                    "[DB BACKUP] A transfer process appeared after the "
                    "archiver lock was acquired")
            _print_json(create_verified_production_backup(cfg))
        finally:
            lock_db.close()
        return 0

    if args.create_migrated_db:
        if not args.backup_file:
            parser.error("--create-migrated-db requires --backup-file")
        target_db = create_migrated_database_from_backup(
            cfg, args.backup_file, dbname=args.new_db)
        _print_json({"migrated_database": target_db})
        return 0

    if args.apply_directory_catalog_schema:
        apply_directory_catalog_schema_to_database(cfg, cfg.pg_dbname)
        _print_json({
            "database": cfg.pg_dbname,
            "schema": "directory_catalog",
            "applied": True,
        })
        return 0

    if args.all_session_health:
        return _run_all_session_health(cfg)

    if args.session_frontier_report:
        return _run_session_frontier_report(cfg, args, parser)

    if args.bootstrap_frontier:
        return _run_frontier_bootstrap(cfg, args, parser)

    if args.apply_incremental_scan_schema:
        return _apply_incremental_scan_schema(cfg, args, parser)

    if args.apply_container_format_schema:
        return _apply_container_format_schema(cfg, args, parser)

    if args.validate_container_format_schema:
        return _run_container_format_schema_report(cfg, validate=True)

    if args.container_format_schema_report:
        return _run_container_format_schema_report(cfg, validate=False)

    if args.session37_boundary_rehearsal:
        return _run_session37_boundary_rehearsal(cfg, args)

    if args.validate_directory_catalog:
        _print_json(validate_directory_catalog(_conninfo(cfg)))
        return 0

    if args.compare_db or args.with_db:
        if not args.compare_db or not args.with_db:
            parser.error("--compare-db requires --with-db")
        source = _conninfo(cfg, dbname=args.compare_db)
        target = _conninfo(cfg, dbname=args.with_db)
        _print_json(compare_databases(source, target))
        return 0

    if args.session_forensics:
        return _run_session_forensics(cfg, args)

    if args.reconcile_stale_sessions:
        return _run_reconcile_sessions(cfg, args)

    if args.cleanup_session_data:
        return _cleanup_session_data(_open_db(cfg), assume_yes=args.yes, cfg=cfg)

    if args.backfill_directory_catalog:
        return _run_backfill(_open_db(cfg), args)

    if args.manifest_status:
        _print_json(export_status(_conninfo(cfg)))
        return 0

    if args.export_small_file_manifests:
        return _run_manifest_export(cfg, args)

    if args.validate_local_manifest_export:
        return _run_manifest_validate(cfg, args)

    if args.manifest_search:
        return _run_manifest_search(cfg, args)

    if args.prune_exported_small_files:
        return _run_manifest_prune(cfg, args)

    if args.export_legacy_cold_db:
        return _run_legacy_cold_export(cfg, args)

    db = _open_db(cfg)
    from src.db_inspector_qt import run_qt_inspector
    try:
        return run_qt_inspector(db, cfg.db_dsn, display_ref=cfg.db_display_ref)
    finally:
        db.close()


def main(argv=None):
    """Dispatch, turning deliberate refusals into one readable line.

    Only :class:`OperationalError` is caught, and only here. It is raised
    exclusively where a command decides *not* to do something — a held archiver
    lock, a running transfer, a missing required flag. Every other exception
    propagates untouched so its traceback still reaches the operator: catching
    ``Exception`` here would swallow the ``KeyError`` from a renamed column and
    report it as if the tool had refused on purpose.
    """
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        return _dispatch(parser, args)
    except OperationalError as exc:
        print(f"{exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
