"""Root runner — launch the LTO Archive Management System CLI.

The application code lives in the ``src/`` package; data files (config.ini,
.env, backup_logs/) live here in the project root. We chdir to
the root so a relative ``config.ini`` resolves exactly as it did before the
package split.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

if __name__ == "__main__" and "--version" in sys.argv[1:]:
    # Answered before anything else is imported: no config.ini is read or
    # created, no database is opened and no database driver is needed.
    from src import __version__
    print(f"owc-lto8-archiver {__version__}")
    raise SystemExit(0)

from src.constants import PROJECT_ROOT
os.chdir(PROJECT_ROOT)

from src.cli import main, run_remote_archiver_headless
from src.config import ConfigManager
from src.db import create_database_manager
from src.exit_codes import ExitCode
from src.logsetup import configure_file_logging, get_logger
from src.db_backup import backup_noun, create_database_backup
from src.tape_reset import execute_reset, prepare_impact_report


_REMOVED_SQLITE_FLAGS = {
    "--optimize-db",
    "--catalog-v3-preflight",
    "--catalog-v3-migrate",
    "--catalog-rebuild",
    "--hashless-origin-migrate",
    "--dry-run",
}


def _run_headless_remote_archive(argv):
    """`python run.py remote-archive [--resume] [--non-interactive]`.

    A promptless, menu-less launch for a stable manual/detached run. It never
    reads stdin, so a closed stdin cannot crash it; it exits with a stable code
    (see src/exit_codes.py). Absence of this subcommand leaves the interactive
    menu (``main()``) completely unchanged.
    """
    import argparse
    parser = argparse.ArgumentParser(
        prog="run.py remote-archive",
        description="Headless remote archive to LTO tape (no interactive menu).")
    parser.add_argument(
        "--resume", action="store_true",
        help="Resume the single active session for the configured host/path.")
    parser.add_argument(
        "--non-interactive", action="store_true",
        help="Never prompt (implied by this subcommand; accepted for clarity).")
    args = parser.parse_args(argv)

    cfg = ConfigManager()
    configure_file_logging(cfg.backup_log_dir)
    db = create_database_manager(cfg)
    try:
        code = run_remote_archiver_headless(cfg, db, resume=args.resume)
    finally:
        try:
            db.close()
        except Exception:
            pass
    raise SystemExit(code)


def _run_tape_reset(argv):
    """Explicit destructive reset; deliberately unavailable from menus.

    Two modes, never mixed:

    * ``--prepare-impact-report OUT`` only reads (the catalog, the IBM LTFS
      log, the label of the mounted cartridge) and writes the report file.
    * ``--impact-report FILE --delete-catalog --yes`` is the destructive
      reset that such a report authorises.
    """
    import argparse
    import json
    parser = argparse.ArgumentParser(
        prog="run.py tape-reset",
        description="Destructively reset one mounted tape generation, or "
                    "prepare the impact report that such a reset requires.")
    parser.add_argument("--tape", required=True,
                        help="Label of the mounted cartridge.")
    parser.add_argument(
        "--impact-report",
        help="Impact report that authorises the reset (written by "
             "--prepare-impact-report).")
    parser.add_argument(
        "--prepare-impact-report", metavar="OUT",
        help="READ-ONLY: write the impact report for --tape to the new file "
             "OUT and stop. Nothing is formatted, ejected or deleted. "
             "Needs --backup-file.")
    parser.add_argument(
        "--backup-file",
        help="With --prepare-impact-report: the catalog backup the report "
             "pins (python run.py --backup-db, or inspect_db.py "
             "--backup-postgres). It is verified first.")
    parser.add_argument(
        "--scratch-host",
        help="With --prepare-impact-report on PostgreSQL: server on which "
             "the backup is restored into a new scratch database to prove "
             "it restores; the scratch database is dropped afterwards.")
    parser.add_argument("--scratch-port",
                        help="Port of the scratch server.")
    parser.add_argument(
        "--scratch-user",
        help="User on the scratch server (password: PGPASSWORD or pgpass).")
    parser.add_argument(
        "--scratch-docker-container",
        help="Docker container whose pg_restore matches the scratch server "
             "(optional; otherwise the client tools on PATH are used).")
    parser.add_argument("--delete-catalog", action="store_true",
                        help="Required for the destructive reset.")
    parser.add_argument("--yes", action="store_true",
                        help="Required for the destructive reset.")
    parser.add_argument("--resume-operation",
                        help="Resume an interrupted reset by operation id.")
    args = parser.parse_args(argv)
    prepare_only = ("backup_file", "scratch_host", "scratch_port",
                    "scratch_user", "scratch_docker_container")
    if args.prepare_impact_report:
        mixed = [name for name in ("impact_report", "delete_catalog", "yes",
                                   "resume_operation") if getattr(args, name)]
        if mixed:
            parser.error(
                "--prepare-impact-report only reads; it cannot be combined "
                "with " + ", ".join(
                    "--" + name.replace("_", "-") for name in mixed))
        if not args.backup_file:
            parser.error("--prepare-impact-report requires --backup-file")
    else:
        if not args.impact_report:
            parser.error(
                "one of --impact-report or --prepare-impact-report is "
                "required")
        stray = [name for name in prepare_only if getattr(args, name)]
        if stray:
            parser.error(
                ", ".join("--" + name.replace("_", "-") for name in stray)
                + " belong to --prepare-impact-report")
    cfg = ConfigManager()
    configure_file_logging(cfg.backup_log_dir)
    if args.prepare_impact_report:
        scratch = None
        if args.scratch_host or args.scratch_port or args.scratch_user:
            from src.pg_backup import scratch_server
            scratch = scratch_server(
                host=args.scratch_host, port=args.scratch_port,
                user=args.scratch_user,
                docker_container=args.scratch_docker_container)
        result = prepare_impact_report(
            cfg, tape_label=args.tape,
            output_path=args.prepare_impact_report,
            backup_path=args.backup_file, scratch_server=scratch)
        print(json.dumps(result, indent=2, default=str))
        return
    db = create_database_manager(cfg)
    try:
        result = execute_reset(
            db, cfg, tape_label=args.tape,
            impact_path=args.impact_report,
            delete_catalog=args.delete_catalog, confirmed=args.yes,
            resume_operation=args.resume_operation)
        print(json.dumps(result, indent=2, default=str))
    finally:
        db.close()


if __name__ == "__main__":
    try:
        if len(sys.argv) > 1 and sys.argv[1] == "remote-archive":
            _run_headless_remote_archive(sys.argv[2:])

        if len(sys.argv) > 1 and sys.argv[1] == "tape-reset":
            _run_tape_reset(sys.argv[2:])
            raise SystemExit(0)

        if "--backup-db" in sys.argv:
            cfg = ConfigManager()
            configure_file_logging(cfg.backup_log_dir)
            path = create_database_backup(cfg)
            print(f"[DB BACKUP] {backup_noun(cfg)} created: {path}")
            raise SystemExit(0)

        removed = sorted(_REMOVED_SQLITE_FLAGS.intersection(sys.argv))
        if removed:
            raise RuntimeError(
                "These legacy maintenance flags were removed long ago. The "
                "catalog schema is managed by the application (SQLite) or "
                "by scripts/sql/*.sql (PostgreSQL). Unsupported flag(s): "
                f"{', '.join(removed)}"
            )
        # The menu returns the exit code of its last failed action (0 when
        # nothing failed), so a failure the operator saw on screen is also
        # visible to whatever launched the process.
        raise SystemExit(int(main() or 0))
    except SystemExit:
        raise
    except RuntimeError as e:
        # Operator-facing errors are raised as RuntimeError with a readable
        # message; the full traceback goes to backup_logs/archiver.log, and
        # LTO_DEBUG=1 re-raises it onto the console for bug reports.
        get_logger().exception("run.py stopped on error")
        if os.environ.get('LTO_DEBUG'):
            raise
        print(f"\n{e}")
        # A printed error is still a failure: never fall off the end with 0
        # (a failed --backup-db, or a headless run that could not reach the
        # database, used to look like success to a scheduled task).
        raise SystemExit(int(ExitCode.FATAL_CONFIG))
    except KeyboardInterrupt:
        print("\n\n[ABORTED] User stopped the script.")
        raise SystemExit(int(ExitCode.USER_STOP))
