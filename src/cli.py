"""Main menu, archiver entry points, DB management submenu."""
import os
from typing import TYPE_CHECKING

from .config import ConfigManager
from .constants import CONFIG_FILE
from .db import _fmt_ts, create_database_manager
from .exit_codes import (ExitCode, StopResult, REASON_BAD_CONFIG,
                         REASON_WINDOWS_REBOOT_PENDING,
                         REASON_USER_REQUESTED_STOP)
from .logsetup import configure_file_logging, get_logger
from .ltfs import TapeManager
from .orchestrators import LocalOrchestrator, RemoteOrchestrator
from .pg_backup import create_database_backup
from .reporting import generate_backup_summary
from .retriever import LTORetriever
from .robocopy import _prepare_robocopy_exclusion, _remove_robocopy_exclusion
from .remote_transport import _cleanup_askpass_helpers
from .runtime import _terminate_all_procs, install_cancel_handler, reset_cancel, uninstall_cancel_handler, unpin_current_process
from .windows_update_guard import (managed_update_policy, pause_windows_updates,
                                   pending_reboot_reasons, print_guard_status,
                                   restore_stale_guard, resume_windows_updates)

if TYPE_CHECKING:
    from .pg_db import PgDatabaseManager


def _start_windows_update_guard(cfg: ConfigManager):
    """Move the Windows Update window outside this run.

    Returns ``(proceed, applied)``. ``proceed`` is False when a restart is
    already staged and the operator has not opted to run into it; ``applied``
    tells the caller whether to call resume_windows_updates() in its finally.
    """
    if not cfg.windows_update_guard:
        return True, False

    # Undo a pause a force-killed run left behind before installing a new one.
    restore_stale_guard()

    reasons = pending_reboot_reasons()
    if reasons:
        print("\n[WU] Windows has a restart pending:")
        for reason in reasons:
            print(f"  - {reason}")
        print("[WU] Pausing updates cannot cancel an already-staged restart. "
              "A restart during a tape write corrupts the LTFS index and "
              "loses every chunk written so far — this cost ~126 GB of "
              "Tape_02 on 2026-07-15.")
        if cfg.windows_update_block_on_pending_reboot:
            print("[WU] Reboot this host first, then start the run. To "
                  "override, set [WINDOWS_UPDATE] block_on_pending_reboot "
                  "= false in config.ini.")
            return False, False
        print("[WU] block_on_pending_reboot = false — proceeding anyway.")

    applied = pause_windows_updates(cfg.windows_update_pause_days)
    # The pause can be written successfully and still be ignored by a managed
    # host's Windows Update Agent, so report what protection actually exists
    # rather than the fact that the registry write returned success.
    print_guard_status(applied, managed_update_policy())
    return True, applied


def run_archiver(cfg: ConfigManager, db: "PgDatabaseManager"):
    # Cross-process single-writer guard: the in-process tape I/O lock cannot
    # stop a second `python run.py` instance from interleaving tape writes.
    try:
        db.acquire_archiver_lock()
    except RuntimeError as e:
        print(str(e))
        return

    proceed, wu_guard = _start_windows_update_guard(cfg)
    if not proceed:
        db.release_archiver_lock()
        return

    added_exclusion = _prepare_robocopy_exclusion()
    reset_cancel()
    install_cancel_handler()
    print("[LOCAL] Press Ctrl+C at any time to stop safely "
          "(the session is saved and can be resumed).")
    try:
        LocalOrchestrator(cfg, db).run()
    except RuntimeError as e:
        get_logger().exception("local archive run stopped")
        print(str(e))
    except KeyboardInterrupt:
        print("\n[LOCAL] Interrupted. Session state saved — re-run to resume.")
    finally:
        # Mirror run_remote_archiver: make sure no robocopy child survives,
        # restore CPU affinity and default Ctrl+C behaviour, then drop the
        # robocopy Defender exclusion.
        _terminate_all_procs()
        unpin_current_process()
        uninstall_cancel_handler()
        reset_cancel()
        _cleanup_askpass_helpers()
        if added_exclusion:
            _remove_robocopy_exclusion()
        if wu_guard:
            resume_windows_updates()
        db.release_archiver_lock()


def run_remote_archiver(cfg: ConfigManager, db: "PgDatabaseManager",
                        non_interactive=False, resume=False):
    """Menu option 6 / headless entry: fetch from a remote host, archive to LTO.

    Returns a :class:`StopResult`. Interactive callers use it only to preserve
    the outcome across a subsequent stdin-closed menu prompt; the headless
    wrapper maps ``result.exit_code`` to the process exit code. This function
    owns the shared setup/teardown (advisory lock, Windows Update guard,
    robocopy exclusion, cancel handler) for BOTH paths, so headless and
    interactive can never diverge on cleanup.
    """
    if not cfg.remote_host or not cfg.remote_user or not cfg.remote_path:
        print("\n[REMOTE] The [REMOTE] section in config.ini is incomplete.")
        print("  Required: remote_host, remote_user, remote_path")
        print("  Optional: remote_password, staging_fill_pct  (default 0.80)")
        cfg_abs = os.path.abspath(CONFIG_FILE)
        print(f"\n[INFO] Config path: {cfg_abs}")
        # Opening the file is an interactive convenience; never in headless mode.
        if os.name == 'nt' and not non_interactive:
            os.startfile(cfg_abs)
        return StopResult(
            exit_code=ExitCode.FATAL_CONFIG, reason=REASON_BAD_CONFIG,
            resumable=False, source="cli",
            detailed_reason="[REMOTE] config section incomplete")

    try:
        db.acquire_archiver_lock()
    except RuntimeError as e:
        print(str(e))
        return StopResult(
            exit_code=ExitCode.FATAL_CONFIG, reason=REASON_BAD_CONFIG,
            resumable=False, source="cli",
            detailed_reason=f"could not acquire archiver lock: {e}")

    proceed, wu_guard = _start_windows_update_guard(cfg)
    if not proceed:
        db.release_archiver_lock()
        # A restart is already staged: transient and self-clearing once the host
        # reboots. Resumable, not a config error.
        return StopResult(
            exit_code=ExitCode.TRANSIENT_RESUMABLE,
            reason=REASON_WINDOWS_REBOOT_PENDING, resumable=True, source="cli",
            detailed_reason="a Windows/SCCM restart is already staged")

    added_exclusion = _prepare_robocopy_exclusion()
    reset_cancel()
    install_cancel_handler()
    print("[REMOTE] Press Ctrl+C at any time to stop safely "
          "(the session is saved and can be resumed).")
    result = None
    try:
        result = RemoteOrchestrator(cfg, db).run(
            non_interactive=non_interactive, resume=resume)
    except RuntimeError as e:
        get_logger().exception("remote archive run stopped")
        print(str(e))
        result = StopResult(
            exit_code=ExitCode.FATAL_CONFIG, reason=REASON_BAD_CONFIG,
            resumable=False, source="cli", detailed_reason=str(e))
    except KeyboardInterrupt:
        print("\n[REMOTE] Interrupted. Session state saved — re-run to resume.")
        result = StopResult(
            exit_code=ExitCode.USER_STOP, reason=REASON_USER_REQUESTED_STOP,
            resumable=True, source="cli", detailed_reason="KeyboardInterrupt")
    finally:
        # Make sure no fetch/tape child survives, restore CPU affinity and the
        # default Ctrl+C behaviour, then drop the robocopy Defender exclusion.
        _terminate_all_procs()
        unpin_current_process()
        uninstall_cancel_handler()
        reset_cancel()
        _cleanup_askpass_helpers()
        if added_exclusion:
            _remove_robocopy_exclusion()
        if wu_guard:
            resume_windows_updates()
        db.release_archiver_lock()
    if result is None:
        result = StopResult(
            exit_code=ExitCode.USER_STOP, reason=REASON_USER_REQUESTED_STOP,
            resumable=True, source="cli")
    return result


def run_remote_archiver_headless(cfg: ConfigManager, db: "PgDatabaseManager",
                                 resume=True):
    """Headless launcher for a stable manual/detached run. No menu, no input().

    Reuses ``run_remote_archiver``'s setup/teardown and returns an integer exit
    code (see :class:`ExitCode`). It never returns to a menu, so a closed stdin
    can never turn into an ``EOFError`` traceback here.
    """
    result = run_remote_archiver(cfg, db, non_interactive=True, resume=resume)
    return int(getattr(result, "exit_code", ExitCode.FATAL_CONFIG))


def _print_tapes_table(db):
    tapes = db.list_tapes()
    if not tapes:
        print("[DB] No tapes registered.")
        return tapes
    BAR_W = 20
    print(f"\n{'ID':>4}  {'Volume Label':<25}  {'Initialized':<19}  Space")
    print("-" * 80)
    for t in tapes:
        date_s  = _fmt_ts(t['date_formatted'])
        # total_capacity is stored in decimal GB (see DEFAULT_TAPE_CAPACITY_GB);
        # convert used bytes with the same base so the bar compares like units.
        cap_gb  = t['total_capacity']
        used_b  = t['used_space'] or 0
        used_gb = used_b / 1000**3
        if cap_gb:
            pct     = min(used_gb / cap_gb, 1.0)
            filled  = round(pct * BAR_W)
            bar     = '█' * filled + '░' * (BAR_W - filled)
            space_s = f"[{bar}] {pct*100:.1f}%  {used_gb:.1f}/{cap_gb:.0f} GB"
        else:
            space_s = f"{used_gb:.1f} GB used  (no capacity set)"
        print(f"{t['tape_id']:>4}  {t['volume_label']:<25}  {date_s:<19}  {space_s}")
    return tapes


def _db_management_menu(db):
    while True:
        print("\n--- Database Management ---")
        print("  1. Delete tape & all file records")
        print("  2. Delete single file record by ID")
        print("  3. Rename tape label")
        print("  4. Set tape capacity (GB)")
        print("  5. Recalculate tape used space")
        print("  6. Wipe file records for tape (keep tape entry)")
        print("  0. Back")
        print("-" * 40)
        sub = input("Choose: ").strip()

        if sub == '0':
            break

        elif sub == '1':
            tapes = _print_tapes_table(db)
            if not tapes:
                continue
            label = input("Enter volume label to DELETE (tape + all file records): ").strip()
            if not db.tape_exists(label):
                print(f"[ERROR] Tape '{label}' not found.")
                continue
            confirm = input(f"Type 'yes' to permanently delete tape '{label}' and ALL its file records: ").strip()
            if confirm.lower() == 'yes':
                db.delete_tape(label)
            else:
                print("[ABORTED]")

        elif sub == '2':
            file_id_s = input("Enter file ID to delete: ").strip()
            if not file_id_s.isdigit():
                print("[ERROR] Invalid file ID.")
                continue
            file_id = int(file_id_s)
            rec = db.get_file_by_id(file_id)
            if not rec:
                print(f"[ERROR] No file record with ID {file_id}.")
                continue
            print(f"\n  ID:        {rec['file_id']}")
            print(f"  Name:      {rec['file_name']}")
            print(f"  Path:      {rec['original_path']}")
            print(f"  Size:      {rec['file_size_bytes']:,} bytes")
            print(f"  Tape:      {rec['tape_label']}")
            print(f"  Backed up: {_fmt_ts(rec['backup_date'])}")
            confirm = input("Type 'yes' to delete this record: ").strip()
            if confirm.lower() == 'yes':
                db.delete_file(file_id)
                print(f"[DB] File record {file_id} deleted.")
            else:
                print("[ABORTED]")

        elif sub == '3':
            tapes = _print_tapes_table(db)
            if not tapes:
                continue
            old_label = input("Enter current volume label: ").strip()
            if not db.tape_exists(old_label):
                print(f"[ERROR] Tape '{old_label}' not found.")
                continue
            new_label = input("Enter new volume label: ").strip()
            if not new_label:
                print("[ERROR] New label cannot be empty.")
                continue
            if db.tape_exists(new_label):
                print(f"[ERROR] Label '{new_label}' already exists.")
                continue
            db.rename_tape(old_label, new_label)

        elif sub == '4':
            tapes = _print_tapes_table(db)
            if not tapes:
                continue
            label = input("Enter volume label: ").strip()
            if not db.tape_exists(label):
                print(f"[ERROR] Tape '{label}' not found.")
                continue
            cap_s = input("Enter capacity in GB (e.g. 12000): ").strip()
            try:
                cap_gb = float(cap_s)
            except ValueError:
                print("[ERROR] Invalid number.")
                continue
            db.update_tape_capacity(label, cap_gb)

        elif sub == '5':
            tapes = _print_tapes_table(db)
            if not tapes:
                continue
            label = input("Enter volume label: ").strip()
            if not db.tape_exists(label):
                print(f"[ERROR] Tape '{label}' not found.")
                continue
            tape_row = next((t for t in tapes if t['volume_label'] == label), None)
            old_used = (tape_row['used_space'] or 0) if tape_row else 0
            new_used = db.recalculate_tape_used_space(label)
            # Decimal GB, matching _print_tapes_table (capacity is decimal GB).
            print(f"[DB] Used space updated: {old_used/1000**3:.2f} GB → {new_used/1000**3:.2f} GB")

        elif sub == '6':
            tapes = _print_tapes_table(db)
            if not tapes:
                continue
            label = input("Enter volume label to wipe file records for: ").strip()
            if not db.tape_exists(label):
                print(f"[ERROR] Tape '{label}' not found.")
                continue
            count = db.count_tape_file_records(label)
            confirm = input(f"Type 'yes' to delete {count} file record(s) for '{label}' (tape entry kept): ").strip()
            if confirm.lower() == 'yes':
                db.delete_files_for_tape(label)
            else:
                print("[ABORTED]")

        else:
            print("[ERROR] Invalid selection.")


def main():
    os.system('cls' if os.name == 'nt' else 'clear')
    print("=" * 60)
    print("   LTO ARCHIVE MANAGEMENT SYSTEM")
    print("=" * 60)

    cfg       = ConfigManager()
    # Diagnostic trace file (backup_logs/archiver.log); console UX unchanged.
    configure_file_logging(cfg.backup_log_dir)
    db        = create_database_manager(cfg)
    tape_mgr  = TapeManager(db, cfg.lto_drive, cfg.ibm_eject_cmd)
    retriever = LTORetriever(
        db, cfg.lto_drive, cfg.staging_dir, cfg.restore_dir,
        manifest_archive_root=cfg.local_manifest_archive_root)

    # The last terminal result from a sub-flow (e.g. the remote archiver). If
    # stdin closes at the menu prompt after a sub-flow already produced a
    # result, that result's exit code is preserved rather than replaced by a
    # generic EOF code — the original failure (a network stop, say) wins.
    last_result = None
    while True:
        print("\n" + "=" * 60)
        print("  MAIN MENU")
        print("=" * 60)
        print("  1. Archive   — Backup files to LTO tape")
        print("  2. Retrieve  — Search database & restore files")
        print("  3. Tape Maintenance — Format / Register tapes")
        print("  4. List Registered Tapes")
        print("  5. Open config.ini")
        print("  6. Remote Archive — Fetch from remote host & backup to LTO")
        print("  7. Database Management — Edit / delete tape & file records")
        print("  8. Backup Summary — Ensure backup_logs/SUMMARY.csv report")
        print("  9. Database Backup — Dump PostgreSQL catalog to db_backups")
        print("  0. Exit")
        print("-" * 60)

        try:
            choice = input("Choose: ").strip()
        except EOFError:
            # stdin is closed (e.g. a detached run). Exit cleanly with no
            # traceback. If a sub-flow already produced a terminal result,
            # preserve its exit code; otherwise a neutral clean exit.
            code = int(getattr(last_result, "exit_code", 0)) if last_result else 0
            reason = getattr(last_result, "reason", None)
            print("\n[MENU] Standard input closed; exiting cleanly"
                  + (f" (last result: {reason})." if reason else "."))
            get_logger().warning(
                "menu stdin closed (EOF); exiting with code %s (reason=%s)",
                code, reason)
            db.close()
            raise SystemExit(code)

        if choice == '1':
            run_archiver(cfg, db)

        elif choice == '2':
            added_exclusion = _prepare_robocopy_exclusion()
            # Same cancel discipline as the archive flows: without a handler,
            # Ctrl+C would exit the menu while a registered robocopy child
            # kept reading the tape unsupervised.
            reset_cancel()
            install_cancel_handler()
            try:
                retriever.run()
            except RuntimeError as e:
                # e.g. tape-verify cancelled — return to the menu, don't exit.
                get_logger().exception("restore run stopped")
                print(str(e))
            except KeyboardInterrupt:
                print("\n[RESTORE] Interrupted.")
            finally:
                _terminate_all_procs()
                uninstall_cancel_handler()
                reset_cancel()
                if added_exclusion:
                    _remove_robocopy_exclusion()

        elif choice == '3':
            print("\n--- Tape Maintenance ---")
            print("1. Format tape        (LtfsCmdFormat.exe — ERASES ALL DATA)")
            print("2. Register tape manually")
            print("3. List available drives")
            print("4. Check tape         (LtfsCmdCheck.exe — repair filesystem errors)")
            print("5. Tape drives info   (LtfsCmdDrives.exe — list drives & status)")
            print("6. Eject tape         (LtfsCmdEject.exe — safely eject tape)")
            print("0. Back")
            sub = input("Choose: ").strip()
            if sub == '1':
                tape_mgr.format_tape()
            elif sub == '2':
                tape_mgr.register_tape()
            elif sub == '3':
                tape_mgr.list_drives()
            elif sub == '4':
                tape_mgr.check_tape()
            elif sub == '5':
                tape_mgr.tape_info()
            elif sub == '6':
                tape_mgr.eject_tape()

        elif choice == '4':
            _print_tapes_table(db)

        elif choice == '5':
            cfg_abs = os.path.abspath(CONFIG_FILE)
            print(f"\n[INFO] Config path: {cfg_abs}")
            if os.name == 'nt':
                os.startfile(cfg_abs)

        elif choice == '6':
            last_result = run_remote_archiver(cfg, db)

        elif choice == '7':
            _db_management_menu(db)

        elif choice == '8':
            path = generate_backup_summary(cfg.backup_log_dir)
            if path:
                print(f"[REPORT] Backup summary CSV: {path}")
            else:
                print("[REPORT] Could not create backup summary CSV.")

        elif choice == '9':
            try:
                path = create_database_backup(cfg)
            except RuntimeError as e:
                print(str(e))
            else:
                print(f"[DB BACKUP] PostgreSQL dump created: {path}")

        elif choice == '0':
            print("Goodbye.")
            db.close()
            break

        else:
            print("[ERROR] Invalid selection.")
