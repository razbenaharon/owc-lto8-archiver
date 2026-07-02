"""Main menu, archiver entry points, DB management submenu."""
import os

try:
    import psutil
except ImportError:  # optional dependency — priority/affinity degrade gracefully
    psutil = None

from .config import ConfigManager
from .constants import CONFIG_FILE
from .db import DatabaseManager, create_database_manager
from .ltfs import TapeManager
from .orchestrators import LocalOrchestrator, RemoteOrchestrator
from .pg_backup import create_database_backup
from .reporting import generate_backup_summary
from .retriever import LTORetriever
from .robocopy import _prepare_robocopy_exclusion, _remove_robocopy_exclusion
from .remote_transport import _cleanup_askpass_helpers
from .runtime import _terminate_all_procs, install_cancel_handler, reset_cancel, uninstall_cancel_handler, unpin_current_process


def run_archiver(cfg: ConfigManager, db: DatabaseManager):
    added_exclusion = _prepare_robocopy_exclusion()
    reset_cancel()
    install_cancel_handler()
    print("[LOCAL] Press Ctrl+C at any time to stop safely "
          "(the session is saved and can be resumed).")
    try:
        LocalOrchestrator(cfg, db).run()
    except RuntimeError as e:
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


def run_remote_archiver(cfg, db):
    """Menu option 6: pull files from a remote host and archive to LTO tape."""
    if not cfg.remote_host or not cfg.remote_user or not cfg.remote_path:
        print("\n[REMOTE] The [REMOTE] section in config.ini is incomplete.")
        print("  Required: remote_host, remote_user, remote_path")
        print("  Optional: remote_password, staging_fill_pct  (default 0.80)")
        cfg_abs = os.path.abspath(CONFIG_FILE)
        print(f"\n[INFO] Config path: {cfg_abs}")
        if os.name == 'nt':
            os.startfile(cfg_abs)
        return

    added_exclusion = _prepare_robocopy_exclusion()
    reset_cancel()
    install_cancel_handler()
    print("[REMOTE] Press Ctrl+C at any time to stop safely "
          "(the session is saved and can be resumed).")
    try:
        RemoteOrchestrator(cfg, db).run()
    except RuntimeError as e:
        print(str(e))
    except KeyboardInterrupt:
        print("\n[REMOTE] Interrupted. Session state saved — re-run to resume.")
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


def _print_tapes_table(db):
    tapes = db.list_tapes()
    if not tapes:
        print("[DB] No tapes registered.")
        return tapes
    BAR_W = 20
    print(f"\n{'ID':>4}  {'Volume Label':<25}  {'Initialized':<19}  Space")
    print("-" * 80)
    for t in tapes:
        date_s  = (t['date_formatted'] or '')[:19]
        cap_gb  = t['total_capacity']
        used_b  = t['used_space'] or 0
        used_gb = used_b / 1024**3
        if cap_gb:
            pct     = min(used_gb / cap_gb, 1.0)
            filled  = round(pct * BAR_W)
            bar     = '█' * filled + '░' * (BAR_W - filled)
            space_s = f"[{bar}] {pct*100:.1f}%  {used_gb:.1f}/{cap_gb:.0f} GiB"
        else:
            space_s = f"{used_gb:.1f} GiB used  (no capacity set)"
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
            print(f"  Backed up: {(rec['backup_date'] or '')[:19]}")
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
            print(f"[DB] Used space updated: {old_used/1024**3:.2f} GB → {new_used/1024**3:.2f} GB")

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
    db        = create_database_manager(cfg)
    tape_mgr  = TapeManager(db, cfg.lto_drive, cfg.ibm_eject_cmd)
    retriever = LTORetriever(db, cfg.lto_drive, cfg.staging_dir, cfg.restore_dir)

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

        choice = input("Choose: ").strip()

        if choice == '1':
            run_archiver(cfg, db)

        elif choice == '2':
            added_exclusion = _prepare_robocopy_exclusion()
            try:
                retriever.run()
            finally:
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
            tapes = db.list_tapes()
            if not tapes:
                print("[DB] No tapes registered yet.")
            else:
                BAR_W = 24
                print(f"\n{'ID':>4}  {'Volume Label':<25}  {'Initialized':<19}  {'Used / Capacity':<22}  Space")
                print("-" * 95)
                for t in tapes:
                    date_s  = (t['date_formatted'] or '')[:19]
                    cap_gb  = t['total_capacity']
                    used_b  = t['used_space'] or 0
                    used_gb = used_b / 1024**3

                    if cap_gb:
                        pct    = min(used_gb / cap_gb, 1.0)
                        filled = round(pct * BAR_W)
                        bar    = '█' * filled + '░' * (BAR_W - filled)
                        space_s = f"[{bar}] {pct*100:.1f}%  {used_gb:.1f}/{cap_gb:.0f} GiB"
                    else:
                        space_s = f"{used_gb:.1f} GiB used  (no capacity set)"

                    print(f"{t['tape_id']:>4}  {t['volume_label']:<25}  {date_s:<19}  {space_s}")

        elif choice == '5':
            cfg_abs = os.path.abspath(CONFIG_FILE)
            print(f"\n[INFO] Config path: {cfg_abs}")
            if os.name == 'nt':
                os.startfile(cfg_abs)

        elif choice == '6':
            run_remote_archiver(cfg, db)

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
