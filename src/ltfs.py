"""LTFS drive readiness, volume labels, TapeManager."""
import os
import subprocess

try:
    import psutil
except ImportError:  # optional dependency — priority/affinity degrade gracefully
    psutil = None

from .constants import DEFAULT_TAPE_CAPACITY_GB, LTFS_DIR
from .db import DatabaseManager
from .runtime import _acquire_tape_io_lock, _release_tape_io_lock


def get_volume_label(drive_path):
    """Detect the volume label of a Windows drive (e.g. 'D:\\')."""
    _acquire_tape_io_lock(f"read volume label {drive_path}")
    try:
        try:
            drive_letter = drive_path.rstrip(":\\/")
            # 'vol' is a cmd.exe builtin; invoke the shell explicitly instead
            # of shell=True with an args list (a Windows-specific footgun).
            result = subprocess.run(
                ['cmd', '/c', 'vol', f'{drive_letter}:'],
                capture_output=True, text=True
            )
            for line in result.stdout.splitlines():
                line = line.strip()
                if line.lower().startswith('volume in drive') and ' is ' in line:
                    return line.rsplit(' is ', 1)[-1].strip()
        except Exception:
            pass
        return None
    finally:
        _release_tape_io_lock()


def _eject_tape_unlocked(tape_drive, ibm_eject_cmd=None):
    """Run LtfsCmdEject.exe for a drive. Caller must hold the tape I/O lock."""
    drive_arg = tape_drive.rstrip(":\\")
    exe       = ibm_eject_cmd or os.path.join(LTFS_DIR, 'LtfsCmdEject.exe')
    exe_dir   = os.path.dirname(exe) or LTFS_DIR
    cmd       = [exe, drive_arg]
    print("\n" + "#" * 60)
    print("[LTO] FINALIZING: Ejecting tape...")
    print("[LTO] PLEASE WAIT — this can take 1-2 minutes.")
    print("#" * 60)
    try:
        result = subprocess.run(cmd, check=True, text=True, capture_output=True,
                                cwd=exe_dir)
        print("[LTO] Tape ejected successfully!")
        if result.stdout:
            print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Eject failed: {e.stderr}")
        print(f"Try manually: cd /d \"{LTFS_DIR}\" && LtfsCmdEject.exe {drive_arg}")
    except FileNotFoundError:
        print(f"[ERROR] LtfsCmdEject.exe not found in: {LTFS_DIR}")


def eject_tape_drive(tape_drive, ibm_eject_cmd=None):
    """Safely eject a tape, serialized against all other tape I/O."""
    _acquire_tape_io_lock(f"eject {tape_drive}")
    try:
        return _eject_tape_unlocked(tape_drive, ibm_eject_cmd)
    finally:
        _release_tape_io_lock()


def _drive_letter(drive_path):
    return (drive_path or '').rstrip(":\\/")


def _ltfs_drive_status(drive_path):
    """Return (status, full_output, error) from IBM LTFS drive info."""
    exe = os.path.join(LTFS_DIR, 'LtfsCmdDrives.exe')
    try:
        result = subprocess.run([exe], text=True, capture_output=True, cwd=LTFS_DIR)
    except FileNotFoundError:
        return None, None, f"LtfsCmdDrives.exe not found in: {LTFS_DIR}"

    output = ((result.stdout or '') + (result.stderr or '')).strip()
    drive_letter = _drive_letter(drive_path).upper()
    for line in output.splitlines():
        parts = line.split()
        if len(parts) >= 4 and parts[0].upper() == drive_letter:
            return parts[-1], output, None
    return None, output, None


def _ensure_lto_drive_ready_unlocked(tape_drive, prefix="[TAPE]"):
    """Check that the configured LTFS drive is mounted and writable enough to use."""
    status, output, error = _ltfs_drive_status(tape_drive)
    if error:
        print(f"{prefix} {error}")

    if status:
        print(f"{prefix} IBM LTFS drive status for {_drive_letter(tape_drive)}: {status}")
        blocking_statuses = {
            "LTFS_UNFORMATTED",
            "NO_LTFS_MEDIA",
            "NO_MEDIA",
            "NOT_MOUNTED",
            "UNFORMATTED",
        }
        if status.upper() in blocking_statuses:
            print(f"{prefix} Drive {tape_drive} is not mounted as a writable LTFS filesystem.")
            if status.upper() == "LTFS_UNFORMATTED":
                print(f"{prefix} Format the cartridge from Tape Maintenance before archiving.")
            elif "MEDIA" in status.upper():
                print(f"{prefix} Load a writable LTFS data cartridge, wait until ready, then retry.")
            else:
                print(f"{prefix} Mount or reload the cartridge, then retry.")
            return False
    elif output:
        print(f"{prefix} Could not identify drive {_drive_letter(tape_drive)} in LtfsCmdDrives.exe output:")
        print(output)

    try:
        if not os.path.isdir(tape_drive):
            print(f"{prefix} Drive path is not available: {tape_drive}")
            return False
        os.listdir(tape_drive)
    except OSError as e:
        print(f"{prefix} Cannot access LTFS drive {tape_drive}: {e}")
        print(f"{prefix} Check that the cartridge is formatted, loaded, and mounted.")
        return False

    return True


def _ensure_lto_drive_ready(tape_drive, prefix="[TAPE]"):
    _acquire_tape_io_lock(f"check drive readiness {tape_drive}")
    try:
        return _ensure_lto_drive_ready_unlocked(tape_drive, prefix=prefix)
    finally:
        _release_tape_io_lock()


class TapeManager:
    def __init__(self, db: DatabaseManager, tape_drive: str, ibm_eject_cmd=None):
        self.db            = db
        self.tape_drive    = tape_drive
        self.ibm_eject_cmd = ibm_eject_cmd

    def _drive_letter(self):
        return _drive_letter(self.tape_drive)

    def _ltfs_drive_status(self):
        """Return the current IBM LTFS status for this drive, if available."""
        return _ltfs_drive_status(self.tape_drive)

    def _print_drive_status(self, prefix="[INFO]"):
        status, output, error = self._ltfs_drive_status()
        if error:
            print(f"{prefix} {error}")
            return status
        if status:
            print(f"{prefix} IBM LTFS drive status for {self._drive_letter()}: {status}")
        elif output:
            print(f"{prefix} Could not identify drive {self._drive_letter()} in LtfsCmdDrives.exe output:")
            print(output)
        return status

    def _print_invalid_medium_hint(self, operation, output):
        if "LTFS60233E" not in (output or ""):
            return
        status = self._print_drive_status("[HINT]")
        print(f"[HINT] IBM LTFS says the medium is not valid for {operation}.")
        if status == "NO_LTFS_MEDIA":
            print("[HINT] The drive currently reports NO_LTFS_MEDIA.")
            print("       Check that a writable data cartridge is fully loaded, not a cleaning/WORM cartridge,")
            print("       then wait for the drive to become ready and try again.")
        elif status:
            print(f"[HINT] Current medium status is {status}.")
            print("       Eject/reload the tape, confirm the cartridge is writable, and close any app using the drive.")
        else:
            print("[HINT] Run Tape Maintenance -> Tape drives info, then confirm the cartridge is loaded and writable.")

    def list_drives(self):
        if os.name == 'nt':
            try:
                result = subprocess.run(
                    ['wmic', 'logicaldisk', 'get', 'DeviceID,Description,VolumeName'],
                    capture_output=True, text=True
                )
                print("\n[DRIVES]\n" + result.stdout)
            except Exception as e:
                print(f"[ERROR] {e}")
        else:
            print("[INFO] Drive listing is only supported on Windows.")

    def format_tape(self):
        print("\n[TAPE MANAGER] Format / Initialize Tape")
        print(f"Target drive: {self.tape_drive}")
        self._print_drive_status()
        print("=" * 60)
        print("WARNING: This will ERASE ALL DATA on the current tape.")
        print('Type  y  to confirm (or press Enter to cancel):')
        if input(">> ").strip().lower() != "y":
            print("[ABORTED] Format cancelled.")
            return

        old_label = get_volume_label(self.tape_drive)
        if old_label:
            print(f"[INFO] Current tape label detected: {old_label}")
            manual_old_label = None
        else:
            manual_old_label = input(
                "Existing DB label to clear after format (optional, Enter to skip): "
            ).strip() or None

        label = input("New Volume Label (e.g. Scalpelab_Tape_X): ").strip()
        if not label:
            print("[ABORTED] No label provided.")
            return

        drive_letter = self._drive_letter()
        exe          = os.path.join(LTFS_DIR, 'LtfsCmdFormat.exe')
        cmd          = [exe, drive_letter, f'/N:{label}']

        print(f"\n[FORMAT] Running: cd /d \"{LTFS_DIR}\" && LtfsCmdFormat.exe {drive_letter} /N:{label}")
        print("[FORMAT] This may take several minutes...")

        _acquire_tape_io_lock(f"format {self.tape_drive}")
        try:
            result = subprocess.run(cmd, check=True, text=True, capture_output=True,
                                    cwd=LTFS_DIR)
            print("[FORMAT] Complete.")
            if result.stdout:
                print(result.stdout)

            cap      = input(f"Tape capacity in GB (default {DEFAULT_TAPE_CAPACITY_GB} "
                             "for 12 TB, Enter to skip): ").strip()
            capacity = int(cap) if cap.isdigit() else DEFAULT_TAPE_CAPACITY_GB
            self.db.replace_formatted_tape(
                label, capacity, previous_labels=[old_label, manual_old_label])
        except subprocess.CalledProcessError as e:
            output = ((e.stdout or '') + (e.stderr or '')).strip()
            print(f"[ERROR] LtfsCmdFormat.exe failed:\n{output}")
            self._print_invalid_medium_hint("Format", output)
        except FileNotFoundError:
            print(f"[ERROR] LtfsCmdFormat.exe not found in: {LTFS_DIR}")
        finally:
            _release_tape_io_lock()

    def register_tape(self):
        label = input("Volume label of tape to register: ").strip()
        if not label:
            return
        cap      = input(f"Capacity in GB (default {DEFAULT_TAPE_CAPACITY_GB} "
                         "for 12 TB, Enter to skip): ").strip()
        capacity = int(cap) if cap.isdigit() else DEFAULT_TAPE_CAPACITY_GB
        self.db.register_tape(label, capacity)

    def check_tape(self):
        """Run LtfsCmdCheck.exe to check and repair the tape filesystem."""
        drive_letter = self._drive_letter()
        exe          = os.path.join(LTFS_DIR, 'LtfsCmdCheck.exe')
        cmd          = [exe, drive_letter]
        print(f"\n[CHECK] Running: LtfsCmdCheck.exe {drive_letter}")
        self._print_drive_status("[CHECK]")
        print("[CHECK] This may take several minutes...")
        _acquire_tape_io_lock(f"check {self.tape_drive}")
        try:
            result = subprocess.run(cmd, text=True, capture_output=True, cwd=LTFS_DIR)
            output = (result.stdout or '') + (result.stderr or '')
            if output.strip():
                print(output.strip())
            if result.returncode == 0:
                print("[CHECK] Complete — no errors found.")
            else:
                print(f"[CHECK] Finished with code {result.returncode}.")
                self._print_invalid_medium_hint("Check", output)
        except FileNotFoundError:
            print(f"[ERROR] LtfsCmdCheck.exe not found in: {LTFS_DIR}")
        finally:
            _release_tape_io_lock()

    def tape_info(self):
        """Run LtfsCmdDrives.exe to display connected tape drives and status."""
        exe = os.path.join(LTFS_DIR, 'LtfsCmdDrives.exe')
        print(f"\n[INFO] Running: LtfsCmdDrives.exe")
        try:
            result = subprocess.run([exe], text=True, capture_output=True, cwd=LTFS_DIR)
            output = (result.stdout or '') + (result.stderr or '')
            if output.strip():
                print(output.strip())
            if result.returncode != 0:
                print(f"[INFO] Finished with code {result.returncode}.")
        except FileNotFoundError:
            print(f"[ERROR] LtfsCmdDrives.exe not found in: {LTFS_DIR}")

    def eject_tape(self):
        """Run LtfsCmdEject.exe to safely eject the tape."""
        return eject_tape_drive(self.tape_drive, self.ibm_eject_cmd)
