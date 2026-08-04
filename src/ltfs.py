"""LTFS drive readiness, volume labels, TapeManager.

Readiness is deliberately split into four separate concepts, because conflating
them is what made the old probe touch the tape filesystem on every chunk:

``device_detected``
    The drive letter is present in IBM's own device table. A non-filesystem
    fact, obtained from ``LtfsCmdDrives.exe`` via :class:`LtfsDriveCommand`.
``mount_status_known``
    IBM reports a non-blocking mount status (not NO_MEDIA / NOT_MOUNTED / …).
``expected_cartridge_verified``
    The loaded volume label matches the cartridge the caller intends to write.
``filesystem_write_attempt``
    The actual write. **This module never performs one.** Only robocopy does,
    and a successful directory listing is NOT evidence that a write will
    succeed — on 2026-07-31 the volume was already frozen read-only while the
    root remained perfectly listable.

The old implementation proved "readiness" with ``os.path.isdir`` +
``os.listdir`` on the mount root, once per chunk. Across the historical LTFS
logs, 124 index-dirty transitions followed a readiness probe by 0.04-0.8 s, and
both cartridge freezes occurred inside the periodic index sync that such a
dirty flag schedules. Those two calls are therefore gone, and are NOT replaced
by ``exists``/``stat``/``scandir``/``glob``/free-space or any other access to
the mount. What remains is a cached, non-filesystem verification.
"""
import os
import subprocess
import threading
import time
from typing import TYPE_CHECKING

from . import ltfs_ownership as _ltfs_ownership
from .constants import DEFAULT_TAPE_CAPACITY_GB, LTFS_DIR
from .logsetup import get_logger
from .ltfs_ownership import LtfsOwnershipError, owns_ltfs, require_ownership
from .runtime import _acquire_tape_io_lock, _release_tape_io_lock

if TYPE_CHECKING:
    from .pg_db import PgDatabaseManager


# IBM mount statuses that make the volume unusable for writing.
BLOCKING_STATUSES = frozenset({
    "LTFS_UNFORMATTED",
    "NO_LTFS_MEDIA",
    "NO_MEDIA",
    "NOT_MOUNTED",
    "UNFORMATTED",
})

# Why a readiness verification ran (structured-log values).
REASON_SESSION_INIT = "session_init"
REASON_NO_CACHED_STATE = "no_cached_state"
REASON_IO_ERROR = "io_error"
REASON_MOUNT_TRANSITION = "mount_transition"
REASON_CARTRIDGE_MISMATCH = "cartridge_mismatch"
REASON_OPERATOR_RESET = "operator_reset"
REASON_DEVICE_STATE_CHANGE = "device_state_change"
REASON_OWNERSHIP_LOST = "tape_ownership_lost"
REASON_PROCESS_RESTART = "process_restart"
REASON_DIAGNOSTIC = "operator_diagnostic"
REASON_DRIVE_CHANGED = "drive_path_changed"
REASON_CARTRIDGE_EXPECTATION_CHANGED = "cartridge_expectation_changed"


class LtfsDriveCommand:
    """Injectable interface over IBM's drive-enumeration helper.

    ``LtfsCmdDrives.exe`` is NOT assumed harmless: it is a subprocess that talks
    to the LTFS management service, and in the production traces its
    ``CGetDeviceListRequest`` is the last event before the index-dirty
    transition. Isolating it here means it can be replaced, counted or measured
    independently, and that tests never invoke the real binary.
    """

    def drive_status(self, drive_path):
        """Return ``(status, full_output, error)`` for ``drive_path``."""
        raise NotImplementedError


class DefaultLtfsDriveCommand(LtfsDriveCommand):
    """Runs the real ``LtfsCmdDrives.exe``."""

    def drive_status(self, drive_path):
        # Running an IBM helper without ownership is exactly the bypass this
        # phase exists to close.
        require_ownership("LtfsCmdDrives.exe")
        exe = os.path.join(LTFS_DIR, 'LtfsCmdDrives.exe')
        try:
            result = subprocess.run(
                [exe], text=True, capture_output=True, cwd=LTFS_DIR)
        except FileNotFoundError:
            return None, None, f"LtfsCmdDrives.exe not found in: {LTFS_DIR}"

        output = ((result.stdout or '') + (result.stderr or '')).strip()
        wanted = _drive_letter(drive_path).upper()
        for line in output.splitlines():
            parts = line.split()
            if len(parts) >= 4 and parts[0].upper() == wanted:
                return parts[-1], output, None
        return None, output, None


_DRIVE_COMMAND = DefaultLtfsDriveCommand()


def set_ltfs_drive_command(command):
    """Swap the drive-enumeration implementation. Returns the previous one."""
    global _DRIVE_COMMAND
    previous = _DRIVE_COMMAND
    _DRIVE_COMMAND = command or DefaultLtfsDriveCommand()
    return previous


def get_ltfs_drive_command():
    return _DRIVE_COMMAND


class ReadinessState:
    """A verification result that is safe to reuse until invalidated."""

    __slots__ = ("drive", "device_detected", "mount_status_known",
                 "mount_status", "expected_cartridge_verified",
                 "cartridge_label", "pid", "verified_at", "generation")

    def __init__(self, drive, device_detected, mount_status_known, mount_status,
                 expected_cartridge_verified, cartridge_label, pid, verified_at,
                 generation):
        self.drive = drive
        self.device_detected = device_detected
        self.mount_status_known = mount_status_known
        self.mount_status = mount_status
        self.expected_cartridge_verified = expected_cartridge_verified
        self.cartridge_label = cartridge_label
        self.pid = pid
        self.verified_at = verified_at
        self.generation = generation

    def as_dict(self):
        return {name: getattr(self, name) for name in self.__slots__}


class ReadinessCache:
    """Holds the last good verification, and the reason it was discarded.

    Deliberately in-process and process-scoped: a restart starts empty, and a
    PID change (fork) invalidates, so cached state can never outlive the
    process that established it.
    """

    def __init__(self):
        self._lock = threading.RLock()
        self._state = None
        self._generation = 0
        self.last_invalidation_reason = None

    def snapshot(self):
        """Current state as a plain dict (for logging/tests), or None."""
        with self._lock:
            return self._state.as_dict() if self._state else None

    def invalidate(self, reason):
        """Discard cached readiness. Safe to call when nothing is cached."""
        with self._lock:
            had_state = self._state is not None
            self._state = None
            self._generation += 1
            self.last_invalidation_reason = reason
        if had_state:
            get_logger().info("ltfs_readiness_invalidated: reason=%s", reason)
        return had_state

    def store(self, state):
        with self._lock:
            self._state = state
            return state

    def valid_for(self, drive, expected_label):
        """Return (state, None) when reusable, else (None, reason_to_recheck)."""
        with self._lock:
            state = self._state
            if state is None:
                return None, self.last_invalidation_reason or REASON_NO_CACHED_STATE
            if state.pid != os.getpid():
                return None, REASON_PROCESS_RESTART
            if state.drive != drive:
                return None, REASON_DRIVE_CHANGED
            if expected_label is not None:
                if not state.expected_cartridge_verified:
                    return None, REASON_CARTRIDGE_EXPECTATION_CHANGED
                if state.cartridge_label != expected_label:
                    # Fail closed: never serve a cached OK for a different
                    # cartridge than the caller intends to write.
                    return None, REASON_CARTRIDGE_MISMATCH
            return state, None

    @property
    def generation(self):
        with self._lock:
            return self._generation


READINESS = ReadinessCache()


def invalidate_readiness(reason):
    """Force the next readiness call to re-verify."""
    return READINESS.invalidate(reason)


def note_tape_io_error(error=None):
    """Any tape/LTFS I/O error invalidates the cached verification."""
    return invalidate_readiness(
        f"{REASON_IO_ERROR}: {error}" if error else REASON_IO_ERROR)


def note_mount_transition(kind="mount"):
    """A mount/unmount/remount makes every prior verification meaningless."""
    return invalidate_readiness(f"{REASON_MOUNT_TRANSITION}: {kind}")


def note_cartridge_mismatch(expected=None, found=None):
    return invalidate_readiness(
        f"{REASON_CARTRIDGE_MISMATCH}: expected={expected!r} found={found!r}")


def note_device_state_change(detail=None):
    return invalidate_readiness(
        f"{REASON_DEVICE_STATE_CHANGE}: {detail}" if detail
        else REASON_DEVICE_STATE_CHANGE)


def note_tape_ownership_lost(detail=None):
    """Losing exclusive tape ownership invalidates readiness.

    Phase 1 provides the hook; Phase 3 wires it to the cross-process lock so a
    handover to another process always forces re-verification.
    """
    return invalidate_readiness(
        f"{REASON_OWNERSHIP_LOST}: {detail}" if detail else REASON_OWNERSHIP_LOST)


def reset_readiness(reason=REASON_OPERATOR_RESET):
    """Explicit operator reset."""
    return invalidate_readiness(reason)


def _on_ownership_change(detail):
    """Called by src.ltfs_ownership whenever LTFS ownership changes hands.

    Registered below at import time. Covers acquisition (including recovery of
    an abandoned mutex) and release, so cached readiness can never outlive an
    ownership boundary — while ownership was not held, any other process on the
    host could have touched the drive.
    """
    return note_tape_ownership_lost(detail)


_ltfs_ownership.set_ownership_change_callback(_on_ownership_change)


def _read_volume_label_unlocked(drive_path):
    """Read the volume label. Caller must hold the tape I/O lock.

    A volume-label query, not filesystem access: it does not open, list or
    stat the mount root.
    """
    require_ownership("volume label read")
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


def get_volume_label(drive_path):
    """Detect the volume label of a Windows drive (e.g. 'D:\\')."""
    _acquire_tape_io_lock(f"read volume label {drive_path}")
    try:
        return _read_volume_label_unlocked(drive_path)
    finally:
        _release_tape_io_lock()


def _eject_tape_unlocked(tape_drive, ibm_eject_cmd=None):
    """Run LtfsCmdEject.exe for a drive. Caller must hold the tape I/O lock."""
    require_ownership("eject")
    # An eject is a mount transition: every cached verification is now stale,
    # whatever the eject's outcome.
    note_mount_transition("eject")
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
    """Return (status, full_output, error) from IBM LTFS drive info.

    Delegates to the injectable :class:`LtfsDriveCommand` so the subprocess can
    be replaced or measured; behaviour of the default implementation is
    unchanged.
    """
    return _DRIVE_COMMAND.drive_status(drive_path)


def _verify_readiness_uncached(tape_drive, prefix, expected_label):
    """Perform the real, non-filesystem verification. Returns a state or None.

    Touches the LTFS *filesystem* nowhere. It asks IBM's device table for the
    mount status, and — only when the caller names an expected cartridge —
    reads the volume label. Both are metadata queries, not root access.
    """
    checks = []
    status, output, error = _ltfs_drive_status(tape_drive)
    checks.append("drive_status")
    if error:
        print(f"{prefix} {error}")

    letter = _drive_letter(tape_drive)
    device_detected = bool(status)

    if not device_detected:
        # Fail closed. Previously this only printed a warning and fell through
        # to os.listdir, which is what actually caught a moved mount letter.
        # With that listing gone, an unidentifiable drive must stop the write —
        # the LTO mount letter is known to move between remounts.
        print(f"{prefix} Could not identify drive {letter} in the IBM LTFS "
              f"device list; refusing to treat it as ready.")
        if output:
            print(output)
        get_logger().warning(
            "ltfs_readiness_failed: drive=%s reason=device_not_detected "
            "checks=%s", tape_drive, ",".join(checks))
        return None

    print(f"{prefix} IBM LTFS drive status for {letter}: {status}")
    if status.upper() in BLOCKING_STATUSES:
        print(f"{prefix} Drive {tape_drive} is not mounted as a writable LTFS filesystem.")
        if status.upper() == "LTFS_UNFORMATTED":
            print(f"{prefix} Format the cartridge from Tape Maintenance before archiving.")
        elif "MEDIA" in status.upper():
            print(f"{prefix} Load a writable LTFS data cartridge, wait until ready, then retry.")
        else:
            print(f"{prefix} Mount or reload the cartridge, then retry.")
        get_logger().warning(
            "ltfs_readiness_failed: drive=%s reason=blocking_status status=%s "
            "checks=%s", tape_drive, status, ",".join(checks))
        return None

    cartridge_verified = False
    label = None
    if expected_label is not None:
        label = _read_volume_label_unlocked(tape_drive)
        checks.append("volume_label")
        if label != expected_label:
            print(f"{prefix} Cartridge mismatch: expected {expected_label!r}, "
                  f"found {label!r}. Refusing to continue.")
            note_cartridge_mismatch(expected_label, label)
            get_logger().error(
                "ltfs_readiness_failed: drive=%s reason=cartridge_mismatch "
                "expected=%s found=%s checks=%s",
                tape_drive, expected_label, label, ",".join(checks))
            return None
        cartridge_verified = True

    get_logger().info(
        "ltfs_readiness_verified: drive=%s status=%s cartridge=%s checks=%s "
        "filesystem_access=none",
        tape_drive, status, label, ",".join(checks))
    return ReadinessState(
        drive=tape_drive,
        device_detected=True,
        mount_status_known=True,
        mount_status=status,
        expected_cartridge_verified=cartridge_verified,
        cartridge_label=label,
        pid=os.getpid(),
        verified_at=time.time(),
        generation=READINESS.generation,
    )


def _ensure_lto_drive_ready_unlocked(tape_drive, prefix="[TAPE]", *,
                                     reason=None, expected_label=None,
                                     force=False):
    """Verify the LTFS drive is usable, reusing cached state when still valid.

    Performs **no LTFS filesystem access at all** — see the module docstring.
    A cache hit costs nothing: no subprocess, no metadata query, no I/O.
    """
    cached, recheck_reason = READINESS.valid_for(tape_drive, expected_label)
    if cached is not None and not force:
        get_logger().info(
            "ltfs_readiness_cache_hit: drive=%s status=%s cartridge=%s "
            "age_s=%.1f reason=%s",
            tape_drive, cached.mount_status, cached.cartridge_label,
            time.time() - cached.verified_at, reason or "per_chunk")
        return True

    why = reason or recheck_reason or REASON_NO_CACHED_STATE
    if force and reason is None:
        why = REASON_DIAGNOSTIC
    get_logger().info(
        "ltfs_readiness_check_started: drive=%s why=%s expected_cartridge=%s "
        "forced=%s", tape_drive, why, expected_label, bool(force))

    state = _verify_readiness_uncached(tape_drive, prefix, expected_label)
    if state is None:
        READINESS.invalidate(f"verification_failed: {why}")
        return False
    READINESS.store(state)
    return True


def _ensure_lto_drive_ready(tape_drive, prefix="[TAPE]", *, reason=None,
                            expected_label=None, force=False):
    _acquire_tape_io_lock(f"check drive readiness {tape_drive}")
    try:
        return _ensure_lto_drive_ready_unlocked(
            tape_drive, prefix=prefix, reason=reason,
            expected_label=expected_label, force=force)
    finally:
        _release_tape_io_lock()


class TapeManager:
    def __init__(self, db: "PgDatabaseManager", tape_drive: str, ibm_eject_cmd=None):
        self.db            = db
        self.tape_drive    = tape_drive
        self.ibm_eject_cmd = ibm_eject_cmd

    def _drive_letter(self):
        return _drive_letter(self.tape_drive)

    def _ltfs_drive_status(self):
        """Return the current IBM LTFS status for this drive, if available.

        Takes ownership: the adapter asserts it, and the interactive menu paths
        (drive status / invalid-medium hint) reach this without any outer lock.
        Reentrant, so it nests safely inside format_tape/check_tape.
        """
        _acquire_tape_io_lock(f"drive status {self.tape_drive}")
        try:
            return _ltfs_drive_status(self.tape_drive)
        finally:
            _release_tape_io_lock()

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
            print("[REFUSED] Formatting an existing labelled cartridge is no "
                  "longer available from the ordinary maintenance menu. Use "
                  "the explicit generation-aware `run.py tape-reset` command.")
            return
        else:
            manual_old_label = input(
                "Existing DB label to clear after format (optional, Enter to skip): "
            ).strip() or None
            if manual_old_label:
                print("[REFUSED] A database label was supplied. Use the explicit "
                      "generation-aware `run.py tape-reset` command.")
                return

        label = input("New Volume Label (e.g. Scalpelab_Tape_X): ").strip()
        if not label:
            print("[ABORTED] No label provided.")
            return
        if self.db.tape_exists(label):
            print(f"[REFUSED] Label '{label}' already exists in the catalog; "
                  "use the explicit tape-reset workflow.")
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
            self.db.register_tape(label, capacity)
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
        """Run LtfsCmdDrives.exe to display connected tape drives and status.

        Spawns the IBM helper directly rather than through
        :class:`LtfsDriveCommand`, so it must take ownership itself — without
        this it was a fully unprotected LTFS path reachable from the menu.
        """
        _acquire_tape_io_lock("tape drives info")
        try:
            return self._tape_info_unlocked()
        finally:
            _release_tape_io_lock()

    def _tape_info_unlocked(self):
        exe = os.path.join(LTFS_DIR, 'LtfsCmdDrives.exe')
        print("\n[INFO] Running: LtfsCmdDrives.exe")
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
