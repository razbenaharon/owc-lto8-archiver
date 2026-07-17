"""Keep a forced Windows Update restart from landing in the middle of a run.

Why this exists: on 2026-07-15 a forced Windows Update restart interrupted an
active tape write on Tape_02. The restart killed the writer before LTFS could
sync its final index, and chunks 18-91 of session 37 (~126 GB) were lost from
the cartridge even though every one of those writes had already been
acknowledged and counted in ``tape_used_after``. The data was not recoverable
from the tape; the whole span had to be re-fetched and re-written.

This module does not fight Windows Update and does not disable the service.
It uses the two supported mechanisms Windows exposes to move the update
window outside the run, and puts both back when the run ends:

  * ``PauseUpdatesExpiryTime`` and the feature/quality pause pairs under
    ``HKLM\\SOFTWARE\\Microsoft\\WindowsUpdate\\UX\\Settings`` — the same knob
    the Settings app writes for "Pause updates". Windows will not download or
    install while the expiry time is in the future, so no restart is staged.
  * ``NoAutoRebootWithLoggedOnUsers`` under the ``WindowsUpdate\\AU`` policy
    key — a belt-and-braces stop on the restart itself, which is the part
    that actually destroys the LTFS index if anything does slip through.

Crash safety matters here more than tidiness. This pipeline gets force-killed
on purpose to recover from tape-stage deadlocks, so ``__exit__`` cannot be
trusted to run. The pre-change snapshot is therefore written to disk
(``backup_logs/_windows_update_guard.json``) *before* anything is modified,
and ``restore_stale_guard()`` replays it on the next start. Leaving updates
paused after a crash is the fail-safe direction for the tape, but it is bad
security hygiene to leave them paused indefinitely, so the next run always
puts them back.
"""
import json
import os
from datetime import datetime, timedelta, timezone

try:
    import winreg
except ImportError:  # non-Windows — every entry point below degrades to a no-op
    winreg = None

from .constants import BACKUP_LOG_DIR
from .logsetup import get_logger
from .runtime import _is_admin

_UX_PATH = r"SOFTWARE\Microsoft\WindowsUpdate\UX\Settings"
_AU_PATH = r"SOFTWARE\Policies\Microsoft\Windows\WindowsUpdate\AU"

# The Settings-app pause writes all five of these together. Windows honours
# PauseUpdatesExpiryTime on its own, but the feature/quality pairs are what
# the Settings UI reads back, so setting only the first leaves the UI showing
# "not paused" and invites an operator to "fix" it mid-run.
_UX_PAUSE_VALUES = (
    "PauseUpdatesExpiryTime",
    "PauseFeatureUpdatesStartTime",
    "PauseFeatureUpdatesEndTime",
    "PauseQualityUpdatesStartTime",
    "PauseQualityUpdatesEndTime",
)

_STATE_FILE = os.path.join(BACKUP_LOG_DIR, "_windows_update_guard.json")

# Windows caps a pause at 35 days; stay well under it. A guard that silently
# fails to apply because the span was rejected is worse than a short pause.
_MAX_PAUSE_DAYS = 30


def _iso(dt):
    """Format as the UTC ISO-8601 string shape Windows writes for these keys."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _read_value(root, path, name):
    """Return (value, regtype) for a registry value, or (None, None) if absent."""
    if winreg is None:
        return None, None
    try:
        with winreg.OpenKey(root, path, 0, winreg.KEY_READ) as key:
            value, regtype = winreg.QueryValueEx(key, name)
            return value, regtype
    except OSError:
        return None, None


def _write_value(root, path, name, value, regtype):
    if winreg is None:
        return
    with winreg.CreateKeyEx(root, path, 0, winreg.KEY_SET_VALUE) as key:
        winreg.SetValueEx(key, name, 0, regtype, value)


def _delete_value(root, path, name):
    if winreg is None:
        return
    try:
        with winreg.OpenKey(root, path, 0, winreg.KEY_SET_VALUE) as key:
            winreg.DeleteValue(key, name)
    except OSError:
        # Already absent, which is the state we were restoring it to anyway.
        pass


def pending_reboot_reasons():
    """Return a list of human-readable reasons Windows wants to restart now.

    A pending restart is the one state the pause cannot save us from: the
    update is already staged, and Windows will take the restart at its next
    opportunity regardless of the pause flag. The only safe move is to reboot
    before starting a multi-hour tape write, not during one.
    """
    if winreg is None:
        return []
    reasons = []
    checks = (
        (winreg.HKEY_LOCAL_MACHINE,
         r"SOFTWARE\Microsoft\Windows\CurrentVersion\WindowsUpdate\Auto Update\RebootRequired",
         "Windows Update has staged an update and is waiting to restart"),
        (winreg.HKEY_LOCAL_MACHINE,
         r"SOFTWARE\Microsoft\Windows\CurrentVersion\Component Based Servicing\RebootPending",
         "Component Based Servicing has a restart pending"),
    )
    for root, path, reason in checks:
        try:
            with winreg.OpenKey(root, path, 0, winreg.KEY_READ):
                reasons.append(reason)
        except OSError:
            pass

    renames, _ = _read_value(
        winreg.HKEY_LOCAL_MACHINE,
        r"SYSTEM\CurrentControlSet\Control\Session Manager",
        "PendingFileRenameOperations")
    if renames:
        reasons.append("Files are queued to be renamed on the next restart")
    return reasons


def _snapshot():
    """Capture the current values of everything pause_windows_updates() writes."""
    snap = {"ux": {}, "au": {}}
    for name in _UX_PAUSE_VALUES:
        value, regtype = _read_value(winreg.HKEY_LOCAL_MACHINE, _UX_PATH, name)
        snap["ux"][name] = None if value is None else {"value": value, "type": regtype}
    value, regtype = _read_value(
        winreg.HKEY_LOCAL_MACHINE, _AU_PATH, "NoAutoRebootWithLoggedOnUsers")
    snap["au"]["NoAutoRebootWithLoggedOnUsers"] = (
        None if value is None else {"value": value, "type": regtype})
    return snap


def _save_state(snap):
    os.makedirs(BACKUP_LOG_DIR, exist_ok=True)
    payload = {"saved_at": _iso(datetime.now(timezone.utc)), "snapshot": snap}
    with open(_STATE_FILE, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2)


def _apply_snapshot(snap):
    """Put every captured value back — restoring absence as absence."""
    for name, entry in snap.get("ux", {}).items():
        if entry is None:
            _delete_value(winreg.HKEY_LOCAL_MACHINE, _UX_PATH, name)
        else:
            _write_value(winreg.HKEY_LOCAL_MACHINE, _UX_PATH,
                         name, entry["value"], entry["type"])
    for name, entry in snap.get("au", {}).items():
        if entry is None:
            _delete_value(winreg.HKEY_LOCAL_MACHINE, _AU_PATH, name)
        else:
            _write_value(winreg.HKEY_LOCAL_MACHINE, _AU_PATH,
                         name, entry["value"], entry["type"])


def restore_stale_guard():
    """Undo a pause left behind by a run that was killed before it could clean up.

    Called at startup, before a new guard is installed. Without this a
    force-killed run (the standard tape-stage deadlock recovery) would leave
    this host silently unpatched for as long as the pause lasts.
    """
    if winreg is None or not os.path.exists(_STATE_FILE):
        return False
    if not _is_admin():
        print("[WU] A previous run left Windows Update paused, but this "
              "process is not elevated and cannot restore it. Re-run as "
              "Administrator to put the update settings back.")
        return False
    try:
        with open(_STATE_FILE, encoding="utf-8") as fh:
            payload = json.load(fh)
        _apply_snapshot(payload["snapshot"])
    except (OSError, ValueError, KeyError):
        get_logger().exception("failed to restore stale Windows Update guard")
        print("[WU] WARNING: could not restore the Windows Update settings "
              f"left by a previous run. Check {_STATE_FILE} and the pause "
              "state in Settings > Windows Update.")
        return False
    os.remove(_STATE_FILE)
    print(f"[WU] Restored Windows Update settings left paused by a previous "
          f"run (saved {payload.get('saved_at', 'unknown')}).")
    return True


def pause_windows_updates(pause_days):
    """Pause Windows Update for ``pause_days`` and block the auto-restart.

    Returns True if WE applied the pause (caller must call
    resume_windows_updates() in a finally block). Returns False when the
    guard could not be applied, which is a warning and not a failure — the
    caller decides whether to proceed.
    """
    if winreg is None:
        return False
    if not _is_admin():
        print("[WU] Running without Administrator privileges — cannot pause "
              "Windows Update. A forced update restart during a tape write "
              "will corrupt the LTFS index and lose the chunks written so "
              "far (this happened on 2026-07-15, ~126 GB). Re-run as "
              "Administrator, or pause updates manually in Settings.")
        return False

    days = max(1, min(int(pause_days), _MAX_PAUSE_DAYS))
    now = datetime.now(timezone.utc)
    end = now + timedelta(days=days)

    snap = _snapshot()
    # Persist BEFORE the first write: a crash between here and the last
    # SetValueEx must still leave the next run enough to undo this.
    _save_state(snap)

    try:
        for name in _UX_PAUSE_VALUES:
            stamp = _iso(now) if name.endswith("StartTime") else _iso(end)
            _write_value(winreg.HKEY_LOCAL_MACHINE, _UX_PATH,
                         name, stamp, winreg.REG_SZ)
        _write_value(winreg.HKEY_LOCAL_MACHINE, _AU_PATH,
                     "NoAutoRebootWithLoggedOnUsers", 1, winreg.REG_DWORD)
    except OSError:
        get_logger().exception("failed to pause Windows Update")
        print("[WU] WARNING: could not pause Windows Update. Proceeding "
              "without the guard — a forced restart can lose the tape's "
              "index. Consider pausing updates manually.")
        # Roll back whatever landed so we don't leave a half-applied pause.
        try:
            _apply_snapshot(snap)
            os.remove(_STATE_FILE)
        except OSError:
            pass
        return False

    print(f"[WU] Windows Update paused until {_iso(end)} ({days}d) and "
          f"automatic restart blocked for this run.")
    return True


def resume_windows_updates():
    """Restore the update settings captured by pause_windows_updates()."""
    if winreg is None or not os.path.exists(_STATE_FILE):
        return
    if not _is_admin():
        return
    try:
        with open(_STATE_FILE, encoding="utf-8") as fh:
            payload = json.load(fh)
        _apply_snapshot(payload["snapshot"])
    except (OSError, ValueError, KeyError):
        get_logger().exception("failed to resume Windows Update")
        print("[WU] WARNING: failed to restore Windows Update settings. "
              f"Check {_STATE_FILE} and re-enable updates in Settings.")
        return
    try:
        os.remove(_STATE_FILE)
    except OSError:
        pass
    print("[WU] Windows Update settings restored.")
