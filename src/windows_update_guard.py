"""Keep a forced Windows Update restart from landing in the middle of a run.

Why this exists: on 2026-07-15 a forced Windows Update restart interrupted an
active tape write on Tape_02. The restart killed the writer before LTFS could
sync its final index, and chunks 18-91 of session 37 (~126 GB) were lost from
the cartridge even though every one of those writes had already been
acknowledged and counted in ``tape_used_after``. The data was not recoverable
from the tape; the whole span had to be re-fetched and re-written.

There are two layers here, because the first one is not always allowed to
work. On a host whose updates are administered centrally — this one is
domain-joined and served by WSUS — policy can set ``SetDisablePauseUXAccess``
and a compliance deadline, and then the pause is ignored and the deadline
restart overrides both ActiveHours and NoAutoRebootWithLoggedOnUsers. The
registry writes still *succeed*, which is exactly the trap: the guard looks
like it worked. ``managed_update_policy()`` detects that case so the operator
is told the truth, and ``RebootSentinel`` is what actually protects the write
there — it races the restart instead of trying to prevent it.

Layer 1 does not fight Windows Update and does not disable the service.
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
import threading
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
_WU_POLICY_PATH = r"SOFTWARE\Policies\Microsoft\Windows\WindowsUpdate"

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


def managed_update_policy():
    """Describe how much of the pause this host's admin policy actually honours.

    Returns a dict with ``managed`` (bool), ``pause_disabled`` (bool),
    ``deadline_days`` (int or None) and a list of human-readable ``notes``.

    This matters because the pause is a *consumer* setting. On a domain-joined
    host whose updates come from WSUS, and especially where policy sets
    ``SetDisablePauseUXAccess=1``, the Windows Update Agent ignores
    PauseUpdatesExpiryTime entirely — the value is still written to the
    registry, so a naive check would report success while the host stays fully
    exposed. Worse, ``SetComplianceDeadline`` + ``ConfigureDeadlineFor*Updates``
    force a restart once the deadline passes, overriding BOTH ActiveHours and
    NoAutoRebootWithLoggedOnUsers. That combination is what destroyed the
    2026-07-15 write on this host: the AU policy was already set to 1 and did
    not help.

    A guard that prints a reassuring "paused" line it cannot back up is worse
    than no guard, so callers use this to tell the operator the truth.
    """
    info = {"managed": False, "pause_disabled": False,
            "deadline_days": None, "notes": []}
    if winreg is None:
        return info

    wu_server, _ = _read_value(
        winreg.HKEY_LOCAL_MACHINE, _WU_POLICY_PATH, "WUServer")
    use_wu_server, _ = _read_value(
        winreg.HKEY_LOCAL_MACHINE, _AU_PATH, "UseWUServer")
    if wu_server and use_wu_server:
        info["managed"] = True
        info["notes"].append(f"Updates are managed by WSUS at {wu_server}")

    pause_disabled, _ = _read_value(
        winreg.HKEY_LOCAL_MACHINE, _WU_POLICY_PATH, "SetDisablePauseUXAccess")
    if pause_disabled:
        info["managed"] = True
        info["pause_disabled"] = True
        info["notes"].append(
            "Policy SetDisablePauseUXAccess=1 removes the 'Pause updates' "
            "feature — the pause this guard writes will NOT be honoured")

    deadline_set, _ = _read_value(
        winreg.HKEY_LOCAL_MACHINE, _WU_POLICY_PATH, "SetComplianceDeadline")
    quality_deadline, _ = _read_value(
        winreg.HKEY_LOCAL_MACHINE, _WU_POLICY_PATH,
        "ConfigureDeadlineForQualityUpdates")
    if deadline_set and quality_deadline is not None:
        info["managed"] = True
        info["deadline_days"] = int(quality_deadline)
        info["notes"].append(
            f"A compliance deadline of {quality_deadline} day(s) forces a "
            "restart once it expires, overriding ActiveHours and "
            "NoAutoRebootWithLoggedOnUsers")
    return info


def print_guard_status(applied, policy):
    """Tell the operator what protection they actually have. No false comfort."""
    if policy["managed"]:
        print("[WU] WARNING: Windows Update on this host is managed by "
              "administrator policy. The pause is NOT reliable protection:")
        for note in policy["notes"]:
            print(f"  - {note}")
        print("[WU] The reboot sentinel is the real guard here: it stops the "
              "run cleanly at a chunk boundary as soon as a restart is "
              "staged, so LTFS syncs its index and the session stays "
              "resumable. For a durable fix, ask IT to exempt this host from "
              "the update deadline policy.")
    elif applied:
        print("[WU] Windows Update paused for this run and automatic restart "
              "blocked.")


class RebootSentinel:
    """Watch for a staged restart and ask the pipeline to stop cleanly.

    This is the guard that survives an administrator-forced update, because it
    does not try to prevent the restart at all — it races it. Windows stages a
    pending-restart marker when it installs an update, typically hours before
    the deadline actually fires the restart. Stopping at the next chunk
    boundary in that window turns the 2026-07-15 outcome (writer killed
    mid-write, LTFS index never synced, ~126 GB of acknowledged writes gone)
    into an ordinary resumable stop.

    The sentinel only ever *sets* the caller's stop event. It never kills the
    writer itself: interrupting a tape write is the exact failure being
    defended against.
    """

    def __init__(self, stop_event, poll_seconds=60, on_detect=None):
        self.stop_event = stop_event
        self.poll_seconds = poll_seconds
        self.on_detect = on_detect
        self.triggered = False
        self._thread = None
        self._cancel = threading.Event()

    def _check_once(self):
        reasons = pending_reboot_reasons()
        if not reasons:
            return False
        self.triggered = True
        message = ("[WU] A Windows restart is now staged while the run is "
                   "active: " + "; ".join(reasons))
        print(f"\n{message}")
        print("[WU] Stopping at the next chunk boundary so the LTFS index is "
              "synced and the session stays resumable. Re-run option 6 to "
              "resume after the host has restarted.")
        get_logger().warning(message)
        if self.on_detect:
            try:
                self.on_detect(reasons)
            except Exception:
                get_logger().exception("reboot sentinel on_detect failed")
        self.stop_event.set()
        return True

    def _loop(self):
        while not self._cancel.is_set() and not self.stop_event.is_set():
            try:
                if self._check_once():
                    return
            except Exception:
                # A registry hiccup must never take the pipeline down.
                get_logger().exception("reboot sentinel check failed")
            self._cancel.wait(self.poll_seconds)

    def start(self):
        if winreg is None:
            return self
        self._thread = threading.Thread(
            target=self._loop, name="reboot-sentinel", daemon=True)
        self._thread.start()
        print(f"[WU] Reboot sentinel armed (polling every {self.poll_seconds}s) "
              "— the run stops cleanly at a chunk boundary if Windows stages "
              "a restart.")
        return self

    def stop(self):
        self._cancel.set()
        if self._thread:
            self._thread.join(timeout=5)


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

    # Deliberately silent on success: the registry write succeeding does not
    # mean the host will honour it (see managed_update_policy). The caller
    # reports status via print_guard_status once it knows the policy context.
    get_logger().info("Windows Update pause written, expiry %s (%dd)",
                      _iso(end), days)
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
