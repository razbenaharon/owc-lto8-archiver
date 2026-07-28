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
import csv
import json
import os
import re
import subprocess
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

# --- Configuration Manager (SCCM) -------------------------------------------
# The 2026-07-15 restart was NOT a WSUS deadline restart. System log 1074 names
# the initiator: CcmExec.exe, "Your computer will restart at 15/07/2026 10:39:01
# to complete the installation of applications and software updates" — the
# Software Center notification. SCCM is a separate control plane from Windows
# Update, so pausing WU cannot influence it and the WU pending-restart markers
# are not the authoritative source for it. The supported query is the client
# SDK method below; the registry key is corroboration only.
_SCCM_NAMESPACE = r"root\ccm\ClientSDK"
_SCCM_CLASS = "CCM_ClientUtilities"
_SCCM_METHOD = "DetermineIfRebootPending"
_SCCM_REBOOT_DATA = (
    r"SOFTWARE\Microsoft\SMS\Mobile Client\Reboot Management\RebootData")

# The client SDK call is a local CIM call; it answers in well under a second on
# a healthy client. The cap only exists so a wedged WMI service cannot stall a
# pre-write check indefinitely.
_SCCM_QUERY_TIMEOUT_S = 20

# SCCM reports "no deadline" as the epoch rather than a null.
_SCCM_EPOCH_YEAR = 1970

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


# --- Reboot signal classification --------------------------------------------
# Severity, not message text, is the decision mechanism. Before 2026-07-28 the
# only lever was a boolean `include_soft`, wired to
# `[WINDOWS_UPDATE] block_on_pending_reboot`. That conflated two unrelated
# questions — "is this marker strong enough to block?" and "does the operator
# want pending reboots to block at all?" — so the only way to stop a benign
# PendingFileRenameOperations entry from blocking every write was to set
# block_on_pending_reboot = false, which ALSO disabled blocking on SCCM, CBS and
# Windows Update. The guard CLAUDE.md calls "the guard that holds" was switched
# off to silence a stale rename queue. Severity fixes that: the rename marker is
# warning-only by classification, so the operator flag can stay true.
SEVERITY_CRITICAL = "critical"
SEVERITY_WARNING = "warning"

# Typed reason codes. Callers and tests match on these, never on message text.
REBOOT_WU_REQUIRED = "windows_update_reboot_required"
REBOOT_WU_POST_REBOOT_REPORTING = "windows_update_post_reboot_reporting"
REBOOT_CBS_PENDING = "cbs_reboot_pending"
REBOOT_CBS_IN_PROGRESS = "cbs_reboot_in_progress"
REBOOT_CBS_PACKAGES_PENDING = "cbs_packages_pending"
REBOOT_COMPUTER_RENAME = "computer_rename_pending"
REBOOT_DOMAIN_JOIN = "domain_join_pending"
REBOOT_SCCM_PENDING = "sccm_reboot_pending"
REBOOT_SCCM_HARD = "sccm_hard_reboot_pending"
REBOOT_SCCM_INDETERMINATE = "sccm_state_unknown"
REBOOT_FILE_RENAME = "pending_file_rename_operations"

_SESSION_MANAGER_PATH = r"SYSTEM\CurrentControlSet\Control\Session Manager"
_COMPUTERNAME_ACTIVE = (
    r"SYSTEM\CurrentControlSet\Control\ComputerName\ActiveComputerName")
_COMPUTERNAME_PENDING = (
    r"SYSTEM\CurrentControlSet\Control\ComputerName\ComputerName")
_NETLOGON_PATH = r"SYSTEM\CurrentControlSet\Services\Netlogon"

#: Key-existence markers that mean Windows has staged work only a restart
#: completes. Each is (code, registry path, message).
_CRITICAL_KEY_MARKERS = (
    (REBOOT_WU_REQUIRED,
     r"SOFTWARE\Microsoft\Windows\CurrentVersion\WindowsUpdate\Auto Update"
     r"\RebootRequired",
     "Windows Update has staged an update and is waiting to restart"),
    (REBOOT_WU_POST_REBOOT_REPORTING,
     r"SOFTWARE\Microsoft\Windows\CurrentVersion\WindowsUpdate\Auto Update"
     r"\PostRebootReporting",
     "Windows Update is waiting to report the result of a restart"),
    (REBOOT_CBS_PENDING,
     r"SOFTWARE\Microsoft\Windows\CurrentVersion\Component Based Servicing"
     r"\RebootPending",
     "Component Based Servicing has a restart pending"),
    (REBOOT_CBS_IN_PROGRESS,
     r"SOFTWARE\Microsoft\Windows\CurrentVersion\Component Based Servicing"
     r"\RebootInProgress",
     "Component Based Servicing reports a restart already in progress"),
    (REBOOT_CBS_PACKAGES_PENDING,
     r"SOFTWARE\Microsoft\Windows\CurrentVersion\Component Based Servicing"
     r"\PackagesPending",
     "Component Based Servicing has packages waiting for a restart"),
)


class RebootSignal:
    """One classified reason the host might be about to restart.

    ``code`` is the stable identifier to branch on, ``source`` records where the
    evidence came from (registry path or SCCM result field) so a diagnostic can
    be traced back, and ``message`` is for humans only.
    """

    __slots__ = ("code", "severity", "source", "message")

    def __init__(self, code, severity, source, message):
        self.code = code
        self.severity = severity
        self.source = source
        self.message = message

    @property
    def is_critical(self):
        return self.severity == SEVERITY_CRITICAL

    def as_dict(self):
        return {"code": self.code, "severity": self.severity,
                "source": self.source, "message": self.message}

    def __repr__(self):
        return f"RebootSignal({self.code}, {self.severity})"

    def __eq__(self, other):
        return (isinstance(other, RebootSignal)
                and self.as_dict() == other.as_dict())

    def __hash__(self):
        return hash((self.code, self.severity, self.source, self.message))


class RebootAssessment:
    """The full classified picture, with the block/allow decision derived once.

    ``blocking`` is true only when at least one CRITICAL signal is present. A
    warning-only assessment is *not* discarded — it stays in ``warnings`` and is
    logged by every caller, so a stale rename queue remains visible in
    diagnostics without stopping a tape write.
    """

    __slots__ = ("signals", "sccm")

    def __init__(self, signals, sccm=None):
        self.signals = list(signals)
        self.sccm = sccm

    @property
    def critical(self):
        return [s for s in self.signals if s.is_critical]

    @property
    def warnings(self):
        return [s for s in self.signals if not s.is_critical]

    @property
    def blocking(self):
        """True when a real restart risk exists. Critical always wins."""
        return bool(self.critical)

    @property
    def blocking_reasons(self):
        return [s.message for s in self.critical]

    @property
    def warning_reasons(self):
        return [s.message for s in self.warnings]

    @property
    def codes(self):
        return [s.code for s in self.signals]

    def reasons(self, include_warnings=True):
        src = self.signals if include_warnings else self.critical
        return [s.message for s in src]

    def warning_summary(self):
        """One line for the log when nothing blocks but something was seen."""
        if not self.warnings:
            return ""
        return ("Pending reboot warning, non-blocking: "
                + "; ".join(f"{s.message} [{s.code}]" for s in self.warnings)
                + " — no critical reboot requirement was detected.")

    def as_dict(self):
        return {"blocking": self.blocking,
                "critical": [s.as_dict() for s in self.critical],
                "warnings": [s.as_dict() for s in self.warnings],
                "sccm": self.sccm}


def _key_exists(path):
    if winreg is None:
        return False
    try:
        with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, path, 0, winreg.KEY_READ):
            return True
    except OSError:
        return False


def pending_reboot_signals():
    """Classify every Windows-side pending-restart marker on this host.

    Critical markers mean Windows has staged work that only a restart
    completes. ``PendingFileRenameOperations`` is deliberately NOT one of them:
    it is a list of file moves to apply *if* a restart happens and it never
    causes one. Routine Edge and Defender updates leave entries there for days,
    so treating it as "a restart is staged" is a permanent false positive that
    no amount of waiting clears — which is exactly what happened here.
    """
    if winreg is None:
        return []
    signals = []

    for code, path, message in _CRITICAL_KEY_MARKERS:
        if _key_exists(path):
            signals.append(RebootSignal(
                code, SEVERITY_CRITICAL, f"HKLM\\{path}", message))

    # A rename or domain join that has been written but not yet applied needs a
    # restart to take effect, and the machine may be restarted to finish it.
    active, _ = _read_value(
        winreg.HKEY_LOCAL_MACHINE, _COMPUTERNAME_ACTIVE, "ComputerName")
    target, _ = _read_value(
        winreg.HKEY_LOCAL_MACHINE, _COMPUTERNAME_PENDING, "ComputerName")
    if active and target and str(active).strip() != str(target).strip():
        signals.append(RebootSignal(
            REBOOT_COMPUTER_RENAME, SEVERITY_CRITICAL,
            f"HKLM\\{_COMPUTERNAME_PENDING}",
            f"A computer rename to '{target}' is pending a restart"))

    for name in ("JoinDomain", "AvoidSpnSet"):
        value, _ = _read_value(
            winreg.HKEY_LOCAL_MACHINE, _NETLOGON_PATH, name)
        if value:
            signals.append(RebootSignal(
                REBOOT_DOMAIN_JOIN, SEVERITY_CRITICAL,
                f"HKLM\\{_NETLOGON_PATH}\\{name}",
                "A domain-join operation is pending a restart"))
            break

    renames, _ = _read_value(
        winreg.HKEY_LOCAL_MACHINE, _SESSION_MANAGER_PATH,
        "PendingFileRenameOperations")
    if renames:
        signals.append(RebootSignal(
            REBOOT_FILE_RENAME, SEVERITY_WARNING,
            f"HKLM\\{_SESSION_MANAGER_PATH}\\PendingFileRenameOperations",
            "Files are queued to be renamed on the next restart"))
    return signals


def pending_reboot_reasons(include_soft=True):
    """Human-readable Windows pending-restart reasons. Compatibility shim.

    Retained for existing callers and diagnostics; the block/allow decision now
    lives in :func:`assess_reboot_state`. ``include_soft=False`` drops the
    warning-severity signals, which is what the old boolean meant.
    """
    signals = pending_reboot_signals()
    if not include_soft:
        signals = [s for s in signals if s.is_critical]
    return [s.message for s in signals]


def _sccm_query_client_sdk():
    """Call CCM_ClientUtilities.DetermineIfRebootPending, or raise.

    Python has no CIM client here, so this shells out to PowerShell and parses
    JSON. Failures are raised rather than swallowed: callers about to start a
    tape write need to distinguish "SCCM says no restart" from "SCCM could not
    be asked", and those two must never collapse into the same answer.
    """
    script = (
        "$ErrorActionPreference='Stop';"
        f"$r = Invoke-CimMethod -Namespace '{_SCCM_NAMESPACE}' "
        f"-ClassName {_SCCM_CLASS} -MethodName {_SCCM_METHOD};"
        "[pscustomobject]@{"
        "ReturnValue=$r.ReturnValue;"
        "RebootPending=[bool]$r.RebootPending;"
        "IsHardRebootPending=[bool]$r.IsHardRebootPending;"
        "InGracePeriod=[bool]$r.InGracePeriod;"
        "RebootDeadline=(&{if($r.RebootDeadline){$r.RebootDeadline.ToString('o')}else{''}})"
        "} | ConvertTo-Json -Compress"
    )
    proc = subprocess.run(
        ["powershell.exe", "-NoProfile", "-NonInteractive", "-Command", script],
        capture_output=True, text=True, timeout=_SCCM_QUERY_TIMEOUT_S,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    )
    if proc.returncode != 0:
        raise RuntimeError(
            (proc.stderr or "powershell returned "
             f"{proc.returncode}").strip().splitlines()[0])
    out = (proc.stdout or "").strip()
    if not out:
        raise RuntimeError("client SDK returned no data")
    return json.loads(out)


def _sccm_registry_reboot_data():
    """Corroborating signal: SCCM writes restart scheduling values under this key.

    Supplementary only — the client SDK is the authority.

    The key's *existence* means nothing: verified on this host 2026-07-17, it is
    present but completely empty while the SDK reports RebootPending=False. It
    is the scheduling values that indicate a staged restart, so treating
    presence as "pending" would have blocked every tape write whenever the SDK
    was briefly unreachable.
    """
    if winreg is None:
        return False
    try:
        with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, _SCCM_REBOOT_DATA,
                            0, winreg.KEY_READ) as key:
            for name in ("RebootBy", "HardReboot", "NotifyUI",
                         "OverrideRebootWindowTime"):
                try:
                    value, _kind = winreg.QueryValueEx(key, name)
                except OSError:
                    continue
                # RebootBy is a deadline stamp; 0 means "nothing scheduled".
                if value:
                    return True
            return False
    except Exception:
        # Corroboration only. Any failure here — missing key, no permission, a
        # stubbed winreg — means "no corroborating signal", never an error worth
        # propagating into a guard the pipeline depends on.
        return False


def sccm_reboot_status():
    """Ask the Configuration Manager client whether it intends to restart.

    Returns a dict:
      ``installed``           — the client SDK namespace answered at all
      ``reboot_pending``      — SCCM wants a restart
      ``hard_reboot_pending`` — the restart cannot be deferred by the user
      ``in_grace_period``     — the countdown to a forced restart is running
      ``deadline``            — ISO string, or None when SCCM reports none
      ``error``               — why the query failed, or None
      ``determinate``         — False means the state is UNKNOWN, not "clear"

    ``determinate`` is the field that matters at a tape-write boundary. A host
    with no SCCM client is determinate-and-clear; a host whose client could not
    be reached is not, and callers must treat that conservatively.
    """
    info = {"installed": False, "reboot_pending": False,
            "hard_reboot_pending": False, "in_grace_period": False,
            "deadline": None, "error": None, "determinate": False,
            "registry_reboot_data": False}
    log = get_logger()
    log.debug("sccm_reboot_check_started")

    if os.name != "nt":
        info["determinate"] = True
        log.debug("sccm_reboot_check_completed: not Windows")
        return info

    info["registry_reboot_data"] = _sccm_registry_reboot_data()

    try:
        raw = _sccm_query_client_sdk()
    except Exception as e:
        msg = str(e)
        # A host with no Configuration Manager client is a legitimate, fully
        # determinate state — there is no SCCM to stage a restart. That is very
        # different from a client that exists but would not answer.
        if "Invalid namespace" in msg or "InvalidNamespace" in msg:
            info["determinate"] = True
            log.debug("sccm_reboot_check_completed: no SCCM client installed")
            return info
        info["error"] = msg
        log.warning("sccm_check_failed: %s", msg)
        if info["registry_reboot_data"]:
            # The SDK is unreachable but SCCM has staged restart data. Report
            # the restart; a false alarm costs one clean stop, a missed one
            # costs the index.
            info["reboot_pending"] = True
            log.warning("sccm_reboot_pending: from registry RebootData "
                        "(client SDK unreachable)")
        return info

    info["installed"] = True
    info["determinate"] = True
    info["reboot_pending"] = bool(raw.get("RebootPending"))
    info["hard_reboot_pending"] = bool(raw.get("IsHardRebootPending"))
    info["in_grace_period"] = bool(raw.get("InGracePeriod"))

    deadline = (raw.get("RebootDeadline") or "").strip()
    if deadline and not deadline.startswith(str(_SCCM_EPOCH_YEAR)):
        info["deadline"] = deadline
        log.warning("sccm_reboot_deadline: %s", deadline)

    if info["reboot_pending"]:
        log.warning("sccm_reboot_pending: deadline=%s grace=%s hard=%s",
                    info["deadline"], info["in_grace_period"],
                    info["hard_reboot_pending"])
    if info["hard_reboot_pending"]:
        log.warning("sccm_hard_reboot_pending")
    if info["in_grace_period"]:
        log.warning("sccm_grace_period: a forced restart countdown is running")

    log.debug("sccm_reboot_check_completed: pending=%s determinate=%s",
              info["reboot_pending"], info["determinate"])
    return info


def _sccm_signals(sccm, block_on_unknown):
    """Turn the SCCM status dict into classified signals.

    Every SCCM signal is critical: the client stating restart intent is the
    highest-confidence evidence available, and it is the control plane that
    actually took the 2026-07-15 restart with 60 seconds of warning.
    """
    signals = []
    if sccm.get("reboot_pending"):
        detail = "Configuration Manager (SCCM) has a restart pending"
        if sccm.get("in_grace_period"):
            detail += " and the forced-restart grace period is running"
        if sccm.get("deadline"):
            detail += f" (deadline {sccm['deadline']})"
        signals.append(RebootSignal(
            REBOOT_SCCM_PENDING, SEVERITY_CRITICAL,
            "CCM_ClientUtilities.DetermineIfRebootPending.RebootPending",
            detail))
    if sccm.get("hard_reboot_pending"):
        signals.append(RebootSignal(
            REBOOT_SCCM_HARD, SEVERITY_CRITICAL,
            "CCM_ClientUtilities.DetermineIfRebootPending.IsHardRebootPending",
            "SCCM reports a hard restart that cannot be deferred"))
    if block_on_unknown and not sccm.get("determinate"):
        # Fail closed, unchanged policy: unknown is never silently "safe".
        signals.append(RebootSignal(
            REBOOT_SCCM_INDETERMINATE, SEVERITY_CRITICAL,
            "CCM_ClientUtilities.DetermineIfRebootPending",
            "SCCM restart state could not be determined "
            f"({sccm.get('error')}) — refusing to start a tape write blind"))
    return signals


def assess_reboot_state(block_on_unknown=True):
    """Classify every restart indicator into a single :class:`RebootAssessment`.

    This is the decision mechanism. Callers ask ``assessment.blocking`` rather
    than inspecting message strings, so adding a marker or rewording a message
    can never silently change whether a tape write is allowed to start.

    ``block_on_unknown`` is the difference between the two callers, and the
    asymmetry is deliberate and unchanged. At a tape-write boundary an
    indeterminate SCCM answer must block: starting a multi-minute write while
    blind to the restart state is the exact bet that lost ~126 GB on
    2026-07-15, and the cost of being wrong is one deferred chunk. The
    background sentinel passes False, because there a transient WMI hiccup
    would stop a perfectly healthy run for no reason — it polls again in 60s,
    and the pre-write gate still backstops it.
    """
    signals = list(pending_reboot_signals())
    sccm = sccm_reboot_status()
    signals.extend(_sccm_signals(sccm, block_on_unknown))
    return RebootAssessment(signals, sccm)


def reboot_block_reasons(block_on_unknown=True, include_soft=None):
    """Reasons a new tape write must not start. Compatibility wrapper.

    Returns ``(blocking_reasons, sccm)``. Only CRITICAL signals appear — a
    warning-only indicator such as ``PendingFileRenameOperations`` never blocks
    on its own. ``include_soft`` is accepted and ignored: severity, not a
    caller-supplied boolean, now decides what blocks. Use
    :func:`assess_reboot_state` for the warnings too.
    """
    assessment = assess_reboot_state(block_on_unknown=block_on_unknown)
    if assessment.warnings and not assessment.blocking:
        get_logger().info("reboot_warning_non_blocking: %s",
                          assessment.warning_summary())
    return assessment.blocking_reasons, assessment.sccm


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

    def __init__(self, stop_event, poll_seconds=60, on_detect=None,
                 include_soft=True):
        self.stop_event = stop_event
        self.poll_seconds = poll_seconds
        self.on_detect = on_detect
        self.include_soft = include_soft
        self.triggered = False
        self._thread = None
        self._cancel = threading.Event()

    def _check_once(self):
        # block_on_unknown=False: a transient WMI failure must not stop a
        # healthy run. The pre-write gate refuses to start the next write while
        # the state is unknown, so nothing slips past on the path that matters.
        #
        # Severity, not include_soft, decides. The sentinel used to trip within
        # 60s on PendingFileRenameOperations unless the operator disabled the
        # whole guard; now that marker is warning-only by classification, so it
        # is logged and the run continues.
        assessment = assess_reboot_state(block_on_unknown=False)
        if not assessment.blocking:
            if assessment.warnings:
                get_logger().info("reboot_sentinel_warning_non_blocking: %s",
                                  assessment.warning_summary())
            return False
        reasons = assessment.blocking_reasons
        self.triggered = True
        message = ("[WU] A restart is now staged while the run is "
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


def ltfs_sync_mode_status(expect_seconds=300):
    """Report what sync mode the *live* LTFS mount actually declared.

    Read from the LTFS Windows event log, because that is the only place the
    running mount states its own configuration. Event 61259 is emitted once per
    mount: ``Sync type is "time", Sync time is 300 sec``.

    This exists because the config file is not a reliable proxy for the mount.
    On 2026-07-16 an MSI reinstall of IBM Storage Archive SDE 2.4.8.4 rewrote
    ``ltfs.conf.local`` back to its packaged contents, silently discarding a
    ``sync_type`` line someone had added — the file on disk and the behaviour of
    the mount had drifted apart with nothing to flag it. Only the mount's own
    declaration settles it.

    Returns a dict with ``determinate``, ``ok``, ``sync_type``, ``sync_seconds``,
    ``declared_at`` and ``error``.
    """
    info = {"determinate": False, "ok": False, "sync_type": None,
            "sync_seconds": None, "declared_at": None, "error": None}
    if os.name != "nt":
        return info

    script = (
        "$ErrorActionPreference='Stop';"
        "$e = Get-WinEvent -LogName LTFS -MaxEvents 4000 -ErrorAction Stop |"
        " Where-Object { $_.Id -eq 61259 } |"
        " Sort-Object TimeCreated -Descending | Select-Object -First 1;"
        "if (-not $e) { 'NONE' } else {"
        " $e.TimeCreated.ToString('o') + '|' + ($e.Message -replace '\\s+',' ') }"
    )
    try:
        proc = subprocess.run(
            ["powershell.exe", "-NoProfile", "-NonInteractive", "-Command",
             script],
            capture_output=True, text=True, timeout=_SCCM_QUERY_TIMEOUT_S,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        if proc.returncode != 0:
            raise RuntimeError((proc.stderr or "").strip().splitlines()[0]
                               if proc.stderr else "Get-WinEvent failed")
        out = (proc.stdout or "").strip()
        if not out or out == "NONE":
            raise RuntimeError("no LTFS mount declaration (event 61259) found")
    except Exception as e:
        info["error"] = str(e)
        get_logger().warning("ltfs_sync_mode_check_failed: %s", e)
        return info

    stamp, _, message = out.partition("|")
    info["declared_at"] = stamp
    info["determinate"] = True

    import re
    m_type = re.search(r'Sync type is "([^"]+)"', message)
    m_secs = re.search(r"Sync time is (\d+) sec", message)
    if m_type:
        info["sync_type"] = m_type.group(1)
    if m_secs:
        info["sync_seconds"] = int(m_secs.group(1))
    info["ok"] = (info["sync_type"] == "time"
                  and info["sync_seconds"] == expect_seconds)
    return info


# The LTFS mount is served by a long-lived host process (IBM Storage Archive).
# Its name has varied across SDE builds, so match any process whose name
# contains "ltfs" rather than pinning one spelling.
_LTFS_PROCESS_MATCH = "ltfs"

# ...but only ONE of those processes actually owns the mount: LtfsMain, which
# performs the mount and emits the 61259 declaration. The others (LtfsManager,
# LtfsMgmtSvc, LtfsAtMntSvc, LtfsGuiCancelShutdown) are services and GUI helpers
# that outlive individual mounts.
#
# 2026-07-26: anchoring on the earliest *any* "ltfs" process broke a real run.
# After a cartridge swap the LTFS services were restarted, but the GUI helper
# LtfsGuiCancelShutdown survived from the previous day, so the mount window was
# anchored ~28 h too early. The health check then swept in the OLD cartridge's
# LOCATE faults and refused to write to a brand-new, provably clean tape.
#
# Preferring LtfsMain keeps the safety property intact: it starts before it
# mounts the volume, so the anchor is still at or before the true mount, and the
# evidence window can never be narrower than the mount itself. The broad match
# stays as a fallback for SDE builds that name the mount process differently.
_LTFS_MOUNT_PROCESS_MATCH = "ltfsmain"


def ltfs_current_mount_status(expect_seconds=300):
    """Verify the *live* LTFS mount declared time@<expect_seconds>, and that the
    declaration belongs to the mount that is running **right now**.

    ``ltfs_sync_mode_status`` reads the newest event 61259, but a time@5 line
    from a *previous* mount would wrongly approve a write after a remount that
    changed the mode. This binds the evidence to the current mount: it finds the
    running LTFS process, and requires the 61259 declaration to have been emitted
    at or after that process started. If no LTFS process is running, or the
    declaration predates it, or either fact cannot be read, the mount is
    reported ``unverifiable`` and the caller fails closed.

    Read-only: it correlates the Windows event log with process metadata. It
    never probes, remounts, or writes to the drive.

    Returns a dict with ``determinate``, ``ok``, ``sync_type``, ``sync_seconds``,
    ``declared_at``, ``mount_identified``, ``bound_to_current``, ``reason``
    (None / ``"not_time5"`` / ``"unverifiable"``) and ``error``.
    """
    info = {"determinate": False, "ok": False, "sync_type": None,
            "sync_seconds": None, "declared_at": None,
            "mount_identified": False, "mount_started_at": None,
            "bound_to_current": False, "reason": "unverifiable", "error": None}
    if os.name != "nt":
        info["error"] = "not Windows — cannot verify the live LTFS mount"
        return info

    # One PowerShell round-trip returns both facts: the earliest start time
    # among running LTFS processes, and the newest 61259 declaration. Parsing
    # and the actual decision happen in Python so they are unit-testable.
    script = (
        "$ErrorActionPreference='Stop';"
        "$m = Get-CimInstance Win32_Process "
        f"-Filter \"Name LIKE '%{_LTFS_MOUNT_PROCESS_MATCH}%'\" "
        "-ErrorAction SilentlyContinue |"
        " Sort-Object CreationDate | Select-Object -First 1;"
        "$mstart = if ($m) { $m.CreationDate.ToString('o') } else { '' };"
        "$p = Get-CimInstance Win32_Process "
        f"-Filter \"Name LIKE '%{_LTFS_PROCESS_MATCH}%'\" "
        "-ErrorAction SilentlyContinue |"
        " Sort-Object CreationDate | Select-Object -First 1;"
        "$pstart = if ($p) { $p.CreationDate.ToString('o') } else { '' };"
        "$e = Get-WinEvent -LogName LTFS -MaxEvents 4000 -ErrorAction Stop |"
        " Where-Object { $_.Id -eq 61259 } |"
        " Sort-Object TimeCreated -Descending | Select-Object -First 1;"
        "$etime = if ($e) { $e.TimeCreated.ToString('o') } else { '' };"
        "$emsg = if ($e) { ($e.Message -replace '\\s+',' ') } else { '' };"
        "[pscustomobject]@{MountProcStart=$mstart; ProcStart=$pstart; "
        "EventTime=$etime; EventMsg=$emsg} | ConvertTo-Json -Compress"
    )
    try:
        proc = subprocess.run(
            ["powershell.exe", "-NoProfile", "-NonInteractive", "-Command",
             script],
            capture_output=True, text=True, timeout=_SCCM_QUERY_TIMEOUT_S,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        if proc.returncode != 0:
            raise RuntimeError((proc.stderr or "").strip().splitlines()[0]
                               if proc.stderr else "query failed")
        out = (proc.stdout or "").strip()
        if not out:
            raise RuntimeError("mount verification query returned no data")
        data = json.loads(out)
    except Exception as e:
        info["error"] = str(e)
        get_logger().warning("ltfs_current_mount_check_failed: %s", e)
        return info

    # Prefer the process that owns the mount; fall back to the broad match only
    # when no LtfsMain-like process is running (differently-named SDE builds).
    proc_start = ((data.get("MountProcStart") or "").strip()
                  or (data.get("ProcStart") or "").strip())
    event_time = (data.get("EventTime") or "").strip()
    message = (data.get("EventMsg") or "").strip()

    if not proc_start:
        info["error"] = "no running LTFS process found — the drive is not mounted"
        get_logger().warning("ltfs_current_mount_unverifiable: %s", info["error"])
        return info
    info["mount_identified"] = True
    info["mount_started_at"] = proc_start

    if not event_time or not message:
        info["error"] = ("no LTFS mount declaration (event 61259) for the "
                         "running mount")
        get_logger().warning("ltfs_current_mount_unverifiable: %s", info["error"])
        return info
    info["declared_at"] = event_time

    import re
    m_type = re.search(r'Sync type is "([^"]+)"', message)
    m_secs = re.search(r"Sync time is (\d+) sec", message)
    if m_type:
        info["sync_type"] = m_type.group(1)
    if m_secs:
        info["sync_seconds"] = int(m_secs.group(1))

    bound = _iso_at_or_after(event_time, proc_start)
    info["bound_to_current"] = bound
    if not bound:
        # Only a stale declaration from a previous mount exists — the live
        # mount's mode is unproven. Fail closed.
        info["error"] = (f"newest LTFS declaration ({event_time}) predates the "
                         f"running mount ({proc_start}); it is from a previous "
                         "mount")
        get_logger().warning("ltfs_current_mount_stale_declaration: %s",
                             info["error"])
        return info

    # The declaration is bound to the live mount; the mount state is now known.
    info["determinate"] = True
    info["ok"] = (info["sync_type"] == "time"
                  and info["sync_seconds"] == expect_seconds)
    info["reason"] = None if info["ok"] else "not_time5"
    return info


# ---------------------------------------------------------------------------
# LTFS drive / medium health
# ---------------------------------------------------------------------------
# Why this exists: on 2026-07-24 Tape_02 was frozen by a permanent write error
# and is read-only for good. It was not sudden. From 2026-07-20 the drive failed
# 45 LOCATE operations with a write-perm error — roughly one per chunk cycle —
# LTFS masked every one of them ("Replace a return code to -1201"), and the run
# carried on for four days until the servo lost track following mid-write. The
# archiver watched Windows Update, RAM, fetch stalls and robocopy, but nothing
# watched the one log that predicted the failure. Stopping on the first of these
# events is the difference between a paused run and a cartridge that only a
# person standing at the drive can replace.

# Errors that mean the volume is already compromised.
_LTFS_FATAL_EVENTS = {
    11083: "index write failed; the medium may be inconsistent",
    11333: "cartridge already carries a permanent write error (mounts read-only)",
    11343: "recovery index written to the index partition after a permanent write error",
    11344: "cartridge frozen by a permanent write error",
    12045: "LTFS dropped the volume to read-only after a failed block write",
    17060: "XML writer could not write a block to the medium",
}

# Errors that mean the drive is failing operations LTFS is silently retrying —
# the early warning that was missed.
_LTFS_DEGRADED_EVENTS = {
    17267: "drive returned a write-perm error on a LOCATE command",
}

# Event 62173 is Information level and carries benign mode-sense chatter
# ("Not Ready to Ready Transition, Medium May Have Changed") as well as real
# media faults, so it counts only when the message itself reports a failure.
_LTFS_62173_FAULT_MARKERS = ("error on write", "error on read",
                             "error on locate", "track following", "servo")

_LTFS_HEALTH_EVENT_IDS = frozenset(
    set(_LTFS_FATAL_EVENTS) | set(_LTFS_DEGRADED_EVENTS) | {62173})

# The authoritative source is IBM's own CSV trace, NOT the Windows "LTFS" event
# log. Verified 2026-07-25: the Windows log contains **no 17267 events at all**
# — the single most valuable early warning — and it had already rotated away
# everything before 07-23, while the CSV still held the full history back to
# 07-16. Gating on the event log would have missed the very failure this guard
# exists to catch.
_LTFS_LOG_CSV_DEFAULT = r"C:\Program Files\IBM\LTFS\log\LogFile.csv"

# "2026/07/24 07:07:41.022" — local time, no zone.
_LTFS_CSV_TIME_FMT = "%Y/%m/%d %H:%M:%S.%f"


def _parse_ltfs_csv_time(text):
    """Parse an LTFS CSV timestamp to a naive local datetime, or None."""
    try:
        return datetime.strptime((text or "").strip(), _LTFS_CSV_TIME_FMT)
    except (ValueError, TypeError):
        return None


def _classify_ltfs_health_rows(rows):
    """Split normalised LTFS rows into (fatal, degraded). Pure — unit-testable.

    Each row is ``{"id": int, "at": str, "message": str}``.
    """
    fatal, degraded = [], []
    for row in rows:
        try:
            eid = int(row.get("id"))
        except (TypeError, ValueError):
            continue
        entry = {"id": eid, "at": (row.get("at") or "").strip(),
                 "message": (row.get("message") or "").strip()}
        if eid in _LTFS_FATAL_EVENTS:
            entry["meaning"] = _LTFS_FATAL_EVENTS[eid]
            fatal.append(entry)
        elif eid in _LTFS_DEGRADED_EVENTS:
            entry["meaning"] = _LTFS_DEGRADED_EVENTS[eid]
            degraded.append(entry)
        elif eid == 62173:
            low = entry["message"].lower()
            if any(m in low for m in _LTFS_62173_FAULT_MARKERS):
                entry["meaning"] = "drive reported a media/positioning fault"
                degraded.append(entry)
    return fatal, degraded


def ltfs_media_health(since_iso=None, log_path=None):
    """Report drive/medium faults the LTFS mount has logged.

    Reads IBM's own ``LogFile.csv`` trace — read-only, and it never touches the
    drive or moves tape. Pass ``since_iso`` (the mount's start time) so faults
    from a previous cartridge cannot block the current one; the evidence then
    resets naturally on every remount.

    Returns a dict with ``determinate``, ``ok``, ``fatal``, ``degraded``,
    ``since``, ``log_path`` and ``error``. ``ok`` is only meaningful when
    ``determinate`` is True; callers that gate a tape write should fail closed on
    an indeterminate result, as the mount-mode gate does.
    """
    path = log_path or _LTFS_LOG_CSV_DEFAULT
    info = {"determinate": False, "ok": False, "fatal": [], "degraded": [],
            "since": since_iso, "log_path": path, "error": None}

    cutoff = None
    if since_iso:
        try:
            parsed = datetime.fromisoformat(str(since_iso))
            # CSV stamps are naive local time; normalise the bound to match.
            if parsed.tzinfo is not None:
                parsed = parsed.astimezone().replace(tzinfo=None)
            cutoff = parsed
        except (ValueError, TypeError) as e:
            info["error"] = f"unparsable since timestamp {since_iso!r}: {e}"
            get_logger().warning("ltfs_media_health_bad_since: %s", info["error"])
            return info

    rows = []
    try:
        # LTFS holds the file open for append; read shared and tolerate the
        # mixed encodings its messages occasionally carry.
        with open(path, "r", encoding="utf-8", errors="replace",
                  newline="") as fh:
            for rec in csv.reader(fh):
                # id, level, timestamp, pid, tid, drive, message
                if len(rec) < 7:
                    continue
                try:
                    eid = int(rec[0])
                except (TypeError, ValueError):
                    continue
                if eid not in _LTFS_HEALTH_EVENT_IDS:
                    continue
                when = _parse_ltfs_csv_time(rec[2])
                if when is None:
                    continue
                if cutoff is not None and when < cutoff:
                    continue
                rows.append({"id": eid, "at": rec[2].strip(),
                             "message": rec[6].strip()})
    except OSError as e:
        info["error"] = f"cannot read {path}: {e}"
        get_logger().warning("ltfs_media_health_check_failed: %s", info["error"])
        return info

    # Newest first, so callers can report the latest event without re-sorting.
    rows.reverse()
    info["fatal"], info["degraded"] = _classify_ltfs_health_rows(rows)
    info["determinate"] = True
    info["ok"] = not info["fatal"] and not info["degraded"]
    return info


def _iso_at_or_after(a_iso, b_iso):
    """True if timestamp a_iso is at or after b_iso. Both are ISO-8601 'o' form.

    Conservative: if either cannot be parsed, returns False so an unparseable
    stamp fails closed rather than silently approving a write.
    """
    try:
        from datetime import datetime as _dt
        a = _dt.fromisoformat(a_iso.replace("Z", "+00:00"))
        b = _dt.fromisoformat(b_iso.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return False
    # Compare in a tz-aware way when possible; if one side is naive, drop tzinfo
    # on both so the comparison never raises.
    if (a.tzinfo is None) != (b.tzinfo is None):
        a = a.replace(tzinfo=None)
        b = b.replace(tzinfo=None)
    return a >= b


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
