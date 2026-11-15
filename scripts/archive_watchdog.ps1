<#
.SYNOPSIS
    Restart a stopped remote-archive session, but only when it is provably safe.

.DESCRIPTION
    On 2026-07-28 the archiver was launched as a background task of an assistant
    session. When that session's remote control dropped, the harness stopped its
    background tasks, which delivered CTRL_C to the archiver. The archiver did
    the right thing -- cooperative stop, session saved -- but nothing restarted
    it, so the backup sat idle ~15 hours until a human noticed.

    This closes that gap. It is deliberately conservative: it refuses far more
    often than it starts. Every check below must pass, or it logs a REFUSE line
    and exits without touching anything.

      1. No archiver process already running   (never two archivers)
      2. No robocopy running                   (a tape write is in flight)
      3. No PostgreSQL advisory lock held      (an archiver holds one)
      4. An active session exists with work left
      5. No chunk left in 'backing'            (ambiguous on-tape outcome)
      6. The LTO drive letter exists
      7. The mounted cartridge matches the session's tape_label
      8. eject_after_session is false          (never auto-eject)

    It starts the run via the LTO-Archive-Resume scheduled task, so the archiver
    is owned by the Task Scheduler service and NOT by this script, this console,
    VS Code, SSH, or any assistant session. That task also sets
    MultipleInstances=IgnoreNew as a second layer against duplicates.

.NOTES
    Read-only with respect to the tape: it checks the drive letter and reads the
    volume label, and never walks the filesystem, moves tape, or ejects.
#>
[CmdletBinding()]
param(
    [string]$TaskName = 'LTO-Archive-Resume',
    [switch]$WhatIfOnly
)

$ErrorActionPreference = 'Stop'
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$LogFile = Join-Path $ProjectRoot 'backup_logs\archive_watchdog.log'

function Write-Log {
    param([string]$Level, [string]$Message)
    $line = "{0} [{1}] {2}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $Level, $Message
    Write-Output $line
    try {
        Add-Content -Path $LogFile -Value $line -Encoding UTF8
    } catch { }
}

function Get-ConfigValue {
    param([string]$Section, [string]$Key)
    $inSection = $false
    foreach ($line in (Get-Content (Join-Path $ProjectRoot 'config.ini'))) {
        $t = $line.Trim()
        if ($t -match '^\[(.+)\]$') { $inSection = ($Matches[1] -eq $Section); continue }
        if ($inSection -and $t -match "^$Key\s*=\s*(.*)$") { return $Matches[1].Trim() }
    }
    return $null
}

function Invoke-Psql {
    param([string]$Sql)
    $db = Get-ConfigValue -Section 'DATABASE' -Key 'dbname'
    $user = Get-ConfigValue -Section 'DATABASE' -Key 'user'
    if (-not $db -or -not $user) { throw 'could not read [DATABASE] dbname/user from config.ini' }
    $out = docker exec lto_pg psql -U $user -d $db -tAc $Sql 2>&1
    if ($LASTEXITCODE -ne 0) { throw "psql failed: $out" }
    return ($out | Out-String).Trim()
}

# >>> hard-stop policy -- MUST mirror src/exit_codes.py
# Source of truth: NO_AUTO_RESTART_REASONS, NO_AUTO_RESTART_EXIT_CODES and
# AUTO_RESTARTABLE_EXIT_CODES in src/exit_codes.py. tests/
# test_watchdog_hard_stop_sync.py fails when these lists drift, and executes
# this block on its own (it touches nothing: no process, database or device).
$HardStopReasons = @(
    # 20 SAFETY_BLOCK -- a human must inspect before continuing
    'ambiguous_backing_chunk',
    'ltfs_sync_mode_not_time5',
    'ltfs_mount_unverifiable',
    'ltfs_media_degraded',
    'unexpected_tape_or_db_state',
    'ltfs_ownership_unavailable',
    'ambiguous_active_sessions',
    'sealed_batch_feature_unavailable',
    'scan_frontier_unavailable',
    'archiver_lock_lost',
    # 30 FATAL_CONFIG -- config/auth/code error, no blind retry
    'ssh_authentication_failed',
    'ssh_permission_denied',
    'ssh_host_key_mismatch',
    'missing_noninteractive_credential',
    'bad_config',
    'no_active_session',
    'noninteractive_requires_resume',
    # 40 USER_STOP -- the operator stopped it on purpose
    'user_requested_stop',
    # 10, but a tape write failed: never retried into automatically
    'tape_write_failed'
)
$HardStopExitCodes = @(20, 30, 40)
$RestartableExitCodes = @(10)

function Get-RecordedFailureRefusal {
    # Decide from the archiver's own last_failure.json record whether an
    # automatic restart is forbidden. Returns $null when a restart is allowed,
    # otherwise the reason for refusing. Fails closed: a record without a
    # usable exit code, or with one this script does not know, refuses.
    param($Failure)
    if ($null -eq $Failure) { return 'last_failure.json is empty or unreadable' }
    $reason = [string]$Failure.reason
    if ($reason -and ($HardStopReasons -contains $reason)) {
        return "last recorded failure '$reason' is a hard stop - an operator must authorise resumption"
    }
    $code = $null
    if ($null -ne $Failure.exit_code) {
        $parsed = 0
        if ([int]::TryParse([string]$Failure.exit_code, [ref]$parsed)) { $code = $parsed }
    }
    if ($null -eq $code) {
        return "last recorded failure '$reason' carries no usable exit code - refusing to guess that it is restartable"
    }
    if ($HardStopExitCodes -contains $code) {
        return "last recorded failure '$reason' ended with exit code $code (hard stop) - an operator must authorise resumption"
    }
    if ($RestartableExitCodes -notcontains $code) {
        return "last recorded failure '$reason' ended with unknown exit code $code - refusing to guess that it is restartable"
    }
    return $null
}

function Get-TaskResultRefusal {
    # Honour the exit code of the last scheduled archiver run as well: a
    # failure that never reached last_failure.json (a config/database error at
    # startup, for example) is visible only here.
    param($LastTaskResult)
    $parsed = [long]0
    if ($null -eq $LastTaskResult -or
        -not [long]::TryParse([string]$LastTaskResult, [ref]$parsed)) {
        return 'the last archiver task result could not be read'
    }
    if ($HardStopExitCodes -contains $parsed) {
        return "the last archiver task run ended with exit code $parsed (hard stop) - an operator must authorise resumption"
    }
    return $null
}
# <<< hard-stop policy

$refusals = @()

try {
    # --- 1. an archiver already running is the normal, healthy case -----------
    $archivers = @(Get-CimInstance Win32_Process -Filter "Name='python.exe'" -ErrorAction SilentlyContinue |
                   Where-Object { $_.CommandLine -and $_.CommandLine -match 'remote-archive' })
    if ($archivers.Count -gt 0) {
        Write-Log 'OK' ("archiver already running (PID {0}) - nothing to do" -f ($archivers.ProcessId -join ','))
        exit 0
    }

    # --- 2. robocopy means a tape write is in flight --------------------------
    # tasklist, case-insensitive: a WMI Name='robocopy.exe' filter MISSES it,
    # because the image registers as 'Robocopy.exe' with a capital R.
    $rc = tasklist | Select-String -Pattern 'robocopy' -CaseSensitive:$false
    if ($rc) { $refusals += 'robocopy is running (tape write in flight)' }

    # --- 3. advisory lock ------------------------------------------------------
    $locks = Invoke-Psql "SELECT count(*) FROM pg_locks WHERE locktype='advisory';"
    if ([int]$locks -ne 0) { $refusals += "archiver advisory lock still held (count=$locks)" }

    # --- 4. is there an active session with work left? -------------------------
    $sessionId = Invoke-Psql @"
SELECT s.session_id FROM remote_sessions s
WHERE s.status='active'
  AND EXISTS (SELECT 1 FROM remote_chunks c
              WHERE c.session_id=s.session_id AND c.status<>'done')
ORDER BY s.session_id DESC LIMIT 1;
"@
    if (-not $sessionId) {
        Write-Log 'OK' 'no active session with pending chunks - nothing to resume'
        exit 0
    }

    $pending = Invoke-Psql "SELECT count(*) FROM remote_chunks WHERE session_id=$sessionId AND status<>'done';"
    $tapeLabel = Invoke-Psql "SELECT tape_label FROM remote_sessions WHERE session_id=$sessionId;"

    # --- 5. an ambiguous 'backing' chunk must never be auto-resumed ------------
    $backing = Invoke-Psql "SELECT count(*) FROM remote_chunks WHERE session_id=$sessionId AND status='backing';"
    if ([int]$backing -ne 0) {
        $refusals += "session $sessionId has $backing chunk(s) in 'backing' - on-tape outcome is unknowable; a human must reconcile"
    }

    # --- 6/7. drive + cartridge: NO LTFS ACCESS FROM THE WATCHDOG -------------
    # Until 2026-07-31 this block ran `Test-Path Z:\` and `cmd /c vol Z:` on
    # every tick the archiver was down -- filesystem and volume access to a tape
    # device from a process holding no lock at all. The archiver's in-process
    # RLock could not serialise it, so those probes could land during another
    # process's tape operation.
    #
    # The watchdog now decides from process/database/status-file state only.
    # Cartridge identity is NOT checked here: it is verified by the archiver's
    # own pre-write gate (_verify_mounted_cartridge), which runs while holding
    # the authoritative cross-process LTFS mutex and fails closed on a mismatch.
    # A restart with the wrong cartridge therefore stops safely at the gate
    # instead of being pre-screened by an unsynchronised probe here.
    $drive = (Get-ConfigValue -Section 'HARDWARE' -Key 'lto_drive') -replace '\\\\', '\'
    if (-not $drive) { $refusals += 'could not read [HARDWARE] lto_drive' }

    # A recorded hard stop must not be auto-resumed into. This reads the
    # archiver's own status file -- no device access. The decision (reason AND
    # exit code, fail closed) lives in Get-RecordedFailureRefusal above.
    $failurePath = Join-Path $PSScriptRoot '..\backup_logs\last_failure.json'
    if (Test-Path $failurePath) {
        try {
            $failure = Get-Content $failurePath -Raw | ConvertFrom-Json
            $failureRefusal = Get-RecordedFailureRefusal $failure
            if ($failureRefusal) { $refusals += $failureRefusal }
        } catch {
            $refusals += 'could not parse backup_logs/last_failure.json'
        }
    }

    # The process exit code of the last scheduled archiver run. Task Scheduler
    # state only -- no device access. Unreadable => refuse (fail closed).
    try {
        $taskInfo = Get-ScheduledTaskInfo -TaskName $TaskName -ErrorAction Stop
        $taskRefusal = Get-TaskResultRefusal $taskInfo.LastTaskResult
    } catch {
        $taskRefusal = "could not read the last result of scheduled task '$TaskName'"
    }
    if ($taskRefusal) { $refusals += $taskRefusal }

    # --- 8. never auto-eject ---------------------------------------------------
    $eject = Get-ConfigValue -Section 'HARDWARE' -Key 'eject_after_session'
    if ($eject -and $eject.Trim().ToLower() -in @('1', 'true', 'yes', 'on')) {
        $refusals += "eject_after_session is '$eject' - refusing to start a run that would eject the cartridge unattended"
    }

    # --- decide ----------------------------------------------------------------
    if ($refusals.Count -gt 0) {
        Write-Log 'REFUSE' ("session {0}: {1}" -f $sessionId, ($refusals -join ' | '))
        exit 2
    }

    if ($WhatIfOnly) {
        Write-Log 'DRYRUN' "all checks passed - would start '$TaskName' for session $sessionId ($pending chunk(s) left, tape $tapeLabel)"
        exit 0
    }

    Write-Log 'START' "all checks passed - starting '$TaskName' for session $sessionId ($pending chunk(s) left, tape $tapeLabel)"
    schtasks /Run /TN $TaskName | Out-Null
    Start-Sleep -Seconds 15
    $now = @(Get-CimInstance Win32_Process -Filter "Name='python.exe'" -ErrorAction SilentlyContinue |
             Where-Object { $_.CommandLine -and $_.CommandLine -match 'remote-archive' })
    if ($now.Count -gt 0) {
        Write-Log 'OK' ("archiver started (PID {0})" -f ($now.ProcessId -join ','))
        exit 0
    }
    Write-Log 'ALERT' "started '$TaskName' but no archiver process appeared - investigate"
    exit 3
}
catch {
    Write-Log 'ALERT' "watchdog error: $($_.Exception.Message)"
    exit 4
}
