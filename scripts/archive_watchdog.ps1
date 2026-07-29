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

    # --- 6/7. drive letter and the CARTRIDGE THAT IS ACTUALLY LOADED ----------
    $drive = (Get-ConfigValue -Section 'HARDWARE' -Key 'lto_drive') -replace '\\\\', '\'
    if (-not $drive) { $refusals += 'could not read [HARDWARE] lto_drive' }
    elseif (-not (Test-Path $drive)) { $refusals += "LTO drive $drive is not present" }
    else {
        $letter = $drive.TrimEnd('\', ':')
        $volLine = (cmd /c "vol ${letter}:" 2>&1) -join ' '
        if ($volLine -match 'Volume in drive \w+ is (\S+)') {
            $loaded = $Matches[1]
            if ($loaded -ne $tapeLabel) {
                $refusals += "wrong cartridge: loaded='$loaded' but session $sessionId expects '$tapeLabel'"
            }
        } else {
            $refusals += "could not read the volume label of $drive"
        }
    }

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
