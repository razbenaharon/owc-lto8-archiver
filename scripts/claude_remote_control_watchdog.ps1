<#
.SYNOPSIS
    Restart the Claude Remote Control session if it has exited. Never stop it.

.DESCRIPTION
    Runs every couple of minutes from the Claude-Remote-Control-Watchdog task.
    Its entire job is: if our Remote Control process is gone, ask Task Scheduler
    to start Claude-Remote-Control again, subject to a cooldown and a backoff.

    It is a start-only watchdog. There is no Stop-Process, no schtasks /End and
    no signal delivery anywhere in this file, by design:

      * A healthy Remote Control session is silent for hours at a time. Killing
        it because a log looked stale is exactly the failure mode that cost the
        backup ~15 hours on 2026-07-28.
      * The one thing that must never happen on this host is an assistant tool
        reaching the archiver. This script therefore never enumerates, signals or
        restarts the archiver, robocopy, LTFS, PostgreSQL, LTO-Archive-Resume or
        LTO-Archive-Watchdog. It looks at claude.exe and at one scheduled task.

    HONEST LIMITATION
      There is no supported way to ask a running Claude Code process whether its
      Remote Control transport is still connected to Anthropic. The CLI exposes
      no health command, and the debug log records startup only. So "healthy"
      here means the process exists and authentication is valid -- process-level
      liveness, not connection-level liveness. A process that is alive but
      silently disconnected will NOT be detected. Nothing in this script pretends
      otherwise, and no health endpoint has been invented to paper over it.

.NOTES
    Backoff: 3 starts inside 30 min -> 15 min cooldown; 6 -> 60 min plus ALERT.
    The counter resets once a session has survived 15 minutes.
#>
[CmdletBinding()]
param(
    [string]$TaskName    = 'Claude-Remote-Control',
    [string]$SessionName = 'owc-lto8-archiver-rc',
    [switch]$WhatIfOnly
)

$ErrorActionPreference = 'Stop'

$StateDir  = Join-Path $env:LOCALAPPDATA 'ClaudeRemoteControl'
$LogDir    = Join-Path $StateDir 'logs'
$LogFile   = Join-Path $LogDir 'watchdog.log'
$StateFile = Join-Path $StateDir 'watchdog.state.json'

foreach ($d in @($StateDir, $LogDir)) {
    if (-not (Test-Path $d)) { New-Item -ItemType Directory -Path $d -Force | Out-Null }
}

function Write-Log {
    param([string]$Level, [string]$Message)
    $line = "{0} [{1}] {2}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $Level, $Message
    Write-Output $line
    try { Add-Content -Path $LogFile -Value $line -Encoding UTF8 } catch { }
}

function Get-State {
    if (Test-Path $StateFile) {
        try { return (Get-Content $StateFile -Raw | ConvertFrom-Json) } catch { }
    }
    [PSCustomObject]@{ lastStartUtc = $null; consecutiveStarts = 0; authAlerted = $false }
}

function Save-State {
    param($State)
    try { $State | ConvertTo-Json | Set-Content -Path $StateFile -Encoding UTF8 } catch { }
}

$MinCooldownSec  = 120     # never restart more than once every 2 minutes
$BurstWindowMin  = 30      # window over which restarts are counted as a burst
$HealthySec      = 900     # a session alive this long clears the burst counter

try {
    $state = Get-State
    $nowUtc = (Get-Date).ToUniversalTime()

    # --- is our Remote Control process alive? --------------------------------
    # Matched on the command line, which is authoritative: it cannot be fooled by
    # a stale PID file or by an unrelated Claude session (VS Code, a terminal, or
    # an assistant harness) that happens to be running at the same time.
    $procs = @(Get-CimInstance Win32_Process -Filter "Name='claude.exe'" -ErrorAction SilentlyContinue |
               Where-Object { $_.CommandLine -and $_.CommandLine -match [regex]::Escape("--remote-control $SessionName") })

    if ($procs.Count -gt 1) {
        # Report, do not resolve. Killing the "extra" one risks killing the good
        # one, and a duplicate is a far smaller problem than an outage.
        Write-Log 'ALERT' ("{0} Remote Control processes match '{1}' (PID {2}) - duplicates should not happen; investigate, this script will not kill any of them" -f $procs.Count, $SessionName, ($procs.ProcessId -join ','))
        exit 0
    }

    if ($procs.Count -eq 1) {
        $p = $procs[0]
        $aliveSec = [int]($nowUtc - $p.CreationDate.ToUniversalTime()).TotalSeconds
        if ($aliveSec -ge $HealthySec -and $state.consecutiveStarts -ne 0) {
            Write-Log 'OK' "Remote Control alive ${aliveSec}s (PID $($p.ProcessId)) - clearing restart counter (was $($state.consecutiveStarts))"
            $state.consecutiveStarts = 0
            $state.authAlerted = $false
            Save-State $state
        }
        exit 0
    }

    # --- it is gone. Is the task already starting it? ------------------------
    $task = Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue
    if (-not $task) {
        Write-Log 'ALERT' "scheduled task '$TaskName' does not exist - cannot restart Remote Control"
        exit 3
    }
    if ($task.State -eq 'Running') {
        # The launcher is up but Claude has not appeared yet (startup takes a few
        # seconds), or the launcher is between guards. Either way, IgnoreNew would
        # refuse a second instance, so there is nothing useful to do.
        Write-Log 'OK' "task '$TaskName' is already Running - launcher is starting Claude; not launching a duplicate"
        exit 0
    }
    if ($task.State -eq 'Disabled') {
        Write-Log 'ALERT' "task '$TaskName' is Disabled - Remote Control will not restart until it is enabled"
        exit 3
    }

    # --- authentication: a problem no amount of restarting will fix -----------
    $launcherLog = Join-Path $LogDir 'launcher.log'
    if (Test-Path $launcherLog) {
        $recentAuth = Get-Content $launcherLog -Tail 40 -ErrorAction SilentlyContinue |
                      Where-Object { $_ -match '\[AUTH\]' } | Select-Object -Last 1
        if ($recentAuth) {
            $lastInfo = Get-ScheduledTaskInfo -TaskName $TaskName -ErrorAction SilentlyContinue
            if ($lastInfo -and $lastInfo.LastTaskResult -eq 10) {
                if (-not $state.authAlerted) {
                    Write-Log 'AUTH' "the launcher last exited with the NOT AUTHENTICATED code (10). Remote Control cannot recover automatically. A human must sign in: claude auth login. Suppressing further restart attempts until authentication succeeds."
                    $state.authAlerted = $true
                    Save-State $state
                }
                # Re-check now: if a human has since signed in, fall through.
                $stillBroken = $true
                try {
                    $exeGuess = Join-Path $env:USERPROFILE '.local\bin\claude.exe'
                    if (-not (Test-Path $exeGuess)) {
                        $extRoot = Join-Path $env:USERPROFILE '.vscode\extensions'
                        $exeGuess = (Get-ChildItem $extRoot -Directory -Filter 'anthropic.claude-code-*' -ErrorAction SilentlyContinue |
                                     ForEach-Object { Join-Path $_.FullName 'resources\native-binary\claude.exe' } |
                                     Where-Object { Test-Path $_ } | Select-Object -Last 1)
                    }
                    if ($exeGuess) {
                        $a = (& $exeGuess auth status --json 2>&1 | Out-String).Trim() | ConvertFrom-Json
                        $stillBroken = -not [bool]$a.loggedIn
                    }
                } catch { }
                if ($stillBroken) {
                    Write-Log 'AUTH' 'still not authenticated - not restarting (this is not a recoverable failure)'
                    exit 10
                }
                Write-Log 'OK' 'authentication restored - resuming normal restart behaviour'
                $state.authAlerted = $false
                $state.consecutiveStarts = 0
            }
        }
    }

    # --- cooldown and backoff ------------------------------------------------
    $required = $MinCooldownSec
    if     ($state.consecutiveStarts -ge 6) { $required = 3600 }
    elseif ($state.consecutiveStarts -ge 3) { $required = 900 }

    if ($state.lastStartUtc) {
        $since = [int]($nowUtc - [datetime]::Parse($state.lastStartUtc).ToUniversalTime()).TotalSeconds
        if ($since -lt $required) {
            Write-Log 'WAIT' "Remote Control is down, but only ${since}s since the last restart (need ${required}s; $($state.consecutiveStarts) recent restart(s)) - backing off"
            exit 0
        }
        if ($since -gt ($BurstWindowMin * 60)) { $state.consecutiveStarts = 0 }
    }

    if ($state.consecutiveStarts -ge 6) {
        Write-Log 'ALERT' "Remote Control has been restarted $($state.consecutiveStarts) times and keeps dying - restarting hourly now. Check $LogDir\launcher.log and the debug logs; this needs a human."
    }

    if ($WhatIfOnly) {
        Write-Log 'DRYRUN' "would run: schtasks /Run /TN $TaskName (restart #$($state.consecutiveStarts + 1))"
        exit 0
    }

    Write-Log 'START' "Remote Control process is gone and task '$TaskName' is '$($task.State)' - starting it (restart #$($state.consecutiveStarts + 1))"
    schtasks /Run /TN $TaskName | Out-Null

    $state.lastStartUtc = $nowUtc.ToString('o')
    $state.consecutiveStarts = [int]$state.consecutiveStarts + 1
    Save-State $state

    Start-Sleep -Seconds 20
    $now = @(Get-CimInstance Win32_Process -Filter "Name='claude.exe'" -ErrorAction SilentlyContinue |
             Where-Object { $_.CommandLine -and $_.CommandLine -match [regex]::Escape("--remote-control $SessionName") })
    if ($now.Count -gt 0) {
        Write-Log 'OK' ("Remote Control restarted (PID {0})" -f ($now.ProcessId -join ','))
        exit 0
    }
    Write-Log 'ALERT' "ran '$TaskName' but no Remote Control process appeared within 20s - see launcher.log"
    exit 4
}
catch {
    Write-Log 'ALERT' "watchdog error: $($_.Exception.Message)"
    exit 5
}
