<#
.SYNOPSIS
    Launch a single, durable Claude Code Remote Control session for this project.

.DESCRIPTION
    On 2026-07-28 the archiver was launched as a background task of an assistant
    session. When that session's Remote Control dropped, the harness stopped its
    background tasks and the backup sat idle ~15 hours. scripts/archive_watchdog.ps1
    fixed the backup side of that. This script fixes the other side: it keeps a
    Remote Control session alive that is owned by the Task Scheduler service, not
    by VS Code, a terminal, an SSH session, or any assistant harness.

    It is deliberately one-shot. It starts at most one Claude process, waits for
    it, records why it exited, and returns. Restarting is somebody else's job
    (the Claude-Remote-Control-Watchdog task, plus Task Scheduler's own
    restart-on-failure). One restart owner is easier to reason about than three.

    WHY STDOUT IS NOT REDIRECTED
      Remote Control only exists on an interactive session, and an interactive
      session needs a TTY. Redirecting Claude's stdout to a file makes stdout a
      pipe, which puts the CLI into non-interactive mode and defeats the whole
      point. So Claude keeps the console it inherits, and diagnostics go to a
      separate file via the CLI's own --debug-file option. This script logs the
      lifecycle (start / exit / refusal / auth failure) itself.

    SESSION CONTINUITY
      A UUID is minted once and stored in session.id. The first launch pins it
      with --session-id; every later launch reattaches with --resume. That is
      precise: unlike --continue, it can never latch onto some *other* recent
      conversation in this directory (such as an unrelated VS Code session).

.NOTES
    This script must never touch the backup. It does not read, signal, start or
    stop the archiver, robocopy, LTFS, PostgreSQL, LTO-Archive-Resume or
    LTO-Archive-Watchdog, and it never becomes their ancestor.

    Logs and state live under %LOCALAPPDATA%, not in the repo: this repo is
    public and the logs carry host paths and the operator's user name.
#>
[CmdletBinding()]
param(
    # Full path to claude.exe. Left empty, it is resolved (see Resolve-ClaudeExe).
    [string]$ClaudeExe,

    # Remote Control display name. Doubles as the marker this script and the
    # watchdog use to recognise *our* process among any other Claude processes.
    [string]$SessionName = 'owc-lto8-archiver-rc',

    # Ignore the stored session id and start a brand new conversation.
    [switch]$FreshSession,

    # Report what would happen and exit without launching.
    [switch]$WhatIfOnly
)

# Task Scheduler and Windows Terminal can ignore -WindowStyle Hidden for
# console applications. Hide the inherited console immediately while retaining
# the TTY that Claude Remote Control requires.
try {
    Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;
public static class ClaudeRemoteConsoleWindow {
    [DllImport("kernel32.dll")]
    public static extern IntPtr GetConsoleWindow();

    [DllImport("user32.dll")]
    [return: MarshalAs(UnmanagedType.Bool)]
    public static extern bool ShowWindow(IntPtr hWnd, int nCmdShow);
}
'@ -ErrorAction Stop
    $consoleWindow = [ClaudeRemoteConsoleWindow]::GetConsoleWindow()
    if ($consoleWindow -ne [IntPtr]::Zero) {
        [void][ClaudeRemoteConsoleWindow]::ShowWindow($consoleWindow, 0)
    }
} catch { }

$ErrorActionPreference = 'Stop'

$ProjectRoot = Split-Path -Parent $PSScriptRoot
$StateDir    = Join-Path $env:LOCALAPPDATA 'ClaudeRemoteControl'
$LogDir      = Join-Path $StateDir 'logs'
$DebugDir    = Join-Path $StateDir 'debug'
$LogFile     = Join-Path $LogDir 'launcher.log'
$SessionFile = Join-Path $StateDir 'session.id'

foreach ($d in @($StateDir, $LogDir, $DebugDir)) {
    if (-not (Test-Path $d)) { New-Item -ItemType Directory -Path $d -Force | Out-Null }
}

function Write-Log {
    param([string]$Level, [string]$Message)
    $line = "{0} [{1}] {2}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $Level, $Message
    Write-Output $line
    try { Add-Content -Path $LogFile -Value $line -Encoding UTF8 } catch { }
}

# --- exit codes the watchdog understands -----------------------------------
#   0  Claude exited normally          -> restartable
#   3  refused: already running        -> nothing to do
#  10  not authenticated               -> a human must run 'claude auth login'
#   4  launcher error                  -> restartable, but log it
$EXIT_OK = 0; $EXIT_DUPLICATE = 3; $EXIT_LAUNCHER_ERROR = 4; $EXIT_AUTH = 10

function Resolve-ClaudeExe {
    # Explicit wins, then the environment override, then a native install at a
    # stable path, then the newest VS Code extension build. The extension path
    # is version-stamped and moves on every extension update, so it is the last
    # resort and is re-resolved on every launch rather than baked in anywhere.
    if ($ClaudeExe) {
        if (-not (Test-Path $ClaudeExe)) { throw "-ClaudeExe '$ClaudeExe' does not exist" }
        return (Resolve-Path $ClaudeExe).Path
    }
    if ($env:CLAUDE_REMOTE_CONTROL_EXE -and (Test-Path $env:CLAUDE_REMOTE_CONTROL_EXE)) {
        return (Resolve-Path $env:CLAUDE_REMOTE_CONTROL_EXE).Path
    }
    $native = Join-Path $env:USERPROFILE '.local\bin\claude.exe'
    if (Test-Path $native) { return $native }

    $extRoot = Join-Path $env:USERPROFILE '.vscode\extensions'
    if (Test-Path $extRoot) {
        $candidates = Get-ChildItem $extRoot -Directory -Filter 'anthropic.claude-code-*' |
            ForEach-Object {
                $exe = Join-Path $_.FullName 'resources\native-binary\claude.exe'
                if (Test-Path $exe) {
                    $v = $null
                    if ($_.Name -match 'anthropic\.claude-code-(\d+\.\d+\.\d+)') {
                        [void][version]::TryParse($Matches[1], [ref]$v)
                    }
                    [PSCustomObject]@{ Exe = $exe; Version = $v }
                }
            } | Where-Object { $_ } | Sort-Object Version -Descending
        if ($candidates) { return $candidates[0].Exe }
    }
    throw 'could not locate claude.exe (pass -ClaudeExe or set CLAUDE_REMOTE_CONTROL_EXE)'
}

function Get-OurRemoteControlProcesses {
    param([string]$Name)
    # The command line is the only authoritative marker. It survives a launcher
    # crash, a stale PID file, and a reboot, which a mutex or PID file does not.
    @(Get-CimInstance Win32_Process -Filter "Name='claude.exe'" -ErrorAction SilentlyContinue |
      Where-Object { $_.CommandLine -and $_.CommandLine -match [regex]::Escape("--remote-control $Name") })
}

function Remove-OldFiles {
    param([string]$Path, [string]$Filter, [int]$Keep = 10)
    try {
        Get-ChildItem $Path -Filter $Filter -File -ErrorAction SilentlyContinue |
            Sort-Object LastWriteTime -Descending | Select-Object -Skip $Keep |
            Remove-Item -Force -ErrorAction SilentlyContinue
    } catch { }
}

$mutex = $null
try {
    $exe = Resolve-ClaudeExe
    $version = (& $exe --version 2>&1 | Out-String).Trim()

    # --- duplicate guard, layer 1: a named mutex closes the start-up race -----
    # Two watchdog ticks, or a watchdog racing the logon trigger, can both pass
    # the process scan below before either has spawned anything. The mutex makes
    # that window unreachable. Global\ first; Local\ if the privilege is absent.
    $created = $false
    try   { $mutex = New-Object System.Threading.Mutex($true, "Global\ClaudeRemoteControl_$SessionName", [ref]$created) }
    catch { $mutex = New-Object System.Threading.Mutex($true, "Local\ClaudeRemoteControl_$SessionName",  [ref]$created) }
    if (-not $created) {
        Write-Log 'REFUSE' "another launcher for '$SessionName' holds the mutex - not starting a second one"
        exit $EXIT_DUPLICATE
    }

    # --- duplicate guard, layer 2: is one already running? -------------------
    $existing = Get-OurRemoteControlProcesses -Name $SessionName
    if ($existing.Count -gt 0) {
        Write-Log 'REFUSE' ("Remote Control '{0}' already running (PID {1}) - not starting a second one" -f $SessionName, ($existing.ProcessId -join ','))
        exit $EXIT_DUPLICATE
    }

    # --- authentication ------------------------------------------------------
    # Checked before launching, because an unauthenticated Claude exits at once
    # and would otherwise spin the watchdog forever against a problem only a
    # human can fix.
    $loggedIn = $false; $authDetail = 'unparsed'
    try {
        $raw  = (& $exe auth status --json 2>&1 | Out-String).Trim()
        $auth = $raw | ConvertFrom-Json
        $loggedIn   = [bool]$auth.loggedIn
        $authDetail = "method=$($auth.authMethod) account=$($auth.email) plan=$($auth.subscriptionType)"
    } catch {
        $authDetail = "could not read auth status: $($_.Exception.Message)"
    }
    if (-not $loggedIn) {
        Write-Log 'AUTH' "NOT AUTHENTICATED ($authDetail). Remote Control cannot start. A human must run: `"$exe`" auth login"
        exit $EXIT_AUTH
    }

    # --- session continuity --------------------------------------------------
    $sessionId = $null
    if (-not $FreshSession -and (Test-Path $SessionFile)) {
        $stored = (Get-Content $SessionFile -Raw).Trim()
        if ($stored -match '^[0-9a-fA-F-]{36}$') { $sessionId = $stored }
    }
    $resuming = [bool]$sessionId
    if (-not $resuming) { $sessionId = [guid]::NewGuid().ToString() }
    $sessionArgs = if ($resuming) { @('--resume', $sessionId) } else { @('--session-id', $sessionId) }
    $mode = if ($resuming) { "resume $sessionId" } else { "new $sessionId" }

    $stamp     = Get-Date -Format 'yyyyMMdd-HHmmss'
    $debugFile = Join-Path $DebugDir "rc-$stamp.log"
    $claudeArgs = @('--remote-control', $SessionName) + $sessionArgs + @('--debug-file', $debugFile)

    if ($WhatIfOnly) {
        Write-Log 'DRYRUN' "would run: `"$exe`" $($claudeArgs -join ' ')  (cwd=$ProjectRoot, version=$version, $authDetail)"
        exit $EXIT_OK
    }

    Remove-OldFiles -Path $DebugDir -Filter 'rc-*.log' -Keep 10

    # Persisted only now, never on a dry run: a stored id whose conversation was
    # never created would make every later launch --resume a session that does
    # not exist.
    if (-not $resuming) { Set-Content -Path $SessionFile -Value $sessionId -Encoding ASCII }

    Write-Log 'START' "launching Remote Control '$SessionName' | exe=$exe | version=$version | cwd=$ProjectRoot | session=$mode | $authDetail | debug=$debugFile"

    Set-Location $ProjectRoot
    $startedAt = Get-Date

    # Foreground and inheriting this console on purpose: Claude needs the TTY,
    # and keeping this script alive for the whole session is what lets the task's
    # MultipleInstances=IgnoreNew act as the third duplicate guard.
    & $exe @claudeArgs
    $code = $LASTEXITCODE

    $ranFor = [int]((Get-Date) - $startedAt).TotalSeconds

    # A resume that dies almost immediately usually means the stored conversation
    # is gone or unreadable. Forgetting it makes the next launch start clean
    # instead of failing the same way forever.
    if ($resuming -and $code -ne 0 -and $ranFor -lt 20) {
        Remove-Item $SessionFile -Force -ErrorAction SilentlyContinue
        Write-Log 'WARN' "resume of $sessionId failed after ${ranFor}s (code $code) - discarded the stored session id; the next launch will start a fresh conversation"
    }

    Write-Log 'EXIT' "Remote Control '$SessionName' exited with code $code after ${ranFor}s - the watchdog will restart it"
    exit $EXIT_OK
}
catch {
    Write-Log 'ALERT' "launcher error: $($_.Exception.Message)"
    exit $EXIT_LAUNCHER_ERROR
}
finally {
    if ($mutex) { try { $mutex.ReleaseMutex() } catch { }; $mutex.Dispose() }
}
