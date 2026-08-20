<#
.SYNOPSIS
    Wait for a failing source drive to genuinely come back, then evacuate it.

.DESCRIPTION
    Written because the obvious readiness checks all lie about removable media.
    A volume that drops while still mounted keeps answering Test-Path,
    Get-Volume and directory listings from cached metadata, and a short read
    of a recently-touched file can be served from the file cache without the
    platter ever being touched. Acting on any of those starts a multi-hundred-
    gigabyte copy against a drive that is not there, which at best fails
    across the board and at worst hammers dying hardware.

    Readiness here therefore requires BOTH:
      1. the physical disk is enumerated and Online (Get-Disk), and
      2. a deep seek-and-read far into a large file succeeds — past anything
         the cache would plausibly hold.

    Only then does it hand off to evacuate_campaign_store.ps1.

.PARAMETER Source
    Root of the failing store, e.g. 'E:\OWC-LTO'.

.PARAMETER Destination
    Root on healthy storage, e.g. 'G:\OWC-LTO-RESCUE'.

.PARAMETER ProbeFile
    A large file on the source used for the deep read.

.PARAMETER ProbeOffset
    Byte offset for the deep read. Must sit well inside ProbeFile.

.PARAMETER TimeoutMinutes
    Give up after this long. 0 waits indefinitely.

.PARAMETER PollSeconds
    Interval between checks. Deliberately unhurried: polling a dead drive
    faster does not bring it back sooner.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$Source,
    [Parameter(Mandatory = $true)][string]$Destination,
    [Parameter(Mandatory = $true)][string]$ProbeFile,
    [long]$ProbeOffset = 1500000000,
    [int]$TimeoutMinutes = 0,
    [int]$PollSeconds = 30
)

function Test-DeepRead {
    param([string]$Path, [long]$Offset)
    try {
        $stream = [System.IO.File]::OpenRead($Path)
    } catch { return $false }
    try {
        if ($stream.Length -le $Offset) { $Offset = [long]($stream.Length / 2) }
        $stream.Seek($Offset, 'Begin') | Out-Null
        $buffer = New-Object byte[] 65536
        return $stream.Read($buffer, 0, 65536) -gt 0
    } catch {
        return $false
    } finally {
        $stream.Dispose()
    }
}

function Test-SourceDiskOnline {
    param([string]$Path)
    $letter = ($Path -replace '^([A-Za-z]):.*$', '$1')
    $partition = Get-Partition -DriveLetter $letter -ErrorAction SilentlyContinue
    if (-not $partition) { return $false }
    $disk = Get-Disk -Number $partition.DiskNumber -ErrorAction SilentlyContinue
    return $disk -and $disk.OperationalStatus -eq 'Online'
}

$deadline = if ($TimeoutMinutes -gt 0) { (Get-Date).AddMinutes($TimeoutMinutes) } else { $null }
Write-Host "Waiting for $Source to come back (disk enumerated + deep read at offset $ProbeOffset)..."

while ($true) {
    $diskOnline = Test-SourceDiskOnline -Path $Source
    $deepRead = $false
    if ($diskOnline) { $deepRead = Test-DeepRead -Path $ProbeFile -Offset $ProbeOffset }

    if ($diskOnline -and $deepRead) {
        Write-Host "$(Get-Date -Format o)  Source is genuinely alive. Starting evacuation."
        break
    }

    $state = if (-not $diskOnline) { 'disk not enumerated' } else { 'disk present but deep read failed' }
    Write-Host "$(Get-Date -Format o)  not ready ($state)"

    if ($deadline -and (Get-Date) -gt $deadline) {
        Write-Warning "Source did not return within $TimeoutMinutes minutes. No evacuation attempted."
        exit 3
    }
    Start-Sleep -Seconds $PollSeconds
}

$evacuate = Join-Path $PSScriptRoot 'evacuate_campaign_store.ps1'
& $evacuate -Source $Source -Destination $Destination
exit $LASTEXITCODE
