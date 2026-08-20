<#
.SYNOPSIS
    Evacuate a campaign metadata/container store off a failing drive, in
    irreplaceable-first order.

.DESCRIPTION
    Written for the case where the source drive is dying: it may vanish
    mid-copy and may never come back. Three consequences shape this script.

    1. ORDER MATTERS MORE THAN SPEED. Receipts are copied before containers.
       A receipt is a few hundred bytes and is the only thing that can later
       prove a container is intact; a partial evacuation that saved every
       container but no receipt leaves unverifiable data. Incident evidence
       and small artifacts follow, then the bulk containers last.

    2. DO NOT HAMMER A DYING DRIVE. Robocopy runs with a single retry and a
       one-second wait. Long retry storms generate heat and head activity on
       a drive that is already failing, and buy nothing: if a read fails
       twice it will usually fail two hundred times.

    3. EVERY RUN IS RESUMABLE. /XC /XN /XO copies only what the destination
       is missing, and /Z restarts a large file mid-transfer rather than
       from the beginning, so re-running after a disconnect picks up where
       the last attempt stopped.

    The script never writes to, deletes from, or repairs the source. Verify
    the evacuated copy afterwards with:

        python scripts/verify_campaign_store.py --root <Destination>\LTO_METADATA\LOCAL_MANIFEST_ARCHIVE\campaign_tape03 --mode full

.PARAMETER Source
    Root of the failing store, e.g. 'E:\OWC-LTO'.

.PARAMETER Destination
    Root on the healthy target drive, e.g. 'G:\OWC-LTO-RESCUE'.

.PARAMETER LogDirectory
    Where per-group robocopy logs are written. Defaults to the destination.
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$Source,
    [Parameter(Mandatory = $true)][string]$Destination,
    [string]$LogDirectory
)

$ErrorActionPreference = 'Continue'

if (-not $LogDirectory) { $LogDirectory = Join-Path $Destination '_evacuation_logs' }
New-Item -ItemType Directory -Force -Path $Destination, $LogDirectory | Out-Null

$stamp = Get-Date -Format 'yyyyMMdd_HHmmss'

# Robocopy exit codes are a bit field: <8 means success (files copied, extra
# files present, etc.); >=8 means at least one file genuinely failed.
function Test-RobocopySuccess([int]$code) { return $code -lt 8 }

# Ordered irreplaceable-first. 'Files' limits a group to matching names so the
# receipts can be lifted out ahead of the multi-gigabyte containers beside them.
# Order is by irreplaceability, not by size or convenience. Receipts are
# first because they are tiny and are what makes everything after them
# provable. The bulk containers come next: they are the only copy of the
# campaign. Vendor diagnostics and database dumps are LAST despite being
# small, because both already exist elsewhere — spending a dying drive's
# remaining working minutes on a duplicate would be the wrong trade.
$groups = @(
    @{ Name = 'receipts';    Rel = 'LTO_METADATA'; Files = @('receipt.json', '*.jsonl.zst', '*.json') }
    @{ Name = 'containers';  Rel = 'LTO_METADATA'; Files = @() }
    @{ Name = 'staging';     Rel = 'LTO_CAMPAIGN_STAGING'; Files = @() }
    @{ Name = 'diagnostics'; Rel = 'LTO_DIAG';     Files = @() }
    @{ Name = 'db_backups';  Rel = 'DB_BACKUPS';   Files = @() }
)

$results = @()
foreach ($group in $groups) {
    $from = Join-Path $Source $group.Rel
    if (-not (Test-Path -LiteralPath $from)) {
        Write-Host "[SKIP] $($group.Name): $from is not present"
        continue
    }
    $to  = Join-Path $Destination $group.Rel
    $log = Join-Path $LogDirectory "$stamp`_$($group.Name).log"

    # /E recurse · /Z restart large files mid-transfer · /R:1 /W:1 minimal
    # retry on failing media · /XC /XN /XO copy only what is missing (resume)
    # · /DCOPY:DAT keep directory timestamps · /NP quiet log · /NFL /NDL keep
    # the log to summaries and failures rather than every filename.
    $arguments = @($from, $to) + $group.Files + @(
        '/E', '/Z', '/R:1', '/W:1', '/XC', '/XN', '/XO',
        '/DCOPY:DAT', '/NP', '/NFL', '/NDL', '/TEE', "/LOG+:$log")

    Write-Host "[COPY] $($group.Name): $from -> $to"
    $started = Get-Date
    & robocopy.exe @arguments | Out-Null
    $code = $LASTEXITCODE
    $ok = Test-RobocopySuccess $code

    $results += [pscustomobject]@{
        Group    = $group.Name
        ExitCode = $code
        Ok       = $ok
        Minutes  = [math]::Round(((Get-Date) - $started).TotalMinutes, 1)
        Log      = $log
    }
    Write-Host ("[{0}] {1}: robocopy exit {2} after {3} min" -f
        $(if ($ok) { 'DONE' } else { 'FAIL' }), $group.Name, $code,
        $results[-1].Minutes)

    # A vanished source is not a per-file error to push through: stop, so the
    # operator can power-cycle before the next attempt instead of the script
    # burning retries against a drive that is no longer there.
    if (-not (Test-Path -LiteralPath $from)) {
        Write-Warning "Source disappeared during '$($group.Name)'. Stopping; re-run to resume."
        break
    }
}

Write-Host "`n==== Evacuation summary ($stamp) ===="
$results | Format-Table -AutoSize
$failed = @($results | Where-Object { -not $_.Ok })
if ($failed) {
    Write-Warning "$($failed.Count) group(s) reported failures - re-run this script to resume; inspect the logs in $LogDirectory"
    exit 1
}
Write-Host "All groups copied. Now verify the destination copy against its receipts."
exit 0
