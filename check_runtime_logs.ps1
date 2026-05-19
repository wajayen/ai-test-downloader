param(
    [string]$ProjectRoot = 'C:\antigravity\ai_test',
    [switch]$FailOnCurrentBuildError
)

$ErrorActionPreference = 'Stop'

$sourceFile = Join-Path $ProjectRoot 'downloader.py'
$distDir = Join-Path $ProjectRoot 'dist'
$errorLog = Join-Path $distDir 'error.log'
$activityLog = Join-Path $distDir 'activity.log'

function Get-CurrentBuildId {
    param([string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) {
        return ''
    }
    $match = Get-Content -LiteralPath $Path |
        Select-String 'APP_BUILD = "([^"]+)"' |
        Select-Object -First 1
    if ($match) {
        return $match.Matches[0].Groups[1].Value
    }
    return ''
}

function Get-LatestStartedBuilds {
    param([string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) {
        return @()
    }
    $builds = New-Object System.Collections.Generic.List[string]
    $pendingAppStart = $false
    foreach ($line in Get-Content -LiteralPath $Path) {
        if ($line -match 'app start build=([0-9]{8}-[0-9]+)') {
            $build = $Matches[1]
            if (-not $builds.Contains($build)) {
                $builds.Add($build)
            }
            $pendingAppStart = $false
            continue
        }
        if ($line -match '^\[[^\]]+\]\s+app start\s*$') {
            $pendingAppStart = $true
            continue
        }
        if ($pendingAppStart -and $line -match '^build:\s*([0-9]{8}-[0-9]+)\s*$') {
            $build = $Matches[1]
            if (-not $builds.Contains($build)) {
                $builds.Add($build)
            }
            $pendingAppStart = $false
            continue
        }
        if ($line -match '^-+\s*$') {
            $pendingAppStart = $false
        }
    }
    return @($builds)
}

function Split-DelimitedLogEntries {
    param([string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) {
        return @()
    }
    $raw = Get-Content -LiteralPath $Path -Raw
    if (-not $raw) {
        return @()
    }
    return @(
        $raw -split '(?m)^-+\s*$' |
            ForEach-Object { $_.Trim() } |
            Where-Object { $_ }
    )
}

function Write-LogEntryTail {
    param(
        [object[]]$Entries,
        [int]$EntryCount = 3,
        [string]$EmptyMessage = '<no log entries>'
    )
    $tailEntries = @($Entries | Select-Object -Last $EntryCount)
    if ($tailEntries.Count -eq 0) {
        Write-Host $EmptyMessage
        return
    }
    foreach ($entry in $tailEntries) {
        Write-Host $entry
        Write-Host '--------------------------------------------------------------------------------'
    }
}

function Get-TraceLogContexts {
    param([string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) {
        return @()
    }
    $source = Get-Content -LiteralPath $Path -Raw
    $match = [regex]::Match($source, 'TRACE_LOG_CONTEXTS\s*=\s*frozenset\(\((?<body>.*?)\)\)', [System.Text.RegularExpressions.RegexOptions]::Singleline)
    if (-not $match.Success) {
        return @()
    }
    return @(
        [regex]::Matches($match.Groups['body'].Value, '["''](?<context>[^"'']+)["'']') |
            ForEach-Object { $_.Groups['context'].Value } |
            Where-Object { $_ }
    )
}

function Remove-InformationalErrorEntries {
    param(
        [object[]]$Entries,
        [string[]]$InformationalContexts
    )
    $contextSet = New-Object 'System.Collections.Generic.HashSet[string]' ([System.StringComparer]::OrdinalIgnoreCase)
    foreach ($context in @($InformationalContexts)) {
        if ($context) {
            [void]$contextSet.Add($context)
        }
    }
    return @(
        $Entries | Where-Object {
            $entry = [string]$_
            $isInformational = $false
            foreach ($context in $contextSet) {
                if ($entry -match ('(?m)^\[[^\]]+\]\s+' + [regex]::Escape($context) + '\s*$')) {
                    $isInformational = $true
                    break
                }
            }
            -not $isInformational
        }
    )
}

function Write-ErrorEntryTail {
    param(
        [object[]]$Entries,
        [int]$EntryCount = 3
    )
    $tailEntries = @($Entries | Select-Object -Last $EntryCount)
    if ($tailEntries.Count -eq 0) {
        Write-Host '<no non-informational error entries>'
        return
    }
    foreach ($entry in $tailEntries) {
        Write-Host $entry
        Write-Host '--------------------------------------------------------------------------------'
    }
}

$currentBuildId = Get-CurrentBuildId -Path $sourceFile
$startedBuilds = @(Get-LatestStartedBuilds -Path $activityLog)
$latestStartedBuild = if ($startedBuilds.Count -gt 0) { $startedBuilds[$startedBuilds.Count - 1] } else { '' }
$traceLogContexts = @(Get-TraceLogContexts -Path $sourceFile)
$activityEntries = @(Split-DelimitedLogEntries -Path $activityLog)
$entries = Remove-InformationalErrorEntries -Entries (Split-DelimitedLogEntries -Path $errorLog) -InformationalContexts $traceLogContexts
$latestStartedErrors = @()
if ($latestStartedBuild) {
    $latestStartedErrors = @($entries | Where-Object { $_ -match ("build:\s*" + [regex]::Escape($latestStartedBuild)) })
}
$currentBuildErrors = @()
if ($currentBuildId) {
    $currentBuildErrors = @($entries | Where-Object { $_ -match ("build:\s*" + [regex]::Escape($currentBuildId)) })
}

Write-Host 'Runtime log pre-fix check'
Write-Host ("Project root: {0}" -f $ProjectRoot)
Write-Host ("Source build: {0}" -f ($(if ($currentBuildId) { $currentBuildId } else { '<unknown>' })))
Write-Host ("Execution logs: {0}, {1}" -f $errorLog, $activityLog)
Write-Host ("Latest started build: {0}" -f ($(if ($latestStartedBuild) { $latestStartedBuild } else { '<none>' })))

if ($startedBuilds.Count -gt 0) {
    Write-Host ("Started builds seen: {0}" -f (($startedBuilds | Select-Object -Last 5) -join ', '))
}

if ($latestStartedErrors.Count -gt 0) {
    $entryWord = if ($latestStartedErrors.Count -eq 1) { 'entry' } else { 'entries' }
    Write-Warning ("Found {0} error {1} for latest started build {2}. These must be inspected and fixed with the next code change." -f $latestStartedErrors.Count, $entryWord, $latestStartedBuild)
    $latestStartedErrors | Select-Object -Last 3 | ForEach-Object {
        Write-Host '--- latest started build error ---'
        Write-Host $_
    }
} else {
    Write-Host 'No error entry found for the latest started build.'
}

Write-Host '--- activity.log entries tail ---'
Write-LogEntryTail -Entries $activityEntries -EntryCount 3 -EmptyMessage '<no activity log entries>'
Write-Host '--- relevant error entries tail (latest/current build, filtered) ---'
$relevantErrors = @()
if ($latestStartedErrors.Count -gt 0) {
    $relevantErrors = @($latestStartedErrors)
} elseif ($currentBuildErrors.Count -gt 0) {
    $relevantErrors = @($currentBuildErrors)
}
Write-ErrorEntryTail -Entries $relevantErrors -EntryCount 3

if ($FailOnCurrentBuildError -and $currentBuildErrors.Count -gt 0) {
    $entryWord = if ($currentBuildErrors.Count -eq 1) { 'entry' } else { 'entries' }
    throw ("Current source build {0} has {1} runtime error {2} in dist\error.log" -f $currentBuildId, $currentBuildErrors.Count, $entryWord)
}
