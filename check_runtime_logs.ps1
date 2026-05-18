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

function Get-LogTail {
    param(
        [string]$Path,
        [int]$LineCount = 40
    )
    if (-not (Test-Path -LiteralPath $Path)) {
        return @()
    }
    return Get-Content -LiteralPath $Path -Tail $LineCount
}

function Get-LatestStartedBuilds {
    param([string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) {
        return @()
    }
    $builds = New-Object System.Collections.Generic.List[string]
    foreach ($line in Get-Content -LiteralPath $Path) {
        if ($line -match 'app start build=([0-9]{8}-[0-9]+)') {
            $build = $Matches[1]
            if (-not $builds.Contains($build)) {
                $builds.Add($build)
            }
        }
    }
    return @($builds)
}

function Split-ErrorEntries {
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

$currentBuildId = Get-CurrentBuildId -Path $sourceFile
$startedBuilds = Get-LatestStartedBuilds -Path $activityLog
$latestStartedBuild = if ($startedBuilds.Count -gt 0) { $startedBuilds[-1] } else { '' }
$entries = Split-ErrorEntries -Path $errorLog
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

Write-Host '--- activity.log tail ---'
Get-LogTail -Path $activityLog -LineCount 20 | ForEach-Object { Write-Host $_ }
Write-Host '--- error.log tail ---'
Get-LogTail -Path $errorLog -LineCount 60 | ForEach-Object { Write-Host $_ }

if ($FailOnCurrentBuildError -and $currentBuildErrors.Count -gt 0) {
    $entryWord = if ($currentBuildErrors.Count -eq 1) { 'entry' } else { 'entries' }
    throw ("Current source build {0} has {1} runtime error {2} in dist\error.log" -f $currentBuildId, $currentBuildErrors.Count, $entryWord)
}
