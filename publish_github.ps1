param(
    [string]$RepoName = "ai-test-downloader",
    [string]$Visibility = "public",
    [string]$BuildId = "",
    [string]$VersionedExe = "",
    [string]$TagName = ""
)

$ErrorActionPreference = "Stop"

$gh = "C:\Program Files\GitHub CLI\gh.exe"
if (!(Test-Path -LiteralPath $gh)) {
    throw "GitHub CLI not found: $gh"
}

$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location -LiteralPath $repoRoot

& $gh auth status | Out-Null

if (-not $TagName -and $BuildId -match '-(\d+)$') {
    $TagName = "v$($Matches[1])"
}
if (-not $VersionedExe -and $BuildId) {
    $VersionedExe = Join-Path $repoRoot ("dist\downloader_{0}.exe" -f ($BuildId -replace '-', '_'))
}

if ($BuildId) {
    git -c safe.directory=$repoRoot add -u
    $hasTrackedChanges = $true
    try {
        git -c safe.directory=$repoRoot diff --cached --quiet
        $hasTrackedChanges = $false
    } catch {
        $hasTrackedChanges = $true
    }
    if ($hasTrackedChanges) {
        $commitLabel = if ($TagName) { $TagName } else { $BuildId }
        git -c safe.directory=$repoRoot commit -m ("Release {0}" -f $commitLabel)
    }
}

$originExists = $false
try {
    git -c safe.directory=$repoRoot remote get-url origin | Out-Null
    $originExists = $true
} catch {
    $originExists = $false
}

if (-not $originExists) {
    & $gh repo create $RepoName "--$Visibility" --source . --remote origin --push
} else {
    git -c safe.directory=$repoRoot push -u origin HEAD
}

if ($TagName) {
    git -c safe.directory=$repoRoot tag -f $TagName
    git -c safe.directory=$repoRoot push origin ("refs/tags/{0}" -f $TagName) --force
}

if ($TagName -and $VersionedExe) {
    if (!(Test-Path -LiteralPath $VersionedExe)) {
        throw "Versioned executable not found: $VersionedExe"
    }
    $releaseNotes = if ($BuildId) { "Automated release for build $BuildId" } else { "Automated release for $TagName" }
    $releaseExists = $true
    try {
        & $gh release view $TagName | Out-Null
    } catch {
        $releaseExists = $false
    }
    if ($releaseExists) {
        & $gh release upload $TagName $VersionedExe --clobber
        & $gh release edit $TagName --title $TagName --notes $releaseNotes
    } else {
        & $gh release create $TagName $VersionedExe --title $TagName --notes $releaseNotes
    }
}
