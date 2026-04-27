param(
    [string]$RepoName = "ai-tets-downloader",
    [string]$Visibility = "public"
)

$ErrorActionPreference = "Stop"

$gh = "C:\Program Files\GitHub CLI\gh.exe"
if (!(Test-Path -LiteralPath $gh)) {
    throw "GitHub CLI not found: $gh"
}

$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location -LiteralPath $repoRoot

& $gh auth status | Out-Null

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

