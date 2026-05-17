param(
    [string]$RepoName = "ai-test-downloader",
    [string]$Visibility = "public",
    [string]$BuildId = "",
    [string]$VersionedExe = "",
    [string]$TagName = "",
    [switch]$DryRun
)

$ErrorActionPreference = "Stop"
if ($null -ne (Get-Variable -Name PSNativeCommandUseErrorActionPreference -ErrorAction SilentlyContinue)) {
    $PSNativeCommandUseErrorActionPreference = $false
}

$gh = "C:\Program Files\GitHub CLI\gh.exe"
if (!(Test-Path -LiteralPath $gh)) {
    throw "GitHub CLI not found: $gh"
}

$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location -LiteralPath $repoRoot

function Invoke-CheckedCommand {
    param(
        [Parameter(Mandatory = $true)]
        [string]$FilePath,
        [string[]]$Arguments = @()
    )
    $previousErrorActionPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    try {
        & $FilePath @Arguments
    } finally {
        $ErrorActionPreference = $previousErrorActionPreference
    }
    $exitCode = $LASTEXITCODE
    if ($exitCode -ne 0) {
        $commandText = ($Arguments | ForEach-Object { if ($_ -match '\s') { '"{0}"' -f $_ } else { $_ } }) -join ' '
        throw "Command failed with exit code ${exitCode}: $FilePath $commandText"
    }
}

function Invoke-LoggedCommand {
    param(
        [Parameter(Mandatory = $true)]
        [string]$FilePath,
        [string[]]$Arguments = @()
    )
    $commandText = (($Arguments | ForEach-Object { if ($_ -match '\s') { '"{0}"' -f $_ } else { $_ } }) -join ' ').Trim()
    if ($DryRun) {
        Write-Host ("[DryRun] {0} {1}" -f $FilePath, $commandText).Trim()
        return
    }
    Invoke-CheckedCommand -FilePath $FilePath -Arguments $Arguments
}

function Get-OriginRepoSlug {
    $originUrl = git -c safe.directory=$repoRoot remote get-url origin 2>$null
    if ($LASTEXITCODE -ne 0 -or -not $originUrl) {
        return $null
    }
    $originUrl = "$originUrl".Trim()
    if ($originUrl -match 'github\.com[:/](.+?)(?:\.git)?$') {
        return $Matches[1]
    }
    return $null
}

function Test-GitHubReleaseExists {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ReleaseTag
    )
    $repoSlug = Get-OriginRepoSlug
    if (-not $repoSlug) {
        return $false
    }
    $apiUrl = "https://api.github.com/repos/$repoSlug/releases/tags/$ReleaseTag"
    try {
        Invoke-RestMethod -Uri $apiUrl -Headers @{ "User-Agent" = "ai-test-downloader-publisher" } | Out-Null
        return $true
    } catch {
        return $false
    }
}

function Wait-RemoteTagVisibility {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ReleaseTag,
        [int]$MaxAttempts = 10
    )
    for ($attempt = 1; $attempt -le $MaxAttempts; $attempt++) {
        $remoteRefs = git -c safe.directory=$repoRoot ls-remote --tags origin $ReleaseTag 2>$null
        if ($LASTEXITCODE -eq 0 -and $remoteRefs) {
            return
        }
        if ($attempt -lt $MaxAttempts) {
            Start-Sleep -Seconds 2
        }
    }
    throw "Remote tag did not become visible in time: $ReleaseTag"
}

function Ensure-GhRelease {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ReleaseTag,
        [Parameter(Mandatory = $true)]
        [string]$AssetPath,
        [Parameter(Mandatory = $true)]
        [string]$ReleaseNotes,
        [int]$MaxAttempts = 5
    )
    if (Test-GitHubReleaseExists -ReleaseTag $ReleaseTag) {
        Invoke-CheckedCommand -FilePath $gh -Arguments @("release", "upload", $ReleaseTag, $AssetPath, "--clobber")
        Invoke-CheckedCommand -FilePath $gh -Arguments @("release", "edit", $ReleaseTag, "--title", $ReleaseTag, "--notes", $ReleaseNotes)
        return
    }
    for ($attempt = 1; $attempt -le $MaxAttempts; $attempt++) {
        try {
            Invoke-CheckedCommand -FilePath $gh -Arguments @("release", "create", $ReleaseTag, $AssetPath, "--title", $ReleaseTag, "--notes", $ReleaseNotes)
            return
        } catch {
            if ($attempt -ge $MaxAttempts) {
                throw
            }
            Start-Sleep -Seconds 2
            if (Test-GitHubReleaseExists -ReleaseTag $ReleaseTag) {
                Invoke-CheckedCommand -FilePath $gh -Arguments @("release", "upload", $ReleaseTag, $AssetPath, "--clobber")
                Invoke-CheckedCommand -FilePath $gh -Arguments @("release", "edit", $ReleaseTag, "--title", $ReleaseTag, "--notes", $ReleaseNotes)
                return
            }
        }
    }
}

if (-not $TagName -and $BuildId -match '-(\d+)$') {
    $TagName = "v$($Matches[1])"
}
if (-not $VersionedExe -and $BuildId) {
    $VersionedExe = Join-Path $repoRoot ("dist\downloader_{0}.exe" -f ($BuildId -replace '-', '_'))
}

if ($BuildId) {
    if ($DryRun) {
        Write-Host "[DryRun] git -c safe.directory=$repoRoot add -u"
    } else {
        git -c safe.directory=$repoRoot add -u
    }
    $hasTrackedChanges = $false
    if ($DryRun) {
        $hasTrackedChanges = $true
    } else {
        git -c safe.directory=$repoRoot diff --cached --quiet
        $diffExitCode = $LASTEXITCODE
        if ($diffExitCode -eq 1) {
            $hasTrackedChanges = $true
        } elseif ($diffExitCode -ne 0) {
            throw "git diff --cached --quiet failed with exit code $diffExitCode"
        }
    }
    if ($hasTrackedChanges) {
        $commitLabel = if ($TagName) { $TagName } else { $BuildId }
        Invoke-LoggedCommand -FilePath "git" -Arguments @("-c", "safe.directory=$repoRoot", "commit", "-m", ("Release {0}" -f $commitLabel))
    }
}

$originExists = $false
git -c safe.directory=$repoRoot remote get-url origin | Out-Null
if ($LASTEXITCODE -eq 0) {
    $originExists = $true
} elseif ($LASTEXITCODE -ne 2 -and $LASTEXITCODE -ne 128) {
    throw "git remote get-url origin failed with exit code $LASTEXITCODE"
}

if (-not $originExists) {
    Invoke-LoggedCommand -FilePath $gh -Arguments @("repo", "create", $RepoName, "--$Visibility", "--source", ".", "--remote", "origin", "--push")
} else {
    Invoke-LoggedCommand -FilePath "git" -Arguments @("-c", "safe.directory=$repoRoot", "push", "-u", "origin", "HEAD")
}

if ($TagName) {
    Invoke-LoggedCommand -FilePath "git" -Arguments @("-c", "safe.directory=$repoRoot", "tag", "-f", $TagName)
    Invoke-LoggedCommand -FilePath "git" -Arguments @("-c", "safe.directory=$repoRoot", "push", "origin", ("refs/tags/{0}" -f $TagName), "--force")
    if ($DryRun) {
        Write-Host ("[DryRun] wait for remote tag visibility: {0}" -f $TagName)
    } else {
        Wait-RemoteTagVisibility -ReleaseTag $TagName
    }
}

if ($TagName -and $VersionedExe) {
    if (!(Test-Path -LiteralPath $VersionedExe)) {
        throw "Versioned executable not found: $VersionedExe"
    }
    $releaseNotes = if ($BuildId) { "Automated release for build $BuildId" } else { "Automated release for $TagName" }
    if ($DryRun) {
        if (Test-GitHubReleaseExists -ReleaseTag $TagName) {
            Write-Host ("[DryRun] gh release upload {0} {1} --clobber" -f $TagName, $VersionedExe)
            Write-Host ("[DryRun] gh release edit {0} --title {0} --notes ""{1}""" -f $TagName, $releaseNotes)
        } else {
            Write-Host ("[DryRun] gh release create {0} {1} --title {0} --notes ""{2}""" -f $TagName, $VersionedExe, $releaseNotes)
        }
    } else {
        Ensure-GhRelease -ReleaseTag $TagName -AssetPath $VersionedExe -ReleaseNotes $releaseNotes
    }
}
