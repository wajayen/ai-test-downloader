$ErrorActionPreference = 'Stop'

$projectRoot = 'C:\antigravity\ai_test'
$pythonExe = Join-Path $projectRoot '.venv\Scripts\python.exe'
$specFile = Join-Path $projectRoot 'downloader.spec'
$distDir = Join-Path $projectRoot 'dist'
$buildDir = Join-Path $projectRoot 'build'
$sourceFile = Join-Path $projectRoot 'downloader.py'
$safeSourceFile = Join-Path $projectRoot 'downloader_safe.py'
$buildInfo = Get-Content -LiteralPath $sourceFile | Select-String 'APP_BUILD = "([^"]+)"' | Select-Object -First 1
if (-not $buildInfo) {
    throw 'APP_BUILD not found in downloader.py'
}
$buildId = $buildInfo.Matches[0].Groups[1].Value
$versionedExe = Join-Path $distDir ("downloader_{0}.exe" -f ($buildId -replace '-', '_'))

& $pythonExe -m py_compile $sourceFile $safeSourceFile
& $pythonExe -m PyInstaller --clean --noconfirm --distpath $distDir --workpath $buildDir $specFile
Copy-Item -LiteralPath (Join-Path $distDir 'downloader.exe') -Destination $versionedExe -Force
