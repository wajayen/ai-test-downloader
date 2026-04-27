$ErrorActionPreference = 'Stop'

$projectRoot = 'C:\antigravity\ai_test'
$pythonExe = Join-Path $projectRoot '.venv\Scripts\python.exe'
$pyinstallerExe = Join-Path $projectRoot '.venv\Scripts\pyinstaller.exe'
$specFile = Join-Path $projectRoot 'downloader.spec'
$distDir = Join-Path $projectRoot 'dist'
$buildDir = Join-Path $projectRoot 'build'

& $pythonExe -m py_compile (Join-Path $projectRoot 'downloader.py') (Join-Path $projectRoot 'downloader_safe.py')
& $pyinstallerExe --noconfirm --distpath $distDir --workpath $buildDir $specFile
