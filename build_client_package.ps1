param(
    [string]$Version = "",
    [string]$Channel = "stable",
    [string]$PackageLabel = "first-client"
)

$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
$DistRoot = Join-Path $Root "dist"
$VersionSource = Join-Path $Root "kryon.pyw"

function Set-AppVersionInFile {
    param(
        [string]$Path,
        [string]$Version
    )
    if (-not (Test-Path $Path)) {
        return
    }
    $content = Get-Content $Path -Raw
    $content = [regex]::Replace($content, 'KRYON ULTIMATE PRO V [0-9.]+', "KRYON ULTIMATE PRO V $Version")
    [System.IO.File]::WriteAllText($Path, $content, (New-Object System.Text.UTF8Encoding($false)))
}

if (-not $Version) {
    if (Test-Path $VersionSource) {
        $versionLine = Select-String -Path $VersionSource -Pattern 'VERSION\s*=\s*"KRYON ULTIMATE PRO V ([0-9.]+)"' | Select-Object -First 1
        if ($versionLine -and $versionLine.Matches.Count -gt 0) {
            $Version = $versionLine.Matches[0].Groups[1].Value
        }
    }
}

if (-not $Version) {
    throw "Impossibile determinare la versione dal file kryon.pyw."
}

$PackageName = "kryon-client-$PackageLabel-$Version"
$PackageDir = Join-Path $DistRoot $PackageName
$AppDir = Join-Path $PackageDir "app"
$AssetsDir = Join-Path $AppDir "immagini kryon"
$RuntimeDir = Join-Path $AppDir ".kryon_runtime"
$ZipPath = Join-Path $DistRoot ("$PackageName.zip")

if (Test-Path $PackageDir) {
    Remove-Item $PackageDir -Recurse -Force
}
if (Test-Path $ZipPath) {
    Remove-Item $ZipPath -Force
}

New-Item -ItemType Directory -Force -Path $PackageDir | Out-Null
New-Item -ItemType Directory -Force -Path $AppDir | Out-Null
New-Item -ItemType Directory -Force -Path $AssetsDir | Out-Null
New-Item -ItemType Directory -Force -Path $RuntimeDir | Out-Null

$files = @(
    "bot_core.py",
    "kryon.pyw",
    "kryon_runtime.py",
    "kryon_license.py",
    "kryon_update.py",
    "layout.json",
    "requirements-client.txt"
)

foreach ($file in $files) {
    $src = Join-Path $Root $file
    if (Test-Path $src) {
        Copy-Item $src -Destination (Join-Path $AppDir $file) -Force
    }
}

Set-AppVersionInFile -Path (Join-Path $AppDir "kryon.pyw") -Version $Version
Set-AppVersionInFile -Path (Join-Path $AppDir "bot_core.py") -Version $Version

$imgRoot = Join-Path $Root "immagini kryon"
if (Test-Path $imgRoot) {
    Copy-Item (Join-Path $imgRoot "*") -Destination $AssetsDir -Recurse -Force
}

$licenseConfig = @{
    api_base_url = "https://kryon-licensing.kryonsubv2.workers.dev"
    activation_endpoint = "/api/license/activate"
    refresh_endpoint = "/api/license/refresh"
    grace_days = 5
    force_packaged_mode = $true
    enforce_packaged_only = $true
}

$updateConfig = @{
    manifest_url = "https://kryon-licensing.kryonsubv2.workers.dev/api/releases/latest"
    channel = $Channel
    auto_check = $true
    check_interval_hours = 6
    require_latest_for_run = $true
    strict_manifest_required = $true
}

$utf8NoBom = New-Object System.Text.UTF8Encoding($false)
[System.IO.File]::WriteAllText((Join-Path $RuntimeDir "license_config.json"), ($licenseConfig | ConvertTo-Json -Depth 4), $utf8NoBom)
[System.IO.File]::WriteAllText((Join-Path $RuntimeDir "update_config.json"), ($updateConfig | ConvertTo-Json -Depth 4), $utf8NoBom)

$installScript = @'
$ErrorActionPreference = "Stop"
$BaseDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $BaseDir
$AppDir = Join-Path $BaseDir "app"

function Resolve-PythonCommand {
    if (Get-Command py -ErrorAction SilentlyContinue) {
        return "py -3.14"
    }
    if (Get-Command python -ErrorAction SilentlyContinue) {
        return "python"
    }
    throw "Python non trovato. Installa Python 3.14 64-bit e rilancia questo script."
}

$pythonCmd = Resolve-PythonCommand
if (-not (Test-Path ".venv")) {
    Invoke-Expression "$pythonCmd -m venv .venv"
}

$venvPython = Join-Path $BaseDir ".venv\Scripts\python.exe"
& $venvPython -m pip install --upgrade pip
& $venvPython -m pip install -r (Join-Path $AppDir "requirements-client.txt")

Write-Host ""
Write-Host "Installazione completata." -ForegroundColor Green
Write-Host "Avvia ora il bot con: AVVIA_KRYON.bat" -ForegroundColor Cyan
'@

$startBat = @'
@echo off
cd /d "%~dp0"
if exist ".venv\Scripts\pythonw.exe" (
  start "" ".venv\Scripts\pythonw.exe" "app\kryon.pyw"
) else (
  echo Esegui prima INSTALLA_KRYON.bat
  pause
)
'@

$startConsoleBat = @'
@echo off
cd /d "%~dp0"
if exist ".venv\Scripts\python.exe" (
  ".venv\Scripts\python.exe" "app\kryon.pyw"
) else (
  echo Esegui prima INSTALLA_KRYON.bat
  pause
)
'@

$installBat = @'
@echo off
cd /d "%~dp0"
powershell -ExecutionPolicy Bypass -File "%~dp0install_client.ps1"
if errorlevel 1 (
  echo.
  echo Installazione non completata.
  pause
)
'@

$readme = @"
# KRYON Cliente - Versione $Version

## Cosa contiene
- Bot KRYON gia' configurato per licensing online e auto-update
- Applicazione concentrata nella cartella `app`
- Launcher semplici visibili in radice

## Installazione cliente
1. Installare MetaTrader 5 sul PC cliente e fare login al broker.
2. Estrarre tutta questa cartella in una posizione stabile, per esempio `C:\KRYON`.
3. Fare doppio clic su `INSTALLA_KRYON.bat`.
4. A fine installazione aprire `AVVIA_KRYON.bat`.
5. Nel popup licenza inserire `email cliente + chiave cliente`.
6. Se la licenza non e' attiva oppure se esiste una versione piu' nuova obbligatoria, il motore non partira'.

## File utili
- `INSTALLA_KRYON.bat`: installer semplice con doppio clic
- `AVVIA_KRYON.bat`: avvio normale
- `AVVIA_KRYON_CONSOLE.bat`: avvio con console debug
- `app\`: contiene il motore del bot e il runtime tecnico

## Nota
Questa build forza il controllo licenza anche se gira da sorgente, quindi il cliente non parte in `DEV MODE`.
"@

[System.IO.File]::WriteAllText((Join-Path $PackageDir "install_client.ps1"), $installScript, $utf8NoBom)
Set-Content -Path (Join-Path $PackageDir "INSTALLA_KRYON.bat") -Value $installBat -Encoding ASCII
Set-Content -Path (Join-Path $PackageDir "AVVIA_KRYON.bat") -Value $startBat -Encoding ASCII
Set-Content -Path (Join-Path $PackageDir "AVVIA_KRYON_CONSOLE.bat") -Value $startConsoleBat -Encoding ASCII
[System.IO.File]::WriteAllText((Join-Path $PackageDir "README_CLIENTE.md"), $readme, $utf8NoBom)

Compress-Archive -Path (Join-Path $PackageDir "*") -DestinationPath $ZipPath -Force

Write-Host "Pacchetto cliente pronto in $PackageDir"
Write-Host "Zip cliente: $ZipPath"
