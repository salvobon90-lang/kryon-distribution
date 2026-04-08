param(
    [string]$Version = "",
    [string]$Channel = "stable",
    [string]$PackageLabel = "first-client"
)

$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
$DistRoot = Join-Path $Root "dist"
$VersionSource = Join-Path $Root "kryon.pyw"

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
$AssetsDir = Join-Path $PackageDir "immagini kryon"
$RuntimeDir = Join-Path $PackageDir ".kryon_runtime"
$ZipPath = Join-Path $DistRoot ("$PackageName.zip")

if (Test-Path $PackageDir) {
    Remove-Item $PackageDir -Recurse -Force
}
if (Test-Path $ZipPath) {
    Remove-Item $ZipPath -Force
}

New-Item -ItemType Directory -Force -Path $PackageDir | Out-Null
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
        Copy-Item $src -Destination (Join-Path $PackageDir $file) -Force
    }
}

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
}

$licenseConfig | ConvertTo-Json -Depth 4 | Set-Content -Path (Join-Path $RuntimeDir "license_config.json") -Encoding UTF8
$updateConfig | ConvertTo-Json -Depth 4 | Set-Content -Path (Join-Path $RuntimeDir "update_config.json") -Encoding UTF8

$installScript = @'
$ErrorActionPreference = "Stop"
$BaseDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $BaseDir

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
& $venvPython -m pip install -r (Join-Path $BaseDir "requirements-client.txt")

Write-Host ""
Write-Host "Installazione completata." -ForegroundColor Green
Write-Host "Avvia ora il bot con: start_kryon.bat" -ForegroundColor Cyan
'@

$startBat = @'
@echo off
cd /d "%~dp0"
if exist ".venv\Scripts\pythonw.exe" (
  start "" ".venv\Scripts\pythonw.exe" "kryon.pyw"
) else (
  echo Esegui prima install_client.ps1
  pause
)
'@

$startConsoleBat = @'
@echo off
cd /d "%~dp0"
if exist ".venv\Scripts\python.exe" (
  ".venv\Scripts\python.exe" "kryon.pyw"
) else (
  echo Esegui prima install_client.ps1
  pause
)
'@

$readme = @"
# KRYON Cliente - Versione $Version

## Cosa contiene
- Bot KRYON gia' configurato per licensing online e auto-update
- Runtime cliente separato
- Launcher rapido

## Installazione cliente
1. Installare MetaTrader 5 sul PC cliente e fare login al broker.
2. Estrarre tutta questa cartella in una posizione stabile, per esempio `C:\KRYON`.
3. Fare clic destro su `install_client.ps1` e avviarlo con PowerShell.
4. A fine installazione aprire `start_kryon.bat`.
5. Nel popup licenza inserire `email cliente + chiave cliente`.

## File utili
- `install_client.ps1`: crea ambiente Python e installa dipendenze
- `start_kryon.bat`: avvio normale
- `start_kryon_console.bat`: avvio con console debug

## Nota
Questa build forza il controllo licenza anche se gira da sorgente, quindi il cliente non parte in `DEV MODE`.
"@

Set-Content -Path (Join-Path $PackageDir "install_client.ps1") -Value $installScript -Encoding UTF8
Set-Content -Path (Join-Path $PackageDir "start_kryon.bat") -Value $startBat -Encoding ASCII
Set-Content -Path (Join-Path $PackageDir "start_kryon_console.bat") -Value $startConsoleBat -Encoding ASCII
Set-Content -Path (Join-Path $PackageDir "README_CLIENTE.md") -Value $readme -Encoding UTF8

Compress-Archive -Path (Join-Path $PackageDir "*") -DestinationPath $ZipPath -Force

Write-Host "Pacchetto cliente pronto in $PackageDir"
Write-Host "Zip cliente: $ZipPath"
