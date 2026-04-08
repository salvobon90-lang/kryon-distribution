param(
    [string]$Version = "",
    [string]$Channel = "stable",
    [string]$RepoOwner = "",
    [string]$RepoName = "kryon-distribution"
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
    throw "Impossibile determinare la versione. Passa -Version oppure verifica kryon.pyw."
}

$ReleaseDir = Join-Path $DistRoot ("kryon-" + $Version)
$AssetsDir = Join-Path $ReleaseDir "immagini kryon"
$ZipPath = Join-Path $DistRoot ("kryon-v" + $Version + ".zip")

New-Item -ItemType Directory -Force -Path $ReleaseDir | Out-Null
New-Item -ItemType Directory -Force -Path $AssetsDir | Out-Null

$files = @(
    "bot_core.py",
    "kryon.pyw",
    "kryon_runtime.py",
    "kryon_license.py",
    "kryon_update.py",
    "layout.json",
    "release_manifest.example.json",
    "SELLING_SETUP.md"
)

foreach ($file in $files) {
    $src = Join-Path $Root $file
    if (Test-Path $src) {
        Copy-Item $src -Destination (Join-Path $ReleaseDir $file) -Force
    }
}

$imgRoot = Join-Path $Root "immagini kryon"
if (Test-Path $imgRoot) {
    Copy-Item (Join-Path $imgRoot "*") -Destination $AssetsDir -Recurse -Force
}

if (Test-Path $ZipPath) {
    Remove-Item $ZipPath -Force
}

Compress-Archive -Path (Join-Path $ReleaseDir "*") -DestinationPath $ZipPath -Force

$sha256 = (Get-FileHash -Path $ZipPath -Algorithm SHA256).Hash.ToLower()
$downloadUrl = "https://your-domain.example/releases/kryon-v$Version.zip"
if ($RepoOwner -and $RepoName) {
    $downloadUrl = "https://github.com/$RepoOwner/$RepoName/releases/download/v$Version/kryon-v$Version.zip"
}

$manifest = @{
    channel = $Channel
    version = $Version
    download_url = $downloadUrl
    sha256 = $sha256
    built_at = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
}

$manifest | ConvertTo-Json -Depth 4 | Set-Content -Path (Join-Path $ReleaseDir "latest.json") -Encoding UTF8

Write-Host "Release pronta in $ReleaseDir"
Write-Host "Zip: $ZipPath"
Write-Host "SHA256: $sha256"
