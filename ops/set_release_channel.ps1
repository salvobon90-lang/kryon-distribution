param(
    [Parameter(Mandatory = $true)]
    [string]$Version,

    [string]$Channel = "stable",
    [string]$RepoOwner = "salvobon90-lang",
    [string]$RepoName = "kryon-distribution",
    [string]$AdminApiKey = "",
    [string]$Notes = ""
)

$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$ZipPath = Join-Path $Root ("dist\kryon-v" + $Version + ".zip")

if (-not (Test-Path $ZipPath)) {
    throw "Zip release non trovato: $ZipPath"
}

if (-not $AdminApiKey) {
    throw "Passa -AdminApiKey con la chiave admin del worker."
}

$downloadUrl = "https://github.com/$RepoOwner/$RepoName/releases/download/v$Version/kryon-v$Version.zip"
$sha256 = (Get-FileHash -Path $ZipPath -Algorithm SHA256).Hash.ToLower()
$payload = @{
    channel = $Channel
    version = $Version
    download_url = $downloadUrl
    sha256 = $sha256
    notes = $Notes
} | ConvertTo-Json -Depth 4

$headers = @{
    "content-type" = "application/json"
    "x-admin-key" = $AdminApiKey
}

$response = Invoke-RestMethod -Method Post -Uri "https://kryon-licensing.kryonsubv2.workers.dev/api/admin/release/set" -Headers $headers -Body $payload
$latest = Invoke-RestMethod -Method Get -Uri ("https://kryon-licensing.kryonsubv2.workers.dev/api/releases/latest?channel=" + $Channel)

Write-Host "Canale aggiornato:" -ForegroundColor Green
$response | ConvertTo-Json -Depth 4
Write-Host ""
Write-Host "Manifest online corrente:" -ForegroundColor Cyan
$latest | ConvertTo-Json -Depth 4
