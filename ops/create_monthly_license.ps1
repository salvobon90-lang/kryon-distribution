param(
    [Parameter(Mandatory = $true)]
    [string]$Email,
    [string]$LicenseKey = "",
    [int]$Days = 30,
    [int]$MaxDevices = 1,
    [string]$Plan = "PRO",
    [string]$ApiBaseUrl = "https://kryon-licensing.kryonsubv2.workers.dev",
    [string]$AdminApiKey = ""
)

$ErrorActionPreference = "Stop"

if (-not $AdminApiKey) {
    $AdminApiKey = Read-Host "Inserisci ADMIN_API_KEY"
}

if (-not $LicenseKey) {
    $prefix = (($Email.Split("@")[0]) -replace "[^A-Za-z0-9]", "").ToUpper()
    if ($prefix.Length -gt 6) {
        $prefix = $prefix.Substring(0, 6)
    }
    if (-not $prefix) {
        $prefix = "CLIENT"
    }
    $chars = (48..57 + 65..90) | ForEach-Object { [char]$_ }
    $suffix = -join (1..4 | ForEach-Object { $chars | Get-Random })
    $LicenseKey = "KRYON-$prefix-$((Get-Date).ToString('yyMM'))-$suffix"
}

$body = @{
    email = $Email
    license_key = $LicenseKey
    plan = $Plan
    days = $Days
    max_devices = $MaxDevices
    update_channel = "stable"
} | ConvertTo-Json -Compress

$response = Invoke-RestMethod `
    -Method Post `
    -Uri "$ApiBaseUrl/api/admin/license/create" `
    -Headers @{ "x-admin-key" = $AdminApiKey } `
    -ContentType "application/json" `
    -Body $body

Write-Host ""
Write-Host "Licenza creata con successo" -ForegroundColor Green
Write-Host "Email      : $Email"
Write-Host "Chiave     : $LicenseKey"
Write-Host "Durata     : $Days giorni"
Write-Host "Max device : $MaxDevices"
Write-Host ""
$response | ConvertTo-Json -Depth 6
