$ErrorActionPreference = "Stop"

Set-Location "E:\searchforemlak"

$lat = "36.856323802189124"
$lng = "30.746730472958518"
$port = 8010
$stdoutLog = "E:\searchforemlak\_api_stdout.log"
$stderrLog = "E:\searchforemlak\_api_stderr.log"

if (Test-Path $stdoutLog) { Remove-Item $stdoutLog -Force }
if (Test-Path $stderrLog) { Remove-Item $stderrLog -Force }

$env:PYTHONPATH = "E:\searchforemlak\.vendor;E:\searchforemlak"
$env:HOST = "127.0.0.1"
$env:PORT = "$port"

$apiProc = Start-Process -FilePath "py" -ArgumentList "-3", ".\main.py" -WorkingDirectory "E:\searchforemlak" -RedirectStandardOutput $stdoutLog -RedirectStandardError $stderrLog -PassThru

try {
    $ready = $false
    for ($i = 0; $i -lt 60; $i++) {
        Start-Sleep -Seconds 2
        try {
            $null = Invoke-RestMethod -Method Get -Uri "http://127.0.0.1:$port/openapi.json" -TimeoutSec 5
            $ready = $true
            break
        } catch {
        }
    }

    if (-not $ready) {
        Write-Host "--- API STDOUT ---"
        if (Test-Path $stdoutLog) { Get-Content $stdoutLog -TotalCount 200 }
        Write-Host "--- API STDERR ---"
        if (Test-Path $stderrLog) { Get-Content $stderrLog -TotalCount 200 }
        throw "API baslatilamadi (port $port)"
    }

    $body = @{
        lat = $lat
        lng = $lng
        listing_type = "all"
        property_type = "all"
        num_pages = 1
    } | ConvertTo-Json -Compress

    $resp = Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:$port/api/scrape/run" -ContentType "application/json" -Body $body -TimeoutSec 420

    $summary = [ordered]@{
        status = $resp.status
        progress = $resp.progress
        total_urls_found = $resp.total_urls_found
        total_urls_scraped = $resp.total_urls_scraped
        total_errors = $resp.total_errors
        listings_count = $resp.listings_count
        search_strategy = $resp.search_context.search_strategy
        balance_strategy = $resp.search_context.balance_strategy
        first_listings = @($resp.listings | Select-Object -First 5)
    }

    $summary | ConvertTo-Json -Depth 7
}
finally {
    if ($apiProc -and (Get-Process -Id $apiProc.Id -ErrorAction SilentlyContinue)) {
        Stop-Process -Id $apiProc.Id -Force -ErrorAction SilentlyContinue
    }
}
