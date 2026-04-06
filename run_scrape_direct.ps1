$ErrorActionPreference = "Stop"
Set-Location "E:\searchforemlak"

$env:PYTHONPATH = "E:\searchforemlak\.vendor;E:\searchforemlak"

if (-not $env:SCRAPE_LAT) { $env:SCRAPE_LAT = "36.856323802189124" }
if (-not $env:SCRAPE_LNG) { $env:SCRAPE_LNG = "30.746730472958518" }
if (-not $env:SCRAPE_LISTING_TYPE) { $env:SCRAPE_LISTING_TYPE = "all" }
if (-not $env:SCRAPE_PROPERTY_TYPE) { $env:SCRAPE_PROPERTY_TYPE = "all" }
if (-not $env:SCRAPE_NUM_PAGES) { $env:SCRAPE_NUM_PAGES = "1" }

py -3 .\run_scrape_direct.py
