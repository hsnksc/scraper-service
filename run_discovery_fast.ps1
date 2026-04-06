param(
    [string]$Lat = "36.856323802189124",
    [string]$Lng = "30.746730472958518",
    [int]$MaxPerSource = 8,
    [int]$QueryLimit = 4,
    [bool]$StrictDetailOnly = $true,
    [bool]$StrictDistrictHeader = $true
)

$ErrorActionPreference = "Stop"

Set-Location "E:\searchforemlak"

function Read-DotEnv {
    param([string]$Path)
    $map = @{}
    if (-not (Test-Path $Path)) { return $map }
    Get-Content $Path | ForEach-Object {
        $line = $_.Trim()
        if (-not $line) { return }
        if ($line.StartsWith("#")) { return }
        $idx = $line.IndexOf("=")
        if ($idx -lt 1) { return }
        $key = $line.Substring(0, $idx).Trim()
        $val = $line.Substring($idx + 1).Trim()
        $map[$key] = $val
    }
    return $map
}

function Normalize-Text {
    param([string]$Text)
    if (-not $Text) { return "" }
    $x = $Text.ToLowerInvariant()
    $x = $x.Replace("ç", "c").Replace("ğ", "g").Replace("ı", "i").Replace("ö", "o").Replace("ş", "s").Replace("ü", "u")
    return $x
}

function Get-ListingTypeHint {
    param([string]$Text)
    $t = Normalize-Text $Text
    if ($t -match "kiralik|kira") { return "rent" }
    if ($t -match "satilik|sale") { return "sale" }
    return $null
}

function Get-PropertyTypeHint {
    param([string]$Text)
    $t = Normalize-Text $Text
    if ($t -match "isyeri|ticari|ofis|dukkan|buro|magaza|depo|plaza") { return "commercial" }
    if ($t -match "arsa|tarla|zeytinlik|bag|bahce") { return "land" }
    if ($t -match "daire|konut|villa|rezidans|ev|apartman|mustakil") { return "residential" }
    return $null
}

function Convert-NumberToken {
    param([string]$Token)
    if (-not $Token) { return $null }
    $x = $Token.Trim().Replace(" ", "")
    if (-not $x) { return $null }

    if ($x.Contains(",") -and $x.Contains(".")) {
        if ($x.LastIndexOf(",") -gt $x.LastIndexOf(".")) {
            $x = $x.Replace(".", "").Replace(",", ".")
        } else {
            $x = $x.Replace(",", "")
        }
    } elseif (($x.Split(".").Count -gt 2) -and -not $x.Contains(",")) {
        $parts = $x.Split(".")
        $valid = ($parts[0].Length -le 3)
        foreach ($p in $parts[1..($parts.Count - 1)]) {
            if ($p.Length -ne 3) { $valid = $false; break }
        }
        if ($valid) { $x = ($parts -join "") } else { return $null }
    } elseif (($x.Split(",").Count -gt 2) -and -not $x.Contains(".")) {
        $parts = $x.Split(",")
        $valid = ($parts[0].Length -le 3)
        foreach ($p in $parts[1..($parts.Count - 1)]) {
            if ($p.Length -ne 3) { $valid = $false; break }
        }
        if ($valid) { $x = ($parts -join "") } else { return $null }
    } elseif ($x.Contains(".")) {
        $parts = $x.Split(".")
        if ($parts.Count -eq 2) {
            if ($parts[1].Length -eq 3 -and $parts[0].Length -ge 1) {
                $x = "$($parts[0])$($parts[1])"
            }
        }
    } elseif ($x.Contains(",")) {
        $parts = $x.Split(",")
        if ($parts.Count -eq 2) {
            if ($parts[1].Length -eq 3 -and $parts[0].Length -ge 1) {
                $x = "$($parts[0])$($parts[1])"
            } else {
                $x = "$($parts[0]).$($parts[1])"
            }
        }
    }
    try {
        $v = [double]$x
        if ($v -gt 0) { return $v }
        return $null
    } catch {
        return $null
    }
}

function Get-PriceHint {
    param([string]$Text)
    if (-not $Text) { return $null }
    $matches = [regex]::Matches(
        $Text,
        "(?<!\d)(\d{1,3}(?:[\.\s]\d{3})+|\d{4,11}|\d+(?:[\.,]\d{1,2})?)\s*(TL|₺)\b",
        [System.Text.RegularExpressions.RegexOptions]::IgnoreCase
    )
    foreach ($m in $matches) {
        $v = Convert-NumberToken $m.Groups[1].Value
        if ($v -and $v -ge 1000) { return $v }
    }
    $m2 = [regex]::Match((Normalize-Text $Text), "(\d+(?:[\,\.]\d+)?)\s*milyon")
    if ($m2.Success) {
        $base = Convert-NumberToken $m2.Groups[1].Value
        if ($base) {
            $v2 = $base * 1000000
            if ($v2 -gt 0) { return $v2 }
        }
    }
    return $null
}

function Get-AreaHint {
    param([string]$Text)
    if (-not $Text) { return $null }
    $m = [regex]::Match($Text, "(?<!\d)(\d{2,4}(?:[\.,]\d{1,2})?)\s*(m²|m2|metrekare)\b", [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)
    if (-not $m.Success) { return $null }
    $v = Convert-NumberToken $m.Groups[1].Value
    if ($v -and $v -ge 10 -and $v -le 5000) { return $v }
    return $null
}

function Build-Candidate {
    param(
        [string]$Url,
        [string]$Source,
        [string]$Title,
        [string]$Snippet
    )
    $mix = "$Url $Title $Snippet"
    $priceTitle = Get-PriceHint $Title
    $priceSnippet = Get-PriceHint $Snippet
    $areaTitle = Get-AreaHint $Title
    $areaSnippet = Get-AreaHint $Snippet
    [PSCustomObject]@{
        url = $Url
        sources = @($Source)
        title = if ($Title) { $Title } else { $null }
        snippet = if ($Snippet) { $Snippet } else { $null }
        listing_type_hint = Get-ListingTypeHint $mix
        property_type_hint = Get-PropertyTypeHint $mix
        price_hint = if ($priceTitle) { $priceTitle } else { $priceSnippet }
        area_hint = if ($areaTitle) { $areaTitle } else { $areaSnippet }
    }
}

function Add-OrMergeCandidate {
    param(
        [hashtable]$Map,
        [pscustomobject]$Candidate
    )
    if (-not $Candidate.url) { return }
    if (-not $Map.ContainsKey($Candidate.url)) {
        $Map[$Candidate.url] = $Candidate
        return
    }
    $cur = $Map[$Candidate.url]
    $allSources = @($cur.sources + $Candidate.sources | Select-Object -Unique)
    $cur.sources = $allSources
    if (-not $cur.title -and $Candidate.title) { $cur.title = $Candidate.title }
    if (-not $cur.snippet -and $Candidate.snippet) { $cur.snippet = $Candidate.snippet }
    if (-not $cur.listing_type_hint -and $Candidate.listing_type_hint) { $cur.listing_type_hint = $Candidate.listing_type_hint }
    if (-not $cur.property_type_hint -and $Candidate.property_type_hint) { $cur.property_type_hint = $Candidate.property_type_hint }
    if (-not $cur.price_hint -and $Candidate.price_hint) { $cur.price_hint = $Candidate.price_hint }
    if (-not $cur.area_hint -and $Candidate.area_hint) { $cur.area_hint = $Candidate.area_hint }
}

function Get-LocalityScore {
    param(
        [pscustomobject]$Candidate,
        [pscustomobject]$Geo
    )
    $header = Normalize-Text "$($Candidate.url) $($Candidate.title)"
    $mix = Normalize-Text "$($Candidate.url) $($Candidate.title) $($Candidate.snippet)"
    $district = Normalize-Text "$($Geo.district)"
    $city = Normalize-Text "$($Geo.city)"
    $full = Normalize-Text "$($Geo.full)"
    $roadNorm = Normalize-Text "$($Geo.road)"
    $stop = @("mahallesi", "mahalle", "mah", "caddesi", "cadde", "cd", "sokak", "sk", "bulvari", "bulvar", "blv", "sitesi")
    $districtTokens = @($district -split "[^a-z0-9]+" | Where-Object { $_ -and $_.Length -ge 3 -and $_ -notin $stop })
    $roadTokens = @($roadNorm -split "[^a-z0-9]+" | Where-Object { $_ -and $_.Length -ge 3 -and $_ -notin $stop })
    $allDistricts = @("muratpasa", "kepez", "konyaalti", "dosemealti", "aksu", "serik", "manavgat", "alanya", "kas", "kumluca", "demre", "finike", "gazipasa", "gundogmus", "ibradi", "korkuteli", "elmali", "kemer")

    $adminDistrict = ""
    foreach ($d in $allDistricts) {
        if ($full.Contains($d)) {
            $adminDistrict = $d
            break
        }
    }

    $score = 0
    $districtHitHeader = $false
    $districtHitAny = $false
    $strongDistrictHeader = $false
    $primaryDistrict = if ($districtTokens.Count -gt 0) { $districtTokens[0] } else { "" }

    if ($primaryDistrict) {
        if ($header -match ("\b" + [regex]::Escape($primaryDistrict) + "\b[\s\-_\/]*mah")) {
            $strongDistrictHeader = $true
        }
        if (-not $strongDistrictHeader -and $adminDistrict -and $header.Contains($adminDistrict) -and $header.Contains($primaryDistrict)) {
            $strongDistrictHeader = $true
        }
    }
    if ($districtTokens.Count -gt 0) {
        foreach ($tok in $districtTokens) {
            if ($strongDistrictHeader) { $districtHitHeader = $true }
            if ($mix.Contains($tok)) { $districtHitAny = $true }
        }
    } elseif ($district) {
        $districtHitHeader = $header.Contains($district) -and ($header -match ("\b" + [regex]::Escape($district) + "\b[\s\-_\/]*mah"))
        $districtHitAny = $mix.Contains($district)
    }

    if ($StrictDistrictHeader -and $districtTokens.Count -gt 0 -and -not $districtHitHeader) {
        return -99
    }

    if ($districtHitHeader) { $score += 6 }
    elseif ($districtHitAny) { $score += 3 }
    if ($city -and $header.Contains($city)) { $score += 1 }

    $roadHits = 0
    foreach ($tok in $roadTokens) {
        if ($header.Contains($tok)) { $roadHits += 1 }
    }
    $score += [Math]::Min($roadHits, 2)
    if ($roadHits -eq 0) {
        foreach ($tok in $roadTokens) {
            if ($mix.Contains($tok)) {
                $score += 1
                break
            }
        }
    }

    if ("$($Candidate.url)" -match "/ilan/") { $score += 1 }

    if ($city -eq "antalya") {
        $headerDistrictHits = @()
        foreach ($d in $allDistricts) {
            if ($header.Contains($d)) { $headerDistrictHits += $d }
        }
        if ($adminDistrict) {
            if ($headerDistrictHits.Count -gt 0 -and ($headerDistrictHits -notcontains $adminDistrict)) {
                return -99
            }
        } elseif ($district -and -not $districtHitHeader -and $headerDistrictHits.Count -gt 0) {
            return -99
        }
    }
    return $score
}

function Is-DetailUrl {
    param([string]$Url)
    if (-not $Url) { return $false }
    $u = "$Url".ToLowerInvariant()
    if ($u.Contains("/ilan/")) { return $true }
    if ($u.Contains("/detay")) { return $true }
    if ($u.Contains("/listing/")) { return $true }
    if ($u.Contains("/site-")) { return $true }
    return $false
}

$envMap = Read-DotEnv "E:\searchforemlak\.env"
$tavilyKey = $envMap["TAVILY_API_KEY"]
$serperKey = $envMap["SERPER_API_KEY"]
$exaKey = $envMap["EXA_API_KEY"]

if (-not $tavilyKey -and -not $serperKey -and -not $exaKey) {
    throw "TAVILY_API_KEY, SERPER_API_KEY veya EXA_API_KEY degerlerinden en az biri gerekli."
}

$geo = $null
try {
    $url = "https://nominatim.openstreetmap.org/reverse?format=jsonv2&lat=$Lat&lon=$Lng&accept-language=tr"
    $geoResp = Invoke-RestMethod -Method Get -Uri $url -Headers @{ "User-Agent" = "searchforemlak-fast-discovery/1.0" } -TimeoutSec 20
    $addr = $geoResp.address
    $district = if ($addr.suburb) { $addr.suburb } elseif ($addr.city_district) { $addr.city_district } else { "" }
    $city = if ($addr.city) { $addr.city } elseif ($addr.town) { $addr.town } elseif ($addr.province) { $addr.province } else { "" }
    $road = if ($addr.road) { $addr.road } else { "" }
    $geo = [PSCustomObject]@{
        district = $district
        city = $city
        road = $road
        full = $geoResp.display_name
    }
} catch {
    $geo = [PSCustomObject]@{
        district = ""
        city = ""
        road = ""
        full = ""
    }
}

$baseLoc = @($geo.road, $geo.district, $geo.city) -ne ""
$locText = ($baseLoc -join " ").Trim()
if (-not $locText) { $locText = "$Lat,$Lng Antalya" }

$queries = @(
    "$locText satilik daire",
    "$locText kiralik daire",
    "$locText satilik isyeri",
    "$locText kiralik isyeri"
) | Select-Object -Unique

$includeDomains = @(
    "emlakjet.com",
    "hepsiemlak.com",
    "sahibinden.com",
    "hurriyetemlak.com",
    "emlakcarsi.com",
    "emlakmarketiantalya.com",
    "rentola.com.tr",
    "tr.flatspotter.com",
    "flatspotter.com",
    "emlakgo.net",
    "remax.com.tr"
)

$effectiveQueryLimit = [Math]::Max(1, [Math]::Min($queries.Count, $QueryLimit))
$querySlice = @($queries | Select-Object -First $effectiveQueryLimit)

$candidates = @{}

if ($tavilyKey) {
    foreach ($q in $querySlice) {
        try {
            $payload = @{
                query = $q
                topic = "general"
                search_depth = "basic"
                max_results = $MaxPerSource
                include_answer = $false
                include_raw_content = $false
                include_domains = $includeDomains
                country = "turkey"
            } | ConvertTo-Json -Depth 6

            $resp = Invoke-RestMethod -Method Post -Uri "https://api.tavily.com/search" -Headers @{
                "Authorization" = "Bearer $tavilyKey"
                "Content-Type" = "application/json"
            } -Body $payload -TimeoutSec 25

            foreach ($row in @($resp.results)) {
                $url = "$($row.url)"
                if (-not $url) { continue }
                $cand = Build-Candidate -Url $url -Source "tavily" -Title "$($row.title)" -Snippet "$($row.content)"
                Add-OrMergeCandidate -Map $candidates -Candidate $cand
            }
        } catch {
        }
    }
}

if ($serperKey) {
    foreach ($q in $querySlice) {
        try {
            $payload = @{
                q = $q
                gl = "tr"
                hl = "tr"
                num = $MaxPerSource
            } | ConvertTo-Json

            $resp = Invoke-RestMethod -Method Post -Uri "https://google.serper.dev/search" -Headers @{
                "X-API-KEY" = $serperKey
                "Content-Type" = "application/json"
            } -Body $payload -TimeoutSec 25

            foreach ($row in @($resp.organic)) {
                $url = "$($row.link)"
                if (-not $url) { continue }
                $cand = Build-Candidate -Url $url -Source "serper" -Title "$($row.title)" -Snippet "$($row.snippet)"
                Add-OrMergeCandidate -Map $candidates -Candidate $cand
            }
        } catch {
        }
    }
}

if ($exaKey) {
    foreach ($q in $querySlice) {
        try {
            $payload = @{
                query = $q
                type = "auto"
                numResults = $MaxPerSource
                includeDomains = $includeDomains
                contents = @{
                    highlights = @{
                        maxCharacters = 1200
                    }
                }
            } | ConvertTo-Json -Depth 8

            $resp = Invoke-RestMethod -Method Post -Uri "https://api.exa.ai/search" -Headers @{
                "x-api-key" = $exaKey
                "Content-Type" = "application/json"
            } -Body $payload -TimeoutSec 25

            foreach ($row in @($resp.results)) {
                $url = "$($row.url)"
                if (-not $url) { $url = "$($row.id)" }
                if (-not $url) { continue }

                $hl = ""
                if ($row.highlights) {
                    $hl = (@($row.highlights) | Select-Object -First 2) -join " "
                }
                $snippet = if ($row.summary) { "$($row.summary)" } elseif ($hl) { $hl } else { "" }
                if ($snippet.Length -gt 900) { $snippet = $snippet.Substring(0, 900) }

                $cand = Build-Candidate -Url $url -Source "exa" -Title "$($row.title)" -Snippet $snippet
                Add-OrMergeCandidate -Map $candidates -Candidate $cand
            }
        } catch {
        }
    }
}

$jinaReaderEnrichedCount = 0

$scored = @()
foreach ($cand in @($candidates.Values)) {
    $s = Get-LocalityScore -Candidate $cand -Geo $geo
    $cand | Add-Member -NotePropertyName "locality_score" -NotePropertyValue $s -Force
    $cand | Add-Member -NotePropertyName "is_local_match" -NotePropertyValue ($s -ge 4) -Force
    $cand | Add-Member -NotePropertyName "is_detail_url" -NotePropertyValue (Is-DetailUrl -Url "$($cand.url)") -Force
    $scored += $cand
}

$filtered = @($scored | Where-Object { $_.is_local_match })
$minKeep = [Math]::Min(4, [Math]::Max(1, $scored.Count))
$localityApplied = $false
$list = $scored
if ($filtered.Count -ge $minKeep) {
    $list = $filtered
    $localityApplied = $true
}

$detailApplied = $false
if ($StrictDetailOnly) {
    $detail = @($list | Where-Object { $_.is_detail_url })
    $detailMinKeep = [Math]::Min(4, [Math]::Max(1, [int][Math]::Floor($list.Count / 3)))
    if ($detail.Count -ge $detailMinKeep) {
        $list = $detail
        $detailApplied = $true
    }
}

$list = @($list | Sort-Object -Property `
    @{ Expression = { if ($_.is_local_match) { 1 } else { 0 } }; Descending = $true }, `
    @{ Expression = { if ($_.is_detail_url) { 1 } else { 0 } }; Descending = $true }, `
    @{ Expression = { $_.locality_score }; Descending = $true }, `
    @{ Expression = { @($_.sources).Count }; Descending = $true })

$output = [ordered]@{
    status = "ok"
    location = "$Lat,$Lng"
    geo = $geo
    jina_reader_enriched_count = $jinaReaderEnrichedCount
    locality = [ordered]@{
        filter_applied = $localityApplied
        min_score = 4
        total_candidates = $scored.Count
        filtered_candidates = $filtered.Count
        detail_filter_applied = $detailApplied
        strict_detail_only = $StrictDetailOnly
        strict_district_header = $StrictDistrictHeader
    }
    query_count = $queries.Count
    candidate_count = $list.Count
    queries = $queries
    candidates = @($list | Select-Object -First 50)
    market_rows = @(
        $list |
            Where-Object { $_.price_hint } |
            Select-Object `
                @{Name="listing_type";Expression={$_.listing_type_hint}}, `
                @{Name="property_type";Expression={$_.property_type_hint}}, `
                @{Name="price_try";Expression={$_.price_hint}}, `
                @{Name="size_m2_try";Expression={$_.area_hint}}, `
                @{Name="price_per_m2_try";Expression={ if ($_.price_hint -and $_.area_hint) { [math]::Round($_.price_hint / $_.area_hint, 2) } else { $null } }}, `
                @{Name="title";Expression={$_.title}}, `
                @{Name="url";Expression={$_.url}} |
            Sort-Object -Property `
                @{ Expression = { if ($_.size_m2_try) { 1 } else { 0 } }; Descending = $true }, `
                @{ Expression = { $_.price_try }; Descending = $true } |
            Select-Object -First 50
    )
}

$output | ConvertTo-Json -Depth 8
