$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

$CacheDir = Join-Path $ProjectRoot "cache"
$PbfFile  = Join-Path $CacheDir "australia-latest.osm.pbf"
$OsrmFile = Join-Path $CacheDir "australia-latest.osrm"

try { docker ps | Out-Null } catch { Write-Error "Docker is not running. Start Docker Desktop first."; exit 1 }

if (!(Test-Path $PbfFile)) {
    Write-Host "Downloading Australia OSM extract (~920 MB)..."
    Invoke-WebRequest -Uri "https://download.geofabrik.de/australia-oceania/australia-latest.osm.pbf" -OutFile $PbfFile -UseBasicParsing
}
Write-Host "PBF file ready."

if (!(Test-Path $OsrmFile)) {
    Write-Host "Step 1/3: osrm-extract (10-20 min)..."
    docker run --rm -v "${CacheDir}:/data" osrm/osrm-backend:latest osrm-extract -p /opt/car.lua /data/australia-latest.osm.pbf

    Write-Host "Step 2/3: osrm-partition..."
    docker run --rm -v "${CacheDir}:/data" osrm/osrm-backend:latest osrm-partition /data/australia-latest.osrm

    Write-Host "Step 3/3: osrm-customize..."
    docker run --rm -v "${CacheDir}:/data" osrm/osrm-backend:latest osrm-customize /data/australia-latest.osrm

    Write-Host "Pre-processing complete!"
} else {
    Write-Host "OSRM data already pre-processed."
}

Write-Host "Starting OSRM server on port 5000..."
docker compose up -d osrm

Start-Sleep -Seconds 3
try {
    $resp = Invoke-RestMethod "http://localhost:5000/nearest/v1/driving/151.2093,-33.8688" -TimeoutSec 5
    if ($resp.code -eq "Ok") { Write-Host "OSRM server is UP and responding." }
} catch {
    Write-Host "Server starting... may need a few more seconds."
}
