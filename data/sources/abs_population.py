"""
ABS 2021 Census Population Data Importer

Downloads SA2-level centroid coordinates from the ABS ArcGIS Feature Server
and Estimated Resident Population (ERP) from the ABS SDMX Data API.
Stores in the population_grid table for radius-based population queries.

Data vintage: 2021 ERP (Estimated Resident Population) by SA2,
              ASGS Edition 3 (2021-2026) boundaries.

Usage:
    py -3.12 data/sources/abs_population.py              # full import
    py -3.12 data/sources/abs_population.py --cached      # use cached data
    py -3.12 data/sources/abs_population.py --test        # test queries only
"""

import argparse
import csv
import io
import json
import logging
import math
import sqlite3
import sys
import time
from pathlib import Path
from typing import Dict, Optional

import requests
from geopy.distance import geodesic

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_ROOT / "pharmacy_finder.db"
CACHE_DIR = PROJECT_ROOT / "data" / "sources" / "_cache"

# ABS ArcGIS FeatureServer — SA2 centroids (layer 2)
# Using f=json (Esri JSON) because f=geojson drops properties for SA2
SA2_CENTROIDS_URL = (
    "https://geo.abs.gov.au/arcgis/rest/services/ASGS2021/SA2/FeatureServer/2/query"
)

# ABS Data API — Estimated Resident Population by SA2
ERP_DATA_URL = (
    "https://data.api.abs.gov.au/rest/data/"
    "ABS,ABS_ANNUAL_ERP_ASGS2021/ERP.SA2..A"
    "?startPeriod=2021&endPeriod=2021&format=csv"
)

BATCH_SIZE = 2000
RATE_LIMIT_S = 0.5

log = logging.getLogger("abs_population")


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def download_sa2_centroids() -> Dict[str, dict]:
    """Download SA2 centroid points from ABS ArcGIS FeatureServer.

    Uses f=json (Esri JSON) because f=geojson drops properties for SA2.
    Returns dict of sa2_code -> {name, lat, lon, area_sqkm, state_name, state_code}.
    """
    centroids: Dict[str, dict] = {}
    offset = 0

    while True:
        params = {
            "where": "1=1",
            "outFields": "sa2_code_2021,sa2_name_2021,"
                         "state_code_2021,state_name_2021,area_albers_sqkm",
            "returnGeometry": "true",
            "f": "json",
            "resultRecordCount": BATCH_SIZE,
            "resultOffset": offset,
            "outSR": "4326",
        }

        log.info("  Fetching SA2 centroids offset=%d ...", offset)
        resp = requests.get(SA2_CENTROIDS_URL, params=params, timeout=120)
        resp.raise_for_status()

        data = resp.json()
        batch = data.get("features", [])

        if not batch:
            break

        for feat in batch:
            attrs = feat.get("attributes", {})
            geom = feat.get("geometry", {})

            sa2_code = str(attrs.get("sa2_code_2021", ""))
            sa2_name = attrs.get("sa2_name_2021", "")

            if not sa2_code or not sa2_name:
                continue

            # Esri JSON geometry — multipoint or point
            lat, lon = None, None
            if "points" in geom and geom["points"]:
                lon, lat = geom["points"][0][0], geom["points"][0][1]
            elif "x" in geom and "y" in geom:
                lon, lat = geom["x"], geom["y"]

            if lat is None or lon is None:
                continue

            centroids[sa2_code] = {
                "name": sa2_name,
                "lat": lat,
                "lon": lon,
                "area_sqkm": attrs.get("area_albers_sqkm"),
                "state_name": attrs.get("state_name_2021", ""),
                "state_code": attrs.get("state_code_2021", ""),
            }

        offset += len(batch)
        log.info("  -> %d SA2 centroids so far", len(centroids))

        if data.get("exceededTransferLimit") or len(batch) >= BATCH_SIZE:
            time.sleep(RATE_LIMIT_S)
            continue
        else:
            break

    log.info("  Downloaded %d SA2 centroids total", len(centroids))
    return centroids


def download_population() -> Dict[str, int]:
    """Download SA2 population from ABS Data API (SDMX CSV).

    Returns dict of sa2_code -> population (2021 ERP).
    """
    log.info("  Fetching ERP data from ABS Data API ...")

    resp = requests.get(ERP_DATA_URL, timeout=120)
    resp.raise_for_status()

    populations: Dict[str, int] = {}
    reader = csv.DictReader(io.StringIO(resp.text))

    for row in reader:
        sa2_code = row.get("ASGS_2021", "")
        pop_str = row.get("OBS_VALUE", "")

        if sa2_code and pop_str:
            try:
                populations[sa2_code] = int(float(pop_str))
            except (ValueError, TypeError):
                continue

    log.info("  Downloaded population for %d SA2 areas", len(populations))
    return populations


def save_cache(data, filename: str):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = CACHE_DIR / filename
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f)
    log.info("  Cached to %s", path)


def load_cache(filename: str):
    path = CACHE_DIR / filename
    if not path.exists():
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def create_table(conn: sqlite3.Connection):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS population_grid (
            sa2_code    TEXT PRIMARY KEY,
            sa2_name    TEXT NOT NULL,
            population  INTEGER,
            area_sqkm   REAL,
            lat         REAL NOT NULL,
            lon         REAL NOT NULL,
            state_name  TEXT,
            state_code  TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_pop_lat
            ON population_grid(lat);
        CREATE INDEX IF NOT EXISTS idx_pop_lon
            ON population_grid(lon);
        CREATE INDEX IF NOT EXISTS idx_pop_state
            ON population_grid(state_code);
    """)
    conn.commit()


def import_population(conn: sqlite3.Connection,
                      centroids: Dict[str, dict],
                      populations: Dict[str, int]) -> int:
    conn.execute("DELETE FROM population_grid")

    count = 0
    for sa2_code, info in centroids.items():
        pop = populations.get(sa2_code)

        conn.execute(
            "INSERT OR REPLACE INTO population_grid "
            "(sa2_code, sa2_name, population, area_sqkm, lat, lon, "
            " state_name, state_code) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (sa2_code, info["name"], pop, info.get("area_sqkm"),
             info["lat"], info["lon"],
             info.get("state_name"), info.get("state_code")),
        )
        count += 1

    conn.commit()
    return count


# ---------------------------------------------------------------------------
# Query functions
# ---------------------------------------------------------------------------

def population_within_radius(lat: float, lon: float, radius_km: float,
                             conn: sqlite3.Connection = None) -> int:
    """Estimate population within a radius of a point.

    Sums the ERP of all SA2 areas whose centroids fall within the
    specified radius. For large SA2 areas partially overlapping the
    radius, the full population is included when the centroid is in range.

    Args:
        lat: Latitude of centre point (WGS84)
        lon: Longitude of centre point (WGS84)
        radius_km: Search radius in kilometres
        conn: SQLite connection (optional; creates one if not provided)

    Returns:
        Estimated total population within the radius
    """
    close_conn = False
    if conn is None:
        conn = sqlite3.connect(str(DB_PATH))
        close_conn = True

    try:
        # Bounding-box pre-filter (1 deg lat ~ 111 km)
        lat_margin = radius_km / 111.0
        lon_margin = radius_km / (111.0 * max(math.cos(math.radians(lat)), 0.01))

        rows = conn.execute(
            "SELECT population, lat, lon FROM population_grid "
            "WHERE population IS NOT NULL "
            "  AND lat BETWEEN ? AND ? "
            "  AND lon BETWEEN ? AND ?",
            (lat - lat_margin, lat + lat_margin,
             lon - lon_margin, lon + lon_margin),
        ).fetchall()

        total = 0
        for pop, sa2_lat, sa2_lon in rows:
            dist = geodesic((lat, lon), (sa2_lat, sa2_lon)).kilometers
            if dist <= radius_km:
                total += pop

        return total
    finally:
        if close_conn:
            conn.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _run_tests():
    log.info("")
    log.info("=" * 60)
    log.info("Test population queries")
    log.info("=" * 60)

    test_points = [
        (-33.8688, 151.2093, "Sydney CBD", 5),
        (-33.8688, 151.2093, "Sydney CBD", 10),
        (-37.8136, 144.9631, "Melbourne CBD", 5),
        (-37.8136, 144.9631, "Melbourne CBD", 10),
        (-42.8821, 147.3272, "Hobart CBD", 10),
        (-27.4698, 153.0251, "Brisbane CBD", 10),
        (-23.7000, 133.8700, "Alice Springs", 20),
    ]

    conn = sqlite3.connect(str(DB_PATH))

    for lat, lon, label, radius in test_points:
        pop = population_within_radius(lat, lon, radius, conn)
        log.info("  %s (%d km radius): ~%s people", label, radius, f"{pop:,}")

    conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Import ABS 2021 Census population data by SA2")
    parser.add_argument("--cached", action="store_true",
                        help="Use cached data instead of downloading")
    parser.add_argument("--test", action="store_true",
                        help="Run test queries after import")
    parser.add_argument("--skip-import", action="store_true",
                        help="Skip import, only run tests")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if args.skip_import:
        if args.test:
            _run_tests()
        return

    conn = sqlite3.connect(str(DB_PATH))
    create_table(conn)

    # --- SA2 centroids ---
    log.info("=" * 60)
    log.info("SA2 centroids")
    log.info("=" * 60)

    if args.cached:
        centroids = load_cache("abs_sa2_centroids.json")
        if centroids is None:
            log.error("No cached SA2 centroid data. Run without --cached first.")
            sys.exit(1)
    else:
        centroids = download_sa2_centroids()
        save_cache(centroids, "abs_sa2_centroids.json")

    # --- Population ---
    log.info("")
    log.info("=" * 60)
    log.info("ERP population data")
    log.info("=" * 60)

    if args.cached:
        populations = load_cache("abs_erp_population.json")
        if populations is None:
            log.error("No cached population data. Run without --cached first.")
            sys.exit(1)
    else:
        populations = download_population()
        save_cache(populations, "abs_erp_population.json")

    # --- Import ---
    log.info("")
    log.info("=" * 60)
    log.info("Importing to database")
    log.info("=" * 60)

    count = import_population(conn, centroids, populations)

    total_pop = conn.execute(
        "SELECT SUM(population) FROM population_grid WHERE population IS NOT NULL"
    ).fetchone()[0]
    matched = conn.execute(
        "SELECT COUNT(*) FROM population_grid WHERE population IS NOT NULL"
    ).fetchone()[0]

    log.info("Imported %d SA2 areas (%d with population data)", count, matched)
    if total_pop:
        log.info("  Total population: %s", f"{total_pop:,}")

    conn.close()

    if args.test:
        _run_tests()


if __name__ == "__main__":
    main()
