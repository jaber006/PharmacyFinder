"""
ABS ASGS 2021 Town Boundary & Postcode Importer

Downloads Suburbs and Localities (SAL) and Postal Areas (POA) from the
ABS ArcGIS Feature Server (geo.abs.gov.au). Stores boundaries in SQLite
for fast point-in-polygon lookups using a bounding-box index + shapely.

Critical for Item 132: "same town" = same locality name + same postcode.

Data vintage: ASGS Edition 3 (2021-2026), based on 2021 Census geography.

Usage:
    py -3.12 data/sources/abs_boundaries.py              # full import
    py -3.12 data/sources/abs_boundaries.py --cached      # use cached data
    py -3.12 data/sources/abs_boundaries.py --test        # test lookups only
"""

import argparse
import json
import logging
import sqlite3
import sys
import time
from pathlib import Path
from typing import List, Optional, Tuple

import requests

try:
    from shapely.geometry import Point, shape
    from shapely import wkt as shapely_wkt
except ImportError:
    print("ERROR: shapely is required. Install with: py -3.12 -m pip install shapely")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_ROOT / "pharmacy_finder.db"
CACHE_DIR = PROJECT_ROOT / "data" / "sources" / "_cache"

# ABS ArcGIS FeatureServer — ASGS 2021 Edition 3
# Layer 1 = generalised boundaries (smaller download, sufficient for town lookups)
SAL_QUERY_URL = "https://geo.abs.gov.au/arcgis/rest/services/ASGS2021/SAL/FeatureServer/1/query"
POA_QUERY_URL = "https://geo.abs.gov.au/arcgis/rest/services/ASGS2021/POA/FeatureServer/1/query"

BATCH_SIZE = 2000
RATE_LIMIT_S = 0.5

log = logging.getLogger("abs_boundaries")

STATE_ABBREVIATIONS = {
    "New South Wales": "NSW",
    "Victoria": "VIC",
    "Queensland": "QLD",
    "South Australia": "SA",
    "Western Australia": "WA",
    "Tasmania": "TAS",
    "Northern Territory": "NT",
    "Australian Capital Territory": "ACT",
    "Other Territories": "OT",
}


def abbreviate_state(state_name: str) -> str:
    return STATE_ABBREVIATIONS.get(state_name, state_name)


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def download_geojson_features(url: str, label: str,
                              out_fields: str = "*") -> List[dict]:
    """Download all features from an ABS ArcGIS FeatureServer endpoint.

    Uses resultOffset pagination. Returns a list of GeoJSON feature dicts.
    """
    all_features: List[dict] = []
    offset = 0

    while True:
        params = {
            "where": "1=1",
            "outFields": out_fields,
            "returnGeometry": "true",
            "f": "geojson",
            "resultRecordCount": BATCH_SIZE,
            "resultOffset": offset,
            "outSR": "4326",
        }

        log.info("  Fetching %s offset=%d ...", label, offset)
        resp = requests.get(url, params=params, timeout=120)
        resp.raise_for_status()

        data = resp.json()
        batch = data.get("features", [])

        if not batch:
            break

        all_features.extend(batch)
        offset += len(batch)
        log.info("  -> %d %s features so far", len(all_features), label)

        if data.get("exceededTransferLimit") or len(batch) >= BATCH_SIZE:
            time.sleep(RATE_LIMIT_S)
            continue
        else:
            break

    log.info("  Downloaded %d %s features total", len(all_features), label)
    return all_features


def save_cache(features, filename: str):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = CACHE_DIR / filename
    with open(path, "w", encoding="utf-8") as f:
        json.dump(features, f)
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

def create_tables(conn: sqlite3.Connection):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS sal_boundaries (
            sal_code    TEXT PRIMARY KEY,
            sal_name    TEXT NOT NULL,
            state_code  TEXT,
            state_name  TEXT,
            area_sqkm   REAL,
            min_lat     REAL,
            max_lat     REAL,
            min_lon     REAL,
            max_lon     REAL,
            geometry_wkt TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS poa_boundaries (
            poa_code    TEXT PRIMARY KEY,
            postcode    TEXT NOT NULL,
            area_sqkm   REAL,
            min_lat     REAL,
            max_lat     REAL,
            min_lon     REAL,
            max_lon     REAL,
            geometry_wkt TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS town_boundaries (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            town_name   TEXT NOT NULL,
            postcode    TEXT NOT NULL,
            state       TEXT NOT NULL,
            sal_code    TEXT,
            geometry_wkt TEXT NOT NULL,
            UNIQUE(town_name, postcode, state)
        );

        CREATE INDEX IF NOT EXISTS idx_sal_bbox
            ON sal_boundaries(min_lat, max_lat, min_lon, max_lon);
        CREATE INDEX IF NOT EXISTS idx_poa_bbox
            ON poa_boundaries(min_lat, max_lat, min_lon, max_lon);
        CREATE INDEX IF NOT EXISTS idx_town_name
            ON town_boundaries(town_name);
        CREATE INDEX IF NOT EXISTS idx_town_postcode
            ON town_boundaries(postcode);
        CREATE INDEX IF NOT EXISTS idx_town_state
            ON town_boundaries(state);
    """)
    conn.commit()


def import_sal_features(conn: sqlite3.Connection,
                        features: List[dict]) -> int:
    count = 0
    for feat in features:
        props = feat.get("properties") or {}
        geom = feat.get("geometry")
        if not props or not geom:
            continue

        sal_code = props.get("sal_code_2021", "")
        sal_name = props.get("sal_name_2021", "")
        state_code = props.get("state_code_2021", "")
        state_name = props.get("state_name_2021", "")
        area_sqkm = props.get("area_albers_sqkm")

        if not sal_code or not sal_name:
            continue

        try:
            shapely_geom = shape(geom)
            if shapely_geom.is_empty:
                continue
            wkt = shapely_geom.wkt
            bounds = shapely_geom.bounds  # (minx, miny, maxx, maxy)
        except Exception as exc:
            log.debug("  Skipping SAL %s: %s", sal_code, exc)
            continue

        conn.execute(
            "INSERT OR REPLACE INTO sal_boundaries "
            "(sal_code, sal_name, state_code, state_name, area_sqkm, "
            " min_lat, max_lat, min_lon, max_lon, geometry_wkt) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (sal_code, sal_name, state_code, state_name, area_sqkm,
             bounds[1], bounds[3], bounds[0], bounds[2], wkt),
        )
        count += 1

    conn.commit()
    return count


def import_poa_features(conn: sqlite3.Connection,
                        features: List[dict]) -> int:
    count = 0
    for feat in features:
        props = feat.get("properties") or {}
        geom = feat.get("geometry")
        if not props or not geom:
            continue

        poa_code = props.get("poa_code_2021", "")
        area_sqkm = props.get("area_albers_sqkm")
        if not poa_code:
            continue

        postcode = poa_code

        try:
            shapely_geom = shape(geom)
            if shapely_geom.is_empty:
                continue
            wkt = shapely_geom.wkt
            bounds = shapely_geom.bounds
        except Exception as exc:
            log.debug("  Skipping POA %s: %s", poa_code, exc)
            continue

        conn.execute(
            "INSERT OR REPLACE INTO poa_boundaries "
            "(poa_code, postcode, area_sqkm, min_lat, max_lat, min_lon, max_lon, geometry_wkt) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (poa_code, postcode, area_sqkm,
             bounds[1], bounds[3], bounds[0], bounds[2], wkt),
        )
        count += 1

    conn.commit()
    return count


def build_town_boundaries(conn: sqlite3.Connection) -> int:
    """Build combined town_boundaries table from SAL + POA data.

    For each SAL, finds the POA containing its centroid to assign a postcode.
    This gives a quick reference table; the precise lookup functions use
    individual SAL and POA boundary checks for maximum accuracy.

    Preloads all POA geometries into memory for speed (~2,600 polygons).
    """
    conn.execute("DELETE FROM town_boundaries")

    # Preload all POA geometries into memory with bboxes
    log.info("  Preloading POA geometries into memory ...")
    poa_index = []  # list of (postcode, min_lat, max_lat, min_lon, max_lon, shapely_geom)
    for row in conn.execute(
        "SELECT postcode, min_lat, max_lat, min_lon, max_lon, geometry_wkt "
        "FROM poa_boundaries"
    ).fetchall():
        try:
            geom = shapely_wkt.loads(row[5])
            poa_index.append((row[0], row[1], row[2], row[3], row[4], geom))
        except Exception:
            continue
    log.info("  Loaded %d POA geometries", len(poa_index))

    cursor = conn.execute("""
        SELECT sal_code, sal_name, state_name, geometry_wkt,
               (min_lat + max_lat) / 2.0 AS centroid_lat,
               (min_lon + max_lon) / 2.0 AS centroid_lon
        FROM sal_boundaries
        WHERE sal_name NOT LIKE '%Migratory%'
          AND sal_name NOT LIKE '%No usual address%'
          AND sal_name NOT LIKE '%Outside Australia%'
    """)
    rows = cursor.fetchall()

    count = 0
    for sal_code, sal_name, state_name, wkt, clat, clon in rows:
        # Find POA(s) whose bbox covers this SAL's centroid
        candidates = [
            (pc, geom) for pc, mn_lat, mx_lat, mn_lon, mx_lon, geom in poa_index
            if mn_lat <= clat <= mx_lat and mn_lon <= clon <= mx_lon
        ]

        postcode = ""
        if len(candidates) == 1:
            postcode = candidates[0][0]
        elif len(candidates) > 1:
            pt = Point(clon, clat)
            for pc, geom in candidates:
                try:
                    if geom.contains(pt):
                        postcode = pc
                        break
                except Exception:
                    continue
            if not postcode:
                postcode = candidates[0][0]

        if not postcode:
            continue

        state = abbreviate_state(state_name) if state_name else ""

        try:
            conn.execute(
                "INSERT OR IGNORE INTO town_boundaries "
                "(town_name, postcode, state, sal_code, geometry_wkt) "
                "VALUES (?,?,?,?,?)",
                (sal_name, postcode, state, sal_code, wkt),
            )
            count += 1
        except sqlite3.IntegrityError:
            pass

        if count % 2000 == 0:
            conn.commit()
            log.info("  ... %d / %d SALs processed", count, len(rows))

    conn.commit()
    log.info("  Built %d town_boundaries records", count)
    return count


# ---------------------------------------------------------------------------
# Lookup functions
# ---------------------------------------------------------------------------

def _lookup_sal(lat: float, lon: float,
                conn: sqlite3.Connection) -> Optional[Tuple[str, str]]:
    """Point-in-polygon against SAL boundaries.

    Returns (sal_name, state_abbreviation) or None.
    """
    candidates = conn.execute(
        "SELECT sal_name, state_name, geometry_wkt FROM sal_boundaries "
        "WHERE min_lat <= ? AND max_lat >= ? "
        "  AND min_lon <= ? AND max_lon >= ?",
        (lat, lat, lon, lon),
    ).fetchall()

    if not candidates:
        return None

    pt = Point(lon, lat)
    for sal_name, state_name, wkt in candidates:
        try:
            if shapely_wkt.loads(wkt).contains(pt):
                return (sal_name, abbreviate_state(state_name or ""))
        except Exception:
            continue

    return None


def _lookup_poa(lat: float, lon: float,
                conn: sqlite3.Connection) -> Optional[str]:
    """Point-in-polygon against POA boundaries.

    Returns postcode string or None.
    """
    candidates = conn.execute(
        "SELECT postcode, geometry_wkt FROM poa_boundaries "
        "WHERE min_lat <= ? AND max_lat >= ? "
        "  AND min_lon <= ? AND max_lon >= ?",
        (lat, lat, lon, lon),
    ).fetchall()

    if not candidates:
        return None

    if len(candidates) == 1:
        return candidates[0][0]

    pt = Point(lon, lat)
    for postcode, wkt in candidates:
        try:
            if shapely_wkt.loads(wkt).contains(pt):
                return postcode
        except Exception:
            continue

    return candidates[0][0] if candidates else None


def get_town(lat: float, lon: float,
             conn: sqlite3.Connection = None) -> Tuple[Optional[str], Optional[str]]:
    """Look up town name and postcode for a coordinate.

    Uses point-in-polygon against SAL (locality name) and POA (postcode)
    boundaries independently for maximum accuracy.

    Args:
        lat: Latitude (WGS84)
        lon: Longitude (WGS84)
        conn: SQLite connection (optional; creates one if not provided)

    Returns:
        (town_name, postcode) — either may be None if not found
    """
    close_conn = False
    if conn is None:
        conn = sqlite3.connect(str(DB_PATH))
        close_conn = True

    try:
        sal_result = _lookup_sal(lat, lon, conn)
        postcode = _lookup_poa(lat, lon, conn)
        town_name = sal_result[0] if sal_result else None
        return (town_name, postcode)
    finally:
        if close_conn:
            conn.close()


def same_town(lat1: float, lon1: float,
              lat2: float, lon2: float,
              conn: sqlite3.Connection = None) -> bool:
    """Check if two points are in the same town.

    "Same town" per Item 132 means same locality name AND same postcode.

    Args:
        lat1, lon1: First point
        lat2, lon2: Second point
        conn: SQLite connection (optional)

    Returns:
        True if both points share the same town name AND postcode.
    """
    close_conn = False
    if conn is None:
        conn = sqlite3.connect(str(DB_PATH))
        close_conn = True

    try:
        town1, pc1 = get_town(lat1, lon1, conn)
        town2, pc2 = get_town(lat2, lon2, conn)

        if not town1 or not town2 or not pc1 or not pc2:
            return False

        return town1.upper() == town2.upper() and pc1 == pc2
    finally:
        if close_conn:
            conn.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _run_tests():
    log.info("")
    log.info("=" * 60)
    log.info("Test lookups")
    log.info("=" * 60)

    test_points = [
        (-33.8688, 151.2093, "Sydney CBD"),
        (-37.8136, 144.9631, "Melbourne CBD"),
        (-27.4698, 153.0251, "Brisbane CBD"),
        (-42.8821, 147.3272, "Hobart CBD"),
        (-34.9285, 138.6007, "Adelaide CBD"),
        (-31.9505, 115.8605, "Perth CBD"),
        (-35.2809, 149.1300, "Canberra CBD"),
        (-12.4634, 130.8456, "Darwin CBD"),
    ]

    conn = sqlite3.connect(str(DB_PATH))

    for lat, lon, label in test_points:
        town, postcode = get_town(lat, lon, conn)
        log.info("  %s: town=%s, postcode=%s", label, town, postcode)

    log.info("")
    log.info("same_town tests:")

    # Two points in Sydney CBD (should be True)
    r = same_town(-33.8688, 151.2093, -33.8700, 151.2100, conn)
    log.info("  Sydney CBD vs nearby point: %s", r)

    # Sydney vs Melbourne (should be False)
    r = same_town(-33.8688, 151.2093, -37.8136, 144.9631, conn)
    log.info("  Sydney vs Melbourne: %s", r)

    conn.close()


def main():
    parser = argparse.ArgumentParser(
        description="Import ABS ASGS 2021 town and postcode boundaries")
    parser.add_argument("--cached", action="store_true",
                        help="Use cached data instead of downloading")
    parser.add_argument("--test", action="store_true",
                        help="Run test lookups after import")
    parser.add_argument("--skip-import", action="store_true",
                        help="Skip import, only run tests")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if args.skip_import:
        if args.test:
            _run_tests()
        return

    conn = sqlite3.connect(str(DB_PATH))
    create_tables(conn)

    # --- SAL boundaries ---
    log.info("=" * 60)
    log.info("SAL (Suburbs and Localities) boundaries")
    log.info("=" * 60)

    if args.cached:
        sal_features = load_cache("abs_sal_boundaries.json")
        if sal_features is None:
            log.error("No cached SAL data. Run without --cached first.")
            sys.exit(1)
    else:
        log.info("Downloading from ABS ArcGIS FeatureServer ...")
        sal_features = download_geojson_features(
            SAL_QUERY_URL, "SAL",
            out_fields="sal_code_2021,sal_name_2021,state_code_2021,"
                       "state_name_2021,area_albers_sqkm",
        )
        save_cache(sal_features, "abs_sal_boundaries.json")

    sal_count = import_sal_features(conn, sal_features)
    log.info("Imported %d SAL boundaries", sal_count)

    # --- POA boundaries ---
    log.info("")
    log.info("=" * 60)
    log.info("POA (Postal Areas) boundaries")
    log.info("=" * 60)

    if args.cached:
        poa_features = load_cache("abs_poa_boundaries.json")
        if poa_features is None:
            log.error("No cached POA data. Run without --cached first.")
            sys.exit(1)
    else:
        log.info("Downloading from ABS ArcGIS FeatureServer ...")
        poa_features = download_geojson_features(
            POA_QUERY_URL, "POA",
            out_fields="poa_code_2021,poa_name_2021,area_albers_sqkm",
        )
        save_cache(poa_features, "abs_poa_boundaries.json")

    poa_count = import_poa_features(conn, poa_features)
    log.info("Imported %d POA boundaries", poa_count)

    # --- Combined town_boundaries ---
    log.info("")
    log.info("=" * 60)
    log.info("Building combined town_boundaries table")
    log.info("=" * 60)

    town_count = build_town_boundaries(conn)
    log.info("Imported %d town boundary records", town_count)

    # Summary
    log.info("")
    log.info("=" * 60)
    log.info("Summary")
    log.info("=" * 60)
    log.info("  SAL boundaries: %d", sal_count)
    log.info("  POA boundaries: %d", poa_count)
    log.info("  town_boundaries: %d", town_count)

    conn.close()

    if args.test:
        _run_tests()


if __name__ == "__main__":
    main()
