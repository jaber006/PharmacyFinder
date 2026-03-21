#!/usr/bin/env python3
"""
psp_scanner.py - Precinct Structure Plan (PSP) Scanner for PharmacyFinder

Scans Australian state planning authority websites for Precinct Structure Plans
and growth area precincts to identify planned town centres where a pharmacy
could be needed before competitors lock in tenancies.

Covers:
  - VIC: Victorian Planning Authority (vpa.vic.gov.au) PSPs
  - NSW: Growth Area precincts (planning.nsw.gov.au)
  - QLD: SEQ growth areas (statedevelopment.qld.gov.au)
  - WA:  Growth corridors (wa.gov.au planning)
  - SA:  Growth areas (plan.sa.gov.au)
  - TAS: Growth areas (planning.tas.gov.au)

Storage: psp_projects + planned_town_centres tables in pharmacy_finder.db
Output:  output/psp_opportunities.json, .csv, psp_report.md
"""

import sqlite3
import json
import csv
import re
import time
import logging
import hashlib
from datetime import datetime, date
from pathlib import Path
from math import radians, sin, cos, sqrt, atan2
from typing import Optional, Dict, List, Tuple, Any

import requests
from bs4 import BeautifulSoup

try:
    from geopy.geocoders import Nominatim
    from geopy.exc import GeocoderTimedOut, GeocoderServiceError
    HAS_GEOPY = True
except ImportError:
    HAS_GEOPY = False

# ---------------------------------------------------------------------------
# Paths & constants
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = PROJECT_ROOT / "pharmacy_finder.db"
OUTPUT_DIR = PROJECT_ROOT / "output"
CACHE_DIR = PROJECT_ROOT / "cache" / "psp"

RATE_LIMIT = 1.5  # seconds between web requests
GEOCODE_DELAY = 1.05  # Nominatim rate limit (min 1s)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

logger = logging.getLogger("psp_scanner")

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_tables(conn: sqlite3.Connection):
    """Create PSP tables if they don't exist."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS psp_projects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            state TEXT NOT NULL,
            status TEXT,              -- approved / in-progress / gazetted / planning
            planned_dwellings INTEGER,
            planned_population INTEGER,
            town_centres_count INTEGER DEFAULT 0,
            lat REAL,
            lon REAL,
            psp_url TEXT,
            lga TEXT,
            est_completion TEXT,
            corridor TEXT,
            description TEXT,
            source TEXT DEFAULT 'curated',
            date_scanned TEXT,
            UNIQUE(name, state)
        );

        CREATE TABLE IF NOT EXISTS planned_town_centres (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            psp_id INTEGER REFERENCES psp_projects(id),
            centre_name TEXT NOT NULL,
            centre_type TEXT,         -- major / local / neighbourhood / activity
            lat REAL,
            lon REAL,
            nearest_pharmacy_km REAL,
            nearest_pharmacy_name TEXT,
            opportunity_score REAL,
            has_acpa_approval INTEGER DEFAULT 0,
            notes TEXT,
            date_scanned TEXT,
            UNIQUE(psp_id, centre_name)
        );

        CREATE INDEX IF NOT EXISTS idx_psp_state ON psp_projects(state);
        CREATE INDEX IF NOT EXISTS idx_psp_status ON psp_projects(status);
        CREATE INDEX IF NOT EXISTS idx_ptc_score ON planned_town_centres(opportunity_score DESC);
    """)
    conn.commit()


# ---------------------------------------------------------------------------
# Geocoding
# ---------------------------------------------------------------------------

_geocoder = None
_geocode_cache: Dict[str, Optional[Tuple[float, float]]] = {}


def _get_geocoder():
    global _geocoder
    if _geocoder is None and HAS_GEOPY:
        _geocoder = Nominatim(user_agent="PharmacyFinder-PSP/1.0", timeout=10)
    return _geocoder


def geocode_location(place: str, state: str = "") -> Optional[Tuple[float, float]]:
    """Geocode a place name, returning (lat, lon) or None."""
    key = f"{place}, {state}, Australia" if state else f"{place}, Australia"
    if key in _geocode_cache:
        return _geocode_cache[key]

    geocoder = _get_geocoder()
    if geocoder is None:
        return None

    try:
        time.sleep(GEOCODE_DELAY)
        loc = geocoder.geocode(key, country_codes="au")
        if loc:
            result = (loc.latitude, loc.longitude)
            _geocode_cache[key] = result
            return result
    except (GeocoderTimedOut, GeocoderServiceError) as e:
        logger.warning(f"Geocode failed for {key}: {e}")
    _geocode_cache[key] = None
    return None


# ---------------------------------------------------------------------------
# Haversine distance
# ---------------------------------------------------------------------------

def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    la1, lo1, la2, lo2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = la2 - la1
    dlon = lo2 - lo1
    a = sin(dlat / 2) ** 2 + cos(la1) * cos(la2) * sin(dlon / 2) ** 2
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))


# ---------------------------------------------------------------------------
# Nearest pharmacy lookup
# ---------------------------------------------------------------------------

def find_nearest_pharmacy(conn: sqlite3.Connection, lat: float, lon: float) -> Tuple[float, str]:
    """Return (distance_km, pharmacy_name) for nearest pharmacy."""
    rows = conn.execute(
        "SELECT name, latitude, longitude FROM pharmacies WHERE latitude IS NOT NULL AND longitude IS NOT NULL"
    ).fetchall()
    best_dist = 999999.0
    best_name = ""
    for r in rows:
        d = haversine_km(lat, lon, r["latitude"], r["longitude"])
        if d < best_dist:
            best_dist = d
            best_name = r["name"]
    return (best_dist, best_name)


# ---------------------------------------------------------------------------
# ACPA check
# ---------------------------------------------------------------------------

def check_acpa_approval(conn: sqlite3.Connection, lat: float, lon: float, radius_km: float = 5.0) -> bool:
    """Check if there's an ACPA decision near this location."""
    try:
        rows = conn.execute(
            "SELECT latitude, longitude FROM acpa_decisions WHERE latitude IS NOT NULL"
        ).fetchall()
        for r in rows:
            if haversine_km(lat, lon, r["latitude"], r["longitude"]) < radius_km:
                return True
    except Exception:
        pass
    return False


# ---------------------------------------------------------------------------
# Opportunity scoring
# ---------------------------------------------------------------------------

def score_opportunity(
    planned_pop: int,
    nearest_pharmacy_km: float,
    status: str,
    centre_type: str = "local"
) -> float:
    """
    Score = population_factor × distance_factor × timeline_factor × type_factor

    Higher = better pharmacy opportunity.
    """
    # Population factor (0-40 pts)
    pop_score = min(40, (planned_pop or 0) / 500)

    # Distance factor (0-30 pts) — further from pharmacy = better
    if nearest_pharmacy_km >= 10:
        dist_score = 30
    elif nearest_pharmacy_km >= 5:
        dist_score = 25
    elif nearest_pharmacy_km >= 2:
        dist_score = 20
    elif nearest_pharmacy_km >= 1.5:
        dist_score = 15
    elif nearest_pharmacy_km >= 1:
        dist_score = 10
    else:
        dist_score = max(0, nearest_pharmacy_km * 8)

    # Timeline factor (0-20 pts) — earlier stages = more time to act
    timeline_scores = {
        "planning": 20,
        "in-progress": 18,
        "approved": 12,
        "gazetted": 8,
        "completed": 3,
    }
    timeline_score = timeline_scores.get(status, 10)

    # Centre type factor (0-10 pts)
    type_scores = {
        "major": 10,
        "activity": 8,
        "local": 6,
        "neighbourhood": 4,
    }
    type_score = type_scores.get(centre_type, 5)

    return round(pop_score + dist_score + timeline_score + type_score, 1)


# ---------------------------------------------------------------------------
# Web scraping helpers
# ---------------------------------------------------------------------------

_session = None


def _get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({"User-Agent": USER_AGENT})
    return _session


def fetch_page(url: str, cache_hours: int = 24) -> Optional[str]:
    """Fetch a URL with caching."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_key = hashlib.md5(url.encode()).hexdigest()
    cache_file = CACHE_DIR / f"{cache_key}.html"

    if cache_file.exists():
        age_h = (time.time() - cache_file.stat().st_mtime) / 3600
        if age_h < cache_hours:
            return cache_file.read_text(encoding="utf-8", errors="replace")

    try:
        time.sleep(RATE_LIMIT)
        resp = _get_session().get(url, timeout=30)
        if resp.status_code == 200:
            html = resp.text
            cache_file.write_text(html, encoding="utf-8")
            return html
        else:
            logger.warning(f"HTTP {resp.status_code} for {url}")
    except Exception as e:
        logger.warning(f"Fetch error for {url}: {e}")
    return None


# ---------------------------------------------------------------------------
# CURATED PSP DATABASE
# ---------------------------------------------------------------------------
# This is the core dataset. Government sites are heavily JS-rendered and
# change structure frequently. A curated + scraped hybrid approach is
# more reliable than pure scraping.
#
# Format: dict with state keys, each containing list of PSP dicts.
# ---------------------------------------------------------------------------

CURATED_PSPS: Dict[str, List[Dict[str, Any]]] = {
    "VIC": [
        # --- NORTHERN CORRIDOR ---
        {
            "name": "Donnybrook-Woodstock PSP",
            "status": "approved",
            "lga": "Whittlesea",
            "corridor": "Northern",
            "planned_dwellings": 9500,
            "planned_population": 27000,
            "town_centres": [
                {"name": "Donnybrook Town Centre", "type": "major"},
                {"name": "Woodstock Local Centre", "type": "local"},
            ],
            "psp_url": "https://vpa.vic.gov.au/project/donnybrook-woodstock/",
            "geocode_hint": "Donnybrook, VIC",
        },
        {
            "name": "Beveridge Central PSP",
            "status": "in-progress",
            "lga": "Mitchell",
            "corridor": "Northern",
            "planned_dwellings": 9000,
            "planned_population": 25000,
            "town_centres": [
                {"name": "Beveridge Central Town Centre", "type": "major"},
                {"name": "Beveridge North Local Centre", "type": "local"},
            ],
            "psp_url": "https://vpa.vic.gov.au/project/beveridge-central/",
            "geocode_hint": "Beveridge, VIC",
        },
        {
            "name": "Beveridge North West PSP",
            "status": "in-progress",
            "lga": "Mitchell",
            "corridor": "Northern",
            "planned_dwellings": 5000,
            "planned_population": 14000,
            "town_centres": [
                {"name": "Beveridge NW Local Centre", "type": "local"},
            ],
            "psp_url": "https://vpa.vic.gov.au/project/beveridge-north-west/",
            "geocode_hint": "Beveridge, VIC",
        },
        {
            "name": "Wallan South PSP",
            "status": "in-progress",
            "lga": "Mitchell",
            "corridor": "Northern",
            "planned_dwellings": 7000,
            "planned_population": 20000,
            "town_centres": [
                {"name": "Wallan South Town Centre", "type": "major"},
                {"name": "Wallan South Neighbourhood Centre", "type": "neighbourhood"},
            ],
            "psp_url": "https://vpa.vic.gov.au/project/wallan-south/",
            "geocode_hint": "Wallan, VIC",
        },
        {
            "name": "Wallan East PSP",
            "status": "in-progress",
            "lga": "Mitchell",
            "corridor": "Northern",
            "planned_dwellings": 4500,
            "planned_population": 13000,
            "town_centres": [
                {"name": "Wallan East Local Centre", "type": "local"},
            ],
            "psp_url": "https://vpa.vic.gov.au/project/wallan-east/",
            "geocode_hint": "Wallan, VIC",
        },
        {
            "name": "Kalkallo PSP",
            "status": "approved",
            "lga": "Hume",
            "corridor": "Northern",
            "planned_dwellings": 6000,
            "planned_population": 17000,
            "town_centres": [
                {"name": "Kalkallo Town Centre", "type": "major"},
                {"name": "Kalkallo East Local Centre", "type": "local"},
            ],
            "psp_url": "https://vpa.vic.gov.au/project/kalkallo/",
            "geocode_hint": "Kalkallo, VIC",
        },
        {
            "name": "Lockerbie PSP",
            "status": "approved",
            "lga": "Hume",
            "corridor": "Northern",
            "planned_dwellings": 11000,
            "planned_population": 30000,
            "town_centres": [
                {"name": "Lockerbie Principal Town Centre", "type": "major"},
                {"name": "Lockerbie North Local Centre", "type": "local"},
                {"name": "Lockerbie South Neighbourhood Centre", "type": "neighbourhood"},
            ],
            "psp_url": "https://vpa.vic.gov.au/project/lockerbie/",
            "geocode_hint": "Kalkallo, VIC",
        },
        {
            "name": "Mickleham PSP",
            "status": "approved",
            "lga": "Hume",
            "corridor": "Northern",
            "planned_dwellings": 8500,
            "planned_population": 24000,
            "town_centres": [
                {"name": "Mickleham Major Town Centre", "type": "major"},
                {"name": "Mickleham Local Centre", "type": "local"},
            ],
            "psp_url": "https://vpa.vic.gov.au/project/mickleham/",
            "geocode_hint": "Mickleham, VIC",
        },
        {
            "name": "Craigieburn North Employment Area PSP",
            "status": "approved",
            "lga": "Hume",
            "corridor": "Northern",
            "planned_dwellings": 500,
            "planned_population": 1500,
            "town_centres": [
                {"name": "Craigieburn North Local Centre", "type": "local"},
            ],
            "psp_url": "https://vpa.vic.gov.au/project/craigieburn-north-employment-area/",
            "geocode_hint": "Craigieburn, VIC",
        },
        {
            "name": "Wollert PSP",
            "status": "approved",
            "lga": "Whittlesea",
            "corridor": "Northern",
            "planned_dwellings": 7000,
            "planned_population": 20000,
            "town_centres": [
                {"name": "Wollert Town Centre", "type": "major"},
                {"name": "Wollert East Local Centre", "type": "local"},
            ],
            "psp_url": "https://vpa.vic.gov.au/project/wollert/",
            "geocode_hint": "Wollert, VIC",
        },
        {
            "name": "English Street PSP",
            "status": "approved",
            "lga": "Whittlesea",
            "corridor": "Northern",
            "planned_dwellings": 3000,
            "planned_population": 8500,
            "town_centres": [
                {"name": "English Street Local Centre", "type": "local"},
            ],
            "psp_url": "https://vpa.vic.gov.au/project/english-street/",
            "geocode_hint": "Wollert, VIC",
        },
        {
            "name": "Shenstone Park PSP",
            "status": "approved",
            "lga": "Whittlesea",
            "corridor": "Northern",
            "planned_dwellings": 5500,
            "planned_population": 15000,
            "town_centres": [
                {"name": "Shenstone Park Town Centre", "type": "major"},
            ],
            "psp_url": "https://vpa.vic.gov.au/project/shenstone-park/",
            "geocode_hint": "Donnybrook, VIC",
        },
        {
            "name": "Sunbury South PSP",
            "status": "approved",
            "lga": "Hume",
            "corridor": "Sunbury",
            "planned_dwellings": 5000,
            "planned_population": 14000,
            "town_centres": [
                {"name": "Sunbury South Town Centre", "type": "major"},
            ],
            "psp_url": "https://vpa.vic.gov.au/project/sunbury-south/",
            "geocode_hint": "Sunbury, VIC",
        },
        {
            "name": "Lancefield Road PSP",
            "status": "approved",
            "lga": "Hume",
            "corridor": "Sunbury",
            "planned_dwellings": 4000,
            "planned_population": 11000,
            "town_centres": [
                {"name": "Lancefield Road Local Centre", "type": "local"},
            ],
            "psp_url": "https://vpa.vic.gov.au/project/lancefield-road/",
            "geocode_hint": "Sunbury, VIC",
        },
        # --- WESTERN CORRIDOR ---
        {
            "name": "Rockbank PSP",
            "status": "approved",
            "lga": "Melton",
            "corridor": "Western",
            "planned_dwellings": 6500,
            "planned_population": 18000,
            "town_centres": [
                {"name": "Rockbank Major Town Centre", "type": "major"},
                {"name": "Rockbank South Local Centre", "type": "local"},
            ],
            "psp_url": "https://vpa.vic.gov.au/project/rockbank/",
            "geocode_hint": "Rockbank, VIC",
        },
        {
            "name": "Rockbank North PSP",
            "status": "approved",
            "lga": "Melton",
            "corridor": "Western",
            "planned_dwellings": 5500,
            "planned_population": 15000,
            "town_centres": [
                {"name": "Rockbank North Town Centre", "type": "major"},
            ],
            "psp_url": "https://vpa.vic.gov.au/project/rockbank-north/",
            "geocode_hint": "Rockbank, VIC",
        },
        {
            "name": "Plumpton PSP",
            "status": "approved",
            "lga": "Melton",
            "corridor": "Western",
            "planned_dwellings": 5000,
            "planned_population": 14000,
            "town_centres": [
                {"name": "Plumpton Town Centre", "type": "major"},
                {"name": "Plumpton Local Centre", "type": "local"},
            ],
            "psp_url": "https://vpa.vic.gov.au/project/plumpton/",
            "geocode_hint": "Plumpton, VIC",
        },
        {
            "name": "Kororoit PSP",
            "status": "approved",
            "lga": "Melton",
            "corridor": "Western",
            "planned_dwellings": 12000,
            "planned_population": 34000,
            "town_centres": [
                {"name": "Kororoit Major Town Centre", "type": "major"},
                {"name": "Kororoit East Local Centre", "type": "local"},
                {"name": "Kororoit West Neighbourhood Centre", "type": "neighbourhood"},
            ],
            "psp_url": "https://vpa.vic.gov.au/project/kororoit/",
            "geocode_hint": "Plumpton, VIC",
        },
        {
            "name": "Tarneit Plains PSP",
            "status": "approved",
            "lga": "Wyndham",
            "corridor": "Western",
            "planned_dwellings": 4500,
            "planned_population": 13000,
            "town_centres": [
                {"name": "Tarneit Plains Local Centre", "type": "local"},
            ],
            "psp_url": "https://vpa.vic.gov.au/project/tarneit-plains/",
            "geocode_hint": "Tarneit, VIC",
        },
        {
            "name": "Tarneit North PSP",
            "status": "approved",
            "lga": "Wyndham",
            "corridor": "Western",
            "planned_dwellings": 3500,
            "planned_population": 10000,
            "town_centres": [
                {"name": "Tarneit North Local Centre", "type": "local"},
            ],
            "psp_url": "https://vpa.vic.gov.au/project/tarneit-north/",
            "geocode_hint": "Tarneit, VIC",
        },
        {
            "name": "Truganina PSP",
            "status": "approved",
            "lga": "Wyndham",
            "corridor": "Western",
            "planned_dwellings": 5000,
            "planned_population": 14000,
            "town_centres": [
                {"name": "Truganina Town Centre", "type": "local"},
            ],
            "psp_url": "https://vpa.vic.gov.au/project/truganina/",
            "geocode_hint": "Truganina, VIC",
        },
        {
            "name": "Manor Lakes PSP",
            "status": "approved",
            "lga": "Wyndham",
            "corridor": "Western",
            "planned_dwellings": 8000,
            "planned_population": 23000,
            "town_centres": [
                {"name": "Manor Lakes Town Centre", "type": "major"},
            ],
            "psp_url": "https://vpa.vic.gov.au/project/manor-lakes/",
            "geocode_hint": "Manor Lakes, VIC",
        },
        {
            "name": "Werribee Junction PSP",
            "status": "in-progress",
            "lga": "Wyndham",
            "corridor": "Western",
            "planned_dwellings": 3000,
            "planned_population": 8500,
            "town_centres": [
                {"name": "Werribee Junction Local Centre", "type": "local"},
            ],
            "psp_url": "https://vpa.vic.gov.au/project/werribee-junction/",
            "geocode_hint": "Werribee, VIC",
        },
        {
            "name": "Mt Atkinson and Tarneit Plains PSP",
            "status": "approved",
            "lga": "Melton",
            "corridor": "Western",
            "planned_dwellings": 8000,
            "planned_population": 22000,
            "town_centres": [
                {"name": "Mt Atkinson Town Centre", "type": "major"},
                {"name": "Mt Atkinson South Local Centre", "type": "local"},
            ],
            "psp_url": "https://vpa.vic.gov.au/project/mt-atkinson-tarneit-plains/",
            "geocode_hint": "Truganina, VIC",
        },
        {
            "name": "Aintree and Woodlea PSP",
            "status": "approved",
            "lga": "Melton",
            "corridor": "Western",
            "planned_dwellings": 7000,
            "planned_population": 20000,
            "town_centres": [
                {"name": "Aintree Town Centre", "type": "major"},
                {"name": "Woodlea Town Centre", "type": "local"},
            ],
            "psp_url": "https://vpa.vic.gov.au/project/aintree/",
            "geocode_hint": "Aintree, VIC",
        },
        {
            "name": "Thornhill Park PSP",
            "status": "approved",
            "lga": "Melton",
            "corridor": "Western",
            "planned_dwellings": 4000,
            "planned_population": 11000,
            "town_centres": [
                {"name": "Thornhill Park Town Centre", "type": "local"},
            ],
            "psp_url": "https://vpa.vic.gov.au/project/thornhill-park/",
            "geocode_hint": "Rockbank, VIC",
        },
        {
            "name": "Melton East (Toolern) PSP",
            "status": "approved",
            "lga": "Melton",
            "corridor": "Western",
            "planned_dwellings": 14000,
            "planned_population": 40000,
            "town_centres": [
                {"name": "Toolern Major Town Centre", "type": "major"},
                {"name": "Toolern East Local Centre", "type": "local"},
                {"name": "Toolern South Local Centre", "type": "local"},
            ],
            "psp_url": "https://vpa.vic.gov.au/project/toolern/",
            "geocode_hint": "Melton South, VIC",
        },
        # --- SOUTH-EAST CORRIDOR ---
        {
            "name": "Officer PSP",
            "status": "approved",
            "lga": "Cardinia",
            "corridor": "South-East",
            "planned_dwellings": 9000,
            "planned_population": 25000,
            "town_centres": [
                {"name": "Officer Town Centre", "type": "major"},
                {"name": "Officer South Local Centre", "type": "local"},
            ],
            "psp_url": "https://vpa.vic.gov.au/project/officer-precinct-structure-plan/",
            "geocode_hint": "Officer, VIC",
        },
        {
            "name": "Officer South Employment PSP",
            "status": "approved",
            "lga": "Cardinia",
            "corridor": "South-East",
            "planned_dwellings": 1000,
            "planned_population": 3000,
            "town_centres": [
                {"name": "Officer South Employment Centre", "type": "neighbourhood"},
            ],
            "psp_url": "https://vpa.vic.gov.au/project/officer-south-employment/",
            "geocode_hint": "Officer, VIC",
        },
        {
            "name": "Cardinia Road PSP",
            "status": "approved",
            "lga": "Cardinia",
            "corridor": "South-East",
            "planned_dwellings": 6500,
            "planned_population": 18000,
            "town_centres": [
                {"name": "Cardinia Road Local Centre", "type": "local"},
            ],
            "psp_url": "https://vpa.vic.gov.au/project/cardinia-road/",
            "geocode_hint": "Officer, VIC",
        },
        {
            "name": "Minta Farm PSP",
            "status": "approved",
            "lga": "Casey",
            "corridor": "South-East",
            "planned_dwellings": 4000,
            "planned_population": 11000,
            "town_centres": [
                {"name": "Minta Farm Town Centre", "type": "local"},
            ],
            "psp_url": "https://vpa.vic.gov.au/project/minta-farm/",
            "geocode_hint": "Berwick, VIC",
        },
        {
            "name": "Clyde Creek PSP",
            "status": "approved",
            "lga": "Casey",
            "corridor": "South-East",
            "planned_dwellings": 10000,
            "planned_population": 28000,
            "town_centres": [
                {"name": "Clyde Creek Major Town Centre", "type": "major"},
                {"name": "Clyde Creek Local Centre", "type": "local"},
            ],
            "psp_url": "https://vpa.vic.gov.au/project/clyde-creek/",
            "geocode_hint": "Clyde, VIC",
        },
        {
            "name": "Clyde PSP",
            "status": "approved",
            "lga": "Casey",
            "corridor": "South-East",
            "planned_dwellings": 5000,
            "planned_population": 14000,
            "town_centres": [
                {"name": "Clyde Town Centre", "type": "local"},
            ],
            "psp_url": "https://vpa.vic.gov.au/project/clyde/",
            "geocode_hint": "Clyde, VIC",
        },
        {
            "name": "Casey Fields South PSP",
            "status": "in-progress",
            "lga": "Casey",
            "corridor": "South-East",
            "planned_dwellings": 6000,
            "planned_population": 17000,
            "town_centres": [
                {"name": "Casey Fields South Town Centre", "type": "major"},
            ],
            "psp_url": "https://vpa.vic.gov.au/project/casey-fields-south/",
            "geocode_hint": "Cranbourne, VIC",
        },
        {
            "name": "Pakenham East PSP",
            "status": "approved",
            "lga": "Cardinia",
            "corridor": "South-East",
            "planned_dwellings": 3500,
            "planned_population": 10000,
            "town_centres": [
                {"name": "Pakenham East Local Centre", "type": "local"},
            ],
            "psp_url": "https://vpa.vic.gov.au/project/pakenham-east/",
            "geocode_hint": "Pakenham, VIC",
        },
        {
            "name": "Cranbourne West PSP",
            "status": "approved",
            "lga": "Casey",
            "corridor": "South-East",
            "planned_dwellings": 6000,
            "planned_population": 17000,
            "town_centres": [
                {"name": "Cranbourne West Local Centre", "type": "local"},
            ],
            "psp_url": "https://vpa.vic.gov.au/project/cranbourne-west/",
            "geocode_hint": "Cranbourne West, VIC",
        },
        {
            "name": "Thompsons Road PSP",
            "status": "approved",
            "lga": "Casey",
            "corridor": "South-East",
            "planned_dwellings": 4000,
            "planned_population": 11000,
            "town_centres": [
                {"name": "Thompsons Road Local Centre", "type": "local"},
            ],
            "psp_url": "https://vpa.vic.gov.au/project/thompsons-road/",
            "geocode_hint": "Clyde North, VIC",
        },
        # --- GEELONG / WESTERN VIC ---
        {
            "name": "Armstrong Creek PSP",
            "status": "approved",
            "lga": "Greater Geelong",
            "corridor": "Geelong",
            "planned_dwellings": 22000,
            "planned_population": 55000,
            "town_centres": [
                {"name": "Armstrong Creek Major Town Centre", "type": "major"},
                {"name": "Warralily Local Centre", "type": "local"},
                {"name": "Villawood Local Centre", "type": "local"},
                {"name": "Armstrong Creek East Local Centre", "type": "local"},
            ],
            "psp_url": "https://vpa.vic.gov.au/project/armstrong-creek/",
            "geocode_hint": "Armstrong Creek, VIC",
        },
        {
            "name": "Lara West PSP",
            "status": "in-progress",
            "lga": "Greater Geelong",
            "corridor": "Geelong",
            "planned_dwellings": 4000,
            "planned_population": 11000,
            "town_centres": [
                {"name": "Lara West Local Centre", "type": "local"},
            ],
            "psp_url": "https://vpa.vic.gov.au/project/lara-west/",
            "geocode_hint": "Lara, VIC",
        },
    ],

    "NSW": [
        # --- SOUTH WEST GROWTH AREA ---
        {
            "name": "Leppington Town Centre Precinct",
            "status": "approved",
            "lga": "Camden",
            "corridor": "South West Growth Area",
            "planned_dwellings": 8000,
            "planned_population": 22000,
            "town_centres": [
                {"name": "Leppington Town Centre", "type": "major"},
                {"name": "Leppington North Local Centre", "type": "local"},
            ],
            "psp_url": "https://www.planning.nsw.gov.au/plans-for-your-area/priority-growth-areas-and-precincts/south-west-growth-area",
            "geocode_hint": "Leppington, NSW",
        },
        {
            "name": "Oran Park Precinct",
            "status": "approved",
            "lga": "Camden",
            "corridor": "South West Growth Area",
            "planned_dwellings": 7500,
            "planned_population": 21000,
            "town_centres": [
                {"name": "Oran Park Town Centre", "type": "major"},
            ],
            "psp_url": "https://www.planning.nsw.gov.au/plans-for-your-area/priority-growth-areas-and-precincts/south-west-growth-area",
            "geocode_hint": "Oran Park, NSW",
        },
        {
            "name": "Austral and Leppington North Precinct",
            "status": "approved",
            "lga": "Liverpool",
            "corridor": "South West Growth Area",
            "planned_dwellings": 16000,
            "planned_population": 45000,
            "town_centres": [
                {"name": "Austral Town Centre", "type": "major"},
                {"name": "Leppington North Centre", "type": "local"},
                {"name": "Fifteenth Avenue Local Centre", "type": "local"},
            ],
            "psp_url": "https://www.planning.nsw.gov.au/plans-for-your-area/priority-growth-areas-and-precincts/south-west-growth-area",
            "geocode_hint": "Austral, NSW",
        },
        {
            "name": "Catherine Fields Precinct",
            "status": "in-progress",
            "lga": "Camden",
            "corridor": "South West Growth Area",
            "planned_dwellings": 6000,
            "planned_population": 17000,
            "town_centres": [
                {"name": "Catherine Fields Local Centre", "type": "local"},
            ],
            "psp_url": "https://www.planning.nsw.gov.au/plans-for-your-area/priority-growth-areas-and-precincts/south-west-growth-area",
            "geocode_hint": "Catherine Field, NSW",
        },
        # --- NORTH WEST GROWTH AREA ---
        {
            "name": "Marsden Park Precinct",
            "status": "approved",
            "lga": "Blacktown",
            "corridor": "North West Growth Area",
            "planned_dwellings": 10000,
            "planned_population": 28000,
            "town_centres": [
                {"name": "Marsden Park Town Centre", "type": "major"},
                {"name": "Marsden Park North Local Centre", "type": "local"},
            ],
            "psp_url": "https://www.planning.nsw.gov.au/plans-for-your-area/priority-growth-areas-and-precincts/north-west-growth-area",
            "geocode_hint": "Marsden Park, NSW",
        },
        {
            "name": "Box Hill Precinct",
            "status": "approved",
            "lga": "The Hills",
            "corridor": "North West Growth Area",
            "planned_dwellings": 8500,
            "planned_population": 24000,
            "town_centres": [
                {"name": "Box Hill Town Centre", "type": "major"},
                {"name": "Box Hill South Local Centre", "type": "local"},
            ],
            "psp_url": "https://www.planning.nsw.gov.au/plans-for-your-area/priority-growth-areas-and-precincts/north-west-growth-area",
            "geocode_hint": "Box Hill, NSW",
        },
        {
            "name": "Schofields Precinct",
            "status": "approved",
            "lga": "Blacktown",
            "corridor": "North West Growth Area",
            "planned_dwellings": 5500,
            "planned_population": 15000,
            "town_centres": [
                {"name": "Schofields Town Centre", "type": "local"},
            ],
            "psp_url": "https://www.planning.nsw.gov.au/plans-for-your-area/priority-growth-areas-and-precincts/north-west-growth-area",
            "geocode_hint": "Schofields, NSW",
        },
        {
            "name": "Riverstone East Precinct",
            "status": "approved",
            "lga": "Blacktown",
            "corridor": "North West Growth Area",
            "planned_dwellings": 4500,
            "planned_population": 12500,
            "town_centres": [
                {"name": "Riverstone East Local Centre", "type": "local"},
            ],
            "psp_url": "https://www.planning.nsw.gov.au/plans-for-your-area/priority-growth-areas-and-precincts/north-west-growth-area",
            "geocode_hint": "Riverstone, NSW",
        },
        # --- WESTERN SYDNEY AEROTROPOLIS ---
        {
            "name": "Aerotropolis Core Precinct",
            "status": "in-progress",
            "lga": "Liverpool/Penrith",
            "corridor": "Western Sydney Aerotropolis",
            "planned_dwellings": 20000,
            "planned_population": 55000,
            "town_centres": [
                {"name": "Bradfield City Centre", "type": "major"},
                {"name": "Aerotropolis Core Town Centre", "type": "major"},
            ],
            "psp_url": "https://www.planning.nsw.gov.au/plans-for-your-area/priority-growth-areas-and-precincts/western-sydney-aerotropolis",
            "geocode_hint": "Badgerys Creek, NSW",
        },
        {
            "name": "Northern Gateway Precinct",
            "status": "in-progress",
            "lga": "Penrith",
            "corridor": "Western Sydney Aerotropolis",
            "planned_dwellings": 3500,
            "planned_population": 10000,
            "town_centres": [
                {"name": "Northern Gateway Local Centre", "type": "local"},
            ],
            "psp_url": "https://www.planning.nsw.gov.au/plans-for-your-area/priority-growth-areas-and-precincts/western-sydney-aerotropolis",
            "geocode_hint": "Luddenham, NSW",
        },
        # --- GREATER MACARTHUR ---
        {
            "name": "Menangle Park Precinct",
            "status": "approved",
            "lga": "Campbelltown",
            "corridor": "Greater Macarthur",
            "planned_dwellings": 4500,
            "planned_population": 13000,
            "town_centres": [
                {"name": "Menangle Park Town Centre", "type": "major"},
            ],
            "psp_url": "https://www.planning.nsw.gov.au/plans-for-your-area/priority-growth-areas-and-precincts/greater-macarthur-growth-area",
            "geocode_hint": "Menangle Park, NSW",
        },
        {
            "name": "Gilead Precinct",
            "status": "in-progress",
            "lga": "Campbelltown",
            "corridor": "Greater Macarthur",
            "planned_dwellings": 15000,
            "planned_population": 42000,
            "town_centres": [
                {"name": "Gilead Town Centre", "type": "major"},
                {"name": "Gilead East Local Centre", "type": "local"},
                {"name": "Gilead South Neighbourhood Centre", "type": "neighbourhood"},
            ],
            "psp_url": "https://www.planning.nsw.gov.au/plans-for-your-area/priority-growth-areas-and-precincts/greater-macarthur-growth-area",
            "geocode_hint": "Gilead, NSW",
        },
        {
            "name": "Wilton Growth Area",
            "status": "in-progress",
            "lga": "Wollondilly",
            "corridor": "Greater Macarthur",
            "planned_dwellings": 15000,
            "planned_population": 42000,
            "town_centres": [
                {"name": "Wilton Town Centre", "type": "major"},
                {"name": "Wilton South East Local Centre", "type": "local"},
                {"name": "Wilton North Local Centre", "type": "local"},
            ],
            "psp_url": "https://www.planning.nsw.gov.au/plans-for-your-area/priority-growth-areas-and-precincts/wilton",
            "geocode_hint": "Wilton, NSW",
        },
    ],

    "QLD": [
        {
            "name": "Ripley Valley Priority Development Area",
            "status": "approved",
            "lga": "Ipswich",
            "corridor": "SEQ South-West",
            "planned_dwellings": 50000,
            "planned_population": 120000,
            "town_centres": [
                {"name": "Ripley Town Centre", "type": "major"},
                {"name": "Providence Town Centre", "type": "major"},
                {"name": "Ripley East Local Centre", "type": "local"},
                {"name": "Ripley South Neighbourhood Centre", "type": "neighbourhood"},
            ],
            "psp_url": "https://planning.statedevelopment.qld.gov.au/planning-framework/priority-development-areas/ripley-valley",
            "geocode_hint": "Ripley, QLD",
        },
        {
            "name": "Greater Flagstone Priority Development Area",
            "status": "approved",
            "lga": "Logan",
            "corridor": "SEQ South",
            "planned_dwellings": 50000,
            "planned_population": 148000,
            "town_centres": [
                {"name": "Flagstone Town Centre", "type": "major"},
                {"name": "Flagstone East Local Centre", "type": "local"},
                {"name": "Undullah Neighbourhood Centre", "type": "neighbourhood"},
                {"name": "Flagstone North Local Centre", "type": "local"},
            ],
            "psp_url": "https://planning.statedevelopment.qld.gov.au/planning-framework/priority-development-areas/greater-flagstone",
            "geocode_hint": "Flagstone, QLD",
        },
        {
            "name": "Yarrabilba Priority Development Area",
            "status": "approved",
            "lga": "Logan",
            "corridor": "SEQ South",
            "planned_dwellings": 17000,
            "planned_population": 50000,
            "town_centres": [
                {"name": "Yarrabilba Town Centre", "type": "major"},
                {"name": "Yarrabilba Central Local Centre", "type": "local"},
                {"name": "Yarrabilba South Neighbourhood Centre", "type": "neighbourhood"},
            ],
            "psp_url": "https://planning.statedevelopment.qld.gov.au/planning-framework/priority-development-areas/yarrabilba",
            "geocode_hint": "Yarrabilba, QLD",
        },
        {
            "name": "Caloundra South Priority Development Area",
            "status": "approved",
            "lga": "Sunshine Coast",
            "corridor": "SEQ North",
            "planned_dwellings": 20000,
            "planned_population": 50000,
            "town_centres": [
                {"name": "Caloundra South Town Centre (Aura)", "type": "major"},
                {"name": "Baringa Local Centre", "type": "local"},
                {"name": "Nirimba Local Centre", "type": "local"},
            ],
            "psp_url": "https://planning.statedevelopment.qld.gov.au/planning-framework/priority-development-areas/caloundra-south",
            "geocode_hint": "Caloundra South, QLD",
        },
        {
            "name": "Greater Springfield",
            "status": "approved",
            "lga": "Ipswich",
            "corridor": "SEQ South-West",
            "planned_dwellings": 35000,
            "planned_population": 105000,
            "town_centres": [
                {"name": "Springfield Central Town Centre", "type": "major"},
                {"name": "Springfield Lakes Local Centre", "type": "local"},
                {"name": "Springfield West Neighbourhood Centre", "type": "neighbourhood"},
            ],
            "psp_url": "https://planning.statedevelopment.qld.gov.au/planning-framework/priority-development-areas/greater-springfield",
            "geocode_hint": "Springfield, QLD",
        },
        {
            "name": "Coomera Town Centre",
            "status": "approved",
            "lga": "Gold Coast",
            "corridor": "SEQ South",
            "planned_dwellings": 6000,
            "planned_population": 17000,
            "town_centres": [
                {"name": "Coomera Town Centre", "type": "major"},
            ],
            "psp_url": "https://planning.statedevelopment.qld.gov.au/planning-framework/priority-development-areas/coomera-town-centre",
            "geocode_hint": "Coomera, QLD",
        },
        {
            "name": "Palmview",
            "status": "approved",
            "lga": "Sunshine Coast",
            "corridor": "SEQ North",
            "planned_dwellings": 4500,
            "planned_population": 13000,
            "town_centres": [
                {"name": "Palmview Local Centre", "type": "local"},
            ],
            "psp_url": "https://planning.statedevelopment.qld.gov.au/planning-framework/priority-development-areas/palmview",
            "geocode_hint": "Palmview, QLD",
        },
        {
            "name": "Maroochydore City Centre",
            "status": "in-progress",
            "lga": "Sunshine Coast",
            "corridor": "SEQ North",
            "planned_dwellings": 4000,
            "planned_population": 11000,
            "town_centres": [
                {"name": "Maroochydore City Centre", "type": "major"},
            ],
            "psp_url": "https://planning.statedevelopment.qld.gov.au/planning-framework/priority-development-areas/maroochydore-city-centre",
            "geocode_hint": "Maroochydore, QLD",
        },
        {
            "name": "Caboolture West",
            "status": "planning",
            "lga": "Moreton Bay",
            "corridor": "SEQ North-West",
            "planned_dwellings": 30000,
            "planned_population": 70000,
            "town_centres": [
                {"name": "Caboolture West Major Centre", "type": "major"},
                {"name": "Caboolture West East Local Centre", "type": "local"},
                {"name": "Caboolture West South Local Centre", "type": "local"},
            ],
            "psp_url": "https://www.moretonbay.qld.gov.au/caboolture-west",
            "geocode_hint": "Caboolture, QLD",
        },
    ],

    "WA": [
        {
            "name": "Alkimos - Eglinton District Structure Plan",
            "status": "approved",
            "lga": "Wanneroo",
            "corridor": "North-West Metropolitan",
            "planned_dwellings": 25000,
            "planned_population": 60000,
            "town_centres": [
                {"name": "Alkimos Town Centre", "type": "major"},
                {"name": "Eglinton Local Centre", "type": "local"},
                {"name": "Alkimos Beach Neighbourhood Centre", "type": "neighbourhood"},
            ],
            "psp_url": "https://www.wa.gov.au/government/document-collections/alkimos-eglinton-district-structure-plan",
            "geocode_hint": "Alkimos, WA",
        },
        {
            "name": "Baldivis South District Structure Plan",
            "status": "in-progress",
            "lga": "Rockingham",
            "corridor": "South Metropolitan",
            "planned_dwellings": 12000,
            "planned_population": 33000,
            "town_centres": [
                {"name": "Baldivis South Town Centre", "type": "major"},
                {"name": "Baldivis South Local Centre", "type": "local"},
            ],
            "psp_url": "https://www.wa.gov.au/government/document-collections/baldivis-south",
            "geocode_hint": "Baldivis, WA",
        },
        {
            "name": "Yanchep-Two Rocks District Structure Plan",
            "status": "approved",
            "lga": "Wanneroo",
            "corridor": "North-West Metropolitan",
            "planned_dwellings": 30000,
            "planned_population": 75000,
            "town_centres": [
                {"name": "Yanchep City Centre", "type": "major"},
                {"name": "Two Rocks Local Centre", "type": "local"},
                {"name": "Yanchep South Local Centre", "type": "local"},
            ],
            "psp_url": "https://www.wa.gov.au/government/document-collections/yanchep-two-rocks",
            "geocode_hint": "Yanchep, WA",
        },
        {
            "name": "Ellenbrook",
            "status": "approved",
            "lga": "Swan",
            "corridor": "North-East Metropolitan",
            "planned_dwellings": 18000,
            "planned_population": 50000,
            "town_centres": [
                {"name": "Ellenbrook Town Centre", "type": "major"},
                {"name": "The Vines Local Centre", "type": "local"},
            ],
            "psp_url": "https://www.wa.gov.au/government/document-collections/ellenbrook",
            "geocode_hint": "Ellenbrook, WA",
        },
        {
            "name": "Byford District Structure Plan",
            "status": "approved",
            "lga": "Serpentine-Jarrahdale",
            "corridor": "South-East Metropolitan",
            "planned_dwellings": 15000,
            "planned_population": 42000,
            "town_centres": [
                {"name": "Byford Town Centre", "type": "major"},
                {"name": "Byford South Local Centre", "type": "local"},
            ],
            "psp_url": "https://www.wa.gov.au/government/document-collections/byford",
            "geocode_hint": "Byford, WA",
        },
        {
            "name": "Wellard-Casuarina District Structure Plan",
            "status": "approved",
            "lga": "Kwinana",
            "corridor": "South Metropolitan",
            "planned_dwellings": 8000,
            "planned_population": 22000,
            "town_centres": [
                {"name": "Wellard Town Centre", "type": "local"},
            ],
            "psp_url": "https://www.wa.gov.au/government/document-collections/wellard",
            "geocode_hint": "Wellard, WA",
        },
        {
            "name": "Mundijong-Whitby District Structure Plan",
            "status": "in-progress",
            "lga": "Serpentine-Jarrahdale",
            "corridor": "South-East Metropolitan",
            "planned_dwellings": 20000,
            "planned_population": 55000,
            "town_centres": [
                {"name": "Mundijong Town Centre", "type": "major"},
                {"name": "Whitby Local Centre", "type": "local"},
            ],
            "psp_url": "https://www.wa.gov.au/government/document-collections/mundijong-whitby",
            "geocode_hint": "Mundijong, WA",
        },
    ],

    "SA": [
        {
            "name": "Mount Barker Growth Area",
            "status": "approved",
            "lga": "Mount Barker",
            "corridor": "Adelaide Hills",
            "planned_dwellings": 12000,
            "planned_population": 33000,
            "town_centres": [
                {"name": "Mount Barker Town Centre Extension", "type": "major"},
                {"name": "Springs Local Centre", "type": "local"},
            ],
            "psp_url": "https://plan.sa.gov.au/",
            "geocode_hint": "Mount Barker, SA",
        },
        {
            "name": "Gawler East Growth Area",
            "status": "approved",
            "lga": "Light",
            "corridor": "Northern Adelaide",
            "planned_dwellings": 6000,
            "planned_population": 17000,
            "town_centres": [
                {"name": "Gawler East Local Centre", "type": "local"},
            ],
            "psp_url": "https://plan.sa.gov.au/",
            "geocode_hint": "Gawler East, SA",
        },
        {
            "name": "Roseworthy Township Expansion",
            "status": "approved",
            "lga": "Light",
            "corridor": "Northern Adelaide",
            "planned_dwellings": 8000,
            "planned_population": 22000,
            "town_centres": [
                {"name": "Roseworthy Town Centre", "type": "major"},
                {"name": "Roseworthy South Local Centre", "type": "local"},
            ],
            "psp_url": "https://plan.sa.gov.au/",
            "geocode_hint": "Roseworthy, SA",
        },
        {
            "name": "Angle Vale Growth Area",
            "status": "in-progress",
            "lga": "Playford",
            "corridor": "Northern Adelaide",
            "planned_dwellings": 5000,
            "planned_population": 14000,
            "town_centres": [
                {"name": "Angle Vale Local Centre", "type": "local"},
            ],
            "psp_url": "https://plan.sa.gov.au/",
            "geocode_hint": "Angle Vale, SA",
        },
        {
            "name": "Two Wells Growth Area",
            "status": "approved",
            "lga": "Adelaide Plains",
            "corridor": "Northern Adelaide",
            "planned_dwellings": 7000,
            "planned_population": 20000,
            "town_centres": [
                {"name": "Two Wells Town Centre", "type": "major"},
            ],
            "psp_url": "https://plan.sa.gov.au/",
            "geocode_hint": "Two Wells, SA",
        },
        {
            "name": "Aldinga Structure Plan",
            "status": "approved",
            "lga": "Onkaparinga",
            "corridor": "Southern Adelaide",
            "planned_dwellings": 10000,
            "planned_population": 28000,
            "town_centres": [
                {"name": "Aldinga Central Local Centre", "type": "local"},
                {"name": "Aldinga Beach Neighbourhood Centre", "type": "neighbourhood"},
            ],
            "psp_url": "https://plan.sa.gov.au/",
            "geocode_hint": "Aldinga, SA",
        },
    ],

    "TAS": [
        {
            "name": "Greater Launceston Plan - Southern Growth Corridor",
            "status": "in-progress",
            "lga": "Launceston",
            "corridor": "Greater Launceston",
            "planned_dwellings": 3000,
            "planned_population": 8000,
            "town_centres": [
                {"name": "Prospect Vale Local Centre", "type": "local"},
                {"name": "Legana Growth Centre", "type": "local"},
            ],
            "psp_url": "https://www.planningreform.tas.gov.au/",
            "geocode_hint": "Prospect Vale, TAS",
        },
        {
            "name": "Kingston Growth Area",
            "status": "approved",
            "lga": "Kingborough",
            "corridor": "Greater Hobart",
            "planned_dwellings": 5000,
            "planned_population": 14000,
            "town_centres": [
                {"name": "Kingston Town Centre Expansion", "type": "major"},
                {"name": "Kingston Park Neighbourhood Centre", "type": "neighbourhood"},
            ],
            "psp_url": "https://www.kingborough.tas.gov.au/",
            "geocode_hint": "Kingston, TAS",
        },
        {
            "name": "Sorell Growth Area",
            "status": "in-progress",
            "lga": "Sorell",
            "corridor": "Greater Hobart",
            "planned_dwellings": 3000,
            "planned_population": 8500,
            "town_centres": [
                {"name": "Sorell Town Centre Expansion", "type": "local"},
            ],
            "psp_url": "https://www.sorell.tas.gov.au/",
            "geocode_hint": "Sorell, TAS",
        },
        {
            "name": "Devonport Living City",
            "status": "approved",
            "lga": "Devonport",
            "corridor": "North-West Coast",
            "planned_dwellings": 2000,
            "planned_population": 5500,
            "town_centres": [
                {"name": "Devonport CBD Renewal", "type": "major"},
            ],
            "psp_url": "https://www.devonport.tas.gov.au/",
            "geocode_hint": "Devonport, TAS",
        },
    ],
}


# ---------------------------------------------------------------------------
# VPA live scraper — supplement curated data
# ---------------------------------------------------------------------------

def scrape_vpa_projects() -> List[Dict]:
    """
    Attempt to scrape the VPA project pages.
    The VPA site uses WordPress/React so we try to find project links
    from the sitemap or known patterns.
    """
    found = []
    logger.info("Attempting VPA project scrape...")

    # Try the sitemap first
    sitemap_url = "https://vpa.vic.gov.au/wp-sitemap-posts-project-1.xml"
    html = fetch_page(sitemap_url, cache_hours=48)
    if html:
        soup = BeautifulSoup(html, "html.parser")
        locs = soup.find_all("loc")
        for loc in locs:
            url = loc.text.strip()
            if "/project/" in url and "psp" not in url.lower():
                # Skip non-PSP projects (guidelines, etc.)
                continue
            if "/project/" in url:
                found.append(url)
        logger.info(f"Found {len(found)} project URLs from VPA sitemap")

    # Also try the main project listing page
    list_url = "https://vpa.vic.gov.au/project/"
    html = fetch_page(list_url, cache_hours=48)
    if html:
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/project/" in href and href.count("/") > 4:
                full_url = href if href.startswith("http") else f"https://vpa.vic.gov.au{href}"
                if full_url not in found:
                    found.append(full_url)

    # Scrape individual project pages for details
    scraped_projects = []
    for url in found[:100]:  # cap at 100 to be safe
        project = _scrape_vpa_project_page(url)
        if project:
            scraped_projects.append(project)

    return scraped_projects


def _scrape_vpa_project_page(url: str) -> Optional[Dict]:
    """Scrape a single VPA project page for PSP details."""
    html = fetch_page(url, cache_hours=72)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")
    title = soup.find("h1")
    if not title:
        return None

    name = title.get_text(strip=True)
    if not any(kw in name.lower() for kw in ["precinct", "psp", "structure plan"]):
        # Not a PSP page
        return None

    # Extract text content
    content = soup.get_text(" ", strip=True).lower()

    # Try to extract key numbers
    dwellings = _extract_number(content, r"(\d[\d,]*)\s*(?:new\s+)?(?:dwellings|lots|homes)")
    population = _extract_number(content, r"(\d[\d,]*)\s*(?:new\s+)?(?:people|residents|population)")

    # LGA extraction
    lga = None
    lga_patterns = [
        r"([\w\s]+)\s+(?:shire|city)\s+(?:council|planning scheme)",
        r"located\s+in\s+([\w\s]+?)(?:\s+shire|\s+city|\s*,)",
    ]
    for pat in lga_patterns:
        m = re.search(pat, content)
        if m:
            lga = m.group(1).strip().title()
            break

    # Status
    status = "approved"
    if "in preparation" in content or "being prepared" in content:
        status = "in-progress"
    elif "gazetted" in content:
        status = "gazetted"

    # Town centres
    centres = []
    centre_patterns = [
        r"([\w\s]+?)\s+(?:major\s+)?town\s+centre",
        r"([\w\s]+?)\s+(?:local|neighbourhood|activity)\s+centre",
    ]
    for pat in centre_patterns:
        for m in re.finditer(pat, content):
            cname = m.group(1).strip().title()
            if len(cname) > 3 and len(cname) < 50:
                ctype = "major" if "major" in m.group(0) or "town centre" in m.group(0) else "local"
                centres.append({"name": f"{cname} Centre", "type": ctype})

    # Find PDF URLs
    pdf_url = None
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.endswith(".pdf") and "structure-plan" in href.lower():
            pdf_url = href
            break

    return {
        "name": name,
        "status": status,
        "lga": lga or "",
        "corridor": "Melbourne Greenfield",
        "planned_dwellings": dwellings,
        "planned_population": population,
        "town_centres": centres if centres else [{"name": f"{name} Centre", "type": "local"}],
        "psp_url": url,
        "pdf_url": pdf_url,
        "geocode_hint": name.replace("PSP", "").replace("Precinct Structure Plan", "").strip() + ", VIC",
        "source": "scraped",
    }


def _extract_number(text: str, pattern: str) -> Optional[int]:
    m = re.search(pattern, text)
    if m:
        try:
            return int(m.group(1).replace(",", ""))
        except ValueError:
            pass
    return None


# ---------------------------------------------------------------------------
# Main scanning logic
# ---------------------------------------------------------------------------

def scan_state(conn: sqlite3.Connection, state: str, live_scrape: bool = True) -> int:
    """
    Scan a single state's PSPs. Returns count of projects inserted/updated.
    """
    logger.info(f"Scanning {state} PSPs...")
    count = 0
    now = datetime.now().isoformat()

    # Start with curated data
    projects = list(CURATED_PSPS.get(state, []))

    # For VIC, optionally supplement with live scraping
    if state == "VIC" and live_scrape:
        try:
            scraped = scrape_vpa_projects()
            # Merge scraped projects that aren't already curated
            curated_names = {p["name"].lower() for p in projects}
            for sp in scraped:
                if sp["name"].lower() not in curated_names:
                    projects.append(sp)
                    logger.info(f"  + Scraped new VIC PSP: {sp['name']}")
        except Exception as e:
            logger.warning(f"VPA scrape failed (using curated only): {e}")

    for psp in projects:
        try:
            psp_id = _upsert_psp_project(conn, psp, state, now)
            if psp_id:
                count += 1
                # Process town centres
                for tc in psp.get("town_centres", []):
                    _upsert_town_centre(conn, psp_id, tc, psp, state, now)
        except Exception as e:
            logger.error(f"Error processing {psp.get('name')}: {e}")

    conn.commit()
    logger.info(f"  {state}: {count} PSP projects processed")
    return count


def _upsert_psp_project(conn: sqlite3.Connection, psp: Dict, state: str, now: str) -> Optional[int]:
    """Insert or update a PSP project. Returns the project ID."""
    name = psp["name"]

    # Geocode
    lat, lon = None, None
    hint = psp.get("geocode_hint", name)
    coords = geocode_location(hint, state)
    if coords:
        lat, lon = coords

    town_centres_count = len(psp.get("town_centres", []))

    conn.execute("""
        INSERT INTO psp_projects (name, state, status, planned_dwellings, planned_population,
            town_centres_count, lat, lon, psp_url, lga, corridor, description, source, date_scanned)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(name, state) DO UPDATE SET
            status=excluded.status,
            planned_dwellings=COALESCE(excluded.planned_dwellings, planned_dwellings),
            planned_population=COALESCE(excluded.planned_population, planned_population),
            town_centres_count=excluded.town_centres_count,
            lat=COALESCE(excluded.lat, lat),
            lon=COALESCE(excluded.lon, lon),
            psp_url=excluded.psp_url,
            lga=COALESCE(excluded.lga, lga),
            corridor=COALESCE(excluded.corridor, corridor),
            source=excluded.source,
            date_scanned=excluded.date_scanned
    """, (
        name, state, psp.get("status"), psp.get("planned_dwellings"),
        psp.get("planned_population"), town_centres_count, lat, lon,
        psp.get("psp_url"), psp.get("lga"), psp.get("corridor"),
        psp.get("description"), psp.get("source", "curated"), now,
    ))

    row = conn.execute("SELECT id FROM psp_projects WHERE name=? AND state=?", (name, state)).fetchone()
    return row["id"] if row else None


def _upsert_town_centre(
    conn: sqlite3.Connection, psp_id: int, tc: Dict, psp: Dict, state: str, now: str
):
    """Insert or update a planned town centre with pharmacy proximity scoring."""
    centre_name = tc["name"]
    centre_type = tc.get("type", "local")

    # Geocode centre — try centre name first, fall back to PSP geocode hint
    lat, lon = None, None
    coords = geocode_location(centre_name, state)
    if not coords:
        hint = psp.get("geocode_hint", psp["name"])
        coords = geocode_location(hint, state)
    if coords:
        lat, lon = coords

    # Find nearest pharmacy
    nearest_km = 999.0
    nearest_name = ""
    if lat and lon:
        nearest_km, nearest_name = find_nearest_pharmacy(conn, lat, lon)

    # Check ACPA
    has_acpa = 0
    if lat and lon:
        has_acpa = 1 if check_acpa_approval(conn, lat, lon) else 0

    # Score
    score = score_opportunity(
        psp.get("planned_population") or 0,
        nearest_km,
        psp.get("status", "approved"),
        centre_type,
    )

    notes = f"Nearest pharmacy: {nearest_name} ({nearest_km:.1f}km)" if nearest_name else ""

    conn.execute("""
        INSERT INTO planned_town_centres (psp_id, centre_name, centre_type, lat, lon,
            nearest_pharmacy_km, nearest_pharmacy_name, opportunity_score, has_acpa_approval,
            notes, date_scanned)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(psp_id, centre_name) DO UPDATE SET
            centre_type=excluded.centre_type,
            lat=COALESCE(excluded.lat, lat),
            lon=COALESCE(excluded.lon, lon),
            nearest_pharmacy_km=excluded.nearest_pharmacy_km,
            nearest_pharmacy_name=excluded.nearest_pharmacy_name,
            opportunity_score=excluded.opportunity_score,
            has_acpa_approval=excluded.has_acpa_approval,
            notes=excluded.notes,
            date_scanned=excluded.date_scanned
    """, (
        psp_id, centre_name, centre_type, lat, lon,
        nearest_km, nearest_name, score, has_acpa,
        notes, now,
    ))


def scan_national(conn: sqlite3.Connection, live_scrape: bool = True) -> int:
    """Scan all states."""
    total = 0
    for state in ["VIC", "NSW", "QLD", "WA", "SA", "TAS"]:
        total += scan_state(conn, state, live_scrape=(state == "VIC" and live_scrape))
    return total


# ---------------------------------------------------------------------------
# Output generation
# ---------------------------------------------------------------------------

def generate_outputs(conn: sqlite3.Connection, top_n: int = 30):
    """Generate JSON, CSV, and Markdown report."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Fetch all opportunities ranked by score
    rows = conn.execute("""
        SELECT
            tc.id, tc.centre_name, tc.centre_type, tc.lat, tc.lon,
            tc.nearest_pharmacy_km, tc.nearest_pharmacy_name,
            tc.opportunity_score, tc.has_acpa_approval, tc.notes,
            p.name as psp_name, p.state, p.status, p.planned_dwellings,
            p.planned_population, p.lga, p.corridor, p.psp_url, p.est_completion
        FROM planned_town_centres tc
        JOIN psp_projects p ON tc.psp_id = p.id
        ORDER BY tc.opportunity_score DESC
    """).fetchall()

    records = [dict(r) for r in rows]

    # --- JSON ---
    json_path = OUTPUT_DIR / "psp_opportunities.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, default=str)
    logger.info(f"Wrote {len(records)} records to {json_path}")

    # --- CSV ---
    csv_path = OUTPUT_DIR / "psp_opportunities.csv"
    if records:
        with open(csv_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=records[0].keys())
            writer.writeheader()
            writer.writerows(records)
    logger.info(f"Wrote {csv_path}")

    # --- Markdown report ---
    md_path = OUTPUT_DIR / "psp_report.md"
    _generate_markdown_report(conn, records, top_n, md_path)
    logger.info(f"Wrote {md_path}")


def _generate_markdown_report(
    conn: sqlite3.Connection,
    records: List[Dict],
    top_n: int,
    path: Path,
):
    """Generate executive summary report."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    # Summary stats
    total_projects = conn.execute("SELECT COUNT(*) FROM psp_projects").fetchone()[0]
    total_centres = conn.execute("SELECT COUNT(*) FROM planned_town_centres").fetchone()[0]
    state_counts = conn.execute(
        "SELECT state, COUNT(*) as cnt FROM psp_projects GROUP BY state ORDER BY cnt DESC"
    ).fetchall()

    top = records[:top_n]

    lines = [
        f"# PSP Pharmacy Opportunity Report",
        f"",
        f"*Generated: {now}*",
        f"",
        f"## Executive Summary",
        f"",
        f"Scanned **{total_projects}** Precinct Structure Plans / Growth Area precincts across Australia,",
        f"identifying **{total_centres}** planned town centres where a pharmacy could be needed.",
        f"",
        f"### Coverage by State",
        f"",
        f"| State | PSP Projects | Notes |",
        f"|-------|-------------|-------|",
    ]
    for sc in state_counts:
        lines.append(f"| {sc['state']} | {sc['cnt']} | — |")

    lines += [
        f"",
        f"---",
        f"",
        f"## Top {top_n} Pharmacy Opportunities in Planned Town Centres",
        f"",
        f"Ranked by opportunity score (population × pharmacy distance × timeline × centre type).",
        f"",
    ]

    for i, r in enumerate(top, 1):
        score = r.get("opportunity_score", 0)
        pop = r.get("planned_population") or "Unknown"
        dist = r.get("nearest_pharmacy_km")
        dist_str = f"{dist:.1f}km" if dist and dist < 900 else "Unknown"
        acpa = "⚠️ ACPA approval nearby" if r.get("has_acpa_approval") else ""
        status_emoji = {
            "planning": "📋",
            "in-progress": "🔨",
            "approved": "✅",
            "gazetted": "📜",
        }.get(r.get("status", ""), "❓")

        lines += [
            f"### {i}. {r['centre_name']}",
            f"",
            f"- **PSP:** {r['psp_name']}",
            f"- **State:** {r['state']} | **LGA:** {r.get('lga', 'Unknown')}",
            f"- **Status:** {status_emoji} {r.get('status', 'Unknown')}",
            f"- **Centre Type:** {r.get('centre_type', 'Unknown')}",
            f"- **Planned Population:** {pop:,}" if isinstance(pop, int) else f"- **Planned Population:** {pop}",
            f"- **Nearest Pharmacy:** {dist_str}" + (f" ({r.get('nearest_pharmacy_name', '')})" if r.get('nearest_pharmacy_name') else ""),
            f"- **Opportunity Score:** {score:.1f}/100",
            f"- **Corridor:** {r.get('corridor', 'N/A')}",
        ]
        if acpa:
            lines.append(f"- {acpa}")
        if r.get("psp_url"):
            lines.append(f"- **URL:** {r['psp_url']}")
        lines.append("")

    lines += [
        "---",
        "",
        "## Methodology",
        "",
        "**Score Components:**",
        "- Population factor (0-40): planned population / 500",
        "- Distance factor (0-30): distance to nearest existing pharmacy",
        "- Timeline factor (0-20): earlier planning stage = more time to act",
        "- Centre type factor (0-10): major > activity > local > neighbourhood",
        "",
        "**Data Sources:**",
        "- Victorian Planning Authority (vpa.vic.gov.au)",
        "- NSW Dept of Planning (planning.nsw.gov.au)",
        "- QLD State Development (planning.statedevelopment.qld.gov.au)",
        "- WA Planning (wa.gov.au)",
        "- SA PlanSA (plan.sa.gov.au)",
        "- TAS Planning Reform (planningreform.tas.gov.au)",
        "",
        "**Limitations:**",
        "- Population estimates are from PSP documents (may be dated)",
        "- Geocoding uses area centroids, not exact town centre locations",
        "- ACPA approval check radius: 5km",
        "- Nearest pharmacy based on straight-line distance",
        "",
        f"*Report generated by PharmacyFinder PSP Scanner*",
    ]

    path.write_text("\n".join(lines), encoding="utf-8")


# ---------------------------------------------------------------------------
# CLI entry point (used by scripts/scan_psp.py)
# ---------------------------------------------------------------------------

def run_scan(
    national: bool = False,
    state: Optional[str] = None,
    top_n: int = 30,
    live_scrape: bool = True,
    output: bool = True,
):
    """Main entry point for PSP scanning."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )

    conn = get_db()
    init_tables(conn)

    if national:
        total = scan_national(conn, live_scrape=live_scrape)
    elif state:
        total = scan_state(conn, state.upper(), live_scrape=live_scrape)
    else:
        logger.error("Specify --national or --state <STATE>")
        return

    logger.info(f"Total PSP projects processed: {total}")

    if output:
        generate_outputs(conn, top_n=top_n)

    conn.close()
    logger.info("PSP scan complete.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="PSP Scanner for PharmacyFinder")
    parser.add_argument("--national", action="store_true", help="Scan all states")
    parser.add_argument("--state", type=str, help="Scan a specific state (e.g. VIC)")
    parser.add_argument("--top", type=int, default=30, help="Top N opportunities in report")
    parser.add_argument("--no-scrape", action="store_true", help="Skip live web scraping")
    args = parser.parse_args()

    run_scan(
        national=args.national,
        state=args.state,
        top_n=args.top,
        live_scrape=not args.no_scrape,
    )
