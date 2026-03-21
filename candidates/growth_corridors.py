#!/usr/bin/env python3
"""
Growth Corridor Scanner — finds the NEXT Beveridge.

Identifies fast-growing suburbs with no pharmacy that are about to get
retail infrastructure. Combines ABS population data, pharmacy gap analysis,
retail pipeline tracking, and government PSP data into a scored ranking.

Tables created in pharmacy_finder.db:
  - growth_corridors: SA2-level growth data + PSP info
  - planned_retail: upcoming shopping centres / commercial DAs

Usage via scripts/scan_growth_corridors.py CLI.
"""

import csv
import json
import logging
import math
import os
import re
import sqlite3
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "pharmacy_finder.db"
OUTPUT_DIR = PROJECT_ROOT / "output"
CACHE_DIR = PROJECT_ROOT / "cache"

ABS_STAT_API = "https://api.data.abs.gov.au"
PLANNING_ALERTS_API = "https://api.planningalerts.org.au"

NATIONAL_AVG_PEOPLE_PER_PHARMACY = 4000
UNDERSERVED_THRESHOLD = 8000  # projected people per pharmacy

# Growth thresholds
MIN_GROWTH_RATE_3YR = 0.20  # 20% in 3 years
MIN_POPULATION = 500  # ignore tiny SA2s

# Scoring weights
SCORE_WEIGHTS = {
    "growth_rate": 30,
    "people_per_pharmacy": 25,
    "distance_nearest_pharmacy": 20,
    "retail_pipeline": 15,
    "psp_approved": 10,
}

CLASSIFICATION = {
    "HOT": 75,
    "WARM": 50,
    "WATCH": 25,
}

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

REQUEST_DELAY = 2.0  # seconds between web requests

AUSTRALIAN_STATES = ["NSW", "VIC", "QLD", "WA", "SA", "TAS", "NT", "ACT"]

# ABS state codes mapping
ABS_STATE_CODES = {
    "1": "NSW", "2": "VIC", "3": "QLD", "4": "SA",
    "5": "WA", "6": "TAS", "7": "NT", "8": "ACT",
    "9": "OT",
}


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class GrowthCorridor:
    sa2_code: str
    sa2_name: str
    state: str
    lat: float
    lon: float
    population_2021: int = 0
    population_current: int = 0
    population_projected: int = 0
    growth_rate_3yr: float = 0.0
    growth_rate_annual: float = 0.0
    psp_name: str = ""
    psp_url: str = ""
    planned_dwellings: int = 0
    planned_town_centres: int = 0
    pharmacies_5km: int = 0
    pharmacies_10km: int = 0
    nearest_pharmacy_km: float = 999.0
    people_per_pharmacy_current: float = 0.0
    people_per_pharmacy_projected: float = 0.0
    planned_retail_count: int = 0
    has_planned_retail: bool = False
    growth_score: float = 0.0
    classification: str = "WATCH"
    score_breakdown: dict = field(default_factory=dict)


@dataclass
class PlannedRetail:
    name: str
    developer: str = ""
    address: str = ""
    lat: float = 0.0
    lon: float = 0.0
    est_completion: str = ""
    gla_sqm: int = 0
    has_pharmacy_tenancy: bool = False
    source_url: str = ""
    sa2_code: str = ""
    sa2_name: str = ""
    state: str = ""


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def get_db(db_path: str = None) -> sqlite3.Connection:
    """Get database connection."""
    path = db_path or str(DB_PATH)
    conn = sqlite3.connect(path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init_tables(conn: sqlite3.Connection):
    """Create growth_corridors and planned_retail tables if they don't exist."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS growth_corridors (
            sa2_code TEXT PRIMARY KEY,
            sa2_name TEXT NOT NULL,
            state TEXT NOT NULL,
            lat REAL,
            lon REAL,
            population_2021 INTEGER DEFAULT 0,
            population_current INTEGER DEFAULT 0,
            population_projected INTEGER DEFAULT 0,
            growth_rate_3yr REAL DEFAULT 0.0,
            growth_rate_annual REAL DEFAULT 0.0,
            psp_name TEXT DEFAULT '',
            psp_url TEXT DEFAULT '',
            planned_dwellings INTEGER DEFAULT 0,
            planned_town_centres INTEGER DEFAULT 0,
            pharmacies_5km INTEGER DEFAULT 0,
            pharmacies_10km INTEGER DEFAULT 0,
            nearest_pharmacy_km REAL DEFAULT 999.0,
            people_per_pharmacy_current REAL DEFAULT 0.0,
            people_per_pharmacy_projected REAL DEFAULT 0.0,
            planned_retail_count INTEGER DEFAULT 0,
            has_planned_retail INTEGER DEFAULT 0,
            growth_score REAL DEFAULT 0.0,
            classification TEXT DEFAULT 'WATCH',
            score_breakdown TEXT DEFAULT '{}',
            last_updated TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS planned_retail (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            developer TEXT DEFAULT '',
            address TEXT DEFAULT '',
            lat REAL DEFAULT 0.0,
            lon REAL DEFAULT 0.0,
            est_completion TEXT DEFAULT '',
            gla_sqm INTEGER DEFAULT 0,
            has_pharmacy_tenancy INTEGER DEFAULT 0,
            source_url TEXT DEFAULT '',
            sa2_code TEXT DEFAULT '',
            sa2_name TEXT DEFAULT '',
            state TEXT DEFAULT '',
            last_updated TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_gc_state ON growth_corridors(state);
        CREATE INDEX IF NOT EXISTS idx_gc_score ON growth_corridors(growth_score DESC);
        CREATE INDEX IF NOT EXISTS idx_gc_classification ON growth_corridors(classification);
        CREATE INDEX IF NOT EXISTS idx_pr_sa2 ON planned_retail(sa2_code);
        CREATE INDEX IF NOT EXISTS idx_pr_state ON planned_retail(state);
    """)
    conn.commit()


def save_corridor(conn: sqlite3.Connection, gc: GrowthCorridor):
    """Upsert a growth corridor record."""
    for attempt in range(5):
        try:
            conn.execute("""
                INSERT OR REPLACE INTO growth_corridors (
                    sa2_code, sa2_name, state, lat, lon,
                    population_2021, population_current, population_projected,
                    growth_rate_3yr, growth_rate_annual,
                    psp_name, psp_url, planned_dwellings, planned_town_centres,
                    pharmacies_5km, pharmacies_10km, nearest_pharmacy_km,
                    people_per_pharmacy_current, people_per_pharmacy_projected,
                    planned_retail_count, has_planned_retail,
                    growth_score, classification, score_breakdown,
                    last_updated
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                gc.sa2_code, gc.sa2_name, gc.state, gc.lat, gc.lon,
                gc.population_2021, gc.population_current, gc.population_projected,
                gc.growth_rate_3yr, gc.growth_rate_annual,
                gc.psp_name, gc.psp_url, gc.planned_dwellings, gc.planned_town_centres,
                gc.pharmacies_5km, gc.pharmacies_10km, gc.nearest_pharmacy_km,
                gc.people_per_pharmacy_current, gc.people_per_pharmacy_projected,
                gc.planned_retail_count, int(gc.has_planned_retail),
                gc.growth_score, gc.classification, json.dumps(gc.score_breakdown),
                datetime.now().isoformat(),
            ))
            conn.commit()
            return
        except sqlite3.OperationalError as e:
            if "locked" in str(e) and attempt < 4:
                time.sleep(2 * (attempt + 1))
            else:
                raise


def save_planned_retail(conn: sqlite3.Connection, pr: PlannedRetail):
    """Insert a planned retail record (dedup by name + address)."""
    for attempt in range(5):
        try:
            existing = conn.execute(
                "SELECT id FROM planned_retail WHERE name=? AND address=?",
                (pr.name, pr.address)
            ).fetchone()
            if existing:
                conn.execute("""
                    UPDATE planned_retail SET
                        developer=?, lat=?, lon=?, est_completion=?,
                        gla_sqm=?, has_pharmacy_tenancy=?, source_url=?,
                        sa2_code=?, sa2_name=?, state=?, last_updated=?
                    WHERE id=?
                """, (
                    pr.developer, pr.lat, pr.lon, pr.est_completion,
                    pr.gla_sqm, int(pr.has_pharmacy_tenancy), pr.source_url,
                    pr.sa2_code, pr.sa2_name, pr.state,
                    datetime.now().isoformat(), existing["id"],
                ))
            else:
                conn.execute("""
                    INSERT INTO planned_retail (
                        name, developer, address, lat, lon, est_completion,
                        gla_sqm, has_pharmacy_tenancy, source_url,
                        sa2_code, sa2_name, state, last_updated
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    pr.name, pr.developer, pr.address, pr.lat, pr.lon,
                    pr.est_completion, pr.gla_sqm, int(pr.has_pharmacy_tenancy),
                    pr.source_url, pr.sa2_code, pr.sa2_name, pr.state,
                    datetime.now().isoformat(),
                ))
            conn.commit()
            return
        except sqlite3.OperationalError as e:
            if "locked" in str(e) and attempt < 4:
                time.sleep(2 * (attempt + 1))
            else:
                raise


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

_session = None

def _get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        _session.headers.update({"User-Agent": USER_AGENT})
    return _session


def _fetch(url: str, params: dict = None, timeout: int = 30, skip_on_ratelimit: bool = False) -> requests.Response:
    """Fetch URL with rate limiting and retries."""
    session = _get_session()
    for attempt in range(3):
        try:
            resp = session.get(url, params=params, timeout=timeout)
            if resp.status_code == 429:
                if skip_on_ratelimit:
                    logger.debug(f"Rate limited on {url}, skipping")
                    return None
                wait = min(int(resp.headers.get("Retry-After", 10)), 15)
                logger.warning(f"Rate limited, waiting {wait}s")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            time.sleep(REQUEST_DELAY)
            return resp
        except requests.RequestException as e:
            logger.warning(f"Request failed (attempt {attempt+1}/3): {e}")
            if attempt < 2:
                time.sleep(5 * (attempt + 1))
    return None


# ---------------------------------------------------------------------------
# 1. POPULATION GROWTH DATA
# ---------------------------------------------------------------------------

def fetch_abs_erp_data(state_filter: str = None) -> dict:
    """
    Fetch ABS Estimated Resident Population by SA2 from the ABS Data API (SDMX).
    
    Uses the ERP_SA2 dataset — Regional Population by SA2.
    Returns dict of sa2_code -> {pop_2021, pop_latest, sa2_name}.
    """
    logger.info("Fetching ABS ERP data by SA2...")
    
    # ABS Data API uses SDMX-JSON format
    # Dataset: ERP_SA2 (Estimated Resident Population by SA2)
    # We'll fetch 2021 and latest available (2023 or 2024)
    
    erp_data = {}
    
    # Try the ABS.Stat SDMX REST API
    # Format: /data/{dataflow}/{key}?startPeriod=YYYY&endPeriod=YYYY
    base_url = f"{ABS_STAT_API}/data/ABS,ERP_SA2,1.0.0"
    
    # Try fetching with broad key first
    try:
        # Fetch 2021 data
        url_2021 = f"{base_url}/all?dimensionAtObservation=AllDimensions&startPeriod=2021&endPeriod=2021"
        resp = _fetch(url_2021, timeout=60)
        if resp and resp.status_code == 200:
            data = resp.json()
            erp_data = _parse_abs_sdmx(data, year="2021")
            logger.info(f"Fetched {len(erp_data)} SA2 records from ABS API (2021)")
        else:
            logger.warning("ABS API returned non-200 or no response for 2021")
    except Exception as e:
        logger.warning(f"ABS API fetch failed: {e}")
    
    # If ABS API didn't work, try alternative approach
    if not erp_data:
        logger.info("Trying ABS alternative API endpoint...")
        try:
            # Try the data.gov.au / ABS beta API
            alt_url = "https://api.data.abs.gov.au/data/ERP_Q,all?startPeriod=2021&endPeriod=2024&dimensionAtObservation=AllDimensions"
            resp = _fetch(alt_url, timeout=60)
            if resp:
                data = resp.json()
                erp_data = _parse_abs_sdmx(data, year="2021")
        except Exception as e:
            logger.warning(f"Alternative ABS API also failed: {e}")
    
    # Fallback: use population_grid from our own DB
    if not erp_data:
        logger.info("Using local population_grid as fallback for population data")
        erp_data = _load_population_from_db(state_filter)
    
    return erp_data


def _parse_abs_sdmx(data: dict, year: str = "2021") -> dict:
    """Parse ABS SDMX-JSON response into sa2_code -> population dict."""
    result = {}
    try:
        # SDMX-JSON structure varies; try common patterns
        if "dataSets" in data:
            datasets = data["dataSets"]
            structure = data.get("structure", {})
            dimensions = structure.get("dimensions", {})
            # Parse dimension values to find SA2 codes
            obs = dimensions.get("observation", [])
            for dim in obs:
                if "SA2" in dim.get("id", "").upper():
                    for idx, val in enumerate(dim.get("values", [])):
                        code = val.get("id", "")
                        name = val.get("name", "")
                        if code and len(code) == 9:  # SA2 codes are 9 digits
                            result[code] = {"sa2_name": name, "pop_2021": 0, "pop_latest": 0}
    except Exception as e:
        logger.warning(f"Failed to parse ABS SDMX data: {e}")
    return result


def _load_population_from_db(state_filter: str = None) -> dict:
    """Load population data from local population_grid table as baseline."""
    conn = get_db()
    try:
        query = "SELECT sa2_code, sa2_name, population, lat, lon, state_code FROM population_grid"
        params = []
        if state_filter:
            # state_code in population_grid is numeric (1=NSW, 2=VIC, etc.)
            state_num = {v: k for k, v in ABS_STATE_CODES.items()}.get(state_filter)
            if state_num:
                query += " WHERE state_code = ?"
                params.append(state_num)
        
        rows = conn.execute(query, params).fetchall()
        result = {}
        for row in rows:
            code = row["sa2_code"]
            result[code] = {
                "sa2_name": row["sa2_name"],
                "pop_2021": row["population"],  # Census 2021 data
                "pop_latest": row["population"],  # Will be updated with ERP
                "lat": row["lat"],
                "lon": row["lon"],
                "state_code": row["state_code"],
            }
        logger.info(f"Loaded {len(result)} SA2 records from local DB")
        return result
    finally:
        conn.close()


def estimate_growth_from_known_corridors(erp_data: dict) -> dict:
    """
    Apply known growth rates to SA2s in major growth corridors.
    
    Matches by SA2 name (case-insensitive, partial match) since SA2 codes
    change between census editions.
    """
    # Known high-growth areas with approximate annual growth rates
    # Source: VPA, NSW DPE, QLD DSDILGP planning documents
    # Keyed by name-matching patterns (list of possible SA2 name substrings)
    KNOWN_HIGH_GROWTH = [
        # VIC — Melbourne growth corridors
        {"names": ["Beveridge"], "state": "VIC", "annual_growth": 0.15, "psp": "Beveridge Central PSP", "dwellings": 30000, "fallback_lat": -37.4761, "fallback_lon": 144.9978},
        {"names": ["Mickleham", "Yuroke"], "state": "VIC", "annual_growth": 0.12, "psp": "Mickleham PSP", "dwellings": 15000, "fallback_lat": -37.53, "fallback_lon": 144.92},
        {"names": ["Donnybrook", "Woodstock"], "state": "VIC", "annual_growth": 0.14, "psp": "Donnybrook-Woodstock PSP", "dwellings": 20000, "fallback_lat": -37.51, "fallback_lon": 145.02},
        {"names": ["Wollert"], "state": "VIC", "annual_growth": 0.18, "psp": "Wollert PSP", "dwellings": 17000, "fallback_lat": -37.59, "fallback_lon": 145.01},
        {"names": ["Craigieburn - South", "Craigieburn - Central"], "state": "VIC", "annual_growth": 0.08, "psp": "Craigieburn PSP", "dwellings": 8000, "fallback_lat": -37.61, "fallback_lon": 144.92},
        {"names": ["Sunbury - South", "Sunbury South"], "state": "VIC", "annual_growth": 0.06, "psp": "Sunbury South PSP", "dwellings": 11000, "fallback_lat": -37.61, "fallback_lon": 144.72},
        {"names": ["Rockbank", "Mount Cottrell"], "state": "VIC", "annual_growth": 0.20, "psp": "Rockbank PSP", "dwellings": 25000, "fallback_lat": -37.75, "fallback_lon": 144.67},
        {"names": ["Aintree", "Cobblebank"], "state": "VIC", "annual_growth": 0.25, "psp": "Plumpton-Kororoit PSP", "dwellings": 22000, "fallback_lat": -37.75, "fallback_lon": 144.66},
        {"names": ["Tarneit"], "state": "VIC", "annual_growth": 0.10, "psp": "Tarneit PSP", "dwellings": 15000, "fallback_lat": -37.83, "fallback_lon": 144.66},
        {"names": ["Truganina"], "state": "VIC", "annual_growth": 0.12, "psp": "Truganina PSP", "dwellings": 12000, "fallback_lat": -37.82, "fallback_lon": 144.72},
        {"names": ["Clyde North"], "state": "VIC", "annual_growth": 0.18, "psp": "Clyde Creek PSP", "dwellings": 25000, "fallback_lat": -38.10, "fallback_lon": 145.36},
        {"names": ["Clyde"], "state": "VIC", "annual_growth": 0.20, "psp": "Clyde PSP", "dwellings": 30000, "fallback_lat": -38.13, "fallback_lon": 145.35},
        {"names": ["Officer"], "state": "VIC", "annual_growth": 0.12, "psp": "Officer PSP", "dwellings": 10000, "fallback_lat": -38.06, "fallback_lon": 145.41},
        {"names": ["Pakenham"], "state": "VIC", "annual_growth": 0.08, "psp": "Pakenham East PSP", "dwellings": 8000, "fallback_lat": -38.07, "fallback_lon": 145.49},
        {"names": ["Armstrong Creek"], "state": "VIC", "annual_growth": 0.15, "psp": "Armstrong Creek PSP", "dwellings": 22000, "fallback_lat": -38.22, "fallback_lon": 144.36},
        {"names": ["Charlemont"], "state": "VIC", "annual_growth": 0.12, "psp": "Armstrong Creek PSP", "dwellings": 8000, "fallback_lat": -38.20, "fallback_lon": 144.35},
        {"names": ["Mambourin"], "state": "VIC", "annual_growth": 0.30, "psp": "Mambourin PSP", "dwellings": 20000, "fallback_lat": -37.86, "fallback_lon": 144.57},
        {"names": ["Fraser Rise"], "state": "VIC", "annual_growth": 0.15, "psp": "Plumpton PSP", "dwellings": 15000, "fallback_lat": -37.69, "fallback_lon": 144.71},
        {"names": ["Manor Lakes"], "state": "VIC", "annual_growth": 0.10, "psp": "Wyndham Vale PSP", "dwellings": 10000, "fallback_lat": -37.87, "fallback_lon": 144.59},
        
        # NSW — Sydney growth corridors
        {"names": ["Box Hill"], "state": "NSW", "annual_growth": 0.20, "psp": "Box Hill PSP", "dwellings": 18000, "fallback_lat": -33.65, "fallback_lon": 150.89},
        {"names": ["Marsden Park"], "state": "NSW", "annual_growth": 0.15, "psp": "Marsden Park North", "dwellings": 10000, "fallback_lat": -33.69, "fallback_lon": 150.83},
        {"names": ["Oran Park"], "state": "NSW", "annual_growth": 0.18, "psp": "Oran Park Precinct", "dwellings": 12000, "fallback_lat": -34.00, "fallback_lon": 150.74},
        {"names": ["Leppington"], "state": "NSW", "annual_growth": 0.12, "psp": "Leppington Precinct", "dwellings": 15000, "fallback_lat": -33.97, "fallback_lon": 150.81},
        {"names": ["Austral"], "state": "NSW", "annual_growth": 0.15, "psp": "Austral-Leppington North", "dwellings": 17000, "fallback_lat": -33.93, "fallback_lon": 150.81},
        {"names": ["Schofields"], "state": "NSW", "annual_growth": 0.10, "psp": "Alex Avenue Precinct", "dwellings": 8000, "fallback_lat": -33.70, "fallback_lon": 150.87},
        {"names": ["Gregory Hills"], "state": "NSW", "annual_growth": 0.10, "psp": "Turner Road Precinct", "dwellings": 7000, "fallback_lat": -34.03, "fallback_lon": 150.77},
        {"names": ["Catherine Field"], "state": "NSW", "annual_growth": 0.12, "psp": "Catherine Field (Part)", "dwellings": 8000, "fallback_lat": -33.98, "fallback_lon": 150.77},
        {"names": ["Wilton"], "state": "NSW", "annual_growth": 0.15, "psp": "Wilton Growth Area", "dwellings": 15000, "fallback_lat": -34.24, "fallback_lon": 150.69},
        {"names": ["Appin"], "state": "NSW", "annual_growth": 0.10, "psp": "Appin Precinct", "dwellings": 12000, "fallback_lat": -34.20, "fallback_lon": 150.79},
        {"names": ["Edmondson Park"], "state": "NSW", "annual_growth": 0.12, "psp": "Edmondson Park", "dwellings": 8000, "fallback_lat": -33.96, "fallback_lon": 150.86},
        {"names": ["Jordan Springs"], "state": "NSW", "annual_growth": 0.10, "psp": "Jordan Springs", "dwellings": 7000, "fallback_lat": -33.73, "fallback_lon": 150.72},
        
        # QLD — SEQ growth corridors
        {"names": ["Ripley"], "state": "QLD", "annual_growth": 0.20, "psp": "Ripley Valley PDA", "dwellings": 50000, "fallback_lat": -27.69, "fallback_lon": 152.81},
        {"names": ["Yarrabilba"], "state": "QLD", "annual_growth": 0.18, "psp": "Yarrabilba PDA", "dwellings": 45000, "fallback_lat": -27.82, "fallback_lon": 153.09},
        {"names": ["Caloundra West", "Caloundra South"], "state": "QLD", "annual_growth": 0.12, "psp": "Caloundra South PDA", "dwellings": 20000, "fallback_lat": -26.83, "fallback_lon": 153.07},
        {"names": ["Caboolture West"], "state": "QLD", "annual_growth": 0.10, "psp": "Caboolture West PDA", "dwellings": 30000, "fallback_lat": -27.06, "fallback_lon": 152.88},
        {"names": ["Springfield"], "state": "QLD", "annual_growth": 0.08, "psp": "Greater Springfield", "dwellings": 12000, "fallback_lat": -27.67, "fallback_lon": 152.91},
        {"names": ["Park Ridge"], "state": "QLD", "annual_growth": 0.12, "psp": "Park Ridge PDA", "dwellings": 15000, "fallback_lat": -27.72, "fallback_lon": 153.04},
        {"names": ["Palmview"], "state": "QLD", "annual_growth": 0.15, "psp": "Palmview", "dwellings": 16000, "fallback_lat": -26.73, "fallback_lon": 153.06},
        {"names": ["Pimpama"], "state": "QLD", "annual_growth": 0.12, "psp": "Pimpama PDA", "dwellings": 10000, "fallback_lat": -27.82, "fallback_lon": 153.30},
        {"names": ["Coomera"], "state": "QLD", "annual_growth": 0.10, "psp": "Coomera Town Centre", "dwellings": 8000, "fallback_lat": -27.87, "fallback_lon": 153.30},
        
        # WA — Perth growth corridors
        {"names": ["Baldivis"], "state": "WA", "annual_growth": 0.08, "psp": "Baldivis District Structure Plan", "dwellings": 15000, "fallback_lat": -32.33, "fallback_lon": 115.78},
        {"names": ["Ellenbrook"], "state": "WA", "annual_growth": 0.06, "psp": "Ellenbrook Development", "dwellings": 10000, "fallback_lat": -31.77, "fallback_lon": 116.01},
        {"names": ["Yanchep"], "state": "WA", "annual_growth": 0.15, "psp": "Yanchep-Two Rocks", "dwellings": 55000, "fallback_lat": -31.55, "fallback_lon": 115.63},
        {"names": ["Two Rocks"], "state": "WA", "annual_growth": 0.10, "psp": "Yanchep-Two Rocks", "dwellings": 15000, "fallback_lat": -31.50, "fallback_lon": 115.59},
        {"names": ["Alkimos", "Eglinton"], "state": "WA", "annual_growth": 0.12, "psp": "Alkimos-Eglinton DSP", "dwellings": 30000, "fallback_lat": -31.63, "fallback_lon": 115.66},
        {"names": ["Byford"], "state": "WA", "annual_growth": 0.10, "psp": "Byford District Structure Plan", "dwellings": 12000, "fallback_lat": -32.22, "fallback_lon": 116.01},
        {"names": ["Mundijong"], "state": "WA", "annual_growth": 0.12, "psp": "Mundijong-Whitby DSP", "dwellings": 18000, "fallback_lat": -32.29, "fallback_lon": 115.88},
        {"names": ["Brabham", "Henley Brook"], "state": "WA", "annual_growth": 0.15, "psp": "Brabham", "dwellings": 8000, "fallback_lat": -31.83, "fallback_lon": 115.98},
        
        # SA — Adelaide growth corridors
        {"names": ["Seaford Meadows", "Aldinga"], "state": "SA", "annual_growth": 0.06, "psp": "Aldinga Township", "dwellings": 5000, "fallback_lat": -35.19, "fallback_lon": 138.49},
        {"names": ["Mount Barker"], "state": "SA", "annual_growth": 0.08, "psp": "Mt Barker Urban Growth", "dwellings": 12000, "fallback_lat": -35.07, "fallback_lon": 138.86},
        {"names": ["Angle Vale", "Virginia"], "state": "SA", "annual_growth": 0.10, "psp": "Angle Vale", "dwellings": 8000, "fallback_lat": -34.65, "fallback_lon": 138.65},
        {"names": ["Munno Para", "Smithfield"], "state": "SA", "annual_growth": 0.06, "psp": "Playford Growth Area", "dwellings": 6000, "fallback_lat": -34.67, "fallback_lon": 138.68},
        {"names": ["Two Wells"], "state": "SA", "annual_growth": 0.12, "psp": "Two Wells Township", "dwellings": 10000, "fallback_lat": -34.59, "fallback_lon": 138.51},
    ]
    
    # Build name-to-code lookup from existing erp_data
    name_to_codes = {}
    for code, data in erp_data.items():
        name = data.get("sa2_name", "").lower()
        if name:
            name_to_codes.setdefault(name, []).append(code)
    
    for info in KNOWN_HIGH_GROWTH:
        matched = False
        for search_name in info["names"]:
            search_lower = search_name.lower()
            # Try exact match first, then partial
            for name, codes in name_to_codes.items():
                if search_lower in name or name.startswith(search_lower):
                    for code in codes:
                        entry = erp_data[code]
                        # Check state matches (state_code is numeric in DB)
                        entry_state = ABS_STATE_CODES.get(str(entry.get("state_code", "")), "")
                        if not entry_state:
                            entry_state = ABS_STATE_CODES.get(code[0] if code else "", "")
                        if entry_state and entry_state != info["state"]:
                            continue
                        
                        known_3yr = (1 + info["annual_growth"]) ** 3 - 1
                        if known_3yr > entry.get("growth_rate_3yr", 0):
                            base_pop = entry.get("pop_2021", entry.get("pop_latest", 0))
                            if base_pop > 0:
                                entry["growth_rate_3yr"] = known_3yr
                                entry["pop_latest"] = int(base_pop * (1 + known_3yr))
                                entry["annual_growth"] = info["annual_growth"]
                        entry["psp_name"] = info.get("psp", "")
                        entry["planned_dwellings"] = info.get("dwellings", 0)
                        matched = True
            if matched:
                break
        
        if not matched:
            # Add as new entry with fallback coordinates
            fake_code = f"GC_{info['names'][0].replace(' ', '_')}"
            erp_data[fake_code] = {
                "sa2_name": info["names"][0],
                "pop_2021": 0,
                "pop_latest": 0,
                "growth_rate_3yr": (1 + info["annual_growth"]) ** 3 - 1,
                "annual_growth": info["annual_growth"],
                "psp_name": info.get("psp", ""),
                "planned_dwellings": info.get("dwellings", 0),
                "lat": info.get("fallback_lat", 0.0),
                "lon": info.get("fallback_lon", 0.0),
                "state_code": {v: k for k, v in ABS_STATE_CODES.items()}.get(info["state"], ""),
            }
    
    return erp_data


def scrape_vpa_psps() -> list[dict]:
    """
    Scrape Victorian Planning Authority for Precinct Structure Plans.
    Returns list of {name, url, status, planned_dwellings}.
    """
    logger.info("Scraping VPA PSP projects...")
    psps = []
    
    try:
        resp = _fetch("https://vpa.vic.gov.au/project/")
        if not resp:
            logger.warning("Failed to fetch VPA projects page")
            return psps
        
        soup = BeautifulSoup(resp.text, "html.parser")
        
        # Look for project links/cards
        project_links = soup.find_all("a", href=re.compile(r"/project/"))
        seen = set()
        
        for link in project_links:
            href = link.get("href", "")
            name = link.get_text(strip=True)
            
            if not name or len(name) < 3 or href in seen:
                continue
            seen.add(href)
            
            # Normalize URL
            if not href.startswith("http"):
                href = f"https://vpa.vic.gov.au{href}"
            
            psps.append({
                "name": name,
                "url": href,
                "state": "VIC",
                "source": "VPA",
            })
        
        logger.info(f"Found {len(psps)} VPA PSP projects")
    except Exception as e:
        logger.warning(f"VPA scraping failed: {e}")
    
    return psps


def scrape_nsw_growth_areas() -> list[dict]:
    """
    Scrape NSW Department of Planning growth areas.
    Returns list of growth area dicts.
    """
    logger.info("Scraping NSW growth areas...")
    areas = []
    
    try:
        resp = _fetch("https://www.planning.nsw.gov.au/plans-for-your-area/priority-growth-areas-and-precincts")
        if not resp:
            logger.warning("Failed to fetch NSW planning page")
            return areas
        
        soup = BeautifulSoup(resp.text, "html.parser")
        
        links = soup.find_all("a", href=re.compile(r"(growth|precinct)", re.I))
        seen = set()
        
        for link in links:
            href = link.get("href", "")
            name = link.get_text(strip=True)
            
            if not name or len(name) < 3 or name in seen:
                continue
            seen.add(name)
            
            if not href.startswith("http"):
                href = f"https://www.planning.nsw.gov.au{href}"
            
            areas.append({
                "name": name,
                "url": href,
                "state": "NSW",
                "source": "NSW DPE",
            })
        
        logger.info(f"Found {len(areas)} NSW growth areas")
    except Exception as e:
        logger.warning(f"NSW growth area scraping failed: {e}")
    
    return areas


def scrape_state_growth_corridors(state_filter: str = None) -> list[dict]:
    """
    Aggregate growth corridor data from state planning authorities.
    """
    all_corridors = []
    
    states_to_scan = [state_filter] if state_filter else ["VIC", "NSW", "QLD", "WA", "SA"]
    
    for state in states_to_scan:
        if state == "VIC":
            all_corridors.extend(scrape_vpa_psps())
        elif state == "NSW":
            all_corridors.extend(scrape_nsw_growth_areas())
        # QLD, WA, SA — use curated data from KNOWN_HIGH_GROWTH
        # Their planning authority websites are harder to scrape programmatically
    
    return all_corridors


# ---------------------------------------------------------------------------
# 2. PHARMACY GAP ANALYSIS
# ---------------------------------------------------------------------------

def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate distance between two points in km using haversine formula."""
    R = 6371.0
    lat1_r, lon1_r = math.radians(lat1), math.radians(lon1)
    lat2_r, lon2_r = math.radians(lat2), math.radians(lon2)
    
    dlat = lat2_r - lat1_r
    dlon = lon2_r - lon1_r
    
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1_r) * math.cos(lat2_r) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    return R * c


def load_pharmacies(conn: sqlite3.Connection) -> list[dict]:
    """Load all pharmacies with coordinates."""
    rows = conn.execute(
        "SELECT name, latitude, longitude, suburb, state FROM pharmacies WHERE latitude IS NOT NULL AND longitude IS NOT NULL"
    ).fetchall()
    return [dict(r) for r in rows]


def pharmacy_gap_analysis(gc: GrowthCorridor, pharmacies: list[dict]) -> GrowthCorridor:
    """
    Count pharmacies within 5km and 10km, find nearest, calculate ratios.
    """
    if gc.lat == 0 or gc.lon == 0:
        return gc
    
    nearest = 999.0
    count_5km = 0
    count_10km = 0
    
    for ph in pharmacies:
        dist = haversine_km(gc.lat, gc.lon, ph["latitude"], ph["longitude"])
        if dist < nearest:
            nearest = dist
        if dist <= 5.0:
            count_5km += 1
        if dist <= 10.0:
            count_10km += 1
    
    gc.pharmacies_5km = count_5km
    gc.pharmacies_10km = count_10km
    gc.nearest_pharmacy_km = round(nearest, 2)
    
    # People per pharmacy ratios
    if count_10km > 0:
        gc.people_per_pharmacy_current = round(gc.population_current / count_10km, 0)
        gc.people_per_pharmacy_projected = round(gc.population_projected / count_10km, 0)
    else:
        # No pharmacy within 10km — infinite ratio, cap at 99999
        gc.people_per_pharmacy_current = 99999
        gc.people_per_pharmacy_projected = 99999
    
    return gc


# ---------------------------------------------------------------------------
# 3. RETAIL PIPELINE
# ---------------------------------------------------------------------------

def check_planning_alerts(lat: float, lon: float, radius_m: int = 5000) -> list[dict]:
    """
    Check PlanningAlerts API for commercial development applications near a point.
    """
    api_key = os.environ.get("PLANNING_ALERTS_KEY", "")
    if not api_key:
        key_file = Path.home() / ".config" / "planningalerts" / "api_key"
        if key_file.exists():
            api_key = key_file.read_text().strip()
    
    if not api_key:
        logger.debug("No PlanningAlerts API key found, skipping")
        return []
    
    results = []
    try:
        params = {
            "key": api_key,
            "lat": lat,
            "lng": lon,
            "radius": radius_m,
        }
        resp = _fetch(f"{PLANNING_ALERTS_API}/applications.json", params=params, skip_on_ratelimit=True)
        if resp and resp.status_code == 200:
            data = resp.json()
            for app in data:
                desc = app.get("description", "").lower()
                # Filter for commercial / retail / shopping centre DAs
                if any(kw in desc for kw in [
                    "shopping centre", "retail", "commercial",
                    "supermarket", "town centre", "mixed use",
                    "pharmacy", "medical centre"
                ]):
                    results.append({
                        "name": app.get("description", "")[:200],
                        "address": app.get("address", ""),
                        "lat": app.get("lat", 0),
                        "lon": app.get("lng", 0),
                        "source_url": app.get("info_url", ""),
                        "da_date": app.get("date_received", ""),
                    })
        logger.info(f"PlanningAlerts: found {len(results)} commercial DAs near ({lat}, {lon})")
    except Exception as e:
        logger.warning(f"PlanningAlerts check failed: {e}")
    
    return results


def scrape_developer_projects(state: str = None) -> list[PlannedRetail]:
    """
    Check major developer websites for planned retail / town centre projects.
    Returns known upcoming projects from curated + scraped sources.
    """
    # Curated list of known planned retail developments in growth corridors
    # Updated from developer announcements and planning documents
    KNOWN_PLANNED_RETAIL = [
        # VIC
        PlannedRetail(
            name="Beveridge Town Centre",
            developer="MAB Corporation / Yarra Valley Water",
            address="Beveridge, VIC",
            lat=-37.4761, lon=144.9978,
            est_completion="2026-06",
            gla_sqm=15000,
            has_pharmacy_tenancy=True,
            source_url="https://www.beveridgetowncentre.com.au",
            state="VIC",
        ),
        PlannedRetail(
            name="Aintree Town Centre (Woodlea)",
            developer="Mirvac / Victoria Police",
            address="Aintree, VIC",
            lat=-37.7483, lon=144.6575,
            est_completion="2027",
            gla_sqm=20000,
            has_pharmacy_tenancy=True,
            source_url="https://woodlea.com.au/town-centre",
            state="VIC",
        ),
        PlannedRetail(
            name="Cobblebank Town Centre",
            developer="Lendlease",
            address="Cobblebank, VIC",
            lat=-37.7200, lon=144.6400,
            est_completion="2027",
            gla_sqm=25000,
            has_pharmacy_tenancy=True,
            source_url="https://www.lendlease.com",
            state="VIC",
        ),
        PlannedRetail(
            name="Clyde Town Centre",
            developer="Stockland",
            address="Clyde, VIC",
            lat=-38.1300, lon=145.3500,
            est_completion="2026",
            gla_sqm=18000,
            has_pharmacy_tenancy=True,
            source_url="https://www.stockland.com.au",
            state="VIC",
        ),
        PlannedRetail(
            name="Mambourin Town Centre",
            developer="Frasers Property",
            address="Mambourin, VIC",
            lat=-37.8600, lon=144.5700,
            est_completion="2028",
            gla_sqm=15000,
            has_pharmacy_tenancy=False,
            source_url="https://www.frasersproperty.com.au",
            state="VIC",
        ),
        PlannedRetail(
            name="Manor Lakes Town Centre Stage 2",
            developer="Dennis Family",
            address="Manor Lakes, VIC",
            lat=-37.8700, lon=144.5900,
            est_completion="2026",
            gla_sqm=10000,
            has_pharmacy_tenancy=True,
            source_url="https://www.manorlakes.com.au",
            state="VIC",
        ),
        PlannedRetail(
            name="Donnybrook Town Centre",
            developer="Oreana Property Group",
            address="Donnybrook, VIC",
            lat=-37.5100, lon=145.0200,
            est_completion="2027",
            gla_sqm=12000,
            has_pharmacy_tenancy=False,
            source_url="https://oreana.com.au/projects",
            state="VIC",
        ),
        PlannedRetail(
            name="Armstrong Creek Town Centre (Villawood)",
            developer="Villawood Properties",
            address="Armstrong Creek, VIC",
            lat=-38.2200, lon=144.3600,
            est_completion="2026",
            gla_sqm=30000,
            has_pharmacy_tenancy=True,
            source_url="https://www.villawoodproperties.com.au",
            state="VIC",
        ),
        PlannedRetail(
            name="Rockbank Town Centre",
            developer="Stockland",
            address="Rockbank, VIC",
            lat=-37.7300, lon=144.6700,
            est_completion="2028",
            gla_sqm=15000,
            has_pharmacy_tenancy=False,
            source_url="https://www.stockland.com.au",
            state="VIC",
        ),
        PlannedRetail(
            name="Fraser Rise Town Centre",
            developer="Stockland",
            address="Fraser Rise, VIC",
            lat=-37.6900, lon=144.7100,
            est_completion="2027",
            gla_sqm=12000,
            has_pharmacy_tenancy=True,
            source_url="https://www.stockland.com.au",
            state="VIC",
        ),
        
        # NSW
        PlannedRetail(
            name="Wilton Town Centre",
            developer="Bradcorp / Walker Corporation",
            address="Wilton, NSW",
            lat=-34.2400, lon=150.6900,
            est_completion="2027",
            gla_sqm=20000,
            has_pharmacy_tenancy=True,
            source_url="https://www.planning.nsw.gov.au",
            state="NSW",
        ),
        PlannedRetail(
            name="Box Hill Town Centre",
            developer="Celestino",
            address="Box Hill, NSW",
            lat=-33.6500, lon=150.8900,
            est_completion="2028",
            gla_sqm=35000,
            has_pharmacy_tenancy=True,
            source_url="https://boxhilldevelopment.com.au",
            state="NSW",
        ),
        PlannedRetail(
            name="Leppington Town Centre",
            developer="Stockland",
            address="Leppington, NSW",
            lat=-33.9700, lon=150.8100,
            est_completion="2027",
            gla_sqm=25000,
            has_pharmacy_tenancy=True,
            source_url="https://www.stockland.com.au",
            state="NSW",
        ),
        PlannedRetail(
            name="Austral Town Centre",
            developer="Various",
            address="Austral, NSW",
            lat=-33.9300, lon=150.8100,
            est_completion="2027",
            gla_sqm=15000,
            has_pharmacy_tenancy=False,
            source_url="https://www.planning.nsw.gov.au",
            state="NSW",
        ),
        PlannedRetail(
            name="Appin Town Centre",
            developer="Walker Corporation",
            address="Appin, NSW",
            lat=-34.2000, lon=150.7900,
            est_completion="2029",
            gla_sqm=20000,
            has_pharmacy_tenancy=False,
            source_url="https://www.planning.nsw.gov.au",
            state="NSW",
        ),
        
        # QLD
        PlannedRetail(
            name="Ripley Town Centre",
            developer="Sekisui House / Oreana",
            address="Ripley, QLD",
            lat=-27.6900, lon=152.8100,
            est_completion="2027",
            gla_sqm=40000,
            has_pharmacy_tenancy=True,
            source_url="https://ripleyvalley.com.au",
            state="QLD",
        ),
        PlannedRetail(
            name="Yarrabilba Town Centre",
            developer="Lendlease",
            address="Yarrabilba, QLD",
            lat=-27.8200, lon=153.0900,
            est_completion="2026",
            gla_sqm=25000,
            has_pharmacy_tenancy=True,
            source_url="https://yarrabilba.com.au",
            state="QLD",
        ),
        PlannedRetail(
            name="Caloundra South Town Centre",
            developer="Stockland",
            address="Caloundra West, QLD",
            lat=-26.8300, lon=153.0700,
            est_completion="2027",
            gla_sqm=30000,
            has_pharmacy_tenancy=True,
            source_url="https://www.stockland.com.au",
            state="QLD",
        ),
        PlannedRetail(
            name="Palmview Town Centre",
            developer="Stockland",
            address="Palmview, QLD",
            lat=-26.7300, lon=153.0600,
            est_completion="2026",
            gla_sqm=18000,
            has_pharmacy_tenancy=True,
            source_url="https://www.stockland.com.au",
            state="QLD",
        ),
        PlannedRetail(
            name="Caboolture West Town Centre",
            developer="AVID Property Group",
            address="Caboolture West, QLD",
            lat=-27.0600, lon=152.8800,
            est_completion="2029",
            gla_sqm=20000,
            has_pharmacy_tenancy=False,
            source_url="https://www.planning.qld.gov.au",
            state="QLD",
        ),
        
        # WA
        PlannedRetail(
            name="Yanchep City Centre",
            developer="Lendlease",
            address="Yanchep, WA",
            lat=-31.5500, lon=115.6300,
            est_completion="2028",
            gla_sqm=50000,
            has_pharmacy_tenancy=True,
            source_url="https://www.lendlease.com",
            state="WA",
        ),
        PlannedRetail(
            name="Alkimos Town Centre",
            developer="Lendlease",
            address="Alkimos, WA",
            lat=-31.6300, lon=115.6600,
            est_completion="2027",
            gla_sqm=25000,
            has_pharmacy_tenancy=True,
            source_url="https://www.lendlease.com",
            state="WA",
        ),
        PlannedRetail(
            name="Mundijong Town Centre",
            developer="Peet / Stockland",
            address="Mundijong, WA",
            lat=-32.2900, lon=115.8800,
            est_completion="2028",
            gla_sqm=15000,
            has_pharmacy_tenancy=False,
            source_url="https://www.peet.com.au",
            state="WA",
        ),
        PlannedRetail(
            name="Brabham Town Centre",
            developer="Peet",
            address="Brabham, WA",
            lat=-31.8300, lon=115.9800,
            est_completion="2027",
            gla_sqm=12000,
            has_pharmacy_tenancy=True,
            source_url="https://www.peet.com.au",
            state="WA",
        ),
        
        # SA
        PlannedRetail(
            name="Two Wells Town Centre",
            developer="Lendlease",
            address="Two Wells, SA",
            lat=-34.5900, lon=138.5100,
            est_completion="2027",
            gla_sqm=15000,
            has_pharmacy_tenancy=True,
            source_url="https://www.lendlease.com",
            state="SA",
        ),
        PlannedRetail(
            name="Mount Barker Town Centre Expansion",
            developer="Various",
            address="Mount Barker, SA",
            lat=-35.0700, lon=138.8600,
            est_completion="2026",
            gla_sqm=10000,
            has_pharmacy_tenancy=True,
            source_url="https://plan.sa.gov.au",
            state="SA",
        ),
    ]
    
    if state:
        return [pr for pr in KNOWN_PLANNED_RETAIL if pr.state == state]
    return KNOWN_PLANNED_RETAIL


def find_retail_for_corridor(gc: GrowthCorridor, planned_retail: list[PlannedRetail]) -> list[PlannedRetail]:
    """Find planned retail developments within 10km of a growth corridor."""
    nearby = []
    for pr in planned_retail:
        if pr.lat == 0 or pr.lon == 0 or gc.lat == 0 or gc.lon == 0:
            # Match by state + name similarity
            if pr.state == gc.state and (
                gc.sa2_name.lower() in pr.address.lower() or
                gc.sa2_name.lower() in pr.name.lower()
            ):
                nearby.append(pr)
            continue
        
        dist = haversine_km(gc.lat, gc.lon, pr.lat, pr.lon)
        if dist <= 10.0:
            nearby.append(pr)
    
    return nearby


# ---------------------------------------------------------------------------
# 4. SCORING
# ---------------------------------------------------------------------------

def score_corridor(gc: GrowthCorridor) -> GrowthCorridor:
    """
    Score a growth corridor 0-100 based on weighted factors.
    """
    breakdown = {}
    
    # 1. Population growth rate (30pts)
    # 20% in 3yr = baseline (10pts), 50%+ = max (30pts)
    growth_pct = gc.growth_rate_3yr * 100
    if growth_pct >= 50:
        growth_pts = 30
    elif growth_pct >= 20:
        growth_pts = 10 + (growth_pct - 20) * (20 / 30)  # linear scale 10-30
    elif growth_pct >= 10:
        growth_pts = growth_pct / 2  # 5-10 pts
    else:
        growth_pts = max(0, growth_pct / 3)
    breakdown["growth_rate"] = round(min(30, growth_pts), 1)
    
    # 2. Projected people per pharmacy ratio (25pts)
    ratio = gc.people_per_pharmacy_projected
    if ratio >= 99999:  # No pharmacy within 10km
        ratio_pts = 25
    elif ratio >= UNDERSERVED_THRESHOLD * 2:  # 16000+
        ratio_pts = 25
    elif ratio >= UNDERSERVED_THRESHOLD:  # 8000+
        ratio_pts = 15 + (ratio - UNDERSERVED_THRESHOLD) / UNDERSERVED_THRESHOLD * 10
    elif ratio >= NATIONAL_AVG_PEOPLE_PER_PHARMACY:  # 4000-8000
        ratio_pts = 5 + (ratio - NATIONAL_AVG_PEOPLE_PER_PHARMACY) / NATIONAL_AVG_PEOPLE_PER_PHARMACY * 10
    else:
        ratio_pts = max(0, ratio / NATIONAL_AVG_PEOPLE_PER_PHARMACY * 5)
    breakdown["people_per_pharmacy"] = round(min(25, ratio_pts), 1)
    
    # 3. Distance to nearest pharmacy (20pts)
    dist = gc.nearest_pharmacy_km
    if dist >= 20:
        dist_pts = 20
    elif dist >= 10:
        dist_pts = 15 + (dist - 10) / 10 * 5
    elif dist >= 5:
        dist_pts = 10 + (dist - 5) / 5 * 5
    elif dist >= 2:
        dist_pts = 5 + (dist - 2) / 3 * 5
    else:
        dist_pts = max(0, dist / 2 * 5)
    breakdown["distance_nearest_pharmacy"] = round(min(20, dist_pts), 1)
    
    # 4. Planned retail infrastructure (15pts)
    if gc.has_planned_retail:
        retail_pts = 10
        if gc.planned_retail_count >= 2:
            retail_pts = 15
    else:
        retail_pts = 0
    breakdown["retail_pipeline"] = round(retail_pts, 1)
    
    # 5. Government PSP approved (10pts)
    if gc.psp_name:
        psp_pts = 10
    else:
        psp_pts = 0
    breakdown["psp_approved"] = round(psp_pts, 1)
    
    # Total
    total = sum(breakdown.values())
    gc.growth_score = round(total, 1)
    gc.score_breakdown = breakdown
    
    # Classification
    if total >= CLASSIFICATION["HOT"]:
        gc.classification = "HOT"
    elif total >= CLASSIFICATION["WARM"]:
        gc.classification = "WARM"
    elif total >= CLASSIFICATION["WATCH"]:
        gc.classification = "WATCH"
    else:
        gc.classification = "LOW"
    
    return gc


# ---------------------------------------------------------------------------
# 5. OUTPUT
# ---------------------------------------------------------------------------

def generate_json_output(corridors: list[GrowthCorridor], output_path: Path):
    """Write all scored corridors to JSON."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    data = []
    for gc in corridors:
        d = asdict(gc)
        data.append(d)
    
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)
    
    logger.info(f"JSON output: {output_path} ({len(data)} corridors)")


def generate_csv_output(corridors: list[GrowthCorridor], output_path: Path):
    """Write summary CSV."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    fields = [
        "classification", "growth_score", "sa2_name", "state",
        "population_current", "population_projected", "growth_rate_3yr",
        "pharmacies_5km", "pharmacies_10km", "nearest_pharmacy_km",
        "people_per_pharmacy_current", "people_per_pharmacy_projected",
        "psp_name", "planned_dwellings", "planned_retail_count",
        "has_planned_retail", "sa2_code",
    ]
    
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for gc in corridors:
            row = {k: getattr(gc, k) for k in fields}
            row["growth_rate_3yr"] = f"{gc.growth_rate_3yr:.1%}"
            writer.writerow(row)
    
    logger.info(f"CSV output: {output_path} ({len(corridors)} corridors)")


def generate_report(corridors: list[GrowthCorridor], output_path: Path, top_n: int = 20):
    """Generate executive Markdown report."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    hot = [c for c in corridors if c.classification == "HOT"]
    warm = [c for c in corridors if c.classification == "WARM"]
    watch = [c for c in corridors if c.classification == "WATCH"]
    
    lines = [
        f"# Growth Corridor Scanner Report",
        f"",
        f"**Generated:** {now}",
        f"**Total corridors analysed:** {len(corridors)}",
        f"**HOT:** {len(hot)} | **WARM:** {len(warm)} | **WATCH:** {len(watch)}",
        f"",
        f"---",
        f"",
        f"## Executive Summary",
        f"",
        f"This report identifies fast-growing Australian suburbs with insufficient",
        f"pharmacy coverage that are expected to receive retail infrastructure.",
        f"These represent greenfield pharmacy opportunities.",
        f"",
        f"**Scoring criteria:**",
        f"- Population growth rate (30pts)",
        f"- Projected people-per-pharmacy ratio (25pts)",
        f"- Distance to nearest pharmacy (20pts)",
        f"- Planned retail infrastructure (15pts)",
        f"- Government PSP approved (10pts)",
        f"",
        f"---",
        f"",
        f"## Top {min(top_n, len(corridors))} Opportunities",
        f"",
    ]
    
    top = corridors[:top_n]
    for i, gc in enumerate(top, 1):
        emoji = "🔥" if gc.classification == "HOT" else "🟡" if gc.classification == "WARM" else "👀"
        lines.extend([
            f"### {i}. {gc.sa2_name}, {gc.state} — {gc.growth_score}/100 {emoji} {gc.classification}",
            f"",
            f"| Metric | Value |",
            f"|--------|-------|",
            f"| Population (current) | {gc.population_current:,} |",
            f"| Population (projected) | {gc.population_projected:,} |",
            f"| Growth rate (3yr) | {gc.growth_rate_3yr:.1%} |",
            f"| Pharmacies within 5km | {gc.pharmacies_5km} |",
            f"| Pharmacies within 10km | {gc.pharmacies_10km} |",
            f"| Nearest pharmacy | {gc.nearest_pharmacy_km:.1f} km |",
            f"| People/pharmacy (current) | {gc.people_per_pharmacy_current:,.0f} |",
            f"| People/pharmacy (projected) | {gc.people_per_pharmacy_projected:,.0f} |",
            f"| PSP | {gc.psp_name or 'None'} |",
            f"| Planned dwellings | {gc.planned_dwellings:,} |",
            f"| Planned retail | {'Yes' if gc.has_planned_retail else 'No'} ({gc.planned_retail_count}) |",
            f"",
            f"**Score breakdown:** {gc.score_breakdown}",
            f"",
        ])
    
    # State breakdown
    lines.extend([
        f"---",
        f"",
        f"## State Breakdown",
        f"",
        f"| State | HOT | WARM | WATCH | Total |",
        f"|-------|-----|------|-------|-------|",
    ])
    
    for state in AUSTRALIAN_STATES:
        state_corridors = [c for c in corridors if c.state == state]
        if not state_corridors:
            continue
        h = len([c for c in state_corridors if c.classification == "HOT"])
        w = len([c for c in state_corridors if c.classification == "WARM"])
        wt = len([c for c in state_corridors if c.classification == "WATCH"])
        lines.append(f"| {state} | {h} | {w} | {wt} | {len(state_corridors)} |")
    
    lines.extend([
        f"",
        f"---",
        f"",
        f"## Methodology",
        f"",
        f"1. **Population data:** ABS Census 2021 baseline + ERP estimates + known growth rates",
        f"   from state planning authorities (VPA, NSW DPE, QLD DSDILGP, WAPC, PlanSA).",
        f"2. **Pharmacy gap:** Counted pharmacies within 5km and 10km of each SA2 centroid.",
        f"   Ratios calculated against national average of ~4,000 people per pharmacy.",
        f"3. **Retail pipeline:** Curated database of planned town centres from Stockland,",
        f"   Lendlease, Frasers, Oreana, and other major developers + PlanningAlerts DA data.",
        f"4. **Scoring:** Weighted composite score with classification thresholds.",
        f"",
        f"**Limitations:**",
        f"- Population projections use growth rates from planning documents, not actuarial forecasts",
        f"- Pharmacy counts are point-in-time from FindAPharmacy data",
        f"- Retail pipeline data relies on public announcements — some projects may be",
        f"  unannounced or have changed timelines",
        f"- SA2 centroids may not represent the actual development location within the SA2",
        f"",
    ])
    
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    
    logger.info(f"Report output: {output_path}")


# ---------------------------------------------------------------------------
# MAIN SCAN ORCHESTRATOR
# ---------------------------------------------------------------------------

def scan_growth_corridors(
    state_filter: str = None,
    top_n: int = 20,
    db_path: str = None,
) -> list[GrowthCorridor]:
    """
    Run the full growth corridor scan pipeline.
    
    1. Fetch/load population data
    2. Apply known growth corridor data
    3. Filter for high-growth SA2s
    4. Run pharmacy gap analysis
    5. Check retail pipeline
    6. Score and rank
    7. Save to DB and output files
    
    Returns sorted list of GrowthCorridor objects.
    """
    logger.info(f"Starting growth corridor scan (state={state_filter or 'ALL'})")
    
    conn = get_db(db_path)
    init_tables(conn)
    
    # Step 1: Load population data
    erp_data = fetch_abs_erp_data(state_filter)
    logger.info(f"Population data: {len(erp_data)} SA2 records")
    
    # Step 2: Enrich with known growth corridor data
    erp_data = estimate_growth_from_known_corridors(erp_data)
    
    # Step 3: Scrape state planning authority data
    state_psps = scrape_state_growth_corridors(state_filter)
    
    # Step 4: Calculate growth rates and filter
    corridors = []
    state_code_to_abbr = ABS_STATE_CODES
    
    for sa2_code, data in erp_data.items():
        pop_2021 = data.get("pop_2021", 0)
        pop_latest = data.get("pop_latest", pop_2021)
        
        # Determine state
        state_num = data.get("state_code", sa2_code[0] if sa2_code else "")
        state = state_code_to_abbr.get(str(state_num), "")
        if not state and sa2_code:
            state = state_code_to_abbr.get(sa2_code[0], "")
        
        # Apply state filter
        if state_filter and state != state_filter:
            continue
        
        # Calculate growth rate
        if pop_2021 and pop_2021 > 0 and pop_latest > pop_2021:
            growth_rate_3yr = (pop_latest - pop_2021) / pop_2021
        else:
            growth_rate_3yr = data.get("growth_rate_3yr", 0)
        
        annual_growth = data.get("annual_growth", 0)
        if not annual_growth and growth_rate_3yr > 0:
            annual_growth = (1 + growth_rate_3yr) ** (1/3) - 1
        
        # Projected population (10 years out)
        if annual_growth > 0:
            pop_projected = int(max(pop_latest, pop_2021) * (1 + annual_growth) ** 10)
        else:
            pop_projected = pop_latest
        
        # Filter: minimum population or known high-growth area
        has_psp = bool(data.get("psp_name"))
        if pop_2021 < MIN_POPULATION and pop_latest < MIN_POPULATION and not has_psp:
            continue
        
        # Filter: must show some growth OR be a known corridor
        if growth_rate_3yr < 0.05 and not has_psp:
            continue
        
        gc = GrowthCorridor(
            sa2_code=sa2_code,
            sa2_name=data.get("sa2_name", ""),
            state=state,
            lat=data.get("lat", 0.0),
            lon=data.get("lon", 0.0),
            population_2021=pop_2021,
            population_current=max(pop_latest, pop_2021),
            population_projected=pop_projected,
            growth_rate_3yr=growth_rate_3yr,
            growth_rate_annual=annual_growth,
            psp_name=data.get("psp_name", ""),
            psp_url=data.get("psp_url", ""),
            planned_dwellings=data.get("planned_dwellings", 0),
        )
        corridors.append(gc)
    
    logger.info(f"Filtered to {len(corridors)} growth corridors")
    
    # Step 5: Pharmacy gap analysis
    pharmacies = load_pharmacies(conn)
    logger.info(f"Loaded {len(pharmacies)} pharmacies for gap analysis")
    
    for gc in corridors:
        pharmacy_gap_analysis(gc, pharmacies)
    
    # Step 6: Retail pipeline
    planned_retail = scrape_developer_projects(state_filter)
    logger.info(f"Found {len(planned_retail)} planned retail developments")
    
    for gc in corridors:
        nearby_retail = find_retail_for_corridor(gc, planned_retail)
        gc.planned_retail_count = len(nearby_retail)
        gc.has_planned_retail = len(nearby_retail) > 0
        
        # Associate retail with SA2 and save
        for pr in nearby_retail:
            pr.sa2_code = gc.sa2_code
            pr.sa2_name = gc.sa2_name
            save_planned_retail(conn, pr)
    
    # Check PlanningAlerts for top candidates (rate-limit friendly)
    pa_checked = 0
    pa_failed = 0
    for gc in sorted(corridors, key=lambda x: x.growth_rate_3yr, reverse=True):
        if pa_checked >= 10 or pa_failed >= 2:  # Stop early on rate limits
            break
        if gc.lat and gc.lon:
            try:
                da_results = check_planning_alerts(gc.lat, gc.lon)
                if da_results:
                    gc.planned_retail_count += len(da_results)
                    gc.has_planned_retail = True
                    for da in da_results:
                        pr = PlannedRetail(
                            name=da.get("name", ""),
                            address=da.get("address", ""),
                            lat=da.get("lat", 0),
                            lon=da.get("lon", 0),
                            source_url=da.get("source_url", ""),
                            sa2_code=gc.sa2_code,
                            sa2_name=gc.sa2_name,
                            state=gc.state,
                        )
                        save_planned_retail(conn, pr)
                pa_checked += 1
            except Exception as e:
                logger.warning(f"PlanningAlerts check failed for {gc.sa2_name}: {e}")
                pa_failed += 1
    
    # Step 7: Score and rank
    for gc in corridors:
        score_corridor(gc)
    
    corridors.sort(key=lambda x: x.growth_score, reverse=True)
    
    # Save to DB
    for gc in corridors:
        save_corridor(conn, gc)
    
    # Generate outputs
    output_dir = OUTPUT_DIR
    generate_json_output(corridors, output_dir / "growth_corridors.json")
    generate_csv_output(corridors, output_dir / "growth_corridors.csv")
    generate_report(corridors, output_dir / "growth_corridor_report.md", top_n=top_n)
    
    conn.close()
    
    # Summary
    hot = len([c for c in corridors if c.classification == "HOT"])
    warm = len([c for c in corridors if c.classification == "WARM"])
    watch = len([c for c in corridors if c.classification == "WATCH"])
    logger.info(f"Scan complete: {len(corridors)} corridors — {hot} HOT, {warm} WARM, {watch} WATCH")
    
    return corridors
