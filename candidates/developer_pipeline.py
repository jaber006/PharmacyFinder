"""
developer_pipeline.py - Developer Pipeline Tracker

Monitors major Australian shopping centre developers for new projects
that could include pharmacy tenancies. Scrapes project pages, geocodes
locations, cross-references with existing pharmacy data, and scores
each project for pharmacy opportunity potential.

Developers tracked:
  Oreana, Stockland, Lendlease, Frasers Property, Vicinity Centres,
  MAB Corporation, Villawood Properties, Scentre Group/Westfield,
  QIC, Charter Hall

Uses requests + BeautifulSoup. Rate limited: 2s between requests.
"""

import sqlite3
import json
import csv
import time
import re
import logging
from datetime import datetime
from pathlib import Path
from math import radians, sin, cos, sqrt, atan2
from typing import Optional

import requests
from bs4 import BeautifulSoup

try:
    from geopy.geocoders import Nominatim
    from geopy.distance import geodesic
    HAS_GEOPY = True
except ImportError:
    HAS_GEOPY = False

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent.parent / "pharmacy_finder.db"
OUTPUT_DIR = Path(__file__).parent.parent / "output"
RATE_LIMIT_SECS = 2.0

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-AU,en;q=0.9",
}

# Anchor tenant keywords
ANCHOR_KEYWORDS = [
    "coles", "woolworths", "aldi", "iga", "costco",
    "big w", "kmart", "target", "bunnings",
]

PHARMACY_KEYWORDS = [
    "pharmacy", "chemist", "priceline", "amcal", "terry white",
    "chemist warehouse", "discount drug", "blooms", "national pharmacies",
    "wizard", "good price", "cincotta",
]

STATE_PATTERNS = {
    "VIC": [r"\bVIC\b", r"\bVictoria\b", r"\bMelbourne\b"],
    "NSW": [r"\bNSW\b", r"\bNew South Wales\b", r"\bSydney\b"],
    "QLD": [r"\bQLD\b", r"\bQueensland\b", r"\bBrisbane\b"],
    "WA": [r"\bWA\b", r"\bWestern Australia\b", r"\bPerth\b"],
    "SA": [r"\bSA\b", r"\bSouth Australia\b", r"\bAdelaide\b"],
    "TAS": [r"\bTAS\b", r"\bTasmania\b", r"\bHobart\b"],
    "NT": [r"\bNT\b", r"\bNorthern Territory\b", r"\bDarwin\b"],
    "ACT": [r"\bACT\b", r"\bCanberra\b"],
}

# ── Developer Scrapers ──────────────────────────────────────────────

DEVELOPERS = {}


def _register(name):
    """Decorator to register a developer scraper function."""
    def wrapper(fn):
        DEVELOPERS[name] = fn
        return fn
    return wrapper


def _get(url: str, session: requests.Session) -> Optional[BeautifulSoup]:
    """GET a URL with rate limiting and error handling."""
    time.sleep(RATE_LIMIT_SECS)
    try:
        resp = session.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        return BeautifulSoup(resp.text, "html.parser")
    except Exception as e:
        logger.warning(f"Failed to fetch {url}: {e}")
        return None


def _detect_state(text: str) -> str:
    """Try to detect Australian state from text."""
    if not text:
        return ""
    for state, patterns in STATE_PATTERNS.items():
        for pat in patterns:
            if re.search(pat, text, re.IGNORECASE):
                return state
    return ""


def _detect_status(text: str) -> str:
    """Detect project status from text."""
    if not text:
        return "unknown"
    t = text.lower()
    if any(w in t for w in ["leasing", "lease", "tenancies available", "now leasing"]):
        return "leasing"
    if any(w in t for w in ["under construction", "construction", "building"]):
        return "under_construction"
    if any(w in t for w in ["completed", "open", "established", "trading"]):
        return "completed"
    if any(w in t for w in ["planning", "planned", "proposed", "approved", "da approved"]):
        return "planning"
    return "unknown"


def _detect_anchors(text: str) -> list:
    """Detect anchor tenants from text."""
    if not text:
        return []
    t = text.lower()
    found = []
    for kw in ANCHOR_KEYWORDS:
        if kw in t:
            found.append(kw.title())
    return found


def _detect_pharmacy(text: str) -> str:
    """Detect if pharmacy tenancy mentioned."""
    if not text:
        return "unknown"
    t = text.lower()
    for kw in PHARMACY_KEYWORDS:
        if kw in t:
            return "yes"
    return "unknown"


def _extract_gla(text: str) -> Optional[float]:
    """Extract GLA in sqm from text."""
    if not text:
        return None
    patterns = [
        r"([\d,]+)\s*(?:sqm|sq\.?\s*m|square\s*met)",
        r"([\d,]+)\s*(?:m2|m²)",
        r"GLA[:\s]*([\d,]+)",
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            val = float(m.group(1).replace(",", ""))
            if val > 100:  # filter noise
                return val
    return None


def _make_project(developer: str, name: str, suburb: str = "", state: str = "",
                  status: str = "unknown", anchors: list = None, gla: float = None,
                  has_pharmacy: str = "unknown", url: str = "", address: str = "") -> dict:
    """Create a standardised project dict."""
    return {
        "developer": developer,
        "project_name": name,
        "address": address or suburb,
        "suburb": suburb,
        "state": state,
        "status": status,
        "anchor_tenants": ",".join(anchors or []),
        "total_gla": gla,
        "has_pharmacy": has_pharmacy,
        "source_url": url,
        "date_scraped": datetime.now().strftime("%Y-%m-%d"),
        "latitude": None,
        "longitude": None,
    }


# ── Individual Developer Scrapers ──────────────────────────────────

@_register("oreana")
def scrape_oreana(session: requests.Session) -> list:
    """Oreana Property Group — Melbourne growth corridor specialist."""
    projects = []
    base = "https://oreana.com.au"

    # Oreana's development page lists projects with links to /projects/slug/
    soup = _get(f"{base}/development/", session)
    if not soup:
        return projects

    seen_urls = set()
    # Find all project links (they embed URLs as text and as hrefs)
    for link in soup.find_all("a", href=True):
        href = link.get("href", "")
        if "/projects/" not in href:
            continue
        # Skip the main /projects/ or /development/ page itself
        slug = href.rstrip("/").split("/projects/")[-1] if "/projects/" in href else ""
        if not slug or slug in ("", "development"):
            continue
        full_url = href if href.startswith("http") else f"{base}{href}"
        if full_url in seen_urls:
            continue
        seen_urls.add(full_url)

    # Also scan for project URLs that appear as text content (some sites embed them)
    for text_node in soup.find_all(string=re.compile(r"oreana\.com\.au/projects/")):
        urls = re.findall(r"https?://oreana\.com\.au/projects/[\w-]+/?", str(text_node))
        for u in urls:
            if u not in seen_urls:
                seen_urls.add(u)

    # Now fetch each project page
    for url in seen_urls:
        detail = _get(url, session)
        if not detail:
            continue
        detail_text = detail.get_text(" ", strip=True)

        # Try to extract project name from h1/h2 or title
        name_tag = detail.find("h1") or detail.find("h2")
        name = name_tag.get_text(strip=True) if name_tag else url.rstrip("/").split("/")[-1].replace("-", " ").title()

        # Detect project type — skip early education, focus on retail/mixed use/convenience
        project_types = detail_text.lower()
        # Include retail, mixed use, convenience, commercial; skip pure residential/early education
        is_relevant = any(kw in project_types for kw in [
            "retail", "mixed use", "convenience", "commercial", "shopping", "town centre",
            "supermarket", "coles", "woolworths", "aldi", "pharmacy",
        ])

        projects.append(_make_project(
            developer="Oreana",
            name=name,
            suburb=name,
            state=_detect_state(detail_text) or "VIC",
            status=_detect_status(detail_text),
            anchors=_detect_anchors(detail_text),
            gla=_extract_gla(detail_text),
            has_pharmacy=_detect_pharmacy(detail_text),
            url=url,
        ))

    logger.info(f"Oreana: found {len(projects)} projects")
    return projects


@_register("stockland")
def scrape_stockland(session: requests.Session) -> list:
    """Stockland — national, town centres."""
    projects = []
    base = "https://www.stockland.com.au"

    # Stockland has residential communities and shopping centres
    for path in ["/residential/communities", "/shopping-centres"]:
        soup = _get(f"{base}{path}", session)
        if not soup:
            continue

        text = soup.get_text(" ", strip=True)
        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            name = link.get_text(strip=True)
            if not name or len(name) < 3:
                continue
            # Filter to actual project/centre links
            if any(seg in href for seg in ["/communities/", "/shopping-centres/"]):
                if href.count("/") >= 3:  # Must be a specific item, not section
                    full_url = href if href.startswith("http") else f"{base}{href}"
                    projects.append(_make_project(
                        developer="Stockland",
                        name=name,
                        suburb=name,
                        state=_detect_state(name + " " + href),
                        status=_detect_status(name),
                        url=full_url,
                    ))

    logger.info(f"Stockland: found {len(projects)} projects")
    return projects


@_register("lendlease")
def scrape_lendlease(session: requests.Session) -> list:
    """Lendlease — national, major developments."""
    projects = []
    base = "https://www.lendlease.com"

    for path in ["/au/communities", "/au/projects"]:
        soup = _get(f"{base}{path}", session)
        if not soup:
            continue

        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            name = link.get_text(strip=True)
            if not name or len(name) < 3:
                continue
            if any(seg in href for seg in ["/communities/", "/projects/"]):
                if href.count("/") >= 3:
                    full_url = href if href.startswith("http") else f"{base}{href}"
                    projects.append(_make_project(
                        developer="Lendlease",
                        name=name,
                        suburb=name,
                        state=_detect_state(name + " " + href),
                        status=_detect_status(name),
                        url=full_url,
                    ))

    logger.info(f"Lendlease: found {len(projects)} projects")
    return projects


@_register("frasers")
def scrape_frasers(session: requests.Session) -> list:
    """Frasers Property — national."""
    projects = []
    base = "https://www.frasersproperty.com.au"

    for path in ["/nsw", "/vic", "/qld", "/wa", "/sa"]:
        soup = _get(f"{base}{path}", session)
        if not soup:
            continue
        state = path.strip("/").upper()

        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            name = link.get_text(strip=True)
            if not name or len(name) < 3:
                continue
            # Look for community/development links
            if any(seg in href.lower() for seg in ["/community/", "/communities/", "/development/"]):
                full_url = href if href.startswith("http") else f"{base}{href}"
                projects.append(_make_project(
                    developer="Frasers Property",
                    name=name,
                    suburb=name,
                    state=state if len(state) <= 3 else _detect_state(name),
                    url=full_url,
                ))

    logger.info(f"Frasers Property: found {len(projects)} projects")
    return projects


@_register("vicinity")
def scrape_vicinity(session: requests.Session) -> list:
    """Vicinity Centres — existing centre expansions."""
    projects = []
    base = "https://www.vicinity.com.au"

    # Vicinity lists their centres and development pipeline
    for path in ["/centres", "/development"]:
        soup = _get(f"{base}{path}", session)
        if not soup:
            continue

        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            name = link.get_text(strip=True)
            if not name or len(name) < 3:
                continue
            if any(seg in href for seg in ["/centres/", "/development/"]):
                full_url = href if href.startswith("http") else f"{base}{href}"
                projects.append(_make_project(
                    developer="Vicinity Centres",
                    name=name,
                    suburb=name,
                    state=_detect_state(name),
                    url=full_url,
                ))

    logger.info(f"Vicinity Centres: found {len(projects)} projects")
    return projects


@_register("mab")
def scrape_mab(session: requests.Session) -> list:
    """MAB Corporation — Melbourne."""
    projects = []
    base = "https://www.mab.com.au"

    # MAB has projects under /projects/communities/, /projects/commercial/, etc.
    for path in ["/projects/communities", "/projects/commercial", "/projects/retail"]:
        soup = _get(f"{base}{path}", session)
        if not soup:
            continue

        seen = set()
        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            name = link.get_text(strip=True)
            if not name or len(name) < 3:
                continue
            if "/projects/" in href and href.count("/") >= 4:
                full_url = href if href.startswith("http") else f"{base}{href}"
                if full_url in seen:
                    continue
                seen.add(full_url)
                projects.append(_make_project(
                    developer="MAB Corporation",
                    name=name,
                    suburb=name,
                    state="VIC",
                    url=full_url,
                ))

    # Also scrape the main page for featured projects
    soup = _get(base, session)
    if soup:
        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            name = link.get_text(strip=True)
            if "/projects/" in href and name and len(name) >= 3:
                full_url = href if href.startswith("http") else f"{base}{href}"
                # Deduplicate
                if not any(p["source_url"] == full_url for p in projects):
                    projects.append(_make_project(
                        developer="MAB Corporation",
                        name=name,
                        suburb=name,
                        state="VIC",
                        url=full_url,
                    ))

    logger.info(f"MAB Corporation: found {len(projects)} projects")
    return projects


@_register("villawood")
def scrape_villawood(session: requests.Session) -> list:
    """Villawood Properties — Melbourne growth areas."""
    projects = []
    base = "https://www.villawood.com.au"

    soup = _get(f"{base}/communities", session)
    if not soup:
        return projects

    for link in soup.find_all("a", href=True):
        href = link.get("href", "")
        name = link.get_text(strip=True)
        if not name or len(name) < 3:
            continue
        if "/communities/" in href or "/community/" in href:
            full_url = href if href.startswith("http") else f"{base}{href}"
            projects.append(_make_project(
                developer="Villawood Properties",
                name=name,
                suburb=name,
                state=_detect_state(name) or "VIC",
                url=full_url,
            ))

    logger.info(f"Villawood Properties: found {len(projects)} projects")
    return projects


@_register("scentre")
def scrape_scentre(session: requests.Session) -> list:
    """Scentre Group/Westfield — major centres."""
    projects = []
    base = "https://www.scentregroup.com.au"

    soup = _get(f"{base}/our-portfolio", session)
    if not soup:
        return projects

    text = soup.get_text(" ", strip=True)
    for link in soup.find_all("a", href=True):
        href = link.get("href", "")
        name = link.get_text(strip=True)
        if not name or len(name) < 3:
            continue
        if any(seg in href.lower() for seg in ["/portfolio/", "/centres/", "/westfield"]):
            full_url = href if href.startswith("http") else f"{base}{href}"
            projects.append(_make_project(
                developer="Scentre Group",
                name=name,
                suburb=name,
                state=_detect_state(name),
                url=full_url,
            ))

    logger.info(f"Scentre Group: found {len(projects)} projects")
    return projects


@_register("qic")
def scrape_qic(session: requests.Session) -> list:
    """QIC — QLD focused."""
    projects = []
    base = "https://www.qic.com.au"

    for path in ["/real-estate/shopping-centres", "/real-estate/developments"]:
        soup = _get(f"{base}{path}", session)
        if not soup:
            continue

        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            name = link.get_text(strip=True)
            if not name or len(name) < 3:
                continue
            if any(seg in href for seg in ["/shopping-centres/", "/developments/"]):
                full_url = href if href.startswith("http") else f"{base}{href}"
                projects.append(_make_project(
                    developer="QIC",
                    name=name,
                    suburb=name,
                    state=_detect_state(name) or "QLD",
                    url=full_url,
                ))

    logger.info(f"QIC: found {len(projects)} projects")
    return projects


@_register("charterhall")
def scrape_charterhall(session: requests.Session) -> list:
    """Charter Hall — national retail."""
    projects = []
    base = "https://www.charterhall.com.au"

    for path in ["/properties/retail", "/properties"]:
        soup = _get(f"{base}{path}", session)
        if not soup:
            continue

        for link in soup.find_all("a", href=True):
            href = link.get("href", "")
            name = link.get_text(strip=True)
            if not name or len(name) < 3:
                continue
            if "/properties/" in href and href.count("/") >= 3:
                full_url = href if href.startswith("http") else f"{base}{href}"
                projects.append(_make_project(
                    developer="Charter Hall",
                    name=name,
                    suburb=name,
                    state=_detect_state(name),
                    url=full_url,
                ))

    logger.info(f"Charter Hall: found {len(projects)} projects")
    return projects


# ── Geocoding ──────────────────────────────────────────────────────

def geocode_projects(projects: list) -> list:
    """Geocode projects using Nominatim."""
    if not HAS_GEOPY:
        logger.warning("geopy not installed — skipping geocoding")
        return projects

    geolocator = Nominatim(user_agent="PharmacyFinder-DevPipeline/1.0", timeout=10)
    geocoded = 0

    for p in projects:
        if p.get("latitude") and p.get("longitude"):
            continue

        query = p.get("suburb") or p.get("project_name", "")
        state = p.get("state", "")
        if not query:
            continue

        search = f"{query}, {state}, Australia" if state else f"{query}, Australia"
        time.sleep(1.1)  # Nominatim 1req/sec

        try:
            location = geolocator.geocode(search)
            if location:
                p["latitude"] = round(location.latitude, 6)
                p["longitude"] = round(location.longitude, 6)
                geocoded += 1
                logger.debug(f"Geocoded {query} -> {location.latitude}, {location.longitude}")
        except Exception as e:
            logger.warning(f"Geocode failed for {query}: {e}")

    logger.info(f"Geocoded {geocoded}/{len(projects)} projects")
    return projects


# ── Cross-referencing ──────────────────────────────────────────────

def _haversine_km(lat1, lon1, lat2, lon2):
    """Haversine distance in km."""
    R = 6371.0
    rlat1, rlon1, rlat2, rlon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = rlat2 - rlat1
    dlon = rlon2 - rlon1
    a = sin(dlat / 2) ** 2 + cos(rlat1) * cos(rlat2) * sin(dlon / 2) ** 2
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))


def cross_reference(projects: list, db_path: Path = DB_PATH) -> list:
    """Cross-reference projects with pharmacy DB tables."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # Load pharmacies for distance calc
    c.execute("SELECT latitude, longitude FROM pharmacies WHERE latitude IS NOT NULL AND longitude IS NOT NULL")
    pharmacies = [(r["latitude"], r["longitude"]) for r in c.fetchall()]
    logger.info(f"Loaded {len(pharmacies)} pharmacies for distance check")

    # Load v2_results that passed
    c.execute("SELECT latitude, longitude FROM v2_results WHERE passed_any = 1 AND latitude IS NOT NULL")
    v2_passed = [(r["latitude"], r["longitude"]) for r in c.fetchall()]

    # Load growth corridors
    c.execute("SELECT sa2_name, lat, lon, growth_score, classification FROM growth_corridors WHERE lat IS NOT NULL")
    corridors = [dict(r) for r in c.fetchall()]

    # Load planned town centres
    c.execute("SELECT centre_name, lat, lon, opportunity_score FROM planned_town_centres WHERE lat IS NOT NULL")
    psp_centres = [dict(r) for r in c.fetchall()]

    conn.close()

    for p in projects:
        lat, lon = p.get("latitude"), p.get("longitude")
        if not lat or not lon:
            p["nearest_pharmacy_km"] = None
            p["in_v2_results"] = False
            p["in_growth_corridor"] = False
            p["growth_corridor_name"] = ""
            p["in_psp_area"] = False
            p["psp_name"] = ""
            continue

        # Nearest pharmacy
        if pharmacies:
            dists = [_haversine_km(lat, lon, plat, plon) for plat, plon in pharmacies]
            p["nearest_pharmacy_km"] = round(min(dists), 2)
        else:
            p["nearest_pharmacy_km"] = None

        # V2 results within 2km
        p["in_v2_results"] = any(
            _haversine_km(lat, lon, vlat, vlon) < 2.0
            for vlat, vlon in v2_passed
        )

        # Growth corridor (within 5km of any corridor centroid)
        p["in_growth_corridor"] = False
        p["growth_corridor_name"] = ""
        for gc in corridors:
            if _haversine_km(lat, lon, gc["lat"], gc["lon"]) < 5.0:
                p["in_growth_corridor"] = True
                p["growth_corridor_name"] = gc["sa2_name"]
                break

        # PSP area (within 3km of a planned town centre)
        p["in_psp_area"] = False
        p["psp_name"] = ""
        for tc in psp_centres:
            if _haversine_km(lat, lon, tc["lat"], tc["lon"]) < 3.0:
                p["in_psp_area"] = True
                p["psp_name"] = tc["centre_name"]
                break

    return projects


# ── Scoring ────────────────────────────────────────────────────────

def score_projects(projects: list) -> list:
    """Score each project 0-100 for pharmacy opportunity."""
    for p in projects:
        score = 0
        breakdown = []

        # No nearby pharmacy (>5km) = 30pts
        dist = p.get("nearest_pharmacy_km")
        if dist is not None and dist > 5.0:
            score += 30
            breakdown.append(f"no_pharmacy_5km(+30, dist={dist}km)")
        elif dist is not None and dist > 2.0:
            score += 15
            breakdown.append(f"no_pharmacy_2km(+15, dist={dist}km)")

        # Growth corridor location = 20pts
        if p.get("in_growth_corridor"):
            score += 20
            breakdown.append(f"growth_corridor(+20, {p.get('growth_corridor_name', '')})")

        # Anchor supermarket confirmed = 15pts
        anchors = p.get("anchor_tenants", "")
        supermarket_anchors = [a for a in anchors.split(",") if a.strip().lower() in
                               ["coles", "woolworths", "aldi", "costco", "iga"]]
        if supermarket_anchors:
            score += 15
            breakdown.append(f"anchor_supermarket(+15, {','.join(supermarket_anchors)})")

        # Under construction / leasing = 15pts
        status = p.get("status", "")
        if status in ("under_construction", "leasing"):
            score += 15
            breakdown.append(f"active_status(+15, {status})")
        elif status == "planning":
            score += 8
            breakdown.append(f"planning_status(+8)")

        # Large GLA (>5000sqm) = 10pts
        gla = p.get("total_gla")
        if gla and gla > 5000:
            score += 10
            breakdown.append(f"large_gla(+10, {gla}sqm)")

        # PSP area = 10pts
        if p.get("in_psp_area"):
            score += 10
            breakdown.append(f"psp_area(+10, {p.get('psp_name', '')})")

        p["score"] = min(score, 100)
        p["score_breakdown"] = "; ".join(breakdown) if breakdown else "no scoring factors"

    # Sort by score desc
    projects.sort(key=lambda x: x.get("score", 0), reverse=True)
    return projects


# ── Database Storage ───────────────────────────────────────────────

def ensure_table(db_path: Path = DB_PATH):
    """Create developer_pipeline table if not exists."""
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS developer_pipeline (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            developer TEXT NOT NULL,
            project_name TEXT NOT NULL,
            address TEXT,
            suburb TEXT,
            state TEXT,
            status TEXT,
            anchor_tenants TEXT,
            total_gla REAL,
            has_pharmacy TEXT DEFAULT 'unknown',
            source_url TEXT,
            date_scraped TEXT,
            latitude REAL,
            longitude REAL,
            nearest_pharmacy_km REAL,
            in_v2_results INTEGER DEFAULT 0,
            in_growth_corridor INTEGER DEFAULT 0,
            growth_corridor_name TEXT,
            in_psp_area INTEGER DEFAULT 0,
            psp_name TEXT,
            score INTEGER DEFAULT 0,
            score_breakdown TEXT,
            UNIQUE(developer, project_name)
        )
    """)
    conn.commit()
    conn.close()


def store_projects(projects: list, db_path: Path = DB_PATH):
    """Upsert projects into developer_pipeline table."""
    ensure_table(db_path)
    conn = sqlite3.connect(str(db_path))
    c = conn.cursor()

    inserted = 0
    updated = 0
    for p in projects:
        try:
            c.execute("""
                INSERT INTO developer_pipeline
                    (developer, project_name, address, suburb, state, status,
                     anchor_tenants, total_gla, has_pharmacy, source_url, date_scraped,
                     latitude, longitude, nearest_pharmacy_km, in_v2_results,
                     in_growth_corridor, growth_corridor_name, in_psp_area, psp_name,
                     score, score_breakdown)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(developer, project_name) DO UPDATE SET
                    address = excluded.address,
                    suburb = excluded.suburb,
                    state = excluded.state,
                    status = excluded.status,
                    anchor_tenants = excluded.anchor_tenants,
                    total_gla = excluded.total_gla,
                    has_pharmacy = excluded.has_pharmacy,
                    source_url = excluded.source_url,
                    date_scraped = excluded.date_scraped,
                    latitude = excluded.latitude,
                    longitude = excluded.longitude,
                    nearest_pharmacy_km = excluded.nearest_pharmacy_km,
                    in_v2_results = excluded.in_v2_results,
                    in_growth_corridor = excluded.in_growth_corridor,
                    growth_corridor_name = excluded.growth_corridor_name,
                    in_psp_area = excluded.in_psp_area,
                    psp_name = excluded.psp_name,
                    score = excluded.score,
                    score_breakdown = excluded.score_breakdown
            """, (
                p["developer"], p["project_name"], p.get("address", ""),
                p.get("suburb", ""), p.get("state", ""), p.get("status", ""),
                p.get("anchor_tenants", ""), p.get("total_gla"),
                p.get("has_pharmacy", "unknown"), p.get("source_url", ""),
                p.get("date_scraped", ""), p.get("latitude"), p.get("longitude"),
                p.get("nearest_pharmacy_km"), int(p.get("in_v2_results", False)),
                int(p.get("in_growth_corridor", False)),
                p.get("growth_corridor_name", ""),
                int(p.get("in_psp_area", False)), p.get("psp_name", ""),
                p.get("score", 0), p.get("score_breakdown", ""),
            ))
            if c.rowcount:
                inserted += 1
        except sqlite3.IntegrityError:
            updated += 1

    conn.commit()
    conn.close()
    logger.info(f"Stored {inserted} new, {updated} updated projects")


# ── Output Generation ──────────────────────────────────────────────

def generate_outputs(projects: list, output_dir: Path = OUTPUT_DIR):
    """Generate JSON, CSV, and Markdown report."""
    output_dir.mkdir(parents=True, exist_ok=True)

    # JSON
    json_path = output_dir / "developer_pipeline.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(projects, f, indent=2, default=str)
    logger.info(f"Wrote {json_path}")

    # CSV
    csv_path = output_dir / "developer_pipeline.csv"
    if projects:
        fieldnames = [
            "developer", "project_name", "address", "suburb", "state", "status",
            "anchor_tenants", "total_gla", "has_pharmacy", "source_url",
            "date_scraped", "latitude", "longitude", "nearest_pharmacy_km",
            "in_v2_results", "in_growth_corridor", "growth_corridor_name",
            "in_psp_area", "psp_name", "score", "score_breakdown",
        ]
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(projects)
    logger.info(f"Wrote {csv_path}")

    # Markdown report
    md_path = output_dir / "developer_pipeline_report.md"
    _generate_report(projects, md_path)
    logger.info(f"Wrote {md_path}")

    return json_path, csv_path, md_path


def _generate_report(projects: list, path: Path):
    """Generate Markdown report."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = [
        f"# Developer Pipeline Report",
        f"",
        f"*Generated: {now}*",
        f"",
        f"## Summary",
        f"",
        f"- **Total projects scanned:** {len(projects)}",
        f"- **Developers covered:** {len(set(p['developer'] for p in projects))}",
        f"- **Projects with score >= 50:** {sum(1 for p in projects if p.get('score', 0) >= 50)}",
        f"- **Projects in growth corridors:** {sum(1 for p in projects if p.get('in_growth_corridor'))}",
        f"- **Projects in PSP areas:** {sum(1 for p in projects if p.get('in_psp_area'))}",
        f"- **No pharmacy within 5km:** {sum(1 for p in projects if (p.get('nearest_pharmacy_km') or 0) > 5.0)}",
        f"",
    ]

    # Top opportunities
    top = [p for p in projects if p.get("score", 0) > 0][:20]
    if top:
        lines.append("## Top Opportunities")
        lines.append("")
        lines.append("| # | Score | Developer | Project | Suburb | State | Status | Nearest Pharmacy |")
        lines.append("|---|-------|-----------|---------|--------|-------|--------|-----------------|")
        for i, p in enumerate(top, 1):
            dist = f"{p['nearest_pharmacy_km']}km" if p.get("nearest_pharmacy_km") else "N/A"
            lines.append(
                f"| {i} | {p.get('score', 0)} | {p['developer']} | {p['project_name']} | "
                f"{p.get('suburb', '')} | {p.get('state', '')} | {p.get('status', '')} | {dist} |"
            )
        lines.append("")

    # By developer
    devs = {}
    for p in projects:
        devs.setdefault(p["developer"], []).append(p)

    lines.append("## By Developer")
    lines.append("")
    for dev, dev_projects in sorted(devs.items()):
        lines.append(f"### {dev} ({len(dev_projects)} projects)")
        lines.append("")
        for p in dev_projects:
            score = p.get("score", 0)
            flag = " ⭐" if score >= 50 else ""
            lines.append(f"- **{p['project_name']}** — {p.get('suburb', 'N/A')}, "
                         f"{p.get('state', '?')} — Score: {score}{flag}")
            if p.get("score_breakdown") and p["score_breakdown"] != "no scoring factors":
                lines.append(f"  - {p['score_breakdown']}")
        lines.append("")

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


# ── Main Pipeline ──────────────────────────────────────────────────

def run_pipeline(developer_filter: str = None, top_n: int = None,
                 db_path: Path = DB_PATH) -> list:
    """
    Run the full developer pipeline.

    Args:
        developer_filter: If set, only scrape this developer (key from DEVELOPERS)
        top_n: If set, return only top N results by score
        db_path: Path to the database

    Returns:
        List of scored project dicts
    """
    session = requests.Session()
    all_projects = []

    # Determine which developers to scrape
    if developer_filter:
        key = developer_filter.lower().strip()
        if key not in DEVELOPERS:
            # Fuzzy match
            matches = [k for k in DEVELOPERS if key in k]
            if matches:
                key = matches[0]
            else:
                logger.error(f"Unknown developer: {developer_filter}. "
                             f"Available: {', '.join(DEVELOPERS.keys())}")
                return []
        scrapers = {key: DEVELOPERS[key]}
    else:
        scrapers = DEVELOPERS

    # Scrape
    for name, scraper_fn in scrapers.items():
        logger.info(f"Scraping {name}...")
        try:
            projects = scraper_fn(session)
            all_projects.extend(projects)
            logger.info(f"  -> {len(projects)} projects from {name}")
        except Exception as e:
            logger.error(f"  -> Failed {name}: {e}")

    if not all_projects:
        logger.warning("No projects found from any developer")
        return []

    # Deduplicate by (developer, project_name)
    seen = set()
    deduped = []
    for p in all_projects:
        key = (p["developer"], p["project_name"])
        if key not in seen:
            seen.add(key)
            deduped.append(p)
    all_projects = deduped
    logger.info(f"Total unique projects: {len(all_projects)}")

    # Geocode
    logger.info("Geocoding projects...")
    all_projects = geocode_projects(all_projects)

    # Cross-reference
    logger.info("Cross-referencing with pharmacy data...")
    all_projects = cross_reference(all_projects, db_path)

    # Score
    logger.info("Scoring projects...")
    all_projects = score_projects(all_projects)

    # Store
    logger.info("Storing to database...")
    store_projects(all_projects, db_path)

    # Generate outputs
    logger.info("Generating outputs...")
    generate_outputs(all_projects)

    # Apply top_n filter
    if top_n:
        all_projects = all_projects[:top_n]

    return all_projects
