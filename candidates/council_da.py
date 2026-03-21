"""
Council DA (Development Application) scanner for PharmacyFinder.

Scrapes council DA tracking portals for commercial development applications
that could create pharmacy opportunities. Uses PlanningAlerts API (aggregates
NSW, VIC, QLD, TAS, WA, SA councils) and state-specific portals where available.

Extracts: DA number, address, description, applicant, status, lodged date, council.
Filters for: shopping centre, medical centre, retail, commercial, pharmacy, mixed use.
"""
import os
import re
import sqlite3
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "pharmacy_finder.db")

PLANNING_ALERTS_URL = "https://api.planningalerts.org.au/applications.json"
PLANNING_ALERTS_KEY = os.environ.get("PLANNING_ALERTS_KEY", "")
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
RATE_LIMIT_SEC = 1.0
USER_AGENT = "PharmacyFinder-CouncilDA/1.0 (pharmacy-location-finder)"

KEYWORDS = [
    "shopping centre", "shopping center", "medical centre", "medical center",
    "retail", "commercial", "pharmacy", "mixed use", "supermarket",
]

# Postcodes by state for PlanningAlerts queries (sample coverage)
STATE_POSTCODES: Dict[str, List[str]] = {
    "NSW": ["2000", "2148", "2150", "2155", "2560", "2570", "2747", "2750", "2765", "2170"],
    "VIC": ["3000", "3008", "3029", "3030", "3149", "3168", "3175", "3199", "3216", "3220"],
    "QLD": ["4000", "4059", "4101", "4113", "4122", "4152", "4207", "4305", "4350", "4551"],
    "TAS": ["7000", "7008", "7010", "7018", "7248", "7249", "7250", "7310", "7320"],
    "WA": ["6000", "6027", "6065", "6108", "6112", "6164", "6171"],
    "SA": ["5000", "5034", "5063", "5082", "5095", "5114", "5162"],
}

DEVELOPMENT_TYPES = ("shopping_centre", "medical_centre", "retail", "mixed_use")


def _ensure_table(conn: sqlite3.Connection) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS geocode_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            address TEXT NOT NULL UNIQUE,
            latitude REAL NOT NULL,
            longitude REAL NOT NULL,
            date_cached TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS council_da (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            da_number TEXT UNIQUE,
            council TEXT,
            state TEXT,
            address TEXT,
            lat REAL,
            lon REAL,
            description TEXT,
            applicant TEXT,
            status TEXT,
            lodged_date TEXT,
            determined_date TEXT,
            development_type TEXT,
            estimated_gla_sqm REAL,
            pharmacy_potential REAL,
            source_url TEXT,
            date_scraped TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_council_da_state ON council_da(state)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_council_da_potential ON council_da(pharmacy_potential)")
    conn.commit()


def _geocode(address: str, conn: sqlite3.Connection) -> Optional[Tuple[float, float]]:
    """Geocode address via Nominatim. Rate limit 1/sec. Uses geocode_cache."""
    if not address or len(address.strip()) < 5:
        return None
    clean = address.strip()
    if ", australia" not in clean.lower() and "australia" not in clean.lower():
        clean += ", Australia"

    cur = conn.cursor()
    cur.execute("SELECT latitude, longitude FROM geocode_cache WHERE address = ?", (clean,))
    row = cur.fetchone()
    if row:
        return (row[0], row[1])

    time.sleep(RATE_LIMIT_SEC)
    try:
        resp = requests.get(
            NOMINATIM_URL,
            params={"q": clean, "format": "json", "countrycodes": "au", "limit": 1},
            headers={"User-Agent": USER_AGENT},
            timeout=10,
        )
        if resp.status_code == 200 and resp.json():
            lat = float(resp.json()[0]["lat"])
            lon = float(resp.json()[0]["lon"])
            if -44 <= lat <= -10 and 113 <= lon <= 154:
                cur.execute(
                    "INSERT OR IGNORE INTO geocode_cache (address, latitude, longitude, date_cached) VALUES (?, ?, ?, ?)",
                    (clean, lat, lon, datetime.now().isoformat()),
                )
                conn.commit()
                return (lat, lon)
    except Exception:
        pass
    return None


def _extract_gla(text: str) -> Optional[float]:
    """Extract GLA in sqm from description."""
    if not text:
        return None
    patterns = [
        r"([\d,]+)\s*(?:sq\s*m|sqm|m²|m2)\s*(?:gla|gross\s*lettable|floor\s*area)?",
        r"(?:gla|gross\s*lettable|floor\s*area)\s*(?:of\s*)?([\d,]+)\s*(?:sq\s*m|sqm|m²)?",
        r"([\d,]+)\s*(?:sq\s*m|sqm)\s*(?:retail|commercial)",
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            try:
                return float(m.group(1).replace(",", ""))
            except ValueError:
                pass
    # Hectares
    m = re.search(r"([\d.]+)\s*ha(?:ectares?)?", text, re.IGNORECASE)
    if m:
        try:
            return float(m.group(1)) * 10000
        except ValueError:
            pass
    return None


def _classify_development(desc: str) -> str:
    """Classify development type from description."""
    t = (desc or "").lower()
    if any(k in t for k in ["shopping centre", "shopping center", "neighbourhood centre"]):
        return "shopping_centre"
    if any(k in t for k in ["medical centre", "medical center", "health precinct", "gp ", "general practice"]):
        return "medical_centre"
    if any(k in t for k in ["mixed use", "mixed-use", "mixed.use"]):
        return "mixed_use"
    if any(k in t for k in ["retail", "commercial", "supermarket", "pharmacy"]):
        return "retail"
    return "retail"


def _score_pharmacy_potential(dev_type: str, gla: Optional[float], desc: str) -> float:
    """Score 0-1 based on development type and size."""
    score = 0.0
    t = (desc or "").lower()

    if dev_type == "shopping_centre":
        score = 0.8
        if gla and gla >= 5000:
            score = min(1.0, score + 0.2)
        if gla and gla >= 10000:
            score = 1.0
    elif dev_type == "medical_centre":
        score = 0.85
        if "8 fte" in t or "8fte" in t or "eight fte" in t:
            score = 1.0
    elif dev_type == "mixed_use":
        score = 0.5
        if "retail" in t or "commercial" in t:
            score = 0.6
    elif dev_type == "retail":
        score = 0.4
        if "supermarket" in t or "pharmacy" in t:
            score = 0.6

    if gla and gla >= 2500 and score < 0.7:
        score = min(1.0, score + 0.2)
    return round(min(1.0, score), 2)


def _parse_date(s: Any) -> Optional[str]:
    if not s:
        return None
    if isinstance(s, str):
        return s[:10] if len(s) >= 10 else s
    return str(s)[:10]


def _fetch_planning_alerts(state: Optional[str], postcodes: Optional[List[str]]) -> List[Dict]:
    """Fetch DAs from PlanningAlerts API."""
    if not PLANNING_ALERTS_KEY:
        return []

    apps = []
    seen_ids = set()

    def _parse_response(data: Any) -> List[Dict]:
        out = []
        items = data if isinstance(data, list) else data.get("applications", data.get("application", []))
        if isinstance(items, dict):
            items = [items]
        for app in items or []:
            a = app.get("application", app) if isinstance(app, dict) else app
            if isinstance(a, dict) and a.get("id") not in seen_ids:
                seen_ids.add(a.get("id"))
                out.append(a)
        return out

    if postcodes:
        for postcode in postcodes[:15]:
            params: Dict[str, Any] = {
                "key": PLANNING_ALERTS_KEY,
                "count": 100,
                "postcode": postcode,
            }
            if state:
                params["state"] = state
            try:
                resp = requests.get(PLANNING_ALERTS_URL, params=params, headers={"User-Agent": USER_AGENT}, timeout=15)
                if resp.status_code == 200:
                    apps.extend(_parse_response(resp.json()))
                time.sleep(0.5)
            except Exception:
                pass
    else:
        params = {"key": PLANNING_ALERTS_KEY, "count": 100}
        if state:
            params["state"] = state
        try:
            resp = requests.get(PLANNING_ALERTS_URL, params=params, headers={"User-Agent": USER_AGENT}, timeout=15)
            if resp.status_code == 200:
                apps.extend(_parse_response(resp.json()))
        except Exception:
            pass

    return apps


def _matches_keywords(text: str) -> bool:
    if not text:
        return False
    t = text.lower()
    return any(kw in t for kw in KEYWORDS)


def scan_state(state: str, db_path: Optional[str] = None) -> int:
    """
    Scan council DAs for a single state. Returns count of new DAs stored.
    Uses PlanningAlerts API; falls back to postcode queries if state param not supported.
    """
    db = db_path or DB_PATH
    conn = sqlite3.connect(db)
    _ensure_table(conn)

    postcodes = STATE_POSTCODES.get(state.upper(), [])
    apps = _fetch_planning_alerts(state.upper(), postcodes)

    if not apps and postcodes:
        for pc in postcodes[:5]:
            apps.extend(_fetch_planning_alerts(None, [pc]))

    seen = set()
    inserted = 0
    cur = conn.cursor()

    for app in apps:
        desc = app.get("description") or ""
        if not _matches_keywords(desc):
            continue

        da_id = str(app.get("id", ""))
        if da_id in seen:
            continue
        seen.add(da_id)

        address = app.get("address", "")
        lat = app.get("lat")
        lon = app.get("lng")
        if lat is not None and lon is not None:
            try:
                lat, lon = float(lat), float(lon)
            except (ValueError, TypeError):
                lat, lon = None, None

        if (lat is None or lon is None) and address:
            coords = _geocode(address, conn)
            if coords:
                lat, lon = coords

        authority = app.get("authority") or {}
        council = authority.get("full_name", "") if isinstance(authority, dict) else str(authority)

        dev_type = _classify_development(desc)
        gla = _extract_gla(desc)
        potential = _score_pharmacy_potential(dev_type, gla, desc)

        da_number = app.get("council_reference") or da_id or f"DA-{app.get('id', '')}"
        try:
            cur.execute("""
                INSERT OR IGNORE INTO council_da
                (da_number, council, state, address, lat, lon, description, applicant, status,
                 lodged_date, determined_date, development_type, estimated_gla_sqm,
                 pharmacy_potential, source_url, date_scraped)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                da_number,
                council,
                state.upper(),
                address,
                lat,
                lon,
                desc[:2000] if desc else None,
                None,  # PlanningAlerts doesn't return applicant
                app.get("status") or "Unknown",
                _parse_date(app.get("date_received") or app.get("date_lodged")),
                _parse_date(app.get("date_decided")),
                dev_type,
                gla,
                potential,
                app.get("info_url") or app.get("comment_url"),
                datetime.now().isoformat(),
            ))
            if cur.rowcount > 0:
                inserted += 1
        except sqlite3.IntegrityError:
            pass

    conn.commit()
    conn.close()
    return inserted


def get_das_for_evaluation(
    min_potential: float = 0.5,
    state: Optional[str] = None,
    db_path: Optional[str] = None,
) -> List[Dict]:
    """Return DAs with pharmacy_potential >= min_potential that have lat/lon."""
    db = db_path or DB_PATH
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    if state:
        cur.execute(
            """SELECT * FROM council_da WHERE pharmacy_potential >= ? AND lat IS NOT NULL AND lon IS NOT NULL AND state = ?""",
            (min_potential, state.upper()),
        )
    else:
        cur.execute(
            """SELECT * FROM council_da WHERE pharmacy_potential >= ? AND lat IS NOT NULL AND lon IS NOT NULL""",
            (min_potential,),
        )
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows
