"""
New Retail Development Scanner for PharmacyFinder
==================================================
Scrapes Australian sources for new retail developments that could create
pharmacy opportunities. Stores results in the 'developments' table of
pharmacy_finder.db AND outputs to JSON/CSV files.

Sources scanned:
  1. Shopping Centre News (shoppingcentrenews.com.au)
  2. Inside Retail (insideretail.com.au)
  3. The Urban Developer (theurbandeveloper.com)
  4. Coles / Woolworths / ALDI / Costco announcements
  5. Google News RSS (retail, residential, medical developments)
  6. PlanningAlerts.org.au API (council DAs — requires API key)
  7. Commercial Real Estate (commercialrealestate.com.au)
  8. Property Council of Australia news

For each development found:
  - Geocodes the location via Nominatim
  - Checks proximity to existing pharmacies in the DB
  - Calculates relevance score for pharmacy opportunity
  - Stores into pharmacy_finder.db 'developments' table

Run directly:  python development_scanner.py
Run via cron:  python development_scanner_cron.py
"""

import json
import csv
import os
import re
import sys
import time
import math
import random
import sqlite3
import hashlib
import logging
from datetime import datetime, timedelta
from email.utils import parsedate_to_datetime
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "pharmacy_finder.db")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
OUTPUT_JSON = os.path.join(OUTPUT_DIR, "development_opportunities.json")
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "development_opportunities.csv")

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)
HEADERS = {"User-Agent": USER_AGENT}
REQUEST_TIMEOUT = 20
RATE_LIMIT_MIN_SECONDS = 1.0
RATE_LIMIT_MAX_SECONDS = 2.0

# PlanningAlerts (free community key at planningalerts.org.au)
PLANNING_ALERTS_ENDPOINT = "https://api.planningalerts.org.au/applications.json"
PLANNING_ALERTS_KEY = os.environ.get("PLANNING_ALERTS_KEY", "")

# DA search keywords
DA_KEYWORDS = [
    "supermarket", "shopping centre", "shopping center", "woolworths",
    "coles", "aldi", "costco", "retail development", "pharmacy",
    "medical centre", "medical center", "hospital", "retail precinct",
    "town centre", "neighbourhood centre",
]

# High-growth postcodes for PlanningAlerts
HIGH_GROWTH_POSTCODES = [
    # NSW growth corridors
    "2765", "2763", "2761", "2557", "2570", "2560", "2155", "2756",
    "2179", "2745", "2750", "2259", "2322", "2529",
    # VIC growth corridors
    "3029", "3030", "3977", "3754", "3064", "3427", "3750",
    "3217", "3335", "3337", "3338", "3428", "3978",
    # QLD growth corridors
    "4209", "4509", "4740", "4300", "4207", "4510",
    "4573", "4575", "4350", "4817", "4870",
    # WA
    "6108", "6164", "6112", "6027", "6065", "6171",
    # SA
    "5114", "5162", "5095", "5120", "5118",
]

# Google News RSS queries — each targets a different development type
NEWS_QUERIES = {
    "new_supermarket": [
        '"new woolworths" opening australia',
        '"new coles" opening australia',
        '"new aldi" store opening australia',
        '"new costco" warehouse australia',
    ],
    "shopping_centre": [
        '"new shopping centre" australia development',
        '"shopping centre" expansion australia',
        '"town centre" development australia retail',
    ],
    "residential": [
        '"master-planned community" australia new suburb',
        '"residential development" australia "new suburb" OR "new estate"',
        '"housing estate" development australia lots',
    ],
    "medical": [
        '"new hospital" development australia',
        '"medical precinct" development australia',
        '"health precinct" australia construction',
    ],
}

# Pharmacy proximity threshold (km)
OPPORTUNITY_DISTANCE_KM = 1.5

# Development type constants
DEV_TYPES = [
    "new_shopping_centre",
    "expansion",
    "new_supermarket",
    "residential",
    "medical",
    "retail_precinct",
    "mixed_use",
    "unknown",
]

# Brand/type base scores for relevance
BRAND_SCORES = {
    "woolworths": 30,
    "coles": 30,
    "aldi": 25,
    "costco": 20,
    "new_shopping_centre": 35,
    "expansion": 25,
    "new_supermarket": 30,
    "residential": 20,
    "medical": 30,
    "retail_precinct": 20,
    "mixed_use": 15,
    "shopping_centre": 35,
    "retail_development": 15,
    "pharmacy": 10,
    "supermarket": 20,
    "unknown": 5,
}

# Logging
LOG_FILE = os.path.join(OUTPUT_DIR, "development_scanner.log")


def build_http_session():
    """Build a requests session with retry policy."""
    session = requests.Session()
    retry = Retry(
        total=2,
        connect=2,
        read=2,
        status=2,
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "HEAD"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


HTTP_SESSION = build_http_session()


def setup_logging():
    """Configure logging to both console and file."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    logger = logging.getLogger("dev_scanner")
    logger.setLevel(logging.INFO)

    # Clear existing handlers
    logger.handlers.clear()

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%H:%M:%S"))
    logger.addHandler(ch)

    # File handler
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S"))
    logger.addHandler(fh)

    return logger


log = setup_logging()


# ---------------------------------------------------------------------------
# Database Schema
# ---------------------------------------------------------------------------

DEVELOPMENTS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS developments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dev_hash TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    location_suburb TEXT,
    location_state TEXT,
    latitude REAL,
    longitude REAL,
    dev_type TEXT NOT NULL DEFAULT 'unknown',
    scale_shops INTEGER,
    scale_sqm REAL,
    scale_dwellings INTEGER,
    developer TEXT,
    expected_completion TEXT,
    source_url TEXT,
    source_name TEXT,
    description TEXT,
    date_discovered TEXT NOT NULL,
    date_updated TEXT,
    nearest_pharmacy_km REAL,
    nearest_pharmacy_name TEXT,
    has_supermarket INTEGER DEFAULT 0,
    supermarket_brand TEXT,
    existing_pharmacies_1km INTEGER DEFAULT 0,
    existing_pharmacies_3km INTEGER DEFAULT 0,
    relevance_score REAL DEFAULT 0,
    is_opportunity INTEGER DEFAULT 0,
    status TEXT DEFAULT 'new'
);
"""

DEVELOPMENTS_INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_dev_type ON developments(dev_type);",
    "CREATE INDEX IF NOT EXISTS idx_dev_state ON developments(location_state);",
    "CREATE INDEX IF NOT EXISTS idx_dev_score ON developments(relevance_score DESC);",
    "CREATE INDEX IF NOT EXISTS idx_dev_status ON developments(status);",
    "CREATE INDEX IF NOT EXISTS idx_dev_opportunity ON developments(is_opportunity);",
]


def init_db():
    """Create the developments table if it doesn't exist."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(DEVELOPMENTS_TABLE_SQL)
    for idx_sql in DEVELOPMENTS_INDEXES_SQL:
        cur.execute(idx_sql)
    # Also ensure geocode_cache exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS geocode_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            address TEXT NOT NULL UNIQUE,
            latitude REAL NOT NULL,
            longitude REAL NOT NULL,
            date_cached TEXT
        )
    """)
    conn.commit()
    conn.close()
    log.info("Database initialized (developments table ready)")


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def haversine_km(lat1, lon1, lat2, lon2):
    """Return distance in km between two lat/lon points."""
    R = 6371.0
    rlat1, rlon1 = math.radians(lat1), math.radians(lon1)
    rlat2, rlon2 = math.radians(lat2), math.radians(lon2)
    dlat = rlat2 - rlat1
    dlon = rlon2 - rlon1
    a = (math.sin(dlat / 2) ** 2
         + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(a))


def safe_get(url, params=None, headers=None, timeout=REQUEST_TIMEOUT):
    """GET with error handling and rate limiting."""
    hdrs = dict(HEADERS)
    if headers:
        hdrs.update(headers)
    try:
        resp = HTTP_SESSION.get(url, params=params, headers=hdrs, timeout=timeout)
        time.sleep(random.uniform(RATE_LIMIT_MIN_SECONDS, RATE_LIMIT_MAX_SECONDS))
        if resp.status_code == 200:
            return resp
        if resp.status_code in (403, 429):
            log.warning("Blocked by %s (HTTP %d)", url, resp.status_code)
            return None
        log.warning("HTTP %d for %s", resp.status_code, url)
        return None
    except requests.RequestException as exc:
        log.warning("Request failed for %s: %s", url, exc)
        time.sleep(random.uniform(RATE_LIMIT_MIN_SECONDS, RATE_LIMIT_MAX_SECONDS))
        return None


def _is_within_australia(lat, lon):
    """Check if coordinates fall within Australia's bounding box."""
    # Australia approx: lat -10 to -44, lon 113 to 154
    return -44.0 <= lat <= -10.0 and 113.0 <= lon <= 154.0


def geocode_address(address):
    """Geocode an address using Nominatim. Returns (lat, lon) or (None, None).
    Always appends ', Australia' and validates returned coords are within Australia.
    """
    if not address or len(address.strip()) < 5:
        return None, None
    clean = address.strip()
    # Always ensure Australia context
    if ", australia" not in clean.lower():
        clean += ", Australia"
    url = "https://nominatim.openstreetmap.org/search"
    params = {"q": clean, "format": "json", "countrycodes": "au", "limit": 1}
    resp = safe_get(url, params=params,
                    headers={"User-Agent": "PharmacyFinder-DevScanner/2.0"},
                    timeout=10)
    if resp:
        try:
            data = resp.json()
            if data:
                lat = float(data[0]["lat"])
                lon = float(data[0]["lon"])
                # Validate coordinates are actually in Australia
                if _is_within_australia(lat, lon):
                    return lat, lon
                else:
                    log.warning("Geocode for '%s' returned non-AU coords (%.4f, %.4f) — rejected",
                                address[:60], lat, lon)
                    return None, None
        except (ValueError, KeyError, IndexError):
            pass
    return None, None


def make_dev_hash(name, address, source):
    """Create a deterministic hash ID for deduplication."""
    raw = f"{name}|{address}|{source}".lower().strip()
    return hashlib.md5(raw.encode()).hexdigest()[:16]


def classify_development(text):
    """Classify a development based on its description text."""
    t = (text or "").lower()

    # Skip hotel/hospitality (not retail development)
    if any(w in t for w in ["hotel", "motel", "resort", "inn ", "accommodation",
                             "hilton", "marriott", "accor", "ihg ",
                             "novotel", "ibis", "mercure", "sofitel", "pullman",
                             "hyatt", "crowne plaza", "radisson", "rydges",
                             "holiday inn", "four seasons", "shangri-la"]):
        # Unless also mentions retail/shopping components
        if not any(kw in t for kw in ["shopping centre", "retail precinct", "supermarket"]):
            return "unknown"

    if "costco" in t:
        return "costco"
    if "woolworths" in t or "woolies" in t:
        return "woolworths"
    if "coles" in t and "colesville" not in t:
        return "coles"
    if "aldi" in t:
        return "aldi"
    if any(w in t for w in ["hospital", "health precinct", "medical precinct",
                             "medical centre", "health hub"]):
        return "medical"
    if any(w in t for w in ["master.planned", "master planned", "residential",
                             "housing estate", "new suburb", "lots released",
                             "dwelling", "housing development"]):
        return "residential"
    if any(w in t for w in ["expansion", "redevelop", "upgrade", "adding new"]):
        return "expansion"
    if any(w in t for w in ["shopping centre", "shopping center", "retail precinct",
                             "town centre", "neighbourhood centre"]):
        return "new_shopping_centre"
    if "supermarket" in t:
        return "new_supermarket"
    if "mixed.use" in t or "mixed use" in t:
        return "mixed_use"
    if "retail" in t:
        return "retail_precinct"
    return "unknown"


def extract_opening_date(text):
    """Try to extract an opening/completion date from text."""
    if not text:
        return None
    patterns = [
        r"(open(?:ing|s)?|complet(?:ed|ion|e)|due|expected|ready|finish)"
        r"\s+(?:in\s+|by\s+)?(?:late\s+|early\s+|mid[\s-]*)?(Q[1-4]\s+)?(\d{4})",
        r"(Q[1-4]\s+20[2-3]\d)",
        r"(20[2-3]\d)",
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            return m.group(0).strip()
    return None


def extract_developer(text):
    """Try to extract a developer/company name from text."""
    if not text:
        return None
    patterns = [
        r"(?:developed?\s+by|developer[:\s]+|proponent[:\s]+|applicant[:\s]+|owned\s+by|"
        r"managed\s+by|built\s+by)\s+([A-Z][A-Za-z0-9\s&'.]+?)(?:\.|,|\s+(?:has|is|will|for|at|in|on|announced))",
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            name = m.group(1).strip()
            if len(name) > 3 and len(name) < 80:
                return name
    return None


def extract_scale_info(text):
    """Extract scale information (shops, sqm, dwellings) from text."""
    info = {"shops": None, "sqm": None, "dwellings": None}
    if not text:
        return info
    t = text.lower()

    # Number of shops/stores/tenants
    m = re.search(r"(\d+)\s*(?:shops?|stores?|tenants?|retail\s+(?:shops?|spaces?))", t)
    if m:
        try:
            info["shops"] = int(m.group(1))
        except (ValueError, TypeError):
            pass

    # Sqm / GLA
    m = re.search(r"([\d,]+)\s*(?:sq\s*m|sqm|square\s*met(?:re|er)s?|m2|m²)", t)
    if m:
        try:
            info["sqm"] = float(m.group(1).replace(",", ""))
        except (ValueError, TypeError):
            pass

    # Hectares to sqm
    m = re.search(r"(\d+(?:\.\d+)?)\s*(?:ha|hectares?)", t)
    if m and not info["sqm"]:
        try:
            info["sqm"] = float(m.group(1)) * 10000
        except (ValueError, TypeError):
            pass

    # Dwellings / lots / homes
    m = re.search(r"(\d[\d,]*)\s*(?:dwellings?|lots?|homes?|residences?|apartments?|units?)", t)
    if m:
        try:
            info["dwellings"] = int(m.group(1).replace(",", ""))
        except (ValueError, TypeError):
            pass

    return info


# ---------------------------------------------------------------------------
# Known Australian locations for validation
# ---------------------------------------------------------------------------

KNOWN_AU_SUBURBS_CITIES = {
    # Capital cities and major CBDs
    "Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide", "Hobart",
    "Darwin", "Canberra", "Gold Coast", "Newcastle", "Wollongong",
    "Sunshine Coast", "Geelong", "Townsville", "Cairns", "Toowoomba",
    "Ballarat", "Bendigo", "Albury", "Launceston", "Mackay",
    "Rockhampton", "Bunbury", "Bundaberg", "Hervey Bay", "Wagga Wagga",
    "Coffs Harbour", "Gladstone", "Mildura", "Shepparton", "Port Macquarie",
    "Tamworth", "Orange", "Dubbo", "Geraldton", "Nowra", "Bathurst",
    "Warrnambool", "Alice Springs", "Mount Gambier", "Lismore",

    # Sydney suburbs
    "Parramatta", "Chatswood", "Bondi", "Manly", "Blacktown", "Penrith",
    "Liverpool", "Campbelltown", "Bankstown", "Hurstville", "Burwood",
    "Strathfield", "Ryde", "Hornsby", "Dee Why", "Brookvale", "Mosman",
    "Neutral Bay", "Cremorne", "Crows Nest", "Lane Cove", "Willoughby",
    "Marsden Park", "Box Hill", "Oran Park", "Leppington", "Austral",
    "Gregory Hills", "Gledswood Hills", "Cobbitty", "The Ponds",
    "Schofields", "Riverstone", "Kellyville", "Rouse Hill", "Castle Hill",
    "Baulkham Hills", "Norwest", "Bella Vista", "Stanhope Gardens",
    "Quakers Hill", "Seven Hills", "Toongabbie", "Westmead", "Merrylands",
    "Granville", "Auburn", "Lidcombe", "Homebush", "Rhodes", "Concord",
    "Five Dock", "Leichhardt", "Rozelle", "Balmain", "Pyrmont",
    "Surry Hills", "Redfern", "Waterloo", "Zetland", "Mascot",
    "Botany", "Maroubra", "Randwick", "Coogee", "Kingsford",
    "Eastgardens", "Pagewood", "Miranda", "Sutherland", "Cronulla",
    "Caringbah", "Engadine", "Menai", "Revesby", "Padstow",
    "Punchbowl", "Canterbury", "Lakemba", "Wiley Park", "Belmore",
    "Campsie", "Marrickville", "Dulwich Hill", "Ashfield", "Croydon",
    "Summer Hill", "Stanmore", "Newtown", "Enmore", "Petersham",
    "Epping", "Eastwood", "Carlingford", "Beecroft", "Pennant Hills",
    "Thornleigh", "Wahroonga", "Turramurra", "Gordon", "Killara",
    "Lindfield", "Roseville", "Artarmon", "St Leonards", "Wollstonecraft",
    "Waverton", "North Sydney", "Kirribilli", "Milsons Point",
    "Barangaroo", "Darling Harbour", "Haymarket", "Chippendale",
    "Glebe", "Annandale", "Lilyfield", "Drummoyne", "Gladesville",
    "Hunters Hill", "Meadowbank", "West Ryde", "Ermington",
    "Dundas", "Telopea", "Oatlands", "North Rocks", "Cherrybrook",

    # Melbourne suburbs
    "Docklands", "Southbank", "South Yarra", "Toorak", "Prahran",
    "St Kilda", "Brighton", "Elsternwick", "Caulfield", "Malvern",
    "Hawthorn", "Camberwell", "Kew", "Balwyn", "Box Hill",
    "Doncaster", "Templestowe", "Bulleen", "Heidelberg", "Ivanhoe",
    "Northcote", "Thornbury", "Preston", "Reservoir", "Coburg",
    "Brunswick", "Carlton", "Fitzroy", "Collingwood", "Richmond",
    "Cremorne", "Abbotsford", "Footscray", "Yarraville", "Williamstown",
    "Altona", "Werribee", "Point Cook", "Tarneit", "Wyndham Vale",
    "Truganina", "Melton", "Caroline Springs", "Hillside", "Sydenham",
    "Sunbury", "Craigieburn", "Epping", "South Morang", "Mernda",
    "Doreen", "Wollert", "Mickleham", "Kalkallo", "Donnybrook",
    "Clyde", "Clyde North", "Cranbourne", "Pakenham", "Officer",
    "Berwick", "Narre Warren", "Dandenong", "Springvale", "Clayton",
    "Oakleigh", "Moorabbin", "Cheltenham", "Mentone", "Mordialloc",
    "Frankston", "Mornington", "Rosebud", "Ringwood", "Croydon",
    "Lilydale", "Bayswater", "Boronia", "Ferntree Gully", "Knox",
    "Glen Waverley", "Mount Waverley", "Chadstone", "Hughesdale",
    "Traralgon", "Warragul", "Drouin", "Bacchus Marsh",
    "Torquay", "Armstrong Creek", "Lara", "Leopold",

    # Brisbane suburbs
    "South Brisbane", "West End", "Fortitude Valley", "New Farm",
    "Teneriffe", "Newstead", "Bowen Hills", "Woolloongabba",
    "Kangaroo Point", "East Brisbane", "Bulimba", "Hawthorne",
    "Morningside", "Coorparoo", "Camp Hill", "Carindale",
    "Mount Gravatt", "Sunnybank", "Wishart", "Upper Mount Gravatt",
    "Eight Mile Plains", "Rochedale", "Springwood", "Logan",
    "Beenleigh", "North Lakes", "Caboolture", "Morayfield",
    "Redcliffe", "Petrie", "Strathpine", "Albany Creek",
    "Chermside", "Nundah", "Kedron", "Stafford", "Everton Park",
    "Mitchelton", "Brookside", "Arana Hills", "Ferny Hills",
    "The Gap", "Ashgrove", "Bardon", "Paddington", "Milton",
    "Toowong", "Indooroopilly", "Kenmore", "Chapel Hill",
    "Jindalee", "Mount Ommaney", "Oxley", "Inala", "Forest Lake",
    "Springfield", "Ipswich", "Goodna", "Redbank Plains",
    "Ripley", "Yarrabilba", "Pimpama", "Coomera", "Helensvale",
    "Southport", "Surfers Paradise", "Broadbeach", "Burleigh Heads",
    "Robina", "Varsity Lakes", "Coolangatta", "Tweed Heads",
    "Palm Beach", "Currumbin",

    # Perth suburbs
    "Fremantle", "Joondalup", "Rockingham", "Mandurah", "Armadale",
    "Midland", "Morley", "Stirling", "Subiaco", "Cottesloe",
    "Claremont", "Nedlands", "Dalkeith", "Applecross", "Como",
    "South Perth", "Victoria Park", "Belmont", "Rivervale",
    "Bayswater", "Bassendean", "Maylands", "Mount Lawley",
    "Inglewood", "Leederville", "West Leederville", "Wembley",
    "Churchlands", "Karrinyup", "Scarborough", "Doubleview",
    "Innaloo", "Osborne Park", "Balcatta", "Nollamara",
    "Mirrabooka", "Westminster", "Balga", "Girrawheen",
    "Wanneroo", "Alkimos", "Yanchep", "Two Rocks",
    "Ellenbrook", "The Vines", "Mundijong", "Byford",
    "Baldivis", "Wellard", "Piara Waters", "Harrisdale",
    "Canning Vale", "Willetton", "Riverton", "Rossmoyne",
    "Bull Creek", "Leeming", "Murdoch", "Cockburn",
    "Success", "Atwell", "Aubin Grove", "Hammond Park",
    "Treeby", "Brabham", "Whiteman Edge",

    # Adelaide suburbs
    "Glenelg", "Henley Beach", "Semaphore", "Port Adelaide",
    "Prospect", "Walkerville", "Norwood", "Unley", "Hyde Park",
    "Goodwood", "Mitcham", "Blackwood", "Aberfoyle Park",
    "Morphett Vale", "Noarlunga", "Seaford", "Aldinga",
    "Mount Barker", "Stirling", "Crafers", "Burnside",
    "Glen Osmond", "Magill", "Rostrevor", "Campbelltown",
    "Paradise", "Modbury", "Tea Tree Gully", "Golden Grove",
    "Mawson Lakes", "Salisbury", "Elizabeth", "Gawler",
    "Munno Para", "Smithfield", "Playford", "Virginia",
    "Two Wells", "Angle Vale",

    # Regional NSW
    "Mudgee", "Cessnock", "Singleton", "Muswellbrook", "Maitland",
    "Raymond Terrace", "Nelson Bay", "Forster", "Tuncurry",
    "Taree", "Kempsey", "Nambucca Heads", "Bellingen", "Grafton",
    "Casino", "Ballina", "Byron Bay", "Tweed Heads", "Murwillumbah",
    "Armidale", "Glen Innes", "Inverell", "Moree", "Narrabri",
    "Gunnedah", "Lithgow", "Katoomba", "Springwood", "Richmond",
    "Windsor", "Gosford", "Wyong", "Tuggerah", "The Entrance",
    "Batemans Bay", "Moruya", "Narooma", "Bega", "Merimbula",
    "Eden", "Cooma", "Queanbeyan", "Yass", "Young", "Cowra",
    "Forbes", "Parkes", "Broken Hill", "Griffith", "Leeton",

    # Regional VIC
    "Echuca", "Swan Hill", "Horsham", "Hamilton", "Portland",
    "Colac", "Camperdown", "Ararat", "Stawell", "Wangaratta",
    "Benalla", "Seymour", "Kilmore", "Wallan", "Broadford",
    "Wodonga", "Sale", "Bairnsdale", "Lakes Entrance", "Orbost",
    "Wonthaggi", "Inverloch", "Cowes", "Moe", "Morwell",

    # Regional QLD
    "Yeppoon", "Emerald", "Longreach", "Mount Isa", "Charters Towers",
    "Ayr", "Ingham", "Innisfail", "Atherton", "Mareeba",
    "Port Douglas", "Cooktown", "Weipa", "Thursday Island",
    "Kingaroy", "Dalby", "Roma", "Charleville", "Warwick",
    "Stanthorpe", "Biloela", "Maryborough", "Gympie", "Nambour",
    "Caloundra", "Maroochydore", "Noosa", "Coolum",
    "Bargara", "Agnes Water",

    # ACT
    "Belconnen", "Woden", "Tuggeranong", "Gungahlin", "Weston Creek",
    "Civic", "Braddon", "Dickson", "Fyshwick", "Kingston",
    "Manuka", "Griffith", "Deakin", "Curtin", "Weston",
    "Molonglo Valley", "Wright", "Coombs", "Denman Prospect",
    "Whitlam", "Taylor", "Throsby", "Moncrieff",

    # Tasmania
    "Glenorchy", "Sandy Bay", "Bellerive", "Rosny", "Clarence",
    "Kingston", "Devonport", "Burnie", "Ulverstone", "Deloraine",
    "Sorell", "Bridgewater", "New Norfolk",

    # Northern Territory
    "Palmerston", "Casuarina", "Nightcliff", "Fannie Bay",
    "Stuart Park", "Katherine", "Tennant Creek", "Nhulunbuy",

    # WA regional
    "Kalgoorlie", "Esperance", "Albany", "Karratha", "Port Hedland",
    "Broome", "Kununurra", "Northam", "Merredin", "Collie",
    "Margaret River", "Busselton", "Dunsborough",
}

# Convert to lowercase set for fast lookup
_KNOWN_AU_SUBURBS_LOWER = {s.lower() for s in KNOWN_AU_SUBURBS_CITIES}

# Words that are NOT valid suburbs/locations — common false positives
_LOCATION_REJECT_WORDS = {
    "australia", "australian", "gazette", "national", "federal",
    "regional", "central", "greater", "inner", "outer", "local",
    "country", "international", "global", "worldwide", "overseas",
    "exclusive", "breaking", "latest", "update", "report", "review",
    "december", "january", "february", "march", "april", "may",
    "june", "july", "august", "september", "october", "november",
    "monday", "tuesday", "wednesday", "thursday", "friday",
    "saturday", "sunday", "today", "tomorrow", "yesterday",
    "new", "the", "this", "that", "first", "second", "third",
    "major", "biggest", "largest", "smallest", "newest", "oldest",
    "best", "worst", "most", "least", "top", "bottom",
    "retail", "shopping", "development", "construction", "building",
    "expansion", "opening", "launch", "project", "plan", "proposal",
    "million", "billion", "percent", "growth", "market", "industry",
    "council", "government", "minister", "premier", "opposition",
    "company", "group", "corporation", "limited", "holdings",
    "oakville", "ontario", "toronto", "vancouver", "calgary",
    "auckland", "wellington", "christchurch", "manchester",
    "birmingham", "london", "paris", "berlin", "tokyo",
}

# Non-Australian locations that regex might pick up
_NON_AU_LOCATIONS = {
    "oakville", "mississauga", "brampton", "surrey",  # Canada
    "new york", "los angeles", "san francisco", "seattle",  # USA
    "london", "manchester", "birmingham", "leeds",  # UK
    "auckland", "wellington", "christchurch", "hamilton",  # NZ (ambiguous)
}

# Common company/brand names that get falsely matched as suburbs
_COMPANY_REJECT_WORDS = {
    "cbre", "jll", "colliers", "cushman", "savills", "knight frank",
    "buchan", "buchan architects", "gapmaps", "gap inc", "banana republic",
    "charter hall", "dexus", "mirvac", "stockland", "lendlease",
    "goodman", "gpт", "scentre", "vicinity", "region group",
    "centennial", "haben", "assemble", "samma", "samma's",
    "jpmorgan", "goldman", "macquarie", "westpac", "anz", "cba",
    "optimisation", "predictive", "fulfilment", "relaunch",
    "adidas", "nike", "kmart", "target", "big w", "myer",
    "david jones", "jb hi-fi", "harvey norman",
    "mcdonalds", "mcdonald's", "hungry jacks", "kfc",
    "parade", "daily telegraph", "herald sun", "perthnow",
    "news.com", "realcommercial", "infrastructure",
    "architects", "properties", "investments", "holdings",
    "partners", "ventures", "solutions", "consulting",
    "corporation", "enterprise", "services", "management",
}


def extract_location_parts(text, article_body=None):
    """Extract suburb and state from text with smart validation.
    
    Priority:
      1. Explicit street address in article body
      2. Suburb + state pattern in article body
      3. Suburb + state pattern in headline/text
      4. Known Australian suburb mention in body then headline
    
    Returns (suburb, state).
    """
    if not text and not article_body:
        return None, None

    # State abbreviation mapping
    state_map = {
        "new south wales": "NSW", "nsw": "NSW",
        "victoria": "VIC", "vic": "VIC",
        "queensland": "QLD", "qld": "QLD",
        "western australia": "WA", "wa": "WA",
        "south australia": "SA", "sa": "SA",
        "tasmania": "TAS", "tas": "TAS",
        "northern territory": "NT", "nt": "NT",
        "australian capital territory": "ACT", "act": "ACT",
    }

    state_abbr_set = {"NSW", "VIC", "QLD", "WA", "SA", "TAS", "NT", "ACT"}

    def _validate_suburb(candidate):
        """Check if a candidate suburb is plausible."""
        if not candidate or len(candidate) < 3:
            return False
        cand_lower = candidate.lower()
        if cand_lower in _LOCATION_REJECT_WORDS:
            return False
        if cand_lower in _NON_AU_LOCATIONS:
            return False
        if cand_lower in _COMPANY_REJECT_WORDS:
            return False
        # Check if any word in the candidate is a company reject word
        for word in cand_lower.split():
            if word in _COMPANY_REJECT_WORDS:
                return False
        # Reject single common English words that aren't places
        if len(candidate.split()) == 1 and len(candidate) < 4:
            return False
        # Reject candidates that look like company descriptions
        if any(w in cand_lower for w in ["architect", "group", "inc", "corp",
                                          "pty", "ltd", "optimisation",
                                          "fulfilment", "relaunch"]):
            return False
        return True

    def _is_known_suburb(candidate):
        """Check if candidate is in our known suburbs list."""
        return candidate.lower() in _KNOWN_AU_SUBURBS_LOWER

    def _extract_state(txt):
        """Extract Australian state from text."""
        if not txt:
            return None
        # Check abbreviations first (exact word boundaries)
        for abbr in state_abbr_set:
            if re.search(r'\b' + abbr + r'\b', txt):
                return abbr
        # Check full names
        for key, abbr in state_map.items():
            if len(key) > 3 and re.search(r'\b' + re.escape(key) + r'\b', txt, re.I):
                return abbr
        return None

    def _try_extract_from_text(txt):
        """Try multiple patterns to extract suburb from text. Returns (suburb, state)."""
        if not txt:
            return None, None

        # Pattern 1: Explicit street address  e.g. "123 Smith Street, Mudgee NSW 2850"
        addr_m = re.search(
            r'\d+[A-Za-z]?\s+[\w\s]+?(?:Street|St|Road|Rd|Drive|Dr|Avenue|Ave|Highway|Hwy|'
            r'Parade|Pde|Way|Boulevard|Blvd|Lane|Ln|Circuit|Cct|Crescent|Cres|Place|Pl|'
            r'Terrace|Tce|Close|Cl)\s*,\s*'
            r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2})'
            r'(?:\s*,?\s*([A-Z]{2,3})\b)?',
            txt
        )
        if addr_m:
            suburb_candidate = addr_m.group(1).strip()
            state_candidate = addr_m.group(2) if addr_m.group(2) else None
            if state_candidate and state_candidate in state_abbr_set:
                if _validate_suburb(suburb_candidate):
                    return suburb_candidate, state_candidate
            elif _validate_suburb(suburb_candidate):
                return suburb_candidate, _extract_state(txt)

        # Pattern 2: "in/at SUBURB, STATE" or "in/at SUBURB, Full State Name"
        m = re.search(
            r'(?:in|at|for|near|to)\s+'
            r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2})'
            r'\s*,\s*'
            r'(New South Wales|Victoria|Queensland|Western Australia|South Australia|'
            r'Tasmania|Northern Territory|Australian Capital Territory|'
            r'NSW|VIC|QLD|WA|SA|TAS|NT|ACT)',
            txt, re.I
        )
        if m:
            suburb_candidate = m.group(1).strip()
            state_text = m.group(2).strip().lower()
            state_candidate = state_map.get(state_text, m.group(2).strip().upper())
            if state_candidate in state_abbr_set and _validate_suburb(suburb_candidate):
                return suburb_candidate, state_candidate

        # Pattern 3: "SUBURB in STATE" e.g. "North Lakes in Queensland"
        m = re.search(
            r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2})\s+in\s+'
            r'(New South Wales|Victoria|Queensland|Western Australia|South Australia|'
            r'Tasmania|Northern Territory|NSW|VIC|QLD|WA|SA|TAS|NT|ACT)',
            txt, re.I
        )
        if m:
            suburb_candidate = m.group(1).strip()
            state_text = m.group(2).strip().lower()
            state_candidate = state_map.get(state_text, m.group(2).strip().upper())
            if state_candidate in state_abbr_set and _validate_suburb(suburb_candidate):
                return suburb_candidate, state_candidate

        # Pattern 4: "in/at SUBURB" where SUBURB is a known Australian location
        for pat in [
            r'(?:in|at|for|near|to|opens?\s+(?:in|at))\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2})',
        ]:
            for m in re.finditer(pat, txt):
                candidate = m.group(1).strip()
                if _is_known_suburb(candidate) and _validate_suburb(candidate):
                    return candidate, _extract_state(txt)

        # Pattern 5: Scan for any known suburb mentioned in the text
        for suburb in KNOWN_AU_SUBURBS_CITIES:
            if re.search(r'\b' + re.escape(suburb) + r'\b', txt, re.I):
                return suburb, _extract_state(txt)

        return None, None

    suburb = None
    state = None

    # Priority 1: Extract from article body (most reliable — full context)
    if article_body:
        suburb, state = _try_extract_from_text(article_body)
        if suburb:
            return suburb, state

    # Priority 2: Extract from headline/title text
    if text:
        suburb, state = _try_extract_from_text(text)
        if suburb:
            return suburb, state

    # Priority 3: Last resort — look for state in combined text
    combined = ((text or "") + " " + (article_body or "")).strip()
    state = _extract_state(combined)

    return suburb, state


def check_supermarket_mention(text):
    """Check if text mentions a supermarket and return (has_super, brand)."""
    if not text:
        return False, None
    t = text.lower()
    brands = [
        ("woolworths", "Woolworths"), ("woolies", "Woolworths"),
        ("coles", "Coles"), ("aldi", "ALDI"),
        ("costco", "Costco"), ("iga", "IGA"),
    ]
    for keyword, brand in brands:
        if keyword in t:
            return True, brand
    if "supermarket" in t:
        return True, None
    return False, None


# ---------------------------------------------------------------------------
# Scanner Class
# ---------------------------------------------------------------------------

class DevelopmentScanner:
    """Scans multiple sources for new retail developments in Australia."""

    def __init__(self):
        self.developments = []
        self.pharmacies = []
        self.existing_dev_hashes = set()
        init_db()
        self._load_pharmacies()
        self._load_existing_hashes()

    def _load_pharmacies(self):
        """Load all pharmacies from the database."""
        log.info("Loading pharmacies from database...")
        try:
            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()
            cur.execute("SELECT name, latitude, longitude, address FROM pharmacies")
            self.pharmacies = [
                {"name": r[0], "lat": r[1], "lng": r[2], "address": r[3]}
                for r in cur.fetchall()
                if r[1] is not None and r[2] is not None
            ]
            conn.close()
            log.info("Loaded %d pharmacies", len(self.pharmacies))
        except Exception as e:
            log.error("Failed to load pharmacies: %s", e)
            self.pharmacies = []

    def _load_existing_hashes(self):
        """Load existing development hashes from DB for dedup."""
        try:
            conn = sqlite3.connect(DB_PATH)
            cur = conn.cursor()
            cur.execute("SELECT dev_hash FROM developments")
            self.existing_dev_hashes = {r[0] for r in cur.fetchall()}
            conn.close()
            log.info("Loaded %d existing development hashes", len(self.existing_dev_hashes))
        except Exception as exc:
            self.existing_dev_hashes = set()
            log.warning("Failed to load existing development hashes: %s", exc)

    def nearest_pharmacy_info(self, lat, lng):
        """Find nearest pharmacy. Returns (dist_km, name, count_1km, count_3km)."""
        if not lat or not lng or not self.pharmacies:
            return None, None, 0, 0
        best_dist = float("inf")
        best_name = None
        count_1km = 0
        count_3km = 0
        for ph in self.pharmacies:
            if ph["lat"] is None or ph["lng"] is None:
                continue
            d = haversine_km(lat, lng, ph["lat"], ph["lng"])
            if d < best_dist:
                best_dist = d
                best_name = ph["name"]
            if d <= 1.0:
                count_1km += 1
            if d <= 3.0:
                count_3km += 1
        return round(best_dist, 2), best_name, count_1km, count_3km

    def add_development(self, name, address, dev_type, source_name, source_url,
                        description="", developer=None, opening_date=None,
                        latitude=None, longitude=None, suburb=None, state=None,
                        scale_shops=None, scale_sqm=None, scale_dwellings=None,
                        has_supermarket=False, supermarket_brand=None,
                        article_body=None):
        """Add a discovered development to the list (dedup by hash)."""
        if not name or len(name.strip()) < 3:
            return False

        # Filter out non-Australian / irrelevant articles
        combined = ((name or "") + " " + (description or "")).lower()
        non_au_signals = [
            "florida", "texas", "california", "new york", "las vegas",
            "houston", "chicago", "london", "uk ", "u.k.", "united states",
            " us ", "u.s.", "canada", "new zealand", "michigan",
            "ohio", "georgia", "virginia", "illinois", "arizona",
            "oregon", "colorado", "tennessee", "carolina", "iowa",
            "indiana", "minnesota", "wisconsin", "missouri",
        ]
        if any(sig in combined for sig in non_au_signals):
            # Unless it also mentions Australia explicitly
            if "australia" not in combined and "australian" not in combined:
                return False

        # Filter hotels/resorts/accommodation — NOT pharmacy opportunities
        hotel_signals = [
            "hotel", "motel", "resort", "hostel", "accommodation",
            "holiday inn", "hilton", "marriott", "accor", "ihg hotel",
            "intercontinental", "novotel", "ibis", "mercure", "sofitel",
            "pullman", "mantra", "peppers", "quest apartment", "meriton suites",
            "ovolo", "adina", "vibe hotel", "rydges", "crowne plaza",
            "radisson", "hyatt", "four seasons", "shangri-la", "w hotel",
            "boutique hotel", "serviced apartment", "bed and breakfast",
            "airbnb", "booking.com", "hotels & resorts", "hotel brand",
            "luxury resort", "spa resort", "beach resort",
        ]
        if any(sig in combined for sig in hotel_signals):
            # Unless it's about a shopping centre near a hotel
            if not any(kw in combined for kw in ["shopping centre", "retail", "supermarket", "pharmacy"]):
                return False

        # Filter historical/nostalgia articles — not actual developments
        historical_signals = [
            "snapshot of the past", "remember when", "history of",
            "back in the day", "nostalgia", "historic photo",
            "looking back", "years ago today", "once was",
            "heritage listed", "heritage listing", "demolished in",
            "no longer exists", "used to be", "historical",
            "blast from the past", "throwback", "vintage photo",
            "old photo", "archival", "in memoriam",
        ]
        if any(sig in combined for sig in historical_signals):
            return False

        # Filter luxury/fashion retail — not pharmacy opportunities
        luxury_signals = [
            "louis vuitton", "gucci", "prada", "chanel", "hermes",
            "hermès", "montblanc", "mont blanc", "burberry", "dior",
            "valentino", "versace", "fendi", "bottega veneta", "cartier",
            "tiffany", "rolex", "omega", "tag heuer", "ralph lauren",
            "armani", "dolce & gabbana", "yves saint laurent", "balenciaga",
            "givenchy", "luxury brand", "luxury retail", "luxury fashion",
            "high-end fashion", "designer store", "flagship boutique",
        ]
        if any(sig in combined for sig in luxury_signals):
            if not any(kw in combined for kw in ["shopping centre", "retail precinct", "mixed-use"]):
                return False

        # Filter obvious non-development noise articles
        noise_phrases = [
            "leaves child in tears", "shoppers blast", "shopper rage",
            "recall", "contamination", "food safety", "price war",
            "dividend", "profit", "share price", "asx", "stock",
            "murder", "death of", "crime", "charged with",
            "recipe", "meal kit", "best buys", "glaring issue",
            "smacked", "annoying", "shoppers furious",
            "free tote", "price hike", "price increase",
            "shoplifting", "theft", "robbery", "assault",
            "customer complaint", "product recall",
            "banana republic", "gap inc", "gap's richard",
            "adidas launches", "nike launches",
            "bus route", "rejected from bar", "divide as aussie",
            "pokies king", "prison", "grim",
        ]
        title_lower = (name or "").lower()
        if any(phrase in title_lower for phrase in noise_phrases):
            return False

        dev_hash = make_dev_hash(name, address or "", source_name)

        # Skip duplicates in this run
        if any(d["dev_hash"] == dev_hash for d in self.developments):
            return False

        # Extract location if not provided
        if not suburb or not state:
            headline_text = (name or "") + " " + (description or "") + " " + (address or "")
            body_text = article_body if article_body else None
            ext_suburb, ext_state = extract_location_parts(headline_text, article_body=body_text)
            suburb = suburb or ext_suburb
            state = state or ext_state

        # Extract scale if not provided
        full_text = (name or "") + " " + (description or "")
        if not scale_shops and not scale_sqm and not scale_dwellings:
            scale = extract_scale_info(full_text)
            scale_shops = scale["shops"]
            scale_sqm = scale["sqm"]
            scale_dwellings = scale["dwellings"]

        # Check supermarket mentions
        if not has_supermarket:
            has_super, brand = check_supermarket_mention(full_text)
            has_supermarket = has_super
            supermarket_brand = supermarket_brand or brand

        self.developments.append({
            "dev_hash": dev_hash,
            "name": name.strip()[:300],
            "location_suburb": suburb,
            "location_state": state,
            "latitude": latitude,
            "longitude": longitude,
            "dev_type": dev_type,
            "scale_shops": scale_shops,
            "scale_sqm": scale_sqm,
            "scale_dwellings": scale_dwellings,
            "developer": developer,
            "expected_completion": opening_date,
            "source_url": (source_url or "")[:500],
            "source_name": source_name,
            "description": (description or "")[:1000],
            "date_discovered": datetime.now().strftime("%Y-%m-%d"),
            "date_updated": datetime.now().strftime("%Y-%m-%d"),
            "nearest_pharmacy_km": None,
            "nearest_pharmacy_name": None,
            "has_supermarket": 1 if has_supermarket else 0,
            "supermarket_brand": supermarket_brand,
            "existing_pharmacies_1km": 0,
            "existing_pharmacies_3km": 0,
            "relevance_score": 0.0,
            "is_opportunity": 0,
            "status": "new",
            "_address_for_geocoding": address or "",
            "_is_new_to_db": dev_hash not in self.existing_dev_hashes,
        })
        return True

    # ------------------------------------------------------------------
    # Source: Shopping Centre News
    # ------------------------------------------------------------------

    def _fetch_article_body(self, url, timeout=10):
        """Fetch an article page and extract body text."""
        resp = safe_get(url, timeout=timeout)
        if not resp:
            return None
        soup = BeautifulSoup(resp.text, "html.parser")
        body = soup.select_one("article, .entry-content, .post-content, .article-content, main")
        if body:
            return body.get_text(" ", strip=True)[:3000]
        return soup.get_text(" ", strip=True)[:3000]

    def scan_shopping_centre_news(self):
        """Scrape Shopping Centre News for development articles."""
        log.info("=== Scanning Shopping Centre News ===")
        urls = [
            "https://shoppingcentrenews.com.au/category/latest-news/",
            "https://shoppingcentrenews.com.au/category/latest-news/page/2/",
            "https://shoppingcentrenews.com.au/category/latest-news/page/3/",
            "https://shoppingcentrenews.com.au/category/latest-news/industry-news/",
        ]
        found = 0
        articles = []
        seen = set()

        for page_url in urls:
            resp = safe_get(page_url)
            if not resp:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            link_tags = soup.select("h3 a[href], h2 a[href], .entry-title a[href], article a[href]")
            if not link_tags:
                link_tags = soup.find_all("a", href=True)

            for a_tag in link_tags:
                href = a_tag.get("href", "")
                title = a_tag.get_text(strip=True)
                if not href or not title or len(title) < 15 or href in seen:
                    continue
                if not href.startswith("http"):
                    href = urljoin("https://shoppingcentrenews.com.au", href)

                title_lower = title.lower()
                keywords = [
                    "new", "open", "develop", "construct", "plan", "expand",
                    "build", "centre", "retail", "supermarket", "coles",
                    "woolworths", "aldi", "costco", "anchor", "redevelop",
                    "village", "shopping", "mixed-use", "precinct", "upgrade",
                    "expansion", "hospital", "medical",
                ]
                if not any(kw in title_lower for kw in keywords):
                    continue

                seen.add(href)
                articles.append((title, href))

        log.info("  Found %d relevant SCN article links", len(articles))

        # Fetch article bodies (limit to 20 to be respectful)
        for title, href in articles[:20]:
            body = self._fetch_article_body(href) if found < 15 else None
            full_text = title + " " + (body or "")
            dev_type = classify_development(full_text)
            suburb, state = extract_location_parts(title, article_body=body)

            self.add_development(
                name=title,
                address=(suburb + ", " + state + ", Australia") if suburb and state
                       else (suburb + ", Australia") if suburb else "",
                dev_type=dev_type,
                source_name="Shopping Centre News",
                source_url=href,
                description=(body or title)[:1000],
                developer=extract_developer(body) if body else None,
                opening_date=extract_opening_date(body or title),
                suburb=suburb,
                state=state,
                article_body=body,
            )
            found += 1

        log.info("Shopping Centre News: found %d articles", found)

    # ------------------------------------------------------------------
    # Source: Inside Retail
    # ------------------------------------------------------------------

    def scan_inside_retail(self):
        """Scrape Inside Retail for development news."""
        log.info("=== Scanning Inside Retail ===")
        urls = [
            "https://insideretail.com.au/news",
            "https://insideretail.com.au/news/page/2",
            "https://insideretail.com.au/industries/property",
        ]
        found = 0
        seen = set()

        for page_url in urls:
            resp = safe_get(page_url)
            if not resp:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            # Inside Retail uses various article card patterns
            link_tags = soup.select(
                "h2 a[href], h3 a[href], .post-title a[href], "
                ".article-title a[href], .card a[href]"
            )
            if not link_tags:
                link_tags = [a for a in soup.find_all("a", href=True)
                             if "/news/" in a.get("href", "")]

            for a_tag in link_tags:
                href = a_tag.get("href", "")
                title = a_tag.get_text(strip=True)
                if not href or not title or len(title) < 15 or href in seen:
                    continue
                if not href.startswith("http"):
                    href = urljoin("https://insideretail.com.au", href)

                title_lower = title.lower()
                keywords = [
                    "new store", "opening", "expand", "develop", "shopping centre",
                    "supermarket", "woolworths", "coles", "aldi", "costco",
                    "retail", "launch", "build", "precinct", "medical",
                ]
                if not any(kw in title_lower for kw in keywords):
                    continue

                seen.add(href)

                # Fetch article body for better location extraction
                article_body = self._fetch_article_body(href) if found < 15 else None
                full_text = title + " " + (article_body or "")

                dev_type = classify_development(full_text)
                suburb, state = extract_location_parts(title, article_body=article_body)

                self.add_development(
                    name=title,
                    address=(suburb + ", " + state + ", Australia") if suburb and state
                           else (suburb + ", Australia") if suburb else "",
                    dev_type=dev_type,
                    source_name="Inside Retail",
                    source_url=href,
                    description=(article_body or title)[:1000],
                    opening_date=extract_opening_date(full_text),
                    developer=extract_developer(article_body) if article_body else None,
                    suburb=suburb,
                    state=state,
                    article_body=article_body,
                )
                found += 1

        log.info("Inside Retail: found %d articles", found)

    # ------------------------------------------------------------------
    # Source: The Urban Developer
    # ------------------------------------------------------------------

    def scan_urban_developer(self):
        """Scrape The Urban Developer for retail/residential/medical developments."""
        log.info("=== Scanning The Urban Developer ===")
        urls = [
            "https://www.theurbandeveloper.com/",
            "https://www.theurbandeveloper.com/articles",
        ]
        found = 0

        for page_url in urls:
            resp = safe_get(page_url)
            if not resp:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            articles = soup.find_all("a", href=True)
            seen = set()

            for a_tag in articles:
                href = a_tag.get("href", "")
                title = a_tag.get_text(strip=True)
                if not title or len(title) < 20 or href in seen:
                    continue
                if "/articles/" not in href:
                    continue

                title_lower = title.lower()
                keywords = [
                    "retail", "shopping", "supermarket", "coles", "woolworths",
                    "aldi", "costco", "centre", "mall", "precinct",
                    "commercial", "mixed-use", "mixed use", "residential",
                    "master.plan", "hospital", "medical", "health",
                    "town centre", "development", "suburb",
                ]
                if not any(kw in title_lower for kw in keywords):
                    continue

                seen.add(href)
                if not href.startswith("http"):
                    href = urljoin("https://www.theurbandeveloper.com", href)

                # Fetch article body for better location extraction
                article_body = self._fetch_article_body(href) if found < 15 else None
                full_text = title + " " + (article_body or "")

                dev_type = classify_development(full_text)
                suburb, state = extract_location_parts(title, article_body=article_body)

                self.add_development(
                    name=title,
                    address=(suburb + ", " + state + ", Australia") if suburb and state
                           else (suburb + ", Australia") if suburb else "",
                    dev_type=dev_type,
                    source_name="The Urban Developer",
                    source_url=href,
                    description=(article_body or title)[:1000],
                    opening_date=extract_opening_date(full_text),
                    developer=extract_developer(article_body or title),
                    suburb=suburb,
                    state=state,
                    article_body=article_body,
                )
                found += 1
                if found >= 25:
                    break

        log.info("The Urban Developer: found %d articles", found)

    # ------------------------------------------------------------------
    # Source: ALDI New Stores
    # ------------------------------------------------------------------

    def scan_aldi_new_stores(self):
        """Scrape ALDI Australia new stores page."""
        log.info("=== Scanning ALDI new stores ===")
        urls_to_try = [
            "https://www.aldi.com.au/stores/new-stores/",
            "https://www.aldi.com.au/about-aldi/new-stores/",
        ]
        found = 0

        for url in urls_to_try:
            resp = safe_get(url)
            if not resp:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            current_state = ""

            for el in soup.find_all(["h2", "h3", "h4", "p", "li", "span", "div"]):
                text = el.get_text(strip=True)
                if not text or len(text) < 5 or len(text) > 300:
                    continue

                # Detect state headings
                states = {
                    "New South Wales": "NSW", "Victoria": "VIC",
                    "Queensland": "QLD", "South Australia": "SA",
                    "Western Australia": "WA", "Tasmania": "TAS",
                }
                for st_full, st_abbr in states.items():
                    if st_full.lower() in text.lower() and len(text) < 40:
                        current_state = st_abbr
                        break

                # Address pattern
                if re.search(
                    r"\d+\s+\w+\s+(street|road|drive|avenue|highway|parade|way|boulevard|lane)",
                    text, re.I
                ):
                    suburb_match = re.search(r",\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)", text)
                    suburb = suburb_match.group(1) if suburb_match else text.split(",")[0].strip()[:40]

                    self.add_development(
                        name="ALDI " + suburb,
                        address=text + (", " + current_state if current_state else "") + ", Australia",
                        dev_type="aldi",
                        source_name="ALDI Website",
                        source_url=url,
                        description="New ALDI store: " + text,
                        opening_date=extract_opening_date(text),
                        suburb=suburb,
                        state=current_state or None,
                        has_supermarket=True,
                        supermarket_brand="ALDI",
                    )
                    found += 1

                # "Opening soon" entries
                elif (re.search(r"(opening|coming soon|now open|new store)", text, re.I)
                      and len(text) < 150 and current_state):
                    self.add_development(
                        name="ALDI " + text.split("-")[0].split("(")[0].strip()[:60],
                        address=text + ", " + current_state + ", Australia",
                        dev_type="aldi",
                        source_name="ALDI Website",
                        source_url=url,
                        description=text,
                        opening_date=extract_opening_date(text),
                        state=current_state,
                        has_supermarket=True,
                        supermarket_brand="ALDI",
                    )
                    found += 1

            if found > 0:
                break

        log.info("ALDI new stores: found %d entries", found)

    # ------------------------------------------------------------------
    # Source: Coles Group Media
    # ------------------------------------------------------------------

    def scan_coles_media(self):
        """Check Coles Group news for new store announcements."""
        log.info("=== Scanning Coles Group news ===")
        urls = [
            "https://www.colesgroup.com.au/news/",
            "https://www.colesgroup.com.au/media-releases/",
        ]
        found = 0
        seen = set()

        for page_url in urls:
            resp = safe_get(page_url)
            if not resp:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            for a_tag in soup.find_all("a", href=True):
                href = a_tag.get("href", "")
                title = a_tag.get_text(strip=True)
                if not title or len(title) < 20 or title.lower().startswith("read more"):
                    continue
                if href in seen:
                    continue

                title_lower = title.lower()
                kws = ["new store", "store open", "store launch", "coles local",
                       "store arrives", "first coles", "new coles", "opening"]
                if not any(kw in title_lower for kw in kws):
                    continue

                if not href.startswith("http"):
                    href = urljoin("https://www.colesgroup.com.au", href)
                seen.add(href)

                suburb, state = extract_location_parts(title)
                self.add_development(
                    name=title,
                    address=(suburb + ", Australia") if suburb else "",
                    dev_type="coles",
                    source_name="Coles Group",
                    source_url=href,
                    description=title,
                    opening_date=extract_opening_date(title),
                    suburb=suburb,
                    state=state,
                    has_supermarket=True,
                    supermarket_brand="Coles",
                )
                found += 1

        log.info("Coles Group: found %d articles", found)

    # ------------------------------------------------------------------
    # Source: Woolworths Group Media
    # ------------------------------------------------------------------

    def scan_woolworths_media(self):
        """Check Woolworths Group media for new store announcements."""
        log.info("=== Scanning Woolworths Group news ===")
        urls = [
            "https://www.woolworthsgroup.com.au/au/en/our-newsroom.html",
            "https://www.woolworthsgroup.com.au/au/en/media/latest-news.html",
        ]
        found = 0
        seen = set()

        for page_url in urls:
            resp = safe_get(page_url)
            if not resp:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            for a_tag in soup.find_all("a", href=True):
                href = a_tag.get("href", "")
                title = a_tag.get_text(strip=True)
                if not title or len(title) < 15 or href in seen:
                    continue

                title_lower = title.lower()
                if not any(kw in title_lower for kw in
                           ["new store", "open", "launch", "expand", "metro",
                            "woolworths metro", "first", "new woolworths"]):
                    continue

                if not href.startswith("http"):
                    href = urljoin("https://www.woolworthsgroup.com.au", href)
                seen.add(href)

                suburb, state = extract_location_parts(title)
                self.add_development(
                    name=title,
                    address=(suburb + ", Australia") if suburb else "",
                    dev_type="woolworths",
                    source_name="Woolworths Group",
                    source_url=href,
                    description=title,
                    opening_date=extract_opening_date(title),
                    suburb=suburb,
                    state=state,
                    has_supermarket=True,
                    supermarket_brand="Woolworths",
                )
                found += 1

        log.info("Woolworths Group: found %d articles", found)

    # ------------------------------------------------------------------
    # Source: Property Council of Australia
    # ------------------------------------------------------------------

    def scan_property_council(self):
        """Scrape Property Council of Australia news."""
        log.info("=== Scanning Property Council of Australia ===")
        urls = [
            "https://www.propertycouncil.com.au/news",
            "https://www.propertycouncil.com.au/news-and-media",
        ]
        found = 0

        for page_url in urls:
            resp = safe_get(page_url)
            if not resp:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            for a_tag in soup.find_all("a", href=True):
                href = a_tag.get("href", "")
                title = a_tag.get_text(strip=True)
                if not title or len(title) < 15:
                    continue

                title_lower = title.lower()
                keywords = [
                    "retail", "shopping centre", "development", "construct",
                    "precinct", "commercial", "mixed-use", "residential",
                    "hospital", "medical", "supermarket", "growth corridor",
                ]
                if not any(kw in title_lower for kw in keywords):
                    continue

                if not href.startswith("http"):
                    href = urljoin("https://www.propertycouncil.com.au", href)

                dev_type = classify_development(title)
                suburb, state = extract_location_parts(title)

                self.add_development(
                    name=title,
                    address=(suburb + ", Australia") if suburb else "",
                    dev_type=dev_type,
                    source_name="Property Council",
                    source_url=href,
                    description=title,
                    suburb=suburb,
                    state=state,
                )
                found += 1

        log.info("Property Council: found %d articles", found)

    # ------------------------------------------------------------------
    # Source: Commercial Real Estate
    # ------------------------------------------------------------------

    def scan_commercial_real_estate(self):
        """Scrape commercialrealestate.com.au for new development listings."""
        log.info("=== Scanning Commercial Real Estate ===")
        urls = [
            "https://www.commercialrealestate.com.au/news/",
            "https://www.commercialrealestate.com.au/news/retail/",
        ]
        found = 0

        for page_url in urls:
            resp = safe_get(page_url)
            if not resp:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            for a_tag in soup.find_all("a", href=True):
                href = a_tag.get("href", "")
                title = a_tag.get_text(strip=True)
                if not title or len(title) < 15:
                    continue

                title_lower = title.lower()
                keywords = [
                    "new development", "shopping centre", "retail",
                    "supermarket", "precinct", "mixed-use", "expansion",
                    "commercial", "opening",
                ]
                if not any(kw in title_lower for kw in keywords):
                    continue

                if not href.startswith("http"):
                    href = urljoin("https://www.commercialrealestate.com.au", href)

                dev_type = classify_development(title)
                suburb, state = extract_location_parts(title)

                self.add_development(
                    name=title,
                    address=(suburb + ", Australia") if suburb else "",
                    dev_type=dev_type,
                    source_name="Commercial Real Estate",
                    source_url=href,
                    description=title,
                    suburb=suburb,
                    state=state,
                )
                found += 1

        log.info("Commercial Real Estate: found %d articles", found)

    # ------------------------------------------------------------------
    # Source: Google News RSS (multiple categories)
    # ------------------------------------------------------------------

    def scan_google_news(self):
        """Search Google News RSS for retail, residential, and medical developments."""
        log.info("=== Scanning Google News RSS ===")
        found = 0

        for category, queries in NEWS_QUERIES.items():
            for query in queries:
                url = "https://news.google.com/rss/search"
                params = {
                    "q": query + " when:30d",
                    "hl": "en-AU",
                    "gl": "AU",
                    "ceid": "AU:en",
                }

                resp = safe_get(url, params=params)
                if not resp:
                    continue

                try:
                    soup = BeautifulSoup(resp.text, "xml")
                    items = soup.find_all("item")
                except Exception:
                    soup = BeautifulSoup(resp.text, "html.parser")
                    items = soup.find_all("item")

                for item in items[:6]:  # Top 6 per query
                    title_el = item.find("title")
                    link_el = item.find("link")
                    pub_date_el = item.find("pubDate")

                    if not title_el:
                        continue

                    title = title_el.get_text(strip=True)

                    # Get link text (RSS quirk)
                    link_text = ""
                    if link_el:
                        link_text = link_el.get_text(strip=True)
                        if not link_text and link_el.next_sibling:
                            link_text = str(link_el.next_sibling).strip()

                    # Age filter
                    if pub_date_el:
                        try:
                            pub_dt = parsedate_to_datetime(pub_date_el.get_text(strip=True))
                            if pub_dt.replace(tzinfo=None) < datetime.now() - timedelta(days=30):
                                continue
                        except Exception:
                            pass

                    # Fetch article body for proper location extraction
                    article_body = None
                    if link_text and link_text.startswith("http"):
                        article_body = self._fetch_article_body(link_text)

                    full_text = title + " " + (article_body or "")
                    dev_type = classify_development(full_text)
                    # Fallback type from category
                    if dev_type == "unknown":
                        type_map = {
                            "new_supermarket": "new_supermarket",
                            "shopping_centre": "new_shopping_centre",
                            "residential": "residential",
                            "medical": "medical",
                        }
                        dev_type = type_map.get(category, "unknown")

                    suburb, state = extract_location_parts(title, article_body=article_body)

                    self.add_development(
                        name=title,
                        address=(suburb + ", " + state + ", Australia") if suburb and state
                               else (suburb + ", Australia") if suburb else "",
                        dev_type=dev_type,
                        source_name=f"Google News ({category})",
                        source_url=link_text,
                        description=(article_body or title)[:1000],
                        opening_date=extract_opening_date(full_text),
                        developer=extract_developer(article_body) if article_body else None,
                        suburb=suburb,
                        state=state,
                        article_body=article_body,
                    )
                    found += 1

        log.info("Google News: found %d articles", found)

    # ------------------------------------------------------------------
    # Source: PlanningAlerts.org.au (Council DAs)
    # ------------------------------------------------------------------

    def scan_planning_alerts(self):
        """Search PlanningAlerts for retail/commercial DAs."""
        log.info("=== Scanning PlanningAlerts.org.au ===")

        if not PLANNING_ALERTS_KEY:
            log.warning(
                "No PLANNING_ALERTS_KEY env var set — skipping. "
                "Get a free key at https://www.planningalerts.org.au/api/howto"
            )
            return

        found = 0
        for postcode in HIGH_GROWTH_POSTCODES:
            params = {"postcode": postcode, "count": 100, "key": PLANNING_ALERTS_KEY}
            resp = safe_get(PLANNING_ALERTS_ENDPOINT, params=params)
            if not resp:
                continue

            try:
                data = resp.json()
            except (ValueError, json.JSONDecodeError):
                continue

            applications = data if isinstance(data, list) else data.get(
                "application", data.get("applications", []))
            if isinstance(applications, dict):
                applications = [applications]

            for app_wrapper in applications:
                app = (app_wrapper.get("application", app_wrapper)
                       if isinstance(app_wrapper, dict) else app_wrapper)
                if not isinstance(app, dict):
                    continue

                desc = (app.get("description") or "").lower()
                address = app.get("address") or ""

                if not any(kw in desc for kw in DA_KEYWORDS):
                    continue

                dev_type = classify_development(desc)
                name = (app.get("description") or "Unknown DA")[:200]
                info_url = app.get("info_url") or app.get("comment_url") or ""
                lat = app.get("lat")
                lng = app.get("lng")
                suburb, state = extract_location_parts(address)
                authority = ""
                if isinstance(app.get("authority"), dict):
                    authority = app["authority"].get("full_name", "")

                self.add_development(
                    name=name,
                    address=address,
                    dev_type=dev_type,
                    source_name="PlanningAlerts",
                    source_url=info_url,
                    description=app.get("description", ""),
                    developer=extract_developer(desc) or authority,
                    opening_date=extract_opening_date(desc),
                    latitude=float(lat) if lat else None,
                    longitude=float(lng) if lng else None,
                    suburb=suburb,
                    state=state,
                )
                found += 1

        log.info("PlanningAlerts: found %d relevant DAs", found)

    # ------------------------------------------------------------------
    # Source: Costco (via Google News RSS)
    # ------------------------------------------------------------------

    def scan_costco(self):
        """Search for Costco Australia expansion news."""
        log.info("=== Scanning Costco Australia ===")
        found = 0

        url = "https://news.google.com/rss/search"
        params = {
            "q": '"costco" "new warehouse" OR "opening" australia when:90d',
            "hl": "en-AU",
            "gl": "AU",
            "ceid": "AU:en",
        }
        resp = safe_get(url, params=params)
        if not resp:
            return

        try:
            soup = BeautifulSoup(resp.text, "xml")
            items = soup.find_all("item")
        except Exception:
            soup = BeautifulSoup(resp.text, "html.parser")
            items = soup.find_all("item")

        for item in items[:10]:
            title_el = item.find("title")
            link_el = item.find("link")
            if not title_el:
                continue

            title = title_el.get_text(strip=True)
            link_text = ""
            if link_el:
                link_text = link_el.get_text(strip=True)
                if not link_text and link_el.next_sibling:
                    link_text = str(link_el.next_sibling).strip()

            if "costco" not in title.lower():
                continue

            # Fetch article body for location extraction
            article_body = None
            if link_text and link_text.startswith("http"):
                article_body = self._fetch_article_body(link_text)

            full_text = title + " " + (article_body or "")
            suburb, state = extract_location_parts(title, article_body=article_body)
            self.add_development(
                name=title,
                address=(suburb + ", " + state + ", Australia") if suburb and state
                       else (suburb + ", Australia") if suburb else "",
                dev_type="costco",
                source_name="Google News (Costco)",
                source_url=link_text,
                description=(article_body or title)[:1000],
                opening_date=extract_opening_date(full_text),
                suburb=suburb,
                state=state,
                has_supermarket=True,
                supermarket_brand="Costco",
                article_body=article_body,
            )
            found += 1

        log.info("Costco news: found %d articles", found)

    # ------------------------------------------------------------------
    # Post-processing
    # ------------------------------------------------------------------

    def geocode_developments(self):
        """Geocode developments missing coordinates."""
        log.info("=== Geocoding developments ===")

        # First, validate any pre-existing coordinates are within Australia
        for d in self.developments:
            if d["latitude"] and d["longitude"]:
                if not _is_within_australia(d["latitude"], d["longitude"]):
                    log.warning("Pre-set coords for '%s' outside Australia (%.4f, %.4f) — clearing",
                                d["name"][:50], d["latitude"], d["longitude"])
                    d["latitude"] = None
                    d["longitude"] = None

        need_geocode = [d for d in self.developments
                        if not d["latitude"] and d.get("_address_for_geocoding")]

        # Also try suburb-based geocoding for those without address
        for d in self.developments:
            if not d["latitude"] and not d.get("_address_for_geocoding") and d["location_suburb"]:
                addr = d["location_suburb"]
                if d["location_state"]:
                    addr += ", " + d["location_state"]
                addr += ", Australia"
                d["_address_for_geocoding"] = addr
                need_geocode.append(d)

        log.info("Need to geocode: %d developments", len(need_geocode))

        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        geocoded = 0
        cache_hits = 0

        for i, dev in enumerate(need_geocode):
            addr = dev["_address_for_geocoding"]
            if not addr or len(addr.strip()) < 5:
                continue

            # Cache check
            try:
                cur.execute("SELECT latitude, longitude FROM geocode_cache WHERE address = ?",
                            (addr,))
                row = cur.fetchone()
                if row:
                    dev["latitude"], dev["longitude"] = row[0], row[1]
                    cache_hits += 1
                    continue
            except Exception:
                pass

            lat, lon = geocode_address(addr)
            if lat and lon:
                dev["latitude"] = lat
                dev["longitude"] = lon
                geocoded += 1
                try:
                    cur.execute(
                        "INSERT OR IGNORE INTO geocode_cache "
                        "(address, latitude, longitude, date_cached) VALUES (?, ?, ?, ?)",
                        (addr, lat, lon, datetime.now().isoformat())
                    )
                    conn.commit()
                except Exception:
                    pass

            if (i + 1) % 10 == 0:
                log.info("  Geocoded %d/%d ...", i + 1, len(need_geocode))

        conn.close()
        total = sum(1 for d in self.developments if d["latitude"])
        log.info("Geocoding: %d API, %d cache, %d total with coords", geocoded, cache_hits, total)

    def check_pharmacy_proximity(self):
        """Check each development against existing pharmacies."""
        log.info("=== Checking pharmacy proximity ===")
        opportunities = 0

        for dev in self.developments:
            if not dev["latitude"] or not dev["longitude"]:
                continue

            dist_km, nearest_name, count_1km, count_3km = self.nearest_pharmacy_info(
                dev["latitude"], dev["longitude"]
            )
            dev["nearest_pharmacy_km"] = dist_km
            dev["nearest_pharmacy_name"] = nearest_name
            dev["existing_pharmacies_1km"] = count_1km
            dev["existing_pharmacies_3km"] = count_3km

            if dist_km is not None and dist_km >= OPPORTUNITY_DISTANCE_KM:
                dev["is_opportunity"] = 1
                opportunities += 1

        log.info("Found %d developments >%.1fkm from nearest pharmacy",
                 opportunities, OPPORTUNITY_DISTANCE_KM)

    def calculate_relevance_scores(self):
        """Calculate relevance score for pharmacy opportunity."""
        log.info("=== Calculating relevance scores ===")
        for dev in self.developments:
            score = 0.0

            # 1. Brand/type base score (0-35)
            score += BRAND_SCORES.get(dev["dev_type"], BRAND_SCORES.get("unknown", 5))

            # 2. Supermarket presence (major factor for pharmacy viability)
            if dev["has_supermarket"]:
                score += 20
                if dev["supermarket_brand"] in ("Woolworths", "Coles"):
                    score += 5  # Full-line supermarket = better anchor

            # 3. Distance from nearest pharmacy
            dist = dev["nearest_pharmacy_km"]
            if dist is not None:
                if dist >= 5.0:
                    score += 30
                elif dist >= 3.0:
                    score += 25
                elif dist >= 1.5:
                    score += 15
                elif dist >= 1.0:
                    score += 5
                elif dist < 0.3:
                    score -= 15  # Very close to existing pharmacy

            # 4. Few existing pharmacies nearby
            if dev["existing_pharmacies_1km"] == 0:
                score += 10
            elif dev["existing_pharmacies_1km"] >= 3:
                score -= 10

            if dev["existing_pharmacies_3km"] == 0:
                score += 10
            elif dev["existing_pharmacies_3km"] >= 5:
                score -= 5

            # 5. Scale bonus
            if dev["scale_shops"] and dev["scale_shops"] >= 50:
                score += 15
            elif dev["scale_shops"] and dev["scale_shops"] >= 15:
                score += 10
            elif dev["scale_shops"] and dev["scale_shops"] >= 5:
                score += 5

            if dev["scale_sqm"] and dev["scale_sqm"] >= 10000:
                score += 10
            elif dev["scale_sqm"] and dev["scale_sqm"] >= 5000:
                score += 5

            if dev["scale_dwellings"] and dev["scale_dwellings"] >= 1000:
                score += 15  # Large residential = good pharmacy demand
            elif dev["scale_dwellings"] and dev["scale_dwellings"] >= 500:
                score += 10

            # 6. Has geocoded location (more actionable)
            if dev["latitude"] and dev["longitude"]:
                score += 5

            # 7. Has completion date (more concrete)
            if dev["expected_completion"]:
                score += 3

            # 8. Has developer info
            if dev["developer"]:
                score += 2

            # 9. Medical developments are high-value for pharmacies
            if dev["dev_type"] == "medical":
                score += 10

            dev["relevance_score"] = max(0, round(score, 1))

        # Sort by relevance
        self.developments.sort(key=lambda d: d["relevance_score"], reverse=True)

    # ------------------------------------------------------------------
    # Run All Scans
    # ------------------------------------------------------------------

    def run_all_scans(self):
        """Execute all scanning sources."""
        log.info("=" * 60)
        log.info("  DEVELOPMENT SCANNER v2.0 — Starting")
        log.info("  Timestamp: %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        log.info("=" * 60)

        scanners = [
            ("Shopping Centre News", self.scan_shopping_centre_news),
            ("Inside Retail", self.scan_inside_retail),
            ("The Urban Developer", self.scan_urban_developer),
            ("ALDI", self.scan_aldi_new_stores),
            ("Coles", self.scan_coles_media),
            ("Woolworths", self.scan_woolworths_media),
            ("Costco", self.scan_costco),
            ("Property Council", self.scan_property_council),
            ("Commercial Real Estate", self.scan_commercial_real_estate),
            ("Google News", self.scan_google_news),
            ("PlanningAlerts", self.scan_planning_alerts),
        ]

        for name, fn in scanners:
            try:
                fn()
            except Exception as e:
                log.error("%s scan failed: %s", name, e, exc_info=True)

        log.info("")
        log.info("Total raw developments found: %d", len(self.developments))

        # Post-processing
        self.geocode_developments()
        self.check_pharmacy_proximity()
        self.calculate_relevance_scores()

    # ------------------------------------------------------------------
    # Database Storage
    # ------------------------------------------------------------------

    def save_to_db(self):
        """Save/update developments in the SQLite database."""
        log.info("=== Saving to database ===")
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()

        inserted = 0
        updated = 0
        db_errors = 0
        total = 0
        total_opps = 0

        try:
            for dev in self.developments:
                dev_hash = dev["dev_hash"]
                dev["_db_action"] = "skipped"

                try:
                    cur.execute("SELECT id FROM developments WHERE dev_hash = ?", (dev_hash,))
                    existing = cur.fetchone()

                    if existing:
                        cur.execute("""
                            UPDATE developments SET
                                name = ?,
                                location_suburb = ?,
                                location_state = ?,
                                latitude = COALESCE(?, latitude),
                                longitude = COALESCE(?, longitude),
                                dev_type = ?,
                                scale_shops = COALESCE(?, scale_shops),
                                scale_sqm = COALESCE(?, scale_sqm),
                                scale_dwellings = COALESCE(?, scale_dwellings),
                                developer = COALESCE(?, developer),
                                expected_completion = COALESCE(?, expected_completion),
                                source_url = ?,
                                source_name = ?,
                                description = ?,
                                date_updated = ?,
                                nearest_pharmacy_km = ?,
                                nearest_pharmacy_name = ?,
                                has_supermarket = ?,
                                supermarket_brand = COALESCE(?, supermarket_brand),
                                existing_pharmacies_1km = ?,
                                existing_pharmacies_3km = ?,
                                relevance_score = ?,
                                is_opportunity = ?
                            WHERE dev_hash = ?
                        """, (
                            dev["name"], dev["location_suburb"], dev["location_state"],
                            dev["latitude"], dev["longitude"],
                            dev["dev_type"],
                            dev["scale_shops"], dev["scale_sqm"], dev["scale_dwellings"],
                            dev["developer"], dev["expected_completion"],
                            dev["source_url"], dev["source_name"], dev["description"],
                            dev["date_updated"],
                            dev["nearest_pharmacy_km"], dev["nearest_pharmacy_name"],
                            dev["has_supermarket"], dev["supermarket_brand"],
                            dev["existing_pharmacies_1km"], dev["existing_pharmacies_3km"],
                            dev["relevance_score"], dev["is_opportunity"],
                            dev_hash,
                        ))
                        updated += 1
                        dev["_db_action"] = "updated"
                    else:
                        cur.execute("""
                            INSERT INTO developments (
                                dev_hash, name, location_suburb, location_state,
                                latitude, longitude, dev_type,
                                scale_shops, scale_sqm, scale_dwellings,
                                developer, expected_completion,
                                source_url, source_name, description,
                                date_discovered, date_updated,
                                nearest_pharmacy_km, nearest_pharmacy_name,
                                has_supermarket, supermarket_brand,
                                existing_pharmacies_1km, existing_pharmacies_3km,
                                relevance_score, is_opportunity, status
                            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """, (
                            dev_hash, dev["name"], dev["location_suburb"], dev["location_state"],
                            dev["latitude"], dev["longitude"], dev["dev_type"],
                            dev["scale_shops"], dev["scale_sqm"], dev["scale_dwellings"],
                            dev["developer"], dev["expected_completion"],
                            dev["source_url"], dev["source_name"], dev["description"],
                            dev["date_discovered"], dev["date_updated"],
                            dev["nearest_pharmacy_km"], dev["nearest_pharmacy_name"],
                            dev["has_supermarket"], dev["supermarket_brand"],
                            dev["existing_pharmacies_1km"], dev["existing_pharmacies_3km"],
                            dev["relevance_score"], dev["is_opportunity"], "new",
                        ))
                        inserted += 1
                        dev["_db_action"] = "inserted"
                        self.existing_dev_hashes.add(dev_hash)

                except sqlite3.Error as e:
                    db_errors += 1
                    log.warning("DB error for '%s': %s", dev["name"][:50], e)

            conn.commit()

            cur.execute("SELECT COUNT(*) FROM developments")
            total = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM developments WHERE is_opportunity = 1")
            total_opps = cur.fetchone()[0]
        finally:
            conn.close()

        log.info("DB: %d inserted, %d updated, %d total (%d opportunities)",
                 inserted, updated, total, total_opps)
        if db_errors:
            log.warning("Database save completed with %d row errors", db_errors)
        return {
            "inserted": inserted,
            "updated": updated,
            "total": total,
            "total_opportunities": total_opps,
            "errors": db_errors,
        }

    # ------------------------------------------------------------------
    # File Output
    # ------------------------------------------------------------------

    def save_to_files(self):
        """Save results to JSON and CSV files."""
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        # Clean internal fields
        clean_devs = []
        for d in self.developments:
            cd = {k: v for k, v in d.items() if not k.startswith("_")}
            clean_devs.append(cd)

        # JSON
        with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
            json.dump({
                "scan_date": datetime.now().isoformat(),
                "total_developments": len(clean_devs),
                "total_opportunities": sum(1 for d in clean_devs if d.get("is_opportunity")),
                "developments": clean_devs,
            }, f, indent=2, ensure_ascii=False)
        log.info("Saved JSON: %s", OUTPUT_JSON)

        # CSV
        csv_fields = [
            "dev_hash", "name", "location_suburb", "location_state",
            "latitude", "longitude", "dev_type",
            "scale_shops", "scale_sqm", "scale_dwellings",
            "developer", "expected_completion",
            "source_url", "source_name",
            "nearest_pharmacy_km", "nearest_pharmacy_name",
            "has_supermarket", "supermarket_brand",
            "existing_pharmacies_1km", "existing_pharmacies_3km",
            "relevance_score", "is_opportunity",
            "date_discovered",
        ]
        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=csv_fields, extrasaction="ignore")
            writer.writeheader()
            for dev in clean_devs:
                writer.writerow(dev)
        log.info("Saved CSV: %s", OUTPUT_CSV)

    # ------------------------------------------------------------------
    # Print Summary
    # ------------------------------------------------------------------

    def print_summary(self):
        """Print a formatted summary to console."""
        total = len(self.developments)
        opps = [d for d in self.developments if d.get("is_opportunity")]
        geocoded = sum(1 for d in self.developments if d["latitude"])
        new_to_db = sum(1 for d in self.developments if d.get("_is_new_to_db"))

        print("")
        print("=" * 70)
        print("  DEVELOPMENT SCANNER v2.0 — RESULTS SUMMARY")
        print("=" * 70)
        print("")
        print("  Scan Date:          %s" % datetime.now().strftime("%Y-%m-%d %H:%M"))
        print("  Pharmacies in DB:   %d" % len(self.pharmacies))
        print("  Total Found:        %d" % total)
        print("  New to DB:          %d" % new_to_db)
        print("  Geocoded:           %d" % geocoded)
        print("  Opportunities:      %d (>%.1fkm from pharmacy)" % (len(opps), OPPORTUNITY_DISTANCE_KM))
        print("")

        # By source
        sources = {}
        for d in self.developments:
            sources[d["source_name"]] = sources.get(d["source_name"], 0) + 1
        if sources:
            print("  By Source:")
            for src, cnt in sorted(sources.items(), key=lambda x: -x[1]):
                print("    %-35s %d" % (src, cnt))
            print("")

        # By type
        types = {}
        for d in self.developments:
            types[d["dev_type"]] = types.get(d["dev_type"], 0) + 1
        if types:
            print("  By Type:")
            for tp, cnt in sorted(types.items(), key=lambda x: -x[1]):
                print("    %-35s %d" % (tp, cnt))
            print("")

        # Top opportunities
        if opps:
            print("-" * 70)
            print("  TOP PHARMACY OPPORTUNITIES (no pharmacy within %.1fkm)" % OPPORTUNITY_DISTANCE_KM)
            print("-" * 70)
            for i, d in enumerate(opps[:15], 1):
                print("")
                print("  %d. [Score: %.0f] %s" % (i, d["relevance_score"], d["name"][:65]))
                loc_parts = []
                if d["location_suburb"]:
                    loc_parts.append(d["location_suburb"])
                if d["location_state"]:
                    loc_parts.append(d["location_state"])
                if loc_parts:
                    print("     Location: %s" % ", ".join(loc_parts))
                print("     Type: %-20s Source: %s" % (d["dev_type"], d["source_name"]))
                if d["nearest_pharmacy_km"] is not None:
                    print("     Nearest pharmacy: %.2f km (%s)" % (
                        d["nearest_pharmacy_km"],
                        d["nearest_pharmacy_name"] or "unknown"))
                    print("     Pharmacies: %d within 1km, %d within 3km" % (
                        d["existing_pharmacies_1km"], d["existing_pharmacies_3km"]))
                if d["has_supermarket"]:
                    print("     Supermarket: %s" % (d["supermarket_brand"] or "Yes"))
                if d["expected_completion"]:
                    print("     Expected: %s" % d["expected_completion"])
                if d["developer"]:
                    print("     Developer: %s" % d["developer"])

        # High-interest non-opportunities
        non_opps = [d for d in self.developments
                    if not d.get("is_opportunity") and d["relevance_score"] >= 30][:10]
        if non_opps:
            print("")
            print("-" * 70)
            print("  WATCH LIST (pharmacy nearby, but worth monitoring)")
            print("-" * 70)
            for i, d in enumerate(non_opps[:10], 1):
                print("")
                print("  %d. [Score: %.0f] %s" % (i, d["relevance_score"], d["name"][:65]))
                if d["nearest_pharmacy_km"] is not None:
                    print("     Nearest pharmacy: %.2f km" % d["nearest_pharmacy_km"])
                print("     Type: %-20s Source: %s" % (d["dev_type"], d["source_name"]))

        if not self.developments:
            print("  No developments found. Check your internet connection.")

        print("")
        print("=" * 70)
        print("  Database:  %s" % DB_PATH)
        print("  JSON:      %s" % OUTPUT_JSON)
        print("  CSV:       %s" % OUTPUT_CSV)
        print("  Log:       %s" % LOG_FILE)
        print("=" * 70)
        print("")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    scanner = DevelopmentScanner()
    scanner.run_all_scans()
    scanner.save_to_db()
    scanner.save_to_files()
    scanner.print_summary()
    return 0


if __name__ == "__main__":
    sys.exit(main())
