"""
New Retail Development Scanner for PharmacyFinder
==================================================
Finds upcoming/new shopping centres, Coles, Woolworths, ALDI, and Costco openings
across Australia. These represent greenfield pharmacy opportunities before they exist.

Sources:
  1. PlanningAlerts.org.au API (development applications - requires API key)
  2. Web scraping: Shopping Centre News, ALDI new stores, Coles/Woolworths media,
     The Urban Developer, Costco
  3. Google News RSS for retail development announcements

For each development found:
  - Geocodes the location via Nominatim
  - Checks proximity to existing pharmacies in the DB
  - Flags as opportunity if no pharmacy within 1.5km
  - Calculates priority score

Output:
  - output/development_opportunities.json
  - output/development_opportunities.csv
  - Formatted console summary
"""

import json
import csv
import os
import re
import sys
import time
import math
import sqlite3
import hashlib
import logging
from datetime import datetime, timedelta
from urllib.parse import quote_plus, urlencode

import requests
from bs4 import BeautifulSoup

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
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
HEADERS = {"User-Agent": USER_AGENT}
REQUEST_TIMEOUT = 15
RATE_LIMIT = 1.5  # seconds between requests

# PlanningAlerts (requires API key - get free community plan at planningalerts.org.au)
PLANNING_ALERTS_ENDPOINT = "https://api.planningalerts.org.au/applications.json"
PLANNING_ALERTS_KEY = os.environ.get("PLANNING_ALERTS_KEY", "")

# Keywords for DA search
DA_KEYWORDS = [
    "supermarket", "shopping centre", "woolworths", "coles",
    "aldi", "costco", "retail development", "pharmacy",
]

# High-growth postcodes for PlanningAlerts search
HIGH_GROWTH_POSTCODES = [
    # NSW growth corridors
    "2765", "2763", "2761", "2557", "2570", "2560", "2155", "2756",
    # VIC growth corridors
    "3029", "3030", "3977", "3754", "3064", "3427", "3750",
    # QLD growth corridors
    "4209", "4509", "4740", "4300", "4207", "4510",
    # WA
    "6108", "6164", "6112", "6027", "6065",
    # SA
    "5114", "5162", "5095",
]

# Google News RSS queries
NEWS_QUERIES = [
    "new woolworths opening australia",
    "new coles opening australia",
    "new shopping centre australia",
    "new aldi store australia",
    "new costco australia",
    "retail development australia new supermarket",
]

# Pharmacy proximity threshold (km)
OPPORTUNITY_DISTANCE_KM = 1.5

# Priority scoring weights by brand/type
BRAND_SCORES = {
    "woolworths": 30,
    "coles": 30,
    "aldi": 25,
    "costco": 20,
    "shopping_centre": 35,
    "retail_development": 15,
    "pharmacy": 10,
    "supermarket": 20,
    "unknown": 10,
}

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("dev_scanner")


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
        resp = requests.get(url, params=params, headers=hdrs, timeout=timeout)
        time.sleep(RATE_LIMIT)
        if resp.status_code == 200:
            return resp
        log.warning("HTTP %d for %s", resp.status_code, url)
        return None
    except requests.RequestException as exc:
        log.warning("Request failed for %s: %s", url, exc)
        return None


def geocode_address(address):
    """Geocode an address using Nominatim. Returns (lat, lon) or (None, None)."""
    if not address or len(address.strip()) < 5:
        return None, None
    # Clean up the address for better geocoding
    clean = address.strip()
    if not any(w in clean.lower() for w in ["australia", "nsw", "vic", "qld", "wa", "sa", "tas", "nt", "act"]):
        clean += ", Australia"
    url = "https://nominatim.openstreetmap.org/search"
    params = {
        "q": clean,
        "format": "json",
        "countrycodes": "au",
        "limit": 1,
    }
    resp = safe_get(url, params=params,
                    headers={"User-Agent": "PharmacyFinder-DevScanner/1.0"},
                    timeout=10)
    if resp:
        try:
            data = resp.json()
            if data:
                return float(data[0]["lat"]), float(data[0]["lon"])
        except (ValueError, KeyError, IndexError):
            pass
    return None, None


def make_development_id(name, address, source):
    """Create a deterministic hash ID for a development."""
    raw = f"{name}|{address}|{source}".lower().strip()
    return hashlib.md5(raw.encode()).hexdigest()[:12]


def classify_development(text):
    """Classify a development based on its description text."""
    t = (text or "").lower()
    if "costco" in t:
        return "costco"
    if "woolworths" in t or "woolies" in t:
        return "woolworths"
    if "coles" in t:
        return "coles"
    if "aldi" in t:
        return "aldi"
    if any(w in t for w in ["shopping centre", "shopping center", "retail precinct", "town centre"]):
        return "shopping_centre"
    if "supermarket" in t:
        return "supermarket"
    if "pharmacy" in t or "chemist" in t:
        return "pharmacy"
    if "retail" in t:
        return "retail_development"
    return "unknown"


def extract_opening_date(text):
    """Try to extract an opening/completion date from text."""
    if not text:
        return None
    patterns = [
        r"(open(?:ing|s)?|complet(?:ed|ion|e)|due|expected|ready)\s+(?:in\s+|by\s+)?(?:late\s+|early\s+|mid[\s-]*)?(Q[1-4]\s+)?(\d{4})",
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
        r"(?:developed?\s+by|developer[:\s]+|proponent[:\s]+|applicant[:\s]+)"
        r"([A-Z][A-Za-z\s&]+?)(?:\.|,|\s+(?:has|is|will|for|at|in|on))",
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            return m.group(1).strip()
    return None


def extract_location_from_title(title):
    """Try to extract a suburb/location from an article title."""
    if not title:
        return None
    # "... in Suburb Name" or "... at Suburb Name"
    m = re.search(
        r"(?:in|at|for|opens?\s+(?:in|at))\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,3})",
        title
    )
    if m:
        loc = m.group(1).strip()
        # Filter out generic words
        skip = {"Australia", "New", "The", "This", "That", "First", "South",
                "North", "East", "West", "Regional"}
        if loc not in skip:
            return loc + ", Australia"
    return None


# ---------------------------------------------------------------------------
# Scanner Class
# ---------------------------------------------------------------------------

class DevelopmentScanner:
    """Scans multiple sources for new retail developments in Australia."""

    def __init__(self):
        self.developments = []
        self.pharmacies = []
        self._load_pharmacies()

    def _load_pharmacies(self):
        """Load all pharmacies from the database."""
        log.info("Loading pharmacies from database...")
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT name, latitude, longitude, address FROM pharmacies")
        self.pharmacies = [
            {"name": r[0], "lat": r[1], "lng": r[2], "address": r[3]}
            for r in cur.fetchall()
        ]
        conn.close()
        log.info("Loaded %d pharmacies", len(self.pharmacies))

    def nearest_pharmacy(self, lat, lng):
        """Find nearest pharmacy and return (distance_km, name)."""
        if not lat or not lng or not self.pharmacies:
            return None, None
        best_dist = float("inf")
        best_name = None
        for ph in self.pharmacies:
            d = haversine_km(lat, lng, ph["lat"], ph["lng"])
            if d < best_dist:
                best_dist = d
                best_name = ph["name"]
        return round(best_dist, 2), best_name

    def add_development(self, name, address, dev_type, source, source_url,
                        description="", developer=None, opening_date=None,
                        latitude=None, longitude=None):
        """Add a discovered development to the list (dedup by ID)."""
        if not name or len(name.strip()) < 3:
            return
        dev_id = make_development_id(name, address or "", source)

        # Skip duplicates
        if any(d["id"] == dev_id for d in self.developments):
            return

        self.developments.append({
            "id": dev_id,
            "name": name.strip(),
            "address": (address or "").strip(),
            "type": dev_type,
            "source": source,
            "source_url": source_url,
            "description": (description or "")[:500],
            "developer": developer,
            "opening_date": opening_date,
            "latitude": latitude,
            "longitude": longitude,
            "nearest_pharmacy_km": None,
            "nearest_pharmacy_name": None,
            "is_opportunity": False,
            "priority_score": 0,
            "date_scanned": datetime.now().isoformat(),
        })

    # ------------------------------------------------------------------
    # Source 1: PlanningAlerts.org.au
    # ------------------------------------------------------------------

    def scan_planning_alerts(self):
        """Search PlanningAlerts for retail development applications."""
        log.info("=== Scanning PlanningAlerts.org.au ===")

        if not PLANNING_ALERTS_KEY:
            log.warning(
                "No PLANNING_ALERTS_KEY env var set -- skipping PlanningAlerts. "
                "Get a free community API key at https://www.planningalerts.org.au/api/howto"
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

            applications = data if isinstance(data, list) else data.get("application", data.get("applications", []))
            if isinstance(applications, dict):
                applications = [applications]

            for app_wrapper in applications:
                app = app_wrapper.get("application", app_wrapper) if isinstance(app_wrapper, dict) else app_wrapper
                if not isinstance(app, dict):
                    continue

                desc = (app.get("description") or "").lower()
                address = app.get("address") or ""

                # Filter: only retail-related DAs
                if not any(kw in desc for kw in DA_KEYWORDS):
                    continue

                dev_type = classify_development(desc)
                name = (app.get("description") or "Unknown DA")[:200]
                info_url = app.get("info_url") or app.get("comment_url") or ""
                lat = app.get("lat")
                lng = app.get("lng")

                self.add_development(
                    name=name,
                    address=address,
                    dev_type=dev_type,
                    source="PlanningAlerts",
                    source_url=info_url,
                    description=app.get("description", ""),
                    developer=extract_developer(desc) or app.get("authority", {}).get("full_name"),
                    opening_date=extract_opening_date(desc),
                    latitude=float(lat) if lat else None,
                    longitude=float(lng) if lng else None,
                )
                found += 1

        log.info("PlanningAlerts: found %d relevant applications", found)

    # ------------------------------------------------------------------
    # Source 2a: ALDI New Stores
    # ------------------------------------------------------------------

    def scan_aldi_new_stores(self):
        """Scrape ALDI Australia new stores page."""
        log.info("=== Scanning ALDI new stores ===")
        # ALDI redirects to a JS-rendered page; try multiple known URLs
        urls_to_try = [
            "https://www.aldi.com.au/stores/new-stores/",
            "https://www.aldi.com.au/storelocator/new-stores",
            "https://www.aldi.com.au/about-aldi/new-stores/",
        ]
        found = 0

        for url in urls_to_try:
            resp = safe_get(url)
            if not resp:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            text_body = soup.get_text(" ", strip=True)

            # ALDI new stores page is often JS-rendered; parse what we can
            current_state = ""

            for el in soup.find_all(["h2", "h3", "h4", "p", "li", "span", "div"]):
                text = el.get_text(strip=True)
                if not text or len(text) < 5 or len(text) > 300:
                    continue

                # State headers
                states = ["New South Wales", "Victoria", "Queensland",
                          "South Australia", "Western Australia", "Tasmania"]
                for st in states:
                    if st.lower() in text.lower() and len(text) < 40:
                        current_state = st
                        break

                # Address patterns
                if re.search(r"\d+\s+\w+\s+(street|road|drive|avenue|highway|parade|way|boulevard|lane)",
                             text, re.I):
                    store_name = "ALDI " + text.split(",")[0].strip()[:60]
                    address = text
                    if current_state and current_state.lower() not in address.lower():
                        address += ", " + current_state

                    self.add_development(
                        name=store_name,
                        address=address,
                        dev_type="aldi",
                        source="ALDI Website",
                        source_url=url,
                        description="New ALDI store: " + text,
                        opening_date=extract_opening_date(text),
                    )
                    found += 1

                # "Opening soon" style entries
                elif (re.search(r"(opening|coming soon|now open|new store)", text, re.I)
                      and len(text) < 150 and current_state):
                    self.add_development(
                        name="ALDI " + text.split("-")[0].split("(")[0].strip()[:60],
                        address=text + ", " + current_state + ", Australia",
                        dev_type="aldi",
                        source="ALDI Website",
                        source_url=url,
                        description=text,
                        opening_date=extract_opening_date(text),
                    )
                    found += 1

            if found > 0:
                break  # Got results from one URL, skip others

        log.info("ALDI new stores: found %d entries", found)

    # ------------------------------------------------------------------
    # Source 2b: Shopping Centre News
    # ------------------------------------------------------------------

    def _fetch_scn_article(self, url):
        """Fetch a Shopping Centre News article and extract details."""
        resp = safe_get(url, timeout=10)
        if not resp:
            return None
        soup = BeautifulSoup(resp.text, "html.parser")
        # Get article body
        body = soup.select_one("article, .entry-content, .post-content, main")
        if body:
            return body.get_text(" ", strip=True)[:2000]
        return soup.get_text(" ", strip=True)[:2000]

    def scan_shopping_centre_news(self):
        """Scrape Shopping Centre News for development articles."""
        log.info("=== Scanning Shopping Centre News ===")
        # Multiple pages to get more coverage
        urls = [
            "https://shoppingcentrenews.com.au/category/latest-news/",
            "https://shoppingcentrenews.com.au/category/latest-news/page/2/",
            "https://shoppingcentrenews.com.au/category/latest-news/industry-news/",
        ]
        found = 0
        articles_to_fetch = []

        for page_url in urls:
            resp = safe_get(page_url)
            if not resp:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            # Find article links - SCN uses h3 > a pattern
            link_tags = soup.select("h3 a[href], h2 a[href], .entry-title a[href]")
            if not link_tags:
                link_tags = soup.find_all("a", href=True)

            seen_urls = set()
            for a_tag in link_tags:
                href = a_tag.get("href", "")
                title = a_tag.get_text(strip=True)

                if not href or not title or len(title) < 15:
                    continue
                if href in seen_urls:
                    continue
                if not href.startswith("http"):
                    href = "https://shoppingcentrenews.com.au" + href

                # Filter for development/new store articles
                title_lower = title.lower()
                keywords = ["new", "open", "develop", "construct", "plan", "expand",
                            "build", "centre", "retail", "supermarket", "coles",
                            "woolworths", "aldi", "costco", "anchor", "redevelop",
                            "village", "shopping"]
                if not any(kw in title_lower for kw in keywords):
                    continue

                seen_urls.add(href)
                articles_to_fetch.append((title, href))

                if len(articles_to_fetch) >= 30:
                    break

        # Fetch top articles for better location/developer extraction
        log.info("  Fetching %d SCN article details...", min(len(articles_to_fetch), 15))
        for title, href in articles_to_fetch:
            # Fetch article body for top articles to get address/developer
            article_text = None
            if found < 15:  # Only fetch detail for first 15 to respect rate limits
                article_text = self._fetch_scn_article(href)

            full_text = (title + " " + (article_text or ""))
            dev_type = classify_development(full_text)
            address = extract_location_from_title(title)

            # Try to extract suburb from article body
            if not address and article_text:
                # Look for "located in SUBURB" or "in the SUBURB suburb"
                m = re.search(
                    r"(?:located\s+in(?:\s+the)?|in\s+the\s+(?:fast-growing|growing|new|outer)\s+)"
                    r"(?:Melbourne|Sydney|Brisbane|Perth|Adelaide|Hobart|Darwin)?\s*"
                    r"(?:suburb\s+of\s+)?([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)",
                    article_text
                )
                if m:
                    address = m.group(1) + ", Australia"
                else:
                    # Try "on the corner of X and Y" or "at ADDRESS"
                    m2 = re.search(r"(?:corner of|located at)\s+([^.]{10,60})", article_text)
                    if m2:
                        address = m2.group(1).strip() + ", Australia"

            developer = extract_developer(article_text) if article_text else None
            opening = extract_opening_date(article_text or title)

            self.add_development(
                name=title[:200],
                address=address or "",
                dev_type=dev_type,
                source="Shopping Centre News",
                source_url=href,
                description=(article_text or title)[:500],
                opening_date=opening,
                developer=developer,
            )
            found += 1

        log.info("Shopping Centre News: found %d articles", found)

    # ------------------------------------------------------------------
    # Source 2c: Coles Group media
    # ------------------------------------------------------------------

    def scan_coles_media(self):
        """Check Coles Group news page for new store announcements."""
        log.info("=== Scanning Coles Group news ===")
        url = "https://www.colesgroup.com.au/news/"
        found = 0

        resp = safe_get(url)
        if not resp:
            log.warning("Could not fetch Coles news page")
            return

        soup = BeautifulSoup(resp.text, "html.parser")
        articles = soup.find_all("a", href=True)

        seen_urls = set()
        for a_tag in articles:
            href = a_tag.get("href", "")
            title = a_tag.get_text(strip=True)

            # Filter junk: too short, no letters, or generic nav text
            if not title or len(title) < 20:
                continue
            if title.lower().startswith("read more"):
                continue
            if href in seen_urls:
                continue

            title_lower = title.lower()
            # Must contain store-related keywords
            store_kws = ["new store", "store open", "store launch", "coles local",
                         "store arrives", "first coles", "new coles"]
            if not any(kw in title_lower for kw in store_kws):
                continue

            if not href.startswith("http"):
                href = "https://www.colesgroup.com.au" + href

            seen_urls.add(href)
            address = extract_location_from_title(title)

            self.add_development(
                name=title[:200],
                address=address or "",
                dev_type="coles",
                source="Coles Group",
                source_url=href,
                description=title,
                opening_date=extract_opening_date(title),
            )
            found += 1

        log.info("Coles Group: found %d articles", found)

    # ------------------------------------------------------------------
    # Source 2d: Woolworths Group media
    # ------------------------------------------------------------------

    def scan_woolworths_media(self):
        """Check Woolworths Group media pages for new store announcements."""
        log.info("=== Scanning Woolworths Group news ===")
        urls = [
            "https://www.woolworthsgroup.com.au/au/en/our-newsroom.html",
            "https://www.woolworthsgroup.com.au/au/en/media/latest-news.html",
        ]
        found = 0

        for page_url in urls:
            resp = safe_get(page_url)
            if not resp:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            articles = soup.find_all("a", href=True)

            for a_tag in articles:
                href = a_tag.get("href", "")
                title = a_tag.get_text(strip=True)

                if not title or len(title) < 15:
                    continue

                title_lower = title.lower()
                if not any(kw in title_lower for kw in
                           ["new store", "open", "launch", "expand", "metro",
                            "woolworths metro", "first"]):
                    continue

                if not href.startswith("http"):
                    href = "https://www.woolworthsgroup.com.au" + href

                address = extract_location_from_title(title)

                self.add_development(
                    name=title[:200],
                    address=address or "",
                    dev_type="woolworths",
                    source="Woolworths Group",
                    source_url=href,
                    description=title,
                    opening_date=extract_opening_date(title),
                )
                found += 1

        log.info("Woolworths Group: found %d articles", found)

    # ------------------------------------------------------------------
    # Source 2e: Costco expansion
    # ------------------------------------------------------------------

    def scan_costco(self):
        """Search Google News for Costco Australia expansion (website is JS-rendered)."""
        log.info("=== Scanning Costco Australia (via news search) ===")
        found = 0

        # Costco's site is fully JS-rendered; use Google News RSS instead
        url = "https://news.google.com/rss/search"
        params = {
            "q": "costco new warehouse australia opening",
            "hl": "en-AU",
            "gl": "AU",
            "ceid": "AU:en",
        }
        resp = safe_get(url, params=params)
        if not resp:
            log.warning("Could not fetch Costco news")
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

            address = extract_location_from_title(title)

            self.add_development(
                name=title[:200],
                address=address or "",
                dev_type="costco",
                source="Google News (Costco)",
                source_url=link_text,
                description=title,
                opening_date=extract_opening_date(title),
            )
            found += 1

        log.info("Costco news: found %d articles", found)

    # ------------------------------------------------------------------
    # Source 2f: The Urban Developer
    # ------------------------------------------------------------------

    def scan_urban_developer(self):
        """Scrape The Urban Developer for retail development news."""
        log.info("=== Scanning The Urban Developer ===")
        # Main page has article links
        url = "https://www.theurbandeveloper.com/"
        found = 0

        resp = safe_get(url)
        if not resp:
            log.warning("Could not fetch The Urban Developer")
            return

        soup = BeautifulSoup(resp.text, "html.parser")

        # Find article links
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
            retail_kws = ["retail", "shopping", "supermarket", "coles", "woolworths",
                          "aldi", "costco", "centre", "mall", "precinct",
                          "commercial", "mixed-use", "mixed use"]
            if not any(kw in title_lower for kw in retail_kws):
                continue

            seen.add(href)
            if not href.startswith("http"):
                href = "https://www.theurbandeveloper.com" + href

            dev_type = classify_development(title)
            address = extract_location_from_title(title)

            self.add_development(
                name=title[:200],
                address=address or "",
                dev_type=dev_type,
                source="The Urban Developer",
                source_url=href,
                description=title,
                opening_date=extract_opening_date(title),
                developer=extract_developer(title),
            )
            found += 1

            if found >= 20:
                break

        log.info("The Urban Developer: found %d articles", found)

    # ------------------------------------------------------------------
    # Source 3: Google News RSS search
    # ------------------------------------------------------------------

    def scan_google_news(self):
        """Search Google News RSS for recent retail development articles."""
        log.info("=== Scanning Google News RSS ===")
        found = 0

        for query in NEWS_QUERIES:
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

            for item in items[:8]:  # Top 8 per query
                title_el = item.find("title")
                link_el = item.find("link")
                pub_date_el = item.find("pubDate")

                if not title_el:
                    continue

                title = title_el.get_text(strip=True)
                link_text = ""
                if link_el:
                    link_text = link_el.get_text(strip=True)
                    if not link_text and link_el.next_sibling:
                        link_text = str(link_el.next_sibling).strip()

                # Skip if too old (>30 days)
                if pub_date_el:
                    try:
                        from email.utils import parsedate_to_datetime
                        pub_dt = parsedate_to_datetime(pub_date_el.get_text(strip=True))
                        if pub_dt.replace(tzinfo=None) < datetime.now() - timedelta(days=30):
                            continue
                    except Exception:
                        pass

                dev_type = classify_development(title)
                # Only keep if we can classify it
                if dev_type == "unknown":
                    # Check if title at least mentions relevant terms
                    t = title.lower()
                    if not any(w in t for w in ["store", "shop", "retail", "centre",
                                                "supermarket", "pharmacy", "development"]):
                        continue

                address = extract_location_from_title(title)

                self.add_development(
                    name=title[:200],
                    address=address or "",
                    dev_type=dev_type,
                    source="Google News",
                    source_url=link_text,
                    description=title,
                    opening_date=extract_opening_date(title),
                )
                found += 1

        log.info("Google News: found %d articles", found)

    # ------------------------------------------------------------------
    # Processing
    # ------------------------------------------------------------------

    def geocode_developments(self):
        """Geocode all developments that don't have coordinates."""
        log.info("=== Geocoding developments ===")
        need_geocode = [d for d in self.developments
                        if not d["latitude"] and d["address"]]
        log.info("Need to geocode: %d developments", len(need_geocode))

        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()

        geocoded = 0
        cache_hits = 0
        for i, dev in enumerate(need_geocode):
            # Check cache first
            cur.execute("SELECT latitude, longitude FROM geocode_cache WHERE address = ?",
                        (dev["address"],))
            row = cur.fetchone()
            if row:
                dev["latitude"], dev["longitude"] = row[0], row[1]
                cache_hits += 1
                continue

            lat, lon = geocode_address(dev["address"])
            if lat and lon:
                dev["latitude"] = lat
                dev["longitude"] = lon
                geocoded += 1
                # Cache it
                try:
                    cur.execute(
                        "INSERT OR IGNORE INTO geocode_cache "
                        "(address, latitude, longitude, date_cached) VALUES (?, ?, ?, ?)",
                        (dev["address"], lat, lon, datetime.now().isoformat())
                    )
                    conn.commit()
                except Exception:
                    pass

            if (i + 1) % 10 == 0:
                log.info("  Geocoded %d/%d ...", i + 1, len(need_geocode))

        conn.close()

        total_with_coords = sum(1 for d in self.developments if d["latitude"])
        log.info("Geocoding done: %d from API, %d from cache, %d total with coordinates",
                 geocoded, cache_hits, total_with_coords)

    def check_pharmacy_proximity(self):
        """Check each development against existing pharmacy locations."""
        log.info("=== Checking pharmacy proximity ===")
        opportunities = 0

        for dev in self.developments:
            if not dev["latitude"] or not dev["longitude"]:
                continue

            dist_km, nearest_name = self.nearest_pharmacy(dev["latitude"], dev["longitude"])
            dev["nearest_pharmacy_km"] = dist_km
            dev["nearest_pharmacy_name"] = nearest_name

            if dist_km is not None and dist_km >= OPPORTUNITY_DISTANCE_KM:
                dev["is_opportunity"] = True
                opportunities += 1

        log.info("Found %d developments with no pharmacy within %.1f km",
                 opportunities, OPPORTUNITY_DISTANCE_KM)

    def calculate_priority_scores(self):
        """Calculate a priority score for each development."""
        log.info("=== Calculating priority scores ===")
        for dev in self.developments:
            score = 0

            # Brand/type score
            score += BRAND_SCORES.get(dev["type"], BRAND_SCORES["unknown"])

            # Distance score: further from pharmacy = higher opportunity
            dist = dev["nearest_pharmacy_km"]
            if dist is not None:
                if dist >= 5.0:
                    score += 40
                elif dist >= 3.0:
                    score += 30
                elif dist >= 1.5:
                    score += 20
                elif dist >= 1.0:
                    score += 10
                elif dist < 0.5:
                    score -= 10

            # Has geocoded location (more actionable)
            if dev["latitude"] and dev["longitude"]:
                score += 10

            # Has opening date (more concrete)
            if dev["opening_date"]:
                score += 5

            # Is a flagged opportunity
            if dev["is_opportunity"]:
                score += 20

            dev["priority_score"] = max(0, score)

        # Sort by priority descending
        self.developments.sort(key=lambda d: d["priority_score"], reverse=True)

    # ------------------------------------------------------------------
    # Run All Scans
    # ------------------------------------------------------------------

    def run_all_scans(self):
        """Execute all scanning sources."""
        log.info("=" * 60)
        log.info("  RETAIL DEVELOPMENT SCANNER - Starting")
        log.info("=" * 60)

        scanners = [
            ("PlanningAlerts", self.scan_planning_alerts),
            ("ALDI", self.scan_aldi_new_stores),
            ("Shopping Centre News", self.scan_shopping_centre_news),
            ("Coles", self.scan_coles_media),
            ("Woolworths", self.scan_woolworths_media),
            ("Costco", self.scan_costco),
            ("Urban Developer", self.scan_urban_developer),
            ("Google News", self.scan_google_news),
        ]

        for name, fn in scanners:
            try:
                fn()
            except Exception as e:
                log.error("%s scan failed: %s", name, e)

        log.info("")
        log.info("Total raw developments found: %d", len(self.developments))

        # Post-processing
        self.geocode_developments()
        self.check_pharmacy_proximity()
        self.calculate_priority_scores()

    # ------------------------------------------------------------------
    # Output
    # ------------------------------------------------------------------

    def save_results(self):
        """Save results to JSON and CSV."""
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        # JSON
        with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
            json.dump({
                "scan_date": datetime.now().isoformat(),
                "total_developments": len(self.developments),
                "total_opportunities": sum(1 for d in self.developments if d["is_opportunity"]),
                "developments": self.developments,
            }, f, indent=2, ensure_ascii=False)
        log.info("Saved JSON: %s", OUTPUT_JSON)

        # CSV
        csv_fields = [
            "id", "name", "address", "type", "source", "source_url",
            "developer", "opening_date", "latitude", "longitude",
            "nearest_pharmacy_km", "nearest_pharmacy_name",
            "is_opportunity", "priority_score", "date_scanned",
        ]
        with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=csv_fields, extrasaction="ignore")
            writer.writeheader()
            for dev in self.developments:
                writer.writerow(dev)
        log.info("Saved CSV: %s", OUTPUT_CSV)

    def print_summary(self):
        """Print a formatted summary to console."""
        total = len(self.developments)
        opps = [d for d in self.developments if d["is_opportunity"]]
        geocoded = sum(1 for d in self.developments if d["latitude"] and d["longitude"])

        print("")
        print("=" * 70)
        print("  RETAIL DEVELOPMENT SCANNER - RESULTS SUMMARY")
        print("=" * 70)
        print("")
        print("  Scan Date: %s" % datetime.now().strftime("%Y-%m-%d %H:%M"))
        print("  Pharmacies in DB: %d" % len(self.pharmacies))
        print("")
        print("  Total Developments Found: %d" % total)
        print("  Geocoded: %d" % geocoded)
        print("  Flagged Opportunities (>%.1fkm from pharmacy): %d" % (
            OPPORTUNITY_DISTANCE_KM, len(opps)))
        print("")

        # By source
        sources = {}
        for d in self.developments:
            sources[d["source"]] = sources.get(d["source"], 0) + 1
        if sources:
            print("  By Source:")
            for src, cnt in sorted(sources.items(), key=lambda x: -x[1]):
                print("    %-30s %d" % (src, cnt))
            print("")

        # By type
        types = {}
        for d in self.developments:
            types[d["type"]] = types.get(d["type"], 0) + 1
        if types:
            print("  By Type:")
            for tp, cnt in sorted(types.items(), key=lambda x: -x[1]):
                print("    %-30s %d" % (tp, cnt))
            print("")

        # Top opportunities
        if opps:
            print("-" * 70)
            print("  TOP GREENFIELD OPPORTUNITIES (no pharmacy within %.1f km)" % OPPORTUNITY_DISTANCE_KM)
            print("-" * 70)
            for i, d in enumerate(opps[:20], 1):
                print("")
                print("  %d. [Score: %d] %s" % (i, d["priority_score"], d["name"][:70]))
                if d["address"]:
                    print("     Address: %s" % d["address"][:70])
                print("     Type: %-20s Source: %s" % (d["type"], d["source"]))
                if d["nearest_pharmacy_km"] is not None:
                    print("     Nearest pharmacy: %.2f km (%s)" % (
                        d["nearest_pharmacy_km"], d["nearest_pharmacy_name"] or "unknown"))
                if d["opening_date"]:
                    print("     Opening: %s" % d["opening_date"])
                if d["developer"]:
                    print("     Developer: %s" % d["developer"])
                if d["source_url"]:
                    url_display = d["source_url"][:80]
                    print("     URL: %s" % url_display)

        # High-interest non-opportunity developments
        non_opps = [d for d in self.developments
                    if not d["is_opportunity"] and d["priority_score"] >= 25]
        if non_opps:
            print("")
            print("-" * 70)
            print("  HIGH-INTEREST DEVELOPMENTS (pharmacy nearby, but worth watching)")
            print("-" * 70)
            for i, d in enumerate(non_opps[:10], 1):
                print("")
                print("  %d. [Score: %d] %s" % (i, d["priority_score"], d["name"][:70]))
                if d["nearest_pharmacy_km"] is not None:
                    print("     Nearest pharmacy: %.2f km" % d["nearest_pharmacy_km"])
                print("     Type: %-20s Source: %s" % (d["type"], d["source"]))

        if not opps and not non_opps and total == 0:
            print("  No developments found. Check your internet connection and try again.")

        print("")
        print("=" * 70)
        print("  Full results: %s" % OUTPUT_JSON)
        print("  CSV summary:  %s" % OUTPUT_CSV)
        print("=" * 70)
        print("")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    scanner = DevelopmentScanner()
    scanner.run_all_scans()
    scanner.save_results()
    scanner.print_summary()
    return 0


if __name__ == "__main__":
    sys.exit(main())
