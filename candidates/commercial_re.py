"""
commercial_re.py - Scrape commercialrealestate.com.au for available retail/medical leases
near qualifying pharmacy sites.

Uses Playwright (headless Chromium) to load pages and extract listing data from
the Apollo GraphQL state embedded in the page.

Caches results per suburb to avoid redundant scraping.
Rate limited: 2 seconds between requests.
"""

import sqlite3
import json
import time
import re
import hashlib
from datetime import datetime
from pathlib import Path
from math import radians, sin, cos, sqrt, atan2
from typing import Optional

try:
    from playwright.sync_api import sync_playwright
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False

try:
    from geopy.distance import geodesic
    HAS_GEOPY = True
except ImportError:
    HAS_GEOPY = False


DB_PATH = Path(__file__).parent.parent / "pharmacy_finder.db"
CACHE_DIR = Path(__file__).parent.parent / "cache" / "commercial_re"
RATE_LIMIT_SECS = 2.0
BASE_URL = "https://www.commercialrealestate.com.au"

# Property type categories on CRE
# retail = 188, offices = 193, medical = 195 (medicalConsulting)
CATEGORY_SLUGS = {
    "retail": "retail",
    "medical": "medical-consulting",
    "office": "offices",
}


def _geodesic_distance_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate geodesic distance in meters between two points."""
    if HAS_GEOPY:
        return geodesic((lat1, lon1), (lat2, lon2)).meters
    R = 6371000
    phi1, phi2 = radians(lat1), radians(lat2)
    dphi = radians(lat2 - lat1)
    dlambda = radians(lon2 - lon1)
    a = sin(dphi / 2) ** 2 + cos(phi1) * cos(phi2) * sin(dlambda / 2) ** 2
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))


def _suburb_from_address(address: str) -> tuple[str, str, str]:
    """Extract suburb, state and postcode from an address string.
    Returns (suburb, state, postcode) e.g. ('bankstown', 'nsw', '2200')
    """
    if not address:
        return ("", "", "")

    state_codes = {"NSW", "VIC", "QLD", "SA", "WA", "TAS", "NT", "ACT"}
    parts = [p.strip() for p in address.split(",")]

    state = ""
    postcode = ""

    # Find state and postcode
    for part in reversed(parts):
        tokens = part.strip().split()
        for token in tokens:
            if token.upper() in state_codes and not state:
                state = token.upper()
            if re.match(r'^\d{4}$', token) and not postcode:
                postcode = token

    # Remove "Australia", state, postcode parts to find suburb
    clean_parts = []
    for p in parts:
        p_stripped = p.strip()
        if p_stripped.upper() == "AUSTRALIA":
            continue
        tokens = p_stripped.split()
        non_meta = [t for t in tokens if t.upper() not in state_codes and not re.match(r'^\d{4}$', t)]
        if not non_meta:
            continue
        # If this part had state/postcode mixed in, extract suburb part
        if any(t.upper() in state_codes for t in tokens) or any(re.match(r'^\d{4}$', t) for t in tokens):
            suburb_part = " ".join(non_meta)
            if suburb_part:
                clean_parts.append(suburb_part)
        else:
            clean_parts.append(p_stripped)

    # Find suburb: last part that doesn't look like a street address
    street_suffixes = {"street", "st", "road", "rd", "avenue", "ave", "drive", "dr",
                       "place", "pl", "lane", "ln", "way", "crescent", "cres",
                       "boulevard", "blvd", "highway", "hwy", "parade", "pde",
                       "terrace", "tce", "court", "ct", "close", "cl"}

    suburb = ""
    if len(clean_parts) >= 2:
        for i in range(len(clean_parts) - 1, -1, -1):
            part_lower = clean_parts[i].lower()
            tokens_lower = part_lower.split()
            if tokens_lower and tokens_lower[-1] in street_suffixes:
                continue
            if tokens_lower and re.match(r'^\d', tokens_lower[0]) and len(tokens_lower) > 1:
                continue
            if "shopping centre" in part_lower or "shop " in part_lower.split(",")[0][:5]:
                if i < len(clean_parts) - 1:
                    suburb = clean_parts[i + 1]
                else:
                    suburb = clean_parts[i]
                break
            suburb = clean_parts[i]
            break
    elif clean_parts:
        suburb = clean_parts[0]

    return (suburb.strip().lower(), state.lower(), postcode)


def _build_search_url(suburb: str, state: str, postcode: str = "",
                      category: str = "retail", page: int = 1) -> str:
    """Build commercialrealestate.com.au search URL.
    
    Format: /for-lease/{suburb}-{state}-{postcode}/{category}/
    Example: /for-lease/hobart-tas-7000/retail/
    """
    slug_parts = [suburb.strip().replace(" ", "-")]
    if state:
        slug_parts.append(state.strip().lower())
    if postcode:
        slug_parts.append(postcode.strip())
    
    location_slug = "-".join(slug_parts)
    cat_slug = CATEGORY_SLUGS.get(category, "retail")
    
    url = f"{BASE_URL}/for-lease/{location_slug}/{cat_slug}/"
    if page > 1:
        url += f"?pn={page}"
    return url


def _cache_key(suburb: str, state: str) -> str:
    raw = f"{suburb.lower().strip()}_{state.lower().strip()}"
    return hashlib.md5(raw.encode()).hexdigest()


def _get_cached(suburb: str, state: str, max_age_hours: int = 24) -> Optional[list]:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file = CACHE_DIR / f"{_cache_key(suburb, state)}.json"
    if not cache_file.exists():
        return None
    try:
        data = json.loads(cache_file.read_text(encoding="utf-8"))
        cached_time = datetime.fromisoformat(data.get("cached_at", "2000-01-01"))
        age_hours = (datetime.now() - cached_time).total_seconds() / 3600
        if age_hours < max_age_hours:
            return data.get("listings", [])
    except (json.JSONDecodeError, ValueError):
        pass
    return None


def _save_cache(suburb: str, state: str, listings: list):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file = CACHE_DIR / f"{_cache_key(suburb, state)}.json"
    cache_file.write_text(
        json.dumps({
            "suburb": suburb,
            "state": state,
            "cached_at": datetime.now().isoformat(),
            "listings": listings,
        }, indent=2),
        encoding="utf-8",
    )


def _parse_rent(text: str) -> tuple[Optional[float], Optional[float]]:
    """Parse rent string into (annual_rent, rent_per_sqm)."""
    if not text:
        return (None, None)
    text = text.lower().replace(",", "").replace("$", "").strip()

    annual = None
    per_sqm = None

    sqm_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:per\s*sq\s*m|/sqm|psm|per\s*m2|per\s*sqm|sqm)', text)
    if sqm_match:
        per_sqm = float(sqm_match.group(1))

    pa_match = re.search(r'(\d+(?:\.\d+)?)\s*(?:pa|p\.a\.|per\s*annum|per\s*year)', text)
    if pa_match:
        annual = float(pa_match.group(1))

    if annual is None and per_sqm is None:
        num_match = re.search(r'(\d+(?:\.\d+)?)', text)
        if num_match:
            val = float(num_match.group(1))
            if val > 1000:
                annual = val
            elif val > 0:
                per_sqm = val

    return (annual, per_sqm)


def _parse_area(text: str) -> Optional[float]:
    """Parse floor area text into sqm value."""
    if not text:
        return None
    text = text.lower().replace(",", "")
    match = re.search(r'(\d+(?:\.\d+)?)\s*(?:m\xb2|sqm|m2|sq\s*m)', text)
    if match:
        return float(match.group(1))
    match = re.search(r'(\d+(?:\.\d+)?)', text)
    if match:
        val = float(match.group(1))
        if 10 <= val <= 10000:
            return val
    return None


def _classify_property_type(text: str) -> str:
    text = (text or "").lower()
    if any(w in text for w in ["medical", "health", "clinic", "consulting"]):
        return "medical"
    if any(w in text for w in ["retail", "shop", "store"]):
        return "retail"
    if any(w in text for w in ["office", "suite"]):
        return "office"
    return "retail"


def _resolve_ref(apollo: dict, ref_or_val) -> dict:
    """Resolve an Apollo __ref to its actual object."""
    if isinstance(ref_or_val, dict):
        if '__ref' in ref_or_val:
            return apollo.get(ref_or_val['__ref'], {})
        return ref_or_val
    return {}


def _extract_listings_from_apollo(apollo: dict) -> list[dict]:
    """Extract listing data from Apollo GraphQL state."""
    listings = []
    
    for key, val in apollo.items():
        if not isinstance(val, dict):
            continue
        if val.get('__typename') != 'PropertyListingType':
            continue
        
        ad_id = val.get('adID', '')
        seo_url = val.get('seoUrl', '')
        listing_url = f"{BASE_URL}{seo_url}" if seo_url else ""
        
        # Address
        address = (
            val.get('displayableAddress', '')
            or val.get('displayableLocationWithPostcode', '')
            or val.get('displayableStreet', '')
        )
        suburb = val.get('suburb', '')
        state = val.get('state', '')
        postcode = val.get('postcode', '')
        if suburb and not address:
            address = f"{suburb}, {state} {postcode}".strip()
        
        # Price
        price_text = val.get('displayablePrice', '')
        
        # Area
        area_text = val.get('area', '') or val.get('areaHeaderDisplay', '')
        
        # Property type
        prop_type_details = val.get('propertyTypeDetails', '')
        main_category = val.get('mainCategory', '')
        prop_type = _classify_property_type(f"{prop_type_details} {main_category}")
        
        # Map location (lat/lng)
        lat, lng = None, None
        map_loc = val.get('mapLocation')
        if map_loc:
            map_data = _resolve_ref(apollo, map_loc)
            lat = map_data.get('latitude')
            lng = map_data.get('longitude')
        
        # Headline for extra context
        headline = val.get('headline', '')
        
        # Agent info from highlights
        agent_name = ""
        agent_phone = ""
        # Check agency ref
        agency = val.get('agency')
        if agency:
            agency_data = _resolve_ref(apollo, agency)
            agent_name = agency_data.get('name', '')
        
        # Find contact details linked to this listing
        # ContactDetails are stored separately; we'll match by listing ad_id pattern
        contact_key_prefix = f"PropertyListingType:{ad_id}"
        for ck, cv in apollo.items():
            if isinstance(cv, dict) and cv.get('__typename') == 'ListingContactDetailType':
                if str(ad_id) in ck or contact_key_prefix in ck:
                    if not agent_name:
                        agent_name = cv.get('fullName', '')
                    break
        
        # Also check highlights for area/features
        highlights = val.get('highlights', [])
        if isinstance(highlights, list):
            for h in highlights:
                h_data = _resolve_ref(apollo, h) if isinstance(h, dict) else {}
                h_text = h_data.get('text', '') or h_data.get('label', '')
                if h_text and ('m' in h_text.lower() or 'sq' in h_text.lower()):
                    if not area_text:
                        area_text = h_text
        
        listing = {
            "ad_id": str(ad_id),
            "address": address,
            "rent_text": price_text,
            "area_text": str(area_text) if area_text else "",
            "property_type": prop_type,
            "listing_url": listing_url,
            "agent_name": agent_name,
            "agent_phone": agent_phone,
            "latitude": lat,
            "longitude": lng,
            "headline": headline,
        }
        listings.append(listing)
    
    # Also try to get agent contact details
    contacts = {}
    for key, val in apollo.items():
        if isinstance(val, dict) and val.get('__typename') == 'ListingContactDetailType':
            name = val.get('fullName', '')
            if name:
                contacts[key] = name
    
    return listings


def scrape_suburb_listings(suburb: str, state: str, postcode: str = "",
                           max_pages: int = 2) -> list[dict]:
    """Scrape commercialrealestate.com.au for lease listings in a suburb.
    
    Returns list of listing dicts.
    """
    cached = _get_cached(suburb, state)
    if cached is not None:
        print(f"  [cache hit] {suburb}, {state} ({len(cached)} listings)")
        return cached

    if not HAS_PLAYWRIGHT:
        print("  [skip] Playwright not available")
        return []

    all_listings = []
    print(f"  [scraping] {suburb}, {state}...")

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=True,
                args=['--disable-blink-features=AutomationControlled']
            )
            ctx = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
                viewport={"width": 1920, "height": 1080},
                locale="en-AU",
            )
            page = ctx.new_page()
            page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

            for page_num in range(1, max_pages + 1):
                # Try retail first, then medical
                for category in ["retail", "medical"]:
                    url = _build_search_url(suburb, state, postcode, category, page_num)
                    print(f"    {category} p{page_num}: {url}")

                    try:
                        page.goto(url, timeout=30000, wait_until="domcontentloaded")
                        time.sleep(4 + RATE_LIMIT_SECS)

                        # Check title for results
                        title = page.title()
                        if "not found" in title.lower() or "error" in title.lower():
                            print(f"      page not found")
                            continue

                        # Extract Apollo state
                        try:
                            apollo_json = page.evaluate("() => JSON.stringify(window.__APOLLO_STATE__ || {})")
                            apollo = json.loads(apollo_json)
                        except Exception:
                            print(f"      no Apollo state")
                            continue

                        listings = _extract_listings_from_apollo(apollo)
                        if listings:
                            all_listings.extend(listings)
                            print(f"      {len(listings)} listings extracted")
                        else:
                            print(f"      no listings found")

                    except Exception as e:
                        print(f"      error: {e}")

                    time.sleep(RATE_LIMIT_SECS)

            browser.close()

    except Exception as e:
        print(f"  [error] Scraping failed: {e}")

    # Deduplicate by ad_id or URL
    seen = set()
    unique = []
    for l in all_listings:
        key = l.get("ad_id") or l.get("listing_url", "")
        if key and key not in seen:
            seen.add(key)
            unique.append(l)
    all_listings = unique

    _save_cache(suburb, state, all_listings)
    print(f"  [done] {len(all_listings)} unique listings for {suburb}, {state}")
    return all_listings


def init_db(db_path: str = None):
    """Create the commercial_matches table if it doesn't exist."""
    db = db_path or str(DB_PATH)
    conn = sqlite3.connect(db, timeout=30)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS commercial_matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            site_id TEXT NOT NULL,
            listing_url TEXT NOT NULL,
            address TEXT,
            latitude REAL,
            longitude REAL,
            rent_annual REAL,
            rent_per_sqm REAL,
            floor_area_sqm REAL,
            lease_type TEXT,
            property_type TEXT,
            distance_to_site_m REAL,
            agent_name TEXT,
            agent_phone TEXT,
            discovered_date TEXT,
            last_checked TEXT,
            status TEXT DEFAULT 'active',
            UNIQUE(site_id, listing_url)
        )
    """)
    conn.commit()
    conn.close()


def get_qualifying_sites(db_path: str = None, state: str = None, top_n: int = None) -> list[dict]:
    """Get qualifying sites from v2_results table."""
    db = db_path or str(DB_PATH)
    conn = sqlite3.connect(db, timeout=30)
    conn.row_factory = sqlite3.Row

    query = "SELECT * FROM v2_results WHERE passed_any = 1"
    params = []

    if state:
        query += " AND UPPER(state) = UPPER(?)"
        params.append(state)

    query += " ORDER BY commercial_score DESC"

    if top_n:
        query += " LIMIT ?"
        params.append(top_n)

    rows = conn.execute(query, params).fetchall()
    sites = [dict(r) for r in rows]
    conn.close()
    return sites


def scan_site(site: dict, db_path: str = None) -> list[dict]:
    """Scan for commercial properties near a qualifying site."""
    db = db_path or str(DB_PATH)
    site_lat = site.get("latitude")
    site_lon = site.get("longitude")
    site_id = site.get("id")
    address = site.get("address", "")

    if not site_lat or not site_lon:
        print(f"  [skip] {site_id}: no coordinates")
        return []

    suburb, state, postcode = _suburb_from_address(address)
    if not suburb:
        print(f"  [skip] {site_id}: couldn't extract suburb from '{address}'")
        return []

    listings = scrape_suburb_listings(suburb, state, postcode)

    matches = []
    now = datetime.now().isoformat()

    for listing in listings:
        l_lat = listing.get("latitude")
        l_lon = listing.get("longitude")
        distance_m = None
        if l_lat and l_lon:
            distance_m = _geodesic_distance_m(site_lat, site_lon, l_lat, l_lon)
            if distance_m > 1500:
                continue

        annual, per_sqm = _parse_rent(listing.get("rent_text", ""))
        floor_area = _parse_area(listing.get("area_text", ""))

        if per_sqm and floor_area and not annual:
            annual = per_sqm * floor_area
        if annual and floor_area and not per_sqm:
            per_sqm = annual / floor_area

        match = {
            "site_id": site_id,
            "listing_url": listing.get("listing_url", ""),
            "address": listing.get("address", ""),
            "latitude": l_lat,
            "longitude": l_lon,
            "rent_annual": annual,
            "rent_per_sqm": per_sqm,
            "floor_area_sqm": floor_area,
            "lease_type": listing.get("property_type", "retail"),
            "property_type": listing.get("property_type", "retail"),
            "distance_to_site_m": distance_m,
            "agent_name": listing.get("agent_name", ""),
            "agent_phone": listing.get("agent_phone", ""),
            "discovered_date": now,
            "last_checked": now,
            "status": "active",
        }
        matches.append(match)

    return matches


def save_matches(matches: list[dict], db_path: str = None):
    """Save matches to the commercial_matches table."""
    if not matches:
        return
    db = db_path or str(DB_PATH)
    conn = sqlite3.connect(db, timeout=30)
    now = datetime.now().isoformat()

    for m in matches:
        conn.execute("""
            INSERT INTO commercial_matches
                (site_id, listing_url, address, latitude, longitude,
                 rent_annual, rent_per_sqm, floor_area_sqm, lease_type, property_type,
                 distance_to_site_m, agent_name, agent_phone,
                 discovered_date, last_checked, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(site_id, listing_url) DO UPDATE SET
                last_checked = ?,
                rent_annual = COALESCE(excluded.rent_annual, rent_annual),
                rent_per_sqm = COALESCE(excluded.rent_per_sqm, rent_per_sqm),
                floor_area_sqm = COALESCE(excluded.floor_area_sqm, floor_area_sqm),
                status = 'active'
        """, (
            m["site_id"], m["listing_url"], m["address"],
            m["latitude"], m["longitude"],
            m["rent_annual"], m["rent_per_sqm"], m["floor_area_sqm"],
            m["lease_type"], m["property_type"],
            m["distance_to_site_m"], m["agent_name"], m["agent_phone"],
            m["discovered_date"], m["last_checked"], m["status"],
            now,
        ))

    conn.commit()
    conn.close()


def run_scan(db_path: str = None, state: str = None, top_n: int = None) -> int:
    """Main entry: scan all qualifying sites for nearby commercial leases."""
    db = db_path or str(DB_PATH)
    init_db(db)

    sites = get_qualifying_sites(db, state=state, top_n=top_n)
    print(f"Found {len(sites)} qualifying sites" + (f" in {state}" if state else ""))

    total_matches = 0
    for i, site in enumerate(sites, 1):
        print(f"\n[{i}/{len(sites)}] {site['name']} -- {site['address']}")
        print(f"  Score: {site.get('commercial_score', 'N/A')}, Rule: {site.get('primary_rule', 'N/A')}")

        matches = scan_site(site, db)
        if matches:
            save_matches(matches, db)
            total_matches += len(matches)
            print(f"  >> {len(matches)} commercial lease(s) found")
        else:
            print(f"  >> No listings found")

    print(f"\n{'='*60}")
    print(f"Total: {total_matches} commercial matches across {len(sites)} sites")
    return total_matches


if __name__ == "__main__":
    run_scan()
