#!/usr/bin/env python3
"""
Shopping Centre Scraper — Verify tenant counts and pharmacy presence.

Strategy: Use Brave Search to find each centre's store directory page,
then scrape it to count tenants and check for pharmacy.

Updates: estimated_tenants, tenants_verified, has_pharmacy
"""

import argparse
import json
import logging
import os
import re
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import quote_plus, urljoin

import requests
from bs4 import BeautifulSoup

DB_PATH = Path(__file__).resolve().parent.parent / "pharmacy_finder.db"
CACHE_DIR = Path(__file__).resolve().parent / "cache" / "shopping_centres"
RATE_LIMIT = 2.0

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,*/*",
    "Accept-Language": "en-AU,en;q=0.9",
}

PHARMACY_KEYWORDS = [
    "pharmacy", "chemist", "discount drug", "priceline", "amcal", "terry white",
    "blooms", "soul pattinson", "wizard", "good price", "national pharmacies",
    "capital chemist", "friendlies", "ramsay pharmacy",
]

log = logging.getLogger("shopping_centre_scraper")


def ensure_columns(conn):
    cursor = conn.cursor()
    existing = {row[1] for row in cursor.execute("PRAGMA table_info(shopping_centres)").fetchall()}
    for col, sql in [
        ("tenants_verified", "ALTER TABLE shopping_centres ADD COLUMN tenants_verified INTEGER DEFAULT 0"),
        ("has_pharmacy", "ALTER TABLE shopping_centres ADD COLUMN has_pharmacy INTEGER DEFAULT 0"),
        ("pharmacy_names", "ALTER TABLE shopping_centres ADD COLUMN pharmacy_names TEXT DEFAULT ''"),
        ("tenant_source", "ALTER TABLE shopping_centres ADD COLUMN tenant_source TEXT DEFAULT ''"),
    ]:
        if col not in existing:
            cursor.execute(sql)
            log.info(f"Added column {col}")
    conn.commit()


def get_brave_api_key():
    key = os.environ.get("BRAVE_SEARCH_API_KEY") or os.environ.get("BRAVE_API_KEY", "")
    if not key:
        config_path = Path.home() / ".openclaw" / "openclaw.json"
        if config_path.exists():
            try:
                data = json.load(open(config_path))
                key = data.get("tools", {}).get("web", {}).get("search", {}).get("apiKey", "")
            except Exception:
                pass
    return key


def brave_search(query, api_key, count=5):
    if not api_key:
        return []
    try:
        resp = requests.get(
            "https://api.search.brave.com/res/v1/web/search",
            params={"q": query, "count": count, "country": "AU"},
            headers={"Accept": "application/json", "X-Subscription-Token": api_key},
            timeout=10,
        )
        if resp.status_code == 200:
            return resp.json().get("web", {}).get("results", [])
        elif resp.status_code == 429:
            log.warning("Brave rate limit hit, waiting 5s...")
            time.sleep(5)
    except Exception as e:
        log.debug(f"Brave search error: {e}")
    return []


def find_store_directory_url(centre_name, state, api_key):
    """Find the shopping centre's store directory URL."""
    results = brave_search(f'"{centre_name}" store directory shops list', api_key)
    
    if not results:
        results = brave_search(f'{centre_name} {state} stores shops', api_key)
    
    if not results:
        return None
    
    # Look for the centre's own website
    aggregator_domains = ["google.com", "yellowpages.com", "broadsheet.com", "timeout.com", 
                          "tripadvisor.com", "yelp.com", "wikipedia.org"]
    
    for r in results:
        url = r.get("url", "")
        domain = url.split("/")[2] if len(url.split("/")) > 2 else ""
        
        if any(agg in domain for agg in aggregator_domains):
            continue
        
        title = r.get("title", "").lower()
        desc = r.get("description", "").lower()
        name_words = set(centre_name.lower().split())
        
        # Check if title/desc mentions stores/directory
        if any(kw in title or kw in desc for kw in ["store", "directory", "shop", "tenant"]):
            if len(name_words & set(title.split())) >= 2:
                return url
        
        # Or if it's clearly the centre's website
        if len(name_words & set(title.split())) >= 2:
            return url
    
    return None


def extract_stores_from_page(html, url):
    """Extract store names from a shopping centre directory page."""
    soup = BeautifulSoup(html, "lxml")
    stores = []
    pharmacies = []
    
    # Method 1: Look for structured store listings
    # Many centres use cards/lists with store names
    for selector in [
        "[class*='store']", "[class*='tenant']", "[class*='retailer']",
        "[class*='shop-']", "[class*='directory']", 
        ".store-card", ".store-item", ".store-list",
        "li[class*='store']", "div[class*='listing']",
    ]:
        for el in soup.select(selector):
            name_el = el.select_one("h2, h3, h4, h5, a, [class*='name'], [class*='title']")
            if name_el:
                name = name_el.get_text(strip=True)
                if name and len(name) > 1 and len(name) < 100:
                    stores.append(name)
    
    # Method 2: Look for store links in a directory section
    if not stores:
        directory_section = None
        for section in soup.find_all(["section", "div", "main"]):
            section_text = " ".join(c.get_text() for c in section.find_all(["h1", "h2", "h3"]) if c)
            if any(kw in section_text.lower() for kw in ["store", "directory", "shop", "our stores"]):
                directory_section = section
                break
        
        if directory_section:
            for link in directory_section.find_all("a", href=True):
                name = link.get_text(strip=True)
                if name and len(name) > 1 and len(name) < 100 and not name.lower().startswith("back"):
                    stores.append(name)
    
    # Method 3: JSON-LD or structured data
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            ld = json.loads(script.string)
            if isinstance(ld, dict):
                tenants = ld.get("tenant", ld.get("containedInPlace", []))
                if isinstance(tenants, list):
                    for t in tenants:
                        name = t.get("name", "") if isinstance(t, dict) else str(t)
                        if name:
                            stores.append(name)
        except (json.JSONDecodeError, TypeError):
            pass
    
    # Method 4: Count links that look like store pages
    if not stores:
        store_links = soup.select("a[href*='/stores/'], a[href*='/store/'], a[href*='/shops/'], a[href*='/shop/']")
        for link in store_links:
            name = link.get_text(strip=True)
            if name and len(name) > 1 and len(name) < 100:
                stores.append(name)
    
    # Method 5: Look for "X stores" text
    if not stores:
        text = soup.get_text()
        m = re.search(r'(\d+)\s*(?:store|shop|retailer|tenant)s?\b', text, re.I)
        if m:
            count = int(m.group(1))
            if 5 <= count <= 500:
                return {"tenant_count": count, "stores": [], "pharmacies": [], "source": "text_count"}
    
    # Deduplicate
    seen = set()
    unique_stores = []
    for s in stores:
        key = s.lower().strip()
        if key not in seen and len(key) > 1:
            seen.add(key)
            unique_stores.append(s)
            # Check if pharmacy
            if any(pk in key for pk in PHARMACY_KEYWORDS):
                pharmacies.append(s)
    
    if unique_stores:
        return {
            "tenant_count": len(unique_stores),
            "stores": unique_stores,
            "pharmacies": pharmacies,
            "source": "directory_scrape",
        }
    
    return None


def scrape_centre_directory(directory_url, session):
    """Fetch and scrape a shopping centre directory page."""
    try:
        time.sleep(RATE_LIMIT)
        resp = session.get(directory_url, timeout=15)
        if resp.status_code != 200:
            return None
        
        result = extract_stores_from_page(resp.text, directory_url)
        if result:
            return result
        
        # Try common subpages
        base = directory_url.rstrip("/")
        for path in ["/stores", "/directory", "/shops", "/store-directory", "/our-stores"]:
            try:
                time.sleep(RATE_LIMIT)
                resp2 = session.get(base + path, timeout=10)
                if resp2.status_code == 200:
                    result = extract_stores_from_page(resp2.text, base + path)
                    if result:
                        return result
            except requests.RequestException:
                continue
    except requests.RequestException as e:
        log.debug(f"Error fetching {directory_url}: {e}")
    
    return None


def run(state=None, limit=None, dry_run=False, verbose=False, **kwargs):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    
    api_key = get_brave_api_key()
    if not api_key:
        log.error("No Brave API key found. Set BRAVE_API_KEY env var or configure in openclaw.")
        return {"error": "no_api_key"}
    
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    ensure_columns(conn)
    
    session = requests.Session()
    session.headers.update(HEADERS)
    
    # Get shopping centres
    sql = "SELECT id, name, address, estimated_tenants FROM shopping_centres WHERE 1=1"
    params = []
    if state:
        sql += " AND address LIKE ?"
        params.append(f"%{state.upper()}%")
    sql += " ORDER BY id"
    if limit:
        sql += " LIMIT ?"
        params.append(limit)
    
    centres = [dict(row) for row in conn.execute(sql, params).fetchall()]
    log.info(f"Processing {len(centres)} shopping centres")
    
    stats = {"updated": 0, "skipped": 0, "failed": 0, "from_cache": 0}
    
    for i, centre in enumerate(centres):
        cid = centre["id"]
        name = centre["name"]
        addr = centre["address"]
        
        # Extract state from address
        cstate = ""
        for st in ["NSW", "VIC", "QLD", "SA", "WA", "TAS", "NT", "ACT"]:
            if st in (addr or "").upper():
                cstate = st
                break
        
        log.info(f"[{i+1}/{len(centres)}] {name}")
        
        # Check cache
        cache_file = CACHE_DIR / f"{cid}.json"
        if cache_file.exists():
            age_h = (time.time() - cache_file.stat().st_mtime) / 3600
            if age_h < 168:
                with open(cache_file) as f:
                    cached = json.load(f)
                if cached.get("tenant_count"):
                    log.info(f"  Cached: {cached['tenant_count']} tenants ({cached.get('source', 'cache')})")
                    if not dry_run:
                        pharm_names = json.dumps(cached.get("pharmacies", []))
                        conn.execute("""
                            UPDATE shopping_centres 
                            SET estimated_tenants = ?, tenants_verified = 1, has_pharmacy = ?,
                                pharmacy_names = ?, tenant_source = ?
                            WHERE id = ?
                        """, (cached["tenant_count"], 1 if cached.get("pharmacies") else 0,
                              pharm_names, cached.get("source", "cache"), cid))
                        conn.commit()
                    stats["updated"] += 1
                    stats["from_cache"] += 1
                    continue
        
        # Find store directory
        log.info(f"  Searching for store directory...")
        directory_url = find_store_directory_url(name, cstate, api_key)
        
        result = None
        if directory_url:
            log.info(f"  Found: {directory_url}")
            result = scrape_centre_directory(directory_url, session)
        
        if result and result.get("tenant_count"):
            pharmacies = result.get("pharmacies", [])
            log.info(f"  {result['tenant_count']} tenants, {len(pharmacies)} pharmacies: {', '.join(pharmacies) if pharmacies else 'none found'}")
            
            # Cache
            with open(cache_file, "w") as f:
                json.dump(result, f, indent=2)
            
            if not dry_run:
                pharm_names = json.dumps(pharmacies)
                conn.execute("""
                    UPDATE shopping_centres 
                    SET estimated_tenants = ?, tenants_verified = 1, has_pharmacy = ?,
                        pharmacy_names = ?, tenant_source = ?
                    WHERE id = ?
                """, (result["tenant_count"], 1 if pharmacies else 0,
                      pharm_names, result.get("source", "unknown"), cid))
                conn.commit()
            
            stats["updated"] += 1
        else:
            log.info(f"  No directory data found")
            with open(cache_file, "w") as f:
                json.dump({"tenant_count": 0, "source": "not_found", "timestamp": datetime.now().isoformat()}, f)
            stats["skipped"] += 1
    
    conn.close()
    
    log.info("=" * 50)
    log.info("SHOPPING CENTRE SCRAPER — SUMMARY")
    log.info(f"  Total processed: {len(centres)}")
    log.info(f"  Updated:         {stats['updated']} ({stats['from_cache']} from cache)")
    log.info(f"  Skipped:         {stats['skipped']}")
    log.info(f"  Failed:          {stats['failed']}")
    log.info("=" * 50)
    
    return stats


def main():
    parser = argparse.ArgumentParser(description="Enrich shopping centres with verified tenant counts")
    parser.add_argument("--state", help="Filter by state (e.g. TAS)")
    parser.add_argument("--limit", type=int, help="Max records to process")
    parser.add_argument("--dry-run", action="store_true", help="Don't write to DB")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                        datefmt="%H:%M:%S")
    
    run(state=args.state, limit=args.limit, dry_run=args.dry_run, verbose=args.verbose)


if __name__ == "__main__":
    main()
