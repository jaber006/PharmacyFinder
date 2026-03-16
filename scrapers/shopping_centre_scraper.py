#!/usr/bin/env python3
"""
Shopping Centre Directory Scraper — Enrich shopping_centres with verified tenant counts.

Strategy:
  1. Google search for '[centre name] store directory' or '[centre name] shops'
  2. Scrape the directory page to count tenants
  3. Check if a pharmacy is already listed

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
from urllib.parse import quote_plus, urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DB_PATH = Path(__file__).resolve().parent.parent / "pharmacy_finder.db"
CACHE_DIR = Path(__file__).resolve().parent / "cache" / "shopping_centres"
RATE_LIMIT_SEC = 2.0  # Be respectful — varied targets

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-AU,en;q=0.9",
}

# Known shopping centre directory URL patterns
KNOWN_DIRECTORY_PATTERNS = {
    "westfield": "https://www.westfield.com.au/{slug}/stores",
    "stockland": "https://www.stockland.com.au/shopping-centres/{slug}/stores",
    "vicinity": "https://www.vicinity.com.au/centres/{slug}",
}

PHARMACY_KEYWORDS = [
    "pharmacy", "chemist", "priceline", "terry white", "amcal",
    "discount drug", "blooms", "chemmart", "guardian", "soul pattinson",
    "wizard", "good price", "cincotta", "national pharmacies",
    "capital chemist", "friendlies", "alive pharmacy", "pharmasave",
]

log = logging.getLogger("shopping_centre_scraper")

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def ensure_columns(conn: sqlite3.Connection):
    """Add enrichment columns if they don't exist."""
    cursor = conn.cursor()
    existing = {row[1] for row in cursor.execute("PRAGMA table_info(shopping_centres)").fetchall()}
    if "tenants_verified" not in existing:
        cursor.execute("ALTER TABLE shopping_centres ADD COLUMN tenants_verified INTEGER DEFAULT 0")
        log.info("Added column tenants_verified")
    if "has_pharmacy" not in existing:
        cursor.execute("ALTER TABLE shopping_centres ADD COLUMN has_pharmacy INTEGER DEFAULT 0")
        log.info("Added column has_pharmacy")
    conn.commit()


def get_shopping_centres(conn: sqlite3.Connection, state: str = None, limit: int = None):
    """Fetch shopping centres to enrich."""
    sql = "SELECT id, name, address, estimated_tenants, gla_sqm FROM shopping_centres WHERE 1=1"
    params = []
    if state:
        sql += " AND address LIKE ?"
        params.append(f"% {state.upper()} %")
    sql += " ORDER BY id"
    if limit:
        sql += " LIMIT ?"
        params.append(limit)
    return conn.execute(sql, params).fetchall()


def update_shopping_centre(conn: sqlite3.Connection, centre_id: int, tenant_count: int,
                           has_pharmacy: bool, source_url: str, dry_run: bool = False):
    """Update a shopping centre with verified data."""
    if dry_run:
        log.info(f"[DRY-RUN] Would update id={centre_id}: tenants={tenant_count}, has_pharmacy={has_pharmacy}")
        return
    conn.execute("""
        UPDATE shopping_centres
        SET estimated_tenants = ?, tenants_verified = 1, has_pharmacy = ?
        WHERE id = ?
    """, (tenant_count, 1 if has_pharmacy else 0, centre_id))
    conn.commit()


# ---------------------------------------------------------------------------
# Google search (simple scraping approach)
# ---------------------------------------------------------------------------

def _slugify(name: str) -> str:
    """Create URL slug from centre name."""
    slug = name.lower()
    slug = re.sub(r"[^a-z0-9\s-]", "", slug)
    slug = re.sub(r"\s+", "-", slug).strip("-")
    return slug


def _guess_directory_url(name: str) -> str | None:
    """Try to guess directory URL from known centre brands."""
    name_lower = name.lower()
    if "westfield" in name_lower:
        slug = _slugify(name.replace("Westfield", "").replace("westfield", "").strip())
        return KNOWN_DIRECTORY_PATTERNS["westfield"].format(slug=slug)
    if "stockland" in name_lower:
        slug = _slugify(name.replace("Stockland", "").replace("stockland", "").strip())
        return KNOWN_DIRECTORY_PATTERNS["stockland"].format(slug=slug)
    return None


def google_search_directory(centre_name: str, session: requests.Session) -> str | None:
    """Search for a shopping centre's store directory page.
    
    Uses a simple Google scrape. Returns the best URL or None.
    """
    query = f"{centre_name} store directory site:*.com.au"
    url = f"https://www.google.com/search?q={quote_plus(query)}&num=5"
    
    try:
        resp = session.get(url, headers={
            **HEADERS,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        }, timeout=15)
        if resp.status_code != 200:
            log.debug(f"Google search returned {resp.status_code}")
            return None
        
        soup = BeautifulSoup(resp.text, "lxml")
        
        # Extract URLs from search results
        for a in soup.select("a[href]"):
            href = a.get("href", "")
            # Google wraps URLs in /url?q=...
            if "/url?q=" in href:
                href = href.split("/url?q=")[1].split("&")[0]
            
            if not href.startswith("http"):
                continue
            
            # Skip Google's own pages
            parsed = urlparse(href)
            if "google" in parsed.netloc:
                continue
            
            # Prefer pages with 'store', 'shop', 'directory', 'tenant' in URL
            path_lower = parsed.path.lower()
            if any(kw in path_lower for kw in ["store", "shop", "directory", "tenant", "retail"]):
                return href
            
            # Or just return first non-Google .com.au result
            if parsed.netloc.endswith(".com.au"):
                return href
    
    except requests.RequestException as e:
        log.debug(f"Google search failed: {e}")
    
    return None


# ---------------------------------------------------------------------------
# Directory page scraping
# ---------------------------------------------------------------------------

def scrape_directory_page(url: str, session: requests.Session) -> dict | None:
    """Scrape a shopping centre directory page for tenant data.
    
    Returns dict with keys: tenant_count, has_pharmacy, tenants (list), source_url
    """
    try:
        resp = session.get(url, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            return None
        
        soup = BeautifulSoup(resp.text, "lxml")
        page_text = soup.get_text(separator=" ").lower()
        tenants = set()
        
        # Strategy 1: Look for store/shop listing elements
        # Common patterns: lists, cards, grid items with store names
        store_selectors = [
            ".store-card", ".store-item", ".shop-item", ".tenant-item",
            ".store-listing", ".directory-item", ".retailer-card",
            "[data-store]", "[data-tenant]", "[data-shop]",
            ".store-name", ".shop-name",
            "li.store", "li.shop",
            ".stores-list li", ".directory-list li",
        ]
        
        for selector in store_selectors:
            elements = soup.select(selector)
            if len(elements) > 5:  # Only trust if we find a reasonable number
                for el in elements:
                    name = el.get_text(strip=True)
                    if name and len(name) > 1 and len(name) < 100:
                        tenants.add(name)
                if tenants:
                    break
        
        # Strategy 2: Look for a-z directory links
        if not tenants:
            for a in soup.select("a"):
                href = a.get("href", "").lower()
                text = a.get_text(strip=True)
                if any(kw in href for kw in ["/store/", "/shop/", "/tenant/"]):
                    if text and len(text) > 1 and len(text) < 100:
                        tenants.add(text)
        
        # Strategy 3: Count from text patterns like "X stores" or "X shops"
        tenant_count_from_text = 0
        if not tenants:
            m = re.search(r"(\d+)\s+(?:stores?|shops?|tenants?|retailers?)", page_text)
            if m:
                tenant_count_from_text = int(m.group(1))
        
        # Check for pharmacy
        has_pharmacy = any(kw in page_text for kw in PHARMACY_KEYWORDS)
        
        tenant_count = len(tenants) if tenants else tenant_count_from_text
        
        if tenant_count > 0:
            return {
                "tenant_count": tenant_count,
                "has_pharmacy": has_pharmacy,
                "tenants": sorted(tenants)[:200] if tenants else [],
                "source_url": url,
            }
        
        return None
    
    except requests.RequestException as e:
        log.debug(f"Failed to scrape {url}: {e}")
        return None


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

def _cache_path(centre_id: int) -> Path:
    return CACHE_DIR / f"centre_{centre_id}.json"


def save_cache(centre_id: int, data: dict):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with open(_cache_path(centre_id), "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)


def load_cache(centre_id: int) -> dict | None:
    p = _cache_path(centre_id)
    if p.exists():
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(state: str = None, limit: int = None, dry_run: bool = False):
    """Main entry point."""
    conn = sqlite3.connect(str(DB_PATH))
    ensure_columns(conn)

    centres = get_shopping_centres(conn, state=state, limit=limit)
    log.info(f"Processing {len(centres)} shopping centres")

    session = requests.Session()
    stats = {"updated": 0, "failed": 0, "skipped": 0, "cached": 0}

    for row in centres:
        centre_id, name, address, current_tenants, gla_sqm = row
        log.info(f"[{centre_id}] {name} — {address}")

        # Check cache first
        cached = load_cache(centre_id)
        if cached and cached.get("tenant_count", 0) > 0:
            log.info(f"  Using cached data ({cached['tenant_count']} tenants)")
            update_shopping_centre(
                conn, centre_id,
                tenant_count=cached["tenant_count"],
                has_pharmacy=cached.get("has_pharmacy", False),
                source_url=cached.get("source_url", "cache"),
                dry_run=dry_run,
            )
            stats["cached"] += 1
            stats["updated"] += 1
            continue

        result = None

        # Step 1: Try known URL patterns
        guessed_url = _guess_directory_url(name)
        if guessed_url:
            log.info(f"  Trying guessed URL: {guessed_url}")
            time.sleep(RATE_LIMIT_SEC)
            result = scrape_directory_page(guessed_url, session)

        # Step 2: Google search for directory
        if not result:
            log.info(f"  Searching Google for directory...")
            time.sleep(RATE_LIMIT_SEC)
            directory_url = google_search_directory(name, session)
            if directory_url:
                log.info(f"  Found: {directory_url}")
                time.sleep(RATE_LIMIT_SEC)
                result = scrape_directory_page(directory_url, session)

        if result and result.get("tenant_count", 0) > 0:
            save_cache(centre_id, result)
            update_shopping_centre(
                conn, centre_id,
                tenant_count=result["tenant_count"],
                has_pharmacy=result.get("has_pharmacy", False),
                source_url=result.get("source_url", ""),
                dry_run=dry_run,
            )
            stats["updated"] += 1
            log.info(f"  ✓ {result['tenant_count']} tenants, pharmacy={'Yes' if result.get('has_pharmacy') else 'No'}")
        else:
            log.info(f"  No directory data found — skipping")
            save_cache(centre_id, {
                "tenant_count": 0, "source": "not_found",
                "searched_at": datetime.now().isoformat()
            })
            stats["skipped"] += 1

    conn.close()

    # Summary
    log.info("=" * 50)
    log.info("SHOPPING CENTRE SCRAPER — SUMMARY")
    log.info(f"  Total processed: {len(centres)}")
    log.info(f"  Updated:         {stats['updated']} ({stats['cached']} from cache)")
    log.info(f"  Skipped:         {stats['skipped']}")
    log.info(f"  Failed:          {stats['failed']}")
    log.info("=" * 50)

    return stats


def main():
    parser = argparse.ArgumentParser(description="Scrape shopping centre directories for tenant counts")
    parser.add_argument("--state", type=str, help="Filter by state (e.g. TAS, VIC)")
    parser.add_argument("--limit", type=int, help="Max centres to process")
    parser.add_argument("--dry-run", action="store_true", help="Don't write to DB")
    parser.add_argument("--verbose", "-v", action="store_true", help="Debug logging")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    run(state=args.state, limit=args.limit, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
