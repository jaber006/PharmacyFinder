#!/usr/bin/env python3
"""
GP Count Scraper — Verify doctor counts at medical centres.

Strategy: Use Brave Search to find each practice's website, then
scrape their staff/team/doctors page to count GPs.

Updates: num_gps, practitioners_json, gp_count_verified, gp_count_source
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
CACHE_DIR = Path(__file__).resolve().parent / "cache" / "gp_counts"
RATE_LIMIT = 2.0  # seconds between web requests

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,*/*",
    "Accept-Language": "en-AU,en;q=0.9",
}

# Common page paths where practices list their doctors
TEAM_PATHS = [
    "/our-staff", "/our-team", "/our-doctors", "/doctors", "/team",
    "/staff", "/about-us", "/about", "/practitioners", "/meet-the-team",
    "/gps", "/general-practitioners", "/meet-our-doctors", "/the-team",
    "/our-gps", "/meet-our-team",
]

log = logging.getLogger("gp_count_scraper")


def ensure_columns(conn):
    cursor = conn.cursor()
    existing = {row[1] for row in cursor.execute("PRAGMA table_info(medical_centres)").fetchall()}
    for col, sql in [
        ("gp_count_verified", "ALTER TABLE medical_centres ADD COLUMN gp_count_verified INTEGER DEFAULT 0"),
        ("gp_count_source", "ALTER TABLE medical_centres ADD COLUMN gp_count_source TEXT DEFAULT ''"),
    ]:
        if col not in existing:
            cursor.execute(sql)
            log.info(f"Added column {col}")
    conn.commit()


def brave_search(query, count=5):
    """Search using Brave Search API."""
    api_key = os.environ.get("BRAVE_SEARCH_API_KEY") or os.environ.get("BRAVE_API_KEY", "")
    if not api_key:
        # Try to read from openclaw config (JSON)
        config_paths = [
            Path.home() / ".openclaw" / "openclaw.json",
            Path.home() / ".openclaw" / "config.yaml",
        ]
        for cp in config_paths:
            if cp.exists():
                try:
                    import json as jn
                    data = jn.load(open(cp))
                    api_key = data.get("tools", {}).get("web", {}).get("search", {}).get("apiKey", "")
                    if api_key:
                        break
                except Exception:
                    pass
    
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
    except Exception as e:
        log.debug(f"Brave search error: {e}")
    return []


def find_practice_website(name, state, address):
    """Use Brave Search to find the practice's website."""
    results = brave_search(f'"{name}" {state} doctors team')
    
    if not results:
        # Try without quotes
        results = brave_search(f'{name} {state} doctors')
    
    if not results:
        return None, None
    
    # Look for the practice's own website (not healthengine/hotdoc/google)
    aggregator_domains = ["healthengine.com", "hotdoc.com", "google.com", "yelp.com", 
                          "yellowpages.com", "truelocal.com", "whitecoat.com", "ratemds.com"]
    
    practice_url = None
    hotdoc_url = None
    
    for r in results:
        url = r.get("url", "")
        domain = url.split("/")[2] if len(url.split("/")) > 2 else ""
        
        # Save hotdoc URL as backup
        if "hotdoc.com" in domain:
            hotdoc_url = url
            continue
        
        # Skip aggregators
        if any(agg in domain for agg in aggregator_domains):
            continue
        
        # Check if this looks like the practice's own site
        title = r.get("title", "").lower()
        name_words = set(name.lower().split())
        title_words = set(title.split())
        if len(name_words & title_words) >= 2 or name.lower()[:12] in title:
            practice_url = url
            break
    
    return practice_url, hotdoc_url


def extract_doctors_from_page(html, url):
    """Extract doctor names from a webpage."""
    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ", strip=True)
    doctors = []
    
    # Method 1: Find "Dr Name" patterns in the text
    dr_pattern = re.findall(r'(?:Dr\.?|Doctor)\s+([A-Z][a-z]+(?:\s+(?:[A-Z][a-z]+|"[^"]*"))*(?:\s+[A-Z][a-z]+)?)', text)
    for name in dr_pattern:
        clean_name = f"Dr {name.strip()}"
        if len(clean_name) > 5 and clean_name not in doctors:
            doctors.append(clean_name)
    
    # Method 2: JSON-LD structured data
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            ld = json.loads(script.string)
            if isinstance(ld, dict):
                for key in ["employee", "member", "makesOffer"]:
                    members = ld.get(key, [])
                    if isinstance(members, list):
                        for m in members:
                            if isinstance(m, dict):
                                name = m.get("name", "")
                                if name and name not in doctors:
                                    doctors.append(name)
        except (json.JSONDecodeError, TypeError):
            pass
    
    # Method 3: Doctor cards/sections with headings
    for tag in soup.find_all(["h2", "h3", "h4", "h5", "strong", "b"]):
        tag_text = tag.get_text(strip=True)
        if re.match(r'^Dr\.?\s+[A-Z]', tag_text):
            name = tag_text.split(" - ")[0].split(" – ")[0].strip()
            if name not in doctors:
                doctors.append(name)
    
    # Deduplicate with fuzzy partial-name matching
    # e.g. "Dr Kshemendra Tillekeratne", "Dr Tillekeratne", "Dr Kshemendra" → 1 person
    cleaned = []
    for d in doctors:
        name = d.lower().replace(".", "").strip()
        name = re.sub(r'^(?:dr|doctor)\s+', '', name).strip()
        if len(name) > 2:
            cleaned.append((d, set(name.split())))
    
    # Merge: if one name's words are a subset of another's, keep the longer one
    merged = []
    used = [False] * len(cleaned)
    for i, (orig_i, words_i) in enumerate(cleaned):
        if used[i]:
            continue
        best_orig = orig_i
        best_words = words_i
        for j, (orig_j, words_j) in enumerate(cleaned):
            if i == j or used[j]:
                continue
            # Check if one is subset of the other
            if words_i <= words_j or words_j <= words_i:
                used[j] = True
                if len(words_j) > len(best_words):
                    best_orig = orig_j
                    best_words = words_j
        merged.append(best_orig)
    
    return merged


def scrape_practice_for_gps(practice_url, session):
    """Scrape a practice website for doctor information."""
    base_url = practice_url.rstrip("/")
    
    # First try the homepage
    try:
        time.sleep(RATE_LIMIT)
        resp = session.get(base_url, timeout=15)
        if resp.status_code == 200:
            doctors = extract_doctors_from_page(resp.text, base_url)
            if len(doctors) >= 2:
                return {"gp_count": len(doctors), "practitioners": doctors, "source": "website_home"}
            
            # Find team/doctors page from homepage links
            soup = BeautifulSoup(resp.text, "lxml")
            team_link = None
            for a in soup.find_all("a", href=True):
                href = a.get("href", "").lower()
                text = a.get_text(strip=True).lower()
                if any(kw in href or kw in text for kw in ["doctor", "team", "staff", "practitioner", "gp", "our-team", "our-doctor", "our-staff"]):
                    team_link = urljoin(base_url, a["href"])
                    break
            
            if team_link:
                time.sleep(RATE_LIMIT)
                resp2 = session.get(team_link, timeout=15)
                if resp2.status_code == 200:
                    doctors = extract_doctors_from_page(resp2.text, team_link)
                    if doctors:
                        return {"gp_count": len(doctors), "practitioners": doctors, "source": "website_team"}
    except requests.RequestException as e:
        log.debug(f"Error fetching {base_url}: {e}")
    
    # Try common team page paths
    for path in TEAM_PATHS:
        try:
            url = base_url + path
            time.sleep(RATE_LIMIT)
            resp = session.get(url, timeout=10)
            if resp.status_code == 200:
                doctors = extract_doctors_from_page(resp.text, url)
                if doctors:
                    return {"gp_count": len(doctors), "practitioners": doctors, "source": f"website_{path.strip('/')}"}
        except requests.RequestException:
            continue
    
    return None


def run(state=None, limit=None, dry_run=False, verbose=False, **kwargs):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    ensure_columns(conn)
    
    session = requests.Session()
    session.headers.update(HEADERS)
    
    # Get medical centres
    sql = "SELECT id, name, address, state, num_gps, practitioners_json FROM medical_centres WHERE 1=1"
    params = []
    if state:
        sql += " AND state = ?"
        params.append(state.upper())
    sql += " ORDER BY id"
    if limit:
        sql += " LIMIT ?"
        params.append(limit)
    
    centres = [dict(row) for row in conn.execute(sql, params).fetchall()]
    log.info(f"Processing {len(centres)} medical centres")
    
    stats = {"updated": 0, "skipped": 0, "failed": 0, "from_cache": 0}
    
    for i, centre in enumerate(centres):
        cid = centre["id"]
        name = centre["name"]
        addr = centre["address"]
        cstate = centre["state"]
        
        log.info(f"[{i+1}/{len(centres)}] {name}")
        
        # Check cache
        cache_file = CACHE_DIR / f"{cid}.json"
        if cache_file.exists():
            age_h = (time.time() - cache_file.stat().st_mtime) / 3600
            if age_h < 168:  # 1 week
                with open(cache_file) as f:
                    cached = json.load(f)
                if cached.get("gp_count"):
                    log.info(f"  Cached: {cached['gp_count']} GPs ({cached.get('source', 'cache')})")
                    if not dry_run:
                        conn.execute("""
                            UPDATE medical_centres 
                            SET num_gps = ?, practitioners_json = ?, gp_count_verified = 1, gp_count_source = ?
                            WHERE id = ?
                        """, (cached["gp_count"], json.dumps(cached.get("practitioners", [])),
                              cached.get("source", "cache"), cid))
                        conn.commit()
                    stats["updated"] += 1
                    stats["from_cache"] += 1
                    continue
        
        result = None
        
        # Step 1: Find practice website via Brave Search
        log.info(f"  Searching for website...")
        practice_url, hotdoc_url = find_practice_website(name, cstate, addr)
        
        # Step 2: Scrape practice website
        if practice_url:
            log.info(f"  Found: {practice_url}")
            result = scrape_practice_for_gps(practice_url, session)
        
        if result and result.get("gp_count"):
            log.info(f"  Found {result['gp_count']} GPs: {', '.join(result['practitioners'][:5])}")
            
            # Cache
            with open(cache_file, "w") as f:
                json.dump(result, f, indent=2)
            
            if not dry_run:
                conn.execute("""
                    UPDATE medical_centres 
                    SET num_gps = ?, practitioners_json = ?, gp_count_verified = 1, gp_count_source = ?
                    WHERE id = ?
                """, (result["gp_count"], json.dumps(result.get("practitioners", [])),
                      result.get("source", "unknown"), cid))
                conn.commit()
            
            stats["updated"] += 1
        else:
            log.info(f"  No data found")
            # Cache the miss too (with shorter TTL)
            with open(cache_file, "w") as f:
                json.dump({"gp_count": 0, "source": "not_found", "timestamp": datetime.now().isoformat()}, f)
            stats["skipped"] += 1
    
    conn.close()
    
    log.info("=" * 50)
    log.info("GP COUNT SCRAPER — SUMMARY")
    log.info(f"  Total processed: {len(centres)}")
    log.info(f"  Updated:         {stats['updated']} ({stats['from_cache']} from cache)")
    log.info(f"  Skipped:         {stats['skipped']}")
    log.info(f"  Failed:          {stats['failed']}")
    log.info("=" * 50)
    
    return stats


def main():
    parser = argparse.ArgumentParser(description="Enrich medical centres with verified GP counts")
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
