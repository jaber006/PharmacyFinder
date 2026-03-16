#!/usr/bin/env python3
"""
Hotdoc GP Scraper — Enrich medical_centres with verified GP counts.

Sources (in priority order):
  1. Hotdoc.com.au search API / practice pages
  2. HealthDirect.gov.au Australian Health Services (fallback)

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

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DB_PATH = Path(__file__).resolve().parent.parent / "pharmacy_finder.db"
CACHE_DIR = Path(__file__).resolve().parent / "cache" / "hotdoc"
RATE_LIMIT_SEC = 1.0

HOTDOC_SEARCH_URL = "https://www.hotdoc.com.au/search"
HOTDOC_API_URL = "https://www.hotdoc.com.au/api/patient/clinics"
HEALTHDIRECT_URL = "https://api.healthdirect.gov.au/servicefinder/v3/search"
HEALTHDIRECT_WEB = "https://www.healthdirect.gov.au/australian-health-services"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-AU,en;q=0.9",
}

log = logging.getLogger("hotdoc_gp_scraper")

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def ensure_columns(conn: sqlite3.Connection):
    """Add enrichment columns if they don't exist."""
    cursor = conn.cursor()
    existing = {row[1] for row in cursor.execute("PRAGMA table_info(medical_centres)").fetchall()}
    if "gp_count_verified" not in existing:
        cursor.execute("ALTER TABLE medical_centres ADD COLUMN gp_count_verified INTEGER DEFAULT 0")
        log.info("Added column gp_count_verified")
    if "gp_count_source" not in existing:
        cursor.execute("ALTER TABLE medical_centres ADD COLUMN gp_count_source TEXT DEFAULT ''")
        log.info("Added column gp_count_source")
    conn.commit()


def get_medical_centres(conn: sqlite3.Connection, state: str = None, limit: int = None):
    """Fetch medical centres to enrich."""
    sql = "SELECT id, name, address, state, num_gps, practitioners_json FROM medical_centres WHERE 1=1"
    params = []
    if state:
        sql += " AND state = ?"
        params.append(state.upper())
    sql += " ORDER BY id"
    if limit:
        sql += " LIMIT ?"
        params.append(limit)
    return conn.execute(sql, params).fetchall()


def update_medical_centre(conn: sqlite3.Connection, centre_id: int, num_gps: int,
                          practitioners_json: str, source: str, dry_run: bool = False):
    """Update a medical centre with verified data."""
    if dry_run:
        log.info(f"[DRY-RUN] Would update id={centre_id}: num_gps={num_gps}, source={source}")
        return
    conn.execute("""
        UPDATE medical_centres
        SET num_gps = ?, practitioners_json = ?, gp_count_verified = 1, gp_count_source = ?
        WHERE id = ?
    """, (num_gps, practitioners_json, source, centre_id))
    conn.commit()


# ---------------------------------------------------------------------------
# Hotdoc scraping
# ---------------------------------------------------------------------------

def _normalise_name(name: str) -> str:
    """Normalise practice name for fuzzy matching."""
    name = name.lower().strip()
    for term in ["medical centre", "medical center", "medical practice", "family practice",
                 "clinic", "surgery", "group practice", "health centre", "health center",
                 "super clinic", "superclinic", "gp "]:
        name = name.replace(term, "")
    return re.sub(r"\s+", " ", name).strip()


def _name_similarity(a: str, b: str) -> float:
    """Simple word-overlap similarity 0-1."""
    wa = set(_normalise_name(a).split())
    wb = set(_normalise_name(b).split())
    if not wa or not wb:
        return 0.0
    return len(wa & wb) / max(len(wa), len(wb))


def search_hotdoc(centre_name: str, suburb: str, session: requests.Session) -> dict | None:
    """Search Hotdoc for a practice and extract GP data.
    
    Returns dict with keys: gp_count, practitioners, source_url  or None.
    """
    # Try the Hotdoc API first (JSON endpoint)
    try:
        params = {
            "q": centre_name,
            "filters": "gp",
            "suburb": suburb,
        }
        resp = session.get(HOTDOC_API_URL, params=params, headers=HEADERS, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            clinics = data if isinstance(data, list) else data.get("clinics", data.get("results", []))
            for clinic in clinics[:5]:
                clinic_name = clinic.get("name", clinic.get("clinic_name", ""))
                if _name_similarity(centre_name, clinic_name) > 0.3:
                    slug = clinic.get("slug", clinic.get("url_slug", ""))
                    if slug:
                        return _scrape_hotdoc_practice(slug, session)
    except (requests.RequestException, json.JSONDecodeError, KeyError) as e:
        log.debug(f"Hotdoc API search failed for '{centre_name}': {e}")

    # Fallback: scrape Hotdoc search page
    try:
        search_url = f"{HOTDOC_SEARCH_URL}?filters=gp&q={quote_plus(centre_name)}&suburb={quote_plus(suburb)}"
        resp = session.get(search_url, headers=HEADERS, timeout=15)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "lxml")
            # Look for practice links
            for link in soup.select("a[href*='/medical-centre/']"):
                link_text = link.get_text(strip=True)
                if _name_similarity(centre_name, link_text) > 0.3:
                    practice_url = link.get("href", "")
                    if practice_url:
                        slug = practice_url.rstrip("/").split("/")[-1]
                        return _scrape_hotdoc_practice(slug, session)
    except requests.RequestException as e:
        log.debug(f"Hotdoc web search failed for '{centre_name}': {e}")

    return None


def _scrape_hotdoc_practice(slug: str, session: requests.Session) -> dict | None:
    """Scrape a Hotdoc practice page for GP details."""
    url = f"https://www.hotdoc.com.au/medical-centres/{slug}"
    try:
        time.sleep(RATE_LIMIT_SEC)
        resp = session.get(url, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            return None

        soup = BeautifulSoup(resp.text, "lxml")
        practitioners = []

        # Try JSON-LD structured data first
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                ld = json.loads(script.string)
                if isinstance(ld, dict) and ld.get("@type") == "MedicalClinic":
                    members = ld.get("member", ld.get("employee", []))
                    if isinstance(members, list):
                        for m in members:
                            name = m.get("name", "")
                            specialty = m.get("medicalSpecialty", m.get("specialty", "GP"))
                            if name:
                                practitioners.append({"name": name, "specialty": str(specialty)})
            except (json.JSONDecodeError, TypeError):
                continue

        # Fallback: scrape doctor cards
        if not practitioners:
            for card in soup.select(".practitioner-card, .doctor-card, [data-testid='practitioner']"):
                name_el = card.select_one(".practitioner-name, .doctor-name, h3, h4")
                spec_el = card.select_one(".practitioner-specialty, .specialty, .subtitle")
                if name_el:
                    practitioners.append({
                        "name": name_el.get_text(strip=True),
                        "specialty": spec_el.get_text(strip=True) if spec_el else "GP"
                    })

        # Fallback: look for doctor links/names in any list
        if not practitioners:
            for el in soup.select("a[href*='/doctor/'], a[href*='/practitioner/']"):
                name = el.get_text(strip=True)
                if name and len(name) > 3 and not name.startswith("http"):
                    practitioners.append({"name": name, "specialty": "GP"})

        if practitioners:
            return {
                "gp_count": len(practitioners),
                "practitioners": practitioners,
                "source_url": url,
            }
    except requests.RequestException as e:
        log.debug(f"Failed to scrape Hotdoc practice {slug}: {e}")

    return None


# ---------------------------------------------------------------------------
# HealthDirect fallback
# ---------------------------------------------------------------------------

def search_healthdirect(centre_name: str, suburb: str, state: str,
                        session: requests.Session) -> dict | None:
    """Search HealthDirect for a medical centre (fallback source)."""
    try:
        # Try the HealthDirect service finder search page
        search_url = (
            f"{HEALTHDIRECT_WEB}/results/{quote_plus(suburb + ' ' + state)}"
            f"?name={quote_plus(centre_name)}&type=GP"
        )
        resp = session.get(search_url, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            return None

        soup = BeautifulSoup(resp.text, "lxml")
        
        # Look for matching service results
        for result in soup.select(".service-result, .search-result, article"):
            title_el = result.select_one("h2, h3, .service-name, .title")
            if not title_el:
                continue
            title = title_el.get_text(strip=True)
            if _name_similarity(centre_name, title) < 0.3:
                continue

            # Try to find practitioner count or list
            practitioners = []
            for li in result.select("li, .practitioner"):
                text = li.get_text(strip=True)
                if re.match(r"^Dr\.?\s", text):
                    practitioners.append({"name": text, "specialty": "GP"})

            # Try to extract GP count from text
            gp_count = len(practitioners)
            if gp_count == 0:
                text = result.get_text()
                m = re.search(r"(\d+)\s+(?:GPs?|doctors?|practitioners?)", text, re.I)
                if m:
                    gp_count = int(m.group(1))

            if gp_count > 0:
                return {
                    "gp_count": gp_count,
                    "practitioners": practitioners,
                    "source_url": search_url,
                }
    except requests.RequestException as e:
        log.debug(f"HealthDirect search failed for '{centre_name}': {e}")

    return None


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

def _cache_path(centre_id: int) -> Path:
    return CACHE_DIR / f"centre_{centre_id}.json"


def save_cache(centre_id: int, data: dict):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with open(_cache_path(centre_id), "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def load_cache(centre_id: int) -> dict | None:
    p = _cache_path(centre_id)
    if p.exists():
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _extract_suburb(address: str) -> str:
    """Extract suburb from address like '1-3 Reeves Street, South Burnie TAS 7320'."""
    parts = [p.strip() for p in address.split(",")]
    if len(parts) >= 2:
        # Last part usually has 'SUBURB STATE POSTCODE'
        loc = parts[-1].strip()
        # Remove postcode
        loc = re.sub(r"\d{4}$", "", loc).strip()
        # Remove state
        loc = re.sub(r"\b(NSW|VIC|QLD|SA|WA|TAS|NT|ACT)\b", "", loc).strip()
        return loc
    return address


def run(state: str = None, limit: int = None, dry_run: bool = False):
    """Main entry point."""
    conn = sqlite3.connect(str(DB_PATH))
    ensure_columns(conn)

    centres = get_medical_centres(conn, state=state, limit=limit)
    log.info(f"Processing {len(centres)} medical centres")

    session = requests.Session()
    stats = {"updated": 0, "failed": 0, "skipped": 0, "cached": 0}

    for row in centres:
        centre_id, name, address, st, current_gps, current_prac = row
        log.info(f"[{centre_id}] {name} — {address}")

        # Check cache first
        cached = load_cache(centre_id)
        if cached and cached.get("gp_count"):
            log.info(f"  Using cached data ({cached['gp_count']} GPs)")
            update_medical_centre(
                conn, centre_id,
                num_gps=cached["gp_count"],
                practitioners_json=json.dumps(cached.get("practitioners", [])),
                source=cached.get("source", "cache"),
                dry_run=dry_run,
            )
            stats["cached"] += 1
            stats["updated"] += 1
            continue

        suburb = _extract_suburb(address)
        result = None

        # Source 1: Hotdoc
        try:
            time.sleep(RATE_LIMIT_SEC)
            result = search_hotdoc(name, suburb, session)
            if result:
                result["source"] = f"hotdoc:{result.get('source_url', '')}"
                log.info(f"  Hotdoc: found {result['gp_count']} GPs")
        except Exception as e:
            log.warning(f"  Hotdoc error: {e}")

        # Source 2: HealthDirect fallback
        if not result:
            try:
                time.sleep(RATE_LIMIT_SEC)
                result = search_healthdirect(name, suburb, st or "", session)
                if result:
                    result["source"] = f"healthdirect:{result.get('source_url', '')}"
                    log.info(f"  HealthDirect: found {result['gp_count']} GPs")
            except Exception as e:
                log.warning(f"  HealthDirect error: {e}")

        if result and result.get("gp_count", 0) > 0:
            save_cache(centre_id, result)
            update_medical_centre(
                conn, centre_id,
                num_gps=result["gp_count"],
                practitioners_json=json.dumps(result.get("practitioners", [])),
                source=result.get("source", "unknown"),
                dry_run=dry_run,
            )
            stats["updated"] += 1
        else:
            log.info(f"  No data found — skipping")
            save_cache(centre_id, {"gp_count": 0, "source": "not_found",
                                   "searched_at": datetime.now().isoformat()})
            stats["skipped"] += 1

    conn.close()

    # Summary
    log.info("=" * 50)
    log.info("HOTDOC GP SCRAPER — SUMMARY")
    log.info(f"  Total processed: {len(centres)}")
    log.info(f"  Updated:         {stats['updated']} ({stats['cached']} from cache)")
    log.info(f"  Skipped:         {stats['skipped']}")
    log.info(f"  Failed:          {stats['failed']}")
    log.info("=" * 50)

    return stats


def main():
    parser = argparse.ArgumentParser(description="Scrape Hotdoc/HealthDirect for verified GP counts")
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
