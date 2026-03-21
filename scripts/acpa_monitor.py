#!/usr/bin/env python3
"""
ACPA Competitor Monitor
=======================
Monitors publicly available ACPA/PBS data for competitor pharmacy applications.

Data sources:
1. health.gov.au ACPA pages - meeting dates, process info
2. PBS Approved Suppliers Portal status page (public portion)
3. Pharmacy Daily / AJP news articles about approvals
4. Pharmacy Guild / industry news
5. Google search for recent pharmacy approval announcements

Cross-references found applications against our v2_results to identify
threats to our target sites.

Usage:
    py -3.12 scripts/acpa_monitor.py [--verbose] [--force-refresh]
"""

import json
import os
import sys
import re
import sqlite3
import math
import logging
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import warnings
warnings.filterwarnings("ignore", category=DeprecationWarning)

try:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("Installing required packages...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "requests", "beautifulsoup4"])
    import requests
    from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
OUTPUT_DIR = PROJECT_ROOT / "output"
OUTPUT_FILE = OUTPUT_DIR / "competitor_applications.json"
DB_PATH = PROJECT_ROOT / "pharmacy_finder.db"
V2_NATIONAL = OUTPUT_DIR / "v2_results_national.json"
CACHE_DIR = PROJECT_ROOT / "cache"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
}

# ACPA-related URLs to monitor
ACPA_URLS = {
    "acpa_main": "https://www.health.gov.au/committees-and-groups/australian-community-pharmacy-authority-acpa",
    "acpa_rules": "https://www.health.gov.au/committees-and-groups/australian-community-pharmacy-authority-acpa/pharmacy-location-rules",
    "acpa_old": "https://www1.health.gov.au/internet/main/publishing.nsf/Content/acpa",
    "pbs_suppliers": "https://www.health.gov.au/our-work/pbs-approved-suppliers",
    "establish_new": "https://www.health.gov.au/our-work/pbs-approved-suppliers/pharmacists/establish-a-new-pharmacy",
    "relocate": "https://www.health.gov.au/our-work/pbs-approved-suppliers/pharmacists/relocate-a-pharmacy",
}

# News sources that report on pharmacy approvals
NEWS_SOURCES = [
    {
        "name": "Pharmacy Daily",
        "search_url": "https://pharmacydaily.com.au/?s=ACPA+approved+new+pharmacy",
        "base_url": "https://pharmacydaily.com.au",
    },
    {
        "name": "AJP",
        "search_url": "https://ajp.com.au/?s=ACPA+pharmacy+approval",
        "base_url": "https://ajp.com.au",
    },
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("acpa_monitor")


# ---------------------------------------------------------------------------
# Utility: Haversine distance
# ---------------------------------------------------------------------------
def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return distance in km between two lat/lon points."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# ---------------------------------------------------------------------------
# Load our target sites from v2_results
# ---------------------------------------------------------------------------
def load_our_sites() -> list[dict]:
    """Load v2_results from DB or JSON file."""
    sites = []
    
    # Try DB first
    if DB_PATH.exists():
        try:
            conn = sqlite3.connect(str(DB_PATH))
            conn.row_factory = sqlite3.Row
            c = conn.cursor()
            c.execute("""
                SELECT id, name, address, latitude, longitude, state, 
                       primary_rule, commercial_score, best_confidence
                FROM v2_results 
                WHERE passed_any = 1
            """)
            for row in c.fetchall():
                sites.append(dict(row))
            conn.close()
            log.info(f"Loaded {len(sites)} target sites from DB")
            return sites
        except Exception as e:
            log.warning(f"DB load failed: {e}")
    
    # Fallback to JSON
    if V2_NATIONAL.exists():
        with open(V2_NATIONAL, "r") as f:
            data = json.load(f)
        for r in data.get("results", []):
            sites.append({
                "id": r["id"],
                "name": r["name"],
                "address": r["address"],
                "latitude": r.get("latitude"),
                "longitude": r.get("longitude"),
                "state": r.get("state"),
                "primary_rule": r.get("primary_rule"),
                "commercial_score": r.get("commercial_score"),
                "best_confidence": r.get("best_confidence"),
            })
        log.info(f"Loaded {len(sites)} target sites from JSON")
    
    return sites


# ---------------------------------------------------------------------------
# Geocode an address (basic - using Nominatim)
# ---------------------------------------------------------------------------
def geocode_address(address: str) -> tuple[float, float] | None:
    """Geocode an address via Nominatim. Returns (lat, lon) or None."""
    try:
        resp = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": address, "format": "json", "limit": 1, "countrycodes": "au"},
            headers={**HEADERS, "User-Agent": "PharmacyFinder/1.0"},
            timeout=10,
        )
        results = resp.json()
        if results:
            return float(results[0]["lat"]), float(results[0]["lon"])
    except Exception as e:
        log.warning(f"Geocoding failed for '{address}': {e}")
    return None


# ---------------------------------------------------------------------------
# Cross-reference a location against our sites
# ---------------------------------------------------------------------------
def find_nearby_sites(lat: float, lon: float, our_sites: list[dict], max_km: float = 2.0) -> list[dict]:
    """Find our target sites within max_km of a given location."""
    nearby = []
    for site in our_sites:
        slat, slon = site.get("latitude"), site.get("longitude")
        if slat is None or slon is None:
            continue
        dist = haversine_km(lat, lon, slat, slon)
        if dist <= max_km:
            nearby.append({
                "site_id": site["id"],
                "site_name": site["name"],
                "site_address": site["address"],
                "distance_km": round(dist, 3),
                "primary_rule": site.get("primary_rule"),
                "commercial_score": site.get("commercial_score"),
            })
    nearby.sort(key=lambda x: x["distance_km"])
    return nearby


def assess_threat(nearby: list[dict]) -> str:
    """Assess threat level based on proximity."""
    if not nearby:
        return "none"
    closest = nearby[0]["distance_km"]
    if closest < 0.3:  # Within 300m - essentially same site
        return "high"
    elif closest < 1.0:
        return "medium"
    else:
        return "low"


# ---------------------------------------------------------------------------
# Scrape ACPA pages for any application/decision data
# ---------------------------------------------------------------------------
def scrape_acpa_pages() -> dict:
    """Scrape ACPA-related pages for meeting dates and any visible application data."""
    results = {
        "meeting_dates": [],
        "page_updates": {},
        "raw_text_snippets": [],
    }
    
    for name, url in ACPA_URLS.items():
        try:
            log.info(f"Fetching {name}: {url}")
            resp = requests.get(url, headers=HEADERS, timeout=15)
            if resp.status_code != 200:
                log.warning(f"  {name} returned {resp.status_code}")
                results["page_updates"][name] = {"status": resp.status_code, "url": url}
                continue
            
            soup = BeautifulSoup(resp.text, "html.parser")
            text = soup.get_text(separator="\n", strip=True)
            
            # Extract meeting dates from tables
            if name in ("acpa_rules", "acpa_old"):
                tables = soup.find_all("table")
                for table in tables:
                    rows = table.find_all("tr")
                    for row in rows:
                        cells = [td.get_text(strip=True) for td in row.find_all(["td", "th"])]
                        if len(cells) >= 2:
                            # Check if first cell looks like a date
                            date_text = cells[0]
                            if re.search(r'\d{1,2}\s+\w+\s+20\d{2}', date_text):
                                meeting = {
                                    "meeting_date": date_text,
                                    "lodgement_window": " to ".join(cells[1:]) if len(cells) > 1 else "",
                                }
                                results["meeting_dates"].append(meeting)
            
            # Look for any references to specific applications or suburbs
            # Pattern: look for suburb/state mentions near "application" or "approved"
            app_patterns = [
                r'application[s]?\s+(?:for|to|in)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)',
                r'(?:approved|rejected)\s+(?:for|in)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)',
                r'new\s+pharmacy\s+(?:in|at)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)',
                r'pharmacy\s+(?:at|in)\s+(\d+[^,]+,\s*[A-Z][a-z]+)',
            ]
            
            for pattern in app_patterns:
                matches = re.findall(pattern, text)
                for match in matches:
                    results["raw_text_snippets"].append({
                        "source": name,
                        "url": url,
                        "matched_text": match,
                        "pattern": pattern,
                    })
            
            # Track page last-updated dates
            date_updated = None
            for elem in soup.find_all(string=re.compile(r'Date last updated|Last updated|Updated')):
                parent = elem.parent
                if parent:
                    date_match = re.search(r'(\d{1,2}\s+\w+\s+20\d{2})', parent.get_text())
                    if date_match:
                        date_updated = date_match.group(1)
            
            results["page_updates"][name] = {
                "status": 200,
                "url": url,
                "last_updated": date_updated,
                "content_length": len(text),
            }
            
        except Exception as e:
            log.error(f"Error scraping {name}: {e}")
            results["page_updates"][name] = {"status": "error", "error": str(e), "url": url}
    
    return results


# ---------------------------------------------------------------------------
# Scrape news sources for pharmacy approval articles
# ---------------------------------------------------------------------------
def scrape_news_sources() -> list[dict]:
    """Scrape pharmacy industry news for ACPA decision reports."""
    articles = []
    
    for source in NEWS_SOURCES:
        try:
            log.info(f"Searching {source['name']}...")
            resp = requests.get(source["search_url"], headers=HEADERS, timeout=15)
            if resp.status_code != 200:
                log.warning(f"  {source['name']} returned {resp.status_code}")
                continue
            
            soup = BeautifulSoup(resp.text, "html.parser")
            
            # Find article links
            for article in soup.find_all("article")[:10]:
                title_elem = article.find(["h2", "h3", "h4"])
                if not title_elem:
                    continue
                
                link = title_elem.find("a")
                title = title_elem.get_text(strip=True)
                href = link["href"] if link and link.get("href") else ""
                
                # Only interested in articles about applications/approvals
                keywords = ["ACPA", "pharmacy approval", "new pharmacy", "s90", 
                           "section 90", "location rule", "PBS approval"]
                if not any(kw.lower() in title.lower() for kw in keywords):
                    continue
                
                # Get date if available
                date_elem = article.find("time") or article.find(class_=re.compile(r"date|time|published"))
                pub_date = date_elem.get_text(strip=True) if date_elem else ""
                
                if not href.startswith("http"):
                    href = source["base_url"].rstrip("/") + "/" + href.lstrip("/")
                
                articles.append({
                    "source": source["name"],
                    "title": title,
                    "url": href,
                    "date": pub_date,
                })
            
            log.info(f"  Found {len([a for a in articles if a['source'] == source['name']])} relevant articles")
            
        except Exception as e:
            log.error(f"Error scraping {source['name']}: {e}")
    
    # Try to extract application details from articles
    enriched = []
    for article in articles[:5]:  # Limit to avoid rate-limiting
        try:
            resp = requests.get(article["url"], headers=HEADERS, timeout=15)
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "html.parser")
                text = soup.get_text(separator=" ", strip=True)
                
                # Extract locations and items mentioned
                locations = re.findall(
                    r'(?:pharmacy|store|premises)\s+(?:at|in|on)\s+([^.]{5,60})',
                    text, re.IGNORECASE
                )
                items = re.findall(r'[Ii]tem\s+(1[3][0-6])', text)
                outcomes = re.findall(
                    r'(approved|rejected|not approved|deferred)',
                    text, re.IGNORECASE
                )
                
                article["extracted_locations"] = locations[:5]
                article["extracted_items"] = list(set(items))
                article["extracted_outcomes"] = list(set(o.lower() for o in outcomes))
            
            enriched.append(article)
        except Exception as e:
            log.warning(f"  Could not fetch article: {e}")
            enriched.append(article)
    
    return enriched


# ---------------------------------------------------------------------------
# Parse applications from the PBS Approved Suppliers portal (public parts)
# ---------------------------------------------------------------------------
def check_pbs_portal() -> dict:
    """Check the PBS Approved Suppliers Portal for any public data."""
    portal_url = "https://pbsapprovedsuppliers.health.gov.au/"
    info = {
        "portal_accessible": False,
        "notes": [],
    }
    
    try:
        resp = requests.get(portal_url, headers=HEADERS, timeout=15, allow_redirects=True)
        info["portal_accessible"] = resp.status_code == 200
        info["status_code"] = resp.status_code
        info["final_url"] = resp.url
        
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "html.parser")
            text = soup.get_text(separator="\n", strip=True)
            
            # Look for any publicly visible application/decision data
            if "login" in text.lower() or "sign in" in text.lower():
                info["notes"].append("Portal requires authentication - individual application data not publicly accessible")
                info["notes"].append("ACPA meeting outcomes are published on the Portal within 5 working days after a meeting")
                info["notes"].append("Only logged-in applicants and surrounding pharmacists can view specific decisions")
            
            # Check for any public notices or announcements
            notices = soup.find_all(class_=re.compile(r"notice|alert|announcement|banner"))
            for notice in notices:
                info["notes"].append(f"Portal notice: {notice.get_text(strip=True)[:200]}")
    
    except Exception as e:
        info["error"] = str(e)
    
    return info


# ---------------------------------------------------------------------------
# Build competitor application records from all sources
# ---------------------------------------------------------------------------
def build_competitor_records(
    acpa_data: dict,
    news_articles: list[dict],
    portal_info: dict,
    our_sites: list[dict],
) -> list[dict]:
    """
    Compile all found competitor application data into structured records.
    
    Note: ACPA individual application data is NOT publicly available.
    Meeting outcomes are only published on the authenticated PBS Approved Suppliers Portal.
    This function compiles what IS publicly findable from news and government pages.
    """
    records = []
    seen_keys = set()
    
    # From news articles that mention specific locations/decisions
    for article in news_articles:
        locations = article.get("extracted_locations", [])
        items = article.get("extracted_items", [])
        outcomes = article.get("extracted_outcomes", [])
        
        for loc in locations:
            loc_clean = loc.strip().rstrip(".")
            key = f"{loc_clean}_{article.get('date', '')}"
            if key in seen_keys:
                continue
            seen_keys.add(key)
            
            # Try to geocode for cross-reference
            coords = geocode_address(loc_clean + ", Australia")
            nearby = []
            threat = "unknown"
            if coords:
                nearby = find_nearby_sites(coords[0], coords[1], our_sites)
                threat = assess_threat(nearby)
            
            record = {
                "application_date": article.get("date", "unknown"),
                "applicant": "unknown (not public)",
                "proposed_address": loc_clean,
                "proposed_item": f"Item {items[0]}" if items else "unknown",
                "decision": outcomes[0] if outcomes else "unknown",
                "decision_date": article.get("date", "unknown"),
                "source": article.get("source", ""),
                "source_url": article.get("url", ""),
                "geocoded_lat": coords[0] if coords else None,
                "geocoded_lon": coords[1] if coords else None,
                "nearby_our_sites": nearby,
                "threat_level": threat,
                "data_quality": "low - extracted from news article",
            }
            records.append(record)
    
    # From raw text snippets on ACPA pages
    for snippet in acpa_data.get("raw_text_snippets", []):
        matched = snippet.get("matched_text", "")
        if not matched or len(matched) < 3:
            continue
        
        key = f"acpa_page_{matched}"
        if key in seen_keys:
            continue
        seen_keys.add(key)
        
        coords = geocode_address(matched + ", Australia")
        nearby = []
        threat = "unknown"
        if coords:
            nearby = find_nearby_sites(coords[0], coords[1], our_sites)
            threat = assess_threat(nearby)
        
        record = {
            "application_date": "unknown",
            "applicant": "unknown (not public)",
            "proposed_address": matched,
            "proposed_item": "unknown",
            "decision": "unknown",
            "decision_date": "unknown",
            "source": f"health.gov.au ({snippet.get('source', '')})",
            "source_url": snippet.get("url", ""),
            "geocoded_lat": coords[0] if coords else None,
            "geocoded_lon": coords[1] if coords else None,
            "nearby_our_sites": nearby,
            "threat_level": threat,
            "data_quality": "low - extracted from government page text",
        }
        records.append(record)
    
    return records


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    verbose = "--verbose" in sys.argv or "-v" in sys.argv
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    log.info("=" * 60)
    log.info("ACPA Competitor Monitor")
    log.info("=" * 60)
    
    # Load our target sites
    our_sites = load_our_sites()
    if not our_sites:
        log.error("No target sites loaded. Run scoring first.")
        return
    
    log.info(f"Monitoring against {len(our_sites)} target sites")
    
    # 1. Scrape ACPA pages
    log.info("\n--- Scraping ACPA pages ---")
    acpa_data = scrape_acpa_pages()
    
    # 2. Scrape news sources
    log.info("\n--- Scraping news sources ---")
    news_articles = scrape_news_sources()
    
    # 3. Check PBS Portal
    log.info("\n--- Checking PBS Portal ---")
    portal_info = check_pbs_portal()
    
    # 4. Build competitor records
    log.info("\n--- Building competitor records ---")
    records = build_competitor_records(acpa_data, news_articles, portal_info, our_sites)
    
    # 5. Compile output
    output = {
        "generated": datetime.now().isoformat(),
        "monitor_version": "1.0.0",
        "summary": {
            "total_applications_found": len(records),
            "high_threat": sum(1 for r in records if r["threat_level"] == "high"),
            "medium_threat": sum(1 for r in records if r["threat_level"] == "medium"),
            "low_threat": sum(1 for r in records if r["threat_level"] == "low"),
            "our_target_sites_monitored": len(our_sites),
        },
        "data_sources_checked": {
            "acpa_pages": acpa_data["page_updates"],
            "news_sources": [
                {"name": s["name"], "articles_found": len([a for a in news_articles if a["source"] == s["name"]])}
                for s in NEWS_SOURCES
            ],
            "pbs_portal": portal_info,
        },
        "acpa_meeting_schedule": acpa_data["meeting_dates"],
        "next_meeting": _get_next_meeting(acpa_data["meeting_dates"]),
        "applications": records,
        "important_notes": [
            "Individual ACPA application data is NOT publicly available.",
            "Meeting outcomes are published on the authenticated PBS Approved Suppliers Portal only.",
            "Applicant names and specific addresses are confidential until approval is granted.",
            "This monitor aggregates publicly available information from news and government sources.",
            "For comprehensive competitor monitoring, consider:",
            "  - Registering on the PBS Approved Suppliers Portal to receive notifications",
            "  - Monitoring local development applications and commercial leasing for pharmacy fitouts",
            "  - Tracking new pharmacy board registrations in target areas",
            "  - Setting up Google Alerts for 'new pharmacy' + target suburb names",
        ],
        "recommended_actions": _generate_recommendations(records, acpa_data),
    }
    
    # Save
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w") as f:
        json.dump(output, f, indent=2, default=str)
    
    log.info(f"\n{'=' * 60}")
    log.info(f"Results saved to {OUTPUT_FILE}")
    log.info(f"Total applications found: {len(records)}")
    log.info(f"  High threat: {output['summary']['high_threat']}")
    log.info(f"  Medium threat: {output['summary']['medium_threat']}")
    log.info(f"  Low threat: {output['summary']['low_threat']}")
    if output.get("next_meeting"):
        log.info(f"Next ACPA meeting: {output['next_meeting']}")
    log.info("=" * 60)
    
    return output


def _get_next_meeting(meetings: list[dict]) -> str | None:
    """Find the next upcoming ACPA meeting date."""
    now = datetime.now()
    for m in meetings:
        date_str = m.get("meeting_date", "")
        try:
            # Try parsing "27 March 2026" format
            meeting_date = datetime.strptime(date_str, "%d %B %Y")
            if meeting_date > now:
                return date_str
        except ValueError:
            # Try alternate formats
            for fmt in ["%d %b %Y", "%B %d, %Y", "%d/%m/%Y"]:
                try:
                    meeting_date = datetime.strptime(date_str, fmt)
                    if meeting_date > now:
                        return date_str
                    break
                except ValueError:
                    continue
    return None


def _generate_recommendations(records: list[dict], acpa_data: dict) -> list[str]:
    """Generate actionable recommendations based on findings."""
    recs = []
    
    high_threats = [r for r in records if r["threat_level"] == "high"]
    if high_threats:
        recs.append(f"⚠️ HIGH PRIORITY: {len(high_threats)} competitor application(s) found near our target sites!")
        for t in high_threats:
            recs.append(f"  - {t['proposed_address']} (near {t['nearby_our_sites'][0]['site_name'] if t['nearby_our_sites'] else 'unknown'})")
    
    medium_threats = [r for r in records if r["threat_level"] == "medium"]
    if medium_threats:
        recs.append(f"⚡ WATCH: {len(medium_threats)} competitor application(s) within 1km of our sites")
    
    if not records:
        recs.append("✅ No competitor applications detected in publicly available data")
        recs.append("Note: This does not mean no applications exist - most data is behind the PBS Portal")
    
    # Meeting-based recommendations
    next_meeting = _get_next_meeting(acpa_data.get("meeting_dates", []))
    if next_meeting:
        recs.append(f"📅 Next ACPA meeting: {next_meeting}")
        recs.append("   Consider lodging applications before the cut-off for this meeting")
    
    recs.append("🔍 Recommended: Set up Google Alerts for 'new pharmacy' + each target suburb")
    recs.append("🔍 Recommended: Register on PBS Approved Suppliers Portal for official notifications")
    recs.append("🔍 Recommended: Monitor commercial leasing in target areas for pharmacy fitout activity")
    
    return recs


if __name__ == "__main__":
    main()
