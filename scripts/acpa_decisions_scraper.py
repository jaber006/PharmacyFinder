#!/usr/bin/env python3
"""
ACPA Historical Decisions Scraper
=================================
Scrapes and compiles historical ACPA meeting outcomes/decisions from
publicly available sources to build a database of past approval/rejection
patterns. This helps predict success rates for our target sites.

Data sources:
1. Pharmacy Daily articles reporting ACPA decisions
2. AJP (Australian Journal of Pharmacy) news
3. AAT (Administrative Appeals Tribunal) decisions involving ACPA
4. Pharmacy Guild reports and submissions
5. health.gov.au archived content

The key insight: individual ACPA decisions are NOT published publicly.
However, AAT appeals (when applicants challenge rejections) ARE public
and contain detailed reasoning about specific locations and items.

Usage:
    py -3.12 scripts/acpa_decisions_scraper.py [--verbose] [--rebuild]
"""

import json
import os
import re
import sys
import sqlite3
import logging
import time
from datetime import datetime
from pathlib import Path

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
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "requests", "beautifulsoup4"])
    import requests
    from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DB_PATH = PROJECT_ROOT / "pharmacy_finder.db"
CACHE_DIR = PROJECT_ROOT / "cache" / "acpa_decisions"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
}

# AAT decisions are the richest public source of ACPA decision data
AAT_SEARCH_URL = "https://www.austlii.edu.au/cgi-bin/viewdb/au/cases/cth/AATA/"
AAT_SEARCH_QUERY = "https://www.austlii.edu.au/cgi-bin/sinosrch.cgi"

# Pharmacy industry news sources
SEARCH_URLS = {
    "pharmacy_daily_acpa": "https://pharmacydaily.com.au/?s=ACPA",
    "pharmacy_daily_s90": "https://pharmacydaily.com.au/?s=section+90+pharmacy",
    "ajp_acpa": "https://ajp.com.au/?s=ACPA+decision",
    "ajp_location_rules": "https://ajp.com.au/?s=pharmacy+location+rules",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("acpa_decisions")


# ---------------------------------------------------------------------------
# Database setup
# ---------------------------------------------------------------------------
def setup_database():
    """Create the acpa_decisions table if it doesn't exist."""
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    c = conn.cursor()
    
    c.execute("""
        CREATE TABLE IF NOT EXISTS acpa_decisions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            decision_date TEXT,
            item_number TEXT,
            state TEXT,
            suburb TEXT,
            address TEXT,
            outcome TEXT,  -- approved, rejected, deferred, overturned_on_appeal
            notes TEXT,
            source TEXT,
            source_url TEXT,
            applicant TEXT,
            case_number TEXT,
            extracted_at TEXT,
            raw_text TEXT
        )
    """)
    
    # Index for quick lookups
    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_acpa_state_suburb 
        ON acpa_decisions(state, suburb)
    """)
    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_acpa_item 
        ON acpa_decisions(item_number)
    """)
    c.execute("""
        CREATE INDEX IF NOT EXISTS idx_acpa_outcome 
        ON acpa_decisions(outcome)
    """)
    
    conn.commit()
    conn.close()
    log.info("Database table 'acpa_decisions' ready")


# ---------------------------------------------------------------------------
# Scrape AAT decisions (richest public source)
# ---------------------------------------------------------------------------
def scrape_aat_decisions() -> list[dict]:
    """
    Scrape Administrative Appeals Tribunal decisions involving ACPA.
    
    These are appeals of ACPA rejection decisions and contain detailed
    information about specific locations, items applied under, and
    reasoning for approval/rejection.
    
    Strategy: Browse AAT year index pages on AustLII looking for
    pharmacy-related case titles, plus search via Google.
    """
    decisions = []
    seen_urls = set()
    
    # Strategy 1: Google search for AAT ACPA decisions on AustLII
    google_queries = [
        'site:austlii.edu.au "AATA" "Australian Community Pharmacy Authority"',
        'site:austlii.edu.au "AATA" "pharmacy" "section 90"',
        'site:austlii.edu.au AATA "pharmacy location rules"',
    ]
    
    for query in google_queries:
        try:
            log.info(f"Searching Google for AAT cases: {query[:60]}...")
            # Use a simple Google search scrape
            resp = requests.get(
                "https://www.google.com/search",
                params={"q": query, "num": 10},
                headers={**HEADERS, "Accept-Language": "en-AU,en;q=0.9"},
                timeout=15,
            )
            if resp.status_code == 200:
                soup = BeautifulSoup(resp.text, "html.parser")
                for link in soup.find_all("a", href=True):
                    href = link["href"]
                    # Extract actual URL from Google redirect
                    if "/url?q=" in href:
                        href = href.split("/url?q=")[1].split("&")[0]
                    if "austlii.edu.au" in href and "AATA" in href and href not in seen_urls:
                        seen_urls.add(href)
                        title = link.get_text(strip=True)
                        log.info(f"  Found AAT case: {title[:60]}...")
                        decision = _parse_aat_case(href, title)
                        if decision:
                            decisions.append(decision)
                        time.sleep(1)
        except Exception as e:
            log.warning(f"  Google search error: {e}")
    
    # Strategy 2: Try AustLII year index pages for recent years
    for year in range(2024, 2018, -1):
        try:
            url = f"https://www.austlii.edu.au/cgi-bin/viewdb/au/cases/cth/AATA/{year}/"
            log.info(f"Checking AAT index for {year}...")
            resp = requests.get(url, headers=HEADERS, timeout=15)
            if resp.status_code != 200:
                log.info(f"  Year {year}: status {resp.status_code}")
                continue
            
            soup = BeautifulSoup(resp.text, "html.parser")
            links = soup.find_all("a", href=True)
            
            pharmacy_links = []
            for link in links:
                text = link.get_text(strip=True).lower()
                if any(kw in text for kw in ["pharmacy", "pharmacist", "acpa", "pharmaceutical benefit"]):
                    href = link["href"]
                    if not href.startswith("http"):
                        href = f"https://www.austlii.edu.au{href}"
                    if href not in seen_urls:
                        seen_urls.add(href)
                        pharmacy_links.append((href, link.get_text(strip=True)))
            
            log.info(f"  Year {year}: {len(pharmacy_links)} pharmacy-related cases")
            
            for href, title in pharmacy_links[:5]:  # Limit per year
                decision = _parse_aat_case(href, title)
                if decision:
                    decisions.append(decision)
                time.sleep(0.5)
                
        except Exception as e:
            log.warning(f"  Error for year {year}: {e}")
    
    # Strategy 3: Known important ACPA AAT cases (manual seeds)
    known_cases = [
        # Add known case URLs here as they're discovered
        # These are public AAT decisions where ACPA rejections were appealed
    ]
    
    for case_url in known_cases:
        if case_url not in seen_urls:
            seen_urls.add(case_url)
            decision = _parse_aat_case(case_url, "")
            if decision:
                decisions.append(decision)
    
    log.info(f"Total AAT decisions found: {len(decisions)}")
    return decisions


def _parse_aat_case(url: str, title: str) -> dict | None:
    """Parse an individual AAT case for ACPA decision details."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            return None
        
        soup = BeautifulSoup(resp.text, "html.parser")
        text = soup.get_text(separator=" ", strip=True)
        # Clean up excessive whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Extract case metadata
        case_num = re.search(r'\[(\d{4})\]\s*AATA\s*(\d+)', text)
        case_number = f"[{case_num.group(1)}] AATA {case_num.group(2)}" if case_num else ""
        
        # Extract year
        year = case_num.group(1) if case_num else ""
        
        # Extract decision date
        date_match = re.search(
            r'(?:Date of decision|Decision date|DATED?)[:;\s]+(\d{1,2}\s+\w+\s+\d{4})',
            text, re.IGNORECASE
        )
        decision_date = date_match.group(1) if date_match else year
        
        # Extract item number
        item_match = re.search(r'[Ii]tem\s+(1[3][0-6])', text)
        item_number = f"Item {item_match.group(1)}" if item_match else ""
        
        # Extract location/suburb
        suburb = ""
        state = ""
        
        # Look for address patterns
        addr_patterns = [
            r'(?:premises|pharmacy|located)\s+at\s+([^.]{10,80})',
            r'(\d+[^,]+,\s*[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s*(?:NSW|VIC|QLD|SA|WA|TAS|NT|ACT))',
            r'in\s+the\s+suburb\s+of\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)',
            r'(?:at|in)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*),?\s+(NSW|VIC|QLD|SA|WA|TAS|NT|ACT)',
        ]
        
        for pattern in addr_patterns:
            match = re.search(pattern, text)
            if match:
                suburb = match.group(1).strip().rstrip(",.")
                if match.lastindex and match.lastindex > 1:
                    state = match.group(2)
                break
        
        # Extract state if not found
        if not state:
            state_match = re.search(
                r'\b(NSW|VIC|QLD|SA|WA|TAS|NT|ACT)\b',
                suburb + " " + text[:2000]
            )
            if state_match:
                state = state_match.group(1)
        
        # Determine outcome
        outcome = "unknown"
        text_lower = text.lower()
        if "set aside" in text_lower and "substitut" in text_lower:
            outcome = "overturned_on_appeal"  # AAT overturned ACPA rejection
        elif "affirm" in text_lower and ("decision" in text_lower or "acpa" in text_lower):
            outcome = "rejected"  # AAT upheld ACPA rejection
        elif "application.*approved" in text_lower:
            outcome = "approved"
        elif "not approved" in text_lower or "reject" in text_lower:
            outcome = "rejected"
        
        # Extract applicant
        applicant = ""
        app_match = re.search(
            r'(?:applicant|appellant)[:\s]+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,3})',
            text[:2000]
        )
        if app_match:
            applicant = app_match.group(1)
        
        # Extract key reasoning (first 500 chars of decision section)
        notes = ""
        reason_match = re.search(
            r'(?:REASONS FOR DECISION|DECISION|CONCLUSION)(.*?)(?:ORDERS|$)',
            text, re.DOTALL | re.IGNORECASE
        )
        if reason_match:
            notes = reason_match.group(1).strip()[:500]
        
        if not (item_number or suburb or "pharmacy" in text_lower[:1000]):
            return None  # Not a pharmacy case
        
        return {
            "decision_date": decision_date,
            "item_number": item_number,
            "state": state,
            "suburb": suburb,
            "address": suburb,  # Best we have
            "outcome": outcome,
            "notes": notes,
            "source": "AAT",
            "source_url": url,
            "applicant": applicant,
            "case_number": case_number,
            "raw_text": text[:3000],  # First 3k chars for reference
        }
        
    except Exception as e:
        log.warning(f"Error parsing AAT case {url}: {e}")
        return None


# ---------------------------------------------------------------------------
# Scrape pharmacy news for decision reports
# ---------------------------------------------------------------------------
def scrape_pharmacy_news() -> list[dict]:
    """Scrape pharmacy industry news for ACPA decision reports."""
    decisions = []
    
    for name, url in SEARCH_URLS.items():
        try:
            log.info(f"Searching {name}...")
            resp = requests.get(url, headers=HEADERS, timeout=15)
            if resp.status_code != 200:
                log.warning(f"  {name} returned {resp.status_code}")
                continue
            
            soup = BeautifulSoup(resp.text, "html.parser")
            
            # Find all links that look like article results
            # (Pharmacy Daily and AJP list results as links within the page)
            article_links = []
            
            # Method 1: Standard article elements
            for article in soup.find_all("article"):
                for link in article.find_all("a", href=True):
                    href = link["href"]
                    text = link.get_text(strip=True)
                    if text and len(text) > 10 and "/news/" in href:
                        article_links.append((href, text))
            
            # Method 2: Any link containing /news/ from the search results
            if not article_links:
                for link in soup.find_all("a", href=True):
                    href = link["href"]
                    text = link.get_text(strip=True)
                    if not text or len(text) < 10:
                        continue
                    if "/news/" in href or "/article/" in href:
                        # Only pharmacy-related
                        keywords = ["ACPA", "approved", "rejected", "application", "section 90",
                                   "s90", "new pharmacy", "pharmacy approval", "location rule",
                                   "pharmacy authority"]
                        if any(kw.lower() in text.lower() for kw in keywords):
                            article_links.append((href, text))
            
            log.info(f"  Found {len(article_links)} relevant article links")
            
            # Process each article (limit to avoid rate-limiting)
            for href, title in article_links[:8]:
                if not href.startswith("http"):
                    base = url.split("/?")[0]
                    href = base.rstrip("/") + "/" + href.lstrip("/")
                
                try:
                    time.sleep(0.5)
                    art_resp = requests.get(href, headers=HEADERS, timeout=15)
                    if art_resp.status_code == 200:
                        art_soup = BeautifulSoup(art_resp.text, "html.parser")
                        
                        # Try to get publication date
                        pub_date = ""
                        time_elem = art_soup.find("time")
                        if time_elem:
                            pub_date = time_elem.get("datetime", time_elem.get_text(strip=True))
                        if not pub_date:
                            date_elem = art_soup.find(class_=re.compile(r"date|published|posted"))
                            if date_elem:
                                pub_date = date_elem.get_text(strip=True)
                        
                        content = art_soup.find("article") or art_soup.find(class_=re.compile(r"content|entry|post"))
                        if content:
                            art_text = content.get_text(separator=" ", strip=True)
                        else:
                            art_text = art_soup.get_text(separator=" ", strip=True)
                        
                        extracted = _extract_decisions_from_text(art_text, href, name, pub_date)
                        decisions.extend(extracted)
                        if extracted:
                            log.info(f"  Extracted {len(extracted)} decisions from: {title[:60]}")
                except Exception as e:
                    log.warning(f"  Could not fetch article {href}: {e}")
            
            log.info(f"  Total from {name}: {len([d for d in decisions if d['source'] == name])} decisions")
            
        except Exception as e:
            log.error(f"Error scraping {name}: {e}")
    
    return decisions


def _extract_decisions_from_text(text: str, url: str, source: str, pub_date: str) -> list[dict]:
    """Extract structured decision data from article text."""
    decisions = []
    
    # Pattern: "Item 13X application for [location]"
    item_loc_pattern = re.compile(
        r'[Ii]tem\s+(1[3][0-6])\s+(?:application\s+)?(?:for|in|at)\s+([^,.]{3,60})',
    )
    
    for match in item_loc_pattern.finditer(text):
        item = f"Item {match.group(1)}"
        location = match.group(2).strip()
        
        # Determine outcome from surrounding text
        context = text[max(0, match.start() - 100):match.end() + 200]
        outcome = "unknown"
        if re.search(r'approved|approval granted|recommended', context, re.IGNORECASE):
            outcome = "approved"
        elif re.search(r'rejected|not approved|refused|denied', context, re.IGNORECASE):
            outcome = "rejected"
        elif re.search(r'deferred', context, re.IGNORECASE):
            outcome = "deferred"
        
        # Extract state
        state_match = re.search(r'\b(NSW|VIC|QLD|SA|WA|TAS|NT|ACT)\b', location + " " + context)
        state = state_match.group(1) if state_match else ""
        
        decisions.append({
            "decision_date": pub_date,
            "item_number": item,
            "state": state,
            "suburb": location,
            "address": location,
            "outcome": outcome,
            "notes": context.strip()[:300],
            "source": source,
            "source_url": url,
            "applicant": "",
            "case_number": "",
            "raw_text": "",
        })
    
    # Also look for general approval/rejection mentions without item numbers
    general_patterns = [
        (r'(?:new\s+pharmacy|pharmacy\s+application)\s+(?:at|in|for)\s+([^,.]{3,60})\s+(?:was\s+)?(?:approved|recommended)', "approved"),
        (r'(?:new\s+pharmacy|pharmacy\s+application)\s+(?:at|in|for)\s+([^,.]{3,60})\s+(?:was\s+)?(?:rejected|not\s+approved)', "rejected"),
        (r'ACPA\s+(?:approved|recommended)\s+(?:a\s+)?(?:new\s+)?pharmacy\s+(?:at|in)\s+([^,.]{3,60})', "approved"),
        (r'ACPA\s+(?:rejected|refused)\s+(?:a\s+)?(?:new\s+)?pharmacy\s+(?:at|in)\s+([^,.]{3,60})', "rejected"),
    ]
    
    for pattern, outcome in general_patterns:
        for match in re.finditer(pattern, text, re.IGNORECASE):
            location = match.group(1).strip()
            state_match = re.search(r'\b(NSW|VIC|QLD|SA|WA|TAS|NT|ACT)\b', location)
            state = state_match.group(1) if state_match else ""
            
            # Avoid duplicates
            if any(d["suburb"] == location for d in decisions):
                continue
            
            decisions.append({
                "decision_date": pub_date,
                "item_number": "",
                "state": state,
                "suburb": location,
                "address": location,
                "outcome": outcome,
                "notes": match.group(0)[:300],
                "source": source,
                "source_url": url,
                "applicant": "",
                "case_number": "",
                "raw_text": "",
            })
    
    return decisions


# ---------------------------------------------------------------------------
# Scrape ACPA annual report statistics
# ---------------------------------------------------------------------------
def scrape_acpa_statistics() -> list[dict]:
    """
    Try to find ACPA annual report data with aggregate statistics.
    The ACPA publishes annual reports with approval/rejection counts.
    """
    stats_decisions = []
    
    # Search for ACPA annual reports
    search_urls = [
        "https://www.health.gov.au/resources?query=ACPA+annual+report&sort_by=field_h_date_published_value&sort_order=DESC",
        "https://www.health.gov.au/committees-and-groups/australian-community-pharmacy-authority-acpa",
    ]
    
    for url in search_urls:
        try:
            log.info(f"Checking for ACPA reports: {url}")
            resp = requests.get(url, headers=HEADERS, timeout=15)
            if resp.status_code != 200:
                continue
            
            soup = BeautifulSoup(resp.text, "html.parser")
            
            # Look for PDF links to annual reports
            pdf_links = soup.find_all("a", href=re.compile(r'\.pdf', re.IGNORECASE))
            for link in pdf_links:
                href = link.get("href", "")
                text = link.get_text(strip=True)
                if any(kw in text.lower() for kw in ["annual report", "acpa report", "statistics"]):
                    log.info(f"  Found report link: {text} -> {href}")
                    # Note: We'd need to download and parse PDFs for full extraction
                    # For now, log it as a data source
                    stats_decisions.append({
                        "decision_date": "",
                        "item_number": "aggregate",
                        "state": "ALL",
                        "suburb": "national",
                        "address": "",
                        "outcome": "statistics",
                        "notes": f"Annual report available: {text}",
                        "source": "ACPA Annual Report",
                        "source_url": href if href.startswith("http") else f"https://www.health.gov.au{href}",
                        "applicant": "",
                        "case_number": "",
                        "raw_text": "",
                    })
        except Exception as e:
            log.warning(f"Error checking for reports: {e}")
    
    return stats_decisions


# ---------------------------------------------------------------------------
# Save to database
# ---------------------------------------------------------------------------
def save_to_database(decisions: list[dict], rebuild: bool = False):
    """Save decisions to the acpa_decisions table."""
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    c = conn.cursor()
    
    if rebuild:
        c.execute("DELETE FROM acpa_decisions")
        log.info("Cleared existing acpa_decisions data")
    
    inserted = 0
    skipped = 0
    
    for d in decisions:
        # Check for duplicates
        c.execute("""
            SELECT id FROM acpa_decisions 
            WHERE source_url = ? AND suburb = ? AND item_number = ?
            LIMIT 1
        """, (d.get("source_url", ""), d.get("suburb", ""), d.get("item_number", "")))
        
        if c.fetchone():
            skipped += 1
            continue
        
        c.execute("""
            INSERT INTO acpa_decisions 
            (decision_date, item_number, state, suburb, address, outcome, notes,
             source, source_url, applicant, case_number, extracted_at, raw_text)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            d.get("decision_date", ""),
            d.get("item_number", ""),
            d.get("state", ""),
            d.get("suburb", ""),
            d.get("address", ""),
            d.get("outcome", "unknown"),
            d.get("notes", ""),
            d.get("source", ""),
            d.get("source_url", ""),
            d.get("applicant", ""),
            d.get("case_number", ""),
            datetime.now().isoformat(),
            d.get("raw_text", "")[:5000],
        ))
        inserted += 1
    
    conn.commit()
    
    # Get totals
    c.execute("SELECT COUNT(*) FROM acpa_decisions")
    total = c.fetchone()[0]
    
    c.execute("""
        SELECT outcome, COUNT(*) FROM acpa_decisions 
        GROUP BY outcome ORDER BY COUNT(*) DESC
    """)
    by_outcome = dict(c.fetchall())
    
    c.execute("""
        SELECT item_number, COUNT(*) FROM acpa_decisions 
        WHERE item_number != '' AND item_number != 'aggregate'
        GROUP BY item_number ORDER BY COUNT(*) DESC
    """)
    by_item = dict(c.fetchall())
    
    c.execute("""
        SELECT state, COUNT(*) FROM acpa_decisions 
        WHERE state != '' AND state != 'ALL'
        GROUP BY state ORDER BY COUNT(*) DESC
    """)
    by_state = dict(c.fetchall())
    
    conn.close()
    
    log.info(f"\nDatabase update: {inserted} inserted, {skipped} duplicates skipped")
    log.info(f"Total records in acpa_decisions: {total}")
    log.info(f"By outcome: {json.dumps(by_outcome)}")
    log.info(f"By item: {json.dumps(by_item)}")
    log.info(f"By state: {json.dumps(by_state)}")
    
    return {
        "inserted": inserted,
        "skipped": skipped,
        "total": total,
        "by_outcome": by_outcome,
        "by_item": by_item,
        "by_state": by_state,
    }


# ---------------------------------------------------------------------------
# Generate analysis report
# ---------------------------------------------------------------------------
def generate_analysis() -> dict:
    """Generate an analysis of historical decisions to predict success rates."""
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    c = conn.cursor()
    
    analysis = {
        "generated": datetime.now().isoformat(),
        "overall": {},
        "by_item": {},
        "by_state": {},
        "insights": [],
    }
    
    # Overall stats
    c.execute("SELECT COUNT(*) FROM acpa_decisions WHERE outcome != 'statistics'")
    total = c.fetchone()[0]
    
    if total == 0:
        analysis["insights"].append("No decision data available yet. Run scraper to collect data.")
        conn.close()
        return analysis
    
    c.execute("SELECT COUNT(*) FROM acpa_decisions WHERE outcome = 'approved'")
    approved = c.fetchone()[0]
    
    c.execute("SELECT COUNT(*) FROM acpa_decisions WHERE outcome = 'rejected'")
    rejected = c.fetchone()[0]
    
    c.execute("SELECT COUNT(*) FROM acpa_decisions WHERE outcome = 'overturned_on_appeal'")
    overturned = c.fetchone()[0]
    
    analysis["overall"] = {
        "total_decisions": total,
        "approved": approved,
        "rejected": rejected,
        "overturned_on_appeal": overturned,
        "approval_rate": round(approved / total * 100, 1) if total > 0 else 0,
        "appeal_success_rate": round(overturned / (overturned + rejected) * 100, 1) if (overturned + rejected) > 0 else 0,
    }
    
    # By item analysis
    c.execute("""
        SELECT item_number, outcome, COUNT(*) 
        FROM acpa_decisions 
        WHERE item_number != '' AND item_number != 'aggregate'
        GROUP BY item_number, outcome
    """)
    
    item_data = {}
    for row in c.fetchall():
        item, outcome, count = row
        if item not in item_data:
            item_data[item] = {"total": 0, "approved": 0, "rejected": 0, "other": 0}
        item_data[item]["total"] += count
        if outcome == "approved":
            item_data[item]["approved"] += count
        elif outcome == "rejected":
            item_data[item]["rejected"] += count
        else:
            item_data[item]["other"] += count
    
    for item, data in item_data.items():
        data["approval_rate"] = round(data["approved"] / data["total"] * 100, 1) if data["total"] > 0 else 0
        analysis["by_item"][item] = data
    
    # Generate insights
    if analysis["overall"]["approval_rate"] > 0:
        analysis["insights"].append(
            f"Overall ACPA approval rate from available data: {analysis['overall']['approval_rate']}%"
        )
    
    if overturned > 0:
        analysis["insights"].append(
            f"{overturned} decisions were overturned on appeal to the AAT "
            f"({analysis['overall']['appeal_success_rate']}% appeal success rate)"
        )
    
    # Item-specific insights
    for item, data in sorted(analysis["by_item"].items()):
        if data["total"] >= 2:
            analysis["insights"].append(
                f"{item}: {data['approval_rate']}% approval rate "
                f"({data['approved']}/{data['total']} decisions found)"
            )
    
    analysis["insights"].append(
        "Note: This analysis is based on publicly available data only. "
        "Actual ACPA statistics may differ significantly. "
        "AAT cases skew toward rejections (as only rejections are appealed)."
    )
    
    conn.close()
    return analysis


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    verbose = "--verbose" in sys.argv or "-v" in sys.argv
    rebuild = "--rebuild" in sys.argv
    
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    log.info("=" * 60)
    log.info("ACPA Historical Decisions Scraper")
    log.info("=" * 60)
    
    # Setup
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    setup_database()
    
    all_decisions = []
    
    # 1. Scrape AAT decisions (richest source)
    log.info("\n--- Scraping AAT decisions ---")
    aat_decisions = scrape_aat_decisions()
    all_decisions.extend(aat_decisions)
    log.info(f"AAT: {len(aat_decisions)} decisions found")
    
    # 2. Scrape pharmacy news
    log.info("\n--- Scraping pharmacy news ---")
    news_decisions = scrape_pharmacy_news()
    all_decisions.extend(news_decisions)
    log.info(f"News: {len(news_decisions)} decisions found")
    
    # 3. Check for ACPA annual reports
    log.info("\n--- Checking for ACPA reports ---")
    stats = scrape_acpa_statistics()
    all_decisions.extend(stats)
    log.info(f"Reports: {len(stats)} entries found")
    
    # 4. Save to database
    log.info("\n--- Saving to database ---")
    db_stats = save_to_database(all_decisions, rebuild=rebuild)
    
    # 5. Generate analysis
    log.info("\n--- Generating analysis ---")
    analysis = generate_analysis()
    
    # Save analysis to file
    analysis_path = PROJECT_ROOT / "output" / "acpa_decision_analysis.json"
    with open(analysis_path, "w") as f:
        json.dump(analysis, f, indent=2)
    log.info(f"Analysis saved to {analysis_path}")
    
    # Print summary
    log.info(f"\n{'=' * 60}")
    log.info("SUMMARY")
    log.info(f"{'=' * 60}")
    log.info(f"Total decisions scraped this run: {len(all_decisions)}")
    log.info(f"New records inserted: {db_stats['inserted']}")
    log.info(f"Duplicates skipped: {db_stats['skipped']}")
    log.info(f"Total in database: {db_stats['total']}")
    
    if analysis.get("insights"):
        log.info("\nInsights:")
        for insight in analysis["insights"]:
            log.info(f"  • {insight}")
    
    log.info(f"\nData sources: AAT ({len(aat_decisions)}), "
             f"News ({len(news_decisions)}), Reports ({len(stats)})")
    log.info("=" * 60)
    
    return {
        "decisions_found": len(all_decisions),
        "db_stats": db_stats,
        "analysis": analysis,
    }


if __name__ == "__main__":
    main()
