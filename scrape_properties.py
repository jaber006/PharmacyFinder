"""
Scrape commercial property listings from realcommercial.com.au
using Playwright for browser automation.

Reads the search plan, scrapes each suburb, saves results.
Respects rate limits with delays between requests.
"""

import csv
import json
import math
import os
import re
import sqlite3
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from playwright.sync_api import sync_playwright, Page, Browser, BrowserContext

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_URL = "https://www.realcommercial.com.au"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'pharmacy_finder.db')
RESULTS_PATH = os.path.join(OUTPUT_DIR, '_scrape_results.json')
PLAN_PATH = os.path.join(OUTPUT_DIR, '_search_plan_clean.json')

REQUEST_DELAY = 4.0  # seconds between page loads
PAGE_TIMEOUT = 25000  # ms
CDP_URL = "http://127.0.0.1:18800"  # Clawdbot browser CDP endpoint

PHARMACY_MIN_SQM = 80
PHARMACY_MAX_SQM = 500
PHARMACY_IDEAL_MIN = 100
PHARMACY_IDEAL_MAX = 400

STATE_PRIORITY = ['TAS', 'VIC', 'NSW', 'QLD', 'SA', 'WA', 'NT', 'ACT']


def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1))*math.cos(math.radians(lat2))*math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))


# ---------------------------------------------------------------------------
# Scraping
# ---------------------------------------------------------------------------

def scrape_suburb(page: Page, suburb: str, state: str) -> List[Dict]:
    """Scrape all lease listings for a suburb from realcommercial.com.au."""
    slug = suburb.lower().replace(' ', '-')
    state_lower = state.lower()
    url = f"{BASE_URL}/for-lease/{slug}-{state_lower}/"
    
    all_listings = []
    page_num = 1
    max_pages = 3  # Don't go beyond 3 pages per suburb
    
    while page_num <= max_pages:
        page_url = url if page_num == 1 else f"{url}?page={page_num}"
        
        try:
            page.goto(page_url, timeout=PAGE_TIMEOUT, wait_until='domcontentloaded')
            # Wait for content to actually render
            try:
                page.wait_for_selector('h2', timeout=8000)
            except:
                page.wait_for_timeout(5000)
        except Exception as e:
            print(f"      [ERR] Failed to load {page_url}: {str(e)[:60]}")
            break
        
        # Extract listings from the page
        listings = page.evaluate("""() => {
            const results = [];
            
            // Find all listing items in the search results
            const listItems = document.querySelectorAll('article, [data-testid*="listing"], li');
            
            for (const item of listItems) {
                // Look for address in h2 links
                const h2 = item.querySelector('h2 a, h2');
                if (!h2) continue;
                
                const address = h2.textContent.trim();
                // Must look like an address with state and postcode
                if (!/\\b(TAS|VIC|NSW|QLD|SA|WA|NT|ACT)\\s+\\d{4}\\b/.test(address)) continue;
                
                // URL
                const link = h2.querySelector('a') || h2.closest('a');
                const url = link ? link.href : '';
                
                // Price - usually in h3
                const h3 = item.querySelector('h3');
                let rent_display = h3 ? h3.textContent.trim() : '';
                
                // Size and type from text content
                const textContent = item.textContent;
                const sizeMatch = textContent.match(/(\\d[\\d,]*)\\s*m²/);
                const size = sizeMatch ? parseInt(sizeMatch[1].replace(',', '')) : 0;
                
                // Property type
                const typeKeywords = ['Shops & Retail', 'Medical & Consulting', 'Offices', 
                    'Warehouse', 'Industrial', 'Showrooms', 'Hotel', 'Development', 'Farming', 'Other'];
                const types = typeKeywords.filter(t => textContent.includes(t));
                
                results.push({
                    address: address,
                    url: url,
                    rent_display: rent_display,
                    floor_area_sqm: size,
                    property_type: types.join(' | '),
                });
            }
            
            return results;
        }""")
        
        if not listings:
            # Fallback: try h2-based extraction
            listings = extract_from_snapshot(page)
        
        if not listings:
            # Second fallback: try link-based extraction 
            listings = page.evaluate("""() => {
                const results = [];
                const links = document.querySelectorAll('a[href*="/for-lease/property-"]');
                const seen = new Set();
                for (const a of links) {
                    const text = a.textContent.trim();
                    if (seen.has(a.href)) continue;
                    if (!/\\b(TAS|VIC|NSW|QLD|SA|WA|NT|ACT)\\s+\\d{4}\\b/.test(text)) continue;
                    seen.add(a.href);
                    
                    const parent = a.closest('li') || a.closest('div') || a.parentElement;
                    const parentText = parent ? parent.textContent : '';
                    const sizeMatch = parentText.match(/(\\d[\\d,]*)\\s*m/);
                    const size = sizeMatch ? parseInt(sizeMatch[1].replace(',', '')) : 0;
                    
                    const typeKeywords = ['Shops & Retail', 'Medical & Consulting', 'Offices',
                        'Warehouse', 'Industrial', 'Showrooms', 'Hotel', 'Development'];
                    const types = typeKeywords.filter(t => parentText.includes(t));
                    
                    // Get price from nearby h3
                    const h3 = parent ? parent.querySelector('h3') : null;
                    const rent = h3 ? h3.textContent.trim() : '';
                    
                    results.push({
                        address: text,
                        url: a.href,
                        rent_display: rent,
                        floor_area_sqm: size,
                        property_type: types.join(' | '),
                    });
                }
                return results;
            }""")
        
        # Deduplicate by URL
        seen_urls = set(l.get('url', '') for l in all_listings)
        for l in listings:
            if l.get('url') and l['url'] not in seen_urls:
                all_listings.append(l)
                seen_urls.add(l['url'])
            elif not l.get('url') and l.get('address'):
                all_listings.append(l)
        
        # Check if there's a next page
        has_next = page.evaluate("""() => {
            const links = document.querySelectorAll('a');
            for (const a of links) {
                if (a.textContent.trim().toLowerCase() === 'next') return true;
            }
            return false;
        }""")
        
        if not has_next or not listings:
            break
        
        page_num += 1
        time.sleep(REQUEST_DELAY)
    
    # Extract rent amounts
    for l in all_listings:
        l['rent_pa'] = 0
        l['rent_sqm'] = 0
        l['agent'] = ''
        
        rd = l.get('rent_display', '')
        m = re.search(r'\$([\d,]+)', rd)
        if m:
            try:
                l['rent_pa'] = float(m.group(1).replace(',', ''))
            except ValueError:
                pass
        
        if l['rent_pa'] > 0 and l.get('floor_area_sqm', 0) > 0:
            l['rent_sqm'] = round(l['rent_pa'] / l['floor_area_sqm'], 2)
    
    return all_listings


def extract_from_snapshot(page: Page) -> List[Dict]:
    """Fallback: extract listings by finding h2 headings with addresses."""
    return page.evaluate("""() => {
        const results = [];
        const headings = document.querySelectorAll('h2');
        
        for (const h2 of headings) {
            const text = h2.textContent.trim();
            if (!/\\b(TAS|VIC|NSW|QLD|SA|WA|NT|ACT)\\s+\\d{4}\\b/.test(text)) continue;
            
            const link = h2.querySelector('a');
            const url = link ? link.href : '';
            
            // Walk to sibling/parent to find price and size
            const parent = h2.closest('li') || h2.closest('div') || h2.parentElement;
            const parentText = parent ? parent.textContent : '';
            
            const h3 = parent ? parent.querySelector('h3') : null;
            const rent = h3 ? h3.textContent.trim() : '';
            
            const sizeMatch = parentText.match(/(\\d[\\d,]*)\\s*m²/);
            const size = sizeMatch ? parseInt(sizeMatch[1].replace(',', '')) : 0;
            
            const typeKeywords = ['Shops & Retail', 'Medical & Consulting', 'Offices', 
                'Warehouse', 'Industrial', 'Showrooms', 'Hotel', 'Development'];
            const types = typeKeywords.filter(t => parentText.includes(t));
            
            results.push({
                address: text,
                url: url,
                rent_display: rent,
                floor_area_sqm: size,
                property_type: types.join(' | '),
            });
        }
        
        return results;
    }""")


# ---------------------------------------------------------------------------
# Suitability scoring
# ---------------------------------------------------------------------------

def score_listing(listing: Dict) -> Tuple[bool, str, int]:
    """Returns (suitable, note, score 0-100)."""
    pt = (listing.get('property_type') or '').lower()
    area = listing.get('floor_area_sqm', 0)
    addr = (listing.get('address') or '').lower()
    
    score = 50
    
    good_type = any(t in pt for t in ['retail', 'shop', 'medical', 'consulting'])
    bad_type = any(t in pt for t in ['warehouse', 'industrial', 'hotel', 'leisure', 
                                       'development', 'farming', 'land', 'rural']) and not good_type
    
    if good_type:
        score += 20
        if 'medical' in pt: score += 5
    if bad_type:
        return False, f"Unsuitable type: {listing.get('property_type','')}", 0
    
    if area > 0:
        if area < PHARMACY_MIN_SQM:
            return False, f"Too small: {area}m²", 10
        if area > PHARMACY_MAX_SQM:
            return False, f"Too large: {area}m²", 10
        if PHARMACY_IDEAL_MIN <= area <= PHARMACY_IDEAL_MAX:
            score += 20
        else:
            score += 10
    
    # Upper floors
    if re.search(r'level\s*[2-9]|floor\s*[2-9]', addr):
        return False, "Upper floor", 15
    if 'shop' in addr or 'tenancy' in addr:
        score += 5
    if 'level' not in addr:
        score += 5
    
    notes = []
    if good_type: notes.append('Retail/Medical')
    if PHARMACY_IDEAL_MIN <= area <= PHARMACY_IDEAL_MAX:
        notes.append(f'{area}m² ideal')
    elif area > 0:
        notes.append(f'{area}m²')
    
    return True, ', '.join(notes) if notes else 'Needs review', min(100, max(0, score))


# ---------------------------------------------------------------------------
# Output generation
# ---------------------------------------------------------------------------

def generate_outputs(scrape_results: Dict, plan: Dict):
    """Generate CSV, JSON summary, and database entries."""
    opps = plan['opportunities']
    
    all_rows = []
    suitable_rows = []
    
    for i, opp in enumerate(opps):
        suburb = opp.get('suburb', '')
        state = opp.get('state', '')
        key = suburb + '|' + state
        
        data = scrape_results.get(key, {})
        listings = data.get('listings', [])
        
        for listing in listings:
            suitable, note, score = score_listing(listing)
            
            row = {
                'opportunity_id': f"{state}_{i:03d}",
                'state': state,
                'opportunity_lat': opp['lat'],
                'opportunity_lng': opp['lon'],
                'rule': opp.get('rules', ''),
                'confidence': opp.get('conf_pct', ''),
                'poi_name': opp.get('poi', ''),
                'poi_type': opp.get('poi_type', ''),
                'nearest_pharmacy_km': opp.get('nearest_pharm_km', ''),
                'composite_score': opp.get('composite', 0),
                'pop_10km': opp.get('pop10', 0),
                'property_address': listing.get('address', ''),
                'property_suburb': suburb,
                'rent_display': listing.get('rent_display', ''),
                'rent_pa': listing.get('rent_pa', 0),
                'rent_sqm': listing.get('rent_sqm', 0),
                'floor_area_sqm': listing.get('floor_area_sqm', 0),
                'property_type': listing.get('property_type', ''),
                'listing_url': listing.get('url', ''),
                'agent': listing.get('agent', ''),
                'suitable': suitable,
                'suitability_score': score,
                'suitability_note': note,
            }
            
            all_rows.append(row)
            if suitable:
                suitable_rows.append(row)
    
    # Write CSVs
    fields = [
        'opportunity_id', 'state', 'opportunity_lat', 'opportunity_lng',
        'rule', 'confidence', 'poi_name', 'nearest_pharmacy_km', 'composite_score',
        'property_address', 'rent_display', 'rent_pa', 'rent_sqm',
        'floor_area_sqm', 'property_type', 'listing_url', 'agent',
        'suitability_score', 'suitability_note',
    ]
    
    def write_csv(rows, path):
        sorted_rows = sorted(rows, key=lambda x: (-x.get('suitability_score',0), -x.get('composite_score',0)))
        with open(path, 'w', newline='', encoding='utf-8') as f:
            w = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
            w.writeheader()
            w.writerows(sorted_rows)
        print(f"  Wrote {len(sorted_rows)} rows -> {path}")
    
    write_csv(suitable_rows, os.path.join(OUTPUT_DIR, 'property_listings.csv'))
    write_csv(all_rows, os.path.join(OUTPUT_DIR, 'property_listings_all.csv'))
    
    # Summary JSON
    summary = build_summary(all_rows, suitable_rows, opps, scrape_results)
    summary_path = os.path.join(OUTPUT_DIR, 'properties_summary.json')
    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2)
    print(f"  Summary -> {summary_path}")
    
    # Database
    update_db(suitable_rows)
    
    return suitable_rows


def build_summary(all_rows, suitable_rows, opps, scrape_results):
    by_state = {}
    for state in STATE_PRIORITY:
        s_suit = [r for r in suitable_rows if r['state'] == state]
        s_all = [r for r in all_rows if r['state'] == state]
        
        if not s_all:
            continue
        
        rents = [r['rent_pa'] for r in s_suit if r.get('rent_pa', 0) > 0]
        sizes = [r['floor_area_sqm'] for r in s_suit if r.get('floor_area_sqm', 0) > 0]
        
        # Unique properties
        unique_all = len(set(r['listing_url'] for r in s_all if r.get('listing_url')))
        unique_suit = len(set(r['listing_url'] for r in s_suit if r.get('listing_url')))
        
        by_state[state] = {
            'opportunities_with_listings': len(set(r['opportunity_id'] for r in s_all)),
            'unique_listings': unique_all,
            'suitable_listings': unique_suit,
            'rent_range': f"${min(rents):,.0f} - ${max(rents):,.0f}" if rents else 'N/A',
            'rent_median': f"${sorted(rents)[len(rents)//2]:,.0f}" if rents else 'N/A',
            'size_range': f"{min(sizes)}-{max(sizes)} m²" if sizes else 'N/A',
            'top_properties': [
                {
                    'address': r['property_address'],
                    'rent': r['rent_display'],
                    'size': r['floor_area_sqm'],
                    'type': r['property_type'],
                    'score': r['suitability_score'],
                    'url': r['listing_url'],
                    'opportunity': r['poi_name'],
                }
                for r in sorted(s_suit, key=lambda x: -x['suitability_score'])[:10]
            ],
        }
    
    # Suburbs scraped
    suburbs_scraped = len(scrape_results)
    total_raw = sum(d.get('listing_count', 0) for d in scrape_results.values())
    
    return {
        'generated': datetime.now().isoformat(),
        'suburbs_scraped': suburbs_scraped,
        'total_raw_listings': total_raw,
        'total_matched': len(all_rows),
        'suitable_for_pharmacy': len(suitable_rows),
        'unique_suitable': len(set(r['listing_url'] for r in suitable_rows if r['listing_url'])),
        'by_state': by_state,
    }


def update_db(results):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS commercial_properties (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            opportunity_id TEXT,
            state TEXT,
            opportunity_lat REAL,
            opportunity_lng REAL,
            opportunity_rule TEXT,
            opportunity_confidence TEXT,
            poi_name TEXT,
            property_address TEXT NOT NULL,
            property_suburb TEXT,
            rent_pa REAL,
            rent_sqm REAL,
            rent_display TEXT,
            floor_area_sqm REAL,
            property_type TEXT,
            listing_url TEXT UNIQUE,
            agent TEXT,
            suitability_score INTEGER,
            suitability_note TEXT,
            date_scraped TEXT
        )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_cp_state ON commercial_properties(state)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_cp_score ON commercial_properties(suitability_score)")
    
    n = 0
    for r in results:
        try:
            c.execute("""INSERT OR REPLACE INTO commercial_properties
                (opportunity_id, state, opportunity_lat, opportunity_lng,
                 opportunity_rule, opportunity_confidence, poi_name,
                 property_address, property_suburb,
                 rent_pa, rent_sqm, rent_display, floor_area_sqm,
                 property_type, listing_url, agent,
                 suitability_score, suitability_note, date_scraped)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (r['opportunity_id'], r['state'], r['opportunity_lat'], r['opportunity_lng'],
                 r['rule'], r['confidence'], r['poi_name'],
                 r['property_address'], r.get('property_suburb',''),
                 r['rent_pa'], r['rent_sqm'], r['rent_display'], r['floor_area_sqm'],
                 r['property_type'], r['listing_url'], r['agent'],
                 r['suitability_score'], r['suitability_note'],
                 datetime.now().isoformat()))
            n += 1
        except: pass
    
    conn.commit()
    conn.close()
    print(f"  Database: {n} properties inserted/updated")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    sys.stdout.reconfigure(line_buffering=True)
    
    import argparse
    parser = argparse.ArgumentParser(description='Scrape commercial properties')
    parser.add_argument('--state', help='Single state to scrape')
    parser.add_argument('--resume', action='store_true', help='Resume from previous results')
    parser.add_argument('--output-only', action='store_true', help='Skip scraping, just generate outputs')
    args = parser.parse_args()
    
    # Load plan
    with open(PLAN_PATH) as f:
        plan = json.load(f)
    
    searches = plan['searches']
    
    if args.state:
        searches = [s for s in searches if s['state'] == args.state.upper()]
    
    # Load existing results
    existing = {}
    if args.resume and os.path.exists(RESULTS_PATH):
        with open(RESULTS_PATH) as f:
            existing = json.load(f)
    
    if args.output_only:
        if not existing:
            with open(RESULTS_PATH) as f:
                existing = json.load(f)
        generate_outputs(existing, plan)
        return
    
    print(f"\n{'='*70}")
    print(f"  COMMERCIAL PROPERTY SCRAPER — realcommercial.com.au")
    print(f"{'='*70}")
    print(f"  Suburbs to scrape: {len(searches)}")
    print(f"  Already scraped:   {len(existing)}")
    print(f"  Delay: {REQUEST_DELAY}s between pages")
    print()
    
    # Filter to remaining
    remaining = [s for s in searches if (s['suburb'] + '|' + s['state']) not in existing]
    print(f"  Remaining: {len(remaining)} suburbs")
    
    if not remaining:
        print("  All suburbs already scraped! Generating outputs...")
        generate_outputs(existing, plan)
        return
    
    # Connect to Clawdbot browser via CDP (has real profile & cookies)
    # Start Clawdbot browser first: clawdbot browser start --profile clawd
    with sync_playwright() as pw:
        print(f"\n  Connecting to Clawdbot browser via CDP ({CDP_URL})...")
        try:
            browser = pw.chromium.connect_over_cdp(CDP_URL)
        except Exception as e:
            print(f"  FAILED to connect to Clawdbot browser: {e}")
            print(f"  Make sure Clawdbot browser is running: clawdbot browser start --profile clawd")
            return
        
        context = browser.contexts[0] if browser.contexts else browser.new_context()
        page = context.new_page()
        
        for i, search in enumerate(remaining, 1):
            suburb = search['suburb']
            state = search['state']
            key = suburb + '|' + state
            
            print(f"\n  [{i}/{len(remaining)}] {suburb}, {state}...", end=' ', flush=True)
            
            try:
                listings = scrape_suburb(page, suburb, state)
                
                existing[key] = {
                    'suburb': suburb,
                    'state': state,
                    'listing_count': len(listings),
                    'listings': listings,
                    'scraped_at': datetime.now().isoformat(),
                }
                
                print(f"OK - {len(listings)} listings")
                
                # Save progress after each suburb
                with open(RESULTS_PATH, 'w') as f:
                    json.dump(existing, f, indent=2)
                
            except Exception as e:
                print(f"FAIL - Error: {str(e)[:60]}")
                existing[key] = {
                    'suburb': suburb,
                    'state': state,
                    'listing_count': 0,
                    'listings': [],
                    'error': str(e)[:200],
                    'scraped_at': datetime.now().isoformat(),
                }
                # Save progress even on error
                with open(RESULTS_PATH, 'w') as f:
                    json.dump(existing, f, indent=2)
            
            # Rate limit
            time.sleep(REQUEST_DELAY)
        
        # Close our page but don't close the browser (it's Clawdbot's)
        try:
            page.close()
        except:
            pass
        browser.close()  # Disconnects CDP, doesn't kill browser
    
    # Generate outputs
    print(f"\n{'='*70}")
    print(f"  GENERATING OUTPUTS")
    print(f"{'='*70}")
    generate_outputs(existing, plan)
    
    # Print summary
    print(f"\n{'='*70}")
    print(f"  SCRAPING COMPLETE")
    print(f"{'='*70}")
    total = sum(d.get('listing_count', 0) for d in existing.values())
    print(f"  Suburbs scraped: {len(existing)}")
    print(f"  Total listings:  {total}")


if __name__ == '__main__':
    main()
