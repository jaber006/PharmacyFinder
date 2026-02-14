"""
Parse property listing snapshots saved as text files.
Browser automation is driven externally; this script processes the results.

Usage:
    1. Browser automation saves snapshots to output/_snapshots/
    2. This script processes them into property_listings.csv
"""
import csv
import json
import math
import os
import re
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional, Tuple

BASE_URL = "https://www.realcommercial.com.au"
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'pharmacy_finder.db')
SNAPSHOT_DIR = os.path.join(OUTPUT_DIR, '_snapshots')

PHARMACY_MIN_SQM = 80
PHARMACY_MAX_SQM = 500
PHARMACY_IDEAL_MIN_SQM = 100
PHARMACY_IDEAL_MAX_SQM = 400

STATE_PRIORITY = ['TAS', 'VIC', 'NSW', 'QLD', 'SA', 'WA', 'NT', 'ACT']


def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1))*math.cos(math.radians(lat2))*math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))


def parse_snapshot(text: str) -> List[Dict]:
    """Parse realcommercial.com.au browser snapshot into listings."""
    listings = []
    current = None
    
    for line in text.split('\n'):
        stripped = line.strip()
        if not stripped:
            continue
        
        # New listing: heading with address pattern "X, Suburb, STATE Postcode"
        addr_match = re.search(
            r'heading\s+"([^"]*,\s*(?:TAS|VIC|NSW|QLD|SA|WA|NT|ACT)\s+\d{4})"',
            stripped
        )
        if addr_match:
            if current and current.get('address'):
                listings.append(current)
            current = {
                'address': addr_match.group(1),
                'url': '',
                'rent_display': '',
                'rent_pa': 0,
                'rent_sqm': 0,
                'floor_area_sqm': 0,
                'property_type': '',
                'agent': '',
            }
            continue
        
        if current is None:
            continue
        
        # URL
        url_match = re.search(r'/url:\s+(/for-lease/property-[^\s]+)', stripped)
        if url_match and not current['url']:
            current['url'] = BASE_URL + url_match.group(1)
        
        # Price heading
        price_match = re.search(r'heading\s+"(\$[\d,.]+[^"]*)"', stripped)
        if price_match and not current['rent_display']:
            current['rent_display'] = price_match.group(1)
            num = re.search(r'\$([\d,]+)', current['rent_display'])
            if num:
                try:
                    current['rent_pa'] = float(num.group(1).replace(',', ''))
                except ValueError:
                    pass
        
        if 'Contact Agent' in stripped and not current['rent_display']:
            current['rent_display'] = 'Contact Agent'
        if 'Price on Application' in stripped and not current['rent_display']:
            current['rent_display'] = 'Price on Application'
        
        # Agent from "Contact X" heading
        agent_match = re.search(r'heading\s+"Contact\s+([^"]+)"', stripped)
        if agent_match and not current['agent']:
            a = agent_match.group(1).strip()
            if a.lower() != 'agent':
                current['agent'] = a
        
        # Size
        size_match = re.search(r'(\d[\d,]*)\s*m²', stripped)
        if size_match and not current['floor_area_sqm']:
            current['floor_area_sqm'] = int(size_match.group(1).replace(',', ''))
        
        # Property type
        types = {
            'Shops & Retail': 'Shops & Retail',
            'Medical & Consulting': 'Medical & Consulting',
            'Offices': 'Offices',
            'Warehouse': 'Warehouse/Industrial',
            'Industrial': 'Warehouse/Industrial',
            'Showroom': 'Showrooms',
            'Hotel': 'Hotel/Leisure',
            'Development': 'Development/Land',
        }
        for kw, label in types.items():
            if kw in stripped and label not in (current.get('property_type') or ''):
                if current['property_type']:
                    current['property_type'] += ' | '
                current['property_type'] += label
    
    if current and current.get('address'):
        listings.append(current)
    
    # Calc rent/sqm
    for l in listings:
        if l['rent_pa'] > 0 and l['floor_area_sqm'] > 0:
            l['rent_sqm'] = round(l['rent_pa'] / l['floor_area_sqm'], 2)
    
    return listings


def is_pharmacy_suitable(listing: Dict) -> Tuple[bool, str, int]:
    """Returns (suitable, note, score 0-100)."""
    pt = (listing.get('property_type') or '').lower()
    area = listing.get('floor_area_sqm', 0)
    addr = (listing.get('address') or '').lower()
    
    score = 50
    
    # Type
    good_type = any(t in pt for t in ['retail', 'shop', 'medical', 'consulting'])
    bad_type = any(t in pt for t in ['warehouse', 'industrial', 'hotel', 'leisure', 'development', 'farming', 'land']) and not good_type
    
    if good_type:
        score += 20
        if 'medical' in pt:
            score += 5
    if bad_type:
        return False, f"Bad type: {listing.get('property_type','')}", 0
    
    # Size
    if area > 0:
        if area < PHARMACY_MIN_SQM:
            return False, f"Too small: {area}m²", 10
        if area > PHARMACY_MAX_SQM:
            return False, f"Too large: {area}m²", 10
        if PHARMACY_IDEAL_MIN_SQM <= area <= PHARMACY_IDEAL_MAX_SQM:
            score += 20
        else:
            score += 10
    
    # Floor level
    if re.search(r'level\s*[2-9]|floor\s*[2-9]|suite\s+[2-9]', addr):
        return False, "Upper floor", 15
    if 'shop' in addr or 'tenancy' in addr:
        score += 5
    if 'level' not in addr:
        score += 5
    
    note = []
    if good_type:
        note.append('Retail/Medical')
    if PHARMACY_IDEAL_MIN_SQM <= area <= PHARMACY_IDEAL_MAX_SQM:
        note.append(f'{area}m² ideal')
    elif area > 0:
        note.append(f'{area}m²')
    
    return True, ', '.join(note) if note else 'Needs review', min(100, max(0, score))


def process_all_snapshots():
    """Process all saved snapshots and generate output files."""
    os.makedirs(SNAPSHOT_DIR, exist_ok=True)
    
    # Load search plan
    plan_path = os.path.join(OUTPUT_DIR, '_search_plan_clean.json')
    if not os.path.exists(plan_path):
        print("No search plan found. Run build_search_plan.py first.")
        return
    
    with open(plan_path) as f:
        plan = json.load(f)
    
    opps = plan['opportunities']
    
    # Load all snapshot results
    results_path = os.path.join(OUTPUT_DIR, '_scrape_results.json')
    if not os.path.exists(results_path):
        print("No scrape results found. Run browser scraping first.")
        return
    
    with open(results_path) as f:
        scrape_results = json.load(f)
    
    # Build suburb -> listings map
    suburb_listings = {}
    for key, data in scrape_results.items():
        suburb_listings[key] = data.get('listings', [])
    
    print(f"Loaded {len(suburb_listings)} suburb results")
    print(f"Total raw listings: {sum(len(v) for v in suburb_listings.values())}")
    
    # Match to opportunities
    all_results = []
    suitable_results = []
    
    for i, opp in enumerate(opps):
        suburb = opp.get('suburb', '')
        state = opp.get('state', '')
        key = suburb + '|' + state
        
        listings = suburb_listings.get(key, [])
        
        for listing in listings:
            suitable, note, score = is_pharmacy_suitable(listing)
            
            result = {
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
                
                'property_address': listing['address'],
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
            
            all_results.append(result)
            if suitable:
                suitable_results.append(result)
    
    # Write CSV
    write_csv(suitable_results, os.path.join(OUTPUT_DIR, 'property_listings.csv'))
    write_csv(all_results, os.path.join(OUTPUT_DIR, 'property_listings_all.csv'))
    write_summary(all_results, suitable_results, opps)
    update_database(suitable_results)
    
    print(f"\nTotal listings: {len(all_results)}")
    print(f"Suitable for pharmacy: {len(suitable_results)}")
    print(f"Unique suitable properties: {len(set(r['listing_url'] for r in suitable_results if r['listing_url']))}")


def write_csv(results, path):
    if not results:
        return
    fields = [
        'opportunity_id', 'state', 'opportunity_lat', 'opportunity_lng',
        'rule', 'confidence', 'poi_name', 'nearest_pharmacy_km', 'composite_score',
        'property_address', 'rent_display', 'rent_pa', 'rent_sqm',
        'floor_area_sqm', 'property_type', 'listing_url', 'agent',
        'suitability_score', 'suitability_note',
    ]
    sorted_results = sorted(results, key=lambda x: (-x.get('suitability_score',0), -x.get('composite_score',0)))
    with open(path, 'w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
        w.writeheader()
        w.writerows(sorted_results)
    print(f"Wrote {len(sorted_results)} rows to {path}")


def write_summary(all_results, suitable, opps):
    path = os.path.join(OUTPUT_DIR, 'properties_summary.json')
    by_state = {}
    for state in STATE_PRIORITY:
        s_all = [r for r in all_results if r['state'] == state]
        s_suit = [r for r in suitable if r['state'] == state]
        if not s_all and not s_suit:
            continue
        
        rents = [r['rent_pa'] for r in s_suit if r.get('rent_pa', 0) > 0]
        sizes = [r['floor_area_sqm'] for r in s_suit if r.get('floor_area_sqm', 0) > 0]
        
        by_state[state] = {
            'total_listings': len(set(r['listing_url'] for r in s_all if r['listing_url'])),
            'suitable_listings': len(set(r['listing_url'] for r in s_suit if r['listing_url'])),
            'rent_min': min(rents) if rents else None,
            'rent_max': max(rents) if rents else None,
            'rent_median': sorted(rents)[len(rents)//2] if rents else None,
            'size_min': min(sizes) if sizes else None,
            'size_max': max(sizes) if sizes else None,
            'top_listings': [
                {
                    'address': r['property_address'],
                    'rent': r['rent_display'],
                    'size': r['floor_area_sqm'],
                    'type': r['property_type'],
                    'score': r['suitability_score'],
                    'url': r['listing_url'],
                    'poi': r['poi_name'],
                }
                for r in sorted(s_suit, key=lambda x: -x['suitability_score'])[:10]
            ],
        }
    
    summary = {
        'generated': datetime.now().isoformat(),
        'total_listings': len(all_results),
        'suitable_listings': len(suitable),
        'unique_suitable': len(set(r['listing_url'] for r in suitable if r['listing_url'])),
        'by_state': by_state,
    }
    
    with open(path, 'w') as f:
        json.dump(summary, f, indent=2)
    print(f"Summary: {path}")


def update_database(results):
    """Add suitable properties to the database."""
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
            listing_url TEXT,
            agent TEXT,
            suitability_score INTEGER,
            suitability_note TEXT,
            date_scraped TEXT,
            UNIQUE(listing_url)
        )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_cp_state ON commercial_properties(state)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_cp_score ON commercial_properties(suitability_score)")
    
    inserted = 0
    for r in results:
        try:
            c.execute("""
                INSERT OR REPLACE INTO commercial_properties
                (opportunity_id, state, opportunity_lat, opportunity_lng,
                 opportunity_rule, opportunity_confidence, poi_name,
                 property_address, property_suburb,
                 rent_pa, rent_sqm, rent_display, floor_area_sqm,
                 property_type, listing_url, agent,
                 suitability_score, suitability_note, date_scraped)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                r['opportunity_id'], r['state'], r['opportunity_lat'], r['opportunity_lng'],
                r['rule'], r['confidence'], r['poi_name'],
                r['property_address'], r.get('property_suburb',''),
                r['rent_pa'], r['rent_sqm'], r['rent_display'], r['floor_area_sqm'],
                r['property_type'], r['listing_url'], r['agent'],
                r['suitability_score'], r['suitability_note'],
                datetime.now().isoformat(),
            ))
            inserted += 1
        except Exception as e:
            pass
    
    conn.commit()
    conn.close()
    print(f"Database: inserted/updated {inserted} properties")


if __name__ == '__main__':
    process_all_snapshots()
