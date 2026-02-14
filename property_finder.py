"""
Commercial Property Finder for Pharmacy Opportunity Zones
=========================================================

Searches realcommercial.com.au for retail/commercial lease listings
near the top-ranked pharmacy opportunity zones. Uses browser automation
since the sites block direct HTTP requests.

Produces:
  - output/property_listings.csv       (all matched properties)
  - output/properties_summary.json     (stats per state)
  - Database: commercial_properties table linked to opportunities

Usage:
    python property_finder.py                  # All states, top 100
    python property_finder.py --state TAS      # Single state
    python property_finder.py --top 50         # Top 50 only
    python property_finder.py --no-browser     # Use urllib fallback
"""

import argparse
import csv
import json
import math
import os
import re
import sqlite3
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

PROPERTY_SEARCH_RADIUS_KM = 5.0   # match properties within this radius
REQUEST_DELAY_S = 4.0              # seconds between requests (rate limit)
MAX_LISTINGS_PER_SUBURB = 20       # cap per search
PHARMACY_MIN_SQM = 80             # minimum viable pharmacy size
PHARMACY_MAX_SQM = 500            # maximum realistic pharmacy size
PHARMACY_IDEAL_MIN_SQM = 100      # ideal minimum
PHARMACY_IDEAL_MAX_SQM = 400      # ideal maximum

BASE_URL = "https://www.realcommercial.com.au"

STATE_PRIORITY = ['TAS', 'VIC', 'NSW', 'QLD', 'SA', 'WA', 'NT', 'ACT']

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'pharmacy_finder.db')

# ---------------------------------------------------------------------------
# Haversine distance (inline to avoid import issues)
# ---------------------------------------------------------------------------

def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Haversine distance in km."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(a))


# ---------------------------------------------------------------------------
# Geocoding helpers
# ---------------------------------------------------------------------------

_SUBURB_COORDS_CACHE: Dict[str, Optional[Tuple[float, float]]] = {}


def geocode_suburb_nominatim(suburb: str, state: str) -> Optional[Tuple[float, float]]:
    """Geocode a suburb using Nominatim (free, rate-limited)."""
    key = f"{suburb},{state}"
    if key in _SUBURB_COORDS_CACHE:
        return _SUBURB_COORDS_CACHE[key]

    query = f"{suburb}, {state}, Australia"
    url = f"https://nominatim.openstreetmap.org/search?q={urllib.parse.quote(query)}&format=json&limit=1&countrycodes=au"
    
    try:
        req = urllib.request.Request(url, headers={
            'User-Agent': 'PharmacyFinder/1.0 (research project)',
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode('utf-8'))
        
        if data:
            coords = (float(data[0]['lat']), float(data[0]['lon']))
            _SUBURB_COORDS_CACHE[key] = coords
            return coords
    except Exception as e:
        print(f"    [WARN] Geocode failed for {query}: {e}")
    
    _SUBURB_COORDS_CACHE[key] = None
    return None


# ---------------------------------------------------------------------------
# Load and rank opportunities
# ---------------------------------------------------------------------------

def load_all_opportunities(states: List[str] = None) -> List[Dict]:
    """Load opportunities from all state CSVs, compute ranking score."""
    if states is None:
        states = STATE_PRIORITY

    all_opps = []
    
    for state in states:
        # Prefer population-ranked, then opportunity_zones
        csv_path = None
        for prefix in ['population_ranked', 'opportunity_zones']:
            path = os.path.join(OUTPUT_DIR, f"{prefix}_{state}.csv")
            if os.path.exists(path):
                csv_path = path
                break
        
        if csv_path is None:
            print(f"  [SKIP] No CSV found for {state}")
            continue

        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Skip false positives
                if row.get('Verification', '').upper() == 'FALSE POSITIVE':
                    continue
                
                try:
                    lat = float(row['Latitude'])
                    lon = float(row['Longitude'])
                except (ValueError, TypeError, KeyError):
                    continue
                
                # Parse confidence (e.g., "95%" -> 0.95)
                conf_str = row.get('Confidence', '0%').replace('%', '').strip()
                try:
                    confidence = float(conf_str) / 100.0
                except ValueError:
                    confidence = 0.5
                
                # Parse composite/opportunity score
                composite = 0
                for score_field in ['Composite Score', 'Opportunity Score']:
                    try:
                        composite = float(row.get(score_field, 0) or 0)
                        if composite > 0:
                            break
                    except (ValueError, TypeError):
                        pass
                
                # Parse population
                pop = 0
                for pop_field in ['Pop 10km', 'Pop 5km', 'Pop 15km']:
                    try:
                        pop = int(float(row.get(pop_field, 0) or 0))
                        if pop > 0:
                            break
                    except (ValueError, TypeError):
                        pass

                # Ranking score: composite score is best, else confidence × population
                if composite > 0:
                    rank_score = composite
                else:
                    rank_score = confidence * max(pop, 1)

                all_opps.append({
                    'latitude': lat,
                    'longitude': lon,
                    'address': row.get('Address', ''),
                    'qualifying_rules': row.get('Qualifying Rules', ''),
                    'evidence': row.get('Evidence', ''),
                    'confidence': confidence,
                    'confidence_pct': row.get('Confidence', ''),
                    'nearest_pharmacy_km': float(row.get('Nearest Pharmacy (km)', 0) or 0),
                    'nearest_pharmacy_name': row.get('Nearest Pharmacy Name', ''),
                    'poi_name': row.get('POI Name', ''),
                    'poi_type': row.get('POI Type', ''),
                    'state': state,
                    'pop_5km': int(float(row.get('Pop 5km', 0) or 0)),
                    'pop_10km': int(float(row.get('Pop 10km', 0) or 0)),
                    'nearest_town': row.get('Nearest Town', ''),
                    'nearest_town_pop': int(float(row.get('Nearest Town Pop', 0) or 0)),
                    'rank_score': rank_score,
                    'composite_score': composite,
                })
    
    # Sort by rank score descending
    all_opps.sort(key=lambda x: x['rank_score'], reverse=True)
    return all_opps


def extract_suburb(opp: Dict) -> str:
    """Extract the most likely suburb name from an opportunity."""
    address = opp.get('address', '')
    
    if address:
        parts = [p.strip() for p in address.split(',')]
        for part in parts:
            part_clean = part.strip()
            # Skip non-suburb parts
            if not part_clean or len(part_clean) < 3:
                continue
            if re.match(r'^\d', part_clean):  # starts with number (street address)
                continue
            if re.match(r'^\d{4}$', part_clean):  # postcode
                continue
            if part_clean in ('NSW', 'VIC', 'QLD', 'WA', 'SA', 'TAS', 'NT', 'ACT', 'Australia'):
                continue
            if re.match(r'^(City of|Municipality of|Shire of|Council of)', part_clean):
                continue
            skip_words = ['road', 'street', 'avenue', 'drive', 'place', 'lane', 'highway',
                         'lookout', 'trail', 'apartments', 'boulevard', 'way', 'crescent',
                         'terrace', 'parade', 'court']
            if any(w in part_clean.lower() for w in skip_words):
                continue
            if part_clean.lower().startswith(('greater ', 'city of ')):
                continue
            # Check for state names embedded
            if part_clean in ('Tasmania', 'Victoria', 'New South Wales', 'Queensland',
                            'South Australia', 'Western Australia', 'Northern Territory',
                            'Australian Capital Territory'):
                continue
            return part_clean
    
    # Fallback: nearest town
    town = opp.get('nearest_town', '')
    if town and town != 'Unknown' and town != '':
        return town
    
    # Last resort: extract from POI name
    poi = opp.get('poi_name', '')
    if poi:
        # Remove chain names / generic words
        cleaned = re.sub(
            r'(IGA|Woolworths|Coles|Aldi|Shopping Centre|Hospital|Supermarket|'
            r'Medical Centre|Health|Pharmacy|Chemist|Mall|Plaza)', 
            '', poi, flags=re.IGNORECASE
        ).strip()
        cleaned = re.sub(r"['\"]s?\s*$", '', cleaned).strip()  # remove trailing 's
        if cleaned and len(cleaned) > 2:
            return cleaned
    
    return ''


# ---------------------------------------------------------------------------
# Scraping: parse realcommercial.com.au search results (from HTML)
# ---------------------------------------------------------------------------

def parse_listing_from_text(text_block: str) -> Optional[Dict]:
    """Parse a single listing from snapshot text."""
    lines = [l.strip() for l in text_block.strip().split('\n') if l.strip()]
    if not lines:
        return None
    
    result = {
        'address': '',
        'url': '',
        'rent_display': '',
        'floor_area_sqm': 0,
        'property_type': '',
        'agent': '',
    }
    
    for line in lines:
        # Address: look for pattern like "Street, Suburb, STATE Postcode"
        if re.search(r'\b(TAS|VIC|NSW|QLD|SA|WA|NT|ACT)\b\s*\d{4}', line):
            if not result['address']:
                result['address'] = line
        
        # Price patterns
        if re.search(r'\$[\d,]+', line) and not result['rent_display']:
            result['rent_display'] = line
        if 'contact agent' in line.lower() and not result['rent_display']:
            result['rent_display'] = 'Contact Agent'
        if 'price on application' in line.lower():
            result['rent_display'] = 'Price on Application'
        
        # Size: "123 m²" or "123 sqm"
        size_match = re.search(r'(\d[\d,]*)\s*m²', line)
        if size_match and not result['floor_area_sqm']:
            result['floor_area_sqm'] = int(size_match.group(1).replace(',', ''))
        
        # Property type
        type_keywords = ['Shops & Retail', 'Medical & Consulting', 'Offices', 
                        'Warehouse', 'Industrial', 'Showroom', 'Hotel']
        for kw in type_keywords:
            if kw.lower() in line.lower():
                if result['property_type']:
                    result['property_type'] += ' • '
                result['property_type'] += kw
    
    return result if result['address'] else None


def parse_realcommercial_snapshot(snapshot_text: str) -> List[Dict]:
    """Parse listings from browser snapshot of realcommercial search results."""
    listings = []
    
    # The snapshot has structured data - parse listing blocks
    # Each listing has: heading with address+link, price heading, size+type text
    
    # Pattern: find all listing headings with addresses
    # In the snapshot, listings appear in <listitem> blocks
    
    current_listing = None
    
    for line in snapshot_text.split('\n'):
        line = line.strip()
        if not line:
            continue
        
        # Detect address in heading links (pattern: "Address, Suburb, STATE Postcode")
        addr_match = re.search(
            r'heading\s+"([^"]+,\s*(?:TAS|VIC|NSW|QLD|SA|WA|NT|ACT)\s+\d{4})"',
            line
        )
        if not addr_match:
            addr_match = re.search(
                r'text:\s+(.+?,\s*(?:TAS|VIC|NSW|QLD|SA|WA|NT|ACT)\s+\d{4})',
                line
            )
        
        if addr_match:
            # Save previous listing
            if current_listing and current_listing.get('address'):
                listings.append(current_listing)
            
            current_listing = {
                'address': addr_match.group(1).strip('" '),
                'url': '',
                'rent_display': '',
                'rent_pa': 0,
                'rent_sqm': 0,
                'floor_area_sqm': 0,
                'property_type': '',
                'agent': '',
            }
            continue
        
        if current_listing is None:
            continue
        
        # URL
        url_match = re.search(r'/url:\s+(/for-lease/property-[^\s]+)', line)
        if url_match and not current_listing['url']:
            current_listing['url'] = BASE_URL + url_match.group(1)
        
        # Price: "$X per annum" or "$X + Outgoings" or "Contact Agent"
        price_match = re.search(r'heading\s+"(\$[\d,.]+ (?:per annum|p\.a\.|pa|net|\+)[^"]*)"', line, re.IGNORECASE)
        if not price_match:
            price_match = re.search(r'heading\s+"(\$[\d,.]+\s*\+\s*(?:Outgoings|GST)[^"]*)"', line, re.IGNORECASE)
        if not price_match:
            price_match = re.search(r'heading\s+"(\$[\d,.]+[^"]*)"', line, re.IGNORECASE)
        if price_match and not current_listing['rent_display']:
            current_listing['rent_display'] = price_match.group(1).strip('" ')
            # Try to extract numeric rent
            rent_num = re.search(r'\$([\d,]+(?:\.\d+)?)', current_listing['rent_display'])
            if rent_num:
                try:
                    current_listing['rent_pa'] = float(rent_num.group(1).replace(',', ''))
                except ValueError:
                    pass
        
        if 'Contact Agent' in line and not current_listing['rent_display']:
            current_listing['rent_display'] = 'Contact Agent'
        if 'Price on Application' in line and not current_listing['rent_display']:
            current_listing['rent_display'] = 'Price on Application'
        
        # Floor area: "123 m²"
        size_match = re.search(r'(\d[\d,]*)\s*m²', line)
        if size_match and not current_listing['floor_area_sqm']:
            current_listing['floor_area_sqm'] = int(size_match.group(1).replace(',', ''))
        
        # Property type
        if current_listing['floor_area_sqm'] or 'Shops & Retail' in line or 'Medical' in line:
            type_keywords = {
                'Shops & Retail': 'Shops & Retail',
                'Medical & Consulting': 'Medical & Consulting', 
                'Offices': 'Offices',
                'Warehouse': 'Warehouse/Industrial',
                'Industrial': 'Warehouse/Industrial',
                'Showroom': 'Showroom',
                'Hotel': 'Hotel/Leisure',
                'Land': 'Land/Development',
            }
            for kw, label in type_keywords.items():
                if kw in line and label not in (current_listing.get('property_type') or ''):
                    if current_listing['property_type']:
                        current_listing['property_type'] += ' • '
                    current_listing['property_type'] += label
        
        # Agent: "Contact <Name>"
        agent_match = re.search(r'heading\s+"Contact\s+([^"]+)"', line)
        if agent_match and not current_listing['agent']:
            agent_name = agent_match.group(1).strip()
            if agent_name.lower() != 'agent':
                current_listing['agent'] = agent_name
    
    # Don't forget the last listing
    if current_listing and current_listing.get('address'):
        listings.append(current_listing)
    
    # Calculate rent per sqm
    for listing in listings:
        if listing['rent_pa'] > 0 and listing['floor_area_sqm'] > 0:
            listing['rent_sqm'] = round(listing['rent_pa'] / listing['floor_area_sqm'], 2)
    
    return listings


# ---------------------------------------------------------------------------
# Scraping: HTTP fallback (urllib) — used when browser not available
# ---------------------------------------------------------------------------

def search_realcommercial_http(suburb: str, state: str) -> List[Dict]:
    """Search realcommercial.com.au via HTTP (fallback, often rate-limited)."""
    suburb_slug = suburb.lower().replace(' ', '-')
    state_lower = state.lower()
    url = f"{BASE_URL}/for-lease/{suburb_slug}-{state_lower}/?activeSort=date-desc&keywords=retail+pharmacy+shop+medical"
    
    results = []
    for attempt in range(2):
        try:
            req = urllib.request.Request(url, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml',
                'Accept-Language': 'en-AU,en;q=0.9',
                'Referer': 'https://www.realcommercial.com.au/',
            })
            with urllib.request.urlopen(req, timeout=30) as resp:
                html = resp.read().decode('utf-8', errors='replace')
            
            # Parse __NEXT_DATA__ JSON
            nd_match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
            if nd_match:
                data = json.loads(nd_match.group(1))
                props = data.get('props', {}).get('pageProps', {})
                search_results = props.get('searchResults', props.get('results', {}))
                listings_data = search_results.get('listings', []) if isinstance(search_results, dict) else []
                
                for ld in listings_data[:MAX_LISTINGS_PER_SUBURB]:
                    listing = _extract_listing_from_json(ld, suburb, state)
                    if listing:
                        results.append(listing)
            break
            
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < 1:
                time.sleep(10)
                continue
            break
        except Exception:
            break
    
    return results


def _extract_listing_from_json(data: dict, suburb: str, state: str) -> Optional[Dict]:
    """Extract listing from realcommercial JSON structure."""
    try:
        address = data.get('address', {})
        if isinstance(address, dict):
            display = address.get('display', '')
            if not display:
                parts = [address.get(k, '') for k in ['streetAddress', 'suburb', 'state', 'postcode'] if address.get(k)]
                display = ', '.join(parts)
        else:
            display = str(address)
        
        if not display:
            return None
        
        url = data.get('listingUrl', data.get('url', ''))
        if url and not url.startswith('http'):
            url = BASE_URL + url
        
        price = data.get('price', {})
        if isinstance(price, dict):
            rent_display = price.get('display', price.get('text', ''))
        else:
            rent_display = str(price) if price else ''
        
        size = 0
        sizes = data.get('propertySizes', data.get('propertySize', []))
        if isinstance(sizes, list) and sizes:
            try:
                size = int(float(str(sizes[0]).replace(',', '').replace('m²', '').strip()))
            except (ValueError, TypeError):
                pass
        elif isinstance(sizes, (int, float)):
            size = int(sizes)
        
        prop_type = data.get('propertyType', data.get('category', ''))
        
        rent_pa = 0
        rent_num = re.search(r'\$([\d,]+)', rent_display) if rent_display else None
        if rent_num:
            try:
                rent_pa = float(rent_num.group(1).replace(',', ''))
            except ValueError:
                pass
        
        return {
            'address': display,
            'url': url,
            'rent_display': rent_display,
            'rent_pa': rent_pa,
            'rent_sqm': round(rent_pa / size, 2) if rent_pa and size else 0,
            'floor_area_sqm': size,
            'property_type': str(prop_type),
            'agent': '',
        }
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Browser-based scraping (main approach)
# ---------------------------------------------------------------------------

def search_suburb_browser(suburb: str, state: str, browser_fn) -> List[Dict]:
    """
    Search realcommercial.com.au using browser automation.
    browser_fn: callable that takes a URL and returns snapshot text.
    """
    suburb_slug = suburb.lower().replace(' ', '-')
    state_lower = state.lower()
    
    # Search for retail/medical properties for lease
    url = f"{BASE_URL}/for-lease/{suburb_slug}-{state_lower}/?activeSort=date-desc"
    
    snapshot = browser_fn(url)
    if not snapshot:
        return []
    
    listings = parse_realcommercial_snapshot(snapshot)
    
    # If no results, try broader search without suburb-specific URL
    if not listings:
        # Try with postcode area
        url2 = f"{BASE_URL}/for-lease/in-{suburb_slug}-{state_lower}/list-1"
        snapshot2 = browser_fn(url2)
        if snapshot2:
            listings = parse_realcommercial_snapshot(snapshot2)
    
    return listings


# ---------------------------------------------------------------------------
# Property filtering
# ---------------------------------------------------------------------------

def is_pharmacy_suitable(listing: Dict) -> Tuple[bool, str]:
    """
    Check if a listing is suitable for a pharmacy.
    Returns (suitable, reason).
    """
    prop_type = (listing.get('property_type') or '').lower()
    area = listing.get('floor_area_sqm', 0)
    address = (listing.get('address') or '').lower()
    
    # Must be retail, medical, or shop
    suitable_types = ['retail', 'shop', 'medical', 'consulting']
    has_suitable_type = any(t in prop_type for t in suitable_types)
    
    # Exclude pure office/warehouse/industrial
    bad_types = ['warehouse', 'industrial', 'hotel', 'land', 'development']
    has_bad_type = any(t in prop_type for t in bad_types) and not has_suitable_type
    
    if has_bad_type:
        return False, f"Unsuitable type: {listing.get('property_type', '')}"
    
    # Check size
    if area > 0:
        if area < PHARMACY_MIN_SQM:
            return False, f"Too small: {area}m² (min {PHARMACY_MIN_SQM}m²)"
        if area > PHARMACY_MAX_SQM:
            return False, f"Too large: {area}m² (max {PHARMACY_MAX_SQM}m²)"
    
    # Exclude upper floors (not ground floor)
    if re.search(r'level\s*[2-9]|floor\s*[2-9]|first\s*floor|suite\s+[2-9]', address):
        return False, "Upper floor (not ground level)"
    
    # Ideal check
    if has_suitable_type and PHARMACY_IDEAL_MIN_SQM <= area <= PHARMACY_IDEAL_MAX_SQM:
        return True, "Ideal: retail/medical, good size"
    elif has_suitable_type:
        return True, "Suitable type" + (f", {area}m²" if area else ", size unknown")
    elif area and PHARMACY_IDEAL_MIN_SQM <= area <= PHARMACY_IDEAL_MAX_SQM:
        return True, f"Good size ({area}m²), type: {listing.get('property_type', 'unknown')}"
    elif not prop_type:
        # Unknown type — include if size is right
        if area and PHARMACY_MIN_SQM <= area <= PHARMACY_MAX_SQM:
            return True, f"Unknown type, size OK ({area}m²)"
        return True, "Unknown type/size — needs review"
    else:
        return False, f"Not retail/medical: {listing.get('property_type', '')}"


def compute_suitability_score(listing: Dict) -> int:
    """Score a listing 0-100 for pharmacy suitability."""
    score = 50  # base
    
    prop_type = (listing.get('property_type') or '').lower()
    area = listing.get('floor_area_sqm', 0)
    
    # Type scoring
    if 'retail' in prop_type or 'shop' in prop_type:
        score += 20
    if 'medical' in prop_type:
        score += 25
    if 'office' in prop_type and 'retail' not in prop_type:
        score -= 10
    
    # Size scoring
    if PHARMACY_IDEAL_MIN_SQM <= area <= PHARMACY_IDEAL_MAX_SQM:
        score += 20
    elif PHARMACY_MIN_SQM <= area <= PHARMACY_MAX_SQM:
        score += 10
    elif area == 0:
        pass  # unknown, neutral
    else:
        score -= 20
    
    # Ground floor bonus (no "level" in address)
    address = (listing.get('address') or '').lower()
    if 'level' not in address and 'floor' not in address and 'suite' not in address:
        score += 5
    if 'shop' in address or 'tenancy' in address:
        score += 5
    
    return max(0, min(100, score))


# ---------------------------------------------------------------------------
# Database integration
# ---------------------------------------------------------------------------

def ensure_db_table(db_path: str = DB_PATH):
    """Create the commercial_properties table if it doesn't exist."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS commercial_properties (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            opportunity_id TEXT,
            state TEXT,
            opportunity_lat REAL,
            opportunity_lng REAL,
            opportunity_rule TEXT,
            opportunity_confidence REAL,
            poi_name TEXT,
            
            property_address TEXT NOT NULL,
            property_suburb TEXT,
            property_state TEXT,
            rent_pa REAL,
            rent_sqm REAL,
            rent_display TEXT,
            floor_area_sqm REAL,
            property_type TEXT,
            listing_url TEXT,
            agent TEXT,
            
            distance_km REAL,
            suitability_score INTEGER,
            suitability_note TEXT,
            
            date_scraped TEXT,
            UNIQUE(listing_url)
        )
    """)
    
    # Index for lookups
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_cp_state ON commercial_properties(state)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_cp_score ON commercial_properties(suitability_score)
    """)
    
    conn.commit()
    conn.close()


def insert_property_to_db(prop: Dict, db_path: str = DB_PATH):
    """Insert a property listing into the database."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        cursor.execute("""
            INSERT OR REPLACE INTO commercial_properties
            (opportunity_id, state, opportunity_lat, opportunity_lng,
             opportunity_rule, opportunity_confidence, poi_name,
             property_address, property_suburb, property_state,
             rent_pa, rent_sqm, rent_display, floor_area_sqm,
             property_type, listing_url, agent,
             distance_km, suitability_score, suitability_note,
             date_scraped)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            prop.get('opportunity_id', ''),
            prop.get('state', ''),
            prop.get('opportunity_lat'),
            prop.get('opportunity_lng'),
            prop.get('rule', ''),
            prop.get('confidence', 0),
            prop.get('poi_name', ''),
            prop.get('property_address', ''),
            prop.get('property_suburb', ''),
            prop.get('property_state', ''),
            prop.get('rent_pa', 0),
            prop.get('rent_sqm', 0),
            prop.get('rent_display', ''),
            prop.get('floor_area_sqm', 0),
            prop.get('property_type', ''),
            prop.get('listing_url', ''),
            prop.get('agent', ''),
            prop.get('distance_km', 0),
            prop.get('suitability_score', 0),
            prop.get('suitability_note', ''),
            datetime.now().isoformat(),
        ))
        conn.commit()
    except Exception as e:
        print(f"    [DB] Error inserting: {e}")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def run_property_search(top_n: int = 100, states: List[str] = None,
                        use_browser: bool = True, verbose: bool = True):
    """Main pipeline: load opportunities, search properties, output results."""
    
    print("\n" + "=" * 70)
    print("  PHARMACY OPPORTUNITY — COMMERCIAL PROPERTY FINDER")
    print("=" * 70)
    print(f"  Mode:       {'Browser automation' if use_browser else 'HTTP fallback'}")
    print(f"  Top N:      {top_n}")
    print(f"  States:     {', '.join(states) if states else 'All'}")
    print(f"  Delay:      {REQUEST_DELAY_S}s between requests")
    print()
    
    # Ensure output directory exists
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Set up database
    ensure_db_table()
    
    # Load and rank opportunities
    print("  Loading opportunities...")
    all_opps = load_all_opportunities(states)
    print(f"  Total opportunities: {len(all_opps)}")
    
    # Take top N
    top_opps = all_opps[:top_n]
    print(f"  Searching top {len(top_opps)} (by composite/rank score)")
    
    # Group by suburb to minimize searches
    suburb_opps: Dict[str, List[Dict]] = {}  # "suburb|state" -> [opps]
    no_suburb = []
    
    for i, opp in enumerate(top_opps):
        opp['_index'] = i
        suburb = extract_suburb(opp)
        state = opp['state']
        
        if suburb:
            key = f"{suburb}|{state}"
            if key not in suburb_opps:
                suburb_opps[key] = []
            suburb_opps[key].append(opp)
        else:
            no_suburb.append(opp)
    
    print(f"  Unique suburbs to search: {len(suburb_opps)}")
    if no_suburb:
        print(f"  Opportunities with no suburb extracted: {len(no_suburb)}")
    
    # Search each suburb
    all_results = []
    searched_suburbs = set()
    suburb_listings_cache: Dict[str, List[Dict]] = {}
    
    total_suburbs = len(suburb_opps)
    
    for idx, (key, opps) in enumerate(suburb_opps.items(), 1):
        suburb, state = key.split('|', 1)
        
        if verbose:
            print(f"\n  [{idx}/{total_suburbs}] {suburb}, {state} "
                  f"({len(opps)} opportunities)")
        
        # Search for properties
        listings = []
        
        if use_browser:
            # The caller should provide a browser function
            # For standalone use, we fall back to HTTP
            listings = search_realcommercial_http(suburb, state)
        else:
            listings = search_realcommercial_http(suburb, state)
        
        time.sleep(REQUEST_DELAY_S)
        
        if verbose:
            print(f"    Raw listings: {len(listings)}")
        
        suburb_listings_cache[key] = listings
        
        # Geocode suburb for distance calculation
        suburb_coords = geocode_suburb_nominatim(suburb, state)
        time.sleep(1)  # rate limit nominatim
        
        # Match listings to each opportunity in this suburb
        for opp in opps:
            opp_lat = opp['latitude']
            opp_lon = opp['longitude']
            
            for listing in listings:
                # Check suitability
                suitable, note = is_pharmacy_suitable(listing)
                score = compute_suitability_score(listing)
                
                # Estimate distance (use suburb centroid if we can't geocode the property)
                if suburb_coords:
                    dist = haversine_km(opp_lat, opp_lon, suburb_coords[0], suburb_coords[1])
                else:
                    dist = 0  # unknown
                
                opp_id = f"{opp['state']}_{opp['_index']:03d}"
                
                result = {
                    'opportunity_id': opp_id,
                    'state': opp['state'],
                    'opportunity_lat': opp_lat,
                    'opportunity_lng': opp_lon,
                    'rule': opp['qualifying_rules'],
                    'confidence': opp['confidence'],
                    'confidence_pct': opp['confidence_pct'],
                    'poi_name': opp['poi_name'],
                    'poi_type': opp['poi_type'],
                    'nearest_pharmacy_km': opp['nearest_pharmacy_km'],
                    'composite_score': opp['composite_score'],
                    
                    'property_address': listing['address'],
                    'property_suburb': suburb,
                    'property_state': state,
                    'rent_pa': listing.get('rent_pa', 0),
                    'rent_sqm': listing.get('rent_sqm', 0),
                    'rent_display': listing.get('rent_display', ''),
                    'floor_area_sqm': listing.get('floor_area_sqm', 0),
                    'property_type': listing.get('property_type', ''),
                    'listing_url': listing.get('url', ''),
                    'agent': listing.get('agent', ''),
                    
                    'distance_km': round(dist, 2),
                    'suitable': suitable,
                    'suitability_score': score,
                    'suitability_note': note,
                }
                
                all_results.append(result)
                
                # Insert into database
                insert_property_to_db(result)
        
        searched_suburbs.add(key)
    
    # Filter to suitable properties only for the main output
    suitable_results = [r for r in all_results if r['suitable']]
    
    print(f"\n{'='*70}")
    print(f"  SEARCH COMPLETE")
    print(f"{'='*70}")
    print(f"  Suburbs searched:       {len(searched_suburbs)}")
    print(f"  Total raw listings:     {len(all_results)}")
    print(f"  Pharmacy-suitable:      {len(suitable_results)}")
    
    # Write CSV output
    _write_property_csv(suitable_results)
    
    # Write summary JSON
    _write_summary_json(all_results, suitable_results, top_opps)
    
    # Write all results (including unsuitable) for reference
    _write_all_listings_csv(all_results)
    
    return suitable_results


def _write_property_csv(results: List[Dict]):
    """Write filtered property listings CSV."""
    output_path = os.path.join(OUTPUT_DIR, 'property_listings.csv')
    
    fieldnames = [
        'opportunity_id', 'state', 'opportunity_lat', 'opportunity_lng',
        'rule', 'confidence_pct', 'poi_name', 'nearest_pharmacy_km', 'composite_score',
        'property_address', 'rent_display', 'rent_pa', 'rent_sqm',
        'floor_area_sqm', 'property_type', 'listing_url', 'agent',
        'distance_km', 'suitability_score', 'suitability_note',
    ]
    
    # Sort by suitability score desc
    results_sorted = sorted(results, key=lambda x: (x.get('suitability_score', 0), x.get('composite_score', 0)), reverse=True)
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        for r in results_sorted:
            writer.writerow(r)
    
    print(f"  CSV output: {output_path} ({len(results_sorted)} rows)")


def _write_all_listings_csv(results: List[Dict]):
    """Write ALL listings (including unsuitable) for reference."""
    output_path = os.path.join(OUTPUT_DIR, 'property_listings_all.csv')
    
    fieldnames = [
        'opportunity_id', 'state', 'opportunity_lat', 'opportunity_lng',
        'rule', 'confidence_pct', 'poi_name',
        'property_address', 'rent_display', 'rent_pa', 'rent_sqm',
        'floor_area_sqm', 'property_type', 'listing_url', 'agent',
        'distance_km', 'suitable', 'suitability_score', 'suitability_note',
    ]
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        for r in results:
            writer.writerow(r)
    
    print(f"  All listings: {output_path} ({len(results)} rows)")


def _write_summary_json(all_results: List[Dict], suitable: List[Dict], opps: List[Dict]):
    """Write summary statistics JSON."""
    output_path = os.path.join(OUTPUT_DIR, 'properties_summary.json')
    
    # Per-state stats
    state_stats = {}
    for state in STATE_PRIORITY:
        state_all = [r for r in all_results if r['state'] == state]
        state_suitable = [r for r in suitable if r['state'] == state]
        state_opps = [o for o in opps if o['state'] == state]
        
        if not state_opps:
            continue
        
        # Unique properties (by URL)
        unique_all = len(set(r['listing_url'] for r in state_all if r.get('listing_url')))
        unique_suitable = len(set(r['listing_url'] for r in state_suitable if r.get('listing_url')))
        
        # Rent stats for suitable
        rents = [r['rent_pa'] for r in state_suitable if r.get('rent_pa', 0) > 0]
        sizes = [r['floor_area_sqm'] for r in state_suitable if r.get('floor_area_sqm', 0) > 0]
        
        state_stats[state] = {
            'opportunities_searched': len(state_opps),
            'total_listings_found': unique_all,
            'suitable_listings': unique_suitable,
            'rent_range': {
                'min': min(rents) if rents else None,
                'max': max(rents) if rents else None,
                'median': sorted(rents)[len(rents) // 2] if rents else None,
            },
            'size_range': {
                'min': min(sizes) if sizes else None,
                'max': max(sizes) if sizes else None,
                'median': sorted(sizes)[len(sizes) // 2] if sizes else None,
            },
            'top_opportunities': [],
        }
        
        # Top 5 suitable listings per state
        state_top = sorted(state_suitable, key=lambda x: x.get('suitability_score', 0), reverse=True)[:5]
        for t in state_top:
            state_stats[state]['top_opportunities'].append({
                'opportunity_id': t['opportunity_id'],
                'poi_name': t['poi_name'],
                'property_address': t['property_address'],
                'rent': t['rent_display'],
                'size_sqm': t['floor_area_sqm'],
                'type': t['property_type'],
                'score': t['suitability_score'],
                'url': t['listing_url'],
            })
    
    summary = {
        'generated': datetime.now().isoformat(),
        'total_opportunities_searched': len(opps),
        'total_listings_found': len(all_results),
        'suitable_listings': len(suitable),
        'unique_suitable': len(set(r['listing_url'] for r in suitable if r.get('listing_url'))),
        'by_state': state_stats,
    }
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, default=str)
    
    print(f"  Summary: {output_path}")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main():
    sys.stdout.reconfigure(line_buffering=True)
    
    parser = argparse.ArgumentParser(
        description='Find commercial properties near pharmacy opportunity zones',
    )
    parser.add_argument('--state', type=str, help='Single state to search (e.g., TAS)')
    parser.add_argument('--top', type=int, default=100, help='Top N opportunities to search (default: 100)')
    parser.add_argument('--no-browser', action='store_true', help='Use HTTP fallback instead of browser')
    parser.add_argument('--quiet', action='store_true')
    
    args = parser.parse_args()
    
    states = None
    if args.state:
        states = [args.state.upper()]
    
    run_property_search(
        top_n=args.top,
        states=states,
        use_browser=not args.no_browser,
        verbose=not args.quiet,
    )


if __name__ == '__main__':
    main()
