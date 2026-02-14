"""
Commercial Property Finder for Top Opportunities

For the highest-scoring opportunity zones, searches realcommercial.com.au
for available retail/medical tenancies nearby.

This makes opportunities immediately actionable — "here's a gap AND here's
a property you could lease."

Usage:
    python find_properties.py --state TAS
    python find_properties.py --state TAS --top 5
    python find_properties.py --all --top 10
"""

import argparse
import csv
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.database import Database
from utils.distance import haversine_distance
from utils.geocoding import Geocoder


# -- Configuration -------------------------------------------------

# Search radius for properties near an opportunity (km)
PROPERTY_SEARCH_RADIUS_KM = 2.0

# Rate limit between web requests
REQUEST_DELAY_S = 2.0


# -- Property search via web scraping ------------------------------

def search_realcommercial(suburb: str, state: str, 
                           property_types: List[str] = None) -> List[Dict]:
    """
    Search realcommercial.com.au for lease listings in a suburb.
    
    Returns list of {address, url, property_type, price, size, description}.
    """
    if property_types is None:
        property_types = ['retail', 'medical']
    
    results = []
    state_lower = state.lower()
    
    # Build search URL
    # realcommercial.com.au/for-lease/in-<suburb>-<state>-<postcode>/list-1
    suburb_slug = suburb.lower().replace(' ', '-')
    url = f"https://www.realcommercial.com.au/for-lease/in-{suburb_slug}-{state_lower}/list-1?activeSort=list-date&keywords=retail+medical+pharmacy+shop"
    
    for attempt in range(3):
        try:
            req = urllib.request.Request(url, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml',
                'Accept-Language': 'en-AU,en;q=0.9',
                'Referer': 'https://www.realcommercial.com.au/',
            })
            
            with urllib.request.urlopen(req, timeout=30) as resp:
                html = resp.read().decode('utf-8', errors='replace')
            
            # Parse listings from HTML
            results = _parse_realcommercial_html(html, suburb, state)
            break
            
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < 2:
                wait = (attempt + 1) * 5
                print(f"    [RATE] Rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue
            err = str(e)[:80]
            print(f"    [WARN] realcommercial.com.au: {err}")
            break
        except Exception as e:
            err = str(e)[:80]
            print(f"    [WARN] realcommercial.com.au: {err}")
            break
    
    return results


def _parse_realcommercial_html(html: str, suburb: str, state: str) -> List[Dict]:
    """Parse realcommercial.com.au search results HTML."""
    results = []
    
    # Look for listing data in JSON-LD or structured data
    # realcommercial uses React/Next.js, data is in __NEXT_DATA__
    next_data_match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
    
    if next_data_match:
        try:
            data = json.loads(next_data_match.group(1))
            # Navigate the JSON structure to find listings
            props = data.get('props', {}).get('pageProps', {})
            
            # Try different possible paths in the data structure
            listings = (props.get('listings', []) or 
                       props.get('searchResults', {}).get('listings', []) or
                       props.get('results', []))
            
            for listing in listings[:20]:  # Cap at 20 per search
                result = _extract_listing(listing, suburb, state)
                if result:
                    results.append(result)
        except (json.JSONDecodeError, AttributeError, TypeError):
            pass
    
    # Fallback: parse basic HTML structure
    if not results:
        # Look for listing cards
        listing_pattern = re.compile(
            r'href="(/for-lease/[^"]*?)"[^>]*>.*?'
            r'(?:address|location)[^>]*>([^<]+)',
            re.DOTALL | re.IGNORECASE
        )
        
        for match in listing_pattern.finditer(html):
            url_path = match.group(1)
            address = match.group(2).strip()
            
            results.append({
                'address': address,
                'url': f"https://www.realcommercial.com.au{url_path}",
                'property_type': 'retail/commercial',
                'price': '',
                'size': '',
                'description': '',
                'suburb': suburb,
                'state': state,
            })
    
    return results


def _extract_listing(listing: dict, suburb: str, state: str) -> Optional[Dict]:
    """Extract a single listing from realcommercial JSON data."""
    try:
        address = (listing.get('address', {}).get('display', '') or
                  listing.get('displayAddress', '') or
                  listing.get('address', ''))
        
        if isinstance(address, dict):
            parts = []
            if address.get('street'):
                parts.append(address['street'])
            if address.get('suburb'):
                parts.append(address['suburb'])
            if address.get('state'):
                parts.append(address['state'])
            address = ', '.join(parts)
        
        if not address:
            return None
        
        url = listing.get('url', '') or listing.get('listingUrl', '')
        if url and not url.startswith('http'):
            url = f"https://www.realcommercial.com.au{url}"
        
        price = (listing.get('price', {}).get('display', '') or
                listing.get('displayPrice', '') or
                listing.get('priceText', '') or '')
        
        if isinstance(price, dict):
            price = price.get('display', str(price))
        
        size = ''
        area = listing.get('propertySizes', [])
        if area:
            if isinstance(area, list) and area:
                size = str(area[0])
            else:
                size = str(area)
        
        prop_type = listing.get('propertyType', '') or listing.get('category', '')
        
        return {
            'address': str(address),
            'url': str(url),
            'property_type': str(prop_type),
            'price': str(price),
            'size': str(size),
            'description': (listing.get('headline', '') or listing.get('title', '') or '')[:200],
            'suburb': suburb,
            'state': state,
        }
    except Exception:
        return None


def search_commercialrealestate(suburb: str, state: str) -> List[Dict]:
    """
    Search commercialrealestate.com.au for lease listings.
    """
    results = []
    state_lower = state.lower()
    suburb_slug = suburb.lower().replace(' ', '-')
    
    url = f"https://www.commercialrealestate.com.au/for-lease/in-{suburb_slug}-{state_lower}/"
    
    try:
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html',
        })
        
        with urllib.request.urlopen(req, timeout=30) as resp:
            html = resp.read().decode('utf-8', errors='replace')
        
        # Similar parsing approach
        next_data_match = re.search(r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
        if next_data_match:
            try:
                data = json.loads(next_data_match.group(1))
                listings = data.get('props', {}).get('pageProps', {}).get('listingsMap', {})
                if isinstance(listings, dict):
                    for listing in listings.values():
                        result = _extract_listing(listing, suburb, state)
                        if result:
                            results.append(result)
            except (json.JSONDecodeError, TypeError):
                pass
                
    except Exception as e:
        err = str(e)[:80]
        if '403' not in err:
            print(f"    [WARN] commercialrealestate.com.au failed: {err}")
    
    return results


# -- Suburb extraction from coordinates ---------------------------

def get_suburb_from_address(address: str) -> str:
    """Extract suburb name from an address string."""
    if not address:
        return ''
    
    # Try to extract suburb from comma-separated address
    parts = [p.strip() for p in address.split(',')]
    
    # Common pattern: "123 Street, Suburb, State, Postcode, Australia"
    for part in parts:
        # Skip parts that are clearly not suburbs
        if any(x in part.lower() for x in ['australia', 'road', 'street', 'avenue',
                                             'drive', 'place', 'lane', 'highway',
                                             'lookout', 'apartments']):
            continue
        if re.match(r'^\d{4}$', part.strip()):  # postcode
            continue
        if part.strip() in ('NSW', 'VIC', 'QLD', 'WA', 'SA', 'TAS', 'NT', 'ACT'):
            continue
        if re.match(r'^(City of|Municipality of|Shire of)', part):
            continue
        # This is probably the suburb
        if len(part) > 2 and not part[0].isdigit():
            return part.strip()
    
    return ''


# -- Main pipeline -------------------------------------------------

def find_properties_for_state(state: str, top_n: int = 10, verbose: bool = True):
    """Find commercial properties near top opportunities in a state."""
    print(f"\n{'='*60}")
    print(f"  COMMERCIAL PROPERTY SEARCH - {state}")
    print(f"{'='*60}")

    # Load population-ranked opps if available, else verified, else original
    csv_path = None
    for prefix in ['population_ranked', 'verified_opportunities', 'opportunity_zones']:
        path = f"output/{prefix}_{state}.csv"
        if os.path.exists(path):
            csv_path = path
            break
    
    if csv_path is None:
        print(f"  [ERROR] No opportunity CSV found for {state}")
        return

    # Load opportunities
    opps = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Skip false positives
            if row.get('Verification') == 'FALSE POSITIVE':
                continue
            try:
                row['_lat'] = float(row['Latitude'])
                row['_lon'] = float(row['Longitude'])
            except (ValueError, TypeError):
                continue
            opps.append(row)

    if not opps:
        print(f"  No valid opportunities to search")
        return

    print(f"  Loaded {len(opps)} opportunities (top {top_n} will be searched)")
    
    # Take top N
    opps = opps[:top_n]

    # Extract unique suburbs to search
    suburbs_to_search = set()
    opp_suburbs = {}  # map opp index -> suburb
    
    for i, opp in enumerate(opps):
        suburb = ''
        
        # Try extracting from address
        address = opp.get('Address', '')
        if address:
            suburb = get_suburb_from_address(address)
        
        # Try nearest town from population data
        if not suburb:
            town = opp.get('Nearest Town', '')
            if town and town != 'Unknown':
                suburb = town
        
        # Try reverse-geocoding the POI name for suburb hints
        if not suburb:
            poi = opp.get('POI Name', '')
            if poi:
                # Remove common non-suburb words from POI name
                cleaned = re.sub(r'(IGA|Woolworths|Coles|Aldi|Shopping Centre|Hospital|Supermarket|Medical)', '', poi, flags=re.IGNORECASE).strip()
                if cleaned and len(cleaned) > 2:
                    suburb = cleaned
        
        if suburb:
            suburbs_to_search.add((suburb, state))
            opp_suburbs[i] = suburb

    print(f"  Unique suburbs to search: {len(suburbs_to_search)}")
    
    if not suburbs_to_search:
        print(f"  [WARN] Could not extract suburb names from addresses")
        return

    # Search each suburb
    all_properties = {}  # suburb -> [properties]
    
    for suburb, st in suburbs_to_search:
        if verbose:
            print(f"\n  Searching {suburb}, {st}...")
        
        properties = search_realcommercial(suburb, st)
        time.sleep(REQUEST_DELAY_S)
        
        if not properties:
            properties = search_commercialrealestate(suburb, st)
            time.sleep(REQUEST_DELAY_S)
        
        if properties:
            all_properties[suburb] = properties
            if verbose:
                print(f"    Found {len(properties)} listings")
        else:
            if verbose:
                print(f"    No listings found")

    # Match properties to opportunities
    results = []
    for i, opp in enumerate(opps):
        suburb = opp_suburbs.get(i, '')
        properties = all_properties.get(suburb, [])
        
        result = {
            **{k: v for k, v in opp.items() if not k.startswith('_')},
            'Properties Available': 'Yes' if properties else 'No',
            'Property Count': len(properties),
            'Property Listings': '',
        }
        
        if properties:
            listings = []
            for p in properties[:3]:  # Top 3 per opportunity
                listing_str = f"{p['address']}"
                if p.get('price'):
                    listing_str += f" ({p['price']})"
                if p.get('url'):
                    listing_str += f" [{p['url']}]"
                listings.append(listing_str)
            result['Property Listings'] = ' | '.join(listings)
        
        results.append(result)

    # Write output
    output_path = f"output/actionable_opportunities_{state}.csv"
    _write_results_csv(results, output_path)

    # Summary
    with_properties = sum(1 for r in results if r['Properties Available'] == 'Yes')
    total_listings = sum(r['Property Count'] for r in results)
    
    print(f"\n  {'='*50}")
    print(f"  PROPERTY SEARCH RESULTS - {state}")
    print(f"  {'='*50}")
    print(f"  Opportunities searched:  {len(results)}")
    print(f"  With properties nearby:  {with_properties}")
    print(f"  Total property listings: {total_listings}")
    print(f"  Output: {output_path}")

    return results


def _write_results_csv(results: List[Dict], output_path: str):
    """Write actionable opportunities to CSV."""
    if not results:
        return

    fieldnames = [
        'Latitude', 'Longitude', 'Address', 'Qualifying Rules', 'Evidence',
        'Confidence', 'Nearest Pharmacy (km)', 'Nearest Pharmacy Name',
        'POI Name', 'POI Type', 'Region', 'Date Scanned',
    ]
    
    # Add optional fields if present
    for field in ['Verification', 'Verification Notes',
                  'Pop 5km', 'Pop 10km', 'Pop 15km',
                  'Nearest Town', 'Nearest Town Pop', 'Nearest Town Dist (km)',
                  'Opportunity Score']:
        if any(field in r for r in results):
            fieldnames.append(field)
    
    # Add property fields
    fieldnames.extend(['Properties Available', 'Property Count', 'Property Listings'])

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        for result in results:
            writer.writerow(result)


# -- Main ----------------------------------------------------------

def main():
    sys.stdout.reconfigure(line_buffering=True)

    parser = argparse.ArgumentParser(
        description='Find commercial properties near top opportunity zones',
    )
    parser.add_argument('--state', type=str, help='State to search (e.g., TAS)')
    parser.add_argument('--all', action='store_true', help='Search all states')
    parser.add_argument('--top', type=int, default=10,
                        help='Number of top opportunities to search (default: 10)')
    parser.add_argument('--quiet', action='store_true')

    args = parser.parse_args()

    if not args.state and not args.all:
        parser.print_help()
        return

    states = []
    if args.all:
        states = ['TAS', 'ACT', 'NT', 'SA', 'WA', 'QLD', 'NSW', 'VIC']
    elif args.state:
        states = [args.state.upper()]

    for state in states:
        find_properties_for_state(state, top_n=args.top, verbose=not args.quiet)


if __name__ == '__main__':
    main()
