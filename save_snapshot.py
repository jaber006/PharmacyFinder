"""Save a parsed snapshot result. Called between browser scrapes."""
import json
import os
import re
import sys

RESULTS_PATH = os.path.join(os.path.dirname(__file__), 'output', '_scrape_results.json')

def parse_snapshot_text(text: str) -> list:
    """Parse realcommercial snapshot text into listings."""
    BASE_URL = "https://www.realcommercial.com.au"
    listings = []
    current = None
    
    for line in text.split('\n'):
        s = line.strip()
        if not s:
            continue
        
        # Address heading
        m = re.search(r'heading\s+"([^"]*,\s*(?:TAS|VIC|NSW|QLD|SA|WA|NT|ACT)\s+\d{4})"', s)
        if m:
            if current and current.get('address'):
                listings.append(current)
            current = {'address': m.group(1), 'url': '', 'rent_display': '', 
                       'rent_pa': 0, 'rent_sqm': 0, 'floor_area_sqm': 0,
                       'property_type': '', 'agent': ''}
            continue
        
        if not current:
            continue
        
        # URL
        um = re.search(r'/url:\s+(/for-lease/property-[^\s]+)', s)
        if um and not current['url']:
            current['url'] = BASE_URL + um.group(1)
        
        # Price
        pm = re.search(r'heading\s+"(\$[\d,.]+[^"]*)"', s)
        if pm and not current['rent_display']:
            current['rent_display'] = pm.group(1)
            nm = re.search(r'\$([\d,]+)', current['rent_display'])
            if nm:
                try: current['rent_pa'] = float(nm.group(1).replace(',',''))
                except: pass
        if 'Contact Agent' in s and not current['rent_display']:
            current['rent_display'] = 'Contact Agent'
        if 'Price on Application' in s and not current['rent_display']:
            current['rent_display'] = 'Price on Application'
        
        # Agent
        am = re.search(r'heading\s+"Contact\s+([^"]+)"', s)
        if am and not current['agent']:
            a = am.group(1).strip()
            if a.lower() != 'agent': current['agent'] = a
        
        # Size
        sm = re.search(r'(\d[\d,]*)\s*m²', s)
        if sm and not current['floor_area_sqm']:
            current['floor_area_sqm'] = int(sm.group(1).replace(',',''))
        
        # Type
        types = {'Shops & Retail': 'Shops & Retail', 'Medical & Consulting': 'Medical & Consulting',
                 'Offices': 'Offices', 'Warehouse': 'Warehouse/Industrial', 
                 'Industrial': 'Warehouse/Industrial', 'Showroom': 'Showrooms',
                 'Hotel': 'Hotel/Leisure', 'Development': 'Development/Land',
                 'Farming': 'Farming/Rural'}
        for kw, label in types.items():
            if kw in s and label not in (current.get('property_type') or ''):
                if current['property_type']: current['property_type'] += ' | '
                current['property_type'] += label
    
    if current and current.get('address'):
        listings.append(current)
    
    for l in listings:
        if l['rent_pa'] > 0 and l['floor_area_sqm'] > 0:
            l['rent_sqm'] = round(l['rent_pa'] / l['floor_area_sqm'], 2)
    
    return listings


def save_result(suburb: str, state: str, snapshot_text: str):
    """Parse snapshot and save to results JSON."""
    listings = parse_snapshot_text(snapshot_text)
    
    # Load existing
    results = {}
    if os.path.exists(RESULTS_PATH):
        with open(RESULTS_PATH) as f:
            results = json.load(f)
    
    key = suburb + '|' + state
    results[key] = {
        'suburb': suburb,
        'state': state,
        'listing_count': len(listings),
        'listings': listings,
    }
    
    with open(RESULTS_PATH, 'w') as f:
        json.dump(results, f, indent=2)
    
    return listings


if __name__ == '__main__':
    # Read suburb|state from args, snapshot from stdin
    if len(sys.argv) >= 3:
        suburb = sys.argv[1]
        state = sys.argv[2]
        snapshot = sys.stdin.read()
        listings = save_result(suburb, state, snapshot)
        print(f"Saved {len(listings)} listings for {suburb}, {state}")
    else:
        print("Usage: echo SNAPSHOT | python save_snapshot.py SUBURB STATE")
