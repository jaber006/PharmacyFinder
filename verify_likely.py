"""
Verify all LIKELY opportunities from scored_v2.json.
- Check coords via Nominatim reverse geocoding
- Output results to likely_verification.json
"""
import json
import time
import urllib.request
import urllib.parse
import ssl
import sys

# Disable SSL verification for corporate proxies
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

def nominatim_reverse(lat, lng):
    """Reverse geocode coordinates via Nominatim."""
    url = f"https://nominatim.openstreetmap.org/reverse?lat={lat}&lon={lng}&format=json&addressdetails=1&zoom=18"
    req = urllib.request.Request(url, headers={'User-Agent': 'PharmacyFinder/1.0 (research)'})
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=15) as resp:
            return json.loads(resp.read().decode('utf-8'))
    except Exception as e:
        return {'error': str(e)}

def nominatim_search(query, limit=3):
    """Search Nominatim for a place."""
    url = f"https://nominatim.openstreetmap.org/search?q={urllib.parse.quote(query)}&format=json&addressdetails=1&limit={limit}&countrycodes=au"
    req = urllib.request.Request(url, headers={'User-Agent': 'PharmacyFinder/1.0 (research)'})
    try:
        with urllib.request.urlopen(req, context=ctx, timeout=15) as resp:
            return json.loads(resp.read().decode('utf-8'))
    except Exception as e:
        return [{'error': str(e)}]

STATE_MAP = {
    'new south wales': 'NSW', 'nsw': 'NSW',
    'victoria': 'VIC', 'vic': 'VIC',
    'queensland': 'QLD', 'qld': 'QLD',
    'south australia': 'SA', 'sa': 'SA',
    'western australia': 'WA', 'wa': 'WA',
    'tasmania': 'TAS', 'tas': 'TAS',
    'northern territory': 'NT', 'nt': 'NT',
    'australian capital territory': 'ACT', 'act': 'ACT',
}

def normalize_state(s):
    if not s:
        return ''
    return STATE_MAP.get(s.lower().strip(), s.upper().strip())

def check_coords(item):
    """Check if coords match expected state/suburb."""
    lat, lng = item['lat'], item['lng']
    result = nominatim_reverse(lat, lng)
    issues = []
    notes = []
    
    if 'error' in result:
        issues.append(f"Nominatim reverse failed: {result['error']}")
        return issues, notes, result
    
    addr = result.get('address', {})
    nom_state = normalize_state(addr.get('state', ''))
    expected_state = item.get('state', '')
    
    if nom_state and expected_state and nom_state != expected_state:
        issues.append(f"State mismatch: expected {expected_state}, coords resolve to {nom_state}")
    
    display_name = result.get('display_name', '')
    notes.append(f"Nominatim resolves to: {display_name}")
    
    # Check if it's in Australia
    country = addr.get('country_code', '')
    if country and country != 'au':
        issues.append(f"Coords not in Australia! Country: {country}")
    
    return issues, notes, result

def verify_poi_exists(item):
    """Search Nominatim to see if the POI can be found."""
    name = item['name']
    state = item['state']
    # Try searching for the POI
    query = f"{name}, {state}, Australia"
    results = nominatim_search(query)
    
    if not results or (len(results) == 1 and 'error' in results[0]):
        return False, f"Could not find '{name}' on OSM"
    
    # Check if any result is within ~5km of our coords
    import math
    for r in results:
        try:
            rlat, rlng = float(r['lat']), float(r['lon'])
            dlat = rlat - item['lat']
            dlng = rlng - item['lng']
            dist_km = math.sqrt(dlat**2 + dlng**2) * 111  # rough km
            if dist_km < 10:
                return True, f"Found on OSM within {dist_km:.1f}km: {r.get('display_name','')}"
        except:
            continue
    
    return False, f"OSM results not near coords. Query: {query}"


def main():
    with open('output/likely_compact.json', 'r', encoding='utf-8') as f:
        likely = json.load(f)
    
    print(f"Processing {len(likely)} LIKELY opportunities...")
    
    results = []
    
    for i, item in enumerate(likely):
        print(f"\n[{i+1}/{len(likely)}] id={item['id']} {item['name']} ({item['poi_type']}, {item['state']})")
        
        entry = {
            'id': item['id'],
            'name': item['name'],
            'poi_type': item['poi_type'],
            'state': item['state'],
            'lat': item['lat'],
            'lng': item['lng'],
            'address': item['address'],
            'verified': None,
            'coords_correct': None,
            'poi_found_osm': None,
            'tenant_count': None,
            'pharmacy_inside': None,
            'missing_pharmacies': [],
            'issues': [],
            'notes': [],
            'nominatim_address': '',
        }
        
        # Step 1: Check coords
        print(f"  Checking coords ({item['lat']}, {item['lng']})...")
        coord_issues, coord_notes, nom_result = check_coords(item)
        entry['issues'].extend(coord_issues)
        entry['notes'].extend(coord_notes)
        entry['coords_correct'] = len(coord_issues) == 0
        if not isinstance(nom_result, dict) or 'error' not in nom_result:
            entry['nominatim_address'] = nom_result.get('display_name', '')
        
        time.sleep(1.1)  # Nominatim rate limit
        
        # Step 2: Verify POI exists on OSM
        print(f"  Searching OSM for '{item['name']}'...")
        found, note = verify_poi_exists(item)
        entry['poi_found_osm'] = found
        entry['notes'].append(note)
        
        time.sleep(1.1)  # Nominatim rate limit
        
        # Step 3: Set verified status
        if entry['coords_correct'] and entry['poi_found_osm']:
            entry['verified'] = True
        elif not entry['coords_correct']:
            entry['verified'] = False
        else:
            entry['verified'] = None  # uncertain - POI not found on OSM but coords ok
        
        results.append(entry)
        
        # Save progress every 5 items
        if (i + 1) % 5 == 0 or i == len(likely) - 1:
            with open('output/likely_verification.json', 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False)
            print(f"  [Saved progress: {len(results)} items]")
    
    print(f"\nDone! Processed {len(results)} items.")
    print(f"Verified: {sum(1 for r in results if r['verified'] == True)}")
    print(f"Failed: {sum(1 for r in results if r['verified'] == False)}")
    print(f"Uncertain: {sum(1 for r in results if r['verified'] is None)}")

if __name__ == '__main__':
    # Force unbuffered output
    sys.stdout.reconfigure(line_buffering=True)
    main()
