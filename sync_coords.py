#!/usr/bin/env python3
"""
sync_coords.py — Master coordinate sync and validation.

Problem: The `opportunities` table has stale/wrong coordinates that were copied
once at build time. Fixing source tables (medical_centres, pharmacies, etc.)
doesn't propagate.

Solution:
1. Match each opportunity to its source POI by name
2. Copy the (verified) coordinates from the source table
3. Validate state boundaries
4. Flag anything suspicious

This should run BEFORE score_v2.py every time.
"""

import sqlite3, json, os, math, sys, re

DB_PATH = 'pharmacy_finder.db'

STATE_BOUNDS = {
    'NSW': {'lat': (-37.6, -28.1), 'lng': (140.9, 153.7)},
    'VIC': {'lat': (-39.2, -33.9), 'lng': (140.9, 150.1)},
    'QLD': {'lat': (-29.2, -10.0), 'lng': (137.9, 153.6)},
    'SA':  {'lat': (-38.1, -25.9), 'lng': (129.0, 141.0)},
    'WA':  {'lat': (-35.2, -13.6), 'lng': (112.9, 129.0)},
    'TAS': {'lat': (-43.7, -39.5), 'lng': (143.5, 148.5)},
    'NT':  {'lat': (-26.1, -10.9), 'lng': (128.9, 138.0)},
    'ACT': {'lat': (-36.0, -35.1), 'lng': (148.7, 149.4)},
}


def extract_state(address):
    """Extract state abbreviation from an address string."""
    if not address:
        return None
    addr = address.upper()
    for state in ['NEW SOUTH WALES', 'QUEENSLAND', 'VICTORIA', 'SOUTH AUSTRALIA', 
                  'WESTERN AUSTRALIA', 'TASMANIA', 'NORTHERN TERRITORY']:
        if state in addr:
            mapping = {
                'NEW SOUTH WALES': 'NSW', 'QUEENSLAND': 'QLD', 'VICTORIA': 'VIC',
                'SOUTH AUSTRALIA': 'SA', 'WESTERN AUSTRALIA': 'WA', 
                'TASMANIA': 'TAS', 'NORTHERN TERRITORY': 'NT'
            }
            return mapping[state]
    for abbr in ['NSW', 'QLD', 'VIC', 'SA', 'WA', 'TAS', 'NT', 'ACT']:
        # Match state abbreviation as standalone word
        if re.search(r'\b' + abbr + r'\b', addr):
            return abbr
    return None


def validate_state(lat, lng, expected_state):
    """Check if coordinates fall within expected state bounds."""
    if not expected_state or expected_state not in STATE_BOUNDS:
        return True  # Can't validate
    bounds = STATE_BOUNDS[expected_state]
    lat_ok = bounds['lat'][0] <= lat <= bounds['lat'][1]
    lng_ok = bounds['lng'][0] <= lng <= bounds['lng'][1]
    return lat_ok and lng_ok


def haversine(lat1, lon1, lat2, lon2):
    R = 6371000
    p = math.pi / 180
    a = 0.5 - math.cos((lat2-lat1)*p)/2 + math.cos(lat1*p)*math.cos(lat2*p)*(1-math.cos((lon2-lon1)*p))/2
    return 2 * R * math.asin(math.sqrt(a))


def fuzzy_match(name1, name2):
    """Simple fuzzy name matching."""
    def normalize(s):
        s = s.lower().strip()
        # Remove common suffixes
        for rem in ['medical centre', 'medical center', 'health centre', 'health center',
                     'shopping centre', 'shopping center', 'private hospital', 'hospital',
                     'iga', 'foodworks', 'coles', 'woolworths', 'supermarket', 'store']:
            s = s.replace(rem, '')
        return s.strip()
    
    n1, n2 = normalize(name1), normalize(name2)
    if not n1 or not n2:
        return False
    return n1 == n2 or n1 in n2 or n2 in n1


def main():
    sys.stdout.reconfigure(encoding='utf-8')
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    print("=" * 60)
    print("COORDINATE SYNC & VALIDATION")
    print("=" * 60)
    
    # Load all source tables
    sources = {}
    
    # Medical centres
    c.execute("SELECT id, name, latitude, longitude, state FROM medical_centres WHERE latitude IS NOT NULL")
    sources['medical_centre'] = {r[1]: {'lat': r[2], 'lng': r[3], 'state': r[4], 'id': r[0]} for r in c.fetchall()}
    
    # Shopping centres
    c.execute("SELECT id, name, latitude, longitude FROM shopping_centres WHERE latitude IS NOT NULL")
    sources['shopping_centre'] = {r[1]: {'lat': r[2], 'lng': r[3], 'id': r[0]} for r in c.fetchall()}
    
    # Hospitals
    c.execute("SELECT id, name, latitude, longitude FROM hospitals WHERE latitude IS NOT NULL")
    sources['hospital'] = {r[1]: {'lat': r[2], 'lng': r[3], 'id': r[0]} for r in c.fetchall()}
    
    # Supermarkets
    c.execute("SELECT id, name, latitude, longitude FROM supermarkets WHERE latitude IS NOT NULL")
    sources['supermarket'] = {r[1]: {'lat': r[2], 'lng': r[3], 'id': r[0]} for r in c.fetchall()}
    
    print(f"Loaded sources: {', '.join(f'{k}={len(v)}' for k,v in sources.items())}")
    
    # Load all opportunities
    c.execute("SELECT id, poi_name, poi_type, latitude, longitude, address, evidence FROM opportunities")
    opportunities = c.fetchall()
    print(f"Opportunities: {len(opportunities)}")
    
    synced = 0
    state_violations = 0
    unmatched = 0
    already_ok = 0
    
    issues = []
    
    for opp in opportunities:
        oid, name, poi_type, lat, lng, address, evidence = opp
        
        if not name or not lat or not lng:
            continue
        
        # 1. Try exact match in source table
        source_table = sources.get(poi_type, {})
        matched = None
        
        if name in source_table:
            matched = source_table[name]
        else:
            # Fuzzy match
            for src_name, src_data in source_table.items():
                if fuzzy_match(name, src_name):
                    matched = src_data
                    break
        
        # Also try across all source tables if poi_type didn't match
        if not matched:
            for stype, stable in sources.items():
                if name in stable:
                    matched = stable[name]
                    break
                for src_name, src_data in stable.items():
                    if fuzzy_match(name, src_name):
                        matched = src_data
                        break
                if matched:
                    break
        
        if matched:
            dist = haversine(lat, lng, matched['lat'], matched['lng'])
            if dist > 100:  # More than 100m difference
                c.execute("UPDATE opportunities SET latitude=?, longitude=? WHERE id=?",
                          (matched['lat'], matched['lng'], oid))
                synced += 1
                if dist > 500:
                    print(f"  SYNC {name[:45]:45s} {dist/1000:.1f}km shift")
            else:
                already_ok += 1
        else:
            unmatched += 1
        
        # 2. State validation (on final coords)
        final_lat = matched['lat'] if matched and haversine(lat, lng, matched['lat'], matched['lng']) > 100 else lat
        final_lng = matched['lng'] if matched and haversine(lat, lng, matched['lat'], matched['lng']) > 100 else lng
        
        expected_state = extract_state(address) or extract_state(str(evidence))
        if expected_state and not validate_state(final_lat, final_lng, expected_state):
            state_violations += 1
            issues.append({
                'id': oid,
                'name': name,
                'lat': final_lat,
                'lng': final_lng,
                'expected_state': expected_state,
                'address': (address or '')[:80]
            })
            print(f"  STATE VIOLATION: {name[:40]} — coords not in {expected_state}")
    
    conn.commit()
    
    print(f"\n{'=' * 60}")
    print(f"SYNC RESULTS")
    print(f"{'=' * 60}")
    print(f"  Synced from source: {synced}")
    print(f"  Already correct:    {already_ok}")
    print(f"  No source match:    {unmatched}")
    print(f"  State violations:   {state_violations}")
    
    if issues:
        print(f"\n  STATE VIOLATIONS (coords outside expected state):")
        for iss in issues:
            print(f"    id={iss['id']} {iss['name'][:40]} expected={iss['expected_state']} at ({iss['lat']:.4f},{iss['lng']:.4f})")
    
    # Save issues for review
    with open('output/coord_sync_issues.json', 'w') as f:
        json.dump(issues, f, indent=2, default=str)
    
    conn.close()
    print(f"\nDone. Run score_v2.py and build_dashboard_v3.py to apply.")


if __name__ == '__main__':
    main()
