"""
PASS Opportunity Verification Script
Automatically flags issues in PASS opportunities without needing Google Maps API.
Checks for: bad geocoding, duplicate coords, phantom POIs, state mismatches, etc.
"""
import json
import sqlite3
import math

DB_PATH = r'C:\Users\MJ\Documents\GitHub\PharmacyFinder\pharmacy_finder.db'
PASS_LIST = r'C:\Users\MJ\Documents\GitHub\PharmacyFinder\output\pass_list.json'
OUTPUT = r'C:\Users\MJ\Documents\GitHub\PharmacyFinder\output\pass_verification.json'

def haversine(lat1, lon1, lat2, lon2):
    """Distance in km between two points"""
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))

def get_state_from_coords(lat, lng):
    """Rough state detection from coordinates"""
    # Very rough bounding boxes for Australian states
    if lat > -10.7 and lat < -11: return 'NT'  # Top End
    if lng < 129.5: return 'WA'  # Far west
    if lat > -26 and lng < 138: 
        if lat > -20: return 'NT'
        return 'SA'  # or NT
    if lat > -10 and lng > 130 and lng < 138: return 'NT'
    if lat > -18 and lng > 128 and lng < 138: return 'NT'  
    if lat > -26 and lng > 138 and lng < 154: return 'QLD'
    if lat > -28 and lng > 148 and lng < 154: return 'QLD'  
    if lat > -29.5 and lng > 148 and lng < 154: return 'QLD'
    if lat < -39 and lng > 143 and lng < 149: return 'TAS'
    if lat < -34 and lng > 140.5 and lng < 150: return 'VIC'
    if lat < -37 and lng > 140 and lng < 150: return 'VIC'
    if lat > -36.5 and lat < -34 and lng > 148 and lng < 150.5: 
        return 'ACT'  # or NSW
    if lng > 114 and lng < 129: return 'WA'
    if lat < -31 and lat > -38 and lng > 134 and lng < 141: return 'SA'
    if lat > -34 and lng > 140 and lng < 154: return 'NSW'
    if lat > -38 and lat < -28 and lng > 148 and lng < 154: return 'NSW'
    return 'Unknown'

def load_pharmacies_from_db():
    """Load all pharmacies from the database"""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT id, name, latitude, longitude, state FROM pharmacies")
    pharmacies = [{'id': r[0], 'name': r[1], 'lat': r[2], 'lng': r[3], 'state': r[4]} for r in cur.fetchall()]
    conn.close()
    return pharmacies

def find_nearest_pharmacies(lat, lng, pharmacies, n=5):
    """Find n nearest pharmacies to a point"""
    dists = []
    for p in pharmacies:
        if p['lat'] and p['lng']:
            d = haversine(lat, lng, p['lat'], p['lng'])
            dists.append((d, p))
    dists.sort(key=lambda x: x[0])
    return dists[:n]

def verify_opportunities():
    with open(PASS_LIST, encoding='utf-8') as f:
        pass_opps = json.load(f)
    
    pharmacies = load_pharmacies_from_db()
    print(f"Loaded {len(pharmacies)} pharmacies from DB")
    print(f"Verifying {len(pass_opps)} PASS opportunities...")
    
    results = []
    
    # Track duplicate coord groups
    coord_groups = {}
    for o in pass_opps:
        k = f"{o.get('lat','')},{o.get('lng','')}"
        coord_groups.setdefault(k, []).append(o)
    
    for o in pass_opps:
        oid = o['id']
        name = o['name']
        state = o.get('state', '')
        lat = o.get('lat', 0)
        lng = o.get('lng', 0)
        addr = o.get('address', '')
        poi_type = o.get('poi_type', '')
        nearest_km = o.get('nearest_pharmacy_km', 0)
        nearest_name = o.get('nearest_pharmacy', '')
        geocoding_flag = o.get('geocoding_flag', '')
        score = o.get('score', 0)
        
        issues = []
        missing_pharmacies = []
        status = "VERIFIED"
        notes = ""
        
        # Check 1: Duplicate coordinates (geocoding failure)
        coord_key = f"{lat},{lng}"
        if coord_key in coord_groups and len(coord_groups[coord_key]) > 2:
            count = len(coord_groups[coord_key])
            issues.append(f"GEOCODING FAILURE: Shares coordinates with {count-1} other POIs - all defaulted to same fallback location ({lat},{lng})")
            status = "ISSUE_FOUND"
        
        # Check 2: Geocoding state mismatch
        if geocoding_flag:
            issues.append(f"STATE MISMATCH: {geocoding_flag}")
            status = "ISSUE_FOUND"
        
        # Check 3: Coords out of Australia
        if lat > -9 or lat < -45 or lng < 112 or lng > 155:
            issues.append(f"COORDS OUT OF AUSTRALIA: ({lat},{lng})")
            status = "PHANTOM"
        
        # Check 4: Generic names that might be phantom
        phantom_indicators = ['Supermarket', 'IGA', 'Foodworks', 'Coles', 'Woolworths']
        if name in phantom_indicators and not addr:
            issues.append(f"GENERIC NAME with no address: '{name}' - may be phantom or unverifiable")
        
        # Check 5: Verify nearest pharmacy distance using our DB
        if lat and lng:
            nearest = find_nearest_pharmacies(lat, lng, pharmacies, 3)
            if nearest:
                db_nearest_km = nearest[0][0]
                db_nearest_name = nearest[0][1]['name']
                
                # If our nearest is significantly different from what scored_v2 says
                if nearest_km and abs(db_nearest_km - nearest_km) > 2:
                    issues.append(f"DISTANCE DISCREPANCY: DB says nearest pharmacy is {db_nearest_name} at {db_nearest_km:.2f}km, but scored_v2 says {nearest_name} at {nearest_km}km")
                
                notes_parts = [f"DB nearest: {db_nearest_name} at {db_nearest_km:.2f}km"]
                for d, p in nearest[1:3]:
                    notes_parts.append(f"{p['name']} at {d:.2f}km")
                notes = "; ".join(notes_parts)
        
        # Check 6: Is the POI type sensible?
        if poi_type == 'hospital' and nearest_km and nearest_km < 0.5:
            # Hospital with pharmacy very close - may already have one inside
            issues.append(f"Hospital with pharmacy only {nearest_km}km away - may already have pharmacy inside")
        
        # Check 7: Shopping centres - check if already has pharmacy inside
        if poi_type == 'shopping_centre' and nearest_km and nearest_km < 0.1:
            issues.append(f"Shopping centre with pharmacy only {nearest_km}km away - likely already has pharmacy inside")
        
        # Check 8: For Item 131 (10km by road), verify straight-line is at least ~7km
        if o.get('original_rules', '') == 'Item 131' and nearest_km and nearest_km < 7:
            issues.append(f"Item 131 requires 10km by road but nearest pharmacy is only {nearest_km}km straight-line")
        
        # Check 9: Very remote NT communities - verify they're real places
        if state == 'NT' and nearest_km and nearest_km > 100:
            # These are likely genuine remote communities
            notes = f"Very remote community ({nearest_km}km from nearest pharmacy). " + notes
        
        # Determine final status
        if status != "PHANTOM":
            if issues:
                status = "ISSUE_FOUND"
            else:
                status = "VERIFIED"
        
        results.append({
            "id": oid,
            "name": name,
            "state": state,
            "lat": lat,
            "lng": lng,
            "poi_type": poi_type,
            "score": score,
            "nearest_pharmacy_km": nearest_km,
            "nearest_pharmacy": nearest_name,
            "status": status,
            "issues": issues,
            "missing_pharmacies": missing_pharmacies,
            "notes": notes
        })
    
    # Summary
    verified = sum(1 for r in results if r['status'] == 'VERIFIED')
    issue_found = sum(1 for r in results if r['status'] == 'ISSUE_FOUND')
    phantom = sum(1 for r in results if r['status'] == 'PHANTOM')
    
    print(f"\n=== VERIFICATION RESULTS ===")
    print(f"VERIFIED: {verified}")
    print(f"ISSUE_FOUND: {issue_found}")
    print(f"PHANTOM: {phantom}")
    
    print(f"\n--- Issues found ---")
    for r in results:
        if r['issues']:
            print(f"  ID={r['id']} {r['name']} ({r['state']})")
            for issue in r['issues']:
                print(f"    - {issue}")
    
    with open(OUTPUT, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    print(f"\nResults saved to {OUTPUT}")
    return results

if __name__ == '__main__':
    verify_opportunities()
