"""
Final PASS verification - comprehensive update based on all findings.
Updates the verification JSON with detailed status for all 281 PASS opportunities.
"""
import json
import sqlite3
import math

DB_PATH = r'C:\Users\MJ\Documents\GitHub\PharmacyFinder\pharmacy_finder.db'
VERIF_PATH = r'C:\Users\MJ\Documents\GitHub\PharmacyFinder\output\pass_verification.json'

with open(VERIF_PATH, encoding='utf-8') as f:
    results = json.load(f)

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# Categorize all results
categories = {
    'bad_geocoding_foodworks': [],  # 42 Foodworks all at same coords
    'bad_geocoding_supermarket': [],  # Supermarket/IGA at shared coords
    'state_mismatch_real': [],  # Real POIs but coords in wrong state
    'verified_remote_nt': [],  # Genuine remote NT communities
    'verified_remote_other': [],  # Genuine remote non-NT
    'wrong_coords_real_poi': [],  # Real POI but coords are wrong
    'verified_clean': [],  # Clean, verified opportunities
}

for r in results:
    oid = r['id']
    
    # Get DB record
    cur.execute("SELECT poi_name, address, latitude, longitude, region, qualifying_rules FROM opportunities WHERE id = ?", (oid,))
    db_row = cur.fetchone()
    
    if not db_row:
        continue
    
    db_name, db_addr, db_lat, db_lng, db_region, db_rules = db_row
    
    # Category 1: Bad Foodworks geocoding
    if r['lat'] == -32.2443163 and r['lng'] == 147.3564635:
        categories['bad_geocoding_foodworks'].append(r)
        continue
    
    # Category 2: Bad Supermarket geocoding (shared coords)
    if r['lat'] == -42.1224456 and r['lng'] == 148.2845945 and r['name'] == 'Supermarket':
        categories['bad_geocoding_supermarket'].append(r)
        continue
    
    # Category 3: State mismatch but potentially real POI
    if r.get('issues') and any('STATE MISMATCH' in i for i in r['issues']):
        # These are real POIs just with wrong state in DB
        categories['state_mismatch_real'].append(r)
        # Mark them with more specific notes
        r['notes'] = f"POI likely exists but geocoded to wrong state. DB region: {db_region}. Needs re-geocoding."
        continue
    
    # Category 4: Genuine remote NT communities
    if r['state'] == 'NT' and r.get('nearest_pharmacy_km', 0) and r['nearest_pharmacy_km'] > 50:
        categories['verified_remote_nt'].append(r)
        r['status'] = 'VERIFIED'
        r['notes'] = f"Remote NT community. Nearest pharmacy {r['nearest_pharmacy_km']}km away. Genuine Item 131 opportunity."
        continue
    
    # Category 5: Other remote verified
    if r.get('nearest_pharmacy_km', 0) and r['nearest_pharmacy_km'] > 15 and not r.get('issues'):
        categories['verified_remote_other'].append(r)
        r['status'] = 'VERIFIED'
        r['notes'] = f"Remote location. Nearest pharmacy {r['nearest_pharmacy_km']}km away."
        continue
    
    # Category 6: Clean verified
    if not r.get('issues'):
        categories['verified_clean'].append(r)
        r['status'] = 'VERIFIED'
        continue

# Print summary
print("=== PASS VERIFICATION SUMMARY ===\n")
print(f"Total PASS opportunities: {len(results)}\n")
print(f"CATEGORY BREAKDOWN:")
print(f"  Bad Foodworks geocoding (all same coords): {len(categories['bad_geocoding_foodworks'])}")
print(f"  Bad Supermarket geocoding (shared coords): {len(categories['bad_geocoding_supermarket'])}")  
print(f"  State mismatch (real POI, wrong state): {len(categories['state_mismatch_real'])}")
print(f"  Verified remote NT communities: {len(categories['verified_remote_nt'])}")
print(f"  Verified remote other states: {len(categories['verified_remote_other'])}")
print(f"  Clean verified: {len(categories['verified_clean'])}")

# Final counts
verified = sum(1 for r in results if r['status'] == 'VERIFIED')
issue_found = sum(1 for r in results if r['status'] == 'ISSUE_FOUND')
phantom = sum(1 for r in results if r['status'] == 'PHANTOM')

print(f"\nFINAL STATUS:")
print(f"  VERIFIED: {verified}")
print(f"  ISSUE_FOUND: {issue_found}")
print(f"  PHANTOM: {phantom}")

# Issues breakdown
geocoding_issues = sum(1 for r in results if any('GEOCODING' in i for i in r.get('issues', [])))
state_issues = sum(1 for r in results if any('STATE MISMATCH' in i for i in r.get('issues', [])))
coord_issues = sum(1 for r in results if any('WRONG COORDINATES' in i for i in r.get('issues', [])))

print(f"\nISSUE TYPES:")
print(f"  Geocoding failures: {geocoding_issues}")
print(f"  State mismatches: {state_issues}")
print(f"  Wrong coordinates: {coord_issues}")

# Save updated results
with open(VERIF_PATH, 'w', encoding='utf-8') as f:
    json.dump(results, f, indent=2, ensure_ascii=False)
print(f"\nFinal results saved to {VERIF_PATH}")

conn.close()
