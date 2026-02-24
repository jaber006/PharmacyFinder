import os
"""
Enhanced PASS verification:
1. Update status for all Foodworks with bad coords to ISSUE_FOUND
2. Check Port Macquarie Private Hospital
3. Cross-reference addresses in the DB opportunities table  
4. Look up real addresses in DB for the Foodworks entries
"""
import json
import sqlite3

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DB_PATH = os.path.join(BASE_DIR, 'pharmacy_finder.db')
VERIF_PATH = r'C:\Users\MJ\Documents\GitHub\PharmacyFinder\output\pass_verification.json'
PASS_LIST = r'C:\Users\MJ\Documents\GitHub\PharmacyFinder\output\pass_list.json'

with open(VERIF_PATH, encoding='utf-8') as f:
    results = json.load(f)

with open(PASS_LIST, encoding='utf-8') as f:
    pass_opps = json.load(f)

# Build lookup by ID
pass_by_id = {o['id']: o for o in pass_opps}

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# Look up DB records for opportunities with bad geocoding
bad_foodworks_ids = []
for r in results:
    if r['lat'] == -32.2443163 and r['lng'] == 147.3564635:
        bad_foodworks_ids.append(r['id'])

print(f"Bad Foodworks entries: {len(bad_foodworks_ids)}")

# Get their DB records to see real addresses
for oid in bad_foodworks_ids:
    cur.execute("SELECT id, poi_name, address, latitude, longitude, region FROM opportunities WHERE id = ?", (oid,))
    row = cur.fetchone()
    if row:
        print(f"  ID={row[0]} | {row[1]} | addr={row[2]} | ({row[3]},{row[4]}) | region={row[5]}")

# Check for opportunities with geocoding mismatch in the DB
print("\n--- Checking DB for state mismatches ---")
cur.execute("""
    SELECT id, poi_name, address, latitude, longitude, region 
    FROM opportunities 
    WHERE qualifying_rules LIKE '%131%'
    AND latitude = -32.2443163 AND longitude = 147.3564635
""")
rows = cur.fetchall()
print(f"DB entries at bad Foodworks coords: {len(rows)}")

# Check Port Macquarie
print("\n--- Port Macquarie Private Hospital ---")
cur.execute("SELECT id, poi_name, address, latitude, longitude FROM opportunities WHERE poi_name LIKE '%Port Macquarie%'")
for row in cur.fetchall():
    print(f"  ID={row[0]} | {row[1]} | addr={row[2]} | ({row[3]},{row[4]})")

# Check how many pharmacies we have in Port Macquarie area
cur_pharm = conn.cursor()
cur_pharm.execute("SELECT id, name, address, latitude, longitude FROM pharmacies WHERE address LIKE '%Port Macquarie%' OR suburb LIKE '%PORT MACQUARIE%'")
pm_pharmas = cur_pharm.fetchall()
print(f"\nPharmacies in Port Macquarie area: {len(pm_pharmas)}")
for p in pm_pharmas:
    print(f"  {p[1]} | {p[2]} | ({p[3]},{p[4]})")

# Now update the verification results with Google Maps findings
for r in results:
    # Port Macquarie Private Hospital - wrong coords, should not be PASS
    if r['id'] == 9356:
        r['status'] = 'ISSUE_FOUND'
        r['issues'].append("WRONG COORDINATES: DB coords (-32.60, 152.02) are ~140km from real Port Macquarie. Hospital is at 86-94 Lake Rd, Port Macquarie NSW 2444. Multiple pharmacies nearby (Lake Road Pharmacy, Chemist Warehouse, etc.)")
        r['missing_pharmacies'] = ['Lake Road Pharmacy (Unit 11/80 Lake Rd)', 'Chemist Warehouse Port Macquarie - Gordon Street', 'Your Discount Chemist Port Macquarie']
    
    # Emerald Medical Centre - wrong coords (geocoded to NSW, real location is QLD)
    if r['id'] == 8716:
        r['status'] = 'ISSUE_FOUND'
        r['issues'].append("WRONG COORDINATES: Real Emerald QLD is at ~(-23.44, 148.16), not (-30.72, 146.80). Emerald has 4+ pharmacies within town. This is NOT a valid PASS.")
        r['missing_pharmacies'] = ['Emerald Superclinic Pharmacy', 'Emerald Plaza Pharmacy', 'Direct Chemist Outlet Emerald', 'Direct Chemist Outlet Central Highlands']
    
    # IGA Emerald - same issue
    if r['id'] == 8901:
        r['status'] = 'ISSUE_FOUND'
        r['issues'].append("WRONG COORDINATES: Same as Emerald Medical Centre - real Emerald QLD has 4+ pharmacies nearby")
    
    # Coles Bay Convenience - VERIFIED via Google Maps
    if r['id'] == 9366:
        r['status'] = 'VERIFIED'
        r['notes'] = "GOOGLE MAPS VERIFIED: Coles Bay Convenience at 3 Garnet Ave. Nearest pharmacy is Swansea Pharmacy (28 Franklin St) ~18km away. Genuinely remote location near Freycinet National Park."
    
    # Yulara Medical Centre - VERIFIED via Google Maps  
    if r['id'] == 7995:
        r['status'] = 'VERIFIED'
        r['notes'] = "GOOGLE MAPS VERIFIED: Congress Yulara Clinic at Lot 233 Yulara Dr, Yulara NT 0872. No pharmacy for ~335km. Genuine remote community near Uluru."
    
    # All bad Foodworks - mark as ISSUE_FOUND with more detail
    if r['lat'] == -32.2443163 and r['lng'] == 147.3564635:
        # Look up real address from DB
        opp = pass_by_id.get(r['id'], {})
        addr = opp.get('address', '')
        if addr:
            r['notes'] = f"Real address from DB: {addr}. Needs re-geocoding."
        else:
            r['notes'] = "No address in DB. Needs re-geocoding from original source."
        r['status'] = 'ISSUE_FOUND'
        if 'GEOCODING FAILURE' not in ' '.join(r['issues']):
            r['issues'].append("GEOCODING FAILURE: Coordinates are wrong (fallback location)")

# Count updated results
verified = sum(1 for r in results if r['status'] == 'VERIFIED')
issue_found = sum(1 for r in results if r['status'] == 'ISSUE_FOUND')
phantom = sum(1 for r in results if r['status'] == 'PHANTOM')

print(f"\n=== UPDATED VERIFICATION ===")
print(f"VERIFIED: {verified}")
print(f"ISSUE_FOUND: {issue_found}")  
print(f"PHANTOM: {phantom}")
print(f"Total: {verified + issue_found + phantom}")

with open(VERIF_PATH, 'w', encoding='utf-8') as f:
    json.dump(results, f, indent=2, ensure_ascii=False)

print(f"\nUpdated results saved to {VERIF_PATH}")

conn.close()
