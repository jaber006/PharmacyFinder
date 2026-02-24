#!/usr/bin/env python3
"""Final fix attempt for remaining INVALID Foodworks entries."""

import sqlite3
import json
import time
import urllib.request
import urllib.parse
import math

DB_PATH = r"C:\Users\MJ\Documents\GitHub\PharmacyFinder\pharmacy_finder.db"

def nominatim_search(query, viewbox=None, limit=5):
    params = {
        'q': query,
        'format': 'json',
        'limit': limit,
        'countrycodes': 'au'
    }
    if viewbox:
        params['viewbox'] = viewbox
        params['bounded'] = '1'
    url = f"https://nominatim.openstreetmap.org/search?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={'User-Agent': 'PharmacyFinder/1.0'})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        print(f"  Error: {e}")
    return []

def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))

def get_nearest_pharmacy(conn, lat, lon):
    cur = conn.execute("SELECT name, latitude, longitude FROM pharmacies")
    best_name, best_dist = None, float('inf')
    for name, plat, plon in cur:
        d = haversine(lat, lon, plat, plon)
        if d < best_dist:
            best_dist, best_name = d, name
    return best_name, best_dist

def main():
    conn = sqlite3.connect(DB_PATH)
    pharmacies = {n: (la, lo) for n, la, lo in conn.execute("SELECT name, latitude, longitude FROM pharmacies")}
    
    # Manual approach for remaining entries
    # These are the ones that couldn't be matched
    manual_fixes = {
        # 7994 NT - Mataranka Foodworks on Stuart Highway - this is genuinely remote, 100km from pharmacy
        # The address says "Stuart Highway, Mulggan, Mataranka" - this is the Mataranka Foodworks
        7994: ("Foodworks, Mataranka, Northern Territory", None),
        
        # 8660 VIC - "Chemist Warehouse" is too generic a pharmacy name. Search Foodworks near generic CW
        8660: None,  # Skip - CW is ambiguous, can't match
        
        # 8663 VIC - Wyndham Discount Chemist in Werribee area
        8663: ("Foodworks, Werribee, Victoria", None),
        
        # 8795 QLD - Moura Pharmacy 46km away 
        8795: ("Foodworks, Moura, Queensland", None),
        
        # 8926 QLD - Barcaldine Pharmacy 107km away
        8926: ("Foodworks, Barcaldine, Queensland", None),
        
        # 9273 NSW - Robinvale Pharmacy 72km away (Robinvale is VIC border)
        9273: ("Foodworks, Robinvale, Victoria", None),
        
        # 9276 NSW - Kandos-Rylstone Pharmacy 7km away
        9276: ("Foodworks, Kandos, New South Wales", None),
        
        # 9284 NSW - Emerton Community Pharmacy
        9284: ("Foodworks, Emerton, New South Wales", None),
    }
    
    fixed = 0
    
    for opp_id, search_info in manual_fixes.items():
        if search_info is None:
            print(f"  ID {opp_id}: Skipping (ambiguous pharmacy name)")
            continue
        
        query, _ = search_info
        row = conn.execute("SELECT region, nearest_pharmacy_name FROM opportunities WHERE id = ?", (opp_id,)).fetchone()
        region, pharm_name = row
        pharm_coords = pharmacies.get(pharm_name)
        
        results = nominatim_search(query)
        time.sleep(1.1)
        
        if results:
            # Pick first result
            rlat, rlon = float(results[0]['lat']), float(results[0]['lon'])
            display = results[0].get('display_name', '')[:80]
            
            if pharm_coords:
                dist_to_pharm = haversine(rlat, rlon, pharm_coords[0], pharm_coords[1])
            else:
                dist_to_pharm = float('inf')
            
            actual_pharm, actual_dist = get_nearest_pharmacy(conn, rlat, rlon)
            
            print(f"  ID {opp_id} ({region}): Found at ({rlat:.6f}, {rlon:.6f}) - {display}")
            print(f"    Nearest pharmacy: {actual_pharm} ({actual_dist:.2f} km)")
            
            conn.execute("""UPDATE opportunities 
                           SET latitude = ?, longitude = ?,
                               nearest_pharmacy_name = ?, nearest_pharmacy_km = ?,
                               verification = 'UNVERIFIED', verification_notes = 'Re-geocoded via Nominatim (manual search)'
                           WHERE id = ?""", (rlat, rlon, actual_pharm, actual_dist, opp_id))
            fixed += 1
        else:
            print(f"  ID {opp_id} ({region}): No results for '{query}'")
    
    # Handle ID 8660 specially - try to find which Chemist Warehouse this is near
    row = conn.execute("SELECT nearest_pharmacy_name FROM opportunities WHERE id = 8660").fetchone()
    print(f"\n  ID 8660: pharmacy name is '{row[0]}' - too generic to match. Keeping INVALID.")
    
    conn.commit()
    print(f"\nFixed {fixed} more entries")
    conn.close()

if __name__ == '__main__':
    main()
