#!/usr/bin/env python3
"""Final search for remaining Foodworks entries."""
import sqlite3, json, time, urllib.request, urllib.parse, math

DB = r"C:\Users\MJ\Documents\GitHub\PharmacyFinder\pharmacy_finder.db"
conn = sqlite3.connect(DB)

def search(q):
    params = urllib.parse.urlencode({'q': q, 'format': 'json', 'limit': 3, 'countrycodes': 'au'})
    url = f"https://nominatim.openstreetmap.org/search?{params}"
    req = urllib.request.Request(url, headers={'User-Agent': 'PharmacyFinder/1.0'})
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode())

def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat, dlon = math.radians(lat2-lat1), math.radians(lon2-lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1))*math.cos(math.radians(lat2))*math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))

def get_nearest(conn, lat, lon):
    best_n, best_d = None, float('inf')
    for n, la, lo in conn.execute('SELECT name, latitude, longitude FROM pharmacies'):
        d = haversine(lat, lon, la, lo)
        if d < best_d: best_d, best_n = d, n
    return best_n, best_d

pharmacies = {n: (la, lo) for n, la, lo in conn.execute("SELECT name, latitude, longitude FROM pharmacies")}

# Try various searches for remaining entries
# Each: (opp_id, search_queries_to_try, pharmacy_name)
attempts = [
    (8663, ["FoodWorks Werribee", "Foodworks Wyndham Vale VIC"], "Wyndham Discount Chemist"),
    (8795, ["Foodworks Biloela QLD", "FoodWorks Moura QLD"], "Moura Pharmacy"),
    (8926, ["Foodworks Longreach QLD", "FoodWorks Alpha QLD"], "Barcaldine Pharmacy"),
    (9273, ["Foodworks Robinvale VIC", "FoodWorks Mildura VIC"], "Robinvale Pharmacy"),
    (9276, ["Foodworks Rylstone NSW", "FoodWorks Kandos NSW", "Foodworks Mudgee NSW"], "Kandos-Rylstone Pharmacy"),
    (9284, ["Foodworks Blackett NSW", "Foodworks Mount Druitt NSW", "Foodworks Emerton NSW"], "Emerton Community Pharmacy"),
    (8660, ["Foodworks Cairns QLD", "FoodWorks Edmonton QLD"], "Chemist Warehouse"),
]

for opp_id, queries, pharm_name in attempts:
    pharm_coords = pharmacies.get(pharm_name)
    found = False
    
    for q in queries:
        results = search(q)
        time.sleep(1.1)
        
        for r in results:
            dn = r.get('display_name', '')
            if 'foodworks' in dn.lower() or 'food works' in dn.lower():
                lat, lon = float(r['lat']), float(r['lon'])
                if pharm_coords:
                    dist = haversine(lat, lon, pharm_coords[0], pharm_coords[1])
                else:
                    dist = 999
                
                pn, pd = get_nearest(conn, lat, lon)
                print(f"ID {opp_id}: '{q}' -> ({lat:.6f}, {lon:.6f}) dist_to_pharm={dist:.1f}km")
                print(f"  Display: {dn[:100]}")
                print(f"  Nearest pharmacy: {pn} ({pd:.2f} km)")
                
                if dist < 10:  # Close enough
                    conn.execute("""UPDATE opportunities 
                                   SET latitude = ?, longitude = ?,
                                       nearest_pharmacy_name = ?, nearest_pharmacy_km = ?,
                                       verification = 'UNVERIFIED', 
                                       verification_notes = 'Re-geocoded via Nominatim'
                                   WHERE id = ?""", (lat, lon, pn, pd, opp_id))
                    found = True
                    print(f"  -> FIXED!")
                    break
        if found:
            break
    
    if not found:
        print(f"ID {opp_id}: No match found, keeping INVALID")

conn.commit()
conn.close()
print("\nDone!")
