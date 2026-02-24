#!/usr/bin/env python3
"""Fix remaining INVALID Foodworks entries by searching Nominatim near the pharmacy."""

import sqlite3
import json
import time
import urllib.request
import urllib.parse
import math

DB_PATH = r"C:\Users\MJ\Documents\GitHub\PharmacyFinder\pharmacy_finder.db"

def nominatim_search(query, lat=None, lon=None, limit=5):
    """Search OpenStreetMap Nominatim. If lat/lon given, search nearby."""
    params = {
        'q': query,
        'format': 'json',
        'limit': limit,
        'countrycodes': 'au'
    }
    if lat and lon:
        params['viewbox'] = f"{lon-0.05},{lat+0.05},{lon+0.05},{lat-0.05}"
        params['bounded'] = '1'
    
    url = f"https://nominatim.openstreetmap.org/search?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={'User-Agent': 'PharmacyFinder/1.0'})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            return data
    except Exception as e:
        print(f"  Error: {e}")
    return []

def nominatim_reverse(lat, lon):
    """Reverse geocode to get place name."""
    params = urllib.parse.urlencode({
        'lat': lat, 'lon': lon, 'format': 'json', 'zoom': 16
    })
    url = f"https://nominatim.openstreetmap.org/reverse?{params}"
    req = urllib.request.Request(url, headers={'User-Agent': 'PharmacyFinder/1.0'})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode())
            return data.get('display_name', ''), data.get('address', {})
    except Exception as e:
        print(f"  Reverse error: {e}")
    return '', {}

def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))

def get_nearest_pharmacy(conn, lat, lon):
    """Find nearest pharmacy to given coordinates."""
    cur = conn.execute("SELECT name, latitude, longitude FROM pharmacies")
    best_name = None
    best_dist = float('inf')
    for name, plat, plon in cur:
        d = haversine(lat, lon, plat, plon)
        if d < best_dist:
            best_dist = d
            best_name = name
    return best_name, best_dist

def main():
    conn = sqlite3.connect(DB_PATH)
    
    # Get all pharmacies
    pharmacies = {}
    for name, lat, lon in conn.execute("SELECT name, latitude, longitude FROM pharmacies"):
        pharmacies[name] = (lat, lon)
    
    # Get INVALID Foodworks opportunities (still at fallback coords or marked INVALID)
    invalids = conn.execute("""
        SELECT id, region, nearest_pharmacy_name, nearest_pharmacy_km, evidence
        FROM opportunities 
        WHERE poi_name = 'Foodworks' AND verification = 'INVALID'
        ORDER BY id
    """).fetchall()
    
    print(f"Found {len(invalids)} INVALID Foodworks entries to retry")
    
    fixed = 0
    still_invalid = 0
    
    for opp_id, region, pharm_name, pharm_km, evidence in invalids:
        pharm_coords = pharmacies.get(pharm_name)
        if not pharm_coords:
            print(f"  ID {opp_id}: Can't find pharmacy '{pharm_name}', skipping")
            still_invalid += 1
            continue
        
        p_lat, p_lon = pharm_coords
        
        # Get suburb name from pharmacy
        display, addr = nominatim_reverse(p_lat, p_lon)
        time.sleep(1.1)
        
        suburb = addr.get('suburb') or addr.get('town') or addr.get('city') or addr.get('village', '')
        state = addr.get('state', '')
        
        if not suburb:
            print(f"  ID {opp_id}: Can't determine suburb for pharmacy '{pharm_name}', skipping")
            still_invalid += 1
            continue
        
        # Search for Foodworks in that suburb
        search_q = f"Foodworks, {suburb}, {state}"
        results = nominatim_search(search_q, p_lat, p_lon)
        time.sleep(1.1)
        
        best_result = None
        best_dist = float('inf')
        
        for r in results:
            rlat, rlon = float(r['lat']), float(r['lon'])
            d = haversine(rlat, rlon, p_lat, p_lon)
            if d < best_dist:
                best_dist = d
                best_result = r
        
        if best_result and best_dist < 5:
            new_lat = float(best_result['lat'])
            new_lon = float(best_result['lon'])
            
            # Calculate actual nearest pharmacy from new coords
            actual_pharm, actual_dist = get_nearest_pharmacy(conn, new_lat, new_lon)
            
            print(f"  ID {opp_id} ({region}): Found Foodworks in {suburb} at ({new_lat:.6f}, {new_lon:.6f}), {best_dist:.2f}km from {pharm_name}")
            print(f"    Nearest pharmacy: {actual_pharm} ({actual_dist:.2f} km)")
            
            conn.execute("""UPDATE opportunities 
                           SET latitude = ?, longitude = ?, 
                               nearest_pharmacy_name = ?, nearest_pharmacy_km = ?,
                               verification = 'UNVERIFIED', verification_notes = 'Re-geocoded via Nominatim'
                           WHERE id = ?""", (new_lat, new_lon, actual_pharm, actual_dist, opp_id))
            fixed += 1
        else:
            # Try broader search without bounding box
            results2 = nominatim_search(f"Foodworks {suburb}", limit=3)
            time.sleep(1.1)
            
            best_result2 = None
            best_dist2 = float('inf')
            for r in results2:
                rlat, rlon = float(r['lat']), float(r['lon'])
                d = haversine(rlat, rlon, p_lat, p_lon)
                if d < best_dist2:
                    best_dist2 = d
                    best_result2 = r
            
            if best_result2 and best_dist2 < 5:
                new_lat = float(best_result2['lat'])
                new_lon = float(best_result2['lon'])
                actual_pharm, actual_dist = get_nearest_pharmacy(conn, new_lat, new_lon)
                
                print(f"  ID {opp_id} ({region}): Found Foodworks near {suburb} at ({new_lat:.6f}, {new_lon:.6f})")
                print(f"    Nearest pharmacy: {actual_pharm} ({actual_dist:.2f} km)")
                
                conn.execute("""UPDATE opportunities 
                               SET latitude = ?, longitude = ?,
                                   nearest_pharmacy_name = ?, nearest_pharmacy_km = ?,
                                   verification = 'UNVERIFIED', verification_notes = 'Re-geocoded via Nominatim (broad search)'
                               WHERE id = ?""", (new_lat, new_lon, actual_pharm, actual_dist, opp_id))
                fixed += 1
            else:
                print(f"  ID {opp_id} ({region}): No Foodworks found near '{pharm_name}' in {suburb} (best: {best_dist2:.1f}km), keeping INVALID")
                still_invalid += 1
    
    conn.commit()
    print(f"\nDone: {fixed} fixed, {still_invalid} still INVALID")
    conn.close()

if __name__ == '__main__':
    main()
