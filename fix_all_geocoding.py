"""
Fix medical centre geocoding by re-geocoding ALL entries using their street address.

Problem: Many medical centres have hardcoded approximate/hallucinated coordinates.
Solution: Re-geocode every address through Nominatim, compare with existing coords,
and update any that are significantly off (>200m).
"""

import sqlite3
import sys
import io
import time
import json
from math import radians, cos, sin, asin, sqrt
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace', line_buffering=True)

import os
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'pharmacy_finder.db')
geolocator = Nominatim(user_agent="pharmacy-finder-geocode-fix/1.0")

def haversine(lat1, lon1, lat2, lon2):
    """Distance in meters between two points."""
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    return 2 * 6371000 * asin(sqrt(a))

def geocode_address(address):
    """Geocode a street address using Nominatim."""
    time.sleep(1.1)  # Rate limit
    
    # Try the full address first
    try:
        location = geolocator.geocode(address, timeout=10, country_codes='au')
        if location:
            return location.latitude, location.longitude, location.address
    except (GeocoderTimedOut, GeocoderServiceError) as e:
        print(f"    Geocoding error: {e}")
        time.sleep(2)
    
    # If full address fails, try simplifying (remove unit/shop numbers)
    import re
    simplified = re.sub(r'^(Shop|Unit|Suite|Level|Ground|Floor)\s*\d*[A-Za-z]?\s*[,/]\s*', '', address, flags=re.IGNORECASE)
    simplified = re.sub(r'^\d+/\d+', lambda m: m.group().split('/')[1], simplified)  # "1/3" -> "3"
    
    if simplified != address:
        try:
            time.sleep(1.1)
            location = geolocator.geocode(simplified, timeout=10, country_codes='au')
            if location:
                return location.latitude, location.longitude, location.address
        except (GeocoderTimedOut, GeocoderServiceError):
            time.sleep(2)
    
    return None, None, None

def main():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Get all medical centres
    c.execute("SELECT id, name, address, latitude, longitude, state FROM medical_centres ORDER BY state, name")
    centres = c.fetchall()
    
    print(f"{'='*70}")
    print(f"RE-GEOCODING {len(centres)} MEDICAL CENTRES")
    print(f"{'='*70}\n")
    
    fixed = 0
    failed = 0
    unchanged = 0
    results = []
    
    for id, name, address, old_lat, old_lng, state in centres:
        if not address:
            print(f"  SKIP (no address): {name}")
            failed += 1
            continue
        
        print(f"  [{state}] {name}")
        print(f"    Address: {address}")
        print(f"    Current: ({old_lat:.6f}, {old_lng:.6f})")
        
        new_lat, new_lng, matched_address = geocode_address(address)
        
        if new_lat is None:
            print(f"    FAILED to geocode!")
            failed += 1
            results.append({'name': name, 'status': 'FAILED', 'address': address})
            continue
        
        distance = haversine(old_lat, old_lng, new_lat, new_lng)
        
        if distance > 200:  # More than 200m off
            print(f"    NEW:     ({new_lat:.6f}, {new_lng:.6f})")
            print(f"    Matched: {matched_address}")
            print(f"    OFFSET:  {distance:.0f}m *** FIXING ***")
            
            c.execute("UPDATE medical_centres SET latitude = ?, longitude = ? WHERE id = ?",
                      (new_lat, new_lng, id))
            
            # Also fix any opportunities that reference this medical centre
            c.execute("""UPDATE opportunities SET latitude = ?, longitude = ? 
                        WHERE poi_name = ? AND poi_type = 'medical_centre'""",
                      (new_lat, new_lng, name))
            
            fixed += 1
            results.append({
                'name': name, 'status': 'FIXED', 'address': address,
                'old': (old_lat, old_lng), 'new': (new_lat, new_lng),
                'offset_m': round(distance)
            })
        else:
            print(f"    OK ({distance:.0f}m offset)")
            unchanged += 1
            results.append({'name': name, 'status': 'OK', 'offset_m': round(distance)})
    
    conn.commit()
    
    print(f"\n{'='*70}")
    print(f"RESULTS")
    print(f"{'='*70}")
    print(f"Total:     {len(centres)}")
    print(f"Fixed:     {fixed} (>200m offset)")
    print(f"OK:        {unchanged} (<200m offset)")
    print(f"Failed:    {failed}")
    
    if fixed > 0:
        print(f"\n--- Fixed entries ---")
        for r in results:
            if r['status'] == 'FIXED':
                print(f"  {r['name']}: {r['offset_m']}m off → corrected")
    
    if failed > 0:
        print(f"\n--- Failed entries ---")
        for r in results:
            if r['status'] == 'FAILED':
                print(f"  {r['name']}: {r['address']}")
    
    # Save results for review
    with open('output/geocode_fix_results.json', 'w') as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\nDetailed results saved to output/geocode_fix_results.json")
    
    conn.close()

if __name__ == '__main__':
    main()
