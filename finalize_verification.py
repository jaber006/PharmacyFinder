"""
Finalize verification results and update database.
Uses Google Maps browser verification for first 100 high-priority opportunities,
keeps remaining items with their original verdicts (not enough data to invalidate).
"""
import json
import sqlite3
import math
from datetime import datetime
import sys
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)

WORKDIR = r'C:\Users\MJ\Documents\GitHub\PharmacyFinder'

def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat, dlon = math.radians(lat2-lat1), math.radians(lon2-lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.asin(min(1, math.sqrt(a)))

# Load current verification results (from Google Maps browser)
with open(f'{WORKDIR}/output/verification_results.json', 'r', encoding='utf-8') as f:
    gmaps_results = json.load(f)

print(f"Google Maps verification results: {len(gmaps_results)}")
print(f"  Invalid: {sum(1 for r in gmaps_results if not r.get('still_valid', True))}")

# Collect all missing pharmacies from verified results
all_missing = {}
for r in gmaps_results:
    for mp in r.get('missing_pharmacies', []):
        key = f"{mp['lat']:.5f},{mp['lng']:.5f}"
        if key not in all_missing:
            all_missing[key] = mp

print(f"Unique missing pharmacies to add: {len(all_missing)}")

# Insert into database
conn = sqlite3.connect(f'{WORKDIR}/pharmacy_finder.db')

inserted = 0
for key, mp in all_missing.items():
    # Check if already exists
    existing = conn.execute(
        "SELECT latitude, longitude FROM pharmacies WHERE latitude BETWEEN ? AND ? AND longitude BETWEEN ? AND ?",
        (mp['lat']-0.004, mp['lat']+0.004, mp['lng']-0.004, mp['lng']+0.004)
    ).fetchall()
    
    if not any(haversine(mp['lat'], mp['lng'], e[0], e[1]) < 0.3 for e in existing if e[0] and e[1]):
        conn.execute(
            "INSERT INTO pharmacies (name, address, latitude, longitude, source, date_scraped, suburb, state, postcode) "
            "VALUES (?,?,?,?,?,?,'','','')",
            (mp['name'], f"({mp['lat']:.6f}, {mp['lng']:.6f})", mp['lat'], mp['lng'], 
             'google_maps_browser', datetime.now().isoformat())
        )
        inserted += 1

conn.commit()
new_total = conn.execute('SELECT COUNT(*) FROM pharmacies').fetchone()[0]
print(f"Inserted {inserted} new pharmacies")
print(f"Total pharmacies in DB: {new_total}")

# Print summary of invalidated opportunities
invalid_list = [r for r in gmaps_results if not r.get('still_valid', True)]
invalid_list.sort(key=lambda x: -(x.get('pop_10km') or 0))

print("\n" + "="*70)
print("INVALIDATED OPPORTUNITIES (from Google Maps verification)")
print("="*70)
for r in invalid_list:
    print(f"  [{r['id']}] {r['name']} ({r['state']})")
    print(f"       pop_10km: {r.get('pop_10km', 0):,}")
    print(f"       {r['verdict_change']}")

# Update final count
total_verified = len(gmaps_results)
valid = sum(1 for r in gmaps_results if r.get('still_valid', True))
invalid = total_verified - valid

print("\n" + "="*70)
print("SUMMARY")
print("="*70)
print(f"Verified: {total_verified} of 385 opportunities")
print(f"  Valid: {valid}")
print(f"  Invalid: {invalid}")
print(f"  Not yet verified: {385 - total_verified} (assumed valid until verified)")
print(f"\nTotal estimated valid PASS+LIKELY: ~{valid + 285} (conservative)")
print(f"Database updated with {inserted} new pharmacies")

conn.close()
