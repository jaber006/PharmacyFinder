"""Sync CSV opportunity coordinates from the database (source of truth after Google Maps verification)."""
import sqlite3, csv, os, sys, io
from math import radians, cos, sin, asin, sqrt

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace', line_buffering=True)

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'pharmacy_finder.db')
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')
STATES = ['ACT', 'NSW', 'NT', 'QLD', 'SA', 'TAS', 'VIC', 'WA']

def haversine(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    a = sin((lat2-lat1)/2)**2 + cos(lat1)*cos(lat2)*sin((lon2-lon1)/2)**2
    return 2 * 6371000 * asin(sqrt(a))

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

# Build lookup from DB opportunities (source of truth after gmaps verification)
c.execute("SELECT poi_name, latitude, longitude FROM opportunities")
db_coords = {}
for name, lat, lng in c.fetchall():
    db_coords[name] = (lat, lng)

print(f"DB has {len(db_coords)} opportunities")

# Also get medical centres, supermarkets etc for POI coords
c.execute("SELECT name, latitude, longitude FROM medical_centres")
mc_coords = {r[0]: (r[1], r[2]) for r in c.fetchall()}

total_fixed = 0

for state in STATES:
    fp = os.path.join(OUTPUT_DIR, f'population_ranked_{state}.csv')
    if not os.path.exists(fp):
        continue
    
    with open(fp, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)
    
    fixed = 0
    for row in rows:
        poi_name = row.get('POI Name', '')
        csv_lat = float(row.get('Latitude', 0) or 0)
        csv_lng = float(row.get('Longitude', 0) or 0)
        
        # Check DB opportunities first
        if poi_name in db_coords:
            db_lat, db_lng = db_coords[poi_name]
            if abs(csv_lat - db_lat) > 0.0001 or abs(csv_lng - db_lng) > 0.0001:
                dist = haversine(csv_lat, csv_lng, db_lat, db_lng)
                if dist > 50:  # Only fix if >50m difference
                    row['Latitude'] = str(db_lat)
                    row['Longitude'] = str(db_lng)
                    fixed += 1
                    if fixed <= 5:
                        print(f"  [{state}] {poi_name}: {dist:.0f}m off -> ({db_lat:.6f}, {db_lng:.6f})")
    
    if fixed > 0:
        with open(fp, 'w', encoding='utf-8', newline='') as f:
            w = csv.DictWriter(f, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(rows)
        if fixed > 5:
            print(f"  [{state}] ... and {fixed - 5} more")
        print(f"  [{state}] Fixed {fixed} entries")
        total_fixed += fixed

print(f"\nTotal CSV entries synced: {total_fixed}")
conn.close()
