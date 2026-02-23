"""Fix coordinates in population_ranked CSVs to match corrected database values."""
import csv
import sqlite3
import os
import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace', line_buffering=True)

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'pharmacy_finder.db')
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

# Build lookup of corrected medical centre coordinates
c.execute("SELECT name, latitude, longitude FROM medical_centres")
mc_coords = {row[0]: (row[1], row[2]) for row in c.fetchall()}

# Also get corrected opportunity coordinates  
c.execute("SELECT id, latitude, longitude FROM opportunities")
opp_coords = {str(row[0]): (row[1], row[2]) for row in c.fetchall()}

print(f"Loaded {len(mc_coords)} medical centre coords and {len(opp_coords)} opportunity coords from DB")

STATES = ['ACT', 'NSW', 'NT', 'QLD', 'SA', 'TAS', 'VIC', 'WA']
total_fixed = 0

for state in STATES:
    filepath = os.path.join(OUTPUT_DIR, f'population_ranked_{state}.csv')
    if not os.path.exists(filepath):
        continue
    
    # Read all rows
    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)
    
    fixed = 0
    for row in rows:
        poi_name = row.get('POI Name', '')
        old_lat = row.get('Latitude', '')
        old_lng = row.get('Longitude', '')
        
        # Check if this POI is a medical centre with corrected coords
        if poi_name in mc_coords:
            new_lat, new_lng = mc_coords[poi_name]
            if old_lat and old_lng:
                old_lat_f = float(old_lat)
                old_lng_f = float(old_lng)
                # Only fix if significantly different
                if abs(old_lat_f - new_lat) > 0.001 or abs(old_lng_f - new_lng) > 0.001:
                    row['Latitude'] = str(new_lat)
                    row['Longitude'] = str(new_lng)
                    fixed += 1
                    print(f"  [{state}] {poi_name}: ({old_lat}, {old_lng}) -> ({new_lat}, {new_lng})")
    
    if fixed > 0:
        # Write back
        with open(filepath, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        print(f"  {state}: Fixed {fixed} rows")
        total_fixed += fixed
    else:
        print(f"  {state}: No fixes needed")

print(f"\nTotal fixed across all CSVs: {total_fixed}")
conn.close()
