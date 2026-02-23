"""
Migrate all CSV enrichment data into the DB opportunities table.
After this, the DB is the single source of truth and CSVs are just exports.
"""
import sqlite3, csv, os, sys, io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace', line_buffering=True)

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'pharmacy_finder.db')
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')
STATES = ['ACT', 'NSW', 'NT', 'QLD', 'SA', 'TAS', 'VIC', 'WA']

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

# Step 1: Add missing columns to opportunities table
new_columns = [
    ('verification', 'TEXT', 'UNVERIFIED'),
    ('pop_5km', 'INTEGER', '0'),
    ('pop_10km', 'INTEGER', '0'),
    ('pop_15km', 'INTEGER', '0'),
    ('nearest_town', 'TEXT', ''),
    ('opp_score', 'REAL', '0'),
    ('pharmacy_5km', 'INTEGER', '0'),
    ('pharmacy_10km', 'INTEGER', '0'),
    ('pharmacy_15km', 'INTEGER', '0'),
    ('chain_count', 'INTEGER', '0'),
    ('independent_count', 'INTEGER', '0'),
    ('competition_score', 'REAL', '0'),
    ('composite_score', 'REAL', '0'),
    ('nearest_competitors', 'TEXT', ''),
    ('growth_indicator', 'TEXT', ''),
    ('growth_details', 'TEXT', ''),
]

print("Step 1: Adding columns to opportunities table...")
existing = [r[1] for r in c.execute("PRAGMA table_info(opportunities)").fetchall()]
for col, type, default in new_columns:
    if col not in existing:
        c.execute(f"ALTER TABLE opportunities ADD COLUMN {col} {type} DEFAULT '{default}'")
        print(f"  Added: {col} ({type})")
    else:
        print(f"  Exists: {col}")

conn.commit()

# Step 2: Load CSV data and update DB rows
print("\nStep 2: Importing CSV enrichment data into DB...")

total_updated = 0
total_inserted = 0

for state in STATES:
    fp = os.path.join(OUTPUT_DIR, f'population_ranked_{state}.csv')
    if not os.path.exists(fp):
        continue
    
    with open(fp, 'r', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))
    
    updated = 0
    inserted = 0
    
    for row in rows:
        poi_name = row.get('POI Name', '')
        lat = float(row.get('Latitude', 0) or 0)
        lng = float(row.get('Longitude', 0) or 0)
        
        if not poi_name or (lat == 0 and lng == 0):
            continue
        
        # Try to find matching DB row by name + region
        c.execute("SELECT id FROM opportunities WHERE poi_name = ? AND region = ?",
                  (poi_name, row.get('Region', state)))
        existing_row = c.fetchone()
        
        if not existing_row:
            # Try just by name
            c.execute("SELECT id FROM opportunities WHERE poi_name = ? AND ABS(latitude - ?) < 0.01 AND ABS(longitude - ?) < 0.01",
                      (poi_name, lat, lng))
            existing_row = c.fetchone()
        
        values = {
            'verification': row.get('Verification', 'UNVERIFIED'),
            'pop_5km': int(float(row.get('Pop 5km', 0) or 0)),
            'pop_10km': int(float(row.get('Pop 10km', 0) or 0)),
            'pop_15km': int(float(row.get('Pop 15km', 0) or 0)),
            'nearest_town': row.get('Nearest Town', ''),
            'opp_score': float(row.get('Opportunity Score', 0) or 0),
            'pharmacy_5km': int(float(row.get('Pharmacy Count 5km', 0) or row.get('Pharmacies 5km', 0) or 0)),
            'pharmacy_10km': int(float(row.get('Pharmacy Count 10km', 0) or row.get('Pharmacies 10km', 0) or 0)),
            'pharmacy_15km': int(float(row.get('Pharmacy Count 15km', 0) or row.get('Pharmacies 15km', 0) or 0)),
            'chain_count': int(float(row.get('Chain Count', 0) or row.get('Chains 5km', 0) or 0)),
            'independent_count': int(float(row.get('Independent Count', 0) or row.get('Independents 5km', 0) or 0)),
            'competition_score': float(row.get('Competition Score', 0) or 0),
            'composite_score': float(row.get('Composite Score', 0) or row.get('Opportunity Score', 0) or 0),
            'nearest_competitors': row.get('Nearest Competitors', ''),
            'growth_indicator': row.get('Growth Indicator', ''),
            'growth_details': row.get('Growth Details', ''),
        }
        
        if existing_row:
            set_clause = ', '.join(f"{k} = ?" for k in values.keys())
            c.execute(f"UPDATE opportunities SET {set_clause} WHERE id = ?",
                      list(values.values()) + [existing_row[0]])
            updated += 1
        else:
            # Insert new opportunity
            c.execute("""INSERT INTO opportunities 
                        (poi_name, poi_type, latitude, longitude, address, qualifying_rules, evidence, 
                         confidence, nearest_pharmacy_km, nearest_pharmacy_name, region,
                         verification, pop_5km, pop_10km, pop_15km, nearest_town, opp_score,
                         pharmacy_5km, pharmacy_10km, pharmacy_15km, chain_count, independent_count,
                         competition_score, composite_score, nearest_competitors, growth_indicator, growth_details)
                        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                      (poi_name, row.get('POI Type', ''), lat, lng, row.get('Address', ''),
                       row.get('Qualifying Rules', ''), row.get('Evidence', ''),
                       row.get('Confidence', ''), float(row.get('Nearest Pharmacy (km)', 0) or 0),
                       row.get('Nearest Pharmacy Name', ''), row.get('Region', state),
                       *values.values()))
            inserted += 1
    
    print(f"  {state}: {updated} updated, {inserted} inserted (from {len(rows)} CSV rows)")
    total_updated += updated
    total_inserted += inserted

conn.commit()
print(f"\nTotal: {total_updated} updated, {total_inserted} inserted")

# Step 3: Verify
c.execute("SELECT COUNT(*) FROM opportunities")
print(f"\nDB opportunities: {c.fetchone()[0]}")
c.execute("SELECT region, COUNT(*) FROM opportunities GROUP BY region ORDER BY region")
for region, count in c.fetchall():
    print(f"  {region}: {count}")

conn.close()
print("\nDone! DB is now the single source of truth.")
