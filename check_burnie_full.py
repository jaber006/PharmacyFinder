import os
import sqlite3, sys, io

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

conn = sqlite3.connect(os.path.join(BASE_DIR, 'pharmacy_finder.db'))
c = conn.cursor()

# Commercial properties near Burnie
print("=== COMMERCIAL PROPERTIES NEAR BURNIE ===")
c.execute("""SELECT property_address, property_suburb, rent_display, floor_area_sqm, listing_url, suitability_score, distance_km 
    FROM commercial_properties 
    WHERE property_suburb LIKE '%urnie%' OR property_suburb LIKE '%ooee%' OR property_suburb LIKE '%omerset%'""")
rows = c.fetchall()
for r in rows:
    print(f"  {r[0]} | {r[1]} | Rent: {r[2]} | {r[3]}sqm | Score: {r[4]} | {r[5]:.1f}km | {r[6]}")
print(f"Total: {len(rows)}")

# What's the opportunity scanner logic? Check the qualifying rules
print("\n=== OPPORTUNITY SCANNER: BURNIE AREA DETAILS ===")
c.execute("""SELECT id, address, qualifying_rules, evidence, confidence, nearest_pharmacy_km, nearest_pharmacy_name, poi_name, poi_type
    FROM opportunities 
    WHERE latitude BETWEEN -41.15 AND -40.95 
    AND longitude BETWEEN 145.80 AND 146.00""")
rows = c.fetchall()
cols = [d[0] for d in c.description]
for r in rows:
    d = dict(zip(cols, r))
    print(f"\n  ID: {d['id']}")
    print(f"  Address: {d['address']}")
    print(f"  Rules: {d['qualifying_rules']}")
    print(f"  Evidence: {d['evidence']}")
    print(f"  Confidence: {d['confidence']}")
    print(f"  Nearest pharmacy: {d['nearest_pharmacy_km']:.2f}km ({d['nearest_pharmacy_name']})")
    print(f"  POI: {d['poi_name']} ({d['poi_type']})")

conn.close()
