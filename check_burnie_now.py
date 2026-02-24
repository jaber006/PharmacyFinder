import os
import sqlite3, sys, io

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

conn = sqlite3.connect(os.path.join(BASE_DIR, 'pharmacy_finder.db'))
c = conn.cursor()

# Opportunities near Burnie (-41.05, 145.90)
print("=== OPPORTUNITIES NEAR BURNIE ===")
c.execute("""SELECT id, latitude, longitude, address, qualifying_rules, confidence, nearest_pharmacy_km, nearest_pharmacy_name, poi_name, poi_type 
    FROM opportunities 
    WHERE latitude BETWEEN -41.15 AND -40.95 
    AND longitude BETWEEN 145.80 AND 146.00""")
rows = c.fetchall()
cols = [d[0] for d in c.description]
for r in rows:
    d = dict(zip(cols, r))
    print(f"  ID:{d['id']} | {d['address']} | Rules: {d['qualifying_rules']} | Nearest: {d['nearest_pharmacy_km']:.1f}km ({d['nearest_pharmacy_name']}) | POI: {d['poi_name']} ({d['poi_type']}) | Conf: {d['confidence']}")
print(f"Total: {len(rows)}")

# All pharmacies in Burnie area
print("\n=== PHARMACIES IN BURNIE AREA ===")
c.execute("""SELECT name, address, suburb, latitude, longitude FROM pharmacies 
    WHERE latitude BETWEEN -41.15 AND -40.95 
    AND longitude BETWEEN 145.80 AND 146.00
    ORDER BY suburb""")
rows = c.fetchall()
for r in rows:
    print(f"  {r[0]} | {r[1]} | ({r[3]:.4f}, {r[4]:.4f})")
print(f"Total: {len(rows)}")

# Commercial properties
print("\n=== COMMERCIAL PROPERTIES NEAR BURNIE ===")
c.execute("""SELECT address, suburb, price, property_type, url FROM commercial_properties 
    WHERE suburb LIKE '%urnie%' OR suburb LIKE '%BURNIE%' OR suburb LIKE '%Cooee%' OR suburb LIKE '%COOEE%'""")
rows = c.fetchall()
for r in rows:
    print(f"  {r[0]} | {r[1]} | {r[2]} | {r[3]}")
print(f"Total: {len(rows)}")

# How many total TAS opportunities?
print("\n=== TAS OPPORTUNITY COUNT ===")
c.execute("""SELECT COUNT(*) FROM opportunities WHERE region = 'TAS'""")
print(f"Total TAS opportunities: {c.fetchone()[0]}")

# Show all TAS opportunities
print("\n=== ALL TAS OPPORTUNITIES ===")
c.execute("""SELECT id, address, qualifying_rules, nearest_pharmacy_km, nearest_pharmacy_name, poi_name, poi_type, confidence
    FROM opportunities WHERE region = 'TAS' ORDER BY nearest_pharmacy_km DESC""")
rows = c.fetchall()
cols = [d[0] for d in c.description]
for r in rows:
    d = dict(zip(cols, r))
    print(f"  {d['address']} | Rules: {d['qualifying_rules']} | Nearest: {d['nearest_pharmacy_km']:.1f}km ({d['nearest_pharmacy_name']}) | {d['poi_name']} ({d['poi_type']})")

conn.close()
