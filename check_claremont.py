import os
import sqlite3, sys, io
from math import radians, cos, sin, asin, sqrt

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

def haversine(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    a = sin((lat2-lat1)/2)**2 + cos(lat1)*cos(lat2)*sin((lon2-lon1)/2)**2
    return 2 * 6371000 * asin(sqrt(a))

conn = sqlite3.connect(os.path.join(BASE_DIR, 'pharmacy_finder.db'))
c = conn.cursor()

# Where is Claremont Village Shopping Centre in our DB?
c.execute("SELECT poi_name, latitude, longitude, nearest_pharmacy_name, nearest_pharmacy_km, address FROM opportunities WHERE poi_name LIKE '%Claremont%'")
for r in c.fetchall():
    print(f"Opportunity: {r[0]}")
    print(f"  Coords: ({r[1]}, {r[2]})")
    print(f"  Nearest pharmacy: {r[3]} ({r[4]:.1f}km)")
    print(f"  Address: {r[5]}")
    print()

# What pharmacies do we have near Claremont?
print("=== PHARMACIES WITH 'CLAREMONT' ===")
c.execute("SELECT name, address, latitude, longitude FROM pharmacies WHERE name LIKE '%Claremont%' OR address LIKE '%Claremont%'")
for r in c.fetchall():
    print(f"  {r[0]} | {r[1]} | ({r[2]}, {r[3]})")

print("\n=== PHARMACIES WITHIN 3km OF CLAREMONT VILLAGE SC ===")
c.execute("SELECT latitude, longitude FROM opportunities WHERE poi_name LIKE '%Claremont Village%'")
row = c.fetchone()
if row:
    sc_lat, sc_lon = row
    c.execute("SELECT name, address, latitude, longitude FROM pharmacies WHERE latitude IS NOT NULL")
    nearby = []
    for name, addr, lat, lng in c.fetchall():
        d = haversine(sc_lat, sc_lon, lat, lng)
        if d <= 3000:
            nearby.append((d, name, addr))
    nearby.sort()
    for d, name, addr in nearby:
        print(f"  {d:.0f}m ({d/1000:.2f}km) - {name} | {addr}")

conn.close()
