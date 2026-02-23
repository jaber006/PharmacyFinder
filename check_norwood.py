import sqlite3, sys, io
from math import radians, cos, sin, asin, sqrt

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

def haversine(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    a = sin((lat2-lat1)/2)**2 + cos(lat1)*cos(lat2)*sin((lon2-lon1)/2)**2
    return 2 * 6371000 * asin(sqrt(a))

# 22 Norwood Avenue, Norwood TAS 7250
# Norwood is in south Launceston. Let me use approximate coords.
# Norwood Ave is near -41.472, 147.147 based on OSM data
PROP_LAT = -41.4557
PROP_LNG = 147.1728

DB = r'C:\Users\MJ\Documents\GitHub\PharmacyFinder\pharmacy_finder.db'
conn = sqlite3.connect(DB)
c = conn.cursor()

# 1. Distance to nearest pharmacies
print("=== NEAREST PHARMACIES ===")
c.execute("SELECT name, address, latitude, longitude FROM pharmacies WHERE latitude IS NOT NULL")
pharmacies = []
for name, addr, lat, lng in c.fetchall():
    d = haversine(PROP_LAT, PROP_LNG, lat, lng)
    pharmacies.append((d, name, addr, lat, lng))
pharmacies.sort()
for d, name, addr, lat, lng in pharmacies[:8]:
    print(f"  {d/1000:.2f}km - {name} ({addr})")

nearest_km = pharmacies[0][0] / 1000
print(f"\nItem 130 requires >= 1.5km. Nearest is {nearest_km:.2f}km. {'PASS' if nearest_km >= 1.5 else 'FAIL'}")

# 2. GPs within 500m
print("\n=== GPS/MEDICAL CENTRES WITHIN 500m ===")
c.execute("SELECT name, address, latitude, longitude FROM medical_centres WHERE latitude IS NOT NULL")
for name, addr, lat, lng in c.fetchall():
    d = haversine(PROP_LAT, PROP_LNG, lat, lng)
    if d <= 500:
        print(f"  {d:.0f}m - {name} ({addr})")

c.execute("SELECT name, address, latitude, longitude FROM gps WHERE latitude IS NOT NULL")
for name, addr, lat, lng in c.fetchall():
    d = haversine(PROP_LAT, PROP_LNG, lat, lng)
    if d <= 500:
        print(f"  {d:.0f}m - {name} ({addr})")

# Also check within 1km for context
print("\n=== GPS/MEDICAL CENTRES WITHIN 1km ===")
c.execute("SELECT name, address, latitude, longitude FROM medical_centres WHERE latitude IS NOT NULL")
for name, addr, lat, lng in c.fetchall():
    d = haversine(PROP_LAT, PROP_LNG, lat, lng)
    if d <= 1000:
        print(f"  {d:.0f}m - {name}")

c.execute("SELECT name, address, latitude, longitude FROM gps WHERE latitude IS NOT NULL")
for name, addr, lat, lng in c.fetchall():
    d = haversine(PROP_LAT, PROP_LNG, lat, lng)
    if d <= 1000:
        print(f"  {d:.0f}m - {name}")

# 3. Supermarkets within 500m
print("\n=== SUPERMARKETS WITHIN 500m ===")
c.execute("SELECT name, address, latitude, longitude, floor_area_sqm FROM supermarkets WHERE latitude IS NOT NULL")
for name, addr, lat, lng, area in c.fetchall():
    d = haversine(PROP_LAT, PROP_LNG, lat, lng)
    if d <= 500:
        print(f"  {d:.0f}m - {name} ({addr}) - {area}sqm")

# Also 1km
print("\n=== SUPERMARKETS WITHIN 1km ===")
c.execute("SELECT name, address, latitude, longitude, floor_area_sqm FROM supermarkets WHERE latitude IS NOT NULL")
for name, addr, lat, lng, area in c.fetchall():
    d = haversine(PROP_LAT, PROP_LNG, lat, lng)
    if d <= 1000:
        print(f"  {d:.0f}m - {name} - {area}sqm")

conn.close()
