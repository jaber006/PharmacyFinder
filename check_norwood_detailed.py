import sqlite3, sys, io
from math import radians, cos, sin, asin, sqrt

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

def haversine(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    a = sin((lat2-lat1)/2)**2 + cos(lat1)*cos(lat2)*sin((lon2-lon1)/2)**2
    return 2 * 6371000 * asin(sqrt(a))

# 22 Norwood Avenue exact coords from OSM
PROP_LAT = -41.4557
PROP_LNG = 147.1728

DB = r'C:\Users\MJ\Documents\GitHub\PharmacyFinder\pharmacy_finder.db'
conn = sqlite3.connect(DB)
c = conn.cursor()

# Nearest pharmacies - show exact distances
print("=== PHARMACY DISTANCES (closest 5) ===")
c.execute("SELECT name, address, latitude, longitude FROM pharmacies WHERE latitude IS NOT NULL")
pharmacies = []
for name, addr, lat, lng in c.fetchall():
    d = haversine(PROP_LAT, PROP_LNG, lat, lng)
    pharmacies.append((d, name, addr, lat, lng))
pharmacies.sort()
for d, name, addr, lat, lng in pharmacies[:5]:
    print(f"  {d:.1f}m ({d/1000:.3f}km) - {name}")
    print(f"    Address: {addr}")
    print(f"    Coords: ({lat:.6f}, {lng:.6f})")
    print()

print(f"ITEM 130 DISTANCE CHECK: Nearest = {pharmacies[0][0]:.1f}m = {pharmacies[0][0]/1000:.3f}km")
print(f"  Required: >= 1.500km")
print(f"  Margin: {pharmacies[0][0]/1000 - 1.5:.3f}km = {pharmacies[0][0] - 1500:.0f}m")
print(f"  {'PASS - but TIGHT' if pharmacies[0][0] >= 1500 else 'FAIL'}")

# GP/Medical within 500m
print("\n=== MEDICAL WITHIN 500m ===")
found_gp = False
for table in ['medical_centres', 'gps']:
    c.execute(f"SELECT name, address, latitude, longitude FROM {table} WHERE latitude IS NOT NULL")
    for name, addr, lat, lng in c.fetchall():
        d = haversine(PROP_LAT, PROP_LNG, lat, lng)
        if d <= 600:
            print(f"  {d:.0f}m - {name} [{table}]")
            print(f"    Coords: ({lat:.6f}, {lng:.6f})")
            if d <= 500:
                found_gp = True

# Supermarket within 500m
print("\n=== SUPERMARKET WITHIN 500m ===")
found_super = False
c.execute("SELECT name, address, latitude, longitude, floor_area_sqm FROM supermarkets WHERE latitude IS NOT NULL")
for name, addr, lat, lng, area in c.fetchall():
    d = haversine(PROP_LAT, PROP_LNG, lat, lng)
    if d <= 600:
        print(f"  {d:.0f}m - {name} | Floor area: {area}sqm (ESTIMATED)")
        print(f"    Coords: ({lat:.6f}, {lng:.6f})")
        if d <= 500:
            found_super = True

print(f"\n=== ITEM 130 SUMMARY ===")
print(f"(a) >= 1.5km from nearest pharmacy: {pharmacies[0][0]/1000:.3f}km - {'PASS' if pharmacies[0][0] >= 1500 else 'FAIL'} (margin: {pharmacies[0][0] - 1500:.0f}m)")
print(f"(b)(i) GP within 500m: {'FOUND' if found_gp else 'NOT FOUND'}")
print(f"(b)(i) Supermarket >= 1000sqm within 500m: {'FOUND (but floor area is ESTIMATED)' if found_super else 'NOT FOUND'}")

conn.close()
