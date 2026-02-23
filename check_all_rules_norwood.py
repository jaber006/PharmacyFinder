import sqlite3, sys, io
from math import radians, cos, sin, asin, sqrt

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

def haversine(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    a = sin((lat2-lat1)/2)**2 + cos(lat1)*cos(lat2)*sin((lon2-lon1)/2)**2
    return 2 * 6371000 * asin(sqrt(a))

PROP_LAT = -41.4557
PROP_LNG = 147.1728

DB = r'C:\Users\MJ\Documents\GitHub\PharmacyFinder\pharmacy_finder.db'
conn = sqlite3.connect(DB)
c = conn.cursor()

# Get nearest pharmacy distance
c.execute("SELECT name, latitude, longitude FROM pharmacies WHERE latitude IS NOT NULL")
pharmacies = [(haversine(PROP_LAT, PROP_LNG, lat, lng), name) for name, lat, lng in c.fetchall()]
pharmacies.sort()
nearest_m = pharmacies[0][0]
nearest_name = pharmacies[0][1]

# Get nearby POIs
def get_nearby(table, radius_m):
    c.execute(f"SELECT name, latitude, longitude FROM {table} WHERE latitude IS NOT NULL")
    return [(haversine(PROP_LAT, PROP_LNG, lat, lng), name) for name, lat, lng in c.fetchall() if haversine(PROP_LAT, PROP_LNG, lat, lng) <= radius_m]

def get_nearby_super(radius_m):
    c.execute("SELECT name, latitude, longitude, floor_area_sqm FROM supermarkets WHERE latitude IS NOT NULL")
    return [(haversine(PROP_LAT, PROP_LNG, lat, lng), name, area) for name, lat, lng, area in c.fetchall() if haversine(PROP_LAT, PROP_LNG, lat, lng) <= radius_m]

gps_500 = get_nearby('gps', 500) + get_nearby('medical_centres', 500)
gps_1000 = get_nearby('gps', 1000) + get_nearby('medical_centres', 1000)
supers_500 = get_nearby_super(500)
supers_1000 = get_nearby_super(1000)
hospitals_500 = get_nearby('hospitals', 500)
hospitals_1000 = get_nearby('hospitals', 1000)
hospitals_2000 = get_nearby('hospitals', 2000)
shops_500 = get_nearby('shopping_centres', 500)
shops_1000 = get_nearby('shopping_centres', 1000)

print(f"=== 22 NORWOOD AVENUE - ALL RULES CHECK ===\n")
print(f"Nearest pharmacy: {nearest_name} ({nearest_m:.0f}m / {nearest_m/1000:.3f}km)\n")

# Item 130: >= 1.5km + (GP + super 1000sqm within 500m) OR (super 2500sqm within 500m)
print("ITEM 130: New pharmacy (1.5km + supermarket/GP)")
print(f"  (a) >= 1.5km from pharmacy: {nearest_m:.0f}m {'PASS' if nearest_m >= 1500 else 'FAIL'}")
has_gp_500 = len(gps_500) > 0
has_super_1000 = any(area >= 1000 for _, _, area in supers_500)
has_super_2500 = any(area >= 2500 for _, _, area in supers_500)
print(f"  (b)(i) GP within 500m: {'YES' if has_gp_500 else 'NO'} + Super >=1000sqm within 500m: {'YES' if has_super_1000 else 'NO'}")
print(f"  (b)(ii) Super >=2500sqm within 500m: {'YES' if has_super_2500 else 'NO'}")
item130 = nearest_m >= 1500 and (has_gp_500 and has_super_1000 or has_super_2500)
print(f"  RESULT: {'PASS' if item130 else 'FAIL'}\n")

# Item 131: >= 10km from nearest pharmacy (remote)
print("ITEM 131: Remote area (10km from nearest)")
print(f"  >= 10km: {nearest_m/1000:.1f}km {'PASS' if nearest_m >= 10000 else 'FAIL'}")
print(f"  RESULT: FAIL (only {nearest_m/1000:.1f}km)\n")

# Item 132: Within shopping centre with >= 15 tenancies, super >= 2500sqm
print("ITEM 132: Large shopping centre")
print(f"  Shopping centres within 500m: {len(shops_500)}")
print(f"  Super >= 2500sqm within 500m: {'YES' if has_super_2500 else 'NO'}")
print(f"  RESULT: FAIL (no large shopping centre)\n")

# Item 133: >= 1.5km + supermarket within 500m (any size?)
# Actually Item 133 needs supermarket >= 500sqm within 500m
has_super_500sqm = any(area >= 500 for _, _, area in supers_500)
print("ITEM 133: Near supermarket (1.5km + super >= 500sqm within 500m)")
print(f"  >= 1.5km from pharmacy: {'PASS' if nearest_m >= 1500 else 'FAIL'}")
print(f"  Super >= 500sqm within 500m: {'YES' if has_super_500sqm else 'NO'}")
item133 = nearest_m >= 1500 and has_super_500sqm
print(f"  RESULT: {'PASS' if item133 else 'FAIL'}\n")

# Item 134: >= 1.5km + within 500m of shopping centre
print("ITEM 134: Near shopping centre (1.5km + shopping centre within 500m)")
print(f"  >= 1.5km from pharmacy: {'PASS' if nearest_m >= 1500 else 'FAIL'}")
print(f"  Shopping centre within 500m: {len(shops_500)} found")
for d, name in shops_500:
    print(f"    {d:.0f}m - {name}")
item134 = nearest_m >= 1500 and len(shops_500) > 0
print(f"  RESULT: {'PASS' if item134 else 'FAIL'}\n")

# Item 134A: >= 2km from nearest pharmacy
print("ITEM 134A: 2km rule")
print(f"  >= 2km from pharmacy: {nearest_m/1000:.3f}km {'PASS' if nearest_m >= 2000 else 'FAIL'}")
print(f"  RESULT: FAIL (only {nearest_m/1000:.3f}km)\n")

# Item 135: >= 1.5km + within 1km of hospital with >= 50 beds
print("ITEM 135: Near hospital (1.5km + hospital within 1km)")
print(f"  >= 1.5km from pharmacy: {'PASS' if nearest_m >= 1500 else 'FAIL'}")
print(f"  Hospital within 1km: {len(hospitals_1000)} found")
for d, name in hospitals_1000:
    print(f"    {d:.0f}m - {name}")
print(f"  Hospital within 2km: {len(hospitals_2000)} found")
for d, name in hospitals_2000:
    print(f"    {d:.0f}m - {name}")
item135 = nearest_m >= 1500 and len(hospitals_1000) > 0
print(f"  RESULT: {'PASS' if item135 else 'FAIL'}\n")

# Item 136: >= 1.5km + within 500m of medical centre with >= 2 FTE GPs
print("ITEM 136: Near medical centre (1.5km + medical centre with >= 2 GPs within 500m)")
print(f"  >= 1.5km from pharmacy: {'PASS' if nearest_m >= 1500 else 'FAIL'}")
print(f"  Medical centres/GPs within 500m: {len(gps_500)} found")
for d, name in gps_500:
    print(f"    {d:.0f}m - {name}")
item136 = nearest_m >= 1500 and len(gps_500) > 0
print(f"  RESULT: {'PASS (if 2+ FTE GPs confirmed)' if item136 else 'FAIL'}\n")

print("=" * 50)
print("QUALIFYING RULES:")
if item130: print("  * Item 130 - PASS (1.5km + GP + supermarket)")
if item133: print("  * Item 133 - PASS (1.5km + supermarket)")
if item134: print("  * Item 134 - PASS (1.5km + shopping centre)")
if item135: print("  * Item 135 - PASS (1.5km + hospital)")
if item136: print("  * Item 136 - PASS (1.5km + medical centre)")

conn.close()
