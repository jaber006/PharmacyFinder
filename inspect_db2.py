import sqlite3
conn = sqlite3.connect('pharmacy_finder.db')
c = conn.cursor()

# Verification values
print("--- Verification values ---")
c.execute("SELECT DISTINCT verification FROM opportunities")
for r in c.fetchall():
    print(r)

# Count by verification
print("\n--- Counts by verification ---")
c.execute("SELECT verification, COUNT(*) FROM opportunities GROUP BY verification")
for r in c.fetchall():
    print(r)

# How many pharmacies near REAL Gordonvale (-17.09, 145.78)
import math
def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))

print("\n--- Pharmacies within 10km of REAL Gordonvale (-17.09, 145.78) ---")
c.execute("SELECT name, latitude, longitude FROM pharmacies")
count = 0
for name, lat, lon in c.fetchall():
    d = haversine(-17.09, 145.78, lat, lon)
    if d < 10:
        count += 1
        print(f"  {d:.1f}km - {name}")
print(f"Total: {count}")

print("\n--- Pharmacies within 10km of WRONG coords (-26.19, 152.66) ---")
c.execute("SELECT name, latitude, longitude FROM pharmacies")
count = 0
for name, lat, lon in c.fetchall():
    d = haversine(-26.19, 152.66, lat, lon)
    if d < 10:
        count += 1
        print(f"  {d:.1f}km - {name}")
print(f"Total: {count}")

# Sample a few opportunities with their nearest_town/region
print("\n--- Sample opportunities ---")
c.execute("SELECT id, nearest_town, region, latitude, longitude, pharmacy_10km, nearest_pharmacy_km, verification FROM opportunities LIMIT 20")
for r in c.fetchall():
    print(r)

conn.close()
