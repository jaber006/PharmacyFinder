import sqlite3, math

def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))

conn = sqlite3.connect('pharmacy_finder.db')
cur = conn.cursor()

# Get TAS Family Medical Centre
cur.execute("SELECT name, address, latitude, longitude FROM medical_centres WHERE name LIKE '%Family%' AND state = 'TAS'")
mc = cur.fetchall()
print("=== TAS FAMILY MEDICAL CENTRE IN DB ===")
for r in mc:
    print(f"  {r[0]} | {r[1]} | ({r[2]}, {r[3]})")
    mc_lat, mc_lon = r[2], r[3]

if not mc:
    # Try broader search
    cur.execute("SELECT name, address, latitude, longitude, state FROM medical_centres")
    print("All medical centres:")
    for r in cur.fetchall():
        print(f"  {r[0]} | {r[1]} | ({r[2]}, {r[3]}) | {r[4]}")
    mc_lat, mc_lon = -41.0508, 145.9066  # approx Burnie

print()
print("=== PHARMACIES NEAR BURNIE WITH DISTANCES ===")
cur.execute("SELECT name, address, latitude, longitude FROM pharmacies WHERE latitude BETWEEN -41.08 AND -41.03 AND longitude BETWEEN 145.86 AND 145.93")
for r in cur.fetchall():
    dist = haversine(mc_lat, mc_lon, r[2], r[3])
    print(f"  {dist*1000:.0f}m - {r[0]} | {r[1]} | ({r[2]:.5f}, {r[3]:.5f})")

# Also check Complete Care specifically
print()
cur.execute("SELECT name, address, latitude, longitude FROM pharmacies WHERE LOWER(name) LIKE '%complete care%'")
print("=== COMPLETE CARE PHARMACY ===")
for r in cur.fetchall():
    dist = haversine(mc_lat, mc_lon, r[2], r[3])
    print(f"  {r[0]} | {r[1]} | ({r[2]:.5f}, {r[3]:.5f}) | {dist*1000:.0f}m from medical centre")

conn.close()
