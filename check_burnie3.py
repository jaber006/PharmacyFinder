import sqlite3, math

def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))

# Correct location: 1/3 Reeves St, South Burnie TAS 7320
# Google Maps Plus Code: WWP7+RM South Burnie
# Approximate coords for South Burnie / Reeves St area
mc_lat = -41.0707  # South Burnie area
mc_lon = 145.9012  # approximate

# Let's use a more precise estimate - South Burnie is south of Burnie CBD
# Reeves St South Burnie is near Upper Burnie
# Plus code WWP7+RM decodes to approximately -41.068, 145.905

mc_lat = -41.068
mc_lon = 145.905

print(f"TAS Family Medical Centre (corrected)")
print(f"Address: 1/3 Reeves St, South Burnie TAS 7320")
print(f"Estimated coords: ({mc_lat}, {mc_lon})")
print()

conn = sqlite3.connect('pharmacy_finder.db')
cur = conn.cursor()

print("=== PHARMACIES BY DISTANCE FROM CORRECTED LOCATION ===")
cur.execute("SELECT name, address, latitude, longitude FROM pharmacies WHERE latitude BETWEEN -41.10 AND -41.03 AND longitude BETWEEN 145.86 AND 145.93")
pharmacies = []
for r in cur.fetchall():
    dist = haversine(mc_lat, mc_lon, r[2], r[3]) * 1000  # metres
    pharmacies.append((dist, r[0], r[1], r[2], r[3]))

pharmacies.sort()
for dist, name, addr, lat, lon in pharmacies:
    marker = " <-- NEAREST" if dist == pharmacies[0][0] else ""
    q300 = " [>300m]" if dist > 300 else " [<300m]"
    print(f"  {dist:.0f}m{q300} - {name} | {addr}{marker}")

nearest = pharmacies[0][0]
print()
print(f"Nearest pharmacy: {pharmacies[0][1]} at {nearest:.0f}m")
print(f"300m rule: {'PASSES' if nearest >= 300 else 'FAILS'} ({nearest:.0f}m vs 300m minimum)")
print()

# Check if any pharmacy is co-located (within 100m)
colocated = [p for p in pharmacies if p[0] < 100]
print(f"Pharmacies within 100m (co-located): {len(colocated)}")
for p in colocated:
    print(f"  {p[0]:.0f}m - {p[1]}")

conn.close()
