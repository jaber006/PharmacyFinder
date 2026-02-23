"""
Fix pharmacy coordinate data:
1. Check how many from each source
2. Find OSM duplicates (already in findapharmacy)
3. Remove duplicates, keeping findapharmacy version
4. Flag remaining OSM pharmacies with bad addresses
5. Re-verify remaining using Google Maps
"""
import sqlite3, os, sys, io
from math import radians, cos, sin, asin, sqrt

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace', line_buffering=True)
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'pharmacy_finder.db')

def haversine(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1; dlon = lon2 - lon1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    return 2 * 6371000 * asin(sqrt(a))

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

# Step 1: Source breakdown
print("STEP 1: Data source breakdown")
print("="*50)
c.execute("SELECT source, COUNT(*) FROM pharmacies GROUP BY source ORDER BY COUNT(*) DESC")
for src, count in c.fetchall():
    print(f"  {src}: {count}")

c.execute("SELECT COUNT(*) FROM pharmacies")
total = c.fetchone()[0]
print(f"\n  TOTAL: {total}")

# Step 2: Find OSM pharmacies
print(f"\nSTEP 2: Analyzing OSM pharmacies")
print("="*50)
c.execute("SELECT id, name, address, latitude, longitude, state FROM pharmacies WHERE source = 'OpenStreetMap'")
osm = c.fetchall()
print(f"  OSM pharmacies: {len(osm)}")

# Check how many have useless addresses (just state name, or very short)
bad_address = 0
no_street = 0
for id, name, address, lat, lng, state in osm:
    if not address or len(address) < 10:
        bad_address += 1
    elif address.strip() in ['NSW', 'VIC', 'QLD', 'SA', 'WA', 'TAS', 'NT', 'ACT']:
        bad_address += 1
    elif ',' not in address and not any(c.isdigit() for c in (address or '')):
        no_street += 1

print(f"  Bad/missing addresses: {bad_address}")
print(f"  No street number: {no_street}")

# Step 3: Find duplicates (OSM pharmacy within 200m of a findapharmacy entry)
print(f"\nSTEP 3: Finding OSM duplicates")
print("="*50)

c.execute("SELECT id, name, latitude, longitude FROM pharmacies WHERE source = 'findapharmacy.com.au'")
fap = c.fetchall()
print(f"  findapharmacy entries: {len(fap)}")

# Build spatial index (simple approach)
duplicates = []
osm_only = []

for osm_id, osm_name, osm_addr, osm_lat, osm_lng, osm_state in osm:
    is_dup = False
    for fap_id, fap_name, fap_lat, fap_lng in fap:
        # Quick filter
        if abs(osm_lat - fap_lat) > 0.01 or abs(osm_lng - fap_lng) > 0.01:
            continue
        dist = haversine(osm_lat, osm_lng, fap_lat, fap_lng)
        if dist < 500:  # Within 500m = likely same pharmacy
            duplicates.append((osm_id, osm_name, osm_addr, fap_name, dist))
            is_dup = True
            break
    if not is_dup:
        osm_only.append((osm_id, osm_name, osm_addr, osm_lat, osm_lng, osm_state))

print(f"  OSM duplicates (within 500m of findapharmacy): {len(duplicates)}")
print(f"  OSM unique (not in findapharmacy): {len(osm_only)}")

# Step 4: Delete duplicates
print(f"\nSTEP 4: Removing {len(duplicates)} OSM duplicates")
print("="*50)
for osm_id, osm_name, osm_addr, fap_name, dist in duplicates[:10]:
    print(f"  DELETE: '{osm_name}' ({osm_addr}) -- dup of '{fap_name}' ({dist:.0f}m)")
if len(duplicates) > 10:
    print(f"  ... and {len(duplicates) - 10} more")

dup_ids = [d[0] for d in duplicates]
if dup_ids:
    c.execute(f"DELETE FROM pharmacies WHERE id IN ({','.join('?' * len(dup_ids))})", dup_ids)
    print(f"  Deleted {c.rowcount} duplicate OSM entries")

# Step 5: Check remaining OSM-only entries
print(f"\nSTEP 5: Remaining OSM-only pharmacies ({len(osm_only)})")
print("="*50)
bad_osm = []
ok_osm = []
for id, name, addr, lat, lng, state in osm_only:
    addr_str = addr or ''
    if len(addr_str) < 10 or addr_str.strip() in ['NSW', 'VIC', 'QLD', 'SA', 'WA', 'TAS', 'NT', 'ACT']:
        bad_osm.append((id, name, addr, lat, lng, state))
    else:
        ok_osm.append((id, name, addr, lat, lng, state))

print(f"  With usable addresses: {len(ok_osm)}")
print(f"  With bad/no addresses: {len(bad_osm)}")

# Delete bad OSM entries (no way to verify without address)
if bad_osm:
    print(f"\n  Removing {len(bad_osm)} OSM entries with unusable addresses:")
    for id, name, addr, lat, lng, state in bad_osm[:10]:
        print(f"    DELETE: '{name}' (addr: '{addr}') [{state}]")
    if len(bad_osm) > 10:
        print(f"    ... and {len(bad_osm) - 10} more")
    
    bad_ids = [b[0] for b in bad_osm]
    c.execute(f"DELETE FROM pharmacies WHERE id IN ({','.join('?' * len(bad_ids))})", bad_ids)
    print(f"  Deleted {c.rowcount} bad OSM entries")

conn.commit()

# Final count
c.execute("SELECT source, COUNT(*) FROM pharmacies GROUP BY source ORDER BY COUNT(*) DESC")
print(f"\nFINAL STATE:")
print("="*50)
for src, count in c.fetchall():
    print(f"  {src}: {count}")
c.execute("SELECT COUNT(*) FROM pharmacies")
new_total = c.fetchone()[0]
print(f"\n  TOTAL: {new_total} (was {total}, removed {total - new_total})")
print(f"\n  Remaining OSM with addresses to verify: {len(ok_osm)}")

# Save list of remaining OSM pharmacies needing verification
if ok_osm:
    print(f"\n  Remaining OSM pharmacies (sample):")
    for id, name, addr, lat, lng, state in ok_osm[:15]:
        print(f"    {name} | {addr} | ({lat:.4f}, {lng:.4f})")
    if len(ok_osm) > 15:
        print(f"    ... and {len(ok_osm) - 15} more")

conn.close()
print(f"\nDone! Next step: verify remaining {len(ok_osm)} OSM pharmacies via Google Maps.")
