"""Quick scan of all data tables to understand what we're working with for rule-first approach."""
import sqlite3, sys
sys.stdout.reconfigure(encoding='utf-8')
conn = sqlite3.connect('pharmacy_finder.db')
cur = conn.cursor()

# Pharmacy town grouping
print("=== PHARMACY TOWNS (suburb+postcode) ===")
cur.execute("""SELECT suburb, postcode, state, COUNT(*) as cnt 
               FROM pharmacies WHERE suburb IS NOT NULL AND postcode IS NOT NULL
               GROUP BY suburb, postcode, state ORDER BY cnt DESC LIMIT 20""")
for r in cur.fetchall():
    print(f"  {r[0]} {r[2]} {r[1]}: {r[3]} pharmacies")

# Single-pharmacy towns
cur.execute("""SELECT COUNT(DISTINCT suburb||'|'||postcode) FROM pharmacies 
               WHERE suburb IS NOT NULL AND postcode IS NOT NULL""")
total_towns = cur.fetchone()[0]
cur.execute("""SELECT suburb, postcode, state, COUNT(*) as cnt 
               FROM pharmacies WHERE suburb IS NOT NULL AND postcode IS NOT NULL
               GROUP BY suburb, postcode HAVING cnt = 1""")
single_ph_towns = cur.fetchall()
print(f"\nTotal pharmacy towns: {total_towns}")
print(f"Single-pharmacy towns: {len(single_ph_towns)}")
print("Sample single-pharmacy towns:")
for r in single_ph_towns[:10]:
    print(f"  {r[0]} {r[2]} {r[1]}")

# Shopping centres - check for pharmacies nearby
print("\n=== SHOPPING CENTRES vs PHARMACIES ===")
cur.execute("SELECT id, name, latitude, longitude, gla_sqm, estimated_gla, estimated_tenants, centre_class FROM shopping_centres WHERE latitude IS NOT NULL")
scs = cur.fetchall()
cur.execute("SELECT latitude, longitude FROM pharmacies WHERE latitude IS NOT NULL")
phs = cur.fetchall()

import math
def hav(lat1,lon1,lat2,lon2):
    R=6371;dLat=math.radians(lat2-lat1);dLon=math.radians(lon2-lon1)
    a=math.sin(dLat/2)**2+math.cos(math.radians(lat1))*math.cos(math.radians(lat2))*math.sin(dLon/2)**2
    return R*2*math.atan2(math.sqrt(a),math.sqrt(1-a))

no_ph = []
for sc in scs:
    sid, name, slat, slng, gla, egla, tenants, cls = sc
    min_d = min(hav(slat, slng, p[0], p[1]) for p in phs)
    if min_d > 0.3:  # no pharmacy within 300m
        no_ph.append((name, gla or egla or 0, tenants or 0, cls, min_d))
print(f"Shopping centres with NO pharmacy within 300m: {len(no_ph)}/{len(scs)}")
for name, gla, tenants, cls, d in sorted(no_ph, key=lambda x: -x[1])[:15]:
    print(f"  {name}: GLA={gla:.0f}, {tenants} tenants, class={cls}, nearest_ph={d:.1f}km")

# Medical centres - high GP counts
print("\n=== MEDICAL CENTRES (potential Item 136) ===")
cur.execute("""SELECT id, name, latitude, longitude, num_gps, total_fte, hours_per_week, state
               FROM medical_centres WHERE latitude IS NOT NULL AND num_gps >= 6
               ORDER BY num_gps DESC""")
mcs = cur.fetchall()
print(f"Medical centres with >=6 GPs: {len(mcs)}")
for mc in mcs[:15]:
    mid, name, mlat, mlng, gps, fte, hrs, state = mc
    min_d = min(hav(mlat, mlng, p[0], p[1]) for p in phs)
    ph_inside = min_d < 0.1
    print(f"  {name} ({state}): {gps} GPs, {fte:.1f} FTE, {hrs:.0f}hrs/wk, nearest_ph={min_d*1000:.0f}m {'[PH INSIDE!]' if ph_inside else ''}")

# Hospitals >=150 beds
print("\n=== HOSPITALS >=100 BEDS ===")
cur.execute("SELECT name, latitude, longitude, bed_count, hospital_type FROM hospitals WHERE bed_count >= 100 AND latitude IS NOT NULL")
hosps = cur.fetchall()
for h in hosps:
    min_d = min(hav(h[1], h[2], p[0], p[1]) for p in phs)
    print(f"  {h[0]}: {h[3]} beds, type={h[4]}, nearest_ph={min_d*1000:.0f}m")

# Count POIs that could be Item 131 seeds
print("\n=== POTENTIAL ITEM 131 SEEDS (POIs far from pharmacies) ===")
cur.execute("SELECT name, latitude, longitude, 'supermarket' as type FROM supermarkets WHERE latitude IS NOT NULL")
all_pois = cur.fetchall()
cur.execute("SELECT name, latitude, longitude, 'medical' as type FROM medical_centres WHERE latitude IS NOT NULL")
all_pois += cur.fetchall()
cur.execute("SELECT name, latitude, longitude, 'gp' as type FROM gps WHERE latitude IS NOT NULL")
all_pois += cur.fetchall()

far_pois = []
for poi in all_pois:
    name, plat, plng, ptype = poi
    min_d = min(hav(plat, plng, p[0], p[1]) for p in phs)
    if min_d >= 8:  # 8km+ straight line from any pharmacy
        far_pois.append((name, ptype, min_d, plat, plng))

print(f"POIs >=8km straight line from any pharmacy: {len(far_pois)}")
for name, ptype, d, lat, lng in sorted(far_pois, key=lambda x: -x[2])[:20]:
    print(f"  {name} ({ptype}): {d:.1f}km from nearest pharmacy")

conn.close()
