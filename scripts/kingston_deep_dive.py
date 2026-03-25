"""Deep dive: Kingston TAS - Item 136 opportunity."""
import sys, json, sqlite3
from geopy.distance import geodesic
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

db = sqlite3.connect('pharmacy_finder.db')
db.row_factory = sqlite3.Row

# Kingston GP Plus coords
LAT, LON = -42.988689, 147.305091

print("=" * 80)
print("DEEP DIVE: GP Plus Super Clinic Kingston")
print("2 Redwood Road, Kingston TAS 7050")
print("=" * 80)
print(f"FTE: 12.0 | GPs: 15 | Hours: 84/wk | Confidence: 0.90")
print(f"Nearest pharmacy: 0.99km (margin +690m over 300m threshold)")
print()

# Pharmacies within 5km
rows = db.execute('SELECT * FROM pharmacies WHERE latitude IS NOT NULL').fetchall()
nearby = []
for r in rows:
    d = geodesic((LAT, LON), (r['latitude'], r['longitude'])).km
    if d < 5:
        nearby.append((d, dict(r)))
nearby.sort()

print(f"PHARMACIES WITHIN 5km ({len(nearby)} found):")
print("-" * 80)
for dist, p in nearby:
    name = p.get('name', '?')
    addr = p.get('address', '?')
    print(f"  {dist:.2f}km - {name}")
    print(f"           {addr}")
    print()

# GPs within 3km
gps = db.execute('SELECT * FROM gps WHERE latitude IS NOT NULL').fetchall()
nearby_gps = []
for g in gps:
    d = geodesic((LAT, LON), (g['latitude'], g['longitude'])).km
    if d < 3:
        nearby_gps.append((d, dict(g)))
nearby_gps.sort()

print(f"GPs WITHIN 3km ({len(nearby_gps)} found):")
print("-" * 80)
for dist, g in nearby_gps:
    name = g.get('name', '') or g.get('practice_name', '?')
    fte = g.get('fte', '') or g.get('gp_fte', '?')
    num = g.get('num_gps', '') or g.get('gp_count', '?')
    addr = g.get('address', '?')
    print(f"  {dist:.2f}km - {name}")
    print(f"           FTE: {fte} | GPs: {num}")
    print(f"           {addr}")
    print()

# Supermarkets within 3km
sups = db.execute('SELECT * FROM supermarkets WHERE latitude IS NOT NULL').fetchall()
nearby_sups = []
for s in sups:
    d = geodesic((LAT, LON), (s['latitude'], s['longitude'])).km
    if d < 3:
        nearby_sups.append((d, dict(s)))
nearby_sups.sort()

print(f"SUPERMARKETS WITHIN 3km ({len(nearby_sups)} found):")
print("-" * 80)
for dist, s in nearby_sups:
    name = s.get('name', '') or s.get('brand', '?')
    gla = s.get('gla_sqm', '?')
    print(f"  {dist:.2f}km - {name} (GLA: {gla}sqm)")

# Shopping centres within 3km
scs = db.execute('SELECT * FROM shopping_centres WHERE latitude IS NOT NULL').fetchall()
nearby_scs = []
for sc in scs:
    d = geodesic((LAT, LON), (sc['latitude'], sc['longitude'])).km
    if d < 3:
        nearby_scs.append((d, dict(sc)))
nearby_scs.sort()

print(f"\nSHOPPING CENTRES WITHIN 3km ({len(nearby_scs)} found):")
print("-" * 80)
for dist, sc in nearby_scs:
    name = sc.get('name', '?')
    gla = sc.get('gla_sqm', '?')
    print(f"  {dist:.2f}km - {name} (GLA: {gla}sqm)")

# Medical centres within 3km
mcs = db.execute('SELECT * FROM medical_centres WHERE latitude IS NOT NULL').fetchall()
nearby_mcs = []
for mc in mcs:
    d = geodesic((LAT, LON), (mc['latitude'], mc['longitude'])).km
    if d < 3:
        nearby_mcs.append((d, dict(mc)))
nearby_mcs.sort()

print(f"\nMEDICAL CENTRES WITHIN 3km ({len(nearby_mcs)} found):")
print("-" * 80)
for dist, mc in nearby_mcs:
    name = mc.get('name', '?')
    fte = mc.get('fte', '') or mc.get('gp_fte', '?')
    num = mc.get('num_gps', '') or mc.get('gp_count', '?')
    hrs = mc.get('hours_per_week', '?')
    print(f"  {dist:.2f}km - {name}")
    print(f"           FTE: {fte} | GPs: {num} | Hours/wk: {hrs}")

# Population
print(f"\nPOPULATION (SA1 areas within 3km):")
print("-" * 80)
pop_rows = db.execute("""
    SELECT SA1_CODE_2021, _lat, _lon, Tot_P_P, Tot_P_M, Tot_P_F
    FROM census_sa1
    WHERE _lat IS NOT NULL AND _lon IS NOT NULL AND Tot_P_P > 0
""").fetchall()
total_pop = 0
sa1_count = 0
for p in pop_rows:
    d = geodesic((LAT, LON), (p['_lat'], p['_lon'])).km
    if d < 3:
        total_pop += (p['Tot_P_P'] or 0)
        sa1_count += 1

print(f"  SA1 areas: {sa1_count}")
print(f"  Total population (3km radius): {total_pop:,}")

# Catchment estimate
print(f"\n{'=' * 80}")
print("COMMERCIAL VIABILITY ESTIMATE")
print("=" * 80)
print(f"  Population within 3km: {total_pop:,}")
print(f"  Existing pharmacies within 5km: {len(nearby)}")
if len(nearby) > 0:
    print(f"  Pop per pharmacy (rough): {total_pop // max(len(nearby), 1):,}")
print(f"  GPs within 3km: {len(nearby_gps)} practices")
print(f"  Anchor medical centre: 15 GPs, 12 FTE, 84hrs/wk")
print(f"  Supermarkets nearby: {len(nearby_sups)}")

db.close()
