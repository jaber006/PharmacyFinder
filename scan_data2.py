"""Deeper scan: single-pharmacy towns with isolation check, Item 132 candidates."""
import sqlite3, sys, math
sys.stdout.reconfigure(encoding='utf-8')
conn = sqlite3.connect('pharmacy_finder.db')
cur = conn.cursor()

def hav(lat1,lon1,lat2,lon2):
    R=6371;dLat=math.radians(lat2-lat1);dLon=math.radians(lon2-lon1)
    a=math.sin(dLat/2)**2+math.cos(math.radians(lat1))*math.cos(math.radians(lat2))*math.sin(dLon/2)**2
    return R*2*math.atan2(math.sqrt(a),math.sqrt(1-a))

# Load all pharmacies
cur.execute("SELECT id, name, suburb, postcode, state, latitude, longitude FROM pharmacies WHERE latitude IS NOT NULL AND longitude IS NOT NULL AND suburb IS NOT NULL AND postcode IS NOT NULL")
all_phs = cur.fetchall()
ph_coords = [(p[5], p[6]) for p in all_phs]

# Group pharmacies by town
towns = {}
for p in all_phs:
    pid, name, suburb, postcode, state, lat, lng = p
    key = f"{suburb}|{postcode}"
    if key not in towns:
        towns[key] = {'suburb': suburb, 'postcode': postcode, 'state': state, 'pharmacies': [], 'lat': lat, 'lng': lng}
    towns[key]['pharmacies'].append({'id': pid, 'name': name, 'lat': lat, 'lng': lng})

# Find single-pharmacy towns where next nearest pharmacy outside town is >=8km SL
print("=== ITEM 132 CANDIDATES: Single-pharmacy towns, isolated ===")
candidates_132 = []
for key, town in towns.items():
    if len(town['pharmacies']) != 1:
        continue
    ph = town['pharmacies'][0]
    # Find nearest pharmacy NOT in this town
    min_outside = float('inf')
    nearest_outside = None
    for op in all_phs:
        okey = f"{op[2]}|{op[3]}"
        if okey == key:
            continue
        d = hav(ph['lat'], ph['lng'], op[5], op[6])
        if d < min_outside:
            min_outside = d
            nearest_outside = op
    if min_outside >= 8:  # 8km SL = likely 10km+ by road
        candidates_132.append({
            'town': town['suburb'],
            'postcode': town['postcode'],
            'state': town['state'],
            'pharmacy': ph['name'],
            'ph_lat': ph['lat'],
            'ph_lng': ph['lng'],
            'next_nearest_km': min_outside,
            'next_nearest_name': nearest_outside[1] if nearest_outside else '?',
        })

candidates_132.sort(key=lambda x: -x['next_nearest_km'])
print(f"Found {len(candidates_132)} single-pharmacy towns with next pharmacy >=8km SL")
for c in candidates_132[:30]:
    print(f"  {c['town']} {c['state']} {c['postcode']}: {c['pharmacy']} | next={c['next_nearest_name']} at {c['next_nearest_km']:.1f}km")

# Check which of these have supermarkets and GPs nearby
print("\n=== ENRICHING with supermarket/GP data ===")
cur.execute("SELECT name, latitude, longitude, estimated_gla, brand FROM supermarkets WHERE latitude IS NOT NULL")
supers = cur.fetchall()
cur.execute("SELECT name, latitude, longitude, fte FROM gps WHERE latitude IS NOT NULL")
gps = cur.fetchall()
cur.execute("SELECT name, latitude, longitude, num_gps, total_fte FROM medical_centres WHERE latitude IS NOT NULL")
mcs = cur.fetchall()

for c in candidates_132[:15]:
    lat, lng = c['ph_lat'], c['ph_lng']
    nearby_supers = [(hav(lat, lng, s[1], s[2]), s[0], s[3] or 0) for s in supers if hav(lat, lng, s[1], s[2]) < 5]
    nearby_supers.sort()
    nearby_gps = [(hav(lat, lng, g[1], g[2]), g[0], g[3] or 0) for g in gps if hav(lat, lng, g[1], g[2]) < 5]
    nearby_mcs = [(hav(lat, lng, m[1], m[2]), m[0], m[3] or 0, m[4] or 0) for m in mcs if hav(lat, lng, m[1], m[2]) < 5]
    
    top_gla = sum(s[2] for s in sorted(nearby_supers, key=lambda x: -x[2])[:2])
    total_fte = sum(g[2] for g in nearby_gps) + sum(m[3] for m in nearby_mcs)
    
    status = []
    if top_gla >= 2500: status.append(f"GLA={top_gla:.0f} OK")
    elif top_gla >= 2000: status.append(f"GLA={top_gla:.0f} CLOSE")
    elif nearby_supers: status.append(f"GLA={top_gla:.0f} LOW")
    else: status.append("NO SUPER")
    
    if total_fte >= 4: status.append(f"FTE={total_fte:.1f} OK")
    elif total_fte > 0: status.append(f"FTE={total_fte:.1f} CHECK")
    else: status.append("NO FTE DATA")
    
    print(f"  {c['town']} {c['state']}: next_ph={c['next_nearest_km']:.1f}km | {' | '.join(status)}")

# Item 136 candidates: medical centres with good criteria
print("\n=== ITEM 136 CANDIDATES (no pharmacy inside, >=300m, >=6 GPs) ===")
cur.execute("""SELECT id, name, latitude, longitude, num_gps, total_fte, hours_per_week, state
               FROM medical_centres WHERE latitude IS NOT NULL AND num_gps >= 6""")
mc_candidates = []
for mc in cur.fetchall():
    mid, name, mlat, mlng, gps, fte, hrs, state = mc
    # Find nearest pharmacy
    nearest_ph_d = min(hav(mlat, mlng, p[0], p[1]) for p in ph_coords)
    nearest_ph_m = nearest_ph_d * 1000
    if nearest_ph_m >= 300 and nearest_ph_d < 0.1 == False:
        # No pharmacy inside (>100m) and >=300m from nearest
        if nearest_ph_m >= 300:
            mc_candidates.append({
                'name': name, 'state': state, 'gps': gps, 'fte': fte,
                'hrs': hrs, 'nearest_ph_m': nearest_ph_m
            })

mc_candidates.sort(key=lambda x: (-x['fte'], -x['nearest_ph_m']))
print(f"Medical centres with >=6 GPs AND nearest pharmacy >=300m: {len(mc_candidates)}")
for c in mc_candidates:
    hrs_status = "OK" if c['hrs'] >= 70 else f"{c['hrs']:.0f}hrs CHECK"
    print(f"  {c['name']} ({c['state']}): {c['gps']} GPs, {c['fte']:.1f} FTE, {hrs_status}, nearest_ph={c['nearest_ph_m']:.0f}m")

conn.close()
