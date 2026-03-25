"""Deep dive: Item 132 opportunities — one-pharmacy towns."""
import sys, json, sqlite3
from geopy.distance import geodesic
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

with open('output/item132_opportunities.json') as f:
    data = json.load(f)

db = sqlite3.connect('pharmacy_finder.db')
db.row_factory = sqlite3.Row

# Pre-load pharmacies and census for speed
all_pharms = [dict(r) for r in db.execute('SELECT * FROM pharmacies WHERE latitude IS NOT NULL').fetchall()]
all_sa1 = [dict(r) for r in db.execute("""
    SELECT SA1_CODE_2021, _lat, _lon, Tot_P_P
    FROM census_sa1
    WHERE _lat IS NOT NULL AND _lon IS NOT NULL AND Tot_P_P > 0
""").fetchall()]
all_gps = [dict(r) for r in db.execute('SELECT * FROM gps WHERE latitude IS NOT NULL').fetchall()]
all_sups = [dict(r) for r in db.execute('SELECT * FROM supermarkets WHERE latitude IS NOT NULL').fetchall()]

for i, opp in enumerate(data, 1):
    lat, lon = opp['lat'], opp['lon']
    suburb = opp.get('suburb', '?')
    postcode = opp.get('postcode', '?')
    state = opp.get('state', '?')
    conf = opp.get('confidence', 0)
    reason = opp.get('reason', '')
    existing = opp.get('existing_pharmacy', '?')
    gp_fte = opp.get('gp_fte', '?')
    sup_detail = opp.get('supermarket_detail', '?')
    dist = opp.get('nearest_pharmacy_km', 0)

    print('=' * 80)
    print(f'{i}. {suburb} {postcode}, {state} (confidence={conf})')
    print('=' * 80)
    print(f'   Existing pharmacy: {existing}')
    print(f'   GP FTE in town: {gp_fte} (need 4.0 to qualify)')
    print(f'   Nearest OTHER pharmacy: {dist}km')
    print(f'   Supermarkets: {sup_detail}')
    print(f'   Status: {reason[:200]}')
    print()

    # Population within 5km
    total_pop = sum(
        (p['Tot_P_P'] or 0)
        for p in all_sa1
        if geodesic((lat, lon), (p['_lat'], p['_lon'])).km < 5
    )

    # Pharmacies within 15km
    nearby_pharms = sorted(
        [(geodesic((lat, lon), (p['latitude'], p['longitude'])).km, p) for p in all_pharms
         if geodesic((lat, lon), (p['latitude'], p['longitude'])).km < 15],
        key=lambda x: x[0]
    )

    # GPs within 5km
    nearby_gps = sorted(
        [(geodesic((lat, lon), (g['latitude'], g['longitude'])).km, g) for g in all_gps
         if geodesic((lat, lon), (g['latitude'], g['longitude'])).km < 5],
        key=lambda x: x[0]
    )

    # Supermarkets within 5km
    nearby_sups = sorted(
        [(geodesic((lat, lon), (s['latitude'], s['longitude'])).km, s) for s in all_sups
         if geodesic((lat, lon), (s['latitude'], s['longitude'])).km < 5],
        key=lambda x: x[0]
    )

    print(f'   POPULATION (5km radius): {total_pop:,}')
    print()

    print(f'   PHARMACIES WITHIN 15km ({len(nearby_pharms)}):')
    for d, ph in nearby_pharms[:8]:
        print(f'     {d:.1f}km - {ph.get("name", "?")}')
        print(f'             {ph.get("address", "?")}')
    print()

    print(f'   GPs WITHIN 5km ({len(nearby_gps)}):')
    for d, g in nearby_gps[:6]:
        name = g.get('name', '') or g.get('practice_name', '?')
        fte = g.get('fte', '') or g.get('gp_fte', '?')
        print(f'     {d:.1f}km - {name} (FTE: {fte})')
    print()

    print(f'   SUPERMARKETS WITHIN 5km ({len(nearby_sups)}):')
    for d, s in nearby_sups[:5]:
        name = s.get('name', '') or s.get('brand', '?')
        gla = s.get('gla_sqm', '?')
        print(f'     {d:.1f}km - {name} (GLA: {gla}sqm)')
    print()

    # Commercial assessment
    print('   COMMERCIAL ASSESSMENT:')
    pop_per_pharm = total_pop // max(len(nearby_pharms), 1)
    gap_to_qualify = 4.0 - float(gp_fte) if gp_fte != '?' else '?'
    print(f'     Pop/pharmacy ratio: {pop_per_pharm:,}')
    print(f'     GP FTE gap to qualify: {gap_to_qualify} more FTE needed')
    print(f'     Isolation: nearest other pharmacy {dist}km away')
    if dist > 20:
        print(f'     >> HIGH isolation — captive market if qualified')
    elif dist > 10:
        print(f'     >> MODERATE isolation')
    else:
        print(f'     >> LOW isolation — competition nearby')
    print()

db.close()
