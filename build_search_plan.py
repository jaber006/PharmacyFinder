"""
Build the search plan for property scraping.
Prioritises TAS, then VIC, NSW, QLD.
Focuses on areas with population > 2000 (where commercial properties exist).
"""
import csv, os, re, json
from collections import Counter

STATE_PRIORITY = ['TAS', 'VIC', 'NSW', 'QLD', 'SA', 'WA', 'NT', 'ACT']
all_opps = []

for state in STATE_PRIORITY:
    for prefix in ['population_ranked', 'opportunity_zones']:
        path = f'output/{prefix}_{state}.csv'
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as f:
                for row in csv.DictReader(f):
                    if row.get('Verification','').upper() == 'FALSE POSITIVE':
                        continue
                    try:
                        lat = float(row['Latitude'])
                        lon = float(row['Longitude'])
                    except:
                        continue
                    conf = float(row.get('Confidence','0').replace('%','').strip() or 0) / 100
                    composite = 0
                    for sf in ['Composite Score', 'Opportunity Score']:
                        try:
                            composite = float(row.get(sf,0) or 0)
                            if composite > 0: break
                        except: pass
                    pop = 0
                    for pf in ['Pop 10km','Pop 5km']:
                        try:
                            pop = int(float(row.get(pf,0) or 0))
                            if pop > 0: break
                        except: pass
                    
                    # For property search, rank by population-weighted confidence
                    # Remote communities won't have listings anyway
                    rank = composite if composite > 0 else conf * max(pop,1)
                    
                    # Property search viability score — areas with people + some infrastructure
                    town_pop = int(float(row.get('Nearest Town Pop',0) or 0))
                    viability = pop * conf  # population × confidence
                    
                    addr = row.get('Address','')
                    suburb = ''
                    if addr:
                        parts = [p.strip() for p in addr.split(',')]
                        skip = ['road','street','avenue','drive','place','lane','highway',
                                'lookout','trail','apartments','boulevard','way','crescent',
                                'terrace','parade','court']
                        for p in parts:
                            if not p or len(p) < 3: continue
                            if p[0].isdigit(): continue
                            if re.match(r'^\d{4}$', p): continue
                            if p in ('NSW','VIC','QLD','WA','SA','TAS','NT','ACT','Australia'): continue
                            if p.startswith(('City of','Municipality of','Shire of','Greater ')): continue
                            if p in ('Tasmania','Victoria','New South Wales','Queensland',
                                    'South Australia','Western Australia','Northern Territory',
                                    'Australian Capital Territory'): continue
                            if any(w in p.lower() for w in skip): continue
                            suburb = p
                            break
                    if not suburb:
                        town = row.get('Nearest Town','')
                        if town and town != 'Unknown': suburb = town
                    
                    all_opps.append({
                        'lat': lat, 'lon': lon, 'state': state,
                        'suburb': suburb, 'rank': rank, 'conf': conf, 
                        'composite': composite, 'viability': viability,
                        'poi': row.get('POI Name',''), 
                        'rules': row.get('Qualifying Rules',''),
                        'nearest_pharm_km': float(row.get('Nearest Pharmacy (km)',0) or 0),
                        'poi_type': row.get('POI Type',''),
                        'conf_pct': row.get('Confidence',''),
                        'pop10': pop, 'town_pop': town_pop,
                    })
            break

# Sort by state priority then viability within state
state_order = {s: i for i, s in enumerate(STATE_PRIORITY)}
all_opps.sort(key=lambda x: (state_order.get(x['state'], 99), -x['viability']))

# Take ALL TAS (they're small), then top from each other state
# Filter: must have suburb and pop10 > 2000 (or TAS with pop > 0)
selected = []
seen_suburbs = set()

# TAS first — take all with a suburb
for o in all_opps:
    if o['state'] == 'TAS' and o['suburb']:
        key = o['suburb'] + '|' + o['state']
        selected.append(o)
        seen_suburbs.add(key)

# Then other states — top 20 per state, pop > 2000
for state in STATE_PRIORITY[1:]:
    state_opps = [o for o in all_opps if o['state'] == state and o['suburb'] and o['pop10'] >= 2000]
    for o in state_opps[:20]:
        key = o['suburb'] + '|' + o['state']
        selected.append(o)
        seen_suburbs.add(key)

# Group by suburb
grouped = {}
for i, o in enumerate(selected):
    o['idx'] = i
    key = o['suburb'] + '|' + o['state']
    if key not in grouped:
        grouped[key] = []
    grouped[key].append(o)

print(f'Total opps across all states: {len(all_opps)}')
print(f'Selected for property search: {len(selected)}')
print(f'Unique suburbs to search: {len(grouped)}')
print()

sc = Counter(o['state'] for o in selected)
for s in STATE_PRIORITY:
    if sc.get(s,0) > 0:
        print(f'  {s}: {sc.get(s,0)} selected')
print()

# Build search list — prioritised by state then rank
searches = []
for key in grouped:
    suburb, state = key.split('|',1)
    count = len(grouped[key])
    best_rank = max(o['rank'] for o in grouped[key])
    best_pop = max(o['pop10'] for o in grouped[key])
    best_conf = max(o['conf'] for o in grouped[key])
    opps_data = grouped[key]
    searches.append({
        'suburb': suburb, 'state': state, 'count': count, 
        'best_rank': best_rank, 'best_pop': best_pop, 'best_conf': best_conf,
    })

# Sort: TAS first, then by viability
searches.sort(key=lambda x: (state_order.get(x['state'], 99), -x['best_rank']))

print('Search plan:')
print('-' * 70)
for i, s in enumerate(searches, 1):
    print(f'  {i:3d}. {s["suburb"]}, {s["state"]} — {s["count"]} opps, '
          f'pop {s["best_pop"]:,}, conf {s["best_conf"]:.0%}, rank {s["best_rank"]:,.0f}')

# Save
plan = {'searches': searches, 'opportunities': selected}
with open('output/_search_plan.json', 'w') as f:
    json.dump(plan, f, indent=2)
print(f'\nSaved: output/_search_plan.json ({len(searches)} suburbs)')
