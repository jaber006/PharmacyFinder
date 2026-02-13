import csv, glob, os

all_opps = []
state_counts = {}

for f in sorted(glob.glob('output/opportunity_zones_*.csv')):
    state = os.path.basename(f).replace('opportunity_zones_','').replace('.csv','')
    with open(f, encoding='utf-8') as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)
        state_counts[state] = len(rows)
        for r in rows:
            r['_state'] = state
            all_opps.append(r)

print('=== OPPORTUNITIES PER STATE ===')
total = 0
for s in ['NSW','VIC','QLD','WA','SA','TAS','NT','ACT']:
    c = state_counts.get(s, 0)
    total += c
    print(f'  {s}: {c}')
print(f'  TOTAL: {total}')

print()
print('=== TOP 10 HIGHEST-CONFIDENCE OPPORTUNITIES (ALL STATES) ===')

def conf_sort(r):
    conf = r.get('Confidence','0%').replace('%','')
    try:
        c = int(conf)
    except:
        c = 0
    try:
        d = float(r.get('Nearest Pharmacy (km)','0'))
    except:
        d = 0
    return (-c, -d)

all_opps.sort(key=conf_sort)

for i, r in enumerate(all_opps[:10]):
    print(f"")
    print(f"  {i+1}. [{r.get('Confidence','?')}] {r.get('POI Name','?')}")
    print(f"     State: {r['_state']}")
    addr = r.get('Address','?')
    if len(addr) > 100:
        addr = addr[:100] + '...'
    print(f"     Address: {addr}")
    print(f"     Rules: {r.get('Qualifying Rules','?')}")
    print(f"     Nearest Pharmacy: {r.get('Nearest Pharmacy (km)','?')} km ({r.get('Nearest Pharmacy Name','?')})")
    print(f"     POI Type: {r.get('POI Type','?')}")
