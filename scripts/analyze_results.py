"""Analyze non-Item-131 scanner results."""
import sys, json
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
from collections import Counter

files = {
    'Item 130': 'output/item130_opportunities.json',
    'Item 132': 'output/item132_opportunities.json',
    'Item 133': 'output/item133_opportunities.json',
    'Item 136': 'output/item136_opportunities.json',
}

all_opps = []
for rule, path in files.items():
    with open(path) as f:
        data = json.load(f)
    for d in data:
        d['rule_item'] = rule
        all_opps.append(d)

all_opps.sort(key=lambda x: (-x.get('confidence', 0), -x.get('nearest_pharmacy_km', 0)))

print(f'TOTAL: {len(all_opps)} opportunities (excl Item 131)\n')

states = Counter(x.get('state', '?') for x in all_opps)
print('By state:', dict(states))

rules = Counter(x.get('rule_item', '?') for x in all_opps)
print('By rule:', dict(rules))
print()

for i, o in enumerate(all_opps, 1):
    name = o.get('name', '?')[:50]
    state = o.get('state', '?')
    conf = o.get('confidence', 0)
    dist = o.get('nearest_pharmacy_km', 0)
    rule = o.get('rule_item', '?')
    reason = o.get('reason', '')[:140]

    extras = ''
    fte = o.get('effective_fte', '')
    if fte:
        extras += f' FTE={fte}'
    ngps = o.get('num_gps', '')
    if ngps:
        extras += f' GPs={ngps}'
    gla = o.get('gla_sqm', '')
    if gla:
        extras += f' GLA={gla}sqm'
    hrs = o.get('hours_per_week', '')
    if hrs:
        extras += f' hrs/wk={hrs}'
    sup = o.get('supermarket_detail', '') or o.get('supermarket', '')
    if sup:
        extras += f' [{sup[:60]}]'
    suburb = o.get('suburb', '')
    postcode = o.get('postcode', '')
    addr = o.get('address', '')[:60]

    print(f'{i:2}. [{rule:<8}] {name:<50} | {state} | conf={conf:.2f} | {dist:.1f}km')
    print(f'    {addr}')
    print(f'   {extras}')
    print(f'    >> {reason}')
    print()
