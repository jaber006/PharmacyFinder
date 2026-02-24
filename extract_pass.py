import json

with open(r'C:\Users\MJ\Documents\GitHub\PharmacyFinder\output\scored_v2.json', encoding='utf-8') as f:
    data = json.load(f)

pass_opps = [d for d in data if d.get('verdict') == 'PASS']
print(f'Total PASS: {len(pass_opps)}')

pass_opps.sort(key=lambda x: x.get('score', 0), reverse=True)

for i, o in enumerate(pass_opps[:30]):
    oid = o['id']
    name = o['name']
    state = o['state']
    score = o.get('score', 0)
    lat = o.get('lat', '')
    lng = o.get('lng', '')
    addr = o.get('address', '')
    nkm = o.get('nearest_pharmacy_km', '?')
    nph = o.get('nearest_pharmacy', '?')
    pt = o.get('poi_type', '?')
    rules = o.get('original_rules', '?')
    print(f"{i+1}. ID={oid} | {name} | {state} | score={score} | ({lat},{lng}) | nearest={nkm}km ({nph}) | type={pt} | rule={rules}")

# Also save full list for batch processing
with open(r'C:\Users\MJ\Documents\GitHub\PharmacyFinder\output\pass_list.json', 'w') as f:
    json.dump(pass_opps, f, indent=2)
print(f"\nSaved {len(pass_opps)} PASS opportunities to pass_list.json")
