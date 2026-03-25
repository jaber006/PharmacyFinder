import json
data = json.load(open('output/item136_opportunities.json','r',encoding='utf-8'))
for d in data[:5]:
    print(f"Name: {d.get('name','?')}")
    reason = d.get('reason','').replace('\u2265','>=')
    print(f"Reason: {reason[:250]}")
    print(f"State: {d.get('state')}, Dist: {d.get('nearest_pharmacy_km')}km, Conf: {d.get('confidence')}")
    print()
