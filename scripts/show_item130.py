import json
data = json.load(open('output/item130_opportunities.json', 'r', encoding='utf-8'))
for i, d in enumerate(data, 1):
    state = d.get('state', '??')
    name = d.get('name', 'Unknown')
    lat, lon = d['lat'], d['lon']
    dist = d.get('nearest_pharmacy_km', '?')
    conf = d.get('confidence', '?')
    reason = d.get('reason', '').replace('\u2265', '>=')
    print(f"{i}. [{state:>3}] {name}")
    print(f"   Coords: {lat:.4f}, {lon:.4f} | Nearest pharmacy: {dist}km | Confidence: {conf}")
    print(f"   {reason[:250]}")
    print()
