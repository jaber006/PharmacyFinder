import json

with open(r'C:\Users\MJ\Documents\GitHub\PharmacyFinder\output\pass_list.json', encoding='utf-8') as f:
    data = json.load(f)

print(f"Total PASS: {len(data)}")
print()

# Group by state
states = {}
for d in data:
    s = d.get('state', 'Unknown')
    states[s] = states.get(s, 0) + 1
print("By state:")
for s, c in sorted(states.items(), key=lambda x: -x[1]):
    print(f"  {s}: {c}")

# Group by poi_type
types = {}
for d in data:
    t = d.get('poi_type', 'Unknown')
    types[t] = types.get(t, 0) + 1
print("\nBy POI type:")
for t, c in sorted(types.items(), key=lambda x: -x[1]):
    print(f"  {t}: {c}")

# Group by rule
rules = {}
for d in data:
    r = d.get('original_rules', 'NONE')
    rules[r] = rules.get(r, 0) + 1
print("\nBy rule:")
for r, c in sorted(rules.items(), key=lambda x: -x[1]):
    print(f"  {r}: {c}")

# Group by score
scores = {}
for d in data:
    s = d.get('score', 0)
    scores[s] = scores.get(s, 0) + 1
print("\nBy score:")
for s, c in sorted(scores.items(), key=lambda x: -x[0]):
    print(f"  {s}: {c}")

# Check for geocoding flags
flagged = [d for d in data if d.get('geocoding_flag')]
print(f"\nGeocoding flagged: {len(flagged)}")
for d in flagged[:10]:
    print(f"  ID={d['id']} {d['name']} | {d['geocoding_flag']}")

# Check for suspicious coords (all same lat/lng)
from collections import Counter
coords = Counter()
for d in data:
    k = f"{d.get('lat','')},{d.get('lng','')}"
    coords[k] += 1
print("\nDuplicate coordinates (potential geocoding issues):")
for k, c in coords.most_common(10):
    if c > 1:
        print(f"  {k}: {c} opportunities")
        # Show names
        for d in data:
            if f"{d.get('lat','')},{d.get('lng','')}" == k:
                print(f"    ID={d['id']} {d['name']}")
