import json

with open("output/item131_opportunities.json") as f:
    data = json.load(f)

print(f"Total Item 131 opportunities: {len(data)}")
print()

# Sort by distance descending (most remote first)
by_dist = sorted(data, key=lambda x: -x.get("nearest_pharmacy_km", 0))

print("=== TOP 15 BY DISTANCE (most remote) ===")
for i, r in enumerate(by_dist[:15], 1):
    state = r.get("state", "?")
    name = r.get("name", "?")[:45]
    dist = r.get("nearest_pharmacy_km", 0)
    pop = r.get("population", 0)
    conf = r.get("confidence", 0)
    print(f"  {i:2}. [{state:3}] {name:45} | {dist:7.1f}km | pop {pop:>6,} | conf {conf:.2f}")

print()
print("=== TOP 10 TAS (by population) ===")
tas = sorted([r for r in data if r.get("state") == "TAS"], key=lambda x: -x.get("population", 0))
for i, r in enumerate(tas[:10], 1):
    name = r.get("name", "?")[:45]
    dist = r.get("nearest_pharmacy_km", 0)
    pop = r.get("population", 0)
    print(f"  {i:2}. {name:45} | {dist:7.1f}km | pop {pop:>6,}")

print()
print("=== BY STATE (count + avg distance) ===")
from collections import defaultdict
states = defaultdict(list)
for r in data:
    states[r.get("state", "?")].append(r.get("nearest_pharmacy_km", 0))
for st in sorted(states, key=lambda s: -len(states[s])):
    dists = states[st]
    avg = sum(dists) / len(dists) if dists else 0
    mx = max(dists) if dists else 0
    print(f"  {st:3}: {len(dists):>5} opps | avg {avg:6.1f}km | max {mx:6.1f}km")
