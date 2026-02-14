"""
Prepare top 200 opportunities for Google Maps verification.
Sorts by confidence (desc) then nearest pharmacy distance (desc) as proxy for importance.
Priority: TAS, VIC, NSW, QLD, SA, WA, NT, ACT
"""
import csv
import os
import json

OUTPUT_DIR = r"C:\Users\MJ\Documents\GitHub\PharmacyFinder\output"
STATE_PRIORITY = ["TAS", "VIC", "NSW", "QLD", "SA", "WA", "NT", "ACT"]

all_opportunities = []

for state in STATE_PRIORITY:
    csv_path = os.path.join(OUTPUT_DIR, f"opportunity_zones_{state}.csv")
    if not os.path.exists(csv_path):
        print(f"Skipping {state} - file not found")
        continue
    
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Parse confidence percentage
            conf_str = row.get("Confidence", "0%").replace("%", "").strip()
            try:
                confidence = int(conf_str)
            except ValueError:
                confidence = 0
            
            # Parse nearest pharmacy distance
            try:
                nearest_km = float(row.get("Nearest Pharmacy (km)", "0"))
            except ValueError:
                nearest_km = 0
            
            # Determine rule type and search radius
            rules = row.get("Qualifying Rules", "")
            if "Item 130" in rules:
                search_radius_km = 1.5
            elif "Item 132" in rules or "Item 133" in rules or "Item 134" in rules or "Item 135" in rules or "Item 136" in rules:
                search_radius_km = 0.5  # 300m + buffer
            else:
                search_radius_km = 1.0
            
            all_opportunities.append({
                "state": state,
                "lat": row.get("Latitude", ""),
                "lng": row.get("Longitude", ""),
                "address": row.get("Address", ""),
                "rules": rules,
                "evidence": row.get("Evidence", "")[:200],
                "confidence": confidence,
                "nearest_km": nearest_km,
                "nearest_name": row.get("Nearest Pharmacy Name", ""),
                "poi_name": row.get("POI Name", ""),
                "poi_type": row.get("POI Type", ""),
                "search_radius_km": search_radius_km,
                "state_priority": STATE_PRIORITY.index(state),
            })

print(f"Total opportunities loaded: {len(all_opportunities)}")
for state in STATE_PRIORITY:
    count = sum(1 for o in all_opportunities if o["state"] == state)
    print(f"  {state}: {count}")

# Sort: state priority first, then confidence desc, then nearest_km desc (farther = more interesting)
all_opportunities.sort(key=lambda x: (x["state_priority"], -x["confidence"], -x["nearest_km"]))

# Take top 200
top200 = all_opportunities[:200]

print(f"\nTop 200 breakdown:")
for state in STATE_PRIORITY:
    count = sum(1 for o in top200 if o["state"] == state)
    if count > 0:
        print(f"  {state}: {count}")

# Save to JSON for the verification script
output_path = os.path.join(OUTPUT_DIR, "top200_to_verify.json")
with open(output_path, "w", encoding="utf-8") as f:
    json.dump(top200, f, indent=2)

print(f"\nSaved to {output_path}")

# Also show first 10 for inspection
print("\nFirst 10 opportunities:")
for i, opp in enumerate(top200[:10]):
    print(f"  {i+1}. {opp['state']} | {opp['confidence']}% | {opp['lat']},{opp['lng']} | {opp['rules']} | {opp['poi_name']} | nearest: {opp['nearest_km']}km")
