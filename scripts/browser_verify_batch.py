"""
Generate batch verification commands for the browser tool.
Outputs a JSON list of {url, lat, lng, state, rules, poi_name} for browser automation.
"""
import json
import os

OUTPUT_DIR = r"C:\Users\MJ\Documents\GitHub\PharmacyFinder\output"

with open(os.path.join(OUTPUT_DIR, "to_google_verify.json"), "r", encoding="utf-8") as f:
    to_verify = json.load(f)

print(f"Total to verify: {len(to_verify)}")

# Generate Google Maps URLs
batch = []
for i, opp in enumerate(to_verify):
    lat = opp["lat"]
    lng = opp["lng"]
    # Use zoom level based on search radius
    radius = opp["search_radius_km"]
    if radius <= 0.5:
        zoom = 16
    elif radius <= 1.5:
        zoom = 15
    else:
        zoom = 14
    
    url = f"https://www.google.com/maps/search/pharmacy/@{lat},{lng},{zoom}z"
    batch.append({
        "index": opp["index"],
        "url": url,
        "lat": lat,
        "lng": lng,
        "state": opp["state"],
        "rules": opp["rules"],
        "poi_name": opp["poi_name"],
        "search_radius_km": radius,
        "confidence": opp["confidence"],
    })

# Save batch
batch_path = os.path.join(OUTPUT_DIR, "verify_batch.json")
with open(batch_path, "w", encoding="utf-8") as f:
    json.dump(batch, f, indent=2)

print(f"Saved {len(batch)} URLs to {batch_path}")
print("\nFirst 10:")
for b in batch[:10]:
    print(f"  #{b['index']} {b['state']} | {b['poi_name'][:40]} | {b['url']}")
