"""
Quick national supermarket GLA from OSM Overpass.
Single query, no brand enrichment, just save what we get.
"""
import requests, json, math, sqlite3, os, sys
from collections import defaultdict
from datetime import datetime

sys.stdout.reconfigure(line_buffering=True)

OVERPASS_URL = "https://overpass.kumi.systems/api/interpreter"  # alternate endpoint
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")
DB_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pharmacy_finder.db")

def polygon_area_sqm(coords):
    n = len(coords)
    if n < 3: return 0
    center_lat = sum(c[0] for c in coords) / n
    lat_to_m = 111320.0
    lon_to_m = 111320.0 * math.cos(math.radians(center_lat))
    points = [((lon - coords[0][1]) * lon_to_m, (lat - coords[0][0]) * lat_to_m) for lat, lon in coords]
    area = sum(points[i][0]*points[(i+1)%n][1] - points[(i+1)%n][0]*points[i][1] for i in range(n))
    return abs(area) / 2.0

def infer_brand(name):
    name_lower = (name or "").lower()
    brands = [
        ("woolworths", "woolworths"), ("coles", "coles"), ("aldi", "aldi"),
        ("iga x-press", "iga_express"), ("iga xpress", "iga_express"), ("iga express", "iga_express"),
        ("iga everyday", "iga_everyday"), ("iga", "iga"),
        ("foodworks", "foodworks"), ("spar", "spar"), ("drakes", "drakes"),
        ("harris farm", "harris_farm"), ("foodland", "foodland"),
    ]
    for keyword, brand in brands:
        if keyword in name_lower:
            return brand
    return "independent"

print(f"National Supermarket GLA Calculator - {datetime.now()}")
print("=" * 60)

# Single query for all Australian supermarkets
query = """[out:json][timeout:300];
area["ISO3166-1"="AU"]->.au;
(way["shop"="supermarket"](area.au););
out body;>;out skel qt;"""

print("Querying Overpass for all Australian supermarkets...")
resp = requests.post(OVERPASS_URL, data={"data": query}, timeout=360)
resp.raise_for_status()
data = resp.json()
elements = data.get("elements", [])
print(f"Got {len(elements)} elements")

# Parse nodes and ways
nodes = {}
supermarkets = []
for el in elements:
    if el["type"] == "node":
        nodes[el["id"]] = (el["lat"], el["lon"])

for el in elements:
    if el["type"] == "way" and "tags" in el:
        tags = el.get("tags", {})
        coords = [nodes[nid] for nid in el.get("nodes", []) if nid in nodes]
        if len(coords) >= 3:
            area = round(polygon_area_sqm(coords))
            if area < 10 or area > 50000:  # filter nonsense
                continue
            center_lat = sum(c[0] for c in coords) / len(coords)
            center_lon = sum(c[1] for c in coords) / len(coords)
            name = tags.get("name", "")
            brand = tags.get("brand", "") or infer_brand(name)
            supermarkets.append({
                "osm_id": el["id"],
                "name": name,
                "brand": infer_brand(name),
                "brand_tag": tags.get("brand", ""),
                "lat": round(center_lat, 6),
                "lon": round(center_lon, 6),
                "area_sqm": area,
                "addr_street": tags.get("addr:street", ""),
                "addr_number": tags.get("addr:housenumber", ""),
                "addr_suburb": tags.get("addr:suburb", ""),
                "addr_state": tags.get("addr:state", ""),
            })

print(f"\nFound {len(supermarkets)} supermarkets with building footprints")

# Save to JSON
os.makedirs(OUTPUT_DIR, exist_ok=True)
output_path = os.path.join(OUTPUT_DIR, "national_supermarket_gla.json")
with open(output_path, "w") as f:
    json.dump(supermarkets, f, indent=2)
print(f"Saved to {output_path}")

# Summary by brand
brands = defaultdict(list)
for s in supermarkets:
    brands[s["brand"]].append(s["area_sqm"])

print(f"\n{'='*60}")
print(f"BRAND SUMMARY")
print(f"{'='*60}")
for brand, areas in sorted(brands.items(), key=lambda x: -len(x[1])):
    avg = sum(areas) / len(areas)
    print(f"  {brand:20s}: {len(areas):4d} stores | avg {avg:,.0f} sqm | range {min(areas):,}-{max(areas):,}")

# State summary (rough, from coordinates)
def guess_state(lat, lon):
    if lat > -20: return "NT/QLD"
    if lat > -29:
        if lon < 138: return "WA"
        if lon < 141: return "SA"
        return "QLD"  
    if lat > -34:
        if lon < 129: return "WA"
        if lon < 141: return "SA/NSW"
        return "NSW"
    if lat > -39:
        if lon < 129: return "WA"
        if lon < 141: return "SA"
        if lon < 147: return "VIC"
        return "NSW/ACT"
    if lat > -44: return "VIC/TAS"
    return "TAS"

states = defaultdict(int)
for s in supermarkets:
    states[guess_state(s["lat"], s["lon"])] += 1
print(f"\nApprox by state:")
for st, cnt in sorted(states.items(), key=lambda x: -x[1]):
    print(f"  {st}: {cnt}")

# Update DB
print(f"\nUpdating database...")
conn = sqlite3.connect(DB_FILE)
c = conn.cursor()

# Match existing TAS supermarkets by proximity
c.execute("SELECT id, name, latitude, longitude FROM supermarkets")
existing = c.fetchall()
matched = 0
for ex_id, ex_name, ex_lat, ex_lon in existing:
    best_dist = 999999
    best_sm = None
    for sm in supermarkets:
        dlat = (sm["lat"] - ex_lat) * 111320
        dlon = (sm["lon"] - ex_lon) * 111320 * math.cos(math.radians(ex_lat))
        dist = math.sqrt(dlat**2 + dlon**2)
        if dist < best_dist:
            best_dist = dist
            best_sm = sm
    if best_sm and best_dist < 150:
        c.execute("UPDATE supermarkets SET floor_area_sqm=?, estimated_gla=?, gla_confidence=? WHERE id=?",
                  (best_sm["area_sqm"], best_sm["area_sqm"], "measured_osm", ex_id))
        matched += 1

conn.commit()
print(f"Updated {matched}/{len(existing)} existing supermarkets in DB")

# Size distribution
print(f"\nSize distribution:")
brackets = [(0,200,"<200"),(200,500,"200-500"),(500,1000,"500-1k"),(1000,2000,"1k-2k"),(2000,3500,"2k-3.5k"),(3500,5000,"3.5k-5k"),(5000,50000,">5k")]
for lo, hi, label in brackets:
    cnt = sum(1 for s in supermarkets if lo <= s["area_sqm"] < hi)
    print(f"  {label:10s}: {cnt}")

conn.close()
print(f"\nDone! {datetime.now()}")
