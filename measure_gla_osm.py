"""
Measure supermarket GLA from OpenStreetMap building footprints.
For single-storey buildings, roof footprint area ≈ floor area.

Uses Overpass API to find building polygons at each supermarket's coordinates,
then calculates the area using the Shapely library.
"""
import sqlite3
import requests
import time
import json
import math
import sys

# Force unbuffered output
sys.stdout.reconfigure(line_buffering=True)

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
DB_PATH = "pharmacy_finder.db"

def get_building_area_osm(lat, lon, name="", radius=50):
    """
    Query OSM Overpass for building footprints near a coordinate.
    Returns area in sqm if found, None otherwise.
    """
    # Search for buildings within radius meters
    # Prioritise shop=supermarket tags, then general buildings
    query = f"""
    [out:json][timeout:25];
    (
      way["shop"="supermarket"](around:{radius},{lat},{lon});
      way["shop"="convenience"](around:{radius},{lat},{lon});
      way["building"]["name"~"IGA|Coles|Woolworths|Foodworks|ALDI|Spar",i](around:{radius},{lat},{lon});
      way["building"](around:{radius},{lat},{lon});
      relation["building"](around:{radius},{lat},{lon});
    );
    out body;
    >;
    out skel qt;
    """
    
    try:
        resp = requests.post(OVERPASS_URL, data={"data": query}, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"  API error: {e}")
        return None
    
    elements = data.get("elements", [])
    
    # Collect nodes for coordinate lookup
    nodes = {}
    for el in elements:
        if el["type"] == "node":
            nodes[el["id"]] = (el["lat"], el["lon"])
    
    # Find ways (building polygons)
    buildings = []
    for el in elements:
        if el["type"] == "way" and "tags" in el:
            tags = el.get("tags", {})
            if "building" in tags or tags.get("shop") == "supermarket":
                # Get polygon coordinates
                coords = []
                for nid in el.get("nodes", []):
                    if nid in nodes:
                        coords.append(nodes[nid])
                if len(coords) >= 3:
                    area = polygon_area_sqm(coords)
                    # Check if it's tagged as a supermarket or shop
                    is_supermarket = (
                        tags.get("shop") == "supermarket" or
                        "supermarket" in tags.get("name", "").lower() or
                        "iga" in tags.get("name", "").lower() or
                        "coles" in tags.get("name", "").lower() or
                        "woolworths" in tags.get("name", "").lower() or
                        "foodworks" in tags.get("name", "").lower()
                    )
                    dist = haversine(lat, lon, 
                                    sum(c[0] for c in coords)/len(coords),
                                    sum(c[1] for c in coords)/len(coords))
                    buildings.append({
                        "area": area,
                        "tags": tags,
                        "is_supermarket": is_supermarket,
                        "distance_m": dist,
                        "name": tags.get("name", "unknown")
                    })
    
    if not buildings:
        return None
    
    # Prefer buildings tagged as supermarket/shop
    supermarkets = [b for b in buildings if b["is_supermarket"]]
    if supermarkets:
        # Pick closest supermarket-tagged building
        best = min(supermarkets, key=lambda b: b["distance_m"])
        return best
    
    # Otherwise look for buildings in typical supermarket size range (200-6000 sqm)
    # This avoids picking entire shopping centres
    close_buildings = [b for b in buildings if b["distance_m"] < 80]
    supermarket_sized = [b for b in close_buildings if 200 <= b["area"] <= 6000]
    if supermarket_sized:
        # Pick the one closest to the coordinate
        best = min(supermarket_sized, key=lambda b: b["distance_m"])
        return best
    
    # If nothing in supermarket range, pick closest small-medium building
    if close_buildings:
        # Sort by distance, prefer reasonable sizes
        reasonable = [b for b in close_buildings if b["area"] < 10000]
        if reasonable:
            best = min(reasonable, key=lambda b: b["distance_m"])
            return best
    
    return None


def polygon_area_sqm(coords):
    """
    Calculate area of a polygon given as [(lat,lon), ...] using the
    Shoelace formula with meter conversion.
    """
    n = len(coords)
    if n < 3:
        return 0
    
    # Convert to approximate meters using center point
    center_lat = sum(c[0] for c in coords) / n
    lat_to_m = 111320.0  # meters per degree latitude
    lon_to_m = 111320.0 * math.cos(math.radians(center_lat))
    
    # Convert to meters relative to first point
    points = []
    for lat, lon in coords:
        x = (lon - coords[0][1]) * lon_to_m
        y = (lat - coords[0][0]) * lat_to_m
        points.append((x, y))
    
    # Shoelace formula
    area = 0
    for i in range(n):
        j = (i + 1) % n
        area += points[i][0] * points[j][1]
        area -= points[j][0] * points[i][1]
    
    return abs(area) / 2.0


def haversine(lat1, lon1, lat2, lon2):
    """Distance in meters between two points."""
    R = 6371000
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))


def main():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    c.execute("SELECT id, name, address, latitude, longitude, floor_area_sqm, estimated_gla, brand, gla_confidence FROM supermarkets")
    supermarkets = c.fetchall()
    
    print(f"Processing {len(supermarkets)} supermarkets via OSM Overpass API...\n")
    
    results = {"found": 0, "not_found": 0, "errors": 0, "details": [], "updates": []}
    
    for i, sm in enumerate(supermarkets):
        sm_id, name, address, lat, lon, floor_area, est_gla, brand, confidence = sm
        print(f"[{i+1}/{len(supermarkets)}] {name} ({brand}) @ {address}")
        print(f"  Current: est_gla={est_gla}, confidence={confidence}")
        
        result = get_building_area_osm(lat, lon, name)
        
        if result:
            osm_area = round(result["area"])
            osm_name = result.get("name", "")
            is_super = result["is_supermarket"]
            dist = round(result["distance_m"])
            
            # Determine confidence
            if is_super:
                new_confidence = "high"
                source = "osm_supermarket"
            else:
                new_confidence = "medium"
                source = "osm_building"
            
            print(f"  [OK] OSM: {osm_area} sqm (tagged: {osm_name}, dist: {dist}m, supermarket_tag: {is_super})")
            
            # Queue DB update (write at end to avoid locks)
            results["updates"].append((osm_area, osm_area, f"{new_confidence}_osm", sm_id))
            
            results["found"] += 1
            results["details"].append({
                "id": sm_id, "name": name, "brand": brand,
                "old_gla": est_gla, "new_gla": osm_area,
                "osm_name": osm_name, "is_supermarket_tag": is_super,
                "distance_m": dist, "confidence": new_confidence
            })
        else:
            print(f"  [--] No building found in OSM")
            results["not_found"] += 1
        
        # Rate limit - be nice to Overpass API
        time.sleep(1.5)
    
    # Batch update DB
    print(f"\nWriting {len(results['updates'])} updates to database...")
    for upd in results["updates"]:
        c.execute("""UPDATE supermarkets 
                    SET floor_area_sqm = ?, estimated_gla = ?, gla_confidence = ?
                    WHERE id = ?""", upd)
    conn.commit()
    conn.close()
    print("Database updated.")
    
    # Summary
    print(f"\n{'='*60}")
    print(f"RESULTS SUMMARY")
    print(f"{'='*60}")
    print(f"Found:     {results['found']}/{len(supermarkets)}")
    print(f"Not found: {results['not_found']}/{len(supermarkets)}")
    
    # Show significant changes
    print(f"\n--- Significant GLA Changes ---")
    for d in results["details"]:
        old = d["old_gla"] or 0
        new = d["new_gla"]
        if old > 0:
            change_pct = ((new - old) / old) * 100
            if abs(change_pct) > 20:
                print(f"  {d['brand']:12s} {d['name']:35s}: {old:.0f} → {new} sqm ({change_pct:+.0f}%)")
    
    # Show rule impact
    print(f"\n--- Rule Impact (threshold crossings) ---")
    thresholds = [
        (500, "Item 134 (≥500sqm)"),
        (1000, "Item 132/134A (≥1,000sqm)"),
        (2500, "Item 133 (≥2,500sqm)")
    ]
    for d in results["details"]:
        old = d["old_gla"] or 0
        new = d["new_gla"]
        for thresh, rule in thresholds:
            if (old >= thresh and new < thresh):
                print(f"  [!!] LOST: {d['name']} -- was {old:.0f}, now {new} sqm -- no longer qualifies for {rule}")
            elif (old < thresh and new >= thresh):
                print(f"  [++] GAINED: {d['name']} -- was {old:.0f}, now {new} sqm -- now qualifies for {rule}")
    
    # Save detailed results
    with open("output/osm_gla_results.json", "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nDetailed results saved to output/osm_gla_results.json")


if __name__ == "__main__":
    main()
