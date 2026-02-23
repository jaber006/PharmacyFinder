import sqlite3
import requests
import math
import json

DB_PATH = "pharmacy_finder.db"
OVERPASS_URL = "https://overpass-api.de/api/interpreter"

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

# Find George Town supermarket
c.execute("SELECT id, name, address, latitude, longitude, floor_area_sqm, estimated_gla, brand FROM supermarkets WHERE name LIKE '%george%' OR address LIKE '%george%'")
rows = c.fetchall()
print("=== George Town Supermarkets ===")
for r in rows:
    print(f"  id={r[0]} | {r[1]} | {r[2]} | ({r[3]},{r[4]}) | floor={r[5]} | est_gla={r[6]} | brand={r[7]}")

# Also check opportunities
c.execute("SELECT id, poi_name, address, latitude, longitude, qualifying_rules FROM opportunities WHERE address LIKE '%george%' OR poi_name LIKE '%george%'")
opps = c.fetchall()
print(f"\n=== George Town Opportunities ===")
for o in opps:
    print(f"  id={o[0]} | {o[1]} | {o[2]} | ({o[3]},{o[4]}) | rules={o[5]}")

conn.close()

# If we found coords, do a detailed OSM query
if rows:
    lat, lon = rows[0][3], rows[0][4]
    print(f"\n=== OSM Query for ({lat}, {lon}) ===")
    
    # Wider search, get ALL buildings
    query = f"""
    [out:json][timeout:25];
    (
      way["building"](around:150,{lat},{lon});
      way["shop"](around:150,{lat},{lon});
      relation["building"](around:150,{lat},{lon});
    );
    out body;
    >;
    out skel qt;
    """
    
    resp = requests.post(OVERPASS_URL, data={"data": query}, timeout=30)
    data = resp.json()
    
    nodes = {}
    for el in data["elements"]:
        if el["type"] == "node":
            nodes[el["id"]] = (el["lat"], el["lon"])
    
    buildings = []
    for el in data["elements"]:
        if el["type"] == "way" and "tags" in el:
            tags = el.get("tags", {})
            coords = []
            for nid in el.get("nodes", []):
                if nid in nodes:
                    coords.append(nodes[nid])
            if len(coords) >= 3:
                # Calculate area
                n = len(coords)
                center_lat = sum(c[0] for c in coords) / n
                lat_to_m = 111320.0
                lon_to_m = 111320.0 * math.cos(math.radians(center_lat))
                points = [((_lon - coords[0][1]) * lon_to_m, (_lat - coords[0][0]) * lat_to_m) for _lat, _lon in coords]
                area = abs(sum(points[i][0]*points[(i+1)%n][1] - points[(i+1)%n][0]*points[i][1] for i in range(n))) / 2.0
                
                # Distance from target
                R = 6371000
                dlat = math.radians(sum(c[0] for c in coords)/n - lat)
                dlon = math.radians(sum(c[1] for c in coords)/n - lon)
                a = math.sin(dlat/2)**2 + math.cos(math.radians(lat)) * math.cos(math.radians(sum(c[0] for c in coords)/n)) * math.sin(dlon/2)**2
                dist = R * 2 * math.asin(math.sqrt(a))
                
                buildings.append({
                    "name": tags.get("name", "unnamed"),
                    "area_sqm": round(area),
                    "distance_m": round(dist),
                    "tags": {k:v for k,v in tags.items() if k in ["building","shop","name","brand","addr:street","addr:housenumber"]}
                })
    
    buildings.sort(key=lambda b: b["distance_m"])
    print(f"\nFound {len(buildings)} buildings within 150m:")
    for b in buildings:
        print(f"  {b['distance_m']:4d}m | {b['area_sqm']:6d} sqm | {b['name']:30s} | {b['tags']}")
