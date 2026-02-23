"""
Fetch supermarket floor areas (GLA) from OpenStreetMap Overpass API
for all Australian states/territories, then update PharmacyFinder DB.

Uses POST requests, retry logic, and multiple Overpass endpoints for resilience.
"""

import requests
import json
import math
import time
import sqlite3
import os
import sys
from datetime import datetime
from collections import defaultdict

OVERPASS_ENDPOINTS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://maps.mail.ru/osm/tools/overpass/api/interpreter",
]

STATES = [
    ("AU-ACT", "ACT"),
    ("AU-TAS", "TAS"),
    ("AU-NT", "NT"),
    ("AU-SA", "SA"),
    ("AU-WA", "WA"),
    ("AU-QLD", "QLD"),
    ("AU-VIC", "VIC"),
    ("AU-NSW", "NSW"),
]
RATE_LIMIT_SECONDS = 35
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "national_supermarket_gla.json")
DB_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pharmacy_finder.db")


def query_overpass(state_code, max_retries=3):
    """Query Overpass for all supermarket ways in a state with retry logic."""
    query = f"""[out:json][timeout:300];
area["ISO3166-2"="{state_code}"]->.searchArea;
(
  way["shop"="supermarket"](area.searchArea);
  relation["shop"="supermarket"](area.searchArea);
);
out body;>;out skel qt;"""
    
    for attempt in range(max_retries):
        endpoint = OVERPASS_ENDPOINTS[attempt % len(OVERPASS_ENDPOINTS)]
        print(f"  Attempt {attempt+1}: POST to {endpoint.split('//')[1].split('/')[0]}...", flush=True)
        
        try:
            resp = requests.post(
                endpoint,
                data={"data": query},
                timeout=320,
                headers={"User-Agent": "PharmacyFinder/1.0 (research project)"}
            )
            
            if resp.status_code == 429:
                wait = 60
                print(f"  Rate limited, waiting {wait}s...", flush=True)
                time.sleep(wait)
                continue
            elif resp.status_code == 504:
                print(f"  Gateway timeout, retrying with different endpoint...", flush=True)
                time.sleep(15)
                continue
            
            resp.raise_for_status()
            data = resp.json()
            return data
            
        except requests.exceptions.Timeout:
            print(f"  Request timeout, retrying...", flush=True)
            time.sleep(15)
        except requests.exceptions.RequestException as e:
            print(f"  Error: {e}", flush=True)
            time.sleep(15)
    
    raise Exception(f"Failed to query {state_code} after {max_retries} attempts")


def calc_polygon_area_sqm(coords):
    """Calculate area in sqm using Shoelace formula with lat/lon to meters conversion."""
    if len(coords) < 3:
        return 0.0
    
    center_lat = sum(c[0] for c in coords) / len(coords)
    center_lat_rad = math.radians(center_lat)
    
    lat_to_m = 111320.0
    lon_to_m = 111320.0 * math.cos(center_lat_rad)
    
    center_lon = sum(c[1] for c in coords) / len(coords)
    points_m = []
    for lat, lon in coords:
        x = (lon - center_lon) * lon_to_m
        y = (lat - center_lat) * lat_to_m
        points_m.append((x, y))
    
    n = len(points_m)
    area = 0.0
    for i in range(n):
        j = (i + 1) % n
        area += points_m[i][0] * points_m[j][1]
        area -= points_m[j][0] * points_m[i][1]
    
    return abs(area) / 2.0


def parse_overpass_results(data, state_name):
    """Parse Overpass JSON into supermarket records with calculated areas."""
    elements = data.get("elements", [])
    
    nodes = {}
    for el in elements:
        if el["type"] == "node":
            nodes[el["id"]] = (el["lat"], el["lon"])
    
    ways_lookup = {}
    for el in elements:
        if el["type"] == "way":
            ways_lookup[el["id"]] = el.get("nodes", [])
    
    supermarkets = []
    
    for el in elements:
        if el["type"] == "way" and el.get("tags", {}).get("shop") == "supermarket":
            tags = el.get("tags", {})
            node_ids = el.get("nodes", [])
            
            coords = []
            for nid in node_ids:
                if nid in nodes:
                    coords.append(nodes[nid])
            
            if len(coords) < 3:
                continue
            
            area_sqm = calc_polygon_area_sqm(coords)
            
            if area_sqm < 10 or area_sqm > 100000:
                continue
            
            center_lat = sum(c[0] for c in coords) / len(coords)
            center_lon = sum(c[1] for c in coords) / len(coords)
            
            supermarkets.append({
                "osm_id": el["id"],
                "osm_type": "way",
                "name": tags.get("name", ""),
                "brand": tags.get("brand", tags.get("operator", "")),
                "latitude": round(center_lat, 7),
                "longitude": round(center_lon, 7),
                "area_sqm": round(area_sqm, 1),
                "addr_street": tags.get("addr:street", ""),
                "addr_housenumber": tags.get("addr:housenumber", ""),
                "addr_suburb": tags.get("addr:suburb", ""),
                "addr_city": tags.get("addr:city", ""),
                "addr_postcode": tags.get("addr:postcode", ""),
                "state": state_name,
            })
        
        elif el["type"] == "relation" and el.get("tags", {}).get("shop") == "supermarket":
            tags = el.get("tags", {})
            members = el.get("members", [])
            
            all_coords = []
            for member in members:
                if member["type"] == "way" and member.get("role", "outer") in ("outer", ""):
                    way_nodes = ways_lookup.get(member["ref"], [])
                    for nid in way_nodes:
                        if nid in nodes:
                            all_coords.append(nodes[nid])
            
            if len(all_coords) < 3:
                continue
            
            area_sqm = calc_polygon_area_sqm(all_coords)
            if area_sqm < 10 or area_sqm > 100000:
                continue
            
            center_lat = sum(c[0] for c in all_coords) / len(all_coords)
            center_lon = sum(c[1] for c in all_coords) / len(all_coords)
            
            supermarkets.append({
                "osm_id": el["id"],
                "osm_type": "relation",
                "name": tags.get("name", ""),
                "brand": tags.get("brand", tags.get("operator", "")),
                "latitude": round(center_lat, 7),
                "longitude": round(center_lon, 7),
                "area_sqm": round(area_sqm, 1),
                "addr_street": tags.get("addr:street", ""),
                "addr_housenumber": tags.get("addr:housenumber", ""),
                "addr_suburb": tags.get("addr:suburb", ""),
                "addr_city": tags.get("addr:city", ""),
                "addr_postcode": tags.get("addr:postcode", ""),
                "state": state_name,
            })
    
    return supermarkets


def normalize_brand(name, brand):
    """Normalize brand name for matching."""
    text = (brand + " " + name).lower().strip()
    
    if "woolworths" in text or "woolies" in text:
        return "woolworths"
    elif "coles" in text and "costco" not in text:
        return "coles"
    elif "aldi" in text:
        return "aldi"
    elif "iga" in text:
        if "x-press" in text or "xpress" in text:
            return "iga_xpress"
        elif "everyday" in text:
            return "iga_everyday"
        return "iga"
    elif "costco" in text:
        return "costco"
    elif "foodworks" in text:
        return "foodworks"
    elif "drakes" in text or "drake" in text:
        return "drakes"
    elif "harris farm" in text:
        return "harris_farm"
    elif "foodland" in text:
        return "foodland"
    elif "spar" in text:
        return "spar"
    elif "nqr" in text:
        return "nqr"
    elif "friendly grocer" in text:
        return "friendly_grocer"
    elif "farmer jacks" in text or "farmer jack" in text:
        return "farmer_jacks"
    elif "spudshed" in text:
        return "spudshed"
    elif "supabarn" in text:
        return "supabarn"
    elif "ritchies" in text:
        return "ritchies"
    else:
        return brand.lower().strip() if brand else "unknown"


def haversine_m(lat1, lon1, lat2, lon2):
    """Haversine distance in meters."""
    R = 6371000
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))


def update_database(all_supermarkets):
    """Update pharmacy_finder.db with OSM data."""
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()
    
    cur.execute("SELECT id, name, address, latitude, longitude, floor_area_sqm, brand FROM supermarkets")
    existing = cur.fetchall()
    print(f"\n  Existing DB rows: {len(existing)}", flush=True)
    
    updated = 0
    inserted = 0
    matched_db_ids = set()
    
    for sm in all_supermarkets:
        best_match = None
        best_dist = 100  # 100m threshold
        
        for row in existing:
            row_id, row_name, row_addr, row_lat, row_lon, row_area, row_brand = row
            if row_lat and row_lon and row_id not in matched_db_ids:
                dist = haversine_m(sm["latitude"], sm["longitude"], row_lat, row_lon)
                if dist < best_dist:
                    best_dist = dist
                    best_match = row
        
        norm_brand = normalize_brand(sm["name"], sm["brand"])
        
        if best_match:
            row_id = best_match[0]
            matched_db_ids.add(row_id)
            cur.execute("""
                UPDATE supermarkets 
                SET floor_area_sqm = ?, estimated_gla = ?, gla_confidence = 'osm_measured',
                    brand = COALESCE(NULLIF(?, ''), brand),
                    name = COALESCE(NULLIF(?, ''), name)
                WHERE id = ?
            """, (sm["area_sqm"], sm["area_sqm"], norm_brand, sm["name"], row_id))
            updated += 1
        else:
            addr_parts = []
            if sm["addr_housenumber"]:
                addr_parts.append(sm["addr_housenumber"])
            if sm["addr_street"]:
                addr_parts.append(sm["addr_street"])
            if sm["addr_suburb"]:
                addr_parts.append(sm["addr_suburb"])
            addr_parts.append(sm["state"])
            addr_parts.append("Australia")
            address = ", ".join(addr_parts)
            
            if not address.strip() or address.strip() == ", Australia":
                address = f"OSM-{sm['osm_id']}, {sm['state']}, Australia"
            
            try:
                cur.execute("""
                    INSERT INTO supermarkets (name, address, latitude, longitude, floor_area_sqm, estimated_gla, brand, gla_confidence, date_scraped)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'osm_measured', ?)
                """, (
                    sm["name"] or "Unknown Supermarket",
                    address,
                    sm["latitude"],
                    sm["longitude"],
                    sm["area_sqm"],
                    sm["area_sqm"],
                    norm_brand,
                    datetime.now().isoformat()
                ))
                inserted += 1
            except sqlite3.IntegrityError:
                address = f"{address} (OSM:{sm['osm_id']})"
                try:
                    cur.execute("""
                        INSERT INTO supermarkets (name, address, latitude, longitude, floor_area_sqm, estimated_gla, brand, gla_confidence, date_scraped)
                        VALUES (?, ?, ?, ?, ?, ?, ?, 'osm_measured', ?)
                    """, (
                        sm["name"] or "Unknown Supermarket",
                        address,
                        sm["latitude"],
                        sm["longitude"],
                        sm["area_sqm"],
                        sm["area_sqm"],
                        norm_brand,
                        datetime.now().isoformat()
                    ))
                    inserted += 1
                except sqlite3.IntegrityError:
                    pass
    
    conn.commit()
    
    cur.execute("SELECT COUNT(*) FROM supermarkets")
    total = cur.fetchone()[0]
    conn.close()
    
    print(f"  Updated: {updated}, Inserted: {inserted}, Total DB rows now: {total}", flush=True)
    return updated, inserted


def print_summary(all_supermarkets):
    """Print detailed summary."""
    print("\n" + "="*70, flush=True)
    print(f"  NATIONAL SUPERMARKET GLA SUMMARY")
    print(f"  Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70)
    
    print(f"\n  Total supermarkets with measured GLA: {len(all_supermarkets)}")
    
    by_state = defaultdict(list)
    for sm in all_supermarkets:
        by_state[sm["state"]].append(sm)
    
    print(f"\n  {'State':<8} {'Count':>6} {'Avg GLA (sqm)':>15} {'Min':>8} {'Max':>8}")
    print(f"  {'-'*6:<8} {'-'*5:>6} {'-'*13:>15} {'-'*6:>8} {'-'*6:>8}")
    for state in ["NSW", "VIC", "QLD", "WA", "SA", "TAS", "NT", "ACT"]:
        sms = by_state.get(state, [])
        if sms:
            areas = [s["area_sqm"] for s in sms]
            print(f"  {state:<8} {len(sms):>6} {sum(areas)/len(areas):>15.1f} {min(areas):>8.1f} {max(areas):>8.1f}")
    
    by_brand = defaultdict(list)
    for sm in all_supermarkets:
        brand = normalize_brand(sm["name"], sm["brand"])
        by_brand[brand].append(sm)
    
    sorted_brands = sorted(by_brand.items(), key=lambda x: len(x[1]), reverse=True)
    
    print(f"\n  {'Brand':<25} {'Count':>6} {'Avg GLA (sqm)':>15} {'Min':>8} {'Max':>8}")
    print(f"  {'-'*23:<25} {'-'*5:>6} {'-'*13:>15} {'-'*6:>8} {'-'*6:>8}")
    for brand, sms in sorted_brands[:25]:
        areas = [s["area_sqm"] for s in sms]
        print(f"  {brand:<25} {len(sms):>6} {sum(areas)/len(areas):>15.1f} {min(areas):>8.1f} {max(areas):>8.1f}")
    
    if len(sorted_brands) > 25:
        remaining = sum(len(sms) for _, sms in sorted_brands[25:])
        print(f"  ... and {len(sorted_brands) - 25} more brands ({remaining} stores)")
    
    print(flush=True)


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    all_supermarkets = []
    
    for i, (state_code, state_name) in enumerate(STATES):
        print(f"\n[{i+1}/{len(STATES)}] Processing {state_name} ({state_code})...", flush=True)
        
        try:
            data = query_overpass(state_code)
            elements_count = len(data.get("elements", []))
            print(f"  Got {elements_count} elements from Overpass", flush=True)
            
            supermarkets = parse_overpass_results(data, state_name)
            print(f"  Parsed {len(supermarkets)} supermarkets with valid polygons", flush=True)
            
            all_supermarkets.extend(supermarkets)
            
        except Exception as e:
            print(f"  ERROR for {state_name}: {e}", flush=True)
            import traceback
            traceback.print_exc()
        
        if i < len(STATES) - 1:
            print(f"  Waiting {RATE_LIMIT_SECONDS}s (rate limit)...", flush=True)
            time.sleep(RATE_LIMIT_SECONDS)
    
    print(f"\n{'='*50}", flush=True)
    print(f"Total supermarkets collected: {len(all_supermarkets)}", flush=True)
    
    # Save JSON
    print(f"\nSaving to {OUTPUT_FILE}...", flush=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump({
            "generated": datetime.now().isoformat(),
            "total_count": len(all_supermarkets),
            "supermarkets": all_supermarkets
        }, f, indent=2, ensure_ascii=False)
    print(f"  Saved {len(all_supermarkets)} records", flush=True)
    
    # Update DB
    print(f"\nUpdating database {DB_FILE}...", flush=True)
    updated, inserted = update_database(all_supermarkets)
    
    # Print summary
    print_summary(all_supermarkets)


if __name__ == "__main__":
    main()
