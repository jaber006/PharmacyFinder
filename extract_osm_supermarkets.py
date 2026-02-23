"""
National Supermarket GLA Calculator v3 - Overpass API
=====================================================
Uses the Overpass API to bulk-extract ALL supermarkets in Australia,
then calculates building footprint areas from the polygon geometry.

Approach: Query all shop=supermarket ways + nodes in Australia in one call.
The Overpass API can handle this for a single country.
"""
import requests
import json
import math
import sqlite3
import csv
import os
import sys
import time
from collections import defaultdict
from datetime import datetime

sys.stdout.reconfigure(line_buffering=True)

OUTPUT_JSON = r"output\national_supermarket_gla.json"
OUTPUT_CSV = r"output\national_supermarket_gla.csv"
DB_PATH = "pharmacy_finder.db"

# Australia bounding box (generous)
AU_BBOX = "-44.0,112.0,-9.0,154.5"

OVERPASS_URLS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://maps.mail.ru/osm/tools/overpass/api/interpreter",
]

SUPERMARKET_BRANDS = {
    'woolworths': ['woolworths', 'woolies'],
    'coles': ['coles'],
    'aldi': ['aldi'],
    'iga_express': ['iga express', 'iga x-press', 'iga xpress'],
    'iga_everyday': ['iga everyday'],
    'iga': ['iga'],
    'foodworks': ['foodworks', 'food works'],
    'spar': ['spar'],
    'drakes': ['drakes', "drake's"],
    'costco': ['costco'],
    'harris_farm': ['harris farm'],
    'ritchies': ["ritchie's", 'ritchies'],
    'friendly_grocer': ['friendly grocer'],
    'foodland': ['foodland'],
    'supabarn': ['supabarn'],
    'nqr': ['nqr', 'not quite right'],
    'fresh_provisions': ['fresh provisions'],
    'romeos': ["romeo's", 'romeos'],
    'independent': [],
}


def classify_brand(name, brand_tag=None):
    combined = ((brand_tag or '') + ' ' + (name or '')).lower().strip()
    for bk in ['iga_express', 'iga_everyday']:
        for pat in SUPERMARKET_BRANDS[bk]:
            if pat in combined:
                return bk
    for bk, pats in SUPERMARKET_BRANDS.items():
        if bk in ('independent', 'iga_express', 'iga_everyday'):
            continue
        for pat in pats:
            if pat in combined:
                return bk
    return 'independent'


def compute_polygon_area(coords):
    """coords = list of (lon, lat) tuples."""
    if len(coords) < 3:
        return 0.0
    lons = [c[0] for c in coords]
    lats = [c[1] for c in coords]
    clat = sum(lats) / len(lats)
    clon = sum(lons) / len(lons)
    clr = math.radians(clat)
    lon_m = 111320.0 * math.cos(clr)
    lat_m = 111320.0
    xs = [(lon - clon) * lon_m for lon in lons]
    ys = [(lat - clat) * lat_m for lat in lats]
    n = len(xs)
    a = sum(xs[i] * ys[(i+1) % n] - xs[(i+1) % n] * ys[i] for i in range(n))
    return abs(a) / 2.0


def extract_address(tags):
    parts = []
    num = tags.get('addr:housenumber', '')
    street = tags.get('addr:street', '')
    suburb = tags.get('addr:suburb', tags.get('addr:city', ''))
    state = tags.get('addr:state', '')
    postcode = tags.get('addr:postcode', '')
    if num and street:
        parts.append(f"{num} {street}")
    elif street:
        parts.append(street)
    if suburb:
        parts.append(suburb)
    if state:
        parts.append(state)
    if postcode:
        parts.append(postcode)
    return ', '.join(parts)


def query_overpass(query, timeout=300, http_timeout=360):
    """Run an Overpass query with fallback URLs."""
    for url in OVERPASS_URLS:
        try:
            print(f"  Trying {url}...")
            resp = requests.post(url, data={"data": query}, timeout=http_timeout)
            if resp.status_code == 200:
                data = resp.json()
                print(f"  Success! Got {len(data.get('elements', []))} elements")
                return data
            elif resp.status_code == 429:
                print(f"  Rate limited, waiting 30s...")
                time.sleep(30)
            elif resp.status_code == 504 or resp.status_code == 400:
                print(f"  HTTP {resp.status_code} (timeout/error), trying next...")
            else:
                print(f"  HTTP {resp.status_code}: {resp.text[:200]}")
        except requests.exceptions.Timeout:
            print(f"  Request timed out after {http_timeout}s, trying next...")
        except Exception as e:
            print(f"  Error: {e}")
    return None


def fetch_all_supermarkets():
    """Fetch all supermarkets in Australia from Overpass."""
    
    # Main query: all shop=supermarket (ways, nodes, relations)
    # Uses out:json with geometry for ways to get coordinates directly
    query = f"""
[out:json][timeout:300][bbox:{AU_BBOX}];
(
  node["shop"="supermarket"];
  way["shop"="supermarket"];
  relation["shop"="supermarket"];
);
out body;
>;
out skel qt;
"""
    
    print("Fetching shop=supermarket from Overpass API...")
    data = query_overpass(query)
    
    if not data:
        print("ERROR: Failed to fetch from Overpass API!")
        return [], []
    
    return parse_overpass_response(data)


def fetch_branded_supermarkets():
    """Fetch supermarkets by brand name (catches ones not tagged as shop=supermarket)."""
    
    # Split into smaller queries to avoid timeout
    brands = ["Woolworths", "Coles", "ALDI", "IGA", "Foodworks", "Drakes", "Foodland"]
    all_areas = []
    all_points = []
    
    for brand in brands:
        query = f"""
[out:json][timeout:120][bbox:{AU_BBOX}];
(
  way["brand"~"{brand}",i]["shop"!="supermarket"];
  node["brand"~"{brand}",i]["shop"!="supermarket"];
  way["name"~"{brand}",i]["shop"]["shop"!="supermarket"];
  node["name"~"{brand}",i]["shop"]["shop"!="supermarket"];
);
out body;
>;
out skel qt;
"""
        print(f"\n  Checking brand: {brand}...")
        data = query_overpass(query, timeout=120, http_timeout=150)
        if data:
            a, p = parse_overpass_response(data)
            all_areas.extend(a)
            all_points.extend(p)
        time.sleep(2)  # Be nice to Overpass
    
    return all_areas, all_points


def parse_overpass_response(data):
    """Parse Overpass JSON response into supermarket records."""
    elements = data.get('elements', [])
    
    # Separate nodes, ways, relations
    nodes = {}
    ways = []
    relations = []
    point_supers = []
    
    for el in elements:
        if el['type'] == 'node':
            nodes[el['id']] = (el['lon'], el['lat'])
            # Check if it's a tagged supermarket node
            if 'tags' in el:
                tags = el['tags']
                if tags.get('shop') == 'supermarket' or any(kw in (tags.get('name', '') + tags.get('brand', '')).lower() 
                    for kw in ['woolworths', 'coles', 'aldi', 'iga', 'foodworks']):
                    point_supers.append({
                        'osm_id': f"node/{el['id']}",
                        'name': tags.get('name', ''),
                        'brand': tags.get('brand', ''),
                        'lat': el['lat'],
                        'lon': el['lon'],
                        'area_sqm': None,
                        'address': extract_address(tags),
                        'operator': tags.get('operator', ''),
                        'source': 'node',
                    })
        elif el['type'] == 'way' and 'tags' in el:
            ways.append(el)
        elif el['type'] == 'relation' and 'tags' in el:
            relations.append(el)
    
    print(f"  Parsed: {len(nodes):,} nodes, {len(ways)} tagged ways, {len(relations)} relations, {len(point_supers)} point supermarkets")
    
    # Process ways into supermarket records
    area_supers = []
    for w in ways:
        tags = w['tags']
        node_refs = w.get('nodes', [])
        
        coords = []
        for nref in node_refs:
            if nref in nodes:
                coords.append(nodes[nref])
        
        if len(coords) < 3:
            continue
        
        area = compute_polygon_area(coords)
        lats = [c[1] for c in coords]
        lons = [c[0] for c in coords]
        
        area_supers.append({
            'osm_id': f"way/{w['id']}",
            'name': tags.get('name', ''),
            'brand': tags.get('brand', ''),
            'lat': round(sum(lats) / len(lats), 6),
            'lon': round(sum(lons) / len(lons), 6),
            'area_sqm': round(area, 1),
            'address': extract_address(tags),
            'operator': tags.get('operator', ''),
            'source': 'way',
        })
    
    # Process relations (multipolygons)
    for r in relations:
        tags = r['tags']
        members = r.get('members', [])
        
        # Collect all outer way coords
        all_coords = []
        for m in members:
            if m['type'] == 'way' and m.get('role', 'outer') == 'outer':
                # Find the way's nodes (they should be in our node dict from the >;)
                # For relations, the way refs are in the response as skeleton elements
                pass
        
        # For now, if we have geometry info from member nodes, use it
        # Relations are a small fraction; we'll get the key data from ways
    
    print(f"  Built {len(area_supers)} way-based supermarkets with area")
    return area_supers, point_supers


def merge_and_dedup(all_areas, all_points):
    """Merge multiple result sets, deduplicate by osm_id and proximity."""
    seen_ids = set()
    merged_areas = []
    merged_points = []
    
    for s in all_areas:
        if s['osm_id'] not in seen_ids:
            seen_ids.add(s['osm_id'])
            merged_areas.append(s)
    
    for s in all_points:
        if s['osm_id'] not in seen_ids:
            seen_ids.add(s['osm_id'])
            merged_points.append(s)
    
    # Now merge points with areas (skip points near areas)
    results = list(merged_areas)
    added = skipped = 0
    for pt in merged_points:
        is_dup = False
        for area in merged_areas:
            if abs(pt['lat'] - area['lat']) < 0.0005 and abs(pt['lon'] - area['lon']) < 0.0005:
                is_dup = True
                if not area['name'] and pt['name']:
                    area['name'] = pt['name']
                if not area['brand'] and pt['brand']:
                    area['brand'] = pt['brand']
                break
        if not is_dup:
            results.append(pt)
            added += 1
        else:
            skipped += 1
    
    print(f"\nMerge: {len(merged_areas)} areas + {added} unique points ({skipped} dup points skipped)")
    return results


def save_results(supermarkets, json_path, csv_path):
    os.makedirs(os.path.dirname(json_path), exist_ok=True)
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(supermarkets, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(supermarkets)} supermarkets to {json_path}")
    keys = ['osm_id', 'name', 'brand', 'brand_classified', 'lat', 'lon',
            'area_sqm', 'address', 'operator', 'source']
    with open(csv_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(supermarkets)
    print(f"Saved CSV to {csv_path}")


def update_database(supermarkets, db_path):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute("SELECT id, name, latitude, longitude, brand, estimated_gla FROM supermarkets")
    db_supers = c.fetchall()
    print(f"\nUpdating DB: {len(db_supers)} existing supermarkets")
    
    tas_osm = [s for s in supermarkets
                if s['lat'] and s['lon']
                and -44.0 <= s['lat'] <= -39.0 and 143.0 <= s['lon'] <= 149.5
                and s['area_sqm'] and s['area_sqm'] > 0]
    print(f"OSM supermarkets in TAS with area: {len(tas_osm)}")
    
    matched = updated = 0
    for db_id, db_name, db_lat, db_lon, db_brand, db_gla in db_supers:
        if not db_lat or not db_lon:
            continue
        best_dist = float('inf')
        best_osm = None
        for osm_s in tas_osm:
            dlat = (db_lat - osm_s['lat']) * 111320
            dlon = (db_lon - osm_s['lon']) * 111320 * math.cos(math.radians(db_lat))
            dist = math.sqrt(dlat**2 + dlon**2)
            if dist < best_dist:
                best_dist = dist
                best_osm = osm_s
        if best_osm and best_dist < 150:
            matched += 1
            a = best_osm['area_sqm']
            c.execute("UPDATE supermarkets SET floor_area_sqm=?, estimated_gla=?, gla_confidence='osm_measured' WHERE id=?",
                      (a, a, db_id))
            updated += 1
            old = db_gla or 0
            diff = a - old
            arrow = "^" if diff > 0 else "v"
            print(f"  OK {(db_name or 'Unknown')[:40]:40s} | old: {old:>6.0f} → OSM: {a:>7.0f}sqm ({arrow}{abs(diff):.0f}) | {best_dist:.0f}m")
    
    conn.commit()
    conn.close()
    print(f"\nDB update complete: {matched} matched, {updated} updated / {len(db_supers)} total")
    return matched, updated


def print_summary(supermarkets):
    print("\n" + "="*80)
    print("  NATIONAL SUPERMARKET GLA SUMMARY - OSM Data")
    print("="*80)
    
    total = len(supermarkets)
    with_area = [s for s in supermarkets if s.get('area_sqm') and s['area_sqm'] > 0]
    
    print(f"\nTotal supermarkets found: {total:,}")
    print(f"  With measured footprint: {len(with_area):,}")
    print(f"  Point-only (no polygon): {total - len(with_area):,}")
    
    if with_area:
        all_areas = [s['area_sqm'] for s in with_area]
        all_areas.sort()
        print(f"\nOverall stats (measured only):")
        print(f"  Mean:   {sum(all_areas)/len(all_areas):,.0f} sqm")
        print(f"  Median: {all_areas[len(all_areas)//2]:,.0f} sqm")
        print(f"  Min:    {all_areas[0]:,.0f} sqm  Max: {all_areas[-1]:,.0f} sqm")
    
    brand_stats = defaultdict(lambda: {'count': 0, 'areas': []})
    for s in supermarkets:
        b = s.get('brand_classified', 'unknown')
        brand_stats[b]['count'] += 1
        if s.get('area_sqm') and s['area_sqm'] > 0:
            brand_stats[b]['areas'].append(s['area_sqm'])
    
    print(f"\n{'Brand':<20} {'Count':>6} {'W/Area':>7} {'Avg':>8} {'Min':>8} {'Med':>8} {'Max':>8}")
    print("-"*73)
    for brand, stats in sorted(brand_stats.items(), key=lambda x: -x[1]['count']):
        cnt = stats['count']
        areas = sorted(stats['areas'])
        na = len(areas)
        if na > 0:
            avg = sum(areas) / na
            med = areas[na // 2]
            print(f"{brand:<20} {cnt:>6} {na:>7} {avg:>7.0f}sqm {areas[0]:>7.0f} {med:>7.0f} {areas[-1]:>7.0f}")
        else:
            print(f"{brand:<20} {cnt:>6} {na:>7}     N/A      N/A     N/A     N/A")
    
    print(f"\nSize Distribution (measured supermarkets):")
    buckets = [
        (0, 200, "< 200sqm (tiny)"),
        (200, 500, "200-500sqm (express)"),
        (500, 1000, "500-1000sqm (small)"),
        (1000, 2500, "1000-2500sqm (medium)"),
        (2500, 5000, "2500-5000sqm (large)"),
        (5000, 10000, "5000-10000sqm (very large)"),
        (10000, 999999, "> 10000sqm (warehouse)"),
    ]
    for lo, hi, label in buckets:
        n = sum(1 for s in with_area if lo <= s['area_sqm'] < hi)
        bar = "#" * max(1, n // 5) if n > 0 else ""
        pct = n / len(with_area) * 100 if with_area else 0
        print(f"  {label:<30} {n:>5} ({pct:4.1f}%)  {bar}")
    
    states = {
        'TAS': (-44.0, -39.5, 143.5, 149.0),
        'VIC': (-39.5, -33.9, 140.9, 150.1),
        'NSW': (-37.6, -28.1, 140.9, 154.0),
        'ACT': (-35.95, -35.1, 148.7, 149.4),
        'QLD': (-29.2, -10.0, 137.9, 153.6),
        'SA': (-38.1, -25.9, 129.0, 141.0),
        'WA': (-35.2, -13.7, 112.9, 129.0),
        'NT': (-26.0, -10.9, 129.0, 138.0),
    }
    print(f"\nBy State (approximate):")
    print(f"{'State':<6} {'Total':>6} {'W/Area':>7} {'Avg GLA':>10}")
    print("-"*35)
    for state, (lat_min, lat_max, lon_min, lon_max) in sorted(states.items()):
        ss = [s for s in supermarkets if s['lat'] and s['lon']
              and lat_min <= s['lat'] <= lat_max and lon_min <= s['lon'] <= lon_max]
        sa = [s['area_sqm'] for s in ss if s.get('area_sqm') and s['area_sqm'] > 0]
        avg_a = sum(sa) / len(sa) if sa else 0
        print(f"{state:<6} {len(ss):>6} {len(sa):>7} {avg_a:>9.0f}sqm")
    
    # Pharmacy rule thresholds
    print(f"\n{'='*60}")
    print("PHARMACY LOCATION RULE THRESHOLDS")
    print(f"{'='*60}")
    t500 = sum(1 for s in with_area if s['area_sqm'] >= 500)
    t1000 = sum(1 for s in with_area if s['area_sqm'] >= 1000)
    t2500 = sum(1 for s in with_area if s['area_sqm'] >= 2500)
    print(f"  >= 500sqm (Item 134A): {t500:,} supermarkets")
    print(f"  >= 1000sqm (Item 134):  {t1000:,} supermarkets")
    print(f"  >= 2500sqm (Item 133):  {t2500:,} supermarkets")


def main():
    print("National Supermarket GLA Calculator v3 - Overpass API")
    print("="*55)
    print(f"Start: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Fetch all supermarkets tagged shop=supermarket
    areas1, points1 = fetch_all_supermarkets()
    
    # The shop=supermarket tag captures the vast majority.
    # Brand-only queries are extremely slow on Overpass for all of AU.
    # We skip them to avoid timeouts - shop=supermarket is comprehensive.
    
    all_supers = merge_and_dedup(areas1, points1)
    
    # Classify
    for s in all_supers:
        s['brand_classified'] = classify_brand(s['name'], s['brand'])
    
    # Flag unusual areas
    for s in all_supers:
        if s.get('area_sqm') and s['area_sqm'] > 20000:
            print(f"  WARNING Very large: {s['name']} = {s['area_sqm']:.0f}sqm ({s['osm_id']})")
    
    save_results(all_supers, OUTPUT_JSON, OUTPUT_CSV)
    
    if os.path.exists(DB_PATH):
        update_database(all_supers, DB_PATH)
    
    print_summary(all_supers)
    
    print(f"\nCompleted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == '__main__':
    main()
