"""
Pharmacy Verification v2 — Single bulk OSM query approach.
Downloads ALL pharmacies in Australia from OSM at once, then compares locally.
Much faster than per-location queries.
"""
import json
import sqlite3
import math
import time
import urllib.request
import urllib.parse
import os
import sys
from datetime import datetime

sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

WORKDIR = r'C:\Users\MJ\Documents\GitHub\PharmacyFinder'
DB_PATH = os.path.join(WORKDIR, 'pharmacy_finder.db')
SCORED_PATH = os.path.join(WORKDIR, 'output', 'scored_v2.json')
OSM_CACHE_PATH = os.path.join(WORKDIR, 'cache', 'osm_all_pharmacies_au.json')
OUTPUT_PATH = os.path.join(WORKDIR, 'output', 'verification_results.json')
MATCH_THRESHOLD_KM = 0.2  # 200m

def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))

def download_all_osm_pharmacies():
    """Download ALL pharmacies in Australia from OSM in one query."""
    cache_file = OSM_CACHE_PATH
    if os.path.exists(cache_file):
        print(f"Loading cached OSM data from {cache_file}")
        with open(cache_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    print("Downloading ALL Australian pharmacies from OSM Overpass API...")
    print("(This is a large query, may take 30-60 seconds)")
    
    # Australia bounding box: roughly -44 to -10 lat, 112 to 154 lng
    query = """[out:json][timeout:120];
area["ISO3166-1"="AU"]->.australia;
(
  node["amenity"="pharmacy"](area.australia);
  way["amenity"="pharmacy"](area.australia);
  relation["amenity"="pharmacy"](area.australia);
);
out center;"""
    
    url = 'https://overpass-api.de/api/interpreter'
    data_bytes = urllib.parse.urlencode({'data': query}).encode('utf-8')
    
    for attempt in range(3):
        try:
            req = urllib.request.Request(url, data=data_bytes, method='POST')
            req.add_header('User-Agent', 'PharmacyFinder/1.0')
            req.add_header('Content-Type', 'application/x-www-form-urlencoded')
            resp = urllib.request.urlopen(req, timeout=180)
            raw = resp.read()
            result = json.loads(raw)
            elements = result.get('elements', [])
            
            pharmacies = []
            for e in elements:
                name = e.get('tags', {}).get('name', 'Unknown Pharmacy')
                lat = e.get('lat') or (e.get('center', {}) or {}).get('lat')
                lng = e.get('lon') or (e.get('center', {}) or {}).get('lon')
                if lat and lng:
                    pharmacies.append({
                        'name': name,
                        'lat': lat,
                        'lng': lng,
                        'osm_id': e.get('id'),
                        'osm_type': e.get('type'),
                        'brand': e.get('tags', {}).get('brand', ''),
                        'operator': e.get('tags', {}).get('operator', ''),
                        'addr_street': e.get('tags', {}).get('addr:street', ''),
                        'addr_suburb': e.get('tags', {}).get('addr:suburb', ''),
                        'addr_state': e.get('tags', {}).get('addr:state', ''),
                        'addr_postcode': e.get('tags', {}).get('addr:postcode', ''),
                    })
            
            print(f"Downloaded {len(pharmacies)} pharmacies from OSM")
            
            # Cache it
            os.makedirs(os.path.dirname(cache_file), exist_ok=True)
            with open(cache_file, 'w', encoding='utf-8') as f:
                json.dump(pharmacies, f, indent=2, ensure_ascii=False)
            
            return pharmacies
            
        except Exception as ex:
            print(f"  Attempt {attempt+1} failed: {ex}")
            if attempt < 2:
                time.sleep(10 * (attempt + 1))
            else:
                raise

def build_spatial_index(pharmacies, grid_size=0.15):
    """Simple grid-based spatial index for fast radius lookups."""
    index = {}
    for p in pharmacies:
        gx = int(p['lat'] / grid_size)
        gy = int(p['lng'] / grid_size)
        key = (gx, gy)
        if key not in index:
            index[key] = []
        index[key].append(p)
    return index, grid_size

def find_nearby(lat, lng, radius_km, spatial_index, grid_size):
    """Find pharmacies within radius_km using spatial index."""
    # How many grid cells to check
    cells = int(radius_km / (grid_size * 111.0)) + 2
    gx = int(lat / grid_size)
    gy = int(lng / grid_size)
    
    results = []
    for dx in range(-cells, cells + 1):
        for dy in range(-cells, cells + 1):
            key = (gx + dx, gy + dy)
            for p in spatial_index.get(key, []):
                dist = haversine(lat, lng, p['lat'], p['lng'])
                if dist <= radius_km:
                    results.append({**p, 'distance_km': round(dist, 2)})
    
    results.sort(key=lambda x: x['distance_km'])
    return results

def get_db_pharmacies(lat, lng, radius_km, conn):
    """Get pharmacies from our DB within radius_km."""
    deg_approx = radius_km / 111.0
    cursor = conn.execute(
        """SELECT id, name, latitude, longitude, address, suburb, state, postcode, source
           FROM pharmacies
           WHERE latitude BETWEEN ? AND ?
           AND longitude BETWEEN ? AND ?""",
        (lat - deg_approx, lat + deg_approx, lng - deg_approx, lng + deg_approx)
    )
    results = []
    for row in cursor.fetchall():
        dist = haversine(lat, lng, row[2], row[3])
        if dist <= radius_km:
            results.append({
                'id': row[0], 'name': row[1], 'lat': row[2], 'lng': row[3],
                'address': row[4], 'suburb': row[5], 'state': row[6],
                'postcode': row[7], 'source': row[8], 'distance_km': round(dist, 2)
            })
    return results

def find_missing(osm_pharmacies, db_pharmacies):
    """Find OSM pharmacies not matching any DB pharmacy within 200m."""
    missing = []
    for osm_ph in osm_pharmacies:
        matched = False
        for db_ph in db_pharmacies:
            if haversine(osm_ph['lat'], osm_ph['lng'], db_ph['lat'], db_ph['lng']) <= MATCH_THRESHOLD_KM:
                matched = True
                break
        if not matched:
            missing.append(osm_ph)
    return missing

def main():
    print("=" * 60)
    print("PHARMACY VERIFICATION v2 — BULK OSM APPROACH")
    print("=" * 60)
    
    # Step 1: Download all OSM pharmacies
    osm_pharmacies = download_all_osm_pharmacies()
    
    # Step 2: Build spatial index
    print("Building spatial index...")
    osm_index, grid_size = build_spatial_index(osm_pharmacies)
    print(f"Index built: {len(osm_index)} grid cells")
    
    # Step 3: Load scored opportunities
    with open(SCORED_PATH, 'r', encoding='utf-8') as f:
        scored = json.load(f)
    
    opportunities = [x for x in scored if x.get('verdict') in ('PASS', 'LIKELY')]
    opportunities.sort(key=lambda x: (
        0 if x['verdict'] == 'PASS' else 1,
        -(x.get('pop_10km') or 0)
    ))
    
    print(f"\nOpportunities to verify: {len(opportunities)}")
    print(f"  PASS: {sum(1 for x in opportunities if x['verdict'] == 'PASS')}")
    print(f"  LIKELY: {sum(1 for x in opportunities if x['verdict'] == 'LIKELY')}")
    
    # Step 4: Connect to DB
    conn = sqlite3.connect(DB_PATH)
    
    # Step 5: Verify each opportunity
    results = []
    for i, opp in enumerate(opportunities):
        lat, lng = opp['lat'], opp['lng']
        
        if (i + 1) % 50 == 0:
            print(f"  Processing {i+1}/{len(opportunities)}...")
        
        # OSM pharmacies within 15km
        osm_nearby = find_nearby(lat, lng, 15.0, osm_index, grid_size)
        osm_5km = [p for p in osm_nearby if p['distance_km'] <= 5.0]
        osm_10km = [p for p in osm_nearby if p['distance_km'] <= 10.0]
        
        # DB pharmacies within 15km
        db_nearby = get_db_pharmacies(lat, lng, 15.0, conn)
        db_5km = [p for p in db_nearby if p['distance_km'] <= 5.0]
        db_10km = [p for p in db_nearby if p['distance_km'] <= 10.0]
        
        # Find missing pharmacies
        missing = find_missing(osm_nearby, db_nearby)
        missing_5km = [p for p in missing if p['distance_km'] <= 5.0]
        missing_10km = [p for p in missing if p['distance_km'] <= 10.0]
        
        # Determine if still valid
        still_valid = True
        verdict_change = None
        
        scored_ph_15 = opp.get('pharmacy_15km', 0)
        scored_ph_10 = opp.get('pharmacy_10km', 0)
        scored_ph_5 = opp.get('pharmacy_5km', 0)
        nearest_db = opp.get('nearest_pharmacy_km', 999)
        
        osm_nearest = osm_nearby[0]['distance_km'] if osm_nearby else 999
        
        # Invalidation logic:
        # 1. If DB says 0 pharmacies within 15km but OSM finds any
        if scored_ph_15 == 0 and len(osm_nearby) > 0:
            still_valid = False
            verdict_change = f"{opp['verdict']} -> INVALID (DB: 0 pharmacies 15km, OSM: {len(osm_nearby)} found, nearest OSM: {osm_nearest:.1f}km)"
        # 2. If DB says 0 within 10km but OSM finds within 10km
        elif scored_ph_10 == 0 and len(osm_10km) > 0:
            still_valid = False 
            verdict_change = f"{opp['verdict']} -> INVALID (DB: 0 pharmacies 10km, OSM: {len(osm_10km)} found within 10km)"
        # 3. If nearest pharmacy was claimed >10km but OSM shows one much closer
        elif nearest_db > 10 and osm_nearest < 5:
            still_valid = False
            verdict_change = f"{opp['verdict']} -> INVALID (DB nearest: {nearest_db:.1f}km, OSM nearest: {osm_nearest:.1f}km)"
        # 4. If missing pharmacies substantially change the competition picture
        elif len(missing) > 0 and (len(osm_nearby) > len(db_nearby) * 1.5 + 2):
            still_valid = False
            verdict_change = f"{opp['verdict']} -> INVALID (DB: {len(db_nearby)} pharmacies, OSM: {len(osm_nearby)} — significant gap)"
        # 5. Minor gaps but still valid
        elif len(missing) > 0:
            verdict_change = f"{opp['verdict']} -> CONFIRMED with gaps (DB: {len(db_nearby)}, OSM: {len(osm_nearby)}, {len(missing)} missing)"
        else:
            verdict_change = f"{opp['verdict']} -> CONFIRMED (DB: {len(db_nearby)}, OSM: {len(osm_nearby)})"
        
        # Clean missing for output (remove spatial index extras)
        clean_missing = [{
            'name': m['name'], 'lat': m['lat'], 'lng': m['lng'],
            'distance_km': m['distance_km'],
            'osm_id': m.get('osm_id'), 'osm_type': m.get('osm_type'),
            'brand': m.get('brand', ''), 'addr_suburb': m.get('addr_suburb', ''),
            'addr_state': m.get('addr_state', ''), 'addr_postcode': m.get('addr_postcode', ''),
        } for m in missing]
        
        result = {
            'id': opp['id'],
            'name': opp['name'],
            'state': opp.get('state'),
            'lat': lat,
            'lng': lng,
            'original_verdict': opp['verdict'],
            'score': opp.get('score'),
            'pop_10km': opp.get('pop_10km', 0),
            'best_rule': opp.get('best_rule_display', ''),
            'db_pharmacies_5km': len(db_5km),
            'db_pharmacies_10km': len(db_10km),
            'db_pharmacies_15km': len(db_nearby),
            'scored_pharmacies_5km': scored_ph_5,
            'scored_pharmacies_10km': scored_ph_10,
            'scored_pharmacies_15km': scored_ph_15,
            'osm_pharmacies_5km': len(osm_5km),
            'osm_pharmacies_10km': len(osm_10km),
            'osm_pharmacies_15km': len(osm_nearby),
            'osm_nearest_km': osm_nearest,
            'db_nearest_km': nearest_db,
            'missing_pharmacies': clean_missing,
            'missing_count': len(missing),
            'verdict_change': verdict_change,
            'still_valid': still_valid
        }
        results.append(result)
    
    # Save results
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    # Summary
    print("\n" + "=" * 60)
    print("VERIFICATION SUMMARY")
    print("=" * 60)
    
    total = len(results)
    valid = sum(1 for r in results if r['still_valid'])
    invalid = sum(1 for r in results if not r['still_valid'])
    pass_total = sum(1 for r in results if r['original_verdict'] == 'PASS')
    pass_valid = sum(1 for r in results if r['still_valid'] and r['original_verdict'] == 'PASS')
    pass_invalid = sum(1 for r in results if not r['still_valid'] and r['original_verdict'] == 'PASS')
    likely_total = sum(1 for r in results if r['original_verdict'] == 'LIKELY')
    likely_valid = sum(1 for r in results if r['still_valid'] and r['original_verdict'] == 'LIKELY')
    likely_invalid = sum(1 for r in results if not r['still_valid'] and r['original_verdict'] == 'LIKELY')
    
    print(f"Total verified: {total}")
    print(f"  Still valid: {valid} ({valid/total*100:.1f}%)")
    print(f"  Invalidated: {invalid} ({invalid/total*100:.1f}%)")
    print(f"")
    print(f"  PASS:   {pass_valid} valid / {pass_invalid} invalid (of {pass_total})")
    print(f"  LIKELY: {likely_valid} valid / {likely_invalid} invalid (of {likely_total})")
    
    # Collect unique missing pharmacies for DB insertion
    all_missing = {}
    for r in results:
        for mp in r['missing_pharmacies']:
            key = f"{mp.get('osm_type','?')}_{mp.get('osm_id','?')}"
            if key not in all_missing and key != '?_?':
                all_missing[key] = mp
    
    print(f"\nUnique missing pharmacies found: {len(all_missing)}")
    
    # Insert missing pharmacies
    inserted = 0
    for key, mp in all_missing.items():
        # Double-check not too close to existing
        existing = conn.execute(
            """SELECT latitude, longitude FROM pharmacies 
               WHERE latitude BETWEEN ? AND ? AND longitude BETWEEN ? AND ?""",
            (mp['lat'] - 0.003, mp['lat'] + 0.003, mp['lng'] - 0.003, mp['lng'] + 0.003)
        ).fetchall()
        
        too_close = any(haversine(mp['lat'], mp['lng'], ex[0], ex[1]) < 0.2 for ex in existing)
        
        if not too_close:
            addr_parts = [mp.get('addr_suburb', ''), mp.get('addr_state', ''), mp.get('addr_postcode', '')]
            address = ', '.join(p for p in addr_parts if p) or f"OSM ({mp['lat']:.6f}, {mp['lng']:.6f})"
            
            conn.execute(
                """INSERT INTO pharmacies (name, address, latitude, longitude, source, date_scraped, suburb, state, postcode)
                   VALUES (?, ?, ?, ?, 'osm_verification', ?, ?, ?, ?)""",
                (mp['name'], address, mp['lat'], mp['lng'], datetime.now().isoformat(),
                 mp.get('addr_suburb', ''), mp.get('addr_state', ''), mp.get('addr_postcode', ''))
            )
            inserted += 1
    
    conn.commit()
    new_total = conn.execute('SELECT COUNT(*) FROM pharmacies').fetchone()[0]
    print(f"Inserted {inserted} new pharmacies into DB")
    print(f"DB pharmacy count: {new_total}")
    conn.close()
    
    # Top invalidated
    invalidated = sorted([r for r in results if not r['still_valid']], key=lambda x: -(x.get('pop_10km') or 0))
    if invalidated:
        print(f"\nTop invalidated opportunities (by population):")
        for r in invalidated[:15]:
            print(f"  [{r['id']}] {r['name']} ({r['state']}) pop_10km={r['pop_10km']:,}")
            print(f"       {r['verdict_change']}")
    
    # Top valid
    still_valid_list = sorted([r for r in results if r['still_valid']], key=lambda x: -(x.get('score') or 0))
    print(f"\nTop still-valid opportunities (by score):")
    for r in still_valid_list[:10]:
        print(f"  [{r['id']}] {r['name']} ({r['state']}) score={r['score']}, pop_10km={r['pop_10km']:,}")
    
    print(f"\nResults saved to: {OUTPUT_PATH}")
    return invalid, inserted

if __name__ == '__main__':
    invalid_count, inserted_count = main()
    print(f"\n{'='*60}")
    print(f"DONE: {invalid_count} invalidated, {inserted_count} pharmacies added")
    print(f"{'='*60}")
