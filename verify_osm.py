"""
Pharmacy Verification via OpenStreetMap Overpass API
Verifies PASS and LIKELY opportunities against OSM pharmacy data.
Compares with our DB to find gaps.
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

# Force unbuffered output
sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

WORKDIR = r'C:\Users\MJ\Documents\GitHub\PharmacyFinder'
DB_PATH = os.path.join(WORKDIR, 'pharmacy_finder.db')
SCORED_PATH = os.path.join(WORKDIR, 'output', 'scored_v2.json')
CACHE_PATH = os.path.join(WORKDIR, 'cache', 'osm_pharmacy_cache.json')
OUTPUT_PATH = os.path.join(WORKDIR, 'output', 'verification_results.json')
RADIUS_M = 15000  # 15km
MATCH_THRESHOLD_M = 200  # 200m to consider a match
RATE_LIMIT_S = 2.5  # seconds between API calls

def haversine(lat1, lon1, lat2, lon2):
    """Distance in km between two lat/lng points."""
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))

def load_cache():
    """Load cached OSM results."""
    if os.path.exists(CACHE_PATH):
        with open(CACHE_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_cache(cache):
    """Save OSM results cache."""
    os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)
    with open(CACHE_PATH, 'w', encoding='utf-8') as f:
        json.dump(cache, f, indent=2, ensure_ascii=False)

def query_overpass(lat, lng, radius=RADIUS_M):
    """Query Overpass API for pharmacies near a location."""
    query = f"""[out:json][timeout:25];
(
  node["amenity"="pharmacy"](around:{radius},{lat},{lng});
  way["amenity"="pharmacy"](around:{radius},{lat},{lng});
);
out center;"""
    url = 'https://overpass-api.de/api/interpreter?data=' + urllib.parse.quote(query)
    
    for attempt in range(3):
        try:
            req = urllib.request.Request(url)
            req.add_header('User-Agent', 'PharmacyFinder/1.0')
            resp = urllib.request.urlopen(req, timeout=30)
            data = json.loads(resp.read())
            elements = data.get('elements', [])
            results = []
            for e in elements:
                name = e.get('tags', {}).get('name', 'Unknown Pharmacy')
                elat = e.get('lat') or e.get('center', {}).get('lat')
                elng = e.get('lon') or e.get('center', {}).get('lon')
                if elat and elng:
                    dist = haversine(lat, lng, elat, elng)
                    results.append({
                        'name': name,
                        'lat': elat,
                        'lng': elng,
                        'distance_km': round(dist, 2),
                        'osm_id': e.get('id'),
                        'osm_type': e.get('type')
                    })
            return results
        except Exception as ex:
            if attempt < 2:
                print(f"    Retry {attempt+1} after error: {ex}")
                time.sleep(5 * (attempt + 1))
            else:
                print(f"    FAILED after 3 attempts: {ex}")
                return None
    return None

def get_db_pharmacies(lat, lng, radius_km, conn):
    """Get pharmacies from our DB within radius_km of a point."""
    # Use a bounding box for initial filter, then haversine for exact
    deg_approx = radius_km / 111.0  # rough degrees
    cursor = conn.execute(
        """SELECT id, name, latitude, longitude, address, suburb, state, postcode
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
                'id': row[0],
                'name': row[1],
                'lat': row[2],
                'lng': row[3],
                'address': row[4],
                'suburb': row[5],
                'state': row[6],
                'postcode': row[7],
                'distance_km': round(dist, 2)
            })
    return results

def find_missing_pharmacies(osm_pharmacies, db_pharmacies, threshold_m=MATCH_THRESHOLD_M):
    """Find OSM pharmacies that don't match any DB pharmacy within threshold."""
    threshold_km = threshold_m / 1000.0
    missing = []
    for osm_ph in osm_pharmacies:
        matched = False
        for db_ph in db_pharmacies:
            dist = haversine(osm_ph['lat'], osm_ph['lng'], db_ph['lat'], db_ph['lng'])
            if dist <= threshold_km:
                matched = True
                break
        if not matched:
            missing.append(osm_ph)
    return missing

def main():
    print("=" * 60)
    print("PHARMACY VERIFICATION VIA OSM OVERPASS API")
    print("=" * 60)
    
    # Load scored data
    with open(SCORED_PATH, 'r', encoding='utf-8') as f:
        scored = json.load(f)
    
    # Filter PASS and LIKELY, sort by pop_10km desc (commercially viable first)
    opportunities = [x for x in scored if x.get('verdict') in ('PASS', 'LIKELY')]
    opportunities.sort(key=lambda x: (
        0 if x['verdict'] == 'PASS' else 1,  # PASS first
        -(x.get('pop_10km') or 0)  # Higher pop first
    ))
    
    print(f"Total opportunities to verify: {len(opportunities)}")
    print(f"  PASS: {sum(1 for x in opportunities if x['verdict'] == 'PASS')}")
    print(f"  LIKELY: {sum(1 for x in opportunities if x['verdict'] == 'LIKELY')}")
    
    # Load cache
    cache = load_cache()
    print(f"Cached locations: {len(cache)}")
    
    # Connect to DB
    conn = sqlite3.connect(DB_PATH)
    
    results = []
    cached_count = 0
    api_count = 0
    
    # Deduplicate by rounding lat/lng to avoid querying same area multiple times
    # Use 0.01 degree grid (~1km) for dedup
    seen_grids = {}
    
    for i, opp in enumerate(opportunities):
        lat = opp['lat']
        lng = opp['lng']
        cache_key = f"{round(lat, 4)},{round(lng, 4)}"
        
        # Progress
        if (i + 1) % 10 == 0 or i == 0:
            print(f"\nProgress: {i+1}/{len(opportunities)} ({cached_count} cached, {api_count} API calls)")
        
        # Query OSM (with cache)
        if cache_key in cache:
            osm_pharmacies = cache[cache_key]
            cached_count += 1
        else:
            # Check if we've queried a very close location already (within 2km)
            found_nearby_cache = False
            for ck, cv in cache.items():
                clat, clng = map(float, ck.split(','))
                if haversine(lat, lng, clat, clng) < 2.0:
                    osm_pharmacies = cv
                    found_nearby_cache = True
                    cached_count += 1
                    break
            
            if not found_nearby_cache:
                print(f"  [{i+1}] Querying OSM for {opp['name']} ({lat:.4f}, {lng:.4f})...")
                osm_pharmacies = query_overpass(lat, lng)
                if osm_pharmacies is None:
                    osm_pharmacies = []  # Failed, treat as empty
                cache[cache_key] = osm_pharmacies
                api_count += 1
                
                # Save cache every 20 API calls
                if api_count % 20 == 0:
                    save_cache(cache)
                    print(f"  [Cache saved: {len(cache)} locations]")
                
                # Rate limit
                time.sleep(RATE_LIMIT_S)
        
        # Get DB pharmacies for comparison
        db_pharmacies = get_db_pharmacies(lat, lng, 15.0, conn)
        
        # Find missing
        missing = find_missing_pharmacies(osm_pharmacies, db_pharmacies)
        
        # Determine verdict
        osm_count = len(osm_pharmacies)
        db_count = len(db_pharmacies)
        db_count_from_scored = opp.get('pharmacy_15km', 0)
        
        # An opportunity is still valid if OSM doesn't show significantly more pharmacies
        # Key: if OSM shows pharmacies that our DB doesn't have, those are gaps
        still_valid = True
        verdict_change = None
        
        if len(missing) > 0:
            # Check if missing pharmacies would change the verdict
            total_actual = db_count + len(missing)
            # If the opportunity was PASS because of low pharmacy count, but actually there are more...
            if opp.get('pharmacy_15km', 0) == 0 and osm_count >= 1:
                still_valid = False
                verdict_change = f"{opp['verdict']} -> INVALID ({osm_count} pharmacies found by OSM, DB shows {db_count_from_scored})"
            elif opp.get('pharmacy_10km', 0) == 0 and any(p['distance_km'] <= 10 for p in osm_pharmacies):
                nearby_osm = sum(1 for p in osm_pharmacies if p['distance_km'] <= 10)
                still_valid = False
                verdict_change = f"{opp['verdict']} -> INVALID ({nearby_osm} pharmacies within 10km found by OSM, DB shows 0)"
            elif opp.get('pharmacy_5km', 0) == 0 and any(p['distance_km'] <= 5 for p in osm_pharmacies):
                nearby_osm = sum(1 for p in osm_pharmacies if p['distance_km'] <= 5)
                # If nearest_pharmacy_km was large but OSM shows nearby ones
                if opp.get('nearest_pharmacy_km', 999) > 10 and any(p['distance_km'] <= 5 for p in osm_pharmacies):
                    still_valid = False
                    verdict_change = f"{opp['verdict']} -> INVALID (pharmacy within 5km found by OSM, DB nearest was {opp.get('nearest_pharmacy_km', '?')}km)"
                else:
                    verdict_change = f"{opp['verdict']} -> NEEDS REVIEW ({len(missing)} missing pharmacies found)"
            else:
                verdict_change = f"{opp['verdict']} -> CONFIRMED (OSM: {osm_count}, DB: {db_count}, {len(missing)} minor gaps)"
        else:
            verdict_change = f"{opp['verdict']} -> CONFIRMED (OSM: {osm_count}, DB: {db_count}, no gaps)"
        
        result = {
            'id': opp['id'],
            'name': opp['name'],
            'state': opp.get('state'),
            'lat': lat,
            'lng': lng,
            'original_verdict': opp['verdict'],
            'score': opp.get('score'),
            'pop_10km': opp.get('pop_10km', 0),
            'db_pharmacies_15km': db_count,
            'db_pharmacies_15km_scored': db_count_from_scored,
            'verified_pharmacies_15km': osm_count,
            'missing_pharmacies': missing,
            'missing_count': len(missing),
            'verdict_change': verdict_change,
            'still_valid': still_valid
        }
        results.append(result)
        
        if not still_valid:
            print(f"  *** INVALIDATED: {opp['name']} - {verdict_change}")
    
    # Save final cache
    save_cache(cache)
    
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
    pass_valid = sum(1 for r in results if r['still_valid'] and r['original_verdict'] == 'PASS')
    pass_invalid = sum(1 for r in results if not r['still_valid'] and r['original_verdict'] == 'PASS')
    likely_valid = sum(1 for r in results if r['still_valid'] and r['original_verdict'] == 'LIKELY')
    likely_invalid = sum(1 for r in results if not r['still_valid'] and r['original_verdict'] == 'LIKELY')
    
    print(f"Total verified: {total}")
    print(f"  Still valid: {valid} ({valid/total*100:.1f}%)")
    print(f"  Invalidated: {invalid} ({invalid/total*100:.1f}%)")
    print(f"  PASS: {pass_valid} valid, {pass_invalid} invalid (of {pass_valid+pass_invalid})")
    print(f"  LIKELY: {likely_valid} valid, {likely_invalid} invalid (of {likely_valid+likely_invalid})")
    
    # Collect ALL missing pharmacies for DB insertion
    all_missing = []
    seen_osm_ids = set()
    for r in results:
        for mp in r['missing_pharmacies']:
            osm_key = f"{mp.get('osm_type','?')}_{mp.get('osm_id','?')}"
            if osm_key not in seen_osm_ids:
                seen_osm_ids.add(osm_key)
                all_missing.append(mp)
    
    print(f"\nUnique missing pharmacies to add to DB: {len(all_missing)}")
    
    # Insert missing pharmacies into DB
    inserted = 0
    for mp in all_missing:
        # Check if already exists (within 200m of any existing)
        existing = conn.execute(
            """SELECT id FROM pharmacies 
               WHERE latitude BETWEEN ? AND ? 
               AND longitude BETWEEN ? AND ?""",
            (mp['lat'] - 0.002, mp['lat'] + 0.002,
             mp['lng'] - 0.002, mp['lng'] + 0.002)
        ).fetchall()
        
        too_close = False
        for ex in existing:
            ex_row = conn.execute("SELECT latitude, longitude FROM pharmacies WHERE id=?", (ex[0],)).fetchone()
            if ex_row and haversine(mp['lat'], mp['lng'], ex_row[0], ex_row[1]) < 0.2:
                too_close = True
                break
        
        if not too_close:
            conn.execute(
                """INSERT INTO pharmacies (name, address, latitude, longitude, source, date_scraped, suburb, state, postcode)
                   VALUES (?, ?, ?, ?, 'osm_verification', ?, '', '', '')""",
                (mp['name'], f"OSM verified ({mp['lat']:.6f}, {mp['lng']:.6f})",
                 mp['lat'], mp['lng'], datetime.now().isoformat())
            )
            inserted += 1
    
    conn.commit()
    print(f"Inserted {inserted} new pharmacies into DB")
    print(f"New total: {conn.execute('SELECT COUNT(*) FROM pharmacies').fetchone()[0]} pharmacies")
    
    conn.close()
    
    # Print top invalidated opportunities
    invalidated = [r for r in results if not r['still_valid']]
    if invalidated:
        invalidated.sort(key=lambda x: -(x.get('pop_10km') or 0))
        print(f"\nTop invalidated opportunities (by population):")
        for r in invalidated[:20]:
            print(f"  {r['name']} ({r['state']}) - pop_10km: {r['pop_10km']:,} - {r['verdict_change']}")
    
    print(f"\nResults saved to: {OUTPUT_PATH}")
    print(f"Cache saved to: {CACHE_PATH}")
    return invalid, inserted

if __name__ == '__main__':
    invalid, inserted = main()
