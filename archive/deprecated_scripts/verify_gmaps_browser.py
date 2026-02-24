"""
PharmacyFinder — Google Maps Browser Verification
Uses Playwright to search Google Maps for pharmacies near each opportunity.
Compares results with our pharmacy database to find gaps and false positives.
"""
import sys, os, json, math, time, re, sqlite3, random
from datetime import datetime

os.environ['PYTHONIOENCODING'] = 'utf-8'
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
sys.stderr.reconfigure(encoding='utf-8', line_buffering=True)

from playwright.sync_api import sync_playwright

WORKDIR = r'C:\Users\MJ\Documents\GitHub\PharmacyFinder'
DB_PATH = os.path.join(WORKDIR, 'pharmacy_finder.db')
SCORED_PATH = os.path.join(WORKDIR, 'output', 'scored_v2.json')
CACHE_PATH = os.path.join(WORKDIR, 'cache', 'gmaps_browser_cache.json')
OUTPUT_PATH = os.path.join(WORKDIR, 'output', 'verification_results.json')
MATCH_THRESHOLD_KM = 0.3  # 300m to consider a match (addresses may vary)
SEARCH_ZOOM = 13  # zoom level for 15km~ radius

def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))

def extract_coords_from_href(href):
    """Extract lat/lng from Google Maps place URL."""
    lat_match = re.search(r'!3d(-?[\d.]+)', href)
    lng_match = re.search(r'!4d(-?[\d.]+)', href)
    if lat_match and lng_match:
        return float(lat_match.group(1)), float(lng_match.group(1))
    return None, None

def extract_name_from_href(href):
    """Extract and decode place name from URL."""
    m = re.search(r'/maps/place/([^/]+)', href)
    if m:
        import urllib.parse
        return urllib.parse.unquote(m.group(1).replace('+', ' '))
    return None

def dedup_by_location(pharmacies, threshold_km=0.05):
    """Remove duplicates that are within threshold_km of each other (same physical location)."""
    if not pharmacies:
        return pharmacies
    deduped = [pharmacies[0]]
    for p in pharmacies[1:]:
        is_dup = False
        for d in deduped:
            if p.get('lat') and d.get('lat'):
                dist = haversine(p['lat'], p['lng'], d['lat'], d['lng'])
                if dist < threshold_km:
                    is_dup = True
                    break
        if not is_dup:
            deduped.append(p)
    return deduped

def load_cache():
    if os.path.exists(CACHE_PATH):
        with open(CACHE_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_cache(cache):
    os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)
    with open(CACHE_PATH, 'w', encoding='utf-8') as f:
        json.dump(cache, f, indent=2, ensure_ascii=False)

def scrape_pharmacies(page, lat, lng, zoom=SEARCH_ZOOM, attempt=0):
    """Navigate to Google Maps and scrape pharmacy results near a location."""
    url = f"https://www.google.com/maps/search/pharmacy/@{lat},{lng},{zoom}z"
    
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
    except Exception as e:
        if attempt < 2:
            time.sleep(5)
            return scrape_pharmacies(page, lat, lng, zoom, attempt + 1)
        return []
    
    # Wait for results to render
    time.sleep(3 + random.uniform(0.5, 1.5))
    
    # Handle consent dialog (first time only)
    try:
        accept = page.locator("button:has-text('Accept all')").first
        if accept.is_visible(timeout=1500):
            accept.click()
            time.sleep(2)
    except:
        pass
    
    # Wait for place links
    try:
        page.wait_for_selector('a[href*="/maps/place/"]', timeout=8000)
    except:
        # No results found for this area
        return []
    
    time.sleep(1)
    
    # Scroll the results feed to load more (if any)
    try:
        feed = page.locator('div[role="feed"]').first
        if feed.is_visible(timeout=2000):
            # Scroll down a couple times to load more results
            for _ in range(3):
                feed.evaluate('el => el.scrollTop = el.scrollHeight')
                time.sleep(0.8)
    except:
        pass
    
    # Extract results
    results = []
    links = page.locator('a[href*="/maps/place/"]').all()
    
    for link in links:
        try:
            href = link.get_attribute('href') or ''
            p_lat, p_lng = extract_coords_from_href(href)
            name = extract_name_from_href(href) or 'Unknown Pharmacy'
            
            # Get card text for address
            address = ''
            try:
                parent = link.locator('xpath=ancestor::div[contains(@class,"Nv2PK")]').first
                card_text = parent.inner_text(timeout=1500)
                # Extract address line
                addr_match = re.search(r'(?:Pharmacy|Chemist)\s*[·•]\s*[^\n]*?[·•]\s*(.+?)(?:\n|$)', card_text)
                if addr_match:
                    address = addr_match.group(1).strip()
                # Also try simpler pattern: line after rating that has a street
                if not address:
                    for line in card_text.split('\n'):
                        line = line.strip()
                        if re.search(r'\d+\s+\w+\s+(St|Rd|Ave|Dr|Hwy|Ln|Ct|Pl|Blvd|Tce|Pde|Way|Cres)', line, re.IGNORECASE):
                            address = line
                            break
            except:
                pass
            
            if p_lat and p_lng:
                results.append({
                    'name': name,
                    'lat': p_lat,
                    'lng': p_lng,
                    'address': address,
                    'source': 'google_maps_browser'
                })
        except:
            continue
    
    # Dedup (Google sometimes shows same location under different names, e.g. brand + parent)
    results = dedup_by_location(results)
    
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
        if row[2] and row[3]:
            dist = haversine(lat, lng, row[2], row[3])
            if dist <= radius_km:
                results.append({
                    'id': row[0], 'name': row[1], 'lat': row[2], 'lng': row[3],
                    'address': row[4], 'suburb': row[5], 'state': row[6],
                    'postcode': row[7], 'source': row[8], 'distance_km': round(dist, 2)
                })
    return results

def find_missing(gmaps_pharmacies, db_pharmacies, threshold_km=MATCH_THRESHOLD_KM):
    """Find Google Maps pharmacies not matching any DB entry within threshold."""
    missing = []
    for gp in gmaps_pharmacies:
        matched = False
        for dp in db_pharmacies:
            if haversine(gp['lat'], gp['lng'], dp['lat'], dp['lng']) <= threshold_km:
                matched = True
                break
        if not matched:
            missing.append(gp)
    return missing

def main():
    print("=" * 70)
    print("PHARMACY VERIFICATION — GOOGLE MAPS BROWSER AUTOMATION")
    print("=" * 70)
    
    # Load scored data
    with open(SCORED_PATH, 'r', encoding='utf-8') as f:
        scored = json.load(f)
    
    # Filter and sort: PASS first (by pop desc), then LIKELY
    opportunities = [x for x in scored if x.get('verdict') in ('PASS', 'LIKELY')]
    opportunities.sort(key=lambda x: (
        0 if x['verdict'] == 'PASS' else 1,
        -(x.get('pop_10km') or 0)
    ))
    
    total_pass = sum(1 for x in opportunities if x['verdict'] == 'PASS')
    total_likely = sum(1 for x in opportunities if x['verdict'] == 'LIKELY')
    print(f"Opportunities to verify: {len(opportunities)} (PASS: {total_pass}, LIKELY: {total_likely})")
    
    # Load cache
    cache = load_cache()
    print(f"Cached results: {len(cache)} locations")
    
    # Connect DB
    conn = sqlite3.connect(DB_PATH)
    
    # Launch browser
    print("\nLaunching browser...")
    pw = sync_playwright().start()
    browser = pw.chromium.launch(headless=True)
    ctx = browser.new_context(
        viewport={"width": 1280, "height": 900},
        locale="en-AU",
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )
    page = ctx.new_page()
    
    results = []
    cached_count = 0
    scraped_count = 0
    
    try:
        for i, opp in enumerate(opportunities):
            lat, lng = opp['lat'], opp['lng']
            cache_key = f"{lat:.4f},{lng:.4f}"
            
            # Progress
            if (i + 1) % 10 == 0 or i == 0:
                pct = (i + 1) / len(opportunities) * 100
                print(f"\n[{i+1}/{len(opportunities)}] ({pct:.0f}%) — {cached_count} cached, {scraped_count} scraped")
            
            # Check cache (exact or nearby within 2km)
            gmaps_pharmacies = None
            if cache_key in cache:
                gmaps_pharmacies = cache[cache_key]
                cached_count += 1
            else:
                # Check for nearby cached results
                for ck, cv in cache.items():
                    try:
                        clat, clng = map(float, ck.split(','))
                        if haversine(lat, lng, clat, clng) < 2.0:
                            gmaps_pharmacies = cv
                            cached_count += 1
                            break
                    except:
                        continue
            
            if gmaps_pharmacies is None:
                # Scrape from Google Maps
                gmaps_pharmacies = scrape_pharmacies(page, lat, lng)
                cache[cache_key] = gmaps_pharmacies
                scraped_count += 1
                
                # Save cache periodically
                if scraped_count % 25 == 0:
                    save_cache(cache)
                    print(f"  [Cache saved: {len(cache)} locations]")
                
                # Rate limit: 2-3 seconds between scrapes
                time.sleep(2 + random.uniform(0.5, 1.5))
            
            # Calculate distances for gmaps results relative to opportunity
            for gp in gmaps_pharmacies:
                gp['distance_km'] = round(haversine(lat, lng, gp['lat'], gp['lng']), 2)
            
            # Filter to 15km radius
            gmaps_15km = [gp for gp in gmaps_pharmacies if gp['distance_km'] <= 15.0]
            gmaps_10km = [gp for gp in gmaps_pharmacies if gp['distance_km'] <= 10.0]
            gmaps_5km = [gp for gp in gmaps_pharmacies if gp['distance_km'] <= 5.0]
            
            # DB pharmacies
            db_nearby = get_db_pharmacies(lat, lng, 15.0, conn)
            db_10km = [p for p in db_nearby if p['distance_km'] <= 10.0]
            db_5km = [p for p in db_nearby if p['distance_km'] <= 5.0]
            
            # Find missing
            missing = find_missing(gmaps_15km, db_nearby)
            missing_10km = find_missing(gmaps_10km, db_10km)
            
            # Scored values
            scored_ph_15 = opp.get('pharmacy_15km', 0)
            scored_ph_10 = opp.get('pharmacy_10km', 0)
            scored_ph_5 = opp.get('pharmacy_5km', 0)
            nearest_db = opp.get('nearest_pharmacy_km', 999)
            gmaps_nearest = min((gp['distance_km'] for gp in gmaps_pharmacies), default=999)
            
            # Verdict logic
            still_valid = True
            verdict_change = None
            
            if scored_ph_15 == 0 and len(gmaps_15km) > 0:
                still_valid = False
                verdict_change = f"{opp['verdict']} -> INVALID (DB: 0 in 15km, Google: {len(gmaps_15km)} found, nearest: {gmaps_nearest:.1f}km)"
            elif scored_ph_10 == 0 and len(gmaps_10km) > 0:
                still_valid = False
                verdict_change = f"{opp['verdict']} -> INVALID (DB: 0 in 10km, Google: {len(gmaps_10km)} within 10km)"
            elif nearest_db > 10 and gmaps_nearest < 5:
                still_valid = False
                verdict_change = f"{opp['verdict']} -> INVALID (DB nearest: {nearest_db:.1f}km, Google nearest: {gmaps_nearest:.1f}km)"
            elif len(missing) > 0 and (len(gmaps_15km) > len(db_nearby) * 1.5 + 2):
                still_valid = False
                verdict_change = f"{opp['verdict']} -> INVALID (DB: {len(db_nearby)}, Google: {len(gmaps_15km)} — significant undercount)"
            elif len(missing) > 0:
                verdict_change = f"{opp['verdict']} -> CONFIRMED with gaps ({len(missing)} missing from DB)"
            else:
                verdict_change = f"{opp['verdict']} -> CONFIRMED (DB: {len(db_nearby)}, Google: {len(gmaps_15km)})"
            
            # Clean output
            clean_missing = [{
                'name': m['name'], 'lat': m['lat'], 'lng': m['lng'],
                'distance_km': m['distance_km'], 'address': m.get('address', '')
            } for m in missing]
            
            clean_gmaps = [{
                'name': g['name'], 'lat': g['lat'], 'lng': g['lng'],
                'distance_km': g['distance_km'], 'address': g.get('address', '')
            } for g in gmaps_15km]
            
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
                'gmaps_pharmacies_5km': len(gmaps_5km),
                'gmaps_pharmacies_10km': len(gmaps_10km),
                'gmaps_pharmacies_15km': len(gmaps_15km),
                'gmaps_nearest_km': gmaps_nearest,
                'db_nearest_km': nearest_db,
                'gmaps_results': clean_gmaps,
                'missing_pharmacies': clean_missing,
                'missing_count': len(missing),
                'verdict_change': verdict_change,
                'still_valid': still_valid
            }
            results.append(result)
            
            if not still_valid:
                print(f"  *** INVALID: {opp['name']} ({opp['state']}) — {verdict_change}")
    
    except KeyboardInterrupt:
        print("\n\nInterrupted! Saving partial results...")
    except Exception as e:
        print(f"\n\nError: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Always save cache and results
        save_cache(cache)
        browser.close()
        pw.stop()
    
    # Save results
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    # Summary
    print("\n" + "=" * 70)
    print("VERIFICATION SUMMARY")
    print("=" * 70)
    
    total = len(results)
    if total == 0:
        print("No results!")
        return 0, 0
    
    valid = sum(1 for r in results if r['still_valid'])
    invalid = sum(1 for r in results if not r['still_valid'])
    pass_valid = sum(1 for r in results if r['still_valid'] and r['original_verdict'] == 'PASS')
    pass_invalid = sum(1 for r in results if not r['still_valid'] and r['original_verdict'] == 'PASS')
    likely_valid = sum(1 for r in results if r['still_valid'] and r['original_verdict'] == 'LIKELY')
    likely_invalid = sum(1 for r in results if not r['still_valid'] and r['original_verdict'] == 'LIKELY')
    
    print(f"Total verified: {total}")
    print(f"  Still valid:  {valid} ({valid/total*100:.1f}%)")
    print(f"  Invalidated:  {invalid} ({invalid/total*100:.1f}%)")
    print(f"")
    print(f"  PASS:   {pass_valid} valid / {pass_invalid} invalid (of {pass_valid+pass_invalid})")
    print(f"  LIKELY: {likely_valid} valid / {likely_invalid} invalid (of {likely_valid+likely_invalid})")
    
    # Collect unique missing pharmacies
    all_missing = {}
    for r in results:
        for mp in r['missing_pharmacies']:
            key = f"{mp['lat']:.5f},{mp['lng']:.5f}"
            if key not in all_missing:
                all_missing[key] = mp
    
    print(f"\nUnique missing pharmacies: {len(all_missing)}")
    
    # Insert into DB
    inserted = 0
    for key, mp in all_missing.items():
        existing = conn.execute(
            """SELECT latitude, longitude FROM pharmacies
               WHERE latitude BETWEEN ? AND ? AND longitude BETWEEN ? AND ?""",
            (mp['lat'] - 0.003, mp['lat'] + 0.003, mp['lng'] - 0.003, mp['lng'] + 0.003)
        ).fetchall()
        
        too_close = any(haversine(mp['lat'], mp['lng'], ex[0], ex[1]) < 0.3 for ex in existing if ex[0] and ex[1])
        
        if not too_close:
            conn.execute(
                """INSERT INTO pharmacies (name, address, latitude, longitude, source, date_scraped, suburb, state, postcode)
                   VALUES (?, ?, ?, ?, 'google_maps_browser', ?, '', '', '')""",
                (mp['name'], mp.get('address', f"({mp['lat']:.6f}, {mp['lng']:.6f})"),
                 mp['lat'], mp['lng'], datetime.now().isoformat())
            )
            inserted += 1
    
    conn.commit()
    new_total = conn.execute('SELECT COUNT(*) FROM pharmacies').fetchone()[0]
    print(f"Inserted {inserted} new pharmacies into DB")
    print(f"DB total: {new_total} pharmacies")
    conn.close()
    
    # Top invalidated
    invalidated = sorted([r for r in results if not r['still_valid']], key=lambda x: -(x.get('pop_10km') or 0))
    if invalidated:
        print(f"\nTop invalidated (by population):")
        for r in invalidated[:20]:
            print(f"  [{r['id']}] {r['name']} ({r['state']}) pop_10km={r['pop_10km']:,}")
            print(f"       {r['verdict_change']}")
    
    print(f"\nResults: {OUTPUT_PATH}")
    print(f"Cache: {CACHE_PATH}")
    return invalid, inserted

if __name__ == '__main__':
    invalid_count, inserted_count = main()
    print(f"\n{'='*70}")
    print(f"DONE: {invalid_count} invalidated, {inserted_count} pharmacies added to DB")
    print(f"{'='*70}")
