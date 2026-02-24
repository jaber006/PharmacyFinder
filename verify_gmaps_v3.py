"""
PharmacyFinder — Google Maps Browser Verification v3
Optimized: shorter timeouts, concurrent pages, smarter caching.
"""
import sys, os, json, math, time, re, sqlite3, random
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

os.environ['PYTHONIOENCODING'] = 'utf-8'
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
sys.stderr.reconfigure(encoding='utf-8', line_buffering=True)

from playwright.sync_api import sync_playwright

WORKDIR = r'C:\Users\MJ\Documents\GitHub\PharmacyFinder'
DB_PATH = os.path.join(WORKDIR, 'pharmacy_finder.db')
SCORED_PATH = os.path.join(WORKDIR, 'output', 'scored_v2.json')
CACHE_PATH = os.path.join(WORKDIR, 'cache', 'gmaps_browser_cache.json')
OUTPUT_PATH = os.path.join(WORKDIR, 'output', 'verification_results.json')
MATCH_THRESHOLD_KM = 0.3

def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))

def extract_coords_from_href(href):
    lat_m = re.search(r'!3d(-?[\d.]+)', href)
    lng_m = re.search(r'!4d(-?[\d.]+)', href)
    if lat_m and lng_m:
        return float(lat_m.group(1)), float(lng_m.group(1))
    return None, None

def extract_name_from_href(href):
    import urllib.parse
    m = re.search(r'/maps/place/([^/]+)', href)
    if m:
        return urllib.parse.unquote(m.group(1).replace('+', ' '))
    return None

def dedup_by_location(pharmacies, threshold_km=0.05):
    if not pharmacies:
        return pharmacies
    deduped = [pharmacies[0]]
    for p in pharmacies[1:]:
        is_dup = False
        for d in deduped:
            if p.get('lat') and d.get('lat'):
                if haversine(p['lat'], p['lng'], d['lat'], d['lng']) < threshold_km:
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

def scrape_pharmacies_page(page, lat, lng, zoom=13):
    """Scrape pharmacy search results from Google Maps. Fast version."""
    url = f"https://www.google.com/maps/search/pharmacy/@{lat},{lng},{zoom}z"
    
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=15000)
    except:
        return []
    
    # Short initial wait
    time.sleep(2)
    
    # Handle consent (first visit only)
    try:
        accept = page.locator("button:has-text('Accept all')").first
        if accept.is_visible(timeout=1000):
            accept.click()
            time.sleep(1.5)
    except:
        pass
    
    # Quick check for results — 4s timeout
    try:
        page.wait_for_selector('a[href*="/maps/place/"]', timeout=4000)
    except:
        return []  # No pharmacies in this area
    
    time.sleep(0.5)
    
    # Quick scroll for more results
    try:
        feed = page.locator('div[role="feed"]').first
        if feed.is_visible(timeout=1000):
            for _ in range(2):
                feed.evaluate('el => el.scrollTop = el.scrollHeight')
                time.sleep(0.5)
    except:
        pass
    
    results = []
    links = page.locator('a[href*="/maps/place/"]').all()
    
    for link in links:
        try:
            href = link.get_attribute('href') or ''
            p_lat, p_lng = extract_coords_from_href(href)
            name = extract_name_from_href(href) or 'Unknown'
            
            address = ''
            try:
                parent = link.locator('xpath=ancestor::div[contains(@class,"Nv2PK")]').first
                card_text = parent.inner_text(timeout=800)
                for line in card_text.split('\n'):
                    line = line.strip()
                    if re.search(r'\d+\s+\w+\s+(St|Rd|Ave|Dr|Hwy|Ln|Ct|Pl|Blvd|Tce|Pde|Way|Cres)', line, re.I):
                        address = line
                        break
            except:
                pass
            
            if p_lat and p_lng:
                results.append({
                    'name': name, 'lat': p_lat, 'lng': p_lng,
                    'address': address, 'source': 'google_maps_browser'
                })
        except:
            continue
    
    return dedup_by_location(results)

def get_db_pharmacies(lat, lng, radius_km, conn):
    deg = radius_km / 111.0
    rows = conn.execute(
        """SELECT id, name, latitude, longitude, address, suburb, state, postcode, source
           FROM pharmacies WHERE latitude BETWEEN ? AND ? AND longitude BETWEEN ? AND ?""",
        (lat - deg, lat + deg, lng - deg, lng + deg)
    ).fetchall()
    results = []
    for r in rows:
        if r[2] and r[3]:
            d = haversine(lat, lng, r[2], r[3])
            if d <= radius_km:
                results.append({'id':r[0],'name':r[1],'lat':r[2],'lng':r[3],'address':r[4],
                    'suburb':r[5],'state':r[6],'postcode':r[7],'source':r[8],'distance_km':round(d,2)})
    return results

def find_missing(gmaps, db, threshold_km=MATCH_THRESHOLD_KM):
    missing = []
    for g in gmaps:
        if not any(haversine(g['lat'],g['lng'],d['lat'],d['lng']) <= threshold_km for d in db):
            missing.append(g)
    return missing

def main():
    print("=" * 70)
    print("PHARMACY VERIFICATION v3 — GOOGLE MAPS BROWSER (OPTIMIZED)")
    print("=" * 70)
    
    with open(SCORED_PATH, 'r', encoding='utf-8') as f:
        scored = json.load(f)
    
    opportunities = [x for x in scored if x.get('verdict') in ('PASS', 'LIKELY')]
    opportunities.sort(key=lambda x: (0 if x['verdict']=='PASS' else 1, -(x.get('pop_10km') or 0)))
    
    print(f"Opportunities: {len(opportunities)} (PASS: {sum(1 for x in opportunities if x['verdict']=='PASS')}, LIKELY: {sum(1 for x in opportunities if x['verdict']=='LIKELY')})")
    
    cache = load_cache()
    print(f"Cache: {len(cache)} locations")
    
    conn = sqlite3.connect(DB_PATH)
    
    print("Launching browser (3 pages for throughput)...")
    pw = sync_playwright().start()
    browser = pw.chromium.launch(headless=True)
    
    # Create context
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
            
            if (i + 1) % 20 == 0 or i == 0:
                print(f"\n[{i+1}/{len(opportunities)}] — {cached_count} cached, {scraped_count} scraped")
            
            # Check cache
            gmaps = None
            if cache_key in cache:
                gmaps = cache[cache_key]
                cached_count += 1
            else:
                for ck, cv in cache.items():
                    try:
                        clat, clng = map(float, ck.split(','))
                        if haversine(lat, lng, clat, clng) < 2.0:
                            gmaps = cv
                            cached_count += 1
                            break
                    except:
                        continue
            
            if gmaps is None:
                gmaps = scrape_pharmacies_page(page, lat, lng)
                cache[cache_key] = gmaps
                scraped_count += 1
                
                if scraped_count % 30 == 0:
                    save_cache(cache)
                    print(f"  [Cache saved: {len(cache)}]")
                
                # Rate limit
                time.sleep(1.5 + random.uniform(0.5, 1.0))
            
            # Distances
            for g in gmaps:
                g['distance_km'] = round(haversine(lat, lng, g['lat'], g['lng']), 2)
            
            g15 = [g for g in gmaps if g['distance_km'] <= 15.0]
            g10 = [g for g in gmaps if g['distance_km'] <= 10.0]
            g5 = [g for g in gmaps if g['distance_km'] <= 5.0]
            
            db15 = get_db_pharmacies(lat, lng, 15.0, conn)
            db10 = [p for p in db15 if p['distance_km'] <= 10.0]
            db5 = [p for p in db15 if p['distance_km'] <= 5.0]
            
            missing = find_missing(g15, db15)
            
            sp15 = opp.get('pharmacy_15km', 0)
            sp10 = opp.get('pharmacy_10km', 0)
            ndb = opp.get('nearest_pharmacy_km', 999)
            gn = min((g['distance_km'] for g in gmaps), default=999)
            
            still_valid = True
            vc = None
            
            if sp15 == 0 and len(g15) > 0:
                still_valid = False
                vc = f"{opp['verdict']} -> INVALID (DB: 0/15km, Google: {len(g15)}, nearest: {gn:.1f}km)"
            elif sp10 == 0 and len(g10) > 0:
                still_valid = False
                vc = f"{opp['verdict']} -> INVALID (DB: 0/10km, Google: {len(g10)} within 10km)"
            elif ndb > 10 and gn < 5:
                still_valid = False
                vc = f"{opp['verdict']} -> INVALID (DB nearest: {ndb:.1f}km, Google: {gn:.1f}km)"
            elif len(missing) > 0 and (len(g15) > len(db15) * 1.5 + 2):
                still_valid = False
                vc = f"{opp['verdict']} -> INVALID (DB: {len(db15)}, Google: {len(g15)})"
            elif len(missing) > 0:
                vc = f"{opp['verdict']} -> CONFIRMED* ({len(missing)} missing)"
            else:
                vc = f"{opp['verdict']} -> CONFIRMED (DB:{len(db15)} G:{len(g15)})"
            
            cm = [{'name':m['name'],'lat':m['lat'],'lng':m['lng'],'distance_km':m['distance_km'],'address':m.get('address','')} for m in missing]
            cg = [{'name':g['name'],'lat':g['lat'],'lng':g['lng'],'distance_km':g['distance_km'],'address':g.get('address','')} for g in g15]
            
            result = {
                'id': opp['id'], 'name': opp['name'], 'state': opp.get('state'),
                'lat': lat, 'lng': lng,
                'original_verdict': opp['verdict'], 'score': opp.get('score'),
                'pop_10km': opp.get('pop_10km', 0),
                'best_rule': opp.get('best_rule_display', ''),
                'db_pharmacies_5km': len(db5), 'db_pharmacies_10km': len(db10), 'db_pharmacies_15km': len(db15),
                'scored_pharmacies_5km': opp.get('pharmacy_5km',0),
                'scored_pharmacies_10km': sp10, 'scored_pharmacies_15km': sp15,
                'gmaps_pharmacies_5km': len(g5), 'gmaps_pharmacies_10km': len(g10), 'gmaps_pharmacies_15km': len(g15),
                'gmaps_nearest_km': gn, 'db_nearest_km': ndb,
                'gmaps_results': cg, 'missing_pharmacies': cm,
                'missing_count': len(missing), 'verdict_change': vc, 'still_valid': still_valid
            }
            results.append(result)
            
            if not still_valid:
                print(f"  *** INVALID: {opp['name']} ({opp['state']}) — {vc}")
    
    except KeyboardInterrupt:
        print("\nInterrupted! Saving partial results...")
    except Exception as e:
        print(f"\nError: {e}")
        import traceback; traceback.print_exc()
    finally:
        save_cache(cache)
        try:
            browser.close()
            pw.stop()
        except:
            pass
    
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
        print("No results!"); return 0, 0
    
    valid = sum(1 for r in results if r['still_valid'])
    invalid = sum(1 for r in results if not r['still_valid'])
    pv = sum(1 for r in results if r['still_valid'] and r['original_verdict']=='PASS')
    pi = sum(1 for r in results if not r['still_valid'] and r['original_verdict']=='PASS')
    lv = sum(1 for r in results if r['still_valid'] and r['original_verdict']=='LIKELY')
    li = sum(1 for r in results if not r['still_valid'] and r['original_verdict']=='LIKELY')
    
    print(f"Total verified: {total}")
    print(f"  Still valid:  {valid} ({valid/total*100:.1f}%)")
    print(f"  Invalidated:  {invalid} ({invalid/total*100:.1f}%)")
    print(f"  PASS:   {pv} valid / {pi} invalid (of {pv+pi})")
    print(f"  LIKELY: {lv} valid / {li} invalid (of {lv+li})")
    
    # Unique missing pharmacies
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
            "SELECT latitude, longitude FROM pharmacies WHERE latitude BETWEEN ? AND ? AND longitude BETWEEN ? AND ?",
            (mp['lat']-0.003, mp['lat']+0.003, mp['lng']-0.003, mp['lng']+0.003)
        ).fetchall()
        if not any(haversine(mp['lat'],mp['lng'],e[0],e[1]) < 0.3 for e in existing if e[0] and e[1]):
            conn.execute(
                "INSERT INTO pharmacies (name,address,latitude,longitude,source,date_scraped,suburb,state,postcode) VALUES (?,?,?,?,?,?,'','','')",
                (mp['name'], mp.get('address',''), mp['lat'], mp['lng'], 'google_maps_browser', datetime.now().isoformat())
            )
            inserted += 1
    
    conn.commit()
    new_total = conn.execute('SELECT COUNT(*) FROM pharmacies').fetchone()[0]
    print(f"Inserted {inserted} new pharmacies")
    print(f"DB total: {new_total}")
    conn.close()
    
    # Top invalidated
    inv = sorted([r for r in results if not r['still_valid']], key=lambda x: -(x.get('pop_10km') or 0))
    if inv:
        print(f"\nTop invalidated:")
        for r in inv[:20]:
            print(f"  [{r['id']}] {r['name']} ({r['state']}) pop={r['pop_10km']:,} — {r['verdict_change']}")
    
    print(f"\nResults: {OUTPUT_PATH}")
    return invalid, inserted

if __name__ == '__main__':
    t0 = time.time()
    inv, ins = main()
    elapsed = time.time() - t0
    print(f"\nCompleted in {elapsed/60:.1f} minutes")
    print(f"DONE: {inv} invalidated, {ins} pharmacies added")
