"""
PharmacyFinder — Google Maps Browser Verification (Robust)
Each scrape has a hard 20s timeout via context manager.
Batches work in chunks, saving results between chunks.
"""
import sys, os, json, math, time, re, sqlite3, random, signal
from datetime import datetime
import urllib.parse
import threading

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
    a = (math.sin(dlat/2)**2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2)
    return R * 2 * math.asin(min(1, math.sqrt(a)))

def load_cache():
    if os.path.exists(CACHE_PATH):
        with open(CACHE_PATH, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_cache(cache):
    os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)
    tmp = CACHE_PATH + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(cache, f, ensure_ascii=False)
    os.replace(tmp, CACHE_PATH)

def cache_lookup(cache, lat, lng, radius_km=2.0):
    key = f"{lat:.4f},{lng:.4f}"
    if key in cache:
        return cache[key]
    for ck, cv in cache.items():
        try:
            clat, clng = map(float, ck.split(','))
            if haversine(lat, lng, clat, clng) < radius_km:
                return cv
        except:
            continue
    return None

def scrape_with_timeout(page, lat, lng, timeout_sec=20):
    """Scrape with a hard timeout. Returns [] on timeout."""
    result_holder = [None]
    error_holder = [None]
    
    def do_scrape():
        try:
            result_holder[0] = _scrape(page, lat, lng)
        except Exception as e:
            error_holder[0] = e
            result_holder[0] = []
    
    t = threading.Thread(target=do_scrape, daemon=True)
    t.start()
    t.join(timeout=timeout_sec)
    
    if t.is_alive():
        # Timed out — try to navigate away to unstick
        try:
            page.goto("about:blank", timeout=3000)
        except:
            pass
        return []
    
    return result_holder[0] if result_holder[0] is not None else []

def _scrape(page, lat, lng):
    """Core Google Maps scrape logic."""
    url = f"https://www.google.com/maps/search/pharmacy/@{lat},{lng},13z"
    page.goto(url, wait_until="domcontentloaded", timeout=12000)
    time.sleep(2)
    
    # Consent
    try:
        btn = page.locator("button:has-text('Accept all')").first
        if btn.is_visible(timeout=1000):
            btn.click()
            time.sleep(1)
    except:
        pass
    
    # Wait for results
    try:
        page.wait_for_selector('a[href*="/maps/place/"]', timeout=4000)
    except:
        return []
    
    time.sleep(0.3)
    
    # Scroll feed
    try:
        feed = page.locator('div[role="feed"]').first
        if feed.is_visible(timeout=800):
            for _ in range(2):
                feed.evaluate('el => el.scrollTop = el.scrollHeight')
                time.sleep(0.4)
    except:
        pass
    
    results = []
    seen = set()
    for link in page.locator('a[href*="/maps/place/"]').all():
        try:
            href = link.get_attribute('href') or ''
            lat_m = re.search(r'!3d(-?[\d.]+)', href)
            lng_m = re.search(r'!4d(-?[\d.]+)', href)
            if not lat_m or not lng_m:
                continue
            plat, plng = float(lat_m.group(1)), float(lng_m.group(1))
            ck = f"{plat:.5f},{plng:.5f}"
            if ck in seen:
                continue
            seen.add(ck)
            
            name_m = re.search(r'/maps/place/([^/]+)', href)
            name = urllib.parse.unquote(name_m.group(1).replace('+', ' ')) if name_m else 'Unknown'
            
            address = ''
            try:
                parent = link.locator('xpath=ancestor::div[contains(@class,"Nv2PK")]').first
                card = parent.inner_text(timeout=600)
                for line in card.split('\n'):
                    s = line.strip()
                    if re.search(r'\d+[/-]?\d*\s+\w+\s+(St|Rd|Ave|Dr|Hwy|Ln|Ct|Pl|Blvd|Tce|Pde|Way|Cres|Cct)', s, re.I):
                        address = s
                        break
            except:
                pass
            
            results.append({'name': name, 'lat': plat, 'lng': plng, 'address': address})
        except:
            continue
    
    return results

def get_db_nearby(lat, lng, radius_km, conn):
    deg = radius_km / 111.0
    rows = conn.execute(
        "SELECT id,name,latitude,longitude,address,suburb,state,postcode,source "
        "FROM pharmacies WHERE latitude BETWEEN ? AND ? AND longitude BETWEEN ? AND ?",
        (lat-deg, lat+deg, lng-deg, lng+deg)).fetchall()
    out = []
    for r in rows:
        if r[2] and r[3]:
            d = haversine(lat, lng, r[2], r[3])
            if d <= radius_km:
                out.append({'id':r[0],'name':r[1],'lat':r[2],'lng':r[3],
                    'address':r[4],'distance_km':round(d,2)})
    return out

def find_missing(gmaps, db, threshold=MATCH_THRESHOLD_KM):
    return [g for g in gmaps
            if not any(haversine(g['lat'],g['lng'],d['lat'],d['lng']) <= threshold for d in db)]

def main():
    t_start = time.time()
    print("=" * 70)
    print("PHARMACY VERIFICATION — GOOGLE MAPS (ROBUST)")
    print("=" * 70)

    with open(SCORED_PATH, 'r', encoding='utf-8') as f:
        scored = json.load(f)
    opps = [x for x in scored if x.get('verdict') in ('PASS','LIKELY')]
    opps.sort(key=lambda x: (0 if x['verdict']=='PASS' else 1, -(x.get('pop_10km') or 0)))
    print(f"Opportunities: {len(opps)}  (PASS: {sum(1 for x in opps if x['verdict']=='PASS')}, LIKELY: {sum(1 for x in opps if x['verdict']=='LIKELY')})")

    cache = load_cache()
    print(f"Cache: {len(cache)} entries loaded from previous run")

    conn = sqlite3.connect(DB_PATH)

    # Process in chunks of 50 with fresh browser each chunk
    CHUNK = 50
    all_results = []
    total_scraped = 0
    total_cached = 0

    for chunk_start in range(0, len(opps), CHUNK):
        chunk_end = min(chunk_start + CHUNK, len(opps))
        chunk = opps[chunk_start:chunk_end]
        
        elapsed = time.time() - t_start
        print(f"\n--- Chunk {chunk_start//CHUNK + 1}: items {chunk_start+1}-{chunk_end} of {len(opps)} (elapsed: {elapsed/60:.1f}m) ---")
        
        # Fresh browser for each chunk
        pw = sync_playwright().start()
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(
            viewport={"width":1280,"height":900}, locale="en-AU",
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36")
        page = ctx.new_page()
        
        chunk_scraped = 0
        chunk_cached = 0
        
        try:
            for j, opp in enumerate(chunk):
                i = chunk_start + j
                lat, lng = opp['lat'], opp['lng']
                cache_key = f"{lat:.4f},{lng:.4f}"
                
                # Cache check
                cached = cache_lookup(cache, lat, lng)
                if cached is not None:
                    gmaps = cached
                    chunk_cached += 1
                    total_cached += 1
                else:
                    gmaps = scrape_with_timeout(page, lat, lng, timeout_sec=20)
                    cache[cache_key] = gmaps
                    chunk_scraped += 1
                    total_scraped += 1
                    time.sleep(2.0 + random.uniform(0.3, 1.0))
                
                # Distances
                for g in gmaps:
                    g['distance_km'] = round(haversine(lat, lng, g['lat'], g['lng']), 2)
                
                g15 = [g for g in gmaps if g['distance_km'] <= 15.0]
                g10 = [g for g in gmaps if g['distance_km'] <= 10.0]
                g5  = [g for g in gmaps if g['distance_km'] <= 5.0]
                
                db15 = get_db_nearby(lat, lng, 15.0, conn)
                db10 = [d for d in db15 if d['distance_km'] <= 10.0]
                db5  = [d for d in db15 if d['distance_km'] <= 5.0]
                
                missing = find_missing(g15, db15)
                
                sp15 = opp.get('pharmacy_15km', 0)
                sp10 = opp.get('pharmacy_10km', 0)
                ndb  = opp.get('nearest_pharmacy_km', 999)
                gn   = min((g['distance_km'] for g in gmaps), default=999)
                
                still_valid = True
                vc = None
                
                if sp15 == 0 and len(g15) > 0:
                    still_valid = False
                    vc = f"{opp['verdict']}->INVALID (DB:0/15km G:{len(g15)} near:{gn:.1f}km)"
                elif sp10 == 0 and len(g10) > 0:
                    still_valid = False
                    vc = f"{opp['verdict']}->INVALID (DB:0/10km G:{len(g10)} in 10km)"
                elif ndb > 10 and gn < 5:
                    still_valid = False
                    vc = f"{opp['verdict']}->INVALID (DB near:{ndb:.1f}km G near:{gn:.1f}km)"
                elif len(missing) > 0 and len(g15) > len(db15) * 1.5 + 2:
                    still_valid = False
                    vc = f"{opp['verdict']}->INVALID (DB:{len(db15)} G:{len(g15)} gap)"
                elif len(missing) > 0:
                    vc = f"{opp['verdict']}->CONFIRMED* ({len(missing)} missing)"
                else:
                    vc = f"{opp['verdict']}->CONFIRMED (DB:{len(db15)} G:{len(g15)})"
                
                cl = lambda lst: [{'name':x['name'],'lat':x['lat'],'lng':x['lng'],
                                   'distance_km':x['distance_km'],'address':x.get('address','')} for x in lst]
                
                result = {
                    'id': opp['id'], 'name': opp['name'], 'state': opp.get('state'),
                    'lat': lat, 'lng': lng,
                    'original_verdict': opp['verdict'], 'score': opp.get('score'),
                    'pop_10km': opp.get('pop_10km', 0),
                    'best_rule': opp.get('best_rule_display', ''),
                    'db_pharmacies_5km': len(db5), 'db_pharmacies_10km': len(db10),
                    'db_pharmacies_15km': len(db15),
                    'scored_pharmacies_5km': opp.get('pharmacy_5km',0),
                    'scored_pharmacies_10km': sp10, 'scored_pharmacies_15km': sp15,
                    'gmaps_pharmacies_5km': len(g5), 'gmaps_pharmacies_10km': len(g10),
                    'gmaps_pharmacies_15km': len(g15),
                    'gmaps_nearest_km': gn, 'db_nearest_km': ndb,
                    'gmaps_results': cl(g15), 'missing_pharmacies': cl(missing),
                    'missing_count': len(missing), 'verdict_change': vc,
                    'still_valid': still_valid,
                }
                all_results.append(result)
                
                if not still_valid:
                    print(f"  INVALID: {opp['name']} ({opp['state']}) — {vc}")
        
        except Exception as e:
            print(f"  Chunk error: {e}")
            import traceback; traceback.print_exc()
        
        finally:
            try: browser.close()
            except: pass
            try: pw.stop()
            except: pass
        
        # Save cache and interim results after each chunk
        save_cache(cache)
        with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
            json.dump(all_results, f, indent=2, ensure_ascii=False)
        
        print(f"  Chunk done: {chunk_scraped} scraped, {chunk_cached} cached. Total: {len(all_results)} results saved.")

    # ── Summary ──────────────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("VERIFICATION SUMMARY")
    print("=" * 70)
    
    total = len(all_results)
    valid = sum(1 for r in all_results if r['still_valid'])
    invalid = sum(1 for r in all_results if not r['still_valid'])
    pv = sum(1 for r in all_results if r['still_valid'] and r['original_verdict']=='PASS')
    pi = sum(1 for r in all_results if not r['still_valid'] and r['original_verdict']=='PASS')
    lv = sum(1 for r in all_results if r['still_valid'] and r['original_verdict']=='LIKELY')
    li = sum(1 for r in all_results if not r['still_valid'] and r['original_verdict']=='LIKELY')
    
    print(f"Total verified: {total}")
    print(f"  Valid:   {valid} ({valid/total*100:.1f}%)")
    print(f"  Invalid: {invalid} ({invalid/total*100:.1f}%)")
    print(f"  PASS:   {pv} valid / {pi} invalid (of {pv+pi})")
    print(f"  LIKELY: {lv} valid / {li} invalid (of {lv+li})")
    
    # Insert missing pharmacies
    all_missing = {}
    for r in all_results:
        for mp in r['missing_pharmacies']:
            key = f"{mp['lat']:.5f},{mp['lng']:.5f}"
            if key not in all_missing:
                all_missing[key] = mp
    print(f"\nUnique missing pharmacies: {len(all_missing)}")
    
    inserted = 0
    for key, mp in all_missing.items():
        existing = conn.execute(
            "SELECT latitude, longitude FROM pharmacies "
            "WHERE latitude BETWEEN ? AND ? AND longitude BETWEEN ? AND ?",
            (mp['lat']-0.004, mp['lat']+0.004, mp['lng']-0.004, mp['lng']+0.004)
        ).fetchall()
        if not any(haversine(mp['lat'],mp['lng'],e[0],e[1]) < 0.3
                   for e in existing if e[0] and e[1]):
            conn.execute(
                "INSERT INTO pharmacies (name,address,latitude,longitude,source,date_scraped,suburb,state,postcode) "
                "VALUES (?,?,?,?,?,?,'','','')",
                (mp['name'], mp.get('address','') or f"({mp['lat']:.6f},{mp['lng']:.6f})",
                 mp['lat'], mp['lng'], 'google_maps_browser', datetime.now().isoformat()))
            inserted += 1
    conn.commit()
    
    new_total = conn.execute('SELECT COUNT(*) FROM pharmacies').fetchone()[0]
    print(f"Inserted {inserted} new pharmacies (DB total: {new_total})")
    conn.close()
    
    # Top invalidated
    inv_list = sorted([r for r in all_results if not r['still_valid']], key=lambda x: -(x.get('pop_10km') or 0))
    if inv_list:
        print(f"\nTop invalidated:")
        for r in inv_list[:20]:
            print(f"  [{r['id']}] {r['name']} ({r['state']}) pop={r['pop_10km']:,}")
            print(f"       {r['verdict_change']}")
    
    elapsed = time.time() - t_start
    print(f"\nDone in {elapsed/60:.1f}m ({total_scraped} scraped, {total_cached} cached)")
    return invalid, inserted

if __name__ == '__main__':
    inv, ins = main()
    print(f"\n{'='*70}")
    print(f"DONE: {inv} invalidated | {ins} pharmacies added")
    print(f"{'='*70}")
