"""
PharmacyFinder — Google Maps Verification (SIMPLE)
No threading, just Playwright's native timeouts.
Saves after each chunk, continues from cache.
"""
import sys, os, json, math, time, re, sqlite3, random
from datetime import datetime
import urllib.parse

os.environ['PYTHONIOENCODING'] = 'utf-8'
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)

from playwright.sync_api import sync_playwright

WORKDIR = r'C:\Users\MJ\Documents\GitHub\PharmacyFinder'
DB_PATH = os.path.join(WORKDIR, 'pharmacy_finder.db')
SCORED_PATH = os.path.join(WORKDIR, 'output', 'scored_v2.json')
CACHE_PATH = os.path.join(WORKDIR, 'cache', 'gmaps_browser_cache.json')
OUTPUT_PATH = os.path.join(WORKDIR, 'output', 'verification_results.json')
MATCH_THRESHOLD_KM = 0.3

def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat, dlon = math.radians(lat2-lat1), math.radians(lon2-lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.asin(min(1, math.sqrt(a)))

def load_cache():
    return json.load(open(CACHE_PATH, 'r', encoding='utf-8')) if os.path.exists(CACHE_PATH) else {}

def save_cache(cache):
    os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)
    with open(CACHE_PATH, 'w', encoding='utf-8') as f:
        json.dump(cache, f, ensure_ascii=False)

def cache_lookup(cache, lat, lng, radius_km=2.0):
    key = f"{lat:.4f},{lng:.4f}"
    if key in cache:
        return cache[key]
    for ck, cv in cache.items():
        try:
            clat, clng = map(float, ck.split(','))
            if haversine(lat, lng, clat, clng) < radius_km:
                return cv
        except: pass
    return None

def scrape(page, lat, lng):
    """Scrape Google Maps pharmacy search. Returns list of results."""
    url = f"https://www.google.com/maps/search/pharmacy/@{lat},{lng},13z"
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=12000)
    except:
        return []
    
    time.sleep(2)
    
    # Consent
    try:
        btn = page.locator("button:has-text('Accept all')").first
        if btn.is_visible(timeout=1000):
            btn.click(); time.sleep(1)
    except: pass
    
    # Wait for results
    try:
        page.wait_for_selector('a[href*="/maps/place/"]', timeout=4000)
    except:
        return []
    
    time.sleep(0.5)
    
    # Scroll
    try:
        feed = page.locator('div[role="feed"]').first
        if feed.is_visible(timeout=800):
            for _ in range(2):
                feed.evaluate('el => el.scrollTop = el.scrollHeight')
                time.sleep(0.4)
    except: pass
    
    results = []
    seen = set()
    for link in page.locator('a[href*="/maps/place/"]').all():
        try:
            href = link.get_attribute('href') or ''
            lat_m = re.search(r'!3d(-?[\d.]+)', href)
            lng_m = re.search(r'!4d(-?[\d.]+)', href)
            if not lat_m or not lng_m: continue
            plat, plng = float(lat_m.group(1)), float(lng_m.group(1))
            ck = f"{plat:.5f},{plng:.5f}"
            if ck in seen: continue
            seen.add(ck)
            name_m = re.search(r'/maps/place/([^/]+)', href)
            name = urllib.parse.unquote(name_m.group(1).replace('+', ' ')) if name_m else 'Unknown'
            results.append({'name': name, 'lat': plat, 'lng': plng, 'address': ''})
        except: pass
    return results

def get_db_nearby(lat, lng, radius_km, conn):
    deg = radius_km / 111.0
    rows = conn.execute(
        "SELECT id,name,latitude,longitude FROM pharmacies WHERE latitude BETWEEN ? AND ? AND longitude BETWEEN ? AND ?",
        (lat-deg, lat+deg, lng-deg, lng+deg)).fetchall()
    return [{'id':r[0],'name':r[1],'lat':r[2],'lng':r[3],'distance_km':round(haversine(lat,lng,r[2],r[3]),2)}
            for r in rows if r[2] and r[3] and haversine(lat,lng,r[2],r[3]) <= radius_km]

def find_missing(gmaps, db, thresh=MATCH_THRESHOLD_KM):
    return [g for g in gmaps if not any(haversine(g['lat'],g['lng'],d['lat'],d['lng']) <= thresh for d in db)]

def main():
    t0 = time.time()
    print("=" * 70)
    print("PHARMACY VERIFICATION — GOOGLE MAPS (SIMPLE)")
    print("=" * 70)
    
    with open(SCORED_PATH, 'r', encoding='utf-8') as f:
        scored = json.load(f)
    opps = sorted([x for x in scored if x.get('verdict') in ('PASS','LIKELY')],
                  key=lambda x: (0 if x['verdict']=='PASS' else 1, -(x.get('pop_10km') or 0)))
    print(f"Opportunities: {len(opps)}")
    
    cache = load_cache()
    print(f"Cache: {len(cache)} entries")
    
    conn = sqlite3.connect(DB_PATH)
    all_results = []
    n_scraped = n_cached = 0
    
    # Process in chunks, fresh browser each chunk
    CHUNK = 50
    for chunk_start in range(0, len(opps), CHUNK):
        chunk = opps[chunk_start:chunk_start+CHUNK]
        elapsed = (time.time() - t0) / 60
        print(f"\n--- Chunk {chunk_start//CHUNK+1}: items {chunk_start+1}-{chunk_start+len(chunk)} of {len(opps)} ({elapsed:.1f}m elapsed) ---")
        
        pw = sync_playwright().start()
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width":1280,"height":900}, locale="en-AU",
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131.0.0.0")
        page = ctx.new_page()
        
        for opp in chunk:
            lat, lng = opp['lat'], opp['lng']
            cached = cache_lookup(cache, lat, lng)
            if cached is not None:
                gmaps = cached
                n_cached += 1
            else:
                gmaps = scrape(page, lat, lng)
                cache[f"{lat:.4f},{lng:.4f}"] = gmaps
                n_scraped += 1
                time.sleep(2.2 + random.uniform(0.3, 1.0))
            
            for g in gmaps:
                g['distance_km'] = round(haversine(lat, lng, g['lat'], g['lng']), 2)
            
            g15 = [g for g in gmaps if g['distance_km'] <= 15.0]
            g10 = [g for g in gmaps if g['distance_km'] <= 10.0]
            db15 = get_db_nearby(lat, lng, 15.0, conn)
            missing = find_missing(g15, db15)
            
            sp15, sp10 = opp.get('pharmacy_15km',0), opp.get('pharmacy_10km',0)
            ndb = opp.get('nearest_pharmacy_km',999)
            gn = min((g['distance_km'] for g in gmaps), default=999)
            
            still_valid = True
            if sp15 == 0 and len(g15) > 0:
                still_valid = False
                vc = f"INVALID (DB:0/15km G:{len(g15)} near:{gn:.1f}km)"
            elif sp10 == 0 and len(g10) > 0:
                still_valid = False
                vc = f"INVALID (DB:0/10km G:{len(g10)} in 10km)"
            elif ndb > 10 and gn < 5:
                still_valid = False
                vc = f"INVALID (DB near:{ndb:.1f} G near:{gn:.1f}km)"
            elif len(missing) > 0:
                vc = f"CONFIRMED* ({len(missing)} missing)"
            else:
                vc = f"CONFIRMED (DB:{len(db15)} G:{len(g15)})"
            
            all_results.append({
                'id': opp['id'], 'name': opp['name'], 'state': opp.get('state'),
                'lat': lat, 'lng': lng, 'original_verdict': opp['verdict'],
                'score': opp.get('score'), 'pop_10km': opp.get('pop_10km',0),
                'db_pharmacies_15km': len(db15), 'gmaps_pharmacies_15km': len(g15),
                'gmaps_nearest_km': gn, 'missing_count': len(missing),
                'missing_pharmacies': [{'name':m['name'],'lat':m['lat'],'lng':m['lng'],'distance_km':m['distance_km']} for m in missing],
                'verdict_change': f"{opp['verdict']}->{vc}", 'still_valid': still_valid,
            })
            
            if not still_valid:
                print(f"  INVALID: {opp['name']} ({opp['state']}) — {vc}")
        
        browser.close()
        pw.stop()
        
        # Save after each chunk
        save_cache(cache)
        with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
            json.dump(all_results, f, indent=2, ensure_ascii=False)
        print(f"  Chunk done. Total: {len(all_results)} results ({n_scraped} scraped, {n_cached} cached)")
    
    # Summary
    print("\n" + "="*70)
    total = len(all_results)
    valid = sum(1 for r in all_results if r['still_valid'])
    invalid = total - valid
    print(f"Total: {total} | Valid: {valid} ({valid/total*100:.1f}%) | Invalid: {invalid}")
    
    # Insert missing
    all_missing = {}
    for r in all_results:
        for mp in r['missing_pharmacies']:
            k = f"{mp['lat']:.5f},{mp['lng']:.5f}"
            if k not in all_missing: all_missing[k] = mp
    inserted = 0
    for mp in all_missing.values():
        ex = conn.execute("SELECT latitude,longitude FROM pharmacies WHERE latitude BETWEEN ? AND ? AND longitude BETWEEN ? AND ?",
            (mp['lat']-0.004, mp['lat']+0.004, mp['lng']-0.004, mp['lng']+0.004)).fetchall()
        if not any(haversine(mp['lat'],mp['lng'],e[0],e[1]) < 0.3 for e in ex if e[0] and e[1]):
            conn.execute("INSERT INTO pharmacies (name,address,latitude,longitude,source,date_scraped,suburb,state,postcode) VALUES (?,?,?,?,?,?,'','','')",
                (mp['name'], f"({mp['lat']:.6f},{mp['lng']:.6f})", mp['lat'], mp['lng'], 'google_maps_browser', datetime.now().isoformat()))
            inserted += 1
    conn.commit()
    print(f"Inserted {inserted} pharmacies (DB total: {conn.execute('SELECT COUNT(*) FROM pharmacies').fetchone()[0]})")
    conn.close()
    
    elapsed = (time.time() - t0) / 60
    print(f"\nDone in {elapsed:.1f}m ({n_scraped} scraped, {n_cached} cached)")
    return invalid, inserted

if __name__ == '__main__':
    inv, ins = main()
    print(f"\n{'='*70}\nDONE: {inv} invalidated | {ins} added\n{'='*70}")
