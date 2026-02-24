"""
PharmacyFinder — Google Maps Browser Verification (Final)
Scrapes Google Maps pharmacy search results via Playwright headless browser.
Compares with our pharmacy_finder.db to find gaps and invalidate false positives.
"""
import sys, os, json, math, time, re, sqlite3, random
from datetime import datetime
import urllib.parse

os.environ['PYTHONIOENCODING'] = 'utf-8'
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)
sys.stderr.reconfigure(encoding='utf-8', line_buffering=True)

from playwright.sync_api import sync_playwright

WORKDIR = r'C:\Users\MJ\Documents\GitHub\PharmacyFinder'
DB_PATH = os.path.join(WORKDIR, 'pharmacy_finder.db')
SCORED_PATH = os.path.join(WORKDIR, 'output', 'scored_v2.json')
CACHE_PATH = os.path.join(WORKDIR, 'cache', 'gmaps_browser_cache.json')
OUTPUT_PATH = os.path.join(WORKDIR, 'output', 'verification_results.json')
MATCH_THRESHOLD_KM = 0.3  # 300m

def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat/2)**2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2)
    return R * 2 * math.asin(min(1, math.sqrt(a)))

# ── Cache ────────────────────────────────────────────────────────────────────
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
    """Return cached result if a query within radius_km exists, else None."""
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

# ── Google Maps Scraping ─────────────────────────────────────────────────────
def scrape_gmaps(page, lat, lng, zoom=13):
    """Search Google Maps for pharmacies near lat/lng, return list of dicts."""
    url = f"https://www.google.com/maps/search/pharmacy/@{lat},{lng},{zoom}z"
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=15000)
    except Exception as e:
        # Retry once
        time.sleep(3)
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=15000)
        except:
            return []

    time.sleep(2.5)

    # Consent dialog (only appears once per context)
    try:
        btn = page.locator("button:has-text('Accept all')").first
        if btn.is_visible(timeout=1200):
            btn.click()
            time.sleep(1.5)
    except:
        pass

    # Wait for place links
    try:
        page.wait_for_selector('a[href*="/maps/place/"]', timeout=5000)
    except:
        return []  # No results — legitimate empty area

    time.sleep(0.5)

    # Scroll the feed to load more (up to 20 results)
    try:
        feed = page.locator('div[role="feed"]').first
        if feed.is_visible(timeout=1000):
            for _ in range(3):
                feed.evaluate('el => el.scrollTop = el.scrollHeight')
                time.sleep(0.6)
    except:
        pass

    # Extract results from place links
    results = []
    seen_coords = set()
    for link in page.locator('a[href*="/maps/place/"]').all():
        try:
            href = link.get_attribute('href') or ''
            lat_m = re.search(r'!3d(-?[\d.]+)', href)
            lng_m = re.search(r'!4d(-?[\d.]+)', href)
            if not lat_m or not lng_m:
                continue
            plat = float(lat_m.group(1))
            plng = float(lng_m.group(1))

            # Dedup by rounded coords (same building)
            coord_key = f"{plat:.5f},{plng:.5f}"
            if coord_key in seen_coords:
                continue
            seen_coords.add(coord_key)

            # Name from URL
            name_m = re.search(r'/maps/place/([^/]+)', href)
            name = urllib.parse.unquote(name_m.group(1).replace('+', ' ')) if name_m else 'Unknown'

            # Try to get address from card text
            address = ''
            try:
                parent = link.locator('xpath=ancestor::div[contains(@class,"Nv2PK")]').first
                card = parent.inner_text(timeout=800)
                # Look for a street address line
                for line in card.split('\n'):
                    s = line.strip()
                    if re.search(r'\d+[/-]?\d*\s+\w+\s+(St|Rd|Ave|Dr|Hwy|Ln|Ct|Pl|Blvd|Tce|Pde|Way|Cres|Cct|Mwy)', s, re.I):
                        address = s
                        break
            except:
                pass

            results.append({
                'name': name,
                'lat': plat,
                'lng': plng,
                'address': address,
            })
        except:
            continue

    return results

# ── DB helpers ───────────────────────────────────────────────────────────────
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
                    'address':r[4],'suburb':r[5],'state':r[6],'postcode':r[7],
                    'source':r[8],'distance_km':round(d,2)})
    return out

def find_missing(gmaps, db, threshold=MATCH_THRESHOLD_KM):
    missing = []
    for g in gmaps:
        if not any(haversine(g['lat'],g['lng'],d['lat'],d['lng']) <= threshold for d in db):
            missing.append(g)
    return missing

# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    t_start = time.time()
    print("=" * 70)
    print("PHARMACY VERIFICATION — GOOGLE MAPS BROWSER SCRAPING")
    print("=" * 70)

    # Load opportunities
    with open(SCORED_PATH, 'r', encoding='utf-8') as f:
        scored = json.load(f)
    opps = [x for x in scored if x.get('verdict') in ('PASS','LIKELY')]
    opps.sort(key=lambda x: (0 if x['verdict']=='PASS' else 1, -(x.get('pop_10km') or 0)))

    n_pass = sum(1 for x in opps if x['verdict']=='PASS')
    n_likely = sum(1 for x in opps if x['verdict']=='LIKELY')
    print(f"Opportunities: {len(opps)}  (PASS: {n_pass}  LIKELY: {n_likely})")

    cache = load_cache()
    print(f"Cache: {len(cache)} locations loaded")

    conn = sqlite3.connect(DB_PATH)

    # Launch browser
    print("Launching headless browser...")
    pw = sync_playwright().start()
    browser = pw.chromium.launch(headless=True)
    ctx = browser.new_context(
        viewport={"width": 1280, "height": 900}, locale="en-AU",
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
    page = ctx.new_page()

    results = []
    n_cached = 0
    n_scraped = 0

    try:
        for i, opp in enumerate(opps):
            lat, lng = opp['lat'], opp['lng']
            cache_key = f"{lat:.4f},{lng:.4f}"

            # Progress every 20
            if i % 20 == 0:
                elapsed = time.time() - t_start
                eta = (elapsed / max(i,1)) * (len(opps) - i) / 60 if i > 0 else 0
                print(f"\n[{i+1}/{len(opps)}]  scraped={n_scraped}  cached={n_cached}  "
                      f"elapsed={elapsed/60:.1f}m  eta={eta:.0f}m")

            # Cache check
            cached = cache_lookup(cache, lat, lng)
            if cached is not None:
                gmaps = cached
                n_cached += 1
            else:
                gmaps = scrape_gmaps(page, lat, lng)
                cache[cache_key] = gmaps
                n_scraped += 1
                # Save cache every 30 scrapes
                if n_scraped % 30 == 0:
                    save_cache(cache)
                    print(f"  [cache saved: {len(cache)} entries]")
                # Rate limit
                time.sleep(2.0 + random.uniform(0.3, 1.2))

            # Add distances from opportunity
            for g in gmaps:
                g['distance_km'] = round(haversine(lat, lng, g['lat'], g['lng']), 2)

            # Filter by radius
            g15 = [g for g in gmaps if g['distance_km'] <= 15.0]
            g10 = [g for g in gmaps if g['distance_km'] <= 10.0]
            g5  = [g for g in gmaps if g['distance_km'] <= 5.0]

            # DB pharmacies
            db15 = get_db_nearby(lat, lng, 15.0, conn)
            db10 = [d for d in db15 if d['distance_km'] <= 10.0]
            db5  = [d for d in db15 if d['distance_km'] <= 5.0]

            missing = find_missing(g15, db15)

            # Scored values
            sp15 = opp.get('pharmacy_15km', 0)
            sp10 = opp.get('pharmacy_10km', 0)
            sp5  = opp.get('pharmacy_5km', 0)
            ndb  = opp.get('nearest_pharmacy_km', 999)
            gn   = min((g['distance_km'] for g in gmaps), default=999)

            # ── Verdict logic ──
            still_valid = True
            vc = None

            if sp15 == 0 and len(g15) > 0:
                still_valid = False
                vc = (f"{opp['verdict']} -> INVALID  "
                      f"(DB: 0/15km, Google: {len(g15)}, nearest: {gn:.1f}km)")
            elif sp10 == 0 and len(g10) > 0:
                still_valid = False
                vc = (f"{opp['verdict']} -> INVALID  "
                      f"(DB: 0/10km, Google: {len(g10)} within 10km)")
            elif ndb > 10 and gn < 5:
                still_valid = False
                vc = (f"{opp['verdict']} -> INVALID  "
                      f"(DB nearest: {ndb:.1f}km, Google nearest: {gn:.1f}km)")
            elif len(missing) > 0 and len(g15) > len(db15) * 1.5 + 2:
                still_valid = False
                vc = (f"{opp['verdict']} -> INVALID  "
                      f"(DB: {len(db15)}, Google: {len(g15)} — significant gap)")
            elif len(missing) > 0:
                vc = f"{opp['verdict']} -> CONFIRMED*  ({len(missing)} missing from DB)"
            else:
                vc = f"{opp['verdict']} -> CONFIRMED  (DB:{len(db15)} G:{len(g15)})"

            # Build result record
            clean = lambda lst: [{'name':x['name'],'lat':x['lat'],'lng':x['lng'],
                                  'distance_km':x['distance_km'],
                                  'address':x.get('address','')} for x in lst]
            result = {
                'id': opp['id'], 'name': opp['name'], 'state': opp.get('state'),
                'lat': lat, 'lng': lng,
                'original_verdict': opp['verdict'],
                'score': opp.get('score'),
                'pop_10km': opp.get('pop_10km', 0),
                'best_rule': opp.get('best_rule_display', ''),
                'db_pharmacies_5km': len(db5),
                'db_pharmacies_10km': len(db10),
                'db_pharmacies_15km': len(db15),
                'scored_pharmacies_5km': sp5,
                'scored_pharmacies_10km': sp10,
                'scored_pharmacies_15km': sp15,
                'gmaps_pharmacies_5km': len(g5),
                'gmaps_pharmacies_10km': len(g10),
                'gmaps_pharmacies_15km': len(g15),
                'gmaps_nearest_km': gn,
                'db_nearest_km': ndb,
                'gmaps_results': clean(g15),
                'missing_pharmacies': clean(missing),
                'missing_count': len(missing),
                'verdict_change': vc,
                'still_valid': still_valid,
            }
            results.append(result)

            if not still_valid:
                print(f"  *** INVALID: {opp['name']} ({opp['state']})  — {vc}")

    except KeyboardInterrupt:
        print("\n\nInterrupted — saving partial results...")
    except Exception as e:
        print(f"\n\nERROR: {e}")
        import traceback; traceback.print_exc()
    finally:
        save_cache(cache)
        print(f"Cache saved ({len(cache)} entries)")
        try: browser.close()
        except: pass
        try: pw.stop()
        except: pass

    # ── Save results ─────────────────────────────────────────────────────────
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    # ── Summary ──────────────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("VERIFICATION SUMMARY")
    print("=" * 70)

    total = len(results)
    if total == 0:
        print("No results generated."); conn.close(); return 0, 0

    valid   = sum(1 for r in results if r['still_valid'])
    invalid = sum(1 for r in results if not r['still_valid'])
    pv = sum(1 for r in results if r['still_valid'] and r['original_verdict']=='PASS')
    pi = sum(1 for r in results if not r['still_valid'] and r['original_verdict']=='PASS')
    lv = sum(1 for r in results if r['still_valid'] and r['original_verdict']=='LIKELY')
    li = sum(1 for r in results if not r['still_valid'] and r['original_verdict']=='LIKELY')

    print(f"Total verified:  {total}")
    print(f"  Still valid:   {valid}  ({valid/total*100:.1f}%)")
    print(f"  Invalidated:   {invalid}  ({invalid/total*100:.1f}%)")
    print(f"  PASS:   {pv} valid / {pi} invalid  (of {pv+pi})")
    print(f"  LIKELY: {lv} valid / {li} invalid  (of {lv+li})")

    # ── Insert missing pharmacies into DB ────────────────────────────────────
    all_missing = {}
    for r in results:
        for mp in r['missing_pharmacies']:
            key = f"{mp['lat']:.5f},{mp['lng']:.5f}"
            if key not in all_missing:
                all_missing[key] = mp
    print(f"\nUnique missing pharmacies found: {len(all_missing)}")

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
                "INSERT INTO pharmacies "
                "(name,address,latitude,longitude,source,date_scraped,suburb,state,postcode) "
                "VALUES (?,?,?,?,?,?,'','','')",
                (mp['name'], mp.get('address','') or f"({mp['lat']:.6f},{mp['lng']:.6f})",
                 mp['lat'], mp['lng'], 'google_maps_browser', datetime.now().isoformat()))
            inserted += 1
    conn.commit()

    new_total = conn.execute('SELECT COUNT(*) FROM pharmacies').fetchone()[0]
    print(f"Inserted {inserted} new pharmacies  (DB total: {new_total})")
    conn.close()

    # Top invalidated
    inv_list = sorted([r for r in results if not r['still_valid']],
                      key=lambda x: -(x.get('pop_10km') or 0))
    if inv_list:
        print(f"\nTop invalidated (by pop_10km):")
        for r in inv_list[:20]:
            print(f"  [{r['id']}] {r['name']} ({r['state']})  pop={r['pop_10km']:,}")
            print(f"         {r['verdict_change']}")

    elapsed = time.time() - t_start
    print(f"\nCompleted in {elapsed/60:.1f} minutes  ({n_scraped} scraped, {n_cached} cached)")
    print(f"Results: {OUTPUT_PATH}")
    return invalid, inserted

if __name__ == '__main__':
    inv, ins = main()
    print(f"\n{'='*70}")
    print(f"DONE:  {inv} invalidated  |  {ins} pharmacies added to DB")
    print(f"{'='*70}")
