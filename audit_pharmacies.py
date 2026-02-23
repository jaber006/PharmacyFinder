"""Audit 20 random pharmacies against Google Maps."""
import sqlite3, os, sys, io, re, time, random, json
from math import radians, cos, sin, asin, sqrt
from urllib.parse import quote
from playwright.sync_api import sync_playwright

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace', line_buffering=True)
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'pharmacy_finder.db')

def haversine(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1; dlon = lon2 - lon1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    return 2 * 6371000 * asin(sqrt(a))

def extract_coords(url):
    m = re.search(r'@(-?\d+\.\d{4,}),(\d+\.\d{4,})', url)
    if m:
        lat, lng = float(m.group(1)), float(m.group(2))
        if -45 < lat < -10 and 110 < lng < 155: return lat, lng
    m = re.search(r'!3d(-?\d+\.\d{4,})!4d(\d+\.\d{4,})', url)
    if m:
        lat, lng = float(m.group(1)), float(m.group(2))
        if -45 < lat < -10 and 110 < lng < 155: return lat, lng
    return None, None

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()
c.execute("SELECT name, address, latitude, longitude, state, source FROM pharmacies ORDER BY RANDOM() LIMIT 20")
samples = c.fetchall()
conn.close()

print(f"PHARMACY AUDIT - 20 random pharmacies vs Google Maps")
print(f"{'='*70}\n")

results = []

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False, args=['--disable-blink-features=AutomationControlled', '--start-minimized'])
    ctx = browser.new_context(user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36', viewport={'width': 1280, 'height': 720}, locale='en-AU')
    page = ctx.new_page()
    
    page.goto("https://www.google.com/maps/@-25,134,5z", timeout=30000)
    time.sleep(3)
    for btn_text in ['Accept all', 'Reject all', 'Accept All']:
        try:
            btn = page.get_by_role("button", name=btn_text)
            if btn.is_visible(timeout=2000): btn.click(); time.sleep(2); break
        except: pass
    
    for i, (name, address, db_lat, db_lng, state, source) in enumerate(samples, 1):
        print(f"[{i}/20] {name}")
        print(f"  Addr: {address}")
        print(f"  DB: ({db_lat:.6f}, {db_lng:.6f}) | Source: {source}")
        
        query = f"{name}, {address or ''}"
        try:
            page.goto(f"https://www.google.com/maps/search/{quote(query)}", timeout=30000, wait_until='domcontentloaded')
            time.sleep(5)
            
            url = page.url
            g_lat, g_lng = extract_coords(url)
            
            if g_lat is None:
                try:
                    first = page.locator('a[href*="/maps/place/"]').first
                    if first.is_visible(timeout=2000):
                        first.click(); time.sleep(3)
                        g_lat, g_lng = extract_coords(page.url)
                except: pass
            
            if g_lat is None:
                time.sleep(3)
                g_lat, g_lng = extract_coords(page.url)
            
            if g_lat is None:
                print(f"  FAILED to get coords")
                results.append({'name': name, 'status': 'FAILED', 'source': source, 'state': state})
            else:
                dist = haversine(db_lat, db_lng, g_lat, g_lng)
                ok = dist <= 200
                print(f"  Google: ({g_lat:.6f}, {g_lng:.6f})")
                print(f"  Offset: {dist:.0f}m {'✅' if ok else '❌'}")
                results.append({'name': name, 'status': 'OK' if ok else 'BAD', 'offset_m': round(dist), 
                               'source': source, 'state': state, 'db': [db_lat, db_lng], 'google': [g_lat, g_lng]})
        except Exception as e:
            print(f"  ERROR: {str(e)[:80]}")
            results.append({'name': name, 'status': 'ERROR', 'source': source})
        print()
    
    browser.close()

print(f"\n{'='*70}")
print(f"PHARMACY AUDIT SUMMARY")
print(f"{'='*70}")
ok = sum(1 for r in results if r['status'] == 'OK')
bad = sum(1 for r in results if r['status'] == 'BAD')
fail = sum(1 for r in results if r['status'] in ('FAILED', 'ERROR'))
print(f"✅ Accurate (<200m):   {ok}/20")
print(f"❌ Inaccurate (>200m): {bad}/20")
print(f"⚠️  Failed:            {fail}/20")
if ok + bad > 0:
    print(f"\nAccuracy rate: {ok}/{ok+bad} = {ok/(ok+bad)*100:.0f}%")

# Breakdown by source
print(f"\nBy data source:")
sources = {}
for r in results:
    src = r.get('source', 'unknown')
    if src not in sources: sources[src] = {'ok': 0, 'bad': 0, 'fail': 0}
    if r['status'] == 'OK': sources[src]['ok'] += 1
    elif r['status'] == 'BAD': sources[src]['bad'] += 1
    else: sources[src]['fail'] += 1
for src, counts in sorted(sources.items()):
    total = counts['ok'] + counts['bad']
    pct = f"{counts['ok']/total*100:.0f}%" if total > 0 else "N/A"
    print(f"  {src}: {counts['ok']}✅ {counts['bad']}❌ {counts['fail']}⚠️  ({pct} accurate)")

if bad > 0:
    print(f"\nInaccurate pharmacies:")
    for r in results:
        if r['status'] == 'BAD':
            print(f"  {r['name']} ({r['state']}) — {r['offset_m']}m off [source: {r['source']}]")

print(f"\nAll results:")
for r in results:
    emoji = '✅' if r['status'] == 'OK' else '❌' if r['status'] == 'BAD' else '⚠️'
    dist = f"{r.get('offset_m', '?')}m" if 'offset_m' in r else r['status']
    print(f"  {emoji} {r['name']} ({r.get('state','')}) — {dist} [{r.get('source','')}]")

# Save
with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output', 'pharmacy_audit_results.json'), 'w') as f:
    json.dump(results, f, indent=2)
