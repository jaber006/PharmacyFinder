"""Verify remaining 184 OSM pharmacies against Google Maps and fix coords."""
import sqlite3, os, sys, io, re, time, json
from math import radians, cos, sin, asin, sqrt
from urllib.parse import quote
from playwright.sync_api import sync_playwright

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace', line_buffering=True)
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'pharmacy_finder.db')
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')

def haversine(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    a = sin((lat2-lat1)/2)**2 + cos(lat1)*cos(lat2)*sin((lon2-lon1)/2)**2
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
c.execute("SELECT id, name, address, latitude, longitude, state FROM pharmacies WHERE source = 'OpenStreetMap'")
pharmacies = c.fetchall()

print(f"VERIFYING {len(pharmacies)} OSM PHARMACIES VIA GOOGLE MAPS")
print(f"{'='*70}\n")

results = []
fixed = 0; ok_count = 0; failed = 0; deleted = 0

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False, args=['--disable-blink-features=AutomationControlled', '--start-minimized'])
    ctx = browser.new_context(user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36', viewport={'width': 1280, 'height': 720}, locale='en-AU')
    page = ctx.new_page()
    
    page.goto("https://www.google.com/maps/@-25,134,5z", timeout=30000)
    time.sleep(3)
    for btn_text in ['Accept all', 'Reject all']:
        try:
            btn = page.get_by_role("button", name=btn_text)
            if btn.is_visible(timeout=2000): btn.click(); time.sleep(2); break
        except: pass
    
    for i, (id, name, address, db_lat, db_lng, state) in enumerate(pharmacies, 1):
        print(f"[{i}/{len(pharmacies)}] {name}")
        
        query = f"{name}, {address}"
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
                # Can't verify - delete if coords might be wrong
                print(f"  FAILED - deleting unverifiable entry")
                c.execute("DELETE FROM pharmacies WHERE id = ?", (id,))
                deleted += 1
                failed += 1
                continue
            
            dist = haversine(db_lat, db_lng, g_lat, g_lng)
            
            if dist > 10000:  # >10km = probably wrong pharmacy matched
                print(f"  Google matched wrong place ({dist/1000:.1f}km away) - deleting")
                c.execute("DELETE FROM pharmacies WHERE id = ?", (id,))
                deleted += 1
                continue
            elif dist > 200:
                print(f"  FIXING: {dist:.0f}m -> ({g_lat:.6f}, {g_lng:.6f})")
                c.execute("UPDATE pharmacies SET latitude=?, longitude=?, source='OpenStreetMap-verify' WHERE id=?",
                          (g_lat, g_lng, id))
                fixed += 1
            else:
                print(f"  OK ({dist:.0f}m)")
                c.execute("UPDATE pharmacies SET source='OpenStreetMap-verify' WHERE id=?", (id,))
                ok_count += 1
                
        except Exception as e:
            print(f"  ERROR: {str(e)[:60]}")
            failed += 1
        
        if (i) % 20 == 0:
            conn.commit()
            print(f"\n  --- {i}/{len(pharmacies)} | Fixed:{fixed} OK:{ok_count} Deleted:{deleted} Failed:{failed} ---\n")
    
    browser.close()

conn.commit()

print(f"\n{'='*70}")
print(f"COMPLETE")
print(f"{'='*70}")
print(f"Fixed:   {fixed}")
print(f"OK:      {ok_count}")
print(f"Deleted: {deleted}")
print(f"Failed:  {failed}")

c.execute("SELECT COUNT(*) FROM pharmacies")
print(f"\nTotal pharmacies now: {c.fetchone()[0]}")

c.execute("SELECT source, COUNT(*) FROM pharmacies GROUP BY source ORDER BY COUNT(*) DESC")
for src, count in c.fetchall():
    print(f"  {src}: {count}")

conn.close()
print("\nDone!")
