"""Resume: verify remaining GPs (from offset 55 onwards, ~34 left)."""
import sqlite3, os, sys, io, re, time
from math import radians, cos, sin, asin, sqrt
from urllib.parse import quote
from playwright.sync_api import sync_playwright

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace', line_buffering=True)
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'pharmacy_finder.db')

def haversine(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    a = sin((lat2-lat1)/2)**2 + cos(lat1)*cos(lat2)*sin((lon2-lon1)/2)**2
    return 2 * 6371000 * asin(sqrt(a))

def extract_coords(url):
    m = re.search(r'!3d(-?\d+\.\d{4,})!4d(\d+\.\d{4,})', url)
    if m:
        lat, lng = float(m.group(1)), float(m.group(2))
        if -45 < lat < -10 and 110 < lng < 155: return lat, lng
    m = re.search(r'@(-?\d+\.\d{4,}),(\d+\.\d{4,})', url)
    if m:
        lat, lng = float(m.group(1)), float(m.group(2))
        if -45 < lat < -10 and 110 < lng < 155: return lat, lng
    return None, None

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()
c.execute("SELECT id, name, address, latitude, longitude FROM gps WHERE latitude IS NOT NULL")
all_gps = c.fetchall()

# Skip the first 55 that were already verified (up to entry 360 in main run = 360 - 154 shops - 118 supers - 38 hospitals - 55 gps done)  
# Actually, the crash was at GP index ~55 (365 - 154 - 118 - 38 = 55). Let's just re-verify from around index 53 to be safe.
gps_to_verify = all_gps[53:]
print(f"Resuming: {len(gps_to_verify)} GPs to verify\n")

fixed = 0; ok = 0; failed = 0

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False, args=['--disable-blink-features=AutomationControlled', '--start-minimized'])
    ctx = browser.new_context(user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36', viewport={'width': 1280, 'height': 720}, locale='en-AU')
    page = ctx.new_page()
    page.goto("https://www.google.com/maps/@-42,147,8z", timeout=30000)
    time.sleep(3)
    for btn in ['Accept all', 'Reject all']:
        try:
            b = page.get_by_role("button", name=btn)
            if b.is_visible(timeout=2000): b.click(); time.sleep(2); break
        except: pass

    for i, (id, name, address, db_lat, db_lng) in enumerate(gps_to_verify, 1):
        print(f"[{i}/{len(gps_to_verify)}] {name}")
        addr = address or ''
        query = f"{name}, {addr}" if addr and len(addr) > 5 else name
        
        try:
            page.goto(f"https://www.google.com/maps/search/{quote(query)}", timeout=30000, wait_until='domcontentloaded')
            time.sleep(5)
            g_lat, g_lng = extract_coords(page.url)
            if g_lat is None:
                try:
                    first = page.locator('a[href*="/maps/place/"]').first
                    if first.is_visible(timeout=2000):
                        first.click(); time.sleep(4)
                        g_lat, g_lng = extract_coords(page.url)
                except: pass
            if g_lat is None:
                time.sleep(4)
                g_lat, g_lng = extract_coords(page.url)
            
            if g_lat is None:
                print(f"  FAILED"); failed += 1; continue
            
            dist = haversine(db_lat, db_lng, g_lat, g_lng)
            if dist > 50000:
                print(f"  WRONG MATCH ({dist/1000:.0f}km)"); failed += 1; continue
            elif dist > 200:
                print(f"  FIXING: {dist:.0f}m -> ({g_lat:.6f}, {g_lng:.6f})")
                c.execute("UPDATE gps SET latitude=?, longitude=? WHERE id=?", (g_lat, g_lng, id))
                c.execute("UPDATE opportunities SET latitude=?, longitude=? WHERE poi_name=?", (g_lat, g_lng, name))
                fixed += 1
            else:
                print(f"  OK ({dist:.0f}m)"); ok += 1
        except Exception as e:
            print(f"  ERROR: {str(e)[:60]}"); failed += 1
    
    browser.close()

conn.commit()
print(f"\nDone! Fixed:{fixed} OK:{ok} Failed:{failed}")

# Rebuild dashboard
import subprocess
subprocess.run([sys.executable, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'build_dashboard.py')], capture_output=True, timeout=30)
print("Dashboard rebuilt!")
conn.close()
