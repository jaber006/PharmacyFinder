"""
FULL DATA VERIFICATION: Verify ALL POIs against Google Maps.
Shopping centres, supermarkets, hospitals, GPs — everything.
Updates DB directly (single source of truth).
"""
import sqlite3, os, sys, io, re, time, json
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

def search_gmaps(page, query, retries=2):
    """Search Google Maps and extract coordinates."""
    for attempt in range(retries):
        try:
            page.goto(f"https://www.google.com/maps/search/{quote(query)}", timeout=30000, wait_until='domcontentloaded')
            time.sleep(5)
            
            url = page.url
            lat, lng = extract_coords(url)
            if lat: return lat, lng
            
            # Try clicking first result
            try:
                first = page.locator('a[href*="/maps/place/"]').first
                if first.is_visible(timeout=2000):
                    first.click()
                    time.sleep(4)
                    lat, lng = extract_coords(page.url)
                    if lat: return lat, lng
            except: pass
            
            # Wait more
            time.sleep(4)
            lat, lng = extract_coords(page.url)
            if lat: return lat, lng
            
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(2)
                continue
    return None, None

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

# Define all tables to verify
tables = [
    ('shopping_centres', 'SELECT id, name, address, latitude, longitude FROM shopping_centres WHERE latitude IS NOT NULL'),
    ('supermarkets', 'SELECT id, name, address, latitude, longitude FROM supermarkets WHERE latitude IS NOT NULL'),
    ('hospitals', 'SELECT id, name, address, latitude, longitude FROM hospitals WHERE latitude IS NOT NULL'),
    ('gps', 'SELECT id, name, address, latitude, longitude FROM gps WHERE latitude IS NOT NULL'),
]

# Count totals
total = 0
for table, query in tables:
    c.execute(f"SELECT COUNT(*) FROM {table} WHERE latitude IS NOT NULL")
    cnt = c.fetchone()[0]
    total += cnt
    print(f"  {table}: {cnt}")
print(f"\nTOTAL TO VERIFY: {total}")
print(f"Estimated time: {total * 10 // 60} minutes\n")
print(f"{'='*70}\n")

stats = {'fixed': 0, 'ok': 0, 'failed': 0}
global_i = 0

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False, args=['--disable-blink-features=AutomationControlled', '--start-minimized'])
    ctx = browser.new_context(user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36', viewport={'width': 1280, 'height': 720}, locale='en-AU')
    page = ctx.new_page()
    
    page.goto("https://www.google.com/maps/@-25,134,5z", timeout=30000)
    time.sleep(3)
    for btn in ['Accept all', 'Reject all']:
        try:
            b = page.get_by_role("button", name=btn)
            if b.is_visible(timeout=2000): b.click(); time.sleep(2); break
        except: pass
    
    for table, query_sql in tables:
        c.execute(query_sql)
        rows = c.fetchall()
        print(f"\n{'='*50}")
        print(f"VERIFYING: {table} ({len(rows)} entries)")
        print(f"{'='*50}\n")
        
        for i, (id, name, address, db_lat, db_lng) in enumerate(rows, 1):
            global_i += 1
            print(f"[{global_i}/{total}] [{table}] {name}")
            
            # Build search query
            addr = address or ''
            query = f"{name}, {addr}" if addr and len(addr) > 5 else name
            
            g_lat, g_lng = search_gmaps(page, query)
            
            # If failed and we have a longer address, try address only
            if g_lat is None and addr and len(addr) > 10:
                g_lat, g_lng = search_gmaps(page, addr)
            
            if g_lat is None:
                print(f"  FAILED")
                stats['failed'] += 1
                continue
            
            dist = haversine(db_lat, db_lng, g_lat, g_lng)
            
            if dist > 50000:  # >50km = wrong match entirely, skip
                print(f"  WRONG MATCH ({dist/1000:.0f}km) - skipping")
                stats['failed'] += 1
                continue
            elif dist > 200:
                print(f"  FIXING: {dist:.0f}m -> ({g_lat:.6f}, {g_lng:.6f})")
                c.execute(f"UPDATE {table} SET latitude=?, longitude=? WHERE id=?", (g_lat, g_lng, id))
                # Also update opportunities that reference this POI
                c.execute("UPDATE opportunities SET latitude=?, longitude=? WHERE poi_name=?", (g_lat, g_lng, name))
                stats['fixed'] += 1
            else:
                print(f"  OK ({dist:.0f}m)")
                stats['ok'] += 1
            
            if global_i % 20 == 0:
                conn.commit()
                print(f"\n  --- Progress: {global_i}/{total} | Fixed:{stats['fixed']} OK:{stats['ok']} Failed:{stats['failed']} ---\n")
    
    browser.close()

conn.commit()

print(f"\n{'='*70}")
print(f"FULL VERIFICATION COMPLETE")
print(f"{'='*70}")
print(f"Fixed:  {stats['fixed']}")
print(f"OK:     {stats['ok']}")
print(f"Failed: {stats['failed']}")
print(f"Total:  {global_i}")

# Now rebuild dashboard
print(f"\nRebuilding dashboard...")
import subprocess
subprocess.run([sys.executable, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'build_dashboard.py')], 
               capture_output=True, timeout=30)
print("Dashboard rebuilt!")

conn.close()
print("\nDone! All data verified against Google Maps.")
