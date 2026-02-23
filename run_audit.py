"""Verify 10 random audit locations against Google Maps via Playwright."""
import sqlite3
import os
import sys
import io
import re
import time
import random
import json
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
        if -45 < lat < -10 and 110 < lng < 155:
            return lat, lng
    m = re.search(r'!3d(-?\d+\.\d{4,})!4d(\d+\.\d{4,})', url)
    if m:
        lat, lng = float(m.group(1)), float(m.group(2))
        if -45 < lat < -10 and 110 < lng < 155:
            return lat, lng
    return None, None

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

samples = []

# 4 pharmacies
c.execute("SELECT 'Pharmacy', name, address, latitude, longitude FROM pharmacies ORDER BY RANDOM() LIMIT 4")
samples.extend(c.fetchall())

# 2 medical centres 
c.execute("SELECT 'Medical Centre', name, address, latitude, longitude FROM medical_centres ORDER BY RANDOM() LIMIT 2")
samples.extend(c.fetchall())

# 2 supermarkets
c.execute("SELECT 'Supermarket', name, address, latitude, longitude FROM supermarkets ORDER BY RANDOM() LIMIT 2")
samples.extend(c.fetchall())

# 1 hospital
c.execute("SELECT 'Hospital', name, address, latitude, longitude FROM hospitals ORDER BY RANDOM() LIMIT 1")
samples.extend(c.fetchall())

# 1 shopping centre
c.execute("SELECT 'Shopping Centre', name, address, latitude, longitude FROM shopping_centres ORDER BY RANDOM() LIMIT 1")
samples.extend(c.fetchall())

random.shuffle(samples)
conn.close()

print(f"RANDOM AUDIT - Verifying {len(samples)} locations against Google Maps\n")
print(f"{'='*70}\n")

results = []

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False, args=['--disable-blink-features=AutomationControlled', '--start-minimized'])
    ctx = browser.new_context(
        user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        viewport={'width': 1280, 'height': 720}, locale='en-AU'
    )
    page = ctx.new_page()
    
    page.goto("https://www.google.com/maps/@-25,134,5z", timeout=30000)
    time.sleep(3)
    for btn_text in ['Accept all', 'Reject all', 'Accept All']:
        try:
            btn = page.get_by_role("button", name=btn_text)
            if btn.is_visible(timeout=2000): btn.click(); time.sleep(2); break
        except: pass
    
    for i, (type, name, address, db_lat, db_lng) in enumerate(samples, 1):
        print(f"[{i}/10] {type}: {name}")
        print(f"  DB coords: ({db_lat:.6f}, {db_lng:.6f})")
        
        query = f"{name}, {address or ''}"
        search_url = f"https://www.google.com/maps/search/{quote(query)}"
        
        try:
            page.goto(search_url, timeout=30000, wait_until='domcontentloaded')
            time.sleep(5)
            
            url = page.url
            g_lat, g_lng = extract_coords(url)
            
            if g_lat is None:
                try:
                    first = page.locator('a[href*="/maps/place/"]').first
                    if first.is_visible(timeout=2000):
                        first.click(); time.sleep(3)
                        url = page.url
                        g_lat, g_lng = extract_coords(url)
                except: pass
            
            if g_lat is None:
                time.sleep(3)
                url = page.url
                g_lat, g_lng = extract_coords(url)
            
            if g_lat is None:
                print(f"  Google: FAILED to get coords")
                results.append({'type': type, 'name': name, 'status': 'FAILED'})
            else:
                dist = haversine(db_lat, db_lng, g_lat, g_lng)
                status = 'OK' if dist <= 200 else 'BAD'
                emoji = '✅' if dist <= 200 else '❌'
                print(f"  Google: ({g_lat:.6f}, {g_lng:.6f})")
                print(f"  Offset: {dist:.0f}m {emoji}")
                results.append({'type': type, 'name': name, 'status': status, 'offset_m': round(dist),
                               'db': [db_lat, db_lng], 'google': [g_lat, g_lng]})
        except Exception as e:
            print(f"  ERROR: {str(e)[:80]}")
            results.append({'type': type, 'name': name, 'status': 'ERROR'})
        
        print()
    
    browser.close()

print(f"\n{'='*70}")
print(f"AUDIT SUMMARY")
print(f"{'='*70}")
ok = sum(1 for r in results if r['status'] == 'OK')
bad = sum(1 for r in results if r['status'] == 'BAD')
fail = sum(1 for r in results if r['status'] in ('FAILED', 'ERROR'))
print(f"✅ Accurate (<200m):  {ok}/10")
print(f"❌ Inaccurate (>200m): {bad}/10")
print(f"⚠️  Failed to verify:  {fail}/10")
print(f"\nAccuracy rate: {ok}/{ok+bad} = {ok/(ok+bad)*100:.0f}%" if ok+bad > 0 else "")

if bad > 0:
    print(f"\nInaccurate entries:")
    for r in results:
        if r['status'] == 'BAD':
            print(f"  {r['type']}: {r['name']} — {r['offset_m']}m off")

print(f"\nFull results:")
for r in results:
    emoji = '✅' if r['status'] == 'OK' else '❌' if r['status'] == 'BAD' else '⚠️'
    dist = f"{r.get('offset_m', '?')}m" if 'offset_m' in r else r['status']
    print(f"  {emoji} [{r['type']}] {r['name']} — {dist}")
