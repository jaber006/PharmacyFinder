"""
Verify medical centre coordinates using Google Maps via Playwright.
Opens a real browser, searches each centre, extracts coords from URL.
No API key needed.
"""

import sqlite3
import json
import re
import os
import sys
import io
import time
import csv
from math import radians, cos, sin, asin, sqrt
from datetime import datetime
from urllib.parse import quote

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace', line_buffering=True)

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'pharmacy_finder.db')
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')
RESULTS_FILE = os.path.join(OUTPUT_DIR, 'gmaps_verification_results.json')

def haversine(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1; dlon = lon2 - lon1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    return 2 * 6371000 * asin(sqrt(a))

def extract_coords(url):
    # @LAT,LNG pattern
    m = re.search(r'@(-?\d+\.\d{4,}),(\d+\.\d{4,})', url)
    if m:
        lat, lng = float(m.group(1)), float(m.group(2))
        if -45 < lat < -10 and 110 < lng < 155:
            return lat, lng
    # !3d !4d pattern
    m = re.search(r'!3d(-?\d+\.\d{4,})!4d(\d+\.\d{4,})', url)
    if m:
        lat, lng = float(m.group(1)), float(m.group(2))
        if -45 < lat < -10 and 110 < lng < 155:
            return lat, lng
    return None, None

def main():
    from playwright.sync_api import sync_playwright
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id, name, address, latitude, longitude, state FROM medical_centres ORDER BY state, name")
    centres = c.fetchall()
    
    print(f"{'='*70}")
    print(f"GOOGLE MAPS VERIFICATION - {len(centres)} medical centres")
    print(f"{'='*70}\n")
    
    results = []
    fixed = 0; ok_count = 0; failed = 0
    
    with sync_playwright() as p:
        # Launch visible browser to avoid detection
        browser = p.chromium.launch(
            headless=False,
            args=['--disable-blink-features=AutomationControlled', '--start-minimized']
        )
        ctx = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
            viewport={'width': 1280, 'height': 720},
            locale='en-AU'
        )
        page = ctx.new_page()
        
        # Initial load + consent
        print("Loading Google Maps...")
        page.goto("https://www.google.com/maps/@-25,134,5z", timeout=30000)
        time.sleep(3)
        
        # Handle consent
        for btn_text in ['Accept all', 'Reject all', 'Accept All']:
            try:
                btn = page.get_by_role("button", name=btn_text)
                if btn.is_visible(timeout=2000):
                    btn.click()
                    time.sleep(2)
                    break
            except:
                pass
        
        print("Ready!\n")
        
        for i, (id, name, address, old_lat, old_lng, state) in enumerate(centres):
            print(f"[{i+1}/{len(centres)}] [{state}] {name}")
            
            query = f"{name}, {address}"
            
            try:
                # Navigate directly to search URL
                search_url = f"https://www.google.com/maps/search/{quote(query)}"
                page.goto(search_url, timeout=30000, wait_until='domcontentloaded')
                
                # Wait for page to settle and URL to update with coords
                time.sleep(5)
                
                # Check URL for coordinates
                url = page.url
                lat, lng = extract_coords(url)
                
                if lat is None:
                    # Maybe we got a results list - click first result
                    try:
                        first = page.locator('a[href*="/maps/place/"]').first
                        if first.is_visible(timeout=2000):
                            first.click()
                            time.sleep(3)
                            url = page.url
                            lat, lng = extract_coords(url)
                    except:
                        pass
                
                if lat is None:
                    # Last try - wait more
                    time.sleep(3)
                    url = page.url
                    lat, lng = extract_coords(url)
                
                if lat is None:
                    print(f"  FAILED")
                    failed += 1
                    results.append({'id': id, 'name': name, 'status': 'FAILED', 'state': state, 'address': address})
                    continue
                
                dist = haversine(old_lat, old_lng, lat, lng)
                
                if dist > 200:
                    print(f"  FIXING: {dist:.0f}m off -> ({lat:.6f}, {lng:.6f})")
                    c.execute("UPDATE medical_centres SET latitude=?, longitude=? WHERE id=?", (lat, lng, id))
                    c.execute("UPDATE opportunities SET latitude=?, longitude=? WHERE poi_name=? AND poi_type='medical_centre'",
                              (lat, lng, name))
                    fixed += 1
                    results.append({'id': id, 'name': name, 'status': 'FIXED', 'state': state,
                                   'old': [old_lat, old_lng], 'new': [lat, lng], 'offset_m': round(dist)})
                else:
                    print(f"  OK ({dist:.0f}m)")
                    ok_count += 1
                    results.append({'id': id, 'name': name, 'status': 'OK', 'state': state, 'offset_m': round(dist)})
                    
            except Exception as e:
                print(f"  ERROR: {str(e)[:80]}")
                failed += 1
                results.append({'id': id, 'name': name, 'status': 'ERROR', 'state': state, 'error': str(e)[:100]})
            
            if (i+1) % 10 == 0:
                conn.commit()
                print(f"  --- {i+1}/{len(centres)} | Fixed:{fixed} OK:{ok_count} Failed:{failed} ---")
        
        browser.close()
    
    conn.commit()
    
    print(f"\n{'='*70}")
    print(f"COMPLETE: Fixed:{fixed} OK:{ok_count} Failed:{failed}")
    print(f"{'='*70}")
    
    if fixed > 0:
        print(f"\nFixed:")
        for r in results:
            if r['status'] == 'FIXED':
                print(f"  {r['name']} ({r['state']}): {r['offset_m']}m")
    
    # Save results
    with open(RESULTS_FILE, 'w', encoding='utf-8') as f:
        json.dump({'timestamp': datetime.now().isoformat(), 'fixed': fixed, 'ok': ok_count, 'failed': failed, 'results': results}, f, indent=2)
    
    # Update CSVs
    print(f"\nUpdating CSVs...")
    c2 = conn.cursor() if not conn else sqlite3.connect(DB_PATH).cursor()
    c2.execute("SELECT name, latitude, longitude FROM medical_centres")
    mc = {r[0]: (r[1], r[2]) for r in c2.fetchall()}
    
    csv_fixed = 0
    for st in ['ACT','NSW','NT','QLD','SA','TAS','VIC','WA']:
        fp = os.path.join(OUTPUT_DIR, f'population_ranked_{st}.csv')
        if not os.path.exists(fp): continue
        with open(fp, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f); fnames = reader.fieldnames; rows = list(reader)
        ch = False
        for row in rows:
            pn = row.get('POI Name','')
            if pn in mc:
                nl, ng = mc[pn]
                ol = float(row.get('Latitude','0') or '0')
                if abs(ol - nl) > 0.0005:
                    row['Latitude'] = str(nl); row['Longitude'] = str(ng); ch = True; csv_fixed += 1
        if ch:
            with open(fp, 'w', encoding='utf-8', newline='') as f:
                w = csv.DictWriter(f, fieldnames=fnames); w.writeheader(); w.writerows(rows)
    
    print(f"CSVs updated: {csv_fixed} entries")
    print("Done!")

if __name__ == '__main__':
    main()
