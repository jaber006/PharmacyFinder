#!/usr/bin/env python3
"""
Final API-based scraper for chain pharmacies.
Uses discovered API endpoints to get full store data.
"""

import requests
import sqlite3
import json
import re
import time
from datetime import datetime

DB_PATH = "pharmacy_finder.db"
HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def is_duplicate(conn, lat, lng, threshold=0.001):
    cursor = conn.execute(
        "SELECT id, name FROM pharmacies WHERE ABS(latitude - ?) < ? AND ABS(longitude - ?) < ?",
        (lat, threshold, lng, threshold)
    )
    return cursor.fetchone() is not None

def insert_pharmacy(conn, name, address, lat, lng, source, suburb=None, state=None, postcode=None):
    if not name or lat is None or lng is None or lat == 0 or lng == 0:
        return False
    if is_duplicate(conn, lat, lng):
        return False
    try:
        conn.execute(
            """INSERT INTO pharmacies (name, address, latitude, longitude, source, date_scraped, suburb, state, postcode)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (name, address, lat, lng, source, datetime.now().isoformat(), suburb, state, postcode)
        )
        return True
    except sqlite3.IntegrityError:
        return False

def parse_address(address):
    """Parse state and postcode from address."""
    m = re.search(r'\b(NSW|VIC|QLD|SA|WA|TAS|NT|ACT)\b\s*(\d{4})', address)
    if m:
        return m.group(1), m.group(2)
    return None, None

def medmate_api(business_id):
    """Fetch from Medmate API."""
    try:
        r = requests.post(
            'https://app.medmate.com.au/connect/api/get_locations',
            headers={**HEADERS, 'Content-Type': 'application/json'},
            json={'businessid': business_id},
            timeout=15
        )
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list):
                return [{
                    'n': s.get('locationname', ''),
                    'a': s.get('address', ''),
                    'lat': float(s.get('latitude', 0) or 0),
                    'lng': float(s.get('longitude', 0) or 0),
                    'sub': s.get('suburb', ''),
                    'st': s.get('state', ''),
                    'pc': s.get('postcode', '')
                } for s in data]
    except:
        pass
    return []

def ssf_xml(base_url):
    """Super Store Finder XML endpoint."""
    stores = []
    url = f"{base_url}wp-content/plugins/superstorefinder-wp/ssf-wp-xml.php?wpml_lang=&t={int(time.time())}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code == 200 and '<item>' in r.text:
            items = re.findall(r'<item>(.*?)</item>', r.text, re.DOTALL)
            for item in items:
                name = re.search(r'<location>(.*?)</location>', item)
                addr = re.search(r'<address>(.*?)</address>', item)
                lat = re.search(r'<latitude>(.*?)</latitude>', item)
                lng = re.search(r'<longitude>(.*?)</longitude>', item)
                
                if name and lat and lng:
                    import html
                    n = html.unescape(name.group(1).strip())
                    a = html.unescape(addr.group(1).strip()) if addr else ''
                    state, pc = parse_address(a)
                    try:
                        stores.append({
                            'n': n, 'a': a,
                            'lat': float(lat.group(1).strip()),
                            'lng': float(lng.group(1).strip()),
                            'st': state or '', 'pc': pc or ''
                        })
                    except ValueError:
                        pass
    except Exception as e:
        print(f"    SSF error: {e}")
    return stores

def storepoint_api(store_id):
    """Storepoint.co API."""
    try:
        r = requests.get(f'https://api.storepoint.co/v2/{store_id}/locations', headers=HEADERS, timeout=15)
        if r.status_code == 200:
            data = r.json()
            locations = data.get('results', {}).get('locations', [])
            stores = []
            for s in locations:
                addr = s.get('streetaddress', '')
                state, pc = parse_address(addr)
                stores.append({
                    'n': s.get('name', ''),
                    'a': addr,
                    'lat': float(s.get('loc_lat', 0) or 0),
                    'lng': float(s.get('loc_long', 0) or 0),
                    'st': state or '',
                    'pc': pc or ''
                })
            return stores
    except:
        pass
    return []

def main():
    conn = get_db()
    before_count = conn.execute("SELECT COUNT(*) FROM pharmacies").fetchone()[0]
    print(f"Database has {before_count} pharmacies\n")
    
    results = {}
    
    # 1. BLOOMS THE CHEMIST - Storepoint API
    print("=== 1. Blooms The Chemist (Storepoint API) ===")
    blooms = storepoint_api("15f056510a1d3a")
    # Prefix names with "Blooms The Chemist"
    for s in blooms:
        if not s['n'].lower().startswith('blooms'):
            s['n'] = f"Blooms The Chemist {s['n']}"
    new = sum(1 for s in blooms if insert_pharmacy(conn, s['n'], s['a'], s['lat'], s['lng'], 'bloomsthechemist.com.au', '', s['st'], s['pc']))
    conn.commit()
    results['Blooms The Chemist'] = {'found': len(blooms), 'new': new}
    print(f"  Found: {len(blooms)}, New: {new}")
    
    # 2. SOUL PATTINSON - SSF Plugin (already done, but re-run to be sure)
    print("\n=== 2. Soul Pattinson (SSF) ===")
    sp = ssf_xml("https://soulpattinson.com.au/")
    new = sum(1 for s in sp if insert_pharmacy(conn, s['n'], s['a'], s['lat'], s['lng'], 'soulpattinson.com.au', '', s['st'], s['pc']))
    conn.commit()
    results['Soul Pattinson'] = {'found': len(sp), 'new': new}
    print(f"  Found: {len(sp)}, New: {new}")
    
    # 3. AMCAL - Medmate API (BID 4)
    print("\n=== 3. Amcal (Medmate) ===")
    amcal = medmate_api(4)
    new = sum(1 for s in amcal if insert_pharmacy(conn, s['n'], s['a'], s['lat'], s['lng'], 'amcal.com.au', s['sub'], s['st'], s['pc']))
    conn.commit()
    results['Amcal'] = {'found': len(amcal), 'new': new}
    print(f"  Found: {len(amcal)}, New: {new}")
    
    # 4. DDS - Medmate API (BID 2)
    print("\n=== 4. Discount Drug Stores (Medmate) ===")
    dds = medmate_api(2)
    new = sum(1 for s in dds if insert_pharmacy(conn, s['n'], s['a'], s['lat'], s['lng'], 'discountdrugstores.com.au', s['sub'], s['st'], s['pc']))
    conn.commit()
    results['Discount Drug Stores'] = {'found': len(dds), 'new': new}
    print(f"  Found: {len(dds)}, New: {new}")
    
    # 5. PHARMASAVE - Medmate API (BID 8)
    print("\n=== 5. PharmaSave (Medmate) ===")
    ps = medmate_api(8)
    new = sum(1 for s in ps if insert_pharmacy(conn, s['n'], s['a'], s['lat'], s['lng'], 'pharmasave.com.au', s['sub'], s['st'], s['pc']))
    conn.commit()
    results['PharmaSave'] = {'found': len(ps), 'new': new}
    print(f"  Found: {len(ps)}, New: {new}")
    
    # 6. CINCOTTA - Medmate API (BID 43 + 72)
    print("\n=== 6. Cincotta (Medmate) ===")
    cin = medmate_api(43) + medmate_api(72)
    new = sum(1 for s in cin if insert_pharmacy(conn, s['n'], s['a'], s['lat'], s['lng'], 'cincottachemist.com.au', s['sub'], s['st'], s['pc']))
    conn.commit()
    results['Cincotta'] = {'found': len(cin), 'new': new}
    print(f"  Found: {len(cin)}, New: {new}")
    
    # 7. FRIENDLIES - Medmate API (BID 65)
    print("\n=== 7. Friendlies (Medmate) ===")
    fr = medmate_api(65)
    new = sum(1 for s in fr if insert_pharmacy(conn, s['n'], s['a'], s['lat'], s['lng'], 'friendliespharmacy.com.au', s['sub'], s['st'], s['pc']))
    conn.commit()
    results['Friendlies'] = {'found': len(fr), 'new': new}
    print(f"  Found: {len(fr)}, New: {new}")
    
    # 8. GOOD PRICE - Medmate API (BID 51)
    print("\n=== 8. Good Price (Medmate) ===")
    gpp = medmate_api(51)
    new = sum(1 for s in gpp if insert_pharmacy(conn, s['n'], s['a'], s['lat'], s['lng'], 'goodpricepharmacy.com.au', s['sub'], s['st'], s['pc']))
    conn.commit()
    results['Good Price'] = {'found': len(gpp), 'new': new}
    print(f"  Found: {len(gpp)}, New: {new}")
    
    # 9. NATIONAL PHARMACIES - Medmate API (BID 92)
    print("\n=== 9. National Pharmacies (Medmate) ===")
    nat = medmate_api(92)
    new = sum(1 for s in nat if insert_pharmacy(conn, s['n'], s['a'], s['lat'], s['lng'], 'nationalpharmacies.com.au', s['sub'], s['st'], s['pc']))
    conn.commit()
    results['National Pharmacies'] = {'found': len(nat), 'new': new}
    print(f"  Found: {len(nat)}, New: {new}")
    
    # 10. PHARMACY 4 LESS - Medmate API (BID 26)
    print("\n=== 10. Pharmacy 4 Less (Medmate) ===")
    p4l = medmate_api(26)
    new = sum(1 for s in p4l if insert_pharmacy(conn, s['n'], s['a'], s['lat'], s['lng'], 'pharmacy4less.com.au', s['sub'], s['st'], s['pc']))
    conn.commit()
    results['Pharmacy 4 Less'] = {'found': len(p4l), 'new': new}
    print(f"  Found: {len(p4l)}, New: {new}")
    
    # 11. CAPITAL CHEMIST - Medmate API (BID 50 + 61)
    print("\n=== 11. Capital Chemist (Medmate) ===")
    cc = medmate_api(50) + medmate_api(61)
    new = sum(1 for s in cc if insert_pharmacy(conn, s['n'], s['a'], s['lat'], s['lng'], 'capitalchemist.com.au', s['sub'], s['st'], s['pc']))
    conn.commit()
    results['Capital Chemist'] = {'found': len(cc), 'new': new}
    print(f"  Found: {len(cc)}, New: {new}")
    
    # 12. WIZARD - Medmate (BID 80) + try other sources
    print("\n=== 12. Wizard (Medmate) ===")
    wiz = medmate_api(80)
    new = sum(1 for s in wiz if insert_pharmacy(conn, s['n'], s['a'], s['lat'], s['lng'], 'wizardpharmacy.com.au', s['sub'], s['st'], s['pc']))
    conn.commit()
    results['Wizard'] = {'found': len(wiz), 'new': new}
    print(f"  Found: {len(wiz)}, New: {new}")
    
    # 13. ALIVE PHARMACY - try SSF and various approaches
    print("\n=== 13. Alive Pharmacy ===")
    alive = ssf_xml("https://www.alivepharmacy.com.au/")
    if not alive:
        # Try website
        try:
            r = requests.get("https://www.alivepharmacy.com.au/our-locations/", headers=HEADERS, timeout=10)
            if r.status_code == 200:
                alive = []
                # Look for coordinates in page
                lat_lng = re.findall(r'data-lat="(-?\d+\.?\d*)".*?data-lng="(-?\d+\.?\d*)"', r.text, re.DOTALL)
                for lat, lng in lat_lng:
                    alive.append({'n': 'Alive Pharmacy', 'a': '', 'lat': float(lat), 'lng': float(lng)})
        except:
            pass
    new = sum(1 for s in alive if insert_pharmacy(conn, s['n'], s['a'], s['lat'], s['lng'], 'alivepharmacy.com.au', s.get('sub', ''), s.get('st', ''), s.get('pc', '')))
    conn.commit()
    results['Alive'] = {'found': len(alive), 'new': new}
    print(f"  Found: {len(alive)}, New: {new}")
    
    # 14. GUARDIAN - domain doesn't resolve, skip
    print("\n=== 14. Guardian ===")
    print("  Domain guardianpharmacies.com.au doesn't resolve - skipping")
    results['Guardian'] = {'found': 0, 'new': 0}
    
    # BONUS: Extra Medmate chains
    print("\n=== Bonus Medmate chains ===")
    bonus = {
        68: ('various-sigma', 'Sigma pharmacies'),
        90: ('stardiscountchemist.com.au', 'Star Discount'),
        16: ('directchemistoutlet.com.au', 'Direct Chemist Outlet'),
        40: ('optimalpharmacyplus.com.au', 'Optimal Pharmacy+'),
        22: ('terrywhitechemmart.com.au', 'TerryWhite (extra)'),
        52: ('priceline.com.au', 'Priceline (extra)'),
    }
    for bid, (source, name) in bonus.items():
        stores = medmate_api(bid)
        new = sum(1 for s in stores if insert_pharmacy(conn, s['n'], s['a'], s['lat'], s['lng'], source, s['sub'], s['st'], s['pc']))
        conn.commit()
        results[name] = {'found': len(stores), 'new': new}
        if new > 0:
            print(f"  {name}: {len(stores)} found, {new} new")
    
    # Summary
    after_count = conn.execute("SELECT COUNT(*) FROM pharmacies").fetchone()[0]
    
    print(f"\n{'='*60}")
    print(f"FINAL SUMMARY")
    print(f"{'='*60}")
    print(f"Before: {before_count}")
    print(f"After:  {after_count}")
    print(f"Total new: {after_count - before_count}")
    print()
    total_found = 0
    total_new = 0
    for chain, data in sorted(results.items()):
        if data['found'] > 0 or data['new'] > 0:
            print(f"  {chain:35s}: {data['found']:4d} found, {data['new']:4d} new")
            total_found += data['found']
            total_new += data['new']
    print(f"  {'TOTAL':35s}: {total_found:4d} found, {total_new:4d} new")
    
    print(f"\nSources breakdown:")
    for row in conn.execute("SELECT source, COUNT(*) as cnt FROM pharmacies GROUP BY source ORDER BY cnt DESC"):
        print(f"  {row[0]:35s}: {row[1]:4d}")
    
    conn.close()
    
    with open("chain_scrape_results.json", "w") as f:
        json.dump(results, f, indent=2)
    
    print(f"\nDone! Added {after_count - before_count} new pharmacies.")

if __name__ == "__main__":
    main()
