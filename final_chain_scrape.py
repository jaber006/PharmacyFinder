#!/usr/bin/env python3
"""
Final comprehensive chain pharmacy scraper.
Uses Medmate API where possible, browser for others.
"""

import requests
import sqlite3
import json
import os
import re
import time
from datetime import datetime

DB_PATH = "pharmacy_finder.db"
DATA_DIR = "chain_data"
os.makedirs(DATA_DIR, exist_ok=True)

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Content-Type': 'application/json',
    'Accept': 'application/json',
    'Origin': 'https://www.amcal.com.au',
    'Referer': 'https://www.amcal.com.au/',
}

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

def fetch_medmate(business_id, chain_name):
    """Fetch stores from Medmate API."""
    try:
        r = requests.post(
            'https://app.medmate.com.au/connect/api/get_locations',
            headers=HEADERS,
            json={'businessid': business_id},
            timeout=15
        )
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list):
                stores = [{
                    'n': s.get('locationname', ''),
                    'a': s.get('address', ''),
                    'lat': float(s.get('latitude', 0) or 0),
                    'lng': float(s.get('longitude', 0) or 0),
                    'sub': s.get('suburb', ''),
                    'st': s.get('state', ''),
                    'pc': s.get('postcode', '')
                } for s in data]
                return stores
    except Exception as e:
        print(f"    Medmate API error for {chain_name}: {e}")
    return []

def fetch_html_stores(url, chain_name):
    """Try to extract store data from HTML pages using requests."""
    stores = []
    try:
        r = requests.get(url, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml'
        }, timeout=15, allow_redirects=True)
        
        if r.status_code != 200:
            return stores
        
        text = r.text
        
        # Look for embedded JSON data with coordinates
        # Pattern 1: Script with store data
        for pat in [
            r'var\s+(?:stores?|locations?|pharmacies|markers|storeData|allStores)\s*=\s*(\[[\s\S]*?\]);',
            r'"(?:stores?|locations?|pharmacies|markers)"\s*:\s*(\[[\s\S]*?\])(?=\s*[,}])',
            r'data-locations=\'(\[[\s\S]*?\])\'',
            r'data-locations="(\[[\s\S]*?\])"',
        ]:
            matches = re.findall(pat, text)
            for m in matches:
                try:
                    data = json.loads(m.replace("'", '"'))
                    if isinstance(data, list) and len(data) > 0:
                        first = data[0]
                        if any(k in first for k in ['lat', 'latitude', 'Latitude', 'position', 'geo']):
                            for s in data:
                                lat = float(s.get('lat', s.get('latitude', s.get('Latitude', 0))) or 0)
                                lng = float(s.get('lng', s.get('lon', s.get('longitude', s.get('Longitude', 0)))) or 0)
                                if lat != 0 and lng != 0:
                                    stores.append({
                                        'n': s.get('name', s.get('title', s.get('storeName', ''))),
                                        'a': s.get('address', s.get('full_address', '')),
                                        'lat': lat,
                                        'lng': lng,
                                        'sub': s.get('suburb', s.get('city', '')),
                                        'st': s.get('state', ''),
                                        'pc': s.get('postcode', s.get('postal_code', ''))
                                    })
                            if stores:
                                return stores
                except (json.JSONDecodeError, ValueError):
                    pass
        
        # Pattern 2: LD+JSON schema
        ld_json = re.findall(r'<script[^>]*type="application/ld\+json"[^>]*>([\s\S]*?)</script>', text)
        for ld in ld_json:
            try:
                data = json.loads(ld)
                items = data if isinstance(data, list) else [data]
                for item in items:
                    if isinstance(item, dict) and item.get('@type') in ['Pharmacy', 'LocalBusiness', 'Store']:
                        geo = item.get('geo', {})
                        addr = item.get('address', {})
                        if geo and geo.get('latitude'):
                            stores.append({
                                'n': item.get('name', ''),
                                'a': addr.get('streetAddress', '') if isinstance(addr, dict) else str(addr),
                                'lat': float(geo['latitude']),
                                'lng': float(geo['longitude']),
                                'sub': addr.get('addressLocality', '') if isinstance(addr, dict) else '',
                                'st': addr.get('addressRegion', '') if isinstance(addr, dict) else '',
                                'pc': addr.get('postalCode', '') if isinstance(addr, dict) else ''
                            })
            except:
                pass
        
    except Exception as e:
        print(f"    HTML fetch error for {chain_name}: {e}")
    
    return stores

def scrape_blooms_sitemap():
    """Scrape Blooms The Chemist store pages from sitemap."""
    stores = []
    try:
        # Get sitemap index
        r = requests.get("https://www.bloomsthechemist.com.au/sitemap_index.xml",
                        headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
        if r.status_code == 200:
            sitemap_urls = re.findall(r'<loc>(https://.*?)</loc>', r.text)
            print(f"    Found {len(sitemap_urls)} sitemaps")
            
            # Find store pages
            for sm_url in sitemap_urls:
                if 'store' in sm_url.lower() or 'page' in sm_url.lower() or 'post' not in sm_url.lower():
                    try:
                        r2 = requests.get(sm_url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
                        if r2.status_code == 200:
                            store_urls = re.findall(r'<loc>(https://www\.bloomsthechemist\.com\.au/(?:store|stores|pharmacy)/[^<]+)</loc>', r2.text)
                            if store_urls:
                                print(f"    Found {len(store_urls)} store URLs in {sm_url}")
                                # Fetch each store page for LD+JSON
                                for surl in store_urls[:200]:
                                    try:
                                        sr = requests.get(surl, headers={'User-Agent': 'Mozilla/5.0'}, timeout=8)
                                        if sr.status_code == 200:
                                            page_stores = []
                                            ld_jsons = re.findall(r'<script[^>]*type="application/ld\+json"[^>]*>([\s\S]*?)</script>', sr.text)
                                            for ld in ld_jsons:
                                                try:
                                                    data = json.loads(ld)
                                                    # Could be @graph
                                                    items = data.get('@graph', [data]) if isinstance(data, dict) else data
                                                    for item in (items if isinstance(items, list) else [items]):
                                                        if isinstance(item, dict) and item.get('geo'):
                                                            geo = item['geo']
                                                            addr = item.get('address', {})
                                                            stores.append({
                                                                'n': item.get('name', ''),
                                                                'a': addr.get('streetAddress', '') if isinstance(addr, dict) else '',
                                                                'lat': float(geo.get('latitude', 0)),
                                                                'lng': float(geo.get('longitude', 0)),
                                                                'sub': addr.get('addressLocality', '') if isinstance(addr, dict) else '',
                                                                'st': addr.get('addressRegion', '') if isinstance(addr, dict) else '',
                                                                'pc': addr.get('postalCode', '') if isinstance(addr, dict) else ''
                                                            })
                                                except:
                                                    pass
                                            time.sleep(0.2)
                                    except:
                                        pass
                    except:
                        pass
    except Exception as e:
        print(f"    Blooms sitemap error: {e}")
    
    return stores

def scrape_chain_website_stores(base_url, chain_name):
    """For chains with /stores or /store-locator pages listing individual store URLs."""
    stores = []
    
    # Try sitemap first
    domain = '/'.join(base_url.split('/')[:3])
    for sitemap_url in [f"{domain}/sitemap.xml", f"{domain}/sitemap_index.xml"]:
        try:
            r = requests.get(sitemap_url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
            if r.status_code == 200:
                # Find store pages
                store_patterns = [
                    rf'<loc>({domain}/store/[^<]+)</loc>',
                    rf'<loc>({domain}/stores/[^<]+)</loc>',
                    rf'<loc>({domain}/pharmacy/[^<]+)</loc>',
                    rf'<loc>({domain}/locations?/[^<]+)</loc>',
                ]
                
                all_urls = []
                for pat in store_patterns:
                    found = re.findall(pat, r.text)
                    all_urls.extend(found)
                
                # Check sub-sitemaps
                sub_sitemaps = re.findall(r'<loc>(https://[^<]+sitemap[^<]*\.xml)</loc>', r.text)
                for sm in sub_sitemaps:
                    try:
                        r2 = requests.get(sm, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
                        if r2.status_code == 200:
                            for pat in store_patterns:
                                all_urls.extend(re.findall(pat, r2.text))
                    except:
                        pass
                
                if all_urls:
                    print(f"    Found {len(all_urls)} store URLs in sitemap")
                    for surl in all_urls[:300]:
                        try:
                            sr = requests.get(surl, headers={'User-Agent': 'Mozilla/5.0'}, timeout=8)
                            if sr.status_code == 200:
                                # Extract from LD+JSON
                                ld_jsons = re.findall(r'<script[^>]*type="application/ld\+json"[^>]*>([\s\S]*?)</script>', sr.text)
                                for ld in ld_jsons:
                                    try:
                                        data = json.loads(ld)
                                        items = data.get('@graph', [data]) if isinstance(data, dict) else [data]
                                        for item in items:
                                            if isinstance(item, dict) and item.get('geo'):
                                                geo = item['geo']
                                                addr = item.get('address', {})
                                                stores.append({
                                                    'n': item.get('name', ''),
                                                    'a': addr.get('streetAddress', '') if isinstance(addr, dict) else '',
                                                    'lat': float(geo.get('latitude', 0)),
                                                    'lng': float(geo.get('longitude', 0)),
                                                    'sub': addr.get('addressLocality', '') if isinstance(addr, dict) else '',
                                                    'st': addr.get('addressRegion', '') if isinstance(addr, dict) else '',
                                                    'pc': addr.get('postalCode', '') if isinstance(addr, dict) else ''
                                                })
                                    except:
                                        pass
                            time.sleep(0.15)
                        except:
                            pass
                    break
        except:
            pass
    
    return stores

def main():
    conn = get_db()
    before_count = conn.execute("SELECT COUNT(*) FROM pharmacies").fetchone()[0]
    print(f"Database has {before_count} pharmacies before scraping\n")
    
    results = {}
    
    # ==========================================
    # 1. AMCAL - Medmate API (BID 4)
    # ==========================================
    print("=== 1. Amcal ===")
    amcal_stores = fetch_medmate(4, "Amcal")
    new = sum(1 for s in amcal_stores if insert_pharmacy(conn, s['n'], s['a'], s['lat'], s['lng'], 'amcal.com.au', s['sub'], s['st'], s['pc']))
    conn.commit()
    results['Amcal'] = {'found': len(amcal_stores), 'new': new}
    print(f"  Found: {len(amcal_stores)}, New: {new}")
    
    # ==========================================
    # 2. DISCOUNT DRUG STORES - Medmate API (BID 2)
    # ==========================================
    print("\n=== 2. Discount Drug Stores ===")
    dds_stores = fetch_medmate(2, "DDS")
    new = sum(1 for s in dds_stores if insert_pharmacy(conn, s['n'], s['a'], s['lat'], s['lng'], 'discountdrugstores.com.au', s['sub'], s['st'], s['pc']))
    conn.commit()
    results['Discount Drug Stores'] = {'found': len(dds_stores), 'new': new}
    print(f"  Found: {len(dds_stores)}, New: {new}")
    
    # ==========================================
    # 3. PHARMASAVE - Medmate API (BID 8)
    # ==========================================
    print("\n=== 3. PharmaSave ===")
    ps_stores = fetch_medmate(8, "PharmaSave")
    new = sum(1 for s in ps_stores if insert_pharmacy(conn, s['n'], s['a'], s['lat'], s['lng'], 'pharmasave.com.au', s['sub'], s['st'], s['pc']))
    conn.commit()
    results['PharmaSave'] = {'found': len(ps_stores), 'new': new}
    print(f"  Found: {len(ps_stores)}, New: {new}")
    
    # ==========================================
    # 4. BLOOMS THE CHEMIST - Medmate API (BID 42) + sitemap
    # ==========================================
    print("\n=== 4. Blooms The Chemist ===")
    blooms_stores = fetch_medmate(42, "Blooms")
    print(f"  Medmate: {len(blooms_stores)} stores")
    # Also try sitemap
    blooms_sitemap = scrape_blooms_sitemap()
    print(f"  Sitemap: {len(blooms_sitemap)} stores")
    all_blooms = blooms_stores + blooms_sitemap
    new = sum(1 for s in all_blooms if insert_pharmacy(conn, s['n'], s['a'], s['lat'], s['lng'], 'bloomsthechemist.com.au', s.get('sub'), s.get('st'), s.get('pc')))
    conn.commit()
    results['Blooms The Chemist'] = {'found': len(all_blooms), 'new': new}
    print(f"  Total found: {len(all_blooms)}, New: {new}")
    
    # ==========================================
    # 5. CINCOTTA - Medmate API (BID 43 + 72)
    # ==========================================
    print("\n=== 5. Cincotta ===")
    cin_stores = fetch_medmate(43, "Cincotta")
    cin2 = fetch_medmate(72, "Cincotta2")
    all_cin = cin_stores + cin2
    new = sum(1 for s in all_cin if insert_pharmacy(conn, s['n'], s['a'], s['lat'], s['lng'], 'cincottachemist.com.au', s['sub'], s['st'], s['pc']))
    conn.commit()
    results['Cincotta'] = {'found': len(all_cin), 'new': new}
    print(f"  Found: {len(all_cin)}, New: {new}")
    
    # ==========================================
    # 6. GOOD PRICE PHARMACY - Medmate (BID 51) + website
    # ==========================================
    print("\n=== 6. Good Price Pharmacy ===")
    gpp_stores = fetch_medmate(51, "GoodPrice")
    print(f"  Medmate: {len(gpp_stores)} stores")
    # Also try website
    gpp_web = fetch_html_stores("https://www.goodpricepharmacy.com.au/store-locator/", "GoodPrice")
    gpp_sitemap = scrape_chain_website_stores("https://www.goodpricepharmacy.com.au/", "GoodPrice")
    all_gpp = gpp_stores + gpp_web + gpp_sitemap
    new = sum(1 for s in all_gpp if insert_pharmacy(conn, s['n'], s['a'], s['lat'], s['lng'], 'goodpricepharmacy.com.au', s.get('sub'), s.get('st'), s.get('pc')))
    conn.commit()
    results['Good Price Pharmacy'] = {'found': len(all_gpp), 'new': new}
    print(f"  Total found: {len(all_gpp)}, New: {new}")
    
    # ==========================================
    # 7. NATIONAL PHARMACIES - Medmate (BID 92) + website
    # ==========================================
    print("\n=== 7. National Pharmacies ===")
    np_stores = fetch_medmate(92, "National")
    print(f"  Medmate: {len(np_stores)} stores")
    np_web = scrape_chain_website_stores("https://www.nationalpharmacies.com.au/", "National")
    all_np = np_stores + np_web
    new = sum(1 for s in all_np if insert_pharmacy(conn, s['n'], s['a'], s['lat'], s['lng'], 'nationalpharmacies.com.au', s.get('sub'), s.get('st'), s.get('pc')))
    conn.commit()
    results['National Pharmacies'] = {'found': len(all_np), 'new': new}
    print(f"  Total found: {len(all_np)}, New: {new}")
    
    # ==========================================
    # 8. CAPITAL CHEMIST - Medmate (BID 61) + website
    # ==========================================
    print("\n=== 8. Capital Chemist ===")
    cc_stores = fetch_medmate(61, "CapitalChemist")
    cc_web = scrape_chain_website_stores("https://www.capitalchemist.com.au/", "CapitalChemist")
    all_cc = cc_stores + cc_web
    new = sum(1 for s in all_cc if insert_pharmacy(conn, s['n'], s['a'], s['lat'], s['lng'], 'capitalchemist.com.au', s.get('sub'), s.get('st'), s.get('pc')))
    conn.commit()
    results['Capital Chemist'] = {'found': len(all_cc), 'new': new}
    print(f"  Found: {len(all_cc)}, New: {new}")
    
    # ==========================================
    # 9. FRIENDLIES - Medmate (BID 65)
    # ==========================================
    print("\n=== 9. Friendlies ===")
    fr_stores = fetch_medmate(65, "Friendlies")
    new = sum(1 for s in fr_stores if insert_pharmacy(conn, s['n'], s['a'], s['lat'], s['lng'], 'friendliespharmacy.com.au', s['sub'], s['st'], s['pc']))
    conn.commit()
    results['Friendlies'] = {'found': len(fr_stores), 'new': new}
    print(f"  Found: {len(fr_stores)}, New: {new}")
    
    # ==========================================
    # 10. WIZARD PHARMACY - website scrape
    # ==========================================
    print("\n=== 10. Wizard Pharmacy ===")
    wiz_stores = scrape_chain_website_stores("https://www.wizardpharmacy.com.au/", "Wizard")
    if not wiz_stores:
        wiz_stores = fetch_html_stores("https://www.wizardpharmacy.com.au/store-locator", "Wizard")
    new = sum(1 for s in wiz_stores if insert_pharmacy(conn, s['n'], s['a'], s['lat'], s['lng'], 'wizardpharmacy.com.au', s.get('sub'), s.get('st'), s.get('pc')))
    conn.commit()
    results['Wizard Pharmacy'] = {'found': len(wiz_stores), 'new': new}
    print(f"  Found: {len(wiz_stores)}, New: {new}")
    
    # ==========================================
    # 11. PHARMACY 4 LESS - website scrape
    # ==========================================
    print("\n=== 11. Pharmacy 4 Less ===")
    p4l_stores = scrape_chain_website_stores("https://www.pharmacy4less.com.au/", "Pharmacy4Less")
    if not p4l_stores:
        p4l_stores = fetch_html_stores("https://www.pharmacy4less.com.au/storelocator", "Pharmacy4Less")
    # Also try Medmate BID 26
    p4l_med = fetch_medmate(26, "Pharmacy4Less")
    all_p4l = p4l_stores + p4l_med
    new = sum(1 for s in all_p4l if insert_pharmacy(conn, s['n'], s['a'], s['lat'], s['lng'], 'pharmacy4less.com.au', s.get('sub'), s.get('st'), s.get('pc')))
    conn.commit()
    results['Pharmacy 4 Less'] = {'found': len(all_p4l), 'new': new}
    print(f"  Found: {len(all_p4l)}, New: {new}")
    
    # ==========================================
    # 12. ALIVE PHARMACY - website scrape
    # ==========================================
    print("\n=== 12. Alive Pharmacy ===")
    alive_stores = scrape_chain_website_stores("https://www.alivepharmacy.com.au/", "Alive")
    if not alive_stores:
        alive_stores = fetch_html_stores("https://www.alivepharmacy.com.au/store-locator", "Alive")
    new = sum(1 for s in alive_stores if insert_pharmacy(conn, s['n'], s['a'], s['lat'], s['lng'], 'alivepharmacy.com.au', s.get('sub'), s.get('st'), s.get('pc')))
    conn.commit()
    results['Alive Pharmacy'] = {'found': len(alive_stores), 'new': new}
    print(f"  Found: {len(alive_stores)}, New: {new}")
    
    # ==========================================
    # 13. SOUL PATTINSON - website scrape
    # ==========================================
    print("\n=== 13. Soul Pattinson ===")
    sp_stores = scrape_chain_website_stores("https://soulpattinson.com.au/", "SoulPattinson")
    if not sp_stores:
        sp_stores = fetch_html_stores("https://soulpattinson.com.au/store-locator/", "SoulPattinson")
    new = sum(1 for s in sp_stores if insert_pharmacy(conn, s['n'], s['a'], s['lat'], s['lng'], 'soulpattinson.com.au', s.get('sub'), s.get('st'), s.get('pc')))
    conn.commit()
    results['Soul Pattinson'] = {'found': len(sp_stores), 'new': new}
    print(f"  Found: {len(sp_stores)}, New: {new}")
    
    # ==========================================
    # 14. GUARDIAN - website scrape
    # ==========================================
    print("\n=== 14. Guardian ===")
    guard_stores = scrape_chain_website_stores("https://www.guardianpharmacies.com.au/", "Guardian")
    if not guard_stores:
        guard_stores = fetch_html_stores("https://www.guardianpharmacies.com.au/store-locator", "Guardian")
    new = sum(1 for s in guard_stores if insert_pharmacy(conn, s['n'], s['a'], s['lat'], s['lng'], 'guardianpharmacies.com.au', s.get('sub'), s.get('st'), s.get('pc')))
    conn.commit()
    results['Guardian'] = {'found': len(guard_stores), 'new': new}
    print(f"  Found: {len(guard_stores)}, New: {new}")
    
    # ==========================================
    # BONUS: Also grab some extra chains from Medmate
    # ==========================================
    print("\n=== Bonus: Additional Medmate chains ===")
    bonus_chains = {
        68: ('various-sigma', 'Various Sigma pharmacies'),  # 216 locations
        90: ('stardiscountchemist.com.au', 'Star Discount Chemist'),  # 21
        77: ('pharmacy777.com.au', 'Pharmacy 777'),  # 12
        16: ('directchemistoutlet.com.au', 'Direct Chemist Outlet'),  # 39
        40: ('optimalpharmacyplus.com.au', 'Optimal Pharmacy+'),  # 15
    }
    
    for bid, (source, name) in bonus_chains.items():
        stores = fetch_medmate(bid, name)
        new = sum(1 for s in stores if insert_pharmacy(conn, s['n'], s['a'], s['lat'], s['lng'], source, s['sub'], s['st'], s['pc']))
        conn.commit()
        results[name] = {'found': len(stores), 'new': new}
        print(f"  {name}: {len(stores)} found, {new} new")
    
    # ==========================================
    # SUMMARY
    # ==========================================
    after_count = conn.execute("SELECT COUNT(*) FROM pharmacies").fetchone()[0]
    
    print("\n" + "="*60)
    print("SCRAPING SUMMARY")
    print("="*60)
    print(f"Before: {before_count}")
    print(f"After:  {after_count}")
    print(f"Total new: {after_count - before_count}")
    print()
    for chain, data in sorted(results.items()):
        print(f"  {chain:35s}: {data['found']:4d} found, {data['new']:4d} new")
    
    print("\nSources breakdown:")
    for row in conn.execute("SELECT source, COUNT(*) as cnt FROM pharmacies GROUP BY source ORDER BY cnt DESC"):
        print(f"  {row[0]:35s}: {row[1]:4d}")
    
    conn.close()
    
    with open("chain_scrape_results.json", "w") as f:
        json.dump(results, f, indent=2)

if __name__ == "__main__":
    main()
