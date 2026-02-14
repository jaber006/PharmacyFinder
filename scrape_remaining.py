#!/usr/bin/env python3
"""
Scrape remaining chains: Soul Pattinson (SSF plugin), Guardian, Wizard, Alive, etc.
"""

import requests
import sqlite3
import json
import re
import xml.etree.ElementTree as ET
import time
from datetime import datetime
import html

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

def parse_state_postcode(address):
    """Extract state and postcode from address string."""
    m = re.search(r'\b(NSW|VIC|QLD|SA|WA|TAS|NT|ACT)\b\s*(\d{4})', address)
    if m:
        return m.group(1), m.group(2)
    return None, None

def scrape_ssf_plugin(base_url, chain_name, source):
    """Scrape Super Store Finder WordPress plugin."""
    stores = []
    url = f"{base_url}wp-content/plugins/superstorefinder-wp/ssf-wp-xml.php?wpml_lang=&t={int(time.time())}"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            print(f"  {chain_name}: SSF endpoint returned {r.status_code}")
            return stores
        
        # Parse XML
        text = r.text
        # Fix common XML issues
        text = re.sub(r'&(?!amp;|lt;|gt;|quot;|apos;|#)', '&amp;', text)
        
        try:
            root = ET.fromstring(text)
        except ET.ParseError:
            # Try cleaning up more
            text = re.sub(r'<!\[CDATA\[.*?\]\]>', '', text, flags=re.DOTALL)
            try:
                root = ET.fromstring(text)
            except:
                print(f"  {chain_name}: XML parse failed")
                return stores
        
        for item in root.findall('.//item'):
            name = item.findtext('location', '').strip()
            addr_raw = item.findtext('address', '').strip()
            lat_str = item.findtext('latitude', '0').strip()
            lng_str = item.findtext('longitude', '0').strip()
            
            # Decode HTML entities
            name = html.unescape(name)
            addr_raw = html.unescape(addr_raw)
            
            try:
                lat = float(lat_str)
                lng = float(lng_str)
            except ValueError:
                continue
            
            state, postcode = parse_state_postcode(addr_raw)
            
            stores.append({
                'n': name,
                'a': addr_raw,
                'lat': lat,
                'lng': lng,
                'st': state or '',
                'pc': postcode or ''
            })
        
        print(f"  {chain_name}: Found {len(stores)} stores via SSF")
    except Exception as e:
        print(f"  {chain_name}: SSF error - {e}")
    
    return stores

def scrape_store_pages_from_sitemap(domain, chain_name, source):
    """Scrape individual store pages for LD+JSON data."""
    stores = []
    
    for sitemap_url in [f"{domain}/sitemap.xml", f"{domain}/sitemap_index.xml", 
                         f"{domain}/store-sitemap.xml", f"{domain}/stores-sitemap.xml",
                         f"{domain}/page-sitemap.xml"]:
        try:
            r = requests.get(sitemap_url, headers=HEADERS, timeout=10)
            if r.status_code != 200:
                continue
            
            # Find store URLs
            all_urls = re.findall(r'<loc>(' + re.escape(domain) + r'/(?:store|stores|pharmacy|location|our-stores)/[^<]+)</loc>', r.text)
            
            # Check sub-sitemaps
            sub_sitemaps = re.findall(r'<loc>(' + re.escape(domain) + r'/[^<]*sitemap[^<]*\.xml)</loc>', r.text)
            for sm in sub_sitemaps:
                try:
                    r2 = requests.get(sm, headers=HEADERS, timeout=10)
                    if r2.status_code == 200:
                        all_urls.extend(re.findall(r'<loc>(' + re.escape(domain) + r'/(?:store|stores|pharmacy|location|our-stores)/[^<]+)</loc>', r2.text))
                except:
                    pass
            
            if all_urls:
                print(f"    Found {len(all_urls)} store page URLs")
                for surl in all_urls:
                    try:
                        sr = requests.get(surl, headers=HEADERS, timeout=8)
                        if sr.status_code == 200:
                            # Extract from LD+JSON
                            for ld in re.findall(r'<script[^>]*type="application/ld\+json"[^>]*>([\s\S]*?)</script>', sr.text):
                                try:
                                    data = json.loads(ld)
                                    items = []
                                    if isinstance(data, dict):
                                        if data.get('@graph'):
                                            items = data['@graph']
                                        else:
                                            items = [data]
                                    elif isinstance(data, list):
                                        items = data
                                    
                                    for item in items:
                                        if isinstance(item, dict) and item.get('geo'):
                                            geo = item['geo']
                                            addr = item.get('address', {})
                                            if isinstance(addr, str):
                                                addr = {'streetAddress': addr}
                                            lat = float(geo.get('latitude', 0))
                                            lng = float(geo.get('longitude', 0))
                                            if lat != 0 and lng != 0:
                                                stores.append({
                                                    'n': item.get('name', ''),
                                                    'a': addr.get('streetAddress', ''),
                                                    'lat': lat,
                                                    'lng': lng,
                                                    'sub': addr.get('addressLocality', ''),
                                                    'st': addr.get('addressRegion', ''),
                                                    'pc': addr.get('postalCode', '')
                                                })
                                except:
                                    pass
                        time.sleep(0.15)
                    except:
                        pass
                break  # Found URLs, stop checking sitemaps
        except:
            pass
    
    print(f"    {chain_name}: Extracted {len(stores)} stores from pages")
    return stores

def scrape_wizard():
    """Wizard Pharmacy - check for data source."""
    stores = []
    
    # Try SSF plugin
    stores = scrape_ssf_plugin("https://www.wizardpharmacy.com.au/", "Wizard", "wizardpharmacy.com.au")
    if stores:
        return stores
    
    # Try sitemap
    stores = scrape_store_pages_from_sitemap("https://www.wizardpharmacy.com.au", "Wizard", "wizardpharmacy.com.au")
    if stores:
        return stores
    
    # Try direct page source
    try:
        r = requests.get("https://www.wizardpharmacy.com.au/store-locator", headers=HEADERS, timeout=15)
        if r.status_code == 200:
            # Look for JSON data
            for pat in [r'"stores"\s*:\s*(\[.*?\])', r'"locations"\s*:\s*(\[.*?\])']:
                m = re.search(pat, r.text, re.DOTALL)
                if m:
                    try:
                        data = json.loads(m.group(1))
                        for s in data:
                            lat = float(s.get('lat', s.get('latitude', 0)) or 0)
                            lng = float(s.get('lng', s.get('longitude', 0)) or 0)
                            if lat and lng:
                                stores.append({
                                    'n': s.get('name', ''),
                                    'a': s.get('address', ''),
                                    'lat': lat, 'lng': lng
                                })
                    except:
                        pass
    except:
        pass
    
    return stores

def scrape_alive():
    """Alive Pharmacy."""
    stores = []
    
    # Try SSF
    stores = scrape_ssf_plugin("https://www.alivepharmacy.com.au/", "Alive", "alivepharmacy.com.au")
    if stores:
        return stores
    
    # Try sitemap
    stores = scrape_store_pages_from_sitemap("https://www.alivepharmacy.com.au", "Alive", "alivepharmacy.com.au")
    if stores:
        return stores
    
    # Try page
    try:
        r = requests.get("https://www.alivepharmacy.com.au/store-locator", headers=HEADERS, timeout=15, allow_redirects=True)
        if r.status_code == 200:
            # Look for store data
            lat_matches = re.findall(r'data-lat="(-?\d+\.?\d*)"', r.text)
            lng_matches = re.findall(r'data-lng="(-?\d+\.?\d*)"', r.text)
            name_matches = re.findall(r'data-name="([^"]*)"', r.text)
            
            if lat_matches:
                for i in range(min(len(lat_matches), len(lng_matches))):
                    stores.append({
                        'n': name_matches[i] if i < len(name_matches) else '',
                        'a': '',
                        'lat': float(lat_matches[i]),
                        'lng': float(lng_matches[i])
                    })
    except:
        pass
    
    return stores

def scrape_guardian():
    """Guardian Pharmacies - try various approaches."""
    stores = []
    
    # guardianpharmacies.com.au doesn't resolve. Try guardian.com.au
    for domain in ['https://www.guardian.com.au', 'https://guardian.com.au']:
        try:
            r = requests.get(f"{domain}/store-locator", headers=HEADERS, timeout=10)
            if r.status_code == 200:
                stores = scrape_store_pages_from_sitemap(domain, "Guardian", "guardian.com.au")
                if stores:
                    return stores
        except:
            pass
    
    # Try SSF on various domains
    for domain in ['https://www.guardianpharmacies.com.au/', 'https://guardian.com.au/']:
        try:
            stores = scrape_ssf_plugin(domain, "Guardian", "guardian.com.au")
            if stores:
                return stores
        except:
            pass
    
    return stores

def scrape_blooms_complete():
    """Blooms The Chemist - comprehensive scrape from sitemap."""
    stores = []
    
    stores = scrape_store_pages_from_sitemap("https://www.bloomsthechemist.com.au", "Blooms", "bloomsthechemist.com.au")
    
    return stores

def scrape_pharmacy4less():
    """Pharmacy 4 Less - Magento-based."""
    stores = []
    
    # Try sitemap
    stores = scrape_store_pages_from_sitemap("https://www.pharmacy4less.com.au", "Pharmacy4Less", "pharmacy4less.com.au")
    if stores:
        return stores
    
    # Try Amasty locator endpoint
    try:
        r = requests.post(
            "https://www.pharmacy4less.com.au/amlocator/index/ajax/",
            headers={**HEADERS, 'X-Requested-With': 'XMLHttpRequest'},
            timeout=15
        )
        if r.status_code == 200:
            data = r.json()
            items = data.get('items', data) if isinstance(data, dict) else data
            if isinstance(items, list):
                for s in items:
                    lat = float(s.get('lat', 0) or 0)
                    lng = float(s.get('lng', 0) or 0)
                    if lat and lng:
                        stores.append({
                            'n': s.get('name', ''),
                            'a': s.get('address', ''),
                            'lat': lat, 'lng': lng
                        })
    except:
        pass
    
    return stores

def scrape_goodprice():
    """Good Price Pharmacy - WordPress SSF or sitemap."""
    stores = scrape_ssf_plugin("https://www.goodpricepharmacy.com.au/", "GoodPrice", "goodpricepharmacy.com.au")
    if stores:
        return stores
    
    stores = scrape_store_pages_from_sitemap("https://www.goodpricepharmacy.com.au", "GoodPrice", "goodpricepharmacy.com.au")
    return stores

def scrape_national():
    """National Pharmacies."""
    stores = scrape_store_pages_from_sitemap("https://www.nationalpharmacies.com.au", "National", "nationalpharmacies.com.au")
    return stores

def scrape_capital():
    """Capital Chemist."""
    stores = scrape_ssf_plugin("https://www.capitalchemist.com.au/", "Capital", "capitalchemist.com.au")
    if stores:
        return stores
    stores = scrape_store_pages_from_sitemap("https://www.capitalchemist.com.au", "Capital", "capitalchemist.com.au")
    return stores

def main():
    conn = get_db()
    before_count = conn.execute("SELECT COUNT(*) FROM pharmacies").fetchone()[0]
    print(f"Database has {before_count} pharmacies before\n")
    
    results = {}
    
    # Soul Pattinson
    print("=== Soul Pattinson ===")
    sp = scrape_ssf_plugin("https://soulpattinson.com.au/", "SoulPattinson", "soulpattinson.com.au")
    new = sum(1 for s in sp if insert_pharmacy(conn, s['n'], s['a'], s['lat'], s['lng'], 'soulpattinson.com.au', s.get('sub'), s.get('st'), s.get('pc')))
    conn.commit()
    results['Soul Pattinson'] = {'found': len(sp), 'new': new}
    print(f"  New: {new}")
    
    # Guardian
    print("\n=== Guardian ===")
    guard = scrape_guardian()
    new = sum(1 for s in guard if insert_pharmacy(conn, s['n'], s['a'], s['lat'], s['lng'], 'guardianpharmacies.com.au', s.get('sub'), s.get('st'), s.get('pc')))
    conn.commit()
    results['Guardian'] = {'found': len(guard), 'new': new}
    print(f"  New: {new}")
    
    # Wizard
    print("\n=== Wizard ===")
    wiz = scrape_wizard()
    new = sum(1 for s in wiz if insert_pharmacy(conn, s['n'], s['a'], s['lat'], s['lng'], 'wizardpharmacy.com.au', s.get('sub'), s.get('st'), s.get('pc')))
    conn.commit()
    results['Wizard'] = {'found': len(wiz), 'new': new}
    print(f"  New: {new}")
    
    # Alive
    print("\n=== Alive ===")
    alive = scrape_alive()
    new = sum(1 for s in alive if insert_pharmacy(conn, s['n'], s['a'], s['lat'], s['lng'], 'alivepharmacy.com.au', s.get('sub'), s.get('st'), s.get('pc')))
    conn.commit()
    results['Alive'] = {'found': len(alive), 'new': new}
    print(f"  New: {new}")
    
    # Blooms complete
    print("\n=== Blooms The Chemist ===")
    blooms = scrape_blooms_complete()
    new = sum(1 for s in blooms if insert_pharmacy(conn, s['n'], s['a'], s['lat'], s['lng'], 'bloomsthechemist.com.au', s.get('sub'), s.get('st'), s.get('pc')))
    conn.commit()
    results['Blooms'] = {'found': len(blooms), 'new': new}
    print(f"  New: {new}")
    
    # Good Price
    print("\n=== Good Price ===")
    gpp = scrape_goodprice()
    new = sum(1 for s in gpp if insert_pharmacy(conn, s['n'], s['a'], s['lat'], s['lng'], 'goodpricepharmacy.com.au', s.get('sub'), s.get('st'), s.get('pc')))
    conn.commit()
    results['Good Price'] = {'found': len(gpp), 'new': new}
    print(f"  New: {new}")
    
    # National
    print("\n=== National Pharmacies ===")
    nat = scrape_national()
    new = sum(1 for s in nat if insert_pharmacy(conn, s['n'], s['a'], s['lat'], s['lng'], 'nationalpharmacies.com.au', s.get('sub'), s.get('st'), s.get('pc')))
    conn.commit()
    results['National'] = {'found': len(nat), 'new': new}
    print(f"  New: {new}")
    
    # Capital Chemist
    print("\n=== Capital Chemist ===")
    cap = scrape_capital()
    new = sum(1 for s in cap if insert_pharmacy(conn, s['n'], s['a'], s['lat'], s['lng'], 'capitalchemist.com.au', s.get('sub'), s.get('st'), s.get('pc')))
    conn.commit()
    results['Capital Chemist'] = {'found': len(cap), 'new': new}
    print(f"  New: {new}")
    
    # Pharmacy 4 Less
    print("\n=== Pharmacy 4 Less ===")
    p4l = scrape_pharmacy4less()
    new = sum(1 for s in p4l if insert_pharmacy(conn, s['n'], s['a'], s['lat'], s['lng'], 'pharmacy4less.com.au', s.get('sub'), s.get('st'), s.get('pc')))
    conn.commit()
    results['Pharmacy 4 Less'] = {'found': len(p4l), 'new': new}
    print(f"  New: {new}")
    
    # Summary
    after_count = conn.execute("SELECT COUNT(*) FROM pharmacies").fetchone()[0]
    print(f"\n{'='*60}")
    print(f"Before: {before_count}, After: {after_count}, New: {after_count - before_count}")
    print(f"{'='*60}")
    for chain, data in results.items():
        print(f"  {chain:30s}: {data['found']:4d} found, {data['new']:4d} new")
    
    conn.close()

if __name__ == "__main__":
    main()
