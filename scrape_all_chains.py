#!/usr/bin/env python3
"""
Scrape chain pharmacy store locators and add to DB.
Uses Playwright for JS-rendered pages.
"""

import sqlite3
import json
import os
import sys
import time
from datetime import datetime

DB_PATH = "pharmacy_finder.db"

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
    if lat is None or lng is None or lat == 0 or lng == 0:
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

def process_sigma_chain(page, base_url, chain_name, source_domain):
    """Process Sigma Healthcare chains (Amcal, DDS, Guardian) using their Vue store locator."""
    states = ['act', 'nsw', 'nt', 'qld', 'sa', 'tas', 'vic', 'wa']
    all_stores = []
    
    for state in states:
        url = f"{base_url}{state}/"
        try:
            page.goto(url, wait_until='networkidle', timeout=15000)
            page.wait_for_timeout(2000)
            
            data = page.evaluate("""() => {
                const el = document.querySelector('.store-locator-hold');
                if (!el || !el.__vue__) return null;
                const app = el.__vue__;
                if (!app.pharmacies) return null;
                return app.pharmacies.map(p => ({
                    n: p.locationname,
                    a: p.address,
                    lat: parseFloat(p.latitude),
                    lng: parseFloat(p.longitude),
                    sub: p.suburb,
                    st: p.state,
                    pc: p.postcode
                }));
            }""")
            
            if data:
                all_stores.extend(data)
                print(f"  {chain_name} {state.upper()}: {len(data)} stores")
            else:
                print(f"  {chain_name} {state.upper()}: 0 stores (no data)")
        except Exception as e:
            print(f"  {chain_name} {state.upper()}: ERROR - {e}")
    
    return all_stores

def process_blooms(page):
    """Blooms The Chemist - Yext-based store finder."""
    all_stores = []
    
    # Try the store finder page
    page.goto("https://www.bloomsthechemist.com.au/store-finder", wait_until='networkidle', timeout=20000)
    page.wait_for_timeout(3000)
    
    # Check for Yext locator
    data = page.evaluate("""() => {
        // Check for various data sources
        const scripts = document.querySelectorAll('script');
        for (const s of scripts) {
            const t = s.textContent;
            if (t.includes('latitude') && t.includes('longitude')) {
                // Try to extract JSON
                const match = t.match(/"locations"\\s*:\\s*(\\[.*?\\])/s) || 
                              t.match(/"stores"\\s*:\\s*(\\[.*?\\])/s) ||
                              t.match(/var\\s+stores\\s*=\\s*(\\[.*?\\]);/s);
                if (match) {
                    try { return JSON.parse(match[1]); } catch(e) {}
                }
            }
        }
        
        // Check for React/Vue data
        const locator = document.querySelector('[data-react-class], [data-stores], .store-finder');
        if (locator) {
            const dataAttr = locator.getAttribute('data-stores') || locator.getAttribute('data-react-props');
            if (dataAttr) {
                try { return JSON.parse(dataAttr); } catch(e) {}
            }
        }
        
        // Check for map markers
        if (window.google && window.google.maps) {
            // Can't easily extract markers
        }
        
        // Look for store elements with data attributes
        const storeEls = document.querySelectorAll('[data-lat], [data-latitude]');
        if (storeEls.length > 0) {
            return Array.from(storeEls).map(el => ({
                lat: parseFloat(el.dataset.lat || el.dataset.latitude),
                lng: parseFloat(el.dataset.lng || el.dataset.longitude || el.dataset.lon),
                n: el.dataset.name || el.querySelector('h2,h3,h4,.name')?.textContent?.trim(),
                a: el.dataset.address || el.querySelector('.address')?.textContent?.trim()
            }));
        }
        
        return null;
    }""")
    
    if data and isinstance(data, list):
        print(f"  Blooms: Found {len(data)} stores in page data")
        return data
    
    # Try individual store pages from sitemap
    print("  Blooms: No inline data found, trying sitemap approach...")
    
    # Navigate to sitemap
    try:
        resp = page.goto("https://www.bloomsthechemist.com.au/sitemap_index.xml", timeout=10000)
        if resp and resp.status == 200:
            content = page.content()
            import re
            sitemaps = re.findall(r'<loc>(https://.*?sitemap.*?\.xml)</loc>', content)
            print(f"  Blooms: Found {len(sitemaps)} sub-sitemaps")
            
            for sitemap_url in sitemaps:
                if 'store' in sitemap_url.lower() or 'pharmacy' in sitemap_url.lower():
                    page.goto(sitemap_url, timeout=10000)
                    sm_content = page.content()
                    store_urls = re.findall(r'<loc>(https://www\.bloomsthechemist\.com\.au/(?:store|pharmacy)/[^<]+)</loc>', sm_content)
                    print(f"  Blooms: Found {len(store_urls)} store URLs in {sitemap_url}")
    except Exception as e:
        print(f"  Blooms sitemap error: {e}")
    
    return all_stores

def scrape_generic_store_page(page, url, chain_name):
    """Generic approach for various chain websites."""
    stores = []
    try:
        page.goto(url, wait_until='networkidle', timeout=20000)
        page.wait_for_timeout(3000)
        
        data = page.evaluate("""() => {
            const result = {stores: [], method: 'none'};
            
            // Method 1: Vue.js data
            const vueEls = document.querySelectorAll('[data-v-]');
            for (const el of document.querySelectorAll('.store-locator-hold, .store-locator, #store-locator, [class*=store-locator]')) {
                if (el.__vue__) {
                    const d = el.__vue__.$data || el.__vue__;
                    const stores = d.pharmacies || d.stores || d.locations || d.markers;
                    if (stores && stores.length) {
                        result.method = 'vue';
                        result.stores = stores.map(s => ({
                            n: s.locationname || s.name || s.title,
                            a: s.address || s.full_address || '',
                            lat: parseFloat(s.latitude || s.lat),
                            lng: parseFloat(s.longitude || s.lng || s.lon),
                            sub: s.suburb || s.city || '',
                            st: s.state || '',
                            pc: s.postcode || s.postal_code || ''
                        }));
                        return result;
                    }
                }
            }
            
            // Method 2: Data attributes
            const dataEls = document.querySelectorAll('[data-lat], [data-latitude], [data-geo-lat]');
            if (dataEls.length > 0) {
                result.method = 'data-attrs';
                result.stores = Array.from(dataEls).map(el => ({
                    lat: parseFloat(el.dataset.lat || el.dataset.latitude || el.dataset.geoLat),
                    lng: parseFloat(el.dataset.lng || el.dataset.longitude || el.dataset.geoLng || el.dataset.lon),
                    n: el.dataset.name || el.querySelector('h2,h3,h4,.name,.title')?.textContent?.trim() || '',
                    a: el.dataset.address || el.querySelector('.address')?.textContent?.trim() || ''
                }));
                return result;
            }
            
            // Method 3: Embedded JSON in scripts
            const scripts = document.querySelectorAll('script:not([src])');
            for (const s of scripts) {
                const t = s.textContent;
                // Look for store arrays
                const patterns = [
                    /"(?:stores|locations|pharmacies|markers|items)"\s*:\s*(\[[\s\S]*?\])\s*[,}]/,
                    /var\s+(?:stores|locations|pharmacies|markers)\s*=\s*(\[[\s\S]*?\]);/
                ];
                for (const pat of patterns) {
                    const m = t.match(pat);
                    if (m) {
                        try {
                            const parsed = JSON.parse(m[1]);
                            if (parsed.length > 0 && parsed[0] && (parsed[0].lat || parsed[0].latitude || parsed[0].position)) {
                                result.method = 'script-json';
                                result.stores = parsed.map(s => ({
                                    n: s.name || s.title || s.locationname || '',
                                    a: s.address || s.full_address || '',
                                    lat: parseFloat(s.lat || s.latitude || (s.position && s.position.lat) || 0),
                                    lng: parseFloat(s.lng || s.lon || s.longitude || (s.position && s.position.lng) || 0),
                                    sub: s.suburb || s.city || '',
                                    st: s.state || '',
                                    pc: s.postcode || s.postal_code || s.zip || ''
                                }));
                                return result;
                            }
                        } catch(e) {}
                    }
                }
            }
            
            // Method 4: Google Maps markers
            // Check for LD+JSON with store info
            const ldJsonScripts = document.querySelectorAll('script[type="application/ld+json"]');
            for (const s of ldJsonScripts) {
                try {
                    const data = JSON.parse(s.textContent);
                    if (data['@type'] === 'Pharmacy' || data['@type'] === 'Store' || 
                        (Array.isArray(data) && data[0] && data[0]['@type'] === 'Pharmacy')) {
                        const items = Array.isArray(data) ? data : [data];
                        result.method = 'ld-json';
                        result.stores = items.filter(i => i.geo).map(i => ({
                            n: i.name || '',
                            a: i.address ? (typeof i.address === 'string' ? i.address : i.address.streetAddress) : '',
                            lat: parseFloat(i.geo.latitude),
                            lng: parseFloat(i.geo.longitude)
                        }));
                        return result;
                    }
                } catch(e) {}
            }
            
            return result;
        }""")
        
        print(f"  {chain_name}: method={data['method']}, stores={len(data['stores'])}")
        return data['stores']
        
    except Exception as e:
        print(f"  {chain_name}: ERROR - {e}")
        return []

def main():
    from playwright.sync_api import sync_playwright
    
    conn = get_db()
    results = {}
    
    # Count existing
    before_count = conn.execute("SELECT COUNT(*) FROM pharmacies").fetchone()[0]
    print(f"Database has {before_count} pharmacies before scraping\n")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        page = context.new_page()
        
        # ============ 1. AMCAL ============
        print("=== 1. Amcal ===")
        # Load from pre-scraped JSON
        if os.path.exists("all_amcal_stores.json"):
            with open("all_amcal_stores.json") as f:
                amcal_data = json.load(f)
            amcal_stores = []
            for state, stores in amcal_data.items():
                amcal_stores.extend(stores)
            print(f"  Loaded {len(amcal_stores)} stores from pre-scraped data")
        else:
            amcal_stores = process_sigma_chain(page, "https://www.amcal.com.au/store-locator/", "Amcal", "amcal.com.au")
        
        new_amcal = 0
        for s in amcal_stores:
            if insert_pharmacy(conn, s['n'], s['a'], s['lat'], s['lng'], 'amcal.com.au', s.get('sub'), s.get('st'), s.get('pc')):
                new_amcal += 1
        conn.commit()
        results['Amcal'] = {'total_found': len(amcal_stores), 'new_added': new_amcal}
        print(f"  Amcal: {len(amcal_stores)} found, {new_amcal} new added\n")
        
        # ============ 2. DISCOUNT DRUG STORES ============
        print("=== 2. Discount Drug Stores ===")
        dds_stores = process_sigma_chain(page, "https://www.discountdrugstores.com.au/store-locator/", "DDS", "discountdrugstores.com.au")
        new_dds = 0
        for s in dds_stores:
            if insert_pharmacy(conn, s['n'], s['a'], s['lat'], s['lng'], 'discountdrugstores.com.au', s.get('sub'), s.get('st'), s.get('pc')):
                new_dds += 1
        conn.commit()
        results['Discount Drug Stores'] = {'total_found': len(dds_stores), 'new_added': new_dds}
        print(f"  DDS: {len(dds_stores)} found, {new_dds} new added\n")
        
        # ============ 3. GUARDIAN ============
        print("=== 3. Guardian ===")
        # Try guardianpharmacies.com.au - may redirect
        guardian_stores = []
        try:
            guardian_stores = process_sigma_chain(page, "https://www.guardianpharmacies.com.au/store-locator/", "Guardian", "guardianpharmacies.com.au")
        except Exception as e:
            print(f"  Guardian via sigma platform failed: {e}")
            # Try alternative URL
            try:
                guardian_stores = scrape_generic_store_page(page, "https://www.guardian.com.au/store-locator", "Guardian")
            except:
                pass
        new_guardian = 0
        for s in guardian_stores:
            if insert_pharmacy(conn, s['n'], s['a'], s['lat'], s['lng'], 'guardianpharmacies.com.au', s.get('sub'), s.get('st'), s.get('pc')):
                new_guardian += 1
        conn.commit()
        results['Guardian'] = {'total_found': len(guardian_stores), 'new_added': new_guardian}
        print(f"  Guardian: {len(guardian_stores)} found, {new_guardian} new added\n")

        # ============ 4. SOUL PATTINSON ============
        print("=== 4. Soul Pattinson ===")
        sp_stores = process_sigma_chain(page, "https://soulpattinson.com.au/store-locator/", "SoulPattinson", "soulpattinson.com.au")
        if not sp_stores:
            sp_stores = scrape_generic_store_page(page, "https://soulpattinson.com.au/store-locator/", "SoulPattinson")
        new_sp = 0
        for s in sp_stores:
            if insert_pharmacy(conn, s['n'], s['a'], s['lat'], s['lng'], 'soulpattinson.com.au', s.get('sub'), s.get('st'), s.get('pc')):
                new_sp += 1
        conn.commit()
        results['Soul Pattinson'] = {'total_found': len(sp_stores), 'new_added': new_sp}
        print(f"  Soul Pattinson: {len(sp_stores)} found, {new_sp} new added\n")
        
        # ============ 5. BLOOMS THE CHEMIST ============
        print("=== 5. Blooms The Chemist ===")
        blooms_stores = scrape_generic_store_page(page, "https://www.bloomsthechemist.com.au/store-finder", "Blooms")
        new_blooms = 0
        for s in blooms_stores:
            if insert_pharmacy(conn, s.get('n',''), s.get('a',''), s.get('lat',0), s.get('lng',0), 'bloomsthechemist.com.au', s.get('sub'), s.get('st'), s.get('pc')):
                new_blooms += 1
        conn.commit()
        results['Blooms The Chemist'] = {'total_found': len(blooms_stores), 'new_added': new_blooms}
        print(f"  Blooms: {len(blooms_stores)} found, {new_blooms} new added\n")
        
        # ============ 6. GOOD PRICE PHARMACY ============
        print("=== 6. Good Price Pharmacy ===")
        gpp_stores = scrape_generic_store_page(page, "https://www.goodpricepharmacy.com.au/store-locator/", "GoodPrice")
        new_gpp = 0
        for s in gpp_stores:
            if insert_pharmacy(conn, s.get('n',''), s.get('a',''), s.get('lat',0), s.get('lng',0), 'goodpricepharmacy.com.au', s.get('sub'), s.get('st'), s.get('pc')):
                new_gpp += 1
        conn.commit()
        results['Good Price Pharmacy'] = {'total_found': len(gpp_stores), 'new_added': new_gpp}
        print(f"  Good Price: {len(gpp_stores)} found, {new_gpp} new added\n")
        
        # ============ 7. WIZARD PHARMACY ============
        print("=== 7. Wizard Pharmacy ===")
        wizard_stores = scrape_generic_store_page(page, "https://www.wizardpharmacy.com.au/store-locator", "Wizard")
        new_wizard = 0
        for s in wizard_stores:
            if insert_pharmacy(conn, s.get('n',''), s.get('a',''), s.get('lat',0), s.get('lng',0), 'wizardpharmacy.com.au', s.get('sub'), s.get('st'), s.get('pc')):
                new_wizard += 1
        conn.commit()
        results['Wizard Pharmacy'] = {'total_found': len(wizard_stores), 'new_added': new_wizard}
        print(f"  Wizard: {len(wizard_stores)} found, {new_wizard} new added\n")
        
        # ============ 8. NATIONAL PHARMACIES ============
        print("=== 8. National Pharmacies ===")
        np_stores = scrape_generic_store_page(page, "https://www.nationalpharmacies.com.au/stores", "NatPharm")
        new_np = 0
        for s in np_stores:
            if insert_pharmacy(conn, s.get('n',''), s.get('a',''), s.get('lat',0), s.get('lng',0), 'nationalpharmacies.com.au', s.get('sub'), s.get('st'), s.get('pc')):
                new_np += 1
        conn.commit()
        results['National Pharmacies'] = {'total_found': len(np_stores), 'new_added': new_np}
        print(f"  National Pharmacies: {len(np_stores)} found, {new_np} new added\n")
        
        # ============ 9. CAPITAL CHEMIST ============
        print("=== 9. Capital Chemist ===")
        cc_stores = scrape_generic_store_page(page, "https://www.capitalchemist.com.au/stores", "CapitalChemist")
        new_cc = 0
        for s in cc_stores:
            if insert_pharmacy(conn, s.get('n',''), s.get('a',''), s.get('lat',0), s.get('lng',0), 'capitalchemist.com.au', s.get('sub'), s.get('st'), s.get('pc')):
                new_cc += 1
        conn.commit()
        results['Capital Chemist'] = {'total_found': len(cc_stores), 'new_added': new_cc}
        print(f"  Capital Chemist: {len(cc_stores)} found, {new_cc} new added\n")
        
        # ============ 10. PHARMASAVE ============
        print("=== 10. PharmaSave ===")
        ps_stores = scrape_generic_store_page(page, "https://www.pharmasave.com.au/store-locator", "PharmaSave")
        new_ps = 0
        for s in ps_stores:
            if insert_pharmacy(conn, s.get('n',''), s.get('a',''), s.get('lat',0), s.get('lng',0), 'pharmasave.com.au', s.get('sub'), s.get('st'), s.get('pc')):
                new_ps += 1
        conn.commit()
        results['PharmaSave'] = {'total_found': len(ps_stores), 'new_added': new_ps}
        print(f"  PharmaSave: {len(ps_stores)} found, {new_ps} new added\n")
        
        # ============ 11. PHARMACY 4 LESS ============
        print("=== 11. Pharmacy 4 Less ===")
        p4l_stores = scrape_generic_store_page(page, "https://www.pharmacy4less.com.au/storelocator", "Pharmacy4Less")
        new_p4l = 0
        for s in p4l_stores:
            if insert_pharmacy(conn, s.get('n',''), s.get('a',''), s.get('lat',0), s.get('lng',0), 'pharmacy4less.com.au', s.get('sub'), s.get('st'), s.get('pc')):
                new_p4l += 1
        conn.commit()
        results['Pharmacy 4 Less'] = {'total_found': len(p4l_stores), 'new_added': new_p4l}
        print(f"  Pharmacy 4 Less: {len(p4l_stores)} found, {new_p4l} new added\n")
        
        # ============ 12. ALIVE PHARMACY ============
        print("=== 12. Alive Pharmacy ===")
        alive_stores = scrape_generic_store_page(page, "https://www.alivepharmacy.com.au/store-locator", "AlivePharmacy")
        new_alive = 0
        for s in alive_stores:
            if insert_pharmacy(conn, s.get('n',''), s.get('a',''), s.get('lat',0), s.get('lng',0), 'alivepharmacy.com.au', s.get('sub'), s.get('st'), s.get('pc')):
                new_alive += 1
        conn.commit()
        results['Alive Pharmacy'] = {'total_found': len(alive_stores), 'new_added': new_alive}
        print(f"  Alive Pharmacy: {len(alive_stores)} found, {new_alive} new added\n")
        
        # ============ 13. FRIENDLIES ============
        print("=== 13. Friendlies ===")
        fr_stores = scrape_generic_store_page(page, "https://www.friendliespharmacy.com.au/store-locator", "Friendlies")
        new_fr = 0
        for s in fr_stores:
            if insert_pharmacy(conn, s.get('n',''), s.get('a',''), s.get('lat',0), s.get('lng',0), 'friendliespharmacy.com.au', s.get('sub'), s.get('st'), s.get('pc')):
                new_fr += 1
        conn.commit()
        results['Friendlies'] = {'total_found': len(fr_stores), 'new_added': new_fr}
        print(f"  Friendlies: {len(fr_stores)} found, {new_fr} new added\n")
        
        # ============ 14. CINCOTTA ============
        print("=== 14. Cincotta ===")
        cin_stores = scrape_generic_store_page(page, "https://www.cincottachemist.com.au/stores", "Cincotta")
        new_cin = 0
        for s in cin_stores:
            if insert_pharmacy(conn, s.get('n',''), s.get('a',''), s.get('lat',0), s.get('lng',0), 'cincottachemist.com.au', s.get('sub'), s.get('st'), s.get('pc')):
                new_cin += 1
        conn.commit()
        results['Cincotta'] = {'total_found': len(cin_stores), 'new_added': new_cin}
        print(f"  Cincotta: {len(cin_stores)} found, {new_cin} new added\n")
        
        browser.close()
    
    # Final summary
    after_count = conn.execute("SELECT COUNT(*) FROM pharmacies").fetchone()[0]
    conn.close()
    
    print("\n" + "="*60)
    print("SCRAPING SUMMARY")
    print("="*60)
    print(f"Before: {before_count} pharmacies")
    print(f"After:  {after_count} pharmacies")
    print(f"Total new: {after_count - before_count}")
    print()
    for chain, data in results.items():
        print(f"  {chain:30s}: {data['total_found']:4d} found, {data['new_added']:4d} new")
    
    # Save results
    with open("chain_scrape_results.json", "w") as f:
        json.dump(results, f, indent=2)

if __name__ == "__main__":
    main()
