#!/usr/bin/env python3
"""
Connect to the existing Clawdbot Chrome browser via CDP to scrape stores.
The browser is already running at CDP port 18800.
"""

import json
import os
import time
from playwright.sync_api import sync_playwright

DATA_DIR = "chain_data"
os.makedirs(DATA_DIR, exist_ok=True)

CDP_URL = "http://127.0.0.1:18800"

EXTRACT_VUE = """() => {
    const el = document.querySelector('.store-locator-hold');
    if (!el || !el.__vue__) return [];
    const app = el.__vue__;
    if (!app.pharmacies) return [];
    return app.pharmacies.map(p => ({
        n: p.locationname || '',
        a: p.address || '',
        lat: parseFloat(p.latitude) || 0,
        lng: parseFloat(p.longitude) || 0,
        sub: p.suburb || '',
        st: p.state || '',
        pc: p.postcode || ''
    }));
}"""

EXTRACT_GENERIC = """() => {
    // Method 1: Vue data on any element
    const allEls = document.querySelectorAll('*');
    for (const el of allEls) {
        if (el.__vue__) {
            const d = el.__vue__.$data || el.__vue__;
            for (const key of ['pharmacies','stores','locations','markers','items','storeList','allStores']) {
                const stores = d[key];
                if (stores && Array.isArray(stores) && stores.length > 0) {
                    const first = stores[0];
                    if (first.latitude || first.lat || first.position || first.geo) {
                        return stores.map(s => ({
                            n: s.locationname || s.name || s.title || s.storeName || '',
                            a: s.address || s.full_address || s.formatted_address || s.streetAddress || '',
                            lat: parseFloat(s.latitude || s.lat || (s.position ? s.position.lat : 0) || (s.geo ? s.geo.latitude : 0) || 0),
                            lng: parseFloat(s.longitude || s.lng || s.lon || (s.position ? s.position.lng : 0) || (s.geo ? s.geo.longitude : 0) || 0),
                            sub: s.suburb || s.city || '',
                            st: s.state || '',
                            pc: s.postcode || s.postal_code || s.zip || ''
                        }));
                    }
                }
            }
        }
    }
    
    // Method 2: Data attributes
    const dataEls = document.querySelectorAll('[data-lat], [data-latitude], [data-store-lat]');
    if (dataEls.length > 0) {
        return Array.from(dataEls).map(el => ({
            lat: parseFloat(el.dataset.lat || el.dataset.latitude || el.dataset.storeLat || 0),
            lng: parseFloat(el.dataset.lng || el.dataset.longitude || el.dataset.storeLng || el.dataset.lon || 0),
            n: el.dataset.name || el.dataset.storeName || el.querySelector('h2,h3,h4,.name,.title,.store-name')?.textContent?.trim() || '',
            a: el.dataset.address || el.querySelector('.address,.store-address')?.textContent?.trim() || ''
        }));
    }
    
    // Method 3: Window global variables
    for (const v of ['stores','locations','pharmacies','storeData','allStores','mapData','storeLocations','storesList']) {
        if (window[v] && Array.isArray(window[v]) && window[v].length > 0) {
            return window[v].map(s => ({
                n: s.name || s.title || s.storeName || '',
                a: s.address || s.formatted_address || '',
                lat: parseFloat(s.lat || s.latitude || 0),
                lng: parseFloat(s.lng || s.lon || s.longitude || 0),
                sub: s.suburb || s.city || '',
                st: s.state || '',
                pc: s.postcode || ''
            }));
        }
    }
    
    // Method 4: Script tags with JSON data
    for (const script of document.querySelectorAll('script[type="application/json"], script[type="text/x-magento-init"]')) {
        try {
            const data = JSON.parse(script.textContent);
            // Recursively find arrays with lat/lng
            function findStores(obj, depth) {
                if (depth > 5) return null;
                if (Array.isArray(obj) && obj.length > 0 && obj[0] && (obj[0].lat || obj[0].latitude)) {
                    return obj;
                }
                if (typeof obj === 'object' && obj !== null) {
                    for (const val of Object.values(obj)) {
                        const found = findStores(val, depth + 1);
                        if (found) return found;
                    }
                }
                return null;
            }
            const found = findStores(data, 0);
            if (found) {
                return found.map(s => ({
                    n: s.name || s.title || '',
                    a: s.address || '',
                    lat: parseFloat(s.lat || s.latitude || 0),
                    lng: parseFloat(s.lng || s.longitude || 0),
                    sub: s.suburb || s.city || '',
                    st: s.state || '',
                    pc: s.postcode || ''
                }));
            }
        } catch(e) {}
    }
    
    return [];
}"""

def scrape_sigma_states(page, base_url, filename, chain_name):
    """Scrape all states for a Sigma chain."""
    states = ['act', 'nsw', 'nt', 'qld', 'sa', 'tas', 'vic', 'wa']
    all_stores = []
    
    for state in states:
        url = f"{base_url}{state}/"
        try:
            page.goto(url, wait_until='domcontentloaded', timeout=12000)
            page.wait_for_timeout(3000)
            data = page.evaluate(EXTRACT_VUE)
            if data and len(data) > 0:
                all_stores.extend(data)
                print(f"    {state.upper()}: {len(data)}")
        except Exception as e:
            print(f"    {state.upper()}: err")
    
    save(all_stores, filename)
    print(f"  {chain_name}: TOTAL {len(all_stores)}")
    return all_stores

def scrape_page(page, url, filename, chain_name):
    """Scrape a single page."""
    try:
        page.goto(url, wait_until='domcontentloaded', timeout=15000)
        page.wait_for_timeout(4000)
        data = page.evaluate(EXTRACT_GENERIC)
    except Exception as e:
        print(f"  {chain_name}: ERR {str(e)[:80]}")
        data = []
    
    save(data or [], filename)
    print(f"  {chain_name}: {len(data or [])}")
    return data or []

def save(data, filename):
    with open(os.path.join(DATA_DIR, filename), 'w') as f:
        json.dump(data, f, indent=2)

def main():
    with sync_playwright() as p:
        try:
            browser = p.chromium.connect_over_cdp(CDP_URL)
            print(f"Connected to existing browser via CDP")
            ctx = browser.contexts[0]
            page = ctx.new_page()
        except Exception as e:
            print(f"CDP connect failed ({e}), launching new browser...")
            browser = p.chromium.launch(headless=False)
            ctx = browser.new_context(
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                viewport={'width': 1200, 'height': 800}
            )
            page = ctx.new_page()
        
        # 1. DDS
        print("=== Discount Drug Stores ===")
        scrape_sigma_states(page, "https://www.discountdrugstores.com.au/store-locator/", "dds.json", "DDS")
        
        # 2. Soul Pattinson
        print("\n=== Soul Pattinson ===")
        scrape_sigma_states(page, "https://soulpattinson.com.au/store-locator/", "soulpattinson.json", "Soul Pattinson")
        
        # 3. Generic chains
        chains = [
            ("https://www.bloomsthechemist.com.au/store-finder", "blooms.json", "Blooms"),
            ("https://www.goodpricepharmacy.com.au/store-locator/", "goodprice.json", "Good Price"),
            ("https://www.wizardpharmacy.com.au/store-locator", "wizard.json", "Wizard"),
            ("https://www.nationalpharmacies.com.au/stores", "national.json", "National"),
            ("https://www.capitalchemist.com.au/stores", "capital.json", "Capital"),
            ("https://www.pharmasave.com.au/store-locator", "pharmasave.json", "PharmaSave"),
            ("https://www.pharmacy4less.com.au/storelocator", "pharmacy4less.json", "Pharmacy4Less"),
            ("https://www.alivepharmacy.com.au/store-locator", "alive.json", "Alive"),
            ("https://www.friendliespharmacy.com.au/store-locator", "friendlies.json", "Friendlies"),
            ("https://www.cincottachemist.com.au/stores", "cincotta.json", "Cincotta"),
        ]
        
        for url, filename, name in chains:
            print(f"\n=== {name} ===")
            scrape_page(page, url, filename, name)
        
        page.close()
        print("\nDone!")

if __name__ == "__main__":
    main()
