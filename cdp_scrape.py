#!/usr/bin/env python3
"""
Connect to existing Chrome via CDP and scrape chain store locators.
"""

import json
import os
import time
from playwright.sync_api import sync_playwright

DATA_DIR = "chain_data"
os.makedirs(DATA_DIR, exist_ok=True)

EXTRACT_VUE_JS = """() => {
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

EXTRACT_GENERIC_JS = """() => {
    const result = [];
    
    // Method 1: Vue.js data on any element
    const allEls = document.querySelectorAll('*');
    for (const el of allEls) {
        if (el.__vue__) {
            const d = el.__vue__.$data || el.__vue__;
            const stores = d.pharmacies || d.stores || d.locations || d.markers || d.items;
            if (stores && Array.isArray(stores) && stores.length > 0) {
                const first = stores[0];
                if (first.latitude || first.lat || first.position) {
                    return stores.map(s => ({
                        n: s.locationname || s.name || s.title || '',
                        a: s.address || s.full_address || s.formatted_address || '',
                        lat: parseFloat(s.latitude || s.lat || (s.position ? s.position.lat : 0) || 0),
                        lng: parseFloat(s.longitude || s.lng || s.lon || (s.position ? s.position.lng : 0) || 0),
                        sub: s.suburb || s.city || '',
                        st: s.state || '',
                        pc: s.postcode || s.postal_code || s.zip || ''
                    }));
                }
            }
        }
    }
    
    // Method 2: Data attributes
    const dataEls = document.querySelectorAll('[data-lat], [data-latitude]');
    if (dataEls.length > 0) {
        return Array.from(dataEls).map(el => ({
            lat: parseFloat(el.dataset.lat || el.dataset.latitude || 0),
            lng: parseFloat(el.dataset.lng || el.dataset.longitude || el.dataset.lon || 0),
            n: el.dataset.name || el.querySelector('h2,h3,h4,.name,.title,.store-name')?.textContent?.trim() || '',
            a: el.dataset.address || el.querySelector('.address,.store-address')?.textContent?.trim() || ''
        }));
    }
    
    // Method 3: Window variables
    const winVars = ['stores', 'locations', 'pharmacies', 'storeData', 'allStores', 'mapData', 'storeLocations'];
    for (const v of winVars) {
        if (window[v] && Array.isArray(window[v]) && window[v].length > 0) {
            return window[v].map(s => ({
                n: s.name || s.title || '',
                a: s.address || '',
                lat: parseFloat(s.lat || s.latitude || 0),
                lng: parseFloat(s.lng || s.lon || s.longitude || 0),
                sub: s.suburb || s.city || '',
                st: s.state || '',
                pc: s.postcode || ''
            }));
        }
    }
    
    return result;
}"""

def scrape_sigma(page, base_url, filename, chain_name):
    """Scrape Sigma Healthcare chains with state pages."""
    states = ['act', 'nsw', 'nt', 'qld', 'sa', 'tas', 'vic', 'wa']
    all_stores = []
    
    for state in states:
        url = f"{base_url}{state}/"
        try:
            page.goto(url, wait_until='domcontentloaded', timeout=10000)
            page.wait_for_timeout(2500)
            data = page.evaluate(EXTRACT_VUE_JS)
            if data:
                all_stores.extend(data)
                if len(data) > 0:
                    print(f"  {state.upper()}: {len(data)}")
        except Exception as e:
            print(f"  {state.upper()}: ERR {str(e)[:50]}")
    
    filepath = os.path.join(DATA_DIR, filename)
    with open(filepath, 'w') as f:
        json.dump(all_stores, f, indent=2)
    print(f"  TOTAL: {len(all_stores)} -> {filename}")
    return all_stores

def scrape_page(page, url, filename, chain_name):
    """Scrape a single page store locator."""
    try:
        page.goto(url, wait_until='domcontentloaded', timeout=15000)
        page.wait_for_timeout(3000)
        data = page.evaluate(EXTRACT_GENERIC_JS)
        if not data:
            data = []
    except Exception as e:
        print(f"  {chain_name}: ERR {str(e)[:80]}")
        data = []
    
    filepath = os.path.join(DATA_DIR, filename)
    with open(filepath, 'w') as f:
        json.dump(data, f, indent=2)
    print(f"  {chain_name}: {len(data)} -> {filename}")
    return data

def main():
    with sync_playwright() as p:
        # Launch a fresh browser (headless=False to avoid detection issues)
        browser = p.chromium.launch(
            headless=False,
            args=['--window-size=1200,800']
        )
        ctx = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            viewport={'width': 1200, 'height': 800}
        )
        page = ctx.new_page()
        
        print("=== Discount Drug Stores ===")
        scrape_sigma(page, "https://www.discountdrugstores.com.au/store-locator/", "dds.json", "DDS")
        
        print("\n=== Soul Pattinson ===")
        scrape_sigma(page, "https://soulpattinson.com.au/store-locator/", "soulpattinson.json", "Soul Pattinson")
        
        print("\n=== Blooms The Chemist ===")
        scrape_page(page, "https://www.bloomsthechemist.com.au/store-finder", "blooms.json", "Blooms")
        
        print("\n=== Good Price Pharmacy ===")
        scrape_page(page, "https://www.goodpricepharmacy.com.au/store-locator/", "goodprice.json", "Good Price")
        
        print("\n=== Wizard Pharmacy ===")
        scrape_page(page, "https://www.wizardpharmacy.com.au/store-locator", "wizard.json", "Wizard")
        
        print("\n=== National Pharmacies ===")
        scrape_page(page, "https://www.nationalpharmacies.com.au/stores", "national.json", "National")
        
        print("\n=== Capital Chemist ===")
        scrape_page(page, "https://www.capitalchemist.com.au/stores", "capital.json", "Capital")
        
        print("\n=== PharmaSave ===")
        scrape_page(page, "https://www.pharmasave.com.au/store-locator", "pharmasave.json", "PharmaSave")
        
        print("\n=== Pharmacy 4 Less ===")
        scrape_page(page, "https://www.pharmacy4less.com.au/storelocator", "pharmacy4less.json", "Pharmacy4Less")
        
        print("\n=== Alive Pharmacy ===")
        scrape_page(page, "https://www.alivepharmacy.com.au/store-locator", "alive.json", "Alive")
        
        print("\n=== Friendlies ===")
        scrape_page(page, "https://www.friendliespharmacy.com.au/store-locator", "friendlies.json", "Friendlies")
        
        print("\n=== Cincotta ===")
        scrape_page(page, "https://www.cincottachemist.com.au/stores", "cincotta.json", "Cincotta")
        
        # Guardian - try different URLs
        print("\n=== Guardian ===")
        # Guardian may use guardian.com.au or guardianpharmacies.com.au
        scrape_page(page, "https://www.guardianpharmacies.com.au/store-locator", "guardian.json", "Guardian")
        
        browser.close()

if __name__ == "__main__":
    main()
