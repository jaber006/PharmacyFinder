#!/usr/bin/env python3
"""
Use Playwright to scrape all chain store locators and save to JSON files.
"""

import json
import os
import time

DATA_DIR = "chain_data"
os.makedirs(DATA_DIR, exist_ok=True)

def scrape_sigma_chain(page, base_url, filename):
    """Scrape Sigma Healthcare chains (Amcal, DDS, Guardian, Soul Pattinson)."""
    states = ['act', 'nsw', 'nt', 'qld', 'sa', 'tas', 'vic', 'wa']
    all_stores = []
    
    for state in states:
        url = f"{base_url}{state}/"
        try:
            page.goto(url, timeout=15000)
            page.wait_for_timeout(3000)
            
            data = page.evaluate("""() => {
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
            }""")
            
            if data:
                all_stores.extend(data)
                print(f"  {state.upper()}: {len(data)} stores")
            else:
                print(f"  {state.upper()}: 0")
        except Exception as e:
            print(f"  {state.upper()}: ERROR - {str(e)[:80]}")
    
    with open(os.path.join(DATA_DIR, filename), 'w') as f:
        json.dump(all_stores, f, indent=2)
    
    print(f"  Total: {len(all_stores)} stores saved to {filename}")
    return len(all_stores)

def scrape_generic(page, url, filename, chain_name):
    """Scrape a generic store locator page."""
    stores = []
    try:
        page.goto(url, timeout=20000)
        page.wait_for_timeout(4000)
        
        data = page.evaluate("""() => {
            const result = [];
            
            // Method 1: Vue.js data
            for (const el of document.querySelectorAll('.store-locator-hold, .store-locator, #store-locator, [class*=store-locator], [class*=store_locator], #storelocator')) {
                if (el.__vue__) {
                    const d = el.__vue__.$data || el.__vue__;
                    const stores = d.pharmacies || d.stores || d.locations || d.markers || d.items;
                    if (stores && stores.length) {
                        return stores.map(s => ({
                            n: s.locationname || s.name || s.title || '',
                            a: s.address || s.full_address || s.formatted_address || '',
                            lat: parseFloat(s.latitude || s.lat || 0),
                            lng: parseFloat(s.longitude || s.lng || s.lon || 0),
                            sub: s.suburb || s.city || '',
                            st: s.state || '',
                            pc: s.postcode || s.postal_code || s.zip || ''
                        }));
                    }
                }
            }
            
            // Method 2: React data
            const reactRoots = document.querySelectorAll('[data-reactroot], #__next, #root');
            
            // Method 3: Data attributes on elements
            const dataEls = document.querySelectorAll('[data-lat], [data-latitude], [data-geo-lat]');
            if (dataEls.length > 0) {
                return Array.from(dataEls).map(el => ({
                    lat: parseFloat(el.dataset.lat || el.dataset.latitude || el.dataset.geoLat || 0),
                    lng: parseFloat(el.dataset.lng || el.dataset.longitude || el.dataset.geoLng || el.dataset.lon || 0),
                    n: el.dataset.name || el.querySelector('h2,h3,h4,.name,.title')?.textContent?.trim() || '',
                    a: el.dataset.address || el.querySelector('.address,.store-address')?.textContent?.trim() || ''
                }));
            }
            
            // Method 4: Embedded JSON in page scripts  
            const scripts = document.querySelectorAll('script:not([src])');
            for (const s of scripts) {
                const t = s.textContent;
                if (t.length < 100) continue;
                
                // Look for store/location arrays with coordinates
                const patterns = [
                    /var\s+(?:stores?|locations?|pharmacies|markers|storeData|allStores|mapData)\s*=\s*(\[[\s\S]*?\]);/,
                    /"(?:stores?|locations?|pharmacies|markers|items)"\s*:\s*(\[[\s\S]*?\])(?:\s*[,}])/
                ];
                
                for (const pat of patterns) {
                    const m = t.match(pat);
                    if (m) {
                        try {
                            const parsed = JSON.parse(m[1]);
                            if (parsed.length > 0) {
                                const first = parsed[0];
                                if (first.lat || first.latitude || first.position || first.geo) {
                                    return parsed.map(s => ({
                                        n: s.name || s.title || s.locationname || '',
                                        a: s.address || s.full_address || '',
                                        lat: parseFloat(s.lat || s.latitude || (s.position && s.position.lat) || (s.geo && s.geo.latitude) || 0),
                                        lng: parseFloat(s.lng || s.lon || s.longitude || (s.position && s.position.lng) || (s.geo && s.geo.longitude) || 0),
                                        sub: s.suburb || s.city || '',
                                        st: s.state || '',
                                        pc: s.postcode || s.postal_code || s.zip || ''
                                    }));
                                }
                            }
                        } catch(e) {}
                    }
                }
            }
            
            // Method 5: Google Maps InfoWindow content (look for store cards)
            const storeCards = document.querySelectorAll('.store-card, .store-item, .location-item, .pharmacy-item, [class*=store-card], [class*=location-card]');
            // Can't easily get lat/lng from these
            
            return result;
        }""")
        
        stores = data if data else []
        
    except Exception as e:
        print(f"  {chain_name}: ERROR - {str(e)[:100]}")
    
    with open(os.path.join(DATA_DIR, filename), 'w') as f:
        json.dump(stores, f, indent=2)
    
    print(f"  {chain_name}: {len(stores)} stores saved to {filename}")
    return len(stores)

def main():
    from playwright.sync_api import sync_playwright
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        page = ctx.new_page()
        
        # Sigma chains (same platform)
        print("\n=== Discount Drug Stores ===")
        scrape_sigma_chain(page, "https://www.discountdrugstores.com.au/store-locator/", "dds.json")
        
        print("\n=== Guardian ===")
        # Guardian might not be on same platform
        try:
            scrape_sigma_chain(page, "https://www.guardianpharmacies.com.au/store-locator/", "guardian.json")
        except:
            print("  Guardian sigma approach failed, trying generic...")
            scrape_generic(page, "https://www.guardian.com.au/store-locator", "guardian.json", "Guardian")
        
        print("\n=== Soul Pattinson ===")
        try:
            scrape_sigma_chain(page, "https://soulpattinson.com.au/store-locator/", "soulpattinson.json")
        except:
            scrape_generic(page, "https://soulpattinson.com.au/store-locator/", "soulpattinson.json", "Soul Pattinson")
        
        # Generic chains
        print("\n=== Blooms The Chemist ===")
        scrape_generic(page, "https://www.bloomsthechemist.com.au/store-finder", "blooms.json", "Blooms")
        
        print("\n=== Good Price Pharmacy ===")
        scrape_generic(page, "https://www.goodpricepharmacy.com.au/store-locator/", "goodprice.json", "Good Price")
        
        print("\n=== Wizard Pharmacy ===")
        scrape_generic(page, "https://www.wizardpharmacy.com.au/store-locator", "wizard.json", "Wizard")
        
        print("\n=== National Pharmacies ===")
        scrape_generic(page, "https://www.nationalpharmacies.com.au/stores", "national.json", "National")
        
        print("\n=== Capital Chemist ===")
        scrape_generic(page, "https://www.capitalchemist.com.au/stores", "capital.json", "Capital")
        
        print("\n=== PharmaSave ===")
        scrape_generic(page, "https://www.pharmasave.com.au/store-locator", "pharmasave.json", "PharmaSave")
        
        print("\n=== Pharmacy 4 Less ===")
        scrape_generic(page, "https://www.pharmacy4less.com.au/storelocator", "pharmacy4less.json", "Pharmacy4Less")
        
        print("\n=== Alive Pharmacy ===")
        scrape_generic(page, "https://www.alivepharmacy.com.au/store-locator", "alive.json", "Alive")
        
        print("\n=== Friendlies ===")
        scrape_generic(page, "https://www.friendliespharmacy.com.au/store-locator", "friendlies.json", "Friendlies")
        
        print("\n=== Cincotta ===")
        scrape_generic(page, "https://www.cincottachemist.com.au/stores", "cincotta.json", "Cincotta")
        
        browser.close()
    
    print("\nDone! All data saved to chain_data/")

if __name__ == "__main__":
    main()
