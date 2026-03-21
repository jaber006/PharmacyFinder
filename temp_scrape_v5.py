"""Extract listing data from __APOLLO_STATE__"""
from playwright.sync_api import sync_playwright
import time, json

url = "https://www.commercialrealestate.com.au/for-lease/hobart-tas-7000/retail/"

with sync_playwright() as p:
    browser = p.chromium.launch(
        headless=True,
        args=['--disable-blink-features=AutomationControlled']
    )
    ctx = browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        viewport={"width": 1920, "height": 1080},
        locale="en-AU",
    )
    page = ctx.new_page()
    page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    page.goto(url, timeout=30000, wait_until="domcontentloaded")
    time.sleep(5)
    
    # Extract Apollo State
    apollo = page.evaluate("() => JSON.stringify(window.__APOLLO_STATE__)")
    data = json.loads(apollo)
    
    # Find listing objects
    listings = []
    for key, val in data.items():
        if isinstance(val, dict):
            # Listings typically have address, price, etc.
            if 'address' in val or 'displayAddress' in val or 'listingId' in val or '__typename' in val:
                typename = val.get('__typename', '')
                if 'Listing' in typename or 'Property' in typename or 'address' in val:
                    listings.append((key, val))
    
    print(f"Total keys in Apollo state: {len(data)}")
    print(f"Listing-like objects: {len(listings)}")
    
    # Print typenames found
    typenames = set()
    for key, val in data.items():
        if isinstance(val, dict) and '__typename' in val:
            typenames.add(val['__typename'])
    print(f"\nTypenames: {sorted(typenames)}")
    
    # Find actual listing data
    for key, val in data.items():
        if isinstance(val, dict) and val.get('__typename') == 'Listing':
            print(f"\nListing keys: {list(val.keys())[:20]}")
            # Print a sample
            for k, v in list(val.items())[:15]:
                if isinstance(v, dict) and '__ref' in v:
                    print(f"  {k}: -> {v['__ref'][:80]}")
                elif isinstance(v, (str, int, float, bool)):
                    print(f"  {k}: {str(v)[:80]}")
                elif isinstance(v, list):
                    print(f"  {k}: [{len(v)} items]")
                else:
                    print(f"  {k}: {type(v).__name__}")
            break  # Just show first listing schema
    
    # Now find ResidentialListing, CommercialListing etc
    for key, val in data.items():
        if isinstance(val, dict) and 'Commercial' in val.get('__typename', ''):
            print(f"\nCommercial object: {key}")
            print(f"  typename: {val['__typename']}")
            for k, v in list(val.items())[:15]:
                if isinstance(v, dict) and '__ref' in v:
                    print(f"  {k}: -> {v['__ref'][:80]}")
                elif isinstance(v, (str, int, float)):
                    print(f"  {k}: {str(v)[:80]}")
            break
    
    # Find Address objects
    addr_count = 0
    for key, val in data.items():
        if isinstance(val, dict) and val.get('__typename') == 'Address':
            if addr_count == 0:
                print(f"\nAddress example: {json.dumps(val, indent=2)[:500]}")
            addr_count += 1
    print(f"\nTotal Address objects: {addr_count}")
    
    # Find Price objects
    price_count = 0
    for key, val in data.items():
        if isinstance(val, dict) and 'Price' in val.get('__typename', ''):
            if price_count == 0:
                print(f"\nPrice example: {json.dumps(val, indent=2)[:500]}")
            price_count += 1
    print(f"Total Price objects: {price_count}")
    
    browser.close()
print("\nDone")
