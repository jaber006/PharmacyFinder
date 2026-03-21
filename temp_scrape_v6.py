"""Extract PropertyListingType from Apollo state"""
from playwright.sync_api import sync_playwright
import time, json

url = "https://www.commercialrealestate.com.au/for-lease/hobart-tas-7000/retail/"

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True, args=['--disable-blink-features=AutomationControlled'])
    ctx = browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        viewport={"width": 1920, "height": 1080}, locale="en-AU",
    )
    page = ctx.new_page()
    page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    page.goto(url, timeout=30000, wait_until="domcontentloaded")
    time.sleep(5)
    
    apollo = json.loads(page.evaluate("() => JSON.stringify(window.__APOLLO_STATE__)"))
    
    # Show PropertyListingType schema
    for key, val in apollo.items():
        if isinstance(val, dict) and val.get('__typename') == 'PropertyListingType':
            print(f"PropertyListingType keys: {sorted(val.keys())}")
            print(f"\nFull first listing:")
            print(json.dumps(val, indent=2, default=str)[:2000])
            break
    
    # Count them
    listings = [v for v in apollo.values() if isinstance(v, dict) and v.get('__typename') == 'PropertyListingType']
    print(f"\n\nTotal PropertyListingType: {len(listings)}")
    
    # Show 3 listings summary
    for i, l in enumerate(listings[:3]):
        print(f"\n--- Listing {i+1} ---")
        for k in ['id', 'address', 'displayAddress', 'priceText', 'propertyType', 'url', 'listingUrl', 
                   'floorArea', 'landArea', 'headline', 'title']:
            if k in l:
                v = l[k]
                if isinstance(v, dict) and '__ref' in v:
                    # Resolve ref
                    ref = apollo.get(v['__ref'], {})
                    print(f"  {k}: {json.dumps(ref, default=str)[:120]}")
                else:
                    print(f"  {k}: {str(v)[:120]}")
    
    # Check MapLocationType for coords
    maps = [v for v in apollo.values() if isinstance(v, dict) and v.get('__typename') == 'MapLocationType']
    print(f"\nMapLocationType: {len(maps)}")
    if maps:
        print(f"  Example: {json.dumps(maps[0], default=str)}")
    
    # Check ListingContactDetailType for agent info  
    contacts = [v for v in apollo.values() if isinstance(v, dict) and v.get('__typename') == 'ListingContactDetailType']
    print(f"\nListingContactDetailType: {len(contacts)}")
    if contacts:
        print(f"  Example: {json.dumps(contacts[0], default=str)[:200]}")
    
    browser.close()
print("\nDone")
