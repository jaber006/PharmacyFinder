"""Full extraction from commercialrealestate.com.au"""
from playwright.sync_api import sync_playwright
import time, json, re

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
    
    print(f"Fetching: {url}")
    page.goto(url, timeout=30000, wait_until="domcontentloaded")
    time.sleep(6)
    
    # Scroll to load more content
    page.evaluate("window.scrollTo(0, document.body.scrollHeight / 2)")
    time.sleep(2)
    
    content = page.content()
    print(f"Title: {page.title()}")
    print(f"HTML: {len(content)} chars")
    
    # Save full HTML
    with open("temp_full.html", "w", encoding="utf-8") as f:
        f.write(content)
    
    # Try to find listing data in the HTML
    # Look for property/listing URLs
    prop_links = set(re.findall(r'"/for-lease/[^"]*?/(\d+)"', content))
    print(f"\nProperty IDs found: {len(prop_links)}")
    for pid in list(prop_links)[:5]:
        print(f"  ID: {pid}")
    
    # Look for address patterns
    addresses = re.findall(r'"address":"([^"]+)"', content)
    print(f"\nAddresses in JSON: {len(addresses)}")
    for a in addresses[:5]:
        print(f"  {a}")
    
    # Find all script tags with data
    scripts = page.query_selector_all('script')
    print(f"\nScript tags: {len(scripts)}")
    for s in scripts:
        sc = s.inner_html()
        if 'listing' in sc.lower() and len(sc) > 1000:
            print(f"  Script with 'listing' ({len(sc)} chars)")
            # Try to extract JSON
            json_match = re.search(r'({.*"listing.*})', sc, re.DOTALL)
            if json_match:
                try:
                    data = json.loads(json_match.group(1))
                    print(f"    Parsed JSON with keys: {list(data.keys())[:10]}")
                except:
                    pass
    
    # Try evaluating JS to get data
    try:
        result = page.evaluate("""() => {
            // Check for common data stores
            const stores = [];
            if (window.__APOLLO_STATE__) stores.push('__APOLLO_STATE__');
            if (window.__NEXT_DATA__) stores.push('__NEXT_DATA__');
            if (window.__data) stores.push('__data');
            if (window.__INITIAL_STATE__) stores.push('__INITIAL_STATE__');
            if (window.initialState) stores.push('initialState');
            if (window.__APP_DATA__) stores.push('__APP_DATA__');
            return stores;
        }""")
        print(f"\nJS data stores: {result}")
    except Exception as e:
        print(f"\nJS eval error: {e}")
    
    # Try getting listing cards via DOM
    cards = page.query_selector_all('[data-testid*="listing"], [class*="listing-card"], article')
    print(f"\nListing cards: {len(cards)}")
    
    # Try aria-based
    listings = page.query_selector_all('[role="listitem"], [role="article"]')
    print(f"Listitem/article elements: {len(listings)}")
    
    # Find all links with /for-lease/ 
    lease_links = page.query_selector_all('a[href*="/for-lease/"]')
    print(f"Lease links: {len(lease_links)}")
    for ll in lease_links[:5]:
        href = ll.get_attribute("href")
        text = ll.inner_text()[:60].replace("\n", " ")
        print(f"  {href[:80]} | {text}")
    
    browser.close()
print("\nDone")
