"""Test correct URL format for commercialrealestate.com.au"""
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
    time.sleep(5)
    
    title = page.title()
    content = page.content()
    print(f"Title: {title}")
    print(f"HTML length: {len(content)}")
    
    # Check for __NEXT_DATA__
    next_data = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', content, re.DOTALL)
    if next_data:
        print("Found __NEXT_DATA__!")
        data = json.loads(next_data.group(1))
        props = data.get("props", {}).get("pageProps", {})
        print(f"pageProps keys: {list(props.keys())[:15]}")
        
        # Look for listings
        for key in props:
            val = props[key]
            if isinstance(val, list) and len(val) > 0:
                print(f"  {key}: list of {len(val)} items")
                if isinstance(val[0], dict):
                    print(f"    first item keys: {list(val[0].keys())[:10]}")
            elif isinstance(val, dict):
                subkeys = list(val.keys())[:10]
                print(f"  {key}: dict with keys {subkeys}")
                # Check for results/listings nested
                for sk in ['results', 'listings', 'items', 'data', 'edges']:
                    if sk in val:
                        inner = val[sk]
                        if isinstance(inner, list):
                            print(f"    {key}.{sk}: list of {len(inner)}")
                            if inner and isinstance(inner[0], dict):
                                print(f"      first keys: {list(inner[0].keys())[:10]}")
                                # Print first listing summary
                                first = inner[0]
                                for fk in ['address', 'displayAddress', 'price', 'propertyType', 'url', 'listingUrl', 'id']:
                                    if fk in first:
                                        print(f"      {fk}: {str(first[fk])[:80]}")
    else:
        print("No __NEXT_DATA__ found")
        # Try other embedded data
        json_blocks = re.findall(r'<script[^>]*type="application/json"[^>]*>(.*?)</script>', content, re.DOTALL)
        print(f"Found {len(json_blocks)} application/json script blocks")
        
        # Check for listing links
        links = re.findall(r'href="(/for-lease/[^"]*?/(\d+))"', content)
        print(f"Found {len(links)} listing links")
        for href, pid in links[:5]:
            print(f"  {href}")
    
    with open("temp_cre_test.html", "w", encoding="utf-8") as f:
        f.write(content[:50000])
    
    browser.close()
print("Done")
