"""Test commercialrealestate.com.au instead - may be less aggressive on bot protection."""
from playwright.sync_api import sync_playwright
import time, json, re

urls = [
    # commercialrealestate.com.au (REA Group)
    "https://www.commercialrealestate.com.au/for-lease/in-hobart,+tas/?searchRadius=5&propertyTypes=retail",
    # Also try without property type filter  
    "https://www.commercialrealestate.com.au/for-lease/in-hobart,+tas/",
]

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
    # Remove webdriver flag
    page = ctx.new_page()
    page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    for i, url in enumerate(urls):
        print(f"\n--- URL {i}: {url}")
        try:
            page.goto(url, timeout=30000, wait_until="networkidle")
        except Exception as e:
            print(f"  Navigation error: {e}")
            try:
                page.goto(url, timeout=30000, wait_until="domcontentloaded")
                time.sleep(5)
            except:
                continue
        
        time.sleep(3)
        
        title = page.title()
        content = page.content()
        print(f"  Title: {title}")
        print(f"  HTML length: {len(content)}")
        
        # Check for bot protection
        if "KPSDK" in content or len(content) < 2000:
            print("  [BLOCKED] Bot protection detected")
            continue
        
        # Look for listings
        selectors = [
            'article', '[class*="listing"]', '[class*="Listing"]',
            'a[href*="/property/"]', '[class*="Card"]', '[class*="card"]',
            '[data-testid]', '[class*="result"]',
        ]
        for sel in selectors:
            els = page.query_selector_all(sel)
            if els:
                print(f"  {sel}: {len(els)} elements")
                if len(els) <= 5:
                    for el in els[:3]:
                        text = el.inner_text()[:100].replace('\n', ' ')
                        print(f"    -> {text}")
        
        # Check for __NEXT_DATA__ or embedded JSON
        next_data = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', content, re.DOTALL)
        if next_data:
            print("  Found __NEXT_DATA__!")
            try:
                data = json.loads(next_data.group(1))
                # Navigate to find listings
                props = data.get("props", {}).get("pageProps", {})
                keys = list(props.keys())[:10]
                print(f"  pageProps keys: {keys}")
            except json.JSONDecodeError:
                print("  (couldn't parse)")
        
        # Save HTML snippet
        with open(f"temp_cre_{i}.html", "w", encoding="utf-8") as f:
            f.write(content[:30000])
        
        time.sleep(2)
    
    browser.close()
print("\nDone")
