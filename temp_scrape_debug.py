"""Quick debug: what does realcommercial show for a TAS suburb?"""
from playwright.sync_api import sync_playwright
import time

# Try a bigger suburb that definitely has listings
urls = [
    "https://www.realcommercial.com.au/for-lease/in-hobart,+tas/list-1?activeSort=list-date&searchRadius=5",
    "https://www.realcommercial.com.au/for-lease/in-burnie,+tas/list-1?activeSort=list-date&searchRadius=5",
]

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    ctx = browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        viewport={"width": 1920, "height": 1080},
    )
    page = ctx.new_page()
    
    for i, url in enumerate(urls):
        print(f"\nFetching: {url}")
        page.goto(url, timeout=30000, wait_until="domcontentloaded")
        time.sleep(4)
        
        # Save screenshot
        page.screenshot(path=f"temp_debug_{i}.png", full_page=False)
        
        # Check page title
        print(f"Title: {page.title()}")
        
        # Look for listing count
        content = page.content()
        
        # Check for various selectors
        selectors = [
            'article', 'div[class*="listing"]', 'div[class*="Listing"]',
            'a[href*="/for-lease/"]', '[class*="Card"]', '[class*="card"]',
            '[data-testid]', '[class*="result"]', '[class*="Result"]',
        ]
        for sel in selectors:
            els = page.query_selector_all(sel)
            if els:
                print(f"  {sel}: {len(els)} elements")
        
        # Save first 5000 chars of HTML for inspection
        with open(f"temp_debug_{i}.html", "w", encoding="utf-8") as f:
            f.write(content[:20000])
        print(f"  Saved HTML ({len(content)} chars total)")
        
        time.sleep(2)
    
    browser.close()
print("\nDone. Check temp_debug_*.png and temp_debug_*.html")
