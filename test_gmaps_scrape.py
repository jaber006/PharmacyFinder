"""Quick test: scrape pharmacy results from Google Maps via Playwright."""
import sys, json, time
sys.stdout.reconfigure(line_buffering=True)

from playwright.sync_api import sync_playwright

lat, lng = -30.9553, 148.3936  # Coonamble
url = f"https://www.google.com/maps/search/pharmacy/@{lat},{lng},14z"

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    ctx = browser.new_context(
        viewport={"width": 1280, "height": 900},
        locale="en-AU",
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    )
    page = ctx.new_page()
    
    print(f"Navigating to: {url}")
    page.goto(url, wait_until="domcontentloaded", timeout=30000)
    
    # Handle consent dialog if it appears
    time.sleep(3)
    try:
        accept_btn = page.locator("button:has-text('Accept all')").first
        if accept_btn.is_visible(timeout=2000):
            accept_btn.click()
            print("Clicked consent Accept all")
            time.sleep(2)
    except:
        print("No consent dialog")
    
    # Wait for results
    time.sleep(5)
    
    # Save screenshot for debugging
    page.screenshot(path="cache/gmaps_test.png", full_page=False)
    print("Screenshot saved to cache/gmaps_test.png")
    
    # Try to find result elements - Google Maps uses role="feed" for search results
    # Each result is in a div with role="article" or similar
    
    # Method 1: Look for elements with pharmacy-related content in the results panel
    results = []
    
    # Try various selectors
    selectors_to_try = [
        'div[role="feed"] > div',
        'div.Nv2PK',  # Common Google Maps result class
        'a[href*="/maps/place/"]',
        'div[jsaction*="mouseover:pane"]',
    ]
    
    for sel in selectors_to_try:
        elements = page.locator(sel).all()
        print(f"Selector '{sel}': {len(elements)} elements")
    
    # Try getting all the text from the results panel
    # The results are in the left sidebar
    try:
        feed = page.locator('div[role="feed"]').first
        if feed.is_visible(timeout=3000):
            # Get all child divs that look like results
            children = feed.locator('> div').all()
            print(f"\nFeed children: {len(children)}")
            for i, child in enumerate(children[:10]):
                text = child.inner_text(timeout=2000)
                if text.strip():
                    print(f"\n--- Result {i} ---")
                    print(text[:300])
    except Exception as e:
        print(f"Feed approach failed: {e}")
    
    # Alternative: extract all links to /maps/place/
    links = page.locator('a[href*="/maps/place/"]').all()
    print(f"\nPlace links: {len(links)}")
    for link in links[:10]:
        href = link.get_attribute('href') or ''
        text = link.inner_text(timeout=2000)
        # Extract coordinates from href if possible
        print(f"  Link: {text[:80]} -> {href[:120]}")
    
    # Dump page HTML for analysis
    html = page.content()
    with open("cache/gmaps_test.html", "w", encoding="utf-8") as f:
        f.write(html)
    print(f"\nHTML saved ({len(html)} chars)")
    
    browser.close()
