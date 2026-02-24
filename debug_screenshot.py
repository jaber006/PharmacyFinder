import sys, os, time
os.environ['PYTHONIOENCODING'] = 'utf-8'
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)

from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    b = p.chromium.launch(headless=True)
    ctx = b.new_context(viewport={"width":1280,"height":900}, locale="en-AU",
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131.0.0.0")
    pg = ctx.new_page()
    
    # Try navigating like the main script does
    url = "https://www.google.com/maps/search/pharmacy/@-27.5598,151.9507,13z"
    pg.goto(url, wait_until="domcontentloaded", timeout=15000)
    time.sleep(3)
    
    pg.screenshot(path="cache/debug_gmaps.png")
    print("Screenshot saved")
    
    # Check for CAPTCHA or blocking
    html = pg.content()
    if 'captcha' in html.lower() or 'unusual traffic' in html.lower():
        print("*** CAPTCHA/BLOCK DETECTED ***")
    elif 'consent' in html.lower():
        print("Consent dialog present")
    else:
        links = pg.locator('a[href*="/maps/place/"]').count()
        print(f"Place links found: {links}")
    
    b.close()
