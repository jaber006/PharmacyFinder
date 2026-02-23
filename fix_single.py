"""Quick fix: verify a single location via Google Maps."""
import re, time, sys, io
from urllib.parse import quote
from playwright.sync_api import sync_playwright

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace', line_buffering=True)

query = "174 Invermay Road, Invermay, Tasmania 7248"

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False, args=['--disable-blink-features=AutomationControlled', '--start-minimized'])
    ctx = browser.new_context(user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36', viewport={'width': 1280, 'height': 720})
    page = ctx.new_page()
    
    page.goto("https://www.google.com/maps/@-41.42,147.13,14z", timeout=30000)
    time.sleep(3)
    for btn in ['Accept all', 'Reject all']:
        try:
            b = page.get_by_role("button", name=btn)
            if b.is_visible(timeout=2000): b.click(); time.sleep(2); break
        except: pass
    
    page.goto(f"https://www.google.com/maps/search/{quote(query)}", timeout=30000, wait_until='domcontentloaded')
    time.sleep(8)
    
    url = page.url
    print(f"URL1: {url}")
    
    # Try clicking first result if it's a list
    try:
        first = page.locator('a[href*="/maps/place/"]').first
        if first.is_visible(timeout=3000):
            first.click()
            time.sleep(4)
            url = page.url
            print(f"URL2: {url}")
    except:
        pass
    
    # Wait more for redirect
    time.sleep(4)
    url = page.url
    print(f"URL3: {url}")
    
    m = re.search(r'@(-?\d+\.\d{4,}),(\d+\.\d{4,})', url)
    if m:
        print(f"Coords: ({m.group(1)}, {m.group(2)})")
    m = re.search(r'!3d(-?\d+\.\d{4,})!4d(\d+\.\d{4,})', url)
    if m:
        print(f"Coords (3d/4d): ({m.group(1)}, {m.group(2)})")
    
    browser.close()
