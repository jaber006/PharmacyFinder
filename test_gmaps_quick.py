"""Quick test: verify Google Maps scraping still works."""
import sys, os, time, re, json
os.environ['PYTHONIOENCODING'] = 'utf-8'
sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)

from playwright.sync_api import sync_playwright

def scrape(page, lat, lng):
    url = f"https://www.google.com/maps/search/pharmacy/@{lat},{lng},13z"
    page.goto(url, wait_until="domcontentloaded", timeout=15000)
    time.sleep(2.5)
    try:
        btn = page.locator("button:has-text('Accept all')").first
        if btn.is_visible(timeout=1500):
            btn.click(); time.sleep(1.5)
    except: pass
    
    try:
        page.wait_for_selector('a[href*="/maps/place/"]', timeout=5000)
    except:
        return []
    
    time.sleep(0.5)
    results = []
    for link in page.locator('a[href*="/maps/place/"]').all():
        href = link.get_attribute('href') or ''
        lat_m = re.search(r'!3d(-?[\d.]+)', href)
        lng_m = re.search(r'!4d(-?[\d.]+)', href)
        name_m = re.search(r'/maps/place/([^/]+)', href)
        if lat_m and lng_m:
            import urllib.parse
            results.append({
                'name': urllib.parse.unquote(name_m.group(1).replace('+', ' ')) if name_m else '?',
                'lat': float(lat_m.group(1)),
                'lng': float(lng_m.group(1))
            })
    return results

with sync_playwright() as p:
    b = p.chromium.launch(headless=True)
    ctx = b.new_context(viewport={"width":1280,"height":900}, locale="en-AU",
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131.0.0.0")
    pg = ctx.new_page()
    
    tests = [
        ("Coonamble", -30.9553, 148.3936),
        ("Toowoomba", -27.5598, 151.9507),
        ("Remote NT", -23.0, 134.0),
    ]
    for name, lat, lng in tests:
        t0 = time.time()
        r = scrape(pg, lat, lng)
        elapsed = time.time() - t0
        print(f"{name}: {len(r)} pharmacies in {elapsed:.1f}s")
        for ph in r[:5]:
            print(f"  {ph['name']}")
    
    b.close()
print("Done")
