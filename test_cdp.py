from playwright.sync_api import sync_playwright
import json, time

p = sync_playwright().start()
b = p.chromium.connect_over_cdp('http://127.0.0.1:18800')
ctx = b.new_context()
page = ctx.new_page()
print('new page created', flush=True)

url = 'https://www.discountdrugstores.com.au/store-locator/nsw/'
page.goto(url, wait_until='domcontentloaded', timeout=12000)
print('navigated', flush=True)
page.wait_for_timeout(3000)

js = """() => {
    const el = document.querySelector('.store-locator-hold');
    if (!el || !el.__vue__) return [];
    return el.__vue__.pharmacies.map(p => ({n: p.locationname, lat: +p.latitude, lng: +p.longitude}));
}"""
data = page.evaluate(js)
print(f'Found {len(data)} stores', flush=True)
if data:
    print(json.dumps(data[0]))

page.close()
ctx.close()
p.stop()
