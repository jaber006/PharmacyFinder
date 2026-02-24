"""
Batch extract pharmacy data using Playwright connecting to existing browser.
"""
import json
import time
import os
import sys
import asyncio

try:
    from playwright.async_api import async_playwright
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'playwright'])
    from playwright.async_api import async_playwright

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
CDP_URL = "http://127.0.0.1:18800"

EXTRACT_JS = """
() => {
    const els = document.querySelectorAll('a[href*="/maps/place/"]');
    const results = [];
    const seen = new Set();
    els.forEach(el => {
        const match = el.href.match(/\\/maps\\/place\\/([^/]+)/);
        if (match) {
            const name = decodeURIComponent(match[1].replace(/\\+/g, ' '));
            if (!seen.has(name)) {
                seen.add(name);
                const dataMatch = el.href.match(/!3d(-?[\\d.]+)!4d(-?[\\d.]+)/);
                results.push({
                    name,
                    lat: dataMatch ? parseFloat(dataMatch[1]) : null,
                    lng: dataMatch ? parseFloat(dataMatch[2]) : null
                });
            }
        }
    });
    return results;
}
"""

async def main():
    with open(os.path.join(OUTPUT_DIR, "verify_batch.json"), "r") as f:
        batch = json.load(f)
    
    print(f"Processing {len(batch)} locations via Playwright CDP...")
    
    async with async_playwright() as p:
        browser = await p.chromium.connect_over_cdp(CDP_URL)
        
        # Get existing context and page
        context = browser.contexts[0]
        pages = context.pages
        page = pages[0] if pages else await context.new_page()
        
        results = []
        
        for i, item in enumerate(batch):
            url = item["url"]
            idx = item["index"]
            
            print(f"[{i+1}/{len(batch)}] #{idx} {item['state']} - {item['poi_name'][:40]}...", end="", flush=True)
            
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=15000)
                
                # Wait for results to load
                await asyncio.sleep(4)
                
                # Try to wait for feed element
                try:
                    await page.wait_for_selector('[role="feed"]', timeout=5000)
                except:
                    pass
                
                # Additional wait for dynamic content
                await asyncio.sleep(1)
                
                # Extract
                pharmacies = await page.evaluate(EXTRACT_JS)
                
                # Filter to nearby locations
                filtered = []
                for ph in pharmacies:
                    if ph.get("lat") and ph.get("lng"):
                        lat_diff = abs(ph["lat"] - item["lat"])
                        lng_diff = abs(ph["lng"] - item["lng"])
                        if lat_diff < 0.5 and lng_diff < 0.5:
                            filtered.append(ph)
                
                results.append({
                    "index": idx,
                    "state": item["state"],
                    "lat": item["lat"],
                    "lng": item["lng"],
                    "poi_name": item["poi_name"],
                    "rules": item["rules"],
                    "search_radius_km": item["search_radius_km"],
                    "confidence": item["confidence"],
                    "pharmacies": filtered,
                    "pharmacy_count": len(filtered),
                })
                
                names = [ph["name"] for ph in filtered[:3]]
                print(f" => {len(filtered)} pharmacies: {', '.join(names)}")
                
            except Exception as e:
                print(f" => ERROR: {str(e)[:60]}")
                results.append({
                    "index": idx,
                    "state": item["state"],
                    "lat": item["lat"],
                    "lng": item["lng"],
                    "poi_name": item["poi_name"],
                    "rules": item["rules"],
                    "search_radius_km": item["search_radius_km"],
                    "confidence": item["confidence"],
                    "pharmacies": [],
                    "pharmacy_count": 0,
                    "error": str(e)[:100],
                })
            
            # Rate limit
            if i < len(batch) - 1:
                delay = 3 + (i % 4)
                await asyncio.sleep(delay)
            
            # Save progress every 10 items
            if (i + 1) % 10 == 0:
                with open(os.path.join(OUTPUT_DIR, "google_extraction_results.json"), "w") as f:
                    json.dump(results, f, indent=2)
                print(f"  [Saved progress: {len(results)} results]")
        
        browser.close()
    
    # Final save
    output_path = os.path.join(OUTPUT_DIR, "google_extraction_results.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)
    
    print(f"\nDone! Saved {len(results)} results to {output_path}")
    total_pharmacies = sum(r["pharmacy_count"] for r in results)
    zero_results = sum(1 for r in results if r["pharmacy_count"] == 0)
    print(f"Total pharmacies found across all locations: {total_pharmacies}")
    print(f"Locations with zero results: {zero_results}")

if __name__ == "__main__":
    asyncio.run(main())
