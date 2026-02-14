"""
Resume batch extraction from where we left off.
Also retry any errors from previous run.
"""
import json
import time
import os
import sys
import asyncio

from playwright.async_api import async_playwright

BASE_DIR = r"C:\Users\MJ\Documents\GitHub\PharmacyFinder"
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
    # Load batch
    with open(os.path.join(OUTPUT_DIR, "verify_batch.json"), "r") as f:
        batch = json.load(f)
    
    # Load existing results
    results_path = os.path.join(OUTPUT_DIR, "google_extraction_results.json")
    with open(results_path, "r") as f:
        existing_results = json.load(f)
    
    # Find which indices are done (without errors)
    done_indices = set()
    error_indices = set()
    for r in existing_results:
        if r.get("error"):
            error_indices.add(r["index"])
        else:
            done_indices.add(r["index"])
    
    # Filter to remaining items (not done or had errors)
    remaining = [item for item in batch if item["index"] not in done_indices]
    
    print(f"Total batch: {len(batch)}")
    print(f"Already done: {len(done_indices)}")
    print(f"Errors to retry: {len(error_indices)}")
    print(f"Remaining: {len(remaining)}")
    
    if not remaining:
        print("All done!")
        return
    
    # Remove error results from existing (will re-do them)
    clean_results = [r for r in existing_results if not r.get("error")]
    
    async with async_playwright() as p:
        browser = await p.chromium.connect_over_cdp(CDP_URL)
        context = browser.contexts[0]
        page = context.pages[0] if context.pages else await context.new_page()
        
        new_results = []
        
        for i, item in enumerate(remaining):
            url = item["url"]
            idx = item["index"]
            
            print(f"[{i+1}/{len(remaining)}] #{idx} {item['state']} - {item['poi_name'][:40]}...", end="", flush=True)
            
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=15000)
                await asyncio.sleep(4)
                
                try:
                    await page.wait_for_selector('[role="feed"]', timeout=5000)
                except:
                    pass
                
                await asyncio.sleep(1)
                
                pharmacies = await page.evaluate(EXTRACT_JS)
                
                filtered = []
                for ph in pharmacies:
                    if ph.get("lat") and ph.get("lng"):
                        lat_diff = abs(ph["lat"] - item["lat"])
                        lng_diff = abs(ph["lng"] - item["lng"])
                        if lat_diff < 0.5 and lng_diff < 0.5:
                            filtered.append(ph)
                
                result = {
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
                }
                new_results.append(result)
                
                names = [ph["name"] for ph in filtered[:3]]
                print(f" => {len(filtered)} pharmacies: {', '.join(names)}")
                
            except Exception as e:
                err_msg = str(e)[:80]
                print(f" => ERROR: {err_msg}")
                new_results.append({
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
                    "error": err_msg,
                })
                
                # If browser is dead, try to reconnect
                if "closed" in err_msg.lower() or "crash" in err_msg.lower():
                    print("  Trying to reconnect...")
                    try:
                        browser = await p.chromium.connect_over_cdp(CDP_URL)
                        context = browser.contexts[0]
                        page = context.pages[0] if context.pages else await context.new_page()
                        print("  Reconnected!")
                    except Exception as e2:
                        print(f"  Reconnect failed: {e2}")
                        break
            
            if i < len(remaining) - 1:
                delay = 3 + (i % 4)
                await asyncio.sleep(delay)
            
            # Save progress every 10 items
            if (i + 1) % 10 == 0 or i == len(remaining) - 1:
                all_results = clean_results + new_results
                with open(results_path, "w", encoding="utf-8") as f:
                    json.dump(all_results, f, indent=2)
                print(f"  [Saved: {len(all_results)} total results]")
        
        try:
            browser.close()
        except:
            pass
    
    # Final save
    all_results = clean_results + new_results
    with open(results_path, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2)
    
    total = len(all_results)
    errors = sum(1 for r in all_results if r.get("error"))
    ok = total - errors
    print(f"\nDone! {ok} successful, {errors} errors, {total} total")

if __name__ == "__main__":
    asyncio.run(main())
