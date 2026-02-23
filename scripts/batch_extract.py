"""
Batch extract pharmacy data using CDP directly.
Connects to the running Clawd browser via CDP and processes all verification URLs.
"""
import json
import time
import os
import sys

# Use websocket to connect to CDP
try:
    import websocket
except ImportError:
    os.system("pip install websocket-client")
    import websocket

import urllib.request

BASE_DIR = r"C:\Users\MJ\Documents\GitHub\PharmacyFinder"
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
CDP_URL = "http://127.0.0.1:18800"

def get_ws_url():
    """Get the WebSocket debugger URL from CDP."""
    req = urllib.request.urlopen(f"{CDP_URL}/json")
    pages = json.loads(req.read())
    # Find the Google Maps tab or first tab
    for page in pages:
        if page.get("type") == "page":
            return page["webSocketDebuggerUrl"]
    return pages[0]["webSocketDebuggerUrl"] if pages else None

EXTRACT_JS = """
(() => {
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
    return JSON.stringify(results);
})()
"""

def main():
    # Load batch
    with open(os.path.join(OUTPUT_DIR, "verify_batch.json"), "r") as f:
        batch = json.load(f)
    
    print(f"Processing {len(batch)} locations via CDP...")
    
    ws_url = get_ws_url()
    if not ws_url:
        print("ERROR: Could not find browser tab")
        sys.exit(1)
    
    print(f"Connecting to: {ws_url}")
    ws = websocket.create_connection(ws_url, timeout=30)
    
    msg_id = 1
    results = []
    
    for i, item in enumerate(batch):
        url = item["url"]
        idx = item["index"]
        
        print(f"\n[{i+1}/{len(batch)}] #{idx} {item['state']} - {item['poi_name'][:40]}...")
        
        # Navigate
        ws.send(json.dumps({
            "id": msg_id,
            "method": "Page.navigate",
            "params": {"url": url}
        }))
        msg_id += 1
        
        # Wait for response
        while True:
            resp = json.loads(ws.recv())
            if resp.get("id") == msg_id - 1:
                break
        
        # Wait for page load
        time.sleep(5)
        
        # Extract pharmacy data
        ws.send(json.dumps({
            "id": msg_id,
            "method": "Runtime.evaluate",
            "params": {
                "expression": EXTRACT_JS,
                "returnByValue": True
            }
        }))
        msg_id += 1
        
        # Wait for result
        extract_result = None
        deadline = time.time() + 10
        while time.time() < deadline:
            resp = json.loads(ws.recv())
            if resp.get("id") == msg_id - 1:
                extract_result = resp
                break
        
        pharmacies = []
        if extract_result and "result" in extract_result:
            try:
                value = extract_result["result"]["result"].get("value", "[]")
                pharmacies = json.loads(value)
            except (json.JSONDecodeError, KeyError, TypeError):
                pass
        
        # Filter out pharmacies that are clearly in wrong state (e.g. Doncaster VIC showing up in TAS search)
        filtered = []
        for p in pharmacies:
            if p.get("lat") and p.get("lng"):
                # Check if pharmacy is roughly near the search center
                lat_diff = abs(p["lat"] - item["lat"])
                lng_diff = abs(p["lng"] - item["lng"])
                if lat_diff < 0.5 and lng_diff < 0.5:  # ~50km tolerance
                    filtered.append(p)
        
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
        
        pharmacy_names = [p["name"] for p in filtered[:5]]
        print(f"  Found {len(filtered)} pharmacies: {', '.join(pharmacy_names)}")
        
        # Rate limit - varies to look more natural
        if i < len(batch) - 1:
            delay = 3 + (i % 3)  # 3-5 seconds between requests
            time.sleep(delay)
    
    ws.close()
    
    # Save results
    output_path = os.path.join(OUTPUT_DIR, "google_extraction_results.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)
    
    print(f"\n\nDone! Saved {len(results)} results to {output_path}")
    
    # Quick summary
    total_pharmacies = sum(r["pharmacy_count"] for r in results)
    zero_results = sum(1 for r in results if r["pharmacy_count"] == 0)
    print(f"Total pharmacies found: {total_pharmacies}")
    print(f"Locations with zero results: {zero_results}")

if __name__ == "__main__":
    main()
