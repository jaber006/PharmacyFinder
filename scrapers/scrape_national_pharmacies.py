"""
National Pharmacy Scraper — findapharmacy.com.au via Funnelback API
==================================================================
Uses Clawdbot browser control HTTP API to make paginated API requests 
through Chrome, bypassing Cloudflare protection.

The Funnelback API returns ALL 4,288 pharmacies via pagination (500 per page).
No geographic queries needed — just paginate through the full dataset.

Requires: clawd browser running with findapharmacy.com.au loaded

Output: output/national_pharmacies_YYYY-MM-DD.json
"""

import json
import time
import sys
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Set

# ---------- Configuration -----------------------------------------

FUNNELBACK_BASE = "https://tpgoa-search.funnelback.squiz.cloud/s/search.html"
CONTROL_URL = "http://127.0.0.1:18791"
PAGE_SIZE = 500
RATE_LIMIT_SECONDS = 1.0

OUTPUT_DIR = Path(__file__).parent.parent / "output"
DATE_STR = datetime.now().strftime("%Y-%m-%d")
OUTPUT_FILE = OUTPUT_DIR / f"national_pharmacies_{DATE_STR}.json"

TARGET_ID = None


def discover_target_id():
    """Find a browser tab (preferably findapharmacy.com.au)."""
    global TARGET_ID
    try:
        resp = urllib.request.urlopen(f"{CONTROL_URL}/tabs?profile=clawd", timeout=10)
        data = json.loads(resp.read())
        for tab in data.get("tabs", []):
            if tab.get("type") == "page" and "findapharmacy" in tab.get("url", ""):
                TARGET_ID = tab["targetId"]
                return TARGET_ID
        for tab in data.get("tabs", []):
            if tab.get("type") == "page":
                TARGET_ID = tab["targetId"]
                return TARGET_ID
    except Exception as e:
        print(f"  Error discovering tabs: {e}")
    return None


def browser_evaluate(js_fn: str) -> any:
    """Evaluate a JS function in the browser via Clawdbot control API."""
    payload = json.dumps({
        "kind": "evaluate",
        "fn": js_fn,
        "profile": "clawd",
        "targetId": TARGET_ID,
    }).encode("utf-8")

    req = urllib.request.Request(
        f"{CONTROL_URL}/act",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST"
    )

    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            result = json.loads(resp.read())
            if result.get("ok"):
                return result.get("result")
    except urllib.error.HTTPError as e:
        body = e.read().decode()[:200]
        print(f"      HTTP {e.code}: {body}")
    except Exception as e:
        print(f"      Browser evaluate error: {e}")
    return None


def fetch_page(start_rank: int) -> Optional[dict]:
    """Fetch a page of pharmacy results from the Funnelback API."""
    url = (
        f"{FUNNELBACK_BASE}?"
        f"collection=tpgoa~sp-locations&"
        f"profile=react-data&"
        f"query=!null&"
        f"num_ranks={PAGE_SIZE}&"
        f"start_rank={start_rank}&"
        f"serviceKeyword=!null"
    )
    
    js_fn = f"() => fetch('{url}', {{headers: {{'Accept': 'application/json'}}}}).then(r => r.json())"
    return browser_evaluate(js_fn)


def parse_pharmacy(data: dict) -> Optional[dict]:
    """Parse a single pharmacy result into a clean dict."""
    try:
        name = data.get("name", "").strip()
        if not name:
            return None

        pharmacy_id = data.get("id", "")
        
        geometry = data.get("geometry", {})
        coords = geometry.get("coordinates", [])
        if len(coords) < 2:
            return None
        try:
            longitude = float(coords[0])
            latitude = float(coords[1])
        except (ValueError, TypeError):
            return None

        if not (-45 <= latitude <= -10 and 110 <= longitude <= 155):
            return None

        state = data.get("state", "").strip().upper()
        state_map = {
            'VICTORIA': 'VIC', 'NEW SOUTH WALES': 'NSW',
            'QUEENSLAND': 'QLD', 'SOUTH AUSTRALIA': 'SA',
            'WESTERN AUSTRALIA': 'WA', 'TASMANIA': 'TAS',
            'NORTHERN TERRITORY': 'NT',
            'AUSTRALIAN CAPITAL TERRITORY': 'ACT',
        }
        state = state_map.get(state, state)

        address_parts = []
        for field in ("address", "address2", "address3"):
            val = data.get(field)
            if val and val.strip():
                address_parts.append(val.strip())
        city = data.get("city", "").strip()
        postcode = data.get("postcode", "").strip()

        full_address_parts = list(address_parts)
        if city:
            full_address_parts.append(city)
        if state:
            full_address_parts.append(state)
        if postcode:
            full_address_parts.append(postcode)
        full_address = ", ".join(full_address_parts)

        days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
        hours_parts = []
        for day in days:
            hours = data.get(day, {})
            if isinstance(hours, dict):
                open_t = hours.get("open", "Closed")
                close_t = hours.get("close", "Closed")
                if open_t and open_t != "Closed":
                    hours_parts.append(f"{day[:3].title()}: {open_t}-{close_t}")
                else:
                    hours_parts.append(f"{day[:3].title()}: Closed")
        opening_hours = "; ".join(hours_parts)

        return {
            "id": pharmacy_id,
            "name": name,
            "address": full_address,
            "street_address": ", ".join(address_parts),
            "suburb": city,
            "state": state,
            "postcode": postcode,
            "latitude": latitude,
            "longitude": longitude,
            "phone": data.get("phone", "").strip(),
            "email": data.get("email", ""),
            "website": data.get("website", ""),
            "opening_hours": opening_hours,
            "services": data.get("services", ""),
            "qcpp": data.get("qcpp", ""),
            "member_type": data.get("memberType", ""),
            "source": "findapharmacy.com.au",
            "date_scraped": datetime.now().isoformat(),
        }
    except Exception:
        return None


def main():
    print("=" * 60)
    print("NATIONAL PHARMACY SCRAPER - findapharmacy.com.au")
    print(f"Date: {DATE_STR}")
    print("Using paginated Funnelback API (no geographic limits)")
    print("=" * 60)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Discover browser tab
    print("\nConnecting to browser...")
    tid = discover_target_id()
    if not tid:
        print("ERROR: No browser tab found.")
        sys.exit(1)
    print(f"  Target tab: {tid}")

    # Get total count first
    print("\nChecking total pharmacy count...")
    test = fetch_page(1)
    if test is None:
        print("ERROR: API test failed")
        sys.exit(1)
    
    total_matching = test.get("response", {}).get("resultpacket", {}).get("resultsSummary", {}).get("totalMatching", 0)
    print(f"  Total pharmacies in index: {total_matching}")
    
    num_pages = (total_matching + PAGE_SIZE - 1) // PAGE_SIZE
    print(f"  Pages to fetch: {num_pages} (@ {PAGE_SIZE}/page)")

    # Fetch all pages
    all_pharmacies = []
    seen_ids: Set[str] = set()
    parse_errors = 0

    for page in range(num_pages):
        start_rank = page * PAGE_SIZE + 1
        print(f"\n  Page {page+1}/{num_pages} (rank {start_rank}-{start_rank+PAGE_SIZE-1})...", end=" ", flush=True)

        data = fetch_page(start_rank)
        if data is None:
            print("ERROR: null response, retrying...")
            time.sleep(3)
            data = fetch_page(start_rank)
            if data is None:
                print("FAILED - skipping page")
                continue

        results = data.get("results", [])
        new_count = 0

        for raw in results:
            parsed = parse_pharmacy(raw)
            if parsed:
                if parsed["id"] not in seen_ids:
                    seen_ids.add(parsed["id"])
                    all_pharmacies.append(parsed)
                    new_count += 1
            else:
                parse_errors += 1

        print(f"{len(results)} returned, {new_count} new (total: {len(all_pharmacies)})")
        time.sleep(RATE_LIMIT_SECONDS)

    # State counts
    state_counts = {}
    for p in all_pharmacies:
        s = p.get("state", "UNKNOWN")
        state_counts[s] = state_counts.get(s, 0) + 1

    # Summary
    summary = {
        "scrape_date": DATE_STR,
        "source": "findapharmacy.com.au (Funnelback API - paginated)",
        "total_in_index": total_matching,
        "total_scraped": len(all_pharmacies),
        "parse_errors": parse_errors,
        "by_state": dict(sorted(state_counts.items())),
        "scrape_completed": datetime.now().isoformat(),
    }

    output_data = {
        "metadata": summary,
        "pharmacies": all_pharmacies,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)

    # Print summary
    print("\n" + "=" * 60)
    print("SCRAPE COMPLETE")
    print("=" * 60)
    print(f"Total in index:  {total_matching}")
    print(f"Total scraped:   {len(all_pharmacies)}")
    print(f"Parse errors:    {parse_errors}")
    print(f"\nBy state:")
    for state, count in sorted(state_counts.items()):
        print(f"  {state}: {count}")
    print(f"\nSaved to: {OUTPUT_FILE}")
    print(f"File size: {OUTPUT_FILE.stat().st_size / 1024 / 1024:.1f} MB")

    return len(all_pharmacies)


if __name__ == "__main__":
    main()
