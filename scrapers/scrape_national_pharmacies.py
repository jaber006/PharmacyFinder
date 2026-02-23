"""
National Pharmacy Scraper — findapharmacy.com.au via Funnelback API
==================================================================
Uses Clawdbot browser control HTTP API (/act endpoint) to make fetch 
requests through Chrome, bypassing Cloudflare protection.

Requires: clawd browser running with findapharmacy.com.au loaded

Output: output/national_pharmacies_YYYY-MM-DD.json
"""

import json
import time
import sys
import os
import urllib.request
import urllib.parse
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set, Optional

# ---------- Configuration -----------------------------------------

FUNNELBACK_BASE = "https://tpgoa-search.funnelback.squiz.cloud/s/search.html"
CONTROL_URL = "http://127.0.0.1:18791"
MAX_RESULTS_PER_QUERY = 500
RATE_LIMIT_SECONDS = 0.8

OUTPUT_DIR = Path(__file__).parent.parent / "output"
DATE_STR = datetime.now().strftime("%Y-%m-%d")
OUTPUT_FILE = OUTPUT_DIR / f"national_pharmacies_{DATE_STR}.json"
CHECKPOINT_FILE = OUTPUT_DIR / f"_scrape_checkpoint_{DATE_STR}.json"

TARGET_ID = None  # will be discovered

# State query points
STATE_QUERY_POINTS = {
    'TAS': [
        (-42.0, 146.5, 300),
    ],
    'ACT': [
        (-35.28, 149.13, 50),
    ],
    'NT': [
        (-12.46, 130.84, 200),
        (-23.70, 133.87, 500),
    ],
    'SA': [
        (-34.93, 138.60, 50),
        (-35.10, 138.55, 50),
        (-34.70, 138.70, 60),
        (-34.85, 138.35, 60),
        (-35.50, 138.50, 150),
        (-33.50, 138.00, 300),
        (-32.00, 137.00, 500),
    ],
    'WA': [
        (-31.95, 115.86, 30),
        (-31.75, 115.85, 40),
        (-32.10, 115.85, 40),
        (-31.95, 116.10, 50),
        (-31.95, 115.60, 50),
        (-33.30, 115.65, 100),
        (-28.77, 114.62, 300),
        (-21.00, 119.00, 600),
        (-34.00, 117.50, 200),
        (-31.50, 121.50, 400),
    ],
    'VIC': [
        (-37.81, 144.96, 8),
        (-37.84, 145.00, 8),
        (-37.78, 145.02, 8),
        (-37.80, 144.88, 8),
        (-37.75, 145.15, 12),
        (-37.90, 145.05, 12),
        (-37.88, 144.75, 12),
        (-37.73, 144.75, 15),
        (-37.95, 145.20, 15),
        (-37.68, 145.00, 15),
        (-37.80, 145.35, 20),
        (-37.60, 145.10, 20),
        (-38.05, 145.10, 15),
        (-37.60, 144.70, 20),
        (-37.55, 145.30, 25),
        (-38.00, 145.40, 20),
        (-37.70, 144.55, 25),
        (-37.50, 144.85, 25),
        (-38.15, 144.35, 40),
        (-37.55, 143.85, 50),
        (-36.76, 144.28, 50),
        (-36.36, 145.39, 50),
        (-38.10, 145.50, 80),
        (-37.80, 147.00, 100),
        (-36.00, 146.50, 100),
        (-36.50, 142.00, 200),
        (-38.35, 145.80, 80),
        (-37.30, 145.80, 80),
    ],
    'QLD': [
        (-27.47, 153.03, 10),
        (-27.40, 153.05, 12),
        (-27.55, 153.05, 12),
        (-27.47, 152.95, 12),
        (-27.35, 153.15, 15),
        (-27.55, 153.15, 15),
        (-27.35, 152.95, 15),
        (-27.60, 152.90, 15),
        (-27.25, 153.10, 20),
        (-27.20, 153.00, 20),
        (-27.70, 153.15, 20),
        (-27.65, 153.35, 20),
        (-27.95, 153.35, 25),
        (-28.05, 153.45, 20),
        (-28.20, 153.50, 20),
        (-26.65, 153.07, 30),
        (-26.40, 153.05, 30),
        (-27.60, 152.70, 30),
        (-27.50, 152.40, 50),
        (-26.00, 152.50, 80),
        (-24.85, 152.35, 50),
        (-23.38, 150.50, 60),
        (-21.14, 149.19, 60),
        (-19.26, 146.80, 50),
        (-16.92, 145.77, 50),
        (-20.00, 148.00, 150),
        (-25.00, 147.00, 300),
        (-18.00, 146.00, 100),
    ],
    'NSW': [
        (-33.87, 151.21, 6),
        (-33.84, 151.25, 8),
        (-33.90, 151.18, 8),
        (-33.87, 151.10, 8),
        (-33.80, 151.18, 8),
        (-33.80, 151.30, 12),
        (-33.75, 151.15, 12),
        (-33.93, 151.25, 10),
        (-33.85, 150.98, 12),
        (-33.92, 151.05, 12),
        (-33.70, 151.05, 15),
        (-33.95, 151.35, 15),
        (-34.05, 151.10, 15),
        (-33.92, 150.85, 15),
        (-33.75, 150.80, 15),
        (-33.70, 150.95, 15),
        (-33.65, 151.20, 15),
        (-33.75, 150.65, 20),
        (-33.85, 150.65, 20),
        (-33.55, 150.70, 20),
        (-34.05, 150.85, 20),
        (-33.40, 151.30, 25),
        (-33.30, 151.50, 25),
        (-32.93, 151.78, 20),
        (-32.75, 151.70, 25),
        (-33.05, 151.65, 25),
        (-32.50, 151.50, 40),
        (-34.42, 150.90, 25),
        (-34.75, 150.65, 50),
        (-35.30, 149.50, 50),
        (-35.85, 150.10, 80),
        (-33.28, 149.10, 60),
        (-33.50, 148.00, 80),
        (-32.25, 148.60, 80),
        (-31.00, 150.80, 80),
        (-30.50, 153.00, 60),
        (-29.50, 153.25, 60),
        (-28.80, 153.30, 60),
        (-31.90, 152.50, 60),
        (-31.40, 152.90, 40),
        (-30.00, 151.50, 100),
        (-33.00, 146.00, 200),
        (-31.50, 145.50, 300),
    ],
}


def discover_target_id():
    """Find a browser tab on findapharmacy.com.au."""
    global TARGET_ID
    try:
        resp = urllib.request.urlopen(f"{CONTROL_URL}/tabs?profile=clawd", timeout=10)
        data = json.loads(resp.read())
        for tab in data.get("tabs", []):
            if tab.get("type") == "page" and "findapharmacy" in tab.get("url", ""):
                TARGET_ID = tab["targetId"]
                return TARGET_ID
        # Fallback: use first page tab
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
            else:
                print(f"      Browser returned error: {result}")
    except urllib.error.HTTPError as e:
        body = e.read().decode()[:200]
        print(f"      HTTP {e.code}: {body}")
    except Exception as e:
        print(f"      Browser evaluate error: {e}")
    return None


def fetch_pharmacies(lat: float, lon: float, radius_km: float) -> Optional[dict]:
    """Fetch pharmacies from the Funnelback API via browser."""
    params = urllib.parse.urlencode({
        "collection": "tpgoa~sp-locations",
        "profile": "react-data",
        "query": "!null",
        "num_ranks": str(MAX_RESULTS_PER_QUERY),
        "sort": "prox",
        "serviceKeyword": "!null",
        "origin": f"{lat},{lon}",
        "maxdist": str(radius_km),
    })
    api_url = f"{FUNNELBACK_BASE}?{params}"

    # Build JS that fetches and returns the full JSON
    # We need to return the data as a serialized string since it can be large
    js_fn = (
        "() => {"
        f"  return fetch('{api_url}', {{headers: {{'Accept': 'application/json'}}}}).then(r => r.json());"
        "}"
    )

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

        phone = data.get("phone", "").strip()

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
            "phone": phone,
            "email": data.get("email", ""),
            "website": data.get("website", ""),
            "opening_hours": opening_hours,
            "services": data.get("services", ""),
            "qcpp": data.get("qcpp", ""),
            "member_type": data.get("memberType", ""),
            "source": "findapharmacy.com.au",
            "date_scraped": datetime.now().isoformat(),
        }
    except Exception as e:
        return None


def load_checkpoint() -> dict:
    if CHECKPOINT_FILE.exists():
        try:
            with open(CHECKPOINT_FILE) as f:
                return json.load(f)
        except Exception:
            pass
    return {"completed_states": [], "pharmacies": {}}


def save_checkpoint(checkpoint: dict):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump(checkpoint, f)


def scrape_state(state: str, seen_ids: Set[str]) -> List[dict]:
    query_points = STATE_QUERY_POINTS.get(state, [])
    if not query_points:
        return []

    pharmacies = []
    cap_warnings = 0

    for i, (lat, lon, radius_km) in enumerate(query_points, 1):
        print(f"    [{i}/{len(query_points)}] ({lat:.2f}, {lon:.2f}) r={radius_km}km...", end=" ", flush=True)

        data = fetch_pharmacies(lat, lon, radius_km)

        if data is None:
            print("ERROR: null response")
            time.sleep(2)
            continue

        if isinstance(data, dict) and "error" in data:
            print(f"ERROR: {data.get('error', 'unknown')}")
            time.sleep(2)
            continue

        results = data.get("results", []) if isinstance(data, dict) else []
        new_count = 0

        for raw in results:
            parsed = parse_pharmacy(raw)
            if parsed and parsed["id"] not in seen_ids:
                seen_ids.add(parsed["id"])
                pharmacies.append(parsed)
                new_count += 1

        hit_cap = len(results) >= MAX_RESULTS_PER_QUERY
        cap_str = " ** HIT 500 CAP **" if hit_cap else ""
        if hit_cap:
            cap_warnings += 1

        print(f"{len(results)} results, {new_count} new{cap_str}")
        time.sleep(RATE_LIMIT_SECONDS)

    if cap_warnings > 0:
        print(f"  WARNING: {cap_warnings} queries hit 500-result cap for {state}")

    return pharmacies


def main():
    print("=" * 60)
    print("NATIONAL PHARMACY SCRAPER - findapharmacy.com.au")
    print(f"Date: {DATE_STR}")
    print("=" * 60)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Discover browser tab
    print("\nConnecting to browser...")
    tid = discover_target_id()
    if not tid:
        print("ERROR: No browser tab found. Make sure clawd browser is running.")
        sys.exit(1)
    print(f"  Target tab: {tid}")

    # Test connection
    print("  Testing API access...")
    test = fetch_pharmacies(-33.87, 151.21, 1)
    if test is None or not isinstance(test, dict):
        print("ERROR: API test failed")
        sys.exit(1)
    test_count = len(test.get("results", []))
    print(f"  OK - test query returned {test_count} results")

    # Load checkpoint
    checkpoint = load_checkpoint()
    completed_states = set(checkpoint.get("completed_states", []))
    all_pharmacies = checkpoint.get("pharmacies", {})
    seen_ids: Set[str] = set()
    for state_list in all_pharmacies.values():
        for p in state_list:
            seen_ids.add(p["id"])

    if completed_states:
        print(f"\n  Resuming: {len(completed_states)} states done, {len(seen_ids)} pharmacies scraped")

    # Scrape each state
    states = ['TAS', 'ACT', 'NT', 'SA', 'WA', 'VIC', 'QLD', 'NSW']

    for state in states:
        if state in completed_states:
            count = len(all_pharmacies.get(state, []))
            print(f"\n  {state}: Already done ({count} pharmacies)")
            continue

        print(f"\n{'='*40}")
        print(f"  {state}")
        print(f"{'='*40}")

        state_pharmacies = scrape_state(state, seen_ids)
        all_pharmacies[state] = state_pharmacies

        print(f"  >> {state}: {len(state_pharmacies)} unique pharmacies")

        completed_states.add(state)
        checkpoint["completed_states"] = list(completed_states)
        checkpoint["pharmacies"] = all_pharmacies
        save_checkpoint(checkpoint)

    # Flatten
    flat_list = []
    for state in states:
        flat_list.extend(all_pharmacies.get(state, []))

    # State counts (from actual pharmacy state field, not query state)
    state_counts = {}
    for p in flat_list:
        s = p.get("state", "UNKNOWN")
        state_counts[s] = state_counts.get(s, 0) + 1

    summary = {
        "scrape_date": DATE_STR,
        "source": "findapharmacy.com.au (Funnelback API)",
        "total_pharmacies": len(flat_list),
        "by_state": dict(sorted(state_counts.items())),
        "scrape_completed": datetime.now().isoformat(),
    }

    output_data = {
        "metadata": summary,
        "pharmacies": flat_list,
    }

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)

    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()

    print("\n" + "=" * 60)
    print("SCRAPE COMPLETE")
    print("=" * 60)
    print(f"Total pharmacies: {len(flat_list)}")
    print(f"\nBy state:")
    for state, count in sorted(state_counts.items()):
        print(f"  {state}: {count}")
    print(f"\nSaved to: {OUTPUT_FILE}")
    print(f"File size: {OUTPUT_FILE.stat().st_size / 1024 / 1024:.1f} MB")

    return len(flat_list)


if __name__ == "__main__":
    main()
