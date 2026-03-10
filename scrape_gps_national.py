#!/usr/bin/env python3
"""
Task 2: National GP scan via OpenStreetMap Overpass API.
Scrapes amenity=doctors, amenity=clinic, healthcare=doctor for all Australian states.
Rate limited, cached, deduplicates by name+coords.
"""
import sqlite3
import requests
import json
import os
import time
import sys

DB_PATH = 'pharmacy_finder.db'
CACHE_DIR = os.path.join('cache', 'overpass')
OVERPASS_URL = 'https://overpass-api.de/api/interpreter'

STATE_BOUNDS = {
    'ACT': {'lat': (-36.0, -35.1), 'lng': (148.7, 149.4)},
    'TAS': {'lat': (-43.7, -39.5), 'lng': (143.5, 148.5)},
    'NT':  {'lat': (-26.1, -10.9), 'lng': (128.9, 138.0)},
    'WA':  {'lat': (-35.2, -13.6), 'lng': (112.9, 129.0)},
    'SA':  {'lat': (-38.1, -25.9), 'lng': (129.0, 141.0)},
    'QLD': {'lat': (-29.2, -10.0), 'lng': (137.9, 153.6)},
    'VIC': {'lat': (-39.2, -33.9), 'lng': (140.9, 150.1)},
    'NSW': {'lat': (-37.6, -28.1), 'lng': (140.9, 153.7)},
}

STATE_NAMES = {
    'NSW': 'New South Wales', 'VIC': 'Victoria', 'QLD': 'Queensland',
    'WA': 'Western Australia', 'SA': 'South Australia', 'TAS': 'Tasmania',
    'NT': 'Northern Territory', 'ACT': 'Australian Capital Territory',
}

SKIP_KEYWORDS = ['veterinary', 'vet clinic', 'dental', 'dentist', 'physiotherapy',
                 'chiropract', 'optom', 'podiatr', 'psycholog', 'massage', 'beauty']

def detect_state(lat, lng):
    # ACT first
    for state in ['ACT', 'TAS', 'NT', 'WA', 'SA', 'QLD', 'VIC', 'NSW']:
        b = STATE_BOUNDS[state]
        if b['lat'][0] <= lat <= b['lat'][1] and b['lng'][0] <= lng <= b['lng'][1]:
            return state
    return None

def cache_path(state):
    return os.path.join(CACHE_DIR, f'gps_{state}.json')

def fetch_overpass(state):
    """Fetch GP data from Overpass API using bounding box for the state."""
    cached = cache_path(state)
    if os.path.exists(cached):
        with open(cached, 'r', encoding='utf-8') as f:
            data = json.load(f)
        print(f"  [{state}] Using cached data ({len(data.get('elements', []))} elements)")
        return data

    b = STATE_BOUNDS[state]
    bbox = f"{b['lat'][0]},{b['lng'][0]},{b['lat'][1]},{b['lng'][1]}"

    query = f"""
    [out:json][timeout:180];
    (
      node["amenity"="doctors"]({bbox});
      way["amenity"="doctors"]({bbox});
      node["amenity"="clinic"]({bbox});
      way["amenity"="clinic"]({bbox});
      node["healthcare"="doctor"]({bbox});
      way["healthcare"="doctor"]({bbox});
      node["healthcare"="clinic"]({bbox});
      way["healthcare"="clinic"]({bbox});
    );
    out center;
    """

    print(f"  [{state}] Querying Overpass API (bbox: {bbox})...")
    try:
        resp = requests.post(OVERPASS_URL, data={'data': query}, timeout=200,
                             headers={'User-Agent': 'PharmacyFinder/2.0 (AU pharmacy location research)'})
        resp.raise_for_status()
        data = resp.json()
        # Cache it
        with open(cached, 'w', encoding='utf-8') as f:
            json.dump(data, f)
        print(f"  [{state}] Got {len(data.get('elements', []))} elements, cached.")
        return data
    except Exception as e:
        print(f"  [{state}] ERROR: {e}")
        return None

def parse_element(el):
    """Parse an OSM element into GP data. Returns dict or None."""
    tags = el.get('tags', {})

    if el.get('type') == 'node':
        lat = el.get('lat')
        lng = el.get('lon')
    else:
        center = el.get('center', {})
        lat = center.get('lat', el.get('lat'))
        lng = center.get('lon', el.get('lon'))

    if not lat or not lng:
        return None

    lat, lng = float(lat), float(lng)
    name = tags.get('name', '').strip()
    if not name:
        name = 'Medical Practice'

    # Skip non-GP facilities
    name_lower = name.lower()
    for kw in SKIP_KEYWORDS:
        if kw in name_lower:
            return None
    healthcare = tags.get('healthcare', '').lower()
    speciality = tags.get('healthcare:speciality', '').lower()
    if healthcare == 'dentist' or 'dentistry' in speciality:
        return None
    amenity = tags.get('amenity', '').lower()
    if amenity == 'dentist' or amenity == 'veterinary':
        return None

    # Build address
    addr_parts = []
    if tags.get('addr:housenumber'):
        addr_parts.append(tags['addr:housenumber'])
    if tags.get('addr:street'):
        addr_parts.append(tags['addr:street'])
    suburb = tags.get('addr:suburb') or tags.get('addr:city') or ''
    if suburb:
        addr_parts.append(suburb)
    state_code = detect_state(lat, lng)
    if state_code:
        addr_parts.append(state_code)
    if tags.get('addr:postcode'):
        addr_parts.append(tags['addr:postcode'])

    address = ', '.join(p for p in addr_parts if p)
    if not address or len(address) < 5:
        address = f"{name}, Australia"

    return {
        'name': name,
        'address': address,
        'latitude': lat,
        'longitude': lng,
        'fte': 1.0,
        'hours_per_week': 38.0,
    }

def dedup_key(name, lat, lng):
    """Key for deduplication: name (lowered) + rounded coords."""
    return f"{name.lower().strip()}|{round(lat, 4)}|{round(lng, 4)}"

def main():
    sys.stdout.reconfigure(encoding='utf-8')
    os.makedirs(CACHE_DIR, exist_ok=True)

    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Load existing GPs for dedup
    c.execute("SELECT name, latitude, longitude FROM gps")
    existing = set()
    for row in c.fetchall():
        existing.add(dedup_key(row[0], row[1], row[2]))
    print(f"Existing GPs in DB: {len(existing)}")

    total_inserted = 0
    total_skipped = 0
    total_dupes = 0

    states = ['ACT', 'TAS', 'NT', 'SA', 'WA', 'QLD', 'VIC', 'NSW']

    for i, state in enumerate(states):
        print(f"\n{'='*60}")
        print(f"Processing {state} ({STATE_NAMES[state]}) [{i+1}/{len(states)}]")
        print(f"{'='*60}")

        data = fetch_overpass(state)
        if data is None:
            print(f"  [{state}] Skipped - no data")
            continue

        elements = data.get('elements', [])
        inserted = 0
        dupes = 0
        skipped = 0

        for el in elements:
            gp = parse_element(el)
            if gp is None:
                skipped += 1
                continue

            key = dedup_key(gp['name'], gp['latitude'], gp['longitude'])
            if key in existing:
                dupes += 1
                continue

            existing.add(key)
            try:
                c.execute("""INSERT INTO gps (name, address, latitude, longitude, fte, hours_per_week, date_scraped)
                             VALUES (?, ?, ?, ?, ?, ?, datetime('now'))""",
                          (gp['name'], gp['address'], gp['latitude'], gp['longitude'],
                           gp['fte'], gp['hours_per_week']))
                inserted += 1
            except Exception as e:
                print(f"  Error inserting {gp['name']}: {e}")

        conn.commit()
        print(f"  [{state}] Inserted: {inserted}, Dupes: {dupes}, Skipped: {skipped}")
        total_inserted += inserted
        total_dupes += dupes
        total_skipped += skipped

        # Rate limit between states (only if we actually hit the API)
        if not os.path.exists(cache_path(state)) or i < len(states) - 1:
            wait = 6
            print(f"  Waiting {wait}s before next state...")
            time.sleep(wait)

    conn.close()

    print(f"\n{'='*60}")
    print(f"NATIONAL GP SCAN COMPLETE")
    print(f"  Total inserted: {total_inserted}")
    print(f"  Total duplicates: {total_dupes}")
    print(f"  Total skipped (non-GP): {total_skipped}")
    print(f"  Total GPs now: {len(existing)}")
    print(f"{'='*60}")

if __name__ == '__main__':
    main()
