#!/usr/bin/env python3
"""Task 1: Fix pharmacy state data using lat/lng bounds."""
import sqlite3

DB_PATH = 'pharmacy_finder.db'

# Check ACT first (it's inside NSW bounds)
STATE_BOUNDS = [
    ('ACT', (-36.0, -35.1), (148.7, 149.4)),
    ('TAS', (-43.7, -39.5), (143.5, 148.5)),
    ('NT',  (-26.1, -10.9), (128.9, 138.0)),
    ('WA',  (-35.2, -13.6), (112.9, 129.0)),
    ('SA',  (-38.1, -25.9), (129.0, 141.0)),
    ('QLD', (-29.2, -10.0), (137.9, 153.6)),
    ('VIC', (-39.2, -33.9), (140.9, 150.1)),
    ('NSW', (-37.6, -28.1), (140.9, 153.7)),
]

def detect_state(lat, lng):
    for state, (lat_min, lat_max), (lng_min, lng_max) in STATE_BOUNDS:
        if lat_min <= lat <= lat_max and lng_min <= lng <= lng_max:
            return state
    return None

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()
c.execute("SELECT id, latitude, longitude FROM pharmacies WHERE (state IS NULL OR state = '') AND latitude IS NOT NULL AND longitude IS NOT NULL")
rows = c.fetchall()
print(f"Found {len(rows)} pharmacies with blank state")

updated = 0
unknown = 0
for pid, lat, lng in rows:
    state = detect_state(lat, lng)
    if state:
        c.execute("UPDATE pharmacies SET state = ? WHERE id = ?", (state, pid))
        updated += 1
    else:
        unknown += 1
        print(f"  Could not determine state for id={pid} lat={lat} lng={lng}")

conn.commit()
conn.close()
print(f"\nUpdated {updated} pharmacies, {unknown} could not be determined")
