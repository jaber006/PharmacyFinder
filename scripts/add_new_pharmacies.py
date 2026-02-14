"""
Add newly discovered pharmacies from Google Maps verification to the database.
Filter out non-retail pharmacies (wholesalers, consultants, etc).
"""
import json
import sqlite3
import os
from datetime import datetime

BASE_DIR = r"C:\Users\MJ\Documents\GitHub\PharmacyFinder"
DB_PATH = os.path.join(BASE_DIR, "pharmacy_finder.db")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

# Non-retail pharmacy keywords to filter out
NON_RETAIL = [
    "symbion", "consultant", "compounding", "services pty",
    "wholesale", "distribution", "supply", "logistics"
]

with open(os.path.join(OUTPUT_DIR, "new_pharmacies_from_google.json"), "r") as f:
    new_pharmacies = json.load(f)

print(f"Found {len(new_pharmacies)} potential new pharmacies")

# Filter to likely retail pharmacies
retail = []
excluded = []
for p in new_pharmacies:
    name_lower = p["name"].lower()
    is_non_retail = any(kw in name_lower for kw in NON_RETAIL)
    if is_non_retail:
        excluded.append(p)
        print(f"  EXCLUDED (non-retail): {p['name']}")
    else:
        retail.append(p)
        print(f"  RETAIL: {p['name']} ({p['state']})")

print(f"\nRetail pharmacies to add: {len(retail)}")
print(f"Excluded: {len(excluded)}")

# Deduplicate by name
seen = set()
unique_retail = []
for p in retail:
    if p["name"] not in seen:
        seen.add(p["name"])
        unique_retail.append(p)

print(f"After dedup: {len(unique_retail)}")

# Add to database
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

added = 0
skipped = 0
for p in unique_retail:
    try:
        cursor.execute(
            """INSERT INTO pharmacies (name, address, latitude, longitude, source, date_scraped, state)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                p["name"],
                f"Google Maps verified location",
                p["lat"],
                p["lng"],
                "google_maps_verification",
                datetime.now().isoformat(),
                p["state"],
            )
        )
        added += 1
        print(f"  Added: {p['name']} at {p['lat']},{p['lng']}")
    except sqlite3.IntegrityError:
        skipped += 1
        print(f"  Skipped (already exists): {p['name']}")

conn.commit()
conn.close()

print(f"\nAdded {added} new pharmacies to database, skipped {skipped}")
