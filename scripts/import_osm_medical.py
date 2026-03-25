"""
Import OSM-extracted medical centres and hospitals from commercial_sites_v4.json
into pharmacy_finder.db, with 100m haversine deduplication.
"""

import json
import sqlite3
import math
from datetime import datetime

DB_PATH = "pharmacy_finder.db"
JSON_PATH = "data/commercial_sites_v4.json"

# Types mapping
MC_TYPES = {"medical", "healthcare_doctor", "healthcare_centre"}
HOSP_TYPES = {"healthcare_hospital"}

EARTH_RADIUS_M = 6_371_000  # metres


def haversine_m(lat1, lon1, lat2, lon2):
    """Return distance in metres between two lat/lon points."""
    rlat1, rlat2 = math.radians(lat1), math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2) ** 2
    return 2 * EARTH_RADIUS_M * math.asin(math.sqrt(a))


def load_existing(conn, table):
    """Load existing (lat, lon) from a table for dedup."""
    rows = conn.execute(f"SELECT latitude, longitude FROM {table}").fetchall()
    return [(r[0], r[1]) for r in rows]


def is_duplicate(lat, lon, existing, threshold_m=100):
    """Check if (lat, lon) is within threshold_m of any existing point."""
    for elat, elon in existing:
        if haversine_m(lat, lon, elat, elon) < threshold_m:
            return True
    return False


def main():
    print("Loading JSON data...")
    with open(JSON_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    sites = data["sites"]
    print(f"Total sites in file: {len(sites)}")

    conn = sqlite3.connect(DB_PATH)
    now = datetime.now().isoformat()

    # Separate by target table
    mc_sites = [s for s in sites if s.get("t") in MC_TYPES]
    hosp_sites = [s for s in sites if s.get("t") in HOSP_TYPES]
    print(f"Medical centre candidates: {len(mc_sites)}")
    print(f"Hospital candidates: {len(hosp_sites)}")

    # --- Medical Centres ---
    print("\n--- Medical Centres ---")
    existing_mc = load_existing(conn, "medical_centres")
    print(f"Existing rows: {len(existing_mc)}")

    mc_inserted = 0
    mc_skipped_dupe = 0
    mc_skipped_noname = 0
    # Track newly inserted coords too so we don't insert OSM dupes of each other
    all_mc_coords = list(existing_mc)

    for s in mc_sites:
        name = (s.get("n") or "").strip()
        if not name:
            mc_skipped_noname += 1
            continue
        lat, lon = s["la"], s["ln"]
        address = (s.get("a") or "").strip()
        suburb = (s.get("b") or "").strip()
        full_address = f"{address}, {suburb}".strip(", ") if address or suburb else ""

        if is_duplicate(lat, lon, all_mc_coords):
            mc_skipped_dupe += 1
            continue

        try:
            conn.execute(
                """INSERT INTO medical_centres
                   (name, address, latitude, longitude, num_gps, total_fte, source, date_scraped)
                   VALUES (?, ?, ?, ?, 0, 0, 'osm', ?)""",
                (name, full_address, lat, lon, now),
            )
            all_mc_coords.append((lat, lon))
            mc_inserted += 1
        except sqlite3.IntegrityError:
            mc_skipped_dupe += 1

    conn.commit()
    print(f"Inserted: {mc_inserted}")
    print(f"Skipped (within 100m of existing): {mc_skipped_dupe}")
    print(f"Skipped (no name): {mc_skipped_noname}")

    # --- Hospitals ---
    print("\n--- Hospitals ---")
    existing_hosp = load_existing(conn, "hospitals")
    print(f"Existing rows: {len(existing_hosp)}")

    hosp_inserted = 0
    hosp_skipped_dupe = 0
    hosp_skipped_noname = 0
    all_hosp_coords = list(existing_hosp)

    for s in hosp_sites:
        name = (s.get("n") or "").strip()
        if not name:
            hosp_skipped_noname += 1
            continue
        lat, lon = s["la"], s["ln"]
        address = (s.get("a") or "").strip()
        suburb = (s.get("b") or "").strip()
        full_address = f"{address}, {suburb}".strip(", ") if address or suburb else ""

        if is_duplicate(lat, lon, all_hosp_coords):
            hosp_skipped_dupe += 1
            continue

        try:
            conn.execute(
                """INSERT INTO hospitals
                   (name, address, latitude, longitude, hospital_type, date_scraped)
                   VALUES (?, ?, ?, ?, 'unknown', ?)""",
                (name, full_address, lat, lon, now),
            )
            all_hosp_coords.append((lat, lon))
            hosp_inserted += 1
        except sqlite3.IntegrityError:
            hosp_skipped_dupe += 1

    conn.commit()

    print(f"Inserted: {hosp_inserted}")
    print(f"Skipped (within 100m of existing): {hosp_skipped_dupe}")
    print(f"Skipped (no name): {hosp_skipped_noname}")

    # --- Final counts ---
    mc_total = conn.execute("SELECT COUNT(*) FROM medical_centres").fetchone()[0]
    hosp_total = conn.execute("SELECT COUNT(*) FROM hospitals").fetchone()[0]
    print(f"\n=== FINAL COUNTS ===")
    print(f"medical_centres: {mc_total}")
    print(f"hospitals: {hosp_total}")

    conn.close()


if __name__ == "__main__":
    main()
