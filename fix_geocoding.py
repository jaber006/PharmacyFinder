#!/usr/bin/env python3
"""
fix_geocoding.py — Audit and fix geocoding errors in the PharmacyFinder opportunities table.

Steps:
1. Back up DB
2. Geocode every opportunity's nearest_town+region via Nominatim (cached)
3. Compare to stored coords — fix any >10km off
4. Recount pharmacies within 10km for ALL opportunities
5. Update nearest_pharmacy_km and nearest_pharmacy_name
6. Mark false positives where pharmacy_10km >= 3 AND nearest_pharmacy_km < 2
7. Report everything
"""

import sqlite3
import math
import time
import shutil
import os
import json
from datetime import datetime
from urllib.request import urlopen, Request
from urllib.parse import quote

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'pharmacy_finder.db')
BACKUP_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'pharmacy_finder_backup_prefixfix.db')

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
USER_AGENT = "PharmacyFinder-GeocodeFix/1.0 (contact: admin@pharmacyfinder.local)"

# Australia bounding box validation
AU_LAT_MIN, AU_LAT_MAX = -44.0, -10.0
AU_LON_MIN, AU_LON_MAX = 113.0, 154.0

DISTANCE_THRESHOLD_KM = 10.0


def haversine(lat1, lon1, lat2, lon2):
    """Calculate distance in km between two points using haversine formula."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(a))


def is_in_australia(lat, lon):
    """Validate coordinates are within Australia."""
    return AU_LAT_MIN <= lat <= AU_LAT_MAX and AU_LON_MIN <= lon <= AU_LON_MAX


def geocode_nominatim(query):
    """Geocode a query string using Nominatim. Returns (lat, lon) or None."""
    encoded = quote(query)
    url = f"{NOMINATIM_URL}?q={encoded}&format=json&countrycodes=au&limit=1"
    req = Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode())
            if data:
                lat = float(data[0]["lat"])
                lon = float(data[0]["lon"])
                return lat, lon
    except Exception as e:
        print(f"  [GEOCODE ERROR] {query}: {e}")
    return None


def geocode_with_cache(conn, query):
    """Geocode with caching in the geocode_cache table."""
    c = conn.cursor()
    c.execute("SELECT latitude, longitude FROM geocode_cache WHERE address = ?", (query,))
    row = c.fetchone()
    if row:
        return row[0], row[1]

    # Rate limit
    time.sleep(1.1)

    result = geocode_nominatim(query)
    if result:
        lat, lon = result
        if is_in_australia(lat, lon):
            c.execute(
                "INSERT OR REPLACE INTO geocode_cache (address, latitude, longitude, date_cached) VALUES (?, ?, ?, ?)",
                (query, lat, lon, datetime.now().isoformat())
            )
            conn.commit()
            return lat, lon
        else:
            print(f"  [OUT OF BOUNDS] {query} → ({lat}, {lon}) — skipping")
    return None


def main():
    print("=" * 80)
    print("PharmacyFinder Geocoding Audit & Fix")
    print("=" * 80)
    print(f"Started: {datetime.now().isoformat()}")

    # Step 1: Backup
    print(f"\n[1] Backing up database to {BACKUP_PATH}")
    shutil.copy2(DB_PATH, BACKUP_PATH)
    print(f"    Backup created ({os.path.getsize(BACKUP_PATH):,} bytes)")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # Load all pharmacies (once, for speed)
    print("\n[2] Loading pharmacies...")
    c.execute("SELECT id, name, latitude, longitude FROM pharmacies")
    pharmacies = [(r["id"], r["name"], r["latitude"], r["longitude"]) for r in c.fetchall()]
    print(f"    Loaded {len(pharmacies)} pharmacies")

    # Load all opportunities
    c.execute("SELECT * FROM opportunities")
    opportunities = [dict(r) for r in c.fetchall()]
    print(f"    Loaded {len(opportunities)} opportunities")

    # Step 2: Audit geocoding
    print("\n[3] Auditing geocoding for all opportunities with nearest_town...")
    fixes = []
    geocode_failures = []
    already_correct = 0
    no_town = 0

    for i, opp in enumerate(opportunities):
        opp_id = opp["id"]
        town = (opp.get("nearest_town") or "").strip()
        region = (opp.get("region") or "").strip()
        old_lat = opp["latitude"]
        old_lon = opp["longitude"]

        if not town or town.lower() == "unknown":
            no_town += 1
            continue

        # Build geocoding query
        if region:
            query = f"{town}, {region}, Australia"
        else:
            query = f"{town}, Australia"

        print(f"  [{i+1}/{len(opportunities)}] {town} ({region})...", end="", flush=True)

        result = geocode_with_cache(conn, query)
        if result is None:
            print(" FAILED to geocode")
            geocode_failures.append((opp_id, town, region))
            continue

        new_lat, new_lon = result
        distance = haversine(old_lat, old_lon, new_lat, new_lon)

        if distance > DISTANCE_THRESHOLD_KM:
            print(f" MISMATCH! {distance:.1f}km off (old: {old_lat:.4f},{old_lon:.4f} -> new: {new_lat:.4f},{new_lon:.4f})")
            fixes.append({
                "id": opp_id,
                "town": town,
                "region": region,
                "old_lat": old_lat,
                "old_lon": old_lon,
                "new_lat": new_lat,
                "new_lon": new_lon,
                "distance_off": round(distance, 1),
            })
        else:
            print(f" OK ({distance:.1f}km)")
            already_correct += 1

    print(f"\n    Results: {already_correct} correct, {len(fixes)} WRONG, {len(geocode_failures)} geocode failures, {no_town} no town name")

    # Step 3: Apply coordinate fixes
    print(f"\n[4] Applying {len(fixes)} coordinate fixes...")
    for fix in fixes:
        c.execute(
            "UPDATE opportunities SET latitude = ?, longitude = ? WHERE id = ?",
            (fix["new_lat"], fix["new_lon"], fix["id"])
        )
        print(f"    Fixed #{fix['id']} {fix['town']} ({fix['region']}): "
              f"({fix['old_lat']:.4f}, {fix['old_lon']:.4f}) -> ({fix['new_lat']:.4f}, {fix['new_lon']:.4f}) "
              f"[was {fix['distance_off']}km off]")
    conn.commit()

    # Step 4: Recount pharmacies for ALL opportunities
    print(f"\n[5] Recounting pharmacies within 10km for all {len(opportunities)} opportunities...")
    pharmacy_changes = []
    false_positives = []

    # Reload opportunities with updated coords
    c.execute("SELECT * FROM opportunities")
    opportunities = [dict(r) for r in c.fetchall()]

    for i, opp in enumerate(opportunities):
        opp_id = opp["id"]
        lat = opp["latitude"]
        lon = opp["longitude"]
        old_count = opp.get("pharmacy_10km", 0)
        old_nearest_km = opp.get("nearest_pharmacy_km")
        old_nearest_name = opp.get("nearest_pharmacy_name")

        # Calculate distances to all pharmacies
        nearest_dist = float("inf")
        nearest_name = None
        count_10km = 0
        count_5km = 0
        count_15km = 0

        for pid, pname, plat, plon in pharmacies:
            d = haversine(lat, lon, plat, plon)
            if d < nearest_dist:
                nearest_dist = d
                nearest_name = pname
            if d <= 5:
                count_5km += 1
            if d <= 10:
                count_10km += 1
            if d <= 15:
                count_15km += 1

        if nearest_dist == float("inf"):
            nearest_dist = 999
            nearest_name = "N/A"

        # Track changes
        if old_count != count_10km:
            pharmacy_changes.append({
                "id": opp_id,
                "town": opp.get("nearest_town", ""),
                "region": opp.get("region", ""),
                "old_count": old_count,
                "new_count": count_10km,
                "old_nearest_km": old_nearest_km,
                "new_nearest_km": round(nearest_dist, 2),
            })

        # Update DB
        c.execute("""
            UPDATE opportunities 
            SET pharmacy_5km = ?, pharmacy_10km = ?, pharmacy_15km = ?,
                nearest_pharmacy_km = ?, nearest_pharmacy_name = ?
            WHERE id = ?
        """, (count_5km, count_10km, count_15km, round(nearest_dist, 4), nearest_name, opp_id))

        if (i + 1) % 20 == 0:
            print(f"    Processed {i+1}/{len(opportunities)}...")

    conn.commit()
    print(f"    Done. {len(pharmacy_changes)} opportunities had pharmacy count changes.")

    # Step 5: Check for false positives
    print(f"\n[6] Checking for false positives (pharmacy_10km >= 3 AND nearest_pharmacy_km < 2)...")
    c.execute("SELECT * FROM opportunities")
    opportunities = [dict(r) for r in c.fetchall()]

    for opp in opportunities:
        opp_id = opp["id"]
        p10 = opp.get("pharmacy_10km", 0)
        nearest_km = opp.get("nearest_pharmacy_km", 999)
        old_ver = opp.get("verification", "")

        if p10 >= 3 and nearest_km < 2 and old_ver != "FAIL_PHARMACY_EXISTS":
            c.execute(
                "UPDATE opportunities SET verification = ? WHERE id = ?",
                ("FAIL_PHARMACY_EXISTS", opp_id)
            )
            false_positives.append({
                "id": opp_id,
                "town": opp.get("nearest_town", ""),
                "region": opp.get("region", ""),
                "pharmacy_10km": p10,
                "nearest_pharmacy_km": round(nearest_km, 2),
                "old_verification": old_ver,
            })

    conn.commit()
    print(f"    {len(false_positives)} new false positives identified")

    # Step 6: Report
    print("\n" + "=" * 80)
    print("GEOCODING AUDIT REPORT")
    print("=" * 80)

    print(f"\nTotal opportunities audited: {len(opportunities)}")
    print(f"Opportunities with wrong coordinates (>10km off): {len(fixes)}")
    print(f"Opportunities with correct coordinates: {already_correct}")
    print(f"Geocode failures: {len(geocode_failures)}")
    print(f"No town name (skipped): {no_town}")
    print(f"Pharmacy count changes: {len(pharmacy_changes)}")
    print(f"New false positives: {len(false_positives)}")

    if fixes:
        print("\n--- COORDINATE FIXES ---")
        print(f"{'ID':>6} {'Town':<25} {'Region':<5} {'Old Coords':<25} {'New Coords':<25} {'Off by (km)':>10}")
        print("-" * 100)
        for f in fixes:
            old_c = f"({f['old_lat']:.4f}, {f['old_lon']:.4f})"
            new_c = f"({f['new_lat']:.4f}, {f['new_lon']:.4f})"
            print(f"{f['id']:>6} {f['town']:<25} {f['region']:<5} {old_c:<25} {new_c:<25} {f['distance_off']:>10.1f}")

    if pharmacy_changes:
        print("\n--- PHARMACY COUNT CHANGES ---")
        print(f"{'ID':>6} {'Town':<25} {'Region':<5} {'Old Count':>10} {'New Count':>10} {'Old Nearest':>12} {'New Nearest':>12}")
        print("-" * 90)
        for ch in pharmacy_changes:
            old_nk = ch.get('old_nearest_km')
            old_nk_str = f"{old_nk:.2f}" if old_nk is not None else "N/A"
            print(f"{ch['id']:>6} {ch['town']:<25} {ch['region']:<5} {ch['old_count']:>10} {ch['new_count']:>10} "
                  f"{old_nk_str:>12} {ch['new_nearest_km']:>12.2f}")

    if false_positives:
        print("\n--- NEW FALSE POSITIVES ---")
        for fp in false_positives:
            print(f"  #{fp['id']} {fp['town']} ({fp['region']}) - {fp['pharmacy_10km']} pharmacies within 10km, "
                  f"nearest at {fp['nearest_pharmacy_km']:.2f}km (was: {fp['old_verification']})")

    # New top 10 leaderboard
    print("\n--- NEW TOP 10 LEADERBOARD (pop_10km >= 2000, excluding false positives) ---")
    c.execute("""
        SELECT id, nearest_town, region, pop_10km, pharmacy_10km, nearest_pharmacy_km, 
               nearest_pharmacy_name, verification, composite_score
        FROM opportunities 
        WHERE pop_10km >= 2000 
          AND LOWER(verification) NOT IN ('fail_pharmacy_exists', 'false_positive', 'false positive')
        ORDER BY composite_score DESC, pop_10km DESC
        LIMIT 10
    """)
    rows = c.fetchall()
    print(f"\n{'Rank':>4} {'ID':>6} {'Town':<25} {'Region':<5} {'Pop 10km':>10} {'Pharm 10km':>10} {'Nearest km':>10} {'Score':>8}")
    print("-" * 85)
    for rank, row in enumerate(rows, 1):
        print(f"{rank:>4} {row[0]:>6} {(row[1] or 'N/A'):<25} {(row[2] or ''):<5} {row[3]:>10} {row[4]:>10} {row[5]:>10.1f} {row[8]:>8.1f}")

    # Pop_10km note
    print("\n--- POP_10KM NOTE ---")
    print("  pop_10km values were NOT recalculated (ABS population data source not embedded in this script).")
    print("  For opportunities with fixed coordinates, pop_10km may be INACCURATE.")
    print("  Recommend re-running population overlay for the fixed opportunities.")

    # Summary of fixes that changed pop_10km inaccuracy
    fixed_ids = {f["id"] for f in fixes}
    print(f"\n  Opportunities with potentially inaccurate pop_10km (coords were fixed): {len(fixed_ids)}")
    for f in fixes:
        opp = next((o for o in opportunities if o["id"] == f["id"]), None)
        if opp:
            print(f"    #{f['id']} {f['town']} ({f['region']}) - pop_10km={opp.get('pop_10km', 'N/A')}")

    conn.close()
    print(f"\nCompleted: {datetime.now().isoformat()}")
    print("=" * 80)


if __name__ == "__main__":
    main()
