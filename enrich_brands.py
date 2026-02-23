"""
Load existing OSM supermarket data, infer brand from name field, 
re-save JSON, and update DB. No Overpass queries.
"""

import json
import math
import sqlite3
import os
from datetime import datetime
from collections import defaultdict

OUTPUT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output", "national_supermarket_gla.json")
DB_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pharmacy_finder.db")


def infer_brand(name, raw_brand=""):
    """Infer normalized brand from name and/or raw brand tag."""
    text = f"{raw_brand} {name}".lower()

    if "woolworths" in text or "woolies" in text:
        return "woolworths"
    if "coles" in text and "costco" not in text and "cole" not in text.replace("coles", ""):
        return "coles"
    if "aldi" in text:
        return "aldi"
    if "costco" in text:
        return "costco"
    # IGA variants — check specific first
    if "iga x-press" in text or "iga xpress" in text:
        return "iga_xpress"
    if "iga everyday" in text:
        return "iga_everyday"
    if "iga" in text:
        return "iga"
    if "foodworks" in text:
        return "foodworks"
    if "foodland" in text:
        return "foodland"
    if "drake" in text:
        return "drakes"
    if "harris farm" in text:
        return "harris_farm"
    if "spudshed" in text:
        return "spudshed"
    if "farmer jack" in text:
        return "farmer_jacks"
    if "supabarn" in text:
        return "supabarn"
    if "friendly grocer" in text:
        return "friendly_grocer"
    if "ritchie" in text:
        return "ritchies"
    if "spar" in text.split():  # exact word match to avoid false positives
        return "spar"
    if "nqr" in text:
        return "nqr"
    if "fresh provisions" in text:
        return "fresh_provisions"
    if "tasfresh" in text:
        return "tasfresh"
    if "source bulk" in text:
        return "source_bulk_foods"
    return "unknown"


def haversine_m(lat1, lon1, lat2, lon2):
    R = 6371000
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(a))


def main():
    # ── 1. Load existing JSON ──────────────────────────────────────────
    with open(OUTPUT_FILE, encoding="utf-8") as f:
        data = json.load(f)

    supermarkets = data["supermarkets"]
    print(f"Loaded {len(supermarkets)} supermarkets from JSON")

    # ── 2. Infer / overwrite brand ─────────────────────────────────────
    brand_changed = 0
    for sm in supermarkets:
        old = sm.get("brand", "")
        inferred = infer_brand(sm.get("name", ""), old)
        if inferred != "unknown" or not old:
            if inferred != old:
                brand_changed += 1
            sm["brand_normalised"] = inferred
        else:
            sm["brand_normalised"] = old.lower().strip() if old else "unknown"

    print(f"Brand inferred/updated for {brand_changed} records")

    # ── 3. Re-save JSON ───────────────────────────────────────────────
    data["generated"] = datetime.now().isoformat()
    data["total_count"] = len(supermarkets)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    print(f"Saved {len(supermarkets)} records to {OUTPUT_FILE}")

    # ── 4. Update DB ──────────────────────────────────────────────────
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    cur.execute("SELECT id, name, address, latitude, longitude FROM supermarkets")
    existing = cur.fetchall()
    print(f"\nExisting DB rows: {len(existing)}")

    updated = 0
    inserted = 0
    matched_db_ids = set()

    for sm in supermarkets:
        brand = sm["brand_normalised"]
        best_match = None
        best_dist = 100.0

        for row in existing:
            rid, rname, raddr, rlat, rlon = row
            if rlat and rlon and rid not in matched_db_ids:
                d = haversine_m(sm["latitude"], sm["longitude"], rlat, rlon)
                if d < best_dist:
                    best_dist = d
                    best_match = row

        if best_match:
            rid = best_match[0]
            matched_db_ids.add(rid)
            cur.execute("""
                UPDATE supermarkets
                SET floor_area_sqm  = ?,
                    estimated_gla   = ?,
                    gla_confidence  = 'osm_measured',
                    brand           = COALESCE(NULLIF(?, ''), brand),
                    name            = COALESCE(NULLIF(?, ''), name)
                WHERE id = ?
            """, (sm["area_sqm"], sm["area_sqm"], brand, sm.get("name", ""), rid))
            updated += 1
        else:
            parts = []
            if sm.get("addr_housenumber"):
                parts.append(sm["addr_housenumber"])
            if sm.get("addr_street"):
                parts.append(sm["addr_street"])
            if sm.get("addr_suburb"):
                parts.append(sm["addr_suburb"])
            parts.append(sm.get("state", ""))
            parts.append("Australia")
            address = ", ".join(p for p in parts if p)
            if not address or address in ("Australia", ", Australia"):
                address = f"OSM-{sm['osm_id']}, {sm.get('state','')}, Australia"

            try:
                cur.execute("""
                    INSERT INTO supermarkets
                        (name, address, latitude, longitude,
                         floor_area_sqm, estimated_gla, brand,
                         gla_confidence, date_scraped)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 'osm_measured', ?)
                """, (
                    sm.get("name") or "Unknown Supermarket",
                    address,
                    sm["latitude"], sm["longitude"],
                    sm["area_sqm"], sm["area_sqm"],
                    brand,
                    datetime.now().isoformat(),
                ))
                inserted += 1
            except sqlite3.IntegrityError:
                address = f"{address} (OSM:{sm['osm_id']})"
                try:
                    cur.execute("""
                        INSERT INTO supermarkets
                            (name, address, latitude, longitude,
                             floor_area_sqm, estimated_gla, brand,
                             gla_confidence, date_scraped)
                        VALUES (?, ?, ?, ?, ?, ?, ?, 'osm_measured', ?)
                    """, (
                        sm.get("name") or "Unknown Supermarket",
                        address,
                        sm["latitude"], sm["longitude"],
                        sm["area_sqm"], sm["area_sqm"],
                        brand,
                        datetime.now().isoformat(),
                    ))
                    inserted += 1
                except sqlite3.IntegrityError:
                    pass

    conn.commit()

    cur.execute("SELECT COUNT(*) FROM supermarkets")
    total = cur.fetchone()[0]
    conn.close()

    print(f"Updated: {updated}  |  Inserted: {inserted}  |  Total DB rows: {total}")

    # ── 5. Summary ─────────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("  NATIONAL SUPERMARKET GLA SUMMARY")
    print("=" * 70)
    print(f"\n  Total supermarkets: {len(supermarkets)}")

    by_state = defaultdict(list)
    for sm in supermarkets:
        by_state[sm["state"]].append(sm)

    print(f"\n  {'State':<6} {'Count':>6} {'Avg GLA':>10} {'Min':>8} {'Max':>8}")
    print(f"  {'-'*6} {'-'*6} {'-'*10} {'-'*8} {'-'*8}")
    for st in ["NSW", "VIC", "QLD", "WA", "SA", "TAS", "NT", "ACT"]:
        sms = by_state.get(st, [])
        if sms:
            areas = [s["area_sqm"] for s in sms]
            print(f"  {st:<6} {len(sms):>6} {sum(areas)/len(areas):>10.0f} {min(areas):>8.0f} {max(areas):>8.0f}")

    by_brand = defaultdict(list)
    for sm in supermarkets:
        by_brand[sm["brand_normalised"]].append(sm)
    sorted_brands = sorted(by_brand.items(), key=lambda x: -len(x[1]))

    print(f"\n  {'Brand':<22} {'Count':>6} {'Avg GLA':>10} {'Min':>8} {'Max':>8}")
    print(f"  {'-'*22} {'-'*6} {'-'*10} {'-'*8} {'-'*8}")
    for brand, sms in sorted_brands:
        areas = [s["area_sqm"] for s in sms]
        print(f"  {brand:<22} {len(sms):>6} {sum(areas)/len(areas):>10.0f} {min(areas):>8.0f} {max(areas):>8.0f}")


if __name__ == "__main__":
    main()
