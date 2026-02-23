"""
Update Pharmacy Database from Scraped Data
===========================================
Reads the scraped national pharmacy JSON and updates the pharmacy_finder.db.

Strategy:
  1. BACKUP the database first
  2. Read the scraped JSON
  3. For each scraped pharmacy:
     - Match against existing DB entries by findapharmacy ID, name+postcode, or coordinates
     - If matched: update fields (address, hours, phone, etc.)
     - If not matched: insert as new
  4. Flag pharmacies in DB from findapharmacy.com.au that no longer appear in scrape
  5. Preserve pharmacies from other sources (chain websites, OSM, etc.)
  6. Report: added, updated, removed, unchanged counts

Usage:
    python scrapers/update_pharmacy_db.py [--json PATH] [--dry-run]
"""

import json
import sys
import shutil
import sqlite3
import argparse
import math
from datetime import datetime
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "pharmacy_finder.db"
DEFAULT_JSON = Path(__file__).parent.parent / "output" / f"national_pharmacies_{datetime.now().strftime('%Y-%m-%d')}.json"


def haversine_km(lat1, lon1, lat2, lon2):
    """Haversine distance in km."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))


def backup_db(db_path: Path):
    """Create a timestamped backup of the database."""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = db_path.parent / f"pharmacy_finder_backup_{ts}.db"
    shutil.copy2(db_path, backup_path)
    print(f"  Backup created: {backup_path}")
    print(f"  Backup size: {backup_path.stat().st_size / 1024 / 1024:.1f} MB")
    return backup_path


def load_scraped_data(json_path: Path) -> list:
    """Load the scraped pharmacy data from JSON."""
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    pharmacies = data.get("pharmacies", [])
    print(f"  Loaded {len(pharmacies)} pharmacies from {json_path.name}")
    print(f"  Metadata: {json.dumps(data.get('metadata', {}), indent=2)}")
    return pharmacies


def normalize_name(name: str) -> str:
    """Normalize pharmacy name for matching."""
    if not name:
        return ""
    # Normalize unicode quotes and common variations
    name = name.replace("\u2019", "'").replace("\u2018", "'")
    name = name.replace("\u201c", '"').replace("\u201d", '"')
    name = name.lower().strip()
    # Remove common suffixes for matching
    for suffix in [" pharmacy", " chemist", " pharmacies"]:
        if name.endswith(suffix):
            pass  # keep suffix for now, just normalize
    return name


def main():
    parser = argparse.ArgumentParser(description="Update pharmacy database from scraped data")
    parser.add_argument("--json", type=str, help="Path to scraped JSON file")
    parser.add_argument("--dry-run", action="store_true", help="Don't actually modify the database")
    args = parser.parse_args()

    json_path = Path(args.json) if args.json else DEFAULT_JSON
    
    if not json_path.exists():
        # Try to find most recent JSON
        output_dir = Path(__file__).parent.parent / "output"
        jsons = sorted(output_dir.glob("national_pharmacies_*.json"), reverse=True)
        if jsons:
            json_path = jsons[0]
        else:
            print(f"ERROR: No scraped JSON found at {json_path}")
            sys.exit(1)

    print("=" * 60)
    print("PHARMACY DATABASE UPDATE")
    print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'DRY RUN - no changes will be made' if args.dry_run else 'LIVE - database will be modified'}")
    print("=" * 60)

    # Step 1: Backup
    print("\n1. Backing up database...")
    if not args.dry_run:
        backup_db(DB_PATH)
    else:
        print("  (skipped - dry run)")

    # Step 2: Load scraped data
    print("\n2. Loading scraped data...")
    scraped = load_scraped_data(json_path)

    # Step 3: Load existing DB
    print("\n3. Loading existing database...")
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM pharmacies")
    total_before = cur.fetchone()[0]
    print(f"  Existing pharmacies: {total_before}")

    # Load all existing pharmacies
    cur.execute("SELECT * FROM pharmacies")
    existing = cur.fetchall()
    print(f"  Loaded {len(existing)} rows")

    # Build lookup indices
    # By name + postcode (normalized)
    name_pc_index = {}
    # By approximate coordinates
    coord_index = []  # [(lat, lon, row_id)]
    # By source
    source_index = {}

    for row in existing:
        row_dict = dict(row)
        row_id = row_dict["id"]
        name_norm = normalize_name(row_dict.get("name", ""))
        postcode = (row_dict.get("postcode") or "").strip()
        source = row_dict.get("source", "")

        key = f"{name_norm}|{postcode}"
        if key not in name_pc_index:
            name_pc_index[key] = []
        name_pc_index[key].append(row_dict)

        lat = row_dict.get("latitude")
        lon = row_dict.get("longitude")
        if lat and lon:
            coord_index.append((lat, lon, row_id, row_dict))

        if source not in source_index:
            source_index[source] = []
        source_index[source].append(row_dict)

    fap_count = len(source_index.get("findapharmacy.com.au", []))
    other_count = total_before - fap_count
    print(f"  From findapharmacy.com.au: {fap_count}")
    print(f"  From other sources: {other_count}")

    # Step 4: Match and update
    print("\n4. Matching scraped data against database...")
    
    stats = {
        "added": 0,
        "updated": 0,
        "unchanged": 0,
        "matched_by_name": 0,
        "matched_by_coords": 0,
    }

    matched_db_ids = set()
    now = datetime.now().isoformat()

    for sp in scraped:
        name_norm = normalize_name(sp.get("name", ""))
        postcode = sp.get("postcode", "").strip()
        lat = sp.get("latitude")
        lon = sp.get("longitude")

        # Try match by name + postcode
        key = f"{name_norm}|{postcode}"
        matches = name_pc_index.get(key, [])
        
        matched = None
        if matches:
            matched = matches[0]
            stats["matched_by_name"] += 1
        else:
            # Try match by coordinates (within 100m)
            if lat and lon:
                for clat, clon, cid, crow in coord_index:
                    if abs(clat - lat) < 0.002 and abs(clon - lon) < 0.002:
                        dist = haversine_km(lat, lon, clat, clon)
                        if dist < 0.1:  # 100 meters
                            matched = crow
                            stats["matched_by_coords"] += 1
                            break

        if matched:
            matched_db_ids.add(matched["id"])
            
            # Check if anything changed
            changed = False
            updates = {}
            
            # Fields to compare/update
            field_map = {
                "name": sp.get("name", ""),
                "address": sp.get("address", ""),
                "latitude": lat,
                "longitude": lon,
                "suburb": sp.get("suburb", ""),
                "state": sp.get("state", ""),
                "postcode": postcode,
                "opening_hours": sp.get("opening_hours", ""),
            }

            for field, new_val in field_map.items():
                old_val = matched.get(field)
                if new_val and str(new_val) != str(old_val or ""):
                    updates[field] = new_val
                    changed = True

            if changed and not args.dry_run:
                updates["source"] = "findapharmacy.com.au"
                updates["date_scraped"] = now
                set_clause = ", ".join(f"{k} = ?" for k in updates)
                values = list(updates.values()) + [matched["id"]]
                cur.execute(f"UPDATE pharmacies SET {set_clause} WHERE id = ?", values)
                stats["updated"] += 1
            elif changed:
                stats["updated"] += 1
            else:
                stats["unchanged"] += 1
        else:
            # New pharmacy — insert
            if not args.dry_run:
                cur.execute("""
                    INSERT INTO pharmacies (name, address, latitude, longitude, source, date_scraped, suburb, state, postcode, opening_hours)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    sp.get("name", ""),
                    sp.get("address", ""),
                    lat,
                    lon,
                    "findapharmacy.com.au",
                    now,
                    sp.get("suburb", ""),
                    sp.get("state", ""),
                    postcode,
                    sp.get("opening_hours", ""),
                ))
            stats["added"] += 1

    # Step 5: Flag removed pharmacies
    print("\n5. Checking for removed pharmacies...")
    fap_entries = source_index.get("findapharmacy.com.au", [])
    removed_count = 0
    removed_list = []
    for entry in fap_entries:
        if entry["id"] not in matched_db_ids:
            removed_count += 1
            removed_list.append({
                "id": entry["id"],
                "name": entry.get("name", ""),
                "suburb": entry.get("suburb", ""),
                "state": entry.get("state", ""),
            })

    print(f"  Pharmacies from findapharmacy.com.au no longer in index: {removed_count}")
    if removed_count > 0 and removed_count <= 20:
        for r in removed_list:
            print(f"    - {r['name']} ({r['suburb']}, {r['state']})")
    elif removed_count > 20:
        for r in removed_list[:10]:
            print(f"    - {r['name']} ({r['suburb']}, {r['state']})")
        print(f"    ... and {removed_count - 10} more")

    # Don't delete removed pharmacies — just note them
    # They may have closed or been renamed

    # Commit
    if not args.dry_run:
        conn.commit()

    cur.execute("SELECT COUNT(*) FROM pharmacies")
    total_after = cur.fetchone()[0]

    # State distribution after update
    cur.execute("SELECT state, COUNT(*) FROM pharmacies GROUP BY state ORDER BY COUNT(*) DESC")
    state_dist = cur.fetchall()

    conn.close()

    # Print summary
    print("\n" + "=" * 60)
    print("UPDATE SUMMARY")
    print("=" * 60)
    print(f"Before:    {total_before} pharmacies")
    print(f"After:     {total_after} pharmacies")
    print(f"")
    print(f"Added:     {stats['added']}")
    print(f"Updated:   {stats['updated']}")
    print(f"Unchanged: {stats['unchanged']}")
    print(f"No longer in findapharmacy index: {removed_count}")
    print(f"")
    print(f"Matched by name+postcode: {stats['matched_by_name']}")
    print(f"Matched by coordinates:   {stats['matched_by_coords']}")
    print(f"")
    print("By state (after update):")
    for state, count in state_dist:
        print(f"  {state or 'UNKNOWN'}: {count}")

    # Save summary
    summary = {
        "date": datetime.now().isoformat(),
        "json_source": str(json_path),
        "dry_run": args.dry_run,
        "before": total_before,
        "after": total_after,
        "added": stats["added"],
        "updated": stats["updated"],
        "unchanged": stats["unchanged"],
        "removed_from_index": removed_count,
        "removed_list": removed_list[:50],
    }
    summary_path = Path(__file__).parent.parent / "output" / f"update_summary_{datetime.now().strftime('%Y-%m-%d')}.json"
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"\nSummary saved to: {summary_path}")


if __name__ == "__main__":
    main()
