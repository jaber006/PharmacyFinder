"""
Pharmacy Deduplication Tool for PharmacyFinder DB.

Finds duplicate pharmacies using name similarity + geographic proximity (<200m).
Groups duplicates, picks the best record per group, and outputs recommended merges.

DRY RUN by default. Use --apply to actually merge (delete inferior duplicates).

Usage:
    py -3.12 scripts/deduplicate_pharmacies.py              # dry run
    py -3.12 scripts/deduplicate_pharmacies.py --apply       # apply merges
    py -3.12 scripts/deduplicate_pharmacies.py --threshold 150  # custom distance (metres)
"""

import sqlite3
import json
import math
import os
import sys
import argparse
from datetime import datetime
from difflib import SequenceMatcher

DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'pharmacy_finder.db')
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '..', 'output')

DEFAULT_DISTANCE_THRESHOLD = 200  # metres
NAME_SIMILARITY_THRESHOLD = 0.65  # 0-1, lower = more aggressive matching


def haversine_m(lat1, lon1, lat2, lon2):
    """Distance in metres between two lat/lon points."""
    R = 6_371_000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def normalise_name(name):
    """Normalise pharmacy name for comparison."""
    if not name:
        return ''
    n = name.upper().strip()
    # Remove common suffixes/prefixes that don't distinguish
    for remove in ['PTY LTD', 'PTY. LTD.', 'P/L', '(CHEMIST)', '(PHARMACY)']:
        n = n.replace(remove, '')
    return n.strip()


def name_similarity(name1, name2):
    """Calculate similarity between two pharmacy names (0-1)."""
    n1 = normalise_name(name1)
    n2 = normalise_name(name2)
    if not n1 or not n2:
        return 0.0
    # Exact match after normalisation
    if n1 == n2:
        return 1.0
    return SequenceMatcher(None, n1, n2).ratio()


def record_completeness(row):
    """Score how complete a pharmacy record is (higher = better)."""
    score = 0
    _id, name, address, lat, lon, source, date_scraped, suburb, state, postcode, opening_hours, coord_verified = row

    if name:
        score += 1
    if address:
        score += 1
    if suburb:
        score += 1
    if state:
        score += 1
    if postcode:
        score += 1
    if opening_hours:
        score += 2  # extra weight for opening hours
    if coord_verified:
        score += 3  # heavily prefer verified coords
    if source and 'pbs' in source.lower():
        score += 3  # PBS-verified is gold standard
    if source and 'osm' not in (source or '').lower():
        score += 1  # prefer non-OSM sources (usually more complete)
    if date_scraped:
        score += 1

    return score


def find_duplicates(conn, distance_threshold):
    """Find pharmacy duplicates using name similarity + proximity."""
    c = conn.cursor()
    c.execute("""SELECT id, name, address, latitude, longitude, source,
                        date_scraped, suburb, state, postcode, opening_hours, coord_verified
                 FROM pharmacies ORDER BY latitude""")
    rows = c.fetchall()

    print(f"  Scanning {len(rows)} pharmacies for duplicates...")

    # Build adjacency list using spatial + name filtering
    lat_threshold = distance_threshold / 111_000  # rough degrees for early exit

    # Union-Find for grouping
    parent = {r[0]: r[0] for r in rows}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    pair_details = []

    for i in range(len(rows)):
        for j in range(i + 1, len(rows)):
            if rows[j][3] - rows[i][3] > lat_threshold:
                break

            dist = haversine_m(rows[i][3], rows[i][4], rows[j][3], rows[j][4])
            if dist > distance_threshold:
                continue

            sim = name_similarity(rows[i][1], rows[j][1])
            if sim < NAME_SIMILARITY_THRESHOLD:
                continue

            union(rows[i][0], rows[j][0])
            pair_details.append({
                'id_a': rows[i][0],
                'id_b': rows[j][0],
                'name_a': rows[i][1],
                'name_b': rows[j][1],
                'distance_m': round(dist, 1),
                'name_similarity': round(sim, 3),
            })

    # Build groups from union-find
    row_map = {r[0]: r for r in rows}
    groups_map = {}
    for r in rows:
        root = find(r[0])
        if root not in groups_map:
            groups_map[root] = []
        groups_map[root].append(r[0])

    # Filter to groups with 2+ members
    duplicate_groups = []
    for root, member_ids in groups_map.items():
        if len(member_ids) < 2:
            continue

        members = []
        for mid in member_ids:
            row = row_map[mid]
            members.append({
                'id': row[0],
                'name': row[1],
                'address': row[2],
                'lat': row[3],
                'lon': row[4],
                'source': row[5],
                'suburb': row[7],
                'state': row[8],
                'postcode': row[9],
                'coord_verified': bool(row[11]),
                'completeness_score': record_completeness(row),
            })

        # Pick best record (highest completeness score)
        members.sort(key=lambda m: m['completeness_score'], reverse=True)
        best = members[0]
        duplicates_to_remove = members[1:]

        duplicate_groups.append({
            'group_size': len(members),
            'keep': best,
            'remove': duplicates_to_remove,
            'members': members,
        })

    duplicate_groups.sort(key=lambda g: g['group_size'], reverse=True)

    return duplicate_groups, pair_details


def apply_merges(conn, groups):
    """Actually delete duplicate pharmacies (keeping the best per group)."""
    c = conn.cursor()
    removed = 0

    for group in groups:
        for dup in group['remove']:
            c.execute("DELETE FROM pharmacies WHERE id = ?", (dup['id'],))
            removed += 1

    conn.commit()
    return removed


def main():
    parser = argparse.ArgumentParser(description='Deduplicate pharmacies in PharmacyFinder DB')
    parser.add_argument('--apply', action='store_true', help='Actually apply merges (default: dry run)')
    parser.add_argument('--threshold', type=int, default=DEFAULT_DISTANCE_THRESHOLD,
                        help=f'Distance threshold in metres (default: {DEFAULT_DISTANCE_THRESHOLD})')
    args = parser.parse_args()

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)

    print(f"Pharmacy Deduplication {'(APPLY MODE)' if args.apply else '(DRY RUN)'}")
    print(f"Distance threshold: {args.threshold}m")
    print(f"Name similarity threshold: {NAME_SIMILARITY_THRESHOLD}")

    groups, pairs = find_duplicates(conn, args.threshold)

    total_dupes = sum(len(g['remove']) for g in groups)

    print(f"\n  Found {len(groups)} duplicate groups ({total_dupes} records to remove)")

    # Build output
    output = {
        'run_mode': 'APPLY' if args.apply else 'DRY_RUN',
        'distance_threshold_m': args.threshold,
        'name_similarity_threshold': NAME_SIMILARITY_THRESHOLD,
        'timestamp': datetime.now().isoformat(),
        'summary': {
            'duplicate_groups': len(groups),
            'records_to_remove': total_dupes,
            'records_to_keep': len(groups),
        },
        'groups': groups,
        'pair_details': pairs,
    }

    # Write output
    out_path = os.path.join(OUTPUT_DIR, 'pharmacy_duplicates.json')
    with open(out_path, 'w') as f:
        json.dump(output, f, indent=2, default=str)
    print(f"  Output: {out_path}")

    # Print sample groups
    for i, group in enumerate(groups[:5]):
        print(f"\n  Group {i + 1} ({group['group_size']} records):")
        print(f"    KEEP: [{group['keep']['id']}] {group['keep']['name']} "
              f"(score={group['keep']['completeness_score']}, source={group['keep']['source']})")
        for dup in group['remove']:
            print(f"    DROP: [{dup['id']}] {dup['name']} "
                  f"(score={dup['completeness_score']}, source={dup['source']})")

    if len(groups) > 5:
        print(f"\n  ... and {len(groups) - 5} more groups (see JSON output)")

    if args.apply:
        print(f"\n  APPLYING merges...")
        removed = apply_merges(conn, groups)
        print(f"  Removed {removed} duplicate records.")
    else:
        print(f"\n  DRY RUN — no changes made. Use --apply to merge.")

    conn.close()
    print("\nDone.")


if __name__ == '__main__':
    main()
