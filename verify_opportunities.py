"""
Opportunity Verification via Overpass API (OpenStreetMap)

For each opportunity zone, queries Overpass API for pharmacies within a
tight radius to find pharmacies missing from our database.  If an unknown
pharmacy is found very close to an opportunity, it's flagged as a likely
false positive.

The verifier:
  1. Reads opportunity CSVs from output/
  2. For each opportunity, queries Overpass for pharmacies nearby
  3. Cross-references against our pharmacy DB
  4. If a NEW pharmacy is found near the opportunity, adds it to the DB
     and flags the opportunity as a likely false positive
  5. Outputs verified_opportunities_<STATE>.csv with a new "Verified" column

Usage:
    python verify_opportunities.py --state TAS
    python verify_opportunities.py --all
    python verify_opportunities.py --state TAS --top 20
"""

import argparse
import csv
import json
import math
import os
import sys
import time
import urllib.parse
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.database import Database
from utils.distance import haversine_distance


# -- Configuration -------------------------------------------------

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
OVERPASS_MIRRORS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://maps.mail.ru/osm/tools/overpass/api/interpreter",
]

# Search radius around each opportunity (metres) — base value
# Adjusted per-rule in get_search_radius()
SEARCH_RADIUS_M = 1500

# If a pharmacy not in our DB is within this distance of an opportunity,
# flag it as a likely false positive (fallback for unknown rules)
FALSE_POSITIVE_RADIUS_KM = 0.5

# Rate limiting
REQUEST_DELAY_S = 1.5

# Batch size for Overpass queries (combine multiple locations per query)
# Keep small — each location generates 6 union clauses
BATCH_SIZE = 10

CACHE_DIR = Path("cache/overpass_verify")


# -- Overpass queries ----------------------------------------------

def build_batch_query(locations: List[Tuple[float, float]], radius_m: int = SEARCH_RADIUS_M) -> str:
    """Build an Overpass query that searches for pharmacies around multiple locations."""
    unions = []
    for lat, lon in locations:
        unions.append(f'node["amenity"="pharmacy"](around:{radius_m},{lat},{lon});')
        unions.append(f'way["amenity"="pharmacy"](around:{radius_m},{lat},{lon});')
        # Also check healthcare=pharmacy and shop=chemist
        unions.append(f'node["healthcare"="pharmacy"](around:{radius_m},{lat},{lon});')
        unions.append(f'way["healthcare"="pharmacy"](around:{radius_m},{lat},{lon});')
        unions.append(f'node["shop"="chemist"](around:{radius_m},{lat},{lon});')
        unions.append(f'way["shop"="chemist"](around:{radius_m},{lat},{lon});')
    
    union_str = "\n".join(unions)
    return f"""[out:json][timeout:60];
(
{union_str}
);
out center;"""


def query_overpass(query: str, cache_key: str = None) -> Optional[dict]:
    """Execute an Overpass API query with caching and fallback mirrors."""
    import urllib.request

    # Check cache first
    if cache_key:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)
        cache_file = CACHE_DIR / f"{cache_key}.json"
        if cache_file.exists():
            age_days = (time.time() - cache_file.stat().st_mtime) / 86400
            if age_days < 7:  # 7-day cache
                try:
                    return json.loads(cache_file.read_text(encoding='utf-8'))
                except Exception:
                    pass

    # Try each mirror with timeout
    for mirror_url in OVERPASS_MIRRORS:
        try:
            encoded_query = urllib.parse.urlencode({'data': query}).encode('utf-8')
            req = urllib.request.Request(
                mirror_url,
                data=encoded_query,
                headers={
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'User-Agent': 'PharmacyFinder/1.0 (opportunity-verification)',
                },
            )
            with urllib.request.urlopen(req, timeout=45) as resp:
                data = json.loads(resp.read().decode('utf-8'))
                # Cache successful response
                if cache_key:
                    cache_file = CACHE_DIR / f"{cache_key}.json"
                    cache_file.write_text(json.dumps(data), encoding='utf-8')
                return data
        except Exception as e:
            err_str = str(e)
            if len(err_str) > 80:
                err_str = err_str[:80] + '...'
            print(f"    [WARN] Overpass mirror {mirror_url.split('/')[2]} failed: {err_str}")
            continue

    # Last resort: check if we have ANY cached version (even expired)
    if cache_key:
        cache_file = CACHE_DIR / f"{cache_key}.json"
        if cache_file.exists():
            try:
                print(f"    [INFO] Using expired cache for {cache_key}")
                return json.loads(cache_file.read_text(encoding='utf-8'))
            except Exception:
                pass

    return None


def extract_pharmacies_from_overpass(data: dict) -> List[Dict]:
    """Extract pharmacy records from Overpass response."""
    pharmacies = []
    seen = set()

    for elem in data.get('elements', []):
        tags = elem.get('tags', {})
        
        # Get coordinates (nodes have lat/lon directly, ways have center)
        if elem['type'] == 'node':
            lat = elem.get('lat')
            lon = elem.get('lon')
        elif elem['type'] == 'way':
            center = elem.get('center', {})
            lat = center.get('lat') or elem.get('lat')
            lon = center.get('lon') or elem.get('lon')
        else:
            continue

        if lat is None or lon is None:
            continue

        # De-duplicate by OSM ID
        osm_id = f"{elem['type']}_{elem['id']}"
        if osm_id in seen:
            continue
        seen.add(osm_id)

        name = (tags.get('name') or tags.get('brand') or
                tags.get('operator') or 'Unknown Pharmacy')
        
        pharmacies.append({
            'osm_id': osm_id,
            'name': name,
            'latitude': lat,
            'longitude': lon,
            'address': _build_address(tags),
            'suburb': tags.get('addr:suburb', ''),
            'state': tags.get('addr:state', ''),
            'postcode': tags.get('addr:postcode', ''),
            'source': 'OpenStreetMap-verify',
        })

    return pharmacies


def _build_address(tags: dict) -> str:
    """Build an address string from OSM tags."""
    parts = []
    if tags.get('addr:housenumber'):
        parts.append(tags['addr:housenumber'])
    if tags.get('addr:street'):
        parts.append(tags['addr:street'])
    if tags.get('addr:suburb'):
        parts.append(tags['addr:suburb'])
    if tags.get('addr:state'):
        parts.append(tags['addr:state'])
    if tags.get('addr:postcode'):
        parts.append(tags['addr:postcode'])
    return ', '.join(parts) if parts else ''


# -- Core verification logic ---------------------------------------

def load_opportunities(state: str, top_n: int = None) -> List[Dict]:
    """Load opportunity zones from CSV."""
    csv_path = f"output/opportunity_zones_{state}.csv"
    if not os.path.exists(csv_path):
        print(f"  [ERROR] No CSV found at {csv_path}")
        return []

    opps = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                row['Latitude'] = float(row['Latitude'])
                row['Longitude'] = float(row['Longitude'])
                row['Nearest Pharmacy (km)'] = float(row.get('Nearest Pharmacy (km)', 0))
            except (ValueError, TypeError):
                continue
            opps.append(row)

    # Sort by confidence desc, then nearest pharmacy desc (far = more interesting)
    opps.sort(key=lambda o: (
        -_parse_confidence(o.get('Confidence', '0%')),
        -o.get('Nearest Pharmacy (km)', 0),
    ))

    if top_n:
        opps = opps[:top_n]

    return opps


def _parse_confidence(s: str) -> float:
    """Parse '85%' -> 0.85."""
    try:
        return float(s.replace('%', '')) / 100
    except (ValueError, TypeError):
        return 0.0


def find_matching_db_pharmacy(osm_pharm: Dict, db_pharmacies: List[Dict],
                              threshold_km: float = 0.1) -> Optional[Dict]:
    """Check if an OSM pharmacy matches one in our DB (within threshold)."""
    for db_p in db_pharmacies:
        dist = haversine_distance(
            osm_pharm['latitude'], osm_pharm['longitude'],
            db_p['latitude'], db_p['longitude'],
        )
        if dist <= threshold_km:
            # Also check name similarity for extra confidence
            osm_name = (osm_pharm.get('name') or '').lower()
            db_name = (db_p.get('name') or '').lower()
            if dist <= 0.05:  # within 50m is almost certainly the same
                return db_p
            # Within 100m + partial name match
            if any(w in db_name for w in osm_name.split() if len(w) > 3):
                return db_p
            if any(w in osm_name for w in db_name.split() if len(w) > 3):
                return db_p
            # Within 50m even without name match
            if dist <= 0.05:
                return db_p
    return None


def verify_state(state: str, db: Database, top_n: int = None, verbose: bool = True):
    """
    Verify opportunities for a given state.
    
    Returns: (verified_opps, new_pharmacies_found, false_positives_flagged)
    """
    print(f"\n{'='*60}")
    print(f"  VERIFYING OPPORTUNITIES — {state}")
    print(f"{'='*60}")

    opps = load_opportunities(state, top_n=top_n)
    if not opps:
        return [], 0, 0

    print(f"  Loaded {len(opps)} opportunities from CSV")

    # Load all pharmacies from DB
    db_pharmacies = db.get_all_pharmacies()
    print(f"  {len(db_pharmacies)} pharmacies in database")

    # Batch opportunities for Overpass queries
    batches = []
    for i in range(0, len(opps), BATCH_SIZE):
        batches.append(opps[i:i + BATCH_SIZE])

    print(f"  Will query Overpass in {len(batches)} batches of up to {BATCH_SIZE}")

    new_pharmacies_found = 0
    false_positives = 0
    all_new_pharmacies = []  # Track all newly discovered pharmacies

    for batch_idx, batch in enumerate(batches):
        locations = [(o['Latitude'], o['Longitude']) for o in batch]
        # Deterministic cache key from coordinates
        import hashlib
        coord_str = "|".join(f"{lat:.6f},{lon:.6f}" for lat, lon in locations)
        cache_hash = hashlib.md5(coord_str.encode()).hexdigest()[:10]
        cache_key = f"{state}_verify_{cache_hash}"

        if verbose:
            print(f"\n  Batch {batch_idx + 1}/{len(batches)} ({len(batch)} locations)...")

        query = build_batch_query(locations)
        data = query_overpass(query, cache_key=cache_key)

        if data is None:
            print(f"    [ERROR] All Overpass mirrors failed for batch {batch_idx + 1}")
            for opp in batch:
                opp['Verification'] = 'UNVERIFIED'
                opp['Verification Notes'] = 'Overpass API unavailable'
            continue

        osm_pharmacies = extract_pharmacies_from_overpass(data)
        if verbose:
            print(f"    Found {len(osm_pharmacies)} pharmacies in OSM near these locations")

        # For each opportunity in the batch, check if any OSM pharmacy
        # is (a) near the opportunity and (b) NOT in our database
        for opp in batch:
            opp_lat = opp['Latitude']
            opp_lon = opp['Longitude']
            
            # Find OSM pharmacies near this specific opportunity
            nearby_osm = []
            for osm_p in osm_pharmacies:
                dist = haversine_distance(opp_lat, opp_lon,
                                          osm_p['latitude'], osm_p['longitude'])
                if dist <= SEARCH_RADIUS_M / 1000:  # within search radius
                    nearby_osm.append((osm_p, dist))

            nearby_osm.sort(key=lambda x: x[1])

            if not nearby_osm:
                opp['Verification'] = 'VERIFIED'
                opp['Verification Notes'] = 'No pharmacies found in OSM within search radius'
                continue

            # Check each nearby OSM pharmacy against our DB
            unknown_near = []
            known_near = []

            for osm_p, dist in nearby_osm:
                match = find_matching_db_pharmacy(osm_p, db_pharmacies)
                if match:
                    known_near.append((osm_p, dist, match))
                else:
                    unknown_near.append((osm_p, dist))

            if not unknown_near:
                opp['Verification'] = 'VERIFIED'
                known_names = [f"{k[0]['name']} ({k[1]:.0f}m)" for k in known_near[:3]]
                opp['Verification Notes'] = f'All {len(known_near)} nearby OSM pharmacies already in DB: {"; ".join(known_names)}'
                continue

            # We found pharmacies NOT in our database!
            # Check the rule context — are any of the unknown pharmacies
            # close enough to invalidate this opportunity?
            rules = opp.get('Qualifying Rules', '')
            is_false_positive = False
            fp_reason = ''

            for osm_p, dist in unknown_near:
                # Add to database
                try:
                    db.insert_pharmacy({
                        'name': osm_p['name'],
                        'address': osm_p.get('address', ''),
                        'latitude': osm_p['latitude'],
                        'longitude': osm_p['longitude'],
                        'source': 'OpenStreetMap-verify',
                        'suburb': osm_p.get('suburb', ''),
                        'state': osm_p.get('state', state),
                        'postcode': osm_p.get('postcode', ''),
                    })
                    new_pharmacies_found += 1
                    all_new_pharmacies.append(osm_p)
                    # Also add to our in-memory list for subsequent checks
                    db_pharmacies.append({
                        'name': osm_p['name'],
                        'latitude': osm_p['latitude'],
                        'longitude': osm_p['longitude'],
                        'address': osm_p.get('address', ''),
                    })
                    if verbose:
                        print(f"    [NEW] {osm_p['name']} at ({osm_p['latitude']:.4f}, {osm_p['longitude']:.4f}) — {dist*1000:.0f}m from opportunity")
                except Exception:
                    pass  # Already exists (duplicate)

                # Check if this makes the opportunity a false positive
                # Only use RULE-SPECIFIC thresholds — each rule has a
                # different distance requirement
                if 'Item 132' in rules and dist <= 0.2:
                    is_false_positive = True
                    fp_reason = f"Pharmacy '{osm_p['name']}' found {dist*1000:.0f}m away (Item 132 requires >200m)"
                elif 'Item 133' in rules and dist <= 0.1:
                    is_false_positive = True
                    fp_reason = f"Pharmacy '{osm_p['name']}' found {dist*1000:.0f}m away (Item 133 requires >100m)"
                elif 'Item 134' in rules and 'Item 134A' not in rules and dist <= 0.2:
                    is_false_positive = True
                    fp_reason = f"Pharmacy '{osm_p['name']}' found {dist*1000:.0f}m away (Item 134 requires >200m)"
                elif 'Item 135' in rules and dist <= 0.3:
                    is_false_positive = True
                    fp_reason = f"Pharmacy '{osm_p['name']}' found {dist*1000:.0f}m away (Item 135 requires >300m)"
                elif 'Item 136' in rules and dist <= 0.3:
                    is_false_positive = True
                    fp_reason = f"Pharmacy '{osm_p['name']}' found {dist*1000:.0f}m away (Item 136 requires >300m)"
                elif 'Item 130' in rules and dist <= 1.5:
                    is_false_positive = True
                    fp_reason = f"Pharmacy '{osm_p['name']}' found {dist:.2f}km away (Item 130 requires >1.5km)"
                elif 'Item 131' in rules and dist <= 10.0:
                    is_false_positive = True
                    fp_reason = f"Pharmacy '{osm_p['name']}' found {dist:.2f}km away (Item 131 requires >10km)"
                elif 'Item 134A' in rules and dist <= 90.0:
                    is_false_positive = True
                    fp_reason = f"Pharmacy '{osm_p['name']}' found {dist:.1f}km away (Item 134A requires >90km)"

            if is_false_positive:
                opp['Verification'] = 'FALSE POSITIVE'
                opp['Verification Notes'] = fp_reason
                false_positives += 1
                if verbose:
                    print(f"    [FP] {opp.get('POI Name', opp.get('Address', '?'))}: {fp_reason}")
            else:
                unknown_names = [f"{u[0]['name']} ({u[1]*1000:.0f}m)" for u in unknown_near[:3]]
                opp['Verification'] = 'NEEDS REVIEW'
                opp['Verification Notes'] = f'Found {len(unknown_near)} new pharmacy(ies) nearby but outside invalidation radius: {"; ".join(unknown_names)}'

        # Rate limit between batches
        if batch_idx < len(batches) - 1:
            time.sleep(REQUEST_DELAY_S)

    # Write verified CSV
    output_path = f"output/verified_opportunities_{state}.csv"
    _write_verified_csv(opps, output_path)

    # Summary
    verified = sum(1 for o in opps if o.get('Verification') == 'VERIFIED')
    needs_review = sum(1 for o in opps if o.get('Verification') == 'NEEDS REVIEW')
    unverified = sum(1 for o in opps if o.get('Verification') == 'UNVERIFIED')

    print(f"\n  {'='*50}")
    print(f"  VERIFICATION RESULTS — {state}")
    print(f"  {'='*50}")
    print(f"  Total opportunities:     {len(opps)}")
    print(f"  [OK] Verified (genuine):    {verified}")
    print(f"  [FP] False positives:       {false_positives}")
    print(f"  [??] Needs review:          {needs_review}")
    print(f"  [--] Unverified (API fail): {unverified}")
    print(f"  [++] New pharmacies found:  {new_pharmacies_found}")
    print(f"  Output: {output_path}")

    return opps, new_pharmacies_found, false_positives


def _write_verified_csv(opps: List[Dict], output_path: str):
    """Write verified opportunities to CSV."""
    if not opps:
        return

    # Original columns + verification columns
    fieldnames = [
        'Latitude', 'Longitude', 'Address', 'Qualifying Rules', 'Evidence',
        'Confidence', 'Nearest Pharmacy (km)', 'Nearest Pharmacy Name',
        'POI Name', 'POI Type', 'Region', 'Date Scanned',
        'Verification', 'Verification Notes',
    ]

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        for opp in opps:
            writer.writerow(opp)


# -- Google Maps browser verification (for high-priority) ----------

def verify_with_google_maps(lat: float, lon: float, radius_m: int = 1000) -> List[Dict]:
    """
    Placeholder for browser-based Google Maps verification.
    This can be implemented to use the browser tool for cases where
    Overpass data might be incomplete (Google Maps has more POI data).
    
    For now, Overpass provides sufficient coverage for Australian pharmacies
    since pharmacy data in OSM Australia is comprehensive.
    """
    # TODO: Implement browser-based Google Maps search if needed
    return []


# -- Main ----------------------------------------------------------

def main():
    # Force unbuffered output
    sys.stdout.reconfigure(line_buffering=True)

    parser = argparse.ArgumentParser(
        description='Verify opportunity zones against real-world pharmacy data',
    )
    parser.add_argument('--state', type=str, help='State to verify (e.g., TAS)')
    parser.add_argument('--all', action='store_true', help='Verify all states')
    parser.add_argument('--top', type=int, default=None,
                        help='Only verify top N opportunities per state')
    parser.add_argument('--verbose', action='store_true', default=True)
    parser.add_argument('--quiet', action='store_true')

    args = parser.parse_args()
    verbose = not args.quiet

    if not args.state and not args.all:
        parser.print_help()
        print("\nExamples:")
        print("  python verify_opportunities.py --state TAS")
        print("  python verify_opportunities.py --all")
        print("  python verify_opportunities.py --state TAS --top 20")
        return

    db = Database()
    db.connect()

    states = []
    if args.all:
        states = ['TAS', 'ACT', 'NT', 'SA', 'WA', 'QLD', 'NSW', 'VIC']
    elif args.state:
        states = [args.state.upper()]

    total_new = 0
    total_fp = 0
    all_results = {}

    for state in states:
        opps, new_found, fp_count = verify_state(
            state, db, top_n=args.top, verbose=verbose,
        )
        total_new += new_found
        total_fp += fp_count
        all_results[state] = {
            'total': len(opps),
            'verified': sum(1 for o in opps if o.get('Verification') == 'VERIFIED'),
            'false_positive': fp_count,
            'needs_review': sum(1 for o in opps if o.get('Verification') == 'NEEDS REVIEW'),
            'new_pharmacies': new_found,
        }

    if len(states) > 1:
        print(f"\n\n{'='*60}")
        print(f"  OVERALL VERIFICATION SUMMARY")
        print(f"{'='*60}")
        print(f"  {'State':<6} {'Total':>6} {'Verified':>10} {'False Pos':>10} {'Review':>8} {'New Pharm':>10}")
        print(f"  {'-'*52}")
        for state, r in all_results.items():
            print(f"  {state:<6} {r['total']:>6} {r['verified']:>10} {r['false_positive']:>10} {r['needs_review']:>8} {r['new_pharmacies']:>10}")
        print(f"  {'-'*52}")
        print(f"  {'TOTAL':<6} {sum(r['total'] for r in all_results.values()):>6} "
              f"{sum(r['verified'] for r in all_results.values()):>10} "
              f"{total_fp:>10} "
              f"{sum(r['needs_review'] for r in all_results.values()):>8} "
              f"{total_new:>10}")

    db.close()


if __name__ == '__main__':
    main()
