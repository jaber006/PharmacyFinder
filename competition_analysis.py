#!/usr/bin/env python3
"""
Competition Analysis for PharmacyFinder opportunities.

For each opportunity zone, queries the pharmacy database to find nearby
pharmacies within 5km, 10km, and 15km. Classifies each as chain or
independent and calculates a competition score.

Lower competition score + higher population = best opportunity.
"""

import csv
import sqlite3
import math
import os
import argparse

# Chain pharmacy brands to detect
CHAIN_BRANDS = [
    'chemist warehouse', 'priceline', 'terrywhite', 'terry white',
    'amcal', 'blooms', 'guardian', 'discount drug stores',
    'good price', 'chemmart', 'pharmacy warehouse', 'star pharmacy',
    'national pharmacies', 'capital chemist', 'friendlies',
    'alive pharmacy', 'chemplus', 'pharmacist advice',
    'cincotta', 'wholelife', 'wizard pharmacy'
]

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'pharmacy_finder.db')
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')


def haversine_km(lat1, lon1, lat2, lon2):
    """Calculate the great-circle distance between two points on Earth."""
    R = 6371.0
    lat1_r, lon1_r = math.radians(lat1), math.radians(lon1)
    lat2_r, lon2_r = math.radians(lat2), math.radians(lon2)
    dlat = lat2_r - lat1_r
    dlon = lon2_r - lon1_r
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1_r) * math.cos(lat2_r) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def is_chain(name):
    """Check if a pharmacy name matches a known chain brand."""
    name_lower = name.lower() if name else ''
    return any(brand in name_lower for brand in CHAIN_BRANDS)


def get_all_pharmacies(db_path):
    """Load all pharmacies from the database."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name, latitude, longitude FROM pharmacies WHERE latitude IS NOT NULL AND longitude IS NOT NULL")
    pharmacies = cursor.fetchall()
    conn.close()
    return pharmacies


def analyze_competition(opp_lat, opp_lon, pharmacies):
    """Analyze competition around an opportunity zone."""
    nearby_5 = []
    nearby_10 = []
    nearby_15 = []

    for name, plat, plon in pharmacies:
        dist = haversine_km(opp_lat, opp_lon, plat, plon)
        entry = {'name': name, 'distance': dist, 'is_chain': is_chain(name)}

        if dist <= 15:
            nearby_15.append(entry)
            if dist <= 10:
                nearby_10.append(entry)
                if dist <= 5:
                    nearby_5.append(entry)

    chain_count = sum(1 for p in nearby_15 if p['is_chain'])
    independent_count = sum(1 for p in nearby_15 if not p['is_chain'])

    # Competition score: weighted count (closer pharmacies matter more)
    # Lower = less competition = better opportunity
    score = 0
    for p in nearby_5:
        score += 3.0  # Very close competition counts triple
    for p in nearby_10:
        if p['distance'] > 5:
            score += 2.0  # Medium distance counts double
    for p in nearby_15:
        if p['distance'] > 10:
            score += 1.0  # Farther competition counts once

    # Chain presence penalty (chains are tougher competition)
    chain_penalty = chain_count * 0.5
    score += chain_penalty

    return {
        'pharmacy_count_5km': len(nearby_5),
        'pharmacy_count_10km': len(nearby_10),
        'pharmacy_count_15km': len(nearby_15),
        'chain_count': chain_count,
        'independent_count': independent_count,
        'nearest_competitors': sorted(nearby_5, key=lambda x: x['distance'])[:3],
        'competition_score': round(score, 1)
    }


def process_state(state, pharmacies):
    """Process a single state's population_ranked CSV and add competition data."""
    input_file = os.path.join(OUTPUT_DIR, f'population_ranked_{state}.csv')
    if not os.path.exists(input_file):
        print(f"  Skipping {state}: no population_ranked CSV found")
        return 0

    # Read existing data
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        original_fieldnames = list(reader.fieldnames)
        rows = list(reader)

    if not rows:
        print(f"  Skipping {state}: no data rows")
        return 0

    # New columns to add
    new_cols = [
        'Pharmacy Count 5km', 'Pharmacy Count 10km', 'Pharmacy Count 15km',
        'Chain Count', 'Independent Count', 'Competition Score',
        'Nearest Competitors', 'Composite Score'
    ]

    # Remove existing competition columns if re-running
    fieldnames = [f for f in original_fieldnames if f not in new_cols]
    fieldnames.extend(new_cols)

    processed_rows = []
    for i, row in enumerate(rows):
        lat = float(row['Latitude'])
        lon = float(row['Longitude'])

        comp = analyze_competition(lat, lon, pharmacies)

        row['Pharmacy Count 5km'] = comp['pharmacy_count_5km']
        row['Pharmacy Count 10km'] = comp['pharmacy_count_10km']
        row['Pharmacy Count 15km'] = comp['pharmacy_count_15km']
        row['Chain Count'] = comp['chain_count']
        row['Independent Count'] = comp['independent_count']
        row['Competition Score'] = comp['competition_score']

        # Format nearest competitors
        competitors_str = '; '.join(
            f"{c['name']} ({c['distance']:.1f}km, {'chain' if c['is_chain'] else 'ind'})"
            for c in comp['nearest_competitors']
        )
        row['Nearest Competitors'] = competitors_str

        # Composite Score: population / (competition + 1) * distance_factor
        pop = float(row.get('Pop 10km', 0) or 0)
        dist = float(row.get('Nearest Pharmacy (km)', 0) or 0)
        comp_score = comp['competition_score']
        # Higher is better: lots of people, far from pharmacy, low competition
        composite = (pop * max(dist, 0.1)) / (comp_score + 1)
        row['Composite Score'] = round(composite, 1)

        processed_rows.append(row)

        if (i + 1) % 100 == 0:
            print(f"    Processed {i + 1}/{len(rows)} opportunities...")

    # Sort by composite score (highest first)
    processed_rows.sort(key=lambda r: float(r.get('Composite Score', 0)), reverse=True)

    # Write back
    output_file = os.path.join(OUTPUT_DIR, f'population_ranked_{state}.csv')
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(processed_rows)

    print(f"  {state}: Analyzed {len(processed_rows)} opportunities")
    # Show top 3
    for j, row in enumerate(processed_rows[:3]):
        print(f"    #{j+1}: {row['POI Name']} - Pop 10km: {row.get('Pop 10km', 'N/A')}, "
              f"Competition: {row['Competition Score']}, Composite: {row['Composite Score']}")

    return len(processed_rows)


def main():
    parser = argparse.ArgumentParser(description='Analyze competition for pharmacy opportunities')
    parser.add_argument('--state', type=str, help='State to analyze (e.g., TAS). Default: all')
    args = parser.parse_args()

    print("=" * 60)
    print("PHARMACY COMPETITION ANALYSIS")
    print("=" * 60)

    print(f"\nLoading pharmacies from database...")
    pharmacies = get_all_pharmacies(DB_PATH)
    print(f"  Loaded {len(pharmacies)} pharmacies")

    states = [args.state.upper()] if args.state else ['ACT', 'NSW', 'NT', 'QLD', 'SA', 'TAS', 'VIC', 'WA']
    total = 0

    for state in states:
        print(f"\nProcessing {state}...")
        total += process_state(state, pharmacies)

    print(f"\n{'=' * 60}")
    print(f"Total opportunities analyzed: {total}")
    print(f"{'=' * 60}")


if __name__ == '__main__':
    main()
