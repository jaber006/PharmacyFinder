"""
Population Data Overlay for Opportunity Zones

Estimates population near each opportunity zone to help prioritize.
A gap near 10,000 people is more valuable than one near 100.

Data sources:
1. OpenStreetMap place nodes (cities, towns, villages, suburbs with population tags)
2. Estimated population from nearby settlement type/size

Outputs:
- Adds population estimates to verified_opportunities CSVs
- Generates population_ranked_<STATE>.csv sorted by population x distance score

Usage:
    python population_overlay.py --state TAS
    python population_overlay.py --all
"""

import argparse
import csv
import json
import math
import os
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.database import Database
from utils.distance import haversine_distance
from utils.overpass_cache import cached_overpass_query
from utils.boundaries import STATE_BOUNDING_BOXES


# -- Configuration -------------------------------------------------

# How far to search for population data (km)
POPULATION_SEARCH_RADIUS_KM = 15

# Default population estimates by place type (conservative)
DEFAULT_POPULATION = {
    'city': 50000,
    'town': 5000,
    'village': 500,
    'hamlet': 100,
    'suburb': 8000,
    'neighbourhood': 3000,
    'isolated_dwelling': 5,
    'locality': 200,
}


# -- Population query via Overpass ---------------------------------

def get_population_data_for_state(state: str) -> List[Dict]:
    """
    Query Overpass for all populated places in a state with population data.
    Returns list of {name, lat, lon, population, place_type}.
    """
    from utils.boundaries import STATE_BOUNDING_BOXES
    import config

    state_name = config.AUSTRALIAN_STATES.get(state, state)
    
    query = f"""
    [out:json][timeout:120];
    area["name"="{state_name}"]["admin_level"="4"]->.state;
    (
      node["place"~"city|town|village|suburb|hamlet|neighbourhood|locality"](area.state);
    );
    out;
    """

    data = cached_overpass_query(
        query=query,
        cache_key=f"population_{state}",
    )

    if data is None:
        print(f"  [WARN] Could not fetch population data for {state}")
        return []

    places = []
    for elem in data.get('elements', []):
        tags = elem.get('tags', {})
        place_type = tags.get('place', '')
        
        # Get population from OSM tags
        pop = None
        for key in ['population', 'population:date', 'census:population']:
            val = tags.get(key if ':' not in key else key.split(':')[0] + ':' + key.split(':')[1])
            if val:
                try:
                    pop = int(val.replace(',', '').replace(' ', ''))
                    break
                except (ValueError, TypeError):
                    pass
        
        # Also check population tag directly
        if pop is None and 'population' in tags:
            try:
                pop = int(str(tags['population']).replace(',', '').replace(' ', ''))
            except (ValueError, TypeError):
                pass

        # Fall back to estimate by place type
        if pop is None:
            pop = DEFAULT_POPULATION.get(place_type, 200)
            estimated = True
        else:
            estimated = False

        places.append({
            'name': tags.get('name', 'Unknown'),
            'latitude': elem['lat'],
            'longitude': elem['lon'],
            'population': pop,
            'place_type': place_type,
            'estimated': estimated,
        })

    return places


def estimate_nearby_population(lat: float, lon: float, places: List[Dict],
                                radius_km: float = POPULATION_SEARCH_RADIUS_KM) -> Dict:
    """
    Estimate population near a point using OSM place data.
    
    Strategy:
    - Only use places with ACTUAL population data (from OSM tags)
    - For cities/towns: use distance-decay to estimate how much of the 
      population is within our radius
    - For suburbs with population: use as-is with spatial overlap estimate
    - Avoid double-counting by only using the largest settlement category
    
    Returns dict with population estimates and nearest town info.
    """
    nearest_town = None
    nearest_town_pop = 0
    nearest_town_dist = float('inf')
    
    # Separate places by type and whether they have real data
    cities_towns = []  # city/town with real population
    villages = []      # village/hamlet with real population
    suburbs = []       # suburb/neighbourhood with real population

    for place in places:
        dist = haversine_distance(lat, lon, place['latitude'], place['longitude'])
        if dist > radius_km:
            continue
        
        entry = {**place, 'dist': dist}
        
        if place['estimated']:
            # Skip estimated populations entirely for suburbs/hamlets
            # Only keep estimated villages (they have small, reasonable defaults)
            if place['place_type'] in ('village', 'hamlet') and dist < 5.0:
                villages.append(entry)
            continue
        
        if place['place_type'] in ('city', 'town'):
            cities_towns.append(entry)
        elif place['place_type'] in ('village', 'hamlet'):
            villages.append(entry)
        elif place['place_type'] in ('suburb', 'neighbourhood'):
            suburbs.append(entry)
        
        # Track nearest significant place (with real population data)
        if place['population'] >= 300 and dist < nearest_town_dist:
            nearest_town = place['name']
            nearest_town_pop = place['population']
            nearest_town_dist = dist

    # Calculate population at different radii
    pop_5km = 0
    pop_10km = 0
    pop_15km = 0

    # For cities/towns: use population with distance decay
    # A city with pop X at distance D contributes a fraction based on
    # how much of its area overlaps with our circle
    for ct in cities_towns:
        pop = ct['population']
        dist = ct['dist']
        
        # Estimate city radius from population
        # Use higher density for larger cities (they spread more)
        if pop > 500000:
            density = 2500  # major metro
        elif pop > 100000:
            density = 2000  # large city
        elif pop > 20000:
            density = 1500  # medium city
        else:
            density = 1000  # small town
        
        city_radius_km = math.sqrt(pop / (math.pi * density))
        city_radius_km = max(city_radius_km, 1.0)  # at least 1km
        city_radius_km = min(city_radius_km, 25.0)  # cap at 25km
        
        # For each search radius, estimate overlap fraction
        for radius, pop_bucket in [(5.0, 'pop_5km'), (10.0, 'pop_10km'), (15.0, 'pop_15km')]:
            if dist > radius + city_radius_km:
                continue  # no overlap possible
            
            if dist + city_radius_km <= radius:
                # City entirely within our radius
                fraction = 1.0
            elif dist <= radius:
                # Our point is within or very close to the city
                # Fraction = overlap area / city area
                overlap_radius = min(radius, city_radius_km)
                fraction = min(1.0, (overlap_radius ** 2) / (city_radius_km ** 2))
            else:
                # Partial overlap
                fraction = max(0.05, 1.0 - (dist - radius) / city_radius_km)
            
            contribution = int(pop * fraction)
            if pop_bucket == 'pop_5km':
                pop_5km += contribution
            elif pop_bucket == 'pop_10km':
                pop_10km += contribution
            else:
                pop_15km += contribution

    # For villages: simple inclusion (they're small enough to treat as points)
    for v in villages:
        pop = v['population']
        dist = v['dist']
        if dist <= 5.0:
            pop_5km += pop
        if dist <= 10.0:
            pop_10km += pop
        if dist <= 15.0:
            pop_15km += pop

    # For suburbs with real data: scale by proximity
    # Don't add suburbs if a city already covers the area (avoid double-counting)
    has_city_nearby = any(ct['dist'] < 5.0 for ct in cities_towns)
    if not has_city_nearby:
        for s in suburbs:
            pop = s['population']
            dist = s['dist']
            # Suburbs are ~1-2km radius, so only count if close
            if dist < 2.0:
                fraction = max(0.1, 1.0 - dist / 3.0)
                contribution = int(pop * fraction)
                if dist <= 5.0:
                    pop_5km += contribution
                if dist <= 10.0:
                    pop_10km += contribution
                if dist <= 15.0:
                    pop_15km += contribution

    return {
        'total_pop_5km': pop_5km,
        'total_pop_10km': pop_10km,
        'total_pop_15km': pop_15km,
        'nearest_town': nearest_town or 'Unknown',
        'nearest_town_pop': nearest_town_pop,
        'nearest_town_dist_km': round(nearest_town_dist, 2) if nearest_town_dist != float('inf') else -1,
    }


def calculate_opportunity_score(pop_data: Dict, nearest_pharmacy_km: float,
                                  confidence: float) -> float:
    """
    Calculate a composite opportunity score.
    
    Score = (population_10km * distance_factor * confidence)
    
    Higher score = more valuable opportunity.
    """
    pop_10km = pop_data['total_pop_10km']
    
    # Distance factor: opportunities far from pharmacies are more valuable
    if nearest_pharmacy_km <= 0:
        dist_factor = 0.5
    elif nearest_pharmacy_km < 0.5:
        dist_factor = 0.3
    elif nearest_pharmacy_km < 1.0:
        dist_factor = 0.5
    elif nearest_pharmacy_km < 2.0:
        dist_factor = 0.8
    elif nearest_pharmacy_km < 5.0:
        dist_factor = 1.0
    elif nearest_pharmacy_km < 10.0:
        dist_factor = 1.2
    else:
        dist_factor = 1.5

    score = pop_10km * dist_factor * confidence
    return round(score, 1)


# -- Process state -------------------------------------------------

def process_state(state: str, verbose: bool = True):
    """Add population data to verified/opportunity CSVs for a state."""
    print(f"\n{'='*60}")
    print(f"  POPULATION OVERLAY - {state}")
    print(f"{'='*60}")

    # Try verified first, fall back to original
    csv_path = f"output/verified_opportunities_{state}.csv"
    if not os.path.exists(csv_path):
        csv_path = f"output/opportunity_zones_{state}.csv"
    if not os.path.exists(csv_path):
        print(f"  [ERROR] No opportunity CSV found for {state}")
        return

    # Load opportunities
    opps = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                row['_lat'] = float(row['Latitude'])
                row['_lon'] = float(row['Longitude'])
                row['_nearest_km'] = float(row.get('Nearest Pharmacy (km)', 0))
                row['_confidence'] = float(row.get('Confidence', '0%').replace('%', '')) / 100
            except (ValueError, TypeError):
                continue
            opps.append(row)

    print(f"  Loaded {len(opps)} opportunities")

    # Fetch population data
    print(f"  Fetching population data from OSM...")
    places = get_population_data_for_state(state)
    print(f"  Found {len(places)} populated places")
    
    places_with_pop = [p for p in places if not p['estimated']]
    print(f"  {len(places_with_pop)} have actual population data, {len(places) - len(places_with_pop)} estimated")

    # Calculate population for each opportunity
    print(f"  Calculating population overlays...")
    for opp in opps:
        pop_data = estimate_nearby_population(opp['_lat'], opp['_lon'], places)
        opp['Pop 5km'] = pop_data['total_pop_5km']
        opp['Pop 10km'] = pop_data['total_pop_10km']
        opp['Pop 15km'] = pop_data['total_pop_15km']
        opp['Nearest Town'] = pop_data['nearest_town']
        opp['Nearest Town Pop'] = pop_data['nearest_town_pop']
        opp['Nearest Town Dist (km)'] = pop_data['nearest_town_dist_km']
        opp['Opportunity Score'] = calculate_opportunity_score(
            pop_data, opp['_nearest_km'], opp['_confidence']
        )

    # Sort by opportunity score
    opps.sort(key=lambda o: o.get('Opportunity Score', 0), reverse=True)

    # Write output
    output_path = f"output/population_ranked_{state}.csv"
    _write_population_csv(opps, output_path)

    # Print top 10
    if verbose:
        print(f"\n  TOP 10 OPPORTUNITIES BY POPULATION SCORE - {state}")
        print(f"  {'='*75}")
        print(f"  {'#':>3} {'Score':>8} {'Pop 10km':>10} {'Nearest km':>10} {'Nearest Town':<20} {'POI':<25}")
        print(f"  {'-'*75}")
        for i, opp in enumerate(opps[:10], 1):
            poi = (opp.get('POI Name', '') or opp.get('Address', ''))[:24]
            town = (opp.get('Nearest Town', '') or '')[:19]
            print(f"  {i:>3} {opp.get('Opportunity Score',0):>8.0f} "
                  f"{opp.get('Pop 10km',0):>10,} "
                  f"{opp['_nearest_km']:>10.2f} "
                  f"{town:<20} "
                  f"{poi:<25}")

    print(f"\n  Output: {output_path}")
    return opps


def _write_population_csv(opps: List[Dict], output_path: str):
    """Write population-ranked opportunities to CSV."""
    if not opps:
        return

    # Determine fieldnames from first row (exclude internal fields)
    base_fields = [
        'Latitude', 'Longitude', 'Address', 'Qualifying Rules', 'Evidence',
        'Confidence', 'Nearest Pharmacy (km)', 'Nearest Pharmacy Name',
        'POI Name', 'POI Type', 'Region', 'Date Scanned',
    ]
    
    # Add verification fields if present
    if 'Verification' in opps[0]:
        base_fields.extend(['Verification', 'Verification Notes'])
    
    # Add population fields
    pop_fields = [
        'Pop 5km', 'Pop 10km', 'Pop 15km',
        'Nearest Town', 'Nearest Town Pop', 'Nearest Town Dist (km)',
        'Opportunity Score',
    ]
    
    fieldnames = base_fields + pop_fields

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        for opp in opps:
            writer.writerow(opp)


# -- Main ----------------------------------------------------------

def main():
    sys.stdout.reconfigure(line_buffering=True)

    parser = argparse.ArgumentParser(
        description='Add population data overlay to opportunity zones',
    )
    parser.add_argument('--state', type=str, help='State to process (e.g., TAS)')
    parser.add_argument('--all', action='store_true', help='Process all states')
    parser.add_argument('--quiet', action='store_true')

    args = parser.parse_args()

    if not args.state and not args.all:
        parser.print_help()
        return

    states = []
    if args.all:
        states = ['TAS', 'ACT', 'NT', 'SA', 'WA', 'QLD', 'NSW', 'VIC']
    elif args.state:
        states = [args.state.upper()]

    for state in states:
        process_state(state, verbose=not args.quiet)


if __name__ == '__main__':
    main()
