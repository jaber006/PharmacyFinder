#!/usr/bin/env python3
"""
Growth Corridor Detection for PharmacyFinder opportunities.

For top opportunities (by composite score), searches for signs of
population growth using web data.

Can be run in two modes:
1. --generate-queries: outputs search queries to a file
2. --apply-results: reads search results and updates CSVs

Or run with --auto to attempt automated detection using known
growth area databases and heuristics.
"""

import csv
import os
import re
import json
import argparse

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')
STATES = ['ACT', 'NSW', 'NT', 'QLD', 'SA', 'TAS', 'VIC', 'WA']

# Known growth corridors and development areas in Australia
# Source: various state government planning documents
KNOWN_GROWTH_AREAS = {
    # NSW
    'Marsden Park': 'NSW Growth Centre - North West',
    'Oran Park': 'NSW Growth Centre - South West',
    'Leppington': 'NSW Growth Centre - South West',
    'Box Hill': 'NSW Growth Centre - The Hills',
    'Schofields': 'NSW Growth Centre - North West',
    'Austral': 'NSW Growth Centre - South West',
    'Gregory Hills': 'NSW Growth Centre - South West',
    'Cobbitty': 'NSW Growth Centre - South West',
    'Wilton': 'NSW Growth Area - Wilton',
    'Calderwood': 'NSW Growth Area - West Shellharbour',
    'Tullimbar': 'NSW Growth Area - West Shellharbour',
    'Spring Farm': 'NSW Growth Area - Greater Macarthur',
    'Menangle Park': 'NSW Growth Area - Greater Macarthur',
    'Gilead': 'NSW Growth Area - Greater Macarthur',
    'Googong': 'NSW Growth Area - Queanbeyan-Palerang',
    'Huntlee': 'NSW Growth Area - Hunter Valley',
    'Chisholm': 'NSW Growth Area - Maitland',
    # VIC
    'Craigieburn': 'VIC Growth Corridor - Northern',
    'Mickleham': 'VIC Growth Corridor - Northern',
    'Kalkallo': 'VIC Growth Corridor - Northern',
    'Donnybrook': 'VIC Growth Corridor - Northern',
    'Beveridge': 'VIC Growth Corridor - Northern',
    'Wallan': 'VIC Growth Corridor - Northern',
    'Wollert': 'VIC Growth Corridor - Northern',
    'Clyde': 'VIC Growth Corridor - South East',
    'Clyde North': 'VIC Growth Corridor - South East',
    'Officer': 'VIC Growth Corridor - South East',
    'Pakenham': 'VIC Growth Corridor - South East',
    'Tarneit': 'VIC Growth Corridor - Western',
    'Truganina': 'VIC Growth Corridor - Western',
    'Wyndham Vale': 'VIC Growth Corridor - Western',
    'Werribee': 'VIC Growth Corridor - Western',
    'Manor Lakes': 'VIC Growth Corridor - Western',
    'Melton South': 'VIC Growth Corridor - Western',
    'Rockbank': 'VIC Growth Corridor - Western',
    'Aintree': 'VIC Growth Corridor - Western',
    'Fraser Rise': 'VIC Growth Corridor - Western',
    'Thornhill Park': 'VIC Growth Corridor - Western',
    'Armstrong Creek': 'VIC Growth Area - Geelong',
    'Charlemont': 'VIC Growth Area - Geelong',
    'Mt Duneed': 'VIC Growth Area - Geelong',
    'Warragul': 'VIC Growth Area - West Gippsland',
    'Drouin': 'VIC Growth Area - West Gippsland',
    'Torquay': 'VIC Growth Area - Surf Coast',
    'Lara': 'VIC Growth Area - Greater Geelong',
    'Sunbury': 'VIC Growth Corridor - Northern',
    'Diggers Rest': 'VIC Growth Corridor - Northern',
    # QLD
    'Springfield': 'QLD Growth Area - Greater Springfield',
    'Yarrabilba': 'QLD Growth Area - Logan',
    'Flagstone': 'QLD Growth Area - Logan',
    'Ripley Valley': 'QLD Growth Area - Ipswich',
    'Caloundra South': 'QLD Growth Area - Sunshine Coast',
    'Aura': 'QLD Growth Area - Sunshine Coast (Aura)',
    'Palmview': 'QLD Growth Area - Sunshine Coast',
    'Pimpama': 'QLD Growth Area - Gold Coast Northern',
    'Coomera': 'QLD Growth Area - Gold Coast Northern',
    'Ormeau': 'QLD Growth Area - Gold Coast Northern',
    'Caboolture West': 'QLD Growth Area - Moreton Bay',
    'Park Ridge': 'QLD Growth Area - Logan',
    'Jimboomba': 'QLD Growth Area - Logan',
    'Redbank Plains': 'QLD Growth Area - Ipswich',
    'Deebing Heights': 'QLD Growth Area - Ipswich',
    'Collingwood Park': 'QLD Growth Area - Ipswich',
    'Bahrs Scrub': 'QLD Growth Area - Logan',
    'Burpengary East': 'QLD Growth Area - Moreton Bay',
    # SA
    'Mount Barker': 'SA Growth Area - Mount Barker',
    'Gawler': 'SA Growth Area - Gawler',
    'Two Wells': 'SA Growth Area - Northern Adelaide',
    'Angle Vale': 'SA Growth Area - Northern Adelaide',
    'Seaford': 'SA Growth Area - Onkaparinga',
    'Aldinga': 'SA Growth Area - Onkaparinga',
    'Munno Para': 'SA Growth Area - Playford',
    'Virginia': 'SA Growth Area - Playford',
    # WA
    'Baldivis': 'WA Growth Area - Rockingham',
    'Wellard': 'WA Growth Area - Kwinana',
    'Byford': 'WA Growth Area - Serpentine-Jarrahdale',
    'Ellenbrook': 'WA Growth Area - Swan',
    'Brabham': 'WA Growth Area - Swan',
    'Alkimos': 'WA Growth Area - Wanneroo',
    'Yanchep': 'WA Growth Area - Wanneroo',
    'Two Rocks': 'WA Growth Area - Wanneroo',
    'Harrisdale': 'WA Growth Area - Armadale',
    'Piara Waters': 'WA Growth Area - Armadale',
    'Treeby': 'WA Growth Area - Cockburn',
    'Haynes': 'WA Growth Area - Armadale',
    'Mandogalup': 'WA Growth Area - Kwinana',
    'Southern River': 'WA Growth Area - Gosnells',
    # ACT
    'Molonglo Valley': 'ACT Growth Area - Molonglo Valley',
    'Denman Prospect': 'ACT Growth Area - Molonglo Valley',
    'Whitlam': 'ACT Growth Area - Molonglo Valley',
    'Ginninderry': 'ACT Growth Area - Ginninderry',
    'Taylor': 'ACT Growth Area - Gungahlin',
    'Throsby': 'ACT Growth Area - Gungahlin',
    'Jacka': 'ACT Growth Area - Gungahlin',
    'Moncrieff': 'ACT Growth Area - Gungahlin',
    'Googong': 'ACT/NSW Growth Area - Googong',
    # TAS
    'Kingston': 'TAS Growth Area - Kingborough',
    'Sorell': 'TAS Growth Area - Sorell',
    'Brighton': 'TAS Growth Area - Brighton',
    'Rokeby': 'TAS Growth Area - Clarence',
    'Howrah': 'TAS Growth Area - Clarence',
    'Legana': 'TAS Growth Area - West Tamar',
    'Prospect Vale': 'TAS Growth Area - Meander Valley',
}

# Population growth keywords to search for in evidence/address
GROWTH_KEYWORDS = [
    'estate', 'development', 'new', 'village', 'springs', 'rise',
    'heights', 'grove', 'gardens', 'park', 'vale', 'meadows',
    'lakes', 'waters', 'views', 'landing', 'reach', 'haven',
]


def get_all_opportunities():
    """Load all opportunities from population_ranked CSVs."""
    all_opps = []

    for state in STATES:
        filepath = os.path.join(OUTPUT_DIR, f'population_ranked_{state}.csv')
        if not os.path.exists(filepath):
            continue

        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            fieldnames = list(reader.fieldnames)
            for row in reader:
                row['_state'] = state
                row['_fieldnames'] = fieldnames
                all_opps.append(row)

    return all_opps


def detect_growth(opp):
    """Detect growth indicators for a single opportunity using heuristics."""
    name = opp.get('POI Name', '').strip()
    address = opp.get('Address', '').strip()
    town = opp.get('Nearest Town', '').strip()
    evidence = opp.get('Evidence', '').strip()

    growth_found = False
    growth_details = []

    # Check against known growth areas
    for area_name, description in KNOWN_GROWTH_AREAS.items():
        area_lower = area_name.lower()
        if (area_lower in address.lower() or
            area_lower in town.lower() or
            area_lower in name.lower()):
            growth_found = True
            growth_details.append(description)
            break

    # Check for growth-suggestive naming patterns in POI/address
    combined_text = f"{name} {address} {town}".lower()
    for kw in GROWTH_KEYWORDS:
        if kw in combined_text and not growth_found:
            # Only flag if it's a populated area (not remote)
            pop_10km = float(opp.get('Pop 10km', 0) or 0)
            if pop_10km > 5000:
                growth_found = True
                growth_details.append(f"Name suggests new development ({kw})")
                break

    # High population + relatively close to pharmacy = likely urban fringe growth
    pop_10km = float(opp.get('Pop 10km', 0) or 0)
    nearest_km = float(opp.get('Nearest Pharmacy (km)', 0) or 0)
    comp_score = float(opp.get('Competition Score', 0) or 0)

    # Urban fringe heuristic: high population but still some distance
    if pop_10km > 20000 and 1.0 < nearest_km < 5.0 and comp_score < 30:
        if not growth_found:
            growth_found = True
            growth_details.append(f"Urban fringe: {pop_10km:,.0f} pop, {nearest_km:.1f}km gap, low competition")

    return {
        'has_growth': growth_found,
        'details': '; '.join(growth_details) if growth_details else ''
    }


def update_csvs_with_growth(opportunities):
    """Write growth data back to CSVs."""
    # Group by state
    by_state = {}
    for opp in opportunities:
        state = opp['_state']
        if state not in by_state:
            by_state[state] = []
        by_state[state].append(opp)

    for state, opps in by_state.items():
        filepath = os.path.join(OUTPUT_DIR, f'population_ranked_{state}.csv')

        # Get fieldnames from first opportunity
        fieldnames = opps[0]['_fieldnames']

        # Add growth columns if not present
        for col in ['Growth Indicator', 'Growth Details']:
            if col not in fieldnames:
                fieldnames.append(col)

        # Clean internal keys and write
        with open(filepath, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            for opp in opps:
                # Remove internal keys
                row = {k: v for k, v in opp.items() if not k.startswith('_')}
                writer.writerow(row)

        growth_count = sum(1 for o in opps if o.get('Growth Indicator') == 'YES')
        if growth_count > 0:
            print(f"  {state}: {growth_count} growth areas flagged out of {len(opps)} opportunities")
        else:
            print(f"  {state}: {len(opps)} opportunities (no growth areas detected)")


def main():
    parser = argparse.ArgumentParser(description='Detect growth corridors for pharmacy opportunities')
    parser.add_argument('--top', type=int, default=0, help='Only analyze top N opportunities (0 = all)')
    args = parser.parse_args()

    print("=" * 60)
    print("GROWTH CORRIDOR DETECTION")
    print("=" * 60)

    print("\nLoading all opportunities...")
    all_opps = get_all_opportunities()
    print(f"  Loaded {len(all_opps)} opportunities across {len(STATES)} states")

    # Analyze growth for each
    print("\nDetecting growth corridors...")
    growth_count = 0

    for i, opp in enumerate(all_opps):
        result = detect_growth(opp)
        opp['Growth Indicator'] = 'YES' if result['has_growth'] else ''
        opp['Growth Details'] = result['details']

        if result['has_growth']:
            growth_count += 1

    print(f"\n  Total growth areas detected: {growth_count}/{len(all_opps)}")

    # Show growth areas
    if growth_count > 0:
        print("\n  Growth corridor opportunities:")
        growth_opps = [o for o in all_opps if o.get('Growth Indicator') == 'YES']
        growth_opps.sort(key=lambda x: float(x.get('Composite Score', 0) or 0), reverse=True)
        for g in growth_opps[:20]:
            print(f"    - {g['POI Name']} ({g['_state']}) | "
                  f"Pop: {g.get('Pop 10km', 'N/A')} | "
                  f"Composite: {g.get('Composite Score', 'N/A')} | "
                  f"{g.get('Growth Details', '')}")

    # Write back
    print("\nUpdating CSVs...")
    update_csvs_with_growth(all_opps)

    print(f"\n{'=' * 60}")
    print(f"Growth corridor detection complete")
    print(f"{'=' * 60}")


if __name__ == '__main__':
    main()
