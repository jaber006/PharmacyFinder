#!/usr/bin/env python3
"""
Growth Corridor Detection for PharmacyFinder opportunities.

Detects whether each opportunity zone is in or near a known growth corridor
using a curated database of Australian growth areas and heuristics.

State-aware matching prevents cross-state false positives (e.g. "Springfield"
in NSW won't match the QLD Springfield growth area).
"""

import csv
import os
import re
import argparse

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')
STATES = ['ACT', 'NSW', 'NT', 'QLD', 'SA', 'TAS', 'VIC', 'WA']

# Known growth corridors and development areas in Australia
# Source: various state government planning documents
# Each entry: (name, applicable_states, description)
# applicable_states restricts matching to the correct state(s)
KNOWN_GROWTH_AREAS = [
    # NSW
    ('Marsden Park', {'NSW'}, 'NSW Growth Centre - North West'),
    ('Oran Park', {'NSW'}, 'NSW Growth Centre - South West'),
    ('Leppington', {'NSW'}, 'NSW Growth Centre - South West'),
    ('Box Hill', {'NSW'}, 'NSW Growth Centre - The Hills'),
    ('Schofields', {'NSW'}, 'NSW Growth Centre - North West'),
    ('Austral', {'NSW'}, 'NSW Growth Centre - South West'),
    ('Gregory Hills', {'NSW'}, 'NSW Growth Centre - South West'),
    ('Cobbitty', {'NSW'}, 'NSW Growth Centre - South West'),
    ('Wilton', {'NSW'}, 'NSW Growth Area - Wilton'),
    ('Calderwood', {'NSW'}, 'NSW Growth Area - West Shellharbour'),
    ('Tullimbar', {'NSW'}, 'NSW Growth Area - West Shellharbour'),
    ('Spring Farm', {'NSW'}, 'NSW Growth Area - Greater Macarthur'),
    ('Menangle Park', {'NSW'}, 'NSW Growth Area - Greater Macarthur'),
    ('Gilead', {'NSW'}, 'NSW Growth Area - Greater Macarthur'),
    ('Googong', {'NSW', 'ACT'}, 'NSW/ACT Growth Area - Googong'),
    ('Huntlee', {'NSW'}, 'NSW Growth Area - Hunter Valley'),
    ('Chisholm', {'NSW'}, 'NSW Growth Area - Maitland'),
    ('Glenmore Park', {'NSW'}, 'NSW Growth Area - Greater Penrith'),
    ('Jordan Springs', {'NSW'}, 'NSW Growth Area - Greater Penrith'),
    ('Edmondson Park', {'NSW'}, 'NSW Growth Centre - South West'),
    # VIC
    ('Craigieburn', {'VIC'}, 'VIC Growth Corridor - Northern'),
    ('Mickleham', {'VIC'}, 'VIC Growth Corridor - Northern'),
    ('Kalkallo', {'VIC'}, 'VIC Growth Corridor - Northern'),
    ('Donnybrook', {'VIC'}, 'VIC Growth Corridor - Northern'),
    ('Beveridge', {'VIC'}, 'VIC Growth Corridor - Northern'),
    ('Wallan', {'VIC'}, 'VIC Growth Corridor - Northern'),
    ('Wollert', {'VIC'}, 'VIC Growth Corridor - Northern'),
    ('Clyde North', {'VIC'}, 'VIC Growth Corridor - South East'),
    ('Clyde', {'VIC'}, 'VIC Growth Corridor - South East'),
    ('Officer', {'VIC'}, 'VIC Growth Corridor - South East'),
    ('Pakenham', {'VIC'}, 'VIC Growth Corridor - South East'),
    ('Tarneit', {'VIC'}, 'VIC Growth Corridor - Western'),
    ('Truganina', {'VIC'}, 'VIC Growth Corridor - Western'),
    ('Wyndham Vale', {'VIC'}, 'VIC Growth Corridor - Western'),
    ('Werribee', {'VIC'}, 'VIC Growth Corridor - Western'),
    ('Manor Lakes', {'VIC'}, 'VIC Growth Corridor - Western'),
    ('Melton South', {'VIC'}, 'VIC Growth Corridor - Western'),
    ('Rockbank', {'VIC'}, 'VIC Growth Corridor - Western'),
    ('Aintree', {'VIC'}, 'VIC Growth Corridor - Western'),
    ('Fraser Rise', {'VIC'}, 'VIC Growth Corridor - Western'),
    ('Thornhill Park', {'VIC'}, 'VIC Growth Corridor - Western'),
    ('Armstrong Creek', {'VIC'}, 'VIC Growth Area - Geelong'),
    ('Charlemont', {'VIC'}, 'VIC Growth Area - Geelong'),
    ('Mt Duneed', {'VIC'}, 'VIC Growth Area - Geelong'),
    ('Warragul', {'VIC'}, 'VIC Growth Area - West Gippsland'),
    ('Drouin', {'VIC'}, 'VIC Growth Area - West Gippsland'),
    ('Torquay', {'VIC'}, 'VIC Growth Area - Surf Coast'),
    ('Lara', {'VIC'}, 'VIC Growth Area - Greater Geelong'),
    ('Sunbury', {'VIC'}, 'VIC Growth Corridor - Northern'),
    ('Diggers Rest', {'VIC'}, 'VIC Growth Corridor - Northern'),
    ('Longwarry', {'VIC'}, 'VIC Growth Area - West Gippsland'),
    # QLD
    ('Springfield', {'QLD'}, 'QLD Growth Area - Greater Springfield'),
    ('Yarrabilba', {'QLD'}, 'QLD Growth Area - Logan'),
    ('Flagstone', {'QLD'}, 'QLD Growth Area - Logan'),
    ('Ripley Valley', {'QLD'}, 'QLD Growth Area - Ipswich'),
    ('Caloundra South', {'QLD'}, 'QLD Growth Area - Sunshine Coast'),
    ('Aura', {'QLD'}, 'QLD Growth Area - Sunshine Coast (Aura)'),
    ('Palmview', {'QLD'}, 'QLD Growth Area - Sunshine Coast'),
    ('Pimpama', {'QLD'}, 'QLD Growth Area - Gold Coast Northern'),
    ('Coomera', {'QLD'}, 'QLD Growth Area - Gold Coast Northern'),
    ('Ormeau', {'QLD'}, 'QLD Growth Area - Gold Coast Northern'),
    ('Caboolture West', {'QLD'}, 'QLD Growth Area - Moreton Bay'),
    ('Park Ridge', {'QLD'}, 'QLD Growth Area - Logan'),
    ('Jimboomba', {'QLD'}, 'QLD Growth Area - Logan'),
    ('Redbank Plains', {'QLD'}, 'QLD Growth Area - Ipswich'),
    ('Deebing Heights', {'QLD'}, 'QLD Growth Area - Ipswich'),
    ('Collingwood Park', {'QLD'}, 'QLD Growth Area - Ipswich'),
    ('Bahrs Scrub', {'QLD'}, 'QLD Growth Area - Logan'),
    ('Burpengary East', {'QLD'}, 'QLD Growth Area - Moreton Bay'),
    ('Merrimac', {'QLD'}, 'QLD Growth Area - Gold Coast'),
    # SA
    ('Mount Barker', {'SA'}, 'SA Growth Area - Mount Barker'),
    ('Gawler', {'SA'}, 'SA Growth Area - Gawler'),
    ('Two Wells', {'SA'}, 'SA Growth Area - Northern Adelaide'),
    ('Angle Vale', {'SA'}, 'SA Growth Area - Northern Adelaide'),
    ('Seaford', {'SA'}, 'SA Growth Area - Onkaparinga'),
    ('Aldinga', {'SA'}, 'SA Growth Area - Onkaparinga'),
    ('Munno Para', {'SA'}, 'SA Growth Area - Playford'),
    ('Virginia', {'SA'}, 'SA Growth Area - Playford'),
    ('Roxby Downs', {'SA'}, 'SA Growth Area - BHP Olympic Dam expansion'),
    # WA
    ('Baldivis', {'WA'}, 'WA Growth Area - Rockingham'),
    ('Wellard', {'WA'}, 'WA Growth Area - Kwinana'),
    ('Byford', {'WA'}, 'WA Growth Area - Serpentine-Jarrahdale'),
    ('Ellenbrook', {'WA'}, 'WA Growth Area - Swan'),
    ('Brabham', {'WA'}, 'WA Growth Area - Swan'),
    ('Alkimos', {'WA'}, 'WA Growth Area - Wanneroo'),
    ('Yanchep', {'WA'}, 'WA Growth Area - Wanneroo'),
    ('Two Rocks', {'WA'}, 'WA Growth Area - Wanneroo'),
    ('Harrisdale', {'WA'}, 'WA Growth Area - Armadale'),
    ('Piara Waters', {'WA'}, 'WA Growth Area - Armadale'),
    ('Treeby', {'WA'}, 'WA Growth Area - Cockburn'),
    ('Haynes', {'WA'}, 'WA Growth Area - Armadale'),
    ('Mandogalup', {'WA'}, 'WA Growth Area - Kwinana'),
    ('Southern River', {'WA'}, 'WA Growth Area - Gosnells'),
    # ACT
    ('Molonglo Valley', {'ACT'}, 'ACT Growth Area - Molonglo Valley'),
    ('Denman Prospect', {'ACT'}, 'ACT Growth Area - Molonglo Valley'),
    ('Whitlam', {'ACT'}, 'ACT Growth Area - Molonglo Valley'),
    ('Ginninderry', {'ACT'}, 'ACT Growth Area - Ginninderry'),
    ('Taylor', {'ACT'}, 'ACT Growth Area - Gungahlin'),
    ('Throsby', {'ACT'}, 'ACT Growth Area - Gungahlin'),
    ('Jacka', {'ACT'}, 'ACT Growth Area - Gungahlin'),
    ('Moncrieff', {'ACT'}, 'ACT Growth Area - Gungahlin'),
    # TAS
    ('Kingston', {'TAS'}, 'TAS Growth Area - Kingborough'),
    ('Sorell', {'TAS'}, 'TAS Growth Area - Sorell'),
    ('Brighton', {'TAS'}, 'TAS Growth Area - Brighton'),
    ('Rokeby', {'TAS'}, 'TAS Growth Area - Clarence'),
    ('Howrah', {'TAS'}, 'TAS Growth Area - Clarence'),
    ('Legana', {'TAS'}, 'TAS Growth Area - West Tamar'),
    ('Prospect Vale', {'TAS'}, 'TAS Growth Area - Meander Valley'),
    # NT
    ('Palmerston', {'NT'}, 'NT Growth Area - Palmerston'),
    ('Zuccoli', {'NT'}, 'NT Growth Area - Palmerston'),
]

# Multi-word growth keywords — must be at least 2 words to reduce false positives
GROWTH_KEYWORDS = [
    'new estate', 'new development', 'land estate',
    'housing estate', 'housing development',
    'master planned', 'masterplanned', 'master plan',
    'growth area', 'growth corridor', 'greenfield',
    'land release', 'new community',
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
    state = opp.get('_state', opp.get('Region', ''))

    growth_found = False
    growth_details = []

    # Check against known growth areas WITH state filtering
    for area_name, valid_states, description in KNOWN_GROWTH_AREAS:
        # Only match if the opportunity is in a valid state for this growth area
        if state not in valid_states:
            continue

        # Use word boundary regex to avoid false matches
        pattern = r'\b' + re.escape(area_name.lower()) + r'\b'

        # Search in address and town (NOT in POI name — "Springfield Shopping Centre"
        # in a non-Springfield town shouldn't match)
        if re.search(pattern, address.lower()) or re.search(pattern, town.lower()):
            growth_found = True
            growth_details.append(description)
            break

    # Check for growth-suggestive naming patterns in address/town only
    # (not POI name, to avoid matching store names like "Newtown" or "Greenfield IGA")
    address_town_text = f"{address} {town}".lower()
    for kw in GROWTH_KEYWORDS:
        if kw in address_town_text and not growth_found:
            pop_10km = float(opp.get('Pop 10km', 0) or 0)
            if pop_10km > 5000:
                growth_found = True
                growth_details.append(f"Address suggests growth ({kw})")
                break

    # Urban fringe heuristic: high population but still gap in pharmacy coverage
    if not growth_found:
        pop_10km = float(opp.get('Pop 10km', 0) or 0)
        nearest_km = float(opp.get('Nearest Pharmacy (km)', 0) or 0)
        comp_score = float(opp.get('Competition Score', 0) or 0)

        # High population + pharmacy gap + low competition = likely urban fringe growth
        if pop_10km > 25000 and 1.5 < nearest_km < 5.0 and comp_score < 20:
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
        fieldnames = list(opps[0]['_fieldnames'])

        # Add growth columns if not present
        for col in ['Growth Indicator', 'Growth Details']:
            if col not in fieldnames:
                fieldnames.append(col)

        # Remove old growth columns that we're replacing
        old_cols = ['Growth Corridor', 'Growth Type', 'Growth Rating',
                    'Growth Notes', 'Growth Distance (km)']
        for col in old_cols:
            if col in fieldnames:
                fieldnames.remove(col)

        # Clean internal keys and write
        with open(filepath, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            for opp in opps:
                # Remove internal keys and old growth columns
                row = {k: v for k, v in opp.items()
                       if not k.startswith('_') and k not in old_cols}
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
