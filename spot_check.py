"""
PharmacyFinder QA — Spot Check 10 Opportunities
=================================================
Verifies data accuracy for a representative sample.

Selection:
  2x Item 130, 2x Item 131, 2x Item 132, 1x Item 133, 
  1x Item 134A, 1x Item 135/136, 1x random
  Includes TAS (Norwood, Burnie)
"""

import sqlite3
import sys
import io
import math
import json
from datetime import datetime

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

DB_PATH = 'pharmacy_finder.db'

def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))

# Expected city/town approximate coordinates for sanity checking
KNOWN_LOCATIONS = {
    'Launceston': (-41.44, 147.14),
    'Hobart': (-42.88, 147.33),
    'Burnie': (-41.05, 145.91),
    'Adelaide': (-34.93, 138.60),
    'Sydney': (-33.87, 151.21),
    'Melbourne': (-37.81, 144.96),
    'Brisbane': (-27.47, 153.03),
    'Perth': (-31.95, 115.86),
    'Darwin': (-12.46, 130.84),
    'Canberra': (-35.28, 149.13),
}

def find_nearest_pharmacy(c, lat, lng):
    """Find the actual nearest pharmacy from the pharmacies table."""
    c.execute("SELECT id, name, latitude, longitude, suburb, state FROM pharmacies")
    pharmacies = c.fetchall()
    
    nearest = None
    min_dist = float('inf')
    for p in pharmacies:
        d = haversine_km(lat, lng, p['latitude'], p['longitude'])
        if d < min_dist:
            min_dist = d
            nearest = p
    return nearest, min_dist

def check_opportunity(c, opp_id, all_pharmacies=None):
    """Run all checks on a single opportunity."""
    c.execute("SELECT * FROM opportunities WHERE id = ?", (opp_id,))
    opp = c.fetchone()
    if not opp:
        return {'id': opp_id, 'error': 'NOT FOUND'}
    
    result = {
        'id': opp['id'],
        'name': opp['poi_name'],
        'town': opp['nearest_town'],
        'state': opp['region'],
        'rule': opp['qualifying_rules'],
        'lat': opp['latitude'],
        'lng': opp['longitude'],
        'db_nearest_pharmacy': opp['nearest_pharmacy_name'],
        'db_nearest_km': opp['nearest_pharmacy_km'],
        'db_pop_5km': opp['pop_5km'],
        'checks': {},
    }
    
    # Check 1: Recalculate nearest pharmacy distance
    nearest_pharm, recalc_dist = find_nearest_pharmacy(c, opp['latitude'], opp['longitude'])
    result['recalc_nearest_pharmacy'] = nearest_pharm['name'] if nearest_pharm else 'NONE'
    result['recalc_nearest_km'] = recalc_dist
    
    db_dist = opp['nearest_pharmacy_km']
    dist_diff = abs(recalc_dist - db_dist) if db_dist else float('inf')
    dist_pct = (dist_diff / max(db_dist, 0.001)) * 100 if db_dist else 0
    
    if dist_pct < 5:
        result['checks']['distance'] = 'PASS'
        result['checks']['distance_note'] = f'Recalc={recalc_dist:.3f}km vs DB={db_dist:.3f}km (diff={dist_pct:.1f}%)'
    elif dist_pct < 20:
        result['checks']['distance'] = 'WARN'
        result['checks']['distance_note'] = f'Recalc={recalc_dist:.3f}km vs DB={db_dist:.3f}km (diff={dist_pct:.1f}%) - minor discrepancy'
    else:
        result['checks']['distance'] = 'FAIL'
        result['checks']['distance_note'] = f'Recalc={recalc_dist:.3f}km vs DB={db_dist:.3f}km (diff={dist_pct:.1f}%) - MAJOR discrepancy'
    
    if nearest_pharm and nearest_pharm['name'] != opp['nearest_pharmacy_name']:
        result['checks']['nearest_name_match'] = 'WARN'
        result['checks']['nearest_name_note'] = f"DB says '{opp['nearest_pharmacy_name']}', recalc found '{nearest_pharm['name']}'"
    else:
        result['checks']['nearest_name_match'] = 'PASS'
    
    # Check 2: Qualifying rule plausibility
    rule = opp['qualifying_rules']
    rules_check = 'PASS'
    rules_note = ''
    
    if 'Item 130' in rule:
        # Item 130: >=1.5km from nearest pharmacy + supermarket/GP within 500m
        if recalc_dist < 1.5:
            rules_check = 'FAIL'
            rules_note = f'Item 130 requires >=1.5km, but nearest pharmacy is {recalc_dist:.2f}km'
        else:
            rules_note = f'Item 130: {recalc_dist:.2f}km from nearest pharmacy (>=1.5km required) - OK'
    
    elif 'Item 131' in rule:
        # Item 131: >=10km by route from nearest pharmacy
        if recalc_dist < 8:  # straight line, so route would be longer
            rules_check = 'WARN'
            rules_note = f'Item 131 requires >=10km by road. Straight-line={recalc_dist:.2f}km - may not qualify'
        elif recalc_dist >= 10:
            rules_note = f'Item 131: {recalc_dist:.2f}km straight-line (road would be longer) - likely qualifies'
        else:
            rules_note = f'Item 131: {recalc_dist:.2f}km straight-line - borderline, road distance needed'
    
    elif 'Item 132' in rule:
        # Item 132: supermarket >= 200m from nearest pharmacy
        if recalc_dist < 0.2:
            rules_check = 'WARN'
            rules_note = f'Item 132 requires >=200m. Distance={recalc_dist*1000:.0f}m - verify this is the right threshold'
        else:
            rules_note = f'Item 132: {recalc_dist*1000:.0f}m from nearest pharmacy (>=200m required) - OK'
    
    elif 'Item 133' in rule:
        # Item 133: small shopping centre
        rules_note = f'Item 133: small shopping centre rule. Distance={recalc_dist:.2f}km'
    
    elif 'Item 134A' in rule:
        # Item 134A: large shopping centre
        rules_note = f'Item 134A: large shopping centre. Distance={recalc_dist:.2f}km'
    
    elif 'Item 136' in rule:
        # Item 136: large medical centre, >=300m from nearest pharmacy
        if recalc_dist < 0.3:
            rules_check = 'WARN'
            rules_note = f'Item 136 requires >=300m. Distance={recalc_dist*1000:.0f}m - may not qualify'
        else:
            rules_note = f'Item 136: {recalc_dist*1000:.0f}m from nearest pharmacy (>=300m required) - OK'
    
    result['checks']['rule'] = rules_check
    result['checks']['rule_note'] = rules_note
    
    # Check 3: Town/location name sensibility
    town = opp['nearest_town']
    town_check = 'PASS'
    town_note = ''
    
    if not town or town == 'Unknown' or town == '':
        town_check = 'WARN'
        town_note = 'No town name set'
    else:
        # Check if coords are roughly near the claimed town
        known = KNOWN_LOCATIONS.get(town)
        if known:
            town_dist = haversine_km(opp['latitude'], opp['longitude'], known[0], known[1])
            if town_dist > 50:
                town_check = 'FAIL'
                town_note = f"Claimed town '{town}' is {town_dist:.0f}km from actual {town}"
            else:
                town_note = f"Town '{town}' checks out ({town_dist:.0f}km from city centre)"
        else:
            town_note = f"Town '{town}' (not in known cities list, assuming OK)"
    
    result['checks']['town'] = town_check
    result['checks']['town_note'] = town_note
    
    # Check 4: Population reasonableness
    pop5 = opp['pop_5km']
    pop_check = 'PASS'
    pop_note = ''
    
    if pop5 == 0:
        if 'Item 131' in rule:
            pop_check = 'WARN'
            pop_note = 'pop_5km=0 for Item 131 remote location - may be valid but worth checking'
        else:
            pop_check = 'FAIL'
            pop_note = 'pop_5km=0 for non-rural rule - suspicious'
    elif pop5 > 1000000:
        # Only major city CBDs should have >1M in 5km
        if recalc_dist > 5:
            pop_check = 'WARN'
            pop_note = f'pop_5km={pop5:,} seems very high for a location {recalc_dist:.1f}km from nearest pharmacy'
        else:
            pop_note = f'pop_5km={pop5:,} - high but plausible for urban area'
    elif pop5 < 100 and 'Item 131' not in rule:
        pop_check = 'WARN'
        pop_note = f'pop_5km={pop5} seems very low for non-rural rule'
    else:
        pop_note = f'pop_5km={pop5:,} - reasonable'
    
    result['checks']['population'] = pop_check
    result['checks']['population_note'] = pop_note
    
    # Overall
    checks = result['checks']
    fails = sum(1 for k, v in checks.items() if not k.endswith('_note') and v == 'FAIL')
    warns = sum(1 for k, v in checks.items() if not k.endswith('_note') and v == 'WARN')
    
    if fails > 0:
        result['overall'] = 'FAIL'
    elif warns > 0:
        result['overall'] = 'WARN'
    else:
        result['overall'] = 'PASS'
    
    return result

def format_result(r):
    """Format a check result for display and markdown."""
    lines = []
    lines.append(f"### #{r['id']}: {r['name']} ({r['town']}, {r['state']})")
    lines.append(f"**Rule:** {r['rule']}  ")
    lines.append(f"**Coords:** {r['lat']:.6f}, {r['lng']:.6f}  ")
    lines.append(f"**Overall:** {'PASS' if r['overall']=='PASS' else ('WARN' if r['overall']=='WARN' else 'FAIL')} {'✅' if r['overall']=='PASS' else ('⚠️' if r['overall']=='WARN' else '❌')}")
    lines.append("")
    lines.append("| Check | Result | Details |")
    lines.append("|-------|--------|---------|")
    
    check_order = ['distance', 'nearest_name_match', 'rule', 'town', 'population']
    labels = {
        'distance': 'Nearest pharmacy distance',
        'nearest_name_match': 'Nearest pharmacy name',
        'rule': 'Qualifying rule',
        'town': 'Town/location',
        'population': 'Population',
    }
    
    for check in check_order:
        if check in r['checks']:
            status = r['checks'][check]
            emoji = '✅' if status == 'PASS' else ('⚠️' if status == 'WARN' else '❌')
            note = r['checks'].get(f'{check}_note', '')
            if not note:
                note = r['checks'].get(f'{check}_note', 'OK')
            lines.append(f"| {labels.get(check, check)} | {emoji} {status} | {note} |")
    
    lines.append("")
    return '\n'.join(lines)


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    
    # Select our 10 opportunities
    # 2x Item 130 (includes Norwood TAS)
    # 2x Item 131 (different states)
    # 2x Item 132 (different states)
    # 1x Item 133
    # 1x Item 134A (includes TAS)
    # 1x Item 136 (includes Burnie TAS)
    # 1x random
    
    spot_check_ids = []
    
    # Item 130 - Norwood TAS (#9367, #9368)
    c.execute("""
        SELECT id FROM opportunities 
        WHERE qualifying_rules LIKE '%Item 130%' 
        AND verification NOT IN ('DATA_QUALITY_FAIL', 'BAD_COORDS', 'DUPLICATE')
        LIMIT 2
    """)
    spot_check_ids.extend([r['id'] for r in c.fetchall()])
    
    # Item 131 - pick from different states (NT and NSW)
    c.execute("""
        SELECT id FROM opportunities 
        WHERE qualifying_rules = 'Item 131'
        AND region = 'NT'
        AND verification NOT IN ('DATA_QUALITY_FAIL', 'BAD_COORDS', 'DUPLICATE', 'NEEDS_POP_DATA')
        AND opp_score > 0
        ORDER BY composite_score DESC
        LIMIT 1
    """)
    spot_check_ids.extend([r['id'] for r in c.fetchall()])
    
    c.execute("""
        SELECT id FROM opportunities 
        WHERE qualifying_rules = 'Item 131'
        AND region = 'NSW'
        AND verification NOT IN ('DATA_QUALITY_FAIL', 'BAD_COORDS', 'DUPLICATE', 'NEEDS_POP_DATA')
        AND opp_score > 0
        ORDER BY composite_score DESC
        LIMIT 1
    """)
    spot_check_ids.extend([r['id'] for r in c.fetchall()])
    
    # Item 132 - pick from QLD and TAS
    c.execute("""
        SELECT id FROM opportunities 
        WHERE qualifying_rules = 'Item 132'
        AND region = 'QLD'
        AND verification NOT IN ('DATA_QUALITY_FAIL', 'BAD_COORDS', 'DUPLICATE')
        AND opp_score > 0
        ORDER BY composite_score DESC
        LIMIT 1
    """)
    spot_check_ids.extend([r['id'] for r in c.fetchall()])
    
    c.execute("""
        SELECT id FROM opportunities 
        WHERE qualifying_rules = 'Item 132'
        AND region = 'TAS'
        AND verification NOT IN ('DATA_QUALITY_FAIL', 'BAD_COORDS', 'DUPLICATE')
        AND opp_score > 0
        ORDER BY composite_score DESC
        LIMIT 1
    """)
    spot_check_ids.extend([r['id'] for r in c.fetchall()])
    
    # Item 133
    c.execute("""
        SELECT id FROM opportunities 
        WHERE qualifying_rules LIKE '%Item 133%'
        AND verification NOT IN ('DATA_QUALITY_FAIL', 'BAD_COORDS', 'DUPLICATE')
        LIMIT 1
    """)
    spot_check_ids.extend([r['id'] for r in c.fetchall()])
    
    # Item 134A (TAS)
    c.execute("""
        SELECT id FROM opportunities 
        WHERE qualifying_rules = 'Item 134A'
        AND region = 'TAS'
        AND verification NOT IN ('DATA_QUALITY_FAIL', 'BAD_COORDS', 'DUPLICATE')
        ORDER BY composite_score DESC
        LIMIT 1
    """)
    spot_check_ids.extend([r['id'] for r in c.fetchall()])
    
    # Item 136 (Burnie TAS)
    c.execute("""
        SELECT id FROM opportunities 
        WHERE qualifying_rules = 'Item 136'
        AND nearest_town LIKE '%Burnie%'
        AND verification NOT IN ('DATA_QUALITY_FAIL', 'BAD_COORDS', 'DUPLICATE')
        LIMIT 1
    """)
    spot_check_ids.extend([r['id'] for r in c.fetchall()])
    
    # Random one from WA
    c.execute("""
        SELECT id FROM opportunities 
        WHERE qualifying_rules != 'NONE'
        AND region = 'WA'
        AND verification NOT IN ('DATA_QUALITY_FAIL', 'BAD_COORDS', 'DUPLICATE', 'NEEDS_POP_DATA')
        AND opp_score > 0
        ORDER BY RANDOM()
        LIMIT 1
    """)
    spot_check_ids.extend([r['id'] for r in c.fetchall()])
    
    print(f"Spot-checking {len(spot_check_ids)} opportunities: {spot_check_ids}")
    print("This may take a moment (calculating distances to all pharmacies)...\n")
    
    results = []
    for opp_id in spot_check_ids:
        print(f"  Checking #{opp_id}...", end=' ')
        r = check_opportunity(c, opp_id)
        print(f"{r['name']} -> {r['overall']}")
        results.append(r)
    
    # Generate markdown report
    md = []
    md.append("# Spot Check Report")
    md.append(f"*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}*\n")
    md.append("## Summary\n")
    
    passes = sum(1 for r in results if r['overall'] == 'PASS')
    warns = sum(1 for r in results if r['overall'] == 'WARN')
    fails = sum(1 for r in results if r['overall'] == 'FAIL')
    
    md.append(f"| Result | Count |")
    md.append(f"|--------|-------|")
    md.append(f"| ✅ PASS | {passes} |")
    md.append(f"| ⚠️ WARN | {warns} |")
    md.append(f"| ❌ FAIL | {fails} |")
    md.append(f"| **Total** | **{len(results)}** |")
    md.append("")
    
    md.append("## Detailed Results\n")
    for r in results:
        md.append(format_result(r))
    
    md.append("## Notes\n")
    md.append("- **Distance check**: Recalculates haversine distance from opportunity coords to ALL pharmacies in DB")
    md.append("- **Rule check**: Verifies the qualifying rule makes sense given the recalculated distance")
    md.append("- **Town check**: Verifies coords are plausibly near the claimed town (for known cities)")
    md.append("- **Population check**: Sanity-checks pop_5km figures")
    md.append("- Minor distance discrepancies (<5%) are expected due to coordinate precision differences")
    md.append("- Item 131 uses road distance (>= 10km), but we check straight-line here. Road > straight-line always.")
    
    with open('rules/SPOT_CHECK.md', 'w', encoding='utf-8') as f:
        f.write('\n'.join(md))
    
    print(f"\nReport written to rules/SPOT_CHECK.md")
    print(f"\nResults: {passes} PASS, {warns} WARN, {fails} FAIL")
    
    conn.close()


if __name__ == '__main__':
    main()
