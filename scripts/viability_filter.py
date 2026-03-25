"""
Commercial viability filter for PharmacyFinder scanner results.
Adds population density, pharmacy saturation, and viability scoring
on top of compliance-only scanner output.
"""
import sys, json, sqlite3
from geopy.distance import geodesic
from collections import defaultdict
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

DB_PATH = 'pharmacy_finder.db'
CATCHMENT_KM = 3.0  # Population catchment radius
PHARMACY_RADIUS_KM = 5.0  # Check for existing pharmacies
MIN_POP_PER_PHARMACY = 4000  # Healthy ratio
IDEAL_POP_PER_PHARMACY = 5000

def load_all_opportunities():
    """Load all non-131 results."""
    files = {
        'Item 130': 'output/item130_opportunities.json',
        'Item 132': 'output/item132_opportunities.json',
        'Item 133': 'output/item133_opportunities.json',
        'Item 136': 'output/item136_opportunities.json',
    }
    all_opps = []
    for rule, path in files.items():
        try:
            with open(path) as f:
                data = json.load(f)
            for d in data:
                d['rule_item'] = rule
                all_opps.append(d)
        except FileNotFoundError:
            pass
    return all_opps


def get_nearby_pharmacies(lat, lon, pharmacies, radius_km):
    """Count pharmacies within radius. Uses bounding box pre-filter."""
    import math
    deg_lat = radius_km / 111.0
    deg_lon = radius_km / (111.0 * max(math.cos(math.radians(lat)), 0.1))
    
    nearby = []
    for p in pharmacies:
        if abs(p['latitude'] - lat) > deg_lat or abs(p['longitude'] - lon) > deg_lon:
            continue
        d = geodesic((lat, lon), (p['latitude'], p['longitude'])).km
        if d < radius_km:
            nearby.append({'name': p['name'], 'distance_km': round(d, 2), 'address': p.get('address', '')})
    nearby.sort(key=lambda x: x['distance_km'])
    return nearby


def get_catchment_population(lat, lon, sa1_rows, radius_km, sa1_index=None):
    """Sum population within radius from SA1 centroids. Uses spatial pre-filter."""
    total = 0
    count = 0
    # Rough degree filter: 1 degree lat ~ 111km, 1 degree lon ~ 111*cos(lat) km
    import math
    deg_lat = radius_km / 111.0
    deg_lon = radius_km / (111.0 * max(math.cos(math.radians(lat)), 0.1))
    
    for s in sa1_rows:
        # Quick bounding box check first
        if abs(s['lat'] - lat) > deg_lat or abs(s['lon'] - lon) > deg_lon:
            continue
        d = geodesic((lat, lon), (s['lat'], s['lon'])).km
        if d < radius_km:
            total += s['pop']
            count += 1
    return total, count


def score_viability(pop, num_pharmacies, compliance_confidence, nearest_km):
    """Score 0-100 commercial viability."""
    score = 0
    
    # Population score (0-30)
    if pop >= 20000:
        score += 30
    elif pop >= 10000:
        score += 20 + (pop - 10000) / 1000
    elif pop >= 5000:
        score += 10 + (pop - 5000) / 500
    else:
        score += pop / 500
    
    # Saturation score (0-30) — fewer pharmacies = better
    if num_pharmacies == 0:
        score += 30
    elif num_pharmacies == 1:
        score += 25
    elif num_pharmacies == 2:
        score += 18
    elif num_pharmacies == 3:
        score += 12
    elif num_pharmacies <= 5:
        score += 5
    # 6+ = 0
    
    # Pop per pharmacy ratio (0-20)
    if num_pharmacies > 0:
        ratio = pop / num_pharmacies
        if ratio >= IDEAL_POP_PER_PHARMACY:
            score += 20
        elif ratio >= MIN_POP_PER_PHARMACY:
            score += 15
        elif ratio >= 3000:
            score += 8
        elif ratio >= 2000:
            score += 3
    else:
        score += 20  # No competition
    
    # Distance from nearest pharmacy (0-10) — more = better
    if nearest_km >= 10:
        score += 10
    elif nearest_km >= 5:
        score += 8
    elif nearest_km >= 2:
        score += 5
    elif nearest_km >= 1:
        score += 3
    else:
        score += 1
    
    # Compliance confidence boost (0-10)
    score += compliance_confidence * 10
    
    return min(round(score, 1), 100)


def main():
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row
    
    # Load reference data
    print("Loading reference data...")
    pharmacies = [dict(r) for r in db.execute('SELECT * FROM pharmacies WHERE latitude IS NOT NULL').fetchall()]
    sa1_rows = []
    for r in db.execute('SELECT SA1_CODE_2021, _lat, _lon, Tot_P_P FROM census_sa1 WHERE _lat IS NOT NULL AND _lon IS NOT NULL AND Tot_P_P > 0').fetchall():
        sa1_rows.append({'code': r['SA1_CODE_2021'], 'lat': r['_lat'], 'lon': r['_lon'], 'pop': r['Tot_P_P'] or 0})
    
    print(f"  {len(pharmacies)} pharmacies, {len(sa1_rows)} SA1 areas loaded")
    
    # Load opportunities
    opps = load_all_opportunities()
    print(f"  {len(opps)} opportunities to evaluate\n")
    
    results = []
    
    for i, opp in enumerate(opps):
        lat, lon = opp['lat'], opp['lon']
        name = opp.get('name', '?')
        
        if (i + 1) % 10 == 0:
            print(f"  Processing {i+1}/{len(opps)}...")
        
        # Get nearby pharmacies
        nearby_pharm = get_nearby_pharmacies(lat, lon, pharmacies, PHARMACY_RADIUS_KM)
        
        # Get catchment population
        pop, sa1_count = get_catchment_population(lat, lon, sa1_rows, CATCHMENT_KM)
        
        # Calculate ratios
        num_pharm = len(nearby_pharm)
        pop_per_pharm = pop / num_pharm if num_pharm > 0 else pop
        nearest_km = opp.get('nearest_pharmacy_km', 0)
        
        # Score viability
        viability = score_viability(pop, num_pharm, opp.get('confidence', 0.5), nearest_km)
        
        result = {
            'rank': 0,
            'viability_score': viability,
            'name': name,
            'rule_item': opp.get('rule_item', '?'),
            'state': opp.get('state', '?'),
            'address': opp.get('address', ''),
            'lat': lat,
            'lon': lon,
            'confidence': opp.get('confidence', 0),
            'nearest_pharmacy_km': nearest_km,
            'pharmacies_within_5km': num_pharm,
            'population_3km': pop,
            'pop_per_pharmacy': round(pop_per_pharm),
            'sa1_areas': sa1_count,
            'nearest_3_pharmacies': nearby_pharm[:3],
            'reason': opp.get('reason', ''),
            # Extra fields from source
            'effective_fte': opp.get('effective_fte', ''),
            'num_gps': opp.get('num_gps', ''),
            'hours_per_week': opp.get('hours_per_week', ''),
            'gla_sqm': opp.get('gla_sqm', ''),
        }
        results.append(result)
    
    # Sort by viability score
    results.sort(key=lambda x: -x['viability_score'])
    for i, r in enumerate(results):
        r['rank'] = i + 1
    
    # Save full results
    with open('output/viability_ranked.json', 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    # Print summary
    print("\n" + "=" * 100)
    print("COMMERCIAL VIABILITY RANKING (Items 130/132/133/136)")
    print("=" * 100)
    print(f"{'#':>3} {'Score':>5} {'Rule':<9} {'Name':<42} {'State':<4} {'Pop':>7} {'Pharm':>5} {'P/P':>6} {'Dist':>6}")
    print("-" * 100)
    
    for r in results:
        flag = ""
        if r['viability_score'] >= 60:
            flag = " ***"
        elif r['viability_score'] >= 45:
            flag = " **"
        elif r['viability_score'] >= 30:
            flag = " *"
        
        print(f"{r['rank']:>3} {r['viability_score']:>5.1f} {r['rule_item']:<9} "
              f"{r['name'][:40]:<42} {r['state']:<4} "
              f"{r['pop_per_pharmacy']:>7,} {r['pharmacies_within_5km']:>5} "
              f"{r['pop_per_pharmacy']:>6,} {r['nearest_pharmacy_km']:>6.1f}{flag}")
    
    # Top 10 detail
    print(f"\n{'=' * 100}")
    print("TOP 10 DETAILED")
    print("=" * 100)
    for r in results[:10]:
        print(f"\n#{r['rank']} — {r['name']} ({r['state']})")
        print(f"  Rule: {r['rule_item']} | Confidence: {r['confidence']}")
        print(f"  Viability Score: {r['viability_score']}/100")
        print(f"  Population (3km): {r['population_3km']:,} across {r['sa1_areas']} SA1 areas")
        print(f"  Pharmacies (5km): {r['pharmacies_within_5km']}")
        print(f"  Pop per pharmacy: {r['pop_per_pharmacy']:,}")
        print(f"  Nearest pharmacy: {r['nearest_pharmacy_km']:.1f}km")
        if r['nearest_3_pharmacies']:
            print(f"  Closest pharmacies:")
            for p in r['nearest_3_pharmacies']:
                print(f"    - {p['distance_km']:.1f}km: {p['name']}")
        if r['effective_fte']:
            print(f"  Medical centre FTE: {r['effective_fte']} | GPs: {r['num_gps']} | Hrs/wk: {r['hours_per_week']}")
        if r['gla_sqm']:
            print(f"  Supermarket GLA: {r['gla_sqm']}sqm")
    
    print(f"\nFull results saved to output/viability_ranked.json")
    db.close()


if __name__ == '__main__':
    main()
