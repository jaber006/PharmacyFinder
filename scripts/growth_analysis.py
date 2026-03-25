"""
Cross-reference fastest growing Australian suburbs with pharmacy coverage.
Uses ABS 2023-24 regional population data + our pharmacy database.
"""
import sqlite3
import json
import os
from geopy.distance import geodesic

DB_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'pharmacy_finder.db')

# ABS fastest growing areas 2023-24 (from Regional Population release)
# Focus on areas with 1000+ growth and their approximate coordinates
GROWTH_AREAS = [
    # (name, state, growth_people, growth_pct, lat, lon, notes)
    ("Fraser Rise - Plumpton", "VIC", 4316, 26.3, -37.68, 144.72, "Melbourne outer west"),
    ("Rockbank - Mount Cottrell", "VIC", 4145, 15.2, -37.75, 144.65, "Melbourne outer west"),
    ("Box Hill - Nelson", "NSW", 4042, 22.0, -33.64, 150.86, "Sydney outer NW"),
    ("Clyde North - South", "VIC", 3932, 19.0, -38.12, 145.35, "Melbourne outer SE"),
    ("Marsden Park - Shanes Park", "NSW", 3497, 14.7, -33.70, 150.83, "Sydney outer NW"),
    ("Ripley", "QLD", 2700, 15.0, -27.71, 152.83, "Ipswich"),
    ("Caloundra West - Baringa", "QLD", 2500, 12.5, -26.81, 153.10, "Sunshine Coast"),
    ("Chambers Flat - Logan Reserve", "QLD", 2400, 19.0, -27.78, 153.10, "Logan"),
    ("Schofields - East", "NSW", 2700, 14.0, -33.70, 150.89, "Sydney outer NW"),
    ("Alkimos - Eglinton", "WA", 2100, 9.5, -31.63, 115.66, "Perth outer NW"),
    ("Munno Para West - Angle Vale", "SA", 2100, 11.0, -34.66, 138.65, "Adelaide outer N"),
    ("Brabham - Henley Brook", "WA", 1500, 10.0, -31.81, 115.98, "Perth outer NE"),
    ("Baldivis - North", "WA", 1500, 8.0, -32.33, 115.79, "Perth outer S"),
    ("Taylor", "ACT", 1050, 28.7, -35.19, 149.07, "Canberra outer N"),
    ("Greenbank - North Maclean", "QLD", 1500, 13.0, -27.72, 153.00, "Logan"),
    ("Austral - Greendale", "NSW", 1800, 16.0, -33.93, 150.81, "Sydney outer SW"),
    ("Mickleham - Yuroke", "VIC", 1800, 11.0, -37.53, 144.89, "Melbourne outer N"),
    ("Tarneit - North", "VIC", 2300, 20.3, -37.83, 144.65, "Melbourne outer W"),
    ("Mount Barker", "SA", 1100, 3.0, -35.07, 138.86, "Adelaide Hills"),
    ("Virginia - Waterloo Corner", "SA", 900, 14.0, -34.72, 138.57, "Adelaide outer N"),
    ("Googong", "NSW", 800, 16.0, -35.44, 149.22, "Outside ACT"),
]

def find_nearest_pharmacy(lat, lon, cursor):
    """Find nearest pharmacy to a location."""
    # Get all pharmacies within ~5km
    cursor.execute("""
        SELECT name, latitude, longitude, suburb, state 
        FROM pharmacies 
        WHERE latitude BETWEEN ? AND ? AND longitude BETWEEN ? AND ?
    """, (lat - 0.1, lat + 0.1, lon - 0.1, lon + 0.1))
    
    nearest = None
    nearest_dist = float('inf')
    
    for row in cursor.fetchall():
        if row[1] and row[2]:
            dist = geodesic((lat, lon), (row[1], row[2])).km
            if dist < nearest_dist:
                nearest_dist = dist
                nearest = row
    
    return nearest, nearest_dist

def count_pharmacies_in_radius(lat, lon, radius_km, cursor):
    """Count pharmacies within radius."""
    # Rough bounding box
    delta = radius_km / 111.0
    cursor.execute("""
        SELECT COUNT(*) FROM pharmacies 
        WHERE latitude BETWEEN ? AND ? AND longitude BETWEEN ? AND ?
    """, (lat - delta, lat + delta, lon - delta, lon + delta))
    
    rough_count = cursor.fetchone()[0]
    if rough_count == 0:
        return 0
    
    # Precise check
    cursor.execute("""
        SELECT latitude, longitude FROM pharmacies 
        WHERE latitude BETWEEN ? AND ? AND longitude BETWEEN ? AND ?
    """, (lat - delta, lat + delta, lon - delta, lon + delta))
    
    count = 0
    for row in cursor.fetchall():
        if geodesic((lat, lon), (row[0], row[1])).km <= radius_km:
            count += 1
    return count

def main():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    print("=" * 120)
    print("  FASTEST GROWING SUBURBS vs PHARMACY COVERAGE")
    print("  ABS Regional Population 2023-24 × PharmacyFinder v4 Database")
    print("=" * 120)
    print()
    
    results = []
    
    for name, state, growth, growth_pct, lat, lon, notes in GROWTH_AREAS:
        nearest, dist = find_nearest_pharmacy(lat, lon, c)
        pharm_1km = count_pharmacies_in_radius(lat, lon, 1.5, c)
        pharm_3km = count_pharmacies_in_radius(lat, lon, 3.0, c)
        pharm_5km = count_pharmacies_in_radius(lat, lon, 5.0, c)
        
        # Count GPs nearby
        c.execute("""
            SELECT COUNT(*) FROM gps 
            WHERE latitude BETWEEN ? AND ? AND longitude BETWEEN ? AND ?
        """, (lat - 0.03, lat + 0.03, lon - 0.03, lon + 0.03))
        gp_count = c.fetchone()[0]
        
        # Pharmacy density ratio (pharmacies per 10k population in 5km)
        # Approximate population in 5km based on growth area
        
        # Opportunity score
        score = 0
        if dist > 1.5: score += 30  # >1.5km to nearest = potential Item 130
        if dist > 3.0: score += 20  # >3km = strong gap
        if pharm_1km == 0: score += 20  # no pharmacy within 1.5km
        if growth > 2000: score += 15  # major growth
        if growth_pct > 15: score += 15  # rapid growth rate
        
        results.append({
            'name': name, 'state': state, 'growth': growth, 
            'growth_pct': growth_pct, 'lat': lat, 'lon': lon,
            'nearest_dist': round(dist, 1),
            'nearest_name': nearest[0] if nearest else 'None',
            'pharm_1km': pharm_1km, 'pharm_3km': pharm_3km, 'pharm_5km': pharm_5km,
            'gp_count': gp_count, 'score': score, 'notes': notes
        })
    
    # Sort by opportunity score
    results.sort(key=lambda x: x['score'], reverse=True)
    
    print(f"{'#':>2} {'Score':>5} {'Suburb':<35} {'St':>3} {'Growth':>6} {'%':>5} {'Dist':>5} {'<1.5km':>6} {'<3km':>5} {'<5km':>5} {'GPs':>4} {'Notes'}")
    print("-" * 120)
    
    for i, r in enumerate(results):
        flag = " ***" if r['score'] >= 50 else " **" if r['score'] >= 30 else ""
        print(f"{i+1:>2} {r['score']:>5} {r['name']:<35} {r['state']:>3} {r['growth']:>+5,} {r['growth_pct']:>4.1f}% {r['nearest_dist']:>4.1f}km {r['pharm_1km']:>6} {r['pharm_3km']:>5} {r['pharm_5km']:>5} {r['gp_count']:>4} {r['notes']}{flag}")
    
    print()
    print("=" * 120)
    
    # Flag opportunities
    print("\n  HIGH-POTENTIAL GROWTH AREAS (Score >= 30):")
    print("-" * 80)
    for r in results:
        if r['score'] >= 30:
            print(f"  {r['name']} ({r['state']}) — +{r['growth']:,} people ({r['growth_pct']}%)")
            print(f"    Nearest pharmacy: {r['nearest_dist']}km ({r['nearest_name'][:40]})")
            print(f"    Pharmacies within 1.5km: {r['pharm_1km']} | 3km: {r['pharm_3km']} | 5km: {r['pharm_5km']}")
            print(f"    GPs within 3km: {r['gp_count']}")
            if r['nearest_dist'] > 1.5 and r['gp_count'] > 0:
                print(f"    >>> POTENTIAL ITEM 130 OPPORTUNITY — {r['nearest_dist']}km gap + GPs nearby")
            elif r['nearest_dist'] > 1.5:
                print(f"    >>> GAP DETECTED — {r['nearest_dist']}km to nearest pharmacy, but needs GP nearby")
            else:
                print(f"    >>> WATCH — rapid growth may create pharmacy demand gap in 2-3 years")
            print()
    
    # Save
    outpath = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'output', 'growth_analysis.json')
    with open(outpath, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2)
    print(f"  Results saved to: {outpath}")
    
    conn.close()

if __name__ == '__main__':
    main()
