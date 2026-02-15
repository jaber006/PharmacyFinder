"""Verify top opportunities for data quality issues."""
import sqlite3, sys, io
from math import radians, cos, sin, asin, sqrt

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

def haversine(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    a = sin((lat2-lat1)/2)**2 + cos(lat1)*cos(lat2)*sin((lon2-lon1)/2)**2
    return 2 * 6371000 * asin(sqrt(a))

DB = r'C:\Users\MJ\Documents\GitHub\PharmacyFinder\pharmacy_finder.db'
conn = sqlite3.connect(DB)
c = conn.cursor()

# Get ALL qualifying opportunities
c.execute("""SELECT id, poi_name, nearest_town, region, composite_score, pop_5km, pop_10km,
             nearest_pharmacy_km, nearest_pharmacy_name, qualifying_rules, latitude, longitude,
             pharmacy_5km
             FROM opportunities 
             WHERE qualifying_rules IS NOT NULL AND qualifying_rules != '' 
             AND qualifying_rules != 'NONE'
             ORDER BY composite_score DESC""")
opps = c.fetchall()

# Load pharmacies for verification
c.execute("SELECT name, latitude, longitude FROM pharmacies WHERE latitude IS NOT NULL")
pharmacies = [(n, la, lo) for n, la, lo in c.fetchall()]

print(f"Verifying {len(opps)} qualifying opportunities...\n")

issues = []
clean = []

for row in opps:
    opp_id, name, town, region, score, pop5, pop10, dist, nearest_ph, rules, lat, lng, ph5 = row
    
    problems = []
    
    # 1. Check coords are in Australia
    if lat and lng:
        if not (-45 < lat < -10 and 110 < lng < 155):
            problems.append(f"COORDS OUTSIDE AUSTRALIA: ({lat:.4f}, {lng:.4f})")
    else:
        problems.append("NO COORDINATES")
    
    # 2. Verify nearest pharmacy distance
    if lat and lng:
        actual_nearest = float('inf')
        actual_name = None
        for ph_name, ph_lat, ph_lng in pharmacies:
            d = haversine(lat, lng, ph_lat, ph_lng) / 1000
            if d < actual_nearest:
                actual_nearest = d
                actual_name = ph_name
        
        if dist and abs(actual_nearest - dist) > 0.5:
            problems.append(f"DISTANCE MISMATCH: DB says {dist:.1f}km, actual {actual_nearest:.1f}km to {actual_name}")
    
    # 3. Check for 0 population with Item 131 (rural should have SOME people)
    if 'Item 131' in (rules or '') and (not pop5 or pop5 == 0):
        problems.append(f"ZERO POP for rural opportunity")
    
    # 4. Check population sanity (>1M in 5km is suspicious for non-metro)
    if pop5 and pop5 > 1000000 and region not in ('NSW', 'VIC', 'QLD'):
        problems.append(f"SUSPICIOUS POP: {pop5:,} in 5km for {region}")
    
    # 5. Check for generic names with no town
    if name in ('IGA', 'Coles', 'Woolworths', 'Supermarket') and (not town or town == '' or town == 'Unknown'):
        problems.append(f"GENERIC NAME + NO TOWN")
    
    # 6. Check Item 136 has reasonable pharmacy distance (should be >= 300m)
    if 'Item 136' in (rules or '') and dist and dist < 0.3:
        problems.append(f"ITEM 136 BUT PHARMACY < 300m ({dist:.2f}km)")
    
    # 7. Check Item 130 has >= 1.5km
    if 'Item 130' in (rules or '') and dist and dist < 1.5:
        problems.append(f"ITEM 130 BUT PHARMACY < 1.5km ({dist:.2f}km)")
    
    # 8. Check Item 131 has >= ~7km straight line (proxy for 10km road)
    if 'Item 131' in (rules or '') and dist and dist < 7:
        problems.append(f"ITEM 131 BUT PHARMACY < 7km straight line ({dist:.1f}km) - may not be 10km by road")
    
    # 9. Pharmacy count sanity - if 0 pharmacies in 5km but distance < 5km, something's wrong
    if ph5 == 0 and dist and dist < 5:
        problems.append(f"0 PHARMACIES IN 5km BUT NEAREST IS {dist:.1f}km")
    
    if problems:
        issues.append((opp_id, name, town, region, score, rules, problems))
    else:
        clean.append((opp_id, name, town, region, score, dist, rules, pop5))

print(f"=== RESULTS ===")
print(f"Clean: {len(clean)}")
print(f"Issues: {len(issues)}")

print(f"\n=== ALL ISSUES ({len(issues)}) ===")
for opp_id, name, town, region, score, rules, problems in sorted(issues, key=lambda x: -x[4]):
    print(f"\n  {name} ({region}) | Town: {town} | Score: {score:.0f} | Rules: {rules}")
    for p in problems:
        print(f"    ❌ {p}")

print(f"\n=== TOP 20 CLEAN OPPORTUNITIES ===")
for i, (opp_id, name, town, region, score, dist, rules, pop5) in enumerate(clean[:20], 1):
    pop_str = f"{pop5:,}" if pop5 else "0"
    print(f"{i}. {name} ({region}) | Town: {town} | Score: {score:.0f} | {dist:.1f}km gap | Pop: {pop_str} | {rules}")

conn.close()
