import sqlite3, json

conn = sqlite3.connect('pharmacy_finder.db')
conn.row_factory = sqlite3.Row

# V2 results - the real engine output (619 sites)
# Get top sites by commercial score, grouped by state
print("=== TOP V2 SITES BY COMMERCIAL SCORE ===\n")
rows = conn.execute("""
    SELECT id, name, address, state, primary_rule, commercial_score, 
           best_confidence, rules_json
    FROM v2_results 
    WHERE passed_any = 1
    ORDER BY commercial_score DESC
    LIMIT 30
""").fetchall()

for r in rows:
    d = dict(r)
    rules = json.loads(d['rules_json'])
    rule_detail = rules[0] if rules else {}
    distances = rule_detail.get('distances', {})
    
    nearest_pharm = distances.get('nearest_pharmacy_m', distances.get('nearest_pharmacy_km', 'N/A'))
    nearest_name = distances.get('nearest_pharmacy_name', 'N/A')
    
    print(f"#{d['id']} | {d['name']} | {d['state']}")
    print(f"  Address: {d['address'][:80]}")
    print(f"  Rule: {d['primary_rule']} | Score: {d['commercial_score']:.3f} | Confidence: {d['best_confidence']}")
    print(f"  Nearest pharmacy: {nearest_pharm}m — {nearest_name}")
    
    # Extra details for Item 136
    if d['primary_rule'] == 'Item 136':
        mc_name = distances.get('medical_centre_name', '')
        mc_gps = distances.get('mc_num_gps', '')
        mc_fte = distances.get('mc_total_fte', '')
        mc_hrs = distances.get('mc_hours_per_week', '')
        print(f"  Medical Centre: {mc_name} | GPs: {mc_gps} | FTE: {mc_fte} | Hours/wk: {mc_hrs}")
    
    # Extra for Item 131
    if d['primary_rule'] == 'Item 131':
        route = distances.get('estimated_route_km', distances.get('nearest_pharmacy_km_geodesic', ''))
        print(f"  Route distance: {route}km")
    
    # Extra for Item 130
    if d['primary_rule'] == 'Item 130':
        print(f"  Pharmacy gap: {distances.get('nearest_pharmacy_km', 'N/A')}km")
    
    print()

# Summary by state and rule
print("=== V2 BY STATE ===")
rows = conn.execute("""
    SELECT state, COUNT(*) as cnt, AVG(commercial_score) as avg_score
    FROM v2_results WHERE passed_any = 1
    GROUP BY state ORDER BY cnt DESC
""").fetchall()
for r in rows:
    print(f"  {r[0]}: {r[1]} sites (avg score: {r[2]:.3f})")

print("\n=== V2 BY RULE ===")
rows = conn.execute("""
    SELECT primary_rule, COUNT(*) as cnt, AVG(commercial_score) as avg_score
    FROM v2_results WHERE passed_any = 1
    GROUP BY primary_rule ORDER BY cnt DESC
""").fetchall()
for r in rows:
    print(f"  {r[0]}: {r[1]} sites (avg score: {r[2]:.3f})")
