import sqlite3, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
conn = sqlite3.connect(r'C:\Users\MJ\Documents\GitHub\PharmacyFinder\pharmacy_finder.db')
c = conn.cursor()
c.execute("""SELECT poi_name, nearest_town, composite_score, pop_5km, pop_10km, pop_15km, 
             nearest_pharmacy_km, nearest_pharmacy_name, pharmacy_5km, pharmacy_10km,
             competition_score, qualifying_rules, growth_indicator, address
             FROM opportunities 
             WHERE region = 'TAS' AND verification = 'VERIFIED' 
             ORDER BY composite_score DESC LIMIT 3""")
for i, r in enumerate(c.fetchall(), 1):
    name, town, score, p5, p10, p15, dist, nearest, ph5, ph10, comp, rules, growth, addr = r
    print(f"=== #{i}: {name} ===")
    print(f"Town: {town}")
    print(f"Address: {addr}")
    print(f"Score: {score:.0f}")
    print(f"Population: {p5:,} (5km) / {p10:,} (10km) / {p15:,} (15km)")
    print(f"Nearest pharmacy: {nearest} ({dist:.1f}km)")
    print(f"Pharmacies: {ph5} in 5km, {ph10} in 10km")
    print(f"Competition score: {comp}")
    print(f"Rules: {rules}")
    print(f"Growth area: {growth}")
    print()
conn.close()
