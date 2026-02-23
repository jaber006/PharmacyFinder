import sqlite3, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
conn = sqlite3.connect(r'C:\Users\MJ\Documents\GitHub\PharmacyFinder\pharmacy_finder.db')
c = conn.cursor()
c.execute("""SELECT poi_name, nearest_town, composite_score, pop_5km, nearest_pharmacy_km, 
             nearest_pharmacy_name, qualifying_rules, address
             FROM opportunities 
             WHERE region = 'TAS' AND verification = 'VERIFIED' 
             AND poi_name NOT LIKE '%Norwood%'
             ORDER BY composite_score DESC""")
for i, r in enumerate(c.fetchall(), 1):
    name, town, score, pop, dist, nearest, rules, addr = r
    print(f"{i}. {name}")
    print(f"   Town: {town} | Score: {score:.0f}")
    print(f"   Pop (5km): {pop:,} | Nearest pharmacy: {nearest} ({dist:.1f}km)")
    print(f"   Rules: {rules}")
    print(f"   Address: {addr}")
    print()
conn.close()
