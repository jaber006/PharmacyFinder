import sqlite3, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
conn = sqlite3.connect(r'C:\Users\MJ\Documents\GitHub\PharmacyFinder\pharmacy_finder.db')
c = conn.cursor()

c.execute("""SELECT poi_name, nearest_town, composite_score, pop_5km, nearest_pharmacy_km, 
             nearest_pharmacy_name, qualifying_rules, verification
             FROM opportunities 
             WHERE region = 'TAS' AND qualifying_rules IS NOT NULL AND qualifying_rules != ''
             ORDER BY composite_score DESC""")
rows = c.fetchall()
print(f"TAS Opportunities: {len(rows)}\n")
for i, (name, town, score, pop, dist, nearest, rules, verif) in enumerate(rows, 1):
    pop_str = f"{pop:,}" if pop else "0"
    print(f"{i}. {name}")
    print(f"   Town: {town} | Score: {score:.0f} | Pop 5km: {pop_str}")
    print(f"   Nearest pharmacy: {nearest} ({dist:.2f}km)")
    print(f"   Rules: {rules} | Status: {verif}")
    print()
conn.close()
