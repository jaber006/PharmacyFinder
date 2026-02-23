import sqlite3, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
conn = sqlite3.connect(r'C:\Users\MJ\Documents\GitHub\PharmacyFinder\pharmacy_finder.db')
c = conn.cursor()
c.execute("SELECT poi_name, nearest_town, composite_score, nearest_pharmacy_km, nearest_pharmacy_name FROM opportunities WHERE region = 'TAS' AND verification = 'UNVERIFIED' ORDER BY composite_score DESC")
for i, r in enumerate(c.fetchall(), 1):
    print(f"{i}. {r[0]} | Town: '{r[1]}' | Score: {r[2]:.0f} | Nearest: {r[4]} ({r[3]:.1f}km)")
conn.close()
