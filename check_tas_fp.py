import os
import sqlite3, sys, io

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
conn = sqlite3.connect(os.path.join(BASE_DIR, 'pharmacy_finder.db'))
c = conn.cursor()
c.execute("SELECT poi_name, nearest_town, nearest_pharmacy_km, nearest_pharmacy_name, composite_score, pop_5km, qualifying_rules FROM opportunities WHERE region = 'TAS' AND verification = 'FALSE_POSITIVE' ORDER BY composite_score DESC")
for r in c.fetchall():
    print(f"{r[0]} ({r[1]})")
    print(f"  Score: {r[4]:.0f} | Pop 5km: {r[5]:,}")
    print(f"  DB said nearest: {r[3]} ({r[2]:.1f}km)")
    print(f"  Rules: {r[6]}")
    print()
conn.close()
