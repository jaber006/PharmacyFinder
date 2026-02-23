"""Quick check of supermarket/POI data in DB"""
import sqlite3

conn = sqlite3.connect('pharmacy_finder.db')
c = conn.cursor()

# List all tables
c.execute("SELECT name FROM sqlite_master WHERE type='table'")
print("=== TABLES ===")
for r in c.fetchall():
    print(r[0])

# Check POIs table
for tbl in ['pois', 'poi', 'points_of_interest', 'locations']:
    try:
        c.execute(f"SELECT COUNT(*) FROM {tbl}")
        print(f"\n{tbl}: {c.fetchone()[0]} rows")
        c.execute(f"PRAGMA table_info({tbl})")
        print(f"Columns: {[r[1] for r in c.fetchall()]}")
        c.execute(f"SELECT DISTINCT type FROM {tbl} LIMIT 20" )
        print(f"Types: {[r[0] for r in c.fetchall()]}")
    except:
        pass

# Check opportunities table for supermarket references
try:
    c.execute("PRAGMA table_info(opportunities)")
    cols = [r[1] for r in c.fetchall()]
    print(f"\nOpportunities columns: {cols}")
    supermarket_cols = [c for c in cols if 'super' in c.lower() or 'gla' in c.lower()]
    print(f"Supermarket/GLA cols: {supermarket_cols}")
    if supermarket_cols:
        for col in supermarket_cols:
            c.execute(f"SELECT {col} FROM opportunities WHERE {col} IS NOT NULL LIMIT 5")
            print(f"  {col} samples: {[r[0] for r in c.fetchall()]}")
except Exception as e:
    print(f"Error: {e}")

conn.close()
