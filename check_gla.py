import sqlite3
conn = sqlite3.connect('pharmacy_finder.db')
c = conn.cursor()

c.execute("SELECT name, address, latitude, longitude, floor_area_sqm, estimated_gla, brand, gla_confidence FROM supermarkets LIMIT 10")
for r in c.fetchall():
    print(f"{r[6]:12s} | {r[0]:40s} | floor={r[4]} | est_gla={r[5]} | conf={r[7]} | ({r[2]:.4f},{r[3]:.4f})")

print("\n--- GLA Stats ---")
c.execute("SELECT COUNT(*) FROM supermarkets WHERE floor_area_sqm IS NOT NULL AND floor_area_sqm > 0")
print(f"With actual floor_area: {c.fetchone()[0]}")
c.execute("SELECT COUNT(*) FROM supermarkets WHERE estimated_gla IS NOT NULL AND estimated_gla > 0")
print(f"With estimated GLA: {c.fetchone()[0]}")
c.execute("SELECT COUNT(*) FROM supermarkets WHERE (floor_area_sqm IS NULL OR floor_area_sqm = 0) AND (estimated_gla IS NULL OR estimated_gla = 0)")
print(f"No GLA data at all: {c.fetchone()[0]}")
c.execute("SELECT COUNT(*) FROM supermarkets")
print(f"Total: {c.fetchone()[0]}")

print("\n--- By Brand ---")
c.execute("SELECT brand, COUNT(*), AVG(estimated_gla) FROM supermarkets GROUP BY brand")
for r in c.fetchall():
    print(f"  {r[0]:15s}: {r[1]} stores, avg est GLA={r[2]:.0f}" if r[2] else f"  {r[0]:15s}: {r[1]} stores, no GLA")

conn.close()
