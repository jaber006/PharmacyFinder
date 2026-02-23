import sqlite3, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
conn = sqlite3.connect(r'C:\Users\MJ\Documents\GitHub\PharmacyFinder\pharmacy_finder.db')
c = conn.cursor()

# Check supermarket data for Norwood area
c.execute("SELECT name, address, floor_area_sqm, brand, latitude, longitude FROM supermarkets WHERE name LIKE '%Norwood%' OR address LIKE '%Norwood%'")
print("Supermarkets matching 'Norwood':")
for r in c.fetchall():
    print(f"  {r[0]} | {r[1]} | Floor area: {r[2]}sqm | Brand: {r[3]}")

# Check all supermarkets near Norwood Launceston (-41.46, 147.15)
print("\nSupermarkets within 3km of Norwood Launceston:")
c.execute("""SELECT name, address, floor_area_sqm, brand, latitude, longitude 
             FROM supermarkets 
             WHERE ABS(latitude - (-41.46)) < 0.03 AND ABS(longitude - 147.15) < 0.03""")
for r in c.fetchall():
    print(f"  {r[0]} | {r[1]} | Floor area: {r[2]}sqm | Brand: {r[3]}")

# Check how many supermarkets have floor area data at all
c.execute("SELECT COUNT(*) FROM supermarkets WHERE floor_area_sqm > 0")
has_area = c.fetchone()[0]
c.execute("SELECT COUNT(*) FROM supermarkets")
total = c.fetchone()[0]
print(f"\nFloor area data: {has_area}/{total} supermarkets have floor area recorded")

conn.close()
