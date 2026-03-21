import sqlite3
conn = sqlite3.connect('pharmacy_finder.db')
c = conn.cursor()

print("=== MEDICAL CENTRES (5 samples) ===")
for row in c.execute("SELECT id, name, address, state, num_gps, practitioners_json FROM medical_centres LIMIT 5").fetchall():
    print(row)

print("\n=== SHOPPING CENTRES (5 samples) ===")
for row in c.execute("SELECT id, name, address, estimated_tenants, gla_sqm FROM shopping_centres LIMIT 5").fetchall():
    print(row)

print("\n=== HOSPITALS (5 samples) ===")
for row in c.execute("SELECT id, name, address, bed_count, hospital_type FROM hospitals LIMIT 5").fetchall():
    print(row)

conn.close()
