import sqlite3
import csv
import os

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'pharmacy_finder.db')
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')

# Google Maps exact coordinates for TAS Family Medical Centre
# From: https://google.com/maps/place/.../@-41.0627153,145.9077191
CORRECT_LAT = -41.0627153
CORRECT_LNG = 145.9077191

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

# Fix DB
c.execute("UPDATE medical_centres SET latitude = ?, longitude = ? WHERE name = 'TAS Family Medical Centre'",
          (CORRECT_LAT, CORRECT_LNG))
print(f"Updated medical_centres: {c.rowcount}")

c.execute("UPDATE opportunities SET latitude = ?, longitude = ? WHERE poi_name = 'TAS Family Medical Centre'",
          (CORRECT_LAT, CORRECT_LNG))
print(f"Updated opportunities: {c.rowcount}")

conn.commit()
conn.close()

# Fix CSV
filepath = os.path.join(OUTPUT_DIR, 'population_ranked_TAS.csv')
with open(filepath, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    fieldnames = reader.fieldnames
    rows = list(reader)

for row in rows:
    if row.get('POI Name') == 'TAS Family Medical Centre':
        print(f"CSV before: ({row['Latitude']}, {row['Longitude']})")
        row['Latitude'] = str(CORRECT_LAT)
        row['Longitude'] = str(CORRECT_LNG)
        print(f"CSV after:  ({row['Latitude']}, {row['Longitude']})")

with open(filepath, 'w', encoding='utf-8', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)

print("Done!")
