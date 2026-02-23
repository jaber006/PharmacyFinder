import sqlite3, csv

DB = r'C:\Users\MJ\Documents\GitHub\PharmacyFinder\pharmacy_finder.db'
conn = sqlite3.connect(DB)
c = conn.cursor()

c.execute("SELECT name, latitude, longitude FROM medical_centres WHERE name LIKE '%TAS Family%'")
print("DB medical_centres:")
for r in c.fetchall():
    print(f"  {r[0]}: ({r[1]}, {r[2]})")

c.execute("SELECT poi_name, latitude, longitude FROM opportunities WHERE poi_name LIKE '%TAS Family%'")
print("\nDB opportunities:")
for r in c.fetchall():
    print(f"  {r[0]}: ({r[1]}, {r[2]})")

csv_path = r'C:\Users\MJ\Documents\GitHub\PharmacyFinder\output\population_ranked_TAS.csv'
with open(csv_path, 'r', encoding='utf-8') as f:
    for row in csv.DictReader(f):
        if 'TAS Family' in row.get('POI Name', ''):
            print(f"\nCSV entry:")
            print(f"  POI Name: {row['POI Name']}")
            print(f"  Lat: {row['Latitude']}")
            print(f"  Lng: {row['Longitude']}")
            print(f"  Town: {row.get('Nearest Town', '')}")

# Also check what Google Maps said the correct coords are
print("\nGoogle Maps verified coords: (-41.062968, 145.914174)")
print("That's in Burnie area - let's check what the CSV has vs this")
conn.close()
