"""Random audit: pick 10 locations across all data types, output for manual Google Maps verification."""
import sqlite3
import os
import sys
import io
import random

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace', line_buffering=True)
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'pharmacy_finder.db')

conn = sqlite3.connect(DB_PATH)
c = conn.cursor()

# Sample from each table
samples = []

# 4 pharmacies (biggest dataset)
c.execute("SELECT name, address, latitude, longitude, state FROM pharmacies ORDER BY RANDOM() LIMIT 4")
for r in c.fetchall():
    samples.append(('Pharmacy', r[0], r[1], r[2], r[3], r[4]))

# 2 medical centres
c.execute("SELECT name, address, latitude, longitude, state FROM medical_centres ORDER BY RANDOM() LIMIT 2")
for r in c.fetchall():
    samples.append(('Medical Centre', r[0], r[1], r[2], r[3], r[4]))

# 2 supermarkets
c.execute("SELECT name, address, latitude, longitude, 'AUS' FROM supermarkets ORDER BY RANDOM() LIMIT 2")
for r in c.fetchall():
    samples.append(('Supermarket', r[0], r[1], r[2], r[3], r[4]))

# 1 hospital
c.execute("SELECT name, address, latitude, longitude, 'AUS' FROM hospitals ORDER BY RANDOM() LIMIT 1")
for r in c.fetchall():
    samples.append(('Hospital', r[0], r[1], r[2], r[3], r[4]))

# 1 shopping centre
c.execute("SELECT name, address, latitude, longitude, 'AUS' FROM shopping_centres ORDER BY RANDOM() LIMIT 1")
for r in c.fetchall():
    samples.append(('Shopping Centre', r[0], r[1], r[2], r[3], r[4]))

random.shuffle(samples)

print(f"RANDOM AUDIT - {len(samples)} locations\n")
for i, (type, name, address, lat, lng, state) in enumerate(samples, 1):
    gmaps_url = f"https://www.google.com/maps/search/{name.replace(' ', '+')},+{(address or '').replace(' ', '+')}"
    print(f"{i}. [{type}] {name}")
    print(f"   Address: {address}")
    print(f"   Coords: ({lat}, {lng})")
    print(f"   State: {state}")
    print(f"   Verify: https://www.google.com/maps/@{lat},{lng},18z")
    print()

conn.close()
