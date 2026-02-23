import sqlite3, sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

conn = sqlite3.connect(r'C:\Users\MJ\Documents\GitHub\PharmacyFinder\pharmacy_finder.db')
c = conn.cursor()

# Correct coordinates: 1-3 Reeves Street, South Burnie
# From Nominatim: -41.0637, 145.9123
# Current (wrong): -41.068, 145.905

new_lat = -41.0637
new_lng = 145.9123

# Fix medical centre
c.execute("""UPDATE medical_centres 
    SET latitude = ?, longitude = ?, address = '1-3 Reeves Street, South Burnie TAS 7320'
    WHERE name = 'TAS Family Medical Centre'""", (new_lat, new_lng))
print(f"Updated medical_centres: {c.rowcount} row(s)")

# Fix the opportunity that references it
c.execute("""UPDATE opportunities 
    SET latitude = ?, longitude = ?, address = '1-3 Reeves Street, South Burnie, Burnie, City of Burnie, Tasmania, 7320, Australia'
    WHERE id = 9381""", (new_lat, new_lng))
print(f"Updated opportunities: {c.rowcount} row(s)")

conn.commit()

# Verify
c.execute("SELECT name, address, latitude, longitude FROM medical_centres WHERE name = 'TAS Family Medical Centre'")
print(f"\nVerify medical centre: {c.fetchone()}")

c.execute("SELECT id, address, latitude, longitude FROM opportunities WHERE id = 9381")
print(f"Verify opportunity: {c.fetchone()}")

conn.close()
print("\nDone! Coordinates fixed.")
