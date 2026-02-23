import sqlite3
conn = sqlite3.connect('pharmacy_finder.db')
cur = conn.cursor()

# Check schema
cur.execute("PRAGMA table_info(medical_centres)")
print("Columns:", [r[1] for r in cur.fetchall()])

# Verify the update worked
cur.execute("SELECT * FROM medical_centres WHERE state = 'TAS'")
for r in cur.fetchall():
    print(f"  {r}")

# Now re-scan TAS Item 136
print("\nRunning TAS scan to check if Item 136 triggers...")
conn.close()
