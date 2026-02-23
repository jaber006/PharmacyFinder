import sqlite3
conn = sqlite3.connect(r'C:\Users\MJ\Documents\GitHub\PharmacyFinder\pharmacy_finder.db')
c = conn.cursor()
c.execute("UPDATE opportunities SET verification = 'UNVERIFIED'")
# Reset ALL to unverified
print(f'Reset {c.rowcount} entries to UNVERIFIED')
conn.commit()
conn.close()
