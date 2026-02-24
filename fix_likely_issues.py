"""Apply fixes from LIKELY verification results. Run AFTER coord audit finishes."""
import json, sqlite3

conn = sqlite3.connect('pharmacy_finder.db')
c = conn.cursor()

with open('output/likely_verification.json', encoding='utf-8') as f:
    data = json.load(f)

fixes = 0
# Fix state mismatches
for d in data:
    if d.get('issues'):
        for issue in d['issues']:
            if 'State mismatch' in issue:
                oid = d['id']
                # Mallacoota is actually in VIC not NSW
                if 'Mallacoota' in d.get('name', ''):
                    c.execute("UPDATE opportunities SET region='VIC' WHERE id=?", (oid,))
                    fixes += 1
                    print(f"Fixed {d['name']}: region -> VIC")
                # IGA Country Grocers Trentham is VIC
                elif 'IGA Country' in d.get('name', ''):
                    c.execute("UPDATE opportunities SET region='VIC' WHERE id=?", (oid,))
                    fixes += 1
                    print(f"Fixed {d['name']}: region -> VIC")
                # Foodworks state mismatch
                elif 'Foodworks' in d.get('name', '') or 'FoodWorks' in d.get('name', ''):
                    # Mark as needing coord fix
                    c.execute("UPDATE opportunities SET verification='NEEDS_COORDS', verification_notes=? WHERE id=?",
                              (issue, oid))
                    fixes += 1
                    print(f"Flagged {d['name']} (id={oid}): {issue}")

    # Flag unverifiable generic names
    if d.get('verified') is None and d.get('name') in ('Clinic', 'Medical Practice'):
        oid = d['id']
        c.execute("UPDATE opportunities SET verification='UNVERIFIABLE', verification_notes='Generic name, cannot verify existence' WHERE id=?", (oid,))
        fixes += 1
        print(f"Flagged generic: {d['name']} (id={oid})")

conn.commit()
conn.close()
print(f"\nTotal fixes: {fixes}")
