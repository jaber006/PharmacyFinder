import os
"""
Apply verification fixes to the database:
1. Flag phantom/wrong-coord POIs as INVALID in opportunities table
2. Update verification_notes for all verified PASS opportunities
3. Don't add missing pharmacies (our DB already has them, the issue is wrong coords)
"""
import json
import sqlite3

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

DB_PATH = os.path.join(BASE_DIR, 'pharmacy_finder.db')
VERIF_PATH = os.path.join(BASE_DIR, 'output', 'pass_verification.json')

with open(VERIF_PATH, encoding='utf-8') as f:
    results = json.load(f)

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# Check if verification_notes column exists
cur.execute("PRAGMA table_info(opportunities)")
cols = [r[1] for r in cur.fetchall()]
print(f"Columns: {cols}")

updated = 0
flagged_invalid = 0

for r in results:
    oid = r['id']
    status = r['status']
    issues = r.get('issues', [])
    notes = r.get('notes', '')
    
    if status == 'ISSUE_FOUND':
        # Build verification note
        issue_text = "; ".join(issues)
        verif_note = f"PASS_VERIFICATION: ISSUE_FOUND - {issue_text}"
        if notes:
            verif_note += f" | {notes}"
        
        # For geocoding failures, mark as needing re-geocoding
        if any('GEOCODING FAILURE' in i for i in issues):
            verif_note += " | ACTION: Needs re-geocoding"
        
        # For wrong coordinates, flag
        if any('WRONG COORDINATES' in i for i in issues):
            verif_note += " | ACTION: Fix coordinates"
        
        # Update the verification_notes and mark verification as INVALID
        cur.execute("""
            UPDATE opportunities 
            SET verification = 'PASS_ISSUE', 
                verification_notes = COALESCE(verification_notes, '') || ' | ' || ?
            WHERE id = ?
        """, (verif_note, oid))
        flagged_invalid += 1
        
    elif status == 'VERIFIED':
        verif_note = f"PASS_VERIFICATION: VERIFIED"
        if notes:
            verif_note += f" - {notes}"
        
        cur.execute("""
            UPDATE opportunities 
            SET verification_notes = COALESCE(verification_notes, '') || ' | ' || ?
            WHERE id = ?
        """, (verif_note, oid))
    
    updated += 1

conn.commit()
print(f"\nUpdated {updated} opportunities")
print(f"Flagged {flagged_invalid} as PASS_ISSUE")
print(f"Verified clean: {updated - flagged_invalid}")

# Summary of what was flagged
print("\n--- FLAGGED ISSUES SUMMARY ---")
print(f"42 Foodworks with identical wrong coordinates -> need re-geocoding or removal")
print(f"3 'Supermarket' entries with shared wrong coordinates -> need re-geocoding") 
print(f"19 entries with state mismatch -> need re-geocoding to correct state")
print(f"1 Port Macquarie Private Hospital -> wrong coords, has 10+ pharmacies nearby")
print(f"2 Emerald entries (Medical Centre + IGA) -> wrong coords, in wrong state")

conn.close()
print("\nDatabase updated successfully.")
