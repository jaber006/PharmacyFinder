#!/usr/bin/env python3
"""
Apply pharmacy validation results to the database.

Reads pharmacy_validation.json and updates the opportunities table:
- Towns WITH pharmacy where DB says nearest > 5km → mark as FALSE_POSITIVE
- Towns WITHOUT pharmacy → mark as VERIFIED
- Towns WITH pharmacy where nearest < 5km → already accounted for, mark VERIFIED
"""

import json
import sqlite3
import os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'pharmacy_finder.db')
VALIDATION_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output', 'pharmacy_validation.json')

def apply_validation():
    # Load validation results
    with open(VALIDATION_PATH) as f:
        validation = json.load(f)
    
    print(f"Loaded {len(validation)} validation results")
    has_pharmacy = sum(1 for v in validation.values() if v is True)
    no_pharmacy = sum(1 for v in validation.values() if v is False)
    print(f"  Has pharmacy: {has_pharmacy}")
    print(f"  No pharmacy: {no_pharmacy}")
    
    # Connect to DB
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    
    # Check if verification column exists, add notes column if needed
    c.execute("PRAGMA table_info(opportunities)")
    cols = [col[1] for col in c.fetchall()]
    if 'verification_notes' not in cols:
        c.execute("ALTER TABLE opportunities ADD COLUMN verification_notes TEXT DEFAULT ''")
        print("Added verification_notes column")
    
    # Get all opportunities
    c.execute("""SELECT id, nearest_town, region, nearest_pharmacy_km, poi_name, verification
                 FROM opportunities""")
    opportunities = c.fetchall()
    print(f"\nTotal opportunities in DB: {len(opportunities)}")
    
    # Apply validation
    false_positive_count = 0
    verified_count = 0
    unchanged_count = 0
    
    for opp in opportunities:
        opp_id = opp['id']
        town = opp['nearest_town'] or ''
        state = opp['region'] or ''
        pharm_dist = opp['nearest_pharmacy_km'] or 0
        
        # Build the key to lookup in validation
        key = f"{town}_{state}"
        
        if key in validation:
            pharmacy_exists = validation[key]
            
            if pharmacy_exists and pharm_dist > 5.0:
                # Town has a pharmacy but our data says nearest is > 5km
                # This means our pharmacy database is incomplete for this area
                # The opportunity is a FALSE_POSITIVE — a pharmacy already serves this town
                c.execute("""UPDATE opportunities 
                           SET verification = 'FALSE_POSITIVE',
                               verification_notes = ?
                           WHERE id = ?""",
                         (f"Pharmacy exists in {town} ({state}) but DB shows nearest at {pharm_dist:.1f}km - pharmacy DB incomplete",
                          opp_id))
                false_positive_count += 1
                
            elif pharmacy_exists and pharm_dist <= 5.0:
                # Town has pharmacy and our data already accounts for it
                # This is a legitimate opportunity (pharmacy exists but there may be room for another)
                c.execute("""UPDATE opportunities 
                           SET verification = 'VERIFIED',
                               verification_notes = ?
                           WHERE id = ?""",
                         (f"Verified: pharmacy exists within {pharm_dist:.1f}km, opportunity valid",
                          opp_id))
                verified_count += 1
                
            elif not pharmacy_exists:
                # No pharmacy in town - genuine underserved area
                c.execute("""UPDATE opportunities 
                           SET verification = 'VERIFIED',
                               verification_notes = ?
                           WHERE id = ?""",
                         (f"Verified: no pharmacy in {town} ({state}), nearest at {pharm_dist:.1f}km",
                          opp_id))
                verified_count += 1
        else:
            # Town not in validation data (likely "Unknown" towns)
            unchanged_count += 1
    
    conn.commit()
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"VALIDATION RESULTS APPLIED")
    print(f"{'='*60}")
    print(f"  FALSE_POSITIVE: {false_positive_count}")
    print(f"  VERIFIED: {verified_count}")
    print(f"  UNCHANGED: {unchanged_count}")
    print(f"  Total: {false_positive_count + verified_count + unchanged_count}")
    
    # Verify the update
    c.execute("SELECT verification, COUNT(*) FROM opportunities GROUP BY verification")
    print(f"\nDB verification breakdown:")
    for row in c.fetchall():
        print(f"  {row[0]}: {row[1]}")
    
    # Also update the data.json file for consistency
    data_json_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output', 'data.json')
    if os.path.exists(data_json_path):
        with open(data_json_path) as f:
            data = json.load(f)
        
        updated = 0
        for item in data:
            town = item.get('nearTown', '')
            state = item.get('state', '')
            key = f"{town}_{state}"
            pharm_dist = item.get('nearPharmKm', 0)
            
            if key in validation:
                pharmacy_exists = validation[key]
                if pharmacy_exists and pharm_dist > 5.0:
                    item['verification'] = 'FALSE_POSITIVE'
                    item['verificationNotes'] = f"Pharmacy exists in {town} but DB shows nearest at {pharm_dist:.1f}km"
                    updated += 1
                elif not pharmacy_exists:
                    item['verification'] = 'VERIFIED'
                    item['verificationNotes'] = f"No pharmacy in {town}, nearest at {pharm_dist:.1f}km"
                    updated += 1
                else:
                    item['verification'] = 'VERIFIED'
                    item['verificationNotes'] = f"Pharmacy within {pharm_dist:.1f}km, opportunity valid"
                    updated += 1
        
        with open(data_json_path, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"\nUpdated {updated} entries in data.json")
    
    conn.close()
    print(f"\nTimestamp: {datetime.now().isoformat()}")

if __name__ == '__main__':
    apply_validation()
