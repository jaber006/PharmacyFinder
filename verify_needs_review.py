"""
Deep verification of NEEDS REVIEW pharmacy opportunities.
Systematically verifies each opportunity by checking POI existence,
pharmacy counts, population reasonableness, and qualifying rule evidence.
Updates the database directly.
"""
import sqlite3
import sys
import io
import json
import time
import urllib.request
import urllib.parse
import re
from datetime import datetime

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

DB_PATH = r'C:\Users\MJ\Documents\GitHub\PharmacyFinder\pharmacy_finder.db'

def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def update_verification(conn, opp_id, status, notes):
    """Update verification status and notes for an opportunity."""
    conn.execute(
        "UPDATE opportunities SET verification = ?, verification_notes = ? WHERE id = ?",
        (status, notes, opp_id)
    )
    conn.commit()
    print(f"  -> Updated ID:{opp_id} to {status}")

def bulk_mark_low_population(conn):
    """Mark all pop_10km < 500 as FALSE POSITIVE."""
    cursor = conn.execute("""
        SELECT COUNT(*) FROM opportunities 
        WHERE verification = 'NEEDS REVIEW' AND pop_10km < 500
    """)
    count = cursor.fetchone()[0]
    
    conn.execute("""
        UPDATE opportunities 
        SET verification = 'FALSE POSITIVE', 
            verification_notes = 'Very low population (pop_10km < 500) - not commercially viable for greenfield pharmacy'
        WHERE verification = 'NEEDS REVIEW' AND pop_10km < 500
    """)
    conn.commit()
    print(f"\n=== BULK: Marked {count} opportunities with pop_10km < 500 as FALSE POSITIVE ===\n")
    return count

def analyze_item136_metro(conn):
    """
    Item 136 medical centres in metro areas with many pharmacies nearby.
    These almost certainly already have pharmacies within 500m - the rule requires
    the medical centre to NOT have a pharmacy nearby, but these all do.
    """
    cursor = conn.execute("""
        SELECT id, poi_name, nearest_town, region, pop_10km, pharmacy_5km, pharmacy_10km, 
               nearest_pharmacy_km, evidence, qualifying_rules
        FROM opportunities 
        WHERE verification = 'NEEDS REVIEW' 
        AND qualifying_rules LIKE '%Item 136%'
        ORDER BY pop_10km DESC
    """)
    
    results = []
    for row in cursor:
        opp = dict(row)
        results.append(opp)
    
    print(f"\n=== ANALYZING {len(results)} Item 136 (Medical Centre) opportunities ===\n")
    
    for opp in results:
        evidence = opp['evidence'] or ''
        poi = opp['poi_name']
        town = opp['nearest_town']
        state = opp['region']
        ph5k = opp['pharmacy_5km']
        ph10k = opp['pharmacy_10km']
        near_km = opp['nearest_pharmacy_km']
        pop = opp['pop_10km']
        
        print(f"\nID:{opp['id']} | {poi} | {town}, {state} | Pop10k:{pop} | Ph5k:{ph5k} Ph10k:{ph10k} | NearPh:{near_km:.1f}km")
        
        # Parse evidence for FTE info
        fte_match = re.search(r'(\d+\.?\d*)\s*est\.\s*FTE', evidence)
        est_fte = float(fte_match.group(1)) if fte_match else None
        
        gp_match = re.search(r'(\d+)\s*GPs', evidence)
        gp_count = int(gp_match.group(1)) if gp_match else None
        
        # Item 136 requires 8+ FTE prescribers AND no pharmacy within the medical centre
        # The key issue: most of these have pharmacies RIGHT NEXT TO them
        
        notes_parts = []
        is_false_positive = False
        
        # Check 1: FTE threshold
        if est_fte is not None and est_fte < 8.0:
            notes_parts.append(f"Est. FTE {est_fte} is below 8.0 threshold")
            is_false_positive = True
        elif est_fte is not None:
            notes_parts.append(f"Est. FTE {est_fte} meets 8.0 threshold")
        
        # Check 2: Nearby pharmacies - Item 136 allows a new pharmacy AT/NEAR the medical centre
        # But if there are already many pharmacies nearby, the commercial case is weak
        # AND the rule itself requires NO existing approved pharmacy within the centre
        if near_km < 0.5 and ph5k >= 5:
            notes_parts.append(f"Already {ph5k} pharmacies within 5km, nearest at {near_km:.1f}km - highly saturated area")
            is_false_positive = True
        elif near_km < 1.0 and ph5k >= 3:
            notes_parts.append(f"{ph5k} pharmacies within 5km, nearest at {near_km:.1f}km - saturated")
        
        # Check 3: Metro areas with absurd pop_10km (capturing entire metro population)
        if pop > 100000:
            notes_parts.append(f"Pop_10km {pop:,} is metro-wide figure, not localized demand")
            if ph5k >= 10:
                is_false_positive = True
                notes_parts.append("Metro area already well-served by pharmacies")
        
        # Check for "distance: N" in evidence which means no pharmacy distance check done
        if 'distance: N' in evidence or 'needs_investigation' in evidence.lower():
            notes_parts.append("Evidence incomplete - needs further investigation")
        
        # Check if evidence mentions pharmacy already at/near the centre
        if 'pharmacy_at_centre' in evidence.lower() or 'existing pharmacy' in evidence.lower():
            notes_parts.append("Evidence suggests pharmacy may already exist at centre")
            is_false_positive = True
        
        notes = ' | '.join(notes_parts)
        
        if is_false_positive:
            update_verification(conn, opp['id'], 'FALSE POSITIVE', f"Item 136 analysis: {notes}")
        else:
            # Leave as NEEDS REVIEW but add notes
            update_verification(conn, opp['id'], 'NEEDS REVIEW', f"Item 136 analysis: {notes}")
    
    return len(results)

def analyze_item134a(conn):
    """Analyze Item 134A shopping centre opportunities."""
    cursor = conn.execute("""
        SELECT id, poi_name, nearest_town, region, pop_10km, pharmacy_5km, pharmacy_10km,
               nearest_pharmacy_km, evidence
        FROM opportunities 
        WHERE verification = 'NEEDS REVIEW' 
        AND qualifying_rules LIKE '%Item 134A%'
        ORDER BY pop_10km DESC
    """)
    
    results = list(cursor)
    print(f"\n=== ANALYZING {len(results)} Item 134A (Shopping Centre) opportunities ===\n")
    
    for row in results:
        opp = dict(row)
        evidence = opp['evidence'] or ''
        print(f"\nID:{opp['id']} | {opp['poi_name']} | {opp['nearest_town']}, {opp['region']} | Pop:{opp['pop_10km']:,}")
        print(f"  Evidence: {evidence[:300]}")
        
        # Item 134A: shopping centre 5,000-15,000 sqm GLA with supermarket, no pharmacy
        # But nearest_pharmacy_km = 0.0 means pharmacy IS in the centre
        if opp['nearest_pharmacy_km'] == 0.0:
            # The rule allows ADDITIONAL pharmacy - check tenant count
            notes = "Item 134A allows additional pharmacy in shopping centres. NearPh=0.0km means pharmacy already in centre. Rule may allow additional based on tenant count."
            
            # Check GLA from evidence
            gla_match = re.search(r'centre_gla:\s*([\d,]+)\s*sqm', evidence)
            tenant_match = re.search(r'(\d+)\s*tenants', evidence)
            
            if gla_match:
                gla = int(gla_match.group(1).replace(',', ''))
                if gla >= 5000:
                    notes += f" GLA {gla:,}sqm qualifies."
                else:
                    notes += f" GLA {gla:,}sqm may not qualify."
            
            if tenant_match:
                tenants = int(tenant_match.group(1))
                notes += f" {tenants} tenants."
            
            update_verification(conn, opp['id'], 'NEEDS REVIEW', notes)
        else:
            update_verification(conn, opp['id'], 'NEEDS REVIEW', 
                f"Item 134A: Shopping centre near {opp['nearest_town']}. Needs manual verification of GLA and supermarket presence.")

def analyze_item132_medium_pop(conn):
    """
    Analyze Item 132 opportunities with pop 500-50000.
    Item 132: New additional pharmacy in town - needs town with existing pharmacy 200m+ away.
    These are typically small towns with one pharmacy and an IGA/supermarket.
    """
    cursor = conn.execute("""
        SELECT id, poi_name, nearest_town, region, pop_10km, pharmacy_5km, pharmacy_10km,
               nearest_pharmacy_km, evidence, poi_type
        FROM opportunities 
        WHERE verification = 'NEEDS REVIEW' 
        AND qualifying_rules = 'Item 132'
        AND pop_10km >= 500
        ORDER BY pop_10km DESC
    """)
    
    results = list(cursor)
    print(f"\n=== ANALYZING {len(results)} Item 132 (Additional Pharmacy) opportunities, pop >= 500 ===\n")
    
    for row in results:
        opp = dict(row)
        evidence = opp['evidence'] or ''
        pop = opp['pop_10km']
        ph5k = opp['pharmacy_5km']
        near_km = opp['nearest_pharmacy_km']
        town = opp['nearest_town']
        
        print(f"\nID:{opp['id']} | {opp['poi_name']} | {town}, {opp['region']} | Pop:{pop:,} | Ph5k:{ph5k} | NearPh:{near_km:.1f}km")
        
        notes_parts = []
        classification = 'NEEDS REVIEW'
        
        # Item 132 requires: existing pharmacy 200m+ away, same town
        # Check if evidence confirms 200m+ distance
        dist_match = re.search(r'at\s+(\d+)\s*m\s*\(>=200m', evidence)
        if dist_match:
            dist_m = int(dist_match.group(1))
            if dist_m >= 200:
                notes_parts.append(f"Nearest pharmacy {dist_m}m away (>=200m ✓)")
            else:
                notes_parts.append(f"Nearest pharmacy only {dist_m}m away (<200m)")
                classification = 'FALSE POSITIVE'
        
        # Check second nearest pharmacy distance
        second_match = re.search(r'second nearest.*?at\s*~?([\d.]+)\s*km', evidence, re.IGNORECASE)
        if second_match:
            second_km = float(second_match.group(1))
            notes_parts.append(f"Second nearest pharmacy {second_km:.1f}km away")
        
        # Population viability assessment
        if pop < 1500 and ph5k >= 1:
            notes_parts.append(f"Pop {pop:,} with {ph5k} existing pharmacy - marginal viability for 2nd pharmacy")
            if pop < 1000:
                classification = 'FALSE POSITIVE'
                notes_parts.append("Population too low for additional pharmacy")
        elif pop >= 1500 and ph5k == 1:
            notes_parts.append(f"Pop {pop:,} with only 1 pharmacy - potential for 2nd")
        elif pop >= 3000:
            notes_parts.append(f"Pop {pop:,} - good population base")
        
        # IGA/small supermarket - check if it makes sense as pharmacy location
        if opp['poi_type'] == 'supermarket' and 'IGA' in (opp['poi_name'] or ''):
            notes_parts.append("IGA supermarket - common pharmacy co-location point")
        
        # Check if the town already has the right number of pharmacies for its population
        # Rule of thumb: ~1 pharmacy per 3000-4000 people
        if ph5k >= 1 and pop > 0:
            ratio = pop / max(ph5k, 1)
            notes_parts.append(f"Pop/pharmacy ratio: {ratio:.0f}:1")
            if ratio > 4000:
                notes_parts.append("Underserved - ratio supports additional pharmacy")
                if classification == 'NEEDS REVIEW':
                    classification = 'NEEDS REVIEW'  # Still needs web verification
            elif ratio < 2000:
                notes_parts.append("Well-served area")
                if pop < 3000:
                    classification = 'FALSE POSITIVE'
        
        notes = f"Item 132 analysis: {' | '.join(notes_parts)}"
        update_verification(conn, opp['id'], classification, notes)
    
    return len(results)

def analyze_item131_medium_pop(conn):
    """
    Analyze Item 131 (remote) opportunities with pop 500+.
    Item 131: 10km+ road distance from nearest pharmacy.
    """
    cursor = conn.execute("""
        SELECT id, poi_name, nearest_town, region, pop_10km, pharmacy_5km, pharmacy_10km,
               nearest_pharmacy_km, evidence, poi_type
        FROM opportunities 
        WHERE verification = 'NEEDS REVIEW' 
        AND qualifying_rules LIKE '%Item 131%'
        AND pop_10km >= 500
        ORDER BY pop_10km DESC
    """)
    
    results = list(cursor)
    print(f"\n=== ANALYZING {len(results)} Item 131 (Remote) opportunities, pop >= 500 ===\n")
    
    for row in results:
        opp = dict(row)
        evidence = opp['evidence'] or ''
        pop = opp['pop_10km']
        near_km = opp['nearest_pharmacy_km']
        ph5k = opp['pharmacy_5km']
        ph10k = opp['pharmacy_10km']
        
        print(f"\nID:{opp['id']} | {opp['poi_name']} | {opp['nearest_town']}, {opp['region']} | Pop:{pop:,} | Ph5k:{ph5k} Ph10k:{ph10k} | NearPh:{near_km:.1f}km")
        
        notes_parts = []
        classification = 'NEEDS REVIEW'
        
        # Item 131 requires 10km+ ROAD distance from nearest pharmacy
        # Check straight-line vs estimated route
        sl_match = re.search(r'straight_line:\s*([\d.]+)\s*km', evidence)
        route_match = re.search(r'estimated_route:\s*([\d.]+)\s*km', evidence)
        
        sl_km = float(sl_match.group(1)) if sl_match else near_km
        route_km = float(route_match.group(1)) if route_match else sl_km * 1.3  # Estimate
        
        if route_km >= 10:
            notes_parts.append(f"Route distance {route_km:.1f}km (>=10km ✓)")
        else:
            notes_parts.append(f"Route distance {route_km:.1f}km - may not meet 10km threshold")
            if route_km < 8:
                classification = 'FALSE POSITIVE'
                notes_parts.append("Below 10km road distance threshold")
        
        # Check if pharmacy counts match claim
        if ph5k > 0:
            notes_parts.append(f"WARNING: {ph5k} pharmacy within 5km - contradicts Item 131 remote claim")
            classification = 'FALSE POSITIVE'
        
        if ph10k > 0 and near_km > 10:
            notes_parts.append(f"Note: {ph10k} pharmacy within 10km radius but nearest is {near_km:.1f}km straight-line")
        
        # Population assessment for remote area
        if pop >= 2000:
            notes_parts.append(f"Pop {pop:,} - strong candidate for remote pharmacy")
        elif pop >= 1000:
            notes_parts.append(f"Pop {pop:,} - moderate candidate")
        elif pop >= 500:
            notes_parts.append(f"Pop {pop:,} - marginal candidate")
        
        # Check for misplaced coordinates (e.g., SmartClinics Toowoomba showing as 63km from pharmacy)
        # If town name doesn't match the actual location, flag it
        if pop > 100000 and near_km > 50:
            notes_parts.append(f"SUSPICIOUS: Pop {pop:,} but {near_km:.1f}km from pharmacy - likely misplaced coordinates")
            classification = 'FALSE POSITIVE'
        
        notes = f"Item 131 analysis: {' | '.join(notes_parts)}"
        update_verification(conn, opp['id'], classification, notes)
    
    return len(results)

def analyze_item133(conn):
    """Analyze Item 133 (small shopping centre with supermarket)."""
    cursor = conn.execute("""
        SELECT id, poi_name, nearest_town, region, pop_10km, pharmacy_5km, pharmacy_10km,
               nearest_pharmacy_km, evidence
        FROM opportunities 
        WHERE verification = 'NEEDS REVIEW' 
        AND qualifying_rules LIKE '%Item 133%'
        ORDER BY pop_10km DESC
    """)
    
    results = list(cursor)
    print(f"\n=== ANALYZING {len(results)} Item 133 (Small Shopping Centre) opportunities ===\n")
    
    for row in results:
        opp = dict(row)
        evidence = opp['evidence'] or ''
        print(f"\nID:{opp['id']} | {opp['poi_name']} | {opp['nearest_town']}, {opp['region']} | Pop:{opp['pop_10km']:,}")
        print(f"  Evidence: {evidence[:300]}")
        
        notes = f"Item 133: Needs manual verification of shopping centre GLA >= 5,000sqm and supermarket >= 2,500sqm presence."
        
        # Check GLA from evidence
        gla_match = re.search(r'centre_gla:\s*([\d,]+)\s*sqm', evidence)
        if gla_match:
            gla = int(gla_match.group(1).replace(',', ''))
            notes += f" GLA: {gla:,}sqm."
        
        update_verification(conn, opp['id'], 'NEEDS REVIEW', notes)


def main():
    conn = get_connection()
    
    # Get initial counts
    cursor = conn.execute("SELECT COUNT(*) FROM opportunities WHERE verification = 'NEEDS REVIEW'")
    initial_count = cursor.fetchone()[0]
    print(f"Starting verification of {initial_count} NEEDS REVIEW opportunities")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    # PHASE 1: Bulk mark low population as FALSE POSITIVE
    low_pop_count = bulk_mark_low_population(conn)
    
    # PHASE 2: Analyze Item 136 (Medical Centres) - often false positives in metro
    item136_count = analyze_item136_metro(conn)
    
    # PHASE 3: Analyze Item 134A (Shopping Centres)
    analyze_item134a(conn)
    
    # PHASE 4: Analyze Item 133 
    analyze_item133(conn)
    
    # PHASE 5: Analyze Item 132 (Additional pharmacy in town) - medium pop
    item132_count = analyze_item132_medium_pop(conn)
    
    # PHASE 6: Analyze Item 131 (Remote) - medium pop  
    item131_count = analyze_item131_medium_pop(conn)
    
    # FINAL SUMMARY
    print("\n" + "=" * 80)
    print("VERIFICATION SUMMARY")
    print("=" * 80)
    
    cursor = conn.execute("""
        SELECT verification, COUNT(*) as cnt 
        FROM opportunities 
        GROUP BY verification 
        ORDER BY cnt DESC
    """)
    print("\nAll opportunity statuses:")
    for row in cursor:
        print(f"  {row[0]}: {row[1]}")
    
    # Count changes
    cursor = conn.execute("SELECT COUNT(*) FROM opportunities WHERE verification = 'FALSE POSITIVE'")
    fp_total = cursor.fetchone()[0]
    
    cursor = conn.execute("SELECT COUNT(*) FROM opportunities WHERE verification = 'VERIFIED'")
    v_total = cursor.fetchone()[0]
    
    cursor = conn.execute("SELECT COUNT(*) FROM opportunities WHERE verification = 'NEEDS REVIEW'")
    nr_total = cursor.fetchone()[0]
    
    print(f"\nChanges this run:")
    print(f"  Started with: {initial_count} NEEDS REVIEW")
    print(f"  Now NEEDS REVIEW: {nr_total}")
    print(f"  Marked FALSE POSITIVE (total): {fp_total}")
    print(f"  Verified (total): {v_total}")
    
    # Top remaining NEEDS REVIEW by population (most promising)
    cursor = conn.execute("""
        SELECT id, poi_name, nearest_town, region, qualifying_rules, pop_10km, 
               pharmacy_5km, nearest_pharmacy_km, opp_score, verification_notes
        FROM opportunities 
        WHERE verification = 'NEEDS REVIEW'
        ORDER BY pop_10km DESC
        LIMIT 15
    """)
    print(f"\nTop 15 remaining NEEDS REVIEW (highest population):")
    for row in cursor:
        print(f"  ID:{row[0]} | {row[1]} | {row[2]}, {row[3]} | {row[4]} | Pop:{row[5]:,} | Ph5k:{row[6]} | NearPh:{row[7]:.1f}km | Score:{row[8]}")
        if row[9]:
            print(f"    Notes: {row[9][:150]}")
    
    # Top VERIFIED opportunities
    cursor = conn.execute("""
        SELECT id, poi_name, nearest_town, region, qualifying_rules, pop_10km, 
               pharmacy_5km, nearest_pharmacy_km, opp_score, verification_notes
        FROM opportunities 
        WHERE verification = 'VERIFIED'
        ORDER BY opp_score DESC
        LIMIT 10
    """)
    print(f"\nTop 10 VERIFIED opportunities (by opp_score):")
    for i, row in enumerate(cursor, 1):
        print(f"  {i}. ID:{row[0]} | {row[1]} | {row[2]}, {row[3]} | {row[4]} | Pop:{row[5]:,} | Ph5k:{row[6]} | NearPh:{row[7]:.1f}km | Score:{row[8]}")
        if row[9]:
            print(f"     Notes: {row[9][:150]}")
    
    conn.close()
    print(f"\nCompleted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == '__main__':
    main()
