"""
Check TAS Family Medical Centre in Burnie for Item 136 eligibility.
"""
import sqlite3
from utils.database import Database
from utils.distance import haversine_distance, find_nearest, find_within_radius, format_distance


def check_burnie():
    db = Database('pharmacy_finder.db')
    db.connect()
    
    # 1. Get TAS Family Medical Centre data
    cursor = db.connection.cursor()
    cursor.execute("SELECT * FROM medical_centres WHERE name LIKE '%TAS Family%' OR name LIKE '%Burnie%'")
    centres = cursor.fetchall()
    cols = [d[0] for d in cursor.description]
    
    print("=" * 70)
    print("ITEM 136 ANALYSIS: BURNIE, TASMANIA")
    print("=" * 70)
    
    if not centres:
        print("\nERROR: TAS Family Medical Centre not found in database!")
        print("Run the medical centre scraper first.")
        db.close()
        return
    
    for row in centres:
        centre = dict(zip(cols, row))
        name = centre['name']
        lat = centre['latitude']
        lon = centre['longitude']
        num_gps = centre['num_gps']
        total_fte = centre['total_fte']
        hours = centre.get('hours_per_week', 0) or 0
        source = centre.get('source', '')
        
        print(f"\n{'─' * 50}")
        print(f"Medical Centre: {name}")
        print(f"Address: {centre['address']}")
        print(f"Coordinates: {lat:.4f}, {lon:.4f}")
        print(f"Number of GPs: {num_gps}")
        print(f"Estimated FTE: {total_fte:.1f}")
        print(f"Hours/week: {hours}")
        print(f"Data source: {source}")
        
        # 2. Check nearest pharmacies
        pharmacies = db.get_all_pharmacies()
        nearby = find_within_radius(lat, lon, pharmacies, 2.0)  # Within 2km
        
        print(f"\n  Pharmacies within 2km:")
        for pharm, dist in nearby[:10]:
            print(f"    {format_distance(dist):>8s} — {pharm.get('name', 'Unknown')}")
            print(f"              {pharm.get('address', '')}")
        
        nearest_pharm, nearest_dist = find_nearest(lat, lon, pharmacies)
        
        # 3. Check pharmacy within medical centre (100m)
        pharmacy_in_centre = any(d <= 0.1 for _, d in nearby)
        
        # 4. Check 300m rule
        passes_300m = nearest_dist >= 0.3 if nearest_dist else True
        
        # 5. Check FTE threshold
        estimated_fte = total_fte if total_fte > 0 else num_gps * 0.8
        passes_fte = estimated_fte >= 8.0 or num_gps >= 8
        
        # 6. Check hours (if known)
        passes_hours = hours >= 70 if hours > 0 else None
        
        print(f"\n  Item 136 Eligibility Check:")
        print(f"  ┌─────────────────────────────────────────────────┐")
        print(f"  │ 8+ FTE prescribers:        {'✅ YES' if passes_fte else '❌ NO':>10s}  ({num_gps} GPs, {estimated_fte:.1f} FTE)")
        print(f"  │ No pharmacy in centre:      {'✅ YES' if not pharmacy_in_centre else '❌ NO':>10s}")
        print(f"  │ Nearest pharmacy >= 300m:   {'✅ YES' if passes_300m else '❌ NO':>10s}  ({format_distance(nearest_dist)} to {nearest_pharm.get('name', 'Unknown') if nearest_pharm else 'N/A'})")
        if passes_hours is not None:
            print(f"  │ Open 70+ hrs/week:          {'✅ YES' if passes_hours else '❌ NO':>10s}  ({hours:.0f} hrs/week)")
        else:
            print(f"  │ Open 70+ hrs/week:          {'⚠️ UNKNOWN':>10s}  (needs verification)")
        print(f"  └─────────────────────────────────────────────────┘")
        
        # Overall verdict
        qualifies = passes_fte and not pharmacy_in_centre and passes_300m
        if qualifies:
            if passes_hours is None or passes_hours:
                print(f"\n  🏆 VERDICT: LIKELY QUALIFIES under Item 136!")
                print(f"     → This is an opportunity for a new pharmacy")
            else:
                print(f"\n  ⚠️  VERDICT: MEETS GP THRESHOLD but hours insufficient ({hours}hrs < 70hrs)")
        else:
            reasons = []
            if not passes_fte:
                reasons.append(f"only {num_gps} GPs ({estimated_fte:.1f} FTE, need 8.0)")
            if pharmacy_in_centre:
                reasons.append("pharmacy already in centre")
            if not passes_300m:
                reasons.append(f"nearest pharmacy only {format_distance(nearest_dist)} away (need 300m)")
            print(f"\n  ❌ VERDICT: DOES NOT QUALIFY")
            print(f"     Reasons: {'; '.join(reasons)}")
    
    # Also show all TAS medical centres
    cursor.execute("SELECT name, address, num_gps, total_fte, hours_per_week FROM medical_centres WHERE state = 'TAS' ORDER BY num_gps DESC")
    all_tas = cursor.fetchall()
    
    print(f"\n{'=' * 70}")
    print(f"ALL TASMANIA MEDICAL CENTRES IN DATABASE ({len(all_tas)} total)")
    print(f"{'=' * 70}")
    for name, address, gps, fte, hours in all_tas:
        flag = " ⭐" if gps >= 8 else ""
        print(f"  {gps:>2d} GPs ({fte:.1f} FTE) — {name}{flag}")
        print(f"              {address}")
    
    db.close()


if __name__ == '__main__':
    check_burnie()
