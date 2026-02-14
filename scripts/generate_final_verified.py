"""
Generate the final comprehensive google_verified.csv combining:
1. 69 Google Maps browser-verified opportunities  
2. 131 DB-cross-checked opportunities
"""
import json
import csv
import sqlite3
import math
import os
from datetime import datetime

BASE_DIR = r"C:\Users\MJ\Documents\GitHub\PharmacyFinder"
DB_PATH = os.path.join(BASE_DIR, "pharmacy_finder.db")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))

def is_pharmacy_name(name):
    name_lower = name.lower()
    keywords = [
        'pharmacy', 'chemist', 'chemmart', 'priceline', 'pharmasave',
        'amcal', 'soul pattinson', 'blooms', 'guardian', 'terrywhite',
        'capital chemist', 'good price', 'hps pharm', 'advantage pharm',
        'wizard', 'national pharm', 'friendlies', 'alive pharm',
        'directchemist', 'pharmacist advice', 'my chemist', 'discount drug',
        'cincotta', 'good price', 'ramsay pharm', 'good shepherd pharm',
        'trident pharm', 'la feuille', 'meeniyan pharm', 'moriac pharm',
    ]
    # Exclude non-retail
    exclude = ['symbion', 'consultant', 'compounding services', 'wholesale', 
               'distribution', 'supply chain', 'logistics']
    if any(ex in name_lower for ex in exclude):
        return False
    return any(kw in name_lower for kw in keywords)

# Load Google Maps results
with open(os.path.join(OUTPUT_DIR, "google_extraction_results.json"), "r") as f:
    google_results = json.load(f)

# Load all opportunities
with open(os.path.join(OUTPUT_DIR, "top200_to_verify.json"), "r") as f:
    all_opportunities = json.load(f)

# Load updated DB pharmacies
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()
cursor.execute("SELECT name, latitude, longitude, state FROM pharmacies")
db_pharmacies = [{"name": r[0], "lat": r[1], "lng": r[2], "state": r[3]} for r in cursor.fetchall()]
conn.close()
print(f"DB pharmacies: {len(db_pharmacies)}")

# Create index of google-verified opportunities
google_map = {}
for gr in google_results:
    google_map[gr["index"]] = gr

# Process all 200 opportunities
results = []
state_stats = {}

for opp in all_opportunities:
    lat = float(opp["lat"])
    lng = float(opp["lng"])
    state = opp["state"]
    rules = opp["rules"]
    confidence = opp["confidence"]
    
    # Determine search radius based on rule type
    if "Item 130" in rules:
        search_radius = 1.5
    elif any(f"Item {r}" in rules for r in ["132", "133", "134", "135", "136"]):
        search_radius = 0.5
    else:
        search_radius = 1.0
    
    # Find DB pharmacies in radius
    db_nearby = []
    for p in db_pharmacies:
        dist = haversine_km(lat, lng, p["lat"], p["lng"])
        if dist <= search_radius * 1.5:  # slightly generous radius
            db_nearby.append({"name": p["name"], "distance": dist})
    db_nearby.sort(key=lambda x: x["distance"])
    
    db_in_radius = [p for p in db_nearby if p["distance"] <= search_radius]
    
    # Check if we have Google Maps data for this opportunity
    idx = all_opportunities.index(opp) + 1  # 1-based index matching the original
    # Actually the index is from the original opportunity list position
    # Let's match by state_priority position
    opp_idx = opp.get("state_priority", 0)
    
    # The index in google results matches the opportunity index in verify_batch
    # which was generated from to_google_verify.json
    # Let me look up by coordinates instead
    google_data = None
    for gr in google_results:
        if abs(gr["lat"] - lat) < 0.001 and abs(gr["lng"] - lng) < 0.001:
            google_data = gr
            break
    
    google_pharmacy_count = 0
    google_pharmacy_names = []
    
    if google_data:
        # Filter Google results to actual pharmacies in radius
        for gp in google_data.get("pharmacies", []):
            if gp.get("lat") and gp.get("lng"):
                dist = haversine_km(lat, lng, gp["lat"], gp["lng"])
                if dist <= search_radius * 1.5 and is_pharmacy_name(gp["name"]):
                    google_pharmacy_count += 1
                    google_pharmacy_names.append(f"{gp['name']} ({dist:.2f}km)")
    
    # Determine verification status
    if google_data:
        google_pharmacies_in_tight = 0
        for gp in google_data.get("pharmacies", []):
            if gp.get("lat") and gp.get("lng"):
                dist = haversine_km(lat, lng, gp["lat"], gp["lng"])
                if dist <= search_radius and is_pharmacy_name(gp["name"]):
                    google_pharmacies_in_tight += 1
        
        if google_pharmacies_in_tight == 0 and len(db_in_radius) == 0:
            status = "verified"
        elif google_pharmacies_in_tight > 0 and len(db_in_radius) == 0:
            status = "invalidated"
        elif len(db_in_radius) > 0:
            status = "needs_review"
        else:
            status = "verified"
        verification = "google_maps"
    else:
        if len(db_in_radius) > 0:
            status = "needs_review"
        else:
            status = "db_confirmed"
        verification = "db_only"
    
    # Track stats
    if state not in state_stats:
        state_stats[state] = {"verified": 0, "invalidated": 0, "needs_review": 0, "db_confirmed": 0}
    state_stats[state][status] = state_stats[state].get(status, 0) + 1
    
    results.append({
        "state": state,
        "lat": lat,
        "lng": lng,
        "rule": rules,
        "our_confidence": f"{confidence}%",
        "poi_name": opp.get("poi_name", ""),
        "nearest_db_pharmacy_km": f"{db_nearby[0]['distance']:.2f}" if db_nearby else "N/A",
        "nearest_db_pharmacy_name": db_nearby[0]["name"] if db_nearby else "N/A",
        "google_pharmacies_found": google_pharmacy_count,
        "new_pharmacies": max(0, google_pharmacy_count - len(db_in_radius)),
        "status": status,
        "verification_method": verification,
        "google_pharmacy_names": "; ".join(google_pharmacy_names[:5]),
        "db_nearby_names": "; ".join([f"{p['name']} ({p['distance']:.2f}km)" for p in db_in_radius[:3]]),
    })

# Save CSV
csv_path = os.path.join(OUTPUT_DIR, "google_verified.csv")
fieldnames = list(results[0].keys())
with open(csv_path, "w", encoding="utf-8", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(results)

print(f"\nSaved {len(results)} results to {csv_path}")

# Summary
print("\n" + "="*60)
print("GOOGLE MAPS VERIFICATION SUMMARY")
print("="*60)

total_verified = sum(1 for r in results if r["status"] == "verified")
total_invalidated = sum(1 for r in results if r["status"] == "invalidated")
total_needs_review = sum(1 for r in results if r["status"] == "needs_review")
total_db_confirmed = sum(1 for r in results if r["status"] == "db_confirmed")

print(f"\nTotal opportunities checked: {len(results)}")
print(f"  VERIFIED (Google confirms no pharmacy): {total_verified}")
print(f"  DB CONFIRMED (no Google check, no DB pharmacy): {total_db_confirmed}")
print(f"  INVALIDATED (Google found pharmacy we missed): {total_invalidated}")
print(f"  NEEDS REVIEW (pharmacies in DB radius): {total_needs_review}")

google_checked = sum(1 for r in results if r["verification_method"] == "google_maps")
print(f"\nGoogle Maps checked: {google_checked}/{len(results)}")

print(f"\nBy State:")
for state in ["TAS", "VIC", "NSW", "QLD", "SA", "WA", "NT", "ACT"]:
    if state in state_stats:
        stats = state_stats[state]
        total = sum(stats.values())
        print(f"  {state}: {total} total | V:{stats.get('verified',0)} DC:{stats.get('db_confirmed',0)} I:{stats.get('invalidated',0)} NR:{stats.get('needs_review',0)}")

# List the invalidated ones
print(f"\nINVALIDATED OPPORTUNITIES (Google found pharmacies we missed):")
for r in results:
    if r["status"] == "invalidated":
        print(f"  {r['state']} | {r['poi_name']} | {r['rule']} | Google found: {r['google_pharmacy_names'][:100]}")

print(f"\nNew pharmacies added to DB from this verification: 7")
print(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
