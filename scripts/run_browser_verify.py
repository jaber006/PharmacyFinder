"""
Process the browser verification results.
This script reads the extraction results and compares against our database.
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
    """Check if a name sounds like a pharmacy."""
    name_lower = name.lower()
    pharmacy_keywords = [
        'pharmacy', 'chemist', 'chemmart', 'priceline', 'pharmasave',
        'amcal', 'soul pattinson', 'blooms', 'guardian', 'terrywhite',
        'capital chemist', 'good price', 'hps pharm', 'advantage pharm',
        'wizard', 'national pharm', 'friendlies', 'alive pharm',
        'directchemist', 'pharmacist advice', 'my chemist', 'discount drug',
        'cincotta', 'good price', 'ramsay pharm'
    ]
    return any(kw in name_lower for kw in pharmacy_keywords)

def load_db_pharmacies():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT id, name, latitude, longitude, address, state FROM pharmacies")
    pharmacies = []
    for row in cursor.fetchall():
        pharmacies.append({
            "id": row[0], "name": row[1], "lat": row[2], "lng": row[3],
            "address": row[4], "state": row[5],
        })
    conn.close()
    return pharmacies

def find_matching_pharmacy(google_name, google_lat, google_lng, db_pharmacies, max_dist_km=1.0):
    """Find if a Google Maps pharmacy matches one in our database."""
    best_match = None
    best_dist = max_dist_km
    
    for p in db_pharmacies:
        dist = haversine_km(google_lat, google_lng, p["lat"], p["lng"])
        if dist > max_dist_km:
            continue
        
        # Check name similarity
        g_name = google_name.lower()
        d_name = p["name"].lower()
        
        # Direct substring match
        if g_name in d_name or d_name in g_name:
            return p, dist
        
        # Word overlap
        g_words = set(g_name.split()) - {'the', 'and', '&', '-', 'pharmacy', 'chemist'}
        d_words = set(d_name.split()) - {'the', 'and', '&', '-', 'pharmacy', 'chemist'}
        if g_words and d_words:
            overlap = len(g_words & d_words)
            if overlap >= 2 or (overlap >= 1 and max(len(g_words), len(d_words)) <= 3):
                if dist < best_dist:
                    best_match = p
                    best_dist = dist
        
        # Very close location match (within 50m) for any pharmacy
        if dist < 0.05:
            if dist < best_dist:
                best_match = p
                best_dist = dist
    
    return best_match, best_dist if best_match else None

def process_results(google_results_path):
    """Process Google Maps extraction results."""
    with open(google_results_path, "r", encoding="utf-8") as f:
        google_results = json.load(f)
    
    db_pharmacies = load_db_pharmacies()
    print(f"DB has {len(db_pharmacies)} pharmacies")
    print(f"Processing {len(google_results)} Google Maps verifications")
    
    # Load original verification data
    with open(os.path.join(OUTPUT_DIR, "to_google_verify.json"), "r", encoding="utf-8") as f:
        to_verify = json.load(f)
    
    verify_map = {v["index"]: v for v in to_verify}
    
    all_results = []
    new_pharmacies = []
    
    for result in google_results:
        idx = result["index"]
        opp = verify_map.get(idx, {})
        center_lat = result.get("lat", opp.get("lat", 0))
        center_lng = result.get("lng", opp.get("lng", 0))
        search_radius = opp.get("search_radius_km", 1.0)
        
        google_pharmacies = result.get("pharmacies", [])
        
        # Filter to actual pharmacies and within search radius
        relevant = []
        for gp in google_pharmacies:
            if not gp.get("lat") or not gp.get("lng"):
                continue
            dist = haversine_km(center_lat, center_lng, gp["lat"], gp["lng"])
            if dist <= search_radius * 2:  # generous radius for checking
                relevant.append({**gp, "distance_km": dist})
        
        # Filter to pharmacy-like names
        pharmacy_relevant = [r for r in relevant if is_pharmacy_name(r["name"])]
        
        # Check which are new vs known
        new_count = 0
        known_count = 0
        new_names = []
        
        for gp in pharmacy_relevant:
            match, dist = find_matching_pharmacy(gp["name"], gp["lat"], gp["lng"], db_pharmacies)
            if match:
                known_count += 1
            else:
                new_count += 1
                new_names.append(gp["name"])
                new_pharmacies.append({
                    "name": gp["name"],
                    "lat": gp["lat"],
                    "lng": gp["lng"],
                    "source": "google_maps_verification",
                    "opportunity_index": idx,
                    "state": opp.get("state", ""),
                })
        
        # Determine status
        in_tight_radius = [r for r in pharmacy_relevant if r["distance_km"] <= search_radius]
        
        if len(in_tight_radius) > 0 and new_count > 0:
            status = "invalidated"  # Found pharmacies Google knows about that we don't
        elif len(in_tight_radius) > 0:
            status = "needs_review"  # Pharmacies nearby but all in our DB
        else:
            status = "verified"  # No pharmacies found in radius by Google either
        
        all_results.append({
            "index": idx,
            "state": opp.get("state", ""),
            "lat": center_lat,
            "lng": center_lng,
            "rules": opp.get("rules", ""),
            "confidence": opp.get("confidence", 0),
            "poi_name": opp.get("poi_name", ""),
            "search_radius_km": search_radius,
            "google_total": len(relevant),
            "google_pharmacies_found": len(pharmacy_relevant),
            "google_in_radius": len(in_tight_radius),
            "known_pharmacies": known_count,
            "new_pharmacies": new_count,
            "new_pharmacy_names": "; ".join(new_names),
            "status": status,
        })
    
    return all_results, new_pharmacies

if __name__ == "__main__":
    results_path = os.path.join(OUTPUT_DIR, "google_extraction_results.json")
    if os.path.exists(results_path):
        results, new_pharm = process_results(results_path)
        
        # Save results
        csv_path = os.path.join(OUTPUT_DIR, "google_verified.csv")
        if results:
            fieldnames = list(results[0].keys())
            with open(csv_path, "w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(results)
            print(f"Saved {len(results)} results to {csv_path}")
        
        # Save new pharmacies
        if new_pharm:
            new_path = os.path.join(OUTPUT_DIR, "new_pharmacies_from_google.json")
            with open(new_path, "w", encoding="utf-8") as f:
                json.dump(new_pharm, f, indent=2)
            print(f"Found {len(new_pharm)} new pharmacies! Saved to {new_path}")
        
        # Summary
        verified = sum(1 for r in results if r["status"] == "verified")
        invalidated = sum(1 for r in results if r["status"] == "invalidated")
        needs_review = sum(1 for r in results if r["status"] == "needs_review")
        print(f"\nVerification Summary:")
        print(f"  Verified: {verified}")
        print(f"  Invalidated: {invalidated}")
        print(f"  Needs Review: {needs_review}")
    else:
        print(f"No extraction results found at {results_path}")
        print("Run browser verification first.")
