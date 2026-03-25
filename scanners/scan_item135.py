"""
Item 135 National Scanner — Large private hospital pharmacy.

Finds large private hospitals (≥150 beds) with no existing pharmacy.
Requirements:
- Private hospital (not public)
- Admission capacity ≥150 (using bed_count as proxy)
- No existing pharmacy within the hospital
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scanners.base import (
    write_results, print_summary, timed_run,
    detect_state_from_address, extract_suburb_postcode, ALL_STATES
)
from engine.context import EvaluationContext

MIN_BED_COUNT = 150
PHARMACY_IN_HOSPITAL_KM = 0.15  # 150m proxy for "inside the hospital"


@timed_run
def scan_item135():
    """Run Item 135 scanner nationally."""
    print("[Item 135] Loading data...")
    ctx = EvaluationContext()
    
    results = []
    total = len(ctx.hospitals)
    print(f"[Item 135] Scanning {total} hospitals for large private hospitals without pharmacy...")
    
    private_count = 0
    large_private = 0
    
    for hosp in ctx.hospitals:
        hlat = hosp["latitude"]
        hlon = hosp["longitude"]
        
        # Must be private
        h_type = (hosp.get("hospital_type") or "").lower()
        if "private" not in h_type:
            continue
        private_count += 1
        
        # Must have ≥150 beds
        beds = hosp.get("bed_count") or 0
        if beds < MIN_BED_COUNT:
            continue
        large_private += 1
        
        # Check: no pharmacy inside/adjacent
        pharmacies_nearby = ctx.pharmacies_within_radius(hlat, hlon, PHARMACY_IN_HOSPITAL_KM)
        if pharmacies_nearby:
            continue  # Already has pharmacy
        
        # Also check slightly wider radius (300m) for nearby pharmacies
        pharmacies_wider = ctx.pharmacies_within_radius(hlat, hlon, 0.3)
        
        nearest_pharm, nearest_dist = ctx.nearest_pharmacy(hlat, hlon)
        nearest_pharmacy_km = round(nearest_dist, 3) if nearest_dist != float('inf') else 999
        
        confidence = 0.75
        if beds >= 200:
            confidence = 0.85
        if beds >= 300:
            confidence = 0.9
        # If pharmacies nearby but not inside, still an opportunity
        if pharmacies_wider:
            confidence -= 0.1  # Pharmacy exists nearby, may serve hospital already
        
        addr = hosp.get("address", "")
        state = detect_state_from_address(addr)
        suburb, postcode = extract_suburb_postcode(addr)
        
        reason = (f"Large private hospital: {hosp['name']}, "
                 f"{beds} beds (≥{MIN_BED_COUNT} required). "
                 f"No pharmacy within {PHARMACY_IN_HOSPITAL_KM*1000:.0f}m. "
                 f"Nearest pharmacy: {nearest_pharmacy_km}km")
        
        results.append({
            "lat": round(hlat, 6),
            "lon": round(hlon, 6),
            "suburb": suburb,
            "postcode": postcode,
            "state": state,
            "rule_item": "Item 135",
            "confidence": round(confidence, 3),
            "nearest_pharmacy_km": nearest_pharmacy_km,
            "reason": reason,
            "name": hosp.get("name", ""),
            "address": addr,
            "source_type": "hospital",
            "bed_count": beds,
            "hospital_type": h_type,
        })
    
    print(f"[Item 135] Stats: {total} hospitals -> {private_count} private -> {large_private} large (>={MIN_BED_COUNT} beds)")
    
    results.sort(key=lambda x: (-x["confidence"], -x["bed_count"]))
    
    write_results(results, "Item 135", extra_columns=["bed_count", "hospital_type"])
    print_summary(results, "Item 135")
    return results


if __name__ == "__main__":
    scan_item135()
