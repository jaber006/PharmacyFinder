"""
Item 136 National Scanner — Large medical centre pharmacy.

Finds medical centres with ≥8 FTE GPs (or ≥10 headcount as proxy) qualifying for a pharmacy.
Requirements:
- ≥8 FTE PBS prescribers (at least 7 must be medical practitioners)
- ≥300m from nearest pharmacy (excluding those in large SCs or private hospitals)
- Centre operates ≥70 hours/week
- No existing pharmacy inside the centre
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scanners.base import (
    write_results, print_summary, timed_run,
    detect_state_from_address, extract_suburb_postcode, ALL_STATES
)
from engine.context import EvaluationContext

MIN_FTE = 8.0
MIN_HEADCOUNT_PROXY = 10   # If FTE unknown, use headcount with 0.8 FTE factor
MIN_HOURS = 70.0
PHARMACY_DISTANCE_M = 300   # 300m from nearest non-excluded pharmacy
PHARMACY_IN_CENTRE_KM = 0.15  # 150m proxy for "inside the centre"


@timed_run
def scan_item136():
    """Run Item 136 scanner nationally."""
    print("[Item 136] Loading data...")
    ctx = EvaluationContext()
    
    results = []
    total = len(ctx.medical_centres)
    print(f"[Item 136] Scanning {total} medical centres...")
    
    for i, mc in enumerate(ctx.medical_centres):
        if (i + 1) % 500 == 0:
            print(f"  Progress: {i+1}/{total} ({len(results)} hits)")
        
        mlat = mc["latitude"]
        mlon = mc["longitude"]
        
        # Check GP count / FTE
        total_fte = mc.get("total_fte") or 0
        num_gps = mc.get("num_gps") or 0
        
        # Use FTE if available, otherwise estimate from headcount
        effective_fte = total_fte
        fte_source = "fte"
        if effective_fte < MIN_FTE and num_gps >= MIN_HEADCOUNT_PROXY:
            effective_fte = num_gps * 0.8  # Estimate ~0.8 FTE per GP
            fte_source = "estimated"
        
        if effective_fte < MIN_FTE:
            continue  # Not enough GPs
        
        # Check hours
        hours = mc.get("hours_per_week") or 0
        hours_ok = hours >= MIN_HOURS if hours > 0 else None  # None = unknown
        
        # Check: no pharmacy inside the centre
        pharmacies_in_centre = ctx.pharmacies_within_radius(mlat, mlon, PHARMACY_IN_CENTRE_KM)
        if pharmacies_in_centre:
            continue  # Already has pharmacy
        
        # Check: ≥300m from nearest non-excluded pharmacy
        nearest_excl, dist_excl = ctx.nearest_pharmacy_excluding_complexes(mlat, mlon)
        
        if nearest_excl and dist_excl < (PHARMACY_DISTANCE_M / 1000.0):
            continue  # Too close
        
        nearest_pharmacy_km = round(dist_excl, 3) if dist_excl != float('inf') else 999
        margin_m = (dist_excl * 1000 - PHARMACY_DISTANCE_M) if dist_excl != float('inf') else 999000
        
        # Calculate confidence
        confidence = 0.7
        if fte_source == "fte":
            confidence += 0.05
        if hours_ok is True:
            confidence += 0.05
        elif hours_ok is None:
            confidence -= 0.1  # Unknown hours
        if effective_fte >= 10:
            confidence += 0.05  # Well above threshold
        if margin_m > 200:
            confidence += 0.05
        confidence = min(max(confidence, 0.3), 0.95)
        
        state = mc.get("state", "") or detect_state_from_address(mc.get("address", ""))
        addr = mc.get("address", "")
        suburb, postcode = extract_suburb_postcode(addr)
        
        reason = (f"Large medical centre: {mc['name']}, "
                 f"FTE={effective_fte:.1f} ({fte_source}), GPs={num_gps}, "
                 f"hours={hours or '?'}/wk. "
                 f"Nearest non-excluded pharmacy: {nearest_pharmacy_km}km (margin +{margin_m:.0f}m)")
        
        results.append({
            "lat": round(mlat, 6),
            "lon": round(mlon, 6),
            "suburb": suburb,
            "postcode": postcode,
            "state": state,
            "rule_item": "Item 136",
            "confidence": round(confidence, 3),
            "nearest_pharmacy_km": nearest_pharmacy_km,
            "reason": reason,
            "name": mc.get("name", ""),
            "address": addr,
            "source_type": "medical_centre",
            "effective_fte": round(effective_fte, 1),
            "num_gps": num_gps,
            "hours_per_week": hours,
            "fte_source": fte_source,
        })
    
    results.sort(key=lambda x: (-x["confidence"], -x["effective_fte"]))
    
    write_results(results, "Item 136", extra_columns=["effective_fte", "num_gps", "hours_per_week", "fte_source"])
    print_summary(results, "Item 136")
    return results


if __name__ == "__main__":
    scan_item136()
