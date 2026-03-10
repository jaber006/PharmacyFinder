"""
Item 130 — New pharmacy >= 1.5 km straight line from nearest approved premises.

Requirements:
(a) At least 1.5 km straight-line from nearest approved pharmacy
(b) One of:
    (i)  Supermarket >= 1,000 sqm GLA within 500m AND >= 1 FTE GP within 500m
    (ii) Supermarket >= 2,500 sqm GLA within 500m (no GP required)

Measurement: geodesic straight-line distance.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from engine.models import Candidate, RuleResult
from engine.rules.general import confidence_from_margin_m

PHARMACY_DISTANCE_KM = 1.5
SUPERMARKET_GP_RADIUS_KM = 0.5
SUPERMARKET_SMALL_GLA = 1000
SUPERMARKET_LARGE_GLA = 2500


def check_item_130(candidate: Candidate, context) -> RuleResult:
    """Evaluate Item 130 for a candidate."""
    reasons = []
    evidence_needed = []
    distances = {}

    nearest_pharm, nearest_dist_km = context.nearest_pharmacy(
        candidate.latitude, candidate.longitude
    )

    if nearest_pharm is None:
        # No pharmacy found at all — extremely remote
        distances["nearest_pharmacy_km"] = 999
        distances["nearest_pharmacy_name"] = "None"
        reasons.append("PASS (a): No pharmacies found — very remote area")
        evidence_needed.append("Verify no approved pharmacies exist in wider area")
        margin_m = 999000
    elif nearest_dist_km < PHARMACY_DISTANCE_KM:
        distances["nearest_pharmacy_km"] = round(nearest_dist_km, 4)
        distances["nearest_pharmacy_name"] = nearest_pharm['name']
        margin_m = (nearest_dist_km - PHARMACY_DISTANCE_KM) * 1000
        reasons.append(
            f"FAIL: Nearest pharmacy {nearest_pharm['name']} is {nearest_dist_km:.3f} km away "
            f"(need >= {PHARMACY_DISTANCE_KM} km). Short by {abs(margin_m):.0f}m."
        )
        return RuleResult(
            item="Item 130", passed=False, reasons=reasons,
            evidence_needed=evidence_needed, confidence=0.0, distances=distances,
        )
    else:
        distances["nearest_pharmacy_km"] = round(nearest_dist_km, 4)
        distances["nearest_pharmacy_name"] = nearest_pharm['name']
        margin_m = (nearest_dist_km - PHARMACY_DISTANCE_KM) * 1000
        reasons.append(
            f"PASS (a): Nearest pharmacy {nearest_pharm['name']} is {nearest_dist_km:.3f} km "
            f"(threshold {PHARMACY_DISTANCE_KM} km, margin +{margin_m:.0f}m)"
        )
        evidence_needed.append("Verify public access door midpoints for precise measurement")

    # (b) Supermarket/GP check within 500m
    nearby_supermarkets = context.supermarkets_within_radius(
        candidate.latitude, candidate.longitude, SUPERMARKET_GP_RADIUS_KM
    )
    nearby_gps = context.gps_within_radius(
        candidate.latitude, candidate.longitude, SUPERMARKET_GP_RADIUS_KM
    )
    distances["supermarkets_within_500m"] = len(nearby_supermarkets)
    distances["gps_within_500m"] = len(nearby_gps)

    # (b)(ii): large supermarket >= 2,500 sqm (no GP needed)
    for s, d in nearby_supermarkets:
        gla = s.get('estimated_gla') or s.get('floor_area_sqm') or 0
        if gla >= SUPERMARKET_LARGE_GLA:
            reasons.append(
                f"PASS (b)(ii): Supermarket {s['name']} ({gla:.0f} sqm GLA) within {d:.3f} km"
            )
            return RuleResult(
                item="Item 130", passed=True, reasons=reasons,
                evidence_needed=evidence_needed,
                confidence=confidence_from_margin_m(margin_m), distances=distances,
            )

    # (b)(i): supermarket >= 1,000 sqm + GP within 500m
    small_super = None
    for s, d in nearby_supermarkets:
        gla = s.get('estimated_gla') or s.get('floor_area_sqm') or 0
        if gla >= SUPERMARKET_SMALL_GLA:
            small_super = (s, d, gla)
            break

    if small_super and nearby_gps:
        s, d, gla = small_super
        g, gd = nearby_gps[0]
        reasons.append(
            f"PASS (b)(i): Supermarket {s['name']} ({gla:.0f} sqm) within {d:.3f} km "
            f"AND GP {g['name']} within {gd:.3f} km"
        )
        return RuleResult(
            item="Item 130", passed=True, reasons=reasons,
            evidence_needed=evidence_needed,
            confidence=confidence_from_margin_m(margin_m), distances=distances,
        )

    # Supermarket/GP requirement not met
    if not nearby_supermarkets:
        reasons.append("FAIL (b): No supermarket within 500m")
    elif not small_super:
        best_gla = max((s.get('estimated_gla') or s.get('floor_area_sqm') or 0
                       for s, _ in nearby_supermarkets), default=0)
        reasons.append(f"FAIL (b): Nearest supermarket GLA ({best_gla:.0f} sqm) below {SUPERMARKET_SMALL_GLA} sqm")
        evidence_needed.append("Verify supermarket GLA from council/planning docs")
    elif not nearby_gps:
        reasons.append(f"FAIL (b)(i): Supermarket found but no GP within 500m")
        evidence_needed.append("Check for GP practices not in database")

    return RuleResult(
        item="Item 130", passed=False, reasons=reasons,
        evidence_needed=evidence_needed, confidence=0.0, distances=distances,
    )
