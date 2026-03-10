"""
Item 131 — New pharmacy ≥ 10 km by shortest lawful access route.

Requirements:
- At least 10 km by shortest lawful access route from nearest approved pharmacy.

Measurement: OSRM driving route distance.
For Stage A screening we estimate route distance as ~1.4× geodesic when
geodesic alone is clearly over or under threshold; only call OSRM for borderline cases.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from engine.models import Candidate, RuleResult
from engine.rules.general import confidence_from_margin_m

ROUTE_DISTANCE_KM = 10.0
# If geodesic > this, almost certainly passes by route too (skip OSRM)
GEODESIC_CLEAR_PASS_KM = 8.0    # 8km straight ≈ 11.2km by road
# If geodesic < this, almost certainly fails (skip OSRM)
GEODESIC_CLEAR_FAIL_KM = 5.0    # 5km straight ≈ 7km by road


def check_item_131(candidate: Candidate, context) -> RuleResult:
    """Evaluate Item 131 for a candidate."""
    reasons = []
    evidence_needed = []
    distances = {}

    nearest_pharm, nearest_dist_km = context.nearest_pharmacy(
        candidate.latitude, candidate.longitude
    )
    distances["nearest_pharmacy_km_geodesic"] = round(nearest_dist_km, 4) if nearest_dist_km != float('inf') else 999
    distances["nearest_pharmacy_name"] = nearest_pharm['name'] if nearest_pharm else "None"

    if not nearest_pharm:
        reasons.append("PASS: No pharmacies in database (remote area)")
        return RuleResult(
            item="Item 131", passed=True, reasons=reasons,
            evidence_needed=["Verify no approved pharmacies exist"],
            confidence=0.5, distances=distances,
        )

    # Quick pre-filter using geodesic
    estimated_route = nearest_dist_km * 1.4
    distances["estimated_route_km"] = round(estimated_route, 2)

    if nearest_dist_km >= GEODESIC_CLEAR_PASS_KM:
        # Clearly passes — geodesic alone is high enough
        margin_m = (estimated_route - ROUTE_DISTANCE_KM) * 1000
        reasons.append(
            f"PASS: Nearest pharmacy {nearest_pharm['name']} is {nearest_dist_km:.2f} km geodesic "
            f"(~{estimated_route:.1f} km by road, threshold {ROUTE_DISTANCE_KM} km)"
        )
        evidence_needed.append("Verify with actual OSRM route for legal-grade distance")
        conf = confidence_from_margin_m(margin_m)
        return RuleResult(
            item="Item 131", passed=True, reasons=reasons,
            evidence_needed=evidence_needed, confidence=conf, distances=distances,
        )

    if nearest_dist_km < GEODESIC_CLEAR_FAIL_KM:
        # Clearly fails
        reasons.append(
            f"FAIL: Nearest pharmacy {nearest_pharm['name']} is only {nearest_dist_km:.2f} km geodesic "
            f"(~{estimated_route:.1f} km by road, need ≥ {ROUTE_DISTANCE_KM} km)"
        )
        return RuleResult(
            item="Item 131", passed=False, reasons=reasons,
            evidence_needed=evidence_needed, confidence=0.0, distances=distances,
        )

    # Borderline — try OSRM for accurate route distance
    route_km = context.get_driving_distance_cached(
        candidate.latitude, candidate.longitude,
        nearest_pharm['latitude'], nearest_pharm['longitude']
    )

    if route_km is not None:
        distances["route_distance_km"] = round(route_km, 3)
        if route_km >= ROUTE_DISTANCE_KM:
            margin_m = (route_km - ROUTE_DISTANCE_KM) * 1000
            reasons.append(
                f"PASS: OSRM route to {nearest_pharm['name']} = {route_km:.2f} km "
                f"(threshold {ROUTE_DISTANCE_KM} km, margin +{margin_m:.0f}m)"
            )
            evidence_needed.append("Verify route is 'shortest lawful access route'")
            conf = confidence_from_margin_m(margin_m)
            return RuleResult(
                item="Item 131", passed=True, reasons=reasons,
                evidence_needed=evidence_needed, confidence=conf, distances=distances,
            )
        else:
            margin_m = (route_km - ROUTE_DISTANCE_KM) * 1000
            reasons.append(
                f"FAIL: OSRM route to {nearest_pharm['name']} = {route_km:.2f} km "
                f"(need ≥ {ROUTE_DISTANCE_KM} km, short by {abs(margin_m):.0f}m)"
            )
            return RuleResult(
                item="Item 131", passed=False, reasons=reasons,
                evidence_needed=evidence_needed, confidence=0.0, distances=distances,
            )
    else:
        # OSRM failed — use estimate with low confidence
        if estimated_route >= ROUTE_DISTANCE_KM:
            margin_m = (estimated_route - ROUTE_DISTANCE_KM) * 1000
            reasons.append(
                f"PASS (estimated): Route to {nearest_pharm['name']} ≈ {estimated_route:.1f} km "
                f"(OSRM unavailable, using 1.4× geodesic)"
            )
            evidence_needed.append("OSRM route verification required")
            return RuleResult(
                item="Item 131", passed=True, reasons=reasons,
                evidence_needed=evidence_needed, confidence=0.55, distances=distances,
            )
        else:
            reasons.append(
                f"FAIL (estimated): Route to {nearest_pharm['name']} ≈ {estimated_route:.1f} km "
                f"(OSRM unavailable)"
            )
            return RuleResult(
                item="Item 131", passed=False, reasons=reasons,
                evidence_needed=evidence_needed, confidence=0.0, distances=distances,
            )
