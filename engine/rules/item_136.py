"""
Item 136 — Large medical centre pharmacy.

Requirements:
(c) At least 300m straight-line from nearest approved pharmacy
    (excluding pharmacies in large shopping centres or hospitals)
(d) At least 8 FTE PBS prescribers, of which ≥ 7 must be medical practitioners
(e) Centre operates at least 70 hours per week
Also: no existing approved pharmacy already in the centre.

Measurement: geodesic straight-line for 300m check.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from engine.models import Candidate, RuleResult
from engine.rules.general import confidence_from_margin_m

PHARMACY_DISTANCE_M = 300       # 300m straight-line
MIN_FTE_PRESCRIBERS = 8.0       # Total PBS prescribers
MIN_MEDICAL_PRACTITIONERS = 7   # Of the 8 FTE, at least 7 must be medical practitioners
MIN_HOURS_PER_WEEK = 70.0
MEDICAL_CENTRE_RADIUS_KM = 0.3  # Must be within 300m of a medical centre
PHARMACY_IN_CENTRE_KM = 0.15    # 150m proxy for "within the centre"


def check_item_136(candidate: Candidate, context) -> RuleResult:
    """Evaluate Item 136 for a candidate."""
    reasons = []
    evidence_needed = []
    distances = {}

    # Must be at/in a medical centre
    nearby_mcs = context.medical_centres_within_radius(
        candidate.latitude, candidate.longitude, MEDICAL_CENTRE_RADIUS_KM
    )

    if not nearby_mcs:
        reasons.append("FAIL: No medical centre within 300m")
        return RuleResult(item="Item 136", passed=False, reasons=reasons, confidence=0.0, distances=distances)

    mc, mc_dist = nearby_mcs[0]
    distances["medical_centre_name"] = mc['name']
    distances["medical_centre_distance_m"] = round(mc_dist * 1000, 0)
    distances["mc_num_gps"] = mc.get('num_gps', 0)
    distances["mc_total_fte"] = mc.get('total_fte', 0)
    distances["mc_hours_per_week"] = mc.get('hours_per_week', 0)

    # No existing pharmacy already in the centre
    pharmas_near_mc = context.pharmacies_within_radius(
        mc['latitude'], mc['longitude'], PHARMACY_IN_CENTRE_KM
    )
    if pharmas_near_mc:
        p, pd = pharmas_near_mc[0]
        reasons.append(
            f"FAIL: Existing pharmacy {p['name']} within {pd*1000:.0f}m of {mc['name']} "
            f"— likely already in the centre"
        )
        evidence_needed.append("Verify if existing pharmacy is inside the medical centre")
        return RuleResult(
            item="Item 136", passed=False, reasons=reasons,
            evidence_needed=evidence_needed, confidence=0.0, distances=distances,
        )
    reasons.append(f"PASS: No existing pharmacy in {mc['name']}")

    # (c) 300m separation from nearest pharmacy
    nearest_pharm, nearest_dist = context.nearest_pharmacy(
        candidate.latitude, candidate.longitude
    )
    nearest_dist_m = nearest_dist * 1000 if nearest_dist != float('inf') else 999000
    distances["nearest_pharmacy_m"] = round(nearest_dist_m, 0)
    distances["nearest_pharmacy_name"] = nearest_pharm['name'] if nearest_pharm else "None"

    if not nearest_pharm:
        margin_m = 999000 - PHARMACY_DISTANCE_M
        reasons.append("PASS (c): No pharmacy found nearby")
    elif nearest_dist_m < PHARMACY_DISTANCE_M:
        reasons.append(
            f"FAIL (c): Nearest pharmacy {nearest_pharm['name']} = {nearest_dist_m:.0f}m "
            f"(need >= {PHARMACY_DISTANCE_M}m). "
            f"Note: pharmacies in large centres/hospitals excluded"
        )
        evidence_needed.append("Check if nearest pharmacy is in a large centre/hospital (would be excluded)")
        return RuleResult(
            item="Item 136", passed=False, reasons=reasons,
            evidence_needed=evidence_needed, confidence=0.0, distances=distances,
        )
    else:
        margin_m = nearest_dist_m - PHARMACY_DISTANCE_M
        reasons.append(
            f"PASS (c): Nearest pharmacy = {nearest_dist_m:.0f}m (margin +{margin_m:.0f}m)"
        )

    # (d) FTE prescriber check
    num_gps = mc.get('num_gps') or 0
    total_fte = mc.get('total_fte') or 0

    # Use num_gps as proxy for medical practitioners if FTE not available
    if total_fte == 0 and num_gps > 0:
        # Estimate: each GP ≈ 0.8 FTE (many part-time)
        total_fte = num_gps * 0.8

    if total_fte < MIN_FTE_PRESCRIBERS:
        if num_gps == 0 and total_fte == 0:
            reasons.append(
                f"UNKNOWN (d): GP/prescriber data unavailable for {mc['name']}"
            )
            evidence_needed.append(f"Verify {mc['name']} has ≥ {MIN_FTE_PRESCRIBERS} FTE PBS prescribers")
            # Don't auto-fail — data might be missing
        else:
            reasons.append(
                f"FAIL (d): {mc['name']} has ~{total_fte:.1f} FTE prescribers "
                f"({num_gps} GPs, need ≥ {MIN_FTE_PRESCRIBERS} FTE)"
            )
            return RuleResult(
                item="Item 136", passed=False, reasons=reasons,
                evidence_needed=evidence_needed, confidence=0.0, distances=distances,
            )
    else:
        reasons.append(
            f"PASS (d): {mc['name']} has ~{total_fte:.1f} FTE prescribers ({num_gps} GPs)"
        )

    # (e) Operating hours ≥ 70 hrs/week
    hours = mc.get('hours_per_week') or 0
    if hours < MIN_HOURS_PER_WEEK:
        if hours == 0:
            reasons.append(f"UNKNOWN (e): Operating hours not available for {mc['name']}")
            evidence_needed.append(f"Verify {mc['name']} operates ≥ {MIN_HOURS_PER_WEEK} hours/week")
        else:
            reasons.append(
                f"FAIL (e): {mc['name']} operates {hours:.0f} hrs/week (need ≥ {MIN_HOURS_PER_WEEK})"
            )
            return RuleResult(
                item="Item 136", passed=False, reasons=reasons,
                evidence_needed=evidence_needed, confidence=0.0, distances=distances,
            )
    else:
        reasons.append(f"PASS (e): {mc['name']} operates {hours:.0f} hrs/week (≥ {MIN_HOURS_PER_WEEK})")

    evidence_needed.extend([
        "Verify public access door midpoints for 300m measurement",
        "Verify prescriber roster and FTE calculation",
        "Verify operating hours from centre management",
        "Confirm ≥ 7 of the 8 FTE are medical practitioners",
    ])

    # Calculate confidence
    conf = confidence_from_margin_m(margin_m)
    # Reduce if prescriber/hours data missing
    if (mc.get('num_gps') or 0) == 0:
        conf *= 0.4
    if (mc.get('hours_per_week') or 0) == 0:
        conf *= 0.5

    # Determine if passed overall
    passed = True
    if total_fte < MIN_FTE_PRESCRIBERS and total_fte > 0:
        passed = False
    if hours > 0 and hours < MIN_HOURS_PER_WEEK:
        passed = False

    return RuleResult(
        item="Item 136", passed=passed, reasons=reasons,
        evidence_needed=evidence_needed, confidence=round(conf, 3), distances=distances,
    )
