"""
Item 136 — Large medical centre pharmacy.

Requirements:
(c) At least 300m straight-line from nearest approved pharmacy.
    Two branches:
    (c)(i) If medical centre IS in a shopping centre or hospital:
           ≥300m from ANY pharmacy, except those in a DIFFERENT large SC or hospital.
    (c)(ii) If medical centre is NOT in a SC or hospital:
            ≥300m from nearest pharmacy, EXCLUDING those in large SCs or hospitals.
(d) At least 8 FTE PBS prescribers, of which ≥7 must be medical practitioners.
(e) Centre operates at least 70 hours per week, AND a GP must be available
    for at least 70 of those operating hours (gp_available_hours).
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
MIN_GP_AVAILABLE_HOURS = 70.0   # GP must be present ≥70 hrs/week
MEDICAL_CENTRE_RADIUS_KM = 0.3  # Must be within 300m of a medical centre
PHARMACY_IN_CENTRE_KM = 0.15    # 150m proxy for "within the centre"
SC_HOSPITAL_PROXIMITY_KM = 0.05 # 50m to determine if MC is inside a SC or hospital
LARGE_SC_MIN_TENANTS = 50       # ≥50 tenants = large shopping centre


def _mc_is_in_complex(mc, context):
    """
    Determine if the medical centre is inside a shopping centre or hospital.
    Returns (is_in_complex: bool, complex_info: dict or None).
    """
    mc_lat, mc_lon = mc['latitude'], mc['longitude']

    # Check shopping centres within 50m
    nearby_scs = context.shopping_centres_within_radius(mc_lat, mc_lon, SC_HOSPITAL_PROXIMITY_KM)
    for sc, sc_dist in nearby_scs:
        return True, {'type': 'shopping_centre', 'name': sc.get('name', 'Unknown SC'), 'data': sc}

    # Check hospitals within 50m
    nearby_hosps = context.hospitals_within_radius(mc_lat, mc_lon, SC_HOSPITAL_PROXIMITY_KM)
    for h, h_dist in nearby_hosps:
        return True, {'type': 'hospital', 'name': h.get('name', 'Unknown Hospital'), 'data': h}

    return False, None


def _pharmacy_is_in_different_large_complex(pharmacy, mc_complex, context):
    """
    Check if a pharmacy is inside a DIFFERENT large SC or hospital
    (different from the one the medical centre is in).
    """
    plat, plon = pharmacy['latitude'], pharmacy['longitude']

    # Check large shopping centres near pharmacy
    nearby_scs = context.shopping_centres_within_radius(plat, plon, 0.3)
    for sc, sc_dist in nearby_scs:
        tenants = sc.get('estimated_tenants') or 0
        if tenants >= LARGE_SC_MIN_TENANTS:
            # Is it a DIFFERENT complex from the MC's?
            if mc_complex and mc_complex['type'] == 'shopping_centre':
                if sc.get('id') != mc_complex['data'].get('id'):
                    return True
            else:
                # MC is in a hospital, so any large SC is "different"
                return True

    # Check hospitals near pharmacy
    nearby_hosps = context.hospitals_within_radius(plat, plon, 0.15)
    for h, h_dist in nearby_hosps:
        h_type = (h.get('hospital_type') or '').lower()
        if 'private' in h_type:
            if mc_complex and mc_complex['type'] == 'hospital':
                if h.get('id') != mc_complex['data'].get('id'):
                    return True
            else:
                # MC is in a SC, so any hospital is "different"
                return True

    return False


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

    # ─── Bug 3 fix: (c) 300m separation — two-branch rule ───
    mc_in_complex, mc_complex_info = _mc_is_in_complex(mc, context)

    if mc_in_complex:
        # (c)(i): MC is in a shopping centre or hospital
        # Check ALL pharmacies within 300m — they must be in a DIFFERENT large SC or hospital
        nearby_pharmacies = context.pharmacies_within_radius(
            candidate.latitude, candidate.longitude, PHARMACY_DISTANCE_M / 1000.0
        )

        blocking_pharmacy = None
        for pharm, pharm_dist in nearby_pharmacies:
            if not _pharmacy_is_in_different_large_complex(pharm, mc_complex_info, context):
                blocking_pharmacy = (pharm, pharm_dist)
                break

        if blocking_pharmacy:
            pharm, pharm_dist = blocking_pharmacy
            nearest_dist_m = pharm_dist * 1000
            distances["nearest_pharmacy_m"] = round(nearest_dist_m, 0)
            distances["nearest_pharmacy_name"] = pharm['name']
            reasons.append(
                f"FAIL (c)(i): MC is in {mc_complex_info['name']}. "
                f"Pharmacy {pharm['name']} at {nearest_dist_m:.0f}m is NOT in a different "
                f"large SC or hospital (need ≥{PHARMACY_DISTANCE_M}m or excluded)"
            )
            return RuleResult(
                item="Item 136", passed=False, reasons=reasons,
                evidence_needed=evidence_needed, confidence=0.0, distances=distances,
            )
        else:
            # All pharmacies within 300m (if any) are in different large complexes
            # Find actual nearest for margin calculation
            nearest_pharm, nearest_dist = context.nearest_pharmacy(
                candidate.latitude, candidate.longitude
            )
            nearest_dist_m = nearest_dist * 1000 if nearest_dist != float('inf') else 999000
            distances["nearest_pharmacy_m"] = round(nearest_dist_m, 0)
            distances["nearest_pharmacy_name"] = nearest_pharm['name'] if nearest_pharm else "None"

            if not nearest_pharm:
                margin_m = 999000 - PHARMACY_DISTANCE_M
                reasons.append(
                    f"PASS (c)(i): MC is in {mc_complex_info['name']}. No pharmacies found nearby"
                )
            elif nearby_pharmacies:
                margin_m = PHARMACY_DISTANCE_M  # All within 300m excluded
                reasons.append(
                    f"PASS (c)(i): MC is in {mc_complex_info['name']}. "
                    f"All pharmacies within {PHARMACY_DISTANCE_M}m are in different large complexes"
                )
            else:
                margin_m = nearest_dist_m - PHARMACY_DISTANCE_M
                reasons.append(
                    f"PASS (c)(i): MC is in {mc_complex_info['name']}. "
                    f"No pharmacy within {PHARMACY_DISTANCE_M}m"
                )
    else:
        # (c)(ii): MC is NOT in a SC or hospital
        # Find nearest pharmacy EXCLUDING those in large SCs or hospitals
        nearest_pharm, nearest_dist = context.nearest_pharmacy_excluding_complexes(
            candidate.latitude, candidate.longitude
        )
        nearest_dist_m = nearest_dist * 1000 if nearest_dist != float('inf') else 999000
        distances["nearest_pharmacy_m"] = round(nearest_dist_m, 0)
        distances["nearest_pharmacy_name"] = nearest_pharm['name'] if nearest_pharm else "None"

        if not nearest_pharm:
            margin_m = 999000 - PHARMACY_DISTANCE_M
            reasons.append("PASS (c)(ii): No non-excluded pharmacy found nearby")
        elif nearest_dist_m < PHARMACY_DISTANCE_M:
            reasons.append(
                f"FAIL (c)(ii): Nearest non-excluded pharmacy {nearest_pharm['name']} = "
                f"{nearest_dist_m:.0f}m (need ≥{PHARMACY_DISTANCE_M}m). "
                f"Pharmacies in large SCs/hospitals already excluded"
            )
            return RuleResult(
                item="Item 136", passed=False, reasons=reasons,
                evidence_needed=evidence_needed, confidence=0.0, distances=distances,
            )
        else:
            margin_m = nearest_dist_m - PHARMACY_DISTANCE_M
            reasons.append(
                f"PASS (c)(ii): Nearest non-excluded pharmacy = {nearest_dist_m:.0f}m "
                f"(margin +{margin_m:.0f}m). Pharmacies in large SCs/hospitals excluded"
            )

    # ─── Bug 4 fix: (d) FTE prescriber check with medical practitioner sub-requirement ───
    num_gps = mc.get('num_gps') or 0
    total_fte = mc.get('total_fte') or 0
    medical_fte = mc.get('medical_fte') or 0

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

    # Bug 4: Check medical practitioner sub-requirement (≥7 of 8 must be medical practitioners)
    if medical_fte > 0:
        if medical_fte < MIN_MEDICAL_PRACTITIONERS:
            reasons.append(
                f"FAIL (d): Only {medical_fte:.1f} medical practitioner FTE "
                f"(need ≥ {MIN_MEDICAL_PRACTITIONERS} of {MIN_FTE_PRESCRIBERS} total). "
                f"Too many non-medical prescribers (dentists, optometrists, etc.)"
            )
            return RuleResult(
                item="Item 136", passed=False, reasons=reasons,
                evidence_needed=evidence_needed, confidence=0.0, distances=distances,
            )
        else:
            reasons.append(
                f"PASS (d): {medical_fte:.1f} medical practitioner FTE (≥ {MIN_MEDICAL_PRACTITIONERS})"
            )
    elif total_fte >= MIN_FTE_PRESCRIBERS:
        # medical_fte data not available — flag it
        evidence_needed.append(
            f"Verify ≥ {MIN_MEDICAL_PRACTITIONERS} of {MIN_FTE_PRESCRIBERS} FTE are medical practitioners "
            f"(not dentists, optometrists, etc.)"
        )

    # ─── (e) Operating hours ≥ 70 hrs/week ───
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

    # ─── Bug 5 fix: GP available hours check ───
    gp_available_hours = mc.get('gp_available_hours') or 0
    if gp_available_hours > 0:
        if gp_available_hours < MIN_GP_AVAILABLE_HOURS:
            reasons.append(
                f"FAIL (e): GP available only {gp_available_hours:.0f} hrs/week at {mc['name']} "
                f"(need GP present ≥ {MIN_GP_AVAILABLE_HOURS} hrs/week for large MC definition)"
            )
            return RuleResult(
                item="Item 136", passed=False, reasons=reasons,
                evidence_needed=evidence_needed, confidence=0.0, distances=distances,
            )
        else:
            reasons.append(
                f"PASS (e): GP available {gp_available_hours:.0f} hrs/week "
                f"(≥ {MIN_GP_AVAILABLE_HOURS} — meets large MC definition)"
            )
    else:
        evidence_needed.append(
            f"Verify a GP is available ≥ {MIN_GP_AVAILABLE_HOURS} hrs/week at {mc['name']} "
            f"(required for 'large medical centre' definition)"
        )

    evidence_needed.extend([
        "Verify public access door midpoints for 300m measurement",
        "Verify prescriber roster and FTE calculation",
        "Verify operating hours from centre management",
    ])

    # Calculate confidence
    conf = confidence_from_margin_m(margin_m)
    # Reduce if prescriber/hours data missing
    if (mc.get('num_gps') or 0) == 0:
        conf *= 0.4
    if (mc.get('hours_per_week') or 0) == 0:
        conf *= 0.5
    if (mc.get('medical_fte') or 0) == 0:
        conf *= 0.7  # Reduce confidence if medical_fte data missing
    if (mc.get('gp_available_hours') or 0) == 0:
        conf *= 0.7  # Reduce confidence if gp_available_hours data missing

    # Determine if passed overall
    passed = True
    if total_fte < MIN_FTE_PRESCRIBERS and total_fte > 0:
        passed = False
    if hours > 0 and hours < MIN_HOURS_PER_WEEK:
        passed = False
    if medical_fte > 0 and medical_fte < MIN_MEDICAL_PRACTITIONERS:
        passed = False
    if gp_available_hours > 0 and gp_available_hours < MIN_GP_AVAILABLE_HOURS:
        passed = False

    return RuleResult(
        item="Item 136", passed=passed, reasons=reasons,
        evidence_needed=evidence_needed, confidence=round(conf, 3), distances=distances,
    )
