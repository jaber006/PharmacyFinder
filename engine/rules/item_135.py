"""
Item 135 — Large private hospital pharmacy.

Requirements:
- Large private hospital: licence to admit ≥ 150 patients at any one time
- No existing approved pharmacy within the hospital
- Hospital type must be 'private' (NOT public)

Note: bed_count in our DB is a proxy for admission capacity.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from engine.models import Candidate, RuleResult

MIN_BED_COUNT = 150
HOSPITAL_RADIUS_KM = 0.3    # Must be within 300m of hospital (inside or adjacent)
PHARMACY_IN_HOSPITAL_KM = 0.15  # 150m — proxy for "within the hospital"


def check_item_135(candidate: Candidate, context) -> RuleResult:
    """Evaluate Item 135 for a candidate."""
    reasons = []
    evidence_needed = []
    distances = {}

    # Find nearby hospitals
    nearby_hospitals = context.hospitals_within_radius(
        candidate.latitude, candidate.longitude, HOSPITAL_RADIUS_KM
    )

    if not nearby_hospitals:
        reasons.append("FAIL: No hospital within 300m")
        return RuleResult(item="Item 135", passed=False, reasons=reasons, confidence=0.0, distances=distances)

    hospital, hosp_dist = nearby_hospitals[0]
    distances["hospital_name"] = hospital['name']
    distances["hospital_distance_m"] = round(hosp_dist * 1000, 0)
    distances["hospital_beds"] = hospital.get('bed_count')
    distances["hospital_type"] = hospital.get('hospital_type', 'unknown')

    # Must be private hospital
    hosp_type = (hospital.get('hospital_type') or 'unknown').lower()
    if 'public' in hosp_type:
        reasons.append(
            f"FAIL: {hospital['name']} is a public hospital — Item 135 requires PRIVATE"
        )
        return RuleResult(item="Item 135", passed=False, reasons=reasons, confidence=0.0, distances=distances)

    # Check bed count (proxy for admission capacity)
    bed_count = hospital.get('bed_count') or 0
    if bed_count < MIN_BED_COUNT:
        if bed_count == 0:
            reasons.append(
                f"UNKNOWN: {hospital['name']} bed count unknown — cannot confirm ≥ {MIN_BED_COUNT}"
            )
            evidence_needed.append(f"Verify {hospital['name']} admission capacity ≥ {MIN_BED_COUNT}")
            # Don't auto-fail — might still qualify
        else:
            reasons.append(
                f"FAIL: {hospital['name']} has {bed_count} beds (need ≥ {MIN_BED_COUNT})"
            )
            return RuleResult(item="Item 135", passed=False, reasons=reasons, confidence=0.0, distances=distances)
    else:
        reasons.append(
            f"PASS: {hospital['name']} has {bed_count} beds (≥ {MIN_BED_COUNT})"
        )

    # Check no existing pharmacy within the hospital
    pharmas_near_hospital = context.pharmacies_within_radius(
        hospital['latitude'], hospital['longitude'], PHARMACY_IN_HOSPITAL_KM
    )
    if pharmas_near_hospital:
        p, pd = pharmas_near_hospital[0]
        reasons.append(
            f"FAIL: Existing pharmacy {p['name']} within {pd*1000:.0f}m of hospital "
            f"— likely already in the hospital"
        )
        evidence_needed.append("Verify if existing pharmacy is actually inside the hospital")
        return RuleResult(
            item="Item 135", passed=False, reasons=reasons,
            evidence_needed=evidence_needed, confidence=0.0, distances=distances,
        )

    reasons.append("PASS: No existing pharmacy within the hospital")

    # Must be private
    if 'private' in hosp_type:
        reasons.append(f"PASS: Hospital type = {hosp_type}")
        conf = 0.80
    elif hosp_type == 'unknown':
        reasons.append("UNKNOWN: Hospital type not confirmed as private")
        evidence_needed.append("Verify hospital is a private hospital")
        conf = 0.45
    else:
        reasons.append(f"UNCERTAIN: Hospital type = {hosp_type}")
        evidence_needed.append("Verify hospital classification")
        conf = 0.35

    # Reduce confidence if bed count unknown
    if bed_count == 0:
        conf *= 0.5

    evidence_needed.extend([
        "Verify hospital licence admits ≥ 150 patients at one time",
        "Confirm no existing approved pharmacy in hospital",
        "Verify hospital is 'private' under relevant legislation",
    ])

    return RuleResult(
        item="Item 135", passed=True if bed_count >= MIN_BED_COUNT or bed_count == 0 else False,
        reasons=reasons, evidence_needed=evidence_needed,
        confidence=round(conf, 3), distances=distances,
    )
