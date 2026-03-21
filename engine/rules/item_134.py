"""
Item 134 — Large shopping centre pharmacy (no existing pharmacy).

Requirements:
(a) Proposed premises in a LARGE SHOPPING CENTRE:
    - Single management
    - GLA ≥ 5,000 sqm
    - Contains supermarket ≥ 2,500 sqm GLA
    - ≥ 50 other commercial establishments
    - Customer parking
(b) No approved premises in the large shopping centre

Note: NO distance requirement from pharmacies outside the centre.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from engine.models import Candidate, RuleResult

MIN_CENTRE_GLA = 5000           # sqm
MIN_TENANTS = 50                # ≥ 50 commercial establishments
MIN_SUPERMARKET_GLA = 2500      # sqm
CENTRE_RADIUS_KM = 0.3          # Must be within 300m (inside/adjacent to centre)
PHARMACY_IN_CENTRE_KM = 0.3     # Proxy for "within the shopping centre"


def check_item_134(candidate: Candidate, context) -> RuleResult:
    """Evaluate Item 134 for a candidate."""
    reasons = []
    evidence_needed = []
    distances = {}

    # Must be in/near a large shopping centre
    nearby_centres = context.shopping_centres_within_radius(
        candidate.latitude, candidate.longitude, CENTRE_RADIUS_KM
    )

    if not nearby_centres:
        reasons.append("FAIL: No shopping centre within 300m — not a shopping centre site")
        return RuleResult(item="Item 134", passed=False, reasons=reasons,
                         confidence=0.0, distances=distances)

    # Use the nearest centre
    centre, centre_dist = nearby_centres[0]
    distances["centre_name"] = centre['name']
    distances["centre_distance_m"] = round(centre_dist * 1000, 0)

    # --- Check centre qualifies as LARGE shopping centre ---

    centre_gla = centre.get('estimated_gla') or centre.get('gla_sqm') or 0
    centre_tenants = centre.get('estimated_tenants') or 0
    distances["centre_gla_sqm"] = centre_gla
    distances["centre_tenants"] = centre_tenants

    # GLA check
    if centre_gla < MIN_CENTRE_GLA:
        if centre_gla == 0:
            reasons.append(
                f"UNKNOWN: Centre {centre['name']} GLA unknown — may still qualify"
            )
            evidence_needed.append("Obtain actual centre GLA — must be ≥ 5,000 sqm")
        else:
            reasons.append(
                f"FAIL: Centre {centre['name']} GLA = {centre_gla:.0f} sqm "
                f"(need ≥ {MIN_CENTRE_GLA} sqm)"
            )
            return RuleResult(item="Item 134", passed=False, reasons=reasons,
                            evidence_needed=evidence_needed, confidence=0.0,
                            distances=distances)
    else:
        reasons.append(f"PASS: Centre GLA = {centre_gla:.0f} sqm (≥ {MIN_CENTRE_GLA})")

    # Tenant count check — must be ≥ 50 for LARGE centre
    if centre_tenants < MIN_TENANTS and centre_tenants > 0:
        reasons.append(
            f"FAIL: Centre has {centre_tenants} tenants (need ≥ {MIN_TENANTS} for large centre)"
        )
        return RuleResult(item="Item 134", passed=False, reasons=reasons,
                        evidence_needed=evidence_needed, confidence=0.0,
                        distances=distances)
    elif centre_tenants == 0:
        reasons.append("UNKNOWN: Tenant count not available — assumed to meet threshold")
        evidence_needed.append("Verify centre has ≥ 50 commercial establishments")
    else:
        reasons.append(f"PASS: Centre has {centre_tenants} tenants (≥ {MIN_TENANTS})")

    # Supermarket ≥ 2,500 sqm GLA check
    nearby_supers = context.supermarkets_within_radius(
        candidate.latitude, candidate.longitude, CENTRE_RADIUS_KM
    )
    has_large_super = False
    for s, sd in nearby_supers:
        gla = s.get('estimated_gla') or s.get('floor_area_sqm') or 0
        if gla >= MIN_SUPERMARKET_GLA:
            has_large_super = True
            reasons.append(
                f"PASS: Supermarket {s['name']} GLA = {gla:.0f} sqm (≥ {MIN_SUPERMARKET_GLA})"
            )
            break

    if not has_large_super:
        if nearby_supers:
            reasons.append(
                f"FAIL: No supermarket ≥ {MIN_SUPERMARKET_GLA} sqm in centre"
            )
        else:
            reasons.append("FAIL: No supermarket found in centre")
        evidence_needed.append("Verify supermarket GLA in centre ≥ 2,500 sqm")
        return RuleResult(item="Item 134", passed=False, reasons=reasons,
                        evidence_needed=evidence_needed, confidence=0.0,
                        distances=distances)

    # --- No existing pharmacy within the centre ---
    pharmas_in_centre = context.pharmacies_within_radius(
        centre['latitude'], centre['longitude'], PHARMACY_IN_CENTRE_KM
    )
    if pharmas_in_centre:
        p, pd = pharmas_in_centre[0]
        reasons.append(
            f"FAIL: Existing pharmacy {p['name']} within {pd*1000:.0f}m of centre — "
            f"likely already in the centre (Item 134 requires NO existing pharmacy)"
        )
        evidence_needed.append("Verify if existing pharmacy is actually inside the shopping centre")
        return RuleResult(
            item="Item 134", passed=False, reasons=reasons,
            evidence_needed=evidence_needed, confidence=0.0, distances=distances,
        )

    reasons.append("PASS: No existing pharmacy within the shopping centre")

    # --- Confidence ---
    conf = 0.80
    # Reduce if tenant count unknown
    if centre_tenants == 0:
        conf *= 0.6
    # Reduce if GLA unknown
    if centre_gla == 0:
        conf *= 0.5

    evidence_needed.extend([
        "Verify centre meets 'large shopping centre' definition (single management, parking)",
        "Verify centre has ≥ 50 commercial establishments (count per Rules definition)",
        "Confirm no existing approved pharmacy within the centre",
        "Verify public access door midpoints for the proposed premises",
    ])

    return RuleResult(
        item="Item 134", passed=True, reasons=reasons,
        evidence_needed=evidence_needed, confidence=round(conf, 3),
        distances=distances,
    )
