"""
Item 133 — Small shopping centre pharmacy.

Requirements:
- Located in a shopping centre with:
  - GLA ≥ 5,000 sqm
  - ≥ 15 commercial tenants
  - At least one supermarket ≥ 2,500 sqm GLA
- (b) At least 500m straight-line from nearest approved pharmacy,
  EXCLUDING pharmacies in large shopping centres or private hospitals
- (c) No existing pharmacy in the small shopping centre
- Not in a large shopping centre or large hospital

Measurement: geodesic straight-line distance.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from engine.models import Candidate, RuleResult
from engine.rules.general import confidence_from_margin_m

PHARMACY_DISTANCE_KM = 0.5     # 500m
MIN_CENTRE_GLA = 5000          # sqm
MIN_TENANTS = 15
MIN_SUPERMARKET_GLA = 2500     # sqm
# Large centre threshold (Items 134/134A) — centres above this are NOT Item 133
LARGE_CENTRE_TENANTS = 50
# Radius to check for pharmacy inside the centre
PHARMACY_IN_CENTRE_KM = 0.1    # 100m — pharmacy within this distance of centre centroid = "in the centre"


def check_item_133(candidate: Candidate, context) -> RuleResult:
    """Evaluate Item 133 for a candidate."""
    reasons = []
    evidence_needed = []
    distances = {}

    # Must be in/near a shopping centre
    nearby_centres = context.shopping_centres_within_radius(
        candidate.latitude, candidate.longitude, 0.3  # 300m — must be in/very near centre
    )

    if not nearby_centres:
        reasons.append("FAIL: No shopping centre within 300m — not a shopping centre site")
        return RuleResult(item="Item 133", passed=False, reasons=reasons, confidence=0.0, distances=distances)

    # Use the nearest centre
    centre, centre_dist = nearby_centres[0]
    distances["centre_name"] = centre['name']
    distances["centre_distance_m"] = round(centre_dist * 1000, 0)

    # Check centre characteristics
    centre_gla = centre.get('estimated_gla') or centre.get('gla_sqm') or 0
    centre_tenants = centre.get('estimated_tenants') or 0
    distances["centre_gla_sqm"] = centre_gla
    distances["centre_tenants"] = centre_tenants

    # Must NOT be a large centre (≥50 tenants → Items 134/134A instead)
    if centre_tenants >= LARGE_CENTRE_TENANTS:
        reasons.append(
            f"FAIL: Centre {centre['name']} has {centre_tenants} tenants — "
            f"this is a large centre (≥{LARGE_CENTRE_TENANTS}), use Item 134/134A instead"
        )
        return RuleResult(item="Item 133", passed=False, reasons=reasons, confidence=0.0, distances=distances)

    # Centre GLA check
    if centre_gla < MIN_CENTRE_GLA:
        reasons.append(
            f"FAIL: Centre {centre['name']} GLA = {centre_gla:.0f} sqm (need ≥ {MIN_CENTRE_GLA} sqm)"
        )
        evidence_needed.append("Verify centre GLA from management/planning docs")
        if centre_gla == 0:
            reasons[-1] += " — GLA unknown, may still qualify"
            evidence_needed.append("Obtain actual centre GLA")
        else:
            return RuleResult(item="Item 133", passed=False, reasons=reasons,
                            evidence_needed=evidence_needed, confidence=0.0, distances=distances)

    if centre_gla >= MIN_CENTRE_GLA:
        reasons.append(f"PASS: Centre GLA = {centre_gla:.0f} sqm (≥ {MIN_CENTRE_GLA})")

    # Tenant count check
    if centre_tenants < MIN_TENANTS and centre_tenants > 0:
        reasons.append(
            f"FAIL: Centre has {centre_tenants} tenants (need ≥ {MIN_TENANTS})"
        )
        return RuleResult(item="Item 133", passed=False, reasons=reasons,
                        evidence_needed=evidence_needed, confidence=0.0, distances=distances)
    elif centre_tenants == 0:
        reasons.append(f"UNKNOWN: Tenant count not available — assumed to meet threshold")
        evidence_needed.append("Verify centre has ≥ 15 commercial tenants")
    else:
        reasons.append(f"PASS: Centre has {centre_tenants} tenants (≥ {MIN_TENANTS})")

    # Supermarket GLA check
    nearby_supers = context.supermarkets_within_radius(
        candidate.latitude, candidate.longitude, 0.3
    )
    has_large_super = False
    for s, sd in nearby_supers:
        gla = s.get('estimated_gla') or s.get('floor_area_sqm') or 0
        if gla >= MIN_SUPERMARKET_GLA:
            has_large_super = True
            reasons.append(f"PASS: Supermarket {s['name']} GLA = {gla:.0f} sqm (≥ {MIN_SUPERMARKET_GLA})")
            break

    if not has_large_super:
        if nearby_supers:
            reasons.append(
                f"FAIL: No supermarket ≥ {MIN_SUPERMARKET_GLA} sqm in centre"
            )
        else:
            reasons.append("FAIL: No supermarket found in centre")
        evidence_needed.append("Verify supermarket GLA in centre")
        return RuleResult(item="Item 133", passed=False, reasons=reasons,
                        evidence_needed=evidence_needed, confidence=0.0, distances=distances)

    # (c) No existing pharmacy in the shopping centre
    # Bug 7 fix: explicitly check for pharmacies inside the centre (within 100m of centroid)
    pharmacies_in_centre = context.pharmacies_within_radius(
        centre['latitude'], centre['longitude'], PHARMACY_IN_CENTRE_KM
    )
    if pharmacies_in_centre:
        p_in, p_in_dist = pharmacies_in_centre[0]
        reasons.append(
            f"FAIL (c): Existing pharmacy {p_in['name']} in the small shopping centre "
            f"({p_in_dist*1000:.0f}m from centre centroid)"
        )
        return RuleResult(item="Item 133", passed=False, reasons=reasons,
                        evidence_needed=evidence_needed, confidence=0.0, distances=distances)
    reasons.append("PASS (c): No existing pharmacy in the small shopping centre")

    # (b) Distance check: ≥ 500m from nearest pharmacy EXCLUDING those in large SCs or private hospitals
    # Bug 2 fix: use nearest_pharmacy_excluding_complexes instead of nearest_pharmacy
    nearest_pharm, nearest_dist = context.nearest_pharmacy_excluding_complexes(
        candidate.latitude, candidate.longitude
    )
    nearest_dist_m = nearest_dist * 1000
    distances["nearest_pharmacy_m"] = round(nearest_dist_m, 0) if nearest_dist_m != float('inf') else 999000
    distances["nearest_pharmacy_name"] = nearest_pharm['name'] if nearest_pharm else "None"

    if not nearest_pharm:
        nearest_dist_m = 999000
        margin_m = nearest_dist_m - 500
        reasons.append("PASS (b): No non-excluded pharmacy found nearby")
    elif nearest_dist_m < 500:
        reasons.append(
            f"FAIL (b): Nearest non-excluded pharmacy {nearest_pharm['name']} = {nearest_dist_m:.0f}m "
            f"(need ≥ 500m; pharmacies in large shopping centres/private hospitals are excluded)"
        )
        return RuleResult(item="Item 133", passed=False, reasons=reasons,
                        evidence_needed=evidence_needed, confidence=0.0, distances=distances)
    else:
        margin_m = nearest_dist_m - 500
        reasons.append(
            f"PASS (b): Nearest non-excluded pharmacy = {nearest_dist_m:.0f}m (margin +{margin_m:.0f}m)"
        )

    evidence_needed.extend([
        "Verify centre meets 'shopping centre' definition under Rules",
        "Verify public access door midpoints",
    ])

    conf = confidence_from_margin_m(margin_m)
    # Reduce confidence if tenant count unknown
    if centre_tenants == 0:
        conf *= 0.7
    # Reduce if GLA is estimated
    if centre_gla == 0:
        conf *= 0.5

    return RuleResult(
        item="Item 133", passed=True, reasons=reasons,
        evidence_needed=evidence_needed, confidence=round(conf, 3), distances=distances,
    )
