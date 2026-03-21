"""
Item 134A — Large shopping centre pharmacy (additional, with existing pharmacy).

Requirements:
(a) Proposed premises in a LARGE SHOPPING CENTRE:
    - Single management
    - GLA ≥ 5,000 sqm
    - Contains supermarket ≥ 2,500 sqm GLA
    - ≥ 50 other commercial establishments
    - Customer parking
(b) Tenant count determines max existing pharmacies:
    - 100-199 commercial establishments → max 1 existing pharmacy (allows 2nd)
    - ≥ 200 commercial establishments → max 2 existing pharmacies (allows 3rd)
(c) No pharmacy has relocated OUT of the centre in last 12 months

Note: NO distance requirement from pharmacies outside the centre.
Unlike Item 134, this item REQUIRES at least one existing pharmacy in the centre.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from engine.models import Candidate, RuleResult

MIN_CENTRE_GLA = 5000           # sqm
MIN_TENANTS_LARGE = 50          # ≥ 50 for large centre definition
MIN_TENANTS_TIER1 = 100         # 100-199 tenants → allows 2nd pharmacy (max 1 existing)
MIN_TENANTS_TIER2 = 200         # ≥ 200 tenants → allows 3rd pharmacy (max 2 existing)
MIN_SUPERMARKET_GLA = 2500      # sqm
CENTRE_RADIUS_KM = 0.3          # Must be within 300m (inside/adjacent to centre)
PHARMACY_IN_CENTRE_KM = 0.3     # Proxy for "within the shopping centre"


def check_item_134a(candidate: Candidate, context) -> RuleResult:
    """Evaluate Item 134A for a candidate."""
    reasons = []
    evidence_needed = []
    distances = {}

    # Must be in/near a large shopping centre
    nearby_centres = context.shopping_centres_within_radius(
        candidate.latitude, candidate.longitude, CENTRE_RADIUS_KM
    )

    if not nearby_centres:
        reasons.append("FAIL: No shopping centre within 300m — not a shopping centre site")
        return RuleResult(item="Item 134A", passed=False, reasons=reasons,
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
            return RuleResult(item="Item 134A", passed=False, reasons=reasons,
                            evidence_needed=evidence_needed, confidence=0.0,
                            distances=distances)
    else:
        reasons.append(f"PASS: Centre GLA = {centre_gla:.0f} sqm (≥ {MIN_CENTRE_GLA})")

    # Tenant count — must be ≥ 50 for large centre, AND ≥ 100 for Item 134A to apply
    if centre_tenants > 0 and centre_tenants < MIN_TENANTS_LARGE:
        reasons.append(
            f"FAIL: Centre has {centre_tenants} tenants (need ≥ {MIN_TENANTS_LARGE} for large centre)"
        )
        return RuleResult(item="Item 134A", passed=False, reasons=reasons,
                        evidence_needed=evidence_needed, confidence=0.0,
                        distances=distances)

    if centre_tenants > 0 and centre_tenants < MIN_TENANTS_TIER1:
        reasons.append(
            f"FAIL: Centre has {centre_tenants} tenants (need ≥ {MIN_TENANTS_TIER1} for Item 134A). "
            f"With 50-99 tenants, only Item 134 applies (no existing pharmacy allowed)"
        )
        return RuleResult(item="Item 134A", passed=False, reasons=reasons,
                        evidence_needed=evidence_needed, confidence=0.0,
                        distances=distances)

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
            reasons.append(f"FAIL: No supermarket ≥ {MIN_SUPERMARKET_GLA} sqm in centre")
        else:
            reasons.append("FAIL: No supermarket found in centre")
        evidence_needed.append("Verify supermarket GLA in centre ≥ 2,500 sqm")
        return RuleResult(item="Item 134A", passed=False, reasons=reasons,
                        evidence_needed=evidence_needed, confidence=0.0,
                        distances=distances)

    # --- Count existing pharmacies within the centre ---
    pharmas_in_centre = context.pharmacies_within_radius(
        centre['latitude'], centre['longitude'], PHARMACY_IN_CENTRE_KM
    )
    existing_count = len(pharmas_in_centre)
    distances["existing_pharmacies_in_centre"] = existing_count

    if existing_count == 0:
        reasons.append(
            "FAIL: No existing pharmacy in the centre — use Item 134 instead (no existing pharmacy)"
        )
        return RuleResult(item="Item 134A", passed=False, reasons=reasons,
                        evidence_needed=evidence_needed, confidence=0.0,
                        distances=distances)

    # Determine max allowed existing pharmacies based on tenant count
    if centre_tenants == 0:
        # Unknown tenant count — can't determine tier
        reasons.append("UNKNOWN: Tenant count not available — cannot determine pharmacy cap")
        evidence_needed.append("Verify centre tenant count to determine Item 134A eligibility")
        # Assume could qualify — low confidence
        max_existing = 2  # Optimistic assumption
    elif centre_tenants >= MIN_TENANTS_TIER2:
        # ≥ 200 tenants → allows up to 2 existing (can add 3rd)
        max_existing = 2
        reasons.append(
            f"PASS: Centre has {centre_tenants} tenants (≥ {MIN_TENANTS_TIER2}) — "
            f"allows up to 2 existing pharmacies (3rd can be added)"
        )
    elif centre_tenants >= MIN_TENANTS_TIER1:
        # 100-199 tenants → allows up to 1 existing (can add 2nd)
        max_existing = 1
        reasons.append(
            f"PASS: Centre has {centre_tenants} tenants ({MIN_TENANTS_TIER1}-{MIN_TENANTS_TIER2-1}) — "
            f"allows up to 1 existing pharmacy (2nd can be added)"
        )
    else:
        # Should have been caught above, but safety net
        max_existing = 0

    # Check if existing count exceeds max
    if existing_count > max_existing:
        reasons.append(
            f"FAIL: Centre already has {existing_count} pharmacies — "
            f"max allowed for this tenant tier is {max_existing}"
        )
        return RuleResult(item="Item 134A", passed=False, reasons=reasons,
                        evidence_needed=evidence_needed, confidence=0.0,
                        distances=distances)

    reasons.append(
        f"PASS: Centre has {existing_count} existing pharmacy(ies), "
        f"max allowed is {max_existing} — room for additional"
    )

    # --- Confidence ---
    conf = 0.75
    # Reduce if tenant count unknown
    if centre_tenants == 0:
        conf *= 0.5
    # Reduce if GLA unknown
    if centre_gla == 0:
        conf *= 0.5

    evidence_needed.extend([
        "Verify centre meets 'large shopping centre' definition (single management, parking)",
        "Verify exact tenant count per Rules definition (critical for Item 134A tiers)",
        "Confirm existing pharmacy count within the centre",
        "Verify no pharmacy has relocated OUT of this centre in the last 12 months",
        "Verify public access door midpoints for the proposed premises",
    ])

    return RuleResult(
        item="Item 134A", passed=True, reasons=reasons,
        evidence_needed=evidence_needed, confidence=round(conf, 3),
        distances=distances,
    )
