"""
Item 132 — Same-town additional pharmacy.

Requirements:
(a)(i)   Candidate must be in the same town as an approved pharmacy
         ("same town" = same suburb name + same postcode)
(a)(ii)  At least 200m straight-line from nearest approved pharmacy
(a)(iii) At least 10km by shortest lawful route from any OTHER approved pharmacy
         (i.e., pharmacies beyond the nearest)
(b)(i)   At least 4 FTE prescribing medical practitioners in the town
(b)(ii)  1 or 2 supermarkets in the same town with combined GLA ≥ 2,500 sqm

Measurement:
- 200m: geodesic straight-line
- 10km: OSRM route distance (estimated for Stage A)
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from engine.models import Candidate, RuleResult
from engine.rules.general import confidence_from_margin_m

NEAREST_DISTANCE_KM = 0.2     # 200m
OTHER_ROUTE_KM = 10.0         # 10km by road
MIN_FTE_GPS = 4.0             # 4 FTE prescribers
MIN_SUPERMARKET_GLA = 2500    # sqm combined
MAX_SUPERMARKETS = 2           # Only 1 or 2 supermarkets count

# Town radius for GP/supermarket counting — used as fallback when suburb/postcode unavailable
TOWN_RADIUS_KM = 5.0


def _same_town(item, candidate_suburb, candidate_postcode):
    """Check if an item (pharmacy/GP/supermarket) is in the same town as the candidate.
    Same town = same suburb name + same postcode (case-insensitive for suburb)."""
    item_suburb = (item.get('suburb') or '').strip().lower()
    item_postcode = (item.get('postcode') or '').strip()
    return (item_suburb == candidate_suburb.lower() and
            item_postcode == candidate_postcode)


def check_item_132(candidate: Candidate, context) -> RuleResult:
    """Evaluate Item 132 for a candidate."""
    reasons = []
    evidence_needed = []
    distances = {}

    # Extract candidate's suburb and postcode for same-town matching
    candidate_suburb = (getattr(candidate, 'suburb', '') or
                        (candidate.town_id.split('|')[0] if candidate.town_id and '|' in candidate.town_id else '') or
                        '').strip()
    candidate_postcode = (getattr(candidate, 'postcode', '') or
                          (candidate.town_id.split('|')[1] if candidate.town_id and '|' in candidate.town_id else '') or
                          '').strip()

    has_town_data = bool(candidate_suburb and candidate_postcode)

    # (a)(i) Must be in the same town as an existing pharmacy
    if has_town_data:
        same_town_pharmacies = [
            p for p in context.pharmacies
            if _same_town(p, candidate_suburb, candidate_postcode)
        ]
        if not same_town_pharmacies:
            reasons.append(
                f"FAIL (a)(i): No pharmacy in the same town "
                f"(suburb={candidate_suburb}, postcode={candidate_postcode})"
            )
            return RuleResult(item="Item 132", passed=False, reasons=reasons,
                            confidence=0.0, distances=distances)
        reasons.append(
            f"PASS (a)(i): {len(same_town_pharmacies)} pharmacy(ies) in same town "
            f"({candidate_suburb} {candidate_postcode})"
        )
    else:
        evidence_needed.append(
            "Candidate suburb/postcode unavailable — same-town check requires manual verification"
        )

    # (a)(ii) Nearest pharmacy ≥ 200m straight-line
    nearest_pharm, nearest_dist_km = context.nearest_pharmacy(
        candidate.latitude, candidate.longitude
    )
    nearest_dist_m = nearest_dist_km * 1000
    distances["nearest_pharmacy_km"] = round(nearest_dist_km, 4) if nearest_dist_km != float('inf') else 999
    distances["nearest_pharmacy_m"] = round(nearest_dist_m, 1) if nearest_dist_m != float('inf') else 999000
    distances["nearest_pharmacy_name"] = nearest_pharm['name'] if nearest_pharm else "None"

    if not nearest_pharm:
        reasons.append("FAIL: No pharmacies found — Item 132 is 'additional pharmacy in town'")
        return RuleResult(item="Item 132", passed=False, reasons=reasons, confidence=0.0, distances=distances)

    if nearest_dist_m < 200:
        reasons.append(
            f"FAIL (a)(ii): Nearest pharmacy {nearest_pharm['name']} is {nearest_dist_m:.0f}m "
            f"(need ≥ 200m straight-line)"
        )
        return RuleResult(item="Item 132", passed=False, reasons=reasons, confidence=0.0, distances=distances)

    margin_200m = nearest_dist_m - 200
    reasons.append(
        f"PASS (a)(ii): Nearest pharmacy {nearest_pharm['name']} = {nearest_dist_m:.0f}m "
        f"(margin +{margin_200m:.0f}m)"
    )

    # (a)(iii) Other pharmacies must be ≥ 10km by route
    nearby_pharmas = context.pharmacies_within_radius(
        candidate.latitude, candidate.longitude, 8.0
    )
    other_pharmas = [(p, d) for p, d in nearby_pharmas
                     if p['id'] != nearest_pharm['id']]

    other_too_close = False
    for p, geodesic_d in other_pharmas:
        estimated_route = geodesic_d * 1.4
        if estimated_route < OTHER_ROUTE_KM:
            other_too_close = True
            reasons.append(
                f"FAIL (a)(iii): Other pharmacy {p['name']} estimated {estimated_route:.1f}km by road "
                f"(geodesic {geodesic_d:.2f}km, need ≥ {OTHER_ROUTE_KM}km route from ALL other pharmacies)"
            )
            distances[f"other_pharmacy_{p['id']}_est_route_km"] = round(estimated_route, 2)
            break

    if other_too_close:
        return RuleResult(item="Item 132", passed=False, reasons=reasons, confidence=0.0, distances=distances)

    if other_pharmas:
        reasons.append(
            f"PASS (a)(iii): {len(other_pharmas)} other pharmacies within 8km geodesic, "
            f"all estimated ≥ {OTHER_ROUTE_KM}km by road"
        )
        evidence_needed.append("Verify OSRM route distances for all other pharmacies")
    else:
        reasons.append("PASS (a)(iii): No other pharmacies within 8km geodesic")

    # (b)(i) At least 4 FTE prescribing medical practitioners in town
    if has_town_data:
        # Filter GPs to same town (suburb + postcode)
        same_town_gps = [
            (g, context.geodesic_km(candidate.latitude, candidate.longitude,
                                     g['latitude'], g['longitude']))
            for g in context.gps
            if _same_town(g, candidate_suburb, candidate_postcode)
        ]
        total_fte = sum(g.get('fte', 1.0) or 1.0 for g, _ in same_town_gps)
        distances["gps_in_town"] = len(same_town_gps)
        distances["total_fte_in_town"] = round(total_fte, 1)
    else:
        # Fallback to radius-based search
        nearby_gps = context.gps_within_radius(
            candidate.latitude, candidate.longitude, TOWN_RADIUS_KM
        )
        same_town_gps = nearby_gps
        total_fte = sum(g.get('fte', 1.0) or 1.0 for g, _ in nearby_gps)
        distances["gps_in_town"] = len(nearby_gps)
        distances["total_fte_in_town"] = round(total_fte, 1)

    if total_fte < MIN_FTE_GPS:
        reasons.append(
            f"FAIL (b)(i): Only {total_fte:.1f} FTE GPs in town "
            f"(need ≥ {MIN_FTE_GPS} FTE)"
        )
        evidence_needed.append("Verify GP FTE counts from Medicare/AHPRA data")
        return RuleResult(
            item="Item 132", passed=False, reasons=reasons,
            evidence_needed=evidence_needed, confidence=0.0, distances=distances,
        )

    reasons.append(
        f"PASS (b)(i): {total_fte:.1f} FTE GPs in town ({len(same_town_gps)} practices)"
    )

    # (b)(ii) 1 or 2 supermarkets in same town with combined GLA ≥ 2,500 sqm
    if has_town_data:
        same_town_supers = [
            s for s in context.supermarkets
            if _same_town(s, candidate_suburb, candidate_postcode)
        ]
    else:
        # Fallback to radius-based
        same_town_supers = [s for s, _ in context.supermarkets_within_radius(
            candidate.latitude, candidate.longitude, TOWN_RADIUS_KM
        )]

    # Sort by GLA descending, take top 2 only (handbook says "1 or 2 supermarkets")
    def _get_gla(s):
        return s.get('estimated_gla') or s.get('floor_area_sqm') or 0

    same_town_supers.sort(key=_get_gla, reverse=True)
    top_supers = same_town_supers[:MAX_SUPERMARKETS]

    combined_gla = sum(_get_gla(s) for s in top_supers)
    distances["supermarkets_in_town"] = len(same_town_supers)
    distances["supermarkets_counted"] = len(top_supers)
    distances["combined_gla_sqm"] = round(combined_gla, 0)

    if combined_gla < MIN_SUPERMARKET_GLA:
        reasons.append(
            f"FAIL (b)(ii): Combined supermarket GLA = {combined_gla:.0f} sqm "
            f"from top {len(top_supers)} of {len(same_town_supers)} supermarkets "
            f"(need ≥ {MIN_SUPERMARKET_GLA} sqm from 1 or 2 supermarkets)"
        )
        evidence_needed.append("Verify supermarket GLA from council records")
        return RuleResult(
            item="Item 132", passed=False, reasons=reasons,
            evidence_needed=evidence_needed, confidence=0.0, distances=distances,
        )

    reasons.append(
        f"PASS (b)(ii): Combined supermarket GLA = {combined_gla:.0f} sqm "
        f"({len(top_supers)} supermarket(s) counted of {len(same_town_supers)} in town)"
    )

    # All checks passed
    evidence_needed.extend([
        "Verify public access door midpoints",
        "Verify OSRM route distances for other pharmacies",
        "Verify GP FTE from AHPRA/Medicare",
        "Verify supermarket GLA from planning documents",
    ])

    conf = confidence_from_margin_m(margin_200m)
    if not has_town_data:
        conf *= 0.7  # Reduce confidence when same-town check couldn't be verified
    return RuleResult(
        item="Item 132", passed=True, reasons=reasons,
        evidence_needed=evidence_needed, confidence=conf, distances=distances,
    )
