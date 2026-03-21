"""
Relocation opportunities scanner — Items 121-125.

This module scans for existing pharmacies that could benefit from relocating,
identifying deal opportunities where we approach pharmacy owners with proposals.

Items:
- Item 121: Expansion/contraction (not usually ACPA — skip)
- Item 122: Relocation within a designated complex (same SC/hospital/medical centre)
- Item 123: Relocation within same town (10km from nearest other pharmacy)
- Item 124: Relocation up to 1km
- Item 125: Relocation of 1 to 1.5km

Key constraint: Original approval must have been in force ≥ 5 years (with exceptions).
Exceptions to 5-year rule:
  - Item 122: relocation within same designated complex
  - Item 123: only pharmacy in town relocating within same town
  - Renovation/refurbishment of same premises
  - Disaster/exceptional circumstances (not Item 125)

This is about finding DEALS — pharmacies in suboptimal locations that could
relocate to better spots (new shopping centres, growing corridors, etc).
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Tuple


@dataclass
class RelocationOpportunity:
    """A potential relocation deal to investigate."""
    item: str                          # e.g. "Item 124"
    existing_pharmacy: Dict            # The pharmacy that could relocate
    proposed_location: Optional[Dict]  # Target location (SC, candidate site, etc.)
    reasons: List[str] = field(default_factory=list)
    evidence_needed: List[str] = field(default_factory=list)
    deal_score: float = 0.0           # 0.0-1.0, how attractive this deal is
    distances: Dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "item": self.item,
            "existing_pharmacy": {
                "name": self.existing_pharmacy.get('name', ''),
                "address": self.existing_pharmacy.get('address', ''),
                "latitude": self.existing_pharmacy.get('latitude'),
                "longitude": self.existing_pharmacy.get('longitude'),
            },
            "proposed_location": {
                "name": self.proposed_location.get('name', ''),
                "latitude": self.proposed_location.get('latitude'),
                "longitude": self.proposed_location.get('longitude'),
            } if self.proposed_location else None,
            "reasons": self.reasons,
            "evidence_needed": self.evidence_needed,
            "deal_score": round(self.deal_score, 3),
            "distances": self.distances,
        }


# --- Constants ---
ITEM_122_COMPLEX_TYPES = ('large_shopping_centre', 'small_shopping_centre',
                          'large_medical_centre', 'large_private_hospital')
ITEM_124_MAX_DISTANCE_KM = 1.0          # Relocation up to 1km
ITEM_125_MIN_DISTANCE_KM = 1.0          # Relocation 1 to 1.5km
ITEM_125_MAX_DISTANCE_KM = 1.5
MIN_CENTRE_GLA = 5000                   # Large/small SC minimum
MIN_LARGE_SC_TENANTS = 50               # Large SC
MIN_SMALL_SC_TENANTS = 15               # Small SC
MIN_SUPERMARKET_GLA = 2500              # sqm
CENTRE_RADIUS_KM = 0.3                  # Proximity to be "in" a centre
PHARMACY_IN_CENTRE_KM = 0.3             # Proxy for "within the shopping centre"


def scan_relocation_opportunities(context, state_filter: str = None) -> List[RelocationOpportunity]:
    """
    Scan all existing pharmacies for relocation opportunities.

    Strategies:
    1. Item 122: Pharmacy in a designated complex that could relocate
       within the same complex (e.g. better tenancy within same SC)
    2. Item 124: Pharmacy near a large/new shopping centre that could
       relocate up to 1km to get into the centre
    3. Item 125: Pharmacy 1-1.5km from a shopping centre opening —
       relocate into the centre

    Returns list of RelocationOpportunity sorted by deal_score (best first).
    """
    opportunities = []

    pharmacies = context.pharmacies
    if state_filter:
        pharmacies = [p for p in pharmacies
                      if (p.get('state') or '').upper() == state_filter.upper()]

    # --- Strategy 1: Item 124/125 — Pharmacy near a large shopping centre ---
    # Find pharmacies NOT in a shopping centre that are near a large SC
    # These could relocate INTO the centre
    for pharmacy in pharmacies:
        plat, plon = pharmacy['latitude'], pharmacy['longitude']

        # Check if pharmacy is already in a shopping centre
        centres_at_pharmacy = context.shopping_centres_within_radius(plat, plon, PHARMACY_IN_CENTRE_KM)
        in_centre = len(centres_at_pharmacy) > 0

        # Find large shopping centres within 1.5km (Item 124 + 125 range)
        nearby_large_centres = []
        for sc, sc_dist in context.shopping_centres_within_radius(plat, plon, ITEM_125_MAX_DISTANCE_KM):
            sc_tenants = sc.get('estimated_tenants') or 0
            sc_gla = sc.get('estimated_gla') or sc.get('gla_sqm') or 0

            # Must qualify as large SC (or at least look promising)
            if sc_tenants >= MIN_LARGE_SC_TENANTS or sc_gla >= MIN_CENTRE_GLA:
                nearby_large_centres.append((sc, sc_dist))

        if not nearby_large_centres:
            continue

        for sc, sc_dist in nearby_large_centres:
            dist_m = sc_dist * 1000

            # Skip if pharmacy is already inside this centre
            if in_centre and sc_dist < PHARMACY_IN_CENTRE_KM:
                continue

            # Check if centre already has pharmacy capacity issues
            pharmas_in_sc = context.pharmacies_within_radius(
                sc['latitude'], sc['longitude'], PHARMACY_IN_CENTRE_KM
            )
            # Exclude the pharmacy we're evaluating
            pharmas_in_sc = [(p, d) for p, d in pharmas_in_sc
                             if p.get('id') != pharmacy.get('id')
                             and p.get('name') != pharmacy.get('name')]

            sc_tenants = sc.get('estimated_tenants') or 0
            sc_gla = sc.get('estimated_gla') or sc.get('gla_sqm') or 0

            # Determine which item applies based on distance
            if dist_m <= ITEM_124_MAX_DISTANCE_KM * 1000:
                item = "Item 124"
                item_label = "Relocation up to 1km"
            elif dist_m <= ITEM_125_MAX_DISTANCE_KM * 1000:
                item = "Item 125"
                item_label = "Relocation 1-1.5km"
            else:
                continue

            reasons = []
            evidence_needed = []

            reasons.append(
                f"Pharmacy '{pharmacy.get('name', 'Unknown')}' is {dist_m:.0f}m from "
                f"large shopping centre '{sc['name']}'"
            )

            # Distance requirement for Item 124(b) / 125(b):
            # If existing premises NOT in a designated complex:
            #   No additional distance requirement beyond the 1km/1.5km cap
            # If existing premises ARE in a large SC:
            #   Proposed must be ≥ 300m from all pharmacies NOT in that SC
            # If existing premises ARE in small SC/medical centre/hospital:
            #   Proposed must be ≥ 500m from all pharmacies NOT in that complex

            if not in_centre:
                reasons.append(
                    f"Existing pharmacy is NOT in a designated complex — "
                    f"{item} applies (no additional distance constraint beyond {item_label})"
                )
            else:
                reasons.append(
                    f"Existing pharmacy IS in a designated complex — "
                    f"additional distance checks may apply"
                )
                evidence_needed.append(
                    "Verify existing pharmacy's complex type and applicable distance constraints"
                )

            # Deal score factors
            score = 0.5

            # Better score if centre is large and has room
            if sc_tenants >= 200:
                score += 0.15
                reasons.append(f"Large centre ({sc_tenants} tenants) — high foot traffic")
            elif sc_tenants >= 100:
                score += 0.10
                reasons.append(f"Mid-size centre ({sc_tenants} tenants)")
            elif sc_tenants >= 50:
                score += 0.05
                reasons.append(f"Centre has {sc_tenants} tenants")

            # Better score if centre has no pharmacy yet
            if len(pharmas_in_sc) == 0:
                score += 0.20
                reasons.append("Centre has NO existing pharmacy — prime opportunity")
            elif len(pharmas_in_sc) == 1:
                score += 0.05
                reasons.append(f"Centre has 1 existing pharmacy")
            else:
                score -= 0.10
                reasons.append(f"Centre already has {len(pharmas_in_sc)} pharmacies")

            # Closer is generally better for the pharmacy owner
            if dist_m < 500:
                score += 0.10
            elif dist_m < 800:
                score += 0.05

            evidence_needed.extend([
                "Verify original approval has been in force ≥ 5 years (or exception applies)",
                f"Verify centre '{sc['name']}' meets designated complex definition",
                "Assess commercial viability of relocation for pharmacy owner",
                "Check if vacancies available in the centre",
            ])

            opportunities.append(RelocationOpportunity(
                item=item,
                existing_pharmacy=pharmacy,
                proposed_location=sc,
                reasons=reasons,
                evidence_needed=evidence_needed,
                deal_score=min(1.0, max(0.0, score)),
                distances={
                    "pharmacy_to_centre_m": round(dist_m, 0),
                    "centre_name": sc['name'],
                    "centre_tenants": sc_tenants,
                    "centre_gla_sqm": sc_gla,
                    "existing_pharmacies_in_centre": len(pharmas_in_sc),
                },
            ))

    # --- Strategy 2: Item 122 — Relocation within same designated complex ---
    # Find pharmacies already in a shopping centre — could move to better spot
    # within the same centre (e.g. better visibility, larger tenancy)
    for pharmacy in pharmacies:
        plat, plon = pharmacy['latitude'], pharmacy['longitude']
        centres_at_pharmacy = context.shopping_centres_within_radius(plat, plon, PHARMACY_IN_CENTRE_KM)

        if not centres_at_pharmacy:
            continue

        sc, sc_dist = centres_at_pharmacy[0]
        sc_tenants = sc.get('estimated_tenants') or 0

        # Only flag if the centre is large enough to likely have better spots
        if sc_tenants < MIN_SMALL_SC_TENANTS:
            continue

        reasons = [
            f"Pharmacy '{pharmacy.get('name', 'Unknown')}' is inside "
            f"'{sc['name']}' ({sc_tenants} tenants)",
            "Item 122 allows relocation within the same designated complex",
            "5-year rule exception applies for within-complex relocations",
            "Opportunity: pharmacy could move to a better tenancy within the same centre",
        ]

        evidence_needed = [
            "Verify centre meets designated complex definition",
            "Check vacancy availability in the centre",
            "Assess if current tenancy is suboptimal (size, visibility, foot traffic)",
            "Contact pharmacy owner to discuss relocation within centre",
        ]

        score = 0.3  # Lower base — this is about optimisation, not greenfield
        if sc_tenants >= 100:
            score += 0.1  # Bigger centre = more options

        opportunities.append(RelocationOpportunity(
            item="Item 122",
            existing_pharmacy=pharmacy,
            proposed_location=sc,
            reasons=reasons,
            evidence_needed=evidence_needed,
            deal_score=round(score, 3),
            distances={
                "pharmacy_to_centre_m": round(sc_dist * 1000, 0),
                "centre_name": sc['name'],
                "centre_tenants": sc_tenants,
            },
        ))

    # Sort by deal score (best first)
    opportunities.sort(key=lambda o: o.deal_score, reverse=True)
    return opportunities
