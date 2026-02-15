"""
Rule Item 134A: New additional pharmacy in a designated complex
(large shopping centre with approved premises)

From ACPA Handbook (Jan 2024, V1.9):

Requirements:
(a) The proposed premises are in a large shopping centre, AND
(b) If the large shopping centre contains:
    (i)  at least 100, but fewer than 200, commercial establishments —
         there is only 1 approved premises in the large shopping centre, OR
    (ii) at least 200 commercial establishments —
         there are at least 1 but no more than 2 approved premises in the
         large shopping centre, AND
(c) No approved premises have relocated OUT of the large shopping centre
    in the 12 months immediately before the day the application was made.

A "large shopping centre" is the same definition as Item 134:
- Single management
- GLA >= 5,000sqm
- Supermarket GLA >= 2,500sqm
- >= 50 commercial establishments
- Customer parking

NO minimum distance from nearest pharmacy.

Restriction: Must stay in same shopping centre for 10 years (unless exceptional circumstances).
"""
from typing import Dict, Optional, Tuple, List
from rules.base_rule import BaseRule
from utils.distance import haversine_distance, find_within_radius, format_distance
import config
import json


class Item134ARule(BaseRule):
    """
    Item 134A: New additional pharmacy in a large shopping centre that already has one.

    Same large shopping centre definition as Item 134, but:
    - 100-199 tenants: max 1 existing pharmacy (allows a 2nd)
    - >= 200 tenants: max 2 existing pharmacies (allows a 3rd)
    - No pharmacy relocated out in last 12 months

    NO minimum distance from nearest pharmacy.
    """

    @property
    def rule_name(self) -> str:
        return "Large Shopping Centre - With Existing Pharmacy (134A)"

    @property
    def item_number(self) -> str:
        return "Item 134A"

    def check_eligibility(self, property_data: Dict) -> Tuple[bool, Optional[str]]:
        """Check if property meets Item 134A requirements."""
        lat = property_data.get('latitude')
        lon = property_data.get('longitude')

        if lat is None or lon is None:
            return False, None

        centres = self.db.get_all_shopping_centres()
        if not centres:
            return False, None

        pharmacies = self.db.get_all_pharmacies()

        for centre in centres:
            # Check if property is within the centre (200m proxy)
            distance_to_centre = haversine_distance(
                lat, lon,
                centre['latitude'], centre['longitude']
            )
            if distance_to_centre > 0.2:
                continue

            # Check centre qualifies as "large shopping centre"
            gla = centre.get('gla_sqm', 0) or centre.get('estimated_gla', 0) or 0
            if gla < config.SHOPPING_CENTRE_THRESHOLDS['large_centre_gla']:
                continue

            # Must have >= 50 commercial establishments (base for large shopping centre)
            tenants = centre.get('estimated_tenants', 0) or 0
            if tenants < config.SHOPPING_CENTRE_THRESHOLDS['large_centre_tenants']:
                continue

            # Count pharmacies currently in the centre
            pharmacies_in_centre = self._count_pharmacies_in_centre(centre, pharmacies)

            # (b) Check tenant count thresholds for additional pharmacy allowance
            if tenants >= 200:
                # >= 200 tenants: must have 1-2 existing pharmacies
                if pharmacies_in_centre < 1 or pharmacies_in_centre > 2:
                    continue
            elif tenants >= 100:
                # 100-199 tenants: must have exactly 1 existing pharmacy
                if pharmacies_in_centre != 1:
                    continue
            else:
                # < 100 tenants: does not qualify for 134A
                continue

            # Check supermarket with GLA >= 2,500sqm
            has_supermarket = self._check_supermarket_in_centre(centre)

            # Build evidence
            supermarkets_info = centre.get('major_supermarkets', [])
            if isinstance(supermarkets_info, str):
                try:
                    supermarkets_info = json.loads(supermarkets_info)
                except:
                    supermarkets_info = [supermarkets_info]

            tier = "100-199" if tenants < 200 else "200+"
            max_existing = 1 if tenants < 200 else 2

            evidence = self.format_evidence(
                rule="Item 134A: Additional pharmacy in large shopping centre",
                centre_name=centre.get('name', 'Unknown'),
                centre_gla=f"{gla:,.0f}sqm (>= 5,000sqm ✓)",
                tenants=f"{tenants} tenants ({tier} tier, max {max_existing} existing pharmacies allowed)",
                existing_pharmacies=f"{pharmacies_in_centre} pharmacy(ies) in centre",
                supermarkets=", ".join(supermarkets_info) if supermarkets_info else "VERIFY",
                distance_note="NO minimum distance required",
                note="VERIFY: single management, customer parking, no pharmacy relocated out in last 12 months"
            )
            return True, evidence

        return False, None

    def _count_pharmacies_in_centre(self, centre: Dict, pharmacies: List[Dict]) -> int:
        """Count how many pharmacies are inside this shopping centre."""
        count = 0
        for pharm in pharmacies:
            d = haversine_distance(
                centre['latitude'], centre['longitude'],
                pharm.get('latitude', 0), pharm.get('longitude', 0)
            )
            if d <= 0.15:  # 150m - inside the centre
                count += 1
        return count

    def _check_supermarket_in_centre(self, centre: Dict) -> bool:
        """Check if there's a supermarket with GLA >= 2,500sqm in the centre."""
        supermarkets = self.db.get_all_supermarkets()
        if not supermarkets:
            supermarket_list = centre.get('major_supermarkets', [])
            if isinstance(supermarket_list, str):
                try:
                    supermarket_list = json.loads(supermarket_list)
                except:
                    supermarket_list = [supermarket_list] if supermarket_list else []
            return len(supermarket_list) > 0

        nearby = find_within_radius(
            centre['latitude'], centre['longitude'],
            supermarkets, 0.3
        )

        for sm, dist in nearby:
            area = sm.get('floor_area_sqm', 0) or sm.get('estimated_gla', 0) or 0
            if area >= config.SHOPPING_CENTRE_THRESHOLDS['supermarket_gla']:
                return True

        # Fallback
        supermarket_list = centre.get('major_supermarkets', [])
        if isinstance(supermarket_list, str):
            try:
                supermarket_list = json.loads(supermarket_list)
            except:
                supermarket_list = [supermarket_list] if supermarket_list else []
        return len(supermarket_list) > 0
