"""
Rule Item 134: New pharmacy in a designated complex (large shopping centre with no approved premises)

From ACPA Handbook (Jan 2024, V1.9):

Requirements:
(a) The proposed premises are in a large shopping centre, AND
(b) There are no approved premises in the large shopping centre.

A "large shopping centre" is defined as a group of shops and associated facilities that:
- Is under single management
- Has a gross leasable area of at least 5,000 sqm
- Contains a supermarket with GLA of at least 2,500 sqm
- Contains at least 50 other commercial establishments
- Has customer parking facilities

NO minimum distance from nearest pharmacy.

Restriction: Must stay in same shopping centre for 10 years (unless exceptional circumstances).
"""
from typing import Dict, Optional, Tuple, List
from rules.base_rule import BaseRule
from utils.distance import haversine_distance, find_within_radius, format_distance
import config
import json


class Item134Rule(BaseRule):
    """
    Item 134: New pharmacy in a large shopping centre (no existing pharmacy).

    Must be INSIDE a qualifying large shopping centre:
    - Single management
    - Centre GLA >= 5,000sqm
    - Supermarket GLA >= 2,500sqm
    - >= 50 commercial establishments
    - Customer parking
    - NO existing pharmacy in the centre

    NO minimum distance from nearest pharmacy required.
    """

    @property
    def rule_name(self) -> str:
        return "Large Shopping Centre - No Existing Pharmacy (>=50 tenants)"

    @property
    def item_number(self) -> str:
        return "Item 134"

    def check_eligibility(self, property_data: Dict) -> Tuple[bool, Optional[str]]:
        """Check if property meets Item 134 requirements."""
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

            # Must have >= 50 commercial establishments
            tenants = centre.get('estimated_tenants', 0) or 0
            min_tenants = config.SHOPPING_CENTRE_THRESHOLDS['large_centre_tenants']
            if tenants < min_tenants:
                continue

            # (b) No approved premises in the large shopping centre
            pharmacy_in_centre = self._check_pharmacy_in_centre(centre, pharmacies)
            if pharmacy_in_centre:
                continue  # Has pharmacy — try Item 134A instead

            # Check supermarket with GLA >= 2,500sqm
            has_supermarket = self._check_supermarket_in_centre(centre)

            # Build evidence
            supermarkets_info = centre.get('major_supermarkets', [])
            if isinstance(supermarkets_info, str):
                try:
                    supermarkets_info = json.loads(supermarkets_info)
                except:
                    supermarkets_info = [supermarkets_info]

            evidence = self.format_evidence(
                rule="Item 134: New pharmacy in large shopping centre (no existing pharmacy)",
                centre_name=centre.get('name', 'Unknown'),
                centre_gla=f"{gla:,.0f}sqm (>= 5,000sqm ✓)",
                tenants=f"{tenants} tenants (>= 50 ✓)" if tenants else "tenants unknown - VERIFY",
                supermarkets=", ".join(supermarkets_info) if supermarkets_info else "VERIFY supermarket >= 2,500sqm",
                no_pharmacy_in_centre="Confirmed ✓" if not pharmacy_in_centre else "FAILED",
                distance_note="NO minimum distance required",
                note="VERIFY: single management, customer parking, supermarket GLA >= 2,500sqm"
            )
            return True, evidence

        return False, None

    def _check_pharmacy_in_centre(self, centre: Dict, pharmacies: List[Dict]) -> bool:
        """Check if there's already a pharmacy in this shopping centre."""
        for pharm in pharmacies:
            d = haversine_distance(
                centre['latitude'], centre['longitude'],
                pharm.get('latitude', 0), pharm.get('longitude', 0)
            )
            if d <= 0.15:  # 150m - inside the centre
                return True
        return False

    def _check_supermarket_in_centre(self, centre: Dict) -> bool:
        """Check if there's a supermarket with GLA >= 2,500sqm in the centre."""
        supermarkets = self.db.get_all_supermarkets()
        if not supermarkets:
            # Fallback to listed supermarkets
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
