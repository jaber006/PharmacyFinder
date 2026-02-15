"""
Rule Item 133: New pharmacy in a designated complex (small shopping centre)

From ACPA Handbook (Jan 2024, V1.9):

Requirements:
(a) The proposed premises are in a small shopping centre, AND
(b) At least 500m, in a straight line, from the nearest approved premises,
    OTHER THAN approved premises in a large shopping centre or private hospital, AND
(c) There are no approved premises in the small shopping centre.

A "small shopping centre" is defined as a group of shops and associated facilities that:
- Is under single management
- Has a gross leasable area of at least 5,000 sqm
- Contains a supermarket with GLA of at least 2,500 sqm
- Contains at least 15 other commercial establishments
- Has customer parking facilities

Distance measurement: Mid-point at ground level of the public access door.

Restriction: Must stay in same shopping centre for 10 years (unless exceptional circumstances).
"""
from typing import Dict, Optional, Tuple, List
from rules.base_rule import BaseRule
from utils.distance import haversine_distance, find_nearest, find_within_radius, format_distance
import config
import json


class Item133Rule(BaseRule):
    """
    Item 133: New pharmacy in a small shopping centre.

    Must be INSIDE a qualifying small shopping centre:
    - Single management
    - Centre GLA >= 5,000sqm
    - Supermarket GLA >= 2,500sqm
    - >= 15 commercial establishments
    - Customer parking

    Distance: >= 500m straight line from nearest pharmacy
    (excluding pharmacies in large shopping centres or private hospitals)

    No existing pharmacy in the shopping centre.
    """

    @property
    def rule_name(self) -> str:
        return "Small Shopping Centre (>=5,000sqm, >=15 tenants)"

    @property
    def item_number(self) -> str:
        return "Item 133"

    def check_eligibility(self, property_data: Dict) -> Tuple[bool, Optional[str]]:
        """Check if property meets Item 133 requirements."""
        lat = property_data.get('latitude')
        lon = property_data.get('longitude')

        if lat is None or lon is None:
            return False, None

        # Get all shopping centres
        centres = self.db.get_all_shopping_centres()
        if not centres:
            return False, None

        pharmacies = self.db.get_all_pharmacies()

        # Check each shopping centre
        for centre in centres:
            # Check if property is within the centre (within 200m as proxy for "inside")
            distance_to_centre = haversine_distance(
                lat, lon,
                centre['latitude'], centre['longitude']
            )
            if distance_to_centre > 0.2:  # 200m
                continue

            # Check centre qualifies as a "small shopping centre"
            # Must have GLA >= 5,000sqm
            gla = centre.get('gla_sqm', 0) or centre.get('estimated_gla', 0) or 0
            if gla < config.SHOPPING_CENTRE_THRESHOLDS['small_centre_gla']:
                continue

            # Must contain supermarket with GLA >= 2,500sqm
            has_qualifying_supermarket = self._check_supermarket_in_centre(centre, lat, lon)

            # Must have >= 15 commercial establishments
            tenants = centre.get('estimated_tenants', 0) or 0
            min_tenants = config.SHOPPING_CENTRE_THRESHOLDS['small_centre_tenants']

            # Check if it's a LARGE shopping centre (>= 50 tenants) - if so, use Item 134 instead
            # Item 133 is specifically for small shopping centres
            # But the handbook only says >= 15 tenants - a centre with >= 50 tenants
            # is a "large shopping centre" and should use Item 134/134A
            is_large = tenants >= config.SHOPPING_CENTRE_THRESHOLDS.get('large_centre_tenants', 50)

            if tenants < min_tenants:
                continue

            # (c) No existing pharmacy in the shopping centre
            pharmacy_in_centre = self._check_pharmacy_in_centre(centre, pharmacies)
            if pharmacy_in_centre:
                continue

            # (b) At least 500m straight line from nearest approved premises,
            # EXCLUDING pharmacies in large shopping centres or private hospitals
            distance_check = self._check_distance_from_pharmacies(
                lat, lon, pharmacies, centres
            )
            if not distance_check[0]:
                continue

            # Build evidence
            supermarkets_info = centre.get('major_supermarkets', [])
            if isinstance(supermarkets_info, str):
                try:
                    supermarkets_info = json.loads(supermarkets_info)
                except:
                    supermarkets_info = [supermarkets_info]

            centre_type = "LARGE" if is_large else "small"
            evidence = self.format_evidence(
                rule=f"Item 133: New pharmacy in {centre_type} shopping centre",
                centre_name=centre.get('name', 'Unknown'),
                centre_gla=f"{gla:,.0f}sqm (>= 5,000sqm ✓)",
                tenants=f"{tenants} tenants (>= 15 ✓)" if tenants else "tenants unknown - VERIFY",
                supermarkets=", ".join(supermarkets_info) if supermarkets_info else "VERIFY supermarket >= 2,500sqm",
                has_qualifying_supermarket="Yes" if has_qualifying_supermarket else "VERIFY",
                distance_check=distance_check[1],
                note="VERIFY: single management, customer parking, no pharmacy in centre, supermarket GLA >= 2,500sqm"
            )
            return True, evidence

        return False, None

    def _check_supermarket_in_centre(self, centre: Dict, lat: float, lon: float) -> bool:
        """Check if there's a supermarket with GLA >= 2,500sqm in the centre."""
        supermarkets = self.db.get_all_supermarkets()
        if not supermarkets:
            # Can't verify but don't block - major supermarket listed is evidence
            supermarket_list = centre.get('major_supermarkets', [])
            if isinstance(supermarket_list, str):
                try:
                    supermarket_list = json.loads(supermarket_list)
                except:
                    supermarket_list = [supermarket_list] if supermarket_list else []
            return len(supermarket_list) > 0

        # Check for supermarkets within 300m of centre
        nearby = find_within_radius(
            centre['latitude'], centre['longitude'],
            supermarkets, 0.3  # 300m
        )

        for sm, dist in nearby:
            area = sm.get('floor_area_sqm', 0) or sm.get('estimated_gla', 0) or 0
            if area >= config.SHOPPING_CENTRE_THRESHOLDS['supermarket_gla']:
                return True

        # Fallback: check if centre has major supermarket listed
        supermarket_list = centre.get('major_supermarkets', [])
        if isinstance(supermarket_list, str):
            try:
                supermarket_list = json.loads(supermarket_list)
            except:
                supermarket_list = [supermarket_list] if supermarket_list else []
        return len(supermarket_list) > 0

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

    def _check_distance_from_pharmacies(
        self, lat: float, lon: float, pharmacies: List[Dict],
        all_centres: List[Dict]
    ) -> Tuple[bool, str]:
        """
        Check >= 500m straight line from nearest pharmacy,
        EXCLUDING pharmacies in large shopping centres or private hospitals.
        """
        if not pharmacies:
            return True, "No pharmacies in database"

        hospitals = self.db.get_all_hospitals()
        threshold_km = config.SHOPPING_CENTRE_THRESHOLDS.get('item_133_distance_km', 0.5)

        nearest_qualifying = None
        nearest_qualifying_dist = float('inf')

        for pharm in pharmacies:
            plat = pharm.get('latitude', 0)
            plon = pharm.get('longitude', 0)
            if not plat or not plon:
                continue

            dist = haversine_distance(lat, lon, plat, plon)

            # Exclude pharmacies in large shopping centres
            in_large_centre = False
            for centre in all_centres:
                tenants = centre.get('estimated_tenants', 0) or 0
                if tenants >= config.SHOPPING_CENTRE_THRESHOLDS.get('large_centre_tenants', 50):
                    d_to_centre = haversine_distance(
                        plat, plon,
                        centre['latitude'], centre['longitude']
                    )
                    if d_to_centre <= 0.15:
                        in_large_centre = True
                        break

            # Exclude pharmacies in private hospitals
            in_hospital = False
            for hospital in hospitals:
                d_to_hosp = haversine_distance(
                    plat, plon,
                    hospital.get('latitude', 0), hospital.get('longitude', 0)
                )
                if d_to_hosp <= 0.15:
                    in_hospital = True
                    break

            if in_large_centre or in_hospital:
                continue  # Exclude from distance check

            if dist < nearest_qualifying_dist:
                nearest_qualifying_dist = dist
                nearest_qualifying = pharm

        if nearest_qualifying is None:
            return True, "No qualifying pharmacies nearby (all excluded)"

        if nearest_qualifying_dist >= threshold_km:
            return True, f"Nearest pharmacy (excl. large centres/hospitals): {nearest_qualifying.get('name', 'Unknown')} at {format_distance(nearest_qualifying_dist)} (>= 500m ✓)"

        return False, f"Too close to {nearest_qualifying.get('name', 'Unknown')} ({format_distance(nearest_qualifying_dist)} < 500m)"
