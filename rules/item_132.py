"""
Rule Item 132: New Additional Pharmacy (at least 10km)

From ACPA Handbook (Jan 2024, V1.9):

This is for establishing a SECOND pharmacy in a town that already has one.

Requirements:
(a) The proposed premises are:
    (i)   in the same town as an approved premises, AND
    (ii)  at least 200m, in a straight line, from the nearest approved premises, AND
    (iii) at least 10km, by the shortest lawful access route, from any approved
          premises OTHER THAN the one mentioned at (ii) above

(b) In the same town as the proposed premises, there are:
    (i)  at least 4 FTE prescribing medical practitioners practising, AND
    (ii) 1 or 2 supermarkets with combined GLA >= 2,500sqm

"Same town" means the same physical locality name AND postcode assigned by
the local planning authority/council. Two towns that share the same postcode
but not the same name are NOT the same town.

Distance measurement:
- Straight line: mid-point at ground level of public access door
- Route: shortest lawful access route from centre at ground level of public entrance

Restriction: Cannot relocate from the town. Ever.
"""
from typing import Dict, Optional, Tuple, List
from rules.base_rule import BaseRule
from utils.distance import (
    haversine_distance, find_nearest, find_within_radius,
    get_driving_distance, format_distance
)
import config


class Item132Rule(BaseRule):
    """
    Item 132: New additional pharmacy in a town that already has one.

    Key concept: This is for a town that already has ONE pharmacy but is 
    geographically isolated (10km+ by road from any other pharmacy outside
    the town). It allows a SECOND pharmacy in the same town.

    Requirements:
    1. Same town as an existing pharmacy (same locality/suburb)
    2. >= 200m straight line from nearest pharmacy  
    3. >= 10km by road from ANY OTHER pharmacy (not the nearest one in town)
    4. >= 4 FTE GPs practising in the same town
    5. 1-2 supermarkets with combined GLA >= 2,500sqm in the same town
    """

    # Max distance (straight line) to consider two locations in the "same town"
    SAME_TOWN_MAX_KM = 5.0

    @property
    def rule_name(self) -> str:
        return "New Additional Pharmacy in Town (10km from others)"

    @property
    def item_number(self) -> str:
        return "Item 132"

    def check_eligibility(self, property_data: Dict) -> Tuple[bool, Optional[str]]:
        """Check if property meets Item 132 requirements."""
        lat = property_data.get('latitude')
        lon = property_data.get('longitude')

        if lat is None or lon is None:
            return False, None

        pharmacies = self.db.get_all_pharmacies()
        if not pharmacies:
            return False, None

        # Find the nearest pharmacy
        nearest_pharmacy, nearest_dist_km = find_nearest(lat, lon, pharmacies)
        if nearest_pharmacy is None:
            return False, None

        # (a)(ii): At least 200m straight line from the nearest approved premises
        if nearest_dist_km < 0.2:  # 200m
            return False, None

        # (a)(i): Must be in the SAME TOWN as an existing pharmacy
        # "Same town" = same locality name + postcode
        # As a practical check: the nearest pharmacy must be close enough to
        # plausibly be in the same town (< 5km) AND share the same suburb/town
        if nearest_dist_km > self.SAME_TOWN_MAX_KM:
            return False, None  # Too far to be "same town"

        town_pharmacy = nearest_pharmacy
        town_name = self._get_town(town_pharmacy)
        
        # If we have suburb data, check it matches
        opp_town = self._get_town_from_address(property_data.get('address', ''))
        if town_name and opp_town:
            # Basic check: do they share the same locality name?
            if town_name.lower().strip() != opp_town.lower().strip():
                # Different locality names - may not be "same town"
                # But allow it if they're very close (within 2km) as addresses can be imprecise
                if nearest_dist_km > 2.0:
                    return False, None

        # (a)(iii): At least 10km by shortest lawful access route from ANY OTHER
        # approved premises (not the nearest one in town)
        other_pharmacies = [p for p in pharmacies
                           if p.get('id') != nearest_pharmacy.get('id')]

        if not other_pharmacies:
            # Only one pharmacy exists. The "any other" requirement is trivially met.
            second_nearest_dist = float('inf')
            second_nearest = None
        else:
            second_nearest, second_nearest_straight = find_nearest(lat, lon, other_pharmacies)

            if second_nearest is None:
                second_nearest_dist = float('inf')
            else:
                # Quick reject: if straight line < 7km, can't be 10km by road
                if second_nearest_straight < 7.0:
                    return False, None

                # For borderline cases, try OSRM
                if second_nearest_straight < 15.0:
                    route_km = get_driving_distance(
                        lat, lon,
                        second_nearest['latitude'], second_nearest['longitude']
                    )
                    if route_km is not None:
                        second_nearest_dist = route_km
                    else:
                        second_nearest_dist = second_nearest_straight * 1.3
                else:
                    second_nearest_dist = second_nearest_straight * 1.3

        if second_nearest_dist < 10.0:
            return False, None

        # (b)(i): At least 4 FTE prescribing medical practitioners in the same town
        gp_check = self._check_town_gps(lat, lon, town_name)

        # (b)(ii): 1 or 2 supermarkets with combined GLA >= 2,500sqm in same town
        supermarket_check = self._check_town_supermarkets(lat, lon, town_name)

        # Build evidence
        second_info = ""
        if second_nearest:
            second_info = f"Second nearest pharmacy: {second_nearest.get('name', 'Unknown')} at ~{format_distance(second_nearest_dist)} by route"
        else:
            second_info = "No other pharmacies in database (only one in town)"

        evidence = self.format_evidence(
            rule="Item 132: New additional pharmacy in town",
            town=town_name or "Unknown",
            nearest_pharmacy=f"{nearest_pharmacy.get('name', 'Unknown')} at {format_distance(nearest_dist_km)} (>=200m ✓, same town ✓)",
            second_nearest=second_info,
            town_gps=gp_check[1] if gp_check[1] else "GP data insufficient - VERIFY 4+ FTE GPs in town",
            town_supermarkets=supermarket_check[1] if supermarket_check[1] else "Supermarket data insufficient - VERIFY 1-2 supermarkets >=2,500sqm combined",
            note="VERIFY: Same town (locality+postcode), 4+ FTE GPs, 1-2 supermarkets >=2,500sqm combined GLA"
        )
        return True, evidence

    def _get_town(self, pharmacy: Dict) -> str:
        """Extract town/suburb name from pharmacy data."""
        town = pharmacy.get('suburb', '') or ''
        if not town:
            address = pharmacy.get('address', '')
            parts = address.split(',')
            if len(parts) >= 2:
                town = parts[-2].strip()
        return town

    def _get_town_from_address(self, address: str) -> str:
        """Extract town from an address string."""
        if not address:
            return ''
        parts = address.split(',')
        if len(parts) >= 3:
            # Typically: street, suburb, state, postcode, country
            return parts[1].strip() if len(parts) > 1 else ''
        return ''

    def _check_town_gps(self, lat: float, lon: float, town_name: str) -> Tuple[bool, Optional[str]]:
        """Check if there are >= 4 FTE prescribing medical practitioners in the same town."""
        gps = self.db.get_all_gps()
        if not gps:
            return False, None

        # Use a radius around the opportunity to approximate "same town" (3km)
        nearby_gps = find_within_radius(lat, lon, gps, 3.0)
        total_fte = sum(gp[0].get('fte', 0) or 0 for gp in nearby_gps)

        if total_fte >= config.FTE_REQUIREMENTS.get('item_132_gp', 4.0):
            return True, f"{total_fte:.1f} FTE GPs in town area (>= 4 required)"

        # Also count by headcount as proxy
        num_gps = len(nearby_gps)
        estimated_fte = num_gps * 0.8
        if estimated_fte >= 4.0:
            return True, f"~{num_gps} GP practices ({estimated_fte:.1f} est. FTE) in town area"

        return False, None

    def _check_town_supermarkets(self, lat: float, lon: float, town_name: str) -> Tuple[bool, Optional[str]]:
        """Check if 1-2 supermarkets in town have combined GLA >= 2,500sqm."""
        supermarkets = self.db.get_all_supermarkets()
        if not supermarkets:
            return False, None

        # Supermarkets within ~3km (same town approximation)
        nearby = find_within_radius(lat, lon, supermarkets, 3.0)

        if not nearby:
            return False, None

        # Get top 2 supermarkets by GLA
        sm_with_area = []
        for sm, dist in nearby:
            area = sm.get('floor_area_sqm', 0) or sm.get('estimated_gla', 0) or 0
            sm_with_area.append((sm, dist, area))

        sm_with_area.sort(key=lambda x: x[2], reverse=True)
        top_2 = sm_with_area[:2]
        combined_gla = sum(x[2] for x in top_2)

        if combined_gla >= 2500:
            names = [f"{x[0].get('name', 'Unknown')} ({x[2]:.0f}sqm)" for x in top_2]
            return True, f"Supermarkets: {', '.join(names)} (combined {combined_gla:.0f}sqm >= 2,500sqm)"

        return False, None
