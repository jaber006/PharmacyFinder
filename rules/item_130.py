"""
Rule Item 130: New Pharmacy (at least 1.5km from nearest pharmacy)

Requirements:
(a) Proposed premises at least 1.5km (straight line) from nearest approved premises
(b) Within 500m of proposed premises, there must be EITHER:
    (i) 1 FTE prescribing medical practitioner AND supermarket with GLA >= 1,000sqm, OR
    (ii) A supermarket with GLA >= 2,500sqm
"""
from typing import Dict, Optional, Tuple, List
from rules.base_rule import BaseRule
from utils.distance import find_nearest, find_within_radius, format_distance
import config


class Item130Rule(BaseRule):
    """
    Item 130: New pharmacy (at least 1.5 km)
    
    A pharmacy is eligible if:
    1. At least 1.5km from nearest existing pharmacy, AND
    2. Within 500m there is either:
       - 1 FTE GP + supermarket >= 1,000sqm GLA, OR
       - Supermarket >= 2,500sqm GLA
    """

    @property
    def rule_name(self) -> str:
        return "New Pharmacy (1.5km + Supermarket/GP)"

    @property
    def item_number(self) -> str:
        return "Item 130"

    def check_eligibility(self, property_data: Dict) -> Tuple[bool, Optional[str]]:
        """
        Check if property meets Item 130 requirements.
        """
        lat = property_data.get('latitude')
        lon = property_data.get('longitude')

        if lat is None or lon is None:
            return False, None

        # Requirement (a): At least 1.5km from nearest pharmacy
        pharmacy_check = self._check_pharmacy_distance(lat, lon)
        if not pharmacy_check[0]:
            return False, None

        # Requirement (b): Supermarket/GP within 500m
        supermarket_gp_check = self._check_supermarket_gp_requirement(lat, lon)
        if not supermarket_gp_check[0]:
            return False, None

        # Both requirements met
        evidence = self.format_evidence(
            rule="1.5km from nearest pharmacy + supermarket/GP within 500m",
            pharmacy_evidence=pharmacy_check[1],
            supermarket_gp_evidence=supermarket_gp_check[1]
        )
        return True, evidence

    def _check_pharmacy_distance(self, lat: float, lon: float) -> Tuple[bool, Optional[str]]:
        """Check if property is at least 1.5km from nearest pharmacy."""
        pharmacies = self.db.get_all_pharmacies()

        if not pharmacies:
            return True, "No existing pharmacies in database"

        nearest_pharmacy, distance_km = find_nearest(lat, lon, pharmacies)

        if nearest_pharmacy is None:
            return True, "No existing pharmacies found"

        threshold = config.RULE_DISTANCES['item_130']

        if distance_km >= threshold:
            return True, f"Nearest pharmacy: {nearest_pharmacy.get('name', 'Unknown')} at {format_distance(distance_km)}"

        return False, None

    def _check_supermarket_gp_requirement(self, lat: float, lon: float) -> Tuple[bool, Optional[str]]:
        """
        Check if within 500m there is either:
        - 1 FTE GP + supermarket >= 1,000sqm, OR
        - Supermarket >= 2,500sqm
        """
        radius_km = config.RULE_DISTANCES.get('item_130_supermarket', 0.5)
        
        # Get supermarkets within 500m
        supermarkets = self.db.get_all_supermarkets()
        nearby_supermarkets = find_within_radius(lat, lon, supermarkets, radius_km) if supermarkets else []
        
        # Get GPs within 500m
        gps = self.db.get_all_gps()
        nearby_gps = find_within_radius(lat, lon, gps, radius_km) if gps else []

        # Option (ii): Large supermarket >= 2,500sqm
        for sm_tuple in nearby_supermarkets:
            sm, dist = sm_tuple
            floor_area = sm.get('floor_area_sqm', 0) or 0
            if floor_area >= 2500:
                return True, f"Large supermarket within 500m: {sm.get('name', 'Unknown')} ({floor_area:.0f}sqm)"

        # Option (i): 1 FTE GP + supermarket >= 1,000sqm
        has_medium_supermarket = False
        medium_sm_name = None
        medium_sm_area = 0
        for sm_tuple in nearby_supermarkets:
            sm, dist = sm_tuple
            floor_area = sm.get('floor_area_sqm', 0) or 0
            if floor_area >= 1000:
                has_medium_supermarket = True
                medium_sm_name = sm.get('name', 'Unknown')
                medium_sm_area = floor_area
                break

        has_gp_fte = False
        gp_name = None
        gp_fte = 0
        for gp_tuple in nearby_gps:
            gp, dist = gp_tuple
            fte = gp.get('fte', 0) or 0
            if fte >= 1.0:
                has_gp_fte = True
                gp_name = gp.get('name', 'Unknown')
                gp_fte = fte
                break
        
        if has_medium_supermarket and has_gp_fte:
            return True, (f"GP ({gp_name}, {gp_fte:.1f} FTE) + "
                         f"Supermarket ({medium_sm_name}, {medium_sm_area:.0f}sqm) within 500m")

        # If we don't have reference data yet, flag for manual review
        if not supermarkets and not gps:
            return True, "Supermarket/GP data not yet populated - manual verification required"

        return False, None
