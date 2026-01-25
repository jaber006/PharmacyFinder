"""
Rule Item 133: Supermarket (1,000+ sqm)
"""
from typing import Dict, Optional, Tuple
from rules.base_rule import BaseRule
from utils.distance import haversine_distance
import config


class Item133Rule(BaseRule):
    """
    A pharmacy is eligible if adjoining or within a supermarket premises with:
    - Floor area >= 1,000 sqm
    - Major supermarket chain (Woolworths, Coles, ALDI)
    """

    @property
    def rule_name(self) -> str:
        return "Supermarket (1,000+ sqm)"

    @property
    def item_number(self) -> str:
        return "Item 133"

    def check_eligibility(self, property_data: Dict) -> Tuple[bool, Optional[str]]:
        """
        Check if property adjoins a qualifying supermarket.

        Args:
            property_data: Property information with latitude/longitude

        Returns:
            (is_eligible, evidence)
        """
        lat = property_data.get('latitude')
        lon = property_data.get('longitude')

        if lat is None or lon is None:
            return False, None

        # Get all supermarkets
        supermarkets = self.db.get_all_supermarkets()

        if not supermarkets:
            return False, None

        # Check each supermarket
        for supermarket in supermarkets:
            # Check if it's a major chain
            name = supermarket.get('name', '')
            is_major = any(
                chain in name.lower()
                for chain in config.MAJOR_SUPERMARKETS
            )

            if not is_major:
                continue

            # Check floor area requirement
            floor_area = supermarket.get('floor_area_sqm')
            if floor_area and floor_area < config.FLOOR_AREA_THRESHOLDS['supermarket']:
                continue

            # Check if property is adjacent (within 100m)
            distance = haversine_distance(
                lat, lon,
                supermarket['latitude'], supermarket['longitude']
            )

            if distance <= 0.1:  # 100 meters
                evidence = self.format_evidence(
                    rule="Adjacent to 1,000+ sqm supermarket",
                    supermarket=name,
                    floor_area_sqm=f"{floor_area:,.0f}" if floor_area else "Not specified",
                    address=supermarket.get('address', 'Unknown')
                )
                return True, evidence

        return False, None
