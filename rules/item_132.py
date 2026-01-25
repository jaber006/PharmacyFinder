"""
Rule Item 132: Major Shopping Centre (15,000+ sqm GLA)
"""
from typing import Dict, Optional, Tuple
from rules.base_rule import BaseRule
from utils.distance import haversine_distance
import config


class Item132Rule(BaseRule):
    """
    A pharmacy is eligible if located within a shopping centre with:
    - GLA >= 15,000 sqm
    - At least one major supermarket (Woolworths, Coles, ALDI)
    """

    @property
    def rule_name(self) -> str:
        return "Major Shopping Centre (15,000+ sqm)"

    @property
    def item_number(self) -> str:
        return "Item 132"

    def check_eligibility(self, property_data: Dict) -> Tuple[bool, Optional[str]]:
        """
        Check if property is within a major shopping centre.

        Args:
            property_data: Property information with latitude/longitude

        Returns:
            (is_eligible, evidence)
        """
        lat = property_data.get('latitude')
        lon = property_data.get('longitude')

        if lat is None or lon is None:
            return False, None

        # Get all shopping centres
        centres = self.db.get_all_shopping_centres()

        if not centres:
            return False, None

        # Check each centre
        for centre in centres:
            # Check GLA requirement
            gla = centre.get('gla_sqm', 0)
            if gla < config.GLA_THRESHOLDS['major_centre']:
                continue

            # Check if property is within the centre
            # For simplicity, check if coordinates are very close (within 200m)
            distance = haversine_distance(
                lat, lon,
                centre['latitude'], centre['longitude']
            )

            if distance > 0.2:  # 200 meters
                continue

            # Check for major supermarket
            supermarkets = centre.get('major_supermarkets', [])
            has_major_supermarket = any(
                any(chain in str(sm).lower() for chain in config.MAJOR_SUPERMARKETS)
                for sm in supermarkets
            )

            if has_major_supermarket:
                supermarket_list = ", ".join(supermarkets)

                evidence = self.format_evidence(
                    rule="Major shopping centre 15,000+ sqm",
                    centre_name=centre.get('name', 'Unknown'),
                    gla_sqm=f"{gla:,.0f}",
                    supermarkets=supermarket_list
                )
                return True, evidence

        return False, None
