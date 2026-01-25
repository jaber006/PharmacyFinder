"""
Rule Item 134: Small Shopping Centre (5,000-15,000 sqm) + Supermarket
"""
from typing import Dict, Optional, Tuple
from rules.base_rule import BaseRule
from utils.distance import haversine_distance
import config


class Item134Rule(BaseRule):
    """
    A pharmacy is eligible if located within a shopping centre with:
    - GLA between 5,000 and 15,000 sqm
    - At least one major supermarket (Woolworths, Coles, ALDI)
    """

    @property
    def rule_name(self) -> str:
        return "Small Shopping Centre (5,000-15,000 sqm)"

    @property
    def item_number(self) -> str:
        return "Item 134"

    def check_eligibility(self, property_data: Dict) -> Tuple[bool, Optional[str]]:
        """
        Check if property is within a small shopping centre with supermarket.

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
            # Check GLA requirement (5,000 - 15,000 sqm)
            gla = centre.get('gla_sqm', 0)
            if gla < config.GLA_THRESHOLDS['small_centre_min']:
                continue
            if gla >= config.GLA_THRESHOLDS['small_centre_max']:
                continue

            # Check if property is within the centre
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
                    rule="Shopping centre 5,000-15,000 sqm with supermarket",
                    centre_name=centre.get('name', 'Unknown'),
                    gla_sqm=f"{gla:,.0f}",
                    supermarkets=supermarket_list
                )
                return True, evidence

        return False, None
