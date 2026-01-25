"""
Rule Item 134A: Very Remote Location (90km from nearest pharmacy)
"""
from typing import Dict, Optional, Tuple
from rules.base_rule import BaseRule
from utils.distance import find_nearest, format_distance
import config


class Item134ARule(BaseRule):
    """
    A pharmacy is eligible if it is at least 90km from the nearest existing pharmacy.
    Distance is measured straight-line from ground-level public access door midpoints.
    """

    @property
    def rule_name(self) -> str:
        return "Very Remote Location (90km)"

    @property
    def item_number(self) -> str:
        return "Item 134A"

    def check_eligibility(self, property_data: Dict) -> Tuple[bool, Optional[str]]:
        """
        Check if property is at least 90km from nearest pharmacy.

        Args:
            property_data: Property information with latitude/longitude

        Returns:
            (is_eligible, evidence)
        """
        lat = property_data.get('latitude')
        lon = property_data.get('longitude')

        if lat is None or lon is None:
            return False, None

        # Get all existing pharmacies
        pharmacies = self.db.get_all_pharmacies()

        if not pharmacies:
            # No pharmacies in database - property qualifies
            return True, "No existing pharmacies in database"

        # Find nearest pharmacy
        nearest_pharmacy, distance_km = find_nearest(lat, lon, pharmacies)

        if nearest_pharmacy is None:
            return True, "No existing pharmacies found"

        # Check if distance meets threshold
        threshold = config.RULE_DISTANCES['item_134a']

        if distance_km >= threshold:
            evidence = self.format_evidence(
                rule="90km from nearest pharmacy",
                nearest_pharmacy=nearest_pharmacy.get('name', 'Unknown'),
                nearest_address=nearest_pharmacy.get('address', 'Unknown'),
                distance=format_distance(distance_km)
            )
            return True, evidence

        return False, None
