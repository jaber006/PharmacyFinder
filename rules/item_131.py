"""
Rule Item 131: GP Proximity (1.5km + 2 FTE GPs)
"""
from typing import Dict, Optional, Tuple
from rules.base_rule import BaseRule
from utils.distance import find_within_radius
import config


class Item131Rule(BaseRule):
    """
    A pharmacy is eligible if there are at least 2.0 FTE GPs within 1.5km.
    FTE calculated as: hours_per_week / 38, minimum 20 hours to count.
    """

    @property
    def rule_name(self) -> str:
        return "GP Proximity (1.5km + 2 FTE)"

    @property
    def item_number(self) -> str:
        return "Item 131"

    def check_eligibility(self, property_data: Dict) -> Tuple[bool, Optional[str]]:
        """
        Check if property has at least 2.0 FTE GPs within 1.5km.

        Args:
            property_data: Property information with latitude/longitude

        Returns:
            (is_eligible, evidence)
        """
        lat = property_data.get('latitude')
        lon = property_data.get('longitude')

        if lat is None or lon is None:
            return False, None

        # Get all GPs
        gps = self.db.get_all_gps()

        if not gps:
            return False, None

        # Find GPs within 1.5km
        radius = config.RULE_DISTANCES['item_131']
        nearby_gps = find_within_radius(lat, lon, gps, radius)

        if not nearby_gps:
            return False, None

        # Calculate total FTE
        total_fte = 0.0
        gp_details = []

        for gp, distance in nearby_gps:
            fte = gp.get('fte', 0.0)
            total_fte += fte

            gp_details.append({
                'name': gp.get('name', 'Unknown'),
                'distance_km': distance,
                'fte': fte
            })

        # Check if meets threshold
        threshold = config.FTE_REQUIREMENTS['item_131']

        if total_fte >= threshold:
            # Format GP list for evidence
            gp_list = ", ".join([
                f"{gp['name']} ({gp['fte']:.1f} FTE)"
                for gp in gp_details
            ])

            evidence = self.format_evidence(
                rule="2+ FTE GPs within 1.5km",
                total_fte=f"{total_fte:.2f}",
                gp_count=len(nearby_gps),
                gps=gp_list
            )
            return True, evidence

        return False, None
