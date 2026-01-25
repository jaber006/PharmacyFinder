"""
Rule Item 136: Medical Centre (4+ FTE GPs)
"""
from typing import Dict, Optional, Tuple
from rules.base_rule import BaseRule
from utils.distance import find_within_radius
import config


class Item136Rule(BaseRule):
    """
    A pharmacy is eligible if located within or adjacent to a medical centre with:
    - Minimum 4.0 FTE GPs operating from the same premises
    - FTE calculated as: hours_per_week / 38, minimum 20 hours to count
    """

    @property
    def rule_name(self) -> str:
        return "Medical Centre (4+ FTE GPs)"

    @property
    def item_number(self) -> str:
        return "Item 136"

    def check_eligibility(self, property_data: Dict) -> Tuple[bool, Optional[str]]:
        """
        Check if property is within or adjacent to a medical centre with 4+ FTE GPs.

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

        # Find GPs at the same location (within 50m to account for geocoding variance)
        # This represents GPs operating from the same medical centre
        colocated_gps = find_within_radius(lat, lon, gps, 0.05)  # 50 meters

        if not colocated_gps:
            return False, None

        # Calculate total FTE at this location
        total_fte = 0.0
        gp_details = []

        for gp, distance in colocated_gps:
            fte = gp.get('fte', 0.0)
            total_fte += fte

            gp_details.append({
                'name': gp.get('name', 'Unknown'),
                'fte': fte
            })

        # Check if meets threshold
        threshold = config.FTE_REQUIREMENTS['item_136']

        if total_fte >= threshold:
            # Format GP list for evidence
            gp_list = ", ".join([
                f"{gp['name']} ({gp['fte']:.1f} FTE)"
                for gp in gp_details
            ])

            evidence = self.format_evidence(
                rule="Medical centre with 4+ FTE GPs",
                total_fte=f"{total_fte:.2f}",
                gp_count=len(colocated_gps),
                gps=gp_list
            )
            return True, evidence

        return False, None
