"""
Rule Item 136: New pharmacy in a Large Medical Centre

Requirements:
(a) Proposed premises are in a large medical centre
(b) No approved pharmacy currently in the large medical centre
(c) Distance from nearest pharmacy >= 300m (with exceptions)
(d) At least 8 FTE PBS prescribers, of which at least 7 must be medical practitioners
(e) Pharmacy hours will meet patient needs

Large Medical Centre definition:
- Under single management
- Open for at least 70 hours per week
- Providing general practice services for at least 70 hours per week

NOTE: This rule requires detailed medical centre data that's hard to scrape.
Currently checks for proximity to GP clusters (4+ FTE GPs within 200m) as a proxy.
"""
from typing import Dict, Optional, Tuple
from rules.base_rule import BaseRule
from utils.distance import find_nearest, find_within_radius, format_distance
import config


class Item136Rule(BaseRule):
    """
    Item 136: New pharmacy in a designated complex (large medical centre)
    
    Since we can't easily determine if a property IS a large medical centre,
    we check if it's near a cluster of GPs that could indicate a medical centre.
    Flagged for manual verification.
    """

    @property
    def rule_name(self) -> str:
        return "Large Medical Centre (8 FTE prescribers)"

    @property
    def item_number(self) -> str:
        return "Item 136"

    def check_eligibility(self, property_data: Dict) -> Tuple[bool, Optional[str]]:
        """
        Check if property might qualify under Item 136.
        
        Uses GP density as a proxy for large medical centres.
        """
        lat = property_data.get('latitude')
        lon = property_data.get('longitude')

        if lat is None or lon is None:
            return False, None

        # Check for GP cluster within 200m (proxy for medical centre)
        gps = self.db.get_all_gps()
        if not gps:
            return False, None

        nearby_gps = find_within_radius(lat, lon, gps, 0.2)  # 200m
        
        if not nearby_gps:
            return False, None

        # Calculate total FTE
        total_fte = sum(gp[0].get('fte', 0) or 0 for gp in nearby_gps)
        num_practices = len(nearby_gps)

        # Need 8+ FTE prescribers to qualify
        required_fte = config.FTE_REQUIREMENTS.get('item_136_prescribers', 8.0)

        if total_fte >= required_fte:
            gp_names = [gp[0].get('name', 'Unknown') for gp in nearby_gps[:5]]
            evidence = self.format_evidence(
                rule="Potential large medical centre (8+ FTE prescribers nearby)",
                total_fte=f"{total_fte:.1f}",
                num_practices=num_practices,
                nearby_practices=", ".join(gp_names),
                note="REQUIRES MANUAL VERIFICATION - check if premises is within a large medical centre"
            )
            return True, evidence

        # Also flag if there's a significant GP cluster (potential opportunity)
        if num_practices >= 3 and total_fte >= 4.0:
            # This doesn't meet the threshold but is worth noting
            pass

        return False, None
