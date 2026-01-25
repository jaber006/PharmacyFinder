"""
Rule Item 135: Hospital (100+ beds)
"""
from typing import Dict, Optional, Tuple
from rules.base_rule import BaseRule
from utils.distance import haversine_distance
import config


class Item135Rule(BaseRule):
    """
    A pharmacy is eligible if located within or adjacent to a public hospital with:
    - Minimum 100 beds
    - Acute care facility
    """

    @property
    def rule_name(self) -> str:
        return "Hospital (100+ beds)"

    @property
    def item_number(self) -> str:
        return "Item 135"

    def check_eligibility(self, property_data: Dict) -> Tuple[bool, Optional[str]]:
        """
        Check if property is within or adjacent to a qualifying hospital.

        Args:
            property_data: Property information with latitude/longitude

        Returns:
            (is_eligible, evidence)
        """
        lat = property_data.get('latitude')
        lon = property_data.get('longitude')

        if lat is None or lon is None:
            return False, None

        # Get all hospitals
        hospitals = self.db.get_all_hospitals()

        if not hospitals:
            return False, None

        # Check each hospital
        for hospital in hospitals:
            # Check bed count requirement
            bed_count = hospital.get('bed_count', 0)
            if bed_count < config.HOSPITAL_BED_COUNT:
                continue

            # Check if property is within or adjacent to hospital
            # Using 300m as threshold for "within premises or adjacent"
            distance = haversine_distance(
                lat, lon,
                hospital['latitude'], hospital['longitude']
            )

            if distance <= 0.3:  # 300 meters
                evidence = self.format_evidence(
                    rule="Within/adjacent to 100+ bed hospital",
                    hospital_name=hospital.get('name', 'Unknown'),
                    bed_count=bed_count,
                    address=hospital.get('address', 'Unknown')
                )
                return True, evidence

        return False, None
