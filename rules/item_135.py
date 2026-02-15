"""
Rule Item 135: New pharmacy in a designated complex (large private hospital)

From ACPA Handbook (Jan 2024, V1.9):

Requirements:
(a) The proposed premises are in a large private hospital, AND
(b) There are no approved pharmacies in the large private hospital.

A "large private hospital" means:
A private hospital that can admit at least 150 patients at any one time
in accordance with the private hospital's licence or registration under
the law of the State or Territory in which the private hospital is located.

"Admit" means where a patient is admitted to hospital for treatment as a
private patient, including same-day admitted patients AND overnight admitted
patients. NOT outpatients.

NO minimum distance from nearest pharmacy.

The pharmacy must be INSIDE the hospital, not just nearby.
"""
from typing import Dict, Optional, Tuple, List
from rules.base_rule import BaseRule
from utils.distance import haversine_distance, format_distance
import config


class Item135Rule(BaseRule):
    """
    Item 135: New pharmacy in a large private hospital.

    Must be INSIDE a qualifying large private hospital:
    - Private hospital (not public)
    - Can admit >= 150 patients at any one time (per licence)
    - No existing pharmacy in the hospital

    NO minimum distance from nearest pharmacy.
    """

    @property
    def rule_name(self) -> str:
        return "Large Private Hospital (>=150 patient admission capacity)"

    @property
    def item_number(self) -> str:
        return "Item 135"

    def check_eligibility(self, property_data: Dict) -> Tuple[bool, Optional[str]]:
        """Check if property meets Item 135 requirements."""
        lat = property_data.get('latitude')
        lon = property_data.get('longitude')

        if lat is None or lon is None:
            return False, None

        hospitals = self.db.get_all_hospitals()
        if not hospitals:
            return False, None

        pharmacies = self.db.get_all_pharmacies()

        for hospital in hospitals:
            # Check if this is a private hospital
            hospital_type = (hospital.get('hospital_type', '') or '').lower()
            # Accept: 'private', 'private_hospital', or unknown (we flag for verification)
            is_private = 'private' in hospital_type or hospital_type == '' or hospital_type == 'unknown'
            # Reject explicitly public hospitals
            if 'public' in hospital_type:
                continue

            # Check patient admission capacity >= 150
            # Note: the DB has 'bed_count' but the handbook says "admit >= 150 patients at any one time"
            # Bed count is our best proxy for admission capacity
            admission_capacity = hospital.get('bed_count', 0) or 0
            if admission_capacity < config.HOSPITAL_ADMISSION_CAPACITY:
                continue

            # Check if property is INSIDE the hospital (200m proxy)
            distance_to_hospital = haversine_distance(
                lat, lon,
                hospital['latitude'], hospital['longitude']
            )
            if distance_to_hospital > 0.2:  # 200m
                continue

            # (b) No approved pharmacy in the hospital
            pharmacy_in_hospital = False
            for pharm in pharmacies:
                d = haversine_distance(
                    hospital['latitude'], hospital['longitude'],
                    pharm.get('latitude', 0), pharm.get('longitude', 0)
                )
                if d <= 0.15:  # 150m - inside hospital
                    pharmacy_in_hospital = True
                    break

            if pharmacy_in_hospital:
                continue

            evidence = self.format_evidence(
                rule="Item 135: New pharmacy in large private hospital",
                hospital_name=hospital.get('name', 'Unknown'),
                hospital_type=hospital_type if hospital_type else "type unknown - VERIFY PRIVATE",
                admission_capacity=f"{admission_capacity} (>= 150 patients ✓)",
                address=hospital.get('address', 'Unknown'),
                no_pharmacy_in_hospital="Confirmed ✓",
                distance_note="NO minimum distance from other pharmacies required",
                note="VERIFY: Hospital is PRIVATE (not public), admission capacity is per licence/registration, pharmacy must be INSIDE hospital"
            )
            return True, evidence

        return False, None
