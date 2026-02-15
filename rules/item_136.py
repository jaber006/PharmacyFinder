"""
Rule Item 136: New pharmacy in a designated complex (large medical centre)

From ACPA Handbook (Jan 2024, V1.9):

Requirements:
(a) The proposed premises are in a large medical centre, AND
(b) There are no approved premises in the large medical centre, AND
(c) Distance requirements:
    (i)  If the medical centre is in a small shopping centre, large shopping centre,
         or private hospital: >= 300m straight line from any approved premises,
         OTHER THAN approved premises in a DIFFERENT large shopping centre or
         private hospital
    (ii) If the medical centre is NOT in such a complex: >= 300m straight line
         from nearest approved premises, OTHER THAN approved premises in a
         large shopping centre or private hospital
(d) At least 8 FTE PBS prescribers (of which at least 7 must be medical practitioners)
    during 2 months before application AND until consideration day
(e) Applicant will make reasonable attempts to match operating hours to patient needs

A "large medical centre" is a medical centre that:
- Is under single management
- Operates for at least 70 hours each week
- Has one or more prescribing medical practitioners at the centre for at least
  70 of the hours each week that the medical centre operates

Distance measurement: Mid-point at ground level of the public access door.
"""
from typing import Dict, Optional, Tuple, List
from rules.base_rule import BaseRule
from utils.distance import (
    haversine_distance, find_nearest, find_within_radius, format_distance
)
import config
import json


class Item136Rule(BaseRule):
    """
    Item 136: New pharmacy in a large medical centre.

    Requirements:
    - Must be INSIDE a large medical centre (single management, 70+ hrs/week)
    - No existing pharmacy in the medical centre
    - >= 300m from nearest pharmacy (excl. pharmacies in large shopping centres/hospitals)
    - 8 FTE PBS prescribers (7 must be medical practitioners)
    - Centre open 70+ hrs/week with GP services 70+ hrs/week
    """

    @property
    def rule_name(self) -> str:
        return "Large Medical Centre (8 FTE prescribers, 70hrs/week)"

    @property
    def item_number(self) -> str:
        return "Item 136"

    def check_eligibility(self, property_data: Dict) -> Tuple[bool, Optional[str]]:
        """Check if property meets Item 136 requirements."""
        lat = property_data.get('latitude')
        lon = property_data.get('longitude')

        if lat is None or lon is None:
            return False, None

        pharmacies = self.db.get_all_pharmacies()

        # ---- Strategy 1: Check medical_centres table ----
        medical_centres = self.db.get_all_medical_centres()
        if medical_centres:
            nearby_centres = find_within_radius(lat, lon, medical_centres, 0.1)  # 100m

            for centre, centre_dist in nearby_centres:
                num_gps = centre.get('num_gps', 0) or 0
                total_fte = centre.get('total_fte', 0) or 0
                hours = centre.get('hours_per_week', 0) or 0

                # Need 8+ FTE PBS prescribers (use headcount as proxy)
                estimated_fte = total_fte if total_fte > 0 else num_gps * 0.8
                required_fte = config.FTE_REQUIREMENTS.get('item_136_prescribers', 8.0)

                if estimated_fte < required_fte and num_gps < 8:
                    continue

                # (b) No pharmacy already in the medical centre (within 100m)
                pharmacy_in_centre = False
                for pharm in pharmacies:
                    d = haversine_distance(
                        centre['latitude'], centre['longitude'],
                        pharm.get('latitude', 0), pharm.get('longitude', 0)
                    )
                    if d <= 0.1:  # 100m
                        pharmacy_in_centre = True
                        break

                if pharmacy_in_centre:
                    continue

                # (c) Distance check: >= 300m from nearest pharmacy,
                # EXCLUDING pharmacies in large shopping centres or private hospitals
                distance_ok = self._check_distance_from_pharmacies(
                    centre['latitude'], centre['longitude'], pharmacies
                )
                if not distance_ok[0]:
                    continue

                # Build confidence notes
                confidence_notes = []
                source = centre.get('source', 'unknown')

                if source == 'manual_research':
                    confidence_notes.append("verified data source")
                elif source in ('hotdoc', 'healthengine'):
                    confidence_notes.append(f"data from {source}")

                if hours >= 70:
                    confidence_notes.append(f"open {hours:.0f}hrs/week (>= 70 ✓)")
                elif hours > 0:
                    confidence_notes.append(f"only {hours:.0f}hrs/week (need 70+) ⚠️")
                else:
                    confidence_notes.append("hours unknown - VERIFY 70+hrs/week")

                pharm_info = distance_ok[1]

                evidence = self.format_evidence(
                    rule="Item 136: Large Medical Centre",
                    centre=f"'{centre.get('name', 'Unknown')}' at {centre.get('address', 'N/A')}",
                    practitioners=f"{num_gps} GPs ({estimated_fte:.1f} est. FTE, need 8 FTE with 7 medical practitioners)",
                    distance=pharm_info,
                    confidence=", ".join(confidence_notes),
                    note="VERIFY: single management, 70+ hrs/week centre + GP services, 8 FTE PBS prescribers (7 medical)"
                )
                return True, evidence

        # ---- Strategy 2: GP cluster fallback ----
        gps = self.db.get_all_gps()
        if not gps:
            return False, None

        nearby_gps = find_within_radius(lat, lon, gps, 0.2)  # 200m

        if not nearby_gps:
            return False, None

        total_fte = sum(gp[0].get('fte', 0) or 0 for gp in nearby_gps)
        num_practices = len(nearby_gps)

        required_fte = config.FTE_REQUIREMENTS.get('item_136_prescribers', 8.0)

        if total_fte >= required_fte:
            # Check distance from pharmacies
            distance_ok = self._check_distance_from_pharmacies(lat, lon, pharmacies)
            if not distance_ok[0]:
                return False, None

            gp_names = [gp[0].get('name', 'Unknown') for gp in nearby_gps[:5]]
            evidence = self.format_evidence(
                rule="Item 136: Potential large medical centre (GP cluster proxy)",
                total_fte=f"{total_fte:.1f}",
                num_practices=num_practices,
                nearby_practices=", ".join(gp_names),
                distance=distance_ok[1],
                note="GP CLUSTER PROXY - REQUIRES MANUAL VERIFICATION of single management, 70+ hrs/week, 8 FTE prescribers (7 medical)"
            )
            return True, evidence

        return False, None

    def _check_distance_from_pharmacies(
        self, lat: float, lon: float, pharmacies: List[Dict]
    ) -> Tuple[bool, str]:
        """
        Check >= 300m from nearest pharmacy,
        EXCLUDING pharmacies in large shopping centres or private hospitals.
        """
        if not pharmacies:
            return True, "No pharmacies in database"

        threshold_km = 0.3  # 300m
        centres = self.db.get_all_shopping_centres()
        hospitals = self.db.get_all_hospitals()

        nearest_qualifying = None
        nearest_qualifying_dist = float('inf')

        for pharm in pharmacies:
            plat = pharm.get('latitude', 0)
            plon = pharm.get('longitude', 0)
            if not plat or not plon:
                continue

            dist = haversine_distance(lat, lon, plat, plon)

            # Exclude pharmacies in large shopping centres
            in_large_centre = False
            for centre in centres:
                tenants = centre.get('estimated_tenants', 0) or 0
                if tenants >= config.SHOPPING_CENTRE_THRESHOLDS.get('large_centre_tenants', 50):
                    d_to_centre = haversine_distance(
                        plat, plon,
                        centre['latitude'], centre['longitude']
                    )
                    if d_to_centre <= 0.15:
                        in_large_centre = True
                        break

            # Exclude pharmacies in private hospitals
            in_hospital = False
            for hospital in hospitals:
                h_type = (hospital.get('hospital_type', '') or '').lower()
                if 'public' in h_type:
                    continue
                d_to_hosp = haversine_distance(
                    plat, plon,
                    hospital.get('latitude', 0), hospital.get('longitude', 0)
                )
                if d_to_hosp <= 0.15:
                    in_hospital = True
                    break

            if in_large_centre or in_hospital:
                continue

            if dist < nearest_qualifying_dist:
                nearest_qualifying_dist = dist
                nearest_qualifying = pharm

        if nearest_qualifying is None:
            return True, "No qualifying pharmacies nearby (all in large centres/hospitals)"

        if nearest_qualifying_dist >= threshold_km:
            return True, f"Nearest pharmacy (excl. large centres/hospitals): {nearest_qualifying.get('name', 'Unknown')} at {format_distance(nearest_qualifying_dist)} (>= 300m ✓)"

        return False, f"Too close to {nearest_qualifying.get('name', 'Unknown')} ({format_distance(nearest_qualifying_dist)} < 300m)"
