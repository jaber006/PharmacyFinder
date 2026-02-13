"""
Rule Item 131: New Pharmacy - Rural/Remote (at least 10km)

Requirements:
- Proposed premises at least 10km by SHORTEST LAWFUL ACCESS ROUTE from nearest approved premises
- No supermarket/GP requirements
- Suitable for rural/remote areas with significant travel distance
"""
from typing import Dict, Optional, Tuple
from rules.base_rule import BaseRule
from utils.distance import find_nearest, get_driving_distance, haversine_distance, format_distance
import config


class Item131Rule(BaseRule):
    """
    Item 131: New pharmacy (at least 10 km by road)
    
    Strategy:
    1. First check straight-line distance to nearest pharmacy
    2. If straight-line < 7km, definitely doesn't qualify (no road is shorter than straight line)
    3. If straight-line >= 10km, likely qualifies - verify with OSRM for nearest few
    4. Only make OSRM calls for the closest pharmacies (saves API calls)
    """

    @property
    def rule_name(self) -> str:
        return "New Pharmacy - Rural (10km route)"

    @property
    def item_number(self) -> str:
        return "Item 131"

    def check_eligibility(self, property_data: Dict) -> Tuple[bool, Optional[str]]:
        lat = property_data.get('latitude')
        lon = property_data.get('longitude')

        if lat is None or lon is None:
            return False, None

        pharmacies = self.db.get_all_pharmacies()

        if not pharmacies:
            return True, "No existing pharmacies in database"

        # Step 1: Find nearest pharmacy by straight-line
        nearest_pharmacy, straight_line_km = find_nearest(lat, lon, pharmacies)

        if nearest_pharmacy is None:
            return True, "No existing pharmacies with valid coordinates"

        # Step 2: Quick reject - if straight-line < 7km, can't be 10km by road
        if straight_line_km < 7.0:
            return False, None

        # Step 3: If straight-line >= 15km, almost certainly qualifies
        # Use straight-line * 1.3 as estimate (roads are ~30% longer on average)
        if straight_line_km >= 15.0:
            estimated_route = straight_line_km * 1.3
            evidence = self.format_evidence(
                rule="10km+ by estimated route distance",
                nearest_pharmacy=nearest_pharmacy.get('name', 'Unknown'),
                nearest_address=nearest_pharmacy.get('address', 'Unknown'),
                straight_line=format_distance(straight_line_km),
                estimated_route=format_distance(estimated_route),
                note="Estimated - manual verification recommended"
            )
            return True, evidence

        # Step 4: For borderline cases (7-15km straight line), try OSRM for nearest pharmacy only
        route_km = get_driving_distance(
            lat, lon,
            nearest_pharmacy['latitude'], nearest_pharmacy['longitude']
        )

        if route_km is not None:
            if route_km >= 10.0:
                evidence = self.format_evidence(
                    rule="10km+ by shortest lawful access route",
                    nearest_pharmacy=nearest_pharmacy.get('name', 'Unknown'),
                    nearest_address=nearest_pharmacy.get('address', 'Unknown'),
                    route_distance=format_distance(route_km),
                    measurement="OSRM driving route"
                )
                return True, evidence
            else:
                return False, None
        else:
            # OSRM failed - use estimate
            estimated_route = straight_line_km * 1.3
            if estimated_route >= 10.0:
                evidence = self.format_evidence(
                    rule="10km+ by estimated route distance",
                    nearest_pharmacy=nearest_pharmacy.get('name', 'Unknown'),
                    straight_line=format_distance(straight_line_km),
                    estimated_route=format_distance(estimated_route),
                    note="OSRM unavailable - estimated"
                )
                return True, evidence

        return False, None
