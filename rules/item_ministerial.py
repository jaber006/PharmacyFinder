"""
Rule: Ministerial Opportunity Finder

NOT a standard ACPA rule (Items 130-136). This identifies locations where the
Health Minister could approve a new pharmacy based on genuine community need,
even though standard rules aren't met.

The Health Minister has discretion to approve pharmacy locations that don't meet
the standard ACPA distance/infrastructure requirements when there's a compelling
case for community need.

Categories:
  A — Near-miss on standard rule distance thresholds
  B — Underserviced high-population areas
  C — Medical need indicators (large medical centres, hospitals, aged care)
  D — Growth corridors (high population, low pharmacy density)

Each opportunity is scored 0-100 on the strength of the ministerial case.
"""
from typing import Dict, Optional, Tuple, List
from rules.base_rule import BaseRule
from utils.distance import (
    haversine_distance, find_nearest, find_within_radius, format_distance,
    get_driving_distance,
)
import config
import json
from datetime import datetime


# ---------------------------------------------------------------------------
# Scoring weights
# ---------------------------------------------------------------------------
WEIGHT_POPULATION = 0.25
WEIGHT_DISTANCE = 0.20
WEIGHT_GP_PRESENCE = 0.15
WEIGHT_NEAR_MISS = 0.20
WEIGHT_PHARMACY_DENSITY = 0.15
WEIGHT_GROWTH = 0.05

# ---------------------------------------------------------------------------
# Thresholds for ministerial categories
# ---------------------------------------------------------------------------
# Category A — near-miss bands (just below standard thresholds)
NEAR_MISS_130_MIN_KM = 1.0     # Item 130 threshold is 1.5km
NEAR_MISS_130_MAX_KM = 1.49
NEAR_MISS_131_MIN_KM = 7.0     # Item 131 threshold is 10km
NEAR_MISS_131_MAX_KM = 9.99
NEAR_MISS_133_MIN_M = 300       # Item 133 threshold is 500m
NEAR_MISS_133_MAX_M = 499
NEAR_MISS_136_MIN_M = 200       # Item 136 threshold is 300m
NEAR_MISS_136_MAX_M = 299

# Category B — underserviced high-population
UNDERSERVICED_POP_5KM = 20000
UNDERSERVICED_DENSITY_PER_PHARMACY = 5000  # 1 pharmacy per 5,000 people
UNDERSERVICED_MIN_DISTANCE_KM = 1.0

# Category C — medical need
MEDICAL_CENTRE_MIN_GPS = 5
MEDICAL_CENTRE_MAX_PHARMACY_DIST_KM = 0.5
HOSPITAL_NEARBY_RADIUS_KM = 0.5

# Category D — growth corridors
GROWTH_POP_5KM = 10000
GROWTH_MAX_PHARMACIES_5KM = 2
GROWTH_BAD_RATIO = 4000  # 1 pharmacy per 4,000+ people is poor


class MinisterialOpportunity:
    """Represents a single ministerial opportunity with scoring and evidence."""

    def __init__(self):
        self.categories: List[str] = []
        self.sub_reasons: List[str] = []
        self.near_miss_rule: Optional[str] = None
        self.near_miss_gap: Optional[str] = None
        self.score: float = 0.0
        self.score_breakdown: Dict[str, float] = {}
        self.evidence_parts: Dict[str, str] = {}

    def add_category(self, category: str, reason: str):
        if category not in self.categories:
            self.categories.append(category)
        self.sub_reasons.append(f"[{category}] {reason}")

    @property
    def category_string(self) -> str:
        return ", ".join(sorted(set(self.categories)))

    def build_case_summary(self, opp_data: Dict) -> str:
        """Build a human-readable case summary for a ministerial application."""
        lines = []
        lines.append(f"## Ministerial Case: {opp_data.get('address', 'Unknown Location')}")
        lines.append(f"**Region:** {opp_data.get('region', 'Unknown')}")
        lines.append(f"**Ministerial Score:** {self.score:.1f}/100")
        lines.append(f"**Categories:** {self.category_string}")
        lines.append("")

        # Community need statement
        lines.append("### Community Need Statement")
        pop_5km = opp_data.get('pop_5km', 0) or 0
        pharm_5km = opp_data.get('pharmacy_5km', 0) or 0
        nearest_km = opp_data.get('nearest_pharmacy_km', 0) or 0
        nearest_name = opp_data.get('nearest_pharmacy_name', 'Unknown')

        if pop_5km > 0 and pharm_5km > 0:
            ratio = pop_5km / pharm_5km
            lines.append(
                f"This location serves a community of approximately **{pop_5km:,} residents** "
                f"within 5km, currently serviced by only **{pharm_5km} pharmacies** "
                f"(ratio of 1 pharmacy per {ratio:,.0f} people). "
            )
        elif pop_5km > 0:
            lines.append(
                f"This location serves a community of approximately **{pop_5km:,} residents** "
                f"within 5km. "
            )

        lines.append(
            f"The nearest pharmacy is **{nearest_name}** at **{format_distance(nearest_km)}**."
        )
        lines.append("")

        # Near-miss analysis
        if self.near_miss_rule:
            lines.append("### Near-Miss on Standard Rules")
            lines.append(f"- **Almost qualifies under:** {self.near_miss_rule}")
            lines.append(f"- **Gap:** {self.near_miss_gap}")
            lines.append("")

        # Category details
        lines.append("### Qualifying Factors")
        for reason in self.sub_reasons:
            lines.append(f"- {reason}")
        lines.append("")

        # Score breakdown
        lines.append("### Score Breakdown")
        for component, value in self.score_breakdown.items():
            lines.append(f"- {component}: {value:.1f}")
        lines.append(f"- **Total: {self.score:.1f}/100**")
        lines.append("")

        # POI info
        poi_name = opp_data.get('poi_name', '')
        poi_type = opp_data.get('poi_type', '')
        if poi_name:
            lines.append("### Key Infrastructure")
            lines.append(f"- **Point of interest:** {poi_name} ({poi_type})")

        # Growth
        growth = opp_data.get('growth_indicator', '')
        growth_details = opp_data.get('growth_details', '')
        if growth and growth != '':
            lines.append(f"- **Growth indicator:** {growth}")
            if growth_details:
                lines.append(f"- **Growth details:** {growth_details}")

        lines.append("")
        lines.append("---")
        return "\n".join(lines)


class ItemMinisterialRule(BaseRule):
    """
    Ministerial Opportunity Finder — NOT a standard ACPA rule.

    Identifies locations that don't qualify under Items 130-136 but where
    there's a strong case for ministerial approval based on genuine
    community need.
    """

    @property
    def rule_name(self) -> str:
        return "Ministerial Opportunity (Community Need)"

    @property
    def item_number(self) -> str:
        return "Ministerial"

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------
    def check_eligibility(self, property_data: Dict) -> Tuple[bool, Optional[str]]:
        """
        Check if a location is a viable ministerial opportunity.

        Unlike standard rules which return True/False, this rule is designed
        to run ONLY on locations that already failed all standard rules
        (qualifying_rules = 'NONE').

        Returns:
            Tuple of (is_ministerial_opportunity, evidence_string)
        """
        lat = property_data.get('latitude')
        lon = property_data.get('longitude')

        if lat is None or lon is None:
            return False, None

        opp = MinisterialOpportunity()

        # Load reference data (cached on self for batch runs)
        pharmacies = self._get_pharmacies()
        nearest_pharmacy, nearest_km = find_nearest(lat, lon, pharmacies)

        if nearest_pharmacy is None:
            return False, None

        # Quick reject: if nearest pharmacy is < 200m, not a ministerial case
        if nearest_km < 0.2:
            return False, None

        # Run all category checks
        self._check_category_a(opp, property_data, lat, lon, nearest_pharmacy, nearest_km)
        self._check_category_b(opp, property_data, lat, lon, nearest_pharmacy, nearest_km)
        self._check_category_c(opp, property_data, lat, lon, nearest_pharmacy, nearest_km)
        self._check_category_d(opp, property_data, lat, lon, nearest_pharmacy, nearest_km)

        if not opp.categories:
            return False, None

        # Calculate score
        opp.score = self._calculate_score(opp, property_data, lat, lon,
                                          nearest_pharmacy, nearest_km)

        # Minimum score threshold to qualify
        if opp.score < 25:
            return False, None

        # Build evidence
        evidence = self._build_evidence(opp, property_data, nearest_pharmacy, nearest_km)
        return True, evidence

    # ------------------------------------------------------------------
    # Extended entry point for batch scans (returns the full opportunity)
    # ------------------------------------------------------------------
    def check_ministerial(self, property_data: Dict) -> Optional[MinisterialOpportunity]:
        """
        Full ministerial check returning the MinisterialOpportunity object
        (used by the batch scan script for richer output).
        """
        lat = property_data.get('latitude')
        lon = property_data.get('longitude')

        if lat is None or lon is None:
            return None

        opp = MinisterialOpportunity()

        pharmacies = self._get_pharmacies()
        nearest_pharmacy, nearest_km = find_nearest(lat, lon, pharmacies)

        if nearest_pharmacy is None:
            return None

        if nearest_km < 0.2:
            return None

        self._check_category_a(opp, property_data, lat, lon, nearest_pharmacy, nearest_km)
        self._check_category_b(opp, property_data, lat, lon, nearest_pharmacy, nearest_km)
        self._check_category_c(opp, property_data, lat, lon, nearest_pharmacy, nearest_km)
        self._check_category_d(opp, property_data, lat, lon, nearest_pharmacy, nearest_km)

        if not opp.categories:
            return None

        opp.score = self._calculate_score(opp, property_data, lat, lon,
                                          nearest_pharmacy, nearest_km)

        if opp.score < 25:
            return None

        return opp

    # ------------------------------------------------------------------
    # Category A: Near-miss on distance thresholds
    # ------------------------------------------------------------------
    def _check_category_a(self, opp: MinisterialOpportunity, prop: Dict,
                          lat: float, lon: float,
                          nearest_pharm: Dict, nearest_km: float):
        """Category A — Near-miss on standard ACPA rule distances."""

        # Item 130 near-miss: 1.0-1.49km with high pop or GP presence
        if NEAR_MISS_130_MIN_KM <= nearest_km <= NEAR_MISS_130_MAX_KM:
            pop_5km = prop.get('pop_5km', 0) or 0
            nearby_gps = find_within_radius(lat, lon, self._get_gps(), 0.5)
            has_pop = pop_5km > 10000
            has_gps = len(nearby_gps) > 0

            if has_pop or has_gps:
                gap = f"{(1.5 - nearest_km)*1000:.0f}m short of 1.5km threshold"
                opp.add_category("A", f"Item 130 near-miss: nearest pharmacy at {format_distance(nearest_km)} ({gap})")
                opp.near_miss_rule = "Item 130 (1.5km from nearest pharmacy)"
                opp.near_miss_gap = gap
                if has_gps:
                    gp_names = [g[0].get('name', 'GP') for g in nearby_gps[:3]]
                    opp.add_category("A", f"GP presence within 500m: {', '.join(gp_names)}")
                if has_pop:
                    opp.add_category("A", f"High population ({pop_5km:,} within 5km)")

        # Item 131 near-miss: 7-9.99km by road
        if nearest_km >= 5.0:  # straight-line >= 5km could be 7-10km by road
            if 5.0 <= nearest_km <= 9.99:
                # Estimate road distance
                road_km = nearest_km * 1.3  # roads are ~30% longer
                if NEAR_MISS_131_MIN_KM <= road_km <= NEAR_MISS_131_MAX_KM:
                    gap = f"{(10 - road_km):.1f}km short of 10km road threshold (estimated)"
                    opp.add_category("A", f"Item 131 near-miss: ~{road_km:.1f}km by road ({gap})")
                    if not opp.near_miss_rule:
                        opp.near_miss_rule = "Item 131 (10km by road from nearest pharmacy)"
                        opp.near_miss_gap = gap

        # Item 133 near-miss: 300-499m from nearest pharmacy, near a shopping centre
        nearest_m = nearest_km * 1000
        if NEAR_MISS_133_MIN_M <= nearest_m <= NEAR_MISS_133_MAX_M:
            centres = self._get_shopping_centres()
            nearby_centres = find_within_radius(lat, lon, centres, 0.2)  # within 200m
            if nearby_centres:
                centre_name = nearby_centres[0][0].get('name', 'Shopping Centre')
                gap = f"{500 - nearest_m:.0f}m short of 500m threshold"
                opp.add_category("A", f"Item 133 near-miss: {format_distance(nearest_km)} from pharmacy, near {centre_name} ({gap})")
                if not opp.near_miss_rule:
                    opp.near_miss_rule = "Item 133 (500m in small shopping centre)"
                    opp.near_miss_gap = gap

        # Item 136 near-miss: 200-299m from nearest pharmacy, near a medical centre
        if NEAR_MISS_136_MIN_M <= nearest_m <= NEAR_MISS_136_MAX_M:
            med_centres = self._get_medical_centres()
            nearby_med = find_within_radius(lat, lon, med_centres, 0.1)  # within 100m
            if nearby_med:
                mc = nearby_med[0][0]
                mc_name = mc.get('name', 'Medical Centre')
                num_gps = mc.get('num_gps', 0) or 0
                gap = f"{300 - nearest_m:.0f}m short of 300m threshold"
                opp.add_category("A", f"Item 136 near-miss: {format_distance(nearest_km)} from pharmacy, near {mc_name} ({num_gps} GPs) ({gap})")
                if not opp.near_miss_rule:
                    opp.near_miss_rule = "Item 136 (300m from pharmacy in large medical centre)"
                    opp.near_miss_gap = gap

    # ------------------------------------------------------------------
    # Category B: Underserviced high-population areas
    # ------------------------------------------------------------------
    def _check_category_b(self, opp: MinisterialOpportunity, prop: Dict,
                          lat: float, lon: float,
                          nearest_pharm: Dict, nearest_km: float):
        """Category B — Underserviced high-population areas."""
        pop_5km = prop.get('pop_5km', 0) or 0
        pharm_5km = prop.get('pharmacy_5km', 0) or 0

        # High population + low pharmacy density
        if pop_5km >= UNDERSERVICED_POP_5KM and pharm_5km > 0:
            people_per_pharmacy = pop_5km / pharm_5km
            if people_per_pharmacy >= UNDERSERVICED_DENSITY_PER_PHARMACY:
                opp.add_category("B",
                    f"Underserviced: {pop_5km:,} people within 5km, "
                    f"only {pharm_5km} pharmacies "
                    f"(1 per {people_per_pharmacy:,.0f} people)"
                )

        # Nearest pharmacy > 1km but doesn't meet ANY standard rule threshold
        if nearest_km >= UNDERSERVICED_MIN_DISTANCE_KM:
            # If we got here, it already failed all standard rules
            opp.add_category("B",
                f"Nearest pharmacy is {format_distance(nearest_km)} away "
                f"but doesn't meet any standard ACPA rule threshold"
            )

    # ------------------------------------------------------------------
    # Category C: Medical need indicators
    # ------------------------------------------------------------------
    def _check_category_c(self, opp: MinisterialOpportunity, prop: Dict,
                          lat: float, lon: float,
                          nearest_pharm: Dict, nearest_km: float):
        """Category C — Medical need indicators."""

        # Large medical centres (5+ GPs) with no pharmacy within 500m
        # but doesn't meet Item 136's 8 FTE / 70hrs requirements
        med_centres = self._get_medical_centres()
        nearby_med = find_within_radius(lat, lon, med_centres, 0.5)

        for mc, mc_dist in nearby_med:
            num_gps = mc.get('num_gps', 0) or 0
            total_fte = mc.get('total_fte', 0) or 0
            hours = mc.get('hours_per_week', 0) or 0

            if num_gps >= MEDICAL_CENTRE_MIN_GPS:
                # Check no pharmacy within 500m of this medical centre
                pharmacies = self._get_pharmacies()
                nearby_pharms = find_within_radius(
                    mc['latitude'], mc['longitude'], pharmacies, 0.5
                )
                if not nearby_pharms:
                    shortfalls = []
                    fte_est = total_fte if total_fte > 0 else num_gps * 0.8
                    if fte_est < 8:
                        shortfalls.append(f"only {fte_est:.1f} FTE (need 8)")
                    if hours > 0 and hours < 70:
                        shortfalls.append(f"only {hours:.0f}hrs/week (need 70)")
                    elif hours == 0:
                        shortfalls.append("hours unknown")

                    shortfall_str = "; ".join(shortfalls) if shortfalls else "doesn't meet FTE/hours"
                    opp.add_category("C",
                        f"Large medical centre '{mc.get('name', 'Unknown')}' "
                        f"({num_gps} GPs) at {format_distance(mc_dist)} "
                        f"with no pharmacy within 500m — {shortfall_str}"
                    )

        # Near hospitals that don't meet Item 135 thresholds
        hospitals = self._get_hospitals()
        nearby_hospitals = find_within_radius(lat, lon, hospitals, HOSPITAL_NEARBY_RADIUS_KM)

        for hosp, hosp_dist in nearby_hospitals:
            beds = hosp.get('bed_count', 0) or 0
            h_type = (hosp.get('hospital_type', '') or '').lower()

            # Check no pharmacy already in/near the hospital
            pharmacies = self._get_pharmacies()
            pharms_near_hosp = find_within_radius(
                hosp['latitude'], hosp['longitude'], pharmacies, 0.2
            )

            if not pharms_near_hosp:
                if 'public' in h_type:
                    # Public hospitals can't use Item 135 (private only)
                    opp.add_category("C",
                        f"Near public hospital '{hosp.get('name', 'Unknown')}' "
                        f"({beds} beds) at {format_distance(hosp_dist)} "
                        f"with no pharmacy within 200m — Item 135 requires PRIVATE hospital"
                    )
                elif beds < config.HOSPITAL_ADMISSION_CAPACITY:
                    # Private but too small for Item 135
                    opp.add_category("C",
                        f"Near hospital '{hosp.get('name', 'Unknown')}' "
                        f"({beds} beds, type: {h_type or 'unknown'}) at {format_distance(hosp_dist)} "
                        f"with no pharmacy within 200m — Item 135 requires 150+ beds"
                    )

        # Near GP clusters that indicate medical need
        gps = self._get_gps()
        nearby_gps = find_within_radius(lat, lon, gps, 0.5)
        if len(nearby_gps) >= 3 and nearest_km > 0.5:
            total_fte = sum(g[0].get('fte', 0) or 0 for g in nearby_gps)
            gp_names = [g[0].get('name', 'GP') for g in nearby_gps[:5]]
            opp.add_category("C",
                f"GP cluster: {len(nearby_gps)} practices within 500m "
                f"({total_fte:.1f} total FTE) — {', '.join(gp_names)}"
            )

    # ------------------------------------------------------------------
    # Category D: Growth corridors
    # ------------------------------------------------------------------
    def _check_category_d(self, opp: MinisterialOpportunity, prop: Dict,
                          lat: float, lon: float,
                          nearest_pharm: Dict, nearest_km: float):
        """Category D — Growth corridors with poor pharmacy-to-population ratio."""
        pop_5km = prop.get('pop_5km', 0) or 0
        pharm_5km = prop.get('pharmacy_5km', 0) or 0

        # High population but only 1-2 pharmacies
        if pop_5km >= GROWTH_POP_5KM and 0 < pharm_5km <= GROWTH_MAX_PHARMACIES_5KM:
            ratio = pop_5km / pharm_5km
            opp.add_category("D",
                f"Growth corridor: {pop_5km:,} people within 5km "
                f"but only {pharm_5km} pharmacy(ies) (1 per {ratio:,.0f})"
            )

        # Bad pharmacy-to-population ratio
        if pop_5km > 0 and pharm_5km > 0:
            ratio = pop_5km / pharm_5km
            if ratio >= GROWTH_BAD_RATIO:
                # Only add if not already covered by the check above
                if not (pop_5km >= GROWTH_POP_5KM and pharm_5km <= GROWTH_MAX_PHARMACIES_5KM):
                    opp.add_category("D",
                        f"Poor pharmacy ratio: 1 per {ratio:,.0f} people "
                        f"({pop_5km:,} pop / {pharm_5km} pharmacies within 5km)"
                    )

        # Growth indicator from existing data
        growth_indicator = prop.get('growth_indicator', '') or ''
        growth_details = prop.get('growth_details', '') or ''
        if growth_indicator and growth_indicator.strip():
            opp.add_category("D",
                f"Growth signal: {growth_indicator}"
                + (f" — {growth_details}" if growth_details else "")
            )

    # ------------------------------------------------------------------
    # Scoring
    # ------------------------------------------------------------------
    def _calculate_score(self, opp: MinisterialOpportunity, prop: Dict,
                         lat: float, lon: float,
                         nearest_pharm: Dict, nearest_km: float) -> float:
        """
        Calculate a ministerial opportunity score from 0-100.

        Components:
        - Population factor (25%): higher population = stronger case
        - Distance factor (20%): further from nearest pharmacy = stronger
        - GP/medical presence (15%): more GPs nearby = stronger
        - Near-miss factor (20%): closer to qualifying under standard rules = stronger
        - Pharmacy density (15%): fewer pharmacies per capita = stronger
        - Growth factor (5%): growth indicators present = stronger
        """
        pop_5km = prop.get('pop_5km', 0) or 0
        pharm_5km = prop.get('pharmacy_5km', 0) or 0

        # 1. Population factor (0-100)
        # 50k+ = max score, scale linearly
        pop_score = min(100, (pop_5km / 50000) * 100)

        # 2. Distance factor (0-100)
        # 0.2km = 0, 5km+ = 100
        dist_score = min(100, max(0, (nearest_km - 0.2) / 4.8 * 100))

        # 3. GP/medical presence (0-100)
        gps = self._get_gps()
        nearby_gps = find_within_radius(lat, lon, gps, 0.5)
        med_centres = self._get_medical_centres()
        nearby_med = find_within_radius(lat, lon, med_centres, 0.5)

        gp_count = len(nearby_gps)
        med_count = len(nearby_med)
        # 10+ GPs or 3+ medical centres = max
        gp_score = min(100, (gp_count / 10) * 60 + (med_count / 3) * 40)

        # 4. Near-miss factor (0-100)
        # Having a near-miss is a strong indicator; multiple = stronger
        near_miss_score = 0
        if "A" in opp.categories:
            near_miss_score = 80
            # Bonus for very close misses
            if opp.near_miss_gap:
                try:
                    # Extract the gap number
                    gap_str = opp.near_miss_gap.split()[0]
                    gap_val = float(gap_str.replace('m', '').replace('km', ''))
                    # Smaller gap = higher score
                    if gap_val < 100:  # within 100m of threshold
                        near_miss_score = 95
                    elif gap_val < 200:
                        near_miss_score = 90
                except (ValueError, IndexError):
                    pass

        # 5. Pharmacy density (0-100)
        # Worse ratio = higher score
        density_score = 0
        if pharm_5km > 0 and pop_5km > 0:
            ratio = pop_5km / pharm_5km
            # 1:8000 = max, 1:2000 = min
            density_score = min(100, max(0, (ratio - 2000) / 6000 * 100))
        elif pharm_5km == 0 and pop_5km > 0:
            density_score = 100

        # 6. Growth factor (0-100)
        growth_score = 0
        if "D" in opp.categories:
            growth_score = 70
        growth_indicator = prop.get('growth_indicator', '') or ''
        if growth_indicator:
            growth_score = max(growth_score, 80)

        # Store breakdown
        opp.score_breakdown = {
            'Population': pop_score * WEIGHT_POPULATION,
            'Distance': dist_score * WEIGHT_DISTANCE,
            'GP/Medical Presence': gp_score * WEIGHT_GP_PRESENCE,
            'Near-miss': near_miss_score * WEIGHT_NEAR_MISS,
            'Pharmacy Density': density_score * WEIGHT_PHARMACY_DENSITY,
            'Growth': growth_score * WEIGHT_GROWTH,
        }

        total = sum(opp.score_breakdown.values())
        return round(total, 1)

    # ------------------------------------------------------------------
    # Evidence builder
    # ------------------------------------------------------------------
    def _build_evidence(self, opp: MinisterialOpportunity, prop: Dict,
                        nearest_pharm: Dict, nearest_km: float) -> str:
        """Build the evidence string for DB storage."""
        parts = {
            'rule': f"MINISTERIAL: Community Need ({opp.category_string})",
            'score': f"{opp.score:.1f}/100",
            'nearest_pharmacy': f"{nearest_pharm.get('name', 'Unknown')} at {format_distance(nearest_km)}",
        }

        if opp.near_miss_rule:
            parts['near_miss'] = f"{opp.near_miss_rule} — {opp.near_miss_gap}"

        pop_5km = prop.get('pop_5km', 0) or 0
        pharm_5km = prop.get('pharmacy_5km', 0) or 0
        if pop_5km > 0:
            parts['population'] = f"{pop_5km:,} within 5km"
        if pharm_5km > 0:
            parts['pharmacies_5km'] = str(pharm_5km)
            if pop_5km > 0:
                parts['ratio'] = f"1 per {pop_5km // pharm_5km:,}"

        parts['categories'] = "; ".join(opp.sub_reasons[:5])
        parts['note'] = "MINISTERIAL APPROVAL REQUIRED — does not meet standard ACPA rules"

        return self.format_evidence(**parts)

    # ------------------------------------------------------------------
    # Cached data loaders (avoid re-querying in batch runs)
    # ------------------------------------------------------------------
    _pharmacy_cache = None
    _gp_cache = None
    _medical_centre_cache = None
    _hospital_cache = None
    _shopping_centre_cache = None

    def _get_pharmacies(self) -> List[Dict]:
        if self._pharmacy_cache is None:
            self.__class__._pharmacy_cache = self.db.get_all_pharmacies()
        return self._pharmacy_cache

    def _get_gps(self) -> List[Dict]:
        if self._gp_cache is None:
            self.__class__._gp_cache = self.db.get_all_gps()
        return self._gp_cache

    def _get_medical_centres(self) -> List[Dict]:
        if self._medical_centre_cache is None:
            self.__class__._medical_centre_cache = self.db.get_all_medical_centres()
        return self._medical_centre_cache

    def _get_hospitals(self) -> List[Dict]:
        if self._hospital_cache is None:
            self.__class__._hospital_cache = self.db.get_all_hospitals()
        return self._hospital_cache

    def _get_shopping_centres(self) -> List[Dict]:
        if self._shopping_centre_cache is None:
            self.__class__._shopping_centre_cache = self.db.get_all_shopping_centres()
        return self._shopping_centre_cache

    @classmethod
    def clear_cache(cls):
        """Clear cached reference data (call between scans or when data changes)."""
        cls._pharmacy_cache = None
        cls._gp_cache = None
        cls._medical_centre_cache = None
        cls._hospital_cache = None
        cls._shopping_centre_cache = None
