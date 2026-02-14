"""
Rule Item 136: New pharmacy in a Large Medical Centre

Requirements (from PBS Pharmacy Location Rules):
(a) Proposed premises are in a large medical centre
(b) No approved pharmacy currently in the large medical centre
(c) Distance from nearest approved pharmacy >= 300m
    (unless pharmacy is in a large shopping centre or hospital)
(d) At least 8 FTE PBS prescribers, of which at least 7 must be medical practitioners
(e) The medical centre operates for at least 70 hours per week
(f) General practice services available for at least 70 hours per week

Large Medical Centre definition:
- Under single management
- Open for at least 70 hours per week
- Providing general practice services for at least 70 hours per week

Data approach:
- Uses the medical_centres table populated by HotDoc/HealthEngine scrapers
- Practitioner headcount is used as a proxy for FTE (with 0.8 multiplier)
- Falls back to GP cluster analysis from the gps table
"""
from typing import Dict, Optional, Tuple
from rules.base_rule import BaseRule
from utils.distance import find_nearest, find_within_radius, format_distance
import config


class Item136Rule(BaseRule):
    """
    Item 136: New pharmacy in a large medical centre.
    
    Uses the medical_centres table (populated by scrapers) to identify
    centres with 8+ FTE prescribers. Also checks GP clusters as fallback.
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
        
        Strategy:
        1. Check medical_centres table for centre within 100m with 8+ GPs
        2. Check that no existing pharmacy is within 100m of the centre  
        3. Check 300m distance from nearest pharmacy
        4. Fall back to GP cluster analysis if no medical_centres data
        """
        lat = property_data.get('latitude')
        lon = property_data.get('longitude')

        if lat is None or lon is None:
            return False, None

        # ---- Strategy 1: Check medical_centres table ----
        medical_centres = self.db.get_all_medical_centres()
        if medical_centres:
            nearby_centres = find_within_radius(lat, lon, medical_centres, 0.1)  # 100m
            
            for centre, centre_dist in nearby_centres:
                num_gps = centre.get('num_gps', 0)
                total_fte = centre.get('total_fte', 0)
                hours = centre.get('hours_per_week', 0)
                
                # Need 8+ FTE prescribers (use headcount as proxy)
                # Practitioners typically work ~0.8 FTE on average
                estimated_fte = total_fte if total_fte > 0 else num_gps * 0.8
                required_fte = config.FTE_REQUIREMENTS.get('item_136_prescribers', 8.0)
                
                if estimated_fte >= required_fte or num_gps >= 8:
                    # Check: no pharmacy already in the medical centre (within 100m)
                    pharmacies = self.db.get_all_pharmacies()
                    pharmacy_in_centre = False
                    for pharm in pharmacies:
                        from utils.distance import haversine_distance
                        d = haversine_distance(
                            centre['latitude'], centre['longitude'],
                            pharm.get('latitude', 0), pharm.get('longitude', 0)
                        )
                        if d <= 0.1:  # 100m
                            pharmacy_in_centre = True
                            break
                    
                    if pharmacy_in_centre:
                        continue  # This centre already has a pharmacy
                    
                    # Check 300m rule: nearest pharmacy must be >= 300m
                    nearest_pharm, pharm_dist = find_nearest(
                        centre['latitude'], centre['longitude'], pharmacies
                    )
                    
                    if nearest_pharm and pharm_dist < 0.3:
                        # Too close to existing pharmacy (unless it's in shopping centre/hospital)
                        continue
                    
                    # Determine confidence
                    confidence_notes = []
                    source = centre.get('source', 'unknown')
                    
                    if source == 'manual_research':
                        confidence_notes.append("verified data source")
                    elif source in ('hotdoc', 'healthengine'):
                        confidence_notes.append(f"data from {source}")
                    
                    if hours >= 70:
                        confidence_notes.append(f"open {hours:.0f}hrs/week")
                    elif hours > 0:
                        confidence_notes.append(f"only {hours:.0f}hrs/week (need 70+)")
                    else:
                        confidence_notes.append("hours unknown")
                    
                    pharm_info = ""
                    if nearest_pharm:
                        pharm_info = f"nearest pharmacy: {nearest_pharm.get('name', 'Unknown')} ({format_distance(pharm_dist)})"
                    
                    evidence = self.format_evidence(
                        rule="Large Medical Centre (Item 136)",
                        centre=f"'{centre.get('name', 'Unknown')}' at {centre.get('address', 'N/A')}",
                        practitioners=f"{num_gps} GPs ({estimated_fte:.1f} est. FTE)",
                        nearest_pharmacy=pharm_info,
                        confidence=", ".join(confidence_notes),
                        note="VERIFY: centre under single management, 70+ hrs/week GP services"
                    )
                    return True, evidence

        # ---- Strategy 2: GP cluster fallback (original approach) ----
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
            gp_names = [gp[0].get('name', 'Unknown') for gp in nearby_gps[:5]]
            evidence = self.format_evidence(
                rule="Potential large medical centre (GP cluster proxy)",
                total_fte=f"{total_fte:.1f}",
                num_practices=num_practices,
                nearby_practices=", ".join(gp_names),
                note="GP CLUSTER PROXY - no confirmed medical centre data. REQUIRES MANUAL VERIFICATION"
            )
            return True, evidence

        return False, None
