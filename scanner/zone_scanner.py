"""
Zone Scanner -- the core engine that flips the Pharmacy Location Rules
from "does this property qualify?" to "where in Australia can a new
pharmacy be opened?"

Strategy: POI-based scanning.  Instead of checking every coordinate we
scan around *existing* points of interest (supermarkets, hospitals, GP
clusters, gaps between pharmacies) and evaluate each location against
every applicable rule.

For each discovered opportunity we record:
  - location (lat/lon + human-readable address)
  - qualifying rule(s)
  - evidence string
  - confidence score  (0.0 - 1.0)
  - distance to nearest existing pharmacy
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from utils.database import Database
from utils.boundaries import in_state, STATE_BOUNDING_BOXES
from utils.distance import (
    find_nearest,
    find_within_radius,
    haversine_distance,
    format_distance,
    get_driving_distance,
)
from utils.geocoding import Geocoder
import config


# -- helpers -------------------------------------------------------

def _nearest_pharmacy(lat: float, lon: float, pharmacies: List[Dict]) -> Tuple[Optional[Dict], float]:
    """Return (nearest_pharmacy_dict, distance_km).  Distance is inf when list is empty."""
    if not pharmacies:
        return None, float('inf')
    return find_nearest(lat, lon, pharmacies)


def _has_nearby_pharmacy(lat: float, lon: float, pharmacies: List[Dict],
                         radius_km: float) -> bool:
    """True if any pharmacy is within *radius_km* of the point."""
    for p in pharmacies:
        if haversine_distance(lat, lon, p['latitude'], p['longitude']) <= radius_km:
            return True
    return False


# -- data-classes for results --------------------------------------

@dataclass
class Opportunity:
    latitude: float
    longitude: float
    address: str = ''
    qualifying_rules: List[str] = field(default_factory=list)
    evidence: List[str] = field(default_factory=list)
    confidence: float = 0.0
    nearest_pharmacy_km: float = 0.0
    nearest_pharmacy_name: str = ''
    poi_name: str = ''
    poi_type: str = ''
    region: str = ''

    def to_dict(self) -> Dict:
        return {
            'latitude': self.latitude,
            'longitude': self.longitude,
            'address': self.address,
            'qualifying_rules': ', '.join(self.qualifying_rules),
            'evidence': ' | '.join(self.evidence),
            'confidence': self.confidence,
            'nearest_pharmacy_km': self.nearest_pharmacy_km,
            'nearest_pharmacy_name': self.nearest_pharmacy_name,
            'poi_name': self.poi_name,
            'poi_type': self.poi_type,
            'region': self.region,
        }


# -- The scanner ---------------------------------------------------

class ZoneScanner:
    """
    Proactive opportunity zone scanner.

    Usage:
        scanner = ZoneScanner(db)
        opportunities = scanner.scan(region='TAS')
    """

    def __init__(self, db: Database):
        self.db = db
        # Cache reference data once per scan
        self._pharmacies: List[Dict] = []
        self._supermarkets: List[Dict] = []
        self._gps: List[Dict] = []
        self._hospitals: List[Dict] = []
        self._shopping_centres: List[Dict] = []
        self._medical_centres: List[Dict] = []

    # -- public API ------------------------------------------------

    def scan(self, region: str = 'TAS', verbose: bool = True,
             reverse_geocode: bool = True, geocode_limit: int = 20) -> List[Opportunity]:
        """
        Run all rule scanners and return a de-duplicated, scored list of
        opportunity zones.

        Args:
            region: State/territory code
            verbose: Print progress to stdout
            reverse_geocode: Reverse-geocode top opportunities to get addresses
            geocode_limit: Max number of opportunities to reverse-geocode
        """
        self._load_reference_data(region=region)

        if verbose:
            self._print_ref_summary()

        all_opps: List[Opportunity] = []

        scanners = [
            ('Item 130', self._scan_item_130),
            ('Item 131', self._scan_item_131),
            ('Item 132', self._scan_item_132),
            ('Item 133', self._scan_item_133),
            ('Item 134', self._scan_item_134),
            ('Item 134A', self._scan_item_134a),
            ('Item 135', self._scan_item_135),
            ('Item 136', self._scan_item_136),
        ]

        for label, fn in scanners:
            if verbose:
                print(f"\n  Scanning {label}...")
            opps = fn(region)
            if verbose:
                print(f"    -> {len(opps)} candidates")
            all_opps.extend(opps)

        # De-duplicate by proximity (merge opportunities within 200m)
        merged = self._merge_nearby(all_opps)
        if verbose:
            print(f"\n  After de-duplication: {len(merged)} unique opportunity zones")

        # Reverse-geocode top opportunities to get human-readable addresses
        if reverse_geocode and merged:
            self._reverse_geocode_opportunities(merged, limit=geocode_limit, verbose=verbose)

        # Persist to DB
        # Clear opportunities for this region AND any region-less leftovers
        self.db.clear_opportunities(region)
        for opp in merged:
            opp.region = region
            self.db.insert_opportunity(opp.to_dict())

        # Verify insertion count
        stored = self.db.get_all_opportunities(region)
        if verbose and len(stored) < len(merged):
            dropped = len(merged) - len(stored)
            print(f"  Note: {dropped} opportunities had DB conflicts (duplicate coords+rules)")
            print(f"  Stored: {len(stored)} opportunities")

        return merged

    def _reverse_geocode_opportunities(self, opps: List[Opportunity],
                                        limit: int = 20, verbose: bool = True):
        """Reverse-geocode top opportunities to get addresses."""
        geocoder = Geocoder(db=self.db)
        to_geocode = [o for o in opps if not o.address][:limit]

        if not to_geocode:
            return

        if verbose:
            print(f"\n  Reverse-geocoding top {len(to_geocode)} opportunities...")

        for i, opp in enumerate(to_geocode):
            try:
                address = geocoder.reverse_geocode(opp.latitude, opp.longitude)
                if address:
                    # Trim to a reasonable length
                    opp.address = address[:200]
            except Exception:
                pass

        geocoded = sum(1 for o in to_geocode if o.address)
        if verbose:
            print(f"    -> {geocoded}/{len(to_geocode)} resolved")

    # -- reference data --------------------------------------------

    def _load_reference_data(self, region: str = None):
        # Pharmacies: always load ALL (needed for distance calculations across state borders)
        self._pharmacies = self.db.get_all_pharmacies()
        
        # Other POIs: filter to target state with buffer for border areas
        # This prevents POIs in other states from generating false opportunities
        all_supermarkets = self.db.get_all_supermarkets()
        all_gps = self.db.get_all_gps()
        all_hospitals = self.db.get_all_hospitals()
        all_shopping_centres = self.db.get_all_shopping_centres()
        all_medical_centres = self.db.get_all_medical_centres()
        
        if region and region in STATE_BOUNDING_BOXES:
            min_lat, max_lat, min_lon, max_lon = STATE_BOUNDING_BOXES[region]
            # Add 0.5 degree buffer (~55km) for border areas
            buf = 0.5
            min_lat -= buf
            max_lat += buf
            min_lon -= buf
            max_lon += buf
            
            def _in_bounds(item):
                lat, lon = item.get('latitude', 0), item.get('longitude', 0)
                return min_lat <= lat <= max_lat and min_lon <= lon <= max_lon
            
            self._supermarkets = [s for s in all_supermarkets if _in_bounds(s)]
            self._gps = [g for g in all_gps if _in_bounds(g)]
            self._hospitals = [h for h in all_hospitals if _in_bounds(h)]
            self._shopping_centres = [c for c in all_shopping_centres if _in_bounds(c)]
            self._medical_centres = [m for m in all_medical_centres if _in_bounds(m)]
        else:
            self._supermarkets = all_supermarkets
            self._gps = all_gps
            self._hospitals = all_hospitals
            self._shopping_centres = all_shopping_centres
            self._medical_centres = all_medical_centres

    def _print_ref_summary(self):
        print(f"\n  Reference data loaded:")
        print(f"    Pharmacies:        {len(self._pharmacies)}")
        print(f"    Supermarkets:      {len(self._supermarkets)}")
        print(f"    GP practices:      {len(self._gps)}")
        print(f"    Hospitals:         {len(self._hospitals)}")
        print(f"    Shopping centres:  {len(self._shopping_centres)}")
        print(f"    Medical centres:   {len(self._medical_centres)}")

    # -- Item 130 -- 1.5 km from pharmacy + supermarket/GP ---------

    def _scan_item_130(self, region: str) -> List[Opportunity]:
        """
        Scan around every supermarket and GP cluster.
        A location qualifies if it is >= 1.5 km from nearest pharmacy AND
        within 500 m of either:
          (i)  a supermarket >= 1,000 sqm GLA + a GP >= 1 FTE, or
          (ii) a supermarket >= 2,500 sqm GLA.

        GLA is estimated by brand classification. Small-format stores
        (IGA Express/X-press, IGA Everyday) that fall below thresholds
        are flagged with lower confidence.
        """
        opps: List[Opportunity] = []
        threshold_km = config.RULE_DISTANCES['item_130']  # 1.5
        seen_coords: set = set()  # avoid duplicates

        # --- A) Scan from supermarkets ---
        for sm in self._supermarkets:
            lat, lon = sm['latitude'], sm['longitude']
            nearest_pharm, dist_km = _nearest_pharmacy(lat, lon, self._pharmacies)

            if dist_km < threshold_km:
                continue  # too close to existing pharmacy

            gla = sm.get('estimated_gla') or sm.get('floor_area_sqm') or 0
            brand = sm.get('brand', '')
            gla_conf = sm.get('gla_confidence', 'low')

            # Option (ii): large supermarket >= 2,500 sqm GLA
            if gla >= 2500:
                grid_key = (round(lat, 3), round(lon, 3))
                if grid_key not in seen_coords:
                    seen_coords.add(grid_key)
                    confidence = 0.80
                    if gla_conf == 'low':
                        confidence = 0.65
                    opp = self._make_opportunity(
                        lat, lon, nearest_pharm, dist_km,
                        rule='Item 130',
                        evidence=(f"Supermarket '{sm.get('name','')}' (est. GLA {gla:.0f} sqm, "
                                  f"brand: {brand or 'unknown'}) "
                                  f"is {format_distance(dist_km)} from nearest pharmacy"),
                        confidence=confidence,
                        poi_name=sm.get('name', ''),
                        poi_type='supermarket',
                    )
                    opps.append(opp)
                continue

            # Option (i): supermarket >= 1,000 sqm GLA + nearby GP >= 1 FTE
            if gla >= 1000:
                nearby_gps = find_within_radius(lat, lon, self._gps, 0.5)
                for gp, gp_dist in nearby_gps:
                    fte = gp.get('fte') or 0
                    if fte >= 1.0:
                        grid_key = (round(lat, 3), round(lon, 3))
                        if grid_key not in seen_coords:
                            seen_coords.add(grid_key)
                            confidence = 0.75
                            gla_note = ''
                            # Lower confidence for borderline brands
                            if brand in ('iga_express', 'iga_everyday'):
                                confidence = 0.55
                                gla_note = ' ⚠️ small-format store, GLA may not qualify'
                            elif brand == 'foodworks':
                                confidence = 0.60
                                gla_note = ' ⚠️ borderline GLA, requires verification'
                            elif gla_conf == 'low':
                                confidence = 0.60
                            opp = self._make_opportunity(
                                lat, lon, nearest_pharm, dist_km,
                                rule='Item 130',
                                evidence=(f"Supermarket '{sm.get('name','')}' (est. GLA {gla:.0f} sqm, "
                                          f"brand: {brand or 'unknown'}) + "
                                          f"GP '{gp.get('name','')}' ({fte:.1f} FTE) within 500 m; "
                                          f"{format_distance(dist_km)} from nearest pharmacy"
                                          f"{gla_note}"),
                                confidence=confidence,
                                poi_name=sm.get('name', ''),
                                poi_type='supermarket',
                            )
                            opps.append(opp)
                        break  # one GP is enough

        # --- B) Scan from GPs that are far from pharmacies ---
        for gp in self._gps:
            lat, lon = gp['latitude'], gp['longitude']
            grid_key = (round(lat, 3), round(lon, 3))
            if grid_key in seen_coords:
                continue

            nearest_pharm, dist_km = _nearest_pharmacy(lat, lon, self._pharmacies)
            if dist_km < threshold_km:
                continue

            fte = gp.get('fte') or 0
            if fte < 1.0:
                continue

            # Check for a supermarket >= 1,000 sqm GLA within 500m
            nearby_sms = find_within_radius(lat, lon, self._supermarkets, 0.5)
            for sm, sm_dist in nearby_sms:
                gla = sm.get('estimated_gla') or sm.get('floor_area_sqm') or 0
                brand = sm.get('brand', '')
                if gla >= 1000:
                    seen_coords.add(grid_key)
                    confidence = 0.75
                    gla_note = ''
                    if brand in ('iga_express', 'iga_everyday'):
                        confidence = 0.55
                        gla_note = ' ⚠️ small-format store, GLA may not qualify'
                    elif brand == 'foodworks':
                        confidence = 0.60
                        gla_note = ' ⚠️ borderline GLA, requires verification'
                    opp = self._make_opportunity(
                        lat, lon, nearest_pharm, dist_km,
                        rule='Item 130',
                        evidence=(f"GP '{gp.get('name','')}' ({fte:.1f} FTE) + "
                                  f"Supermarket '{sm.get('name','')}' (est. GLA {gla:.0f} sqm, "
                                  f"brand: {brand or 'unknown'}) within 500 m; "
                                  f"{format_distance(dist_km)} from nearest pharmacy"
                                  f"{gla_note}"),
                        confidence=confidence,
                        poi_name=gp.get('name', ''),
                        poi_type='gp',
                    )
                    opps.append(opp)
                    break

        return opps

    # -- Item 131 -- 10 km by road (rural) -------------------------

    def _scan_item_131(self, region: str) -> List[Opportunity]:
        """
        Find gaps >= 10 km (by road) between existing pharmacies.
        Strategy: for every supermarket / GP / hospital that is > 7 km
        straight-line from nearest pharmacy, verify with OSRM.
        """
        opps: List[Opportunity] = []
        candidate_pois: List[Dict] = []

        # Build candidate list from all POIs
        for sm in self._supermarkets:
            candidate_pois.append({**sm, '_poi_type': 'supermarket'})
        for gp in self._gps:
            candidate_pois.append({**gp, '_poi_type': 'gp'})
        for h in self._hospitals:
            candidate_pois.append({**h, '_poi_type': 'hospital'})

        for poi in candidate_pois:
            lat, lon = poi['latitude'], poi['longitude']
            nearest_pharm, straight_km = _nearest_pharmacy(lat, lon, self._pharmacies)

            if straight_km < 7.0:
                continue  # can't be 10 km by road

            # Very far -> almost certainly qualifies
            if straight_km >= 15.0:
                est_route = straight_km * 1.3
                opp = self._make_opportunity(
                    lat, lon, nearest_pharm, straight_km,
                    rule='Item 131',
                    evidence=(f"Near '{poi.get('name','')}' ({poi['_poi_type']}); "
                              f"straight-line {format_distance(straight_km)}, "
                              f"est. route {format_distance(est_route)} from nearest pharmacy"),
                    confidence=0.7,
                    poi_name=poi.get('name', ''),
                    poi_type=poi['_poi_type'],
                )
                opps.append(opp)
                continue

            # Borderline (7-15 km): use OSRM
            if nearest_pharm:
                route_km = get_driving_distance(
                    lat, lon,
                    nearest_pharm['latitude'], nearest_pharm['longitude'],
                )
                if route_km is not None and route_km >= 10.0:
                    opp = self._make_opportunity(
                        lat, lon, nearest_pharm, route_km,
                        rule='Item 131',
                        evidence=(f"Near '{poi.get('name','')}' ({poi['_poi_type']}); "
                                  f"route distance {format_distance(route_km)} from nearest pharmacy "
                                  f"(verified via OSRM)"),
                        confidence=0.85,
                        poi_name=poi.get('name', ''),
                        poi_type=poi['_poi_type'],
                    )
                    opps.append(opp)
                    time.sleep(0.3)  # rate-limit OSRM

        return opps

    # -- Item 132 -- Major shopping centre >= 15 000 sqm -------------

    def _scan_item_132(self, region: str) -> List[Opportunity]:
        """
        Scan shopping centres with GLA >= 15 000 sqm that do NOT already
        have a pharmacy inside (within 200 m).
        """
        opps: List[Opportunity] = []
        min_gla = config.GLA_THRESHOLDS['major_centre']

        for centre in self._shopping_centres:
            gla = centre.get('gla_sqm') or 0
            if gla < min_gla:
                continue

            lat, lon = centre['latitude'], centre['longitude']

            # Check if pharmacy already inside (within 200 m)
            if _has_nearby_pharmacy(lat, lon, self._pharmacies, 0.2):
                continue

            nearest_pharm, dist_km = _nearest_pharmacy(lat, lon, self._pharmacies)

            opp = self._make_opportunity(
                lat, lon, nearest_pharm, dist_km,
                rule='Item 132',
                evidence=(f"Major shopping centre '{centre.get('name','')}' "
                          f"(GLA {gla:,.0f} sqm) with no pharmacy within 200 m"),
                confidence=0.85,
                poi_name=centre.get('name', ''),
                poi_type='shopping_centre',
            )
            opps.append(opp)

        return opps

    # -- Item 133 -- Supermarket >= 1 000 sqm without adjacent pharmacy

    def _scan_item_133(self, region: str) -> List[Opportunity]:
        """
        Item 133 — small shopping centre with supermarket >= 1,000 sqm GLA.

        Supermarkets >= 1,000 sqm GLA that have no pharmacy adjacent (within 100 m).
        GLA is estimated by brand. Small-format stores (IGA Express, IGA Everyday)
        are excluded or flagged with lower confidence.
        """
        opps: List[Opportunity] = []
        min_gla = config.FLOOR_AREA_THRESHOLDS['supermarket']  # 1,000

        for sm in self._supermarkets:
            name = sm.get('name', '')
            gla = sm.get('estimated_gla') or sm.get('floor_area_sqm') or 0
            brand = sm.get('brand', '')
            gla_conf = sm.get('gla_confidence', 'low')

            if gla < min_gla:
                continue

            lat, lon = sm['latitude'], sm['longitude']

            # Already has pharmacy next door? (within 100m = adjacent)
            if _has_nearby_pharmacy(lat, lon, self._pharmacies, 0.1):
                continue

            nearest_pharm, dist_km = _nearest_pharmacy(lat, lon, self._pharmacies)

            # Confidence based on brand and GLA reliability
            is_major = brand in ('woolworths', 'coles')
            if is_major:
                confidence = 0.75
            elif brand in ('aldi', 'drakes', 'harris_farm'):
                confidence = 0.70
            elif brand == 'iga':
                confidence = 0.65
            elif brand == 'foodworks':
                confidence = 0.55  # borderline GLA
            elif brand in ('iga_express', 'iga_everyday'):
                confidence = 0.45  # likely below 1,000 sqm — flagged
            else:
                confidence = 0.55  # unknown brand

            gla_note = ''
            if brand in ('iga_express', 'iga_everyday'):
                gla_note = ' ⚠️ small-format store — actual GLA likely below 1,000 sqm, requires verification'
            elif brand == 'foodworks':
                gla_note = ' ⚠️ Foodworks GLA varies widely (500-1,500 sqm), requires verification'
            elif gla_conf == 'low':
                gla_note = ' ⚠️ GLA estimate is low confidence'

            opp = self._make_opportunity(
                lat, lon, nearest_pharm, dist_km,
                rule='Item 133',
                evidence=(f"Supermarket '{name}' (est. GLA {gla:,.0f} sqm, brand: {brand or 'unknown'}) "
                          f"with no adjacent pharmacy (nearest {format_distance(dist_km)})"
                          f"{gla_note}"),
                confidence=confidence,
                poi_name=name,
                poi_type='supermarket',
            )
            opps.append(opp)

        return opps

    # -- Item 134 -- Shopping centre 5 000-15 000 sqm + supermarket --

    def _scan_item_134(self, region: str) -> List[Opportunity]:
        """
        Shopping centres 5 000-15 000 sqm GLA that have a supermarket
        but no pharmacy inside.
        """
        opps: List[Opportunity] = []
        gla_min = config.GLA_THRESHOLDS['small_centre_min']
        gla_max = config.GLA_THRESHOLDS['small_centre_max']

        for centre in self._shopping_centres:
            gla = centre.get('gla_sqm') or 0
            if gla < gla_min or gla >= gla_max:
                continue

            lat, lon = centre['latitude'], centre['longitude']

            # Must have a supermarket inside
            supermarkets_json = centre.get('major_supermarkets', [])
            if isinstance(supermarkets_json, str):
                import json as _json
                try:
                    supermarkets_json = _json.loads(supermarkets_json)
                except Exception:
                    supermarkets_json = []
            has_sm = bool(supermarkets_json)

            if not has_sm:
                # Check if a supermarket is within 200 m
                nearby_sms = find_within_radius(lat, lon, self._supermarkets, 0.2)
                has_sm = len(nearby_sms) > 0

            if not has_sm:
                continue

            # Already has pharmacy?
            if _has_nearby_pharmacy(lat, lon, self._pharmacies, 0.2):
                continue

            nearest_pharm, dist_km = _nearest_pharmacy(lat, lon, self._pharmacies)

            opp = self._make_opportunity(
                lat, lon, nearest_pharm, dist_km,
                rule='Item 134',
                evidence=(f"Shopping centre '{centre.get('name','')}' "
                          f"(GLA {gla:,.0f} sqm) with supermarket but no pharmacy"),
                confidence=0.75,
                poi_name=centre.get('name', ''),
                poi_type='shopping_centre',
            )
            opps.append(opp)

        return opps

    # -- Item 134A -- Very remote >= 90 km --------------------------

    def _scan_item_134a(self, region: str) -> List[Opportunity]:
        """
        Scan for any populated point >= 90 km straight-line from a pharmacy.
        Use all POIs as proxies for populated places.
        """
        opps: List[Opportunity] = []
        threshold_km = config.RULE_DISTANCES['item_134a']  # 90

        all_pois: List[Dict] = []
        for sm in self._supermarkets:
            all_pois.append({**sm, '_poi_type': 'supermarket'})
        for gp in self._gps:
            all_pois.append({**gp, '_poi_type': 'gp'})
        for h in self._hospitals:
            all_pois.append({**h, '_poi_type': 'hospital'})

        for poi in all_pois:
            lat, lon = poi['latitude'], poi['longitude']
            nearest_pharm, dist_km = _nearest_pharmacy(lat, lon, self._pharmacies)

            if dist_km < threshold_km:
                continue

            opp = self._make_opportunity(
                lat, lon, nearest_pharm, dist_km,
                rule='Item 134A',
                evidence=(f"Very remote -- '{poi.get('name','')}' ({poi['_poi_type']}) "
                          f"is {format_distance(dist_km)} from nearest pharmacy (threshold 90 km)"),
                confidence=0.9,
                poi_name=poi.get('name', ''),
                poi_type=poi['_poi_type'],
            )
            opps.append(opp)

        return opps

    # -- Item 135 -- Large PRIVATE hospital >= 150 beds ---------------

    def _scan_item_135(self, region: str) -> List[Opportunity]:
        """
        Item 135 requires a "large private hospital" with >= 150 beds
        that does NOT have an adjacent pharmacy (within 300 m).

        Must be PRIVATE hospitals, not public.
        Hospitals with unknown type or unknown bed count get lower confidence.
        """
        opps: List[Opportunity] = []
        min_beds = config.HOSPITAL_BED_COUNT  # 150

        # Hospital types that qualify as "private"
        private_types = {'private', 'private_non_profit', 'private_for_profit', 'religious'}
        # Types that are definitely NOT private
        public_types = {'public', 'government'}

        for hosp in self._hospitals:
            beds = hosp.get('bed_count') or 0
            hosp_type = (hosp.get('hospital_type') or 'unknown').lower().strip()

            # Skip hospitals that are definitely public
            if hosp_type in public_types:
                continue

            is_private = hosp_type in private_types
            beds_known = beds > 0

            # Determine if this hospital qualifies
            if beds_known and beds < min_beds:
                continue  # Below threshold, skip

            if not beds_known and not is_private:
                continue  # Unknown beds AND unknown type — too uncertain

            lat, lon = hosp['latitude'], hosp['longitude']

            if _has_nearby_pharmacy(lat, lon, self._pharmacies, 0.3):
                continue

            nearest_pharm, dist_km = _nearest_pharmacy(lat, lon, self._pharmacies)

            # Build evidence and confidence
            notes = []
            if is_private and beds_known and beds >= min_beds:
                confidence = 0.80
                notes.append(f"Private hospital, {beds} beds (>= {min_beds})")
            elif is_private and not beds_known:
                confidence = 0.55
                notes.append(f"Private hospital, bed count unknown — requires verification")
            elif not is_private and beds_known and beds >= min_beds:
                confidence = 0.50
                notes.append(f"Hospital type '{hosp_type}' (may be private), {beds} beds")
            else:
                confidence = 0.40
                notes.append(f"Hospital type '{hosp_type}', bed count unknown — requires verification")

            opp = self._make_opportunity(
                lat, lon, nearest_pharm, dist_km,
                rule='Item 135',
                evidence=(f"Hospital '{hosp.get('name','')}' ({beds} beds, type: {hosp_type}) "
                          f"with no adjacent pharmacy (nearest {format_distance(dist_km)}). "
                          f"{'; '.join(notes)}"),
                confidence=confidence,
                poi_name=hosp.get('name', ''),
                poi_type='hospital',
            )
            opps.append(opp)

        return opps

    # -- Item 136 -- Large medical centre (8+ FTE prescribers) -----

    def _scan_item_136(self, region: str) -> List[Opportunity]:
        """
        Large medical centres with 8+ FTE prescribers and no pharmacy
        within 300m.

        Strategy 1 (primary): Use medical_centres table populated by
        HotDoc/HealthEngine scrapers — these have actual practitioner counts.

        Strategy 2 (fallback): GP cluster analysis from OSM data.
        """
        opps: List[Opportunity] = []
        seen_coords: set = set()

        # --- Strategy 1: medical_centres table (best data) ---
        for mc in self._medical_centres:
            num_gps = mc.get('num_gps', 0) or 0
            fte = mc.get('total_fte', 0) or 0
            # Use headcount as proxy if FTE not set: avg GP ~0.8 FTE
            estimated_fte = fte if fte > 0 else num_gps * 0.8

            if estimated_fte < 8.0 and num_gps < 8:
                continue

            lat, lon = mc['latitude'], mc['longitude']
            grid_key = (round(lat, 3), round(lon, 3))

            # Check no pharmacy within 100m of the centre (i.e., not co-located)
            pharmacy_in_centre = _has_nearby_pharmacy(lat, lon, self._pharmacies, 0.1)
            if pharmacy_in_centre:
                continue

            # Check nearest pharmacy is >= 300m away
            nearest_pharm, dist_km = _nearest_pharmacy(lat, lon, self._pharmacies)
            if nearest_pharm and dist_km < 0.3:
                continue

            # Determine confidence based on data source
            source = mc.get('source', '')
            hours = mc.get('hours_per_week', 0) or 0
            if source == 'manual_research':
                confidence = 0.85
            elif source in ('hotdoc', 'healthengine'):
                confidence = 0.75
            else:
                confidence = 0.65
            
            # Boost confidence if hours data confirms 70+ hrs/week
            if hours >= 70:
                confidence = min(confidence + 0.1, 0.95)
            elif hours > 0 and hours < 70:
                confidence -= 0.15  # Doesn't meet hours requirement

            seen_coords.add(grid_key)
            opp = self._make_opportunity(
                lat, lon, nearest_pharm, dist_km,
                rule='Item 136',
                evidence=(f"Medical centre '{mc.get('name','')}' — "
                          f"{num_gps} GPs ({estimated_fte:.1f} est. FTE)"
                          f"{f', open {hours:.0f}hrs/wk' if hours > 0 else ''}"
                          f" — nearest pharmacy {format_distance(dist_km)}"
                          f" [source: {source}]"),
                confidence=confidence,
                poi_name=mc.get('name', ''),
                poi_type='medical_centre',
            )
            opps.append(opp)

        # --- Strategy 2: GP cluster fallback (OSM data) ---
        for gp in self._gps:
            lat, lon = gp['latitude'], gp['longitude']

            grid_key = (round(lat, 3), round(lon, 3))
            if grid_key in seen_coords:
                continue
            seen_coords.add(grid_key)

            nearby_gps = find_within_radius(lat, lon, self._gps, 0.2)
            total_fte = sum((g[0].get('fte') or 0) for g in nearby_gps)

            if total_fte < 8.0:
                continue

            if _has_nearby_pharmacy(lat, lon, self._pharmacies, 0.3):
                continue

            nearest_pharm, dist_km = _nearest_pharmacy(lat, lon, self._pharmacies)

            gp_names = [g[0].get('name', '?') for g in nearby_gps[:5]]
            opp = self._make_opportunity(
                lat, lon, nearest_pharm, dist_km,
                rule='Item 136',
                evidence=(f"GP cluster ({total_fte:.1f} FTE across {len(nearby_gps)} practices "
                          f"within 200 m): {', '.join(gp_names)}; "
                          f"no pharmacy within 300 m — requires manual verification"),
                confidence=0.5,  # Lower confidence for cluster proxy
                poi_name=gp_names[0] if gp_names else '',
                poi_type='medical_centre',
            )
            opps.append(opp)

        return opps

    # -- helpers ---------------------------------------------------

    def _make_opportunity(self, lat, lon, nearest_pharm, dist_km,
                          rule, evidence, confidence,
                          poi_name='', poi_type='') -> Opportunity:
        return Opportunity(
            latitude=lat,
            longitude=lon,
            qualifying_rules=[rule],
            evidence=[evidence],
            confidence=confidence,
            nearest_pharmacy_km=dist_km if dist_km != float('inf') else -1,
            nearest_pharmacy_name=(nearest_pharm.get('name', '') if nearest_pharm else ''),
            poi_name=poi_name,
            poi_type=poi_type,
        )

    @staticmethod
    def _merge_nearby(opps: List[Opportunity], radius_m: float = 200) -> List[Opportunity]:
        """
        Merge opportunities whose centres are within *radius_m* metres.
        When merging, combine qualifying_rules and evidence, keep the
        highest confidence, and average the coordinates.
        """
        if not opps:
            return []

        radius_km = radius_m / 1000.0
        merged: List[Opportunity] = []
        used = [False] * len(opps)

        for i, opp in enumerate(opps):
            if used[i]:
                continue
            group = [opp]
            used[i] = True

            for j in range(i + 1, len(opps)):
                if used[j]:
                    continue
                d = haversine_distance(opp.latitude, opp.longitude,
                                       opps[j].latitude, opps[j].longitude)
                if d <= radius_km:
                    group.append(opps[j])
                    used[j] = True

            # Merge group into a single Opportunity
            if len(group) == 1:
                merged.append(group[0])
            else:
                avg_lat = sum(g.latitude for g in group) / len(group)
                avg_lon = sum(g.longitude for g in group) / len(group)
                all_rules: List[str] = []
                all_evidence: List[str] = []
                for g in group:
                    for r in g.qualifying_rules:
                        if r not in all_rules:
                            all_rules.append(r)
                    all_evidence.extend(g.evidence)

                best = max(group, key=lambda g: g.confidence)
                merged_opp = Opportunity(
                    latitude=avg_lat,
                    longitude=avg_lon,
                    qualifying_rules=all_rules,
                    evidence=all_evidence,
                    confidence=best.confidence,
                    nearest_pharmacy_km=best.nearest_pharmacy_km,
                    nearest_pharmacy_name=best.nearest_pharmacy_name,
                    poi_name=best.poi_name,
                    poi_type=best.poi_type,
                )
                merged.append(merged_opp)

        # Sort by confidence descending
        merged.sort(key=lambda o: o.confidence, reverse=True)
        return merged
