"""
EvaluationContext: loads all reference data from SQLite and provides
spatial query methods for rule evaluation.

Uses geopy.distance.geodesic for all straight-line measurements.
Spatial grid index for fast nearest-neighbor filtering.
"""
import sqlite3
import json
import math
import os
import time
from collections import defaultdict
from typing import List, Dict, Optional, Tuple
from geopy.distance import geodesic

# OSRM route cache to avoid hammering the public server
_osrm_cache: Dict[str, Optional[float]] = {}
_osrm_last_call: float = 0.0

# Grid cell size in degrees (~11km at equator, ~8km at -35 latitude)
GRID_CELL_DEG = 0.1


def _grid_key(lat: float, lon: float) -> Tuple[int, int]:
    """Convert lat/lon to grid cell key."""
    return (int(lat / GRID_CELL_DEG), int(lon / GRID_CELL_DEG))


def _nearby_keys(lat: float, lon: float, radius_km: float) -> List[Tuple[int, int]]:
    """Get grid cell keys that could contain points within radius_km."""
    # Rough: 1 degree lat ≈ 111km, 1 degree lon ≈ 111km * cos(lat)
    lat_cells = max(1, int(radius_km / (GRID_CELL_DEG * 111)) + 1)
    lon_factor = max(0.5, math.cos(math.radians(lat)))
    lon_cells = max(1, int(radius_km / (GRID_CELL_DEG * 111 * lon_factor)) + 1)
    
    base_lat, base_lon = _grid_key(lat, lon)
    keys = []
    for dlat in range(-lat_cells, lat_cells + 1):
        for dlon in range(-lon_cells, lon_cells + 1):
            keys.append((base_lat + dlat, base_lon + dlon))
    return keys


class _SpatialIndex:
    """Simple grid-based spatial index for fast radius queries."""
    
    def __init__(self, items: List[Dict]):
        self.grid: Dict[Tuple[int, int], List[Dict]] = defaultdict(list)
        for item in items:
            key = _grid_key(item['latitude'], item['longitude'])
            self.grid[key].append(item)
    
    def candidates_near(self, lat: float, lon: float, radius_km: float) -> List[Dict]:
        """Get items that MIGHT be within radius_km (rough filter)."""
        result = []
        for key in _nearby_keys(lat, lon, radius_km):
            result.extend(self.grid.get(key, []))
        return result


class EvaluationContext:
    """Holds all reference data and provides spatial queries."""

    def __init__(self, db_path: str = None, state_filter: str = None):
        if db_path is None:
            db_path = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                "pharmacy_finder.db"
            )
        self.db_path = db_path
        self.state_filter = state_filter
        
        # Data stores
        self.pharmacies: List[Dict] = []
        self.gps: List[Dict] = []
        self.supermarkets: List[Dict] = []
        self.hospitals: List[Dict] = []
        self.shopping_centres: List[Dict] = []
        self.medical_centres: List[Dict] = []
        
        # Spatial indexes
        self._pharm_idx: Optional[_SpatialIndex] = None
        self._gp_idx: Optional[_SpatialIndex] = None
        self._super_idx: Optional[_SpatialIndex] = None
        self._hosp_idx: Optional[_SpatialIndex] = None
        self._sc_idx: Optional[_SpatialIndex] = None
        self._mc_idx: Optional[_SpatialIndex] = None
        
        self._load_data()

    def _load_data(self):
        """Load all reference data from SQLite."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row

        # Pharmacies — always load ALL (we need national coverage for distance checks)
        cur = conn.cursor()
        cur.execute("SELECT * FROM pharmacies WHERE latitude IS NOT NULL AND longitude IS NOT NULL")
        self.pharmacies = [dict(r) for r in cur.fetchall()]

        # GPs
        cur.execute("SELECT * FROM gps WHERE latitude IS NOT NULL AND longitude IS NOT NULL")
        self.gps = [dict(r) for r in cur.fetchall()]

        # Supermarkets
        cur.execute("SELECT * FROM supermarkets WHERE latitude IS NOT NULL AND longitude IS NOT NULL")
        self.supermarkets = [dict(r) for r in cur.fetchall()]

        # Hospitals
        cur.execute("SELECT * FROM hospitals WHERE latitude IS NOT NULL AND longitude IS NOT NULL")
        self.hospitals = [dict(r) for r in cur.fetchall()]

        # Shopping centres
        cur.execute("SELECT * FROM shopping_centres WHERE latitude IS NOT NULL AND longitude IS NOT NULL")
        raw_centres = [dict(r) for r in cur.fetchall()]
        for c in raw_centres:
            try:
                c['major_supermarkets'] = json.loads(c.get('major_supermarkets', '[]') or '[]')
            except (json.JSONDecodeError, TypeError):
                c['major_supermarkets'] = []
        self.shopping_centres = raw_centres

        # Medical centres
        cur.execute("SELECT * FROM medical_centres WHERE latitude IS NOT NULL AND longitude IS NOT NULL")
        self.medical_centres = [dict(r) for r in cur.fetchall()]

        conn.close()

        # Build spatial indexes
        self._pharm_idx = _SpatialIndex(self.pharmacies)
        self._gp_idx = _SpatialIndex(self.gps)
        self._super_idx = _SpatialIndex(self.supermarkets)
        self._hosp_idx = _SpatialIndex(self.hospitals)
        self._sc_idx = _SpatialIndex(self.shopping_centres)
        self._mc_idx = _SpatialIndex(self.medical_centres)

        print(f"[Context] Loaded: {len(self.pharmacies)} pharmacies, "
              f"{len(self.gps)} GPs, {len(self.supermarkets)} supermarkets, "
              f"{len(self.hospitals)} hospitals, {len(self.shopping_centres)} shopping centres, "
              f"{len(self.medical_centres)} medical centres")

    # --- Spatial queries using geodesic ---

    @staticmethod
    def geodesic_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Geodesic distance in km between two points."""
        return geodesic((lat1, lon1), (lat2, lon2)).kilometers

    def _nearest_from_index(self, idx: _SpatialIndex, lat: float, lon: float,
                            max_search_km: float = 50.0) -> Tuple[Optional[Dict], float]:
        """Find nearest item using spatial index, expanding search radius."""
        for radius in (2, 5, 15, max_search_km, 200):
            candidates = idx.candidates_near(lat, lon, radius)
            if candidates:
                best = None
                best_dist = float('inf')
                for p in candidates:
                    d = self.geodesic_km(lat, lon, p['latitude'], p['longitude'])
                    if d < best_dist:
                        best_dist = d
                        best = p
                if best is not None:
                    return best, best_dist
        return None, float('inf')

    def _within_radius_from_index(self, idx: _SpatialIndex, lat: float, lon: float,
                                   radius_km: float) -> List[Tuple[Dict, float]]:
        """Find all items within radius using spatial index."""
        candidates = idx.candidates_near(lat, lon, radius_km)
        results = []
        for p in candidates:
            d = self.geodesic_km(lat, lon, p['latitude'], p['longitude'])
            if d <= radius_km:
                results.append((p, d))
        results.sort(key=lambda x: x[1])
        return results

    def nearest_pharmacy(self, lat: float, lon: float) -> Tuple[Optional[Dict], float]:
        """Find nearest pharmacy and its geodesic distance in km."""
        return self._nearest_from_index(self._pharm_idx, lat, lon, 200)

    def pharmacies_within_radius(self, lat: float, lon: float, radius_km: float) -> List[Tuple[Dict, float]]:
        """All pharmacies within radius_km, sorted by distance."""
        return self._within_radius_from_index(self._pharm_idx, lat, lon, radius_km)

    def nearest_gp(self, lat: float, lon: float) -> Tuple[Optional[Dict], float]:
        """Find nearest GP practice."""
        return self._nearest_from_index(self._gp_idx, lat, lon)

    def gps_within_radius(self, lat: float, lon: float, radius_km: float) -> List[Tuple[Dict, float]]:
        """All GPs within radius_km."""
        return self._within_radius_from_index(self._gp_idx, lat, lon, radius_km)

    def supermarkets_within_radius(self, lat: float, lon: float, radius_km: float) -> List[Tuple[Dict, float]]:
        """All supermarkets within radius_km."""
        return self._within_radius_from_index(self._super_idx, lat, lon, radius_km)

    def nearest_supermarket(self, lat: float, lon: float) -> Tuple[Optional[Dict], float]:
        """Find nearest supermarket."""
        return self._nearest_from_index(self._super_idx, lat, lon)

    def hospitals_within_radius(self, lat: float, lon: float, radius_km: float) -> List[Tuple[Dict, float]]:
        """All hospitals within radius_km."""
        return self._within_radius_from_index(self._hosp_idx, lat, lon, radius_km)

    def shopping_centres_within_radius(self, lat: float, lon: float, radius_km: float) -> List[Tuple[Dict, float]]:
        """All shopping centres within radius_km."""
        return self._within_radius_from_index(self._sc_idx, lat, lon, radius_km)

    def medical_centres_within_radius(self, lat: float, lon: float, radius_km: float) -> List[Tuple[Dict, float]]:
        """All medical centres within radius_km."""
        return self._within_radius_from_index(self._mc_idx, lat, lon, radius_km)

    def get_driving_distance_cached(self, lat1: float, lon1: float, lat2: float, lon2: float) -> Optional[float]:
        """
        Get OSRM driving distance with caching and rate limiting (1 req/sec).
        Returns distance in km or None.
        """
        global _osrm_last_call
        
        # Cache key (round to 5 decimal places for stability)
        key = f"{lat1:.5f},{lon1:.5f}->{lat2:.5f},{lon2:.5f}"
        if key in _osrm_cache:
            return _osrm_cache[key]

        # Rate limit: 1 request per second
        now = time.time()
        elapsed = now - _osrm_last_call
        if elapsed < 1.0:
            time.sleep(1.0 - elapsed)

        from utils.distance import get_driving_distance
        result = get_driving_distance(lat1, lon1, lat2, lon2)
        _osrm_last_call = time.time()
        _osrm_cache[key] = result
        return result

    def estimate_driving_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """
        Estimate driving distance as ~1.4x geodesic (no API call).
        Used for quick pre-filtering before hitting OSRM.
        """
        straight = self.geodesic_km(lat1, lon1, lat2, lon2)
        return straight * 1.4  # Road distance typically ~1.3-1.5x straight line
