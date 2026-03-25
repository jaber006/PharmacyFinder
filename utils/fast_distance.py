"""
High-performance distance calculations for PharmacyFinder.

Uses:
- Numba JIT parallel CPU (12 threads on Ryzen 5 5600X)
- scipy cKDTree for O(log n) nearest-neighbor queries
- NumPy vectorization as fallback
- Multiprocessing for parallel scanner execution

Benchmarks (6,500 × 3,600 = 23.4M distances):
- Numba parallel: ~1-2s
- NumPy vectorized: ~3-5s
- geopy.geodesic: ~20 minutes (single-threaded, 50μs/call)

Usage:
    from utils.fast_distance import (
        haversine_km,                # single pair
        haversine_matrix,            # all pairs between two sets
        nearest_from_set,            # nearest in set B for each in set A
        within_radius_batch,         # all within radius for each point
        build_kdtree, query_kdtree,  # O(log n) spatial index
        parallel_map,                # multiprocessing wrapper
    )
"""
import math
import numpy as np
from typing import List, Tuple, Optional, Callable
from multiprocessing import Pool, cpu_count

R = 6371.0  # Earth radius km

# ============================================================
# Numba JIT parallel (12 CPU threads)
# ============================================================

_numba_available = False
try:
    from numba import njit, prange

    @njit(parallel=True, cache=True)
    def _haversine_matrix_numba(lats1, lons1, lats2, lons2):
        """Numba JIT: parallel haversine matrix across all CPU cores."""
        n = lats1.shape[0]
        m = lats2.shape[0]
        out = np.empty((n, m), dtype=np.float64)
        for i in prange(n):
            lat1 = lats1[i] * 0.017453292519943295  # radians
            lon1 = lons1[i] * 0.017453292519943295
            cos_lat1 = math.cos(lat1)
            for j in range(m):
                lat2 = lats2[j] * 0.017453292519943295
                lon2 = lons2[j] * 0.017453292519943295
                dlat = lat2 - lat1
                dlon = lon2 - lon1
                a = math.sin(dlat * 0.5) ** 2 + cos_lat1 * math.cos(lat2) * math.sin(dlon * 0.5) ** 2
                out[i, j] = 6371.0 * 2.0 * math.asin(math.sqrt(a))
        return out

    @njit(parallel=True, cache=True)
    def _nearest_indices_numba(lats1, lons1, lats2, lons2):
        """Numba JIT: find nearest point in set 2 for each in set 1."""
        n = lats1.shape[0]
        m = lats2.shape[0]
        indices = np.empty(n, dtype=np.int64)
        distances = np.empty(n, dtype=np.float64)
        for i in prange(n):
            lat1 = lats1[i] * 0.017453292519943295
            lon1 = lons1[i] * 0.017453292519943295
            cos_lat1 = math.cos(lat1)
            best_d = 1e18
            best_j = 0
            for j in range(m):
                lat2 = lats2[j] * 0.017453292519943295
                lon2 = lons2[j] * 0.017453292519943295
                dlat = lat2 - lat1
                dlon = lon2 - lon1
                a = math.sin(dlat * 0.5) ** 2 + cos_lat1 * math.cos(lat2) * math.sin(dlon * 0.5) ** 2
                d = 6371.0 * 2.0 * math.asin(math.sqrt(a))
                if d < best_d:
                    best_d = d
                    best_j = j
            indices[i] = best_j
            distances[i] = best_d
        return indices, distances

    _numba_available = True
    print("[fast_distance] Numba parallel CPU ready (12 threads)", flush=True)
except ImportError:
    print("[fast_distance] Numba not available, using NumPy", flush=True)


# ============================================================
# scipy cKDTree for O(log n) queries
# ============================================================

_scipy_available = False
try:
    from scipy.spatial import cKDTree
    _scipy_available = True
except ImportError:
    pass


def build_kdtree(lats: np.ndarray, lons: np.ndarray) -> 'cKDTree':
    """Build a cKDTree from lat/lon arrays (converts to radians for ball_tree)."""
    if not _scipy_available:
        raise ImportError("scipy required for cKDTree")
    # Convert to Cartesian for proper distance
    lat_rad = np.radians(lats)
    lon_rad = np.radians(lons)
    x = R * np.cos(lat_rad) * np.cos(lon_rad)
    y = R * np.cos(lat_rad) * np.sin(lon_rad)
    z = R * np.sin(lat_rad)
    return cKDTree(np.column_stack([x, y, z]))


def query_kdtree(tree: 'cKDTree', lats: np.ndarray, lons: np.ndarray, k: int = 1):
    """Query nearest k points. Returns (distances_km, indices)."""
    lat_rad = np.radians(lats)
    lon_rad = np.radians(lons)
    x = R * np.cos(lat_rad) * np.cos(lon_rad)
    y = R * np.cos(lat_rad) * np.sin(lon_rad)
    z = R * np.sin(lat_rad)
    points = np.column_stack([x, y, z])
    dists, indices = tree.query(points, k=k)
    return dists, indices


def query_radius(tree: 'cKDTree', lat: float, lon: float, radius_km: float):
    """Find all points within radius_km. Returns list of indices."""
    lat_rad = math.radians(lat)
    lon_rad = math.radians(lon)
    x = R * math.cos(lat_rad) * math.cos(lon_rad)
    y = R * math.cos(lat_rad) * math.sin(lon_rad)
    z = R * math.sin(lat_rad)
    # Euclidean distance approximation for small angles
    return tree.query_ball_point([x, y, z], r=radius_km)


# ============================================================
# Core functions
# ============================================================

def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Single-pair haversine distance in km."""
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return R * 2 * math.asin(math.sqrt(a))


def haversine_matrix(
    lats1: np.ndarray, lons1: np.ndarray,
    lats2: np.ndarray, lons2: np.ndarray,
) -> np.ndarray:
    """All-pairs haversine. Returns (N, M) distance matrix in km."""
    lats1 = np.asarray(lats1, dtype=np.float64)
    lons1 = np.asarray(lons1, dtype=np.float64)
    lats2 = np.asarray(lats2, dtype=np.float64)
    lons2 = np.asarray(lons2, dtype=np.float64)

    if _numba_available:
        return _haversine_matrix_numba(lats1, lons1, lats2, lons2)

    # NumPy vectorized fallback
    lat1 = np.radians(lats1[:, None])
    lon1 = np.radians(lons1[:, None])
    lat2 = np.radians(lats2[None, :])
    lon2 = np.radians(lons2[None, :])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    return R * 2 * np.arcsin(np.sqrt(a))


def nearest_from_set(
    query_lats: np.ndarray, query_lons: np.ndarray,
    ref_lats: np.ndarray, ref_lons: np.ndarray,
) -> Tuple[np.ndarray, np.ndarray]:
    """For each query point, find nearest ref point. Returns (indices, distances_km)."""
    query_lats = np.asarray(query_lats, dtype=np.float64)
    query_lons = np.asarray(query_lons, dtype=np.float64)
    ref_lats = np.asarray(ref_lats, dtype=np.float64)
    ref_lons = np.asarray(ref_lons, dtype=np.float64)

    if _numba_available:
        return _nearest_indices_numba(query_lats, query_lons, ref_lats, ref_lons)

    dist_matrix = haversine_matrix(query_lats, query_lons, ref_lats, ref_lons)
    indices = np.argmin(dist_matrix, axis=1)
    distances = dist_matrix[np.arange(len(query_lats)), indices]
    return indices, distances


def within_radius_batch(
    query_lats: np.ndarray, query_lons: np.ndarray,
    ref_lats: np.ndarray, ref_lons: np.ndarray,
    radius_km: float,
) -> List[List[Tuple[int, float]]]:
    """For each query, find all refs within radius. Returns list of (index, dist) lists."""
    dist_matrix = haversine_matrix(query_lats, query_lons, ref_lats, ref_lons)
    results = []
    for i in range(len(query_lats)):
        row = dist_matrix[i]
        mask = row <= radius_km
        nearby = [(int(j), float(row[j])) for j in np.where(mask)[0]]
        nearby.sort(key=lambda x: x[1])
        results.append(nearby)
    return results


# ============================================================
# Multiprocessing
# ============================================================

def parallel_map(func: Callable, items: list, num_workers: int = None, chunk_size: int = 50) -> list:
    """Map function over items using all CPU cores."""
    if num_workers is None:
        num_workers = min(cpu_count(), 12)
    if len(items) <= chunk_size:
        return [func(item) for item in items]
    with Pool(num_workers) as pool:
        return pool.map(func, items, chunksize=chunk_size)


# ============================================================
# Benchmark
# ============================================================

if __name__ == "__main__":
    import time

    np.random.seed(42)
    n_pharm = 6500
    n_super = 3600

    pharm_lats = np.random.uniform(-44, -10, n_pharm)
    pharm_lons = np.random.uniform(113, 154, n_pharm)
    super_lats = np.random.uniform(-44, -10, n_super)
    super_lons = np.random.uniform(113, 154, n_super)

    total_pairs = n_pharm * n_super
    print(f"Benchmark: {n_pharm:,} pharmacies × {n_super:,} supermarkets = {total_pairs:,} distances")
    print(f"CPU: {cpu_count()} threads")
    print()

    # Warmup Numba JIT
    if _numba_available:
        _ = haversine_matrix(pharm_lats[:5], pharm_lons[:5], super_lats[:5], super_lons[:5])
        print("Numba JIT warmed up")

    # Full matrix
    t = time.perf_counter()
    dist = haversine_matrix(pharm_lats, pharm_lons, super_lats, super_lons)
    dt = time.perf_counter() - t
    rate = total_pairs / dt
    print(f"Full matrix:     {dt:.3f}s | {rate/1e6:.1f}M pairs/sec | shape={dist.shape}")

    # Nearest pharmacy for each supermarket
    t = time.perf_counter()
    idx, dists = nearest_from_set(super_lats, super_lons, pharm_lats, pharm_lons)
    dt = time.perf_counter() - t
    print(f"Nearest lookup:  {dt:.3f}s for {n_super:,} queries")
    print(f"  >1.5km from pharmacy: {(dists > 1.5).sum()}")
    print(f"  >10km from pharmacy:  {(dists > 10).sum()}")

    # cKDTree benchmark
    if _scipy_available:
        t = time.perf_counter()
        tree = build_kdtree(pharm_lats, pharm_lons)
        kd_dists, kd_idx = query_kdtree(tree, super_lats, super_lons)
        dt = time.perf_counter() - t
        print(f"cKDTree nearest: {dt:.3f}s (O(log n) per query)")

    print(f"\nMemory: {dist.nbytes / 1024 / 1024:.1f}MB for distance matrix")
    print("Ready for PharmacyFinder scanners.")
