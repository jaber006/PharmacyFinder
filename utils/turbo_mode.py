"""
turbo_mode.py - Weaponise ALL hardware for PharmacyFinder
GPU (RTX 2060 6GB) + CPU (Ryzen 5600X 12t) + RAM (16GB)
"""
import sqlite3
import numpy as np
import os
import sys
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "pharmacy_finder.db"

# ============================================================
# 1. IN-MEMORY DATABASE (918MB DB → pure RAM queries)
# ============================================================
_mem_db = None

def get_memory_db():
    """Load entire SQLite DB into RAM. ~10-100x faster queries."""
    global _mem_db
    if _mem_db is not None:
        return _mem_db
    
    print("[turbo] Loading 918MB database into RAM...", end=" ", flush=True)
    t = time.perf_counter()
    
    # Open disk DB and copy to memory
    disk_db = sqlite3.connect(str(DB_PATH))
    _mem_db = sqlite3.connect(":memory:")
    disk_db.backup(_mem_db)
    disk_db.close()
    
    dt = time.perf_counter() - t
    print(f"done in {dt:.1f}s")
    
    # Enable WAL mode and optimizations
    _mem_db.execute("PRAGMA journal_mode=OFF")
    _mem_db.execute("PRAGMA synchronous=OFF")
    _mem_db.execute("PRAGMA cache_size=-2000000")  # 2GB cache
    _mem_db.execute("PRAGMA temp_store=MEMORY")
    
    return _mem_db


def query_memory(sql, params=None):
    """Execute query against in-memory DB."""
    db = get_memory_db()
    cur = db.execute(sql, params or [])
    return cur.fetchall()


# ============================================================
# 2. PRE-LOADED COORDINATE ARRAYS (instant numpy access)
# ============================================================
_coord_cache = {}

def get_coordinates(table, lat_col="latitude", lon_col="longitude", where=None):
    """Load coordinates as numpy arrays (cached)."""
    cache_key = f"{table}:{where}"
    if cache_key in _coord_cache:
        return _coord_cache[cache_key]
    
    db = get_memory_db()
    sql = f"SELECT {lat_col}, {lon_col} FROM {table}"
    if where:
        sql += f" WHERE {where}"
    
    rows = db.execute(sql).fetchall()
    lats = np.array([r[0] for r in rows], dtype=np.float64)
    lons = np.array([r[1] for r in rows], dtype=np.float64)
    
    _coord_cache[cache_key] = (lats, lons)
    return lats, lons


# ============================================================
# 3. PARALLEL OSRM (12 concurrent road distance requests)
# ============================================================
def parallel_osrm_distances(origins, destinations, max_workers=12):
    """
    Compute road distances in parallel using OSRM.
    origins: list of (lat, lon)
    destinations: list of (lat, lon) 
    Returns: list of distances in km
    """
    import urllib.request
    
    OSRM_BASE = "http://localhost:5000"
    
    def get_road_distance(origin, destination):
        """Single OSRM route request."""
        try:
            url = (f"{OSRM_BASE}/route/v1/driving/"
                   f"{origin[1]},{origin[0]};{destination[1]},{destination[0]}"
                   f"?overview=false")
            with urllib.request.urlopen(url, timeout=5) as resp:
                data = json.loads(resp.read())
                if data.get("code") == "Ok" and data.get("routes"):
                    return data["routes"][0]["distance"] / 1000  # meters to km
        except Exception:
            pass
        return None
    
    results = [None] * len(origins)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for i, (orig, dest) in enumerate(zip(origins, destinations)):
            future = executor.submit(get_road_distance, orig, dest)
            futures[future] = i
        
        for future in as_completed(futures):
            idx = futures[future]
            results[idx] = future.result()
    
    return results


# ============================================================
# 4. GPU BULK CANDIDATE SCREENING
# ============================================================
def gpu_screen_candidates(candidate_lats, candidate_lons, 
                          pharmacy_lats, pharmacy_lons,
                          min_distance_km=1.5):
    """
    Screen ALL candidates against ALL pharmacies on GPU.
    Returns mask of candidates that are >= min_distance from nearest pharmacy.
    """
    try:
        from numba import cuda
        import math
        
        @cuda.jit
        def min_distance_kernel(c_lats, c_lons, p_lats, p_lons, min_dists):
            """Find minimum distance from each candidate to any pharmacy."""
            i = cuda.grid(1)
            if i >= c_lats.shape[0]:
                return
            
            lat1 = c_lats[i] * 0.017453292519943295
            lon1 = c_lons[i] * 0.017453292519943295
            cos_lat1 = math.cos(lat1)
            
            min_d = 1e10
            for j in range(p_lats.shape[0]):
                lat2 = p_lats[j] * 0.017453292519943295
                lon2 = p_lons[j] * 0.017453292519943295
                dlat = lat2 - lat1
                dlon = lon2 - lon1
                a = math.sin(dlat * 0.5)**2 + cos_lat1 * math.cos(lat2) * math.sin(dlon * 0.5)**2
                d = 6371.0 * 2.0 * math.asin(math.sqrt(a))
                if d < min_d:
                    min_d = d
            
            min_dists[i] = min_d
        
        n = len(candidate_lats)
        d_c_lats = cuda.to_device(candidate_lats)
        d_c_lons = cuda.to_device(candidate_lons)
        d_p_lats = cuda.to_device(pharmacy_lats)
        d_p_lons = cuda.to_device(pharmacy_lons)
        d_min_dists = cuda.device_array(n, dtype=np.float64)
        
        threads = 256
        blocks = (n + threads - 1) // threads
        
        min_distance_kernel[blocks, threads](d_c_lats, d_c_lons, d_p_lats, d_p_lons, d_min_dists)
        cuda.synchronize()
        
        min_dists = d_min_dists.copy_to_host()
        return min_dists, min_dists >= min_distance_km
        
    except Exception as e:
        print(f"[turbo] GPU screening failed ({e}), falling back to CPU")
        return cpu_screen_candidates(candidate_lats, candidate_lons,
                                     pharmacy_lats, pharmacy_lons, min_distance_km)


def cpu_screen_candidates(candidate_lats, candidate_lons,
                          pharmacy_lats, pharmacy_lons, min_distance_km=1.5):
    """CPU fallback using scipy KDTree."""
    from scipy.spatial import cKDTree
    
    # Convert to radians for ball_tree
    pharm_rad = np.deg2rad(np.column_stack([pharmacy_lats, pharmacy_lons]))
    cand_rad = np.deg2rad(np.column_stack([candidate_lats, candidate_lons]))
    
    tree = cKDTree(pharm_rad)
    dists, _ = tree.query(cand_rad, k=1)
    dists_km = dists * 6371.0  # approximate
    
    return dists_km, dists_km >= min_distance_km


# ============================================================
# 5. BENCHMARK
# ============================================================
def benchmark():
    """Full hardware benchmark."""
    print("=" * 60)
    print("TURBO MODE - Hardware Benchmark")
    print("=" * 60)
    
    # 1. Memory DB
    t = time.perf_counter()
    db = get_memory_db()
    
    # Count records
    pharm_count = db.execute("SELECT COUNT(*) FROM pharmacies").fetchone()[0]
    poi_count = (
        db.execute("SELECT COUNT(*) FROM supermarkets").fetchone()[0] +
        db.execute("SELECT COUNT(*) FROM hospitals").fetchone()[0] +
        db.execute("SELECT COUNT(*) FROM shopping_centres").fetchone()[0] +
        db.execute("SELECT COUNT(*) FROM medical_centres").fetchone()[0] +
        db.execute("SELECT COUNT(*) FROM gps").fetchone()[0]
    )
    census_count = db.execute(
        "SELECT COUNT(*) FROM census_sa1"
    ).fetchone()[0] if db.execute(
        "SELECT name FROM sqlite_master WHERE name='census_sa1'"
    ).fetchone() else 0
    
    print(f"\nIn-Memory DB: {pharm_count:,} pharmacies, {poi_count:,} POIs, {census_count:,} census areas")
    
    # 2. Query speed test
    t = time.perf_counter()
    for _ in range(1000):
        db.execute("SELECT * FROM pharmacies WHERE state = 'NSW' LIMIT 10").fetchall()
    dt = time.perf_counter() - t
    print(f"Query speed: 1,000 queries in {dt:.3f}s ({1000/dt:.0f} queries/sec)")
    
    # 3. Load pharmacy coordinates
    t = time.perf_counter()
    rows = db.execute("SELECT latitude, longitude FROM pharmacies").fetchall()
    p_lats = np.array([r[0] for r in rows], dtype=np.float64)
    p_lons = np.array([r[1] for r in rows], dtype=np.float64)
    print(f"Loaded {len(p_lats):,} pharmacy coordinates in {time.perf_counter()-t:.3f}s")
    
    # 4. GPU screening test
    n_candidates = 72737  # full OSM extract size
    c_lats = np.random.uniform(-44, -10, n_candidates).astype(np.float64)
    c_lons = np.random.uniform(113, 154, n_candidates).astype(np.float64)
    
    t = time.perf_counter()
    min_dists, mask = gpu_screen_candidates(c_lats, c_lons, p_lats, p_lons, 1.5)
    dt = time.perf_counter() - t
    passing = mask.sum()
    print(f"\nGPU screening: {n_candidates:,} candidates × {len(p_lats):,} pharmacies")
    print(f"  Time: {dt:.3f}s | {n_candidates*len(p_lats)/dt/1e6:.0f}M distances/sec")
    print(f"  Passing (>1.5km): {passing:,} ({100*passing/n_candidates:.1f}%)")
    
    # 5. Parallel OSRM test (only if OSRM running)
    try:
        import urllib.request
        urllib.request.urlopen("http://localhost:5000/health", timeout=2)
        osrm_available = True
    except Exception:
        osrm_available = False
    
    if osrm_available:
        test_origins = [(r[0], r[1]) for r in zip(c_lats[:24], c_lons[:24])]
        test_dests = [(r[0], r[1]) for r in zip(p_lats[:24], p_lons[:24])]
        t = time.perf_counter()
        results = parallel_osrm_distances(test_origins, test_dests, max_workers=12)
        dt = time.perf_counter() - t
        success = sum(1 for r in results if r is not None)
        print(f"\nParallel OSRM: 24 routes in {dt:.3f}s ({success}/24 successful)")
    else:
        print(f"\nOSRM: offline (Docker stopped - will use haversine pre-filter + public fallback)")
    
    print(f"\n{'=' * 60}")
    print("TURBO MODE READY")
    print(f"  GPU: RTX 2060 (6GB VRAM)")
    print(f"  CPU: Ryzen 5600X (12 threads)")
    print(f"  RAM: In-memory DB ({pharm_count + poi_count + census_count:,} records)")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    benchmark()
