"""
Local OSRM routing client with fallback to public server.

Connects to a self-hosted OSRM instance (localhost:5000) for fast
Australian road distance calculations. Falls back to the public
OSRM server if the local instance is unavailable.

Includes a caching layer to avoid redundant queries.
"""

import hashlib
import json
import time
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

LOCAL_OSRM = "http://localhost:5000"
PUBLIC_OSRM = "http://router.project-osrm.org"

CACHE_DIR = Path(__file__).resolve().parent.parent / "cache" / "osrm_cache"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# How long cached results stay valid (seconds).  Road networks rarely change.
CACHE_TTL = 60 * 60 * 24 * 30  # 30 days

# Rate-limit for public server (seconds between requests)
PUBLIC_RATE_LIMIT = 1.0
_last_public_call: float = 0.0

# ---------------------------------------------------------------------------
# Server detection
# ---------------------------------------------------------------------------

_server_url: Optional[str] = None
_server_checked_at: float = 0.0
_SERVER_CHECK_INTERVAL = 60  # re-check every 60 seconds


def _check_local_server() -> bool:
    """Return True if the local OSRM server responds."""
    try:
        r = requests.get(
            f"{LOCAL_OSRM}/nearest/v1/driving/151.2093,-33.8688",
            timeout=2,
        )
        return r.status_code == 200 and r.json().get("code") == "Ok"
    except Exception:
        return False


def get_server() -> str:
    """Return the best available OSRM server URL (local preferred)."""
    global _server_url, _server_checked_at

    now = time.time()
    if _server_url and (now - _server_checked_at) < _SERVER_CHECK_INTERVAL:
        return _server_url

    if _check_local_server():
        _server_url = LOCAL_OSRM
        logger.info("Using LOCAL OSRM server")
    else:
        _server_url = PUBLIC_OSRM
        logger.info("Local OSRM unavailable — falling back to public server")

    _server_checked_at = now
    return _server_url


def is_local() -> bool:
    """Return True if currently using the local OSRM instance."""
    return get_server() == LOCAL_OSRM


# ---------------------------------------------------------------------------
# Caching helpers
# ---------------------------------------------------------------------------


def _cache_key(*args) -> str:
    """Create a deterministic cache key from arguments."""
    raw = json.dumps(args, sort_keys=True)
    return hashlib.sha256(raw.encode()).hexdigest()


def _cache_get(key: str) -> Optional[dict]:
    """Read a cached result if it exists and hasn't expired."""
    path = CACHE_DIR / f"{key}.json"
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
        if time.time() - data.get("ts", 0) > CACHE_TTL:
            path.unlink(missing_ok=True)
            return None
        return data.get("value")
    except Exception:
        return None


def _cache_set(key: str, value: dict) -> None:
    """Write a result to the cache."""
    path = CACHE_DIR / f"{key}.json"
    try:
        path.write_text(json.dumps({"ts": time.time(), "value": value}))
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Rate limiting (public server only)
# ---------------------------------------------------------------------------


def _rate_limit_public():
    """Sleep if needed to avoid hammering the public OSRM server."""
    global _last_public_call
    if get_server() != PUBLIC_OSRM:
        return
    elapsed = time.time() - _last_public_call
    if elapsed < PUBLIC_RATE_LIMIT:
        time.sleep(PUBLIC_RATE_LIMIT - elapsed)
    _last_public_call = time.time()


# ---------------------------------------------------------------------------
# Core API
# ---------------------------------------------------------------------------


def route_distance(
    lat1: float,
    lon1: float,
    lat2: float,
    lon2: float,
    *,
    include_duration: bool = False,
) -> Optional[float] | Tuple[Optional[float], Optional[float]]:
    """
    Calculate road driving distance between two points.

    Args:
        lat1, lon1: Origin coordinates
        lat2, lon2: Destination coordinates
        include_duration: If True, return (distance_km, duration_min)

    Returns:
        Distance in km (float), or None if no route found.
        If include_duration=True, returns (distance_km, duration_min).
    """
    # Round coordinates to 6 decimal places (~0.1 m precision)
    coords = (round(lat1, 6), round(lon1, 6), round(lat2, 6), round(lon2, 6))
    cache_k = _cache_key("route", *coords)

    cached = _cache_get(cache_k)
    if cached is not None:
        dist = cached.get("distance_km")
        dur = cached.get("duration_min")
        if include_duration:
            return dist, dur
        return dist

    server = get_server()
    _rate_limit_public()

    url = f"{server}/route/v1/driving/{lon1},{lat1};{lon2},{lat2}"
    params = {"overview": "false", "steps": "false"}

    for attempt in range(3):
        try:
            resp = requests.get(url, params=params, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            if data.get("code") == "Ok" and data.get("routes"):
                route = data["routes"][0]
                distance_km = route["distance"] / 1000.0
                duration_min = route["duration"] / 60.0

                _cache_set(cache_k, {
                    "distance_km": round(distance_km, 3),
                    "duration_min": round(duration_min, 2),
                })

                if include_duration:
                    return round(distance_km, 3), round(duration_min, 2)
                return round(distance_km, 3)
            else:
                if include_duration:
                    return None, None
                return None

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            if attempt < 2:
                time.sleep(2 * (attempt + 1))
                continue
            if include_duration:
                return None, None
            return None
        except Exception as e:
            logger.error("OSRM route error: %s", e)
            if include_duration:
                return None, None
            return None

    if include_duration:
        return None, None
    return None


def route_distance_batch(
    origins: List[Tuple[float, float]],
    destinations: List[Tuple[float, float]],
) -> Optional[List[List[Optional[float]]]]:
    """
    Many-to-many driving distances using the OSRM Table API.

    Args:
        origins: List of (lat, lon) tuples
        destinations: List of (lat, lon) tuples

    Returns:
        2D list [origin_idx][dest_idx] of distances in km.
        None if the request fails entirely.
    """
    if not origins or not destinations:
        return None

    all_coords = list(origins) + list(destinations)
    coords_str = ";".join(f"{lon},{lat}" for lat, lon in all_coords)

    src_indices = list(range(len(origins)))
    dst_indices = list(range(len(origins), len(origins) + len(destinations)))

    # Check cache for the whole batch
    cache_k = _cache_key("table", origins, destinations)
    cached = _cache_get(cache_k)
    if cached is not None:
        return cached

    server = get_server()
    _rate_limit_public()

    url = f"{server}/table/v1/driving/{coords_str}"
    params = {
        "sources": ";".join(str(i) for i in src_indices),
        "destinations": ";".join(str(i) for i in dst_indices),
        "annotations": "distance",
    }

    for attempt in range(3):
        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()

            if data.get("code") != "Ok":
                logger.warning("OSRM table API returned: %s", data.get("code"))
                return None

            # Convert metres → km, replace None for unreachable
            distances = data.get("distances", [])
            result = []
            for row in distances:
                result.append([
                    round(d / 1000.0, 3) if d is not None else None
                    for d in row
                ])

            _cache_set(cache_k, result)
            return result

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            if attempt < 2:
                time.sleep(2 * (attempt + 1))
                continue
            return None
        except Exception as e:
            logger.error("OSRM table error: %s", e)
            return None

    return None


def route_duration(
    lat1: float, lon1: float, lat2: float, lon2: float
) -> Optional[float]:
    """Return driving duration in minutes between two points."""
    result = route_distance(lat1, lon1, lat2, lon2, include_duration=True)
    if isinstance(result, tuple):
        return result[1]
    return None


# ---------------------------------------------------------------------------
# Convenience / status
# ---------------------------------------------------------------------------


def status() -> Dict:
    """Return current OSRM connection status."""
    local_up = _check_local_server()
    return {
        "local_available": local_up,
        "active_server": LOCAL_OSRM if local_up else PUBLIC_OSRM,
        "cache_dir": str(CACHE_DIR),
        "cache_files": len(list(CACHE_DIR.glob("*.json"))),
    }


def clear_cache() -> int:
    """Remove all cached results. Returns count of files removed."""
    files = list(CACHE_DIR.glob("*.json"))
    for f in files:
        f.unlink(missing_ok=True)
    return len(files)


# ---------------------------------------------------------------------------
# Quick self-test
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    print("OSRM Status:", json.dumps(status(), indent=2))

    # Sydney CBD → Parramatta
    sydney = (-33.8688, 151.2093)
    parramatta = (-33.8151, 151.0011)

    print(f"\nSydney CBD -> Parramatta:")
    result = route_distance(*sydney, *parramatta, include_duration=True)
    if isinstance(result, tuple):
        dist, dur = result
        print(f"  Distance: {dist} km")
        print(f"  Duration: {dur} min")
    else:
        print(f"  Distance: {result} km")

    # Batch test: Sydney CBD -> Parramatta + Bondi
    bondi = (-33.8915, 151.2767)
    print(f"\nBatch: Sydney CBD -> [Parramatta, Bondi]:")
    batch = route_distance_batch([sydney], [parramatta, bondi])
    if batch:
        print(f"  To Parramatta: {batch[0][0]} km")
        print(f"  To Bondi: {batch[0][1]} km")
