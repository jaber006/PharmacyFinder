"""
Overpass API caching layer.

Provides reliable Overpass queries with:
- Local file-based caching (7-day expiry by default)
- Automatic fallback to cached data on API failure (429/504)
- Multiple mirror support
- Rate limiting

Usage:
    from utils.overpass_cache import cached_overpass_query

    data = cached_overpass_query(
        query='[out:json]...',
        cache_key='supermarkets_TAS',
    )
"""

import hashlib
import json
import os
import time
from pathlib import Path
from typing import Optional

import requests


# -- Configuration -------------------------------------------------

CACHE_DIR = Path("cache/overpass")
CACHE_EXPIRY_DAYS = 7

OVERPASS_MIRRORS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://maps.mail.ru/osm/tools/overpass/api/interpreter",
]

REQUEST_TIMEOUT = 120  # seconds
RATE_LIMIT_DELAY = 1.0  # seconds between requests

_last_request_time = 0.0


# -- Public API ----------------------------------------------------

def cached_overpass_query(
    query: str,
    cache_key: str = None,
    cache_expiry_days: float = CACHE_EXPIRY_DAYS,
    timeout: int = REQUEST_TIMEOUT,
    session: requests.Session = None,
) -> Optional[dict]:
    """
    Execute an Overpass API query with caching.

    Args:
        query: The Overpass QL query string
        cache_key: Unique key for caching. If None, auto-generated from query hash.
        cache_expiry_days: Cache expiry in days (default: 7)
        timeout: Request timeout in seconds
        session: Optional requests.Session to reuse

    Returns:
        Parsed JSON response dict, or None if all mirrors fail AND no cache exists.
    """
    if cache_key is None:
        cache_key = _hash_query(query)

    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file = CACHE_DIR / f"{cache_key}.json"

    # 1. Check cache (fresh)
    cached = _read_cache(cache_file, max_age_days=cache_expiry_days)
    if cached is not None:
        return cached

    # 2. Rate-limit
    _rate_limit()

    # 3. Try each mirror
    data = _query_mirrors(query, timeout=timeout, session=session)

    if data is not None:
        # Cache successful response
        _write_cache(cache_file, data)
        return data

    # 4. Fallback: use expired cache if available
    expired_cache = _read_cache(cache_file, max_age_days=None)  # no age limit
    if expired_cache is not None:
        print(f"    [CACHE] Using expired cache for '{cache_key}' (API unavailable)")
        return expired_cache

    return None


def invalidate_cache(cache_key: str = None, state: str = None):
    """
    Invalidate cached data.

    Args:
        cache_key: Specific key to invalidate
        state: Invalidate all caches for a state (prefix match)
    """
    if not CACHE_DIR.exists():
        return

    if cache_key:
        cache_file = CACHE_DIR / f"{cache_key}.json"
        if cache_file.exists():
            cache_file.unlink()
            print(f"    [CACHE] Invalidated: {cache_key}")

    if state:
        for f in CACHE_DIR.glob(f"{state}_*.json"):
            f.unlink()
            print(f"    [CACHE] Invalidated: {f.name}")


def get_cache_stats() -> dict:
    """Get cache statistics."""
    if not CACHE_DIR.exists():
        return {'total': 0, 'size_mb': 0, 'states': {}}

    files = list(CACHE_DIR.glob("*.json"))
    total_size = sum(f.stat().st_size for f in files)

    # Group by state prefix
    states = {}
    for f in files:
        prefix = f.stem.split('_')[0] if '_' in f.stem else 'other'
        states[prefix] = states.get(prefix, 0) + 1

    return {
        'total': len(files),
        'size_mb': round(total_size / 1024 / 1024, 2),
        'states': states,
    }


# -- Internal helpers ----------------------------------------------

def _hash_query(query: str) -> str:
    """Generate a deterministic cache key from a query string."""
    return hashlib.md5(query.strip().encode()).hexdigest()[:16]


def _read_cache(cache_file: Path, max_age_days: Optional[float] = None) -> Optional[dict]:
    """Read cached data if it exists and is within age limit."""
    if not cache_file.exists():
        return None

    if max_age_days is not None:
        age_days = (time.time() - cache_file.stat().st_mtime) / 86400
        if age_days > max_age_days:
            return None

    try:
        return json.loads(cache_file.read_text(encoding='utf-8'))
    except (json.JSONDecodeError, OSError):
        return None


def _write_cache(cache_file: Path, data: dict):
    """Write data to cache file."""
    try:
        cache_file.write_text(json.dumps(data), encoding='utf-8')
    except OSError as e:
        print(f"    [CACHE] Warning: could not write cache: {e}")


def _rate_limit():
    """Enforce rate limiting between requests."""
    global _last_request_time
    elapsed = time.time() - _last_request_time
    if elapsed < RATE_LIMIT_DELAY:
        time.sleep(RATE_LIMIT_DELAY - elapsed)
    _last_request_time = time.time()


def _query_mirrors(query: str, timeout: int = REQUEST_TIMEOUT,
                   session: requests.Session = None) -> Optional[dict]:
    """Try each Overpass mirror in order."""
    sess = session or requests.Session()

    for mirror_url in OVERPASS_MIRRORS:
        try:
            response = sess.post(
                mirror_url,
                data={'data': query},
                timeout=timeout,
                headers={'User-Agent': 'PharmacyFinder/1.0'},
            )

            if response.status_code == 200:
                return response.json()

            mirror_name = mirror_url.split('/')[2]
            if response.status_code in (429, 504):
                print(f"    [WARN] {mirror_name}: HTTP {response.status_code} (rate limited / timeout)")
            else:
                print(f"    [WARN] {mirror_name}: HTTP {response.status_code}")

        except requests.exceptions.Timeout:
            mirror_name = mirror_url.split('/')[2]
            print(f"    [WARN] {mirror_name}: request timed out")
        except Exception as e:
            mirror_name = mirror_url.split('/')[2]
            err = str(e)[:80]
            print(f"    [WARN] {mirror_name}: {err}")

    return None
