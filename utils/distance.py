"""
Distance calculation utilities using Haversine formula and OSRM routing.
"""
import math
import requests
from typing import Tuple, List, Dict, Optional
from geopy.distance import geodesic


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate straight-line distance between two points using Haversine formula.

    Args:
        lat1, lon1: Coordinates of first point
        lat2, lon2: Coordinates of second point

    Returns:
        Distance in kilometers
    """
    # Convert to radians
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)

    # Haversine formula
    a = (math.sin(delta_lat / 2) ** 2 +
         math.cos(lat1_rad) * math.cos(lat2_rad) *
         math.sin(delta_lon / 2) ** 2)
    c = 2 * math.asin(math.sqrt(a))

    # Earth's radius in kilometers
    radius = 6371.0

    return c * radius


def distance_between_points(point1: Tuple[float, float], point2: Tuple[float, float]) -> float:
    """
    Calculate distance between two coordinate points using geopy (more accurate).

    Args:
        point1: (latitude, longitude)
        point2: (latitude, longitude)

    Returns:
        Distance in kilometers
    """
    return geodesic(point1, point2).kilometers


def find_nearest(
    target_lat: float,
    target_lon: float,
    locations: List[Dict]
) -> Tuple[Optional[Dict], Optional[float]]:
    """
    Find the nearest location from a list to a target point.

    Args:
        target_lat, target_lon: Target coordinates
        locations: List of dicts with 'latitude' and 'longitude' keys

    Returns:
        Tuple of (nearest_location, distance_km) or (None, None) if no locations
    """
    if not locations:
        return None, None

    min_distance = float('inf')
    nearest_location = None

    for location in locations:
        if 'latitude' not in location or 'longitude' not in location:
            continue

        distance = haversine_distance(
            target_lat, target_lon,
            location['latitude'], location['longitude']
        )

        if distance < min_distance:
            min_distance = distance
            nearest_location = location

    return nearest_location, min_distance


def find_within_radius(
    target_lat: float,
    target_lon: float,
    locations: List[Dict],
    radius_km: float
) -> List[Tuple[Dict, float]]:
    """
    Find all locations within a specified radius of a target point.

    Args:
        target_lat, target_lon: Target coordinates
        locations: List of dicts with 'latitude' and 'longitude' keys
        radius_km: Search radius in kilometers

    Returns:
        List of tuples (location, distance_km) sorted by distance
    """
    results = []

    for location in locations:
        if 'latitude' not in location or 'longitude' not in location:
            continue

        distance = haversine_distance(
            target_lat, target_lon,
            location['latitude'], location['longitude']
        )

        if distance <= radius_km:
            results.append((location, distance))

    # Sort by distance
    results.sort(key=lambda x: x[1])

    return results


def get_driving_distance(
    origin_lat: float,
    origin_lon: float,
    dest_lat: float,
    dest_lon: float,
    osrm_server: str = "http://router.project-osrm.org"
) -> Optional[float]:
    """
    Calculate driving distance using OSRM routing service.

    Args:
        origin_lat, origin_lon: Origin coordinates
        dest_lat, dest_lon: Destination coordinates
        osrm_server: OSRM server URL (default: public OSRM server)

    Returns:
        Driving distance in kilometers, or None if route not found
    """
    import time as _time
    
    for attempt in range(3):
        try:
            url = f"{osrm_server}/route/v1/driving/{origin_lon},{origin_lat};{dest_lon},{dest_lat}"
            params = {
                'overview': 'false',
                'steps': 'false'
            }

            response = requests.get(url, params=params, timeout=15)
            response.raise_for_status()

            data = response.json()

            if data.get('code') == 'Ok' and data.get('routes'):
                # Distance is in meters, convert to kilometers
                distance_m = data['routes'][0]['distance']
                return distance_m / 1000.0
            else:
                return None

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            if attempt < 2:
                _time.sleep(2 * (attempt + 1))
                continue
            return None
        except Exception as e:
            return None


def get_driving_time(
    origin_lat: float,
    origin_lon: float,
    dest_lat: float,
    dest_lon: float,
    osrm_server: str = "http://router.project-osrm.org"
) -> Optional[float]:
    """
    Calculate driving time using OSRM routing service.

    Args:
        origin_lat, origin_lon: Origin coordinates
        dest_lat, dest_lon: Destination coordinates
        osrm_server: OSRM server URL

    Returns:
        Driving time in minutes, or None if route not found
    """
    try:
        url = f"{osrm_server}/route/v1/driving/{origin_lon},{origin_lat};{dest_lon},{dest_lat}"
        params = {
            'overview': 'false',
            'steps': 'false'
        }

        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()

        data = response.json()

        if data.get('code') == 'Ok' and data.get('routes'):
            # Duration is in seconds, convert to minutes
            duration_s = data['routes'][0]['duration']
            return duration_s / 60.0
        else:
            return None

    except Exception as e:
        print(f"Error getting driving time: {e}")
        return None


def format_distance(distance_km: float) -> str:
    """
    Format distance for display.

    Args:
        distance_km: Distance in kilometers

    Returns:
        Formatted string (e.g., "2.5 km" or "850 m")
    """
    if distance_km < 1.0:
        return f"{int(distance_km * 1000)} m"
    else:
        return f"{distance_km:.2f} km"


def calculate_fte_from_hours(hours_per_week: float) -> float:
    """
    Calculate FTE (Full Time Equivalent) from hours per week.
    38 hours/week = 1.0 FTE
    Minimum 20 hours to count.

    Args:
        hours_per_week: Weekly hours worked

    Returns:
        FTE value, or 0 if below minimum threshold
    """
    if hours_per_week < 20:
        return 0.0

    return hours_per_week / 38.0
