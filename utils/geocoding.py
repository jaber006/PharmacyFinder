"""
Geocoding utilities using Nominatim (OpenStreetMap) - Free, no API key required.
"""
import time
from typing import Optional, Tuple
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError
from utils.database import Database


class Geocoder:
    def __init__(self, api_key: str = None, db: Optional[Database] = None):
        self.api_key = api_key  # Not used, kept for compatibility
        self.db = db
        # Nominatim requires a user agent
        self.geolocator = Nominatim(user_agent="pharmacy-location-finder/1.0")
        self.rate_limit_delay = 1.0  # Nominatim requires 1 second between requests

    def geocode(self, address: str, use_cache: bool = True) -> Optional[Tuple[float, float]]:
        """
        Convert an address to coordinates (latitude, longitude).

        Args:
            address: Street address to geocode
            use_cache: Whether to use cached results from database

        Returns:
            Tuple of (latitude, longitude) or None if not found
        """
        # Normalize address
        normalized_address = self._normalize_address(address)

        # Check cache first
        if use_cache and self.db:
            cached = self.db.get_cached_geocode(normalized_address)
            if cached:
                return cached

        # Call Nominatim (OpenStreetMap) API
        try:
            # Rate limiting - Nominatim requires 1 second between requests
            time.sleep(self.rate_limit_delay)

            location = self.geolocator.geocode(
                normalized_address,
                timeout=10,
                country_codes='au'  # Limit to Australia
            )

            if location:
                lat = location.latitude
                lng = location.longitude

                # Cache the result
                if self.db:
                    self.db.cache_geocode(normalized_address, lat, lng)

                return (lat, lng)
            else:
                print(f"Geocoding failed for {address}: No results found")
                return None

        except GeocoderTimedOut:
            print(f"Geocoding timeout for {address}")
            return None
        except GeocoderServiceError as e:
            print(f"Geocoding service error for {address}: {e}")
            return None
        except Exception as e:
            print(f"Error geocoding {address}: {e}")
            return None

    def reverse_geocode(self, latitude: float, longitude: float) -> Optional[str]:
        """
        Convert coordinates to an address.

        Args:
            latitude, longitude: Coordinates to reverse geocode

        Returns:
            Formatted address string or None if not found
        """
        try:
            # Rate limiting
            time.sleep(self.rate_limit_delay)

            location = self.geolocator.reverse(
                (latitude, longitude),
                timeout=10
            )

            if location:
                return location.address
            else:
                return None

        except Exception as e:
            print(f"Error reverse geocoding ({latitude}, {longitude}): {e}")
            return None

    def batch_geocode(self, addresses: list, use_cache: bool = True) -> dict:
        """
        Geocode multiple addresses.

        Args:
            addresses: List of addresses to geocode
            use_cache: Whether to use cached results

        Returns:
            Dict mapping addresses to (lat, lng) tuples or None
        """
        results = {}

        for address in addresses:
            coords = self.geocode(address, use_cache=use_cache)
            results[address] = coords

        return results

    def _normalize_address(self, address: str) -> str:
        """
        Normalize an address for consistent caching.

        Args:
            address: Raw address string

        Returns:
            Normalized address
        """
        # Convert to lowercase and strip whitespace
        normalized = address.lower().strip()

        # Ensure it includes Australia if not already specified
        if 'australia' not in normalized and not any(
            state in normalized for state in ['nsw', 'vic', 'qld', 'wa', 'sa', 'tas', 'nt', 'act']
        ):
            normalized += ', australia'

        return normalized

    def validate_australian_address(self, address: str) -> bool:
        """
        Check if an address is in Australia.

        Args:
            address: Address to validate

        Returns:
            True if address is in Australia
        """
        coords = self.geocode(address)
        if not coords:
            return False

        lat, lng = coords

        # Rough bounding box for Australia
        # Latitude: -44 to -10
        # Longitude: 113 to 154
        return -44 <= lat <= -10 and 113 <= lng <= 154
