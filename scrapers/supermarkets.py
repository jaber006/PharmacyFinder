"""
Scraper for supermarket locations (Woolworths, Coles, ALDI, IGA).

Sources:
1. OpenStreetMap Overpass API (free, comprehensive, has coordinates)
2. Manual CSV import
"""
import requests
import time
import json
from typing import List, Dict, Optional
from utils.database import Database
from utils.geocoding import Geocoder
from utils.boundaries import in_state
from utils.overpass_cache import cached_overpass_query
import config


class SupermarketScraper:
    def __init__(self, db: Database, geocoder: Geocoder):
        self.db = db
        self.geocoder = geocoder
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': config.SCRAPER_CONFIG['user_agent']
        })

    def scrape_all(self, region: str = 'TAS') -> int:
        """
        Scrape all supermarket locations from available sources.

        Args:
            region: Australian state/territory code

        Returns:
            Number of supermarkets scraped
        """
        total = 0

        # Primary: OpenStreetMap Overpass API
        print(f"  [1/1] Scraping OpenStreetMap for supermarkets in {region}...")
        osm_count = self.scrape_osm_overpass(region)
        total += osm_count
        print(f"        Found {osm_count} supermarkets from OSM")

        print(f"  Total supermarkets: {total}")
        return total

    def scrape_osm_overpass(self, region: str = 'TAS') -> int:
        """
        Scrape supermarket locations from OpenStreetMap.
        Uses cached_overpass_query for reliable results with automatic
        fallback to cached data on API failure.
        """
        count = 0
        state_name = config.AUSTRALIAN_STATES.get(region, region)

        query = f"""
        [out:json][timeout:120];
        area["name"="{state_name}"]["admin_level"="4"]->.state;
        (
          node["shop"="supermarket"](area.state);
          way["shop"="supermarket"](area.state);
          relation["shop"="supermarket"](area.state);
        );
        out center;
        """

        data = cached_overpass_query(
            query=query,
            cache_key=f"supermarkets_{region}",
            session=self.session,
        )

        if data is None:
            print(f"        [ERROR] Overpass unavailable and no cache for supermarkets_{region}")
            return 0

        elements = data.get('elements', [])
        print(f"        Overpass returned {len(elements)} elements")

        for element in elements:
            sm_data = self._parse_osm_supermarket(element, region)
            if sm_data:
                self.db.insert_supermarket(sm_data)
                count += 1

        return count

    def _parse_osm_supermarket(self, element: Dict, region: str) -> Optional[Dict]:
        """Parse OSM element into supermarket data."""
        try:
            tags = element.get('tags', {})

            if element.get('type') == 'node':
                lat = element.get('lat')
                lon = element.get('lon')
            else:
                center = element.get('center', {})
                lat = center.get('lat', element.get('lat'))
                lon = center.get('lon', element.get('lon'))

            if not lat or not lon:
                return None

            lat, lon = float(lat), float(lon)
            if not in_state(lat, lon, region):
                return None

            name = tags.get('name', 'Supermarket')
            brand = tags.get('brand', '')

            # Use brand name if name is generic
            if brand and ('supermarket' in name.lower() or name == brand):
                name = brand

            # Build address
            addr_parts = []
            if tags.get('addr:housenumber'):
                addr_parts.append(tags['addr:housenumber'])
            if tags.get('addr:street'):
                addr_parts.append(tags['addr:street'])
            if tags.get('addr:suburb') or tags.get('addr:city'):
                addr_parts.append(tags.get('addr:suburb', tags.get('addr:city', '')))
            addr_parts.append(region)
            if tags.get('addr:postcode'):
                addr_parts.append(tags['addr:postcode'])

            address = ', '.join(p for p in addr_parts if p)
            if not address or address == region:
                address = f"{name}, {region}, Australia"

            # Estimate floor area for major chains
            floor_area = self._estimate_floor_area(name, brand)

            return {
                'name': name,
                'address': address,
                'latitude': float(lat),
                'longitude': float(lon),
                'floor_area_sqm': floor_area,
            }

        except Exception:
            return None

    def _estimate_floor_area(self, name: str, brand: str = '') -> Optional[float]:
        """
        Estimate floor area based on supermarket chain/type.
        Major supermarkets in Australia typically:
        - Woolworths: 2,500-4,000 sqm
        - Coles: 2,500-4,000 sqm
        - ALDI: 1,200-1,500 sqm
        - IGA: 500-2,000 sqm
        """
        combined = f"{name} {brand}".lower()

        if 'woolworths' in combined or 'woolies' in combined:
            return 3000.0  # Average Woolworths
        elif 'coles' in combined:
            return 3000.0  # Average Coles
        elif 'aldi' in combined:
            return 1300.0  # Average ALDI
        elif 'iga' in combined:
            return 1200.0  # Average IGA
        elif 'harris farm' in combined:
            return 2000.0
        else:
            return 1500.0  # Conservative default for unknown

    def add_manual_supermarket(self, name: str, address: str,
                                floor_area_sqm: Optional[float] = None) -> bool:
        """Manually add a supermarket."""
        try:
            coords = self.geocoder.geocode(address)
            if not coords:
                print(f"Could not geocode: {address}")
                return False

            lat, lon = coords

            self.db.insert_supermarket({
                'name': name,
                'address': address,
                'latitude': lat,
                'longitude': lon,
                'floor_area_sqm': floor_area_sqm or self._estimate_floor_area(name),
            })
            print(f"  Added supermarket: {name}")
            return True

        except Exception as e:
            print(f"Error adding supermarket: {e}")
            return False

    def import_from_csv(self, csv_path: str) -> int:
        """Import supermarkets from CSV."""
        import csv
        count = 0

        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    name = row.get('name', '')
                    address = row.get('address', '')
                    if not address:
                        continue

                    floor_area = row.get('floor_area_sqm')
                    if floor_area:
                        try:
                            floor_area = float(floor_area)
                        except (ValueError, TypeError):
                            floor_area = None

                    lat = row.get('latitude')
                    lon = row.get('longitude')
                    if not lat or not lon:
                        coords = self.geocoder.geocode(address)
                        if coords:
                            lat, lon = coords
                        else:
                            continue
                    else:
                        lat, lon = float(lat), float(lon)

                    self.db.insert_supermarket({
                        'name': name or 'Supermarket',
                        'address': address,
                        'latitude': lat,
                        'longitude': lon,
                        'floor_area_sqm': floor_area,
                    })
                    count += 1

        except Exception as e:
            print(f"Error importing CSV: {e}")

        return count

    def is_major_supermarket(self, name: str) -> bool:
        """Check if a supermarket is a major chain."""
        name_lower = name.lower()
        return any(chain in name_lower for chain in config.MAJOR_SUPERMARKETS)
