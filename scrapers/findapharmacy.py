"""
Scraper for findapharmacy.com.au — Pharmacy Guild of Australia.

This scraper uses the Funnelback search API that powers the
findapharmacy.com.au website. No Selenium needed — the API returns
structured JSON with pharmacy name, address, coordinates, opening hours,
and services.

API endpoint:
    https://tpgoa-search.funnelback.squiz.cloud/s/search.html

Key parameters:
    collection=tpgoa~sp-locations
    profile=react-data
    query=!null
    num_ranks=500       (max per request)
    sort=prox
    origin=LAT,LON      (search centre)
    maxdist=RADIUS_KM    (search radius in km)
    serviceKeyword=!null

The API caps at 500 results per query so we use multiple origin points
to ensure full coverage across each state.
"""

import requests
import json
import time
import os
import math
from typing import List, Dict, Optional, Tuple, Set
from datetime import datetime
from pathlib import Path

from utils.database import Database


# ---------- configuration -----------------------------------------

FUNNELBACK_URL = (
    "https://tpgoa-search.funnelback.squiz.cloud/s/search.html"
)

FUNNELBACK_PARAMS_TEMPLATE = {
    "collection": "tpgoa~sp-locations",
    "profile": "react-data",
    "query": "!null",
    "num_ranks": 500,
    "sort": "prox",
    "serviceKeyword": "!null",
}

# State centre points and radii for querying.
# For large states (NSW, VIC, QLD) we use multiple origin points
# to stay under the 500-result cap per query.
# Format: { state: [ (lat, lon, maxdist_km), ... ] }
STATE_QUERY_POINTS = {
    'TAS': [
        (-42.0, 146.5, 300),      # covers all of Tasmania
    ],
    'ACT': [
        (-35.28, 149.13, 50),     # Canberra area
    ],
    'NT': [
        (-12.46, 130.84, 200),    # Darwin
        (-23.70, 133.87, 500),    # Alice Springs / central
    ],
    'SA': [
        (-34.93, 138.60, 50),     # Adelaide CBD
        (-35.10, 138.55, 50),     # Adelaide south
        (-34.70, 138.70, 60),     # Adelaide north/east
        (-34.85, 138.35, 60),     # Adelaide west
        (-35.50, 138.50, 150),    # South SA
        (-33.50, 138.00, 300),    # Mid/North SA
        (-32.00, 137.00, 500),    # Far north SA
    ],
    'WA': [
        (-31.95, 115.86, 30),     # Perth CBD
        (-31.75, 115.85, 40),     # Perth north
        (-32.10, 115.85, 40),     # Perth south
        (-31.95, 116.10, 50),     # Perth east
        (-31.95, 115.60, 50),     # Perth west/coast
        (-33.30, 115.65, 100),    # Bunbury/SW
        (-28.77, 114.62, 300),    # Geraldton/midwest
        (-21.00, 119.00, 600),    # Pilbara/Kimberley
        (-34.00, 117.50, 200),    # Great Southern
        (-31.50, 121.50, 400),    # Goldfields
    ],
    'VIC': [
        (-37.81, 144.96, 8),      # Melbourne CBD core
        (-37.84, 145.00, 8),      # Melbourne S inner
        (-37.78, 145.02, 8),      # Melbourne E inner
        (-37.80, 144.88, 8),      # Melbourne W inner
        (-37.75, 145.15, 12),     # Melbourne E (Doncaster/Box Hill)
        (-37.90, 145.05, 12),     # Melbourne SE inner (Glen Waverley)
        (-37.88, 144.75, 12),     # Melbourne W (Footscray)
        (-37.73, 144.75, 15),     # Melbourne NW (Essendon)
        (-37.95, 145.20, 15),     # Melbourne SE (Dandenong)
        (-37.68, 145.00, 15),     # Melbourne N (Heidelberg)
        (-37.80, 145.35, 20),     # Melbourne outer E (Ringwood)
        (-37.60, 145.10, 20),     # Melbourne NE (Eltham)
        (-38.05, 145.10, 15),     # Melbourne S (Frankston)
        (-37.60, 144.70, 20),     # Melbourne NW (Tullamarine)
        (-37.55, 145.30, 25),     # Yarra Ranges
        (-38.00, 145.40, 20),     # Berwick/Pakenham
        (-37.70, 144.55, 25),     # Melton/Sunbury
        (-37.50, 144.85, 25),     # Craigieburn/Whittlesea
        (-38.15, 144.35, 40),     # Geelong
        (-37.55, 143.85, 50),     # Ballarat
        (-36.76, 144.28, 50),     # Bendigo
        (-36.36, 145.39, 50),     # Shepparton
        (-38.10, 145.50, 80),     # Gippsland W
        (-37.80, 147.00, 100),    # Gippsland E
        (-36.00, 146.50, 100),    # NE Victoria
        (-36.50, 142.00, 200),    # NW Victoria / Mildura
        (-38.35, 145.80, 80),     # South Gippsland
        (-37.30, 145.80, 80),     # Yarra Valley/Alpine
    ],
    'QLD': [
        (-27.47, 153.03, 10),     # Brisbane CBD
        (-27.40, 153.05, 12),     # Brisbane north inner
        (-27.55, 153.05, 12),     # Brisbane south inner
        (-27.47, 152.95, 12),     # Brisbane west inner
        (-27.35, 153.15, 15),     # Brisbane NE / Nundah
        (-27.55, 153.15, 15),     # Brisbane SE / Carindale
        (-27.35, 152.95, 15),     # Brisbane NW / Mitchelton
        (-27.60, 152.90, 15),     # Brisbane SW / Inala
        (-27.25, 153.10, 20),     # Pine Rivers / Redcliffe
        (-27.20, 153.00, 20),     # Caboolture
        (-27.70, 153.15, 20),     # Logan
        (-27.65, 153.35, 20),     # Redlands / Capalaba
        (-27.95, 153.35, 25),     # Gold Coast N
        (-28.05, 153.45, 20),     # Gold Coast Central
        (-28.20, 153.50, 20),     # Gold Coast S / Tweed
        (-26.65, 153.07, 30),     # Sunshine Coast S
        (-26.40, 153.05, 30),     # Sunshine Coast N
        (-27.60, 152.70, 30),     # Ipswich
        (-27.50, 152.40, 50),     # Toowoomba
        (-26.00, 152.50, 80),     # Wide Bay / Bundaberg
        (-24.85, 152.35, 50),     # Bundaberg/Gladstone
        (-23.38, 150.50, 60),     # Rockhampton
        (-21.14, 149.19, 60),     # Mackay
        (-19.26, 146.80, 50),     # Townsville
        (-16.92, 145.77, 50),     # Cairns
        (-20.00, 148.00, 150),    # Central QLD
        (-25.00, 147.00, 300),    # Western QLD
        (-18.00, 146.00, 100),    # North QLD
    ],
    'NSW': [
        (-33.87, 151.21, 6),      # Sydney CBD core
        (-33.84, 151.25, 8),      # Sydney E inner
        (-33.90, 151.18, 8),      # Sydney S inner
        (-33.87, 151.10, 8),      # Sydney W inner
        (-33.80, 151.18, 8),      # Sydney N inner
        (-33.80, 151.30, 12),     # North Shore / Mosman
        (-33.75, 151.15, 12),     # Upper North Shore
        (-33.93, 151.25, 10),     # Eastern suburbs
        (-33.85, 150.98, 12),     # Parramatta
        (-33.92, 151.05, 12),     # Strathfield/Burwood
        (-33.70, 151.05, 15),     # Ryde/Hornsby
        (-33.95, 151.35, 15),     # Sutherland/Cronulla
        (-34.05, 151.10, 15),     # Sydney SW (Campbelltown)
        (-33.92, 150.85, 15),     # Liverpool/Fairfield
        (-33.75, 150.80, 15),     # Blacktown
        (-33.70, 150.95, 15),     # Hills District
        (-33.65, 151.20, 15),     # Northern Beaches
        (-33.75, 150.65, 20),     # Penrith
        (-33.85, 150.65, 20),     # Blue Mountains
        (-33.55, 150.70, 20),     # Hawkesbury
        (-34.05, 150.85, 20),     # Macarthur
        (-33.40, 151.30, 25),     # Central Coast
        (-33.30, 151.50, 25),     # Central Coast North
        (-32.93, 151.78, 20),     # Newcastle
        (-32.75, 151.70, 25),     # Newcastle North / Lake Macquarie
        (-33.05, 151.65, 25),     # Lake Macquarie South
        (-32.50, 151.50, 40),     # Hunter Valley
        (-34.42, 150.90, 25),     # Wollongong
        (-34.75, 150.65, 50),     # South Coast
        (-35.30, 149.50, 50),     # Queanbeyan/Yass
        (-35.85, 150.10, 80),     # Far South Coast
        (-33.28, 149.10, 60),     # Bathurst / Orange
        (-33.50, 148.00, 80),     # Central West
        (-32.25, 148.60, 80),     # Dubbo / Mudgee
        (-31.00, 150.80, 80),     # Tamworth / New England
        (-30.50, 153.00, 60),     # Coffs Harbour
        (-29.50, 153.25, 60),     # Byron / Ballina
        (-28.80, 153.30, 60),     # Tweed/Lismore
        (-31.90, 152.50, 60),     # Port Macquarie
        (-31.40, 152.90, 40),     # Kempsey/Nambucca
        (-30.00, 151.50, 100),    # Northern Tablelands
        (-33.00, 146.00, 200),    # Far West NSW
        (-31.50, 145.50, 300),    # Outback NSW
    ],
}

CHECKPOINT_DIR = Path("scrapers/_checkpoints")

RATE_LIMIT_SECONDS = 0.5  # be gentle


# ---------- scraper class -----------------------------------------

class FindAPharmacyScraper:
    """
    Scrapes the Funnelback API behind findapharmacy.com.au.
    
    Usage:
        scraper = FindAPharmacyScraper(db)
        count = scraper.scrape_all('TAS')
    """

    def __init__(self, db: Database, geocoder=None):
        self.db = db
        self.geocoder = geocoder  # not needed — API has coords
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Referer": "https://findapharmacy.com.au/",
            "Accept": "application/json, text/javascript, */*; q=0.01",
        })
        self._seen_ids: Set[str] = set()  # for deduplication

    # -- public API ------------------------------------------------

    def scrape_all(self, region: str = 'TAS') -> int:
        """
        Scrape all pharmacies for a state/territory.
        
        Args:
            region: State/territory code (e.g. 'TAS', 'NSW')
            
        Returns:
            Number of pharmacies stored in the DB.
        """
        query_points = STATE_QUERY_POINTS.get(region)
        if not query_points:
            print(f"        No query points configured for {region}, using default centre")
            query_points = [(-28.57, 132.08, 5000)]  # all of Australia

        self._seen_ids.clear()
        total_new = 0
        total_dupes = 0

        # Load checkpoint if available
        checkpoint = self._load_checkpoint(region)
        completed_origins = set()
        if checkpoint:
            completed_origins = set(tuple(o) for o in checkpoint.get('completed_origins', []))
            print(f"        Resuming from checkpoint ({len(completed_origins)} origins already done)")

        for i, (lat, lon, radius_km) in enumerate(query_points, 1):
            origin_key = (lat, lon, radius_km)
            if origin_key in completed_origins:
                continue

            print(f"        [{i}/{len(query_points)}] Querying origin ({lat:.2f}, {lon:.2f}) r={radius_km} km...")

            try:
                results = self._query_api(lat, lon, radius_km)
                if results is None:
                    print(f"          -> API error, skipping")
                    continue

                new_count = 0
                for pharmacy in results:
                    if self._store_pharmacy(pharmacy, region):
                        new_count += 1
                    else:
                        total_dupes += 1

                total_new += new_count
                print(f"          -> {len(results)} results, {new_count} new")

                # Warn if we hit the 500 cap
                if len(results) >= 500:
                    print(f"          WARNING: Hit 500-result cap! Some pharmacies may be missed.")

            except Exception as e:
                print(f"          -> Error: {e}")

            # Save checkpoint
            completed_origins.add(origin_key)
            self._save_checkpoint(region, completed_origins)

            # Rate limit
            time.sleep(RATE_LIMIT_SECONDS)

        # Clean up checkpoint on completion
        self._clear_checkpoint(region)

        print(f"        Total: {total_new} new pharmacies stored, {total_dupes} duplicates skipped")
        return total_new

    def scrape_all_australia(self) -> int:
        """Scrape ALL Australian pharmacies across all states."""
        total = 0
        for state in STATE_QUERY_POINTS:
            print(f"\n  Scraping {state}...")
            count = self.scrape_all(state)
            total += count
        return total

    # -- API query -------------------------------------------------

    def _query_api(self, lat: float, lon: float, radius_km: float) -> Optional[List[Dict]]:
        """
        Query the Funnelback API.

        Args:
            lat, lon: Origin point
            radius_km: Max distance in km

        Returns:
            List of pharmacy dicts, or None on error.
        """
        params = dict(FUNNELBACK_PARAMS_TEMPLATE)
        params["origin"] = f"{lat},{lon}"
        params["maxdist"] = str(radius_km)

        try:
            resp = self.session.get(
                FUNNELBACK_URL,
                params=params,
                timeout=60,
            )
            resp.raise_for_status()
            data = resp.json()
            return data.get("results", [])
        except requests.exceptions.Timeout:
            print(f"          Timeout querying API")
            return None
        except requests.exceptions.HTTPError as e:
            print(f"          HTTP error: {e}")
            return None
        except (json.JSONDecodeError, ValueError) as e:
            print(f"          JSON parse error: {e}")
            return None
        except Exception as e:
            print(f"          Unexpected error: {e}")
            return None

    # -- data storage ----------------------------------------------

    def _store_pharmacy(self, data: Dict, region: str) -> bool:
        """
        Parse and store a single pharmacy result.
        Returns True if new, False if duplicate.
        """
        try:
            pharmacy_id = data.get("id", "")
            name = data.get("name", "").strip()
            if not name:
                return False

            # Dedup by ID
            if pharmacy_id in self._seen_ids:
                return False
            self._seen_ids.add(pharmacy_id)

            # Parse coordinates — geometry.coordinates = [lon, lat]
            geometry = data.get("geometry", {})
            coords = geometry.get("coordinates", [])
            if len(coords) < 2:
                return False

            try:
                longitude = float(coords[0])
                latitude = float(coords[1])
            except (ValueError, TypeError):
                return False

            # Sanity check — coordinates should be in Australia
            if not (-45 <= latitude <= -10 and 110 <= longitude <= 155):
                return False

            # Filter by state if the result has a state field
            result_state = data.get("state", "").upper().strip()
            # Normalize full state names to abbreviations
            state_name_map = {
                'VICTORIA': 'VIC', 'NEW SOUTH WALES': 'NSW',
                'QUEENSLAND': 'QLD', 'SOUTH AUSTRALIA': 'SA',
                'WESTERN AUSTRALIA': 'WA', 'TASMANIA': 'TAS',
                'NORTHERN TERRITORY': 'NT',
                'AUSTRALIAN CAPITAL TERRITORY': 'ACT',
            }
            result_state = state_name_map.get(result_state, result_state)
            if result_state and result_state != region:
                # Allow results from nearby states (border pharmacies)
                # but we still store them — they're real pharmacies
                pass

            # Build full address
            address_parts = []
            for field in ("address", "address2", "address3"):
                val = data.get(field)
                if val and val.strip():
                    address_parts.append(val.strip())
            city = data.get("city", "").strip()
            state = data.get("state", "").strip()
            postcode = data.get("postcode", "").strip()
            if city:
                address_parts.append(city)
            if state:
                address_parts.append(state)
            if postcode:
                address_parts.append(postcode)
            address = ", ".join(address_parts) if address_parts else f"{name}, {region}"

            # Build opening hours summary
            opening_hours = self._format_opening_hours(data)

            pharmacy_data = {
                "name": name,
                "address": address,
                "latitude": latitude,
                "longitude": longitude,
                "source": "findapharmacy.com.au",
                "opening_hours": opening_hours,
                "suburb": city,
                "state": result_state or region,
                "postcode": postcode,
            }

            self.db.insert_pharmacy(pharmacy_data)
            return True

        except Exception as e:
            return False

    def _format_opening_hours(self, data: Dict) -> str:
        """Format opening hours from the API response."""
        days = ["monday", "tuesday", "wednesday", "thursday",
                "friday", "saturday", "sunday"]
        parts = []
        for day in days:
            hours = data.get(day, {})
            if isinstance(hours, dict):
                open_time = hours.get("open", "Closed")
                close_time = hours.get("close", "Closed")
                if open_time and open_time != "Closed":
                    parts.append(f"{day[:3].title()}: {open_time}-{close_time}")
                else:
                    parts.append(f"{day[:3].title()}: Closed")
        return "; ".join(parts)

    # -- checkpoint / resume ---------------------------------------

    def _load_checkpoint(self, region: str) -> Optional[Dict]:
        path = CHECKPOINT_DIR / f"findapharmacy_{region}.json"
        if path.exists():
            try:
                with open(path, "r") as f:
                    return json.load(f)
            except Exception:
                return None
        return None

    def _save_checkpoint(self, region: str, completed_origins: set):
        CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
        path = CHECKPOINT_DIR / f"findapharmacy_{region}.json"
        data = {
            "region": region,
            "completed_origins": [list(o) for o in completed_origins],
            "timestamp": datetime.now().isoformat(),
        }
        with open(path, "w") as f:
            json.dump(data, f)

    def _clear_checkpoint(self, region: str):
        path = CHECKPOINT_DIR / f"findapharmacy_{region}.json"
        if path.exists():
            path.unlink()
